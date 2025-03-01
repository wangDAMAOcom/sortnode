/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */
#include <backend.h>
#include <basic.h>
#include <config.h>
#include <cross_node_join_manager.h>
#include <data_source.h>
#include <data_space.h>
#include <expression.h>
#include <handler.h>
#include <multiple.h>
#include <parser.h>
#include <partition_method.h>
#include <plan.h>
#include <routine.h>
#include <session.h>
#include <sql_exception.h>
#include <statement.h>
#include <string.h>
#include <sub_query.h>
#include <table_info.h>
#include <transfer_rule.h>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <limits>
#include <vector>

#ifndef DBSCALE_DISABLE_SPARK
#include <spark_client.h>
#endif

#define DBSCALE_SPARK_TMP "dbscale_spark_tmp"
#define MAX_RECURSION_LEVEL 500

using namespace dbscale;
using namespace dbscale::sql;
using namespace dbscale::plan;
namespace dbscale {
namespace sql {

// TODO: Support mul partition keys if needed.

#define HAS_IDENTIFY_COLUMNS 0
#define NO_IDENTIFY_COLUMNS 1

enum condition_type {
  CONDITION_TYPE_NON,
  CONDITION_TYPE_EQUAL,
  CONDITION_TYPE_RANGE,
  CONDITION_TYPE_RANDOM
};

enum ConditionAndOr { CONDITION_AND, CONDITION_OR, CONDITION_NO_COND };

bool check_and_transform_autocommit_value(string &value) {
  if (!strcasecmp(value.c_str(), "ON") || !strcasecmp(value.c_str(), "'ON'") ||
      !strcasecmp(value.c_str(), "'TRUE'") || !strcasecmp(value.c_str(), "1")) {
    return true;
  }

  if (!strcasecmp(value.c_str(), "OFF") || !strcasecmp(value.c_str(), "'ON'") ||
      !strcasecmp(value.c_str(), "FALSE") || !strcasecmp(value.c_str(), "0")) {
    return false;
  }

  throw dbscale::sql::SQLError(
      "Variable 'autocommit' can't be set to the value", "42000", 1231);
}

inline bool is_terminator_has_null(char *term, unsigned int len) {
  if (len == 0) return false;

  for (unsigned int i = 0; i < len; ++i) {
    if (term[i] == '\0') return true;
  }
  return false;
}

bool is_executed_as_table(record_scan *rs) {
  return rs->subquerytype == SUB_SELECT_TABLE ||
         rs->subquerytype == SUB_SELECT_MUL_COLUMN;
}

static bool judge_need_apply_meta_datasource(
    map<DataSpace *, const char *> &spaces_map) {
  DataSpace *meta_dataspace = Backend::instance()->get_metadata_data_space();
  map<DataSpace *, const char *>::iterator it_space = spaces_map.begin();
  bool need_apply_meta = true;
  for (; it_space != spaces_map.end(); ++it_space) {
    if (is_share_same_server(meta_dataspace, it_space->first)) {
      need_apply_meta = false;
      break;
    }
  }
  return need_apply_meta;
}

CenterJoinGarbageCollector::CenterJoinGarbageCollector() {
  reconstruct_sql = NULL;
  generated_sql = NULL;
  table_vector_query = NULL;
  fake_rs_vector.clear();
  handler = NULL;
  new_table_vector = NULL;
}

void CenterJoinGarbageCollector::clear_garbages() {
  if (reconstruct_sql) delete reconstruct_sql;
  reconstruct_sql = NULL;

  Backend *backend = Backend::instance();
  DataSpace *ds =
      backend->get_data_space_for_table("mytest", "center_join_dataspace");
  if (generated_sql) {
    unsigned int generated_sql_size = generated_sql->size();
    unsigned int tmp_table_size = (generated_sql_size - 1) / 3;
    for (unsigned int i = tmp_table_size * 2 + 1; i < generated_sql_size; ++i) {
      string drop_sql = generated_sql->at(i);
      // TODO this method will throw exception, better not in destructor, fix it
      // in the future
      try {
        ds->execute_one_modify_sql(drop_sql.c_str(), handler, NULL);
      } catch (...) {
        LOG_ERROR("Drop center join tmp tables failed for sql [%s]\n",
                  drop_sql.c_str());
      }
    }
    delete generated_sql;
  }
  generated_sql = NULL;

  if (table_vector_query) delete table_vector_query;
  table_vector_query = NULL;

  if (new_table_vector) {
    delete new_table_vector;
    new_table_vector = NULL;
  }
  vector<record_scan *>::iterator it = fake_rs_vector.begin();
  for (; it != fake_rs_vector.end(); ++it) {
    delete *it;
  }
  fake_rs_vector.clear();

  vector<DataSpace *>::iterator it_ds = ds_vector.begin();
  for (; it_ds != ds_vector.end(); ++it_ds) {
    Schema *join_schema = backend->find_schema(TMP_TABLE_SCHEMA);
    join_schema->remove_table((*it_ds)->get_name());
    delete *it_ds;
  }
  ds_vector.clear();

  DataSpace *ds_spark =
      backend->get_data_space_for_table(spark_dst_schema.c_str(), NULL);
  if (!spark_sql_drop_sql.empty()) {
    try {
      ds_spark->execute_one_modify_sql(spark_sql_drop_sql.c_str(), handler,
                                       NULL);
    } catch (...) {
      LOG_ERROR("Drop center join tmp tables failed for sql [%s]\n",
                spark_sql_drop_sql.c_str());
    }
  }
  if (!spark_sql_table_name.empty())
    backend->remove_spark_sql_table_name(spark_sql_table_name);
  spark_sql_table_name.clear();
}

void check_partition_distinct(table_link *par_tb) {
  /*Assume that this sql will affect more than one partition of partition
   * table.*/
  /*Check the distinct usage for partition table. Currently, dbscale does
   * not support the partition table record scan has distinct opertion,
   * which should be done by using table subquery.*/
  record_scan *par_rs = par_tb->join->cur_rec_scan;
  if (par_rs->options & SQL_OPT_DISTINCT ||
      par_rs->options & SQL_OPT_DISTINCTROW) {
    LOG_ERROR(
        "Not support to use distinct[row] for partition table directly, plz "
        "check manual and use table subquery for distinct1 with options %d.\n",
        par_rs->options);
    throw UnSupportPartitionSQL(
        "UnSupport partition table sql with distinct, plz check manual and use "
        "table subquery for distinct.");
  }
}

void Statement::fulfill_tb_list(record_scan *rs, table_link *tl,
                                list<TableNameStruct> *tns_list) {
  if (tl->join->type == JOIN_NODE_SINGLE && tl->join->cur_rec_scan == rs) {
    TableNameStruct tns;
    tns.schema_name =
        string(tl->join->schema_name ? tl->join->schema_name : schema);
    tns.table_name = string(tl->join->table_name);
    tns.table_alias = string(tl->join->alias ? tl->join->alias : "");
    tns_list->push_back(tns);
  }
  if (tl->next) fulfill_tb_list(rs, tl->next, tns_list);
}

void inline append_column_fields_and_groupby_fragment(
    vector<TableColumnInfo> *column_info_vec, string &column_prefix,
    string &all_column_fields, string &groupby_fragment) {
  size_t column_total_count = column_info_vec->size();
  for (unsigned int i = 0; i < column_total_count; ++i) {
    all_column_fields.append(column_prefix);
    groupby_fragment.append(column_prefix);

    all_column_fields.append("`");
    all_column_fields.append((*column_info_vec)[i].column_name);
    // FIXME: uncomment below 2 line after issue #3294
    // all_column_fields.append(" AS ");
    // all_column_fields.append((*columns)[i]);
    all_column_fields.append("`, ");
    groupby_fragment.append("`");
    groupby_fragment.append((*column_info_vec)[i].column_name);
    groupby_fragment.append("`, ");
  }
}

bool need_hex(string column_type) {
  boost::to_lower(column_type);
  if (column_type.find("binary") != string::npos ||
      column_type.find("blob") != string::npos ||
      column_type.find("bit") != string::npos)
    return true;
  return false;
}

#define SAVE_STR_STMT(var, str)          \
  var = NULL;                            \
  if (str != NULL && strlen(str) != 0) { \
    tmp = new char[strlen(str) + 1];     \
    strncpy(tmp, str, strlen(str) + 1);  \
    var = tmp;                           \
    stmt_add_dynamic_str(tmp);           \
  }

void Statement::fulfill_hex_pos_real(const char *schema_name,
                                     const char *table_name,
                                     set<string> col_names) {
  hex_pos.clear();
  TableInfoCollection *tic = TableInfoCollection::instance();
  TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);
  vector<TableColumnInfo> *column_info_vec;
  try {
    column_info_vec =
        ti->element_table_column->get_element_vector_table_column_info(
            stmt_session);
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info column "
        "info(vector_table_column_info) of table [%s.%s]\n",
        schema_name, table_name);
    ti->release_table_info_lock();
    throw;
  }
  int pos = 0;
  vector<TableColumnInfo>::iterator it = column_info_vec->begin();
  for (; it != column_info_vec->end(); ++it) {
    if (need_hex(it->column_type)) {
      if (!col_names.empty()) {
        string tmp_name = it->column_name;
        boost::to_lower(tmp_name);
        if (col_names.count(tmp_name)) {
          hex_pos.insert(pos);
        }
      } else
        hex_pos.insert(pos);
    }
    ++pos;
  }
  ti->release_table_info_lock();
  LOG_DEBUG(
      "Statement::fulfill_hex_pos with hex_pos size %d for table [%s.%s].\n",
      hex_pos.size(), schema_name, table_name);
}

void Statement::fulfill_hex_pos(insert_op_node *insert_op) {
  table_link *modify_table = get_one_modify_table();
  name_item *cols_head = insert_op->insert_column_list;
  name_item *col = cols_head;
  const char *schema_name = modify_table->join->schema_name
                                ? modify_table->join->schema_name
                                : schema;
  const char *table_name = modify_table->join->table_name;
  set<string> col_names;
  if (col != NULL) do {
      string n(col->name);
      boost::to_lower(n);
      if (col_names.count(n)) {
        throw Error("Insert with duplicate col.");
      }
      col_names.insert(n);
      col = col->next;
    } while (col != cols_head);
  fulfill_hex_pos_real(schema_name, table_name, col_names);
  LOG_DEBUG("Statement::fulfill_hex_pos with hex_pos size %d for sql [%s].\n",
            hex_pos.size(), sql);
}

int fill_column_to_expend_columns(vector<string> &expend_columns,
                                  string column_prefix, const char *schema_name,
                                  const char *table_name, int field_pos,
                                  set<int> hex_pos, Session *stmt_session) {
  TableInfoCollection *tic = TableInfoCollection::instance();
  TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);

  vector<TableColumnInfo> *column_info_vec;
  try {
    column_info_vec =
        ti->element_table_column->get_element_vector_table_column_info(
            stmt_session);
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info column "
        "info(vector_table_column_info) of table [%s.%s]\n",
        schema_name, table_name);
    ti->release_table_info_lock();
    throw;
  }

  size_t column_total_count = column_info_vec->size();
  for (unsigned int i = 0; i < column_total_count; ++i) {
    string expend_column_str;
    if (hex_pos.count(field_pos)) {
      expend_column_str.append("hex(");
    }
    expend_column_str.append(column_prefix);
    expend_column_str.append("`");
    expend_column_str.append((*column_info_vec)[i].column_name);
    expend_column_str.append("`");
    if (hex_pos.count(field_pos)) {
      expend_column_str.append(")");
    }
    ++field_pos;
    expend_columns.push_back(expend_column_str);
  }
  ti->release_table_info_lock();
  return field_pos;
}

void Statement::handle_insert_select_hex_column() {
  if (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT) {
    LOG_DEBUG("Statement::handle_insert_select_hex_column with sql[%s].\n",
              sql);
    string tmp_select_sub_sql = select_sub_sql;
    try {
      vector<string> expend_columns;
      insert_op_node *insert_op = st.sql->insert_oper;
      fulfill_hex_pos(insert_op);
      expend_field_list(insert_op, expend_columns, sql);
      adjust_select_sub_sql_for_hex(expend_columns);
    } catch (exception &e) {
      LOG_DEBUG(
          "Statement::handle_insert_select_hex_column get exception %s.\n",
          e.what());
      select_sub_sql.assign(tmp_select_sub_sql.c_str());
      hex_pos.clear();
    }
  }
}

void Statement::adjust_select_sub_sql_for_hex(vector<string> expend_columns) {
  if (hex_pos.empty() || expend_columns.empty()) {
    hex_pos.clear();
    return;
  }
  vector<string>::iterator it = expend_columns.begin();
  string new_col_str;
  new_col_str.append((*it).c_str());
  ++it;
  for (; it != expend_columns.end(); ++it) {
    new_col_str.append(",");
    new_col_str.append((*it).c_str());
  }
  string new_sql("select ");
  new_sql.append(new_col_str.c_str());
  new_sql.append(" ");
  insert_op_node *insert_op = st.sql->insert_oper;
  record_scan *rs = insert_op->select_values;

  if (rs->from_pos >= 1) {
    new_sql.append(sql + rs->from_pos - 1);
  }
  LOG_DEBUG(
      "Statement::adjust_select_sub_sql_for_hex get new select_sub_sql is "
      "[%s].\n",
      new_sql.c_str());
  select_sub_sql.assign(new_sql.c_str());
}

void Statement::expend_field_list_real(field_item *field,
                                       vector<string> &expend_columns,
                                       const char *handle_sql,
                                       list<TableNameStruct> &tns_list,
                                       record_scan *rs) {
  int field_pos = 0;
  while (field) {
    bool has_added = false;
    if (field->field_expr && field->field_expr->type == EXPR_STR) {
      StrExpression *str_expr = (StrExpression *)(field->field_expr);
      LOG_DEBUG(
          "Statement::expend_field_list handle column %s with expend_columns "
          "size %d with star_flag %d tns_list size %d.\n",
          str_expr->str_value, expend_columns.size(),
          str_expr->get_has_star_flag() ? 1 : 0, tns_list.size());
      if (str_expr->get_has_star_flag()) {
        string field_string_value = string(str_expr->str_value);
        size_t first_dot_position = field_string_value.find(".");
        size_t second_dot_position =
            field_string_value.find(".", first_dot_position + 1);
        if (first_dot_position != string::npos && (!handle_sql || !rs)) {
          LOG_ERROR(
              "Statement::expend_field_list_real should has non NULL "
              "handle_sql and rs if not only *.\n");
          return;
        }

        if (first_dot_position == string::npos) {
          // field is '*'
          list<TableNameStruct>::iterator it = tns_list.begin();
          for (; it != tns_list.end(); ++it) {
            string column_prefix;
            column_prefix.append("`");
            if (!(*it).table_alias.empty())
              column_prefix.append((*it).table_alias);
            else
              column_prefix.append((*it).table_name);
            column_prefix.append("`.");

            field_pos = fill_column_to_expend_columns(
                expend_columns, column_prefix, (*it).schema_name.c_str(),
                (*it).table_name.c_str(), field_pos, hex_pos, stmt_session);
          }
        } else if (second_dot_position == string::npos) {
          // field is 't1.*'
          string table_name_in_field =
              field_string_value.substr(0, first_dot_position);
          string column_prefix;
          list<TableNameStruct>::iterator it = tns_list.begin();
          string schema_name, table_name;
          for (; it != tns_list.end(); ++it) {
            if (!(*it).table_alias.empty()) {
              if (!strcmp(it->table_alias.c_str(),
                          table_name_in_field.c_str())) {  // table alias match
                column_prefix.append("`");
                column_prefix.append((*it).table_alias);
                column_prefix.append("`.");
                break;
              }
            } else if (!strcmp(
                           it->table_name.c_str(),
                           table_name_in_field.c_str())) {  // table name match
              column_prefix.append("`");
              column_prefix.append((*it).table_name);
              column_prefix.append("`.");
              break;
            }
          }
          if (it == tns_list.end()) {
            char msg[128];
            sprintf(msg, "Table name [%s] do not found in table list.",
                    table_name_in_field.c_str());
            LOG_ERROR("%s\n", msg);
            throw Error(msg);
          }

          field_pos = fill_column_to_expend_columns(
              expend_columns, column_prefix, (*it).schema_name.c_str(),
              (*it).table_name.c_str(), field_pos, hex_pos, stmt_session);
        } else {
          // the expression is like 'test.t1.*'
          string schema_name_in_field =
              field_string_value.substr(0, first_dot_position);
          string table_name_in_field = field_string_value.substr(
              first_dot_position + 1,
              second_dot_position - first_dot_position - 1);
          string column_prefix =
              field_string_value.substr(0, second_dot_position + 1);

          map<string, string, strcasecomp> alias_table_map =
              get_alias_table_map(rs);
          if (alias_table_map.count(table_name_in_field))
            table_name_in_field = alias_table_map[table_name_in_field];

          field_pos = fill_column_to_expend_columns(
              expend_columns, column_prefix, schema_name_in_field.c_str(),
              table_name_in_field.c_str(), field_pos, hex_pos, stmt_session);
        }
        has_added = true;
      }
    }
    if (!has_added) {
      string expend_column;
      if (hex_pos.count(field_pos)) {
        expend_column.append("hex(");
        if (field->alias) {
          expend_column.append(handle_sql + field->head_pos - 1,
                               field->name_size);
          expend_column.append(")");
          expend_column.append(
              handle_sql + field->name_size + field->head_pos - 1,
              field->tail_pos - field->head_pos - field->name_size + 1);
        } else {
          expend_column.append(handle_sql + field->head_pos - 1,
                               field->tail_pos - field->head_pos + 1);
          expend_column.append(")");
        }

      } else
        expend_column.append(handle_sql + field->head_pos - 1,
                             field->tail_pos - field->head_pos + 1);
      ++field_pos;
      expend_columns.push_back(expend_column);
    }
    field = field->next;
  }
#ifdef DEBUG
  if (!expend_columns.empty()) {
    vector<string>::iterator it = expend_columns.begin();
    string tmp_str;
    tmp_str.append((*it).c_str());
    ++it;
    for (; it != expend_columns.end(); ++it) {
      tmp_str.append(",");
      tmp_str.append((*it).c_str());
    }
    LOG_DEBUG("Get expend column [%s] for Statement::expend_field_list.\n",
              tmp_str.c_str());
  }
#endif
}

void Statement::expend_field_list(insert_op_node *insert_op,
                                  vector<string> &expend_columns,
                                  const char *handle_sql) {
  record_scan *rs = insert_op->select_values;
  list<TableNameStruct> tns_list;
  table_link *tl = rs->first_table;
  if (!tl) {
    LOG_DEBUG(
        "Statement::expend_field_list skip due to no find table directly from "
        "select top rs.\n");
    return;
  }
  fulfill_tb_list(rs, tl, &tns_list);
  field_item *field = rs->field_list_head;
  expend_field_list_real(field, expend_columns, handle_sql, tns_list, rs);
}

// return true if replace DISTINCT with GROUP BY, otherwise false or throw
// exception.
bool Statement::check_and_replace_par_distinct_with_groupby(
    table_link *tb_link, const char *_query_sql) {
  record_scan *rs = tb_link->join->cur_rec_scan;
  if (!(rs->options & SQL_OPT_DISTINCT || rs->options & SQL_OPT_DISTINCTROW))
    return false;

  const char *handled_sql = NULL;
  bool handle_sql_local = false;
  if (_query_sql) {
    handled_sql = _query_sql;
    handle_sql_local = true;
  } else {
    handled_sql = sql;
  }
#ifdef DEBUG
  LOG_DEBUG(
      "sql [%s] in Statement::check_and_replace_par_distinct_with_groupby()\n",
      handled_sql);
#endif

  if (!rs->group_by_list) {
    string head_str, last_field_to_where_end_str, tail_str;
    string tmp_sql(handled_sql);

    list<TableNameStruct> tns_list;
    table_link *tl = rs->first_table;
    fulfill_tb_list(rs, tl, &tns_list);

    field_item *field = rs->field_list_head;
    string all_column_fields = string("");
    string groupby_fragment = string("");
    while (field) {
      if (field->field_expr && field->field_expr->type == EXPR_STR) {
        StrExpression *str_expr = (StrExpression *)(field->field_expr);
        if (str_expr->get_has_star_flag()) {
          string field_string_value = string(str_expr->str_value);
          size_t first_dot_position = field_string_value.find(".");
          size_t second_dot_position =
              field_string_value.find(".", first_dot_position + 1);

          if (first_dot_position == string::npos) {
            // field is '*'
            TableInfoCollection *tic = TableInfoCollection::instance();
            list<TableNameStruct>::iterator it = tns_list.begin();
            for (; it != tns_list.end(); ++it) {
              TableInfo *ti = tic->get_table_info_for_read(
                  (*it).schema_name.c_str(), (*it).table_name.c_str());
              vector<TableColumnInfo> *column_info_vec;
              try {
                column_info_vec =
                    ti->element_table_column
                        ->get_element_vector_table_column_info(stmt_session);
              } catch (...) {
                LOG_ERROR(
                    "Error occured when try to get table info column "
                    "info(vector_table_column_info) of table [%s.%s]\n",
                    (*it).schema_name.c_str(), (*it).table_name.c_str());
                ti->release_table_info_lock();
                throw;
              }

              string column_prefix;
              column_prefix.append("`");
              column_prefix.append((*it).schema_name);
              column_prefix.append("`.`");
              if (!(*it).table_alias.empty())
                column_prefix.append((*it).table_alias);
              else
                column_prefix.append((*it).table_name);
              column_prefix.append("`.");
              append_column_fields_and_groupby_fragment(
                  column_info_vec, column_prefix, all_column_fields,
                  groupby_fragment);
              ti->release_table_info_lock();
            }
          } else if (second_dot_position == string::npos) {
            // field is 't1.*'
            string table_name_in_field =
                field_string_value.substr(0, first_dot_position);
            TableInfoCollection *tic = TableInfoCollection::instance();
            TableInfo *ti = NULL;
            string column_prefix;
            list<TableNameStruct>::iterator it = tns_list.begin();
            string schema_name, table_name;
            for (; it != tns_list.end(); ++it) {
              if (!(*it).table_alias.empty()) {
                if (!strcmp(
                        it->table_alias.c_str(),
                        table_name_in_field.c_str())) {  // table alias match
                  ti = tic->get_table_info_for_read((*it).schema_name.c_str(),
                                                    (*it).table_name.c_str());
                  column_prefix.append("`");
                  column_prefix.append((*it).table_alias);
                  column_prefix.append("`.");
                  break;
                }
              } else if (!strcmp(it->table_name.c_str(),
                                 table_name_in_field
                                     .c_str())) {  // table name match
                ti = tic->get_table_info_for_read((*it).schema_name.c_str(),
                                                  (*it).table_name.c_str());
                column_prefix.append("`");
                column_prefix.append((*it).table_name);
                column_prefix.append("`.");
                break;
              }
            }
            if (it == tns_list.end()) {
              char msg[128];
              sprintf(msg, "Table name [%s] do not found in table list.",
                      table_name_in_field.c_str());
              LOG_ERROR("%s\n", msg);
              throw Error(msg);
            }

            vector<TableColumnInfo> *column_info_vec;
            try {
              column_info_vec =
                  ti->element_table_column
                      ->get_element_vector_table_column_info(stmt_session);
            } catch (...) {
              LOG_ERROR(
                  "Error occured when try to get table info column "
                  "info(vector_table_column_info) of table [%s.%s]\n",
                  (*it).schema_name.c_str(), (*it).table_name.c_str());
              ti->release_table_info_lock();
              throw;
            }

            append_column_fields_and_groupby_fragment(
                column_info_vec, column_prefix, all_column_fields,
                groupby_fragment);
            ti->release_table_info_lock();
          } else {
            // the expression is like 'test.t1.*'
            string schema_name_in_field =
                field_string_value.substr(0, first_dot_position);
            string table_name_in_field = field_string_value.substr(
                first_dot_position + 1,
                second_dot_position - first_dot_position - 1);
            string column_prefix =
                field_string_value.substr(0, second_dot_position + 1);

            map<string, string, strcasecomp> alias_table_map =
                get_alias_table_map(rs);
            if (alias_table_map.count(table_name_in_field))
              table_name_in_field = alias_table_map[table_name_in_field];
            TableInfoCollection *tic = TableInfoCollection::instance();
            TableInfo *ti = tic->get_table_info_for_read(
                schema_name_in_field.c_str(), table_name_in_field.c_str());

            vector<TableColumnInfo> *column_info_vec;
            try {
              column_info_vec =
                  ti->element_table_column
                      ->get_element_vector_table_column_info(stmt_session);
            } catch (...) {
              LOG_ERROR(
                  "Error occured when try to get table info column "
                  "info(vector_table_column_info) of table [%s.%s]\n",
                  schema_name_in_field.c_str(), table_name_in_field.c_str());
              ti->release_table_info_lock();
              throw;
            }
            append_column_fields_and_groupby_fragment(
                column_info_vec, column_prefix, all_column_fields,
                groupby_fragment);
            ti->release_table_info_lock();
          }
        } else {
          string expr_str = string(str_expr->str_value);
          size_t first_dot_pos = expr_str.find(".");
          size_t second_dot_pos = expr_str.find(".", first_dot_pos + 1);

          if (first_dot_pos == string::npos) {
            string str("`");
            str.append(expr_str);
            str.append("`");
            all_column_fields.append(str);
            groupby_fragment.append(str);

          } else if (second_dot_pos == string::npos) {
            string str("`");
            str.append(expr_str, 0, first_dot_pos);
            str.append("`.`");
            str.append(expr_str, first_dot_pos + 1,
                       expr_str.length() - first_dot_pos - 1);
            str.append("`");
            all_column_fields.append(str);
            groupby_fragment.append(str);
          } else {
            string str("`");
            str.append(expr_str, 0, first_dot_pos);
            str.append("`.`");
            str.append(expr_str, first_dot_pos + 1,
                       second_dot_pos - first_dot_pos - 1);
            str.append("`.`");
            str.append(expr_str, second_dot_pos + 1,
                       expr_str.length() - second_dot_pos - 1);
            str.append("`");
            all_column_fields.append(str);
            groupby_fragment.append(str);
          }

          if (field->alias != NULL) {
            all_column_fields.append(" AS `");
            all_column_fields.append(field->alias);
            all_column_fields.append("`");
          }
          all_column_fields.append(", ");
          groupby_fragment.append(", ");
        }
      } else {
        LOG_ERROR(
            "UnSupport partition table sql with distinct, plz check manual and "
            "use table subquery for distinct2.\n");
        throw UnSupportPartitionSQL(
            "UnSupport partition table sql with distinct, plz check manual and "
            "use table subquery for distinct.");
      }
      field = field->next;
    }
    boost::erase_tail(all_column_fields, 2);  // cut the tail ', '
    boost::erase_tail(groupby_fragment, 2);   // cut the tail ', '

    unsigned int distinct_start_pos;
    unsigned int distinct_end_pos;
    if (handle_sql_local) {
      distinct_start_pos = rs->distinct_start_pos - (rs->start_pos - 1);
      distinct_end_pos = rs->distinct_end_pos - (rs->start_pos - 1);
    } else {
      distinct_start_pos = rs->distinct_start_pos;
      distinct_end_pos = rs->distinct_end_pos;
    }

    head_str.assign(tmp_sql, 0, distinct_start_pos - 1);
    if (handle_sql_local) {
      head_str.append(tmp_sql, distinct_end_pos,
                      (rs->field_list_head->head_pos - (rs->start_pos - 1)) -
                          distinct_end_pos - 1);
      last_field_to_where_end_str.assign(
          tmp_sql, rs->field_list_tail->tail_pos - (rs->start_pos - 1),
          rs->opt_where_end_pos - rs->field_list_tail->tail_pos);
      tail_str.assign(
          tmp_sql, rs->opt_where_end_pos - (rs->start_pos - 1),
          tmp_sql.length() - (rs->opt_where_end_pos - (rs->start_pos - 1)));
    } else {
      head_str.append(tmp_sql, distinct_end_pos,
                      rs->field_list_head->head_pos - distinct_end_pos - 1);
      last_field_to_where_end_str.assign(
          tmp_sql, rs->field_list_tail->tail_pos,
          rs->opt_where_end_pos - rs->field_list_tail->tail_pos);
      tail_str.assign(tmp_sql, rs->opt_where_end_pos,
                      tmp_sql.length() - (rs->opt_where_end_pos));
    }

    no_distinct_sql.clear();
    no_distinct_sql.assign(head_str);
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
    no_distinct_sql.append(all_column_fields);
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
    no_distinct_sql.append(last_field_to_where_end_str);
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
    no_distinct_sql.append(" GROUP BY ");
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
    no_distinct_sql.append(groupby_fragment);
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
    no_distinct_sql.append(tail_str);
#ifdef DEBUG
    LOG_DEBUG("change distinct sql from [%s] to [%s]\n", handled_sql,
              no_distinct_sql.c_str());
#endif
    return true;
  } else {
    LOG_ERROR(
        "Not support to use distinct[row] with group by for partition table "
        "directly,"
        " plz check manual and use table subquery for distinct3 with options "
        "%d.\n",
        rs->options);
    throw UnSupportPartitionSQL(
        "UnSupport partition table sql with distinct,"
        " plz check manual and use table subquery for distinct.");
  }
}

DataSpace *get_prepare_sql_dataspace() {
  DataSpace *data_space;
  Backend *backend = Backend::instance();
  data_space = backend->get_metadata_data_space();
  if (!data_space) {
    data_space = backend->get_catalog();
  }

  return data_space;
}

inline Schema *get_alias_schema(
    table_link *table,
    map<table_link *, DataSpace *> &record_scan_all_table_spaces,
    const char *cur_schema) {
  DataSpace *space = NULL;
  if (record_scan_all_table_spaces.count(table))
    space = record_scan_all_table_spaces[table];

  if (!space) {
    Backend *backend = Backend::instance();
    space = backend->get_data_space_for_table(
        table->join->schema_name ? table->join->schema_name : cur_schema,
        table->join->table_name);
#ifdef DEBUG
    ACE_ASSERT(space);
#endif
  }
  if (space->get_dataspace_type() == SCHEMA_TYPE &&
      ((Schema *)space)->get_is_alias()) {
    return (Schema *)space;
  }

  return NULL;
}

inline bool is_shard_partition_table(
    table_link *table,
    map<table_link *, DataSpace *> &record_scan_all_table_spaces,
    const char *cur_schema, bool &is_tmp) {
  DataSpace *space = NULL;
  if (record_scan_all_table_spaces.count(table))
    space = record_scan_all_table_spaces[table];
  if (!space) {
    Backend *backend = Backend::instance();
    space = backend->get_data_space_for_table(
        table->join->schema_name ? table->join->schema_name : cur_schema,
        table->join->table_name);
#ifdef DEBUG
    ACE_ASSERT(space);
#endif
  }
  if (space->get_data_source()) {
    return false;
  }
  if (((Table *)space)->is_duplicated()) return false;
  PartitionedTable *part = (PartitionedTable *)space;
  if (part->get_partition_scheme()->is_shard()) {
    if (part->is_tmp_table_space()) is_tmp = true;
    return true;
  }
  return false;
}

inline void adjust_tmp_shard_table(const char *tmp_table, string &new_tb_name,
                                   unsigned virtual_machine_id,
                                   unsigned int partition_id) {
  new_tb_name.assign(tmp_table);
  new_tb_name.append("_shard_");
  char str[256];
  int len = sprintf(str, "%d", virtual_machine_id);
  str[len] = '\0';
  new_tb_name.append(str);
  len = sprintf(str, "%d", partition_id);
  str[len] = '\0';
  new_tb_name.append("_");
  new_tb_name.append(str);
  LOG_DEBUG("Adjust the shard table for tmp table %s to %s.\n", tmp_table,
            new_tb_name.c_str());
}

void adjust_shard_schema(const char *sql_schema, const char *cur_schema,
                         string &new_schema, unsigned int virtual_machine_id,
                         unsigned int partition_id) {
  const char *old_schema = NULL;
  if (sql_schema)
    old_schema = sql_schema;
  else
    old_schema = cur_schema;
  new_schema.clear();
  new_schema.append(old_schema);
  if (virtual_machine_id > 0) {
    Backend::instance()->get_shard_schema_string(virtual_machine_id,
                                                 partition_id, new_schema);
  }
}

void adjust_alias_schema(
    const char *old_sql, const char *cur_schema, stmt_node *st,
    map<table_link *, DataSpace *> &record_scan_all_table_spaces,
    string &new_sql, Schema *alias) {
#ifdef DEBUG
  ACE_ASSERT(st);
#endif
  vector<table_link *> alias_schema;
  Schema *tmp_schema = NULL;
  table_link *tmp_table = st->table_list_head;
  while (tmp_table) {
    tmp_schema =
        get_alias_schema(tmp_table, record_scan_all_table_spaces, cur_schema);
    if (tmp_schema != alias) {
      LOG_ERROR("Find a non_alias schema %s while expect alias schema %s.\n",
                tmp_schema ? tmp_schema->get_name() : "NULL",
                alias->get_name());
      throw Error("Unsupport usage for alias schema.");
    }
    alias_schema.push_back(tmp_table);
    tmp_table = tmp_table->next;
  }
  if (alias_schema.empty()) {
    new_sql.assign(old_sql);
    return;
  }
  new_sql.clear();
  string new_schema_tmp;
  const char *alias_real_name = alias->get_alias_real_name();
  vector<table_link *>::iterator it = alias_schema.begin();
  size_t start_copy_pos = 0;
  for (; it != alias_schema.end(); ++it) {
    new_sql.append(old_sql + start_copy_pos,
                   (*it)->start_pos - 1 - start_copy_pos);
    start_copy_pos = (*it)->end_pos;
    new_sql.append(alias_real_name);
    new_sql.append(".");
    new_sql.append((*it)->join->table_name);
  }
  new_sql.append(old_sql + start_copy_pos);
  LOG_DEBUG("After replace the alias schema, sql [%s] is adjusted to [%s].\n",
            old_sql, new_sql.c_str());
}

void adjust_virtual_machine_schema(
    unsigned int virtual_machine_id, unsigned int partition_id,
    const char *old_sql, const char *cur_schema, stmt_node *st,
    map<table_link *, DataSpace *> &record_scan_all_table_spaces,
    string &new_sql) {
#ifdef DEBUG
  ACE_ASSERT(virtual_machine_id);
  ACE_ASSERT(st);
#endif
  vector<table_link *> shard_tables;
  vector<table_link *> shard_tmp_tables;
  table_link *tmp_table = st->table_list_head;
  while (tmp_table) {
    bool is_tmp = false;
    if (!strcasecmp(tmp_table->join->table_name, "dual")) {
      LOG_DEBUG("SKip adjust shard schema for dual.\n");
      tmp_table = tmp_table->next;
      continue;
    }
    if (is_shard_partition_table(tmp_table, record_scan_all_table_spaces,
                                 cur_schema, is_tmp)) {
      if (is_tmp)
        shard_tmp_tables.push_back(tmp_table);
      else
        shard_tables.push_back(tmp_table);
    }
    tmp_table = tmp_table->next;
  }
  if (shard_tables.empty() && shard_tmp_tables.empty()) {
    new_sql.assign(old_sql);
    return;
  }
  new_sql.clear();
  string new_schema_tmp;
  vector<table_link *>::iterator it = shard_tables.begin();
  size_t start_copy_pos = 0;
  for (; it != shard_tables.end(); ++it) {
    new_sql.append(old_sql + start_copy_pos,
                   (*it)->start_pos - 1 - start_copy_pos);
    start_copy_pos = (*it)->end_pos;
    adjust_shard_schema((*it)->join->schema_name, cur_schema, new_schema_tmp,
                        virtual_machine_id, partition_id);
    new_sql.append("`");
    new_sql.append(new_schema_tmp.c_str());
    new_sql.append("`.`");
    new_sql.append((*it)->join->table_name);
    new_sql.append("`");
  }
  new_sql.append(old_sql + start_copy_pos);
  LOG_DEBUG(
      "After replace the shard machine schema, sql [%s] is adjusted to [%s].\n",
      old_sql, new_sql.c_str());
  if (!shard_tmp_tables.empty()) {
    string new_tb_tmp;
    vector<table_link *>::iterator it2 = shard_tmp_tables.begin();
    for (; it2 != shard_tmp_tables.end(); ++it2) {
      adjust_tmp_shard_table((*it2)->join->table_name, new_tb_tmp,
                             virtual_machine_id, partition_id);
      string old_tb((*it2)->join->table_name);
      replace_tmp_table_name(new_sql, old_tb, new_tb_tmp);
    }
    LOG_DEBUG(
        "After replace the tmp shard machine schema, sql is adjusted to "
        "[%s].\n",
        new_sql.c_str());
  }
}

inline PartitionedTable *get_part_ds_from_table(table_link *par_tb,
                                                const char *schema) {
  Backend *backend = Backend::instance();
  const char *schema_name =
      par_tb->join->schema_name ? par_tb->join->schema_name : schema;

  const char *table_name = par_tb->join->table_name;

  DataSpace *ds = backend->get_data_space_for_table(schema_name, table_name);
  return (PartitionedTable *)ds;
}

/* Whether two partition tables are using the same deplay topo*/
inline bool is_virtual_map_equal(PartitionedTable *pt1, PartitionedTable *pt2) {
  // check whether pt1 and pt2 with the virtual partition lay out originally,
  // which means the virtual_partition_id%real_parition_id=0
  if (pt1->is_virtual_map_nature() && pt2->is_virtual_map_nature()) return true;

  map<unsigned int, unsigned int> vi1 = pt1->get_virtual_map();
  map<unsigned int, unsigned int> vi2 = pt2->get_virtual_map();
  if (vi1.size() != vi2.size()) return false;
  // TODO: use a backend map to store the equal/unequal partition table space,
  // which will be deleted in the migration and insert for a new equal/unequal
  // find in issue #3412.
  map<unsigned int, unsigned int>::iterator it = vi1.begin();
  for (; it != vi1.end(); ++it) {
    if (it->second != vi2[it->first]) return false;
  }
  return true;
}
bool is_partition_table_cover(PartitionedTable *pt1, PartitionedTable *pt2) {
  if (pt1->get_key_num() != pt2->get_key_num()) return false;
  if (pt1->get_partition_type() != pt2->get_partition_type()) return false;
  if (pt1->get_partition_scheme() != pt2->get_partition_scheme()) return false;
  if (!is_virtual_map_equal(pt1, pt2)) return false;
  return true;
}

/* value could be key_name or table_name.key_name or
 * schema_name.table_name.key_name*/
bool column_name_equal_key(const char *value, const char *schema_name,
                           size_t schema_length, const char *table_name,
                           size_t table_length, const char *key_name,
                           size_t key_length) {
  const char *tmp = NULL, *tmp2 = NULL;
  size_t len = strlen(value);
  if (len == key_length + table_length + 1) {
    tmp = strchr(value, '.');
    if (!tmp) return false;
    if (strcasecmp(tmp + 1, key_name)) return false;
    if (lower_case_table_names)
      return strncasecmp(value, table_name, tmp - value) == 0;
    else
      return strncmp(value, table_name, tmp - value) == 0;
  }
  if (len == key_length + table_length + schema_length + 2) {
    tmp = strchr(value, '.');
    if (!tmp) return false;
    tmp2 = strchr(tmp + 1, '.');
    if (!tmp2) return false;
    if (strcasecmp(tmp2 + 1, key_name)) return false;
    if (lower_case_table_names) {
      if (strncasecmp(tmp + 1, table_name, tmp2 - tmp - 1)) return false;
      return strncasecmp(value, schema_name, tmp - value) == 0;
    } else {
      if (strncmp(tmp + 1, table_name, tmp2 - tmp - 1)) return false;
      return strncmp(value, schema_name, tmp - value) == 0;
    }
  }
  return false;
}

inline bool column_name_equal_key_alias_without_len(const char *value,
                                                    const char *schema_name,
                                                    const char *table_name,
                                                    const char *key_name,
                                                    const char *alias_name) {
  bool ret = false;
  ret = (strcasecmp(value, key_name) == 0);
  if (ret) return true;
  size_t schema_length = strlen(schema_name);
  if (!schema_length) return false;

  size_t key_length = strlen(key_name);
  size_t table_length = strlen(table_name);
  if (alias_name)  // if table has alias, we only can use alias to find the
                   // equality
    ret = column_name_equal_key(value, schema_name, schema_length, alias_name,
                                strlen(alias_name), key_name, key_length);
  else {
    ret = column_name_equal_key(value, schema_name, schema_length, table_name,
                                table_length, key_name, key_length);
  }
  return ret;
}

inline bool column_name_equal_key_alias(
    const char *value, const char *schema_name, size_t schema_length,
    const char *table_name, size_t table_length, const char *key_name,
    const char *alias_name) {
  bool ret = false;
  ret = (strcasecmp(value, key_name) == 0);
  if (ret) return true;
  if (!schema_length) return false;
  size_t key_length = strlen(key_name);
  if (alias_name)  // if table has alias, we only can use alias to find the
                   // equality
    ret = column_name_equal_key(value, schema_name, schema_length, alias_name,
                                strlen(alias_name), key_name, key_length);
  else {
    ret = column_name_equal_key(value, schema_name, schema_length, table_name,
                                table_length, key_name, key_length);
  }
  return ret;
}

/* check whether this str_expr is possible belonging to a (sub)condition of
 * a "where" or "having".*/
condition_type check_condition_type(StrExpression *expr) {
  if (!expr->parent) return CONDITION_TYPE_NON;

  expr_type type = expr->parent->type;
  switch (type) {
    case EXPR_LIKE:
    case EXPR_NOTLIKE:
    case EXPR_IN:
    case EXPR_NOT_IN:
    case EXPR_IS:
    case EXPR_IS_NOT:
    case EXPR_REGEXP:
    case EXPR_NOTREGEXP:
      return CONDITION_TYPE_RANDOM;
    case EXPR_EQ:
      return CONDITION_TYPE_EQUAL;
    case EXPR_GR:
    case EXPR_LESS:
    case EXPR_GE:
    case EXPR_LESSE:
    case EXPR_NE:
      return CONDITION_TYPE_RANGE;
    default:
      return CONDITION_TYPE_NON;
  }
  return CONDITION_TYPE_NON;
}

RangeType check_range_type(StrExpression *expr, bool is_left) {
  expr_type type = expr->parent->type;
  switch (type) {
    case EXPR_GR:
      if (!is_left) return RANGE_TYPE_LT;
      return RANGE_TYPE_GT;
    case EXPR_LESS:
      if (!is_left) return RANGE_TYPE_GT;
      return RANGE_TYPE_LT;
    case EXPR_GE:
      if (!is_left) return RANGE_TYPE_LE;
      return RANGE_TYPE_GE;
    case EXPR_LESSE:
      if (!is_left) return RANGE_TYPE_GE;
      return RANGE_TYPE_LE;
    default:
      return RANGE_TYPE_EQ;
  }
}

inline const char *Statement::get_str_from_simple_value(Expression *expr) {
  if (expr->type == EXPR_INT) return ((IntExpression *)expr)->str_value;
  if (expr->type == EXPR_STRING) return ((StrExpression *)expr)->str_value;
  if (expr->type == EXPR_FLOAT) return ((FloatExpression *)expr)->str_value;
  if (expr->type == EXPR_BOOL) return ((BoolExpression *)expr)->str_value;
  return NULL;
}

inline const char *Statement::get_gmp_from_simple_value(Expression *expr) {
  if (expr->type == EXPR_INT) return ((IntExpression *)expr)->str_value;
  if (expr->type == EXPR_FLOAT) return ((FloatExpression *)expr)->str_value;
  if (expr->type == EXPR_BOOL) return ((BoolExpression *)expr)->str_value;

  try {
    if (expr->is_null()) return NULL;
  } catch (...) {
    return NULL;
  }

#ifdef DEBUG
  LOG_DEBUG("expr type is %d\n", expr->type);
#endif
  if (expr->type == EXPR_STR) return NULL;
  GMP_Expression gmp_expr;
  mpf_init2(gmp_expr.value, DECIMAL_STORE_BIT);
  try {
    expr->get_gmp_value(&gmp_expr);
  } catch (...) {
    mpf_clear(gmp_expr.value);
    return NULL;
  }

  char *expr_str = new char[GMP_N_DIGITS + 5];
  gmp_sprintf(expr_str, "%.Ff", gmp_expr.value);
  mpf_clear(gmp_expr.value);
  simple_expression.push_back(expr_str);
  need_clean_simple_expression = true;
#ifdef DEBUG
  LOG_DEBUG("gmp value = [%s]\n", expr_str);
#endif
  return (const char *)expr_str;
}

inline const char *Statement::expr_is_simple_value(Expression *expr) {
  if (expr->type == EXPR_INT) return ((IntExpression *)expr)->str_value;
  if (expr->type == EXPR_STRING) return ((StrExpression *)expr)->str_value;
  if (expr->type == EXPR_FLOAT) return ((FloatExpression *)expr)->str_value;
  if (expr->type == EXPR_BOOL) return ((BoolExpression *)expr)->str_value;

  try {
    if (expr->is_null()) {
      if (expr->type == EXPR_NULL) {
        // use 0 rather than NULL, in case the partition method is mod
        return null_key_str.c_str();
      }
      return NULL;
    }
  } catch (...) {
    return NULL;
  }

#ifdef DEBUG
  LOG_DEBUG("expr type is %d\n", expr->type);
#endif
  if (expr->type == EXPR_STR) return NULL;
  GMP_Expression gmp_expr;
  mpf_init2(gmp_expr.value, DECIMAL_STORE_BIT);
  try {
    expr->get_gmp_value(&gmp_expr);
  } catch (...) {
    mpf_clear(gmp_expr.value);
    return NULL;
  }
  char *expr_str = new char[GMP_N_DIGITS + 5];
  gmp_sprintf(expr_str, "%.Ff", gmp_expr.value);
  mpf_clear(gmp_expr.value);
  simple_expression.push_back(expr_str);
  need_clean_simple_expression = true;
#ifdef DEBUG
  LOG_DEBUG("gmp value = [%s]\n", expr_str);
#endif
  return (const char *)expr_str;
}

inline Expression *get_peer(Expression *expr) {
  CompareExpression *cmp_expr = (CompareExpression *)expr->parent;
  const Expression *peer = NULL;
  peer = cmp_expr->left == expr ? cmp_expr->right : cmp_expr->left;

  return (Expression *)peer;
}

inline Expression *get_bool_binary_peer(Expression *expr) {
  BoolBinaryExpression *cmp_expr = (CompareExpression *)expr->parent;
  const Expression *peer = NULL;
  peer = cmp_expr->left == expr ? cmp_expr->right : cmp_expr->left;

  return (Expression *)peer;
}

inline pair<Expression *, bool> get_range_peer(Expression *expr) {
  CompareExpression *cmp_expr = (CompareExpression *)expr->parent;
  Expression *peer = cmp_expr->left;
  bool is_left;
  if (peer == expr) {
    peer = cmp_expr->right;
    is_left = true;
  } else {
    peer = cmp_expr->left;
    is_left = false;
  }

  pair<Expression *, bool> peer_pair(peer, is_left);
  return peer_pair;
}

inline bool is_column(Expression *expr) {
  /* TODO: EXPR_STR contains not only column name, but also routine name, and
   * other special names. Fix it to identify the column name.*/
  if (expr->type != EXPR_STR && expr->type != EXPR_UNIT) return false;
  return true;
}

inline bool expr_equal_alias(Expression *expr, const char *alias) {
  if (expr->type == EXPR_STR && alias != NULL) {
    string str;
    const char *col_pos;
    expr->to_string(str);
    col_pos = str.c_str();

    if (strcasecmp(col_pos, alias) == 0) {
      return true;
    }
  }

  return false;
}

inline void split_column_expr(string value, string &schema_name,
                              string &table_name, string &column_name) {
  unsigned int first_dot_position = value.find(".");
  unsigned int second_dot_position = value.find(".", first_dot_position + 1);
  if (first_dot_position == (unsigned)string::npos) {
    column_name = value;
  } else if (second_dot_position == (unsigned)string::npos) {
    table_name = value.substr(0, first_dot_position);
    column_name = value.substr(first_dot_position + 1);
  } else {
    schema_name = value.substr(0, first_dot_position);
    table_name = value.substr(first_dot_position + 1,
                              second_dot_position - first_dot_position - 1);
    column_name = value.substr(second_dot_position + 1);
  }
}

inline bool expr_equal_column(Expression *expr1, Expression *expr2) {
  if (expr1->type == EXPR_STR && expr2->type == EXPR_STR) {
    string value1, schema1, table1, column1;
    string value2, schema2, table2, column2;
    expr1->to_string(value1);
    expr2->to_string(value2);
    split_column_expr(value1, schema1, table1, column1);
    split_column_expr(value2, schema2, table2, column2);
    if (!schema1.empty() && !schema2.empty()) {
      if (lower_case_table_names &&
          strcasecmp(schema1.c_str(), schema2.c_str()))
        return false;
      if (!lower_case_table_names && strcmp(schema1.c_str(), schema2.c_str()))
        return false;
    }
    if (!table1.empty() && !table2.empty()) {
      if (lower_case_table_names && strcasecmp(table1.c_str(), table2.c_str()))
        return false;
      if (!lower_case_table_names && strcmp(table1.c_str(), table2.c_str()))
        return false;
    }
    if (!strcasecmp(column1.c_str(), column2.c_str())) return true;
  }
  return false;
}

int item_in_select_fields_pos(Expression *expr, record_scan *rs) {
  field_item *field = rs->field_list_head;
  int column_index = 0;

  while (field) {
    if (field->field_expr && (field->field_expr->is_equal(expr) ||
                              expr_equal_alias(expr, field->alias) ||
                              expr_equal_column(field->field_expr, expr))) {
      return column_index;
    }
    ++column_index;
    field = field->next;
  }

  return COLUMN_INDEX_UNDEF;
}

inline bool cmp_peer_is_column(Expression *expr) {
  return is_column(get_peer(expr));
}

ConditionAndOr is_and_or_condition(Expression *cond, record_scan *rs) {
  Expression *condition = rs->condition;

  Expression *tmp = cond;

  ConditionAndOr ret = CONDITION_AND;

  while (tmp->parent) {
    tmp = tmp->parent;
    if (tmp->type == EXPR_OR)
      ret = CONDITION_OR;
    else if (tmp->type != EXPR_AND) {
      /*The type should be EXPR_AND or EXPR_OR, otherwise the query result may
       * be wrong.
       *
       * For example:
       *
       *  select * from t1 where ((c5 =1) and (c6=1)) =0;
       *
       *  We should not use c5 or c6 for and_condition*/
      ret = CONDITION_NO_COND;
      return ret;
    }
  }

  if (tmp != condition && !tmp->is_join_condition) ret = CONDITION_NO_COND;

  return ret;
}

bool is_not_condition(Expression *cond) {
  Expression *tmp = cond;

  while (tmp->parent) {
    tmp = tmp->parent;
    if (tmp->type == EXPR_NOT) return true;
  }

  return false;
}

inline bool is_acceptable_join_par_table(join_node *join) {
  join_node *parent = join->upper;
  if (!parent) return true;
  if ((parent->join_bit & JT_RIGHT) && parent->right != join) return false;
  return is_acceptable_join_par_table(parent);
}

/*This function only check current record scan and its parent, and do not
 * consider the parent of parent record_scan.*/
bool is_subquery_in_select_list(record_scan *rs) {
  if (rs->subquerytype == SUB_SELECT_ONE_VALUE && rs->condition_belong) {
    Expression *tmp = rs->condition_belong;
    while (tmp->parent) tmp = tmp->parent;
    if (tmp == rs->upper->condition || tmp == rs->upper->having) return false;
    return true;
  }
  return false;
}

inline DataServer *get_write_server_of_source(DataSource *source) {
  DataServer *ret = NULL;
  if (source->is_load_balance()) {
    LoadBalanceDataSource *lb = (LoadBalanceDataSource *)source;
    ret = get_write_server_of_source(lb->get_data_sources()[0]);
  } else {
    ret = source->get_master_server();
  }
  LOG_DEBUG("Get dataserver [%s%@] from source [%s%@].\n", ret->get_name(), ret,
            source->get_name(), source);
  return ret;
}

bool is_simple_expression(Expression *expr) {
  if (expr->type == EXPR_INT || expr->type == EXPR_STRING ||
      expr->type == EXPR_FLOAT || expr->type == EXPR_BOOL)
    return true;
  return false;
}

const char *get_simple_expression_value(Expression *expr) {
  if (expr->type == EXPR_INT) return ((IntExpression *)expr)->str_value;
  if (expr->type == EXPR_STRING) return ((StrExpression *)expr)->str_value;
  if (expr->type == EXPR_FLOAT) return ((FloatExpression *)expr)->str_value;
  if (expr->type == EXPR_BOOL) return ((BoolExpression *)expr)->str_value;
  return NULL;
}

void trace_condition_find_or_expression(Expression *expr,
                                        vector<Expression *> *or_expr) {
  if (expr->type == EXPR_AND) {
    BoolBinaryCaculateExpression *and_expr =
        (BoolBinaryCaculateExpression *)expr;
    trace_condition_find_or_expression(and_expr->left, or_expr);
    trace_condition_find_or_expression(and_expr->right, or_expr);
  } else if (expr->type == EXPR_OR) {
    or_expr->push_back(expr);
  }
}

vector<Expression *> find_top_or_expression(Expression *expr) {
  vector<Expression *> ret;
  trace_condition_find_or_expression(expr, &ret);
  return ret;
}

bool check_need_condition_or_equality(Expression *expr, const char *schema_name,
                                      const char *table_name,
                                      const char *key_name,
                                      const char *alias_name,
                                      vector<const char *> *key_value_vector) {
  if (!expr) return false;
  if (expr->type == EXPR_OR) {
    BoolBinaryCaculateExpression *or_expr =
        (BoolBinaryCaculateExpression *)expr;
    bool ret1 = check_need_condition_or_equality(or_expr->left, schema_name,
                                                 table_name, key_name,
                                                 alias_name, key_value_vector);
    bool ret2 = check_need_condition_or_equality(or_expr->right, schema_name,
                                                 table_name, key_name,
                                                 alias_name, key_value_vector);
    return ret1 && ret2;
  } else if (expr->type == EXPR_EQ) {
    CompareExpression *eq_expr = (CompareExpression *)expr;
    if (eq_expr->left->type == EXPR_STR &&
        is_simple_expression(eq_expr->right)) {
      StrExpression *str_expr = (StrExpression *)eq_expr->left;
      if (column_name_equal_key_alias_without_len(str_expr->str_value,
                                                  schema_name, table_name,
                                                  key_name, alias_name)) {
        const char *right_value = get_simple_expression_value(eq_expr->right);
        key_value_vector->push_back(right_value);
        return true;
      }
    }
    if (is_simple_expression(eq_expr->left) &&
        eq_expr->right->type == EXPR_STR) {
      StrExpression *str_expr = (StrExpression *)eq_expr->right;
      if (column_name_equal_key_alias_without_len(str_expr->str_value,
                                                  schema_name, table_name,
                                                  key_name, alias_name)) {
        const char *left_value = get_simple_expression_value(eq_expr->left);
        key_value_vector->push_back(left_value);
        return true;
      }
    }
  }
  return false;
}

vector<const char *> get_or_condition_key_values(Expression *expr,
                                                 const char *schema_name,
                                                 const char *table_name,
                                                 const char *key_name,
                                                 const char *alias_name) {
  vector<const char *> key_value_vector;
  if (!expr) return key_value_vector;
  vector<Expression *> or_exprs = find_top_or_expression(expr);
  vector<Expression *>::iterator it = or_exprs.begin();
  for (; it != or_exprs.end(); ++it) {
    bool can_find_equality = check_need_condition_or_equality(
        *it, schema_name, table_name, key_name, alias_name, &key_value_vector);
    if (can_find_equality && !key_value_vector.empty()) {
      return key_value_vector;
    }
  }

  key_value_vector.clear();
  return key_value_vector;
}

void Statement::check_column_name(set<string> *valid_columns,
                                  set<string> &curr_column_names,
                                  const char *column_name,
                                  const char *full_table_name) {
  string s(column_name);
  boost::to_lower(s);
  if (!valid_columns->count(s)) {
    char msg[512];
    snprintf(msg, sizeof(msg),
             "Unknown column '%s' in 'field list' of table %s", column_name,
             full_table_name);
    LOG_ERROR("%s\n", msg);
    throw dbscale::sql::SQLError(msg, "42S22", ERROR_UNKNOWN_COLUMN_CODE);
  }
  if (!curr_column_names.count(s)) {
    curr_column_names.insert(s);
  } else {
    char msg[512];
    snprintf(msg, sizeof(msg), "Column '%s' specified twice of table %s",
             column_name, full_table_name);
    LOG_ERROR("%s\n", msg);
    throw dbscale::sql::SQLError(msg, "42000", ERROR_DUPLICATE_COLUMN_CODE);
  }
}

void Statement::check_column_count_name(PartitionedTable *part_space,
                                        name_item *list, int &column_count) {
  column_count = 0;
  name_item *head = list;
  name_item *tmp = head;
  set<string> curr_column_names;

  ACE_RW_Mutex *tc_lock = NULL;
  try {
    tc_lock = part_space->get_table_columns_lock(full_table_name);
    tc_lock->acquire_read();
    set<string> *valid_columns = part_space->get_table_columns(full_table_name);
    do {
      check_column_name(valid_columns, curr_column_names, tmp->name,
                        full_table_name.c_str());
      tmp = tmp->next;
    } while (tmp != head);
    tc_lock->release();
  } catch (...) {
    tc_lock->release();
    throw;
  }
  column_count = curr_column_names.size();
}

void Statement::check_column_count_name(PartitionedTable *part_space,
                                        Expression *assign_list) {
  set<string> curr_column_names;
  ListExpression *expr_list = (ListExpression *)assign_list;
  expr_list_item *head = expr_list->expr_list_head;
  expr_list_item *tmp = head;
  CompareExpression *cmp_tmp = NULL;
  StrExpression *str_tmp = NULL;
  ACE_RW_Mutex *tc_lock = NULL;

  try {
    tc_lock = part_space->get_table_columns_lock(full_table_name);
    tc_lock->acquire_read();

    set<string> *valid_columns = part_space->get_table_columns(full_table_name);
    do {
      cmp_tmp = (CompareExpression *)tmp->expr;
      str_tmp = (StrExpression *)cmp_tmp->left;
      check_column_name(valid_columns, curr_column_names, str_tmp->str_value,
                        full_table_name.c_str());
      tmp = tmp->next;
    } while (tmp != head);
    tc_lock->release();
  } catch (...) {
    tc_lock->release();
    throw;
  }
}

void release_avg_list(list<AvgDesc> &avg_list);
Statement::~Statement() {
  map<record_scan *, map<const char *, RangeValues *> >::iterator it_rs;
  for (it_rs = record_scan_par_key_ranges.begin();
       it_rs != record_scan_par_key_ranges.end(); ++it_rs) {
    map<const char *, RangeValues *> range_map = it_rs->second;
    map<const char *, RangeValues *>::iterator it;
    for (it = range_map.begin(); it != range_map.end(); ++it) {
      delete it->second;
    }
    record_scan_par_key_ranges[it_rs->first].clear();
  }

  FieldExpression *field_expr;
  while (!new_field_expressions.empty()) {
    field_expr = new_field_expressions.front();
    new_field_expressions.pop_front();
    delete field_expr;
  }

  while (!re_parser_stmt_list.empty()) {
    Statement *tmp = re_parser_stmt_list.front();
    tmp->free_resource();
    delete tmp;
    re_parser_stmt_list.pop_front();
  }

  stmt_dynamic_add_str_cleanup();
  clear_simple_expression();

  while (!union_all_stmts.empty()) {
    union_all_stmts.front()->free_resource();
    delete union_all_stmts.front();
    union_all_stmts.front() = NULL;
    union_all_stmts.pop_front();
  }
  vector<SeparatedExecNode *>::iterator it = exec_nodes.begin();
  for (; it != exec_nodes.end(); ++it) {
    (*it)->clean_node();
    delete (*it);
  }
  exec_nodes.clear();
  map<record_scan *, vector<vector<TableStruct *> *> *>::iterator it_table =
      record_scan_join_tables.begin();
  for (; it_table != record_scan_join_tables.end(); ++it_table) {
    vector<vector<TableStruct *> *>::iterator it_table_vector =
        it_table->second->begin();
    for (; it_table_vector != it_table->second->end(); ++it_table_vector) {
      vector<TableStruct *>::iterator it_table_struct =
          (*it_table_vector)->begin();
      for (; it_table_struct != (*it_table_vector)->end(); ++it_table_struct) {
        delete (*it_table_struct);
      }
      delete (*it_table_vector);
    }
    delete it_table->second;
  }
  clear_simple_expression();
  release_avg_list(avg_list);
}
void Statement::clean_separated_node() {
  vector<SeparatedExecNode *>::iterator it = exec_nodes.begin();
  for (; it != exec_nodes.end(); ++it) {
    (*it)->clean_node();
    delete (*it);
  }
  exec_nodes.clear();
  need_clean_exec_nodes = false;
  record_scan_exec_node_map.clear();
}

/* Whether the given partition table's key is equal to any of the equality of
 * the stored key of related record_scan.*/
bool Statement::is_all_par_table_equal(record_scan *rs, table_link *table,
                                       const char *kept_key,
                                       const char *key_name) {
  const char *schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  const char *table_name = table->join->table_name;
  const char *table_alias = table->join->alias;

  vector<string>::iterator it;
  vector<string> *vec = &record_scan_par_key_equality[rs][kept_key];
  it = vec->begin();
  /*skip the first, which is the key itself and not the equality.  otherwise
   * if the two partition tables' key name is the same, such as c1. dbscale
   * will always consider these two partition tables can be merged.
   */
  if (it != vec->end()) ++it;
  for (; it != vec->end(); ++it) {
    if (column_name_equal_key_alias((*it).c_str(), schema_name,
                                    strlen(schema_name), table_name,
                                    strlen(table_name), key_name, table_alias))
      return true;
  }

  return false;
}

bool Statement::is_CNJ_all_par_table_equal(record_scan *rs, table_link *table1,
                                           table_link *table2,
                                           const char *kept_key,
                                           const char *key_name) {
  const char *schema_name =
      table2->join->schema_name ? table2->join->schema_name : schema;
  const char *table_name = table2->join->table_name;
  const char *table_alias = table2->join->alias;

  const char *schema_name_tmp =
      table1->join->schema_name ? table1->join->schema_name : schema;
  const char *table_name_tmp = table1->join->table_name;
  const char *table_alias_tmp = table1->join->alias;
  string full_column(schema_name_tmp);
  full_column.append(".");
  if (table_alias_tmp)
    full_column.append(table_alias_tmp);
  else
    full_column.append(table_name_tmp);
  full_column.append(".");
  full_column.append(kept_key);

  vector<string>::iterator it;
  vector<string> *vec =
      &record_scan_par_key_equality_full_table[rs][full_column];
  it = vec->begin();
  /*skip the first, which is the key itself and not the equality.  otherwise
   * if the two partition tables' key name is the same, such as c1. dbscale
   * will always consider these two partition tables can be merged.
   */

  if (it != vec->end()) ++it;
  for (; it != vec->end(); ++it) {
    if (column_name_equal_key_alias((*it).c_str(), schema_name,
                                    strlen(schema_name), table_name,
                                    strlen(table_name), key_name, table_alias))
      return true;
  }

  return false;
}

/* get the possible partitions to execute sql.
 *
 * Return value: false, only need some partions, which is stored in par_id.
 *               true, need all partitions.*/
bool Statement::get_partitions_all_par_tables(record_scan *rs,
                                              PartitionMethod *method,
                                              vector<unsigned int> *par_id) {
  /*TODO: some partition method may support range condition, so we need to add
   * more interface to PartitionMethod, and let PartitionMethod to decide how
   * to handle.*/
  table_link *par_tb = one_table_node.only_one_table
                           ? one_table_node.table
                           : record_scan_par_table_map[rs];
  DataSpace *tmp_space = one_table_node.only_one_table
                             ? one_table_node.space
                             : (record_scan_all_table_spaces.count(par_tb)
                                    ? record_scan_all_table_spaces[par_tb]
                                    : NULL);
  if (!tmp_space || !tmp_space->is_partitioned()) {
    LOG_ERROR("Got error when get partitions for table.\n");
    throw Error("Not support sql, fail to get table partitions.");
  }
  PartitionedTable *par_table = (PartitionedTable *)tmp_space;
  vector<const char *> *key_names = par_table->get_key_names();

  if (method->get_type() == PART_HASH && hash_method_cs && key_names &&
      !key_names->empty()) {
    const char *schema_name =
        par_tb->join->schema_name ? par_tb->join->schema_name : schema;
    const char *table_name = par_tb->join->table_name;
    bool is_column_collation_cs_or_bin = false;
    bool is_data_type_string_like = true;
    get_column_collation_using_schema_table(
        schema_name, table_name, key_names->at(0),
        is_column_collation_cs_or_bin, is_data_type_string_like);
    if (is_data_type_string_like && !is_column_collation_cs_or_bin) return true;
  }

  try {
    string replace_null_char = stmt_session->get_query_sql_replace_null_char();
    method->fullfil_part_ids(this, rs, key_names, par_id, replace_null_char);
  } catch (...) {
    clear_simple_expression();
    throw;
  }

  if (rs->need_range && record_scan_par_key_ranges.count(rs)) {
    map<const char *, RangeValues *> range_map = record_scan_par_key_ranges[rs];
    map<const char *, RangeValues *>::iterator it;
    for (it = range_map.begin(); it != range_map.end(); ++it) {
      delete it->second;
    }
    record_scan_par_key_ranges[rs].clear();
  }
  if (need_clean_simple_expression) clear_simple_expression();
  if (par_id->empty()) return true;

  return false;
}

void Statement::clear_simple_expression() {
  vector<char *>::iterator sit;
  LOG_DEBUG("clear_simple_expression\n");
  for (sit = simple_expression.begin(); sit != simple_expression.end(); ++sit) {
    char *tmp = *sit;
    delete[] tmp;
  }
  simple_expression.clear();
  need_clean_simple_expression = false;
}

void Statement::get_partitons_one_record_scan(record_scan *rs,
                                              PartitionMethod *method,
                                              unsigned int partition_num,
                                              vector<unsigned int> *par_ids) {
  bool need_all_partition = true;
  need_all_partition = get_partitions_all_par_tables(rs, method, par_ids);
  if (need_all_partition) {
    par_ids->clear();
    unsigned int i = 0;
    for (; i < partition_num; ++i) par_ids->push_back(i);
    return;
  }
  unsigned int par_ids_size = par_ids->size();
  if (par_ids_size == 0 || par_ids_size > partition_num) {
    LOG_ERROR(
        "Got abnormal number of parittion ids [%d] with"
        " partition_num [%d], the sql is [%s].\n",
        par_ids_size, partition_num, sql);
    throw UnSupportPartitionSQL("Got abnormal number of parittion ids");
  }
}

// split one Str-value into schema, table, and column.
void split_value_into_schema_table_column(string value, string &schema_name,
                                          string &table_name,
                                          string &column_name) {
  vector<string> split_name;
  boost::split(split_name, value, boost::is_any_of("."));
  if (split_name.size() == 1) {
    column_name = split_name.at(0);
  } else if (split_name.size() == 2) {
    table_name = split_name.at(0);
    column_name = split_name.at(1);
  } else {
    schema_name = split_name.at(0);
    table_name = split_name.at(1);
    column_name = split_name.at(2);
  }
}

int get_pos_from_field_list(Expression *expr, record_scan *rs) {
  int position = 0;
  string group_schema;
  string group_table;
  string group_column;
  string local_group_str;
  expr->to_string(local_group_str);
  split_value_into_schema_table_column(local_group_str, group_schema,
                                       group_table, group_column);

  field_item *fi = rs->field_list_head;
  if (fi) {
    do {
      string local_field_str;
      if (fi->field_expr && fi->field_expr->type == EXPR_STR) {
        fi->field_expr->to_string(local_field_str);
        string schema_name;
        string table_name;
        string column_name;
        split_value_into_schema_table_column(local_field_str, schema_name,
                                             table_name, column_name);
        if (!strcasecmp(column_name.c_str(), group_column.c_str()) &&
            !fi->alias) {
          break;
        }
      }
      ++position;
      fi = fi->next;
    } while (fi && fi != rs->field_list_tail);
  }
  LOG_DEBUG("%s pos is %d.\n", local_group_str.c_str(), position);

  return position;
}

string construct_string_seperated_with_comma(set<string> string_set) {
  string ret;
  bool need_comma = false;
  set<string>::iterator it = string_set.begin();
  for (; it != string_set.end(); ++it) {
    if (need_comma) ret.append(", ");
    need_comma = true;
    ret.append(*it);
  }
  return ret;
}

void adjust_key_according_type(set<string> &index_set,
                               map<int, string> &column_type_map) {
  /*the string in column_type_map is like `xmmc` varchar(1100). the string in
   * index_set is like `xmmc`*/
  set<string> ret_set;
  set<string>::iterator it = index_set.begin();
  for (; it != index_set.end(); ++it) {
    if (ret_set.size() > MAX_INDEX_TMP_COLUMNS) break;
    map<int, string>::iterator it2 = column_type_map.begin();
    bool has_insert_key = false;
    for (; it2 != column_type_map.end(); ++it2) {
      if (ret_set.size() > MAX_INDEX_TMP_COLUMNS) break;
      string c_type = it2->second;
      string k_column = *it;
      // k_column.append(" ");
      size_t pos = c_type.find(k_column.c_str());
      if (pos != std::string::npos) {
        if (c_type.find("blob", pos + k_column.length()) != std::string::npos ||
            c_type.find("text", pos + k_column.length()) != std::string::npos) {
          string tmp = *it;
          tmp.append("(50)");
          ret_set.insert(tmp);
          has_insert_key = true;
          break;
        }

        size_t pos1 = c_type.find("varchar", pos + k_column.length());
        if (pos1 == std::string::npos)
          pos1 = c_type.find("char", pos + k_column.length());
        if (pos1 != std::string::npos) {
          size_t start_pos = c_type.find("(", pos1);
          if (start_pos != std::string::npos) {
            size_t end_pos = c_type.find(")", start_pos);
            if (end_pos != std::string::npos &&
                end_pos - start_pos >
                    3) {  // larger 3 means the length number of char/varchar
                          // inside () is larger than 99
              string tmp = *it;
              tmp.append("(50)");
              ret_set.insert(tmp);
              has_insert_key = true;
              break;
            }
          }
        }
      }
    }
    if (!has_insert_key) ret_set.insert(*it);
  }
  index_set = ret_set;
}

/*
 * DBScale 跨节点临时表索引实现的总体设计：

   主要针对2种跨节点场景的临时表添加索引：1.
 子查询产生的临时表；2.rs内跨节点挪动表产生的临时表

   1.
 对于子查询，代码入口为ReconstructSQL::local_execute_query_to_create中的original_stmt->get_table_index_for_rs(original_rs->upper,
 default_schema, table_alias, column_vector, tmp_string_map)
   可以看出，为子查询的临时表添加索引是基于该子查询在父rs中的alias衍生表的查询条件或查询列进行选择和添加的。

   选取索引列和添加索引类型的代码实现为Statement::get_table_index_for_rs，大致逻辑如下：
      a. 找到可能的index的列，range，等值，简单等值
      b. 将他们添加进index
      c. 我们会生成两种index，一种代表range，另一种代表等值。
      d. range index = column_vector+range_index
      e. 如果 range index 包含 equality index，忽略 equality_index
      f. equality index = column_vector+equality_index

   2.
 对于rs内跨节点挪动表产生的临时表，主要是在cross_join_utils.cc的DataMoveJoinInfo::handle_equal_columns中进行索引列的选取，这里主要基于等值条件列进行筛选。

   另外添加索引有一些限制情况需要考虑:
   1.
 索引总长度，目前代码实现上一个索引的总长度限制在1000以里，最多10个字段，每个字段最大99的长度。
   2. 对于blob和text和varchar/char类型，需要考虑使用前缀索引。
   对于子查询的索引，上述限制的实现参考 Statement::adjust_key_according_type
   对于rs内跨节点挪动表产生的临时表，上述限制的实现参考
 DataMoveJoinInfo::handle_equal_columns中搜索MAX_INDEX_TMP_COLUMNS

   索引信息最终拼接到临时表上的代码实现位于construct_sql.cc的ConstructMethod::generate_create_column_related_string中。
 * */

/* get table index for an record_scan
   The returned sql is the index we recommanded.
   1. find the candidates column of index, range to range_index(col<2),
   simple equal to column_vector(col=2), multiple equality to
   equality_index(tt.col=t.c1)
   2. construct range_index, column_vector, equality_index into index string.
   we construct two indexes, one is for range, one is for equality
   range index = column_vector+range_index.
   if range index contains equality_index, ignore equality_index.
   equality index = column_vector+equality_index.
*/
string Statement::get_table_index_for_rs(record_scan *rs,
                                         const char *table_schema,
                                         const char *table_alias,
                                         set<string> column_vector,
                                         map<int, string> &column_type_map) {
  expr_list_item *str_list_head = rs->str_expr_list_head;
  expr_list_item *tmp = str_list_head;
  StrExpression *str_expr = NULL;
  condition_type cond_type = CONDITION_TYPE_NON;

  set<string> index_set;
  set<string> range_index;
  set<string> equality_index;
  string index;
  if (!tmp) return index;

  // 1. find the candidates column of index
  do {
    str_expr = (StrExpression *)tmp->expr;
    cond_type = check_condition_type(str_expr);
    if (cond_type == CONDITION_TYPE_NON || cond_type == CONDITION_TYPE_RANDOM) {
      tmp = tmp->next;
      continue;
    }
    ConditionAndOr and_or_type = is_and_or_condition(str_expr->parent, rs);
    if (and_or_type != CONDITION_AND) {
      tmp = tmp->next;
      continue;
    }

    bool can_be_key = false;
    string schema_name;
    string table_name;
    string column_name;
    split_value_into_schema_table_column(str_expr->str_value, schema_name,
                                         table_name, column_name);
    // check the column name is in column vector or not
    if (!column_vector.count(column_name)) {
      tmp = tmp->next;
      continue;
    }

    if (schema_name.empty() && table_name.empty()) {
      if (column_vector.count(column_name)) can_be_key = true;
    } else if (schema_name.empty()) {
      if (!strcasecmp(table_schema, schema) &&
          !strcasecmp(table_name.c_str(), table_alias))
        can_be_key = true;
    } else {
      if (!strcasecmp(schema_name.c_str(), table_schema) &&
          !strcasecmp(table_name.c_str(), table_alias))
        can_be_key = true;
    }

    if (!can_be_key) {
      tmp = tmp->next;
      continue;
    }

    // Add ` for column
    string tmp_column_name = "`";
    tmp_column_name.append(column_name);
    tmp_column_name.append("`");
    column_name = tmp_column_name;

    Expression *peer_expr = get_peer(str_expr);
    bool peer_is_simple_value = expr_is_simple_value(peer_expr);
    if (cond_type == CONDITION_TYPE_EQUAL && peer_is_simple_value) {
      index_set.insert(column_name);
      if (equality_index.count(column_name)) equality_index.erase(column_name);
      if (range_index.count(column_name)) range_index.erase(column_name);
    }

    if (cond_type == CONDITION_TYPE_EQUAL && !index_set.count(column_name) &&
        !peer_is_simple_value) {
      equality_index.insert(column_name);
    }

    if (cond_type == CONDITION_TYPE_RANGE && !index_set.count(column_name) &&
        peer_is_simple_value) {
      range_index.insert(column_name);
    }
    tmp = tmp->next;
  } while (tmp != str_list_head);

  adjust_key_according_type(index_set, column_type_map);
  adjust_key_according_type(equality_index, column_type_map);
  adjust_key_according_type(range_index, column_type_map);

  // 2. construct range_index, column_vector, equality_index into index string.

  if (range_index.empty() && equality_index.empty() && !index_set.empty()) {
    index.append(" KEY `ind_where` (");
    string index_set_string = construct_string_seperated_with_comma(index_set);
    index.append(index_set_string);
    index.append(")");
  }

  if (!range_index.empty()) {
    index.append(" KEY `ind_where` (");

    string index_set_string = construct_string_seperated_with_comma(index_set);
    index.append(index_set_string);

    set<string>::iterator it = range_index.begin();
    if (!index_set_string.empty()) index.append(", ");
    index.append(*it);

    index.append(")");
  }

  if (!equality_index.empty()) {
    if (!index.empty()) index.append(",");
    index.append(" KEY `ind_equality` (");
    string index_set_string = construct_string_seperated_with_comma(index_set);
    index.append(index_set_string);

    if (!index_set_string.empty()) index.append(", ");

    string equality_index_string =
        construct_string_seperated_with_comma(equality_index);
    index.append(equality_index_string);
    index.append(")");
  }
  return index;
}

/*
 * Get a vector<all equality str> for each key (it can be a const value or
 * a column name), it stored in map<rs, map<key, vector<all equality str>>>.
 * Also store map<rs, map<key, const value>> (this const value is from
 * 'and'condition) and map<rs, map<key, vector<'or' const value>>>.  When find
 * the first partition table we fullfil the above two map.*/
void Statement::fullfil_par_key_equality(
    record_scan *rs, const char *schema_name, const char *table_name,
    const char *table_alias, const char *key_name,
    vector<string> *added_equality_vector) {
  map<const char *, bool> find_equality;
  expr_list_item *str_list_head = rs->str_expr_list_head;
  expr_list_item *tmp = str_list_head;
  StrExpression *str_expr = NULL;
  condition_type cond_type = CONDITION_TYPE_NON;
  ConditionAndOr and_or_type = CONDITION_AND;
  const char *schema_tmp = schema_name;
  const char *table_tmp = table_name;
  const char *key_tmp = NULL;
  bool need_to_find_or = true;
  bool need_range = rs->need_range;
  record_scan_par_key_values[rs][key_name].clear();
  vector<const char *> or_equality_value = get_or_condition_key_values(
      rs->condition, schema_name, table_name, key_name, table_alias);
  if (!or_equality_value.empty()) {
    record_scan_par_key_values[rs][key_name] = or_equality_value;
  }
  need_clean_record_scan_par_key_equality = true;

  record_scan_par_key_equality[rs][key_name].push_back(key_name);
  vector<string> *vec = &(record_scan_par_key_equality[rs][key_name]);
  if (added_equality_vector && !added_equality_vector->empty())
    vec->insert(vec->end(), added_equality_vector->begin(),
                added_equality_vector->end());
  RangeValues *range_values = NULL;
#ifdef DEBUG
  LOG_DEBUG(
      "record_scan_par_key_equality size is %d with key_name size %d "
      "record_scan_par_key_values is %d.\n",
      record_scan_par_key_equality.size(), vec->size(),
      record_scan_par_key_values.size());
#endif

  if (need_range) {
    if (record_scan_par_key_ranges[rs].count(key_name) != 0) {
      if (record_scan_par_key_ranges[rs][key_name] != NULL) {
        record_scan_par_key_ranges[rs][key_name]->clear();
      }
    }
    record_scan_par_key_ranges[rs][key_name] = NULL;

    if (!tmp) {
      range_values = new RangeValues();
      record_scan_par_key_ranges[rs][key_name] = range_values;
      need_clean_record_scan_par_key_ranges = true;
      return;
    }

    range_values = new RangeValues();
  }

  if (!tmp) {
    return;
  }

  // use to deal with a=1 and b=1
  // 1. while first find a=1, will record the '1' to key_values_set,
  //    after find b=1, push b to keys directly
  // 2. while first find b=1, will record 1 to non_key_values_map,
  //    and b to the map's set, then if find a=1,
  //    push all set's value to keys
  set<string> key_values_set;
  map<string, map<string, const char *> > non_key_values_map;

  unsigned int i = 0;
  for (; i < vec->size(); ++i) {
    string tmp_str = vec->at(i);
    key_tmp = tmp_str.c_str();
    do {
      str_expr = (StrExpression *)tmp->expr;
      cond_type = check_condition_type(str_expr);
      if (cond_type == CONDITION_TYPE_NON) {
        tmp = tmp->next;
        continue;
      }
      if (schema_tmp) {
        if (!column_name_equal_key_alias_without_len(str_expr->str_value,
                                                     schema_tmp, table_tmp,
                                                     key_tmp, table_alias)) {
          // only the first time, try to fullfil the non_key_values_map,
          // for the next loops, we will only use key=1 to find column=1 in the
          // map, cause it will record all non_key=1 in the first loop, it's
          // correct
          if (i == 0 && is_column(str_expr) &&
              cond_type == CONDITION_TYPE_EQUAL &&
              is_and_or_condition(str_expr->parent, rs) == CONDITION_AND) {
            Expression *peer = get_peer(str_expr);
            const char *peer_value = expr_is_simple_value(peer);

            if (peer_value && !is_column(peer)) {
              // if '1' in the key_values_set, when find b=1,
              // will push b to keys' vector,
              // else, will record '1' to non_key_values_map,
              // wait for whether '1' will appear in key=1
              if (key_values_set.count(peer_value)) {
                if (!find_equality.count(str_expr->str_value)) {
                  vec->push_back(str_expr->str_value);
                  find_equality[str_expr->str_value] = true;
                }
              } else {
                if (non_key_values_map.count(peer_value)) {
                  non_key_values_map[peer_value][str_expr->str_value] =
                      str_expr->str_value;
                } else {
                  map<string, const char *> s_map;
                  s_map[str_expr->str_value] = str_expr->str_value;
                  non_key_values_map[peer_value] = s_map;
                }
              }
            }
          }

          tmp = tmp->next;
          continue;
        }
      } else {
        if (strcasecmp(str_expr->str_value, key_tmp)) {
          tmp = tmp->next;
          continue;
        }
      }
      if (!is_column(str_expr)) {
        tmp = tmp->next;
        continue;
      }
      and_or_type = is_and_or_condition(str_expr->parent, rs);
      switch (and_or_type) {
        case CONDITION_NO_COND:
          break;
        case CONDITION_AND: {
          // if and condition belong to a not expr, ignore it anyway.
          if (is_not_condition(str_expr)) {
            break;
          }
          switch (cond_type) {
            case CONDITION_TYPE_RANDOM:
              // ignore it, for and condition we only care about equal in hash,
              // and sequence range in range, if there is no equal or sequence
              // range we will not consider it.
              break;
            case CONDITION_TYPE_RANGE: {
              pair<Expression *, bool> peer_pair = get_range_peer(str_expr);
              Expression *peer = peer_pair.first;
              bool is_left = peer_pair.second;
              const char *peer_value = expr_is_simple_value(peer);

              if (!peer_value || is_column(peer)) break;

              if (need_range) {
                RangeType range_type = check_range_type(str_expr, is_left);
                Range *range = new Range(range_type, peer_value);
                range_values->add_and_range(range);
              }

              if (str_expr->copy_expr != NULL) {
                Expression *copy_expr = str_expr->copy_expr;
                pair<Expression *, bool> peer_pair = get_range_peer(copy_expr);
                Expression *peer = peer_pair.first;
                bool is_left = peer_pair.second;
                const char *peer_value = expr_is_simple_value(peer);

                if (!peer_value || is_column(peer)) break;

                if (need_range) {
                  RangeType range_type = check_range_type(str_expr, is_left);
                  Range *range = new Range(range_type, peer_value);
                  range_values->add_and_range(range);
                }
              }
              break;
            }
            case CONDITION_TYPE_EQUAL: {
              Expression *peer = get_peer(str_expr);
              const char *peer_value = expr_is_simple_value(peer);

              if (!peer_value) {
                if (is_column(peer)) {
                  peer_value = ((StrExpression *)peer)->str_value;
                  if (!peer_value) break;
                  if (find_equality.count(peer_value)) break;

                  vec->push_back(peer_value);
                  find_equality[peer_value] = true;
                }
              } else {
                // Only store one value for 'and condition'
                record_scan_par_key_values[rs][key_name].clear();
                record_scan_par_key_values[rs][key_name].push_back(peer_value);

                // for a=1, will record '1' to key_values_set,
                // and then push all value in non_key_values_map[1] to keys
                key_values_set.insert(peer_value);
                if (non_key_values_map.count(peer_value)) {
                  map<string, const char *>::iterator it_begin =
                      non_key_values_map[peer_value].begin();
                  map<string, const char *>::iterator it_end =
                      non_key_values_map[peer_value].end();
                  map<string, const char *>::iterator it_map;
                  for (it_map = it_begin; it_map != it_end; ++it_map) {
                    const char *col_value = it_map->second;
                    if (!find_equality.count(col_value)) {
                      vec->push_back(col_value);
                      find_equality[col_value] = true;
                    }
                  }
                }

                if (need_range) {
                  Range *range = new Range(RANGE_TYPE_EQ, peer_value);
                  range_values->clear();
                  range_values->add_and_range(range);
                }

                need_to_find_or = false;
              }
              break;
            }
            default:
              break;
          }
          break;
        }
        case CONDITION_OR: {
          if (!need_to_find_or) break;

          if (is_not_condition(str_expr) ||
              cond_type == CONDITION_TYPE_RANDOM) {
            record_scan_par_key_values[rs][key_name].clear();

            if (need_range) {
              range_values->clear();
            }

            need_to_find_or = false;

            break;
          }
          if (cond_type == CONDITION_TYPE_RANGE) {
            record_scan_par_key_values[rs][key_name].clear();

            if (need_range) {
              pair<Expression *, bool> peer_pair = get_range_peer(str_expr);
              Expression *peer = peer_pair.first;
              bool is_left = peer_pair.second;
              const char *peer_value = expr_is_simple_value(peer);

              if (!peer_value || is_column(peer)) {
                range_values->clear();
                need_to_find_or = false;
              } else {
                RangeType range_type = check_range_type(str_expr, is_left);
                Range *range = new Range(range_type, peer_value);
                range_values->add_or_range(range);
              }

              if (str_expr->copy_expr != NULL) {
                Expression *copy_expr = str_expr->copy_expr;
                pair<Expression *, bool> peer_pair = get_range_peer(copy_expr);
                Expression *peer = peer_pair.first;
                bool is_left = peer_pair.second;
                const char *peer_value = expr_is_simple_value(peer);

                if (!peer_value || is_column(peer)) {
                  range_values->clear();
                  need_to_find_or = false;
                } else {
                  RangeType range_type = check_range_type(str_expr, is_left);
                  Range *range = new Range(range_type, peer_value);
                  range_values->add_or_range(range);
                }
              }
            }
            break;
          }
          if (cond_type == CONDITION_TYPE_EQUAL) {
            Expression *peer = get_peer(str_expr);
            const char *peer_value = expr_is_simple_value(peer);

            if (!peer_value || is_column(peer)) {
              record_scan_par_key_values[rs][key_name].clear();
              if (need_range) {
                range_values->clear();
              }
              need_to_find_or = false;
              break;
            } else {
              if (need_range) {
                RangeType range_type = check_range_type(str_expr, true);
                Range *range = new Range(range_type, peer_value);
                range_values->add_or_range(range);
              }
            }
          }
        }
      }
      tmp = tmp->next;
    } while (tmp != str_list_head);

    if (vec->size() == 1) {
      // There is no related equal condition found
      vec->clear();
    }

    if (need_range) {
      record_scan_par_key_ranges[rs][key_name] = range_values;
      need_clean_record_scan_par_key_ranges = true;
    }
    schema_tmp = table_tmp = NULL;
  }
}
void Statement::fullfil_par_key_equality_full_table(
    record_scan *rs, const char *schema_name, const char *table_name,
    const char *table_alias, const char *key_name,
    vector<string> *added_equality_vector) {
  map<const char *, bool> find_equality;
  expr_list_item *str_list_head = rs->str_expr_list_head;
  expr_list_item *tmp = str_list_head;
  StrExpression *str_expr = NULL;
  condition_type cond_type = CONDITION_TYPE_NON;
  ConditionAndOr and_or_type = CONDITION_AND;
  const char *schema_tmp = schema_name;
  const char *table_tmp = table_name;
  const char *key_tmp = NULL;

  if (!tmp) return;

  string full_key_name(schema_name);
  full_key_name.append(".");
  if (table_alias)
    full_key_name.append(table_alias);
  else
    full_key_name.append(table_name);
  full_key_name.append(".");
  full_key_name.append(key_name);

  if (!record_scan_par_key_equality_full_table[rs][full_key_name].empty())
    return;
  record_scan_par_key_equality_full_table[rs][full_key_name].clear();
  record_scan_par_key_equality_full_table[rs][full_key_name].push_back(
      key_name);
  vector<string> *vec =
      &(record_scan_par_key_equality_full_table[rs][full_key_name]);
  if (added_equality_vector && !added_equality_vector->empty())
    vec->insert(vec->end(), added_equality_vector->begin(),
                added_equality_vector->end());

  // use to deal with a=1 and b=1 1. while first find a=1, will record the '1'
  // to key_values_set, after find b=1, push b to keys directly 2. while first
  // find b=1, will record 1 to non_key_values_map, and b to the map's set,
  // then if find a=1, push all set's value to keys
  set<string> key_values_set;
  map<string, map<string, const char *> > non_key_values_map;

  unsigned int i = 0;
  for (; i < vec->size(); ++i) {
    string tmp_str = vec->at(i);
    key_tmp = tmp_str.c_str();
    do {
      str_expr = (StrExpression *)tmp->expr;
      cond_type = check_condition_type(str_expr);
      if (cond_type == CONDITION_TYPE_NON) {
        tmp = tmp->next;
        continue;
      }
      if (schema_tmp) {
        if (!column_name_equal_key_alias_without_len(str_expr->str_value,
                                                     schema_tmp, table_tmp,
                                                     key_tmp, table_alias)) {
          // only the first time, try to fullfil the non_key_values_map,
          // for the next loops, we will only use key=1 to find column=1 in the
          // map, cause it will record all non_key=1 in the first loop, it's
          // correct
          if (i == 0 && is_column(str_expr) &&
              cond_type == CONDITION_TYPE_EQUAL &&
              is_and_or_condition(str_expr->parent, rs) == CONDITION_AND) {
            Expression *peer = get_peer(str_expr);
            const char *peer_value = expr_is_simple_value(peer);

            if (peer_value && !is_column(peer)) {
              // if '1' in the key_values_set, when find b=1,
              // will push b to keys' vector,
              // else, will record '1' to non_key_values_map,
              // wait for whether '1' will appear in key=1
              if (key_values_set.count(peer_value)) {
                if (!find_equality.count(str_expr->str_value)) {
                  vec->push_back(str_expr->str_value);
                  find_equality[str_expr->str_value] = true;
                }
              } else {
                if (non_key_values_map.count(peer_value)) {
                  non_key_values_map[peer_value][str_expr->str_value] =
                      str_expr->str_value;
                } else {
                  map<string, const char *> s_map;
                  s_map[str_expr->str_value] = str_expr->str_value;
                  non_key_values_map[peer_value] = s_map;
                }
              }
            }
          }

          tmp = tmp->next;
          continue;
        }
      } else {
        if (strcasecmp(str_expr->str_value, key_tmp)) {
          tmp = tmp->next;
          continue;
        }
      }
      and_or_type = is_and_or_condition(str_expr->parent, rs);
      switch (and_or_type) {
        case CONDITION_NO_COND:
          break;
        case CONDITION_AND: {
          // if and condition belong to a not expr, ignore it anyway.
          if (is_not_condition(str_expr)) {
            break;
          }
          switch (cond_type) {
            case CONDITION_TYPE_EQUAL: {
              Expression *peer = get_peer(str_expr);
              const char *peer_value = expr_is_simple_value(peer);

              if (!peer_value) {
                if (is_column(peer)) {
                  peer_value = ((StrExpression *)peer)->str_value;
                  if (!peer_value) break;
                  if (find_equality.count(peer_value)) break;

                  vec->push_back(peer_value);
                  find_equality[peer_value] = true;
                }
              } else {
                // for a=1, will record '1' to key_values_set, and then push
                // all value in non_key_values_map[1] to keys
                key_values_set.insert(peer_value);
                if (non_key_values_map.count(peer_value)) {
                  map<string, const char *>::iterator it_begin =
                      non_key_values_map[peer_value].begin();
                  map<string, const char *>::iterator it_end =
                      non_key_values_map[peer_value].end();
                  map<string, const char *>::iterator it_map;
                  for (it_map = it_begin; it_map != it_end; ++it_map) {
                    const char *col_value = it_map->second;
                    if (!find_equality.count(col_value)) {
                      vec->push_back(col_value);
                      find_equality[col_value] = true;
                    }
                  }
                }
              }
              break;
            }
            default:
              break;
          }
          break;
        }
        default:
          break;
      }
      tmp = tmp->next;
    } while (tmp != str_list_head);

    if (vec->size() == 1) {
      // There is no related equal condition found
      vec->clear();
    }

    schema_tmp = table_tmp = NULL;
  }
}

/* Get the parition id of the insert row.*/
unsigned int Statement::get_partition_one_insert_row(
    Expression *column_values, vector<unsigned int> *key_pos_vec,
    int64_t auto_inc_val, int value_count, PartitionMethod *method,
    string replace_null_char) {
  vector<const char *> key_values;
  ListExpression *expr_list = (ListExpression *)column_values;
  expr_list_item *head = expr_list->expr_list_head;
  vector<unsigned int>::iterator it = key_pos_vec->begin();

  for (; it != key_pos_vec->end(); ++it) {
    expr_list_item *tmp = head;
    unsigned int i = 0, key_pos = *it;
    const char *value = NULL;

    /**
     * key_pos == value_count means that the auto_increment key is
     * also a partitioned key but not specified in column list
     */
    if (auto_inc_status != NO_AUTO_INC_FIELD && ((int)key_pos == value_count)) {
#ifdef DEBUG
      LOG_DEBUG(
          "Partition key not specified but is auto_increment field, set it "
          "internally"
          " to calculate partition id.\n");
#endif
      char value_tmp[21];
      sprintf(value_tmp, "%ld", auto_inc_val);
      value = (const char *)value_tmp;
      key_values.push_back(value);
      continue;
    }

    /**
     * set to a proper value if auto_increment_field is 0 or NULL
     */
    if (key_pos == (unsigned int)auto_increment_key_pos &&
        auto_inc_status == AUTO_INC_VALUE_NULL) {
#ifdef DEBUG
      LOG_DEBUG(
          "Partition key is auto_increment field,"
          " since its value is 0/NULL, set it internally to calculate "
          "partition id.\n");
#endif
      char value_tmp[21];
      sprintf(value_tmp, "%ld", auto_inc_val);
      value = (const char *)value_tmp;
      key_values.push_back(value);
      continue;
    }

    for (; i < key_pos; ++i) tmp = tmp->next;
    Expression *expr_key = tmp->expr;
    value = expr_is_simple_value(expr_key);

    if (!value) {
      LOG_ERROR(
          "The partition key value of stmt [%s] is not a literal,"
          "it can not be handled by DBScale.\n",
          sql);
      // TODO: handle this situation by send the stmt "select expr_key"
      throw UnSupportPartitionSQL(
          "The partition key value of stmt is not a literal");
    }
    key_values.push_back(value);
  }

  unsigned int partition_id = 0;
  try {
    if (replace_null_char.empty())
      replace_null_char = stmt_session->get_query_sql_replace_null_char();
    partition_id = method->get_partition_id(&key_values, replace_null_char);
  } catch (...) {
    clear_simple_expression();
    throw;
  }
  if (need_clean_simple_expression) clear_simple_expression();
  return partition_id;
}

/*Get partition id for a insert asign stmt (insert into t set c=1). We assume
 * that the asign_list should not be NULL.*/
unsigned int Statement::get_partition_insert_asign(
    Expression *asign_list, const char *schema_name, size_t schema_len,
    const char *table_name, size_t table_len, const char *table_alias,
    vector<const char *> *key_names, int64_t auto_inc_val,
    PartitionMethod *method) {
  vector<const char *> key_values;
  ListExpression *expr_list = (ListExpression *)asign_list;
  expr_list_item *head = expr_list->expr_list_head;
  vector<const char *>::iterator it = key_names->begin();

  for (; it != key_names->end(); ++it) {
    expr_list_item *tmp = head;
    CompareExpression *cmp_tmp = NULL;
    StrExpression *str_tmp = NULL;
    Expression *value_expr = NULL;
    const char *key_name = *it;

    do {
      cmp_tmp = (CompareExpression *)tmp->expr;
      str_tmp = (StrExpression *)cmp_tmp->left;
      if (column_name_equal_key_alias(str_tmp->str_value, schema_name,
                                      schema_len, table_name, table_len,
                                      key_name, table_alias)) {
        value_expr = cmp_tmp->right;
        break;
      }
      tmp = tmp->next;
    } while (tmp != head);
    const char *value;
    bool part_key_is_auto_inc_key = false;
    if (!auto_inc_key_name.empty()) {
      part_key_is_auto_inc_key = column_name_equal_key_alias(
          auto_inc_key_name.c_str(), schema_name, schema_len, table_name,
          table_len, key_name, table_alias);
    }

    if (!value_expr) {
      if (part_key_is_auto_inc_key) {
#ifdef DEBUG
        LOG_DEBUG(
            "Partition key not specified but is auto_increment field, set it "
            "internally"
            " to calculate partition id.\n");
#endif
        char value_tmp[21];
        sprintf(value_tmp, "%ld", auto_inc_val);
        value = (const char *)value_tmp;
      } else {
        LOG_ERROR(
            "Cannot find partition key [%s] from assign list of sql [%s].\n",
            key_name, sql);
        throw UnSupportPartitionSQL(
            "Cannot find partition key from assign list");
      }
    } else if (part_key_is_auto_inc_key &&
               auto_inc_status == AUTO_INC_VALUE_NULL) {
#ifdef DEBUG
      LOG_DEBUG(
          "Partition key is auto_increment field,"
          " since its value is 0/NULL, set it internally to calculate "
          "partition id.\n");
#endif
      char value_tmp[21];
      sprintf(value_tmp, "%ld", auto_inc_val);
      value = (const char *)value_tmp;
    } else {
      value = expr_is_simple_value(value_expr);
      if (!value) {
        LOG_ERROR(
            "The insert value of key of stmt [%s] is not a literal,"
            "it can not be handled by DBScale.\n",
            sql);
        // TODO: handle this situation by send the stmt "select expr_key"
        throw UnSupportPartitionSQL(
            "The insert value of key of stmt is not a literal");
      }
    }
    key_values.push_back(value);
  }

  unsigned int partition_id = 0;
  try {
    string replace_null_char = stmt_session->get_query_sql_replace_null_char();
    partition_id = method->get_partition_id(&key_values, replace_null_char);
  } catch (...) {
    clear_simple_expression();
    throw;
  }
  if (need_clean_simple_expression) clear_simple_expression();
  return partition_id;
}

bool Statement::get_auto_inc_from_insert_assign(
    Expression *assign_list, const char *schema_name, size_t schema_len,
    const char *table_name, size_t table_len, const char *table_alias,
    expr_list_item **auto_inc_expr_item) {
  *auto_inc_expr_item = NULL;
  expr_list_item *head = ((ListExpression *)assign_list)->expr_list_head;
  expr_list_item *tmp = head;
  CompareExpression *cmp_tmp = NULL;
  StrExpression *str_tmp = NULL;

  do {
    cmp_tmp = (CompareExpression *)tmp->expr;
    str_tmp = (StrExpression *)cmp_tmp->left;
    if (column_name_equal_key_alias(str_tmp->str_value, schema_name, schema_len,
                                    table_name, table_len,
                                    auto_inc_key_name.c_str(), table_alias)) {
      *auto_inc_expr_item = tmp;
      return true;
    }
    tmp = tmp->next;
  } while (tmp != head);

  LOG_DEBUG("Auto_increment field [%s] is not find in assign list\n",
            auto_inc_key_name.c_str());
  return false;
}

void Statement::check_int_value(int64_t parsed_val, int64_t &curr_auto_inc_val,
                                const char *schema_name, const char *table_name,
                                PartitionedTable *part_space,
                                bool is_multi_row_insert, Session *s) {
  if (parsed_val == LLONG_MAX || parsed_val == LLONG_MIN) {
    part_space->rollback_auto_inc_value(full_table_name, old_auto_inc_val,
                                        is_multi_row_insert);
    LOG_ERROR(
        "Auto_increment value of stmt [%s] is not valid, value out of range.\n",
        sql);
    throw Error("Auto_increment value is not valid, value out of range.");
  } else if (parsed_val == 0) {
    set_auto_inc_status(AUTO_INC_VALUE_NULL);
  } else if (parsed_val == curr_auto_inc_val) {
    set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
  } else if (parsed_val > curr_auto_inc_val) {
    LOG_DEBUG(
        "Set auto_increment_value for table %s.%s to bigger one, old=%Q, "
        "new=%Q\n",
        schema_name, table_name, curr_auto_inc_val, parsed_val);
    part_space->set_auto_increment_value(full_table_name, parsed_val);
    curr_auto_inc_val = parsed_val;
    set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
  } else if (parsed_val < curr_auto_inc_val) {
    part_space->rollback_auto_inc_value(full_table_name, old_auto_inc_val,
                                        is_multi_row_insert);
    if (check_auto_increment_value &&
        !part_space->is_auto_inc_partitioning_key(schema_name, table_name, s)) {
      LOG_ERROR(
          "Auto increment value is not valid with parsed_val %d and "
          "curr_auto_inc_val %d.\n",
          parsed_val, curr_auto_inc_val);
      throw Error(
          "Auto_increment value is not valid."
          " Cannot guarantee uniqueness of auto-increment field if value is"
          " not larger than the last insert id when auto-increment field is"
          " not the partition key.");
    }
    LOG_DEBUG(
        "Auto_increment value [%Q] is not big enough, but auto_increment field "
        "is also"
        " partition key, so treat as valid.\n",
        parsed_val);
    set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
  }
}

// check for save inc value in session for reduce lock.
int64_t Statement::check_inc_int_value(
    int64_t parsed_val, int64_t curr_auto_inc_val, PartitionedTable *part_space,
    Session *session, const char *schema_name, const char *table_name) {
  if (parsed_val == LLONG_MAX || parsed_val == LLONG_MIN) {
    LOG_ERROR(
        "Auto_increment value of stmt [%s] is not valid, value out of range.\n",
        sql);
    throw Error("Auto_increment value is not valid, value out of range.");
  } else if (parsed_val == 0) {
    set_auto_inc_status(AUTO_INC_VALUE_NULL);
    return curr_auto_inc_val;
  } else if (parsed_val > curr_auto_inc_val) {
    LOG_DEBUG(
        "Set auto_increment_value for table %s.%s to bigger one, old=%Q, "
        "new=%Q\n",
        schema_name, table_name, curr_auto_inc_val, parsed_val);
    if (session->set_auto_increment_value(full_table_name, parsed_val)) {
      set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
      return parsed_val;
    } else {
      ACE_Thread_Mutex *row_lock = NULL;

      row_lock = part_space->get_row_autoinc_lock(full_table_name);
      if (row_lock) row_lock->acquire();

      int inc_step;  // not used
      curr_auto_inc_val = part_space->get_auto_increment_value(
          full_table_name, schema_name, table_name, 1, inc_step);
      if (curr_auto_inc_val <= parsed_val) {
        part_space->set_auto_increment_value(full_table_name, parsed_val);
        set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
        if (row_lock) row_lock->release();
        return parsed_val;
      } else {
        if (row_lock) row_lock->release();
        if (check_auto_increment_value) {
          LOG_ERROR(
              "Auto increment value is not valid with parsed_val %d and "
              "curr_auto_inc_val %d.\n",
              parsed_val, curr_auto_inc_val);
          throw Error(
              "Auto_increment value is not valid."
              " Cannot guarantee uniqueness of auto-increment field if value is"
              " not larger than the last insert id when auto-increment field is"
              " not the partition key.");
        } else {
          set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
          return parsed_val;
        }
      }
    }
  } else if (parsed_val == curr_auto_inc_val) {
    set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
    return curr_auto_inc_val;
  } else if (parsed_val < curr_auto_inc_val) {
    if (check_auto_increment_value && !part_space->is_auto_inc_partitioning_key(
                                          schema_name, table_name, session)) {
      LOG_ERROR(
          "Auto increment value is not valid with parsed_val %d and "
          "curr_auto_inc_val %d.\n",
          parsed_val, curr_auto_inc_val);
      throw Error(
          "Auto_increment value is not valid."
          " Cannot guarantee uniqueness of auto-increment field if value is"
          " not larger than the last insert id when auto-increment field is"
          " not the partition key.");
    }
    LOG_DEBUG(
        "Auto_increment value [%Q] is not big enough, but auto_increment field "
        "is also"
        " partition key, so treat as valid.\n",
        parsed_val);
    set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
  }
  return curr_auto_inc_val;
}

// used only when there is an auto_increment field in partitioned table
int64_t Statement::get_auto_increment_value(PartitionedTable *part_space,
                                            ExecutePlan *plan,
                                            const char *schema_name,
                                            const char *table_name,
                                            Expression *expr, int row_num,
                                            bool is_multi_row_insert) {
  int64_t curr_auto_inc_val = 0;
  if (!multiple_mode &&
      plan->session->is_contain_auto_inc_value(full_table_name)) {
    curr_auto_inc_val = plan->session->get_auto_inc_value(full_table_name);
    is_multi_row_insert = true;
    if (!expr) {
      return curr_auto_inc_val;
    }
    int64_t parsed_val = 0;
    if (expr->type == EXPR_INT) {
      parsed_val = ((IntExpression *)expr)->get_integer_value();
      check_inc_int_value(parsed_val, curr_auto_inc_val, part_space,
                          plan->session, schema_name, table_name);
    } else if (expr->type == EXPR_NULL) {
      set_auto_inc_status(AUTO_INC_VALUE_NULL);
    } else if (expr->type == EXPR_STRING || st.type == STMT_LOAD) {
      const char *str = static_cast<StrExpression *>(expr)->str_value;
      if (is_natural_number(str)) {
        parsed_val = strtoll(str, NULL, 10);
        check_inc_int_value(parsed_val, curr_auto_inc_val, part_space,
                            plan->session, schema_name, table_name);
      } else if (!strcasecmp(str, "NULL")) {
        set_auto_inc_status(AUTO_INC_VALUE_NULL);
      } else {
        LOG_ERROR(
            "The auto_increment field [%s] should be positive INT or NULL\n",
            str);
        throw Error("The auto_increment field should be positive INT or NULL.");
      }
    } else {
      LOG_ERROR(
          "The auto_increment field of stmt [%s] should be positive INT or "
          "NULL\n",
          sql);
      throw Error("The auto_increment field should be positive INT or NULL.");
    }
    return curr_auto_inc_val;
  } else {
    ACE_Thread_Mutex *row_lock = NULL;
    try {
      int insert_times = 0;
      bool updated_part_space_cache = false;
      if (auto_inc_lock_mode == AUTO_INC_LOCK_MODE_INTERLEAVED &&
          !multiple_mode) {
        insert_times = plan->session->get_insert_inc_times(full_table_name);
        if (insert_times > 3) {
          row_num = row_num > INSERT_INC_NUM ? row_num : INSERT_INC_NUM;
          is_multi_row_insert = true;
        }
      }
      if (auto_inc_step != 0 &&
          multi_row_insert_saved_auto_inc_value + auto_inc_step <=
              multi_row_insert_max_valid_auto_inc_value) {
        multi_row_insert_saved_auto_inc_value += auto_inc_step;
        curr_auto_inc_val = multi_row_insert_saved_auto_inc_value;
      } else {
        row_lock = part_space->get_row_autoinc_lock(full_table_name);
        if (row_lock) row_lock->acquire();
        curr_auto_inc_val =
            part_space->get_auto_inc_value(plan->handler, this, schema_name,
                                           table_name, row_num, auto_inc_step);
        if (row_num > 1) {
          multi_row_insert_saved_auto_inc_value = curr_auto_inc_val;
          multi_row_insert_max_valid_auto_inc_value =
              multi_row_insert_saved_auto_inc_value +
              (row_num - 1) * auto_inc_step;
        }
        updated_part_space_cache = true;
      }
      if (!expr) {
        if (row_lock) {
          row_lock->release();
          row_lock = NULL;
        }
        if (auto_inc_lock_mode == AUTO_INC_LOCK_MODE_INTERLEAVED &&
            !multiple_mode && updated_part_space_cache) {
          if (insert_times > 3) {
            multi_row_insert_saved_auto_inc_value =
                multi_row_insert_max_valid_auto_inc_value;
            plan->session->set_auto_inc_value(full_table_name,
                                              curr_auto_inc_val + 1,
                                              row_num - 1, insert_times);
          } else {
            plan->session->set_auto_inc_value(full_table_name, 0, 0,
                                              insert_times + 1);
          }
        }
        return curr_auto_inc_val;
      }
      int64_t parsed_val = 0;
      if (expr->type == EXPR_INT) {
        parsed_val = ((IntExpression *)expr)->get_integer_value();
        if (multiple_mode) {
          if (parsed_val == 0)
            set_auto_inc_status(AUTO_INC_VALUE_NULL);
          else if (instance_option_value["record_auto_increment_delete_value"]
                       .uint_val &&
                   plan->session->mul_can_set_auto_increment_value(
                       full_table_name, parsed_val)) {
            // for oltp test, when in multiple_mode, dbscale should can insert
            // automent value if the value has been deleted by this session
            curr_auto_inc_val = parsed_val;
            set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
          } else if (get_session()->has_bridge_mark_sql() &&
                     get_session()
                         ->get_session_option("is_bridge_session")
                         .bool_val) {
            if (parsed_val > curr_auto_inc_val)
              part_space->set_auto_increment_value(full_table_name, parsed_val);
            curr_auto_inc_val = parsed_val;
            set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
          } else if (!check_auto_increment_value) {
            curr_auto_inc_val = parsed_val;
            set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
          } else {
            LOG_ERROR(
                "In multiple_mode, dbscale do not support partition table set "
                "auto_increment value\n");
            throw Error(
                "In multiple_mode, dbscale do not support partition table set "
                "auto_increment value");
          }
        } else {
          check_int_value(parsed_val, curr_auto_inc_val, schema_name,
                          table_name, part_space, is_multi_row_insert,
                          plan->session);
        }
      } else if (expr->type == EXPR_NULL) {
        set_auto_inc_status(AUTO_INC_VALUE_NULL);
      } else if (expr->type == EXPR_STRING || st.type == STMT_LOAD) {
        const char *str = static_cast<StrExpression *>(expr)->str_value;
        if (is_natural_number(str)) {
          parsed_val = strtoll(str, NULL, 10);
          if (multiple_mode) {
            if (parsed_val == 0)
              set_auto_inc_status(AUTO_INC_VALUE_NULL);
            else if (instance_option_value["record_auto_increment_delete_value"]
                         .uint_val &&
                     plan->session->mul_can_set_auto_increment_value(
                         full_table_name, parsed_val)) {
              // for oltp test, when in multiple_mode, dbscale should can insert
              // automent value if the value has been deleted by this session
              curr_auto_inc_val = parsed_val;
              set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
            } else if (get_session()->has_bridge_mark_sql() &&
                       get_session()
                           ->get_session_option("is_bridge_session")
                           .bool_val) {
              if (parsed_val > curr_auto_inc_val)
                part_space->set_auto_increment_value(full_table_name,
                                                     parsed_val);
              curr_auto_inc_val = parsed_val;
              set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
            } else if (!check_auto_increment_value) {
              curr_auto_inc_val = parsed_val;
              set_auto_inc_status(AUTO_INC_NO_NEED_MODIFY);
            } else {
              LOG_ERROR(
                  "In multiple_mode, dbscale do not support partition table "
                  "set auto_increment value\n");
              throw Error(
                  "In multiple_mode, dbscale do not support partition table "
                  "set auto_increment value");
            }
          } else {
            check_int_value(parsed_val, curr_auto_inc_val, schema_name,
                            table_name, part_space, is_multi_row_insert,
                            plan->session);
          }
        } else if (!strcasecmp(str, "NULL")) {
          set_auto_inc_status(AUTO_INC_VALUE_NULL);
        } else {
          part_space->rollback_auto_inc_value(full_table_name, old_auto_inc_val,
                                              is_multi_row_insert);
          LOG_ERROR(
              "The auto_increment field should be positive INT or NULL\n");
          throw Error(
              "The auto_increment field should be positive INT or NULL.");
        }
      } else {
        part_space->rollback_auto_inc_value(full_table_name, old_auto_inc_val,
                                            is_multi_row_insert);
        LOG_ERROR(
            "The auto_increment field of stmt [%s] should be positive INT or "
            "NULL\n",
            sql);
        throw Error("The auto_increment field should be positive INT or NULL.");
      }
      if (row_lock) {
        row_lock->release();
        row_lock = NULL;
      }

      if (auto_inc_lock_mode == AUTO_INC_LOCK_MODE_INTERLEAVED &&
          !multiple_mode) {
        if (insert_times > 3) {
          plan->session->set_auto_inc_value(full_table_name,
                                            curr_auto_inc_val + 1, row_num - 1,
                                            insert_times);
        } else {
          plan->session->set_auto_inc_value(full_table_name, 0, 0,
                                            insert_times + 1);
        }
      }
      if (row_num > 1 && auto_inc_step != 0) {
        if (curr_auto_inc_val > multi_row_insert_saved_auto_inc_value) {
          int64_t extra_v =
              curr_auto_inc_val - multi_row_insert_saved_auto_inc_value;
          multi_row_insert_saved_auto_inc_value = curr_auto_inc_val;
          int m = extra_v % auto_inc_step;
          if (m != 0) {
            multi_row_insert_saved_auto_inc_value += (auto_inc_step - m);
          }
        }
      }

      return curr_auto_inc_val;
    } catch (...) {
      if (row_lock) {
        row_lock->release();
        row_lock = NULL;
      }

      throw;
    }
  }
}

int64_t Statement::get_auto_inc_value_one_insert_row(
    PartitionedTable *part_space, ExecutePlan *plan, const char *schema_name,
    const char *table_name, Expression *expr, int row_num) {
  ACE_Thread_Mutex *stmt_lock =
      part_space->get_stmt_autoinc_lock(full_table_name);
  if (stmt_lock) stmt_lock->acquire();
  try {
    int64_t curr_auto_inc_val = get_auto_increment_value(
        part_space, plan, schema_name, table_name, expr, row_num, false);
    if (stmt_lock) stmt_lock->release();
    return curr_auto_inc_val;
  } catch (...) {
    if (stmt_lock) stmt_lock->release();
    throw;
  }
}

int64_t Statement::get_auto_inc_value_multi_insert_row(
    PartitionedTable *part_space, ExecutePlan *plan, const char *schema_name,
    const char *table_name, Expression *expr, int row_num) {
  return get_auto_increment_value(part_space, plan, schema_name, table_name,
                                  expr, row_num, true);
}

void Statement::init_auto_increment_params(ExecutePlan *plan,
                                           const char *schema_name,
                                           const char *table_name,
                                           PartitionedTable *part_space) {
  int alter_table = part_space->set_auto_increment_info(
      plan->handler, this, schema_name, table_name, true, true);
  if (alter_table == 1) {
    Driver *driver = Driver::get_driver();
    driver->acquire_session_mutex();
    set<Session *, compare_session> *session_set = driver->get_session_set();
    set<Session *, compare_session>::iterator it = session_set->begin();
    for (; it != session_set->end(); ++it) {
      (*it)->reset_auto_increment_info(full_table_name);
    }
    driver->release_session_mutex();
  }
  if (auto_inc_status != NO_AUTO_INC_FIELD) {
    auto_increment_key_pos =
        part_space->get_auto_increment_key_pos(full_table_name);
    auto_inc_key_name = part_space->get_auto_increment_key(full_table_name);
  }
}

/**
 * For multiple row insert statement only.
 *
 * roll back last_insert_id to the value before current multiple row
 * insert statement is analyticed;
 * we could also rollback auto_increment_value to the same point if
 * current auto_inc_lock_mode is TRADITIONAL or CONSECUTIVE, but not
 * if INTERLEAVED cause we do not hold the mutex in that condition.
 */
void Statement::rollback_auto_inc_params(ExecutePlan *plan,
                                         PartitionedTable *part_table) {
  part_table->rollback_auto_inc_value(full_table_name, old_auto_inc_val, true);
  int enable_last_insert_id_session =
      plan->handler->get_session()
          ->get_session_option("enable_last_insert_id")
          .int_val;
  if (enable_last_insert_id_session)
    plan->handler->get_session()->set_last_insert_id(old_last_insert_id);
}

/* Get the key position from the given column list, such as insert. The
 * position is start from 0. For this function, we assume that the list is not
 * NULL.*/
unsigned int Statement::get_key_pos_from_column_list(
    name_item *list, const char *schema_name, size_t schema_len,
    const char *table_name, size_t table_len, const char *table_alias,
    const char *key_name) {
  name_item *head = list;
  name_item *tmp = head;
  unsigned int pos = 0;

  do {
    if (column_name_equal_key_alias(tmp->name, schema_name, schema_len,
                                    table_name, table_len, key_name,
                                    table_alias)) {
#ifdef DEBUG
      LOG_DEBUG("Find key pos [%d] from column list of sql [%s].\n", (int)pos,
                sql);
#endif
      return pos;
    }
    tmp = tmp->next;
    ++pos;
  } while (tmp != head);

  if (auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    if (column_name_equal_key_alias(auto_inc_key_name.c_str(), schema_name,
                                    schema_len, table_name, table_len, key_name,
                                    table_alias)) {
      /**
       * partition field is also auto_increment field,
       * but not specified in column list, since pos's value marks
       * end() of key_vec, use it to mark such situation.
       */
      return pos;
    }
  }

  LOG_ERROR("Cannot find partition key [%s] from column list of sql [%s].\n",
            key_name, sql);
  throw UnSupportPartitionSQL(
      "Cannot find partition key from column list of sql");
  return 0;
}

int Statement::get_auto_inc_pos_from_column_list(
    name_item *list, const char *schema_name, size_t schema_len,
    const char *table_name, size_t table_len, const char *table_alias) {
  name_item *head = list;
  name_item *tmp = head;
  int pos = 0;
  do {
    if (column_name_equal_key_alias(tmp->name, schema_name, schema_len,
                                    table_name, table_len,
                                    auto_inc_key_name.c_str(), table_alias)) {
      return pos;
    }
    tmp = tmp->next;
    ++pos;
  } while (tmp != head);
#ifdef DEBUG
  LOG_DEBUG(
      "Auto_increment field [%s] not specified in column list, statement is "
      "[%s]\n",
      auto_inc_key_name.c_str(), sql);
#endif
  set_auto_inc_status(AUTO_INC_NOT_SPECIFIED);
  return -1;
}

bool Statement::select_need_divide(record_scan *top_select,
                                   record_scan *partition,
                                   unsigned int dataspace_merge_start,
                                   const char *schema_name, size_t schema_len,
                                   const char *table_name, size_t table_len,
                                   const char *table_alias,
                                   vector<const char *> *key_names) {
  /* If the partition record_scan is not the top one, it and all its upper
   * record_scan, except top record_scan, should not contain
   * order_by|limit|group_by, otherwise this select_part should be divided.*/
  record_scan *tmp = partition;
  while (tmp != top_select) {
    if (tmp->order_by_list || tmp->limit || tmp->group_by_list) {
      LOG_DEBUG(
          "Record scan of partition table contains order_by|limit|group_by,"
          " so the select part of sql[%s] must be divided for execution.",
          sql);
      return true;
    }
    tmp = tmp->upper;
#ifdef DEBUG
    ACE_ASSERT(tmp);
#endif
  }

  /*When Partition record_scan is a sub_select condition,
   If the condition type is 'exists', it can not execute without divided;
   (may allow in the future)
   If the condition type is 'in'|'not in'|'compare', the select list of
   partition record_scan should contain the partition key.*/
  if (partition->condition_belong) {
    Expression *expr = partition->condition_belong->parent;
    if (expr == NULL) return true;
    switch (expr->type) {
      case EXPR_EXISTS: {
        // TODO: we need to ensure that: the condition contains
        //"par.key=[outer_table.column]"
        return true;
        break;
      }
      case EXPR_EQ:
      case EXPR_IN: {
        // select field list should contain all 'par.key'
        bool field_has_all_key = true;
        vector<const char *>::iterator it = key_names->begin();
        for (; it != key_names->end(); ++it) {
          bool field_has_key = false;
          const char *key_name = *it;
          field_item *tmp = partition->field_list_head;
          while (tmp) {
            Expression *expr_field = tmp->field_expr;
            if (expr_field && expr_field->type == EXPR_STR) {
              StrExpression *str_field = (StrExpression *)expr_field;
              if (is_column(str_field)) {
                if (column_name_equal_key_alias(
                        str_field->str_value, schema_name, schema_len,
                        table_name, table_len, key_name, table_alias)) {
                  field_has_key = true;
                  break;
                }
              }
            }
            tmp = tmp->next;
          }
          if (!field_has_key) {
            field_has_all_key = false;
            break;
          }
        }
        if (!field_has_all_key) {
          LOG_DEBUG(
              "Fail to find the partition_key in the field list of"
              " partition record_scan of sql[%s].\n",
              sql);
          return true;
        }
        break;
      }
      default:  // NOT_IN, other compare
      {
        return true;
      }
    }
  }
  /*The select part should only has one dataspace(table or higher level) after
   * merge, otherwise it should be divided.*/
  if (!can_dataspace_merge(dataspace_merge_start)) return true;

  return false;
}

/* Try to merge dataspaces from spaces[dataspace_merge_start] to
 * spaces[spaces.size()]*/
bool Statement::can_dataspace_merge(unsigned int dataspace_merge_start) {
  unsigned int i = dataspace_merge_start;
  DataSpace *merge_space = spaces[i];
  ++i;
  for (; i < spaces.size(); ++i) {
    if (stmt_session->is_dataspace_cover_session_level(merge_space, spaces[i]))
      continue;
    else if (stmt_session->is_dataspace_cover_session_level(spaces[i],
                                                            merge_space))
      merge_space = spaces[i];
    else {
      LOG_DEBUG("Fail to merge the dataspaces of sql [%s].\n", sql);
      return false;
    }
  }
  spaces[dataspace_merge_start] = merge_space;
  need_clean_spaces = true;
  return true;
}

/* Check whether the set list expression contain the given columns*/
bool set_list_contain_key(Expression *update_set_list,
                          vector<const char *> *columns,
                          const char *schema_name, const char *table_name,
                          const char *table_alias, string &key_value) {
  ListExpression *expr_list = (ListExpression *)update_set_list;
  expr_list_item *head = expr_list->expr_list_head;

  vector<const char *>::iterator it = columns->begin();

  for (; it != columns->end(); ++it) {
    expr_list_item *tmp = head;
    CompareExpression *cmp_tmp = NULL;
    StrExpression *str_tmp = NULL;
    const char *key_name = *it;

    do {
      cmp_tmp = (CompareExpression *)tmp->expr;
      str_tmp = (StrExpression *)cmp_tmp->left;
      if (column_name_equal_key_alias(
              str_tmp->str_value, schema_name, strlen(schema_name), table_name,
              strlen(table_name), key_name, table_alias)) {
        Expression *right_expr = cmp_tmp->right;
        const char *str_val = right_expr->str_value;
        key_value = str_val ? str_val : "";
        return true;
      }
      tmp = tmp->next;
    } while (tmp != head);
  }

  return false;
}

bool set_list_contain_key(Expression *update_set_list,
                          vector<string *> *columns, const char *schema_name,
                          const char *table_name, const char *table_alias,
                          string &key_value) {
  vector<const char *> column_values;
  vector<string *>::iterator it = columns->begin();
  for (; it != columns->end(); ++it) column_values.push_back((*it)->c_str());
  return set_list_contain_key(update_set_list, &column_values, schema_name,
                              table_name, table_alias, key_value);
}

bool Statement::check_and_assemble_multi_send_node(ExecutePlan *plan) {
  ACE_UNUSED_ARG(plan);
#ifndef DBSCALE_TEST_DISABLE

  dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "multi_send_node") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "working_thread") &&
      !strcasecmp(sql, "select * FROM test_multi_send_node")) {
    Backend *backend = Backend::instance();
    DataSpace *data_space =
        backend->get_data_space_for_table("test", "test_multi_send_node");

    TransferRule *rule = backend->dispatch_rule;
    /* Construct the dispatch strategy */
    DataSpace *data_space_send =
        backend->get_data_space_for_table("test", "test_multi_send_dataspace");
    vector<const char *> *key_names =
        ((PartitionedTable *)data_space_send)->get_key_names();
    PartitionMethod *method =
        ((PartitionedTable *)data_space_send)->get_partition_method();
    DispatchStrategy *dispatch_strategy = rule->get_dispatch_strategy();
    dispatch_strategy->method = method;
    dispatch_strategy->key_names = key_names;  // key names should be one column
                                               // in table test_multi_send_node
    dispatch_strategy->local_key_name = string(key_names->at(0));
    dispatch_strategy->part_table = (PartitionedTable *)data_space_send;
    dispatch_strategy->dup_table = NULL;

    assemble_dispatch_packet_plan(plan, rule, data_space, data_space_send,
                                  "select * FROM test_multi_send_node", NULL);
    return true;
  } else if (!strcasecmp(test_info->test_case_name.c_str(),
                         "multi_send_node") &&
             !strcasecmp(test_info->test_case_operation.c_str(),
                         "working_thread_dup") &&
             !strcasecmp(sql, "select * FROM test_multi_send_node")) {
    Backend *backend = Backend::instance();
    DataSpace *data_space =
        backend->get_data_space_for_table("test", "test_multi_send_node");

    TransferRule *rule = backend->dispatch_rule;
    /* Construct the dispatch strategy */
    DataSpace *data_space_send =
        backend->get_data_space_for_table("test", "test_multi_send_dataspace");
    DuplicatedTable *dup_tab = new DuplicatedTable(
        (PartitionedTable *)data_space_send, "duplicated", "");
    DispatchStrategy *dispatch_strategy = rule->get_dispatch_strategy();
    dispatch_strategy->dup_table = dup_tab;

    Schema *join_schema = backend->find_schema(TMP_TABLE_SCHEMA);
    join_schema->add_table(dup_tab);

    assemble_dispatch_packet_plan(plan, rule, data_space, data_space_send,
                                  "select * FROM test_multi_send_node", NULL);
    return true;
  } else {
    return false;
  }
#else
  return false;
#endif
}

const char *Statement::adjust_stmt_sql_for_alias(DataSpace *dataspace,
                                                 const char *old_sql) {
  string new_sql_tmp;
  const char *used_sql = old_sql;

  if (!dataspace) {
    LOG_ERROR("Internal error for Statement::adjust_stmt_sql_for_shard.\n");
    throw Error("Internal error for Statement::adjust_stmt_sql_for_shard.");
  }

  if (dataspace->get_dataspace_type() == SCHEMA_TYPE &&
      ((Schema *)dataspace)->get_is_alias()) {
    Parser *parser = Driver::get_driver()->get_parser();
    Statement *new_stmt = parser->parse(old_sql, stmt_allow_dot_in_ident, true,
                                        NULL, NULL, NULL, ctype);
    re_parser_stmt_list.push_back(new_stmt);
    new_stmt->check_and_refuse_partial_parse();

    adjust_alias_schema(old_sql, schema, new_stmt->get_stmt_node(),
                        record_scan_all_table_spaces, new_sql_tmp,
                        (Schema *)dataspace);
    record_shard_sql_str(new_sql_tmp);  // just reuse the shard_sql_vec to store
                                        // the alias schema sql
    used_sql = get_last_shard_sql();
  }

  return used_sql;
}

const char *Statement::adjust_stmt_sql_for_shard(DataSpace *dataspace,
                                                 const char *old_sql) {
  string new_sql_tmp;
  const char *used_sql = old_sql;
  if (!dataspace) {
    LOG_ERROR("Internal error for Statement::adjust_stmt_sql_for_shard.\n");
    throw Error("Internal error for Statement::adjust_stmt_sql_for_shard.");
  }

  if (dataspace->get_virtual_machine_id() > 0) {
    adjust_virtual_machine_schema(dataspace->get_virtual_machine_id(),
                                  dataspace->get_partition_id(), old_sql,
                                  schema, get_latest_stmt_node(),
                                  record_scan_all_table_spaces, new_sql_tmp);
    record_shard_sql_str(new_sql_tmp);
    used_sql = get_last_shard_sql();
  }
  return used_sql;
}

void Statement::check_stmt_sql_for_dbscale_row_id(stmt_type type,
                                                  record_scan *scanner) {
  if (type != STMT_SELECT) return;
  field_item *field = scanner->field_list_head;
  if (!field) return;
  if (field->alias && (!strcasecmp(field->alias, "dbscale_row_id") ||
                       !strcasecmp(field->alias, "dbscale_row_num"))) {
    is_first_column_dbscale_row_id = true;
  }
}

void Statement::assemble_connect_by_plan(ExecutePlan *plan,
                                         DataSpace *dataspace) {
  record_scan *rs = st.scanner;
  string new_sql;
  if (!rs->from_pos) {
    LOG_ERROR("Not support connect by without from table.\n");
    throw Error("Not support connect by without from table.");
  }
  if (rs->from_pos >= strlen(sql)) {
    LOG_ERROR("Fail to get from pos for connect by sql.\n");
    throw Error("Fail to get from pos for connect by sql.");
  }

  new_sql.append(sql + rs->start_pos - 1, rs->from_pos - rs->start_pos);
  list<string>::reverse_iterator it = new_select_items.rbegin();
  for (; it != new_select_items.rend(); ++it) {
    new_sql.append(",");
    new_sql.append(*it);
    new_sql.append(" ");
  }

  new_sql.append(sql + rs->from_pos - 1);
  record_statement_stored_string(new_sql);
  sql = statement_stored_string.back().c_str();
  Statement *new_stmt = NULL;
  re_parser_stmt(&(st.scanner), &new_stmt, sql);

  const char *used_sql = adjust_stmt_sql_for_shard(dataspace, sql);

  ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
  ExecuteNode *conn_node = plan->get_connect_by_node(connect_by_desc);
  conn_node->add_child(fetch_node);
  ExecuteNode *filter_node = plan->get_project_node(new_select_items.size());
  filter_node->add_child(conn_node);
  ExecuteNode *send_node = plan->get_send_node();
  send_node->add_child(filter_node);
  plan->set_start_node(send_node);
}

void Statement::assemble_select_plain_value_plan(ExecutePlan *plan,
                                                 const char *str_value,
                                                 const char *alias_name) {
  LOG_DEBUG("Assemble select plain value plan.\n");
  ExecuteNode *node = plan->get_select_plain_value_node(str_value, alias_name);
  plan->set_start_node(node);
}

void Statement::assemble_direct_exec_plan(ExecutePlan *plan,
                                          DataSpace *dataspace) {
  LOG_DEBUG("Assemble direct exec plan.\n");
  if (plan->get_is_parse_transparent()) {
    ExecuteNode *node = plan->get_direct_execute_node(dataspace, sql);
    plan->set_start_node(node);
    LOG_DEBUG("Assemble sql [%s] parse transparent.\n", sql);
    return;
  }
  if (Backend::instance()->need_deal_with_metadata(st.type, &st))
    st.need_apply_metadata = !is_share_same_server(
        Backend::instance()->get_metadata_data_space(), dataspace);
  if (is_connect_by) {
    assemble_connect_by_plan(plan, dataspace);
    return;
  }

  const char *used_sql = adjust_stmt_sql_for_shard(dataspace, sql);

  if (union_all_sub) {
    ExecuteNode *node = plan->get_fetch_node(dataspace, used_sql);
    union_all_nodes.push_back(node);
  } else {
    if (is_into_outfile()) {
      ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
      ExecuteNode *send_node = plan->get_into_outfile_node();
      send_node->add_child(fetch_node);
      plan->set_start_node(send_node);
    } else if (is_select_into()) {
      ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
      ExecuteNode *send_node = plan->get_select_into_node();
      send_node->add_child(fetch_node);
      plan->set_start_node(send_node);
    } else {
      plan->session->set_is_simple_direct_stmt(true);
      ExecuteNode *node = plan->get_direct_execute_node(dataspace, used_sql);
      plan->set_start_node(node);
    }
  }
}

void Statement::assemble_dbscale_estimate_select_plan(ExecutePlan *plan,
                                                      DataSpace *dataspace,
                                                      Statement *statement) {
  LOG_DEBUG("Assemble estimate select exec plan.\n");
  ExecuteNode *node = plan->get_estimate_select_node(dataspace, sql, statement);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_estimate_select_partition_plan(
    ExecutePlan *plan, vector<unsigned int> *par_ids,
    PartitionedTable *par_table, const char *sql, Statement *statement) {
  LOG_DEBUG("Assemble estimate select exec plan.\n");
  ExecuteNode *node = plan->get_estimate_select_partition_node(
      par_ids, par_table, sql, statement);
  plan->set_start_node(node);
}

void Statement::assemble_direct_exec_plan(ExecutePlan *plan,
                                          DataSpace *dataspace,
                                          string &customized_sql) {
  LOG_DEBUG("Assemble direct execute plan with generated sql [%s].\n",
            customized_sql.c_str());
  const char *used_sql =
      adjust_stmt_sql_for_shard(dataspace, customized_sql.c_str());

  ExecuteNode *node = plan->get_direct_execute_node(dataspace, used_sql);
  plan->set_start_node(node);
}

void Statement::assemble_forward_master_role_plan(ExecutePlan *plan,
                                                  bool is_slow_query) {
  LOG_DEBUG("Assemble forward master role plan %@ for sql %s.\n", plan, sql);

  // session use db expired(dropped)
  Session *s = plan->session;
  string plan_cur_schema;
  s->get_schema(plan_cur_schema);
  if (enable_acl) {
    if (!s->is_contain_schema(plan_cur_schema)) {
      // operation about other database db
      if (st.table_list_tail && st.table_list_tail->join) {
        const char *schema_name = st.table_list_tail->join->schema_name;
        if (schema_name) {
          // sql schema name is dropped database
          string tmp_schema(schema_name);
          if (!s->is_contain_schema(tmp_schema)) {
            string err;
            err.append(tmp_schema);
            err.append(" doesn't exist");
            throw Error(err.c_str());
          }
          plan->set_forward_sql_target_db(tmp_schema);
        }
      } else if (st.type == STMT_CREATE_DB || st.type == STMT_DROP_DB) {
        plan->set_forward_sql_target_db(default_login_schema);
      } else if (is_dbscale_command(st.type)) {
        if (!s->is_contain_schema(plan_cur_schema))
          plan->set_forward_sql_target_db(default_login_schema);
      } else {
        throw Error("No database selected", ERROR_NO_DATABASE_SELECTED_CODE);
      }
    }
  }
  ExecuteNode *node = plan->get_forward_master_role_node(sql, is_slow_query);
  plan->set_start_node(node);
  plan->set_is_forward_plan();
}

void Statement::assenble_dbscale_reset_zoo_info_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale reset zoo info node.\n");
  ExecuteNode *node = plan->get_reset_zoo_info_node();
  plan->set_start_node(node);
}

void Statement::assemble_one_partition_plan(ExecutePlan *plan,
                                            DataSpace *dataspace) {
  LOG_DEBUG("Assemble one partition plan.\n");
  assemble_direct_exec_plan(plan, dataspace);
  if (st.type <= STMT_DDL_START || st.type >= STMT_DDL_END) {
    plan->session->set_execute_plan_touch_partition_nums(1);
  }
}

void Statement::assemble_one_partition_plan(ExecutePlan *plan,
                                            DataSpace *dataspace,
                                            string &customized_sql) {
  LOG_DEBUG("Assemble one partition plan with generated sql [%s].\n",
            customized_sql.c_str());
  assemble_direct_exec_plan(plan, dataspace, customized_sql);
}

void clean_up_execute_nodes(vector<ExecuteNode *> *nodes) {
  vector<ExecuteNode *>::iterator it = nodes->begin();
  for (; it != nodes->end(); ++it) {
    (*it)->clean();
    delete (*it);
  }
  nodes->clear();
}

void Statement::assemble_mul_par_result_set_plan(ExecutePlan *plan,
                                                 vector<unsigned int> *par_ids,
                                                 PartitionedTable *par_table,
                                                 bool need_check_distinct) {
  LOG_DEBUG("Assemble mul par result set plan [%d] partitions of [%s].\n",
            par_ids->size(), par_table->get_name());
  vector<ExecuteNode *> nodes;
  generate_select_plan_nodes(plan, par_ids, par_table, &nodes, sql,
                             need_check_distinct);
  if (union_all_sub) {
    if (is_into_outfile()) {
      clean_up_execute_nodes(&nodes);
      LOG_ERROR("Can not SELECT INTO OUTFILE in UNION ALL statement.\n");
      throw NotSupportedError(
          "Can not SELECT INTO OUTFILE in UNION ALL statement.");
    }
    unsigned int i = 0;
    for (; i < nodes.size(); ++i) {
      union_all_nodes.push_back(nodes[i]);
    }
  } else if (fe_join) {
    unsigned int i = 0;
    for (; i < nodes.size(); ++i) {
      fe_join_nodes.push_back(nodes[i]);
    }
  } else {
    ExecuteNode *send_node;
    try {
      if (is_into_outfile()) {
        send_node = plan->get_into_outfile_node();
        if (plan->get_fetch_node_no_thread())
          plan->set_fetch_node_no_thread(false);
      } else if (is_select_into()) {
        send_node = plan->get_select_into_node();
        if (plan->get_fetch_node_no_thread())
          plan->set_fetch_node_no_thread(false);
      } else {
        send_node = plan->get_send_node();
        if (is_handle_sub_query()) plan->set_fetch_node_no_thread(false);
        send_node->set_is_first_column_dbscale_row_id(
            is_first_column_dbscale_row_id);
      }
    } catch (...) {
      clean_up_execute_nodes(&nodes);
      throw;
    }

    unsigned int i = 0;
    for (; i < nodes.size(); ++i) {
      send_node->add_child(nodes[i]);
    }
    plan->set_start_node(send_node);
  }
}

long get_integer_value_from_expr(IntExpression *expr) {
  return expr->get_integer_value();
}

long Statement::get_limit_clause_offset_value(IntExpression *limit_offset,
                                              ExecutePlan *plan,
                                              record_scan *rs) {
  if (limit_offset) {
    return get_integer_value_from_expr(limit_offset);
  } else if (rs->limit->offset_is_uservar) {
    if (plan->session->is_call_store_procedure()) {
      map<string, string> *user_var_map = plan->session->get_user_var_map();
      string var_name(rs->limit->offset_uservar_str);
      boost::to_upper(var_name);
      string var_value;
      if (user_var_map->count(var_name)) {
        var_value = (*user_var_map)[var_name];
        if (!is_natural_number(var_value.c_str())) {
          LOG_ERROR("Offset value [%s] of limit clause is invalid.\n",
                    var_value.c_str());
          throw Error("Offset value of limit clause is invalid");
        }
        return atoi(var_value.c_str());
      } else {
        LOG_ERROR(
            "Offset value of limit clause is invalid, uservar not exist\n");
        throw Error("Offset value of limit clause is invalid");
      }
    }
  }
  return 0;
}

ExecuteNode *Statement::generate_limit_subplan(ExecutePlan *plan,
                                               record_scan *rs,
                                               ExecuteNode **tail_node) {
  long offset = 0;
  long num = 0;
  IntExpression *limit_offset = rs->limit->offset;
  IntExpression *limit_num = rs->limit->num;

  offset = get_limit_clause_offset_value(limit_offset, plan, rs);
  if (limit_num)
    num = get_integer_value_from_expr(limit_num);
  else {
    LOG_ERROR("limit num is not correct for sql [%s].\n",
              fetch_sql_tmp.c_str());
    throw Error("limit num is not correct");
  }
  ExecuteNode *limit = plan->get_limit_node(offset, num);

  if (offset) {
    unsigned int limit_pos = rs->limit_pos;
    string new_sql(fetch_sql_tmp.c_str(), limit_pos + 4);
    new_sql.append(" ");
    char new_num[128];
    size_t len = sprintf(new_num, "%ld", offset + num);
    new_sql.append(new_num, len);
    fetch_sql_tmp.clear();
    fetch_sql_tmp.append(new_sql.c_str());
    LOG_DEBUG("Limit node rebuild the fetch sql %s.\n", fetch_sql_tmp.c_str());
  }

  *tail_node = limit;

  return limit;
}

void Statement::init_uservar(ExecutePlan *plan, Session *work_session) {
  if (!work_session) return;
  Session *session = plan->session;
  map<string, string> *user_var_map = work_session->get_user_var_map();
  map<string, string>::iterator it = user_var_map->begin();
  for (; it != user_var_map->end(); it++) {
    bool is_str = false;
    if (!it->second.empty()) {
      is_str = (it->second[0] == '\'');
    }
    session->add_user_var_value(it->first, it->second, is_str);
  }
}

void Statement::assemble_dispatch_packet_plan(
    ExecutePlan *plan, TransferRule *rule, DataSpace *exec_space,
    DataSpace *send_space, const char *sql, Session *work_session) {
  PartitionScheme *send_scheme = NULL;
  PartitionScheme *exec_scheme = NULL;
  if (!send_space->get_data_source()) {
    if (!send_space->is_duplicated())
      send_scheme = ((PartitionedTable *)send_space)->get_partition_scheme();
    else
      send_scheme = ((DuplicatedTable *)send_space)
                        ->get_partition_table()
                        ->get_partition_scheme();
  }
  if (!exec_space->get_data_source()) {
    if (!exec_space->is_duplicated())
      exec_scheme = ((PartitionedTable *)exec_space)->get_partition_scheme();
    else
      exec_scheme = ((DuplicatedTable *)exec_space)
                        ->get_partition_table()
                        ->get_partition_scheme();
  }

  if ((exec_scheme && exec_scheme->is_shard()) ||
      (send_scheme && send_scheme->is_shard())) {
    LOG_ERROR(
        "Unsupport shard partition table for data_read cross node join.\n");

    throw NotSupportedError(
        "Unsupport shard partition table for data_read cross node join.");
  }

  ExecuteNode *dispatch_packet_node = NULL;
  if (send_space->get_data_source())
    dispatch_packet_node = plan->get_send_node();
  else
    dispatch_packet_node = plan->get_dispatch_packet_node(rule);
  if (exec_space->get_data_source()) {
    ExecuteNode *fetch_node = plan->get_fetch_node(exec_space, sql);
    dispatch_packet_node->add_child(fetch_node);
  } else {
    // If it is a subquery, then the sql was not handled by reconstruct, we
    // should regenerate it.
    if (rule->is_sub_query()) {
      re_parser_stmt(&(st.scanner), &(plan->statement), sql);
      plan->statement->set_fe_join(true);
      plan->statement->set_default_schema(schema);
      plan->statement->init_uservar(plan, work_session);
      try {
        plan->statement->generate_execution_plan(plan);
#ifndef DBSCALE_TEST_DISABLE
        Backend *bk = Backend::instance();
        dbscale_test_info *test_info = bk->get_dbscale_test_info();
        if (!strcasecmp(test_info->test_case_name.c_str(), "execution_plan") &&
            !strcasecmp(test_info->test_case_operation.c_str(),
                        "fail_in_assemble_dispatch_packet_plan")) {
          throw UnSupportPartitionSQL(
              "dbscale_test operation execution_plan for case "
              "fail_in_assemble_dispatch_packet_plan");
        }
#endif
      } catch (...) {
        work_session->set_fe_error(true);
        if (dispatch_packet_node) {
          vector<ExecuteNode *> fe_join_nodes_tmp =
              plan->statement->get_fe_join_nodes();
          for (unsigned int i = 0; i < fe_join_nodes_tmp.size(); ++i) {
            dispatch_packet_node->add_child(fe_join_nodes_tmp[i]);
          }
          dispatch_packet_node->clean();
          delete dispatch_packet_node;
          dispatch_packet_node = NULL;
        }
        LOG_ERROR("got error when generate execution plan.\n");
        throw;
      }
      vector<ExecuteNode *> fe_join_nodes_tmp =
          plan->statement->get_fe_join_nodes();
      for (unsigned int i = 0; i < fe_join_nodes_tmp.size(); ++i) {
        dispatch_packet_node->add_child(fe_join_nodes_tmp[i]);
      }
    } else {
      DataSpace *space = NULL;
      unsigned int i = 0;
      PartitionedTable *par_table = (PartitionedTable *)exec_space;
      unsigned int partition_num = par_table->get_real_partition_num();
      for (; i < partition_num; ++i) {
        space = par_table->get_partition(i);
        ExecuteNode *fetch_node = plan->get_fetch_node(space, sql);
        dispatch_packet_node->add_child(fetch_node);
      }
      plan->session->set_execute_plan_touch_partition_nums(-1);
    }
  }
  plan->set_start_node(dispatch_packet_node);
}

void Statement::assemble_mul_par_modify_plan(ExecutePlan *plan,
                                             vector<unsigned int> *par_ids,
                                             PartitionedTable *par_table) {
  LOG_DEBUG("Assemble mul par modify plan [%d] partitions of [%s].\n",
            par_ids->size(), par_table->get_name());
  DataSpace *space = NULL;
  map<DataSpace *, const char *> spaces_map;

  // map to mark how many SQLs each dataspace executed.
  // 3 means [CREATE DB stmt + USE DB stmt + CREATE TB stmt]
  // 2 means [CREATE DB stmt + CREATE TB stmt]
  // 1 means others
  map<DataSpace *, int> sql_count_map;
  vector<unsigned int>::iterator it = par_ids->begin();
  bool need_handle_create_database_ok = false;
  for (; it != par_ids->end(); ++it) {
    space = par_table->get_partition(*it);
    if (!space->get_virtual_machine_id()) {
      if (st.type == STMT_CREATE_TB) {
        table_link *table = one_table_node.only_one_table ? one_table_node.table
                                                          : par_tables[0];
        const char *schema_name =
            table->join->schema_name ? table->join->schema_name : schema;
        string new_sql = Backend::instance()->fetch_create_db_sql_from_metadata(
            plan->session, schema_name, true, NULL);
        int has_use = 0;
        if (plan->session->get_schema() &&
            strcmp(plan->session->get_schema(), "")) {
          if (!lower_case_compare(plan->session->get_schema(), schema_name)) {
            /*Do the use schema only for the current schema is the created
             * schema.*/
            new_sql.append(";USE `");
            new_sql.append(plan->session->get_schema());
            new_sql.append("`");
            has_use = 1;
          }
        } else {
          new_sql.append(";USE `");
          new_sql.append(DEFAULT_LOGIN_SCHEMA);
          new_sql.append("`");
          has_use = 1;
        }
        new_sql.append(";");
        new_sql.append(sql);
        record_shard_sql_str(new_sql);
        spaces_map[space] = get_last_shard_sql();
        sql_count_map[space] = 2 + has_use;
        LOG_DEBUG(
            "create table stmt changed from [%s] to [%s] with sqls num %d\n",
            sql, new_sql.c_str(), sql_count_map[space]);
        need_handle_create_database_ok = true;
      } else {
        spaces_map[space] = sql;
        sql_count_map[space] = 1;
      }
    } else {
      string new_sql_tmp;
      adjust_virtual_machine_schema(space->get_virtual_machine_id(),
                                    space->get_partition_id(), sql, schema,
                                    get_latest_stmt_node(),
                                    record_scan_all_table_spaces, new_sql_tmp);
      if (st.type == STMT_CREATE_TB) {
        string new_schema_tmp;
        table_link *table = one_table_node.only_one_table ? one_table_node.table
                                                          : par_tables[0];
        adjust_shard_schema(table->join->schema_name, schema, new_schema_tmp,
                            space->get_virtual_machine_id(),
                            space->get_partition_id());
        string new_sql = Backend::instance()->fetch_create_db_sql_from_metadata(
            plan->session,
            table->join->schema_name ? table->join->schema_name : schema, true,
            new_schema_tmp.c_str());
        new_sql.append(";");
        new_sql.append(new_sql_tmp.c_str());
        record_shard_sql_str(new_sql);
        spaces_map[space] = get_last_shard_sql();
        sql_count_map[space] = 2;
        need_handle_create_database_ok = true;
      } else {
        record_shard_sql_str(new_sql_tmp);
        spaces_map[space] = get_last_shard_sql();
        sql_count_map[space] = 1;
      }
    }
  }
  if (Backend::instance()->need_deal_with_metadata(st.type, &st))
    st.need_apply_metadata = judge_need_apply_meta_datasource(spaces_map);
  ExecuteNode *node = plan->get_mul_modify_node(
      spaces_map, need_handle_create_database_ok, &sql_count_map);
  if (!is_cross_node_join()) plan->session->reset_table_without_given_schema();
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(node);

  if (st.type == STMT_UPDATE || st.type == STMT_DELETE ||
      st.type == STMT_INSERT || st.type == STMT_REPLACE) {
    if (par_ids->size() == par_table->get_real_partition_num()) {
      plan->session->set_execute_plan_touch_partition_nums(-1);
    } else {
      plan->session->set_execute_plan_touch_partition_nums(par_ids->size());
    }
  }
}

void Statement::assemble_mul_part_insert_plan(ExecutePlan *plan,
                                              PartitionedTable *part_table) {
  LOG_DEBUG("Assemble mul partition insert plan [%d] partitions of [%s].\n",
            multi_insert_id_sqls.size(), part_table->get_name());
  DataSpace *dataspace = NULL;
  map<DataSpace *, const char *> spaces_map;

  if (close_cross_node_transaction) {
    LOG_ERROR(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0\n");
    throw Error(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0");
  }

  map<unsigned int, string>::iterator it = multi_insert_id_sqls.begin();
  for (; it != multi_insert_id_sqls.end(); ++it) {
    dataspace = part_table->get_partition(it->first);
    const char *used_sql =
        adjust_stmt_sql_for_shard(dataspace, it->second.c_str());
    spaces_map[dataspace] = used_sql;
  }
  ExecuteNode *node = plan->get_mul_modify_node(spaces_map);
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(node);
  if (multi_insert_id_sqls.size() == part_table->get_real_partition_num()) {
    plan->session->set_execute_plan_touch_partition_nums(-1);
  } else {
    plan->session->set_execute_plan_touch_partition_nums(
        multi_insert_id_sqls.size());
  }
}

bool Statement::prepare_two_phase_sub_sql(vector<string *> **columns,
                                          table_link *modify_table) {
  bool can_quick;
  if (st.type == STMT_UPDATE || st.type == STMT_DELETE)
    *columns = new vector<string *>();
  try {
    can_quick = generate_two_phase_modify_sub_sqls(*columns, modify_table);
  } catch (exception &e) {
    vector<string *>::iterator it = (*columns)->begin();
    for (; it != (*columns)->end(); ++it) {
      delete *it;
    }
    (*columns)->clear();
    delete *columns;
    throw;
  }
  return can_quick;
}

void build_simple_two_phase_plan(ExecutePlan *plan, DataSpace *modify_space,
                                 stmt_node *st, vector<ExecuteNode *> *nodes,
                                 vector<string *> *columns, bool can_quick,
                                 const char *modify_sub_sql,
                                 bool is_replace_set_value = false) {
  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  ExecuteNode *update_select;

  if (st->type == STMT_INSERT_SELECT || st->type == STMT_REPLACE_SELECT) {
    update_select = plan->get_insert_select_node(modify_sub_sql, nodes);
  } else {
    update_select = plan->get_modify_select_node(
        modify_sub_sql, columns, can_quick, nodes, is_replace_set_value);
  }
  ok_node->add_child(ok_merge_node);
  ok_merge_node->add_child(update_select);

  ExecuteNode *modify_node = plan->get_modify_node(modify_space);
  update_select->add_child(modify_node);

  plan->set_start_node(ok_node);
}

void Statement::assemble_two_phase_insert_part_select_part_plan(
    ExecutePlan *plan, table_link *modify_table, vector<unsigned int> *key_pos,
    PartitionedTable *par_table_select, vector<unsigned int> *par_ids) {
  const char *schema_name = modify_table->join->schema_name
                                ? modify_table->join->schema_name
                                : schema;
  const char *table_name = modify_table->join->table_name;

  LOG_DEBUG(
      "Assemble two phase insert partition select partition plan of table "
      "[%s.%s].\n",
      schema_name, table_name);

  Backend *backend = Backend::instance();

#ifdef DEBUG
  bool is_par_table = backend->table_is_partitioned(schema_name, table_name);
  bool is_dup_table = backend->table_is_duplicated(schema_name, table_name);
  ACE_ASSERT(is_par_table || is_dup_table);
#endif
  DataSpace *dspace =
      backend->get_data_space_for_table(schema_name, table_name);

  bool is_duplicated = false;
  PartitionedTable *par_table = NULL;
  if (((Table *)dspace)->is_duplicated()) {
    is_duplicated = true;
    par_table = ((DuplicatedTable *)dspace)->get_partition_table();
  } else {
    if (close_cross_node_transaction) {
      LOG_ERROR(
          "Refuse to execute cross node transaction when "
          "close_cross_node_transaction != 0\n");
      throw Error(
          "Refuse to execute cross node transaction when "
          "close_cross_node_transaction != 0");
    }
    par_table = (PartitionedTable *)dspace;
  }

  // columns will be deleted by the clean of node.
  vector<string *> *columns = NULL;
  prepare_two_phase_sub_sql(&columns, modify_table);
  handle_insert_select_hex_column();

  vector<ExecuteNode *> nodes;
  record_scan *select_rs = NULL;

  int load_insert =
      plan->session->get_session_option("use_load_data_for_insert_select")
          .int_val;
  if (!is_duplicated && st.type == STMT_INSERT_SELECT &&
      !plan->statement->is_cross_node_join() &&
      auto_inc_status == NO_AUTO_INC_FIELD &&
      (load_insert == FIFO_INSERT_SELECT ||
       load_insert == LOAD_INSERT_SELECT)) {
    ExecuteNode *parent_node = NULL;
    if (load_insert == FIFO_INSERT_SELECT) {
      LOG_DEBUG("Assemble INSERT SELECT from partition table plan via fifo.\n");
      prepare_insert_select_via_fifo(plan, NULL, schema_name, table_name, true);
    } else if (load_insert == LOAD_INSERT_SELECT) {
      LOG_DEBUG("Assemble INSERT SELECT from partition table plan via LOAD.\n");
      prepare_insert_select_via_load(plan, schema_name, table_name);
    }
    select_rs = generate_select_plan_nodes(plan, par_ids, par_table_select,
                                           &nodes, select_sub_sql.c_str());
    if (select_rs &&
        check_can_pushdown_for_modify_select(*par_table_select, select_rs,
                                             nodes, par_table, NULL)) {
      LOG_DEBUG("Found a pushdown modify select sql\n");
      free_re_parser_list();
      st.scanner = get_stmt_node()->scanner;
      plan->statement = this;
      assemble_modify_node_according_to_fetch_node(*plan, nodes, par_table);
      clean_up_execute_nodes(&nodes);
      return;
    }
    if (load_insert == FIFO_INSERT_SELECT)
      parent_node = plan->get_into_outfile_node();
    else if (load_insert == LOAD_INSERT_SELECT)
      parent_node =
          plan->get_load_select_partition_node(schema_name, table_name);
    size_t nodes_num = nodes.size();
    for (size_t i = 0; i < nodes_num; ++i) {
      parent_node->add_child(nodes[i]);
    }
    plan->set_start_node(parent_node);
    return;
  }

  ExecuteNode *update_select = NULL;
  select_rs = generate_select_plan_nodes(plan, par_ids, par_table_select,
                                         &nodes, select_sub_sql.c_str());
  if (select_rs && check_can_pushdown_for_modify_select(
                       *par_table_select, select_rs, nodes, par_table, NULL)) {
    LOG_DEBUG("Found a pushdown modify select sql\n");
    free_re_parser_list();
    assemble_modify_node_according_to_fetch_node(*plan, nodes, par_table);
    clean_up_execute_nodes(&nodes);
    return;
  }

  update_select = plan->get_par_insert_select_node(
      modify_sub_sql.c_str(), par_table, par_table->get_partition_method(),
      *key_pos, &nodes, schema_name, table_name, is_duplicated);

  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  ok_node->add_child(ok_merge_node);
  ok_merge_node->add_child(update_select);

  /*For modify normal table, we add modify node directly; for modify partition
   * table, we add modify node dynamically during execution.*/

  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(ok_node);
}
void Statement::assemble_two_phase_modify_partition_plan(
    ExecutePlan *plan,
    DataSpace *space,  // only for insert_select
    PartitionMethod *method, table_link *modify_table,
    vector<unsigned int> *key_pos,  // only for insert_select
    vector<unsigned int> *par_ids)  // only for update/delete_select
{
  LOG_DEBUG("Assemble two phase modify partition plan of table [%s].\n",
            modify_table->join->table_name);

  const char *schema_name = modify_table->join->schema_name
                                ? modify_table->join->schema_name
                                : schema;
  const char *table_name = modify_table->join->table_name;

  Backend *backend = Backend::instance();

#ifdef DEBUG
  bool is_par_table = backend->table_is_partitioned(schema_name, table_name);
  bool is_dup_table = backend->table_is_duplicated(schema_name, table_name);
  ACE_ASSERT(is_par_table || is_dup_table);
#endif
  DataSpace *dspace =
      backend->get_data_space_for_table(schema_name, table_name);

  bool is_duplicated = false;
  PartitionedTable *par_table = NULL;
  if (((Table *)dspace)->is_duplicated()) {
    is_duplicated = true;
    par_table = ((DuplicatedTable *)dspace)->get_partition_table();
  } else {
    if (close_cross_node_transaction) {
      LOG_ERROR(
          "Refuse to execute cross node transaction when "
          "close_cross_node_transaction != 0\n");
      throw Error(
          "Refuse to execute cross node transaction when "
          "close_cross_node_transaction != 0");
    }
    par_table = (PartitionedTable *)dspace;
  }

  if (!is_duplicated) {
    int load_insert =
        plan->session->get_session_option("use_load_data_for_insert_select")
            .int_val;
    if (st.type == STMT_INSERT_SELECT && load_insert == FIFO_INSERT_SELECT &&
        auto_inc_status == NO_AUTO_INC_FIELD &&
        !plan->statement->is_union_table_sub() &&
        !plan->statement->is_cross_node_join() &&
        !plan->session->is_in_explain()) {
      vector<ExecuteNode *> nodes;
      LOG_DEBUG("Assemble INSERT SELECT from partition table plan via fifo.\n");
      prepare_insert_select_via_fifo(plan, NULL, schema_name, table_name, true);
      const char *used_sql =
          adjust_stmt_sql_for_shard(space, select_sub_sql.c_str());
      if (used_sql == select_sub_sql.c_str())
        used_sql = adjust_stmt_sql_for_alias(space, select_sub_sql.c_str());
      ExecuteNode *fetch_node = plan->get_fetch_node(space, used_sql);
      ExecuteNode *send_node = plan->get_into_outfile_node();

      send_node->add_child(fetch_node);
      plan->set_start_node(send_node);
      return;
    } else if (st.type == STMT_INSERT_SELECT &&
               load_insert == LOAD_INSERT_SELECT &&
               auto_inc_status == NO_AUTO_INC_FIELD &&
               !plan->statement->is_union_table_sub() &&
               !plan->statement->is_cross_node_join() &&
               !plan->session->is_in_explain()) {
      vector<ExecuteNode *> nodes;
      LOG_DEBUG("Assemble INSERT SELECT from partition table plan via LOAD.\n");
      prepare_insert_select_via_load(plan, schema_name, table_name);
      const char *used_sql =
          adjust_stmt_sql_for_shard(space, select_sub_sql.c_str());
      if (used_sql == select_sub_sql.c_str())
        used_sql = adjust_stmt_sql_for_alias(space, select_sub_sql.c_str());

      ExecuteNode *fetch_node = plan->get_fetch_node(space, used_sql);
      ExecuteNode *load_node =
          plan->get_load_select_partition_node(schema_name, table_name);
      load_node->add_child(fetch_node);
      plan->set_start_node(load_node);
      return;
    }
  }

  // columns will be deleted by the clean of node.
  vector<string *> *columns = NULL;
  bool can_quick = prepare_two_phase_sub_sql(&columns, modify_table);
  handle_insert_select_hex_column();

  if (st.type == STMT_DELETE &&
      (st.scanner->order_by_list == NULL && st.scanner->limit != NULL &&
       st.scanner->limit->offset == NULL)) {
    vector<string *>::iterator it = columns->begin();
    for (; it != columns->end(); ++it) {
      delete *it;
    }
    columns->clear();
    delete columns;
    assemble_modify_limit_partition_plan(plan, par_ids, par_table);
    return;
  }

  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();

  ExecuteNode *update_select = NULL;
  vector<ExecuteNode *> nodes;

  if (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT) {
    ExecuteNode *fetch_node;
#ifndef DBSCALE_DISABLE_SPARK
    if (spark_insert) {
      analysis_spark_sql_join(plan);
      fetch_node = spark_node;
    } else {
#endif
      /*The select part only need one fetch node to execute the select_sql
       * directly. Cause the select part of insert select stmt here, doesnot
       * contain the partition table.*/
      const char *used_sql =
          adjust_stmt_sql_for_shard(space, select_sub_sql.c_str());
      if (used_sql == select_sub_sql.c_str())
        used_sql = adjust_stmt_sql_for_alias(space, select_sub_sql.c_str());
      fetch_node = plan->get_fetch_node(space, used_sql);
#ifndef DBSCALE_DISABLE_SPARK
    }
#endif
    nodes.push_back(fetch_node);
    update_select = plan->get_par_insert_select_node(
        modify_sub_sql.c_str(), par_table, method, *key_pos, &nodes,
        schema_name, table_name, is_duplicated);
    plan->session->set_execute_plan_touch_partition_nums(-1);
  } else {
    generate_select_plan_nodes(plan, par_ids, par_table, &nodes,
                               select_sub_sql.c_str());
    update_select = plan->get_par_modify_select_node(
        modify_sub_sql.c_str(), par_table, method, columns, can_quick, &nodes);
  }
  ok_node->add_child(ok_merge_node);
  ok_merge_node->add_child(update_select);

  /*For modify normal table, we add modify node directly; for modify partition
   * table, we add modify node dynamically during execution.*/
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(ok_node);
}

void Statement::assemble_two_phase_modify_no_partition_plan(
    ExecutePlan *plan, DataSpace *modify_space, DataSpace *select_space,
    table_link *modify_table) {
  LOG_DEBUG("Assemble two phase modify no partition plan.\n");
  // columns will be deleted by the clean of node.
  vector<string *> *columns = NULL;
  bool can_quick = prepare_two_phase_sub_sql(&columns, modify_table);
  handle_insert_select_hex_column();

  ExecuteNode *fetch_node;
#ifndef DBSCALE_DISABLE_SPARK
  if (spark_insert) {
    analysis_spark_sql_join(plan);
    fetch_node = spark_node;
  } else {
#endif
    const char *used_sql =
        adjust_stmt_sql_for_shard(select_space, select_sub_sql.c_str());
    if (used_sql == select_sub_sql.c_str())
      used_sql =
          adjust_stmt_sql_for_alias(select_space, select_sub_sql.c_str());

    fetch_node = plan->get_fetch_node(select_space, used_sql);
#ifndef DBSCALE_DISABLE_SPARK
  }
#endif

  vector<ExecuteNode *> nodes;
  nodes.push_back(fetch_node);

  build_simple_two_phase_plan(plan, modify_space, &st, &nodes, columns,
                              can_quick, modify_sub_sql.c_str(),
                              is_update_set_subquery);
}

void Statement::assemble_modify_limit_partition_plan(
    ExecutePlan *plan, vector<unsigned int> *par_ids,
    PartitionedTable *par_table) {
  long num = 0;
  IntExpression *limit_num = st.scanner->limit->num;
  if (limit_num) num = get_integer_value_from_expr(limit_num);

  string tmp_sql(sql);
  ExecuteNode *node = plan->get_modify_limit_node(
      st.scanner, par_table, *par_ids, (unsigned int)num, tmp_sql);
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(node);

  LOG_DEBUG("Assemble delete limit plan [%d] partitions of [%s].\n",
            par_ids->size(), par_table->get_name());
  return;
}

void Statement::assemble_two_phase_modify_normal_plan(
    ExecutePlan *plan, vector<unsigned int> *par_ids,
    PartitionedTable *par_table, table_link *modify_table) {
  LOG_DEBUG("Assemble two phase modify normal plan.\n");
  Backend *backend = Backend::instance();
  const char *schema_name = modify_table->join->schema_name
                                ? modify_table->join->schema_name
                                : schema;
  const char *table_name = modify_table->join->table_name;
  DataSpace *modify_space =
      backend->get_data_space_for_table(schema_name, table_name);
  DataServer *modify_server =
      get_write_server_of_source(modify_space->get_data_source());

  vector<ExecuteNode *> nodes;
  record_scan *select_rs = NULL;
  do {
    if (st.type == STMT_INSERT_SELECT) {
      bool is_external_load =
          modify_server && modify_server->get_is_external_load();
      int load_insert =
          plan->session->get_session_option("use_load_data_for_insert_select")
              .int_val;
      bool should_load_insert = auto_inc_status == NO_AUTO_INC_FIELD &&
                                !plan->statement->is_cross_node_join();
      bool insert_select_use_fifo =
          should_load_insert && (load_insert == FIFO_INSERT_SELECT);
      bool insert_select_use_load =
          should_load_insert && (load_insert == LOAD_INSERT_SELECT);
      ExecuteNode *parent_node = NULL;
      if (is_external_load || insert_select_use_fifo) {
        LOG_DEBUG(
            "Assemble INSERT SELECT from partition table plan via fifo.\n");
        prepare_insert_select_via_fifo(plan, modify_server, schema_name,
                                       table_name, !is_external_load);
      } else if (insert_select_use_load) {
        LOG_DEBUG(
            "Assemble INSERT SELECT from partition table plan via LOAD.\n");
        prepare_insert_select_via_load(plan, schema_name, table_name);
      } else {
        break;
      }
      select_rs = generate_select_plan_nodes(plan, par_ids, par_table, &nodes,
                                             select_sub_sql.c_str());
      if (select_rs && check_can_pushdown_for_modify_select(
                           *par_table, select_rs, nodes, NULL, modify_space)) {
        LOG_DEBUG("Found a pushdown modify select sql\n");
        free_re_parser_list();
        st.scanner = get_stmt_node()->scanner;
        plan->statement = this;
        assemble_modify_node_according_to_fetch_node(*plan, nodes, NULL);
        clean_up_execute_nodes(&nodes);
        return;
      }
      if (is_external_load || insert_select_use_fifo) {
        parent_node = plan->get_into_outfile_node();
      } else if (insert_select_use_load) {
        parent_node = plan->get_load_select_node(schema_name, table_name);
      }
      size_t nodes_num = nodes.size();
      for (size_t i = 0; i < nodes_num; ++i) {
        parent_node->add_child(nodes[i]);
      }
      plan->set_start_node(parent_node);
      return;
    }
  } while (0);

  LOG_DEBUG("Assemble two phase modify normal plan [%d] partitions of [%s].\n",
            par_ids->size(), par_table->get_name());
  // columns will be deleted by the clean of node.
  vector<string *> *columns = NULL;

  bool can_quick = prepare_two_phase_sub_sql(&columns, modify_table);
  handle_insert_select_hex_column();
  select_rs = generate_select_plan_nodes(plan, par_ids, par_table, &nodes,
                                         select_sub_sql.c_str());
  if (select_rs && check_can_pushdown_for_modify_select(
                       *par_table, select_rs, nodes, NULL, modify_space)) {
    LOG_DEBUG("Found a pushdown modify select sql\n");
    free_re_parser_list();
    assemble_modify_node_according_to_fetch_node(*plan, nodes, par_table);
    clean_up_execute_nodes(&nodes);
    return;
  }

  build_simple_two_phase_plan(plan, modify_space, &st, &nodes, columns,
                              can_quick, modify_sub_sql.c_str());
}

void Statement::check_load_local_infile(ExecutePlan *plan) {
  if (plan->session->get_local_infile() == false) {
    throw dbscale::sql::SQLError(
        "The used command is not allowed with this MySQL version", "42000",
        1148);
  }
}

void Statement::assemble_load_local_plan(ExecutePlan *plan,
                                         DataSpace *dataspace) {
  check_load_local_infile(plan);
  ExecuteNode *load_node = plan->get_load_local_node(dataspace, sql);
  plan->session->set_load_data_local_command(true);
  plan->set_start_node(load_node);
}

void Statement::assemble_kill_plan(ExecutePlan *plan, int killed_cluster_id,
                                   uint32_t kid) {
  ExecuteNode *kill_node = plan->get_kill_node(killed_cluster_id, kid);
  plan->set_start_node(kill_node);
}

void Statement::assemble_load_data_infile_external_plan(
    ExecutePlan *plan, DataSpace *dataspace, DataServer *dataserver) {
  ExecuteNode *load_node =
      plan->get_load_data_infile_external_node(dataspace, dataserver);
  plan->set_start_node(load_node);
}

void Statement::assemble_load_local_external_plan(ExecutePlan *plan,
                                                  DataSpace *dataspace,
                                                  DataServer *dataserver) {
  ExecuteNode *load_node =
      plan->get_load_local_external_node(dataspace, dataserver, sql);
  plan->session->set_load_data_local_command(true);
  plan->set_start_node(load_node);
}

void Statement::assemble_partition_load_local_plan(ExecutePlan *plan,
                                                   PartitionedTable *par_table,
                                                   const char *schema_name,
                                                   const char *table_name) {
  LOG_DEBUG("assemble partition_load_local plan for table [%s.%s].\n",
            schema_name, table_name);
  check_load_local_infile(plan);
  ExecuteNode *load_node = plan->get_load_local_part_table_node(
      par_table, sql, schema_name, table_name);
  plan->session->set_load_data_local_command(true);
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(load_node);
}
void Statement::assemble_partition_load_data_infile_plan(
    ExecutePlan *plan, PartitionedTable *par_table, const char *schema_name,
    const char *table_name) {
  ExecuteNode *load_node = plan->get_load_data_infile_part_table_node(
      par_table, sql, schema_name, table_name);
  plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan->set_start_node(load_node);
}

void Statement::assemble_load_data_infile_plan(ExecutePlan *plan,
                                               DataSpace *dataspace) {
  ExecuteNode *load_node = plan->get_load_data_infile_node(dataspace, sql);
  plan->set_start_node(load_node);
}

void Statement::assemble_dbscale_set_plan(ExecutePlan *plan) {
  ExecuteNode *dynamic_configuration_node =
      plan->get_dynamic_configuration_node();
  plan->set_start_node(dynamic_configuration_node);
}

void Statement::assemble_dbscale_set_rep_strategy_plan(ExecutePlan *plan) {
  ExecuteNode *rep_strategy_node = plan->get_rep_strategy_node();
  plan->set_start_node(rep_strategy_node);
}

void Statement::assemble_dbscale_set_server_weight(ExecutePlan *plan) {
  ExecuteNode *set_server_weight_node = plan->get_set_server_weight_node();
  plan->set_start_node(set_server_weight_node);
}

void Statement::assemble_show_option_plan(ExecutePlan *plan) {
  ExecuteNode *show_option_node = plan->get_show_option_node();
  plan->set_start_node(show_option_node);
}

void Statement::assemble_show_dynamic_option_plan(ExecutePlan *plan) {
  ExecuteNode *show_dynamic_option_node = plan->get_show_dynamic_option_node();
  plan->set_start_node(show_dynamic_option_node);
}

void Statement::assemble_change_startup_config_plan(ExecutePlan *plan) {
  ExecuteNode *change_startup_config_node =
      plan->get_change_startup_config_node();
  plan->set_start_node(change_startup_config_node);
}

void Statement::assemble_show_changed_startup_config_plan(ExecutePlan *plan) {
  ExecuteNode *show_changed_startup_config_node =
      plan->get_show_changed_startup_config_node();
  plan->set_start_node(show_changed_startup_config_node);
}

void Statement::assemble_dbscale_clean_monitor_point_plan(ExecutePlan *plan) {
  ExecuteNode *clean_node = plan->get_dbscale_clean_monitor_point_node();
  plan->set_start_node(clean_node);
}

void Statement::assemble_dbscale_purge_monitor_point_plan(ExecutePlan *plan) {
  ExecuteNode *purge_node = plan->get_dbscale_purge_monitor_point_node();
  plan->set_start_node(purge_node);
}

void Statement::assemble_dbscale_get_global_consistence_point_plan(
    ExecutePlan *plan) {
  if (!multiple_mode ||
      (multiple_mode && MultipleManager::instance()->get_is_cluster_master())) {
    ExecuteNode *consistence_node =
        plan->get_dbscale_global_consistence_point_node();
    plan->set_start_node(consistence_node);
  } else {
    assemble_forward_master_role_plan(plan, true);
  }
}
void Statement::assemble_show_engine_lock_waiting_status_plan(ExecutePlan *plan,
                                                              int engine_type) {
  ExecuteNode *node = plan->get_show_engine_lock_waiting_node(engine_type);
  plan->set_start_node(node);
}
void Statement::assemble_show_dataserver_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show dataserver plan\n");
  ExecuteNode *node = plan->get_show_dataserver_node();
  plan->set_start_node(node);
}

void Statement::assemble_show_backend_threads_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show backend threads plan\n");
  ExecuteNode *node = plan->get_show_backend_threads_node();
  plan->set_start_node(node);
}

void Statement::assemble_show_partition_scheme_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show partition scheme plan\n");
  ExecuteNode *node = plan->get_show_partition_scheme_node();
  plan->set_start_node(node);
}

void Statement::assemble_show_schema_plan(ExecutePlan *plan,
                                          const char *schema_name) {
  LOG_DEBUG("Assemble show schema plan\n");
  ExecuteNode *node = plan->get_show_schema_node(schema_name);
  plan->set_start_node(node);
}
void Statement::assemble_show_table_plan(ExecutePlan *plan, const char *schema,
                                         const char *table, bool use_like) {
  LOG_DEBUG("Assemble show table dataspace plan\n");
  ExecuteNode *node = plan->get_show_table_node(schema, table, use_like);
  plan->set_start_node(node);
}

void Statement::assemble_show_migrate_clean_tables_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show migrate clean tables plan\n");
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master()) {
    LOG_ERROR("slave dbscale do not migrate table\n");
    throw NotSupportedError("slave dbscale do not support migrate table.");
  }
  Backend *backend = Backend::instance();
  DataSpace *data_space = backend->get_config_data_space();

  ExecuteNode *node = plan->get_direct_execute_node(data_space, sql);
  plan->set_start_node(node);
}

void Statement::assemble_migrate_clean_tables_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble migrate clean tables plan\n");
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master()) {
    LOG_ERROR("slave dbscale do not migrate table\n");
    throw NotSupportedError("slave dbscale do not support migrate table.");
  }

  const char *migrate_id = st.sql->migrate_clean_oper->migrate_id;
  ExecuteNode *node = plan->get_migrate_clean_node(migrate_id);
  plan->set_start_node(node);
}

bool check_need_show_weight(DataSource *datasource) {
  DataSourceType datasource_type = datasource->get_data_source_type();
  if (datasource_type == DATASOURCE_TYPE_REPLICATION ||
      datasource_type == DATASOURCE_TYPE_RWSPLIT) {
    DataSource *lb_ds =
        ((ReplicationDataSource *)datasource)->get_read_source();
    SchedulePolicy type =
        ((LoadBalanceDataSource *)lb_ds)->get_scheduler_type();
    if (type == SCHEDULE_POLICY_WEIGHT) {
      return true;
    }
  }
  if (datasource_type == DATASOURCE_TYPE_LOAD_BALANCE) {
    SchedulePolicy type =
        ((LoadBalanceDataSource *)datasource)->get_scheduler_type();
    if (type == SCHEDULE_POLICY_WEIGHT) {
      return true;
    }
  }
  return false;
}

void Statement::assemble_show_datasource_plan(ExecutePlan *plan) {
  const char *name = st.sql->show_datasource_oper->data_source_name;
  const char *type_name = st.sql->show_datasource_oper->data_source_type;
  list<const char *> names;
  list<DataSource *> type_data_sources;
  Backend *backend = Backend::instance();
  bool need_show_weight = false;
  if (type_name) {
    int type = -1;
    if (!strcasecmp(type_name, "SERVER"))
      type = DATASOURCE_TYPE_SERVER;
    else if (!strcasecmp(type_name, "RWSPLIT"))
      type = DATASOURCE_TYPE_RWSPLIT;
    else if (!strcasecmp(type_name, "LOAD_BALANCE"))
      type = DATASOURCE_TYPE_LOAD_BALANCE;
    else if (!strcasecmp(type_name, "REPLICATION"))
      type = DATASOURCE_TYPE_REPLICATION;
    else if (!strcasecmp(type_name, "SHARE_DISK"))
      type = DATASOURCE_TYPE_SHARE_DISK;
    else if (!strcasecmp(type_name, "READ_ONLY"))
      type = DATASOURCE_TYPE_READ_ONLY;
    else if (!strcasecmp(type_name, "MGR"))
      type = DATASOURCE_TYPE_MGR;
    else {
      LOG_DEBUG("Unknown DataSource type.\n");
      throw UnknownDataSourceException("Unknown DataSource type.");
    }
    backend->find_data_source_by_type(type, type_data_sources);
    if (!type_data_sources.empty()) {
      list<DataSource *>::iterator it = type_data_sources.begin();
      for (; it != type_data_sources.end(); ++it) {
        if (!need_show_weight) need_show_weight = check_need_show_weight((*it));
        names.push_back((*it)->get_name());
      }
    } else {
      LOG_DEBUG("No DataSource of the type.\n");
      throw UnknownDataSourceException("No DataSource of the type.");
    }
  } else {
    names.push_back(name);
    DataSource *datasource = backend->find_data_source(name);
    if (datasource) need_show_weight = check_need_show_weight(datasource);
  }
  ExecuteNode *datasource_node =
      plan->get_show_data_source_node(names, need_show_weight);
  plan->set_start_node(datasource_node);
}

void Statement::assemble_dbscale_show_partition_plan(ExecutePlan *plan) {
  ExecuteNode *partition_node = plan->get_show_partition_node();
  plan->set_start_node(partition_node);
}

void Statement::assemble_dbscale_show_user_sql_count_plan(ExecutePlan *plan,
                                                          const char *user_id) {
  LOG_DEBUG("Assemble show user sql count plan.\n");
  ExecuteNode *count_node = plan->get_show_user_sql_count_node(user_id);
  plan->set_start_node(count_node);
}

void Statement::assemble_dbscale_show_auto_increment_info(ExecutePlan *plan) {
  LOG_DEBUG("assemble_dbscale_show_auto_increment_info\n");
  join_node *node = st.sql->auto_increment_info_oper->table;
  const char *table_name = node->table_name;
  const char *schema_name =
      node->schema_name ? node->schema_name : plan->session->get_schema();
  Backend *backend = Backend::instance();
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)space)->is_partitioned()) {
    ExecuteNode *node =
        plan->get_show_auto_increment_info_node((PartitionedTable *)space);
    plan->set_start_node(node);
  } else {
    LOG_ERROR(
        "Not find this partition table [%s.%s] to show auto_increment info\n",
        schema_name, table_name);
    throw Error("Not a partitioned table to show auto_increment info.");
  }
}

void Statement::assemble_dbscale_set_auto_increment_offset(ExecutePlan *plan) {
  LOG_DEBUG("assemble_dbscale_set_auto_increment_offset\n");
  join_node *node = st.sql->auto_increment_info_oper->table;
  const char *table_name = node->table_name;
  const char *schema_name =
      node->schema_name ? node->schema_name : plan->session->get_schema();

  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);
  if (!Backend::instance()->has_auto_increment_field(full_table_name)) {
    LOG_ERROR("Not find table [%s.%s] has auto_increment info\n", schema_name,
              table_name);
    throw Error("Not find table has auto_increment info.");
  }
  Backend *backend = Backend::instance();
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)space)->is_partitioned()) {
    ExecuteNode *node =
        plan->get_set_auto_increment_offset_node((PartitionedTable *)space);
    plan->set_start_node(node);
  } else {
    LOG_ERROR(
        "Not find this partition table [%s.%s] to set_auto_increment_offset.\n",
        schema_name, table_name);
    throw Error("Not a partitioned table to set_auto_increment_offset.");
  }
}
void Statement::assemble_dbscale_show_virtual_map_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale show virtual map plan\n");
  if (!st.table_list_head)  // statemet does't contains table
    throw Error("Not find table name.");
  table_link *link = st.table_list_head;
  join_node *node = link->join;
  const char *table_name = node->table_name;
  const char *schema_name =
      node->schema_name ? node->schema_name : plan->session->get_schema();
  Backend *backend = Backend::instance();
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)space)->is_partitioned()) {
    ExecuteNode *node = NULL;
    if (st.type == STMT_DBSCALE_SHOW_VIRTUAL_MAP)
      node = plan->get_show_virtual_map_node((PartitionedTable *)space);
    if (st.type == STMT_DBSCALE_SHOW_SHARD_MAP)
      node = plan->get_show_shard_map_node((PartitionedTable *)space);
    plan->set_start_node(node);
  } else {
    LOG_ERROR("Not find this partition table [%s.%s]\n", schema_name,
              table_name);
    throw Error("Not find this partition table.");
  }
}

void Statement::assemble_dbscale_mul_sync_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale mul sync plan\n");
  const char *sync_topic = st.sql->mul_sync_oper->topic_name;
  const char *sync_param = st.sql->mul_sync_oper->sync_param;
  const char *sync_cond = st.sql->mul_sync_oper->sync_cond;
  const char *sync_state = st.sql->mul_sync_oper->sync_state;
  unsigned long version_id = st.sql->mul_sync_oper->version_id;
  ExecuteNode *node = plan->get_dbscale_mul_sync_node(
      sync_topic, sync_state, sync_param, sync_cond, version_id);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_status_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale show status plan\n");
  ExecuteNode *node = plan->get_show_status_node();
  plan->set_start_node(node);
}

void Statement::assemble_show_catchup_plan(ExecutePlan *plan,
                                           const char *source_name) {
  LOG_DEBUG("assemble dbscale show catchup plan.\n");
  ExecuteNode *node = plan->get_show_catchup_node(source_name);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_skip_wait_catchup_plan(
    ExecutePlan *plan, const char *source_name) {
  LOG_DEBUG("Assemble dbscale force kill catchup plan.\n");
  ExecuteNode *node = plan->get_dbscale_skip_wait_catchup_node(source_name);
  plan->set_start_node(node);
}

void Statement::assemble_show_transaction_sqls_plan(ExecutePlan *plan) {
  ExecuteNode *show_transaction_sqls_node =
      plan->get_show_transaction_sqls_node();
  plan->set_start_node(show_transaction_sqls_node);
}

void Statement::assemble_federated_thread_plan(ExecutePlan *plan,
                                               const char *table_name) {
  LOG_DEBUG("Start assemble federated thread plan [%s].\n", sql);
  Backend *backend = Backend::instance();
  DataSpace *send_space =
      backend->get_data_space_for_table(TMP_TABLE_SCHEMA, table_name);
  DataSource *send_source = send_space->get_data_source();
  CrossNodeJoinManager *cross_join_manager =
      backend->get_cross_node_join_manager();
  Session *work_session = NULL;
  if (send_source) {
    work_session = cross_join_manager->get_work_session(table_name);
  } else {
    work_session =
        cross_join_manager->get_federated_table_work_session(table_name);
  }
  if (!work_session) {
    LOG_ERROR(
        "Fail to get work session from join manager for table [%s] in assemble "
        "federated thread plan.\n",
        table_name);
    throw Error("Fail to get work session from join manager.");
  }
  plan->session->get_session_option("cross_node_join_method").int_val = 1;
  plan->session->get_session_option("max_federated_cross_join_rows").ulong_val =
      work_session->get_session_option("max_federated_cross_join_rows")
          .ulong_val;
  LOG_DEBUG("Federated table [%s] get work session [%@].\n", table_name,
            work_session);
  TransferRuleManager *rule_manager = work_session->get_transfer_rule_manager();
  TransferRule *rule =
      rule_manager->get_transfer_rule_by_remote_table_name(table_name);
  DispatchStrategy *dispatch_strategy = rule->get_dispatch_strategy();
  string execution_plan_sql = rule->get_execution_plan_sql();
  DataSpace *exec_space = rule->get_data_space();
  DataSource *exec_source = exec_space->get_data_source();
  LOG_DEBUG("Get federated execute sql [%s].\n", execution_plan_sql.c_str());
  plan->set_federated(true);
  if (send_source && exec_source) {
    ExecuteNode *node =
        plan->get_direct_execute_node(exec_space, execution_plan_sql.c_str());
    plan->set_start_node(node);
    return;
  }
  if (!send_source) {
    if (send_space->is_duplicated())
      send_space = ((DuplicatedTable *)send_space)->get_partition_table();
    vector<const char *> *key_names =
        ((PartitionedTable *)send_space)->get_key_names();
    PartitionMethod *method =
        ((PartitionedTable *)send_space)->get_partition_method();
    dispatch_strategy->method = method;
    dispatch_strategy->key_names = key_names;
    dispatch_strategy->part_table = (PartitionedTable *)send_space;
  }

  assemble_dispatch_packet_plan(plan, rule, exec_space, send_space,
                                execution_plan_sql.c_str(), work_session);
}

bool groupby_on_par_key(order_item *groupby, const char *schema_name,
                        const char *table_name, const char *table_alias,
                        const char *key) {
  field_item *item = groupby->field;
  Expression *field_expr = item->field_expr;
  if (field_expr->type == EXPR_STR) {
    const char *str = ((StrExpression *)field_expr)->str_value;
    if (table_name && column_name_equal_key_alias(
                          str, schema_name, strlen(schema_name), table_name,
                          strlen(table_name), key, table_alias))
      return true;
  }
  return false;
}

/* Whether the group by clause of record scan contains all the partition keys
 * of the partition table of this record scan.
 *
 * TODO: currently if there are several partition tables, we will only use the
 * partition keys of the first partition table. Fix it.*/
bool Statement::group_by_all_keys(record_scan *rs) {
  if (!(record_scan_par_table_map.count(rs) ||
        (one_table_node.only_one_table && one_table_node.rs == rs)) ||
      !rs->group_by_list)
    return false;
  table_link *par_table = one_table_node.only_one_table
                              ? one_table_node.table
                              : record_scan_par_table_map[rs];
  const char *schema_name =
      par_table->join->schema_name ? par_table->join->schema_name : schema;
  const char *table_name = par_table->join->table_name;
  const char *table_alias = par_table->join->alias;
  PartitionedTable *par_space =
      (PartitionedTable *)(one_table_node.only_one_table
                               ? one_table_node.space
                               : record_scan_all_table_spaces[par_table]);
  vector<const char *> *key_names = par_space->get_key_names();

  vector<const char *>::iterator it_keys;
  for (it_keys = key_names->begin(); it_keys != key_names->end(); ++it_keys) {
    bool find_key = false;
    order_item *item = rs->group_by_list;
    if (item) {
      do {
        if (groupby_on_par_key(item, schema_name, table_name, table_alias,
                               *it_keys)) {
          find_key = true;
          break;
        }
        item = item->next;
      } while (item != rs->group_by_list);
    }
    if (!find_key) return false;
  }

  return true;
}

ExecuteNode *Statement::generate_order_by_subplan(
    ExecutePlan *plan, record_scan *rs, list<AggregateDesc> &aggr_list,
    list<AvgDesc> &avg_list, ExecuteNode **tail_node) {
  ExecuteNode *top_node = NULL;
  ExecuteNode *bottom_node = NULL;
  string new_sql;

  string original_sql(fetch_sql_tmp);
  const char *query_sql = original_sql.c_str();

  /* If the groupby list contains all partition keys, all the records related
   * to the same group will located in only one partition. In that case, we
   * can consider the groupby as a orderby, so that we can ignore the
   * aggregate functions and push order by and limit clauses to the fetch
   * nodes. Finally, we can get a better performance.*/

  // generate new SQL
  new_sql.append(query_sql + rs->start_pos - 1, rs->from_pos - rs->start_pos);
#ifndef DBSCALE_TEST_DISABLE
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "reparse_sql") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "wrong_from_pos")) {
    new_sql.append("   ");
    LOG_DEBUG("test reparse_sql with wrong_from_pos\n");
  }
#endif

  if (rs->group_by_list) {
    if (!groupby_all_par_keys) {
      ExecuteNode *group_node = plan->get_group_node(&group_by_list, aggr_list);
      bottom_node = group_node;
      top_node = group_node;
      if (!avg_list.empty()) {
        ExecuteNode *avg_node = plan->get_avg_node(avg_list);
        avg_node->add_child(top_node);
        top_node = avg_node;
      }
      if (!select_expr_list.empty()) {
        ExecuteNode *calculate_expr_node =
            plan->get_expr_calculate_node(select_expr_list);
        calculate_expr_node->add_child(top_node);
        top_node = calculate_expr_node;
      }

      /* group by with having with aggregate function */
      if (rs->having) {
        if (!simple_having) {  // having with aggregate function
          ExecuteNode *having_node = NULL;
          having_node = plan->get_expr_filter_node(this->having);
          having_node->add_child(top_node);
          top_node = having_node;
          new_sql.append(query_sql + rs->from_pos - 1,
                         rs->having_pos - rs->from_pos);
          if (rs->order_by_list) {
            if (!order_in_group_flag) {
              ExecuteNode *order_node =
                  plan->get_single_sort_node(&order_by_list);
              order_node->add_child(top_node);
              top_node = order_node;
            }
          }
        } else {  // with having which can push down
          if (rs->order_by_list) {
            new_sql.append(query_sql + rs->from_pos - 1,
                           rs->order_pos - rs->from_pos);
            if (!order_in_group_flag) {
              ExecuteNode *order_node =
                  plan->get_single_sort_node(&order_by_list);
              order_node->add_child(top_node);
              top_node = order_node;
            }
          } else if (rs->limit) {
            new_sql.append(query_sql + rs->from_pos - 1,
                           rs->limit_pos - rs->from_pos);
          } else {
            new_sql.append(query_sql + rs->from_pos - 1);
          }
        }
      } else {  // without having
        if (rs->order_by_list) {
          new_sql.append(query_sql + rs->from_pos - 1,
                         rs->order_pos - rs->from_pos);
          if (!order_in_group_flag) {
            ExecuteNode *order_node =
                plan->get_single_sort_node(&order_by_list);
            order_node->add_child(top_node);
            top_node = order_node;
          }
        } else if (rs->limit) {
          new_sql.append(query_sql + rs->from_pos - 1,
                         rs->limit_pos - rs->from_pos);
        } else {
          new_sql.append(query_sql + rs->from_pos - 1);
        }
      }
    } else {  // group by can be push down
      if (!avg_list.empty()) release_avg_list(avg_list);
      if (rs->order_by_list) {
        if (order_in_group_flag) {
          ExecuteNode *sort_node = plan->get_sort_node(&group_by_list);
          top_node = sort_node;
          bottom_node = sort_node;
        } else {
          ExecuteNode *sort_node = plan->get_sort_node(&order_by_list);
          top_node = sort_node;
          bottom_node = sort_node;
        }
      } else {
        ExecuteNode *sort_node = plan->get_sort_node(&group_by_list);
        top_node = sort_node;
        bottom_node = sort_node;
      }
      if (MYSQL_VERSION_8 ==
              Backend::instance()->get_backend_server_version() &&
          !rs->order_by_list) {
        if (rs->limit) {
          new_sql.append(query_sql + rs->from_pos - 1,
                         rs->limit_pos - rs->from_pos);
        } else {
          new_sql.append(query_sql + rs->from_pos - 1);
        }
        order_item *oi = rs->group_by_list;
        if (oi) {
          new_sql.append(" ORDER BY ");
          while (oi) {
            string s;
            oi->field->field_expr->to_string(s);
            if (oi->field->field_expr->is_plain_expression()) {
              size_t pos = 0;
              while (pos < s.size() && s.find(".", pos) != string::npos) {
                pos = s.find(".", pos);
                s.replace(pos, 1, "`.`");
                pos += 3;
              }
              new_sql.append("`").append(s).append("`, ");
            } else {
              new_sql.append(s).append(", ");
            }
            oi = oi->next;
            if (oi == rs->group_by_list) {
              new_sql.erase(new_sql.size() - 2);
              new_sql.append(" ");
              break;
            }
          }
        }
        if (rs->limit) new_sql.append(query_sql + rs->limit_pos - 1);
      } else {
        new_sql.append(query_sql + rs->from_pos - 1);
      }
    }
  } else {  // without group by
    if (rs->order_by_list) {
      ExecuteNode *sort_node = plan->get_sort_node(&order_by_list);
      top_node = sort_node;
      bottom_node = sort_node;
    }
    if (!avg_list.empty()) release_avg_list(avg_list);

    new_sql.append(query_sql + rs->from_pos - 1);
  }

  order_item *oi = rs->group_by_list;
  if (Backend::instance()->get_backend_server_version() == MYSQL_VERSION_8 &&
      oi && !groupby_all_par_keys) {
    new_sql.append(" ORDER BY ");
    while (oi) {
      string s;
      oi->field->field_expr->to_string(s);
      if (oi->field->field_expr->is_plain_expression()) {
        size_t pos = 0;
   