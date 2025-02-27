#ifndef DBSCALE_STATEMENT_H
#define DBSCALE_STATEMENT_H

/*
 * Copyright (C) 2012,2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/Synch.h>
#include <expression.h>
#include <gmp.h>
#include <log.h>
#include <option.h>
#include <packet.h>
#include <partition_method.h>
#include <sql_parser.h>

#include <algorithm>
#include <list>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "ace/Condition_T.h"
#define FEDERATED_SELECT_END_STRING "WHERE 1=0"
#define SIZE_OF_END_STRING 9
#define DBSCALE_TMP_COLUMN_VALUE "dbscale_tmp_column_value"
#define DBSCALE_TMP_COLUMN_KEY "dbscale_tmp_column_key"
#define DBSCALE_TMP_UPDATE_TABLE "dbscale_tmp.dbscale_update_tmp"
#define RECYCLE_TABLE_POSTFIX "_PRESERVED__"
#define RECYCLE_TYPE_TRUNCATE "TRUNCATE_TABLE"
#define RECYCLE_TYPE_DROP "DROP_TABLE"

namespace dbscale {
namespace plan {}
};  // namespace dbscale

using std::map;
using std::string;
using std::vector;
using namespace std;
using namespace dbscale;
using namespace dbscale::plan;

namespace dbscale {
class PartitionScheme;
class DataSpace;
class Schema;
class DataServer;
class PartitionedTable;
class Connection;
class SPWorker;
class DispatchStrategy;
class SeparatedExecNode;
class TransferRule;
class ReconstructSQL;
struct ssl_option_struct;

#define COLUMN_INDEX_UNDEF (-1)
#define GMP_DEFAULT_PREC 150
#define GMP_N_DIGITS 65
#define TMP_TABLE_SCHEMA "dbscale_tmp"
#define INSERT_INC_NUM 10

struct exprposcmp {
  bool operator()(Expression *lexpr, Expression *rexpr) const {
    return lexpr->start_pos > rexpr->start_pos;
  }
};

struct TableNameStruct {
  string schema_name;
  string table_name;
  string table_alias;
};

struct SortDesc {
  int column_index;
  order_dir sort_order;
  CharsetType ctype;
  bool is_cs;
  bool is_utf8mb4_bin;
};

struct AggregateDesc {
  int column_index;
  CurrentStatementFunctionType type;
};
/* This is a struct for function avg(column).
 *
 * sum_index, count_index, avg_index record the position of sum(column),
 * count(column), avg(column) in field list. it starts with 1.
 * 0 represents that the value has not set.
 * for the result packet from mysql, we can caculte the value of avg(column).
 * value = sum(column) / count(column). if count(column) = 0 or sum(column) is
 * NULL value. we will not caculte the value of avg(column).we set
 * "reset_value=false" and we will not replace the value of avg(column) in new
 * packet
 */

struct AvgDesc {
  int sum_index;
  int count_index;
  int avg_index;
  mpf_t value;
  bool reset_value;
  char decimal;
  bool mpf_inited;
  AvgDesc() { mpf_inited = false; }
  void clone(const AvgDesc &desc) {
    this->sum_index = desc.sum_index;
    this->count_index = desc.count_index;
    this->avg_index = desc.avg_index;
    this->reset_value = desc.reset_value;
    this->decimal = desc.decimal;
    if (desc.mpf_inited) {
      this->mpf_inited = true;
      mpf_init_set(this->value, desc.value);
    }
  }
};

struct SelectExprDesc {
  unsigned int index;
  Expression *expr;
  string column_data;
  bool is_null;
};

struct ConnectByDesc {
  int where_index;
  int start_index;
  int prior_index;
  int recur_index;
};

enum SPExecuteFlag {
  EXECUTE_ON_SCHEMA_DATASPACE,
  EXECUTE_ON_ONE_DATASPACE,
  EXECUTE_ON_DBSCALE,
  SP_EXECUTE_FLAG_UNKNOWN
};

typedef enum {
  HAS_AUTO_INC_FIELD,
  NO_AUTO_INC_FIELD,
  AUTO_INC_NOT_SPECIFIED,
  AUTO_INC_VALUE_NULL,
  AUTO_INC_NO_NEED_MODIFY
} AutoIncStatus;

enum { INNODB, TOKUDB };

typedef enum { FIELD_EXPR, HAVING_EXPR } STMT_EXPR_TYPE;

typedef enum {
  CONTAINS_NO_FUNCTION,  // The flied contains no function
  CONTAINS_VALID_AGGR,   // The flied contains only valid aggregate function
  CONTAINS_OTHER_FUNC    // The flied contains other invalid aggregate function
} FieldFunctionSituation;

typedef enum {
  RESET_DROP = 0,  // just for dbscale test, drop dbscale_tmp database
  RESET_REINIT,    // reinit all tmp tables
  RESET_SLAVE,     // for dbscale internal, reset tmp tables on slaves
  RESET_DROP_DBSCALE_REPLICATION = 20
} RESET_TMP_TABLE_TYPE;

struct prepare_stmt_group_handle_meta {
  stmt_type st_type;
  int keyword_values_end_pos;
};

namespace plan {
class ExecutePlan;
class ExecuteNode;
}  // end namespace plan
namespace sql {
class Session;
bool is_partition_table_cover(PartitionedTable *pt1, PartitionedTable *pt2);
bool is_subquery_in_select_list(record_scan *rs);
DataSpace *get_prepare_sql_dataspace();
void adjust_virtual_machine_schema(
    unsigned int virtual_machine_id, unsigned int partition_id,
    const char *old_sql, const char *cur_schema, stmt_node *st,
    map<table_link *, DataSpace *> &record_scan_all_table_spaces,
    string &new_sql);
void adjust_shard_schema(const char *sql_schema, const char *cur_schema,
                         string &new_schema, unsigned int virtual_machine_id,
                         unsigned int partition_id);

// if sql just use one table, we need not use map to record sql's info
// use one_table_struct to record info.
struct one_table_struct {
  table_link *table;  // record table
  DataSpace *space;   // record dataspace
  record_scan *rs;    // record the record_scan
  bool only_one_table;
};

class CenterJoinGarbageCollector {
 public:
  CenterJoinGarbageCollector();
  ~CenterJoinGarbageCollector() {}

  void clear_garbages();

  void set_reconstruct_sql(ReconstructSQL *reconstruct_sql) {
    this->reconstruct_sql = reconstruct_sql;
  }

  void set_generated_sql(vector<string> *generated_sql) {
    this->generated_sql = generated_sql;
  }

  void set_new_table_vector(vector<vector<TableStruct> > *table_vector_query) {
    this->new_table_vector = table_vector_query;
  }
  void set_table_vector_query(
      vector<vector<TableStruct> > *table_vector_query) {
    this->table_vector_query = table_vector_query;
  }

  void add_fake_record_scan(record_scan *fake_rs) {
    fake_rs_vector.push_back(fake_rs);
  }

  void add_data_space(DataSpace *ds) { ds_vector.push_back(ds); }

  void set_handler(Handler *handler) { this->handler = handler; }

  void set_spark_sql_query(string sql) { spark_sql_drop_sql = sql; }

  void set_spark_sql_table_name(string table_name) {
    spark_sql_table_name = table_name;
  }

 private:
  ReconstructSQL *reconstruct_sql;
  vector<string> *generated_sql;
  vector<vector<TableStruct> > *table_vector_query;
  vector<record_scan *> fake_rs_vector;
  vector<DataSpace *> ds_vector;
  Handler *handler;
  string spark_sql_drop_sql;
  vector<vector<TableStruct> > *new_table_vector;
  string spark_sql_table_name;
};

class Statement {
 public:
  Statement(const char *sql) : sql(sql), sep_node_cond(sep_node_mutex) {
    par_table_num = 0;
    partial_parsed = false;
    new_select_item_num = 0;
    having = NULL;
    simple_having = true;
    simple_field_expr = true;
    need_deal_field_expr = false;
    groupby_all_par_keys = false;
    order_in_group_flag = false;
    old_auto_inc_val = -1;
    auto_increment_key_pos = -1;
    old_last_insert_id = -1;
    union_all_sub = false;
    union_table_sub = false;
    need_clean_new_field_expressions = false;
    need_clean_exec_nodes = false;
    need_clean_record_scan_par_key_ranges = false;
    need_clean_record_scan_par_key_equality = false;
    need_clean_par_tables = false;
    one_table_node.only_one_table = false;
    need_clean_spaces = false;
    need_clean_record_scan_par_table_map = false;
    need_clean_record_scan_all_spaces_map = false;
    need_clean_record_scan_all_par_tables_map = false;
    need_clean_record_scan_one_space_tmp_map = false;
    need_clean_record_scan_all_table_spaces = false;
    need_clean_select_uservar_vec = false;
    need_clean_group_by_list = false;
    update_on_one_server = false;
    delete_on_one_server = false;
    auto_inc_status = NO_AUTO_INC_FIELD;
    select_uservar_flag = false;
    select_field_num = 0;
    subquery_type = SUB_SELECT_NON;
    num_of_separated_exec_space = 0;
    num_of_separated_par_table = 0;
    is_insert_select_via_fifo = false;
    use_load_insert_select = false;
    cross_node_join = false;
    spark_insert = false;
    cross_node_join_rec_scan = NULL;
    auto_inc_step = 0;
    stmt_allow_dot_in_ident = false;
    multi_row_insert_saved_auto_inc_value = 0;
    multi_row_insert_max_valid_auto_inc_value = 0;
    first_union_stmt = NULL;
    is_call_store_proc_directly = false;
    is_partial_parsing_without_table_name = false;
    is_partial_parsing_related_to_table = false;
    subquery_table_id = 0;
    has_ignore_session_var = false;
    is_may_cross_node_join_related = false;
    need_clean_simple_expression = false;
    work_pointer = 0;
    fin_size = 0;
    running_threads = 0;
    is_sep_node_parallel_exec = false;
    fe_join = false;
    need_clean_shard_sql_list = false;
    need_clean_statement_stored_string = false;
    need_reanalysis_space = false;
    stmt_session = NULL;
    create_part_tb_def = NULL;
    contains_view = false;
    need_center_join = false;
    need_spark = false;
    null_key_str.assign("0");
    spark_invest_stmt = false;
    need_merge_subquery = false;
    need_handle_acl_related_stmt = true;
    is_update_via_delete_insert = false;
    is_update_via_delete_insert_revert = false;
    stmt_with_limit_using_quick_limit = false;
    is_first_column_dbscale_row_id = false;
    ctype = CHARSET_TYPE_OTHER;
    is_connect_by = false;
    is_update_set_subquery = false;
    memset(schema, 0, sizeof(schema));
  }
  virtual ~Statement();
  void set_sql(const char *sql) { this->sql = sql; }
  Session *get_session() { return stmt_session; }
  void set_session(Session *s) { stmt_session = s; }
  void clean_separated_node();
  void re_init() {
    if (need_center_join || need_spark) {
      cj_garbage_collector.clear_garbages();
    }
    if (need_clean_exec_nodes) clean_separated_node();
    if (need_clean_record_scan_par_key_ranges) {
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
      record_scan_par_key_ranges.clear();
      need_clean_record_scan_par_key_ranges = false;
    }
    if (need_clean_record_scan_par_key_equality) {
      record_scan_par_key_equality.clear();
      record_scan_par_key_values.clear();
      need_clean_record_scan_par_key_equality = false;
    }

    if (need_clean_new_field_expressions) {
      FieldExpression *field_expr;
      while (!new_field_expressions.empty()) {
        field_expr = new_field_expressions.front();
        new_field_expressions.pop_front();
        delete field_expr;
      }
      new_field_expressions.clear();
      new_select_items.clear();
      avg_list.clear();
      select_expr_list.clear();
      need_clean_new_field_expressions = false;
    }

    if (need_clean_par_tables) {
      par_tables.clear();
      need_clean_par_tables = false;
    }
    if (need_clean_spaces) {
      spaces.clear();
      need_clean_spaces = false;
    }
    if (need_clean_record_scan_par_table_map) {
      record_scan_par_table_map.clear();
      need_clean_record_scan_par_table_map = false;
    }
    if (need_clean_record_scan_all_spaces_map) {
      record_scan_all_spaces_map.clear();
      need_clean_record_scan_all_spaces_map = false;
    }
    if (need_clean_record_scan_all_par_tables_map) {
      record_scan_all_par_tables_map.clear();
      need_clean_record_scan_all_par_tables_map = false;
    }
    if (need_clean_record_scan_one_space_tmp_map) {
      record_scan_one_space_tmp_map.clear();
      need_clean_record_scan_one_space_tmp_map = false;
    }
    if (need_clean_record_scan_all_table_spaces) {
      record_scan_all_table_spaces.clear();
      need_clean_record_scan_all_table_spaces = false;
    }
    if (need_clean_select_uservar_vec) {
      select_uservar_vec.clear();
      need_clean_select_uservar_vec = false;
    }
    if (need_clean_group_by_list) {
      group_by_list.clear();
      order_by_list.clear();
      need_clean_group_by_list = true;
    }
    if (need_clean_shard_sql_list) {
      shard_sql_list.clear();
      need_clean_shard_sql_list = false;
    }
    if (need_clean_statement_stored_string) {
      statement_stored_string.clear();
      need_clean_statement_stored_string = false;
    }
    global_table_merged.clear();
    update_on_one_server = false;
    delete_on_one_server = false;
    sql = NULL;
    one_table_node.only_one_table = false;
    par_table_num = 0;
    partial_parsed = false;
    new_select_item_num = 0;
    having = NULL;
    simple_having = true;
    simple_field_expr = true;
    need_deal_field_expr = false;
    groupby_all_par_keys = false;
    order_in_group_flag = false;
    old_auto_inc_val = -1;
    auto_increment_key_pos = -1;
    auto_inc_step = 0;
    stmt_allow_dot_in_ident = false;
    multi_row_insert_saved_auto_inc_value = 0;
    multi_row_insert_max_valid_auto_inc_value = 0;
    old_last_insert_id = -1;
    auto_inc_status = NO_AUTO_INC_FIELD;
    select_uservar_flag = false;
    select_field_num = 0;
    num_of_separated_exec_space = 0;
    num_of_separated_par_table = 0;
    is_call_store_proc_directly = false;
    is_partial_parsing_without_table_name = false;
    is_partial_parsing_related_to_table = false;
    has_ignore_session_var = false;
    need_clean_simple_expression = false;
    is_sep_node_parallel_exec = false;
    fe_join = false;
    grant_user_name_vec.clear();
    need_center_join = false;
    need_spark = false;
    cross_node_join = false;
    spark_insert = false;
    spark_invest_stmt = false;
    grant_sqls.clear();
    null_key_str.assign("0");
    need_reanalysis_space = false;
    stmt_session = NULL;
    create_part_tb_def = NULL;
    contains_view = false;
    is_first_column_dbscale_row_id = false;
    ctype = CHARSET_TYPE_OTHER;
  }

  const char *get_value_from_bool_expression(
      const char *column_name, BoolBinaryCaculateExpression *expr);

  const char *get_value_from_compare_expression(const char *column_name,
                                                CompareExpression *expr);

  string get_column_name(string value);

  bool is_readonly() const { return is_read_only(); }
  void set_partial_parsed(bool b) { partial_parsed = b; }
  bool is_partial_parsed() { return partial_parsed; }

  void execute() { this->do_execute(); }

  void free_resource() {
    if (need_center_join || need_spark) {
      cj_garbage_collector.clear_garbages();
    }
    free_re_parser_list();
    this->do_free_resource();
  }

  void free_re_parser_list() {
    list<Statement *>::iterator it;
    while (!re_parser_stmt_list.empty()) {
      it = re_parser_stmt_list.begin();
      (*it)->free_resource();
      delete *it;
      *it = NULL;
      re_parser_stmt_list.erase(it);
    }
  }

  const char *get_sql() { return sql; }

  stmt_node *get_stmt_node() { return &st; }

  stmt_node *get_latest_stmt_node() {
    if (re_parser_stmt_list.empty())
      return get_stmt_node();
    else {
      if (!re_parser_stmt_list.back())
        throw Error("Internal error for get_latest_stmt_node.");
      return re_parser_stmt_list.back()->get_stmt_node();
    }
  }

  vector<table_link *> *get_par_tables() { return &par_tables; }

  int get_part_table_num() { return par_table_num; }

  int get_spaces_size() { return spaces.size(); }

  void set_default_schema(const char *schema) {
    copy_string(this->schema, schema, sizeof(this->schema));
  }

  const char *get_select_sub_sql() const { return select_sub_sql.c_str(); }
  const char *get_modify_sub_sql() const { return modify_sub_sql.c_str(); }
  const char *get_schema() const { return schema; }
  void set_sql_schema(const char *schema_name) { sql_schema = schema_name; }
  const char *get_sql_schema() const { return sql_schema.c_str(); }
  bool has_sql_schema() { return sql_schema.length() > 0; }

  int get_identify_columns(const char *schema_name, const char *table_name,
                           const char *table_alias, vector<string *> *columns);

  bool generate_two_phase_modify_sub_sqls(vector<string *> *columns,
                                          table_link *modify_table);

  void assemble_modify_all_partition_plan(ExecutePlan *plan,
                                          PartitionedTable *par_space);
  // This function can be virtual if need
  void generate_execution_plan(ExecutePlan *plan);

  bool generate_forward_master_role_plan(ExecutePlan *plan);

  bool get_partitions_all_par_tables(record_scan *rs, PartitionMethod *method,
                                     vector<unsigned int> *par_id);

  void get_partitons_one_record_scan(record_scan *rs, PartitionMethod *method,
                                     unsigned int partition_num,
                                     vector<unsigned int> *par_id);

  unsigned int get_partition_one_insert_row(Expression *column_values,
                                            vector<unsigned int> *key_pos_vec,
                                            int64_t auto_inc_val,
                                            int value_count,
                                            PartitionMethod *method,
                                            string replace_null_char);

  void get_all_tables(join_node *tables,
                      vector<vector<TableStruct> > *join_table_sequence);
  unsigned int get_partition_insert_asign(
      Expression *asign_list, const char *schema_name, size_t schema_len,
      const char *table_name, size_t table_len, const char *table_alias,
      vector<const char *> *key_names, int64_t auto_inc_val,
      PartitionMethod *method);

  bool get_auto_inc_from_insert_assign(Expression *assign_list,
                                       const char *schema_name,
                                       size_t schema_len,
                                       const char *table_name, size_t table_len,
                                       const char *table_alias,
                                       expr_list_item **auto_inc_expr_item);

  unsigned int get_key_pos_from_column_list(
      name_item *list, const char *schema_name, size_t schema_len,
      const char *table_name, size_t table_len, const char *table_alias,
      const char *key_name);

  int get_auto_inc_pos_from_column_list(
      name_item *list, const char *schema_name, size_t schema_len,
      const char *table_name, size_t table_len, const char *table_alias);

  int64_t get_auto_increment_value(PartitionedTable *part_space,
                                   ExecutePlan *plan, const char *schema_name,
                                   const char *table_name, Expression *expr,
                                   int row_num, bool is_multi_row_insert);

  string get_table_index_for_rs(record_scan *rs, const char *table_schema,
                                const char *table_alias,
                                set<string> column_vector,
                                map<int, string> &column_type_map);

  void check_int_value(int64_t parsed_val, int64_t &curr_auto_inc_val,
                       const char *schema_name, const char *table_name,
                       PartitionedTable *part_space, bool is_multi_row_insert,
                       Session *s);

  int64_t check_inc_int_value(int64_t parsed_val, int64_t curr_auto_inc_val,
                              PartitionedTable *part_space, Session *s,
                              const char *schema_name, const char *table_name);

  int64_t get_auto_inc_value_one_insert_row(PartitionedTable *part_space,
                                            ExecutePlan *plan,
                                            const char *schema_name,
                                            const char *table_name,
                                            Expression *expr, int row_num = 1);

  int64_t get_auto_inc_value_multi_insert_row(
      PartitionedTable *part_space, ExecutePlan *plan, const char *schema_name,
      const char *table_name, Expression *expr, int row_num = 1);

  void check_and_replace_view(Session *session, table_link **table);
  DataSpace *check_and_replace_odbc_uservar(ExecutePlan *plan);
  bool check_migrate_block_table(ExecutePlan *plan);
  void init_auto_increment_params(ExecutePlan *plan, const char *schema_name,
                                  const char *table_name,
                                  PartitionedTable *dataspace);
  void rollback_auto_inc_params(ExecutePlan *plan,
                                PartitionedTable *part_table);

  void check_column_name(set<string> *valid_columns,
                         set<string> &curr_column_names,
                         const char *column_name, const char *full_table_name);

  void check_column_count_name(PartitionedTable *part_space, name_item *list,
                               int &column_count);
  void check_column_count_name(PartitionedTable *part_space,
                               Expression *assign_list);

  expr_list_item *get_auto_inc_expr(expr_list_item *head);

  void set_old_auto_inc_value(int64_t val) { old_auto_inc_val = val; }
  int64_t get_old_auto_inc_value() { return old_auto_inc_val; }

  AutoIncStatus get_auto_inc_status() { return auto_inc_status; }

  void set_auto_inc_status(AutoIncStatus status) { auto_inc_status = status; }

  void set_auto_inc_status(bool has_auto_inc_field) {
    if (has_auto_inc_field) {
      auto_inc_status = HAS_AUTO_INC_FIELD;
    } else {
      auto_inc_status = NO_AUTO_INC_FIELD;
    }
  }

  bool rs_contains_aggr_group_limit(record_scan *rs, table_link *par_table);
  bool table_link_same_dataspace(table_link *modify_table,
                                 table_link *par_table);
  void set_auto_increment_key_pos(int pos) { auto_increment_key_pos = pos; }
  int get_auto_increment_key_pos() { return auto_increment_key_pos; }
  void set_old_last_insert_id(int64_t val) { old_last_insert_id = val; }

  void get_execution_plan_for_max_count_certain(ExecutePlan *plan);
  string &get_full_table_name() { return full_table_name; }
  void set_full_table_name(const char *schema_name, const char *table_name);

  void check_value_count_auto_increment(int column_count, int value_count,
                                        PartitionedTable *part_space,
                                        int row_index);

  void build_insert_sql_prefix(string &sql_prefix, insert_op_node *insert);

  void build_insert_sql_assign_auto_inc(string &insert_sql,
                                        Expression *value_expr,
                                        int64_t curr_auto_inc_val);

  void build_insert_sql_auto_inc(string &insert_sql, insert_op_node *insert,
                                 expr_list_item *auto_inc_expr_item,
                                 int64_t curr_auto_inc_val);

  void append_insert_sql_row(string &part_sql, ListExpression *expr_list);

  void append_insert_sql_row_auto_inc(string &part_sql, insert_row *row,
                                      expr_list_item *auto_inc_expr_item,
                                      int64_t curr_auto_inc_val);

  bool select_need_divide(record_scan *top_select, record_scan *partition,
                          unsigned int dataspace_merge_start,
                          const char *schema_name, size_t schema_len,
                          const char *table_name, size_t table_len,
                          const char *table_alias,
                          vector<const char *> *key_names);

  bool is_all_par_table_equal(record_scan *rs, table_link *table,
                              const char *kept_key, const char *key_name);

  bool is_CNJ_all_par_table_equal(record_scan *rs, table_link *table1,
                                  table_link *table2, const char *kept_key,
                                  const char *key_name);

  bool can_dataspace_merge(unsigned int dataspace_merge_start);

  table_link *find_table_link(const char *table_full_name, record_scan *rs,
                              bool ignore_schema = false);

  bool has_and_rebuild_autocommit_sql(ExecutePlan *plan);
  void replace_func_found_rows_with_value(field_item *field,
                                          const record_scan *scan,
                                          ExecutePlan *plan,
                                          field_item *top_field);
  void rebuild_found_rows_sql(record_scan *rs, ExecutePlan *plan);
  string replace_found_rows(const char *query, ExecutePlan *plan,
                            unsigned int head_pos, unsigned int tail_pos);
  string add_original_field(const char *query, unsigned int head_pos,
                            unsigned int tail_pos);

  void replace_sql_function_field(PreviousStatementFunctionType func_type,
                                  string &sql, record_scan *rs,
                                  ExecutePlan *plan);
  void get_all_table_vector(record_scan *rs,
                            vector<vector<TableStruct> > *join_table_sequence);

  void prepare_table_vector_and_leftjoin_subquery_from_rs(
      record_scan *rs, list<TableStruct *> *table_struct_list,
      map<TableStruct *, table_link *> *table_struct_map);

  bool is_into_outfile() {
    if (st.type == STMT_SELECT && st.into_outfile != NULL) return true;

    return false;
  }

  bool is_user_grant_stmt() {
    if (st.type == STMT_GRANT || st.type == STMT_CREATE_USER ||
        st.type == STMT_ALTER_USER || st.type == STMT_DROP_USER)
      return true;
    return false;
  }

  void mark_table_info_to_delete(ExecutePlan *plan);
  void validate_into_outfile();
  into_outfile_item *get_into_outfile() { return st.into_outfile; }

  bool is_select_into() {
    if (st.type == STMT_SELECT && st.sql->select_oper->select_into != NULL)
      return true;

    return false;
  }
  select_into_item *get_select_into() {
    return st.sql->select_oper->select_into;
  }

  vector<const char *> *get_key_values(record_scan *rs, const char *key_name) {
    return &record_scan_par_key_values[rs][key_name];
  }

  RangeValues *get_key_ranges(record_scan *rs, const char *key_name) {
    return record_scan_par_key_ranges[rs][key_name];
  }

  bool is_set_password(ExecutePlan *plan);

  void build_uservar_sql(var_item *var_item_list, ExecutePlan *plan);
  void deal_set_var();

  FieldExpression *get_field_expression(int col_index, Expression *expr) {
    return this->do_get_field_expression(col_index, expr);
  }

  vector<ExecuteNode *> get_union_all_nodes() { return union_all_nodes; }
  void analysis_center_join(ExecutePlan *plan);
#ifndef DBSCALE_DISABLE_SPARK
  map<string, unsigned int> generate_table_partition_map(
      const char *spark_partition);
  void analysis_spark_sql_join(ExecutePlan *plan);
#endif
  vector<ExecuteNode *> get_fe_join_nodes() { return fe_join_nodes; }
  void set_fe_join(bool b) { fe_join = b; }
  void set_union_all_sub(bool b) { union_all_sub = b; }
  bool is_union_all_sub() { return union_all_sub; }

  void handle_federated_join_method();

  bool get_select_uservar_flag() { return select_uservar_flag; }
  unsigned int get_select_field_num() { return select_field_num; }
  vector<pair<unsigned int, string> > *get_select_uservar_vec() {
    return &select_uservar_vec;
  }

  bool is_can_swap_type();
  map<string, string, strcasecomp> get_alias_table_map(record_scan *rs);
  string get_insert_select_modify_schema() {
    return insert_select_modify_schema;
  }
  string get_insert_select_modify_table() { return insert_select_modify_table; }
  string get_local_load_script() { return local_load_script; }
  bool insert_select_via_fifo() { return is_insert_select_via_fifo; }

  bool is_load_insert_select() { return use_load_insert_select; };
  void set_load_insert_select(bool b) { use_load_insert_select = b; }

  void set_sub_query_stmt(sub_select_type t) { subquery_type = t; }

  sub_select_type get_sub_query_stmt() { return subquery_type; }

  void set_spark_invest_stmt(bool b) { spark_invest_stmt = b; }

  bool get_spark_invest_stmt() { return spark_invest_stmt; }

  bool is_handle_sub_query() { return subquery_type != SUB_SELECT_NON; }

  map<record_scan *, bool> &get_record_scan_need_cross_node_join() {
    return record_scan_need_cross_node_join;
  }

  vector<DataSpace *> &get_spaces() { return spaces; }

  map<record_scan *, SeparatedExecNode *> &get_record_scan_exec_node_map() {
    return record_scan_exec_node_map;
  }

  map<record_scan *, list<record_scan *> > &get_record_scan_dependent_map() {
    return record_scan_dependent_map;
  }

  vector<vector<TableStruct *> *> *get_record_scan_join_tables(
      record_scan *rs) {
    vector<vector<TableStruct *> *> *ret = NULL;
    if (record_scan_join_tables.count(rs)) ret = record_scan_join_tables[rs];
    return ret;
  }

  SeparatedExecNode *create_one_separated_exec_node(record_scan *node_rs,
                                                    ExecutePlan *plan);

  bool need_force_move_global(record_scan *root);

  void set_cross_node_join(bool b) {
    if (b) check_and_refuse_partial_parse();
    cross_node_join = b;
  }
  void check_and_refuse_partial_parse() {
    if (is_partial_parsed()) {
      LOG_ERROR("got partial parsed sql\n");
      throw Error("got partial parsed sql");
    }
  }
  bool is_cross_node_join() { return cross_node_join; }

  void record_statement_stored_string(string &s) {
    statement_stored_string.push_back(s);
    need_clean_statement_stored_string = true;
  }
  const char *get_last_statement_stored_string() {
    return statement_stored_string.rbegin()->c_str();
  }
  void record_shard_sql_str(string &s) {
    shard_sql_list.push_back(s);
    need_clean_shard_sql_list = true;
  }
  const char *get_last_shard_sql() { return shard_sql_list.rbegin()->c_str(); }

  const char *adjust_stmt_sql_for_shard(DataSpace *dataspace,
                                        const char *old_sql);
  const char *adjust_stmt_sql_for_alias(DataSpace *dataspace,
                                        const char *old_sql);
  const char *adjust_stmt_sql_for_shard_for_show_table_status(
      DataSpace *ds, const char *old_sql);

  void get_table_vector(record_scan *rs,
                        vector<vector<TableStruct> > *join_table_sequence);
  vector<TableStruct *> *get_table_vector(record_scan *rs);

  string get_subquery_table_name(record_scan *rs) {
    string ret;
    if (record_scan_subquery_names.count(rs))
      ret = record_scan_subquery_names[rs];
    return ret;
  }
  DataSpace *get_column_table_subquery_space(record_scan *rs) {
    DataSpace *ret = NULL;
    if (column_output_dataspace.count(rs)) ret = column_output_dataspace[rs];
    return ret;
  }
  string get_subquery_table_names_for_replace(record_scan *rs) {
    string ret;
    if (record_scan_subquery_names_for_replace.count(rs))
      ret = record_scan_subquery_names_for_replace[rs];
    return ret;
  }
  map<string, string> get_real_subquery_table_names() {
    if (get_is_sep_node_parallel_exec()) {
      map<string, string> ret;
      real_subquery_names_mutex.acquire();
      ret = record_scan_real_subquery_names;
      real_subquery_names_mutex.release();

      return ret;
    } else
      return record_scan_real_subquery_names;
  }
  void set_subquery_table_names_for_replace(record_scan *rs, string name) {
    name.append("_");
    char table_num[10];
    sprintf(table_num, "%d", subquery_table_id);
    name.append((const char *)table_num);
    subquery_table_id++;
    record_scan_subquery_names_for_replace[rs] = name;
  }
  void set_real_subquery_table_names(string name, string real_name) {
    if (get_is_sep_node_parallel_exec()) {
      real_subquery_names_mutex.acquire();
      record_scan_real_subquery_names[name] = real_name;
      real_subquery_names_mutex.release();
    } else {
      record_scan_real_subquery_names[name] = real_name;
    }
  }

  void set_select_var_sql(string sql) { select_var_sql = sql; }
  string get_select_var_sql() { return select_var_sql; }
  vector<string> *get_uservar_list() { return &uservar_list; }
  vector<string> *get_sessionvar_list() { return &sessionvar_list; }

  void handle_session_var(ExecutePlan *plan);

  list<SortDesc> *get_order_by_list() { return &order_by_list; }

  void set_union_table_sub(bool b) { union_table_sub = b; }
  bool is_union_table_sub() { return union_table_sub; }

  void set_union_sub_table_name(string name) { union_sub_table_name = name; }
  Statement *get_first_union_stmt();
  vector<vector<TableStruct> > *get_union_table_vector() {
    return &union_table_vector;
  }

  bool is_call_store_procedure_directly() {
    return is_call_store_proc_directly;
  }

  string get_current_sp_worker_name() { return current_sp_worker_name; }
  SPExecuteFlag check_procedure_execution_flag(const char *sp_schema,
                                               const char *cur_schema,
                                               Statement *stmt,
                                               DataSpace **data_space);
  bool check_federated_name();
  bool check_federated_show_status();
  bool check_federated_select();
  void assemble_union_table_sub(ExecutePlan *plan);
  void get_fe_tmp_table_name(string sql, set<string> &name_vec);

  void set_is_may_cross_node_join_related(bool b) {
    is_may_cross_node_join_related = b;
  }
  bool get_is_may_cross_node_join_related() {
    return is_may_cross_node_join_related;
  }
  DataSpace *get_subquery_table_dataspace(DataSpace *local_space,
                                          const char *key_name, bool is_dup);
  void handle_federated_join();
  void handle_create_or_drop_view(ExecutePlan *plan);
  void handle_create_or_drop_event();
  bool check_spj_subquery_can_merge(record_scan *parent_rs,
                                    record_scan *child_rs);
  table_link *find_partition_table_table_link(record_scan *rs);

  map<table_link *, DataSpace *> &get_record_scan_all_table_spaces_map() {
    return record_scan_all_table_spaces;
  }
  void refresh_norm_table_spaces();
  void refresh_part_table_links();
  vector<const char *> *get_grant_user_name_vec() {
    return &grant_user_name_vec;
  }
  int check_rs_contains_partition_key(record_scan *rs, DataSpace *ds);
  void handle_union_all_record_scan_tree(record_scan *root,
                                         record_scan *top_union_rs,
                                         ExecutePlan *plan);
  void handle_union_record_scan_tree(record_scan *root,
                                     record_scan *top_union_rs);
  bool check_union_record_scan_can_merge(record_scan *root,
                                         record_scan *top_union_rs);
  join_node *get_first_join_table(record_scan *rs);
  void get_all_join_table(join_node *jtable, vector<join_node *> *jtables);
  string get_first_column(record_scan *rs);
  void revise_record_scans(record_scan *last_union_rs,
                           record_scan *top_union_rs, ExecutePlan *plan,
                           bool &right_part_re_distribute);
  list<string> get_grant_sqls() { return grant_sqls; }
  map<int, string> get_left_join_table_position(record_scan *rs) {
    map<int, string> ret;
    if (get_is_sep_node_parallel_exec()) {
      cross_join_mutex.acquire();
      if (left_join_table_position.count(rs))
        ret = left_join_table_position[rs];
      cross_join_mutex.release();
    } else {
      if (left_join_table_position.count(rs))
        ret = left_join_table_position[rs];
    }
    return ret;
  }
  void set_left_join_tmp_table_names(record_scan *rs, map<int, string> tables) {
    if (get_is_sep_node_parallel_exec()) {
      cross_join_mutex.acquire();
      left_join_table_position[rs] = tables;
      cross_join_mutex.release();
    } else {
      left_join_table_position[rs] = tables;
    }
  }
  map<int, TableStruct *> get_left_join_tables(record_scan *rs) {
    map<int, TableStruct *> ret;
    if (left_join_tables.count(rs)) ret = left_join_tables[rs];
    return ret;
  }
  void set_record_scan_one_space_tmp_map(record_scan *rs, DataSpace *ds) {
    record_scan_one_space_tmp_map[rs] = ds;
  }
  DataSpace *get_record_scan_table_subquery_space(record_scan *rs) {
    DataSpace *ret = NULL;
    if (record_scan_table_subquery_space_map.count(rs))
      ret = record_scan_table_subquery_space_map[rs];
    return ret;
  }
  DataSpace *get_record_scan_one_space(record_scan *rs) {
    DataSpace *ret = NULL;
    if (record_scan_one_space_tmp_map.count(rs))
      ret = record_scan_one_space_tmp_map[rs];
    return ret;
  }
  bool check_simple_select(record_scan *rs);
  bool check_rs_no_invalid_aggr(record_scan *rs);
  bool parent_has_no_table(record_scan *rs);
  table_link *get_one_table_link(record_scan *rs, DataSpace *space);
  void refresh_tables_for_merged_subquery(record_scan *old_rs,
                                          record_scan *new_rs);
  void set_need_merge_subquery(bool b) { need_merge_subquery = b; }
  bool check_rs_contains_table(record_scan *rs);
  int get_child_rs_num(record_scan *rs);
  void adjust_spaces(DataSpace *ds) {
    if ((int)spaces.size() > 1) spaces[spaces.size() - 2] = ds;
  }
  void set_client_charset_type(CharsetType type) { ctype = type; }
  CharsetType get_client_charset_type() { return ctype; }
  bool check_duplicated_table();
  void check_disaster_relate_option(dynamic_config_op_node *config_oper);

 private:
  void check_and_replace_oracle_sequence(ExecutePlan *plan, table_link **table);
  void init_spark_schema(DataSpace *ds);
  virtual void do_execute() = 0;
  virtual void do_free_resource() = 0;
  virtual bool is_read_only() const = 0;

 protected:
  string get_spark_dst_jdbc_url(DataSpace *ds);
  int get_field_num();
  bool assemble_dbscale_cal_uservar_plan(ExecutePlan *plan);
  void assemble_connect_by_plan(ExecutePlan *plan, DataSpace *dataspace);
  void assemble_show_create_event_plan(ExecutePlan *plan);
  void assemble_show_tables_plan(ExecutePlan *plan);
  void assemble_show_events_plan(ExecutePlan *plan);
  void assemble_federated_table_status_node(ExecutePlan *plan);
  void assemble_empty_set_node(ExecutePlan *plan, int field_num);
  void assemble_forward_master_role_plan(ExecutePlan *plan,
                                         bool is_slow_query = false);
  void assemble_execute_query_plan(ExecutePlan *plan);
  void assemble_direct_exec_plan(ExecutePlan *plan, DataSpace *dataspace);
  void assemble_direct_exec_plan(ExecutePlan *plan, DataSpace *dataspace,
                                 string &customized_sql);
  void assemble_one_partition_plan(ExecutePlan *plan, DataSpace *dataspace);
  void assemble_one_partition_plan(ExecutePlan *plan, DataSpace *dataspace,
                                   string &customized_sql);
  void assemble_dispatch_packet_plan(ExecutePlan *plan, TransferRule *rule,
                                     DataSpace *exec_space,
                                     DataSpace *send_space, const char *sql,
                                     Session *work_session);
  void assemble_mul_par_result_set_plan(ExecutePlan *plan,
                                        vector<unsigned int> *par_ids,
                                        PartitionedTable *par_table,
                                        bool need_check_distinct);
  void assemble_mul_par_modify_plan(ExecutePlan *plan,
                                    vector<unsigned int> *par_ids,
                                    PartitionedTable *par_table);
  void assemble_mul_part_insert_plan(ExecutePlan *plan,
                                     PartitionedTable *part_table);
  void assemble_two_phase_modify_normal_plan(ExecutePlan *plan,
                                             vector<unsigned int> *par_ids,
                                             PartitionedTable *par_table,
                                             table_link *modify_table);
  void assemble_modify_limit_partition_plan(ExecutePlan *plan,
                                            vector<unsigned int> *par_ids,
                                            PartitionedTable *par_table);
  void assemble_two_phase_modify_no_partition_plan(ExecutePlan *plan,
                                                   DataSpace *modify_space,
                                                   DataSpace *select_space,
                                                   table_link *modify_table);
  void assemble_load_local_plan(ExecutePlan *plan, DataSpace *dataspace);
  void assemble_partition_load_local_plan(ExecutePlan *plan,
                                          PartitionedTable *part_table,
                                          const char *schema_name,
                                          const char *table_name);
  void assemble_partition_load_data_infile_plan(ExecutePlan *plan,
                                                PartitionedTable *part_table,
                                                const char *schema_name,
                                                const char *table_name);
  void assemble_load_data_infile_plan(ExecutePlan *plan, DataSpace *dataspace);
  void assenble_dbscale_reset_zoo_info_plan(ExecutePlan *plan);

  void assemble_load_local_external_plan(ExecutePlan *plan,
                                         DataSpace *dataspace,
                                         DataServer *dataserver);
  void assemble_load_data_infile_external_plan(ExecutePlan *plan,
                                               DataSpace *dataspace,
                                               DataServer *dataserver);
  void assemble_kill_plan(ExecutePlan *plan, int cluster_id, uint32_t kid);
  void assemble_dbscale_show_partition_plan(ExecutePlan *plan);
  void assemble_dbscale_show_virtual_map_plan(ExecutePlan *plan);
  void assemble_dbscale_show_auto_increment_info(ExecutePlan *plan);
  void assemble_dbscale_set_auto_increment_offset(ExecutePlan *plan);
  void assemble_dbscale_mul_sync_plan(ExecutePlan *plan);
  void assemble_dbscale_show_status_plan(ExecutePlan *plan);
  void assemble_show_catchup_plan(ExecutePlan *plan, const char *source_name);
  void assemble_dbscale_skip_wait_catchup_plan(ExecutePlan *plan,
                                               const char *source_name);
  void assemble_show_transaction_sqls_plan(ExecutePlan *plan);
  void assemble_show_databases_plan(ExecutePlan *plan);
  void assemble_dbscale_estimate_select_plan(ExecutePlan *plan,
                                             DataSpace *dataspace,
                                             Statement *statement);
  void assemble_select_plain_value_plan(ExecutePlan *plan,
                                        const char *str_value,
                                        const char *alias_name);
  void assemble_dbscale_estimate_select_partition_plan(
      ExecutePlan *plan, vector<unsigned int> *par_ids,
      PartitionedTable *part_table, const char *sql, Statement *statement);

  void assemble_show_warnings_plan(ExecutePlan *plan);
  void assemble_dbscale_show_default_session_variables(ExecutePlan *plan);
  void assemble_dbscale_show_version_plan(ExecutePlan *plan);
  void assemble_show_version_plan(ExecutePlan *plan);
  void assemble_show_components_version_plan(ExecutePlan *plan);
  void assemble_dbscale_show_hostname_plan(ExecutePlan *plan);
  void assemble_dbscale_show_all_fail_transaction_plan(ExecutePlan *plan);
  void assemble_dbscale_show_detail_fail_transaction_plan(ExecutePlan *plan,
                                                          const char *xid);
  void assemble_dbscale_show_partition_table_status_plan(
      ExecutePlan *plan, const char *table_name);
  void assemble_dbscale_show_schema_acl_info(ExecutePlan *plan,
                                             bool is_show_all);
  void assemble_dbscale_show_table_acl_info(ExecutePlan *plan,
                                            bool is_show_all);
  void assemble_dbscale_show_path_info(ExecutePlan *plan);
  void assemble_dbscale_show_warnings(ExecutePlan *plan);
  void assemble_dbscale_show_critical_errors(ExecutePlan *plan);
  void assemble_dbscale_clean_fail_transaction_plan(ExecutePlan *plan,
                                                    const char *xid);
  void assemble_dbscale_add_default_session_variables(ExecutePlan *plan);
  void assemble_dbscale_remove_default_session_variables(ExecutePlan *plan);
  void assemble_dbscale_show_shard_partition_table_plan(
      ExecutePlan *plan, const char *scheme_name);
  void assemble_dbscale_show_rebalance_work_load_plan(ExecutePlan *plan,
                                                      const char *scheme_name,
                                                      list<string> sources,
                                                      const char *schema_name,
                                                      int is_remove);

  void assemble_two_phase_modify_partition_plan(
      ExecutePlan *plan,
      DataSpace *space,  // only for insert_select
      PartitionMethod *method, table_link *modify_table,
      vector<unsigned int> *key_pos = NULL,   // only for insert_select
      vector<unsigned int> *par_ids = NULL);  // only for update/delete_select
  void fulfill_insert_key_pos(name_item *column_list,
                              vector<const char *> *key_names,
                              const char *schema_name, size_t schema_len,
                              const char *table_name, size_t table_len,
                              const char *table_alias,
                              vector<unsigned int> *key_pos_def,
                              vector<unsigned int> *key_pos_real);

  void assemble_dbscale_restart_spark_agent_node(ExecutePlan *plan);
  void assemble_transaction_plan(ExecutePlan *plan);
  void assemble_lock_tables_plan(ExecutePlan *plan);
  void assemble_transaction_unlock_plan(ExecutePlan *plan);
  void assemble_xatransaction_plan(ExecutePlan *plan);
  void assemble_cluster_xatransaction_plan(ExecutePlan *plan);
  void assemble_show_func_or_proc_status_plan(ExecutePlan *plan);
  void assemble_show_user_memory_status_plan(ExecutePlan *plan);
  void assemble_show_session_id_plan(ExecutePlan *plan, const char *server_name,
                                     int connection_id);
  void assemble_show_user_status_plan(ExecutePlan *plan, const char *user_id,
                                      const char *user_name,
                                      bool only_show_running, bool instance,
                                      bool show_status_count);
  void assemble_show_user_processlist_plan(ExecutePlan *plan,
                                           const char *cluster_id,
                                           const char *user_id, int local);
  void assemble_backend_server_execute_plan(ExecutePlan *plan,
                                            const char *stmt_sql);
  void assemble_execute_on_all_masterserver_execute_plan(ExecutePlan *plan,
                                                         const char *stmt_sql);
  void assemble_dbscale_execute_on_dataserver_plan(ExecutePlan *plan,
                                                   const char *dataserver_name,
                                                   const char *stmt_sql);
  void assemble_migrate_plan(ExecutePlan *plan);
  void assemble_dynamic_add_slave_plan(
      ExecutePlan *plan, dynamic_add_slave_op_node *dynamic_add_slave_oper);
  void assemble_dynamic_add_pre_disaster_master_plan(
      ExecutePlan *plan, dynamic_add_pre_disaster_master_op_node
                             *dynamic_add_pre_disaster_master_oper);
  void assemble_dynamic_add_data_server_plan(
      ExecutePlan *plan,
      dynamic_add_data_server_op_node *dynamic_add_data_server_oper);
  void assemble_dynamic_add_data_source_plan(
      ExecutePlan *plan,
      dynamic_add_data_source_op_node *dynamic_add_data_source_oper, int type);
  void assemble_dynamic_add_data_space_plan(
      ExecutePlan *plan,
      dynamic_add_data_space_op_node *dynamic_add_data_space_oper);

  void assemble_set_schema_acl_plan(
      ExecutePlan *plan, set_schema_acl_op_node *set_schema_acl_oper);
  void assemble_set_user_not_allow_operation_time_plan(
      ExecutePlan *plan, set_user_not_allow_operation_time_node *oper);
  void assemble_reload_user_not_allow_operation_time_plan(ExecutePlan *plan,
                                                          string user_name);
  void assemble_show_user_not_allow_operation_time_plan(ExecutePlan *plan,
                                                        string user_name);
  void assemble_set_table_acl_plan(ExecutePlan *plan,
                                   set_table_acl_op_node *set_table_acl_oper);
  void assemble_dbscale_reset_tmp_table_plan(ExecutePlan *plan);
  void assemble_dbscale_reload_func_type_plan(ExecutePlan *plan);
  void assemble_dbscale_shutdown_plan(ExecutePlan *plan);
  void assemble_dbscale_set_pool_plan(ExecutePlan *plan, pool_node *pool_info);
  void assemble_dbscale_reset_info_plan(
      ExecutePlan *plan, dbscale_reset_info_op_node *reset_info_oper);
  void assemble_dbscale_block_plan(ExecutePlan *plan);
  void assemble_dbscale_disable_server_plan(ExecutePlan *plan);
  void assemble_dbscale_flush_pool_plan(ExecutePlan *plan,
                                        pool_node *pool_info);
  void assemble_dbscale_force_flashback_online_plan(ExecutePlan *plan);
  void assemble_dbscale_xa_recover_slave_dbscale_plan(ExecutePlan *plan);
  void assemble_dynamic_change_master_plan(
      ExecutePlan *plan,
      dynamic_change_master_op_node *dynamic_change_master_oper);
  void assemble_dynamic_remove_slave_plan(
      ExecutePlan *plan,
      dynamic_remove_slave_op_node *dynamic_remove_slave_oper);
  void assemble_dynamic_remove_schema_plan(ExecutePlan *plan,
                                           const char *schema_name,
                                           bool is_force);
  void assemble_dynamic_remove_table_plan(ExecutePlan *plan,
                                          const char *table_name,
                                          bool is_force);
  void assemble_dynamic_change_table_scheme_plan(ExecutePlan *plan,
                                                 const char *table_name,
                                                 const char *scheme_name);
  void assemble_dynamic_change_multiple_master_active_plan(
      ExecutePlan *plan, dynamic_change_multiple_master_active_op_node
                             *dynamic_change_multiple_master_active_oper);
  void assemble_dynamic_change_dataserver_ssh_plan(ExecutePlan *plan,
                                                   const char *server_name,
                                                   const char *username,
                                                   const char *pwd, int port);
  void assemble_dynamic_remove_plan(ExecutePlan *plan);
  void assemble_dbscale_help_plan(ExecutePlan *plan, const char *name);
  void assemble_dbscale_show_white_list(ExecutePlan *plan);
  void assemble_dbscale_show_join_plan(ExecutePlan *plan, const char *name);
  void assemble_dbscale_show_base_status_plan(ExecutePlan *plan);
  void assemble_show_monitor_point_status_plan(ExecutePlan *plan);
  void assemble_show_global_monitor_point_status_plan(ExecutePlan *plan);
  void assemble_show_histogram_monitor_point_status_plan(ExecutePlan *plan);
  void assemble_dbscale_show_outline_monitor_info_plan(ExecutePlan *plan);
  void assemble_dbscale_create_outline_hint_plan(
      ExecutePlan *plan, dbscale_operate_outline_hint_node *oper);
  void assemble_dbscale_flush_outline_hint_plan(
      ExecutePlan *plan, dbscale_operate_outline_hint_node *oper);
  void assemble_dbscale_show_outline_hint_plan(
      ExecutePlan *plan, dbscale_operate_outline_hint_node *oper);
  void assemble_dbscale_delete_outline_hint_plan(
      ExecutePlan *plan, dbscale_operate_outline_hint_node *oper);
  void assemble_com_query_prepare_plan(ExecutePlan *plan, DataSpace *space,
                                       const char *query_sql, const char *name);
  void assemble_com_query_exec_prepare_plan(ExecutePlan *plan, const char *name,
                                            var_item *var_item_list);
  void assemble_com_query_drop_prepare_plan(ExecutePlan *plan,
                                            DataSpace *dataspace,
                                            const char *name, const char *sql);
  void assemble_dbscale_flush_plan(ExecutePlan *plan, dbscale_flush_type type);
  void assemble_dbscale_flush_weak_pwd_plan(ExecutePlan *plan);
  void assemble_dbscale_flush_config_to_file_plan(ExecutePlan *plan,
                                                  const char *file_name,
                                                  bool flush_all);
  void assemble_dbscale_flush_acl_plan(ExecutePlan *plan);
  void assemble_dbscale_flush_table_info_plan(ExecutePlan *plan,
                                              const char *schema_name,
                                              const char *table_name);
  void assemble_show_table_location_plan(ExecutePlan *plan,
                                         const char *schema_name,
                                         const char *table_name);
  void assemble_dbscale_dynamic_update_white_plan(
      ExecutePlan *plan, bool is_add, const char *ip,
      const ssl_option_struct &ssl_option_value, const char *comment,
      const char *user_name);
  void assemble_dbscale_show_user_sql_count_plan(ExecutePlan *plan,
                                                 const char *user_id);
  void assemble_show_pool_info_plan(ExecutePlan *plan);
  void assemble_show_pool_version_plan(ExecutePlan *plan);
  void assemble_show_lock_usage_plan(ExecutePlan *plan);
  void assemble_show_execution_profile_plan(ExecutePlan *plan);
  void assemble_show_datasource_plan(ExecutePlan *plan);
  void assemble_show_dataserver_plan(ExecutePlan *plan);
  void assemble_show_backend_threads_plan(ExecutePlan *plan);
  void assemble_show_partition_scheme_plan(ExecutePlan *plan);
  void assemble_show_schema_plan(ExecutePlan *plan, const char *schema);
  void assemble_show_table_plan(ExecutePlan *plan, const char *schema,
                                const char *table, bool use_like);
  void assemble_show_migrate_clean_tables_plan(ExecutePlan *plan);
  void assemble_migrate_clean_tables_plan(ExecutePlan *plan);
  void assemble_show_engine_lock_waiting_status_plan(ExecutePlan *plan,
                                                     int engine_type);
  void assemble_dbscale_get_global_consistence_point_plan(ExecutePlan *plan);
  void assemble_dbscale_set_plan(ExecutePlan *plan);
  void assemble_dbscale_set_rep_strategy_plan(ExecutePlan *plan);
  void assemble_dbscale_set_server_weight(ExecutePlan *plan);
  void assemble_show_option_plan(ExecutePlan *plan);
  void assemble_show_dynamic_option_plan(ExecutePlan *plan);
  void assemble_change_startup_config_plan(ExecutePlan *plan);
  void assemble_dbscale_create_oracle_sequence_plan(ExecutePlan *plan);
  void assemble_show_changed_startup_config_plan(ExecutePlan *plan);
  void assemble_dbscale_purge_monitor_point_plan(ExecutePlan *plan);
  void assemble_dbscale_clean_monitor_point_plan(ExecutePlan *plan);
  void handle_one_par_table_one_spaces(ExecutePlan *plan);
  void handle_one_par_table_mul_spaces(ExecutePlan *plan);
  void handle_mul_par_table_mul_spaces(ExecutePlan *plan);
  void handle_no_par_table_one_spaces(ExecutePlan *plan);
  void assemble_show_table_status_plan(ExecutePlan *plan);
  void assemble_dbscale_shutdown_plan(ExecutePlan *plan, int cluster_id);
  void assemble_dbscale_request_cluster_id_plan(ExecutePlan *plan);
  void assemble_dbscale_request_cluster_info_plan(ExecutePlan *plan);
  void assemble_dbscale_show_async_task_plan(ExecutePlan *plan);
  void assemble_dbscale_request_cluster_user_status_plan(
      ExecutePlan *plan, const char *id, bool only_show_running,
      bool show_status_count);
  void assemble_dbscale_request_node_info_plan(ExecutePlan *plan);
  void assemble_dbscale_request_cluster_inc_info_plan(ExecutePlan *plan);
  void assemble_dbscale_clean_temp_table_cache(ExecutePlan *plan);
  void check_is_multiple_mode();
  void assemble_dbscale_request_all_cluster_inc_info_plan(ExecutePlan *plan);
  void assemble_set_plan(ExecutePlan *plan, DataSpace *dataSpace = NULL);
  void assemble_keepmaster_plan(ExecutePlan *plan);
  void assemble_dbscale_test_plan(ExecutePlan *plan);
  void assemble_async_task_control_plan(ExecutePlan *plan,
                                        unsigned long long id);
  void assemble_load_dataspace_config_file_plan(ExecutePlan *plan,
                                                const char *filename);
  void assemble_federated_thread_plan(ExecutePlan *plan,
                                      const char *table_name);
  void assemble_dbscale_set_priority_plan(ExecutePlan *plan);
#ifndef CLOSE_ZEROMQ
  void assemble_dbscale_message_service_plan(ExecutePlan *plan);
  void assemble_dbscale_create_binlog_task_plan(ExecutePlan *plan);
  void assemble_dbscale_task_plan(ExecutePlan *plan);
  void assemble_dbscale_binlog_task_add_filter_plan(ExecutePlan *plan);
  void assemble_dbscale_drop_task_filter_plan(ExecutePlan *plan);
  void assemble_dbscale_drop_task_plan(ExecutePlan *plan);
  void assemble_dbscale_show_client_task_status_plan(
      ExecutePlan *plan, const char *task_name, const char *server_task_name);
  void assemble_dbscale_show_server_task_status_plan(ExecutePlan *plan,
                                                     const char *task_name);
#endif
  void assemble_dbscale_wise_group_plan(ExecutePlan *plan,
                                        list<AggregateDesc> &aggr_list,
                                        string new_sql);
  void assemble_dbscale_pages_plan(ExecutePlan *plan,
                                   list<AggregateDesc> &aggr_list,
                                   string new_sql, int page_size);
  void assemble_create_trigger_plan(ExecutePlan *plan);
  void assemble_dbscale_show_create_oracle_seq_plan(ExecutePlan *plan);
  void assemble_dbscale_show_seq_status_plan(ExecutePlan *plan);
  void assemble_dbscale_show_trx_block_info_plan(ExecutePlan *plan,
                                                 bool is_loacl);
  void assemble_dbscale_internal_set_plan(ExecutePlan *plan);
  void assemble_dbscale_restore_table_plan(ExecutePlan *plan,
                                           const char *schema,
                                           const char *table);
  void assemble_dbscale_clean_recycle_table_plan(ExecutePlan *plan,
                                                 const char *schema,
                                                 const char *table);
  void assemble_dbscale_show_prepare_cache_status_plan(ExecutePlan *plan);
  void assemble_dbscale_flush_prepare_cache_hit_plan(ExecutePlan *plan);
  record_scan *find_root_union_record_scan(record_scan *rs,
                                           record_scan **last_union);
  ExecuteNode *generate_order_by_subplan(ExecutePlan *plan, record_scan *rs,
                                         list<AggregateDesc> &aggr_list,
                                         list<AvgDesc> &avg_list,
                                         ExecuteNode **tail_node);
  ExecuteNode *generate_aggregate_by_subplan(ExecutePlan *plan, record_scan *rs,
                                             list<AggregateDesc> &aggr_list,
                                             list<AvgDesc> &avg_list,
                                             ExecuteNode **tail_node);
  record_scan *generate_select_plan_nodes(ExecutePlan *plan,
                                          vector<unsigned int> *par_ids,
                                          PartitionedTable *par_table,
                                          vector<ExecuteNode *> *nodes,
                                          const char *query_sql,
                                          bool need_check_distinct = true);
  void fullfil_par_key_equality(record_scan *rs, const char *schema_name,
                                const char *table_name, const char *table_alias,
                                const char *key_name,
                                vector<string> *added_equality_vector = NULL);
  void fullfil_par_key_equality_full_table(
      record_scan *rs, const char *schema_name, const char *table_name,
      const char *table_alias, const char *key_name,
      vector<string> *added_equality_vector);
  void handle_par_key_equality_full_table(record_scan *root);
  ExecuteNode *generate_limit_subplan(ExecutePlan *plan, record_scan *rs,
                                      ExecuteNode **tail);
  long get_limit_clause_offset_value(IntExpression *limit_offset,
                                     ExecutePlan *plan, record_scan *rs);
  void handle_select_expr(record_scan *rs);
  void handle_avg_function(record_scan *rs, list<AvgDesc> &avg_list);
  void reset_avg_sql(string column_name, AvgDesc &desc);
  void add_avg_func(int field_num, Expression *expr);
  void re_parser_stmt(record_scan **new_rs, Statement **new_stmt,
                      const char *query_sql);
  bool group_by_all_keys(record_scan *rs);
  table_link *get_one_modify_table();
  bool can_tables_associate(table_link *tables);
  bool is_tables_share_same_dataserver(table_link *tables);
  bool table_contains_set_list(table_link *tables);
  void get_modify_tables(vector<table_link *> *modify_tables);
  bool first_table_contains_all_data(table_link *tables);
  bool table_contains_all_data(table_link *tables, table_link *first_table);

  void generate_execution_plan_with_no_table(ExecutePlan *plan);

  bool prepare_two_phase_sub_sql(vector<string *> **columns,
                                 table_link *modify_table);

  void assemble_two_phase_insert_part_select_part_plan(
      ExecutePlan *plan, table_link *modify_table,
      vector<unsigned int> *key_pos, PartitionedTable *par_table_select,
      vector<unsigned int> *par_ids);

  void add_dataspace_to_map(
      map<DataSpace *, list<lock_table_item *> *> &lock_table_list,
      DataSpace *space, lock_table_item *item);
  void build_lock_map(
      ExecutePlan *plan,
      map<DataSpace *, list<lock_table_item *> *> &lock_table_list);
  void deal_lock_table(ExecutePlan *plan);
  void authenticate_statement(ExecutePlan *plan);
  bool is_dbscale_command(stmt_type type) {
    if (type > DBSCALE_ADMIN_COMMAND_START &&
        type < DBSCALE_ADMIN_COMMAND_END) {
      return true;
    }
    return false;
  }
  void assemble_create_db_plan(ExecutePlan *plan);

  void handle_rename_table(ExecutePlan *plan, rename_table_node_list *head);
  void assemble_rename_normal_table(ExecutePlan *plan, DataSpace *dataspace);
  void assemble_drop_mul_table(ExecutePlan *plan, bool is_view = false);
  void assemble_rename_partitioned_table(ExecutePlan *plan,
                                         PartitionedTable *old_table,
                                         PartitionedTable *new_table,
                                         const char *old_schema_name,
                                         const char *old_table_name,
                                         const char *new_schema_name,
                                         const char *new_table_name);
  bool check_and_assemble_multi_send_node(ExecutePlan *plan);
  bool has_same_partitioned_key(PartitionedTable *old_part_table,
                                PartitionedTable *new_part_table);

  const char *expr_is_simple_value(Expression *expr);
  const char *get_str_from_simple_value(Expression *expr);
  const char *get_gmp_from_simple_value(Expression *expr);
  void clear_simple_expression();

  virtual FieldExpression *do_get_field_expression(int col_index,
                                                   Expression *expr) = 0;
  void handle_expression(Expression *expr, Expression **parent, record_scan *rs,
                         STMT_EXPR_TYPE stmt_expr_type);
  int create_new_select_item(Expression *expr, record_scan *rs,
                             bool for_order_by_list = false);

  void prepare_select_item(record_scan *rs);
  void modify_select_item(record_scan **new_rs, Statement **new_stmt,
                          const char *query_sql, size_t sql_len);
  void add_group_item_list(order_item *start, list<SortDesc> &group_list,
                           record_scan *rs);
  string get_column_collation(Expression *expr, record_scan *rs,
                              bool check_alias = true);
  string get_column_collation_using_schema_table(
      string schema_name, string table_name, string column_name,
      bool &is_column_collation_cs_or_bin, bool &is_data_type_string_like);

  TableStruct get_table_struct_from_record_scan(string column_name,
                                                record_scan *rs);
  void add_order_item_list(order_item *start, list<SortDesc> &group_list,
                           record_scan *rs);
  bool can_use_one_table_plan(ExecutePlan *plan, DataSpace *space,
                              stmt_node *st);
  bool child_rs_has_join_tables(record_scan *root);
  bool check_field_list_contains_invalid_function(record_scan *rs);
  bool check_field_list_invalid_distinct(record_scan *rs);
  FieldFunctionSituation expression_contain_function_type(
      Expression *expr, record_scan *rs, bool error_on_unknown_expr,
      bool is_check_for_update = false);
  bool is_expression_contain_aggr(Expression *expr, record_scan *rs,
                                  bool error_on_unknown_expr,
                                  bool is_check_for_update = false);
  bool is_expression_contain_invaid_function(Expression *expr, record_scan *rs,
                                             bool error_on_unknown_expr,
                                             bool is_check_for_update = false);
  void check_single_par_table_in_sub_query();

  void rectify_auto_inc_key_pos(name_item *column_list, const char *schema_name,
                                size_t schema_len, const char *table_name,
                                size_t table_len, const char *table_alias);

  void rebuild_union_all_sql_and_generate_execution_plan(ExecutePlan *plan);
  void build_union_node_group(ExecutePlan *plan);
  void build_max_union_node_group();
  void get_union_all_node_spaces(ExecuteNode *node, set<DataSpace *> &spaces);
  void generate_union_all_sub_sql_and_execution_nodes(ExecutePlan *plan,
                                                      const char *sub_query);
  void get_sub_sqls(record_scan *scan, string sql);
  bool check_union_contain_star(record_scan *rs);
  DataSpace *get_sub_sqls_poses(record_scan *scan);
  void check_found_rows_with_group_by(ExecutePlan *plan);
  void deal_select_assign_uservar(record_scan *rs);
  void check_load_local_infile(ExecutePlan *plan);
  void prepare_insert_select_via_fifo(ExecutePlan *plan,
                                      DataServer *modify_server,
                                      const char *schema, const char *table,
                                      bool load_select);
  void prepare_insert_select_via_load(ExecutePlan *plan, const char *schema,
                                      const char *table);
  DataSpace *merge_par_tables_one_record_scan(record_scan *root);

  DataSpace *merge_dataspace_one_record_scan(record_scan *root,
                                             DataSpace *par_ds);

  DataSpace *merge_parent_child_record_scan(record_scan *parent_rs,
                                            record_scan *child_rs,
                                            int record_scan_position,
                                            ExecutePlan *plan);

  void analysis_record_scan_tree(record_scan *root, ExecutePlan *plan,
                                 unsigned int recursion_level = 0);

  bool check_rs_has_table(record_scan *root);
  bool sub_query_need_cross_node_join(DataSpace *ds_tmp, record_scan *cur_rs);
  DataSpace *handle_cross_node_join(record_scan *rs, ExecutePlan *plan,
                                    bool force_no_merge = false);

  DataSpace *handle_cross_node_join_with_subquery(record_scan *rs);

  bool can_two_par_table_record_scan_merge(record_scan *rs1, record_scan *rs2);
  bool can_par_global_table_record_scan_merge(record_scan *rs1,
                                              record_scan *rs2);

  void analysis_sequence_of_join_tables(
      vector<vector<TableStruct *> *> *join_table_sequence);
  TableStruct *get_join_final_table(vector<TableStruct *> *vec);
  void set_join_final_table(record_scan *rs);
  void set_vec_final_table(vector<vector<TableStruct *> *> *vec);

  bool check_two_partition_table_can_merge(record_scan *rs, table_link *tb_link,
                                           PartitionedTable *tb1,
                                           PartitionedTable *tb2);

  bool check_CNJ_two_partition_table_can_merge(record_scan *rs,
                                               table_link *tb_link1,
                                               table_link *tb_link2,
                                               PartitionedTable *tb1,
                                               PartitionedTable *tb2);
  void init_table_subquery_dataspace(record_scan *rs, ExecutePlan *plan);
  void init_column_table_subquery_tmp_table_name(record_scan *rs,
                                                 DataSpace *column_space,
                                                 ExecutePlan *plan);
  string init_left_join_tmp_table_dataspace(table_link *table);
  string get_key_for_left_join_tmp_table(Expression *condition,
                                         table_link *table);
  string get_column_in_table(string peer_string, table_link *table);
  string get_column_from_peer(string peer_string);
  string get_key_for_table_subquery(record_scan *rs);
  DataSpace *check_subquery_table_can_merge();
  TableStruct *get_subquery_table_struct(record_scan *rs);

  void init_sp_worker_from_create_sql(SPWorker *spworker, ExecutePlan *plan,
                                      const char *create_sql,
                                      const char *routine_schema,
                                      const char *cur_schema, string &sp_name);

  bool recursive_check_call_stmt_dataspace_info(
      Statement *stmt, const char *top_sp_schema, const char *cur_schema_name,
      bool &has_partial_parsed, bool &has_non_single_node_stmt,
      SPExecuteFlag &sp_exe_flag, bool &stored_procedure_contain_cursor,
      DataSpace **data_space_sub_call, string &sp_name);
  void set_order_by_indexs(record_scan *rs);
  void set_limit_str(ExecutePlan *plan, record_scan *rs);
  string get_expr_or_alias_in_select_items(int column_index, record_scan *rs,
                                           bool is_union);
  string append_order_by_to_union(record_scan *rs, const char *ori_sql);
  string append_limit_to_union(const char *ori_sql);
  void set_call_store_procedure_directly(bool b) {
    is_call_store_proc_directly = b;
  }
  TableStruct check_parent_table_alias(record_scan *parent_rs,
                                       TableStruct table);

  bool get_is_sep_node_parallel_exec() { return is_sep_node_parallel_exec; }
  bool get_explain_result_one_source(
      list<list<string> > *explain_result, Handler *handler,
      DataSpace *dataspace, Statement *stmt, string *sql, unsigned int row_id,
      string *exec_node_name, bool is_server_explain_stmt_always_extended);
  void assemble_explain_plan(ExecutePlan *plan);
  bool check_left_join_with_cannot_merge_right_par(record_scan *rs);
  bool check_left_join_sub_left(record_scan *rs);
  bool check_mul_table_left_join_sub(record_scan *rs);
  bool check_left_join(table_link *table);
  bool check_left_join_on_par_key(table_link *table);
  bool check_column_in_condition(Expression *condition, const char *column,
                                 table_link *table);
  bool check_column_in_peer(string peer, const char *column, table_link *table);
  void handle_column_subquery(record_scan *root);
  bool check_rs_contain_partkey_equal_value(record_scan *rs);
  bool check_express_contain_partkey_equal_value(Expression *where_expr,
                                                 const char *schema_name,
                                                 const char *table_name,
                                                 const char *key_name,
                                                 const char *alias_name);
  bool check_column_record_scan_can_merge(
      record_scan *parent_rs, record_scan *child_rs,
      int check_column_record_scan_can_merge, ExecutePlan *plan);
  TableStruct get_table_from_record_scan(record_scan *parent_rs,
                                         string peer_string);
  bool is_separated_exec_result_as_sub_query(record_scan *rs);
  TableStruct *check_column_belongs_to(
      const char *column_name,
      vector<vector<TableStruct> > *table_vector_vector);
  void adjust_cross_node_join_rs(record_scan *rs, TableStruct table_parent);
  void init_uservar(ExecutePlan *plan, Session *work_session);
  void adjust_create_drop_db_sql_for_shard(DataSpace *space,
                                           const char *schema_name,
                                           string &new_sql, int db_name_end_pos,
                                           const char *sql);
  void replace_grant_hosts();
  void replace_set_pass_host();
  string get_key_for_left_join_subquery_tmp_table(Expression *condition,
                                                  const char *schema_name,
                                                  const char *table_alias);
  string get_column_in_table_without_throw(string peer_string,
                                           string schema_name,
                                           string table_alias);
  bool contains_left_join_subquery(record_scan *rs, DataSpace *ds);
  bool only_has_one_non_global_tb_can_merge_global(record_scan *root,
                                                   DataSpace *ds);
  void prepare_dataspace_for_leftjoin_subquery(join_node *join_tables);
  const char *get_partition_key_for_auto_space();

  bool child_rs_is_partitioned_space(record_scan *root);
  void print_sql_for_rs(record_scan *root);
  void set_charset_and_isci_accroding_to_collation(string &collation_name,
                                                   CharsetType &ctype,
                                                   bool &is_ci);
  void handle_related_condition_full_table(
      record_scan *rs, list<TableStruct *> *table_struct_list,
      map<TableStruct *, set<TableStruct *> > *table_map);
  bool check_tables_has_related_condition(
      TableStruct *table, list<TableStruct *> *merge_list,
      map<TableStruct *, set<TableStruct *> > *table_map);
  TableStruct *find_table_struct_from_list(
      const char *schema_name, const char *table_name, const char *alias,
      list<TableStruct *> *table_struct_list);
  void get_related_condition_full_table(
      Expression *condition, list<TableStruct *> *table_struct_list,
      map<TableStruct *, set<TableStruct *> > *table_map,
      map<string, set<TableStruct *> > *equal_values);
  TableStruct *find_column_table_from_list(
      string column_str, list<TableStruct *> *table_struct_list);
  TableStruct *check_column_belongs_to(const char *column_name,
                                       list<TableStruct *> *table_struct_list);
  void handle_connect_by_expr(connect_by_node *conn_node);
  string generate_full_column_name(string expr_str);
  bool is_partition_table_contain_partition_key(const char *par_key);
  void check_create_or_alter_table_restrict(const char *shcema_name,
                                            const char *table_name);
  void assemble_dbscale_show_fetchnode_buffer_usage_plan(ExecutePlan *plan);

 protected:
  const char *sql;
  string sql_tmp;
  string view_sql;
  string exec_sql_after_separated_exec;
  stmt_node st;
  string insert_sql;
  string create_tb_sql_with_create_db_if_not_exists;
  char schema[256 + 1];
  string sql_schema;
  string select_sub_sql;
  string modify_sub_sql;
  string outfile_sql;
  string select_into_sql;
  string var_sql;
  vector<const char *> grant_user_name_vec;
  string select_var_sql;
  string no_distinct_sql;
  string sql_sequence_val_replaced;
  string no_part_table_def_sql;
  vector<string> uservar_list;
  vector<string> sessionvar_list;
  set<string> useful_session_vars;
  string rownum_tmp_sql;
  // in set statement, if contain ignore session variables, rebuild the sql
  string session_var_sql;
  bool has_ignore_session_var;
  CharsetType ctype;

  bool partial_parsed;

  string fetch_sql_tmp;
  AutoIncStatus auto_inc_status;
  int64_t old_auto_inc_val;
  int64_t old_last_insert_id;
  int auto_increment_key_pos;
  string auto_inc_key_name;

  bool is_insert_select_via_fifo;
  bool use_load_insert_select;
  string insert_select_modify_schema;
  string insert_select_modify_table;
  string local_load_script;

  string union_sub_talbe_name;
  bool union_table_sub;
  bool need_clean_new_field_expressions;
  bool need_clean_exec_nodes;
  bool need_clean_record_scan_par_key_ranges;
  bool need_clean_record_scan_par_key_equality;
  bool need_clean_par_tables;
  bool need_clean_spaces;
  bool need_clean_record_scan_par_table_map;
  bool need_clean_record_scan_all_spaces_map;
  bool need_clean_record_scan_all_par_tables_map;
  bool need_clean_record_scan_one_space_tmp_map;
  bool need_clean_record_scan_all_table_spaces;
  bool need_clean_select_uservar_vec;
  bool need_clean_group_by_list;
  bool update_on_one_server;
  bool delete_on_one_server;
  bool union_all_sub;
  int auto_inc_step;
  bool stmt_allow_dot_in_ident;

  // current saved auto inc value that has just used in current statement.
  int64_t multi_row_insert_saved_auto_inc_value;

  // the max valid auto inc value of current statement when get auto
  // inc value from last time, curr stmt should re-fetch new auto inc value
  // from dataspace if this param is touched by
  // multi_row_insert_saved_auto_inc_value.
  int64_t multi_row_insert_max_valid_auto_inc_value;

  list<string> union_all_sub_sqls;
  vector<ExecuteNode *> union_all_nodes;
  ExecuteNode *union_all_node;
  list<Statement *> union_all_stmts;
  map<unsigned int, unsigned int> union_all_sub_sql_poses;
  vector<set<DataSpace *> > union_space_group;
  vector<list<ExecuteNode *> > union_node_group;

  // To identify if the query is regenerated from the federated execute sql.
  bool fe_join;
  /* For query comes from federated table, we store the generated execute node
     in this vector, set the dispatch node as the start node.*/
  vector<ExecuteNode *> fe_join_nodes;
  /* for some function, we need re_parser sql, then we should keep these new
   * statement then release them together.*/
  list<Statement *> re_parser_stmt_list;

  // Number of partition table used in stmt after analysis
  int par_table_num;

  // if global table merge with another table,and the final table is not global
  // table push the global table into this vector
  unordered_set<DataSpace *> global_table_merged;

  /*Partition tables used in stmt after analysis. If two partition table can
   * be merged, dbscale will only record one*/
  vector<table_link *> par_tables;

  // Dataspaces can be used to execute sql after analysis
  vector<DataSpace *> spaces;
  vector<char *> simple_expression;

  // vector of separated exec node generated for this stmt
  vector<SeparatedExecNode *> exec_nodes;

  /*Map to store the vector id of dataspace for each record_scan. We only
   * support one dataspace for each record_scan, after possible merge.*/
  map<record_scan *, int> record_scan_space_map;

  /*Map to store the vector id of partition table for each record_scan. We
   * only support one parititon table for each record_scan, after possible
   * merge.*/
  map<record_scan *, table_link *> record_scan_par_table_map;

  /*Map to store all table dataspaces for one specified record_scan.*/
  map<record_scan *, list<DataSpace *> > record_scan_all_spaces_map;

  /*Map to store all partition tables for one specified record_scan.*/
  map<record_scan *, list<table_link *> > record_scan_all_par_tables_map;

  /*Map to store the temporary merged dataspace for each record_scan.*/
  map<record_scan *, DataSpace *> record_scan_one_space_tmp_map;

  /*Map to store table subquery's dataspace */
  map<record_scan *, DataSpace *> record_scan_table_subquery_space_map;

  /*Map to store the dependent record_scan list of each record_scan. The
   * dependent record_scan is separated execution record_scan.*/
  map<record_scan *, list<record_scan *> > record_scan_dependent_map;

  /*Map to store the record_scan, which need cross node join.*/
  map<record_scan *, bool> record_scan_need_cross_node_join;

  /*Map to store the record_scan that contains seperated table subquery.*/
  map<record_scan *, bool> record_scan_seperated_table_subquery;

  /* Map to subquery final dataspaces, this map can be only used in table
   * subquery. */
  map<record_scan *, DataSpace *> table_subquery_final_dataspace;
  map<record_scan *, string> dataspace_partition_key;
  bool need_reanalysis_space;

  /* Map to the table which its subquery missing table, the key of the map is
   * parent record scan. For example, select * from t2 where t2.c1 = (select
   * min(t1.c2) from t1 where t1.c1 = t2.c1); t2 in subquery is the missing
   * table.
   */
  map<record_scan *, TableStruct> subquery_missing_table;

  /* Map to record_scan need the column */
  map<record_scan *, TableStruct> record_scan_need_table;

  /*Map to store the relation between the record_scan and separed execution
   * node.*/
  map<record_scan *, SeparatedExecNode *> record_scan_exec_node_map;

  /* Map to store all the equality value of the kept partition key.
   *
   *   (map<rs, map<kept_key, vector<equality values>>>)*/
  map<record_scan *, map<const char *, vector<string> > >
      record_scan_par_key_equality;

  /* This variable is only used in CROSS NODE JOIN can merge situation. It is
   * similar with record_scan_par_key_equality. Each record_scan contains one
   * pair of (key, vector<const char *>) in record_scan_par_key_equality.
   * Each record_scan contains many pairs of (key, vector<const char *>) in
   * record_scan_par_key_equality_full_table.  */
  map<record_scan *, map<string, vector<string> > >
      record_scan_par_key_equality_full_table;

  /* Map to store the const values of kept parition key (and its equalities).
   * If it is empty, it means all possible partitions are need.*/
  map<record_scan *, map<const char *, vector<const char *> > >
      record_scan_par_key_values;

  /* Map to store the range values of kept parition key.
   * If it is empty, it means all possible partitions are need.
   */
  map<record_scan *, map<const char *, RangeValues *> >
      record_scan_par_key_ranges;

  /**
   * Map to store multi-row insert sqls, partition_id as key, sql which
   * used to insert into partition server as value.
   */
  map<unsigned int, string> multi_insert_id_sqls;

  /**
   * Map to store tables used for cross node join.
   */
  map<record_scan *, vector<vector<TableStruct *> *> *> record_scan_join_tables;

  /**
   * Map to store table subquery tmp table names.
   */
  map<record_scan *, string> record_scan_subquery_names;

  /*
   * Map one union record scan to last union record scan.
   */
  map<record_scan *, record_scan *> last_union_rs_map;
  /*
   * Map one union record scan to all its union leaf record_scan.
   */
  map<record_scan *, list<record_scan *> > union_leaf_record_scan;
  /**
   * Map to store table subquery tmp table idents, if parent of the
   * record scan is a cross node join, the tmp table idents will be
   * set to the join sequence vector, when use the vector to get create
   * sqls, these tmp table idents will be replaced to real tmp table name.
   */
  map<record_scan *, string> record_scan_subquery_names_for_replace;
  /**
   * Map to store table subquery tmp table idents and the real tmp table name.
   * if the subquery's parent is a cross node join, the tmp table idents will
   * be set in cross node join sequence vector, when we use the cross node join
   * sequence vector to get create sqls, these tmp table idents will be replaced
   * to real tmp table names.
   */
  map<string, string> record_scan_real_subquery_names;
  ACE_Thread_Mutex real_subquery_names_mutex;
  /**
   * Map to store the TableStruct and missing table_link;
   */
  map<TableStruct, table_link *> missing_table_map;

  /* Map used to store the dataspace for each table_link to reduce the invoke
   * of get_data_space_for_table */
  map<table_link *, DataSpace *> record_scan_all_table_spaces;
  /* Store the seperate node output dataspace for dependent subquery, the key is
   * parent record_scan*/
  map<record_scan *, map<DataSpace *, DataSpace *> >
      gtable_dependent_output_dataspace;

  map<record_scan *, map<DataSpace *, DataSpace *> > gtable_can_merge_dataspace;

  /* Store the output space for the column subquery, which will store the
   * result into a tmp table, which space is the stored value in this map.*/
  map<record_scan *, DataSpace *> column_output_dataspace;

  /* Map to store the tmp table name for left join right table.*/
  map<record_scan *, map<int, string> > left_join_table_position;
  ACE_Thread_Mutex cross_join_mutex;
  map<record_scan *, map<int, TableStruct *> > left_join_tables;
  set<TableStruct *> left_join_right_tables;
  map<table_link *, pair<record_scan *, int> > left_join_par_tables;

  /* map used for left join, first  record_scan means current rs, integer means
   * the position of the record_scan in first record_scan*/
  map<record_scan *, map<int, record_scan *> >
      left_join_record_scan_position_subquery;
  /* set contains the left join subquery situation. */
  set<record_scan *> left_join_subquery_rs;
  set<record_scan *> record_scan_has_no_table;

  int subquery_table_id;
  int new_select_item_num;
  list<string> new_select_items;
  list<FieldExpression *> new_field_expressions;
  list<SortDesc> group_by_list;
  list<SortDesc> order_by_list;
  Expression *having;
  bool groupby_all_par_keys;
  bool order_in_group_flag;
  bool simple_having;
  bool simple_field_expr;
  bool need_deal_field_expr;
  list<AvgDesc> avg_list;
  list<SelectExprDesc> select_expr_list;
  bool select_uservar_flag;
  unsigned int select_field_num;
  vector<pair<unsigned int, string> > select_uservar_vec;

  sub_select_type subquery_type;
  int num_of_separated_exec_space;
  int num_of_separated_par_table;
  bool cross_node_join;
  bool spark_insert;
  ExecuteNode *spark_node;
  record_scan *cross_node_join_rec_scan;

  list<pair<int, order_dir> > union_order_by_index;
  string union_limit_str;
  string union_sub_table_name;
  Statement *first_union_stmt;
  vector<vector<TableStruct> > union_table_vector;
  string current_sp_worker_name;
  bool is_call_store_proc_directly;

  bool is_may_cross_node_join_related;

  bool need_clean_simple_expression;

  bool is_sep_node_parallel_exec;  // flag to indicate whether current stmt is
                                   // execution separated node parallelly

  /* This map is used to store the subquery have the peer string. */
  map<record_scan *, string> subquery_peer;

  /* The vec to keep the sql string of shard sql, which will be used during
   * execution. We only use this vector to keep the string object and will not
   * use this vec directly.*/
  list<string> shard_sql_list;
  bool need_clean_shard_sql_list;

  list<string> statement_stored_string;
  bool need_clean_statement_stored_string;

  list<string> grant_sqls;

  bool spark_invest_stmt;

  bool need_merge_subquery;
  ConnectByDesc connect_by_desc;
  bool is_connect_by;
  bool is_update_set_subquery;

 public:
  /*
   * for dbscale status
   * just record the value in parser func
   * the value will be passed to mysqlsession
   */
  bool is_partial_parsing_without_table_name;
  bool is_partial_parsing_related_to_table;
  struct one_table_struct one_table_node;
  bool is_need_parse_transparent();

  string full_table_name;

 protected:
  vector<SeparatedExecNode *> work_vec;
  vector<SeparatedExecNode *> fin_vec;
  size_t work_pointer;
  size_t fin_size;
  size_t running_threads;
  ACE_Thread_Mutex sep_node_mutex;
  ACE_Condition<ACE_Thread_Mutex> sep_node_cond;
  Session *stmt_session;

 public:
  bool get_allow_dot_in_ident() { return stmt_allow_dot_in_ident; }
  void set_stmt_allow_dot_in_ident(bool b) { stmt_allow_dot_in_ident = b; }
  bool support_navicat_profile_sql(ExecutePlan *plan);
  bool check_and_replace_par_distinct_with_groupby(
      table_link *tb_link, const char *_query_sql = NULL);
  void fulfill_hex_pos(insert_op_node *insert_op);
  void fulfill_hex_pos_real(const char *schema_name, const char *table_name,
                            set<string> col_names);
  void expend_field_list(insert_op_node *insert_op,
                         vector<string> &expend_columns,
                         const char *handle_sql);
  void expend_field_list_real(field_item *field, vector<string> &expend_columns,
                              const char *handle_sql,
                              list<TableNameStruct> &tns_list, record_scan *rs);
  void adjust_select_sub_sql_for_hex(vector<string> expend_columns);
  void handle_insert_select_hex_column();
  set<int> &get_hex_pos() { return hex_pos; }
  void fulfill_tb_list(record_scan *rs, table_link *tl,
                       list<TableNameStruct> *tns_list);
  unsigned int get_select_part_start_pos(record_scan *rs);
  void init_parallel_separated_node_execution();
  void loop_and_find_executable_sep_nodes();
  bool assign_threads_for_work_vec();
  void report_finish_separated_node_exec(SeparatedExecNode *node,
                                         bool get_exception);
  void clean_up_parallel_separated_node_execution();
  bool wait_threads_for_execution();
  void check_fin_tasks();
  bool sep_node_can_execute_parallel(ExecutePlan *plan);
  /* The map is used to store the merging subquery position and merge_table.
   * Position means the sequence number of the record_scan.  Merge_table means
   * the dataspace of subquery. */
  map<record_scan *, map<int, TableStruct> > can_merge_record_scan_position;

 private:
  string explain_sql;
  /* explain_info is a container to store explain info,
     the inner list is element of a row of final result set,
     each row contains totally 12 columns:
     "exec_node_depth * '-' + dbscale_exec_node_name", "data_source_name" and
     the result from MySQL explain result */
  list<list<string> > explain_info;

 public:
  list<list<string> > *get_explain_info() { return &explain_info; }
  void clean_explain_info() { explain_info.clear(); }
  /* When cross node join need to add an string to record scan when reparse
   * the sql, put the string to this map. */
  map<record_scan *, string> record_scan_add_string;

 public:
  PartitionedTable *get_create_part_tb_def() { return create_part_tb_def; }
  void reset_create_part_tb_def() { create_part_tb_def = NULL; }

 private:
  void add_partitioned_table_config_before_create_tb(
      table_link *table, const char *part_table_scheme,
      const char *part_table_key);
  PartitionedTable *create_part_tb_def;
  bool contains_view;

  CenterJoinGarbageCollector cj_garbage_collector;
  list<string> sub_call_sql_list;
  bool need_center_join;
  bool need_spark;

 private:
  void authenticate_db(const char *dbname, ExecutePlan *plan,
                       stmt_type st_type);
  void authenticate_table(ExecutePlan *plan, const char *schema_name,
                          const char *table_name, stmt_type st_type,
                          bool is_sub_select = false);
  void authenticate_global_priv(ExecutePlan *plan, stmt_type st_type);

  void refuse_modify_table_checking();
  bool handle_view_related_stmt(ExecutePlan *plan, table_link **table);
  void handle_coalecse_function(table_link **table);
  void check_acl_and_safe_mode(ExecutePlan *plan);
  void row_count_and_last_insert_id_checking(ExecutePlan *plan);
  DataSpace *get_dataspace_for_explain(ExecutePlan *plan);
  void generate_plan_for_explain_related_stmt(ExecutePlan *plan);
  bool generate_plan_for_federate_session(ExecutePlan *plan, table_link *table);
  table_link *adjust_select_sql_for_funs_and_autocommit(ExecutePlan *plan,
                                                        table_link *table);
  void generate_plan_for_prepare_related_stmt(ExecutePlan *plan);
  void generate_plan_for_flush_config_to_file(ExecutePlan *plan);
  void generate_plan_for_flush_acl(ExecutePlan *plan);
  void generate_plan_for_flush_table_info(ExecutePlan *plan,
                                          const char *schema_name,
                                          const char *table_name);
  void adjust_sql_for_into_file_or_select_into(table_link **table);
  void generate_plan_for_partial_parse_stmt(ExecutePlan *plan,
                                            const char *schema_name,
                                            const char *table_name,
                                            table_link *table);
  void generate_plan_for_rename_or_drop_table_stmt(ExecutePlan *plan);
  void check_part_table_primary_key(const char *part_key);
  bool prepare_dataspace_for_extend_create_tb_stmt(ExecutePlan *plan,
                                                   table_link *table);
  void generate_plan_for_one_table_situation(ExecutePlan *plan,
                                             table_link *one_table,
                                             DataSpace *one_space);
  void handle_separated_nodes_before_finial_execute(ExecutePlan *plan);
  void generate_plan_for_set_stmt(ExecutePlan *plan);
  bool generate_plan_according_to_par_table_num_and_spaces(ExecutePlan *plan);
  void generate_plan_for_non_support_trx_stmt(ExecutePlan *plan);
  void check_stmt_for_lock_mode(ExecutePlan *plan);
  void generate_plan_for_information_schema(ExecutePlan *plan,
                                            const char *table_name,
                                            table_link *table);
  void pre_work_for_non_one_table_exec_plan(ExecutePlan *plan,
                                            bool only_one_table,
                                            record_scan *one_rs,
                                            DataSpace *one_space,
                                            table_link *one_table);
  bool generate_plan_for_no_table_situation(ExecutePlan *plan);
  void get_dbscale_group_items(map<int, pair<int, string> > &replace_items,
                               set<string> &item_alias,
                               list<AggregateDesc> &aggr_list);
  void get_dbscale_page_items(map<int, pair<int, string> > &replace_items,
                              list<AggregateDesc> &aggr_list);
  void generate_dbscale_wise_group_plan(ExecutePlan *plan);
  void generate_dbscale_pages_plan(ExecutePlan *plan);
  void generate_new_sql(map<int, pair<int, string> > replace_items,
                        string &new_sql);

  void add_drop_partition_acl_check(ExecutePlan *plan, ACLType acl_type);

  void generate_plan_for_create_tmp_table(ExecutePlan *plan);
  bool handle_executable_comments_before_cnj(ExecutePlan *plan);
  bool handle_executable_comments_after_cnj(ExecutePlan *plan);
  bool is_spark_one_partition_sql();

  void construct_sql_function(PartitionedTable *par_ds,
                              unsigned int partition_num,
                              unsigned int partition_id);
  string null_key_str;

  set<int> hex_pos;

  bool is_empty_top_select_with_limit();
  void move_top_select_limit_to_table_subquery(table_link **table);

  bool is_update_via_delete_insert;
  bool is_update_via_delete_insert_revert;
  bool stmt_with_limit_using_quick_limit;
  DataSpace *update_via_delete_insert_space_insert;
  string update_sql_delete_part;
  string update_sql_insert_part;  // maybe a string of several sqls split by
                                  // semicolon(;)
  string update_sql_insert_part_revert;  // for state_grid, sql to exec in case
                                         // the delete or insert part of update
                                         // sql failed
  void check_auto_inc_value_in_update_set_list(
      Expression *set_list,
      map<string, CompareExpression *, strcasecomp> &set_col_names,
      PartitionedTable *par_space, ExecutePlan *plan, const char *schema_name,
      const char *table_name);
  void set_need_handle_acl_related_stmt(bool b) {
    need_handle_acl_related_stmt = b;
  }
  bool need_handle_acl_related_stmt;

 public:
  bool check_is_select_version_comment();
  bool get_stmt_with_limit_using_quick_limit() {
    return stmt_with_limit_using_quick_limit;
  }
  bool get_is_update_via_delete_insert() { return is_update_via_delete_insert; }
  bool get_is_update_via_delete_insert_revert() {
    return is_update_via_delete_insert_revert;
  }
  string &get_update_sql_insert_part_revert() {
    return update_sql_insert_part_revert;
  }
  DataSpace *get_update_via_delete_insert_space_insert() {
    return update_via_delete_insert_space_insert;
  }
  string remove_schema_from_full_columns();

  void check_stmt_sql_for_dbscale_row_id(stmt_type type, record_scan *scanner);
  bool is_first_column_dbscale_row_id;
  bool get_is_first_column_dbscale_row_id() {
    return is_first_column_dbscale_row_id;
  }
  bool get_need_handle_acl_related_stmt() {
    return need_handle_acl_related_stmt;
  }

  ExecuteNode *try_assemble_bridge_mark_sql_plan(ExecutePlan *plan,
                                                 table_link *t);

 public:
  void stmt_dynamic_add_str_cleanup() {
    vector<char *>::iterator it = stmt_dynamic_add_str_vec.begin();
    for (; it != stmt_dynamic_add_str_vec.end(); ++it) {
      delete[](*it);
    }
    stmt_dynamic_add_str_vec.clear();
  }
  void stmt_add_dynamic_str(char *str) {
    stmt_dynamic_add_str_vec.push_back(str);
  }
  std::vector<char *> stmt_dynamic_add_str_vec;

  void do_replace_rownum(record_scan *rs, string &old_sql);
  unsigned int do_add_limit_after_where(record_scan *rs, string &old_sql,
                                        unsigned int add_pos);
  void replace_rownum(record_scan *rs, string &old_sql);
  unsigned int add_limit_after_where(record_scan *rs, string &old_sql,
                                     unsigned int add_pos);
  void replace_rownum_add_limit();

 private:
  void replace_view_select_field_alias(string &view_select_sql);

 private:
  bool check_can_pushdown_for_modify_select(PartitionedTable &select_par_table,
                                            record_scan *select_rs,
                                            vector<ExecuteNode *> &select_nodes,
                                            PartitionedTable *modify_par_table,
                                            DataSpace *modify_space);
  void assemble_modify_node_according_to_fetch_node(
      ExecutePlan &plan, const vector<ExecuteNode *> &fetch_nodes,
      PartitionedTable *modify_par_table);

 public:
  void assemble_move_table_to_recycle_bin_plan(ExecutePlan *plan);
  const char *get_table_from_recycle_bin(const char *from_schema,
                                         const char *from_table);
  DataSpace *create_tmp_dataspace_from_table(const char *from_schema,
                                             const char *from_table,
                                             const char *to_table);
  void destroy_tmp_dataspace(DataSpace *dspace, Schema *the_schema,
                             const char *table_name);
  ExecuteNode *assemble_modify_node_for_partitioned_table(ExecutePlan *plan,
                                                          const char *sql,
                                                          PartitionedTable *pt);
};

}  // namespace sql
}  // namespace dbscale

#endif /* DBSCALE_STATEMENT_H */
