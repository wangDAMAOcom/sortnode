uop.");
    }
    string value, schema_name, table_name, column_name;
    expr->to_string(value);
    if (item != st.scanner->group_by_list) order.append(", ");
    if (expr->type == EXPR_STR) {
      split_column_expr(value, schema_name, table_name, column_name);
      str.assign("`");
      if (!schema_name.empty()) {
        str.append(schema_name);
        str.append("`.`");
      }
      if (!table_name.empty()) {
        str.append(table_name);
        str.append("`.`");
      }
      str.append(column_name);
      str.append("`");
    } else {
      str.append(value);
    }
    order.append(str);
    if (item->order == ORDER_ASC) {
      order.append(" ASC");
    } else if (item->order == ORDER_DESC) {
      order.append(" DESC");
    }
    order_alias.insert(str);
    item = item->next;
  } while (item != st.scanner->group_by_list);

  group_order_item *order_item = st.dbscale_group_order->next;
  do {
    order.append(", ");
    order.append("`");
    order.append(order_item->order_field);
    order.append("`");
    if (order_item->order == ORDER_ASC) {
      order.append(" ASC");
    } else if (order_item->order == ORDER_DESC) {
      order.append(" DESC");
    }
    if (order_alias.count(string(order_item->order_field))) {
      LOG_ERROR("Group by fields should not contain wise order items.\n");
      throw Error("Group by fields should not contain wise order items.");
    }
    order_item = order_item->next;
  } while (order_item != st.dbscale_group_order->next);
  replace_items[st.scanner->group_start_pos] =
      make_pair(st.scanner->group_end_pos, order);
}

void Statement::generate_new_sql(map<int, pair<int, string> > replace_items,
                                 string &new_sql) {
  int last_pos = 0;
  map<int, pair<int, string> >::iterator it = replace_items.begin();
  for (; it != replace_items.end(); it++) {
    new_sql.append(sql + last_pos, it->first - last_pos - 1);
    new_sql.append(it->second.second);
    last_pos = it->second.first;
  }
  new_sql.append(sql + last_pos, strlen(sql) - last_pos);
  LOG_DEBUG("New sql after replace group items [%s].\n", new_sql.c_str());
}

void Statement::assemble_create_trigger_plan(ExecutePlan *plan) {
  const char *schema_name = NULL;
  if (st.routine_d && st.routine_d->tr_node) {
    schema_name = st.routine_d->tr_node->schema_name;
  }
  DataSpace *dataspace = Backend::instance()->get_data_space_for_table(
      schema_name ? schema_name : schema, NULL);
  assemble_direct_exec_plan(plan, dataspace);
}

void Statement::handle_create_or_drop_event() {
  Backend *backend = Backend::instance();
  const char *schema_name = st.sql->event_oper->schema_name
                                ? st.sql->event_oper->schema_name
                                : schema;
  const char *event_name = st.sql->event_oper->event_name;
  DataSpace *dataspace = backend->get_data_space_for_table(schema_name, NULL);
  DataSource *datasource = dataspace->get_data_source();
  if (!datasource) {
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
    LOG_ERROR("Can not get datasource for schema %s\n.", schema_name);
    throw Error("Fail to get datasource.");
  }
  int group_id = datasource->get_group_id();
  char id[32];
  sprintf(id, "%d", group_id);
  if (st.type == STMT_CREATE_EVENT) {
    string query_sql(
        "REPLACE INTO dbscale.events(event_name, group_id) VALUES('");
    query_sql.append(schema_name);
    query_sql.append(".");
    query_sql.append(event_name);
    query_sql.append("', ");
    query_sql.append(id);
    query_sql.append(")");
    try {
      dataspace->execute_one_modify_sql(query_sql.c_str());
    } catch (...) {
      LOG_ERROR("Fail to store event infomation on server.\n");
      throw;
    }
  } else if (st.type == STMT_DROP_EVENT) {
    string query_sql("DELETE FROM dbscale.events WHERE event_name ='");
    query_sql.append(schema_name);
    query_sql.append(".");
    query_sql.append(event_name);
    query_sql.append("'");
    try {
      dataspace->execute_one_modify_sql(query_sql.c_str());
    } catch (...) {
      LOG_ERROR("Fail to remove event infomation on server.\n");
      throw;
    }
  }
}

bool Statement::check_simple_select(record_scan *rs) {
  // rs should not contain distinct
  if (rs->options & SQL_OPT_DISTINCT) return false;

  // rs should not contain aggregate function
  field_item *field = rs->field_list_head;
  try {
    while (field) {
      if (field->field_expr) {
        if (is_expression_contain_aggr(field->field_expr, rs, false)) {
          return false;
        }
      }
      field = field->next;
    }
  } catch (...) {
    return false;
  }

  // rs should not contain group by, having, limit, order by
  if (rs->group_by_list || rs->having || rs->limit || rs->order_by_list)
    return false;

  return true;
}
bool Statement::check_rs_no_invalid_aggr(record_scan *rs) {
  if (rs->options & SQL_OPT_DISTINCT) return false;
  field_item *field = rs->field_list_head;
  try {
    while (field) {
      if (field->field_expr) {
        is_expression_contain_aggr(field->field_expr, rs, false);
      }
      field = field->next;
    }
  } catch (...) {
    return false;
  }
  return true;
}
table_link *Statement::get_one_table_link(record_scan *rs, DataSpace *space) {
  if (!rs || !space) return NULL;
  Backend *backend = Backend::instance();

  table_link *tmp_tb = rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == rs) {
      const char *schema_name =
          tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
      if (!strcasecmp(schema_name, "dbscale_tmp")) {
        tmp_tb = tmp_tb->next;
        continue;
      }
      const char *table_name = tmp_tb->join->table_name;
      DataSpace *tmp_space =
          backend->get_data_space_for_table(schema_name, table_name);
      if (space == tmp_space) return tmp_tb;
    }
    tmp_tb = tmp_tb->next;
  }
  return NULL;
}
bool Statement::check_rs_contains_table(record_scan *rs) {
  table_link *tmp_tb = rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == rs) return true;
    tmp_tb = tmp_tb->next;
  }
  return false;
}
int Statement::get_child_rs_num(record_scan *rs) {
  int ret = 0;
  record_scan *tmp = rs->children_begin;
  for (; tmp; tmp = tmp->next) ret++;
  return ret;
}
void Statement::refresh_tables_for_merged_subquery(record_scan *old_rs,
                                                   record_scan *new_rs) {
  DataSpace *space = get_record_scan_table_subquery_space(old_rs);
  table_link *table = get_one_table_link(new_rs, space);
  if (!table) {
    LOG_ERROR(
        "Do not find table link for merge child with no table parent "
        "situation.\n");
    throw Error("Do not support sql, can not find table link for subquery.");
  }
  spaces.clear();
  spaces.push_back(space);
  if (space->get_data_source()) {
    par_table_num = 0;
    par_tables.clear();
  } else {
    par_table_num = 1;
    par_tables.clear();
    par_tables.push_back(table);
  }
  record_scan_one_space_tmp_map[st.scanner] = space;
  record_scan_all_table_spaces[table] = space;
  record_scan_par_table_map[table->join->cur_rec_scan] = table;
}

string Statement::remove_schema_from_full_columns() {
  string query_sql(sql);
  LOG_DEBUG("Query before remove schema from columns [%s].\n",
            query_sql.c_str());
  expr_list_item *str_expr_list_head =
      get_latest_stmt_node()->full_column_list_head;
  expr_list_item *str_expr_list_end =
      get_latest_stmt_node()->full_column_list_end;

  set<Expression *, exprposcmp> expression_set;
  for (expr_list_item *str_expr_item = str_expr_list_head;
       str_expr_item != str_expr_list_end; str_expr_item = str_expr_item->next)
    expression_set.insert(str_expr_item->expr);

  if (str_expr_list_end) expression_set.insert(str_expr_list_end->expr);

  set<Expression *>::iterator it = expression_set.begin();
  for (; it != expression_set.end(); ++it) {
    if ((*it)->type == EXPR_STR) {
      StrExpression *str_expr = (StrExpression *)(*it);
      string replace_sql = str_expr->get_no_schema_column();
      if (!replace_sql.empty() && str_expr->start_pos && str_expr->end_pos) {
        query_sql.replace(str_expr->start_pos - 1,
                          str_expr->end_pos - str_expr->start_pos + 1,
                          replace_sql);
      }
    }
  }
  LOG_DEBUG("Query after remove schema from columns [%s].\n",
            query_sql.c_str());
  return query_sql;
}

void Statement::assemble_dbscale_show_create_oracle_seq_plan(
    ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_show_create_oracle_seq_node(
      st.sql->seq->schema_name, st.sql->seq->sequence_name);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_seq_status_plan(ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_show_seq_status_node(
      st.sql->seq->schema_name, st.sql->seq->sequence_name);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_fetchnode_buffer_usage_plan(
    ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_show_fetchnode_buffer_usage_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_show_trx_block_info_plan(ExecutePlan *plan,
                                                          bool is_local) {
  ExecuteNode *node = plan->get_dbscale_show_trx_block_info_node(is_local);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_internal_set_plan(ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_internal_set_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_prepare_cache_status_plan(
    ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_show_prepare_cache_status_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_flush_prepare_cache_hit_plan(
    ExecutePlan *plan) {
  ExecuteNode *node = plan->get_dbscale_flush_prepare_cache_hit_node();
  plan->set_start_node(node);
}

ExecuteNode *Statement::try_assemble_bridge_mark_sql_plan(ExecutePlan *plan,
                                                          table_link *table) {
  if (table->join && table->join->schema_name &&
      strcasecmp("retl", table->join->schema_name) == 0 &&
      table->join->table_name &&
      strcasecmp("retl_mark", table->join->table_name) == 0) {
    if (st.type != STMT_UPDATE) {
      LOG_INFO(
          "bridge mark sql is not type of UPDATE, ignore it, the sql is [%s]\n",
          sql);
      return NULL;
    }
    plan->session->set_bridge_mark_sql(
        sql, st.sql->update_oper->update_table_end_pos);
    return plan->get_return_ok_node();
  }
  return NULL;
}

bool check_table_and_column_name_for_par_key(const char *table_name,
                                             const char *table_alias,
                                             const char *par_key,
                                             const char *str) {
  string field_item_str = str;
  vector<string> names;
  boost::split(names, field_item_str, boost::is_any_of("."));
  if (names.size() == 1) {
    // if no table name specified, we check par_key only,
    // if the par_key != names[0] then we shouldn't push down the modify_select
    return strcasecmp(par_key, names[0].c_str()) == 0;
  } else if (names.size() == 2) {
    return strcasecmp(table_alias ? table_alias : table_name,
                      names[0].c_str()) == 0 &&
           strcasecmp(par_key, names[1].c_str()) == 0;
  }
  return false;
}

/**
 * if this modify_select sql can pushdown to backend server(s), return true
 * if modify_table is a normal_table:
 *   then select_nodes must has the same DataSpace as the modify table
 * if modify_table is a par_table:
 *   then 3 things need to check:
 *   1, all select_nodes are fetch nodes(to exclude distinct, aggr funcs etc.)
 *   2, select table, modify table has the same part rules
 *   3, par_key_index in select_field_list == par_key_index in modify_field_list
 *   4, par_key of select table in select field list can't be complex expr
 *
 * TODO sql: insert into t1 select t2.pk, t2.c2 from t2,t3 where t2,pk = t3.pk;
 *    t1, t2 ,t3 have the same part_scheme.
 *    we only support that the select fields belong to table t2 which is the
 *    first table in the join tables, cause the select_par_table is t2 here
 *    should support when the sql is:
 *     insert into t1 select t3.pk, t3.c2 from t2,t3 where t2.pk = t3.pk;
 * */
bool Statement::check_can_pushdown_for_modify_select(
    PartitionedTable &select_par_table, record_scan *select_rs,
    vector<ExecuteNode *> &select_nodes, PartitionedTable *modify_par_table,
    DataSpace *modify_space) {
  if (hex_pos.size() > 0) return false;
  if (is_cross_node_join() || select_par_table.is_tmp_table_space() ||
      (modify_space && modify_space->is_tmp_table_space()) ||
      (modify_par_table && modify_par_table->is_tmp_table_space()))
    return false;
  vector<ExecuteNode *>::iterator n_it = select_nodes.begin();
  if (!modify_par_table && modify_space) {
    for (; n_it != select_nodes.end(); ++n_it) {
      ExecuteNode *node = *n_it;
      if (!node->is_fetch_node()) return false;
      DataSource *source1 = node->get_dataspace()->get_data_source();
      DataSource *source2 = modify_space->get_data_source();
      if (!is_share_same_server(source1->get_master_server(),
                                source2->get_master_server())) {
        return false;
      }
    }
    return true;
  }
  if (!modify_par_table || modify_space) return false;

  // 1, to check all select nodes are fetch nodes
  n_it = select_nodes.begin();
  for (; n_it != select_nodes.end(); ++n_it) {
    if (!(*n_it)->is_fetch_node()) return false;
  }
  // 2, to check part rules
  // TODO check partition_scheme contents instead of pointers
  if (select_par_table.get_partition_scheme() !=
      modify_par_table->get_partition_scheme())
    return false;
  // 3, to check par key position, and select_tb_pk_field
  const char *modify_tb_pk_name = modify_par_table->get_key_names()->at(0);
  const char *select_tb_pk_name = select_par_table.get_key_names()->at(0);
  int modify_tb_pk_pos = 0, select_tb_pk_pos = 0;
  name_item *modify_field_list = st.sql->insert_oper->insert_column_list;
  field_item *select_field_list = select_rs->field_list_head;
  if (!select_field_list || !select_field_list->field_expr) {
    return false;
  }
  bool is_star_expr = false;
  const char *tb_schema = NULL, *tb_name = NULL;
  if (select_field_list->field_expr->get_str_value()) {
    vector<string> names;
    tb_name = select_par_table.get_name();
    string full_field_name = select_field_list->field_expr->get_str_value();
    boost::split(names, full_field_name, boost::is_any_of("."));
    switch (names.size()) {
      case 1:
        break;
      case 2: {
        const char *table_alias = select_rs->first_table->join->alias;
        tb_name = table_alias ? table_alias : tb_name;
        if (strcasecmp(names[0].c_str(), tb_name) != 0) return false;
      } break;
      default:
        return false;
    }
    is_star_expr = strcmp(names.back().c_str(), "*") == 0;
  }
  string full_table_name;
  vector<unsigned int> key_pos;
  int tmp_pos = 1;
  if (modify_field_list) {
    do {
      vector<string> names;
      string full_field_name = modify_field_list->name;
      boost::split(names, full_field_name, boost::is_any_of("."));
      if (strcasecmp(names.back().c_str(), modify_tb_pk_name) == 0) {
        modify_tb_pk_pos = tmp_pos;
        break;
      }
      modify_field_list = modify_field_list->next;
      ++tmp_pos;
    } while (modify_field_list != st.sql->insert_oper->insert_column_list);
  } else {
    tb_schema = modify_par_table->get_schema()->get_name();
    tb_name = modify_par_table->get_name();
    splice_full_table_name(tb_schema, tb_name, full_table_name);
    modify_par_table->get_key_pos_vec(tb_schema, tb_name, key_pos,
                                      get_session());
    modify_tb_pk_pos = key_pos[0] + 1;
  }
  if (!is_star_expr) {
    select_tb_pk_pos = modify_tb_pk_pos;
    tmp_pos = 1;
    while (tmp_pos != modify_tb_pk_pos && select_field_list) {
      ++tmp_pos;
      select_field_list = select_field_list->next;
    }
    if (select_field_list && select_field_list->field_expr->get_str_value()) {
      tb_name = select_par_table.get_name();
      join_node *first_table = select_rs->first_table->join;
      if (!check_table_and_column_name_for_par_key(
              tb_name, first_table ? first_table->alias : NULL,
              select_tb_pk_name,
              select_field_list->field_expr->get_str_value())) {
        return false;
      }
    } else {
      return false;
    }
  } else {
    tb_schema = select_par_table.get_schema_name();
    tb_name = select_par_table.get_name();
    full_table_name.clear();
    splice_full_table_name(tb_schema, tb_name, full_table_name);
    key_pos.clear();
    select_par_table.get_key_pos_vec(tb_schema, tb_name, key_pos,
                                     get_session());
    select_tb_pk_pos = key_pos[0] + 1;
    if (select_tb_pk_pos != modify_tb_pk_pos) {
      // insert into t2 (c2,c3,c1) select *, c1 from t1
      if (!select_field_list->next) return false;
      TableInfoCollection *tic = TableInfoCollection::instance();
      TableInfo *ti = tic->get_table_info_for_read(full_table_name);
      vector<TableColumnInfo> *column_info_vec;
      try {
        column_info_vec =
            ti->element_table_column->get_element_vector_table_column_info(
                get_session());
      } catch (...) {
        LOG_ERROR(
            "Error occured when try to get table info "
            "column(vector_table_column_info) of table [%s]\n",
            full_table_name.c_str());
        ti->release_table_info_lock();
        throw;
      }
      ti->release_table_info_lock();
      tmp_pos = 1;
      modify_tb_pk_pos -= column_info_vec->size();
      select_field_list = select_field_list->next;
      while (select_field_list && tmp_pos != modify_tb_pk_pos) {
        select_field_list = select_field_list->next;
        ++tmp_pos;
      }
      if (select_field_list && select_field_list->field_expr->get_str_value()) {
        vector<string> names;
        string full_field_name = select_field_list->field_expr->get_str_value();
        join_node *first_table = select_rs->first_table->join;
        tb_name = select_par_table.get_name();
        if (!check_table_and_column_name_for_par_key(
                tb_name, first_table ? first_table->alias : NULL,
                select_tb_pk_name, full_field_name.c_str())) {
          return false;
        }
      } else {
        return false;
      }
    }
  }
  return true;
}

void Statement::assemble_modify_node_according_to_fetch_node(
    ExecutePlan &plan, const vector<ExecuteNode *> &fetch_nodes,
    PartitionedTable *modify_par_table) {
  vector<ExecuteNode *>::const_iterator node_it = fetch_nodes.begin();
  map<DataSpace *, const char *> spaces_map;
  map<DataSpace *, int> sql_count_map;
  for (; node_it != fetch_nodes.end(); ++node_it) {
    DataSpace *space = (*node_it)->get_dataspace();
    if (!space->get_virtual_machine_id()) {
      spaces_map[space] = sql;
      sql_count_map[space] = 1;
    } else {
      string new_sql_tmp;
      adjust_virtual_machine_schema(
          space->get_virtual_machine_id(), space->get_partition_id(), sql,
          modify_par_table->get_schema_name(), get_latest_stmt_node(),
          record_scan_all_table_spaces, new_sql_tmp);
      record_shard_sql_str(new_sql_tmp);
      spaces_map[space] = get_last_shard_sql();
      sql_count_map[space] = 1;
    }
  }
  ExecuteNode *node =
      plan.get_mul_modify_node(spaces_map, false, &sql_count_map);
  if (spaces_map.size() > 1)
    plan.session->set_affected_servers(AFFETCED_MUL_SERVER);
  plan.set_start_node(node);
}

bool Statement::is_partition_table_contain_partition_key(const char *par_key) {
  if (!par_key || st.type != STMT_CREATE_TB) return false;
  if (!st.sql || !st.sql->create_tb_oper) {
    string error_msg;
    error_msg.assign("sql statement node memory error.");
    throw Error(error_msg.c_str());
  }
  name_item *col_list = st.sql->create_tb_oper->column_name_list;
  while (col_list) {
    const char *col = col_list->name;
    if (strcasecmp(col, par_key) == 0) return true;
    col_list = col_list->next;
  }
  return false;
}

// replace ROWNUM compare INTNUM to "1"
void Statement::do_replace_rownum(record_scan *rs, string &old_sql) {
  where_rownum_expr *re = rs->where_rownum_expr;
  if (re) {
    if (rs->subquerytype == SUB_SELECT_ONE_COLUMN ||
        rs->subquerytype == SUB_SELECT_MUL_COLUMN ||
        rs->subquerytype == SUB_SELECT_ONE_COLUMN_MIN ||
        rs->subquerytype == SUB_SELECT_ONE_COLUMN_MAX ||
        rs->subquerytype == SUB_SELECT_UNSUPPORT) {
      LOG_ERROR("Not yet support 'ROWNUM & IN/ALL/ANY/SOME subquery'\n");
      throw Error("Not yet support 'ROWNUM & IN/ALL/ANY/SOME subquery'");
    }
    while (re) {
      string replace_str = "1";
      unsigned int start_pos = re->start_pos;
      unsigned int end_pos = re->end_pos;
      if (start_pos < rs->opt_where_start_pos ||
          end_pos > rs->opt_where_end_pos) {
        LOG_ERROR("Only support 'ROWNUM' in where condition.\n");
        throw Error("Only support 'ROWNUM' in where condition.");
      }
      ConditionAndOr and_or_type = is_and_or_condition(re->cond, rs);
      if (and_or_type == CONDITION_NO_COND || and_or_type == CONDITION_OR) {
        LOG_ERROR("Only support 'ROWNUM' in 'AND' Expression.\n");
        throw Error("Only support 'ROWNUM' in 'AND' Expression.");
      }
      for (unsigned int j = 0; j < end_pos - start_pos; ++j)
        replace_str = replace_str + " ";
      old_sql.replace(start_pos - 1, end_pos - start_pos + 1, replace_str);
      re = re->next;
    }
  }
  LOG_DEBUG("do_replace_rownum completed! sql is %s.\n", old_sql.c_str());
}

// add limit n after where
unsigned int Statement::do_add_limit_after_where(record_scan *rs,
                                                 string &old_sql,
                                                 unsigned int add_pos) {
  LOG_DEBUG("start to do_add_limit_after_where.\n");
  string add_limit_str = " limit ";
  uint64_t limit_n = (unsigned long)(~0);
  where_rownum_expr *re = rs->where_rownum_expr;

  if (!re) return 0;
  while (re) {
    uint64_t n = 0;
    switch (re->type) {
      case COMPARE_EQ:
        if (re->val == 1)
          n = 1;
        else
          n = 0;
        break;
      case COMPARE_NE:
        n = re->val - 1;
        break;
      case COMPARE_GE:
        if (re->val == 0 || re->val == 1)
          n = (unsigned long)(~0);
        else
          n = 0;
        break;
      case COMPARE_GT:
        if (re->val == 0)
          n = (unsigned long)(~0);
        else
          n = 0;
        break;
      case COMPARE_LE:
        n = re->val;
        break;
      case COMPARE_LT:
        if (re->val == 0)
          n = 0;
        else
          n = re->val - 1;
        break;
      default:
        LOG_ERROR("Find unsuport 'ROWNUM' compare type.\n");
        throw Error("Find unsuport 'ROWNUM' compare type.");
    }
    limit_n = limit_n < n ? limit_n : n;
    re = re->next;
  }

  char tmp_c[30];
  sprintf(tmp_c, "%lu", limit_n);
  add_limit_str = add_limit_str + string(tmp_c);
  old_sql.replace(rs->end_pos + add_pos, 0, add_limit_str);
  return add_limit_str.length();
}
void Statement::replace_rownum(record_scan *rs, string &old_sql) {
  do_replace_rownum(rs, old_sql);
  record_scan *rs_it = rs->children_begin;
  if (!rs_it) return;
  for (rs_it = rs->children_begin;; rs_it = rs_it->next) {
    replace_rownum(rs_it, old_sql);
    if (rs_it == rs->children_end) return;
  }
}
unsigned int Statement::add_limit_after_where(record_scan *rs, string &old_sql,
                                              unsigned int add_pos) {
  unsigned int tmp_add = do_add_limit_after_where(rs, old_sql, add_pos);
  record_scan *rs_it = rs->children_begin;
  if (!rs_it) return tmp_add;
  for (rs_it = rs->children_begin;; rs_it = rs_it->next) {
    add_pos = add_pos + add_limit_after_where(rs_it, old_sql, add_pos);
    if (rs_it == rs->children_end) return add_pos;
  }
}
void Statement::replace_rownum_add_limit() {
  rownum_tmp_sql = string(sql);
  string tmp_str = rownum_tmp_sql;
  boost::to_lower(tmp_str);
  if (tmp_str.find("limit") != string::npos) {
    LOG_ERROR("Not yet suport 'ROWNUM & LIMIT'\n");
    throw Error("Not yet suport 'ROWNUM & LIMIT'");
  }
  record_scan *rs = st.cur_rec_scan;
  replace_rownum(rs, rownum_tmp_sql);
  add_limit_after_where(rs, rownum_tmp_sql, 0);
  sql = rownum_tmp_sql.c_str();
  LOG_DEBUG("replace rownum add limit sql is: %s.\n", sql);
}

void Statement::assemble_move_table_to_recycle_bin_plan(ExecutePlan *plan) {
  string from_schema, from_table, to_table;
  table_link *table = st.table_list_head;
  from_schema = table->join->schema_name ? table->join->schema_name : schema;
  from_table = table->join->table_name;
  to_table = from_table + RECYCLE_TABLE_POSTFIX;
  LOG_INFO(
      "assemble move table to recycle bin plan from table: [%s.%s] to table: "
      "[%s.%s]\n",
      from_schema.c_str(), from_table.c_str(), from_schema.c_str(),
      to_table.c_str());
  // precheck
  if (get_table_from_recycle_bin(from_schema.c_str(), from_table.c_str()) !=
      NULL) {
    throw Error(
        "Can't drop/truncate this table, should clean it from recycle bin "
        "first");
  }
  // create tmp dataspace
  DataSpace *tmp_dspace = create_tmp_dataspace_from_table(
      from_schema.c_str(), from_table.c_str(), to_table.c_str());

  Schema *the_schema = Backend::instance()->find_schema(from_schema.c_str());
  // assemble move to recycle bin plan
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  ExecuteNode *ok_node = plan->get_ok_node();
  try {
    ExecuteNode *rename_ok_merge_node = NULL;
    const char *rename_sql_format = "RENAME TABLE `%s`.`%s` TO `%s`.`%s`";
    string rename_sql = (boost::format(rename_sql_format) % from_schema %
                         from_table % from_schema % to_table)
                            .str();
    DataSpace *dspace = Backend::instance()->get_data_space_for_table(
        from_schema.c_str(), from_table.c_str());
    if (dspace->is_partitioned()) {
      PartitionedTable *pt = (PartitionedTable *)dspace;
      rename_ok_merge_node = assemble_modify_node_for_partitioned_table(
          plan, rename_sql.c_str(), pt);
    } else {
      rename_ok_merge_node = plan->get_ok_merge_node();
      ExecuteNode *rename_node =
          plan->get_modify_node(dspace, rename_sql.c_str(), false);
      rename_ok_merge_node->add_child(rename_node);
    }
    // on auth server
    DataSpace *auth_space = Backend::instance()->get_auth_data_space();
    ExecuteNode *rename_on_auth_node =
        plan->get_modify_node(auth_space, rename_sql.c_str(), false);
    rename_ok_merge_node->add_child(rename_on_auth_node);

    ok_merge_node->add_child(rename_ok_merge_node);
    const char *recycle_type = NULL;
    switch (st.type) {
      case STMT_TRUNCATE: {
        ExecuteNode *create_tb_ok_merge_node = NULL;
        const char *create_tb_sql_format =
            "CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`";
        string create_tb_sql =
            (boost::format(create_tb_sql_format) % from_schema % from_table %
             from_schema % to_table)
                .str();
        if (dspace->is_partitioned()) {
          PartitionedTable *pt = (PartitionedTable *)dspace;
          create_tb_ok_merge_node = assemble_modify_node_for_partitioned_table(
              plan, create_tb_sql.c_str(), pt);
        } else {
          create_tb_ok_merge_node = plan->get_ok_merge_node();
          ExecuteNode *create_tb_node =
              plan->get_modify_node(dspace, create_tb_sql.c_str(), false);
          create_tb_ok_merge_node->add_child(create_tb_node);
        }
        // on auth server
        ExecuteNode *create_tb_on_auth_node =
            plan->get_modify_node(auth_space, create_tb_sql.c_str(), false);
        create_tb_ok_merge_node->add_child(create_tb_on_auth_node);
        ok_merge_node->add_child(create_tb_ok_merge_node);
      }
        recycle_type = RECYCLE_TYPE_TRUNCATE;
        break;
      case STMT_DROP_TB:
        recycle_type = RECYCLE_TYPE_DROP;
        break;
      default:
        throw Error(
            "move table to recycle bin should only used for truncate/drop "
            "table");
    }
    DataSpace *auth_dspace = Backend::instance()->get_auth_data_space();
    const char *recycle_bin_sql_format =
        "INSERT INTO dbscale.table_recycle_bin(original_table_name, "
        "new_table_name, recycle_type) VALUES('%s.%s', '%s.%s', '%s')";
    boost::format f = boost::format(recycle_bin_sql_format) % from_schema %
                      from_table % from_schema % to_table % recycle_type;
    ExecuteNode *insert_recycle_bin_node =
        plan->get_modify_node(auth_dspace, f.str().c_str(), false);
    ok_merge_node->add_child(insert_recycle_bin_node);
  } catch (...) {
    destroy_tmp_dataspace(tmp_dspace, the_schema, to_table.c_str());
    throw;
  }
  destroy_tmp_dataspace(tmp_dspace, the_schema, to_table.c_str());
  ok_node->add_child(ok_merge_node);
  plan->set_start_node(ok_node);
}

const char *Statement::get_table_from_recycle_bin(const char *from_schema,
                                                  const char *from_table) {
  DataSpace *auth_dspace = Backend::instance()->get_auth_data_space();
  Connection *conn = NULL;
  string recycle_type;
  try {
    conn = auth_dspace->get_connection(NULL, "dbscale", true);
    if (!conn) {
      throw Error(
          "can not get connection from auth to get_table_from_recycle_bin");
    }
    string full_tb_name = from_schema;
    full_tb_name.append(".").append(from_table);
    const char *check_sql_format =
        "SELECT recycle_type FROM dbscale.table_recycle_bin WHERE "
        "original_table_name ='%s.%s'";
    string check_sql =
        (boost::format(check_sql_format) % from_schema % from_table).str();
    conn->query_for_one_value(check_sql.c_str(), recycle_type, 0);
    conn->get_pool()->add_back_to_free(conn);
  } catch (...) {
    if (conn) conn->get_pool()->add_back_to_dead(conn);
    throw;
  }
  LOG_DEBUG("get table: [%s.%s] from table_recycle_bin, recycle_type: [%s]\n",
            from_schema, from_table, recycle_type.c_str());
  if (recycle_type.empty()) {
    return 0;
  } else if (recycle_type == RECYCLE_TYPE_TRUNCATE) {
    return RECYCLE_TYPE_TRUNCATE;
  } else if (recycle_type == RECYCLE_TYPE_DROP) {
    return RECYCLE_TYPE_DROP;
  } else {
    throw Error("dbscale.table_recycle_bin crupted");
  }
}

DataSpace *Statement::create_tmp_dataspace_from_table(const char *from_schema,
                                                      const char *from_table,
                                                      const char *to_table) {
  LOG_DEBUG("create tmp dataspace from table: [%s.%s] to table: [%s.%s]\n",
            from_schema, from_table, from_schema, to_table);
  Backend *backend = Backend::instance();
  Schema *schema = backend->find_schema(from_schema);
  if (!schema) {
    LOG_DEBUG("Can't find schema: [%s] using catalog\n", from_schema);
    return backend->get_catalog();
  }
  Table *t = backend->get_table_by_name(from_schema, from_table);
  if (!t) {
    LOG_DEBUG("Can't find table: [%s.%s] using catalog\n", from_schema,
              from_table);
    return backend->get_catalog();
  }
  Table *ret = NULL;
  DataSource *ds = t->get_data_source();
  if (!t->is_partitioned()) {
    ret = new Table(to_table, ds, schema, t->is_independent(), NULL);
  } else {
    PartitionedTable *pt = (PartitionedTable *)t;
    const char *p_key = pt->get_key_names()->operator[](0);
    size_t len_key1 = strlen(p_key);
    char *tmp = new char[len_key1 + 1];
    strncpy(tmp, p_key, len_key1 + 1);
    add_dynamic_str(tmp);
    PartitionScheme *p_scheme = pt->get_partition_scheme();
    PartitionType p_type = p_scheme->get_type();
    PartitionedTable *new_pt = new PartitionedTable(
        to_table, tmp, p_scheme, schema, p_type, pt->is_independent(), NULL,
        pt->get_virtual_times());
    new_pt->set_virtual_map(pt->get_virtual_map());
    ret = new_pt;
  }
  schema->add_table(ret);
  backend->add_data_space(ret);
  return ret;
}

void Statement::destroy_tmp_dataspace(DataSpace *dspace, Schema *the_schema,
                                      const char *table_name) {
  if (dspace != Backend::instance()->get_catalog()) {
    Backend::instance()->remove_data_space(dspace);
    the_schema->remove_table(table_name);
    delete dspace;
  }
}

void Statement::assemble_dbscale_restore_table_plan(ExecutePlan *plan,
                                                    const char *schema,
                                                    const char *table) {
  LOG_DEBUG("assemble_dbscale_restore_table_plan for table [%s.%s]\n", schema,
            table);
  string from_schema, from_table, to_table;
  from_schema = schema;
  to_table = table;
  from_table = to_table + RECYCLE_TABLE_POSTFIX;
  // precheck
  const char *recycle_type = get_table_from_recycle_bin(schema, table);
  if (recycle_type == NULL) {
    throw Error("table not found to restore");
  }
  string recycle_type_str = recycle_type;
  DataSpace *dspace =
      Backend::instance()->get_data_space_for_table(schema, table);
  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();

  ExecuteNode *precheck_ok_merge_node = plan->get_ok_merge_node();
  ExecuteNode *restore_precheck_node =
      plan->get_restore_recycle_table_precheck_node(
          schema, from_table.c_str(), table, recycle_type, dspace);
  precheck_ok_merge_node->add_child(restore_precheck_node);
  ok_merge_node->add_child(precheck_ok_merge_node);

  // create tmp dataspace
  DataSpace *tmp_dspace = create_tmp_dataspace_from_table(
      from_schema.c_str(), to_table.c_str(), from_table.c_str());

  Schema *the_schema = Backend::instance()->find_schema(from_schema.c_str());
  DataSpace *auth_space = Backend::instance()->get_auth_data_space();
  try {
    if (recycle_type_str == RECYCLE_TYPE_TRUNCATE) {
      ExecuteNode *drop_table_ok_merge_node = NULL;
      const char *drop_tb_sql_format = "DROP TABLE `%s`.`%s`";
      string drop_tb_sql =
          (boost::format(drop_tb_sql_format) % from_schema % to_table).str();
      if (dspace->is_partitioned()) {
        PartitionedTable *pt = (PartitionedTable *)dspace;
        drop_table_ok_merge_node = assemble_modify_node_for_partitioned_table(
            plan, drop_tb_sql.c_str(), pt);
      } else {
        drop_table_ok_merge_node = plan->get_ok_merge_node();
        ExecuteNode *drop_tb_node =
            plan->get_modify_node(dspace, drop_tb_sql.c_str(), false);
        drop_table_ok_merge_node->add_child(drop_tb_node);
      }
      ExecuteNode *drop_tb_on_auth_node =
          plan->get_modify_node(auth_space, drop_tb_sql.c_str(), false);
      drop_table_ok_merge_node->add_child(drop_tb_on_auth_node);
      ok_merge_node->add_child(drop_table_ok_merge_node);
    }

    ExecuteNode *rename_ok_merge_node = NULL;
    const char *rename_sql_format = "RENAME TABLE `%s`.`%s` TO `%s`.`%s`";
    string rename_sql = (boost::format(rename_sql_format) % from_schema %
                         from_table % from_schema % to_table)
                            .str();
    if (dspace->is_partitioned()) {
      PartitionedTable *pt = (PartitionedTable *)dspace;
      rename_ok_merge_node = assemble_modify_node_for_partitioned_table(
          plan, rename_sql.c_str(), pt);
    } else {
      rename_ok_merge_node = plan->get_ok_merge_node();
      ExecuteNode *rename_node =
          plan->get_modify_node(dspace, rename_sql.c_str(), false);
      rename_ok_merge_node->add_child(rename_node);
    }
    ExecuteNode *rename_tb_on_auth =
        plan->get_modify_node(auth_space, rename_sql.c_str(), false);
    rename_ok_merge_node->add_child(rename_tb_on_auth);
    ok_merge_node->add_child(rename_ok_merge_node);

    DataSpace *auth_dspace = Backend::instance()->get_auth_data_space();
    const char *recycle_bin_sql_format =
        "DELETE FROM dbscale.table_recycle_bin WHERE original_table_name = "
        "'%s.%s'";
    boost::format f =
        boost::format(recycle_bin_sql_format) % from_schema % to_table;
    ExecuteNode *delete_recycle_bin_node =
        plan->get_modify_node(auth_dspace, f.str().c_str(), false);
    ok_merge_node->add_child(delete_recycle_bin_node);
  } catch (...) {
    destroy_tmp_dataspace(tmp_dspace, the_schema, from_table.c_str());
    throw;
  }
  destroy_tmp_dataspace(tmp_dspace, the_schema, from_table.c_str());
  ok_node->add_child(ok_merge_node);
  plan->set_start_node(ok_node);
}

void Statement::assemble_dbscale_clean_recycle_table_plan(ExecutePlan *plan,
                                                          const char *schema,
                                                          const char *table) {
  DataSpace *dspace =
      Backend::instance()->get_data_space_for_table(schema, table);
  const char *recycle_type = get_table_from_recycle_bin(schema, table);
  if (recycle_type == NULL) {
    throw Error("Can't find this table to clean");
  }
  string recycle_type_str = recycle_type;
  string from_schema, from_table, to_table;
  from_schema = schema;
  from_table = table;
  to_table = from_table + RECYCLE_TABLE_POSTFIX;

  // create tmp dataspace
  DataSpace *tmp_dspace = create_tmp_dataspace_from_table(
      from_schema.c_str(), from_table.c_str(), to_table.c_str());
  Schema *the_schema = Backend::instance()->find_schema(schema);

  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = NULL;
  const char *drop_recycle_tb_format = "DROP TABLE `%s`.`%s`";
  string drop_sql =
      (boost::format(drop_recycle_tb_format) % schema % to_table).str();
  try {
    if (dspace->is_partitioned()) {
      PartitionedTable *pt = (PartitionedTable *)dspace;
      ok_merge_node = assemble_modify_node_for_partitioned_table(
          plan, drop_sql.c_str(), pt);
    } else {
      ok_merge_node = plan->get_ok_merge_node();
      ExecuteNode *drop_tb_node =
          plan->get_modify_node(dspace, drop_sql.c_str(), false);
      ok_merge_node->add_child(drop_tb_node);
    }

    DataSpace *auth_dspace = Backend::instance()->get_auth_data_space();
    ExecuteNode *drop_tb_node_on_auth =
        plan->get_modify_node(auth_dspace, drop_sql.c_str(), false);
    ok_merge_node->add_child(drop_tb_node_on_auth);

    const char *clean_recycle_bin_table_format =
        "DELETE FROM dbscale.table_recycle_bin where original_table_name = "
        "'%s.%s'";
    string clean_recycle_bin_sql =
        (boost::format(clean_recycle_bin_table_format) % schema % table).str();
    ExecuteNode *clean_recycle_table_ok_merge_node = plan->get_ok_merge_node();
    ExecuteNode *clean_recycle_node = plan->get_modify_node(
        auth_dspace, clean_recycle_bin_sql.c_str(), false);
    clean_recycle_table_ok_merge_node->add_child(clean_recycle_node);
    ok_merge_node->add_child(clean_recycle_table_ok_merge_node);

    ok_node->add_child(ok_merge_node);
  } catch (...) {
    destroy_tmp_dataspace(tmp_dspace, the_schema, to_table.c_str());
    throw;
  }
  destroy_tmp_dataspace(tmp_dspace, the_schema, to_table.c_str());
  plan->set_start_node(ok_node);
}

ExecuteNode *Statement::assemble_modify_node_for_partitioned_table(
    ExecutePlan *plan, const char *sql, PartitionedTable *pt) {
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  for (unsigned int i = 0; i < pt->get_real_partition_num(); ++i) {
    ExecuteNode *modify_node =
        plan->get_modify_node(pt->get_partition(i), sql, true);
    ok_merge_node->add_child(modify_node);
  }
  return ok_merge_node;
}

}  // namespace sql
}  // namespace dbscale
