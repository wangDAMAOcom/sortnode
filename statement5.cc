ble_vector(record_scan *rs) {
  vector<TableStruct *> *vec = new vector<TableStruct *>();
  const char *table_name, *schema_name, *alias;
  table_link *tmp_tb = rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == rs) {
      TableStruct *table_struct = new TableStruct();
      table_name = tmp_tb->join->table_name;
      schema_name =
          tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
      alias = tmp_tb->join->alias ? tmp_tb->join->alias : table_name;
      table_struct->table_name = table_name;
      table_struct->schema_name = schema_name;
      table_struct->alias = alias;
      vec->push_back(table_struct);
      LOG_DEBUG(
          "Get table vector table_name = [%s], schema_name = [%s], alias = "
          "[%s]\n",
          table_struct->table_name.c_str(), table_struct->schema_name.c_str(),
          table_struct->alias.c_str());
    }
    tmp_tb = tmp_tb->next;
  }
  return vec;
}

void Statement::prepare_dataspace_for_leftjoin_subquery(
    join_node *join_tables) {
  /*目前只有rs有left join，那么这个rs的所有子查询都当作left
   * join的子查询来处理，这个是目前代码的实现限制。*/

  // set the subquery dataspace and key
  DataSpace *dataspace = record_scan_one_space_tmp_map[join_tables->sub_select];
  if (!dataspace) {
    LOG_ERROR(
        "Find unexpected not support left join subquery with left join sub "
        "select space empty. the sql is [%s]",
        sql);
    throw NotSupportedError("Find unexpected unsupport left join subquery.");
  }
  if (dataspace->is_partitioned()) {
    join_condition *condition = join_tables->upper->condition;
    if (!condition || condition->type != JOIN_COND_ON) {
      LOG_ERROR("DBScale only support cross left join with ON condition.\n");
      throw NotSupportedError(
          "DBScale only support cross left join with on condition.");
    }
    const char *schema_name =
        join_tables->schema_name ? join_tables->schema_name : schema;
    const char *table_alias = join_tables->alias;
    string key = get_key_for_left_join_subquery_tmp_table(
        condition->expr_condition, schema_name, table_alias);
    /*需要找到 key 以便 left join 的迁移数据，如果找不到，用 normal 表代替*/
    if (key.empty()) {
      PartitionedTable *par_ds = (PartitionedTable *)dataspace;
      Partition *par = par_ds->get_partition(0);
      table_subquery_final_dataspace[join_tables->sub_select] = par;
    } else {
      dataspace_partition_key[join_tables->sub_select] = key;
      table_subquery_final_dataspace[join_tables->sub_select] = dataspace;
    }
  } else {
    table_subquery_final_dataspace[join_tables->sub_select] = dataspace;
  }
}

void Statement::prepare_table_vector_and_leftjoin_subquery_from_rs(
    record_scan *rs, list<TableStruct *> *table_struct_list,
    map<TableStruct *, table_link *> *table_struct_map) {
  LOG_DEBUG(
      "Statement::prepare_table_vector_and_leftjoin_subquery_from_rs for rs "
      "%@.\n",
      rs);
  /*本函数遍历rs的join关系，将所有的非子查询表按从左到右的顺序填充到table_struct_list中，并通过table_struct_map为每个table_struct记录table_link信息。
    对于join中的表子查询，将
    1. 位置信息记录到 left_join_record_scan_position_subquery 中
    2. 对应的record_scan记录到 left_join_subquery_rs中
    3. 准备的临时表dataspace信息存储在dataspace_partition_key 和
    table_subquery_final_dataspace，存储的信息在Statement::init_table_subquery_dataspace中用于生产table
    子查询所需要的真实dataspace。并最终设置到record_scan_one_space_tmp_map
    中。*/
  join_node *join_tables = rs->join_tables;  // tables in the from
  vector<join_node *> join_node_vector;
  TableStruct *table_struct;

  bool record_scan_contains_left_join = rs->contains_left_join;
  int left_join_subquery_num = 0;
  join_node_vector.push_back(join_tables);
  string schema_name;
  string table_name;
  string alias;
  while (!join_node_vector.empty()) {
    join_tables = join_node_vector.back();
    while (join_tables) {
      join_tables = join_tables->left;
      join_node_vector.push_back(join_tables);
    }
    join_node_vector.pop_back();
    if (!join_node_vector.empty()) {
      join_tables = join_node_vector.back();
      join_node_vector.pop_back();
      if (join_tables->type == JOIN_NODE_SINGLE) {
        table_name = join_tables->table_name;
        schema_name =
            join_tables->schema_name ? join_tables->schema_name : schema;
        alias = join_tables->alias ? join_tables->alias : table_name;

        table_struct = new TableStruct();
        table_struct->table_name = table_name;
        table_struct->schema_name = schema_name;
        table_struct->alias = alias;
        table_struct_list->push_back(table_struct);
        (*table_struct_map)[table_struct] = join_tables->delink;
      } else if (join_tables->type == JOIN_NODE_SUBSELECT) {
        if (join_tables->upper && record_scan_contains_left_join) {
          left_join_record_scan_position_subquery[rs]
                                                 [table_struct_list->size() +
                                                  left_join_subquery_num] =
                                                     join_tables->sub_select;
          left_join_subquery_rs.insert(join_tables->sub_select);
          ++left_join_subquery_num;

          /*该函数通过填充 dataspace_partition_key 和
           * table_subquery_final_dataspace 来为left
           * join相关的子查询准备dataspace*/
          prepare_dataspace_for_leftjoin_subquery(join_tables);
        }
      }
      join_node_vector.push_back(join_tables->right);
    }
  }
}

bool Statement::only_has_one_non_global_tb_can_merge_global(record_scan *root,
                                                            DataSpace *ds) {
  ACE_ASSERT(ds);
  DataSpace *part_ds = NULL;
  vector<DataSpace *> norm_spaces;
  int part_ds_num = 0;

  if (record_scan_dependent_map.count(root) &&
      !record_scan_dependent_map[root].empty()) {
    return false;
  }

  if (ds->is_partitioned()) {
    part_ds = ds;
    ++part_ds_num;
  } else
    norm_spaces.push_back(ds);

  record_scan *tmp = root->children_begin;

  for (; tmp; tmp = tmp->next) {
    if (record_scan_dependent_map.count(tmp) &&
        !record_scan_dependent_map[tmp].empty()) {
      return false;
    }

    if (record_scan_par_table_map.count(tmp)) {
      if (++part_ds_num > 1) return false;
      table_link *par_table1 = record_scan_par_table_map[tmp];
      part_ds = (PartitionedTable *)record_scan_all_table_spaces[par_table1];
    } else {
      if (record_scan_one_space_tmp_map.count(tmp)) {
        DataSpace *global_space = record_scan_one_space_tmp_map[tmp];
        norm_spaces.push_back(global_space);
      }
    }
  }

  DataSpace *fin_space = part_ds;

  if (!norm_spaces.empty()) {
    vector<DataSpace *>::iterator it = norm_spaces.begin();
    for (; it != norm_spaces.end(); ++it) {
      if (!fin_space) {
        fin_space = *it;
        continue;
      } else if (stmt_session->is_dataspace_cover_session_level(fin_space,
                                                                *it)) {
        continue;
      } else if (stmt_session->is_dataspace_cover_session_level(*it,
                                                                fin_space)) {
        fin_space = *it;
        continue;
      } else {
        return false;
      }
    }
  }

  return true;
}

bool Statement::contains_left_join_subquery(record_scan *rs, DataSpace *ds) {
  if (rs->contains_left_join && rs->contains_subquery &&
      !only_has_one_non_global_tb_can_merge_global(
          rs,
          ds))  // If there is only one non_global_tb and can merge with other
                // global tb, not treat this rs contians left_join_subquery
    return true;

  return false;
}

void Statement::init_column_table_subquery_tmp_table_name(
    record_scan *rs, DataSpace *column_space, ExecutePlan *plan) {
  if (plan->session->is_in_lock()) {
    LOG_ERROR("Not support cross node join in lock table mode.\n");
    throw Error("Not support cross node join in lock table mode.");
  }
  string table_name(SUB_COLUMN_TABLE_NAME);
  table_name.append("_");
  table_name.append(column_space->get_name());
  table_name.append("_");
  table_name.append(SUB_COLUMN_TABLE_NAME);
  record_scan_subquery_names[rs] = table_name;
}

string Statement::get_key_for_table_subquery(record_scan *rs) {
  Backend *backend = Backend::instance();
  /* For union table subquery, the join_belong should be NULL.
   * The partition key will be handled by final dataspace.
   *
   * For the union stmt situation, dbscale may need to use table subquery cross
   * node join to move the data of one child-record_scan (such as left child) to
   * the other part (such as right child), in current implementation, the child
   * record scan of union does not set the "rs->join_belong ", so we can use it
   * to distinguish the child record scan of union which need to do table
   * subquery. For table subquery cross node join of union, the partition key is
   * specify by other logic, normally is the first filed of select list, so here
   * just return an empty key.
   */
  string key;
  if (!rs->join_belong) return key;
  const char *table_alias = rs->join_belong->alias;

  handle_par_key_equality_full_table(rs->upper);

  vector<TableStruct *> *table_vector;
  if (record_scan_need_cross_node_join.count(rs->upper)) {
    vector<vector<TableStruct *> *> *table_sequence =
        record_scan_join_tables[rs->upper];
    table_vector = (*table_sequence)[table_sequence->size() - 1];
  } else {
    table_vector = get_table_vector(rs->upper);
  }

  vector<TableStruct *>::iterator it = table_vector->begin();
  for (; it != table_vector->end(); it++) {
    string full_column((*it)->schema_name);
    full_column.append(".");
    full_column.append((*it)->alias);
    full_column.append(".");
    DataSpace *space = backend->get_data_space_for_table(
        (*it)->schema_name.c_str(), (*it)->table_name.c_str());
    if (space->get_data_source()) continue;
    const char *key_name = ((PartitionedTable *)space)->get_key_names()->at(0);
    full_column.append(key_name);
    vector<string> *vec =
        &record_scan_par_key_equality_full_table[rs->upper][full_column];
    vector<string>::iterator it_vec = vec->begin();
    if (it_vec != vec->end()) {
      it_vec++;
    }
    for (; it_vec != vec->end(); it_vec++) {
      size_t first_dot_position = it_vec->find(".");
      size_t second_dot_position = it_vec->find(".", first_dot_position + 1);
      if (first_dot_position != string::npos &&
          second_dot_position == string::npos) {
        string table_name = it_vec->substr(0, first_dot_position);
        if (lower_case_table_names) {
          if (!strcasecmp(table_name.c_str(), table_alias)) {
            key = it_vec->substr(first_dot_position + 1);
            break;
          }
        } else {
          if (!strcmp(table_name.c_str(), table_alias)) {
            key = it_vec->substr(first_dot_position + 1);
            break;
          }
        }
      }
    }
  }
  if (!record_scan_need_cross_node_join.count(rs->upper)) {
    for (it = table_vector->begin(); it != table_vector->end(); it++) {
      delete *it;
    }
    delete table_vector;
  }
  return key;
}

string Statement::get_column_in_table_without_throw(string peer_string,
                                                    string schema_name,
                                                    string table_alias) {
  string key;
  string column;
  string peer_schema;
  string peer_table;
  split_value_into_schema_table_column(peer_string, peer_schema, peer_table,
                                       column);

  if (!peer_schema.empty()) {
    if (!lower_case_compare(peer_schema.c_str(), schema_name.c_str()) &&
        !lower_case_compare(peer_table.c_str(), table_alias.c_str()))
      key = column;
  } else if (!peer_table.empty()) {
    if (!lower_case_compare(peer_table.c_str(), table_alias.c_str()))
      key = column;
  }
  return key;
}

/* Get column from one condition peer belongs to the table.
 */
string Statement::get_column_in_table(string peer_string, table_link *table) {
  string key;
  string column;
  string peer_schema;
  string peer_table;
  string schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  string table_name = table->join->table_name;
  string table_alias =
      table->join->alias ? table->join->alias : table->join->table_name;
  split_value_into_schema_table_column(peer_string, peer_schema, peer_table,
                                       column);
  if (!peer_schema.empty()) {
    if (!lower_case_compare(peer_schema.c_str(), schema_name.c_str()) &&
        !lower_case_compare(peer_table.c_str(), table_alias.c_str()))
      key = column;
  } else if (!peer_table.empty()) {
    if (!lower_case_compare(peer_table.c_str(), table_alias.c_str()))
      key = column;
  } else {
    TableInfoCollection *tic = TableInfoCollection::instance();
    TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);
    map<string, TableColumnInfo *, strcasecomp> *column_info_map;
    try {
      column_info_map =
          ti->element_table_column
              ->get_element_map_columnname_table_column_info(stmt_session);
    } catch (...) {
      LOG_ERROR("Error eccured when get table info for table [%s.%s]\n",
                schema_name.c_str(), table_name.c_str());
      ti->release_table_info_lock();
      throw;
    }
    string tmp_colname(column);
    boost::to_lower(tmp_colname);
    if (column_info_map != NULL) {
      if (column_info_map->count(tmp_colname)) key = column;
    }
    ti->release_table_info_lock();
  }
  return key;
}

string Statement::get_key_for_left_join_subquery_tmp_table(
    Expression *condition, const char *schema_name, const char *table_alias) {
  string ret;
  if (condition->type == EXPR_EQ) {
    CompareExpression *expr = (CompareExpression *)condition;
    Expression *left = expr->left;
    Expression *right = expr->right;
    if (left->type == EXPR_STR && right->type == EXPR_STR) {
      string left_peer, right_peer;
      left->to_string(left_peer);
      right->to_string(right_peer);
      string key1 = get_column_in_table_without_throw(left_peer, schema_name,
                                                      table_alias);
      string key2 = get_column_in_table_without_throw(right_peer, schema_name,
                                                      table_alias);
      if (key1.empty() && !key2.empty()) {
        ret = key2;
      } else if (!key1.empty() && key2.empty()) {
        ret = key1;
      }
    }
    return ret;
  }
  if (condition->type == EXPR_AND) {
    BoolBinaryCaculateExpression *tmp_expr =
        (BoolBinaryCaculateExpression *)condition;
    ret = get_key_for_left_join_subquery_tmp_table(tmp_expr->left, schema_name,
                                                   table_alias);
    if (ret.empty())
      ret = get_key_for_left_join_subquery_tmp_table(tmp_expr->right,
                                                     schema_name, table_alias);
  }
  return ret;
}

/* Find a equal column of the table from on condition.
 * only when the two sites of the equal condition belong to diffrent table
 * and one belongs to current table, return the column.
 */
string Statement::get_key_for_left_join_tmp_table(Expression *condition,
                                                  table_link *table) {
  string ret;
  if (condition->type == EXPR_EQ) {
    CompareExpression *expr = (CompareExpression *)condition;
    Expression *left = expr->left;
    Expression *right = expr->right;
    if (left->type == EXPR_STR && right->type == EXPR_STR) {
      string left_peer, right_peer;
      left->to_string(left_peer);
      right->to_string(right_peer);
      string key1 = get_column_in_table(left_peer, table);
      string key2 = get_column_in_table(right_peer, table);
      if (key1.empty() && !key2.empty()) {
        ret = key2;
      } else if (!key1.empty() && key2.empty()) {
        ret = key1;
      }
    }
    return ret;
  }
  if (condition->type == EXPR_AND) {
    BoolBinaryCaculateExpression *tmp_expr =
        (BoolBinaryCaculateExpression *)condition;
    ret = get_key_for_left_join_tmp_table(tmp_expr->left, table);
    if (ret.empty())
      ret = get_key_for_left_join_tmp_table(tmp_expr->right, table);
  }
  return ret;
}

bool Statement::check_column_in_peer(string peer, const char *column,
                                     table_link *table) {
  string peer_column;
  string peer_schema;
  string peer_table;
  string schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  string table_alias =
      table->join->alias ? table->join->alias : table->join->table_name;
  split_value_into_schema_table_column(peer, peer_schema, peer_table,
                                       peer_column);
  if (!peer_schema.empty()) {
    if (!lower_case_compare(peer_schema.c_str(), schema_name.c_str()) &&
        !lower_case_compare(peer_table.c_str(), table_alias.c_str()) &&
        !strcasecmp(column, peer_column.c_str()))
      return true;
  } else if (!peer_table.empty()) {
    if (!lower_case_compare(peer_table.c_str(), table_alias.c_str()) &&
        !strcasecmp(column, peer_column.c_str()))
      return true;
  } else {
    if (!strcasecmp(column, peer_column.c_str())) return true;
  }
  return false;
}

bool Statement::check_column_in_condition(Expression *condition,
                                          const char *column,
                                          table_link *table) {
  if (condition->type == EXPR_EQ) {
    CompareExpression *expr = (CompareExpression *)condition;
    Expression *left = expr->left;
    Expression *right = expr->right;
    if (left->type == EXPR_STR && right->type == EXPR_STR) {
      string left_peer, right_peer;
      left->to_string(left_peer);
      right->to_string(right_peer);
      bool lb = check_column_in_peer(left_peer, column, table);
      bool rb = check_column_in_peer(right_peer, column, table);
      if (lb && !rb) return true;
      if (!lb && rb) return true;
    }
  }
  if (condition->type == EXPR_AND) {
    BoolBinaryCaculateExpression *tmp_expr =
        (BoolBinaryCaculateExpression *)condition;
    if (check_column_in_condition(tmp_expr->left, column, table))
      return true;
    else if (check_column_in_condition(tmp_expr->right, column, table))
      return true;
  }
  return false;
}

bool Statement::check_left_join_on_par_key(table_link *table) {
  Backend *backend = Backend::instance();
  join_node *join = table->join;
  join_condition *condition = join->upper->condition;
  if (!condition || condition->type != JOIN_COND_ON) {
    LOG_ERROR("DBScale only support cross left join with ON condition.\n");
    throw NotSupportedError(
        "DBScale only support cross left join with on condition.");
  }
  const char *schema_name = join->schema_name ? join->schema_name : schema;
  const char *table_name = join->table_name;
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->get_data_source()) return true;
  const char *key_name = ((PartitionedTable *)space)->get_key_names()->at(0);
  Expression *expr = condition->expr_condition;
  if (check_column_in_condition(expr, key_name, table)) return true;
  return false;
}

/* If table is right part of a left join with out equal on partition key,
 * move the data of the table to a tmp table then do cross node join with left
 * part. If find a equal column of the table from on condition, place the tmp
 * table on table scheme with equal column as partition key, if not, place the
 * tmp table on first partition of the table.
 */
string Statement::init_left_join_tmp_table_dataspace(table_link *table) {
  string table_name;
  Backend *backend = Backend::instance();
  join_node *join = table->join;
  join_condition *condition = join->upper->condition;
  if (!condition || condition->type != JOIN_COND_ON) {
    LOG_ERROR("DBScale only support cross left join with ON condition.\n");
    throw NotSupportedError(
        "DBScale only support cross left join with on condition.");
  }
  Expression *expr = condition->expr_condition;
  string key = get_key_for_left_join_tmp_table(expr, table);
  const char *local_table = join->table_name;
  const char *local_schema = join->schema_name ? join->schema_name : schema;
  LOG_DEBUG("Statement::init_left_join_tmp_table_dataspace for table %s.%s.\n",
            local_schema, local_table);
  DataSpace *local_space =
      backend->get_data_space_for_table(local_schema, local_table);
  if (key.empty()) {
    PartitionedTable *par_ds = (PartitionedTable *)local_space;
    Partition *par = par_ds->get_partition(0);
    local_space = par;
    key = "dbscale_key";
  }
  DataSpace *tab = NULL;
  local_space->acquire_tmp_table_read_mutex();
  table_name = local_space->get_tmp_table_name(key.c_str());
  local_space->release_tmp_table_mutex();
  if (table_name.empty()) {
    local_space->acquire_tmp_table_write_mutex();
    table_name = local_space->get_tmp_table_name(key.c_str());
    if (table_name.empty()) {
      JoinTableInfo table_info = {key, "", "", "", ""};
      try {
        tab = local_space->init_join_table_space(table_name, &table_info, false,
                                                 true);
      } catch (...) {
        LOG_ERROR("Got error then init left join tmp table dataspace.\n");
        local_space->release_tmp_table_mutex();
        throw;
      }
      backend->add_join_table_spaces(local_space, tab);
      local_space->set_tmp_table_name(key.c_str(), table_name);
    }
    local_space->release_tmp_table_mutex();
  }
  return table_name;
}

/*This function init the dataspace of table_subquery tables,
 *there are four situations:
 *1.parent is a normal table, locate the tmp table with parent.
 *2.parent is a partition table, if we can find a partition key, init the tmp
 *table as a partitioned table with the partition key, or init the tmp table as
 *a duplicated table. 3.parent has no table, locate the tmp table on current
 *local normal table. 4.parent has no table, locate the tmp table on current
 *local partition table's first partition. 5.otherwise, use catalog.
 *
 *We use the locate dataspace and key_name as key map to tmp table dataspace
 *name already initialized. If we can get the tmp table dataspace name with
 *key_name from local_space, use the dataspace already exists. If not, create a
 *new tmp table dataspace and store it to local_space with key_name as map key.
 *There are three kind of key_name, one is partition key for partitioned tmp
 *tables, one is "dbscale_dup" for duplicated tmp tables, one is "dbscale_key"
 *for normal tmp tables.
 *
 *Set record_scan_one_space_tmp_map of current rs as new tmp table dataspace.
 */
void Statement::init_table_subquery_dataspace(record_scan *rs,
                                              ExecutePlan *plan) {
  if (plan->session->is_in_lock()) {
    LOG_ERROR("Not support cross node join in lock table mode.\n");
    throw Error("Not support cross node join in lock table mode.");
  }

  Backend *backend = Backend::instance();
  /*In this function, current parameter rs is a child table subquery,
   *cur_dataspace is the record_scan_one_space of itself, dataspace is
   *the record_scan_one_space of it's parent record scan.
   */
  bool is_dup = false;
  string tmp_key_name;
  const char *key_name = NULL;
  string key;
  DataSpace *dataspace = record_scan_one_space_tmp_map[rs->upper];
  DataSpace *cur_dataspace = record_scan_one_space_tmp_map[rs];
  string schema_name(get_schema());
  DataSource *cur_ds = NULL;
  DataSpace *local_space = NULL;  // The final dataspace of tmp table.
  if (cur_dataspace) cur_ds = cur_dataspace->get_data_source();
  DataSource *ds = NULL;
  if (dataspace) ds = dataspace->get_data_source();
  if (ds) {
    local_space = dataspace;
    key_name = "dbscale_key";
  } else if (dataspace) {
    key = get_key_for_table_subquery(rs);
    if (key.empty()) {
      local_space = dataspace;
      key_name = "dbscale_dup";
      is_dup = true;
    } else {
      local_space = dataspace;
      key_name = key.c_str();
    }
  } else if (cur_ds) {
    local_space = cur_dataspace;
    key_name = "dbscale_key";
  } else if (cur_dataspace) {
    PartitionedTable *par_ds = (PartitionedTable *)cur_dataspace;
    Partition *par = par_ds->get_partition(0);
    local_space = par;
    key_name = "dbscale_key";
  } else {
    /*For the situation that current rs is a union all query record scan
     *and there is no table in it's parent.
     */
    local_space = backend->get_data_space_for_table(NULL, NULL);
    key_name = "dbscale_key";
  }
  if (table_subquery_final_dataspace.count(rs)) {
    local_space = table_subquery_final_dataspace[rs];
    is_dup = false;
    tmp_key_name = dataspace_partition_key[rs];
    key_name = tmp_key_name.c_str();
  }

  DataSpace *tab = get_subquery_table_dataspace(local_space, key_name, is_dup);

  local_space->acquire_tmp_table_read_mutex();
  string table_name = local_space->get_tmp_table_name(key_name);
  local_space->release_tmp_table_mutex();

  record_scan_table_subquery_space_map[rs] = record_scan_one_space_tmp_map[rs];
  record_scan_subquery_names[rs] = table_name;
  record_scan_one_space_tmp_map[rs] = tab;

  if (!dataspace) record_scan_one_space_tmp_map[rs->upper] = tab;

  need_clean_record_scan_one_space_tmp_map = true;
  /* Set the subquery tmp table idents, if the parent is a cross node join
   * the ident will be set to cross node join sequence vector, when use the
   * sequence vector to get create sql, these tmp table idents will be replaced
   * to real tmp table names.
   **/
  set_subquery_table_names_for_replace(rs, table_name);
}

TableStruct *Statement::get_subquery_table_struct(record_scan *rs) {
  const char *table_name = record_scan_subquery_names_for_replace[rs].c_str();
  const char *schema_name = get_schema();
  string tmp_table_name(table_name);
  if (tmp_table_name.find(SUB_QUERY_TABLE_NAME) != string::npos)
    schema_name = TMP_TABLE_SCHEMA;
  if (!rs->join_belong) {
    LOG_ERROR(
        "Find unecpect table subquery, maybe it is a not support sql: %s.\n",
        sql);
    throw Error("Not support SQL");
  }
  const char *alias = rs->join_belong->alias;
  TableStruct *table_struct = new TableStruct();
  table_struct->table_name = table_name;
  table_struct->schema_name = schema_name;
  table_struct->alias = alias;
  return table_struct;
}

/*This function should analysis the cross node join with table_subquery tables,
 *there are tow situations:
 *1.itself is a cross node join, add the subquery table to the end vector of
 *join_table_sequence; 2.itself is not a cross node join, and it only could be a
 *partitioned table, create a new join_table_sequence. If it is a left join and
 *the table subquery is on the left side, add the tmp table to the end of table
 *vector. Do not support left join with where condition right now.
 *
 * Return: the planed finial execution dataspace*/
DataSpace *Statement::handle_cross_node_join_with_subquery(record_scan *rs) {
  const char *table_name, *schema_name, *alias;
  TableStruct *table_struct;
  vector<TableStruct *> *table_struct_vector;

  if (left_join_record_scan_position_subquery.count(rs)) {
    /* 这里将left join
     * rs中的子查询最终生产的临时表的table_struct按照子查询所在的pos插入到
     * new_table_struct_vector中*/
    vector<vector<TableStruct *> *> *table_struct_vector =
        record_scan_join_tables[rs];
    vector<vector<TableStruct *> *>::iterator it = table_struct_vector->begin();
    vector<vector<TableStruct *> *> *new_table_struct_vector =
        new vector<vector<TableStruct *> *>();
    int table_count = 0;
    int subquery_count = 0;
    while (left_join_record_scan_position_subquery[rs].count(table_count +
                                                             subquery_count)) {
      record_scan *subquery_rs =
          left_join_record_scan_position_subquery[rs]
                                                 [table_count + subquery_count];
      table_struct = get_subquery_table_struct(subquery_rs);
      vector<TableStruct *> *new_table_vector = new vector<TableStruct *>();
      new_table_vector->push_back(table_struct);
      new_table_struct_vector->push_back(new_table_vector);
      ++subquery_count;
    }
    for (; it != table_struct_vector->end(); ++it) {
      new_table_struct_vector->push_back(*it);
      table_count += (*it)->size();
      while (left_join_record_scan_position_subquery[rs].count(
          table_count + subquery_count)) {
        record_scan *subquery_rs =
            left_join_record_scan_position_subquery[rs][table_count +
                                                        subquery_count];
        table_struct = get_subquery_table_struct(subquery_rs);
        vector<TableStruct *> *new_table_vector = new vector<TableStruct *>();
        new_table_vector->push_back(table_struct);
        new_table_struct_vector->push_back(new_table_vector);
        ++subquery_count;
      }
    }
    /*最后判断，是不是FROM最后面的表是子查询，如果是的话，需要修改record_scan_one_space_tmp_map.
     * 因为rs中最后是子查询的话，初始准备record_scan_one_space_tmp_map是不会考虑子查询的dataspace的。对于非left join的子查询，子查询最终会挪动到 rs的最终space上，但对于left join的子查询，
       子查询的位置是不能挪动的，所以这里需要将rs的最后子查询的最终space设置为整个rs的最终dataspace。*/
    if (left_join_record_scan_position_subquery[rs].count(table_count +
                                                          subquery_count - 1)) {
      record_scan *subquery_rs =
          left_join_record_scan_position_subquery[rs][table_count +
                                                      subquery_count - 1];
      DataSpace *final_ds = record_scan_one_space_tmp_map[subquery_rs];
      if (final_ds->is_partitioned()) {
        need_reanalysis_space = true;
      }
      record_scan_one_space_tmp_map[rs] = final_ds;
    }
    delete table_struct_vector;
    record_scan_join_tables[rs] = new_table_struct_vector;
    set_join_final_table(rs);
  } else if (record_scan_join_tables.count(rs)) {
    // current rs is a cross node join.
    // Add the subquery tmp table to the end vector of
    // join table sequence.
    record_scan *tmp = rs->children_begin;
    for (; tmp; tmp = tmp->next) {
      if (record_scan_subquery_names.count(tmp)) {
        table_struct = get_subquery_table_struct(tmp);
        (*record_scan_join_tables[rs])[record_scan_join_tables[rs]->size() - 1]
            ->push_back(table_struct);
      }
    }
    set_join_final_table(rs);
  } else {
    // current rs is not a cross node join. Create a new join table sequence.
    if (record_scan_one_space_tmp_map[rs] &&
        !record_scan_one_space_tmp_map[rs]->get_data_source()) {
      vector<vector<TableStruct *> *> *join_table_sequence =
          new vector<vector<TableStruct *> *>();
      table_struct_vector = new vector<TableStruct *>();
      record_scan *tmp = rs->children_begin;
      for (; tmp; tmp = tmp->next) {
        if (record_scan_subquery_names.count(tmp)) {
          table_struct = get_subquery_table_struct(tmp);
          table_struct_vector->push_back(table_struct);
        }
      }
      join_table_sequence->push_back(table_struct_vector);
      table_link *tmp_tb = rs->first_table;
      table_struct_vector = NULL;
      while (tmp_tb) {
        if (tmp_tb->join->cur_rec_scan == rs) {
          if (!table_struct_vector)
            table_struct_vector = new vector<TableStruct *>();
          table_name = tmp_tb->join->table_name;
          schema_name =
              tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
          alias = tmp_tb->join->alias ? tmp_tb->join->alias : table_name;
          table_struct = new TableStruct();
          table_struct->table_name = table_name;
          table_struct->schema_name = schema_name;
          table_struct->alias = alias;
          table_struct_vector->push_back(table_struct);
        }
        tmp_tb = tmp_tb->next;
      }
      if (table_struct_vector) {
        join_table_sequence->push_back(table_struct_vector);
      }

      record_scan_join_tables[rs] = join_table_sequence;
      set_join_final_table(rs);
    }
  }
  record_scan_need_cross_node_join[rs] = true;

  return record_scan_one_space_tmp_map[rs];
}

bool Statement::sub_query_need_cross_node_join(DataSpace *ds_tmp,
                                               record_scan *cur_rs) {
  for (auto &table_st : can_merge_record_scan_position[cur_rs]) {
    TableStruct &table = table_st.second;
    DataSpace *ds = Backend::instance()->get_data_space_for_table(
        table.schema_name.c_str(), table.table_name.c_str());
    if (ds == ds_tmp && global_table_merged.count(ds)) {
      if (record_scan_one_space_tmp_map.count(cur_rs) &&
          !is_share_same_server(record_scan_one_space_tmp_map[cur_rs], ds))
        return true;
      else if (!record_scan_one_space_tmp_map.count(cur_rs))
        return true;
    }
  }
  return false;
}

/*This function analysis the cross node join record_scan tables, and
 * figure out the finial execution dataspace for this record_scan in
 * advance.
 *
 * Return: the planed finial execution dataspace*/
DataSpace *Statement::handle_cross_node_join(record_scan *rs, ExecutePlan *plan,
                                             bool force_no_merge) {
#ifndef CROSS_NODE_JOIN_DISABLE
  if (st.type != STMT_SELECT && st.type != STMT_INSERT_SELECT &&
      st.type != STMT_REPLACE_SELECT) {
    LOG_ERROR("Not support non select sql do cross node join.\n");
    throw Error("Not support non select sql do cross node join.");
  }
  if (plan->session->is_in_lock()) {
    LOG_ERROR("Not support cross node join in lock table mode.\n");
    throw Error("Not support cross node join in lock table mode.");
  }
  if (is_connect_by) {
    LOG_ERROR("Not support connect by for cross node join.\n");
    throw Error("Not support connect by for cross node join.");
  }
  handle_par_key_equality_full_table(rs);

  Backend *backend = Backend::instance();
  DataSpace *dataspace = NULL, *dataspace_tmp = NULL, *ret_dataspace = NULL;
  TableStruct *table_struct;
  vector<TableStruct *> *table_struct_vector;
  vector<vector<TableStruct *> *> *join_table_sequence =
      new vector<vector<TableStruct *> *>();
  record_scan_join_tables[rs] = join_table_sequence;
  list<TableStruct *> table_struct_list;
  map<TableStruct *, table_link *> table_struct_map;

  prepare_table_vector_and_leftjoin_subquery_from_rs(rs, &table_struct_list,
                                                     &table_struct_map);
  list<TableStruct *>::iterator it;

  map<TableStruct *, set<TableStruct *> >
      record_scan_related_condition_table_map;
  handle_related_condition_full_table(rs, &table_struct_list,
                                      &record_scan_related_condition_table_map);

  int left_join_subquery_count = 0;
  int table_struct_count = 0;

  if (!table_struct_list.empty()) {
    table_struct = table_struct_list.front();
    if (check_left_join(table_struct_map[table_struct])) {
      if (!check_left_join_on_par_key(table_struct_map[table_struct]))
        /*将所有left
         * join
         * 右侧的表的on条件不是分片列的table_struct存储在left_join_right_tables中。*/
        left_join_right_tables.insert(table_struct);
    }
  }

  vector<TableStruct *> *table_struct_vector_end = NULL;
  // Construct the join table sequence, put the tables that can merge
  // into one vector.
  bool has_skip_merge_table = false;
  while (!table_struct_list.empty()) {
    has_skip_merge_table = false;
    table_struct_vector = new vector<TableStruct *>();
    while (left_join_record_scan_position_subquery.count(rs) &&
           left_join_record_scan_position_subquery[rs].count(
               table_struct_count + left_join_subquery_count)) {
      ++left_join_subquery_count;
    }
    bool table_struct_position_end = false;
    // Example: select * from t1 inner join t2 left join t3 on t1.c1 = t3.c1 and
    // t2.c1 = t3.c1; 如果t1和t2不能merge，则t1跳过与t3的merge。
    bool skip_left_join_merge = false;
    table_struct = table_struct_list.front();
    // If current table is a right table of left join, the dataspace should be
    // the tmp table.
    if (left_join_right_tables.count(table_struct)) {
      /*left
       * join的右表不是按照分片列进行on条件，需要在本地挪动成安装分片列进行on条件。*/
      string tmp_table;
      try {
        tmp_table =
            init_left_join_tmp_table_dataspace(table_struct_map[table_struct]);
        LOG_DEBUG("Prepare to redistribute left join partition table %s.\n",
                  tmp_table.c_str());
      } catch (...) {
        for (it = table_struct_list.begin(); it != table_struct_list.end();
             ++it) {
          table_struct_vector->push_back(*it);
        }
        join_table_sequence->push_back(table_struct_vector);
        LOG_ERROR("Got exception when init left join tmp table dataspace.\n");
        throw;
      }
      /*left_join_table_position和left_join_tables用于后续拼语句代码使用.

        left_join_table_position 在拼语句中就是 left_join_position. 然后通过
        CrossNodeJoinTask 传递给拼语句模块。 src/subquery.cc CrossNodeJoinTask
        task = {... &left_join_position, left_join_sqls};

        拼语句模块会将这个值传递给 helper 并最终使用在 CrossNodeJoinInfo
        中。判断当前是否有left join
        需要提前处理，将目前不能merge的表变成临时表的代码详见
        DataMoveJoinInfo::handle_analysis_left_join_expr_str_expression 和
        ConstructMethod::generate_left_join_sql。
      */
      left_join_table_position[rs][join_table_sequence->size() +
                                   left_join_subquery_count] = tmp_table;
      left_join_tables[rs][join_table_sequence->size() +
                           left_join_subquery_count] = table_struct;
      dataspace = backend->get_data_space_for_table(TMP_TABLE_SCHEMA,
                                                    tmp_table.c_str());
      ret_dataspace = dataspace;
    } else {
      dataspace = backend->get_data_space_for_table(
          table_struct->schema_name.c_str(), table_struct->table_name.c_str());
    }
    list<TableStruct *> can_merge_table_struct_list;
    table_link *first_table = table_struct_map[table_struct];
    can_merge_table_struct_list.push_back(table_struct);
    /* We check if the child of the record_scan is missing dataspace and the
     * missing dataspace is current table.
     */
    if (subquery_missing_table.count(rs) &&
        subquery_missing_table[rs] == *table_struct) {
      /*当前rs包含子rs所缺的表，并且这个表就是当前table_struct。那么当前rs的最终dataspace必须是这个table_struct的dataspace，即挪动所有数据到这个dataspace。所以这个table_struct所在的group必须是最后一个group。这里我们通过flag
       * table_struct_position_end来标识当前table_struct
       * group应该要被设置为最后一个group，最后一个group用table_struct_vector_end进行标识。*/
      table_struct_position_end = true;  // this table space must be the finial
      /*The *table_struct is different from subquery_missing_table[rs] for the
       * table name. The equal checking in the if indicate the schema.alias is
       * equal. By default subquery_missing_table[rs]'s table_name is alias,
       * here we adjust to the real table name from *table_struct.*/
      subquery_missing_table[rs] = *table_struct;
      ret_dataspace = dataspace;
    }
    ++table_struct_count;
    table_struct_list.pop_front();
    it = table_struct_list.begin();
    /*如下循环遍历table_struct_list为可以merge的表进行分组。从左往右寻找可以merge的表放入can_merge_table_struct_list。left
     * join是特殊，通常碰到不能merge的表就跳过，继续判定后面的表，因为join顺序是可以调整的，但left
     * join是不允许跳过的，因为join顺序是不能变的，所以一旦碰到left
     * join的表不能merge，那么循环就应该被break。*/
    while (it != table_struct_list.end()) {
      /*所有的left join 表子查询都是不能被merge的，并且不能被跨过。*/
      if (left_join_record_scan_position_subquery.count(rs) &&
          left_join_record_scan_position_subquery[rs].count(
              table_struct_count + left_join_subquery_count)) {
        break;
      }
      if (check_left_join(table_struct_map[*it])) {
        if (subquery_missing_table.count(rs) &&
            (**it != *(table_struct_list.back()) ||
             subquery_missing_table[rs] != **it)) {
          for (it = table_struct_list.begin(); it != table_struct_list.end();
               ++it) {
            table_struct_vector->push_back(*it);
          }
          for (it = can_merge_table_struct_list.begin();
               it != can_merge_table_struct_list.end(); ++it) {
            table_struct_vector->push_back(*it);
          }
          join_table_sequence->push_back(table_struct_vector);
          LOG_ERROR("Not support left join with subquery missing table.\n");
          throw NotSupportedError(
              "Not support left join with subquery missing table.");
        }
        if (skip_left_join_merge) break;
      }
      // Now we put the left join tmp table in one table vector, maybe we can
      // merge it with other table in the future.
      if (left_join_right_tables.count(table_struct)) {
        // If current table is a right table of left join, check if next is a
        // left join too.
        if (check_left_join(table_struct_map[*it])) {
          try {
            if (!check_left_join_on_par_key(table_struct_map[*it]))
              left_join_right_tables.insert(*it);
          } catch (...) {
            for (it = table_struct_list.begin(); it != table_struct_list.end();
                 ++it) {
              table_struct_vector->push_back(*it);
            }
            for (it = can_merge_table_struct_list.begin();
                 it != can_merge_table_struct_list.end(); ++it) {
              table_struct_vector->push_back(*it);
            }
            join_table_sequence->push_back(table_struct_vector);
            LOG_ERROR("Got exception when check left join on par key 1.\n");
            throw;
          }
        }
        break;
      }
      table_struct = *it;
      dataspace_tmp = backend->get_data_space_for_table(
          table_struct->schema_name.c_str(), table_struct->table_name.c_str());
      if (!dataspace->get_data_source() && !dataspace_tmp->get_data_source()) {
        if (check_CNJ_two_partition_table_can_merge(
                table_struct_map[table_struct]->join->cur_rec_scan, first_table,
                table_struct_map[table_struct], (PartitionedTable *)dataspace,
                (PartitionedTable *)dataspace_tmp) &&
            (!has_skip_merge_table ||
             check_tables_has_related_condition(
                 table_struct, &can_merge_table_struct_list,
                 &record_scan_related_condition_table_map))) {
          can_merge_table_struct_list.push_back(table_struct);
          if (subquery_missing_table.count(rs) &&
              subquery_missing_table[rs] == *table_struct) {
            table_struct_position_end = true;
            ret_dataspace = dataspace_tmp;
            subquery_missing_table[rs] = *table_struct;
          }
          it = table_struct_list.erase(it);
          ++table_struct_count;
        } else {
          if (check_left_join(table_struct_map[table_struct])) {
            /*不能merge的情况下，如果是left
             * join，需要检查右表的分片列是否在on条件中，如果是，那么右表需要额外挪动。*/
            try {
              if (!check_left_join_on_par_key(table_struct_map[table_struct]))
                left_join_right_tables.insert(table_struct);
            } catch (...) {
              for (it = table_struct_list.begin();
                   it != table_struct_list.end(); ++it) {
                table_struct_vector->push_back(*it);
              }
              for (it = can_merge_table_struct_list.begin();
                   it != can_merge_table_struct_list.end(); ++it) {
                table_struct_vector->push_back(*it);
              }
              join_table_sequence->push_back(table_struct_vector);
              LOG_ERROR("Got exception when check left join on par key 2.\n");
              throw;
            }
            break;
          }
          ++it;
          skip_left_join_merge = true;
          has_skip_merge_table = true;
        }
      } else {
        if (!dataspace_tmp->get_data_source() &&
            check_left_join(table_struct_map[table_struct])) {
          /*left join的左边不是分片表，而右边是分片表*/
          try {
            if (!check_left_join_on_par_key(table_struct_map[table_struct]))
              left_join_right_tables.insert(table_struct);
          } catch (...) {
            for (it = table_struct_list.begin(); it != table_struct_list.end();
                 ++it) {
              table_struct_vector->push_back(*it);
            }
            for (it = can_merge_table_struct_list.begin();
                 it != can_merge_table_struct_list.end(); ++it) {
              table_struct_vector->push_back(*it);
            }
            join_table_sequence->push_back(table_struct_vector);
            LOG_ERROR("Got exception when check left join on par key 3.\n");
            throw;
          }
          ++it;
          break;
        }
        /*如下是2个space(包含norm_table)的merge判定.*/
        if (stmt_session->is_dataspace_cover_session_level(dataspace,
                                                           dataspace_tmp) &&
            (!force_no_merge ||
             !sub_query_need_cross_node_join(dataspace_tmp, rs)) &&
            (!has_skip_merge_table ||
             check_tables_has_related_condition(
                 table_struct, &can_merge_table_struct_list,
                 &record_scan_related_condition_table_map))) {
          can_merge_table_struct_list.push_back(table_struct);
          if (subquery_missing_table.count(rs) &&
              subquery_missing_table[rs] == *table_struct) {
            table_struct_position_end = true;
            ret_dataspace = dataspace;
            subquery_missing_table[rs] = *table_struct;
          }
          it = table_struct_list.erase(it);
          ++table_struct_count;
          if (!stmt_session->is_dataspace_cover_session_level(dataspace_tmp,
                                                              dataspace))
            gtable_can_merge_dataspace[rs][dataspace_tmp] = dataspace;
        } else if (stmt_session->is_dataspace_cover_session_level(dataspace_tmp,
                                                                  dataspace) &&
                   (!force_no_merge ||
                    !sub_query_need_cross_node_join(dataspace, rs)) &&
                   (!has_skip_merge_table ||
                    check_tables_has_related_condition(
                        table_struct, &can_merge_table_struct_list,
                        &record_scan_related_condition_table_map))) {
          can_merge_table_struct_list.push_back(table_struct);
          if (subquery_missing_table.count(rs) &&
              subquery_missing_table[rs] == *table_struct) {
            table_struct_position_end = true;
            ret_dataspace = dataspace_tmp;
            subquery_missing_table[rs] = *table_struct;
          }
          it = table_struct_list.erase(it);
          ++table_struct_count;
          dataspace = dataspace_tmp;
          if (!stmt_session->is_dataspace_cover_session_level(dataspace,
                                                              dataspace_tmp))
            gtable_can_merge_dataspace[rs][dataspace] = dataspace_tmp;
        } else {
          ++it;
          /*2个norm table 的left join不能merge场景。*/
          if (check_left_join(table_struct_map[table_struct])) {
            break;
          } else {
            skip_left_join_merge = true;
            has_skip_merge_table = true;
          }
        }
      }
    }
    list<TableStruct *>::iterator it_list = can_merge_table_struct_list.begin();
    for (; it_list != can_merge_table_struct_list.end(); ++it_list) {
      table_struct_vector->push_back(*it_list);
    }

    // if we have record_scan_need_table, the final dataspace must be it. so
    // ignore table_struct_position_end
    if (table_struct_position_end && !table_struct_vector_end &&
        !record_scan_need_table.count(rs)) {
      table_struct_vector_end = table_struct_vector;
      LOG_DEBUG("ADD the table in to table_struct_vector_end\n");
    } else {
      join_table_sequence->push_back(table_struct_vector);
      LOG_DEBUG("ADD the table in to join_table_sequence\n");
    }
  }

  if (subquery_missing_table.count(rs) && !table_struct_vector_end) {
    LOG_ERROR(
        "The missing table in subquery dose not exists in parent subquery.\n");
    throw Error(
        "The missing table in subquery dose not exists in parent subquery. "
        "DBScale dose not support this sql, please check. ");
  }

  /* In the situation of missing subquery
   * Example: select * from t1, t2 where t2.c1 = (select min(t3.c2) from t3
   * where t3.c1 = t1.c1); If the record_scan is father, we will change the
   * position of the joining. The output: select * from t2, t1 where t2.c1 =
   * (select min(t3.c2) from t3 where t3.c1 = t1.c1); else if the record_scan is
   * child, we would add the missing table in it. Output is: select min(t3.c2)
   * from t3, t1 where t3.c1 = t1.c1;
   */
  if (table_struct_vector_end) {
    /*  In the situation of missing subquery, normally this if cause is used for
     * parent record scan. if current record_scan should change its join order,
     * the table_struct_vector_end is not NULL if we want to set the join
     * dataspace(output dataspace), we should set the table_struct_vector_end
     */
    /*对于包含缺表的父rs，这里将该缺表的table_struct
     * group放到所有group的最后。这样他就是最终dataspace。*/
    join_table_sequence->push_back(table_struct_vector_end);
    Backend *backend = Backend::instance();
    TableStruct table_struct_tmp;
    if (subquery_missing_table.count(rs))
      table_struct_tmp = subquery_missing_table[rs];

    DataSpace *ds_global =
        backend->get_data_space_for_table(table_struct_tmp.schema_name.c_str(),
                                          table_struct_tmp.table_name.c_str());
    if (ds_global != ret_dataspace) {
      gtable_dependent_output_dataspace[rs][ds_global] = ret_dataspace;
    }
  } else if (record_scan_need_table.count(rs)) {
    /* In the situation of missing subquery, normally this if cause is used for
     * missing subquery record scan. if current record_scan is dependent, we
     * need temporarily add the missing table to its join table sequence. and
     * remove it after construct sql.
     */
    /*当前rs缺表，如下代码将会把所缺的表放到一个单独的table_struct
     * group中，放到所有group最后。这样添加的group是临时性的，因为当前rs并不真的包含这个表，只是为了让后续的跨节点sql生成逻辑将所有的数据挪动到这个缺表所在的dataspace上。最终在src/sub_query.cc/SimpleCrossNodeJoinNode::do_pre_execute中通过判定条件need_merge_all_method来移除/跳过这个table的finial
     * select sql生成。need_merge_all_method的设置在
     * src/statement.cc/create_one_separated_exec_node中。*/
    TableStruct *last_table_struct = new TableStruct();
    *last_table_struct = record_scan_need_table[rs];
    vector<TableStruct *> *last_table_struct_vector =
        new vector<TableStruct *>();
    last_table_struct_vector->push_back(last_table_struct);
    join_table_sequence->push_back(last_table_struct_vector);
    ret_dataspace = backend->get_data_space_for_table(
        last_table_struct->schema_name.c_str(),
        last_table_struct->table_name.c_str());
    dataspace = ret_dataspace;
    if (join_table_sequence->size() <= 1) {
      LOG_ERROR(
          "Not support dependent subquery with no simple table in current "
          "rs.\n");
      throw NotSupportedError(
          "Not support dependent subquery with no simple table in current rs.");
    }
  }
  // TODO: rebuild the vector contains table_struct_vector and return the proper
  // dataspace.
  analysis_sequence_of_join_tables(join_table_sequence);

  if (!table_struct_vector_end) ret_dataspace = dataspace;

  set_join_final_table(NULL);
  if (!ret_dataspace->get_data_source() && !record_scan_need_table.count(rs)) {
    TableStruct *tmp_ts =
        get_join_final_table(*(record_scan_join_tables[rs]->end() - 1));
    table_link *par_table_t = table_struct_map[tmp_ts];
    if (left_join_table_position[rs].count(record_scan_join_tables[rs]->size() -
                                           1 + left_join_subquery_count))
      left_join_par_tables[par_table_t] =
          make_pair(rs, record_scan_join_tables[rs]->size() - 1 +
                            left_join_subquery_count);
    ++par_table_num;
    par_tables.push_back(par_table_t);
    need_clean_par_tables = true;
    record_scan_par_table_map[rs] = par_table_t;
    need_clean_record_scan_par_table_map = true;
  }

  record_scan_need_cross_node_join[rs] = true;
  if (rs->contains_left_join) {
    join_node *tables = rs->join_tables;
    while (tables && tables->right) {
      tables = tables->right;
    }
    if (tables && tables->type == JOIN_NODE_SUBSELECT) {
      ret_dataspace = table_subquery_final_dataspace[tables->sub_select];
    }
  }
  return ret_dataspace;
#endif

  ACE_UNUSED_ARG(rs);
  LOG_ERROR("SQL %s need cross node join.\n", sql);
  throw Error("There are two or more data space conflict.");
}

void Statement::set_vec_final_table(vector<vector<TableStruct *> *> *vec) {
  Backend *backend = Backend::instance();
  vector<vector<TableStruct *> *>::iterator it_v = vec->begin();
  for (; it_v != vec->end(); ++it_v) {
    vector<TableStruct *>::iterator it = (*it_v)->begin();
    TableStruct *tmp = *it;
    DataSpace *space1 = backend->get_data_space_for_table(
        (*it)->schema_name.c_str(), (*it)->table_name.c_str());
    if (space1->is_partitioned()) {
      (*it)->is_final = true;
      continue;
    }
    ++it;
    for (; it != (*it_v)->end(); ++it) {
      DataSpace *space2 = backend->get_data_space_for_table(
          (*it)->schema_name.c_str(), (*it)->table_name.c_str());
      if (space2->is_partitioned()) {
        tmp = *it;
        break;
      }
      if (stmt_session->is_dataspace_cover_session_level(space1, space2)) {
        // do nothing
      } else if (stmt_session->is_dataspace_cover_session_level(space2,
                                                                space1)) {
        space1 = space2;
        tmp = *it;
      }
    }
    tmp->is_final = true;
  }
}

void Statement::set_join_final_table(record_scan *rs) {
  if (rs) {
    if (!record_scan_join_tables.count(rs)) {
      LOG_ERROR("Fail to get join tables when set final join table.\n");
      throw Error(
          "Not support sql, fail to get join tables when set final join "
          "table.");
    }
    set_vec_final_table(record_scan_join_tables[rs]);
  } else {
    map<record_scan *, vector<vector<TableStruct *> *> *>::iterator it_m =
        record_scan_join_tables.begin();
    for (; it_m != record_scan_join_tables.end(); ++it_m) {
      set_vec_final_table(it_m->second);
    }
  }
}

TableStruct *Statement::get_join_final_table(vector<TableStruct *> *vec) {
  if (vec->empty()) {
    LOG_ERROR("Empty join vector.\n");
    throw Error("Not support sql, empty join vector.");
  }
  vector<TableStruct *>::iterator it = vec->begin();
  TableStruct *ret = *it;
  ++it;
  for (; it != vec->end(); ++it) {
    if ((*it)->is_final) {
      ret = *it;
      break;
    }
  }
  return ret;
}

void Statement::analysis_sequence_of_join_tables(
    vector<vector<TableStruct *> *> *join_table_sequence) {
  // TODO: rebuild the vector contains table_struct_vector used for cross node
  // join.
  ACE_UNUSED_ARG(join_table_sequence);
}

bool Statement::check_rs_contain_partkey_equal_value(record_scan *rs) {
  if (!rs->join_tables || rs->join_tables->type != JOIN_NODE_SINGLE)
    return false;
  const char *schema_name = rs->join_tables->schema_name
                                ? rs->join_tables->schema_name
                                : get_schema();
  const char *table_name = rs->join_tables->table_name;
  DataSpace *ds =
      Backend::instance()->get_data_space_for_table(schema_name, table_name);
  if (!ds || !ds->is_partitioned()) return false;
  PartitionedTable *partition_table = (PartitionedTable *)ds;
  const char *part_key = partition_table->get_key_names()->at(0);
  return check_express_contain_partkey_equal_value(
      rs->condition, schema_name, table_name, part_key, rs->join_tables->alias);
}

bool Statement::check_express_contain_partkey_equal_value(
    Expression *where_condition, const char *schema_name,
    const char *table_name, const char *key_name, const char *alias_name) {
  if (!where_condition) return false;
  if (where_condition->type == EXPR_AND) {
    BoolBinaryCaculateExpression *and_expr =
        (BoolBinaryCaculateExpression *)where_condition;
    bool ret1 = check_express_contain_partkey_equal_value(
        and_expr->left, schema_name, table_name, key_name, alias_name);
    bool ret2 = check_express_contain_partkey_equal_value(
        and_expr->right, schema_name, table_name, key_name, alias_name);
    return ret1 || ret2;
  } else if (where_condition->type == EXPR_EQ) {
    CompareExpression *eq_expr = (CompareExpression *)where_condition;
    if (is_column(eq_expr->left) && is_simple_expression(eq_expr->right)) {
      StrExpression *str_expr = (StrExpression *)eq_expr->left;
      if (column_name_equal_key_alias_without_len(str_expr->str_value,
                                                  schema_name, table_name,
                                                  key_name, alias_name)) {
        return true;
      }
    }
    if (is_column(eq_expr->right) && is_simple_expression(eq_expr->left)) {
      StrExpression *str_expr = (StrExpression *)eq_expr->right;
      if (column_name_equal_key_alias_without_len(str_expr->str_value,
                                                  schema_name, table_name,
                                                  key_name, alias_name)) {
        return true;
      }
    }
  }
  return false;
}

void Statement::handle_column_subquery(record_scan *root) {
  /*本函数最终记录需要处理的column subquery对应的等值条件column字符串，例如
   * a in (select ...)rs 记录subquery_peer[rs] = a*/
  if (root->upper && root->condition_belong && root->condition_belong->parent &&
      (root->condition_belong->parent->type == EXPR_IN ||
       root->condition_belong->parent->type == EXPR_NOT_IN) &&
      root->subquerytype == SUB_SELECT_ONE_COLUMN) {
    // 获取 In 左侧的 Expr，如果不是 EXPR_STR 则不需处理
    Expression *peer = get_bool_binary_peer(root->condition_belong);
    if (peer->type != EXPR_STR) {
      return;
    }
    // 看上层是否都为 And，如果不是都是 AND，则不需处理
    Expression *parent = root->condition_belong->parent;
    ConditionAndOr and_or = is_and_or_condition(parent, root->upper);
    if (and_or != CONDITION_AND) {
      return;
    }
    // 看下是不是已经有dual了，如果已经有了说明处理过了，不需处理
    if (root->first_table) {
      table_link *ltable = root->first_table;
      const char *table_name = ltable->join->table_name;
      if (!strcasecmp(table_name, "dual")) return;
    }
    string peer_string;
    peer->to_string(peer_string);
    /*这里不直接进行处理，只是记录到subquery_peer中，后续在
     * Statement::merge_parent_child_record_scan 中
     * check_column_record_scan_can_merge 处理。*/
    // here we do not handle the column, just store it.
    subquery_peer[root] = peer_string;
  }
}

#ifndef DBSCALE_DISABLE_SPARK

map<string, unsigned int> Statement::generate_table_partition_map(
    const char *spark_partition) {
  map<string, unsigned int> table_partition_map;
  vector<string> split_name;
  boost::split(split_name, spark_partition, boost::is_any_of(":"));
  unsigned int item_size = split_name.size();
  if (item_size % 3 != 0) {
    LOG_ERROR(
        "The executable comments of SPARK_PARTITION_NUM should be like "
        "[schema1:table1:num1:schema2:table2:num2]\n");
    throw Error(
        "The executable comments of SPARK_PARTITION_NUM should be like "
        "[schema1:table1:num1:schema2:table2:num2]");
  }

  for (unsigned int i = 0; i < item_size; i += 3) {
    string schema_name = split_name.at(i);
    string table_name = split_name.at(i + 1);
    string partition_num_str = split_name.at(i + 2);
    unsigned int partition_num = atoi(partition_num_str.c_str());
    string full_table_name(schema_name);
    full_table_name.append(".");
    full_table_name.append(table_name);
    table_partition_map[full_table_name] = partition_num;
  }
  return table_partition_map;
}

void Statement::analysis_spark_sql_join(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  bool is_insert_select = false;
  if (spark_insert) {
    is_insert_select = true;

    sql = select_sub_sql.c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
  }

  LOG_DEBUG("Execute the sql using Spark SQL.\n");
  record_scan *rs = st.scanner;
  map<string, unsigned int> table_partition_map;

  if (st.exe_comment_spark_partition_num) {
    table_partition_map =
        generate_table_partition_map(st.exe_comment_spark_partition_num);
  }

  cj_garbage_collector.set_handler(plan->handler);
  ReconstructSQL *reconstruct_sql = new ReconstructSQL(
      schema, &st, "dbscale_tmp", stmt_session, DATA_MOVE_CENTER);
  reconstruct_sql->handler = plan->handler;
  cj_garbage_collector.set_reconstruct_sql(reconstruct_sql);

  vector<vector<TableStruct> > *table_vector_query =
      new vector<vector<TableStruct> >();
  cj_garbage_collector.set_table_vector_query(table_vector_query);

  vector<string> *generated_sql = new vector<string>();
  cj_garbage_collector.set_generated_sql(generated_sql);

  reconstruct_sql->set_orignal_sql(sql);

  set<string> table_name_set;
  table_link *ltable = rs->first_table;
  while (ltable) {
    const char *schema_name =
        ltable->join->schema_name ? ltable->join->schema_name : schema;
    const char *table_name = ltable->join->table_name;
    const char *alias =
        ltable->join->alias ? ltable->join->alias : ltable->join->table_name;

    // if the table is added previously, ignore it
    string full_table_name;
    splice_full_table_name(schema_name, table_name, full_table_name);
    if (table_name_set.count(full_table_name)) {
      ltable = ltable->next;
      continue;
    } else {
      table_name_set.insert(full_table_name);
    }
    TableStruct table_struct;
    table_struct.schema_name = schema_name;
    table_struct.table_name = table_name;
    table_struct.alias = alias;
    vector<TableStruct> table_vector;
    table_vector.push_back(table_struct);
    table_vector_query->push_back(table_vector);
    ltable = ltable->next;
  }

  CrossNodeJoinTask task = {
      rs, table_vector_query, generated_sql, NULL, NULL, NULL, NULL};
  bool use_spark_sql_method = true;
  reconstruct_sql->set_is_use_spark(use_spark_sql_method);
  reconstruct_sql->construct_mul_record_scan_sqls(task);
  vector<string> tmp_tables = reconstruct_sql->get_sorted_tmp_tables();
  DataSpace *ds =
      backend->get_data_space_for_table(spark_dst_schema.c_str(), NULL);
  if (ds->is_normal()) {
    if (!spark_without_insert) init_spark_schema(ds);

    string spark_dst_url = get_spark_dst_jdbc_url(ds);
    string final_sql = generated_sql->at(2 * tmp_tables.size());

    string target_name = spark_dst_schema;
    if (!spark_without_insert) {
      target_name.append(".spark_table");
      int cluster_id = backend->get_cluster_id();
      target_name.append("_");
      target_name.append(SSTR(cluster_id));

      string target_name_base = target_name;
      int loops_num = 0;
      target_name.append("_");
      target_name.append(SSTR(loops_num));
      while (!backend->try_add_spark_sql_table_name(target_name)) {
        ++loops_num;
        target_name = target_name_base;
        target_name.append("_");
        target_name.append(SSTR(loops_num));
      }
      cj_garbage_collector.set_spark_sql_table_name(target_name);
      string drop_sql = "DROP TABLE if exists ";
      drop_sql.append(target_name);
      cj_garbage_collector.set_spark_sql_query(drop_sql);
    }
    string master;
    string url = spark_dbscale_url;
    string dst_url = spark_dst_url;
    string user = spark_user;
    string pwd = spark_password;
    string job_id = plan->session->get_spark_job_id();
    plan->session->set_cur_spark_job_id(job_id);
    string executor_memory;
    int executor_cores;
    OptionParser::instance()->get_option_value(&master, "spark_master");
    OptionParser::instance()->get_option_value(&url, "spark_dbscale_url");
    OptionParser::instance()->get_option_value(&dst_url, "spark_dst_url");
    OptionParser::instance()->get_option_value(&user, "spark_user");
    OptionParser::instance()->get_option_value(&pwd, "spark_password");
    OptionParser::instance()->get_option_value(&executor_memory,
                                               "spark_executor_memory");
    OptionParser::instance()->get_option_value(&executor_cores,
                                               "spark_executor_cores");
    vector<SparkTableInfo> spark_table_infos;
    vector<string>::iterator it_sql = generated_sql->begin();
    vector<string>::iterator it = tmp_tables.begin();

    int loop_num = 0;
    for (; it != tmp_tables.end(); ++it) {
      // string create_sql = *it_sql; create sql ignore
      ++it_sql;
      string select_sql = *it_sql;
      ++it_sql;
      string select_first_part(select_sql, 0, 14);
      bool is_no_column_sql = false;
      if (!strcmp(select_first_part.c_str(), " SELECT 1 from"))
        is_no_column_sql = true;

      TableStruct ts = table_vector_query->at(loop_num).at(0);
      DataSpace *ds = backend->get_data_space_for_table(ts.schema_name.c_str(),
                                                        ts.table_name.c_str());
      int real_partition_num = 0;
      if (ds->is_partitioned()) {
        int par_num = ((PartitionedTable *)ds)->get_real_partition_num();
        if (is_no_column_sql)
          real_partition_num = par_num;
        else {
          string full_table_name;
          splice_full_table_name(ts.schema_name, ts.table_name,
                                 full_table_name);
          if (table_partition_map.count(full_table_name)) {
            real_partition_num = table_partition_map[full_table_name];
          } else if (st.exe_comment_default_partition_num) {
            real_partition_num = atoi(st.exe_comment_default_partition_num);
          } else {
            real_partition_num =
                ((PartitionedTable *)ds)->get_real_partition_num();
          }

          if (real_partition_num % par_num) {
            real_partition_num =
                real_partition_num + par_num - real_partition_num % par_num;
          }
        }

        LOG_DEBUG("The partition number is [%d] with sql [%s]\n",
                  real_partition_num, select_sql.c_str());
      }
      SparkTableInfo spark_table_info = {*it, ts.schema_name, ts.table_name,
                                         select_sql, real_partition_num};
      spark_table_infos.push_back(spark_table_info);
      ++loop_num;
      LOG_DEBUG("Spark parameter name = [%s], sql = [%s]\n", (*it).c_str(),
                select_sql.c_str());
    }

    LOG_DEBUG(
        "Spark parameter url=[%s], spark_dst_url=[%s], user=[%s], "
        "password=[%s], sql=[%s], dest_table=[%s], spark_master=[%s], "
        "job_id=[%s]\n",
        url.c_str(), spark_dst_url.c_str(), user.c_str(), pwd.c_str(),
        final_sql.c_str(), target_name.c_str(), spark_master.c_str(),
        job_id.c_str());

    SparkConfigParameter config_parameter;
    config_parameter.url = url;
    config_parameter.dst_url = dst_url;
    config_parameter.user = user;
    config_parameter.password = pwd;
    config_parameter.dbtable = spark_table_infos;
    config_parameter.sql = final_sql;
    config_parameter.dst_table = target_name;
    config_parameter.spark_master = spark_master;
    config_parameter.memory_limit = executor_memory;
    config_parameter.core_num = executor_cores;
    config_parameter.job_id = job_id;
    config_parameter.trans_method = spark_without_insert;

    if (spark_without_insert) {
      if (is_insert_select) {
        spark_node = plan->get_spark_node(config_parameter, true);
      } else {
        ExecuteNode *spark_node = plan->get_spark_node(config_parameter, false);
        plan->set_start_node(spark_node);
        set_cross_node_join(true);
      }
      return;
    }
    plan->session->start_executing_spark();
    SparkReturnValue return_value = run_spark_service(config_parameter);
    plan->session->stop_executing_spark();

    if (!return_value.success) {
      throw Error(return_value.error_string.c_str());
    }

    string planed_sql = "SELECT * FROM ";
    planed_sql.append(target_name);
    order_item *order_by_list = st.scanner->order_by_list
                                    ? st.scanner->order_by_list
                                    : st.scanner->group_by_list;
    if (order_by_list) {
      planed_sql.append(" ORDER BY ");
      do {
        string order_by_str;
        if (order_by_list->field->alias) {
          order_by_str = order_by_list->field->alias;
        } else {
          Expression *expr = order_by_list->field->field_expr;
          string schema_name, table_name, column_name;
          expr->to_string(order_by_str);
          split_value_into_schema_table_column(order_by_str, schema_name,
                                               table_name, column_name);
          order_by_str = column_name;
        }
        if (order_by_list != st.scanner->order_by_list) planed_sql.append(", ");
        planed_sql.append(order_by_str);
        order_by_list = order_by_list->next;
      } while (order_by_list != st.scanner->order_by_list);
    }

    string *tmp_sql = &(generated_sql->at(2 * tmp_tables.size()));
    *tmp_sql = planed_sql;
    sql = generated_sql->at(2 * tmp_tables.size()).c_str();
    ExecuteNode *node = plan->get_direct_execute_node(ds, sql);
    plan->set_start_node(node);
    set_cross_node_join(true);
  } else {
    throw Error("Not support partition table as final dataspace.\n");
  }
}
#endif

void Statement::analysis_center_join(ExecutePlan *plan) {
  LOG_DEBUG("Execute the sql using Center Join.\n");
  record_scan *rs = st.scanner;

  cj_garbage_collector.set_handler(plan->handler);
  ReconstructSQL *reconstruct_sql = new ReconstructSQL(
      schema, &st, "dbscale_tmp", stmt_session, DATA_MOVE_CENTER);
  cj_garbage_collector.set_reconstruct_sql(reconstruct_sql);

  vector<vector<TableStruct> > *table_vector_query =
      new vector<vector<TableStruct> >();
  cj_garbage_collector.set_table_vector_query(table_vector_query);

  vector<string> *generated_sql = new vector<string>();
  cj_garbage_collector.set_generated_sql(generated_sql);

  reconstruct_sql->set_orignal_sql(sql);

  set<string> table_name_set;
  table_link *ltable = rs->first_table;
  while (ltable) {
    const char *schema_name =
        ltable->join->schema_name ? ltable->join->schema_name : schema;
    const char *table_name = ltable->join->table_name;
    const char *alias =
        ltable->join->alias ? ltable->join->alias : ltable->join->table_name;

    // if the table is added previously, ignore it
    string full_table_name;
    splice_full_table_name(schema_name, table_name, full_table_name);
    if (table_name_set.count(full_table_name)) {
      ltable = ltable->next;
      continue;
    } else {
      table_name_set.insert(full_table_name);
    }

    TableStruct table_struct;
    table_struct.schema_name = schema_name;
    table_struct.table_name = table_name;
    table_struct.alias = alias;
    vector<TableStruct> table_vector;
    table_vector.push_back(table_struct);
    table_vector_query->push_back(table_vector);
    ltable = ltable->next;
  }

  CrossNodeJoinTask task = {
      rs, table_vector_query, generated_sql, NULL, NULL, NULL, NULL};
  reconstruct_sql->construct_mul_record_scan_sqls(task);
  set<string> tmp_tables = reconstruct_sql->get_tmp_tables();

  Backend *backend = Backend::instance();
  DataSpace *ds =
      backend->get_data_space_for_table("mytest", "center_join_dataspace");
  if (ds->get_data_source()) {
    string last_select_sql = generated_sql->at(2 * tmp_tables.size());
    LOG_DEBUG("The final center join sql is [%s]\n", last_select_sql.c_str());
    SeparatedExecNode *final_node =
        new LocalExecNode(this, st.scanner, plan->handler);
    ((LocalExecNode *)final_node)->set_new_sql(last_select_sql);

    set<string>::iterator it = tmp_tables.begin();
    vector<string>::iterator it_sql = generated_sql->begin();
    for (; it != tmp_tables.end(); ++it) {
      DataSpace *ds_tmp = ds->init_center_join_space(*it);
      cj_garbage_collector.add_data_space(ds_tmp);
      string create_sql = *it_sql;
      ++it_sql;
      string insert_sql = *it_sql;
      ++it_sql;

      record_scan *fake_rs = new record_scan();
      cj_garbage_collector.add_fake_record_scan(fake_rs);

      CenterJoinParameter center_join_parameter = {create_sql, insert_sql};
      SeparatedExecNode *sep_node = new CenterJoinNode(
          this, fake_rs, plan->handler, center_join_parameter);

      record_scan_dependent_map[st.scanner].push_back(fake_rs);
      exec_nodes.push_back(sep_node);
      need_clean_exec_nodes = true;
      record_scan_exec_node_map[fake_rs] = sep_node;
    }
    need_clean_exec_nodes = true;
    exec_nodes.push_back(final_node);
    record_scan_exec_node_map[st.scanner] = final_node;

    par_table_num = 0;
    num_of_separated_par_table = 0;
    par_tables.clear();
    num_of_separated_exec_space = 0;
    spaces.clear();
    spaces.push_back(ds);
  } else {
    throw Error("Not support partition table as final dataspace.\n");
  }
}

record_scan *Statement::find_root_union_record_scan(record_scan *rs,
                                                    record_scan **last_union) {
  *last_union = NULL;
  if (!rs->opt_union_all) *last_union = rs;
  while (rs->upper && rs->upper->is_select_union) {
    if (!rs->upper->opt_union_all) *last_union = rs->upper;
    rs = rs->upper;
  }
  return rs;
}
/*
 * This function is handling the leaf record scan in union statement, except the
 * last union left and right record scan if they are leaf. If the record scan is
 * cross node join, generate a new seperated node for it. Otherwise, ignore.
 */
void Statement::handle_union_all_record_scan_tree(record_scan *root,
                                                  record_scan *top_union_rs,
                                                  ExecutePlan *plan) {
  if (root->union_select_left && root->union_select_left->is_select_union) {
    ;
  } else if (root->union_select_left) {
    if (record_scan_need_cross_node_join.count(root->union_select_left)) {
      record_scan_dependent_map[top_union_rs].push_back(
          root->union_select_left);
      SeparatedExecNode *exec_node =
          create_one_separated_exec_node(root->union_select_left, plan);
      exec_nodes.push_back(exec_node);
      need_clean_exec_nodes = true;
      record_scan_exec_node_map[root->union_select_left] = exec_node;
    } else {
      list<record_scan *>::iterator it_rs =
          record_scan_dependent_map[root->union_select_left].begin();
      for (; it_rs != record_scan_dependent_map[root->union_select_left].end();
           ++it_rs) {
        record_scan_dependent_map[top_union_rs].push_back(*it_rs);
      }
    }
    DataSpace *ds_left = record_scan_one_space_tmp_map[root->union_select_left];
    if (ds_left && ds_left->is_partitioned()) {
      table_link *delete_par =
          record_scan_par_table_map[root->union_select_left];
      vector<table_link *>::iterator it2 = par_tables.begin();
      for (; it2 != par_tables.end(); ++it2) {
        table_link *delete_tmp = *it2;
        if (delete_tmp == delete_par) {
          par_tables.erase(it2);
          par_table_num--;
          break;
        }
      }
    }
  }

  if (root->union_select_right && root->union_select_right->is_select_union)
    ;
  else if (root->union_select_right) {
    if (record_scan_need_cross_node_join.count(root->union_select_right)) {
      record_scan_dependent_map[top_union_rs].push_back(
          root->union_select_right);
      SeparatedExecNode *exec_node =
          create_one_separated_exec_node(root->union_select_right, plan);
      exec_nodes.push_back(exec_node);
      need_clean_exec_nodes = true;
      record_scan_exec_node_map[root->union_select_right] = exec_node;
    } else {
      list<record_scan *>::iterator it_rs =
          record_scan_dependent_map[root->union_select_right].begin();
      for (; it_rs != record_scan_dependent_map[root->union_select_right].end();
           ++it_rs) {
        record_scan_dependent_map[top_union_rs].push_back(*it_rs);
      }
    }
    DataSpace *ds_right =
        record_scan_one_space_tmp_map[root->union_select_right];
    if (ds_right && ds_right->is_partitioned()) {
      table_link *delete_par =
          record_scan_par_table_map[root->union_select_right];
      vector<table_link *>::iterator it2 = par_tables.begin();
      for (; it2 != par_tables.end(); ++it2) {
        table_link *delete_tmp = *it2;
        if (delete_tmp == delete_par) {
          par_tables.erase(it2);
          par_table_num--;
          break;
        }
      }
    }
  }
}

map<string, string, strcasecomp> Statement::get_alias_table_map(
    record_scan *rs) {
  map<string, string, strcasecomp> ret;
  table_link *tl = rs->first_table;
  while (tl) {
    if (tl->join->cur_rec_scan == rs) {
      if (tl->join->alias) ret[tl->join->alias] = tl->join->table_name;
    }
    tl = tl->next;
  }
  return ret;
}

void Statement::get_all_join_table(join_node *jtable,
                                   vector<join_node *> *jtables) {
  if (jtable) {
    if (jtable->table_name) jtables->push_back(jtable);
    if (jtable->left) {
      get_all_join_table(jtable->left, jtables);
    }
    if (jtable->right) {
      get_all_join_table(jtable->right, jtables);
    }
  }
}
join_node *Statement::get_first_join_table(record_scan *rs) {
  join_node *join_table = rs->join_tables;
  while (join_table->type == JOIN_NODE_JOIN) {
    join_table = join_table->left;
  }
  return join_table;
}

string Statement::get_first_column(record_scan *rs) {
  string left_key =
      rs->field_list_head->alias ? rs->field_list_head->alias : "";
  if (!left_key.empty()) return left_key;
  if (rs->field_list_head->field_expr &&
      rs->field_list_head->field_expr->type == EXPR_STR) {
    rs->field_list_head->field_expr->to_string(left_key);
    if (!strcmp(left_key.c_str(), "*")) {
      join_node *first_table = get_first_join_table(rs);
      if (first_table->type == JOIN_NODE_SINGLE) {
        const char *schema_name =
            first_table->schema_name ? first_table->schema_name : schema;
        const char *table_name = first_table->table_name;
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
        if (!column_info_vec->empty())
          left_key = column_info_vec->at(0).column_name;
        ti->release_table_info_lock();
      } else if (first_table->type == JOIN_NODE_SUBSELECT) {
        left_key = get_first_column(first_table->sub_select);
      }
      if (!strcmp(left_key.c_str(), "*")) left_key.clear();
    }
  }
  if (left_key.empty()) {
    LOG_ERROR("Union should give an alias for first field. \n");
    throw Error("Union should give an alias for first field.");
  }
  return left_key;
}

void Statement::revise_record_scans(record_scan *end_union_rs,
                                    record_scan *top_union_rs,
                                    ExecutePlan *plan,
                                    bool &right_part_re_distribute) {
  // 1. fetch the final dataspace
  list<record_scan *> record_scan_list = union_leaf_record_scan[top_union_rs];
  record_scan *first_record_scan = record_scan_list.front();
  DataSpace *right_ds =
      record_scan_one_space_tmp_map[end_union_rs->union_select_right];
  DataSpace *final_ds = right_ds;
  if (!right_ds) {
    list<record_scan *>::iterator it = record_scan_list.begin();
    while (!record_scan_one_space_tmp_map.count(*it) ||
           !record_scan_one_space_tmp_map[*it]) {
      ++it;
    }
    final_ds = record_scan_one_space_tmp_map[*it];
  }
  if (end_union_rs == top_union_rs) {
    /*处理 situation 2的情况。*/
    record_scan_one_space_tmp_map[end_union_rs] = final_ds;
    num_of_separated_exec_space = spaces.size();
    spaces.push_back(final_ds);
  }
  /*
   * TODO:
   * 1. we can choose the key pos of left part according to the key pos of right
   * part
   * 2. if the left part is partition table and right part is not, we should
   * move the right part to the left part
   */
  // 2. the final dataspace is partition table, get the partition key we needed
  // for left
  string left_key = first_record_scan->field_list_head->alias
                        ? first_record_scan->field_list_head->alias
                        : "";
  if (final_ds->is_partitioned()) {
    if (left_key.empty()) {
      left_key = get_first_column(first_record_scan);
    }
  } else {
    // 3. reset the key if the dataspace is normal
    left_key = "";
  }

  left_key = get_column_name(left_key);
  string right_key;
  // 4. get the partition key we needed for right, if not partitioned, empty
  // string We always use the first select field item as key, if the partition
  // dataspace's key pos is not the first, we need to use a separate node(table
  // subquery) to re-distribute the data of right child record scan.
  if (right_ds && right_ds->is_partitioned()) {
    int right_key_position = check_rs_contains_partition_key(
        end_union_rs->union_select_right, right_ds);
    if (right_key_position != 0) {
      right_key = end_union_rs->union_select_right->field_list_head->alias
                      ? end_union_rs->union_select_right->field_list_head->alias
                      : "";
      if (right_key.empty()) {
        right_key = get_first_column(end_union_rs->union_select_right);
      }
    }
  }
  right_key = get_column_name(right_key);

  // 5. regard all the record scan before end_union_rs as union all
  first_record_scan = first_record_scan->upper;
  while (first_record_scan != end_union_rs) {
    first_record_scan->opt_union_all = true;
    first_record_scan = first_record_scan->upper;
  }

  // 6. handle the left record_scan, and use table subquery cross node join to
  // move the data of left child to right part.
  /*这里处理section A，将其挪动到section B的final ds上。*/
  end_union_rs->union_select_left->subquerytype = SUB_SELECT_TABLE;
  record_scan_dependent_map[top_union_rs].push_back(
      end_union_rs->union_select_left);
  table_subquery_final_dataspace[end_union_rs->union_select_left] = final_ds;
  dataspace_partition_key[end_union_rs->union_select_left] = left_key;

  if (!end_union_rs->union_select_left->is_select_union &&
      record_scan_one_space_tmp_map.count(end_union_rs->union_select_left) &&
      record_scan_one_space_tmp_map[end_union_rs->union_select_left] &&
      record_scan_one_space_tmp_map[end_union_rs->union_select_left]
          ->is_partitioned()) {
    table_link *delete_par =
        record_scan_par_table_map[end_union_rs->union_select_left];
    vector<table_link *>::iterator it2 = par_tables.begin();
    for (; it2 != par_tables.end(); ++it2) {
      table_link *delete_tmp = *it2;
      if (delete_tmp == delete_par) {
        par_tables.erase(it2);
        par_table_num--;
        break;
      }
    }
  }
  init_table_subquery_dataspace(end_union_rs->union_select_left, plan);
  SeparatedExecNode *exec_node =
      create_one_separated_exec_node(end_union_rs->union_select_left, plan);
  exec_nodes.push_back(exec_node);
  need_clean_exec_nodes = true;
  record_scan_exec_node_map[end_union_rs->union_select_left] = exec_node;

  // 7. handle the right record_scan, if the right key is not empty, means we
  // need to use a table subquery to re-distribute the data of right part, so
  // that the part key of the partition table should be the first field.
  /*这里处理section
   * B,如果B的select的第一列不是分片列，需要以子查询的方式在本地重新挪动成以select第一列为分片列的分片表。*/
  if (right_ds && right_ds->is_partitioned() && !right_key.empty()) {
    end_union_rs->union_select_right->subquerytype = SUB_SELECT_TABLE;
    record_scan_dependent_map[top_union_rs].push_back(
        end_union_rs->union_select_right);
    table_subquery_final_dataspace[end_union_rs->union_select_right] = final_ds;
    dataspace_partition_key[end_union_rs->union_select_right] = right_key;
    init_table_subquery_dataspace(end_union_rs->union_select_right, plan);
    SeparatedExecNode *exec_node =
        create_one_separated_exec_node(end_union_rs->union_select_right, plan);
    exec_nodes.push_back(exec_node);
    need_clean_exec_nodes = true;
    record_scan_exec_node_map[end_union_rs->union_select_right] = exec_node;
    if (end_union_rs == top_union_rs && !top_union_rs->upper)
      need_reanalysis_space = true;
    right_part_re_distribute = true;
  }
}

string Statement::get_column_name(string value) {
  unsigned first_dot_position = value.find(".");
  unsigned second_dot_position = value.find(".", first_dot_position + 1);
  string ret;
  if (first_dot_position == (unsigned)string::npos) {
    ret = value;
  } else if (second_dot_position == (unsigned)string::npos) {
    ret = value.substr(first_dot_position + 1);
  } else {
    ret = value.substr(second_dot_position + 1);
  }
  return ret;
}

/* can merge situation:
 * 1. if all the related dataspaces are normal table, check if dataspace
 * covering is enough.
 * 2. if all the related dataspace are partition table, check if dataspace is
 * covering and the pos of partition key in select list should be the same.
 */
bool Statement::check_union_record_scan_can_merge(record_scan *root,
                                                  record_scan *top_union_rs) {
  bool can_merge = true;
  DataSpace *merge_ds = NULL;
  record_scan *merged_rs = NULL;
  list<record_scan *> record_scan_list = union_leaf_record_scan[top_union_rs];
  list<record_scan *>::iterator it = record_scan_list.begin();
  for (; it != record_scan_list.end(); ++it) {
    DataSpace *current_ds = NULL;
    if (record_scan_one_space_tmp_map.count(*it))
      current_ds = record_scan_one_space_tmp_map[*it];
    if (!merge_ds) {
      merge_ds = current_ds;
      merged_rs = *it;
      continue;
    }

    if (current_ds && merge_ds) {
      bool current_is_par_table = current_ds->is_partitioned();
      bool merge_is_par_table = merge_ds->is_partitioned();
      if (current_is_par_table && merge_is_par_table) {
        if (!is_partition_table_cover((PartitionedTable *)merge_ds,
                                      (PartitionedTable *)current_ds)) {
          can_merge = false;
          break;
        } else {
          int merged_key_position =
              check_rs_contains_partition_key(merged_rs, merge_ds);
          int current_key_position =
              check_rs_contains_partition_key(*it, current_ds);

          if (merged_key_position == -1 ||
              current_key_position != merged_key_position) {
            can_merge = false;
            break;
          }
        }
        table_link *delete_par = record_scan_par_table_map[*it];
        vector<table_link *>::iterator it2 = par_tables.begin();
        for (; it2 != par_tables.end(); ++it2) {
          table_link *delete_tmp = *it2;
          if (delete_tmp == delete_par) {
            par_tables.erase(it2);
            par_table_num--;
            break;
          }
        }
      } else if (!current_is_par_table && !merge_is_par_table) {
        if (stmt_session->is_dataspace_cover_session_level(merge_ds,
                                                           current_ds)) {
          // do nothing
        } else if (stmt_session->is_dataspace_cover_session_level(current_ds,
                                                                  merge_ds)) {
          merge_ds = current_ds;
        } else {
          can_merge = false;
          break;
        }
      } else {
        can_merge = false;
        break;
      }
    }
  }
  if (can_merge) {
    record_scan_one_space_tmp_map[root] = merge_ds;
    num_of_separated_exec_space = spaces.size();
    spaces.push_back(merge_ds);
  }
  return can_merge;
}

void Statement::handle_union_record_scan_tree(record_scan *root,
                                              record_scan *top_union_rs) {
  if (root->union_select_left && !root->union_select_left->is_select_union) {
    union_leaf_record_scan[top_union_rs].push_back(root->union_select_left);
  }

  if (root->union_select_right && !root->union_select_right->is_select_union) {
    union_leaf_record_scan[top_union_rs].push_back(root->union_select_right);
  }
}

bool Statement::child_rs_is_partitioned_space(record_scan *root) {
  record_scan *tmp = root->children_begin;
  for (; tmp; tmp = tmp->next) {
    if (child_rs_is_partitioned_space(tmp)) return true;
  }
  if (record_scan_one_space_tmp_map.count(root) &&
      record_scan_one_space_tmp_map[root] &&
      record_scan_one_space_tmp_map[root]->is_partitioned())
    return true;
  return false;
}

void Statement::print_sql_for_rs(record_scan *root) {
  string str_tmp;
  stmt_node *node = get_latest_stmt_node();
  if (root->end_pos - root->start_pos < 100)
    str_tmp.assign(node->handled_sql + root->start_pos - 1,
                   root->end_pos - root->start_pos + 1);
  else {
    str_tmp.assign(node->handled_sql + root->start_pos - 1, 48);
    str_tmp.append("...to...");
    str_tmp.append(node->handled_sql + root->end_pos - 49, 48);
  }
  root->record_sql = node->handled_sql + root->start_pos - 1;
  root->record_sql_len = root->end_pos - root->start_pos + 1;
  LOG_DEBUG("Record_scan [%@] sql [%s]\n", root, str_tmp.c_str());
}

bool Statement::check_rs_has_table(record_scan *root) {
  record_scan *tmp = root->children_begin;
  for (; tmp; tmp = tmp->next) {
    if (check_rs_has_table(tmp)) return true;
  }
  if (root->first_table) return true;
  record_scan_has_no_table.insert(root);
  return false;
}

void Statement::analysis_record_scan_tree(record_scan *root, ExecutePlan *plan,
                                          unsigned int recursion_level) {
  if (st.type == STMT_CREATE_SELECT || st.type == STMT_CREATE_LIKE) {
    LOG_ERROR(
        "Not support CREATE SELECT or CREATE LIKE statement when it contains "
        "partition table "
        "or can not merge.\n");
    throw NotSupportedError(
        "Not support CREATE SELECT or CREATE LIKE statement when it contains "
        "partition table "
        "or can not merge.");
  }
  if (recursion_level > (max_union_all_sqls > MAX_RECURSION_LEVEL
                             ? max_union_all_sqls
                             : MAX_RECURSION_LEVEL)) {
    LOG_ERROR("Reach max recursion level [%d] with [%d] for sql[%s].\n",
              (max_union_all_sqls > MAX_RECURSION_LEVEL ? max_union_all_sqls
                                                        : MAX_RECURSION_LEVEL),
              recursion_level, sql);
    throw Error("Reach max recursion level.");
  }
  if (root->is_select_union && !check_rs_has_table(root)) {
    LOG_DEBUG(
        "Union with out table, ignore analysis record scan tree for this "
        "union.\n");
    return;
  }
  stmt_type type = st.type;
  if (st.type == STMT_DBSCALE_ESTIMATE) type = st.estimate_type;
  record_scan *tmp = root->children_begin;
  for (; tmp; tmp = tmp->next) {
    analysis_record_scan_tree(tmp, plan, recursion_level + 1);
  }

  print_sql_for_rs(root);

  if (root->is_select_union) {
    /*  DBScale 的union支持总体设计：
     *
     *  如果union/union
     all的rs的dataspace不满足以下2中可以merge的场景，都需要走本设计的逻辑进行实现：(见
     Statement.cc::check_union_record_scan_can_merge)
        1. if all the related dataspaces are normal table, check if dataspace
     covering is enough.
        2. if all the related dataspace are partition table, check if dataspace
     is covering and the pos of partition key in select list should be the same.

        为了支持Union运算，我们将整个union/union
     all的语句划分为3部分（顶层union是从右往左第一个union, 叫做last_union_rs,
     由statement.cc的find_root_union_record_scan函数实现）：
        1. 顶层union 左边，我们叫做section A
        2. 顶层union 右边的第一个rs，我们叫做section B
        3. 顶层union第一个rs之后还有union all的, 我们把这些剩余的union
     all叫做section C

        例如：A union all B union all C union D union F union ALL G
        顶层union是 ...D union F...；section A 为 A ... D; section B为为F；
     section C 为sectin A union section B的结果 union ALL G。 对于section
     A，将所有的union当作union all处理，因为重复的数据会在最终section
     C的union那里处理； 对于section A 和
     B，我们需要将他们调整为可以merge的形式进行union运算。 if Section A can not
     merge with Section B. If B is normal table, generate Separated Node for
     Section A, dataspace is B. If B is partition table, generate Separated Node
     for Section A, dataspace is B, and the partition key should be the first
     select list value. After that, we should check if the pos of partition key
     in select list of B is 0, if 0, ignore the creation of the Seperated Node,
     if 1, create Seperated Node for section B. 这样调整后，section A和section
     B的union运算就可以直接下推去运算了。

        对于section C，有如下几种情况需要考虑：
        situation 1. 整个union/union all的语句里只有union all，没有top union。
     那么直接按照union all的支持逻辑进行处理就好。 situation 2. top
     union之后没有别的union all了，也就是说只有section A和section B。那么top
     union所在的rs： if the Section A+B have no parent, it would generate
     DirectExecuteNode or SendNode-FetchNode. if the Section A+B have parent,
     and not table_subquery, use previous logic. if the Section A+B contains
     parent, and table subquery. The seperated Node of it should not be UNION
     ALL table subquery. situation 3. top union之后还有别的union
     all，也就是同时存在section A和section B和section C。那么section C的最终rs：
           if the Section C have no parent, it would generate union all node.
           if the Section C have parent, and not table_subquery, use previous
     logic. if the Section C have parent, and table_subquery, The seperated Node
     of it should be UNION ALL table subquery.
     *
     *
     * */

    record_scan *last_union_rs = NULL;
    /*如下代码找出顶层的union为top_union_rs，而last_union_rs为当前或父rs中第一个不包含union/union
     all的rs。这里last_union_rs是设计中的顶层
     union，top_union_rs是最顶层的union/union all。
     如果last_union_rs为空，那么是situation 1; 如果last_union_rs ==
     top_union_rs 那么就是situation 2；否则situation 3。*/
    record_scan *top_union_rs =
        find_root_union_record_scan(root, &last_union_rs);
    if (last_union_rs_map.count(top_union_rs)) {
      last_union_rs = last_union_rs_map[top_union_rs];
    } else {
      last_union_rs_map[top_union_rs] = last_union_rs;
    }

    if (!last_union_rs) {  // no last union rs means union all statements.
      /*situation 1的处理逻辑入口。*/
      Backend *backend = Backend::instance();
      // for union, we add a catalog for the union record scan
      DataSpace *space = backend->get_data_space_for_table(NULL, NULL);
      record_scan_one_space_tmp_map[root] = space;
      handle_union_all_record_scan_tree(root, top_union_rs, plan);
    } else {  // union statement should do seperatedly when we met last union
              // rs.
      handle_union_record_scan_tree(root, top_union_rs);
      if (root == last_union_rs) {
        /* we should check the union can merge
         * if can merge, treat it as normal record scan
         * else we can generate seperated node for it.
         */
        bool can_merge = check_union_record_scan_can_merge(root, top_union_rs);
        LOG_DEBUG("Found union, the union is can merge = [%d]\n", can_merge);
        if (!can_merge) {
          /*处理 situaiton 2和3*/
          bool right_part_re_distribute = false;
          revise_record_scans(last_union_rs, top_union_rs, plan,
                              right_part_re_distribute);
          if (!right_part_re_distribute) {
            if (record_scan_dependent_map.count(root->union_select_right) &&
                !record_scan_dependent_map[root->union_select_right].empty()) {
              list<record_scan *>::iterator it_rs =
                  record_scan_dependent_map[root->union_select_right].begin();
              for (; it_rs !=
                     record_scan_dependent_map[root->union_select_right].end();
                   ++it_rs) {
                record_scan_dependent_map[top_union_rs].push_back(*it_rs);
              }
            }
          }
        } else {
          if (last_union_rs == top_union_rs && top_union_rs->upper &&
              !top_union_rs->upper->upper &&
              (type == STMT_INSERT_SELECT || type == STMT_REPLACE_SELECT)) {
            if (spaces.size() > 1) {
              while (num_of_separated_exec_space--) {
                spaces.erase(spaces.begin());
              }
              num_of_separated_exec_space = 0;
            }
          }
          if (last_union_rs == top_union_rs && !top_union_rs->upper)
            need_reanalysis_space = true;
          // for union can merge situation, we should add the child
          // record_scan_dependent_map to top union
          if (record_scan_dependent_map.count(root->union_select_left) &&
              !record_scan_dependent_map[root->union_select_left].empty()) {
            list<record_scan *>::iterator it_rs =
                record_scan_dependent_map[root->union_select_left].begin();
            for (; it_rs !=
                   record_scan_dependent_map[root->union_select_left].end();
                 ++it_rs) {
              record_scan_dependent_map[top_union_rs].push_back(*it_rs);
            }
          }
          if (record_scan_dependent_map.count(root->union_select_right) &&
              !record_scan_dependent_map[root->union_select_right].empty()) {
            list<record_scan *>::iterator it_rs =
                record_scan_dependent_map[root->union_select_right].begin();
            for (; it_rs !=
                   record_scan_dependent_map[root->union_select_right].end();
                 ++it_rs) {
              record_scan_dependent_map[top_union_rs].push_back(*it_rs);
            }
          }
        }
        last_union_rs_map[top_union_rs] = NULL;
      } else {
        // Before last_union_rs, we think the union is union all sub.
        handle_union_all_record_scan_tree(
            root, last_union_rs->union_select_left, plan);
      }
    }

    if (!root->upper) {
      SeparatedExecNode *exec_node = create_one_separated_exec_node(root, plan);
      exec_nodes.push_back(exec_node);
      need_clean_exec_nodes = true;
      record_scan_exec_node_map[root] = exec_node;
      if (spaces.size() > 1) {
        while (num_of_separated_exec_space--) {
          spaces.erase(spaces.begin());
        }
        num_of_separated_exec_space = 0;
      }
    }
    return;
  }
  int record_scan_position = 0;
  if (!cross_node_join && root->upper && root->condition &&
      !(!root->upper->upper && type != STMT_SELECT)) {
    /*dbscale 的correlated subquery支持总体设计：
     *
     * 1.
     依赖analysis_record_scan_tree从下往上逐层遍历record_scan（rs），通过函数check_record_scan_independent判定当前rs是否缺表以及所缺的表TableStruct。
       2.  为每个缺表的rs填充2个数据结构：`record_scan_need_table
     map<record_scan, TableStruct>` 和 `subquery_missing_table map<record_scan,
     TableStruct>`。
           其中record_scan_need_table的key缺表的当前rs，value为所缺的表的tablestruct；subquery_missing_table的key为包含缺表的父rs，value是所包含的缺表tablestruct。
           在填充过程中会进行约束检测，主要约束包括：
           a. 一个rs最多只能缺一张表； b.
     一个父rs所有的子rs要么不缺表，要么缺的是同一张表；c.
     不允许跨rs缺表，并且子rs缺表的父rs不允许再缺表；d. 缺的表不能为子查询；e.
     缺表子查询当前rs的表不能仅有一个子查询而不包含物理表。
           【上述数据结构填充和约束判定在当前if判定包含的代码段内实现。】
       3.
     对于subquery_missing_table标识的父rs，它所包含的缺表将是该rs的最终dataspace，【代码入口见handle_cross_node_join中关于subquery_missing_table的代码】。
       4.
     对于record_scan_need_table标识的当前rs，他所缺的表将是该rs的最终要挪动到的dataspace。由于当前rs并不包含这个表，所以实现跨节点语句生成之前会先将该表的table_struct放到rs的table
     struct group的末尾，而后在处理跨节点语句生成时再跳过最后一个table
     struct的sql生成，来实现将当前rs的数据以临时表的形式挪动到所缺的table_struct的dataspace上。【代码入口见handle_cross_node_join中关于record_scan_need_table的代码】
       5.
     最终缺表的当前rs与包含缺表的父rs都挪动到了同一个dataspace里，就可以下推直接执行了。
       6.
     对于record_scan_need_table标识的rs，在src/sub_query.cc/SimpleCrossNodeJoinNode::do_pre_execute生成语句时，将通过调用optimizer.construct_merge_all_sqls来跳过最后临时填充的缺表table_struct的语句生成。并最终在如下2处进行跳过:
           a.
     CNJAnalysisUtil::check_column_belongs_to中跳过最后一个table_struct; b.
     ConstructMethod::generate_last_select_sql
     在生成last_select_sql的from部分时跳过最后一个table_struct，不将他拼接进去。
     * */

    vector<vector<TableStruct> > table_vector;

    vector<TableStruct> one_table_vector;
    table_vector.push_back(one_table_vector);
    get_all_table_vector(root, &table_vector);
    set<TableStruct> table_struct;
    // Check the record scan is individual or not.
    int independent = CheckComponent::check_record_scan_independent(
        &table_vector, root, schema, &table_struct, stmt_session);
    if (INDEPENDENT_TRUE != independent) {
      if (INDEPENDENT_FALSE == independent) {
        if (table_struct.size() > 1) {
          LOG_ERROR(
              "Not support dependent subquery contains two different table.\n");
          throw NotSupportedError(
              "Not support dependent subquery contains two different table.");
        }
        set<TableStruct>::iterator table = table_struct.begin();
        TableStruct new_table = check_parent_table_alias(root->upper, *table);
        /* Currently, we do not support record scan contains two dependent
         * clidren which does not contains the same table. Currently, we do not
         * support can merge dataspace.
         * TODO: We need to handle the can merge situation.
         */
        if (subquery_missing_table.count(root->upper) &&
            subquery_missing_table[root->upper] != new_table) {
          LOG_ERROR(
              "Not support multiple subqueries dependent different tables.\n");
          throw NotSupportedError(
              "Not support multiple subqueries dependent different tables.");
        }

        record_scan *tmp = root->children_begin;
        for (; tmp; tmp = tmp->next) {
          if (record_scan_need_table.count(tmp) &&
              subquery_missing_table.count(root)) {
            LOG_ERROR(
                "Not support dependent subquery contains dependent "
                "subquery.\n");
            throw NotSupportedError(
                "Not support dependent subquery contains dependent subquery.");
          }
        }

        if (root->upper->is_select_union) {
          LOG_ERROR("Found dependent subquery in UNION sub select.\n");
          string error_info("Found dependent subquery in UNION sub select.");
          LOG_ERROR("%s\n", error_info.c_str());
          throw Error(error_info.c_str());
        }
        Backend *backend = Backend::instance();
        DataSpace *ds_dependent = backend->get_data_space_for_table(
            new_table.schema_name.c_str(), new_table.table_name.c_str());
        /* maybe the record scan contains unsupported function. so we check it.
         */
        if (ds_dependent->is_partitioned() &&
            (check_field_list_contains_invalid_function(root) ||
             check_field_list_invalid_distinct(root))) {
          LOG_ERROR(
              "We do not support dependent subquery with a not supported "
              "function. \n");
          throw Error(
              "We do not support dependent subquery with a not supported "
              "function.");
        }
        subquery_missing_table[root->upper] = new_table;
        record_scan_need_table[root] = new_table;
        root->subquerytype = SUB_SELECT_DEPENDENT;
      }
    }
  }
  /*DBScale 关于 in/not in 子查询实现的总体设计：

    总体而已，包括如下步骤：
     1. 确定需要进行处理的 in/not in 子查询，并记录子查询rs所关联的父查询列信息
        代码入口为handle_column_subquery。
     2. 判断IN的子查询是否可以和父查询相关表进行merge
        例如 ... where t1.a in (select c ...from t2)
        t2所在rs的finial dataspace是否可以和t1进行merge

        merge的判断代码入口在Statement::merge_parent_child_record_scan的check_column_record_scan_can_merge

        如果可以merge，那么将对应的table_struct的pos记录到can_merge_record_scan_position
        如果不能merge，则：
          a.如果子查询类型为 SUB_SELECT_ONE_COLUMN 并且选项
    use_table_for_one_column_subquery 值为
    0，则表示后面会执行子查询并将结果直接拼接至语句，如 t1.a in (1, 2,
    3)，走可以merge的逻辑
          b.其他情况则标记这个rs需要通过column_table_subquery的方式挪动到父rs关联表的dataspace上，然后走可以merge的逻辑。
     3.
    通过column_table_subquery的方式挪动的代码实现在SeparatedExecNode::execute_column_table_subquery
     4.
    对于可以merge场景，利用check_column_record_scan_can_merge进行跨界点拼接语句生成，具体而言，将会把整个可以merge的子查询当作一个常量在拼语句的时候拼接到父rs关联表对应的table_struct
    vec
        所生成的查询语句中,具体参考（expression.cc::SubSelectExpression::do_trace_expression_tree）。


  */
  if (!record_scan_need_table.count(root)) {
    LOG_DEBUG("Do the handle_column_subquery. \n");
    handle_column_subquery(root);
  }

  /*
    DBScale关于表以及record_scan的merge实现的总体设计：

can
merge的含义：就是是否能够将与两个record_scan相关的语句当做只与一个record_scan
相关的情况看待，将2个表当作一个表来看待；主要是针对执行时将语句投递到哪个dataspace执行。

下面的情况是可以merge的：

1. 在一个查询中（`record_scan`）,
两个normal表可以merge，就需要其中一个表所在的机器上面，能够找到所有的另一个表的数据。对应到dbscale代码上面就是`is_dataspace_cover(ds1,
ds2)`
2. 在一个查询中 ( `record_scan` ) , 两个`partition`表可以`merge`，
判断两个`partition`表所占用的后端机器完全相同，分区方式相同，最后还必须有等值分区列。等值分区列的含义是在WHERE函数中，两个表的分区列的等值条件，并且相等的条件的父表达式中只能包含
AND。对应到dbscale上的代码就是`check_two_partition_table_can_merge(rs, tb_link,
tb1, tb2);`
3. 在一个查询中 ( `record_scan` ) ,
一个norm表和一个partition表可以merge，就需要这个normal表的数据是完全同步到partition表所在的所有分片上的。对应的dbscale代码就是
Session::is_dataspace_cover_session_level

如下部分是record_scan之间的，我们通常是判断2个record_scan的finial_dataspace
（record_scan_one_space_tmp_map）来判定merge关系，对应的代码入口为Statement::merge_parent_child_record_scan：

4.
父子关系的子查询可以merge，两个normal表，就需要其中一个表所在的机器上面，能够找到所有的另一个表的数据。对应到dbscale代码上面就是is_dataspace_cover(ds1,
ds2)，与情况一类似，只是”情况一“表述的是一个子查询内部的判断merge的关系，而情况四则是父子子查询相关的。
5.
父子关系的子查询可以merge，两个partition表可以merge，判断两个partition表所占用的后端机器完全相同，分区方式相同，还必须要有分区列比较等值条件。分区列等值比较条件包括：part1.key
= (select part2.key from part2), 类似于上面的等值条件还包括：IN， SOME, ANY ...
6.
父子关系子查询可以merge，一个normal表和一个partition表可以merge，并且这个normal表上面的数据需要全部出现再partition表的所有分片上面。与情况三基本一致。

如下部分是同一个父rs的子rs间的merge，对应的代码入口为statement.cc::analysis_record_scan_tree
中bool all_child_par_can_merge =
false;之后的代码，主要是处理父rs为空dataspace的情况。

7. 两个相同父亲的子查询之间的merge，两个normal表的情况与1,4相仿。
8.
两个相同父亲的子查询之间的merge，两个partition表，需要判断两个partition表所占用的后端机器完全相同，分区方式相同，
最后，需要语句中group by子块，以分区列作为group by的expr。
9. 两个相同父亲的子查询之间的merge，
一个partition表和一个normal表，需要partition表包含所有的normal表的数据。

10. dual 表的处理，分3种情况：
    a. rs之间finial
dataspace是dual的是可以merge，这个主要处理的是我们跨节点过程中填充的 (select 1
from dual) 具体代码参考statement.cc::merge_parent_child_record_scan 15441   if
(child_rs->first_table && child_rs->first_table->join) { 15442     string
table_name = child_rs->first_table->join->table_name; 15443     if
(!table_name.compare("dual")) 15444       return parent_ds; 15445   } b.
rs内部，dbscale_tmp.dual表，这个是不能和其他任意表进行merge，dbscale_tmp.dual的dataspace是在
Backend::initialize()中设置为auth的，auth本身是不能和其他正常的space进行merge的。
     c.
rs内部，非dbscale_tmp的dual，并且还有其他非dual表，那么dual会按照catalog库处理（除非有其他配置）进行合并，这个通常是客户sql直接写的dual表，dbscale内部不会生成这类情况的语句。

  */

  /*
   * 1. try to merge the par tables inside this record scan
   *
   * 2. try to merge the normal table dataspaces inside this record scan
   *
   * 3. try to merge with childrens
   *
   * 4. try to merge with the output dataspaces of separated exec table
   * subquery
   *
   * */
  DataSpace *par_ds = NULL, *ds = NULL, *merged_ds = NULL;
  DataSpace *modify_ds = NULL;
  bool need_handle_cross_node = false;
  /*Firstly merge the par tables*/
  int par_table_num_cur_rs = 0;

  bool need_use_extra_subquery = false;
  if (record_scan_all_par_tables_map.count(root) &&
      !record_scan_all_par_tables_map[root].empty()) {
    par_table_num_cur_rs = record_scan_all_par_tables_map[root].size();
    if (!cross_node_join && par_table_num_cur_rs &&
        (check_field_list_contains_invalid_function(root) ||
         check_field_list_invalid_distinct(root)) &&
        !record_scan_need_table.count(root)) {
      /*DBScale 对非max/min/avg/sum/count聚集函数以及无法转换为group
        by的disctict的支持总体设计：

        1. 总的思想是将数据挪动到一个mysql里由mysql来进行计算支持。
        2. 实现上我们通过模拟correlated
        subquery的缺表挪动，将当前这个record_scan的所有表最终挪动到一个非分片的dataspace。

        具体实现而言:
        1.
        通过函数check_field_list_contains_invalid_function和check_field_list_invalid_distinct来判断需要进行模拟correlated
        subquery的缺表挪动
        2. 将 dbscale_tmp.dual 这个表当作是当前 record scan 的最终的Dataspace。
           最终的DataSpace会被当作是`dbscale_tmp.dual`, `dbscale_tmp.dual`
        是落在 auth datasource。这样做的目的是我们不能允许这个 DataSpace 和
        其它的表merge，进而改变了最终表是个一normal表的情况。
           但是我们又不能允许业务的语句在auth上执行，那么我们会在执行的过程中判断出当前的DataSpace是什么，然后替换成可以执行的一个DataSource
        (具体实现参考 SimpleCrossNodeJoinNode::handle_last_create_dataspace)。*/
      if (!subquery_missing_table.count(root)) {
        TableStruct table_struct;
        table_struct.schema_name = "dbscale_tmp";
        table_struct.table_name = "dual";
        table_struct.alias = "dual";
        record_scan_need_table[root] = table_struct;
        need_handle_cross_node = true;
        need_use_extra_subquery = true;
      }
    }
  }
  if (par_table_num_cur_rs) {
    par_ds = merge_par_tables_one_record_scan(root);
    if (!par_ds || need_handle_cross_node) {
      LOG_DEBUG(
          "Do handle_cross_node_join after "
          "merge_par_tables_one_record_scan.\n");
      par_ds = handle_cross_node_join(root, plan);
    }
    if (!par_ds->get_data_source()) {
      par_table_num_cur_rs = 1;
    } else {
      par_table_num_cur_rs = 0;
    }
  }

  ds = par_ds;

  /*Then merge the normal tables*/
  if (!record_scan_need_cross_node_join.count(root)) {
    if (record_scan_all_spaces_map.count(root) &&
        !record_scan_all_spaces_map[root].empty()) {
      if (par_ds && check_left_join_with_cannot_merge_right_par(root)) {
        LOG_DEBUG(
            "Set ds to NULL cause check_left_join_with_cannot_merge_right_par "
            "is true.\n");
        ds = NULL;
      } else
        ds = merge_dataspace_one_record_scan(root, par_ds);
      if (!ds) {
        LOG_DEBUG(
            "Do handle_cross_node_join after "
            "check_left_join_with_cannot_merge_right_par or "
            "merge_dataspace_one_record_scan.\n");
        ds = handle_cross_node_join(root, plan);
      }
      if (!ds->get_data_source()) {
        par_table_num_cur_rs = 1;
      } else {
        par_table_num_cur_rs = 0;
      }
    }
  }

  if (!need_use_extra_subquery &&
      (root->options & SQL_OPT_DISTINCT ||
       root->options & SQL_OPT_DISTINCTROW) &&
      child_rs_is_partitioned_space(root)) {
    if (check_field_list_invalid_distinct(root) &&
        !record_scan_need_table.count(root)) {
      // TODO: the invalid func also need to be check here
      if (!subquery_missing_table.count(root)) {
        TableStruct table_struct;
        table_struct.schema_name = "dbscale_tmp";
        table_struct.table_name = "dual";
        table_struct.alias = "dual";
        record_scan_need_table[root] = table_struct;
        need_handle_cross_node = true;
        need_use_extra_subquery = true;
      }
    }
  }
  /*dbscale对left join支持总体设计：
      Left Join 总共分两个方面的实现：
      1. A left join B left join C
      2. A left join B left join (subquery)C  即left
   join同时包含子查询，入口判断代码为contains_left_join_subquery

     对于不包含子查询的情况：
        对于 left join，不能改变他们之间的顺序。所以left join
   不能自由的结合join。这就需要我们让它从左向右进行join。
        从左向右join，就会涉及到什么情况是 can merge
   的。那么就有几种情况需要考虑。假设处理 A 和 B 的 left join的时候
        1. B 是非分片表，那么就将A的数据挪动到B即可。
        2. B 是分片表，并且等值列是分片列，那么就将 A 按照等值列进行重新分布到 B
   的数据源上面。
        3. B 是分片表，并且等值列不是分片列，那么就将 B 按照等值列重新分布到 B
   的数据源，然后 继续按照 2 进行操作。(见handle_cross_node_join)
        4. B 是分片表且没有等值列，将 B 挪动到 B 的一个分片，然后按照 1
   继续执行。

        对于3和4
   B需要进行挪动的实现，最终的语句拼接代码见DataMoveJoinInfo::handle_analysis_left_join_expr_str_expression和ConstructMethod::generate_left_join_sql.
        挪动的B表在往后续C挪动的跨节点insert
   select语句中将不能再使用B表名，而应该使用B挪动后的临时表命令，替换的代码见SimpleCrossNodeJoinNode::replace_left_join_tmp_table。

    对于包含子查询的情况：
        1. 首先是记录 left join 的子查询的位置， 例如：
           A LEFT JOIN B LEFT JOIN (subquery)C 的子查询的位置是 3
           A LEFT JOIN B LEFT JOIN (subquery)C LEFT JOIN D LEFT JOIN (subquery)F
   的子查询的位置是 3 和 5
        2. 在处理跨节点的时候会将这个位置分配给 left_join_table_position 和
   left_join_tables
   然后在后面拼语句的时候使用。(见Statement::handle_cross_node_join中的left_join_subquery_count)
        3. 这些子查询都会在处理 subquery 的时候就按照 table subquery 的
   基本流程处理变成一张表。
        4. 在处理当前的查询的时候，会将这些table
   subquery的表插入到需要执行的列表中，按照前面的没有子查询的情况继续处理。(见Statement::handle_cross_node_join_with_subquery)
   *
   *
   * */

  if (ds && !record_scan_need_cross_node_join.count(root) &&
      contains_left_join_subquery(root, ds)) {
    LOG_DEBUG(
        "Do handle_cross_node_join due to "
        "!record_scan_need_cross_node_join.count and "
        "contains_left_join_subquery.\n");
    ds = handle_cross_node_join(root, plan);
    if (ds->is_partitioned()) par_table_num_cur_rs = 1;
  }

  if (ds) {
    record_scan_one_space_tmp_map[root] = ds;
    need_clean_record_scan_one_space_tmp_map = true;
  }

  merged_ds = ds;

  if (!root->upper &&
      (type == STMT_UPDATE || type == STMT_DELETE ||
       type == STMT_INSERT_SELECT || type == STMT_REPLACE_SELECT)) {
    modify_ds = merged_ds;
  }

  /*Start to merge with children*/
  if (root->children_begin) {
    if (!root->upper &&
        (type == STMT_INSERT_SELECT || type == STMT_REPLACE_SELECT)) {
      merged_ds = NULL;
      /*For insert...select, it will only has one direct child record_scan.*/
      if (record_scan_need_cross_node_join.count(root->children_begin) &&
          record_scan_need_cross_node_join[root->children_begin]) {
        record_scan_dependent_map[root].push_back(root->children_begin);
        SeparatedExecNode *exec_node =
            create_one_separated_exec_node(root->children_begin, plan);
        exec_nodes.push_back(exec_node);
        need_clean_exec_nodes = true;
        record_scan_exec_node_map[root->children_begin] = exec_node;
      } else if (root->children_begin->is_select_union &&
                 root->children_begin->opt_union_all) {
        // find a partition key for select stmt
        list<record_scan *> record_scan_list =
            union_leaf_record_scan[root->children_begin];
        record_scan *first_record_scan;
        if (record_scan_list.empty()) {
          record_scan *tmp_child = root->children_begin;
          while (tmp_child->is_select_union) {
            tmp_child = tmp_child->children_begin;
          }
          first_record_scan = tmp_child;
        } else
          first_record_scan = record_scan_list.front();
        string left_key = first_record_scan->field_list_head->alias
                              ? first_record_scan->field_list_head->alias
                              : "";
        if (left_key.empty()) {
          left_key = get_first_column(first_record_scan);
        }
        root->children_begin->subquerytype = SUB_SELECT_TABLE;
        record_scan_one_space_tmp_map[root->children_begin] =
            record_scan_one_space_tmp_map[root];
        table_subquery_final_dataspace[root->children_begin] =
            record_scan_one_space_tmp_map[root->children_begin];
        dataspace_partition_key[root->children_begin] = left_key;
        init_table_subquery_dataspace(root->children_begin, plan);
        record_scan_dependent_map[root].push_back(root->children_begin);
        SeparatedExecNode *exec_node =
            create_one_separated_exec_node(root->children_begin, plan);
        exec_nodes.push_back(exec_node);
        need_clean_exec_nodes = true;
        record_scan_exec_node_map[root->children_begin] = exec_node;
        DataSpace *tmp_ds = record_scan_one_space_tmp_map[root];

        if (tmp_ds->is_partitioned()) {
          merged_ds = NULL;
          need_reanalysis_space = 1;
        } else {
          merged_ds = tmp_ds;
        }
        exec_node->set_subquery_to_select_stmt(true);
      } else if (record_scan_dependent_map.count(root->children_begin) &&
                 !record_scan_dependent_map[root->children_begin].empty()) {
        list<record_scan *>::iterator it =
            record_scan_dependent_map[root->children_begin].begin();
        for (; it != record_scan_dependent_map[root->children_begin].end();
             ++it)
          record_scan_dependent_map[root].push_back((*it));
      }

      if (!merged_ds)
        merged_ds =
            merge_parent_child_record_scan(root, root->children_begin, 0, plan);

      num_of_separated_exec_space = spaces.size();
      num_of_separated_par_table = par_table_num - par_table_num_cur_rs;
      if (record_scan_one_space_tmp_map.count(root->children_begin) &&
          record_scan_one_space_tmp_map[root->children_begin] &&
          !record_scan_one_space_tmp_map[root->children_begin]
               ->get_data_source())
        num_of_separated_par_table--;
      if (!merged_ds) {
#ifdef DEBUG
        /*If insert part can not merge the select part, the select part must
         * has dataspace.*/
        ACE_ASSERT(record_scan_one_space_tmp_map.count(root->children_begin));
        ACE_ASSERT(record_scan_one_space_tmp_map[root->children_begin]);
#endif
        if (!record_scan_one_space_tmp_map.count(root->children_begin) ||
            !record_scan_one_space_tmp_map[root->children_begin])
          throw NotSupportedError("Internal handle exception.");
        spaces.push_back(record_scan_one_space_tmp_map[root->children_begin]);
        spaces.push_back(record_scan_one_space_tmp_map[root]);

        if (!record_scan_one_space_tmp_map[root]->is_tmp_table_space() &&
            record_scan_one_space_tmp_map[root->children_begin]
                ->is_tmp_table_space()) {
          need_reanalysis_space =
              1;  // always do the re-analysis for client insert...select stmt
                  // if the select dataspace is a non-dup tmp dataspace client
                  // stmt insert table should not be tmp table.
        }
        need_clean_spaces = true;
        if (need_reanalysis_space) num_of_separated_par_table = 0;

        SeparatedExecNode *exec_node =
            create_one_separated_exec_node(root, plan);
        exec_nodes.push_back(exec_node);
        need_clean_exec_nodes = true;
        record_scan_exec_node_map[root] = exec_node;
      } else {
        spaces.push_back(merged_ds);
        need_clean_spaces = true;
        SeparatedExecNode *exec_node =
            create_one_separated_exec_node(root, plan);
        exec_nodes.push_back(exec_node);
        need_clean_exec_nodes = true;
        record_scan_exec_node_map[root] = exec_node;
      }
      if (exec_nodes.size() == 1) {
        while (num_of_separated_exec_space--) {
          spaces.erase(spaces.begin());
        }
      }
      return;
    }

    tmp = root->children_begin;
    bool all_child_par_can_merge = false;
    if (!merged_ds) {
      /*If current record_scan has no table, and only have more than one
       * partition table child record_scan. DBScale should try to merge these
       * child record_scan. In some case, they can execute together.*/
      vector<record_scan *> par_table_rs_vec;
      vector<record_scan *> normal_table_rs_vec;
      for (; tmp; tmp = tmp->next) {
        if (record_scan_par_table_map.count(tmp))
          par_table_rs_vec.push_back(tmp);
        else
          normal_table_rs_vec.push_back(tmp);
      }
      size_t num = par_table_rs_vec.size();

      LOG_DEBUG("merged_ds %@, num %d, has distinct %d.\n", merged_ds, num,
                (root->options & SQL_OPT_DISTINCT ||
                 root->options & SQL_OPT_DISTINCTROW)
                    ? 1
                    : 0);
      if (num > 0 && (root->options & SQL_OPT_DISTINCT ||
                      root->options & SQL_OPT_DISTINCTROW)) {
        // If there is partition table, should check the root record scan does
        // not have distinct
        all_child_par_can_merge = false;
      } else if (num >= 2) {
        all_child_par_can_merge = true;
        vector<record_scan *>::iterator it = par_table_rs_vec.begin();
        record_scan *tmp_par_rs = *it;
        ++it;
        for (; it != par_table_rs_vec.end(); ++it) {
          if (!can_two_par_table_record_scan_merge(tmp_par_rs, *it)) {
            all_child_par_can_merge = false;
            break;
          }
        }
        if (all_child_par_can_merge) {
          vector<record_scan *>::iterator n_it = normal_table_rs_vec.begin();
          for (; n_it != normal_table_rs_vec.end(); ++n_it) {
            if (!can_par_global_table_record_scan_merge(tmp_par_rs, *n_it)) {
              all_child_par_can_merge = false;
              break;
            }
          }
        }
      } else if (num == 1 && !normal_table_rs_vec.empty()) {
        all_child_par_can_merge = true;
        record_scan *tmp_par_rs = par_table_rs_vec[0];
        vector<record_scan *>::iterator n_it = normal_table_rs_vec.begin();
        for (; n_it != normal_table_rs_vec.end(); ++n_it) {
          if (!can_par_global_table_record_scan_merge(tmp_par_rs, *n_it)) {
            all_child_par_can_merge = false;
            break;
          }
        }
      }
      if (all_child_par_can_merge) {
        merged_ds = record_scan_one_space_tmp_map[par_table_rs_vec[0]];
        record_scan_one_space_tmp_map[root] = merged_ds;
        need_clean_record_scan_one_space_tmp_map = true;
        // remove the merged par tables from vector par_tables
        vector<record_scan *>::iterator it = par_table_rs_vec.begin();
        ++it;
        table_link *delete_par = NULL, *delete_tmp = NULL;
        for (; it != par_table_rs_vec.end(); ++it) {
          delete_par = record_scan_par_table_map[*it];
          vector<table_link *>::iterator it2 = par_tables.begin();
          for (; it2 != par_tables.end(); ++it2) {
            delete_tmp = *it2;
            if (delete_tmp == delete_par) {
              par_tables.erase(it2);
              par_table_num--;
              break;
            }
          }
        }
      }
    }
    if (!all_child_par_can_merge) {
      /*Normal process for merge the parent with children.*/
      tmp = root->children_begin;
      for (; tmp; tmp = tmp->next) {
        /*Handle the situation that the subquery is a UNION query.
         *Handle the situation that table subquery
         *parent has no table, current space is a partition table,
         *and has more than one subquery.
         *
         *If the parent record_can has distinct, and the subquery has partition
         *table
         *
         *For the situation mentioned above, will not merge the child with
         *parent, treate them as seperated execute node.
         */
        if ((!record_scan_one_space_tmp_map[root] &&
             record_scan_one_space_tmp_map[tmp] &&
             !record_scan_one_space_tmp_map[tmp]->get_data_source() &&
             tmp->subquerytype == SUB_SELECT_TABLE && tmp->next) ||
            (tmp->is_select_union && !record_scan_has_no_table.count(tmp)) ||
            ((root->options & SQL_OPT_DISTINCT ||
              root->options & SQL_OPT_DISTINCTROW) &&
             record_scan_one_space_tmp_map[tmp] &&
             record_scan_one_space_tmp_map[tmp]->is_partitioned()))
          merged_ds = NULL;
        else
          merged_ds = merge_parent_child_record_scan(
              root, tmp, record_scan_position, plan);

        if (table_subquery_final_dataspace.count(tmp)) {
          if (table_subquery_final_dataspace[tmp]->get_data_source())
            par_table_num_cur_rs = 0;
          else
            par_table_num_cur_rs = 1;
        }

        LOG_DEBUG(
            "merge_parent_child_record_scan, par_table_num_cur_rs=[%d], "
            "par_table_num = [%d], has_merged_ds [%d]\n",
            par_table_num_cur_rs, par_table_num, merged_ds ? 1 : 0);

        if (merged_ds) {
          record_scan_one_space_tmp_map[root] = merged_ds;
          need_clean_record_scan_one_space_tmp_map = true;
          ++record_scan_position;
        } else {
          if (!record_scan_one_space_tmp_map[root] &&
              !record_scan_one_space_tmp_map[tmp] &&
              !(record_scan_dependent_map.count(tmp) &&
                !record_scan_dependent_map[tmp].empty()) &&
              !tmp->is_select_union) {
            /*parent and child has no table and child has no dependent
             * child_child. It means: this sql is a simple sql with table,
             * such as "select (select select(1));"*/
            continue;
          }
          if (is_executed_as_table(tmp)) {
            if (type == STMT_CREATE_SELECT) {
              if (record_scan_one_space_tmp_map[tmp] &&
                  record_scan_one_space_tmp_map[root]) {
                if (!record_scan_one_space_tmp_map[tmp]->get_data_source() ||
                    !record_scan_one_space_tmp_map[root]->get_data_source()) {
                  throw UnSupportPartitionSQL(
                      "UnSupport partition table sql for create select stmt.");
                }
                if (!stmt_session->is_dataspace_cover_session_level(
                        record_scan_one_space_tmp_map[root],
                        record_scan_one_space_tmp_map[tmp])) {
                  throw Error(
                      "Not support create select stmt needs cross node "
                      "operation.");
                }
              }
            }
            // TODO: choose a best dataspace for parent, if it has no table
            init_table_subquery_dataspace(tmp, plan);
            record_scan_seperated_table_subquery[root] = true;
            if (!record_scan_one_space_tmp_map[root])
              record_scan_one_space_tmp_map[root] =
                  record_scan_one_space_tmp_map[tmp];
          }

          if (record_scan_need_table.count(tmp)) {
            if (!record_scan_need_cross_node_join.count(tmp)) {
              LOG_DEBUG(
                  "Do handle_cross_node_join due to "
                  "!record_scan_need_cross_node_join.count and "
                  "record_scan_need_table.count.\n");
              ds = handle_cross_node_join(tmp, plan);
            }
          }

          if (record_scan_one_space_tmp_map.count(tmp) &&
              record_scan_one_space_tmp_map[tmp]) {
            spaces.push_back(record_scan_one_space_tmp_map[tmp]);
            need_clean_spaces = true;
          }
          if (is_separated_exec_result_as_sub_query(tmp))
            ++record_scan_position;
          record_scan_dependent_map[root].push_back(tmp);
          SeparatedExecNode *exec_node =
              create_one_separated_exec_node(tmp, plan);
          exec_nodes.push_back(exec_node);
          need_clean_exec_nodes = true;
          record_scan_exec_node_map[tmp] = exec_node;
        }
      }
    }
  }

  /*Try to merge the parent merged dataspace with the output dataspace of
   * separated execution table subquery.*/
  if (record_scan_dependent_map.count(root) &&
      !record_scan_dependent_map[root].empty()) {
    list<record_scan *>::iterator it = record_scan_dependent_map[root].begin();
    record_scan *dep_rs = NULL;
    DataSpace *dep_ds = NULL;
    DataSpace *cur_ds = record_scan_one_space_tmp_map[root];
    bool should_cross_node_join = false;
    if (!left_join_record_scan_position_subquery.count(root)) {
      for (; it != record_scan_dependent_map[root].end(); ++it) {
        dep_rs = *it;
        if (dep_rs->subquerytype == SUB_SELECT_TABLE) {
          /*If a record scan contains one/more separated execute table subquery,
           * dbscale should check whether the finial dataspace of separated
           * execute table subquery can be executed with this record scan.*/
          dep_ds = record_scan_one_space_tmp_map[dep_rs];
          if (!cur_ds) {
            cur_ds = dep_ds;
          } else if (!dep_ds->get_data_source()) {
            // IF subquery dataspace is a partitioned table or duplicated table,
            // then it can merge with parent.
            // do nothing
          } else if (stmt_session->is_dataspace_cover_session_level(cur_ds,
                                                                    dep_ds)) {
            // do nothing
          } else if (stmt_session->is_dataspace_cover_session_level(dep_ds,
                                                                    cur_ds)) {
            cur_ds = dep_ds;
          } else {
            should_cross_node_join = true;
            break;
          }
          if (record_scan_need_cross_node_join.count(root) &&
              record_scan_exec_node_map.count(dep_rs)) {
            should_cross_node_join = true;
          }
          record_scan_one_space_tmp_map[root] = cur_ds;
          need_clean_record_scan_one_space_tmp_map = true;
        }
      }
    }
    if (should_cross_node_join ||
        left_join_record_scan_position_subquery.count(root)) {
      /*If needs cross node join, dbscale should re-analysis the cross
       * node join path.*/
      cur_ds = handle_cross_node_join_with_subquery(root);
      record_scan_one_space_tmp_map[root] = cur_ds;
      need_clean_record_scan_one_space_tmp_map = true;
      if (cur_ds->get_data_source()) par_table_num_cur_rs = 0;
    }
  }

  /*Finially create the separated exec node for the top record_scan.*/
  if (!root->upper) {
    Backend *backend = Backend::instance();
    DataSpace *root_ds = NULL;
    if (record_scan_one_space_tmp_map.count(root) &&
        record_scan_one_space_tmp_map[root]) {
      root_ds = record_scan_one_space_tmp_map[root];
    } else
      root_ds = backend->get_data_space_for_table(schema, NULL);
    num_of_separated_exec_space = spaces.size();
    if (type == STMT_SELECT && record_scan_one_space_tmp_map.count(root) &&
        record_scan_one_space_tmp_map[root] &&
        record_scan_one_space_tmp_map[root]->is_partitioned()) {
      par_table_num_cur_rs = 1;
    } else if (type == STMT_SELECT &&
               record_scan_one_space_tmp_map.count(root) &&
               record_scan_one_space_tmp_map[root] &&
               !record_scan_one_space_tmp_map[root]->is_partitioned()) {
      par_table_num_cur_rs = 0;
    }
    LOG_DEBUG("final, par_table_num_cur_rs=[%d], par_table_num = [%d]\n",
              par_table_num_cur_rs, par_table_num);

    if (enable_global_plan && need_force_move_global(root)) {
      // `need_force_move_global` means do not merge any dataspace.
      record_scan_need_cross_node_join[root] = true;
      DataSpace *ds = handle_cross_node_join(root, plan, true);
      if (!ds->get_data_source()) {
        par_table_num_cur_rs = 1;
      } else {
        par_table_num_cur_rs = 0;
      }
      record_scan_one_space_tmp_map[root] = ds;
      root_ds = record_scan_one_space_tmp_map[root];
    }
    global_table_merged.clear();
    num_of_separated_par_table = par_table_num - par_table_num_cur_rs;

    spaces.push_back(root_ds);
    need_clean_spaces = true;
    SeparatedExecNode *exec_node = create_one_separated_exec_node(root, plan);
    exec_nodes.push_back(exec_node);
    need_clean_exec_nodes = true;
    record_scan_exec_node_map[root] = exec_node;

    /*For update and modify stmt, we should also keep the modify space which
     * will finially do the modify operation.*/
    if (type == STMT_UPDATE || type == STMT_DELETE) {
      if (record_scan_one_space_tmp_map.size() > 1) {
        num_of_separated_exec_space = spaces.size();
        if (modify_ds) {
          spaces.push_back(modify_ds);
          need_clean_spaces = true;
        }
      }
    }
  } else {
    if (enable_global_plan && need_force_move_global(root)) {
      record_scan_need_cross_node_join[root] = true;
      DataSpace *ds = handle_cross_node_join(root, plan, true);
      if (!ds->get_data_source()) {
        par_table_num_cur_rs = 1;
      } else {
        par_table_num_cur_rs = 0;
      }
      record_scan_one_space_tmp_map[root] = ds;
    }
    global_table_merged.clear();
  }
  return;
}

bool Statement::need_force_move_global(record_scan *root) {
  // `need_force_move_global` means do not merge any dataspace.
  bool need_force_move_global = false;
  for (auto &table_st : can_merge_record_scan_position[root]) {
    TableStruct &table = table_st.second;
    DataSpace *ds = Backend::instance()->get_data_space_for_table(
        table.schema_name.c_str(), table.table_name.c_str());
    if (global_table_merged.count(ds)) {
      if (record_scan_one_space_tmp_map.count(root) &&
          !is_share_same_server(record_scan_one_space_tmp_map[root], ds))
        need_force_move_global = true;
      else if (!record_scan_one_space_tmp_map.count(root))
        need_force_move_global = true;
    }
  }
  need_force_move_global =
      need_force_move_global && (!record_scan_need_cross_node_join.count(root));
  return need_force_move_global;
}

SeparatedExecNode *Statement::create_one_separated_exec_node(
    record_scan *node_rs, ExecutePlan *plan) {
  SeparatedExecNode *ret = NULL;
  if (node_rs->subquerytype == SUB_SELECT_DEPENDENT) {
    ret =
        new SimpleCrossNodeJoinNode((Statement *)this, node_rs, plan->handler);
    set_is_may_cross_node_join_related(true);
    map<DataSpace *, DataSpace *> gtable_output_dataspace;
    if (node_rs->upper &&
        gtable_dependent_output_dataspace.count(node_rs->upper))
      gtable_output_dataspace =
          gtable_dependent_output_dataspace[node_rs->upper];

    ret->set_output_dataspace(gtable_output_dataspace);
  } else if (node_rs->is_select_union && is_executed_as_table(node_rs)) {
    ret = new UnionTableSubqueryNode((Statement *)this, node_rs, plan->handler);
    set_is_may_cross_node_join_related(true);
    if (table_subquery_final_dataspace.count(node_rs))
      ret->set_final_table_subquery_dataspace(
          table_subquery_final_dataspace[node_rs]);
  } else if (record_scan_need_cross_node_join.count(node_rs)) {
#ifdef DEBUG
    Statement *top_stmt = plan->session->get_original_stmt();
    if (top_stmt && top_stmt->st.type == STMT_SELECT &&
        !top_stmt->st.scanner->is_select_union && top_stmt != this)
      ACE_ASSERT(0);
#endif
    ret =
        new SimpleCrossNodeJoinNode((Statement *)this, node_rs, plan->handler);
    if (table_subquery_final_dataspace.count(node_rs))
      ret->set_final_table_subquery_dataspace(
          table_subquery_final_dataspace[node_rs]);
    set_is_may_cross_node_join_related(true);
    if (record_scan_need_table.count(node_rs)) ret->set_need_merge_all_method();
  } else {
    ret = new LocalExecNode((Statement *)this, node_rs, plan->handler);
    if (table_subquery_final_dataspace.count(node_rs))
      ret->set_final_table_subquery_dataspace(
          table_subquery_final_dataspace[node_rs]);
  }
  return ret;
}

void Statement::assemble_dbscale_show_default_session_variables(
    ExecutePlan *plan) {
  ExecuteNode *dbscale_show_default_session_var_node =
      plan->get_dbscale_show_default_session_var_node();
  plan->set_start_node(dbscale_show_default_session_var_node);
  return;
}
void Statement::assemble_dbscale_show_version_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale_show_version plan\n");
  ExecuteNode *dbscale_show_version_node =
      plan->get_dbscale_show_version_node();
  plan->set_start_node(dbscale_show_version_node);
  return;
}
void Statement::assemble_show_components_version_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble show components version plan\n");
  ExecuteNode *show_conponents_version_node =
      plan->get_show_components_version_node();
  plan->set_start_node(show_conponents_version_node);
  return;
}
void Statement::assemble_show_version_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble show version plan\n");
  ExecuteNode *show_version_node = plan->get_show_version_node();
  plan->set_start_node(show_version_node);
  return;
}
void Statement::assemble_dbscale_show_hostname_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale_show_hostname plan\n");
  ExecuteNode *dbscale_show_hostname_node =
      plan->get_dbscale_show_hostname_node();
  plan->set_start_node(dbscale_show_hostname_node);
  return;
}
void Statement::assemble_dbscale_show_all_fail_transaction_plan(
    ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale_show_all fail transaction plan\n");
  ExecuteNode *node = plan->get_dbscale_show_all_fail_transaction_node();
  plan->set_start_node(node);
  return;
}
void Statement::assemble_dbscale_show_detail_fail_transaction_plan(
    ExecutePlan *plan, const char *xid) {
  LOG_DEBUG("assemble dbscale show detail fail transaction plan\n");
  ExecuteNode *node = plan->get_dbscale_show_detail_fail_transaction_node(xid);
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_partition_table_status_plan(
    ExecutePlan *plan, const char *table_name) {
  LOG_DEBUG("assemble dbscale show partition table status. \n");
  ExecuteNode *node =
      plan->get_dbscale_show_partition_table_status_node(table_name);
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_schema_acl_info(ExecutePlan *plan,
                                                      bool is_show_all) {
  LOG_DEBUG("assemble dbscale show schema ACL info plan\n");
  ExecuteNode *node = plan->get_dbscale_show_schema_acl_info_node(is_show_all);
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_table_acl_info(ExecutePlan *plan,
                                                     bool is_show_all) {
  LOG_DEBUG("assemble dbscale show table ACL info plan\n");
  ExecuteNode *node = plan->get_dbscale_show_table_acl_info_node(is_show_all);
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_path_info(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale show path info.\n");
  ExecuteNode *node = plan->get_dbscale_show_path_info();
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_warnings(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale show warnings plan.\n");
  ExecuteNode *node = plan->get_dbscale_show_warnings_node();
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_show_critical_errors(ExecutePlan *plan) {
  LOG_DEBUG("assemble dbscale show critical errors.\n");
  ExecuteNode *node = plan->get_dbscale_show_critical_errors_node();
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_clean_fail_transaction_plan(ExecutePlan *plan,
                                                             const char *xid) {
  LOG_DEBUG("assemble dbscale clean fail transaction plan\n");
  ExecuteNode *node = plan->get_dbscale_clean_fail_transaction_node(xid);
  plan->set_start_node(node);
  return;
}
void Statement::assemble_dbscale_show_shard_partition_table_plan(
    ExecutePlan *plan, const char *scheme_name) {
  ExecuteNode *node = plan->get_dbscale_show_shard_partition_node(scheme_name);
  plan->set_start_node(node);
  return;
}
void Statement::assemble_dbscale_show_rebalance_work_load_plan(
    ExecutePlan *plan, const char *scheme_name, list<string> sources,
    const char *schema_name, int is_remove) {
  ExecuteNode *node = plan->get_dbscale_show_rebalance_work_load_node(
      scheme_name, sources, schema_name, is_remove);
  plan->set_start_node(node);
  return;
}

void Statement::assemble_dbscale_add_default_session_variables(
    ExecutePlan *plan) {
  DataSpace *dataspace =
      Backend::instance()->get_data_space_for_table(schema, NULL);
  ExecuteNode *dbscale_add_default_session_var_node =
      plan->get_dbscale_add_default_session_var_node(dataspace);
  plan->set_start_node(dbscale_add_default_session_var_node);
  return;
}
void Statement::assemble_dbscale_remove_default_session_variables(
    ExecutePlan *plan) {
  ExecuteNode *dbscale_remove_default_session_var_node =
      plan->get_dbscale_remove_default_session_var_node();
  plan->set_start_node(dbscale_remove_default_session_var_node);
  return;
}

void Statement::mark_table_info_to_delete(ExecutePlan *plan) {
  const char *schema_name;
  const char *table_name;
  table_link *table = st.table_list_head;

  switch (st.type) {
    case STMT_ALTER_TABLE: {
      schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      table_name = table->join->table_name;
      plan->session->add_table_info_to_delete(schema_name, table_name);
      break;
    }
    case STMT_DROP_DB: {
      schema_name = st.sql->drop_db_oper->dbname->name;
      plan->session->add_db_table_info_to_delete(schema_name);
      break;
    }
    case STMT_DROP_TB: {
      while (table) {
        schema_name =
            table->join->schema_name ? table->join->schema_name : schema;
        table_name = table->join->table_name;
        plan->session->add_table_info_to_delete(schema_name, table_name);
        table = table->next;
      }
      break;
    }
    default:
      break;
  }
}

bool Statement::check_federated_name() {
  if (!st.table_list_head || st.table_list_head != st.table_list_tail)
    return false;
  bool ret = false;
  const char *table_name = NULL;
  table_link *table = st.table_list_head;
  if (table) table_name = table->join->table_name;
  if (table_name) {
    string table_string(table_name);
    if (table_string.compare(0, strlen(TMP_TABLE_NAME), TMP_TABLE_NAME) == 0)
      ret = true;
  }
  return ret;
}

inline bool Statement::check_federated_show_status() {
  bool ret = false;
  const char *table_name = NULL;
  table_name = st.sql->show_tables_oper->table_pattern;
  if (table_name) {
    string table_string(table_name);
    // if (table_string.find(TMP_TABLE_NAME) == 0)
    if (table_string.compare(0, strlen(TMP_TABLE_NAME), TMP_TABLE_NAME) == 0) {
      ret = true;
    }
  }

  return ret;
}

void Statement::get_fe_tmp_table_name(string sql, set<string> &name_vec) {
  string tmp_table_name;
  size_t start_pos = sql.find(TMP_TABLE_NAME);
  while (start_pos != string::npos) {
    size_t end_pos1 = sql.find(".", start_pos);
    size_t end_pos2 = sql.find(",", start_pos);
    size_t end_pos3 = sql.find(" ", start_pos);
    size_t end_pos = end_pos1 < end_pos2
                         ? (end_pos1 < end_pos3 ? end_pos1 : end_pos3)
                         : (end_pos2 < end_pos3 ? end_pos2 : end_pos3);
    size_t end_pos4 = sql.find("`", start_pos);
    end_pos = end_pos < end_pos4 ? end_pos : end_pos4;
    tmp_table_name.assign(sql, start_pos, end_pos - start_pos);
    if (tmp_table_name.find("_fed_") != string::npos)
      name_vec.insert(tmp_table_name);
    start_pos = sql.find(TMP_TABLE_NAME, end_pos);
  }
}
bool Statement::check_federated_select() {
  bool ret = false;
  string sql_string(sql);
  if (sql_string.find(FEDERATED_SELECT_END_STRING) ==
      sql_string.length() - SIZE_OF_END_STRING)
    ret = true;

  return ret;
}
int Statement::get_field_num() {
  int field_num = 0;
  record_scan *rs = st.scanner;
  for (field_item *fi = rs->field_list_head; fi != NULL; fi = fi->next) {
    ++field_num;
  }
  return field_num;
}

void Statement::assemble_show_create_event_plan(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();
  dataspace = backend->get_data_space_for_table(schema, NULL);
  assemble_direct_exec_plan(plan, dataspace);
}

void Statement::assemble_show_tables_plan(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  LOG_DEBUG("Assemble show tables plan.\n");
  if (!datasource_in_one) {
    const char *schema_name;
    map<DataSpace *, bool> dataspaces;
    if (st.sql->show_tables_oper == NULL)
      schema_name = schema;
    else {
      schema_name = st.sql->show_tables_oper->db_name
                        ? st.sql->show_tables_oper->db_name
                        : schema;
    }

    backend->get_all_dataspace_for_schema(schema_name, dataspaces);
    map<DataSpace *, bool>::iterator it = dataspaces.begin();

    if (dataspaces.size() == 1) {
      assemble_direct_exec_plan(plan, it->first);
    } else {
      ExecuteNode *send_node = plan->get_send_node();
      list<int> column_index;
      column_index.push_back(0);
      ExecuteNode *distinct_node = plan->get_distinct_node(column_index);
      send_node->add_child(distinct_node);
      ExecuteNode *fetch_node;

      list<DataSpace *> space_list;
      list<DataSpace *>::iterator it_s = space_list.begin();
      bool share_same_server = false;
      for (; it != dataspaces.end(); ++it) {
        share_same_server = false;
        for (it_s = space_list.begin(); it_s != space_list.end(); ++it_s) {
          if (is_share_same_server(it->first, *it_s)) {
            share_same_server = true;
            break;
          }
        }
        if (!share_same_server) {
          space_list.push_back(it->first);
          const char *used_sql = adjust_stmt_sql_for_shard(it->first, sql);
          fetch_node = plan->get_fetch_node(it->first, used_sql);
          distinct_node->add_child(fetch_node);
        }
      }

      plan->set_start_node(send_node);
    }
  } else {
    LOG_DEBUG("Show tables from metadata_data_space.\n");
    DataSpace *space = Backend::instance()->get_metadata_data_space();
    if (!space) {
      space = Backend::instance()->get_catalog();
    }
    assemble_direct_exec_plan(plan, space);
  }
}
void Statement::assemble_federated_table_status_node(ExecutePlan *plan) {
  LOG_DEBUG("Assemble federated table status node.\n");
  ExecuteNode *node = plan->get_federated_table_node();
  plan->set_start_node(node);
}

void Statement::assemble_empty_set_node(ExecutePlan *plan, int field_num) {
  LOG_DEBUG("Assemble federated table status node.\n");
  ExecuteNode *node = plan->get_empty_set_node(field_num);
  plan->set_start_node(node);
}

void Statement::handle_federated_join() {
  vector<SeparatedExecNode *>::iterator it = exec_nodes.begin();
  for (; it != exec_nodes.end(); ++it) {
    (*it)->set_tmp_tables_to_dead();
  }
}

TableStruct Statement::check_parent_table_alias(record_scan *parent_rs,
                                                TableStruct table) {
  TableStruct ret = table;
  table_link *tmp_tb = parent_rs->first_table;
  bool found_table = false;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == parent_rs) {
      const char *table_name = tmp_tb->join->table_name;
      const char *schema_name =
          tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
      const char *alias =
          tmp_tb->join->alias ? tmp_tb->join->alias : table_name;
      TableStruct table_struct;
      table_struct.table_name = table_name;
      table_struct.schema_name = schema_name;
      table_struct.alias = alias;
      if (table.schema_name.empty() && table.alias == string(alias)) {
        ret = table_struct;
        found_table = true;
        break;
      } else if (table_struct == table) {
        ret = table_struct;
        found_table = true;
        break;
      }
    }
    tmp_tb = tmp_tb->next;
  }
  if (!found_table) {
    LOG_ERROR(
        "Not support dependent subquery that parent contains no missing "
        "table.\n");
    throw NotSupportedError(
        "Not support dependent subquery that parent contains no missing "
        "table.");
  }
  missing_table_map[ret] = tmp_tb;
  return ret;
}

void Statement::handle_create_or_drop_view(ExecutePlan *plan) {
  if (!on_view) return;
  Backend *backend = Backend::instance();
  if (st.type == STMT_CREATE_VIEW) {
    create_view_op_node *view_node = st.sql->select_oper->create_view_oper;
    const char *view_name = view_node->view;
    const char *view_schema = view_node->schema ? view_node->schema : schema;

    DataSpace *schema_space =
        backend->get_data_space_for_table(view_schema, NULL);
    DataSpace *meta = backend->get_auth_data_space();
    if (((schema_space->get_dataspace_type() == SCHEMA_TYPE &&
          ((Schema *)schema_space)
              ->get_is_schema_pushdown_stored_procedure()) ||
         schema_space->get_dataspace_type() == CATALOG_TYPE) &&
        (!is_share_same_server(meta, schema_space))) {
      try {
        schema_space->execute_one_modify_sql(sql, plan->handler, view_schema);
      } catch (...) {
        LOG_ERROR("Fail to create view on schema dataspace [%s].\n",
                  view_schema);
        throw;
      }
    }

    string tmp_view(view_schema);
    tmp_view.append(".");
    tmp_view.append(view_name);
    vector<vector<string> > view_sqls;
    string query_sql(
        "SELECT VIEW_DEFINITION, CHARACTER_SET_CLIENT FROM "
        "INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '");
    query_sql.append(view_schema);
    query_sql.append("' AND TABLE_NAME = '");
    query_sql.append(view_name);
    query_sql.append("';");
    DataSpace *dataspace = backend->get_config_data_space();
    if (!dataspace) {
      LOG_ERROR("Can not find the metadata dataspace.\n");
      throw Error(
          "Fail to get meta dataspace, please make sure meta datasource is "
          "configured.");
    }

    Connection *conn = NULL;
    try {
      conn = dataspace->get_connection(plan->session);
      if (!conn) {
        LOG_ERROR(
            "Fail to get connection on config source when create view.\n");
        throw Error("Fail to get config source connection.");
      }
      if (conn->get_session_var_map_md5() !=
          plan->session->get_session_var_map_md5()) {
        conn->set_session_var(plan->session->get_session_var_map());
#ifdef DEBUG
        LOG_DEBUG("conn set session var with session var map size %d.\n",
                  plan->session->get_session_var_map()->size());
#endif
      }
      conn->reset();
      conn->query_for_all_column(query_sql.c_str(), &view_sqls);
      conn->get_pool()->add_back_to_free(conn);
    } catch (...) {
      if (conn) conn->get_pool()->add_back_to_dead(conn);
      throw;
    }

    string view_sql = view_sqls[0][0];
    string view_char = view_sqls[0][1];
    if (MYSQL_VERSION_8 == Backend::instance()->get_backend_server_version())
      replace_view_select_field_alias(view_sql);

    if (view_char.empty()) view_char.assign("utf8");
    bool insert_view_ok = Driver::get_driver()->get_config_helper()->add_view(
        view_schema, view_name, view_sql.c_str(), view_char.c_str());
    if (!insert_view_ok) {
      LOG_ERROR("Fail to add view to dbscale, the sql is [%s].\n",
                view_sql.c_str());
    }
    backend->add_one_view(tmp_view, view_sql, ctype);
#ifndef CLOSE_MULTIPLE
    char param[256] = {0};
    if (multiple_mode) {
      sprintf(param, "%s %s %s %s", "1", view_schema, view_name,
              view_char.c_str());
      if (!backend->start_sync_topic(VIEW_TOPIC_NAME, param)) {
        LOG_ERROR("Fail to create view cause start VIEW_TOPIC error.\n");
        throw Error("Fail to create view cause start VIEW_TOPIC error");
      }
      try {
        MultipleManager *mul = MultipleManager::instance();
        MultipleSyncTool *sync_tool = mul->get_sync_tool();
        ViewInfoMessage *message =
            new ViewInfoMessage(view_schema, view_name, view_char);
        mul->pub_cluster_management_message(message);
        sync_tool->get_sync_topic()->set_sync_info_op_param(param);
        sync_tool->generate_sync_topic_sql(SYNC_TOPIC_STATE_FIN, param, NULL);
        sync_tool->publish_sync_message();
        sync_tool->wait_all_children_sync();
      } catch (Exception &e) {
        LOG_ERROR("error happen in add view, due to %s.\n", e.what());
        throw Error("error hanpen in add_view sync.\n");
      }
    }
#endif
    LOG_DEBUG("Create view [%s] with sql [%s].\n", tmp_view.c_str(),
              view_sql.c_str());
  } else {
    table_link *table = st.table_list_head;
    const char *schema_name, *table_name;
    set<string> view_name_set;
    while (table) {
      schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      table_name = table->join->table_name;

      string drop_sql("DROP VIEW IF EXISTS `");
      drop_sql.append(schema_name);
      drop_sql.append("`.`");
      drop_sql.append(table_name);
      drop_sql.append("`");
      DataSpace *schema_space =
          backend->get_data_space_for_table(schema_name, NULL);
      DataSpace *meta = backend->get_auth_data_space();
      if (((schema_space->get_dataspace_type() == SCHEMA_TYPE &&
            ((Schema *)schema_space)
                ->get_is_schema_pushdown_stored_procedure()) ||
           schema_space->get_dataspace_type() == CATALOG_TYPE) &&
          (!is_share_same_server(meta, schema_space))) {
        try {
          schema_space->execute_one_modify_sql(drop_sql.c_str(), plan->handler,
                                               schema_name);
        } catch (...) {
          LOG_ERROR("Fail to drop view on schema dataspace [%s].\n",
                    schema_name);
          throw;
        }
      }

      bool insert_view_ok =
          Driver::get_driver()->get_config_helper()->remove_view(schema_name,
                                                                 table_name);
      if (!insert_view_ok) {
        LOG_ERROR("Fail to drop view to dbscale, the view is [%s.%s].\n",
                  schema_name, table_name);
      }
      string full_view(schema_name);
      full_view.append(".");
      full_view.append(table_name);
      table = table->next;
      view_name_set.insert(full_view);
    }
    backend->remove_view_set(view_name_set);
#ifndef CLOSE_MULTIPLE
    string param;
    if (multiple_mode) {
      param += "0";
      set<string>::iterator it;
      for (it = view_name_set.begin(); it != view_name_set.end(); ++it) {
        param += " ";
        param += *it;
      }
      if (!backend->start_sync_topic(VIEW_TOPIC_NAME, param.c_str())) {
        LOG_ERROR("Fail to drop view cause start VIEW_TOPIC error.\n");
        throw Error("Fail to drop view cause start VIEW_TOPIC error");
      }
      try {
        MultipleManager *mul = MultipleManager::instance();
        MultipleSyncTool *sync_tool = mul->get_sync_tool();
        ViewInfoMessage *message = new ViewInfoMessage(view_name_set);
        mul->pub_cluster_management_message(message);
        sync_tool->get_sync_topic()->set_sync_info_op_param(param.c_str());
        sync_tool->generate_sync_topic_sql(SYNC_TOPIC_STATE_FIN, param.c_str(),
                                           NULL);
        sync_tool->publish_sync_message();
        sync_tool->wait_all_children_sync();
      } catch (Exception &e) {
        LOG_ERROR("error happen in add view, due to %s.\n", e.what());
        throw Error("error hanpen in add_view sync.\n");
      }
    }
#endif
  }
  Driver *driver = Driver::get_driver();
  driver->acquire_session_mutex();
  set<Session *, compare_session> *session_set = driver->get_session_set();
  set<Session *, compare_session>::iterator it = session_set->begin();
  for (; it != session_set->end(); ++it) {
    (*it)->set_view_updated(true);
  }
  driver->release_session_mutex();
}

bool Statement::check_left_join(table_link *table) {
  bool ret = false;
  join_node *join = table->join->upper;
  if (join && join->join_bit & JT_LEFT && join->right == table->join) {
    ret = true;
  }
  return ret;
}

void Statement::replace_view_select_field_alias(string &view_select_sql) {
  Statement *new_stmt = NULL;
  Parser *parser = Driver::get_driver()->get_parser();
  try {
    new_stmt = parser->parse(view_select_sql.c_str(), stmt_allow_dot_in_ident,
                             true, NULL, NULL, NULL, ctype);
    field_item *column_it_in_select_sql = new_stmt->st.scanner->field_list_head;
    name_item *column_it_in_view_sql =
        this->st.sql->select_oper->create_view_oper->column_list;
    int size_change = 0;
    if (column_it_in_view_sql) {
      while (column_it_in_select_sql) {
        string::iterator replace_begin =
            view_select_sql.begin() +
            column_it_in_select_sql->alias_item->start_pos + size_change - 1;
        string::iterator replace_end =
            view_select_sql.begin() +
            column_it_in_select_sql->alias_item->end_pos + size_change;
        string replace_to = "`" + string(column_it_in_view_sql->name) + "`";
        size_change = replace_to.size() -
                      (column_it_in_select_sql->alias_item->end_pos -
                       column_it_in_select_sql->alias_item->start_pos + 1);
        view_select_sql.replace(replace_begin, replace_end, replace_to);
        column_it_in_view_sql = column_it_in_view_sql->next;
        column_it_in_select_sql = column_it_in_select_sql->next;
      }
    }
  } catch (exception &e) {
    string mes = "Replacing view select field alias error: ";
    mes.append(e.what());
    LOG_ERROR("%s", mes.c_str());
    new_stmt->free_resource();
    delete new_stmt;
    new_stmt = NULL;
    throw Error(mes.c_str());
  }
  new_stmt->free_resource();
  delete new_stmt;
  new_stmt = NULL;
}

bool Statement::check_left_join_with_cannot_merge_right_par(record_scan *rs) {
  Backend *backend = Backend::instance();
  join_node *join_tables = rs->join_tables;
  bool has_cannot_merge_right_par = false;
  while (join_tables) {
    join_node *table = join_tables->right;
    if (!table) table = join_tables;
    if (table->type == JOIN_NODE_SINGLE) {
      const char *schema_name =
          table->schema_name ? table->schema_name : schema;
      const char *table_name = table->table_name;
      DataSpace *space =
          backend->get_data_space_for_table(schema_name, table_name);
      if (!space->get_data_source()) {
        if (table == join_tables) {
          has_cannot_merge_right_par = false;
        } else {
          if (join_tables->join_bit & JT_LEFT)
            has_cannot_merge_right_par = true;
          else
            has_cannot_merge_right_par = false;
        }
      }
    }
    join_tables = join_tables->left;
  }
  return has_cannot_merge_right_par;
}

bool Statement::check_left_join_sub_left(record_scan *rs) {
  bool ret = false;
  join_node *join_tables = rs->join_tables;
  if (join_tables && join_tables->join_bit & JT_LEFT &&
      join_tables->left->type == JOIN_NODE_SUBSELECT &&
      join_tables->right->type != JOIN_NODE_JOIN && join_tables->condition &&
      join_tables->condition->type == JOIN_COND_ON && !join_tables->upper) {
    ret = true;
  }
  return ret;
}

bool Statement::check_mul_table_left_join_sub(record_scan *rs) {
  join_node *join_tables = rs->join_tables;
  if (join_tables && join_tables->left && join_tables->left->left &&
      join_tables->join_bit & JT_LEFT)
    return true;

  while (join_tables && join_tables->left) {
    join_tables = join_tables->left;
    if (join_tables->join_bit & JT_LEFT) return true;
  }
  return false;
}

bool Statement::sep_node_can_execute_parallel(ExecutePlan *plan) {
  /*Currently, there are following restrictions for separated node parallel
   * execution:
   *
   * 1. not support in transaction
   *
   * 2. not support in execution prepare
   *
   * 3. not support user var or session var
   *
   * 4. must be select stmt
   *
   * 5. not support found_rows*/
  Session *s = plan->session;
  if (s->is_in_transaction() || s->is_executing_prepare() ||
      s->get_var_flag() || select_uservar_flag)
    return false;
  if (st.type == STMT_SELECT) {
    if (st.sql->select_oper->options & SQL_OPT_SQL_CALC_FOUND_ROWS)
      return false;
  } else
    return false;

  return true;
}

void Statement::init_parallel_separated_node_execution() {
#ifdef DEBUG
  ACE_ASSERT(work_pointer == 0);
  ACE_ASSERT(fin_size == 0);
  ACE_ASSERT(running_threads == 0);
  ACE_ASSERT(work_vec.size() == 0);
  ACE_ASSERT(fin_vec.size() == 0);
#endif
  work_pointer = 0;
  fin_size = 0;
  running_threads = 0;
}

void Statement::clean_up_parallel_separated_node_execution() {
  size_t i = 0;
  for (; i != work_pointer; ++i) {
    /*work_pointer can used to indicate how many thread_task has been started,
      all this task will be wait for thread exit.

      Here should use work_pointer and work_vec, rather than fin_vec, in case of
      the exception situation, which there may be unfinished task.
      */
    work_vec[i]->wait_for_cond();  // ensure there is no running thread using
                                   // the nodes inside fin_vec
  }

  work_vec.clear();
  fin_vec.clear();
  work_pointer = 0;
  fin_size = 0;
  running_threads = 0;
}

void Statement::loop_and_find_executable_sep_nodes() {
  vector<SeparatedExecNode *>::iterator it = exec_nodes.begin();
  for (; it + 1 != exec_nodes.end(); ++it) {
    SeparatedExecNode *tmp = *it;
#ifdef DEBUG
    ACE_ASSERT(tmp->get_sen_state() != SEN_STATE_NON);
#endif
    if (!tmp->get_is_in_work_vec()) {  // this node has not yet be put into
                                       // work_vec
      /*here we use is_in_work_vec rather than sen_state==SEN_STATE_NON to
       * check the node is not in work_vec. Cause the sen_state may be changed
       * by the worker thread, so it may be un-safe for read.*/
#ifdef DEBUG
      ACE_ASSERT(tmp->get_sen_state() == SEN_STATE_INIT);
#endif
      if (tmp->has_no_un_executed_dependent_node()) {  // this node can be put
                                                       // into work_vec now
        tmp->set_sen_state(SEN_STATE_CAN_EXEC);
        tmp->set_is_in_work_vec(true);
        work_vec.push_back(tmp);
      }
    }
  }
}

/*If all separated execution node, except the last one, has been assigned,
 * return true, otherwise return false.*/
bool Statement::assign_threads_for_work_vec() {
  sep_node_mutex.acquire();
  while (work_pointer < work_vec.size() && running_threads < stmt_max_threads) {
    Backend *backend = Backend::instance();
    BackendThread *bthread =
        backend->get_backend_thread_pool()->get_one_from_free();
    if (!bthread) {
      // TODO: handle this exception
    }
    bthread->set_task(work_vec[work_pointer]);
    bthread->wakeup_handler_thread();
    ++work_pointer;
    ++running_threads;
  }

  if (work_pointer == (exec_nodes.size() - 1)) {
#ifdef DEBUG
    ACE_ASSERT(work_pointer == work_vec.size());
#endif
    LOG_DEBUG(
        "Coordinator thread has finished the separated node parallel"
        " execution assignment with work_pointer %d, exec_nodes.size %d.\n",
        work_pointer, work_vec.size());
    sep_node_mutex.release();
    return true;
  }

  sep_node_mutex.release();
  return false;
}

void Statement::report_finish_separated_node_exec(SeparatedExecNode *node,
                                                  bool get_exception) {
  if (!get_exception) {
    /*If got exception, we should not set the node state to fin, otherwise the
     * other separated node which dependt this node may be executed
     * unexpectedly.*/
    node->set_sen_state(SEN_STATE_FIN_EXEC);
  }
  sep_node_mutex.acquire();
  fin_vec.push_back(node);
  running_threads--;
  sep_node_cond.signal();
  sep_node_mutex.release();
  LOG_DEBUG("Node %@ finish the separated node exec by backend thread.\n",
            node);
}

void Statement::check_fin_tasks() {
  size_t new_fin_task_num = fin_vec.size() - fin_size;
  size_t pos_max = fin_vec.size();
  size_t i = fin_size;
  SeparatedExecNode *node = NULL;
  LOG_DEBUG("new fin task num is %d i %d pos_max %d.\n", new_fin_task_num, i,
            pos_max);
  for (; i < pos_max; ++i) {
    node = fin_vec[i];
    node->reproduce_exception();
  }
}

bool Statement::wait_threads_for_execution() {
  sep_node_mutex.acquire();

  LOG_DEBUG("Start to wait for node execution.\n");
  while (fin_size == fin_vec.size()) {
    sep_node_cond.wait();
  }
  LOG_DEBUG("Finish to wait for node execution.\n");

  try {
    check_fin_tasks();
  } catch (...) {
    sep_node_mutex.release();
    throw;
  }

  fin_size = fin_vec.size();
  if (fin_size == (exec_nodes.size() - 1)) {
    sep_node_mutex.release();
    return true;
  }
  sep_node_mutex.release();

  return false;
}

bool Statement::is_separated_exec_result_as_sub_query(record_scan *rs) {
  bool ret = false;
  if (rs->subquerytype == SUB_SELECT_DEPENDENT ||
      rs->subquerytype == SUB_SELECT_EXISTS ||
      rs->subquerytype == SUB_SELECT_COLUMN ||
      rs->subquerytype == SUB_SELECT_UNION)
    ret = true;
  return ret;
}

TableStruct *Statement::check_column_belongs_to(
    const char *column_name,
    vector<vector<TableStruct> > *table_vector_vector) {
  TableStruct *ret = NULL;
  int table_nums = 0;

  vector<vector<TableStruct> >::iterator table_vector_vector_iter;
  for (table_vector_vector_iter = table_vector_vector->begin();
       table_vector_vector_iter != table_vector_vector->end();
       ++table_vector_vector_iter) {
    vector<TableStruct>::iterator table_vector_iter;
    for (table_vector_iter = table_vector_vector_iter->begin();
         table_vector_iter != table_vector_vector_iter->end();
         ++table_vector_iter) {
      string schema_name = table_vector_iter->schema_name;
      string table_name = table_vector_iter->table_name;

      TableInfoCollection *tic = TableInfoCollection::instance();
      TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);
      map<string, TableColumnInfo *, strcasecomp> *column_info_map;
      try {
        column_info_map =
            ti->element_table_column
                ->get_element_map_columnname_table_column_info(stmt_session);
      } catch (...) {
        LOG_ERROR(
            "Error occured when try to get table info column "
            "info(map_columnname_table_column_info) of table [%s.%s]\n",
            schema_name.c_str(), table_name.c_str());
        ti->release_table_info_lock();
        throw;
      }

      string tmp_colname(column_name);
      boost::to_lower(tmp_colname);
      if (column_info_map != NULL) {
        if (column_info_map->count(tmp_colname)) {
          ++table_nums;
          ret = &(*table_vector_iter);
        }
      }
      ti->release_table_info_lock();
    }
  }
  if (table_nums == 0) {
    string error_message;
    error_message.append("No column in the tables named: ");
    error_message.append(column_name);
    throw Error(error_message.c_str());

  } else if (table_nums == 1) {
    ;
  } else {
    string error_message;
    error_message.append(
        "You should add the table name to the column name since more than one "
        "table contains ");
    error_message.append(column_name);
    throw Error(error_message.c_str());
  }

  return ret;
}
bool Statement::table_link_same_dataspace(table_link *modify_table,
                                          table_link *par_table) {
  Backend *backend = Backend::instance();
  const char *modify_schema = modify_table->join->schema_name
                                  ? modify_table->join->schema_name
                                  : schema;
  const char *par_schema =
      par_table->join->schema_name ? par_table->join->schema_name : schema;
  DataSpace *modify_dataspace = backend->get_data_space_for_table(
      modify_schema, modify_table->join->table_name);
  DataSpace *par_dataspace = backend->get_data_space_for_table(
      par_schema, par_table->join->table_name);

  return modify_dataspace == par_dataspace;
}
bool Statement::is_can_swap_type() {
  stmt_type type = st.type;
  if (type == STMT_CREATE_TB || type == STMT_ALTER_TABLE ||
      type == STMT_CREATE_VIEW || type == STMT_DROP_VIEW ||
      type > DBSCALE_ADMIN_COMMAND_START) {
    return false;
  }
  return true;
}

Statement *Statement::get_first_union_stmt() {
  record_scan *top_record_scan = st.scanner;
  string executed_sql(sql);
  if (!first_union_stmt) {
    string left_sql =
        executed_sql.substr(top_record_scan->union_select_left->start_pos - 1,
                            top_record_scan->union_select_left->end_pos);
    Statement *new_stmt = NULL;
    Parser *parser = Driver::get_driver()->get_parser();
    new_stmt = parser->parse(left_sql.c_str(), stmt_allow_dot_in_ident, true,
                             NULL, NULL, NULL, ctype);
    union_all_stmts.push_back(new_stmt);
    new_stmt->check_and_refuse_partial_parse();
    new_stmt->set_union_all_sub(true);
    new_stmt->set_default_schema(schema);
    first_union_stmt = new_stmt;
  }
  return first_union_stmt;
}

void Statement::set_full_table_name(const char *schema_name,
                                    const char *table_name) {
  splice_full_table_name(schema_name, table_name, full_table_name);
}

void Statement::replace_grant_hosts() {
  user_clause_item_list *user_clause_list = NULL;
  if (st.type == STMT_CREATE_USER || st.type == STMT_ALTER_USER) {
    if (st.sql->create_user_oper != NULL)
      user_clause_list = st.sql->create_user_oper->user_clause;
  } else if (st.type == STMT_DROP_USER) {
    if (st.sql->drop_user_oper != NULL)
      user_clause_list = st.sql->drop_user_oper->user_clause;
  } else if (st.type == STMT_REVOKE) {
    if (st.sql->revoke_oper != NULL)
      user_clause_list = st.sql->revoke_oper->user_clause;
  } else if (st.type == STMT_GRANT) {
    if (st.sql->grant_oper != NULL)
      user_clause_list = st.sql->grant_oper->user_clause;
  } else {
    if (st.sql->show_grants_oper != NULL)
      user_clause_list = st.sql->show_grants_oper->user_clause;
  }
  if (!user_clause_list) return;
  user_clause_item_list *tmp = user_clause_list;
  while (tmp) {
    const char *com_str = tmp->user_name;
    if (!strcmp(dbscale_internal_user, com_str) ||
        !strcmp(dbscale_read_only_user, com_str)) {
      LOG_ERROR(
          "Not support account management operation for dbscale internal "
          "user or dbscale read only user.\n");
      throw NotSupportedError(
          "Not support account management operation for dbscale internal "
          "user or dbscale read only user.");
    }
    tmp = tmp->next;
  }
  string ori_sql(sql);
  LOG_DEBUG("Orig sql before replace hosts is [%s].\n", sql);
  if (st.type == STMT_GRANT && st.sql->grant_oper &&
      st.sql->grant_oper->comment) {
    ori_sql = string(ori_sql, 0, st.sql->grant_oper->comment_start_pos - 1);
    LOG_DEBUG(
        "remove comment info in Grant statement, the sql to handle now is "
        "[%s].\n",
        ori_sql.c_str());
  }
  if (st.type == STMT_ALTER_USER && st.sql->create_user_oper &&
      st.sql->create_user_oper->ssl_option) {
    unsigned int start_pos = st.sql->create_user_oper->require_start_pos;
    unsigned int end_pos = st.sql->create_user_oper->require_end_pos;
    ori_sql.replace(start_pos - 1, end_pos - start_pos + 1, "REQUIRE NONE");
  }
  if (st.type == STMT_CREATE_USER && st.sql->create_user_oper &&
      st.sql->create_user_oper->ssl_option) {
    unsigned int start_pos = st.sql->create_user_oper->require_start_pos;
    unsigned int end_pos = st.sql->create_user_oper->require_end_pos;
    ori_sql.replace(start_pos - 1, end_pos - start_pos + 1, "REQUIRE NONE");
  }
  vector<string>::iterator host_it = dbscale_hosts_vec.begin();
  for (; host_it != dbscale_hosts_vec.end(); ++host_it) {
    string grant_sql;
    user_clause_item_list *user_clause = user_clause_list;
    unsigned int head_pos = user_clause->host_head_pos;
    unsigned int end_pos = user_clause->host_end_pos;
    if (head_pos == end_pos)
      grant_sql.assign(ori_sql, 0, head_pos);
    else
      grant_sql.assign(ori_sql, 0, head_pos - 1);
    while (user_clause) {
      grant_user_name_vec.push_back(user_clause->user_name);
      grant_sql.append("@'");
      grant_sql.append(*host_it);
      grant_sql.append("'");
      user_clause = user_clause->next;
      if (user_clause) {
        head_pos = user_clause->host_head_pos;
        unsigned int last_end_pos = end_pos;
        end_pos = user_clause->host_end_pos;
        if (head_pos == end_pos)
          grant_sql.append(ori_sql, last_end_pos, head_pos - last_end_pos);
        else
          grant_sql.append(ori_sql, last_end_pos, head_pos - last_end_pos - 1);
      }
    }
    if (end_pos < ori_sql.length())
      grant_sql.append(ori_sql, end_pos, ori_sql.length() - end_pos);
    LOG_DEBUG("Query sql after replace hosts is [%s].\n", grant_sql.c_str());
    grant_sqls.push_back(grant_sql);
  }
  sql = grant_sqls.front().c_str();
  Statement *new_stmt = NULL;
  re_parser_stmt(&(st.scanner), &new_stmt, sql);
}

void Statement::replace_set_pass_host() {
  string ori_sql(sql);
  LOG_DEBUG("Orig sql before replace hosts is [%s].\n", sql);
  set_op_node *set_oper = st.sql->set_oper;
  unsigned int head_pos = set_oper->host_head_pos;
  unsigned int end_pos = set_oper->host_end_pos;
  if (head_pos != 0) {
    vector<string>::iterator host_it = dbscale_hosts_vec.begin();
    for (; host_it != dbscale_hosts_vec.end(); ++host_it) {
      string set_sql;
      if (head_pos == end_pos)
        set_sql.assign(ori_sql, 0, head_pos);
      else
        set_sql.assign(ori_sql, 0, head_pos - 1);
      set_sql.append("@'");
      set_sql.append(*host_it);
      set_sql.append("'");
      set_sql.append(ori_sql, end_pos, ori_sql.length() - end_pos);
      LOG_DEBUG("Query sql after replace hosts is [%s].\n", set_sql.c_str());
      grant_sqls.push_back(set_sql);
    }
    sql = grant_sqls.front().c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
  }
}

void Statement::get_execution_plan_for_max_count_certain(ExecutePlan *plan) {
  AggregateDesc desc;
  unsigned int column_index = 0;
  list<AggregateDesc> aggr_list;
  field_item *field = st.scanner->field_list_head;
  while (field) {
    ++column_index;
    field = field->next;
  }
  bool need_handle_one_more_max = false;
  if (plan->statement->get_stmt_node()->execute_max_count_max_certain)
    need_handle_one_more_max = true;
  if (column_index < 2) {
    throw Error("maxcountcertain should have at least two fields");
  } else if (column_index < 3 && need_handle_one_more_max) {
    throw Error(
        "maxcountcertain with last max should have at least three fields");
  }
  if (!need_handle_one_more_max) {
    desc.column_index = column_index - 2;
    desc.type = AGGREGATE_TYPE_MAX;
    aggr_list.push_back(desc);
    desc.column_index = column_index - 1;
    desc.type = AGGREGATE_TYPE_COUNT;
    aggr_list.push_back(desc);
  } else {
    desc.column_index = column_index - 3;
    desc.type = AGGREGATE_TYPE_MAX;
    aggr_list.push_back(desc);
    desc.column_index = column_index - 2;
    desc.type = AGGREGATE_TYPE_COUNT;
    aggr_list.push_back(desc);
    desc.column_index = column_index - 1;
    desc.type = AGGREGATE_TYPE_MAX;
    aggr_list.push_back(desc);
  }
  if (!st.scanner->children_begin) {
    throw Error("maxcountcertain should have one child record_scan");
  }
  list<SortDesc> group_description;
  int group_column_num = 0;
  record_scan *child_rs = st.scanner->children_begin;
  order_item *group_by_list = child_rs->group_by_list;
  if (group_by_list) {
    do {
      SortDesc sort_desc;
      group_column_num =
          get_pos_from_field_list(group_by_list->field->field_expr, st.scanner);
      sort_desc.column_index = group_column_num;
      sort_desc.sort_order = group_by_list->order;
      string collation_name =
          get_column_collation(group_by_list->field->field_expr, st.scanner);
      set_charset_and_isci_accroding_to_collation(
          collation_name, sort_desc.ctype, sort_desc.is_cs);

      group_description.push_back(sort_desc);
      group_by_list = group_by_list->next;
    } while (group_by_list != child_rs->group_by_list);
  }

  ExecuteNode *group_by_node =
      plan->get_group_node(&group_description, aggr_list);

  Backend *backend = Backend::instance();
  table_link *tl = st.scanner->first_table;
  const char *schema_name =
      tl->join->schema_name ? tl->join->schema_name : schema;
  const char *table_name = tl->join->table_name;
  DataSpace *ds = backend->get_data_space_for_table(schema_name, table_name);
  if (!ds->is_partitioned()) {
    LOG_ERROR(
        "Table %s.%s should be partition table for using maxcountcertain.\n",
        schema_name, table_name);
    throw Error("maxcountcertain can only used in partition table");
  }

  ExecuteNode *send_node = plan->get_send_node();
  PartitionedTable *partition_table = (PartitionedTable *)ds;
  unsigned int partition_num = partition_table->get_real_partition_num();
  for (unsigned int i = 0; i < partition_num; ++i) {
    DataSpace *space = partition_table->get_partition(i);
    const char *used_sql = adjust_stmt_sql_for_shard(space, sql);
    ExecuteNode *fetch_node = plan->get_fetch_node(space, used_sql);
    group_by_node->add_child(fetch_node);
  }
  send_node->add_child(group_by_node);
  plan->set_start_node(send_node);
}

void Statement::init_spark_schema(DataSpace *ds) {
  Backend *backend = Backend::instance();
  Connection *conn = NULL;
  if (!backend->get_inited_spark_schema()) {
    try {
      string schema_name;
      string query_sql(
          "select schema_name from information_schema.SCHEMATA where "
          "schema_name='");
      query_sql.append(spark_dst_schema);
      query_sql.append("'");
      conn = ds->get_connection(get_session());
      if (!conn)
        throw Error("Fail to get conn for Statement::init_spark_schema");
      conn->query_for_one_value(query_sql.c_str(), schema_name, 0);
      conn->get_pool()->add_back_to_free(conn);
      conn = NULL;
      if (schema_name.size() == 0) {
        string sql = "CREATE DATABASE IF NOT EXISTS ";
        sql.append(spark_dst_schema);
        ds->execute_one_modify_sql(sql.c_str());
      }
    } catch (...) {
      LOG_ERROR("Fail to initialize dbscale_spark_tmp databases.\n");
      if (conn) conn->get_pool()->add_back_to_dead(conn);
      throw;
    }
    backend->set_inited_spark_schema();
  }
}
string Statement::get_spark_dst_jdbc_url(DataSpace *ds) {
  string dst_url("jdbc:mysql://");
  if (ds->is_normal()) {
    DataSource *data_source = ds->get_data_source();
    if (data_source) {
      DataServer *data_server = data_source->get_master_server();
      if (data_server) {
        string host_ip = data_server->get_host_ip();
        unsigned int port = data_server->get_port();
        dst_url.append(host_ip);
        dst_url.append(":");
        dst_url.append(SSTR(port));
      }
    }
  }
  return dst_url;
}

DataSpace *Statement::get_subquery_table_dataspace(DataSpace *local_space,
                                                   const char *key_name,
                                                   bool is_dup) {
  Backend *backend = Backend::instance();
  Schema *join_schema = backend->find_schema(TMP_TABLE_SCHEMA);
  Table *tab = NULL;

  local_space->acquire_tmp_table_read_mutex();
  string table_name = local_space->get_tmp_table_name(key_name);
  local_space->release_tmp_table_mutex();
  if (table_name.empty()) {
    local_space->acquire_tmp_table_write_mutex();
    table_name = local_space->get_tmp_table_name(key_name);
    if (table_name.empty()) {
      string key_str;
      if (key_name)
        key_str.assign(key_name);
      else
        key_str.assign("");
      JoinTableInfo table_info = {key_str, "", "", "", ""};
      try {
        tab = (Table *)(local_space->init_join_table_space(
            table_name, &table_info, is_dup, true));
      } catch (...) {
        LOG_ERROR("Got error when init table sub query tmp table dataspace.\n");
        local_space->release_tmp_table_mutex();
        throw;
      }
      backend->add_join_table_spaces(local_space, (DataSpace *)tab);
      local_space->set_tmp_table_name(key_name, table_name);
    }
    local_space->release_tmp_table_mutex();
  }

  // The tmp table dataspace may not exists.
  if (!tab) tab = join_schema->get_table(table_name.c_str());

  return (DataSpace *)tab;
}

bool Statement::is_spark_one_partition_sql() {
  if (st.type == STMT_SELECT && st.execute_on_partition &&
      st.exe_comment_partition_id && st.exe_comment_partition_num &&
      st.exe_comment_schema && st.exe_comment_table) {
    return true;
  }
  return false;
}

bool Statement::handle_executable_comments_after_cnj(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  if (is_spark_one_partition_sql()) {
    int id = atoi(st.exe_comment_partition_id);
    int num = atoi(st.exe_comment_partition_num);
    DataSpace *ds = backend->get_data_space_for_table(st.exe_comment_schema,
                                                      st.exe_comment_table);
    if (ds && ds->is_partitioned()) {
      PartitionedTable *par_ds = (PartitionedTable *)ds;
      int partition_num = par_ds->get_real_partition_num();
      if (partition_num < num) construct_sql_function(par_ds, num, id);
      re_parser_stmt(&(st.scanner), &(plan->statement), sql);
      Partition *partition = par_ds->get_partition(id % partition_num);
      if (partition) {
        assemble_direct_exec_plan(plan, partition);
        return true;
      }
    } else {
      LOG_ERROR("Execution on partition, should use partition table.\n");
      throw Error("Execution on partition, should use partition table.\n");
    }
  }
  return false;
}

/* the bool return value is to indicate weather the generate execution plan
 * should finished. */
bool Statement::handle_executable_comments_before_cnj(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  if (st.execute_max_count_certain) {
    get_execution_plan_for_max_count_certain(plan);
    return true;
  }

  if (st.execute_on_datasource) {
    table_link *table = st.table_list_head;
    const char *schema_name = NULL;
    const char *table_name = NULL;
    while (table) {
      schema_name = table->join->schema_name;
      table_name = table->join->table_name;
      if (table_name && !schema_name) {
        throw Error(
            "execute on datasource directly should specify the database name "
            "in statement");
      }
      table = table->next;
    }
    DataSource *datasource =
        backend->find_data_source(st.execute_on_datasource);
    if (!datasource) {
      throw Error("execute on datasource directly but datasource not exists");
    }
    DataSpace *space = datasource->get_one_space();
    if (!space) {
      throw Error(
          "execute on datasource directly but datasource has no related "
          "dataspace");
    }
    assemble_direct_exec_plan(plan, space);
    return true;
  }

  if (st.execute_on_auth_master)
    if (st.type == STMT_ALTER_TABLE || st.type == STMT_CREATE_FUNCTION ||
        st.type == STMT_DROP_FUNCTION || st.type == STMT_CREATE_PROCEDURE ||
        st.type == STMT_DROP_PROC || st.type == STMT_CREATE_TB ||
        st.type == STMT_DROP_DB || st.type == STMT_CREATE_SELECT ||
        st.type == STMT_CREATE_LIKE || st.type == STMT_DROP_TB ||
        st.type == STMT_CREATE_DB || st.type == STMT_ALTER_DB ||
        st.type == STMT_RENAME_TABLE) {
      DataSpace *dataspace = backend->get_auth_data_space();
      assemble_direct_exec_plan(plan, dataspace);
      return true;
    }

  if (st.type == STMT_SELECT && st.dbscale_group) {
    generate_dbscale_wise_group_plan(plan);
    return true;
  }
  if (st.type == STMT_SELECT && st.dbscale_pages) {
    generate_dbscale_pages_plan(plan);
    return true;
  }
  return false;
}

void Statement::construct_sql_function(PartitionedTable *par_ds,
                                       unsigned int partition_num,
                                       unsigned int partition_id) {
  string constructed_sql;
  vector<const char *> *key_vector = par_ds->get_key_names();

  PartitionMethod *part_method = par_ds->get_partition_method();
  string partition_sql = part_method->get_partition_sql(
      key_vector->at(0), partition_num, partition_id);
  record_scan *rs = st.scanner;

  if (rs->opt_where_start_pos && rs->opt_where_end_pos &&
      rs->opt_where_start_pos != rs->opt_where_end_pos) {
    unsigned int after_where_end_pos = 0;
    if (strlen(sql) > rs->opt_where_end_pos + 1)
      after_where_end_pos = rs->opt_where_end_pos + 1;
    constructed_sql.append(sql, rs->start_pos - 1,
                           rs->opt_where_start_pos - rs->start_pos);
    constructed_sql.append(sql, rs->opt_where_start_pos - 1, 6);
    constructed_sql.append(" (");
    constructed_sql.append(
        sql, rs->opt_where_start_pos - 1 + 6,
        rs->opt_where_end_pos - rs->opt_where_start_pos + 1 - 6);
    constructed_sql.append(") AND ");
    constructed_sql.append(partition_sql);
    if (after_where_end_pos)
      constructed_sql.append(sql, after_where_end_pos,
                             rs->end_pos - after_where_end_pos + 1);
  } else {
    unsigned int where_add_pos;
    if (rs->group_start_pos)
      where_add_pos = rs->group_start_pos - 1;
    else if (rs->having_pos)
      where_add_pos = rs->having_pos - 1;
    else if (rs->order_pos)
      where_add_pos = rs->order_pos;
    else if (rs->limit_pos)
      where_add_pos = rs->limit_pos;
    else
      where_add_pos = rs->end_pos;
    constructed_sql.append(sql, rs->start_pos - 1,
                           where_add_pos - rs->start_pos + 1);
    constructed_sql.append(" WHERE ");
    constructed_sql.append(partition_sql);
    constructed_sql.append(sql, where_add_pos, where_add_pos - rs->end_pos);
  }

  LOG_DEBUG("The constructed sql for spark partition is [%s]\n",
            constructed_sql.c_str());
  sql_tmp = constructed_sql;
  sql = sql_tmp.c_str();
}

void Statement::generate_dbscale_pages_plan(ExecutePlan *plan) {
  int page_size = 0;
  if (st.page_size) page_size = atoi(st.page_size);
  if (page_size <= 1) {
    LOG_ERROR("DBScale page size should bigger than 1.\n");
    throw Error("DBScale page size should bigger than 1.");
  }
  if (st.scanner->is_contain_star) {
    LOG_ERROR("Not support '*' in DBScale page size.\n");
    throw Error("Not support '*' in DBScale page size.");
  }
  if (!st.scanner->order_by_list) {
    LOG_ERROR("DBScale page size should have one order by field.\n");
    throw Error("DBScale page size should have one order by field.");
  }
  string new_sql;
  list<AggregateDesc> aggr_list;
  map<int, pair<int, string> > replace_items;
  get_dbscale_page_items(replace_items, aggr_list);

  generate_new_sql(replace_items, new_sql);
  assemble_dbscale_pages_plan(plan, aggr_list, new_sql, page_size);
}

void Statement::assemble_dbscale_pages_plan(ExecutePlan *plan,
                                            list<AggregateDesc> &aggr_list,
                                            string new_sql, int page_size) {
  record_scan *new_rs;
  Statement *new_stmt = NULL;
  re_parser_stmt(&new_rs, &new_stmt, new_sql.c_str());
  add_order_item_list(new_rs->order_by_list, order_by_list, new_rs);

  Backend *backend = Backend::instance();
  list<ExecuteNode *> nodes;
  table_link *table = st.table_list_head;
  const char *schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  const char *table_name = table->join->table_name;
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->is_partitioned()) {
    PartitionedTable *par_table = (PartitionedTable *)space;
    unsigned int partition_num = par_table->get_real_partition_num();
    for (unsigned int i = 0; i < partition_num; ++i) {
      DataSpace *ds = par_table->get_partition(i);
      const char *used_sql = adjust_stmt_sql_for_shard(ds, new_sql.c_str());
      ExecuteNode *fetch_node = plan->get_fetch_node(ds, used_sql);
      nodes.push_back(fetch_node);
    }
  } else if (space->is_normal()) {
    ExecuteNode *fetch_node = plan->get_fetch_node(space, new_sql.c_str());
    nodes.push_back(fetch_node);
  } else {
    LOG_ERROR("Not support space type.\n");
    throw Error("Not suppport space type.");
  }

  ExecuteNode *send_node = plan->get_send_node();
  ExecuteNode *page_node =
      plan->get_dbscale_pages_node(&order_by_list, aggr_list, page_size);
  ExecuteNode *sort_node = plan->get_sort_node(&order_by_list);
  list<ExecuteNode *>::iterator it = nodes.begin();
  for (; it != nodes.end(); ++it) {
    sort_node->add_child(*it);
  }
  page_node->add_child(sort_node);
  send_node->add_child(page_node);
  plan->set_start_node(send_node);
}

void Statement::generate_dbscale_wise_group_plan(ExecutePlan *plan) {
  if (!st.dbscale_group_order || !st.dbscale_group_item) {
    LOG_ERROR("Fail to get dbscale wise group order list or aggr items.\n");
    throw Error("Fail to get dbscale wise group order list or aggr items.");
  }
  if (st.scanner->children_begin) {
    LOG_ERROR("Not support subquery in dbscale wise group.\n");
    throw Error("Not support subquery in dbscale wise group.");
  }
  if (!st.table_list_head || st.table_list_head->next) {
    LOG_ERROR("Only support one table in dbscale wise group.\n");
    throw Error("Only support one table in dbscale wise group.");
  }
  if (st.scanner->is_contain_star) {
    LOG_ERROR("Not support '*' in dbscale wise group.\n");
    throw Error("Not support '*' in dbscale wise group");
  }
  if (st.scanner->order_by_list || !st.scanner->group_by_list ||
      st.scanner->is_with_rollup) {
    LOG_ERROR(
        "DBScale wise group should have no order by field and have group by "
        "field without rollup.\n");
    throw Error(
        "DBScale wise group should have no order by field and have group by "
        "field without rollup.");
  }
  string new_sql;
  list<AggregateDesc> aggr_list;
  map<int, pair<int, string> > replace_items;
  set<string> item_alias;
  get_dbscale_group_items(replace_items, item_alias, aggr_list);

  group_aggr_item *aggr_item = st.dbscale_group_item->next;
  do {
    if (!item_alias.count(string(aggr_item->aggr_field))) {
      LOG_DEBUG("DBScale wise group items not in select list.\n");
      throw Error("DBScale wise group items not in select list");
    }
    aggr_item = aggr_item->next;
  } while (aggr_item != st.dbscale_group_item->next);

  generate_new_sql(replace_items, new_sql);
  assemble_dbscale_wise_group_plan(plan, aggr_list, new_sql);
}

void Statement::assemble_dbscale_wise_group_plan(ExecutePlan *plan,
                                                 list<AggregateDesc> &aggr_list,
                                                 string new_sql) {
  add_group_item_list(st.scanner->group_by_list, group_by_list, st.scanner);
  record_scan *new_rs;
  Statement *new_stmt = NULL;
  re_parser_stmt(&new_rs, &new_stmt, new_sql.c_str());
  add_order_item_list(new_rs->order_by_list, order_by_list, new_rs);

  Backend *backend = Backend::instance();
  list<ExecuteNode *> nodes;
  table_link *table = st.table_list_head;
  const char *schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  const char *table_name = table->join->table_name;
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->is_partitioned()) {
    PartitionedTable *par_table = (PartitionedTable *)space;
    unsigned int partition_num = par_table->get_real_partition_num();
    for (unsigned int i = 0; i < partition_num; ++i) {
      DataSpace *ds = par_table->get_partition(i);
      const char *used_sql = adjust_stmt_sql_for_shard(ds, new_sql.c_str());
      ExecuteNode *fetch_node = plan->get_fetch_node(ds, used_sql);
      nodes.push_back(fetch_node);
    }
  } else if (space->is_normal()) {
    ExecuteNode *fetch_node = plan->get_fetch_node(space, new_sql.c_str());
    nodes.push_back(fetch_node);
  } else {
#if DEBUG
    ACE_ASSERT(0);  // Should not be here
#endif
  }

  ExecuteNode *send_node = plan->get_send_node();
  ExecuteNode *group_node =
      plan->get_dbscale_wise_group_node(&group_by_list, aggr_list);
  ExecuteNode *sort_node = plan->get_sort_node(&order_by_list);
  list<ExecuteNode *>::iterator it = nodes.begin();
  for (; it != nodes.end(); it++) {
    sort_node->add_child(*it);
  }
  group_node->add_child(sort_node);
  send_node->add_child(group_node);
  plan->set_start_node(send_node);
}

void Statement::get_dbscale_page_items(
    map<int, pair<int, string> > &replace_items,
    list<AggregateDesc> &aggr_list) {
  AggregateDesc desc;
  CurrentStatementFunctionType type;
  int column_index = 0;
  field_item *field = st.scanner->field_list_head;
  set<CurrentStatementFunctionType> agg_func;
  while (field) {
    if (field->field_expr) {
      type = field->field_expr->get_cur_func_type();
      if (type != AGGREGATE_TYPE_NON) {
        if (type != AGGREGATE_TYPE_MAX && type != AGGREGATE_TYPE_MIN &&
            type != AGGREGATE_TYPE_COUNT) {
          LOG_ERROR("Only support MAX/MIN/COUNT for dbscale pages.\n");
          throw Error("Only support MAX/MIN/COUNT for dbscale pages.");
        }
        if (agg_func.count(type)) {
          LOG_ERROR("Only support one MAX/MIN/COUNT for dbscale pages.\n");
          throw Error("nly support one MAX/MIN/COUNT for dbscale pages.");
        } else {
          agg_func.insert(type);
        }
        string column;
        if (type == AGGREGATE_TYPE_COUNT) {
          column.assign("1");
        } else {
          string value, schema_name, table_name, column_name;
          Expression *expr =
              ((FunctionExpression *)field->field_expr)->param_list;
          expr->to_string(value);
          if (((ListExpression *)expr)->expr_list_head->expr->type ==
              EXPR_STR) {
            split_column_expr(value, schema_name, table_name, column_name);
            column.assign("`");
            if (!schema_name.empty()) {
              column.append(schema_name);
              column.append("`.`");
            }
            if (!table_name.empty()) {
              column.append(table_name);
              column.append("`.`");
            }
            column.append(column_name);
            column.append("`");
          } else {
            column.append(value);
          }
        }
        replace_items[field->field_expr->start_pos] =
            make_pair(field->field_expr->end_pos, column);
        desc.column_index = column_index;
        desc.type = type;
        aggr_list.push_back(desc);
      }
    }
    ++column_index;
    field = field->next;
  }
}

void Statement::get_dbscale_group_items(
    map<int, pair<int, string> > &replace_items, set<string> &item_alias,
    list<AggregateDesc> &aggr_list) {
  AggregateDesc desc;
  CurrentStatementFunctionType type;
  int column_index = 0;
  field_item *field = st.scanner->field_list_head;
  while (field) {
    if (field->field_expr) {
      type = field->field_expr->get_cur_func_type();
      if (type != AGGREGATE_TYPE_NON) {
        if (type != AGGREGATE_TYPE_MAX && type != AGGREGATE_TYPE_MIN &&
            type != AGGREGATE_TYPE_COUNT) {
          LOG_ERROR("Only support MAX/MIN/COUNT for dbscale wise group now.\n");
          throw Error("Only support MAX/MIN/COUNT for dbscale wise group now.");
        }
        if (!field->alias) {
          LOG_ERROR(
              "Aggregate function in dbscale wise group should has a alias.\n");
          throw Error(
              "Aggregate function in dbscale wise group should has a alias.");
        }
        string name(field->alias);
        if (item_alias.count(name)) {
          LOG_ERROR(
              "Not support the same field alias in dbscale wise group.\n");
          throw Error(
              "Not support the same field alias in dbscale wise group.");
        }
        item_alias.insert(name);
        string column;
        if (type == AGGREGATE_TYPE_COUNT) {
          column.assign("1");
        } else {
          string value, schema_name, table_name, column_name;
          Expression *expr =
              ((FunctionExpression *)field->field_expr)->param_list;
          expr->to_string(value);
          if (((ListExpression *)expr)->expr_list_head->expr->type ==
              EXPR_STR) {
            split_column_expr(value, schema_name, table_name, column_name);
            column.assign("`");
            if (!schema_name.empty()) {
              column.append(schema_name);
              column.append("`.`");
            }
            if (!table_name.empty()) {
              column.append(table_name);
              column.append("`.`");
            }
            column.append(column_name);
            column.append("`");
          } else {
            column.append(value);
          }
        }
        replace_items[field->field_expr->start_pos] =
            make_pair(field->field_expr->end_pos, column);
        desc.column_index = column_index;
        desc.type = type;
        aggr_list.push_back(desc);
      }
    }
    ++column_index;
    field = field->next;
  }

  set<string> order_alias;
  string order("ORDER BY ");
  order_item *item = st.scanner->group_by_list;
  do {
    string str;
    Expression *expr = item->field->field_expr;
    int column_index = item_in_select_fields_pos(expr, st.scanner);
    if (column_index == COLUMN_INDEX_UNDEF) {
      LOG_ERROR(
          "Select list should contain group by field in dbscale wise group.\n");
      throw Error(
          "Select list should contain group by field in dbscale wise gr