pe != STMT_CREATE_EVENT) {
      check_and_replace_view(plan->session, table);
    } else if (st.type == STMT_SHOW_INDEX) {
      dataspace = backend->get_metadata_data_space();
      if (!dataspace) {
        throw Error("fail to get metadata dataspace");
      }
      assemble_direct_exec_plan(plan, dataspace);
#ifndef DBSCALE_TEST_DISABLE
      plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
      return true;
    } else if (st.type == STMT_SHOW_CREATE_TABLE ||
               st.type == STMT_SHOW_FIELDS) {
      table_link *tb = *table;
      if (tb) {
        string full_view;
        string sub_sql;

        full_view.assign(tb->join->schema_name ? tb->join->schema_name
                                               : schema);
        full_view.append(".");
        full_view.append(tb->join->table_name);
        sub_sql.clear();
        plan->session->get_one_view(full_view, sub_sql);
        if (!sub_sql.empty()) {
          LOG_DEBUG("Find a view for show create table stmt, %s.\n",
                    full_view.c_str());
          dataspace = backend->get_metadata_data_space();
          if (!dataspace) {
            LOG_ERROR("Fail to get meta datasource.\n");
            throw Error("Fail to get meta datasource.");
          }
          assemble_direct_exec_plan(plan, dataspace);
#ifndef DBSCALE_TEST_DISABLE
          plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
          return true;
        }
      }
    }
    if (!(*table) && st.table_list_head) st.table_list_head = NULL;
  }

  return false;
}

void Statement::check_acl_and_safe_mode(ExecutePlan *plan) {
  if (enable_acl && !plan->session->get_has_check_acl() &&
      (!is_partial_parsed() || st.type != STMT_NON) &&
      (strcmp(supreme_admin_user, plan->session->get_username()) &&
       strcmp(dbscale_internal_user, plan->session->get_username())) &&
      !is_cross_node_join() && !plan->session->get_is_federated_session() &&
      !is_union_table_sub()) {
    plan->session->set_has_check_acl(
        true);  // only check the authenticate once for one statement
    authenticate_statement(plan);
  }

  if (dbscale_safe_sql_mode == 2) {
    if (st.type == STMT_UPDATE) {
      if (!is_partial_parsed() && st.sql->update_oper &&
          !st.sql->update_oper->condition)
        throw Error(
            "Refuse update stmt without condition for "
            "dbscale_safe_sql_mode=2.");
    } else if (st.type == STMT_DELETE) {
      if (!is_partial_parsed() && st.sql->delete_oper &&
          !st.sql->delete_oper->condition)
        throw Error(
            "Refuse delete stmt without condition for "
            "dbscale_safe_sql_mode=2.");
    }
  }
}

void Statement::row_count_and_last_insert_id_checking(ExecutePlan *plan) {
  int enable_last_insert_id_session =
      plan->handler->get_session()
          ->get_session_option("enable_last_insert_id")
          .int_val;
  if (st.last_insert_id_num && !enable_last_insert_id_session) {
    LOG_ERROR("Unsupport sql, SELECT LAST_INSERT_ID() has been disabled.\n");
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError("SELECT LAST_INSERT_ID() has been disabled.");
  }

  if (st.row_count_num != st.scanner->row_count_num || st.row_count_num > 1) {
    LOG_ERROR(
        "Unsupport sql, the sql is [%s], only support one"
        " row_count() in top level select.\n",
        sql);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError(
        "only support one row_count() in top level select.");
  }
  if (st.last_insert_id_num != st.scanner->last_insert_id_num ||
      st.last_insert_id_num > 1) {
    LOG_ERROR(
        "Unsupport sql, the sql is [%s], only support one"
        " last_insert_id() in top level select.\n",
        sql);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError(
        "only support one last_insert_id() in top level select.");
  }
}

DataSpace *Statement::get_dataspace_for_explain(ExecutePlan *plan) {
  table_link *link = st.table_list_head;
  join_node *node = NULL;
  bool use_table_dataspace = true;
  DataSpace *standard_space = NULL;
  Backend *backend = Backend::instance();
  while (link) {
    node = link->join;
    const char *table_name = node->table_name;
    const char *schema_name =
        node->schema_name ? node->schema_name : plan->session->get_schema();
    DataSpace *tmp_space =
        backend->get_data_space_for_table(schema_name, table_name);
    if (!(tmp_space->get_data_source())) {
      use_table_dataspace = false;
      break;
    }
    if (!standard_space) {
      standard_space = tmp_space;
    }
    if (!is_share_same_server(standard_space, tmp_space)) {
      use_table_dataspace = false;
      break;
    }
    link = link->next;
  }
  LOG_DEBUG("explain use table dataspace %d\n", use_table_dataspace);
  if (standard_space && use_table_dataspace) {
    LOG_DEBUG("explain use data space %s\n", standard_space->get_name());
  } else {
    standard_space = backend->get_metadata_data_space();
    if (!standard_space) {
      standard_space = backend->get_catalog();
    }
    LOG_DEBUG("explain use data space %s\n", standard_space->get_name());
  }
  return standard_space;
}

void Statement::generate_plan_for_explain_related_stmt(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;
  if (st.type == STMT_EXPLAIN) {
    dataspace = get_dataspace_for_explain(plan);
    assemble_direct_exec_plan(plan, dataspace);
    return;
  }
  if (st.type == STMT_DBSCALE_EXPLAIN) {
    if (!st.sql->explain_oper) {
      throw Error("no sql detected to explain");
    }
    Session *session = plan->session;
    session->set_in_explain(true);
    session->set_explain_node_sequence_id(0);
    session->reset_explain_sub_sql_list();
    session->clean_explain_element_list();

    explain_sql.clear();
    explain_sql.append(sql + st.sql->explain_oper->sql_start_pos - 1);
    LOG_DEBUG("Generate explain sql [%s]\n", explain_sql.c_str());
    Parser *parser = Driver::get_driver()->get_parser();
    Statement *explain_stmt =
        parser->parse(explain_sql.c_str(), stmt_allow_dot_in_ident, true, NULL,
                      NULL, NULL, ctype);
    explain_stmt->set_default_schema(schema);

    ExecutePlan *new_plan = NULL;
    try {
      new_plan = (ExecutePlan *)(plan->handler->get_execute_plan(explain_stmt));
      explain_stmt->generate_execution_plan(new_plan);
      if (!new_plan->start_node) {
        LOG_ERROR("generate_execution_plan for %s error\n",
                  explain_sql.c_str());
        throw Error("generate_execution_plan error");
      }
      list<ExecuteNode *> node_list;

      int seq_id = session->inc_and_get_explain_node_sequence_id();
      new_plan->start_node->set_node_id(seq_id);
      new_plan->sql = explain_stmt->get_sql();
      node_list.push_back(new_plan->start_node);
      session->update_explain_info(node_list, 0, seq_id);
    } catch (...) {
      if (new_plan) {
        delete new_plan;
        new_plan = NULL;
      }
      if (explain_stmt) {
        explain_stmt->free_resource();
        delete explain_stmt;
        explain_stmt = NULL;
      }
      session->set_in_explain(false);
      throw;
    }

    if (new_plan) {
      delete new_plan;
      new_plan = NULL;
    }
    if (explain_stmt) {
      explain_stmt->free_resource();
      delete explain_stmt;
      explain_stmt = NULL;
    }

    session->set_in_explain(false);
    assemble_explain_plan(plan);
    return;
  }
}

bool Statement::generate_plan_for_federate_session(ExecutePlan *plan,
                                                   table_link *table) {
  if (st.type != STMT_SET) plan->session->set_has_check_first_sql(true);
  if (st.type == STMT_SHOW_TABLE_STATUS && check_federated_show_status()) {
    plan->session->set_is_federated_session(true);
    assemble_federated_table_status_node(plan);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return true;
  } else if (st.type == STMT_SELECT && check_federated_name() &&
             check_federated_select()) {
    int field_num = get_field_num();
    plan->session->set_is_federated_session(true);
    assemble_empty_set_node(plan, field_num);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return true;
  } else if (plan->session->get_is_federated_session() &&
             st.type == STMT_SELECT && check_federated_name() &&
             (MethodType)cross_node_join_method == DATA_MOVE_READ &&
             subquery_type == SUB_SELECT_NON) {
    // If it is a subquery, then it is not a query received from federated
    // table.
    assemble_federated_thread_plan(plan, table->join->table_name);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return true;
  }
  return false;
}

table_link *Statement::adjust_select_sql_for_funs_and_autocommit(
    ExecutePlan *plan, table_link *table) {
  if (st.type == STMT_SELECT) {
    plan->session->set_error_packet(NULL);
    if (st.session_var_list != NULL) {
      sql_tmp.assign(sql);
      bool has_autocommit = has_and_rebuild_autocommit_sql(plan);
      if (has_autocommit) {
        sql = sql_tmp.c_str();
        Statement *new_stmt = NULL;
        re_parser_stmt(&(st.scanner), &new_stmt, sql);
        table = new_stmt->get_stmt_node()->table_list_head;
        LOG_DEBUG("Query sql after replace AUTOCOMMIT is [%s]\n", sql);
      }
    }

    if (st.has_found_rows) {
      sql_tmp.assign(sql);
      rebuild_found_rows_sql(st.scanner, plan);
      sql = sql_tmp.c_str();
      Statement *new_stmt = NULL;
      re_parser_stmt(&(st.scanner), &new_stmt, sql);
      table = new_stmt->get_stmt_node()->table_list_head;
      LOG_DEBUG("Query sql after replace found_rows() is [%s]\n", sql);
    }

    if (st.row_count_num) {
      sql_tmp.assign(sql);
      replace_sql_function_field(FUNCTION_TYPE_ROW_COUNT, sql_tmp, st.scanner,
                                 plan);
      Statement *new_stmt = NULL;
      sql = sql_tmp.c_str();
      re_parser_stmt(&(st.scanner), &new_stmt, sql);
      table = new_stmt->get_stmt_node()->table_list_head;
      LOG_DEBUG("Query sql after replace row_count() is [%s]\n", sql);
    }

    if (st.last_insert_id_num) {
      set<DataSpace *> shard_table;
      DataSpace *tmp_dataspace = NULL;
      table_link *tmp_table = table;
      const char *schema_name = NULL, *table_name = NULL;
      while (tmp_table) {
        schema_name = tmp_table->join->schema_name
                          ? tmp_table->join->schema_name
                          : schema;
        table_name = tmp_table->join->table_name;
        if (schema_name && !strcmp(schema_name, "information_schema")) {
          tmp_table = tmp_table->next;
          continue;
        } else {
          tmp_dataspace = Backend::instance()->get_data_space_for_table(
              schema_name, table_name);
        }
        if (!tmp_dataspace->get_data_source()) {
          if (((Table *)tmp_dataspace)->is_partitioned()) {
            shard_table.insert(tmp_dataspace);
          }
        }
        tmp_table = tmp_table->next;
      }
      if (shard_table.size()) {
        sql_tmp.assign(sql);
        replace_sql_function_field(FUNCTION_TYPE_LAST_INSERT_ID, sql_tmp,
                                   st.scanner, plan);
        Statement *new_stmt = NULL;
        sql = sql_tmp.c_str();
        re_parser_stmt(&(st.scanner), &new_stmt, sql);
        table = new_stmt->get_stmt_node()->table_list_head;
        LOG_DEBUG("Query sql after replace last_insert_id() is [%s]\n", sql);
      }
    }

    if (st.var_item_list) deal_select_assign_uservar(st.scanner);
  }
  return table;
}

void Statement::generate_plan_for_prepare_related_stmt(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;

  if (st.type == STMT_PREPARE) {
    const char *prepare_sql = st.sql->prepare_oper->preparable_stmt;
    if (st.var_item_list == NULL) {
      dataspace = get_prepare_sql_dataspace();
      assemble_com_query_prepare_plan(plan, dataspace, prepare_sql,
                                      st.sql->prepare_oper->prepare_name);
    } else {
      /*
       * process SQL like :
       *    SET @s='SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse';
       *    PREPARE prod FROM @s;
       *
       * Then the prepare_sql is
       *    SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse
       *    where equals the user var @s delete the \' lies head and the lies \'
       * tail
       *
       */
      string var_name(prepare_sql);
      boost::to_upper(var_name);
      string var_value = plan->session->get_user_var_origin_value(var_name);
      dataspace = get_prepare_sql_dataspace();
      assemble_com_query_prepare_plan(plan, dataspace, var_value.c_str(),
                                      st.sql->prepare_oper->prepare_name);
    }
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
  if (st.type == STMT_EXEC_PREPARE) {
    assemble_com_query_exec_prepare_plan(
        plan, st.sql->exec_prepare_oper->prepare_name, st.var_item_list);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
  if (st.type == STMT_DROP_PREPARE) {
    dataspace = get_prepare_sql_dataspace();
    assemble_com_query_drop_prepare_plan(
        plan, dataspace, st.sql->drop_prepare_oper->prepare_name, sql);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
}

void Statement::generate_plan_for_flush_config_to_file(ExecutePlan *plan) {
  const char *file_name = st.sql->dbscale_flush_config_to_file_oper->file_name;
  bool flush_all =
      st.sql->dbscale_flush_config_to_file_oper->flush_all == 0 ? false : true;
  if (!file_name) {
    file_name = OptionParser::instance()->get_config_file().c_str();
  }
  assemble_dbscale_flush_config_to_file_plan(plan, file_name, flush_all);
#ifndef DBSCALE_TEST_DISABLE
  plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
  return;
}

void Statement::generate_plan_for_flush_acl(ExecutePlan *plan) {
  assemble_dbscale_flush_acl_plan(plan);
  return;
}

void Statement::generate_plan_for_flush_table_info(ExecutePlan *plan,
                                                   const char *schema_name,
                                                   const char *table_name) {
  assemble_dbscale_flush_table_info_plan(plan, schema_name, table_name);
  return;
}

void Statement::assemble_dbscale_flush_table_info_plan(ExecutePlan *plan,
                                                       const char *schema_name,
                                                       const char *table_name) {
  LOG_DEBUG("Assemble DBSCALE FLUSH table info plan\n");
  ExecuteNode *node =
      plan->get_dbscale_flush_table_info_node(schema_name, table_name);
  plan->set_start_node(node);
}

void Statement::adjust_sql_for_into_file_or_select_into(table_link **table) {
  if (is_into_outfile()) {
    validate_into_outfile();
    into_outfile_item *into_outfile = get_latest_stmt_node()->into_outfile;
    unsigned int start_pos = into_outfile->start_pos;
    unsigned int end_pos = into_outfile->end_pos;
    outfile_sql.append(sql, start_pos - 1);
    outfile_sql.append(end_pos - start_pos + 1, ' ');
    outfile_sql.append(sql + end_pos, strlen(sql) - end_pos);
    sql = outfile_sql.c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    (*table) = new_stmt->get_stmt_node()->table_list_head;
  }

  if (is_select_into()) {
    select_into_item *select_into =
        get_latest_stmt_node()->sql->select_oper->select_into;
    name_item *into_header = select_into->into_list;
    name_item *into_tmp = into_header;
    bool all_uservar = true;
    do {
      if (into_tmp->is_uservar == false) {
        all_uservar = false;
        break;
      }
      into_tmp = into_tmp->next;
    } while (into_tmp != into_header);

    if (all_uservar) {
      unsigned int start_pos = select_into->start_pos;
      unsigned int end_pos = select_into->end_pos;
      select_into_sql.append(sql, start_pos - 1);
      select_into_sql.append(end_pos - start_pos + 1, ' ');
      select_into_sql.append(sql + end_pos, strlen(sql) - end_pos);
      sql = select_into_sql.c_str();
      Statement *new_stmt = NULL;
      re_parser_stmt(&(st.scanner), &new_stmt, sql);
      (*table) = new_stmt->get_stmt_node()->table_list_head;
    }
  }
}

void Statement::generate_plan_for_partial_parse_stmt(ExecutePlan *plan,
                                                     const char *schema_name,
                                                     const char *table_name,
                                                     table_link *table) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();

  if (st.type == STMT_CREATE_SELECT || st.type == STMT_CREATE_LIKE) {
    LOG_ERROR(
        "Not support partial parse for create select or create like stmt %s.\n",
        sql);
    string err(
        "Not support partial parse for create select or create like stmt:");
    if (st.error_message) err.append(st.error_message);
    LOG_ERROR("%s\n", err.c_str());
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError(err.c_str());
  }
  if (table) {
    schema_name = table->join->schema_name ? table->join->schema_name : schema;
    table_name = table->join->table_name;
  } else {
    schema_name = NULL;
    table_name = NULL;
  }

  if (table) {
    if (st.type == STMT_SELECT &&
        !strcasecmp(schema_name, "information_schema")) {
      if (!strcasecmp(table_name, "events") &&
          !table->next) {  // query information_schema.events, which is not
                           // stored on meta source
        // send to the schema source, which store the event object
        dataspace = backend->get_data_space_for_table(schema, NULL);
      } else {
        if (backend->get_metadata_data_space())
          dataspace = backend->get_metadata_data_space();
        else
          dataspace = backend->get_catalog();
      }
      assemble_direct_exec_plan(plan, dataspace);
      return;
    }
  }

  dataspace = backend->get_data_space_for_table(schema_name, table_name);
  if (table && !table->join->schema_name && !is_union_table_sub() &&
      !is_cross_node_join()) {
    plan->session->add_table_without_given_schema(dataspace);
  }
  if (dataspace->is_normal()) {
    if (st.type == STMT_LOAD) {
      if (st.sql->load_oper->local) {
        assemble_load_local_plan(plan, dataspace);
      } else {
        assemble_load_data_infile_plan(plan, dataspace);
      }
    } else {
      assemble_direct_exec_plan(plan, dataspace);
    }
  } else {
    LOG_ERROR(
        "The SQL which contains partition table can not be parsed: [%s].\n",
        st.error_message);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    string error_message(
        "The SQL which contains partition table can not be parsed: ");
    error_message.append(st.error_message);
    throw UnSupportPartitionSQL(error_message.c_str());
  }
#ifndef DBSCALE_TEST_DISABLE
  plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
  return;
}

void Statement::generate_plan_for_rename_or_drop_table_stmt(ExecutePlan *plan) {
  if (st.type == STMT_RENAME_TABLE) {
    rename_table_node_list *head = st.sql->rename_table_oper->table_head;
    rename_table_node_list *tail = st.sql->rename_table_oper->table_tail;
    if (head != tail) {
      LOG_ERROR("DBScale do not support multiple-table rename.");
#ifndef DBSCALE_TEST_DISABLE
      plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
      throw NotSupportedError("Not supported multiple-table rename.");
    }

    handle_rename_table(plan, head);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
  if (st.type == STMT_DROP_TB) {
    if (enable_table_recycle_bin && st.sql->drop_tb_oper->op_tmp == 0 &&
        st.sql->drop_tb_oper->op_exists == 0)
      assemble_move_table_to_recycle_bin_plan(plan);
    else
      assemble_drop_mul_table(plan);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
}

static bool check_col_names_contain_str(name_item *col_list,
                                        const char *part_key) {
  if (!col_list) return true;
  name_item *col_name_head = col_list;
  name_item *col_name = col_list;
  bool is_cover = false;
  do {
    if (!strcasecmp(col_name->name, part_key)) {
      is_cover = true;
      break;
    };
    col_name = col_name->next;
  } while ((col_name != col_name_head));
  return is_cover;
}

void Statement::check_part_table_primary_key(const char *part_key) {
  if (!check_part_primary) return;
  if (st.type != STMT_CREATE_TB && st.type != STMT_ALTER_TABLE) return;
  name_item *primary_key = NULL;
  unique_key_list *unique_keys = NULL;

  if (st.type == STMT_CREATE_TB) {
    if (!st.sql || !st.sql->create_tb_oper) {
      string error_msg;
      error_msg.assign("sql statement node memory error.");
      throw Error(error_msg.c_str());
    }
    primary_key = st.sql->create_tb_oper->primary_key_cols;
    unique_keys = st.sql->create_tb_oper->all_unique_keys;
  } else if (st.type == STMT_ALTER_TABLE) {
    if (!st.sql || !st.sql->alter_tb_oper) {
      string error_msg;
      error_msg.assign("sql statement node memory error.");
      throw Error(error_msg.c_str());
    }
    if (st.sql->alter_tb_oper->alter_type != ADD_COLUMN &&
        st.sql->alter_tb_oper->alter_type != ADD_PRIMARY_KEY &&
        st.sql->alter_tb_oper->alter_type != MODIFY_COLUMN &&
        st.sql->alter_tb_oper->alter_type != CHANGE_COLUMN &&
        st.sql->alter_tb_oper->alter_type != ADD_UNIQUE_KEY)
      return;
    primary_key = st.sql->alter_tb_oper->primary_key_cols;
    unique_keys = st.sql->alter_tb_oper->all_unique_keys;
  }
  if (!unique_keys && !primary_key) return;

  bool is_cover = check_col_names_contain_str(primary_key, part_key);
  if (!is_cover) {
    string error_msg;
    error_msg.assign("primary key must contain partition key.");
    LOG_ERROR("%s\n", error_msg.c_str());
    throw Error(error_msg.c_str());
  }

  while (unique_keys) {
    is_cover = check_col_names_contain_str(unique_keys->col_list, part_key);
    if (!is_cover) {
      string error_msg;
      error_msg.assign("unique key must contain partition key.");
      LOG_ERROR("%s\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }
    unique_keys = unique_keys->next;
  }
  return;
}

// return true means sql be executed on master role dbscale
bool Statement::prepare_dataspace_for_extend_create_tb_stmt(ExecutePlan *plan,
                                                            table_link *table) {
  if (st.type != STMT_CREATE_TB) return false;
  if (dbscale_safe_sql_mode == 2 && st.sql->create_tb_oper->data_type_list) {
    for (data_type_node *it = st.sql->create_tb_oper->data_type_list;
         it != NULL; it = it->next) {
      if (800000 <= it->data_type && it->data_type < 1000000) {
        throw Error(
            "dbscale_safe_sql_mode is 2 CREATE TABLE disable double/float "
            "type ");
      }
    }
  }
  const char *part_table_scheme = NULL;
  const char *part_table_key = NULL;
  if (st.sql->create_tb_oper->part_table_def_start_pos > 0) {
    LOG_DEBUG("Handle create part table space.\n");
    part_table_scheme = st.sql->create_tb_oper->part_table_scheme;
    part_table_key = st.sql->create_tb_oper->part_table_key;
  } else if (plan->session->get_session_option("auto_space_level").uint_val ==
             AUTO_SPACE_TABLE) {
#ifndef CLOSE_MULTIPLE
    if (multiple_mode) {
      if (!MultipleManager::instance()->get_is_cluster_master()) {
        LOG_ERROR(
            "When auto-space-level = 2, the create sql should only executed on "
            "master dbscale.\n");
        throw Error(
            "When auto-space-level = 2, the create sql should only executed on "
            "master dbscale.");
      }
    }
#endif
    Backend *backend = Backend::instance();
    PartitionScheme *partition_scheme = backend->get_last_scheme();
    if (!partition_scheme) {
      string error_msg;
      error_msg.assign(
          "auto_space_level is set as AUTO_SPACE_TABLE, but DBScale does not "
          "config any partition scheme.");
      LOG_ERROR("%s\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }
    part_table_scheme = partition_scheme->get_name();
    part_table_key = get_partition_key_for_auto_space();
  }
  if (part_table_scheme && part_table_key) {
    check_part_table_primary_key(part_table_key);
#ifndef CLOSE_MULTIPLE
    if (multiple_mode) {
      if (MultipleManager::instance()->get_is_cluster_master()) {
        MultipleManager::instance()->acquire_start_config_lock(
            get_session()->get_zk_start_config_lock_extra_info());
        try {
          add_partitioned_table_config_before_create_tb(
              table, part_table_scheme, part_table_key);
        } catch (...) {
          LOG_ERROR("Fail to add_partitioned_table_config_before_create_tb.\n");
          MultipleManager::instance()->release_start_config_lock(
              get_session()->get_start_config_lock_extra_info());
          throw;
        }
        MultipleManager::instance()->release_start_config_lock(
            get_session()->get_start_config_lock_extra_info());
      } else {
        LOG_DEBUG("CREATE TB or ALTER TB be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
#ifndef DBSCALE_TEST_DISABLE
        plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
        return true;
      }
    } else
#endif
      add_partitioned_table_config_before_create_tb(table, part_table_scheme,
                                                    part_table_key);
  }
  return false;
}

void Statement::generate_plan_for_one_table_situation(ExecutePlan *plan,
                                                      table_link *one_table,
                                                      DataSpace *one_space) {
#ifndef DBSCALE_TEST_DISABLE
  if (on_test_stmt) {
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (st.type == STMT_INSERT)
      while (
          !strcasecmp(test_info->test_case_name.c_str(),
                      "swap_backend_monitor") &&
          !strcasecmp(test_info->test_case_operation.c_str(), "block_insert")) {
        timespec_t t = (timespec_t)ACE_Time_Value(0, 1000000);  // sleep 1s
        ACE_OS::nanosleep(&t);
      }
  }
#endif

  if (handle_executable_comments_after_cnj(plan)) {
    return;
  }

  LOG_DEBUG("Only has one table, so skip the analysis_record_scan_tree.\n");
  one_table_node.space = one_space;
  one_table_node.only_one_table = true;

  if (!one_space->get_data_source()) {
    one_table_node.table = one_table;
    vector<const char *> *key_names =
        ((PartitionedTable *)one_space)->get_key_names();

    table_link *par_tb_tmp = one_table;
    const char *schema_name_tmp =
        par_tb_tmp->join->schema_name ? par_tb_tmp->join->schema_name : schema;

    const char *table_name_tmp = par_tb_tmp->join->table_name;
    const char *table_alias_tmp = par_tb_tmp->join->alias;

    one_table_node.rs = st.scanner;
    if (st.type != STMT_INSERT) {
      vector<const char *>::iterator it_key = key_names->begin();
      for (; it_key != key_names->end(); ++it_key) {
        fullfil_par_key_equality(one_table_node.rs, schema_name_tmp,
                                 table_name_tmp, table_alias_tmp, *it_key);
      }
    }
    check_found_rows_with_group_by(plan);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    handle_one_par_table_one_spaces(plan);
    return;

  } else {
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    handle_no_par_table_one_spaces(plan);
    return;
  }
}

void Statement::handle_separated_nodes_before_finial_execute(
    ExecutePlan *plan) {
  plan->session->add_cross_node_join_sql(sql);
  plan->session->get_status()->item_inc(TIMES_CROSS_NODE_JOIN);
  plan->session->set_is_complex_stmt(true);
  plan->add_session_traditional_num();
  /* Execute the subquery separated exection node list.
   *
   * Basic process:
   * 1. work through the exec_nodes vector, invoke the node->init_node()
   * 2. work through the exec_nodes vector, invoke the node->execute()
   * 3. work through the exec_nodes vecotr, invoke the node->clean_node()
   *
   * Note: do not invoke the node->execute() for the last node, which is done
   *       by the current statement.
   * */
  try {
    if (!lower_case_table_names) {
      const char *error_message =
          "DBScale is not supported that execute cross node join with "
          "lower-case-table-names=0.Try lower-case-table-names=1.";
      LOG_ERROR("%s\n", error_message);
      throw Error(error_message);
    }
    MethodType join_type = (MethodType)(
        plan->session->get_session_option("cross_node_join_method").int_val);
    if (exec_nodes.size() == 1 &&
        record_scan_need_cross_node_join.count(st.scanner) &&
        join_type == DATA_MOVE_READ) {
      handle_federated_join_method();
    }
    SeparatedExecNode *last_node = NULL;
    vector<SeparatedExecNode *>::iterator it = exec_nodes.begin();
    for (; it != exec_nodes.end(); ++it) {
      (*it)->init_node();
    }
    if (stmt_max_threads <= 1 || exec_nodes.size() <= 2 ||
        !sep_node_can_execute_parallel(plan)) {
      // there is meaningless for parallel execution if the exec_nodes less
      // than 3, cause the last one will not be executed parallelly
      it = exec_nodes.begin();
      for (; it + 1 != exec_nodes.end(); ++it) {
        (*it)->pre_execute();
        (*it)->execute();
        (*it)->post_execute();
      }
      last_node = (*it);
    } else {
      LOG_DEBUG(
          "Start to execute the separated node parallelly with %d nodes.\n",
          exec_nodes.size());
      is_sep_node_parallel_exec = true;
      init_parallel_separated_node_execution();
      bool has_finish_node_assignment = false;
      bool has_finish_node_execution_wait = false;
      try {
        while (1) {
          if (!has_finish_node_assignment) {
            loop_and_find_executable_sep_nodes();
            has_finish_node_assignment = assign_threads_for_work_vec();
            LOG_DEBUG(
                "After threads assignment, has_finish_node_assignment is %d,"
                " work_pointer is %d, work_vec size is %d, running_threads is "
                "%d.\n",
                has_finish_node_assignment ? 1 : 0, work_pointer,
                work_vec.size(), running_threads);
          }
          if (!has_finish_node_execution_wait) {
            has_finish_node_execution_wait = wait_threads_for_execution();
            LOG_DEBUG(
                "After node execution wait, has_finish_node_execution_wait is "
                "%d,"
                " fin_size is %d.\n",
                has_finish_node_execution_wait ? 1 : 0, fin_size);
          }
          if (has_finish_node_execution_wait) break;
        }
      } catch (...) {
        LOG_DEBUG("Get exception/error during sparated node parallelly.\n");
        clean_up_parallel_separated_node_execution();
        throw;
      }
      clean_up_parallel_separated_node_execution();
      last_node = exec_nodes[exec_nodes.size() - 1];
    }

    last_node->pre_execute();
    exec_sql_after_separated_exec.clear();
    exec_sql_after_separated_exec.append(last_node->get_execute_sql());
    sql = exec_sql_after_separated_exec.c_str();
    while (num_of_separated_exec_space--) {
      spaces.erase(spaces.begin());
    }
    if (par_table_num >= num_of_separated_par_table) {
      par_table_num = par_table_num - num_of_separated_par_table;
      while (num_of_separated_par_table-- && par_tables.size() > 1) {
        par_tables.erase(par_tables.begin());
      }
    }
    if (record_scan_need_cross_node_join.count(st.scanner) ||
        record_scan_seperated_table_subquery.count(st.scanner)) {
      if (st.type == STMT_UPDATE || st.type == STMT_DELETE)
        set_cross_node_join(false);
      else
        set_cross_node_join(true);
    }
    record_scan *child_rs = st.scanner->children_begin;
    Statement *new_stmt = NULL;
    DataSpace *tmp_space = record_scan_one_space_tmp_map[st.scanner];
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    if (st.type == STMT_UPDATE)
      *(st.sql->update_oper) = *(new_stmt->get_stmt_node()->sql->update_oper);
    if (st.type == STMT_INSERT_SELECT)
      *(st.sql->insert_oper) = *(new_stmt->get_stmt_node()->sql->insert_oper);
    record_scan_one_space_tmp_map[st.scanner] = tmp_space;
    refresh_norm_table_spaces();
    refresh_part_table_links();
    if (need_merge_subquery)
      refresh_tables_for_merged_subquery(child_rs, st.scanner->children_begin);
  } catch (...) {
    plan->session->clear_column_replace_string();
    throw;
  }
  plan->session->clear_column_replace_string();
}

void Statement::generate_plan_for_set_stmt(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();
  if (is_set_password(plan)) {
    dataspace = backend->get_auth_data_space();
    assemble_direct_exec_plan(plan, dataspace);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }

  if (!spaces.size()) {
    dataspace = backend->get_data_space_for_table(schema, NULL);
  } else if (par_table_num != 0) {
    LOG_ERROR("Unsupport set user variable select from partition table.\n");
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError(
        "Unsupport set user variable select from partition table");
  } else if (spaces.size() == 1) {
    dataspace = spaces[0];
  } else {
    LOG_ERROR(
        "Unsupport set user variable select from table"
        " execute in different server.\n");
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError(
        "Unsupport set user variable select from partition table");
  }

  if (st.sql->set_oper && st.sql->set_oper->names && st.sql->set_oper->value) {
    string charset_val = st.sql->set_oper->value;
    plan->handler->set_names_charset_in_session(plan->handler->get_session(),
                                                charset_val);
  }
  handle_session_var(plan);
  deal_set_var();
  assemble_set_plan(plan, dataspace);
#ifndef DBSCALE_TEST_DISABLE
  plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
  return;
}

bool Statement::generate_plan_according_to_par_table_num_and_spaces(
    ExecutePlan *plan) {
  if (!spaces.size()) {
    LOG_ERROR("Fail to find dataspace for sql [%s].\n", sql);
    throw Error("Fail to find dataspace.");
  } else if (!par_table_num && spaces.size() == 1) {
    handle_no_par_table_one_spaces(plan);
    return true;
  } else if (par_table_num == 1 && spaces.size() == 1) {
    check_found_rows_with_group_by(plan);
    handle_one_par_table_one_spaces(plan);
    return true;
  } else if (!par_table_num && spaces.size() > 1) {
    if (spaces.size() == 2) {
      if (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT ||
          (st.type == STMT_DBSCALE_ESTIMATE &&
           st.estimate_type == STMT_INSERT_SELECT)) {
        /*For insert...select stmt, two dataspaces is executable. Cause the
         * select part can executed independent from the insert part.*/
        handle_no_par_table_one_spaces(plan);
        return true;
      }
      if (st.type == STMT_UPDATE || st.type == STMT_DELETE) {
        if (record_scan_one_space_tmp_map[st.scanner] == spaces[0]) {
          /*For update/delete stmt with subquery, the select part dataspace
           * should cover the modify part. spaces[0] stored the merged select
           * part dataspace. record_scan_one_space_tmp_map[st.scanner] stored
           * the merged dataspace of top record scan.*/
          handle_no_par_table_one_spaces(plan);
          return true;
        }
      }
    }
    LOG_ERROR("Unsupport normal sql, the sql is [%s].\n", sql);
    throw UnSupportPartitionSQL("Unsupport normal sql");
  } else if (par_table_num == 1) {  // mul dataspace, such as insert_select stmt
    check_found_rows_with_group_by(plan);
    handle_one_par_table_mul_spaces(plan);
    return true;
  } else {  // mul par_table and mul dataspace
    // TODO: support it by execute each record_scan, which can not be merged,
    // respectively.
    check_found_rows_with_group_by(plan);
    plan->session->set_is_complex_stmt(true);
    handle_mul_par_table_mul_spaces(plan);
    return true;
    //    LOG_ERROR("Unsupport partition sql, the sql is [%s].\n", sql); throw
    //    UnSupportPartitionSQL();
  }

  return false;
}

void Statement::generate_plan_for_non_support_trx_stmt(ExecutePlan *plan) {
  if (st.type == STMT_RELEASE_SAVEPOINT) {
    LOG_WARN("Release Savepoint is not supported, sql is %s, just ignore it.\n",
             sql);
    ExecuteNode *node = plan->get_return_ok_node();
    plan->set_start_node(node);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  } else {
    /*Savepoint is not support when enable-xa-transaction is enabled, just
     * ignore it.*/
    LOG_WARN(
        "Savepoint is not support when enable-xa-transaction is enabled, sql "
        "is %s, just ignore it.\n",
        sql);
    ExecuteNode *node = plan->get_return_ok_node();
    plan->set_start_node(node);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
}

void Statement::check_stmt_for_lock_mode(ExecutePlan *plan) {
  try {
    deal_lock_table(plan);
  } catch (dbscale::sql::SQLError &e) {
    if (e.get_errno() == ERROR_NOT_LOCK_CODE)
      LOG_DEBUG("NotLockError for sql [%s]\n", sql);
    else if (e.get_errno() == ERROR_NOT_EXECUTE_FOR_LOCK_CODE)
      LOG_DEBUG("NotExecutableForLock sql [%s]\n", sql);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw;
  }
}

void Statement::generate_plan_for_information_schema(ExecutePlan *plan,
                                                     const char *table_name,
                                                     table_link *table) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();

  if (skip_informatic_reference_query &&
      !strcasecmp(table_name, "KEY_COLUMN_USAGE") && table->next &&
      !strcasecmp(table->next->join->table_name, "KEY_COLUMN_USAGE") &&
      table->next->next &&
      !strcasecmp(table->next->next->join->table_name,
                  "REFERENTIAL_CONSTRAINTS")) {
    LOG_INFO(
        "Skip the reference constrain query from informatic due to "
        "skip-informatic-reference-query.\n");
    int field_num = get_field_num();
    assemble_empty_set_node(plan, field_num);
    return;
  }

  if (support_navicat_profile_sql(plan)) {
    return;
  } else if (!strcasecmp(table_name, "events") &&
             !table->next) {  // query information_schema.events, which is not
                              // stored on meta source
    // send to the schema source, which store the event object
    dataspace = backend->get_data_space_for_table(schema, NULL);
  } else if ((!strcasecmp(table_name, "tables") ||
              !strcasecmp(table_name, "columns")) &&
             table->next) {
    const char *schema_name = table->next->join->schema_name
                                  ? table->next->join->schema_name
                                  : schema;
    if (!strcasecmp(schema_name, "information_schema")) {
      dataspace = backend->get_metadata_data_space();
      if (!dataspace) {
        LOG_ERROR("Fail to get meta datasource.\n");
        throw Error("Fail to get meta datasource.");
      }
    } else {
      dataspace = backend->get_data_space_for_table(
          schema_name, table->next->join->table_name);
    }
#ifdef DEBUG
    ACE_ASSERT(dataspace);
#endif
    if (!dataspace->is_normal()) {
      LOG_ERROR(
          "unsupport join between information_schema.tables/columns and a "
          "partitioned table.\n");
      throw NotSupportedError(
          "unsupport join between information_schema.tables/columns and a "
          "partitioned table.");
    }
    LOG_DEBUG(
        "found table join between information_schema.tables/columns and a "
        "normal table, dataspace name is [%s]\n",
        dataspace->get_name());
  } else {
    if (backend->get_metadata_data_space())
      dataspace = backend->get_metadata_data_space();
    else
      dataspace = backend->get_catalog();
  }
  assemble_direct_exec_plan(plan, dataspace);
}

void Statement::pre_work_for_non_one_table_exec_plan(ExecutePlan *plan,
                                                     bool only_one_table,
                                                     record_scan *one_rs,
                                                     DataSpace *one_space,
                                                     table_link *one_table) {
  if (st.has_where_rownum) {
    LOG_ERROR("Only support normal table to use rownum.\n");
    throw NotSupportedError("Only support normal table to use rownum.");
  }
  if (only_one_table) {  // fail to use one table mode, so need to re-fill the
                         // info to related structures, which is skipped in the
                         // previous code
    record_scan_all_table_spaces[one_table] = one_space;
    need_clean_record_scan_all_table_spaces = true;
    record_scan_all_par_tables_map[one_rs].push_back(one_table);
    need_clean_record_scan_all_par_tables_map = true;
  }
  record_scan_par_key_equality.clear();
  record_scan_par_key_values.clear();
  need_center_join = false;
#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "cross_node_join") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "use_center_join")) {
    need_center_join = true;
  }
#endif
  if (need_center_join && st.type == STMT_SELECT && !union_all_sub &&
      !contains_view && !st.scanner->is_select_union)
    analysis_center_join(plan);
  else
    analysis_record_scan_tree(st.scanner, plan);

  if ((exec_nodes.size() > 1 ||
       (exec_nodes.size() == 1 &&
        record_scan_need_cross_node_join.count(st.scanner))) &&
      st.type != STMT_DBSCALE_ESTIMATE) {
    // For a statement which contains seperated node, we should regard it as not
    // read only.
    plan->session->set_read_only(false);
    handle_separated_nodes_before_finial_execute(plan);
  } else if (st.type != STMT_DBSCALE_ESTIMATE) {
    /* For the sql which need to execute, We should not set the
     * par_table_num=1 since we need the dataspace to assemble execution
     * plan. */
    if (par_table_num > num_of_separated_par_table) {
      par_table_num = par_table_num - num_of_separated_par_table;
      while (num_of_separated_par_table-- && par_tables.size() > 1) {
        par_tables.erase(par_tables.begin());
      }
    }
  }
}

bool Statement::generate_plan_for_no_table_situation(ExecutePlan *plan) {
  if ((!st.table_list_head && st.type != STMT_LOAD && st.type != STMT_SET) ||
      st.type == STMT_LOCK_TB || st.type == STMT_CREATE_EVENT ||
      st.type == STMT_DROP_EVENT || st.type == STMT_CREATE_PROCEDURE ||
      st.type == STMT_DBSCALE_MIGRATE ||
      st.type == STMT_DBSCALE_SHOW_VIRTUAL_MAP ||
      st.type == STMT_DBSCALE_SHOW_SHARD_MAP ||
      st.type == STMT_DBSCALE_SHOW_AUTO_INCREMENT_VALUE ||
      st.type == STMT_DBSCALE_DYNAMIC_SET_AUTO_INCREMENT_OFFSET ||
      st.type == STMT_DBSCALE_REQUEST_CLUSTER_INC_INFO ||
      st.type == STMT_CREATE_VIEW || st.type == STMT_DROP_VIEW ||
      st.type == STMT_DBSCALE_SHOW_TABLE_LOCATION ||
      st.type == STMT_DBSCALE_BLOCK || st.type == STMT_DBSCALE_CHECK_TABLE ||
      st.type == STMT_CREATE_TRIGGER || st.type == STMT_DBSCALE_RESTORE_TABLE ||
      st.type == STMT_DBSCALE_CLEAN_RECYCLE_TABLE) {
    if (st.type == STMT_FLUSH) {
      /*The unsupport flush stmt, just ignore it.*/
      LOG_WARN("DBScale find unsupport flush stmt %s, just ignore it.\n", sql);
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      return true;
    }

#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    generate_execution_plan_with_no_table(plan);
    return true;
  }

  return false;
}

void Statement::generate_plan_for_create_tmp_table(ExecutePlan *plan) {
  LOG_DEBUG("generate_plan_for_create_tmp_table.\n");
  join_node *table = st.sql->create_tb_oper->table;
  const char *schema_name = table->schema_name ? table->schema_name : schema;
  const char *table_name = table->table_name;
  Backend *backend = Backend::instance();
  DataSpace *ds = backend->get_data_space_for_table(schema_name, table_name);

  if (ds->is_partitioned() || ds->is_duplicated() || ds->is_tmp_table_space()) {
    LOG_ERROR(
        "Unsupport CREATE TEMPORARY TABLE for table name [%s.%s], it should be "
        "a normal table.\n",
        schema_name, table_name);
    throw NotSupportedError("CREATE TEMPORARY TABLE should be a normal table.");
  }

  ExecuteNode *node = plan->get_direct_execute_node(ds, sql);
  plan->set_start_node(node);
  st.is_create_or_drop_temp_table = true;
}

bool Statement::is_empty_top_select_with_limit() {
  if (st.type == STMT_SELECT && st.scanner->limit &&
      !st.scanner->group_by_list && !st.scanner->having &&
      !st.scanner->condition  // no groupby or having or condition or order by
      && !st.scanner->distinct_start_pos &&
      !st.scanner->order_by_list  // no distinct or order by for top select
      && st.scanner->children_begin != NULL &&
      st.scanner->children_begin ==
          st.scanner->children_end           // only has one subquery
      && !st.scanner->children_begin->limit  // the subquery has no limit
      && st.scanner->join_tables &&
      st.scanner->join_tables->type ==
          JOIN_NODE_SUBSELECT) {  // from only has one table subquery
    field_item *fi = st.scanner->field_list_head;
    if (fi) {
      do {
        if (fi->field_expr->type == EXPR_STR ||
            fi->field_expr->type == EXPR_UNIT) {  // select * or select column
          fi = fi->next;
        } else
          return false;
      } while (fi && fi != st.scanner->field_list_tail);
    }
    return true;
  }
  return false;
}

void Statement::move_top_select_limit_to_table_subquery(table_link **table) {
  string limit_str(sql + st.scanner->limit_pos - 1,
                   st.scanner->end_pos - st.scanner->limit_pos + 1);
  string newsql(sql, st.scanner->children_begin->end_pos);
  newsql.append(" ");
  newsql.append(limit_str.c_str());
  newsql.append(
      sql + st.scanner->children_begin->end_pos,
      st.scanner->limit_pos - st.scanner->children_begin->end_pos - 1);
  sql_tmp.assign(newsql.c_str());
  LOG_DEBUG(
      "Statement::move_top_select_limit_to_table_subquery adjust the sql to "
      "[%s] from [%s].\n",
      newsql.c_str(), sql);
  sql = sql_tmp.c_str();
  Statement *new_stmt = NULL;
  re_parser_stmt(&(st.scanner), &new_stmt, sql);
  *table = new_stmt->get_stmt_node()->table_list_head;
}

string Statement::generate_full_column_name(string expr_str) {
  string str;
  size_t first_dot_pos = expr_str.find(".");
  size_t second_dot_pos = expr_str.find(".", first_dot_pos + 1);

  if (first_dot_pos == string::npos) {
    str.assign("`");
    str.append(expr_str);
    str.append("`");
  } else if (second_dot_pos == string::npos) {
    str.assign("`");
    str.append(expr_str, 0, first_dot_pos);
    str.append("`.`");
    str.append(expr_str, first_dot_pos + 1,
               expr_str.length() - first_dot_pos - 1);
    str.append("`");
  } else {
    str.assign("`");
    str.append(expr_str, 0, first_dot_pos);
    str.append("`.`");
    str.append(expr_str, first_dot_pos + 1, second_dot_pos - first_dot_pos - 1);
    str.append("`.`");
    str.append(expr_str, second_dot_pos + 1,
               expr_str.length() - second_dot_pos - 1);
    str.append("`");
  }
  return str;
}

void Statement::handle_connect_by_expr(connect_by_node *conn_node) {
  is_connect_by = true;

  string expr_str;
  if (st.scanner->condition) {
    expr_str.assign(sql, st.scanner->opt_where_expr_start_pos - 1,
                    st.scanner->opt_where_end_pos -
                        st.scanner->opt_where_expr_start_pos + 1);
    new_select_items.push_back(expr_str);
    connect_by_desc.where_index = --new_select_item_num;
  } else {
    connect_by_desc.where_index = 0;
  }

  expr_str.assign(sql, conn_node->with_start_pos - 1,
                  conn_node->with_end_pos - conn_node->with_start_pos + 1);
  new_select_items.push_back(expr_str);
  connect_by_desc.start_index = --new_select_item_num;

  expr_str.assign(sql, conn_node->prior_start_pos - 1,
                  conn_node->prior_end_pos - conn_node->prior_start_pos + 1);
  new_select_items.push_back(expr_str);
  connect_by_desc.prior_index = --new_select_item_num;

  expr_str.assign(sql, conn_node->recur_start_pos - 1,
                  conn_node->recur_end_pos - conn_node->recur_start_pos + 1);
  new_select_items.push_back(expr_str);
  connect_by_desc.recur_index = --new_select_item_num;
}

void Statement::check_and_replace_oracle_sequence(ExecutePlan *plan,
                                                  table_link **table) {
  if (!enable_oracle_sequence) return;
  if (!st.seq_items_head) return;
  if (st.type == STMT_CREATE_PROCEDURE || st.type == STMT_CREATE_FUNCTION)
    return;

  int alt_len = 0;
  oracle_sequence_item *seq_item = st.seq_items_head;
  sql_sequence_val_replaced = sql;
  while (seq_item) {
    string schemaname;
    int replace_start_pos = seq_item->seq_name_start_pos + alt_len;
    int replace_end_pos = seq_item->next_or_curr_end_pos + alt_len;
    if (seq_item->seq_schema_name_start_pos > 0) {
      schemaname = string(sql_sequence_val_replaced,
                          seq_item->seq_schema_name_start_pos - 1 + alt_len,
                          seq_item->seq_schema_name_end_pos -
                              seq_item->seq_schema_name_start_pos + 1);
      replace_start_pos = seq_item->seq_schema_name_start_pos + alt_len;
    } else {
      schemaname = plan->session->get_schema();
    }
    string seqname = string(
        sql_sequence_val_replaced, seq_item->seq_name_start_pos - 1 + alt_len,
        seq_item->seq_name_end_pos - seq_item->seq_name_start_pos + 1);
    bool is_nextval = seq_item->is_nextval;
    int64_t seqval;
    if (is_nextval) {
      seqval =
          Backend::instance()->get_oracle_sequence_nextval(seqname, schemaname);
      LOG_DEBUG("get sequence %s.%s.nextval=%Q\n", schemaname.c_str(),
                seqname.c_str(), seqval);
    } else {
      seqval =
          Backend::instance()->get_oracle_sequence_currval(seqname, schemaname);
      LOG_DEBUG("get sequence %s.%s.currval=%Q\n", schemaname.c_str(),
                seqname.c_str(), seqval);
    }
    char tmp[21];
    int val_len = sprintf(tmp, "%ld", seqval);
    string tmp_sql =
        string(sql_sequence_val_replaced, 0, replace_start_pos - 1);
    tmp_sql.append(tmp);
    tmp_sql.append(sql_sequence_val_replaced.c_str() + replace_end_pos);
    sql_sequence_val_replaced = tmp_sql;
    alt_len = (val_len - (replace_end_pos - replace_start_pos + 1)) + alt_len;
    seq_item = seq_item->next;
  }
  sql = sql_sequence_val_replaced.c_str();
  LOG_DEBUG("get sql with sequence value replaced: %s\n", sql);
  Statement *new_stmt = NULL;
  re_parser_stmt(&(st.scanner), &new_stmt, sql);
  *table = new_stmt->get_stmt_node()->table_list_head;
}

void Statement::assemble_dbscale_create_oracle_sequence_plan(
    ExecutePlan *plan) {
  create_oracle_seq_op_node *oper = st.sql->create_oracle_seq_oper;
  ExecuteNode *node = plan->get_create_oracle_sequence_node(oper);
  plan->set_start_node(node);
}

DataSpace *Statement::check_and_replace_odbc_uservar(ExecutePlan *plan) {
  if (st.var_item_list == NULL) return NULL;

  DataSpace *odbc_space = NULL;
  bool has_non_odbc = false;
  Backend *backend = Backend::instance();
  DataSpace *space = NULL;
  table_link *table = st.table_list_head;
  while (table) {
    const char *schema_name =
        table->join->schema_name ? table->join->schema_name : schema;
    const char *table_name = table->join->table_name;
    space = backend->get_data_space_for_table(schema_name, table_name);
    if (space->get_data_source() &&
        space->get_data_source()->get_data_source_type() ==
            DATASOURCE_TYPE_ODBC) {
      if (has_non_odbc) {
        LOG_ERROR("Not support odbc dataspace cross join with user var.\n");
        throw Error("Not support odbc dataspace cross join with user var.");
      }
      if (odbc_space && odbc_space != space) {
        LOG_ERROR("Not support odbc dataspace cross join with user var.\n");
        throw Error("Not support odbc dataspace cross join with user var.");
      }
      odbc_space = space;
    } else {
      if (odbc_space) {
        LOG_ERROR("Not support odbc dataspace cross join with user var.\n");
        throw Error("Not support odbc dataspace cross join with user var.");
      }
      has_non_odbc = true;
    }
    table = table->next;
  }

  if (odbc_space) {
    Session *session = plan->session;
    map<string, string> *user_var_map = session->get_user_var_map();
    string new_query_sql_string;
    var_item *var = st.var_item_list;
    list<var_item *> var_list;
    while (var) {
      var_list.push_front(var);
      var = var->next;
    }
    list<var_item *>::iterator it = var_list.begin();
    unsigned int start_pos = 0;
    for (; it != var_list.end(); ++it) {
      new_query_sql_string.append(sql, start_pos,
                                  (*it)->start_pos - start_pos - 1);

      string var_name = (*it)->value;
      boost::to_upper(var_name);
      if (user_var_map->count(var_name)) {
        new_query_sql_string.append((*user_var_map)[var_name]);
      } else {
        new_query_sql_string.append("NULL");
      }
      start_pos = (*it)->end_pos;
    }
    new_query_sql_string.append(sql, start_pos, strlen(sql) - start_pos);
    LOG_DEBUG("New sql after replace odbc user var [%s]\n",
              new_query_sql_string.c_str());
    record_statement_stored_string(new_query_sql_string);
    sql = statement_stored_string.back().c_str();
  }

  return odbc_space;
}

bool Statement::assemble_dbscale_cal_uservar_plan(ExecutePlan *plan) {
  bool ret = false;
  bool has_init_gmp_expr = false;
  bool has_init_sequence = false;
  map<const char *, oracle_sequence_item *> ora_sequence;
  map<string, pair<string, bool> > var_map;
  map<string, string> ref_var_map;
  set<string> null_vars;
  expr_list_item *head =
      ((ListExpression *)st.sql->set_oper->set_list)->expr_list_head;
  expr_list_item *tmp = head;

  CompareExpression *cmp;
  VarExpression *var_expr;
  Expression *val_expr;
  do {
    cmp = (CompareExpression *)(tmp->expr);
    var_expr = (VarExpression *)(cmp->left);
    val_expr = cmp->right;
    if (var_expr->var_scope == VAR_SCOPE_USER) {
      string var(var_expr->str_value);
      boost::to_upper(var);

      if (val_expr->type == EXPR_NULL) {
        null_vars.insert(var);
      } else if (val_expr->type == EXPR_STR) {
        if (enable_oracle_sequence && st.seq_items_head) {
          if (!has_init_sequence) {
            oracle_sequence_item *seq_item = st.seq_items_head;
            while (seq_item) {
              ora_sequence[seq_item->full_name] = seq_item;
              seq_item = seq_item->next;
            }
            has_init_sequence = true;
          }
          if (ora_sequence.count(val_expr->str_value)) {
            oracle_sequence_item *seq_item = ora_sequence[val_expr->str_value];
            const char *schema_name =
                seq_item->schema_name ? seq_item->schema_name : schema;
            const char *seq_name = seq_item->seq_name;
            string schemaname(schema_name);
            string seqname(seq_name);
            bool is_nextval = seq_item->is_nextval;
            int64_t seqval;
            if (is_nextval) {
              seqval = Backend::instance()->get_oracle_sequence_nextval(
                  seqname, schemaname);
              LOG_DEBUG("get sequence %s.%s.nextval=%Q\n", schemaname.c_str(),
                        seqname.c_str(), seqval);
            } else {
              seqval = Backend::instance()->get_oracle_sequence_currval(
                  seqname, schemaname);
              LOG_DEBUG("get sequence %s.%s.currval=%Q\n", schemaname.c_str(),
                        seqname.c_str(), seqval);
            }
            char tmp[21];
            snprintf(tmp, 21, "%ld", seqval);
            string val(tmp);
            var_map[var] = make_pair(val, false);
            continue;
          }
        }
        return false;
      } else if (val_expr->type == EXPR_STRING) {
        string val(val_expr->str_value);
        var_map[var] = make_pair(val, true);
      } else if (val_expr->type == EXPR_VAR) {
        if (((VarExpression *)val_expr)->var_scope == VAR_SCOPE_USER) {
          string val(val_expr->str_value);
          boost::to_upper(val);
          ref_var_map[var] = val;
        } else {
          return false;
        }
      } else if (val_expr->get_function_type() == FUNCTION_TYPE_CONCAT) {
        string total_val;
        ListExpression *list_expr =
            ((FunctionExpression *)val_expr)->param_list;
        expr_list_item *item = list_expr->expr_list_head;
        do {
          string val;
          Expression *value = item->expr;
          if (value->type == EXPR_VAR &&
              ((VarExpression *)value)->var_scope == VAR_SCOPE_USER) {
            val.assign(value->str_value);
            boost::to_upper(val);
            val = *(plan->session->get_user_var_value(val));
            if (val[0] == '\'') {
              boost::erase_head(val, 1);
              boost::erase_tail(val, 1);
            }
          } else {
            const char *value_str = get_str_from_simple_value(value);
            if (value_str) {
              val.assign(value_str);
            } else {
              LOG_ERROR("Fail to get concat list string value.\n");
              return false;
            }
          }
          total_val.append(val);
          item = item->next;
        } while (item != list_expr->expr_list_head);
        var_map[var] = make_pair(total_val, true);
      } else {
        map<string, string> *user_var_map = plan->session->get_user_var_map();
        if (!has_init_gmp_expr) {
          var_item *var_item_list = st.var_item_list;
          while (var_item_list != NULL) {
            string var_name = var_item_list->value;
            boost::to_upper(var_name);
            if (var_item_list->expr) {
              VarExpression *expr = (VarExpression *)var_item_list->expr;
              if (user_var_map->count(var_name)) {
                expr->set_user_var_null(false);
                expr->set_user_var_value(((*user_var_map)[var_name]).c_str());
              } else {
                expr->set_user_var_null(true);
              }
            }
            var_item_list = var_item_list->next;
          }
          has_init_gmp_expr = true;
        }
        try {
          const char *val_str = get_gmp_from_simple_value(val_expr);
          if (val_str) {
            string val(val_str);
            var_map[var] = make_pair(val, false);
            if (need_clean_simple_expression) clear_simple_expression();
          } else {
            LOG_INFO(
                "Not support handle current userver on dbscale, send it to "
                "server.\n");
            return false;
          }
        } catch (...) {
          LOG_ERROR("Get exception when get gmp from simple value.\n");
          clear_simple_expression();
          return false;
        }
      }
    } else {
      return false;
    }
    tmp = tmp->next;
  } while (tmp != head);

  if (!var_map.empty() || !ref_var_map.empty() || !null_vars.empty()) {
    ret = true;
    map<string, pair<string, bool> >::iterator it = var_map.begin();
    for (; it != var_map.end(); ++it) {
      plan->session->add_user_var_value(it->first, it->second.first,
                                        it->second.second, true);
      LOG_DEBUG("DBScale handle uservar [%s] = [%s].\n", it->first.c_str(),
                it->second.first.c_str());
    }
    map<string, string>::iterator it_ref = ref_var_map.begin();
    for (; it_ref != ref_var_map.end(); ++it_ref) {
      plan->session->set_var_by_ref(it_ref->first, it_ref->second);
      LOG_DEBUG("DBScale handle uservar [%s] with [%s].\n",
                it_ref->first.c_str(), it_ref->second.c_str());
    }
    set<string>::iterator it_set = null_vars.begin();
    for (; it_set != null_vars.end(); ++it_set) {
      plan->session->remove_user_var_by_name(*it_set);
      LOG_DEBUG("DBScale handle null uservar [%s], remove it.\n",
                it_set->c_str());
    }
  }

  if (ret) {
    ExecuteNode *node = plan->get_return_ok_node();
    plan->set_start_node(node);
  }
  return ret;
}

bool Statement::generate_forward_master_role_plan(ExecutePlan *plan) {
  if (!multiple_mode) return false;
  if (st.type == STMT_ALTER_TABLE || st.type == STMT_DROP_DB ||
      st.type == STMT_DROP_TB || st.type == STMT_RENAME_TABLE ||
      st.type == STMT_DBSCALE_CREATE_OUTLINE_HINT ||
      st.type == STMT_DBSCALE_FLUSH_OUTLINE_HINT ||
      st.type == STMT_DBSCALE_DELETE_OUTLINE_HINT) {
    check_acl_and_safe_mode(plan);
    MultipleManager *mul = MultipleManager::instance();
    bool is_master = mul->get_is_cluster_master();
    if (!is_master) {
      LOG_DEBUG("Forward sql [%s] to master.\n", sql);
      assemble_forward_master_role_plan(plan, true);
      return true;
    }
  }
  return false;
}

bool Statement::check_duplicated_table() {
  const char *schema_name = NULL;
  const char *table_name = NULL;
  DataSpace *dataspace = NULL;
  table_link *table = st.table_list_head;
  while (table) {
    schema_name = table->join->schema_name ? table->join->schema_name : schema;
    table_name = table->join->table_name;
    dataspace =
        Backend::instance()->get_data_space_for_table(schema_name, table_name);
    if (dataspace->is_duplicated()) return true;
    table = table->next;
  }
  return false;
}

void Statement::check_disaster_relate_option(
    dynamic_config_op_node *config_oper) {
  if (!config_oper || !config_oper->option_name) return;
  Backend *backend = Backend::instance();
  string option_name(config_oper->option_name);
  boost::to_lower(option_name);
  boost::replace_all(option_name, "-", "_");
  if (option_name == "enable_disaster_mode") {
    strcasecmp(config_oper->int_val, "1")
        ? backend->validate_disable_disaster_mode()
        : backend->validate_enable_disaster_mode();
  }
  if (enable_disaster_mode) return;
  if (option_name == "slave_dbscale_mode") {
    LOG_ERROR(
        "Option \"slave-dbscale-mode\" can only be set when "
        "\"enable-disaster-mode\" is open.\n");
    throw Error(
        "Option \"slave-dbscale-mode\" can only be set when "
        "\"enable-disaster-mode\" is open.\n");
  }
}

/*when create or alter table, have some restrictions.
1.restrict_create_table is 1, then can not drop primary key.
2.part table can not drop and alter part key.
3.check_part_primary is 1, then primary key must contain part key, unique key
must be part key*/
void Statement::check_create_or_alter_table_restrict(const char *schema_name,
                                                     const char *table_name) {
  if (st.type != STMT_CREATE_TB && st.type != STMT_ALTER_TABLE) return;
  if (st.type == STMT_ALTER_TABLE) {
    if (st.sql->alter_tb_oper &&
        st.sql->alter_tb_oper->alter_type == DROP_COLUMN &&
        restrict_create_table) {
      map<string, TableColumnInfo *, strcasecomp> *column_info_map;
      TableInfoCollection *tic = TableInfoCollection::instance();
      TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);
      try {
        column_info_map =
            ti->element_table_column
                ->get_element_map_columnname_table_column_info(stmt_session);
      } catch (...) {
        LOG_ERROR(
            "Error occured when try to get table info column "
            "info(map_table_column_info) of table [%s.%s]\n",
            schema_name, table_name);
        ti->release_table_info_lock();
        throw;
      }
      if (ti->primary_key_col_nums == 1) {
        // if drop column is primary col, check whether have other primary
        // key
        if (column_info_map->count(
                st.sql->alter_tb_oper->change_column->name) &&
            !strcasecmp(
                (*column_info_map)[st.sql->alter_tb_oper->change_column->name]
                    ->column_key.c_str(),
                "PRI")) {
          string error_msg;
          error_msg.assign("can't drop primary key column.");
          LOG_ERROR("%s\n", error_msg.c_str());
          ti->release_table_info_lock();
          throw Error(error_msg.c_str());
        }
      }
      ti->release_table_info_lock();
    }
  }
  DataSpace *dataspace =
      Backend::instance()->get_data_space_for_table(schema_name, table_name);
  if (dataspace->is_partitioned()) {
    vector<const char *> *key_names =
        ((PartitionedTable *)dataspace)->get_key_names();
    const char *key = key_names->at(0);
    if (st.type == STMT_ALTER_TABLE && st.sql->alter_tb_oper &&
        (st.sql->alter_tb_oper->alter_type == DROP_COLUMN ||
         st.sql->alter_tb_oper->alter_type == CHANGE_COLUMN) &&
        (!strcasecmp(st.sql->alter_tb_oper->change_column->name, key))) {
      string error_msg;
      error_msg.assign("can't drop or change part_key.");
      LOG_ERROR("%s\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }
    check_part_table_primary_key(key);
  }
}
bool Statement::is_need_parse_transparent() {
  if (use_partial_parse == NONE || use_partial_parse == PARTIAL_PARSE)
    return false;
  if (is_stmt_type_parse_transparent(get_stmt_node())) return true;
  return false;
}

void Statement::generate_execution_plan(ExecutePlan *plan) {
  if (enable_read_only &&
      (st.type >= STMT_DDL_START && st.type <= STMT_CALL &&
       st.type != STMT_SET && st.type != STMT_SHOW_WARNINGS &&
       st.type != STMT_CHANGE_DB)) {
    LOG_INFO("Read only is on,can not execute sql %s.\n", sql);
    throw ReadOnlyError();
  }

  if (reinforce_enable_read_only && ((st.type >= STMT_NORMAL_GTID_START &&
                                      st.type <= STMT_NORMAL_GTID_STOP) ||
                                     (st.type >= STMT_DBSCALE_GTID_START &&
                                      st.type <= STMT_DBSCALE_GTID_STOP))) {
    LOG_INFO("rein-force-enable-read-only is on,can not execute sql %s.\n",
             sql);
    throw ReinForceReadOnlyError();
  }
  if (is_need_parse_transparent() &&
      !plan->session->is_in_cluster_xa_transaction()) {
    st.need_apply_metadata = false;
    LOG_DEBUG("Sql [%s] parse transparent that type is [%d].\n", sql, st.type);
    plan->set_is_parse_transparent(true);
    assemble_direct_exec_plan(plan, Backend::instance()->get_catalog());
    return;
  }
#ifndef CLOSE_MULTIPLE
  if (generate_forward_master_role_plan(plan)) return;
#endif
  switch (st.type) {
    case STMT_ALTER_TABLE:
      LOG_INFO("Do alter table, the statement is [%s], user is [%s].\n", sql,
               plan->session->get_user_host().c_str());
      break;
    case STMT_DROP_TB:
      LOG_INFO("Do drop table, the statement is [%s], user is [%s].\n", sql,
               plan->session->get_user_host().c_str());
      break;
    case STMT_DROP_DB:
      LOG_INFO("Do alter database, the statement is [%s], user is [%s].\n", sql,
               plan->session->get_user_host().c_str());
      break;
    default:
      break;
  }
#ifndef DBSCALE_TEST_DISABLE
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (strcasecmp(test_info->test_case_name.c_str(), "duplicated_table") ||
      strcasecmp(test_info->test_case_operation.c_str(), "init_space_di")) {
#endif
    if (check_duplicated_table() && !is_cross_node_join() &&
        !is_union_all_sub()) {
      LOG_WARN(
          "Find a dup tmp table stmt for non cross node join, refuse it.\n");
      throw NotSupportedError(
          "Should not execute sql on duplicated tmp table directly");
    }
#ifndef DBSCALE_TEST_DISABLE
  }
#endif
  plan->session->set_select_lock_type(st.select_lock_type);
  bool is_info_schema_mirror_tb_reload_internal_session =
      plan->session->get_is_info_schema_mirror_tb_reload_internal_session();
  stmt_session = plan->session;
#ifndef DBSCALE_TEST_DISABLE
  if (on_test_stmt) {
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "generate_plan") &&
        !strcasecmp(test_info->test_case_operation.c_str(), "exception")) {
      bk->set_dbscale_test_info("", "", "");
      throw Error("generate_plan get exception.");
    }
  }
#endif

  Backend *backend = Backend::instance();
  if (is_partial_parsed() && is_into_outfile()) {
    LOG_ERROR("Cannot support partial parse for SELECT INTO OUTFILE.\n");
    throw NotSupportedError(
        "Cannot support partial parse for SELECT INTO OUTFILE");
  }
  if (is_partial_parsed() && is_user_grant_stmt()) {
    LOG_ERROR("Cannot support partial parse for USER ACCOUNT STMT.\n");
    throw NotSupportedError(
        "Cannot support partial parse for USER ACCOUNT STMT");
  }
  if (is_partial_parsed() && st.type == STMT_SET) {
    if (!force_exec_partial_set) {
      string err(
          "Not support partial parse for set stmt when close option "
          "force-execute-partial-set:");
      if (st.error_message) {
#ifndef DBSCALE_TEST_DISABLE
        string tmp(st.error_message);
        if (tmp.find("mystery character") == string::npos) {
          err.append(st.error_message);
        }
#else
        err.append(st.error_message);
#endif
      }
      LOG_ERROR("%s\n", err.c_str());
#ifndef DBSCALE_TEST_DISABLE
      plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
      throw NotSupportedError(err.c_str());
    }
    assemble_direct_exec_plan(plan, Backend::instance()->get_catalog());
    return;
  }

  table_link *table = st.table_list_head;

  if (!is_partial_parsed() &&
      plan->session->get_session_option("is_bridge_session").bool_val &&
      table) {
    ExecuteNode *node = try_assemble_bridge_mark_sql_plan(plan, table);
    if (node) {
      plan->set_start_node(node);
      return;
    }
  }

  if (st.type == STMT_SET && !table && st.sql->set_oper &&
      !st.sql->set_oper->names) {
    if (assemble_dbscale_cal_uservar_plan(plan)) return;
  }

  if (plan->session->get_session_option("restrict_create_table").int_val != 0) {
    if (st.type == STMT_CREATE_TB) {
      if (is_partial_parsed()) {
        string err("Not support partial parse for create table:");
        if (st.error_message) err.append(st.error_message);
        LOG_ERROR("%s\n", err.c_str());
        throw NotSupportedError(err.c_str());
      }
      if (backend->is_cluster_contain_mgr_datasource()) {
        string se_name;
        if (strlen(st.sql->create_tb_oper->engine_name))
          se_name = st.sql->create_tb_oper->engine_name;
        else
          se_name = backend->get_backend_server_default_storage_engine();
        if (strcasecmp(se_name.c_str(), "InnoDB")) {
          LOG_INFO("Table must use the InnoDB storage engine\n");
          throw Error(ERROR_STORAGE_ENGINE_ERROR_CODE);
        }
      }
      if (!st.sql->create_tb_oper->primary_key_cols &&
          !st.sql->create_tb_oper->all_unique_keys &&
          !st.sql->create_tb_oper->table_like) {
        LOG_INFO("Create Table must specify primary key or unique key\n");
        throw Error(ERROR_CREATE_TB_NO_PRIM_KEY_CODE);
      }
      if (!force_create_table_engine.empty() && st.engine_head) {
        LOG_DEBUG("force_create_table_engine is %s.\n",
                  force_create_table_engine.c_str());
        engine_name_item *ni = st.engine_head;
        string tmp_sql;
        const char *start_pos = sql;
        const char *copy_pos = sql;
        const char *sql_tail = sql + strlen(sql);
        while (ni) {
          if (strcasecmp(ni->engine_name, force_create_table_engine.c_str())) {
            copy_pos = sql + ni->start_pos;
            tmp_sql.append(start_pos, copy_pos - start_pos - 1);
            tmp_sql.append(" engine=");
            tmp_sql.append(force_create_table_engine.c_str());
            start_pos = sql + ni->end_pos;
          }
          ni = ni->next;
        }
        if (start_pos != sql) {
          if (start_pos <= sql_tail) tmp_sql.append(start_pos);
          LOG_DEBUG("Force adjust table engine with sql: %s.\n",
                    tmp_sql.c_str());
          record_statement_stored_string(tmp_sql);
          sql = statement_stored_string.back().c_str();
          Statement *new_stmt = NULL;
          re_parser_stmt(&(st.scanner), &new_stmt, sql);
          table = new_stmt->get_stmt_node()->table_list_head;
        }
      }
    }
    if (st.type == STMT_ALTER_TABLE) {
      if (is_partial_parsed()) {
        string err("Not support partial parse for alter table:");
        if (st.error_message) err.append(st.error_message);
        LOG_ERROR("%s\n", err.c_str());
        throw NotSupportedError(err.c_str());
      }
      if (backend->is_cluster_contain_mgr_datasource() &&
          st.sql->alter_tb_oper && st.sql->alter_tb_oper->engine_name &&
          strcasecmp(st.sql->alter_tb_oper->engine_name, "InnoDB")) {
        LOG_INFO("Table must use the InnoDB storage engine\n");
        throw Error(ERROR_STORAGE_ENGINE_ERROR_CODE);
      }
      if (st.alter_tb_modify_primary_key_counter < 0) {
        LOG_INFO("Not allow to drop table primary key\n");
        throw Error(ERROR_ALTER_TB_NOT_ALLOW_DROP_PRIM_KEY_CODE);
      }
    }
  }

  DataSpace *odbc_space = check_and_replace_odbc_uservar(plan);

  if (odbc_space) {
    assemble_direct_exec_plan(plan, odbc_space);
    return;
  }

  if (st.type == STMT_DBSCALE_CREATE_ORACLE_SEQ ||
      st.type == STMT_DBSCALE_ALTER_ORACLE_SEQ) {
    if (!enable_oracle_sequence) {
      throw Error("oracle sequence feature is disabled");
    }
#ifndef CLOSE_MULTIPLE
    bool is_master = true;
    if (multiple_mode) {
      MultipleManager *mul = MultipleManager::instance();
      is_master = mul->get_is_cluster_master();
    }
    if (st.type == STMT_DBSCALE_ALTER_ORACLE_SEQ &&
        st.sql->create_oracle_seq_oper->alter_what &
            (ALTER_SEQUENCE_MIN | ALTER_SEQUENCE_MAX) &&
        !is_master) {
      LOG_DEBUG(
          "alter sequence min or max should be execute on master dbscale\n");
      assemble_forward_master_role_plan(plan);
    } else {
#endif
      assemble_dbscale_create_oracle_sequence_plan(plan);
#ifndef CLOSE_MULTIPLE
    }
#endif
    return;
  }
  check_and_replace_oracle_sequence(plan, &table);

  if (get_latest_stmt_node()->terminater_semicolon > 0) {
    string new_query_sql_string;
    new_query_sql_string.append(
        sql, get_latest_stmt_node()->terminater_semicolon - 1);
    record_statement_stored_string(new_query_sql_string);
    sql = statement_stored_string.back().c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    table = new_stmt->get_stmt_node()->table_list_head;
  }

  if (get_latest_stmt_node()->full_column_list_head) {
    string new_query_sql_string;
    new_query_sql_string = remove_schema_from_full_columns();
    record_statement_stored_string(new_query_sql_string);
    sql = statement_stored_string.back().c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    table = new_stmt->get_stmt_node()->table_list_head;
  }

  if (st.connect_by_num) {
    bool is_centralized = backend->is_centralized_cluster();
    if (!is_centralized && enable_start_with_connect_by) {
      LOG_ERROR(
          "Only support centralized cluster open config "
          "enable_start_with_connect_by.\n");
      throw Error(
          "Only support centralized cluster open config "
          "enable_start_with_connect_by.");
    }
    if (enable_start_with_connect_by && is_centralized) {
      LOG_DEBUG("start with connect by direct send server\n");
    } else {
      plan->session->set_is_complex_stmt(true);
      if (st.connect_by_num > 1) {
        LOG_ERROR("Only support one connect by in top record scan.\n");
        throw NotSupportedError(
            "Only support one connect by in top record scan.");
      }
      if (!st.scanner->connect_by_oper) {
        LOG_ERROR("Only support connect by in top record scan.\n");
        throw NotSupportedError("Only support connect by in top record scan.");
      }
      connect_by_node *conn_node = st.scanner->connect_by_oper;
      handle_connect_by_expr(conn_node);

      string new_query_sql_string;
      if (st.scanner->condition) {
        new_query_sql_string.append(sql, st.scanner->opt_where_start_pos - 1);
        if (conn_node->cond_start_pos > 0) {
          if (conn_node->cond_start_pos >= conn_node->cond_end_pos) {
            LOG_ERROR("Fail to get connect by conditions.");
            throw Error("Fail to get connect by conditions.");
          }
          new_query_sql_string.append(" WHERE ");
          new_query_sql_string.append(
              sql, conn_node->cond_start_pos - 1,
              conn_node->cond_end_pos - conn_node->cond_start_pos + 1);
        }
        if (conn_node->start_pos - st.scanner->opt_where_end_pos > 1)
          new_query_sql_string.append(
              sql, st.scanner->opt_where_end_pos,
              conn_node->start_pos - st.scanner->opt_where_end_pos - 1);
      } else {
        new_query_sql_string.append(sql, st.scanner->opt_where_start_pos);
        if (conn_node->cond_start_pos > 0) {
          if (conn_node->cond_start_pos >= conn_node->cond_end_pos) {
            LOG_ERROR("Fail to get connect by conditions.");
            throw Error("Fail to get connect by conditions.");
          }
          new_query_sql_string.append(" WHERE ");
          new_query_sql_string.append(
              sql, conn_node->cond_start_pos - 1,
              conn_node->cond_end_pos - conn_node->cond_start_pos + 1);
        }
        if (conn_node->start_pos - st.scanner->opt_where_end_pos > 1)
          new_query_sql_string.append(
              sql, st.scanner->opt_where_end_pos,
              conn_node->start_pos - st.scanner->opt_where_end_pos - 1);
      }
      record_statement_stored_string(new_query_sql_string);
      sql = statement_stored_string.back().c_str();
      Statement *new_stmt = NULL;
      re_parser_stmt(&(st.scanner), &new_stmt, sql);
      table = new_stmt->get_stmt_node()->table_list_head;
    }
  }

  if (is_empty_top_select_with_limit()) {
    move_top_select_limit_to_table_subquery(&table);
  }

  if (handle_executable_comments_before_cnj(plan)) {
    return;
  }
  if (!is_info_schema_mirror_tb_reload_internal_session)
    refuse_modify_table_checking();

#ifndef DBSCALE_TEST_DISABLE
  plan->handler->prepare_gen_plan.start_timing();
#endif
  DataSpace *dataspace = NULL;
  const char *schema_name = NULL, *table_name = NULL;
  table_link *table_tmp = table;

  if (st.type == STMT_REPLICATION) {
    LOG_ERROR("Not support replication related stmt %s.\n", sql);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    throw NotSupportedError("Unsupport replication related stmt.");
  }

  if ((st.type == STMT_CREATE_TB || st.type == STMT_CREATE_SELECT ||
       st.type == STMT_CREATE_LIKE) &&
      st.sql->create_tb_oper->op_tmp == 1) {
    generate_plan_for_create_tmp_table(plan);
    return;
  }
  check_stmt_sql_for_dbscale_row_id(st.type, st.scanner);

  if (!is_info_schema_mirror_tb_reload_internal_session) {
    if (handle_view_related_stmt(
            plan, &table))  // return true means the plan has been generated
      return;
    handle_coalecse_function(&table);
  }

  table_tmp = table;
  if (st.type != STMT_DBSCALE_SHOW_TABLE_LOCATION &&
      st.type != STMT_DBSCALE_REQUEST_CLUSTER_INC_INFO && table) {
    if (enable_block_table) {
      unsigned int tmp_migrate_wait_timeout = migrate_wait_timeout;
      while (!check_migrate_block_table(plan)) {
        if (ACE_Reactor::instance()->reactor_event_loop_done()) {
          LOG_ERROR("generate_plan failed due to reactor_event_loop_done\n");
          throw Error("generate_plan failed due to reactor_event_loop_done");
        }
        if (tmp_migrate_wait_timeout > 0) {
          LOG_INFO("current sql [%s] is waiting for migrate lock\n", sql);
          sleep(1);
          tmp_migrate_wait_timeout--;
        } else {
          throw Error(
              "DBScale can't execute this statement cause table is blocked "
              "during migration.");
        }
      }
    }
    if (st.type != STMT_DBSCALE_MIGRATE && st.type != STMT_DBSCALE_BLOCK)
      while (table_tmp != NULL) {
        schema_name =
            table->join->schema_name ? table->join->schema_name : schema;
        plan->session->add_in_using_table(schema_name,
                                          table_tmp->join->table_name);
        table_tmp = table_tmp->next;
      }
  }
  schema_name = NULL;

#ifndef DBSCALE_TEST_DISABLE
  if (on_test_stmt && check_and_assemble_multi_send_node(plan)) {
    plan->handler->prepare_gen_plan.end_timing_and_report();
    return;
  }
#endif

  if (st.type == STMT_RELEASE_SAVEPOINT ||
      !check_xa_transaction_support_sql(&st, plan->session)) {
    generate_plan_for_non_support_trx_stmt(plan);
    return;
  }

  if (!plan->session->get_is_info_schema_mirror_tb_reload_internal_session())
    check_acl_and_safe_mode(plan);

  if (plan->session->is_in_lock()) {
    check_stmt_for_lock_mode(plan);
  }

  if (!is_info_schema_mirror_tb_reload_internal_session)
    row_count_and_last_insert_id_checking(plan);

  if (st.type == STMT_ALTER_TABLE || st.type == STMT_DROP_DB ||
      st.type == STMT_DROP_TB) {
    mark_table_info_to_delete(plan);
    if (st.type == STMT_DROP_TB) {
      const char *schema_name;
      const char *table_name;
      table_link *table = st.table_list_head;
      if (st.sql->drop_tb_oper->op_tmp == 1) {
        st.is_create_or_drop_temp_table = true;
      }

      if (!st.sql->drop_tb_oper->op_tmp) {
        int tmp_table_count = 0;
        int total_table_count = 0;
        while (table) {
          schema_name =
              table->join->schema_name ? table->join->schema_name : schema;
          table_name = table->join->table_name;
          if (plan->session->is_temp_table(schema_name, table_name)) {
            ++tmp_table_count;
          }
          table = table->next;
          ++total_table_count;
        }
        if (tmp_table_count > 0 && tmp_table_count != total_table_count) {
          throw NotSupportedError("User temp table should be dropped alone.");
        }
        if (tmp_table_count > 0) {
          st.is_create_or_drop_temp_table = true;
        } else {
          // dropping multi non-tmp tables
          if (enable_table_recycle_bin && total_table_count > 1) {
            throw NotSupportedError(
                "when using enable_table_recycle_bin, tables should be dropped "
                "one by one.");
          }
        }
      }
    }
    if (st.type == STMT_ALTER_TABLE) {
      table_link *table = st.table_list_head;
      const char *schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      const char *table_name = table->join->table_name;
      if (plan->session->is_temp_table(schema_name, table_name))
        st.is_create_or_drop_temp_table = true;
    }
  }

  if (st.type == STMT_DBSCALE_EXPLAIN || st.type == STMT_EXPLAIN) {
    generate_plan_for_explain_related_stmt(plan);
    return;
  }

  if (!is_info_schema_mirror_tb_reload_internal_session)
    if (plan->session->get_is_federated_session() ||
        !plan->session->get_has_check_first_sql())
      if (generate_plan_for_federate_session(plan, table)) return;

  if (st.var_item_list != NULL) {
    LOG_DEBUG(
        "The sql [%s] has user variable, build a set sql for the session.\n",
        sql);
    var_item *var_item_list = st.var_item_list;
    build_uservar_sql(var_item_list, plan);
    plan->session->set_var_flag(true);
  } else {
    plan->session->set_var_flag(false);
  }

  if (st.type == STMT_SELECT && st.select_plain_value &&
      !is_handle_sub_query() && !st.sql_contain_charset_str && st.scanner &&
      st.scanner->field_list_head && st.scanner->field_list_head->field_expr &&
      is_simple_expression(st.scanner->field_list_head->field_expr)) {
    const char *str_value =
        get_simple_expression_value(st.scanner->field_list_head->field_expr);
    const char *alias_name = st.scanner->field_list_head->alias;
    if (st.scanner->field_list_head->field_expr->type == EXPR_BOOL) {
      alias_name = st.scanner->field_list_head->field_expr->get_bool_value()
                       ? "TRUE"
                       : "FALSE";
    }
    assemble_select_plain_value_plan(plan, str_value, alias_name);
    return;
  }

  table = adjust_select_sql_for_funs_and_autocommit(plan, table);

  if (st.type == STMT_EXEC_PREPARE || st.type == STMT_PREPARE ||
      st.type == STMT_DROP_PREPARE) {
    generate_plan_for_prepare_related_stmt(plan);
    return;
  }

  if (st.type == STMT_DBSCALE_FLUSH_CONFIG_TO_FILE) {
    generate_plan_for_flush_config_to_file(plan);
    return;
  }

  if (st.type == STMT_DBSCALE_FLUSH_TABLE_INFO) {
    const char *schema_name =
        st.sql->dbscale_flush_table_info_oper->schema_name;
    const char *table_name = st.sql->dbscale_flush_table_info_oper->table_name;
    generate_plan_for_flush_table_info(plan, schema_name, table_name);
    return;
  }

  if (st.type == STMT_DBSCALE_FLUSH_ACL) {
    bool silently = st.sql->dbscale_flush_acl_oper->silently;
    if (!silently && !enable_acl) {
      LOG_ERROR("enable-acl=0, no ACL to flush\n");
      throw Error("enable-acl=0, no ACL to flush");
    }
    generate_plan_for_flush_acl(plan);
    return;
  }

  adjust_sql_for_into_file_or_select_into(&table);

  defined_lock_type dl_type = st.defined_lock;
  if (dl_type != DEFINED_LOCK_NON) {
    if (st.scanner->defined_lock_num > 1) {
      LOG_INFO("does not support multiple get_lock() or release_lock()\n");
      throw NotSupportedError(
          "does not support multiple get_lock() or release_lock().");
    }
    field_item *field = st.scanner->field_list_head;
    bool use_alias_name = false;
    int defined_lock_func_num = 0;
    while (field) {
      if (field->field_expr && field->field_expr->type == EXPR_FUNC) {
        FunctionExpression *func_expr =
            (FunctionExpression *)(field->field_expr);
        if (func_expr->name) {
          const char *value = func_expr->name->str_value;
          if (!strcasecmp(value, "get_lock") ||
              !strcasecmp(value, "release_lock") ||
              !strcasecmp(value, "release_all_locks")) {
            defined_lock_func_num++;
            if (field->alias) {
              use_alias_name = true;
              plan->session->set_defined_lock_field_name(field->alias);
            }
          }
        }
      }
      field = field->next;
    }
    if (defined_lock_func_num != 1) {
      LOG_INFO("does not support complex with get_lock() or release_lock()\n");
      throw NotSupportedError(
          "does not support complex with get_lock() or release_lock().");
    }
    if (!use_alias_name) {
      int head_pos = 0, tail_pos = 0;
      head_pos =
          plan->statement->get_stmt_node()->scanner->defined_lock_head_pos;
      tail_pos =
          plan->statement->get_stmt_node()->scanner->defined_lock_tail_pos;
      string str = sql;
      str = str.substr(head_pos - 1, tail_pos - head_pos + 1);
      plan->session->set_defined_lock_field_name(str.c_str());
      LOG_DEBUG("The defined lock field is %s\n", str.c_str());
    }
  }

  if (st.table_list_head &&
      (st.type == STMT_SELECT || st.type == STMT_UPDATE ||
       st.type == STMT_DELETE || st.type == STMT_INSERT)) {
  } else {
    if (generate_plan_for_no_table_situation(plan)) return;
  }

  if (is_partial_parsed() && st.type != STMT_CREATE_TB &&
      st.type != STMT_ALTER_TABLE && st.type != STMT_DBSCALE_ESTIMATE) {
    generate_plan_for_partial_parse_stmt(plan, schema_name, table_name, table);
    return;
  }

  if (st.type == STMT_RENAME_TABLE || st.type == STMT_DROP_TB) {
    generate_plan_for_rename_or_drop_table_stmt(plan);
    return;
  }
  if (st.type == STMT_CREATE_TB) {
    if (prepare_dataspace_for_extend_create_tb_stmt(plan, table)) return;
  }
  DataSpace *insert_dataspace = NULL;
  bool only_related_one_server = true;
  DataSpace *one_server_dataspace = NULL;
  DataSpace *one_space = NULL;
  record_scan *one_rs = NULL;
  bool only_one_table = false;
  table_link *one_table = NULL;
  if (table && !table->next) {
    only_one_table = true;
    one_table = table;
  }

  if (plan->session->is_in_transaction() && st.type == STMT_SELECT &&
      plan->session->get_select_lock_type()) {
    set<DataSpace *> shard_table;
    set<DataSpace *> non_shard_table;
    DataSpace *tmp_dataspace = NULL;
    table_link *tmp_table = st.table_list_head;
    while (tmp_table) {
      schema_name =
          tmp_table->join->schema_name ? tmp_table->join->schema_name : schema;
      table_name = tmp_table->join->table_name;
      if (schema_name && !strcmp(schema_name, "information_schema")) {
        tmp_table = tmp_table->next;
        continue;
      } else {
        tmp_dataspace =
            backend->get_data_space_for_table(schema_name, table_name);
      }
      if (!tmp_dataspace->get_data_source()) {
        if (!((Table *)tmp_dataspace)->is_duplicated()) {
          PartitionedTable *tmp_par_ds = (PartitionedTable *)tmp_dataspace;
          if (tmp_par_ds->get_partition_scheme()->is_shard()) {
            shard_table.insert(tmp_dataspace);
          }
        }
      } else {
        DataSource *ds = tmp_dataspace->get_data_source();
        if (ds && ds->get_data_source_type() == DATASOURCE_TYPE_REPLICATION) {
          ReplicationDataSource *rep_datasource =
              dynamic_cast<ReplicationDataSource *>(tmp_dataspace->data_source);
          if (rep_datasource && rep_datasource->has_slave_source()) {
            non_shard_table.insert(tmp_dataspace);
          }
        }
      }
      tmp_table = tmp_table->next;
    }
    if (shard_table.size() && non_shard_table.size()) {
      std::set<DataSpace *>::iterator it1;
      std::set<DataSpace *>::iterator it2;
      for (it1 = shard_table.begin(); it1 != shard_table.end(); ++it1) {
        for (it2 = non_shard_table.begin(); it2 != non_shard_table.end();
             ++it2) {
          if (stmt_session->is_dataspace_cover_session_level(*it1, *it2)) {
            LOG_ERROR(
                "Does not support SELECT sql global table JOIN shard table "
                "with FOR UPDATE or LOCK IN SHARE MODE\n");
            throw NotSupportedError(
                "Does not support SELECT sql global table JOIN shard table "
                "with FOR UPDATE or LOCK IN SHARE MODE.");
          }
        }
      }
    }
  }

  bool has_non_shard_table = false;
  bool has_shard_table = false;
  if (table && table->join) {
    schema_name = table->join->schema_name ? table->join->schema_name : schema;
    table_name = table->join->table_name;
    check_create_or_alter_table_restrict(schema_name, table_name);
  }
  Catalog *catalog = backend->get_default_catalog();
  const char *catalog_source_name = catalog->get_data_source()->get_name();
  while (table) {
    schema_name = table->join->schema_name ? table->join->schema_name : schema;
    table_name = table->join->table_name;

    if (st.type == STMT_SELECT &&
        !strcasecmp(schema_name, "information_schema")) {
      generate_plan_for_information_schema(plan, table_name, table);
      return;
    }

    record_scan *rs_table = table->join->cur_rec_scan;
    if ((st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT) &&
        !strcasecmp(schema_name, "information_schema")) {
      dataspace = backend->get_auth_data_space();
    } else {
      dataspace = backend->get_data_space_for_table(schema_name, table_name);
    }
    if (table->join->schema_name == NULL && !is_union_table_sub() &&
        !is_cross_node_join()) {
      plan->session->add_table_without_given_schema(dataspace);
    }
    if (only_one_table)  // if only_one_table, there is no need to maintain
                         // record_scan_all_table_spaces.
      one_space = dataspace;
    else {
      record_scan_all_table_spaces[table] = dataspace;
      need_clean_record_scan_all_table_spaces = true;
    }

    if (!dataspace->get_data_source()) {
      if (DEFINED_LOCK_NON != dl_type) {
        LOG_INFO(
            "does not support get_lock() or release_lock() from partition "
            "table\n");
        throw NotSupportedError(
            "Does not support get_lock() or release_lock() from partition "
            "table.");
      }
      if (!((Table *)dataspace)->is_duplicated()) {
        PartitionedTable *tmp_par_ds = (PartitionedTable *)dataspace;
        PartitionType part_type = tmp_par_ds->get_partition_type();
        if (tmp_par_ds->get_partition_scheme()->is_shard())
          has_shard_table = true;
        if (part_type == PARTITION_TYPE_RANGE) {
          rs_table->need_range = true;
        }
        unsigned int n = tmp_par_ds->get_real_partition_num();
        if (n > 1) {
          if (!only_one_table) {  // if only has one table, there is no need to
                                  // maintain the record_scan_all_par_tables_map
            record_scan_all_par_tables_map[rs_table].push_back(table);
            need_clean_record_scan_all_par_tables_map = true;
          } else {
            one_rs =
                rs_table;  // record the record scan of one table, if can not
                           // execute on one table mode later, we should re-fill
                           // the record_scan_all_par_tables_map
          }
        } else if (n == 1) {
          record_scan_all_spaces_map[rs_table].push_back(
              tmp_par_ds->get_partition(0));
          need_clean_record_scan_all_spaces_map = true;
          dataspace = tmp_par_ds->get_partition(0);
        } else {
#ifdef DEBUG
          ACE_ASSERT(0);  // Should not be here
#endif
        }
        // TODO: for partition table, we should check whether it only operate
        // one partition.
        if (n != 1 && only_related_one_server) only_related_one_server = false;

      } else {
        if (!only_one_table) {  // if only has one table, there is no need to
                                // maintain the record_scan_all_par_tables_map
          record_scan_all_par_tables_map[rs_table].push_back(table);
          need_clean_record_scan_all_par_tables_map = true;
        } else {
          one_rs = rs_table;  // record the record scan of one table, if can not
                              // execute on one table mode later, we should
                              // re-fill the record_scan_all_par_tables_map
        }
        only_related_one_server = false;
      }
    } else {
      const char *tb_source_name = dataspace->data_source->get_name();
      if (DEFINED_LOCK_NON != dl_type &&
          strcasecmp(catalog_source_name, tb_source_name)) {
        LOG_INFO("the table on data source [%s] instead of catalog\n",
                 tb_source_name);
        throw NotSupportedError(
            "Does not support get_lock() or release_lock() from non catalog "
            "table.");
      }
      has_non_shard_table = true;

      if (!is_readonly() && dataspace->data_source->get_data_source_type() ==
                                DATASOURCE_TYPE_READ_ONLY) {
        LOG_ERROR("Unsupport execute write op on read_only datasource[%s].\n",
                  dataspace->data_source->get_name());
        string tmp("Unsupport execute write op on read_only datasources:");
        tmp.append(dataspace->data_source->get_name());
        throw ReadOnlyDatasourceWriteFail(tmp.c_str());
      }

      record_scan_all_spaces_map[rs_table].push_back(dataspace);
      need_clean_record_scan_all_spaces_map = true;
    }

    if (only_related_one_server) {
      if (!one_server_dataspace) {
        one_server_dataspace = dataspace;
        if (st.type == STMT_INSERT_SELECT) insert_dataspace = dataspace;
      } else {
        only_related_one_server = false;
        if (is_info_schema_mirror_tb_reload_internal_session) {
          // insert_dataspace is dbscale; one_server_dataspace is Auth dataspace
          only_related_one_server = true;
          one_server_dataspace = dataspace;
        } else if (is_share_same_server(one_server_dataspace, dataspace)) {
          only_related_one_server = true;
        } else if (stmt_session->is_dataspace_cover_session_level(
                       one_server_dataspace, dataspace)) {
          only_related_one_server = true;
        } else if ((st.type == STMT_SELECT || st.type == STMT_INSERT_SELECT) &&
                   stmt_session->is_dataspace_cover_session_level(
                       dataspace, one_server_dataspace)) {
          only_related_one_server = true;
          one_server_dataspace = dataspace;
        }
      }
    }

    table = table->next;
  }

  if (insert_dataspace && only_related_one_server &&
      (insert_dataspace != one_server_dataspace)) {
    only_related_one_server = false;
    spaces.clear();
    spaces.push_back(one_server_dataspace);
    need_clean_spaces = true;
    handle_no_par_table_one_spaces(plan);
    return;
  }

  if (plan->session->is_in_lock() && has_non_shard_table && has_shard_table) {
    if (st.type == STMT_SELECT || st.type == STMT_UPDATE ||
        st.type == STMT_DELETE || st.type == STMT_INSERT_SELECT ||
        st.type == STMT_REPLACE_SELECT) {
      LOG_ERROR(
          "Does not support DML sql mix with shard and non-shard table in lock "
          "mode.\n");
      throw NotSupportedError(
          "Does not support DML sql mix with shard and non-shard table in lock "
          "mode.");
    }
  }

  if (only_related_one_server && !union_table_sub) {
    par_table_num = 0;
    spaces.clear();
    spaces.push_back(one_server_dataspace);
    need_clean_spaces = true;
  } else if (st.scanner->is_select_union && st.scanner->opt_union_all &&
             union_all_sub) {
    if (!record_scan_all_spaces_map.empty()) {
      map<record_scan *, list<DataSpace *> >::iterator it_space =
          record_scan_all_spaces_map.begin();
      DataSpace *union_space = it_space->second.front();
      par_table_num = 0;
      spaces.clear();
      spaces.push_back(union_space);
      need_clean_spaces = true;
    } else {
#ifdef DEBUG
      ACE_ASSERT(0);  // Should not be here
#endif
    }

  } else if (only_one_table && can_use_one_table_plan(plan, one_space, &st)) {
    generate_plan_for_one_table_situation(plan, one_table, one_space);
    return;
  } else {
#ifndef DBSCALE_DISABLE_SPARK
    if (!only_one_table && !is_spark_one_partition_sql()) {
      bool need_spark_session =
          plan->session->get_session_option("use_spark").int_val;
      need_spark =
          need_spark_session ? need_spark_session : st.execute_on_spark;
      if (need_spark && st.type == STMT_SELECT && !union_all_sub &&
          !st.scanner->is_select_union) {
        analysis_spark_sql_join(plan);
        return;
      }
      if (need_spark && st.type == STMT_INSERT_SELECT && !union_all_sub &&
          !st.scanner->is_select_union) {
        spark_insert = true;
        table_link *modify_tb = get_one_modify_table();
        const char *schema_name = modify_tb->join->schema_name
                                      ? modify_tb->join->schema_name
                                      : schema;
        const char *table_name = modify_tb->join->table_name;
        DataSpace *modify_ds =
            backend->get_data_space_for_table(schema_name, table_name);
        DataSpace *select_ds =
            backend->get_data_space_for_table(spark_dst_schema.c_str(), NULL);

        record_scan_all_table_spaces[modify_tb] = modify_ds;
        par_table_num = 0;
        if (modify_ds->is_partitioned()) {
          ++par_table_num;
          par_tables.push_back(modify_tb);
          need_clean_par_tables = true;
          record_scan_par_table_map[st.scanner] = modify_tb;
        }
        table_link *select_tb = modify_tb->next;
        record_scan_all_table_spaces[select_tb] = select_ds;

        spaces.push_back(select_ds);
        spaces.push_back(modify_ds);
      }
    }
    if (!spark_insert)
#endif

      pre_work_for_non_one_table_exec_plan(plan, only_one_table, one_rs,
                                           one_space, one_table);
  }

  if (st.var_item_list != NULL) {
    LOG_DEBUG(
        "The sql [%s] has user variable, build a set"
        " sql for the session.\n",
        sql);
    var_item *var_item_list = st.var_item_list;
    build_uservar_sql(var_item_list, plan);
    plan->session->set_var_flag(true);
  } else {
    plan->session->set_var_flag(false);
  }

  if ((!only_related_one_server ||
       (only_related_one_server && union_table_sub)) &&
      st.scanner->is_select_union && st.scanner->opt_union_all &&
      !union_all_sub) {
    plan->session->set_is_complex_stmt(true);
    rebuild_union_all_sql_and_generate_execution_plan(plan);
#ifndef DBSCALE_TEST_DISABLE
    plan->handler->prepare_gen_plan.end_timing_and_report();
#endif
    return;
  }
  if (st.type == STMT_SET) {
    generate_plan_for_set_stmt(plan);
    return;
  }

  if (handle_executable_comments_after_cnj(plan)) {
    return;
  }

#ifndef DBSCALE_TEST_DISABLE
  plan->handler->prepare_gen_plan.end_timing_and_report();
#endif

  if (generate_plan_according_to_par_table_num_and_spaces(plan)) return;

  // Should not be here
  ACE_ASSERT(0);
}

/* Get the identify columns of one table for generate_sub_sqls(), it could be
 * primary key columns for most case. And this function also do some checks to
 * ensure the sql is executable, basing on the identify columns and sql type.
 *
 * Return   HAS_IDENTIFY_COLUMNS :
 *                 means the modify sql can be executed part by part.
 *          NO_IDENTIFY_COLUMNS :
 *                 means there is no primary key, so the modify sql only can be
 *executed as whole.
 **/
int Statement::get_identify_columns(const char *schema_name,
                                    const char *table_name,
                                    const char *table_alias,
                                    vector<string *> *columns) {
  Backend *backend = Backend::instance();
  DataSpace *space = backend->get_data_space_for_table(schema_name, table_name);
  bool is_par_table = backend->table_is_partitioned(schema_name, table_name);
  int ret = HAS_IDENTIFY_COLUMNS;

  if (is_par_table) {
    // Add partition keys into the front of column list
    PartitionedTable *par_table = (PartitionedTable *)space;
    vector<const char *> *key_names = par_table->get_key_names();
    vector<const char *>::iterator it = key_names->begin();
    for (; it != key_names->end(); ++it) {
      columns->push_back(new string(*it));
    }

    space = ((PartitionedTable *)space)->get_partition(0);
  }

  // TODO: Retry if got exception
  Connection *conn = NULL;
  try {
    conn = space->get_connection(get_session(), schema_name, true);
    if (!conn) {
      throw Error("Fail to get connection for getting identify columns.");
    }
    int r = 0;
    bool found_key = false;

    r = conn->get_identify_columns_name(schema_name, table_name, true, columns,
                                        &found_key);
    if (!found_key && !r) {
      r = conn->get_identify_columns_name(schema_name, table_name, false,
                                          columns, &found_key);

      /*For update sql, if there is no primary key(s), it must be executed as
       * a whole.*/
      if (st.type == STMT_UPDATE ||
          (st.type == STMT_DELETE && st.cur_rec_scan->order_by_list))
        ret = NO_IDENTIFY_COLUMNS;
    }
    if (r) {
      conn->get_pool()->add_back_to_dead(conn);
      conn = NULL;
      throw Error("Fail to get identify columns.");
    }
    conn->get_pool()->add_back_to_free(conn);
    conn = NULL;

    if (!found_key) {
      LOG_ERROR(
          "Unsupport sql [%s], cannot find the column info for the "
          "modify table.\n",
          sql);
      throw UnSupportPartitionSQL(
          "cannot find the column info for the modify table");
    }
  } catch (Exception &e) {
    if (conn) conn->get_pool()->add_back_to_dead(conn);
    LOG_ERROR(
        "Got exception [%s] when try to get the identify columns of table"
        " [%s.%s] in sql [%s].\n",
        e.what(), schema_name, table_name, sql);
    throw;
  }

  if (is_par_table && ret == HAS_IDENTIFY_COLUMNS && st.type == STMT_UPDATE) {
    Expression *set_list = st.sql->update_oper->update_set_list;
    string key_value = "";
    if (set_list_contain_key(set_list, columns, schema_name, table_name,
                             table_alias, key_value)) {
      LOG_ERROR(
          "Unsupport sql [%s], cannot modify the primary key for dbscale "
          "currently.\n",
          sql);
      throw UnSupportPartitionSQL(
          "cannot modify the primary key for dbscale currently");
    }
  }
  return ret;
}

void Statement::handle_federated_join_method() {
  if (record_scan_join_tables.count(st.scanner) &&
      record_scan_join_tables[st.scanner]) {
    vector<vector<TableStruct *> *> *join_table_sequence =
        record_scan_join_tables[st.scanner];
    int size = join_table_sequence->size();
    /* We can only support 2 table federated method */
    if (size == 2) {
      exec_nodes.at(0)->update_join_method(DATA_MOVE_READ);
    }
  }
}

table_link *Statement::find_table_link(const char *table_full_name,
                                       record_scan *rs, bool ignore_schema) {
  if (!table_full_name) return NULL;
  table_link *tmp = rs->first_table;
  string s1, s2;

  while (tmp && tmp->join->cur_rec_scan == rs) {
    s1.clear();
    if (tmp->join->alias) {
      s1.append(tmp->join->alias);
    } else {
      if (tmp->join->schema_name && !ignore_schema) {
        s1.append(tmp->join->schema_name);
        s1.append(".");
      }
      s1.append(tmp->join->table_name);
    }
    if (lower_case_table_names) {
      if ((strlen(table_full_name) == s1.size() &&
           !strcasecmp(table_full_name, s1.c_str()))) {
        return tmp;
      }
    } else {
      if ((strlen(table_full_name) == s1.size() &&
           !strcmp(table_full_name, s1.c_str()))) {
        return tmp;
      }
    }
    tmp = tmp->next;
  }
  return NULL;
}

unsigned int Statement::get_select_part_start_pos(record_scan *rs) {
  if (rs->where_pos) return rs->where_pos;
  if (rs->order_pos) return rs->order_pos;
  if (rs->limit_pos) return rs->limit_pos;
  LOG_ERROR(
      "Not support multiple-table update/delete without where condition.\n");
  throw NotSupportedError(
      "Not support multiple-table update/delete without where condition.");
}

/*Generate one select sql and one modify sql(partial) according to the
 * original sql.
 *
 * Only accept one modfiy table!
 *
 * Return 1. fullfil the identify column of the modify table;
 *        2. table_link of the modify table;
 *        3. return a bool to indicate whether the modify sql can be executed
 *part by part.
 **/
bool Statement::generate_two_phase_modify_sub_sqls(vector<string *> *columns,
                                                   table_link *modify_table) {
  int ret = NO_IDENTIFY_COLUMNS;
  stmt_type type = st.type;
  select_sub_sql.clear();
  modify_sub_sql.clear();
  string select_head("SELECT ");

  if (type == STMT_INSERT_SELECT || type == STMT_REPLACE_SELECT) {
    record_scan *insert_select = st.scanner->children_begin;
    unsigned int start_pos = insert_select->start_pos;
    unsigned int end_pos = insert_select->end_pos;
    unsigned int insert_end_pos = start_pos;
    while (insert_end_pos > 1 && (*(sql + insert_end_pos - 2) == ' ' ||
                                  *(sql + insert_end_pos - 2) == '('))
      insert_end_pos--;
    select_sub_sql.append(sql + start_pos - 1, end_pos - start_pos + 1);

    if (st.sql->insert_oper->insert_column_list &&
        auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
      modify_sub_sql.append(sql, st.sql->insert_oper->column_list_end_pos);
      modify_sub_sql.append(", ");
      modify_sub_sql.append(auto_inc_key_name);
      modify_sub_sql.append(") VALUES ");
      LOG_DEBUG(
          "Auto_increment field is not specified in column list,"
          " append it to modify_sub_sql internally.\n");
    } else {
      modify_sub_sql.append(sql, insert_end_pos - 1);
      modify_sub_sql.append(" VALUES ");
    }
  } else {
    const char *schema_name = modify_table->join->schema_name
                                  ? modify_table->join->schema_name
                                  : schema;
    const char *table_name = modify_table->join->table_name;
    const char *table_alias = modify_table->join->alias;

    ret = get_identify_columns(schema_name, table_name, table_alias, columns);
    /*If there is no primary key, we are not able to guarantee the limit num.
     * For example: table t1 (c1 int) with data (1 , 2 ,2).
     *
     *  The sql is:
     *
     *   update t1 set c1 = 10 where c1 in (select a from t_other) limit 2.
     *
     *   We can get two c1 values (1, 2) by executing the select sql:
     *
     *   select c1 from t1 where c1 in (select a from t_other) limit 2.
     *
     *   But the generated update sql:
     *
     *    update t1 set c1=10 where c1 =1 or c1=2.
     *
     *   will update 3 records of t1.*/
    if (ret == NO_IDENTIFY_COLUMNS && modify_table->join->cur_rec_scan->limit) {
      LOG_ERROR(
          "Unsupport SQL [%s], modify SQL no identify column but"
          " with limit, which is not support by dbscale currently.\n",
          sql);
      throw UnSupportPartitionSQL(
          "modify SQL no identify column but with limit, which is not support "
          "by dbscale currently");
    }

    vector<string *>::iterator it = columns->begin();
    if (table_alias) {
      select_head += table_alias;
    } else {
      select_head += table_name;
    }
    select_head += ".";
    select_head += (*it)->c_str();
    ++it;
    for (; it != columns->end(); ++it) {
      select_head += ",";
      if (table_alias) {
        select_head += table_alias;
      } else {
        select_head += table_name;
      }
      select_head += ".";
      select_head += (*it)->c_str();
    }
    select_head += " ";

    switch (type) {
      case STMT_UPDATE: {
        if (modify_table->join->cur_rec_scan->join_tables == nullptr) {
          string err = "fail to get tables for sql ";
          LOG_ERROR("%s [%s]\n", err.c_str(), sql);
          throw Error(err.c_str());
        }
        if (modify_table->join->cur_rec_scan->join_tables->type ==
            JOIN_NODE_SINGLE) {
          Expression *update_set_list = st.sql->update_oper->update_set_list;
          ListExpression *expr_list = (ListExpression *)update_set_list;
          expr_list_item *head = expr_list->expr_list_head;
          expr_list_item *set_contain_subquery = head;
          CompareExpression *cmp_tmp = NULL;
          Expression *set_expr = NULL;
          int subquery_num = 0;
          do {
            cmp_tmp = (CompareExpression *)head->expr;
            set_expr = cmp_tmp->right;
            if (set_expr->type == EXPR_SUBSELECT) {
              if (subquery_num == 1) {
                LOG_ERROR("update set only support one subquery now. \n");
                throw Error("update set only support one subquery now.");
              }
              subquery_num += 1;
              set_contain_subquery = head;
            }
            head = head->next;
          } while (head != expr_list->expr_list_head);
          if (subquery_num == 1) {
            cmp_tmp = (CompareExpression *)set_contain_subquery->expr;
            set_expr = cmp_tmp->right;
            int start_pos = modify_table->join->cur_rec_scan->sub_start_pos;
            int end_pos = modify_table->join->cur_rec_scan->sub_end_pos;
            select_sub_sql.append(sql, start_pos, end_pos - start_pos - 1);
            modify_sub_sql.append(sql, 0, cmp_tmp->left->end_pos);
            modify_sub_sql.append(DBSCALE_TMP_COLUMN_VALUE);
            modify_sub_sql.append(sql, end_pos, strlen(sql) - end_pos + 1);
            is_update_set_subquery = true;
          } else {
            unsigned int pos = get_select_part_start_pos(st.scanner);
            select_sub_sql += select_head;
            select_sub_sql += "from ";
            select_sub_sql += schema_name;
            select_sub_sql += ".";
            select_sub_sql += table_name;
            select_sub_sql += " ";
            select_sub_sql.append(sql + pos - 1, strlen(sql) - pos + 1);

            modify_sub_sql.append(sql, pos - 1);
            modify_sub_sql += " where ";
          }
          ret = NO_IDENTIFY_COLUMNS;
        } else if (modify_table->join->cur_rec_scan->join_tables->type ==
                   JOIN_NODE_JOIN) {
          unsigned int table_pos = st.sql->update_oper->update_table_pos;
          unsigned int set_start_pos =
              st.sql->update_oper->update_set_start_pos;
          unsigned int set_end_pos = st.sql->update_oper->update_set_end_pos;
          ListExpression *set_list =
              (ListExpression *)(st.sql->update_oper->update_set_list);
          expr_list_item *item = set_list->expr_list_head;
          list<string> set_value_list;
          bool is_set_value_list_contain_field = false;
          // check whether set list contains filed expression.
          do {
            if (item->expr->type == EXPR_EQ) {
              Expression *value_expr = ((CompareExpression *)item->expr)->right;
              if (value_expr->type == EXPR_SUBSELECT) {
                throw Error(
                    "Not support cross node join update sql set list contain "
                    "select field.");
              }
              if (is_expression_contain_aggr(value_expr,
                                             modify_table->join->cur_rec_scan,
                                             false, true)) {
                is_set_value_list_contain_field = true;
              }
              string value_string;
              value_expr->to_string(value_string);
              set_value_list.push_back(value_string);
            } else {
              LOG_ERROR("Unknown update set expression for sql [%s]\n", sql);
              throw Error("Unknown update set expression for sql");
            }
            item = item->next;
          } while (item != set_list->expr_list_head);

          /*set list does not contain field, use modify select deal this sql
            deal sql like "UPDATE t2 DP INNER JOIN ( SELECT* from t1   ) X  SET
            DP.c2 = 2   WHERE DP.c1 = X.c1 AND DP.c2 = X.c2;" select_sub_sql:
            [SELECT test.DP.c1  FROM t2 DP INNER JOIN ( SELECT* from t1   ) X
                             WHERE DP.c1 = X.c1 AND DP.c2 = X.c2]
            update_sub_sql: [UPDATE test.t2 AS DP SET  DP.c2 = 2    WHERE ]
           */
          if (!is_set_value_list_contain_field) {
            select_sub_sql += select_head;
            select_sub_sql += " FROM ";
            select_sub_sql.append(sql + table_pos - 1,
                                  set_start_pos - table_pos);
            select_sub_sql.append(sql + set_end_pos, strlen(sql) - set_end_pos);
            LOG_DEBUG("select_sub_sql [%s]\n", select_sub_sql.c_str());

            modify_sub_sql = "UPDATE ";
            modify_sub_sql += schema_name;
            modify_sub_sql += ".";
            modify_sub_sql += table_name;
            if (table_alias) {
              modify_sub_sql += " AS ";
              modify_sub_sql += table_alias;
            }
            modify_sub_sql += " ";
            modify_sub_sql.append(sql + set_start_pos - 1,
                                  set_end_pos - set_start_pos + 1);
            modify_sub_sql += " WHERE ";
            LOG_DEBUG("modify_sub_sql [%s]\n", modify_sub_sql.c_str());
          } else {
            /*set list contain field, use SeparatedExecNode deal this sql.
              deal sql like "UPDATE t2 DP INNER JOIN ( SELECT* from t1   ) X SET
              DP.c2 = X.c1 * X.c2, DP.c1 = DP.c1 + 10 WHERE DP.c1 = X.c1 AND
              DP.c2 = X.c2" create_sql     [create table
              dbscale_tmp.dbscale_update_tmp (dbscale_tmp_column_key_0 int, ...,
                              dbscale_tmp_column_value_0 int, ...)]
              select_sub_sql [SELECT test.DP.c1 , X.c1 * X.c2 as
              dbscale_tmp_column_value0 , DP.c1 + 10 as
              dbscale_tmp_column_value1 FROM t2 DP INNER JOIN ( SELECT* from t1
              ) X  WHERE DP.c1 = X.c1 AND DP.c2 = X.c2] modify_sub_sql [UPDATE
              test.t2 AS DP, dbscale_tmp.dbscale_update_tmp set DP.c2 =
              dbscale_tmp_column_value0 , DP.c1 = dbscale_tmp_column_value1
                              WHERE  test.DP.c1 =
              dbscale_tmp.dbscale_update_tmp.dbscale_tmp_column_key0 ]*/

            // generate select_sub_sql
            select_sub_sql += select_head;
            list<string>::iterator it = set_value_list.begin();
            int index = 0;
            for (; it != set_value_list.end(); ++it) {
              char column_alis[100];
              size_t len = sprintf(column_alis, ", %s as %s%d ", it->c_str(),
                                   DBSCALE_TMP_COLUMN_VALUE, index);
              select_sub_sql.append(column_alis, len);
              ++index;
            }
            select_sub_sql += " FROM ";
            select_sub_sql.append(sql + table_pos - 1,
                                  set_start_pos - table_pos);
            select_sub_sql.append(sql + set_end_pos, strlen(sql) - set_end_pos);
            LOG_DEBUG("select_sub_sql [%s]\n", select_sub_sql.c_str());

            // generate modify_sub_sql
            modify_sub_sql = "UPDATE ";
            modify_sub_sql += schema_name;
            modify_sub_sql += ".";
            modify_sub_sql += table_name;
            if (table_alias) {
              modify_sub_sql += " AS ";
              modify_sub_sql += table_alias;
            }
            modify_sub_sql += ", ";
            modify_sub_sql += DBSCALE_TMP_UPDATE_TABLE;
            modify_sub_sql += " set ";
            item = set_list->expr_list_head;
            index = 0;
            do {
              Expression *key_expr = ((CompareExpression *)item->expr)->left;
              string key_str;
              key_expr->to_string(key_str);
              modify_sub_sql += key_str;

              char set_update_tmp_value[100];
              size_t len = sprintf(set_update_tmp_value, " = %s%d , ",
                                   DBSCALE_TMP_COLUMN_VALUE, index);
              modify_sub_sql.append(set_update_tmp_value, len);
              ++index;

              item = item->next;
            } while (item != set_list->expr_list_head);

            boost::erase_tail(modify_sub_sql, 2);
            modify_sub_sql += " WHERE ";

            string modify_table_name;
            modify_table_name += schema_name;
            modify_table_name += ".";
            if (table_alias) {
              modify_table_name += table_alias;
            } else {
              modify_table_name += table_name;
            }

            index = 0;
            vector<string *>::iterator it_c = columns->begin();
            for (; it_c != columns->end(); ++it_c) {
              char update_where_tmp[100];
              size_t len = sprintf(update_where_tmp, " %s.%s = %s.%s%d and ",
                                   modify_table_name.c_str(), (*it_c)->c_str(),
                                   DBSCALE_TMP_UPDATE_TABLE,
                                   DBSCALE_TMP_COLUMN_KEY, index);
              modify_sub_sql.append(update_where_tmp, len);
              ++index;
            }
            boost::erase_tail(modify_sub_sql, 4);

            LOG_DEBUG("modify_sub_sql [%s]\n", modify_sub_sql.c_str());
            throw Error(
                "Not support update sql set list contain select field.");
          }
        } else {
          LOG_ERROR("Not support this type update sql\n");
          throw Error("Not support this type update sql");
        }
        break;
      }
      case STMT_DELETE: {
        unsigned int pos = get_select_part_start_pos(st.scanner);
        select_sub_sql += select_head;
        select_sub_sql += "from ";

        table_link *head = st.scanner->first_table;
        table_link *tmp = head;

        if (tmp->join->schema_name) {
          select_sub_sql += tmp->join->schema_name;
          select_sub_sql += '.';
        }
        select_sub_sql += tmp->join->table_name;
        if (tmp->join->alias) {
          select_sub_sql += " as ";
          select_sub_sql += tmp->join->alias;
        }
        tmp = tmp->next;
        while (tmp) {
          if (tmp->join->cur_rec_scan != st.scanner) break;
          select_sub_sql += ',';
          if (tmp->join->schema_name) {
            select_sub_sql += tmp->join->schema_name;
            select_sub_sql += '.';
          }
          select_sub_sql += tmp->join->table_name;
          if (tmp->join->alias) {
            select_sub_sql += " as ";
            select_sub_sql += tmp->join->alias;
          }

          tmp = tmp->next;
        }

        select_sub_sql += " ";
        select_sub_sql.append(sql + pos - 1, strlen(sql) - pos + 1);

        modify_sub_sql += "DELETE FROM ";
        modify_sub_sql += schema_name;
        modify_sub_sql += ".";
        modify_sub_sql += table_name;
        modify_sub_sql += " WHERE";
        break;
      }
      default:
        break;
    }
  }

  LOG_DEBUG(
      "Assemble select_sub_sql [%s], modify_sub_sql [%s] for"
      " sql [%s].\n",
      select_sub_sql.c_str(), modify_sub_sql.c_str(), sql);

  return (ret == HAS_IDENTIFY_COLUMNS);
}

void Statement::set_order_by_indexs(record_scan *rs) {
  while (rs->is_select_union) rs = rs->union_select_left;
  order_item *tmp = st.scanner->order_by_list;
  order_item *start = tmp;
  Expression *expr = tmp->field->field_expr;
  int column_index;
  do {
    column_index = item_in_select_fields_pos(expr, rs);
    order_dir sort_order = tmp->order;
    if (column_index != COLUMN_INDEX_UNDEF) {
      union_order_by_index.push_back(make_pair(column_index, sort_order));
    } else {
      LOG_ERROR("The order by expr is not in select fields or contain star.\n");
      throw NotSupportedError(
          "Not support order by expression not in select fields or contain "
          "star.");
    }
  } while (tmp != start);
}

void Statement::set_limit_str(ExecutePlan *plan, record_scan *rs) {
  long offset = 0;
  long num = 0;
  IntExpression *limit_offset = rs->limit->offset;
  IntExpression *limit_num = rs->limit->num;
  offset = get_limit_clause_offset_value(limit_offset, plan, rs);
  if (limit_num)
    num = get_integer_value_from_expr(limit_num);
  else {
    LOG_ERROR("limit num is not correct for sql [%s].\n", sql);
    throw Error("limit num is not correct");
  }

  union_limit_str.assign("LIMIT");
  union_limit_str.append(" ");
  char new_num[128];
  size_t len = sprintf(new_num, "%ld", offset + num);
  union_limit_str.append(new_num, len);
}

string Statement::get_expr_or_alias_in_select_items(int column_index,
                                                    record_scan *rs,
                                                    bool is_union) {
  string ret;
  field_item *field = rs->field_list_head;
  int i = 0;

  while (i++ < column_index) {
    if (!field) {
      LOG_ERROR("Fail to find [%d] column in fields.\n", column_index);
      throw Error("Fail to find correct column in select list.");
    }
    field = field->next;
  }

  if (!field) {
    LOG_ERROR("Fail to find [%d] column in fields.\n", column_index);
    throw Error("Fail to find correct column in select list.");
  }
  if (field->alias) {
    ret.assign(field->alias);
  } else if (is_union) {
    string ret_tmp;
    field->field_expr->to_string(ret_tmp);
    size_t pos = ret_tmp.rfind(".");
    if (pos != string::npos)
      ret = ret_tmp.substr(pos + 1);
    else
      ret = ret_tmp;
  } else {
    if (field->field_expr) {
      field->field_expr->to_string(ret);
    }
  }
  return ret;
}

string Statement::append_order_by_to_union(record_scan *rs,
                                           const char *ori_sql) {
  bool is_union = false;
  while (rs->is_select_union) {
    is_union = true;
    rs = rs->union_select_left;
  }
  string original_sql(ori_sql);
  string order_str(" ORDER BY ");
  string str;
  list<pair<int, order_dir> >::iterator it = union_order_by_index.begin();
  str = get_expr_or_alias_in_select_items(it->first, rs, is_union);
  order_str.append(str);
  if (it->second == ORDER_DESC)
    order_str.append(" DESC");
  else
    order_str.append(" ASC");
  for (it++; it != union_order_by_index.end(); ++it) {
    str.clear();
    str = get_expr_or_alias_in_select_items(it->first, rs, is_union);
    order_str.append(", ");
    order_str.append(str);
    if (it->second == ORDER_DESC)
      order_str.append(" DESC");
    else
      order_str.append(" ASC");
  }
  original_sql.append(order_str);
  return original_sql;
}

string Statement::append_limit_to_union(const char *ori_sql) {
  string original_sql(ori_sql);
  original_sql.append(" ");
  original_sql.append(union_limit_str);
  return original_sql;
}

/*Get dataspaces from a execute node.*/
void Statement::get_union_all_node_spaces(ExecuteNode *node,
                                          set<DataSpace *> &spaces) {
  list<ExecuteNode *> children;
  node->get_children(children);
  if (children.empty()) {
    DataSpace *space = node->get_dataspace();
    if (space) {
      spaces.insert(space);
    } else {
      LOG_ERROR("Fail to get dataspace from node [%s].\n",
                node->get_executenode_name());
      throw Error("Fail to get dataspace from execute node");
    }
  }
  list<ExecuteNode *>::iterator it = children.begin();
  for (; it != children.end(); it++) {
    get_union_all_node_spaces(*it, spaces);
  }
}

/*If any two ExecuteNode may get the same connection from kept connections,
 *put them into different groups.
 *Use union_node_group to store the execute node group, union_space_group to
 *store dataspace for every node group. Check if the dataspaces got from a node
 *and dataspace group stored in union_spaces_group may get the same connection
 *from kept connections, put the node and it's dataspaces into the first group
 *that all dataspaces may not get the same connection, if not found the group,
 *create a new one.
 */
void Statement::build_union_node_group(ExecutePlan *plan) {
  vector<ExecuteNode *>::iterator it = union_all_nodes.begin();
  set<DataSpace *> spaces;
  list<ExecuteNode *> node_list;
  get_union_all_node_spaces(*it, spaces);
  node_list.push_back(*it);
  union_node_group.push_back(node_list);
  union_space_group.push_back(spaces);
  it++;
  for (; it != union_all_nodes.end(); it++) {
    set<DataSpace *> spaces;
    get_union_all_node_spaces(*it, spaces);
    unsigned int i = 0;
    bool node_grouped = false;
    set<DataSpace *>::iterator it_s = spaces.begin();
    while (i < union_node_group.size()) {
      bool has_space = false;
      if (union_node_group[i].size() >= max_union_all_sqls) {
        has_space = true;
      } else {
        for (; it_s != spaces.end(); it_s++) {
          if (union_space_group[i].count(*it_s)) {
            has_space = true;
            break;
          } else {
            set<DataSpace *>::iterator it_c = union_space_group[i].begin();
            for (; it_c != union_space_group[i].end(); it_c++) {
              if (plan->session->check_same_kept_connection(*it_s, *it_c)) {
                has_space = true;
                union_space_group[i].insert(*it_c);
                break;
              }
            }
            if (has_space) break;
          }
        }
      }
      if (has_space) {
        ++i;
      } else {
        union_node_group[i].push_back(*it);
        union_space_group[i].insert(spaces.begin(), spaces.end());
        node_grouped = true;
        break;
      }
    }
    if (!node_grouped) {
      list<ExecuteNode *> node_list;
      node_list.push_back(*it);
      union_node_group.push_back(node_list);
      union_space_group.push_back(spaces);
    }
  }
}

void Statement::build_max_union_node_group() {
  vector<ExecuteNode *>::iterator it = union_all_nodes.begin();
  list<ExecuteNode *> node_list;
  node_list.push_back(*it);
  union_node_group.push_back(node_list);
  it++;
  int group_size = 0;
  for (; it != union_all_nodes.end(); it++) {
    if (union_node_group[group_size].size() < max_union_all_sqls) {
      union_node_group[group_size].push_back(*it);
    } else {
      list<ExecuteNode *> node_list;
      node_list.push_back(*it);
      union_node_group.push_back(node_list);
      group_size++;
    }
  }
}

void Statement::rebuild_union_all_sql_and_generate_execution_plan(
    ExecutePlan *plan) {
  string union_all_sql(sql);
  record_scan *union_all_rs = NULL;
  if (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT)
    union_all_rs = st.scanner->children_begin;
  else
    union_all_rs = st.scanner;
  get_sub_sqls(union_all_rs, union_all_sql);

  if (union_all_rs->limit) set_limit_str(plan, union_all_rs);

  list<string>::iterator it = union_all_sub_sqls.begin();
  if (union_all_rs->order_by_list) {
    Parser *parser = Driver::get_driver()->get_parser();
    Statement *new_stmt = parser->parse(it->c_str(), stmt_allow_dot_in_ident,
                                        true, NULL, NULL, NULL, ctype);
    union_all_stmts.push_back(new_stmt);
    new_stmt->check_and_refuse_partial_parse();
    if (is_cross_node_join()) new_stmt->set_cross_node_join(true);
    set_order_by_indexs(new_stmt->get_stmt_node()->scanner);
  }
  generate_union_all_sub_sql_and_execution_nodes(plan, it->c_str());
  list<SortDesc> *tmp_order_list = union_all_stmts.back()->get_order_by_list();
  it++;
  for (; it != union_all_sub_sqls.end(); ++it)
    generate_union_all_sub_sql_and_execution_nodes(plan, it->c_str());

  ExecuteNode *tail = NULL;
  ExecuteNode *head = NULL;
  ExecuteNode *order_node = NULL;
  ExecuteNode *limit_node = NULL;
  if (plan->session->is_in_transaction() || plan->session->is_in_lock()) {
    build_union_node_group(plan);
    LOG_DEBUG(
        "Union all in transaction, split union all nodes to [%d] groups.\n",
        union_node_group.size());
  } else if (union_all_sub_sqls.size() > max_union_all_sqls) {
    build_max_union_node_group();
    LOG_DEBUG(
        "Reach max_union_all_sqls, split union all nodes to [%d] groups.\n",
        union_node_group.size());
  }
  if (union_node_group.size() <= 1)
    head = tail = plan->get_union_all_node(union_all_nodes);

  if (union_all_rs->limit) {
    long offset = 0;
    long num = 0;
    IntExpression *limit_offset = union_all_rs->limit->offset;
    IntExpression *limit_num = union_all_rs->limit->num;
    offset = get_limit_clause_offset_value(limit_offset, plan, union_all_rs);
    if (limit_num)
      num = get_integer_value_from_expr(limit_num);
    else {
      LOG_ERROR("limit num is not correct for sql [%s].\n", sql);
      throw Error("limit num is not correct");
    }

    limit_node = plan->get_limit_node(offset, num);
    if (!head) head = limit_node;
    if (tail) tail->add_child(limit_node);
    tail = limit_node;
  }
  if (union_all_rs->order_by_list) {
    order_node = plan->get_sort_node(tmp_order_list);
    if (!head) head = order_node;
    if (tail) tail->add_child(order_node);
    tail = order_node;
  }

  if (union_node_group.size() > 1) {
    if (tail && !strcmp(tail->get_executenode_name(), "MySQLSortNode")) {
      vector<ExecuteNode *>::iterator it = union_all_nodes.begin();
      for (; it != union_all_nodes.end(); ++it) {
        (*it)->clean();
        delete *it;
      }
      if (head) {
        head->clean();
        delete head;
      }
      LOG_ERROR(
          "Not support order by when UNION ALL query need group execute.\n");
      throw NotSupportedError(
          "Not support order by when UNION ALL need group execute.");
    }
    ExecuteNode *union_group_node =
        plan->get_union_group_node(union_node_group);
    if (!head) head = union_group_node;
    if (tail) tail->add_child(union_group_node);
    tail = union_group_node;
  }
  union_all_node = head;

  for (unsigned int i = 0; i < union_all_nodes.size(); ++i)
    tail->add_child(union_all_nodes[i]);

  if (!union_table_sub) {
    ExecuteNode *send_node = plan->get_send_node();
    send_node->add_child(union_all_node);
    plan->set_start_node(send_node);
  }
}

/* If the union query is a table subquery, we should insert the
 * result to the subquery tmp table, so we generate a insert sql,
 * treate the union subquery as a normal insert select query.
 **/
void Statement::assemble_union_table_sub(ExecutePlan *plan) {
  string insert_sql("INSERT INTO ");
  insert_sql.append(TMP_TABLE_SCHEMA);
  insert_sql.append(".");
  insert_sql.append(union_sub_table_name);
  insert_sql.append(" VALUES ");

  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  ExecuteNode *insert_select = NULL;
  ExecuteNode *modify_node = NULL;
  vector<ExecuteNode *> nodes;
  nodes.push_back(union_all_node);

  Backend *backend = Backend::instance();
  bool is_duplicated = false;
  DataSpace *space = backend->get_data_space_for_table(
      TMP_TABLE_SCHEMA, union_sub_table_name.c_str());
  if (space->is_duplicated()) {
    space = ((DuplicatedTable *)space)->get_partition_table();
    is_duplicated = true;
  }
  if (space->get_data_source()) {
    insert_select = plan->get_insert_select_node(insert_sql.c_str(), &nodes);
    modify_node = plan->get_modify_node(space);
    insert_select->add_child(modify_node);
  } else {
    vector<unsigned int> key_pos;
    if (!is_duplicated)
      ((PartitionedTable *)space)
          ->get_key_pos_vec(TMP_TABLE_SCHEMA, union_sub_table_name.c_str(),
                            key_pos, stmt_session);
    insert_select = plan->get_par_insert_select_node(
        insert_sql.c_str(), (PartitionedTable *)space,
        ((PartitionedTable *)space)->get_partition_method(), key_pos, &nodes,
        TMP_TABLE_SCHEMA, union_sub_table_name.c_str(), is_duplicated);
    plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
  }
  ok_merge_node->add_child(insert_select);
  ok_node->add_child(ok_merge_node);
  plan->set_start_node(ok_node);
}

void Statement::get_sub_sqls(record_scan *scan, string union_all_sql) {
  string union_all_sub_sql;
  LOG_DEBUG("Get union sub sqls of %s.\n", union_all_sql.c_str());
  get_sub_sqls_poses(scan);
  map<unsigned int, unsigned int>::iterator it =
      union_all_sub_sql_poses.begin();
  for (; it != union_all_sub_sql_poses.end(); ++it) {
    union_all_sub_sql.assign(union_all_sql, it->first - 1,
                             it->second - it->first + 1);
    union_all_sub_sqls.push_back(union_all_sub_sql);
    LOG_DEBUG("UNION ALL sub query [%s]\n", union_all_sub_sql.c_str());
  }
}

DataSpace *Statement::get_sub_sqls_poses(record_scan *scan) {
  DataSpace *left_space = NULL;
  DataSpace *right_space = NULL;
  record_scan *left_scan = scan->union_select_left;
  record_scan *right_scan = scan->union_select_right;

  if (scan->is_select_union &&
      scan->opt_union_all) {  // handle union all situation
    left_space = get_sub_sqls_poses(left_scan);
    right_space = get_sub_sqls_poses(right_scan);
  } else if (scan->is_select_union) {  // handle union situation, union can be
                                       // seen as can merge here
    union_all_sub_sql_poses[scan->start_pos] = scan->end_pos;
    DataSpace *tmp_ds = record_scan_one_space_tmp_map[scan->union_select_left];
    if (!tmp_ds)
      tmp_ds = record_scan_one_space_tmp_map[scan->union_select_right];
    return tmp_ds;
  } else {  // leaf record scan.
    union_all_sub_sql_poses[scan->start_pos] = scan->end_pos;
    if (record_scan_all_par_tables_map.count(scan)) return NULL;
    DataSpace *tmp_ds = merge_dataspace_one_record_scan(scan, NULL);
    if (tmp_ds) return tmp_ds;
    return NULL;
  }

  if (left_scan->union_dataspace_can_merge) {
    if (!left_space || !right_space) {
      if (!scan->opt_union_all) {
        LOG_ERROR(
            "Unsupport sql, the sql is [%s], unsupport UNION without ALL"
            " when dataspace can not merge.\n",
            sql);
        throw NotSupportedError(
            "UNION without ALL when dataspace can not merge is not supported.");
      }
      return NULL;
    } else if (stmt_session->is_dataspace_cover_session_level(left_space,
                                                              right_space)) {
      if (union_all_sub_sql_poses.count(left_scan->start_pos)) {
        union_all_sub_sql_poses.erase(left_scan->start_pos);
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // Should not be here
#endif
      }
      if (union_all_sub_sql_poses.count(right_scan->start_pos)) {
        union_all_sub_sql_poses.erase(right_scan->start_pos);
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // Should not be here
#endif
      }
      union_all_sub_sql_poses[scan->start_pos] = scan->end_pos;
      scan->union_dataspace_can_merge = true;
      return left_space;
    } else if (stmt_session->is_dataspace_cover_session_level(right_space,
                                                              left_space)) {
      if (union_all_sub_sql_poses.count(left_scan->start_pos)) {
        union_all_sub_sql_poses.erase(left_scan->start_pos);
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // Should not be here
#endif
      }
      if (union_all_sub_sql_poses.count(right_scan->start_pos)) {
        union_all_sub_sql_poses.erase(right_scan->start_pos);
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // Should not be here
#endif
      }
      union_all_sub_sql_poses[scan->start_pos] = scan->end_pos;
      scan->union_dataspace_can_merge = true;
      return right_space;
    } else if (!scan->opt_union_all) {
      LOG_ERROR(
          "Unsupport sql, the sql is [%s], unsupport UNION without ALL"
          " when dataspace can not merge.\n",
          sql);
      throw NotSupportedError(
          "UNION without ALL when dataspace can not merge is not supported.");
    }
    return NULL;
  }
  if (!scan->opt_union_all) {
    LOG_ERROR(
        "Unsupport sql, the sql is [%s], unsupport UNION without ALL"
        " when dataspace can not merge.\n",
        sql);
    throw NotSupportedError(
        "UNION without ALL when dataspace can not merge is not supported.");
  }
  return NULL;
}

bool Statement::check_union_contain_star(record_scan *rs) {
  if (rs->is_select_union) {
    if (check_union_contain_star(rs->union_select_left)) return true;
    if (check_union_contain_star(rs->union_select_right)) return true;
  }
  if (rs->is_contain_star) return true;
  return false;
}

void Statement::generate_union_all_sub_sql_and_execution_nodes(
    ExecutePlan *plan, const char *sub_sql) {
  Statement *new_stmt = NULL;
  string new_sql;
  Parser *parser = Driver::get_driver()->get_parser();
  new_stmt = parser->parse(sub_sql, stmt_allow_dot_in_ident, true, NULL, NULL,
                           NULL, ctype);
  union_all_stmts.push_back(new_stmt);
  new_stmt->check_and_refuse_partial_parse();
  if (is_cross_node_join()) new_stmt->set_cross_node_join(true);

  if (st.scanner->order_by_list &&
      !new_stmt->get_stmt_node()->scanner->order_by_list) {
    if (check_union_contain_star(new_stmt->get_stmt_node()->scanner)) {
      vector<ExecuteNode *>::iterator it = union_all_nodes.begin();
      for (; it != union_all_nodes.end(); ++it) {
        (*it)->clean();
        delete *it;
      }
      LOG_ERROR("The order by expr is not in select fields or contain star.\n");
      throw NotSupportedError(
          "Not support order by expression not in select fields or contain "
          "star.");
    }
    new_sql =
        append_order_by_to_union(new_stmt->get_stmt_node()->scanner, sub_sql);
    new_stmt = parser->parse(new_sql.c_str(), stmt_allow_dot_in_ident, true,
                             NULL, NULL, NULL, ctype);
    union_all_stmts.push_back(new_stmt);
    new_stmt->check_and_refuse_partial_parse();
    if (is_cross_node_join()) new_stmt->set_cross_node_join(true);

  } else if (st.scanner->order_by_list && union_all_sub_sqls.size() > 1 &&
             new_stmt->get_stmt_node()->scanner->order_by_list) {
    LOG_ERROR(
        "Not support order by in union sub query with order by outside.\n");
    throw NotSupportedError("Not support order by in union sub query.");
  }
  if (st.scanner->limit && !new_stmt->get_stmt_node()->scanner->limit) {
    new_sql = append_limit_to_union(new_sql.c_str());
    new_stmt = parser->parse(new_sql.c_str(), stmt_allow_dot_in_ident, true,
                             NULL, NULL, NULL, ctype);
    union_all_stmts.push_back(new_stmt);
    new_stmt->check_and_refuse_partial_parse();
    if (is_cross_node_join()) new_stmt->set_cross_node_join(true);
  }
  LOG_DEBUG("New union sub query after handle limit and order by [%s].\n",
            new_sql.c_str());

  new_stmt->set_union_all_sub(true);
  new_stmt->set_default_schema(schema);
  new_stmt->generate_execution_plan(plan);
  vector<ExecuteNode *> union_all_nodes_tmp = new_stmt->get_union_all_nodes();
  for (unsigned int i = 0; i < union_all_nodes_tmp.size(); ++i) {
    union_all_nodes.push_back(union_all_nodes_tmp[i]);
  }
  /*get table vector for constrct create sql of table sub query with the first
   *union sub query that contains tables.
   */
  if (union_table_sub && !first_union_stmt &&
      new_stmt->get_stmt_node()->table_list_head) {
    record_scan *tmp_rs = new_stmt->get_stmt_node()->scanner;
    while (tmp_rs->is_select_union) {
      tmp_rs = tmp_rs->union_select_left;
    }
    first_union_stmt = new_stmt;
    vector<TableStruct> table_vector;
    union_table_vector.push_back(table_vector);
    get_table_vector(tmp_rs, &union_table_vector);
  }
}

void Statement::check_found_rows_with_group_by(ExecutePlan *plan) {
  if (st.type == STMT_SELECT) {
    if ((st.cur_rec_scan->options & SQL_OPT_SQL_CALC_FOUND_ROWS) &&
        st.scanner->group_by_list) {
      LOG_ERROR(
          "Unsupport sql, the sql is [%s], not support"
          " SQL_OPT_SQL_CALC_FOUND_ROWS with group_by for partitioned table.\n",
          sql);
      throw NotSupportedError(
          "SQL_CALC_FOUND_ROWS with GROUP BY for partitioned table is not "
          "supported.");
    }
    plan->session->set_found_rows(0);
    plan->session->set_real_fetched_rows(0);
  }
}

void Statement::assemble_drop_mul_table(ExecutePlan *plan, bool is_view) {
  Backend *backend = Backend::instance();
  table_link *table = st.table_list_head;
  const char *schema_name, *table_name;
  map<DataSpace *, vector<string *> *> table_name_space;
  map<DataSpace *, vector<string *> *>::iterator merge_it;
  DataSpace *dataspace;
  while (table) {
    schema_name = table->join->schema_name ? table->join->schema_name : schema;
    if (dbscale_safe_sql_mode > 0 &&
        Backend::instance()->is_system_schema(schema_name)) {
      throw Error(
          "Refuse drop table of system schema for dbscale_safe_sql_mode>0");
    }

    table_name = table->join->table_name;
    string *drop_full_table_name;
    if (schema_name != schema) {
      drop_full_table_name = new string("`");
      drop_full_table_name->append(schema_name);
      drop_full_table_name->append("`.`");
      drop_full_table_name->append(table_name);
      drop_full_table_name->append("`");
    } else {
      drop_full_table_name = new string("`");
      drop_full_table_name->append(table_name);
      drop_full_table_name->append("`");
    }
    dataspace = backend->get_data_space_for_table(schema_name, table_name);
    if (backend->table_is_partitioned(schema_name, table_name)) {
      PartitionedTable *part_table = (PartitionedTable *)dataspace;
      for (unsigned int i = 0; i < part_table->get_real_partition_num(); ++i) {
        string *part_full_table_name = new string(*drop_full_table_name);
        if (table_name_space.size() > 0) {
          bool insert_flag = true;
          for (merge_it = table_name_space.begin();
               merge_it != table_name_space.end(); ++merge_it) {
            if (is_share_same_server(merge_it->first,
                                     part_table->get_partition(i))) {
              merge_it->second->push_back(part_full_table_name);
              insert_flag = false;
              break;
            }
          }
          if (insert_flag) {
            vector<string *> *tmp_table_list = new vector<string *>();
            tmp_table_list->push_back(part_full_table_name);
            table_name_space[part_table->get_partition(i)] = tmp_table_list;
          }
        } else {
          vector<string *> *tmp_table_list = new vector<string *>();
          tmp_table_list->push_back(part_full_table_name);
          table_name_space[part_table->get_partition(i)] = tmp_table_list;
        }
      }
      delete drop_full_table_name;
    } else {
      if (table_name_space.size() > 0) {
        bool insert_flag = true;
        for (merge_it = table_name_space.begin();
             merge_it != table_name_space.end(); ++merge_it) {
          if (is_share_same_server(merge_it->first, dataspace)) {
            merge_it->second->push_back(drop_full_table_name);
            insert_flag = false;
            break;
          }
        }
        if (insert_flag) {
          vector<string *> *tmp_table_list = new vector<string *>();
          tmp_table_list->push_back(drop_full_table_name);
          table_name_space[dataspace] = tmp_table_list;
        }
      } else {
        vector<string *> *tmp_table_list = new vector<string *>();
        tmp_table_list->push_back(drop_full_table_name);
        table_name_space[dataspace] = tmp_table_list;
      }
    }
    table = table->next;
  }
  DataSpace *meta_datasource = Backend::instance()->get_metadata_data_space();
  map<DataSpace *, vector<string *> *>::iterator it_space =
      table_name_space.begin();
  bool need_apply_meta = true;
  for (; it_space != table_name_space.end(); ++it_space) {
    if (is_share_same_server(meta_datasource, it_space->first)) {
      need_apply_meta = false;
      break;
    }
  }
  st.need_apply_metadata = need_apply_meta;
  ExecuteNode *ok_node = plan->get_ok_node();
  ExecuteNode *drop_node = plan->get_drop_mul_table_node();
  ExecuteNode *modify_node;
  string sql("DROP");
  if (st.sql->drop_tb_oper->op_tmp == 1) {
    sql.append(" TEMPORARY");
    st.is_create_or_drop_temp_table = true;
  }
  if (is_view)
    sql.append(" VIEW");
  else
    sql.append(" TABLE");
  if (st.sql->drop_tb_oper->op_exists == 1) {
    sql.append(" IF EXISTS");
  }
  LOG_DEBUG("The generate sql prefix is %s.\n", sql.c_str());
  map<DataSpace *, vector<string *> *>::iterator it;
  for (it = table_name_space.begin(); it != table_name_space.end(); ++it) {
    string *real_sql = new string(sql);
    real_sql->append(" ");
    unsigned int i = 0;
    for (; i < it->second->size() - 1; ++i) {
      real_sql->append(*(*(it->second))[i]);
      real_sql->append(",");
    }
    real_sql->append(*(*(it->second))[i]);
    modify_node = plan->get_modify_node(it->first, real_sql->c_str(), true);
    drop_node->add_child(modify_node);
    delete real_sql;
  }
  vector<string *>::iterator itinner;
  for (it = table_name_space.begin(); it != table_name_space.end(); ++it) {
    for (itinner = it->second->begin(); itinner != it->second->end();
         ++itinner) {
      delete *itinner;
    }
    delete it->second;
  }
  ok_node->add_child(drop_node);
  plan->set_start_node(ok_node);
}

TableStruct *Statement::check_column_belongs_to(
    const char *column_name, list<TableStruct *> *table_struct_list) {
  TableStruct *ret = NULL;
  int table_nums = 0;
  list<TableStruct *>::iterator it = table_struct_list->begin();
  for (; it != table_struct_list->end(); ++it) {
    string schema_name = (*it)->schema_name;
    string table_name = (*it)->table_name;

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
        ret = *it;
      }
    }
    ti->release_table_info_lock();
  }
  if (table_nums > 1) {
    string error_message;
    error_message.append(
        "You should add the table name to the column name since more than one "
        "table contains ");
    error_message.append(column_name);
    throw Error(error_message.c_str());
  }

  return ret;
}

TableStruct *Statement::find_column_table_from_list(
    string column_str, list<TableStruct *> *table_struct_list) {
  unsigned first_dot_position = column_str.find(".");
  unsigned second_dot_position = column_str.find(".", first_dot_position + 1);
  string column;
  string table_alias;
  string schema_name;

  TableStruct *ret = NULL;
  int table_nums = 0;
  if (first_dot_position == (unsigned)string::npos) {
    column = column_str;
    TableStruct *table_struct =
        check_column_belongs_to(column.c_str(), table_struct_list);
    if (!table_struct) {
      string error_message("Could not find columns named: ");
      error_message.append(column);
      throw Error(error_message.c_str());
    }
    table_alias = table_struct->alias;
    schema_name = table_struct->schema_name;
  } else if (second_dot_position == (unsigned)string::npos) {
    schema_name = string(schema);
    table_alias = column_str.substr(0, first_dot_position);
    column = column_str.substr(first_dot_position + 1);
  } else {
    schema_name = column_str.substr(0, first_dot_position);
    table_alias = column_str.substr(
        first_dot_position + 1, second_dot_position - first_dot_position - 1);
    column = column_str.substr(second_dot_position + 1);
  }

  list<TableStruct *>::iterator it = table_struct_list->begin();
  for (; it != table_struct_list->end(); ++it) {
    if (!strcmp((*it)->alias.c_str(), table_alias.c_str()) &&
        !strcmp((*it)->schema_name.c_str(), schema_name.c_str())) {
      ret = *it;
      ++table_nums;
    }
  }
  if (table_nums > 1) {
    string error_message;
    error_message.append("More than one table contains ");
    error_message.append(column_str);
    throw Error(error_message.c_str());
  }
  return ret;
}

void Statement::get_related_condition_full_table(
    Expression *condition, list<TableStruct *> *table_struct_list,
    map<TableStruct *, set<TableStruct *> > *table_map,
    map<string, set<TableStruct *> > *equal_values) {
  if (!condition) return;
  if (condition->type != EXPR_AND) {
    if ((condition->type == EXPR_EQ || condition->type == EXPR_EQ_N ||
         condition->type == EXPR_GE || condition->type == EXPR_GR ||
         condition->type == EXPR_LESSE || condition->type == EXPR_LESS ||
         condition->type == EXPR_NE || condition->type == EXPR_ASSIGN) &&
        ((CompareExpression *)condition)->left->type == EXPR_STR &&
        ((CompareExpression *)condition)->right->type == EXPR_STR) {
      Expression *left = ((CompareExpression *)condition)->left;
      Expression *right = ((CompareExpression *)condition)->right;
      if (is_column(left) && is_column(right)) {
        string left_str, right_str;
        left->to_string(left_str);
        right->to_string(right_str);
        TableStruct *left_ts =
            find_column_table_from_list(left_str, table_struct_list);
        TableStruct *right_ts =
            find_column_table_from_list(right_str, table_struct_list);
        if (left_ts && right_ts) {
          (*table_map)[left_ts].insert(right_ts);
          (*table_map)[right_ts].insert(left_ts);
        }
      }
    } else if (condition->type == EXPR_EQ) {
      Expression *left = ((CompareExpression *)condition)->left;
      Expression *right = ((CompareExpression *)condition)->right;
      string peer_column;
      const char *peer_value = NULL;
      if (is_column(left)) {
        peer_value = expr_is_simple_value(right);
        left->to_string(peer_column);
      } else if (is_column(right)) {
        peer_value = expr_is_simple_value(left);
        right->to_string(peer_column);
      }
      if (peer_value) {
        TableStruct *column_tb =
            find_column_table_from_list(peer_column, table_struct_list);
        if (!column_tb) return;
        (*equal_values)[peer_value].insert(column_tb);
      }
    }
  } else {
    BoolBinaryCaculateExpression *bbc_expr =
        (BoolBinaryCaculateExpression *)condition;
    get_related_condition_full_table(bbc_expr->left, table_struct_list,
                                     table_map, equal_values);
    get_related_condition_full_table(bbc_expr->right, table_struct_list,
                                     table_map, equal_values);
  }
}

TableStruct *Statement::find_table_struct_from_list(
    const char *schema_name, const char *table_name, const char *alias,
    list<TableStruct *> *table_struct_list) {
  list<TableStruct *>::iterator it = table_struct_list->begin();
  for (; it != table_struct_list->end(); ++it) {
    if (!strcasecmp(schema_name, (*it)->schema_name.c_str()) &&
        !strcasecmp(table_name, (*it)->table_name.c_str()) &&
        !strcasecmp(alias, (*it)->alias.c_str()))
      return *it;
  }
  return NULL;
}

bool Statement::check_tables_has_related_condition(
    TableStruct *table, list<TableStruct *> *merge_list,
    map<TableStruct *, set<TableStruct *> > *table_map) {
  if (!table_map->count(table)) return false;
  bool has_related_condition = false;
  list<TableStruct *>::iterator it = merge_list->begin();
  for (; it != merge_list->end(); ++it) {
    if ((*table_map)[table].count(*it)) {
      has_related_condition = true;
      break;
    }
  }
  return has_related_condition;
}

void Statement::handle_related_condition_full_table(
    record_scan *rs, list<TableStruct *> *table_struct_list,
    map<TableStruct *, set<TableStruct *> > *table_map) {
  map<string, set<TableStruct *> > equal_values;
  set<TableStruct *> tmp_set;
  equal_values["dbscale_equal_columns"] = tmp_set;

  Expression *condition = rs->condition;
  if (!condition) return;
  get_related_condition_full_table(condition, table_struct_list, table_map,
                                   &equal_values);

  table_link *tmp_tb = rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == rs) {
      join_node *join_tables = tmp_tb->join->upper;
      if (join_tables && join_tables->left->type == JOIN_NODE_SINGLE &&
          join_tables->right->type == JOIN_NODE_SINGLE &&
          join_tables->condition &&
          join_tables->condition->type == JOIN_COND_USING &&
          !join_tables->upper) {
        join_node *left_tb = join_tables->left;
        join_node *right_tb = join_tables->right;

        const char *schema_name =
            left_tb->schema_name ? left_tb->schema_name : schema;
        const char *table_name = left_tb->table_name;
        const char *alias = left_tb->alias ? left_tb->alias : table_name;
        TableStruct *left_ts = find_table_struct_from_list(
            schema_name, table_name, alias, table_struct_list);

        schema_name = right_tb->schema_name ? right_tb->schema_name : schema;
        table_name = right_tb->table_name;
        alias = right_tb->alias ? right_tb->alias : table_name;
        TableStruct *right_ts = find_table_struct_from_list(
            schema_name, table_name, alias, table_struct_list);

        if (left_ts && right_ts) {
          (*table_map)[left_ts].insert(right_ts);
          (*table_map)[right_ts].insert(left_ts);
        }
      } else if (join_tables && join_tables->left->type == JOIN_NODE_SINGLE &&
                 join_tables->right->type == JOIN_NODE_SINGLE &&
                 join_tables->condition &&
                 join_tables->condition->type == JOIN_COND_ON) {
        get_related_condition_full_table(join_tables->condition->expr_condition,
                                         table_struct_list, table_map,
                                         &equal_values);
      }
    }
    tmp_tb = tmp_tb->next;
  }

  set<TableStruct *>::iterator it_tb;
  set<TableStruct *>::iterator it_tb1;
  map<string, set<TableStruct *> >::iterator it_eq = equal_values.begin();
  for (; it_eq != equal_values.end(); ++it_eq) {
    set<TableStruct *> tb_set = it_eq->second;
    for (it_tb = tb_set.begin(); it_tb != tb_set.end(); ++it_tb) {
      for (it_tb1 = tb_set.begin(); it_tb1 != tb_set.end(); ++it_tb1) {
        (*table_map)[*it_tb].insert(*it_tb1);
      }
    }
  }

  // if table t1 has relation to t2, and t2 has relation to t3, we should add
  // relation t1 & t3.
  map<TableStruct *, set<TableStruct *> >::iterator it_map = table_map->begin();
  for (; it_map != table_map->end(); ++it_map) {
    set<TableStruct *> ts = it_map->second;
    set<TableStruct *>::iterator it_set = ts.begin();
    for (; it_set != ts.end(); ++it_set) {
      if (table_map->count(*it_set)) {
        set<TableStruct *> ts1 = (*table_map)[*it_set];
        set<TableStruct *>::iterator it_set1 = ts1.begin();
        for (; it_set1 != ts1.end(); ++it_set1) {
          if (!(*table_map)[it_map->first].count(*it_set1))
            (*table_map)[it_map->first].insert(*it_set1);
        }
      }
    }
  }
}

void Statement::handle_par_key_equality_full_table(record_scan *root) {
  list<table_link *> &par_tb_list = record_scan_all_par_tables_map[root];
  list<table_link *>::iterator it_tb = par_tb_list.begin();
  table_link *par_tb_tmp = NULL;
  for (; it_tb != par_tb_list.end(); ++it_tb) {
    par_tb_tmp = *it_tb;
    const char *schema_name_tmp =
        par_tb_tmp->join->schema_name ? par_tb_tmp->join->schema_name : schema;

    const char *table_name_tmp = par_tb_tmp->join->table_name;
    const char *table_alias_tmp = par_tb_tmp->join->alias;

    DataSpace *par_ds_tmp = record_scan_all_table_spaces[par_tb_tmp];
    if (!par_ds_tmp->is_partitioned()) {
      continue;
    }

    vector<const char *> *key_names =
        ((PartitionedTable *)par_ds_tmp)->get_key_names();
    vector<const char *>::iterator it_key = key_names->begin();
    for (; it_key != key_names->end(); ++it_key) {
      vector<string> added_equality_vector;
      join_node *join_tables = par_tb_tmp->join->upper;
      if (join_tables && join_tables->left->type == JOIN_NODE_SINGLE &&
          join_tables->right->type == JOIN_NODE_SINGLE &&
          join_tables->condition &&
          join_tables->condition->type == JOIN_COND_USING &&
          !join_tables->upper) {
        join_node *opponent_table = NULL;
        if (join_tables->right == par_tb_tmp->join) {
          opponent_table = join_tables->left;
        } else {
          opponent_table = join_tables->right;
        }

        const char *opponent_schema =
            opponent_table->schema_name ? opponent_table->schema_name : schema;
        string using_added_equality;
        using_added_equality.append(opponent_schema);
        using_added_equality.append(".");
        using_added_equality.append(opponent_table->table_name);
        using_added_equality.append(".");
        name_item *column_list = join_tables->condition->column_list;
        name_item *tmp = column_list;
        if (tmp) {
          do {
            if (!strcasecmp(*it_key, tmp->name)) {
              string column_name(using_added_equality);
              column_name.append(tmp->name);
              added_equality_vector.push_back(column_name);
            }
            tmp = tmp->next;
          } while (tmp != column_list);
        }
      }
      fullfil_par_key_equality_full_table(root, schema_name_tmp, table_name_tmp,
                                          table_alias_tmp, *it_key,
                                          &added_equality_vector);
    }
  }
}

/*Try to merge all partition tables inside one record_scan.
 *
 * Return: the final merged dataspace or NULL if merge fail.*/
DataSpace *Statement::merge_par_tables_one_record_scan(record_scan *root) {
  list<table_link *> &par_tb_list = record_scan_all_par_tables_map[root];
  list<table_link *>::iterator it_tb = par_tb_list.begin();

  table_link *par_tb = NULL;
  table_link *par_tb_tmp = NULL;
  table_link *first_par_tb = NULL;
  const char *schema_name_tmp = NULL;
  const char *table_name_tmp = NULL;
  const char *table_alias_tmp = NULL;
  DataSpace *par_ds_tmp = NULL, *par_ds = NULL;

  bool skip_this_par_table = false;
  bool first_is_dup = false;

  for (; it_tb != par_tb_list.end(); ++it_tb) {
    par_tb_tmp = *it_tb;
    skip_this_par_table = false;
    schema_name_tmp =
        par_tb_tmp->join->schema_name ? par_tb_tmp->join->schema_name : schema;

    table_name_tmp = par_tb_tmp->join->table_name;
    table_alias_tmp = par_tb_tmp->join->alias;

    par_ds_tmp = record_scan_all_table_spaces[par_tb_tmp];

    if (!par_tb) {
      par_tb = par_tb_tmp;
      par_ds = par_ds_tmp;
    } else {
      if (((Table *)par_ds)->is_duplicated()) {
        if (is_duplicated_table_merge(par_ds_tmp, par_ds))
          skip_this_par_table = true;
        par_ds = par_ds_tmp;
      } else if (((Table *)par_ds_tmp)->is_duplicated()) {
        if (is_duplicated_table_merge(par_ds, par_ds_tmp))
          skip_this_par_table = true;
      } else {
        if (check_two_partition_table_can_merge(
                par_tb_tmp->join->cur_rec_scan, par_tb_tmp,
                (PartitionedTable *)par_ds, (PartitionedTable *)par_ds_tmp)) {
          skip_this_par_table = true;
        } else {
          LOG_DEBUG(
              "The partition table dataspace [%s] is conflicting with"
              " partition table dataspace [%s] for sql [%s].\n",
              par_ds->get_name(), par_ds_tmp->get_name(), sql);
          return NULL;
        }
      }
    }
    if (!record_scan_par_key_equality.count(root) &&
        !((Table *)par_ds_tmp)->is_duplicated()) {
      vector<const char *> *key_names =
          ((PartitionedTable *)par_ds_tmp)->get_key_names();
      vector<const char *>::iterator it_key = key_names->begin();
      for (; it_key != key_names->end(); ++it_key) {
        vector<string> added_equality_vector;
        join_node *join_tables = par_tb_tmp->join->upper;
        if (join_tables && join_tables->left->type == JOIN_NODE_SINGLE &&
            join_tables->right->type == JOIN_NODE_SINGLE &&
            join_tables->condition &&
            join_tables->condition->type == JOIN_COND_USING &&
            !join_tables->upper) {
          join_node *opponent_table = NULL;
          if (join_tables->right == par_tb_tmp->join) {
            opponent_table = join_tables->left;
          } else {
            opponent_table = join_tables->right;
          }

          const char *opponent_schema = opponent_table->schema_name
                                            ? opponent_table->schema_name
                                            : schema;
          string using_added_equality;
          using_added_equality.append(opponent_schema);
          using_added_equality.append(".");
          using_added_equality.append(opponent_table->table_name);
          using_added_equality.append(".");
          name_item *column_list = join_tables->condition->column_list;
          name_item *tmp = column_list;
          if (tmp) {
            do {
              if (!strcasecmp(*it_key, tmp->name)) {
                string column_name(using_added_equality);
                column_name.append(tmp->name);
                added_equality_vector.push_back(column_name);
              }
              tmp = tmp->next;
            } while (tmp != column_list);
          }
        }
        fullfil_par_key_equality(root, schema_name_tmp, table_name_tmp,
                                 table_alias_tmp, *it_key,
                                 &added_equality_vector);
      }
    }
    if (!skip_this_par_table) {
      if (!first_par_tb) {
        first_par_tb = par_tb_tmp;
        if (((Table *)par_ds_tmp)->is_duplicated()) {
          first_is_dup = true;
        }
      }
    }
  }
  /*Only add to vector par_tables if merge success.*/
  if (first_par_tb && !record_scan_need_table.count(root)) {
    if (first_is_dup) {
      /*If duplicated table merged with other partition table, we will always
       * ignore the duplicated table as par_tables.*/
      if (par_tb_list.size() >= 2) {
        it_tb = par_tb_list.begin();
        ++it_tb;
        first_par_tb = *it_tb;
      }
    }
    ++par_table_num;
    par_tables.push_back(first_par_tb);
    need_clean_par_tables = true;
    record_scan_par_table_map[root] = first_par_tb;
    need_clean_record_scan_par_table_map = true;
  }

  return par_ds;
}

/*Try to merge all dataspaces inside one record scan. The merge start from the
 * only one partition dataspace (param: par_ds).
 *
 * Return: the final merged data space or NULL if merge fail.*/
DataSpace *Statement::merge_dataspace_one_record_scan(record_scan *root,
                                                      DataSpace *par_ds) {
  list<DataSpace *> &ds_list = record_scan_all_spaces_map[root];

  list<DataSpace *>::iterator it = ds_list.begin();
  DataSpace *ds = par_ds, *ds_tmp = NULL;
  for (; it != ds_list.end(); ++it) {
    ds_tmp = *it;
    if (!ds) {
      ds = ds_tmp;
    } else {
      if (stmt_session->is_dataspace_cover_session_level(ds, ds_tmp)) {
        if (enable_global_plan) {
          // this means there is global table and is merged.
          if (ds_tmp->get_data_source() &&
              ds_tmp->get_data_source()->get_data_source_type() ==
                  DATASOURCE_TYPE_REPLICATION)
            global_table_merged.insert(ds_tmp);
        }
      } else if (stmt_session->is_dataspace_cover_session_level(ds_tmp, ds)) {
        ds = ds_tmp;
      } else {
        LOG_DEBUG(
            "The dataspace [%s] is conflicting with dataspace [%s]"
            " for sql [%s].\n",
            ds->get_name(), ds_tmp->get_name(), sql);
        return NULL;
      }
    }
  }
  return ds;
}

/* Check spj situation and check the can merge situation, if can merge, we
 * should not generate SeperatedNode for this subquery. */
bool Statement::check_spj_subquery_can_merge(record_scan *parent_rs,
                                             record_scan *child_rs) {
  if (child_rs->join_belong && !child_rs->limit && child_rs->join_tables &&
      child_rs->join_tables->type == JOIN_NODE_SINGLE) {
    ;
  } else {
    return false;
  }

  if (child_rs->having) {
    if (child_rs->having->check_contains_function()) return false;
  }

  if (!record_scan_one_space_tmp_map.count(parent_rs) ||
      !record_scan_one_space_tmp_map.count(child_rs))
    return false;
  DataSpace *child_ds = record_scan_one_space_tmp_map[child_rs];
  DataSpace *parent_ds = record_scan_one_space_tmp_map[parent_rs];

  if (!child_ds || !parent_ds) return false;

  if (child_ds->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)child_ds)->is_partitioned() &&
      parent_ds->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)parent_ds)->is_partitioned()) {
    vector<const char *> *key_names =
        ((PartitionedTable *)child_ds)->get_key_names();
    if (child_rs->group_by_list) {
      join_node *join = child_rs->first_table->join;
      string child_table_name = join->alias ? join->alias : join->table_name;

      string child_schema_name = join->schema_name ? join->schema_name : schema;
      bool find_key = false;
      order_item *item = child_rs->group_by_list;
      do {
        if (groupby_on_par_key(item, child_schema_name.c_str(),
                               child_table_name.c_str(),
                               child_table_name.c_str(), key_names->at(0))) {
          find_key = true;
          break;
        }
        item = item->next;
      } while (item != child_rs->group_by_list);
      if (!find_key) return false;
    }
    if (check_rs_contains_partition_key(child_rs, child_ds) == -1) return false;

    /* use a fake table link to fullfil the can merge method, the can merge
     * method  will only use the table name, schema name, alias in table link.
     * */
    table_link *parent_table_link = find_partition_table_table_link(parent_rs);
    if (parent_table_link) {
      join_node tmp_node = {JOIN_NODE_SINGLE,
                            1,
                            schema,
                            child_rs->join_belong->alias,
                            child_rs->join_belong->alias,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            0,
                            0};
      table_link fake_child_table_link = {&tmp_node, NULL, NULL, 0, 0, 0, 0};

      if (!record_scan_need_cross_node_join.count(parent_rs) ||
          !record_scan_need_cross_node_join[parent_rs])
        handle_par_key_equality_full_table(parent_rs);
      bool can_merge = check_CNJ_two_partition_table_can_merge(
          parent_rs, parent_table_link, &fake_child_table_link,
          (PartitionedTable *)parent_ds, (PartitionedTable *)child_ds);
      return can_merge;
    }
  }
  return false;
}

/*
 * Checking the target record scan, normally subquery, whether it contains the
 * partition key of the target partition table dataspace. Normally we check the
 * select field list of the target record scan, and if found, we return the pos
 * of the filed in the select list, otherwise we return -1, which means there is
 * no key in the target record scan.
 */
int Statement::check_rs_contains_partition_key(record_scan *rs, DataSpace *ds) {
  int position = 0;
  if (!rs->first_table) return -1;
  join_node *join = rs->first_table->join;
  string child_table_name = join->alias ? join->alias : join->table_name;
  string table_with_star(child_table_name);
  table_with_star.append(".*");

  string child_schema_name = join->schema_name ? join->schema_name : schema;
  string schema_table_star(child_schema_name);
  schema_table_star.append(".");
  schema_table_star.append(table_with_star);

  vector<const char *> *key_names = ((PartitionedTable *)ds)->get_key_names();
  bool contains_partition_key = false;
  field_item *fi = rs->field_list_head;
  do {
    string column_name;
    if (fi->field_expr) {
      if (fi->field_expr->type == EXPR_STR) {
        fi->field_expr->to_string(column_name);
        if (!strcasecmp(column_name.c_str(), key_names->at(0)) && !fi->alias) {
          contains_partition_key = true;
          break;
        } else if (!strcmp(column_name.c_str(), "*")) {
          contains_partition_key = true;
          break;
        } else if (!strcmp(column_name.c_str(), table_with_star.c_str())) {
          contains_partition_key = true;
          break;
        } else if (!strcmp(column_name.c_str(), schema_table_star.c_str())) {
          contains_partition_key = true;
          break;
        }
      }
    }
    ++position;
    fi = fi->next;
  } while (fi && fi != rs->field_list_tail);

  if (!contains_partition_key) return -1;
  return position;
}

table_link *Statement::find_partition_table_table_link(record_scan *rs) {
  Backend *backend = Backend::instance();
  TableStruct *table_struct = NULL;
  table_link *ret = NULL;
  if (record_scan_join_tables.count(rs) && record_scan_join_tables[rs]) {
    vector<vector<TableStruct *> *> *join_table_sequence =
        record_scan_join_tables[rs];
    vector<TableStruct *> *last_table_vector =
        join_table_sequence->at(join_table_sequence->size() - 1);
    vector<TableStruct *>::iterator it = last_table_vector->begin();
    for (; it != last_table_vector->end(); ++it) {
      DataSpace *ds = backend->get_data_space_for_table(
          (*it)->schema_name.c_str(), (*it)->table_name.c_str());
      if (ds->get_dataspace_type() == TABLE_TYPE &&
          ((Table *)ds)->is_partitioned()) {
        table_struct = *it;
        break;
      }
    }
  } else {
    table_link *table_l = rs->first_table;
    while (table_l) {
      if (table_l->join->cur_rec_scan == rs) {
        string table_name = table_l->join->table_name;
        string schema_name =
            table_l->join->schema_name ? table_l->join->schema_name : schema;
        DataSpace *ds = backend->get_data_space_for_table(schema_name.c_str(),
                                                          table_name.c_str());
        if (ds->get_dataspace_type() == TABLE_TYPE &&
            ((Table *)ds)->is_partitioned()) {
          return table_l;
        }
      }
      table_l = table_l->next;
    }
  }
  if (table_struct) {
    table_link *tmp_tb = rs->first_table;
    while (tmp_tb) {
      if (tmp_tb->join->cur_rec_scan == rs) {
        string table_name = tmp_tb->join->table_name;
        string schema_name =
            tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
        string alias = tmp_tb->join->alias ? tmp_tb->join->alias : table_name;
        if (!strcmp(table_struct->schema_name.c_str(), schema_name.c_str()) &&
            !strcmp(table_struct->alias.c_str(), alias.c_str())) {
          ret = tmp_tb;
          break;
        }
      }
      tmp_tb = tmp_tb->next;
    }
  }
  return ret;
}

/* For subquery in IN EXPRESSION, we can check the merge of the child
 * record_scan with the parent table. if the child is norm_table, we check the
 * cover of them. If the child is partition table and the parent is parttion
 * table, check two partition table can merge using partition key. */
bool Statement::check_column_record_scan_can_merge(record_scan *parent_rs,
                                                   record_scan *child_rs,
                                                   int record_scan_position,
                                                   ExecutePlan *plan) {
  /*

  判断 can merge 的情况，In 子查询 can merge 的情况是，
  举个例子：

  t1.c1 In (select t2.c2 from t2)

  c1 是 t1 的分区列， c2 是 t2 的分区列，t1 和 t2 cover。

  这里的 merge 是t1可以和这个子查询 merge。并不是 parent 的 final dataspace
  可以和 child merge。所以要记录到can_merge_record_scan_position中，在处理 t1
  的时候一起处理子查询。

  */
  ACE_UNUSED_ARG(plan);
  string peer_string;              // parent string
  TableStruct table_parent;        // parent table struct
  DataSpace *space_parent = NULL;  // parent space
  string subquery_string;          // child string
  TableStruct table_child;         // child table struct
  DataSpace *space_child = NULL;   // child space
  DataSpace *child_ds = NULL;      // temporary space
  Backend *backend = Backend::instance();
  bool ret = false;
  /* We need to find out the expressions in "IN", and we should also check the
   * dataspaces can_merge using two record_scan. */
  if (subquery_peer.count(child_rs)) {
    // Find one possible key of parent dataspace
    peer_string = subquery_peer[child_rs];
    try {
      table_parent = get_table_from_record_scan(parent_rs, peer_string);
    } catch (Error &e) {
      LOG_ERROR(
          "Fail to get_table_from_record_scan in "
          "check_column_record_scan_can_merge due to %s.\n",
          e.what());
      return false;
    }
    space_parent = backend->get_data_space_for_table(
        table_parent.schema_name.c_str(), table_parent.table_name.c_str());
    Expression *child_expr = child_rs->field_list_head->field_expr;
    if (!space_parent->get_data_source()) {
      if (child_expr->type != EXPR_STR) {
        LOG_DEBUG(
            "The type of the child expression is not EXPR_STR, so can not "
            "merge.\n");
        return false;
      }
      child_expr->to_string(subquery_string);
      /* We get the exactly space of the child, we should use this space to
       * compare the key with subquery_string */
      try {
        table_child = get_table_from_record_scan(child_rs, subquery_string);
      } catch (Error &e) {
        LOG_ERROR(
            "Fail to get_table_from_record_scan in "
            "check_column_record_scan_can_merge due to %s.\n",
            e.what());
        return false;
      }

      space_child = backend->get_data_space_for_table(
          table_child.schema_name.c_str(), table_child.table_name.c_str());
    }
    child_ds = record_scan_one_space_tmp_map[child_rs];
    // For child is norm_table, If they cover, they can merge
    if (child_ds->get_data_source()) {
      if (stmt_session->is_dataspace_cover_session_level(space_parent,
                                                         child_ds))
        ret = true;
    } else if (!child_ds->get_data_source() &&
               !space_parent->get_data_source()) {
      // 判断 subquery_string 是 table_child 的 key
      // 判断 peer_string 是 table_parent 的 key
      // 如果都是 key，并且dataspace可以cover，那么就是can merge的

      PartitionedTable *child_table = (PartitionedTable *)child_ds;
      PartitionedTable *parent_table = (PartitionedTable *)space_parent;
      // Find another possible key of child dataspace
      bool child_is_key = false;
      /* Findout whether the child select list is key of the child dataspace
       * */
      vector<const char *> *child_key_names =
          ((PartitionedTable *)space_child)->get_key_names();
      child_is_key = column_name_equal_key_alias_without_len(
          subquery_string.c_str(), table_child.schema_name.c_str(),
          table_child.table_name.c_str(), child_key_names->at(0),
          table_child.alias.c_str());
      bool parent_is_key = false;
      vector<const char *> *parent_key_names = parent_table->get_key_names();
      parent_is_key = column_name_equal_key_alias_without_len(
          peer_string.c_str(), table_parent.schema_name.c_str(),
          table_parent.table_name.c_str(), parent_key_names->at(0),
          table_parent.alias.c_str());

      if (parent_is_key && child_is_key &&
          is_partition_table_cover(child_table, parent_table)) {
        ret = true;
      }
    }
    if (!(child_rs->subquerytype == SUB_SELECT_ONE_COLUMN && stmt_session &&
          !stmt_session->get_session_option("use_table_for_one_column_subquery")
               .int_val)) {
      if (!ret && stmt_session &&
          (MethodType)stmt_session->get_session_option("cross_node_join_method")
                  .int_val == DATA_MOVE_WRITE &&
          !union_all_sub) {  // union and federated will be supported in issue
                             // #3365
        /*currently only support column table subquery for data move write*/
        child_rs->subquerytype = SUB_SELECT_COLUMN;
        /*对于不能merge的情况，dbscale会把这个子查询按照SUB_SELECT_COLUMN的方式，或动到space_parent上，并以select
         * column from '临时表'
         * 的形式替换到当前子查询中，具体参考subquery.cc的SeparatedExecNode::execute_column_table_subquery*/
        DataSpace *column_table_space = space_parent;
        if (gtable_can_merge_dataspace.count(parent_rs) &&
            gtable_can_merge_dataspace[parent_rs].count(space_parent)) {
          column_table_space =
              gtable_can_merge_dataspace[parent_rs][space_parent];
        }
        column_output_dataspace[child_rs] = column_table_space;
        init_column_table_subquery_tmp_table_name(child_rs, column_table_space,
                                                  plan);
      }
    }

    // 设置可以merge，这里需要注意，子查询可能和父查询中任意的表merge，也就是
    // 说，这个子查询可能需要和非父查询最终节点一起执行，例如：
    // t1, t2, t3 where t2.c1 in (select c2 from)
    // 执行的结构就是 t1，t2 where t2.c1 in (select c2 from ), t3
    // 子查询并不是在最终的dataspace上执行。这里就需要设置具体在哪个表执行。
    can_merge_record_scan_position[parent_rs][record_scan_position] =
        table_parent;
    // can_merge_record_scan_position传递给了
    // helper->table_position_map，用于后续的拼语句。
  }
#ifdef DEBUG
  LOG_DEBUG("Statement::check_column_record_scan_can_merge can_merge = [%d]\n",
            ret);
#endif
  return ret;
}

void Statement::adjust_cross_node_join_rs(record_scan *rs,
                                          TableStruct table_parent) {
  TableStruct *table_struct;
  vector<TableStruct *> *table_struct_vector;
  if (record_scan_need_cross_node_join.count(rs)) {
    table_struct_vector = new vector<TableStruct *>();
    table_struct = new TableStruct();
    *table_struct = table_parent;
    table_struct_vector->push_back(table_struct);
    record_scan_join_tables[rs]->push_back(table_struct_vector);
    set_join_final_table(rs);
  }
}

TableStruct Statement::get_table_from_record_scan(record_scan *parent_rs,
                                                  string peer_string) {
  unsigned first_dot_position = peer_string.find(".");
  unsigned second_dot_position = peer_string.find(".", first_dot_position + 1);
  string column;
  string table_alias;
  string schema_name;
  string table_name;

  TableStruct ret;
  bool ret_has_value = false;
  vector<vector<TableStruct> > table_vector_vector;
  vector<TableStruct> table_vector;
  table_link *tmp_tb = parent_rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == parent_rs) {
      TableStruct table_struct;
      table_struct.table_name = tmp_tb->join->table_name;
      table_struct.schema_name =
          tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
      table_struct.alias =
          tmp_tb->join->alias ? tmp_tb->join->alias : table_struct.table_name;
      table_vector.push_back(table_struct);
    }
    tmp_tb = tmp_tb->next;
  }

  if (first_dot_position == (unsigned)string::npos) {
    column = peer_string;
    if (!strcmp(column.c_str(), "*")) {
      /*If only has one table, and the column is *, we can dirtectly return
       * the table.*/
      if (table_vector.size() == 1)
        return table_vector[0];
      else {
        string error_message(
            "Could not find table with column * with no table or more than one "
            "table in record_scan");
        throw Error(error_message.c_str());
      }
    }
    table_vector_vector.push_back(table_vector);
    TableStruct *table_struct =
        check_column_belongs_to(column.c_str(), &table_vector_vector);
    if (!table_struct) {
      string error_message("Could not find columns named: ");
      error_message.append(column);
      throw Error(error_message.c_str());
    }
    ret = *table_struct;
    ret_has_value = true;
    table_alias = table_struct->alias;
    schema_name = table_struct->schema_name;
    table_name = table_struct->table_name;
  } else if (second_dot_position == (unsigned)string::npos) {
    /* the expression is like 't1.c1' */
    string tmp_table_alias = peer_string.substr(0, first_dot_position);
    if (first_dot_position != (unsigned)string::npos) {
      for (unsigned int i = 0; i < table_vector.size(); i++) {
        TableStruct &tmp_struct = table_vector[i];
        if (tmp_struct.alias == tmp_table_alias) {
          schema_name = tmp_struct.schema_name;
          table_alias = tmp_struct.alias;
          table_name = tmp_struct.table_name;
          ret = tmp_struct;
          ret_has_value = true;
          break;
        }
      }
      if (ret_has_value == false) {
        schema_name = string(schema);
        table_alias = peer_string.substr(0, first_dot_position);
        column = peer_string.substr(first_dot_position + 1);
      }
    }
  } else {
    /* the expression is like 'test.t1.c1' */
    schema_name = peer_string.substr(0, first_dot_position);
    table_alias = peer_string.substr(
        first_dot_position + 1, second_dot_position - first_dot_position - 1);
    column = peer_string.substr(second_dot_position + 1);
  }

  if (table_name.empty()) {
    vector<TableStruct>::iterator it = table_vector.begin();
    for (; it != table_vector.end(); ++it) {
      if (!strcmp(it->alias.c_str(), table_alias.c_str()) &&
          !strcmp(it->schema_name.c_str(), schema_name.c_str())) {
        ret = *it;
        return ret;
      }
    }
  }
  if (ret_has_value) return ret;
  string error_message("Could not find table: ");
  error_message.append(schema_name);
  error_message.append(" with alias: ");
  error_message.append(table_alias);

  throw Error(error_message.c_str());
  return ret;
}

/*Try to merge the dataspace of parent record_scan and child record_scan.
 *
 * Return: The merged dataspace or NULL if fail.*/
DataSpace *Statement::merge_parent_child_record_scan(record_scan *parent_rs,
                                                     record_scan *child_rs,
                                                     int record_scan_position,
                                                     ExecutePlan *plan) {
#ifdef DEBUG
  ACE_ASSERT(child_rs);
  ACE_ASSERT(parent_rs);
  if (child_rs->upper != parent_rs) {
    LOG_ERROR(
        "Wrong params for merge_parent_child_record_scan,"
        " the child_rs should be the child of parent_rs.\n");
    ACE_ASSERT(0);
  }
#endif

  bool child_is_par_table = false;
  bool parent_is_par_table = false;

  DataSpace *parent_ds = record_scan_one_space_tmp_map.count(parent_rs)
                             ? record_scan_one_space_tmp_map[parent_rs]
                             : NULL;
  DataSpace *child_ds = record_scan_one_space_tmp_map.count(child_rs)
                            ? record_scan_one_space_tmp_map[child_rs]
                            : NULL;
  if (st.type == STMT_CREATE_SELECT) {
    if (parent_ds && child_ds &&
        stmt_session->is_dataspace_cover_session_level(parent_ds, child_ds)) {
      return parent_ds;
    }
    return NULL;
  }
  if (left_join_subquery_rs.count(child_rs)) {
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return NULL due to "
        "left_join_subquery_rs.count.\n");
    return NULL;
  }

  if (record_scan_dependent_map.count(child_rs) &&
      !record_scan_dependent_map[child_rs].empty()) {
    // If has dependent child record scan, it need to be executed separared.
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return NULL due to "
        "record_scan_dependent_map.count.\n");
    return NULL;
  }

  /*if the child is a cross node join, return NULL.*/
  if (record_scan_need_cross_node_join.count(child_rs)) {
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return NULL due to "
        "record_scan_need_cross_node_join.count.\n");
    return NULL;
  }

  if (!child_ds) {
    if (child_rs->is_select_union &&
        !record_scan_has_no_table.count(child_rs)) {
      LOG_DEBUG(
          "Statement::merge_parent_child_record_scan return NULL due to "
          "child_rs->is_select_union.\n");
      return NULL;
    }
    /*The child record scan is a simple select, such as "select 1", so just
     * merged it with parent.*/
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return parent_ds due to "
        "child_ds is NULL.\n");
    return parent_ds;
  }

  if (child_rs->first_table && child_rs->first_table->join) {
    string table_name = child_rs->first_table->join->table_name;
    if (!strcmp(table_name.c_str(), "dual")) {
      LOG_DEBUG(
          "Statement::merge_parent_child_record_scan return parent_ds due to "
          "child_rs table is dual.\n");
      return parent_ds;
    }
  }

  /*Duplicated table can not be merged cross record scan.*/
  if (parent_ds && parent_ds->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)parent_ds)->is_duplicated()) {
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return parent_ds due to "
        "parent_ds is_duplicated.\n");
    return NULL;
  }
  if (child_ds && child_ds->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)child_ds)->is_duplicated()) {
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return parent_ds due to "
        "child_ds is_duplicated.\n");
    return parent_ds;
  }

  if (child_rs->condition_belong && record_scan_need_table.count(parent_rs)) {
    LOG_DEBUG(
        "Statement::merge_parent_child_record_scan return NULL due to "
        "record_scan_need_table.count.\n");
    return NULL;
  }

  if (subquery_missing_table.count(parent_rs)) {
    /* For dependent situation: 1. if norm_table, check cover.  2. if
     * part_table, using its child record scan to check can merge.
     */
    TableStruct table = subquery_missing_table[parent_rs];
    Backend *backend = Backend::instance();
    DataSpace *ret_ds = backend->get_data_space_for_table(
        table.schema_name.c_str(), table.table_name.c_str());
    if (parent_ds != ret_ds &&
        !record_scan_need_cross_node_join.count(parent_rs)) {
      LOG_ERROR(
          "The sql [%s] is not supported, parent final table is not child "
          "missing table.\n",
          sql);
      throw Error(
          "The sql is not supported, parent final table is not child missing "
          "table.");
    }
    if (child_ds->get_data_source() && ret_ds->get_data_source()) {
      if (stmt_session->is_dataspace_cover_session_level(ret_ds, child_ds))
        return ret_ds;
    } else if (!child_ds->get_data_source() && !ret_ds->get_data_source()) {
      if (check_two_partition_table_can_merge(
              child_rs, missing_table_map[table], (PartitionedTable *)child_ds,
              (PartitionedTable *)ret_ds)) {
        return ret_ds;
      } else {
        return NULL;
      }
    } else {
      return NULL;
    }
  } else if (check_column_record_scan_can_merge(parent_rs, child_rs,
                                                record_scan_position, plan)) {
    return parent_ds;
  }

  if (check_spj_subquery_can_merge(parent_rs, child_rs)) {
    if (record_scan_par_table_map.count(child_rs)) {
      record_scan_par_table_map[parent_rs] =
          record_scan_par_table_map[child_rs];
    }
    return child_ds;
  }

  if (child_ds->get_data_source() == NULL) child_is_par_table = true;
  if (parent_ds && parent_ds->get_data_source() == NULL)
    parent_is_par_table = true;

  if (child_is_par_table && parent_is_par_table) {
    /*two partition table inside different sub-querys can not be executed
      together currently.*/
    return NULL;
  }

  if (!parent_rs->upper &&
      (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT ||
       st.type == STMT_SET)) {
    // Only do merge for insert...select and set stmt with normal table
    if (child_is_par_table || parent_is_par_table) return NULL;
  }

  if (child_is_par_table) {
    if (is_subquery_in_select_list(child_rs))
      /*If partition table subquery is in the select list, it can not be
       * merged.*/
      return NULL;
    table_link *par_table = record_scan_par_table_map[child_rs];
    if ((rs_contains_aggr_group_limit(child_rs, par_table) ||
         (child_rs->options & SQL_OPT_DISTINCT ||
          child_rs->options & SQL_OPT_DISTINCTROW)) &&
        (!check_rs_contain_partkey_equal_value(child_rs))) {
      /*one or merged record_scan should be executed separared if it contains
       * order_by|limit|group_by|distinct, as well as it has upper record_scan.
       */
      if (is_executed_as_table(child_rs) && !parent_ds &&
          parent_rs->children_begin == child_rs &&
          parent_rs->children_end == child_rs) {
        if (check_rs_contains_partition_key(child_rs, child_ds) != -1) {
          dataspace_partition_key[child_rs] =
              ((PartitionedTable *)child_ds)->get_key_names()->at(0);
          table_subquery_final_dataspace[child_rs] = child_ds;
        } else {
          string column_name;
          field_item *fi = child_rs->field_list_head;
          if (fi->alias) {
            dataspace_partition_key[child_rs] = fi->alias;
            table_subquery_final_dataspace[child_rs] = child_ds;
          } else if (fi->field_expr && fi->field_expr->type == EXPR_STR) {
            fi->field_expr->to_string(column_name);
            if (strcmp(column_name.c_str(), "*")) {
              string local_schema;
              string local_table;
              string local_column;
              split_value_into_schema_table_column(column_name, local_schema,
                                                   local_table, local_column);
              dataspace_partition_key[child_rs] = local_column;
              table_subquery_final_dataspace[child_rs] = child_ds;
            } else {
              Backend::instance()->record_latest_important_dbscale_warning(
                  "The first column fail to be partition key of tmp table, "
                  "this table subquery use normal table directly.\n");
            }
          } else {
            Backend::instance()->record_latest_important_dbscale_warning(
                "The first column fail to be partition key of tmp table, this "
                "table subquery use normal table directly.\n");
          }
        }

        if (!parent_rs->upper && table_subquery_final_dataspace.count(child_rs))
          need_reanalysis_space = true;
      }
      return NULL;
    }

    /*When Partition record_scan is a sub_select condition,
      If the condition type is 'exists', it can not execute without divided;
      (may allow in the future)
      If the condition type is 'in'|'=', the select list of
      partition record_scan should contain the partition key.*/
    if (child_rs->condition_belong) {
      Expression *expr = child_rs->condition_belong->parent;
      if (expr == NULL) {
        /*This subquery, with partition table, is in the select list.*/
        /*TODO: we need to separated the subquery in select list and condition
         * list!!!*/
        return NULL;
      }
      switch (expr->type) {
        case EXPR_EQ:
        case EXPR_IN: {
          vector<const char *> *key_names =
              ((PartitionedTable *)child_ds)->get_key_names();
          table_link *par_table = record_scan_par_table_map[child_rs];
          const char *schema_name = par_table->join->schema_name
                                        ? par_table->join->schema_name
                                        : schema;
          const char *table_name = par_table->join->table_name;
          const char *table_alias = par_table->join->alias;

          // select field list should contain all 'par.key'
          bool field_has_all_key = true;
          vector<const char *>::iterator it = key_names->begin();
          for (; it != key_names->end(); ++it) {
            bool field_has_key = false;
            const char *key_name = *it;
            field_item *tmp = child_rs->field_list_head;
            while (tmp) {
              Expression *expr_field = tmp->field_expr;
              if (expr_field && expr_field->type == EXPR_STR) {
                StrExpression *str_field = (StrExpression *)expr_field;
                if (is_column(str_field)) {
                  if (column_name_equal_key_alias(
                          str_field->str_value, schema_name,
                          strlen(schema_name), table_name, strlen(table_name),
                          key_name, table_alias)) {
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
            return NULL;
          }
          break;
        }
        default:  // NOT_IN, other compare
        {
          return NULL;
        }
      }
    }
  }

  if (!parent_ds) {
    if (child_is_par_table) {
      field_item *field = parent_rs->field_list_head;
      try {
        while (field) {
          if (field->field_expr) {
            if (is_expression_contain_aggr(field->field_expr, parent_rs,
                                           false)) {
              if (parent_rs->upper) return NULL;
            }
          }
          field = field->next;
        }
      } catch (...) {
        return NULL;
      }

      if (!record_scan_par_table_map.count(parent_rs) &&
          record_scan_par_table_map.count(child_rs)) {
        record_scan_par_table_map[parent_rs] =
            record_scan_par_table_map[child_rs];
      }
    }
    return child_ds;
  }

  if (stmt_session->is_dataspace_cover_session_level(parent_ds, child_ds)) {
    /* We defined that two record_scan can not merge. */
    join_node *join_tables = parent_rs->join_tables;
    if (join_tables && join_tables->join_bit & JT_LEFT &&
        child_rs->subquerytype == SUB_SELECT_TABLE) {
      return NULL;
    }
    return parent_ds;
  }

  if (!parent_rs->upper &&
      (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT)) {
    /*For insert..select stmt, should not use child_rs to merge parent_rs.*/
    return NULL;
  }

  if (stmt_session->is_dataspace_cover_session_level(child_ds, parent_ds)) {
    /* We defined that two record_scan can not merge. */
    join_node *join_tables = parent_rs->join_tables;
    if (join_tables && join_tables->join_bit & JT_LEFT &&
        child_rs->subquerytype == SUB_SELECT_TABLE) {
      return NULL;
    }
    if (record_scan_par_table_map.count(child_rs)) {
      record_scan_par_table_map[parent_rs] =
          record_scan_par_table_map[child_rs];
    }

    return child_ds;
  }

  return NULL;
}

bool Statement::check_two_partition_table_can_merge(record_scan *rs,
                                                    table_link *tb_link,
                                                    PartitionedTable *tb1,
                                                    PartitionedTable *tb2) {
  /*If the two partition table use the same deploy topo(the same
   * partition_scheme and the same partition type), we can just ignore
   * one of them. Cause now dbscale only support local join. In order
   * to ensure the correct of execution, the condition of record_scan
   * should contains the pattern {par1.key = par2.key}*/
  if (is_partition_table_cover(tb1, tb2)) {
    vector<const char *> *key_names = tb2->get_key_names();
    vector<const char *> *kept_keys = tb1->get_key_names();
    unsigned int j = 0;
    for (; j < kept_keys->size(); ++j) {
      const char *kept_key = kept_keys->at(j);
      const char *key_name = key_names->at(j);
      if (!is_all_par_table_equal(rs, tb_link, kept_key, key_name)) {
        break;
      }
    }
    if (j == kept_keys->size()) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

bool Statement::check_CNJ_two_partition_table_can_merge(record_scan *rs,
                                                        table_link *tb_link1,
                                                        table_link *tb_link2,
                                                        PartitionedTable *tb1,
                                                        PartitionedTable *tb2) {
  /*If the two partition table use the same deploy topo(the same
   * partition_scheme and the same partition type), we can just ignore
   * one of them. Cause now dbscale only support local join. In order
   * to ensure the correct of execution, the condition of record_scan
   * should contains the pattern {par1.key = par2.key}*/
  if (is_partition_table_cover(tb1, tb2)) {
    vector<const char *> *key_names = tb2->get_key_names();
    vector<const char *> *kept_keys = tb1->get_key_names();
    unsigned int j = 0;
    for (; j < kept_keys->size(); ++j) {
      const char *kept_key = kept_keys->at(j);
      const char *key_name = key_names->at(j);
      if (!is_CNJ_all_par_table_equal(rs, tb_link1, tb_link2, kept_key,
                                      key_name)) {
        break;
      }
    }
    if (j == kept_keys->size()) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

void Statement::get_all_table_vector(
    record_scan *rs, vector<vector<TableStruct> > *join_table_sequence) {
  join_node *tables = rs->join_tables;
  get_all_tables(tables, join_table_sequence);
}
void Statement::get_all_tables(
    join_node *tables, vector<vector<TableStruct> > *join_table_sequence) {
  const char *table_name, *schema_name, *alias;
  if (tables->left) get_all_tables(tables->left, join_table_sequence);
  if (tables->right) get_all_tables(tables->right, join_table_sequence);
  if (tables->type != JOIN_NODE_JOIN) {
    TableStruct table_struct;
    table_name = tables->table_name;
    schema_name = tables->schema_name ? tables->schema_name : schema;
    alias = tables->alias ? tables->alias : table_name;
    if (table_name) {
      table_struct.table_name = table_name;
      table_struct.alias = table_name;
    }
    if (schema_name) table_struct.schema_name = schema_name;
    if (alias) {
      table_struct.alias = alias;
      if (tables->sub_select) table_struct.table_name = alias;
    }
    (*join_table_sequence)[0].push_back(table_struct);
  }
}

void Statement::get_table_vector(
    record_scan *rs, vector<vector<TableStruct> > *join_table_sequence) {
  const char *table_name, *schema_name, *alias;
  table_link *tmp_tb = rs->first_table;
  while (tmp_tb) {
    if (tmp_tb->join->cur_rec_scan == rs) {
      TableStruct table_struct;
      table_name = tmp_tb->join->table_name;
      schema_name =
          tmp_tb->join->schema_name ? tmp_tb->join->schema_name : schema;
      alias = tmp_tb->join->alias ? tmp_tb->join->alias : table_name;
      table_struct.table_name = table_name;
      table_struct.schema_name = schema_name;
      table_struct.alias = alias;
      (*join_table_sequence)[0].push_back(table_struct);
      LOG_DEBUG(
          "Get table vector table_name = [%s], schema_name = [%s], alias = "
          "[%s]\n",
          table_struct.table_name.c_str(), table_struct.schema_name.c_str(),
          table_struct.alias.c_str());
    }
    tmp_tb = tmp_tb->next;
  }
}

vector<TableStruct *> *Statement::get_ta