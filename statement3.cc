      if (!ret) {
          schema->set_schema_pushdown_stored_procedure(old_val);
          throw Error("got error when update schema config in config source");
        }
#ifndef CLOSE_MULTIPLE
        if (multiple_mode) {
          Backend::instance()->flush_config_to_zoo();
        }
#endif
      }
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      break;
    }
    case STMT_DBSCALE_DYNAMIC_SET_MASTER_PRIORITY: {
      if (!strcmp(normal_admin_user, plan->session->get_username()) == 0 &&
          !strcmp(supreme_admin_user, plan->session->get_username()) == 0 &&
          !strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
        LOG_ERROR(
            "Current User does not have the priority of dynamic set master "
            "priority.\n");
        throw Error(
            "Current User does not have the priority of dynamic set master "
            "priority.");
      }

      if (multiple_mode) {
        if (st.sql->dbscale_set_master_priority_oper->is_local) {
          Backend::instance()->set_server_master_priority(
              st.sql->dbscale_set_master_priority_oper->server_name,
              st.sql->dbscale_set_master_priority_oper->master_priority);
        } else {
          MultipleManager::instance()->set_master_priority(
              string(st.sql->dbscale_set_master_priority_oper->server_name),
              st.sql->dbscale_set_master_priority_oper->master_priority);
        }
      } else
        Backend::instance()->set_server_master_priority(
            st.sql->dbscale_set_master_priority_oper->server_name,
            st.sql->dbscale_set_master_priority_oper->master_priority);

      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      break;
    }
    case STMT_DBSCALE_RELOAD_MIRROR_USER: {
      check_dbscale_management_acl(plan);
      Backend::instance()->reload_mirror_user();
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      break;
    }
    case STMT_DBSCALE_RESET_TMP_TABLE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      dbscale_reset_tmp_table_op_node *reset_oper =
          st.sql->dbscale_reset_tmp_table_oper;
      int type = reset_oper->type;
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      // type == 2 is used just for dbscale internal, the command type that
      // slave recieved from master is 2.
      if (is_master || type == RESET_SLAVE ||
          (type == RESET_DROP && reset_oper->table_pool_name)) {
#endif
        assemble_dbscale_reset_tmp_table_plan(plan);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("reset tmp join tables cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_RESET_ZOO_INFO: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) == 0) {
        assenble_dbscale_reset_zoo_info_plan(plan);
      } else {
        LOG_ERROR(
            "Current User does not have the privilege of zookeeper reset "
            "info.\n");
        throw Error(
            "Current User does not have the privilege of zookeeper reset "
            "info.");
      }
    } break;
    case STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(normal_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege to execute "
            "STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS, only "
            "root/dbscale/dbscale_internal_user can.\n");
        throw Error(
            "Current User does not have the privilege to execute "
            "STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS, only "
            "root/dbscale/dbscale_internal_user can.");
      }
      LOG_DEBUG("Assemble dbscale set_info_mirror_tb_status_plan.\n");
      info_mirror_tb_status_node *tb_status =
          st.sql->info_mirror_tb_status_oper;
      ExecuteNode *node =
          plan->get_set_info_schema_mirror_tb_status_node(tb_status);
      plan->set_start_node(node);
      return;
    } break;
    case STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS_OUT_OF_DATE: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(normal_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege to execute "
            "STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS_OUT_OF_DATE,"
            " only root/dbscale/dbscale_internal_user can.\n");
        throw Error(
            "Current User does not have the privilege to execute "
            "STMT_SET_INFO_SCHEMA_MIRROR_TB_STATUS_OUT_OF_DATE,"
            " only root/dbscale/dbscale_internal_user can.");
      }
      LOG_DEBUG(
          "Assemble dbscale set_info_mirror_tb_status_out_of_date_plan.\n");
      const char *db_tb_name =
          st.mirror_tb_out_of_date_tb_name;  // if not null, should be like
                                             // table_schema.table_name
      const char *schema_name = DBSCALE_RESERVED_STR;
      const char *table_name = DBSCALE_RESERVED_STR;
      vector<string> strs;
      if (db_tb_name) {
        boost::split(strs, db_tb_name, boost::is_any_of("."));
        if (strs.size() != 2) {
          throw Error(
              "the mirror table name should be like table_schema.table_name");
        }
        schema_name = strs[0].c_str();
        table_name = strs[1].c_str();
      }
      Backend::instance()->set_informationschema_mirror_tb_out_of_date(
          true, schema_name, table_name);
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      return;
    } break;
    case STMT_DBSCALE_DYNAMIC_CHANGE_MASTER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        if (!mul->get_is_cluster_master()) {
          LOG_DEBUG(
              "Dynamic change master operation be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      assemble_dynamic_change_master_plan(plan,
                                          st.sql->dynamic_change_master_oper);
    } break;
    case STMT_DBSCALE_SET_POOL_INFO: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_set_pool_plan(plan, st.sql->pool_oper);
    } break;
    case STMT_DBSCALE_RESET_INFO: {
      assemble_dbscale_reset_info_plan(plan, st.sql->dbscale_reset_info_oper);
    } break;
    case STMT_DBSCALE_BLOCK: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_block_plan(plan);
    } break;
    case STMT_DBSCALE_FLASHBACK_FORCE_ONLINE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        if (!mul->get_is_cluster_master()) {
          LOG_DEBUG(
              "flashback force online operation be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      assemble_dbscale_force_flashback_online_plan(plan);
    } break;
    case STMT_DBSCALE_XA_RECOVER_SLAVE_DBSCALE: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_xa_recover_slave_dbscale_plan(plan);
    } break;
    case STMT_DBSCALE_CHECK_TABLE: {
      ExecuteNode *node = plan->get_dbscale_check_table_node();
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_CHECK_METADATA: {
      bool is_master = true;
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
#endif
      if (!is_master) {
        assemble_forward_master_role_plan(plan);
      } else {
        ExecuteNode *node = plan->get_dbscale_check_metadata();
        plan->set_start_node(node);
      }
    } break;
    case STMT_DBSCALE_CHECK_DISK_IO: {
      ExecuteNode *node = plan->get_dbscale_check_disk_io_node();
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_DISABLE_SERVER: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_disable_server_plan(plan);
    } break;
    case STMT_DBSCALE_FLUSH_POOL_VERSION: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_flush_pool_plan(plan, st.sql->pool_oper);
    } break;
    case STMT_DBSCALE_PURGE_CONNECTION_POOL: {
      check_dbscale_management_acl(plan);
      LOG_DEBUG("Assemble dbscale purge connection pool plan.\n");
      ExecuteNode *node =
          plan->get_dbscale_purge_connection_pool_node(st.sql->pool_oper);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_FLUSH: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        if (!mul->get_is_cluster_master()) {
          LOG_DEBUG("Flush View on slave dbscale, so forword to master.\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      assemble_dbscale_flush_plan(plan, st.sql->dbscale_flush_oper->type);
    } break;
    case STMT_DBSCALE_FLUSH_WEAK_PASSWORD_FILE: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_flush_weak_pwd_plan(plan);
    } break;
    case STMT_DBSCALE_RELOAD_CONFIG: {
      check_dbscale_management_acl(plan);
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        if (!mul->get_is_cluster_master()) {
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
      ExecuteNode *node = plan->get_dbscale_reload_config_node(sql);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_RELOAD_SLAVE_CONFIG: {
      check_dbscale_management_acl(plan);
      ExecuteNode *node = plan->get_dbscale_reload_config_node(sql);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_SET_PRIORITY: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_set_priority_plan(plan);
    } break;
    case STMT_DBSCALE_DYNAMIC_CHANGE_MULTIPLE_MASTER_ACTIVE: {
      check_dbscale_management_acl(plan);
      assemble_dynamic_change_multiple_master_active_plan(
          plan, st.sql->dynamic_change_multiple_master_active_oper);
    } break;
    case STMT_DBSCALE_DYNAMIC_CHANGE_REMOTE_SSH: {
      check_dbscale_management_acl(plan);

      const char *server_name =
          st.sql->dynamic_change_dataserver_ssh_oper->server_name;
      const char *username =
          st.sql->dynamic_change_dataserver_ssh_oper->username;
      const char *pwd = st.sql->dynamic_change_dataserver_ssh_oper->pwd;
      int port = st.sql->dynamic_change_dataserver_ssh_oper->port;
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }

      bool need_forward =
          st.sql->dynamic_change_dataserver_ssh_oper->need_forward_to_master;
      if (is_master || !need_forward) {
#endif
        assemble_dynamic_change_dataserver_ssh_plan(plan, server_name, username,
                                                    pwd, port);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add slave cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_REMOVE_SLAVE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_remove_slave_plan(plan,
                                           st.sql->dynamic_remove_slave_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add slave cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_REMOVE_SCHEMA: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        const char *schema_name =
            st.sql->dynamic_remove_schema_oper->schema_name;
        bool is_force = st.sql->dynamic_remove_schema_oper->is_force;
        assemble_dynamic_remove_schema_plan(plan, schema_name, is_force);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "dynamic remove schema dataspace cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_REMOVE_TABLE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        const char *table_name = st.sql->dynamic_remove_table_oper->table_name;
        bool is_force = st.sql->dynamic_remove_table_oper->is_force;
        assemble_dynamic_remove_table_plan(plan, table_name, is_force);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "dynamic remove table dataspace cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_CHANGE_TABLE_SCHEME: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        const char *table_name =
            st.sql->dynamic_remove_table_pattern_oper->table_name;
        const char *scheme_name =
            st.sql->dynamic_remove_table_pattern_oper->scheme_name;
        assemble_dynamic_change_table_scheme_plan(plan, table_name,
                                                  scheme_name);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "dynamic remove table dataspace cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_REMOVE_DATASERVER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_remove_plan(plan);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add slave cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif

    } break;
    case STMT_DBSCALE_DYNAMIC_REMOVE_PARTITION_SCHEME:
    case STMT_DBSCALE_DYNAMIC_REMOVE_DATASOURCE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_remove_plan(plan);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic remove datasource on slave dbscale.\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_SHOW_HELP: {
      assemble_dbscale_help_plan(plan, st.sql->show_help_oper->cmd_name);
    } break;
    case STMT_DBSCALE_SHOW_SLOW_SQL_TOP_N: {
      if (slow_query_time == 0) {
        throw SlowQueryDisabledError();
      }
      LOG_DEBUG("Assemble dbscale show slow sql top n plan\n");
      ExecuteNode *node = plan->get_dbscale_show_slow_sql_top_n_node();
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_REQUEST_SLOW_SQL_TOP_N: {
      if (slow_query_time == 0) {
        throw SlowQueryDisabledError();
      }
      LOG_DEBUG("Assemble dbscale request slow sql top n plan\n");
      ExecuteNode *node = plan->get_dbscale_request_slow_sql_top_n_node();
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_SHOW_AUDIT_USER_LIST: {
      if (strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
          strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
          strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
        LOG_DEBUG("Assemble dbscale show audit user list plan\n");
        ExecuteNode *node = plan->get_dbscale_show_audit_user_list_node();
        plan->set_start_node(node);
      } else {
        LOG_ERROR(
            "Current User does not have the privilege to show audit user "
            "list.\n");
        throw Error(
            "Current User does not have the privilege to show audit user list");
      }
    } break;
    case STMT_DBSCALE_SHOW_WHITE_LIST: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, no ACL info to show\n");
        throw Error("enable-acl=0, no ACL info to show");
      }
      if (strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
          strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
          strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
        assemble_dbscale_show_white_list(plan);
      } else {
        LOG_ERROR(
            "Current User does not have the privilege to check white list.\n");
        throw Error(
            "Current User does not have the privilege to check white list.");
      }
    } break;
    case STMT_DBSCALE_SHOW_JOIN_STATUS: {
      assemble_dbscale_show_join_plan(plan, st.sql->show_join_oper->name);
    } break;
    case STMT_DBSCALE_SHOW_BASE_STATUS: {
      assemble_dbscale_show_base_status_plan(plan);
    } break;
    case STMT_DBSCALE_MONITOR_POINT_STATUS:
    case STMT_DBSCALE_HANDLER_MONITOR_POINT_STATUS: {
      assemble_show_monitor_point_status_plan(plan);
    } break;
    case STMT_DBSCALE_GLOBAL_MONITOR_POINT_STATUS: {
      assemble_show_global_monitor_point_status_plan(plan);
    } break;
    case STMT_DBSCALE_HISTOGRAM_MONITOR_POINT_STATUS: {
      assemble_show_histogram_monitor_point_status_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_OUTLINE_INFO: {
      assemble_dbscale_show_outline_monitor_info_plan(plan);
      break;
    }
    case STMT_DBSCALE_CREATE_OUTLINE_HINT: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dbscale_create_outline_hint_plan(
            plan, st.sql->dbscale_operate_outline_hint_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "create_outline_hint cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      break;
    }
    case STMT_DBSCALE_FLUSH_OUTLINE_HINT: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dbscale_flush_outline_hint_plan(
            plan, st.sql->dbscale_operate_outline_hint_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "flush_outline_hint cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      break;
    }
    case STMT_DBSCALE_SHOW_OUTLINE_HINT: {
      assemble_dbscale_show_outline_hint_plan(
          plan, st.sql->dbscale_operate_outline_hint_oper);
      break;
    }
    case STMT_DBSCALE_DELETE_OUTLINE_HINT: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dbscale_delete_outline_hint_plan(
            plan, st.sql->dbscale_operate_outline_hint_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG(
            "delete_outline_hint cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      break;
    }
    case STMT_DBSCALE_SHOW_POOL_INFO: {
      assemble_show_pool_info_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_POOL_VERSION: {
      assemble_show_pool_version_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_LOCK_USAGE: {
      assemble_show_lock_usage_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_EXECUTION_PROFILE: {
      assemble_show_execution_profile_plan(plan);
      break;
    }
    case STMT_CHANGE_DB: {
      const char *schema_name = st.sql->change_db_oper->dbname->name;
      dataspace = backend->get_data_space_for_table(schema_name, NULL);
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_CREATE_DB: {
      const char *schema_name = st.sql->show_create_db_oper->dbname->name;
      dataspace = backend->get_data_space_for_table(schema_name, NULL);
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    // see issue #1888
    // NEW FOR REMOTE DISASTER RECOVERY
    case STMT_SHOW_BINARY_LOGS:
    case STMT_RESET_SLAVE:
    case STMT_START_SLAVE:
    case STMT_STOP_SLAVE:
    case STMT_CHANGE_MASTER: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        if (slave_dbscale_mode) {
          plan->session->set_read_only(false);
          dataspace = backend->get_catalog();
          assemble_direct_exec_plan(plan, dataspace);
          backend->set_need_send_stop_slave_flag(true);
        } else {
          ExecuteNode *node = plan->get_slave_dbscale_error_node();
          plan->set_start_node(node);
        }
#ifndef CLOSE_MULTIPLE
      } else {
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    // END NEW FOR REMOTE DISASTER RECOVERY
    case STMT_SHOW_PROCESSLIST:
    case STMT_SHOW_MASTER_STATUS:
    case STMT_SHOW_SLAVE_STATUS: {
      if (slave_dbscale_mode) plan->session->set_read_only(false);
      dataspace = backend->get_catalog();
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_CREATE_TRIGGER: {
      dataspace = backend->get_auth_data_space();
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_TRIGGERS: {
      const char *schema_name = st.sql->show_triggers_oper->db_name;
      if (schema_name) {
        dataspace = backend->get_data_space_for_table(schema_name, NULL);
      } else {
        dataspace = backend->get_data_space_for_table(schema, NULL);
      }
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_CREATE_FUNCTION_OR_PROCEDURE: {
      const char *schema_name = st.sql->show_create_func_or_proc_oper->db_name;
      if (schema_name) {
        dataspace = backend->get_data_space_for_table(schema_name, NULL);
      } else {
        dataspace = backend->get_data_space_for_table(schema, NULL);
      }
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_CREATE_VIEW: {
      if (is_partial_parsed()) {
        LOG_ERROR("partial parsed show create view sql\n");
        throw Error("fail to parse show create view sql");
      }
      if (on_view) {
        dataspace = backend->get_config_data_space();
        if (!dataspace) {
          LOG_ERROR("Can not find the metadata dataspace.\n");
          throw Error(
              "Fail to get meta dataspace, please make sure meta datasource is "
              "configured.");
        }
      } else {
        /*If dbscale no need to maintain view, the view will be created on the
         * schema dataspace.*/
        create_view_op_node *view_node = st.sql->view_oper;
        const char *view_schema =
            view_node->schema ? view_node->schema : schema;
        dataspace = backend->get_data_space_for_table(view_schema, NULL);
      }
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_FUNCTION_OR_PROCEDURE_CODE: {
      dataspace = backend->get_metadata_data_space();
      if (!dataspace) {
        dataspace = backend->get_catalog();
      }
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_FUNCTION_OR_PROCEDURE_STATUS: {
      dataspace = backend->get_metadata_data_space();
      if (!dataspace) {
        assemble_show_func_or_proc_status_plan(plan);
      } else {
        assemble_direct_exec_plan(plan, dataspace);
      }
    } break;
    case STMT_ALTER_FUNCTION_OR_PROCEDURE: {
      bool is_centralized_cluster = backend->is_centralized_cluster();
      if (is_centralized_cluster) {
        dataspace = backend->get_catalog();
        assemble_direct_exec_plan(plan, dataspace);
      } else {
        throw NotSupportedError(
            "Only support ALTER PROCEDURE under centralized cluster");
      }
    } break;
    case STMT_DBSCALE_CLEAN_PROCEDURE_METADATA: {
      const char *sp_schema = st.routine_d->proc_node->schema_name
                                  ? st.routine_d->proc_node->schema_name
                                  : schema;
      const char *sp_name = st.routine_d->proc_node->proc_name;

      string sp_full_name = sp_schema;
      sp_full_name.append(".").append(sp_name);

      backend->remove_one_procedure_meta_data_local(sp_full_name);
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
    } break;
    case STMT_CREATE_DB:
    case STMT_DROP_DB:
    case STMT_ALTER_DB: {
#ifndef CLOSE_MULTIPLE
      /*Due to the create db may need to create new dataspace, which need to
       * sync in multiple mode, so forward to master role dbscale.*/
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        bool is_master = mul->get_is_cluster_master();

        if (!is_master) {
          LOG_DEBUG("CREATE/DROP DB statment be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
    }
      assemble_create_db_plan(plan);
      break;
    case STMT_KILL_THREAD: {
      bool is_kill_query = st.sql->kill_oper->is_kill_query;
      if (is_kill_query) {
        LOG_ERROR("Not support KILL QUERY statement, use KILL instead.\n");
        throw NotSupportedError("Not support KILL QUERY, use KILL instead");
      }

      uint32_t kid = 0;
      if (st.sql->kill_oper->kid_is_uservar) {
        map<string, string> *user_var_map = plan->session->get_user_var_map();
        string var_name(st.sql->kill_oper->kid_uservar_str);
        boost::to_upper(var_name);
        string var_value;
        if (user_var_map->count(var_name)) {
          var_value = (*user_var_map)[var_name];
          if (!is_natural_number(var_value.c_str())) {
            LOG_ERROR("Handler %@ killing with invalid thread id [%s].\n",
                      plan->handler, var_value.c_str());
            throw Error("Killing with invalid thread id");
          }
          kid = atoi(var_value.c_str());
        } else {
          LOG_ERROR(
              "Handler %@ killing with invalid thread id, uservar not exist.\n",
              plan->handler);
          throw Error("Killing with invalid thread id");
        }
      } else {
        kid = (uint32_t)st.sql->kill_oper->kid;
      }

      if (kid == 0) {
        LOG_ERROR("Handler %@ killing with invalid thread id [0].\n",
                  plan->handler);
        throw Error("Killing with invalid thread id");
      }
      const char *cluster_id = st.sql->kill_oper->cluster_id;
      int killed_cluster_id = 0;
      if (cluster_id) {
        killed_cluster_id = atoi(cluster_id);
        if (killed_cluster_id == 0)
          throw Error("Killing with invalid cluster id");
      }

      if (kid == plan->handler->get_thread_id()) {
        if (cluster_id == NULL ||
            killed_cluster_id == backend->get_cluster_id()) {
          Backend::instance()->record_latest_important_dbscale_warning(
              "Handler %p self killing with thread id %d.\n",
              (void *)(plan->handler), kid);
          throw ThreadIsKilled();
        }
      }
      if (cluster_id == NULL) {
        killed_cluster_id = backend->get_cluster_id();
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master && killed_cluster_id != backend->get_cluster_id()) {
        assemble_forward_master_role_plan(plan);
        break;
      } else {
#endif
        assemble_kill_plan(plan, killed_cluster_id, kid);
#ifndef CLOSE_MULTIPLE
      }
#endif
    } break;
    case STMT_DBSCALE_SET: {
      dynamic_config_op_node *config_oper = st.sql->dynamic_config_oper;
      var_scope_type vst = config_oper->vst;

      check_disaster_relate_option(config_oper);
      if (!(strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
            strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
            strcmp(dbscale_internal_user, plan->session->get_username()) == 0 ||
            vst == VAR_SCOPE_SESSION)) {
        LOG_ERROR(
            "Current User does not have the privilege of dynamic "
            "configuration.\n");
        throw Error(
            "Current User does not have the privilege of dynamic "
            "configuration.");
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master && vst == VAR_SCOPE_GLOBAL) {
        LOG_DEBUG(
            "dynamic set global option cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      } else {
        if (vst == VAR_SCOPE_INSTANCE && config_oper->cluster_id > 0 &&
            backend->get_cluster_id() != config_oper->cluster_id) {
          if (!is_master) {
            LOG_DEBUG(
                "dynamic set instance option cmd with other dbscale cluster id "
                "be execute on slave dbscale\n");
            assemble_forward_master_role_plan(plan);
            break;
          } else {
            MultipleManager *mul = MultipleManager::instance();
            mul->execute_master_query(sql, config_oper->cluster_id);
            mul->execute_master_query("dbscale flush config to file",
                                      config_oper->cluster_id);
            ExecuteNode *node = plan->get_return_ok_node();
            plan->set_start_node(node);
            break;
          }
        }
#endif
        assemble_dbscale_set_plan(plan);
#ifndef CLOSE_MULTIPLE
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_SET_SERVER_WEIGHT: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        if (strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
            strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
            strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
          assemble_dbscale_set_server_weight(plan);
        } else {
          LOG_ERROR(
              "Current User does not have the privilege to set server "
              "weight\n");
          throw Error(
              "Current User does not have the privilege to set server weight");
        }
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("Set server weight be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif

    } break;
    case STMT_DBSCALE_DYNAMIC_SET_AUTO_INCREMENT_OFFSET: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_set_auto_increment_offset(plan);
      break;
    } break;
    case STMT_DBSCALE_DYNAMIC_SET_REP_STRATEGY: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        if (strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
            strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
            strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
          assemble_dbscale_set_rep_strategy_plan(plan);
        } else {
          LOG_ERROR(
              "Current User does not have the privilege to change "
              " load_balance_strategy of replication data_source.\n");
          throw Error(
              "Current User does not have the privilege to change "
              "load_balance_strategy of replication data_source.");
        }
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("Set rep strategy be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_PURGE_MONITOR_POINT: {
      assemble_dbscale_purge_monitor_point_plan(plan);
    } break;
    case STMT_DBSCALE_CLEAN_MONITOR_POINT: {
      assemble_dbscale_clean_monitor_point_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_OPTION: {
      assemble_show_option_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_DYNAMIC_OPTION: {
      assemble_show_dynamic_option_plan(plan);
    } break;
    case STMT_DBSCALE_CHANGE_STARTUP_CONFIG: {
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      } else {
        LOG_ERROR(
            "Change startup config is only supported under Multiple DBScale "
            "mode.\n");
        throw NotSupportedError(
            "Change startup config is only supported under Multiple DBScale "
            "mode.");
      }
      if (is_master) {
        if (strcmp(normal_admin_user, plan->session->get_username()) == 0 ||
            strcmp(supreme_admin_user, plan->session->get_username()) == 0 ||
            strcmp(dbscale_internal_user, plan->session->get_username()) == 0) {
          assemble_change_startup_config_plan(plan);
        } else {
          LOG_ERROR(
              "Current User does not have the privilege to change startup "
              "config.\n");
          throw Error(
              "Current User does not have the privilege to change startup "
              "config.");
        }
      } else {
        LOG_DEBUG("Change startup config be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#else
      LOG_ERROR(
          "Change startup config is only supported under Multiple DBScale "
          "mode.\n");
      throw NotSupportedError(
          "Change startup config is only supported under Multiple DBScale "
          "mode.");
#endif
    } break;
    case STMT_DBSCALE_SHOW_CHANGED_STARTUP_CONFIG: {
      check_dbscale_management_acl(plan);
#ifdef CLOSE_MULTIPLE
      LOG_ERROR(
          "Change startup config is only supported under Multiple DBScale "
          "mode.\n");
      throw NotSupportedError(
          "Change startup config is only supported under Multiple DBScale "
          "mode.");
#endif
      if (!multiple_mode) {
        LOG_ERROR(
            "Show change startup config is only supported under Multiple "
            "DBScale mode.\n");
        throw NotSupportedError(
            "Show change startup config is only supported under Multiple "
            "DBScale mode.");
      }
      assemble_show_changed_startup_config_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_TRANSACTION_SQLS: {
      if (enable_record_transaction_sqls) {
        assemble_show_transaction_sqls_plan(plan);
      } else {
        LOG_ERROR("Record transaction sqls has been disabled.\n");
        throw NotSupportedError("Record transaction sqls has been disabled.");
      }
    } break;
    case STMT_SHOW_GRANTS:
    case STMT_SHOW_CREATE_USER: {
      if (is_partial_parsed()) throw Error("not support partial parsed sql");

      if (plan->session->is_in_lock()) {
        LOG_ERROR("Forbid grant in lock table mode.\n");
        throw NotSupportedError("Not support grant in lock table mode.");
      }
      replace_grant_hosts();
      dataspace = backend->get_auth_data_space();
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_GRANT: {
      if (is_partial_parsed()) throw Error("not support partial parsed sql");

      if (st.sql->grant_oper != NULL) {
        user_clause_item_list *user_clause_list =
            st.sql->grant_oper->user_clause;
        if (user_clause_list->next) {
          throw NotSupportedError(
              "not support multi users in one grant statement");
        }
        if (!user_clause_list->user_name ||
            strlen(user_clause_list->user_name) == 0) {
          throw NotSupportedError("not support anonymous user");
        }
        if (!is_ip_compatible(user_clause_list->host_name)) {
          throw NotSupportedError(
              "only support IP as host value, IP format can be an single \% or "
              "{num | \%}.{num | \%}.{num | \%}.{num | \%}");
        }
        if (st.sql->grant_oper->is_grant_proxy) {
          throw NotSupportedError(
              "handle_acl_by_grant_stmt not support grant proxy");
        }
        if (st.sql->grant_oper->object_type != GRANT_OBJECT_TYPE_TABLE) {
          throw NotSupportedError(
              "only support grant for table object currently");
        }
        if (st.sql->grant_oper->priv_level == GRANT_PRIV_LEVEL_A_STAR) {
          const char *c = st.sql->grant_oper->priv_level_db_name;
          if (c && (*c == '%' || *c == '_')) {
            throw NotSupportedError(
                "not support wildcard in db name when grant database level "
                "privilege");
          }
          if (strlen(c) > 1) {
            c += 1;
            while (*c) {
              if ((*c == '%' || *c == '_') && *(c - 1) != '\\') {
                throw NotSupportedError(
                    "not support wildcard in db name when grant database level "
                    "privilege");
              }
              ++c;
            }
          }
        }
        const char *password = user_clause_list->password;
        if (password ||
            !Backend::instance()->user_existed(user_clause_list->user_name)) {
          string err_msg;
          bool is_legal = validate_password_complex(password, err_msg);
          if (!is_legal) {
            LOG_ERROR("Password is illegal, %s.\n", err_msg.c_str());
            throw Error(err_msg.c_str());
          }
        }
      }
    }
    case STMT_DROP_USER:
    case STMT_CREATE_USER:
    case STMT_ALTER_USER:
    case STMT_REVOKE: {
      if (is_partial_parsed()) throw Error("not support partial parsed sql");

      if (plan->session->is_in_lock()) {
        LOG_ERROR("Forbid grant in lock table mode.\n");
        throw NotSupportedError("Not support grant in lock table mode.");
      }
      if (st.type == STMT_REVOKE) {
        if (st.sql->revoke_oper) {
          user_clause_item_list *user_clause_list =
              st.sql->revoke_oper->user_clause;
          if (user_clause_list->next) {
            throw NotSupportedError(
                "not support multi users in one revoke statement");
          }
        }
        if (st.sql->revoke_oper->priv_level == GRANT_PRIV_LEVEL_A_STAR) {
          const char *c = st.sql->revoke_oper->priv_level_db_name;
          if (strlen(c) > 1) {
            if (c && (*c == '%' || *c == '_')) {
              throw NotSupportedError(
                  "not support wildcard in db name when grant database level "
                  "privilege");
            }
            c += 1;
            while (*c) {
              if ((*c == '%' || *c == '_') && *(c - 1) != '\\') {
                throw NotSupportedError(
                    "not support wildcard in db name when revoke database "
                    "level privilege");
              }
              ++c;
            }
          }
        }
      }
      if (st.type == STMT_DROP_USER) {
        if (st.sql->drop_user_oper) {
          user_clause_item_list *user_clause_list =
              st.sql->drop_user_oper->user_clause;
          if (user_clause_list->next) {
            throw NotSupportedError(
                "not support multi users in one drop user statement");
          }
        }
      }
      if (st.type == STMT_CREATE_USER) {
        if (st.sql->create_user_oper != NULL) {
          user_clause_item_list *user_clause_list =
              st.sql->create_user_oper->user_clause;
          if (user_clause_list->next) {
            throw NotSupportedError(
                "not support multi users in one create user statement");
          }
          if (!is_ip_compatible(user_clause_list->host_name)) {
            throw NotSupportedError("only support IP as host value");
          }
          const char *password = user_clause_list->password;
          string err_msg;
          bool is_legal = validate_password_complex(password, err_msg);
          if (!is_legal) {
            LOG_ERROR("Password is illegal, %s.\n", err_msg.c_str());
            throw Error(err_msg.c_str());
          }
          if (Backend::instance()->get_backend_server_version() ==
              MYSQL_VERSION_57) {
            if (st.create_user_default_auth_plugin &&
                strcasecmp(st.create_user_default_auth_plugin,
                           "mysql_native_password")) {
              LOG_INFO(
                  "backend dataserver is based on 5.7, create user statement "
                  "should specify authentication_plugin as "
                  "'mysql_native_password' explicitly.");
              throw Error(
                  "backend dataserver is based on 5.7, create user statement "
                  "should specify authentication_plugin as "
                  "'mysql_native_password' explicitly.");
            }
          } else if (Backend::instance()->get_backend_server_version() ==
                     MYSQL_VERSION_8) {
            if (st.create_user_default_auth_plugin &&
                strcasecmp(st.create_user_default_auth_plugin,
                           "mysql_native_password") &&
                strcasecmp(st.create_user_default_auth_plugin,
                           "caching_sha2_password")) {
              LOG_INFO(
                  "backend dataserver is based on 8.0, create user statement "
                  "should specify authentication_plugin as "
                  "'mysql_native_password' or 'caching_sha2_password' "
                  "explicitly.");
              throw Error(
                  "backend dataserver is based on 8.0, create user statement "
                  "should specify authentication_plugin as "
                  "'mysql_native_password' or 'caching_sha2_password' "
                  "explicitly.");
            }
          }
        }
      }
      // make sure that alter user is not changing auth plugin when using 8.0
      // backend cause dbscale only support mysql_native_password auth plugin
      if (st.type == STMT_ALTER_USER && st.sql->create_user_oper != NULL) {
        if (Backend::instance()->get_backend_server_version() ==
            MYSQL_VERSION_57) {
          if (st.create_user_default_auth_plugin &&
              strcasecmp(st.create_user_default_auth_plugin,
                         "mysql_native_password")) {
            LOG_INFO(
                "backend dataserver is based on 5.7 alter user should not "
                "change it's auth plugin to any plugin except "
                "'mysql_native_password'");
            throw Error(
                "backend dataserver is based on 5.7 alter user should not "
                "change it's auth plugin to any plugin except "
                "'mysql_native_password'");
          }
        } else if (Backend::instance()->get_backend_server_version() ==
                   MYSQL_VERSION_8) {
          if (st.create_user_default_auth_plugin &&
              strcasecmp(st.create_user_default_auth_plugin,
                         "mysql_native_password") &&
              strcasecmp(st.create_user_default_auth_plugin,
                         "caching_sha2_password")) {
            LOG_INFO(
                "backend dataserver is based on 8.0 alter user should not "
                "change it's auth plugin to any plugin except "
                "'mysql_native_password' or 'caching_sha2_password'");
            throw Error(
                "backend dataserver is based on 8.0 alter user should not "
                "change it's auth plugin to any plugin except "
                "'mysql_native_password' or 'caching_sha2_password'");
          }
        }
      }
      bool is_return_ok = false;
      if (st.type == STMT_GRANT || st.type == STMT_REVOKE) {
        grant_priv_type_list *priv_type = NULL;
        if (st.type == STMT_GRANT)
          priv_type = st.sql->grant_oper->grant_priv_type;
        else {
          priv_type = st.sql->revoke_oper->grant_priv_type;
        }

        ACLType acl_type = ACL_TYPE_EMPTY;
        int acl_count = 0;
        while (priv_type) {
          acl_type |= priv_type->type;
          ++acl_count;
          priv_type = priv_type->next;
        }
        if ((acl_type & ACL_TYPE_ADD_PARTITION ||
             acl_type & ACL_TYPE_DROP_PARTITION)) {
          check_dbscale_management_acl(plan);
          if (acl_count != 1) {
            string err_msg = "ADD PARTITION/DROP PARTITION need use alone";
            LOG_ERROR("%s\n", err_msg.c_str());
            throw NotSupportedError(err_msg.c_str());
          }
          add_drop_partition_acl_check(plan, acl_type);
          is_return_ok = true;
        }
      }

#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG(
            "drop_user/create_user/revoke/grant cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
        set_need_handle_acl_related_stmt(false);
        break;
      }
#endif
      replace_grant_hosts();
      if (is_return_ok) {
        ExecuteNode *node = plan->get_return_ok_node();
        plan->set_start_node(node);
        break;
      }
      dataspace = backend->get_auth_data_space();
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_DROP_EVENT:
    case STMT_CREATE_EVENT: {
      const char *schema_name = schema;

      if (st.sql->event_oper) {
        schema_name = st.sql->event_oper->schema_name
                          ? st.sql->event_oper->schema_name
                          : schema;
      } else {
        throw NotSupportedError(
            "Unsupport event, maybe parse fail, plz check log.");
      }
      dataspace = backend->get_data_space_for_table(schema_name, NULL);
      if (dataspace) {
        assemble_direct_exec_plan(plan, dataspace);
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);
#endif
        LOG_ERROR("Can not get dataspace for schema %s\n.", schema_name);
        throw Error("Fail to get dataspace.");
      }
    } break;
    case STMT_DBSCALE_KEEPMASTER:
      assemble_keepmaster_plan(plan);
      break;
    case STMT_DBSCALE_TEST:
      assemble_dbscale_test_plan(plan);
      break;
    case STMT_DROP_VIEW: {
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        bool is_master = mul->get_is_cluster_master();

        if (!is_master) {
          LOG_DEBUG("DROP VIEW statment be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      if (on_view) {
        dataspace = backend->get_config_data_space();
        if (!dataspace) {
          LOG_ERROR("Can not find the metadata dataspace.\n");
          throw Error(
              "Fail to get meta dataspace, please make sure meta datasource is "
              "configured.");
        }
        assemble_direct_exec_plan(plan, dataspace);
      } else {
        assemble_drop_mul_table(plan, true);
      }
    } break;
    case STMT_CREATE_VIEW: {
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        bool is_master = mul->get_is_cluster_master();

        if (!is_master) {
          LOG_DEBUG("CREATE VIEW statment be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      if (is_partial_parsed()) {
        LOG_ERROR("partial parsed create view sql\n");
        throw Error("fail to parse create view sql");
      }
      if (on_view) {
        dataspace = backend->get_config_data_space();
        if (!dataspace) {
          LOG_ERROR("Can not find the metadata dataspace.\n");
          throw Error(
              "Fail to get meta dataspace, please make sure meta datasource is "
              "configured.");
        }
      } else {
        /*If dbscale no need to maintain view, the view will be created on the
         * schema dataspace.*/
#ifdef DEBUG
        ACE_ASSERT(st.sql->select_oper);
#endif
        create_view_op_node *view_node = st.sql->select_oper->create_view_oper;
        const char *view_schema =
            view_node->schema ? view_node->schema : schema;

        dataspace = backend->get_data_space_for_table(view_schema, NULL);
      }
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_CREATE_PROCEDURE:
      if (is_partial_parsed() || st.has_unsupport_routine_element) {
        dataspace = backend->get_data_space_for_table(schema, NULL);
        assemble_direct_exec_plan(plan, dataspace);
        break;
      } else {
        const char *routine_schema = schema;
#ifndef CLOSE_PROCEDURE
        routine_node *routine_d = st.routine_d;
        store_procedure_node *proc_node = routine_d->proc_node;
        routine_schema =
            proc_node->schema_name ? proc_node->schema_name : schema;
        if (!routine_schema)
          throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_NO_SCHEMA_CODE],
                                       "42000", ERROR_NO_SCHEMA_CODE);
        dataspace = backend->get_data_space_for_table(routine_schema, NULL);
        const char *proc_name = proc_node->proc_name;

        if (!enable_oracle_sequence &&
            ((dataspace->get_dataspace_type() == SCHEMA_TYPE &&
              ((Schema *)dataspace)
                  ->get_is_schema_pushdown_stored_procedure()) ||
             dataspace->get_dataspace_type() == CATALOG_TYPE)) {
#ifdef DEBUG
          LOG_DEBUG("schema pushdown stored procedure directly\n");
#endif
          assemble_direct_exec_plan(plan, dataspace);
          break;
        }

        string sp_name(routine_schema);
        sp_name.append(".");
        sp_name.append(proc_name);
        current_sp_worker_name = sp_name;
        SPWorker *run_spworker = plan->session->get_sp_worker(sp_name);
        if (!run_spworker) {
          ProcedureMetaData *meta_data =
              backend->get_procedure_meta_data(sp_name);
          if (!meta_data) {
            SPWorker *spworker = new SPWorker(sp_name, plan->session);
            init_sp_worker_from_create_sql(spworker, plan, st.handled_sql,
                                           routine_schema, schema, sp_name);
            if (spworker->get_execution_flag() == EXECUTE_ON_ONE_DATASPACE) {
              DataSpace *data_space = spworker->get_data_space();
              try {
                data_space->execute_one_modify_sql(
                    st.handled_sql, plan->handler, routine_schema);
              } catch (...) {
                spworker->clean_sp_worker_for_free();
                delete spworker;
                spworker = NULL;
              }
            }
            if (spworker) {
              spworker->clean_sp_worker_for_free();
              delete spworker;
              spworker = NULL;
            }
          }
        }
#endif

        assemble_direct_exec_plan(plan, dataspace);
      }
      break;
    case STMT_DROP_PROC: {
      if (multiple_mode &&
          !MultipleManager::instance()->get_is_cluster_master()) {
        assemble_forward_master_role_plan(plan);
        break;
      }
      if (is_partial_parsed() || st.has_unsupport_routine_element) {
        dataspace = backend->get_data_space_for_table(schema, NULL);
        assemble_direct_exec_plan(plan, dataspace);
      } else {
        routine_node *routine_d = st.routine_d;
        store_procedure_node *proc_node = routine_d->proc_node;
        const char *routine_schema =
            proc_node->schema_name ? proc_node->schema_name : schema;
        if (!routine_schema)
          throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_NO_SCHEMA_CODE],
                                       "42000", ERROR_NO_SCHEMA_CODE);
        const char *proc_name = proc_node->proc_name;
        dataspace = backend->get_data_space_for_table(routine_schema, NULL);

        if (!enable_oracle_sequence &&
            ((dataspace->get_dataspace_type() == SCHEMA_TYPE &&
              ((Schema *)dataspace)
                  ->get_is_schema_pushdown_stored_procedure()) ||
             dataspace->get_dataspace_type() == CATALOG_TYPE)) {
#ifdef DEBUG
          LOG_DEBUG("schema pushdown stored procedure directly\n");
#endif
          assemble_direct_exec_plan(plan, dataspace);
          break;
        }

        string sp_name(routine_schema);
        sp_name.append(".");
        sp_name.append(proc_name);
        string sp_name_with_enclose("`");
        sp_name_with_enclose.append(routine_schema);
        sp_name_with_enclose.append("`.`");
        sp_name_with_enclose.append(proc_name);
        sp_name_with_enclose.append("`");

        ProcedureMetaData *meta_data =
            backend->get_procedure_meta_data(sp_name);
        if (meta_data) {
          if (meta_data->execution_flag == EXECUTE_ON_ONE_DATASPACE) {
            DataSpace *data_space_another = meta_data->data_space;
            try {
              data_space_another->execute_one_modify_sql(
                  st.handled_sql, plan->handler, routine_schema);
            } catch (...) {
              Backend::instance()->record_latest_important_dbscale_warning(
                  "Got error when execute Drop procedure, DataSpace: [%p], "
                  "sql: [%s]\n",
                  data_space_another, st.handled_sql);
            }
          }
        } else {
          vector<string> sp_vec;
          const char *create_sp_sql;
          try {
            fetch_create_procedure_sql(sp_name_with_enclose.c_str(), dataspace,
                                       &sp_vec);
          } catch (SQLError &e) {
            if (e.get_error_code() != ERROR_PROCEDURE_NOT_EXIST_CODE ||
                !st.if_exists)
              throw;
            Backend::instance()->record_latest_important_dbscale_warning(
                "Procedure [%s] does not exist.\n", sp_name.c_str());
          }
          SPWorker *spworker = new SPWorker(sp_name, plan->session);
          if (!sp_vec.empty()) {
            create_sp_sql = sp_vec[0].c_str();
            init_sp_worker_from_create_sql(spworker, plan, create_sp_sql,
                                           routine_schema, schema, sp_name);
            if (spworker->get_execution_flag() == EXECUTE_ON_ONE_DATASPACE) {
              DataSpace *data_space_another = spworker->get_data_space();
              try {
                data_space_another->execute_one_modify_sql(
                    st.handled_sql, plan->handler, routine_schema);
              } catch (...) {
                Backend::instance()->record_latest_important_dbscale_warning(
                    "Got error when execute Drop procedure, DataSpace: [%p], "
                    "sql: [%s]\n",
                    data_space_another, st.handled_sql);
              }
            }
          }
          spworker->clean_sp_worker_for_free();
          delete spworker;
        }
        plan->session->remove_sp_worker(sp_name);
        backend->remove_procedure_meta_data(sp_name);
        assemble_direct_exec_plan(plan, dataspace);
      }
    } break;
    case STMT_CALL: {
#ifndef DBSCALE_TEST_DISABLE
      dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "sp_execution") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "simple_sp_test_init")) {
        LOG_DEBUG(
            "Do dbscale test operation 'simple_sp_test_init' for case "
            "'sp_execution'.\n");
        SPWorker *spworker =
            new SPWorker(string("test.test_sp"), plan->session);
        plan->session->add_sp_worker(spworker);
        plan->session->set_cur_sp_worker(spworker);
        spworker->add_param("param_in", SP_PARAM_IN);
        spworker->add_param("param2", SP_PARAM_IN);
        /*
        spworker->add_param("param_inout", SP_PARAM_INOUT);
        */
        spworker->add_param("param_out", SP_PARAM_OUT);
        spworker->add_variable("var1");
        spworker->add_variable("var2");

        /* test compound statment and simple statment */
        CompondStatement *comp_s = new CompondStatement(spworker, "");
        SimpleExecStatement *se_s1 = new SimpleExecStatement(
            spworker, "set @var1=1, @var2=@param_in + 2", true, this);
        SimpleExecStatement *se_s2 = new SimpleExecStatement(
            spworker, "delete from test.t2", false, this);
        SimpleExecStatement *se_s3 = new SimpleExecStatement(
            spworker, "insert into t2 values (@var1, @var2)", true, this);
        SimpleExecStatement *se_s4 =
            new SimpleExecStatement(spworker, "select * from t2", false, this);

        /* test out parameter */
        SimpleExecStatement *se_s5 =
            new SimpleExecStatement(spworker, "set @param_out = 1", true, this);
        SimpleExecStatement *se_s10 =
            new SimpleExecStatement(spworker, "select @param2", true, this);

        /* test if */
        IntExpression *int_expr = new ((get_stmt_node())) IntExpression("1");
        VarExpression *var_expr =
            new ((get_stmt_node())) VarExpression("var1", VAR_SCOPE_USER);
        CompareExpression *com_expr = new ((get_stmt_node()))
            CompareExpression(var_expr, int_expr, EXPR_EQ, EXPR_SCOPE_NON);

        ExprConditionHandler *expr_cond = new ExprConditionHandler(com_expr);
        expr_cond->add_expr_var_map(var_expr, "var1");

        IFItemStatement *if_item = new IFItemStatement(spworker, expr_cond);
        SimpleExecStatement *se_s6 = new SimpleExecStatement(
            spworker, "insert into t2 values (6,6)", false, this);
        if_item->set_cond_stmt(se_s6);

        IFElseStatement *if_else = new IFElseStatement(spworker);

        SimpleExecStatement *se_s7 = new SimpleExecStatement(
            spworker, "insert into t2 values (8,8)", false, this);

        if_else->add_if_item(if_item);
        if_else->set_else_stmt(se_s7);

        /* test while */
        IntExpression *int_expr2 = new ((get_stmt_node())) IntExpression("5");
        VarExpression *var_expr2 =
            new ((get_stmt_node())) VarExpression("var1", VAR_SCOPE_USER);
        CompareExpression *com_expr2 = new ((get_stmt_node()))
            CompareExpression(var_expr2, int_expr2, EXPR_LESS, EXPR_SCOPE_NON);

        ExprConditionHandler *expr_cond2 = new ExprConditionHandler(com_expr2);
        expr_cond2->add_expr_var_map(var_expr2, "var1");

        SimpleExecStatement *se_s8 = new SimpleExecStatement(
            spworker, "insert into t2 values (@var1+20, @var1+20)", true, this);
        SimpleExecStatement *se_s9 =
            new SimpleExecStatement(spworker, "set @var1=@var1+1", true, this);

        CompondStatement *comp_s2 = new CompondStatement(spworker, "if_while");
        comp_s2->add_child_stmt(se_s8);
        comp_s2->add_child_stmt(se_s9);

        WhileStatement *while_s = new WhileStatement(spworker, expr_cond2, true,
                                                     "");  // while as while

        while_s->add_child_stmt(comp_s2);

        comp_s->add_child_stmt(se_s1);
        comp_s->add_child_stmt(se_s2);
        comp_s->add_child_stmt(se_s3);
        comp_s->add_child_stmt(se_s4);
        comp_s->add_child_stmt(if_else);
        comp_s->add_child_stmt(while_s);
        comp_s->add_child_stmt(se_s5);
        comp_s->add_child_stmt(se_s10);
        spworker->set_sp_statement(comp_s);

        string sp_name;
        sp_name.append("test.test_sp");
        SPWorker *run_spworker = plan->session->get_sp_worker(sp_name);

        string var1("param_in");
        boost::to_upper(var1);
        string var2("param_out");
        boost::to_upper(var2);
        string var3("param2");
        boost::to_upper(var3);

        run_spworker->add_param_value_for_execution(var1, "3", EXPR_INT);
        run_spworker->add_param_value_for_execution(var3, "'abc'", EXPR_STRING);
        run_spworker->add_param_value_for_execution(var2, "out", EXPR_VAR);
        run_spworker->init_variable_for_execution();
        plan->session->set_call_store_procedure(true);
        run_spworker->exec_worker();
        run_spworker->clean_sp_worker_after_execution();
        plan->session->set_cur_sp_worker(NULL);

        ExecuteNode *node = plan->get_call_sp_return_ok_node();
        plan->set_start_node(node);
        break;
      }
#endif
      plan->session->set_is_complex_stmt(true);
      const char *schema_name =
          st.sql->call_oper->db_name ? st.sql->call_oper->db_name : schema;
      dataspace = backend->get_data_space_for_table(schema_name, NULL);

      const char *procedure_name = st.sql->call_oper->procedure_name;
      string sp_name(schema_name);
      sp_name.append(".");
      sp_name.append(procedure_name);

      if (!enable_oracle_sequence &&
          ((dataspace->get_dataspace_type() == SCHEMA_TYPE &&
            ((Schema *)dataspace)->get_is_schema_pushdown_stored_procedure()) ||
           dataspace->get_dataspace_type() == CATALOG_TYPE)) {
#ifdef DEBUG
        LOG_DEBUG("schema pushdown stored procedure directly for sp %s\n",
                  sp_name.c_str());
#endif
        plan->session->set_read_only(false);
        if (backend->get_read_only_procedures()) {
          if (backend->get_read_only_procedures()->count(sp_name)) {
            plan->session->set_read_only(true);
            LOG_DEBUG(
                "Procedure %s is configured as read-only-procedure, set "
                "statement read-only as true\n",
                sp_name.c_str());
          }
        }
        assemble_direct_exec_plan(plan, dataspace);
        break;
      }
#ifdef DEBUG
      bool is_schema_type = dataspace->get_dataspace_type() == SCHEMA_TYPE;
      LOG_DEBUG(
          "schema name is %s with type is schematype %d and pushdown procedure "
          "%d and enable-oracle-sequence %d.\n",
          schema_name, is_schema_type ? 1 : 0,
          is_schema_type ? (((Schema *)dataspace)
                                    ->get_is_schema_pushdown_stored_procedure()
                                ? 1
                                : 0)
                         : 0,
          enable_oracle_sequence);
#endif

      string sp_name_with_enclose("`");
      sp_name_with_enclose.append(schema_name);
      sp_name_with_enclose.append("`.`");
      sp_name_with_enclose.append(procedure_name);
      sp_name_with_enclose.append("`");

      string fake_sp_name = plan->session->get_next_nest_sp_name(sp_name);
      SPWorker *run_spworker = plan->session->get_sp_worker(fake_sp_name);

#ifndef CLOSE_PROCEDURE
      bool is_centralized_cluster = backend->is_centralized_cluster();
#ifdef DEBUG
      LOG_DEBUG(
          "is_centralized_cluster=%d with "
          "spworker %d\n",
          is_centralized_cluster, run_spworker ? 1 : 0);
#endif
      if (!is_centralized_cluster) {
        // Get spworker from session
        ProcedureMetaData *meta_data =
            backend->get_procedure_meta_data(sp_name);
        SPExecuteFlag execution_flag = EXECUTE_ON_DBSCALE;
        if (!run_spworker) {
          const char *create_sp_sql = NULL;
          vector<string> sp_vec;
          if (meta_data) {
            create_sp_sql = meta_data->procedure_string.c_str();
            execution_flag = meta_data->execution_flag;
          } else {
            fetch_create_procedure_sql(sp_name_with_enclose.c_str(), dataspace,
                                       &sp_vec);

            if (!sp_vec.empty()) {
              create_sp_sql = sp_vec[0].c_str();
            }
          }

          if (create_sp_sql && execution_flag == EXECUTE_ON_DBSCALE) {
            LOG_DEBUG("Get create procedure sql:[%s]\n", create_sp_sql);
            SPWorker *spworker = new SPWorker(fake_sp_name, plan->session);
            int extra_snippet_val =
                plan->session->get_extra_snippet_of_sp_var();
            spworker->set_extra_snippet_val(extra_snippet_val);
            init_sp_worker_from_create_sql(spworker, plan, create_sp_sql,
                                           schema_name, schema, sp_name);
            plan->session->add_sp_worker(spworker);
            if (!meta_data) {
              meta_data = new ProcedureMetaData(create_sp_sql,
                                                spworker->get_execution_flag(),
                                                spworker->get_data_space());
              if (!backend->add_procedure_meta_data(spworker->get_name(),
                                                    sp_name, meta_data)) {
                delete meta_data;
                meta_data = NULL;
              }
            }
            run_spworker = plan->session->get_sp_worker(fake_sp_name);
          }
        }

        if (run_spworker &&
            run_spworker->get_execution_flag() == EXECUTE_ON_DBSCALE) {
          run_spworker->set_is_for_call_store_procedure_directly(false);
#ifdef DEBUG
          LOG_DEBUG("SP %s can not be exec on one server.\n", sp_name.c_str());
#endif
          plan->session->get_status()->item_inc(
              TIMES_CALL_STMT_EXECUTE_ON_DBSCALE_SIDE);
          // try to add param value to spworker
          ListExpression *param_list =
              (ListExpression *)(st.sql->call_oper->param_list);
          list<const char *> values_list;
          list<expr_type> type_list;
          if (param_list) {
            expr_list_item *head = param_list->expr_list_head;
            expr_list_item *tmp = head;
            do {
              const char *expr_str = NULL;
              if (tmp->expr->type != EXPR_VAR)
                expr_str = expr_is_simple_value(tmp->expr);
              else if (tmp->expr->type == EXPR_VAR) {
                VarExpression *var_tmp = (VarExpression *)tmp->expr;
                if (var_tmp->var_scope == VAR_SCOPE_USER) {
                  expr_str = var_tmp->str_value;
                }
              }
              if (expr_str) {
                values_list.push_back(expr_str);
                type_list.push_back(tmp->expr->type);
              } else {
                throw NotSupportedError(
                    "Unsupport param value for store procedure execution.");
              }
              tmp = tmp->next;
            } while (tmp != head);
          }
          if (values_list.size() != run_spworker->get_param_list()->size()) {
            // TODO: thow sql error
            throw Error("Incorrect number of arguments for PROCEDURE.");
          }

          list<string> *sp_param_list = run_spworker->get_param_list();
          list<string>::iterator it = sp_param_list->begin();
          list<const char *>::iterator it2 = values_list.begin();
          list<expr_type>::iterator it3 = type_list.begin();
          for (; it != sp_param_list->end();) {
            run_spworker->add_param_value_for_execution(*it, *it2, *it3);
            ++it;
            ++it2;
            ++it3;
          }
          run_spworker->init_variable_for_execution();

          plan->session->set_call_store_procedure(true);
          try {
            plan->session->set_cur_sp_worker(run_spworker);
            plan->session->increase_call_sp_nest();
            plan->session->set_schema(schema_name);
            run_spworker->exec_worker();
            run_spworker->clean_sp_worker_after_execution();
            plan->session->set_schema(schema);
          } catch (...) {
            plan->session->decrease_call_sp_nest();
            run_spworker->clean_sp_worker_after_execution();
            plan->session->pop_cur_sp_worker_one_recurse();
            plan->session->set_schema(schema);
            throw;
          }

          ExecuteNode *node = plan->get_call_sp_return_ok_node();
          plan->set_start_node(node);

          break;
        }
        if (meta_data &&
            meta_data->execution_flag == EXECUTE_ON_ONE_DATASPACE) {
          dataspace = meta_data->data_space;
#ifdef DEBUG
          LOG_DEBUG("SP %s can be exec on one server.\n", sp_name.c_str());
#endif
        }
      }
#endif
      plan->session->set_read_only(false);
      if (backend->get_read_only_procedures()) {
        if (backend->get_read_only_procedures()->count(sp_name)) {
          plan->session->set_read_only(true);
          LOG_DEBUG(
              "Procedure %s is configured as read-only-procedure, set "
              "statement read-only as true\n",
              sp_name.c_str());
        }
      }
      set_call_store_procedure_directly(true);
      plan->session->increase_call_sp_nest();
      if (!run_spworker) {
        run_spworker = new SPWorker(fake_sp_name, plan->session);
        plan->session->add_sp_worker(run_spworker);
      }
      run_spworker->set_is_for_call_store_procedure_directly(true);
      plan->session->set_cur_sp_worker(run_spworker);
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_SHOW_WARNINGS: {
      assemble_show_warnings_plan(plan);
      break;
    }
    case STMT_DBSCALE_MESSAGE_SERVICE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_message_service_plan(plan);
#else
      throw NotSupportedError("Not support dbscale service command");
#endif
      break;
    }
    case STMT_DBSCALE_CREATE_BINLOG_TASK: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_create_binlog_task_plan(plan);
#else
      throw NotSupportedError("Not support dbscale service command");
#endif
      break;
    }
    case STMT_DBSCALE_TASK: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_task_plan(plan);
#else
      throw NotSupportedError("Not support dbscale service command");
#endif
      break;
    }
    case STMT_DBSCALE_BINLOG_TASK_ADD_FILTER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_binlog_task_add_filter_plan(plan);
#else
      throw NotSupportedError("Now support dbscale service command");
#endif
      break;
    }
    case STMT_DBSCALE_TASK_DROP_FILTER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_drop_task_filter_plan(plan);
#else
      throw NotSupportedError("Now support dbscale service command");
#endif
      break;
    }
    case STMT_DBSCALE_DROP_TASK: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_ZEROMQ
      assemble_dbscale_drop_task_plan(plan);
#else
      throw NotSupportedError("Not support dbscale drop task command");
#endif
      break;
    }
    case STMT_DBSCALE_SHOW_CLIENT_TASK_STATUS: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
#ifndef CLOSE_ZEROMQ
      client_task_status_op_node *node = st.sql->client_task_status_oper;
      assemble_dbscale_show_client_task_status_plan(plan, node->task_name,
                                                    node->server_task_name);
#else
      throw NotSupportedError(
          "Not support dbscale show client task status command");
#endif
      break;
    }
    case STMT_DBSCALE_SHOW_SERVER_TASK_STATUS: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
#ifndef CLOSE_ZEROMQ
      server_task_status_op_node *node = st.sql->server_task_status_oper;
      assemble_dbscale_show_server_task_status_plan(plan, node->task_name);
#else
      throw NotSupportedError(
          "Not support dbscale show server task status command");
#endif
      break;
    }
    case STMT_DBSCALE_RESTART_SPARK_AGENT: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_restart_spark_agent_node(plan);
      break;
    }
    case STMT_DBSCALE_SHOW_CREATE_ORACLE_SEQUENCE: {
      assemble_dbscale_show_create_oracle_seq_plan(plan);
      break;
    }
    case STMT_DBSCALE_SHOW_SEQUENCE_STATUS: {
      assemble_dbscale_show_seq_status_plan(plan);
      break;
    }
    case STMT_DBSCALE_SHOW_TRANSACTION_BLOCK_INFO: {
      dbscale_show_trx_block_info_node *node =
          st.sql->dbscale_show_trx_block_info_oper;
      assemble_dbscale_show_trx_block_info_plan(plan, (bool)node->is_local);
      break;
    }
    case STMT_DBSCALE_INTERNAL_SET: {
      assemble_dbscale_internal_set_plan(plan);
    } break;
    case STMT_DBSCALE_RESTORE_TABLE: {
      join_node *join = st.table_list_head->join;
      const char *schema_name = join->schema_name ? join->schema_name : schema;
      const char *table_name = join->table_name;
      assemble_dbscale_restore_table_plan(plan, schema_name, table_name);
      break;
    }
    case STMT_DBSCALE_CLEAN_RECYCLE_TABLE: {
      join_node *join = st.table_list_head->join;
      const char *schema_name = join->schema_name ? join->schema_name : schema;
      const char *table_name = join->table_name;
      assemble_dbscale_clean_recycle_table_plan(plan, schema_name, table_name);
      break;
    }
    case STMT_DBSCALE_SHOW_PREPARE_CACHE_STATUS: {
      assemble_dbscale_show_prepare_cache_status_plan(plan);
      break;
    }
    case STMT_DBSCALE_FLUSH_PREPARE_CACHE_INFO: {
      assemble_dbscale_flush_prepare_cache_hit_plan(plan);
      break;
    }
    case STMT_DBSCALE_BACKUP_START:
    case STMT_DBSCALE_BACKUP_END:
    case STMT_DBSCALE_RECOVERY_START:
    case STMT_DBSCALE_RECOVERY_END: {
      ExecuteNode *node = plan->get_return_ok_node();
      plan->set_start_node(node);
      break;
    }
    case STMT_DBSCALE_SHOW_FETCHNODE_BUFFER_USAGE: {
      assemble_dbscale_show_fetchnode_buffer_usage_plan(plan);
      break;
    }
    default:
      if (check_is_select_version_comment()) {
        /*The login operation will execute the stmt "select @@version_comment
         * limit 1", which should be directed to auth, in case the conn pool of
         * catalog is full.*/
        dataspace = backend->get_auth_data_space();
      } else {
        dataspace = backend->get_data_space_for_table(schema, NULL);
      }
      assemble_direct_exec_plan(plan, dataspace);
      return;
  }
}

void Statement::add_drop_partition_acl_check(ExecutePlan *plan,
                                             ACLType acl_type) {
  if (st.type == STMT_REVOKE) {
    map<string, map<string, pair<ssl_option_struct, string> > >
        tmp_user_info_map;
    Backend::instance()->get_white_user_info(tmp_user_info_map);
    user_clause_item_list *user_clause = st.sql->revoke_oper->user_clause;
    while (user_clause) {
      const char *user_id = user_clause->user_name;
      if (!tmp_user_info_map.count(user_id)) {
        string err_msg = "Can't find any matching row in the user table";
        LOG_ERROR("%s user name is [%s]\n", err_msg.c_str(), user_id);
        throw NotSupportedError(err_msg.c_str());
      }
      user_clause = user_clause->next;
    }
  } else if (st.type == STMT_GRANT) {
    string schema_name = "", full_table_name = "", user_name = "";
    map<string, map<string, TableACLType, strcasecomp> > table_acl_map;
    map<string, map<string, SchemaACLType, strcasecomp> > schema_acl_map;
    Backend::instance()->get_table_acl_info_all(&table_acl_map);
    Backend::instance()->get_schema_acl_info_all(&schema_acl_map);
    TableACLType tb_acl_type = ACL_TYPE_EMPTY;
    SchemaACLType db_acl_type = ACL_TYPE_EMPTY;
    grant_node *oper = st.sql->grant_oper;
    if (oper->user_clause->password != nullptr) {
      string schema_acl_str;
      get_acl_str(schema_acl_str, acl_type);
      string err_msg = "not allow create new user execute grant sql ";
      err_msg.append(schema_acl_str);
      LOG_ERROR("%s\n", err_msg.c_str());
      throw NotSupportedError(err_msg.c_str());
    }
    switch (oper->priv_level) {
      case GRANT_PRIV_LEVEL_STAR:
      case GRANT_PRIV_LEVEL_STAR_STAR:
      case GRANT_PRIV_LEVEL_A_STAR: {
        if (oper->priv_level == GRANT_PRIV_LEVEL_STAR) {
          schema_name = string(plan->session->get_schema());
        } else if (oper->priv_level == GRANT_PRIV_LEVEL_STAR_STAR) {
          schema_name = string(DBSCALE_RESERVED_STR);
        } else if (oper->priv_level == GRANT_PRIV_LEVEL_A_STAR) {
          schema_name = string(oper->priv_level_db_name);
          boost::replace_all(schema_name, "\\_", "_");
          boost::replace_all(schema_name, "\\%", "%");
        }
        user_name = oper->user_clause->user_name;
        break;
      }
      case GRANT_PRIV_LEVEL_A:
      case GRANT_PRIV_LEVEL_A_B: {
        if (oper->priv_level == GRANT_PRIV_LEVEL_A) {
          schema_name = string(plan->session->get_schema());
        } else {
          schema_name = string(oper->priv_level_db_name);
          boost::replace_all(schema_name, "\\_", "_");
          boost::replace_all(schema_name, "\\%", "%");
        }
        const char *table_name = oper->priv_level_tb_name;
        splice_full_table_name(schema_name.c_str(), table_name,
                               full_table_name);
        user_name = oper->user_clause->user_name;
        break;
      }
    }

    if (!(table_acl_map.count(user_name) || schema_acl_map.count(user_name))) {
      string err_msg = "Can't find any matching row in the user table";
      LOG_ERROR("%s user name is [%s]\n", err_msg.c_str(), user_name.c_str());
      throw NotSupportedError(err_msg.c_str());
    }
    if (table_acl_map.count(user_name)) {
      auto it = table_acl_map[user_name].find(full_table_name);
      if (it != table_acl_map[user_name].end()) tb_acl_type = it->second;
    }
    if (schema_acl_map.count(user_name)) {
      auto it = schema_acl_map[user_name].find(schema_name);
      if (it != schema_acl_map[user_name].end()) db_acl_type = it->second;
    }

    if (acl_type & ACL_TYPE_ADD_PARTITION ||
        acl_type & ACL_TYPE_DROP_PARTITION) {
      if (!(tb_acl_type & ACL_TYPE_ALTER || db_acl_type & ACL_TYPE_ALTER)) {
        string err_msg =
            "grant add partition/drop partition need authority ALTER ";
        LOG_ERROR("%s\n", err_msg.c_str());
        throw NotSupportedError(err_msg.c_str());
      }
    }
  }
}

bool Statement::check_is_select_version_comment() {
  if (st.type == STMT_SELECT && st.session_var_list && st.scanner->limit) {
    if (!st.session_var_list->next) {
      string var_name = st.session_var_list->value;
      boost::to_upper(var_name);
      if (!strcmp(var_name.c_str(), "VERSION_COMMENT")) {
        if (!strcasecmp(sql, "select @@version_comment limit 1")) return true;
      }
    }
  }
  return false;
}

#ifndef CLOSE_ZEROMQ
void Statement::assemble_dbscale_task_plan(ExecutePlan *plan) {
  ExecuteNode *task_node = plan->get_task_node();
  plan->set_start_node(task_node);
}

void Statement::assemble_dbscale_create_binlog_task_plan(ExecutePlan *plan) {
  ExecuteNode *binlog_task_node = plan->get_binlog_task_node();
  plan->set_start_node(binlog_task_node);
}

void Statement::assemble_dbscale_message_service_plan(ExecutePlan *plan) {
  ExecuteNode *message_service_node = plan->get_message_service_node();
  plan->set_start_node(message_service_node);
}

void Statement::assemble_dbscale_binlog_task_add_filter_plan(
    ExecutePlan *plan) {
  ExecuteNode *binlog_task_add_filter_node =
      plan->get_binlog_task_add_filter_node();
  plan->set_start_node(binlog_task_add_filter_node);
}

void Statement::assemble_dbscale_drop_task_filter_plan(ExecutePlan *plan) {
  ExecuteNode *drop_task_filter_node = plan->get_drop_task_filter_node();
  plan->set_start_node(drop_task_filter_node);
}

void Statement::assemble_dbscale_drop_task_plan(ExecutePlan *plan) {
  ExecuteNode *drop_task_node = plan->get_drop_task_node();
  plan->set_start_node(drop_task_node);
}

void Statement::assemble_dbscale_show_client_task_status_plan(
    ExecutePlan *plan, const char *task_name, const char *server_task_name) {
  ExecuteNode *show_client_task_status_node =
      plan->get_show_client_task_status_node(task_name, server_task_name);
  plan->set_start_node(show_client_task_status_node);
}

void Statement::assemble_dbscale_show_server_task_status_plan(
    ExecutePlan *plan, const char *task_name) {
  ExecuteNode *show_server_task_status_node =
      plan->get_show_server_task_status_node(task_name);
  plan->set_start_node(show_server_task_status_node);
}
#endif

void Statement::assemble_dbscale_restart_spark_agent_node(ExecutePlan *plan) {
#ifndef DBSCALE_DISABLE_SPARK
  plan->session->start_executing_spark();
  SparkReturnValue return_value = restart_spark_service();
  plan->session->stop_executing_spark();

  if (!return_value.success) {
    throw Error(return_value.error_string.c_str());
  }
#endif
  ExecuteNode *node = plan->get_return_ok_node();
  plan->set_start_node(node);
}

void Statement::assemble_keepmaster_plan(ExecutePlan *plan) {
  ExecuteNode *keepmaster_node = plan->get_keepmaster_node();
  plan->set_start_node(keepmaster_node);
}

void Statement::assemble_dbscale_test_plan(ExecutePlan *plan) {
  ExecuteNode *dbscale_test_node = plan->get_dbscale_test_node();
  plan->set_start_node(dbscale_test_node);
}

void Statement::assemble_async_task_control_plan(ExecutePlan *plan,
                                                 unsigned long long id) {
  ExecuteNode *async_task_node = plan->get_async_task_control_node(id);
  plan->set_start_node(async_task_node);
}

void Statement::assemble_load_dataspace_config_file_plan(ExecutePlan *plan,
                                                         const char *filename) {
  ExecuteNode *node = plan->get_load_dataspace_file_node(filename);
  plan->set_start_node(node);
}

void Statement::assemble_create_db_plan(ExecutePlan *plan) {
  LOG_DEBUG("assemble_create_db_plan\n");
  Backend *backend = Backend::instance();
  const char *db_name = NULL;
  if (st.type == STMT_DROP_DB || st.type == STMT_ALTER_DB) {
    if (st.type == STMT_DROP_DB) {
      db_name = st.sql->drop_db_oper->dbname->name;
    } else {
      db_name = st.sql->alter_db_oper->db_name;
    }
    if (dbscale_safe_sql_mode > 0 &&
        Backend::instance()->is_system_schema(db_name)) {
      throw Error(
          "Refuse drop/alter system schema for dbscale_safe_sql_mode>0");
    }
    if (!strcasecmp(db_name, default_login_schema)) {
      LOG_ERROR("default_login_schema can not be dropped/altered.\n");
      throw Error("default_login_schema can not be dropped/altered");
    }
  } else if (st.type == STMT_CREATE_DB) {
    db_name = st.sql->create_db_oper->dbname->name;
    DataSource *schema_source = NULL;
    if (!backend->find_schema(db_name)) {
      if (plan->session->get_session_option("auto_space_level").uint_val ==
          AUTO_SPACE_SCHEMA) {
        schema_source = backend->get_one_source_from_last_scheme();
      } else {
        schema_source = backend->get_catalog()->get_data_source();
      }
      /*Here will first assign the schema dataspace, even if the create
       * dataspace operation fail in the mysql_plan.cc, the assigned
       * dataspace will not be rollback.*/
      if (schema_source) {
        try {
          LOG_INFO("Assign source %s to schema space %s.\n",
                   schema_source->get_name(), db_name);
          backend->add_schema_space_for_multiple(
              db_name, schema_source->get_name(), NULL,
              PUSHDOWN_PROCEDURE_DEPEND, false);
        } catch (...) {
          LOG_ERROR("Auto assign source to schema %s fail.\n", db_name);
          throw;
        }
      }
    }
  }
  if (db_name != NULL) {
    if (st.type == STMT_DROP_DB) {
      vector<string> tables;
      backend->get_all_table_name_for_db(db_name, tables);
      vector<string>::iterator table_it = tables.begin();
      for (; table_it != tables.end(); ++table_it) {
        plan->session->add_in_using_table(db_name, (*table_it).c_str());
      }
    }
    vector<DataSpace *> dataspaces;
    backend->get_all_dataspace_for_database(db_name, dataspaces);

    DataSpace *auth_data_space = backend->get_auth_data_space();
#ifndef DBSCALE_TEST_DISABLE
    dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(),
                    "auth_source_restrict") &&
        !strcasecmp(test_info->test_case_operation.c_str(),
                    "send_create_drop_db_to_auth")) {
      dataspaces.push_back(auth_data_space);
      LOG_DEBUG("dbscale test create_db send to auth source.\n");
    }
#else
    if (restrict_auth_source_topo && !datasource_in_one)
      dataspaces.push_back(auth_data_space);
#endif

    set<DataSpace *> merge_db;
    if (dataspaces.size() == 0) {
      throw Error("The schema name can not get a dataspace.");
    } else {
      ConfigSourceRepResource dataservers_rep_relation;
      plan->session->get_local_dataservers_rep_relation(
          dataservers_rep_relation);
#ifdef DEBUG
      map<DataSource *, set<DataSource *> > resource_dbug =
          dataservers_rep_relation.get_resource();
      map<DataSource *, set<DataSource *> >::iterator it_dbug =
          resource_dbug.begin();
      LOG_DEBUG("start print dataservers_rep_relation\n");
      for (; it_dbug != resource_dbug.end(); ++it_dbug) {
        DataSource *source = it_dbug->first;
        set<DataSource *> &slaves = it_dbug->second;
        set<DataSource *>::iterator it2 = slaves.begin();
        for (; it2 != slaves.end(); ++it2) {
          LOG_DEBUG("The datasource is [%s%@], its child is [%s%@]\n",
                    source->get_name(), source, (*it2)->get_name(), *it2);
        }
      }
      LOG_DEBUG("finish print dataservers_rep_relation\n");
      for (size_t i = 0; i < dataspaces.size(); ++i) {
        DataSource *source = dataspaces[i]->get_data_source();
        string source_name = source ? source->get_name() : "";
        LOG_DEBUG(
            "get dataspace [%s%@] with datasource name [%s] to calc merge_db\n",
            dataspaces[i]->get_name(), dataspaces[i], source_name.c_str());
      }
#endif

      set<DataSpace *> merge_db_tmp_1;
      vector<DataSpace *> merge_db_tmp_2;
      if (!merge_db_tmp_1.count(dataspaces[0])) {
        merge_db_tmp_2.push_back(dataspaces[0]);
        merge_db_tmp_1.insert(dataspaces[0]);
      }
      for (unsigned int i = 1; i < dataspaces.size(); ++i) {
        for (unsigned int j = 0; j < merge_db_tmp_2.size(); ++j) {
          if (use_same_shard_schema(dataspaces[i], merge_db_tmp_2[j],
                                    dataservers_rep_relation)) {
            merge_db_tmp_2[j] = dataspaces[i];
          } else if (!use_same_shard_schema(merge_db_tmp_2[j], dataspaces[i],
                                            dataservers_rep_relation)) {
            if (!merge_db_tmp_1.count(dataspaces[i])) {
              merge_db_tmp_2.push_back(dataspaces[i]);
              merge_db_tmp_1.insert(dataspaces[i]);
            }
          }
        }
      }
      if (merge_db_tmp_2.size() == 0) {
        throw Error("The schema name can not get a dataspace.");
      }
      merge_db_tmp_1.clear();
      for (size_t i = 0; i < merge_db_tmp_2.size(); ++i) {
        merge_db_tmp_1.insert(merge_db_tmp_2[i]);  // erase duplicated items
      }
      // check all dataspace in merge_db_tmp_1, find and use its master if
      // possible
      map<DataSource *, set<DataSource *> > resource =
          dataservers_rep_relation.get_resource();
      set<DataSpace *>::iterator it2 = merge_db_tmp_1.begin();
      set<DataSource *> used_top_source;
      for (; it2 != merge_db_tmp_1.end(); ++it2) {
        if ((*it2)->get_virtual_machine_id() > 0) {
          merge_db.insert(*it2);
        } else {
          DataSource *source = (*it2)->get_data_source();
          map<DataSource *, set<DataSource *> >::iterator it = resource.begin();
          bool got_parent = false;
          for (; it != resource.end(); ++it) {
            if (it->second.count(source)) {
              got_parent = true;
              source = dataservers_rep_relation.get_real_top_source(it->first);
              used_top_source.insert(source);
              break;
            }
          }
          if (!got_parent) {
            merge_db.insert(*it2);
          }
        }
      }
      // for all top datasource we used, add its related dataspace in merge_db
      // but we should make sure no dup dataserver between 2 datasources, which
      // means that 2 datasource in fact share same write dataserver, such as a
      // replication datasource with its inner master-source datasource
      set<DataServer *> used_servers;
      set<DataSpace *>::iterator it_merge_db = merge_db.begin();
      for (; it_merge_db != merge_db.end(); ++it_merge_db) {
        DataServer *used_server =
            (*it_merge_db)->get_data_source()->get_master_server();
        used_servers.insert(used_server);
      }
      set<DataSource *>::iterator it_used_top_source = used_top_source.begin();
      for (; it_used_top_source != used_top_source.end();
           ++it_used_top_source) {
        DataSource *source = *it_used_top_source;
        DataServer *used_server = source->get_master_server();
        if (used_servers.count(used_server)) {
          LOG_DEBUG(
              "found datasource '%s' shard same write server with other top "
              "datasource in dataservers_rep_relation, ignore it\n",
              source->get_name());
          continue;
        }
        DataSpace *space = source->get_one_space();
        if (!space) {
          string msg = "no schema/table using datasource '";
          msg.append(source->get_name());
          msg.append(
              "' yet, please dynamic add at least one schema/table using that "
              "datasource firstly");
          throw Error(msg.c_str());
        }
        if (!merge_db.count(space)) {
          merge_db.insert(space);
          used_servers.insert(used_server);
        }
      }
    }
#ifdef DEBUG
    set<DataSpace *>::iterator it_dbug = merge_db.begin();
    for (; it_dbug != merge_db.end(); ++it_dbug) {
      DataSource *source = (*it_dbug)->get_data_source();
      string source_name = source ? source->get_name() : "";
      LOG_DEBUG("get merge_db dataspace [%s%@] with datasource name [%s]\n",
                (*it_dbug)->get_name(), *it_dbug, source_name.c_str());
    }
#endif
    ExecuteNode *ok_node = plan->get_ok_node();
    ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
    ExecuteNode *modify_node;
    string new_sql_tmp;
    set<DataSpace *>::iterator it = merge_db.begin();
    bool need_apply_meta = true;
    for (; it != merge_db.end(); ++it) {
      DataSpace *space = *it;
      if (need_apply_meta) {
        if (is_share_same_server(
                space, Backend::instance()->get_metadata_data_space()))
          need_apply_meta = false;
      }
      if (space->get_virtual_machine_id() > 0) {
        if (st.type == STMT_DROP_DB)
          adjust_create_drop_db_sql_for_shard(space, db_name, new_sql_tmp, 0,
                                              sql);
        else if (st.type == STMT_CREATE_DB)
          adjust_create_drop_db_sql_for_shard(
              space, db_name, new_sql_tmp,
              st.sql->create_db_oper->db_name_end_pos, sql);
        else if (st.type == STMT_ALTER_DB)
          adjust_create_drop_db_sql_for_shard(
              space, db_name, new_sql_tmp,
              st.sql->alter_db_oper->db_name_end_pos, sql);
        record_shard_sql_str(new_sql_tmp);
        modify_node = plan->get_modify_node(space, get_last_shard_sql());
      } else {
        modify_node = plan->get_modify_node(space, sql);
      }
      ok_merge_node->add_child(modify_node);
    }
    st.need_apply_metadata = need_apply_meta;

    ok_node->add_child(ok_merge_node);
    plan->set_start_node(ok_node);
  }
}

void Statement::adjust_create_drop_db_sql_for_shard(DataSpace *space,
                                                    const char *sql_schema,
                                                    string &new_sql,
                                                    int db_name_end_pos,
                                                    const char *sql) {
  string new_schema;
  unsigned int vid = space->get_virtual_machine_id();
#ifdef DEBUG
  ACE_ASSERT(vid > 0);
#endif
  unsigned int pid = space->get_partition_id();
  stmt_type type = st.type;
  adjust_shard_schema(sql_schema, schema, new_schema, vid, pid);
  switch (type) {
    case STMT_DROP_DB: {
      if (st.sql->drop_db_oper->op_exists)
        new_sql.assign("DROP DATABASE IF EXISTS `");
      else
        new_sql.assign("DROP DATABASE `");
      break;
    }
    case STMT_CREATE_DB: {
      if (st.sql->create_db_oper->op_exists)
        new_sql.assign("CREATE DATABASE IF NOT EXISTS `");
      else
        new_sql.assign("CREATE DATABASE `");
      break;
    }
    case STMT_ALTER_DB: {
      new_sql.assign("ALTER DATABASE `");
      break;
    }
    default:
      throw Error("unsupport sql type for shard table schema adjust.");
  }
  new_sql.append(new_schema.c_str());
  new_sql.append("`");
  if (db_name_end_pos > 0) new_sql.append(&sql[db_name_end_pos]);
}

void Statement::assemble_migrate_plan(ExecutePlan *plan) {
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master()) {
    LOG_ERROR("slave dbscale do not migrate table\n");
    throw NotSupportedError("slave dbscale do not support migrate table.");
  }

  if (!enable_block_table) {
    LOG_ERROR("migrate table failed, plz set option [enable-block-table=1]\n");
    throw Error("migrate table failed, plz set option [enable-block-table=1].");
  }

  ExecuteNode *node = plan->get_migrate_node();
  plan->session->set_is_complex_stmt(true);
  plan->set_start_node(node);
}
void Statement::assemble_lock_tables_plan(ExecutePlan *plan) {
  map<DataSpace *, list<lock_table_item *> *> *lock_table_list =
      new map<DataSpace *, list<lock_table_item *> *>;
  plan->session->remove_all_lock_tables();
  try {
    build_lock_map(plan, *lock_table_list);
  } catch (...) {
    map<DataSpace *, list<lock_table_item *> *>::iterator it;
    while (!lock_table_list->empty()) {
      it = lock_table_list->begin();
      list<lock_table_item *> *table_list = it->second;
      lock_table_list->erase(it);
      table_list->clear();
      delete table_list;
    }
    delete lock_table_list;
    throw;
  }
  ExecuteNode *lock_node = plan->get_lock_node(lock_table_list);
  plan->set_start_node(lock_node);
}
void Statement::build_lock_map(
    ExecutePlan *plan,
    map<DataSpace *, list<lock_table_item *> *> &lock_table_list) {
  lock_table_item *head = st.sql->lock_tb_oper->lock_tb_list;
  lock_table_item *item = head;
  Backend *backend = Backend::instance();
  string full_view;
  string sub_sql;
  do {
    lock_table_full_item *lock_item = plan->session->add_lock_table(item);

    full_view.assign(lock_item->schema_name);
    full_view.append(".");
    full_view.append(lock_item->table_name);
    sub_sql.clear();
    plan->session->get_one_view(full_view, sub_sql);
    if (!sub_sql.empty()) {
      LOG_DEBUG("Find a view for lock table stmt, %s.\n", full_view.c_str());
      if (item->type == LOCK_TYPE_WRITE) {
        LOG_ERROR("Not support lock view %s for write.\n", full_view.c_str());
        throw NotSupportedError("Not support lock view for write.");
      }
      DataSpace *meta = Backend::instance()->get_metadata_data_space();
      if (!meta) {
        LOG_ERROR("Fail to get meta datasource.\n");
        throw Error("Fail to get meta datasource.");
      }
      add_dataspace_to_map(lock_table_list, meta, item);
    } else {
      Table *table = backend->get_table_by_name(lock_item->schema_name,
                                                lock_item->table_name);
      if (table == NULL) {
        DataSpace *space = backend->get_data_space_for_table(
            lock_item->schema_name, lock_item->table_name);
        add_dataspace_to_map(lock_table_list, space, item);
        LOG_DEBUG("Add table %s to space %@.\n", item->name, space);
      } else if (table->is_partitioned()) {
        PartitionedTable *part_table = (PartitionedTable *)table;
        for (unsigned int i = 0; i < part_table->get_real_partition_num();
             ++i) {
          if (part_table->get_partition(i)->get_virtual_machine_id() > 0) {
            string new_table_tmp;
            adjust_shard_schema(
                lock_item->schema_name, schema, new_table_tmp,
                part_table->get_partition(i)->get_virtual_machine_id(),
                part_table->get_partition(i)->get_partition_id());
            new_table_tmp.append(".");
            new_table_tmp.append(lock_item->table_name);
            record_shard_sql_str(new_table_tmp);
            lock_table_item *new_item =
                (lock_table_item *)assign_mem_for_struct(
                    &st, sizeof(lock_table_item));
            new_item->type = item->type;
            new_item->name = get_last_shard_sql();
            new_item->alias = item->alias;
            add_dataspace_to_map(lock_table_list, part_table->get_partition(i),
                                 new_item);
          } else {
            add_dataspace_to_map(lock_table_list, part_table->get_partition(i),
                                 item);
          }
        }
      } else {
        add_dataspace_to_map(lock_table_list, table, item);
      }
    }
    item = item->next;
  } while (item != head);

  ConfigSourceRepResource server_rep_resource;
  stmt_session->get_local_dataservers_rep_relation(server_rep_resource);
  map<DataSpace *, list<lock_table_item *> *>::iterator it_ds1 =
      lock_table_list.begin();
  map<DataSpace *, list<lock_table_item *> *>::iterator it_ds2 =
      lock_table_list.begin();
  for (; it_ds1 != lock_table_list.end(); ++it_ds1) {
    it_ds2 = it_ds1;
    ++it_ds2;
    for (; it_ds2 != lock_table_list.end(); ++it_ds2) {
      if (it_ds1->first->get_virtual_machine_id() ==
          it_ds2->first->get_virtual_machine_id()) {
        if (is_dataspace_contain(it_ds1->first, it_ds2->first,
                                 server_rep_resource)) {
          it_ds1->second->insert(it_ds1->second->end(), it_ds2->second->begin(),
                                 it_ds2->second->end());
        } else if (is_dataspace_contain(it_ds2->first, it_ds1->first,
                                        server_rep_resource)) {
          it_ds2->second->insert(it_ds2->second->end(), it_ds1->second->begin(),
                                 it_ds1->second->end());
        }
      }
    }
    it_ds1->second->sort();
    it_ds1->second->unique();
  }
}
void Statement::add_dataspace_to_map(
    map<DataSpace *, list<lock_table_item *> *> &lock_table_list,
    DataSpace *space, lock_table_item *item) {
  if (lock_table_list.count(space)) {
    list<lock_table_item *> *table_list = lock_table_list[space];
    table_list->push_back(item);
  } else {
    map<DataSpace *, list<lock_table_item *> *>::iterator it =
        lock_table_list.begin();
    for (; it != lock_table_list.end(); ++it) {
      if (is_share_same_server(space, it->first)) {
        list<lock_table_item *> *table_list = it->second;
        table_list->push_back(item);
        return;
      }
    }
    list<lock_table_item *> *new_table_list = new list<lock_table_item *>();
    new_table_list->push_back(item);
    lock_table_list[space] = new_table_list;
  }
}
void Statement::assemble_transaction_unlock_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble transaction or unlock plan.\n");
  ExecuteNode *node = plan->get_transaction_unlock_node(sql, true);
  plan->set_start_node(node);
}

void Statement::assemble_xatransaction_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble xa transaction.\n");
  ExecuteNode *node = plan->get_xatransaction_node(sql, true);
  plan->set_start_node(node);
}
void Statement::assemble_cluster_xatransaction_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble cluster xa transaction.\n");
  ExecuteNode *node = plan->get_cluster_xatransaction_node();
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_add_data_server_plan(
    ExecutePlan *plan,
    dynamic_add_data_server_op_node *dynamic_add_data_server_oper) {
  LOG_DEBUG("Assemble dynamic add data server plan.\n");
  ExecuteNode *node =
      plan->get_dynamic_add_data_server_node(dynamic_add_data_server_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_add_data_source_plan(
    ExecutePlan *plan,
    dynamic_add_data_source_op_node *dynamic_add_data_source_oper, int type) {
  LOG_DEBUG("Assemble dynamic add data source plan.\n");
  DataSourceType stype = (DataSourceType)type;
  ExecuteNode *node = plan->get_dynamic_add_data_source_node(
      dynamic_add_data_source_oper, stype);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_add_data_space_plan(
    ExecutePlan *plan,
    dynamic_add_data_space_op_node *dynamic_add_data_space_oper) {
  LOG_DEBUG("Assemble add data space.\n");
  ExecuteNode *node =
      plan->get_dynamic_add_data_space_node(dynamic_add_data_space_oper);
  plan->set_start_node(node);
}
void Statement::assemble_set_user_not_allow_operation_time_plan(
    ExecutePlan *plan, set_user_not_allow_operation_time_node *oper) {
  LOG_DEBUG("Assemble set user allow operation time plan.\n");
  ExecuteNode *node = plan->get_set_user_not_allow_operation_time_node(oper);
  plan->set_start_node(node);
}
void Statement::assemble_reload_user_not_allow_operation_time_plan(
    ExecutePlan *plan, string user_name) {
  LOG_DEBUG("Assemble reload user allow operation time plan.\n");
  ExecuteNode *node =
      plan->get_reload_user_not_allow_operation_time_node(user_name);
  plan->set_start_node(node);
}
void Statement::assemble_show_user_not_allow_operation_time_plan(
    ExecutePlan *plan, string user_name) {
  LOG_DEBUG("Assemble show user allow operation time plan.\n");
  ExecuteNode *node =
      plan->get_show_user_not_allow_operation_time_node(user_name);
  plan->set_start_node(node);
}
void Statement::assemble_set_schema_acl_plan(
    ExecutePlan *plan, set_schema_acl_op_node *set_schema_acl_oper) {
  LOG_DEBUG("Assemble set schema ACL plan.\n");
  ExecuteNode *node = plan->get_set_schema_acl_node(set_schema_acl_oper);
  plan->set_start_node(node);
}
void Statement::assemble_set_table_acl_plan(
    ExecutePlan *plan, set_table_acl_op_node *set_table_acl_oper) {
  LOG_DEBUG("Assemble set table ACL plan.\n");
  ExecuteNode *node = plan->get_set_table_acl_node(set_table_acl_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_add_slave_plan(
    ExecutePlan *plan, dynamic_add_slave_op_node *dynamic_add_slave_oper) {
  LOG_DEBUG("Assemble dynamic add slave plan.\n");
  ExecuteNode *node = plan->get_dynamic_add_slave_node(dynamic_add_slave_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_add_pre_disaster_master_plan(
    ExecutePlan *plan, dynamic_add_pre_disaster_master_op_node
                           *dynamic_add_pre_disaster_master_oper) {
  LOG_DEBUG("Assemble dynamic add pre_disaster_master plan.\n");
  if (!enable_disaster_mode) {
    LOG_ERROR(
        "Dynamic add pre disaster master failed because enable_disaster_mode "
        "param is 1.\n");
    throw DynamicOpFail(
        "Dynamic add pre disaster master failed because enable_disaster_mode "
        "status is error.");
  }
  if (slave_dbscale_mode == 1) {
    LOG_ERROR(
        "Dynamic add pre disaster master failed because slave_dbscale_mode "
        "param is 1.\n");
    throw DynamicOpFail(
        "Dynamic add pre disaster master failed because slave_dbscale_mode "
        "status is error.");
  }

  const char *pre_disaster_master_hosts = pre_disaster_master_info.c_str();
  if (pre_disaster_master_hosts[0] == '\0') {
    LOG_ERROR(
        "Dynamic add pre disaster master failed because "
        "pre_disaster_master_hosts param is null.\n");
    throw DynamicOpFail(
        "Dynamic add pre disaster master failed because "
        "pre_disaster_master_hosts param is null.");
  } else {
    dynamic_add_pre_disaster_master_oper->add_server_op->server_host =
        pre_disaster_master_hosts;
    LOG_DEBUG(
        "Dynamic add pre disaster master when pre_disaster_master_hosts param "
        "is [%s]\n",
        pre_disaster_master_hosts);
  }

  dynamic_add_data_server_op_node *cur_server_op =
      dynamic_add_pre_disaster_master_oper->add_server_op;
  dynamic_add_data_source_op_node *cur_source_op =
      dynamic_add_pre_disaster_master_oper->add_source_op;
  dynamic_add_slave_op_node *cur_slave_op =
      dynamic_add_pre_disaster_master_oper->add_slave_op;

#ifndef DBSCALE_TEST_DISABLE
  /*just for test pre-disaster-master*/
  dbscale_test_info *test_info = Backend::instance()->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(),
                  "pre_disaster_master_test")) {
    if (!strcasecmp(test_info->test_case_operation.c_str(), "datasource")) {
      cur_source_op->server_list->server_name = "pre_disaster_server_error";
    } else if (!strcasecmp(test_info->test_case_operation.c_str(), "slave")) {
      cur_slave_op->slave_info->slave_source = "pre_disaster_source_error";
    }
  }
#endif

  LOG_DEBUG(
      "master_source[%s], slave_source[%s], server_name[%s], "
      "pool-set[%d-%d-%d-%d], "
      "server_host[%s], server_port[%d], by_add_pre_disaster_master[%d]\n",
      cur_slave_op->master_name, cur_source_op->source_name,
      cur_server_op->server_name, cur_source_op->server_list->min,
      cur_source_op->server_list->max, cur_source_op->server_list->low,
      cur_source_op->server_list->high, cur_server_op->server_host,
      cur_server_op->server_port, cur_server_op->by_add_pre_disaster_master);

  try {
    ExecuteNode *add_server_node =
        plan->get_dynamic_add_data_server_node(cur_server_op);
    plan->set_start_node(add_server_node);
    plan->execute();
    plan->clean_start_node();
    ExecuteNode *add_source_node = plan->get_dynamic_add_data_source_node(
        cur_source_op, DATASOURCE_TYPE_SERVER);
    plan->set_start_node(add_source_node);
    plan->execute();
    plan->clean_start_node();
  } catch (ErrorPacketException &e) {
    LOG_ERROR(
        "Dynamic add pre disaster master failed because dynamic add dataserver "
        "or datasource failed.\n");
    throw;
  }

  ExecuteNode *add_slave_node = plan->get_dynamic_add_slave_node(cur_slave_op);
  plan->set_start_node(add_slave_node);
}  // namespace sql
void Statement::assemble_dbscale_reset_tmp_table_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble reset tmp table plan.\n");
  ExecuteNode *node = plan->get_reset_tmp_table_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_reload_func_type_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble reload function type map plan.\n");
  ExecuteNode *node = plan->get_reload_function_type_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_shutdown_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble shutdown plan.\n");
  ExecuteNode *node = plan->get_shutdown_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_set_pool_plan(ExecutePlan *plan,
                                               pool_node *pool_info) {
  LOG_DEBUG("Assemble dynamic set pool plan.\n");
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master() &&
      !pool_info->is_internal) {
    assemble_forward_master_role_plan(plan);
  } else {
    ExecuteNode *node = plan->get_dbscale_set_pool_info_node(pool_info);
    plan->set_start_node(node);
  }
}

void Statement::assemble_dbscale_reset_info_plan(
    ExecutePlan *plan, dbscale_reset_info_op_node *oper) {
  LOG_DEBUG("Assemble reset dbscale info plan.\n");
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master() &&
      !oper->is_internal) {
    assemble_forward_master_role_plan(plan);
  } else {
    ExecuteNode *node = plan->get_dbscale_reset_info_plan(oper);
    plan->set_start_node(node);
  }
}

void Statement::assemble_dbscale_disable_server_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale disbale server plan.\n");
  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master()) {
    assemble_forward_master_role_plan(plan);
  } else {
    ExecuteNode *node = plan->get_dbscale_disable_server_node();
    plan->set_start_node(node);
  }
}
void Statement::assemble_dbscale_block_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale block plan.\n");

  if (multiple_mode && !MultipleManager::instance()->get_is_cluster_master()) {
    LOG_ERROR("slave dbscale do not support block table\n");
    throw NotSupportedError("slave dbscale do not support block table.");
  }
  if (!enable_block_table) {
    LOG_ERROR("block table failed, plz set option [enable-block-table=1]\n");
    throw Error("block table failed, plz set option [enable-block-table=1].");
  }
  ExecuteNode *node = plan->get_dbscale_block_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_force_flashback_online_plan(
    ExecutePlan *plan) {
  const char *name = st.sql->flashback_oper->server_name;
  ExecuteNode *node = plan->get_dbscale_force_flashback_online_node(name);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_xa_recover_slave_dbscale_plan(
    ExecutePlan *plan) {
  const char *xa_source =
      st.sql->dbscale_xa_recover_slave_dbscale_oper->xa_recover_source_name;
  const char *top_source =
      st.sql->dbscale_xa_recover_slave_dbscale_oper->top_source_name;
  const char *ka_update_v =
      st.sql->dbscale_xa_recover_slave_dbscale_oper->ka_update_version;
  ExecuteNode *node = plan->get_dbscale_xa_recover_slave_dbscale_node(
      xa_source, top_source, ka_update_v);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_flush_pool_plan(ExecutePlan *plan,
                                                 pool_node *pool_info) {
  LOG_DEBUG("Assemble dbscale flush pool plan.\n");
  ExecuteNode *node = plan->get_dbscale_flush_pool_info_node(pool_info);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_flush_plan(ExecutePlan *plan,
                                            dbscale_flush_type type) {
  LOG_DEBUG("Assemble dbscale flush plan.\n");
  ExecuteNode *node = plan->get_dbscale_flush_node(type);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_flush_weak_pwd_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale flush weak password file plan.\n");
  ExecuteNode *node = plan->get_dbscale_flush_weak_pwd_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_set_priority_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale set priority plan.\n");
  string user_name = get_stmt_node()->sql->priority_oper->user_name;
  int tmp_priority_value = get_stmt_node()->sql->priority_oper->priority_value;
  ExecuteNode *node =
      plan->get_dbscale_set_priority_info_node(user_name, tmp_priority_value);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_change_master_plan(
    ExecutePlan *plan,
    dynamic_change_master_op_node *dynamic_change_master_oper) {
  LOG_DEBUG("Assemble dynamic change master plan.\n");
  ExecuteNode *node =
      plan->get_dynamic_change_master_node(dynamic_change_master_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_change_multiple_master_active_plan(
    ExecutePlan *plan, dynamic_change_multiple_master_active_op_node
                           *dynamic_change_multiple_master_active_oper) {
  LOG_DEBUG(
      "Assemble dynamic change multiple master datasource's active source "
      "plan.\n");
  ExecuteNode *node = plan->get_dynamic_change_multiple_master_active_node(
      dynamic_change_multiple_master_active_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_change_dataserver_ssh_plan(
    ExecutePlan *plan, const char *server_name, const char *username,
    const char *pwd, int port) {
  LOG_DEBUG("Assemble dynamic change dataserver remote ssh plan.\n");
  ExecuteNode *node = plan->get_dynamic_change_dataserver_ssh_node(
      server_name, username, pwd, port);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_remove_slave_plan(
    ExecutePlan *plan,
    dynamic_remove_slave_op_node *dynamic_remove_slave_oper) {
  LOG_DEBUG("Assemble dynamic remove slave plan.\n");
  ExecuteNode *node =
      plan->get_dynamic_remove_slave_node(dynamic_remove_slave_oper);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_remove_schema_plan(ExecutePlan *plan,
                                                    const char *schema_name,
                                                    bool is_force) {
  LOG_DEBUG("Assemble dynamic remove sehema plan.\n");
  ExecuteNode *node =
      plan->get_dynamic_remove_schema_node(schema_name, is_force);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_remove_table_plan(ExecutePlan *plan,
                                                   const char *table_name,
                                                   bool is_force) {
  LOG_DEBUG("Assemble dynamic remove table plan.\n");
  ExecuteNode *node = plan->get_dynamic_remove_table_node(table_name, is_force);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_change_table_scheme_plan(
    ExecutePlan *plan, const char *table_name, const char *scheme_name) {
  LOG_DEBUG("Assemble dynamic change table scheme plan.\n");
  ExecuteNode *node =
      plan->get_dynamic_change_table_scheme_node(table_name, scheme_name);
  plan->set_start_node(node);
}
void Statement::assemble_dynamic_remove_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dynamic remove plan.\n");
  const char *name = st.sql->dynamic_remove_oper->name;
  ExecuteNode *node = plan->get_dynamic_remove_node(name);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_show_white_list(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale show white list plan\n");
  sql =
      "SELECT user_name, ip, ssl_option, ssl_cipher, x509_issuer, "
      "x509_subject, comment FROM dbscale.WHITE_USER_INFO order by user_name, "
      "ip";
  DataSpace *dataspace = Backend::instance()->get_config_data_space();
  if (!dataspace) {
    LOG_ERROR("Can not find the metadata dataspace.\n");
    throw Error(
        "Fail to get meta dataspace, please make sure meta datasource is "
        "configured.");
  }
  assemble_direct_exec_plan(plan, dataspace);
}
void Statement::assemble_dbscale_help_plan(ExecutePlan *plan,
                                           const char *name) {
  LOG_DEBUG("Assemble dbscale help plan");
  ExecuteNode *node = plan->get_dbscale_help_node(name);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_show_join_plan(ExecutePlan *plan,
                                                const char *name) {
  LOG_DEBUG("Assemble dbscale show join status node.\n");
  ExecuteNode *node = plan->get_dbscale_show_join_node(name);
  plan->set_start_node(node);
}
void Statement::assemble_show_user_memory_status_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show user memory status plan.\n");
  ExecuteNode *node = plan->get_show_user_memory_status_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_show_base_status_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble dbscale show base status node.\n");
  ExecuteNode *node = plan->get_dbscale_show_base_status_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_user_status_plan(
    ExecutePlan *plan, const char *user_id, const char *user_name,
    bool only_show_running, bool instance, bool show_status_count) {
  LOG_DEBUG("Assemble show user status plan.\n");
  ExecuteNode *node = plan->get_show_user_status_node(
      user_id, user_name, only_show_running, instance, show_status_count);
  plan->set_start_node(node);
}
void Statement::assemble_show_user_processlist_plan(ExecutePlan *plan,
                                                    const char *cluster_id,
                                                    const char *user_id,
                                                    int local) {
  LOG_DEBUG("Assemble show user processlist plan.\n");
  ExecuteNode *node =
      plan->get_show_user_processlist_node(cluster_id, user_id, local);
  plan->set_start_node(node);
}
void Statement::assemble_show_session_id_plan(ExecutePlan *plan,
                                              const char *server_name,
                                              int connection_id) {
  LOG_DEBUG("Assemble show session id by server and connection id plan.\n");
  Backend *backend = Backend::instance();
  DataServer *server = backend->find_data_server(server_name);
  if (server) {
    ExecuteNode *node =
        plan->get_show_session_id_node(server_name, connection_id);
    plan->set_start_node(node);
  } else {
    LOG_ERROR("assemble_show_session_id_plan Unknown DataServer name [%s]\n",
              server_name);
    throw Error("Unknown DataServer name.");
  }
}

void Statement::assemble_dbscale_execute_on_dataserver_plan(
    ExecutePlan *plan, const char *dataserver_name, const char *stmt_sql) {
  if (plan->session->is_in_transaction()) {
    LOG_ERROR(
        "current session is in transaction, can not execute this statement.\n");
    throw Error(
        "current session is in transaction, can not execute this statement");
  }
  Backend *backend = Backend::instance();
  Parser *parser = Driver::get_driver()->get_parser();
  Statement *stmt_tmp = NULL;
  try {
    stmt_tmp = parser->parse(stmt_sql, stmt_allow_dot_in_ident, true, NULL,
                             NULL, NULL, ctype);
  } catch (exception &e) {
    if (stmt_tmp) {
      stmt_tmp->free_resource();
      delete stmt_tmp;
      stmt_tmp = NULL;
    }
    LOG_ERROR("got error when parse dbscale execute on dataserver sql: %s\n",
              e.what());
    throw Error(e.what());
  }
  if (!stmt_tmp) {
    LOG_ERROR("Fail to parse sql [%s].\n", stmt_sql);
    throw Error("Fail to parse target sql.");
  }
  stmt_type type = stmt_tmp->get_stmt_node()->type;
  if (!(type > STMT_READ_ONLY_START && type < STMT_READ_ONLY_END) &&
      (allow_modify_server_directly == 0)) {
    stmt_tmp->free_resource();
    delete stmt_tmp;
    stmt_tmp = NULL;
    LOG_ERROR(
        "when allow_modify_server_directly=0, modify sql is not allowed via "
        "STMT_DBSCALE_EXECUTE_ON_DATASERVER, set "
        "allow_modify_server_directly=1 if you really want.\n");
    throw Error(
        "when allow_modify_server_directly=0, modify sql is not allowed via "
        "STMT_DBSCALE_EXECUTE_ON_DATASERVER, set "
        "allow_modify_server_directly=1 if you really want");
  }
  if (stmt_tmp->is_partial_parsed()) {
    stmt_tmp->free_resource();
    delete stmt_tmp;
    stmt_tmp = NULL;
    LOG_ERROR(
        "STMT_DBSCALE_EXECUTE_ON_DATASERVER not support partial parsed sql.\n");
    throw Error("not support partial parsed sql");
  }
  if (stmt_tmp->get_stmt_node()->next) {
    stmt_tmp->free_resource();
    delete stmt_tmp;
    stmt_tmp = NULL;
    LOG_ERROR("only support 1 statement at a time.\n");
    throw Error("only support 1 statement at a time");
  }
  stmt_tmp->free_resource();
  delete stmt_tmp;
  stmt_tmp = NULL;

  DataServer *server = backend->find_data_server(dataserver_name);
  if (!server) {
    LOG_ERROR("Unknown dataserver [%s]\n", dataserver_name);
    throw Error("unknown dataserver");
  }
  LOG_DEBUG("assemble_dbscale_execute_on_dataserver_plan.\n");

  list<DataSource *> server_sources;
  backend->find_data_source_by_type(DATASOURCE_TYPE_SERVER, server_sources);
  list<DataSource *>::iterator it = server_sources.begin();
  ServerDataSource *used_server_ds = NULL;
  for (; it != server_sources.end(); ++it) {
    ServerDataSource *sd = (ServerDataSource *)(*it);
    const char *server_name = sd->get_server()->get_name();
    if (!strcmp(dataserver_name, server_name)) {
      used_server_ds = sd;
      break;
    }
  }
  if (!used_server_ds) {
    LOG_ERROR("can not find server_datasource named [%s]\n", dataserver_name);
    throw Error("can not find related server datasource");
  }
  string space_name = dataserver_name;
  Schema *sch = NULL;
  do {
    space_name = DBSCALE_RESERVED_STR + space_name;
    sch = backend->find_schema(space_name.c_str());
  } while (sch != NULL);
  sch = new Schema(space_name.c_str(), used_server_ds,
                   backend->get_default_catalog());
  sch->set_need_force_conn(true);
  LOG_DEBUG(
      "Add tmp schema named [%s] to execute "
      "STMT_DBSCALE_EXECUTE_ON_DATASERVER, this schema will be desdroyed in "
      "DirectExecuteNode clean().\n",
      sch->get_name());
  plan->session->set_is_simple_direct_stmt(true);
  plan->set_need_destroy_dataspace_after_exec(true);
  ExecuteNode *node = plan->get_direct_execute_node((DataSpace *)sch, stmt_sql);
  plan->set_start_node(node);
}

void Statement::assemble_backend_server_execute_plan(ExecutePlan *plan,
                                                     const char *stmt_sql) {
  LOG_DEBUG("Assemble backend server execute plan.\n");
  ExecuteNode *node = plan->get_backend_server_execute_node(stmt_sql);
  plan->set_start_node(node);
}
void Statement::assemble_execute_on_all_masterserver_execute_plan(
    ExecutePlan *plan, const char *stmt_sql) {
  LOG_DEBUG("Assemble execute on all masterserver execute plan.\n");
  ExecuteNode *node =
      plan->get_execute_on_all_masterserver_execute_node(stmt_sql);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_dynamic_update_white_plan(
    ExecutePlan *plan, bool is_add, const char *ip,
    const ssl_option_struct &ssl_option_value, const char *comment,
    const char *user_name) {
  LOG_DEBUG("assemble_dbscale_dynamic_update_white_plan.\n");
  ExecuteNode *node = plan->get_dynamic_update_white_node(
      is_add, ip, ssl_option_value, comment, user_name);
  plan->set_start_node(node);
}
void Statement::assemble_show_table_location_plan(ExecutePlan *plan,
                                                  const char *schema_name,
                                                  const char *table_name) {
  LOG_DEBUG("Assemble show table loction plan.\n");
  ExecuteNode *node =
      plan->get_show_table_location_node(schema_name, table_name);
  plan->set_start_node(node);
}
void Statement::assemble_show_monitor_point_status_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show monitor point status plan.\n");
  ExecuteNode *node = plan->get_show_monitor_point_status_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_global_monitor_point_status_plan(
    ExecutePlan *plan) {
  LOG_DEBUG("Assemble show global monitor point status plan.\n");
  ExecuteNode *node = plan->get_show_global_monitor_point_status_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_histogram_monitor_point_status_plan(
    ExecutePlan *plan) {
  LOG_DEBUG("Assemble show histogram monitor point status plan.\n");
  ExecuteNode *node = plan->get_show_histogram_monitor_point_status_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_outline_monitor_info_plan(
    ExecutePlan *plan) {
  LOG_DEBUG("Assemble show outline monitor info.\n");
  ExecuteNode *node = plan->get_show_outline_monitor_info_node();
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_create_outline_hint_plan(
    ExecutePlan *plan, dbscale_operate_outline_hint_node *oper) {
  LOG_DEBUG("Assemble create outline hint info.\n");
  ExecuteNode *node = plan->get_dbscale_create_outline_hint_node(oper);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_flush_outline_hint_plan(
    ExecutePlan *plan, dbscale_operate_outline_hint_node *oper) {
  LOG_DEBUG("Assemble flush outline hint info.\n");
  ExecuteNode *node = plan->get_dbscale_flush_outline_hint_node(oper);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_show_outline_hint_plan(
    ExecutePlan *plan, dbscale_operate_outline_hint_node *oper) {
  LOG_DEBUG("Assemble show outline hint info.\n");
  ExecuteNode *node = plan->get_dbscale_show_outline_hint_node(oper);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_delete_outline_hint_plan(
    ExecutePlan *plan, dbscale_operate_outline_hint_node *oper) {
  LOG_DEBUG("Assemble delete outline hint info.\n");
  ExecuteNode *node = plan->get_dbscale_delete_outline_hint_node(oper);
  plan->set_start_node(node);
}
void Statement::assemble_com_query_prepare_plan(ExecutePlan *plan,
                                                DataSpace *dataspace,
                                                const char *query_sql,
                                                const char *name) {
  LOG_DEBUG("Assemble COM_QUERY PREPARE plan.\n");
  ExecuteNode *node =
      plan->get_com_query_prepare_node(dataspace, query_sql, name, sql);
  plan->set_start_node(node);
}
void Statement::assemble_com_query_exec_prepare_plan(ExecutePlan *plan,
                                                     const char *name,
                                                     var_item *var_item_list) {
  LOG_DEBUG("Assemble COM_QUERY EXEC PREPARE plan.\n");
  ExecuteNode *node =
      plan->get_com_query_exec_prepare_node(name, var_item_list);
  plan->set_start_node(node);
}
void Statement::assemble_com_query_drop_prepare_plan(ExecutePlan *plan,
                                                     DataSpace *dataspace,
                                                     const char *name,
                                                     const char *sql) {
  LOG_DEBUG("Assemble COM_QUERY DROP PREPARE plan.\n");
  ExecuteNode *node =
      plan->get_com_query_drop_prepare_node(dataspace, name, sql);
  plan->set_start_node(node);
}
void Statement::assemble_dbscale_flush_config_to_file_plan(
    ExecutePlan *plan, const char *file_name, bool flush_all) {
  LOG_DEBUG("Assemble DBSCALE FLUSH CONFIG TO FILE plan, flush_all is %d\n",
            flush_all);
  ExecuteNode *node =
      plan->get_dbscale_flush_config_to_file_node(file_name, flush_all);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_flush_acl_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble DBSCALE FLUSH ACL plan\n");
  ExecuteNode *node = plan->get_dbscale_flush_acl_node();
  plan->set_start_node(node);
}

void Statement::assemble_show_func_or_proc_status_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble function or procedure status plan.\n");
  Backend *backend = Backend::instance();
  Catalog *catalog = backend->get_default_catalog();
  map<string, Schema *, strcasecomp> schema_map_tmp;
  catalog->get_schema_map(schema_map_tmp);
  map<string, Schema *, strcasecomp> *schema_map = &schema_map_tmp;

  ExecuteNode *send_node = plan->get_send_node();
  list<const char *> *pattern1 = new list<const char *>();

  for (map<string, Schema *, strcasecomp>::iterator it = schema_map->begin();
       it != schema_map->end(); ++it) {
    pattern1->push_back(it->first.c_str());

    list<const char *> *pattern2 = new list<const char *>();
    pattern2->push_back(it->first.c_str());
    ExecuteNode *filter_node =
        plan->get_regex_filter_node(FILTER_FLAG_MATCH, 0, pattern2);
    const char *used_sql = adjust_stmt_sql_for_shard(it->second, sql);
    ExecuteNode *fetch_node = plan->get_fetch_node(it->second, used_sql);
    filter_node->add_child(fetch_node);
    send_node->add_child(filter_node);
  }

  ExecuteNode *filter_node =
      plan->get_regex_filter_node(FILTER_FLAG_UNMATCH, 0, pattern1);
  ExecuteNode *fetch_node = plan->get_fetch_node(catalog, sql);
  filter_node->add_child(fetch_node);
  send_node->add_child(filter_node);

  plan->set_start_node(send_node);
} /* assemble_show_func_or_proc_status_plan */

void Statement::assemble_dbscale_shutdown_plan(ExecutePlan *plan,
                                               int cluster_id) {
  check_is_multiple_mode();
  ExecuteNode *node = plan->get_dbscale_cluster_shutdown_node(cluster_id);
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_request_cluster_id_plan(ExecutePlan *plan) {
  check_is_multiple_mode();
  if (MultipleManager::instance()->get_is_cluster_master()) {
    ExecuteNode *node = plan->get_dbscale_request_cluster_id_node();
    plan->set_start_node(node);
  } else {
    LOG_ERROR(
        "This dbscale is not cluster master, but get request get_cluster_id\n");
    throw Error(
        "This dbscale is not cluster master, but get request get_cluster_id");
  }
}

void Statement::assemble_dbscale_request_all_cluster_inc_info_plan(
    ExecutePlan *plan) {
  check_is_multiple_mode();
  ExecuteNode *node = plan->get_dbscale_request_all_cluster_inc_info_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_request_node_info_plan(ExecutePlan *plan) {
  check_is_multiple_mode();
  ExecuteNode *node = plan->get_dbscale_request_node_info_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_request_cluster_info_plan(ExecutePlan *plan) {
  check_is_multiple_mode();
  ExecuteNode *node = plan->get_dbscale_request_cluster_info_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_show_async_task_plan(ExecutePlan *plan) {
  ExecuteNode *node = plan->get_show_async_task_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_clean_temp_table_cache(ExecutePlan *plan) {
  LOG_DEBUG("Assemble clean temp table cache plan.\n");
  ExecuteNode *node = plan->get_clean_temp_table_cache_node();
  plan->set_start_node(node);
}

void Statement::assemble_dbscale_request_cluster_user_status_plan(
    ExecutePlan *plan, const char *id, bool only_show_running,
    bool show_status_count) {
  ExecuteNode *node = plan->get_dbscale_request_cluster_user_status_node(
      id, only_show_running, show_status_count);
  plan->set_start_node(node);
}

void Statement::check_is_multiple_mode() {
  if (!multiple_mode) {
    LOG_ERROR("the command is only support in multiple dbscale mode\n");
    throw Error("the command is only support in multiple dbscale mode");
  }
}

void Statement::assemble_dbscale_request_cluster_inc_info_plan(
    ExecutePlan *plan) {
  check_is_multiple_mode();
  if (!MultipleManager::instance()->get_is_cluster_master()) {
    LOG_ERROR(
        "This dbscale is not cluster master, but get request "
        "get_cluster_inc_info\n");
    throw Error(
        "This dbscale is not cluster master, but get request "
        "get_cluster_inc_info");
  }
  DataSpace *space = NULL;
  table_link *link = st.table_list_head;
  join_node *node = link->join;
  const char *schema_name = node->schema_name ? node->schema_name : schema;
  const char *table_name = node->table_name;
  Backend *backend = Backend::instance();
  space = backend->get_data_space_for_table(schema_name, table_name);
  if (space->get_dataspace_type() == TABLE_TYPE &&
      ((Table *)space)->is_partitioned()) {
    ExecuteNode *cluster_node = plan->get_dbscale_request_cluster_inc_info_node(
        (PartitionedTable *)space, schema_name, table_name);
    plan->set_start_node(cluster_node);
  } else {
    LOG_ERROR(
        "Not support get cluster Auto_increment value for normal table.\n");
    throw Error(
        "Not support get cluster Auto_increment value for normal table.");
  }
}
void Statement::assemble_show_table_status_plan(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  const char *schema_name;
  if (st.sql->show_tables_oper == NULL)
    schema_name = schema;
  else {
    schema_name = st.sql->show_tables_oper->db_name
                      ? st.sql->show_tables_oper->db_name
                      : schema;
  }

  int is_simple = st.sql->show_tables_oper->is_simple;

  if (st.sql->show_tables_oper->table_pattern) {
    string table_string(st.sql->show_tables_oper->table_pattern);
    unsigned first_dot_position = table_string.find(".");
    if (first_dot_position == (unsigned)string::npos) {
      table_string.assign(schema_name);
      table_string.append(".");
      table_string.append(st.sql->show_tables_oper->table_pattern);
    }
    string replace_str1_ori("\\_");
    string replace_str1_new("_");
    string replace_str2_ori("\\%");
    string replace_str2_new("%");

    replace_tmp_table_name(table_string, replace_str1_ori, replace_str1_new,
                           false);
    replace_tmp_table_name(table_string, replace_str2_ori, replace_str2_new,
                           false);

    LOG_DEBUG("show table status like get table name after repalce %s.\n",
              table_string.c_str());

    string sub_sql;
    plan->session->get_one_view(table_string, sub_sql);
    if (!sub_sql.empty()) {
      LOG_DEBUG("Find a view for show table status like stmt, %s.\n",
                table_string.c_str());
      DataSpace *dataspace = backend->get_metadata_data_space();
      if (!dataspace) {
        LOG_ERROR("Fail to find meta datasource.\n");
        throw Error("Fail to find meta datasource.");
      }
      assemble_direct_exec_plan(plan, dataspace);
      return;
    }
  }

  map<pair<DataSpace *, enum FilterFlag>, list<const char *> *> dataspaces;
  backend->get_dataspace_pattern_for_schema(schema_name, dataspaces);
  map<pair<DataSpace *, enum FilterFlag>, list<const char *> *>::iterator it =
      dataspaces.begin();
  if (!is_simple) {
    if (dataspaces.size() == 1) {
      assemble_direct_exec_plan(plan, (it->first).first);
      if (it->second) {
        it->second->clear();
        delete it->second;
      }
      return;
    } else {
      plan->session->set_execute_plan_touch_partition_nums(-1);
      list<AggregateDesc> aggregate_desc;
      struct AggregateDesc aggre_desc[8];
      aggre_desc[0].column_index = 4;
      aggre_desc[0].type = AGGREGATE_TYPE_SUM;
      aggre_desc[1].column_index = 6;
      aggre_desc[1].type = AGGREGATE_TYPE_SUM;
      aggre_desc[2].column_index = 7;
      aggre_desc[2].type = AGGREGATE_TYPE_MAX;
      aggre_desc[3].column_index = 8;
      aggre_desc[3].type = AGGREGATE_TYPE_SUM;
      aggre_desc[4].column_index = 9;
      aggre_desc[4].type = AGGREGATE_TYPE_SUM;
      aggre_desc[5].column_index = 11;
      aggre_desc[5].type = AGGREGATE_TYPE_MIN;
      aggre_desc[6].column_index = 12;
      aggre_desc[6].type = AGGREGATE_TYPE_MAX;
      aggre_desc[7].column_index = 13;
      aggre_desc[7].type = AGGREGATE_TYPE_MAX;
      for (int i = 0; i < 8; ++i) {
        aggregate_desc.push_back(aggre_desc[i]);
      }
      list<SortDesc> group;
      struct SortDesc sd;
      sd.column_index = 0;
      sd.sort_order = ORDER_ASC;
      sd.is_cs = false;
      sd.ctype = CHARSET_TYPE_OTHER;
      sd.is_utf8mb4_bin = false;
      group.push_back(sd);
      ExecuteNode *send_node = plan->get_send_node();
      ExecuteNode *fetch_node;
      ExecuteNode *filter_node;
      ExecuteNode *single_node;
      ExecuteNode *group_by_node = plan->get_group_node(&group, aggregate_desc);
      ExecuteNode *avg_show_table_status_node =
          plan->get_avg_show_table_status_node();
      for (; it != dataspaces.end(); ++it) {
        single_node = plan->get_single_sort_node(&group);
        filter_node =
            plan->get_regex_filter_node((it->first).second, 0, it->second);
        const char *used_sql = adjust_stmt_sql_for_shard_for_show_table_status(
            (it->first).first, sql);
        fetch_node = plan->get_fetch_node((it->first).first, used_sql);
        filter_node->add_child(fetch_node);
        single_node->add_child(filter_node);
        group_by_node->add_child(single_node);
      }
      avg_show_table_status_node->add_child(group_by_node);
      send_node->add_child(avg_show_table_status_node);
      plan->set_start_node(send_node);
    }
  } else {
    plan->session->set_execute_plan_touch_partition_nums(-1);
    list<AggregateDesc> aggregate_desc;
    struct AggregateDesc aggre_desc[3];
    aggre_desc[0].column_index = 4;
    aggre_desc[0].type = AGGREGATE_TYPE_SUM;
    aggre_desc[1].column_index = 6;
    aggre_desc[1].type = AGGREGATE_TYPE_SUM;
    aggre_desc[2].column_index = 7;
    aggre_desc[2].type = AGGREGATE_TYPE_MAX;
    for (int i = 0; i < 3; ++i) {
      aggregate_desc.push_back(aggre_desc[i]);
    }
    list<SortDesc> group;
    struct SortDesc sd;
    sd.column_index = 0;
    sd.sort_order = ORDER_ASC;
    sd.is_cs = false;
    sd.ctype = CHARSET_TYPE_OTHER;
    sd.is_utf8mb4_bin = false;
    group.push_back(sd);
    ExecuteNode *send_node = plan->get_send_node();
    ExecuteNode *fetch_node;
    ExecuteNode *filter_node;
    ExecuteNode *single_node;
    ExecuteNode *group_by_node = plan->get_group_node(&group, aggregate_desc);
    ExecuteNode *avg_show_table_status_node =
        plan->get_avg_show_table_status_node();
    string no_simple_sql(sql, st.sql->show_tables_oper->simple_start_pos - 1);
    record_statement_stored_string(no_simple_sql);
    const char *no_simple_sql_str = get_last_statement_stored_string();

    for (; it != dataspaces.end(); ++it) {
      single_node = plan->get_single_sort_node(&group);
      filter_node =
          plan->get_regex_filter_node((it->first).second, 0, it->second);
      const char *used_sql = adjust_stmt_sql_for_shard_for_show_table_status(
          (it->first).first, no_simple_sql_str);
      fetch_node = plan->get_fetch_node((it->first).first, used_sql);
      filter_node->add_child(fetch_node);
      single_node->add_child(filter_node);
      group_by_node->add_child(single_node);
    }
    avg_show_table_status_node->add_child(group_by_node);
    send_node->add_child(avg_show_table_status_node);
    plan->set_start_node(send_node);
  }
}

const char *Statement::adjust_stmt_sql_for_shard_for_show_table_status(
    DataSpace *ds, const char *old_sql) {
  string new_sql_tmp(old_sql);
  const char *used_sql = old_sql;
  if (ds->get_virtual_machine_id() > 0) {
    const char *schema_name = st.sql->show_tables_oper->db_name
                                  ? st.sql->show_tables_oper->db_name
                                  : schema;
    string new_schema_tmp;
    adjust_shard_schema(schema_name, schema_name, new_schema_tmp,
                        ds->get_virtual_machine_id(), ds->get_partition_id());
    int start_pos = 0;
    int end_pos = 0;
    if (st.sql->show_tables_oper->db_start_pos) {
      start_pos = st.sql->show_tables_oper->db_start_pos;
      start_pos = new_sql_tmp.find(" ", start_pos);
      end_pos = st.sql->show_tables_oper->db_end_pos;
      string new_schema_tmp_with_space(" `");
      new_schema_tmp_with_space.append(new_schema_tmp);
      new_schema_tmp_with_space.append("` ");
      new_sql_tmp.replace(start_pos, end_pos - start_pos + 1,
                          new_schema_tmp_with_space);
    } else if (st.sql->show_tables_oper->table_pattern_start_pos) {
      start_pos = st.sql->show_tables_oper->table_pattern_start_pos - 2;
      string new_schema_tmp_with_space(" FROM `");
      new_schema_tmp_with_space.append(new_schema_tmp);
      new_schema_tmp_with_space.append("` ");
      new_sql_tmp.replace(start_pos, 1, new_schema_tmp_with_space);
    } else if (st.scanner->where_pos) {
      start_pos = st.scanner->where_pos - 2;
      string new_schema_tmp_with_space(" FROM `");
      new_schema_tmp_with_space.append(new_schema_tmp);
      new_schema_tmp_with_space.append("` ");
      new_sql_tmp.replace(start_pos, 1, new_schema_tmp_with_space);
    } else {
      new_sql_tmp.append(" FROM `");
      new_sql_tmp.append(new_schema_tmp);
      new_sql_tmp.append("`");
    }

    LOG_DEBUG("After replace shard schema, the sql is [%s]\n",
              new_sql_tmp.c_str());
    record_shard_sql_str(new_sql_tmp);
    used_sql = get_last_shard_sql();
  }
  return used_sql;
}

void Statement::assemble_show_warnings_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show warnings plan.\n");

  if (!(plan->session->get_load_warning_packets()->empty())) {
    // previous load data got warnings
    ExecuteNode *show_load_warning_node = plan->get_show_load_warning_node();
    plan->set_start_node(show_load_warning_node);
    return;
  }

  set<DataSpace *> warning_spaces = plan->session->get_warning_dataspaces();

  if (warning_spaces.empty()) {
    /*There is no kept warning space,
     *if session contains warnings, return warnings
     *else if session load_warning_packet_list is not empty, return those
     *warning packets else return an empty result.*/
    ExecuteNode *show_warning_node = plan->get_show_warning_node();
    plan->set_start_node(show_warning_node);
    return;
  }

  ExecuteNode *send_node = plan->get_send_node();
  set<DataSpace *>::iterator it = warning_spaces.begin();

  ExecuteNode *fetch_node = NULL;
  for (; it != warning_spaces.end(); ++it) {
    LOG_DEBUG("Use warnig space %s to assemble fetch node.\n",
              (*it)->get_name());
    fetch_node = plan->get_fetch_node(*it, sql);
    send_node->add_child(fetch_node);
  }

  plan->set_start_node(send_node);
}

void Statement::assemble_show_events_plan(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();
  const char *event_schema = schema;
  if (get_stmt_node()->sql->show_event_oper &&
      get_stmt_node()->sql->show_event_oper->schema) {
    event_schema = get_stmt_node()->sql->show_event_oper->schema;
  }
  dataspace = backend->get_data_space_for_table(event_schema, NULL);
  assemble_direct_exec_plan(plan, dataspace);
}

void Statement::assemble_show_databases_plan(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  DataSpace *dataspace = backend->get_auth_data_space();
#ifdef DEBUG
  ACE_ASSERT(dataspace != NULL);
#endif
  assemble_direct_exec_plan(plan, dataspace);
}
void Statement::authenticate_db(const char *dbname, ExecutePlan *plan,
                                stmt_type st_type) {
  string dbname_string = string(dbname);
  if ((st_type != STMT_CREATE_DB && st_type != STMT_DROP_DB) &&
      !plan->session->is_contain_schema(dbname_string)) {
    Backend::instance()->record_latest_important_dbscale_warning(
        "authenticate_db does not contain [%s] for user [%s]\n", dbname,
        plan->session->get_username());
    string err_msg = "Access denied for user [";
    err_msg.append(plan->session->get_username());
    err_msg.append("] to database [");
    err_msg.append(dbname);
    err_msg.append("], session auth schema does not contain target db");
    throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                 ERROR_AUTH_DENIED_CODE);
  }

  if (st_type == STMT_CHANGE_DB) {
    return;
  }
  SchemaACLType db_acl_type = plan->session->get_schema_acl_type(dbname);
  SchemaACLType global_acl_type =
      plan->session->get_schema_acl_type(DBSCALE_RESERVED_STR);
  vector<ACLType> acl_type_vec;
  acl_type_vec.push_back((ACLType)global_acl_type);
  acl_type_vec.push_back((ACLType)db_acl_type);
  if (!is_stmt_acl_check_ok(st_type, acl_type_vec)) {
    string err_msg = "Access denied for user [";
    err_msg.append(plan->session->get_user_name());
    err_msg.append("@");
    err_msg.append(plan->session->get_client_ip());
    err_msg.append("] to database [");
    err_msg.append(dbname);
    err_msg.append("]");
    Backend::instance()->record_latest_important_dbscale_warning(
        err_msg.c_str());
    throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                 ERROR_AUTH_DENIED_CODE);
  }
}

void Statement::authenticate_global_priv(ExecutePlan *plan, stmt_type st_type) {
  SchemaACLType global_acl_type =
      plan->session->get_schema_acl_type(DBSCALE_RESERVED_STR);
  vector<ACLType> acl_type_vec;
  acl_type_vec.push_back((ACLType)global_acl_type);
  if (!is_stmt_acl_check_ok(st_type, acl_type_vec, true)) {
    string err_msg = "Access denied for user [";
    err_msg.append(plan->session->get_user_name());
    err_msg.append("@");
    err_msg.append(plan->session->get_client_ip());
    err_msg.append("]");
    Backend::instance()->record_latest_important_dbscale_warning(
        err_msg.c_str());
    throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                 ERROR_AUTH_DENIED_CODE);
  }
}

void Statement::authenticate_table(ExecutePlan *plan, const char *schema_name,
                                   const char *table_name, stmt_type st_type,
                                   bool is_sub_select) {
  ACE_UNUSED_ARG(is_sub_select);
  SchemaACLType global_acl_type =
      plan->session->get_schema_acl_type(DBSCALE_RESERVED_STR);
  if ((st_type == STMT_SELECT || st_type == STMT_SUB_SELECT) &&
      !strcasecmp(schema_name, "information_schema")) {
    if (is_information_schema_table_can_always_select(table_name)) {
      return;
    }
    if (is_information_schema_table_select_priv_check_ok(global_acl_type)) {
      return;
    }
    string err_msg = "Access denied for user [";
    err_msg.append(plan->session->get_user_name());
    err_msg.append("@");
    err_msg.append(plan->session->get_client_ip());
    err_msg.append("] to table [information_schema.");
    err_msg.append(table_name);
    err_msg.append("]");
    Backend::instance()->record_latest_important_dbscale_warning(
        err_msg.c_str());
    throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                 ERROR_AUTH_DENIED_CODE);
  }
  if ((st_type == STMT_SELECT || st_type == STMT_SUB_SELECT) &&
      !strcasecmp(schema_name, "performance_schema") &&
      (!strcasecmp(table_name, "session_variables") ||
       !strcasecmp(table_name, "session_status") ||
       !strcasecmp(table_name, "session_account_connect_attrs") ||
       !strcasecmp(table_name, "global_status") ||
       !strcasecmp(table_name, "global_variables") ||
       !strcasecmp(table_name, "persisted_variables") ||
       !strcasecmp(table_name, "processlist") ||
       !strcasecmp(table_name, "variables_info"))) {
    return;
  }
  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);
  TableACLType tb_acl_type = plan->session->get_table_acl_type(full_table_name);
  SchemaACLType db_acl_type = plan->session->get_schema_acl_type(schema_name);
  vector<ACLType> acl_type_vec;
  acl_type_vec.push_back((ACLType)global_acl_type);
  acl_type_vec.push_back((ACLType)db_acl_type);
  acl_type_vec.push_back((ACLType)tb_acl_type);
  if (!is_stmt_acl_check_ok(st_type, acl_type_vec)) {
    string err_msg = "Access denied for user [";
    err_msg.append(plan->session->get_user_name());
    err_msg.append("@");
    err_msg.append(plan->session->get_client_ip());
    err_msg.append("] to table [");
    err_msg.append(full_table_name);
    err_msg.append("]");
    Backend::instance()->record_latest_important_dbscale_warning(
        err_msg.c_str());
    throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                 ERROR_AUTH_DENIED_CODE);
  }
}

void Statement::authenticate_statement(ExecutePlan *plan) {
  const char *db_name = NULL;
  stmt_type type = st.type;

  // check user authority
  if (is_stmt_mysql_acl_type_can_always_execute(type)) {
    return;
  }
  if (is_stmt_mysql_acl_type_not_clear(type)) {
    if (dbscale_acl_strict_mode == 0) {
      return;
    } else {
      string err_msg = "Access denied for user [";
      err_msg.append(plan->session->get_user_name());
      err_msg.append("@");
      err_msg.append(plan->session->get_client_ip());
      err_msg.append(
          "] to execute this type of statement, because option "
          "dbscale-acl-strict-mode > 0");
      Backend::instance()->record_latest_important_dbscale_warning(
          err_msg.c_str());
      throw dbscale::sql::SQLError(err_msg.c_str(), "42000",
                                   ERROR_AUTH_DENIED_CODE);
    }
  }
  if (is_dbscale_command(type)) {
    return;
  }

  switch (type) {
    case STMT_CREATE_DB: {
      db_name = st.sql->create_db_oper->dbname->name;
      authenticate_db(db_name, plan, STMT_CREATE_DB);
    } break;
    case STMT_DROP_DB: {
      db_name = st.sql->drop_db_oper->dbname->name;
      authenticate_db(db_name, plan, STMT_DROP_DB);
    } break;
    case STMT_ALTER_DB: {
      db_name = st.sql->alter_db_oper->db_name;
      authenticate_db(db_name, plan, STMT_ALTER_DB);
    } break;
    case STMT_CHANGE_DB: {
      db_name = st.sql->change_db_oper->dbname->name;
      authenticate_db(db_name, plan, STMT_CHANGE_DB);
    } break;
    case STMT_REPLACE_SELECT:
    case STMT_INSERT_SELECT:
    case STMT_CREATE_SELECT:
    case STMT_CREATE_LIKE: {
      table_link *table = st.table_list_head;
      if (table) {
        const char *schema_name =
            table->join->schema_name ? table->join->schema_name : schema;
        const char *table_name = table->join->table_name;
        if (type == STMT_CREATE_SELECT || type == STMT_CREATE_LIKE) {
          authenticate_table(plan, schema_name, table_name, STMT_CREATE_TB,
                             false);
        } else {
          authenticate_table(plan, schema_name, table_name, STMT_INSERT, false);
        }
        table = table->next;
        if (table) {
          schema_name =
              table->join->schema_name ? table->join->schema_name : schema;
          table_name = table->join->table_name;
          authenticate_table(plan, schema_name, table_name, STMT_SELECT, true);
        }
      }
    } break;
    default: {
      table_link *table = st.table_list_head;
      if (table) {
        while (table) {
          const char *schema_name =
              table->join->schema_name ? table->join->schema_name : schema;
          const char *table_name = table->join->table_name;
          bool is_sub_select = table->join->cur_rec_scan->upper ? true : false;
          if (st.type == STMT_ALTER_TABLE &&
              st.sql->alter_tb_oper->alter_type == ADD_PARTITION)
            st.type = STMT_ALTER_ADD_PARTITION;
          else if (st.type == STMT_ALTER_TABLE &&
                   st.sql->alter_tb_oper->alter_type == DROP_PARTITION)
            st.type = STMT_ALTER_DROP_PARTITION;
          authenticate_table(plan, schema_name, table_name, st.type,
                             is_sub_select);
          if (st.type == STMT_ALTER_ADD_PARTITION ||
              st.type == STMT_ALTER_DROP_PARTITION)
            st.type = STMT_ALTER_TABLE;
          table = table->next;
        }
      } else {
        try {
          authenticate_db(schema, plan, st.type);
        } catch (dbscale::sql::SQLError &e) {
          authenticate_global_priv(plan, st.type);
        }
      }
    } break;
  }
}
void Statement::deal_lock_table(ExecutePlan *plan) {
  stmt_type type = st.type;
  switch (type) {
    case STMT_LOCK_TB:
    case STMT_SHOW_CREATE_TABLE:
    case STMT_SHOW_INDEX:
    case STMT_TABLE_MAINTENANCE:
    case STMT_SHOW_FIELDS:
    case STMT_DBSCALE_SHOW_PARTITIONS:
      return;
    case STMT_CREATE_DB:
    case STMT_DROP_DB:
    case STMT_CREATE_VIEW:
    case STMT_RENAME_TABLE:
      throw dbscale::sql::SQLError(
          "Can't execute the given command because "
          "you have active locked tables or an active transaction",
          "HY000", ERROR_NOT_EXECUTE_FOR_LOCK_CODE);
      break;
    default: {
      if (!st.table_list_head) break;
      table_link *link = st.table_list_head;
      join_node *node = NULL;
      plan->session->reset_lock_table_state();
      while (link) {
        node = link->join;
        if ((node->schema_name &&
             strcasecmp(node->schema_name, "INFORMATION_SCHEMA") == 0) ||
            (strlen(schema) > 0 &&
             strcasecmp(schema, "INFORMATION_SCHEMA") == 0)) {
          link = link->next;
          continue;
        }
        if (!plan->session->contains_lock_table(
                node->table_name, node->schema_name, node->alias)) {
          string err_msg = "Table '";
          err_msg += node->alias ? node->alias : node->table_name;
          err_msg += "' was not locked with LOCK TABLES";
          throw dbscale::sql::SQLError(err_msg.c_str(), "HY000",
                                       ERROR_NOT_LOCK_CODE);
        }
        link = link->next;
      }
      break;
    }
  }
}

void Statement::validate_into_outfile() {
  into_outfile_item *into_outfile = st.into_outfile;
  // for filename and the 5 fileds and lines separator,
  // the string format should be "abc" or 'abc',
  // so, here can use the first char to judge it
  char filename_c = sql[into_outfile->filename_pos - 1];
  if (filename_c != '\'' && filename_c != '\"') {
    throw NotSupportedError("Error format of filename.");
  }

  if (strlen(into_outfile->fields_enclosed) > 1 ||
      strlen(into_outfile->fields_escaped) > 1) {
    throw dbscale::sql::SQLError(
        "Field separator argument is not what is expected; "
        "check the manual",
        "42000", 1083);
  }

  if (into_outfile->fields_term_len == 0) {
    LOG_ERROR(
        "Fields separator argument of FIELDS TERMINATED BY can not be ''.\n");
    throw NotSupportedError(
        "Fields separator argument of FIELDS TERMINATED BY can not be ''.");
  }

  bool has_null = is_terminator_has_null(into_outfile->fields_term,
                                         into_outfile->fields_term_len) ||
                  is_terminator_has_null(into_outfile->fields_escaped,
                                         into_outfile->fields_escaped_len) ||
                  is_terminator_has_null(into_outfile->fields_enclosed,
                                         into_outfile->fields_enclosed_len) ||
                  is_terminator_has_null(into_outfile->lines_start,
                                         into_outfile->lines_start_len) ||
                  is_terminator_has_null(into_outfile->lines_term,
                                         into_outfile->lines_term_len);
  if (has_null) {
    LOG_ERROR("Fields separator argument can not contain '\\0'.\n");
    throw NotSupportedError("Fields separator argument can not contain '\\0'.");
  }

  if (into_outfile->is_local) {
    // in local mode, the last char of LINES TERMINATED BY should be '\n'
    if (into_outfile->lines_term[into_outfile->lines_term_len - 1] != '\n') {
      LOG_ERROR("The last character of INTO OUTFILE LOCAL should be '\\n'.\n");
      const char *err_msg =
          "The last character of INTO OUTFILE LOCAL should be '\\n'";
      throw Error(err_msg);
    }
  } else {
    // in non-local mode, it will write file on server side, it should check
    // file exists
    const char *filename = into_outfile->filename;
    fstream file(filename);
    if (file) {
      LOG_ERROR("into outfile filename already exist.\n");
      string err_msg = "File '";
      err_msg += filename;
      err_msg += "' already exists";
      throw dbscale::sql::SQLError(err_msg.c_str(), "HY000", ERROR_FILE_EXIST);
    }
    // check file can write
    ofstream file1;
    file1.open(filename);
    if (!file1.good()) {
      file1.close();
      LOG_ERROR("Failed to write to file for SELECT INTO OUTFILE.\n");
      string err_msg("Can't create/write to file '");
      err_msg += filename;
      err_msg += "'";
      throw dbscale::sql::SQLError(err_msg.c_str(), "HY000", 1);
    }
    file1.close();
  }
}

void Statement::handle_no_par_table_one_spaces(ExecutePlan *plan) {
  DataSpace *dataspace =
      one_table_node.only_one_table ? one_table_node.space : spaces[0];
  Backend *backend = Backend::instance();
  if (st.type == STMT_SELECT && st.has_where_rownum) replace_rownum_add_limit();
#ifdef DEBUG
  LOG_DEBUG("handle no partition table one space, the space name [%s]\n",
            dataspace->get_name());
#endif
  switch (st.type) {
    case STMT_LOAD: {
      DataServer *server =
          get_write_server_of_source(dataspace->get_data_source());
      if (server->get_is_external_load()) {
        if (st.sql->load_oper->local) {
          assemble_load_local_external_plan(plan, dataspace, server);
        } else {
          assemble_load_data_infile_external_plan(plan, dataspace, server);
        }
      } else {
        if (st.sql->load_oper->local) {
          assemble_load_local_plan(plan, dataspace);
        } else {
          assemble_load_data_infile_plan(plan, dataspace);
        }
      }
      break;
    }
    case STMT_DBSCALE_SHOW_PARTITIONS: {
      assemble_dbscale_show_partition_plan(plan);
      break;
    }
    case STMT_UPDATE:
    case STMT_DELETE:
    case STMT_REPLACE_SELECT:
    case STMT_INSERT_SELECT: {
      const char *schema_name, *table_name;
      DataSpace *modify_space, *select_space = dataspace;
      table_link *modify_table = get_one_modify_table();
      schema_name = modify_table->join->schema_name
                        ? modify_table->join->schema_name
                        : schema;
      table_name = modify_table->join->table_name;
      modify_space = backend->get_data_space_for_table(schema_name, table_name);

      if (!modify_space->get_data_source()) {
        /* If the partition table only has one partition, it will be treated
         * as a normal table. But here the dataspace should be adjusted to the
         * partition, rather than partition table.*/
        PartitionedTable *tmp_p = (PartitionedTable *)modify_space;
        if (tmp_p->get_real_partition_num() != 1) {
          Backend::instance()->record_latest_important_dbscale_warning(
              "Meet a mul part partition table in "
              "handle_no_par-table_one_spaces.\n");
#ifdef DEBUG
          ACE_ASSERT(0);
#endif
        }
        modify_space = tmp_p->get_partition(0);
      }

      DataServer *modify_server =
          get_write_server_of_source(modify_space->get_data_source());
      if (st.type == STMT_INSERT_SELECT && modify_server &&
          modify_server->get_is_external_load()) {
        prepare_insert_select_via_fifo(plan, modify_server, schema_name,
                                       table_name, false);
        const char *used_sql =
            adjust_stmt_sql_for_shard(dataspace, select_sub_sql.c_str());
        ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
        ExecuteNode *send_node = plan->get_into_outfile_node();

        send_node->add_child(fetch_node);
        plan->set_start_node(send_node);
        break;
      }
      int load_insert =
          plan->session->get_session_option("use_load_data_for_insert_select")
              .int_val;
      if (st.type == STMT_INSERT_SELECT && load_insert == FIFO_INSERT_SELECT &&
          !plan->statement->is_union_table_sub() &&
          !plan->statement->is_cross_node_join() &&
          !plan->session->is_in_explain()) {
        prepare_insert_select_via_fifo(plan, modify_server, schema_name,
                                       table_name, true);
        const char *used_sql =
            adjust_stmt_sql_for_shard(dataspace, select_sub_sql.c_str());
        ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
        ExecuteNode *send_node = plan->get_into_outfile_node();

        send_node->add_child(fetch_node);
        plan->set_start_node(send_node);
        break;
      } else if (st.type == STMT_INSERT_SELECT &&
                 load_insert == LOAD_INSERT_SELECT &&
                 !plan->statement->is_union_table_sub() &&
                 !plan->statement->is_cross_node_join() &&
                 !plan->session->is_in_explain()) {
        vector<ExecuteNode *> nodes;
        LOG_DEBUG(
            "Assemble INSERT SELECT from partition table plan via LOAD.\n");
        prepare_insert_select_via_load(plan, schema_name, table_name);
        const char *used_sql =
            adjust_stmt_sql_for_shard(dataspace, select_sub_sql.c_str());
        ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, used_sql);
        ExecuteNode *load_node =
            plan->get_load_select_node(schema_name, table_name);
        load_node->add_child(fetch_node);
        plan->set_start_node(load_node);
        break;
      }

      if (modify_space != select_space) {
        /*The modify_space is a parent space of dataspace. In this case we
         * need to divide this sql into two sqls.*/
        assemble_two_phase_modify_no_partition_plan(plan, modify_space,
                                                    select_space, modify_table);
      } else {
        if (enable_last_insert_id) {
          vector<join_node *> tables;
          get_all_join_table(st.scanner->join_tables, &tables);
          for (auto table : tables) {
            schema_name = table->schema_name ? table->schema_name : schema;
            table_name = table->table_name;
            modify_space->set_auto_increment_info(
                plan->handler, this, schema_name, table_name, false, false);
          }
        }
        assemble_direct_exec_plan(plan, dataspace);
      }
      break;
    }

    case STMT_DBSCALE_ESTIMATE: {
      assemble_dbscale_estimate_select_plan(plan, dataspace, this);
    } break;
    case STMT_SHOW_INDEX: {
      DataSpace *meta = Backend::instance()->get_metadata_data_space();
      if (!meta) {
        throw Error("fail to get metadata dataspace");
      }
      assemble_direct_exec_plan(plan, meta);
      break;
    }
    case STMT_SHOW_FIELDS:
    case STMT_SHOW_CREATE_TABLE: {
      DataSpace *dspace = dataspace;
      DataSpace *meta = Backend::instance()->get_metadata_data_space();
      if (meta) dspace = meta;
      table_link *table = st.table_list_head;
      const char *schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      const char *table_name = table->join->table_name;
      if (plan->session->is_temp_table(schema_name, table_name)) {
        dspace = backend->get_data_space_for_table(schema_name, table_name);
      }
      assemble_direct_exec_plan(plan, dspace);
      break;
    }
    case STMT_TRUNCATE: {
      table_link *table = st.table_list_head;
      while (table) {
        const char *schema_name =
            table->join->schema_name ? table->join->schema_name : schema;
        if (dbscale_safe_sql_mode > 0 &&
            Backend::instance()->is_system_schema(schema_name)) {
          throw Error(
              "Refuse truncate table of system schema for "
              "dbscale_safe_sql_mode>0");
        }

        table = table->next;
      }
      if (enable_table_recycle_bin) {
        if (st.table_list_num > 1)
          throw NotSupportedError(
              "when using enable_table_recycle_bin, tables should be truncated "
              "one by one.");
        else
          assemble_move_table_to_recycle_bin_plan(plan);
      } else
        assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_CREATE_TB: {
      table_link *table = st.table_list_head;
      const char *schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      if (!Backend::instance()->is_centralized_cluster())
        create_tb_sql_with_create_db_if_not_exists =
            Backend::instance()->fetch_create_db_sql_from_metadata(
                plan->session, schema_name, true, NULL);
      else
        create_tb_sql_with_create_db_if_not_exists =
            "SET @_DBSCALE_RESERVED_VAR=1";  // not run create database if not
                                             // exists
      if (!is_cross_node_join())
        plan->session->reset_table_without_given_schema();
      int has_use = 0;
      if (plan->session->get_schema() &&
          strcmp(plan->session->get_schema(), "")) {
        if (!lower_case_compare(plan->session->get_schema(), schema_name)) {
          /*Do the use schema only for the current schema is the created
           * schema.*/
          create_tb_sql_with_create_db_if_not_exists.append(";USE `");
          create_tb_sql_with_create_db_if_not_exists.append(
              plan->session->get_schema());
          create_tb_sql_with_create_db_if_not_exists.append("`");
          has_use = 1;
        }
      } else {
        create_tb_sql_with_create_db_if_not_exists.append(";USE `");
        create_tb_sql_with_create_db_if_not_exists.append(DEFAULT_LOGIN_SCHEMA);
        create_tb_sql_with_create_db_if_not_exists.append("`");
        has_use = 1;
      }
      create_tb_sql_with_create_db_if_not_exists.append(";");
      create_tb_sql_with_create_db_if_not_exists.append(sql);
      map<DataSpace *, int> sql_count_map;
      map<DataSpace *, const char *> spaces_map;
      sql_count_map[dataspace] = 2 + has_use;
      spaces_map[dataspace] =
          create_tb_sql_with_create_db_if_not_exists.c_str();
      if (Backend::instance()->need_deal_with_metadata(st.type, &st))
        st.need_apply_metadata = judge_need_apply_meta_datasource(spaces_map);
      ExecuteNode *node =
          plan->get_mul_modify_node(spaces_map, true, &sql_count_map);
      plan->set_start_node(node);
    } break;
    case STMT_INSERT:
    case STMT_REPLACE: {
      int enable_last_insert_id_session =
          plan->session->get_session_option("enable_last_insert_id").int_val;
      if (enable_last_insert_id_session) {
        insert_op_node *insert = NULL;
        if (plan->session->is_call_store_procedure()) {
          insert = st.sql->insert_oper;
        } else {
          insert = get_latest_stmt_node()->sql->insert_oper;
        }
        // we do not handle last insert id with insert set
        if (!insert->assign_list) {
          table_link *table = st.table_list_head;
          const char *schema_name =
              table->join->schema_name ? table->join->schema_name : schema;
          const char *table_name = table->join->table_name;
          int alter_table = dataspace->set_auto_increment_info(
              plan->handler, this, schema_name, table_name, false, false);
          if (alter_table == 1) {
            Driver *driver = Driver::get_driver();
            driver->acquire_session_mutex();
            set<Session *, compare_session> *session_set =
                driver->get_session_set();
            set<Session *, compare_session>::iterator it = session_set->begin();
            for (; it != session_set->end(); ++it) {
              (*it)->reset_auto_increment_info(full_table_name);
            }
            driver->release_session_mutex();
          }
          if (auto_inc_status != NO_AUTO_INC_FIELD) {
            // we do not handle last insert id with insert set
            string full_table_name;
            splice_full_table_name(schema_name, table_name, full_table_name);
            TableInfoCollection *tic = TableInfoCollection::instance();
            {
              string auto_inc_name;
              TableInfo *ti = tic->get_table_info_for_read(full_table_name);
              try {
                auto_inc_name =
                    ti->element_table_auto_inc_name->get_element_string(
                        get_session());
                int auto_inc_pos =
                    ti->element_table_auto_inc_pos->get_element_int64_t(
                        get_session());
                auto_inc_key_name = auto_inc_name;
                // we check from 0
                auto_increment_key_pos = auto_inc_pos - 1;
              } catch (...) {
                LOG_ERROR(
                    "Error occured when try to get table info table auto inc "
                    "name(string) of table [%s.%s]\n",
                    schema_name, table_name);
              }
              ti->release_table_info_lock();
            }
          }
        }
      }
    }
    default:
      assemble_direct_exec_plan(plan, dataspace);
  }
}

inline bool check_xa_transaction_support_sql(stmt_node *st, Session *s) {
  // savepoint and rollback_to stmt is not support for xa transaction
  if (enable_xa_transaction &&
      s->get_session_option("close_session_xa").int_val == 0) {
    stmt_type type = st->type;
    if (type == STMT_SAVEPOINT) return false;
    if (type == STMT_ROLLBACK) {
      RollbackType rollback_type = st->sql->rollback_point_oper->type;
      if (rollback_type == ROLLBACK_TYPE_TO_POINT) return false;
    }
  }
  return true;
}

void Statement::assemble_rename_normal_table(ExecutePlan *plan,
                                             DataSpace *dataspace) {
  LOG_DEBUG("Assemble rename normal table plan.\n");
  assemble_direct_exec_plan(plan, dataspace);
}

void Statement::assemble_rename_partitioned_table(
    ExecutePlan *plan, PartitionedTable *old_table, PartitionedTable *new_table,
    const char *old_schema_name, const char *old_table_name,
    const char *new_schema_name, const char *new_table_name) {
  LOG_DEBUG("Assemble rename partitioned partition plan.\n");

  unsigned int partition_num = old_table->get_real_partition_num();

  ExecuteNode *rename_table_node = plan->get_rename_table_node(
      old_table, new_table, old_schema_name, old_table_name, new_schema_name,
      new_table_name);
  ExecuteNode *ok_merge_node = plan->get_ok_merge_node();
  ExecuteNode *modify_node = NULL;
  DataSpace *space = NULL;
  rename_table_node->add_child(ok_merge_node);

  DataSpace *meta_datasource = Backend::instance()->get_metadata_data_space();
  bool need_apply_meta = true;
  for (unsigned int i = 0; i != partition_num; ++i) {
    space = old_table->get_partition(i);
    if (is_share_same_server(meta_datasource, space)) {
      need_apply_meta = false;
    }
    modify_node = plan->get_modify_node(space, sql, true);
    ok_merge_node->add_child(modify_node);
  }

  st.need_apply_metadata = need_apply_meta;
  plan->session->add_table_info_to_delete(old_schema_name, old_table_name);
  plan->set_start_node(rename_table_node);
}

bool Statement::has_same_partitioned_key(PartitionedTable *old_part_table,
                                         PartitionedTable *new_part_table) {
  vector<const char *> *old_key_names = old_part_table->get_key_names();
  vector<const char *> *new_key_names = new_part_table->get_key_names();
  if (old_key_names->size() != new_key_names->size()) {
    return false;
  }
  // if partition-key is not the same, then the data will distribute
  // in diff data-source, so forbid it
  unsigned int key_size = old_key_names->size();
  for (unsigned int i = 0; i != key_size; ++i) {
    const char *old_key = old_key_names->at(i);
    const char *new_key = new_key_names->at(i);
    if (!strcasecmp(old_key, new_key)) {
      return false;
    }
  }

  return true;
}

void Statement::handle_rename_table(ExecutePlan *plan,
                                    rename_table_node_list *head) {
  Backend *backend = Backend::instance();

  // get the old table
  join_node *old_node = head->old_table;
  const char *old_schema_name =
      old_node->schema_name ? old_node->schema_name : schema;
  const char *old_table_name = old_node->table_name;
  DataSpace *old_table =
      backend->get_data_space_for_table(old_schema_name, old_table_name);
  DataSource *old_source = old_table->get_data_source();

  // get the new table
  join_node *new_node = head->new_table;
  const char *new_schema_name =
      new_node->schema_name ? new_node->schema_name : schema;
  const char *new_table_name = new_node->table_name;
  DataSpace *new_table =
      backend->get_data_space_for_table(new_schema_name, new_table_name);
  DataSource *new_source = new_table->get_data_source();

  // both are normal tables
  if (old_source != NULL && new_source != NULL) {
    if (old_source != new_source) {
      LOG_ERROR(
          "DBScale do not support to rename normal table "
          "with different data-source.");
      throw NotSupportedError(
          "Not supported rename normal table with different data-source.");
    }
    assemble_rename_normal_table(plan, old_table);
    return;
  }

  // one is normal table, other is partitioned table
  if ((old_source != NULL && new_source == NULL) ||
      (old_source == NULL && new_source != NULL)) {
    LOG_ERROR(
        "DBScale do not support to rename table "
        "between normal table and partitioned table.");
    throw NotSupportedError(
        "Not supported rename table between normal table and partitioned "
        "table.");
  }

  // both are partitioned table
  PartitionedTable *old_part_table = (PartitionedTable *)old_table;
  PartitionScheme *old_scheme = old_part_table->get_partition_scheme();
  PartitionedTable *new_part_table = (PartitionedTable *)new_table;
  PartitionScheme *new_scheme = new_part_table->get_partition_scheme();
  if (old_scheme != new_scheme) {
    LOG_ERROR(
        "DBScale is not support rename partitioned table"
        " with different partition-scheme.");
    throw NotSupportedError(
        "Not supported rename partitioned table with different "
        "partition-scheme.");
  }
  if (has_same_partitioned_key(old_part_table, new_part_table)) {
    LOG_ERROR(
        "DBScale do not support to rename partitioned table "
        "with different partition-key.");
    throw NotSupportedError(
        "Not supported rename partitioned table with different partition-key.");
  }
  if (old_part_table->get_partition_scheme()->is_shard() !=
      new_part_table->get_partition_scheme()->is_shard()) {
    LOG_ERROR(
        "DBScale do not support to rename shard partitioned table "
        "to non-shard, and also not support non-shard to shard.");
    throw NotSupportedError(
        "Not supported rename shard partitioned table to non-shard, and also "
        "not support non-shard to shard.");
  }
  if (old_part_table->get_partition_scheme()->is_shard() &&
      lower_case_compare(old_schema_name, new_schema_name)) {
    LOG_ERROR(
        "DBScale do not support to rename shard partitioned table "
        "to different schema.");
    throw NotSupportedError(
        "Not supported rename shard partitioned table to different schema.");
  }
  assemble_rename_partitioned_table(plan, old_part_table, new_part_table,
                                    old_schema_name, old_table_name,
                                    new_schema_name, new_table_name);
}

void Statement::assemble_set_plan(ExecutePlan *plan, DataSpace *dataspace) {
  LOG_DEBUG("Assemble set plan.\n");
  if (!dataspace)
    dataspace = Backend::instance()->get_data_space_for_table(schema, NULL);
  ExecuteNode *node = NULL;
  if (has_ignore_session_var) {
    if (!session_var_sql.length()) {
      /*All session vars are ignored or skipped. So just return OK packet*/
      node = plan->get_return_ok_node();
    } else
      node = plan->get_set_node(dataspace, session_var_sql.c_str());
  } else
    node = plan->get_set_node(dataspace, sql);
  plan->set_start_node(node);
}

const char *Statement::get_value_from_bool_expression(
    const char *column_name, BoolBinaryCaculateExpression *expr) {
  CompareExpression *expr_left = dynamic_cast<CompareExpression *>(expr->left);
  CompareExpression *expr_right =
      dynamic_cast<CompareExpression *>(expr->right);
  const char *tmp_str = NULL;
  if ((!expr_left || expr_left->type != EXPR_EQ) &&
      (!expr_right || expr_right->type != EXPR_EQ))
    return NULL;
  if (expr_left != NULL) {
    tmp_str = get_value_from_compare_expression(column_name, expr_left);
  }
  if (tmp_str == NULL && expr_right != NULL) {
    tmp_str = get_value_from_compare_expression(column_name, expr_right);
  }
  return tmp_str;
}
const char *Statement::get_value_from_compare_expression(
    const char *column_name, CompareExpression *expr) {
  StrExpression *left = dynamic_cast<StrExpression *>(expr->left);
  if (left == NULL) return NULL;
  string lower_str_value(left->str_value);
  if (boost::algorithm::to_lower_copy(lower_str_value) == column_name) {
    StrExpression *right = dynamic_cast<StrExpression *>(expr->right);
    if (right == NULL) return NULL;
    return right->str_value;
  }
  return NULL;
}

// return false if need block request
bool Statement::check_migrate_block_table(ExecutePlan *plan) {
  Backend *backend = Backend::instance();
  if (backend->need_slow_down()) {
    // TODO: only slow down the request, which will be sent to the source server
    timespec_t t =
        (timespec_t)ACE_Time_Value(0, slow_down_request_time);  // sleep 5 ms
    ACE_OS::nanosleep(&t);
  }

  // TODO: deal with partial parse sql.
  if (!st.table_list_head)  // statemet does't contains table
    return true;
  if (st.type == STMT_DBSCALE_BLOCK) return true;
  table_link *link = st.table_list_head;
  join_node *node = NULL;

  if (!backend->has_migrate_block_table()) return true;

  while (link) {
    node = link->join;
    const char *table_name = node->table_name;
    const char *alias_name = node->alias;
    const char *schema_name =
        node->schema_name ? node->schema_name : plan->session->get_schema();
    // if the handler is using the table_name, we Should wait until transaction
    // or lock end.
    if (plan->session->is_table_in_using(schema_name, table_name)) {
      link = link->next;
      continue;
    }
    MigrateBlockType block_type =
        backend->get_table_block_type(schema_name, table_name);
    if (block_type == BLOCK_ALL_TYPE)
      return false;
    else if (block_type == BLOCK_PARTITION_TYPE) {
      if (st.type == STMT_DBSCALE_MIGRATE ||
          (st.type > STMT_DDL_START && st.type < STMT_DDL_END) ||
          st.type == STMT_LOAD) {
        LOG_DEBUG(
            "the table is in partition block state, not support "
            "migrate/load/ddl operation on table.\n");
        return false;
      }
      DataSpace *space =
          backend->get_data_space_for_table(schema_name, table_name);
      if (space->get_dataspace_type() != TABLE_TYPE) {
        return true;
      } else if (!((Table *)space)->is_partitioned() || space->is_shard()) {
        // no need block normal table for migrate partition
        // now shard table not support migrate virtual partition.
        // migrate shard partition no need add mutex for BLOCK_PARTITION_TYPE
        return true;
      }

      switch (st.type) {
        case STMT_INSERT:
          break;
        case STMT_SELECT:
        case STMT_UPDATE:
        case STMT_DELETE: {
          const char *key_name =
              ((PartitionedTable *)space)->get_key_names()->at(0);
          // TODO:optimize the invoke of fullfil_par_key_equality, if it is
          // invoked here, reuse the result for the following processing.
          record_scan_par_key_equality.clear();
          record_scan_par_key_values.clear();
          fullfil_par_key_equality(st.cur_rec_scan, schema_name, table_name,
                                   alias_name, key_name);
          if (record_scan_par_key_values[st.cur_rec_scan][key_name].size() != 1)
            return false;
        } break;
        default:
          return false;
      }
    }
    link = link->next;
  }
  return true;
}

void Statement::check_and_replace_view(Session *session, table_link **table) {
  table_link *tmp_table = *table;
  map<unsigned int, pair<unsigned int, string> > view_sqls;
  map<unsigned int, string> view_alias;
  string sub_sql;
  string full_view;
  const char *view_schema = NULL;
  const char *view_name = NULL;
  bool view_is_on_top_rs = false;

  while (tmp_table) {
    join_node *join = tmp_table->join;
    view_schema = join->schema_name ? join->schema_name : schema;
    view_name = join->table_name;
    full_view.assign(view_schema);
    full_view.append(".");
    full_view.append(view_name);
    sub_sql.clear();
    session->get_one_view(full_view, sub_sql);
    if (!sub_sql.empty()) {
      view_sqls[tmp_table->start_pos] = make_pair(tmp_table->end_pos, sub_sql);
      if (!join->alias) {
        view_alias[tmp_table->start_pos] = string(view_name);
      }
      if (!view_is_on_top_rs && tmp_table->join->cur_rec_scan == st.scanner) {
        view_is_on_top_rs = true;
      }
    }
    tmp_table = tmp_table->next;
  }

  if (!view_sqls.empty()) {
    stmt_type check_type = st.type;
    if (st.type == STMT_DBSCALE_EXPLAIN || st.type == STMT_EXPLAIN) {
      if (st.sql->explain_oper) check_type = st.sql->explain_oper->explain_type;
    }
    if ((check_type == STMT_INSERT_SELECT || check_type == STMT_UPDATE ||
         check_type == STMT_DELETE) &&
        !view_is_on_top_rs) {
      // view in the select part of insert...select or update, which is ok
    } else if (check_type != STMT_SELECT) {
      LOG_ERROR("Only support do select on view now.\n");
      throw NotSupportedError("Only support do select on view.");
    }
    contains_view = true;
    string ori_sql(sql);
    view_sql.clear();
    unsigned int pos = 0;
    map<unsigned int, pair<unsigned int, string> >::iterator it =
        view_sqls.begin();
    for (; it != view_sqls.end(); it++) {
      view_sql.append(ori_sql, pos, it->first - pos - 1);
      view_sql.append("(");
      view_sql.append(it->second.second);
      view_sql.append(")");
      if (view_alias.count(it->first)) {
        view_sql.append(" AS ");
        view_sql.append(view_alias[it->first]);
      }
      pos = it->second.first;
    }
    view_sql.append(ori_sql, pos, ori_sql.length() - pos);
    sql = view_sql.c_str();
    Statement *new_stmt = NULL;
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    *table = new_stmt->get_stmt_node()->table_list_head;
    st.handled_sql = new_stmt->get_stmt_node()->handled_sql;
    LOG_DEBUG("Query sql after replace view is [%s]=[%@]\n", sql, this);
    check_and_replace_view(session, table);
  }
}

bool Statement::child_rs_has_join_tables(record_scan *root) {
  record_scan *tmp = root->children_begin;
  if (tmp) {
    if (tmp->join_tables) return true;
    for (; tmp; tmp = tmp->next) {
      if (child_rs_has_join_tables(tmp)) return true;
    }
  }
  return false;
}

bool Statement::can_use_one_table_plan(ExecutePlan *plan, DataSpace *space,
                                       stmt_node *st) {
  if (space->get_data_source()) return true;
  /*For partition table, should not has subquery with table link.
   *subquery no table_link means sql like INSERT INTO t1 SELECT 1,2,3 .*/
  if (child_rs_has_join_tables(st->scanner)) return false;
  /* For partition table, if the select stmt contains invalid function, we
   * should return false and use analysis record scan. */
  if (st->type == STMT_SELECT &&
      (check_field_list_contains_invalid_function(st->scanner) ||
       check_field_list_invalid_distinct(st->scanner)))
    return false;
  /*For partition table, the update/delete with limit can not be executed with
   * one table plan.*/
  if (st->type != STMT_UPDATE && st->type != STMT_DELETE) return true;
  if (st->scanner->limit) {
    IntExpression *limit_offset = st->scanner->limit->offset;
    IntExpression *limit_num = st->scanner->limit->num;
    long num = 0;
    if (limit_num) num = get_integer_value_from_expr(limit_num);
    if (!limit_offset && (num == 1) &&
        (st->type == STMT_UPDATE || st->type == STMT_DELETE) &&
        plan->session->get_session_option("update_delete_quick_limit")
            .int_val) {
      if (plan->session->is_in_transaction()) {
        stmt_with_limit_using_quick_limit = true;
        return true;
      } else {
        throw Error(
            "when enable 'update_delete_quick_limit', update/delete with limit "
            "1 should be executed in transaction");
      }
    } else {
      return false;
    }
  }
  return true;
}

bool Statement::get_explain_result_one_source(
    list<list<string> > *explain_result, Handler *handler, DataSpace *dataspace,
    Statement *stmt, string *sql, unsigned int row_id, string *exec_node_name,
    bool is_server_explain_stmt_always_extended) {
  bool got_error = false;
  ExecutePlan *plan = (ExecutePlan *)(handler->get_execute_plan(stmt));
  ExecuteNode *fetch_node = plan->get_fetch_node(dataspace, sql->c_str());
  ExecuteNode *fetch_explain_result_node = plan->get_fetch_explain_result_node(
      explain_result, is_server_explain_stmt_always_extended);
  fetch_explain_result_node->add_child(fetch_node);
  plan->set_start_node(fetch_explain_result_node);
  try {
    plan->execute();
  } catch (...) {
    got_error = true;
  }
  delete plan;
  plan = NULL;

  if (got_error || explain_result->empty()) {
    LOG_ERROR("explain got unexpected result.\n");
    return false;
  }

  list<list<string> >::iterator it = explain_result->begin();
  (*it).push_front(dataspace->get_data_source()->get_name());
  if (row_id == 0) {
    (*it).push_front(*exec_node_name);
  } else {
    (*it).push_front("");
  }
  ++it;
  for (; it != explain_result->end(); ++it) {
    (*it).push_front("");
    (*it).push_front("");
  }
  return true;
}

void Statement::assemble_explain_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble explain plan.\n");
  bool is_server_explain_stmt_always_extended =
      Backend::instance()->get_is_server_explain_stmt_always_extended();
  Session *session = plan->session;
  list<ExplainElement *>::iterator it =
      session->get_explain_element_list()->begin();
  int sub_plan_total_count = (*it)->get_sub_plan_id();

  bool got_error = false;
  string error_msg;
  for (; it != session->get_explain_element_list()->end(); ++it) {
    if (got_error) break;
    ExplainElement *ei = *it;

    string exec_node_name_with_depth = "";
    if (ei->is_a_start_node()) {
      exec_node_name_with_depth.append(
          ei->get_depth() + (sub_plan_total_count - ei->get_sub_plan_id()),
          '*');
    } else {
      exec_node_name_with_depth.append(
          ei->get_depth() + (sub_plan_total_count - ei->get_sub_plan_id()),
          '-');
    }
    exec_node_name_with_depth.append(ei->get_ori_node_name());

    DataSpace *ds = ei->get_dataspace();
    if (ds) {
      string sql = string("EXPLAIN ") + ei->get_sql();
      Parser *parser = Driver::get_driver()->get_parser();
      Statement *tmp_stmt = parser->parse(sql.c_str(), stmt_allow_dot_in_ident,
                                          true, NULL, NULL, NULL, ctype);
      try {
        tmp_stmt->check_and_refuse_partial_parse();
      } catch (...) {
        tmp_stmt->free_resource();
        delete tmp_stmt;
        tmp_stmt = NULL;
        got_error = true;
        error_msg = ei->get_sql();
        break;
      }
      tmp_stmt->set_default_schema(schema);

      // FIXME: fetch explain info paralleled.
      if (!ds->get_data_source()) {  // partitioned table
        PartitionedTable *pt = (PartitionedTable *)ds;
        unsigned int partition_num = pt->get_real_partition_num();
        for (unsigned int i = 0; i < partition_num; ++i) {
          list<list<string> > explain_result_one_source;
          DataSpace *dataspace = pt->get_partition(i);
          bool ret = get_explain_result_one_source(
              &explain_result_one_source, plan->handler, dataspace, tmp_stmt,
              &sql, i, &exec_node_name_with_depth,
              is_server_explain_stmt_always_extended);
          if (!ret) {  // got error
            got_error = true;
            error_msg = ei->get_sql();
            break;
          }

          list<list<string> >::iterator it = explain_result_one_source.begin();
          for (; it != explain_result_one_source.end(); ++it) {
            explain_info.push_back(*it);
          }
        }
        if (!got_error)
          plan->session->set_execute_plan_touch_partition_nums(-1);
      } else {
        list<list<string> > explain_result_one_source;
        bool ret = get_explain_result_one_source(
            &explain_result_one_source, plan->handler, ds, tmp_stmt, &sql, 0,
            &exec_node_name_with_depth, is_server_explain_stmt_always_extended);
        if (!ret) {  // got error
          got_error = true;
          error_msg = ei->get_sql();
        } else {
          list<list<string> >::iterator it = explain_result_one_source.begin();
          for (; it != explain_result_one_source.end(); ++it) {
            explain_info.push_back(*it);
          }
        }
      }
      tmp_stmt->free_resource();
      delete tmp_stmt;
      tmp_stmt = NULL;
    } else {
      list<string> row;
      row.push_back(exec_node_name_with_depth);
      int count = is_server_explain_stmt_always_extended ? 13 : 11;
      for (int i = 0; i < count; ++i) {
        row.push_back("");
      }
      explain_info.push_back(row);
    }
  }
  session->clean_explain_element_list();

  if (got_error) {
    string msg =
        string("explain statement get exception when handle sub sql [");
    msg.append(error_msg);
    msg.append("].");
    LOG_ERROR("%s\n", msg.c_str());
    plan->set_cancel_execute(true);
  } else {
    ExecuteNode *start_node =
        plan->get_explain_node(is_server_explain_stmt_always_extended);
    plan->set_start_node(start_node);
  }
}

void Statement::refresh_norm_table_spaces() {
  SeparatedExecNode *last_node = exec_nodes[exec_nodes.size() - 1];
  if (st.type == STMT_SELECT && last_node->get_node_dataspace()) {
    par_table_num = 0;
    spaces.clear();
    par_tables.clear();
    spaces.push_back(last_node->get_node_dataspace());
    record_scan_one_space_tmp_map[st.scanner] = last_node->get_node_dataspace();
  }
}

void Statement::refresh_part_table_links() {
  stmt_node *latest_st = get_latest_stmt_node();
  vector<table_link *> tmp_par_tables;
  if (st.type == STMT_UPDATE) {
    update_on_one_server = true;
  }
  if (st.type == STMT_DELETE) {
    delete_on_one_server = true;
  }
  if (par_table_num <= 0) return;

  if (!need_reanalysis_space) {
    if (par_tables.size() == 1 &&
        (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT) &&
        st.scanner->children_begin &&
        st.scanner->children_begin->is_select_union) {
      return;
    }
    vector<table_link *>::iterator it = par_tables.begin();
    for (; it != par_tables.end(); ++it) {
      table_link *tmp = latest_st->table_list_head;
      while (tmp) {
        // The last part table may be a tmp table for left join. For example:
        // original sql is: select * from t1 left join t2 on t1.c1 = t2.c1.
        // current sql might be: select * from tmp_t1 left join tmp_t2 t2 on
        // tmp_t1.c1 = t2.c2. when we refresh the table links, we should compare
        // with tmp_t2 instead of t2.
        if (left_join_par_tables.count(*it)) {
          const char *schema_name1 = TMP_TABLE_SCHEMA;
          const char *schema_name2 =
              tmp->join->schema_name ? tmp->join->schema_name : schema;
          const char *table_name1 =
              left_join_table_position[left_join_par_tables[*it].first]
                                      [left_join_par_tables[*it].second]
                                          .c_str();
          const char *table_name2 = tmp->join->table_name;
          if (!lower_case_compare(table_name1, table_name2) &&
              !lower_case_compare(schema_name1, schema_name2)) {
            tmp_par_tables.push_back(tmp);
            DataSpace *space = Backend::instance()->get_data_space_for_table(
                TMP_TABLE_SCHEMA, table_name2);
            record_scan_all_table_spaces[tmp] = space;
            record_scan_par_table_map[tmp->join->cur_rec_scan] = tmp;
            break;
          }
        } else {
          const char *schema_name1 =
              (*it)->join->schema_name ? (*it)->join->schema_name : schema;
          const char *schema_name2 =
              tmp->join->schema_name ? tmp->join->schema_name : schema;
          const char *table_name1 = (*it)->join->table_name;
          const char *table_name2 = tmp->join->table_name;
          if (!lower_case_compare(table_name1, table_name2) &&
              !lower_case_compare(schema_name1, schema_name2)) {
            tmp_par_tables.push_back(tmp);
            record_scan_all_table_spaces[tmp] =
                record_scan_all_table_spaces[*it];
            record_scan_par_table_map[tmp->join->cur_rec_scan] = tmp;
            break;
          }
        }
        tmp = tmp->next;
      }
    }
#ifdef DEBUG
    ACE_ASSERT(tmp_par_tables.size() == par_tables.size());
#endif
  } else {
    if (st.type == STMT_INSERT_SELECT || st.type == STMT_REPLACE_SELECT) {
      table_link *tmp = latest_st->scanner->first_table;
      record_scan_par_table_map[tmp->join->cur_rec_scan] = tmp;
      Backend *backend = Backend::instance();
      const char *schema_name =
          tmp->join->schema_name ? tmp->join->schema_name : schema;
      DataSpace *ds =
          backend->get_data_space_for_table(schema_name, tmp->join->table_name);
      record_scan_all_table_spaces[tmp] = ds;
      bool select_table_is_part = false;
      par_table_num = 0;
      if (ds->is_partitioned()) {
        ++par_table_num;
        select_table_is_part = true;
      }

      tmp = tmp->next;
      record_scan_par_table_map[tmp->join->cur_rec_scan] = tmp;
      schema_name = tmp->join->schema_name ? tmp->join->schema_name : schema;
      ds =
          backend->get_data_space_for_table(schema_name, tmp->join->table_name);
      record_scan_all_table_spaces[tmp] = ds;
      if (ds->is_partitioned()) {
        ++par_table_num;
        tmp_par_tables.push_back(tmp);
      }

      if (select_table_is_part) {
        tmp_par_tables.push_back(latest_st->scanner->first_table);
      }
    } else {
      table_link *tmp = latest_st->table_list_head;
      Backend *backend = Backend::instance();
      DataSpace *ds = NULL;
      while (tmp) {
        const char *schema_name =
            tmp->join->schema_name ? tmp->join->schema_name : schema;
        ds = backend->get_data_space_for_table(schema_name,
                                               tmp->join->table_name);
        if (ds->is_partitioned()) {
          break;
        }
        tmp = tmp->next;
      }
      if (!tmp) {
        par_table_num = 0;
      } else {
#ifdef DEBUG
        ACE_ASSERT(tmp);
#endif
        tmp_par_tables.push_back(tmp);
        par_table_num = 1;
        record_scan_par_table_map[tmp->join->cur_rec_scan] = tmp;
        record_scan_all_table_spaces[tmp] = ds;
      }
    }
  }
  par_tables = tmp_par_tables;
}

const char *Statement::get_partition_key_for_auto_space() {
  name_item *partition_key_item = st.sql->create_tb_oper->primary_key_cols;
  if (!partition_key_item)
    partition_key_item = st.sql->create_tb_oper->all_unique_keys
                             ? st.sql->create_tb_oper->all_unique_keys->col_list
                             : NULL;
  if (!partition_key_item)
    partition_key_item = st.sql->create_tb_oper->first_key_cols;
  if (!partition_key_item)
    partition_key_item = st.sql->create_tb_oper->first_non_key_cols;
  if (!partition_key_item) {
    LOG_ERROR("Fail to determine a partition key from the sql.\n");
    throw Error("Fail to determine a partition key from the sql.");
  }

  return partition_key_item->name;
}

void Statement::add_partitioned_table_config_before_create_tb(
    table_link *table, const char *part_table_scheme,
    const char *part_table_key) {
  create_part_tb_def = NULL;
  Backend *backend = Backend::instance();
  const char *schema_name =
      table->join->schema_name ? table->join->schema_name : schema;
  const char *table_name = table->join->table_name;
  Schema *schema = backend->find_schema(schema_name);
  if (!schema) {
    string error_msg;
    error_msg.assign("Schema [");
    error_msg.append(schema_name);
    error_msg.append("] not found.");
    LOG_ERROR("%s\n", error_msg.c_str());
    throw Error(error_msg.c_str());
  }
  ACE_RW_Thread_Mutex *dynamic_add_mutex =
      backend->get_dynamic_modify_rep_mutex();
  ACE_Write_Guard<ACE_RW_Thread_Mutex> guard(*dynamic_add_mutex);
  Table *tab = schema->get_table(table_name);
  if (tab && tab->is_normal()) {
    string error_msg;
    error_msg.assign("Table [");
    error_msg.append(schema_name);
    error_msg.append(".");
    error_msg.append(table_name);
    error_msg.append("] has already configured as a normal table.");
    LOG_ERROR("%s\n", error_msg.c_str());
    throw Error(error_msg.c_str());
  }
  PartitionScheme *sch = backend->find_partition_scheme(part_table_scheme);
  if (!sch) {
    string error_msg;
    error_msg.assign("Partition scheme [");
    error_msg.append(part_table_scheme);
    error_msg.append("] not found.");
    LOG_ERROR("%s\n", error_msg.c_str());
    throw Error(error_msg.c_str());
  }
  if (tab) {  // partitioned table already configured
    LOG_ERROR(
        "Partitioned table configuration already exists, plz create it "
        "directly, or dynamic remove that"
        " configuration if you want another one.\n");
    throw Error("Partitioned table configuration already exists");
  }
  Connection *conn = NULL;
  bool has_table = false;
  try {
    conn = schema->get_connection();
    if (!conn) {
      LOG_ERROR("Failed to get connection when dynamic_add_table.\n");
      throw Error("Failed to get connection when dynamic_add_table.");
    }
    char sql[400];
    sprintf(sql,
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE "
            "TABLE_SCHEMA='%s' AND TABLE_NAME='%s'",
            schema_name, table_name);
    string value;
    conn->query_for_one_value(sql, value, 0);
    conn->get_pool()->add_back_to_free(conn);
    if (value != "0") {
      has_table = true;
    }
  } catch (exception &e) {
    if (conn) {
      conn->get_pool()->add_back_to_dead(conn);
    }
    string msg =
        "got error when check whether schema has contained the target table "
        "due to : ";
    msg.append(e.what());
    LOG_ERROR("%s\n", msg.c_str());
    throw Error(msg.c_str());
  }
  if (has_table) {
    throw Error("The table already created");
  }

  size_t len_key1 = strlen(part_table_key);
  char *tmp = new char[len_key1 + 1];
  strncpy(tmp, part_table_key, len_key1 + 1);
  add_dynamic_str(tmp);
  part_table_key = tmp;
  create_part_tb_def =
      new PartitionedTable(table_name, part_table_key, sch, schema,
                           sch->get_type(), true, NULL, DEFAULT_VIRTUAL_TIMES);
  schema->add_table(create_part_tb_def);
  backend->add_data_space(create_part_tb_def);
}

bool Statement::support_navicat_profile_sql(ExecutePlan *plan) {
  if (!is_partial_parsed() && st.type == STMT_SELECT) {
    table_link *tbl = st.scanner->first_table;
    if (tbl && tbl->join) {
      if (!strcasecmp(tbl->join->table_name, "PROFILING")) {
        field_item *fi = st.scanner->field_list_head;
        if (fi && fi->next && fi->field_expr &&
            fi->field_expr->type == EXPR_STR && fi->next->field_expr &&
            fi->next->field_expr->type == EXPR_FUNC) {
          StrExpression *str_expr = (StrExpression *)fi->field_expr;
          CurrentStatementFunctionType func_type =
              fi->next->field_expr->get_cur_func_type();
          if (!strcasecmp(str_expr->str_value, "QUERY_ID") &&
              func_type == AGGREGATE_TYPE_SUM) {
            ExecuteNode *navicate_node = plan->get_navicat_profile_sql_node();
            plan->set_start_node(navicate_node);
            return true;
          }
        }
      }
    }
  }
  return false;
}

void Statement::refuse_modify_table_checking() {
  if (!is_partial_parsed() || get_stmt_node()->table_list_head) {
    if (st.type == STMT_UPDATE || st.type == STMT_DELETE) {
      table_link *tbl = st.scanner->first_table;
      if (tbl && tbl->join) {
        const char *schema_name =
            tbl->join->schema_name ? tbl->join->schema_name : schema;
        string full_table_name;
        if (tbl) {
          splice_full_table_name(schema_name, tbl->join->table_name,
                                 full_table_name);
        }
        if (stmt_session->is_refuse_modify_table(full_table_name)) {
          LOG_ERROR(
              "Not support update/delete on a table [%s] which contains no "
              "primary key or key.\n",
              full_table_name.c_str());
          throw NotSupportedError(
              "Not support update/delete on a table which contains no primary "
              "key or key.");
        }
      }
    }
  }
}

void Statement::handle_coalecse_function(table_link **table) {
  if (is_partial_parsed()) return;
  // only consider the top record_scan
  map<unsigned int, pair<unsigned int, string> > coalesce_sqls;

  field_item *fi = st.scanner->field_list_head;
  while (fi) {
    if (!fi->field_expr) {
      continue;
    }
    CurrentStatementFunctionType type = fi->field_expr->get_cur_func_type();
    if (type == AGGREGATE_TYPE_COALESCE) {
      FunctionExpression *expr = (FunctionExpression *)fi->field_expr;
      ListExpression *param_list = expr->param_list;
      expr_list_item *elh = param_list->expr_list_head;
      if (!elh) return;
      string alias;
      string case_when_sql("CASE");
      try {
        do {
          case_when_sql.append(" WHEN (");
          string para;
          elh->expr->to_string(para);
          case_when_sql.append(para);
          case_when_sql.append(" IS NOT NULL) THEN ");
          case_when_sql.append(para);
          elh = elh->next;
        } while (elh && elh != param_list->expr_list_head);
        if (fi->alias)
          alias = fi->alias;
        else
          expr->to_string(alias);
      } catch (...) {
        // ignore replace
        return;
      }
      case_when_sql.append(" ELSE NULL END AS `");
      case_when_sql.append(alias);
      case_when_sql.append("`");
      coalesce_sqls[fi->head_pos] = make_pair(fi->tail_pos, case_when_sql);
    }
    fi = fi->next;
  }

  if (!coalesce_sqls.empty()) {
    string ori_sql(sql);
    string new_sql;
    unsigned int pos = 0;
    map<unsigned int, pair<unsigned int, string> >::iterator it =
        coalesce_sqls.begin();
    for (; it != coalesce_sqls.end(); ++it) {
      new_sql.append(ori_sql, pos, it->first - pos - 1);
      new_sql.append(it->second.second);
      pos = it->second.first;
    }
    new_sql.append(ori_sql, pos, ori_sql.length() - pos);
    Statement *new_stmt = NULL;
    char *tmp;
    SAVE_STR_STMT(sql, new_sql.c_str());
    re_parser_stmt(&(st.scanner), &new_stmt, sql);
    *table = new_stmt->get_stmt_node()->table_list_head;
    LOG_DEBUG("Query sql after replace coalesce is [%s]=[%@]\n", sql, this);
  }
}

bool Statement::handle_view_related_stmt(ExecutePlan *plan,
                                         table_link **table) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();

  if (on_view && !is_partial_parsed()) {
    if (*table && st.type != STMT_CREATE_VIEW &&
        st.type != STMT_SHOW_CREATE_TABLE && st.type != STMT_SHOW_FIELDS &&
        st.type != STMT_DROP_VIEW && st.type != STMT_LOCK_TB &&
        st.type != STMT_DROP_TB && st.type != STMT_CREATE_PROCEDURE &&
        st.type != STMT_CREATE_FUNCTION && st.type != STMT_CREATE_TRIGGER &&
        st.type != STMT_SHOW_INDEX && st.ty