/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */
#include <handler.h>
#include <log_tool.h>
#include <multiple.h>
#include <plan.h>
#include <routine.h>

#include <string>

using namespace dbscale;
using namespace dbscale::plan;
using std::string;

ExecutePlan::ExecutePlan(Statement *statement, Session *session, Driver *driver,
                         Handler *handler) {
  this->statement = statement;
  this->sql = statement->get_sql();
  this->tmp_table_create_sql = NULL;
  this->session = session;
  this->driver = driver;
  this->handler = handler;
  this->res_node = NULL;
  this->start_node = NULL;
  this->is_cursor = false;
  this->need_destroy_dataspace_after_exec = false;
  this->keep_conn_old = false;
  this->migrate_tool = NULL;
  this->federated = false;
  this->dispatch_federated = false;
  this->added_traditional_num = false;
  this->can_swap = false;
  this->connection_swaped_out = false;
  this->slow_query_time_local = slow_query_time;
  this->fetch_node_no_thread = false;
  this->cancel_execute = false;
  this->is_parse_transparent = false;
  this->send_cluster_xa_transaction_ddl_error_packet = false;
  get_bthread_count = 0;
  is_forward_plan = false;
  single_server_innodb_rollback_on_timeout = INNODB_ROLLBACK_ON_TIMEOUT_UNKNOWN;
#ifdef DEBUG
  this->plan_cost_time = 0;
#endif
}

void ExecuteNode::add_child(ExecuteNode *child) {
  this->do_add_child(child);
  child->parent = this;
}
void ExecuteNode::get_children(list<ExecuteNode *> &children) {
  this->do_get_children(children);
}

void ExecuteNode::acquire_start_config_lock(string extra_info) {
#ifndef CLOSE_MULTIPLE
  if (multiple_mode) {
    try {
      MultipleManager::instance()->acquire_start_config_lock(extra_info);
    } catch (...) {
      set_status(EXECUTE_STATUS_COMPLETE);
      throw;
    }
  }
#else
  ACE_UNUSED_ARG(extra_info);
#endif
}
void ExecuteNode::release_start_config_lock() {
#ifndef CLOSE_MULTIPLE
  if (multiple_mode) {
    try {
      MultipleManager::instance()->release_start_config_lock(
          plan->session->get_start_config_lock_extra_info());
    } catch (...) {
      set_status(EXECUTE_STATUS_COMPLETE);
      throw;
    }
  }
#endif
}

void ExecuteNode::swap_ready_list_with_row_map_list(
    ExecuteNode *node,
    AllocList<Packet *, Session *, StaticAllocator<Packet *> > **child_list,
    int ready_row_size, unsigned long buffer_ready_row_size) {
  AllocList<Packet *, Session *, StaticAllocator<Packet *> > *tmp_list =
      row_map[node];
  if (tmp_list->empty() && can_swap_ready_rows) {
    row_map[node] = *child_list;
    *child_list = tmp_list;
  } else {
    size_t push_count = 0;
    try {
      list<Packet *>::iterator it = (*child_list)->begin();
      for (; it != (*child_list)->end(); ++it) {
        tmp_list->push_back(*it);
        push_count++;
      }
      (*child_list)->clear();
    } catch (ListOutOfMemError &e) {
      LOG_DEBUG("due to ListOutOfMemError,need pop some packet\n");
      for (; push_count > 0; push_count--) {
        (*child_list)->pop_front();
      }
      throw e;
    }
  }
  row_map_size[node] += ready_row_size;
  buffer_row_map_size[node] += buffer_ready_row_size;
  received_packet_num += (unsigned long)ready_row_size;
  if (((received_packet_num & 0x03FF) == 0 || max_mergenode_buffer_rows == 0) &&
      max_mergenode_ready_rows_size && ready_row_size) {
    max_mergenode_buffer_rows = max_mergenode_buffer_rows_size /
                                row_map[node]->front()->total_capacity();
    if (!max_mergenode_buffer_rows) max_mergenode_buffer_rows = 1000;
  }
}

void ExecutePlan::clean_start_node() {
  LOG_DEBUG("Execute plan start node %@ start clean\n", this);
  if (start_node) {
    start_node->clean();
    delete start_node;
    start_node = NULL;
  }
}

inline void ExecutePlan::clean() {
  try {
    LOG_DEBUG("Execute plan %@ start clean\n", this);
    clean_start_node();
    session->set_var_flag(false);
    session->giveback_left_alloc_to_backend();
    if (added_traditional_num || session->is_event_add_failed())
      session->sub_traditional_num();
    added_traditional_num = false;
  } catch (exception &e) {
    LOG_ERROR("Error occurs when clean execute plan since %s\n", e.what());
  }
}

bool ExecutePlan::handle_sp_condition(bool keep_conn_old, const char *error_msg,
                                      const char *table_info_error_sqlstate,
                                      unsigned int table_info_error_code) {
  if (session->is_call_store_procedure()) {
    SPWorker *cur_sp_worker = session->get_cur_sp_worker();
    if (cur_sp_worker->get_is_for_call_store_procedure_directly()) {
      if (session->get_call_sp_nest() > 1 && session->get_sp_error_code() > 0)
        throw ErrorPacketException(error_msg);
      else
        return false;
    }

    unsigned int error_code = 0;
    string sql_state;
    if (table_info_error_code > 0) {
      error_code = table_info_error_code;
      sql_state = table_info_error_sqlstate;
    } else if (session->get_sp_error_code() > 0) {
      error_code = session->get_sp_error_code();
      sql_state = session->get_sp_error_state();
    } else {
      fullfil_error_info(&error_code, &sql_state);
    }

    if (cur_sp_worker && cur_sp_worker->has_routine_handler) {
      LOG_DEBUG("Handle cur sp worker [%s] for condition handling.\n",
                cur_sp_worker->get_name());
      if (error_code > 0) {
        SPConditionHandler *cond = cur_sp_worker->find_and_exec_condition(
            error_code, sql_state.c_str());
        if (cond) {  // error is handled by sp condition handler
          if ((error_code == ERROR_SP_FETCH_NO_DATA_CODE) &&
              session->get_session_option("cursor_use_free_conn").int_val &&
              session->check_for_transaction()) {
            deal_sp_cursor_fetch_no_data_ignore_trx();
            LOG_DEBUG(
                "Error packet of sp cursor of ERROR_SP_FETCH_NO_DATA_CODE in "
                "trx is handled by sp condition, so continue the sp running "
                "here.\n");
          } else {
            deal_plan_exception(PACKET_ERROR_NON_SEND_TYPE, keep_conn_old);
            LOG_DEBUG(
                "Error packet is handled by sp condition, so continue the sp "
                "running here.\n");
          }
          session->set_cur_stmt_execute_fail(false);
          return true;
        }
        if (session->get_call_sp_nest() >=
            1) {  // nested call sp throw error, we need re-throw error and let
                  // upper sp try to handle it.
          throw ErrorPacketException(error_msg);
        }
      }
    } else {
      if (session->get_call_sp_nest() >=
          1) {  // nested call sp throw error, we need re-throw error and let
                // upper sp try to handle it.
        throw ErrorPacketException(error_msg);
      }
    }
  }

  return false;
}

BackendThread *ExecutePlan::get_one_bthread() {
#ifndef DBSCALE_TEST_DISABLE
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "bthread") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "get_null")) {
    return NULL;
  }
#endif
  BackendThread *bthread = NULL;
  bool from_thread_pool = false;
  if (!max_fetch_node_threads ||
      get_bthread_count < (unsigned int)max_fetch_node_threads) {
    from_thread_pool = true;
  } else {
    if (this->statement &&
        ((this->statement->get_stmt_node()->type == STMT_SHOW_TABLE_STATUS) ||
         !(this->statement->get_union_all_nodes().empty()))) {
      /*for STMT_SHOW_TABLE_STATUS and union like sql, should get thread
        from backend_thread_pool.
        The reason: in this kind of plan, the fetch nodes may not belong to the
        same father node, so the bthread_vector may be clean before some fetch
        node get thread.
      */
      from_thread_pool = true;
    }
  }
  if (from_thread_pool) {
    Backend *backend = Backend::instance();
    bthread = backend->get_backend_thread_pool()->get_one_from_free();
    if (!bthread) {
      return NULL;
    }
    bthread_vector.push_back(bthread);
  } else {
    bthread = bthread_vector.at(get_bthread_count % max_fetch_node_threads);
  }
  ++get_bthread_count;
  return bthread;
}
void ExecutePlan::start_all_bthread() {
  // TODO: add mutex if there is thread concurrency.
  if (!bthread_vector.empty()) {
    vector<BackendThread *>::iterator it = bthread_vector.begin();
    for (; it != bthread_vector.end(); ++it) (*it)->wakeup_handler_thread();
    bthread_vector.clear();
  }
}

inline void set_session_has_fetch_node_back(Session *session) {
  // After execute plan reset session has fetch node state to false.
  if (session->get_has_fetch_node()) session->set_has_fetch_node(false);
  session->giveback_left_alloc_to_backend();
}

inline void set_session_state_back_to_working(Session *session) {
  if (session->get_session_state() == SESSION_STATE_HANDLING_RESULT) {
    /*Now the server result handling is done, so change the state back to
      working */
    session->set_session_state(SESSION_STATE_WORKING);
  }
}

inline bool need_check_keep(ExecutePlan *plan) {
  if (plan->session->is_in_cluster_xa_transaction()) return false;
  /*For DML sql, if it in the transaction or not in transaction with
   * auto_commit=1, it can be consider no need to check keep conn.*/
  Statement *stmt = plan->statement;
  if (stmt->get_stmt_node()->type == STMT_SELECT ||
      stmt->get_stmt_node()->type == STMT_INSERT ||
      stmt->get_stmt_node()->type == STMT_UPDATE ||
      stmt->get_stmt_node()->type == STMT_DELETE) {
    if ((plan->session->get_auto_commit_is_on() &&
         !plan->session->is_in_transaction()) ||
        plan->session->is_in_transaction())
      return false;
    return true;
  }

  return true;
}

void ExecutePlan::generate_plan_can_swap() {
  can_swap = false;
  if (start_node) can_swap = start_node->can_swap();
}

bool ExecutePlan::plan_can_swap() { return can_swap; }

void ExecutePlan::execute() {
  if (cancel_execute) {
    return;
  }
  if (session->is_in_explain()) {
    int seq_id = session->inc_and_get_explain_node_sequence_id();
    this->start_node->set_node_id(seq_id);
    list<ExecuteNode *> node_list;
    node_list.push_back(this->start_node);
    session->update_explain_info(node_list, 0, seq_id);
    return;
  }
  if (session->get_session_state() == SESSION_STATE_WORKING) {
    keep_conn_old = handler->is_keep_conn();
    session->set_affected_rows(-1);
    if (statement->get_stmt_node()->type != STMT_SHOW_WARNINGS) {
      if (support_show_warning) {
        if (!session->is_keeping_connection()) {
          session->remove_warning_conn_to_free();
        }
        session->reset_warning_dataspaces();
        session->reset_warning_map();
        session->reset_warning_info();
        session->reset_load_warning_packet_list();
      }
    }
    if (need_check_keep(this))
      handler->set_keep_conn(handler->check_keep_conn_beforehand(statement));
#ifdef DEBUG
    plan_start_timing();
#endif
  }

  try {
    try {
      if (is_cursor) {
#ifdef DEBUG
        /*Currently, cursor is not support for session swap.*/
        if (session->get_session_state() != SESSION_STATE_WORKING)
          ACE_ASSERT(0);
#endif
        check_ddl_in_cluster_xa_transaction_before_node_execute();
        start_node->execute();
      } else {
        while (start_node && !start_node->is_finished()) {
          check_ddl_in_cluster_xa_transaction_before_node_execute();
          start_node->execute();
          if (session->need_swap_for_server_waiting()) {
            can_swap = false;
            LOG_DEBUG("Session %@ is waiting for server, so swap it out.\n",
                      session);
            return;
          }
        }
      }
    } catch (Exception &e) {
      if (session->is_call_store_procedure() &&
          session->get_call_sp_nest() >= 1)
        throw;
      bool is_duplicate_entry_in_error_packet = false;
      if (statement->get_stmt_node()->type == STMT_SELECT &&
          select_error_not_rollback && !session->in_auto_trx()) {
        LOG_DEBUG("start to check select error not rollback.\n");
        string err_message = string(e.what());
        boost::to_lower(err_message);
        if (e.get_errno() == ERROR_PACKET_EXCEPTION_CODE) {
          if (!check_select_error_not_rollback_in_packet(
                  start_node->get_error_packet())) {
            handler->flush_net_buffer();
            send_error_packet();
          } else {
            throw;
          }
        } else if (e.get_errno() == ERROR_CLIENT_BROKEN_CODE ||
                   e.get_errno() == ERROR_SERVER_SEND_FAIL_CODE ||
                   e.get_errno() == ERROR_SERVER_RECEIVE_FAIL_CODE ||
                   e.get_errno() == ERROR_SERVER_SHUTDOWN_CODE ||
                   (e.get_errno() == ERROR_EXECUTE_NODE_FAILED_CODE &&
                    err_message.find("connection has been killed") !=
                        string::npos)) {
          e.append_error_message(". this transaction restarted.");
          throw e;
        } else {
          bool is_skip_handle_exception = false;
          session->acquire_has_send_client_error_packet_mutex();
          if (session->get_has_send_client_error_packet()) {
            LOG_DEBUG(
                "Session has send error packet, so ignore this "
                "handle_exception of "
                "%d,%s.\n",
                e.get_errno(), e.what());
            is_skip_handle_exception = true;
          } else
            session->set_has_send_client_error_packet();
          session->release_has_send_client_error_packet_mutex();
          if (!is_skip_handle_exception) {
            handler->handle_exception_just_send_to_client(e.get_errno(),
                                                          e.what());
          }
        }
      } else if (e.get_errno() == ERROR_PACKET_EXCEPTION_CODE) {
        if (trx_pk_duplicate_not_rollback) {
          is_duplicate_entry_in_error_packet =
              has_duplicate_entry_in_error_packet(
                  start_node->get_error_packet());
        }
        /* if autocommit is on, now session is in transaction state, but
          send_error_packet will record the error code, it cause the throw out
          won't execute commit, then this session is always in transaction state
          until error happened or commit. now it is supposed to run the code in
          the normal way if autocommit is on, because each sql is
          non-interference as one transaction.*/
        if (trx_pk_duplicate_not_rollback &&
            is_duplicate_entry_in_error_packet &&
            session->get_affected_server_type() != AFFETCED_MUL_SERVER &&
            !session->in_auto_trx()) {
          LOG_DEBUG(
              "find Duplicate entry key affetced one server, transaction won't "
              "rollback.\n");
          send_error_packet();
        } else {
          throw;
        }
      } else {
        throw;
      }
    }
#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(),
                    "info_schema_mirror_tb_reload") &&
        !strcasecmp(test_info->test_case_operation.c_str(),
                    "get_execute_node_error")) {
      if (session->get_is_info_schema_mirror_tb_reload_internal_session()) {
        throw ExecuteNodeError("test mirror exception.");
      }
    } else if (!strcasecmp(test_info->test_case_name.c_str(),
                           "info_schema_mirror_tb_reload") &&
               !strcasecmp(test_info->test_case_operation.c_str(),
                           "get_error_packet")) {
      if (session->get_is_info_schema_mirror_tb_reload_internal_session()) {
        throw ErrorPacketException("test mirror exception.");
      }
    }
#endif

#ifndef DBSCALE_TEST_DISABLE
    handler->after_exec_recv.start_timing();
#endif
    set_session_state_back_to_working(session);
    set_session_has_fetch_node_back(session);
    if (need_check_keep(this))
      handler->set_keep_conn(handler->need_to_keep_conn(statement));
    stmt_node *st = statement->get_stmt_node();
    stmt_type type = st->type;
    if (type == STMT_SET) {
      handler->deal_with_set(statement);
    }

    if (statement->get_need_handle_acl_related_stmt()) {
      handle_acl_related_stmt(st);
    }

    if (enable_acl && (type == STMT_CREATE_DB || type == STMT_DROP_DB)) {
      const char *db_name = NULL;
      if (type == STMT_CREATE_DB)
        db_name = statement->get_stmt_node()->sql->create_db_oper->dbname->name;
      else
        db_name = statement->get_stmt_node()->sql->drop_db_oper->dbname->name;
      if (multiple_mode &&
          MultipleManager::instance()->get_is_cluster_master()) {
        Backend *backend = Backend::instance();
        backend->notify_other_dbscale_nodes_create_drop_db(db_name, type);
        Backend::instance()->update_auth_info_for_cluster_user(db_name, type);
        Backend::instance()->clear_auth_info(true);
        backend->inc_acl_version();
      } else {
        Backend::instance()->clear_auth_info(false);
      }
      if (type == STMT_CREATE_DB) {
        if (db_name) {
          string schema_name(db_name);
          session->add_auth_schema_to_set(schema_name);
        }
      }
      string db_str = db_name;
      Backend::instance()->add_session_auth_schema(db_str, type);
    }
    if (enable_acl && (type == STMT_REVOKE || type == STMT_GRANT)) {
      handler->recover_replication_info_in_slave_dbscale_mode();
    }

    if (enable_acl && (type == STMT_DROP_USER || type == STMT_CREATE_USER ||
                       type == STMT_REVOKE || type == STMT_GRANT)) {
      vector<const char *> *grant_user_name_vec =
          statement->get_grant_user_name_vec();
      if (grant_user_name_vec && !grant_user_name_vec->empty()) {
        Backend::instance()->notify_all_dbscale_remove_auth_info_for_users(
            grant_user_name_vec);
      }
    }

    if (handler->be_killed()) throw ThreadIsKilled();
    if (!is_cursor) {
      clean();
    }

    if (enable_calculate_select_total_memory && print_total_memory_info &&
        type == STMT_SELECT &&
        Backend::instance()->check_session_mem_status(session)) {
      LOG_DEBUG("fetchnode memory check ok\n");
    }

#ifdef DEBUG
    plan_end_timing();
#endif
#ifndef DBSCALE_TEST_DISABLE
    handler->after_exec_recv.end_timing_and_report();
#endif
  } catch (ErrorPacketException &e) {
    LOG_INFO("Plan get an error packet for stmt %s.\n",
             this->statement->get_sql());
    handler->fin_alter_table(statement->get_stmt_node()->type,
                             statement->get_stmt_node());
    session->set_cur_stmt_execute_fail(true);
    if (session->is_call_store_procedure() &&
        session->get_call_sp_nest() >= 1) {
      Packet *packet = NULL;
      packet = start_node->get_error_packet();
      if (packet) session->save_sp_error_response_info(packet);
    }
    set_session_state_back_to_working(session);
    set_session_has_fetch_node_back(session);
    if (!handler->be_killed()) {
      if (handle_sp_condition(keep_conn_old, e.what())) return;
      deal_plan_exception(PACKET_ERROR_TYPE, keep_conn_old);
      if (this->statement->is_handle_sub_query() ||
          session->is_call_store_procedure()) {
        /*This is a separated exec subquery or routine, dbscale should throw
         * an NestStatementErrorPacketException exception to terminate the
         * parent separated exec node processing or routine processing.*/
        throw NestStatementErrorException();
      }
      if (this->statement->is_cross_node_join()) {
        throw CrossNodeJoinFail();
      }
      if (this->session->get_is_silence_ok_stmt())
        throw SilenceOKStmtFail(e.what());
    } else {
      clean();
      throw ThreadIsKilled();
    }
  } catch (ExecuteNodeError &e) {
    handler->fin_alter_table(statement->get_stmt_node()->type,
                             statement->get_stmt_node());
    session->set_cur_stmt_execute_fail(true);
    session->set_error_generated_by_dbscale(true);
    set_session_state_back_to_working(session);
    set_session_has_fetch_node_back(session);
    LOG_ERROR("Catch Execute Node Error.\n");
    if (!handler->be_killed()) {
      if (migrate_tool) {
        clean();
        throw Error("migrate get exception.");
      }
      if (session->get_is_info_schema_mirror_tb_reload_internal_session()) {
        clean();
        throw Error("info_schema_mirror_tb_reload_internal get exception.");
      }
      bool is_skip_handle_exception = false;
      session->acquire_has_send_client_error_packet_mutex();
      if (session->get_has_send_client_error_packet()) {
        LOG_DEBUG(
            "Session has send error packet, so ignore this handle_exception of "
            "%d,%s.\n",
            e.get_errno(), e.what());
        is_skip_handle_exception = true;
      }
      session->release_has_send_client_error_packet_mutex();
      deal_plan_exception(EXECUTE_NODE_ERROR_TYPE, keep_conn_old);
      if (!is_skip_handle_exception) {
        handler->handle_exception(&e, true, e.get_errno(), e.what());
      }
      if (this->statement->is_handle_sub_query() ||
          session->is_call_store_procedure()) {
        // TODO: avoid sending more than one error packet to client, if do the
        // separated execution parallelly. A mutex inside the top statment
        // could be introduced.
        throw NestStatementErrorException();
      }
      if (this->statement->is_cross_node_join()) {
        throw CrossNodeJoinFail();
      }
      if (this->session->get_is_silence_ok_stmt())
        throw SilenceOKStmtFail(e.what());
    } else {
      clean();
      throw ThreadIsKilled();
    }

    return;
  } catch (dbscale::sql::SQLError &e) {
    handler->fin_alter_table(statement->get_stmt_node()->type,
                             statement->get_stmt_node());
    session->set_cur_stmt_execute_fail(true);
    if (session->is_call_store_procedure() &&
        session->get_call_sp_nest() >= 1) {
      Packet *packet = NULL;
      packet = start_node->get_error_packet();
      if (packet) session->save_sp_error_response_info(packet);
    }
    session->set_error_generated_by_dbscale(true);
    set_session_state_back_to_working(session);
    set_session_has_fetch_node_back(session);
    if (e.get_error_code() != ERROR_SP_FETCH_NO_DATA_CODE)
      LOG_ERROR("SQLError occur in execute plan:%s.\n", e.what());
    if (handler->be_killed()) {
      clean();
      throw ThreadIsKilled();
    }
    if (handle_sp_condition(keep_conn_old, e.what())) return;
    deal_plan_exception(EXCEPTION_TYPE, keep_conn_old);
    throw;
  } catch (Exception &e) {
    handler->fin_alter_table(statement->get_stmt_node()->type,
                             statement->get_stmt_node());
    session->set_cur_stmt_execute_fail(true);
    session->set_error_generated_by_dbscale(true);
    set_session_state_back_to_working(session);
    set_session_has_fetch_node_back(session);
    LOG_ERROR("Error occur in execute plan:%s.\n", e.what());
    if (handler->be_killed()) {
      clean();
      throw ThreadIsKilled();
    }
    deal_plan_exception(EXCEPTION_TYPE, keep_conn_old);
    throw;
  }
}

void ExecutePlan::handle_acl_related_stmt(stmt_node *st) {
  stmt_type type = st->type;
  Backend *backend = Backend::instance();
  if (type == STMT_GRANT) {
    try {
      backend->handle_acl_by_grant_stmt(st->sql->grant_oper,
                                        session->get_schema());
    } catch (...) {
      LOG_INFO("got error during handle_acl_by_grant_stmt");
    }
  } else if (type == STMT_REVOKE) {
    try {
      backend->handle_acl_by_revoke_stmt(st->sql->revoke_oper,
                                         session->get_schema());
    } catch (...) {
      LOG_INFO("got error during handle_acl_by_revoke_stmt");
    }
  } else if (type == STMT_CREATE_USER) {
    try {
      backend->handle_white_user_info_by_create_alter_drop_user_stmt(st->sql,
                                                                     type);
    } catch (...) {
      LOG_INFO(
          "got error during handle_white_user_info_by_create_drop_user_stmt");
    }
    try {
      backend->add_default_schema_acl_for_new_user(
          st->sql->create_user_oper->user_clause);
    } catch (...) {
      LOG_INFO("got error during add_default_schema_acl_for_new_user");
    }
  } else if (type == STMT_DROP_USER) {
    vector<const char *> *grant_user_name_vec =
        statement->get_grant_user_name_vec();
    Backend::instance()->remove_acl_for_users(grant_user_name_vec);
    try {
      backend->handle_white_user_info_by_create_alter_drop_user_stmt(st->sql,
                                                                     type);
    } catch (...) {
      LOG_INFO(
          "got error during handle_white_user_info_by_create_drop_user_stmt");
    }
  } else if (type == STMT_ALTER_USER) {
    try {
      backend->handle_white_user_info_by_create_alter_drop_user_stmt(st->sql,
                                                                     type);
    } catch (...) {
      LOG_INFO(
          "got error during "
          "handle_white_user_info_by_create_drop_user_stmt.\n");
    }
  }
}

void ExecutePlan::deal_sp_cursor_fetch_no_data_ignore_trx() {
  if (migrate_tool) {
    clean();
    throw Error("migrate get exception.");
  }
  if (session->get_is_info_schema_mirror_tb_reload_internal_session()) {
    clean();
    throw Error("info_schema_mirror_tb_reload_internal get exception.");
  }
  handler->flush_net_buffer();
  clean();
  deal_lock_error();
}

void ExecutePlan::deal_plan_exception(PlanExceptionType type,
                                      bool keep_conn_old) {
  if (migrate_tool) {
    clean();
    throw Error("migrate get exception.");
  }
  if (session->get_is_info_schema_mirror_tb_reload_internal_session()) {
    clean();
    throw Error("info_schema_mirror_tb_reload_internal get exception.");
  }
  handler->set_keep_conn(keep_conn_old);
  handler->flush_net_buffer();
  if (type == PACKET_ERROR_TYPE) send_error_packet();
  if (statement->get_stmt_node()->type == STMT_LOCK_TB)
    handler->set_keep_conn(true);
  // the connection may have execute begin, so it must execute rollback
  // set keep conn true, then connection will not be add back to free in
  // plan::clean(), the connection will execute rollback in session::rollback()
  // ddl should not keep the connection cause the connection has execute commit.
  // if the connection is not kept in kept_connection_map.
  // it means it does not send begin, so need add the connection back to free in
  // plan::clean()
  // for session is_in_cluster_xa_transaction, wo don't need to think about keep
  // conn
  if (session->check_for_transaction() &&
      !session->is_in_cluster_xa_transaction() &&
      !session->get_kept_connections().empty())
    handler->set_keep_conn(true);
  handler->end_transaction_timing();
  clean();
  deal_lock_error();
  handler->set_keep_conn(keep_conn_old);
  deal_error_transaction();
  handler->set_keep_conn(session->is_keeping_connection());
}
void ExecutePlan::deal_error_transaction() {
  bool is_consistent_start_tran = false;
  if (statement->get_stmt_node()->type == STMT_START_TRANSACTION &&
      statement->get_stmt_node()->sql->start_tran_oper->with_consistence)
    is_consistent_start_tran = true;
  /*If the stmt is start transaction with consistence snapshot, the session
   * should be rollback.*/
  if (!session->check_for_transaction() && !is_consistent_start_tran) {
    return;
  }
  if (session->is_server_shutdown()) {
    session->rollback();
    session->cluster_xa_rollback();
    session->set_xa_id((unsigned long)0);
    if (!server_shutdown_keep_session) {
      throw ServerShutDown("In transaction, server shutdown.");
    }
    session->acquire_has_send_client_error_packet_mutex();
    if (!session->get_has_send_client_error_packet()) {
      Packet *packet = handler->get_error_packet(
          9002, "In transaction, server shutdown.", NULL);
      try {
        handler->send_to_client(packet);
      } catch (...) {
      }
      delete packet;
    }
    session->set_has_send_client_error_packet();
    session->release_has_send_client_error_packet_mutex();
  } else if ((!enable_xa_transaction ||
              session->get_session_option("close_session_xa").int_val != 0) &&
             use_savepoint &&
             /*For commit and rollback, we should not try to roll back to
              * savepoint.*/
             statement->get_stmt_node()->type != STMT_COMMIT &&
             !(statement->get_stmt_node()->type == STMT_ROLLBACK &&
               statement->get_stmt_node()->sql->rollback_point_oper->type ==
                   ROLLBACK_TYPE_ALL)) {
    if (statement->get_stmt_node()->type == STMT_ROLLBACK &&
        statement->get_stmt_node()->sql->rollback_point_oper->type ==
            ROLLBACK_TYPE_TO_POINT) {
      // rollback to got an error
    } else if (session->get_affected_server_type() == AFFETCED_MUL_SERVER) {
      LOG_DEBUG("DBScale ROLLBACK TO SAVEPOINT for sql [%s]\n",
                statement->get_sql());
      string str = "ROLLBACK TO ";
      str += SAVEPOINT_NAME;
      handler->deal_with_transaction((const char *)str.c_str());
    }
    session->set_xa_id((unsigned long)0);
  } else {
    session->rollback();
    if (!silently_rollback) session->set_transaction_error(true);
    session->set_xa_id((unsigned long)0);
  }
}
inline void ExecutePlan::deal_lock_error() {
  bool has_execute_unlock = false;
  if (statement->get_stmt_node()->type == STMT_LOCK_TB ||
      statement->get_stmt_node()->type == STMT_FLUSH_TABLE_WITH_READ_LOCK) {
    session->set_in_lock(
        true);  // the lock_tb stmt execute fail, so the session is not in lock.
                // we should set it to lock otherwise the session->unlock will
                // not work.
    session->unlock();
    has_execute_unlock = true;
  }
  if (enable_auto_unlock_during_error && !has_execute_unlock) session->unlock();
}
void ExecutePlan::check_ddl_in_cluster_xa_transaction_before_node_execute() {
  if (!enable_cluster_xa_transaction) return;

  stmt_type type = statement->get_stmt_node()->type;
  if (session->is_in_cluster_xa_transaction() &&
      ((type > STMT_DDL_START && type < STMT_DDL_END) ||
       type == STMT_CREATE_USER || type == STMT_ALTER_USER ||
       type == STMT_DROP_USER || type == STMT_GRANT || type == STMT_REVOKE)) {
    LOG_DEBUG(
        "DDL stmt can not execute when session is in cluster xa "
        "transaction.\n");
    set_send_cluster_xa_transaction_ddl_error_packet(true);
    throw ErrorPacketException();
  }
}
