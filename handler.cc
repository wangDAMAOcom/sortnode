/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <basic.h>
#include <driver.h>
#include <exception.h>
#include <handler.h>
#include <log.h>
#include <log_tool.h>
#include <multiple.h>
#include <option.h>
#include <socket_base_manager.h>
#include <xa_transaction.h>

#include "ace/os_include/netinet/os_tcp.h"

bool is_implict_commit_sql(stmt_type type) {
  if (type == STMT_TRUNCATE || type == STMT_ALTER_TABLE ||
      type == STMT_CREATE_TB || type == STMT_DROP_DB || type == STMT_ALTER_DB ||
      type == STMT_DROP_TB || type == STMT_CREATE_DB || type == STMT_LOCK_TB ||
      type == STMT_CREATE_VIEW || type == STMT_CREATE_EVENT ||
      type == STMT_CREATE_TRIGGER || type == STMT_DROP_PROC ||
      type == STMT_DROP_VIEW || type == STMT_CREATE_PROCEDURE ||
      type == STMT_CREATE_SELECT || type == STMT_CREATE_LIKE ||
      type == STMT_START_TRANSACTION || type == STMT_RENAME_TABLE ||
      type == STMT_GRANT || type == STMT_FLUSH_TABLE_WITH_READ_LOCK ||
      type == STMT_FLUSH_PRIVILEGES || type == STMT_CREATE_USER ||
      type == STMT_DROP_USER || type == STMT_SET_PASSWORD ||
      type == STMT_REVOKE)
    return true;
  return false;
}

#ifndef DBSCALE_TEST_DISABLE
void HandlerMonitorPoint::end_timing_and_report() {
  if (handler->need_monitor) {
    end_timing();
    handler->report_monitor_value(name.c_str(), cost);
  }
}
void HandlerMonitorPoint::start_timing() {
  if (handler->need_monitor) {
    start_date = ACE_OS::gettimeofday();
  }
}
void HandlerMonitorPoint::end_timing() {
  if (handler->need_monitor && start_date.sec() != 0) {
    end_date = ACE_OS::gettimeofday();
    cost = end_date - start_date;
  }
}
#endif

void MonitorPointSQLStatistic::start_timing() {
  start_time = ACE_OS::gettimeofday();
}

void MonitorPointSQLStatistic::end_timing(monitor_stage stage) {
  if (start_time == ACE_Time_Value::zero) return;
  auto cur_time = ACE_OS::gettimeofday();
  switch (stage) {
    case monitor_stage::SQL_PARSE:
      parse_time = cur_time - start_time;
      break;
    case monitor_stage::PLAN_GENERATION:
      plan_generation_time = cur_time - start_time;
      break;
    case monitor_stage::PLAN_EXECUTION:
      plan_execution_time = cur_time - start_time;
      ACE_UINT64 usec;
      plan_execution_time.to_usec(usec);
      break;
    case monitor_stage::FAILED:
      failed = true;
      report_to_handler_self();
      break;
    case monitor_stage::SUCCEED:
      if (!query_sql.empty()) report_to_handler_self();
    default:
      break;
  }
}

void MonitorPointSQLStatistic::report_to_handler_self() {
  ACE_Time_Value schedule_cost = parse_time + plan_generation_time;
  ACE_Time_Value total_cost = schedule_cost + plan_execution_time;

  // if cur sql failed, the recorded time for parse, plan_generation,
  // plan_execution could be 0, cause it could failed before plan execution, or
  // before plan generation, or even before parse.
  // Record partial data will spoil the accumulated data in backend
  if (!failed) {
    handler->report_global_monitor_value(total_cost, schedule_cost,
                                         plan_execution_time);
    handler->report_monitor_point_histogram_value(total_cost, 1);
    handler->report_outline_monitor_info(
        no_param_sql_id.c_str(),
        SQLMonitorInfo(parse_time, plan_generation_time, plan_execution_time,
                       no_param_sql.c_str()));
  } else {
    handler->report_global_monitor_value(ACE_Time_Value(0), ACE_Time_Value(0),
                                         ACE_Time_Value(0));
  }
  handler->report_monitor_point_number_value(failed, 1);
}

bool MonitorPointSQLStatistic::should_record_query_sql(stmt_type type) {
  return (type == STMT_SELECT) || (type == STMT_INSERT) ||
         (type == STMT_UPDATE) || (type == STMT_DELETE) ||
         (type == STMT_REPLACE) || (type == STMT_LOAD) ||
         (type == STMT_REPLACE_SELECT) || (type == STMT_INSERT_SELECT) ||
         ((type > STMT_DDL_START) && (type < STMT_DDL_END));
}

void MonitorPointSQLStatistic::set_outline_info(const char *no_param_sql,
                                                const char *no_param_sql_id) {
  this->no_param_sql = no_param_sql;
  this->no_param_sql_id = no_param_sql_id;
}

Handler::Handler()
    : backend(Backend::instance()),
      buffer_len(0),
      thread_id(0),
      is_being_release(false),
      base(NULL),
      need_monitor(false),
      num_of_send(0),
      num_of_recv(0),
      is_signal_handler(false),
      is_migrate_async_handler(false),
      sql_statistic(this),
      trx_timing_stop(false),
      mul_stmt_send_error_packet(false)
#ifndef DBSCALE_TEST_DISABLE
      ,
      handler_svc("handler_svc", this),
      handler_as_listener("handler_as_listener", this),
      handler_recy("handler_recy", this),
      handler_swap("handler_swap", this),
      parse_sql("parse_sql", this),
      gen_plan("gen_plan", this),
      exec_send("exec_send", this),
      exec_recv("exec_recv", this),
      send_op("send_op_only", this),
      prepare_gen_plan("prepare_gen_plan", this),
      handler_last_send("handler_last_send", this),
      handler_recv_from_client("handler_recv_from_client", this),
      after_exec_recv("after_exec_recv", this),
      after_handle_recv("after_handle_recv", this)
#endif
{
  exiting_timeout_tv = TimeValue(dbscale_exiting_timeout, 0);
  wait_timeout_tv = TimeValue(wait_timeout, 0);
  stream = NULL;
  transaction_time_handler = new TransactionTimerHandler(this);
}

Handler::~Handler() {
  if (net_buffer) delete net_buffer;
  if (transaction_time_handler) {
    end_transaction_timing();
    delete transaction_time_handler;
  }
}
void Handler::report_monitor_point_to_backend() {
  Backend *backend = Backend::instance();
  map<string, ACE_Time_Value>::iterator it = monitor_point_map.begin();
  for (; it != monitor_point_map.end(); ++it) {
    backend->report_monitor_handler_value(it->first.c_str(), it->second);
  }
  reset_monitor_point_map();
}

void Handler::re_init_handler(bool clean_session) {
  // we do not re init the buffer_size
  buffer_len = 0;
  mul_stmt_send_error_packet = false;
  do_re_init_handler();

  if (clean_session) {
    Session *s = get_session();
    if (s) {
      if (!s->get_stream()) {
        if (stream) {
          stream->close();
          stream = NULL;
        }
      }
      s->set_session_state(SESSION_STATE_CLOSE);
      s->reset_top_stmt_and_plan();
      delete s;
    }
  }
  set_session(NULL);
  /* reset the stream socket of handler, in case this handle is freed by
     thread pool and close the in using client socket.
     */
  peer().set_handle(-1);
}

int Handler::open(void *p) {
  if (super::open(p) == -1) return -1;
  // Remove this handler from the reactor because all the read and
  // write are handled in svc function. We need to pass the
  // DONT_CALL mask, otherwise the reactor will close the handle.
  this->reactor()->remove_handler(
      this, ACE_Event_Handler::READ_MASK | ACE_Event_Handler::DONT_CALL);
  this->wakeup_handler_thread();
  return 0;
}

void Handler::handle_exception(exception *e, bool need_error_to_client,
                               uint16_t error_code, const char *message,
                               const char *sql_state) {
#ifndef DBSCALE_TEST_DISABLE
  LOG_INFO("Handle_exception %s\n", e->what());
#else
  if (strcmp(e->what(), "Client broken"))
    LOG_WARN("Handle_exception %s\n", e->what());
#endif
  get_session()->set_query_sql("", 0);
  if (get_session()->get_is_info_schema_mirror_tb_reload_internal_session()) {
    LOG_WARN(
        "Handler::handle_exception ignore send error to client for "
        "info_schema_mirror_tb_reload.\n");
    return;
  }
  if (get_session()->get_is_federated_session() &&
      get_session()->get_work_session())
    get_session()->get_work_session()->set_fe_error(true);

  get_session()->acquire_has_send_client_error_packet_mutex();
  if (need_error_to_client &&
      !get_session()->get_has_send_client_error_packet()) {
    get_session()->set_cur_stmt_error_code(error_code);
    Packet *packet = get_error_packet(error_code, message, sql_state);
    try {
      send_to_client(packet);
    } catch (exception &e) {
    }
    delete packet;
  }
  get_session()->set_has_send_client_error_packet();
  get_session()->release_has_send_client_error_packet_mutex();
}

void Handler::handle_exception_just_send_to_client(uint16_t error_code,
                                                   const char *message,
                                                   const char *sql_state) {
  Packet *packet = get_error_packet(error_code, message, sql_state);
  try {
    send_to_client(packet);
  } catch (exception &e) {
  }
  delete packet;
}

Connection *clean_fail_conn(Connection *conn, Session *session) {
  if (conn) {
    session->remove_using_conn(conn);
    session->remove_prepare_read_connection(conn);
    conn->get_pool()->add_back_to_dead(conn);
    conn = NULL;
  }
  return conn;
}

void Handler::try_kill_xa_start_conn(Connection *conn) {
  if (!conn) return;
  Connection *killer = NULL;
  killer = conn->get_server()->get_one_admin_conn();
  if (killer) {
    LOG_INFO("use killer %@ to kill XA_START conn %@\n", killer, conn);
    try {
      killer->kill_thread(conn->get_thread_id());
      release_connection(killer);
      killer = NULL;
    } catch (exception &e) {
      LOG_WARN("got error [%s] when use killer %@ to kill XA_START conn %@\n",
               e.what(), killer, conn);
      release_connection(killer);
    }
  } else {
    LOG_WARN("fail to get killer to kill XA_START conn %@\n", conn);
  }
}

bool Handler::parse_transparent_mode_change_user(Connection *cur_conn,
                                                 Session *cur_session) {
  bool is_change_user_now = false;
  string user_host = get_user_host();
  unsigned int pos = user_host.find("@");
  string user = user_host.substr(0, pos);

  if ((!strcmp(user.c_str(), supreme_admin_user) ||
       !strcmp(user.c_str(), normal_admin_user) ||
       !strcmp(user.c_str(), dbscale_internal_user))) {
    LOG_DEBUG(
        "This is a root/dbscale/dbscale_internal user, no need to change "
        "user any more.\n");
  } else {
    string host = user_host.substr(pos + 1, user_host.length() - pos - 1);
    string password = "";
    try {
      if (cur_session->get_cur_auth_plugin() == CACHING_SHA2_PASSWORD) {
        password = "";
      } else {
        cur_conn->get_password(
            password, user.c_str(), host.c_str(),
            Backend::instance()->get_backend_server_version());
      }
      cur_conn->set_session(cur_session);
      if (cur_conn->change_user(user.c_str(), NULL, password.c_str(),
                                cur_session->get_schema())) {
        cur_conn->set_changed_user_conn(true);
        is_change_user_now = true;
        LOG_DEBUG("success change user[%s].\n", user.c_str());
      }
    } catch (exception &e) {
      LOG_ERROR("send_to_server_retry chang_user_fail due to [%s],\n",
                e.what());
      throw ServerSendFail();
    }
  }
  return is_change_user_now;
}

/* For one transaction, we only do the error retry if "send to server" fails
 * at first time, for all other case ("send to client", "receive from
 * client", "receive from server", "send to server at the second time or
 * more") we will send an error to client if possible.*/
Connection *Handler::send_to_server_retry(DataSpace *space, Packet *packet,
                                          const char *schema, bool read_only,
                                          bool try_merge_begin,
                                          bool need_recv_begin,
                                          bool skip_keep_conn,
                                          bool off_sql_log_bin) {
  Connection *conn = NULL;
  Session *cur_s = get_session();
  bool add_lock_kept_conn = true;
  /* if is in xa transaction, we use xa_conn, if xa_conn is NULL,
   * we use kept conn to start cluster xa, and the conn will execute
   * error due to xa command can't be executed in transaction.*/
  if (enable_cluster_xa_transaction && cur_s->is_in_cluster_xa_transaction())
    conn = cur_s->get_cluster_xa_transaction_conn();
  if (!conn && !skip_keep_conn &&
      (!read_only || cur_s->is_keeping_connection() ||
       cur_s->has_temp_table_set() || !support_prepare_rwsplit))
    conn = cur_s->get_kept_connection(space);
  if (!skip_keep_conn && !conn && cur_s->get_lock_kept_conn()) {
    conn = cur_s->get_defined_lock_kept_conn(space);
    add_lock_kept_conn = false;
  }
  if (conn) {
    // this starting_xa is not the part of cluster xa code.
    bool starting_xa = false;
    try {
      // conn->reset();
      if (conn->get_schema_name_md5() != cur_s->get_schema_name_md5() ||
          conn->get_schema_name_md5().empty()) {
        connection_init_db(&conn, get_session()->get_schema(), space);
        conn->reset();
      }
      set_user_var(&conn, space);
      if (!add_lock_kept_conn && !skip_keep_conn &&
          !cur_s->is_in_cluster_xa_transaction() &&
          cur_s->check_for_transaction() && is_keep_conn()) {
        if (enable_xa_transaction &&
            get_session()->get_session_option("close_session_xa").int_val ==
                0 &&
            !get_session()->is_in_transaction_consistent()) {
          /* if cur_s get_xa_id is 0
           * we should start the xa transaction for this transaction if in xa
           * mode."*/
          try {
            cur_s->acquire_xa_mutex();
            if (cur_s->get_xa_id() == 0) {
              Driver *driver = Driver::get_driver();
              XA_helper *xa_helper = driver->get_xa_helper();
              xa_helper->start_xa_transaction(cur_s);
            }
            cur_s->release_xa_mutex();
            starting_xa = true;
          } catch (...) {
            cur_s->release_xa_mutex();
            throw;
          }
        }
        cur_s->insert_using_conn(conn);
        execute_begin(space, conn, packet, try_merge_begin);
      }
      send_to_server(conn, packet);
      LOG_DEBUG("Get a connection [%@] from kept connection for execution.\n",
                conn);
      if (need_recv_begin)
        conn->handle_merge_exec_begin_or_xa_start(get_session());
    } catch (exception &e) {
      if (starting_xa) {
        try_kill_xa_start_conn(conn);
      }
      if (conn) {
        LOG_ERROR("kept conn %@ exec failed due to %s\n", conn, e.what());
        if (!add_lock_kept_conn)
          get_session()->remove_defined_lock_kept_conn(space);
        conn->reset_cluster_xa_session_before_to_dead();
        conn->get_pool()->add_back_to_dead(conn);
        get_session()->remove_kept_connection(space);
        get_session()->remove_using_conn(conn);
      }
      throw;
    }
    if (!add_lock_kept_conn && !skip_keep_conn &&
        cur_s->check_for_transaction() && is_keep_conn())
      cur_s->set_kept_connection(space, conn);
    if (cur_s->get_lock_kept_conn() && add_lock_kept_conn &&
        cur_s->check_dataspace_is_catalog_space(space) &&
        !cur_s->check_lock_conn_in_kept_conn(space) &&
        !cur_s->check_cluster_conn_is_xa_conn(conn))
      cur_s->set_defined_lock_kept_conn(space, conn);
    return conn;
  }
  if (is_keep_conn() && !cur_s->is_trx_specify_read_only()) {
    read_only = false;
  }
  // TODO: config the retry times and duration
  Backend *backend = Backend::instance();
  unsigned int max_retry_times = backend->get_max_retry_times();
  unsigned int retry_times = 0;
  bool retry_to_send_again = true;

  while (retry_to_send_again) {
    Packet new_packet;
    bool use_new_packet = false;
#ifdef DEBUG
    LOG_DEBUG("send_to_server_retry retry-times = %u.\n", retry_times);
#endif
    retry_to_send_again = false;
    bool starting_xa = false;
    try {
      starting_xa = false;
      conn = space->get_connection(get_session(), schema, read_only);
      if (cur_s->is_executing_prepare() && conn) {
        conn = cur_s->check_connection_in_prepare_read_connection(conn);
        cur_s->set_executing_prepare(false);
      }
      LOG_DEBUG(
          "Get a connection [%@] from space[%s%@], schema[%s] for execution.\n",
          conn, space->get_name(), space, schema);

#ifndef DBSCALE_TEST_DISABLE
      /*just for test fail_get_conn*/
      dbscale_test_info *test_info = get_session()->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "get_conn") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "fail_get_conn")) {
        LOG_DEBUG("set conn to null.\n");
        conn->get_pool()->add_back_to_free(conn);
        conn = NULL;
      }
#endif

      if (!conn) {
        LOG_ERROR("Fail to get an usable connection from space [%s].\n",
                  space->get_name());
        throw Error("Fail to get an usable connection");
      }
      cur_s->insert_using_conn(conn);

      bool is_change_user_now = false;
      if (cur_s->get_top_plan() &&
          cur_s->get_top_plan()->get_is_parse_transparent()) {
        is_change_user_now = parse_transparent_mode_change_user(conn, cur_s);
      }
      if (conn->get_session_var_map_md5() != cur_s->get_session_var_map_md5() ||
          is_change_user_now) {
        if (is_change_user_now) {
          conn->set_session_var(cur_s->get_session_var_map(), true, true);
        } else {
          conn->set_session_var(cur_s->get_session_var_map());
        }
#ifdef DEBUG
        LOG_DEBUG("conn set session var with session var map size %d.\n",
                  cur_s->get_session_var_map()->size());
#endif
        conn->reset();
      }

      if (conn->get_schema_name_md5() != cur_s->get_schema_name_md5() ||
          conn->get_schema_name_md5().empty() ||
          cur_s->get_schema_name_md5().empty()) {
        connection_init_db(&conn, cur_s->get_schema(), space);
#ifdef DEBUG
        LOG_DEBUG("conn init db with session schema %s, space %@.\n",
                  cur_s->get_schema(), space);
#endif
        conn->reset();
      }
      if (off_sql_log_bin) {
        string sql_log_bin = conn->get_session_var_value("SQL_LOG_BIN");
        if (strcasecmp(sql_log_bin.c_str(), "OFF")) {
          TimeValue timeout(backend_sql_net_timeout);
          conn->execute_one_modify_sql("SET SQL_LOG_BIN=OFF;", &timeout);
          conn->set_session_var_value("SQL_LOG_BIN", "OFF");
          conn->reset();
        }
      }
      set_user_var(&conn, space);
      /*only when session in keep conn state, we can send begin for this
        connction. then the connection will be kept in kept_connection_map. the
        connection will not be put back to free cause it has sent begin. then
        connection will send commit or rollback before add back to free.
       */
      if (!skip_keep_conn && cur_s->check_for_transaction() && is_keep_conn() &&
          !cur_s->is_in_cluster_xa_transaction()) {
        if (enable_xa_transaction &&
            get_session()->get_session_option("close_session_xa").int_val ==
                0 &&
            !get_session()->is_in_transaction_consistent()) {
          /* if cur_s get_xa_id is 0
           * we should start the xa transaction for this transaction if in xa
           * mode."*/
          try {
            cur_s->acquire_xa_mutex();
            if (cur_s->get_xa_id() == 0) {
              Driver *driver = Driver::get_driver();
              XA_helper *xa_helper = driver->get_xa_helper();
              xa_helper->start_xa_transaction(cur_s);
            }
            cur_s->release_xa_mutex();
            starting_xa = true;
          } catch (...) {
            cur_s->release_xa_mutex();
            throw;
          }
        }
        use_new_packet =
            execute_begin(space, conn, packet, try_merge_begin, &new_packet);
      }
#ifndef DBSCALE_TEST_DISABLE
      if (starting_xa) {
        Backend *bk = Backend::instance();
        dbscale_test_info *test_info = bk->get_dbscale_test_info();
        if (!strcasecmp(test_info->test_case_name.c_str(), "xa_trx") &&
            !strcasecmp(test_info->test_case_operation.c_str(),
                        "send_to_server_fail_after_xa_start")) {
          bk->set_dbscale_test_info("", "", "");
          LOG_INFO(
              "dbscale_test backend 'xa_trx' "
              "'send_to_server_fail_after_xa_start'\n");
          throw ServerSendFail();
        }
      }
#endif
      if (use_new_packet) {
        send_to_server(conn, &new_packet);
      } else {
        send_to_server(conn, packet);
      }
      if (need_recv_begin)
        conn->handle_merge_exec_begin_or_xa_start(get_session());
    } catch (ServerSendFail &e) {
      LOG_INFO("Retry to send to server again. failed conn is %@\n", conn);
      if (starting_xa) {
        try_kill_xa_start_conn(conn);
      }
      conn = clean_fail_conn(conn, get_session());
      retry_to_send_again = true;
      if (retry_times++ < max_retry_times)
        continue;
      else
        throw;
    } catch (ServerReceiveFail &e) {
      LOG_INFO("Retry to send to server again. failed conn is %@\n", conn);
      if (starting_xa) {
        try_kill_xa_start_conn(conn);
      }
      conn = clean_fail_conn(conn, get_session());
      retry_to_send_again = true;
      if (retry_times++ < max_retry_times)
        continue;
      else
        throw;
    } catch (PeerShutdown &e) {
      LOG_INFO("Retry to send to server again. failed conn is %@\n", conn);
      if (starting_xa) {
        try_kill_xa_start_conn(conn);
      }
      conn = clean_fail_conn(conn, get_session());
      retry_to_send_again = true;
      if (retry_times++ < max_retry_times)
        continue;
      else
        throw;
    } catch (HandlerError &e) {
      conn = clean_fail_conn(conn, get_session());
      throw;
    } catch (...) {
      get_session()->set_server_shutdown(true);
      conn = clean_fail_conn(conn, get_session());
      throw;
    }
  }
  if (!skip_keep_conn && is_keep_conn())
    cur_s->set_kept_connection(space, conn);
  if (!skip_keep_conn && cur_s->get_lock_kept_conn() &&
      cur_s->check_dataspace_is_catalog_space(space) &&
      !cur_s->check_lock_conn_in_kept_conn(space))
    cur_s->set_defined_lock_kept_conn(space, conn);
  return conn;
}

bool Handler::execute_begin(DataSpace *space, Connection *conn, Packet *packet,
                            bool try_merge_begin, Packet *new_packet) {
  bool use_new_packet = false;
  if (!enable_xa_transaction ||
      get_session()->get_session_option("close_session_xa").int_val != 0 ||
      get_session()->is_in_transaction_consistent()) {
    list<char *> savepoint_list = get_session()->get_all_savepoint();
    if (try_merge_begin && !use_savepoint && savepoint_list.empty() &&
        !conn->is_odbconn()) {
      conn->set_merge_exec_begin(true);
      string sql = "BEGIN;";
      rebuild_query_merge_sql(packet, space, sql, new_packet);
      use_new_packet = true;
    } else {
      conn->execute_transaction_unlock_stmt("begin");
      if (get_session()->has_bridge_mark_sql() &&
          get_session()->get_session_option("is_bridge_session").bool_val) {
        string bridge_mark_sql = get_session()->get_bridge_mark_sql();
        get_session()->adjust_retl_mark_tb_name(space, bridge_mark_sql);
        conn->execute_transaction_unlock_stmt(bridge_mark_sql.c_str());
      }
      if (!savepoint_list.empty()) {
        list<char *>::iterator it = savepoint_list.begin();
        for (; it != savepoint_list.end(); ++it) {
          string str = "SAVEPOINT ";
          str.append(*it);
          conn->execute_transaction_unlock_stmt((const char *)str.c_str());
        }
      }
      if (use_savepoint) {
        string str = "SAVEPOINT ";
        str += SAVEPOINT_NAME;
        conn->execute_transaction_unlock_stmt((const char *)str.c_str());
      }
    }
  } else {
    Driver *driver = Driver::get_driver();
    XA_helper *xa_helper = driver->get_xa_helper();
    list<char *> savepoint_list = get_session()->get_all_savepoint();
    if (try_merge_begin && !use_savepoint && savepoint_list.empty() &&
        !conn->is_odbconn() && !conn->is_merge_exec_xa_start()) {
      string xa_start;
      xa_helper->get_start_xa_query(get_session(), space, conn, xa_start);
      rebuild_query_merge_sql(packet, space, xa_start, new_packet);
      conn->set_merge_exec_xa_start(true);
      use_new_packet = true;
    } else {
      xa_helper->init_conn_to_start_xa(get_session(), space, conn);
    }
  }

  return use_new_packet;
}

void Handler::set_user_var(Connection **conn, DataSpace *dataspace) {
  Session *cur_s = get_session();
#ifdef DEBUG
  ASSERT(cur_s);
#endif
  try {
    if (cur_s->get_var_flag()) {
#ifdef DEBUG
      LOG_DEBUG(
          "need to set user variables for connection [%@] before use it,"
          " sql is [%s]\n",
          conn, get_session()->get_var_sql().c_str());
#endif
      (*conn)->set_user_var(get_session()->get_var_sql());
    }
    (*conn)->reset();
  } catch (HandlerError &e) {
    if (*conn) {
      get_session()->remove_using_conn(*conn);
      (*conn)->get_pool()->add_back_to_dead(*conn);
      get_session()->remove_kept_connection(dataspace);
      *conn = NULL;
    }
    throw;
  } catch (exception &e) {
    if (*conn) {
      get_session()->remove_using_conn(*conn);
      (*conn)->get_pool()->add_back_to_dead(*conn);
      get_session()->remove_kept_connection(dataspace);
      *conn = NULL;
    }
    get_session()->set_server_shutdown(true);
    throw;
  }
}

int Handler::connection_init_db(Connection **conn, const char *schema,
                                DataSpace *dataspace) {
  if (schema && schema[0]) {
    try {
      const char *conn_schema = (*conn)->get_schema();
      if (!conn_schema || !conn_schema[0] || strcmp(conn_schema, schema)) {
        (*conn)->change_schema(schema);
      }
      return 0;
    } catch (ServerShutDown &e) {
      if (*conn) {
        get_session()->remove_using_conn(*conn);
        (*conn)->get_pool()->add_back_to_dead(*conn);
        get_session()->remove_kept_connection(dataspace);
        *conn = NULL;
      }
      get_session()->set_server_shutdown(true);
      throw HandlerError(e.what());
    } catch (HandlerError &e) {
      if (get_session()->check_table_without_given_schema(dataspace)) {
        if (*conn) {
          put_back_connection(dataspace, *conn);
          *conn = NULL;
        }
        throw;
      }
      LOG_DEBUG(
          "Warning, fail to change schema. May be use a table"
          " in other schema"
          "rather than current one [%s].\n",
          schema);
      // No return error, to enable select data from other schema
      return 0;
    } catch (exception &e) {
      LOG_ERROR("Get exception '%s' during the init of connection\n", e.what());
      if (*conn) {
        get_session()->remove_using_conn(*conn);
        (*conn)->get_pool()->add_back_to_dead(*conn);
        get_session()->remove_kept_connection(dataspace);
        *conn = NULL;
      }
      get_session()->set_server_shutdown(true);
      throw;
    }
  }
  return 0;
}

void Handler::set_nodelay() {
  int nodelay = 1;
  if (stream->set_option(ACE_IPPROTO_TCP, TCP_NODELAY, &nodelay,
                         sizeof(nodelay)) == -1) {
    Backend::instance()->record_latest_important_dbscale_warning(
        "Set client connection with TCP_NODELAY flag failed.\n");
  } else {
    LOG_DEBUG("Set client connection with TCP_NODELAY flag successfully.\n");
  }
}

bool Handler::work_as_event_base_listener() {
  LOG_DEBUG("Thread handler %@ work as a cur listener thread of base %@\n",
            this, get_base());
#ifndef DBSCALE_TEST_DISABLE
  handler_as_listener.start_timing();
#endif
  get_base()->svc();
  if (get_base()->is_stop_base() ||
      ACE_Reactor::instance()->reactor_event_loop_done()) {
    if (get_session()) {
      get_session()->clean_in_using_table();
      if (!get_session()->is_empty_remove_sp_vec())
        get_session()->clear_all_sp_in_remove_sp_vector();
      this->remove_session();
      re_init_handler(true);
    }
    LOG_INFO("Cur_listener thread %@ of base %@ quit due to dbscale stop.\n",
             this, get_base());
    get_base()->set_has_finished(true);
    this->get_pool()->add_back_to_free(this);
    return true;
  }
  get_base()->acquire_ready_list_lock();
  get_base()->set_cur_listener(NULL);
  get_base()->release_ready_list_lock();
#ifndef DBSCALE_TEST_DISABLE
  handler_as_listener.end_timing_and_report();
#endif
  return false;
}

int Handler::svc() {
  ACE_Message_Block *mb = NULL;
  LOG_DEBUG("Handler::svc\n");
  bool swap = false;
  bool skip_message_wait = false;

  while (1) {
    if (!skip_message_wait) {
      mb = NULL;
      if (this->getq(mb) == -1) {
        LOG_ERROR("Thread handler error: getq return -1\n");
        if (!is_being_release) this->get_pool()->add_back_to_dead(this);
        break;
      }
      if (mb->msg_type() == ACE_Message_Block::MB_HANGUP) {
        LOG_INFO("Thread handler stop work, cause MB_HANGUP\n");
        mb->release();
        if (!is_being_release) this->get_pool()->add_back_to_dead(this);
        continue;  // continue to get the quit message
      }
      char message = (char)(*(mb->rd_ptr()));
      mb->release();
      if (message == THREAD_MESSAGE_QUIT) {
        LOG_INFO("Thread handler %@ exit, get message quit.\n", this);
        break;
      } else if (message == THREAD_MESSAGE_WAKEUP) {
#ifdef DEBUG
        LOG_DEBUG("Thread handler %@ wakeup, get message wakeup.\n", this);
#endif
      }
    }

#ifndef DBSCALE_TEST_DISABLE
    handler_svc.start_timing();
#endif
    if (get_base() && get_base()->get_cur_listener() == this) {
      if (work_as_event_base_listener()) break;
    }

    skip_message_wait = false;

    if (transparent_mode && get_session() &&
        get_session()->get_is_admin_accepter()) {
      stream = &peer();
      get_session()->set_stream(stream);
      set_nodelay();
      get_session()->set_handler(this);
      LOG_DEBUG("Start to log in transport in transparent mode\n");
      try {
        if (enable_read_only || reinforce_enable_read_only) {
          LOG_ERROR(
              "refuse login in transparent mode because enable_read_only or "
              "reinforce_enable_read_only is open.\n");
          throw Error(
              "enable_read_only or reinforce_enable_read_only is open.\n");
        }
        Driver::get_driver()->check_trans_conn_num();
        get_session()->init_internal_base();
        LOG_DEBUG(
            "Finish to log in transport in transparent mode and succeed\n");
        InternalEventBase *internal_base = get_session()->get_internal_base();
        while (1) {
          if (event_base_loop(internal_base->get_event_base(), 0) < 0) {
            LOG_ERROR(
                "SocketEventBase::svc fail with errno %d for event_base_loop\n",
                errno);
            break;
          }
          if (check_trans_conn_status()) {
            Driver::get_driver()->remove_trans_session(get_session());
            break;
          }
        }
      } catch (HandlerError &e) {
        LOG_ERROR(
            "Error occurs when connecting to master server in transparent "
            "mode\n");
        refuse_login(1040, e.what());
      } catch (SockError &e) {
        LOG_ERROR(
            "Error occurs when connecting to master server in transparent "
            "mode\n");
        refuse_login();
      } catch (Error &e) {
        LOG_ERROR(
            "Error occurs when connecting to master server in transparent "
            "mode since %s\n",
            e.what());
        refuse_login();
      }
    } else {
      if (get_session()->get_session_state() == SESSION_STATE_INIT) {
        stream = &peer();
        get_session()->set_stream(stream);
        set_nodelay();
        set_nonblock(stream);
      } else {
        stream = get_session()->get_stream();
        thread_id = get_session()->get_thread_id();
      }
      swap = false;
      try {
        if (get_session()->get_session_state() == SESSION_STATE_INIT) {
          begin_session();
          get_session()->init_session_option();
          get_session()->set_option_values();
          get_session()
              ->init_internal_base();  // the internal event base may be used
                                       // during the receive_from_client inside
                                       // authenticate
          authenticate();
          get_session()->init_session();
          get_session()->set_has_more_result(
              false);  // if multiple stmt, will be set to true for middle stmt,
                       // eof or ok packet should with has more result flag

          if (enable_session_swap && get_session()->need_swap_handler()) {
#ifdef DEBUG
            LOG_DEBUG("Swap session %d from handler %@ after login.\n",
                      get_session()->get_thread_id(), this);
#endif
            Session *cur_s = get_session();
            re_init_handler(false);
            /*add handler back to pool before session switch, so that this
             * handler thread may be assigned a new working session more likely
             * before it go to sleep.*/
            skip_message_wait = recycle_working_handler();
            cur_s->switch_out_to_wait_client_event();
            swap = true;
          }
        }
        if (!swap) {
          LOG_DEBUG(
              "Handler %@ start to serve session %d %@ with handler socket %d "
              "session socket %d session record client_socket %d.\n",
              this, get_session()->get_thread_id(), get_session(),
              stream->get_handle(), get_session()->get_stream()->get_handle(),
              get_session()->get_client_socket());
#ifdef DEBUG
          LOG_DEBUG("Session state %d.\n",
                    (int)(get_session()->get_session_state()));
#endif
          for (;;) {
            if (get_session()->get_session_state() !=
                    SESSION_STATE_HANDLING_RESULT &&
                be_killed()) {
              /*If the session is swapped in to receive from server, it should
               * goto the plan execution event the session is killed, otherwise
               * the resource of this session will not be released.*/
              LOG_INFO("Handler %@ is killed.\n", this);
              throw ThreadIsKilled();
            }
            exiting_timeout_tv = TimeValue(dbscale_exiting_timeout, 0);
            wait_timeout_tv = TimeValue(wait_timeout, 0);
            Session *cur_s = get_session();
            if (get_session()->get_session_state() !=
                SESSION_STATE_HANDLING_RESULT)
              while (cur_s->is_prepare_kill()) {
                if (be_killed()) {
                  LOG_INFO("Handler %@ is killed.\n", this);
                  throw ThreadIsKilled();
                }
                ACE_OS::sleep(1);
              }

            if (cur_s->get_session_state() != SESSION_STATE_HANDLING_RESULT) {
              /*
               * one sql can do swap should record in session.
               * if record in handler, firt need_do_swap = true, then session
               * swap out to another handler. then second handler may set
               * need_do_swap = false, this will lead one sql block.
               */
              cur_s->set_can_swap(Driver::get_driver()->get_need_do_swap());
              cur_s->reset_statement_level_session_variables();
#ifndef DBSCALE_TEST_DISABLE
              exec_send.start_timing();
#endif

              if (need_update_option()) {
                cur_s->set_option_values();
              }
              if (on_view && cur_s->is_view_updated()) {
                cur_s->update_view_map();
              }
              cur_s->set_load_data_local_command(false);

              handle_request();
              if (get_trx_timing_stop()) {
                cur_s->rollback();
                cur_s->reset_top_stmt_and_plan();
                cur_s->set_keep_conn(cur_s->is_keeping_connection());
                cur_s->set_stmt(NULL);
              }
              mul_stmt_send_error_packet = false;
              if (need_monitor) ++num_of_send;
#ifndef DBSCALE_TEST_DISABLE
              exec_send.end_timing_and_report();
#endif
              if (enable_session_swap && cur_s->has_client_buffer_packet()) {
                // This is to handle the second packet of MYSQL_COM_CLOSE_STMT
                continue;
              }
              if (!cur_s->need_swap_for_server_waiting() &&
                  cur_s->get_audit_log_flag_local() !=
                      AUDIT_LOG_MODE_MASK_NON) {
                record_audit_log(cur_s->get_cur_stmt_type());
              }
            }
            /*Before the session waiting for server is swapped out, it will be
             * set to SESSION_STATE_WAITING_SERVER state.*/
            if (cur_s->get_session_state() == SESSION_STATE_HANDLING_RESULT) {
#ifndef DBSCALE_TEST_DISABLE
              exec_recv.start_timing();
#endif
              cur_s->reset_server_base_related();
              handle_server_result();
              if (need_monitor) ++num_of_recv;
              cur_s->set_session_state(SESSION_STATE_WORKING);
#ifndef DBSCALE_TEST_DISABLE
              exec_recv.end_timing_and_report();
#endif
              if (cur_s->get_audit_log_flag_local() !=
                  AUDIT_LOG_MODE_MASK_NON) {
                record_audit_log(cur_s->get_cur_stmt_type());
              }
              get_session()->set_query_sql("", 0);
            }
            if (!cur_s->need_swap_for_server_waiting()) {
              cur_s->clean_in_using_table();
              if (!is_keep_conn()) {
                if (support_show_warning)
                  cur_s->remove_non_warning_conn_to_free();
              }
              get_session()->set_query_sql("", 0);
            }

            if (Backend::instance()->is_exiting()) {
              LOG_INFO("Quit client connection because dbscale is exiting.\n");
              throw HandlerQuit();
            }

            if (enable_session_swap && cur_s->need_swap_handler() &&
                cur_s->get_can_swap()) {
              LOG_DEBUG("Swap session %d from handler %@.\n",
                        cur_s->get_thread_id(), this);
              re_init_handler(false);
              /*add handler back to pool before session switch, so that this
               * handler thread may be assigned a new working session more
               * likely before it go to sleep.*/
#ifndef DBSCALE_TEST_DISABLE
              handler_recy.start_timing();
#endif
              skip_message_wait = recycle_working_handler();
#ifndef DBSCALE_TEST_DISABLE
              handler_recy.end_timing_and_report();
              handler_swap.start_timing();
#endif
              if (!cur_s->need_swap_for_server_waiting()) {
                cur_s->record_idle_time();
                cur_s->switch_out_to_wait_client_event();
              } else {
                cur_s->switch_out_to_wait_server_event();
              }
#ifndef DBSCALE_TEST_DISABLE
              handler_swap.end_timing_and_report();
#endif
              swap = true;
              break;
            } else {
              cur_s->record_idle_time();
            }
            // if (cur_s->get_lock_new_connection()) {}
            if (cur_s->get_lock_kept_conn()) {
              int ret = cur_s->check_ping_and_change_master();
              if (ret == 1) {
                throw HandlerQuit();
              } else if (ret == 2) {
                throw ThreadIsKilled();
              } else if (ret == 3) {
                throw ThreadIsKilled();
              }
            }
          }
        }
      } catch (HandlerQuit &e) {
        LOG_DEBUG("Quiting connection!\n");
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_query_sql("quit", 4);
          record_audit_log(STMT_QUIT, true);
        }
      } catch (ThreadIsKilled &e) {
        LOG_INFO("Thread %@ is killed.\n", this);
        get_session()->set_cur_stmt_error_code(2013);
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_query_sql("(Killed)", strlen("(Killed)"));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }

      } catch (ClientBroken &e) {
        get_session()->rollback();
        set_keep_conn(get_session()->is_keeping_connection());
        handle_exception(&e);
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_cur_stmt_error_code(ERROR_DBSCALE_EXCEPTION_CODE);
          get_session()->set_query_sql("(ClientBroken)",
                                       strlen("(ClientBroken)"));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }

      } catch (ServerSendFail &e) {
        handle_exception(&e, true, e.get_errno(), e.what());
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_cur_stmt_error_code(e.get_errno());
          get_session()->set_query_sql("(ServerSendFail)",
                                       strlen("(ServerSendFail)"));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }

      } catch (ServerReceiveFail &e) {
        handle_exception(&e, true, e.get_errno(), e.what());
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_cur_stmt_error_code(e.get_errno());
          get_session()->set_query_sql("(ServerReceiveFail)",
                                       strlen("(ServerReceiveFail)"));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }

      } catch (Exception &e) {
        // send error to client
        char message[256];
        sprintf(message, "DBScale fails due to '%s'.", e.what());
        message[255] = '\0';
        handle_exception(&e, true, e.get_errno(), (const char *)message);
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_cur_stmt_error_code(ERROR_DBSCALE_EXCEPTION_CODE);
          get_session()->set_query_sql(message, strlen(message));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }

      } catch (exception &e) {
        char message[256];
        sprintf(message, "DBScale fails due to '%s'.", e.what());
        message[255] = '\0';
        handle_exception(&e, true, ERROR_UNKNOWN_ERROR_CODE,
                         (const char *)message);
        if (get_session()->get_audit_log_flag_local() !=
            AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          get_session()->set_cur_stmt_error_code(ERROR_DBSCALE_EXCEPTION_CODE);
          get_session()->set_query_sql(message, strlen(message));
          record_audit_log(get_session()->get_cur_stmt_type(), true);
        }
      }
    }
    if (swap) {
#ifndef DBSCALE_TEST_DISABLE
      handler_svc.end_timing_and_report();
#endif
      continue;
    }
    get_session()->clean_in_using_table();
    if (!get_session()->is_empty_remove_sp_vec())
      get_session()->clear_all_sp_in_remove_sp_vector();
    this->remove_session();
    re_init_handler(true);
    /*
    LOG_DEBUG("Handler %@ add back to pool %s %@.\n", this,
    get_pool()->get_name(), get_pool());
    this->get_pool()->add_back_to_free(this);
    */
#ifndef DBSCALE_TEST_DISABLE
    handler_recy.start_timing();
#endif
    skip_message_wait = recycle_working_handler();
#ifndef DBSCALE_TEST_DISABLE
    handler_recy.end_timing_and_report();
#endif
    end_session();
#ifndef DBSCALE_TEST_DISABLE
    handler_svc.end_timing_and_report();
#endif
  }
  Driver *driver = Driver::get_driver();
  driver->remove_handler(this);
  end_transaction_timing();
  LOG_INFO("Handler service end\n");
  return 0;
}

/*Try to get a ready session from the socket base,
 *
 *  if success return true, so the handler message wait will be skipped
 *
 *  otherwise return false, handler is add back to free to pool, and wait for
 *  handler message wakeup.*/
bool Handler::recycle_working_handler() {
  if (!get_base()) {
    this->get_pool()->add_back_to_free(this);
    LOG_DEBUG("Handler %@ add back to pool %s %@.\n", this,
              get_pool()->get_name(), get_pool());
    return false;
  }

  SocketEventBase *base = get_base();
  Session *session = NULL;
  /*We need to check the ready list twice to ensure that the ready session
   * will be served if there is free handler.*/

  /*check before add back to pool*/
  base->acquire_ready_list_lock();
  if (base->get_turn_to_be_listener_now() ||
      base->get_ready_size() < ready_event_low)
    if (base->check_cur_listener(this)) {
      LOG_DEBUG("Handler %@ start to work as listener for base %@\n", this,
                base);
      base->set_turn_to_be_listener_now(false);
      base->release_ready_list_lock();
      return true;
    }
  base->set_turn_to_be_listener_now(false);
  if (base->is_session_ready_list_empty()) {
#ifdef DEBUG
    LOG_DEBUG(
        "Ready list is empty now for first check in recycle_working_handler "
        "for handler %@.\n",
        this);
#endif
  } else {
    session = base->get_one_ready_session();
    base->inc_count_by_obtain();
  }
  base->release_ready_list_lock();

  if (session) {
    enable_ready_session_to_handler(session);
    LOG_DEBUG(
        "session %d is dispatched to handler %@ in recycle_working_handler.\n",
        session->get_thread_id(), this);
    return true;
  }

  /*check during add back to pool*/

  this->get_pool()->acquire_pool_mutex();
  base->acquire_ready_list_lock();
  if (base->is_session_ready_list_empty()) {
#ifdef DEBUG
    LOG_DEBUG(
        "Ready list is empty now for second check in recycle_working_handler "
        "for handler %@.\n",
        this);
#endif
  } else {
    session = base->get_one_ready_session();
    base->inc_count_by_obtain();
  }
  if (!session) {
    // If no ready session obtain, this handler will be added back to pool, so
    // running session dec.
    base->dec_running_session();
  }
  base->release_ready_list_lock();

  if (session) {
    this->get_pool()->release_pool_mutex();
    enable_ready_session_to_handler(session);
    LOG_DEBUG(
        "session %d is dispatched to handler %@ in recycle_working_handler.\n",
        session->get_thread_id(), this);
    return true;
  }

  this->get_pool()->add_back_to_free_without_lock(this);
  this->get_pool()->release_pool_mutex();
#ifdef DEBUG
  LOG_DEBUG("Handler %@ add back to pool %s %@.\n", this,
            get_pool()->get_name(), get_pool());
#endif
  return false;
}

void Handler::enable_ready_session_to_handler(Session *session) {
  this->set_session(session);
  session->set_handler(this);
  /*
  if (get_base())
    get_base()->remove_session_from_base(session);
    */
  if (session->get_session_state() == SESSION_STATE_WAITING_SERVER) {
    session->set_session_state(SESSION_STATE_HANDLING_RESULT);
  } else {
    session->set_session_state(SESSION_STATE_WORKING);
  }
}

void Handler::flush_net_buffer() {
  if (buffer_len > 0) {
    net_buffer->wr_ptr(net_buffer->base());
    net_buffer->pack3int(buffer_len);
    buffer_len = 0;
    net_buffer->rd_ptr(net_buffer->base() + get_packet_head_size());
    send_to_client(net_buffer, false, true);
  }
}

void Handler::send_to_client_by_buffer(Packet *packet) {
  char *p = packet->base();
  char *data = packet->rd_ptr();
  size_t data_len = Packet::unpack3uint(&p) + get_packet_head_size();

  if (buffer_size - buffer_len <= data_len) {
    if (2 * buffer_size - buffer_len <= data_len) {
      /*In this case, if we copy the source data to the send_buffer, we need
       * to do the memcpy and send_raw more than twice. So we can optimize it
       * by send the source data directly without copying to send_buffer.*/
      flush_net_buffer();
      send_to_client(packet, false);
      return;
    }
    if (buffer_len == 0) {
      /*In this case, the send_buffer is empty and the length of source data
       * is larger than the send_buffer. So we can optimize it by directly
       * send the source data to reduce one memcpy.*/
      send_to_client(packet, false);
    } else {
      size_t len = buffer_size - buffer_len;

      net_buffer->wr_ptr(net_buffer->base() + get_packet_head_size() +
                         buffer_len);
      net_buffer->packdata(data, len);
      buffer_len = buffer_size;
      flush_net_buffer();

      data_len -= len;
      net_buffer->wr_ptr(net_buffer->base() + get_packet_head_size());
      net_buffer->packdata(data + len, data_len);
      buffer_len = data_len;
    }
  } else {
    net_buffer->wr_ptr(net_buffer->base() + get_packet_head_size() +
                       buffer_len);
    net_buffer->packdata(data, data_len);
    buffer_len += data_len;
  }
}

void Handler::receive_from_client(Packet *packet) {
  // the option wait_timeout can be dynamic set, so, the handler_wait_timeout
  // may not exactly correct, cause take off the timeout arg in recv,
  // here, if swap timeout, it should throw but not receive
  Session *session = get_session();
  if (!session->get_is_super_user() && session->get_do_swap_route() &&
      session->get_client_swap_timeout()) {
    LOG_INFO("Quit client connection because wait timeout.\n");
    session->set_do_swap_route(false);
    throw HandlerQuit();
  }

  while (1) {
    try {
      do_receive_from_client(packet);
      return;
    } catch (SockError &e) {
      // receive from client timeout throw sockerror
      if (Backend::instance()->is_exiting()) {
        LOG_INFO("Quit client connection because dbscale is exiting.\n");
        throw HandlerQuit();
      }
      if (be_killed()) {
        LOG_INFO("Handler %@ is killed.\n", this);
        throw ThreadIsKilled();
      }
      if (handler_wait_timeout()) {
        LOG_INFO("Quit client connection because wait timeout.\n");
        throw HandlerQuit();
      }
      if (handler_login_timeout()) {
        LOG_DEBUG("Quit client connection because login timeout.\n");
        throw HandlerQuit();
      }
      throw;
    } catch (HandlerQuit &e) {
      throw;
    } catch (ThreadIsKilled &e) {
      throw;
    } catch (exception &e) {
      get_session()->get_status()->item_inc(TIMES_CLIENT_BROKEN_NOT_USING_QUIT);
      LOG_DEBUG("Fail to receive from client due to [%s].\n", e.what());
      throw ClientBroken();
    }
  }
}

void Handler::kill_timeout_connection(Connection *conn, Packet *packet,
                                      bool is_kill_query,
                                      const TimeValue *timeout) {
  Connection *killer = NULL;
  try {
    killer =
        conn->get_server()->create_connection(admin_user, admin_password, NULL);
  } catch (exception &e) {
    LOG_ERROR(
        "Create killer connection for timeout conn [%@] failed cause [%s].\n",
        conn, e.what());
    throw;
  }
  if (killer == NULL) {
    LOG_ERROR("Create killer connection for timeout conn [%@] failed.\n", conn);
    throw Error("Create killer connection failed");
  }

  TimeValue tv(backend_sql_net_timeout, 0);
  try {
    char sql[256];
    if (is_kill_query)
      snprintf(sql, sizeof(sql), "KILL QUERY %d;", conn->get_thread_id());
    else
      snprintf(sql, sizeof(sql), "KILL %d;", conn->get_thread_id());
#ifdef DEBUG
    if (is_kill_query)
      LOG_DEBUG("create killer query [%@] to conn [%@].\n", killer, conn);
    else
      LOG_DEBUG("send force kill [%@] to conn [%@].\n", killer, conn);
#endif
    killer->execute(sql);
    Packet kill_packet;
    killer->recv_packet(&kill_packet, &tv);
    packet->rewind();
    if (is_kill_query) do_receive_from_server(conn, packet, timeout);
  } catch (SockError &e) {
    if (e.get_errno() == ETIME) {
#ifdef DEBUG
      LOG_DEBUG("kill conn [%@] got ETIME error.\n", conn);
#endif
      if (is_kill_query) {
        try {
          kill_timeout_connection(conn, packet, false, timeout);
        } catch (...) {
          // do nothing, catch exception for recycle killer
        }
      }
    }
#ifdef DEBUG
    else {
      LOG_DEBUG("kill conn [%@] got non_ETIME SockError.\n", conn);
    }
#endif
    if (killer) {
#ifdef DEBUG
      LOG_DEBUG("put back killer [%@] to dead.\n", killer);
#endif
      delete killer;
      killer = NULL;
    }
    throw;
  } catch (exception &e) {
#ifdef DEBUG
    LOG_DEBUG("kill conn [%@] got exception.\n", conn);
#endif
    if (killer) {
#ifdef DEBUG
      LOG_DEBUG("put back killer [%@] to dead.\n", killer);
#endif
      delete killer;
      killer = NULL;
    }
    throw;
  }

  if (killer) {
#ifdef DEBUG
    LOG_DEBUG("put back killer [%@] to free.\n", killer);
#endif
    delete killer;
    killer = NULL;
  }
}

void Handler::receive_from_server(Connection *conn, Packet *packet,
                                  const TimeValue *timeout) {
  try {
    try {
      if (conn->get_server_swap_timeout() && !conn->is_odbconn() &&
          enable_session_swap && get_session()->get_can_swap()) {
        conn->set_server_swap_timeout(false);
        LOG_ERROR("Server swap timeout.\n");
        throw SockError("Server swap timeout.", ETIME);
      }
      do_receive_from_server(conn, packet, timeout);
    } catch (SockError &e) {
      LOG_ERROR("Fail to receive from server due to [%s].\n", e.what());
      if (e.get_errno() != ETIME) {
        throw;
      }
      get_session()->set_mul_node_timeout(true);
      if (conn->get_is_handshake_is_inited() &&
          conn->get_need_kill_timeout_connection()) {
        kill_timeout_connection(conn, packet, true, timeout);
      }
      if (!conn->get_need_kill_timeout_connection()) {
        conn->set_need_kill_timeout_conn(true);
        throw;
      }
    }
  } catch (exception &e) {
    LOG_ERROR("Fail to receive from server due to [%s].\n", e.what());
    if (!conn->get_need_kill_timeout_connection()) throw;
    if (!get_session()->is_error_generated_by_force_stop_trx()) {
      get_session()->set_server_shutdown(true);
    } else {
      get_session()->set_error_generated_by_force_stop_trx(false);
    }
    throw ServerReceiveFail();
  }
}

void Handler::exec_xa_rollback_for_free_conn(DataSpace *space,
                                             Connection *conn) {
  if (conn->is_start_xa_conn()) {
    Driver *driver = Driver::get_driver();
    XA_helper *xa_helper = driver->get_xa_helper();
    xa_helper->exec_xa_rollback(this->get_session(), space, conn);
  }
}

void Handler::put_back_connection(DataSpace *space, Connection *conn,
                                  bool skip_keep_conn) {
  Session *s = get_session();
  if (s->is_drop_current_schema()) {
    conn->set_schema(NULL);
  }
  auto change_user_and_could_free = [&]() -> bool {
    if (conn->is_changed_user_conn()) {
      try {
        if (conn->change_user(admin_user, admin_password, NULL, NULL)) {
          conn->set_changed_user_conn(false);
          map<string, string> *session_var_map = s->get_session_var_map();
          conn->set_session_var(session_var_map, false, true);
          return true;
        }
      } catch (exception &e) {
        LOG_ERROR("put_back_connection chang_user_fail due to [%s],\n",
                  e.what());
      }
      return false;
    }
    return true;
  };

  if (!skip_keep_conn &&
      (s->get_lock_kept_conn() || s->is_in_cluster_xa_transaction() ||
       (is_keep_conn() ||
        (support_show_warning && s->dataspace_has_warning(space)) ||
        conn->is_in_prepare() || conn->has_tmp_table()))) {
    conn->set_status(CONNECTION_STATUS_FREE);
    if (!s->check_conn_in_kept_connections(space) &&
        !s->check_lock_conn_in_kept_conn(space) &&
        !s->check_cluster_conn_is_xa_conn(conn)) {
      if (change_user_and_could_free()) {
        exec_xa_rollback_for_free_conn(space, conn);
        conn->get_pool()->add_back_to_free(conn);
      } else {
        conn->get_pool()->add_back_to_dead(conn);
      }
    }
    if (s->get_is_complex_stmt() && conn->get_wait_num()) {
      conn->waken_conn();
    }
  } else {
    s->remove_using_conn(conn);
    if (!skip_keep_conn) {
      s->remove_kept_connection(space);
      s->remove_defined_lock_kept_conn(space);
    }
    if (be_killed() || (close_load_conn && conn->is_load()) ||
        s->lock_conn_add_back_to_dead()) {
      conn->reset_cluster_xa_session_before_to_dead();
      conn->get_pool()->add_back_to_dead(conn);
    } else if (!s->check_conn_in_kept_connections(space, conn)) {
      if (change_user_and_could_free()) {
        exec_xa_rollback_for_free_conn(space, conn);
        conn->get_pool()->add_back_to_free(conn);
      } else {
        conn->get_pool()->add_back_to_dead(conn);
      }
    }
  }
}

void Handler::check_and_record_slow_query_time() {
  if (get_session()->get_slow_query_time_local() == 0 ||
      (end_date < start_date))
    return;

  unsigned long cost_time = (end_date - start_date).msec();
  LOG_DEBUG("Handler %@ cost %d ms.\n", this, cost_time);
  if (cost_time > get_session()->get_slow_query_time_local()) {
    LOG_DEBUG("Find a slow query cost %d ms, %s.\n", cost_time,
              get_session()->get_top_stmt()
                  ? get_session()->get_top_stmt()->get_sql()
                  : IDLE_SQL);
    log_slow_query(end_date, cost_time);
    get_session()->get_status()->item_inc(
        TIMES_STMT_RECORD_AS_SLOW_QUERY_BY_DBSCALE);
  }
}

void Handler::log_slow_query(ACE_Time_Value &_end_date,
                             unsigned long _cost_time) {
  ACE_Date_Time now;
  long cur_year = now.year();
  long cur_month = now.month();
  long cur_day = now.day();
  long cur_hour = now.hour();
  long cur_minute = now.minute();
  long cur_second = now.second();

  char tmp[64];
  string slow_info;

  // slow info line #1
  slow_info.append("# Time: ");
  sprintf(tmp, "%02ld%02ld%02ld %02ld:%02ld:%02ld\n", cur_year % 100, cur_month,
          cur_day, cur_hour, cur_minute, cur_second);
  slow_info.append(tmp);

  // slow info line #2
  uint32_t thread_id = get_session()->get_thread_id();
  const char *user_name = get_session()->get_username();
  string client_ip = get_session()->get_client_ip();
  slow_info.append("# User@Host: ");
  slow_info.append(user_name);
  slow_info.append("[");
  slow_info.append(user_name);
  slow_info.append("] @  [");
  slow_info.append(client_ip);
  slow_info.append("]  Id:     ");
  sprintf(tmp, "%d", thread_id);
  slow_info.append(tmp);
  slow_info.append("\n");

  // slow info line #3
  slow_info.append("# Query_time: ");
  double time = ((double)_cost_time) / 1000;
  sprintf(tmp, "%.3f", time);
  slow_info.append(tmp);
  slow_info.append("  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0\n");

  // slow info line #4 (currently we always log the USE statement)
  if (strlen(get_session()->get_schema())) {
    slow_info.append("use ");
    slow_info.append(get_session()->get_schema());
    slow_info.append(";\n");
  }

  // slow info line #5
  sprintf(tmp, "SET timestamp=%ld;\n", _end_date.msec() / 1000);
  slow_info.append(tmp);

  // slow info line #6
  slow_info.append(get_session()->get_top_stmt()
                       ? get_session()->get_top_stmt()->get_sql()
                       : IDLE_SQL);
  slow_info.append(";\n");

  Backend::instance()->get_slow_log_tool()->add_log_info_to_buff(
      slow_info.c_str(), time, get_session()->get_query_sql());
}
void Handler::stop_dbscale() {
  LOG_INFO("before dbscale exit, flush config to file\n");
  OptionParser *parser = OptionParser::instance();
  Backend::instance()->flush_config_to_file(parser->get_config_file().c_str(),
                                            false);
  ACE_Reactor::instance()->end_event_loop();
}

int TransactionTimerHandler::handle_timeout(const ACE_Time_Value &current_time,
                                            const void *act) {
  ACE_UNUSED_ARG(act);
  ACE_UNUSED_ARG(current_time);
  stop_schedule_timer();
  Session *session = handler->get_session();
  if (!session || (session && session->is_call_store_procedure())) {
    handler->set_trx_timing_stop(false);
    return 0;
  }
  ACE_Thread_Mutex *stop_mutex = handler->get_handler_trx_timing_stop_mutex();
  ACE_Guard<ACE_Thread_Mutex> guard(*stop_mutex);
  LOG_DEBUG("Transaction in session [%@] has been timeout\n", session);
  ExecutePlan *tmp_plan = session->get_top_plan();

  if (!tmp_plan) {
    handler->set_trx_timing_stop(true);
    session->killall_using_conns();
    session->reset_top_stmt_and_plan();
    session->rollback();
    handler->set_keep_conn(session->is_keeping_connection());
    return 0;
  }
  handler->set_trx_timing_stop(true);
  return 0;
}

void TransactionTimerHandler::start_schedule_timer() {
  auto transaction_executing_timeout =
      handler->get_session()
          ->get_session_option("transaction_executing_timeout")
          .ulong_val;
  if (!transaction_executing_timeout || is_running) return;
  this->reactor(ACE_Reactor::instance());
  this->timer_id = this->reactor()->schedule_timer(
      this, 0, ACE_Time_Value(transaction_executing_timeout),
      ACE_Time_Value(transaction_executing_timeout));
  is_running = true;
}
