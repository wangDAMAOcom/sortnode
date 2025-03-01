/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <basic.h>
#include <multiple.h>
#include <plan.h>
#include <session.h>
#include <socket_base_manager.h>

using namespace dbscale;
using namespace dbscale::sql;

extern bool is_implict_commit_sql(stmt_type type);

#ifndef DBSCALE_TEST_DISABLE
void MonitorPoint::end_timing_and_report() {
  if (session->monitor_flag) {
    end_timing();
    session->report_monitor_value(name.c_str(), cost);
  }
}
void MonitorPoint::start_timing() {
  if (session->monitor_flag) start_date = ACE_OS::gettimeofday();
}
void MonitorPoint::end_timing() {
  if (session->monitor_flag && start_date.sec() != 0) {
    end_date = ACE_OS::gettimeofday();
    cost = end_date - start_date;
  }
}
#endif

Session::Session()
    : client_socket(-1),
      read_socket_event(NULL),
      socket_base(NULL),
      internal_event(NULL),
      internal_base(NULL),
      session_state(SESSION_STATE_INIT),
      stream(NULL),
      thread_id(0),
      keep_conn(false),
      update_option(false),
      is_killed(false),
      in_killing(false),
      handler(NULL),
      top_plan(NULL),
      top_stmt(NULL),
      is_complex_stmt(false),
      is_simple_direct_stmt(false),
      transfer_rule(NULL),
      monitor_flag(false),
      is_call_sp(false),
      ddl_comment_sql(""),
      drop_current_schema(false),
      call_sp_nest(0),
      transaction_error(false),
      real_time_queries(0),
      global_table_real_time_queries(global_real_time_queries_conf),
      is_keepmaster(false),
      is_readonly(false),
      affect_server_type(AFFETCED_NON_SERVER),
      server_shutdown(false),
      has_var(false),
      in_lock(false),
      is_federated_session(false),
      fe_error(false),
      in_transaction(false),
      event_add_failed(false),
      executing_prepare(false),
      auto_commit_is_on(true),
      has_check_first_sql(false),
      stmt(NULL),
      is_query_command(false),
      is_load_data_local_command(false),
      wakeup_by_empty_leader(false),
      follower_wakeuped(false),
      cond_follower(follower_mutex),
      has_send_client_error_packet(false),
      view_updated(true),
      do_logining(false),
      is_super_user(false),
      do_swap_route(false),
      client_can_read(false),
      // is_cross_node_sql(false),
      // is_touch_partition_sql(false),
      is_admin_accepter(false),
      trans_conn(NULL),
      // session_load_balance_strategy(SESSION_LOAD_BALANCE_STRATEGY_DEFAULT),
      bev_client(NULL),
      bev_server(NULL),
      client_broken(false),
      server_broken(false),
      trans_state_ok(false)
#ifndef DBSCALE_TEST_DISABLE
      ,
      mp_svc("session_svc", this),
      mp_stmt_exec("exec_stmt", this)
#endif
{
  query_sql[SQL_LEN] = 0;
  query_sql_len = 0;
  in_transaction_consistent = false;
  trx_specify_read_only = false;
  in_explain = false;
  explain_node_id = 0;
  execution_profile.set_profile(false);
  mul_connection_num = 0;
  arrived_transaction_num = 0;
  non_swap_session = false;
  net_in = 0;
  net_out = 0;
  federated_finished_killed = false;
  is_need_swap_handler = true;
  prepare_kill_session = false;
  con_select = 0;
  con_insert = 0;
  con_update = 0;
  con_delete = 0;
  con_insert_select = 0;
  can_swap = false;
  user_priority = (UserPriorityValue)default_user_priority;
  skip_mutex_in_operation = false;
  mul_node_timeout = false;
  work_session = NULL;
  error_generated_by_dbscale = false;
  execute_plan_touch_partiton_nums = 0;
  times_move_data = 0;
  has_global_flush_read_lock = false;
  cur_stmt_execute_fail = false;
  refuse_version = 0;
  client_swap_timeout = false;
  cur_stmt_error_code = 0;
  cur_stmt_type = STMT_NON;
  audit_log_flag_local = AUDIT_LOG_MODE_MASK_NON;
  is_silence_ok_stmt = false;
  has_check_acl = false;
  silence_ok_stmt_id = 0;
  acl_version = 0;
  load_warning_count = 0;
  executing_spark = false;
  column_replace_num = 0;
  query_sql_replace_null_char.clear();
  is_info_schema_mirror_tb_reload_internal_session = false;
  ssl_stream = NULL;
  has_do_audit_log = false;
  execute_node_id_in_plan = 0;
  enable_session_packet_pool_local = enable_session_packet_pool;
  init_packet_pool_vec();
  need_abort_xa_transaction = false;
  select_lock_type = 0;
  enable_silent_execute_prepare_local = enable_silent_execute_prepare;
  got_error_er_must_change_password = false;
  flow_rate_control_query_cmd_total_count = 0;
  queue<pair<unsigned int, int> > tmp;
  swap(flow_rate_control_queue, tmp);  // reset the queue
  max_query_count_per_flow_control_gap_local =
      max_query_count_per_flow_control_gap;
  has_more_result = false;
  retl_tb_end_pos = 0;
  can_change_affected_rows = true;
  cancel_reset_stmt_level_variables_once = false;
  affected_rows = 0;
  stmt_auto_commit_int_value = 1;
  defined_lock_depth = 0;
  need_lock_kept_conn = false;
  has_fetch_node = false;
  max_alloc = 0;
  left_alloc = 0;
  client_scramble[0] = '\0';
  cur_auth_plugin = MYSQL_NATIVE_PASSWORD;
  cluster_xa_transaction_state = SESSION_XA_TRX_XA_NONE;
  cluster_xa_conn = NULL;
  cluster_xid = "";
  is_login_dbscale_read_only_user = false;
  is_surround_sql_with_trx_succeed = false;
  error_generated_by_force_stop_trx = false;
}

Session::~Session() {
  LOG_DEBUG("Session %@ %d is closing.\n", this, get_thread_id());
#ifndef DBSCALE_TEST_DISABLE
  mp_svc.end_timing_and_report();
  report_monitor_point_to_backend();
#endif

  if (internal_event) {
    event_del(internal_event);
    event_free(internal_event);
    internal_event = NULL;
  }
  if (internal_base) {
    delete internal_base;
    internal_base = NULL;
  }

  // The invoke of reset_top_stmt_and_plan should be done before the delete of
  // session by the outer logic.
  clean_explain_element_list();
  reset_warning_info();
  reset_load_warning_packet_list();
  flush_session_sql_cache(true);
  release_packet_pool_vec();
  set_lock_kept_conn(false);
  clean_defined_lock_conn();
  start_transaction_time = 0;
  Backend::instance()->giveback_left_alloc_size(left_alloc);

#ifdef HAVE_OPENSSL
  if (ssl_stream) {
    SSL *ssl = ssl_stream->ssl();
    SSL_set_quiet_shutdown(ssl, 1);
    SSL_shutdown(ssl);
    delete ssl_stream;
    ssl_stream = NULL;
  }
#endif
}

void Session::clean_explain_element_list() {
  list<ExplainElement *>::iterator it = explain_element_list.begin();
  for (; it != explain_element_list.end(); ++it) {
    delete *it;
  }
  explain_element_list.clear();
}

void Session::update_explain_info(list<ExecuteNode *> &node_list, int depth,
                                  int sub_plan_id) {
  ++depth;
  while (!node_list.empty()) {
    ExecuteNode *tmp_node = NULL;
    tmp_node = node_list.back();

    list<ExecuteNode *> node_list_2;
    tmp_node->get_children(node_list_2);
    update_explain_info(node_list_2, depth, sub_plan_id);

    string ori_node_name;
    ori_node_name.append(tmp_node->get_executenode_name());
    int node_id = tmp_node->get_node_id();
    if (node_id > 0) {
      char node_id_str[10];
      sprintf(node_id_str, "%d", node_id);
      ori_node_name.append(" [");
      ori_node_name.append(node_id_str);
      ori_node_name.append("]");
      if (tmp_node->plan->tmp_table_create_sql) {
        explain_sub_sql_list.push_back(
            pair<string, string>(string(node_id_str).append("-1"),
                                 tmp_node->plan->tmp_table_create_sql));
      }
      explain_sub_sql_list.push_back(
          pair<string, string>(string(node_id_str), tmp_node->plan->sql));
    }

    DataSpace *ds = NULL;
    string explain_sql = "";
    if (!strcmp(tmp_node->get_executenode_name(), "MySQLFetchNode") ||
        !strcmp(tmp_node->get_executenode_name(), "MySQLDirectExecuteNode")) {
      ds = tmp_node->get_dataspace();
      explain_sql = tmp_node->get_explain_sql();
    }
    ExplainElement *ei =
        new ExplainElement(depth, ori_node_name, ds, explain_sql, sub_plan_id,
                           node_id > 0 ? true : false);
    explain_element_list.push_front(ei);

    if (!strcmp(tmp_node->get_executenode_name(), "MySQLMulModifyNode")) {
      map<DataSpace *, const char *> *spaces_map = tmp_node->get_spaces_map();
      if (!spaces_map) continue;
      map<DataSpace *, const char *>::iterator it = (*spaces_map).begin();
      for (; it != (*spaces_map).end(); ++it) {
        ExplainElement *ei = new ExplainElement(depth, "", it->first,
                                                it->second, sub_plan_id, false);
        explain_element_list.push_back(ei);
      }
    }
    node_list.pop_back();
  }
}

void Session::clean_net_attribute() {
  sub_traditional_num();
  if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
    if (socket_base) {
      socket_base->desc_bind_client();
      if (get_session_state() == SESSION_STATE_WAITING_CLIENT)
        event_del(read_socket_event);
    }
  }
  if (read_socket_event) {
    event_free(read_socket_event);
  }

  socket_base = NULL;
  read_socket_event = NULL;
  if (stream) {
    stream->close();
    delete stream;
    stream = NULL;
  }
}

/*init the session state and read socket event.*/
void Session::init_session() {
  LOG_DEBUG("Session %d %@ is init with client socket %d.\n", get_thread_id(),
            this, client_socket);
#ifdef DEBUG
  ACE_ASSERT(client_socket != -1);
#endif

  set_session_state(SESSION_STATE_WORKING);
#ifndef DBSCALE_TEST_DISABLE
  mp_svc.start_timing();
#endif

  if (ACE_Reactor::instance()->reactor_event_loop_done()) return;
}

void Session::init_transmode_internal_base() {
  LOG_DEBUG("start init_transmode_internal_base in transparent mode.\n", this);
  bool is_centralized_cluster_without_auth =
      Backend::instance()->is_centralized_cluster(false);
  DataSource *single_datasource =
      Backend::instance()->get_auth_data_space()->get_data_source();
  if (!is_centralized_cluster_without_auth || !single_datasource ||
      single_datasource->get_work_state() != DATASOURCE_STATE_WORKING ||
      (single_datasource->get_is_in_failover() &&
       !single_datasource->can_get_connection()) ||
      single_datasource->get_master_server()->get_is_disable()) {
    LOG_ERROR(
        "In centralized cluster the datasource state is abnormal %@ %d %d\n",
        single_datasource,
        single_datasource ? single_datasource->get_work_state() : -1,
        single_datasource ? single_datasource->can_get_connection() : 0);
    throw Error("centralized cluster's datasource state is abnormal");
  }
  socket_base->add_bind_client();
  internal_base = new InternalEventBase(this);
  LOG_DEBUG("start create_trans_conn in transparent mode.\n", this);
  create_trans_conn();
  trans_conn->raw_connect_for_trans();
  set_nonblock(trans_conn->get_stream());
  // avoid negle and delayed ack 40ms BUG
  set_nodelay(trans_conn->get_stream());
  set_tcp_user_timeout_and_keepalive(trans_conn->get_stream(), trans_conn,
                                     tcp_multiple_times);
  LOG_DEBUG("start create bufferevent in transparent mode.\n", this);
  bev_client =
      bufferevent_socket_new(internal_base->get_event_base(),
                             get_client_socket(), BEV_OPT_CLOSE_ON_FREE);
  bev_server = bufferevent_socket_new(internal_base->get_event_base(),
                                      trans_conn->get_conn_trans_socket(),
                                      BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev_client, handle_client_readcb, NULL,
                    handle_libevent_errorcb, this);
  bufferevent_setcb(bev_server, handle_server_readcb, NULL,
                    handle_libevent_errorcb, this);
  bufferevent_setwatermark(bev_client, EV_READ | EV_WRITE, 0,
                           max_transparent_buffer_size);
  bufferevent_setwatermark(bev_server, EV_READ | EV_WRITE, 0,
                           max_transparent_buffer_size);
  bufferevent_enable(bev_client, EV_READ | EV_WRITE);
  bufferevent_enable(bev_server, EV_READ | EV_WRITE);
  trans_state_ok = true;
  Driver::get_driver()->add_trans_session(this);
  LOG_DEBUG("finish init_transmode_internal_base in transparent mode.\n", this);
}

void Session::stop_trans_event() {
  try {
    if (internal_base && trans_state_ok) {
      event_base_loopbreak(internal_base->get_event_base());
    }
  } catch (...) {
    LOG_DEBUG("Error occurs when stop_trans_event\n");
  }
  client_broken = true;
  server_broken = true;
}

void Session::init_internal_base() {
  SocketEventBaseManager *sm = SocketEventBaseManager::instance();
  socket_base = sm->get_one_event_base();
  LOG_DEBUG("Session %d %@ is assigned to base %@.\n", get_thread_id(), this,
            socket_base);
  if (transparent_mode && is_admin_accepter) {
    init_transmode_internal_base();
    return;
  }

  read_socket_event =
      event_new(socket_base->get_event_base(), client_socket,
                EV_TIMEOUT | EV_READ, handle_client_read_event, (void *)this);

  socket_base->add_bind_client();

  internal_base = new InternalEventBase(this);
  internal_event = event_new(internal_base->get_event_base(),
                             get_client_socket(), EV_TIMEOUT | EV_READ,
                             handle_client_for_session_read_event, this);
}

void Session::close_trans_conn() {
  try {
    if (!client_broken && bev_client && bufferevent_get_enabled(bev_client)) {
      bufferevent_setfd(bev_client, -1);
      evutil_closesocket(client_socket);
      bufferevent_free(bev_client);
    }
    bev_client = NULL;
    if (stream) {
      // stream->close();
      delete stream;
      stream = NULL;
    }
    if (!server_broken && bev_server && bufferevent_get_enabled(bev_server)) {
      bufferevent_setfd(bev_server, -1);
      evutil_closesocket(trans_conn->get_conn_trans_socket());
      bufferevent_free(bev_server);
    }
    bev_server = NULL;
    if (trans_conn) {
      // trans_conn->close();
      delete trans_conn;
      trans_conn = NULL;
    }
    client_broken = true;
    server_broken = true;
  } catch (exception &e) {
    LOG_ERROR("Got exception when close trans connection : [%s].\n", e.what());
  }
}

void Session::switch_out_to_wait_client_event() {
#ifdef DEBUG
  ACE_ASSERT(socket_base);
#endif
  LOG_DEBUG("Session %@ with id %d is switched out to wait client event.\n",
            this, get_thread_id());
  set_session_state(SESSION_STATE_WAITING_CLIENT);
  reset_all_top_stmt_and_plan();
  set_handler(NULL);

  this->set_do_swap_route(true);
  struct event *e = get_read_socket_event();
  socket_base->add_event_to_base(this, e);
}

void Session::switch_out_to_wait_server_event() {
#ifdef DEBUG
  ACE_ASSERT(need_swap_for_server_waiting());
#endif
  LOG_DEBUG("Session %@ with id %d is switched out to wait server event.\n",
            this, get_thread_id());
  set_session_state(SESSION_STATE_WAITING_SERVER);
  set_handler(NULL);
  map<evutil_socket_t, SocketEventBase *> tmp_map =
      server_base_map;  // use value copy, incase the server base map is changed
                        // during loop
  map<evutil_socket_t, SocketEventBase *>::iterator it = tmp_map.begin();
  evutil_socket_t socket_tmp;
  SocketEventBase *base_tmp = NULL;
  struct event *event_tmp = NULL;

  for (; it != tmp_map.end(); ++it) {
    socket_tmp = it->first;
    base_tmp = server_base_map[socket_tmp];
    event_tmp = server_event_map[socket_tmp];
#ifdef DEBUG
    LOG_DEBUG("Add server event %@ to base %@ with socket %d.\n", event_tmp,
              base_tmp, socket_tmp);
#endif
    base_tmp->add_event_to_base(this, event_tmp);
  }
}

void Session::report_monitor_point_to_backend() {
  Backend *backend = Backend::instance();
  map<string, ACE_Time_Value>::iterator it = monitor_point_map.begin();
  for (; it != monitor_point_map.end(); ++it) {
    backend->report_monitor_value(it->first.c_str(), it->second);
  }
  reset_monitor_point_map();
}

void Session::set_stream(Stream *s) {
  client_socket = s->get_handle();
  stream = new Stream(client_socket);
  LOG_DEBUG("Session %@ init the socket with %d.\n", this, client_socket);
}

void Session::set_top_plan(ExecutePlan *plan) {
#ifdef DEBUG
  LOG_DEBUG("Session %@ set top_plan %@.\n", this, plan);
#endif
  if (top_plan) {
    nest_top_plan.push_back(plan);
    return;
  }
  top_plan = plan;
}

void Session::reset_top_stmt() {
  Statement *tmp_stmt = NULL;
  if (nest_top_stmt.empty()) {
    tmp_stmt = top_stmt;
    top_stmt = NULL;
  } else {
    tmp_stmt = nest_top_stmt.back();
    nest_top_stmt.pop_back();
  }
  if (tmp_stmt) {
    tmp_stmt->free_resource();
    /*one simple plan may generate mul stmts, such as com_query_prepare.
     *then Session will just save the first stmt, and new other stmt.
     *so if the stmt is simple, we just save the stmt we have saved before or
     *the stmt is null*/
    if (get_is_simple_direct_stmt()) {
      if (stmt == tmp_stmt || !stmt) {
        tmp_stmt->re_init();
        stmt = tmp_stmt;
      } else {
        delete tmp_stmt;
      }
    } else {
      /*if the stmt is complex, we can clean the saved stmt only when we meet
       * the stmt we saved */
      if (stmt == tmp_stmt) stmt = NULL;
      delete tmp_stmt;
    }
    tmp_stmt = NULL;
  }
}

void Session::reset_top_stmt_and_plan() {
  /*Only reset the last nest top_plan (stmt) or top_plan (stmt)*/
  ExecutePlan *tmp_plan = NULL;
  Statement *tmp_stmt = NULL;
  if (nest_top_plan.empty()) {
    tmp_plan = top_plan;
    top_plan = NULL;
  } else {
    tmp_plan = nest_top_plan.back();
    nest_top_plan.pop_back();
  }
  if (nest_top_stmt.empty()) {
    tmp_stmt = top_stmt;
    top_stmt = NULL;
  } else {
    tmp_stmt = nest_top_stmt.back();
    nest_top_stmt.pop_back();
  }
#ifdef DEBUG
  LOG_DEBUG("Session %@ reset top stmt [%@] and plan [%@]\n", this, tmp_stmt,
            tmp_plan);
#endif
  if (tmp_plan) tmp_plan->clean_plan();
  if (tmp_stmt) {
    tmp_stmt->free_resource();
    /*one simple plan may generate mul stmts, such as com_query_prepare.
     *then Session will just save the first stmt, and new other stmt.
     *so if the stmt is simple, we just save the stmt we have saved before or
     *the stmt is null*/
    if (get_is_simple_direct_stmt()) {
      if (stmt == tmp_stmt || !stmt) {
        tmp_stmt->re_init();
        stmt = tmp_stmt;
      } else {
        delete tmp_stmt;
        tmp_stmt = NULL;
      }
    } else {
      /*if the stmt is complex, we can clean the saved stmt only when we meet
       * the stmt we saved */
      if (stmt == tmp_stmt) stmt = NULL;
      delete tmp_stmt;
      tmp_stmt = NULL;
    }
    tmp_stmt = NULL;
  }
  if (tmp_plan) {
    delete tmp_plan;
    tmp_plan = NULL;
  }
}

void Session::reset_all_top_stmt_and_plan() {
#ifdef DEBUG
  LOG_DEBUG("Session %@ reset all top stmt and plan\n", this);
#endif
  if (top_plan) {
    top_plan->clean_plan(); /*Ensure that the plan is clean before deleted.*/
  }
  list<ExecutePlan *>::iterator it = nest_top_plan.begin();
  for (; it != nest_top_plan.end(); ++it) {
    (*it)->clean_plan();
  }
  if (top_stmt) {
    top_stmt->free_resource();
    delete top_stmt;
    top_stmt = NULL;
  }
  list<Statement *>::iterator it2 = nest_top_stmt.begin();
  for (; it2 != nest_top_stmt.end(); ++it2) {
    (*it2)->free_resource();
    delete (*it2);
    (*it2) = NULL;
  }
  nest_top_stmt.clear();
  if (top_plan) {
    delete top_plan;
    top_plan = NULL;
  }
  it = nest_top_plan.begin();
  for (; it != nest_top_plan.end(); ++it) {
    delete (*it);
    (*it) = NULL;
  }

  nest_top_plan.clear();
}

bool Session::is_may_backend_exec_swap_able() {
  if (!enable_session_swap || !enable_session_swap_during_execution)
    return false;
  if (!is_need_swap_handler) return false;
  if (!get_top_stmt()) return false;
  if (!nest_top_plan.empty() ||
      !nest_top_stmt.empty())  // Does not support nest plan execution.
    return false;
  if (is_call_store_procedure() ||
      get_top_stmt()->get_is_may_cross_node_join_related() ||
      get_top_stmt()->is_union_table_sub() || get_is_complex_stmt() ||
      !get_top_stmt()->is_can_swap_type())
    return false;

  if (contains_odbc_conn()) return false;
  if (!get_can_swap()) return false;
  return true;
}

bool Session::add_traditional_num() {
  if (enable_session_swap && !non_swap_session && socket_base) {
    /*SocketEventBaseManager::clean_up_unhandled_session will first remove the
     * socket_base from session, then delete session, so here the socket_base
     * may be NULL.*/
    non_swap_session = true;
    socket_base->increase_traditional_num();
    return true;
  }
  return false;
}

void Session::sub_traditional_num() {
  if (non_swap_session && socket_base) {
    non_swap_session = false;
    socket_base->decrease_traditional_num();
  }
  event_add_failed = false;
}

void Session::set_handler(Handler *h) {
  handler = h;
  ExecutePlan *tmp_plan = get_top_plan();
  if (tmp_plan) {
    tmp_plan->handler = h;
    ExecuteNode *start_node = tmp_plan->start_node;
    start_node->set_handler(h);
#ifdef DEBUG
    LOG_DEBUG("Reset the handler of top_plan for session %@.\n", this);
#endif
  }
}

void Session::update_view_map() {
  set_view_updated(false);
  Backend *backend = Backend::instance();
  backend->update_view_map(view_map);
}

void Session::add_table_info_to_delete(const char *_schema_name,
                                       const char *_table_name) {
  string schema_name(_schema_name);
  string table_name(_table_name);
#ifdef DEBUG
  LOG_DEBUG("Session [%@] add_table_info_to_delete [%s.%s]\n", this,
            _schema_name, _table_name);
#endif
  table_info_to_delete_pair_vec.push_back(
      pair<string, string>(schema_name, table_name));
}
void Session::add_db_table_info_to_delete(const char *_schema_name) {
  string schema_name(_schema_name);
#ifdef DEBUG
  LOG_DEBUG("Session [%@] add_db_table_info_to_delete [%s]\n", this,
            _schema_name);
#endif
  table_info_to_delete_pair_vec.push_back(
      pair<string, string>(schema_name, string(" ")));
}
void Session::apply_table_info_to_delete() {
  if (!table_info_to_delete_pair_vec.empty()) {
    TableInfoCollection *tic = TableInfoCollection::instance();
#ifdef DEBUG
    LOG_DEBUG("Session [%@] start apply_table_info_to_delete\n", this);
#endif
    tic->delete_table_info_pair_vec(table_info_to_delete_pair_vec);
#ifdef DEBUG
    LOG_DEBUG("Session [%@] finish apply_table_info_to_delete\n", this);
#endif
#ifndef CLOSE_MULTIPLE
    if (multiple_mode) {
      bool is_master = true;
      MultipleManager *mul = MultipleManager::instance();
      is_master = mul->get_is_cluster_master();
      if (is_master) {
        MultipleManager::instance()->acquire_start_config_lock(
            get_zk_start_config_lock_extra_info());
        try {
          Backend::instance()->flush_config_to_zoo();
          MultipleManager *mul = MultipleManager::instance();
          mul->pub_ka_reset_table_info(table_info_to_delete_pair_vec);
        } catch (...) {
          MultipleManager::instance()->release_start_config_lock(
              start_config_lock_extra_info);
          LOG_ERROR("Fail to apply table info to delete to slaves.\n");
          throw;
        }
        MultipleManager::instance()->release_start_config_lock(
            start_config_lock_extra_info);
      }
    }
#endif
    table_info_to_delete_pair_vec.clear();
  }
}
bool Session::contains_using_connection(DataServer *server, int connection_id) {
  bool ret = false;
  using_conns_mutex.acquire();
  set<Connection *>::iterator it = using_conns.begin();
  for (; it != using_conns.end(); ++it) {
    if ((*it)->get_server() == server &&
        (*it)->get_thread_id() == (unsigned int)connection_id) {
      ret = true;
      break;
    }
  }
  using_conns_mutex.release();
  return ret;
}
void Session::reset_cluster_xa_transaction(bool reset_cluster_xa_conn_null) {
  LOG_DEBUG(
      "change xa state from %s to none.\n",
      cluster_xa_transaction_to_string(cluster_xa_transaction_state).c_str());
  cluster_xa_transaction_state = SESSION_XA_TRX_XA_NONE;
  if (cluster_xa_conn) {
    cluster_xa_conn->set_cluster_xa_session(NULL);
  }
  if (reset_cluster_xa_conn_null) cluster_xa_conn = NULL;
  cluster_xid = "";
}
bool Session::check_for_transaction() {
  if (is_in_transaction()) return true;
  if (is_in_cluster_xa_transaction()) return true;
  if (!get_auto_commit_is_on()) {
    Statement *statement = get_top_stmt();
    if (!statement) {
      return true;
    }
    if (!is_implict_commit_sql(statement->get_stmt_node()->type) ||
        statement->get_stmt_node()->type == STMT_START_TRANSACTION) {
      return true;
    }
  }
  return false;
}

void Session::flush_session_sql_cache(bool force) {
  if (session_cache_sqls == 0) return;

  string all_sql_string;
  if (force) {
    vector<string>::iterator it = session_sql_cache.begin();
    if (session_sql_cache.empty()) return;
    for (; it != session_sql_cache.end(); ++it) {
      all_sql_string.append("The SQL is : ");
      all_sql_string.append(*it);
      if (sql_is_cross_node_join.count(*it)) {
        all_sql_string.append("   and it is cross node join.");
      }
      all_sql_string.append("\n");
    }
    LOG_INFO("Session [%@] %s\n", this, all_sql_string.c_str());
    session_sql_cache.clear();
  } else {
    ACE_Log_Priority log_level = LM_DEBUG;
    OptionParser::instance()->get_option_value(&log_level, "log_level");
    if (log_level != LM_INFO || session_cache_sqls == 0) {
      session_sql_cache.clear();
      sql_is_cross_node_join.clear();
      return;
    }
    if ((unsigned int)session_cache_sqls <= session_sql_cache.size())
      flush_session_sql_cache(true);
  }
}

string Session::get_column_replace_string(string key) {
  if (key.length() < 60) return key;
  string ret;
  column_replace_map_mutex.acquire();
  if (column_replace_map.count(key)) {
    ret = column_replace_map[key];
  } else {
    char new_col_name[64];
    sprintf(new_col_name, "dbscale_r_col%d", column_replace_num);
    ++column_replace_num;
    column_replace_map[key] = new_col_name;
    ret = new_col_name;
  }
  column_replace_map_mutex.release();
  return ret;
}

string Session::get_zk_start_config_lock_extra_info() {
  ACE_Date_Time now;
  long cur_year = now.year();
  long cur_month = now.month();
  long cur_day = now.day();
  long cur_hour = now.hour();
  long cur_minute = now.minute();
  long cur_second = now.second();
  long cur_microsec = now.microsec();
  char tmp[128];
  sprintf(tmp, "_session_%d_%02ld%02ld%02ld_%02ld:%02ld:%02ld:%02ld",
          get_thread_id(), cur_year % 100, cur_month, cur_day, cur_hour,
          cur_minute, cur_second, cur_microsec);
  start_config_lock_extra_info = tmp;
  return start_config_lock_extra_info;
}

string Session::get_spark_job_id() {
  Backend *backend = Backend::instance();
  string job_id;
  job_id.append(SSTR(backend->get_cluster_id()));
  job_id.append("_");
  job_id.append(SSTR(backend->get_dbscale_start_time().sec()));
  job_id.append("_");
  job_id.append(SSTR(thread_id));
  return job_id;
}
void Session::get_local_dataservers_rep_relation(
    ConfigSourceRepResource &dataservers_rep_relation) {
  Backend *backend = Backend::instance();
  unsigned int version = local_dataservers_rep_relation.get_version();
  if (!backend->check_dataservers_rep_updates(version)) {
    backend->get_dataservers_rep_relation(local_dataservers_rep_relation);
  }
  dataservers_rep_relation = local_dataservers_rep_relation;
}
bool Session::is_dataspace_cover_session_level(DataSpace *ds1, DataSpace *ds2) {
  Backend *backend = Backend::instance();
  unsigned int version = local_dataservers_rep_relation.get_version();
  if (!backend->check_dataservers_rep_updates(version)) {
    backend->get_dataservers_rep_relation(local_dataservers_rep_relation);
  }
  if (!is_share_same_server(ds1, ds2) && global_table_need_cross_node_join() &&
      ds2->get_data_source() &&
      ds2->get_data_source()->get_data_source_type() ==
          DATASOURCE_TYPE_REPLICATION) {
    return false;
  }
  return is_dataspace_cover(ds1, ds2, local_dataservers_rep_relation);
}

void Session::set_global_time_queries(bool is_read_sql) {
  if (is_read_sql) {
    if (global_table_real_time_queries > 0) {
      --global_table_real_time_queries;
      global_table_can_merge = false;
    } else
      global_table_can_merge = true;
  } else {
    global_table_can_merge = true;
    global_table_real_time_queries = global_real_time_queries_conf;
  }
}

bool Session::global_table_need_cross_node_join() {
  return !global_table_can_merge;
}

bool Session::is_datasource_cover_session_level(DataSource *ds1,
                                                DataSource *ds2) {
  if (ds1 == ds2) return true;
  Backend *backend = Backend::instance();
  unsigned int version = local_dataservers_rep_relation.get_version();
  if (!backend->check_dataservers_rep_updates(version)) {
    backend->get_dataservers_rep_relation(local_dataservers_rep_relation);
  }
  return is_slave(ds1, ds2, local_dataservers_rep_relation);
}

void PacketPool::get_free_packet_from_pool(
    int count, list<Packet *, StaticAllocator<Packet *> > *ret_list) {
  int get_count = 0;
  packet_pool_lock.acquire();
  while (!list_out->empty() || !list_in->empty()) {
    list<Packet *, StaticAllocator<Packet *> >::iterator it = list_out->begin();
    while (it != list_out->end()) {
      ret_list->push_back(*it);
      packet_pool_cur_packet_count--;
      ++get_count;
      ++it;
      if (get_count == count) {
        break;
      }
    }
    if (get_count) list_out->erase(list_out->begin(), it);
    if (get_count == count) break;
    if (list_in->empty()) break;
    list<Packet *, StaticAllocator<Packet *> > *tmp = list_out;
    list_out = list_in;
    list_in = tmp;
  }
#ifdef DEBUG
  if (get_count > 0)
    LOG_DEBUG(
        "get packet from packet pool, current packet count in pool [%Q] is "
        "[%d]\n",
        pool_id, packet_pool_cur_packet_count);
#endif
  packet_pool_lock.release();
}

void PacketPool::put_free_packet_back_to_pool(
    int &count, list<Packet *, StaticAllocator<Packet *> > &pkt_list) {
  packet_pool_lock.acquire();
  if (packet_pool_cur_packet_count < packet_pool_max_packet_count_local) {
    list<Packet *, StaticAllocator<Packet *> >::iterator it = pkt_list.begin();
    for (; it != pkt_list.end(); ++it) {
      list_in->push_back(*it);
    }
    pkt_list.clear();

    packet_pool_cur_packet_count += count;
    count = 0;
    packet_pool_lock.release();
#ifdef DEBUG
    LOG_DEBUG(
        "add one packet to packet pool, current packet count in pool [%Q] is "
        "[%d]\n",
        pool_id, packet_pool_cur_packet_count);
#endif
  } else {
    packet_pool_lock.release();
#ifdef DEBUG
    LOG_DEBUG(
        "when put free packet back to pool [%Q], it is full, delete the packet "
        "directly\n",
        pool_id);
#endif
    list<Packet *, StaticAllocator<Packet *> >::iterator it = pkt_list.begin();
    for (; it != pkt_list.end(); ++it) {
      delete *it;
    }
    pkt_list.clear();
    count = 0;
  }
}

void PacketPool::add_packet_to_pool_no_lock(Packet *packet) {
  list_in->push_back(packet);
  ++packet_pool_cur_packet_count;
#ifdef DEBUG
  LOG_DEBUG(
      "add one packet to packet pool, current packet count in pool [%Q] is "
      "[%d]\n",
      pool_id, packet_pool_cur_packet_count);
#endif
}

void PacketPool::release_all_packet_in_packet_pool() {
#ifdef DEBUG
  LOG_DEBUG("try to release all packets in packet pool [%Q]\n", pool_id);
#endif
  packet_pool_lock.acquire();
  Backend::instance()->put_free_packet_back_to_backend_pool(packet_list_one);
  Backend::instance()->put_free_packet_back_to_backend_pool(packet_list_two);

  while (!packet_list_one.empty()) {
    Packet *p = packet_list_one.front();
    packet_list_one.pop_front();
    delete p;
  }
  while (!packet_list_two.empty()) {
    Packet *p = packet_list_two.front();
    packet_list_two.pop_front();
    delete p;
  }
  packet_pool_cur_packet_count = 0;
  packet_pool_lock.release();
}

void Session::init_packet_pool_vec() {
  if (!enable_session_packet_pool_local) {
    packet_pool_count = 0;
    packet_pool_init_packet_count_local = 0;
    packet_pool_packet_bundle_local = 0;
    return;
  }
  packet_pool_count = session_packet_pool_count;
  packet_pool_init_packet_count_local = session_packet_pool_init_packet_count;
  packet_pool_packet_bundle_local = session_packet_pool_packet_bundle;
  for (int i = 0; i < packet_pool_count; ++i) {
    PacketPool *packet_pool = new PacketPool(i);
    // get packet from backend for each pool, if not enough, alloc the
    // insufficient packets
    int get_count = Backend::instance()->fulfill_packet_pool_from_backend_pool(
        packet_pool, packet_pool_init_packet_count_local);
    size_t insufficient_count = packet_pool_init_packet_count_local - get_count;
    for (size_t j = 0; j < insufficient_count; ++j) {
      Packet *packet = Backend::instance()->get_new_packet(row_packet_size);
      packet_pool->add_packet_to_pool_no_lock(packet);
    }
    packet_pool_vec.push_back(packet_pool);
  }
}

void Session::release_packet_pool_vec() {
  for (size_t i = 0; i < (size_t)packet_pool_count; ++i) {
    delete packet_pool_vec[i];
  }
  packet_pool_vec.clear();
}

void Session::get_free_packet_from_pool(
    size_t id, int count,
    list<Packet *, StaticAllocator<Packet *> > *ret_list) {
#ifdef DEBUG
  LOG_DEBUG("get packet from packet pool [%Q]\n", id);
#endif
  // we assume ret_list is empty before use
  // we do not modify packet_pool_vec, so no need mutex here
  packet_pool_vec[id]->get_free_packet_from_pool(count, ret_list);
}

void Session::put_free_packet_back_to_pool(
    size_t id, int &count,
    list<Packet *, StaticAllocator<Packet *> > &pkt_list) {
  packet_pool_vec[id]->put_free_packet_back_to_pool(count, pkt_list);
}

// return true means the running xa transaction need to be aborted.
bool Session::add_running_xa_map(unsigned long xid, RunningXAType type,
                                 map<DataSpace *, Connection *> *kept_map) {
  bool ret = false;
  running_xa_map_mutex.acquire();
  running_xa_map[xid] = type;
  LOG_DEBUG("add_running_xa_map xid [%d] with type %d.\n", xid, (int)type);
  if (type == RUNNING_XA_PREPARE && kept_map) {
    map<DataSpace *, Connection *>::iterator it = kept_map->begin();
    for (; it != kept_map->end(); ++it) {
      if (it->first->get_data_source()) {
        running_source_set.insert(it->first->get_data_source());
      }
    }
  }
  if (need_abort_xa_transaction) {
    running_xa_map[xid] = RUNNING_XA_ROLLBACK;
    LOG_INFO(
        "Session %@ with xid[%d] find need_abort_xa_transaction with type %d "
        "in Session::add_running_xa_map.\n",
        this, xid, (int)type);
    ret = true;
  }
  running_xa_map_mutex.release();
  return ret;
}

#ifdef DEBUG
void print_flow_rate_control_queue_content(
    queue<pair<unsigned int, int> > &flow_rate_control_queue) {
  queue<pair<unsigned int, int> > tmp = flow_rate_control_queue;
  LOG_DEBUG("start print flow_rate_control_queue content\n");
  while (!tmp.empty()) {
    pair<unsigned int, int> a = tmp.front();
    LOG_DEBUG("%d,%d\n", a.first, a.second);
    tmp.pop();
  }
  LOG_DEBUG("end print flow_rate_control_queue content\n");
}
#endif

bool Session::query_command_flow_rate_control_ok() {
  unsigned int id = get_cur_login_time();  // seconds since this session login
  if (!flow_rate_control_queue.empty()) {
    pair<unsigned int, int> head = flow_rate_control_queue.front();
    unsigned int head_id = head.first;
    while (id - head_id >
           (unsigned int)flow_control_gap) {  // clean all records before
                                              // flow_control_gap (in seconds)
      flow_rate_control_query_cmd_total_count -= head.second;
      flow_rate_control_queue.pop();
      if (flow_rate_control_queue.empty()) {
        break;
      }
      head = flow_rate_control_queue.front();
      head_id = head.first;
    }
  }
  if (!flow_rate_control_queue.empty()) {
    if (1 + flow_rate_control_query_cmd_total_count >
        max_query_count_per_flow_control_gap) {
      return false;
    } else {
      if (flow_rate_control_queue.back().first == id) {
        ++(flow_rate_control_queue.back().second);
        ++flow_rate_control_query_cmd_total_count;
      } else {
        pair<int, int> p(id, 1);
        flow_rate_control_queue.push(p);
        ++flow_rate_control_query_cmd_total_count;
      }
#ifdef DEBUG
      print_flow_rate_control_queue_content(flow_rate_control_queue);
#endif
      return true;
    }
  } else {
#ifdef DEBUG
    ACE_ASSERT(flow_rate_control_query_cmd_total_count == 0);
#endif
    pair<int, int> p(id, 1);
    flow_rate_control_queue.push(p);
    ++flow_rate_control_query_cmd_total_count;
#ifdef DEBUG
    print_flow_rate_control_queue_content(flow_rate_control_queue);
#endif
    return true;
  }
}

map<unsigned long, RunningXAType> Session::get_running_xa_map(
    DataSource *fail_source) {
  map<unsigned long, RunningXAType> ret;
  running_xa_map_mutex.acquire();
  LOG_DEBUG("Session::get_running_xa_map for session %@ with fail_source %s.\n",
            this, fail_source->get_name());
  if (running_source_set.count(fail_source)) {
    unsigned long x_id = get_xa_id();
    if (running_xa_map.count(x_id) &&
        running_xa_map[x_id] == RUNNING_XA_PREPARE) {
      need_abort_xa_transaction = true;
      running_xa_map[x_id] = RUNNING_XA_ROLLBACK;
      LOG_INFO(
          "Session::get_running_xa_map adjust xid[%d] to rollback flag for "
          "fail_source [%s].\n",
          x_id, fail_source->get_name());
    } else {
      ret = running_xa_map;
      LOG_INFO(
          "Session::get_running_xa_map collect xid[%d] with [%d] flag for "
          "fail_source [%s].\n",
          x_id, (int)running_xa_map[x_id], fail_source->get_name());
    }
  }
  running_xa_map_mutex.release();
  return ret;
}

void Session::add_table_without_given_schema(DataSpace *space) {
  if (space->is_partitioned()) {
    PartitionedTable *table = (PartitionedTable *)space;
    unsigned int partition_num = table->get_real_partition_num();
    for (unsigned int i = 0; i < partition_num; ++i) {
      DataSpace *space = table->get_partition(i);
      table_without_given_schema.insert(space);
    }
  } else {
    table_without_given_schema.insert(space);
  }
}

void Session::set_bridge_mark_sql(const char *sql, size_t _retl_tb_end_pos) {
  bridge_mark_sql = sql;
  retl_tb_end_pos = _retl_tb_end_pos;
  LOG_DEBUG(
      "Set session bridge_mark_sql: [%@:%s], with retl table end pos %Q\n",
      this, sql, retl_tb_end_pos);
}
void Session::adjust_retl_mark_tb_name(DataSpace *space, string &sql) {
  unsigned int virtual_machine_id = space->get_virtual_machine_id();
  if (virtual_machine_id != 0) {
    string tb_name = "retl_mark";
    Backend::instance()->get_shard_schema_string(
        virtual_machine_id, space->get_partition_id(), tb_name);
    sql.replace(retl_tb_end_pos - 9, 9, tb_name);
  }
}

void Session::set_affected_rows(int64_t affected_rows) {
  if (!get_can_change_affected_rows()) return;
  LOG_DEBUG("Set affected rows orginal: [%q] to: [%q]\n", this->affected_rows,
            affected_rows);
  this->affected_rows = affected_rows;
}

void Session::force_close_stream() {
  if (stream) stream->close();
}

void Session::set_defined_lock_kept_conn(DataSpace *ds, Connection *conn) {
  defined_lock_mutex.acquire();
  if (defined_lock_kept_conn.count(ds)) {
    defined_lock_kept_conn[ds]->reset_cluster_xa_session_before_to_dead();
    defined_lock_kept_conn[ds]->get_pool()->add_back_to_dead(
        defined_lock_kept_conn[ds]);
  }
  defined_lock_kept_conn[ds] = conn;
  defined_lock_mutex.release();
  LOG_DEBUG(
      "Session [%@] defined lock keep dataspace [%@] the connection [%@].\n",
      this, ds, conn);
}

void Session::clean_defined_lock_conn() {
  ACE_Guard<ACE_Thread_Mutex> guard(defined_lock_mutex);
  map<DataSpace *, Connection *>::iterator it;
  while (defined_lock_kept_conn.size() > 0) {
    it = defined_lock_kept_conn.begin();
    if (lock_conn_add_back_to_dead()) {
      remove_using_conn(it->second);
      it->second->get_pool()->add_back_to_dead(it->second);
    }
    defined_lock_kept_conn.erase(it);
  }
  defined_lock_space_kept_conn.clear();
}

bool Session::lock_conn_add_back_to_dead() {
  if (!need_lock_kept_conn && defined_lock_depth > 0) return true;
  return false;
}

bool Session::check_lock_conn_in_kept_conn(DataSpace *ds) {
  ACE_Guard<ACE_Thread_Mutex> guard(defined_lock_mutex);
  if (defined_lock_kept_conn.count(ds)) return true;
  check_defined_lock_kept_conn_for_same_server(ds);
  if (defined_lock_space_kept_conn.count(ds)) return true;
  return false;
}

Connection *Session::check_defined_lock_kept_conn_for_same_server(
    DataSpace *dataspace) {
  if (!dataspace) {
    LOG_DEBUG(
        "fail to do check_defined_lock_kept_conn_for_same_server because no "
        "dataspace\n");
    return NULL;
  }
  DataSource *source = dataspace->get_data_source();
  if (!source) {
    LOG_WARN(
        "fail to do check_defined_lock_kept_conn_for_same_server because no "
        "datasource found for dataspace %@\n",
        dataspace);
    return NULL;
  }
  if (source->get_data_source_type() == DATASOURCE_TYPE_LOAD_BALANCE) {
    LOG_WARN(
        "fail to do check_defined_lock_kept_conn_for_same_server because "
        "datasource "
        "for dataspace %@ is type of LOAD_BALANCE\n",
        dataspace);
    return NULL;
  }
  if (source->get_data_source_type() == DATASOURCE_TYPE_ODBC) {
    LOG_WARN(
        "fail to do check_defined_lock_kept_conn_for_same_server because "
        "datasource "
        "for dataspace %@ is type of ODBC\n",
        dataspace);
    return NULL;
  }
  if (defined_lock_kept_conn.empty()) return NULL;
  Connection *ret = NULL;
  map<DataSpace *, Connection *>::iterator it;
  for (it = defined_lock_kept_conn.begin(); it != defined_lock_kept_conn.end();
       it++) {
    if (dataspace->get_virtual_machine_id() ==
        it->first->get_virtual_machine_id()) {
      if (is_share_same_server(
              source->get_master_server(),
              it->first->get_data_source()->get_master_server())) {
        defined_lock_space_kept_conn[dataspace] = it->first;
        ret = it->second;
        break;
      }
    }
  }
  if (ret)
    LOG_DEBUG(
        "Find conn %@ which is belonging to the same server of source %s%@.\n",
        ret, source->get_name(), source);
  return ret;
}

bool Session::check_dataspace_is_catalog_space(DataSpace *dataspace) {
  DataSource *source = dataspace->get_data_source();
  if (!source) {
    LOG_WARN(
        "fail to do check_dataspace_is_catalog_space because no datasource "
        "found for dataspace %@\n",
        dataspace);
    return false;
  }
  Backend *backend = Backend::instance();
  Catalog *catalog = backend->get_default_catalog();
  const char *catalog_name = catalog->get_data_source()->get_name();
  const char *ds_name = source->get_name();
  if (!strcasecmp(catalog_name, ds_name)) return true;
  return false;
}

void Session::remove_defined_lock_kept_conn(DataSpace *dataspace) {
  ACE_Guard<ACE_Thread_Mutex> guard(defined_lock_mutex);
  DataSpace *real_dataspace = NULL;
  if (defined_lock_kept_conn.count(dataspace))
    real_dataspace = dataspace;
  else if (defined_lock_space_kept_conn.count(dataspace))
    real_dataspace = defined_lock_space_kept_conn[dataspace];

  map<DataSpace *, DataSpace *>::iterator it =
      defined_lock_space_kept_conn.begin();
  for (; it != defined_lock_space_kept_conn.end();) {
    if (it->second == real_dataspace) {
      defined_lock_space_kept_conn.erase(it++);
    } else
      it++;
  }
  if (defined_lock_kept_conn.count(real_dataspace)) {
    LOG_DEBUG("Session [%@] remove kept_connection [%@].\n", this,
              defined_lock_kept_conn[real_dataspace]);
    defined_lock_kept_conn.erase(real_dataspace);
  }
}

Connection *Session::get_defined_lock_kept_conn_real(DataSpace *dataspace) {
  Connection *conn = NULL;
  if (defined_lock_kept_conn.count(dataspace)) {
    LOG_DEBUG("Get kept connection [%@] from session [%@].\n",
              defined_lock_kept_conn[dataspace], this);
    conn = defined_lock_kept_conn[dataspace];

  } else {
    if (defined_lock_space_kept_conn.count(dataspace)) {
      conn = defined_lock_kept_conn[defined_lock_space_kept_conn[dataspace]];
      LOG_DEBUG("Get kept connection.\n");
    } else {
      conn = check_defined_lock_kept_conn_for_same_server(dataspace);
    }
  }
  return conn;
}

Connection *Session::get_defined_lock_kept_conn(DataSpace *dataspace) {
  ACE_Guard<ACE_Thread_Mutex> guard(defined_lock_mutex);
  Connection *conn = NULL;
  conn = get_defined_lock_kept_conn_real(dataspace);
  if (!conn) return NULL;

  if (conn->get_resource_status() == RESOURCE_STATUS_DEAD) {
    LOG_ERROR("Kept connection has been killed!\n");
    throw HandlerError("Connection has been killed!");
  }

  return conn;
}

int Session::check_ping_and_change_master() {
  map<DataSpace *, Connection *> tmp_map = get_defined_lock_kept_conn();
  map<DataSpace *, Connection *>::iterator it;
  if (tmp_map.size() > 1) {
    LOG_WARN(
        "Multiple dataspaces are not supported, session will be closed.\n");
    return 1;
  }
  for (it = tmp_map.begin(); it != tmp_map.end(); ++it) {
    const char *server_name =
        it->first->get_data_source()->get_master_server()->get_name();
    Connection *tmp_conn = it->second;
    if (tmp_conn) {
      if (strcasecmp(server_name, tmp_conn->get_server()->get_name())) {
        LOG_WARN("The catalog source change master, session will be closed.\n");
        return 2;
      }
      if (tmp_conn->ping()) {
        LOG_WARN("The new connection is broken, session will be closed.\n");
        return 3;
      }
    } else {
      LOG_WARN("The new connection is NULL, session will be closed.\n");
      return 3;
    }
  }
  return 0;
}

bool Session::get_alloc(unsigned long long size) {
  Backend *backend = Backend::instance();
  ACE_Guard<ACE_Thread_Mutex> guard(left_alloc_mutex);
  if (left_alloc < size) {
    int retry = session_select_retry_alloc_times;
    unsigned long long alloc_size = session_select_once_alloc_size * 1024;
    while (retry > 0) {
      if (backend->get_alloc_size(alloc_size)) {
        max_alloc += alloc_size;
        left_alloc += alloc_size;
      }
      if (left_alloc >= size) break;
      --retry;
      ACE_OS::sleep(1);
    }
    if (left_alloc < size) {
      LOG_WARN(
          "session %@ has no enough memory,session::get_alloc(), max_alloc is "
          "%Q B, left_alloc is %Q B, want to alloc %Q B.\n",
          this, max_alloc, left_alloc, size);
      return false;
    }
  }
  left_alloc -= size;
#ifdef DEBUG
  LOG_DEBUG(
      "session::get_alloc(), max_alloc is %Q B, left_alloc is "
      "%Q B.\n",
      max_alloc, left_alloc);
#endif
  return true;
}

void Session::giveback_left_alloc(unsigned long long size) {
  if (size) {
    ACE_Guard<ACE_Thread_Mutex> guard(left_alloc_mutex);
    left_alloc += size;
#ifdef DEBUG
    LOG_DEBUG(
        "session::giveback_left_alloc(), max_alloc is %u KB, "
        "left_alloc is %Q B.\n",
        max_alloc, left_alloc);
#endif
  }
}

void Session::giveback_left_alloc_to_backend() {
  Backend *backend = Backend::instance();
  unsigned long long old_left_alloc = 0;
  {
    ACE_Guard<ACE_Thread_Mutex> guard(left_alloc_mutex);
#ifdef DEBUG
    LOG_DEBUG(
        "session %@ start giveback_left_alloc_to_backend, max_alloc is "
        "%Q B, left_alloc is %Q B.\n",
        this, max_alloc, left_alloc);
#endif
    if (left_alloc) {
      max_alloc -= left_alloc;
      old_left_alloc = left_alloc;
      left_alloc = 0;
    }
  }
  backend->giveback_left_alloc_size(old_left_alloc);
}
