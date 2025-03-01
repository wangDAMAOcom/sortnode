/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <connection.h>
#include <cross_node_join.h>
#include <data_space.h>
#include <log_tool.h>
#include <mysql_handler.h>
#include <mysql_session.h>
#include <pool.h>
#include <routine.h>
#include <xa_transaction.h>

namespace dbscale {
namespace mysql {

MySQLSession::MySQLSession()
    : compress(false),
      lock_head(NULL),
      xid((unsigned long)0),
      seq_id((unsigned int)0),
      is_xa_prepared(false),
      gsid(0),
      sp_extra_snippet_val(0),
      packet_number(0),
      compress_packet_number(0),
      last_insert_id(0),
      binary_resultset(false),
      packet_from_buff(false),
      client_buffer_packet(default_receive_packet_size) {
  found_rows = 1;
  error_packet = NULL;
  result_packet = NULL;
  username[0] = '\0';
  password[0] = '\0';
  schema[0] = '\0';
  sp_error_code = 0;
  sp_error_sql_state = "";
  sp_error_message = "";
  query_sql[0] = '\0';
  entire_query_sql.clear();
  backend_get_consistence = false;
  is_consistence_transaction = false;
  no_status = false;

  federated_num = 0;
  finished_federated_num = 0;

  schema_acl_map = new map<string, SchemaACLType, strcasecomp>;
  table_acl_map = new map<string, TableACLType, strcasecomp>;

  init_uncompress_buffer(true, false);
  auto_inc_value_map.clear();
  real_fetched_rows = 0;
  postponed_ok_packet_state = END_POSTPONE_PACKET;

  transaction_sqls_num = 0;

  kept_space_map = new map<DataSpace *, DataSpace *>();
  client_capabilities = 0;

  init_session_option();
  LOG_DEBUG("Session [%@] %d is created.\n", this, get_thread_id());
}

MySQLSession::~MySQLSession() {
  LOG_DEBUG("Session [%@] %d start destructing.\n", this, get_thread_id());
  cluster_xa_rollback();
  rollback();
  unlock();
  if (transparent_mode && is_admin_accepter) {
    close_trans_conn();
  }
  clean_net_attribute();
  remove_all_connection_to_dead();
  clean_all_prepare_items();
  clean_all_prepare_read_connection();
  remove_all_sp_worker();
  clear_profile_tree();

  if (uncompress_buffer.buff) {
    delete[] uncompress_buffer.buff;
    uncompress_buffer.buff = NULL;
  }
  if (uncompress_buffer.extra_buff) {
    delete[] uncompress_buffer.extra_buff;
    uncompress_buffer.extra_buff = NULL;
  }

  map<unsigned int, vector<MySQLColumnType> *>::iterator it;
  for (it = column_types.begin(); it != column_types.end(); it++) {
    vector<MySQLColumnType> *types_vec = it->second;
    types_vec->clear();
    delete types_vec;
  }
  clean_all_using_table();
  clean_all_tx_record_redo_log_kept_connections();
  if (stmt) delete stmt;
  warning_spaces.clear();
  if (schema_acl_map) {
    delete schema_acl_map;
    schema_acl_map = NULL;
  }
  if (table_acl_map) {
    delete table_acl_map;
    table_acl_map = NULL;
  }

  if (!no_status) dbscale_status.collect_dbscale_status();
  update_server_hotspot_count();
  kept_space_map->clear();
  delete kept_space_map;
  LOG_DEBUG("Session [%@] destructed.\n", this);
}

void MySQLSession::remove_all_sp_worker() {
  LOG_DEBUG(
      "Start to remove all sp_worker in session [%@], the size of "
      "sp_worker_map is %d.\n",
      this, sp_worker_map.size());
  map<string, SPWorker *>::iterator it = sp_worker_map.begin();
  for (; it != sp_worker_map.end(); it++) {
    SPWorker *spw = it->second;
    spw->clean_sp_worker_for_free();
    LOG_DEBUG("Session [%@] remove sp_worker [%@], sp_worker name is [%s].\n",
              this, spw, spw->get_name());
    delete spw;
  }
  sp_worker_map.clear();
  reset_all_nest_sp_counter();
}

void MySQLSession::reset_all_nest_sp_counter() {
  sp_fake_name.clear();
  sp_fake_name_record.clear();
}

string MySQLSession::get_next_nest_sp_name(string &sp_name) {
  string ret = sp_name;
  if (sp_fake_name.count(sp_name)) {
    sp_fake_name[sp_name]++;
    char val[40];
    sprintf(val, "%s%d", DBSCALE_RESERVED_STR, sp_fake_name[sp_name]);
    ret.append(val);
    LOG_DEBUG(
        "sp name of [%s] update to [%s], to avoid nested call sp related "
        "issues.\n",
        sp_name.c_str(), ret.c_str());
    sp_fake_name_record[sp_name].push_back(ret);
  } else
    sp_fake_name[sp_name] = 0;
  return ret;
}

int MySQLSession::get_extra_snippet_of_sp_var() {
  return ++sp_extra_snippet_val;
}

void MySQLSession::reset_session_to_beginning() {
  /* ------------------------------------------------*/
  // clean up
  rollback();
  unlock();
  // remove_all_connection_to_dead();
  clean_all_prepare_items();
  clean_all_using_table();
  clean_all_prepare_read_connection();
  remove_all_sp_worker();
  clear_profile_tree();

  user_var_map.clear();
  user_var_origin_map.clear();

  // session_var_map.clear();
  temp_table_mutex.acquire();
  temp_table_set.clear();
  temp_table_mutex.release();

  is_query_command = false;
  set_auto_commit_is_on(true);
  set_stmt_auto_commit_value(true);
  last_insert_id = 0;
  stmt_auto_commit_int_value = 1;
  init_session_option();
#ifdef DEBUG
  LOG_DEBUG("Reset session finished.\n");
#endif
}

void MySQLSession::re_init_session() {
  rollback();
  unlock();
  remove_all_connection_to_dead();
  auto_inc_value_map.clear();

  in_transaction = false;
  in_transaction_consistent = false;
  trx_specify_read_only = false;
  server_shutdown = false;
  is_need_swap_handler = true;
  sub_traditional_num();
  in_lock = false;
  compress = false;
  lock_head = NULL;
  real_time_queries = 0;
  set_session_var_map_md5("");
  set_schema_name_md5("");
  is_readonly = false;
  xid = (unsigned long)0;
  seq_id = (unsigned int)0;
  is_xa_prepared = false;
  reset_table_without_given_schema();
  has_var = false;
  found_rows = 1;
  error_packet = NULL;
  drop_current_schema = false;
  wakeup_by_empty_leader = false;
  follower_wakeuped = false;
  is_consistence_transaction = false;
  sp_extra_snippet_val = 0;
  affect_server_type = AFFETCED_NON_SERVER;
  executing_prepare = false;
  gsid = 0;
  has_check_first_sql = false;
  flow_rate_control_query_cmd_total_count = 0;
  max_query_count_per_flow_control_gap_local =
      max_query_count_per_flow_control_gap;
  queue<pair<unsigned int, int> > tmp;
  swap(flow_rate_control_queue, tmp);  // reset the queue

  binary_resultset = false;
  packet_number = 0;
  compress_packet_number = 0;
  last_insert_id = 0;

  do_swap_route = false;
  client_can_read = false;
  enable_silent_execute_prepare_local = enable_silent_execute_prepare;

  username[0] = '\0';
  password[0] = '\0';
  schema[0] = '\0';
  sp_error_code = 0;
  sp_error_sql_state = "";
  sp_error_message = "";
  query_sql[0] = '\0';
  entire_query_sql.clear();
  session_var_map.clear();
  user_var_map.clear();
  kept_connections.clear();
  kept_space_map->clear();
  prepare_read_connections.clear();
  savepoint_list.clear();
  redo_logs_map.clear();
  using_conns.clear();
  clean_all_using_table();
  remove_all_sp_worker();
  clean_all_tx_record_redo_log_kept_connections();
  auth_schema_set.clear();
  if (schema_acl_map) {
    schema_acl_map->clear();
  }
  if (table_acl_map) {
    table_acl_map->clear();
  }
  temp_table_mutex.acquire();
  temp_table_set.clear();
  temp_table_mutex.release();

  is_query_command = false;
  is_load_data_local_command = false;
  warning_spaces.clear();
  is_federated_session = false;
  has_send_client_error_packet = false;
  do_logining = false;
  stmt_auto_commit_value.clear();
  set_mul_node_timeout(false);

  federated_num = 0;
  finished_federated_num = 0;
  query_sql_replace_null_char.clear();
  cross_tmp_table_collate.clear();
  cross_tmp_table_charset.clear();
  has_check_acl = false;
  need_abort_xa_transaction = false;
  select_lock_type = 0;
  got_error_er_must_change_password = false;
  has_more_result = false;
  retl_tb_end_pos = 0;
  // release_all_packet_in_packet_pool();
#ifdef HAVE_OPENSSL
  if (ssl_stream) {
    SSL *ssl = ssl_stream->ssl();
    SSL_set_quiet_shutdown(ssl, 1);
    SSL_shutdown(ssl);
    delete ssl_stream;
    ssl_stream = NULL;
  }
#endif
  stmt_auto_commit_int_value = 1;
}

CrossNodeJoinTable *MySQLSession::reuse_session_kept_join_tmp_table(
    CrossNodeJoinTablePool *pool) {
  if (!pool) return NULL;
  CrossNodeJoinTable *ret = NULL;
  ACE_Guard<ACE_Thread_Mutex> guard(tmp_table_mutex);
  if (cross_join_tmp_tables_map.count(pool) &&
      !cross_join_tmp_tables_map[pool].empty()) {
    ret = cross_join_tmp_tables_map[pool].back();
    cross_join_tmp_tables_map[pool].pop_back();
  }

  bool need_add_to_dead = is_in_transaction() || (!deep_clean_tmp_table);
  if (need_add_to_dead && ret) {
    kept_dead_table.push_back(ret);
    pool->add_to_dead_in_tran(ret);
    return NULL;
  }
  return ret;
}

void MySQLSession::set_kept_cross_join_tmp_table(CrossNodeJoinTable *table) {
  if (enable_reuse_tmp_table) {
    CrossNodeJoinTablePool *table_pool = (CrossNodeJoinTablePool *)table->pool;
    cross_join_tmp_tables_map[table_pool].push_back(table);
  } else {
    cross_join_tmp_tables.push_back(table);
  }
}

void MySQLSession::add_back_kept_cross_join_tmp_table() {
  if (!enable_reuse_tmp_table) {
    list<CrossNodeJoinTable *>::iterator it = cross_join_tmp_tables.begin();
    for (; it != cross_join_tmp_tables.end(); it++) {
      (*it)->pool->add_back_to_free(*it);
    }
    cross_join_tmp_tables.clear();
  } else {
    map<CrossNodeJoinTablePool *, vector<CrossNodeJoinTable *> >::iterator iter;
    for (iter = cross_join_tmp_tables_map.begin();
         iter != cross_join_tmp_tables_map.end(); iter++) {
      vector<CrossNodeJoinTable *> &tmp_vec = iter->second;
      for (size_t i = 0; i < tmp_vec.size(); i++) {
        tmp_vec[i]->pool->add_back_to_free(tmp_vec[i]);
      }
    }
    cross_join_tmp_tables_map.clear();
    if (!kept_dead_table.empty()) {
      for (size_t i = 0; i < kept_dead_table.size(); i++) {
        CrossNodeTableTrxGCThread::instance()->push_one_dead_table(
            kept_dead_table[i]);
        ((CrossNodeJoinTablePool *)(kept_dead_table[i]->pool))
            ->del_from_dead_in_tran(kept_dead_table[i]);
      }
      kept_dead_table.clear();
      CrossNodeTableTrxGCThread::instance()->signal();
    }
  }
}

void MySQLSession::set_kept_connection(DataSpace *dataspace, Connection *conn,
                                       bool need_session_mutex) {
  /*
    if (get_is_simple_direct_stmt() || is_skip_mutex_in_operation()) {
      kept_connections[dataspace] = conn;
  #ifdef DEBUG
      ACE_ASSERT(conn != NULL);
  #endif
    } else {
  */
  if (is_in_cluster_xa_transaction() && check_cluster_conn_is_xa_conn(conn)) {
    LOG_DEBUG(
        "connection [%@] is cluster_xa_conn, cann't add to kept_connections.\n",
        conn);
    return;
  }
  if (need_session_mutex) mutex.acquire();
  conn->set_status(CONNECTION_STATUS_WORKING);
  if (kept_connections.count(dataspace)) {
    kept_connections[dataspace]->get_pool()->add_back_to_dead(
        kept_connections[dataspace]);
  }
  kept_connections[dataspace] = conn;

#ifdef DEBUG
  ACE_ASSERT(conn != NULL);
#endif
  if (need_session_mutex) mutex.release();
  /*
    }
  */
  LOG_DEBUG("Session [%@] keep dataspace [%@] the connection [%@].\n", this,
            dataspace, conn);
}

/* We assume that the mutex lock has been acquired for this function*/
Connection *MySQLSession::check_kept_connection_for_same_server(
    DataSpace *dataspace) {
#ifdef DEBUG
  ACE_ASSERT(dataspace);
#endif
  DataSource *source = dataspace->get_data_source();
  if (!source) {
    LOG_WARN(
        "fail to do check_kept_connection_for_same_server because no "
        "datasource found for dataspace %@\n",
        dataspace);
    return NULL;
  }
  if (source->get_data_source_type() == DATASOURCE_TYPE_LOAD_BALANCE) {
    LOG_WARN(
        "fail to do check_kept_connection_for_same_server because datasource "
        "for dataspace %@ is type of LOAD_BALANCE\n",
        dataspace);
    return NULL;
  }
  if (source->get_data_source_type() == DATASOURCE_TYPE_ODBC) {
    LOG_WARN(
        "fail to do check_kept_connection_for_same_server because datasource "
        "for dataspace %@ is type of ODBC\n",
        dataspace);
    return NULL;
  }
  if (kept_connections.empty()) return NULL;
  Connection *ret = NULL;
  map<DataSpace *, Connection *>::iterator it;
  for (it = kept_connections.begin(); it != kept_connections.end(); it++) {
    if (dataspace->get_virtual_machine_id() ==
        it->first->get_virtual_machine_id()) {
      if (is_share_same_server(
              source->get_master_server(),
              it->first->get_data_source()->get_master_server())) {
        (*kept_space_map)[dataspace] = it->first;
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

bool MySQLSession::remove_kept_connection_real(DataSpace *dataspace,
                                               bool clean_dead) {
  DataSpace *real_dataspace = NULL;
  if (kept_connections.count(dataspace))
    real_dataspace = dataspace;
  else if (kept_space_map->count(dataspace))
    real_dataspace = (*kept_space_map)[dataspace];
  else {
    return false;
  }
  if (kept_connections.count(real_dataspace))
    if ((kept_connections[real_dataspace]->is_in_prepare() ||
         kept_connections[real_dataspace]->has_tmp_table()) &&
        !clean_dead) {
      return false;
    }
  map<DataSpace *, DataSpace *>::iterator it = kept_space_map->begin();
  for (; it != kept_space_map->end();) {
    if (it->second == real_dataspace) {
      kept_space_map->erase(it++);
    } else
      it++;
  }
  if (kept_connections.count(real_dataspace)) {
    LOG_DEBUG("Session [%@] remove kept_connection [%@].\n", this,
              kept_connections[real_dataspace]);
    kept_connections.erase(real_dataspace);
  }
  return true;
}

bool MySQLSession::remove_kept_connection(DataSpace *dataspace,
                                          bool clean_dead) {
  bool ret = true;
  /*
    if (get_is_simple_direct_stmt() || is_skip_mutex_in_operation())
      ret = remove_kept_connection_real(dataspace, clean_dead);
    else {
  */
  mutex.acquire();
  ret = remove_kept_connection_real(dataspace, clean_dead);
  mutex.release();
  /*
    }
  */
  return ret;
}

bool MySQLSession::check_conn_in_kept_connections(DataSpace *dataspace,
                                                  Connection *conn) {
  ACE_Guard<ACE_Thread_Mutex> guard(mutex);
  if (kept_connections.count(dataspace)) {
    if (conn && kept_connections[dataspace] != conn) return false;
    return true;
  } else if (kept_space_map->count(dataspace)) {
    Connection *kept_conn = kept_connections[(*kept_space_map)[dataspace]];
    if (conn && kept_conn && conn != kept_conn) return false;
    return true;
  }
  return false;
}

Connection *MySQLSession::get_kept_connection_real(DataSpace *dataspace) {
  Connection *conn = NULL;
  if (kept_connections.count(dataspace)) {
    LOG_DEBUG("Get kept connection [%@] from session [%@].\n",
              kept_connections[dataspace], this);
    conn = kept_connections[dataspace];

  } else {
    if (kept_space_map->count(dataspace)) {
      conn = kept_connections[(*kept_space_map)[dataspace]];
      LOG_DEBUG("Get kept connection.\n");
#ifdef DEBUG
      ACE_ASSERT(conn);
#endif
    } else {
      conn = check_kept_connection_for_same_server(dataspace);
    }
  }
  return conn;
}

bool MySQLSession::check_same_kept_connection(DataSpace *dataspace1,
                                              DataSpace *dataspace2) {
  Connection *conn1 = NULL;
  Connection *conn2 = NULL;
  conn1 = get_kept_connection_real(dataspace1);
  conn2 = get_kept_connection_real(dataspace2);
  if (conn1 == conn2) return true;
  return false;
}

Connection *MySQLSession::get_kept_connection(DataSpace *dataspace) {
  Connection *conn = NULL;
  if (get_is_simple_direct_stmt() || is_skip_mutex_in_operation()) {
    conn = get_kept_connection_real(dataspace);
  } else {
    mutex.acquire();
    conn = get_kept_connection_real(dataspace);
    mutex.release();
  }
  if (!conn) return NULL;

  if (conn->get_resource_status() == RESOURCE_STATUS_DEAD) {
    LOG_ERROR("Kept connection has been killed!\n");
    throw HandlerError("Connection has been killed!");
  }

  if (get_is_complex_stmt()) {
    if (conn->get_status() == CONNECTION_STATUS_WORKING) {
      conn->wait_conn();
    }
    if (conn->get_status() == CONNECTION_STATUS_TO_DEAD) {
      LOG_ERROR("Kept connection has been released!\n");
      throw HandlerError("Connection has been released!");
    }
    conn->set_status(CONNECTION_STATUS_WORKING);
  }
  conn->reset();

  return conn;
}

void MySQLSession::remove_all_connection_to_free() {
  mutex.acquire();
  add_back_kept_cross_join_tmp_table();
  map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
  while (it != kept_connections.end()) {
    if (it->second->is_in_prepare() || it->second->has_tmp_table() ||
        check_cluster_conn_is_xa_conn(it->second)) {
      it->second->set_status(CONNECTION_STATUS_FREE);
      it++;
      continue;
    } else {
      if (!defined_lock_need_kept_conn(it->first)) {
        remove_using_conn(it->second);
        if (ACE_Reactor::instance()->reactor_event_loop_done()) {
          LOG_DEBUG("event loop is done, add back to dead [%@]\n", it->second);
          it->second->get_pool()->add_back_to_dead(it->second);
        } else {
          if (close_load_conn && it->second->is_load()) {
            it->second->get_pool()->add_back_to_dead(it->second);
          } else {
            it->second->get_pool()->add_back_to_free(it->second);
          }
        }
      }
      kept_connections.erase(it++);
    }
    kept_space_map->clear();
  }
  mutex.release();
}

void MySQLSession::add_warning_dataspace(DataSpace *space, Connection *conn) {
  DataSpace *kept_space = NULL;
  mutex.acquire();
  map<DataSpace *, Connection *>::iterator it;
  for (it = kept_connections.begin(); it != kept_connections.end(); it++) {
    if (it->second == conn) {
      kept_space = it->first;
      break;
    }
  }
  warning_spaces.insert(space);
  if (!kept_space) {
    set_kept_connection(space, conn, false);
  } else {
    warning_map[space] = kept_space;
  }
  mutex.release();
}

void MySQLSession::remove_warning_conn_to_free() {
  mutex.acquire();
  map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
  while (it != kept_connections.end()) {
    DataSpace *space = it->first;
    Connection *conn = it->second;
    if (!warning_spaces.count(space)) {
      it++;
      continue;
    }
    if (it->second->is_in_prepare() || it->second->has_tmp_table() ||
        check_cluster_conn_is_xa_conn(it->second)) {
      it++;
      continue;
    }
    if (!defined_lock_need_kept_conn(space)) {
      remove_using_conn(conn);
      if (close_load_conn && conn->is_load())
        conn->get_pool()->add_back_to_dead(conn);
      else
        conn->get_pool()->add_back_to_free(conn);
    }
    kept_connections.erase(it++);
  }
  mutex.release();
}

void MySQLSession::remove_non_warning_conn_to_free() {
  mutex.acquire();

  map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
  while (it != kept_connections.end()) {
    DataSpace *space = it->first;
    if (warning_spaces.count(space)) {
      it++;
      continue;
    }
    if (it->second->is_in_prepare() || it->second->has_tmp_table() ||
        check_cluster_conn_is_xa_conn(it->second)) {
      it++;
      continue;
    }
    if (!defined_lock_need_kept_conn(space)) {
      remove_using_conn(it->second);
      if (close_load_conn && it->second->is_load())
        it->second->get_pool()->add_back_to_dead(it->second);
      else
        it->second->get_pool()->add_back_to_free(it->second);
    }
    kept_connections.erase(it++);
  }
  kept_space_map->clear();

  mutex.release();
}

void MySQLSession::clear_dataspace_warning(DataSpace *space, bool clean_conn) {
  mutex.acquire();
  if (warning_spaces.count(space)) {
    if (warning_map.count(space)) {
      DataSpace *kept_space = warning_map[space];
      Connection *conn = kept_connections[kept_space];
      if (conn) {
        if (clean_conn) {
          if (close_load_conn && conn->is_load())
            conn->get_pool()->add_back_to_dead(conn);
          else
            conn->get_pool()->add_back_to_free(conn);
        }
        kept_connections.erase(kept_space);
      }

      map<DataSpace *, DataSpace *>::iterator it = warning_map.begin();
      while (it != warning_map.end()) {
        if (it->second == kept_space) {
          warning_map.erase(it++);
        } else {
          it++;
        }
      }
    } else {
      Connection *conn = kept_connections[space];
      if (conn) {
        if (clean_conn) {
          if (close_load_conn && conn->is_load())
            conn->get_pool()->add_back_to_dead(conn);
          else
            conn->get_pool()->add_back_to_free(conn);
        }
        kept_connections.erase(space);
      }
    }
    warning_spaces.erase(space);
  }
  mutex.release();
}
void MySQLSession::clean_all_prepare_read_connection() {
  map<uint32_t, Connection *>::iterator it;
  set<Connection *> tmp_set;
  for (it = prepare_read_connections.begin();
       it != prepare_read_connections.end();) {
    tmp_set.insert(it->second);
    prepare_read_connections.erase(it++);
  }
  set<Connection *>::iterator it2;
  Connection *conn = NULL;
  for (it2 = tmp_set.begin(); it2 != tmp_set.end(); it2++) {
    conn = *it2;
    remove_using_conn(conn);
    // the function will be use the session clean. these connection has been
    // send the prepare statement. So we do not just put them back to free.
    conn->get_pool()->add_back_to_dead(conn);
  }
  prepare_read_connections.clear();
  tmp_set.clear();
}

void MySQLSession::set_prepare_read_connection(uint32_t gsid,
                                               Connection *conn) {
  if (!conn) return;
  if (!prepare_read_connections.count(gsid)) {
    conn->set_status(CONNECTION_STATUS_WORKING);
    prepare_read_connections[gsid] = conn;
  } else {
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
  }
  LOG_DEBUG("Session [%@] keep gsid = %u the connection [%@].\n", this, gsid,
            conn);
}

void MySQLSession::remove_prepare_read_connection(uint32_t gsid) {
  if (prepare_read_connections.count(gsid)) {
    prepare_read_connections.erase(gsid);
  }
  LOG_DEBUG("Session [%@] remove gtid = %d.\n", this, gsid);
}
void MySQLSession::remove_prepare_read_connection(Connection *conn) {
  if (!conn) {
    return;
  }
  map<uint32_t, Connection *>::iterator it;
  for (it = prepare_read_connections.begin();
       it != prepare_read_connections.end();) {
    if (conn == it->second)
      prepare_read_connections.erase(it++);
    else
      it++;
  }
}

Connection *MySQLSession::get_prepare_read_connection(uint32_t gsid) {
  Connection *conn = NULL;
  if (prepare_read_connections.count(gsid)) {
    conn = prepare_read_connections[gsid];
  }
  if (!conn) {
    return NULL;
  }
  if (conn->get_status() == CONNECTION_STATUS_WORKING) {
    conn->wait_conn();
  }
  if (conn->get_status() == CONNECTION_STATUS_TO_DEAD) {
    LOG_ERROR("prepare read connection has been released!\n");
    throw HandlerError("Connection has been released!");
  }
  conn->set_status(CONNECTION_STATUS_WORKING);
  conn->reset();

  return conn;
}

Connection *MySQLSession::check_connection_in_prepare_read_connection(
    Connection *conn) {
  if (!conn) {
    return NULL;
  }
  map<uint32_t, Connection *>::iterator it = prepare_read_connections.begin();
  for (; it != prepare_read_connections.end(); it++) {
    if (is_share_same_server(it->second->get_server(), conn->get_server())) {
      conn->get_pool()->add_back_to_free(conn);
      return it->second;
    }
  }
  return conn;
}

void MySQLSession::remove_all_connection_to_dead() {
  mutex.acquire();
  add_back_kept_cross_join_tmp_table();
  map<DataSpace *, Connection *>::iterator it;
  while (kept_connections.size() > 0) {
    it = kept_connections.begin();
    if (!defined_lock_need_kept_conn(it->first)) {
      remove_using_conn(it->second);
      it->second->reset_cluster_xa_session_before_to_dead();
      it->second->get_pool()->add_back_to_dead(it->second);
    }
    kept_connections.erase(it);
  }
  kept_space_map->clear();
  mutex.release();
}
void MySQLSession::unlock() {
  LOG_DEBUG("execute implicit unlock tables from session [%@].\n", this);
  if (is_in_lock() || get_has_global_flush_read_lock()) {
    set_in_lock(false);
    set_has_global_flush_read_lock(false);
    bool b = add_traditional_num();
    implicit_execute_stmt("UNLOCK TABLES");
    if (b) sub_traditional_num();
    remove_all_lock_tables();
  }
}
void MySQLSession::rollback() {
  if (check_for_transaction() && !is_in_cluster_xa_transaction()) {
    LOG_DEBUG("execute implicit rollback from session [%@].\n", this);
    get_status()->item_inc(TIMES_TXN);
    bool b = add_traditional_num();
    if (!enable_xa_transaction ||
        get_session_option("close_session_xa").int_val != 0 ||
        is_in_transaction_consistent()) {
      set_in_transaction(false);
      implicit_execute_stmt("ROLLBACK");
      remove_all_savepoint();
    } else {
      execute_xa_rollback();
      set_in_transaction(false);
    }
    if (b) sub_traditional_num();
  }
}

Connection *MySQLSession::get_cluster_xa_transaction_conn() {
  Backend *bk = Backend::instance();
  if (cluster_xa_conn &&
      cluster_xa_conn->get_resource_status() == RESOURCE_STATUS_DEAD) {
    /* here we should reset_cluster_xa_session_before_to_dead, because xa_conn
     * won't be returned and then, no one will deal with it except here.
     * so here we should clean this dead cluster_xa_conn.
     */
    if (cluster_xa_conn->get_is_reset_to_dead()) {
      get_handler()->clean_dead_conn(&cluster_xa_conn, bk->get_catalog());
    }
    LOG_ERROR("cluster xa connection has been killed!\n");
    throw HandlerError("cluster xa Connection has been killed!");
  }
  if (cluster_xa_conn) cluster_xa_conn->reset();
  return cluster_xa_conn;
}
// this function can only used when session is quit or server is down
// because it deal with XA_PREPARED add conn to dead, not xa rollback.
void MySQLSession::cluster_xa_rollback() {
  Backend *bk = Backend::instance();
  if (is_in_cluster_xa_transaction()) {
    int return_num = 1;
    string xa_rollback = "XA ROLLBACK " + get_session_cluster_xid() + ";";
    if (get_cluster_xa_transaction_state() == SESSION_XA_TRX_XA_ACTIVE) {
      xa_rollback = "XA END " + get_session_cluster_xid() + ";" + xa_rollback;
      ++return_num;
    } else if (get_cluster_xa_transaction_state() == SESSION_XA_TRX_XA_IDLE) {
      // do nothing
    } else {
      if (!cluster_xa_conn) {
        reset_cluster_xa_transaction();
        LOG_DEBUG(
            "cluster xa conn is NULL, session state is XA_PREPARED, "
            "reset_cluster_xa_transaction return.\n");
        return;
      } else {
        // xa_conn is XA_PREPARED, cannot add back to free, we need to add to
        // dead
        LOG_DEBUG(
            "session state is XA_PREPARED, don't need to rollback, add xa_conn "
            "to dead.\n");
        get_handler()->clean_dead_conn(&cluster_xa_conn, bk->get_catalog());
      }
      return;
    }

    LOG_DEBUG("session rollback, execute_xa_rollback %s.\n",
              xa_rollback.c_str());

    try {
      if (!cluster_xa_conn) {
        reset_cluster_xa_transaction();
        LOG_DEBUG(
            "xa conn is NULL, xa rollback reset_cluster_xa_transaction and "
            "return.\n");
        return;
      }

      if (cluster_xa_conn->get_resource_status() == RESOURCE_STATUS_DEAD) {
        if (cluster_xa_conn->get_is_reset_to_dead()) {
          cluster_xa_conn->get_pool()->add_back_to_dead(cluster_xa_conn);
        }
        reset_cluster_xa_transaction();
        LOG_ERROR(
            "Kept connection has been killed! xa rollback "
            "reset_cluster_xa_transaction "
            "and return.\n");
        return;
      }

      cluster_xa_conn->execute_mul_modify_sql(xa_rollback.c_str(), return_num);
      Connection *tmp_xa_conn = cluster_xa_conn;
      reset_cluster_xa_transaction();
      get_handler()->put_back_connection(bk->get_catalog(), tmp_xa_conn);
    } catch (HandlerError &e) {
      LOG_ERROR(
          "Server [%s%@] fail to do cluster xa rollback for sql %s due to "
          "error %s.\n",
          cluster_xa_conn->get_server()->get_name(),
          cluster_xa_conn->get_server(), xa_rollback.c_str(), e.what());
      get_handler()->clean_dead_conn(&cluster_xa_conn, bk->get_catalog());
    } catch (...) {
      LOG_ERROR(
          "Server [%s%@] fail to do cluster xa rollback for sql %s due to conn "
          "error.\n",
          cluster_xa_conn->get_server()->get_name(),
          cluster_xa_conn->get_server(), xa_rollback.c_str());
      get_handler()->clean_dead_conn(&cluster_xa_conn, bk->get_catalog());
    }
  }
}

void MySQLSession::remove_fail_conn_from_session(DataSpace *space,
                                                 Connection *conn) {
  remove_using_conn(conn);
  conn->get_pool()->add_back_to_dead(conn);
  if (support_show_warning) clear_dataspace_warning(space, false);
  remove_kept_connection(space, true);
  remove_prepare_read_connection(conn);
}

void MySQLSession::execute_xa_rollback() {
  if (kept_connections.empty()) return;
  map<DataSpace *, Connection *>::iterator it;
  Driver *driver = Driver::get_driver();
  XA_helper *xa_helper = driver->get_xa_helper();
  map<DataSpace *, Connection *> local_kept_conn = kept_connections;
  for (it = local_kept_conn.begin(); it != local_kept_conn.end(); it++) {
    int tmp = xa_helper->exec_xa_rollback(this, it->first, it->second);
    if (tmp) {
      remove_fail_conn_from_session(it->first, it->second);
    }
  }
  remove_all_connection_to_free();

  reset_seq_id();
  set_xa_id((unsigned long)0);
}

void MySQLSession::implicit_execute_stmt(const char *sql) {
  LOG_DEBUG("Session %@ implicit execute stmt %s.\n", this, sql);
  map<DataSpace *, Connection *>::iterator it;
  Connection *conn = NULL;
  map<DataSpace *, Connection *> local_kept_conn = kept_connections;
  for (it = local_kept_conn.begin(); it != local_kept_conn.end(); it++) {
    try {
      conn = it->second;
      conn->execute_transaction_unlock_stmt(sql);
    } catch (exception &e) {
      LOG_ERROR(
          "connection [%@] get exception when executing implicit [%s] "
          "from session [%@] with [%s].\n",
          it->second, sql, this, e.what());
      remove_fail_conn_from_session(it->first, it->second);
    }
  }
  conn = NULL;
  if (!is_keeping_connection()) remove_all_connection_to_free();
}
void MySQLSession::add_savepoint(const char *sql) {
  list<char *>::iterator it = savepoint_list.begin();
  for (; it != savepoint_list.end(); it++) {
    if (strcasecmp(sql, *it) == 0) {
      char *tmp = *it;
      savepoint_list.erase(it);
      delete[] tmp;
      break;
    }
  }
  int len = strlen(sql) + 1;
  char *str = new char[len];
  strncpy(str, sql, len);
  savepoint_list.push_back(str);
  LOG_DEBUG("Session [%@] add savepoint [%s].\n", this, sql);
}

void MySQLSession::remove_all_savepoint() {
  list<char *>::iterator it = savepoint_list.begin();
  for (; it != savepoint_list.end();) {
    char *tmp = *it;
    savepoint_list.erase(it++);
    delete[] tmp;
  }
  LOG_DEBUG("Session [%@] remove_all_savepoint.\n", this);
}

void MySQLSession::remove_savepoint(const char *point) {
  bool find_point = false;
  list<char *>::iterator it = savepoint_list.begin();
  for (; it != savepoint_list.end();) {
    if (find_point) {
      char *tmp = *it;
      savepoint_list.erase(it++);
      delete[] tmp;
    } else {
      if (strcasecmp(point, *it) == 0) {
        find_point = true;
      }
      it++;
    }
  }
  LOG_DEBUG("Session [%@] remove savepoint after point [%s].\n", this, point);
}
lock_table_full_item *MySQLSession::add_lock_table(lock_table_item *item) {
  lock_table_node *tmp = new lock_table_node();
  int len = strlen(item->name);
  const char *dot = strchr(item->name, '.');
  if (dot) {
    int table_len, schema_len;
    schema_len = dot - item->name + 1;
    table_len = len - schema_len + 1;
    tmp->item.schema_name = new char[schema_len];
    copy_string((char *)tmp->item.schema_name, item->name, schema_len);
    tmp->item.table_name = new char[table_len];
    copy_string((char *)tmp->item.table_name, dot + 1, table_len);
  } else {
    tmp->item.table_name = new char[len + 1];
    copy_string((char *)tmp->item.table_name, item->name, len + 1);

    len = strlen(get_schema());
    tmp->item.schema_name = new char[len + 1];
    copy_string((char *)tmp->item.schema_name, get_schema(), len + 1);
  }
  LOG_DEBUG("lock item schema_name=[%s], table_name=[%s],pre_schema=[%s]\n",
            tmp->item.schema_name, tmp->item.table_name, get_schema());
  if (item->alias) {
    len = strlen(item->alias);
    tmp->item.alias = new char[len + 1];
    copy_string((char *)tmp->item.alias, item->alias, len + 1);
  } else {
    tmp->item.alias = NULL;
  }
  tmp->item.type = item->type;
  tmp->item.operation = LOCK_TABLES;
  tmp->is_appear = false;
  if (lock_head) {
    tmp->next = lock_head;
    lock_head = tmp;
  } else {
    tmp->next = NULL;
    lock_head = tmp;
  }
  return &tmp->item;
}
void MySQLSession::remove_all_lock_tables() {
  lock_table_node *tmp = NULL;
  while (lock_head) {
    tmp = lock_head;
    lock_head = lock_head->next;
    delete[] tmp->item.table_name;
    delete[] tmp->item.schema_name;
    if (tmp->item.alias) delete[] tmp->item.alias;
    delete tmp;
    tmp = NULL;
  }
  lock_head = NULL;
  LOG_DEBUG("Session [%@] remove_all_lock_tables.\n", this);
}

bool MySQLSession::contains_lock_table(const char *table, const char *schema,
                                       const char *alias) {
  lock_table_node *tmp = lock_head;
  while (tmp) {
    if (tmp->is_appear) {
      tmp = tmp->next;
      continue;
    }
    const char *schema_name = NULL;
    if (schema == NULL)
      schema_name = get_schema();
    else
      schema_name = schema;

    if (lower_case_compare(table, tmp->item.table_name) == 0 &&
        lower_case_compare(schema_name, tmp->item.schema_name) == 0) {
      if (alias == NULL && tmp->item.alias == NULL) {
        tmp->is_appear = true;
        return true;
      } else if (alias != NULL && tmp->item.alias != NULL) {
        if (lower_case_compare(alias, tmp->item.alias) == 0) {
          tmp->is_appear = true;
          return true;
        }
      }
    }
    tmp = tmp->next;
  }
  return false;
}
void MySQLSession::reset_lock_table_state() {
  lock_table_node *tmp = lock_head;
  while (tmp) {
    tmp->is_appear = false;
    tmp = tmp->next;
  }
}

void MySQLSession::killall_using_conns() {
  Connection *kill_conn = NULL;
  Connection *killer = NULL;
  using_conns_mutex.acquire();

  if (check_in_in_xa_commit()) return;
  set<Connection *>::iterator it = using_conns.begin();
  for (; it != using_conns.end(); it++) {
    kill_conn = *it;
    /*get killer conn by create a new one, in case the conn pool is full.*/
    try {
      if (!kill_conn->get_server()->get_driver()) {
        /*maybe an odbc conn, skip it*/
        continue;
      }
      killer = kill_conn->get_server()->get_one_admin_conn();
      if (!killer) {
        // Most likely, the back end server is crashed.
        LOG_ERROR("Fail to get the killer connetion from pool %@.\n",
                  kill_conn->get_pool());
        throw ThreadKillFailed("fail to get killer connection.");
      }
      LOG_DEBUG("get killer conn %@.\n", killer);
      killer->kill_thread(kill_conn->get_thread_id());
    } catch (...) {
      LOG_ERROR("fail to kill connection [%d] from server [%s]\n",
                kill_conn->get_thread_id(),
                kill_conn->get_server()->get_name());
    }
    if (killer) {
      LOG_DEBUG("clean killer conn %@.\n", killer);
      killer->close();
      delete killer;
      killer = NULL;
    }
  }
  using_conns_mutex.release();
}

bool MySQLSession::check_in_in_xa_commit() {
  if (enable_xa_transaction &&
      get_session_option("close_session_xa").int_val == 0 &&
      is_in_transaction() && get_is_xa_prepared()) {
    /*The using conn for xa commit should not be killed! otherwise the xa
     * transaction may get a inconsistent result: some kept conn finish xa
     * commit, while others is killed. Cause the server is not crash or
     * restart, so there is no xa recover to replay the redo log.*/
    LOG_DEBUG(
        "Skip killing using conns for a xa transaction,"
        " which has finished prepare.");
    using_conns_mutex.release();
    return true;
  }
  return false;
}

void MySQLSession::set_option_values() {
  OptionParser *config = OptionParser::instance();
  config->update_option_values(&option_value_uint, &option_value_ul);
  get_handler()->set_update_option(false);
}

void MySQLSession::add_sp_worker(SPWorker *spw) {
  // TODO: Store the sp_worker to the global space, such as backend
  remove_sp_vector_mutex.acquire();
  vector<string>::iterator found = std::find(
      remove_sp_vector.begin(), remove_sp_vector.end(), spw->get_name());
  if (found != remove_sp_vector.end()) {
    remove_sp_vector.erase(found);
  }
  remove_sp_vector_mutex.release();
  sp_worker_map[string(spw->get_name())] = spw;
  LOG_DEBUG("Session [%@] add sp_worker [%@], sp_worker name is [%s].\n", this,
            spw, spw->get_name());
}

void MySQLSession::set_cur_sp_worker(SPWorker *spw) {
  // spw can be NULL only in test mode.
  cur_sp_worker = spw;
  if (cur_sp_worker)
    cur_sp_worker_stack.push_front(cur_sp_worker);
  else
    cur_sp_worker_stack.clear();
}

void MySQLSession::pop_cur_sp_worker_one_recurse() {
  cur_sp_worker_stack.pop_front();
  cur_sp_worker = cur_sp_worker_stack.front();
}

SPWorker *MySQLSession::get_sp_worker(string &name) {
  if (sp_worker_map.count(name)) {
    ACE_Guard<ACE_Thread_Mutex> guard(remove_sp_vector_mutex);
    if (std::find(remove_sp_vector.begin(), remove_sp_vector.end(), name) !=
        remove_sp_vector.end())
      return NULL;
    SPWorker *ret = sp_worker_map[name];
    ret->reset_for_new_call();
    return ret;
  }
  return NULL;
}

void MySQLSession::remove_sp_worker(string &name) {
  if (sp_worker_map.count(name)) {
    remove_sp_worker_by_name(name);
    if (sp_fake_name_record.count(name)) {
      int n = sp_fake_name_record[name].size();
      for (int i = 0; i < n; i++) {
        if (sp_worker_map.count(sp_fake_name_record[name][i]))
          remove_sp_worker_by_name(sp_fake_name_record[name][i]);
      }
      sp_fake_name_record.erase(name);
    }
  }
}

void MySQLSession::remove_sp_worker_by_name(string &name) {
  LOG_DEBUG("Remove sp_worker in Session [%@], sp_worker name [%s].\n", this,
            name.c_str());
  sp_worker_map[name]->clean_sp_worker_for_free();
  delete sp_worker_map[name];
  sp_worker_map[name] = NULL;
  sp_worker_map.erase(name);
}

bool MySQLSession::add_remove_sp_name(string &sp_name) {
  remove_sp_vector_mutex.acquire();
  bool contains_procedure = false;
  if (sp_worker_map.count(sp_name)) {
    remove_sp_vector.push_back(sp_name);
    contains_procedure = true;
  }
  remove_sp_vector_mutex.release();
  return contains_procedure;
}

void MySQLSession::clear_all_sp_in_remove_sp_vector() {
  remove_sp_vector_mutex.acquire();
  for (unsigned int i = 0; i < remove_sp_vector.size(); i++) {
    string sp_name = remove_sp_vector.at(i);
    remove_sp_worker(sp_name);
  }
  remove_sp_vector.clear();
  remove_sp_vector_mutex.release();
}

/* move from handler */

void MySQLSession::store_data_to_uncompress_buffer(size_t data_len,
                                                   const char *copy_start_pos) {
  uncompress_buffer.start_pos = uncompress_buffer.buff;
  uncompress_buffer.end_pos = uncompress_buffer.buff;
  if (data_len <= (size_t)uncompress_buffer_size) {
    memcpy(uncompress_buffer.end_pos, copy_start_pos, data_len);
    uncompress_buffer.end_pos += data_len;
  } else {
    memcpy(uncompress_buffer.end_pos, copy_start_pos, uncompress_buffer_size);
    *(uncompress_buffer.end_pos + uncompress_buffer_size) = '\0';
    int extra_uncompress_buffer_size = data_len - uncompress_buffer_size;
    uncompress_buffer.extra_buff = new char[extra_uncompress_buffer_size + 1];
    memcpy(uncompress_buffer.extra_buff,
           copy_start_pos + uncompress_buffer_size,
           extra_uncompress_buffer_size);
    uncompress_buffer.end_pos =
        uncompress_buffer.extra_buff + extra_uncompress_buffer_size;
  }
  *(uncompress_buffer.end_pos) = '\0';
  uncompress_buffer.has_data = true;
  uncompress_buffer.data_len = data_len;
  LOG_DEBUG(
      "Compressed packet contains another one or more packets after "
      "uncompressed,"
      " store rearward packet(s) to buffer, stored data length = %d\n",
      data_len);
}

void MySQLSession::get_data_from_uncompress_buffer(Packet *packet) {
  packet_from_buff = true;

  size_t packet_real_len = 0;
  if (!uncompress_buffer.extra_buff ||
      uncompress_buffer.start_pos_offset + 3 <= uncompress_buffer_size ||
      uncompress_buffer.start_pos_offset >= uncompress_buffer_size) {
    packet_real_len =
        Packet::unpack3uint(uncompress_buffer.start_pos) + PACKET_HEADER_SIZE;
  } else {
    packet_real_len +=
        (uint32_t)((unsigned char)(*uncompress_buffer.start_pos));
    if (uncompress_buffer.start_pos_offset + 1 == uncompress_buffer_size) {
      packet_real_len +=
          (uint32_t)((unsigned char)(*uncompress_buffer.extra_buff) << 8);
      packet_real_len += (uint32_t)(
          (unsigned char)(*(uncompress_buffer.extra_buff + 1)) << 16);
    } else {
      packet_real_len +=
          (uint32_t)((unsigned char)(*(uncompress_buffer.start_pos + 1)) << 8);
      packet_real_len +=
          (uint32_t)((unsigned char)(*uncompress_buffer.extra_buff) << 16);
    }
  }
  size_t pack_len = packet_real_len > uncompress_buffer.data_len
                        ? uncompress_buffer.data_len
                        : packet_real_len;
  LOG_DEBUG("Get packet from compressed buffer\n");
  packet->size(pack_len + 1);
  packet->rewind();
  if (!uncompress_buffer.extra_buff) {
    packet->packdata(uncompress_buffer.start_pos, pack_len);
    uncompress_buffer.start_pos += pack_len;
  } else {
    if (uncompress_buffer.start_pos_offset >= uncompress_buffer_size ||
        uncompress_buffer.start_pos_offset + pack_len <=
            (size_t)uncompress_buffer_size) {
      packet->packdata(uncompress_buffer.start_pos, pack_len);
      uncompress_buffer.start_pos += pack_len;
    } else {
      // data splited into 2 buffers
      int pre_len = uncompress_buffer_size - uncompress_buffer.start_pos_offset;
      packet->packdata(uncompress_buffer.start_pos, pre_len);
      packet->packdata(uncompress_buffer.extra_buff, pack_len - pre_len);
      uncompress_buffer.start_pos =
          uncompress_buffer.extra_buff + pack_len - pre_len;
    }
  }

  uncompress_buffer.start_pos_offset += pack_len;
  uncompress_buffer.data_len -= pack_len;

  char *end = packet->wr_ptr();
  packet->packchar('\0');
  packet->wr_ptr(end);
  packet->rd_ptr(packet->base());

  uint8_t number = static_cast<uint8_t>(packet->base()[PACKET_NUM_OFFSET]);
  check_packet_number(packet_number, number);

  if (uncompress_buffer.start_pos == uncompress_buffer.end_pos) {
    init_uncompress_buffer(false, uncompress_buffer.extra_buff ? true : false);
  }
}

void MySQLSession::init_uncompress_buffer(bool init_original_buff,
                                          bool has_extra_buff) {
  if (init_original_buff) {
    uncompress_buffer.buff = NULL;
  }
  if (has_extra_buff) {
    delete[] uncompress_buffer.extra_buff;
  }
  uncompress_buffer.extra_buff = NULL;
  uncompress_buffer.start_pos = NULL;
  uncompress_buffer.end_pos = NULL;
  uncompress_buffer.has_data = false;
  uncompress_buffer.start_pos_offset = 0;
  uncompress_buffer.data_len = 0;
}

void MySQLSession::check_packet_number(uint8_t expect_number,
                                       uint8_t real_number) {
  if (expect_number != real_number) {
    LOG_ERROR("Packet out of order: expecting %d got %d\n", expect_number,
              real_number);
    if (uncompress_buffer.buff) {
      delete[] uncompress_buffer.buff;
      uncompress_buffer.buff = NULL;
      uncompress_buffer.start_pos = NULL;
      uncompress_buffer.end_pos = NULL;
      if (uncompress_buffer.extra_buff) {
        delete[] uncompress_buffer.extra_buff;
        uncompress_buffer.extra_buff = NULL;
      }
    }
    throw HandlerError("packet out of order");
  }
}

bool MySQLSession::contains_odbc_conn() {
  map<DataSpace *, Connection *>::iterator it;
  for (it = kept_connections.begin(); it != kept_connections.end(); it++) {
    Connection *conn = it->second;
    if (conn->is_odbconn()) {
      return true;
    }
  }
  return false;
}

void MySQLSession::record_relate_source_redo_log(
    map<DataSpace *, Connection *> *kept_connections) {
  string redo_log;
  map<DataSpace *, Connection *>::iterator it = kept_connections->begin();
  for (; it != kept_connections->end(); it++) {
    redo_log += it->first->get_data_source()->get_name();
    redo_log += ":";
  }
  if (!kept_connections->empty())
    boost::erase_tail(redo_log, 1);  // cut the tail ':'
  spread_redo_log_to_buffer(redo_log.c_str(), true);
}

void MySQLSession::spread_redo_log_to_buffer(const char *sql,
                                             bool is_relate_source) {
  unsigned long xid = get_xa_id();
  unsigned int seq_id = 0;
  if (!is_relate_source) seq_id = get_next_seq_id();

  redo_logs_map_mutex.acquire();
  map<DataSpace *, list<string> >::iterator iter = redo_logs_map.begin();
  for (; iter != redo_logs_map.end(); iter++) {
    char tmp[256];
    int group_id = 0;
    unsigned int thread_id = 0;
    if (kept_connections.count(iter->first)) {
      Connection *conn = kept_connections[iter->first];
      group_id = conn->get_group_id();
      thread_id = conn->get_thread_id();
    } else {
      // redo_logs_map may record some space which has been erased from
      // kept_connections eg: when do commit, then check read only connection,
      // then execute rollback and erase it from kept_connections but the space
      // is still in redo_logs_map
      LOG_DEBUG(
          "spread_redo_log_to_buffer get exception, find redo space[%s] not in "
          "kept_connections\n",
          iter->first->get_name());
    }

    int len = sprintf(tmp, "'%lu-%u-%d-%d-%u'", xid,
                      iter->first->get_virtual_machine_id(), group_id,
                      Backend::instance()->get_cluster_id(), thread_id);
    tmp[len] = '\0';

    string stmt("(");
    stmt += tmp;
    stmt += ", ";

    len = sprintf(tmp, "%d", seq_id);
    tmp[len] = '\0';

    stmt += tmp;
    stmt += ", ";

    len = sprintf(tmp, "'%d-%lu'", Backend::instance()->get_cluster_id(), xid);
    tmp[len] = '\0';

    stmt += tmp;
    stmt += ", ";

    len = sprintf(tmp, "%d, %lu", Backend::instance()->get_cluster_id(), xid);
    tmp[len] = '\0';

    stmt += tmp;
    stmt += ", '";

    string str(sql);
    find_and_insert_str(&str, "\\", "\\");
    find_and_insert_str(&str, "'", "\\");

    stmt += str.c_str();
    stmt += "')";

    (iter->second).push_back(stmt);
  }
  redo_logs_map_mutex.release();
}

bool MySQLSession::silence_roll_back_group_sql(uint32_t id) {
  MySQLPrepareItem *prepare_item = get_prepare_item(id);
  if (!prepare_item || prepare_item->get_group_com_query_stmt_str().empty())
    return true;

  prepare_item->get_group_com_query_stmt_str().clear();
  set_is_silence_ok_stmt(false);
  set_binary_packet(NULL);
  set_binary_resultset(false);
  return true;
}

bool MySQLSession::silence_execute_group_ok_stmt(uint32_t id) {
  MySQLPrepareItem *prepare_item = get_prepare_item(id);
  if (!prepare_item || prepare_item->get_group_com_query_stmt_str().empty())
    return true;
  bool session_need_swap = need_swap_handler();
  set_is_silence_ok_stmt(true);
  set_binary_resultset(true);
  set_need_swap_handler(false);  // should not swap for silence execution

  /* Handle the query */
  try {
    set_silence_ok_stmt_id(0);
    ((MySQLHandler *)get_handler())
        ->handle_query_command(
            prepare_item->get_group_com_query_stmt_str().c_str(),
            prepare_item->get_group_com_query_stmt_str().length());
  } catch (Exception &e) {
    LOG_ERROR(
        "silence prepare stmt execute get exception %s for sql %s, so quit "
        "client connection.\n",
        e.what(), prepare_item->get_group_com_query_stmt_str().c_str());
    prepare_item->get_group_com_query_stmt_str().clear();
    set_is_silence_ok_stmt(false);
    set_binary_packet(NULL);
    set_binary_resultset(false);
    set_need_swap_handler(session_need_swap);
    throw HandlerQuit();
    return false;
  }

  prepare_item->get_group_com_query_stmt_str().clear();
  set_is_silence_ok_stmt(false);
  set_need_swap_handler(session_need_swap);
  set_binary_packet(NULL);
  set_binary_resultset(false);

  return true;
}

bool MySQLSession::silence_reload_info_schema_mirror_tb(string &sql) {
  bool session_need_swap = need_swap_handler();
  set_is_silence_ok_stmt(true);
  set_binary_resultset(false);
  set_need_swap_handler(false);
  set_is_info_schema_mirror_tb_reload_internal_session(true);
  set_audit_log_flag_local();
  reset_slow_query_time_local();
  set_session_var_map("CHARACTER_SET_CLIENT", "utf8");
  set_session_var_map("CHARACTER_SET_CONNECTION", "utf8");
  set_session_var_map("CHARACTER_SET_RESULTS", "utf8");
  set_session_var_map("CHARACTER_SET_DATABASE", "utf8");
  set_session_var_map("CHARACTER_SET_SERVER", "utf8");

  try {
    set_silence_ok_stmt_id(0);
    ((MySQLHandler *)get_handler())
        ->handle_query_command(sql.c_str(), sql.length());
  } catch (exception &e) {
    LOG_ERROR(
        "info schema mirror table silence reload get exception [%s] for sql "
        "[%s].\n",
        e.what(), sql.c_str());
    set_is_silence_ok_stmt(false);
    set_binary_resultset(false);
    set_need_swap_handler(session_need_swap);
    reset_top_stmt_and_plan();
    return false;
  } catch (...) {  // if not catch exception, the throw may lead dbscale
                   // coredump
    LOG_ERROR(
        "info schema mirror table silence reload get exception for sql [%s].\n",
        sql.c_str());
    set_is_silence_ok_stmt(false);
    set_binary_resultset(false);
    set_need_swap_handler(session_need_swap);
    reset_top_stmt_and_plan();
    return false;
  }
  set_is_silence_ok_stmt(false);
  set_need_swap_handler(session_need_swap);
  set_binary_resultset(false);
  remove_all_connection_to_free();
  warning_spaces.clear();
  return true;
}

void MySQLSession::add_user_var_value(string user_var, string value,
                                      bool is_str, bool skip_add_bslash) {
  user_var_origin_map[user_var] = value;

  string val;
  if (is_str) {
    string tmp(value);
    if (!skip_add_bslash) {
      find_and_insert_str(&tmp, "\\", "\\");
      find_and_insert_str(&tmp, "'", "\\");
    } else {
      find_and_insert_bslash(&tmp, "'");
    }
    val.append("'");
    val.append(tmp.c_str());
    val.append("'");
  } else {
    val = value;
  }
  user_var_map[user_var] = val;
  LOG_DEBUG("Add user variable [%s] value to [%s].\n", user_var.c_str(),
            value.c_str());
}

void MySQLSession::flush_acl() {
  Backend *backend = Backend::instance();
  backend->set_schema_acl_info_for_user(get_username(), this);
  backend->set_table_acl_info_for_user(get_username(), this);
  backend->get_auth_info_for_user(get_username(), this);
}

void MySQLSession::get_user_not_allow_operation_now_str(string &ret) {
  for (map<int, int>::iterator it = user_not_allow_operation_time.begin();
       it != user_not_allow_operation_time.end(); it++) {
    string from = get_hms_str(it->first);
    string to = get_hms_str(it->second);
    ret = ret + "[" + from + " - " + to + "]  ";
  }
}

bool MySQLSession::is_user_not_allow_operation_now() {
  if (user_not_allow_operation_time.empty()) {
    return false;
  }
  ACE_Date_Time now(ACE_OS::gettimeofday());
  long cur_hour = now.hour();
  long cur_minute = now.minute();
  long cur_second = now.second();
  int now_sec = cur_hour * 3600 + cur_minute * 60 + cur_second;
  map<int, int>::iterator it = user_not_allow_operation_time.begin();
  for (; it != user_not_allow_operation_time.end(); it++) {
    if (now_sec >= it->first && now_sec <= it->second) {
      return true;
    }
  }
  return false;
}

void MySQLSession::get_using_conn_ids(
    map<DataServer *, list<uint32_t> > &thread_ids) {
  using_conns_mutex.acquire();
  set<Connection *>::iterator it = using_conns.begin();
  for (; it != using_conns.end(); it++) {
    thread_ids[(*it)->get_server()].push_back((*it)->get_thread_id());
  }
  using_conns_mutex.release();
}

void MySQLSession::save_postponed_ok_packet(Packet *p,
                                            bool need_set_packet_number) {
  switch (postponed_ok_packet_state) {
    case START_POSTPONE_PACKET:
      postponed_ok_packet_state = BEGIN_OK_PACKET;
      break;
    case BEGIN_OK_PACKET:
      postponed_ok_packet_state = DML_OK_PACKET;
      break;
    case DML_OK_PACKET:
      postponed_ok_packet_state = COMMIT_OK_PACKET;
    default:
      break;
  }
  LOG_DEBUG("Try save postponed ok packet, state: [%d]\n",
            postponed_ok_packet_state);
  if (postponed_ok_packet_state != DML_OK_PACKET) return;
  MySQLOKResponse res(p);
  res.unpack();
  LOG_DEBUG("Save postponed ok packet, affacted rows:[%d]\n",
            res.get_affected_rows());
  res.pack(&postponed_ok_packet);
  postponed_ok_packet_need_set_packet_number = need_set_packet_number;
}

map<string, string> MySQLSession::get_character_set_var_map() {
  map<string, string> charactor_set_var_map;
  if (session_var_map.count("CHARACTER_SET_CLIENT"))
    charactor_set_var_map["CHARACTER_SET_CLIENT"] =
        session_var_map["CHARACTER_SET_CLIENT"];
  if (session_var_map.count("CHARACTER_SET_CONNECTION"))
    charactor_set_var_map["CHARACTER_SET_CONNECTION"] =
        session_var_map["CHARACTER_SET_CONNECTION"];
  if (session_var_map.count("CHARACTER_SET_RESULTS"))
    charactor_set_var_map["CHARACTER_SET_RESULTS"] =
        session_var_map["CHARACTER_SET_RESULTS"];
  return charactor_set_var_map;
}

int MySQLSession::get_matched_or_change_message(string str, string &message) {
  // message example: Rows matched: 72  Changed: 0  Warnings: 0
  int rows_count = 0;
  string rows_matched_change = "";
  try {
    size_t p = message.find(str);
    if (p == string::npos) return -1;
    p += str.size() + 1;
    size_t p1 = message.find(" ", p);
    if (p1 == string::npos) return -1;
    rows_matched_change = message.substr(p, p1 - p);
    if (!rows_matched_change.empty()) {
      rows_count = std::stoi(rows_matched_change);
      return rows_count;
    }
  } catch (exception &e) {
    LOG_ERROR("Get Message [%s] rows matched [%s] count [%d]\n",
              message.c_str(), rows_matched_change.c_str(), rows_count);
    return -1;
  }
  return rows_count;
}
}  // namespace mysql
}  // namespace dbscale
