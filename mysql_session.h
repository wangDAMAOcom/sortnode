#ifndef DBSCALE_MYSQL_SESSION_H
#define DBSCALE_MYSQL_SESSION_H

/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/Synch.h>
#include <handler.h>
#include <option.h>
#include <packet.h>
#include <session.h>
#include <stdio.h>
#include <string.h>

#include <boost/assign.hpp>

#include "mysql_compress.h"
#include "mysql_request.h"
#include "transfer_rule.h"

using namespace dbscale::sql;

namespace dbscale {
namespace mysql {

#define SCHEMA_LEN 256
#define USER_LEN 256
#define PASSWORD_LEN 256
#define MAX_RECORD_SQL_NUMS 200

struct execute_param {
  bool is_null;
  MySQLColumnType type;
  string value;
};

class prepare_stmt_info {
 public:
  prepare_stmt_info() {
    query_sql = "";
    st_type = STMT_NON;
    para_num = 0;
  }
  prepare_stmt_info(string sql, stmt_type type, uint16_t num) {
    query_sql = sql;
    st_type = type;
    para_num = num;
  }
  string query_sql;
  stmt_type st_type;
  uint16_t para_num;
};

struct auto_inc_param {
  auto_inc_param(int64_t v, int64_t n, int t) {
    num = n;
    value = v;
    times = t;
  }
  auto_inc_param() {
    num = 0;
    value = 0;
    times = 0;
  }
  int64_t num;    // the valid number of increment value
  int64_t value;  // the current increment value
  // TODO:Reduce the auto_inc_param.times in some case.
  int times;  // the times the session has get the table's auto increment value
              // from dataspace
};

struct servercomp {
  bool operator()(pair<const char *, unsigned int> lhs,
                  pair<const char *, unsigned int> rhs) const {
    if (lhs.first < rhs.first) return true;
    if (lhs.second < rhs.second) return true;
    return false;
  }
};

struct long_data_packet_desc {
  char *data;
  uint32_t len;
};

class MySQLPrepareItem : public PrepareItem {
 public:
  MySQLPrepareItem(string _query_sql, uint32_t r_stmt_id, uint32_t w_stmt_id,
                   uint16_t _num_params, uint16_t _num_columns)
      : PrepareItem(_query_sql),
        r_stmt_id(r_stmt_id),
        w_stmt_id(w_stmt_id),
        num_params(_num_params),
        num_columns(_num_columns),
        prepare_packet(NULL),
        read_only(false) {
    meta.st_type = STMT_NON;
  }
  MySQLPrepareItem()
      : r_stmt_id(0), w_stmt_id(0), prepare_packet(NULL), read_only(false) {
    meta.st_type = STMT_NON;
  }
  ~MySQLPrepareItem() {
    if (prepare_packet) {
      delete prepare_packet;
    }
  }
  uint32_t get_r_stmt_id() { return r_stmt_id; }
  void set_r_stmt_id(uint32_t stmt_id) { this->r_stmt_id = stmt_id; }
  uint32_t get_w_stmt_id() { return w_stmt_id; }
  void set_w_stmt_id(uint32_t stmt_id) { this->w_stmt_id = stmt_id; }
  uint16_t get_num_params() { return num_params; }
  list<string> *get_split_prepare_sql() { return &split_prepare_sql; }
  void set_split_prepare_sql(list<string> &split_prepare_sql) {
    this->split_prepare_sql = split_prepare_sql;
  }
  void set_num_params(uint16_t num_params) { this->num_params = num_params; }
  uint16_t get_num_columns() { return num_columns; }
  void set_num_columns(uint16_t num_columns) {
    this->num_columns = num_columns;
  }
  list<MySQLColumnType> *get_param_type_list() { return &param_type_list; }
  list<MySQLColumnType> *get_column_type_list() { return &column_type_list; }
  Packet *get_prepare_packet() { return prepare_packet; }
  void set_prepare_packet(Packet *packet) { this->prepare_packet = packet; }
  void set_read_only(bool b) { read_only = b; }
  bool is_read_only() { return read_only; }

  void clear_all_long_data_values() { long_data_packets.clear(); }

  void add_long_data_packet(int pos, char *data, uint32_t len) {
    long_data_packet_desc tmp_pd;
    tmp_pd.data = data;
    tmp_pd.len = len;
    long_data_packets[pos].push_back(tmp_pd);
  }

  void reset_stmt() {
    map<int, vector<long_data_packet_desc> >::iterator it =
        long_data_packets.begin();
    for (; it != long_data_packets.end(); ++it) {
      vector<long_data_packet_desc>::iterator it_ld = it->second.begin();
      for (; it_ld != it->second.end(); ++it_ld) {
        delete it_ld->data;
      }
      it->second.clear();
    }
    long_data_packets.clear();
  }

  vector<long_data_packet_desc> *get_long_data_packet(int pos) {
    if (long_data_packets.count(pos))
      return &long_data_packets[pos];
    else
      return NULL;
  }

  void set_prepare_stmt_group_handle_meta(prepare_stmt_group_handle_meta &m) {
    meta = m;
  }

  prepare_stmt_group_handle_meta &get_prepare_stmt_group_handle_meta() {
    return meta;
  }

  void store_sql_into_group_com_query(const char *sql) {
    if (meta.st_type == STMT_INSERT || meta.st_type == STMT_REPLACE) {
      if (group_com_query_stmt_str.empty())
        group_com_query_stmt_str.assign(sql);
      else {
        group_com_query_stmt_str.append(" , ");
        group_com_query_stmt_str.append(sql + meta.keyword_values_end_pos);
      }
    }
  }

  string &get_group_com_query_stmt_str() { return group_com_query_stmt_str; }

 private:
  uint32_t r_stmt_id;
  uint32_t w_stmt_id;
  list<string> split_prepare_sql;
  uint16_t num_params;
  uint16_t num_columns;
  list<MySQLColumnType> param_type_list;
  list<MySQLColumnType> column_type_list;
  map<int, vector<long_data_packet_desc> > long_data_packets;
  Packet *prepare_packet;
  bool read_only;
  prepare_stmt_group_handle_meta meta;
  string group_com_query_stmt_str;
};
class MySQLExecuteItem : public ExecuteItem {
 public:
  uint32_t get_stmt_id() { return stmt_id; }
  void set_stmt_id(uint32_t stmt_id) { this->stmt_id = stmt_id; }
  const char *get_name() { return name; }
  void set_name(const char *name) { this->name = name; }
  uint8_t get_flags() { return flags; }
  void set_flags(uint8_t flags) { this->flags = flags; }
  list<execute_param> *get_param_list() { return &param_list; }

 private:
  uint32_t stmt_id;
  const char *name;
  uint8_t flags;
  list<execute_param> param_list;
};

class MySQLSession : public Session {
 public:
  MySQLSession();

  virtual ~MySQLSession();

  lock_table_node *get_lock_head() const { return lock_head; }
  const char *get_username() const { return username; }
  void set_username(const char *value) {
    copy_string(username, value, sizeof(username));
  }
  const char *get_password() const { return password; }
  void set_password(const char *value) {
    copy_string(password, value, sizeof(password));
  }
  const char *get_schema() const { return schema; }
  void get_schema(string &s) const { s.assign(schema, SCHEMA_LEN + 1); }
  void set_schema(const char *value) {
    if (!value) value = default_login_schema;
    copy_string(schema, value, sizeof(schema));
    set_schema_name_md5(calculate_md5(schema));
  }

  void save_sp_error_response_info(Packet *packet) {
    if (!packet) return;
    MySQLErrorResponse response(packet);
    response.unpack();
    sp_error_code = response.get_error_code();
    sp_error_sql_state = response.get_sqlstate();
    sp_error_message = response.get_error_message();
  }
  void save_sp_error_response_info(unsigned int error_code,
                                   const char *error_sqlstate,
                                   const char *error_message) {
    sp_error_code = error_code;
    sp_error_sql_state = error_sqlstate;
    sp_error_message = error_message;
  }
  void reset_sp_error_response_info() {
    sp_error_code = 0;
    sp_error_sql_state = "";
    sp_error_message = "";
  }
  unsigned int get_sp_error_code() { return sp_error_code; }
  string get_sp_error_state() { return sp_error_sql_state; }
  string get_sp_error_message() { return sp_error_message; }

  bool check_conn_in_kept_connections(DataSpace *dataspace, Connection *conn);
  Connection *get_kept_connection(DataSpace *dataspace);
  bool check_same_kept_connection(DataSpace *dataspace1, DataSpace *dataspace2);

  bool is_dataspace_keeping_connection(DataSpace *dataspace) {
    if (kept_connections.count(dataspace)) {
      return true;
    }
    return false;
  }

  CrossNodeJoinTable *reuse_session_kept_join_tmp_table(
      CrossNodeJoinTablePool *pool);
  void set_kept_cross_join_tmp_table(CrossNodeJoinTable *table);
  void add_back_kept_cross_join_tmp_table();
  void set_kept_connection(DataSpace *dataspace, Connection *conn,
                           bool need_session_mutex = true);

  bool remove_kept_connection(DataSpace *dataspace, bool clean_dead = false);

  void remove_all_connection_to_free();

  void remove_all_connection_to_dead();

  void remove_all_sp_worker();

  void remove_warning_conn_to_free();
  void remove_non_warning_conn_to_free();
  void clear_dataspace_warning(DataSpace *space, bool clean_conn = true);

  void rollback();
  Connection *get_cluster_xa_transaction_conn();
  void cluster_xa_rollback();
  void unlock();
  void implicit_execute_stmt(const char *sql);

  void set_compress(bool compress) { this->compress = compress; }
  bool get_compress() { return compress; }
  void set_local_infile(bool local_infile) {
    this->local_infile = local_infile;
  }
  bool get_local_infile() { return local_infile; }

  bool session_is_in_prepare() {
    mutex.acquire();
    map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
    for (; it != kept_connections.end(); it++) {
      if (it->second->is_in_prepare()) {
        mutex.release();
        return true;
      }
    }
    mutex.release();
    return false;
  }
  uint32_t get_prepare_num() {
    mutex.acquire();
    uint32_t num = 0;
    map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
    for (; it != kept_connections.end(); it++) {
      num += it->second->get_prepare_num();
    }
    mutex.release();
    return num;
  }

  map<DataSpace *, Connection *> get_kept_connections() {
    mutex.acquire();
    map<DataSpace *, Connection *> ret = kept_connections;
    mutex.release();
    return ret;
  }

  void get_extra_info(string &extra_info) {
    mutex.acquire();
    map<DataSpace *, Connection *>::iterator tmp_it;
    for (tmp_it = kept_connections.begin(); tmp_it != kept_connections.end();
         tmp_it++) {
      extra_info = extra_info + tmp_it->first->get_name() + " ";
    }
    mutex.release();
  }

  void get_conn_list_str(stringstream &kept_conn_list) {
    mutex.acquire();
    kept_conn_list.str("");
    kept_conn_list.clear();
    map<DataSpace *, Connection *>::iterator tmp_it;
    for (tmp_it = kept_connections.begin(); tmp_it != kept_connections.end();
         tmp_it++) {
      kept_conn_list << tmp_it->second->get_server()->get_host_ip() << "-"
                     << tmp_it->second->get_server()->get_port() << "-"
                     << tmp_it->second->get_thread_id() << ";";
    }
    mutex.release();
  }

  void get_using_conn_ids(map<DataServer *, list<uint32_t> > &thread_ids);

  // record connections that executed sql affected_rows > 0
  void record_xa_modified_conn(Connection *conn) {
    if (optimize_xa_non_modified_sql && enable_xa_transaction &&
        get_session_option("close_session_xa").int_val == 0 &&
        check_for_transaction() && !is_in_transaction_consistent() &&
        !is_in_cluster_xa_transaction()) {
      xa_modified_conn_mutex.acquire();
      if (!xa_modified_conn.count(conn)) {
        xa_modified_conn.insert(conn);
      }
      xa_modified_conn_mutex.release();
    }
  }

  bool is_xa_modified_conn(Connection *conn) {
    if (xa_modified_conn.count(conn)) {
      return true;
    }
    return false;
  }
  void clean_xa_modified_conn_map() {
    xa_modified_conn.clear();
    LOG_DEBUG("xa_modified_conn cleaned.\n");
  }
  map<Connection *, bool> *get_tx_record_redo_log_kept_connections() {
    return &tx_record_redo_log_kept_connection;
  }
  void set_tx_record_redo_log_kept_connections(Connection *conn) {
    tx_record_redo_log_kept_connection[conn] = true;
  }
  void set_tx_record_redo_log_connection_to_dead(Connection *conn) {
    if (conn) {
      if (tx_record_redo_log_kept_connection.count(conn))
        tx_record_redo_log_kept_connection[conn] = false;
    }
  }
  void clean_all_tx_record_redo_log_kept_connections() {
    map<Connection *, bool>::iterator it =
        tx_record_redo_log_kept_connection.begin();
    for (; it != tx_record_redo_log_kept_connection.end(); it++) {
      if (it->second) {
        it->first->get_pool()->add_back_to_free(it->first);
      } else {
        it->first->get_pool()->add_back_to_dead(it->first);
      }
    }
    tx_record_redo_log_kept_connection.clear();
  }
  CharsetType get_client_charset_type() {
    CharsetType ret = CHARSET_TYPE_OTHER;
    if (session_var_map.count("CHARACTER_SET_CLIENT")) {
      if (session_var_map["CHARACTER_SET_CLIENT"].find("gbk") != string::npos ||
          session_var_map["CHARACTER_SET_CLIENT"].find("GBK") != string::npos)
        ret = CHARSET_TYPE_GBK;
      else if (session_var_map["CHARACTER_SET_CLIENT"].find("gb18030") !=
                   string::npos ||
               session_var_map["CHARACTER_SET_CLIENT"].find("GB18030") !=
                   string::npos)
        ret = CHARSET_TYPE_GB18030;
    }
    return ret;
  }
  unsigned char get_client_charset() const { return client_charset; }
  void set_client_charset(unsigned char charset) { client_charset = charset; }
  map<string, string> get_character_set_var_map();

  map<string, string> *get_session_var_map() { return &session_var_map; }
  void flush_acl();
  void set_session_var_map(string var, string value) {
    session_var_map[var] = value;
    set_session_var_map_md5(calculate_md5(&session_var_map));
    return;
  }

  void set_session_var_map(map<string, string> session_var_map) {
    this->session_var_map = session_var_map;
    set_session_var_map_md5(calculate_md5(&session_var_map));
  }

  string get_session_var_value(string var) {
    if (session_var_map.count(var)) {
      return session_var_map[var];
    }
    return "";
  }

  void prepare_set_session_next_trx_level(const string &value) {
    session_next_trx_level = value;
    LOG_DEBUG("prepare_set_session_next_trx_level = %s\n", value.c_str());
  }

  void overwrite_session_next_trx_level() {
    session_next_trx_level.clear();
    session_old_trx_level.clear();
    LOG_DEBUG("overwrite_session_next_trx_level\n");
  }

  void set_session_next_trx_level() {
    if (!session_next_trx_level.empty()) {
      // set trx more than once, but not reset
      if (session_old_trx_level.empty()) {
        session_old_trx_level = get_session_var_value("TRANSACTION_ISOLATION");
      }
      if (session_old_trx_level != session_next_trx_level) {
        set_session_var_map("TRANSACTION_ISOLATION", session_next_trx_level);
      } else {
        session_old_trx_level.clear();
      }
      LOG_DEBUG("set_session_next_trx_level = %s, session_old_trx_level = %s\n",
                session_next_trx_level.c_str(), session_old_trx_level.c_str());
      session_next_trx_level.clear();
    }
  }

  void reset_session_next_trx_level() {
    if (!session_old_trx_level.empty()) {
      set_session_var_map("TRANSACTION_ISOLATION", session_old_trx_level);
      LOG_DEBUG("reset_session_next_trx_level = %s\n",
                session_old_trx_level.c_str());
      session_old_trx_level.clear();
    }
  }

  void set_var_by_ref(string user_var, string ref_var) {
    if (user_var_map.count(ref_var)) {
      user_var_map[user_var] = user_var_map[ref_var];
      user_var_origin_map[user_var] = user_var_origin_map[ref_var];
    }
  }
  void add_user_var_value(string user_var, string value, bool is_str,
                          bool skip_add_bslash = false);

  void remove_user_var_by_name(string user_var) {
    if (user_var_map.count(user_var)) user_var_map.erase(user_var);
    if (user_var_origin_map.count(user_var))
      user_var_origin_map.erase(user_var);
  }
  map<string, string> *get_user_var_map() { return &user_var_map; }

  string *get_user_var_value(string user_var) {
    if (user_var_map.count(user_var)) {
      return &user_var_map[user_var];
    }
    return NULL;
  }
  string get_user_var_origin_value(string user_var) {
    if (user_var_origin_map.count(user_var))
      return user_var_origin_map[user_var];

    return "";
  }

  void set_var_sql(string sql) { var_sql = sql; }
  string get_var_sql() { return var_sql; }

  void add_savepoint(const char *sql);
  void remove_all_savepoint();
  void remove_savepoint(const char *point);

  lock_table_full_item *add_lock_table(lock_table_item *item);
  void remove_all_lock_tables();
  bool contains_lock_table(const char *table, const char *schema,
                           const char *alias);
  void reset_lock_table_state();
  void re_init_session();
  void reset_session_to_beginning();
  list<char *> get_all_savepoint() { return savepoint_list; }
  void set_found_rows(uint64_t num) {
    if (get_is_simple_direct_stmt())
      found_rows = num;
    else {
      mutex_num.acquire();
      found_rows = num;
      mutex_num.release();
    }
  }
  void add_found_rows(uint64_t num) {
    mutex_num.acquire();
    found_rows += num;
    mutex_num.release();
  }
  uint64_t get_found_rows() { return found_rows; }
  void set_real_fetched_rows(
      uint64_t num)  // to record real returned rows of a select stmt
  {
    real_fetched_rows = num;
  }
  uint64_t get_real_fetched_rows() { return real_fetched_rows; }

  void set_error_packet(Packet *packet) { error_packet = packet; }
  Packet *get_error_packet() { return error_packet; }
  void set_result_packet(Packet *packet) { result_packet = packet; }
  Packet *get_result_packet() { return result_packet; }
  void set_xa_id(unsigned long id) { xid = id; }
  unsigned long get_xa_id() { return xid; }
  unsigned int get_next_seq_id() { return ++seq_id; }
  void reset_seq_id() { seq_id = 0; }

  void execute_xa_rollback();

  void set_user_addr(const char *username, InetAddr addr) {
    set_user_name(username);
    user_addr.clear();
    char addr_str[BUFSIZ];
    addr.addr_to_string(addr_str, sizeof(addr_str));
    user_addr.append(username);
    user_addr.append("@");
    user_addr.append(addr_str);
    char host_ip[INET6_ADDRSTRLEN + 1];
    addr.get_host_addr(host_ip, INET6_ADDRSTRLEN);
    client_ip.assign(host_ip);
  }
  void init_user_priority() {
    Backend *backend = Backend::instance();
    backend->acquire_user_priority_mutex();
    user_priority = backend->get_user_priority(get_user_name());
    backend->release_user_priority_mutex();
  }

  string get_client_ip() { return client_ip; }
  const char *get_user_addr() { return user_addr.c_str(); }
  void get_user_addr(string &s) { s.assign(user_addr); }

  // need enable-acl
  bool is_contain_schema(string &schema_name, bool with_mutex = true) {
    if (schema_name.empty()) return true;
    // INFORMATION_SCHEMA is always case insensitive
    if (strcasecmp(schema_name.c_str(), INFORMATION_SCHEMA_STR) == 0)
      return true;
    if (with_mutex) {
      auth_schema_set_mutex.acquire();
    }
    bool ret = false;
    set<string>::iterator it = auth_schema_set.begin();
    for (; it != auth_schema_set.end(); it++) {
      if (!lower_case_table_names &&
          strcmp((*it).c_str(), schema_name.c_str()) == 0) {
        ret = true;
        break;
      } else if (lower_case_table_names &&
                 strcasecmp((*it).c_str(), schema_name.c_str()) == 0) {
        ret = true;
        break;
      }
    }
    if (with_mutex) auth_schema_set_mutex.release();
    return ret;
  }

  set<string> &get_auth_schema_set() {
    ACE_Guard<ACE_Thread_Mutex> guard(auth_schema_set_mutex);
    return auth_schema_set;
  }

  void set_auth_schema_set(set<string> &_auth_schema_set) {
    ACE_Guard<ACE_Thread_Mutex> guard(auth_schema_set_mutex);
    auth_schema_set = _auth_schema_set;
#ifdef DEBUG
    set<string>::iterator it = auth_schema_set.begin();
    for (; it != auth_schema_set.end(); it++) {
      LOG_DEBUG("session %@ add auth schema [%s]\n", this, (*it).c_str());
    }
#endif
  }

  // need enable-acl
  virtual void add_auth_schema_to_set(string schema_name) {
    ACE_Guard<ACE_Thread_Mutex> guard(auth_schema_set_mutex);
    if (!is_contain_schema(schema_name, false)) {
      if (lower_case_table_names) {
        transform(schema_name.begin(), schema_name.end(), schema_name.begin(),
                  ::tolower);
      }
      auth_schema_set.insert(schema_name);
      LOG_DEBUG("session %@ add schema [%s] to its own auth_schema_set\n", this,
                schema_name.c_str());
    }
  }
  void delete_auth_schema_set(string schema_name) {
    ACE_Guard<ACE_Thread_Mutex> guard(auth_schema_set_mutex);
    if (is_contain_schema(schema_name, false)) {
      if (lower_case_table_names) {
        transform(schema_name.begin(), schema_name.end(), schema_name.begin(),
                  ::tolower);
      }
      auth_schema_set.erase(schema_name);
      LOG_DEBUG("session delete auth schema [%s]\n", schema_name.c_str());
    }
  }
  void set_user_not_allow_operation_time(map<int, int> times) {
    user_not_allow_operation_time = times;
  }
  void get_user_not_allow_operation_now_str(string &ret);
  bool is_user_not_allow_operation_now();

  void set_schema_acl_info(
      map<string, SchemaACLType, strcasecomp> *_schema_acl_map) {
    if (schema_acl_map) {
      delete schema_acl_map;
      schema_acl_map = NULL;
    }
    schema_acl_map = new map<string, SchemaACLType, strcasecomp>;
    *schema_acl_map = *_schema_acl_map;
  }
  map<string, SchemaACLType, strcasecomp> *get_schema_acl_map() {
    return schema_acl_map;
  }
  SchemaACLType get_schema_acl_type(const char *schema_name) {
    if (schema_acl_map->count(schema_name))
      return (*schema_acl_map)[schema_name];
    else
      return ACL_TYPE_EMPTY;
  }

  void set_table_acl_info(
      map<string, TableACLType, strcasecomp> *_table_acl_map) {
    if (table_acl_map) {
      delete table_acl_map;
      table_acl_map = NULL;
    }
    table_acl_map = new map<string, TableACLType, strcasecomp>;
    *table_acl_map = *_table_acl_map;
  }
  map<string, TableACLType, strcasecomp> *get_table_acl_map() {
    return table_acl_map;
  }

  TableACLType get_table_acl_type(string &full_table_name) {
    if (table_acl_map->count(full_table_name))
      return (*table_acl_map)[full_table_name];
    return ACL_TYPE_EMPTY;
  }

  Connection *check_kept_connection_for_same_server(DataSpace *dataspace);

  void clean_up_redo_logs_map() {
    // This function will only be invoked one thread at the beginning of xa
    // transaction, so no mutex need.
    redo_logs_map.clear();
  }

  void record_redo_log_to_buffer(DataSpace *space, string &sql) {
    redo_logs_map_mutex.acquire();
    redo_logs_map[space].push_back(sql);
    redo_logs_map_mutex.release();
  }

  void record_relate_source_redo_log(
      map<DataSpace *, Connection *> *kept_connections);
  void spread_redo_log_to_buffer(const char *sql,
                                 bool is_relate_source = false);

  list<string> *get_redo_logs_map(DataSpace *space) {
    // This function will only be invoked during xa prepare. There is only one
    // thread at that time, so no mutex need.
    if (redo_logs_map.count(space)) return &(redo_logs_map[space]);
    return NULL;
  }
  map<DataSpace *, list<string> > *get_redo_logs_map() {
    // This function will only be invoked during xa prepare. There is only one
    // thread at that time, so no mutex need.
    if (!redo_logs_map.empty()) return &redo_logs_map;
    return NULL;
  }

  bool get_is_xa_prepared() { return is_xa_prepared; };
  void set_xa_prepared() { is_xa_prepared = true; }
  void reset_is_xa_prepared() { is_xa_prepared = false; };

  DataSpace *get_space_from_kept_space_map(DataSpace *space) {
    DataSpace *ret = NULL;
    mutex.acquire();
    if (kept_space_map->count(space)) ret = (*kept_space_map)[space];
    mutex.release();
    return ret;
  }

  bool get_need_wait_for_consistence_point() {
    // only commit transaction will invoke this function, no need add mutex
    LOG_DEBUG("get_need_wait_for_consistence_point server num=%d\n",
              modify_server_set.size());
    return modify_server_set.size() > 1 ? true : false;
  }

  bool get_xa_need_wait_for_consistence_point() {
    // only commit transaction will invoke this function, no need add mutex
    LOG_DEBUG("get_xa_need_wait_for_consistence_point server num=%d\n",
              modify_server_set.size());
    return modify_server_set.size() >= 1 ? true : false;
  }

  void record_modify_server(const char *server_name,
                            unsigned int virtual_machine_id) {
    mutex_record_modify_server.acquire();
    if (!get_need_wait_for_consistence_point())
      modify_server_set.insert(make_pair(server_name, virtual_machine_id));
    mutex_record_modify_server.release();
  }
  void clear_modify_server() {
    // only commit or rollback transaction will invoke this function, no need
    // add mutex
    modify_server_set.clear();
  }

  void acquire_using_conn_mutex() {
    /*
    if (get_is_simple_direct_stmt())
      return;
    */
    using_conns_mutex.acquire();
  }

  void release_using_conn_mutex() {
    /*
    if (get_is_simple_direct_stmt())
      return;
    */
    using_conns_mutex.release();
  }

  void killall_using_conns();
  bool check_in_in_xa_commit();

  void set_option_values();
  unsigned int get_option_value_uint(string option_name) {
    return option_value_uint[option_name];
  }
  unsigned long get_option_value_ul(string option_name) {
    return option_value_ul[option_name];
  }
  MySQLPrepareItem *get_prepare_item(uint32_t stmt_id) {
    if (prepare_map.count(stmt_id)) return prepare_map[stmt_id];
    return NULL;
  }
  void set_prepare_item(uint32_t stmt_id, MySQLPrepareItem *item) {
    prepare_map[stmt_id] = item;
  }
  void clean_target_prepare_stmt(uint32_t id) {
    if (prepare_map.count(id)) {
      prepare_map[id]->get_group_com_query_stmt_str().clear();
    }
  }
  void clean_all_group_sql_of_prepare_stmt() {
    map<uint32_t, MySQLPrepareItem *>::iterator it = prepare_map.begin();
    for (; it != prepare_map.end(); it++) {
      it->second->get_group_com_query_stmt_str().clear();
    }
  }
  MySQLPrepareItem *get_prepare_item(const char *name) {
    string prepare_name(name);
    boost::to_lower(prepare_name);
    if (prepare_map_name.count(prepare_name))
      return prepare_map_name[prepare_name];
    return NULL;
  }
  /*
   * not matter prepare sucess or fail
   * the prepare with the same name
   * will be cleared
   */
  void set_prepare_item(const char *name, MySQLPrepareItem *item) {
    clean_prepare_item(name);
    string prepare_name(name);
    boost::to_lower(prepare_name);
    prepare_map_name[prepare_name] = item;
  }
  void clean_prepare_item(const char *name) {
    string prepare_name(name);
    boost::to_lower(prepare_name);
    if (prepare_map_name.count(prepare_name)) {
#ifdef DEBUG
      LOG_DEBUG("session %@ clean_prepare_item name [%s]\n", this, name);
#endif
      MySQLPrepareItem *prepare_item = prepare_map_name[prepare_name];
      prepare_map_name.erase(prepare_name);
      delete prepare_item;
    }
  }
  void clean_prepare_item(uint32_t stmt_id) {
    if (prepare_map.count(stmt_id)) {
#ifdef DEBUG
      LOG_DEBUG("session %@ clean_prepare_item stmt_id [%u]\n", this, stmt_id);
#endif
      MySQLPrepareItem *prepare_item = prepare_map[stmt_id];
      prepare_map.erase(stmt_id);
      delete prepare_item;
      if (column_types.count(stmt_id)) {
        delete column_types[stmt_id];
        column_types.erase(stmt_id);
      }
    }
  }
  void clean_all_prepare_items() {
    map<uint32_t, MySQLPrepareItem *>::iterator it;
    for (it = prepare_map.begin(); it != prepare_map.end();) {
      uint32_t stmt_id = it->first;
      delete it->second;
      prepare_map.erase(it++);
      if (column_types.count(stmt_id)) {
        delete column_types[stmt_id];
        column_types.erase(stmt_id);
      }
    }
    map<string, MySQLPrepareItem *>::iterator it_name;
    for (it_name = prepare_map_name.begin(); it_name != prepare_map_name.end();
         it_name++) {
      delete it_name->second;
    }
    prepare_map_name.clear();
  }

  vector<MySQLColumnType> *get_column_types(unsigned int id) {
    if (column_types.count(id)) return column_types[id];

    return NULL;
  }
  void add_column_types(unsigned int id, vector<MySQLColumnType> *types) {
#ifdef DEBUG
    if (column_types.count(id))
      LOG_ERROR(
          "dup id may be from different mysql server which may cause prepare "
          "fail.\n");
#endif
    column_types[id] = types;
  }

  void set_dbscale_test_info(const char *case_name, const char *operation,
                             const char *param) {
    dbscale_test.test_case_name.clear();
    dbscale_test.test_case_name.append(case_name);
    dbscale_test.test_case_operation.clear();
    dbscale_test.test_case_operation.append(operation);
    dbscale_test.test_case_param.clear();
    if (param) dbscale_test.test_case_param.append(param);
  }
  dbscale_test_info *get_dbscale_test_info() { return &dbscale_test; }

  void add_sp_worker(SPWorker *spw);
  void reset_all_nest_sp_counter();

  SPWorker *get_sp_worker(string &name);
  void remove_sp_worker(string &name);

  SPWorker *get_cur_sp_worker() { return cur_sp_worker; }
  void set_cur_sp_worker(SPWorker *spw);
  void pop_cur_sp_worker_one_recurse();

  void add_transaction_sql(const char *sql) {
    if (transaction_sqls_num > (unsigned int)MAX_RECORD_SQL_NUMS) {
      return;
    }
    transaction_sqls.push_back(sql);
    transaction_sqls_num++;
  }
  void clear_transaction_sqls() {
    transaction_sqls.clear();
    transaction_sqls_num = 0;
    LOG_DEBUG("Clear transaction sqls in current session.\n");
  }
  unsigned int get_transaction_sqls_num() { return transaction_sqls_num; }
  list<string> *get_transaction_sqls() { return &transaction_sqls; }
  void add_warning_dataspace(DataSpace *space, Connection *conn);

  /*Assueme there is no concurrency operation on warning_spaces for function
   * get_warning_dataspace, reset_warning_dataspaces,
   * is_contain_warning_dataspace.*/
  set<DataSpace *> get_warning_dataspaces() { return warning_spaces; }

  void reset_warning_dataspaces() { warning_spaces.clear(); }

  bool is_contain_warning_dataspace() { return !warning_spaces.empty(); }

  bool dataspace_has_warning(DataSpace *space) {
    bool ret = false;
    mutex.acquire();
    if (!is_contain_warning_dataspace()) {
      mutex.release();
      return ret;
    }
    ret = warning_spaces.count(space);
    LOG_DEBUG("Check space [%s] is a warning space with result %d\n",
              space->get_name(), ret ? 1 : 0);
    mutex.release();
    return ret;
  }

  void reset_warning_map() { warning_map.clear(); }

  TransferRuleManager *get_transfer_rule_manager() {
    return &transfer_rule_manager;
  }

  void reset_transfer_rule_manager() { transfer_rule_manager.reset(); }

  bool contains_odbc_conn();
  bool add_remove_sp_name(string &sp_name);
  void clear_all_sp_in_remove_sp_vector();
  void add_in_using_table(const char *schema_name, const char *table_name) {
    string full_name;
    splice_full_table_name(schema_name, table_name, full_name);
    check_table_mutex.acquire();
    in_using_table.insert(full_name);
    check_table_mutex.release();
  }
  bool is_table_in_using(string schema_name, string table_name) {
    string full_name;
    splice_full_table_name(schema_name, table_name, full_name);
    check_table_mutex.acquire();
    if (in_using_table.count(full_name)) {
      check_table_mutex.release();
      return true;
    }
    check_table_mutex.release();
    return false;
  }
  void clean_in_using_table() {
    if (is_keeping_connection()) {
      return;
    }
    check_table_mutex.acquire();
    in_using_table.clear();
    check_table_mutex.release();
  }
  void clean_all_using_table() {
    check_table_mutex.acquire();
    in_using_table.clear();
    check_table_mutex.release();
  }
  void reset_auto_increment_info(string &full_table_name) {
    if (auto_inc_value_map.count(full_table_name)) {
      auto_inc_value_map.erase(full_table_name);
    }
  }

  int64_t get_auto_inc_value(string &full_table_name) {
    if (auto_inc_value_map.count(full_table_name)) {
      if (auto_inc_value_map[full_table_name].num > 0) {
        auto_inc_value_map[full_table_name].num--;
        int64_t ret = auto_inc_value_map[full_table_name].value;
        auto_inc_value_map[full_table_name].value++;
        return ret;
      }
    }
    string error_msg = "no auto inc value information in session for table ";
    error_msg += full_table_name;
    LOG_ERROR("%s\n", error_msg.c_str());
    throw Error(error_msg.c_str());
    return 0;
  }
  bool set_auto_increment_value(string &full_table_name, int64_t parsed_val) {
    if (auto_inc_value_map.count(full_table_name)) {
      if (auto_inc_value_map[full_table_name].num > 0 &&
          auto_inc_value_map[full_table_name].num +
                  auto_inc_value_map[full_table_name].value >
              parsed_val) {
        auto_inc_value_map[full_table_name].num -=
            parsed_val + 1 - auto_inc_value_map[full_table_name].value;
        auto_inc_value_map[full_table_name].value = parsed_val + 1;
        return true;
      } else {
        auto_inc_value_map[full_table_name].num = 0;
      }
    }
    return false;
  }

  bool is_contain_auto_inc_value(string &full_table_name) {
    if (auto_inc_value_map.count(full_table_name)) {
      if (auto_inc_value_map[full_table_name].num > 0) {
        return true;
      }
    }
    return false;
  }

  int get_insert_inc_times(string &full_table_name) {
    if (auto_inc_value_map.count(full_table_name)) {
      return auto_inc_value_map[full_table_name].times;
    }
    return 0;
  }

  void set_auto_inc_value(string &full_table_name, int64_t value, int64_t num,
                          int times) {
    if (auto_inc_value_map.count(full_table_name)) {
      auto_inc_value_map[full_table_name].num = num;
      auto_inc_value_map[full_table_name].value = value;
      auto_inc_value_map[full_table_name].times = times;
    } else {
      struct auto_inc_param param(value, num, times);
      auto_inc_value_map[full_table_name] = param;
    }
  }

  void get_prepare_stmt_info(uint32_t stmt_id, string &sql, stmt_type &type,
                             uint16_t &para_num) {
    if (prepare_stmt_id_map.count(stmt_id)) {
      sql = prepare_stmt_id_map[stmt_id]->query_sql;
      type = prepare_stmt_id_map[stmt_id]->st_type;
      para_num = prepare_stmt_id_map[stmt_id]->para_num;
    }
  }
  bool reset_prepare_column_type(uint32_t stmt_id, bool is_long_data = false) {
    if (is_long_data) {
      return remove_prepare_stmt_sql(stmt_id);
    }
    if (prepare_stmt_id_map.count(stmt_id)) {
      return true;
    }
    return false;
  }
  void set_prepare_stmt_sql(uint32_t stmt_id, string sql, stmt_type type,
                            uint16_t num) {
    prepare_stmt_info *ps = new prepare_stmt_info(sql, type, num);
    prepare_stmt_id_map[stmt_id] = ps;
  }
  bool remove_prepare_stmt_sql(uint32_t stmt_id) {
    if (prepare_stmt_id_map.count(stmt_id)) {
      delete prepare_stmt_id_map[stmt_id];
      prepare_stmt_id_map[stmt_id] = NULL;
      prepare_stmt_id_map.erase(stmt_id);
      if (column_types.count(stmt_id)) {
        delete column_types[stmt_id];
        column_types.erase(stmt_id);
      }
      return true;
    }
    return false;
  }

  bool set_and_get_backend_get_consistence(bool b) {
    consistence_mutex.acquire();
    backend_get_consistence = b;
    bool ret = is_consistence_transaction;
    consistence_mutex.release();
    return ret;
  }

  void start_commit_consistence_transaction() {
    consistence_mutex.acquire();
    if (backend_get_consistence) {
      Backend::instance()->wait_for_consistence_point();
      backend_get_consistence = false;
    }
    is_consistence_transaction = true;
    consistence_mutex.release();
  }

  void end_commit_consistence_transaction() {
    consistence_mutex.acquire();
    is_consistence_transaction = false;
    if (backend_get_consistence) {
      Backend::instance()->decrease_transaction_num_for_consistence_point();
    }
    consistence_mutex.release();
  }

  void set_no_status(bool b) { no_status = b; }

  string get_next_nest_sp_name(string &sp_name);
  int get_extra_snippet_of_sp_var();

  void clean_all_prepare_read_connection();
  void set_prepare_read_connection(uint32_t gsid, Connection *conn);
  void remove_prepare_read_connection(uint32_t gsid);
  void remove_prepare_read_connection(Connection *conn);
  Connection *get_prepare_read_connection(uint32_t gsid);
  Connection *check_connection_in_prepare_read_connection(Connection *conn);

  uint32_t get_gsid() { return ++gsid; }
  void increase_finished_federated_num() {
    federated_num_mutex.acquire();
    finished_federated_num++;
    federated_num_mutex.release();
  }

  void set_federated_num(int num) {
    federated_num_mutex.acquire();
    federated_num = num;
    federated_num_mutex.release();
  }
  bool federated_session_finished() {
    federated_num_mutex.acquire();
    bool ret = (federated_num == finished_federated_num);
    federated_num_mutex.release();
    return ret;
  }
  void reset_federated_finished() {
    federated_num_mutex.acquire();
    finished_federated_num = 0;
    federated_num_mutex.release();
  }
  Packet *get_client_buffer_packet() { return &client_buffer_packet; }

  bool has_client_buffer_packet() { return client_buffer_packet.length() != 0; }

  void do_create_trans_conn() {
    TimeValue timeout(connect_timeout);
    DataSource *datasource =
        Backend::instance()->get_auth_data_space()->get_data_source();
    trans_conn = new MySQLConnection((MySQLDriver *)Driver::get_driver(),
                                     datasource->get_master_server(), NULL,
                                     NULL, NULL, &timeout);
  }

 private:
  map<string, auto_inc_param> auto_inc_value_map;
  boost::unordered_map<uint32_t, prepare_stmt_info *> prepare_stmt_id_map;
  char username[USER_LEN + 1];
  string user_addr;
  char password[PASSWORD_LEN + 1];
  char schema[SCHEMA_LEN + 1];
  unsigned int sp_error_code;
  string sp_error_sql_state;
  string sp_error_message;
  string client_ip;
  int autocommit;
  uint64_t found_rows;
  uint64_t real_fetched_rows;
  Packet *error_packet;
  Packet *result_packet;
  bool compress;
  bool local_infile;
  map<DataSpace *, Connection *> kept_connections;
  list<CrossNodeJoinTable *> cross_join_tmp_tables;
  // this map stores the tmp_table kept by session,so other session can not get
  // this table from pool but this session can reuse this table.
  map<CrossNodeJoinTablePool *, vector<CrossNodeJoinTable *> >
      cross_join_tmp_tables_map;
  ACE_Thread_Mutex tmp_table_mutex;
  vector<CrossNodeJoinTable *> kept_dead_table;

  set<Connection *> xa_modified_conn;
  ACE_Thread_Mutex xa_modified_conn_mutex;
  map<Connection *, bool> tx_record_redo_log_kept_connection;
  map<string, string> session_var_map;
  string session_next_trx_level;
  string session_old_trx_level;
  map<string, string> user_var_map;
  map<string, string> user_var_origin_map;
  /* Map use to keep the dataspace map for the dataspaces share the same data
   * server. map->second is the dataspace which is kept in kept_connections.*/
  map<DataSpace *, DataSpace *> *kept_space_map;
  string var_sql;
  list<char *> savepoint_list;
  lock_table_node *lock_head;
  ACE_Thread_Mutex mutex;
  ACE_Thread_Mutex mutex_num;
  unsigned long xid;
  unsigned int seq_id;
  bool is_xa_prepared;
  set<string> auth_schema_set;
  ACE_Thread_Mutex auth_schema_set_mutex;
  map<int, int> user_not_allow_operation_time;
  map<string, SchemaACLType, strcasecomp> *schema_acl_map;
  map<string, TableACLType, strcasecomp> *table_acl_map;
  map<DataSpace *, list<string> > redo_logs_map;
  ACE_Thread_Mutex redo_logs_map_mutex;

  map<string, unsigned int> option_value_uint;
  map<string, unsigned long> option_value_ul;

  map<uint32_t, Connection *> prepare_read_connections;
  map<uint32_t, MySQLPrepareItem *> prepare_map;
  map<string, MySQLPrepareItem *> prepare_map_name;
  uint32_t gsid;
  map<unsigned int, vector<MySQLColumnType> *> column_types;
  dbscale_test_info dbscale_test;
  map<string, SPWorker *> sp_worker_map;
  map<string, vector<string> > sp_fake_name_record;
  map<string, int> sp_fake_name;
  set<pair<const char *, unsigned int>, servercomp> modify_server_set;
  ACE_Thread_Mutex mutex_record_modify_server;
  bool drop_current_schema;
  list<string> transaction_sqls;
  unsigned int transaction_sqls_num;
  SPWorker *cur_sp_worker;
  list<SPWorker *> cur_sp_worker_stack;
  set<DataSpace *> warning_spaces;
  map<DataSpace *, DataSpace *> warning_map;

  TransferRuleManager transfer_rule_manager;
  bool backend_get_consistence;
  bool is_consistence_transaction;
  ACE_Thread_Mutex consistence_mutex;
  int sp_extra_snippet_val;

  // move from handler
  ACE_Thread_Mutex remove_sp_vector_mutex;
  set<string> in_using_table;
  ACE_Thread_Mutex check_table_mutex;

  unsigned char client_charset;

 private:
  uint8_t packet_number;
  uint8_t compress_packet_number;
  uint64_t last_insert_id;
  uint32_t execute_stmt_id;
  bool binary_resultset;
  Packet *binary_packet;
  UncompressBuff uncompress_buffer;
  bool packet_from_buff;
  bool no_status;
  // client may receive more than one packet once,
  // use the buffer to store next packets
  Packet client_buffer_packet;

  int federated_num;
  int finished_federated_num;

  ACE_Thread_Mutex federated_num_mutex;

 public:
  void store_data_to_uncompress_buffer(size_t data_len,
                                       const char *copy_start_pos);
  void get_data_from_uncompress_buffer(Packet *packet);
  void init_uncompress_buffer(bool init_original_buff, bool has_extra_buff);
  void check_packet_number(uint8_t expect_number, uint8_t real_number);
  bool is_packet_from_buff() { return packet_from_buff; }
  void set_packet_from_buff(bool b) { packet_from_buff = b; }
  bool uncompress_buff_is_null() { return (uncompress_buffer.buff == NULL); }
  void uncompress_malloc_buff() {
    uncompress_buffer.buff = new char[uncompress_buffer_size + 1];
  }
  bool uncompress_has_data() { return uncompress_buffer.has_data; }
  bool is_binary_resultset() { return binary_resultset; }
  void set_binary_resultset(bool b) { binary_resultset = b; }
  Packet *get_binary_packet() { return binary_packet; }
  void set_binary_packet(Packet *packet) { binary_packet = packet; }
  uint32_t get_execute_stmt_id() { return execute_stmt_id; }
  void set_execute_stmt_id(uint32_t id) { execute_stmt_id = id; }
  uint64_t get_last_insert_id() { return last_insert_id; }
  void set_last_insert_id(uint64_t id) { last_insert_id = id; }
  uint8_t get_packet_number() { return packet_number; }
  void set_packet_number(uint8_t num) { packet_number = num; }
  void add_packet_number() { packet_number++; }
  uint8_t get_compress_packet_number() { return compress_packet_number; }
  void set_compress_packet_number(uint8_t num) { compress_packet_number = num; }
  void add_compress_packet_number() { compress_packet_number++; }
  void reset_dbscale_status_item(int item) { dbscale_status.reset_item(item); }
  int get_matched_or_change_message(string str, string &message);
  uint32_t get_client_capabilities() { return client_capabilities; }
  void set_client_capabilities(uint32_t capabilities) {
    client_capabilities = capabilities;
  }

 private:
  dbscale_status_item dbscale_status;
  map<DataServer *, unsigned int> hotspot_count;
  ACE_Thread_Mutex hotspot_count_mutex;
  uint32_t client_capabilities;

 public:
  dbscale_status_item *get_status() { return &dbscale_status; }
  void add_server_hotspot_count(DataServer *server, unsigned int value) {
    hotspot_count_mutex.acquire();
    hotspot_count[server] += value;
    hotspot_count_mutex.release();
  }
  void update_server_hotspot_count() {
    map<string, DataServer *> servers;
    Backend::instance()->get_data_servers(servers);
    map<string, DataServer *>::iterator it = servers.begin();
    for (; it != servers.end(); it++) {
      hotspot_count_mutex.acquire();
      it->second->update_hotspot_count(hotspot_count[it->second]);
      hotspot_count[it->second] = 0;
      hotspot_count_mutex.release();
    }
  }

 private:
  void remove_sp_worker_by_name(string &name);
  Connection *get_kept_connection_real(DataSpace *dataspace);
  bool remove_kept_connection_real(DataSpace *dataspace,
                                   bool clean_dead = false);
  void remove_fail_conn_from_session(DataSpace *space, Connection *conn);

 public:
  bool silence_execute_group_ok_stmt(uint32_t id);
  bool silence_roll_back_group_sql(uint32_t id);
  bool silence_reload_info_schema_mirror_tb(string &sql);

  void save_postponed_ok_packet(Packet *p, bool need_set_packet_number);
  Packet *get_postponed_ok_packet() { return &postponed_ok_packet; }
  bool get_postponed_ok_packet_need_set_packet_number() const {
    return postponed_ok_packet_need_set_packet_number;
  }

 public:
  enum AutoTrxState {
    START_POSTPONE_PACKET,
    BEGIN_OK_PACKET,
    DML_OK_PACKET,
    COMMIT_OK_PACKET,
    END_POSTPONE_PACKET
  };
  bool should_save_postponed_packet() const {
    return postponed_ok_packet_state != END_POSTPONE_PACKET;
  }
  void start_save_postponed_ok_packet() {
    postponed_ok_packet_state = START_POSTPONE_PACKET;
  }
  void end_save_postponed_ok_packet() {
    postponed_ok_packet_state = END_POSTPONE_PACKET;
  }
  bool in_auto_trx() const {
    return postponed_ok_packet_state != END_POSTPONE_PACKET;
  }

 private:
  AutoTrxState postponed_ok_packet_state;
  Packet postponed_ok_packet;
  bool postponed_ok_packet_need_set_packet_number;
};

}  // namespace mysql
}  // namespace dbscale

#endif /* DBSCALE_MYSQL_SESSION_H */
