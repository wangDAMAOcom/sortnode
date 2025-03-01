#ifndef DBSCALE_SESSION_H
#define DBSCALE_SESSION_H

/*
 * Copyright (C) 2012 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/SOCK_Stream.h>
#include <ace/SSL/SSL_Context.h>
#include <ace/SSL/SSL_SOCK_Stream.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <execute_profile.h>
#include <memory_allocater.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <stl_st_node_allcator.h>
#include <stream.h>
#include <table_info.h>

#include <boost/unordered_map.hpp>
#include <queue>

#include "ace/Condition_T.h"
#include "cluster_status.h"
#include "infrequent_resource.h"
#include "sha1.h"
#include "statement.h"
#include "table_pool.h"

using namespace dbscale;
using namespace dbscale::sql;

namespace dbscale {

class Connection;
class Handler;
class SPWorker;
class Schema;
class Table;
class MigrateTool;
class TransferRuleManager;
struct CrossNodeJoinTable;
class SocketEventBase;
class InternalEventBase;
namespace plan {
class ExecutePlan;
}

enum RunningXAType {
  RUNNING_XA_COMMIT,
  RUNNING_XA_ROLLBACK,
  RUNNING_XA_PREPARE
};

namespace sql {

#define SQL_LEN 1024
#define IDLE_SQL "Idle"
#define ENTIRE_QUERY_SQL_MAX_LEN 2097152  // 2MB

class Session;
enum AffetcedServerType {
  AFFETCED_NON_SERVER,
  AFFETCED_ONE_SERVER,
  AFFETCED_MUL_SERVER
};

/*
  according to mysql documentation, the xa transaction state can be followings
*/
enum SessionXaTrxState {
  SESSION_XA_TRX_XA_NONE,
  SESSION_XA_TRX_XA_ACTIVE,
  SESSION_XA_TRX_XA_IDLE,
  SESSION_XA_TRX_XA_PREPARED,
};

struct lock_table_full_item {
  lock_type type;
  lock_operation operation;
  const char *table_name;
  const char *schema_name;
  const char *alias;
};
struct lock_table_node {
  bool is_appear;
  struct lock_table_full_item item;
  struct lock_table_node *next;
};
struct dbscale_test_info {
  string test_case_name;
  string test_case_operation;
  string test_case_param;
};
struct show_lock_table_item {
  lock_type type;
  string name;
};

class PrepareItem {
 public:
  PrepareItem(string _query) : query(_query) {}
  PrepareItem() {}
  string &get_query() { return query; }
  void set_query(string &query) { this->query = query; }

 protected:
  string query;
};
class ExecuteItem {};

#ifndef DBSCALE_TEST_DISABLE
class MonitorPoint {
 public:
  MonitorPoint(const char *name, Session *session)
      : name(name), session(session) {}

  void start_timing();
  void end_timing();
  void end_timing_and_report();
  unsigned long get_cost_time() { return cost.msec(); }

 private:
  ACE_Time_Value start_date;
  ACE_Time_Value end_date;
  ACE_Time_Value cost;
  string name;
  Session *session;
};
#endif

class ExplainElement {
 public:
  ExplainElement(int _depth, string _ori_node_name, DataSpace *_ds, string _sql,
                 int _sub_plan_id, bool _is_a_start_node) {
    depth = _depth;
    ori_node_name = _ori_node_name;
    dataspace = _ds;
    sql = _sql;
    sub_plan_id = _sub_plan_id;
    a_start_node = _is_a_start_node;
  }
  int get_depth() { return depth; }
  string get_ori_node_name() { return ori_node_name; }
  DataSpace *get_dataspace() { return dataspace; }
  string &get_sql() { return sql; }
  int get_sub_plan_id() { return sub_plan_id; }
  bool is_a_start_node() { return a_start_node; }

 private:
  int depth;
  string ori_node_name;
  DataSpace *dataspace;
  string sql;
  int sub_plan_id;
  bool a_start_node;
};

enum SessionState {
  SESSION_STATE_INIT,             // session before authtication
  SESSION_STATE_WAITING_CLIENT,   // session after authtication and this session
                                  // is idle and swap out from handler
  SESSION_STATE_WORKING,          // session is working in a handler
  SESSION_STATE_WAITING_SERVER,   // session is waiting the result from backend
                                  // server
  SESSION_STATE_HANDLING_RESULT,  // session is after waiting result from
                                  // backend server, and handing the received
                                  // result
  SESSION_STATE_CLOSE             // session is closed
};

class PacketPool {
 public:
  PacketPool(size_t id) {
    packet_pool_max_packet_count_local = session_packet_pool_max_packet_count;
    packet_pool_cur_packet_count = 0;
    pool_id = id;
    list_in = &packet_list_one;
    list_out = &packet_list_two;
  }
  ~PacketPool() { release_all_packet_in_packet_pool(); }

  void get_free_packet_from_pool(
      int count, list<Packet *, StaticAllocator<Packet *> > *ret_list);
  void put_free_packet_back_to_pool(
      int &count, list<Packet *, StaticAllocator<Packet *> > &pkt_list);
  void add_packet_to_pool_no_lock(Packet *packet);
  void release_all_packet_in_packet_pool();
  list<Packet *, StaticAllocator<Packet *> > *get_packet_list() {
    return list_in;
  }  // NOTE: no mutex to keep thread safe

 private:
  size_t pool_id;
  int packet_pool_max_packet_count_local;
  int packet_pool_cur_packet_count;
  ACE_Thread_Mutex packet_pool_lock;
  list<Packet *, StaticAllocator<Packet *> > packet_list_one;
  list<Packet *, StaticAllocator<Packet *> > packet_list_two;
  list<Packet *, StaticAllocator<Packet *> > *list_in;
  list<Packet *, StaticAllocator<Packet *> > *list_out;
};

class Session {
 public:
  Session();
  void reset_flex_alloc() { alloc_for_flex.reset_mem_alloc(); }
  virtual ~Session();
  virtual const char *get_username() const = 0;
  virtual void set_username(const char *username) = 0;
  virtual const char *get_password() const = 0;
  virtual void set_password(const char *password) = 0;
  virtual const char *get_schema() const = 0;
  virtual void get_schema(string &s) const = 0;
  virtual void set_schema(const char *schema) = 0;
  virtual lock_table_node *get_lock_head() const = 0;
  virtual Connection *get_kept_connection(DataSpace *dataspace) = 0;
  virtual bool check_conn_in_kept_connections(DataSpace *dataspace,
                                              Connection *conn = NULL) = 0;
  virtual bool check_same_kept_connection(DataSpace *dataspace1,
                                          DataSpace *dataspace2) = 0;
  virtual map<DataSpace *, Connection *> get_kept_connections() = 0;
  virtual CrossNodeJoinTable *reuse_session_kept_join_tmp_table(
      CrossNodeJoinTablePool *pool) = 0;
  virtual void get_using_conn_ids(
      map<DataServer *, list<uint32_t> > &thread_ids) = 0;
  virtual bool is_dataspace_keeping_connection(DataSpace *dataspace) = 0;
  virtual void set_kept_connection(DataSpace *dataspace, Connection *conn,
                                   bool need_session_mutex = true) = 0;
  virtual bool remove_kept_connection(DataSpace *dataspace,
                                      bool clean_dead = false) = 0;
  virtual void set_kept_cross_join_tmp_table(CrossNodeJoinTable *table) = 0;
  virtual void add_back_kept_cross_join_tmp_table() = 0;
  virtual void remove_all_connection_to_free() = 0;
  virtual void remove_all_connection_to_dead() = 0;
  virtual void record_xa_modified_conn(Connection *conn) = 0;
  virtual bool is_xa_modified_conn(Connection *conn) = 0;
  virtual void clean_xa_modified_conn_map() = 0;
  virtual void rollback() = 0;
  virtual void cluster_xa_rollback() = 0;
  virtual void unlock() = 0;
  virtual bool session_is_in_prepare() = 0;
  virtual uint32_t get_prepare_num() = 0;
  virtual void get_conn_list_str(stringstream &ostr) = 0;
  virtual void get_extra_info(string &str) = 0;

  virtual void save_sp_error_response_info(Packet *packet) = 0;
  virtual void save_sp_error_response_info(unsigned int error_code,
                                           const char *error_sqlstate,
                                           const char *error_message) = 0;
  virtual void reset_sp_error_response_info() = 0;
  virtual unsigned int get_sp_error_code() = 0;
  virtual string get_sp_error_state() = 0;
  virtual string get_sp_error_message() = 0;
  virtual void flush_acl() = 0;
  virtual bool should_save_postponed_packet() const = 0;
  virtual void start_save_postponed_ok_packet() = 0;
  virtual void end_save_postponed_ok_packet() = 0;
  virtual bool in_auto_trx() const = 0;
  virtual map<string, string> get_character_set_var_map() = 0;

  Statement *get_stmt() { return stmt; }
  bool need_all_dbscale_column() {
    return user_name == need_all_dbscale_column_user;
  }

  void set_stmt(Statement *s) {
    if (stmt) {
      stmt->free_resource();
      delete stmt;
      stmt = NULL;
    }
    stmt = s;
  }

  void add_select() { ++con_select; }

  int get_select() { return con_select; }

  void add_insert() { ++con_insert; }

  int get_insert() { return con_insert; }

  void add_update() { ++con_update; }

  int get_update() { return con_update; }

  void add_delete() { ++con_delete; }

  int get_delete() { return con_delete; }

  void add_insert_select() { ++con_insert_select; }

  int get_insert_select() { return con_insert_select; }

  void set_is_query_command(bool b) { is_query_command = b; }
  bool get_is_query_command() { return is_query_command; }
  void set_load_data_local_command(bool b) { is_load_data_local_command = b; }
  bool get_load_data_local_command() { return is_load_data_local_command; }
  virtual void clean_all_group_sql_of_prepare_stmt() = 0;
  virtual void clean_target_prepare_stmt(uint32_t id) = 0;
  void set_in_transaction(bool b) {
    in_transaction = b;
    if (in_transaction) {
      start_transaction_time = ACE_OS::gettimeofday();
    }
    if (enable_record_transaction_sqls && !b) {
      clear_transaction_sqls();
    }
    if (!in_transaction) clear_modify_server();
    if (b == false) {
      in_transaction_consistent = false;
      trx_specify_read_only = false;
      reset_session_next_trx_level();
    } else {
      set_session_next_trx_level();
    }
#ifdef DEBUG
    LOG_DEBUG("Session [%@] set in_transaction [%d].\n", this, b ? 1 : 0);
#endif
  }
  void record_auto_increment_delete_values(string full_table_name,
                                           vector<const char *> *vec) {
    vector<const char *>::iterator it = vec->begin();
    if (mul_auto_increment_value_map[full_table_name].size() >
        instance_option_value["record_auto_increment_delete_value"].uint_val)
      return;
    for (; it != vec->end(); ++it) {
      mul_auto_increment_value_map[full_table_name].insert(atol(*it));
    }
  }
  bool mul_can_set_auto_increment_value(string full_table_name, long value) {
    bool ret = false;
    if (mul_auto_increment_value_map.count(full_table_name)) {
      ret = mul_auto_increment_value_map[full_table_name].count(value);
      mul_auto_increment_value_map[full_table_name].erase(value);
    }
    return ret;
  }
  void clear_auto_increment_delete_values() {
    if (!mul_auto_increment_value_map.empty())
      mul_auto_increment_value_map.clear();
  }
  void set_event_add_failed(bool b) { event_add_failed = b; }
  bool is_event_add_failed() { return event_add_failed; }

  bool is_in_transaction() { return in_transaction; }
  bool is_in_cluster_xa_transaction() {
    return !(cluster_xa_transaction_state == SESSION_XA_TRX_XA_NONE);
  }
  string cluster_xa_transaction_to_string(SessionXaTrxState state) {
    switch (state) {
      case SESSION_XA_TRX_XA_NONE:
        return "XA_NONE";
      case SESSION_XA_TRX_XA_ACTIVE:
        return "XA_ACTIVE";
      case SESSION_XA_TRX_XA_IDLE:
        return "XA_IDLE";
      case SESSION_XA_TRX_XA_PREPARED:
        return "XA_PREPARED";
      default:
        return "XA_UNKNOWN";
    }
  }
  void set_cluster_xa_transaction_state(SessionXaTrxState state) {
    LOG_DEBUG(
        "change xa state from %s to %s.\n",
        cluster_xa_transaction_to_string(cluster_xa_transaction_state).c_str(),
        cluster_xa_transaction_to_string(state).c_str());
    cluster_xa_transaction_state = state;
  }
  void reset_cluster_xa_transaction(bool reset_cluster_xa_conn_null = true);
  SessionXaTrxState get_cluster_xa_transaction_state() {
    return cluster_xa_transaction_state;
  }
  virtual Connection *get_cluster_xa_transaction_conn() = 0;
  void set_cluster_xa_transaction_conn(Connection *conn) {
    cluster_xa_conn = conn;
  }
  bool check_cluster_conn_is_xa_conn(Connection *conn) {
    if (!cluster_xa_conn) return false;
    return conn == cluster_xa_conn;
  }
  string get_session_cluster_xid() { return cluster_xid; }
  void set_session_cluster_xid(string xid_str) { cluster_xid = xid_str; }
  bool is_in_transaction_consistent() { return in_transaction_consistent; }
  void set_trx_specify_read_only(bool b) { trx_specify_read_only = b; }
  bool is_trx_specify_read_only() { return trx_specify_read_only; }
  void set_in_transaction_consistent(bool b) { in_transaction_consistent = b; }
  bool check_for_transaction();

  void reset_server_shutdown() { server_shutdown = false; }

  void set_server_shutdown(bool b) {
    if (!server_shutdown) server_shutdown = b;
    LOG_DEBUG("Session [%@] set server_shutdown [%d].\n", this, b ? 1 : 0);
  }

  bool is_server_shutdown() { return server_shutdown; }

  void set_in_lock(bool b) {
    in_lock = b;
    LOG_DEBUG("Session [%@] set in_lock [%d].\n", this, b ? 1 : 0);
  }

  bool is_in_lock() { return in_lock; }

  void set_has_global_flush_read_lock(bool b) {
    has_global_flush_read_lock = b;
    LOG_DEBUG("Session [%@] set has_global_flush_read_lock [%d].\n", this,
              b ? 1 : 0);
  }

  bool get_has_global_flush_read_lock() { return has_global_flush_read_lock; }

  void reset_table_without_given_schema() {
    table_without_given_schema.clear();
  }

  void add_table_without_given_schema(DataSpace *space);

  bool check_table_without_given_schema(DataSpace *space) {
    if (table_without_given_schema.count(space)) return true;
    return false;
  }

  virtual CharsetType get_client_charset_type() = 0;
  virtual map<string, string> *get_session_var_map() = 0;
  virtual void set_session_var_map(string var, string value) = 0;
  virtual void set_session_var_map(map<string, string> var_map) = 0;
  virtual void prepare_set_session_next_trx_level(const string &value) = 0;
  virtual void overwrite_session_next_trx_level() = 0;
  virtual void set_session_next_trx_level() = 0;
  virtual void reset_session_next_trx_level() = 0;
  void set_session_var_map_md5(string value) { session_var_map_md5 = value; }
  string get_session_var_map_md5() { return session_var_map_md5; }
  void set_schema_name_md5(string value) { schema_name_md5 = value; }
  string get_schema_name_md5() { return schema_name_md5; }
  void set_affected_rows(int64_t affected_rows);
  int64_t get_affected_rows() { return this->affected_rows; }

  virtual void add_savepoint(const char *sql) = 0;
  virtual void remove_all_savepoint() = 0;
  virtual void remove_savepoint(const char *point) = 0;
  virtual lock_table_full_item *add_lock_table(lock_table_item *table) = 0;
  virtual void reset_lock_table_state() = 0;
  virtual void remove_all_lock_tables() = 0;
  virtual bool contains_lock_table(const char *table, const char *schema,
                                   const char *alias) = 0;
  virtual list<char *> get_all_savepoint() = 0;
  virtual bool get_compress() = 0;
  virtual void set_compress(bool compress) = 0;
  virtual bool get_local_infile() = 0;
  virtual void set_local_infile(bool local_infile) = 0;
  virtual void set_var_by_ref(string user_var, string ref_var) = 0;
  virtual void add_user_var_value(string user_var, string value, bool is_str,
                                  bool skip_add_bslash = false) = 0;
  virtual void remove_user_var_by_name(string user_var) = 0;
  virtual map<string, string> *get_user_var_map() = 0;
  virtual string get_user_var_origin_value(string var_name) = 0;
  virtual string *get_user_var_value(string var_name) = 0;
  virtual void set_var_sql(string sql) = 0;
  virtual string get_var_sql() = 0;
  void set_var_flag(bool flag) { has_var = flag; }
  bool get_var_flag() { return has_var; }

  void set_read_only(bool b) {
    if (!b) {
      enable_real_time_queries();
      is_readonly = b;
    } else {
      is_readonly = !is_real_time_queries();
    }
  }
  bool is_read_only() {
    if (is_keepmaster) return false;
    if (slave_dbscale_mode) return false;
    return is_readonly;
  }

  void set_global_time_queries(bool is_read_sql);
  bool global_table_need_cross_node_join();

  virtual void set_real_fetched_rows(uint64_t num) = 0;
  virtual uint64_t get_real_fetched_rows() = 0;
  virtual void set_found_rows(uint64_t num) = 0;
  virtual void set_error_packet(Packet *packet) = 0;
  virtual uint64_t get_found_rows() = 0;
  virtual void set_xa_id(unsigned long xid) = 0;
  virtual unsigned long get_xa_id() = 0;
  virtual unsigned int get_next_seq_id() = 0;
  virtual void reset_seq_id() = 0;
  virtual bool get_is_xa_prepared() = 0;
  virtual void set_xa_prepared() = 0;
  virtual void reset_is_xa_prepared() = 0;
  virtual string get_client_ip() = 0;

  void record_idle_time() {
    if (wait_timeout == 0) return;
    idle_time = ACE_OS::gettimeofday();
  }
  bool need_swap_handler() { return is_need_swap_handler; }
  void set_need_swap_handler(bool b) { is_need_swap_handler = b; }
  unsigned int get_idle_time() {
    return (ACE_OS::gettimeofday() - idle_time).sec();
  }
  unsigned int get_cur_login_time() {
    return (ACE_OS::gettimeofday() - login_time).sec();
  }
  bool query_command_flow_rate_control_ok();
  int get_max_query_count_per_flow_control_gap() {
    return max_query_count_per_flow_control_gap_local;
  }
  void record_query_time() {
    if (enable_record_query_time) query_time = ACE_OS::gettimeofday();
  }
  void record_login_time() { login_time = ACE_OS::gettimeofday(); }
  unsigned long get_query_time() {
    if (enable_record_query_time && query_sql[0]) {
      return (ACE_OS::gettimeofday() - query_time).msec();
    }
    return 0;
  }
  ACE_Time_Value get_login_time() { return login_time; }
  unsigned long get_in_transaction_time() {
    if (enable_record_query_time && start_transaction_time != 0 &&
        is_in_transaction()) {
      return (ACE_OS::gettimeofday() - start_transaction_time).msec();
    }
    return 0;
  }

  size_t get_last_truncate_string_pos(const char *str, size_t str_len,
                                      size_t len) const {
    /* now we just support UTF8, so set ctype CHARSET_TYPE_UTF8
     * utf8 use 1-4 bite to store char,
     * 1 bite char: 0xxxxxxx
     * 2 bite char: 110xxxxx 10xxxxxx
     * 3 bite char: 1110xxxx 10xxxxxx 10xxxxxx
     * 4 bite char: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
     * gbk use 1-2 bite to store char
     * 1 bite char: 0xxxxxxx
     * 2 bite char: 1xxxxxxx xxxxxxxx
     * gb18030 use 1,2 or 4 bite to store char
     * 1 bite char: 0x00 - 0x7F
     * 2 bite char: 0x81 - 0xFE, 0x40 - 0x7E or 0x80 - 0xFE
     * 4 bite char: 0x81 - 0xFE, 0x30 - 0x39, 0x81 - 0xFE, 0x30 - 0x39
     */
    CharsetType ctype = CHARSET_TYPE_UTF8;
    if (str_len <= len) return str_len;
    switch (ctype) {
      case CHARSET_TYPE_OTHER:
      case CHARSET_TYPE_UTF8:
      case CHARSET_TYPE_UTF8MB4:
        if ((static_cast<unsigned char>(str[len - 2]) & 0xC0) == 0xC0)
          return len - 2;
        else if ((static_cast<unsigned char>(str[len - 3]) & 0xE0) == 0xE0)
          return len - 3;
        else if ((static_cast<unsigned char>(str[len - 4]) & 0xF0) == 0xF0)
          return len - 4;
        else
          return len;
      case CHARSET_TYPE_GBK:
        if ((static_cast<unsigned char>(str[len - 2]) & 0x80) != 0)
          return len - 2;
        else
          return len;
      case CHARSET_TYPE_GB18030: {
        size_t l = 0;
        while (l <= len - 2) {
          // 2 or 4 bite char
          if ((static_cast<unsigned char>(str[l]) & 0x80) != 0) {
            // 2 bite char
            if ((l + 2 <= len - 2) &&
                (static_cast<unsigned char>(str[l + 1]) & 0xC0) != 0)
              l += 2;
            else if (l + 4 <= len - 2)
              l += 4;
            else
              break;
          } else
            ++l;
        }
        if (l == len - 1) return len;
        return l;
      }
      default:
        return len;
    }
    return len;
  }
  void clear_query_sql() {
    query_sql[0] = '\0';
    query_sql_len = 0;
    entire_query_sql.clear();
  }
  void set_query_sql(const char *sql, size_t len) {
    query_sql_len = len;
    copy_string(query_sql, sql, SQL_LEN);
    entire_query_sql.clear();
    if (len >= SQL_LEN) {
      if (len > ENTIRE_QUERY_SQL_MAX_LEN) len = ENTIRE_QUERY_SQL_MAX_LEN;
      entire_query_sql.assign(sql, 0, len);
    }
  }
  const char *get_query_sql() const {
    if (!query_sql[0])
      return IDLE_SQL;
    else
      return query_sql;
  }
  void get_query_sql(string &s) const {
    size_t len = s.size();
    s.assign(get_query_sql(), SQL_LEN + 1);
    if (this->query_sql_len > SQL_LEN) {
      size_t l = get_last_truncate_string_pos(get_query_sql(),
                                              this->query_sql_len, SQL_LEN);
      if (l != SQL_LEN) s[len + l] = '\0';
    }
  }
  const char *get_query_sql_entire_len()
      const  //"entire len" is <=ENTIRE_QUERY_SQL_MAX_LEN in fact
  {
    if (!query_sql[0]) {
      return IDLE_SQL;
    } else if (!entire_query_sql.empty()) {
      return entire_query_sql.c_str();
    } else {
      return query_sql;
    }
  }

  virtual const char *get_user_addr() = 0;
  virtual void get_user_addr(string &s) = 0;
  virtual bool is_contain_schema(string &schema_name,
                                 bool with_mutex = true) = 0;
  virtual set<string> &get_auth_schema_set() = 0;
  virtual void set_auth_schema_set(set<string> &auth_schema_set) = 0;
  virtual void delete_auth_schema_set(string schema) = 0;
  virtual void add_auth_schema_to_set(string schema_name) = 0;
  virtual void record_redo_log_to_buffer(DataSpace *space, string &sql) = 0;
  virtual map<DataSpace *, list<string> > *get_redo_logs_map() = 0;
  virtual list<string> *get_redo_logs_map(DataSpace *space) = 0;
  virtual void clean_up_redo_logs_map() = 0;
  virtual DataSpace *get_space_from_kept_space_map(DataSpace *space) = 0;

  virtual void set_user_not_allow_operation_time(map<int, int> times) = 0;
  virtual bool is_user_not_allow_operation_now() = 0;
  virtual void get_user_not_allow_operation_now_str(string &ret) = 0;

  virtual void set_schema_acl_info(
      map<string, SchemaACLType, strcasecomp> *_schema_acl_map) = 0;
  virtual map<string, SchemaACLType, strcasecomp> *get_schema_acl_map() = 0;
  virtual SchemaACLType get_schema_acl_type(const char *schema_name) = 0;

  virtual void set_table_acl_info(
      map<string, TableACLType, strcasecomp> *_table_acl_map) = 0;
  virtual map<string, TableACLType, strcasecomp> *get_table_acl_map() = 0;
  virtual TableACLType get_table_acl_type(string &full_table_name) = 0;

  void set_affected_servers(AffetcedServerType type) {
    affect_server_type = type;
  }
  AffetcedServerType get_affected_server_type() { return affect_server_type; }
  void increase_one_affected_server() {
    if (affect_server_type == AFFETCED_NON_SERVER) {
      affect_server_type = AFFETCED_ONE_SERVER;
    } else {
      affect_server_type = AFFETCED_MUL_SERVER;
    }
  }
  virtual void acquire_using_conn_mutex() = 0;
  virtual void release_using_conn_mutex() = 0;
  virtual void killall_using_conns() = 0;
  virtual bool check_in_in_xa_commit() = 0;
  void insert_using_conn(Connection *conn) {
    /*
    if (get_is_simple_direct_stmt()) {
      if ( get_is_killed()) {
        throw ExecuteNodeError("Thread is killing.\n");
      }
      using_conns.insert(conn);

    } else {
    */
    using_conns_mutex.acquire();
    if (get_is_killed()) {
      using_conns_mutex.release();
      throw ExecuteNodeError("Thread is killing.\n");
    }
    using_conns.insert(conn);
    cur_stmt_conns.insert(conn);
    using_conns_mutex.release();
    /*
    }
    */
  }

  set<Connection *> *get_using_conn() { return &using_conns; }

  unsigned long long get_using_conn_num() {
    using_conns_mutex.acquire();
    size_t ret = using_conns.size();
    using_conns_mutex.release();
    return (unsigned long long)ret;
  }

  bool contains_using_connection(DataServer *server, int connection_id);

  void remove_using_conn(Connection *conn) {
    /* TODO consider how to deal it
    if (get_is_simple_direct_stmt()) {
      if (using_conns.count(conn))
        using_conns.erase(conn);
    } else {
    */
    using_conns_mutex.acquire();
    if (using_conns.count(conn)) using_conns.erase(conn);
    if (cur_stmt_conns.count(conn)) cur_stmt_conns.erase(conn);
    using_conns_mutex.release();
    /*
    }
    */
  }

  void reset_cur_stmt_conns() { cur_stmt_conns.clear(); }

  set<Connection *> get_cur_stmt_conns() { return cur_stmt_conns; }

  /*Currently, re_init_session is only be used for migration clean tool*/
  virtual void re_init_session() = 0;
  virtual void set_option_values() = 0;
  virtual unsigned int get_option_value_uint(string option_name) = 0;
  virtual unsigned long get_option_value_ul(string option_name) = 0;
  bool get_is_keepmaster() { return is_keepmaster; }
  void set_is_keepmaster(bool b) { is_keepmaster = b; }

  virtual void set_dbscale_test_info(const char *case_name,
                                     const char *operation,
                                     const char *param) = 0;
  virtual dbscale_test_info *get_dbscale_test_info() = 0;
  virtual void add_sp_worker(SPWorker *spw) = 0;
  virtual SPWorker *get_sp_worker(string &sp_name) = 0;
  virtual void remove_sp_worker(string &sp_name) = 0;
  virtual SPWorker *get_cur_sp_worker() = 0;
  virtual void set_cur_sp_worker(SPWorker *spw) = 0;
  virtual void pop_cur_sp_worker_one_recurse() = 0;

  void set_ddl_comment_sql(string ddl_sql) { ddl_comment_sql = ddl_sql; }
  string *get_ddl_comment_sql() { return &ddl_comment_sql; }

  bool is_call_store_procedure() { return is_call_sp; }
  void set_call_store_procedure(bool b) { is_call_sp = b; }

  void increase_call_sp_nest() {
    ++call_sp_nest;
    if (call_sp_nest > max_allowed_nested_sp_depth) {
      string error_msg =
          "Reach max depth of recursive routine, please check your configure.";
      LOG_ERROR("%s\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }
    LOG_DEBUG("increase one sp_nest, current total call_sp_nest=%d\n",
              call_sp_nest);
  }
  void decrease_call_sp_nest() {
    if (call_sp_nest) {
      call_sp_nest--;
      LOG_DEBUG("decrease one sp_nest, current total call_sp_nest=%d\n",
                call_sp_nest);
    }
  }
  void reset_call_sp_nest() {
#ifdef DEBUG
    LOG_DEBUG("reset call sp_nest = 0\n");
#endif
    call_sp_nest = 0;
  }
  int get_call_sp_nest() { return call_sp_nest; }

  void reset_table_info_to_delete() { table_info_to_delete_pair_vec.clear(); }
  void add_table_info_to_delete(const char *schema_name,
                                const char *table_name);
  void add_db_table_info_to_delete(const char *schema_name);
  void apply_table_info_to_delete();

  bool session_modify_mul_server() {
    return get_need_wait_for_consistence_point();
  }

  virtual bool get_need_wait_for_consistence_point() = 0;
  virtual bool get_xa_need_wait_for_consistence_point() = 0;
  virtual void record_modify_server(const char *server_name,
                                    unsigned int virtual_machine_id) = 0;
  virtual void clear_modify_server() = 0;

  void set_drop_current_schema(bool b) { drop_current_schema = b; }
  bool is_drop_current_schema() { return drop_current_schema; }
  bool get_can_swap() { return can_swap; }

  void set_can_swap(bool b) { can_swap = b; }

  virtual bool has_client_buffer_packet() = 0;
  virtual void add_transaction_sql(const char *sql) = 0;
  virtual void clear_transaction_sqls() = 0;
  virtual list<string> *get_transaction_sqls() = 0;
  virtual void add_warning_dataspace(DataSpace *space, Connection *conn) = 0;
  virtual set<DataSpace *> get_warning_dataspaces() = 0;
  virtual void reset_warning_dataspaces() = 0;
  virtual bool is_contain_warning_dataspace() = 0;
  virtual bool dataspace_has_warning(DataSpace *space) = 0;
  virtual void remove_non_warning_conn_to_free() = 0;
  virtual void remove_warning_conn_to_free() = 0;
  virtual void clear_dataspace_warning(DataSpace *space,
                                       bool clean_conn = true) = 0;
  virtual void reset_warning_map() = 0;
  virtual void set_no_status(bool b) = 0;
  virtual bool add_remove_sp_name(string &sp_name) = 0;
  virtual void clear_all_sp_in_remove_sp_vector() = 0;
  virtual string get_session_var_value(string var) = 0;
  virtual void add_in_using_table(const char *schema_name,
                                  const char *table_name) = 0;
  virtual bool is_table_in_using(string schema_name, string table_name) = 0;
  virtual void clean_in_using_table() = 0;
  virtual void clean_all_using_table() = 0;
  virtual void set_tx_record_redo_log_kept_connections(Connection *conn) = 0;
  virtual void spread_redo_log_to_buffer(const char *sql,
                                         bool is_relate_source = false) = 0;
  virtual bool set_and_get_backend_get_consistence(bool b) = 0;
  virtual void start_commit_consistence_transaction() = 0;
  virtual void end_commit_consistence_transaction() = 0;
  // function for auto increment lock.
  virtual int64_t get_auto_inc_value(string &full_table_name) = 0;
  virtual bool is_contain_auto_inc_value(string &full_table_name) = 0;
  virtual void set_auto_inc_value(string &full_table_name, int64_t value,
                                  int64_t num, int times) = 0;
  virtual int get_insert_inc_times(string &full_table_name) = 0;
  virtual bool set_auto_increment_value(string &full_table_name,
                                        int64_t parsed_val) = 0;
  virtual void reset_auto_increment_info(string &full_table_name) = 0;
  // function for auto increment lock end.

  // move from handler
  virtual void set_last_insert_id(uint64_t id) = 0;
  virtual uint64_t get_last_insert_id() = 0;

  virtual dbscale_status_item *get_status() = 0;
  virtual void add_server_hotspot_count(DataServer *server,
                                        unsigned int value) = 0;
  virtual void update_server_hotspot_count() = 0;
  virtual string get_next_nest_sp_name(string &sp_name) = 0;
  virtual void reset_all_nest_sp_counter() = 0;
  virtual int get_extra_snippet_of_sp_var() = 0;
  virtual void set_prepare_read_connection(uint32_t gsid, Connection *conn) = 0;
  virtual void remove_prepare_read_connection(uint32_t gsid) = 0;
  virtual void remove_prepare_read_connection(Connection *conn) = 0;
  virtual Connection *get_prepare_read_connection(uint32_t gsid) = 0;
  virtual Connection *check_connection_in_prepare_read_connection(
      Connection *conn) = 0;
  virtual void increase_finished_federated_num() = 0;
  virtual void set_federated_num(int num) = 0;
  virtual bool federated_session_finished() = 0;
  virtual void reset_federated_finished() = 0;
  virtual void init_user_priority() = 0;
  bool is_executing_prepare() { return executing_prepare; }
  void set_executing_prepare(bool b) { executing_prepare = b; }
  virtual uint32_t get_gsid() = 0;

  bool is_keeping_connection() {
    return is_in_transaction() || is_in_lock() || has_global_flush_read_lock;
  }

  string &get_server_scramble() { return server_scramble; }
  string &get_user_host() { return user_host; }

  void set_user_priority(UserPriorityValue priority) {
    user_priority = priority;
  }

  UserPriorityValue get_user_priority() { return user_priority; }

  void init_session();
  void switch_out_to_wait_client_event();
  void switch_out_to_wait_server_event();

  evutil_socket_t get_client_socket() const { return client_socket; }
  struct event *get_read_socket_event() const {
    return read_socket_event;
  }

  SocketEventBase *get_socket_base() const { return socket_base; }

  struct event *get_internal_server_event() const {
    return internal_server_event;
  }

  struct event *get_internal_event() const {
    return internal_event;
  }
  InternalEventBase *get_internal_base() const { return internal_base; }

  void init_internal_base();

  bool add_traditional_num();

  void sub_traditional_num();

  void set_session_state(SessionState s) {
#ifdef DEBUG
    LOG_DEBUG("Session %@ set state to %d\n", this, (int)s);
#endif
    session_state = s;
  }
  SessionState get_session_state() { return session_state; }

  Stream *get_stream() { return stream; }
  void set_stream(Stream *s);
  void force_close_stream();

  void set_client_scramble(const char *scramble) {
    memset(client_scramble, 0, SHA2_HASH_SIZE);
    uint8_t scramble_len = cur_auth_plugin == MYSQL_NATIVE_PASSWORD
                               ? SHA1_HASH_SIZE
                               : SHA2_HASH_SIZE;
    if (scramble) {
      memcpy(client_scramble, scramble, scramble_len);
    }
    client_scramble[SHA2_HASH_SIZE] = '\0';
  }

  const char *get_client_scramble() { return client_scramble; }
  string &get_server_sramble() { return server_scramble; }

  string get_spark_job_id();
  string get_cur_spark_job_id() { return spark_job_id; }
  void set_cur_spark_job_id(string job_id) { spark_job_id = job_id; }
  uint32_t get_thread_id() const { return thread_id; }
  void set_thread_id(uint32_t id) {
    thread_id = id;
    if (thread_id % 30 == 5) monitor_flag = true;
#ifdef DEBUG
    LOG_DEBUG("Session %@ init the thread id with %d.\n", this, id);
#endif
  }
  void set_keep_conn(bool b) { keep_conn = b; }
  bool get_keep_conn() const { return keep_conn; }

  bool need_update_option() {
    bool b;
    mutex_update_option.acquire();
    b = update_option;
    mutex_update_option.release();
    return b;
  }
  void set_update_option(bool b) {
    mutex_update_option.acquire();
    update_option = b;
    mutex_update_option.release();
  }

  void set_in_killing(bool b) { in_killing = b; }
  void set_is_killed(bool b) { is_killed = b; }

  bool get_in_killing() { return in_killing; }
  bool get_is_killed() { return is_killed; }

  void report_monitor_value(const char *str, ACE_Time_Value value) {
    string tmp(str);
    if (monitor_point_map.count(tmp)) {
      monitor_point_map[tmp] += value;
    } else
      monitor_point_map[tmp] = value;
  }

  void reset_monitor_point_map() { monitor_point_map.clear(); }

  void set_mul_connection_num(int num) { mul_connection_num = num; }

  int get_mul_connection_num() { return mul_connection_num; }

  void add_arrived_transaction_num() { ++arrived_transaction_num; }

  void acquire_trans_num_mutex() { arrived_trans_num_mutex.acquire(); }

  void release_trans_num_mutex() { arrived_trans_num_mutex.release(); }

  int get_arrived_transaction_num() { return arrived_transaction_num; }

  void reset_arrived_transaction_num() { arrived_transaction_num = 0; }

  map<string, ACE_Time_Value> *get_monitor_point_map() {
    return &monitor_point_map;
  }

  void report_monitor_point_to_backend();

  void set_handler(Handler *h);
  Handler *get_handler() { return handler; }

  void set_top_plan(ExecutePlan *plan);
  ExecutePlan *get_top_plan() {
    if (nest_top_plan.empty()) return top_plan;
    return nest_top_plan.back();
  }
  /* this remove would not free the resources, you should handle it
   * yourself.*/
  void remove_top_plan() {
    if (nest_top_plan.empty())
      top_plan = NULL;
    else
      nest_top_plan.pop_back();
  }

  void remove_top_stmt() {
    if (nest_top_stmt.empty()) {
      if (top_stmt) {
        top_stmt->free_resource();
        delete top_stmt;
      }
      top_stmt = NULL;
    } else
      nest_top_stmt.pop_back();
  }

  void set_top_stmt(Statement *stmt) {
    if (top_stmt) {
      nest_top_stmt.push_back(stmt);
      return;
    }
    top_stmt = stmt;
  }

  Statement *get_original_stmt() { return top_stmt; }

  Statement *get_top_stmt() {
    if (nest_top_stmt.empty()) return top_stmt;
    return nest_top_stmt.back();
  }

  void reset_top_stmt_and_plan();
  void reset_top_stmt();
  void reset_all_top_stmt_and_plan();

  /*Is it possible for this session can be swapped out from handler during the
   * sql execution.
   *
   *    If return false, this session will definitely not be swapped out during
   *    the execution.
   *
   *       If return true, this session may be swapped out. It depende the
   *       implementation of related execution node.
   *
   *          Currently, only direct node supports the sql execution swap out.
   *          */
  bool is_may_backend_exec_swap_able();

  void set_is_complex_stmt(bool b) {
    /*If this stmt is consider as complex for cur sql, it will not
     * be * changed.*/
    if (!is_complex_stmt) is_complex_stmt = b;
  }

  void reset_is_complex_stmt() { is_complex_stmt = false; }

  void set_is_simple_direct_stmt(bool b) {
    if (is_complex_stmt) return;
    is_simple_direct_stmt = b;
  }

  bool get_is_simple_direct_stmt() {
    if (is_complex_stmt) return false;
    return is_simple_direct_stmt;
  }

  void reset_is_simple_direct_stmt() { is_simple_direct_stmt = false; }

  void reset_skip_mutex_in_operation() { skip_mutex_in_operation = false; }

  bool is_skip_mutex_in_operation() {
    if (is_complex_stmt) return false;
    return skip_mutex_in_operation;
  }

  void set_skip_mutex_in_operation(bool b) {
    if (is_complex_stmt) return;
    skip_mutex_in_operation = b;
  }

  bool get_is_complex_stmt() { return is_complex_stmt; }

#ifndef DBSCALE_TEST_DISABLE
  MonitorPoint &get_stmt_exec_mp() { return mp_stmt_exec; }
#endif

  void add_server_base(evutil_socket_t fd, SocketEventBase *base,
                       struct event *e) {
#ifdef DEBUG
    LOG_DEBUG("Session %@ add server base %@ server event %@ with fd %d.\n",
              this, base, e, fd);
#endif
    server_base_map[fd] = base;
    server_event_map[fd] = e;
  }

  SocketEventBase *get_server_base(evutil_socket_t fd) {
    if (server_base_map.count(fd)) return server_base_map[fd];
    return NULL;
  }

  void reset_server_base_related() {
    server_base_map.clear();
    server_event_map.clear();
  }

  bool need_swap_for_server_waiting() { return !server_base_map.empty(); }

  MemoryAllocater *get_mem_alloc() { return &mem_alloc; }

  MemoryAllocater *get_mem_alloc_for_flex() { return &alloc_for_flex; }

  void reset_mem_alloc() { mem_alloc.reset_mem_alloc(); }

  PacketAllocater *get_packet_alloc() { return &packet_alloc; }

  void reset_packet_alloc() { packet_alloc.reset_mem_alloc(); }

  PacketAllocater *get_fetch_node_packet_alloc() {
    return &fetchnode_packet_alloc;
  }

  void reset_fetch_node_packet_alloc() {
    fetchnode_packet_alloc.reset_mem_alloc();
  }

  bool get_auto_commit_is_on() { return auto_commit_is_on; }

  void set_auto_commit_is_on(bool b) { auto_commit_is_on = b; }

  bool is_empty_remove_sp_vec() { return remove_sp_vector.empty(); }

  void set_transaction_error(bool b) {
    transaction_error = b;
    LOG_DEBUG("Session [%@] set transaction_error [%d].\n", this, b ? 1 : 0);
  }

  bool is_transaction_error() { return transaction_error; }

  bool is_real_time_queries() {
    bool ret = false;
    if (real_time_queries > 0) {
#ifdef DEBUG
      LOG_DEBUG("real_time_queries : %d\n", real_time_queries);
#endif
      real_time_queries--;
      ret = true;
    }
    return ret;
  }
  void enable_real_time_queries() {
//    unsigned int real_time_queries_conf
//    = option_value_uint[string("real_time_queries")];
#ifdef DEBUG
    LOG_DEBUG("set real_time_queries : %d\n", real_time_queries_conf);
#endif
    real_time_queries = real_time_queries_conf;
  }

  void clear_profile_tree() { execution_profile.clear_execute_profile(); }
  ExecuteProfileHandler *get_profile_handler() { return &execution_profile; }

  bool is_in_profile() { return execution_profile.get_profile(); }
  void set_in_profile(bool b) {
    execution_profile.set_profile(b);
    LOG_DEBUG("Session [%@] set profile [%d].\n", this, b ? 1 : 0);
  }

  bool get_has_check_first_sql() const { return has_check_first_sql; }

  void set_has_check_first_sql(bool b) { has_check_first_sql = b; }

  void clean_net_attribute();

  OptionValue &get_session_option(const char *option_name) {
    return session_option_value[option_name];
  }
  void init_session_option() {
    OptionParser *config = OptionParser::instance();
    config->init_session_option(session_option_value);
  }
  map<string, OptionValue> &get_session_option_map() {
    return session_option_value;
  }

  string get_alter_table_name() { return alter_table_name; }
  void set_alter_table_name(string s) { alter_table_name = s; }
  void reset_alter_table_name() { alter_table_name.clear(); }

  unsigned long long get_net_in() { return this->net_in; }

  unsigned long long get_net_out() { return this->net_out; }

  void set_net_in(unsigned long long net_in) {
    if (get_is_simple_direct_stmt())
      this->net_in += net_in;
    else {
      net_status_mutex.acquire();
      this->net_in += net_in;
      net_status_mutex.release();
    }
  }

  void set_net_out(unsigned long long net_out) {
    if (get_is_simple_direct_stmt())
      this->net_out += net_out;
    else {
      net_status_mutex.acquire();
      this->net_out += net_out;
      net_status_mutex.release();
    }
  }

  Session *get_work_session() { return work_session; }

  void set_work_session(Session *s) { work_session = s; }

  void reset_statement_level_session_variables(bool do_reset_mem_alloc = true) {
    LOG_DEBUG("Reset statement level session variables\n");
    if (!is_empty_remove_sp_vec()) clear_all_sp_in_remove_sp_vector();
    reset_is_complex_stmt();
    reset_is_simple_direct_stmt();
    reset_server_base_related();
    if (do_reset_mem_alloc) reset_mem_alloc();
    reset_packet_alloc();
    reset_fetch_node_packet_alloc();
    reset_server_shutdown();
    reset_flex_alloc();
    reset_table_without_given_schema();
    work_session = NULL;
    set_federated_num(1);
    reset_federated_finished();
    prepare_kill_session = false;
    reset_stmt_auto_commit_value();
    reset_has_send_client_error_packet();
    reset_cur_stmt_conns();
    reset_slow_query_time_local();
    reset_skip_mutex_in_operation();

    explain_sub_sql_list.clear();
    clean_explain_element_list();
    reset_table_info_to_delete();
    cur_stmt_execute_fail = false;
    cur_stmt_error_code = 0;
    cur_stmt_type = STMT_NON;
    clear_query_sql();
    reset_cross_tmp_table_collate();
    reset_cross_tmp_table_charset();
    has_check_acl = false;
    can_change_affected_rows = true;
    cancel_reset_stmt_level_variables_once = false;
    is_surround_sql_with_trx_succeed = false;
  }
  void enable_prepare_kill() { prepare_kill_session = true; }
  bool is_prepare_kill() { return prepare_kill_session; }
  void reset_stmt_auto_commit_value() { stmt_auto_commit_value.clear(); }

  void set_stmt_auto_commit_value(bool b) {
    if (b)
      stmt_auto_commit_value = "1";
    else
      stmt_auto_commit_value = "0";
    stmt_auto_commit_int_value = b;
  }

  string &get_stmt_auto_commit_value() { return stmt_auto_commit_value; }

  int get_stmt_auto_commit_int_value() { return stmt_auto_commit_int_value; }

  string get_user_name() { return user_name; }

  string &get_user_name_ref() { return user_name; }

  void set_user_name(string user) { user_name = user; }

  bool get_mul_node_timeout() { return mul_node_timeout; }
  void set_mul_node_timeout(bool b) { mul_node_timeout = b; }

  bool is_error_generated_by_dbscale() { return error_generated_by_dbscale; }
  void set_error_generated_by_dbscale(bool b) {
    error_generated_by_dbscale = b;
  }
  bool is_error_generated_by_force_stop_trx() {
    return error_generated_by_force_stop_trx;
  }
  void set_error_generated_by_force_stop_trx(bool b) {
    error_generated_by_force_stop_trx = b;
  }
  string get_start_config_lock_extra_info() {
    return start_config_lock_extra_info;
  }

  bool is_refuse_modify_table(string full_table_name) {
    return refuse_modify_table.count(full_table_name);
  }

  void set_refuse_modify_table(set<string> &refuse_modify_table_tmp) {
    refuse_modify_table = refuse_modify_table_tmp;
  }

  int get_refuse_modify_table_version() { return refuse_version; }

  void set_refuse_version(int tmp_refuse_version) {
    refuse_version = tmp_refuse_version;
  }

  void set_client_swap_timeout(bool b) { client_swap_timeout = b; }
  bool get_client_swap_timeout() { return client_swap_timeout; }

  bool is_executing_spark() { return executing_spark; }

  void start_executing_spark() { executing_spark = true; }

  void stop_executing_spark() { executing_spark = false; }

  bool get_has_more_result() { return has_more_result; }
  void set_has_more_result(bool b) { has_more_result = b; }

 public:
  void set_in_explain(bool b) { in_explain = b; }
  bool is_in_explain() { return in_explain; }
  void set_explain_node_sequence_id(int id) { explain_node_id = id; }
  int inc_and_get_explain_node_sequence_id() { return ++explain_node_id; }
  void reset_explain_sub_sql_list() { explain_sub_sql_list.clear(); }
  list<pair<string, string> > *get_explain_sub_sql_list() {
    return &explain_sub_sql_list;
  }
  void update_explain_info(list<ExecuteNode *> &node_list, int depth,
                           int sub_plan_id);
  list<ExplainElement *> *get_explain_element_list() {
    return &explain_element_list;
  }
  void clean_explain_element_list();
  void set_execute_plan_touch_partition_nums(int n) {
    if (execute_plan_touch_partiton_nums != 0 && n != -1 && n != 0) return;
    execute_plan_touch_partiton_nums = n;
  }
  int get_execute_plan_touch_partition_nums() {
    return execute_plan_touch_partiton_nums;
  }
  void inc_times_move_data() { ++times_move_data; }
  int get_times_move_data() const { return times_move_data; }
  void reset_times_move_data() { times_move_data = 0; }
  int get_acl_version() { return acl_version; }
  void set_acl_version(int val) { acl_version = val; }
  void set_cur_auth_plugin(AuthPlugin cur_plugin) {
    cur_auth_plugin = cur_plugin;
  }
  AuthPlugin get_cur_auth_plugin() { return cur_auth_plugin; }

 private:
  void init_packet_pool_vec();
  void release_packet_pool_vec();

 protected:
  int flow_rate_control_query_cmd_total_count;
  int max_query_count_per_flow_control_gap_local;
  bool has_more_result;

 private:
  string spark_job_id;
  vector<PacketPool *> packet_pool_vec;
  bool in_explain;
  int explain_node_id;
  list<ExplainElement *> explain_element_list;
  list<pair<string, string> > explain_sub_sql_list;
  map<string, OptionValue> session_option_value;
  evutil_socket_t client_socket;
  struct event *read_socket_event;
  SocketEventBase *socket_base;
  struct event *internal_event;
  InternalEventBase *internal_base;
  SessionState session_state;
  Stream *stream;
  unsigned long slow_query_time_local;
  unsigned int audit_log_flag_local;
  int execute_plan_touch_partiton_nums;  //-1 means touch all partition
  int times_move_data;

  int refuse_version;
  set<string> refuse_modify_table;
  /*maps to maintain the socket event and event base of server conn with its
   * socket. These two maps will be used in session server waiting swap.*/
  map<evutil_socket_t, SocketEventBase *> server_base_map;
  map<evutil_socket_t, struct event *> server_event_map;
  bool executing_spark;

 private:
  int acl_version;
  string server_scramble;
  string user_host;
  int con_select;
  int con_insert;
  int con_update;
  int con_delete;
  int con_insert_select;
  char client_scramble[SHA2_HASH_SIZE + 1];
  AuthPlugin cur_auth_plugin;
  uint32_t thread_id;
  bool keep_conn;
  bool update_option;
  ACE_Thread_Mutex mutex_update_option;
  bool is_killed;
  bool in_killing;

  map<string, ACE_Time_Value> monitor_point_map;
  Handler *handler;

  ExecutePlan *top_plan;
  Statement *top_stmt;
  string alter_table_name;

  /*The handle_query_command may be invoked nested in current code, such as
   * prepare execute stmt. In this case, we need to record all nest top plan
   * to ensure the object free.
   *
   * And also, nest handle_query_command means nest plan execution, which is
   * not support for session swap during plan execution currently.
   *
   * The prepare execution's nest plan execution code will be refined in issue
   * #1910.*/
  list<ExecutePlan *> nest_top_plan;
  list<Statement *> nest_top_stmt;

  /*If cur sql generate more than one separate node, we will consider it as
   * * complex stmt.
   *
   * And if the conn may be complete inside session, such as modify select
   * stmt in transaction, we also consider it as complex; so the
   * insert_select, modify_select, modify_limit, and multiple_partition stmt
   * after merge, will be marked as complex stmt.
   *
   * And for call procedure, also consider it as complex.*/
  bool is_complex_stmt;

  /*If is_complex_stmt is false and the plan only has the direct node,
   * is_simple_direct_stmt is true. In this case only one thread for one
   * session, so the lock protection inside the session may be eliminated in
   * some cases.*/
  bool is_simple_direct_stmt;

  bool skip_mutex_in_operation;

  TransferRule *transfer_rule;
  MemoryAllocater mem_alloc;
  MemoryAllocater alloc_for_flex;

  PacketAllocater packet_alloc;
  PacketAllocater fetchnode_packet_alloc;  // PacketAllocater for one thread
                                           // deal all fetch nodes.

  /* We should record the number of the mysql transactions in one DBSCale
   * transactions. For example: DBSCALE TRANSACTION => server1, server2,
   * server3. The number should be 3. The number is set when
   * "commit"/"rollback" statement is issued. Currently, the number is equal
   * to the size of the kept_connections.  Now this can be used in all the
   * session witch will use multiple connections. */
  int mul_connection_num;

  int arrived_transaction_num;
  ACE_Thread_Mutex arrived_trans_num_mutex;
  string session_var_map_md5;
  string schema_name_md5;

  /*
   * Be used to record net traffic
   */
  unsigned long long net_in;
  unsigned long long net_out;
  ACE_Thread_Mutex net_status_mutex;

  Session *work_session;
  bool error_generated_by_dbscale;
  bool is_login_dbscale_read_only_user;
  string start_config_lock_extra_info;
  bool error_generated_by_force_stop_trx;

 public:
  bool monitor_flag;
  // tmp global table
  set<DataSpace *> global_tmp_space;

 protected:
  queue<pair<unsigned int, int> >
      flow_rate_control_queue;  // queue<pair<second, query count in this
                                // second> >
  vector<string> remove_sp_vector;
  map<string, set<int64_t> > mul_auto_increment_value_map;
  bool is_call_sp;
  string ddl_comment_sql;
  bool drop_current_schema;
  int call_sp_nest;
  bool transaction_error;
  unsigned int real_time_queries;
  unsigned int
      global_table_real_time_queries;  // real time queries for global table
  bool global_table_can_merge;
  bool is_keepmaster;
  bool is_readonly;
  AffetcedServerType affect_server_type;
  ExecuteProfileHandler execution_profile;
  bool server_shutdown;
  int64_t affected_rows;
  bool has_var;
  bool in_lock;
  bool is_federated_session;
  bool fe_error;
  set<DataSpace *> table_without_given_schema;
  bool in_transaction;
  bool in_transaction_consistent;  // when do start "transaction with consistent
                                   // snapshot"
  SessionXaTrxState cluster_xa_transaction_state;
  Connection *cluster_xa_conn;
  string cluster_xid;
  bool trx_specify_read_only;
  bool event_add_failed;
  bool executing_prepare;
  set<Connection *> using_conns;
  set<Connection *> cur_stmt_conns;
  ACE_Thread_Mutex using_conns_mutex;
  bool non_swap_session;  // If the session can not swap after send to server,
                          // we set the session as non_swap_session=1

  bool auto_commit_is_on;
  string stmt_auto_commit_value;
  int stmt_auto_commit_int_value;
  bool has_check_first_sql;  // For federated table session, the first sql will
                             // be select...where 1=0;
  // reuse Statement to improve performance, if sql is simple, re_init stmt and
  // reuse it. if sql is complex, delete stmt.
  Statement *stmt;
  bool is_query_command;

  bool is_load_data_local_command;
  bool federated_finished_killed;
  bool is_need_swap_handler;
  ACE_Time_Value query_time;
  ACE_Time_Value idle_time;
  ACE_Time_Value login_time;
  ACE_Time_Value start_transaction_time;
  char query_sql[SQL_LEN + 1];
  size_t query_sql_len;
  string entire_query_sql;
  /* In Federated Cross NOde Join, the kill command should set the variable to
   * tell the session to wait for kill. */
  bool prepare_kill_session;

  // container to store tables name, their table info should be deleted after
  // plan executed.
 public:
  vector<pair<string, string> > table_info_to_delete_pair_vec;

 protected:
  string user_name;

  bool mul_node_timeout;
  bool client_swap_timeout;
  map<DataSpace *, Connection *> defined_lock_kept_conn;
  map<DataSpace *, DataSpace *> defined_lock_space_kept_conn;
  ACE_Thread_Mutex defined_lock_mutex;

 public:
  string query_sql_replace_null;
  inline void set_query_sql_replace_null(string &str) {
    query_sql_replace_null = str;
  }
  inline string &get_query_sql_replace_null() { return query_sql_replace_null; }
  vector<size_t> query_sql_null_pos;
  inline void reset_query_sql_null_pos() { query_sql_null_pos.clear(); }
  inline void add_query_sql_null_pos(size_t pos) {
    query_sql_null_pos.push_back(pos);
  }
  inline vector<size_t> get_query_sql_null_pos() const {
    return query_sql_null_pos;
  }
  string query_sql_replace_null_char;
  inline void set_query_sql_replace_null_char(string c) {
    query_sql_replace_null_char = c;
  }
  inline string get_query_sql_replace_null_char() const {
    return query_sql_replace_null_char;
  }

  bool get_federated_finished_killed() { return federated_finished_killed; }
  void set_federated_finished_killed(bool can_kill) {
    federated_finished_killed = can_kill;
  }
  /*federate cross node join released functions*/

  void set_defined_lock_kept_conn(DataSpace *ds, Connection *conn);
  map<DataSpace *, Connection *> get_defined_lock_kept_conn() {
    defined_lock_mutex.acquire();
    map<DataSpace *, Connection *> ret = defined_lock_kept_conn;
    defined_lock_mutex.release();
    return ret;
  }
  Connection *get_defined_lock_kept_conn(DataSpace *dataspace);
  Connection *get_defined_lock_kept_conn_real(DataSpace *dataspace);
  Connection *check_defined_lock_kept_conn_for_same_server(
      DataSpace *dataspace);
  bool defined_lock_need_kept_conn(DataSpace *dataspace) {
    if (need_lock_kept_conn && check_lock_conn_in_kept_conn(dataspace))
      return true;
    return false;
  }
  void remove_defined_lock_kept_conn(DataSpace *dataspace);
  void clean_defined_lock_conn();
  void set_lock_kept_conn(bool b) { need_lock_kept_conn = b; }
  bool get_lock_kept_conn() { return need_lock_kept_conn; }
  bool lock_conn_add_back_to_dead();
  void defined_lock_depth_add() { defined_lock_depth++; }
  void defined_lock_depth_sub() { defined_lock_depth--; }
  int get_defined_lock_depth() { return defined_lock_depth; }
  void reset_defined_lock_depth() { defined_lock_depth = 0; }
  void set_defined_lock_field_name(const char *str) {
    defined_lock_field_name = str;
  }
  string get_defined_lock_field_name() { return defined_lock_field_name; }
  bool check_lock_conn_in_kept_conn(DataSpace *ds);
  bool check_dataspace_is_catalog_space(DataSpace *dataspace);

  /*used by federated thread*/
  void set_wakeup_by_empty_leader() { wakeup_by_empty_leader = true; }
  bool is_wakeup_by_empty_leader() { return wakeup_by_empty_leader; }
  void reinit_follower_session() {
    wakeup_by_empty_leader = false;
    follower_wakeuped = false;
  }
  void follower_session_wait() {
    follower_mutex.acquire();
    if (!follower_wakeuped) {
      add_traditional_num();
      cond_follower.wait();
      sub_traditional_num();
    }
    follower_mutex.release();
  }
  void follower_session_wakup() {
    follower_mutex.acquire();
    cond_follower.signal();
    follower_wakeuped = true;
    follower_mutex.release();
  }

  void set_transfer_rule(TransferRule *rule) { transfer_rule = rule; }

  TransferRule *get_transfer_rule() { return transfer_rule; }
  void set_is_federated_session(bool b) { is_federated_session = b; }

  bool get_is_federated_session() const { return is_federated_session; }

  void set_fe_error(bool b) { fe_error = b; }
  bool get_fe_error() { return fe_error; }

  /*used by working thread*/
  virtual TransferRuleManager *get_transfer_rule_manager() = 0;
  virtual void reset_transfer_rule_manager() = 0;

  void reset_has_send_client_error_packet() {
    has_send_client_error_packet = false;
  }
  void reset_slow_query_time_local() {
    slow_query_time_local = slow_query_time;
  }
  unsigned long get_slow_query_time_local() { return slow_query_time_local; }
  void set_audit_log_flag_local() { audit_log_flag_local = audit_log_flag; }
  unsigned int get_audit_log_flag_local() { return audit_log_flag_local; }

  bool get_has_send_client_error_packet() {
    return has_send_client_error_packet;
  }

  void set_has_send_client_error_packet() {
    has_send_client_error_packet = true;
  }

  void acquire_has_send_client_error_packet_mutex() {
#ifdef DEBUG
    LOG_DEBUG("Try to acquire has_send_client_error_packet_mutex.\n");
#endif
    send_client_error_packet_mutex.acquire();
  }
  void release_has_send_client_error_packet_mutex() {
    send_client_error_packet_mutex.release();
#ifdef DEBUG
    LOG_DEBUG("Try to release has_send_client_error_packet_mutex.\n");
#endif
  }

  void update_view_map();
  bool is_view_updated() { return view_updated; }
  void set_view_updated(bool b) { view_updated = b; }
  void get_one_view(string view, string &view_sql) {
    if (view_map.count(view))
      view_sql = view_map[view];
    else
      view_sql.clear();
  }

  void add_cross_node_join_sql(const char *sql) {
    ACE_Log_Priority log_level = LM_DEBUG;
    OptionParser::instance()->get_option_value(&log_level, "log_level");
    if (log_level != LM_INFO || session_cache_sqls == 0) {
      return;
    }
    sql_is_cross_node_join.insert(sql);
  }
  void add_session_sql_cache(const char *sql) {
    if (session_cache_sqls == 0) return;
    ACE_Log_Priority log_level = LM_DEBUG;
    OptionParser::instance()->get_option_value(&log_level, "log_level");
    if (log_level != LM_INFO) {
      return;
    }
    session_sql_cache.push_back(sql);
  }
  void flush_session_sql_cache(bool force);
  virtual bool contains_odbc_conn() = 0;

 protected:
  bool wakeup_by_empty_leader;
  bool follower_wakeuped;
  ACE_Thread_Mutex follower_mutex;
  ACE_Condition<ACE_Thread_Mutex> cond_follower;
  UserPriorityValue user_priority;
  bool can_swap;
  /*For sep node parallel execution(Issue #1555), there may be several backend
   * thread executing separate node with an error packet, which need to be
   * send to client. But only one error packet should be sent to client.
   *
   * So flag has_send_client_error_packet will be used to check whether one
   * error packet has been sent to client.*/
  bool has_send_client_error_packet;
  ACE_Thread_Mutex send_client_error_packet_mutex;

  bool view_updated;
  map<string, string> view_map;
  vector<string> session_sql_cache;
  set<string> sql_is_cross_node_join;
  ACE_Thread_Mutex add_load_warning_packet_list_mutex;
  string defined_lock_field_name;
  bool need_lock_kept_conn;
  int defined_lock_depth;

 public:
  bool is_do_logining() { return do_logining; }
  void set_do_login(bool b) { do_logining = b; }
  void set_is_super_user(bool b) { is_super_user = b; }
  bool get_is_super_user() { return is_super_user; }
  void set_is_dbscale_read_only_user(bool b) {
    is_login_dbscale_read_only_user = b;
  }
  bool get_is_dbscale_read_only_user() {
    return is_login_dbscale_read_only_user;
  }
  list<WarnInfo *> get_warning_info() { return warnings; }
  void add_warning_info(WarnInfo *w) { warnings.push_back(w); }
  void reset_warning_info() {
    if (!warnings.empty()) {
      list<WarnInfo *>::iterator it = warnings.begin();
      for (; it != warnings.end(); ++it) {
        delete *it;
      }
      warnings.clear();
    }
  }
  void add_load_warning_packet_list(vector<Packet *> *warning_packet_list,
                                    uint64_t row_count) {
    LOG_DEBUG("Add load warning packet list row_count: [%Q]\n", row_count);
    add_load_warning_packet_list_mutex.acquire();
    load_warning_packets.insert(warning_packet_list);
    load_warning_count += row_count;
    add_load_warning_packet_list_mutex.release();
  }

  set<vector<Packet *> *> *get_load_warning_packets() {
    return &load_warning_packets;
  }
  uint64_t get_load_warning_count() { return load_warning_count; }
  void reset_load_warning_packet_list() {
    load_warning_count = 0;
    if (cancel_reset_stmt_level_variables_once) return;
    LOG_DEBUG("Reset load warnings packet list\n");
    if (!load_warning_packets.empty()) {
      set<vector<Packet *> *>::iterator it = load_warning_packets.begin();
      for (; it != load_warning_packets.end(); ++it) {
        vector<Packet *>::iterator it2 = (*it)->begin();
        for (; it2 != (*it)->end(); ++it2) {
          delete *it2;
        }
        delete *it;
      }
      load_warning_packets.clear();
    }
  }
  void set_cur_stmt_execute_fail(bool b) {
    LOG_DEBUG("set cur stmt execute fail: [%d]\n", b);
    cur_stmt_execute_fail = b;
  }
  bool get_cur_stmt_execute_fail() { return cur_stmt_execute_fail; }
  void set_do_swap_route(bool b) { do_swap_route = b; }
  bool get_do_swap_route() { return do_swap_route; }
  void set_client_can_read(bool b) { client_can_read = b; }
  bool get_client_can_read() { return client_can_read; }
  bool get_enable_silent_execute_prepare_local() {
    return enable_silent_execute_prepare_local;
  }
  uint16_t get_cur_stmt_error_code() { return cur_stmt_error_code; }
  void set_cur_stmt_error_code(uint16_t code) { cur_stmt_error_code = code; }
  void set_cur_stmt_type(stmt_type type) {
    if (cur_stmt_type == STMT_NON) cur_stmt_type = type;
  }
  stmt_type get_cur_stmt_type() { return cur_stmt_type; }
  void acquire_xa_mutex() { xa_transaction_mutex.acquire(); }
  void release_xa_mutex() { xa_transaction_mutex.release(); }
  bool need_get_max_inc_value(string full_table_name) {
    if (multiple_mode && enable_session_get_max_inc_value) {
      if (inc_table_set.count(full_table_name))
        return false;
      else {
        inc_table_set.insert(full_table_name);
        return true;
      }
    }
    return false;
  }

  string get_column_replace_string(string key);
  string get_zk_start_config_lock_extra_info();

  void clear_column_replace_string() {
    column_replace_map.clear();
    column_replace_num = 0;
  }

  int check_ping_and_change_master();

 protected:
  bool do_logining;  // whether this session is doing login operation
  set<string> inc_table_set;

  bool is_super_user;
  list<WarnInfo *> warnings;
  set<vector<Packet *> *> load_warning_packets;
  uint64_t load_warning_count;

  bool has_global_flush_read_lock;  // used for flush tables with read lock

  bool cur_stmt_execute_fail;  // whether current stmt execution fail
  bool do_swap_route;
  bool client_can_read;
  bool enable_silent_execute_prepare_local;

  uint16_t cur_stmt_error_code;
  stmt_type cur_stmt_type;
  ACE_RW_Thread_Mutex xa_transaction_mutex;

  unsigned int column_replace_num;
  map<string, string, strcasecomp> column_replace_map;
  ACE_Thread_Mutex column_replace_map_mutex;
  string cross_tmp_table_collate;
  string cross_tmp_table_charset;

 public:
  bool get_is_silence_ok_stmt() const { return is_silence_ok_stmt; }
  void set_is_silence_ok_stmt(bool si) { is_silence_ok_stmt = si; }
  void set_is_info_schema_mirror_tb_reload_internal_session(bool b) {
    is_info_schema_mirror_tb_reload_internal_session = b;
  }
  bool get_is_info_schema_mirror_tb_reload_internal_session() {
    return is_info_schema_mirror_tb_reload_internal_session;
  }

  void set_silence_ok_stmt_id(uint32_t id) { silence_ok_stmt_id = id; }
  uint32_t get_silence_ok_stmt_id() { return silence_ok_stmt_id; }

  virtual bool silence_execute_group_ok_stmt(uint32_t id) = 0;
  virtual bool silence_roll_back_group_sql(uint32_t id) = 0;
  virtual bool silence_reload_info_schema_mirror_tb(string &sql) = 0;

  void add_temp_table_set(const char *schema_name, const char *table_name,
                          Connection *conn) {
    string name(schema_name);
    name.append(".");
    name.append(table_name);

    temp_table_mutex.acquire();
    temp_table_set[name] = conn;
    temp_table_mutex.release();
  }
  void remove_temp_table_set(const char *schema_name, const char *table_name) {
    string name(schema_name);
    name.append(".");
    name.append(table_name);

    temp_table_mutex.acquire();
    temp_table_set.erase(name);
    temp_table_mutex.release();
  }

  bool is_temp_table_set_contain_db(const char *schema_name) {
    bool ret = false;
    temp_table_mutex.acquire();
    map<string, Connection *>::iterator it = temp_table_set.begin();
    for (; it != temp_table_set.end(); ++it) {
      string fulltable = it->first;
      vector<string> db_and_temp_table;
      boost::split(db_and_temp_table, fulltable, boost::is_any_of("."),
                   boost::token_compress_on);
      if (db_and_temp_table.size() > 0) {
        if (!strcasecmp(schema_name, db_and_temp_table[0].c_str())) {
          ret = true;
        }
      }
    }
    temp_table_mutex.release();
    return ret;
  }

  void remove_temp_table_set_by_conn(Connection *conn) {
    temp_table_mutex.acquire();
    map<string, Connection *>::iterator it = temp_table_set.begin();
    for (; it != temp_table_set.end();) {
      if (it->second == conn) {
        temp_table_set.erase(it++);
      } else {
        ++it;
      }
    }
    temp_table_mutex.release();
  }
  bool has_temp_table_set() {
    temp_table_mutex.acquire();
    bool ret = !temp_table_set.empty();
    temp_table_mutex.release();
    return ret;
  }

  bool is_temp_table(const char *schema_name, const char *table_name) {
    string name(schema_name);
    name.append(".");
    name.append(table_name);

    temp_table_mutex.acquire();
    bool ret = temp_table_set.count(name);
    temp_table_mutex.release();
    return ret;
  }
  void reset_cross_tmp_table_collate() { cross_tmp_table_collate.clear(); }
  string get_cross_tmp_table_collate() { return cross_tmp_table_collate; }
  void set_cross_tmp_table_collate(const char *co) {
    cross_tmp_table_collate.assign(co);
  }
  void reset_cross_tmp_table_charset() { cross_tmp_table_charset.clear(); }
  string get_cross_tmp_table_charset() { return cross_tmp_table_charset; }
  void set_cross_tmp_table_charset(const char *ch) {
    cross_tmp_table_charset.assign(ch);
  }
  void set_has_check_acl(bool b) { has_check_acl = b; }
  bool get_has_check_acl() { return has_check_acl; }
  void set_has_do_audit_log(bool b) { has_do_audit_log = b; }
  bool get_has_do_audit_log() { return has_do_audit_log; }
  void get_local_dataservers_rep_relation(
      ConfigSourceRepResource &local_dataservers_rep_relation);
  bool is_dataspace_cover_session_level(DataSpace *ds1, DataSpace *ds2);
  bool is_datasource_cover_session_level(DataSource *ds1, DataSource *ds2);

  bool add_running_xa_map(unsigned long xid, RunningXAType type,
                          map<DataSpace *, Connection *> *kept_map = NULL);
  map<unsigned long, RunningXAType> get_running_xa_map(DataSource *fail_source);
  void clear_running_xa_map() {
    running_xa_map_mutex.acquire();
    running_xa_map.clear();
    running_source_set.clear();
    need_abort_xa_transaction = false;
    running_xa_map_mutex.release();
  }

 private:
  bool is_silence_ok_stmt;
  bool is_info_schema_mirror_tb_reload_internal_session;
  uint32_t silence_ok_stmt_id;
  bool has_do_audit_log;
  ConfigSourceRepResource local_dataservers_rep_relation;
  size_t execute_node_id_in_plan;
  bool enable_session_packet_pool_local;
  int packet_pool_count;
  int packet_pool_init_packet_count_local;
  int packet_pool_packet_bundle_local;

  // below map and set is used for xa recover of source failover.
  map<unsigned long, RunningXAType> running_xa_map;
  set<DataSource *> running_source_set;

  ACE_Thread_Mutex running_xa_map_mutex;

 protected:
  bool need_abort_xa_transaction;
  map<string, Connection *> temp_table_set;
  ACE_Thread_Mutex temp_table_mutex;

  bool has_check_acl;

  int select_lock_type;

 public:
  void set_ssl_stream(ACE_SSL_SOCK_Stream *ssl) { ssl_stream = ssl; }
  ACE_SSL_SOCK_Stream *get_ssl_stream() { return ssl_stream; }
  size_t get_next_id_in_plan() {
    size_t ret = execute_node_id_in_plan;
    ++execute_node_id_in_plan;
    return ret;
  }
  int get_packet_pool_count() { return packet_pool_count; }
  size_t get_enable_session_packet_pool_local() {
    return enable_session_packet_pool_local;
  }
  size_t get_session_packet_pool_packet_bundle_local() {
    return packet_pool_packet_bundle_local;
  }

  void get_free_packet_from_pool(
      size_t id, int count,
      list<Packet *, StaticAllocator<Packet *> > *ret_list);
  void put_free_packet_back_to_pool(
      size_t id, int &count,
      list<Packet *, StaticAllocator<Packet *> > &pkt_list);
  void add_packet_to_pool_no_lock(Packet *packet);

  void set_select_lock_type(int type) { select_lock_type = type; }
  int get_select_lock_type() { return select_lock_type; }

 protected:
  ACE_SSL_SOCK_Stream *ssl_stream;

 public:
  bool is_got_error_er_must_change_password() {
    return got_error_er_must_change_password;
  }
  void set_got_error_er_must_change_password(bool b) {
    got_error_er_must_change_password = b;
  }

 protected:
  bool got_error_er_must_change_password;
  size_t retl_tb_end_pos;
  bool is_admin_accepter;
  Connection *trans_conn;

 public:
  // void set_session_load_balance_strategy(SessionLoadBalanceStrategy strategy)
  // {
  //   session_load_balance_strategy = strategy;
  // }
  // SessionLoadBalanceStrategy get_session_load_balance_strategy() {
  //   return session_load_balance_strategy;
  // }
  void set_is_admin_accepter(bool b) { is_admin_accepter = b; }
  bool get_is_admin_accepter() { return is_admin_accepter; }
  Connection *get_trans_conn() { return trans_conn; }
  bufferevent *get_bev_client() { return bev_client; }
  bufferevent *get_bev_server() { return bev_server; }
  void set_client_broken(bool b) { client_broken = b; }
  bool get_client_broken() { return client_broken; }
  void set_server_broken(bool b) { server_broken = b; }
  bool get_server_broken() { return server_broken; }
  void close_trans_conn();
  void create_trans_conn() { do_create_trans_conn(); }
  void init_transmode_internal_base();
  void stop_trans_event();
  virtual void do_create_trans_conn() = 0;

 private:
  // SessionLoadBalanceStrategy session_load_balance_strategy;
  struct event *internal_server_event;
  struct bufferevent *bev_client;
  struct bufferevent *bev_server;
  bool client_broken;
  bool server_broken;
  bool trans_state_ok;

#ifndef DBSCALE_TEST_DISABLE
 public:
  MonitorPoint mp_svc;
  MonitorPoint mp_stmt_exec;
#endif

 public:
  void set_bridge_mark_sql(const char *sql, size_t retl_tb_end_pos);
  string &get_bridge_mark_sql() { return bridge_mark_sql; }
  bool has_bridge_mark_sql() { return !bridge_mark_sql.empty(); }
  void adjust_retl_mark_tb_name(DataSpace *space, string &sql);

 private:
  string bridge_mark_sql;

 public:
  void set_can_change_affected_rows(bool b) { can_change_affected_rows = b; }
  bool get_can_change_affected_rows() { return can_change_affected_rows; }
  void set_cancel_reset_stmt_level_variables_one() {
    cancel_reset_stmt_level_variables_once = true;
  }

 private:
  bool can_change_affected_rows;
  bool cancel_reset_stmt_level_variables_once;

 public:
  bool get_alloc(unsigned long long size);
  void giveback_left_alloc(unsigned long long size);
  inline bool get_has_fetch_node() { return has_fetch_node; }
  void set_has_fetch_node(bool b) { has_fetch_node = b; }
  void giveback_left_alloc_to_backend();
  bool check_mem_in_session(unsigned long long &total) {
    LOG_DEBUG(
        "session memory info is :"
        "max_alloc is %Q,left_alloc is %Q\n",
        max_alloc, left_alloc);
    if (left_alloc) {
      LOG_DEBUG(
          "there are memory leak in enable-calculate-select-total-memory in "
          "session,max_alloc is %Q,left_alloc is %Q\n",
          max_alloc, left_alloc);
      return false;
    }
    total = max_alloc;
    return true;
  }
  unsigned long long get_left_alloc() { return left_alloc; }
  unsigned long long get_max_alloc() { return max_alloc; }
  bool get_is_surround_sql_with_trx_succeed() {
    return is_surround_sql_with_trx_succeed;
  }
  void set_is_surround_sql_with_trx_succeed(bool b) {
    is_surround_sql_with_trx_succeed = b;
  }

 private:
  bool has_fetch_node;
  unsigned long long left_alloc;
  unsigned long long max_alloc;
  ACE_Thread_Mutex left_alloc_mutex;
  bool is_surround_sql_with_trx_succeed;
};

}  // namespace sql
}  // namespace dbscale

#endif /* DBSCALE_SESSION_H */
