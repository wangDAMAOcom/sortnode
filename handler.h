#ifndef DBSCALE_HANDLER_H
#define DBSCALE_HANDLER_H

/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/SOCK_Stream.h>
#include <ace/Svc_Handler.h>
#include <backend.h>
#include <basic.h>
#include <cluster_status.h>
#include <connection.h>
#include <log.h>
#include <parser.h>
#include <session.h>
#include <statement.h>
#include <thread_resource.h>

#include <list>
#include <unordered_map>

using std::list;
using namespace dbscale;
using namespace dbscale::sql;

namespace dbscale {
#define SAVEPOINT_NAME "dbscale_save_point_internal"

template <class T>
class Pool;
class Backend;
class MigrateDataTool;
class MultipleManager;
class MigrateCleanTool;
class MigrateHandlerTask;
class SocketEventBase;
class Handler;

#ifndef DBSCALE_TEST_DISABLE
class HandlerMonitorPoint {
 public:
  HandlerMonitorPoint(const char *name, Handler *handler)
      : name(name), handler(handler) {}

  void start_timing();
  void end_timing();
  void end_timing_and_report();

 private:
  ACE_Time_Value start_date;
  ACE_Time_Value end_date;
  ACE_Time_Value cost;
  string name;
  Handler *handler;
};
#endif

enum class monitor_stage {
  SQL_PARSE,
  PLAN_GENERATION,
  PLAN_EXECUTION,
  SUCCEED,
  FAILED
};

class MonitorPointSQLStatistic {
 public:
  MonitorPointSQLStatistic(Handler *handler)
      : start_time(ACE_Time_Value::zero),
        parse_time(ACE_Time_Value::zero),
        plan_generation_time(ACE_Time_Value::zero),
        plan_execution_time(ACE_Time_Value::zero),
        failed(false),
        query_sql(),
        no_param_sql(),
        no_param_sql_id(),
        handler(handler),
        is_need_monitor(true),
        record_as_error(false),
        should_record(false) {}

  void start_timing();
  void end_timing(monitor_stage stage);
  void set_query_sql(const char *sql) { query_sql = sql; }
  void set_outline_info(const char *no_param_sql, const char *no_param_sql_id);
  static bool should_record_query_sql(stmt_type type);
  void set_record_as_error(bool b) { record_as_error = b; }
  bool is_record_as_error() { return record_as_error; }
  void set_is_should_record(bool b) { should_record = b; }
  bool is_should_record() { return should_record; }

  // is_need_monitor will be reset to true for every SQL we received.
  // The first time we call need_monitor will return true, and any other calls
  // to this function will return false until we reset it.
  // We use this function to ensure that only the first
  // handle_query_command will monitor time using
  bool need_monitor() {
    return is_need_monitor;
    is_need_monitor = false;
  }
  void reset() {
    is_need_monitor = true;
    start_time = ACE_Time_Value::zero;
    parse_time = ACE_Time_Value::zero;
    plan_generation_time = ACE_Time_Value::zero;
    plan_execution_time = ACE_Time_Value::zero;
    failed = false;
    query_sql.clear();
    no_param_sql.clear();
    no_param_sql_id.clear();
    record_as_error = false;
    should_record = false;
  }

 private:
  void report_to_handler_self();

 private:
  ACE_Time_Value start_time;
  ACE_Time_Value parse_time;
  ACE_Time_Value plan_generation_time;
  ACE_Time_Value plan_execution_time;
  bool failed;
  string query_sql;
  string no_param_sql;
  string no_param_sql_id;
  Handler *handler;
  bool is_need_monitor;
  bool record_as_error;
  bool should_record;
};

struct MonitorPointSQLStatisticTimer {
  MonitorPointSQLStatisticTimer(MonitorPointSQLStatistic &statistic,
                                monitor_stage type)
      : statistic(statistic), type(type) {
    statistic.start_timing();
  }
  ~MonitorPointSQLStatisticTimer() { statistic.end_timing(type); }
  MonitorPointSQLStatistic &statistic;
  monitor_stage type;
};

class TransactionTimerHandler : public ACE_Event_Handler {
 public:
  TransactionTimerHandler(Handler *h)
      : is_running(false), timer_id(-1), handler(h) {}

  ~TransactionTimerHandler() {}

  void start_schedule_timer();

  void stop_schedule_timer() {
    if (is_running) {
      this->reactor()->cancel_timer(this->timer_id);
      is_running = false;
      timer_id = -1;
    }
    is_running = false;
  }

  bool get_is_timer_handler_running() { return is_running; }

  // callback function
  int handle_timeout(const ACE_Time_Value &current_time, const void *act = 0);

  long get_timer_id() { return timer_id; }

 private:
  bool is_running;
  long timer_id;
  Handler *handler;
};

class Handler : public ThreadResource,
                public ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_MT_SYNCH> {
  typedef ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_MT_SYNCH> super;

 public:
  Handler();
  virtual ~Handler();
  void set_signal_handler() { is_signal_handler = true; }
  bool get_is_migrate_async_handler() { return is_migrate_async_handler; }
  void set_migrate_async_handler(bool b) { is_migrate_async_handler = b; }

  void report_monitor_value(const char *str, ACE_Time_Value value) {
    string tmp(str);
    if (monitor_point_map.count(tmp)) {
      monitor_point_map[tmp] += value;
    } else
      monitor_point_map[tmp] = value;
  }
  virtual void recover_replication_info_in_slave_dbscale_mode() {
    throw NotImplementedError();
  }
  void reset_monitor_point_map() { monitor_point_map.clear(); }

  map<string, ACE_Time_Value> *get_monitor_point_map() {
    return &monitor_point_map;
  }

  void report_monitor_point_to_backend();

  void report_global_monitor_value(ACE_Time_Value total_cost,
                                   ACE_Time_Value schedule_cost,
                                   ACE_Time_Value backend_cost) {
    handler_monitor_point_mutex.acquire();
    string str("total");
    if (global_monitor_point_map.count(str)) {
      global_monitor_point_map[str] += total_cost;
    } else {
      global_monitor_point_map[str] = total_cost;
    }
    str = "schedule";
    if (global_monitor_point_map.count(str)) {
      global_monitor_point_map[str] += schedule_cost;
    } else {
      global_monitor_point_map[str] = schedule_cost;
    }
    str = "backend";
    if (global_monitor_point_map.count(str)) {
      global_monitor_point_map[str] += backend_cost;
    } else {
      global_monitor_point_map[str] = backend_cost;
    }
    handler_monitor_point_mutex.release();
  }
  void report_monitor_point_number_value(bool failed, unsigned long num) {
    handler_monitor_point_mutex.acquire();
    if (!failed) {
      string str("success");
      if (num_of_executed.count(str)) {
        num_of_executed[str] += num;
      } else
        num_of_executed[str] = num;
    } else {
      string str("fail");
      if (num_of_executed.count(str)) {
        num_of_executed[str] += num;
      } else
        num_of_executed[str] = num;
    }
    handler_monitor_point_mutex.release();
  }
  void report_monitor_point_histogram_value(ACE_Time_Value cost,
                                            unsigned long value) {
    unsigned long inter = histogram_monitor_point_grad;
    unsigned long key = cost.msec() / inter;
    handler_monitor_point_mutex.acquire();
    if (histogram_monitor_point.count(key)) {
      histogram_monitor_point[key] += value;
    } else {
      histogram_monitor_point[key] = value;
    }
    handler_monitor_point_mutex.release();
  }
  void report_outline_monitor_info(const char *sql_id,
                                   const SQLMonitorInfo &sql_monitor_info) {
    ACE_Guard<ACE_Thread_Mutex> _(sql_outline_monitor_info_map_lock);
    sql_outline_monitor_info_map[sql_id] += sql_monitor_info;
  }
  void swap_outline_monitor_info(SQLOutlineMonitorInfoMap &m) {
    ACE_Guard<ACE_Thread_Mutex> _(sql_outline_monitor_info_map_lock);
    m.swap(sql_outline_monitor_info_map);
  }
  void reset_global_monitor_point_map() { global_monitor_point_map.clear(); }
  map<string, ACE_Time_Value> *get_global_monitor_point_map() {
    return &global_monitor_point_map;
  }
  void reset_global_monitor_point_number_map() { num_of_executed.clear(); }
  map<string, unsigned long> *get_global_monitor_point_number_map() {
    return &num_of_executed;
  }
  void reset_histogram_monitor_point_map() { histogram_monitor_point.clear(); }
  map<unsigned long, unsigned long> *get_histogram_monitor_point_map() {
    return &histogram_monitor_point;
  }

  void re_init_handler(bool clean_session);
  int open(void *);
  int close(u_long flags = 0) {
    ACE_UNUSED_ARG(flags);
    return 0;
  }
  int svc();
  void set_nodelay();
  void stop_dbscale();
  void set_stream(Stream *value) { stream = value; }
  Stream *get_stream() { return stream; }
  void begin_session() {
#ifdef DEBUG
    LOG_DEBUG("Handler::begin_session enter\n");
#endif
    this->do_begin_session();
#ifdef DEBUG
    LOG_DEBUG("Handler::begin_session leave\n");
#endif
  }
  void end_session() {
#ifdef DEBUG
    LOG_DEBUG("Handler::end_session enter\n");
#endif
    this->do_end_session();
#ifdef DEBUG
    LOG_DEBUG("Handler::end_session leave\n");
#endif
  }
  Session *get_session() { return this->do_get_session(); }
  void set_session(Session *s) { this->do_set_session(s); }
  MigrateDataTool *get_insert_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return do_get_insert_select_migrate_tool(
        source_space, target_source, schema_name, source_schema_name,
        source_table_name, target_schema_name, target_table_name);
  }
  MigrateDataTool *get_load_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return do_get_load_select_migrate_tool(
        source_space, target_source, schema_name, source_schema_name,
        source_table_name, target_schema_name, target_table_name);
  }
  MigrateDataTool *get_physical_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return do_get_physical_migrate_tool(
        source_space, target_source, schema_name, source_schema_name,
        source_table_name, target_schema_name, target_table_name);
  }
  MigrateCleanTool *get_migrate_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    return do_get_migrate_clean_tool(source, type, schema_name,
                                     clean_schema_name, table_name, vid_set,
                                     source_name, target_source_name);
  }
  MigrateCleanTool *get_migrate_physical_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    return do_get_migrate_physical_clean_tool(
        source, type, schema_name, clean_schema_name, table_name, vid_set,
        source_name, target_source_name);
  }
  MigrateHandlerTask *get_migrate_handler_task(string sql, string schema_name) {
    return do_get_migrate_handler_task(sql, schema_name);
  }
  void authenticate() {
#ifdef DEBUG
    LOG_DEBUG("Handler::authenticate enter\n");
#endif
    this->do_authenticate();
#ifdef DEBUG
    LOG_DEBUG("Handler::authenticate leave\n");
#endif
  }
  void refuse_login(uint16_t error_code = 9003, const char *error_msg = NULL) {
#ifdef DEBUG
    LOG_DEBUG("Handler::refuse_login in transport mode\n");
#endif
    this->do_refuse_login(error_code, error_msg);
    if (get_session()->get_stream()) {
      get_session()->get_stream()->close();
    }
  }

  void handle_request() {
#ifdef DEBUG
    LOG_DEBUG("Handler::handle_request enter\n");
#endif
    this->do_handle_request();
#ifdef DEBUG
    LOG_DEBUG("Handler::handle_request leave\n");
#endif
  }

  void handle_server_result() {
#ifdef DEBUG
    LOG_DEBUG("Handler::handle_server_result enter\n");
    ACE_ASSERT(get_session()->get_session_state() ==
               SESSION_STATE_HANDLING_RESULT);
#endif
    this->do_handle_server_result();
#ifdef DEBUG
    LOG_DEBUG("Handler::handle_server_result leave\n");
#endif
  }

  Statement *create_statement(const char *sql) {
    return this->do_create_statement(sql);
  }

  void handle_exception(exception *e, bool need_error_to_client = false,
                        uint16_t error_code = 0, const char *message = NULL,
                        const char *sql_state = "HY000");
  void handle_exception_just_send_to_client(uint16_t error_code = 0,
                                            const char *message = NULL,
                                            const char *sql_state = "HY000");

  void receive_from_client(Packet *packet);

  bool handler_wait_timeout() { return do_handler_wait_timeout(); }

  bool handler_login_timeout() { return do_handler_login_timeout(); }

  /* send_to_client_by_buffer should only be used for send_node and
   * direct_node.*/
  void send_to_client_by_buffer(Packet *packet);
  /* if send_to_client_by_buffer is used, invoker should invoke
   * flush_net_buffer after the execution of stmt.*/
  void flush_net_buffer();

  void send_to_client(Packet *packet, bool need_set_packet_number = true,
                      bool is_buffer_packet = false) {
    if (is_signal_handler || get_session()->get_is_silence_ok_stmt()) return;
    try {
      if (be_killed()) throw ThreadIsKilled();
      do_send_to_client(packet, need_set_packet_number, is_buffer_packet);
    } catch (exception &e) {
      get_session()->get_status()->item_inc(TIMES_CLIENT_BROKEN_NOT_USING_QUIT);
      LOG_ERROR("Fail to send to client due to [%s].\n", e.what());
      throw ClientBroken();
    }
  }

  void send_to_server(Connection *conn, Packet *packet) {
    try {
      do_send_to_server(conn, packet);
    } catch (exception &e) {
      LOG_ERROR("Fail to send to server due to [%s].\n", e.what());
      get_session()->set_server_shutdown(true);
      throw ServerSendFail();
    }
  }

  void kill_timeout_connection(Connection *conn, Packet *packet,
                               bool is_kill_query, const TimeValue *timeout);
  void receive_from_server(Connection *conn, Packet *packet,
                           const TimeValue *timeout = NULL);
  void put_back_connection(DataSpace *space, Connection *conn,
                           bool skip_keep_conn = false);
  void exec_xa_rollback_for_free_conn(DataSpace *space, Connection *conn);

  void clean_dead_conn(Connection **conn, DataSpace *space,
                       bool need_remove_kept_conn = true) {
    this->do_clean_dead_conn(conn, space, need_remove_kept_conn);
  }

  /*Different engine may have different logic for transaction, lock and
   * prepare statement. So there is no a common logic to adjust whether to
   * keep connection or not.*/
  bool check_keep_conn_beforehand(Statement *stmt) {
    return this->do_check_keep_conn_beforehand(stmt, get_session());
  }

  bool need_to_keep_conn(Statement *stmt) {
    return this->do_need_to_keep_conn(stmt, get_session());
  }

  int64_t get_part_table_next_insert_id(const char *schema_name,
                                        const char *table_name,
                                        PartitionedTable *part_space) {
    return this->do_get_part_table_next_insert_id(schema_name, table_name,
                                                  part_space);
  }

  void get_table_column_count_name(const char *schema_name,
                                   const char *table_name,
                                   PartitionedTable *part_space) {
    this->do_get_table_column_count_name(schema_name, table_name, part_space);
  }

  bool set_auto_inc_info(const char *schema_name, const char *table_name,
                         DataSpace *dataspace, bool is_partition_table) {
    return this->do_set_auto_inc_info(schema_name, table_name, dataspace,
                                      is_partition_table);
  }

  uint64_t get_last_insert_id_from_server(DataSpace *dataspace,
                                          Connection **conn, uint64_t num = 0,
                                          bool set_num = false) {
    return this->do_get_last_insert_id_from_server(dataspace, conn, num,
                                                   set_num);
  }

  void deal_with_transaction(const char *sql) {
    this->do_deal_with_transaction(sql);
  }

  void deal_with_transaction_error(stmt_type type) {
    this->do_deal_with_transaction_error(type);
  }

  void deal_with_set(Statement *stmt) {
    this->do_deal_with_set(stmt, get_session());
  }

  bool parse_transparent_mode_change_user(Connection *cur_conn,
                                          Session *cur_session);

  /* For one transaction, we only do the error retry if "send to server" fails
   * at first time, for all other case ("send to client", "receive from
   * client", "receive from server", "send to server at the second time or
   * more") we will send an error to client if possible.*/
  Connection *send_to_server_retry(DataSpace *space, Packet *packet,
                                   const char *schema = NULL,
                                   bool read_only = false,
                                   bool try_merge_begin = true,
                                   bool need_recv_begin = true,
                                   bool skip_keep_conn = false,
                                   bool off_sql_log_bin = false);
  void try_kill_xa_start_conn(Connection *conn);
  bool execute_begin(DataSpace *space, Connection *conn, Packet *packet,
                     bool try_merge_begin, Packet *new_packet = NULL);

  void rebuild_query_merge_sql(Packet *packet, DataSpace *space, string &sql,
                               Packet *new_packet = NULL) {
    do_rebuild_query_merge_sql(packet, space, sql, new_packet);
  }

  Packet *get_error_packet(uint16_t error_code, const char *message,
                           const char *sql_state) {
    return do_get_error_packet(error_code, message, sql_state);
  }

  void set_keep_conn(bool b) { get_session()->set_keep_conn(b); }
  bool is_keep_conn() { return get_session()->get_keep_conn(); }

  virtual size_t get_packet_head_size() { return 0; }
  virtual void set_names_charset_in_session(Session *session,
                                            const string &charset) {
    ACE_UNUSED_ARG(session);
    ACE_UNUSED_ARG(charset);
  }

  virtual void fin_alter_table(stmt_type type, stmt_node *st) {
    ACE_UNUSED_ARG(type);
    ACE_UNUSED_ARG(st);
  }

  virtual void clean_tmp_table_in_transaction(CrossNodeJoinTable *tmp_table) {
    ACE_UNUSED_ARG(tmp_table);
  }

  void remove_session() { this->do_remove_session(); }

  int connection_init_db(Connection **conn, const char *schema,
                         DataSpace *dataspace);
  void set_user_var(Connection **conn, DataSpace *dataspace);

  uint32_t get_thread_id() const { return thread_id; }
  void set_thread_id(uint32_t id) {
    get_session()->set_thread_id(id);
    thread_id = id;
  }

  bool be_killed() { return get_session()->get_is_killed(); }
  void set_killed(bool b) { get_session()->set_is_killed(b); }

  bool is_in_killing() { return get_session()->get_in_killing(); }
  void set_in_killing(bool b) { get_session()->set_in_killing(b); }

  bool need_update_option() { return get_session()->need_update_option(); }
  void set_update_option(bool b) { get_session()->set_update_option(b); }

  void set_pool(BasicThreadPool<Handler> *p) { pool = p; }
  BasicThreadPool<Handler> *get_pool() { return pool; }

  void record_affected_rows(Packet *packet) { do_record_affected_rows(packet); }

  const char *get_client_scramble() {
    return (const char *)(get_session()->get_client_scramble());
  }
  const char *get_server_scramble() {
    return get_session()->get_server_scramble().c_str();
  }
  string get_user_host() { return get_session()->get_user_host(); }

  virtual void *get_execute_plan(Statement *stmt) {
    ACE_UNUSED_ARG(stmt);
    throw NotImplementedError();
  }

  SocketEventBase *get_base() { return base; }

  void set_base(SocketEventBase *b) { base = b; }

  bool recycle_working_handler();

  void enable_ready_session_to_handler(Session *s);

  bool work_as_event_base_listener();

  stringstream *get_convert_result_to_binary_stringstream() {
    return &convert_result_to_binary_stringstream;
  }

 protected:
  TimeValue wait_timeout_tv;
  TimeValue exiting_timeout_tv;
  BasicThreadPool<Handler> *pool;
  unsigned long timestamp;

  void send_message_to_handler(char message) {
    ACE_Message_Block *mb;
    ACE_NEW(mb, ACE_Message_Block(1));
    char *pos = mb->wr_ptr();
    *(pos) = message;
    this->putq(mb);
#ifdef DEBUG
    LOG_DEBUG("Send message %c to handler thread.\n", message);
#endif
  }

 private:
  // ACE_Acceptor requires this class to be instancable, so we cannot use pure
  // abstract functions here, we have define all the functions that will throw
  // NotImplementedError instead.
  virtual void do_begin_session() { throw NotImplementedError(); }
  virtual void do_end_session() { throw NotImplementedError(); }
  virtual Session *do_get_session() { throw NotImplementedError(); }
  virtual void do_set_session(Session *s) {
    ACE_UNUSED_ARG(s);
    throw NotImplementedError();
  }
  virtual MigrateDataTool *do_get_insert_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    ACE_UNUSED_ARG(source_space);
    ACE_UNUSED_ARG(target_source);
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(source_schema_name);
    ACE_UNUSED_ARG(source_table_name);
    ACE_UNUSED_ARG(target_schema_name);
    ACE_UNUSED_ARG(target_table_name);
    throw NotImplementedError();
  }
  virtual MigrateDataTool *do_get_physical_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    ACE_UNUSED_ARG(source_space);
    ACE_UNUSED_ARG(target_source);
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(source_schema_name);
    ACE_UNUSED_ARG(source_table_name);
    ACE_UNUSED_ARG(target_schema_name);
    ACE_UNUSED_ARG(target_table_name);
    throw NotImplementedError();
  }
  virtual MigrateDataTool *do_get_load_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    ACE_UNUSED_ARG(source_space);
    ACE_UNUSED_ARG(target_source);
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(source_schema_name);
    ACE_UNUSED_ARG(source_table_name);
    ACE_UNUSED_ARG(target_schema_name);
    ACE_UNUSED_ARG(target_table_name);
    throw NotImplementedError();
  }
  virtual MigrateCleanTool *do_get_migrate_physical_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    ACE_UNUSED_ARG(source);
    ACE_UNUSED_ARG(type);
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(clean_schema_name);
    ACE_UNUSED_ARG(table_name);
    ACE_UNUSED_ARG(vid_set);
    ACE_UNUSED_ARG(source_name);
    ACE_UNUSED_ARG(target_source_name);
    throw NotImplementedError();
  }
  virtual MigrateCleanTool *do_get_migrate_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    ACE_UNUSED_ARG(source);
    ACE_UNUSED_ARG(type);
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(clean_schema_name);
    ACE_UNUSED_ARG(table_name);
    ACE_UNUSED_ARG(vid_set);
    ACE_UNUSED_ARG(source_name);
    ACE_UNUSED_ARG(target_source_name);
    throw NotImplementedError();
  }
  virtual MigrateHandlerTask *do_get_migrate_handler_task(string sql,
                                                          string schema_name) {
    ACE_UNUSED_ARG(sql);
    ACE_UNUSED_ARG(schema_name);
    throw NotImplementedError();
  }

  virtual void do_authenticate() { throw NotImplementedError(); }
  virtual void do_refuse_login(uint16_t error_code, const char *error_msg) {
    ACE_UNUSED_ARG(error_code);
    ACE_UNUSED_ARG(error_msg);
    throw NotImplementedError();
  }
  virtual void do_handle_request() { throw NotImplementedError(); }
  virtual void do_handle_server_result() { throw NotImplementedError(); }
  virtual Statement *do_create_statement(const char *sql) {
    ACE_UNUSED_ARG(sql);
    throw NotImplementedError();
  }
  virtual void do_receive_from_client(Packet *packet) {
    ACE_UNUSED_ARG(packet);
    throw NotImplementedError();
  }
  virtual void do_send_to_client(Packet *packet,
                                 bool need_set_packet_number = true,
                                 bool is_buffer_packet = false) {
    ACE_UNUSED_ARG(packet);
    ACE_UNUSED_ARG(need_set_packet_number);
    ACE_UNUSED_ARG(is_buffer_packet);
    throw NotImplementedError();
  }
  virtual void do_record_affected_rows(Packet *packet) {
    ACE_UNUSED_ARG(packet);
    throw NotImplementedError();
  }
  virtual void do_receive_from_server(Connection *conn, Packet *packet,
                                      const TimeValue *tv = NULL) {
    ACE_UNUSED_ARG(conn);
    ACE_UNUSED_ARG(packet);
    ACE_UNUSED_ARG(tv);
    throw NotImplementedError();
  }
  virtual void do_send_to_server(Connection *conn, Packet *packet) {
    ACE_UNUSED_ARG(conn);
    ACE_UNUSED_ARG(packet);
    throw NotImplementedError();
  }
  virtual Packet *do_get_error_packet(uint16_t error_code, const char *message,
                                      const char *sql_state) {
    ACE_UNUSED_ARG(error_code);
    ACE_UNUSED_ARG(message);
    ACE_UNUSED_ARG(sql_state);
    throw NotImplementedError();
  }
  virtual bool do_need_to_keep_conn(Statement *stmt, Session *s) {
    ACE_UNUSED_ARG(s);
    ACE_UNUSED_ARG(stmt);
    return false;
  }
  virtual void do_clean_dead_conn(Connection **conn, DataSpace *space,
                                  bool remove_kept_conn) {
    ACE_UNUSED_ARG(conn);
    ACE_UNUSED_ARG(space);
    ACE_UNUSED_ARG(remove_kept_conn);
    throw NotImplementedError();
  }
  virtual bool do_handler_wait_timeout() { throw NotImplementedError(); }
  virtual bool do_handler_login_timeout() { throw NotImplementedError(); }
  virtual bool do_check_keep_conn_beforehand(Statement *stmt, Session *s) {
    ACE_UNUSED_ARG(s);
    ACE_UNUSED_ARG(stmt);
    return false;
  }
  virtual void do_deal_with_transaction(const char *sql) {
    ACE_UNUSED_ARG(sql);
    throw NotImplementedError();
  }
  virtual void do_deal_with_transaction_error(stmt_type type) {
    ACE_UNUSED_ARG(type);
    throw NotImplementedError();
  }
  virtual void do_deal_with_set(Statement *stmt, Session *s) {
    ACE_UNUSED_ARG(s);
    ACE_UNUSED_ARG(stmt);
    throw NotImplementedError();
  }

  virtual int64_t do_get_part_table_next_insert_id(
      const char *schema_name, const char *table_name,
      PartitionedTable *part_space) {
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(table_name);
    ACE_UNUSED_ARG(part_space);
    return -1;
  }

  virtual void do_get_table_column_count_name(const char *schema_name,
                                              const char *table_name,
                                              PartitionedTable *part_space) {
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(table_name);
    ACE_UNUSED_ARG(part_space);
  }

  virtual bool do_set_auto_inc_info(const char *schema_name,
                                    const char *table_name,
                                    DataSpace *dataspace,
                                    bool is_partition_table) {
    ACE_UNUSED_ARG(schema_name);
    ACE_UNUSED_ARG(table_name);
    ACE_UNUSED_ARG(dataspace);
    ACE_UNUSED_ARG(is_partition_table);
    return false;
  }

  virtual uint64_t do_get_last_insert_id_from_server(DataSpace *dataspace,
                                                     Connection **conn,
                                                     uint64_t num,
                                                     bool set_num) {
    ACE_UNUSED_ARG(dataspace);
    ACE_UNUSED_ARG(conn);
    ACE_UNUSED_ARG(num);
    ACE_UNUSED_ARG(set_num);
    return 0;
  }

  virtual void do_set_last_insert_id(int64_t val) { ACE_UNUSED_ARG(val); }

  virtual int64_t do_get_last_insert_id() { return 0; }

  virtual void do_remove_session() {}

  virtual void do_re_init_handler() {}

  virtual void do_rebuild_query_merge_sql(Packet *packet, DataSpace *space,
                                          string &sql, Packet *new_packet) {
    ACE_UNUSED_ARG(packet);
    ACE_UNUSED_ARG(space);
    ACE_UNUSED_ARG(sql);
    ACE_UNUSED_ARG(new_packet);
  }

  /*return 0 is OK*/
  int check_trans_conn_status() {
    bool client_broken = get_session()->get_client_broken();
    bool server_broken = get_session()->get_server_broken();
    if (!client_broken && !server_broken) {
      return 0;
    } else {
      return -1;
    }
  }

 protected:
  Backend *backend;
  Stream *stream;
  Packet *net_buffer;
  unsigned long buffer_len;
  unsigned long buffer_size;
  uint32_t thread_id;
  bool is_being_release;
  SocketEventBase *base;
  map<string, ACE_Time_Value> monitor_point_map;
  map<string, ACE_Time_Value> global_monitor_point_map;
  map<string, unsigned long> num_of_executed;
  map<unsigned long, unsigned long> histogram_monitor_point;
  // This stringstream is used in this handler thread
  // to avoid many construct and deconstruct
  stringstream convert_result_to_binary_stringstream;

 public:
  bool need_monitor;
  unsigned long num_of_send;
  unsigned long num_of_recv;
  ACE_Thread_Mutex handler_monitor_point_mutex;

 protected:
  void check_and_record_slow_query_time();
  virtual void record_audit_log(stmt_type type, bool force = false) {
    ACE_UNUSED_ARG(type);
    ACE_UNUSED_ARG(force);
  }
  ACE_Time_Value
      start_date;  // used only when need record slow query or audit log
  ACE_Time_Value end_date;  // used only when need record slow query or audit
                            // log
  bool is_signal_handler;
  bool is_migrate_async_handler;

 private:
  void log_slow_query(ACE_Time_Value &_end_date, unsigned long _cost_time);

 public:
  unsigned long get_cost_time() const {
    return (end_date - start_date).msec();
  }  // used only when need record audit log

  virtual void deal_autocommit_with_ok_eof_packet(Packet *packet) {
    ACE_UNUSED_ARG(packet);
  }

  void start_transaction_timing() {
    LOG_DEBUG("Handler %@ start_transaction_timing\n", this);
    transaction_time_handler->start_schedule_timer();
  }

  void end_transaction_timing() {
    LOG_DEBUG("Handler %@ end_transaction_timing\n", this);
    transaction_time_handler->stop_schedule_timer();
  }

  void set_trx_timing_stop(bool stop) { trx_timing_stop = stop; }

  bool get_trx_timing_stop() { return trx_timing_stop; }

  ACE_Thread_Mutex *get_handler_trx_timing_stop_mutex() {
    return &handler_trx_timing_stop_mutex;
  }

  void set_mul_stmt_send_error_packet(bool is_error) {
    mul_stmt_send_error_packet = is_error;
  }

 protected:
  MonitorPointSQLStatistic sql_statistic;
  SQLOutlineMonitorInfoMap sql_outline_monitor_info_map;
  ACE_Thread_Mutex sql_outline_monitor_info_map_lock;
  TransactionTimerHandler *transaction_time_handler;
  ACE_Thread_Mutex handler_trx_timing_stop_mutex;
  bool trx_timing_stop;
  bool mul_stmt_send_error_packet;

#ifndef DBSCALE_TEST_DISABLE
 public:
  HandlerMonitorPoint handler_svc;
  HandlerMonitorPoint handler_as_listener;
  HandlerMonitorPoint handler_recy;
  HandlerMonitorPoint handler_swap;
  HandlerMonitorPoint parse_sql;
  HandlerMonitorPoint gen_plan;
  HandlerMonitorPoint exec_send;
  HandlerMonitorPoint exec_recv;
  HandlerMonitorPoint send_op;
  HandlerMonitorPoint prepare_gen_plan;
  HandlerMonitorPoint handler_last_send;
  HandlerMonitorPoint handler_recv_from_client;

  HandlerMonitorPoint after_exec_recv;
  HandlerMonitorPoint after_handle_recv;
#endif
};

}  // namespace dbscale

#endif /* DBSCALE_HANDLER_H */
