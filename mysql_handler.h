#ifndef DBSCALE_MYSQL_HANDLER_H
#define DBSCALE_MYSQL_HANDLER_H

/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <exception.h>
#include <handler.h>
#include <log.h>
#include <packet.h>

#include <boost/algorithm/string.hpp>

#include "mysql_comm.h"
#include "mysql_compress.h"
#include "mysql_migrate_util.h"
#include "mysql_password.h"
#include "mysql_request.h"
#include "mysql_session.h"
#include "mysql_statement.h"

using namespace dbscale;
using namespace dbscale::sql;
using namespace dbscale::mysql;

#ifndef DBSCALE_TEST_DISABLE
extern bool test_receive_server_slow;
extern bool test_receive_data_failed;
#endif

namespace dbscale {

class InternalEventBase;
namespace mysql {

class MySQLDriver;

typedef struct HandShakePacket {
  unsigned char protocol_version;  // select @@protocol_version;
  string server_version;           // select version();
  uint32_t thread_id;              // driver->get_next_global_thread_id();
  char server_scramble[256];       //先直接用获取的
  uint32_t server_capabilities;    //先直接用获取的
  // uint8_t scramble_len;
  unsigned char server_charset;  //后端默认就是8
  uint16_t server_status;        //暂时就给2
  string auth_plugin_name;       // native_password
} HandShakePacket;

class MySQLHandler : public Handler {
 public:
  MySQLHandler();
  ~MySQLHandler() {}

  void send_mysql_packet_to_client_by_buffer(Packet *packet,
                                             bool is_row_packet = false);
  bool get_databases(Connection *conn, bool is_super_user);
  void init_sessionvar(Connection *conn);
  void fin_alter_table(stmt_type type, stmt_node *st);

  void *get_execute_plan(Statement *stmt);

  int execute_replace_str(string &query, list<execute_param> *param_list);
  int execute_replace_str(MySQLPrepareItem *prepare_item, string &query_sql,
                          list<execute_param> *param_list);
  void handle_query_command(Packet *packet);
  int handle_query_command(const char *query_sql, size_t cmd_len);
  void deal_with_metadata_execute(stmt_type type, const char *sql,
                                  const char *schema, stmt_node *st);
  void deal_autocommit_with_ok_eof_packet(Packet *packet);
  void clean_tmp_table_in_transaction(CrossNodeJoinTable *tmp_table);

 protected:
  size_t get_packet_head_size() { return PACKET_HEADER_SIZE; }
  void record_audit_log(stmt_type type, bool force = false);

 private:
  string generate_ddl_comment_sql(const char *sql);
  void record_audit_login(uint32_t thread_id, string user, string host,
                          string ip, const char *schema, int error_code);
  string get_login_hostip();
  bool white_filter_check(string &client_ip, const char *user_name);
  void send_login_error_packet(const char *error_msg);
  bool white_filter_check(string &client_ip, const char *user_name,
                          ssl_option_struct &ssl_option_value);
  void do_authenticate();
  void do_authenticate_with_auth_pool_conn();
  bool login_check(const char *user, const char *host, const char *schema,
                   bool client_client_ssl);
  void structure_handshake_by_dbscale(Packet &packet, string &server_scramble,
                                      uint32_t &thread_id);
  void do_authenticate_check_by_dbscale(MySQLAuthRequest *auth, Packet &packet,
                                        string &server_scramble,
                                        const char *host);
  bool do_authenticate_check_password(const char *client_scramble,
                                      string server_scramble);
  void check_and_init_session_charset(Session *session);
  void set_names_charset_in_session(Session *session, const string &charset);
  bool handle_authenticate_succeed(const char *user, const char *host,
                                   const char *schema_name,
                                   uint32_t client_flags, Connection *conn);
  void recover_replication_info_in_slave_dbscale_mode();
  void handle_auth_switch_request(MySQLAuthSwitchRequest &auth_switch_request);
  void handle_auth_switch_response(Packet &packet);
  void handle_auth_switch_response(
      MySQLAuthSwitchResponse &auth_switch_response);
  void do_begin_session();
  void do_end_session();
  Session *do_get_session() { return s; }
  void do_set_session(Session *s) {
    this->s = s;
    session = (MySQLSession *)s;
  }
  MigrateDataTool *do_get_physical_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return new PhysicalMigrateDataTool(
        this, source_space, target_source, schema_name.c_str(),
        source_schema_name.c_str(), source_table_name.c_str(),
        target_schema_name.c_str(), target_table_name.c_str());
  }
  MigrateDataTool *do_get_insert_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return new InsertSelectMigrateDataTool(
        this, source_space, target_source, schema_name.c_str(),
        source_schema_name.c_str(), source_table_name.c_str(),
        target_schema_name.c_str(), target_table_name.c_str());
  }
  MigrateDataTool *do_get_load_select_migrate_tool(
      DataSpace *source_space, DataSource *target_source, string schema_name,
      string source_schema_name, string source_table_name,
      string target_schema_name, string target_table_name) {
    return new LoadSelectMigrateDataTool(
        this, source_space, target_source, schema_name.c_str(),
        source_schema_name.c_str(), source_table_name.c_str(),
        target_schema_name.c_str(), target_table_name.c_str());
  }
  MigrateCleanTool *do_get_migrate_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    return new DeleteSelectMigrateCleanTool(
        this, source, type, schema_name, clean_schema_name, table_name, vid_set,
        source_name, target_source_name);
  }

  MigrateCleanTool *do_get_migrate_physical_clean_tool(
      DataSource *source, migrate_type type, const char *schema_name,
      const char *clean_schema_name, const char *table_name,
      set<unsigned int> &vid_set, string source_name,
      string target_source_name) {
    return new PhysicalMigrateCleanTool(this, source, type, schema_name,
                                        clean_schema_name, table_name, vid_set,
                                        source_name, target_source_name);
  }

  MigrateHandlerTask *do_get_migrate_handler_task(string sql,
                                                  string schema_name) {
    return new MySQLMigrateHandlerTask(sql, schema_name);
  }

  void do_handle_request();
  void do_refuse_login(uint16_t error_code = 9003,
                       const char *error_msg = NULL);
  void do_handle_server_result();
  void receive_from_client_compress(Packet *packet);
  void receive_from_client_swap(Packet *packet);
  void receive_from_client_no_swap(Packet *packet);
  void wait_socket_read(InternalEventBase *internal_base);
  void wait_client_read();
  void do_receive_from_client(Packet *packet);
  bool do_need_to_keep_conn(Statement *stmt, Session *s);
  bool do_check_keep_conn_beforehand(Statement *stmt, Session *s);
  void do_send_to_client(Packet *packet, bool need_set_packet_number = true,
                         bool is_buffer_packet = false);
  void do_receive_from_server(Connection *conn, Packet *packet,
                              const TimeValue *timeout = NULL);

  void do_send_to_server(Connection *conn, Packet *packet);
  void do_deal_with_transaction(const char *sql);
  void do_deal_with_transaction_error(stmt_type type);
  void do_deal_with_set(Statement *stmt, Session *s);
  void do_re_init_handler();
  virtual Packet *do_get_error_packet(uint16_t error_code, const char *message,
                                      const char *sql_state);

  int64_t do_get_part_table_next_insert_id(const char *schema_name,
                                           const char *table_name,
                                           PartitionedTable *part_space);

  void do_get_table_column_count_name(const char *schema_name,
                                      const char *table_name,
                                      PartitionedTable *part_space);

  bool do_set_auto_inc_info(const char *schema_name, const char *table_name,
                            DataSpace *dataspace, bool is_partition_table);

  uint64_t do_get_last_insert_id_from_server(DataSpace *dataspace,
                                             Connection **conn, uint64_t num,
                                             bool set_num);
  Statement *do_create_statement(const char *sql) {
    LOG_DEBUG("MySQLHandler::do_create_statement\n");
    return new MySQLStatement(sql);
  }

  void do_rebuild_query_merge_sql(Packet *packet, DataSpace *space, string &sql,
                                  Packet *new_packet);

  bool has_more_results() { return (phase_direct(phase) == SERVER_TO_CLIENT); }

  Phase calc_next_phase(const char *base, Phase phase);

  bool is_ok_packet(const char *packet) {
    return static_cast<unsigned char>(packet[4]) == 0;
  }

  void deal_with_metadata_final(stmt_type type, const char *sql,
                                const char *schema, stmt_node *st);

  bool is_eof_packet(const char *packet) {
    return static_cast<unsigned char>(packet[4]) == 0xfe;
  }

  void do_record_affected_rows(Packet *packet);

  bool ok_packet_server_status(const char *packet) {
    const char *p = packet;
    p += 4;
    p += 1;
    Packet::unpack_lenenc_int(&p);
    Packet::unpack_lenenc_int(&p);
    return Packet::unpack2uint(p);
  }

  bool eof_packet_server_status(const char *packet) {
    return Packet::unpack2uint(packet + 4 + 1 + 2);
  }

  bool more_results_exists(const char *packet) {
    uint16_t status = 0;
    if (is_ok_packet(packet))
      status = ok_packet_server_status(packet);
    else
      status = eof_packet_server_status(packet);
    return (status & SERVER_MORE_RESULTS_EXISTS);
  }

  bool is_field_list_command(const char *packet) {
    return is_command(packet, MYSQL_COM_FIELD_LIST);
  }
  // this function is only used in need_keep_conn(),
  // so, we don't need to modify it for in cluster xa transaction
  bool is_autocommit_on() {
    // if return true, it means autocommit = 1
    // else autocommit = 0
    return !s->check_for_transaction();
  }
  bool is_command(const char *packet, MySQLCommand command) {
    return static_cast<MySQLCommand>(packet[4]) == command;
  }

  bool is_quit_command(const char *packet) {
    return is_command(packet, MYSQL_COM_QUIT);
  }
  bool is_on(string value) {
    if (!strcmp(value.c_str(), "ON") || !strcmp(value.c_str(), "'ON'") ||
        !strcmp(value.c_str(), "'TRUE'") || !strcmp(value.c_str(), "1")) {
      return true;
    } else {
      return false;
    }
  }
  void forecast_keep_conn_state(Statement *stmt, Session *s, bool *lock_state,
                                bool *transaction_state, bool *auto_commit,
                                bool *global_flush_read_lock_state);

  void do_remove_session();

  void do_clean_dead_conn(Connection **conn, DataSpace *space,
                          bool remove_kept_conn) {
    if (*conn) {
      Session *s = get_session();
      if (remove_kept_conn) s->remove_kept_connection(space, true);
      s->remove_using_conn(*conn);
      if (support_show_warning && remove_kept_conn)
        s->clear_dataspace_warning(space, false);
      s->remove_prepare_read_connection(*conn);
      s->remove_temp_table_set_by_conn(*conn);
      s->remove_defined_lock_kept_conn(space);
      (*conn)->reset_cluster_xa_session_before_to_dead();
      (*conn)->get_pool()->add_back_to_dead(*conn);
      *conn = NULL;
    }
  }
  bool do_handler_wait_timeout() {
    get_session()->flush_session_sql_cache(true);
    if (wait_timeout == 0) return false;
    if (Backend::instance()->pattern_match_ip(session->get_client_ip()))
      return false;
    if (wait_timeout > session->get_idle_time()) return false;
    if (session->get_is_super_user())  // the super user root/dbscale_internal
                                       // will not wait_timeout
      return false;
    LOG_INFO("wait_timeout=[%d], idle_time=[%d]\n", wait_timeout,
             session->get_idle_time());
    return true;
  }

  bool do_handler_login_timeout() {
    if (max_login_time == 0 || !session->is_do_logining()) return false;
    if (max_login_time > session->get_cur_login_time()) return false;
    LOG_WARN("max_login_time=[%d], login_time=[%d]\n", max_login_time,
             session->get_cur_login_time());
    return true;
  }

  void unpack_param_value(char **pos, MySQLColumnType type, string &s,
                          const char *charset);

  void handle_prepare_command(Packet *packet);
  void handle_execute_command(Packet *packet);
  void handle_close_command(Packet *packet);
  void handle_long_data_command(Packet *packet);
  void handle_reset_stmt_command(Packet *packet);
  void prepare_direct_receive_from_server(Connection *prepare_conn,
                                          string &query_sql,
                                          uint32_t &global_stmt_id,
                                          uint32_t &backend_stmt_id,
                                          stmt_type &st_type, bool is_gsid);
  void prepare_deal_error_packet(Packet *packet, bool is_print = true);

  bool is_blocking_stmt(MySQLStatement *stmt);
  bool is_federated_follower(MySQLStatement *stmt);
  bool is_dbscale_show_stmt(stmt_type type);

 private:
  MySQLRequest *recv_request();
  bool handle_packet_before_send(Packet **packet);
  bool get_meta_for_com_prepare(const char *sql,
                                prepare_stmt_group_handle_meta &meta,
                                list<int> &param_pos,
                                list<string> &split_prepare_sql,
                                vector<string> &all_table_name,
                                stmt_type &st_type, size_t &mem_size);
  bool is_should_use_txn_or_throw(stmt_type type);

 private:
  MySQLDriver *driver;
  MySQLSession *session;
  Phase phase;
  Session *s;
  char execute_param_str_value[100];
  string cur_hint_sql;

 private:
  void try_conceal_dbscale_unsupported_flags_for_server(MySQLInitResponse &init,
                                                        uint32_t flag,
                                                        const char *flag_name);
  void try_conceal_dbscale_unsupported_flags_for_client(MySQLAuthRequest &auth,
                                                        uint32_t flag,
                                                        const char *flag_name);
  void set_auto_commit_off_if_necessary(Packet *packet);
  void record_outline_info(const char *sql, size_t sql_len);

  bool check_auto_inc_for_temp_table(const char *schema_name,
                                     const char *table_name,
                                     DataSpace *dataspace);
  void progress_explain_head_before_attach_hint(string &remove_explain_sql,
                                                MySQLStatement *&stmt_pt);
  bool do_prepare_attach_index_hint(const char *query_sql,
                                    const outline_hint_item &oh_item,
                                    vector<pair<int, string>> &hint_pos_value,
                                    MySQLStatement *&stmt_pt);
  bool sql_attach_outline_hint(const char *query_sql);
  void get_plainpwd_by_dbscale_RSAkey();
  void do_fake_full_auth_get_plainpwd();
  void do_real_full_auth_get_plainpwd(MySQLConnection *conn, Packet *packet);
};

}  // namespace mysql
}  // namespace dbscale

#endif /* DBSCALE_MYSQL_HANDLER_H */
