#ifndef DBSCALE_MYSQL_PLAN_H
#define DBSCALE_MYSQL_PLAN_H

/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/Synch.h>
#include <backend_thread.h>
#include <connection.h>
#include <exception.h>
#include <expression.h>
#include <plan.h>
#include <remote_execute.h>

#include <bitset>
#include <fstream>
#include <iostream>
#include <string>

#include "ace/Condition_T.h"
#include "boost/unordered_map.hpp"
#include "mysql_comm.h"
#include "mysql_compare.h"
#include "mysql_driver.h"

#ifndef CLOSE_ZEROMQ
#include <mysql_binlog_message.h>
#endif
#define MAX_FETCH_NODE_RECEIVE 10
#define LOAD_CLIENT_PACKET_SIZE 16384  // 16k
#define DEFAULT_BUFFER_SIZE 65536      // 64K
#define DEFAULT_LOAD_PACKET_SIZE (16 * 1024L)
#define MAX_CONNECT_BY_CACHED_ROWS (1024 * 1024 * 32)  // bitset 4M
#define DIR_SEPERATOR '/'
#define LOGIN_SELECT_VERSION_COMMENT "select @@version_comment limit 1"
#define LOGIN_SELECT_VERSION_COMMENT_LEN 32

using namespace dbscale;
using namespace dbscale::sql;
using namespace dbscale::plan;
namespace dbscale {
namespace mysql {

enum XACommand {
  XA_ROLLBACK,
  XA_ROLLBACK_AFTER_PREPARE,
  XA_PREPARE,
  XA_COMMIT
};

enum BackendThreadStatus { THREAD_STOP, THREAD_CREATED, THREAD_STARTED };

enum XASwapState {
  XA_INIT_STATE,
  XA_FIRST_PHASE_SEND_STATE,
  XA_FIRST_PHASE_RECV_STATE,
  XA_SECOND_PHASE_SEND_STATE,
  XA_SECOND_PHASE_RECV_STATE,
  XA_END_STATE
};

void pack_header(Packet *packet, size_t load_length);
void found_rows(MySQLHandler *handler, MySQLDriver *driver, Connection *conn,
                MySQLSession *session);
void send_ok_packet_to_client(Handler *handler, uint64_t affected_rows,
                              uint16_t warnings);
void record_migrate_error_message(
    ExecutePlan *plan, Packet *packet, string msg,
    bool server_innodb_rollback_on_timeout = false);
void store_warning_packet(Connection *conn, Handler *handler,
                          MySQLDriver *driver,
                          vector<Packet *> **warning_packet_list,
                          bool &has_add_warning_packet,
                          uint64_t &warning_packet_list_size);
void handle_warnings_OK_and_eof_packet_inernal(MySQLDriver *driver,
                                               Packet *packet, Handler *handler,
                                               DataSpace *space,
                                               Connection *conn,
                                               bool skip_keep_conn = false);
size_t dirname_length(const char *name);
void convert_dirname(string &to, const char *from, const char *from_end);
class MySQLExecuteNode;
class MySQLExecutePlan : public ExecutePlan {
 public:
  MySQLExecutePlan(Statement *statement, Session *session, Driver *driver,
                   Handler *handler);

 private:
  ExecuteNode *do_get_query_one_column_node(DataSpace *dataspace,
                                            const char *sql, bool only_one_row);
  ExecuteNode *do_get_query_mul_column_node(DataSpace *dataspace,
                                            const char *sql);
  ExecuteNode *do_get_query_one_column_aggr_node(DataSpace *dataspace,
                                                 const char *sql, bool get_min);
  ExecuteNode *do_get_query_exists_node(DataSpace *dataspace, const char *sql);
  ExecuteNode *do_get_fetch_node(DataSpace *dataspace, const char *sql);
  ExecuteNode *do_get_navicat_profile_sql_node();
  ExecuteNode *do_get_send_node();
#ifndef DBSCALE_DISABLE_SPARK
  ExecuteNode *do_get_spark_node(SparkConfigParameter config, bool is_insert);
#endif
  ExecuteNode *do_get_dispatch_packet_node(TransferRule *rule);
  ExecuteNode *do_get_into_outfile_node();
  ExecuteNode *do_get_select_into_node();
  ExecuteNode *do_get_modify_limit_node(record_scan *rs,
                                        PartitionedTable *table,
                                        vector<unsigned int> &par_ids,
                                        unsigned int limit, string &sql);
  ExecuteNode *do_get_sort_node(list<SortDesc> *sort_desc);
  ExecuteNode *do_get_project_node(unsigned int skip_columns);
  ExecuteNode *do_get_direct_execute_node(DataSpace *dataspace,
                                          const char *sql);
  ExecuteNode *do_get_show_warning_node();
  ExecuteNode *do_get_show_load_warning_node();
  ExecuteNode *do_get_transaction_unlock_node(const char *sql,
                                              bool is_send_to_client);
  ExecuteNode *do_get_xatransaction_node(const char *sql,
                                         bool is_send_to_client);
  ExecuteNode *do_get_cluster_xatransaction_node();
  ExecuteNode *do_get_avg_node(list<AvgDesc> &avg_list);
  ExecuteNode *do_get_lock_node(
      map<DataSpace *, list<lock_table_item *> *> *lock_table_list);
  ExecuteNode *do_get_migrate_node();
  ExecuteNode *do_get_limit_node(long offset, long num);
  ExecuteNode *do_get_ok_node();
  ExecuteNode *do_get_ok_merge_node();
  ExecuteNode *do_get_modify_node(DataSpace *dataspace, const char *sql,
                                  bool re_parse_shard);
  ExecuteNode *do_get_modify_select_node(const char *modify_sql,
                                         vector<string *> *columns,
                                         bool can_quick,
                                         vector<ExecuteNode *> *nodes,
                                         bool is_replace_set_value);
  ExecuteNode *do_get_insert_select_node(const char *modify_sql,
                                         vector<ExecuteNode *> *nodes);
  ExecuteNode *do_get_aggr_node(list<AggregateDesc> &aggregate_desc);
  ExecuteNode *do_get_group_node(list<SortDesc> *group_desc,
                                 list<AggregateDesc> &aggregate_desc);
  ExecuteNode *do_get_dbscale_wise_group_node(
      list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc);
  ExecuteNode *do_get_dbscale_pages_node(list<SortDesc> *group_desc,
                                         list<AggregateDesc> &aggregate_desc,
                                         int page_size);
  ExecuteNode *do_get_single_sort_node(list<SortDesc> *sort_desc);
  ExecuteNode *do_get_load_local_node(DataSpace *dataspace, const char *sql);

  ExecuteNode *do_get_load_local_external_node(DataSpace *dataspace,
                                               DataServer *dataserver,
                                               const char *sql);
  ExecuteNode *do_get_load_data_infile_external_node(DataSpace *dataspace,
                                                     DataServer *dataserver);
  ExecuteNode *do_get_kill_node(int cluster_id, uint32_t kid);
  ExecuteNode *do_get_load_local_part_table_node(PartitionedTable *dataspace,
                                                 const char *sql,
                                                 const char *schema_name,
                                                 const char *table_name);
  ExecuteNode *do_get_load_data_infile_part_table_node(
      PartitionedTable *dataspace, const char *sql, const char *schema_name,
      const char *table_name);
  ExecuteNode *do_get_load_data_infile_node(DataSpace *dataspace,
                                            const char *sql);
  ExecuteNode *do_get_load_select_node(const char *schema_name,
                                       const char *table_name);
  ExecuteNode *do_get_load_select_partition_node(const char *schema_name,
                                                 const char *table_name);
  ExecuteNode *do_get_distinct_node(list<int> column_indexes);
  ExecuteNode *do_get_show_partition_node();
  ExecuteNode *do_get_show_status_node();
  ExecuteNode *do_get_show_catchup_node(const char *source_name);
  ExecuteNode *do_get_dbscale_skip_wait_catchup_node(const char *source_name);
  ExecuteNode *do_get_show_async_task_node();
  ExecuteNode *do_get_clean_temp_table_cache_node();
  ExecuteNode *do_get_explain_node(bool is_server_explain_stmt_always_extended);
  ExecuteNode *do_get_fetch_explain_result_node(
      list<list<string> > *result, bool is_server_explain_stmt_always_extended);
  ExecuteNode *do_get_show_virtual_map_node(PartitionedTable *tab);
  ExecuteNode *do_get_show_shard_map_node(PartitionedTable *tab);
  ExecuteNode *do_get_show_auto_increment_info_node(PartitionedTable *tab);
  ExecuteNode *do_get_set_auto_increment_offset_node(PartitionedTable *tab);
  ExecuteNode *do_get_show_transaction_sqls_node();
  ExecuteNode *do_get_rows_node(list<list<string> *> *row_list);
  ExecuteNode *do_get_par_modify_select_node(const char *modify_sql,
                                             PartitionedTable *par_table,
                                             PartitionMethod *method,
                                             vector<string *> *columns,
                                             bool can_quick,
                                             vector<ExecuteNode *> *nodes);
  ExecuteNode *do_get_par_insert_select_node(
      const char *modify_sql, PartitionedTable *par_table,
      PartitionMethod *method, vector<unsigned int> &key_pos,
      vector<ExecuteNode *> *nodes, const char *schema_name,
      const char *table_name, bool is_duplicated);
  ExecuteNode *do_get_regex_filter_node(enum FilterFlag filter_way,
                                        int column_index,
                                        list<const char *> *pattern);
  ExecuteNode *do_get_dbscale_request_cluster_inc_info_node(
      PartitionedTable *space, const char *schema_name, const char *table_name);
  ExecuteNode *do_get_mul_modify_node(
      map<DataSpace *, const char *> &spaces_map,
      map<DataSpace *, int> *sql_count_map,
      bool need_handle_more_result = false);
  ExecuteNode *do_get_dbscale_cluster_shutdown_node(int cluster_id);
  ExecuteNode *do_get_dbscale_request_cluster_id_node();
  ExecuteNode *do_get_dbscale_request_all_cluster_inc_info_node();
  ExecuteNode *do_get_dbscale_flush_node(dbscale_flush_type type);
  ExecuteNode *do_get_dbscale_flush_weak_pwd_node();
  ExecuteNode *do_get_dbscale_request_cluster_info_node();
  ExecuteNode *do_get_dbscale_request_cluster_user_status_node(
      const char *id, bool only_show_running, bool show_status_count);
  ExecuteNode *do_get_dbscale_clean_temp_table_cache_node();
  ExecuteNode *do_get_dbscale_request_node_info_node();
  ExecuteNode *do_get_expr_filter_node(Expression *expr);
  ExecuteNode *do_get_expr_calculate_node(
      list<SelectExprDesc> &select_expr_list);
  ExecuteNode *do_get_estimate_select_node(DataSpace *dataspace,
                                           const char *sql,
                                           Statement *statement);
  ExecuteNode *do_get_select_plain_value_node(const char *str_value,
                                              const char *alias_name);
  ExecuteNode *do_get_estimate_select_partition_node(
      vector<unsigned int> *par_ids, PartitionedTable *par_table,
      const char *sql, Statement *statement);
  ExecuteNode *do_get_avg_show_table_status_node();
  ExecuteNode *do_get_show_user_memory_status_node();
  ExecuteNode *do_get_show_user_sql_count_node(const char *user_id);
  ExecuteNode *do_get_show_user_status_node(const char *user_id,
                                            const char *user_name,
                                            bool only_show_running,
                                            bool instance,
                                            bool show_status_count);
  ExecuteNode *do_get_show_user_processlist_node(const char *cluster_id,
                                                 const char *user_id,
                                                 int local);
  ExecuteNode *do_get_show_session_id_node(const char *server_name,
                                           int connection_id);
  ExecuteNode *do_get_backend_server_execute_node(const char *stmt_sql);
  ExecuteNode *do_get_execute_on_all_masterserver_execute_node(
      const char *stmt_sql);
  ExecuteNode *do_get_show_table_location_node(const char *schema_name,
                                               const char *table_name);
  ExecuteNode *do_get_dbscale_erase_auth_info_node(name_item *username_list,
                                                   bool all_dbscale_node);
  ExecuteNode *do_get_dynamic_update_white_node(
      bool is_add, const char *ip, const ssl_option_struct &ssl_option_value,
      const char *comment, const char *user_name);
  ExecuteNode *do_get_dynamic_add_slave_node(
      dynamic_add_slave_op_node *dynamic_add_slave_oper);
  ExecuteNode *do_get_dynamic_add_data_server_node(
      dynamic_add_data_server_op_node *dynamic_add_data_server_oper);
  ExecuteNode *do_get_dynamic_add_data_source_node(
      dynamic_add_data_source_op_node *dynamic_add_data_source_oper,
      DataSourceType type);
  ExecuteNode *do_get_dynamic_add_data_space_node(
      dynamic_add_data_space_op_node *dynamic_add_data_space_oper);
  ExecuteNode *do_get_set_schema_acl_node(
      set_schema_acl_op_node *set_schema_acl_oper);
  ExecuteNode *do_get_set_user_not_allow_operation_time_node(
      set_user_not_allow_operation_time_node *oper);
  ExecuteNode *do_get_reload_user_not_allow_operation_time_node(
      string user_name);
  ExecuteNode *do_get_show_user_not_allow_operation_time_node(string user_name);
  ExecuteNode *do_get_set_table_acl_node(
      set_table_acl_op_node *set_table_acl_oper);
  ExecuteNode *do_get_reset_tmp_table_node();
  ExecuteNode *do_get_reload_function_type_node();
  ExecuteNode *do_get_shutdown_node();
  ExecuteNode *do_get_dbscale_set_pool_info_node(pool_node *pool_info);
  ExecuteNode *do_get_dbscale_reset_info_plan(dbscale_reset_info_op_node *oper);
  ExecuteNode *do_get_dbscale_block_node();
  ExecuteNode *do_get_dbscale_disable_server_node();
  ExecuteNode *do_get_dbscale_check_table_node();
  ExecuteNode *do_get_dbscale_check_disk_io_node();
  ExecuteNode *do_get_dbscale_check_metadata();
  ExecuteNode *do_get_dbscale_flush_pool_info_node(pool_node *pool_info);
  ExecuteNode *do_get_dbscale_force_flashback_online_node(const char *name);
  ExecuteNode *do_get_dbscale_xa_recover_slave_dbscale_node(
      const char *xa_source, const char *top_source, const char *ka_update_v);
  ExecuteNode *do_get_dbscale_purge_connection_pool_node(pool_node *pool_info);
  ExecuteNode *do_get_dbscale_reload_config_node(const char *sql);
  ExecuteNode *do_get_dynamic_change_master_node(
      dynamic_change_master_op_node *dynamic_change_master_oper);
  ExecuteNode *do_get_dynamic_change_multiple_master_active_node(
      dynamic_change_multiple_master_active_op_node
          *dynamic_change_multiple_master_active_oper);
  ExecuteNode *do_get_dynamic_change_dataserver_ssh_node(
      const char *server_name, const char *username, const char *pwd, int port);
  ExecuteNode *do_get_dynamic_remove_slave_node(
      dynamic_remove_slave_op_node *dynamic_remove_slave_oper);
  ExecuteNode *do_get_dynamic_remove_schema_node(const char *schema_name,
                                                 bool is_force);
  ExecuteNode *do_get_dynamic_remove_table_node(const char *table_name,
                                                bool is_force);
  ExecuteNode *do_get_dynamic_change_table_scheme_node(const char *table_name,
                                                       const char *scheme_name);
  ExecuteNode *do_get_dynamic_remove_node(const char *name);
  ExecuteNode *do_get_show_monitor_point_status_node();
  ExecuteNode *do_get_show_global_monitor_point_status_node();
  ExecuteNode *do_get_show_histogram_monitor_point_status_node();
  ExecuteNode *do_get_show_outline_monitor_info_node();
  ExecuteNode *do_get_dbscale_create_outline_hint_node(
      dbscale_operate_outline_hint_node *oper);
  ExecuteNode *do_get_dbscale_flush_outline_hint_node(
      dbscale_operate_outline_hint_node *oper);
  ExecuteNode *do_get_dbscale_show_outline_hint_node(
      dbscale_operate_outline_hint_node *oper);
  ExecuteNode *do_get_dbscale_delete_outline_hint_node(
      dbscale_operate_outline_hint_node *oper);
  ExecuteNode *do_get_dbscale_global_consistence_point_node();
  ExecuteNode *do_get_show_pool_info_node();
  ExecuteNode *do_get_show_pool_version_node();
  ExecuteNode *do_get_com_query_prepare_node(DataSpace *dataspace,
                                             const char *query_sql,
                                             const char *name, const char *sql);
  ExecuteNode *do_get_com_query_exec_prepare_node(const char *name,
                                                  var_item *var_item_list);
  ExecuteNode *do_get_com_query_drop_prepare_node(DataSpace *dataspace,
                                                  const char *name,
                                                  const char *prepare_sql);
  ExecuteNode *do_get_dbscale_flush_config_to_file_node(const char *file_name,
                                                        bool flush_all);
  ExecuteNode *do_get_dbscale_flush_acl_node();
  ExecuteNode *do_get_dbscale_flush_table_info_node(const char *schema_name,
                                                    const char *table_name);
  ExecuteNode *do_get_show_backend_threads_node();
  ExecuteNode *do_get_show_lock_usage_node();
  ExecuteNode *do_get_show_engine_lock_waiting_node(int engine_type);
  ExecuteNode *do_get_show_dataserver_node();
  ExecuteNode *do_get_show_partition_scheme_node();
  ExecuteNode *do_get_show_schema_node(const char *schema);
  ExecuteNode *do_get_show_table_node(const char *schema, const char *table,
                                      bool use_like);
  ExecuteNode *do_get_migrate_clean_node(const char *migrate_id);
  ExecuteNode *do_get_show_execution_profile_node();
  ExecuteNode *do_get_show_data_source_node(list<const char *> &names,
                                            bool need_show_weight);
  ExecuteNode *do_get_dynamic_configuration_node();
  ExecuteNode *do_get_rep_strategy_node();
  ExecuteNode *do_get_set_server_weight_node();
  ExecuteNode *do_get_show_option_node();
  ExecuteNode *do_get_show_dynamic_option_node();
  ExecuteNode *do_get_change_startup_config_node();
  ExecuteNode *do_get_show_changed_startup_config_node();
  ExecuteNode *do_get_rename_table_node(PartitionedTable *old_table,
                                        PartitionedTable *new_table,
                                        const char *old_schema_name,
                                        const char *old_table_name,
                                        const char *new_schema_name,
                                        const char *new_table_name);
  ExecuteNode *do_get_set_node(DataSpace *dataspace, const char *sql);
  ExecuteNode *do_get_union_all_node(vector<ExecuteNode *> &nodes);
  ExecuteNode *do_get_create_oracle_sequence_node(
      create_oracle_seq_op_node *oper);
  ExecuteNode *do_get_union_group_node(vector<list<ExecuteNode *> > &nodes);
  ExecuteNode *do_get_drop_mul_table_node();
  ExecuteNode *do_get_send_dbscale_one_column_node(bool only_one_row);
  ExecuteNode *do_get_send_dbscale_mul_column_node();
  ExecuteNode *do_get_send_dbscale_one_column_aggr_node(bool get_min);
  ExecuteNode *do_get_send_dbscale_exists_node();
  ExecuteNode *do_get_keepmaster_node();
  ExecuteNode *do_get_dbscale_show_default_session_var_node();
  ExecuteNode *do_get_dbscale_show_version_node();
  ExecuteNode *do_get_show_version_node();
  ExecuteNode *do_get_show_components_version_node();
  ExecuteNode *do_get_dbscale_show_hostname_node();
  ExecuteNode *do_get_dbscale_show_all_fail_transaction_node();
  ExecuteNode *do_get_dbscale_show_detail_fail_transaction_node(
      const char *xid);
  ExecuteNode *do_get_dbscale_show_partition_table_status_node(
      const char *table_name);
  ExecuteNode *do_get_dbscale_show_schema_acl_info_node(bool is_show_all);
  ExecuteNode *do_get_dbscale_show_table_acl_info_node(bool is_show_all);
  ExecuteNode *do_get_dbscale_show_path_info();
  ExecuteNode *do_get_dbscale_show_warnings_node();
  ExecuteNode *do_get_dbscale_show_critical_errors_node();
  ExecuteNode *do_get_dbscale_clean_fail_transaction_node(const char *xid);
  ExecuteNode *do_get_dbscale_show_base_status_node();
  ExecuteNode *do_get_dbscale_mul_sync_node(const char *sync_topic,
                                            const char *sync_state,
                                            const char *sync_param,
                                            const char *sync_cond,
                                            unsigned long version_id);

  ExecuteNode *do_get_dbscale_add_default_session_var_node(
      DataSpace *dataspace);
  ExecuteNode *do_get_dbscale_remove_default_session_var_node();
  ExecuteNode *do_get_dbscale_test_node();
  ExecuteNode *do_get_async_task_control_node(unsigned long long id);
  ExecuteNode *do_get_load_dataspace_file_node(const char *filename);
  ExecuteNode *do_get_dbscale_help_node(const char *name);
  ExecuteNode *do_get_dbscale_show_audit_user_list_node();
  ExecuteNode *do_get_dbscale_show_slow_sql_top_n_node();
  ExecuteNode *do_get_dbscale_request_slow_sql_top_n_node();
  ExecuteNode *do_get_dbscale_show_join_node(const char *name);
  ExecuteNode *do_get_dbscale_show_shard_partition_node(const char *name);
  ExecuteNode *do_get_dbscale_show_rebalance_work_load_node(
      const char *name, list<string> sources, const char *schema_name,
      int is_remove);
  ExecuteNode *do_get_call_sp_return_ok_node();
  ExecuteNode *do_get_dbscale_purge_monitor_point_node();
  ExecuteNode *do_get_dbscale_clean_monitor_point_node();
  ExecuteNode *do_get_cursor_direct_node(DataSpace *dataspace, const char *sql);
  ExecuteNode *do_get_cursor_send_node();
  ExecuteNode *do_get_federated_table_node();
  ExecuteNode *do_get_empty_set_node(int field_num);
#ifndef CLOSE_ZEROMQ
  ExecuteNode *do_get_message_service_node();
  ExecuteNode *do_get_binlog_task_node();
  ExecuteNode *do_get_task_node();
  ExecuteNode *do_get_binlog_task_add_filter_node();
  ExecuteNode *do_get_drop_task_filter_node();
  ExecuteNode *do_get_drop_task_node();
  ExecuteNode *do_get_show_client_task_status_node(
      const char *task_name, const char *server_task_name);
  ExecuteNode *do_get_show_server_task_status_node(const char *task_name);
#endif
  virtual void do_send_error_packet();
  ExecuteNode *do_get_return_ok_node();
  ExecuteNode *do_get_slave_dbscale_error_node();
  ExecuteNode *do_get_forward_master_role_node(const char *sql,
                                               bool is_slow_query);
  ExecuteNode *do_get_reset_zoo_info_node();
  ExecuteNode *do_get_set_info_schema_mirror_tb_status_node(
      info_mirror_tb_status_node *tb_status);
  ExecuteNode *do_get_dbscale_set_priority_info_node(string user_name,
                                                     int tmp_priority_value);
  ExecuteNode *do_get_dbscale_update_audit_user_node(const char *username,
                                                     bool is_add);
  ExecuteNode *do_get_connect_by_node(ConnectByDesc connect_by_desc);
  ExecuteNode *do_get_dbscale_show_create_oracle_seq_node(
      const char *seq_schema, const char *seq_name);
  ExecuteNode *do_get_dbscale_show_seq_status_node(const char *seq_schema,
                                                   const char *seq_name);
  ExecuteNode *do_get_dbscale_show_fetchnode_buffer_usage_node();
  ExecuteNode *do_get_dbscale_show_trx_block_info_node(bool is_local);
  ExecuteNode *do_get_dbscale_internal_set_node();
  ExecuteNode *do_get_restore_recycle_table_precheck_node(
      const char *from_schema, const char *from_table, const char *to_table,
      const char *recycle_type, DataSpace *dspace);
  ExecuteNode *do_get_empty_node();
  ExecuteNode *do_get_dbscale_show_prepare_cache_node();
  ExecuteNode *do_get_dbscale_flush_prepare_cache_hit_node();
  bool has_duplicate_entry_in_error_packet(Packet *packet);
  bool check_select_error_not_rollback_in_packet(Packet *packet);

  void do_fullfil_error_info(unsigned int *error_code, string *sql_state);
};

class MySQLExecuteNode : public ExecuteNode {
 public:
  MySQLExecuteNode(ExecutePlan *plan, DataSpace *dataspace = NULL);
  ~MySQLExecuteNode() {
    if (ready_rows) delete ready_rows;
    ready_rows = NULL;
  }
  const char *get_executenode_name() { return name; }
  virtual bool is_finished() {
    return status == EXECUTE_STATUS_COMPLETE && ready_rows->empty();
  }
  void set_status(ExecuteStatus s) { status = s; }
  virtual Packet *get_header_packet() { return NULL; };
  virtual list<Packet *> *get_field_packets() { return NULL; };
  virtual Packet *get_eof_packet() { return NULL; };
  virtual Packet *get_end_packet() { return NULL; };
  virtual Packet *get_error_packet() = 0;
  virtual void handle_error_throw() { return; }
  virtual void handle_discarded_packets() {}

  virtual void set_node_error() { return; }
  virtual bool is_got_error() { return false; }

  virtual bool notify_parent() {
    ACE_ASSERT(0);
    return false;
  }
  virtual void start_thread() {}
  virtual void set_thread_status_started() {}
  // sub class interface
  virtual void clear_results_packet() {}
  virtual void handle_result_packets(
      list<Packet *, StaticAllocator<Packet *> > &result_packets,
      bool is_send_to_client = false) {
    ACE_UNUSED_ARG(result_packets);
    ACE_UNUSED_ARG(is_send_to_client);
  }

  bool has_sql_calc_found_rows() {
    if (plan->statement->get_stmt_node()->type == STMT_SELECT &&
        plan->statement->get_stmt_node()->cur_rec_scan->options &
            SQL_OPT_SQL_CALC_FOUND_ROWS) {
      return true;
    }
    return false;
  }

  void set_handler(Handler *h) { handler = (MySQLHandler *)h; }

  virtual int get_rowmap_size(ExecuteNode *node) {
    ACE_UNUSED_ARG(node);
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
    return 0;
  }

  virtual unsigned long get_buffer_rowmap_size(ExecuteNode *node) {
    ACE_UNUSED_ARG(node);
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
    return 0;
  }

  void rebuild_packet_first_column_dbscale_row_id(Packet *row, uint64_t row_num,
                                                  unsigned int field_num);
  void set_by_add_pre_disaster_master(bool b) {
    by_add_pre_disaster_master = b;
  }
  bool get_by_add_pre_disaster_master() { return by_add_pre_disaster_master; }

 private:
  virtual void do_add_child(ExecuteNode *child) {
    ACE_UNUSED_ARG(child);
    ACE_ASSERT(0);
  }

  virtual void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
  }
  bool by_add_pre_disaster_master;

 public:
  int profile_id;
  const char *name;
  MySQLDriver *driver;
  MySQLSession *session;
  MySQLHandler *handler;
  AllocList<Packet *, Session *, StaticAllocator<Packet *> > *ready_rows;
  ExecuteStatus status;
};

class MySQLEmptyNode : public MySQLExecuteNode {
 public:
  MySQLEmptyNode(ExecutePlan *plan) : MySQLExecuteNode(plan) {}
  void execute() {}
  bool is_finished() { return true; }
  void clean() {}
  Packet *get_error_packet() { return NULL; }
};

/**
 * The MySQLInnerNode is a basic class for the Execute Node which is the Inner
 * Node in the Execute plan tree.
 */
class MySQLInnerNode : public MySQLExecuteNode {
 public:
  MySQLInnerNode(ExecutePlan *plan, DataSpace *dataspace = NULL);

  virtual Packet *get_header_packet() {
    MySQLExecuteNode *child = children.front();
    return child->get_header_packet();
  }

  virtual list<Packet *> *get_field_packets() {
    MySQLExecuteNode *child = children.front();
    return child->get_field_packets();
  }

  virtual Packet *get_eof_packet() {
    MySQLExecuteNode *child = children.front();
    return child->get_eof_packet();
  }

  virtual Packet *get_end_packet() {
    LOG_DEBUG("Node %@ start to get end packet.\n", this);
    MySQLExecuteNode *child = children.front();
    return child->get_end_packet();
  }

  virtual Packet *get_error_packet();
  virtual void children_execute();
  bool notify_parent();
  virtual void wait_children();
  void init_row_map();
  void clean();

  void set_children_thread_status_start();
  /* the handler should be set to all the execution node when doing swap. */
  void set_handler(Handler *h) {
    handler = (MySQLHandler *)h;
    list<MySQLExecuteNode *>::iterator it = children.begin();
    for (; it != children.end(); it++) {
      (*it)->set_handler(h);
    }
  }

  void handle_swap_connections();
  void handle_error_all_children();
  bool can_swap() {
    if (node_can_swap) {
      list<MySQLExecuteNode *>::iterator it = children.begin();
      for (; it != children.end(); it++) {
        node_can_swap = node_can_swap & (*it)->can_swap();
      }
    }
    return node_can_swap;
  }

  void set_children_error();
  virtual void handle_discarded_packets() {
    list<MySQLExecuteNode *>::iterator it = this->children.begin();
    for (; it != this->children.end(); it++) {
      (*it)->handle_discarded_packets();
    }
  }

 private:
  virtual void do_add_child(ExecuteNode *child) {
    children.push_back((MySQLExecuteNode *)child);
  }
  virtual void do_get_children(list<ExecuteNode *> &children) {
    list<MySQLExecuteNode *>::iterator it = this->children.begin();
    for (; it != this->children.end(); it++) {
      children.push_back(*it);
    }
  }

  virtual void do_clean() {}

 public:
  unsigned long ready_rows_buffer_size;

  list<MySQLExecuteNode *> children;
  bool all_children_finished;
  /* This is used as a flag to indicate the inner node can swap */
  bool node_can_swap;
  bool one_child_got_error;

  bool thread_started;
#ifndef DBSCALE_TEST_DISABLE
 protected:
  int loop_count;
  bool need_record_loop_count;
#endif
};

/**
 * MySQLDirectExecuteNode send the query directly to one data source, and
 * receive packet then send to client.
 */
class MySQLDirectExecuteNode : public MySQLExecuteNode {
 public:
  MySQLDirectExecuteNode(ExecutePlan *plan, DataSpace *dataspace,
                         const char *sql);

  void clean();

  Packet *get_error_packet() { return error_packet; }
  bool can_swap();

  void execute();
  void deal_into_outfile();
  void update_column_display_name(Packet *packet, string field_str);
  bool is_stmt_insert_like(ExecutePlan *plan);
  string get_explain_sql() { return string(sql); }
  const char *get_node_sql() { return sql; }

 protected:
  bool is_direct_node;
  virtual void handle_send_client_packet(Packet *packet, bool is_row_packet);
  virtual void handle_send_field_packet(Packet *packet);
  virtual void handle_send_last_eof_packet(Packet *packet);
  unsigned int field_num;
  unsigned int defined_lock_field_num;

 private:
  bool is_show_full_fields;
  string show_full_fields_schema_name;
  string show_full_fields_table_name;
  string defined_lock_name;
  bool is_show_full_fields_via_mirror_tb;
  void handle_call_sp_directly();
  void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
    return;
  }
  bool deal_with_com_prepare(bool read_only, MySQLExecuteRequest &exec_req,
                             MySQLPrepareItem *prepare_item);
  void re_init_com_prepare_conn(Packet **real_exec_packet);

  Connection *connect_with_cur_user(stmt_type type);
  void prepare_work(bool &need_check_last_insert_id, int64_t &before_insert_id);
  void handle_error_packet(Packet *packet);
  bool handle_result_set(Packet *packet);
  void handle_set_last_insert_id(bool need_check_last_insert_id,
                                 int64_t &before_insert_id);
  bool handle_ok_packet(Packet *packet, bool need_check_last_insert_id,
                        int64_t &before_insert_id, bool &non_modified_conn);
  void execute_grant_sqls();
  void set_kept_defined_lock_conn(bool b) { kept_lock_new_connection = b; }
  bool get_kept_defined_lock_conn() { return kept_lock_new_connection; }
  const char *sql;
  Connection *conn;
  bool got_error;
  Packet *packet;
  Packet *error_packet;
  bool stmt_insert_like;
  vector<pair<unsigned int, string> > uservar_vec;
  bool select_uservar_flag;
  unsigned int select_field_num;
  vector<bool> field_is_number_vec;
  string password;
  string user;
  string host;
  bool has_change_user;

  ExecuteProfileHandler *execute_profile;
  unsigned int profile_id;
  int64_t before_insert_id;
  bool need_check_last_insert_id;
  uint64_t federated_max_rows;
  uint64_t cross_join_max_rows;
  bool direct_prepare_exec_packet;
  bool kept_lock_new_connection;
};

struct conn_result {
  Connection *conn;
  DataSpace *space;
  Packet *packet;
  bool is_dead_conn;
  string err_message;
};

class MySQLSetNode : public MySQLExecuteNode {
 public:
  MySQLSetNode(ExecutePlan *plan, DataSpace *dataspace, const char *sql);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void execute();
  void conn_set_session_var();
  bool can_swap();
  void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
    return;
  }

 private:
  void replace_query_sql(string &query_sql);
  void exe_state_working();
  bool handle_swap();
  void exe_state_handling_result();

 private:
  bool is_need_trans(string value);
  const char *sql;
  Connection *conn;
  bool got_error;
  Packet *packet;
  Packet *error_packet;
  vector<conn_result> commit_conn_result;
  string real_sql;
  map<DataSpace *, Connection *> local_connections;
};

class MySQLTransactionUNLockNode : public MySQLExecuteNode {
 public:
  MySQLTransactionUNLockNode(ExecutePlan *plan, const char *sql,
                             bool is_send_to_client);
  virtual ~MySQLTransactionUNLockNode();
  void clean();
  bool can_swap();
  Packet *get_error_packet() { return error_packet; }
  void deal_savepoint();
  virtual void execute();
  void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
    return;
  }

 protected:
  void handle_if_no_kept_conn();
  void handle_if_not_in_transaction();
  void report_trans_end_for_consistence_point();

  bool execute_send_part();
  void execute_receive_part();

  bool receive_bridge_mark_sql_return(Connection *conn, Packet *packet);
  TimeValue *determine_recv_timeout(TimeValue &tv);

  const char *sql;
  Connection *conn;
  bool got_error;
  bool is_send_to_client;
  Packet *packet;
  Packet *error_packet;
  vector<conn_result> commit_conn_result;
};

class MySQLXATransactionNode : public MySQLTransactionUNLockNode {
 public:
  MySQLXATransactionNode(ExecutePlan *plan, const char *sql,
                         bool is_send_to_client);
  void execute();

 private:
  int exec_xa_for_all_kept_conn(
      map<DataSpace *, Connection *> *kept_connections, XACommand type,
      bool is_send);
  int exec_non_xa_for_all_kept_conn(
      map<DataSpace *, Connection *> *kept_connections, DataSpace *dataspace,
      XACommand type, bool is_send);
  list<DataSpace *> get_transaction_modified_spaces(Session *s);
  void receive_for_non_modify_connection(
      map<DataSpace *, Connection *> *kept_connections, DataSpace *dataspace);
  void xa_swap_conns(map<Connection *, bool> *tx_rrlkc);
  void xa_swap_conns(Connection *conn);
  void xa_swap_conns(map<DataSpace *, Connection *> *kept_connections);
  void report_xa_trans_end_for_consistence_point();
  void handle_auth_space_for_xa(DataSpace *auth_space, Connection *auth_conn);

  bool is_modify_sql(string s) {
    string::size_type pos = s.find_first_of('\'');
    if (!strcasecmp(s.substr(pos + 1, 3).c_str(), "USE") ||
        !strcasecmp(s.substr(pos + 1, 3).c_str(), "SET")) {
      return false;
    }
    return true;
  }
  void change_xa_state(XASwapState state);
  bool xa_init();
  void handle_optimize_xa_non_modified_sql();
  bool readonly_conn_can_optimize(DataSpace *dataspace, Connection *conn,
                                  string xid);

  stmt_type st_type;
  map<DataSpace *, Connection *> kept_connections;
  bool is_error;
  unsigned int warnings;
  XACommand xa_command_type;
  list<DataSpace *> dataspace_list;
  int fail_num;
  XASwapState xa_state;
  bool has_swap;
  bool can_swap;
  bool has_optimized_readonly_conn;
  bool all_kept_conn_optimized;
  bool commit_fail;
  vector<DataSpace *> handle_xa_success_spaces;
};

class MySQLClusterXATransactionNode : public MySQLExecuteNode {
 public:
  MySQLClusterXATransactionNode(ExecutePlan *plan);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void execute();
  void handle_send_client_packet(Packet *packet, bool is_row_packet);
  void handle_error_packet(Packet *packet);
  void handle_more_result(Packet *packet);

 private:
  stmt_type st_type;
  const char *xa_sql;
  Packet *error_packet;
  Packet *packet;
  Connection *xa_conn;
  DataSpace *dataspace;
  bool got_error;
};

class MySQLLockNode : public MySQLExecuteNode {
 public:
  MySQLLockNode(ExecutePlan *plan,
                map<DataSpace *, list<lock_table_item *> *> *lock_table_list);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void execute();
  string build_sql(DataSpace *space);
  string adjust_table_name(string ori_name);

 private:
  Connection *conn;
  bool got_error;
  Packet *packet;
  Packet *error_packet;
  map<DataSpace *, list<lock_table_item *> *> *lock_table_list;
};

class MySQLMigrateNode : public MySQLExecuteNode {
 public:
  MySQLMigrateNode(ExecutePlan *plan);

  void clean();
  Packet *get_error_packet() { return NULL; }
  void execute();

 protected:
  int init_migrate_task();
  void send_error_packet(string msg);
  bool migrate_sync(DynamicAddSpaceType type);
  void start_tasks();
  void wake_up_tasks();
  void stop_all_tasks();
  bool check_all_task_status(MigrateStatus s);
  void wait_all_task_stop();
  bool update_migrate_config();
  bool flush_one_server(Connection *conn);
  bool flush_all_servers_with_consistent_point();
  bool unlock_one_server(Connection *conn);
  bool flush_all_servers();
  void unlock_all_servers();
  bool generate_migrate_flush_group();
  void prepare_space_for_migrate(migrate_op_node *node);
  void generate_partition_migrate_task(
      const char *schema_name, const char *table_name,
      map<unsigned int, DataSource *> &migrate_partition_map, DataSpace *space,
      migrate_type type);
  void rollback_migrate_topo();
  void change_topo();
  void set_error_message(string m) {
    LOG_ERROR("%s\n", m.c_str());
    if (error_message == "") error_message = m;
  }
  string generate_error_message() {
    string task_error_message;
    list<MigrateThreadTask *>::iterator it = migrate_task_list.begin();
    for (; it != migrate_task_list.end(); it++) {
      string msg = (*it)->get_error_message();
      if (msg != "") {
        task_error_message += msg;
        task_error_message += "\n";
      }
    }
    if (task_error_message != "") {
      boost::erase_tail(task_error_message, 1);  // cut the tail '\n'
      return task_error_message;
    }
    return error_message;
  }

 private:
  list<MigrateThreadTask *> migrate_task_list;
  MigrateStatus m_status;
  map<DataSpace *, migrate_type>
      migrate_space_map;  // record all migrate space and migrate type
  map<DataSpace *, PartitionScheme *>
      migrate_shard_scheme_map;  // record migrate shard scheme
  map<DataSpace *, DataSource *>
      migrate_norm_table_map;  // record norm table space map to target source
  bool need_roll_back_topo;    // before change topo, all migrate space need
                               // rollback_migrate_topo
  long migrate_command_id;
  map<Connection *, list<MigrateThreadTask *> > flush_conn_map;
  string migrate_sync_param;
  string error_message;
};

#ifndef DBSCALE_DISABLE_SPARK
class MySQLSparkNode : public MySQLExecuteNode, public BackendThreadTask {
 public:
  MySQLSparkNode(ExecutePlan *plan, SparkConfigParameter config,
                 bool is_insert);

  void set_spark_return_value(SparkReturnValue return_value) {
    mutex.acquire();
    spark_result = return_value;
    service_finished = true;
    mutex.release();
  }
  void set_error_msg(string err_msg) { error_msg = err_msg; }

  Packet *get_header_packet() { return head_packet; }

  list<Packet *> *get_field_packets() { return &column_list; }

  Packet *get_eof_packet() { return eof_packet; }

  bool notify_parent();
  void execute();
  void clean();
  void set_column_packet();
  void send_row_packet();
  void add_to_kept_rows(vector<vector<string> > &datas);
  void add_to_ready_rows(vector<vector<string> > &datas);
  void pack_row_data(Packet *packet, vector<string> &row_data);
  void start_spark_thread();
  void stop_spark_service();
  int svc();
  Packet *get_error_packet() { return NULL; }

  SparkConfigParameter config;

 private:
  ACE_Thread_Mutex mutex;
  ACE_Condition<ACE_Thread_Mutex> cond;
  Packet *head_packet;
  Packet *eof_packet;
  bool column_received;
  bool service_finished;
  bool finished;
  bool got_error;
  list<vector<string> > *read_rows;
  list<vector<string> > *kept_rows;
  list<Packet *> column_list;
  vector<string> columns;
  BackendThreadStatus thread_status;
  BackendThread *bthread;
  ACE_thread_t l_id;
  ACE_hthread_t l_handle;
  SparkReturnValue spark_result;
  SparkReturnValue fetch_result;
  string error_msg;
  int kept_rows_size;
  unsigned long max_spark_buffer_rows;
  unsigned long max_spark_buffer_rows_size;
  bool is_insert_select;
  bool has_started;
};
#endif

class MySQLFetchNode : public MySQLExecuteNode, public BackendThreadTask {
 public:
  MySQLFetchNode(ExecutePlan *plan, DataSpace *dataspace, const char *sql);

  Packet *get_header_packet() { return &header_packet; }

  list<Packet *> *get_field_packets() { return &field_packets; }

  Packet *get_eof_packet() { return eof_packet; }
  void set_thread_status_started() {
    if (bthread) thread_status = THREAD_STARTED;
  }
  virtual void set_node_error() { got_error = true; }

  Packet *get_end_packet() {
    LOG_DEBUG("FetchNode %@ get end_packet = %@\n", this, end_packet);
    return end_packet;
  }

  bool is_finished() {
    bool ret;
    ret = (ready_rows->empty() && (status == EXECUTE_STATUS_COMPLETE)) ||
          (thread_status == THREAD_STOP && !plan->get_fetch_node_no_thread());

    return ret;
  }

  void handle_dead_xa_conn(Connection *conn, DataSpace *space);
  int fetch_row();
  void clean();
  void deal_error_packet() {
    MySQLErrorResponse response(error_packet);
    if (response.is_shutdown()) {
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      session->set_server_shutdown(true);
    }
    if (conn) {
      if (!plan->get_migrate_tool())
        handler->put_back_connection(dataspace, conn);
      conn = NULL;
    }
    throw ErrorPacketException();
  }

  void throw_error_packet() {
    if (error_packet) {
      deal_error_packet();
    } else {
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      throw ExecuteNodeError(error_message.c_str());
    }
  }

  /**
   * Return ture if send row packet to parent, else return false.
   */
  bool notify_parent();
  void start_thread();
  void handle_error_throw();

  Packet *get_error_packet() { return error_packet; }

  void handle_error_packet(Packet *packet) {
    bool b = false;
    if (conn)
      b = conn->get_server()->get_innodb_rollback_on_timeout() ==
          INNODB_ROLLBACK_ON_TIMEOUT_ON;
    mutex.acquire();
    record_migrate_error_message(plan, packet,
                                 "FetchNode node get an error packet", b);
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    error_packet = packet;
    cond.signal();
    mutex.release();
  }

  int svc();
  int receive_header_packets();

  void execute();

  void get_partition_key_pos();
  bool migrate_filter(Packet *row);

  void wakeup_federated_follower();
  void handle_federated_empty_resultset(Packet *packet,
                                        unsigned int &count_index);
  bool can_swap();
  void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
    return;
  }
  Connection *get_connection_from_node() { return conn; }
  string get_explain_sql() { return sql; }
  virtual bool is_got_error() { return got_error; }
  void add_kept_list_to_ready_and_signal();
  virtual bool is_fetch_node() const { return true; }

 private:
  bool force_use_non_trx_conn;
  Connection *conn;
  string sql;
  ACE_Thread_Mutex mutex;
  BackendThreadStatus thread_status;
  BackendThread *bthread;
  bool sql_sent;
  bool got_error;
  Packet header_packet;
  list<Packet *> field_packets;
  Packet *eof_packet;
  Packet *end_packet;
  bool is_get_end_packet;
  Packet *error_packet;
  ACE_Condition<ACE_Thread_Mutex> cond;
  string error_message;
  unsigned int ready_rows_size;

  unsigned long ready_buffer_rows_size;
  unsigned long max_fetchnode_buffer_rows_size;
  vector<unsigned int> *migrate_partition_key_pos_vec;
  bool header_received;
  bool has_get_partition_key_pos;
  list<Packet *, StaticAllocator<Packet *> > pkt_list;
  int kept_ready_packets;
  list<Packet *, StaticAllocator<Packet *> > ready_rows_kept;
  int local_fetch_signal_batch;
};

class MySQLSendNode : public MySQLInnerNode {
 public:
  MySQLSendNode(ExecutePlan *plan);

  virtual void send_header();
  virtual void send_row(MySQLExecuteNode *ready_child);
  void do_clean() {
    if (session->get_work_session())
      session->get_work_session()->increase_finished_federated_num();

    while (!pkt_list.empty()) {
      Packet *p = pkt_list.front();
      pkt_list.pop_front();
      delete p;
    }
    pkt_list.clear();
    pkt_list_size = 0;
  }
  void build_eof_packet() {
    MySQLEOFResponse eof_packet;
    eof_packet.pack(&generated_eof);
  }

  virtual void send_eof();

  virtual void handle_child(MySQLExecuteNode *child) {
    if (send_header_flag) {
      send_header();
      send_header_flag = false;

      Packet *packet = get_eof_packet();
      MySQLEOFResponse eof(packet);
      eof.unpack();
      /*
       * If the EOF packet has the SERVER_STATUS_CURSOR_EXISTS flag set,
       * row-data will not follow, otherwise, it will.
       */
      if (eof.cursor_exists()) {
        return;
      }
    }

    send_row(child);
  }

  void handle_children();
  void execute();
  virtual void flush_net_buffer() { handler->flush_net_buffer(); }
  void clear_row_map();

 private:
  list<Packet *, StaticAllocator<Packet *> > pkt_list;
  int pkt_list_size;
  unsigned int field_num;

 protected:
  bool send_header_flag;
  uint64_t row_num;
  int send_packet_profile_id;
  int wait_child_profile_id;
  Packet generated_eof;
  vector<pair<unsigned int, string> > uservar_vec;
  bool select_uservar_flag;
  unsigned int select_field_num;
  vector<bool> field_is_number_vec;
  uint64_t federated_max_rows;
  uint64_t cross_join_max_rows;
  unsigned int tmp_id;
};

class MySQLSendToDBScaleNode : public MySQLSendNode {
 public:
  MySQLSendToDBScaleNode(ExecutePlan *plan);

  // By default, just ignore the header and eof part
  virtual void send_eof(){};
  virtual void send_header(){};
  // The child class must re-write send_row function!
  virtual void send_row(MySQLExecuteNode *ready_child);

 protected:
  SubQueryResultNode *res_node;
};

class MySQLSendExistsToDBScaleNode : public MySQLSendToDBScaleNode {
 public:
  MySQLSendExistsToDBScaleNode(ExecutePlan *plan);
  void send_row(MySQLExecuteNode *ready_child);

 private:
  bool set_res_node;
};

class MySQLSendOneColumnToDBScaleNode : public MySQLSendToDBScaleNode {
 public:
  MySQLSendOneColumnToDBScaleNode(ExecutePlan *plan, bool only_one_row);
  void send_header();
  void send_row(MySQLExecuteNode *ready_child);

 private:
  void handle_one_row(Packet *row);
  MySQLColumnType col_type;
  bool only_one_row;
  bool has_get_one_row;
};

class MySQLSendMulColumnToDBScaleNode : public MySQLSendToDBScaleNode {
 public:
  MySQLSendMulColumnToDBScaleNode(ExecutePlan *plan);
  void send_header();
  void send_row(MySQLExecuteNode *ready_child);

 private:
  void handle_one_row(Packet *row);
  bool has_get_one_row;
  vector<MySQLColumnType> col_types;
  unsigned int col_nums;
};

class MySQLQueryForOneColumnNode : public MySQLDirectExecuteNode {
 public:
  MySQLQueryForOneColumnNode(ExecutePlan *plan, DataSpace *dataspace,
                             const char *sql, bool only_one_row);

 protected:
  void handle_send_client_packet(Packet *packet, bool is_row_packet);
  void handle_send_field_packet(Packet *packet);

 private:
  void handle_one_row(Packet *row);
  bool has_get_one_row;
  SubQueryResultNode *res_node;
  MySQLColumnType col_type;
  bool only_one_row;
  uint64_t cross_join_max_rows;
  uint64_t row_num;
};

class MySQLQueryForMulColumnNode : public MySQLDirectExecuteNode {
 public:
  MySQLQueryForMulColumnNode(ExecutePlan *plan, DataSpace *dataspace,
                             const char *sql);

 protected:
  void handle_send_client_packet(Packet *packet, bool is_row_packet);
  void handle_send_field_packet(Packet *packet);

 private:
  void handle_one_row(Packet *row);
  bool has_get_one_row;
  SubQueryResultNode *res_node;
  vector<MySQLColumnType> col_types;
  unsigned int col_nums;
};

class MySQLQueryExistsNode : public MySQLDirectExecuteNode {
 public:
  MySQLQueryExistsNode(ExecutePlan *plan, DataSpace *dataspace,
                       const char *sql);

 protected:
  void handle_send_client_packet(Packet *packet, bool is_row_packet);

 private:
  void handle_one_row(Packet *row);
  bool has_get_one_row;
  SubQueryResultNode *res_node;
};

/*For compare any/some/all (subquery), we will transform it to "compare
 * any/some (select min)" or "compare all (select max)".
 *
 * Cause >any/some(list of value) is equal to > min(list of value);
 *
 *       >all(list of value) is equal to > max(list of value)*/
class MySQLSendOneColumnAggrToDBScaleNode : public MySQLSendToDBScaleNode {
 public:
  MySQLSendOneColumnAggrToDBScaleNode(ExecutePlan *plan, bool get_min);
  void send_eof();
  void send_header();
  void send_row(MySQLExecuteNode *ready_child);

 private:
  void handle_one_row(Packet *row);
  MySQLColumnType col_type;
  Packet aggr_packet;
  int get_min;
  bool has_get_one_row;
};

class MySQLQueryForOneColumnAggrNode : public MySQLDirectExecuteNode {
 public:
  MySQLQueryForOneColumnAggrNode(ExecutePlan *plan, DataSpace *dataspace,
                                 const char *sql, bool get_min);

 protected:
  void handle_send_client_packet(Packet *packet, bool is_row_packet);
  void handle_send_last_eof_packet(Packet *packet);
  void handle_send_field_packet(Packet *packet);

 private:
  void handle_one_row(Packet *row);
  SubQueryResultNode *res_node;
  MySQLColumnType col_type;
  bool has_get_one_row;
  Packet aggr_packet;
  int get_min;
};

class MySQLIntoOutfileNode : public MySQLSendNode {
 public:
  MySQLIntoOutfileNode(ExecutePlan *plan);
  ~MySQLIntoOutfileNode();

  void send_header();
  void send_row(MySQLExecuteNode *ready_child);
  void execute();
  void write_into_outfile(Packet *packet, into_outfile_item *into_outfile,
                          ofstream &file, spawn_param *param = NULL);
  void clean();

  const char *get_filename() { return filename; }
  void set_got_error() { param.set_got_error(true); }

 private:
  void prepare_fifo_or_load();
  void open_fifo(ExecutePlan *plan);
  void load_insert_select();

 private:
  bool is_local;
  ofstream file;
  const char *filename;
  uint64_t affect_rows;
  vector<bool> is_column_number;
  ACE_thread_t t_id;
  ACE_hthread_t t_handle;
  int pipe_fd;
  spawn_param param;
  FILE *fd;
  ACE_thread_t l_id;
  ACE_hthread_t l_handle;
  char *row_buffer;
};

class MySQLSelectIntoNode : public MySQLSendNode {
 public:
  MySQLSelectIntoNode(ExecutePlan *plan);

  void send_header();
  void send_row(MySQLExecuteNode *ready_child);
  void send_eof(){};
  void execute();

 private:
  void generate_select_into_vec(select_into_item *select_into);
  void write_into_name_list(Packet *row);

 private:
  uint64_t affect_rows;
  vector<bool> field_is_number_vec;
  vector<pair<bool, string> > select_into_vec;
};

class MySQLSortNode : public MySQLInnerNode {
 public:
  MySQLSortNode(ExecutePlan *plan, list<SortDesc> *sort_desc = NULL);

  virtual void sort();
  void last_sort();
  void merge(list<Packet *> *row_list);
  void execute();

 public:
  void init_merge_sort_variables();
  void adjust_column_index();
  void check_column_valid();
  void insert_row(MySQLExecuteNode *child);
  void pop_row();
  unsigned int get_last_rows();
  void add_last_one_list();
  int get_rowmap_size(ExecuteNode *node) { return row_map_size[node]; }
  unsigned long get_buffer_rowmap_size(ExecuteNode *node) {
    return buffer_row_map_size[node];
  }

 public:
  list<SortDesc> sort_desc;
  AllocList<Packet *, Session *, StaticAllocator<Packet *> > *sort_ready_nodes;
  vector<MySQLColumnType> column_types;
  bool column_inited_flag;
  unsigned int column_num;
  bool adjust_column_flag;
  bool check_column_type;
  unsigned int child_num;
  vector<Packet *> merge_sort_vec;  // used for store merge sort temporary
                                    // result
  unsigned int sort_node_size;
  vector<MySQLExecuteNode *>
      sort_pos;  // record the MySQLExecuteNode for merge_sort_vec
  map<ExecuteNode *, bool> child_finished;
};

class MySQLProjectNode : public MySQLInnerNode {
 public:
  MySQLProjectNode(ExecutePlan *plan, unsigned skip_columns);

  void init_header_packet();

  Packet *get_header_packet() {
    if (!header_packet_inited) init_header_packet();
    return &header_packet;
  }

  void init_field_packet();

  list<Packet *> *get_field_packets() {
    if (!field_packets_inited) init_field_packet();
    return &field_packets;
  }

  void init_columns_num() {
    if (!header_packet_inited) init_header_packet();
  }

  void handle_child(MySQLExecuteNode *child);
  void handle_children();
  void execute();

 private:
  MySQLExecuteNode *first_child;
  unsigned int skip_columns;
  unsigned int columns_num;
  list<Packet *> field_packets;
  bool field_packets_inited;
  bool header_packet_inited;
  Packet header_packet;
};

class MySQLLimitNode : public MySQLInnerNode {
 public:
  MySQLLimitNode(ExecutePlan *plan, long offset, long num);

  int handle_child(MySQLExecuteNode *child);
  void handle_children();

  void execute();

 private:
  long offset;
  long num;
  long row_count;
};

class MySQLOKNode : public MySQLInnerNode {
 public:
  MySQLOKNode(ExecutePlan *plan);

  void execute();
};

class MySQLOKMergeNode : public MySQLInnerNode {
 public:
  MySQLOKMergeNode(ExecutePlan *plan);

  void handle_children();
  void handle_child(MySQLExecuteNode *child);
  void rebuild_ok_packet();
  virtual void execute();

 private:
  void do_clean() {
    if (ok_packet) delete ok_packet;
    ok_packet = NULL;
  }
  Packet *ok_packet;
  uint64_t affect_rows;
  uint16_t warnings;
  string update_msg;
};

class MySQLInnerPipeNode : public MySQLInnerNode {
 public:
  MySQLInnerPipeNode(ExecutePlan *plan);

  virtual void handle_children();
  virtual void handle_child(MySQLExecuteNode *child);
  virtual void execute();
  virtual void handle_before_complete() {}
};

class MySQLAvgNode : public MySQLInnerPipeNode {
 public:
  MySQLAvgNode(ExecutePlan *plan, list<AvgDesc> &avg_list);
  virtual void handle_child(MySQLExecuteNode *child);
  virtual void clean();

 private:
  void init_column_index();

 private:
  bool inited_column_index;

  list<AvgDesc> avg_list;
  map<int, MySQLColumnType> avg_column_type;
};

class MySQLConnectByNode : public MySQLInnerPipeNode {
 public:
  MySQLConnectByNode(ExecutePlan *plan, ConnectByDesc connect_by_desc);
  void handle_child(MySQLExecuteNode *child);
  void handle_before_complete();
  void do_clean();
  void handle_discarded_packets();

 private:
  void init_column_index();
  void find_recur_row_result(unsigned int id);

  bool inited_column_index;
  int start_index;
  int prior_index;
  int recur_index;
  int where_index;
  unsigned int max_result_num;

  unsigned int row_id;
  list<unsigned int> start_ids;
  map<unsigned int, pair<string, Packet *> > id_prior_packets;
  map<string, list<unsigned int> > recur_ids;
  bitset<MAX_CONNECT_BY_CACHED_ROWS> *loop_flag;
  bitset<MAX_CONNECT_BY_CACHED_ROWS> *ignore_flag;
};

class MySQLLoadSelectAnalysisNode;

class MySQLLoadSelectNode : public MySQLInnerPipeNode {
  friend class MySQLLoadSelectAnalysisNode;

 public:
  MySQLLoadSelectNode(ExecutePlan *plan, const char *s, const char *t);
  ~MySQLLoadSelectNode();
  virtual void handle_child(MySQLExecuteNode *child);
  virtual void do_clean();
  void set_max_len(unsigned int len);
  void set_load_conn(Connection *conn) { load_conn = conn; }

  Packet *get_error_packet() {
    Packet *error_packet = handler->get_error_packet(
        ERROR_EXECUTE_NODE_FAILED_CODE,
        "Load select node execute failed, plz check log!", NULL);
    get_error = true;
    return error_packet;
  }

 protected:
  void init_load_select();

  char *handle_one_row(Packet *packet,
                       size_t &copy_len);  // read packet data and transter
                                           // select packet struct to string
  void pack_packet(Packet *packet, char *data,
                   size_t len);  // transter string to load packet struct.
  void generate_load_sql();
  void handle_before_complete();  // send last data packet and set empty packet
                                  // to end load
  void init_header_packet();

 protected:
  bool inited_load;
  bool get_error;
  Connection *load_conn;
  unsigned int packet_num;
  int end_load_times;
  Packet *packet;
  char *data;
  size_t data_buffer_size;
  const char *schema_name;
  const char *table_name;
  string sql;
  size_t data_len;
  size_t max_len;
  TimeValue timeout;
  struct into_outfile_item into_outfile;
  vector<bool> is_column_number;
  DataSpace *dataspace;
  char *row_buffer;
};

class MySQLLoadLocalPartitionNode;
class MySQLLoadSelectPartitionNode : public MySQLLoadSelectNode {
  friend class MySQLLoadSelectAnalysisNode;

 public:
  MySQLLoadSelectPartitionNode(ExecutePlan *plan, const char *s, const char *t);
  void handle_child(MySQLExecuteNode *child);
  void do_clean();
  Packet *get_error_packet();
  void signal() {
    ACE_GUARD_REACTION(
        ACE_Thread_Mutex, guard, lock,
        LOG_ERROR("%p\n", "acquire LOAD DATA partitioned table lock");
        throw HandlerError("acquire LOAD DATA partitioned table failed"));
    cond.signal();
  }
  void add_to_ready_packets_from_Analysis(unsigned int par_id, Packet *packet) {
    ready_packet_lock.acquire();
    ready_packets[par_id]->push_back(packet);
    wakeup_analysis_sync_cond();
    ready_packet_lock.release();
  }
  void wait_analysis_sync_cond() {
    analysis_sync_lock.acquire();
    analysis_sync_cond.wait();
    analysis_sync_lock.release();
  }

  void wakeup_analysis_sync_cond() {
    analysis_sync_lock.acquire();
    analysis_sync_cond.signal();
    analysis_sync_lock.release();
  }
  void wakeup_analysis_sync_for_fin() {
    LOG_DEBUG("wakeup_analysis_sync_for_fin start.\n");
    analysis_sync_lock.acquire();
    analysis_finish_num++;
    analysis_sync_cond.signal();
    analysis_sync_lock.release();
  }
  void flush_ready_packets_to_child();

  void wait_analysis_node_finish();

  void check_all_analysis_node();

  void assign_row_to_analysis(Packet *packet);

 private:
  void clean_up_analysis_nodes();
  void init_load_select();
  void handle_before_complete();  // send last data packet and set empty packet
                                  // to end load
  void report_result_to_client();
  void wait_partition();
  void flush_all();
  Partition *get_partition(int num) {
    DataSpace *dataspace =
        Backend::instance()->get_data_space_for_table(schema_name, table_name);
    return (static_cast<PartitionedTable *>(dataspace))->get_partition(num);
  }

 private:
  vector<MySQLLoadLocalPartitionNode *> partition_nodes;
  unsigned int partition_num;
  ACE_Thread_Mutex lock;
  ACE_Condition<ACE_Thread_Mutex> cond;

  vector<list<Packet *> *> ready_packets;
  ACE_Thread_Mutex ready_packet_lock;
  ACE_Thread_Mutex analysis_lock;
  size_t analysis_num;
  vector<MySQLLoadSelectAnalysisNode *> analysis_vec;

  ACE_Thread_Mutex analysis_sync_lock;
  ACE_Condition<ACE_Thread_Mutex> analysis_sync_cond;
  size_t analysis_finish_num;
  /*Indicate the last filled analysis node */
  size_t last_added_analysis_node_pos;
};

class MySQLLoadSelectAnalysisNode : public BackendThreadTask {
 public:
  MySQLLoadSelectAnalysisNode(MySQLLoadSelectPartitionNode *parent,
                              vector<unsigned int> &key_pos_vec,
                              PartitionMethod *m, int partition_num,
                              DataSpace *space, MySQLDriver *driver)
      : table_node(parent),
        sync_cond(sync_lock),
        table_node_is_waiting(false),
        waiting_size(0),
        partition_num(partition_num),
        dataspace(space),
        driver(driver) {
    analysis_is_waiting = false;
    has_finished = false;
    is_stop = false;
    thread_status = THREAD_STOP;
    bthread = NULL;
    table_node_has_get_empty_packet = false;
    max_analysis_wait_size_local = max_load_select_analysis_wait_size;
    key_pos = key_pos_vec;
    method = m;
    int i = 0;
    for (; i < partition_num; i++) {
      char *buffer = new char[DEFAULT_LOAD_PACKET_SIZE + 128];
      datas.push_back(buffer);
      data_lens.push_back(0);
      data_buffer_size.push_back(DEFAULT_LOAD_PACKET_SIZE + 128);
    }
#ifndef DBSCALE_TEST_DISABLE
    max_analysis_wait_size_local = 100;
#endif
  }

  ~MySQLLoadSelectAnalysisNode();

  void wait_sync_cond() { sync_cond.wait(); }
  void wakeup_sync_cond() { sync_cond.signal(); }
  void acquire_sync_lock() { sync_lock.acquire(); }
  void release_sync_lock() { sync_lock.release(); }

  void wakeup_sync_parent_waiting_before_fin() {
    LOG_DEBUG("wakeup_sync_parent_waiting_before_fin start.\n");
    /*If analysis node fin due to exception, the parent node may still in
     * waiting for adding row packet, so we should wakeup it.*/
    acquire_sync_lock();
    if (table_node_is_waiting) {
      wakeup_sync_cond();
      table_node_is_waiting = false;
    }
    release_sync_lock();
  }

  /*used by MySQLLoadSelectPartitionNode thread*/
  void add_packet_to_analysis(Packet *row);

  /*used by MySQLLoadSelectAnalysisNode thread*/
  Packet *get_packet_from_waiting_packets();
  size_t get_waiting_size() {
    size_t ret = 0;
    acquire_sync_lock();
    ret = waiting_size;
    release_sync_lock();
    return ret;
  }

  void handle_one_row();

  int svc();
  void start_thread();

  void flush_rows(unsigned int par_id);
  void flush_all();

  void set_table_node_has_get_empty_packet() {
    acquire_sync_lock();
    table_node_has_get_empty_packet = true;
    if (analysis_is_waiting) {
      wakeup_sync_cond();
      analysis_is_waiting = false;
    }
    release_sync_lock();
  }

  void check_exception() {
    if (exception == boost::exception_ptr()) return;
    rethrow_exception(exception);
  }
  bool has_finish() { return has_finished; }
  void set_is_stop() {
    sync_lock.acquire();
    is_stop = true;
    sync_cond.signal();
    sync_lock.release();
  }

  string get_field_replace_null_str(string &field, const char *buffer,
                                    size_t len);

 private:
  MySQLLoadSelectPartitionNode *table_node;
  ACE_Thread_Mutex sync_lock;
  ACE_Condition<ACE_Thread_Mutex> sync_cond;
  bool table_node_is_waiting;
  list<Packet *> waiting_packets;
  size_t waiting_size;
  bool analysis_is_waiting;
  bool has_finished;
  boost::exception_ptr exception;
  bool is_stop;
  BackendThreadStatus thread_status;
  BackendThread *bthread;
  bool table_node_has_get_empty_packet;  // means the
                                         // MySQLLoadSelectPartitionNode has
                                         // finish the select part.

  vector<unsigned int> key_pos;
  unsigned int max_analysis_wait_size_local;
  PartitionMethod *method;

  vector<char *> datas;
  vector<size_t> data_lens;
  vector<size_t> data_buffer_size;

  int partition_num;
  DataSpace *dataspace;
  MySQLDriver *driver;
};

class MySQLUnionAllNode : public MySQLInnerPipeNode {
 public:
  MySQLUnionAllNode(ExecutePlan *plan, vector<ExecuteNode *> &nodes);
  void handle_children();
  Packet *get_error_packet();

 private:
  void check_union_columns();
  Packet error_packet;
  bool got_error;
  uint64_t column_size;
  vector<ExecuteNode *> nodes;
};

class MySQLUnionGroupNode : public MySQLExecuteNode {
 public:
  MySQLUnionGroupNode(ExecutePlan *plan, vector<list<ExecuteNode *> > &nodes);

  Packet *get_header_packet() {
    MySQLExecuteNode *child = (MySQLExecuteNode *)(nodes[0].front());
    return child->get_header_packet();
  }

  list<Packet *> *get_field_packets() {
    MySQLExecuteNode *child = (MySQLExecuteNode *)(nodes[0].front());
    return child->get_field_packets();
  }

  Packet *get_eof_packet() {
    MySQLExecuteNode *child = (MySQLExecuteNode *)(nodes[0].front());
    return child->get_eof_packet();
  }

  Packet *get_end_packet() {
    LOG_DEBUG("Node %@ start to get end packet.\n", this);
    MySQLExecuteNode *child = (MySQLExecuteNode *)(nodes[0].front());
    return child->get_end_packet();
  }

  Packet *get_error_packet();
  void execute();
  void children_execute();
  bool notify_parent();
  void wait_children();
  void init_row_map();
  void clean();
  void handle_children();
  void handle_child(MySQLExecuteNode *child);

  void set_children_thread_status_start(unsigned int group_num);

  void handle_error_all_children();

 private:
  void do_add_child(ExecuteNode *child) {
    children.push_back((MySQLExecuteNode *)child);
  }
  void do_get_children(list<ExecuteNode *> &children) {
    list<MySQLExecuteNode *>::iterator it = this->children.begin();
    for (; it != this->children.end(); it++) {
      children.push_back(*it);
    }
  }
  void check_union_columns();

  list<MySQLExecuteNode *> children;
  vector<list<ExecuteNode *> > nodes;
  unsigned int finished_group_num;
  uint64_t column_size;
  bool got_error;
  Packet error_packet;
};

class MySQLModifyNode : public MySQLExecuteNode, public BackendThreadTask {
 public:
  MySQLModifyNode(ExecutePlan *plan, DataSpace *dataspace);

  bool is_finished() {
    return (ready_rows->empty() && status == EXECUTE_STATUS_COMPLETE) ||
           thread_status == THREAD_STOP;
  }

  void clean();

  void handle_error_packet(Packet *packet);

  int svc();

  void execute();

  Packet *get_error_packet() { return error_packet; }

  void deal_error_packet() {
    MySQLErrorResponse response(error_packet);
    if (response.is_shutdown()) {
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      session->set_server_shutdown(true);
    }
    if (conn) {
      if (!plan->get_migrate_tool())
        handler->put_back_connection(dataspace, conn);
      conn = NULL;
    }
    handle_auth_space_error();
    throw ErrorPacketException();
  }

  void handle_auth_space_error() {
    Backend *backend = Backend::instance();
    if (dataspace == backend->get_auth_data_space()) {
      session->get_status()->item_inc(TIMES_FAIL_STORE_METADATA);
      throw ExecuteNodeError(
          "Operation fail on auth space, plz contact"
          " the admin to check the possible in-consistence"
          " data in the auth space.");
    }
  }

  void throw_error_packet() {
    if (error_packet) {
      deal_error_packet();
    } else {
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      handle_auth_space_error();
      throw ExecuteNodeError(
          "ModifyNode got unexpected error during execution.");
    }
  }

  bool notify_parent();
  void add_sql(const char *stmt_sql, size_t len = 0,
               bool need_shard_parse = false);
  void add_sql_during_exec(const char *stmt_sql, size_t len = 0);

 private:
  string sql;
  list<Packet *> ok_packets;
  BackendThreadStatus thread_status;
  BackendThread *bthread;
  ACE_Thread_Mutex mutex;
  ACE_Thread_Mutex sql_mutex;
  ACE_Condition<ACE_Thread_Mutex> cond_sql;
  ACE_Condition<ACE_Thread_Mutex> cond_notify;
  list<string *> sql_list;
  string execute_sql;
  bool got_error;
  Packet *error_packet;
  Connection *conn;
  Statement *new_stmt_for_shard;
};

class MySQLModifyLimitNode : public MySQLInnerNode {
 public:
  MySQLModifyLimitNode(ExecutePlan *plan, record_scan *rs,
                       PartitionedTable *table, vector<unsigned int> &par_ids,
                       unsigned int limit, string &sql);
  virtual void clean();

 private:
  struct LinkedHashMap {
    typedef list<pair<MySQLModifyNode *, unsigned int> >::iterator iterator;

    inline iterator begin() { return fifo_list.begin(); }

    inline iterator end() { return fifo_list.end(); }

    inline size_t size() { return fifo_list.size(); }

    inline bool empty() { return fifo_list.empty(); }

    void insert(MySQLModifyNode *node, unsigned int num) {
      fifo_list.push_back(pair<MySQLModifyNode *, unsigned int>(node, num));
      iterator iter = --fifo_list.end();
      hash_map[node] = iter;
    }

    void erase(MySQLModifyNode *node) {
      boost::unordered_map<MySQLModifyNode *, iterator>::iterator map_iter;
      if ((map_iter = hash_map.find(node)) != hash_map.end()) {
        fifo_list.erase(map_iter->second);
        hash_map.erase(map_iter);
      }
    }

    bool find(MySQLModifyNode *node, unsigned int &num) {
      boost::unordered_map<MySQLModifyNode *, iterator>::iterator map_iter;
      if ((map_iter = hash_map.find(node)) != hash_map.end()) {
        num = map_iter->second->second;
        return true;
      } else
        return false;
    }

   private:
    list<pair<MySQLModifyNode *, unsigned int> > fifo_list;
    boost::unordered_map<MySQLModifyNode *, iterator> hash_map;
  };

  void child_add_sql(MySQLModifyNode *node, const char *sql, size_t len) const {
    node->add_sql_during_exec(sql, len);
  }
  inline void table_sql_for_par(PartitionedTable *par_table, unsigned int id,
                                string &sql) const;
  inline DataSpace *table_get_par_dataspace(PartitionedTable *par_table,
                                            unsigned int id) const;
  void modify_limit_in_sql(string &sql, unsigned int limit_num) const;
  void get_limit_start_and_end(string &sql, unsigned int &start_pos,
                               unsigned int &end_pos) const;
  void create_modify_node(LinkedHashMap &modify_nodes);
  virtual void children_modify_end();
  void execute();
  void generate_new_sql(LinkedHashMap &modify_nodes);
  void handle_children_affect_rows(LinkedHashMap &modify_nodes);
  void handle_response_packet(LinkedHashMap &modify_nodes);
  void rebuild_ok_packet();

 private:
  string sql;
  PartitionedTable *table;
  vector<unsigned int> par_ids;
  map<DataSpace *, string> modify_sqls;
  unsigned int limit;
  record_scan *rs;
  unsigned int total_affect_rows;
  unsigned int total_warning_rows;
};

class MySQLNormalMergeNode : public MySQLInnerNode {
 public:
  MySQLNormalMergeNode(ExecutePlan *plan, const char *modify_sql,
                       vector<ExecuteNode *> *nodes);

  virtual void clean();

  void get_filed_num() {
    field_num = select_node_children.front()->get_field_packets()->size();
  }

  void add_select_child(ExecuteNode *node) {
    select_node_children.push_back((MySQLExecuteNode *)node);
    node->parent = this;
  }

  void add_modify_child_for_explain_stmt(ExecuteNode *node) {
    modify_node_children_for_explain_stmt.push_back((MySQLExecuteNode *)node);
  }

  virtual void do_get_children(list<ExecuteNode *> &children) {
    list<MySQLExecuteNode *>::iterator it = this->select_node_children.begin();
    for (; it != this->select_node_children.end(); it++) {
      children.push_back(*it);
    }

    it = this->modify_node_children_for_explain_stmt.begin();
    for (; it != this->modify_node_children_for_explain_stmt.end(); it++) {
      children.push_back(*it);
    }

    it = this->children.begin();
    for (; it != this->children.end(); it++) {
      children.push_back(*it);
    }
  }

  void select_node_children_execute();
  void wait_children_select_node();

  virtual void handle_select_node_children() = 0;

  void child_add_sql(MySQLModifyNode *node, const char *sql, size_t len) {
    node->add_sql_during_exec(sql, len);
  }

  virtual void children_modify_end() {
    list<MySQLExecuteNode *>::iterator it;
    for (it = children.begin(); it != children.end(); it++) {
      child_add_sql((MySQLModifyNode *)(*it), "", 0);
    }
  }

  virtual void handle_select_complete() {
    // Add an empty sql to modify node to notify the finish.
    children_modify_end();
  }

  virtual size_t get_sql_length(string *s) { return s->length(); }

  void rotate_sql(MySQLModifyNode *node, string *sql, string *added_sql);
  bool is_sql_len_valid(size_t len) { return len <= max_sql_len; }

  void set_max_packet_size(size_t len) { max_sql_len = len; }

  bool is_sql_len_enough(size_t len) { return len >= generated_sql_length; }
  virtual void add_str_before_field_value(string *str, bool is_null) {
    ACE_UNUSED_ARG(str);
    ACE_UNUSED_ARG(is_null);
  }

  void append_field_to_sql(string *sql, int field_pos, MySQLRowResponse *row,
                           bool handle_hex = false);

  void init_column_as_str();

  void execute();
  void handle_children();
  void handle_child(MySQLExecuteNode *child);
  Packet *get_error_packet();
  void set_select_node_children_thread_status_start();

 protected:
  list<MySQLExecuteNode *> select_node_children;
  list<MySQLExecuteNode *>
      modify_node_children_for_explain_stmt;  // used for Explain SQL
  unsigned int field_num;
  string modify_sql;
  size_t max_sql_len;
  bool replace_set_value;
  vector<bool> column_as_str;
  bool init_column_flag;
  bool select_complete_handled;
  unsigned int generated_sql_length;
};

class MySQLModifySelectNode : public MySQLNormalMergeNode {
 public:
  MySQLModifySelectNode(ExecutePlan *plan, const char *modify_sql,
                        vector<string *> *columns, vector<ExecuteNode *> *nodes,
                        bool execute_quick = true,
                        bool is_replace_set_value = false);

  void clean();

  void handle_select_complete() {
    if (!execute_quick) {
      LOG_DEBUG("modify_sql :  %s  execute_sql :[%s].\n", modify_sql.c_str(),
                execute_sql.c_str());
      if (replace_set_value) {
        child_add_sql((MySQLModifyNode *)children.front(), execute_sql.c_str(),
                      execute_sql.size());
      } else if (execute_sql.size() > modify_sql.size())
        child_add_sql((MySQLModifyNode *)children.front(), execute_sql.c_str(),
                      get_sql_length(&execute_sql));
    }

    // Add an empty sql to modify node to notify the finish.
    children_modify_end();
  }

  void handle_select_node_child(MySQLExecuteNode *child, string *sql);
  void handle_select_node_children();
  size_t get_sql_length(string *s) { return s->length() - 3; }
  void add_str_before_field_value(string *str, bool is_null) {
    if (is_null)
      str->append(" is ");
    else
      str->append("=");
  }
  void rotate_modify_sql(string *sql, string *added_sql);
  MySQLModifyNode *get_next_migrate_node();

 private:
  list<MySQLExecuteNode *>::iterator migrate_iter;
  vector<string *> *columns;
  bool execute_quick;
  string execute_sql;
};

class MySQLInsertSelectNode : public MySQLNormalMergeNode {
 public:
  MySQLInsertSelectNode(ExecutePlan *plan, const char *modify_sql,
                        vector<ExecuteNode *> *nodes);

  void handle_select_node_child(MySQLExecuteNode *child, string *sql);
  void handle_select_node_children();
  size_t get_sql_length(string *s) { return s->length() - 1; }
  void handle_select_complete() {
    if (modify_sql_exe.size() > modify_sql.size())
      child_add_sql((MySQLModifyNode *)children.front(), modify_sql_exe.c_str(),
                    get_sql_length(&modify_sql_exe));
#ifdef DEBUG
    LOG_DEBUG("The insert select sql size is [%d].\n", generated_sql_length);
#endif
    children_modify_end();
  }
  void rotate_insert_sql(string *sql, string *added_sql);
  MySQLModifyNode *get_next_migrate_node();

 private:
  string modify_sql_exe;
  list<MySQLExecuteNode *>::iterator migrate_iter;
  uint64_t select_row_num;
  uint64_t cross_join_max_rows;
};

class MySQLPartitionMergeNode : public MySQLNormalMergeNode {
 public:
  MySQLPartitionMergeNode(ExecutePlan *plan, const char *modify_sql,
                          vector<ExecuteNode *> *nodes,
                          PartitionedTable *par_table, PartitionMethod *method);

 protected:
  unsigned int get_par_id_one_row(MySQLRowResponse *row);
  unsigned int get_par_id_one_row(MySQLRowResponse *row, int auto_inc_key_pos,
                                  int64_t curr_auto_inc_val);
  MySQLModifyNode *get_partiton_modify_node(unsigned int id) {
    if (!modify_node_map.count(id)) add_partition_modify_node(id);
    return modify_node_map[id];
  }
  void add_partition_modify_node(unsigned int id) {
    DataSpace *space = par_table->get_partition(id);
    MySQLModifyNode *modify = new MySQLModifyNode(this->plan, space);
    modify_node_map[id] = modify;
    node_sql_map[id].append(modify_sql.c_str());
    row_map[(MySQLExecuteNode *)modify] =
        new AllocList<Packet *, Session *, StaticAllocator<Packet *> >();
    row_map[(MySQLExecuteNode *)modify]->set_session(plan->session);
    add_child(modify);
    modify->execute();
  }
  void children_add_sql();

 protected:
  PartitionedTable *par_table;
  PartitionMethod *method;
  /*map store the node id and the related modify node.*/
  map<unsigned int, MySQLModifyNode *> modify_node_map;
  /*map store the node id and the sql should be applied on this node.*/
  map<unsigned int, string> node_sql_map;

  vector<unsigned int> key_pos_vec;
  uint64_t select_row_num;
  uint64_t cross_join_max_rows;
};

class MySQLEstimateSelectNode : public MySQLExecuteNode {
 public:
  MySQLEstimateSelectNode(ExecutePlan *plan, DataSpace *dataspace,
                          const char *sql, Statement *statement);
  MySQLEstimateSelectNode(ExecutePlan *plan, vector<unsigned int> *par_ids,
                          PartitionedTable *par_table, const char *sql,
                          Statement *statement);

  void clean();
  Packet *get_error_packet() { return error_packet; }
  void execute();

 private:
  void handle_error_packet(Packet *packet);
  void get_estimate_sql();
  void send_estimate_result_to_client(int rows);
  int fetch_affacted_rows(Connection *conn);
  int send_to_server_estimate();
  void generate_select_sql();

  const char *sql;
  stmt_node *new_stmt;
  bool got_error;
  Packet *packet;
  Packet *error_packet;
  vector<unsigned int> *par_ids;
  PartitionedTable *par_table;
  string exec_sql;
  const char *schema;
  Statement *statement;
};

class MySQLPartitionModifySelectNode : public MySQLPartitionMergeNode {
 public:
  MySQLPartitionModifySelectNode(ExecutePlan *plan, const char *modify_sql,
                                 vector<ExecuteNode *> *nodes,
                                 PartitionedTable *par_table,
                                 PartitionMethod *method,
                                 vector<string *> *columns,
                                 bool execute_quick = true);
  void clean() {
    MySQLPartitionMergeNode::clean();
    vector<string *>::iterator it = columns->begin();
    for (; it != columns->end(); it++) {
      delete *it;
    }
    columns->clear();
    delete columns;
  }

  void handle_select_complete() {
    if (!execute_quick) children_add_sql();
    // Add an empty sql to modify node to notify the finish.
    children_modify_end();
  }

  void handle_select_node_child(MySQLExecuteNode *child);
  void handle_select_node_children();
  void add_str_before_field_value(string *str, bool is_null) {
    if (is_null)
      str->append(" is ");
    else
      str->append("=");
  }

 protected:
  // get the identify key columns pos in the result of select_part
  void fulfill_key_pos_one_row(vector<unsigned int> *pos) {
    unsigned int par_key_num = par_table->get_key_num();
    unsigned int i = 0;
    // By default, the first par_key_num columns of row are the value of
    // parititon keys
    for (; i < par_key_num; i++) pos->push_back(i);
  }
  size_t get_sql_length(string *s) {
    // remove the redundant " OR" from the tail of generated sql
    return s->length() - 3;
  }

 private:
  vector<string *> *columns;
  bool execute_quick;
};

class MySQLPartitionInsertSelectNode : public MySQLPartitionMergeNode {
 public:
  MySQLPartitionInsertSelectNode(
      ExecutePlan *plan, const char *modify_sql, vector<ExecuteNode *> *nodes,
      PartitionedTable *par_table, PartitionMethod *method,
      vector<unsigned int> &key_pos, const char *schema_name,
      const char *table_name, bool is_duplicated = false);
  void handle_select_node_child(MySQLExecuteNode *child);
  void handle_select_node_child_duplicated(MySQLExecuteNode *child);
  void handle_select_node_children();

  void handle_select_complete() {
    if (plan->statement->get_auto_inc_status() != NO_AUTO_INC_FIELD) {
      if (stmt_lock) stmt_lock->release();
    }
    children_add_sql();
    // Add an empty sql to modify node to notify the finish.
    children_modify_end();
  }

 protected:
  size_t get_sql_length(string *s) {
    // remove the redundant "," from the tail of generated sql
    return s->length() - 1;
  }
  unsigned int get_part_id(MySQLRowResponse *row);
  void build_value_list_str(string &str, MySQLRowResponse *row);

 private:
  int auto_increment_key_pos;
  int64_t curr_auto_inc_val;
  string schema_name_insert;
  string table_name_insert;
  bool need_update_last_insert_id;
  bool need_auto_inc_lock;
  map<int, string> modify_sql_vector;
  ACE_Thread_Mutex *stmt_lock;

  int64_t get_auto_increment_value(MySQLRowResponse *row);
  void update_last_insert_id();
  void auto_inc_prepare();

  bool is_duplicated;
  bool has_init_duplicated_modify_node;
};

typedef void (*aggregate_handler)(
    Packet *packet /*packet to return*/,
    Packet *field_packet /* field packet refer to current column */,
    vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
    MySQLColumnType column_type);

void handle_max(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type);
void handle_certain_max(Packet *packet, Packet *field_packet,
                        vector<MySQLRowResponse *> *mysql_rows,
                        unsigned column_index, MySQLColumnType column_type);
void handle_min(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type);
void handle_sum(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type);
void handle_count(Packet *packet, Packet *field_packet,
                  vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                  MySQLColumnType column_type);
void handle_normal(Packet *packet, Packet *field_packet,
                   vector<MySQLRowResponse *> *mysql_rows,
                   unsigned column_index, MySQLColumnType column_type);

void init_column_handlers(unsigned int column_num,
                          list<AggregateDesc> &aggregate_desc,
                          vector<aggregate_handler> &column_handlers,
                          bool is_certain = false);

class MySQLAggregateNode : public MySQLInnerNode {
 public:
  MySQLAggregateNode(ExecutePlan *plan, list<AggregateDesc> &aggregate_desc);

  void init_mysql_rows();
  void handle();
  void execute();

 private:
  virtual void do_clean();

 private:
  Packet *result_packet;
  unsigned int column_num;
  vector<MySQLRowResponse *> mysql_rows;
  vector<MySQLColumnType> column_types;
  list<AggregateDesc> aggregate_desc;
  vector<aggregate_handler> column_handlers;
  unsigned int max_row_size;
};

class MySQLWiseGroupNode : public MySQLSortNode {
 public:
  MySQLWiseGroupNode(ExecutePlan *plan, list<SortDesc> *group_desc,
                     list<AggregateDesc> &aggregate_desc);

  bool add_to_group(Packet *packet);
  virtual Packet *merge_group();
  virtual void group_by();
  void handle_before_complete();
  void handle();
  void execute();

 protected:
  void do_clean();

 protected:
  list<Packet *, StaticAllocator<Packet *> > *ready_nodes;
  list<AggregateDesc> aggregate_desc;
  vector<MySQLRowResponse *> group_rows;
  bool init_column_handler_flag;
  vector<aggregate_handler> column_handlers;
  unsigned int max_row_size;
  unsigned int group_size;
  unsigned int max_group_buffer_rows;
  unsigned int max_group_buffer_rows_size;
};

class MySQLPagesNode : public MySQLWiseGroupNode {
 public:
  MySQLPagesNode(ExecutePlan *plan, list<SortDesc> *group_desc,
                 list<AggregateDesc> &aggregate_desc, int page_size);

  Packet *merge_group();
  void group_by();

 private:
  int page_size;
  int count_index;
  unsigned long rownum;
};

class MySQLGroupNode : public MySQLSortNode {
 public:
  MySQLGroupNode(ExecutePlan *plan, list<SortDesc> *sort_desc,
                 list<AggregateDesc> &aggregate_desc);

  bool add_to_group(Packet *packet);
  Packet *merge_group();
  void group_by();
  void handle_before_complete();
  void handle();
  void execute();

 private:
  virtual void do_clean();

 private:
  AllocList<Packet *, Session *, StaticAllocator<Packet *> > ready_nodes;
  list<AggregateDesc> aggregate_desc;
  vector<MySQLRowResponse *> group_rows;
  bool init_column_handler_flag;
  vector<aggregate_handler> column_handlers;
  unsigned int max_row_size;
};

class MySQLSingleSortNode : public MySQLSortNode {
 public:
  MySQLSingleSortNode(ExecutePlan *plan, list<SortDesc> *sort_desc);

  void execute();
  void init_heap();
  void max_heapify(unsigned int i);
  void heap_insert(MySQLExecuteNode *node);
  void handle_before_complete();
  void sort();

 private:
  void do_clean();
  unsigned int heap_size;
  unsigned int max_single_sort_buffer_rows;
  unsigned int max_single_sort_buffer_rows_size;
  vector<Packet *> rows_heap;
  AllocList<Packet *, Session *, StaticAllocator<Packet *> > ready_nodes;
};

class MySQLLoadDataInfileExternal : public MySQLExecuteNode {
 public:
  MySQLLoadDataInfileExternal(ExecutePlan *plan, DataSpace *dataspace,
                              DataServer *dataserver);

  void execute();
  void clean() {}
  Packet *get_error_packet() { return NULL; }

 private:
  DataServer *dataserver;
};

class MySQLLoadLocalExternal : public MySQLExecuteNode {
 public:
  MySQLLoadLocalExternal(ExecutePlan *plan, DataSpace *dataspace,
                         DataServer *dataserver, const char *sql);

  void execute();
  void clean();
  Packet *get_error_packet() { return NULL; }

 private:
  void build_command();
  void spawn_command();
  void send_file_request_to_client();

 private:
  DataServer *dataserver;
  const char *sql;
  spawn_param param;
  ACE_thread_t t_id;
  ACE_hthread_t t_handle;
  FILE *fd;
  ofstream file;
  char fifo_name[30];
  string load_local_external_cmd;
};

class MySQLKillNode : public MySQLExecuteNode {
 public:
  MySQLKillNode(ExecutePlan *plan, int cluster_id, uint32_t kid);

  void execute();
  void clean() {}
  Packet *get_error_packet() { return NULL; }

 private:
  bool handle_federated_situation(Session *h);
  uint32_t kid;
  int cluster_id;
};

class MySQLLoadLocalNode : public MySQLExecuteNode {
 public:
  MySQLLoadLocalNode(ExecutePlan *plan, DataSpace *dataspace, const char *sql);

  void execute();
  void clean() {}
  Packet *get_error_packet() { return NULL; }

 private:
  const char *sql;
  load_op_node *load_data_node;
  string field_terminate;
  char field_escape;
  char field_enclose;
  string line_terminate;
  string line_starting;
  bool has_field_enclose;
  bool has_line_starting;
  vector<Packet *> *warning_packet_list;
  bool has_add_warning_packet;
  uint64_t warning_packet_list_size;
  uint64_t warning_count;
  uint64_t affected_row_count;
};

class MySQLLoadDataInfile {
 public:
  MySQLLoadDataInfile();
  ~MySQLLoadDataInfile();
  void build_error_packet(Packet &packet, const char *file);
  void sql_add_local(ExecutePlan *plan, const char *sql);
  bool open_local_file(const char *file_path);
  void init_file_buf();
  void read_file_into_buf(Packet &buffer);
  void init_file_buf_for_load_insert();
  void read_file_into_buf_for_load_insert(Packet &buffer);
  void file_close() { filestr.close(); }

 private:
  ifstream filestr;
  filebuf *pbuf;
  uint64_t size;
  uint64_t size_remain;
  uint64_t max_size;
  char *data_buffer;

 public:
  string *new_sql;
  uint64_t size_temp;
  load_op_node *load_data_node;
  string field_terminate;
  char field_escape;
  char field_enclose;
  string line_terminate;
  string line_starting;
  bool has_field_enclose;
  bool has_line_starting;
  vector<Packet *> *warning_packet_list;
  bool has_add_warning_packet;
  uint64_t warning_packet_list_size;
  uint64_t warning_count;
  uint64_t affected_row_count;
};

class MySQLLoadDataInfileNode : public MySQLExecuteNode,
                                public MySQLLoadDataInfile {
 public:
  MySQLLoadDataInfileNode(ExecutePlan *plan, DataSpace *dataspace,
                          const char *sql);
  void execute();
  void clean() {
    file_close();
    delete MySQLLoadDataInfile::new_sql;
    if (error_packet) delete error_packet;
    error_packet = NULL;
  }
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *error_packet;
  const char *sql;
};

class MySQLLoadLocalPartTableNode;
class MySQLLoadLocalPartitionNode : public MySQLExecuteNode,
                                    public ACE_Task<ACE_MT_SYNCH> {
  enum { SENDING_QUERY, REQUESTING_FILE, FETCHED_RESULT } step;

 public:
  MySQLLoadLocalPartitionNode(ExecutePlan *plan, MySQLExecuteNode *table_node,
                              DataSpace *dataspace, const char *sql);

  int svc();
  void execute();
  bool notify_parent();

  void pack_packet(Packet *packet, string data, unsigned int len);

  bool requesting_file() {
    if (exception == boost::exception_ptr()) return step == REQUESTING_FILE;
    rethrow_exception(exception);
    return false;
  }
  bool fetched_result() {
    if (exception == boost::exception_ptr()) return step == FETCHED_RESULT;
    rethrow_exception(exception);
    return false;
  }
  void check_exception() {
    if (exception == boost::exception_ptr()) return;
    rethrow_exception(exception);
  }
  void clean();
  Packet *get_result() { return &result; }
  Packet *get_error_packet() { return NULL; }

  void add_packet_to_ready_list(Packet *p);

  bool rotate_ready_list();

  Packet *get_packet_from_ready_list();

  void stop_execute();

  bool check_stop() {
    // assume that the full_lock mutex has been acquired by the invoker
    if (is_stop || ACE_Reactor::instance()->reactor_event_loop_done())
      return true;
    return false;
  }
  void wake_up_parent_is_waiting() {
    full_lock.acquire();
    if (parent_is_waiting) full_cond.signal();
    full_lock.release();
  }

  void set_table_node_has_get_empty_packet(bool b) {
    full_lock.acquire();
    table_node_has_get_empty_packet = b;
    if (ready_packets1.empty() && ready_packets2.empty()) {
      ready_to_finish = true;
    }
    full_lock.release();
  }

  bool get_ready_to_finish() { return ready_to_finish; }

  bool get_task_is_finish() { return task_is_finish; }

  void wait_ready_to_finish() {
    full_lock.acquire();
    if (!ready_to_finish) {
      parent_is_waiting = true;
      full_cond.wait();
      parent_is_waiting = false;
    }
    full_lock.release();
  }

  void set_max_ready_packets_local(unsigned int i) {
    max_ready_packets_local = i;
  }

  uint64_t get_warning_count() { return warning_count; }
  uint64_t get_affected_row_count() { return affected_row_count; }
  vector<Packet *> *get_buffered_warning_packet() {
    return warning_packet_list;
  }

 private:
  vector<Packet *> *warning_packet_list;
  bool has_add_warning_packet;
  uint64_t warning_packet_list_size;
  uint64_t warning_count;
  uint64_t affected_row_count;

  ExecuteNode *table_node;
  const char *sql;
  Packet result;
  boost::exception_ptr exception;

  list<Packet *> *ready_packets;
  list<Packet *> *using_packets;
  list<Packet *> ready_packets1;
  list<Packet *> ready_packets2;
  size_t ready_packet_size;
  /*lock and cond used to let the parent node to wait the child node to send
   * the ready packets, when the ready packets are too many.*/
  ACE_Thread_Mutex full_lock;
  ACE_Condition<ACE_Thread_Mutex> full_cond;
  bool parent_is_waiting;
  bool child_is_waiting;
  bool is_stop;
  bool table_node_has_get_empty_packet;
  bool ready_to_finish;
  bool task_is_finish;
  unsigned int max_ready_packets_local;
};

class MySQLLoadPacketAnalysisNode;
class MySQLLoadLocalPartTableNode : public MySQLExecuteNode {
 public:
  MySQLLoadLocalPartTableNode(ExecutePlan *plan, PartitionedTable *dataspace,
                              const char *sql, const char *_schema_name,
                              const char *_table_name);

  void execute();
  virtual void clean();
  Packet *get_error_packet();

  void signal() {
    ACE_GUARD_REACTION(
        ACE_Thread_Mutex, guard, lock,
        LOG_ERROR("%p\n", "acquire LOAD DATA partitioned table lock");
        throw HandlerError("acquire LOAD DATA partitioned table failed"));
    cond.signal();
  }
  void aquire_lock() {
    if (lock.acquire()) {
      LOG_ERROR("%p\n", "acquire LOAD DATA partitioned table lock");
      throw HandlerError("acquire LOAD DATA partitioned table failed");
    }
  }
  void release_lock() { lock.release(); }
  virtual Packet *read_one_packet();
  void add_to_ready_packets_from_Analysis(unsigned int part_id, Packet *packet);
  string get_field_terminate() { return field_terminate; }
  char get_field_escape() { return field_escape; }
  char get_field_enclose() { return field_enclose; }
  string get_line_terminate() { return line_terminate; }
  string get_line_starting() { return line_starting; }
  bool get_has_field_enclose() { return has_field_enclose; }
  bool get_has_line_starting() { return has_line_starting; }
  vector<unsigned int> *get_key_pos_vec() { return &key_pos_vec; }
  const char *get_schema_name() { return schema_name; }
  const char *get_table_name() { return table_name; }
  bool get_need_update_last_insert_id() { return need_update_last_insert_id; }
  int get_auto_inc_key_pos() { return auto_inc_key_pos; }
  PartitionMethod *get_partition_method() { return partition_method; }
  string &get_full_table_name() { return full_table_name; }
  bool get_auto_inc_at_last_field() { return auto_inc_at_last_field; }
  unsigned int get_table_fields_num() { return table_fields_num; }
  PartitionedTable *get_part_space() { return part_space; }
  Statement *get_stmt() { return stmt; }
  unsigned int get_max_analysis_wait_size_local() {
    return max_analysis_wait_size_local;
  }
  bool is_column_list_has_append_auto_inc() const {
    return sql_with_auto_inc_name != NULL;
  }
  bool is_sql_specify_column_list() const { return sql_specify_column_list; }
  bool is_table_has_auto_inc_column() const {
    return table_has_auto_inc_column;
  }
  unsigned long get_max_packet_size() { return max_packet_size; }
  unsigned int get_specified_column_count() { return specified_column_count; }
  bool get_load_data_strict_mode_this_stmt() {
    return load_data_strict_mode_this_stmt;
  }
  void eat_all_packet_from_client();
  void add_empty_pkt_to_analysis_node();

  void acquire_analysis_lock() {
    if (analysis_num > 1) analysis_lock.acquire();
  }
  void release_analysis_lock() {
    if (analysis_num > 1) analysis_lock.release();
  }

  void wait_analysis_sync_cond() {
    analysis_sync_lock.acquire();
    analysis_sync_cond.wait();
    analysis_sync_lock.release();
  }

  void wakeup_analysis_sync_cond() {
    analysis_sync_lock.acquire();
    analysis_sync_cond.signal();
    analysis_sync_lock.release();
  }
  void wakeup_analysis_sync_cond_for_feed() {
    analysis_sync_lock.acquire();
    need_to_feed_analysis = true;
    analysis_sync_cond.signal();
    analysis_sync_lock.release();
  }

  void wakeup_analysis_sync_for_fin() {
    analysis_sync_lock.acquire();
    analysis_finish_num++;
    analysis_sync_cond.signal();
    analysis_sync_lock.release();
  }

  bool wait_and_check_analysis();

  void fill_packets_to_analysis();

  bool is_has_end_send_data() { return has_end_send_data; }

  size_t is_first_row_complete(Packet *packet);

 protected:
  void clean_up_analysis_nodes();
  void add_packet_to_child(MySQLLoadLocalPartitionNode *child, Packet *packet);
  void set_schema_name(const char *_schema_name) {
    copy_string(schema_name, _schema_name, sizeof(schema_name));
  }
  void set_table_name(const char *_table_name) {
    copy_string(table_name, _table_name, sizeof(table_name));
  }

  void set_sql(const char *sql) { this->sql = sql; }
  const char *get_sql() { return sql; }
  Partition *get_partition(int num) {
    return (static_cast<PartitionedTable *>(dataspace))->get_partition(num);
  }
  void flush_ready_packets_to_child();

  void flush_all();
  virtual void send_query();
  void wait_children();
  virtual void request_file_and_send_data_to_server();
  void report_result_to_client();
  unsigned int get_table_fields(string schema_name, string table_name);

  const char *sql;
  string *sql_with_auto_inc_name;
  bool sql_specify_column_list;
  unsigned int specified_column_count;
  bool load_data_strict_mode_this_stmt;
  bool table_has_auto_inc_column;
  ACE_Thread_Mutex lock;
  ACE_Condition<ACE_Thread_Mutex> cond;
  int partition_num;
  load_op_node *load_data_node;
  string field_terminate;
  char field_escape;
  char field_enclose;
  string line_terminate;
  string line_starting;
  bool has_field_enclose;
  bool has_line_starting;
  vector<unsigned int> key_pos_vec;
  vector<MySQLLoadLocalPartitionNode *> partition_nodes;
  char schema_name[125];
  char table_name[125];
  bool need_update_last_insert_id;
  int auto_inc_key_pos;
  PartitionMethod *partition_method;
  Statement *stmt;
  PartitionedTable *part_space;
  unsigned int table_fields_num;
  bool has_not_support_terminate;
  string full_table_name;

  bool auto_inc_at_last_field;

  vector<list<Packet *> *> ready_packets;
  ACE_Thread_Mutex ready_packet_lock;
  ACE_Thread_Mutex analysis_lock;
  size_t analysis_num;
  vector<MySQLLoadPacketAnalysisNode *> analysis_vec;

  ACE_Thread_Mutex analysis_sync_lock;
  ACE_Condition<ACE_Thread_Mutex> analysis_sync_cond;
  bool has_got_empty_packet;
  bool need_to_feed_analysis;
  bool has_end_send_data;
  size_t analysis_finish_num;
  /*Global loop count for
   * MySQLLoadLocalPartTableNode::fill_packets_to_analysis, which indicate
   * that it is turn to which analysis node to be filled*/
  size_t analysis_loop_count;
  /*Indicate the last filled analysis node, it may not "analysis_loop_count-1"
   * in some case, if some analysis is full and so need to fill in this loop.
   * */
  size_t last_added_analysis_node_pos;

  unsigned int max_ready_packets_local;
  unsigned int max_analysis_wait_size_local;
  unsigned long max_packet_size;
};

struct handle_send_file_data_params {
  handle_send_file_data_params() {
    packet_data_start_pos = NULL;
    sub_str_pos_line_term = string::npos;
    sub_str_pos_field_term = string::npos;
    field_end_pos = NULL;
    enclose_field_end_spec = false;
    need_handle_line_term = false;
    need_handle_field_term = false;
    is_first_part_enough = false;
  }
  const char *packet_data_start_pos;
  string::size_type sub_str_pos_line_term;
  string::size_type sub_str_pos_field_term;
  const char *field_end_pos;
  bool enclose_field_end_spec;
  bool need_handle_line_term;
  bool need_handle_field_term;
  bool is_first_part_enough;
};

struct handle_load_local_params {
  handle_load_local_params() {
    pos = NULL;
    packet_data_start_pos = pos;
    line_term_len = 0;
    field_term_len = 0;
    sub_str_pos_line_term = string::npos;
    sub_str_pos_field_term = string::npos;
    field_end_pos = NULL;
    enclose_field_end_spec = false;
    need_handle_line_term = false;
    need_handle_field_term = false;
    is_first_part_enough = false;
  }
  const char *pos;
  const char *packet_data_start_pos;
  int line_term_len;
  int field_term_len;
  string::size_type sub_str_pos_line_term;
  string::size_type sub_str_pos_field_term;
  const char *field_end_pos;
  bool enclose_field_end_spec;
  bool need_handle_line_term;
  bool need_handle_field_term;
  bool is_first_part_enough;
};

struct handle_char_one_by_one_params {
  handle_char_one_by_one_params() {
    start = NULL;
    pos = NULL;
    end = NULL;
    packet_data_start_pos = NULL;
    is_char_escaped = false;
    need_enclose = false;
    enclose_field_end = false;
    fields_count = 0;
    line_len = 0;
    line_term_len = 0;
    field_term_len = 0;
    field_start = NULL;
    line_start = NULL;
  }
  const char *start;
  const char *pos;
  const char *end;
  const char *packet_data_start_pos;
  bool is_char_escaped;
  bool need_enclose;
  bool enclose_field_end;
  unsigned int fields_count;
  size_t line_len;
  int line_term_len;
  int field_term_len;
  const char *field_start;
  const char *line_start;
};

class MySQLLoadPacketAnalysisNode : public BackendThreadTask {
 public:
  MySQLLoadPacketAnalysisNode(MySQLLoadLocalPartTableNode *table_node,
                              MySQLDriver *driver, MySQLSession *session,
                              MySQLHandler *handler, ExecutePlan *plan)
      : auto_inc_value_local_remain_count(0),
        auto_inc_value_local(0),
        table_node(table_node),
        partition_num(0),
        key_pos_vec(NULL),
        table_fields_num(0),
        part_space(NULL),
        driver(driver),
        session(session),
        handler(handler),
        plan(plan),
        table_node_has_get_empty_packet(false),
        sync_cond(sync_lock),
        analysis_is_waiting(false),
        has_finished(false) {
    column_values.expr_list_head = NULL;
    column_values.expr_list_tail = NULL;
    column_pos_one_row = 0;
    memset(&tmp_st, 0, sizeof(stmt_node));
    auto_inc_value_local_capacity = load_data_auto_inc_local_capacity;
    waiting_size = 0;
    is_stop = false;
    thread_status = THREAD_STOP;
    bthread = NULL;
    is_analysis_last_packet = false;
    max_analysis_wait_size_local = 0;
    col_replace_null_char.clear();
    auto_inc_step = 1;
    line_buffer = NULL;
    tmp_buffer = NULL;
    previous_packet_escaped_pos_vec = NULL;
  }
  ~MySQLLoadPacketAnalysisNode();
  void fill_field_value(char *field, size_t pos_vec_size, const char *begin,
                        const char *start, const char *end, int enclose,
                        size_t len, vector<int> &pos_vec);
  char *get_field_replace_null_str(stmt_node *st, size_t len, char *field);
  Expression *get_column_expr(stmt_node *st, size_t pos_vec_size, size_t len,
                              char *field, bool need_replace_internal_nul_char,
                              char *field_replace_null);
  void add_expr_to_column_values(stmt_node *st, Expression *expr);
  void init_analysis(int partition_num);
  void send_file_data(Packet *packet);
  void add_column_value(stmt_node *st, const char *start, const char *end,
                        vector<int> &escaped_pos_vec, bool need_enclose);
  void add_column_value_shell_1(stmt_node *st, const char *start,
                                const char *end, vector<int> &escaped_pos_vec,
                                bool need_enclose);
  void add_column_value_shell_2(stmt_node *st, const char *start,
                                const char *end, vector<int> &escaped_pos_vec,
                                bool need_enclose);
  void handle_row(const char *line_start, const char *line_end);
  void flush_rows();
  void set_is_analysis_last_packet(bool b) { is_analysis_last_packet = b; }

  int svc();

  void set_table_node_has_get_empty_packet(bool b) {
    table_node_has_get_empty_packet = b;
  }
  void wait_sync_cond() { sync_cond.wait(); }
  void wakeup_sync_cond() { sync_cond.signal(); }
  void acquire_sync_lock() { sync_lock.acquire(); }
  void release_sync_lock() { sync_lock.release(); }

  bool add_packet_to_analysis(Packet *packet, bool get_empty_packet = false) {
    bool ret = false;
    acquire_sync_lock();
    waiting_packets.push_back(packet);
    waiting_size++;
    if (get_empty_packet) set_table_node_has_get_empty_packet(true);
    if (analysis_is_waiting) {
      wakeup_sync_cond();
      analysis_is_waiting = false;
    }
    if (waiting_size >= max_analysis_wait_size_local) ret = true;
    release_sync_lock();
    return ret;
  }
  size_t get_waiting_size() {
    size_t ret = 0;
    acquire_sync_lock();
    ret = waiting_size;
    release_sync_lock();
    return ret;
  }

  Packet *get_packet_from_waiting_packets();

  bool has_finish() { return has_finished; }

  void check_exception() {
    if (exception == boost::exception_ptr()) return;
    rethrow_exception(exception);
  }

  void set_is_stop() {
    sync_lock.acquire();
    is_stop = true;
    sync_cond.signal();
    sync_lock.release();
  }

  void start_thread();

  void flush_tmp_buffer();
  const char *handle_previous_line_in_tmp_buffer(Packet *packet,
                                                 size_t line_len,
                                                 const char *pos,
                                                 const char *end,
                                                 vector<int> *escaped_pos_vec);
  void save_to_previous_buff(const char *end, const char *line_start,
                             const char *field_start, unsigned int fields_count,
                             bool need_enclose, bool enclose_field_end,
                             bool is_char_escaped,
                             vector<int> *escaped_pos_vec);
  void check_tmp_buffer_tail_contain_line_term_prefix(
      handle_load_local_params &param, Packet *packet, size_t line_len);
  void check_tmp_buffer_tail_contain_field_term_prefix(
      handle_load_local_params &param, Packet *packet, size_t line_len);
  void handle_line_term_first_part_enough(
      int line_len, string::size_type sub_str_pos_line_term, const char *pos);
  void handle_line_term_first_part_not_enough(
      int line_len, int line_term_len, string::size_type sub_str_pos_line_term,
      const char *pos);
  void handle_field_term_first_part_enough(
      int line_len, string::size_type sub_str_pos_field_term, const char *pos);
  void handle_field_term_first_part_not_enough(
      int line_len, int field_term_len,
      string::size_type sub_str_pos_field_term, const char *pos);
  void handle_previous_line_char_one_by_one(
      handle_char_one_by_one_params &param, vector<int> *escaped_pos_vec);
  void handle_char_in_packet_one_by_one(handle_char_one_by_one_params &param,
                                        vector<int> *escaped_pos_vec);
  void save_current_line_status(handle_char_one_by_one_params &param,
                                vector<int> *escaped_pos_vec);
  void concatenate_one_row_from_previous_pkt_and_cur_pkt_to_handle(
      handle_char_one_by_one_params &param, vector<int> *escaped_pos_vec);
  int64_t get_auto_inc_value(const char *line_start, const char *line_end);
  int64_t check_auto_inc_value(const char *line_start, const char *line_end);

 private:
  ListExpression column_values;
  size_t column_pos_one_row;
  string col_replace_null_char;
  vector<Packet *> buffers;
  Packet *tmp_buffer;
  int auto_inc_step;
  unsigned int previous_packet_fields_count;
  vector<int> *previous_packet_escaped_pos_vec;
  bool previous_packet_is_char_escaped;
  bool unbroken_field_found;
  bool just_found_field_before_end;
  bool previous_packet_unbroken_field_found;
  bool previous_packet_just_found_field_before_end;
  bool previous_packet_need_enclose;
  bool previous_packet_enclose_field_end;
  int64_t auto_inc_value_local_remain_count;
  int64_t auto_inc_value_local;
  int auto_inc_value_local_capacity;
  MySQLLoadLocalPartTableNode *table_node;
  int partition_num;
  stmt_node tmp_st;  // for memory allocation
  char *line_buffer;

  string field_terminate;
  char field_escape;
  char field_enclose;
  string line_terminate;
  string line_starting;
  bool has_field_enclose;
  bool has_line_starting;
  vector<unsigned int> *key_pos_vec;

  const char *schema_name;
  const char *table_name;
  bool need_update_last_insert_id;
  int auto_inc_key_pos;
  PartitionMethod *partition_method;
  string full_table_name;

  bool load_data_strict_mode_this_stmt;
  bool auto_inc_at_last_field;
  bool need_append_auto_inc_column;
  bool has_set_auto_inc_status;
  bool sql_specify_column_list;
  bool table_has_auto_inc_column;
  bool column_list_has_append_auto_inc;
  unsigned int table_fields_num;
  PartitionedTable *part_space;
  Statement *stmt;

  MySQLDriver *driver;
  MySQLSession *session;
  MySQLHandler *handler;
  ExecutePlan *plan;

  bool table_node_has_get_empty_packet;
  ACE_Thread_Mutex sync_lock;
  ACE_Condition<ACE_Thread_Mutex> sync_cond;
  list<Packet *> waiting_packets;
  size_t waiting_size;
  bool analysis_is_waiting;
  bool has_finished;
  boost::exception_ptr exception;
  bool is_stop;
  BackendThreadStatus thread_status;
  BackendThread *bthread;
  bool is_analysis_last_packet;
  unsigned int max_analysis_wait_size_local;
  unsigned long max_packet_size;
};

class MySQLLoadDataInfilePartTableNode : public MySQLLoadLocalPartTableNode,
                                         public MySQLLoadDataInfile {
 public:
  MySQLLoadDataInfilePartTableNode(ExecutePlan *plan,
                                   PartitionedTable *dataspace, const char *sql,
                                   const char *schema_name,
                                   const char *table_name);
  void request_file_and_send_data_to_server();
  void send_query();
  void clean();
  Packet *read_one_packet();

 private:
  bool delete_flag;
};

class MySQLDistinctNode : public MySQLSortNode {
 public:
  MySQLDistinctNode(ExecutePlan *plan, list<int> &column_indexes);

  // TODO: improve the algorithm with bi-partitioned insertion algorithm
  void insert_row(Packet *row);
  void handle_child(MySQLExecuteNode *child);
  void handle_children();
  void execute();

 private:
  void do_clean();

 public:
  list<int> column_indexes;
  list<Packet *, StaticAllocator<Packet *> > ready_nodes;
};

class MySQLComQueryPrepareNode : public MySQLExecuteNode {
 public:
  MySQLComQueryPrepareNode(ExecutePlan *plan, DataSpace *space,
                           const char *query_sql, const char *name,
                           const char *sql);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void handle_error_packet(Packet *packet);
  Packet *packet;
  Packet *error_packet;
  /*
   * if the prepare statement is
   *   PREPARE prod FROM "INSERT INTO t VALUES(?,?)"
   * the preapre_sql = "INSERT INTO t VALUES(?,?)"
   */
  DataSpace *dataspace;
  char *prepare_sql;
  const char *prepare_name;
  const char *sql;
};
class MySQLComQueryExecPrepareNode : public MySQLExecuteNode {
 public:
  MySQLComQueryExecPrepareNode(ExecutePlan *plan, const char *name,
                               var_item *var_item_list);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void handle_execute_prepare(MySQLPrepareItem *prepare_item,
                              string &query_sql);
  Packet *packet;
  Packet *error_packet;
  const char *prepare_name;
  var_item *parameters;
};

class MySQLComQueryDropPrepareNode : public MySQLExecuteNode {
 public:
  MySQLComQueryDropPrepareNode(ExecutePlan *plan, DataSpace *dataspace,
                               const char *name, const char *prepare_sql);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
  const char *prepare_name;
  const char *drop_prepare_sql;
  DataSpace *dataspace;
};

class MySQLDynamicRemoveSlaveNode : public MySQLExecuteNode {
 public:
  MySQLDynamicRemoveSlaveNode(
      ExecutePlan *plan,
      dynamic_remove_slave_op_node *dynamic_remove_slave_oper);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }
  void do_delay_remove_slave();
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

 private:
  void check_config();
  void dynamic_remove_slave();
  void rollback();
  const char *master_name;
  const char *slave_name;
  const char *slave_server_name;
  bool is_force;
  Packet *packet;
  Packet *error_packet;
  DataSource *target_ds;
  DataSource *slave_ds;
};

class MySQLDynamicRemoveSchemaNode : public MySQLExecuteNode {
 public:
  MySQLDynamicRemoveSchemaNode(ExecutePlan *plan, const char *schema_name,
                               bool is_force);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  string schema_name;
  bool is_force;
  Packet *packet;
  Packet *error_packet;
};

class MySQLDynamicRemoveTableNode : public MySQLExecuteNode {
 public:
  MySQLDynamicRemoveTableNode(ExecutePlan *plan, const char *table_name,
                              bool is_force);

  void clean();
  virtual void execute();
  Packet *get_error_packet() { return error_packet; }
  bool check_space_has_tables(Table *table);
  bool check_table_exists();
  void sync_topic(char *param);

 protected:
  string table_name;
  bool is_force;
  Packet *packet;
  Packet *error_packet;
  string t_name;
  string s_name;
  int mul_topic_type;
};

class MySQLDynamicChangeTableSchemeNode : public MySQLDynamicRemoveTableNode {
 public:
  MySQLDynamicChangeTableSchemeNode(ExecutePlan *plan, const char *table_name,
                                    const char *scheme_name);
  void execute();

 private:
  string scheme_name;
};

class MySQLDynamicRemoveOPNode : public MySQLExecuteNode {
 public:
  MySQLDynamicRemoveOPNode(ExecutePlan *plan, const char *server_name);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void dynamic_remove_server();
  void dynamic_remove_source();
  void dynamic_remove_partition_scheme();
  bool contain_unclean_table(const char *op_name);
  const char *op_name;
  Packet *error_packet;
};

class MySQLDynamicAddDataServerNode : public MySQLExecuteNode {
 public:
  MySQLDynamicAddDataServerNode(
      ExecutePlan *plan,
      dynamic_add_data_server_op_node *dynamic_add_data_server_oper);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void check_config();
  void check_dbscale_server_host();
  void dynamic_init_data_server();
  void dynamic_add_data_server();
  Packet *packet;
  Packet *error_packet;
  const char *server_name;
  const char *server_user;
  const char *server_password;
  const char *server_host;
  const char *server_alias_host;
  const char *remote_user;
  const char *remote_password;
  const char *local_load_script;
  const char *external_load_script;
  DataServer *data_server;
  int server_port;
  int remote_port;
  int location_id;
  bool is_master_backup;
  bool is_external_load;
  bool mgr_server;
  bool dbscale_server;
  string dbscale_server_host;
  int dbscale_server_port;
  vector<string> dbscale_host_ip_vec;
};

class MySQLDynamicAddSlaveNode : public MySQLExecuteNode {
 public:
  MySQLDynamicAddSlaveNode(ExecutePlan *plan,
                           dynamic_add_slave_op_node *dynamic_add_slave_oper);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void check_config();
  void dynamic_add_slave();
  void rollback();
  Packet *packet;
  Packet *error_packet;
  dynamic_add_data_source_server_info *server_info;
  const char *master_name;
  const char *slave_source_name;
  bool is_add_slave_server;
  RWSplitDataSource *target_ds;
  DataServer *data_server;
  DataSource *slave_source;
  DataSourceType ds_type;
};

class MySQLDynamicChangeMasterNode : public MySQLExecuteNode {
 public:
  MySQLDynamicChangeMasterNode(
      ExecutePlan *plan,
      dynamic_change_master_op_node *dynamic_change_master_oper);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
  dynamic_change_master_op_node *dynamic_change_master_oper;
  void dynamic_change_master();
};

class MySQLDynamicChangeMultipleMasterActiveNode : public MySQLExecuteNode {
 public:
  MySQLDynamicChangeMultipleMasterActiveNode(
      ExecutePlan *plan, dynamic_change_multiple_master_active_op_node *);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  const char *mm_ds_name;
  const char *mm_ds_new_active_name;
  Packet *packet;
  Packet *error_packet;
  void dynamic_change_multiple_master_active();
};

class MySQLDynamicChangeDataServerSShNode : public MySQLExecuteNode {
 public:
  MySQLDynamicChangeDataServerSShNode(ExecutePlan *plan,
                                      const char *server_name,
                                      const char *username, const char *pwd,
                                      int port);
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  const char *server_name;
  const char *username;
  const char *pwd;
  int port;
  Packet *packet;
  Packet *error_packet;
};
class MySQLShowMonitorPointStatusNode : public MySQLExecuteNode {
 public:
  MySQLShowMonitorPointStatusNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
};

class MySQLShowGlobalMonitorPointStatusNode : public MySQLExecuteNode {
 public:
  MySQLShowGlobalMonitorPointStatusNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
};

class MySQLShowHistogramMonitorPointStatusNode : public MySQLExecuteNode {
 public:
  MySQLShowHistogramMonitorPointStatusNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
};

class MySQLDBScaleCreateOutlineHintNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleCreateOutlineHintNode(ExecutePlan *plan,
                                    dbscale_operate_outline_hint_node *oper);
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }
  bool is_finished() { return status == EXECUTE_STATUS_COMPLETE; }

 private:
  Packet *error_packet;
  dbscale_operate_outline_hint_node *oper;
};

class MySQLDBScaleFlushOutlineHintNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleFlushOutlineHintNode(ExecutePlan *plan,
                                   dbscale_operate_outline_hint_node *oper);
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }
  bool is_finished() { return status == EXECUTE_STATUS_COMPLETE; }

 private:
  Packet *error_packet;
  dbscale_operate_outline_hint_node *oper;
};

class MySQLDBScaleDeleteOutlineHintNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleDeleteOutlineHintNode(ExecutePlan *plan,
                                    dbscale_operate_outline_hint_node *oper);
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }
  bool is_finished() { return status == EXECUTE_STATUS_COMPLETE; }

 private:
  Packet *error_packet;
  dbscale_operate_outline_hint_node *oper;
};

class MySQLReturnOKNode : public MySQLExecuteNode {
 public:
  MySQLReturnOKNode(ExecutePlan *plan);
  void execute();
  virtual void clean() {}
  virtual Packet *get_error_packet() { return NULL; }
  void set_ok_packet(Packet *p, bool need_set_packet_number);

 protected:
  virtual void do_execute() {}

 private:
  Packet *ok_packet;
  bool need_set_packet_number;
};

class MySQLSlaveDBScaleErrorNode : public MySQLExecuteNode {
 public:
  MySQLSlaveDBScaleErrorNode(ExecutePlan *plan) : MySQLExecuteNode(plan) {
    this->name = "MySQLSlaveDBScaleErrorNode";
  }

  virtual void clean() {}
  virtual Packet *get_error_packet() { return NULL; }
  void execute();
};

class MySQLCreateOracleSequenceNode : public MySQLReturnOKNode {
 public:
  MySQLCreateOracleSequenceNode(ExecutePlan *plan,
                                create_oracle_seq_op_node *oper);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

 protected:
  void do_execute();
  void generate_update_seq_sql(const char *schema_name, char *sql);

 private:
  Packet *error_packet;
  create_oracle_seq_op_node *oper;
};

class MySQLResetZooInfoNode : public MySQLReturnOKNode {
 public:
  MySQLResetZooInfoNode(ExecutePlan *plan);
  void do_execute();
};

class MySQLSetInfoSchemaMirrorTbStatusNode : public MySQLReturnOKNode {
 public:
  MySQLSetInfoSchemaMirrorTbStatusNode(ExecutePlan *plan,
                                       info_mirror_tb_status_node *tb_status);
  void do_execute();

 private:
  info_mirror_tb_status_node *tb_status;
};

class MySQLDBScaleFlushConfigToFileNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleFlushConfigToFileNode(ExecutePlan *plan, const char *file_name,
                                    bool flush_all);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  const char *file_name;
  bool flush_all;
  Packet *error_packet;
};

class MySQLDBScaleFlushACLNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleFlushACLNode(ExecutePlan *plan);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
};

class MySQLDBScaleFlushTableInfoNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleFlushTableInfoNode(ExecutePlan *plan, const char *schema_name,
                                 const char *table_name);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  const char *schema_name;
  const char *table_name;
};

class MySQLDBScaleGlobalConsistencePointNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleGlobalConsistencePointNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
};

class MySQLRowsNode : public MySQLInnerNode {
 public:
  MySQLRowsNode(ExecutePlan *plan, list<list<string> *> *row_list);
  void handle_child(MySQLExecuteNode *child);
  void build_row();
  void handle_children();
  void execute();

 private:
  void do_clean();
  Packet *new_row;
  list<list<string> *> *row_list;
};
class MySQLExprCalculateNode : public MySQLInnerPipeNode {
 public:
  MySQLExprCalculateNode(ExecutePlan *plan,
                         list<SelectExprDesc> &select_expr_list);
  void handle_child(MySQLExecuteNode *child);
  void init_expr_list();
  void calculate_expr(SelectExprDesc *desc);
  Packet *reset_packet_value(Packet *row);

 private:
  list<SelectExprDesc> expr_list;
  bool inited_expr_list;
  list<FieldExpression *> field_expr_list;
  unsigned int column_num;
};
class MySQLFilterNode : public MySQLInnerNode {
 public:
  MySQLFilterNode(ExecutePlan *plan) : MySQLInnerNode(plan) {}
  void handle_child(MySQLExecuteNode *child);
  void handle_children();
  void execute();

 private:
  virtual bool filter(Packet *row) = 0;
};

class MySQLRegexFilterNode : public MySQLFilterNode {
 public:
  MySQLRegexFilterNode(ExecutePlan *plan, enum FilterFlag filter_way,
                       int column_index, list<const char *> *pattern);

 private:
  bool filter(Packet *row);
  void do_clean();

 private:
  list<const char *> *pattern;
  int column_index;
  enum FilterFlag filter_way;
};

/* MySQLExprFilterNode used for filter rows (WHERE/HAVING) */
class MySQLExprFilterNode : public MySQLFilterNode {
 public:
  MySQLExprFilterNode(ExecutePlan *plan, Expression *filter_expr);

 private:
  void init_filter();
  bool filter(Packet *row);

 private:
  Expression *filter_expr;
  bool inited_filter_expr;
  bool inited_result_type;
  list<FieldExpression *> field_expr_list;
  ResultType result_type;
};

class MySQLAvgShowTableStatusNode : public MySQLInnerNode {
 public:
  virtual ~MySQLAvgShowTableStatusNode();
  Packet *get_header_packet();

  list<Packet *> *get_field_packets();

  MySQLAvgShowTableStatusNode(ExecutePlan *plan);
  void handle_child(MySQLExecuteNode *child);
  void handle_children();
  void execute();

 private:
  list<Packet *> *field_packet;
  Packet *packet;
  int is_simple;
};

class MySQLDropMulTableNode : public MySQLInnerNode {
 public:
  MySQLDropMulTableNode(ExecutePlan *plan);
  void handle_child(MySQLExecuteNode *child);
  void handle_children();
  void execute();
  void wait_children();
  Packet *get_error_packet() { return error_packet; }

 private:
  void rebuild_ok_packet();
  void build_ERROR_packet(Packet **packet);
  bool get_error_table(Packet *packet);
  void do_clean();

 private:
  uint16_t warnings;
  list<string *> error_table_list;
  Packet *error_packet;
  Packet *ok_packet;
  uint64_t affect_rows;
  bool error_flag;
};
class MySQLRenameTableNode : public MySQLInnerNode {
 public:
  MySQLRenameTableNode(ExecutePlan *plan, PartitionedTable *old_table,
                       PartitionedTable *new_table, const char *old_schema_name,
                       const char *old_table_name, const char *new_schema_name,
                       const char *new_table_name);
  void execute();

 private:
  PartitionedTable *old_table;
  PartitionedTable *new_table;
  const char *old_schema_name;
  const char *old_table_name;
  const char *new_schema_name;
  const char *new_table_name;
};

class MySQLDBScaleMigrateCleanNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleMigrateCleanNode(ExecutePlan *plan, const char *migrate_id);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  const char *migrate_id;
  Packet *error_packet;
};

class MySQLMulModifyNode : public MySQLExecuteNode {
 public:
  MySQLMulModifyNode(ExecutePlan *plan,
                     map<DataSpace *, const char *> &spaces_map,
                     map<DataSpace *, int> *sql_count_map,
                     bool need_handle_more_result);
  void execute();
  void clean();
  Packet *get_error_packet() { return error_packet; }
  map<DataSpace *, const char *> *get_spaces_map() { return &spaces_map; }

 private:
  void handle_error_update_via_delete_insert_revert(bool &need_erase_conn,
                                                    DataSpace *space);
  int execute_send_on(DataSpace *space);
  bool execute_recv_on(DataSpace *space, int retry_count = 0);
  void swap_session();
  bool deal_with_create_part_table_def(bool all_ok);
  void deal_with_packets();
  void deal_with_errors(bool handle_create_part_table_def_ok);
  void handle_error_packet(DataSpace *space, Connection *conn);
  void handle_ok_packet(DataSpace *space, Connection *conn);
  void deal_connections();
  void get_tokudb_consistent_point();
  void do_get_children(list<ExecuteNode *> &children) {
    ACE_UNUSED_ARG(children);
    return;
  }
  bool check_ddl_executing(DataSpace *space);
  map<DataSpace *, const char *> spaces_map;
  pair<bool, int> update_via_del_ins_revert_del_stat;
  pair<bool, int> update_via_del_ins_revert_ins_stat;
  bool update_via_del_ins_has_reverted;
  map<DataSpace *, int> sql_count_map;
  Connection *conn;
  bool got_error;
  Packet *error_packet;
  Packet *packet;
  map<DataSpace *, Connection *> executed_conn;
  vector<conn_result> commit_conn_result;
  vector<Packet *> ok_packets;
  bool need_handle_more_result;
  bool is_ddl_blocked;
  int local_disable_parallel_modify;
};

class MySQLDBScaleMulSyncNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleMulSyncNode(ExecutePlan *plan, const char *sync_topic,
                          const char *sync_state, const char *sync_param,
                          const char *sync_cond, unsigned long version_id);

  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

  Packet *get_error_packet() { return error_packet; }

 private:
  void do_execute();

  void generate_mul_sync_error_packet();

 private:
  const char *sync_topic;
  const char *sync_state;
  const char *sync_param;
  const char *sync_cond;

  Packet *error_packet;
  unsigned long version_id;
};

class MySQLMultipleMasterForwardNode : public MySQLExecuteNode {
 public:
  MySQLMultipleMasterForwardNode(ExecutePlan *plan, const char *sql,
                                 bool is_slow_query = false);

  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

  Packet *get_error_packet() { return error_packet; }
  string get_session_var_sql();
  string get_session_option_sql();
  void handle_result_packets(
      list<Packet *, StaticAllocator<Packet *> > &recv_list,
      bool is_send_to_client = false);
  void forward_slow_query_to_master_role_node(
      vector<pair<string, bool> > &query_list);
  void forward_query_to_master_role_node(
      vector<pair<string, bool> > &query_list);
  void do_forward_query_to_master(Connection *conn,
                                  vector<pair<string, bool> > &query_list);

 protected:
  void execute();

 private:
  void get_result_packets(Connection *conn,
                          list<Packet *, StaticAllocator<Packet *> > &recv_list,
                          int &eof_count,
                          unsigned int timeout = mul_dbscale_forward_timeout);
  void clear_recv_packets(
      list<Packet *, StaticAllocator<Packet *> > &recv_list);

 private:
  Packet *error_packet;
  string exec_sql;
  bool is_slow_query;
};

class MySQLResetTmpTableNode : public MySQLReturnOKNode {
 public:
  MySQLResetTmpTableNode(ExecutePlan *plan);
  void clean(){};

 protected:
  void do_execute();
};

class MySQLLoadFuncTypeNode : public MySQLReturnOKNode {
 public:
  MySQLLoadFuncTypeNode(ExecutePlan *plan);
  void clean(){};

 protected:
  void do_execute();
};

class MySQLDBScaleCleanTempTableCacheNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleCleanTempTableCacheNode(ExecutePlan *plan);
  void clean(){};

 protected:
  void do_execute();
};

class MySQLShutDownNode : public MySQLReturnOKNode {
 public:
  MySQLShutDownNode(ExecutePlan *plan);
  void clean(){};

 protected:
  void do_execute();
};

class MySQLDBScaleSetAutoIncrementOffsetNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleSetAutoIncrementOffsetNode(ExecutePlan *plan,
                                         PartitionedTable *tab);
  void clean(){};

 protected:
  void do_execute();
  PartitionedTable *tab;
};

class MySQLReloadUserAllowOperationTimeNode : public MySQLReturnOKNode {
 public:
  MySQLReloadUserAllowOperationTimeNode(ExecutePlan *plan, string user_name);
  void clean(){};

 private:
  void do_execute();
  string user_name;
};

class MySQLSetUserAllowOperationTimeNode : public MySQLReturnOKNode {
 public:
  MySQLSetUserAllowOperationTimeNode(
      ExecutePlan *plan, set_user_not_allow_operation_time_node *oper);
  void clean(){};

 private:
  void do_execute();
  bool is_allow;
  string user_name;
  int from_hour;
  int from_minute;
  int from_second;
  int to_hour;
  int to_minute;
  int to_second;
};

class MySQLSetSchemaACLNode : public MySQLReturnOKNode {
 public:
  MySQLSetSchemaACLNode(ExecutePlan *plan,
                        set_schema_acl_op_node *set_schema_acl_oper);
  void clean(){};

 private:
  void do_execute();
  const char *username;
  const char *schema;
  SchemaACLType type;
  bool is_reset;
};

class MySQLSetTableACLNode : public MySQLReturnOKNode {
 public:
  MySQLSetTableACLNode(ExecutePlan *plan,
                       set_table_acl_op_node *set_table_acl_oper);
  void clean(){};

 private:
  void do_execute();
  const char *user_name;
  const char *schema_name;
  const char *table_name;
  TableACLType type;
  bool is_reset;
};

class MySQLDynamicConfigurationNode : public MySQLReturnOKNode {
 public:
  MySQLDynamicConfigurationNode(ExecutePlan *plan);
  void clean();

 protected:
  void do_execute();

 private:
  void set_option(dynamic_config_op_node *config_oper,
                  list<set_option_err_msg> &val_err_list,
                  list<string> &name_err_list,
                  map<string, OptionValue> *option_map);
  Packet *packet;
  bool got_error;
};
class MySQLBlockNode : public MySQLReturnOKNode {
 public:
  MySQLBlockNode(ExecutePlan *plan);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  bool got_error;
};
class MySQLDisableServer : public MySQLReturnOKNode {
 public:
  MySQLDisableServer(ExecutePlan *plan);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

 protected:
  void do_execute();

 private:
  Packet *error_packet;
};
class MySQLReloadConfigNode : public MySQLReturnOKNode {
 public:
  MySQLReloadConfigNode(ExecutePlan *plan, const char *sql);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);
  void execute_on_slave_dbscale();

 protected:
  void do_execute();

 private:
  string error_message;
  Packet *error_packet;
  const char *exec_sql;
};

class MySQLFlushNode : public MySQLReturnOKNode {
 public:
  MySQLFlushNode(ExecutePlan *plan, dbscale_flush_type type);

 protected:
  void do_execute();
  dbscale_flush_type type;
};

class MySQLFlushWeekPwdFileNode : public MySQLReturnOKNode {
 public:
  MySQLFlushWeekPwdFileNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLCheckDiskIONode : public MySQLReturnOKNode {
 public:
  MySQLCheckDiskIONode(ExecutePlan *plan);
  ~MySQLCheckDiskIONode() { delete log_file_addr; }

 protected:
  void do_execute();
  void prepare_log_file_addr();
  void prepare_log_content();
  string log_content;
  static const char *log_file_name;
  ACE_FILE_Addr *log_file_addr;
};

class MySQLPurgePoolInfoNode : public MySQLReturnOKNode {
 public:
  MySQLPurgePoolInfoNode(ExecutePlan *plan, pool_node *pool_info);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);
  void purge_connection_pool_for_source(DataSource *source);

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  const char *pool_name;
};

class MySQLFlushPoolInfoNode : public MySQLReturnOKNode {
 public:
  MySQLFlushPoolInfoNode(ExecutePlan *plan, pool_node *pool_info);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);
  void flush_connection_pool_for_source(DataSource *source);

 protected:
  void do_execute();

 private:
  string error_message;
  Packet *error_packet;
  const char *pool_name;
  bool is_force;
};

class MySQLForceFlashbackOnlineNode : public MySQLReturnOKNode {
 public:
  MySQLForceFlashbackOnlineNode(ExecutePlan *plan, const char *server_name);
  Packet *get_error_packet() { return error_packet; }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  const char *server_name;
};

class MySQLXARecoverSlaveDBScaleNode : public MySQLReturnOKNode {
 public:
  MySQLXARecoverSlaveDBScaleNode(ExecutePlan *plan, const char *xa_source,
                                 const char *top_source,
                                 const char *ka_update_v);
  Packet *get_error_packet() { return error_packet; }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  const char *xa_source_name;
  const char *top_source_name;
  unsigned long ka_update_v_cond;
};

class MySQLSetPriorityNode : public MySQLReturnOKNode {
 public:
  MySQLSetPriorityNode(ExecutePlan *plan, string user_name,
                       int tmp_priority_value);
  Packet *get_error_packet() { return error_packet; }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  string user_name;
  UserPriorityValue priority_value;
};
class MySQLResetPoolInfoNode : public MySQLReturnOKNode {
 public:
  MySQLResetPoolInfoNode(ExecutePlan *plan, pool_node *pool_info);
  void clean();
  Packet *get_error_packet() { return error_packet; }
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

 protected:
  void do_execute();
  void set_connection_pool();
  void set_login_pool();
  template <typename T>
  void reset_pool_max_water_mark(Pool<T> *pool);
  void set_backend_pool();
  void set_handler_pool();
  void set_connection_pool_for_source(DataSource *source);

 private:
  Packet *error_packet;
  bool got_error;
  POOL_TYPE type;
  const char *pool_name;
  unsigned int max_value;
  bool is_internal;
};

class MySQLResetInfoPlanNode : public MySQLReturnOKNode {
 public:
  MySQLResetInfoPlanNode(ExecutePlan *plan, dbscale_reset_info_op_node *oper);

 protected:
  void do_execute();
  void clean();
  Packet *get_error_packet() { return error_packet; }

 private:
  void reset_tps_info();

 private:
  Packet *error_packet;
  RESET_INFO_TYPE reset_type;
  bool is_internal;
};
class MySQLRepStrategyNode : public MySQLReturnOKNode {
 public:
  MySQLRepStrategyNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLDynamicSetServerWeightNode : public MySQLReturnOKNode {
 public:
  MySQLDynamicSetServerWeightNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLKeepmasterNode : public MySQLReturnOKNode {
 public:
  MySQLKeepmasterNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLCallSPReturnOKNode : public MySQLReturnOKNode {
  /*For call store procedure, here we only need to return the last ok
   * packet.*/
 public:
  MySQLCallSPReturnOKNode(ExecutePlan *plan);
  void execute();

 protected:
  void do_execute(){};
};

class MySQLDBScaleTestNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleTestNode(ExecutePlan *plan);

 protected:
  void do_execute();
};
class MySQLAsyncTaskControlNode : public MySQLReturnOKNode {
 public:
  MySQLAsyncTaskControlNode(ExecutePlan *plan, unsigned long long id);

 protected:
  void do_execute();

 private:
  unsigned long long id;
};

class MySQLLoadDataSpaceFileNode : public MySQLReturnOKNode {
 public:
  MySQLLoadDataSpaceFileNode(ExecutePlan *plan, const char *filename);

 protected:
  void do_execute();

 private:
  const char *filename;
};

class MySQLDBScalePurgeMonitorPointNode : public MySQLReturnOKNode {
 public:
  MySQLDBScalePurgeMonitorPointNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLDBScaleCleanMonitorPointNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleCleanMonitorPointNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLShowEngineLockWaitingNode : public MySQLExecuteNode {
 public:
  MySQLShowEngineLockWaitingNode(ExecutePlan *plan, int engine_type);
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *packet;
  Packet *error_packet;
  bool is_have_send(vector<DataServer *> &has_send, DataServer *tmp);
  int engine_type;
};

class MySQLDBScaleShowNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleShowNode(ExecutePlan *plan);
  virtual void clean();
  virtual void execute();
  virtual void init_data() = 0;
  void set_head_packet(uint64_t columns, uint8_t number = 0);
  void reset_column_list();
  void reset_row_list();
  void add_column_packet(const char *catalog, const char *schema,
                         const char *table, const char *org_table,
                         const char *column, const char *org_conlumn,
                         uint16_t charset, uint32_t column_length,
                         MySQLColumnType column_type, uint16_t flags,
                         char decimals, uint8_t number);
  void add_row_packet(list<const char *> &row_data);
  void add_row_packet(list<string> &row_data);
  void add_row_packet(Packet *packet);
  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate = NULL, uint8_t number = 0);
  Packet *get_error_packet() { return error_packet; }
  bool is_row_list_empty() {
    if (row_list.empty()) return true;
    return false;
  }
  void set_need_record_result_str() { is_need_record_result_str = true; }
  list<string> *get_field_str_list() { return &field_str_list; }
  list<string> *get_result_str_list();
  bool filter_packet_item(Packet *packet, filter_info_item *root);
  bool filter_packet(Packet *packet);
  void set_filter_info_filter_index(filter_info_item *root, const char *column,
                                    int index);
  void generate_filter_index();

 protected:
  Packet *head_packet;
  Packet *error_packet;
  Packet *eof_packet;
  list<Packet *> column_list;
  list<Packet *> row_list;
  bool is_need_record_result_str;
  uint64_t column_num;
  list<string> result_str_list;
  list<string> field_str_list;
  int filter_index;
};

class MySQLShowOutlineMonitorInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowOutlineMonitorInfoNode(ExecutePlan *plan);
  void init_data();

 private:
  std::string order_column;
  order_dir order;
};

class MySQLSelectPlainValueNode : public MySQLExecuteNode {
 public:
  MySQLSelectPlainValueNode(ExecutePlan *plan, const char *str_value,
                            const char *alias_name);
  void clean() {}
  void execute();
  Packet *get_error_packet() { return NULL; }

 private:
  const char *col_name;
  const char *col_value;
};

class MySQLShowUserNotAllowOperationTimeNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowUserNotAllowOperationTimeNode(ExecutePlan *plan, string user_name);
  void init_data();

 private:
  string user_name;
};

class MySQLDBScaleShowDefaultSessionVarNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowDefaultSessionVarNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowVersionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowVersionNode(ExecutePlan *plan);
  void init_data();
};

class MySQLShowVersionNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowVersionNode(ExecutePlan *plan);
  void init_data();
};

class ShowComponentsVersionNode : public MySQLDBScaleShowNode {
 public:
  ShowComponentsVersionNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowHostnameNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowHostnameNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowPartitionTableStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPartitionTableStatusNode(ExecutePlan *plan,
                                           const char *table_name);
  void init_data();

 private:
  const char *table_name;
  string tmp_table_name;
  string tmp_schema_name;
  string auto_increment_key;
  char key_pos_str[20];
  char auto_increment_key_pos_str[20];
  char auto_increment_base_value_str[20];
  string virtual_map;
  string shard_map;
};

class MySQLDBScaleShowSchemaACLInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowSchemaACLInfoNode(ExecutePlan *plan, bool is_show_all);
  void init_data();

 private:
  bool is_show_all;
};

class MySQLDBScaleShowTableACLInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowTableACLInfoNode(ExecutePlan *plan, bool is_show_all);
  void init_data();

 private:
  bool is_show_all;
};

class MySQLDBScaleShowPathInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPathInfoNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowWarningsNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowWarningsNode(ExecutePlan *plan);
  void init_data();
  list<pair<string, string> > dbscale_warning_list;
};

class MySQLDBScaleShowCriticalErrorNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowCriticalErrorNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowAllFailTransactionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowAllFailTransactionNode(ExecutePlan *plan);
  void init_data();
};
class MySQLDBScaleShowDetailFailTransactionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowDetailFailTransactionNode(ExecutePlan *plan, const char *xid);
  void init_data();

 private:
  string xid;
};

class MySQLDBScaleCleanFailTransactionNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleCleanFailTransactionNode(ExecutePlan *plan, const char *xid);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  const char *xid;
  Packet *error_packet;
};

class MySQLDBScaleShowBaseStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowBaseStatusNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowPartitionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPartitionNode(ExecutePlan *plan);
  void init_data();

 private:
  bool set_info();
  list<const char *> data_source_names;
  const char *table_name;
  const char *schema_name;
  string expresion;
  string part_type_name;
};

class MySQLDBScaleShowDataSourceNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowDataSourceNode(ExecutePlan *plan, list<const char *> &names,
                                 bool need_show_weight);
  void init_data();

 private:
  bool need_show_weight;
  list<string> columns;
  list<const char *> names;
};

class MySQLCheckMetaDataNode : public MySQLReturnOKNode {
 public:
  MySQLCheckMetaDataNode(ExecutePlan *plan);

  void clean();

  void set_error_packet(uint16_t error_code, const char *error_message,
                        const char *sqlstate, uint8_t number = 0);

  int pre_options();

  int pre_check_table_structure_field_type(const string &database_name,
                                           const string &tb_name);

  int pre_check_table_structure_field_type(
      const string &database_name, const string &tb_name,
      vector<vector<string> > &one_column,
      vector<vector<string> > &remote_one_column);

  int find_missing_value(vector<vector<string> > &local_data_vec,
                         vector<vector<string> > &remote_data_vec,
                         const string &table_name);

  int compare_meta_data_vec_one_row(vector<string> &local_vec,
                                    vector<string> &remote_vec);

  int check_field_type_exist(vector<string> &field_type_vec, string &field_type,
                             vector<vector<string> > &one_column,
                             vector<vector<string> > &remote_one_column);

  Connection *get_connection();

  DataServer *get_remote_dataserver();

  Packet *get_error_packet() { return error_packet; }

 protected:
  void do_execute();

 private:
  Connection *conn;
  Connection *remote_conn;
  DataServer *remote_server;
  Packet *error_packet;
  vector<vector<string> > one_column;
  vector<vector<string> > remote_one_column;
};

class MySQLCheckTableNode : public MySQLDBScaleShowNode {
 public:
  MySQLCheckTableNode(ExecutePlan *plan);
  void init_data();
  string full_table_name;
};

class MySQLShowWarningNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowWarningNode(ExecutePlan *plan);
  void init_data();
};

class MySQLShowLoadWarningNode : public MySQLExecuteNode {
 public:
  MySQLShowLoadWarningNode(ExecutePlan *plan);
  bool can_swap() { return false; }
  void clean() {}

 protected:
  void execute();
  Packet *get_error_packet() { return NULL; }
};

class MySQLNavicateProfileSqlNode : public MySQLDBScaleShowNode {
 public:
  MySQLNavicateProfileSqlNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowUserMemoryStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowUserMemoryStatusNode(ExecutePlan *plan);
  void init_data();
  void get_user_memory_status(list<Packet *> &row_list);
};

class MySQLDBScaleShowSessionIdNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowSessionIdNode(ExecutePlan *plan, const char *server_name,
                                int connection_id);
  void init_data();
  void get_session_info();

 private:
  const char *server_name;
  int connection_id;
};

class MySQLDBScaleShowUserStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowUserStatusNode(ExecutePlan *plan, const char *user_id,
                                 const char *user_name, bool only_show_running,
                                 bool instance, bool show_status_count);
  void init_data();
  void get_all_users_status();
  void get_user_status_by_id();
  void get_status_by_name();
  void get_user_status_count();

 private:
  const char *user_id;
  const char *user_name;
  bool only_show_running;
  bool instance;
  bool show_status_count;
};

class MySQLDBScaleShowUserProcesslistNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowUserProcesslistNode(ExecutePlan *plan, const char *cluster_id,
                                      const char *user_id, int local);
  void init_data();

 private:
  void get_user_processlist();

 private:
  const char *cluster_id_str;
  const char *user_id_str;
  unsigned int user_id;
  int full;
  int local;
};

class MySQLDBScaleExecuteOnDataserverNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleExecuteOnDataserverNode(ExecutePlan *plan,
                                      const char *dataserver_name,
                                      const char *stmt_sql);
  void clean();

 protected:
  void do_execute();

 private:
  Packet *packet;
  Packet *error_packet;
  const char *dataserver_name;
  const char *stmt_sql;
};

class MySQLDBScaleBackendServerExecuteNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleBackendServerExecuteNode(ExecutePlan *plan, const char *stmt_sql);
  void clean();

 protected:
  void do_execute();

 private:
  Packet *packet;
  const char *stmt_sql;
};

class MySQLDBScaleExecuteOnAllMasterserverExecuteNode
    : public MySQLReturnOKNode {
 public:
  MySQLDBScaleExecuteOnAllMasterserverExecuteNode(ExecutePlan *plan,
                                                  const char *stmt_sql);
  void clean();

 protected:
  void do_execute();
  void adjust_data_source_by_type(map<string, DataSource *> &src_ds,
                                  list<DataSource *> &dst_ds);

 private:
  Packet *packet;
  const char *stmt_sql;
};

class MySQLDBScaleEraseAuthInfoNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleEraseAuthInfoNode(ExecutePlan *plan, name_item *username_list,
                                bool all_dbscale_node);
  void clean() {}

 protected:
  void do_execute();

 private:
  name_item *username_list;
  bool all_dbscale_node;
};

class MySQLDBScaleShowTableLocationNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowTableLocationNode(ExecutePlan *plan, const char *schema_name,
                                    const char *table_name);
  void init_data();
  void get_table_location();
  void get_slave_info(LoadBalanceDataSource *cur_ds,
                      list<const char *> &row_data, int group_id);

 private:
  const char *schema_name;
  const char *table_name;
};

class MySQLDBScaleDynamicUpdateWhiteNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleDynamicUpdateWhiteNode(ExecutePlan *plan, bool is_add,
                                     const char *ip,
                                     const ssl_option_struct &ssl_option_value,
                                     const char *comment,
                                     const char *user_name);
  Packet *get_error_packet() { return error_packet; }
  void clean() {
    if (error_packet) {
      delete error_packet;
      error_packet = NULL;
    }
  }

 protected:
  void do_execute();

 private:
  bool is_add;
  string user_name;
  const char *ip;
  string comment;
  ssl_option_struct ssl_option_value;
  Packet *error_packet;
};

class MySQLDBScaleShowDataServerNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowDataServerNode(ExecutePlan *plan);
  void init_data();

 private:
  string server_connection;
  char port[10];
  char remote_port[10];
  char location_id[21];
  char master_priority[21];
};
class MySQLDBScaleShowBackendThreadsNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowBackendThreadsNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowPartitionSchemeNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPartitionSchemeNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowSchemaNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowSchemaNode(ExecutePlan *plan, const char *schema_name);
  void init_data();

 private:
  const char *schema_name;
};
class MySQLDBScaleShowTableNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowTableNode(ExecutePlan *plan, const char *schema,
                            const char *table, bool use_like);
  void init_data();

 private:
  const char *schema_name;
  const char *table_name;
  bool use_like;
};

class MySQLDBScaleShowPoolInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPoolInfoNode(ExecutePlan *plan);
  void init_data();

 private:
  list<PoolInfo *> *get_connection_pool_info();
  list<PoolInfo *> *get_thread_pool_info();
  list<PoolInfo *> *get_tmp_table_pool_info(desc_or_null desc_type);
  const char *name_;
};

class MySQLDBScaleShowUserSqlCountNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowUserSqlCountNode(ExecutePlan *plan, const char *user_id);
  void init_data();
  void output(Session *cur_session);
  void output_historical_totally();
  void get_user_sql_count();
  void get_user_sql_count_by_id();

 private:
  const char *user_id;
};

class MySQLDBScaleShowPoolVersionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPoolVersionNode(ExecutePlan *plan);
  void get_connection_pool_for_source(DataSource *source);
  void init_data();
};

class MySQLDBScaleShowStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowStatusNode(ExecutePlan *plan);
  void init_data();

 private:
  string variable_name;
};

class MySQLShowCatchupNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowCatchupNode(ExecutePlan *plan, const char *source_name);
  void init_data();

 private:
  const char *source_name;
};

class MySQLSkipWaitCatchupNode : public MySQLReturnOKNode {
 public:
  MySQLSkipWaitCatchupNode(ExecutePlan *plan, const char *source_name);
  void clean();
  Packet *get_error_packet() { return error_packet; }

 protected:
  void do_execute();

 private:
  Packet *error_packet;
  const char *source_name;
};

class MySQLDBScaleShowAsyncTaskNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowAsyncTaskNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleExplainNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleExplainNode(ExecutePlan *plan,
                          bool is_server_explain_stmt_always_extended);
  void init_data();
  void init_data_for_explain_node_sql();
  void handle_data(bool has_more_result);
  void execute();

 private:
  bool is_server_explain_stmt_always_extended;
};

class MySQLFetchExplainResultNode : public MySQLInnerNode {
 public:
  MySQLFetchExplainResultNode(ExecutePlan *plan, list<list<string> > *result,
                              bool is_server_explain_stmt_always_extended);
  void execute();
  void handle_children();
  void handle_child(MySQLExecuteNode *child);
  bool can_swap() { return false; }

 private:
  int send_packet_profile_id;
  int wait_child_profile_id;
  list<list<string> > *result;
  bool is_server_explain_stmt_always_extended;
};

class MySQLDBScaleShowAutoIncInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowAutoIncInfoNode(ExecutePlan *plan, PartitionedTable *tab);
  void init_data();

 private:
  PartitionedTable *tab;
};

class MySQLDBScaleShowShardMapNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowShardMapNode(ExecutePlan *plan, PartitionedTable *tab);
  void init_data();

 private:
  PartitionedTable *tab;
};

class MySQLDBScaleShowVirtualMapNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowVirtualMapNode(ExecutePlan *plan, PartitionedTable *tab);
  void init_data();

 private:
  PartitionedTable *tab;
};

class MySQLDBScaleClusterShutdownNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleClusterShutdownNode(ExecutePlan *plan, int cluster_id);
  void do_execute();

 private:
  int cluster_id;
};

/*MySQLDBScaleRequestClusterIdNode is used by slave dbscale.
 * slave dbscale use this node to get cluster_server_id from master dbscale*/
class MySQLDBScaleRequestClusterIdNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestClusterIdNode(ExecutePlan *plan);
  void init_data();
};

/*MySQLDBScaleRequestAllClusterIncInfoNode is used by master dbscale.
 * when cluster_server_id > cluster_server_num, when new slave dbscale join
 * cluster, master dbscale should reset cluster server num. and master dbscale
 * will clear all slave dbscale's auto increment info. master dbscale will also
 * get all auto increment info from all slave dbscale. this node will return
 * auto increment info*/
class MySQLDBScaleRequestAllClusterIncInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestAllClusterIncInfoNode(ExecutePlan *plan);
  void init_data();
};

/*MySQLDBScaleRequestClusterIncInfoNode is used by slave dbscale.
 * when slave dbscale first insert into auto increment table,
 * slave dbscale will use this node get init value from master dbscale.
 * master dbscale will get init info from backend mysql,
 * then publish the table's auto increment into to zookeeper.
 * other slave dbscale will init the table's auto increment from zookeeper*/
class MySQLDBScaleRequestClusterInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestClusterInfoNode(ExecutePlan *plan);
  void init_data();
  Packet *generate_error_cluster_node_info(string host_port);
};

class MySQLDBScaleRequestNodeInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestNodeInfoNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleRequestClusterIncInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestClusterIncInfoNode(ExecutePlan *plan,
                                        PartitionedTable *space,
                                        const char *schema_name,
                                        const char *table_name);
  void init_data();
  int64_t get_auto_inc_value();

 private:
  PartitionedTable *space;
  const char *schema_name;
  const char *table_name;
};

class MySQLDBScaleRequestUserStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestUserStatusNode(ExecutePlan *plan, const char *id,
                                    bool only_show_running, bool show_count);
  void init_data();

 private:
  void get_all_user_status();
  void get_user_status_by_id();
  void get_user_status_count();

 private:
  const char *user_id;
  bool only_show_running;
  bool show_status_count;
};

class MySQLDBScaleShowLockUsageNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowLockUsageNode(ExecutePlan *plan);
  void init_data();

 private:
  void send_lock_tables_to_client(lock_table_node *head,
                                  lock_operation operation,
                                  set<Session *, compare_session>::iterator it);
};

class MySQLDBScaleShowExecutionProfileNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowExecutionProfileNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowTransactionSqlsNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowTransactionSqlsNode(ExecutePlan *plan);
  void init_data();

 private:
  uint32_t user_id;
};

class MySQLDBScaleShowOptionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowOptionNode(ExecutePlan *plan);
  void init_data();

 private:
  void get_row_datas();
  list<const char *> columns;
  list<const char *> row_data;
  map<string, string> option_val_map;
};

class MySQLDBScaleShowDynamicOptionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowDynamicOptionNode(ExecutePlan *plan);
  void init_data();

 private:
  void get_row_datas();
  list<const char *> columns;
  list<const char *> row_data;
  map<string, string> option_val_map;
};

class MySQLChangeStartupConfigNode : public MySQLReturnOKNode {
 public:
  MySQLChangeStartupConfigNode(ExecutePlan *plan);

 protected:
  void do_execute();
};

class MySQLShowChangedStartupConfigNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowChangedStartupConfigNode(ExecutePlan *plan);
  void init_data();

 private:
  void get_row_datas();
  list<const char *> columns;
  list<const char *> row_data;
  map<string, string> config_map;
};

class MySQLDBScaleHelpNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleHelpNode(ExecutePlan *plan, const char *name);
  void init_data();

 private:
  const char *cmd_name;
};

class MySQLDBScaleShowAuditUserListNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowAuditUserListNode(ExecutePlan *plan);
  void init_data();
};

class MySQLDBScaleShowSlowSqlTopNNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowSlowSqlTopNNode(ExecutePlan *plan);
  void init_data();
  size_t top_n;
};

class MySQLDBScaleRequestSlowSqlTopNNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleRequestSlowSqlTopNNode(ExecutePlan *plan);
  void init_data();
  size_t top_n;
};

class MySQLDBScaleShowJoinNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowJoinNode(ExecutePlan *plan, const char *name);
  void init_data();

 private:
  const char *op_name;
};

class MySQLDBScaleShowShardPartitionNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowShardPartitionNode(ExecutePlan *plan, const char *name);
  void init_data();

 private:
  const char *scheme_name;
};

class MySQLDBScaleShowRebalanceWorkLoadNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowRebalanceWorkLoadNode(ExecutePlan *plan, const char *name,
                                        list<string> sources,
                                        const char *schema_name, int is_remove);
  void init_data();
  string get_extra_info_for_table(const char *schema_name,
                                  const char *table_name);

 private:
  const char *scheme_name;
  list<string> sources;
  const char *schema_name;
  int is_remove;
};

class MySQLDBScaleAddDefaultSessionVarNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleAddDefaultSessionVarNode(ExecutePlan *plan, DataSpace *dataspace);
  virtual void clean();
  virtual void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *error_packet;
  Packet *packet;
  DataSpace *dataspace;
  Connection *conn;
  bool got_error;
};
class MySQLDBScaleRemoveDefaultSessionVarNode : public MySQLExecuteNode {
 public:
  MySQLDBScaleRemoveDefaultSessionVarNode(ExecutePlan *plan);
  virtual void clean();
  virtual void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *error_packet;
  Packet *packet;
  Packet *ok_packet;
};

/* It is similar with SendNode*/
class MySQLDispatchPacketNode : public MySQLSendNode {
 public:
  MySQLDispatchPacketNode(ExecutePlan *plan, TransferRule *rule);

  virtual void send_header();
  virtual void send_row(MySQLExecuteNode *ready_child);
  virtual void send_eof();
  virtual void flush_net_buffer();
  void dispatch_error_packet();

 private:
  void do_clean();
  void send_header_fields_and_eof_packet(Packet *packet);
  void send_row_packet(Packet *packet);
  int key_position;
  DispatchStrategy *dispatch_strategy;
  TransferRule *rule;
};

class MySQLCursorDirectNode : public MySQLExecuteNode {
 public:
  MySQLCursorDirectNode(ExecutePlan *plan, DataSpace *dataspace,
                        const char *sql);

  void clean();
  Packet *get_error_packet() { return error_packet; }
  void execute();
  void set_cursor_execute_state(CursorExecuteStatus cur_ces) { ces = cur_ces; }
  bool get_one_row(vector<string> &vector) {
    if (row_vector.empty()) {
      return false;
    }
    vector.swap(row_vector);
    row_vector.clear();
    return true;
  }

 private:
  void handle_error_packet(Packet *packet);
  void open_execute();
  void fetch_execute();
  void close_execute();

  bool force_use_non_trx_conn;
  const char *sql;
  Connection *conn;
  bool got_error;
  Packet *packet;
  Packet *error_packet;
  vector<pair<unsigned int, string> > uservar_vec;
  bool select_uservar_flag;
  unsigned int select_field_num;
  vector<bool> field_is_number_vec;
  const char *server_name;
  CursorExecuteStatus ces;
  vector<string> row_vector;
  bool not_found_flag;
  unsigned int field_num;
  unsigned int row_num;
};

class MySQLCursorSendNode : public MySQLInnerNode {
 public:
  MySQLCursorSendNode(ExecutePlan *plan);

  virtual void dealwith_header();
  virtual void send_row(MySQLExecuteNode *ready_child);
  virtual void handle_child(MySQLExecuteNode *child) { send_row(child); }

  void handle_children();
  void execute();
  void set_cursor_execute_state(CursorExecuteStatus cur_ces) { ces = cur_ces; }
  bool get_one_row(vector<string> &vector) {
    if (row_vector.empty()) {
      return false;
    }
    vector.swap(row_vector);
    row_vector.clear();
    return true;
  }
  Packet *get_error_packet() {
    if (not_found_flag) {
      return error_packet;
    } else {
      return MySQLInnerNode::get_error_packet();
    }
  }
  void clean() {
    MySQLInnerNode::clean();
    if (error_packet) {
      delete error_packet;
      error_packet = 0;
    }
  }

 private:
  void open_execute();
  void fetch_execute();
  uint64_t row_num;
  vector<pair<unsigned int, string> > uservar_vec;
  bool select_uservar_flag;
  unsigned int field_num;
  unsigned int select_field_num;
  vector<bool> field_is_number_vec;
  CursorExecuteStatus ces;
  vector<string> row_vector;
  Packet *error_packet;
  bool not_found_flag;
};

class MySQLFederatedTableNode : public MySQLExecuteNode {
 public:
  MySQLFederatedTableNode(ExecutePlan *plan);
  void clean() {}
  void execute();
  Packet *get_error_packet() { return NULL; }
};
class MySQLEmptySetNode : public MySQLExecuteNode {
 public:
  MySQLEmptySetNode(ExecutePlan *plan, int field_num);
  void clean() {}
  void execute();
  Packet *get_error_packet() { return NULL; }

 private:
  int field_num;
};
class MySQLDynamicAddDataSourceNode : public MySQLExecuteNode {
 public:
  MySQLDynamicAddDataSourceNode(
      ExecutePlan *plan,
      dynamic_add_data_source_op_node *dynamic_add_data_source_oper,
      DataSourceType type);
  void clean();
  void execute();
  bool check_group_id_char();
  Packet *get_error_packet() { return error_packet; }

 private:
  void check_config();
  Packet *packet;
  Packet *error_packet;
  dynamic_add_data_source_server_info *server_list;
  int server_list_size;
  const char *group_id_c;
  int group_id;
  DataSourceType type;
  const char *source_name;
  LoadBalanceStrategy lb_strategy;
  int semi_sync;
};

class MySQLDynamicAddDataSpaceNode : public MySQLExecuteNode {
 public:
  MySQLDynamicAddDataSpaceNode(
      ExecutePlan *plan,
      dynamic_add_data_space_op_node *dynamic_add_data_space_oper);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void check_config();
  void add_schema_dataspace();
  void add_normal_table_dataspace();
  void add_partition_scheme_dataspace();
  void add_partition_table();
  void check_add_table_dataspace();
  Packet *packet;
  Packet *error_packet;
  dynamic_add_data_space_op_node *data_space_info;
  const char *partition_key_name;
  const char *partition_scheme_name;
  const char *name_pattern;
  string schema_name;
  string table_name;
  bool is_force;
};

#ifndef CLOSE_ZEROMQ
class MySQLMessageServiceNode : public MySQLExecuteNode {
 public:
  MySQLMessageServiceNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void start_server_service();
  void stop_server_service();
  void start_client_service();
  void stop_client_service();

 private:
  Packet *error_packet;
  bool is_start;
  bool is_stop;
};

class MySQLBinlogTaskNode : public MySQLExecuteNode {
 public:
  MySQLBinlogTaskNode(ExecutePlan *plan);

  void clean();
  void execute();
  ;
  Packet *get_error_packet() { return error_packet; }

 private:
  void create_task_service();

 private:
  Packet *error_packet;
};

class MySQLTaskNode : public MySQLExecuteNode {
 public:
  MySQLTaskNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void start_task(bool is_server, string task_name, task_thread_type type);
  void stop_task(bool is_server, string task_name, task_thread_type type);

 private:
  Packet *error_packet;
};

class MySQLBinlogTaskAddFilterNode : public MySQLExecuteNode {
 public:
  MySQLBinlogTaskAddFilterNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void add_filter_to_database();
  void add_filter();

 private:
  Packet *error_packet;
  string task_name;
  bool is_server;
  vector<filter_table_struct> filter_table_vector;
};

class MySQLDropTaskFilterNode : public MySQLExecuteNode {
 public:
  MySQLDropTaskFilterNode(ExecutePlan *plan);
  void drop_filter_to_database();
  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  Packet *error_packet;
  string task_name;
  bool is_server;
};

class MySQLDropTaskNode : public MySQLExecuteNode {
 public:
  MySQLDropTaskNode(ExecutePlan *plan);

  void clean();
  void execute();
  Packet *get_error_packet() { return error_packet; }

 private:
  void drop_task(bool is_server, string task_name);
  void delete_task_metadata(bool is_server, string task);

 private:
  Packet *error_packet;
};

class MySQLShowClientTaskStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowClientTaskStatusNode(ExecutePlan *plan, const char *task_name,
                                const char *server_task_name);
  void init_data();

 private:
  void handle_one_client_task(const char *name);
  const char *task_name;
  const char *server_task_name;
};

class MySQLShowServerTaskStatusNode : public MySQLDBScaleShowNode {
 public:
  MySQLShowServerTaskStatusNode(ExecutePlan *plan, const char *task_name);
  void init_data();

 private:
  void handle_one_server_task(const char *name);
  const char *task_name;
};

#endif

class MySQLDBScaleUpdateAuditUserNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleUpdateAuditUserNode(ExecutePlan *plan, const char *username,
                                  bool is_add);
  void do_execute();

 private:
  string username;
  bool is_add;
};

class MySQLDBScaleShowCreateOracleSeqPlan : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowCreateOracleSeqPlan(ExecutePlan *plan, const char *seq_schema,
                                      const char *seq_name);
  void init_data();

 private:
  string seq_schema;
  string seq_name;
};

class MySQLDBScaleShowSeqStatusPlan : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowSeqStatusPlan(ExecutePlan *plan, const char *seq_schema,
                                const char *seq_name);
  void init_data();

 private:
  string seq_schema;
  string seq_name;
};

class MySQLDBScaleShowFetchNodeBufferNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowFetchNodeBufferNode(ExecutePlan *plan)
      : MySQLDBScaleShowNode(plan) {}
  void init_data();
};
class MySQLDBScaleShowTrxBlockInfoNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowTrxBlockInfoNode(ExecutePlan *plan, bool is_local);
  void init_data();

 private:
  bool is_local;
};

class MySQLDBScaleInternalSetNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleInternalSetNode(ExecutePlan *plan);

 private:
  void do_execute();
};

class MySQLDBScaleShowOutlineHintNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowOutlineHintNode(ExecutePlan *plan,
                                  dbscale_operate_outline_hint_node *oper);
  void init_data();

 private:
  list<const char *> columns;
  dbscale_operate_outline_hint_node *oper;
};
class MySQLRestoreRecycleTablePrecheckNode : public MySQLExecuteNode {
 public:
  MySQLRestoreRecycleTablePrecheckNode(
      ExecutePlan *plan, const char *from_schema, const char *from_table,
      const char *to_table, const char *recycle_type, DataSpace *dspace);
  void execute();
  void clean() {}
  bool notify_parent() { return false; }
  Packet *get_error_packet() { return error_packet; }

 private:
  string from_schema;
  string from_table;
  string to_table;
  string recycle_type;
  DataSpace *table_dataspace;
  Packet *error_packet;
};

class MySQLDBScaleShowPrepareCacheNode : public MySQLDBScaleShowNode {
 public:
  MySQLDBScaleShowPrepareCacheNode(ExecutePlan *plan);
  void init_data();

 private:
  MySQLDriver *driver;
};

class MySQLDBScaleFlushPrepareCacheHitNode : public MySQLReturnOKNode {
 public:
  MySQLDBScaleFlushPrepareCacheHitNode(ExecutePlan *plan);
  void do_execute();

 private:
  MySQLDriver *driver;
};

}  // namespace mysql
}  // namespace dbscale
#endif
