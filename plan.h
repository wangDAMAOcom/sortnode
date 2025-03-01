#ifndef DBSCALE_PLAN_H
#define DBSCALE_PLAN_H

#include <ace/Semaphore.h>
#include <backend_thread.h>
#include <data_source.h>
#include <execute_profile.h>
#include <packet.h>
#include <session.h>
#include <statement.h>
#include <stl_st_node_allcator.h>

#include <string>

#ifndef DBSCALE_DISABLE_SPARK
#include <spark_client.h>
#endif

#define THREAD_MESSAGE_WORK '0'
#define THREAD_MESSAGE_QUIT '1'

using namespace std;
using namespace dbscale;
using namespace dbscale::sql;
/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */
namespace dbscale {

class Driver;
struct ssl_option_struct;

namespace plan {

enum ExecuteStatus {
  EXECUTE_STATUS_START,
  EXECUTE_STATUS_FETCH_DATA,
  EXECUTE_STATUS_WAIT,
  EXECUTE_STATUS_HANDLE,
  EXECUTE_STATUS_BEFORE_COMPLETE,
  EXECUTE_STATUS_COMPLETE,

  EXECUTE_STATUS_SELECT_WAIT,
  EXECUTE_STATUS_SELECT_FETCH_DATA,
  EXECUTE_STATUS_SELECT_HANDLE,
  EXECUTE_STATUS_SELECT_COMPLETE
};

enum CursorExecuteStatus { OPEN_EXECUTE, FETCH_EXECUTE };

enum FilterFlag { FILTER_FLAG_MATCH, FILTER_FLAG_UNMATCH, NON };

enum PlanExceptionType {
  PACKET_ERROR_TYPE,
  EXECUTE_NODE_ERROR_TYPE,
  EXCEPTION_TYPE,
  PACKET_ERROR_NON_SEND_TYPE
};

class SubQueryResultNode {
 public:
  SubQueryResultNode() {}

  /*For different kind of result should generated different replace sql:
   *
   * 1. for one value result, should genrerated a string for that value
   *
   * 2. for one column result, should generated a ',' separated string
   *
   * 3. for one column subquery with all/any/some, should return a string
   * "select min/max".
   *
   * 4. for table result, should generated a select stmt, which could be
   * executed inside one dataspace.*/
  const char *get_replace_sql() {
    LOG_DEBUG("Get replace sql: %s\n", replace_sql.c_str());
    return replace_sql.c_str();
  };

  string replace_sql;
  vector<string> row_columns;
};

class ExecutePlan {
 public:
  ExecutePlan(Statement *statement, Session *session, Driver *driver,
              Handler *handler);

  virtual ~ExecutePlan() { clean(); }

  void execute();
  void deal_error_transaction();
  void deal_plan_exception(PlanExceptionType type, bool keep_conn_old);
  void deal_sp_cursor_fetch_no_data_ignore_trx();
  void deal_lock_error();
  void set_need_destroy_dataspace_after_exec(bool b) {
    need_destroy_dataspace_after_exec = b;
  }
  void check_ddl_in_cluster_xa_transaction_before_node_execute();
  bool get_need_destroy_dataspace_after_exec() {
    return need_destroy_dataspace_after_exec;
  }
  void set_is_cursor(bool is_cursor) { this->is_cursor = is_cursor; }

#ifdef DEBUG
  inline void plan_start_timing() { plan_start_date = ACE_OS::gettimeofday(); }
  inline void plan_end_timing() {
    plan_end_date = ACE_OS::gettimeofday();
    plan_cost_time += (plan_end_date - plan_start_date).msec();
    LOG_DEBUG("The plan %@ cost %d ms.\n", this, plan_cost_time);
  }
#endif

  bool plan_can_swap();
  void handle_acl_related_stmt(stmt_node *st);
  void generate_plan_can_swap();

  void fullfil_error_info(unsigned int *error_code, string *sql_state) {
    do_fullfil_error_info(error_code, sql_state);
  }

  void set_sub_query_result_node(SubQueryResultNode *res_node) {
    this->res_node = res_node;
  }

  SubQueryResultNode *get_sub_query_result_node() { return res_node; }

  ExecuteNode *get_fetch_node(DataSpace *dataspace, const char *sql) {
    return do_get_fetch_node(dataspace, sql);
  }
  ExecuteNode *get_navicat_profile_sql_node() {
    return do_get_navicat_profile_sql_node();
  }
#ifndef DBSCALE_DISABLE_SPARK
  ExecuteNode *get_spark_node(SparkConfigParameter config, bool is_insert) {
    return do_get_spark_node(config, is_insert);
  }
#endif

  ExecuteNode *get_send_node() {
    if (this->statement->get_spark_invest_stmt())
      return get_send_dbscale_mul_column_node();
    if (!this->statement->is_handle_sub_query()) return do_get_send_node();
    switch (this->statement->get_sub_query_stmt()) {
      case SUB_SELECT_ONE_VALUE:
        LOG_DEBUG("Get send_dbscale_one_value_node instead of send_node\n");
        return get_send_dbscale_one_column_node(true);
      case SUB_SELECT_ONE_COLUMN:
        LOG_DEBUG("Get send_dbscale_one_column_node instead of send_node\n");
        return get_send_dbscale_one_column_node(false);
      case SUB_SELECT_ONE_COLUMN_MIN:
        LOG_DEBUG(
            "Get send_dbscale_one_column_aggr_node min instead of send_node\n");
        return get_send_dbscale_one_column_aggr_node(true);
      case SUB_SELECT_ONE_COLUMN_MAX:
        LOG_DEBUG(
            "Get send_dbscale_one_column_aggr_node max instead of send_node\n");
        return get_send_dbscale_one_column_aggr_node(false);
      case SUB_SELECT_EXISTS:
        /*对于EXISTS子查询，分为2种情况：
          1. correlated场景，这种场景下rs的sub_query_type会被调整为
          SUB_SELECT_DEPENDENT，见代码Statement::analysis_record_scan_tree中correlated
          子查询处理部分
          2. 非correlated场景， 主要的思想就是，如果 Exists
          的子查询是能查出数据的，就给 `select 1 from dual where 1=1 `
          如果不能查出数据，就给`select 1 from dual where 1=0`
             代码见：MySQLQueryExistsNode::handle_send_client_packet。
        */

        LOG_DEBUG("Get get_send_dbscale_exists_node instead of send_node\n");
        return get_send_dbscale_exists_node();
      default:
        LOG_ERROR("Unsupport sub query stmt type during get send node.\n");
        throw NotSupportedError(
            "Unsupport sub query stmt type during get send node.");
    }
  }

  ExecuteNode *get_dispatch_packet_node(TransferRule *rule) {
    set_dispatch_federated(true);
    return do_get_dispatch_packet_node(rule);
  }

  ExecuteNode *get_into_outfile_node() { return do_get_into_outfile_node(); }
  ExecuteNode *get_modify_limit_node(record_scan *rs, PartitionedTable *table,
                                     vector<unsigned int> &par_ids,
                                     unsigned int limit, string &sql) {
    return do_get_modify_limit_node(rs, table, par_ids, limit, sql);
  }

  ExecuteNode *get_select_into_node() { return do_get_select_into_node(); }
  ExecuteNode *get_sort_node(list<SortDesc> *sort_desc) {
    return do_get_sort_node(sort_desc);
  }
  ExecuteNode *get_single_sort_node(list<SortDesc> *sort_desc) {
    return do_get_single_sort_node(sort_desc);
  }
  ExecuteNode *get_project_node(unsigned int skip_columns) {
    return do_get_project_node(skip_columns);
  }
  ExecuteNode *get_aggr_node(list<AggregateDesc> &aggregate_desc) {
    return do_get_aggr_node(aggregate_desc);
  }
  ExecuteNode *get_group_node(list<SortDesc> *group_desc,
                              list<AggregateDesc> &aggregate_desc) {
    return do_get_group_node(group_desc, aggregate_desc);
  }
  ExecuteNode *get_dbscale_wise_group_node(
      list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc) {
    return do_get_dbscale_wise_group_node(group_desc, aggregate_desc);
  }
  ExecuteNode *get_dbscale_pages_node(list<SortDesc> *group_desc,
                                      list<AggregateDesc> &aggregate_desc,
                                      int page_size) {
    return do_get_dbscale_pages_node(group_desc, aggregate_desc, page_size);
  }
  ExecuteNode *get_direct_execute_node(DataSpace *dataspace, const char *sql) {
    if (this->statement->is_need_parse_transparent()) {
      return do_get_direct_execute_node(dataspace, sql);
    }
    if (this->statement->get_spark_invest_stmt())
      return get_query_mul_column_node(dataspace, sql);
    if (!this->statement->is_handle_sub_query())
      return do_get_direct_execute_node(dataspace, sql);
    switch (this->statement->get_sub_query_stmt()) {
      case SUB_SELECT_ONE_VALUE:
        return get_query_one_column_node(dataspace, sql, true);
      case SUB_SELECT_ONE_COLUMN:
        return get_query_one_column_node(dataspace, sql, false);
      case SUB_SELECT_ONE_COLUMN_MIN:
        return get_query_one_column_aggr_node(dataspace, sql, true);
      case SUB_SELECT_ONE_COLUMN_MAX:
        return get_query_one_column_aggr_node(dataspace, sql, false);
      case SUB_SELECT_EXISTS:
        return get_query_exists_node(dataspace, sql);
      default:
        throw NotSupportedError(
            "Unsupport sub query stmt type during get_direct_execute_node.");
        ;
    }
  }
  ExecuteNode *get_show_warning_node() { return do_get_show_warning_node(); }
  ExecuteNode *get_show_load_warning_node() {
    return do_get_show_load_warning_node();
  }
  ExecuteNode *get_transaction_unlock_node(const char *sql,
                                           bool is_send_to_client) {
    return do_get_transaction_unlock_node(sql, is_send_to_client);
  }
  ExecuteNode *get_xatransaction_node(const char *sql, bool is_send_to_client) {
    return do_get_xatransaction_node(sql, is_send_to_client);
  }
  ExecuteNode *get_cluster_xatransaction_node() {
    return do_get_cluster_xatransaction_node();
  }
  ExecuteNode *get_avg_node(list<AvgDesc> &avg_list) {
    return do_get_avg_node(avg_list);
  }
  ExecuteNode *get_lock_node(
      map<DataSpace *, list<lock_table_item *> *> *lock_table_list) {
    return do_get_lock_node(lock_table_list);
  }
  ExecuteNode *get_migrate_node() { return do_get_migrate_node(); }
  ExecuteNode *get_limit_node(long offset, long num) {
    return do_get_limit_node(offset, num);
  }
  ExecuteNode *get_ok_node() { return do_get_ok_node(); }
  ExecuteNode *get_ok_merge_node() { return do_get_ok_merge_node(); }
  ExecuteNode *get_modify_node(DataSpace *dataspace, const char *sql = NULL,
                               bool re_parse_shard = false) {
    session->increase_one_affected_server();
    return do_get_modify_node(dataspace, sql, re_parse_shard);
  }

  ExecuteNode *get_modify_select_node(const char *modify_sql,
                                      vector<string *> *columns, bool can_quick,
                                      vector<ExecuteNode *> *nodes,
                                      bool is_replace_set_value = false) {
    return do_get_modify_select_node(modify_sql, columns, can_quick, nodes,
                                     is_replace_set_value);
  }
  ExecuteNode *get_insert_select_node(const char *modify_sql,
                                      vector<ExecuteNode *> *nodes) {
    return do_get_insert_select_node(modify_sql, nodes);
  }
  ExecuteNode *get_par_insert_select_node(
      const char *modify_sql, PartitionedTable *par_table,
      PartitionMethod *method, vector<unsigned int> &key_pos,
      vector<ExecuteNode *> *nodes, const char *schema_name,
      const char *table_name, bool is_duplicated = false) {
    return do_get_par_insert_select_node(modify_sql, par_table, method, key_pos,
                                         nodes, schema_name, table_name,
                                         is_duplicated);
  }
  ExecuteNode *get_par_modify_select_node(const char *modify_sql,
                                          PartitionedTable *par_table,
                                          PartitionMethod *method,
                                          vector<string *> *columns,
                                          bool can_quick,
                                          vector<ExecuteNode *> *nodes) {
    return do_get_par_modify_select_node(modify_sql, par_table, method, columns,
                                         can_quick, nodes);
  }

  ExecuteNode *get_load_local_node(DataSpace *dataspace, const char *sql) {
    return do_get_load_local_node(dataspace, sql);
  }
  ExecuteNode *get_load_local_external_node(DataSpace *dataspace,
                                            DataServer *dataserver,
                                            const char *sql) {
    return do_get_load_local_external_node(dataspace, dataserver, sql);
  }
  ExecuteNode *get_load_data_infile_external_node(DataSpace *dataspace,
                                                  DataServer *dataserver) {
    return do_get_load_data_infile_external_node(dataspace, dataserver);
  }
  ExecuteNode *get_kill_node(int cluster_id, uint32_t kid) {
    return do_get_kill_node(cluster_id, kid);
  }
  ExecuteNode *get_load_local_part_table_node(PartitionedTable *dataspace,
                                              const char *sql,
                                              const char *schema_name,
                                              const char *table_name) {
    return do_get_load_local_part_table_node(dataspace, sql, schema_name,
                                             table_name);
  }
  ExecuteNode *get_load_data_infile_part_table_node(PartitionedTable *dataspace,
                                                    const char *sql,
                                                    const char *schema_name,
                                                    const char *table_name) {
    return do_get_load_data_infile_part_table_node(dataspace, sql, schema_name,
                                                   table_name);
  }
  ExecuteNode *get_load_data_infile_node(DataSpace *dataspace,
                                         const char *sql) {
    return do_get_load_data_infile_node(dataspace, sql);
  }
  ExecuteNode *get_load_select_node(const char *schema_name,
                                    const char *table_name) {
    return do_get_load_select_node(schema_name, table_name);
  }
  ExecuteNode *get_load_select_partition_node(const char *schema_name,
                                              const char *table_name) {
    return do_get_load_select_partition_node(schema_name, table_name);
  }
  ExecuteNode *get_distinct_node(list<int> column_indexes) {
    return do_get_distinct_node(column_indexes);
  }
  ExecuteNode *get_show_partition_node() {
    return do_get_show_partition_node();
  }
  ExecuteNode *get_show_user_sql_count_node(const char *user_id) {
    return do_get_show_user_sql_count_node(user_id);
  }
  ExecuteNode *get_show_status_node() { return do_get_show_status_node(); }
  ExecuteNode *get_show_catchup_node(const char *source_name) {
    return do_get_show_catchup_node(source_name);
  }
  ExecuteNode *get_dbscale_skip_wait_catchup_node(const char *source_name) {
    return do_get_dbscale_skip_wait_catchup_node(source_name);
  }
  ExecuteNode *get_show_async_task_node() {
    return do_get_show_async_task_node();
  }
  ExecuteNode *get_clean_temp_table_cache_node() {
    return do_get_clean_temp_table_cache_node();
  }
  ExecuteNode *get_explain_node(bool is_server_explain_stmt_always_extended) {
    return do_get_explain_node(is_server_explain_stmt_always_extended);
  }
  ExecuteNode *get_fetch_explain_result_node(
      list<list<string> > *explain_result,
      bool is_server_explain_stmt_always_extended) {
    return do_get_fetch_explain_result_node(
        explain_result, is_server_explain_stmt_always_extended);
  }
  ExecuteNode *get_show_virtual_map_node(PartitionedTable *tab) {
    return do_get_show_virtual_map_node(tab);
  }
  ExecuteNode *get_show_shard_map_node(PartitionedTable *tab) {
    return do_get_show_shard_map_node(tab);
  }
  ExecuteNode *get_show_auto_increment_info_node(PartitionedTable *tab) {
    return do_get_show_auto_increment_info_node(tab);
  }

  ExecuteNode *get_set_auto_increment_offset_node(PartitionedTable *tab) {
    return do_get_set_auto_increment_offset_node(tab);
  }

  ExecuteNode *get_show_transaction_sqls_node() {
    return do_get_show_transaction_sqls_node();
  }
  ExecuteNode *get_rows_node(list<list<string> *> *row_list) {
    return do_get_rows_node(row_list);
  }

  ExecuteNode *get_estimate_select_node(DataSpace *dataspace, const char *sql,
                                        Statement *statement) {
    return do_get_estimate_select_node(dataspace, sql, statement);
  }
  ExecuteNode *get_select_plain_value_node(const char *str_value,
                                           const char *alias_name) {
    return do_get_select_plain_value_node(str_value, alias_name);
  }
  ExecuteNode *get_estimate_select_partition_node(vector<unsigned int> *par_ids,
                                                  PartitionedTable *par_table,
                                                  const char *sql,
                                                  Statement *statement) {
    return do_get_estimate_select_partition_node(par_ids, par_table, sql,
                                                 statement);
  }

  ExecuteNode *get_regex_filter_node(enum FilterFlag filter_way,
                                     int column_index,
                                     list<const char *> *pattern) {
    return do_get_regex_filter_node(filter_way, column_index, pattern);
  }
  ExecuteNode *get_dbscale_cluster_shutdown_node(int cluster_id) {
    return do_get_dbscale_cluster_shutdown_node(cluster_id);
  }
  ExecuteNode *get_dbscale_request_cluster_id_node() {
    return do_get_dbscale_request_cluster_id_node();
  }
  ExecuteNode *get_dbscale_request_all_cluster_inc_info_node() {
    return do_get_dbscale_request_all_cluster_inc_info_node();
  }
  ExecuteNode *get_dbscale_request_cluster_info_node() {
    return do_get_dbscale_request_cluster_info_node();
  }
  ExecuteNode *get_dbscale_request_cluster_user_status_node(
      const char *id, bool only_show_running, bool show_status_count) {
    return do_get_dbscale_request_cluster_user_status_node(
        id, only_show_running, show_status_count);
  }
  ExecuteNode *get_dbscale_request_node_info_node() {
    return do_get_dbscale_request_node_info_node();
  }
  ExecuteNode *get_dbscale_request_cluster_inc_info_node(
      PartitionedTable *space, const char *schema_name,
      const char *table_name) {
    return do_get_dbscale_request_cluster_inc_info_node(space, schema_name,
                                                        table_name);
  }
  ExecuteNode *get_expr_filter_node(Expression *expr) {
    return do_get_expr_filter_node(expr);
  }

  ExecuteNode *get_expr_calculate_node(list<SelectExprDesc> &select_expr_list) {
    return do_get_expr_calculate_node(select_expr_list);
  }
  ExecuteNode *get_avg_show_table_status_node() {
    return do_get_avg_show_table_status_node();
  }
  ExecuteNode *get_show_data_source_node(list<const char *> &names,
                                         bool need_show_weight) {
    return do_get_show_data_source_node(names, need_show_weight);
  }
  ExecuteNode *get_dynamic_configuration_node() {
    return do_get_dynamic_configuration_node();
  }
  ExecuteNode *get_rep_strategy_node() { return do_get_rep_strategy_node(); }
  ExecuteNode *get_set_server_weight_node() {
    return do_get_set_server_weight_node();
  }
  ExecuteNode *get_show_option_node() { return do_get_show_option_node(); }
  ExecuteNode *get_show_dynamic_option_node() {
    return do_get_show_dynamic_option_node();
  }
  ExecuteNode *get_change_startup_config_node() {
    return do_get_change_startup_config_node();
  }
  ExecuteNode *get_show_changed_startup_config_node() {
    return do_get_show_changed_startup_config_node();
  }
  ExecuteNode *get_dbscale_global_consistence_point_node() {
    return do_get_dbscale_global_consistence_point_node();
  }
  ExecuteNode *get_dynamic_add_data_server_node(
      dynamic_add_data_server_op_node *dynamic_add_data_server_oper) {
    return do_get_dynamic_add_data_server_node(dynamic_add_data_server_oper);
  }
  ExecuteNode *get_dynamic_add_data_source_node(
      dynamic_add_data_source_op_node *dynamic_add_data_source_oper,
      DataSourceType type) {
    return do_get_dynamic_add_data_source_node(dynamic_add_data_source_oper,
                                               type);
  }
  ExecuteNode *get_dynamic_add_data_space_node(
      dynamic_add_data_space_op_node *dynamic_add_data_oper) {
    return do_get_dynamic_add_data_space_node(dynamic_add_data_oper);
  }
  ExecuteNode *get_set_user_not_allow_operation_time_node(
      set_user_not_allow_operation_time_node *oper) {
    return do_get_set_user_not_allow_operation_time_node(oper);
  }
  ExecuteNode *get_reload_user_not_allow_operation_time_node(string user_name) {
    return do_get_reload_user_not_allow_operation_time_node(user_name);
  }
  ExecuteNode *get_show_user_not_allow_operation_time_node(string user_name) {
    return do_get_show_user_not_allow_operation_time_node(user_name);
  }
  ExecuteNode *get_set_schema_acl_node(
      set_schema_acl_op_node *set_schema_acl_oper) {
    return do_get_set_schema_acl_node(set_schema_acl_oper);
  }
  ExecuteNode *get_set_table_acl_node(
      set_table_acl_op_node *set_table_acl_oper) {
    return do_get_set_table_acl_node(set_table_acl_oper);
  }
  ExecuteNode *get_dynamic_add_slave_node(
      dynamic_add_slave_op_node *dynamic_add_slave_oper) {
    return do_get_dynamic_add_slave_node(dynamic_add_slave_oper);
  }
  ExecuteNode *get_reset_tmp_table_node() {
    return do_get_reset_tmp_table_node();
  }
  ExecuteNode *get_reload_function_type_node() {
    return do_get_reload_function_type_node();
  }
  ExecuteNode *get_shutdown_node() { return do_get_shutdown_node(); }
  ExecuteNode *get_dbscale_set_pool_info_node(pool_node *pool_info) {
    return do_get_dbscale_set_pool_info_node(pool_info);
  }
  ExecuteNode *get_dbscale_reset_info_plan(dbscale_reset_info_op_node *oper) {
    return do_get_dbscale_reset_info_plan(oper);
  }
  ExecuteNode *get_dbscale_block_node() { return do_get_dbscale_block_node(); }
  ExecuteNode *get_dbscale_disable_server_node() {
    return do_get_dbscale_disable_server_node();
  }
  ExecuteNode *get_dbscale_check_table_node() {
    return do_get_dbscale_check_table_node();
  }
  ExecuteNode *get_dbscale_check_disk_io_node() {
    return do_get_dbscale_check_disk_io_node();
  }
  ExecuteNode *get_dbscale_check_metadata() {
    return do_get_dbscale_check_metadata();
  }
  ExecuteNode *get_dbscale_flush_pool_info_node(pool_node *pool_info) {
    return do_get_dbscale_flush_pool_info_node(pool_info);
  }
  ExecuteNode *get_dbscale_force_flashback_online_node(const char *name) {
    return do_get_dbscale_force_flashback_online_node(name);
  }
  ExecuteNode *get_dbscale_xa_recover_slave_dbscale_node(
      const char *xa_source, const char *top_source, const char *ka_update_v) {
    return do_get_dbscale_xa_recover_slave_dbscale_node(xa_source, top_source,
                                                        ka_update_v);
  }
  ExecuteNode *get_dbscale_purge_connection_pool_node(pool_node *pool_info) {
    return do_get_dbscale_purge_connection_pool_node(pool_info);
  }
  ExecuteNode *get_dbscale_flush_node(dbscale_flush_type type) {
    return do_get_dbscale_flush_node(type);
  }
  ExecuteNode *get_dbscale_flush_weak_pwd_node() {
    return do_get_dbscale_flush_weak_pwd_node();
  }
  ExecuteNode *get_dbscale_reload_config_node(const char *sql) {
    return do_get_dbscale_reload_config_node(sql);
  }
  ExecuteNode *get_dbscale_set_priority_info_node(string user_name,
                                                  int tmp_priority_value) {
    return do_get_dbscale_set_priority_info_node(user_name, tmp_priority_value);
  }
  ExecuteNode *get_dynamic_change_master_node(
      dynamic_change_master_op_node *dynamic_change_master_oper) {
    return do_get_dynamic_change_master_node(dynamic_change_master_oper);
  }
  ExecuteNode *get_dynamic_change_multiple_master_active_node(
      dynamic_change_multiple_master_active_op_node
          *dynamic_change_multiple_master_active_oper) {
    return do_get_dynamic_change_multiple_master_active_node(
        dynamic_change_multiple_master_active_oper);
  }
  ExecuteNode *get_dynamic_change_dataserver_ssh_node(const char *server_name,
                                                      const char *username,
                                                      const char *pwd,
                                                      int port) {
    return do_get_dynamic_change_dataserver_ssh_node(server_name, username, pwd,
                                                     port);
  }
  ExecuteNode *get_dynamic_remove_slave_node(
      dynamic_remove_slave_op_node *dynamic_remove_slave_oper) {
    return do_get_dynamic_remove_slave_node(dynamic_remove_slave_oper);
  }
  ExecuteNode *get_dynamic_remove_schema_node(const char *schema_name,
                                              bool is_force) {
    return do_get_dynamic_remove_schema_node(schema_name, is_force);
  }
  ExecuteNode *get_dynamic_remove_table_node(const char *table_name,
                                             bool is_force) {
    return do_get_dynamic_remove_table_node(table_name, is_force);
  }
  ExecuteNode *get_dynamic_change_table_scheme_node(const char *table_name,
                                                    const char *scheme_name) {
    return do_get_dynamic_change_table_scheme_node(table_name, scheme_name);
  }
  ExecuteNode *get_dynamic_remove_node(const char *name) {
    return do_get_dynamic_remove_node(name);
  }
  ExecuteNode *get_dbscale_help_node(const char *name) {
    return do_get_dbscale_help_node(name);
  }
  ExecuteNode *get_dbscale_show_audit_user_list_node() {
    return do_get_dbscale_show_audit_user_list_node();
  }
  ExecuteNode *get_dbscale_show_slow_sql_top_n_node() {
    return do_get_dbscale_show_slow_sql_top_n_node();
  }
  ExecuteNode *get_dbscale_request_slow_sql_top_n_node() {
    return do_get_dbscale_request_slow_sql_top_n_node();
  }
  ExecuteNode *get_dbscale_show_join_node(const char *name) {
    return do_get_dbscale_show_join_node(name);
  }
  ExecuteNode *get_dbscale_show_shard_partition_node(const char *name) {
    return do_get_dbscale_show_shard_partition_node(name);
  }
  ExecuteNode *get_dbscale_show_rebalance_work_load_node(
      const char *name, list<string> sources, const char *schema_name,
      int is_remove) {
    return do_get_dbscale_show_rebalance_work_load_node(name, sources,
                                                        schema_name, is_remove);
  }
  ExecuteNode *get_show_user_memory_status_node() {
    return do_get_show_user_memory_status_node();
  }
  ExecuteNode *get_show_user_status_node(const char *user_id,
                                         const char *user_name,
                                         bool only_show_running, bool instance,
                                         bool show_status_count) {
    return do_get_show_user_status_node(user_id, user_name, only_show_running,
                                        instance, show_status_count);
  }
  ExecuteNode *get_show_user_processlist_node(const char *cluster_id,
                                              const char *user_id, int local) {
    return do_get_show_user_processlist_node(cluster_id, user_id, local);
  }
  ExecuteNode *get_show_session_id_node(const char *server_name,
                                        int connection_id) {
    return do_get_show_session_id_node(server_name, connection_id);
  }
  ExecuteNode *get_backend_server_execute_node(const char *stmt_sql) {
    return do_get_backend_server_execute_node(stmt_sql);
  }
  ExecuteNode *get_execute_on_all_masterserver_execute_node(
      const char *stmt_sql) {
    return do_get_execute_on_all_masterserver_execute_node(stmt_sql);
  }
  ExecuteNode *get_dbscale_erase_auth_info_node(name_item *username_list,
                                                bool all_dbscale_node) {
    return do_get_dbscale_erase_auth_info_node(username_list, all_dbscale_node);
  }
  ExecuteNode *get_show_table_location_node(const char *schema_name,
                                            const char *table_name) {
    return do_get_show_table_location_node(schema_name, table_name);
  }
  ExecuteNode *get_dynamic_update_white_node(
      bool is_add, const char *ip, const ssl_option_struct &ssl_option_value,
      const char *comment, const char *user_name) {
    return do_get_dynamic_update_white_node(is_add, ip, ssl_option_value,
                                            comment, user_name);
  }
  ExecuteNode *get_dbscale_show_base_status_node() {
    return do_get_dbscale_show_base_status_node();
  }
  ExecuteNode *get_show_monitor_point_status_node() {
    return do_get_show_monitor_point_status_node();
  }
  ExecuteNode *get_show_global_monitor_point_status_node() {
    return do_get_show_global_monitor_point_status_node();
  }
  ExecuteNode *get_show_histogram_monitor_point_status_node() {
    return do_get_show_histogram_monitor_point_status_node();
  }
  ExecuteNode *get_show_outline_monitor_info_node() {
    return do_get_show_outline_monitor_info_node();
  }
  ExecuteNode *get_dbscale_create_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) {
    return do_get_dbscale_create_outline_hint_node(oper);
  }
  ExecuteNode *get_dbscale_flush_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) {
    return do_get_dbscale_flush_outline_hint_node(oper);
  }
  ExecuteNode *get_dbscale_show_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) {
    return do_get_dbscale_show_outline_hint_node(oper);
  }
  ExecuteNode *get_dbscale_delete_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) {
    return do_get_dbscale_delete_outline_hint_node(oper);
  }
  ExecuteNode *get_com_query_prepare_node(DataSpace *dataspace,
                                          const char *query_sql,
                                          const char *name, const char *sql) {
    return do_get_com_query_prepare_node(dataspace, query_sql, name, sql);
  }
  ExecuteNode *get_com_query_exec_prepare_node(const char *name,
                                               var_item *var_item_list) {
    return do_get_com_query_exec_prepare_node(name, var_item_list);
  }
  ExecuteNode *get_com_query_drop_prepare_node(DataSpace *dataspace,
                                               const char *name,
                                               const char *prepare_sql) {
    return do_get_com_query_drop_prepare_node(dataspace, name, prepare_sql);
  }
  ExecuteNode *get_dbscale_flush_config_to_file_node(const char *file_name,
                                                     bool flush_all) {
    return do_get_dbscale_flush_config_to_file_node(file_name, flush_all);
  }
  ExecuteNode *get_dbscale_flush_acl_node() {
    return do_get_dbscale_flush_acl_node();
  }
  ExecuteNode *get_dbscale_flush_table_info_node(const char *schema_name,
                                                 const char *table_name) {
    return do_get_dbscale_flush_table_info_node(schema_name, table_name);
  }
  ExecuteNode *get_show_pool_info_node() {
    return do_get_show_pool_info_node();
  }
  ExecuteNode *get_show_pool_version_node() {
    return do_get_show_pool_version_node();
  }
  ExecuteNode *get_show_execution_profile_node() {
    return do_get_show_execution_profile_node();
  }
  ExecuteNode *get_show_lock_usage_node() {
    return do_get_show_lock_usage_node();
  }
  ExecuteNode *get_show_engine_lock_waiting_node(int engine_type) {
    return do_get_show_engine_lock_waiting_node(engine_type);
  }
  ExecuteNode *get_show_backend_threads_node() {
    return do_get_show_backend_threads_node();
  }

  ExecuteNode *get_show_dataserver_node() {
    return do_get_show_dataserver_node();
  }
  ExecuteNode *get_show_partition_scheme_node() {
    return do_get_show_partition_scheme_node();
  }
  ExecuteNode *get_show_schema_node(const char *schema) {
    return do_get_show_schema_node(schema);
  }
  ExecuteNode *get_show_table_node(const char *schema, const char *table,
                                   bool use_like) {
    return do_get_show_table_node(schema, table, use_like);
  }

  ExecuteNode *get_migrate_clean_node(const char *migrate_id) {
    return do_get_migrate_clean_node(migrate_id);
  }

  ExecuteNode *get_rename_table_node(PartitionedTable *old_table,
                                     PartitionedTable *new_table,
                                     const char *old_schema_name,
                                     const char *old_table_name,
                                     const char *new_schema_name,
                                     const char *new_table_name) {
    return do_get_rename_table_node(old_table, new_table, old_schema_name,
                                    old_table_name, new_schema_name,
                                    new_table_name);
  }

  ExecuteNode *get_mul_modify_node(
      map<DataSpace *, const char *> &spaces_map,
      bool need_handle_more_result = false,
      map<DataSpace *, int> *sql_count_map = NULL) {
    return do_get_mul_modify_node(spaces_map, sql_count_map,
                                  need_handle_more_result);
  }
  ExecuteNode *get_set_node(DataSpace *dataspace, const char *sql) {
    return do_get_set_node(dataspace, sql);
  }
  ExecuteNode *get_create_oracle_sequence_node(
      create_oracle_seq_op_node *oper) {
    return do_get_create_oracle_sequence_node(oper);
  }
  ExecuteNode *get_union_all_node(vector<ExecuteNode *> &nodes) {
    return do_get_union_all_node(nodes);
  }
  ExecuteNode *get_union_group_node(vector<list<ExecuteNode *> > &nodes) {
    return do_get_union_group_node(nodes);
  }
  void send_error_packet() { return do_send_error_packet(); }
  void set_start_node(ExecuteNode *node) { start_node = node; }
  ExecuteNode *get_start_node() { return start_node; }
  ExecuteNode *get_drop_mul_table_node() {
    return do_get_drop_mul_table_node();
  }
  ExecuteNode *get_send_dbscale_one_column_node(bool only_one_row) {
    return do_get_send_dbscale_one_column_node(only_one_row);
  }
  ExecuteNode *get_send_dbscale_mul_column_node() {
    return do_get_send_dbscale_mul_column_node();
  }

  ExecuteNode *get_query_one_column_node(DataSpace *dataspace, const char *sql,
                                         bool only_one_row) {
    return do_get_query_one_column_node(dataspace, sql, only_one_row);
  }
  ExecuteNode *get_query_mul_column_node(DataSpace *dataspace,
                                         const char *sql) {
    return do_get_query_mul_column_node(dataspace, sql);
  }
  ExecuteNode *get_send_dbscale_one_column_aggr_node(bool get_min) {
    return do_get_send_dbscale_one_column_aggr_node(get_min);
  }
  ExecuteNode *get_send_dbscale_exists_node() {
    return do_get_send_dbscale_exists_node();
  }
  ExecuteNode *get_query_one_column_aggr_node(DataSpace *dataspace,
                                              const char *sql, bool get_min) {
    return do_get_query_one_column_aggr_node(dataspace, sql, get_min);
  }
  ExecuteNode *get_query_exists_node(DataSpace *dataspace, const char *sql) {
    return do_get_query_exists_node(dataspace, sql);
  }
  ExecuteNode *get_keepmaster_node() { return do_get_keepmaster_node(); }
  ExecuteNode *get_dbscale_test_node() { return do_get_dbscale_test_node(); }
  ExecuteNode *get_async_task_control_node(unsigned long long id) {
    return do_get_async_task_control_node(id);
  }
  ExecuteNode *get_load_dataspace_file_node(const char *filename) {
    return do_get_load_dataspace_file_node(filename);
  }
  ExecuteNode *get_call_sp_return_ok_node() {
    return do_get_call_sp_return_ok_node();
  }
  ExecuteNode *get_dbscale_purge_monitor_point_node() {
    return do_get_dbscale_purge_monitor_point_node();
  }
  ExecuteNode *get_dbscale_clean_monitor_point_node() {
    return do_get_dbscale_clean_monitor_point_node();
  }
  ExecuteNode *get_dbscale_show_default_session_var_node() {
    return do_get_dbscale_show_default_session_var_node();
  }
  ExecuteNode *get_dbscale_show_version_node() {
    return do_get_dbscale_show_version_node();
  }
  ExecuteNode *get_show_version_node() { return do_get_show_version_node(); }
  ExecuteNode *get_show_components_version_node() {
    return do_get_show_components_version_node();
  }
  ExecuteNode *get_dbscale_show_hostname_node() {
    return do_get_dbscale_show_hostname_node();
  }
  ExecuteNode *get_dbscale_show_all_fail_transaction_node() {
    return do_get_dbscale_show_all_fail_transaction_node();
  }
  ExecuteNode *get_dbscale_show_detail_fail_transaction_node(const char *xid) {
    return do_get_dbscale_show_detail_fail_transaction_node(xid);
  }
  ExecuteNode *get_dbscale_show_partition_table_status_node(
      const char *table_name) {
    return do_get_dbscale_show_partition_table_status_node(table_name);
  }
  ExecuteNode *get_dbscale_show_schema_acl_info_node(bool is_show_all) {
    return do_get_dbscale_show_schema_acl_info_node(is_show_all);
  }
  ExecuteNode *get_dbscale_show_table_acl_info_node(bool is_show_all) {
    return do_get_dbscale_show_table_acl_info_node(is_show_all);
  }
  ExecuteNode *get_dbscale_show_path_info() {
    return do_get_dbscale_show_path_info();
  }
  ExecuteNode *get_dbscale_show_warnings_node() {
    return do_get_dbscale_show_warnings_node();
  }
  ExecuteNode *get_dbscale_show_critical_errors_node() {
    return do_get_dbscale_show_critical_errors_node();
  }
  ExecuteNode *get_dbscale_clean_fail_transaction_node(const char *xid) {
    return do_get_dbscale_clean_fail_transaction_node(xid);
  }
  ExecuteNode *get_dbscale_mul_sync_node(const char *sync_topic,
                                         const char *sync_state,
                                         const char *sync_param,
                                         const char *sync_cond,
                                         unsigned long version_id) {
    return do_get_dbscale_mul_sync_node(sync_topic, sync_state, sync_param,
                                        sync_cond, version_id);
  }
  ExecuteNode *get_dbscale_add_default_session_var_node(DataSpace *dataspace) {
    return do_get_dbscale_add_default_session_var_node(dataspace);
  }
  ExecuteNode *get_dbscale_remove_default_session_var_node() {
    return do_get_dbscale_remove_default_session_var_node();
  }
  ExecuteNode *get_federated_table_node() {
    return do_get_federated_table_node();
  }
  ExecuteNode *get_empty_set_node(int field_num) {
    return do_get_empty_set_node(field_num);
  }
  ExecuteNode *get_cursor_direct_node(DataSpace *dataspace, const char *sql) {
    return do_get_cursor_direct_node(dataspace, sql);
  }
  ExecuteNode *get_cursor_send_node() { return do_get_cursor_send_node(); }
  ExecuteNode *get_slave_dbscale_error_node() {
    LOG_DEBUG("get slave_dbscale_error_node\n");
    return do_get_slave_dbscale_error_node();
  }
  ExecuteNode *get_return_ok_node() { return do_get_return_ok_node(); }
  ExecuteNode *get_forward_master_role_node(const char *sql,
                                            bool is_slow_query) {
    return do_get_forward_master_role_node(sql, is_slow_query);
  }
  ExecuteNode *get_set_info_schema_mirror_tb_status_node(
      info_mirror_tb_status_node *tb_status) {
    return do_get_set_info_schema_mirror_tb_status_node(tb_status);
  }
  ExecuteNode *get_reset_zoo_info_node() {
    return do_get_reset_zoo_info_node();
  }
#ifndef CLOSE_ZEROMQ
  ExecuteNode *get_task_node() { return do_get_task_node(); }
  ExecuteNode *get_binlog_task_node() { return do_get_binlog_task_node(); }
  ExecuteNode *get_message_service_node() {
    return do_get_message_service_node();
  }
  ExecuteNode *get_binlog_task_add_filter_node() {
    return do_get_binlog_task_add_filter_node();
  }
  ExecuteNode *get_drop_task_filter_node() {
    return do_get_drop_task_filter_node();
  }
  ExecuteNode *get_drop_task_node() { return do_get_drop_task_node(); }
  ExecuteNode *get_show_client_task_status_node(const char *task_name,
                                                const char *server_task_name) {
    return do_get_show_client_task_status_node(task_name, server_task_name);
  }
  ExecuteNode *get_show_server_task_status_node(const char *task_name) {
    return do_get_show_server_task_status_node(task_name);
  }
#endif
  ExecuteNode *get_dbscale_update_audit_user_node(const char *username,
                                                  bool is_add) {
    return do_get_dbscale_update_audit_user_node(username, is_add);
  }
  ExecuteNode *get_connect_by_node(ConnectByDesc connect_by_desc) {
    return do_get_connect_by_node(connect_by_desc);
  }
  ExecuteNode *get_dbscale_show_create_oracle_seq_node(const char *seq_schema,
                                                       const char *seq_name) {
    return do_get_dbscale_show_create_oracle_seq_node(seq_schema, seq_name);
  }
  ExecuteNode *get_dbscale_show_seq_status_node(const char *seq_schema,
                                                const char *seq_name) {
    return do_get_dbscale_show_seq_status_node(seq_schema, seq_name);
  }
  ExecuteNode *get_dbscale_show_fetchnode_buffer_usage_node() {
    return do_get_dbscale_show_fetchnode_buffer_usage_node();
  }
  ExecuteNode *get_dbscale_show_trx_block_info_node(bool is_local) {
    return do_get_dbscale_show_trx_block_info_node(is_local);
  }
  ExecuteNode *get_dbscale_internal_set_node() {
    return do_get_dbscale_internal_set_node();
  }
  ExecuteNode *get_restore_recycle_table_precheck_node(const char *from_schema,
                                                       const char *from_table,
                                                       const char *to_table,
                                                       const char *recycle_type,
                                                       DataSpace *dspace) {
    return do_get_restore_recycle_table_precheck_node(
        from_schema, from_table, to_table, recycle_type, dspace);
  }

  ExecuteNode *get_dbscale_show_prepare_cache_status_node() {
    return do_get_dbscale_show_prepare_cache_node();
  }

  ExecuteNode *get_dbscale_flush_prepare_cache_hit_node() {
    return do_get_dbscale_flush_prepare_cache_hit_node();
  }

  ExecuteNode *get_empty_node() { return do_get_empty_node(); }

  bool handle_sp_condition(bool keep_conn_old, const char *error_msg = NULL,
                           const char *table_info_error_sqlstate = NULL,
                           unsigned int table_info_error_code = 0);
  void set_migrate_tool(MigrateTool *t) { migrate_tool = t; }

  MigrateTool *get_migrate_tool() { return migrate_tool; }

  void clean_start_node();
  void clean_plan() { clean(); }

  bool is_federated() { return federated; }
  void set_federated(bool b) { federated = b; }
  bool is_dispatch_federated() { return dispatch_federated; }
  void set_dispatch_federated(bool b) { dispatch_federated = b; }

  bool is_send_cluster_xa_transaction_ddl_error_packet() {
    return send_cluster_xa_transaction_ddl_error_packet;
  }
  void set_send_cluster_xa_transaction_ddl_error_packet(bool b) {
    send_cluster_xa_transaction_ddl_error_packet = b;
  }

  void add_session_traditional_num() {
    if (!added_traditional_num)
      added_traditional_num = session->add_traditional_num();
  }

  void set_connection_swaped_out(bool swaped_out) {
    connection_swaped_out = swaped_out;
  }

  bool get_connection_swaped_out() { return connection_swaped_out; }

  void set_fetch_node_no_thread(bool no_thread) {
    fetch_node_no_thread = no_thread;
  }

  bool get_fetch_node_no_thread() { return fetch_node_no_thread; }
  void set_cancel_execute(bool b) { cancel_execute = b; }

 private:
  virtual void clean();
  virtual ExecuteNode *do_get_query_one_column_node(DataSpace *dataspace,
                                                    const char *sql,
                                                    bool only_one_row) = 0;
  virtual ExecuteNode *do_get_query_mul_column_node(DataSpace *dataspace,
                                                    const char *sql) = 0;
  virtual ExecuteNode *do_get_query_one_column_aggr_node(DataSpace *dataspace,
                                                         const char *sql,
                                                         bool get_min) = 0;
  virtual ExecuteNode *do_get_query_exists_node(DataSpace *dataspace,
                                                const char *sql) = 0;
  virtual ExecuteNode *do_get_fetch_node(DataSpace *dataspace,
                                         const char *sql) = 0;
  virtual ExecuteNode *do_get_navicat_profile_sql_node() = 0;
  virtual ExecuteNode *do_get_send_node() = 0;
#ifndef DBSCALE_DISABLE_SPARK
  virtual ExecuteNode *do_get_spark_node(SparkConfigParameter config,
                                         bool is_insert) = 0;
#endif
  virtual ExecuteNode *do_get_dispatch_packet_node(TransferRule *rule) = 0;
  virtual ExecuteNode *do_get_into_outfile_node() = 0;
  virtual ExecuteNode *do_get_select_into_node() = 0;
  virtual ExecuteNode *do_get_modify_limit_node(record_scan *rs,
                                                PartitionedTable *table,
                                                vector<unsigned int> &par_ids,
                                                unsigned int limit,
                                                string &sql) = 0;
  virtual ExecuteNode *do_get_sort_node(list<SortDesc> *sort_desc) = 0;
  virtual ExecuteNode *do_get_single_sort_node(list<SortDesc> *sort_desc) = 0;
  virtual ExecuteNode *do_get_project_node(unsigned int skip_columns) = 0;
  virtual ExecuteNode *do_get_aggr_node(
      list<AggregateDesc> &aggregate_desc) = 0;
  virtual ExecuteNode *do_get_group_node(
      list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc) = 0;
  virtual ExecuteNode *do_get_dbscale_wise_group_node(
      list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc) = 0;
  virtual ExecuteNode *do_get_dbscale_pages_node(
      list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc,
      int page_size) = 0;
  virtual ExecuteNode *do_get_direct_execute_node(DataSpace *dataspace,
                                                  const char *sql) = 0;

  virtual ExecuteNode *do_get_show_warning_node() = 0;
  virtual ExecuteNode *do_get_show_load_warning_node() = 0;
  virtual ExecuteNode *do_get_transaction_unlock_node(
      const char *sql, bool is_send_to_client) = 0;
  virtual ExecuteNode *do_get_estimate_select_node(DataSpace *dataspace,
                                                   const char *sql,
                                                   Statement *statement) = 0;
  virtual ExecuteNode *do_get_select_plain_value_node(
      const char *str_value, const char *alias_name) = 0;
  virtual ExecuteNode *do_get_estimate_select_partition_node(
      vector<unsigned int> *par_ids, PartitionedTable *par_table,
      const char *sql, Statement *statement) = 0;

  virtual ExecuteNode *do_get_xatransaction_node(const char *sql,
                                                 bool is_send_to_client) = 0;
  virtual ExecuteNode *do_get_cluster_xatransaction_node() = 0;
  virtual ExecuteNode *do_get_avg_node(list<AvgDesc> &avg_list) = 0;
  virtual ExecuteNode *do_get_lock_node(
      map<DataSpace *, list<lock_table_item *> *> *lock_table_list) = 0;
  virtual ExecuteNode *do_get_migrate_node() = 0;
  virtual ExecuteNode *do_get_limit_node(long offset, long num) = 0;
  virtual ExecuteNode *do_get_ok_node() = 0;
  virtual ExecuteNode *do_get_ok_merge_node() = 0;
  virtual ExecuteNode *do_get_modify_node(DataSpace *dataspace, const char *sql,
                                          bool re_parse_shard) = 0;
  virtual ExecuteNode *do_get_modify_select_node(const char *modify_sql,
                                                 vector<string *> *columns,
                                                 bool can_quick,
                                                 vector<ExecuteNode *> *nodes,
                                                 bool is_replace_set_value) = 0;
  virtual ExecuteNode *do_get_insert_select_node(
      const char *modify_sql, vector<ExecuteNode *> *nodes) = 0;
  virtual ExecuteNode *do_get_par_modify_select_node(
      const char *modify_sql, PartitionedTable *par_table,
      PartitionMethod *method, vector<string *> *columns, bool can_quick,
      vector<ExecuteNode *> *nodes) = 0;
  virtual ExecuteNode *do_get_par_insert_select_node(
      const char *modify_sql, PartitionedTable *par_table,
      PartitionMethod *method, vector<unsigned int> &key_pos,
      vector<ExecuteNode *> *nodes, const char *schema_name,
      const char *table_name, bool is_duplicated) = 0;

  virtual ExecuteNode *do_get_load_local_node(DataSpace *dataspace,
                                              const char *sql) = 0;
  virtual ExecuteNode *do_get_show_user_sql_count_node(const char *user_id) = 0;
  virtual ExecuteNode *do_get_load_local_external_node(DataSpace *dataspace,
                                                       DataServer *dataserver,
                                                       const char *sql) = 0;
  virtual ExecuteNode *do_get_load_data_infile_external_node(
      DataSpace *dataspace, DataServer *dataserver) = 0;
  virtual ExecuteNode *do_get_kill_node(int cluster_id, uint32_t kid) = 0;
  virtual ExecuteNode *do_get_load_local_part_table_node(
      PartitionedTable *dataspace, const char *sql, const char *schema_name,
      const char *table_name) = 0;
  virtual ExecuteNode *do_get_load_data_infile_part_table_node(
      PartitionedTable *dataspace, const char *sql, const char *schema_name,
      const char *table_name) = 0;
  virtual ExecuteNode *do_get_load_data_infile_node(DataSpace *dataspace,
                                                    const char *sql) = 0;
  virtual ExecuteNode *do_get_load_select_node(const char *schema_name,
                                               const char *table_name) = 0;
  virtual ExecuteNode *do_get_load_select_partition_node(
      const char *schema_name, const char *table_name) = 0;
  virtual ExecuteNode *do_get_distinct_node(list<int> column_indexes) = 0;
  virtual ExecuteNode *do_get_show_partition_node() = 0;
  virtual ExecuteNode *do_get_show_status_node() = 0;
  virtual ExecuteNode *do_get_show_catchup_node(const char *source_name) = 0;
  virtual ExecuteNode *do_get_dbscale_skip_wait_catchup_node(
      const char *source_name) = 0;
  virtual ExecuteNode *do_get_show_async_task_node() = 0;
  virtual ExecuteNode *do_get_clean_temp_table_cache_node() = 0;
  virtual ExecuteNode *do_get_explain_node(bool) = 0;
  virtual ExecuteNode *do_get_fetch_explain_result_node(
      list<list<string> > *result,
      bool is_server_explain_stmt_always_extended) = 0;
  virtual ExecuteNode *do_get_show_virtual_map_node(PartitionedTable *tab) = 0;
  virtual ExecuteNode *do_get_show_shard_map_node(PartitionedTable *tab) = 0;
  virtual ExecuteNode *do_get_show_auto_increment_info_node(
      PartitionedTable *tab) = 0;
  virtual ExecuteNode *do_get_set_auto_increment_offset_node(
      PartitionedTable *tab) = 0;
  virtual ExecuteNode *do_get_show_transaction_sqls_node() = 0;
  virtual ExecuteNode *do_get_rows_node(list<list<string> *> *row_list) = 0;
  virtual ExecuteNode *do_get_dbscale_cluster_shutdown_node(int cluster_id) = 0;
  virtual ExecuteNode *do_get_dbscale_request_cluster_id_node() = 0;
  virtual ExecuteNode *do_get_dbscale_request_all_cluster_inc_info_node() = 0;
  virtual ExecuteNode *do_get_dbscale_request_cluster_info_node() = 0;
  virtual ExecuteNode *do_get_dbscale_flush_node(dbscale_flush_type type) = 0;
  virtual ExecuteNode *do_get_dbscale_flush_weak_pwd_node() = 0;
  virtual ExecuteNode *do_get_dbscale_request_cluster_user_status_node(
      const char *id, bool only_show_running, bool show_status_count) = 0;
  virtual ExecuteNode *do_get_dbscale_request_node_info_node() = 0;
  virtual ExecuteNode *do_get_dbscale_request_cluster_inc_info_node(
      PartitionedTable *space, const char *schema_name,
      const char *table_name) = 0;
  virtual ExecuteNode *do_get_regex_filter_node(
      enum FilterFlag filter_way, int index_index,
      list<const char *> *pattern) = 0;
  virtual ExecuteNode *do_get_expr_filter_node(Expression *expr) = 0;
  virtual ExecuteNode *do_get_expr_calculate_node(
      list<SelectExprDesc> &select_expr_list) = 0;
  virtual ExecuteNode *do_get_avg_show_table_status_node() = 0;
  virtual ExecuteNode *do_get_com_query_prepare_node(DataSpace *dataspace,
                                                     const char *query_sql,
                                                     const char *name,
                                                     const char *sql) = 0;
  virtual ExecuteNode *do_get_com_query_exec_prepare_node(
      const char *name, var_item *var_item_list) = 0;
  virtual ExecuteNode *do_get_com_query_drop_prepare_node(
      DataSpace *dataspace, const char *name, const char *prepare_sql) = 0;
  virtual ExecuteNode *do_get_dbscale_flush_config_to_file_node(
      const char *file_name, bool flush_all) = 0;
  virtual ExecuteNode *do_get_dbscale_flush_acl_node() = 0;
  virtual ExecuteNode *do_get_dbscale_flush_table_info_node(
      const char *schema_name, const char *table_name) = 0;
  virtual ExecuteNode *do_get_show_user_memory_status_node() = 0;
  virtual ExecuteNode *do_get_show_user_status_node(const char *user_id,
                                                    const char *user_name,
                                                    bool only_show_running,
                                                    bool instance,
                                                    bool show_status_count) = 0;
  virtual ExecuteNode *do_get_show_user_processlist_node(const char *cluster_id,
                                                         const char *user_id,
                                                         int local) = 0;
  virtual ExecuteNode *do_get_show_session_id_node(const char *server_name,
                                                   int connection_id) = 0;
  virtual ExecuteNode *do_get_backend_server_execute_node(
      const char *stmt_sql) = 0;
  virtual ExecuteNode *do_get_execute_on_all_masterserver_execute_node(
      const char *stmt_sql) = 0;
  virtual ExecuteNode *do_get_show_table_location_node(
      const char *schema_name, const char *table_name) = 0;
  virtual ExecuteNode *do_get_dbscale_erase_auth_info_node(
      name_item *username_list, bool all_dbscale_node) = 0;
  virtual ExecuteNode *do_get_dynamic_update_white_node(
      bool is_add, const char *ip, const ssl_option_struct &ssl_option_value,
      const char *comment, const char *user_name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_base_status_node() = 0;
  virtual ExecuteNode *do_get_dynamic_add_slave_node(
      dynamic_add_slave_op_node *dynamic_add_slave_oper) = 0;
  virtual ExecuteNode *do_get_dynamic_add_data_server_node(
      dynamic_add_data_server_op_node *dynamic_add_data_server_oper) = 0;
  virtual ExecuteNode *do_get_dynamic_add_data_source_node(
      dynamic_add_data_source_op_node *dynamic_add_data_source_oper,
      DataSourceType type) = 0;
  virtual ExecuteNode *do_get_dynamic_add_data_space_node(
      dynamic_add_data_space_op_node *dynamic_add_data_space_oper) = 0;
  virtual ExecuteNode *do_get_set_schema_acl_node(
      set_schema_acl_op_node *set_schema_acl_oper) = 0;
  virtual ExecuteNode *do_get_set_user_not_allow_operation_time_node(
      set_user_not_allow_operation_time_node *oper) = 0;
  virtual ExecuteNode *do_get_reload_user_not_allow_operation_time_node(
      string user_name) = 0;
  virtual ExecuteNode *do_get_show_user_not_allow_operation_time_node(
      string user_name) = 0;
  virtual ExecuteNode *do_get_set_table_acl_node(
      set_table_acl_op_node *set_table_acl_oper) = 0;
  virtual ExecuteNode *do_get_reset_tmp_table_node() = 0;
  virtual ExecuteNode *do_get_reload_function_type_node() = 0;
  virtual ExecuteNode *do_get_shutdown_node() = 0;
  virtual ExecuteNode *do_get_dbscale_set_pool_info_node(
      pool_node *pool_info) = 0;
  virtual ExecuteNode *do_get_dbscale_reset_info_plan(
      dbscale_reset_info_op_node *oper) = 0;
  virtual ExecuteNode *do_get_dbscale_block_node() = 0;
  virtual ExecuteNode *do_get_dbscale_disable_server_node() = 0;
  virtual ExecuteNode *do_get_dbscale_check_table_node() = 0;
  virtual ExecuteNode *do_get_dbscale_check_disk_io_node() = 0;
  virtual ExecuteNode *do_get_dbscale_check_metadata() = 0;
  virtual ExecuteNode *do_get_dbscale_flush_pool_info_node(
      pool_node *pool_info) = 0;
  virtual ExecuteNode *do_get_dbscale_force_flashback_online_node(
      const char *name) = 0;
  virtual ExecuteNode *do_get_dbscale_xa_recover_slave_dbscale_node(
      const char *xa_source, const char *top_source,
      const char *ka_update_v) = 0;
  virtual ExecuteNode *do_get_dbscale_purge_connection_pool_node(
      pool_node *pool_info) = 0;
  virtual ExecuteNode *do_get_dbscale_reload_config_node(const char *sql) = 0;
  virtual ExecuteNode *do_get_dbscale_set_priority_info_node(
      string user_name, int tmp_priority_value) = 0;
  virtual ExecuteNode *do_get_dynamic_change_master_node(
      dynamic_change_master_op_node *dynamic_change_master_oper) = 0;
  virtual ExecuteNode *do_get_dynamic_change_multiple_master_active_node(
      dynamic_change_multiple_master_active_op_node
          *dynamic_change_multiple_master_active_oper) = 0;
  virtual ExecuteNode *do_get_dynamic_change_dataserver_ssh_node(
      const char *server_name, const char *username, const char *pwd,
      int port) = 0;
  virtual ExecuteNode *do_get_dynamic_remove_slave_node(
      dynamic_remove_slave_op_node *dynamic_remove_slave_oper) = 0;
  virtual ExecuteNode *do_get_dynamic_remove_schema_node(
      const char *schema_name, bool is_force) = 0;
  virtual ExecuteNode *do_get_dynamic_remove_table_node(const char *table_name,
                                                        bool is_force) = 0;
  virtual ExecuteNode *do_get_dynamic_change_table_scheme_node(
      const char *table_name, const char *scheme_name) = 0;
  virtual ExecuteNode *do_get_dynamic_remove_node(const char *name) = 0;
  virtual ExecuteNode *do_get_dbscale_help_node(const char *name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_audit_user_list_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_slow_sql_top_n_node() = 0;
  virtual ExecuteNode *do_get_dbscale_request_slow_sql_top_n_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_join_node(const char *name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_shard_partition_node(
      const char *name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_rebalance_work_load_node(
      const char *name, list<string> sources, const char *schema_name,
      int is_remove) = 0;
  virtual ExecuteNode *do_get_show_monitor_point_status_node() = 0;
  virtual ExecuteNode *do_get_show_global_monitor_point_status_node() = 0;
  virtual ExecuteNode *do_get_show_histogram_monitor_point_status_node() = 0;
  virtual ExecuteNode *do_get_show_outline_monitor_info_node() = 0;
  virtual ExecuteNode *do_get_dbscale_create_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) = 0;
  virtual ExecuteNode *do_get_dbscale_flush_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) = 0;
  virtual ExecuteNode *do_get_dbscale_show_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) = 0;
  virtual ExecuteNode *do_get_dbscale_delete_outline_hint_node(
      dbscale_operate_outline_hint_node *oper) = 0;
  virtual ExecuteNode *do_get_dbscale_global_consistence_point_node() = 0;
  virtual ExecuteNode *do_get_show_pool_info_node() = 0;
  virtual ExecuteNode *do_get_show_pool_version_node() = 0;
  virtual ExecuteNode *do_get_show_lock_usage_node() = 0;
  virtual ExecuteNode *do_get_show_engine_lock_waiting_node(
      int engine_type) = 0;
  virtual ExecuteNode *do_get_show_dataserver_node() = 0;
  virtual ExecuteNode *do_get_show_backend_threads_node() = 0;
  virtual ExecuteNode *do_get_show_partition_scheme_node() = 0;
  virtual ExecuteNode *do_get_show_schema_node(const char *schema) = 0;
  virtual ExecuteNode *do_get_show_table_node(const char *schema,
                                              const char *table,
                                              bool use_like) = 0;
  virtual ExecuteNode *do_get_migrate_clean_node(const char *migrate_id) = 0;
  virtual ExecuteNode *do_get_show_execution_profile_node() = 0;
  virtual ExecuteNode *do_get_show_data_source_node(list<const char *> &names,
                                                    bool need_show_weight) = 0;
  virtual ExecuteNode *do_get_dynamic_configuration_node() = 0;
  virtual ExecuteNode *do_get_rep_strategy_node() = 0;
  virtual ExecuteNode *do_get_set_server_weight_node() = 0;
  virtual ExecuteNode *do_get_show_option_node() = 0;
  virtual ExecuteNode *do_get_show_dynamic_option_node() = 0;
  virtual ExecuteNode *do_get_change_startup_config_node() = 0;
  virtual ExecuteNode *do_get_show_changed_startup_config_node() = 0;
  virtual ExecuteNode *do_get_rename_table_node(PartitionedTable *old_table,
                                                PartitionedTable *new_table,
                                                const char *old_schema_name,
                                                const char *old_table_name,
                                                const char *new_schema_name,
                                                const char *new_table_name) = 0;
  virtual ExecuteNode *do_get_mul_modify_node(
      map<DataSpace *, const char *> &spaces_map,
      map<DataSpace *, int> *sql_count_map,
      bool need_handle_more_result = false) = 0;
  virtual ExecuteNode *do_get_set_node(DataSpace *dataspace,
                                       const char *sql) = 0;
  virtual ExecuteNode *do_get_union_all_node(vector<ExecuteNode *> &nodes) = 0;
  virtual ExecuteNode *do_get_create_oracle_sequence_node(
      create_oracle_seq_op_node *oper) = 0;
  virtual ExecuteNode *do_get_union_group_node(
      vector<list<ExecuteNode *> > &nodes) = 0;
  virtual void do_send_error_packet() = 0;
  virtual ExecuteNode *do_get_drop_mul_table_node() = 0;
  virtual ExecuteNode *do_get_send_dbscale_one_column_node(
      bool only_one_row) = 0;
  virtual ExecuteNode *do_get_send_dbscale_mul_column_node() = 0;
  virtual ExecuteNode *do_get_send_dbscale_one_column_aggr_node(
      bool get_min) = 0;
  virtual ExecuteNode *do_get_send_dbscale_exists_node() = 0;
  virtual ExecuteNode *do_get_keepmaster_node() = 0;
  virtual ExecuteNode *do_get_dbscale_test_node() = 0;
  virtual ExecuteNode *do_get_async_task_control_node(
      unsigned long long id) = 0;
  virtual ExecuteNode *do_get_load_dataspace_file_node(
      const char *filename) = 0;
  virtual ExecuteNode *do_get_dbscale_show_default_session_var_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_version_node() = 0;
  virtual ExecuteNode *do_get_show_version_node() = 0;
  virtual ExecuteNode *do_get_show_components_version_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_hostname_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_all_fail_transaction_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_detail_fail_transaction_node(
      const char *xid) = 0;
  virtual ExecuteNode *do_get_dbscale_show_partition_table_status_node(
      const char *table_name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_schema_acl_info_node(
      bool is_show_all) = 0;
  virtual ExecuteNode *do_get_dbscale_show_table_acl_info_node(
      bool is_show_all) = 0;
  virtual ExecuteNode *do_get_dbscale_show_path_info() = 0;
  virtual ExecuteNode *do_get_dbscale_show_warnings_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_critical_errors_node() = 0;
  virtual ExecuteNode *do_get_dbscale_clean_fail_transaction_node(
      const char *xid) = 0;
  virtual ExecuteNode *do_get_dbscale_mul_sync_node(
      const char *sync_topic, const char *sync_state, const char *sync_param,
      const char *sync_cond, unsigned long version_id) = 0;

  virtual ExecuteNode *do_get_dbscale_add_default_session_var_node(
      DataSpace *dataspace) = 0;
  virtual ExecuteNode *do_get_dbscale_remove_default_session_var_node() = 0;
  virtual ExecuteNode *do_get_call_sp_return_ok_node() = 0;
  virtual ExecuteNode *do_get_dbscale_purge_monitor_point_node() = 0;
  virtual ExecuteNode *do_get_dbscale_clean_monitor_point_node() = 0;
  virtual ExecuteNode *do_get_cursor_direct_node(DataSpace *dataspace,
                                                 const char *sql) = 0;
  virtual ExecuteNode *do_get_cursor_send_node() = 0;
  virtual ExecuteNode *do_get_federated_table_node() = 0;
  virtual ExecuteNode *do_get_empty_set_node(int field_num) = 0;
#ifndef CLOSE_ZEROMQ
  virtual ExecuteNode *do_get_message_service_node() = 0;
  virtual ExecuteNode *do_get_binlog_task_node() = 0;
  virtual ExecuteNode *do_get_task_node() = 0;
  virtual ExecuteNode *do_get_binlog_task_add_filter_node() = 0;
  virtual ExecuteNode *do_get_drop_task_filter_node() = 0;
  virtual ExecuteNode *do_get_drop_task_node() = 0;
  virtual ExecuteNode *do_get_show_client_task_status_node(
      const char *task_name, const char *server_task_name) = 0;
  virtual ExecuteNode *do_get_show_server_task_status_node(
      const char *task_name) = 0;
#endif
  virtual void do_fullfil_error_info(unsigned int *error_code,
                                     string *sql_state) = 0;

  virtual ExecuteNode *do_get_return_ok_node() = 0;
  virtual ExecuteNode *do_get_slave_dbscale_error_node() = 0;
  virtual ExecuteNode *do_get_forward_master_role_node(const char *sql,
                                                       bool is_slow_query) = 0;
  virtual ExecuteNode *do_get_reset_zoo_info_node() = 0;
  virtual ExecuteNode *do_get_set_info_schema_mirror_tb_status_node(
      info_mirror_tb_status_node *tb_status) = 0;
  virtual ExecuteNode *do_get_dbscale_update_audit_user_node(
      const char *username, bool is_add) = 0;
  virtual ExecuteNode *do_get_connect_by_node(
      ConnectByDesc connect_by_desc) = 0;
  virtual ExecuteNode *do_get_dbscale_show_create_oracle_seq_node(
      const char *seq_schema, const char *seq_name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_seq_status_node(
      const char *seq_schema, const char *seq_name) = 0;
  virtual ExecuteNode *do_get_dbscale_show_fetchnode_buffer_usage_node() = 0;
  virtual ExecuteNode *do_get_dbscale_show_trx_block_info_node(
      bool is_local) = 0;
  virtual ExecuteNode *do_get_dbscale_internal_set_node() = 0;
  virtual ExecuteNode *do_get_restore_recycle_table_precheck_node(
      const char *from_schema, const char *from_table, const char *to_table,
      const char *recycle_type, DataSpace *dspace) = 0;
  virtual ExecuteNode *do_get_dbscale_show_prepare_cache_node() = 0;
  virtual ExecuteNode *do_get_dbscale_flush_prepare_cache_hit_node() = 0;
  virtual ExecuteNode *do_get_empty_node() = 0;
  virtual bool has_duplicate_entry_in_error_packet(Packet *packet) = 0;
  virtual bool check_select_error_not_rollback_in_packet(Packet *packet) = 0;

 public:
  unsigned long get_slow_query_time_local() { return slow_query_time_local; }
  BackendThread *get_one_bthread();
  void start_all_bthread();
  bool get_is_forward_plan() { return is_forward_plan; }
  void set_is_forward_plan() { is_forward_plan = true; }
  string get_forward_sql_target_db() { return forward_sql_target_db; }
  void set_forward_sql_target_db(const string &target_db) {
    forward_sql_target_db = target_db;
  }
  void set_single_server_innodb_rollback_on_timeout(InnodbRollbackOnTimeout v) {
    single_server_innodb_rollback_on_timeout = v;
  }
  InnodbRollbackOnTimeout get_single_server_innodb_rollback_on_timeout() {
    return single_server_innodb_rollback_on_timeout;
  }
  void set_is_parse_transparent(bool b) { this->is_parse_transparent = b; }
  bool get_is_parse_transparent() { return this->is_parse_transparent; }

 public:
  const char *sql;
  const char *tmp_table_create_sql;
  Handler *handler;
  Statement *statement;
  Session *session;
  Driver *driver;
  ExecuteNode *start_node;
  SubQueryResultNode *res_node;
  bool is_cursor;
  bool need_destroy_dataspace_after_exec;
  bool keep_conn_old;
  MigrateTool *migrate_tool;
  bool federated;
  bool dispatch_federated;
  bool can_swap;
  unsigned long slow_query_time_local;
  bool send_cluster_xa_transaction_ddl_error_packet;

 private:
  bool added_traditional_num;
  bool connection_swaped_out;
  bool fetch_node_no_thread;
  bool cancel_execute;
  unsigned int get_bthread_count;
  vector<BackendThread *> bthread_vector;
  bool is_forward_plan;

  // the database of forward master 'use db'
  string forward_sql_target_db;
  InnodbRollbackOnTimeout single_server_innodb_rollback_on_timeout;
  bool is_parse_transparent;

#ifdef DEBUG
 public:
  ACE_Time_Value plan_start_date;
  ACE_Time_Value plan_end_date;
  unsigned long plan_cost_time;
#endif
};

class ExecuteNode {
 public:
  ExecuteNode(ExecutePlan *plan, DataSpace *dataspace = NULL) {
    this->plan = plan;
    parent = NULL;
    this->dataspace = dataspace;
    this->statement = plan->statement;
    this->slow_query_time_local = plan->get_slow_query_time_local();
    this->profile_id = 0;
    need_restrict_row_map_size = false;
    node_id = 0;
    received_packet_num = 0;
    max_mergenode_buffer_rows = 0;
    max_mergenode_buffer_rows_size = max_mergenode_ready_rows_size * 1024;
#ifdef DEBUG
    node_cost_time = 0;
#endif
    use_packet_pool = plan->session->get_enable_session_packet_pool_local();
    packet_pool_packet_bundle_local =
        plan->session->get_session_packet_pool_packet_bundle_local();
    id_in_plan = 0;
    can_swap_ready_rows = true;
    is_first_column_dbscale_row_id = false;
  }
  virtual ~ExecuteNode() {}

  void set_node_id(int id) { node_id = id; }
  int get_node_id() { return node_id; }
  void add_child(ExecuteNode *child);
  void get_children(list<ExecuteNode *> &children);
  virtual void execute() = 0;
  virtual void clean() = 0;
  virtual Packet *get_error_packet() = 0;
  virtual const char *get_executenode_name() = 0;
  virtual bool get_one_row(vector<string> &row_vector) {
    ACE_UNUSED_ARG(row_vector);
    return false;
  }
  virtual bool can_swap() { return false; }
  virtual map<DataSpace *, const char *> *get_spaces_map() { return NULL; }

  virtual void set_cursor_execute_state(CursorExecuteStatus cur_ces) {
    ACE_UNUSED_ARG(cur_ces);
  }
  void add_row_packet(ExecuteNode *node, Packet *row) {
    row_map[node]->push_back(row);
    row_map_size[node]++;
    buffer_row_map_size[node] += row->total_capacity();
    ++received_packet_num;
    if (((received_packet_num & 0x03FF) == 0 ||
         max_mergenode_buffer_rows == 0) &&
        max_mergenode_ready_rows_size) {
      max_mergenode_buffer_rows =
          max_mergenode_buffer_rows_size / row->total_capacity();
      if (!max_mergenode_buffer_rows) max_mergenode_buffer_rows = 1000;
    }
#ifdef DEBUG
    LOG_DEBUG("Add row %@ from %@\n", row, node);
#endif
  }

  void swap_ready_list_with_row_map_list(
      ExecuteNode *node,
      AllocList<Packet *, Session *, StaticAllocator<Packet *> > **child_list,
      int ready_row_size, unsigned long buffer_ready_row_size);

  virtual bool is_finished() = 0;
  virtual void set_handler(Handler *h) = 0;

  void set_statement(Statement *statement) { this->statement = statement; }

  DataSpace *get_dataspace() { return dataspace; }

  virtual string get_explain_sql() {
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
    return "";
  }

  virtual Connection *get_connection_from_node() { return NULL; }
  virtual const char *get_node_sql() { return NULL; }

  virtual int get_rowmap_size(ExecuteNode *node) = 0;
  virtual unsigned long get_buffer_rowmap_size(ExecuteNode *node) = 0;
  bool get_need_restrict_row_map_size() { return need_restrict_row_map_size; }
  virtual bool is_fetch_node() const { return false; }

  ExecutePlan *plan;
  ExecuteNode *parent;
  map<ExecuteNode *,
      AllocList<Packet *, Session *, StaticAllocator<Packet *> > *>
      row_map;
  DataSpace *dataspace;  // data space for get connection
  Statement *statement;
  unsigned int profile_id;
  unsigned long slow_query_time_local;
  void acquire_start_config_lock(string extra_info);
  void release_start_config_lock();
  virtual void set_status(ExecuteStatus s) = 0;
  size_t get_id_in_plan() { return id_in_plan; }
  bool is_first_column_dbscale_row_id;
  void set_is_first_column_dbscale_row_id(bool b) {
    is_first_column_dbscale_row_id = b;
  }

 private:
  virtual void do_add_child(ExecuteNode *child) = 0;
  virtual void do_get_children(list<ExecuteNode *> &children) = 0;
  ExecuteNode *children;
  int node_id;

 protected:
  bool use_packet_pool;
  size_t id_in_plan;
  size_t packet_pool_packet_bundle_local;
  /*
   * map to record each list's size in row_map,
   * reminder to set the init value to 0 before use.
   */
  map<ExecuteNode *, int> row_map_size;

  map<ExecuteNode *, unsigned long> buffer_row_map_size;

  bool need_restrict_row_map_size;
  unsigned long received_packet_num;
  unsigned long max_mergenode_buffer_rows;
  unsigned long max_mergenode_buffer_rows_size;

  bool can_swap_ready_rows;

#ifdef DEBUG
  ACE_Time_Value node_start_date;
  ACE_Time_Value node_end_date;
  unsigned long node_cost_time;
  void node_start_timing() { node_start_date = ACE_OS::gettimeofday(); }
  void node_end_timing() {
    node_end_date = ACE_OS::gettimeofday();
    node_cost_time += (node_end_date - node_start_date).msec();
  }
#endif
};

}  // namespace plan
}  // namespace dbscale
#endif
