#include "mysql_plan.h"

#include <async_task_manager.h>
#include <backend.h>
#include <basic.h>
#include <cluster_status.h>
#include <config.h>
#include <cross_node_join_manager.h>
#include <data_space.h>
#include <exception.h>
#include <frontend.h>
#include <log.h>
#include <log_tool.h>
#include <monitor_point_handler.h>
#include <mul_sync_topic.h>
#include <multiple.h>
#include <mysql_migrate_util.h>
#include <mysql_parser.h>
#include <mysql_xa_transaction.h>
#include <mysqld_error.h>
#include <parser.h>
#include <partition_method.h>
#include <plan.h>
#include <socket_base_manager.h>
#include <sql_parser.h>
#include <xa_transaction.h>

#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/format.hpp>

#include "mysql_connection.h"
#include "mysql_driver.h"
#include "mysql_request.h"
#include "sha1.h"


void init_column_types(list<Packet *> *field_packets,
                       vector<MySQLColumnType> &column_types,
                       unsigned int &column_num, bool *column_inited_flag) {
  if (column_inited_flag == NULL || *column_inited_flag == false) {
    list<Packet *>::iterator it_field;
    for (it_field = field_packets->begin(); it_field != field_packets->end();
         it_field++) {
      MySQLColumnResponse col_resp(*it_field);
      col_resp.unpack();
      column_types.push_back(col_resp.get_column_type());
    }

    column_num = field_packets->size();
    if (column_inited_flag) *column_inited_flag = true;
  }
}

/* class MySQLSendNode */

MySQLSendNode::MySQLSendNode(ExecutePlan *plan) : MySQLInnerNode(plan) {
  send_header_flag = true;
  this->name = "MySQLSendNode";
  this->send_packet_profile_id = -1;
  this->wait_child_profile_id = -1;
  select_uservar_flag = statement->get_select_uservar_flag();
  select_field_num = statement->get_select_field_num();
  if (select_uservar_flag) {
    uservar_vec = *(statement->get_select_uservar_vec());
  }
  this->federated_max_rows = (uint64_t)(
      session->get_session_option("max_federated_cross_join_rows").ulong_val);
  this->cross_join_max_rows = (uint64_t)(
      session->get_session_option("max_cross_join_moved_rows").ulong_val);
#ifndef DBSCALE_TEST_DISABLE
  /*just for test federated_max_rows*/
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "federated_max_rows") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "abort")) {
    this->federated_max_rows = 1;
  }
#endif
  row_num = 0;
  tmp_id = 0;
  node_can_swap = session->is_may_backend_exec_swap_able();
  pkt_list.clear();
  pkt_list_size = 0;
  field_num = 0;
  ready_rows->set_session(plan->session);
}

void MySQLSendNode::send_header() {
  LOG_DEBUG("Start to Send header.\n");
  Packet *header_packet = get_header_packet();
  handler->send_mysql_packet_to_client_by_buffer(header_packet);
  list<Packet *> *field_packets = get_field_packets();

  vector<MySQLColumnType> v_ctype;
  if (session->is_binary_resultset()) {
    if (session->get_prepare_item(session->get_execute_stmt_id())) {
      list<MySQLColumnType> *column_type_list =
          session->get_prepare_item(session->get_execute_stmt_id())
              ->get_column_type_list();
      list<MySQLColumnType>::iterator it_c = column_type_list->begin();
      for (; it_c != column_type_list->end(); it_c++) {
        v_ctype.push_back(*it_c);
      }
    }
  }

  size_t pos = 0;
  for (std::list<Packet *>::iterator it = field_packets->begin();
       it != field_packets->end(); it++) {
    if (select_uservar_flag) {
      MySQLColumnResponse column_response(*it);
      column_response.unpack();
      bool is_number = column_response.is_number();
      field_is_number_vec.push_back(is_number);
    }

    if (session->is_binary_resultset()) {
      if (!v_ctype.empty() && v_ctype.size() > pos) {
        MySQLColumnType prepare_type = v_ctype[pos];
        MySQLColumnResponse col_resp(*it);
        col_resp.unpack();
        MySQLColumnType col_type = col_resp.get_column_type();
        if (col_type != prepare_type) {
          col_resp.set_column_type(prepare_type);
          col_resp.pack(*it);
        }
        LOG_DEBUG(
            "Adjust the column %s from type %d to type %d for binary result "
            "convert.\n",
            col_resp.get_column(), int(col_type), int(prepare_type));
      }
    }

    handler->send_mysql_packet_to_client_by_buffer(*it);
    ++field_num;
    pos++;
  }
  Packet *eof_packet = get_eof_packet();
  handler->deal_autocommit_with_ok_eof_packet(eof_packet);
  handler->send_mysql_packet_to_client_by_buffer(eof_packet);
}

void MySQLExecuteNode::rebuild_packet_first_column_dbscale_row_id(
    Packet *row, uint64_t row_num, unsigned int field_num) {
  MySQLRowResponse row_resp(row);
  vector<string> vec;
  row_resp.fill_columns_to_vector(&vec, field_num);
  char tmp[21];
  sprintf(tmp, "%lu", row_num);
  vec[0] = tmp;
  list<const char *> row_data;
  for (unsigned int i = 0; i < field_num; ++i) {
    row_data.push_back(vec[i].c_str());
  }
  MySQLRowResponse row_resp2(row_data);
  row_resp2.pack(row);
}

void MySQLSendNode::send_row(MySQLExecuteNode *ready_child) {
  send_packet_profile_id = session->get_profile_handler()->start_serial_monitor(
      get_executenode_name(), "SendNode send packet", "",
      send_packet_profile_id);
  Packet *end_row = NULL;
  if (!row_map[ready_child]->empty()) end_row = row_map[ready_child]->back();
  if (statement->get_select_uservar_flag() && end_row != NULL) {
    MySQLRowResponse row_response(end_row);
    set_select_uservar_by_result(end_row, field_is_number_vec, uservar_vec,
                                 select_field_num, session);
  }
  Packet *row = NULL;
  if (!plan->is_federated() && !plan->statement->is_cross_node_join() &&
      !session->is_binary_resultset()) {
    while (!row_map[ready_child]->empty()) {
      row_num++;
      try {
        row = row_map[ready_child]->front();
        if (is_first_column_dbscale_row_id) {
          rebuild_packet_first_column_dbscale_row_id(row, row_num, field_num);
        }
        handler->send_mysql_packet_to_client_by_buffer(row);
      } catch (...) {
        row_map[ready_child]->pop_front();
        delete row;
        throw;
      }
      row_map[ready_child]->pop_front();
      if (use_packet_pool) {
        pkt_list.push_back(row);
        pkt_list_size++;
        // if ((size_t)pkt_list_size >= packet_pool_packet_bundle_local) {
        //  session->put_free_packet_back_to_pool(ready_child->get_id_in_plan(),
        //  pkt_list_size, pkt_list);
        //}
      } else {
        delete row;
      }
    }
  } else {
    while (!row_map[ready_child]->empty()) {
      if (plan->is_federated() && row_num >= federated_max_rows) {
        status = EXECUTE_STATUS_COMPLETE;
        LOG_ERROR(
            "Reach the max rows of [%u] for federated middle result set.\n",
            row_num);
        throw ExecuteNodeError(
            "Reach the max row number of federated middle result set.");
      }
      if (plan->statement->is_cross_node_join() &&
          row_num > cross_join_max_rows) {
        status = EXECUTE_STATUS_COMPLETE;
        LOG_ERROR(
            "Reach the max rows of [%u] for cross node join max moved rows.\n",
            row_num);
        throw ExecuteNodeError(
            "Reach the max row number of cross node join max moved rows.");
      }
      row = row_map[ready_child]->front();
      row_num++;
      if (is_first_column_dbscale_row_id) {
        rebuild_packet_first_column_dbscale_row_id(row, row_num, field_num);
      }
      if (session->is_binary_resultset()) {
        MySQLRowResponse row_resp(row);
        if (session->get_prepare_item(session->get_execute_stmt_id())) {
          list<MySQLColumnType> *column_type_list =
              session->get_prepare_item(session->get_execute_stmt_id())
                  ->get_column_type_list();
          row_resp.convert_to_binary(
              column_type_list,
              handler->get_convert_result_to_binary_stringstream(), &row);
        } else {
          LOG_ERROR("Failed to get prepare item for execute #%d.\n",
                    session->get_execute_stmt_id());
          throw ExecuteCommandFail("Cannot get prepare item.");
        }
      }
      try {
        handler->send_mysql_packet_to_client_by_buffer(row);
      } catch (...) {
        row_map[ready_child]->pop_front();
        delete row;
        throw;
      }
      row_map[ready_child]->pop_front();
      if (use_packet_pool) {
        pkt_list.push_back(row);
        pkt_list_size++;
        // if ((size_t)pkt_list_size >= packet_pool_packet_bundle_local) {
        //  session->put_free_packet_back_to_pool(ready_child->get_id_in_plan(),
        //  pkt_list_size, pkt_list);
        //}
      } else {
        delete row;
      }
    }
  }
  if (use_packet_pool && !pkt_list.empty()) {
    session->put_free_packet_back_to_pool(ready_child->get_id_in_plan(),
                                          pkt_list_size, pkt_list);
  }
  session->get_profile_handler()->end_execute_monitor(send_packet_profile_id);
}

void MySQLSendNode::clear_row_map() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    Packet *row;
    while (!row_map[*it]->empty()) {
      row = row_map[*it]->front();
      row_map[*it]->pop_front();
      delete row;
    }
  }
}

void MySQLSendNode::send_eof() {
  LOG_DEBUG("Start to send eof.\n");
  Packet *end_packet = get_end_packet();
  if (!end_packet) {
    build_eof_packet();
    end_packet = &generated_eof;
  }
  if (plan->session->is_call_store_procedure() ||
      plan->session->get_has_more_result()) {
    rebuild_eof_with_has_more_flag(end_packet, driver);
    LOG_DEBUG(
        "For the call store procedure or multiple stmt, the last eof"
        " should be with flag has_more_result in send node.\n");
  }
  LOG_DEBUG("end_packet = %@\n", end_packet);
  handler->deal_autocommit_with_ok_eof_packet(end_packet);
  handler->send_mysql_packet_to_client_by_buffer(end_packet);
}

void MySQLSendNode::handle_children() {
  bool has_handle_child = false;
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) {
      handle_child(*it);
      has_handle_child = true;
    }
  }
  if (has_handle_child && plan->get_fetch_node_no_thread()) {
    handle_discarded_packets();
    session->reset_fetch_node_packet_alloc();
  }
}

void MySQLSendNode::execute() {
  ExecuteProfileHandler *execute_profile = session->get_profile_handler();
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        tmp_id = execute_profile->start_serial_monitor(get_executenode_name(),
                                                       "SendNode all", "");
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
        if (node_can_swap && plan->plan_can_swap()) {
          handle_swap_connections();
          return;
        }
        status = EXECUTE_STATUS_WAIT;

      case EXECUTE_STATUS_WAIT:
        try {
          wait_child_profile_id =
              session->get_profile_handler()->start_serial_monitor(
                  get_executenode_name(), "SendNode wait", "",
                  wait_child_profile_id);

          wait_children();
          if (one_child_got_error && !all_children_finished) {
            status = EXECUTE_STATUS_FETCH_DATA;
            clear_row_map();
          }
          session->get_profile_handler()->end_execute_monitor(
              wait_child_profile_id);
        } catch (ExecuteNodeError &e) {
          status = EXECUTE_STATUS_COMPLETE;
          throw e;
        }
        break;

      case EXECUTE_STATUS_HANDLE:
        try {
          handle_children();
        } catch (ClientBroken &e) {
          if (plan->get_fetch_node_no_thread()) set_children_error();
          throw;
        }
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        if (send_header_flag) {
          send_header();
          send_header_flag = false;
        }
        send_eof();
        flush_net_buffer();
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        node_end_timing();
#endif
        execute_profile->end_execute_monitor(tmp_id);
#ifdef DEBUG
        LOG_DEBUG("MySQLSendNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        break;
    }
  }
  if (!has_sql_calc_found_rows()) {
    session->set_found_rows(row_num);
  }
  session->set_real_fetched_rows(row_num);
}

void append_column_value_to_replace_sql(string &replace_sql,
                                        const char *column_str,
                                        uint64_t column_len, ResultType type,
                                        CharsetType ctype) {
  if (type == RESULT_TYPE_STRING) {
    string tmp;
    tmp.append(column_str, column_len);
    deal_with_str_column_value(&tmp, ctype);
    replace_sql.append(tmp);
  } else
    replace_sql.append(column_str, column_len);
}

/* class MySQLProjectNode */

MySQLProjectNode::MySQLProjectNode(ExecutePlan *plan, unsigned skip_columns)
    : MySQLInnerNode(plan) {
  this->skip_columns = skip_columns;
  this->name = "MySQLProjectNode";
  field_packets_inited = false;
  header_packet_inited = false;
  columns_num = 0;
  node_can_swap = session->is_may_backend_exec_swap_able();
}

void MySQLProjectNode::init_header_packet() {
  first_child = children.front();
  MySQLResultSetHeaderResponse header(first_child->get_header_packet());
  header.unpack();
  uint64_t columns = header.get_columns();
  columns_num = columns - skip_columns;
  header.set_columns(columns_num);
  header.pack(&header_packet);
  header_packet_inited = true;
}

void MySQLProjectNode::init_field_packet() {
  unsigned int i = 0;
  field_packets.clear();
  first_child = children.front();
  list<Packet *> *field_list = first_child->get_field_packets();
  list<Packet *>::iterator it = field_list->begin();

  columns_num = field_list->size() - skip_columns;
  while (i++ < columns_num) {
    field_packets.push_back(*it);
    it++;
  }
  field_packets_inited = true;
}

void MySQLProjectNode::handle_child(MySQLExecuteNode *child) {
  Packet *row_packet;
  Packet *new_row;
  while (!row_map[child]->empty()) {
    row_packet = row_map[child]->front();
    row_map[child]->pop_front();

    MySQLRowResponse row(row_packet);
    row.set_current(columns_num - 1);
    row.move_next();
    char *start_pos = row.get_row_data();
    char *end_pos = row.get_current_data();
    uint64_t row_length = end_pos - start_pos;
    row.set_row_data(start_pos);
    row.set_row_length(row_length);
    new_row = Backend::instance()->get_new_packet(row_packet_size);
    row.pack(new_row);
    ready_rows->push_back(new_row);
    delete row_packet;
  }
}

void MySQLProjectNode::handle_children() {
  LOG_DEBUG("Node %@ start handle children.\n", this);
  init_columns_num();
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) handle_child(*it);
  }
  LOG_DEBUG("Node %@ handle children complete.\n", this);
}

void MySQLProjectNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
        if (node_can_swap && plan->plan_can_swap()) {
          handle_swap_connections();
          return;
        }
        status = EXECUTE_STATUS_WAIT;

      case EXECUTE_STATUS_WAIT:
        try {
          wait_children();
        } catch (ExecuteNodeError &e) {
          status = EXECUTE_STATUS_COMPLETE;
          throw e;
        }
        break;

      case EXECUTE_STATUS_HANDLE:
#ifdef DEBUG
        node_start_timing();
#endif
        handle_children();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;
        return;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        status = EXECUTE_STATUS_COMPLETE;
      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLProjectNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

/* class MySQLSortNode */

MySQLSortNode::MySQLSortNode(ExecutePlan *plan, list<SortDesc> *sort_desc_para)
    : MySQLInnerNode(plan) {
  need_restrict_row_map_size = true;
  if (sort_desc_para) {
    list<SortDesc>::iterator it = sort_desc_para->begin();
    for (; it != sort_desc_para->end(); it++) sort_desc.push_back(*it);
  }
  this->name = "MySQLSortNode";
  column_inited_flag = false;
  status = EXECUTE_STATUS_START;
  adjust_column_flag = false;
  check_column_type = false;
  sort_node_size = 0;
  sort_ready_nodes = ready_rows;
  node_can_swap = session->is_may_backend_exec_swap_able();
  can_swap_ready_rows = false;

#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "fetchnode") &&
      !strcasecmp(test_info->test_case_operation.c_str(),
                  "max_fetchnode_ready_rows_size")) {
    LOG_DEBUG(
        "Do dbscale test operation 'max_fetch_node_ready_rows_size' for case "
        "'fetchnode'\n");
    need_record_loop_count = true;
  }
#endif
}

/* Here we use Binary Inserting Sort*/
void MySQLSortNode::insert_row(MySQLExecuteNode *child) {
  Packet *packet = row_map[child]->front();

  int low = 0, high = sort_node_size - 1;
  int m = 0;
  while (low <= high) {
    m = (low + high) / 2;
    if (MySQLColumnCompare::compare(merge_sort_vec[m], packet, &sort_desc,
                                    &column_types) < 0) {
      low = m + 1;
    } else {
      high = m - 1;
    }
  }

  for (int j = sort_node_size; j > low; j--) {
    merge_sort_vec[j] = merge_sort_vec[j - 1];
    sort_pos[j] = sort_pos[j - 1];
  }

  merge_sort_vec[low] = packet;
  sort_pos[low] = child;
  sort_node_size++;
}

void MySQLSortNode::pop_row() {
  MySQLExecuteNode *node = sort_pos[sort_node_size - 1];
  Packet *packet = merge_sort_vec[sort_node_size - 1];
  sort_ready_nodes->push_back(packet);
  ready_rows_buffer_size += packet->total_capacity();
  ACE_ASSERT(packet != NULL);
#ifdef DEBUG
  LOG_DEBUG("packet = %@ in pop_row\n", packet);
#endif
  buffer_row_map_size[node] -= row_map[node]->front()->total_capacity();
  row_map[node]->pop_front();
  row_map_size[node]--;
  sort_node_size--;
}

unsigned int MySQLSortNode::get_last_rows() {
  unsigned int rows = 0;
  list<MySQLExecuteNode *>::iterator it_child;
  for (it_child = children.begin(); it_child != children.end(); ++it_child) {
    rows += row_map[*it_child]->size();
  }

  return rows;
}

void MySQLSortNode::add_last_one_list() {
  MySQLExecuteNode *node = NULL;
  list<Packet *, StaticAllocator<Packet *> > *row_list = NULL;
  Packet *packet = NULL;
  node = sort_pos[0];
  row_list = row_map[node];
  while (!row_list->empty()) {
    packet = row_list->front();
    sort_ready_nodes->push_back(packet);
    ready_rows_buffer_size += packet->total_capacity();
    buffer_row_map_size[node] -= packet->total_capacity();
    row_list->pop_front();
    row_map_size[node]--;
  }
}

void MySQLSortNode::sort() {
  sort_node_size = 0;
  list<MySQLExecuteNode *>::iterator it_child;
  for (it_child = children.begin(); it_child != children.end(); ++it_child) {
    if (!row_map[*it_child]->empty()) {
      insert_row(*it_child);
    }
  }
  pop_row();

  map<MySQLExecuteNode *, list<Packet *>::iterator>::iterator it;
  while (true) {
    if (!row_map[sort_pos[sort_node_size]]->empty()) {
      insert_row(sort_pos[sort_node_size]);
      pop_row();
    } else {
      return;
    }
  }
}

void MySQLSortNode::last_sort() {
  if (get_last_rows() == 0) {
    return;
  }

  sort_node_size = 0;
  list<MySQLExecuteNode *>::iterator it_child;
  for (it_child = children.begin(); it_child != children.end(); ++it_child) {
    if (!row_map[*it_child]->empty()) {
      insert_row(*it_child);
    }
  }
  pop_row();

  if (sort_node_size == 0) {
    add_last_one_list();
    return;
  }

  map<MySQLExecuteNode *, list<Packet *>::iterator>::iterator it;
  while (true) {
    if (sort_node_size == 1 && row_map[sort_pos[sort_node_size]]->empty()) {
      add_last_one_list();
      return;
    } else if (!row_map[sort_pos[sort_node_size]]->empty()) {
      insert_row(sort_pos[sort_node_size]);
      pop_row();
    } else {
      pop_row();
    }
  }
}

void MySQLSortNode::init_merge_sort_variables() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    merge_sort_vec.push_back(NULL);
    sort_pos.push_back(NULL);
  }
}

void MySQLSortNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        init_row_map();
        init_merge_sort_variables();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
        if (node_can_swap && plan->plan_can_swap()) {
          handle_swap_connections();
          return;
        }
        status = EXECUTE_STATUS_WAIT;

      case EXECUTE_STATUS_WAIT:
        try {
          wait_children();
        } catch (ExecuteNodeError &e) {
          status = EXECUTE_STATUS_COMPLETE;
          throw e;
        }
        break;

      case EXECUTE_STATUS_HANDLE: {
#ifdef DEBUG
        node_start_timing();
#endif
        init_column_types(get_field_packets(), column_types, column_num,
                          &column_inited_flag);
        adjust_column_index();
        check_column_valid();
        sort();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;

        LOG_DEBUG("sortnode sorted ready buffer size is %Q,count is %u.\n",
                  ready_rows_buffer_size, sort_ready_nodes->size());
        unsigned long size_of_packets =
            sort_ready_nodes->size() * (sizeof(Packet));
        unsigned long size_of_ace_data_blocks =
            sort_ready_nodes->size() * (sizeof(ACE_Data_Block));
        unsigned long size_of_packet_pointers =
            sort_ready_nodes->size() * (sizeof(Packet *));
        unsigned long total_list_use_mem =
            (size_of_packets + size_of_ace_data_blocks +
             size_of_packet_pointers) /
            1024;
        unsigned long left_max_sorted_rows_buffer = 0;
        if (sort_rows_size > total_list_use_mem) {
          left_max_sorted_rows_buffer = sort_rows_size - total_list_use_mem;
        }

        if ((ready_rows_buffer_size / 1024) < left_max_sorted_rows_buffer) {
          LOG_DEBUG(
              "ready_rows_buffer_size is %Q ,left_max_sorted_rows_buffer is "
              "%Q.\n",
              ready_rows_buffer_size, left_max_sorted_rows_buffer);
          break;
        } else
          return;
      }
      case EXECUTE_STATUS_BEFORE_COMPLETE:
#ifdef DEBUG
        node_start_timing();
#endif
        last_sort();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_COMPLETE;
        break;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLSortNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLSortNode::adjust_column_index() {
  if (!adjust_column_flag) {
    list<SortDesc>::iterator it;
    for (it = sort_desc.begin(); it != sort_desc.end(); it++) {
      if (it->column_index < 0) {
        it->column_index += column_num;
      }
    }
    adjust_column_flag = true;
  }
}

void MySQLSortNode::check_column_valid() {
  if (!check_column_type) {
    list<SortDesc>::iterator it;
    for (it = sort_desc.begin(); it != sort_desc.end(); it++) {
      if (!field_is_valid_for_compare(column_types[it->column_index])) {
        LOG_ERROR("Unsupport column type %d for compare.\n",
                  column_types[it->column_index]);
        throw ExecuteNodeError(
            "Unsupport column type for compare,\
                               please check the log for detail.");
      }
    }
    check_column_type = true;
  }
}

/* class MySQLFetchNode */

MySQLFetchNode::MySQLFetchNode(ExecutePlan *plan, DataSpace *dataspace,
                               const char *sql)
    : MySQLExecuteNode(plan, dataspace), conn(NULL), cond(mutex) {
  this->sql.append(sql);
  this->name = "MySQLFetchNode";
  has_get_partition_key_pos = false;
  thread_status = THREAD_STOP;
  bthread = NULL;
  sql_sent = false;
  got_error = false;
  this->eof_packet = NULL;
  this->end_packet = NULL;
  is_get_end_packet = false;
  this->error_packet = NULL;
  ready_rows_size = 0;

  ready_buffer_rows_size = 0;
  if (max_fetchnode_ready_rows_size > 18446744073709551615UL / 1024UL) {
    max_fetchnode_buffer_rows_size = 18446744073709551615UL;
  } else {
    max_fetchnode_buffer_rows_size = max_fetchnode_ready_rows_size * 1024;
  }
  migrate_partition_key_pos_vec = NULL;
  header_received = false;
  pkt_list.clear();
  if (use_packet_pool) {
    id_in_plan = session->get_next_id_in_plan() %
                 ((size_t)session->get_packet_pool_count());
  } else {
    id_in_plan = 0;
  }
  kept_ready_packets = 0;
  local_fetch_signal_batch = fetch_signal_batch;
  force_use_non_trx_conn = false;
  ready_rows->set_session(plan->session);
  plan->session->set_has_fetch_node(true);
  LOG_DEBUG("Plan %@ FetchNode %@ id %Q\n", plan, this, id_in_plan);
}

void MySQLFetchNode::execute() {
  if (plan->session->check_for_transaction() &&
      session->get_session_option("cursor_use_free_conn").int_val) {
    force_use_non_trx_conn = plan->is_cursor;
  }
  if (!sql_sent) {
    session->set_skip_mutex_in_operation(true);
    try {
#ifndef DBSCALE_TEST_DISABLE
      Backend *bk = Backend::instance();
      dbscale_test_info *test_info = bk->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "fetchnode") &&
          !strcasecmp(test_info->test_case_operation.c_str(), "unkown_error")) {
        throw Error("unknown error.");
      }
#endif
      Packet exec_packet;
      MySQLQueryRequest query(sql.c_str());
      query.set_sql_replace_char(
          plan->session->get_query_sql_replace_null_char());
      query.pack(&exec_packet);
      if (plan->get_migrate_tool()) {
        conn = plan->get_migrate_tool()->get_migrate_read_conn();
        handler->send_to_server(conn, &exec_packet);
      } else {
        if (plan->statement->is_cross_node_join()) {
          conn = handler->send_to_server_retry(
              dataspace, &exec_packet, session->get_schema(), false, true,
              false, force_use_non_trx_conn);
        } else {
          conn = handler->send_to_server_retry(
              dataspace, &exec_packet, session->get_schema(),
              session->is_read_only(), true, false, force_use_non_trx_conn);
        }
      }
      LOG_DEBUG("Get connection %@ for node %@.\n", conn, this);
      sql_sent = true;
      session->set_skip_mutex_in_operation(false);
    } catch (Exception &e) {
      mutex.acquire();
      got_error = true;
      mutex.release();
      status = EXECUTE_STATUS_COMPLETE;
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      LOG_ERROR("Send packet error : %s.\n", e.what());
      error_message.append(e.what());
      record_migrate_error_message(plan, error_message);
      return;
    } catch (exception &e) {
      mutex.acquire();
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      mutex.release();
      if (conn) {
        if (!plan->get_migrate_tool())
          handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
        else {
          conn->set_status(CONNECTION_STATUS_TO_DEAD);
          conn = NULL;
        }
      }
      error_message.append("Execute Node failed: ");
      error_message.append(e.what());
      LOG_ERROR("[%s]\n", error_message.c_str());
      record_migrate_error_message(plan, error_message);
      return;
    }
  }
}

bool MySQLFetchNode::can_swap() {
  return session->is_may_backend_exec_swap_able() & (!got_error);
}

void MySQLFetchNode::clean() {
  /* check if the fetch thread finished */
  if (thread_status == THREAD_STARTED) {
    LOG_DEBUG("Fetch node %@ cleaning wait.\n", this);
    this->wait_for_cond();
    LOG_DEBUG("Fetch node %@ cleaning wait finish.\n", this);
  } else if (thread_status == THREAD_CREATED) {
    bthread->re_init_backend_thread();
    bthread->get_pool()->add_back_to_free(bthread);
  } else {
    got_error = true;
  }

  Packet *packet;
  if (eof_packet) {
    delete eof_packet;
    eof_packet = NULL;
  }

  while (!field_packets.empty()) {
    packet = field_packets.front();
    field_packets.pop_front();
    delete packet;
  }
  field_packets.clear();
  packet = NULL;

  while (!ready_rows->empty()) {
    packet = ready_rows->front();
    ready_rows->pop_front();
    delete packet;
  }

  while (!ready_rows_kept.empty()) {
    packet = ready_rows_kept.front();
    ready_rows_kept.pop_front();
    delete packet;
  }
  kept_ready_packets = 0;

  ready_rows->clear();
  packet = NULL;

  if (use_packet_pool && !pkt_list.empty()) {
    int s = (int)pkt_list.size();
    session->put_free_packet_back_to_pool(id_in_plan, s, pkt_list);
    pkt_list.clear();
  }

  if (end_packet) {
    delete end_packet;
    end_packet = NULL;
  }

  if (conn) {
    if (!got_error || error_packet) {
      if (!plan->get_migrate_tool())
        handler->put_back_connection(dataspace, conn, force_use_non_trx_conn);
    } else {
      if (!plan->get_migrate_tool())
        handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
      else
        conn->set_status(CONNECTION_STATUS_TO_DEAD);
    }
    conn = NULL;
  }

  if (error_packet) {
    if (error_packet != &header_packet) {
      delete error_packet;
    }
    error_packet = NULL;
  }
  if (migrate_partition_key_pos_vec) {
    migrate_partition_key_pos_vec->clear();
    delete migrate_partition_key_pos_vec;
    migrate_partition_key_pos_vec = NULL;
  }
}

void MySQLFetchNode::add_kept_list_to_ready_and_signal() {
  try {
    ACE_Guard<ACE_Thread_Mutex> guard(mutex);
    if (!ready_rows_kept.empty()) {
      while (!ready_rows_kept.empty()) {
        ready_rows->push_back(ready_rows_kept.front());

        ready_buffer_rows_size += ready_rows_kept.front()->total_capacity();
        ready_rows_size++;
        ready_rows_kept.pop_front();
      }
      cond.signal();
      kept_ready_packets = 0;
    }
  } catch (ListOutOfMemError &e) {
    throw;
  } catch (...) {
    cond.signal();
    throw;
  }
}

void MySQLFetchNode::get_partition_key_pos() {
  if (plan->get_migrate_tool() && !has_get_partition_key_pos) {
    has_get_partition_key_pos = true;
    vector<const char *> *key_names_vec =
        plan->get_migrate_tool()->get_partition_key_names();
    if (key_names_vec == NULL)
      migrate_partition_key_pos_vec = NULL;
    else {
      migrate_partition_key_pos_vec = new vector<unsigned int>();
      vector<const char *>::iterator it = key_names_vec->begin();
      list<Packet *>::iterator it_field;
      unsigned int index = 0;
      // record the pos of partition key
      for (it_field = field_packets.begin(); it_field != field_packets.end();
           it_field++, index++) {
        MySQLColumnResponse col_resp(*it_field);
        col_resp.unpack();

        for (it = key_names_vec->begin(); it != key_names_vec->end(); it++) {
          if (strcmp(*it, col_resp.get_column()) == 0) {
            migrate_partition_key_pos_vec->push_back(index);
            break;
          }
        }
      }
    }
  }
}
bool MySQLFetchNode::migrate_filter(Packet *row) {
  if (migrate_partition_key_pos_vec == NULL) return true;
  MySQLRowResponse row_res(row);
  vector<string> partition_key_value;
  vector<unsigned int>::iterator it = migrate_partition_key_pos_vec->begin();
  for (; it != migrate_partition_key_pos_vec->end(); it++) {
    unsigned int pos = *it;
    // check whether the value of partition key is NULL
    if (!row_res.field_is_null(pos)) {
      uint64_t str_len = 0;
      const char *column_str = row_res.get_str(pos, &str_len);
      string tmp;
      tmp.append(column_str, str_len);
      partition_key_value.push_back(tmp);
    } else {
      LOG_ERROR("Get partition key value[NULL] when do migrate.\n");
#ifdef DEBUG
      ACE_ASSERT(0);
#endif
      throw Error("Get partition key value[NULL] when do migrate.");
    }
  }
  vector<const char *> values;
  for (unsigned i = 0; i < partition_key_value.size(); i++) {
    values.push_back(partition_key_value.at(i).c_str());
  }
  return plan->get_migrate_tool()->filter(&values);
}
/* start the FetchNode thread using this method. */
void MySQLFetchNode::start_thread() {
  if (thread_status == THREAD_STOP && !got_error &&
      !plan->get_fetch_node_no_thread()) {
    // start the new thread
    bthread = plan->get_one_bthread();
    if (!bthread) {
      got_error = true;
      LOG_ERROR(
          "Fail to get a backend thread from pool, so the fetch node stop.\n");
      throw Error(
          "Fail to get a backend thread from pool, so the fetch node stop");
    }
    bthread->set_task(this);
    thread_status = THREAD_CREATED;
  }
}

int MySQLFetchNode::fetch_row() {
  Packet *packet;
  if (is_get_end_packet || got_error) {
    status = EXECUTE_STATUS_COMPLETE;
    return 0;
  }
  if (!header_received && receive_header_packets()) {
    return 1;
  }
  ExecuteProfileHandler *execute_profile = session->get_profile_handler();
  unsigned int tmp_id = execute_profile->start_parallel_monitor(
      get_executenode_name(), conn->get_server()->get_name(), sql.c_str());

  try {
    ready_rows_size = 0;
    packet = Backend::instance()->get_new_packet(
        row_packet_size, session->get_fetch_node_packet_alloc());
    handler->receive_from_server(conn, packet);

    unsigned int count_index = -1;
    handle_federated_empty_resultset(packet, count_index);
    // Receiving row packets
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        LOG_ERROR("FetchNode %@ get an error packet.\n", this);
        Packet *tmp_packet = NULL;
        // Receiving error packet
        // packet is located on session->get_fetch_node_packet_alloc()
        // session need refresh session->get_fetch_node_packet_alloc()
        // so mv packet memory from session->get_fetch_node_packet_alloc() to
        // new memory
        transfer_packet(&packet, &tmp_packet);
        handle_error_packet(tmp_packet);
        return 1;
      }
      if (count_index != (unsigned int)-1) {
        uint64_t count_num = 1;
        MySQLRowResponse count_row(packet);
        count_num = count_row.get_uint(count_index);
        if (count_num == 0) {
          wakeup_federated_follower();
        }
        count_index = -1;
      }
#ifdef DEBUG
      LOG_DEBUG("FetchNode %@ get a row packet.\n", this);
#endif
      parent->add_row_packet(this, packet);
      ready_rows_size++;
      if (max_fetchnode_ready_rows_size &&
          ready_buffer_rows_size > max_fetchnode_buffer_rows_size) {
#ifndef DBSCALE_TEST_DISABLE
        LOG_INFO("FetchNode Full\n");
#endif
        return 1;
      }
      bool row_map_size_overtop =
          parent->get_need_restrict_row_map_size() &&
          ((parent->get_buffer_rowmap_size(this) >=
            max_fetchnode_buffer_rows_size) ||
           (parent->get_rowmap_size(this) >= rowmap_size_config));
      if (row_map_size_overtop) return 1;
      packet = Backend::instance()->get_new_packet(
          row_packet_size, session->get_fetch_node_packet_alloc());
      handler->receive_from_server(conn, packet);
    }
    if (driver->is_eof_packet(packet)) {
      if (support_show_warning)
        handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                          conn);
    }
    LOG_DEBUG("FetchNode %@ got a eof packet.\n", this);
    is_get_end_packet = true;
    delete packet;
  } catch (...) {
    LOG_ERROR("FetchNode %@ exit cause self error: %s.\n", this, e.what());
    handle_error_packet(NULL);
    error_message.append("Execute Node failed: ");
    error_message.append(e.what());
    if (conn)
      handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
    conn = NULL;
  }
  execute_profile->end_execute_monitor(tmp_id);
  return 1;
}

void MySQLFetchNode::handle_error_throw() {
  if (got_error) throw_error_packet();
}

/** Return ture if send row packet to parent, else return false.
 * Return ture if send row packet to parent, else return false.
 */
bool MySQLFetchNode::notify_parent() {
  Packet *packet;
  int ret = false;
  if (plan->get_fetch_node_no_thread()) {
    session->set_skip_mutex_in_operation(true);
    ret = fetch_row();
    session->set_skip_mutex_in_operation(false);
    if (is_finished() || got_error) {
      if (got_error) {
        ;  // we do nothing here, we would throw exception later.
      } else {
#ifdef DEBUG
        LOG_DEBUG("FetchNode %@ is finished.\n", this);
#endif
      }
      ret = false;
      status = EXECUTE_STATUS_COMPLETE;
    }
    return ret;
  }

  ACE_Guard<ACE_Thread_Mutex> guard(mutex);
  // check fetch node finished or get error
  if (is_finished() || got_error) {
    if (got_error) {
      throw_error_packet();
    } else {
#ifdef DEBUG
      LOG_DEBUG("FetchNode %@ is finished.\n", this);
#endif
      ret = false;
    }
  } else {
    if (ready_rows->empty()) cond.wait();

    if (got_error) {
      throw_error_packet();
      ret = false;
    } else if (ready_rows->empty()) {  // the result set is empty
      ACE_ASSERT(is_finished());
      LOG_DEBUG("FetchNode %@ is finished after wait.\n", this);
      ret = false;
    } else {
      bool is_migrate =
          plan->get_migrate_tool() && migrate_partition_key_pos_vec;
      bool row_map_size_overtop =
          parent->get_need_restrict_row_map_size() &&
          ((parent->get_buffer_rowmap_size(this) >=
            max_fetchnode_buffer_rows_size) ||
           (parent->get_rowmap_size(this) >= rowmap_size_config));
#ifdef DEBUG
      if (parent->get_need_restrict_row_map_size())
        LOG_DEBUG("the parent of fetchnode %s is %d,parent buffer size is %u\n",
                  parent->get_executenode_name(), row_map_size_overtop,
                  parent->get_buffer_rowmap_size(this));
#endif

      // filter packet for partition table
      if (is_migrate) {
        if (!row_map_size_overtop) {
          while (!ready_rows->empty()) {
            packet = ready_rows->front();
            ready_rows->pop_front();
            ready_rows_size--;
            if (migrate_filter(packet))
              parent->add_row_packet(this, packet);
            else
              delete packet;
          }
        }
      } else if (!row_map_size_overtop) {
        if (ready_rows_size > 0) {
          parent->swap_ready_list_with_row_map_list(
              this, &ready_rows, ready_rows_size, ready_buffer_rows_size);
          ready_rows_size = 0;
          ready_buffer_rows_size = 0;
        }
      }
      ret = true;
    }
  }

  return ret;
}

/* If the fetch node get an empty result set, check the if
 * it contains federated table, if is and there's only one last
 * federated thread has not registed to the rule, then the leader
 * federated thread has not be created, wakeup a waiting thread.
 */
void MySQLFetchNode::wakeup_federated_follower() {
  set<string> name_vec;
  plan->statement->get_fe_tmp_table_name(sql, name_vec);
  set<string>::iterator it = name_vec.begin();
  string tmp_table_name;
  Backend *backend = Backend::instance();
  for (; it != name_vec.end(); it++) {
    tmp_table_name.assign(*it);
    LOG_DEBUG("Get federated cross node join table name %s from sql %s.\n",
              tmp_table_name.c_str(), sql.c_str());
    DataSpace *send_space = backend->get_data_space_for_table(
        TMP_TABLE_SCHEMA, tmp_table_name.c_str());
    DataSource *send_source = send_space->get_data_source();
    CrossNodeJoinManager *cross_join_manager =
        backend->get_cross_node_join_manager();
    Session *work_session =
        cross_join_manager->get_work_session(tmp_table_name);
#ifdef DEBUG
    ACE_ASSERT(work_session);
#endif
    if (!send_source) tmp_table_name.append("_par0");
    TransferRuleManager *rule_manager =
        work_session->get_transfer_rule_manager();
    TransferRule *rule =
        rule_manager->get_transfer_rule_by_remote_table_name(tmp_table_name);
    if (rule->is_leader_thread()) {
      rule->wakeup_one_follower();
    }
  }
}

void MySQLFetchNode::handle_federated_empty_resultset(
    Packet *packet, unsigned int &count_index) {
  // TODO:Change the way of checking the sql if is a federated tmp table join
  // sql.
  if (session->get_session_option("cross_node_join_method").int_val ==
          DATA_MOVE_READ &&
      sql.find(TMP_TABLE_NAME) != string::npos) {
    if (driver->is_eof_packet(packet)) {
      wakeup_federated_follower();
    } else {
      // Check the query if contains 'COUNT()'.
      list<AggregateDesc> aggr_list;
      get_aggregate_functions(plan->statement->get_stmt_node()->scanner,
                              aggr_list);
      list<AggregateDesc>::iterator it_agg = aggr_list.begin();
      for (; it_agg != aggr_list.end(); it_agg++) {
        if (it_agg->type == AGGREGATE_TYPE_COUNT) {
          count_index = it_agg->column_index;
          break;
        }
      }
    }
  }
}

void MySQLFetchNode::handle_dead_xa_conn(Connection *conn, DataSpace *space) {
  if (conn->is_start_xa_conn()) {
    unsigned long xid = session->get_xa_id();
    char tmp[256];
    int len =
        sprintf(tmp, "%lu-%u-%d-%d-%u", xid, space->get_virtual_machine_id(),
                conn->get_group_id(), Backend::instance()->get_cluster_id(),
                conn->get_thread_id());
    tmp[len] = '\0';
    string sql = "XA END '";
    sql += tmp;
    sql += "'; XA ROLLBACK '";
    sql += tmp;
    sql += "';";
    Connection *exe_conn = NULL;
    exe_conn = space->get_connection(session);
    if (exe_conn) {
      try {
        exe_conn->execute(sql.c_str(), 2);
        exe_conn->handle_client_modify_return();
        conn->set_start_xa_conn(false);
        exe_conn->get_pool()->add_back_to_free(conn);
        exe_conn = NULL;
      } catch (...) {
        LOG_DEBUG("Fail to rollback %s on conn %@ before add to dead\n",
                  sql.c_str(), conn);
        if (exe_conn) {
          exe_conn->get_pool()->add_back_to_dead(exe_conn);
        }
      }
    }
  }
}

int MySQLFetchNode::svc() {
  Packet *packet = NULL;
  if (is_finished()) return FINISHED;
#ifdef DEBUG
  node_start_timing();
#endif

  if (!header_received) LOG_DEBUG("FetchNode %@ SQL : %s\n", this, sql.c_str());
  if (!header_received && receive_header_packets()) {
    return FINISHED;
  }
  ExecuteProfileHandler *execute_profile = session->get_profile_handler();
  unsigned int tmp_id = execute_profile->start_parallel_monitor(
      get_executenode_name(), conn->get_server()->get_name(), sql.c_str());
  int get_row_packet_num = 0;
#ifndef DBSCALE_TEST_DISABLE
  int loop_count = 1;
#endif
  try {
    if (use_packet_pool && pkt_list.empty()) {
      session->get_free_packet_from_pool(
          id_in_plan, packet_pool_packet_bundle_local, &pkt_list);
    }
    if (!use_packet_pool || pkt_list.empty()) {
      packet = Backend::instance()->get_new_packet(row_packet_size);
    } else {
      packet = pkt_list.front();
      pkt_list.pop_front();
    }
    handler->receive_from_server(conn, packet);
    get_partition_key_pos();  // get partition pos for migrate
    unsigned int count_index = -1;
    handle_federated_empty_resultset(packet, count_index);
    // Receiving row packets
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        MySQLErrorResponse error(packet);
        error.unpack();
        LOG_ERROR("FetchNode node %@ get an error packet %@, %d (%s) %s.\n",
                  this, packet, error.get_error_code(), error.get_sqlstate(),
                  error.get_error_message());

        handle_error_packet(packet);
        packet = NULL;
        return FINISHED;
      }

      if (parent->is_finished()) {
        if (!session->is_keeping_connection()) {
          LOG_INFO(
              "FetchNode %@ is not keeping connection, exit cause parent "
              "finished.\n",
              this);
          if (conn) {
            if (!plan->get_migrate_tool()) {
              handle_dead_xa_conn(conn, dataspace);
              handler->clean_dead_conn(&conn, dataspace,
                                       !force_use_non_trx_conn);
            } else
              conn->set_status(CONNECTION_STATUS_TO_DEAD);
            conn = NULL;
          }
          handle_error_packet(NULL);
          delete packet;
          packet = NULL;
          return FINISHED;
        } else {
          while (!driver->is_eof_packet(packet)) {
            handler->receive_from_server(conn, packet);
            if (driver->is_error_packet(packet)) {
              LOG_ERROR("FetchNode %@ get an error packet.\n", this);
              handle_error_packet(packet);
              packet = NULL;
              return FINISHED;
            }
          }
          break;
        }
      }

      if (count_index != (unsigned int)-1) {
        uint64_t count_num = 1;
        MySQLRowResponse count_row(packet);
        count_num = count_row.get_uint(count_index);
        if (count_num == 0) {
          wakeup_federated_follower();
        }
        count_index = -1;
      }
#ifdef DEBUG
      LOG_DEBUG("FetchNode %@ get a row packet.\n", this);
#endif
#ifndef DBSCALE_TEST_DISABLE
      loop_count++;
      if (loop_count == 5) {
        Backend *bk = Backend::instance();
        dbscale_test_info *test_info = bk->get_dbscale_test_info();
        if (test_info->test_case_name.length() &&
            !strcasecmp(test_info->test_case_name.c_str(), "load_select") &&
            !strcasecmp(test_info->test_case_operation.c_str(),
                        "load_select_fetch_fail")) {
          ACE_OS::sleep(2);
          throw Exception("dbscale test fail.");
        }
      }
#endif

      get_row_packet_num++;
      kept_ready_packets++;
      ready_rows_kept.push_back(packet);

      packet = NULL;
      if (kept_ready_packets >= local_fetch_signal_batch) {
        add_kept_list_to_ready_and_signal();
      }

      if (get_row_packet_num >= MAX_FETCH_NODE_RECEIVE) {
        if (kept_ready_packets > 0) {
          add_kept_list_to_ready_and_signal();
        }
        return WORKING;
      }
      if (max_fetchnode_ready_rows_size) {
        bool need_break = false;
        while (max_fetchnode_ready_rows_size &&
               ready_buffer_rows_size >= max_fetchnode_buffer_rows_size) {
          LOG_DEBUG("Fetchnode full,ready_buffer_rows_size in node is %u \n",
                    ready_buffer_rows_size);
          if (parent->is_finished() ||
              ACE_Reactor::instance()->reactor_event_loop_done()) {
            LOG_INFO(
                "fetchnode sleeping cancelled because parent finished or ACE "
                "reactor event loop done.\n");

            if ((parent->is_finished() && !session->is_keeping_connection()) ||
                ACE_Reactor::instance()->reactor_event_loop_done()) {
              LOG_INFO(
                  "FetchNode %@ is not keeping connection, exit cause parent "
                  "finished.\n",
                  this);
              if (conn) {
                if (!plan->get_migrate_tool()) {
                  handle_dead_xa_conn(conn, dataspace);
                  handler->clean_dead_conn(&conn, dataspace,
                                           !force_use_non_trx_conn);
                } else
                  conn->set_status(CONNECTION_STATUS_TO_DEAD);
                conn = NULL;
              }
              handle_error_packet(NULL);
              delete packet;
              packet = NULL;
              return FINISHED;
            } else {
              while (!driver->is_eof_packet(packet)) {
                handler->receive_from_server(conn, packet);
                if (driver->is_error_packet(packet)) {
                  LOG_ERROR("FetchNode %@ get an error packet.\n", this);
                  handle_error_packet(packet);
                  packet = NULL;
                  return FINISHED;
                }
              }
              need_break = true;
              break;
            }
          }
          if (kept_ready_packets > 0) {
            add_kept_list_to_ready_and_signal();
          }
          return NEED_SLEEP;
        }
        if (need_break) break;
      }
      if (use_packet_pool && pkt_list.empty()) {
        session->get_free_packet_from_pool(
            id_in_plan, packet_pool_packet_bundle_local, &pkt_list);
      }
      if (!use_packet_pool || pkt_list.empty()) {
        packet = Backend::instance()->get_new_packet(row_packet_size);
      } else {
        packet = pkt_list.front();
        pkt_list.pop_front();
      }
      handler->receive_from_server(conn, packet);
    }
    if (kept_ready_packets > 0) {
      add_kept_list_to_ready_and_signal();
    }

    if (driver->is_eof_packet(packet)) {
      if (support_show_warning)
        handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                          conn);
    }
  } catch (ListOutOfMemError &e) {
    LOG_ERROR("FetchNode %@ exit cause self error: %s\n", this, e.what());
    string msg = "FetchNode exit cause self error:";
    msg += e.what();
    record_migrate_error_message(plan, msg);
    error_message.append("There are no enough memory for the sql.");
    handle_error_packet(NULL);
    if (packet) {
      delete packet;
      packet = NULL;
    }
    cond.signal();
    return FINISHED;
  } catch (exception &e) {
    LOG_ERROR("FetchNode %@ exit cause self error: %s.\n", this, e.what());
    string msg = "FetchNode exit cause self error:";
    msg += e.what();
    record_migrate_error_message(plan, msg);
    handle_error_packet(NULL);
    error_message.append("Execute Node failed: ");
    error_message.append(e.what());
    if (packet) {
      delete packet;
      packet = NULL;
    }
    cond.signal();
    return FINISHED;
  }

  if (conn) {
    if (has_sql_calc_found_rows()) {
      found_rows(handler, driver, conn, session);
      Packet *packet_tmp = session->get_error_packet();
      if (packet_tmp) {
        LOG_ERROR("FetchNode %@ get an error packet.\n", this);
        handle_error_packet(packet_tmp);
        cond.signal();
        return FINISHED;
      }
    }
  }

  LOG_DEBUG("FetchNode %@ got a eof packet.\n", this);
  // Receiving end packet
  mutex.acquire();
  end_packet = packet;
  packet = NULL;
  status = EXECUTE_STATUS_COMPLETE;
  cond.signal();
  mutex.release();
  execute_profile->end_execute_monitor(tmp_id);

  if (plan->statement->get_stmt_node()->has_unknown_func && conn)
    session->record_xa_modified_conn(conn);

  // release the conn assp
  if (conn) {
    if (!plan->get_migrate_tool())
      handler->put_back_connection(dataspace, conn, force_use_non_trx_conn);
    conn = NULL;
  }
#ifdef DEBUG
  node_end_timing();
#endif
#ifdef DEBUG
  LOG_DEBUG("MySQLFetchNode %@ cost %d ms\n", this, node_cost_time);
#endif

  return FINISHED;
}

int MySQLFetchNode::receive_header_packets() {
  Packet *tmp_packet;
  header_received = true;
  try {
    conn->handle_merge_exec_begin_or_xa_start(session);
    handler->receive_from_server(conn, &header_packet);
    if (driver->is_ok_packet(&header_packet)) {
      LOG_DEBUG("fetch node get an ok packet, sql[%s].\n", sql.c_str());
      throw HandlerError("Unexpected error, fetch node get an ok packet.");
    }

    if (driver->is_error_packet(&header_packet)) {
      MySQLErrorResponse error(&header_packet);
      error.unpack();
      LOG_ERROR("FetchNode node %@ get an error packet %@, %d (%s) %s.\n", this,
                &header_packet, error.get_error_code(), error.get_sqlstate(),
                error.get_error_message());

      handle_error_packet(&header_packet);
      return 1;
    }

    tmp_packet = Backend::instance()->get_new_packet(row_packet_size);
    // Receiving column packets.
    handler->receive_from_server(conn, tmp_packet);
    while (!driver->is_eof_packet(tmp_packet)) {
      if (driver->is_error_packet(tmp_packet)) {
        handle_error_packet(tmp_packet);
        return 1;
      }
      field_packets.push_back(tmp_packet);
      tmp_packet = Backend::instance()->get_new_packet(row_packet_size);
      handler->receive_from_server(conn, tmp_packet);
    }
  } catch (Exception &e) {
    mutex.acquire();
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    if (conn) {
      if (!plan->get_migrate_tool())
        handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
      else {
        conn->set_status(CONNECTION_STATUS_TO_DEAD);
        conn = NULL;
      }
    }
    LOG_ERROR("Receive header error : %s.\n", e.what());
    error_message.append(e.what());
    cond.signal();
    mutex.release();
    return 1;
  } catch (exception &e) {
    mutex.acquire();
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    if (conn) {
      if (!plan->get_migrate_tool())
        handler->clean_dead_conn(&conn, dataspace, !force_use_non_trx_conn);
      else {
        conn->set_status(CONNECTION_STATUS_TO_DEAD);
        conn = NULL;
      }
    }
    LOG_ERROR("Receive header error : unknown error.\n");
    error_message.append("Execute Node failed: ");
    error_message.append(e.what());
    cond.signal();
    mutex.release();
    return 1;
  }

  LOG_DEBUG("Fetch Node %@ receive header finished.\n", this);
  // Set eof packet
  eof_packet = tmp_packet;
  return 0;
}

/* class MySQLInnerNode */

MySQLInnerNode::MySQLInnerNode(ExecutePlan *plan, DataSpace *dataspace)
    : MySQLExecuteNode(plan, dataspace) {
  this->name = "MySQLInnerNode";
  all_children_finished = false;
#ifndef DBSCALE_TEST_DISABLE
  loop_count = 0;
  need_record_loop_count = false;
#endif
  node_can_swap = false;
  one_child_got_error = false;
  thread_started = false;
  ready_rows_buffer_size = 0;
}

Packet *MySQLInnerNode::get_error_packet() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it)->get_error_packet()) {
      return (*it)->get_error_packet();
    }
  }

  return NULL;
}

void MySQLInnerNode::children_execute() {
#ifdef DEBUG
  LOG_DEBUG("Node %@ children start to execute.\n", this);
#endif
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    (*it)->execute();
    if ((*it)->is_got_error()) {
      break;
    }
    node_can_swap = node_can_swap & (*it)->can_swap();
  }
  if (it != children.end()) {
#ifdef DEBUG
    LOG_DEBUG(
        "Node %@ children execute finished, but some child do not execute "
        "because previous child got error.\n",
        this);
#endif
    handle_error_all_children();
  } else {
#ifdef DEBUG
    LOG_DEBUG("Node %@ children execute finished.\n", this);
#endif
  }
}

bool MySQLInnerNode::notify_parent() {
  Packet *row;
  if (ready_rows->empty()) {
    return false;
  }

  while (!ready_rows->empty()) {
    row = ready_rows->front();
    ready_rows->pop_front();
    parent->add_row_packet(this, row);
  }
  ready_rows_buffer_size = 0;
  return true;
}

void MySQLInnerNode::set_children_error() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    (*it)->set_node_error();
  }
}

void MySQLInnerNode::set_children_thread_status_start() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    (*it)->set_thread_status_started();
  }
}

void MySQLInnerNode::handle_error_all_children() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    try {
      (*it)->handle_error_throw();
    } catch (...) {
      it++;
      for (; it != children.end(); it++) {
        try {
          (*it)->handle_error_throw();
        } catch (...) {
        }
      }
      throw;
    }
  }
}

void MySQLInnerNode::wait_children() {
#ifdef DEBUG
  LOG_DEBUG("Node %@ %s start to wait.\n", this, this->name);
#endif
  if (all_children_finished) {
    handle_error_all_children();
    status = EXECUTE_STATUS_BEFORE_COMPLETE;
  } else {
    list<MySQLExecuteNode *>::iterator it;
    for (it = children.begin(); it != children.end(); ++it) {
      (*it)->start_thread();
    }
    set_children_thread_status_start();
    plan->start_all_bthread();

    unsigned int finished_child = 0;

    for (it = children.begin(); it != children.end(); ++it) {
      if (!(*it)->notify_parent() && (*it)->is_finished()) {
        finished_child++;
      }
      if ((*it)->is_got_error()) {
        one_child_got_error = true;
      }
#ifndef DBSCALE_TEST_DISABLE
      if (need_record_loop_count && loop_count++ == 10) {
        LOG_DEBUG(
            "throw ExecuteNodeError to test for option "
            "'max-fetchnode-ready-rows-size'\n");
        ACE_Time_Value sleep_tv(2, 0);
        ACE_OS::sleep(sleep_tv);
        throw ExecuteNodeError(
            "test for option 'max-fetchnode-ready-rows-size'");
      }
#endif
    }

    if (finished_child == children.size()) {
      handle_error_all_children();
      all_children_finished = true;
      status = EXECUTE_STATUS_BEFORE_COMPLETE;
    } else {
      status = EXECUTE_STATUS_HANDLE;
    }
  }
#ifdef DEBUG
  LOG_DEBUG("Node %@ %s wait finished.\n", this, this->name);
#endif
}

void MySQLInnerNode::init_row_map() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    row_map[*it] =
        new AllocList<Packet *, Session *, StaticAllocator<Packet *> >();
    row_map[*it]->set_session(plan->session);
    row_map_size[*it] = 0;
    buffer_row_map_size[*it] = 0;
  }
}

void MySQLInnerNode::clean() {
  Packet *packet;
  MySQLExecuteNode *free_node;

  status = EXECUTE_STATUS_COMPLETE;

  // clean ready packets
  while (!ready_rows->empty()) {
    packet = ready_rows->front();
    ready_rows->pop_front();
    delete packet;
  }

  while (!children.empty()) {
    free_node = children.front();
    free_node->clean();

    // clean remaining rows
    if (row_map[free_node]) {
      while (!row_map[free_node]->empty()) {
        packet = row_map[free_node]->front();
        row_map[free_node]->pop_front();
        delete packet;
      }

      delete row_map[free_node];
      row_map[free_node] = NULL;
    }

    // delete child node
    children.pop_front();
    delete free_node;
  }

  do_clean();
}

void MySQLInnerNode::handle_swap_connections() {
  if (!plan->get_connection_swaped_out()) {
    plan->set_connection_swaped_out(true);
    list<MySQLExecuteNode *>::iterator it;
    int conn_num = 0;
    for (it = children.begin(); it != children.end(); ++it) {
      Connection *conn = (*it)->get_connection_from_node();
      if (conn) {
        conn->set_session(session);
        struct event *conn_event = conn->get_conn_socket_event();
        evutil_socket_t conn_socket = conn->get_conn_socket();
        SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
        ACE_ASSERT(conn_base);
#endif
        session->add_server_base(conn_socket, conn_base, conn_event);
        LOG_DEBUG(
            "Finish to prepare the conn socket session swap for conn %@ and "
            "session %@.\n",
            conn, session);
        conn_num++;
      }
    }
    session->set_mul_connection_num(conn_num);
  }
}
