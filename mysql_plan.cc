/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */
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

using namespace dbscale;
using namespace dbscale::sql;
using namespace dbscale::plan;
using namespace dbscale::mysql;

#define DBSCALE_BACKEND_THREADS_NUMS 9
#define CHECK_ERROR_INFO "CHECK TABLE GET ERROR"
#define MAX_DEAD_CONN_PER_MIN 1000

#define STR_LEN_INDEX(start, end) (end) - (start) + 1

struct dbscale_threads_str {
  const char *thread_name;
  const char *desc;
  const char *extra;
};

struct dbscale_threads_str threads_message[DBSCALE_BACKEND_THREADS_NUMS] = {
    {"MySQLXA_purge_thread ",
     "Backend thread used to purge XA transaction meta data", ""},
    {"XA_readonly_conn_handler",
     "Backend thread used to handle read-only connections during an XA "
     "transaction",
     ""},
    {"MigrateCleanThread  ",
     "Backend thread used to do purge tasks after a migration job", ""},
    {"MultipleSyncTool",
     "Backend thread used to handle sync messages when in multiple-dbscale "
     "mode",
     ""},
    {"MultipleManager  ",
     "Backend thread used to handle multiple dbscale related situations", ""},
    {"LicenseCheck", "Backend thread used to check license status", ""},
    {"ManagerThread ", "Backend thread used to do the memory free work", ""},
    {"CrossNodeJoinCleanThread",
     "Backend thread used to purge cross node join related meta data", ""},
    {"MessageHandler ", "Backend thread used to handle different messages", ""},
};

struct dbscale_help_str {
  const char *cmd;
  const char *des;
  const char *extra;
};

// plz use Dictionary order when add new command here
struct dbscale_help_str help_message[] = {
    {"DBSCALE [ADD | REMOVE] AUDIT USER user_name", "add/remove audit user",
     ""},
    {"DBSCALE ADD DEFAULT SESSION VARIABLES para_list",
     "add backend mysql variables which need maintain in dbscale", ""},
    {"DBSCALE BACKEND SERVER EXECUTE sql",
     "send SET, GRANT, DROP USER, FLUSH PRIVILEGES command to backend "
     "dataserver to set variables or user",
     ""},
    {"DBSCALE BLOCK TABLE name_or_full ALL block_time", "", ""},
    {"DBSCALE BLOCK TABLE name_or_full PARTITION", "", ""},
    {"DBSCALE CLEAN FAIL TRANSACTION [xid_str]",
     "clean uncommited xa transaction", ""},
    {"DBSCALE CLEAN MIGRATE int_val", "clean migrate table", ""},
    {"DBSCALE DYNAMIC ADD DATASERVER "
     "server_name=name,server_host=host,server_port=port,server_user=user,"
     "server_password=pwd[,server_alias_host=host]",
     "", ""},
    {"DBSCALE DYNAMIC ADD HASH_PARTITION_TABLE DATASPACE \"schema.table\" "
     "PARTITION_KEY=key_name PARTITION_SCHEME=scheme_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD HASH_TYPE PARTITION_SCHEME scheme_name "
     "PARTITION=par_name [PARTITION ...] [hash_type_str] [IS_SHARD] "
     "[SHARD_NUMS shard_nums]",
     "", ""},
    {"DBSCALE DYNAMIC ADD LOAD_BALANCE DATASOURCE source_name, "
     "SERVER=server_name-min-max-low-high [SERVER...] [GROUP_ID = INTNUM]",
     "dynamic add load_balance datasource", ""},
    {"DBSCALE DYNAMIC ADD MODE_TYPE PARTITION_SCHEME scheme_name "
     "PARTITION=par_name [PARTITION ...] [NOT_SIMPLE] [IS_SHARD] [SHARD_NUMS "
     "shard_nums]",
     "", ""},
    {"DBSCALE DYNAMIC ADD MOD_PARTITION_TABLE DATASPACE \"schema.table\" "
     "PARTITION_KEY=key_name PARTITION_SCHEME=scheme_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD NORMAL_TABLE DATASPACE \"schema.table\" "
     "DATASOURCE=source_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD PARTITION_SCHEME scheme_name PARTITION=ds_name "
     "[PARTITION=ds_name]",
     "", "PARTITION list must bigger than 1"},
    {"DBSCALE DYNAMIC ADD PARTITION_TABLE DATASPACE schema.table "
     "[PARTERN='pattern'] PARTITION_KEY=key_name PARTITION_SCHEME=scheme_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD READ_ONLY DATASOURCE source_name, "
     "server_name-min-max-low-high [GROUP_ID = INTNUM]",
     "dynamic add read_only datasource", ""},
    {"DBSCALE DYNAMIC ADD REPLICATION DATASOURCE source_name, "
     "MASTER=server_name-min-max-low-high SLAVE=server_name-min-max-low-high "
     "[SLAVE...] [GROUP_ID = INTNUM]",
     "dynamic add replication datasource", ""},
    {"DBSCALE DYNAMIC ADD RWSPLIT DATASOURCE source_name, "
     "MASTER=server_name-min-max-low-high SLAVE=server_name-min-max-low-high "
     "[SLAVE ...] [GROUP_ID = INTNUM]",
     "dynamic add rwsplit datasource", ""},
    {"DBSCALE DYNAMIC ADD SCHEMA DATASPACE schema_name DATASOURCE=source_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD SERVER DATASOURCE source_name, "
     "server_name-min-max-low-high [GROUP_ID = INTNUM]",
     "dynamic add server datasource", ""},
    {"DBSCALE DYNAMIC ADD SHARE_DISK DATASOURCE source_name, "
     "ACTIVE=server_name-min-max-low-high "
     "COLD_STANDBY=server_name-min-max-low-high [GROUP_ID = INTNUM]",
     "dynamic add share_disk datasource", ""},
    {"DBSCALE DYNAMIC ADD SLAVE server-min-max-low-high TO "
     "replication_source_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD SLAVE slave_source_name TO replication_source_name",
     "", ""},
    {"DBSCALE DYNAMIC ADD/REMOVE WHITE TO user1@'192.168.0.1' COMMENT 'comment "
     "info'",
     "dynamic update white ip list", ""},
    {"DBSCALE DYNAMIC REMOVE DATASERVER server_name", "remove a dataserver",
     ""},
    {"DBSCALE DYNAMIC REMOVE SCHEMA `schema_name`", "remove a schema dataspace",
     ""},
    {"DBSCALE DYNAMIC REMOVE TABLE `schema_name`.`table_name`",
     "remove a table dataspace", ""},
    {"DBSCALE DYNAMIC REMOVE PARTITION_SCHEME scheme_name", "", ""},
    {"DBSCALE DYNAMIC REMOVE SLAVE slave_name FROM master_name",
     "remove a slave from from replication datasource", ""},
    {"DBSCALE DYNAMIC CHANGE MASTER datasource_name TO new_master",
     "change replcation datasource topu online", ""},
    {"DBSCALE DYNAMIC CHANGE MULTIPLEMASTER mul_master_source ACTIVE TO "
     "mul_master_source_lb_mul_master_server",
     "", ""},
    {"DBSCALE DYNAMIC SET LOAD_BALANCE_STRATEGY = \"val\" FOR data_source_name",
     "", ""},
    {"DBSCALE DYNAMIC SET DATASOURCE datasource_name SERVER server_name WEIGHT "
     "TO intnum",
     "", ""},
    {"DBSCALE DYNAMIC SET name_or_full AUTO_INCREMENT OFFSET TO INTNUM", "",
     ""},
    {"DBSCALE DYNAMIC SET MASTER PRIORITY FOR server_name TO INTNUM", "", ""},
    {"DBSCALE ESTIMATE sql", "", ""},
    {"DBSCALE FLASHBACK DATASERVER name_or_string FORCE ONLINE",
     "set data-server online status to normal when flashback error", ""},
    {"DBSCALE FLUSH [ALL] CONFIG TO FILE [file_name]",
     "flush online dbscale config to file", ""},
    {"DBSCALE FLUSH ACL", "flush current user's ACL info, make ACL up-to-date",
     ""},
    {"DBSCALE FLUSH [FORCE] CONNECTION POOL [pool_name]",
     "flush connection pool version", ""},
    {"DBSCALE GET GLOBAL CONSISTENCE POINT", "", ""},
    {"DBSCALE MIGRATE NO_PARTITION norm_table_name TO source_name,PARTITION "
     "part_table_name vid TO source_name [vid ...],"
     "SHARD part_table_name shard_id TO source_name, SPLIT part_table_name "
     "FROM source_name TO target_source_name [target_source_name ...];",
     "", ""},
    {"DBSCALE HELP [cmd_name]", "", ""},
    {"DBSCALE REMOVE DEFAULT SESSION VARIABLES para_list",
     "remove backend mysql variables no need to maintain in dbscale", ""},
    {"DBSCALE RESET AUTH_INFO opt_user_name", "", ""},
    {"DBSCALE RESET BACKEND THREAD POOL INTNUM", "", ""},
    {"DBSCALE RESET CONNECTION POOL [pool_name] INTNUM", "", ""},
    {"DBSCALE RESET LOGIN HANDLER THREAD POOL INTNUM", "", ""},
    {"DBSCALE RESET pool_type pool_name INTNUM", "",
     "pool_type include [CONNECTION POOL|LOGIN HANDLER THREAD POOL|BACKEND "
     "THREAD POOL|HANDLER THREAD POOL]"},
    {"DBSCALE RESET TMP_TABLE", "clean cross join tmp table", ""},
    {"DBSCALE RESET ZOOKEEPER CONFIG INFO", "clean zookeeper config",
     "only for root user"},
    {"DBSCALE SET PRIORITY number TO USER BY USER_NAME user_name",
     "set user priority", "number can be 1,2,3, the priority 1 > 2 > 3"},
    {"DBSCALE SET SESSION | INSTANCE | GLOBAL option_name = val",
     "set dbscale variables online",
     "check dbscale document for which option can set online"},
    {"DBSCALE SET ACL ON SCHEMA db_name TO user_name [NO_TOUCH | READ_ONLY | "
     "WRITABLE]",
     "set ACL on db_name to user_name", ""},
    {"DBSCALE SET DEFAULT ACL TO user_name [NO_TOUCH | READ_ONLY | WRITABLE]",
     "set default ACL to a user", ""},
    {"DBSCALE RESET ACL ON SCHEMA db_name TO user_name",
     "clean ACL strategy on db_name to user_name", ""},
    {"DBSCALE RESET ACL ON ALL SCHEMA TO user_name",
     "clean all ACL strategy on all schema to user_name", ""},
    {"DBSCALE SET ACL ON TABLE db_name.table_name TO user_name [NO_TOUCH | "
     "READ_ONLY | WRITABLE]",
     "set ACL on table (db_name.table_name) to user_name", ""},
    {"DBSCALE RESET ACL ON TABLE db_name.table_name TO user_name",
     "clean ACL strategy on table (db_name.table_name) to user_name", ""},
    {"DBSCALE RESET ACL ON ALL TABLE IN SCHEMA db_name TO user_name",
     "clean all ACL strategy on all table in schema db_name to user_name", ""},
    {"DBSCALE SET SCHEMA PUSHDOWN_PROCEDURE = INT",
     "set dbscale schema pushdown procedure behavior",
     "check dbscale document for which value for INT"},
    {"DBSCALE SHOW AUDIT USER LIST", "show audit user list", ""},
    {"DBSCALE SHOW AUTO_INCREMENT OFFSET FOR name_or_full", "", ""},
    {"DBSCALE SHOW BACKEND_THREADS", "", ""},
    {"DBSCALE SHOW CATCHUP BINLOG INFO FOR DATASOURCE source_name", "", ""},
    {"DBSCALE SHOW CONNECTION POOL [server_name]", "", ""},
    {"DBSCALE SHOW DEFAULT SESSION VARIABLES para_list",
     "show the backend mysql variables which maintain in dbscale", ""},
    {"DBSCALE SHOW DATASERVERS", "", ""},
    {"DBSCALE SHOW DATASOURCE ds_name", "", ""},
    {"DBSCALE SHOW DATASOURCE TYPE = type_name", "",
     "type name amongs [server,share_disk,load-balance,rwsplit,replication]"},
    {"DBSCALE SHOW SCHEMA DATASPACE [schema_name]", "", ""},
    {"DBSCALE SHOW EXECUTEION PROFILE", "",
     "need to \'DBSCALE SET execute_profile=1\' before"},
    {"DBSCALE SHOW FAIL TRANSACTION [xid]", "", ""},
    {"DBSCALE SHOW INNODB_LOCK_WAITING STATUS", "", ""},
    {"DBSCALE SHOW JOIN STATUS {name}", "", ""},
    {"DBSCALE SHOW MIGRATE CLEAN TABLES",
     "show the tables need clean after migreate", ""},
    {"DBSCALE SHOW LOCK USAGE", "", ""},
    {"DBSCALE SHOW USER SQL COUNT [user_id]",
     "show the user dml sql execution count.", ""},
    {"DBSCALE SHOW OPTION", "show options used by dbscale",
     "DBSCALE SHOW [SESSION | INSTANCE | GLOBAL] OPTION [LIKE "
     "'\%option_name\%']"},
    {"DBSCALE SHOW PARTITION TABLE STATUS FOR table_name", "", ""},
    {"DBSCALE SHOW PARTITION TABLE table_name SHARD MAP", "", ""},
    {"DBSCALE SHOW PARTITION TABLE table_name VIRTUAL MAP", "", ""},
    {"DBSCALE SHOW PARTITION_SCHEME", "", ""},
    {"DBSCALE SHOW PARTITIONS FROM tbl_name", "", ""},
    {"DBSCALE SHOW PATH INFO", "show dbscale install path and log path", ""},
    {"DBSCALE SHOW SHARD PARTITION TABLES [part_name]", "show all shard tables",
     ""},
    {"DBSCALE SHOW STATUS", "show dbscale cluster status", ""},
    {"DBSCALE SHOW TABLE DATASPACE [ [LIKE table_name | table_name] IN SCHEMA "
     "schema_name ]",
     "show table dataspace config info", ""},
    {"DBSCALE SHOW TABLE LOCATION table_name",
     "show table's location, include host, port, data_source and so on", ""},
    {"DBSCALE SHOW THREAD POOL INFO", "", ""},
    {"DBSCALE SHOW TRANSACTION SQLS FOR user_id", "",
     "Option \'record-transaction-sqls\' need turn on"},
    {"DBSCALE SHOW USER MEMORY STATUS", "show all user's memory",
     "only useful when backend server is mariadb"},
    {"DBSCALE SHOW USER STATUS [user_id | FOR user_name | RUNNING]", "", ""},
    {"DBSCALE SHOW VERSIONS", "show dbscale version information", ""},
    {"DBSCALE SHOW VERSION CONNECTION POOL [pool_name]", "", ""},
    {"DBSCALE SHOW WARNINGS", "", ""},
    {"DBSCALE SKIP WAIT CATCHUP BINLOG THREAD FOR DATASOURCE source_name", "",
     ""},
    {"DBSCALE STOP BLOCK TABLE name_or_full", "", ""},
    {"DBSCALE [SHOW |REQUEST] SESSION ID WITH DATASERVER = server_name "
     "CONNECTION = connection_id;",
     "", ""},
    {"DBSCALE EXPLAIN original_stmt", "", ""},
    {"DBSCALE SHOW PROCESSLIST [cluster_id] USER user_id [LOCAL]", "", ""},
};
namespace dbscale {
namespace sql {
void get_aggregate_functions(record_scan *rs, list<AggregateDesc> &aggr_list);
extern void adjust_shard_schema(const char *sql_schema, const char *cur_schema,
                                string &new_schema,
                                unsigned int virtual_machine_id,
                                unsigned int partition_id);
}  // namespace sql
}  // namespace dbscale
namespace dbscale {
Schema *get_or_create_schema(string schema_name);
extern void sync_migrate_change_topo(string topic_name, string param);
Schema *get_migrate_schema(string schema_name) {
  Backend *backend = Backend::instance();
  Schema *schema = backend->find_schema(schema_name.c_str());
  if (schema == NULL) {
    LOG_ERROR("get exception dring get schema[%s], plz dynamic add it\n",
              schema_name.c_str());
    throw Error("get exception dring get schema");
  }
  return schema;
}

bool handle_tokudb_lock_timeout(Connection *conn, Packet *packet) {
  try {
    if (support_tokudb && conn && packet) {
      MySQLErrorResponse error(packet);
      error.unpack();
      if (error.get_error_code() == 1205) {
        string tokudb_last_lock_timeout;
        conn->query_for_one_value("select @@tokudb_last_lock_timeout",
                                  tokudb_last_lock_timeout, 0);
        LOG_INFO("select @@tokudb_last_lock_timeout result is :%s\n",
                 tokudb_last_lock_timeout.c_str());
        // result like below:
        //@@tokudb_last_lock_timeout: {"mysql_thread_id":6052,
        //"dbname":"./wen/t1-main", "requesting_txnid":1558,
        //"blocking_txnid":1554, "key":"0001000000"}
        int pos_start, pos_end;
        pos_start = tokudb_last_lock_timeout.find("\"blocking_txnid\":");
        if (pos_start >= 0) {
          pos_end = tokudb_last_lock_timeout.find(",", pos_start);
          string blocking_txnid = tokudb_last_lock_timeout.substr(
              pos_start + 17, pos_end - pos_start - 17);
          string sql =
              "select locks_mysql_thread_id from "
              "information_schema.TokuDB_locks where locks_trx_id=";
          sql += blocking_txnid;
          LOG_INFO("generate get lock sql:%s\n", sql.c_str());

          vector<string> vec;
          TimeValue tv(backend_sql_net_timeout);
          conn->query_for_one_column(sql.c_str(), 0, &vec, &tv, true);
          vector<string>::iterator it;
          LOG_INFO("plz execute below command:\n");
          for (it = vec.begin(); it != vec.end(); it++) {
            LOG_INFO(
                "DBSCALE SHOW SESSION ID WITH DATASERVER = %s CONNECTION = "
                "%s;\n",
                conn->get_server()->get_name(), it->c_str());
          }
          LOG_INFO(
              "plz kill the session to release lock if necessary by "
              "command:\"kill cluster_id session_id\"\n");
        }
      }
    }
  } catch (exception &e) {
    LOG_ERROR("handle_tokudb_lock_timeout get exception [%s]\n", e.what());
    return true;
  }
  return false;
}
namespace mysql {
#define MAX_PACKET_SIZE ((2 << 23) - 1)

// define the max length of string number, like max_legth=strlen("1234567890");
#define MAX_LENGTH_NUM (10)

static string find_and_replace_quota(const char *sql) {
  string tmp_sql(sql);
  size_t pos = tmp_sql.find("\"");
  while (pos != string::npos) {
    tmp_sql.replace(pos, 1, "\\\"");
    pos = tmp_sql.find("\"", pos + 2);
  }
  return tmp_sql;
}

void transfer_packet(Packet **old_packet, Packet **new_packet) {
  char *p = (*old_packet)->base();
  char *data = (*old_packet)->rd_ptr();
  size_t data_len = Packet::unpack3uint(&p) + PACKET_HEADER_SIZE;
  size_t packet_len =
      data_len > (size_t)row_packet_size ? data_len : row_packet_size;
  *new_packet = Backend::instance()->get_new_packet(packet_len);
  (*new_packet)->rewind();
  (*new_packet)->packdata(data, data_len);
  *(*new_packet)->wr_ptr() = '\0';
  delete *old_packet;
}

void pack_header(Packet *packet, size_t load_length) {
  packet->rewind();
  packet->pack3int(load_length);
  packet->packchar(0);
  packet->wr_ptr(load_length);
  *packet->wr_ptr() = '\0';
}

void check_lines_term_null(into_outfile_item *&into_outfile) {
  // if the LINES TERMINATED BY spectaror is NULL(""),
  // it will use the FIELDS TERMINATED BY replace it.
  if (into_outfile->lines_term_len == 0) {
    unsigned int length = into_outfile->fields_term_len;
    into_outfile->lines_term_len = length;
    for (unsigned int i = 0; i != length; i++) {
      into_outfile->lines_term[i] = into_outfile->fields_term[i];
    }
    into_outfile->lines_term[length] = '\0';
  }
}

ResultType get_result_type_from_column_type(MySQLColumnType col_type) {
  switch (col_type) {
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_NEWDECIMAL:
      return RESULT_TYPE_NUM;
      break;

    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      return RESULT_TYPE_STRING;
      break;

    default:
      throw UnSupportPartitionSQL("Column type in SelectNode is not support.");
  }
}

void rebuild_eof_with_has_more_flag(Packet *packet, MySQLDriver *driver) {
  if (driver->is_eof_packet(packet)) {
    MySQLEOFResponse eof(packet);
    eof.unpack();
    if (eof.has_more_result()) return;
    eof.set_has_more_result();
    // Packet *new_packet = new Packet(row_packet_size);
    eof.pack(packet);
    // delete tmp; *packet = new_packet;
  }
}

void rebuild_ok_with_has_more_flag(Packet *packet, MySQLDriver *driver) {
  if (driver->is_ok_packet(packet)) {
    MySQLOKResponse ok(packet);
    ok.unpack();
    if (ok.has_more_result()) return;
    ok.set_has_more_result();
    ok.pack(packet);
  }
}

void reset_packet_size(Packet *p, size_t len) {
  p->rewind();
  p->base();
  p->size(len);
}

void deal_with_str_column_value(string *str, CharsetType ctype) {
  size_t len = str->length();
  if (!len) return;

  int null_num = 0;
  for (size_t i = 0; i != len - 1; ++i) {
    if ((*str)[i + null_num] == '\0') {
      str->replace(i + null_num, 1, "\\0");
      ++null_num;
    }
  }

  if (ctype == CHARSET_TYPE_OTHER) {
    ctype = charset_type_maps[default_charset_type];
  }
  if (ctype == CHARSET_TYPE_GBK || ctype == CHARSET_TYPE_GB18030) {
    size_t i = 0;
    while (i < str->length()) {
      char c = (*str)[i];
      if ((unsigned char)c > 0x80 && (unsigned char)c < 0xFF) {
        ++i;
      } else {
        if (c == '\\' || c == '\'') {
          str->insert(i, 1, '\\');
          ++i;
        }
      }
      ++i;
    }
  } else {
    find_and_insert_str(str, "\\", "\\");
    find_and_insert_str(str, "'", "\\");
  }
}

void write_result_to_uservar(Session *session, string uservar_name,
                             const char *str, unsigned int length = 0,
                             bool is_number = false) {
  if (!str) {
    session->remove_user_var_by_name(uservar_name);
    LOG_DEBUG("Remove user variable [%s] cause it is NULL.\n",
              uservar_name.c_str());
    return;
  }

  string value(str, length);
  session->add_user_var_value(uservar_name, value, !is_number);
}

void print_warning_infos(Connection *conn) {
  if (!conn) return;
  vector<vector<string> > vec;
  try {
    conn->query_for_all_column("SHOW WARNINGS", &vec);
  } catch (...) {
    LOG_ERROR("Got exception when conn [%@] execute show warnings.\n", conn);
  }
  if (!vec.empty()) {
    string warning_info("SHOW WARNINGS;\n|Level|Code|Message|\n");
    vector<vector<string> >::iterator it = vec.begin();
    for (; it != vec.end(); it++) {
      vector<string>::iterator it1 = it->begin();
      for (; it1 != it->end(); it1++) {
        warning_info.append("|");
        warning_info.append(*it1);
      }
      warning_info.append("|\n");
    }
    LOG_INFO("%s", warning_info.c_str());
  }
}

void deal_with_var(Connection *conn, ExecutePlan *plan, MySQLDriver *driver) {
  Handler *handler = plan->handler;
  Session *session = plan->session;
  if (conn) {
    conn->reset();
    string var_sql = plan->statement->get_select_var_sql();
    if (var_sql.size() == 0) {
      return;
    }

    Packet *packet = Backend::instance()->get_new_packet(row_packet_size);
    Packet exec_packet;
    vector<string> *uservar_list = plan->statement->get_uservar_list();
    vector<string> *sessionvar_list = plan->statement->get_sessionvar_list();
    MySQLQueryRequest query(var_sql.c_str());
    query.set_sql_replace_char(
        plan->session->get_query_sql_replace_null_char());
    query.pack(&exec_packet);

    map<string, string> *conn_session_var_map = conn->get_session_var_map();
    map<string, string> *session_session_var_map =
        session->get_session_var_map();
    try {
      LOG_DEBUG("deal with var send sql [%s]\n", var_sql.c_str());
      handler->send_to_server(conn, &exec_packet);
      handler->receive_from_server(conn, packet);

      if (driver->is_error_packet(packet)) {
        LOG_ERROR("Internal select user variable sql got an error packet.\n");
        throw ErrorPacketException();
      }

      vector<bool> is_str_vec;
      vector<uint32_t> column_len;
      handler->receive_from_server(conn, packet);
      while (!driver->is_eof_packet(packet)) {
        if (driver->is_error_packet(packet)) {
          LOG_ERROR("Internal select user variable sql got an error packet.\n");
          throw ErrorPacketException();
        }
        MySQLColumnResponse column_response(packet);
        column_response.unpack();
        bool is_string = column_response.is_number() ? false : true;
        is_str_vec.push_back(is_string);
        unsigned int length = column_response.get_column_length();
        column_len.push_back(length);
        handler->receive_from_server(conn, packet);
      }

      handler->receive_from_server(conn, packet);
      while (!driver->is_eof_packet(packet)) {
        if (driver->is_error_packet(packet)) {
          LOG_ERROR("Internal select user variable sql got an error packet.\n");
          throw ErrorPacketException();
        }

        MySQLRowResponse row_response(packet);
        unsigned int user_num = uservar_list->size();
        unsigned int session_num = sessionvar_list->size();
        unsigned int fields_num = user_num + session_num;
        for (unsigned int i = 0; i != fields_num; i++) {
          if (i < user_num) {
            if (row_response.field_is_null(i)) {
              write_result_to_uservar(session, uservar_list->at(i), NULL);
            } else {
              uint64_t length;
              const char *str = row_response.get_str(i, &length);
              write_result_to_uservar(session, uservar_list->at(i), str, length,
                                      !is_str_vec[i]);
            }
          } else {
            string var_name = sessionvar_list->at(i - user_num);
            string var_value;
            if (row_response.field_is_null(i)) {
              var_value = "NULL";
              is_str_vec[i] = false;
            } else {
              uint64_t length;
              const char *str = row_response.get_str(i, &length);
              var_value = string(str, length);
            }
            if (!strcasecmp(var_name.c_str(), "CHARACTER_SET_CLIENT")) {
              int number = Backend::instance()->get_charset_number(var_value);
              if (number > 0)
                ((MySQLSession *)session)->set_client_charset(number);
            }
            if (is_str_vec[i]) {
              var_value = "'" + var_value + "'";
            }
            (*conn_session_var_map)[var_name] = var_value;
            (*session_session_var_map)[var_name] = var_value;
          }
        }
        session->set_session_var_map_md5(
            calculate_md5(session_session_var_map));
        conn->set_session_var_map_md5(calculate_md5(conn_session_var_map));
        handler->receive_from_server(conn, packet);
      }
      delete packet;
    } catch (Exception &e) {
      LOG_ERROR("Send select user variable to server got error!\n");
      delete packet;
      throw;
    }
  }
}

inline void handle_warnings_OK_and_eof_packet_inernal(
    MySQLDriver *driver, Packet *packet, Handler *handler, DataSpace *space,
    Connection *conn, bool skip_keep_conn) {
  if (!support_show_warning || skip_keep_conn ||
      (conn && conn->is_changed_user_conn()))  // do not handle keep conn for
                                               // changed_user conn
    return;
  uint16_t warnings = 0;
  if (driver->is_ok_packet(packet)) {
    MySQLOKResponse ok(packet);
    ok.unpack();
    warnings = ok.get_warnings();
  } else if (driver->is_eof_packet(packet)) {
    MySQLEOFResponse eof_response(packet);
    eof_response.unpack();
    warnings = eof_response.get_warnings();
  }
  if (!warnings) return;
  vector<Packet *> *warning_packet_list = NULL;
  uint64_t warning_packet_list_size = 0;
  bool has_add_warning_packet = false;
  Session *session = handler->get_session();
  LOG_DEBUG("session load warning count: [%Q]\n",
            session->get_load_warning_count());
  bool has_saved_too_many_warnings =
      session->get_load_warning_count() > MAX_LOAD_WARNING_PACKET_LIST_SIZE;
  try {
    if (!has_saved_too_many_warnings || support_log_warning_info)
      store_warning_packet(conn, handler, driver, &warning_packet_list,
                           has_add_warning_packet, warning_packet_list_size);
  } catch (std::exception &e) {
    LOG_ERROR("Getting warnings error: [%s]\n", e.what());
    return;
  }
  if (!has_add_warning_packet) return;
  if (support_log_warning_info) {
    MySQLResultSetHeaderResponse result_set(warning_packet_list->front());
    result_set.unpack();
    uint64_t columns_num = result_set.get_columns();
    vector<string> columns_name = vector<string>(0);
    vector<Packet *>::iterator it = warning_packet_list->begin() + 1;
    while (!driver->is_eof_packet(*it)) {
      // unpack FieldPacket
      MySQLFieldListColumnResponse field(*it);
      field.unpack();
      columns_name.push_back(string(field.get_column()));
      ++it;
    }
    ++it;
    string row_data = "query sql: ";
    row_data += handler->get_session()->get_query_sql();
    while (!driver->is_eof_packet(*it)) {
      // unpack RowPacket
      MySQLRowResponse row(*it);
      row.unpack();
      uint64_t field_len = 0;
      row_data += " [SHOW WARNING] ";
      for (unsigned int i = 0; i < columns_num; i++) {
        string cur_data = row.get_str(i, &field_len);
        cur_data = cur_data.substr(0, field_len);
        row_data += columns_name[i] + ": " + cur_data + ". ";
      }
      row_data += "\n";
      ++it;
    }
    LOG_INFO("%s", row_data.c_str());
  }
  if (!has_saved_too_many_warnings && warning_packet_list) {
    session->add_load_warning_packet_list(warning_packet_list,
                                          warning_packet_list_size);
  } else if (warning_packet_list) {
    for (vector<Packet *>::iterator it = warning_packet_list->begin();
         it != warning_packet_list->end(); ++it)
      delete (*it);
    delete warning_packet_list;
  }
  handler->get_session()->get_status()->item_inc(TIMES_WARNINGS_COUNT,
                                                 warnings);
  if (has_saved_too_many_warnings) {
    handler->get_session()->add_warning_dataspace(space, conn);
  }
  LOG_DEBUG("Get warning space %s%@\n", space->get_name(), space);
}

void handle_warnings_OK_and_eof_packet(ExecutePlan *plan, Packet *packet,
                                       Handler *handler, DataSpace *space,
                                       Connection *conn,
                                       bool skip_keep_conn = false) {
  if (plan == NULL) return;
  MySQLDriver *driver = (MySQLDriver *)plan->driver;
  handle_warnings_OK_and_eof_packet_inernal(driver, packet, handler, space,
                                            conn, skip_keep_conn);
}

inline void handle_warnings_OK_packet(Packet *packet, Handler *handler,
                                      DataSpace *space, Connection *conn,
                                      bool skip_keep_conn = false) {
  if (!support_show_warning || skip_keep_conn ||
      (conn && conn->is_changed_user_conn()))  // do not handle keep conn for
                                               // changed_user conn
    return;
  MySQLOKResponse ok(packet);
  ok.unpack();
  uint16_t warnings = ok.get_warnings();

  if (warnings) {
    handler->get_session()->get_status()->item_inc(TIMES_WARNINGS_COUNT,
                                                   warnings);
    handler->get_session()->add_warning_dataspace(space, conn);
    LOG_DEBUG("Get warning space %s%@\n", space->get_name(), space);
  }
}

/** Write the row data to the file.
 */
void MySQLIntoOutfileNode::write_into_outfile(Packet *packet,
                                              into_outfile_item *into_outfile,
                                              ofstream &file,
                                              spawn_param *param) {
  if (param && param->has_got_error()) {
    return;
  }
  string line_data;

  line_data.append(into_outfile->lines_start);

  char enclosed = into_outfile->fields_enclosed[0];
  char escaped = into_outfile->fields_escaped[0];

  MySQLRowResponse row(packet);
  unsigned int size = is_column_number.size();
  for (unsigned int i = 0; i != size; i++) {
    uint64_t row_len;
    const char *row_data = row.get_str(i, &row_len);
    bool is_null = row.field_is_null(i);
    /**
     * If the FIELDS ESCAPED BY is NULL(""), the NULL value will output as
     * "NULL", else the NULL values will output as the ESCAPED char + "N", like
     * the ESCAPED is "\\", output is "\N". But if the FIELDS TERNINATED BY is
     * NULL, it will output its length sapce.
     */
    if (is_null && !plan->statement->insert_select_via_fifo()) {
      if (into_outfile->fields_escaped_len == 0) {
        line_data.append("NULL");
      } else {
        line_data.append(into_outfile->fields_escaped);
        line_data.append("N");
      }

      if (i != size - 1) {
        line_data.append(into_outfile->fields_term);
      }

      continue;
    }

    /**
     * Process the field enclosed.
     * The output will add enclosed char before and after the column,
     * unless set the optionally and the column is a number.
     */
    bool enclosed_output_flag = false;
    if (into_outfile->fields_enclosed_len != 0 &&
        (!into_outfile->is_enclosed_optionally || !is_column_number[i])) {
      enclosed_output_flag = true;
    }

    if (enclosed_output_flag) {
      line_data.append(into_outfile->fields_enclosed);
    }

    char *escaped_str = NULL;
    if (row_len * 2 + 1 > DEFAULT_BUFFER_SIZE) {
      escaped_str = new char[row_len * 2 + 1];
    } else {
      escaped_str = row_buffer;
    }
    char *escaped_begin = escaped_str;
    const char *c = row_data;
    int index = 0;
    unsigned int row_index = 0;
    if (!is_column_number[i]) {
      // set the field escaped code, and the field is not a number
      while (row_index++ != row_len) {
        if (*c == '\0') {
          escaped_str[index++] = escaped;
          escaped_str[index++] = '0';
          c++;
        } else if (*c == escaped || *c == enclosed) {
          escaped_str[index++] = escaped;
          escaped_str[index++] = *c++;
        } else if (into_outfile->fields_enclosed_len == 0 &&
                   (*c == into_outfile->fields_term[0] ||
                    *c == into_outfile->lines_term[0])) {
          bool need_escaped = true;
          if (*c == into_outfile->fields_term[0] &&
              into_outfile->fields_term_len > 1) {
            if (row_len - row_index + 1 < into_outfile->fields_term_len)
              need_escaped = false;
            else if (strncmp((const char *)c,
                             (const char *)into_outfile->fields_term,
                             into_outfile->fields_term_len) != 0)
              need_escaped = false;
          } else if (*c == into_outfile->lines_term[0] &&
                     into_outfile->lines_term_len > 1) {
            if (row_len - row_index + 1 < into_outfile->lines_term_len)
              need_escaped = false;
            else if (strncmp((const char *)c,
                             (const char *)into_outfile->lines_term,
                             into_outfile->lines_term_len) != 0)
              need_escaped = false;
          }
          if (need_escaped) {
            escaped_str[index++] = escaped;
            escaped_str[index++] = *c++;
          } else {
            escaped_str[index++] = *c++;
          }
        } else {
          escaped_str[index++] = *c++;
        }
      }
      if (!is_null && row_len == 0 &&
          plan->statement->insert_select_via_fifo()) {  // for GOS specifically
        escaped_str[index++] = ' ';
      }
    } else {
      while (row_index++ != row_len) escaped_str[index++] = *c++;
    }
    escaped_str[index] = '\0';
    line_data.append(escaped_str);
    if (row_len * 2 + 1 > DEFAULT_BUFFER_SIZE) {
      delete[] escaped_begin;
    }

    if (enclosed_output_flag) line_data.append(into_outfile->fields_enclosed);

    if (i != size - 1) {
      line_data.append(into_outfile->fields_term);
    }
  }

  if (!is_local) {
    line_data.append(into_outfile->lines_term);
  } else {
    line_data.append(into_outfile->lines_term,
                     into_outfile->lines_term_len - 1);
  }
  if (param && param->has_got_error()) {
    return;
  }
  if (!is_local) {
    file << line_data.c_str();
  } else {
    MySQLRowResponse row_res(line_data);
    row_res.pack(packet);
  }
}

bool check_error_packet_found_rows(Packet *packet, MySQLSession *session,
                                   MySQLDriver *driver) {
  if (driver->is_error_packet(packet)) {
    LOG_ERROR("Found_rows get an error packet.\n");
    session->set_error_packet(packet);
    return true;
  }
  return false;
}

void found_rows(MySQLHandler *handler, MySQLDriver *driver, Connection *conn,
                MySQLSession *session) {
  Packet *packet = Backend::instance()->get_new_packet(row_packet_size);
  Packet exec_packet;

  conn->reset();
  MySQLQueryRequest query("SELECT FOUND_ROWS()");
  query.pack(&exec_packet);

  try {
    handler->send_to_server(conn, &exec_packet);
    handler->receive_from_server(conn, packet);

    if (check_error_packet_found_rows(packet, session, driver)) return;

    handler->receive_from_server(conn, packet);
    while (!driver->is_eof_packet(packet)) {
      if (check_error_packet_found_rows(packet, session, driver)) return;
      handler->receive_from_server(conn, packet);
    }

    handler->receive_from_server(conn, packet);
    while (!driver->is_eof_packet(packet)) {
      if (check_error_packet_found_rows(packet, session, driver)) return;
      MySQLRowResponse row(packet);
      session->add_found_rows(row.get_uint(0));
      handler->receive_from_server(conn, packet);
    }
  } catch (Exception &e) {
    LOG_ERROR("Send FOUND_ROWS() to server got error!\n");
    delete packet;
    throw;
  }
  delete packet;
}

void record_modify_server(ExecutePlan *plan, Session *session,
                          const char *server_name, unsigned int vid,
                          bool is_non_modified_conn) {
  if (session->check_for_transaction() &&
      !session->is_in_cluster_xa_transaction()) {
    stmt_type type = plan->statement->get_stmt_node()->type;
    switch (type) {
      case STMT_UPDATE:
      case STMT_DELETE:
      case STMT_INSERT:
      case STMT_REPLACE:
      case STMT_REPLACE_SELECT:
      case STMT_INSERT_SELECT:
      case STMT_LOAD: {
        if (!is_non_modified_conn)
          session->record_modify_server(server_name, vid);
        break;
      }
      default:
        break;
    }
  }
}

void record_xa_modify_sql(ExecutePlan *plan, Session *session,
                          DataSpace *dataspace, const char *sql,
                          bool is_non_modified_conn) {
  if (enable_xa_transaction &&
      session->get_session_option("close_session_xa").int_val == 0 &&
      session->check_for_transaction() &&
      !session->is_in_transaction_consistent()) {
    stmt_type type = plan->statement->get_stmt_node()->type;
    Driver *driver = Driver::get_driver();
    XA_helper *xa_helper = driver->get_xa_helper();
    switch (type) {
      case STMT_UPDATE:
      case STMT_DELETE:
      case STMT_INSERT:
      case STMT_REPLACE:
      case STMT_REPLACE_SELECT:
      case STMT_INSERT_SELECT: {
        if (!is_non_modified_conn)
          xa_helper->record_xa_redo_log(session, dataspace, sql);
        break;
      }
      case STMT_SET:
      case STMT_CHANGE_DB: {
        BackendServerVersion v =
            Backend::instance()->get_backend_server_version();
        if (!(v == MYSQL_VERSION_57 || v == MYSQL_VERSION_8)) {
          xa_helper->record_xa_redo_log(session, dataspace, sql, true);
        }
        break;
      }
      default:
        break;
    }
  }
}

void send_ok_packet_to_client(Handler *handler, uint64_t affected_rows,
                              uint16_t warnings) {
  Packet ok_packet;
  MySQLOKResponse ok(affected_rows, warnings);
  ok.pack(&ok_packet);
  handler->deal_autocommit_with_ok_eof_packet(&ok_packet);
  handler->record_affected_rows(&ok_packet);
  handler->send_to_client(&ok_packet);
}

void record_migrate_error_message(ExecutePlan *plan, Packet *packet, string msg,
                                  bool server_innodb_rollback_on_timeout) {
  if (plan->get_migrate_tool() && packet) {
    try {
      MySQLErrorResponse error(packet);
      error.unpack();
      char message[1000];
      sprintf(message, "%s %d (%s) %s.", msg.c_str(), error.get_error_code(),
              error.get_sqlstate(), error.get_error_message());
      plan->get_migrate_tool()->set_error_message(message);
      plan->get_migrate_tool()->add_select_error_not_rollback_info(
          error.get_error_code(), server_innodb_rollback_on_timeout);
    } catch (exception &e) {
      LOG_ERROR("record_migrate_error_message get exception [%s]\n", e.what());
    }
  }
}
void record_migrate_error_message(ExecutePlan *plan, string msg) {
  if (plan->get_migrate_tool()) {
    plan->get_migrate_tool()->set_error_message(msg);
  }
}

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

void set_select_uservar_by_result(
    Packet *packet, vector<bool> &field_is_num_vec,
    vector<pair<unsigned int, string> > &uservar_vec,
    unsigned int select_field_num, Session *session) {
  MySQLRowResponse row_response(packet);
  unsigned int fields_num = field_is_num_vec.size();

  unsigned int j = 0;
  for (unsigned int i = 0; i != fields_num; i++) {
    if (j == uservar_vec.size()) break;
    pair<unsigned int, string> uservar_pair = uservar_vec[j];
    if (uservar_pair.first != i + select_field_num - fields_num) {
      continue;
    }

    string uservar_name = uservar_pair.second;
    if (row_response.field_is_null(i)) {
      write_result_to_uservar(session, uservar_name, NULL);
      j++;
      continue;
    }

    uint64_t length;
    const char *str = row_response.get_str(i, &length);
    write_result_to_uservar(session, uservar_name, str, length,
                            field_is_num_vec[i]);
    j++;
  }
}

void store_warning_packet(Connection *conn, Handler *handler,
                          MySQLDriver *driver,
                          vector<Packet *> **warning_packet_list,
                          bool &has_add_warning_packet,
                          uint64_t &warning_packet_list_size) {
  Packet exec_packet;
  MySQLQueryRequest show_warning_query("SHOW WARNINGS");
  show_warning_query.pack(&exec_packet);
  conn->reset();
  handler->send_to_server(conn, &exec_packet);
  // receive header
  Packet *packet = NULL;
  packet = Backend::instance()->get_new_packet();
  try {
    handler->receive_from_server(conn, packet);
    if (driver->is_error_packet(packet)) {
      MySQLErrorResponse error(packet);
      error.unpack();
      LOG_ERROR(
          "when try to fetch show warnings info, get an error packet, %d (%s) "
          "%s.\n",
          packet, error.get_error_code(), error.get_sqlstate(),
          error.get_error_message());
      throw Error("when try to fetch show warnings info, get error packet");
    }
    if (!has_add_warning_packet) {
      *warning_packet_list = NULL;
      (*warning_packet_list) = new vector<Packet *>();
      (*warning_packet_list)->push_back(packet);
      packet = NULL;
      packet = Backend::instance()->get_new_packet();
    }
    // receive column packets
    handler->receive_from_server(conn, packet);
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        MySQLErrorResponse error(packet);
        error.unpack();
        LOG_ERROR(
            "when try to fetch show warnings info, get an error packet, %d "
            "(%s) "
            "%s.\n",
            packet, error.get_error_code(), error.get_sqlstate(),
            error.get_error_message());
        throw Error("when try to fetch show warnings info, get error packet");
      }
      if (!has_add_warning_packet) {
        (*warning_packet_list)->push_back(packet);
        packet = NULL;
        packet = Backend::instance()->get_new_packet();
      }
      handler->receive_from_server(conn, packet);
    }
    if (!has_add_warning_packet) {
      (*warning_packet_list)->push_back(packet);
      packet = NULL;
      packet = Backend::instance()->get_new_packet();
      has_add_warning_packet = true;
    } else {
      // remove the previous tailed eof packet in warning_packet_list, then
      // append more warning packet
      Packet *tmp_pkt = (*warning_packet_list)->back();
      (*warning_packet_list)->pop_back();
      delete tmp_pkt;
    }
    // receive row packets
    handler->receive_from_server(conn, packet);
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        MySQLErrorResponse error(packet);
        error.unpack();
        LOG_ERROR(
            "when try to fetch show warnings info, get an error packet, %d "
            "(%s) "
            "%s.\n",
            packet, error.get_error_code(), error.get_sqlstate(),
            error.get_error_message());
        throw Error("when try to fetch show warnings info, get error packet");
      }
      if (load_data_quick_error > 0 &&
          handler->get_session()->get_top_stmt()->get_stmt_node()->type ==
              STMT_LOAD) {
        const char *field_data = NULL;
        uint64_t row_len = 0;
        MySQLRowResponse response(packet);
        // Level, Code, Message
        field_data = response.get_str(0, &row_len);
        string level = response.field_is_null(0) ? string("")
                                                 : string(field_data, row_len);
        field_data = response.get_str(1, &row_len);
        string code = response.field_is_null(1) ? string("")
                                                : string(field_data, row_len);
        field_data = response.get_str(2, &row_len);
        string message = response.field_is_null(2)
                             ? string("")
                             : string(field_data, row_len);
        LOG_ERROR(
            "got error when load data: Level:[%s], Code:[%s], Message:[%s]\n",
            level.c_str(), code.c_str(), message.c_str());
        LOG_ERROR(
            "load_data_quick_error is enabled, cancel current load data task, "
            "this will lead client broken.\n");
        throw Error(
            "got error when load data, load_data_quick_error is enabled, "
            "cancel "
            "current load data task, this will lead client broken, see log for "
            "more infomation");
      }
#ifdef DEBUG
      LOG_DEBUG("store one warning packet during load data.\n");
#endif
      (*warning_packet_list)->push_back(packet);
      packet = NULL;
      warning_packet_list_size++;
      packet = Backend::instance()->get_new_packet();
      handler->receive_from_server(conn, packet);
    }
#ifndef DBSCALE_TEST_DISABLE
    dbscale_test_info *test_info =
        handler->get_session()->get_dbscale_test_info();
    if (test_info->test_case_name == "show warnings" &&
        test_info->test_case_operation == "test memory leak") {
      throw ExecuteNodeError("Show warnings Fake Error!");
    }
#endif
    (*warning_packet_list)->push_back(packet);
  } catch (...) {
    delete packet;
    if (*warning_packet_list) {
      vector<Packet *>::iterator it = (*warning_packet_list)->begin();
      for (; it != (*warning_packet_list)->end(); ++it) {
        delete *it;
      }
      delete *warning_packet_list;
      *warning_packet_list = NULL;
    }
    throw;
  }
}

size_t check_first_row_complete(Packet *packet, string field_terminate,
                                string line_terminate, bool has_field_enclose,
                                char field_enclose, char field_escape,
                                unsigned int table_fields_num) {
  ACE_UNUSED_ARG(table_fields_num);
  const char *pos = packet->base() + PACKET_HEADER_SIZE;
  const char *end = packet->base() + packet->length();
  const char *start = pos;
#ifdef DEBUG
  unsigned int fields_count = 0;
#endif
  bool is_char_escaped = false;
  bool need_enclose = false;
  if (has_field_enclose && field_enclose == *start) need_enclose = true;
  bool enclose_field_end = false;
  int field_term_len = field_terminate.length();
  int line_term_len = line_terminate.length();
  while (pos != end) {
    if (is_char_escaped) {
      is_char_escaped = false;
      pos++;
      continue;
    }

    if (*pos == line_terminate[0]) {
      if (line_term_len == 1) {
        break;
      } else if (end - pos < line_term_len) {
        pos = end;
        break;
      } else if (line_terminate == string(pos, line_term_len)) {
        pos += line_term_len - 1;
        break;
      }
    }

    if (*pos == field_escape) {
      is_char_escaped = true;
      pos++;
      continue;
    }

    if (need_enclose && !enclose_field_end && *pos == field_enclose &&
        pos != start) {
      enclose_field_end = true;
      pos++;
      continue;
    }

    if (enclose_field_end && *pos == field_enclose) {
    } else if (*pos == field_terminate[0]) {
      if (need_enclose && !enclose_field_end) {
        pos++;
        continue;
      }
      if (end - pos < field_term_len) {
        pos++;
        continue;
      }
      if (field_term_len != 1 &&
          field_terminate != string(pos, field_term_len)) {
        pos++;
        continue;
      } else {
        pos += field_term_len - 1;
      }
#ifdef DEBUG
      fields_count++;
#endif
      start = pos + 1;
      if (has_field_enclose && *start == field_enclose) {
        need_enclose = true;
        enclose_field_end = false;
      } else {
        need_enclose = false;
      }
    }
    pos++;
    enclose_field_end = false;
  }
  if (pos == end) {
    /*This packet does not reach the end of packet, so we need to read one
     * more packet.*/
    return packet->length();
  }

#ifdef DEBUG
  if (table_fields_num) ACE_ASSERT(fields_count <= table_fields_num);
#endif
  // TODO: return 0, if it contains the complete row in issue #3675
  /*
  if (*pos == line_terminate && fields_count == table_fields_num - 1)
    return 0;
  */
  return pos - (packet->base() + PACKET_HEADER_SIZE) + 1;
}

void divide_packet_by_first_row(Packet *packet, size_t first_row_len,
                                Packet **first_row_packet,
                                Packet **rest_row_packet) {
  Packet *pf = Backend::instance()->get_new_packet(first_row_len +
                                                   PACKET_HEADER_SIZE + 1);
  memcpy(pf->base() + PACKET_HEADER_SIZE, packet->base() + PACKET_HEADER_SIZE,
         first_row_len);
  char *p = packet->base();
  size_t len = Packet::unpack3uint(&p);
  Packet *pr = Backend::instance()->get_new_packet(len - first_row_len +
                                                   PACKET_HEADER_SIZE + 1);
  memcpy(pr->base() + PACKET_HEADER_SIZE,
         packet->base() + PACKET_HEADER_SIZE + first_row_len,
         len - first_row_len);
  *first_row_packet = pf;
  *rest_row_packet = pr;

  pf->rewind();
  pr->rewind();

  pack_header(pf, first_row_len);
  pack_header(pr, len - first_row_len);

#ifdef DEBUG
  LOG_DEBUG(
      "Find a divided packet with first incomplete row pos %d, reset packet "
      "len %d for pacekt %@.\n",
      first_row_len, len - first_row_len, packet);
#endif
  if (len == first_row_len) {
    LOG_DEBUG("Find a one row packet, should not divide it.\n");
    delete pf;
    delete pr;
    *first_row_packet = NULL;
    *rest_row_packet = NULL;
  }
}

void pack_packet(Packet *packet, string data, unsigned int len) {
  packet->size(len + PACKET_HEADER_SIZE + 1);
  try {
    memcpy(packet->base() + PACKET_HEADER_SIZE, data.c_str(), len);
  } catch (...) {
    LOG_ERROR("pack_packet get exception for pack_packet.\n");
    throw;
  }
  pack_header(packet, len);
}

void redo_load(Connection *load_conn, string sql, Handler *handler,
               MySQLDriver *driver, uint64_t &affected_row_count,
               uint64_t &warning_count, uint64_t &warning_packet_list_size,
               vector<Packet *> **warning_packet_list,
               bool &has_add_warning_packet) {
  LOG_DEBUG("redo_load\n");
  // send empty to tell server load end
  TimeValue timeout =
      TimeValue(redo_load_timeout > 0 ? redo_load_timeout : UINT_MAX, 0);
  Packet *packet = Backend::instance()->get_new_packet();
  pack_packet(packet, "", 0);
  handler->send_to_server(load_conn, packet);
  handler->receive_from_server(load_conn, packet, &timeout);
  if (driver->is_error_packet(packet)) {
    MySQLErrorResponse error(packet);
    error.unpack();
    LOG_ERROR("redo_load get an error packet %d (%s) %s.\n",
              error.get_error_code(), error.get_sqlstate(),
              error.get_error_message());
    char err_msg[256];
    sprintf(err_msg, "redo_load get an error packet, %d (%s) %s.\n",
            error.get_error_code(), error.get_sqlstate(),
            error.get_error_message());

    if (packet) {
      delete packet;
    }
    throw Error(err_msg);
  }

  MySQLOKResponse ok(packet);
  ok.unpack();
  uint64_t warnings = ok.get_warnings();
  uint64_t affected_rows = ok.get_affected_rows();
  affected_row_count += affected_rows;
  warning_count += warnings;
  if (support_show_warning &&
      (warning_packet_list_size < MAX_LOAD_WARNING_PACKET_LIST_SIZE) &&
      warnings)
    store_warning_packet(load_conn, handler, driver, warning_packet_list,
                         has_add_warning_packet, warning_packet_list_size);
  load_conn->reset();

  // do load again
  MySQLQueryRequest query(sql.c_str());
  query.set_sql_replace_char(
      handler->get_session()->get_query_sql_replace_null_char());
  Packet exec_query;
  query.pack(&exec_query);
  handler->send_to_server(load_conn, &exec_query);
  handler->receive_from_server(load_conn, packet, &timeout);
  if (driver->is_error_packet(packet)) {
    MySQLErrorResponse error(packet);
    error.unpack();
    LOG_ERROR("redo_load get an error packet %d (%s) %s.\n",
              error.get_error_code(), error.get_sqlstate(),
              error.get_error_message());
    char err_msg[256];
    sprintf(err_msg, "redo_load get an error packet, %d (%s) %s.\n",
            error.get_error_code(), error.get_sqlstate(),
            error.get_error_message());

    if (packet) {
      delete packet;
    }
    throw Error(err_msg);
  }
  if (packet) {
    delete packet;
  }
}

/* class MySQLExecutePlan */

MySQLExecutePlan::MySQLExecutePlan(Statement *statement, Session *session,
                                   Driver *driver, Handler *handler)
    : ExecutePlan(statement, session, driver, handler) {}

ExecuteNode *MySQLExecutePlan::do_get_fetch_node(DataSpace *dataspace,
                                                 const char *sql) {
  return new MySQLFetchNode(this, dataspace, sql);
}
ExecuteNode *MySQLExecutePlan::do_get_navicat_profile_sql_node() {
  return new MySQLNavicateProfileSqlNode(this);
}

#ifndef DBSCALE_DISABLE_SPARK
ExecuteNode *MySQLExecutePlan::do_get_spark_node(SparkConfigParameter config,
                                                 bool is_insert) {
  return new MySQLSparkNode(this, config, is_insert);
}
#endif

ExecuteNode *MySQLExecutePlan::do_get_send_node() {
  return new MySQLSendNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dispatch_packet_node(TransferRule *rule) {
  return new MySQLDispatchPacketNode(this, rule);
}

ExecuteNode *MySQLExecutePlan::do_get_into_outfile_node() {
  return new MySQLIntoOutfileNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_modify_limit_node(
    record_scan *rs, PartitionedTable *table, vector<unsigned int> &par_ids,
    unsigned int limit, string &sql) {
  return new MySQLModifyLimitNode(this, rs, table, par_ids, limit, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_select_into_node() {
  return new MySQLSelectIntoNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_sort_node(list<SortDesc> *sort_desc) {
  return new MySQLSortNode(this, sort_desc);
}

ExecuteNode *MySQLExecutePlan::do_get_project_node(unsigned int skip_columns) {
  return new MySQLProjectNode(this, skip_columns);
}

ExecuteNode *MySQLExecutePlan::do_get_aggr_node(
    list<AggregateDesc> &aggregate_desc) {
  return new MySQLAggregateNode(this, aggregate_desc);
}

ExecuteNode *MySQLExecutePlan::do_get_group_node(
    list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc) {
  ExecuteNode *node = new MySQLGroupNode(this, group_desc, aggregate_desc);
#ifdef DEBUG
  LOG_DEBUG("in MySQLExecutePlan::do_get_group_node(), get node %@\n", node);
#endif
  return node;
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_wise_group_node(
    list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc) {
  return new MySQLWiseGroupNode(this, group_desc, aggregate_desc);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_pages_node(
    list<SortDesc> *group_desc, list<AggregateDesc> &aggregate_desc,
    int page_size) {
  return new MySQLPagesNode(this, group_desc, aggregate_desc, page_size);
}

ExecuteNode *MySQLExecutePlan::do_get_single_sort_node(
    list<SortDesc> *sort_desc) {
  return new MySQLSingleSortNode(this, sort_desc);
}

ExecuteNode *MySQLExecutePlan::do_get_direct_execute_node(DataSpace *dataspace,
                                                          const char *sql) {
  return new MySQLDirectExecuteNode(this, dataspace, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_show_warning_node() {
  return new MySQLShowWarningNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_show_load_warning_node() {
  return new MySQLShowLoadWarningNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_query_one_column_node(
    DataSpace *dataspace, const char *sql, bool only_one_row) {
  return new MySQLQueryForOneColumnNode(this, dataspace, sql, only_one_row);
}
ExecuteNode *MySQLExecutePlan::do_get_query_mul_column_node(
    DataSpace *dataspace, const char *sql) {
  return new MySQLQueryForMulColumnNode(this, dataspace, sql);
}
ExecuteNode *MySQLExecutePlan::do_get_query_one_column_aggr_node(
    DataSpace *dataspace, const char *sql, bool get_min) {
  return new MySQLQueryForOneColumnAggrNode(this, dataspace, sql, get_min);
}
ExecuteNode *MySQLExecutePlan::do_get_query_exists_node(DataSpace *dataspace,
                                                        const char *sql) {
  return new MySQLQueryExistsNode(this, dataspace, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_transaction_unlock_node(
    const char *sql, bool is_send_to_client) {
  return new MySQLTransactionUNLockNode(this, sql, is_send_to_client);
}

ExecuteNode *MySQLExecutePlan::do_get_xatransaction_node(
    const char *sql, bool is_send_to_client) {
  return new MySQLXATransactionNode(this, sql, is_send_to_client);
}

ExecuteNode *MySQLExecutePlan::do_get_cluster_xatransaction_node() {
  return new MySQLClusterXATransactionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_avg_node(list<AvgDesc> &avg_list) {
  return new MySQLAvgNode(this, avg_list);
}
ExecuteNode *MySQLExecutePlan::do_get_lock_node(
    map<DataSpace *, list<lock_table_item *> *> *lock_table_list) {
  return new MySQLLockNode(this, lock_table_list);
}

ExecuteNode *MySQLExecutePlan::do_get_migrate_node() {
  return new MySQLMigrateNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_limit_node(long offset, long num) {
  return new MySQLLimitNode(this, offset, num);
}

ExecuteNode *MySQLExecutePlan::do_get_ok_node() {
  return new MySQLOKNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_drop_mul_table_node() {
  return new MySQLDropMulTableNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_ok_merge_node() {
  return new MySQLOKMergeNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_modify_node(DataSpace *dataspace,
                                                  const char *sql,
                                                  bool re_parse_shard) {
  MySQLModifyNode *modify_node = new MySQLModifyNode(this, dataspace);
  if (sql) {
    if (re_parse_shard) {
      modify_node->add_sql_during_exec(sql);
    } else {
      modify_node->add_sql(sql);
    }
    modify_node->add_sql("");
  }
  return modify_node;
}

ExecuteNode *MySQLExecutePlan::do_get_modify_select_node(
    const char *modify_sql, vector<string *> *columns, bool can_quick,
    vector<ExecuteNode *> *nodes, bool is_replace_set_value) {
  MySQLModifySelectNode *node = new MySQLModifySelectNode(
      this, modify_sql, columns, nodes, can_quick, is_replace_set_value);
  return node;
}

ExecuteNode *MySQLExecutePlan::do_get_insert_select_node(
    const char *modify_sql, vector<ExecuteNode *> *nodes) {
  MySQLInsertSelectNode *node =
      new MySQLInsertSelectNode(this, modify_sql, nodes);
  return node;
}

ExecuteNode *MySQLExecutePlan::do_get_par_modify_select_node(
    const char *modify_sql, PartitionedTable *par_table,
    PartitionMethod *method, vector<string *> *columns, bool can_quick,
    vector<ExecuteNode *> *nodes) {
  MySQLPartitionModifySelectNode *node = new MySQLPartitionModifySelectNode(
      this, modify_sql, nodes, par_table, method, columns, can_quick);
  return node;
}
ExecuteNode *MySQLExecutePlan::do_get_par_insert_select_node(
    const char *modify_sql, PartitionedTable *par_table,
    PartitionMethod *method, vector<unsigned int> &key_pos,
    vector<ExecuteNode *> *nodes, const char *schema_name,
    const char *table_name, bool is_duplicated) {
  MySQLPartitionInsertSelectNode *node = new MySQLPartitionInsertSelectNode(
      this, modify_sql, nodes, par_table, method, key_pos, schema_name,
      table_name, is_duplicated);
  return node;
}

ExecuteNode *MySQLExecutePlan::do_get_load_local_node(DataSpace *dataspace,
                                                      const char *sql) {
  return new MySQLLoadLocalNode(this, dataspace, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_load_local_external_node(
    DataSpace *dataspace, DataServer *dataserver, const char *sql) {
  return new MySQLLoadLocalExternal(this, dataspace, dataserver, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_load_data_infile_external_node(
    DataSpace *dataspace, DataServer *dataserver) {
  return new MySQLLoadDataInfileExternal(this, dataspace, dataserver);
}

ExecuteNode *MySQLExecutePlan::do_get_kill_node(int cluster_id, uint32_t kid) {
  return new MySQLKillNode(this, cluster_id, kid);
}

ExecuteNode *MySQLExecutePlan::do_get_load_local_part_table_node(
    PartitionedTable *dataspace, const char *sql, const char *schema_name,
    const char *table_name) {
  return new MySQLLoadLocalPartTableNode(this, dataspace, sql, schema_name,
                                         table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_load_data_infile_part_table_node(
    PartitionedTable *dataspace, const char *sql, const char *schema_name,
    const char *table_name) {
  return new MySQLLoadDataInfilePartTableNode(this, dataspace, sql, schema_name,
                                              table_name);
}

ExecuteNode *MySQLExecutePlan::do_get_load_data_infile_node(
    DataSpace *dataspace, const char *sql) {
  return new MySQLLoadDataInfileNode(this, dataspace, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_load_select_node(const char *schema_name,
                                                       const char *table_name) {
  return new MySQLLoadSelectNode(this, schema_name, table_name);
}

ExecuteNode *MySQLExecutePlan::do_get_load_select_partition_node(
    const char *schema_name, const char *table_name) {
  return new MySQLLoadSelectPartitionNode(this, schema_name, table_name);
}

ExecuteNode *MySQLExecutePlan::do_get_distinct_node(list<int> column_indexes) {
  return new MySQLDistinctNode(this, column_indexes);
}
ExecuteNode *MySQLExecutePlan::do_get_rows_node(
    list<list<string> *> *row_list) {
  return new MySQLRowsNode(this, row_list);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_cluster_shutdown_node(
    int cluster_id) {
  return new MySQLDBScaleClusterShutdownNode(this, cluster_id);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_cluster_id_node() {
  return new MySQLDBScaleRequestClusterIdNode(this);
}
ExecuteNode *
MySQLExecutePlan::do_get_dbscale_request_all_cluster_inc_info_node() {
  return new MySQLDBScaleRequestAllClusterIncInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_cluster_info_node() {
  return new MySQLDBScaleRequestClusterInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_cluster_user_status_node(
    const char *id, bool only_show_running, bool show_status_count) {
  return new MySQLDBScaleRequestUserStatusNode(this, id, only_show_running,
                                               show_status_count);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_clean_temp_table_cache_node() {
  return new MySQLDBScaleCleanTempTableCacheNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_node_info_node() {
  return new MySQLDBScaleRequestNodeInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_cluster_inc_info_node(
    PartitionedTable *space, const char *schema_name, const char *table_name) {
  return new MySQLDBScaleRequestClusterIncInfoNode(this, space, schema_name,
                                                   table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_regex_filter_node(
    enum FilterFlag filter_way, int column_index, list<const char *> *pattern) {
  return new MySQLRegexFilterNode(this, filter_way, column_index, pattern);
}
ExecuteNode *MySQLExecutePlan::do_get_expr_filter_node(Expression *expr) {
  return new MySQLExprFilterNode(this, expr);
}
ExecuteNode *MySQLExecutePlan::do_get_expr_calculate_node(
    list<SelectExprDesc> &select_expr_list) {
  return new MySQLExprCalculateNode(this, select_expr_list);
}
ExecuteNode *MySQLExecutePlan::do_get_avg_show_table_status_node() {
  return new MySQLAvgShowTableStatusNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_show_data_source_node(
    list<const char *> &names, bool need_show_weight) {
  return new MySQLDBScaleShowDataSourceNode(this, names, need_show_weight);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_configuration_node() {
  return new MySQLDynamicConfigurationNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_rep_strategy_node() {
  return new MySQLRepStrategyNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_set_server_weight_node() {
  return new MySQLDynamicSetServerWeightNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_option_node() {
  return new MySQLDBScaleShowOptionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_dynamic_option_node() {
  return new MySQLDBScaleShowDynamicOptionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_change_startup_config_node() {
  return new MySQLChangeStartupConfigNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_changed_startup_config_node() {
  return new MySQLShowChangedStartupConfigNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_add_data_server_node(
    dynamic_add_data_server_op_node *dynamic_add_data_server_oper) {
  return new MySQLDynamicAddDataServerNode(this, dynamic_add_data_server_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_add_data_source_node(
    dynamic_add_data_source_op_node *dynamic_add_data_source_oper,
    DataSourceType type) {
  return new MySQLDynamicAddDataSourceNode(this, dynamic_add_data_source_oper,
                                           type);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_add_data_space_node(
    dynamic_add_data_space_op_node *dynamic_add_data_space_oper) {
  return new MySQLDynamicAddDataSpaceNode(this, dynamic_add_data_space_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_set_schema_acl_node(
    set_schema_acl_op_node *set_schema_acl_oper) {
  return new MySQLSetSchemaACLNode(this, set_schema_acl_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_set_user_not_allow_operation_time_node(
    set_user_not_allow_operation_time_node *oper) {
  return new MySQLSetUserAllowOperationTimeNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_reload_user_not_allow_operation_time_node(
    string user_name) {
  return new MySQLReloadUserAllowOperationTimeNode(this, user_name);
}
ExecuteNode *MySQLExecutePlan::do_get_show_user_not_allow_operation_time_node(
    string user_name) {
  return new MySQLShowUserNotAllowOperationTimeNode(this, user_name);
}
ExecuteNode *MySQLExecutePlan::do_get_set_table_acl_node(
    set_table_acl_op_node *set_table_acl_oper) {
  return new MySQLSetTableACLNode(this, set_table_acl_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_add_slave_node(
    dynamic_add_slave_op_node *dynamic_add_slave_oper) {
  return new MySQLDynamicAddSlaveNode(this, dynamic_add_slave_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_reset_tmp_table_node() {
  return new MySQLResetTmpTableNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_reload_function_type_node() {
  return new MySQLLoadFuncTypeNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_shutdown_node() {
  return new MySQLShutDownNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_change_master_node(
    dynamic_change_master_op_node *dynamic_change_master_oper) {
  return new MySQLDynamicChangeMasterNode(this, dynamic_change_master_oper);
}
ExecuteNode *
MySQLExecutePlan::do_get_dynamic_change_multiple_master_active_node(
    dynamic_change_multiple_master_active_op_node
        *dynamic_change_multiple_master_active_oper) {
  return new MySQLDynamicChangeMultipleMasterActiveNode(
      this, dynamic_change_multiple_master_active_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_change_dataserver_ssh_node(
    const char *server_name, const char *username, const char *pwd, int port) {
  return new MySQLDynamicChangeDataServerSShNode(this, server_name, username,
                                                 pwd, port);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_remove_slave_node(
    dynamic_remove_slave_op_node *dynamic_remove_slave_oper) {
  return new MySQLDynamicRemoveSlaveNode(this, dynamic_remove_slave_oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_remove_schema_node(
    const char *schema_name, bool is_force) {
  return new MySQLDynamicRemoveSchemaNode(this, schema_name, is_force);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_remove_table_node(
    const char *table_name, bool is_force) {
  return new MySQLDynamicRemoveTableNode(this, table_name, is_force);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_change_table_scheme_node(
    const char *table_name, const char *scheme_name) {
  return new MySQLDynamicChangeTableSchemeNode(this, table_name, scheme_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_remove_node(const char *name) {
  return new MySQLDynamicRemoveOPNode(this, name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_help_node(const char *name) {
  return new MySQLDBScaleHelpNode(this, name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_audit_user_list_node() {
  return new MySQLDBScaleShowAuditUserListNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_slow_sql_top_n_node() {
  return new MySQLDBScaleShowSlowSqlTopNNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_request_slow_sql_top_n_node() {
  return new MySQLDBScaleRequestSlowSqlTopNNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_join_node(const char *name) {
  return new MySQLDBScaleShowJoinNode(this, name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_shard_partition_node(
    const char *name) {
  return new MySQLDBScaleShowShardPartitionNode(this, name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_rebalance_work_load_node(
    const char *name, list<string> sources, const char *schema_name,
    int is_remove) {
  return new MySQLDBScaleShowRebalanceWorkLoadNode(this, name, sources,
                                                   schema_name, is_remove);
}
ExecuteNode *MySQLExecutePlan::do_get_show_user_memory_status_node() {
  return new MySQLDBScaleShowUserMemoryStatusNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_user_status_node(
    const char *user_id, const char *user_name, bool only_show_running,
    bool instance, bool show_status_count) {
  return new MySQLDBScaleShowUserStatusNode(
      this, user_id, user_name, only_show_running, instance, show_status_count);
}
ExecuteNode *MySQLExecutePlan::do_get_show_user_processlist_node(
    const char *cluster_id, const char *user_id, int local) {
  return new MySQLDBScaleShowUserProcesslistNode(this, cluster_id, user_id,
                                                 local);
}
ExecuteNode *MySQLExecutePlan::do_get_show_session_id_node(
    const char *server_name, int connection_id) {
  return new MySQLDBScaleShowSessionIdNode(this, server_name, connection_id);
}
ExecuteNode *MySQLExecutePlan::do_get_backend_server_execute_node(
    const char *stmt_sql) {
  return new MySQLDBScaleBackendServerExecuteNode(this, stmt_sql);
}
ExecuteNode *MySQLExecutePlan::do_get_execute_on_all_masterserver_execute_node(
    const char *stmt_sql) {
  return new MySQLDBScaleExecuteOnAllMasterserverExecuteNode(this, stmt_sql);
}
ExecuteNode *MySQLExecutePlan::do_get_show_table_location_node(
    const char *schema_name, const char *table_name) {
  return new MySQLDBScaleShowTableLocationNode(this, schema_name, table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_erase_auth_info_node(
    name_item *username_list, bool all_dbscale_node) {
  return new MySQLDBScaleEraseAuthInfoNode(this, username_list,
                                           all_dbscale_node);
}
ExecuteNode *MySQLExecutePlan::do_get_dynamic_update_white_node(
    bool is_add, const char *ip, const ssl_option_struct &ssl_option_value,
    const char *comment, const char *user_name) {
  return new MySQLDBScaleDynamicUpdateWhiteNode(
      this, is_add, ip, ssl_option_value, comment, user_name);
}
ExecuteNode *MySQLExecutePlan::do_get_show_monitor_point_status_node() {
  return new MySQLShowMonitorPointStatusNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_global_monitor_point_status_node() {
  return new MySQLShowGlobalMonitorPointStatusNode(this);
}
ExecuteNode *
MySQLExecutePlan::do_get_show_histogram_monitor_point_status_node() {
  return new MySQLShowHistogramMonitorPointStatusNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_outline_monitor_info_node() {
  return new MySQLShowOutlineMonitorInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_create_outline_hint_node(
    dbscale_operate_outline_hint_node *oper) {
  return new MySQLDBScaleCreateOutlineHintNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_outline_hint_node(
    dbscale_operate_outline_hint_node *oper) {
  return new MySQLDBScaleFlushOutlineHintNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_outline_hint_node(
    dbscale_operate_outline_hint_node *oper) {
  return new MySQLDBScaleShowOutlineHintNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_delete_outline_hint_node(
    dbscale_operate_outline_hint_node *oper) {
  return new MySQLDBScaleDeleteOutlineHintNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_com_query_prepare_node(
    DataSpace *dataspace, const char *query_sql, const char *name,
    const char *sql) {
  return new MySQLComQueryPrepareNode(this, dataspace, query_sql, name, sql);
}
ExecuteNode *MySQLExecutePlan::do_get_com_query_exec_prepare_node(
    const char *name, var_item *var_item_list) {
  return new MySQLComQueryExecPrepareNode(this, name, var_item_list);
}
ExecuteNode *MySQLExecutePlan::do_get_com_query_drop_prepare_node(
    DataSpace *dataspace, const char *name, const char *prepare_sql) {
  return new MySQLComQueryDropPrepareNode(this, dataspace, name, prepare_sql);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_config_to_file_node(
    const char *file_name, bool flush_all) {
  return new MySQLDBScaleFlushConfigToFileNode(this, file_name, flush_all);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_table_info_node(
    const char *schema_name, const char *table_name) {
  return new MySQLDBScaleFlushTableInfoNode(this, schema_name, table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_acl_node() {
  return new MySQLDBScaleFlushACLNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_global_consistence_point_node() {
  return new MySQLDBScaleGlobalConsistencePointNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_pool_info_node() {
  return new MySQLDBScaleShowPoolInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_pool_version_node() {
  return new MySQLDBScaleShowPoolVersionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_set_pool_info_node(
    pool_node *pool_info) {
  return new MySQLResetPoolInfoNode(this, pool_info);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_reset_info_plan(
    dbscale_reset_info_op_node *oper) {
  return new MySQLResetInfoPlanNode(this, oper);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_block_node() {
  return new MySQLBlockNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_disable_server_node() {
  return new MySQLDisableServer(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_check_table_node() {
  return new MySQLCheckTableNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_check_disk_io_node() {
  return new MySQLCheckDiskIONode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_check_metadata() {
  return new MySQLCheckMetaDataNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_pool_info_node(
    pool_node *pool_info) {
  return new MySQLFlushPoolInfoNode(this, pool_info);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_force_flashback_online_node(
    const char *name) {
  return new MySQLForceFlashbackOnlineNode(this, name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_xa_recover_slave_dbscale_node(
    const char *xa_source, const char *top_source, const char *ka_update_v) {
  return new MySQLXARecoverSlaveDBScaleNode(this, xa_source, top_source,
                                            ka_update_v);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_purge_connection_pool_node(
    pool_node *pool_info) {
  return new MySQLPurgePoolInfoNode(this, pool_info);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_node(
    dbscale_flush_type type) {
  return new MySQLFlushNode(this, type);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_weak_pwd_node() {
  return new MySQLFlushWeekPwdFileNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_reload_config_node(
    const char *sql) {
  return new MySQLReloadConfigNode(this, sql);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_set_priority_info_node(
    string user_name, int tmp_priority_value) {
  return new MySQLSetPriorityNode(this, user_name, tmp_priority_value);
}
ExecuteNode *MySQLExecutePlan::do_get_show_engine_lock_waiting_node(
    int engine_type) {
  return new MySQLShowEngineLockWaitingNode(this, engine_type);
}
ExecuteNode *MySQLExecutePlan::do_get_show_dataserver_node() {
  return new MySQLDBScaleShowDataServerNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_backend_threads_node() {
  return new MySQLDBScaleShowBackendThreadsNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_partition_scheme_node() {
  return new MySQLDBScaleShowPartitionSchemeNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_schema_node(const char *schema) {
  return new MySQLDBScaleShowSchemaNode(this, schema);
}
ExecuteNode *MySQLExecutePlan::do_get_show_table_node(const char *schema,
                                                      const char *table,
                                                      bool use_like) {
  return new MySQLDBScaleShowTableNode(this, schema, table, use_like);
}
ExecuteNode *MySQLExecutePlan::do_get_migrate_clean_node(
    const char *migrate_id) {
  return new MySQLDBScaleMigrateCleanNode(this, migrate_id);
}
ExecuteNode *MySQLExecutePlan::do_get_show_lock_usage_node() {
  return new MySQLDBScaleShowLockUsageNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_execution_profile_node() {
  return new MySQLDBScaleShowExecutionProfileNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_select_plain_value_node(
    const char *str_value, const char *alias_name) {
  return new MySQLSelectPlainValueNode(this, str_value, alias_name);
}

ExecuteNode *MySQLExecutePlan::do_get_estimate_select_node(
    DataSpace *dataspace, const char *sql, Statement *statement) {
  return new MySQLEstimateSelectNode(this, dataspace, sql, statement);
}

ExecuteNode *MySQLExecutePlan::do_get_estimate_select_partition_node(
    vector<unsigned int> *par_ids, PartitionedTable *par_table, const char *sql,
    Statement *statement) {
  return new MySQLEstimateSelectNode(this, par_ids, par_table, sql, statement);
}

ExecuteNode *MySQLExecutePlan::do_get_rename_table_node(
    PartitionedTable *old_table, PartitionedTable *new_table,
    const char *old_schema_name, const char *old_table_name,
    const char *new_schema_name, const char *new_table_name) {
  return new MySQLRenameTableNode(this, old_table, new_table, old_schema_name,
                                  old_table_name, new_schema_name,
                                  new_table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_mul_modify_node(
    map<DataSpace *, const char *> &spaces_map,
    map<DataSpace *, int> *sql_count_map, bool need_handle_more_result) {
  return new MySQLMulModifyNode(this, spaces_map, sql_count_map,
                                need_handle_more_result);
}

ExecuteNode *MySQLExecutePlan::do_get_show_partition_node() {
  return new MySQLDBScaleShowPartitionNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_show_user_sql_count_node(
    const char *user_id) {
  return new MySQLDBScaleShowUserSqlCountNode(this, user_id);
}

ExecuteNode *MySQLExecutePlan::do_get_show_status_node() {
  return new MySQLDBScaleShowStatusNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_catchup_node(
    const char *source_name) {
  return new MySQLShowCatchupNode(this, source_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_skip_wait_catchup_node(
    const char *source_name) {
  return new MySQLSkipWaitCatchupNode(this, source_name);
}
ExecuteNode *MySQLExecutePlan::do_get_show_async_task_node() {
  return new MySQLDBScaleShowAsyncTaskNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_clean_temp_table_cache_node() {
  return new MySQLDBScaleCleanTempTableCacheNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_explain_node(
    bool is_server_explain_stmt_always_extended) {
  return new MySQLDBScaleExplainNode(this,
                                     is_server_explain_stmt_always_extended);
}

ExecuteNode *MySQLExecutePlan::do_get_fetch_explain_result_node(
    list<list<string> > *result, bool is_server_explain_stmt_always_extended) {
  return new MySQLFetchExplainResultNode(
      this, result, is_server_explain_stmt_always_extended);
}

ExecuteNode *MySQLExecutePlan::do_get_show_virtual_map_node(
    PartitionedTable *tab) {
  return new MySQLDBScaleShowVirtualMapNode(this, tab);
}

ExecuteNode *MySQLExecutePlan::do_get_show_shard_map_node(
    PartitionedTable *tab) {
  return new MySQLDBScaleShowShardMapNode(this, tab);
}

ExecuteNode *MySQLExecutePlan::do_get_show_auto_increment_info_node(
    PartitionedTable *tab) {
  return new MySQLDBScaleShowAutoIncInfoNode(this, tab);
}

ExecuteNode *MySQLExecutePlan::do_get_set_auto_increment_offset_node(
    PartitionedTable *tab) {
  return new MySQLDBScaleSetAutoIncrementOffsetNode(this, tab);
}

ExecuteNode *MySQLExecutePlan::do_get_show_transaction_sqls_node() {
  return new MySQLDBScaleShowTransactionSqlsNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_set_node(DataSpace *dataspace,
                                               const char *sql) {
  return new MySQLSetNode(this, dataspace, sql);
}

ExecuteNode *MySQLExecutePlan::do_get_create_oracle_sequence_node(
    create_oracle_seq_op_node *oper) {
  return new MySQLCreateOracleSequenceNode(this, oper);
}

ExecuteNode *MySQLExecutePlan::do_get_union_all_node(
    vector<ExecuteNode *> &nodes) {
  return new MySQLUnionAllNode(this, nodes);
}

ExecuteNode *MySQLExecutePlan::do_get_union_group_node(
    vector<list<ExecuteNode *> > &nodes) {
  return new MySQLUnionGroupNode(this, nodes);
}

ExecuteNode *MySQLExecutePlan::do_get_keepmaster_node() {
  return new MySQLKeepmasterNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_test_node() {
  return new MySQLDBScaleTestNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_async_task_control_node(
    unsigned long long id) {
  return new MySQLAsyncTaskControlNode(this, id);
}

ExecuteNode *MySQLExecutePlan::do_get_load_dataspace_file_node(
    const char *filename) {
  return new MySQLLoadDataSpaceFileNode(this, filename);
}

ExecuteNode *MySQLExecutePlan::do_get_call_sp_return_ok_node() {
  return new MySQLCallSPReturnOKNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_purge_monitor_point_node() {
  return new MySQLDBScalePurgeMonitorPointNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_clean_monitor_point_node() {
  return new MySQLDBScaleCleanMonitorPointNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_send_dbscale_one_column_node(
    bool only_one_row) {
  return new MySQLSendOneColumnToDBScaleNode(this, only_one_row);
}
ExecuteNode *MySQLExecutePlan::do_get_send_dbscale_mul_column_node() {
  return new MySQLSendMulColumnToDBScaleNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_send_dbscale_one_column_aggr_node(
    bool get_min) {
  return new MySQLSendOneColumnAggrToDBScaleNode(this, get_min);
}

ExecuteNode *MySQLExecutePlan::do_get_send_dbscale_exists_node() {
  return new MySQLSendExistsToDBScaleNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_default_session_var_node() {
  return new MySQLDBScaleShowDefaultSessionVarNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_version_node() {
  return new MySQLDBScaleShowVersionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_version_node() {
  return new MySQLShowVersionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_components_version_node() {
  return new ShowComponentsVersionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_hostname_node() {
  return new MySQLDBScaleShowHostnameNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_all_fail_transaction_node() {
  return new MySQLDBScaleShowAllFailTransactionNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_detail_fail_transaction_node(
    const char *xid) {
  return new MySQLDBScaleShowDetailFailTransactionNode(this, xid);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_partition_table_status_node(
    const char *table_name) {
  return new MySQLDBScaleShowPartitionTableStatusNode(this, table_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_schema_acl_info_node(
    bool is_show_all) {
  return new MySQLDBScaleShowSchemaACLInfoNode(this, is_show_all);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_table_acl_info_node(
    bool is_show_all) {
  return new MySQLDBScaleShowTableACLInfoNode(this, is_show_all);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_path_info() {
  return new MySQLDBScaleShowPathInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_warnings_node() {
  return new MySQLDBScaleShowWarningsNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_critical_errors_node() {
  return new MySQLDBScaleShowCriticalErrorNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_clean_fail_transaction_node(
    const char *xid) {
  return new MySQLDBScaleCleanFailTransactionNode(this, xid);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_base_status_node() {
  return new MySQLDBScaleShowBaseStatusNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_mul_sync_node(
    const char *sync_topic, const char *sync_state, const char *sync_param,
    const char *sync_cond, unsigned long version_id) {
  return new MySQLDBScaleMulSyncNode(this, sync_topic, sync_state, sync_param,
                                     sync_cond, version_id);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_add_default_session_var_node(
    DataSpace *dataspace) {
  return new MySQLDBScaleAddDefaultSessionVarNode(this, dataspace);
}
ExecuteNode *
MySQLExecutePlan::do_get_dbscale_remove_default_session_var_node() {
  return new MySQLDBScaleRemoveDefaultSessionVarNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_federated_table_node() {
  return new MySQLFederatedTableNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_empty_set_node(int field_num) {
  return new MySQLEmptySetNode(this, field_num);
}
ExecuteNode *MySQLExecutePlan::do_get_cursor_direct_node(DataSpace *dataspace,
                                                         const char *sql) {
  return new MySQLCursorDirectNode(this, dataspace, sql);
}
ExecuteNode *MySQLExecutePlan::do_get_cursor_send_node() {
  return new MySQLCursorSendNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_return_ok_node() {
  return new MySQLReturnOKNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_slave_dbscale_error_node() {
  return new MySQLSlaveDBScaleErrorNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_reset_zoo_info_node() {
  return new MySQLResetZooInfoNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_set_info_schema_mirror_tb_status_node(
    info_mirror_tb_status_node *tb_status) {
  return new MySQLSetInfoSchemaMirrorTbStatusNode(this, tb_status);
}

ExecuteNode *MySQLExecutePlan::do_get_forward_master_role_node(
    const char *sql, bool is_slow_query) {
  return new MySQLMultipleMasterForwardNode(this, sql, is_slow_query);
}
#ifndef CLOSE_ZEROMQ
ExecuteNode *MySQLExecutePlan::do_get_message_service_node() {
  return new MySQLMessageServiceNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_binlog_task_node() {
  return new MySQLBinlogTaskNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_task_node() {
  return new MySQLTaskNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_binlog_task_add_filter_node() {
  return new MySQLBinlogTaskAddFilterNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_drop_task_filter_node() {
  return new MySQLDropTaskFilterNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_drop_task_node() {
  return new MySQLDropTaskNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_show_client_task_status_node(
    const char *task_name, const char *server_task_name) {
  return new MySQLShowClientTaskStatusNode(this, task_name, server_task_name);
}
ExecuteNode *MySQLExecutePlan::do_get_show_server_task_status_node(
    const char *task_name) {
  return new MySQLShowServerTaskStatusNode(this, task_name);
}
#endif

ExecuteNode *MySQLExecutePlan::do_get_dbscale_update_audit_user_node(
    const char *username, bool is_add) {
  return new MySQLDBScaleUpdateAuditUserNode(this, username, is_add);
}

ExecuteNode *MySQLExecutePlan::do_get_connect_by_node(
    ConnectByDesc connect_by_desc) {
  return new MySQLConnectByNode(this, connect_by_desc);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_create_oracle_seq_node(
    const char *seq_schema, const char *seq_name) {
  return new MySQLDBScaleShowCreateOracleSeqPlan(this, seq_schema, seq_name);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_seq_status_node(
    const char *seq_schema, const char *seq_name) {
  return new MySQLDBScaleShowSeqStatusPlan(this, seq_schema, seq_name);
}
ExecuteNode *
MySQLExecutePlan::do_get_dbscale_show_fetchnode_buffer_usage_node() {
  return new MySQLDBScaleShowFetchNodeBufferNode(this);
}
ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_trx_block_info_node(
    bool is_local) {
  return new MySQLDBScaleShowTrxBlockInfoNode(this, is_local);
}
ExecuteNode *MySQLExecutePlan::do_get_restore_recycle_table_precheck_node(
    const char *from_schema, const char *from_table, const char *to_table,
    const char *recycle_type, DataSpace *dspace) {
  return new MySQLRestoreRecycleTablePrecheckNode(
      this, from_schema, from_table, to_table, recycle_type, dspace);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_internal_set_node() {
  return new MySQLDBScaleInternalSetNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_empty_node() {
  return new MySQLEmptyNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_show_prepare_cache_node() {
  return new MySQLDBScaleShowPrepareCacheNode(this);
}

ExecuteNode *MySQLExecutePlan::do_get_dbscale_flush_prepare_cache_hit_node() {
  return new MySQLDBScaleFlushPrepareCacheHitNode(this);
}

bool MySQLExecutePlan::has_duplicate_entry_in_error_packet(Packet *packet) {
  if (packet) {
    MySQLErrorResponse error(packet);
    error.unpack();
    if (error.get_error_code() == 1062) return true;
  }
  return false;
}

bool MySQLExecutePlan::check_select_error_not_rollback_in_packet(
    Packet *packet) {
  bool should_rollback = false;
  // get_migrate_tool not null, means part table select error.
  if (get_migrate_tool() != NULL) {
    should_rollback = get_migrate_tool()->check_select_error_not_rollback();
    return should_rollback;
  }
  // directexecutenode select error
  if (packet) {
    MySQLErrorResponse error(packet);
    error.unpack();
    if (error.get_error_code() == ER_LOCK_WAIT_TIMEOUT) {
      LOG_DEBUG(
          "check_select_error_not_rollback_in_packet get error code "
          "ER_LOCK_WAIT_TIMEOUT.\n");
      InnodbRollbackOnTimeout single_server_innodb_rollback_on_timeout =
          get_single_server_innodb_rollback_on_timeout();
#ifndef DBSCALE_TEST_DISABLE
      Backend *bk = Backend::instance();
      dbscale_test_info *test_info = bk->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "rollback_error") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "innodb_rollback_on_timeout_on")) {
        LOG_DEBUG("Test transaction with innodb_rollback_on_timeout on.\n");
        should_rollback = true;
      } else {
        should_rollback = single_server_innodb_rollback_on_timeout ==
                          INNODB_ROLLBACK_ON_TIMEOUT_ON;
      }
#else
      should_rollback = single_server_innodb_rollback_on_timeout ==
                        INNODB_ROLLBACK_ON_TIMEOUT_ON;
#endif
    } else if (error.get_error_code() == ER_LOCK_DEADLOCK ||
               error.is_shutdown()) {
      LOG_DEBUG(
          "check_select_error_not_rollback_in_packet find dead lock or lost "
          "connection.\n");
      should_rollback = true;  // deadlock and lost connection must rollback
    }
    if (!should_rollback) {
      LOG_DEBUG(
          "check_select_error_not_rollback_in_packet check should_rollback is "
          "false.\n");
      string error_message = string(error.get_error_message());
      error_message.append(
          ". due to innodb_rollback_on_timeout_on is off. this transaction not "
          "restart.");
      error.set_error_message(error_message.c_str());
      error.pack(packet);
    }
  }
  return should_rollback;
}

void MySQLExecutePlan::do_send_error_packet() {
  if (is_dispatch_federated()) {
    ((MySQLDispatchPacketNode *)start_node)->dispatch_error_packet();
  } else {
    Packet *packet;
    packet = ((MySQLExecuteNode *)start_node)->get_error_packet();
    if (packet) {
      MySQLErrorResponse error(packet);
      error.unpack();
      LOG_INFO(
          "get an error packet, %d (%s) %s, and try to send it to client.\n",
          error.get_error_code(), error.get_sqlstate(),
          error.get_error_message());
      session->acquire_has_send_client_error_packet_mutex();
      if (session->get_has_send_client_error_packet()) {
        session->release_has_send_client_error_packet_mutex();
        LOG_DEBUG(
            "Session has send the error packet to client, so ignore this error "
            "packet.\n");
        return;
      }
      session->set_has_send_client_error_packet();
      session->release_has_send_client_error_packet_mutex();
      session->set_cur_stmt_error_code(error.get_error_code());
      if ((error.get_error_code() == ERROR_AUTH_DENIED_CODE) &&
          !strcmp(error.get_sqlstate(), "42000")) {
        string msg = error.get_error_message();
        if ((msg.find("Access denied for user") != string::npos) &&
            (msg.find("to database 'information_schema'") != string::npos) &&
            !lower_case_compare(session->get_schema(), default_login_schema)) {
          msg.append(". do you forget to use database?");
          error.set_error_message(msg.c_str());
          Packet pkt;
          error.pack(&pkt);
          handler->send_to_client(&pkt);
          return;
        }
      }
      handler->set_mul_stmt_send_error_packet(true);
      handler->send_to_client(packet);
    } else {
      if (is_send_cluster_xa_transaction_ddl_error_packet()) {
        string err_message =
            "XAER_RMFAIL: The command cannot be executed when global "
            "transaction is in the  ";
        err_message.append(session
                               ->cluster_xa_transaction_to_string(
                                   session->get_cluster_xa_transaction_state())
                               .c_str());
        err_message.append(" state");
        Packet error_packet;
        MySQLErrorResponse error(ERROR_XAER_RMFAIL_CODE, err_message.c_str(),
                                 "XAE07");
        error.pack(&error_packet);
        session->acquire_has_send_client_error_packet_mutex();
        if (session->get_has_send_client_error_packet()) {
          session->release_has_send_client_error_packet_mutex();
          LOG_DEBUG(
              "Session has send the error packet to client, so ignore this "
              "error "
              "packet.\n");
          return;
        }
        session->set_has_send_client_error_packet();
        session->release_has_send_client_error_packet_mutex();
        session->set_cur_stmt_error_code(error.get_error_code());
        handler->send_to_client(&error_packet);
        set_send_cluster_xa_transaction_ddl_error_packet(false);
        return;
      }
      LOG_ERROR(
          "MySQLExecutePlan::do_send_error_packet fail to find an error "
          "packet.\n");
      throw Error("Get an error packet.");
    }
  }
}

void MySQLExecutePlan::do_fullfil_error_info(unsigned int *error_code,
                                             string *sql_state) {
  Packet *packet;
  packet = ((MySQLExecuteNode *)start_node)->get_error_packet();
  if (packet) {
    MySQLErrorResponse response(packet);
    response.unpack();
    *error_code = (unsigned int)response.get_error_code();
    sql_state->clear();
    sql_state->append(response.get_sqlstate());
  }
}

/* class MySQLExecuteNode */

MySQLExecuteNode::MySQLExecuteNode(ExecutePlan *plan, DataSpace *dataspace)
    : ExecuteNode(plan, dataspace) {
  this->driver = (MySQLDriver *)plan->driver;
  this->session = (MySQLSession *)plan->session;
  this->handler = (MySQLHandler *)plan->handler;
  this->name = "MySQLExecuteNode";
  this->profile_id = -1;
  status = EXECUTE_STATUS_START;
  ready_rows = new AllocList<Packet *, Session *, StaticAllocator<Packet *> >();
  ready_rows->set_session(plan->session);
  by_add_pre_disaster_master = false;
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

/* class MySQLClusterXATransactionNode */
MySQLClusterXATransactionNode::MySQLClusterXATransactionNode(ExecutePlan *plan)
    : MySQLExecuteNode(plan) {
  this->name = "MySQLClusterXATransactionNode";
  error_packet = NULL;
  packet = NULL;
  xa_conn = NULL;
  dataspace = NULL;
  got_error = false;
  st_type = plan->statement->get_stmt_node()->type;
  xa_sql = plan->statement->get_sql();
}
void MySQLClusterXATransactionNode::handle_send_client_packet(
    Packet *packet, bool is_row_packet) {
  handler->send_mysql_packet_to_client_by_buffer(packet, is_row_packet);
}
void MySQLClusterXATransactionNode::handle_error_packet(Packet *packet) {
  status = EXECUTE_STATUS_COMPLETE;
  error_packet = packet;
  MySQLErrorResponse response(error_packet);
  response.unpack();
  LOG_DEBUG(
      "MySQLClusterXATransactionNode get an error packet when execute xa_sql "
      "[%s]: %d (%s) "
      "%s\n",
      xa_sql, response.get_error_code(), response.get_sqlstate(),
      response.get_error_message());
  if (response.is_shutdown()) {
    if (xa_conn) {
      handler->clean_dead_conn(&xa_conn, dataspace);
    }
  }
  throw ErrorPacketException();
}

void MySQLClusterXATransactionNode::handle_more_result(Packet *packet) {
  try {
    handler->receive_from_server(xa_conn, packet);
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        handle_error_packet(packet);
      }
      handle_send_client_packet(packet, false);

      handler->receive_from_server(xa_conn, packet);
    }
    handle_send_client_packet(packet, false);
    MySQLEOFResponse eof(packet);
    eof.unpack();
    // receive rows and last eof
    handler->receive_from_server(xa_conn, packet);
#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "client_exception") &&
        !strcasecmp(test_info->test_case_operation.c_str(),
                    "client_exception")) {
      delete packet;
      packet = Backend::instance()->get_new_packet(DEFAULT_PACKET_SIZE,
                                                   session->get_packet_alloc());
      LOG_DEBUG("test after delete packet, then ClientBroken\n");
      throw ClientBroken();
    }
#endif
    while ((!driver->is_eof_packet(packet)) &&
           (!driver->is_error_packet(packet))) {
      handle_send_client_packet(packet, true);
      handler->receive_from_server(xa_conn, packet);
    }
    if (driver->is_error_packet(packet)) handle_error_packet(packet);
    if (driver->is_eof_packet(packet)) {
      if (support_show_warning)
        handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                          xa_conn);
    }
#ifndef DBSCALE_TEST_DISABLE
    handler->handler_last_send.start_timing();
#endif
    handle_send_client_packet(packet, false);
    handler->flush_net_buffer();
#ifndef DBSCALE_TEST_DISABLE
    handler->handler_last_send.end_timing_and_report();
#endif
  } catch (...) {
    this->packet = packet;
    throw;
  }

  this->packet = packet;
}

void MySQLClusterXATransactionNode::execute() {
  Backend *backend = Backend::instance();
  string xid_str = "";
  size_t start_pos = 0;
  size_t end_pos = 0;
  dataspace = backend->get_catalog();

  if (session->get_session_state() == SESSION_STATE_WORKING) {
#ifdef DEBUG
    node_start_timing();
#endif
    LOG_DEBUG("MySQLClusterXATransactionNode sql : %s .\n", xa_sql);

    packet = backend->get_new_packet(DEFAULT_PACKET_SIZE, NULL);
    Packet exec_packet(DEFAULT_PACKET_SIZE, NULL);

    try {
      Packet *real_exec_packet = NULL;
      MySQLQueryRequest query(xa_sql);
      query.set_sql_replace_char(
          plan->session->get_query_sql_replace_null_char());
      query.pack(&exec_packet);
      real_exec_packet = &exec_packet;

#ifndef DBSCALE_TEST_DISABLE
      handler->send_op.start_timing();
#endif

      xa_conn = handler->send_to_server_retry(dataspace, real_exec_packet,
                                              session->get_schema(), false);

      if (!xa_conn) {
        LOG_ERROR("fail to get connections to execute xa command.\n");
        throw Error("fail to get connections to execute xa command");
      } else {
        xa_conn->set_cluster_xa_session(session);
      }
#ifndef DBSCALE_TEST_DISABLE
      handler->send_op.end_timing_and_report();
#endif
    } catch (ExecuteNodeError &e) {
      got_error = true;
      if (xa_conn) {
        handler->clean_dead_conn(&xa_conn, dataspace);
      }
      status = EXECUTE_STATUS_COMPLETE;
      throw e;
    } catch (exception &e) {
      status = EXECUTE_STATUS_COMPLETE;
      LOG_ERROR("MySQLClusterXATransactionNode fail due to exception [%s].\n",
                e.what());
      string error_message(
          "MySQLClusterXATransactionNode fail due to exception:");
      error_message.append(e.what());
      got_error = true;
      if (xa_conn) {
        handler->clean_dead_conn(&xa_conn, dataspace);
      }
      throw ExecuteNodeError(error_message.c_str());
    }
  }
  /*receive packet*/
  try {
    bool has_more_result = false;

    do {
      handler->receive_from_server(xa_conn, packet);
      if (driver->is_error_packet(packet)) {
        LOG_DEBUG("MySQLClusterXATransactionNode : receive error packet.\n");
        has_more_result = false;
        handle_error_packet(packet);
      } else if (driver->is_ok_packet(packet)) {
        MySQLOKResponse ok(packet);
        ok.unpack();
        has_more_result = ok.has_more_result();
        LOG_DEBUG("MySQLClusterXATransactionNode : receive ok packet.\n");
        handler->send_to_client(packet);
        if (support_show_warning)
          handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                            xa_conn);
      } else if (driver->is_result_set_header_packet(packet)) {
        LOG_DEBUG(
            "MySQLClusterXATransactionNode : receive result_set packet.\n");
        handle_send_client_packet(packet, false);
        handle_more_result(packet);
        has_more_result = false;
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // should not be here
#endif
        handle_send_client_packet(packet, false);
        has_more_result = false;
      }
#ifdef DEBUG
      LOG_DEBUG("Has more result %d.\n", has_more_result ? 1 : 0);
#endif
    } while (has_more_result);
    switch (st_type) {
      case STMT_XA_START_TRANSACTION:
        start_pos =
            plan->statement->get_stmt_node()->xid_item->xid_start_pos - 1;
        end_pos = plan->statement->get_stmt_node()->xid_item->xid_end_pos;
        xid_str = string(xa_sql, start_pos, end_pos - start_pos);
        LOG_DEBUG(
            "start_pos is %u, end_pos is %u, xa start transaction with xid "
            "%s.\n",
            start_pos, end_pos, xid_str.c_str());
        session->set_session_cluster_xid(xid_str);
        session->set_cluster_xa_transaction_conn(xa_conn);
        session->set_cluster_xa_transaction_state(SESSION_XA_TRX_XA_ACTIVE);
        /* after receive STMT_XA_START_TRANSACTION,
         * we should set xa session.*/
        xa_conn->set_cluster_xa_session(session);
        break;
      case STMT_XA_END_TRANSACTION:
        session->set_cluster_xa_transaction_state(SESSION_XA_TRX_XA_IDLE);
        break;
      case STMT_XA_PREPARE_TRANSACTION:
        session->set_cluster_xa_transaction_state(SESSION_XA_TRX_XA_PREPARED);
        break;
      case STMT_XA_COMMIT_TRANSACTION:
      case STMT_XA_COMMIT_ONE_PHASE_TRANSACTION:
      case STMT_XA_ROLLBACK_TRANSACTION:
        session->reset_cluster_xa_transaction();
        handler->put_back_connection(dataspace, xa_conn);
        xa_conn = NULL;
        break;
      default:
        break;
    }
  } catch (ErrorPacketException &e) {
    LOG_DEBUG(
        "MySQLClusterXATransactionNode get an error packet when execute xa_sql "
        "[%s].\n",
        xa_sql);
    status = EXECUTE_STATUS_COMPLETE;
    if (xa_conn && error_packet) {
      handler->put_back_connection(dataspace, xa_conn);
      xa_conn = NULL;
    }
    throw e;
  } catch (ExecuteNodeError &e) {
    got_error = true;
    if (xa_conn) {
      handler->clean_dead_conn(&xa_conn, dataspace);
    }
    status = EXECUTE_STATUS_COMPLETE;
    throw e;
  } catch (exception &e) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR("MySQLClusterXATransactionNode fail due to exception [%s].\n",
              e.what());
    string error_message(
        "MySQLClusterXATransactionNode fail due to exception:");
    error_message.append(e.what());
    got_error = true;
    if (xa_conn) {
      handler->clean_dead_conn(&xa_conn, dataspace);
    }
    throw ExecuteNodeError(error_message.c_str());
  }

#ifdef DEBUG
  node_end_timing();
  LOG_DEBUG("MySQLClusterXATransactionNode %@ cost %d ms\n", this,
            node_cost_time);
#endif
  status = EXECUTE_STATUS_COMPLETE;
  return;
}
void MySQLClusterXATransactionNode::clean() {
  if (xa_conn) {
    if (!got_error || error_packet) {
      handler->put_back_connection(dataspace, xa_conn);
      xa_conn = NULL;
    } else {
      handler->clean_dead_conn(&xa_conn, dataspace);
    }
  }
  if (error_packet && error_packet != packet) {
    delete error_packet;
    error_packet = NULL;
  }
  if (packet) {
    delete packet;
    packet = NULL;
    error_packet = NULL;
  }
}

/* class MySQLLockNode */
MySQLLockNode::MySQLLockNode(
    ExecutePlan *plan,
    map<DataSpace *, list<lock_table_item *> *> *lock_table_list)
    : MySQLExecuteNode(plan), conn(NULL) {
  this->lock_table_list = lock_table_list;
  this->name = "MySQLLockNode";
  got_error = false;
  error_packet = NULL;
}
void MySQLLockNode::clean() {
  if (lock_table_list) {
    map<DataSpace *, list<lock_table_item *> *>::iterator it;
    while (!lock_table_list->empty()) {
      it = lock_table_list->begin();
      list<lock_table_item *> *table_list = it->second;
      lock_table_list->erase(it);
      table_list->clear();
      delete table_list;
    }
    delete lock_table_list;
    lock_table_list = NULL;
  }
  if (conn) {
    if (!got_error || error_packet) {
      handler->put_back_connection(dataspace, conn);
    } else {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn = NULL;
  }
  if (packet) {
    delete packet;
    packet = NULL;
    error_packet = NULL;
  }
}

string MySQLLockNode::adjust_table_name(string ori_name) {
  string ret;
  vector<string> split_name;
  boost::split(split_name, ori_name, boost::is_any_of("."));
  if (split_name.size() == 1) {
    ret.assign("`");
    ret.append(split_name.at(0));
    ret.append("`");
  } else if (split_name.size() == 2) {
    ret.assign("`");
    ret.append(split_name.at(0));
    ret.append("`.`");
    ret.append(split_name.at(1));
    ret.append("`");
  }
  return ret;
}

string MySQLLockNode::build_sql(DataSpace *space) {
  list<lock_table_item *> *table_list = (*lock_table_list)[space];
  list<lock_table_item *>::iterator lock_it;
  string str = "lock tables ";
  for (lock_it = table_list->begin(); lock_it != table_list->end(); lock_it++) {
    if (lock_it != table_list->begin()) {
      str += ",";
    }
    str += adjust_table_name((*lock_it)->name);
    str += " ";
    if ((*lock_it)->type == LOCK_TYPE_READ) {
      str += "read";
    } else {
      str += "write";
    }
  }
  LOG_DEBUG("lock sql [%s] for space %@\n", str.c_str(), space);
  lock_table_list->erase(space);
  table_list->clear();
  delete table_list;
  return str;
}

void MySQLLockNode::execute() {
#ifdef DEBUG
  node_start_timing();
#endif
  packet = Backend::instance()->get_new_packet();
  Packet exec_packet;
  MySQLQueryRequest query;
  query.set_sql_replace_char(plan->session->get_query_sql_replace_null_char());
  map<DataSpace *, list<lock_table_item *> *>::iterator it;
  DataSpace *space = NULL;
  while (!lock_table_list->empty()) {
    it = lock_table_list->begin();
    space = it->first;
    string str = build_sql(space);
    const char *sql = (const char *)str.c_str();
    query.set_query(sql);
    query.pack(&exec_packet);

    try {
      conn = handler->send_to_server_retry(
          space, &exec_packet, session->get_schema(), session->is_read_only());
      LOG_DEBUG("Receiving lock result from server\n");
      handler->receive_from_server(conn, packet);
      if (driver->is_error_packet(packet)) {
        status = EXECUTE_STATUS_COMPLETE;
        error_packet = packet;
        MySQLErrorResponse response(error_packet);
        if (response.is_shutdown()) {
          if (conn) {
            handler->clean_dead_conn(&conn, space);
          }
          session->set_server_shutdown(true);
          dataspace = space;
        }
        throw ErrorPacketException();
      }
      if (lock_table_list->size() == 0) {
        handler->record_affected_rows(packet);
        handler->send_to_client(packet);
      }

      if (conn) {
        handler->put_back_connection(space, conn);
        conn = NULL;
      }
    } catch (ErrorPacketException &e) {
      LOG_DEBUG("MySQLLockNode get an error packet when execute sql [%s].\n",
                sql);
      throw e;
    } catch (ExecuteNodeError &e) {
      if (conn) {
        handler->clean_dead_conn(&conn, space);
      }
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      throw e;
    } catch (exception &e) {
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      LOG_DEBUG("MySQLLockNode fail due to exception [%s].\n", e.what());
      string error_message("MySQLLockNode fail due to exception:");
      error_message.append(e.what());
      if (conn) {
        handler->clean_dead_conn(&conn, space);
      }
      throw ExecuteNodeError(error_message.c_str());
      ;
    }
  }
  status = EXECUTE_STATUS_COMPLETE;
  delete lock_table_list;
  lock_table_list = NULL;
#ifdef DEBUG
  node_end_timing();
#endif
#ifdef DEBUG
  LOG_DEBUG("MySQLLockNode %@ cost %d ms\n", this, node_cost_time);
#endif
}

/* class  MySQLTransactionUNLockNode */

MySQLTransactionUNLockNode::MySQLTransactionUNLockNode(ExecutePlan *plan,
                                                       const char *sql,
                                                       bool is_send_to_client)
    : MySQLExecuteNode(plan), conn(NULL) {
  this->name = "MySQLTransactionUNLockNode";
  this->sql = sql;
  this->is_send_to_client = is_send_to_client;
  got_error = false;
  packet = NULL;
  error_packet = NULL;
}

MySQLTransactionUNLockNode::~MySQLTransactionUNLockNode() {}

void MySQLTransactionUNLockNode::clean() {
  session->clear_running_xa_map();
  if (conn) {
    if (!got_error || error_packet) {
      handler->put_back_connection(dataspace, conn);
    } else {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn = NULL;
  }
  if (!handler->is_keep_conn()) {
    session->add_back_kept_cross_join_tmp_table();
    map<DataSpace *, Connection *> kept_connections =
        session->get_kept_connections();
    if (!kept_connections.empty()) {
      map<DataSpace *, Connection *>::iterator it;
      for (it = kept_connections.begin(); it != kept_connections.end(); it++) {
        handler->put_back_connection(it->first, it->second);
      }
    }
  }
  if (error_packet && error_packet != packet) {
    delete error_packet;
    error_packet = NULL;
  }
  if (packet) {
    delete packet;
    packet = NULL;
    error_packet = NULL;
  }
  if (optimize_xa_non_modified_sql) session->clean_xa_modified_conn_map();
  // session->clear_auto_increment_delete_values();
  LOG_DEBUG("Finish clean\n");
}

void MySQLTransactionUNLockNode::deal_savepoint() {
  // savepoint is not support for xa transaction
  // savepoint is not support for cluster xa transaction
  if ((!enable_xa_transaction ||
       session->get_session_option("close_session_xa").int_val != 0) &&
      session->check_for_transaction() &&
      !session->is_in_cluster_xa_transaction() &&
      plan->statement->get_stmt_node()->type == STMT_SAVEPOINT) {
    if (is_send_to_client) {
      LOG_DEBUG("deal_savepoint in TransactionUNLockNode %@\n", this);
      session->add_savepoint(
          plan->statement->get_stmt_node()->sql->savepoint_oper->point);
    }
  }
}

void MySQLTransactionUNLockNode::handle_if_no_kept_conn() {
  status = EXECUTE_STATUS_COMPLETE;
  deal_savepoint();
  if (!is_send_to_client || session->is_call_store_procedure()) return;

  bool has_dead_conn = false;
  vector<conn_result>::iterator it = commit_conn_result.begin();
  for (; it != commit_conn_result.end(); it++) {
    if (it->is_dead_conn) {
      has_dead_conn = true;
      break;
    }
  }

  if (has_dead_conn)
    throw ExecuteNodeError(
        "Get exception for commit with dead server connection.");

  send_ok_packet_to_client(handler, 0, 0);
  return;
}

void MySQLTransactionUNLockNode::handle_if_not_in_transaction() {
  status = EXECUTE_STATUS_COMPLETE;
  if (!is_send_to_client || session->is_call_store_procedure()) return;

  send_ok_packet_to_client(handler, 0, 0);
  return;
}

void MySQLTransactionUNLockNode::report_trans_end_for_consistence_point() {
  if (session->get_need_wait_for_consistence_point()) {
    session->end_commit_consistence_transaction();
  }
}
bool MySQLTransactionUNLockNode::execute_send_part() {
  map<DataSpace *, Connection *> kept_connections =
      session->get_kept_connections();
  if (kept_connections.empty()) {
    handle_if_no_kept_conn();
    return true;
  }
  if (!session->is_in_transaction() &&
      plan->statement->get_stmt_node()->type != STMT_UNLOCK_TB) {
    handle_if_not_in_transaction();
    return true;
  }

  stmt_type st_type = plan->statement->get_stmt_node()->type;
  if (st_type == STMT_COMMIT) {
    if (session->get_need_wait_for_consistence_point()) {
      session->start_commit_consistence_transaction();
    }
  }

  MySQLQueryRequest query(sql);
  query.set_sql_replace_char(plan->session->get_query_sql_replace_null_char());
  Packet exec_packet;
  query.pack(&exec_packet);

  map<DataSpace *, Connection *>::iterator it2;
  for (it2 = kept_connections.begin(); it2 != kept_connections.end();) {
    dataspace = it2->first;
    try {
#ifndef DBSCALE_TEST_DISABLE
      dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(),
                      "send_to_server_retry") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "throw_errors_commit") &&
          !strcasecmp(dataspace->get_name(), "tbl_bob")) {
        throw ExecuteNodeError("dbscale test execute node error.");
      }
#endif
      conn = handler->send_to_server_retry(dataspace, &exec_packet, "", false);
      it2++;
    } catch (...) {
      LOG_ERROR("TransactionUNLockNode fail send due to exception.\n");
      LOG_ERROR("Error dataspace name is [%s]\n", dataspace->get_name());
      list<string> *transaction_sqls = session->get_transaction_sqls();
      if (transaction_sqls->size()) {
        string error_massage("All SQLs related to this transaction are:\n");
        list<string>::iterator it_sql = transaction_sqls->begin();
        for (; it_sql != transaction_sqls->end(); it_sql++) {
          error_massage.append("SQL: ");
          error_massage.append(*it_sql);
          error_massage.append("\n");
        }
        LOG_ERROR("%s\n", error_massage.c_str());
      }
      conn_result con_ret;
      con_ret.is_dead_conn = true;
      con_ret.conn = NULL;
      con_ret.space = it2->first;
      con_ret.packet = NULL;
      commit_conn_result.push_back(con_ret);
      conn = it2->second;
      handler->clean_dead_conn(&conn, it2->first);
      kept_connections.erase(it2++);
    }

    conn = NULL;
  }

  /* Check if we can swap, the function is used in DirectExecutionNode.  If it
   * can swap, the state changes from SESSION_STATE_WORKING =>
   * SESSION_STATE_WAITING_SERVER. Otherwise, session state cahnge from
   * SESSION_STATE_WORKING => SESSION_STATE_HANDLING_RESULT */
  if (session->is_may_backend_exec_swap_able()) {
    map<DataSpace *, Connection *>::iterator it3;
    for (it3 = kept_connections.begin(); it3 != kept_connections.end(); it3++) {
      conn = it3->second;
      if (conn) {
        conn->set_session(session);
        struct event *conn_event = conn->get_conn_socket_event();
        evutil_socket_t conn_socket = conn->get_conn_socket();
        SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
        ACE_ASSERT(conn_base);
#endif
        session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
        LOG_DEBUG(
            "Finish to prepare the conn socket session swap for conn %@ and "
            "session %@.\n",
            conn, session);
#endif
      }
    }
    conn = NULL;
    session->set_mul_connection_num(kept_connections.size());
    return true;
  }
  return false;
}
void MySQLTransactionUNLockNode::execute() {
#ifdef DEBUG
  node_start_timing();
#endif
  if (enable_cluster_xa_transaction &&
      session->is_in_cluster_xa_transaction()) {
    LOG_ERROR("is in xa transaction, cann't execute transaction sqls.\n");
    string err_message =
        "XAER_RMFAIL: The command cannot be executed when global transaction "
        "is in the  ";
    err_message.append(session
                           ->cluster_xa_transaction_to_string(
                               session->get_cluster_xa_transaction_state())
                           .c_str());
    err_message.append(" state");
    error_packet = Backend::instance()->get_new_packet();
    MySQLErrorResponse error(ERROR_XAER_RMFAIL_CODE, err_message.c_str(),
                             "XAE07");
    error.pack(error_packet);
    throw ErrorPacketException();
  }

  if (close_cross_node_transaction && session->session_modify_mul_server()) {
    LOG_ERROR(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0.\n");
    throw Error(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0.");
  }
  if (enable_xa_transaction &&
      plan->session->get_session_option("close_session_xa").int_val != 0 &&
      session->session_modify_mul_server()) {
    LOG_ERROR(
        "enable-xa-transaction =1 and current session close-session-xa !=0, so "
        "current session should not execute cross node transaction.\n");
    throw Error(
        "enable-xa-transaction =1 and current session close-session-xa !=0, so "
        "current session should not execute cross node transaction.");
  }

  if (session->get_session_state() == SESSION_STATE_WORKING) {
    if (execute_send_part()) return;
  }
  execute_receive_part();
}

bool MySQLTransactionUNLockNode::can_swap() {
  return session->is_may_backend_exec_swap_able();
}
void MySQLTransactionUNLockNode::execute_receive_part() {
  map<DataSpace *, Connection *> kept_connections =
      session->get_kept_connections();
  stmt_type st_type = plan->statement->get_stmt_node()->type;
  packet = Backend::instance()->get_new_packet();
  try {
    map<DataSpace *, Connection *>::iterator it;
    for (it = kept_connections.begin(); it != kept_connections.end(); it++) {
      try {
        conn = it->second;
        handler->receive_from_server(conn, packet);
        if (driver->is_error_packet(packet)) {
          conn_result con_ret;
          con_ret.conn = conn;
          con_ret.space = it->first;
          con_ret.is_dead_conn = false;

          MySQLErrorResponse response(packet);
          if (response.is_shutdown()) {
            if (conn) {
              con_ret.is_dead_conn = true;
            }
            session->set_server_shutdown(true);
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
          }
          Packet *tmp_packet = Backend::instance()->get_new_packet();
          con_ret.packet = packet;
          packet = tmp_packet;
          commit_conn_result.push_back(con_ret);
          conn = NULL;
          continue;
        }
        conn->reset();
        handler->put_back_connection(it->first, it->second);
        conn = NULL;

      } catch (...) {
        LOG_DEBUG("TransactionUNLockNode fail receive due to exception.\n");
        conn_result con_ret;
        con_ret.is_dead_conn = true;
        con_ret.conn = NULL;
        con_ret.space = it->first;
        con_ret.packet = NULL;
        commit_conn_result.push_back(con_ret);
        if (session->defined_lock_need_kept_conn(it->first))
          session->remove_defined_lock_kept_conn(it->first);
        handler->clean_dead_conn(&conn, it->first);
      }
    }
    conn = NULL;

    if (commit_conn_result.empty()) {
      if (is_send_to_client) {
        handler->deal_autocommit_with_ok_eof_packet(packet);
        handler->record_affected_rows(packet);
        if (!session->is_call_store_procedure()) {
#ifdef DEBUG
          LOG_DEBUG("Sending transaction or unlock result to client\n");
#endif
          handler->send_to_client(packet);
        }
        deal_savepoint();
        is_send_to_client = false;
      }
    } else {
      /* If there is dead conn, we ignore all error packet, and throw
       * ExecuteNodeError. Otherwise, we store and use the first
       * error_packet.*/
      bool has_dead_conn = false;
      error_packet = NULL;
      vector<conn_result>::iterator it = commit_conn_result.begin();
      for (; it != commit_conn_result.end(); it++) {
        if (it->is_dead_conn) {
          has_dead_conn = true;
          if (it->conn) handler->clean_dead_conn(&(it->conn), it->space);
          continue;
        }
        if (it->packet) {
          if (error_packet)
            delete it->packet;
          else
            error_packet = it->packet;
          if (it->conn) handler->put_back_connection(it->space, it->conn);
        }
      }

      if (has_dead_conn)
        throw ExecuteNodeError(
            "Get exception for commit with dead server connection.");
      if (error_packet) throw ErrorPacketException();
      throw ExecuteNodeError("Unexpect behavior.");
    }
  } catch (ErrorPacketException &e) {
    LOG_DEBUG(
        "TransactionUNLockNode get an error packet when execute sql [%s].\n",
        sql);
    if (st_type == STMT_COMMIT) report_trans_end_for_consistence_point();
    status = EXECUTE_STATUS_COMPLETE;
    throw e;
  } catch (ExecuteNodeError &e) {
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
      conn = NULL;
    }
    got_error = true;
    if (st_type == STMT_COMMIT) report_trans_end_for_consistence_point();
    status = EXECUTE_STATUS_COMPLETE;
    throw e;
  } catch (exception &e) {
    got_error = true;
    if (st_type == STMT_COMMIT) report_trans_end_for_consistence_point();

    status = EXECUTE_STATUS_COMPLETE;
    LOG_DEBUG("TransactionUNLockNode fail due to exception [%s].\n", e.what());
    string error_message("TransactionUNLockNode fail due to exception:");
    error_message.append(e.what());
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
      /* Set the conn = NULL in case clean this conn again in deconstruct
       * method. */
      conn = NULL;
    }
    throw ExecuteNodeError(error_message.c_str());
    ;
  }
#ifdef DEBUG
  node_end_timing();
#endif
#ifdef DEBUG
  LOG_DEBUG("MySQLTransactionUNLockNode %@ cost %d ms\n", this, node_cost_time);
#endif
  if (st_type == STMT_COMMIT) report_trans_end_for_consistence_point();
  status = EXECUTE_STATUS_COMPLETE;
}

/* class MySQLXATransactionNode */
MySQLXATransactionNode::MySQLXATransactionNode(ExecutePlan *plan,
                                               const char *sql,
                                               bool is_send_to_client)
    : MySQLTransactionUNLockNode(plan, sql, is_send_to_client),
      has_optimized_readonly_conn(false) {
  this->name = "MySQLXATransactionNode";
  st_type = plan->statement->get_stmt_node()->type;
  is_error = false;
  warnings = 0;
  xa_command_type = XA_ROLLBACK;
  fail_num = 0;
  can_swap = false;
  has_swap = false;
  xa_state = XA_INIT_STATE;
  all_kept_conn_optimized = false;
  commit_fail = false;
}

void handle_conn_and_session_for_error(Connection *conn, Session *s,
                                       DataSpace *space) {
  s->get_handler()->clean_dead_conn(&conn, space);
}

int MySQLXATransactionNode::exec_non_xa_for_all_kept_conn(
    map<DataSpace *, Connection *> *kept_connections, DataSpace *dataspace,
    XACommand type, bool is_send_only) {
  try {
    if (is_send_only) {
      map<DataSpace *, Connection *>::iterator it;
      unsigned long xid = session->get_xa_id();
      char tmp[256];
      // TODO: This read-only connection will clean by backend thread in the
      // future #1427
      for (it = kept_connections->begin(); it != kept_connections->end();) {
        if (it->second->get_resource_status() == RESOURCE_STATUS_DEAD) {
          LOG_ERROR("Kept connection has been killed!\n");
          throw HandlerError("Connection has been killed!");
        }

        int len = sprintf(
            tmp, "%lu-%u-%d-%d-%u", xid, it->first->get_virtual_machine_id(),
            it->second->get_group_id(), Backend::instance()->get_cluster_id(),
            it->second->get_thread_id());
        tmp[len] = '\0';
        string sql = "XA END '";
        sql += tmp;
        sql += "'; XA ROLLBACK '";
        sql += tmp;
        sql += "';";
        if (it->first != dataspace) {
          try {
            if (it->second) {
              it->second->execute(sql.c_str(), 2);
              it->second->set_start_xa_conn(false);
            }
            it++;
          } catch (...) {
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
            session->remove_kept_connection(it->first);
            session->remove_using_conn(it->second);
            it->second->get_pool()->add_back_to_dead(it->second);
            it->second = NULL;
            kept_connections->erase(it++);
          }
        } else {
          it++;
        }
      }
      if (dataspace && kept_connections->count(dataspace)) {
        if (type == XA_PREPARE) {
          int len = sprintf(
              tmp, "%lu-%u-%d-%d-%u", xid, dataspace->get_virtual_machine_id(),
              dataspace->get_group_id(), Backend::instance()->get_cluster_id(),
              ((*kept_connections)[dataspace])->get_thread_id());
          tmp[len] = '\0';
          string sql = "XA END '";
          sql += tmp;
          sql += "';XA COMMIT '";
          sql += tmp;
          sql += "' ONE PHASE";
          ((*kept_connections)[dataspace])->execute(sql.c_str(), 2);
          ((*kept_connections)[dataspace])->set_start_xa_conn(false);
        } else if (type == XA_ROLLBACK) {
          string sql = "XA END '";
          sql += tmp;
          sql += "'; XA ROLLBACK '";
          sql += tmp;
          sql += "';";
          ((*kept_connections)[dataspace])->execute(sql.c_str(), 2);
          ((*kept_connections)[dataspace])->set_start_xa_conn(false);
        } else {
          LOG_ERROR(
              "XACommand is not XA_PREPARE or XA_ROLLBACK in "
              "MySQLXATransactionNode\n");
          throw HandlerError("XACommand is not XA_PREPARE or XA_ROLLBACK");
        }
      }

      if (can_swap) {
        xa_swap_conns(kept_connections);
        session->set_mul_connection_num(kept_connections->size());
      }
      return 0;
    } else {
      if (dataspace && kept_connections->count(dataspace)) {
        if (((*kept_connections)[dataspace])->get_resource_status() ==
            RESOURCE_STATUS_DEAD) {
          LOG_ERROR("Kept connection has been killed!\n");
          throw HandlerError("Connection has been killed!");
        }

        ((*kept_connections)[dataspace])->handle_client_modify_return();
      }
    }
  } catch (...) {
    if (dataspace && kept_connections->count(dataspace)) {
      if (session->defined_lock_need_kept_conn(dataspace))
        session->remove_defined_lock_kept_conn(dataspace);
      session->remove_kept_connection(dataspace);
      session->remove_using_conn((*kept_connections)[dataspace]);
      ((*kept_connections)[dataspace])
          ->get_pool()
          ->add_back_to_dead(((*kept_connections)[dataspace]));
      ((*kept_connections)[dataspace]) = NULL;
      is_error = true;
    }
    receive_for_non_modify_connection(kept_connections, dataspace);
    return 1;
  }
  receive_for_non_modify_connection(kept_connections, dataspace);
  return 0;
}

void MySQLXATransactionNode::receive_for_non_modify_connection(
    map<DataSpace *, Connection *> *kept_connections, DataSpace *dataspace) {
  map<DataSpace *, Connection *>::iterator it;
  for (it = kept_connections->begin(); it != kept_connections->end(); it++) {
    if (it->first != dataspace) {
      try {
        if (it->second) {
          it->second->handle_client_modify_return();
        }
      } catch (...) {
        if (session->defined_lock_need_kept_conn(it->first))
          session->remove_defined_lock_kept_conn(it->first);
        session->remove_kept_connection(it->first);
        session->remove_using_conn(it->second);
        it->second->get_pool()->add_back_to_dead(it->second);
        it->second = NULL;
      }
    }
  }
}

list<DataSpace *> MySQLXATransactionNode::get_transaction_modified_spaces(
    Session *s) {
  map<DataSpace *, list<string> > *space_map = s->get_redo_logs_map();
  list<DataSpace *> dataspace_list;
  if (space_map == 0) {
    LOG_ERROR("redo log map is empty.\n");
    throw Error("redo log map is empty.");
  }
  map<DataSpace *, list<string> >::iterator it = space_map->begin();
  list<string>::iterator list_it;
  for (; it != space_map->end(); it++) {
    for (list_it = (it->second).begin(); list_it != (it->second).end();
         list_it++) {
      if (is_modify_sql(*list_it)) {
        dataspace_list.push_back(it->first);
        break;
      }
    }
  }
  return dataspace_list;
}

void MySQLXATransactionNode::xa_swap_conns(
    map<DataSpace *, Connection *> *kept_connections) {
  Connection *conn = NULL;
  map<DataSpace *, Connection *>::iterator it3;
  for (it3 = kept_connections->begin(); it3 != kept_connections->end(); it3++) {
    conn = it3->second;
    if (conn) {
      conn->set_session(session);
      struct event *conn_event = conn->get_conn_socket_event();
      evutil_socket_t conn_socket = conn->get_conn_socket();
      SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
      ACE_ASSERT(conn_base);
#endif
      session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
      LOG_DEBUG(
          "Finish to prepare the conn socket session swap for conn %@ and "
          "session %@.\n",
          conn, session);
#endif
      // has_swap = true should only when session has add_server_base
      has_swap = true;
    }
  }
}
void MySQLXATransactionNode::xa_swap_conns(Connection *conn) {
  conn->set_session(session);
  struct event *conn_event = conn->get_conn_socket_event();
  evutil_socket_t conn_socket = conn->get_conn_socket();
  SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
  ACE_ASSERT(conn_base);
#endif
  session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
  LOG_DEBUG(
      "Finish to prepare the conn socket session swap for conn %@ and session "
      "%@.\n",
      conn, session);
#endif
  session->set_mul_connection_num(1);
  has_swap = true;
}

void MySQLXATransactionNode::xa_swap_conns(map<Connection *, bool> *tx_rrlkc) {
  map<Connection *, bool>::iterator tx_it;
  Connection *conn = NULL;
  for (tx_it = tx_rrlkc->begin(); tx_it != tx_rrlkc->end(); tx_it++) {
    conn = tx_it->first;
    if (conn) {
      conn->set_session(session);
      struct event *conn_event = conn->get_conn_socket_event();
      evutil_socket_t conn_socket = conn->get_conn_socket();
      SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
      ACE_ASSERT(conn_base);
#endif
      session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
      LOG_DEBUG(
          "Finish to prepare the conn socket session swap for conn %@ and "
          "session %@.\n",
          conn, session);
#endif
      has_swap = true;
    }
  }
}
int MySQLXATransactionNode::exec_xa_for_all_kept_conn(
    map<DataSpace *, Connection *> *kept_connections, XACommand type,
    bool is_send) {
  int ret = 0, tmp = 0;
  map<DataSpace *, Connection *>::iterator it;
  Driver *driver = Driver::get_driver();
  XA_helper *xa_helper = driver->get_xa_helper();
  unsigned long xid = session->get_xa_id();
  if (type == XA_PREPARE) {
    if (is_send) {
      for (it = kept_connections->begin(); it != kept_connections->end();) {
        tmp = xa_helper->exec_xa_commit_first_phase(session, it->first,
                                                    it->second);
        if (tmp) {
          ret++;
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, true);
          }
          if (tmp == ERR_XA_CONN || r) {
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
            handle_conn_and_session_for_error(it->second, session, it->first);
            char tmp_xid[256];
            int len = sprintf(tmp_xid, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp_xid[len] = '\0';
            Backend::instance()->add_source_need_rollback_xid_machine(
                it->first->get_data_source(), tmp_xid);
          }
          kept_connections->erase(it++);
        } else
          it++;
      }
      if (can_swap) {
        map<Connection *, bool> *tx_rrlkc =
            session->get_tx_record_redo_log_kept_connections();
        xa_swap_conns(tx_rrlkc);
        xa_swap_conns(kept_connections);
        session->set_mul_connection_num(kept_connections->size() +
                                        tx_rrlkc->size());
      }
      return ret;
    } else {
      map<Connection *, bool> *tx_rrlkc =
          session->get_tx_record_redo_log_kept_connections();
      map<Connection *, bool>::iterator tx_it;
      for (tx_it = tx_rrlkc->begin(); tx_it != tx_rrlkc->end(); tx_it++) {
        tmp = xa_helper->handle_after_xa_prepare(session, tx_it->first);
        if (tmp) {
          ret++;
        }
        if (tmp == ERR_XA_CONN) {
          session->set_tx_record_redo_log_connection_to_dead(tx_it->first);
        }
      }
      session->clean_all_tx_record_redo_log_kept_connections();
      for (it = kept_connections->begin(); it != kept_connections->end();) {
        tmp = xa_helper->handle_after_xa_prepare(session, it->second);
        (it->second)->reset();
        if (tmp) {
          ret++;
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, true);
          }
          if (tmp == ERR_XA_CONN || r) {
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
            handle_conn_and_session_for_error(it->second, session, it->first);
            char tmp_xid[256];
            int len = sprintf(tmp_xid, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp_xid[len] = '\0';
            Backend::instance()->add_source_need_rollback_xid_machine(
                it->first->get_data_source(), tmp_xid);
          }
          kept_connections->erase(it++);
        } else
          it++;
      }
      return ret;
    }
  } else if (type == XA_ROLLBACK) {
    if (is_send) {
      for (it = kept_connections->begin(); it != kept_connections->end();) {
        tmp = xa_helper->exec_xa_rollback(session, it->first, it->second);
        if (tmp) {
          // For rollback, we assume that the only possible reason for execution
          // fail is server unavaliable.
          ret++;
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, true);
          }
          if (tmp == ERR_XA_CONN || r) {
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
            handle_conn_and_session_for_error(it->second, session, it->first);
            char tmp_xid[256];
            int len = sprintf(tmp_xid, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp_xid[len] = '\0';
            Backend::instance()->add_source_need_rollback_xid_machine(
                it->first->get_data_source(), tmp_xid);
          }

          kept_connections->erase(it++);
        } else
          it++;
      }
    }
    return ret;
  } else if (type == XA_ROLLBACK_AFTER_PREPARE) {
    if (is_send) {
      for (it = kept_connections->begin(); it != kept_connections->end();) {
        tmp = xa_helper->exec_xa_rollback_after_prepare(session, it->first,
                                                        it->second);
        if (tmp) {
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, true);
          }
          if (r) {
            char tmp[256];
            int len = sprintf(tmp, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp[len] = '\0';
            Backend::instance()->add_source_need_rollback_xid_machine(
                it->first->get_data_source(), tmp);
          }
          // For rollback, we assume that the only possible reason for execution
          // fail is server unavaliable.
          ret++;
          if (session->defined_lock_need_kept_conn(it->first))
            session->remove_defined_lock_kept_conn(it->first);
          handle_conn_and_session_for_error(it->second, session, it->first);
          kept_connections->erase(it++);
        } else
          it++;
      }
    }
    return ret;
  } else if (type == XA_COMMIT) {
    if (is_send) {
#ifndef DBSCALE_TEST_DISABLE
      /*just for test federated_max_rows*/
      Backend *bk = Backend::instance();
      dbscale_test_info *test_info = bk->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "xa_running_test") &&
          !strcasecmp(test_info->test_case_operation.c_str(), "commit_block")) {
        LOG_DEBUG(
            "Do dbscale test operation 'commit_block' for case "
            "'xa_running_test'.\n");
        while (strcasecmp(test_info->test_case_operation.c_str(), "")) {
          ACE_OS::sleep(1);
        }
        LOG_DEBUG(
            "Finish test operation 'commit_block' for case "
            "'xa_running_test'.\n");
      }
#endif

      for (it = kept_connections->begin(); it != kept_connections->end();) {
        tmp = xa_helper->exec_xa_commit_second_phase(session, it->first,
                                                     it->second);
        if (tmp) {
          // For commit, we assume that the only possible reason for execution
          // fail is server unavaliable.
          ret++;
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, false);
          }
          if (r) {
            // the xid machine need commit success, add to backend and wait for
            // recover
            char tmp[256];
            int len = sprintf(tmp, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp[len] = '\0';
            Backend::instance()->add_source_need_recover_xid_machine(
                it->first->get_data_source(), tmp);
            commit_fail = true;
          } else
            handle_xa_success_spaces.push_back(it->first);
          if (session->defined_lock_need_kept_conn(it->first))
            session->remove_defined_lock_kept_conn(it->first);
          handle_conn_and_session_for_error(it->second, session, it->first);
          kept_connections->erase(it++);
        } else
          it++;
      }
      if (can_swap) {
        xa_swap_conns(kept_connections);
        session->set_mul_connection_num(kept_connections->size());
      }
      return ret;
    } else {
      for (it = kept_connections->begin(); it != kept_connections->end();
           it++) {
        tmp = xa_helper->handle_after_xa_commit(session, it->first, it->second);
        if (tmp) {
          // For commit, we assume that the only possible reason for execution
          // fail is server unavaliable.
          ret++;
          int r = 1;
          if (!ACE_Reactor::instance()->reactor_event_loop_done()) {
            r = xa_helper->handle_xa_if_prepared_server_alived(
                session, it->first, it->second, false);
          }
          if (r) {
            // the xid machine need commit success, add to backend and wait
            // for recover
            unsigned long xid = session->get_xa_id();
            char tmp[256];
            int len = sprintf(tmp, "%lu-%u-%d-%d-%u", xid,
                              it->first->get_virtual_machine_id(),
                              it->second->get_group_id(),
                              Backend::instance()->get_cluster_id(),
                              it->second->get_thread_id());
            tmp[len] = '\0';
            Backend::instance()->add_source_need_recover_xid_machine(
                it->first->get_data_source(), tmp);
            commit_fail = true;
          } else
            handle_xa_success_spaces.push_back(it->first);
          if (session->defined_lock_need_kept_conn(it->first))
            session->remove_defined_lock_kept_conn(it->first);
          handle_conn_and_session_for_error(it->second, session, it->first);
        }
      }
      if (!commit_fail) {
        unsigned long xid = session->get_xa_id();
        char tmp_xid[256];
        int len = sprintf(tmp_xid, "%d-%lu",
                          Backend::instance()->get_cluster_id(), xid);
        tmp_xid[len] = '\0';

        // only all connection commit success, then do clean
        for (it = kept_connections->begin(); it != kept_connections->end();
             it++) {
          MySQLXA_purge_thread::instance()->add_xid_to_purge(
              it->first->get_data_source(), tmp_xid);
        }
        vector<DataSpace *>::iterator it2 = handle_xa_success_spaces.begin();
        for (; it2 != handle_xa_success_spaces.end(); it2++) {
          MySQLXA_purge_thread::instance()->add_xid_to_purge(
              (*it2)->get_data_source(), tmp_xid);
        }
      }
      return ret;
    }
  }
  return ret;
}
void MySQLXATransactionNode::change_xa_state(XASwapState state) {
  LOG_DEBUG("xa state change from [%d] to [%d] \n", xa_state, state);
  xa_state = state;
}

bool MySQLXATransactionNode::xa_init() {
  kept_connections = session->get_kept_connections();
  map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
  for (; it != kept_connections.end(); ++it) {
    if (it->first == Backend::instance()->get_auth_data_space()) {
      DataSpace *auth_space = it->first;
      Connection *auth_conn = it->second;
      handle_auth_space_for_xa(auth_space, auth_conn);
      kept_connections.erase(it);
      break;
    }
  }
  if (kept_connections.empty()) {
    handle_if_no_kept_conn();
    return false;
  }
  if (!session->is_in_transaction()) {
    handle_if_not_in_transaction();
    return false;
  }
  if (!session->get_xa_id()) {
    handle_if_not_in_transaction();
    return false;
  }

  if (st_type == STMT_COMMIT) {
    if (session->get_xa_need_wait_for_consistence_point()) {
      session->start_commit_consistence_transaction();
    }
    if (!session->get_has_global_flush_read_lock()) {
      /*In flush tables with read lock mode, one server may has more than one
       * kept conn to execute flush command and hold the global read lock.  IN
       * this case, do xa commit will deadlock. So in this case, we should do
       * xa rollback, which is the default value for xa_command_type.*/
      xa_command_type = XA_PREPARE;
    }
  }

  if (!optimize_xa_non_modified_sql)
    dataspace_list = get_transaction_modified_spaces(session);
  else
    handle_optimize_xa_non_modified_sql();
  can_swap = session->is_may_backend_exec_swap_able();
  change_xa_state(XA_FIRST_PHASE_SEND_STATE);
  return true;
}

void MySQLXATransactionNode::handle_auth_space_for_xa(DataSpace *auth_space,
                                                      Connection *auth_conn) {
  unsigned long xid = session->get_xa_id();
  char tmp[256];
  int len =
      sprintf(tmp, "%lu-%u-%d-%d-%u", xid, auth_space->get_virtual_machine_id(),
              auth_conn->get_group_id(), Backend::instance()->get_cluster_id(),
              auth_conn->get_thread_id());
  tmp[len] = '\0';
  string sql = "XA END '";
  sql += tmp;
  sql += "'; XA ROLLBACK '";
  sql += tmp;
  sql += "';";
  try {
    auth_conn->execute(sql.c_str(), 2);
    auth_conn->handle_client_modify_return();
    auth_conn->set_start_xa_conn(false);
  } catch (...) {
    session->remove_kept_connection(auth_space);
    session->remove_using_conn(auth_conn);
    auth_conn->get_pool()->add_back_to_dead(auth_conn);
  }
}

void MySQLXATransactionNode::handle_optimize_xa_non_modified_sql() {
  if (optimize_xa_non_modified_sql && !has_optimized_readonly_conn &&
      !plan->session->is_in_lock() && plan->session->is_in_transaction()) {
    has_optimized_readonly_conn = true;
    bool contain_readonly_conn = false;
    map<DataSpace *, Connection *>::iterator it;
    for (it = kept_connections.begin(); it != kept_connections.end();) {
      char tmp[256];
      int len = sprintf(
          tmp, "%lu-%u-%d-%d-%u", session->get_xa_id(),
          it->first->get_virtual_machine_id(), it->second->get_group_id(),
          Backend::instance()->get_cluster_id(), it->second->get_thread_id());
      tmp[len] = '\0';
      if (readonly_conn_can_optimize(it->first, it->second, tmp)) {
        kept_connections.erase(it++);
        contain_readonly_conn = true;
        continue;
      }
      it++;
    }
    if (contain_readonly_conn) {
      session->get_status()->item_inc(TIMES_XA_OPTIMIZE_READ_ONLY_STMT);
      for (unsigned int i = 0; i < optimize_xa_non_modified_sql_thread_num;
           ++i) {
        Backend::instance()->get_xa_readonly_conn_handler(i)->signal_cond();
      }
      if (kept_connections.empty()) {
        all_kept_conn_optimized = true;
      }
    }

    dataspace_list.clear();
    map<DataSpace *, Connection *>::iterator map_it;
    for (map_it = kept_connections.begin(); map_it != kept_connections.end();
         map_it++) {
      dataspace_list.push_back(map_it->first);
    }
  }
}

bool MySQLXATransactionNode::readonly_conn_can_optimize(DataSpace *dataspace,
                                                        Connection *conn,
                                                        string xid) {
  // should not put back the read only conn in the lock mode.
  if (!session->get_has_global_flush_read_lock() &&
      !plan->session->is_xa_modified_conn(conn)) {
    // should not put back the read only conn in get_lock.
    if (session->defined_lock_need_kept_conn(dataspace)) return false;
    if (session->remove_kept_connection(dataspace)) {
      session->remove_using_conn(conn);
      // should check whether remove kept_connections success or not
      // Otherwise there are two threads share one connection
      Backend::instance()
          ->get_xa_readonly_conn_handler(
              atoll(xid.c_str()) % optimize_xa_non_modified_sql_thread_num)
          ->add_conn_to_map(conn, xid);
      LOG_DEBUG(
          "in xa transaction, add readonly connection [%@] to backend thread, "
          "this backend thread will send 'XA ROLLBACK' to server\n",
          conn);
      return true;
    }
  }
  return false;
}
void MySQLXATransactionNode::report_xa_trans_end_for_consistence_point() {
  if (session->get_xa_need_wait_for_consistence_point()) {
    session->end_commit_consistence_transaction();
  }
}
void MySQLXATransactionNode::execute() {
  if (close_cross_node_transaction && session->session_modify_mul_server()) {
    LOG_ERROR(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0.\n");
    throw Error(
        "Refuse to execute cross node transaction when "
        "close_cross_node_transaction != 0.");
  }

  if (all_kept_conn_optimized) {
    status = EXECUTE_STATUS_COMPLETE;
    change_xa_state(XA_END_STATE);
  }
  try {
    if (xa_state == XA_INIT_STATE) {
      if (!xa_init()) return;
    }

    if (!all_kept_conn_optimized &&
        (dataspace_list.size() == 1 || dataspace_list.size() == 0)) {
      DataSpace *space = NULL;
      if (dataspace_list.size() == 1) {
        space = dataspace_list.front();
        if (xa_state == XA_FIRST_PHASE_SEND_STATE)
          session->get_status()->item_inc(
              TIMES_XA_OPTIMIZE_ONLY_AFFECT_ONE_SERVER);
      }
      switch (xa_state) {
        case XA_FIRST_PHASE_SEND_STATE: {
          fail_num = exec_non_xa_for_all_kept_conn(&kept_connections, space,
                                                   xa_command_type, true);
          // if get error, do not swap
          if (fail_num) {
            change_xa_state(XA_END_STATE);
            break;
          }
          change_xa_state(XA_FIRST_PHASE_RECV_STATE);
          if (has_swap) {
            has_swap = false;
            return;
          }
        }
        case XA_FIRST_PHASE_RECV_STATE: {
          fail_num = exec_non_xa_for_all_kept_conn(&kept_connections, space,
                                                   xa_command_type, false);
          change_xa_state(XA_END_STATE);
          break;
        }
        default: {
          LOG_ERROR("XATransactionNode get unknown xa_state [%d]\n", xa_state);
#ifdef DEBUG
          ACE_ASSERT(0);
#endif
        }
      }
    } else if (!all_kept_conn_optimized) {
      switch (xa_state) {
        case XA_FIRST_PHASE_SEND_STATE: {
          unsigned long xid = session->get_xa_id();
          session->add_running_xa_map(xid,
                                      xa_command_type == XA_PREPARE
                                          ? RUNNING_XA_PREPARE
                                          : RUNNING_XA_ROLLBACK,
                                      &kept_connections);

          session->record_relate_source_redo_log(&kept_connections);
          fail_num = exec_xa_for_all_kept_conn(&kept_connections,
                                               xa_command_type, true);
          change_xa_state(XA_FIRST_PHASE_RECV_STATE);
          if (has_swap) {
            has_swap = false;
            return;
          }
        }
        case XA_FIRST_PHASE_RECV_STATE: {
          fail_num += exec_xa_for_all_kept_conn(&kept_connections,
                                                xa_command_type, false);
          kept_connections.clear();
          kept_connections = session->get_kept_connections();

          if (xa_command_type == XA_PREPARE) {
            if (fail_num == 0) {
              session->acquire_using_conn_mutex();
              // check be killed before go to the last commit, if this check
              // passed, kill will not terminate this handler during the second
              // phase commit.
              if (handler->be_killed()) {
                session->release_using_conn_mutex();
                throw ThreadIsKilled();
              }
              session->set_xa_prepared();
              session->release_using_conn_mutex();
              xa_command_type = XA_COMMIT;
            } else {
              is_error = true;
              xa_command_type = XA_ROLLBACK_AFTER_PREPARE;
            }
          }
          change_xa_state(XA_SECOND_PHASE_SEND_STATE);
        }
        case XA_SECOND_PHASE_SEND_STATE: {
          unsigned long xid = session->get_xa_id();
          if (session->add_running_xa_map(xid, xa_command_type == XA_COMMIT
                                                   ? RUNNING_XA_COMMIT
                                                   : RUNNING_XA_ROLLBACK)) {
            // this running xa transaction contains fail source conn, so abort
            // finial commit.
            if (xa_command_type == XA_COMMIT)
              xa_command_type = XA_ROLLBACK_AFTER_PREPARE;
            LOG_INFO(
                "Set xa transaction %d to XA_ROLLBACK_AFTER_PREPARE for "
                "session %@.\n",
                xid, session);
          }

          if (xa_command_type == XA_COMMIT ||
              xa_command_type == XA_ROLLBACK_AFTER_PREPARE) {
            if (exec_xa_for_all_kept_conn(&kept_connections, xa_command_type,
                                          true)) {
              if (Backend::instance()->get_backend_server_version() ==
                      MYSQL_VERSION_57 ||
                  Backend::instance()->get_backend_server_version() ==
                      MYSQL_VERSION_8) {
                warnings++;
                WarnInfo *w = new WarnInfo(
                    WARNING_COMMIT_DELAY_SUCCESS_CODE,
                    dbscale_err_msg[WARNING_COMMIT_DELAY_SUCCESS_CODE]);
                session->add_warning_info(w);
              } else {
                is_error = true;
              }
            }
          }
          change_xa_state(XA_SECOND_PHASE_RECV_STATE);
          if (has_swap) {
            has_swap = false;
            return;
          }
        }
        case XA_SECOND_PHASE_RECV_STATE: {
          if (xa_command_type == XA_COMMIT ||
              xa_command_type == XA_ROLLBACK_AFTER_PREPARE) {
            if (exec_xa_for_all_kept_conn(&kept_connections, xa_command_type,
                                          false)) {
              if (Backend::instance()->get_backend_server_version() ==
                      MYSQL_VERSION_57 ||
                  Backend::instance()->get_backend_server_version() ==
                      MYSQL_VERSION_8) {
                warnings++;
                WarnInfo *w = new WarnInfo(
                    WARNING_COMMIT_DELAY_SUCCESS_CODE,
                    dbscale_err_msg[WARNING_COMMIT_DELAY_SUCCESS_CODE]);
                session->add_warning_info(w);
              } else {
                is_error = true;
              }
            }
          }
          change_xa_state(XA_END_STATE);
          break;
        }
        default: {
          LOG_ERROR("XATransactionNode get unknown xa_state [%d]\n", xa_state);
#ifdef DEBUG
          ACE_ASSERT(0);
#endif
        }
      }
    }
    if (!session->get_has_global_flush_read_lock()) {
      // In global read lock mode, should not add conn back to free
      session->remove_all_connection_to_free();
    }

    if (is_send_to_client) {
      try {
        // TODO: if commit failed, block the failed source.
        if (is_error) {
          packet = NULL;
          if (xa_command_type == XA_COMMIT) {
            packet = handler->get_error_packet(
                ERROR_XA_EXECUTE_FAIL_CODE,
                "Get error during final transaction commit phase.", NULL);
          } else {
            packet = handler->get_error_packet(
                ERROR_XA_EXECUTE_FAIL_CODE, "Fail to end the XA transaction!",
                NULL);
          }
          if (!session->is_call_store_procedure()) {
            handler->send_to_client(packet);
            delete packet;
            packet = NULL;
            session->set_has_send_client_error_packet();
          }
          error_packet = packet;
          if (session->is_call_store_procedure())
            throw ErrorPacketException();
          else
            throw ExecuteNodeError(
                "MySQLXATransactionNode::execute get error.");
        } else {
          if (!session->is_call_store_procedure()) {
            send_ok_packet_to_client(handler, 0, warnings);
          }
        }
      } catch (...) {
        status = EXECUTE_STATUS_COMPLETE;
        throw;
      }

      is_send_to_client = false;
    }

    status = EXECUTE_STATUS_COMPLETE;
    if (st_type == STMT_COMMIT) report_xa_trans_end_for_consistence_point();

  } catch (Exception &e) {
    status = EXECUTE_STATUS_COMPLETE;
    if (st_type == STMT_COMMIT) report_xa_trans_end_for_consistence_point();

    LOG_ERROR("XATransactionNode fail due to Exception [%s].\n", e.what());
    throw;
  } catch (exception &e) {
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    if (st_type == STMT_COMMIT) report_xa_trans_end_for_consistence_point();

    LOG_ERROR("XATransactionNode fail due to exception [%s].\n", e.what());
    string error_message("XATransactionNode fail due to exception:");
    error_message.append(e.what());
    throw ExecuteNodeError(error_message.c_str());
    ;
  }
}

/* class  MySQLDirectExecuteNode */

MySQLDirectExecuteNode::MySQLDirectExecuteNode(ExecutePlan *plan,
                                               DataSpace *dataspace,
                                               const char *sql)
    : MySQLExecuteNode(plan, dataspace),
      sql(sql),
      conn(NULL),
      got_error(false),
      packet(NULL),
      error_packet(NULL) {
  this->name = "MySQLDirectExecuteNode";
  stmt_insert_like = is_stmt_insert_like(plan);
  select_uservar_flag = statement->get_select_uservar_flag();
  select_field_num = statement->get_select_field_num();
  if (select_uservar_flag) {
    uservar_vec = *(statement->get_select_uservar_vec());
  }
  field_num = 0;
  federated_max_rows = (uint64_t)(
      session->get_session_option("max_federated_cross_join_rows").ulong_val);
  cross_join_max_rows = (uint64_t)(
      session->get_session_option("max_cross_join_moved_rows").ulong_val);
  direct_prepare_exec_packet = false;
  has_change_user = false;
  is_show_full_fields = false;
  is_show_full_fields_via_mirror_tb = false;
  is_direct_node = true;
  kept_lock_new_connection = false;
  defined_lock_field_num = 0;

#ifndef DBSCALE_TEST_DISABLE
  /*just for test federated_max_rows*/
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "federated_max_rows") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "abort")) {
    federated_max_rows = 1;
  }
#endif
}

bool MySQLDirectExecuteNode::is_stmt_insert_like(ExecutePlan *plan) {
  return plan->statement->get_stmt_node()->table_list_head &&
         (plan->statement->get_stmt_node()->type == STMT_INSERT_SELECT ||
          plan->statement->get_stmt_node()->type == STMT_INSERT ||
          plan->statement->get_stmt_node()->type == STMT_REPLACE);
}

void MySQLDirectExecuteNode::clean() {
  if (conn && !(plan->get_is_parse_transparent() &&
                (session->is_in_transaction() ||
                 session->is_in_cluster_xa_transaction()))) {
    Backend *backend = Backend::instance();
    stmt_type type = plan->statement->get_stmt_node()->type;
    if (plan->get_is_parse_transparent() ||
        (has_change_user &&
         (type == STMT_GRANT || type == STMT_SET_PASSWORD ||
          type == STMT_DROP_USER || type == STMT_CREATE_USER ||
          type == STMT_SHOW_DATABASES || type == STMT_SHOW_TABLES ||
          type == STMT_SHOW_GRANTS || type == STMT_REVOKE ||
          type == STMT_ALTER_USER ||
          (type == STMT_SELECT &&
           dataspace == backend->get_metadata_data_space() &&
           datasource_in_one)))) {
      bool add_back_to_dead = true;
      if (!got_error) {
        try {
          conn->set_session(session);
          const char *schema_name = NULL;
          if (session->is_drop_current_schema())
            schema_name = default_login_schema;
          else
            schema_name = session->get_schema();
          if (conn->change_user(admin_user, admin_password, NULL,
                                schema_name)) {
            conn->set_changed_user_conn(false);
            map<string, string> *session_var_map =
                session->get_session_var_map();
            conn->set_session_var(session_var_map, false, has_change_user);
            add_back_to_dead = false;
          }
          session->remove_using_conn(conn);
        } catch (exception &e) {
          session->remove_using_conn(conn);
          LOG_ERROR("When change user back to admin user with error:%s\n",
                    e.what());
        }
      }
      if (add_back_to_dead) {
        handler->clean_dead_conn(&conn, dataspace, false);
      } else {
        handler->put_back_connection(dataspace, conn);
      }
    } else {
      if (!got_error || error_packet) {
        handler->put_back_connection(dataspace, conn);
      } else {
        handler->clean_dead_conn(&conn, dataspace);
      }
    }
    conn = NULL;
  }
  if (packet) {
    delete packet;
    packet = NULL;
    error_packet = NULL;
  }
  if (session->get_work_session())
    session->get_work_session()->increase_finished_federated_num();

  if (session->is_drop_current_schema()) {
    session->set_schema(NULL);
  }
  if (plan->get_need_destroy_dataspace_after_exec()) {
    LOG_DEBUG("destroy dataspace named [%s] in DirectExecutionNode clean.\n",
              dataspace->get_name());
    delete dataspace;
    dataspace = NULL;
  }
}
void MySQLDirectExecuteNode::update_column_display_name(Packet *packet,
                                                        string field_str) {
  MySQLColumnResponse col_resp(packet);
  col_resp.unpack();
  col_resp.set_column(field_str.c_str());
  col_resp.pack(packet);
}

void MySQLDirectExecuteNode::prepare_work(bool &need_check_last_insert_id,
                                          int64_t &before_insert_id) {
  int enable_last_insert_id_session =
      session->get_session_option("enable_last_insert_id").int_val;
  stmt_node *st = plan->statement->get_stmt_node();
  if (enable_last_insert_id_session && stmt_insert_like) {
    // prepare the auto_incrment info and old last insert id value
    AutoIncStatus inc_status = plan->statement->get_auto_inc_status();
    need_check_last_insert_id = inc_status != NO_AUTO_INC_FIELD;
    if ((dataspace && dataspace->get_dataspace_type() == PARTITION_TYPE) ||
        ((dataspace && dataspace->get_dataspace_type() == TABLE_TYPE &&
          dataspace->is_partitioned()))) {
      need_check_last_insert_id = false;
    } else if (st->last_insert_id_num) {
      need_check_last_insert_id = true;
    }
    stmt_type cur_type = plan->statement->get_stmt_node()->type;
    if (plan->get_is_parse_transparent() && is_stmt_type_of_dml(cur_type)) {
      need_check_last_insert_id = true;
    }
    if (need_check_last_insert_id) {
      before_insert_id = handler->get_session()->get_last_insert_id();
      handler->get_last_insert_id_from_server(dataspace, &conn,
                                              before_insert_id, true);
    }
  } else if (enable_last_insert_id_session) {
    if (st->last_insert_id_num) {
      if (dataspace && dataspace->get_dataspace_type() == PARTITION_TYPE) {
        LOG_ERROR(
            "Not support last_insert_id for non insert partition table.\n");
        throw Error(
            "Not support last insert id for non insert partition table.");
      }
      need_check_last_insert_id = true;
    }
    if (need_check_last_insert_id) {
      before_insert_id = handler->get_session()->get_last_insert_id();
      handler->get_last_insert_id_from_server(dataspace, &conn,
                                              before_insert_id, true);
    }
  }
}

void MySQLDirectExecuteNode::handle_set_last_insert_id(
    bool need_check_last_insert_id, int64_t &before_insert_id) {
  int enable_last_insert_id_session =
      session->get_session_option("enable_last_insert_id").int_val;
  if (enable_last_insert_id_session && need_check_last_insert_id && conn) {
    int64_t after_insert_id = 0;
    try {
      after_insert_id =
          handler->get_last_insert_id_from_server(dataspace, &conn);
      int64_t session_before_insert_id =
          handler->get_session()->get_last_insert_id();
      LOG_DEBUG("session before:%B, before:%B, after:%B.\n",
                session_before_insert_id, before_insert_id, after_insert_id);
      if ((session_before_insert_id != after_insert_id)) {
        handler->get_session()->set_last_insert_id(after_insert_id);
      }
    } catch (exception &e) {
      LOG_DEBUG("Update session last insert id with error:%s", e.what());
    }
  }
}
bool MySQLDirectExecuteNode::handle_ok_packet(Packet *packet,
                                              bool need_check_last_insert_id,
                                              int64_t &before_insert_id,
                                              bool &is_non_modified_conn) {
  bool has_more_result = false;
  MySQLOKResponse ok(packet);
  ok.unpack();
  uint64_t affect_rows = ok.get_affected_rows();
  if (affect_rows) is_non_modified_conn = false;
  has_more_result = ok.has_more_result();
#ifdef DEBUG
  LOG_DEBUG("affected rows =%d, has_more_result = %d\n", affect_rows,
            has_more_result ? 1 : 0);
#endif
  if (!has_more_result) {
    // we only record the affected rows info if the ok packet is the last
    // packet
    session->set_affected_rows(affect_rows);
  }

  int enable_last_insert_id_session =
      session->get_session_option("enable_last_insert_id").int_val;
  if (enable_last_insert_id_session && need_check_last_insert_id) {
    int64_t after_insert_id = 0;
    after_insert_id = handler->get_last_insert_id_from_server(dataspace, &conn);
    int64_t session_before_insert_id =
        handler->get_session()->get_last_insert_id();
    LOG_DEBUG("session before:%d, before:%d, after:%d.\n",
              session_before_insert_id, before_insert_id, after_insert_id);
    if ((session_before_insert_id != after_insert_id)) {
      handler->get_session()->set_last_insert_id(after_insert_id);
      // if has more result set, we may get a new last insert id later
      if (has_more_result) before_insert_id = after_insert_id;
    }
  }

  stmt_type type = plan->statement->get_stmt_node()->type;
  if (type == STMT_CHANGE_DB) {
    const char *schema_name =
        plan->statement->get_stmt_node()->sql->change_db_oper->dbname->name;
    conn->set_schema(schema_name);
    session->set_schema(schema_name);
  }
  if (type == STMT_CREATE_VIEW || type == STMT_DROP_VIEW)
    plan->statement->handle_create_or_drop_view(plan);
  if (type == STMT_CREATE_EVENT || type == STMT_DROP_EVENT) {
    if (enable_event) plan->statement->handle_create_or_drop_event();
  }
  if (type == STMT_GRANT || type == STMT_DROP_USER ||
      type == STMT_CREATE_USER || type == STMT_SHOW_DATABASES ||
      type == STMT_SHOW_TABLES || type == STMT_SET_PASSWORD ||
      type == STMT_REVOKE || type == STMT_ALTER_USER)
    execute_grant_sqls();
  if ((type == STMT_CREATE_TB || type == STMT_CREATE_SELECT ||
       type == STMT_CREATE_LIKE) &&
      plan->statement->get_stmt_node()->sql->create_tb_oper->op_tmp == 1) {
    if (!session->is_dataspace_keeping_connection(dataspace) &&
        session->get_space_from_kept_space_map(dataspace) == NULL) {
      session->set_kept_connection(dataspace, conn);
    }
    join_node *table =
        plan->statement->get_stmt_node()->sql->create_tb_oper->table;
    const char *schema_name =
        table->schema_name ? table->schema_name : plan->statement->get_schema();
    const char *table_name = table->table_name;
    conn->inc_tmp_tb_num(schema_name, table_name);
    plan->session->add_temp_table_set(schema_name, table_name, conn);
    plan->statement->get_stmt_node()->is_create_or_drop_temp_table = true;
  } else if (type == STMT_DROP_TB) {
    table_link *table = plan->statement->get_stmt_node()->table_list_head;
    while (table) {
      const char *schema_name = table->join->schema_name
                                    ? table->join->schema_name
                                    : plan->statement->get_schema();
      const char *table_name = table->join->table_name;
      if (conn->dec_tmp_tb_num(schema_name, table_name)) {
        plan->session->remove_temp_table_set(schema_name, table_name);
        plan->statement->get_stmt_node()->is_create_or_drop_temp_table = true;
      }
      table = table->next;
    }
  } else if (type == STMT_ALTER_TABLE) {
    table_link *table = plan->statement->get_stmt_node()->table_list_head;
    const char *schema_name = table->join->schema_name
                                  ? table->join->schema_name
                                  : plan->statement->get_schema();
    const char *table_name = table->join->table_name;
    if (plan->session->is_temp_table(schema_name, table_name))
      plan->statement->get_stmt_node()->is_create_or_drop_temp_table = true;
  } else if (type == STMT_DROP_USER) {
    // drop plainpwd from backend map
    string tmp_username = plan->statement->get_stmt_node()
                              ->sql->drop_user_oper->user_clause->user_name;
    Backend::instance()->drop_sha2_plainpwd(tmp_username);
  }
  return has_more_result;
}

void MySQLDirectExecuteNode::handle_send_client_packet(Packet *packet,
                                                       bool is_row_packet) {
  handler->send_mysql_packet_to_client_by_buffer(packet, is_row_packet);
}

void MySQLDirectExecuteNode::handle_send_field_packet(Packet *packet) {
  handle_send_client_packet(packet, false);
}

void MySQLDirectExecuteNode::handle_send_last_eof_packet(Packet *packet) {
  handle_send_client_packet(packet, false);
}

bool MySQLDirectExecuteNode::handle_result_set(Packet *packet) {
  bool has_more_result = false;
  try {
    uint64_t row_num = 0;

    handler->receive_from_server(conn, packet);

    vector<MySQLColumnType> v_ctype;
    if (session->is_binary_resultset() && !direct_prepare_exec_packet &&
        is_direct_node) {
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
    while (!driver->is_eof_packet(packet)) {
      if (driver->is_error_packet(packet)) {
        handle_error_packet(packet);
      }

      if (session->is_binary_resultset() && !direct_prepare_exec_packet &&
          is_direct_node) {
        if (!v_ctype.empty() && v_ctype.size() > pos) {
          MySQLColumnType prepare_type = v_ctype[pos];
          MySQLColumnResponse col_resp(packet);
          col_resp.unpack();
          MySQLColumnType col_type = col_resp.get_column_type();
          if (col_type != prepare_type) {
            col_resp.set_column_type(prepare_type);
            col_resp.pack(packet);
          }
          LOG_DEBUG(
              "Adjust the column %s from type %d to type %d for binary result "
              "convert.\n",
              col_resp.get_column(), int(col_type), int(prepare_type));
        }
      }

      if (plan->statement->has_sql_schema() &&
          dataspace->get_dataspace_type() == PARTITION_TYPE &&
          dataspace->get_virtual_machine_id() != 0) {
        MySQLColumnResponse col_resp(packet);
        col_resp.unpack();
        LOG_DEBUG("reset field packet schema name from [%s] to [%s]\n",
                  col_resp.get_schema(), plan->statement->get_sql_schema());
        col_resp.set_schema(plan->statement->get_sql_schema());
        col_resp.pack(packet);
      }
      if (get_kept_defined_lock_conn()) {
        MySQLFieldListColumnResponse response(packet);
        response.unpack();
        if (!strcasecmp(defined_lock_name.c_str(), response.get_column()))
          defined_lock_field_num = field_num;
        response.pack(packet);
      }
      handle_send_field_packet(packet);
      field_num++;

      handler->receive_from_server(conn, packet);
      pos++;
    }
    handler->deal_autocommit_with_ok_eof_packet(packet);
    handle_send_client_packet(packet, false);
    MySQLEOFResponse eof(packet);
    eof.unpack();
    // receive rows and last eof
    handler->receive_from_server(conn, packet);
#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "client_exception") &&
        !strcasecmp(test_info->test_case_operation.c_str(),
                    "client_exception")) {
      delete packet;
      packet = Backend::instance()->get_new_packet(DEFAULT_PACKET_SIZE,
                                                   session->get_packet_alloc());
      LOG_DEBUG("test after delete packet, then ClientBroken\n");
      throw ClientBroken();
    }
#endif
    if (is_show_full_fields_via_mirror_tb && driver->is_eof_packet(packet)) {
      LOG_DEBUG(
          "via mirror table to show full fields, got empty set, reset it as "
          "error packet\n");
      uint8_t num = driver->get_packet_number(packet);
      delete packet;
      string err_msg = "Table '";
      err_msg.append(show_full_fields_schema_name);
      err_msg.append(".");
      err_msg.append(show_full_fields_table_name);
      err_msg.append("' doesn't exist (via mirror table)");
      MySQLErrorResponse err_resp(1146, err_msg.c_str(), "42S02", num);
      bool can_swap = session->is_may_backend_exec_swap_able();
      packet = Backend::instance()->get_new_packet(
          DEFAULT_PACKET_SIZE, can_swap ? session->get_packet_alloc() : NULL);
      err_resp.pack(packet);
#ifndef DBSCALE_TEST_DISABLE
      handler->handler_last_send.start_timing();
#endif
      handle_send_client_packet(packet, false);
      handler->flush_net_buffer();
#ifndef DBSCALE_TEST_DISABLE
      handler->handler_last_send.end_timing_and_report();
#endif
      has_more_result = false;
    } else {
      // need_refactor_show_variables_packet only use for sgrdb
      // when execute sql `show variables like "%validate%"`
      stmt_type type = plan->statement->get_stmt_node()->type;
      bool need_refactor_show_variables_packet = false;
#ifdef IS_GRID_VERSION
      bool need_refactor_select_version_comment = false;
#endif
      if (type == STMT_SHOW_VARIABLES && need_validate_password) {
        string show_sql(plan->statement->get_sql());
        boost::to_upper(show_sql);
        if (show_sql.find("LIKE") != string::npos &&
            show_sql.find("VALIDATE") != string::npos) {
          need_refactor_show_variables_packet = true;
        }
      }
#ifdef IS_GRID_VERSION
      else if (Backend::instance()->is_grid() && type == STMT_SELECT) {
        /*
          when login grid
          will send LOGIN_SELECT_VERSION_COMMENT sql result to client
          we will stop this result to client
          and send custom version comment to client
        */
        const char *sql = plan->statement->get_sql();
        size_t len = strlen(sql);
        if (len == LOGIN_SELECT_VERSION_COMMENT_LEN) {
          if (strcmp(sql, LOGIN_SELECT_VERSION_COMMENT) == 0) {
            need_refactor_select_version_comment = true;
          }
        }
      }
#endif
      while ((!driver->is_eof_packet(packet)) &&
             (!driver->is_error_packet(packet))) {
        row_num++;
        if (plan->is_federated() && row_num > federated_max_rows) {
          status = EXECUTE_STATUS_COMPLETE;
          got_error = true;
          LOG_ERROR(
              "Reach the max rows of [%u] for federated middle result set.\n",
              row_num);
          throw ExecuteNodeError(
              "Reach the max row number of federated middle result set.");
        }
        if (plan->statement->is_cross_node_join() &&
            row_num > cross_join_max_rows) {
          status = EXECUTE_STATUS_COMPLETE;
          got_error = true;
          LOG_ERROR(
              "Reach the max rows of [%u] for cross node join max moved "
              "rows.\n",
              row_num);
          throw ExecuteNodeError(
              "Reach the max row number of cross node join max moved rows.");
        }
        if (session->is_binary_resultset() && !direct_prepare_exec_packet &&
            is_direct_node) {
          MySQLRowResponse row_resp(packet);
          if (session->get_prepare_item(session->get_execute_stmt_id())) {
            list<MySQLColumnType> *column_type_list =
                session->get_prepare_item(session->get_execute_stmt_id())
                    ->get_column_type_list();
            row_resp.convert_to_binary(
                column_type_list,
                handler->get_convert_result_to_binary_stringstream(), &packet);
          } else {
            LOG_ERROR("Failed to get prepare item for execute #%d.\n",
                      session->get_execute_stmt_id());
            throw ExecuteCommandFail("Cannot get prepare item.");
          }
        }
#ifdef IS_GRID_VERSION
        if (need_refactor_select_version_comment) {
          handler->receive_from_server(conn, packet);
          continue;
        }
#endif
        if (need_refactor_show_variables_packet) {
          MySQLRowResponse row_res(packet);
          uint64_t str_len = 0;
          const char *column_str = row_res.get_str(0, &str_len);
          string var_name(column_str, str_len);
          if (var_name.find("validate_password") != string::npos) {
            handler->receive_from_server(conn, packet);
            continue;
          }
        }
        if (plan->statement->get_is_first_column_dbscale_row_id()) {
          rebuild_packet_first_column_dbscale_row_id(packet, row_num,
                                                     field_num);
        }
        if (get_kept_defined_lock_conn()) {
          defined_lock_type dl_type =
              plan->statement->get_stmt_node()->defined_lock;
          if (DEFINED_RELEASE_ALL_LOCKS == dl_type) {
            session->reset_defined_lock_depth();
          } else {
            MySQLRowResponse response(packet);
            response.unpack();
            if (!response.field_is_null(defined_lock_field_num)) {
              uint64_t field_len = 0;
              string column_str =
                  response.get_str(defined_lock_field_num, &field_len);
              column_str = column_str.substr(0, field_len);
              if (!strcasecmp(column_str.c_str(), "1")) {
                if (DEFINED_GET_LOCK == dl_type) {
                  session->defined_lock_depth_add();
                } else if (DEFINED_RELEASE_LOCK == dl_type) {
                  session->defined_lock_depth_sub();
                }
              }
            }
          }
        }
        handle_send_client_packet(packet, true);
        handler->receive_from_server(conn, packet);
      }

      if (plan->session->is_call_store_procedure() ||
          plan->session->get_has_more_result()) {
        rebuild_eof_with_has_more_flag(packet, driver);
        LOG_DEBUG(
            "For the call store procedure or multiple stmt, the last eof "
            "should be"
            " with flag has_more_result in direct node.\n");
      }

      if (driver->is_error_packet(packet)) {
        has_more_result = false;
        handle_error_packet(packet);
      }
      if (driver->is_eof_packet(packet)) {
        if (need_refactor_show_variables_packet) {
          Packet new_packet;
          list<const char *> row_data;
          char value[64];

          row_data.push_back("validate_password");
          sprintf(value, "%d", need_validate_password);
          row_data.push_back(value);
          MySQLRowResponse need_validate_row(row_data);
          need_validate_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_length");
          sprintf(value, "%d", validate_password_length);
          row_data.push_back(value);
          MySQLRowResponse validate_len_row(row_data);
          validate_len_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_mixed_case_count");
          sprintf(value, "%d", validate_password_mixed_case_count);
          row_data.push_back(value);
          MySQLRowResponse validate_case_row(row_data);
          validate_case_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_number_count");
          sprintf(value, "%d", validate_password_number_count);
          row_data.push_back(value);
          MySQLRowResponse validate_num_row(row_data);
          validate_num_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_special_char_count");
          sprintf(value, "%d", validate_password_special_char_count);
          row_data.push_back(value);
          MySQLRowResponse validate_special_row(row_data);
          validate_special_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_policy");
          sprintf(value, "%s", validate_password_policy.c_str());
          row_data.push_back(value);
          MySQLRowResponse validate_policy_row(row_data);
          validate_policy_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);

          row_data.clear();
          new_packet.rewind();
          row_data.push_back("validate_password_dictionary_file");
          sprintf(value, "%s", validate_password_dictionary_file.c_str());
          row_data.push_back(value);
          MySQLRowResponse validate_weak_dict_row(row_data);
          validate_weak_dict_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);
        }
#ifdef IS_GRID_VERSION
        else if (need_refactor_select_version_comment) {
          Packet new_packet;
          list<const char *> row_data;
          string everdb_version = "";
          Backend::instance()->get_everdb_powerby_version_with_prefix(
              everdb_version, EVERDB_VERSION_LOGIN_VERSION_PREFIX);

          row_data.push_back(everdb_version.c_str());
          MySQLRowResponse need_validate_row(row_data);
          need_validate_row.pack(&new_packet);
          handle_send_client_packet(&new_packet, true);
        }
#endif

        if (support_show_warning)
          handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                            conn);
      }
#ifndef DBSCALE_TEST_DISABLE
      handler->handler_last_send.start_timing();
#endif
      handler->deal_autocommit_with_ok_eof_packet(packet);
      handle_send_last_eof_packet(packet);
      handler->flush_net_buffer();
#ifndef DBSCALE_TEST_DISABLE
      handler->handler_last_send.end_timing_and_report();
#endif
      if (plan->session->get_has_more_result()) {
        has_more_result = false;
      } else if (!plan->session->is_call_store_procedure() ||
                 plan->statement->is_call_store_procedure_directly()) {
        eof.set_packet(packet);
        eof.unpack();
        has_more_result = eof.has_more_result();
      } else
        has_more_result = false;
      if (plan->statement->get_stmt_node()->type == STMT_SELECT) {
        if (conn) {
          if (has_sql_calc_found_rows()) {
            session->set_found_rows(0);
            found_rows(handler, driver, conn, session);
            Packet *packet_tmp = session->get_error_packet();
            if (packet_tmp) {
              handle_error_packet(packet_tmp);
            }
          } else {
            session->set_found_rows(row_num);
          }
        }
        session->set_real_fetched_rows(row_num);
      }
    }
  } catch (...) {
    this->packet = packet;
    throw;
  }
  /* Make direct node's packet point to the new packet */
  this->packet = packet;
  return has_more_result;
}

void MySQLDirectExecuteNode::handle_error_packet(Packet *packet) {
  status = EXECUTE_STATUS_COMPLETE;
  error_packet = packet;
  MySQLErrorResponse response(error_packet);
  response.unpack();
  LOG_DEBUG(
      "DirectExecuteNode get an error packet when execute sql [%s]: %d (%s) "
      "%s\n",
      sql, response.get_error_code(), response.get_sqlstate(),
      response.get_error_message());
  if (response.is_shutdown()) {
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    session->set_server_shutdown(true);
  }
  if (handle_tokudb_lock_timeout(conn, packet)) {
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
  }
  throw ErrorPacketException();
}

bool check_server_pool_dead_conn_too_frequency(DataSpace *space) {
  if (space) return false;
  DataSource *source = space->get_data_source();
  if (!source) return false;
  DataServer *server = source->get_master_server();
  if (!server) return false;
  unsigned int dead_count_one_min = server->get_pool_dead_count_min();
  if (dead_count_one_min > MAX_DEAD_CONN_PER_MIN) {
    LOG_INFO(
        "Find dead_count_one_min to be %d, larger than MAX_DEAD_CONN_PER_MIN "
        "%d for server %s.\n",
        dead_count_one_min, MAX_DEAD_CONN_PER_MIN, server->get_name());
    return true;
  }
  return false;
}

Connection *MySQLDirectExecuteNode::connect_with_cur_user(stmt_type type) {
  string user_host = handler->get_user_host();
  unsigned int pos = user_host.find("@");
  user = user_host.substr(0, pos);

  if ((!strcmp(user.c_str(), supreme_admin_user) ||
       !strcmp(user.c_str(), normal_admin_user) ||
       !strcmp(user.c_str(), dbscale_internal_user))) {
    LOG_DEBUG(
        "This is a root/dbscale/dbscale_internal user, no need to change user "
        "any more.\n");
    return NULL;
  }
  if (type == STMT_SELECT && Backend::instance()->is_mirror_user(user)) {
    LOG_DEBUG("This is a mirror user, skip change user.\n");
    return NULL;
  }

  if (check_server_pool_dead_conn_too_frequency(dataspace)) {
    LOG_ERROR(
        "Too frequence vist auth source, skip change user, plz do 'insert into "
        "dbscale.mirror_user values('%s'); dbscale reload mirror user;'",
        user.c_str());
    return NULL;
  }
  host = user_host.substr(pos + 1, user_host.length() - pos - 1);
  try {
    conn = dataspace->get_connection(session);
    if (!conn) {
      got_error = true;
      return NULL;
    }
    if (session->get_cur_auth_plugin() == CACHING_SHA2_PASSWORD) {
      password = "";
    } else {
      conn->get_password(password, user.c_str(), host.c_str(),
                         Backend::instance()->get_backend_server_version());
    }
    conn->set_session(session);
    if (conn->change_user(user.c_str(), NULL, password.c_str(),
                          session->get_schema())) {
      session->insert_using_conn(conn);
      conn->set_changed_user_conn(true);
    } else {
      conn->get_pool()->add_back_to_dead(conn);
      conn = NULL;
    }
  } catch (HandlerError &e_handler) {
    got_error = true;
    throw;
  } catch (exception &e) {
    LOG_ERROR("change user to %s with error:%s\n", user_host.c_str(), e.what());
    if (conn) {
      conn->get_pool()->add_back_to_dead(conn);
    }
    conn = NULL;
  }
  return conn;
}
bool MySQLDirectExecuteNode::deal_with_com_prepare(
    bool read_only, MySQLExecuteRequest &exec_req,
    MySQLPrepareItem *prepare_item) {
  Packet tmp_packet;
  handler->receive_from_server(conn, &tmp_packet);
  if (driver->is_error_packet(&tmp_packet)) {
    MySQLErrorResponse error(&tmp_packet);
    error.unpack();
    LOG_ERROR("command prepare execute failed: %d (%s) %s\n",
              error.get_error_code(), error.get_sqlstate(),
              error.get_error_message());
    LOG_ERROR("Got error packet when execute MYSQL_COM_EXECUTE.\n");
    // if success return true
    return false;
  } else {
    MySQLPrepareResponse prep_resp(&tmp_packet);
    prep_resp.unpack();
    if (read_only) {
      prepare_item->set_r_stmt_id(prep_resp.statement_id);
    } else {
      prepare_item->set_w_stmt_id(prep_resp.statement_id);
    }
    if (!conn->inc_prepare_num(prep_resp.statement_id)) {
      LOG_ERROR(
          "There is no prepare statement in connection [%@] when execute drop "
          "prepare statement.\n",
          conn);
      throw HandlerError("prepare_num has overflowed.");
    }
    if (prep_resp.num_params > 0) {
      handler->receive_from_server(conn, &tmp_packet);
      while (!driver->is_eof_packet(&tmp_packet)) {
        handler->receive_from_server(conn, &tmp_packet);
      }
    }
    if (prep_resp.num_columns > 0) {
      handler->receive_from_server(conn, &tmp_packet);
      while (!driver->is_eof_packet(&tmp_packet)) {
        handler->receive_from_server(conn, &tmp_packet);
      }
    }
    if (read_only) {
      session->set_prepare_read_connection(exec_req.stmt_id, conn);
    } else {
      if (!session->is_dataspace_keeping_connection(dataspace) &&
          session->get_space_from_kept_space_map(dataspace) == NULL) {
        session->set_kept_connection(dataspace, conn);
      }
    }
  }
  return true;
}
void MySQLDirectExecuteNode::re_init_com_prepare_conn(
    Packet **real_exec_packet) {
  *real_exec_packet = session->get_binary_packet();
  MySQLExecuteRequest exec_req(*real_exec_packet);
  exec_req.unpack();
  MySQLPrepareItem *prepare_item = session->get_prepare_item(exec_req.stmt_id);
  bool read_only = prepare_item->is_read_only();
  if (read_only && !session->is_keeping_connection()) {
    if (conn == NULL) {
      conn = session->get_prepare_read_connection(exec_req.stmt_id);
    }
    if (conn == NULL) {
      session->set_executing_prepare(true);
      conn = handler->send_to_server_retry(dataspace,
                                           prepare_item->get_prepare_packet(),
                                           session->get_schema(), read_only);
      if (!deal_with_com_prepare(true, exec_req, prepare_item)) {
        return;
      }
    }
    exec_req.stmt_id = prepare_item->get_r_stmt_id();
    exec_req.pack(*real_exec_packet);
  } else {
    if (prepare_item->get_w_stmt_id() == 0) {
      conn = handler->send_to_server_retry(dataspace,
                                           prepare_item->get_prepare_packet(),
                                           session->get_schema(), false, false);
      if (!deal_with_com_prepare(false, exec_req, prepare_item)) {
        return;
      }
    }
    exec_req.stmt_id = prepare_item->get_w_stmt_id();
    exec_req.pack(*real_exec_packet);
  }

  if (*real_exec_packet == NULL) {
    throw ExecuteNodeError("Get NULL packet for execute_command.\n");
  }
}

bool MySQLDirectExecuteNode::can_swap() {
  return session->is_may_backend_exec_swap_able();
}

inline void MySQLDirectExecuteNode::handle_call_sp_directly() {
  if (plan->statement->is_call_store_procedure_directly()) {
    LOG_DEBUG(
        "in MySQLDirectExecuteNode, call sp directly, current call_sp_nest=%d, "
        "decrease one.\n",
        session->get_call_sp_nest());
    session->decrease_call_sp_nest();
    session->pop_cur_sp_worker_one_recurse();
  }
}

inline void get_schema_and_table_name_capitalize(string table_name_or_full,
                                                 string &schema_name,
                                                 string &table_name) {
  boost::to_upper(table_name_or_full);
  size_t dot_pos = table_name_or_full.find(".");
  if (dot_pos == string::npos) {
    schema_name = "";
  } else {
    schema_name = string(table_name_or_full, 0, dot_pos);
    boost::trim(schema_name);
    boost::trim_if(schema_name, boost::is_any_of("`"));
    boost::trim_if(schema_name, boost::is_any_of("\""));
  }
  table_name =
      string(table_name_or_full, dot_pos == string::npos ? 0 : dot_pos + 1);
  boost::trim(table_name);
  boost::trim_if(table_name, boost::is_any_of("`"));
  boost::trim_if(table_name, boost::is_any_of("\""));
}

void MySQLDirectExecuteNode::execute() {
  if (session->get_session_state() == SESSION_STATE_WORKING) {
    if (plan->statement->get_stmt_node()->type ==
        STMT_DBSCALE_SHOW_MIGRATE_CLEAN_TABLES)
      sql = "SELECT * FROM dbscale.MIGRATE_CLEAN_SHARD;";
#ifdef DEBUG
    node_start_timing();
#endif
    LOG_DEBUG("MySQLDirectExecuteNode sql : %s .\n", sql);
    execute_profile = session->get_profile_handler();
    profile_id =
        execute_profile->start_serial_monitor(get_executenode_name(), "", sql);
    before_insert_id = 0;
    need_check_last_insert_id = false;

    prepare_work(need_check_last_insert_id, before_insert_id);

    bool can_swap = session->is_may_backend_exec_swap_able();

    packet = Backend::instance()->get_new_packet(
        DEFAULT_PACKET_SIZE, can_swap ? session->get_packet_alloc() : NULL);
    Packet exec_packet(DEFAULT_PACKET_SIZE,
                       can_swap ? session->get_packet_alloc() : NULL);

    Backend *backend = Backend::instance();

    try {
      stmt_type type = plan->statement->get_stmt_node()->type;
      Packet *real_exec_packet = NULL;
      string sql_tmp;
      if (session->is_binary_resultset() &&
          dataspace == backend->get_catalog() &&
          !plan->statement->is_cross_node_join() &&
          !backend->get_metadata_data_space()) {
        re_init_com_prepare_conn(&real_exec_packet);
        direct_prepare_exec_packet = true;
      } else {
        bool inforschema_mirror_table_available =
            (enable_info_schema_mirror_table > 0) && datasource_in_one &&
            (!plan->session->is_call_store_procedure()) &&
            (dataspace == backend->get_auth_data_space() ||
             dataspace == backend->get_metadata_data_space()) &&
            (!strcmp(session->get_username(), supreme_admin_user) ||
             !strcmp(session->get_username(), normal_admin_user) ||
             !strcmp(session->get_username(), dbscale_internal_user) ||
             Backend::instance()->is_mirror_user(
                 string(session->get_username())));

        if (inforschema_mirror_table_available &&
            (type == STMT_SELECT || type == STMT_SHOW_FIELDS ||
             type == STMT_SHOW_TABLES || type == STMT_SHOW_INDEX)) {
          string origin_sql(sql);
          try {
            set<string> infor_tb_replaced;
            string default_schema = plan->session->get_schema();
            boost::to_upper(default_schema);
            string only_used_mirror_tb_name;
            string only_used_schema_name;
            string only_used_table_name;
            switch (type) {
              case STMT_SHOW_FIELDS: {
                show_fields_op_node *oper =
                    plan->statement->get_stmt_node()->sql->show_fields_oper;
                if (oper->is_show_full && !(plan->statement->get_stmt_node()
                                                ->cur_rec_scan->condition)) {
                  table_link *table =
                      plan->statement->get_stmt_node()->table_list_head;
                  const char *schema_name = table->join->schema_name
                                                ? table->join->schema_name
                                                : default_schema.c_str();
                  const char *table_name = table->join->table_name;
                  sql_tmp =
                      "select COLUMN_NAME as `Field`, COLUMN_TYPE as `Type`, "
                      "COLLATION_NAME as `Collation`, IS_NULLABLE as `Null`, "
                      "COLUMN_KEY as `Key`, COLUMN_DEFAULT as `Default`, EXTRA "
                      "as `Extra`, PRIVILEGES as `Privileges`, COLUMN_COMMENT "
                      "as `Comment` from dbscale.COLUMNS where TABLE_SCHEMA='";
                  sql_tmp.append(schema_name);
                  sql_tmp.append("' and TABLE_NAME='");
                  sql_tmp.append(table_name);
                  sql_tmp.append("'");
                  if (oper->is_show_like_or_where) {
                    sql_tmp.append("and COLUMN_NAME like '");
                    sql_tmp.append(oper->is_show_like_or_where);
                    sql_tmp.append("'");
                  }
                  infor_tb_replaced.insert("COLUMNS");
                  is_show_full_fields = true;
                  show_full_fields_schema_name = schema_name;
                  show_full_fields_table_name = table_name;
                  only_used_mirror_tb_name = "COLUMNS";
                  only_used_schema_name = schema_name;
                  only_used_table_name = table_name;
                } else {
                  sql_tmp = origin_sql;
                }
                break;
              }
              case STMT_SHOW_TABLES: {
                show_tables_op_node *show_tables_oper =
                    plan->statement->get_stmt_node()->sql->show_tables_oper;
                if (!(plan->statement->get_stmt_node()
                          ->cur_rec_scan->condition)) {
                  string schema_name = "Tables_in_";
                  string tmp;
                  if (show_tables_oper->db_name) {
                    tmp = show_tables_oper->db_name;
                  } else {
                    tmp = default_schema;
                  }
                  boost::to_lower(tmp);
                  schema_name.append(tmp);
                  if (show_tables_oper->is_show_like_or_where) {
                    schema_name.append(" (");
                    schema_name.append(show_tables_oper->is_show_like_or_where);
                    schema_name.append(")");
                  }
                  sql_tmp = "select TABLE_NAME as `";
                  sql_tmp.append(schema_name);
                  if (show_tables_oper->is_show_full) {
                    sql_tmp.append("`, TABLE_TYPE as `Table_type");
                  }
                  sql_tmp.append("` from dbscale.TABLES where TABLE_SCHEMA='");
                  if (show_tables_oper->db_name) {
                    sql_tmp.append(show_tables_oper->db_name);
                  } else {
                    sql_tmp.append(default_schema);
                  }
                  if (show_tables_oper->is_show_like_or_where) {
                    sql_tmp.append("' and TABLE_NAME like '");
                    sql_tmp.append(show_tables_oper->is_show_like_or_where);
                  }
                  sql_tmp.append("'");
                  infor_tb_replaced.insert("TABLES");
                  only_used_mirror_tb_name = "TABLES";
                  only_used_schema_name = tmp;
                  only_used_table_name = DBSCALE_RESERVED_STR;
                } else {
                  sql_tmp = origin_sql;
                }
                break;
              }
              case STMT_SHOW_INDEX: {
                if (!(plan->statement->get_stmt_node()
                          ->cur_rec_scan->condition)) {
                  table_link *table =
                      plan->statement->get_stmt_node()->table_list_head;
                  const char *schema_name = table->join->schema_name
                                                ? table->join->schema_name
                                                : default_schema.c_str();
                  const char *table_name = table->join->table_name;
                  sql_tmp =
                      "select TABLE_NAME as `Table`, NON_UNIQUE as "
                      "`Non_unique`, INDEX_NAME as `Key_name`, SEQ_IN_INDEX as "
                      "`Seq_in_index`, COLUMN_NAME as `Column_name`, COLLATION "
                      "as `Collation`, CARDINALITY as `Cardinality`, SUB_PART "
                      "as `Sub_part`, PACKED as `Packed`, NULLABLE as `Null`, "
                      "INDEX_TYPE as `Index_type`, COMMENT as `Comment`, "
                      "INDEX_COMMENT as `Index_comment` from "
                      "dbscale.STATISTICS where TABLE_SCHEMA='";
                  sql_tmp.append(schema_name);
                  sql_tmp.append("' and TABLE_NAME='");
                  sql_tmp.append(table_name);
                  sql_tmp.append("'");
                  infor_tb_replaced.insert("STATISTICS");
                  only_used_mirror_tb_name = "STATISTICS";
                  only_used_schema_name = schema_name;
                  only_used_table_name = table_name;
                } else {
                  sql_tmp = origin_sql;
                }
                break;
              }
              case STMT_SELECT: {
                sql_tmp = sql;
                bool default_schema_is_information_schema =
                    (default_schema == "INFORMATION_SCHEMA");
                table_link *table =
                    plan->statement->get_stmt_node()->table_list_head;
                vector<pair<table_link *, string> > table_replace_pos;
                while (table) {
                  string schema_name;
                  string table_name;
                  string table_name_or_full =
                      string(sql_tmp, table->start_pos - 1,
                             table->end_pos - table->start_pos + 1);
                  get_schema_and_table_name_capitalize(table_name_or_full,
                                                       schema_name, table_name);
                  if ((!schema_name.empty() &&
                       schema_name != "INFORMATION_SCHEMA") ||
                      (schema_name.empty() &&
                       !default_schema_is_information_schema) ||
                      (table_name != "COLUMNS" && table_name != "TABLES" &&
                       table_name != "KEY_COLUMN_USAGE" &&
                       table_name != "TABLE_CONSTRAINTS" &&
                       table_name != "STATISTICS" &&
                       (table_name != "PARTITIONS" ||
                        (table_name == "PARTITIONS" &&
                         Backend::instance()
                             ->get_info_mirror_tb_exclude_partitions())) &&
                       table_name != "ROUTINES")) {
                    table = table->next;
                    continue;
                  }
                  table_replace_pos.push_back(make_pair(table, table_name));
                  table = table->next;
                }
                if (!table_replace_pos.empty()) {
                  vector<pair<table_link *, string> >::iterator it =
                      table_replace_pos.begin();
                  int alter_pos_val = 0;
                  for (; it != table_replace_pos.end(); it++) {
                    string replace_val = string("dbscale.");
                    replace_val.append(it->second);
                    infor_tb_replaced.insert(it->second);
                    sql_tmp.replace(
                        size_t((int)(it->first->start_pos - 1) + alter_pos_val),
                        it->first->end_pos - it->first->start_pos + 1,
                        replace_val);
                    alter_pos_val +=
                        replace_val.length() -
                        int(it->first->end_pos - it->first->start_pos + 1);
                  }
                }
              }
              default:
                break;
            }

            bool can_use_informationschema_mirror_table = false;
            if (!only_used_schema_name.empty()) {
              can_use_informationschema_mirror_table =
                  Backend::instance()->is_informationschema_mirror_tb_status_ok(
                      only_used_mirror_tb_name, only_used_schema_name,
                      only_used_table_name);
            } else if (!infor_tb_replaced.empty()) {
              can_use_informationschema_mirror_table =
                  Backend::instance()
                      ->is_all_informationschema_mirror_tb_status_ok(
                          infor_tb_replaced);
            }
            if (can_use_informationschema_mirror_table) {
              sql = sql_tmp.c_str();
              LOG_DEBUG(
                  "informationschema_mirror_tb_status ok, DirectExecutionNode "
                  "sql to auth/metadata sql is changed to [%s]\n",
                  sql);
              if (is_show_full_fields) is_show_full_fields_via_mirror_tb = true;
            }
          } catch (exception &e) {
            sql = origin_sql.c_str();
            LOG_WARN(
                "mirror table get exception [%s] for sql [%s], skip the mirror "
                "replace operation.\n",
                e.what(), sql);
          }
        }
        MySQLQueryRequest query(sql);
        query.set_sql_replace_char(
            plan->session->get_query_sql_replace_null_char());
        query.pack(&exec_packet);
        real_exec_packet = &exec_packet;
      }

      if (!plan->get_is_parse_transparent() &&
          (type == STMT_GRANT || type == STMT_DROP_USER ||
           type == STMT_CREATE_USER || type == STMT_SHOW_DATABASES ||
           type == STMT_SHOW_TABLES || type == STMT_SET_PASSWORD ||
           type == STMT_REVOKE || type == STMT_SHOW_GRANTS ||
           type == STMT_ALTER_USER || type == STMT_NON ||
           (type == STMT_SELECT &&
            dataspace == backend->get_metadata_data_space() &&
            datasource_in_one))) {
        /*If add new type here, should also modify the
         * MySQLDirectExecuteNode::clean to ensure this conn will be added
         * back to dead.*/
        if (connect_with_cur_user(type)) {
          has_change_user = true;
        }
      }
      defined_lock_type dl_type =
          plan->statement->get_stmt_node()->defined_lock;
      if (dl_type != DEFINED_LOCK_NON) {
        if (enable_cluster_xa_transaction &&
            session->is_in_cluster_xa_transaction()) {
          if (session->check_cluster_conn_is_xa_conn(conn)) {
            LOG_DEBUG(
                "conn %@ is cluster xa conn, we won't handle it executenode "
                "error.\n",
                conn);
            conn = NULL;
          }
          throw ExecuteNodeError(
              "Can't execute get_lock() or release_lock() in cluster xa "
              "transaction");
        }
        defined_lock_name = session->get_defined_lock_field_name();
        conn = session->get_defined_lock_kept_conn(dataspace);
        if (conn) {
          session->set_lock_kept_conn(true);
          set_kept_defined_lock_conn(true);
          session->check_kept_connection_for_same_server(dataspace);
        } else {
          if (dl_type == DEFINED_GET_LOCK) {
            session->set_lock_kept_conn(true);
            set_kept_defined_lock_conn(true);
          }
        }
      }

#ifndef DBSCALE_TEST_DISABLE
      handler->send_op.start_timing();
#endif

      if (conn) {
        if (conn->get_session_var_map_md5() !=
                session->get_session_var_map_md5() ||
            has_change_user) {
          map<string, string> *session_var_map = session->get_session_var_map();
          conn->set_session_var(session_var_map, true, has_change_user);
        }
        const char *sch = session->get_schema();
        try {
          if (sch || sch[0]) conn->change_schema(sch);
        } catch (HandlerError &e) {
          if (enable_acl) {
            string msg = e.what();
            if (msg.find("1044 (42000) Access denied for user") !=
                string::npos) {
              throw ExecuteNodeError(e.what());
            }
          }
        } catch (...) {
          throw;
        }
        conn->reset();
        handler->send_to_server(conn, real_exec_packet);
      } else {
        if (plan->statement->is_cross_node_join()) {
          conn = handler->send_to_server_retry(dataspace, real_exec_packet,
                                               session->get_schema(), false);
        } else {
          if (direct_prepare_exec_packet) {
            // TODO: mark as can not swap
            conn = handler->send_to_server_retry(
                dataspace, real_exec_packet, session->get_schema(),
                session->is_read_only(), false, false);

          } else {
            conn = handler->send_to_server_retry(
                dataspace, real_exec_packet, session->get_schema(),
                session->is_read_only(), true, false);
          }
        }
      }
      if (conn) {
        plan->set_single_server_innodb_rollback_on_timeout(
            conn->get_server()->get_innodb_rollback_on_timeout());
      }
#ifndef DBSCALE_TEST_DISABLE
      handler->send_op.end_timing_and_report();
#endif
    } catch (ExecuteNodeError &e) {
      session->set_lock_kept_conn(false);
      if (conn) {
        handler->clean_dead_conn(&conn, dataspace);
      }
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      throw e;
    } catch (exception &e) {
      session->set_lock_kept_conn(false);
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      LOG_ERROR("DirectExecuteNode fail due to exception [%s].\n", e.what());
      string error_message("DirectExecuteNode fail due to exception:");
      error_message.append(e.what());
      if (conn) {
        handler->clean_dead_conn(&conn, dataspace);
      }
      throw ExecuteNodeError(error_message.c_str());
      ;
    }

    if (!conn->is_odbconn() && session->is_may_backend_exec_swap_able()) {
      conn->set_session(session);
      struct event *conn_event = conn->get_conn_socket_event();
      evutil_socket_t conn_socket = conn->get_conn_socket();
      SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
      ACE_ASSERT(conn_base);
#endif
      session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
      LOG_DEBUG(
          "Finish to prepare the conn socket session swap for conn %@ and "
          "session %@.\n",
          conn, session);
#endif

      return;
    }
  }

  /*session state is SESSION_STATE_HANDLING_RESULT for swap mode.*/
  const char *server_name = NULL;
  bool is_readonly_conn = true;

  /*The handler of ExecuteNode should be reset for
   * session_state_handling_result, cause the handler may has been
   * swapped.*/
  try {
    bool has_more_result = false;
    server_name = conn->get_server()->get_name();
    conn->handle_merge_exec_begin_or_xa_start(session);

    do {
      handler->receive_from_server(conn, packet);
      if (driver->is_error_packet(packet)) {
        has_more_result = false;
        is_readonly_conn = false;
        handle_set_last_insert_id(need_check_last_insert_id, before_insert_id);
        handle_error_packet(packet);
      } else if (driver->is_ok_packet(packet)) {
        handler->deal_with_metadata_execute(
            plan->statement->get_stmt_node()->type, plan->statement->get_sql(),
            session->get_schema(), plan->statement->get_stmt_node());
        if (support_show_warning)
          handle_warnings_OK_and_eof_packet(plan, packet, handler, dataspace,
                                            conn);
        has_more_result = handle_ok_packet(packet, need_check_last_insert_id,
                                           before_insert_id, is_readonly_conn);
        if (!plan->session->is_call_store_procedure() &&
            !plan->statement->is_cross_node_join() &&
            !plan->statement->is_union_table_sub() &&
            !plan->session->get_is_silence_ok_stmt()) {
          if (plan->session->get_has_more_result()) {
            rebuild_ok_with_has_more_flag(packet, driver);
            LOG_DEBUG(
                "For multiple stmt, the ok packet of middle stmt should be with"
                "flag has_more_result.\n");
          }
          handler->deal_autocommit_with_ok_eof_packet(packet);
          handler->send_to_client(packet);
        } else
          LOG_DEBUG(
              "In store procedure call or cross node join, so skip sending"
              " of ok packet in direct node.\n");
      } else if (driver->is_result_set_header_packet(packet)) {
        handle_send_client_packet(packet, false);
        has_more_result = handle_result_set(packet);
        if (!has_more_result) {
          handle_set_last_insert_id(need_check_last_insert_id,
                                    before_insert_id);
        }
      } else {
#ifdef DEBUG
        ACE_ASSERT(0);  // should not be here
#endif
        handle_send_client_packet(packet, false);
        has_more_result = false;
      }
#ifdef DEBUG
      LOG_DEBUG("Has more result %d.\n", has_more_result ? 1 : 0);
#endif
    } while (has_more_result);

    if (get_kept_defined_lock_conn() &&
        session->get_defined_lock_depth() <= 0) {
      session->clean_defined_lock_conn();
      session->set_lock_kept_conn(false);
    }

    if (select_uservar_flag) {
      deal_with_var(conn, plan, driver);
    }
  } catch (ErrorPacketException &e) {
    LOG_DEBUG("DirectExecuteNode get an error packet when execute sql [%s].\n",
              sql);
    status = EXECUTE_STATUS_COMPLETE;
    if (conn && error_packet && plan->session->is_call_store_procedure()) {
      handler->put_back_connection(dataspace, conn);
      conn = NULL;
    }

    handle_call_sp_directly();
    throw e;
  } catch (ExecuteNodeError &e) {
    if (get_kept_defined_lock_conn()) {
      session->clean_defined_lock_conn();
      session->set_lock_kept_conn(false);
    }
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    handle_call_sp_directly();
    throw e;
  } catch (exception &e) {
    if (get_kept_defined_lock_conn()) {
      session->clean_defined_lock_conn();
      session->set_lock_kept_conn(false);
    }
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR("DirectExecuteNode fail due to exception [%s].\n", e.what());
    string error_message("DirectExecuteNode fail due to exception:");
    error_message.append(e.what());
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    handle_call_sp_directly();
    throw ExecuteNodeError(error_message.c_str());
    ;
  }

#ifdef DEBUG
  node_end_timing();
#endif
  execute_profile->end_execute_monitor(profile_id);
  handle_call_sp_directly();
#ifdef DEBUG
  LOG_DEBUG("MySQLDirectNode %@ cost %d ms\n", this, node_cost_time);
#endif
  status = EXECUTE_STATUS_COMPLETE;

  if (plan->statement->get_stmt_node()->has_unknown_func)
    is_readonly_conn = false;
  if (!is_readonly_conn && conn) {
    session->record_xa_modified_conn(conn);
  }
  record_xa_modify_sql(plan, session, dataspace, sql, is_readonly_conn);
  record_modify_server(plan, session, server_name,
                       dataspace->get_virtual_machine_id(), is_readonly_conn);
}  // end of MySQLDirectExecuteNode::execute()

void MySQLDirectExecuteNode::execute_grant_sqls() {
  list<string> sqls = plan->statement->get_grant_sqls();
  try {
    list<string>::iterator it = sqls.begin();
    it++;
    for (; it != sqls.end(); it++) {
      LOG_DEBUG("Execute grant sql:[%s].\n", it->c_str());
      conn->execute_one_modify_sql_with_sqlerror(it->c_str());
    }
  } catch (dbscale::sql::SQLError &e) {
    string tmp("Got error packet when execute grant sqls, due to ");
    tmp += e.what();
    LOG_ERROR("%s.\n", tmp.c_str());
    throw ExecuteNodeError(tmp.c_str());
  }
}

/* class MySQLSetNode */
MySQLSetNode::MySQLSetNode(ExecutePlan *plan, DataSpace *dataspace,
                           const char *sql)
    : MySQLExecuteNode(plan), conn(NULL) {
  this->sql = sql;
  this->dataspace = dataspace;
  this->name = "MySQLSetNode";
  got_error = false;
  packet = NULL;
  error_packet = NULL;
}
bool MySQLSetNode::is_need_trans(string value) {
  size_t length = value.length();
  size_t pos = 0;
  pos = value.find("'");
  while (pos != string::npos && pos < length) {
    if (pos + 1 < length && value[pos + 1] == '\'') {
      pos++;
    } else {
      int i = pos - 1;
      size_t count = 0;
      while (i >= 0) {
        if (value[i] == '\\') {
          count++;
          i--;
        } else {
          break;
        }
      }
      if (!(count % 2)) {
        return true;
      }
    }
    if (pos + 1 < length) {
      pos = value.find("'", pos + 1);
    } else {
      pos++;
    }
  }
  return false;
}

bool MySQLSetNode::can_swap() {
  return session->is_may_backend_exec_swap_able();
}

void MySQLSetNode::replace_query_sql(string &query_sql) {
  if (plan->statement->get_stmt_node()->row_count_num) {
    plan->statement->replace_sql_function_field(
        FUNCTION_TYPE_ROW_COUNT, query_sql,
        plan->statement->get_stmt_node()->scanner, plan);
  }
  if (plan->statement->get_stmt_node()->last_insert_id_num) {
    plan->statement->replace_sql_function_field(
        FUNCTION_TYPE_LAST_INSERT_ID, query_sql,
        plan->statement->get_stmt_node()->scanner, plan);
  }
}

void MySQLSetNode::exe_state_working() {
#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(),
                  "set_session_error_condition") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "server_exception")) {
    sql = "bad sql";
  }
#endif
  Packet exec_packet;
  string query_sql(sql);
  replace_query_sql(query_sql);
  MySQLQueryRequest query(query_sql.c_str());
  query.set_sql_replace_char(plan->session->get_query_sql_replace_null_char());
  query.pack(&exec_packet);
  real_sql += query_sql.c_str();

  map<DataSpace *, Connection *> kept_connections =
      session->get_kept_connections();
  try {
    if (!kept_connections.empty()) {
      map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
      for (; it != kept_connections.end();) {
        conn = it->second;
        if (conn) {
          dataspace = it->first;
          conn->reset();
          try {
            handler->set_user_var(&conn, dataspace);
            handler->send_to_server(conn, &exec_packet);
            local_connections[dataspace] = conn;
          } catch (...) {
            LOG_ERROR("MySQLSetNode fail send due to exception.\n");
            LOG_ERROR("Error dataspace name is [%s]\n", dataspace->get_name());
            conn_result con_ret;
            con_ret.is_dead_conn = true;
            con_ret.conn = conn;
            con_ret.space = dataspace;
            con_ret.packet = NULL;
            commit_conn_result.push_back(con_ret);
            if (session->defined_lock_need_kept_conn(it->first))
              session->remove_defined_lock_kept_conn(it->first);
            session->remove_kept_connection(dataspace);
            session->remove_using_conn(conn);
            if (conn) {
              conn->get_pool()->add_back_to_dead(conn);
              kept_connections.erase(it++);
              conn = NULL;
            }
            if (local_connections.empty()) throw;
            break;
          }
        }
        it++;
      }
    } else {
      conn = handler->send_to_server_retry(
          dataspace, &exec_packet, session->get_schema(),
          session->is_read_only(), true, false);
    }
  } catch (ExecuteNodeError &e) {
    if (session->defined_lock_need_kept_conn(dataspace))
      session->remove_defined_lock_kept_conn(dataspace);
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn_set_session_var();
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    throw;
  } catch (exception &e) {
    if (session->defined_lock_need_kept_conn(dataspace))
      session->remove_defined_lock_kept_conn(dataspace);
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn_set_session_var();
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    LOG_DEBUG("SetNode fail due to exception [%s].\n", e.what());
    string error_message("SetNode fail due to exception:");
    error_message.append(e.what());
    throw ExecuteNodeError(error_message.c_str());
  }
}

void MySQLSetNode::exe_state_handling_result() {
  try {
#ifdef DEBUG
    LOG_DEBUG("Receiving result from server\n");
#endif
    if (!local_connections.empty()) {
      map<DataSpace *, Connection *>::iterator it = local_connections.begin();
      for (; it != local_connections.end(); it++) {
        conn = it->second;
        try {
          conn->handle_merge_exec_begin_or_xa_start(session);

          handler->receive_from_server(conn, packet);
          if (driver->is_error_packet(packet)) {
            conn_result con_ret;
            con_ret.conn = conn;
            con_ret.space = it->first;
            con_ret.is_dead_conn = false;

            MySQLErrorResponse response(packet);
            if (response.is_shutdown()) {
              if (conn) {
                con_ret.is_dead_conn = true;
              }
              session->set_server_shutdown(true);
            }
            Packet *tmp_packet = Backend::instance()->get_new_packet();
            con_ret.packet = packet;
            packet = tmp_packet;
            commit_conn_result.push_back(con_ret);
            conn = NULL;
            continue;
          }
        } catch (ErrorPacketException &e) {
          LOG_DEBUG("SetNode get an error packet when execute sql [%s].\n",
                    sql);
          conn_result con_ret;
          con_ret.is_dead_conn = false;
          con_ret.conn = conn;
          con_ret.space = it->first;
          con_ret.packet = packet;
          packet = NULL;
          commit_conn_result.push_back(con_ret);
        } catch (...) {
          LOG_ERROR("MySQLSetNode fail send due to exception.\n");
          conn_result con_ret;
          con_ret.is_dead_conn = true;
          con_ret.conn = conn;
          con_ret.space = it->first;
          con_ret.packet = NULL;
          commit_conn_result.push_back(con_ret);
          session->remove_kept_connection(it->first);
          session->remove_using_conn(it->second);
          it->second->get_pool()->add_back_to_dead(it->second);
        }
      }
    } else {
      conn->handle_merge_exec_begin_or_xa_start(session);

      handler->receive_from_server(conn, packet);
    }

    if (!commit_conn_result.empty()) {
      bool has_dead_conn = false;
      error_packet = NULL;
      vector<conn_result>::iterator it = commit_conn_result.begin();
      for (; it != commit_conn_result.end(); it++) {
        if (it->is_dead_conn) {
          has_dead_conn = true;
          if (it->conn) handler->clean_dead_conn(&(it->conn), it->space);
          continue;
        }
        if (it->packet) {
          if (error_packet)
            delete it->packet;
          else
            error_packet = it->packet;
          if (it->conn) handler->put_back_connection(it->space, it->conn);
        }
      }

      if (has_dead_conn)
        throw ExecuteNodeError(
            "Get exception for SetNode with dead server connection.");
      if (error_packet) throw ErrorPacketException();
      throw ExecuteNodeError("Unexpect behavior.");
    }
    if (driver->is_error_packet(packet)) {
      status = EXECUTE_STATUS_COMPLETE;
      error_packet = packet;
      packet = NULL;
      MySQLErrorResponse response(error_packet);
      if (response.is_shutdown()) {
        if (conn) {
          handler->clean_dead_conn(&conn, dataspace);
        }
        session->set_server_shutdown(true);
      }
      throw ErrorPacketException();
    }
    if (driver->is_ok_packet(packet)) {
      deal_with_var(conn, plan, driver);
    }
    status = EXECUTE_STATUS_COMPLETE;
    if (!session->is_call_store_procedure()) {
      handler->deal_autocommit_with_ok_eof_packet(packet);
      handler->record_affected_rows(packet);
#ifndef DBSCALE_TEST_DISABLE
      dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(),
                      "set_session_error_condition") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "client_exception")) {
        throw ClientBroken();
      }
#endif
      handler->send_to_client(packet);
    }
    record_xa_modify_sql(plan, session, dataspace, real_sql.c_str(), false);
  } catch (ErrorPacketException &e) {
    conn_set_session_var();
    LOG_DEBUG("SetNode get an error packet when execute sql [%s].\n", sql);
    throw;
  } catch (exception &e) {
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn_set_session_var();
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    LOG_DEBUG("SetNode fail due to exception [%s].\n", e.what());
    string error_message("SetNode fail due to exception:");
    error_message.append(e.what());
    throw ExecuteNodeError(error_message.c_str());
  }
}

bool MySQLSetNode::handle_swap() {
  /*  Check if we can swap, the function is used in Swapable Node.  If it can
   *  swap, the state changes from SESSION_STATE_WORKING =>
   *  SESSION_STATE_WAITING_SERVER. Otherwise, session state cahnge from
   *  SESSION_STATE_WORKING => SESSION_STATE_HANDLING_RESULT */
  if (session->is_may_backend_exec_swap_able()) {
    if (local_connections.size()) {
      map<DataSpace *, Connection *>::iterator it3;
      for (it3 = local_connections.begin(); it3 != local_connections.end();
           it3++) {
        conn = it3->second;
        if (conn) {
          conn->set_session(session);
          struct event *conn_event = conn->get_conn_socket_event();
          evutil_socket_t conn_socket = conn->get_conn_socket();
          SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
          ACE_ASSERT(conn_base);
#endif
          session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
          LOG_DEBUG(
              "Finish to prepare the conn socket session swap for conn %@ and "
              "session %@.\n",
              conn, session);
#endif
        }
      }
      conn = NULL;
    } else {
      if (conn) {
        conn->set_session(session);
        struct event *conn_event = conn->get_conn_socket_event();
        evutil_socket_t conn_socket = conn->get_conn_socket();
        SocketEventBase *conn_base = conn->get_socket_base();
#ifdef DEBUG
        ACE_ASSERT(conn_base);
#endif
        session->add_server_base(conn_socket, conn_base, conn_event);
#ifdef DEBUG
        LOG_DEBUG(
            "Finish to prepare the conn socket session swap for conn %@ and "
            "session %@.\n",
            conn, session);
#endif
      }
    }
    session->set_mul_connection_num(local_connections.size());
    return true;
  }
  return false;
}

void MySQLSetNode::execute() {
  if (session->get_session_state() == SESSION_STATE_WORKING) {
    if (!packet) packet = Backend::instance()->get_new_packet();

    exe_state_working();
    if (handle_swap()) return;
  }

  exe_state_handling_result();

}  // end of MySQLSetNode::execute()

void MySQLSetNode::conn_set_session_var() {
  map<DataSpace *, Connection *> kept_connections =
      session->get_kept_connections();
  bool deal_with_exception = false;
  if (kept_connections.size()) {
    map<DataSpace *, Connection *>::iterator it = kept_connections.begin();
    for (; it != kept_connections.end(); it++) {
      Connection *kept_conn = it->second;
      if (kept_conn) {
        try {
          if (session->get_session_var_map_md5() !=
              kept_conn->get_session_var_map_md5())
            kept_conn->set_session_var(session->get_session_var_map());
        } catch (HandlerError &e) {
          kept_conn->clean_session_var_map();
          continue;
        } catch (exception &e) {
          deal_with_exception = true;
          if (kept_conn) {
            handler->clean_dead_conn(&kept_conn, dataspace);
          }
          continue;
        }
      }
    }
  } else if (conn) {
    try {
      if (session->get_session_var_map_md5() != conn->get_session_var_map_md5())
        conn->set_session_var(session->get_session_var_map());
    } catch (HandlerError &e) {
      conn->clean_session_var_map();
    } catch (exception &e) {
      if (conn) {
        handler->clean_dead_conn(&conn, dataspace);
      }
      deal_with_exception = true;
    }
  }
  if (deal_with_exception) {
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    string error_message("SetNode fail due to exception:");
    throw ExecuteNodeError(error_message.c_str());
  }
}

void MySQLSetNode::clean() {
  if (conn) {
    if (!got_error || error_packet) {
      handler->put_back_connection(dataspace, conn);
    } else {
      handler->clean_dead_conn(&conn, dataspace);
    }
    conn = NULL;
  }
  if (error_packet) delete error_packet;
  if (packet) {
    delete packet;
    packet = NULL;
    error_packet = NULL;
  }
}

#ifndef DBSCALE_DISABLE_SPARK
/* class MySQLSparkNode */
MySQLSparkNode::MySQLSparkNode(ExecutePlan *plan, SparkConfigParameter config,
                               bool is_insert)
    : MySQLExecuteNode(plan), cond(mutex) {
  this->config = config;
  this->name = "MySQLSparkNode";
  read_rows = new list<vector<string> >();
  kept_rows = new list<vector<string> >();
  column_received = false;
  service_finished = false;
  finished = false;
  got_error = false;
  head_packet = NULL;
  eof_packet = NULL;
  spark_result.success = false;
  fetch_result.success = true;
  kept_rows_size = 0;
  bthread = NULL;
  thread_status = THREAD_STOP;
  max_spark_buffer_rows = 0;
  if (max_fetchnode_ready_rows_size > 18446744073709551615UL / 1024UL) {
    max_spark_buffer_rows_size = 18446744073709551615UL;
  } else {
    max_spark_buffer_rows_size = max_fetchnode_ready_rows_size * 1024;
  }
  this->is_insert_select = is_insert;
  has_started = false;
}

void MySQLSparkNode::clean() {
  if (thread_status == THREAD_STARTED) {
    LOG_DEBUG("Fetch node %@ cleaning wait.\n", this);
    this->wait_for_cond();
    LOG_DEBUG("Fetch node %@ cleaning wait finish.\n", this);
  } else if (thread_status == THREAD_CREATED) {
    bthread->re_init_backend_thread();
    bthread->get_pool()->add_back_to_free(bthread);
  }
  if (head_packet) {
    delete head_packet;
    head_packet = NULL;
  }
  if (eof_packet) {
    delete eof_packet;
    eof_packet = NULL;
  }
  if (!column_list.empty()) {
    list<Packet *>::iterator it = column_list.begin();
    for (; it != column_list.end(); ++it) {
      delete *it;
    }
    column_list.clear();
  }
  delete kept_rows;
  delete read_rows;
}

void MySQLSparkNode::stop_spark_service() {
  SparkKillParameter spark_kill_param;
  spark_kill_param.job_id = config.job_id;
  SparkReturnValue return_value = kill_spark_service(spark_kill_param);
  if (!return_value.success) {
    LOG_ERROR("Fail to stop spark service:%s\n",
              return_value.error_string.c_str());
  }
}

bool MySQLSparkNode::notify_parent() {
  int ret = false;
  ACE_Guard<ACE_Thread_Mutex> guard(mutex);
  // check fetch node finished or get error
  if (is_finished() || got_error) {
    if (got_error) {
      LOG_ERROR("Spark node got exception [%s].\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    } else {
#ifdef DEBUG
      LOG_DEBUG("SparkNode %@ is finished.\n", this);
#endif
      ret = false;
    }
  } else {
    if (ready_rows->empty()) cond.wait();

    if (got_error) {
      LOG_ERROR("Spark node got exception [%s].\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    } else if (ready_rows->empty()) {  // the result set is empty
      while (ready_rows->empty()) {
        if (is_finished()) {
          LOG_DEBUG("FetchNode %@ is finished after wait.\n", this);
          ret = false;
          break;
        }
        cond.wait();
      }
    } else {
      if (kept_rows_size > 0) {
        parent->swap_ready_list_with_row_map_list(this, &ready_rows,
                                                  kept_rows_size, 0);
        kept_rows_size = 0;
      }
      ret = true;
    }
  }

  return ret;
}

void *start_spark_service(void *arg) {
  MySQLSparkNode *node = (MySQLSparkNode *)arg;
  SparkConfigParameter config = node->config;

  node->plan->session->start_executing_spark();
  SparkReturnValue return_value = run_spark_service(config);
  node->plan->session->stop_executing_spark();

  node->set_spark_return_value(return_value);
  return NULL;
}

void MySQLSparkNode::execute() {
  if (has_started) return;
  has_started = true;

  bool thread_created = false;
  try {
    start_spark_thread();
    thread_status = THREAD_STARTED;
    thread_created = true;
    plan->start_all_bthread();

    mutex.acquire();
    if (!column_received && thread_status != THREAD_STOP) {
      cond.wait();
    }
    mutex.release();

    if (service_finished && !spark_result.success) {
      LOG_ERROR("SparkNode got error [%s].\n",
                spark_result.error_string.c_str());
      throw Error(spark_result.error_string.c_str());
    }
    if (got_error) {
      LOG_ERROR("SparkNode got error [%s].\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }

    head_packet = Backend::instance()->get_new_packet();
    MySQLResultSetHeaderResponse result_set_header(columns.size());
    result_set_header.pack(head_packet);
    if (!is_insert_select)
      handler->send_mysql_packet_to_client_by_buffer(head_packet);

    set_column_packet();

    if (!is_insert_select) {
      list<Packet *>::iterator it = column_list.begin();
      for (; it != column_list.end(); ++it) {
        handler->send_mysql_packet_to_client_by_buffer(*it);
      }
    }

    eof_packet = Backend::instance()->get_new_packet();
    MySQLEOFResponse eof;
    eof.pack(eof_packet);
    handler->deal_autocommit_with_ok_eof_packet(eof_packet);

    if (is_insert_select) {
      LOG_DEBUG("Spark insert select, execute just return.\n");
      return;
    }

    handler->send_mysql_packet_to_client_by_buffer(eof_packet);

    send_row_packet();

    if (service_finished && !spark_result.success) {
      LOG_ERROR("SparkNode got error [%s].\n",
                spark_result.error_string.c_str());
      throw Error(spark_result.error_string.c_str());
    }
    if (got_error) {
      LOG_ERROR("SparkNode got error [%s].\n", error_msg.c_str());
      throw Error(error_msg.c_str());
    }

    MySQLEOFResponse end;
    end.pack(eof_packet);
    handler->deal_autocommit_with_ok_eof_packet(eof_packet);
    handler->send_mysql_packet_to_client_by_buffer(eof_packet);

    handler->flush_net_buffer();
  } catch (...) {
    if (thread_created) ACE_Thread::join(l_handle);
    handler->flush_net_buffer();
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR("Got exception when execute spark node.\n");
    throw;
  }

  ACE_Thread::join(l_handle);

  if (service_finished && !spark_result.success) {
    LOG_ERROR("SparkNode got error [%s].\n", spark_result.error_string.c_str());
    throw Error(spark_result.error_string.c_str());
  }
  if (got_error) {
    LOG_ERROR("SparkNode got error [%s].\n", error_msg.c_str());
    throw Error(error_msg.c_str());
  }

#ifdef DEBUG
  LOG_DEBUG("MySQLSparkNode %@ cost %d ms.\n", this, node_cost_time);
#endif
  status = EXECUTE_STATUS_COMPLETE;
}

void MySQLSparkNode::set_column_packet() {
  const char *catalog = "def";
  const char *schema = session->get_schema();
  const char *table = "tables";
  const char *org_table = "tables";

  vector<string>::iterator it = columns.begin();
  for (; it != columns.end(); ++it) {
    Packet *packet = Backend::instance()->get_new_packet();
    MySQLColumnResponse field(catalog, schema, table, org_table, it->c_str(),
                              it->c_str(), 8, NAME_LEN + 1,
                              MYSQL_TYPE_VAR_STRING, 0, 0, 0);
    field.pack(packet);
    column_list.push_back(packet);
  }
}

void MySQLSparkNode::send_row_packet() {
  Packet packet(row_packet_size);
  while (1) {
    if (session->get_is_killed()) {
      LOG_INFO("Session is killed when SparkNode send row packet.\n");
      throw Error("Session is killed.");
    }
    if (ACE_Reactor::instance()->reactor_event_loop_done()) {
      LOG_INFO("DBScale is exiting when SparkNode send row packet.\n");
      throw Error("DBScale is exiting.");
    }
    mutex.acquire();
    if (kept_rows_size == 0) {
      if (finished) {
        mutex.release();
        return;
      } else {
        cond.wait();
      }
    }
    if (kept_rows_size > 0) {
      list<vector<string> > *tmp_rows;
      tmp_rows = read_rows;
      read_rows = kept_rows;
      kept_rows = tmp_rows;
      kept_rows_size = 0;
    }
    mutex.release();

    while (!read_rows->empty()) {
      try {
        pack_row_data(&packet, read_rows->front());
        handler->send_mysql_packet_to_client_by_buffer(&packet);
      } catch (...) {
        read_rows->pop_front();
        LOG_ERROR("MySQLSparkNode failed to send rows to client.\n");
        throw;
      }
      read_rows->pop_front();
    }
  }
}

void MySQLSparkNode::add_to_ready_rows(vector<vector<string> > &datas) {
  try {
    ACE_Guard<ACE_Thread_Mutex> guard(mutex);
    if (!datas.empty()) {
      Packet *packet = NULL;
      kept_rows_size += datas.size();
      if ((kept_rows_size & 0x03FF) == 0 || max_spark_buffer_rows == 0) {
        vector<string>::iterator it = datas[0].begin();
        int row_size = 0;
        for (; it != datas[0].end(); ++it) {
          row_size += it->length();
        }
        row_size = row_size > row_packet_size ? row_size : row_packet_size;
        max_spark_buffer_rows = max_spark_buffer_rows_size / row_size;
        if (!max_spark_buffer_rows) max_spark_buffer_rows = 1000;
      }
      vector<vector<string> >::iterator it = datas.begin();
      for (; it != datas.end(); ++it) {
        packet = Backend::instance()->get_new_packet(row_packet_size);
        pack_row_data(packet, *it);
        ready_rows->push_back(packet);
      }
      cond.signal();
    }
  } catch (...) {
    cond.signal();
    throw;
  }
}

void MySQLSparkNode::add_to_kept_rows(vector<vector<string> > &datas) {
  try {
    ACE_Guard<ACE_Thread_Mutex> guard(mutex);
    if (!datas.empty()) {
      kept_rows->insert(kept_rows->end(), datas.begin(), datas.end());
      cond.signal();
      kept_rows_size += datas.size();
      if ((kept_rows_size & 0x03FF) == 0 || max_spark_buffer_rows == 0) {
        vector<string>::iterator it = datas[0].begin();
        int row_size = 0;
        for (; it != datas[0].end(); ++it) {
          row_size += it->length();
        }
        row_size = row_size > row_packet_size ? row_size : row_packet_size;
        max_spark_buffer_rows = max_spark_buffer_rows_size / row_size;
        if (!max_spark_buffer_rows) max_spark_buffer_rows = 1000;
      }
    }
  } catch (...) {
    cond.signal();
    throw;
  }
}

void MySQLSparkNode::pack_row_data(Packet *packet, vector<string> &row_data) {
  list<const char *> tmp_row_data;
  vector<string>::iterator it = row_data.begin();
  for (; it != row_data.end(); ++it) {
    tmp_row_data.push_back(it->c_str());
  }
  MySQLRowResponse row(tmp_row_data);
  row.pack(packet);
}

int MySQLSparkNode::svc() {
  SparkFetchParameter param;
  param.job_id = config.job_id;
  if (!column_received) {
    columns = get_columns_from_spark(param, fetch_result).columns;
    if (!fetch_result.success) {
      LOG_ERROR("SparkNode got error [%s].\n",
                fetch_result.error_string.c_str());
      got_error = true;
      set_error_msg(fetch_result.error_string);
      mutex.acquire();
      column_received = true;
      cond.signal();
      mutex.release();
      return FINISHED;
    }
    if (columns.empty()) {
      if (service_finished) {
        columns = get_columns_from_spark(param, fetch_result).columns;
        if (!fetch_result.success) {
          LOG_ERROR("SparkNode got error [%s].\n",
                    fetch_result.error_string.c_str());
          got_error = true;
          set_error_msg(fetch_result.error_string);
          mutex.acquire();
          column_received = true;
          cond.signal();
          mutex.release();
          return FINISHED;
        }
        if (columns.empty()) {
          LOG_ERROR("Fail to get columns from spark.\n");
          got_error = true;
          set_error_msg("Fail to get columns from spark.");
          mutex.acquire();
          column_received = true;
          cond.signal();
          mutex.release();
          return FINISHED;
        }
      }
      return NEED_SLEEP;
    }
    mutex.acquire();
    column_received = true;
    cond.signal();
    mutex.release();
  }
  vector<vector<string> > fetch_datas;
  while (1) {
    if (session->get_is_killed()) {
      LOG_INFO("Session is killed when SparkNode fetch data.\n");
      stop_spark_service();
      throw Error("Session is killed.");
    }
    if (ACE_Reactor::instance()->reactor_event_loop_done()) {
      LOG_INFO("DBScale is exiting when SparkNode fetch data.\n");
      stop_spark_service();
      throw Error("DBScale is exiting.");
    }
    // string Copy-On-Write
    fetch_datas = fetch_data_from_spark(param, fetch_result).data_list;

#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(),
                    "dbscale_spark_insert") &&
        !strcasecmp(test_info->test_case_operation.c_str(), "exception")) {
      got_error = true;
      set_error_msg("Got exception when fetch data.");
      stop_spark_service();
      break;
    }
#endif

    if (!fetch_result.success) {
      LOG_ERROR("SparkNode got error [%s].\n",
                fetch_result.error_string.c_str());
      got_error = true;
      set_error_msg(fetch_result.error_string);
      break;
    }
    if (fetch_datas.empty()) {
      if (service_finished) {
        fetch_datas = fetch_data_from_spark(param, fetch_result).data_list;
        if (!fetch_result.success) {
          LOG_ERROR("SparkNode got error [%s].\n",
                    fetch_result.error_string.c_str());
          got_error = true;
          set_error_msg(fetch_result.error_string);
          break;
        }
        if (fetch_datas.empty()) {
          break;
        } else {
          if (is_insert_select)
            add_to_ready_rows(fetch_datas);
          else
            add_to_kept_rows(fetch_datas);
          continue;
        }
      }
      return NEED_SLEEP;
    }

    if (is_insert_select)
      add_to_ready_rows(fetch_datas);
    else
      add_to_kept_rows(fetch_datas);

    if ((unsigned long)kept_rows_size > max_spark_buffer_rows) {
      return NEED_SLEEP;
    }
  }
  if (is_insert_select) {
    ACE_Thread::join(l_handle);
    status = EXECUTE_STATUS_COMPLETE;
  }
  mutex.acquire();
  finished = true;
  cond.signal();
  mutex.release();
  return FINISHED;
}

void MySQLSparkNode::start_spark_thread() {
  int ret =
      ACE_Thread::spawn((ACE_THR_FUNC)start_spark_service, this,
                        THR_JOINABLE | THR_SCHED_DEFAULT, &l_id, &l_handle);
  if (ret == -1) {
    LOG_ERROR("Got error when create a new thread to run spark service.\n");
    throw Error("Got error when create a new thread to run spark service.");
  }

  bthread = plan->get_one_bthread();
  if (!bthread) {
    got_error = true;
    LOG_ERROR(
        "Fail to get a backend thread from pool, so the spark node stop.\n");
    throw Error(
        "Fail to get a backend thread from pool, so the spark node stop.");
  }
  thread_status = THREAD_CREATED;
  bthread->set_task(this);
}
#endif

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
/* class MySQLQueryForMulColumnNode*/
MySQLQueryForMulColumnNode::MySQLQueryForMulColumnNode(ExecutePlan *plan,
                                                       DataSpace *dataspace,
                                                       const char *sql)
    : MySQLDirectExecuteNode(plan, dataspace, sql), has_get_one_row(false) {
  this->name = "MySQLQueryForMulColumnNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
  col_nums = 0;
  is_direct_node = false;
}

void MySQLQueryForMulColumnNode::handle_send_client_packet(Packet *packet,
                                                           bool is_row_packet) {
  if (is_row_packet) {
    if (has_get_one_row) {
      throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_ROW_CODE], "21000",
                                   ERROR_ONE_ROW_CODE);
    }
    if (!has_get_one_row) {
      res_node->replace_sql.clear();
      res_node->row_columns.clear();
    }
    handle_one_row(packet);
    has_get_one_row = true;
  }
}

void MySQLQueryForMulColumnNode::handle_send_field_packet(Packet *packet) {
  MySQLColumnResponse col_resp(packet);
  col_resp.unpack();
  MySQLColumnType col_type = col_resp.get_column_type();
  col_types.push_back(col_type);
  col_nums++;
}

void MySQLQueryForMulColumnNode::handle_one_row(Packet *packet) {
  MySQLRowResponse row(packet);

  unsigned int i = 0;
  for (; i < col_nums; i++) {
    ResultType res = get_result_type_from_column_type(col_types[i]);
    string one_column;
    if (res == RESULT_TYPE_STRING) one_column.append("'");
    if (!row.field_is_null(i)) {
      uint64_t str_len = 0;
      const char *column_tmp = row.get_str(i, &str_len);
      CharsetType ctype = session->get_client_charset_type();
      append_column_value_to_replace_sql(one_column, column_tmp, str_len, res,
                                         ctype);
    } else
      one_column.append("NULL");
    if (res == RESULT_TYPE_STRING) one_column.append("'");
    res_node->row_columns.push_back(one_column);
  }
}

/* class MySQLQueryForOneColumnNode */
MySQLQueryForOneColumnNode::MySQLQueryForOneColumnNode(ExecutePlan *plan,
                                                       DataSpace *dataspace,
                                                       const char *sql,
                                                       bool only_one_row)
    : MySQLDirectExecuteNode(plan, dataspace, sql),
      has_get_one_row(false),
      col_type(MYSQL_TYPE_END),
      only_one_row(only_one_row) {
  this->name = "MySQLQueryForOneColumnNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  if (replace_empty_result_null)
    res_node->replace_sql = string("NULL");
  else
    res_node->replace_sql = string("select 1 from dual where 1=0");
  row_num = 0;
  cross_join_max_rows = (uint64_t)(
      session->get_session_option("max_cross_join_moved_rows").ulong_val);
  is_direct_node = false;
}
void MySQLQueryForOneColumnNode::handle_send_client_packet(Packet *packet,
                                                           bool is_row_packet) {
  if (is_row_packet) {
    if (only_one_row && has_get_one_row) {
      throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_ROW_CODE], "21000",
                                   ERROR_ONE_ROW_CODE);
    }
    if (!has_get_one_row) res_node->replace_sql.clear();
    handle_one_row(packet);
    has_get_one_row = true;
  }
}

void MySQLQueryForOneColumnNode::handle_send_field_packet(Packet *packet) {
  if (col_type == MYSQL_TYPE_END) {
    MySQLColumnResponse col_resp(packet);
    col_resp.unpack();
    col_type = col_resp.get_column_type();
  } else {
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_COLUMN_CODE],
                                 "21000", ERROR_ONE_COLUMN_CODE);
  }
}

void MySQLQueryForOneColumnNode::handle_one_row(Packet *packet) {
  row_num++;
  if (row_num > cross_join_max_rows) {
    LOG_ERROR(
        "Reach the max rows of [%u] for cross node join max moved rows.\n",
        row_num);
    throw ExecuteNodeError(
        "Reach the max row number of cross node join max moved rows.");
  }

  MySQLRowResponse row(packet);

  ResultType res = get_result_type_from_column_type(col_type);
  string &replace_sql = res_node->replace_sql;

  if (has_get_one_row) replace_sql.append(",");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");

  if (!row.field_is_null(0)) {
    uint64_t str_len = 0;
    const char *column_tmp = row.get_str(0, &str_len);
    CharsetType ctype = session->get_client_charset_type();
    append_column_value_to_replace_sql(replace_sql, column_tmp, str_len, res,
                                       ctype);
  } else
    replace_sql.append("NULL");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");
}

/* class MySQLQueryForOneColumnAggrNode */
MySQLQueryForOneColumnAggrNode::MySQLQueryForOneColumnAggrNode(
    ExecutePlan *plan, DataSpace *dataspace, const char *sql, bool get_min)
    : MySQLDirectExecuteNode(plan, dataspace, sql),
      col_type(MYSQL_TYPE_END),
      has_get_one_row(false),
      get_min(get_min ? 1 : -1) {
  this->name = "MySQLQueryForOneColumnAggrNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
  is_direct_node = false;
}

void MySQLQueryForOneColumnAggrNode::handle_send_client_packet(
    Packet *packet, bool is_row_packet) {
  if (is_row_packet) {
    if (!has_get_one_row) res_node->replace_sql.clear();
    handle_one_row(packet);
    has_get_one_row = true;
  }
}

void MySQLQueryForOneColumnAggrNode::handle_send_last_eof_packet(
    Packet *packet) {
  if (!has_get_one_row) return;
  ACE_UNUSED_ARG(packet);
  string &replace_sql = res_node->replace_sql;
  replace_sql.append("select ");

  ResultType res = get_result_type_from_column_type(col_type);

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");

  MySQLRowResponse row(&aggr_packet);
  if (!row.field_is_null(0)) {
    uint64_t str_len = 0;
    const char *column_tmp = row.get_str(0, &str_len);
    CharsetType ctype = session->get_client_charset_type();
    append_column_value_to_replace_sql(replace_sql, column_tmp, str_len, res,
                                       ctype);
  } else
    replace_sql.append("NULL");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");
}

void MySQLQueryForOneColumnAggrNode::handle_send_field_packet(Packet *packet) {
  if (col_type == MYSQL_TYPE_END) {
    MySQLColumnResponse col_resp(packet);
    col_resp.unpack();
    col_type = col_resp.get_column_type();
  } else {
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_COLUMN_CODE],
                                 "21000", ERROR_ONE_COLUMN_CODE);
  }
}

void MySQLQueryForOneColumnAggrNode::handle_one_row(Packet *row) {
  if (!has_get_one_row) {
    MySQLRowResponse row_p(row);
    uint64_t row_len = row->length();
    reset_packet_size(&aggr_packet, row_len + 9);
    row_p.pack(&aggr_packet);
  } else {
    MySQLRowResponse row1(&aggr_packet);
    MySQLRowResponse row2(row);
    int ret = MySQLColumnCompare::compare(&row1, &row2, 0, col_type,
                                          CHARSET_TYPE_OTHER, true);
    if (ret * get_min == 1) {
      uint64_t row_len = row->length();
      reset_packet_size(&aggr_packet, row_len + 9);
      row2.pack(&aggr_packet);
    }
  }
}

/* class MySQLQueryExistsNode */
MySQLQueryExistsNode::MySQLQueryExistsNode(ExecutePlan *plan,
                                           DataSpace *dataspace,
                                           const char *sql)
    : MySQLDirectExecuteNode(plan, dataspace, sql), has_get_one_row(false) {
  this->name = "MySQLQueryExistsNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
  is_direct_node = false;
}
void MySQLQueryExistsNode::handle_send_client_packet(Packet *packet,
                                                     bool is_row_packet) {
  ACE_UNUSED_ARG(packet);
  if (is_row_packet) {
    if (!has_get_one_row) {
      res_node->replace_sql = string("select 1 from dual where 1=1");
      has_get_one_row = true;
    }
  }
}

/* class MySQLSendToDBScaleNode */
MySQLSendToDBScaleNode::MySQLSendToDBScaleNode(ExecutePlan *plan)
    : MySQLSendNode(plan) {
  node_can_swap = false;
  this->name = "MySQLSendToDBScaleNode";
}

void MySQLSendToDBScaleNode::send_row(MySQLExecuteNode *ready_child) {
  ACE_UNUSED_ARG(ready_child);
  throw NotImplementedError();
}

/* class MySQLSendMulColumnToDBScaleNode*/
MySQLSendMulColumnToDBScaleNode::MySQLSendMulColumnToDBScaleNode(
    ExecutePlan *plan)
    : MySQLSendToDBScaleNode(plan) {
  this->name = "MySQLSendMulColumnToDBScaleNode";
  col_nums = 0;
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
  has_get_one_row = false;
}

void MySQLSendMulColumnToDBScaleNode::send_row(MySQLExecuteNode *ready_child) {
  Packet *row;
  if (row_map[ready_child]->size() > 1 || row_num != 0)
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_ROW_CODE], "21000",
                                 ERROR_ONE_ROW_CODE);
  if (!has_get_one_row && row_map[ready_child]->size() > 0) {
    res_node->replace_sql.clear();
    res_node->row_columns.clear();
    has_get_one_row = true;
  }
  while (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    handle_one_row(row);
    row_map[ready_child]->pop_front();
    delete row;
    row_num++;
  }
}

void MySQLSendMulColumnToDBScaleNode::send_header() {
  list<Packet *> *field_packets = get_field_packets();

  list<Packet *>::iterator it = field_packets->begin();
  for (; it != field_packets->end(); it++) {
    MySQLColumnResponse col_resp(*it);
    col_resp.unpack();
    MySQLColumnType col_type = col_resp.get_column_type();
    col_types.push_back(col_type);
  }
  col_nums = field_packets->size();
}

void MySQLSendMulColumnToDBScaleNode::handle_one_row(Packet *packet) {
  MySQLRowResponse row(packet);

  unsigned int i = 0;
  for (; i < col_nums; i++) {
    ResultType res = get_result_type_from_column_type(col_types[i]);
    string one_column;
    if (res == RESULT_TYPE_STRING) one_column.append("'");
    if (!row.field_is_null(i)) {
      uint64_t str_len = 0;
      const char *column_tmp = row.get_str(i, &str_len);
      CharsetType ctype = session->get_client_charset_type();
      append_column_value_to_replace_sql(one_column, column_tmp, str_len, res,
                                         ctype);
    } else
      one_column.append("NULL");
    if (res == RESULT_TYPE_STRING) one_column.append("'");
    res_node->row_columns.push_back(one_column);
  }
}

/* MySQLSendOneColumnToDBScaleNode */
MySQLSendOneColumnToDBScaleNode::MySQLSendOneColumnToDBScaleNode(
    ExecutePlan *plan, bool only_one_row)
    : MySQLSendToDBScaleNode(plan),
      col_type(MYSQL_TYPE_END),
      only_one_row(only_one_row),
      has_get_one_row(false) {
  this->name = "MySQLSendOneColumnToDBScaleNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  if (replace_empty_result_null)
    res_node->replace_sql = string("NULL");
  else
    res_node->replace_sql = string("select 1 from dual where 1=0");
}

void MySQLSendOneColumnToDBScaleNode::send_header() {
  list<Packet *> *field_packets = get_field_packets();
  if (field_packets->size() != 1) {
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_COLUMN_CODE],
                                 "21000", ERROR_ONE_COLUMN_CODE);
  }
  MySQLColumnResponse col_resp(field_packets->front());
  col_resp.unpack();
  col_type = col_resp.get_column_type();
}

void MySQLSendOneColumnToDBScaleNode::send_row(MySQLExecuteNode *ready_child) {
  Packet *row;
  if (only_one_row && (row_map[ready_child]->size() > 1 || row_num != 0))
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_ROW_CODE], "21000",
                                 ERROR_ONE_ROW_CODE);
  if (!has_get_one_row && row_map[ready_child]->size() > 0) {
    res_node->replace_sql.clear();
    has_get_one_row = true;
  }
  while (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    handle_one_row(row);
    row_map[ready_child]->pop_front();
    delete row;
    row_num++;
  }
}

void MySQLSendOneColumnToDBScaleNode::handle_one_row(Packet *packet) {
  MySQLRowResponse row(packet);

  ResultType res = get_result_type_from_column_type(col_type);
  string &replace_sql = res_node->replace_sql;

  if (row_num) replace_sql.append(",");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");

  if (!row.field_is_null(0)) {
    uint64_t str_len = 0;
    const char *column_tmp = row.get_str(0, &str_len);
    CharsetType ctype = session->get_client_charset_type();
    append_column_value_to_replace_sql(replace_sql, column_tmp, str_len, res,
                                       ctype);
  } else
    replace_sql.append("NULL");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");
}

/* class MySQLSendOneColumnAggrToDBScaleNode */
MySQLSendOneColumnAggrToDBScaleNode::MySQLSendOneColumnAggrToDBScaleNode(
    ExecutePlan *plan, bool get_min)
    : MySQLSendToDBScaleNode(plan),
      col_type(MYSQL_TYPE_END),
      get_min(get_min ? 1 : -1),
      has_get_one_row(false) {
  this->name = "MySQLSendOneColumnAggrToDBScaleNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
}

void MySQLSendOneColumnAggrToDBScaleNode::send_eof() {
  if (!has_get_one_row) return;
  string &replace_sql = res_node->replace_sql;
  replace_sql.append("select ");

  ResultType res = get_result_type_from_column_type(col_type);

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");

  MySQLRowResponse row(&aggr_packet);
  if (!row.field_is_null(0)) {
    uint64_t str_len = 0;
    const char *column_tmp = row.get_str(0, &str_len);
    CharsetType ctype = session->get_client_charset_type();
    append_column_value_to_replace_sql(replace_sql, column_tmp, str_len, res,
                                       ctype);
  } else
    replace_sql.append("NULL");

  if (res == RESULT_TYPE_STRING) replace_sql.append("'");
}

void MySQLSendOneColumnAggrToDBScaleNode::send_header() {
  list<Packet *> *field_packets = get_field_packets();
  if (field_packets->size() != 1) {
    throw dbscale::sql::SQLError(dbscale_err_msg[ERROR_ONE_COLUMN_CODE],
                                 "21000", ERROR_ONE_COLUMN_CODE);
  }
  MySQLColumnResponse col_resp(field_packets->front());
  col_resp.unpack();
  col_type = col_resp.get_column_type();
}

void MySQLSendOneColumnAggrToDBScaleNode::send_row(
    MySQLExecuteNode *ready_child) {
  if (!has_get_one_row && row_map[ready_child]->size() > 0) {
    res_node->replace_sql.clear();
    has_get_one_row = true;
  }
  Packet *row;
  if (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    handle_one_row(row);
    row_map[ready_child]->pop_front();
    delete row;
    row_num++;
  }
}

void MySQLSendOneColumnAggrToDBScaleNode::handle_one_row(Packet *row) {
  if (!row_num) {
    MySQLRowResponse row_p(row);
    uint64_t row_len = row->length();
    reset_packet_size(&aggr_packet, row_len + 9);
    row_p.pack(&aggr_packet);
  } else {
    MySQLRowResponse row1(&aggr_packet);
    MySQLRowResponse row2(row);
    int ret = MySQLColumnCompare::compare(&row1, &row2, 0, col_type,
                                          CHARSET_TYPE_OTHER, true);
    if (ret * get_min == 1) {
      uint64_t row_len = row->length();
      reset_packet_size(&aggr_packet, row_len + 9);
      row2.pack(&aggr_packet);
    }
  }
}

/* MySQLSendExistsToDBScaleNode */
MySQLSendExistsToDBScaleNode::MySQLSendExistsToDBScaleNode(ExecutePlan *plan)
    : MySQLSendToDBScaleNode(plan) {
  this->name = "MySQLSendExistsToDBScaleNode";
#ifdef DEBUG
  ACE_ASSERT(plan->get_sub_query_result_node());
#endif
  res_node = plan->get_sub_query_result_node();
  res_node->replace_sql = string("select 1 from dual where 1=0");
  set_res_node = false;
}

void MySQLSendExistsToDBScaleNode::send_row(MySQLExecuteNode *ready_child) {
  Packet *row;
  while (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    if (!set_res_node) {
      res_node->replace_sql = string("select 1 from dual where 1=1");
      set_res_node = true;
    }
    row_map[ready_child]->pop_front();
    delete row;
    row_num++;
  }
}

/* class MySQLIntoOutfileNode */
MySQLIntoOutfileNode::MySQLIntoOutfileNode(ExecutePlan *plan)
    : MySQLSendNode(plan) {
  this->name = "MySQLIntoOutfileNode";
  affect_rows = 0;
  into_outfile_item *into_outfile = plan->statement->get_into_outfile();
  is_local = into_outfile->is_local;
  filename = into_outfile->filename;

  node_can_swap = false;
  row_buffer = new char[DEFAULT_BUFFER_SIZE];
}

void MySQLIntoOutfileNode::prepare_fifo_or_load() {
  if (plan->statement->insert_select_via_fifo()) {
    LOG_DEBUG("external INSERT SELECT via fifo start to open fifo\n");
    open_fifo(plan);
    LOG_DEBUG("external INSERT SELECT via fifo end to open fifo\n");
  } else if (plan->statement->is_load_insert_select()) {
    load_insert_select();
  } else {
    param.set_got_error(false);
    param.set_external_load_flag(UNDEFINE);
    pipe_fd = 0;
    if (!is_local) {
      file.open(filename);
    }
  }
}

MySQLIntoOutfileNode::~MySQLIntoOutfileNode() { delete[] row_buffer; }

void *monitor_worker_via_fifo(void *arg) {
  spawn_param *param = (spawn_param *)arg;
  if (param->fd) {
    ExternalLoadResultHandler exp_handler;
    try {
      exp_handler.handler_expect_result(param->fd, param);
      const char *inserted_rows = exp_handler.get_inserted_rows();
      param->insert_rows = atoll(inserted_rows);
    } catch (...) {
      param->set_got_error(true);
    }
    if (param->fd) {
      pclose(param->fd);
    }
  }
  return NULL;
}

void *load_insert_select_exec(void *arg) {
  MySQLIntoOutfileNode *node = (MySQLIntoOutfileNode *)arg;
  char fields_term =
      node->plan->session->get_session_option("load_insert_select_fields_term")
          .char_val[0];
  char lines_term =
      node->plan->session->get_session_option("load_insert_select_lines_term")
          .char_val[0];
  string load_sql("LOAD DATA INFILE '");
  load_sql.append(node->get_filename());
  load_sql.append("' INTO TABLE ");
  load_sql.append(node->plan->statement->get_insert_select_modify_schema());
  load_sql.append(".");
  load_sql.append(node->plan->statement->get_insert_select_modify_table());
  load_sql.append(" FIELDS TERMINATED BY '");
  load_sql.append(1, fields_term);
  load_sql.append("' ENCLOSED BY '\"' LINES TERMINATED BY '");
  load_sql.append(1, lines_term);
  load_sql.append("'");

  LOG_DEBUG("LOAD DATA for INSERT SELECT sql [%s]\n", load_sql.c_str());

  Statement *new_stmt = NULL;
  ExecutePlan *new_plan = NULL;
  try {
    Parser *parser = MySQLParser::instance();
    new_stmt = parser->parse(
        load_sql.c_str(),
        allow_dot_in_ident);  // use global value of allow_dot_in_ident
    new_stmt->set_default_schema(node->plan->statement->get_schema());
    new_stmt->set_load_insert_select(true);
    new_plan = (ExecutePlan *)node->handler->get_execute_plan(new_stmt);
    new_stmt->generate_execution_plan(new_plan);
    new_plan->execute();
  } catch (...) {
    LOG_ERROR("Load insert select error in load execution.\n");
    node->set_got_error();
  }
  if (new_stmt) {
    new_stmt->free_resource();
    delete new_stmt;
    new_stmt = NULL;
  }
  if (new_plan) {
    delete new_plan;
    new_plan = NULL;
  }
  return NULL;
}

void MySQLIntoOutfileNode::load_insert_select() {
  param.set_got_error(false);
  param.set_external_load_flag(UNDEFINE);
  pipe_fd = 0;
  int ret =
      ACE_Thread::spawn((ACE_THR_FUNC)load_insert_select_exec, this,
                        THR_JOINABLE | THR_SCHED_DEFAULT, &l_id, &l_handle);
  if (ret == -1) {
    LOG_ERROR("Error when execute load.\n");
    throw HandlerError("Error when execute load.");
  }
  file.open(filename);
}

void MySQLIntoOutfileNode::open_fifo(ExecutePlan *plan) {
  string insert_select_via_fifo_cmd;
  string script = plan->statement->get_local_load_script();
  if (script[0] != '/') {
    insert_select_via_fifo_cmd.append("./");
  }
  insert_select_via_fifo_cmd.append(script);
  insert_select_via_fifo_cmd.append(" ");
  insert_select_via_fifo_cmd.append(
      plan->statement->get_insert_select_modify_schema());
  insert_select_via_fifo_cmd.append(" ");
  insert_select_via_fifo_cmd.append(
      plan->statement->get_insert_select_modify_table());
  insert_select_via_fifo_cmd.append(" , \\\" ");
  insert_select_via_fifo_cmd.append(filename);
  LOG_DEBUG("full command [%s].\n", insert_select_via_fifo_cmd.c_str());
  fd = popen(insert_select_via_fifo_cmd.c_str(), "r");
  if (fd == NULL) {
    fprintf(stderr, "execute command failed");
    LOG_ERROR("Fail to execute command %s.\n",
              insert_select_via_fifo_cmd.c_str());
    throw HandlerError("Fail to execute command.");
  }

  param.set_got_error(false);
  param.insert_rows = 0;
  param.fd = fd;
  param.set_external_load_flag(UNDEFINE);
  int ret =
      ACE_Thread::spawn((ACE_THR_FUNC)monitor_worker_via_fifo, &param,
                        THR_JOINABLE | THR_SCHED_DEFAULT, &t_id, &t_handle);
  if (ret == -1) {
    LOG_ERROR("Error when execute command %s.\n",
              insert_select_via_fifo_cmd.c_str());
    throw HandlerError("Error when execute command.");
  }
  file.open(filename);
  while (param.get_external_load_flag() == UNDEFINE) {
    LOG_DEBUG("Waiting for external load script report load file status.\n");
    ACE_OS::sleep(1);
  }
  if (param.get_external_load_flag() == FILE_NOT_OPEN) {
    file.close();
    ACE_Thread::join(t_handle);
    throw ExecuteNodeError(
        "Failed to execute statement, fail to open file, check log for more "
        "information.");
  }
}

void MySQLIntoOutfileNode::send_header() {
  string catalog_name;
  string schema_name;
  string table_name;
  uint32_t column_len = 0;
  list<Packet *> *field_packets = get_field_packets();
  for (std::list<Packet *>::iterator it = field_packets->begin();
       it != field_packets->end(); it++) {
    MySQLColumnResponse column_response(*it);
    column_response.unpack();
    is_column_number.push_back(column_response.is_number());
    column_len += column_response.get_column_length();
    if (it == field_packets->begin()) {
      catalog_name = column_response.get_catalog();
      schema_name = column_response.get_schema();
      table_name = column_response.get_table();
    }
  }
  // rebuild a column, and send to client
  if (is_local) {
    Packet res_header_packet;
    MySQLResultSetHeaderResponse result_set_header(1, 0);
    result_set_header.pack(&res_header_packet);
    handler->send_mysql_packet_to_client_by_buffer(&res_header_packet);
    MySQLColumnResponse my_col(catalog_name.c_str(), schema_name.c_str(),
                               table_name.c_str(), table_name.c_str(),
                               "into-outfile-col", "into-outfile-col", 8,
                               column_len + 1, MYSQL_TYPE_VAR_STRING, 0, 0, 0);
    Packet col_packet;
    my_col.pack(&col_packet);
    handler->send_mysql_packet_to_client_by_buffer(&col_packet);
    Packet *eof_packet = get_eof_packet();
    handler->deal_autocommit_with_ok_eof_packet(eof_packet);
    handler->send_mysql_packet_to_client_by_buffer(eof_packet);
    handler->flush_net_buffer();
  }
}

void MySQLIntoOutfileNode::send_row(MySQLExecuteNode *ready_child) {
  into_outfile_item *into_outfile = plan->statement->get_into_outfile();
  check_lines_term_null(into_outfile);
  Packet *row;
  while (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    write_into_outfile(row, into_outfile, file, &param);
    affect_rows++;
    if (is_local) {
      handler->send_to_client(row);
    } else if (param.has_got_error() ||
               param.get_external_load_flag() == FINISH) {
      status = EXECUTE_STATUS_COMPLETE;
      file.close();
      if (plan->statement->insert_select_via_fifo()) {
        ACE_Thread::join(t_handle);
      }
      throw ExecuteNodeError(
          "Got error when execute statement, check log for more information.");
    }

    row_map[ready_child]->pop_front();
    delete row;
  }
}

void MySQLIntoOutfileNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        prepare_fifo_or_load();
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
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
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        if (send_header_flag) {
          send_header();
          send_header_flag = false;
        }
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE: {
        if (!is_local) {
          file.close();
        }
        if (plan->statement->insert_select_via_fifo()) {
          LOG_DEBUG(
              "external INSERT SELECT via fifo start wait monitor thread\n");
          ACE_Thread::join(t_handle);
          LOG_DEBUG(
              "external INSERT SELECT via fifo end wait monitor thread\n");

          if (param.has_got_error()) {
            LOG_ERROR(
                "Got error when execute INSERT SELECT statement, %Q rows "
                "inserted.\n",
                param.insert_rows);
            throw ExecuteNodeError(
                "Got error when execute INSERT SELECT statement, "
                "please check log for more information.");
          } else {
            if (!session->is_call_store_procedure())
              send_ok_packet_to_client(handler, param.insert_rows, 0);
#ifdef DEBUG
            LOG_DEBUG(
                "MySQLIntoOutfileNode %@ for external INSERT SELECT cost %d "
                "ms\n",
                this, node_cost_time);
#endif
          }
        } else if (plan->statement->is_load_insert_select()) {
          LOG_DEBUG("Wait for load thread.\n");
          ACE_Thread::join(l_handle);
          LOG_DEBUG("End wait for load thread.\n");
          Packet *result_packet = session->get_result_packet();
          if (param.has_got_error() || !result_packet) {
            LOG_ERROR("Got error when execute INSERT SELECT statement.\n",
                      param.insert_rows);
            throw ExecuteNodeError(
                "Got error when execute INSERT SELECT statement, "
                "please check log for more information.");
          }
          handler->send_to_client(result_packet);
          delete result_packet;
          session->set_result_packet(NULL);
        } else if (!is_local) {
          if (!session->is_call_store_procedure())
            send_ok_packet_to_client(handler, affect_rows, 0);
#ifdef DEBUG
          LOG_DEBUG("MySQLIntoOutfileNode %@ cost %d ms\n", this,
                    node_cost_time);
#endif
        } else {
          send_eof();
          flush_net_buffer();
        }
        break;
      }
      default:
        break;
    }
  }
}

/* class MySQLSelectIntoNode */
MySQLSelectIntoNode::MySQLSelectIntoNode(ExecutePlan *plan)
    : MySQLSendNode(plan) {
  this->name = "MySQLSelectIntoNode";
  affect_rows = 0;
  select_into_item *select_into = plan->statement->get_select_into();
  generate_select_into_vec(select_into);
  node_can_swap = false;
}

void MySQLSelectIntoNode::generate_select_into_vec(
    select_into_item *select_into) {
  name_item *into_list = select_into->into_list;
  name_item *head = into_list;
  do {
    bool is_uservar = into_list->is_uservar;
    string name = into_list->name;
    if (is_uservar) {
      boost::to_upper(name);
    }
    pair<bool, string> is_uservar_pair(is_uservar, name);
    select_into_vec.push_back(is_uservar_pair);
    into_list = into_list->next;
  } while (into_list != head);
}

void MySQLSelectIntoNode::write_into_name_list(Packet *row) {
  Session *session = plan->session;
  MySQLRowResponse row_response(row);
  unsigned int fields_num = field_is_number_vec.size();

  for (unsigned int i = 0; i != fields_num; i++) {
    if (!select_into_vec[i].first) {
      continue;
    }

    string uservar_name = select_into_vec[i].second;
    if (row_response.field_is_null(i)) {
      write_result_to_uservar(session, uservar_name, NULL);
      continue;
    }

    uint64_t length;
    const char *str = row_response.get_str(i, &length);
    write_result_to_uservar(session, uservar_name, str, length,
                            field_is_number_vec[i]);
  }
}

void MySQLSelectIntoNode::send_header() {
  list<Packet *> *field_packets = get_field_packets();
  for (std::list<Packet *>::iterator it = field_packets->begin();
       it != field_packets->end(); it++) {
    MySQLColumnResponse column_response(*it);
    column_response.unpack();
    field_is_number_vec.push_back(column_response.is_number());
  }
}

void MySQLSelectIntoNode::send_row(MySQLExecuteNode *ready_child) {
  Packet *row;
  while (!row_map[ready_child]->empty()) {
    row = row_map[ready_child]->front();
    affect_rows++;
    row_map[ready_child]->pop_front();
    try {
      if (affect_rows > 1) {
        throw dbscale::sql::SQLError(
            dbscale_err_msg[ERROR_SELECT_INTO_ONE_ROW_CODE], "42000",
            ERROR_SELECT_INTO_ONE_ROW_CODE);
      } else if (field_is_number_vec.size() != select_into_vec.size()) {
        throw dbscale::sql::SQLError(
            dbscale_err_msg[ERROR_SELECT_INTO_DIFF_COLUMNS_CODE], "21000",
            ERROR_SELECT_INTO_DIFF_COLUMNS_CODE);
      } else {
        write_into_name_list(row);
      }
      if (statement->get_select_uservar_flag()) {
        set_select_uservar_by_result(
            row, field_is_number_vec,
            *(plan->statement->get_select_uservar_vec()),
            plan->statement->get_select_field_num(), plan->session);
      }
      delete row;
      row = NULL;
    } catch (dbscale::sql::SQLError &e) {
      delete row;
      row = NULL;
      throw e;
    }
  }
}

void MySQLSelectIntoNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
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
        try {
          handle_children();
        } catch (...) {
          status = EXECUTE_STATUS_COMPLETE;
          throw;
        }
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        if (send_header_flag) {
          send_header();
          send_header_flag = false;
        }
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
        if (!session->is_call_store_procedure())
          send_ok_packet_to_client(handler, affect_rows, 0);
#ifdef DEBUG
        LOG_DEBUG("MySQLIntoOutfileNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        break;
    }
  }
}

void MySQLIntoOutfileNode::clean() {
  MySQLSendNode::clean();
  if (plan->statement->insert_select_via_fifo() ||
      plan->statement->is_load_insert_select()) {
    for (int i = 0; i < 3; i++) {  // delete fifo, try 3 times.
      if (!remove(filename)) {     // return 0, delete fifo successful.
        break;
      }
      if (i == 2) {  // try 3 times but failed.
        LOG_ERROR("Error deleting fifo file.\n");
      }
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

/* class MySQLLimitNode */

MySQLLimitNode::MySQLLimitNode(ExecutePlan *plan, long offset, long num)
    : MySQLInnerNode(plan), offset(offset), num(num), row_count(0) {
  this->name = "MySQLLimitNode";
  node_can_swap = session->is_may_backend_exec_swap_able();
}

int MySQLLimitNode::handle_child(MySQLExecuteNode *child) {
  Packet *packet = NULL;
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    if (row_count < offset) {
      delete packet;
      row_count++;
      continue;
    }
    if (row_count >= offset + num) {
      delete packet;
      return 1;
    }
    ready_rows->push_back(packet);
    row_count++;
  }
  return 0;
}

void MySQLLimitNode::handle_children() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty())
      if (handle_child(*it)) break;
  }
  LOG_DEBUG(
      "Limit node %@, row_count %d, ready_rows %d"
      "after handle_children.\n",
      this, row_count, ready_rows->size());
}

void MySQLLimitNode::execute() {
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
        if (row_count < offset + num) handle_children();
        if (ready_rows->empty() &&
            (all_children_finished || row_count >= offset + num)) {
          status = EXECUTE_STATUS_COMPLETE;
        } else if (row_count >= offset + num) {
          status = EXECUTE_STATUS_HANDLE;
        } else {
          status = EXECUTE_STATUS_FETCH_DATA;
        }
#ifdef DEBUG
        node_end_timing();
#endif
        if (!ready_rows->empty()) return;
        break;
      case EXECUTE_STATUS_BEFORE_COMPLETE:
        status = EXECUTE_STATUS_COMPLETE;
      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLLimitNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

/* class MySQLKillNode */

MySQLKillNode::MySQLKillNode(ExecutePlan *plan, int cluster_id, uint32_t kid)
    : MySQLExecuteNode(plan), kid(kid), cluster_id(cluster_id) {
  this->name = "MySQLKillNode";
}

void MySQLKillNode::execute() {
#ifdef DEBUG
  ACE_ASSERT(kid != handler->get_session()->get_thread_id());
#endif
  Session *h = NULL;
#ifndef CLOSE_MULTIPLE
  Backend *backend = Backend::instance();
  if (multiple_mode && cluster_id != ERROR_CLUSTER_ID &&
      cluster_id != backend->get_cluster_id()) {
    MultipleManager *mul = MultipleManager::instance();
    if (mul->get_is_cluster_master()) {
      string kill_sql("KILL ");
      char s_cluster_id[20];
      sprintf(s_cluster_id, "%d", cluster_id);
      kill_sql.append(s_cluster_id);
      kill_sql.append(" ");
      char s_kid[20];
      sprintf(s_kid, "%d", kid);
      kill_sql.append(s_kid);
      mul->execute_master_query(kill_sql.c_str(), cluster_id);
    }
    Packet ok_packet;
    MySQLOKResponse ok(0, 0);
    ok.pack(&ok_packet);
    handler->deal_autocommit_with_ok_eof_packet(&ok_packet);
    handler->record_affected_rows(&ok_packet);
    if (!plan->session->is_call_store_procedure()) {
      handler->send_to_client(&ok_packet);
    }

    status = EXECUTE_STATUS_COMPLETE;
    return;
  }
#endif
  try {
    h = driver->get_session_by_thread_id_for_kill(kid);
    if (!h) {
      throw FindKillThreadFail();
    }
    if (h->is_executing_spark()) {
#ifndef DBSCALE_DISABLE_SPARK
      string job_id = h->get_cur_spark_job_id();
      SparkKillParameter spark_kill_param;
      spark_kill_param.job_id = job_id;
      SparkReturnValue return_value = kill_spark_service(spark_kill_param);
      if (!return_value.success) {
        throw Error(return_value.error_string.c_str());
      }
#endif
    } else {
      h->enable_prepare_kill();
      if (h->get_transfer_rule_manager()) {
        handle_federated_situation(h);
      }
      driver->prepare_session_kill(h);
      h->killall_using_conns();
      driver->finish_session_kill(h);
    }
  } catch (FindKillThreadFail &e) {
    LOG_WARN("Fail to find the kill thread %d.\n", kid);
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError("Fail to find kill thread id.");
  } catch (ThreadKillFailed &e) {
    LOG_ERROR("Fail to kill thread %d, due to %s.\n", kid, e.what());
    status = EXECUTE_STATUS_COMPLETE;
    string message("Fail to kill thread due to ");
    message += e.what();
    throw ExecuteNodeError(message.c_str());
  } catch (Exception &e) {
    LOG_ERROR("Got unexpect expection for kill node.\n");
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError(e.what());
  }

  Packet ok_packet;
  MySQLOKResponse ok(0, 0);
  ok.pack(&ok_packet);
  handler->deal_autocommit_with_ok_eof_packet(&ok_packet);
  handler->record_affected_rows(&ok_packet);
  if (!plan->session->is_call_store_procedure()) {
    handler->send_to_client(&ok_packet);
  }

  status = EXECUTE_STATUS_COMPLETE;
}

bool MySQLKillNode::handle_federated_situation(Session *h) {
  bool ret = false;
  while (!ret) {
    LOG_DEBUG("Waiting for federated session finished.\n");
    bool session_available = driver->check_session_available_aquire(h);
    if (session_available) {
      if (h->federated_session_finished())
        ret = true;
      else {
        int killed_session_count = 0;
        try {
          killed_session_count =
              h->get_transfer_rule_manager()->killall_transfer_rule_sessions();
        } catch (...) {
          driver->release_session_mutex();
          throw;
        }
        h->set_federated_num(killed_session_count);
      }
      driver->release_session_mutex();
    } else {
      ret = true;
    }
    if (h->federated_session_finished()) break;
    ACE_OS::sleep(2);
  }
  return ret;
}

/* class MySQLOKNode */

MySQLOKNode::MySQLOKNode(ExecutePlan *plan) : MySQLInnerNode(plan) {
  this->name = "MySQLOKNode";
}

void MySQLOKNode::execute() {
  init_row_map();
  MySQLExecuteNode *node = children.front();
  node->execute();
  node->notify_parent();
  handler->deal_with_metadata_execute(
      plan->statement->get_stmt_node()->type, plan->statement->get_sql(),
      session->get_schema(), plan->statement->get_stmt_node());

  if (!plan->session->is_call_store_procedure() &&
      !plan->statement->is_cross_node_join() &&
      !plan->statement->is_union_table_sub() &&
      !plan->session->get_is_silence_ok_stmt()) {
    Packet *packet = row_map[node]->front();
    handler->deal_autocommit_with_ok_eof_packet(packet);
    handler->record_affected_rows(packet);
    if (!plan->get_migrate_tool()) {
      if (plan->session->get_has_more_result()) {
        rebuild_ok_with_has_more_flag(packet, driver);
        LOG_DEBUG(
            "For multiple stmt, the ok packet of middle stmt should be with"
            "flag has_more_result.\n");
      }
      handler->send_to_client(packet);
    }
    LOG_DEBUG("OK Node send packet %@ to client.\n", packet);
  } else
    LOG_DEBUG(
        "In store procedure call or cross node join or union table subquery, "
        "so skip the sending"
        " of ok packet in ok node with call [%d] cross join [%d] union [%d] "
        "sliense [%d].\n",
        plan->session->is_call_store_procedure() ? 1 : 0,
        plan->statement->is_cross_node_join() ? 1 : 0,
        plan->statement->is_union_table_sub() ? 1 : 0,
        plan->session->get_is_silence_ok_stmt() ? 1 : 0);
  status = EXECUTE_STATUS_COMPLETE;
}

/* class MySQLSlaveDBScaleErrorNode */
void MySQLSlaveDBScaleErrorNode::execute() {
  try {
    Backend *backend = Backend::instance();
    if (backend->get_need_send_stop_slave_flag()) {
      LOG_DEBUG("slave dbscale server need stop slave\n");
      DataSpace *catalog = backend->get_catalog();
      catalog->execute_one_modify_sql("stop slave");
      backend->set_need_send_stop_slave_flag(false);
    }

    LOG_DEBUG(
        "this is a dbscale server and 'slave-dbscale-mode' is off,so it is "
        "denied to be a slave\n");
    Packet error_packet;
    MySQLErrorResponse error(
        9001, "'slave-dbscale-mode' is off, dbscale is denied to be a slave",
        "", 0);
    error.pack(&error_packet);
    handler->send_to_client(&error_packet);
  } catch (exception &e) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR("Execute Node fail in MySQLSlaveDBScaleErrorNode due to [%s].\n",
              e.what());
    throw;
  }
  status = EXECUTE_STATUS_COMPLETE;
}

/* class MySQLExprCalculateNode */
MySQLExprCalculateNode::MySQLExprCalculateNode(
    ExecutePlan *plan, list<SelectExprDesc> &select_expr_list)
    : MySQLInnerPipeNode(plan) {
  this->name = "MySQLExprCalculateNode";
  this->expr_list = select_expr_list;
  inited_expr_list = false;
  column_num = 0;
}

void MySQLExprCalculateNode::handle_child(MySQLExecuteNode *child) {
  Packet *row;
  init_expr_list();
  while (!row_map[child]->empty()) {
    row = row_map[child]->front();
    row_map[child]->pop_front();
    row = reset_packet_value(row);
    ready_rows->push_back(row);
  }
}
void MySQLExprCalculateNode::calculate_expr(SelectExprDesc *desc) {
  desc->is_null = false;
  desc->column_data.clear();
  ExpressionValue expr_value;
  desc->expr->get_expression_value(&expr_value);
  switch (expr_value.value_type) {
    case RESULT_TYPE_NULL: {
      desc->is_null = true;
    } break;
    case RESULT_TYPE_NUM: {
      char column[GMP_N_DIGITS + 5];
      char formator[10];
      sprintf(formator, "%%.%dFf", expr_value.gmp_value.deci_num);
      int len = gmp_sprintf(column, formator, expr_value.gmp_value.value);
      column[len] = '\0';
      desc->column_data.append(column);
    } break;
    case RESULT_TYPE_BOOL: {
      if (expr_value.bool_value)
        desc->column_data.append("1");
      else
        desc->column_data.append("0");
    } break;
    case RESULT_TYPE_STRING: {
      desc->column_data.append(expr_value.str_value);
    } break;
    default:
      LOG_ERROR("Unsupport result type for MySQLExprCalculateNode.\n");
      throw ExecuteNodeError(
          "Unsupport result type for MySQLExprCalculateNode.");
  }
}
Packet *MySQLExprCalculateNode::reset_packet_value(Packet *row) {
  MySQLRowResponse row_res(row);
  int column_len = 0;
  char *column_data = NULL;
  Packet *new_row = Backend::instance()->get_new_packet(row_packet_size);
  new_row->wr_ptr(new_row->base() + PACKET_HEADER_SIZE);

  list<SelectExprDesc>::iterator it;
  list<FieldExpression *>::iterator fe_it;
  for (fe_it = field_expr_list.begin(); fe_it != field_expr_list.end();
       fe_it++) {
    (*fe_it)->set_row(row);
  }

  unsigned int need_len = 1;  // for terminate '\0'
  it = expr_list.begin();
  fe_it = field_expr_list.begin();
  for (unsigned int i = 0; i < column_num; i++) {
    row_res.set_current(i);
    if (it != expr_list.end() && it->index == i) {
      need_len = need_len + 9 + it->column_data.size();
      it++;
    } else {
      need_len = need_len + 9 + row_res.get_current_length();
    }
    if (fe_it != field_expr_list.end() &&
        (unsigned int)((*fe_it)->column_index) == i) {
      (*fe_it)->is_null = row_res.field_is_null();
    }
    row_res.move_next();
  }
  for (it = expr_list.begin(); it != expr_list.end(); it++) {
    calculate_expr(&(*it));
  }
  unsigned int remain_len =
      new_row->size() - (new_row->wr_ptr() - new_row->base());
  if (remain_len < need_len) {
    uint64_t resize_len = need_len + new_row->wr_ptr() - new_row->base();
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              new_row->size(), resize_len);
    new_row->size(resize_len);
  }
  it = expr_list.begin();
  for (unsigned int i = 0; i < column_num; i++) {
    row_res.set_current(i);
    if (it != expr_list.end() && it->index == i) {
      if (it->is_null) {
        new_row->packchar(
            '\373');  //'\373' represents that the field value is NULL
      } else {
        new_row->pack_lenenc_int(it->column_data.size());
        new_row->packdata(it->column_data.c_str(), it->column_data.size());
      }
      it++;
    } else {
      if (row_res.field_is_null())
        new_row->packchar(
            '\373');  //'\373' represents that the field value is NULL
      else {
        column_len = row_res.get_current_length();
        column_data = row_res.get_current_data();
        new_row->pack_lenenc_int(column_len);
        new_row->packdata(column_data, column_len);
      }
    }
    row_res.move_next();
  }
  size_t load_length = new_row->length() - PACKET_HEADER_SIZE;
  pack_header(new_row, load_length);
  delete row;
  return new_row;
}
void get_expression_type(list<Packet *> *field_packets,
                         FieldExpression *expression, ResultType &result_type);

void init_expr_low(Expression *expr, STMT_EXPR_TYPE stmt_expr_type,
                   list<Packet *> *field_packets,
                   list<FieldExpression *> &field_expression_list) {
  expr_type expr_type = expr->type;
  switch (expr_type) {
    case EXPR_EQ:
    case EXPR_GR:
    case EXPR_LESS:
    case EXPR_GE:
    case EXPR_LESSE:
    case EXPR_NE:
    case EXPR_EQ_N:
    case EXPR_AND:
    case EXPR_OR:
    case EXPR_XOR:
    case EXPR_ELSE: {
      BoolBinaryExpression *bi_expr = (BoolBinaryExpression *)expr;
      init_expr_low(bi_expr->left, stmt_expr_type, field_packets,
                    field_expression_list);
      init_expr_low(bi_expr->right, stmt_expr_type, field_packets,
                    field_expression_list);
    } break;

    case EXPR_IF_COND:
    case EXPR_WHEN_THEN: {
      ConditionExpression *cond_expr = (ConditionExpression *)expr;
      init_expr_low(cond_expr->left, stmt_expr_type, field_packets,
                    field_expression_list);
      init_expr_low(cond_expr->right, stmt_expr_type, field_packets,
                    field_expression_list);
    } break;

    case EXPR_MINUS: {
      ArithmeticUnaryExpression *un_expr = (ArithmeticUnaryExpression *)expr;
      init_expr_low(un_expr->right, stmt_expr_type, field_packets,
                    field_expression_list);
    } break;

    case EXPR_ADD:
    case EXPR_SUB:
    case EXPR_MUL:
    case EXPR_DIV: {
      ArithmeticBinaryExpression *bi_expr = (ArithmeticBinaryExpression *)expr;
      init_expr_low(bi_expr->left, stmt_expr_type, field_packets,
                    field_expression_list);
      init_expr_low(bi_expr->right, stmt_expr_type, field_packets,
                    field_expression_list);
    } break;
    case EXPR_IS:
    case EXPR_IS_NOT: {
      TruthExpression *t_expr = (TruthExpression *)expr;
      init_expr_low(t_expr->left, stmt_expr_type, field_packets,
                    field_expression_list);
      init_expr_low(t_expr->right, stmt_expr_type, field_packets,
                    field_expression_list);
    } break;

    case EXPR_IF:
    case EXPR_IFNULL:
    case EXPR_CASE: {
      if (stmt_expr_type == FIELD_EXPR) {
        TerCondExpression *ter_cond_expr = (TerCondExpression *)expr;
        expr_list_item *item = ter_cond_expr->list_expr->expr_list_head;
        do {
          init_expr_low(item->expr, stmt_expr_type, field_packets,
                        field_expression_list);
          item = item->next;
        } while (item != ter_cond_expr->list_expr->expr_list_head);
        if (ter_cond_expr->else_expr)
          init_expr_low(ter_cond_expr->else_expr, stmt_expr_type, field_packets,
                        field_expression_list);
      } else {
        throw NotImplementedError("Filter expression is too complex.");
      }
    } break;

    case EXPR_FIELD: {
      ResultType result_type;
      FieldExpression *field_expr = (FieldExpression *)expr;
      if (field_expr->column_index < 0) {
        field_expr->column_index += field_packets->size();
      }
      get_expression_type(field_packets, field_expr, result_type);
      field_expr->result_type = result_type;
      field_expression_list.push_back(field_expr);
    } break;

    case EXPR_INT:
    case EXPR_STRING:
    case EXPR_FLOAT:
    case EXPR_NULL:
      break;

    default:
      throw NotImplementedError("expression is too complex");
  }
}

void MySQLExprCalculateNode::init_expr_list() {
  if (!inited_expr_list) {
    list<SelectExprDesc>::iterator it;
    for (it = expr_list.begin(); it != expr_list.end(); it++) {
      init_expr_low(it->expr, FIELD_EXPR, get_field_packets(), field_expr_list);
    }
    column_num = get_field_packets()->size();
    inited_expr_list = true;
  }
}

/* class MySQLConnectByNode */
MySQLConnectByNode::MySQLConnectByNode(ExecutePlan *plan,
                                       ConnectByDesc connect_by_desc)
    : MySQLInnerPipeNode(plan) {
  this->name = "MySQLConnectByNode";
  inited_column_index = false;
  where_index = connect_by_desc.where_index;
  start_index = connect_by_desc.start_index;
  prior_index = connect_by_desc.prior_index;
  recur_index = connect_by_desc.recur_index;
  row_id = 0;
  max_result_num =
      session->get_session_option("max_connect_by_result_num").uint_val;
  loop_flag = new bitset<MAX_CONNECT_BY_CACHED_ROWS>();
  ignore_flag = new bitset<MAX_CONNECT_BY_CACHED_ROWS>();
}

void MySQLConnectByNode::do_clean() {
  unsigned int i = 0;
  // the clean may cost a long time,
  // witch may delay the recieving of next request packet from client.
  for (; i < row_id; ++i) {
    if (!loop_flag->test(i)) delete id_prior_packets[i].second;
  }
  if (loop_flag) delete loop_flag;
  if (ignore_flag) delete ignore_flag;
}

void MySQLConnectByNode::handle_discarded_packets() {
  if (loop_flag->all()) return;
  unsigned int i = 0;
  for (; i < row_id; ++i) {
    if (!loop_flag->test(i)) {
      loop_flag->set(i);
      delete id_prior_packets[i].second;
    }
  }
}

void MySQLConnectByNode::init_column_index() {
  list<Packet *> *field_packets = get_field_packets();
  unsigned int column_num = field_packets->size();

  if (where_index) where_index += column_num;
  start_index += column_num;
  prior_index += column_num;
  recur_index += column_num;

  if (start_index < 0 || prior_index < 0 || recur_index < 0 ||
      where_index < 0) {
    LOG_ERROR("Fail to init connect by column index.\n");
    throw Error("Fail to init connect by column index.");
  }

  inited_column_index = true;
}

void MySQLConnectByNode::handle_child(MySQLExecuteNode *child) {
  if (!inited_column_index) init_column_index();

  Packet *row = NULL;
  while (!row_map[child]->empty()) {
    if (row_id >= max_result_num || row_id >= MAX_CONNECT_BY_CACHED_ROWS) {
      LOG_ERROR("Reach the max connect by result cache number.\n");
      throw Error("Reach the max connect by result cache number.");
    }
    row = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row_res(row);
    uint64_t row_len;
    bool is_null;

    int where_result = 1;
    if (where_index) {
      is_null = row_res.field_is_null(where_index);
      if (!is_null) where_result = row_res.get_int((unsigned int)where_index);
    }

    int start_flag = 0;
    is_null = row_res.field_is_null(start_index);
    if (!is_null) start_flag = row_res.get_int((unsigned int)start_index);

    string prior_value;
    is_null = row_res.field_is_null(prior_index);
    if (!is_null) {
      const char *prior_data =
          row_res.get_str((unsigned int)prior_index, &row_len);
      prior_value.assign(prior_data, row_len);
    }

    string recur_value;
    is_null = row_res.field_is_null(recur_index);
    if (!is_null) {
      const char *recur_data =
          row_res.get_str((unsigned int)recur_index, &row_len);
      recur_value.assign(recur_data, row_len);
    }

    if (!where_result) ignore_flag->set(row_id);
    if (start_flag) start_ids.push_back(row_id);
    id_prior_packets[row_id] = make_pair(prior_value, row);
    if (!recur_value.empty()) recur_ids[recur_value].push_back(row_id);
    ++row_id;
  }
}

/* How to implement connect by:
 * 1. Erase the connect by part from original query sql, and append start, where
 * conditions, append prior and recursion related columns to the select fiedld
 * list. For example: SELECT * FROM t1 where t1.c3<100 window connect by t1.c1 =
 * prior t1.c2 start with t1.c1 = 1; will be changed to: SELECT *, t1.c1, t1.c2,
 * (t1.c1 = 1), (t1.c3<100) FROM t1;
 * 2. Store all results data in id_prior_packets, the key is row id, the value
 * is prior column value and row packet.
 * 3. Check the (t1.c1 = 1) column，if 1, means the row is the start row, put
 * into start_ids.
 * 4. Check the (t1.c3<100) column, if 0, the row will be filtered, put into
 * ignore_flag.
 * 5. Store each row in recur_ids, the key is recur column vlaue, value is all
 * the row ids has the same recur value.
 * 6. Use loop_flag to check if a row has already been put into ready_rows,
 * check each row before put into ready_rows, if 1, means there is a connect by
 * data loop, do not support.
 * 7. Use start_ids to start the recursive function:
 *    a. Put the row to ready_rows.
 *    b. Get the prior column value of the row.
 *    c. Check recur_ids, if there are rows has recur column value equal to the
 * prior column vlaue d  if yes goto a if no return
 * */
void MySQLConnectByNode::handle_before_complete() {
  list<unsigned int>::iterator it = start_ids.begin();
  for (; it != start_ids.end(); ++it) {
    if (loop_flag->test(*it)) {
      LOG_ERROR("Not support CONNECT BY loop for query data.\n");
      throw Error("Not support CONNECT BY loop for query data.");
    }
    if (!ignore_flag->test(*it)) {
      ready_rows->push_back(id_prior_packets[*it].second);
      loop_flag->set(*it);
    }
    find_recur_row_result(*it);
  }
}

void MySQLConnectByNode::find_recur_row_result(unsigned int id) {
  string prior_value = id_prior_packets[id].first;
  if (recur_ids.count(prior_value)) {
    list<unsigned int> id_list = recur_ids[prior_value];
    list<unsigned int>::iterator it = id_list.begin();
    for (; it != id_list.end(); ++it) {
      if (loop_flag->test(*it)) {
        LOG_ERROR("Not support CONNECT BY loop for query data.\n");
        throw Error("Not support CONNECT BY loop for query data.");
      }
      if (!ignore_flag->test(*it)) {
        ready_rows->push_back(id_prior_packets[*it].second);
        loop_flag->set(*it);
      }
      find_recur_row_result(*it);
    }
  }
}

/* class MySQLAvgNode */
MySQLAvgNode::MySQLAvgNode(ExecutePlan *plan, list<AvgDesc> &avg_list)
    : MySQLInnerPipeNode(plan) {
  for (auto &desc : avg_list) {
    AvgDesc desc_clone;
    desc_clone.clone(desc);
    this->avg_list.push_back(desc_clone);
  }
  inited_column_index = false;
  this->name = "MySQLAvgNode";
}
void MySQLAvgNode::clean() {
  MySQLInnerNode::clean();
  for (auto &desc : avg_list) {
    if (desc.mpf_inited) mpf_clear(desc.value);
  }
}

void MySQLAvgNode::init_column_index() {
  if (!inited_column_index) {
    list<Packet *> *field_packets = get_field_packets();
    unsigned int column_num = field_packets->size();

    /* adjust column index */
    list<AvgDesc>::iterator it;
    for (it = avg_list.begin(); it != avg_list.end(); it++) {
      it->sum_index += column_num;
      it->count_index += column_num;
      if (it->avg_index < 0) it->avg_index += column_num;
    }

    /* adjust the avg result decimal */
    list<Packet *>::iterator it_field;
    unsigned int index = 0;
    for (it_field = field_packets->begin(); it_field != field_packets->end();
         it_field++, index++) {
      MySQLColumnResponse col_resp(*it_field);
      col_resp.unpack();
      for (it = avg_list.begin(); it != avg_list.end(); it++) {
        if (it->avg_index == (int)index) {
          avg_column_type[it->avg_index] = col_resp.get_column_type();
          it->decimal = col_resp.get_decimals();
          LOG_DEBUG("decimal=%d, index=%d\n", it->decimal, index);
          break;
        }
      }
    }
    inited_column_index = true;
  }
}

void MySQLAvgNode::handle_child(MySQLExecuteNode *child) {
  Packet *row;
  mpf_t sum;
  uint64_t count = 0;
  list<AvgDesc>::iterator it;
  char avg_column[GMP_N_DIGITS + 5];

  init_column_index();

  unsigned int column_num = get_field_packets()->size();
  mpf_init2(sum, DECIMAL_STORE_BIT);
  while (!row_map[child]->empty()) {
    row = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row_res(row);
    Packet *new_row = Backend::instance()->get_new_packet(row_packet_size);
    vector<int> column_len(column_num, 0);
    vector<string> column_data(column_num, "");
    vector<bool> reset_value(column_num, false);
    unsigned int needed_length = 0;

    new_row->wr_ptr(new_row->base() + PACKET_HEADER_SIZE);
    for (it = avg_list.begin(); it != avg_list.end(); it++) {
      it->reset_value = true;
      if (!row_res.field_is_null(it->sum_index) &&
          !row_res.field_is_null(it->count_index)) {
        row_res.get_mpf(it->sum_index, sum);
        count = row_res.get_uint(it->count_index);
        if (count == 0) {
          it->reset_value = false;
          continue;
        }
      } else {
        it->reset_value = false;
        continue;
      }
      mpf_div_ui(it->value, sum, count);
    }

    for (unsigned int i = 0; i < column_num; i++) {
      row_res.set_current(i);
      for (it = avg_list.begin(); it != avg_list.end(); it++) {
        if (it->avg_index == (int)i && it->reset_value) {
          if (it->decimal == 0)
            column_len[i] = gmp_sprintf(avg_column, "%.4Ff", it->value);
          else if (it->decimal == 31 &&
                   avg_column_type[i] == MYSQL_TYPE_DOUBLE) {
            column_len[i] = gmp_sprintf(avg_column, "%.16Ff", it->value);
            for (int j = column_len[i] - 1; j > 0; j--) {
              if (avg_column[j] == '0') {
                column_len[i]--;
              } else if (avg_column[j] == '.') {
                column_len[i]--;
                break;
              } else {
                break;
              }
            }
          } else {
            char formator[10];
            sprintf(formator, "%%.%dFf", it->decimal);
            column_len[i] = gmp_sprintf(avg_column, formator, it->value);
          }
          column_data[i] = avg_column;
          reset_value[i] = true;
          break;
        }
      }

      if (!row_res.field_is_null() && !reset_value[i]) {
        column_len[i] = row_res.get_current_length();
      }
      needed_length += column_len[i];
      row_res.move_next();
    }

    needed_length += 1 + 9 * column_num;
    /**
     *   max(1,9)    // '\373' or max required length of pack_lenenc_int()
     * + column_len  // packed data length
     * + 1           // for terminate '\0'
     */
    if (new_row->size() - (new_row->wr_ptr() - new_row->base()) <
        needed_length) {
      uint64_t resize_len = needed_length + new_row->wr_ptr() - new_row->base();
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                new_row->size(), resize_len);
      new_row->size(resize_len);
    }

    for (unsigned int i = 0; i < column_num; i++) {
      row_res.set_current(i);
      if (row_res.field_is_null() && !reset_value[i])
        new_row->packchar(
            '\373');  //'\373' represents that the field value is NULL
      else {
        if (!reset_value[i]) {
          column_data[i] = row_res.get_current_data();
        }
        new_row->pack_lenenc_int(column_len[i]);
        new_row->packdata(column_data[i].c_str(), column_len[i]);
      }
      row_res.move_next();
    }
    size_t load_length = new_row->length() - PACKET_HEADER_SIZE;
    pack_header(new_row, load_length);
    ready_rows->push_back(new_row);
    delete row;
  }
  mpf_clear(sum);
}

/* class MySQLUnionGroupNode */

MySQLUnionGroupNode::MySQLUnionGroupNode(ExecutePlan *plan,
                                         vector<list<ExecuteNode *> > &nodes)
    : MySQLExecuteNode(plan, NULL) {
  this->name = "MySQLUnionGroupNode";
  this->nodes = nodes;
  finished_group_num = 0;
  column_size = 0;
  got_error = false;
}

void MySQLUnionGroupNode::check_union_columns() {
  if (column_size == 0) {
    MySQLResultSetHeaderResponse header(
        ((MySQLExecuteNode *)(nodes[0].front()))->get_header_packet());
    header.unpack();
    column_size = header.get_columns();
  }
  list<ExecuteNode *>::iterator it = nodes[finished_group_num].begin();
  for (; it != nodes[finished_group_num].end(); it++) {
    MySQLResultSetHeaderResponse header(
        ((MySQLExecuteNode *)(*it))->get_header_packet());
    header.unpack();
    if (column_size != header.get_columns()) {
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      MySQLErrorResponse error(
          1222, "The used SELECT statements have a different number of columns",
          "21000");
      error.pack(&error_packet);
      throw ErrorPacketException();
    }
  }
}

void MySQLUnionGroupNode::handle_children() {
  list<ExecuteNode *>::iterator it;
  for (it = nodes[finished_group_num].begin();
       it != nodes[finished_group_num].end(); ++it) {
    if (!row_map[*it]->empty()) handle_child((MySQLExecuteNode *)(*it));
  }
}

void MySQLUnionGroupNode::handle_child(MySQLExecuteNode *child) {
  Packet *row;
  while (!row_map[child]->empty()) {
    row = row_map[child]->front();
    ready_rows->push_back(row);
    row_map[child]->pop_front();
  }
}

void MySQLUnionGroupNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START: {
#ifdef DEBUG
        node_start_timing();
#endif
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;
      }
      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
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
        try {
          handle_children();
        } catch (...) {
          status = EXECUTE_STATUS_COMPLETE;
          throw;
        }
        status = EXECUTE_STATUS_FETCH_DATA;
        break;
      case EXECUTE_STATUS_BEFORE_COMPLETE:
        status = EXECUTE_STATUS_COMPLETE;
      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        node_end_timing();
#endif
#ifdef DEBUG
        LOG_DEBUG("MySQLUnionGroupNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        break;
    }
  }
}

Packet *MySQLUnionGroupNode::get_error_packet() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it)->get_error_packet()) {
      return (*it)->get_error_packet();
    }
  }
  if (got_error) {
    return &error_packet;
  }
  return NULL;
}

void MySQLUnionGroupNode::children_execute() {
  LOG_DEBUG("Node %@ children start to execute.\n", this);
  if (finished_group_num < nodes.size()) {
    list<ExecuteNode *> execute_nodes = nodes[finished_group_num];
    list<ExecuteNode *>::iterator it = execute_nodes.begin();
    for (; it != execute_nodes.end(); ++it) {
      (*it)->execute();
    }
  }
  LOG_DEBUG("Node %@ children execute finished.\n", this);
}

bool MySQLUnionGroupNode::notify_parent() {
  Packet *row;
  if (ready_rows->empty()) {
    return false;
  }

  while (!ready_rows->empty()) {
    row = ready_rows->front();
    ready_rows->pop_front();
    parent->add_row_packet(this, row);
  }

  return true;
}

void MySQLUnionGroupNode::set_children_thread_status_start(
    unsigned int group_num) {
  list<ExecuteNode *>::iterator it = nodes[group_num].begin();
  for (; it != nodes[group_num].end(); ++it) {
    ((MySQLExecuteNode *)(*it))->set_thread_status_started();
  }
}

void MySQLUnionGroupNode::handle_error_all_children() {
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

void MySQLUnionGroupNode::wait_children() {
  LOG_DEBUG("Node %@ start to wait.\n", this);
  list<ExecuteNode *> node_list = nodes[finished_group_num];
  list<ExecuteNode *>::iterator it = node_list.begin();
  for (; it != node_list.end(); it++) {
    ((MySQLExecuteNode *)(*it))->start_thread();
  }
  set_children_thread_status_start(finished_group_num);
  plan->start_all_bthread();

  unsigned int finished_child = 0;
  it = node_list.begin();
  for (; it != node_list.end(); it++) {
    if (!((MySQLExecuteNode *)(*it))->notify_parent() && (*it)->is_finished()) {
      finished_child++;
    }
  }
  // Check union columns for one group after this group nodes got header
  // packets.
  check_union_columns();
  if (finished_child == node_list.size()) {
    finished_group_num++;
  }

  if (finished_group_num == nodes.size()) {
    handle_error_all_children();
    status = EXECUTE_STATUS_BEFORE_COMPLETE;
  } else {
    status = EXECUTE_STATUS_HANDLE;
  }
  LOG_DEBUG("Node %@ wait finished.\n", this);
}

void MySQLUnionGroupNode::init_row_map() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    row_map[*it] =
        new AllocList<Packet *, Session *, StaticAllocator<Packet *> >();
    row_map[*it]->set_session(plan->session);
    row_map_size[*it] = 0;
  }
}

void MySQLUnionGroupNode::clean() {
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
}

/* class MySQLUnionAllNode */
MySQLUnionAllNode::MySQLUnionAllNode(ExecutePlan *plan,
                                     vector<ExecuteNode *> &nodes)
    : MySQLInnerPipeNode(plan) {
  this->name = "MySQLUnionAllNode";
  column_size = 0;
  this->nodes = nodes;
  got_error = false;
}

void MySQLUnionAllNode::handle_children() {
  if (column_size == 0) check_union_columns();
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) handle_child(*it);
  }
  status = EXECUTE_STATUS_FETCH_DATA;
}

void MySQLUnionAllNode::check_union_columns() {
  vector<ExecuteNode *>::iterator it = nodes.begin();
  MySQLResultSetHeaderResponse header(
      ((MySQLExecuteNode *)(*it))->get_header_packet());
  header.unpack();
  column_size = header.get_columns();
  it++;
  for (; it != nodes.end(); it++) {
    MySQLResultSetHeaderResponse header(
        ((MySQLExecuteNode *)(*it))->get_header_packet());
    header.unpack();
    if (column_size != header.get_columns()) {
      got_error = true;
      status = EXECUTE_STATUS_COMPLETE;
      MySQLErrorResponse error(
          1222, "The used SELECT statements have a different number of columns",
          "21000");
      error.pack(&error_packet);
      throw ErrorPacketException();
    }
  }
}

Packet *MySQLUnionAllNode::get_error_packet() {
  Packet *error = MySQLInnerPipeNode::get_error_packet();
  if (error) {
    return error;
  } else if (got_error) {
    return &error_packet;
  }
  return NULL;
}

/* class MySQLInnerPipeNode */
MySQLInnerPipeNode::MySQLInnerPipeNode(ExecutePlan *plan)
    : MySQLInnerNode(plan) {
  this->name = "MySQLInnerPipeNode";
  node_can_swap = session->is_may_backend_exec_swap_able();
}
void MySQLInnerPipeNode::handle_children() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) handle_child(*it);
  }
  status = EXECUTE_STATUS_FETCH_DATA;
}

void MySQLInnerPipeNode::handle_child(MySQLExecuteNode *child) {
  Packet *row;
  while (!row_map[child]->empty()) {
    row = row_map[child]->front();
    ready_rows->push_back(row);
    row_map[child]->pop_front();
  }
}

void MySQLInnerPipeNode::execute() {
  ExecuteProfileHandler *execute_profile = session->get_profile_handler();
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START: {
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;
      }
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
        profile_id = execute_profile->start_serial_monitor(
            get_executenode_name(), profile_id);
        try {
          handle_children();
        } catch (...) {
          status = EXECUTE_STATUS_COMPLETE;
          throw;
        }
#ifdef DEBUG
        node_end_timing();
#endif
        execute_profile->end_execute_monitor(profile_id);
        status = EXECUTE_STATUS_FETCH_DATA;
        return;
        break;
      case EXECUTE_STATUS_BEFORE_COMPLETE:
        handle_before_complete();
        status = EXECUTE_STATUS_COMPLETE;
      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLInnerPipeNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        break;
    }
  }
}
/* class MySQLOKMergeNode */

MySQLOKMergeNode::MySQLOKMergeNode(ExecutePlan *plan) : MySQLInnerNode(plan) {
  this->name = "MySQLOKMergeNode";
  ok_packet = NULL;
  affect_rows = 0;
  warnings = 0;
}

void MySQLOKMergeNode::handle_children() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) handle_child(*it);
  }

  status = EXECUTE_STATUS_FETCH_DATA;
}

void MySQLOKMergeNode::handle_child(MySQLExecuteNode *child) {
  Packet *packet = NULL;
  int first_msg = 0;
  int second_msg = 0;
  stmt_type type = plan->statement->get_stmt_node()->type;
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLOKResponse ok(packet);
    ok.unpack();
    affect_rows += ok.get_affected_rows();
    warnings += ok.get_warnings();
    if (type == STMT_UPDATE) {
      first_msg +=
          session->get_matched_or_change_message("matched:", ok.get_message());
      second_msg +=
          session->get_matched_or_change_message("Changed:", ok.get_message());
    }
    LOG_DEBUG("After handle modify node %@, affected_row[%d], warnings[%d].\n",
              child, affect_rows, warnings);
    if (!ok_packet) {
      ok_packet = packet;
      LOG_DEBUG("Keep packet %@ for ok packet rebuild.\n", packet);
    } else {
      LOG_DEBUG("Delete packet %@ after handle modify node %@.\n", packet,
                this);
      delete packet;
    }
  }
  if (type == STMT_UPDATE) {
    string ok_msg = "Rows matched: ";
    ok_msg.append(to_string(first_msg));
    ok_msg.append("  Changed: ");
    ok_msg.append(to_string(second_msg));
    ok_msg.append("  Warnings: ");
    ok_msg.append(to_string(warnings));
    update_msg = ok_msg;
  }
  LOG_DEBUG("Handle modify node %@ finish.\n", child);
}

void MySQLOKMergeNode::rebuild_ok_packet() {
  LOG_DEBUG("Rebuild ok packet with affected_rows [%d] warnings [%d].\n",
            affect_rows, warnings);
  if (ok_packet) {
    MySQLOKResponse ok(ok_packet);
    ok.set_affected_rows(affect_rows);
    ok.set_warnings(warnings);
    ok.set_message(update_msg);
    Packet *new_packet = Backend::instance()->get_new_packet(row_packet_size);
    ok.pack(new_packet);
    delete ok_packet;
    ok_packet = new_packet;
  } else {
    MySQLOKResponse ok(affect_rows, warnings);
    ok_packet = Backend::instance()->get_new_packet(row_packet_size);
    ok.pack(ok_packet);
  }
  ready_rows->push_back(ok_packet);
  ok_packet = NULL;
}

void MySQLOKMergeNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START: {
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;
      }

      case EXECUTE_STATUS_FETCH_DATA:
        children_execute();
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
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        rebuild_ok_packet();
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLOKMergeNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        break;
    }
  }
}

/* classs MySQLModifyNode */

MySQLModifyNode::MySQLModifyNode(ExecutePlan *plan, DataSpace *dataspace)
    : MySQLExecuteNode(plan, dataspace),
      cond_sql(sql_mutex),
      cond_notify(mutex),
      new_stmt_for_shard(NULL) {
  this->name = "MySQLModifyNode";
  thread_status = THREAD_STOP;
  bthread = NULL;
  got_error = false;
  conn = NULL;
  status = EXECUTE_STATUS_START;
  this->error_packet = NULL;
}

void MySQLModifyNode::handle_error_packet(Packet *packet) {
  try {
    ACE_Guard<ACE_Thread_Mutex> guard(mutex);
    record_migrate_error_message(plan, packet,
                                 "Modify node get an error packet");
    got_error = true;
    status = EXECUTE_STATUS_COMPLETE;
    error_packet = packet;
    if (handle_tokudb_lock_timeout(conn, packet)) {
      if (conn) {
        handler->clean_dead_conn(&conn, dataspace);
      }
    }
    cond_notify.signal();
  } catch (...) {
    cond_notify.signal();
  }
}

void MySQLModifyNode::execute() {
  if (thread_status == THREAD_STOP) {
    status = EXECUTE_STATUS_FETCH_DATA;

#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "bthread") &&
        !strcasecmp(test_info->test_case_operation.c_str(), "get_null")) {
      bthread = NULL;
    } else {
#endif
      Backend *backend = Backend::instance();
      bthread = backend->get_backend_thread_pool()->get_one_from_free();
#ifndef DBSCALE_TEST_DISABLE
    }
#endif
    if (!bthread) {
      got_error = true;
      LOG_ERROR("Fail to get thread for MySQLModifyNode::execute.\n");
      throw Error(
          "Fail to get a backend thread from pool, so stop execute the sql");
    }
    thread_status = THREAD_CREATED;
    LOG_DEBUG("Modify node %@ start to work.\n", this);
    bthread->set_task(this);
    thread_status = THREAD_STARTED;
    bthread->wakeup_handler_thread();
    // After the acquire of lock, the backend thread must is start to running
    // the task.
  }
}

void MySQLModifyNode::clean() {
  if (thread_status == THREAD_STARTED) {
    LOG_DEBUG("Modify node %@ cleaning wait.\n", this);
    this->wait_for_cond();
    LOG_DEBUG("Modify node %@ cleaning wait finish.\n", this);
  } else if (thread_status == THREAD_CREATED) {
    bthread->re_init_backend_thread();
    bthread->get_pool()->add_back_to_free(bthread);
  } else {
    got_error = true;
  }
  Packet *packet = NULL;
  while (!ready_rows->empty()) {
    packet = ready_rows->front();
    ready_rows->pop_front();
    delete packet;
  }

  string *s;
  while (!sql_list.empty()) {
    s = sql_list.front();
    sql_list.pop_front();
    delete s;
  }

  if (conn) {
    if (!got_error || error_packet) {
      if (plan->statement->is_cross_node_join())
        handler->put_back_connection(dataspace, conn, true);
      else if (!plan->get_migrate_tool())
        handler->put_back_connection(dataspace, conn);
    } else {
      if (plan->statement->is_cross_node_join())
        handler->clean_dead_conn(&conn, dataspace, false);
      else if (!plan->get_migrate_tool())
        handler->clean_dead_conn(&conn, dataspace);
      else {
        conn->set_status(CONNECTION_STATUS_TO_DEAD);
      }
    }
    conn = NULL;
  }

  if (error_packet) {
    delete error_packet;
    error_packet = NULL;
  }
  if (new_stmt_for_shard) {
    new_stmt_for_shard->free_resource();
    delete new_stmt_for_shard;
    new_stmt_for_shard = NULL;
  }

  if (session->is_drop_current_schema()) {
    session->set_schema(NULL);
  }
}

static int check_ddl_need_retry(Session *session, DataSpace *space,
                                const char *sql) {
  string query_sql =
      "SELECT PROCESSLIST_STATE FROM PERFORMANCE_SCHEMA.THREADS WHERE "
      "PROCESSLIST_INFO=\"";
  string tmp_sql = find_and_replace_quota(sql);
  query_sql.append(tmp_sql.c_str()).append("\"");
  Connection *conn = NULL;
  TimeValue tv(backend_sql_net_timeout);
  bool need_recheck = true;
  while (need_recheck) {
    need_recheck = false;
    try {
      conn = space->get_connection(session);
      if (conn) {
        bool find_meta_lock = false;
        vector<string> result;
        conn->query_for_one_column(query_sql.c_str(), 0, &result, &tv, true);
        if (result.size() == 0) {
          conn->get_pool()->add_back_to_free(conn);
          return 1;
        }
        vector<string>::iterator it = result.begin();
        for (; it != result.end(); ++it) {
          string value = *it;
          if (value.find("metadata lock") != string::npos) {
            find_meta_lock = true;
            break;
          }
        }
        conn->get_pool()->add_back_to_free(conn);
        conn = NULL;
        if (find_meta_lock) return 0;
        need_recheck = true;  // wait ddl run finished
        sleep(5);
      }
    } catch (...) {
      if (conn) {
        conn->get_pool()->add_back_to_dead(conn);
      }
    }
  }
  return -1;
}

int MySQLModifyNode::svc() {
  status = EXECUTE_STATUS_FETCH_DATA;
  string *s = NULL;
  Packet *packet = NULL;
  string real_sql;
  bool off_sql_log_bin = false;
  size_t list_size = 0;
  bool need_retry = false;
  TimeValue tv(ddl_recv_timeout, 0);
  stmt_type sql_type = plan->statement->get_stmt_node()->type;
  if (run_ddl_retry &&
      (sql_type == STMT_DROP_TB || sql_type == STMT_RENAME_TABLE)) {
    need_retry = true;
    LOG_DEBUG("run ddl statement so can retry if failed\n");
  }
  int retry_count = 0;
retry:
  try {
    while (1) {
      if (ACE_Reactor::instance()->reactor_event_loop_done()) {
        LOG_ERROR(
            "MySQLModifyNode svc failed due to reactor_event_loop_done\n");
        throw Error(
            "MySQLModifyNode svc failed due to reactor_event_loop_done");
      }
      sql_mutex.acquire();
      if (!retry_count && sql_list.empty()) {
        LOG_DEBUG("Modify node %@ start to wait_children new sql.\n", this);
        cond_sql.wait();
      }
      if (!sql_list.empty() && !retry_count) {
        s = sql_list.front();
        sql_list.pop_front();
        sql = string(*s);
      }
      if (!retry_count) list_size = sql_list.size();
      sql_mutex.release();

      if (!session->is_keeping_connection()) {
        if (parent->is_finished()) {
          LOG_INFO("ModifyNode %@ exit cause parent finished.\n", this);

          if (plan->statement->is_cross_node_join()) {
            // Do nothing
          } else if (!plan->get_migrate_tool())
            conn = session->get_kept_connection(dataspace);
          else
            conn = plan->get_migrate_tool()->get_migrate_write_conn(this);
          if (conn) {
            if (plan->statement->is_cross_node_join())
              handler->clean_dead_conn(&conn, dataspace, false);
            else if (!plan->get_migrate_tool())
              handler->clean_dead_conn(&conn, dataspace);
            else {
              conn->set_status(CONNECTION_STATUS_TO_DEAD);
              conn = NULL;
            }
          }
          delete s;
          status = EXECUTE_STATUS_COMPLETE;
          return FINISHED;
        }
      }

      LOG_DEBUG("Modify node handle sql [%s] with [%d] sql append.\n",
                sql.c_str(), list_size);
      if (!strlen(sql.c_str())) {
        LOG_DEBUG("Modify node %@ finish.\n", this);
        delete s;
        s = NULL;
        break;
      }

      if (enable_xa_transaction &&
          plan->session->get_session_option("close_session_xa").int_val == 0) {
        real_sql.clear();
        real_sql += sql;
      }

      Packet exec_packet;
      MySQLQueryRequest query(sql.c_str());
      query.set_sql_replace_char(
          plan->session->get_query_sql_replace_null_char());
      query.pack(&exec_packet);
      delete s;
      s = NULL;
#ifdef DEBUG
      if (retry_count == 0) node_start_timing();
#endif
      stmt_type st_type = plan->statement->get_stmt_node()->type;
      bool is_ddl =
          (STMT_DDL_START < st_type && st_type < STMT_DDL_END) ? true : false;
      bool skip_keep_conn = false;
      if (need_retry) {
        int check_working_time = 0;
        while (dataspace->data_source->get_work_state() !=
                   DATASOURCE_STATE_WORKING &&
               check_working_time < 60) {
          sleep(1);
          check_working_time += 1;
        }
        if (check_working_time == 60) {
          conn = NULL;
          retry_count = 5;  // no need retry
          LOG_ERROR(
              "Fail to get an usable connection, because %s is "
              "not "
              "working\n",
              dataspace->get_name());
          throw Error(
              "Fail to get an usable connection, because space is not working");
        }
      }
      if (plan->get_migrate_tool()) {
        conn = plan->get_migrate_tool()->get_migrate_write_conn(this);
        conn->reset();
        handler->send_to_server(conn, &exec_packet);
      } else {
        if (plan->statement->is_cross_node_join()) {
          if (dataspace->get_data_source() &&
              dataspace->get_data_source()->get_master_server() &&
              dataspace->get_data_source()
                  ->get_master_server()
                  ->is_mgr_server() &&
              strstr(sql.c_str(), TMP_TABLE_SCHEMA) != NULL &&
              strstr(sql.c_str(), TMP_TABLE_NAME) != NULL) {
            off_sql_log_bin = true;
          }
          skip_keep_conn = true;
          conn = handler->send_to_server_retry(
              dataspace, &exec_packet, session->get_schema(), false, true, true,
              skip_keep_conn, off_sql_log_bin);
        } else if (is_ddl) {
          skip_keep_conn = true;
          if ((st_type == STMT_DROP_DB &&
               session->is_temp_table_set_contain_db(session->get_schema())) ||
              plan->statement->get_stmt_node()->is_create_or_drop_temp_table) {
            skip_keep_conn = false;
          }
          conn = handler->send_to_server_retry(
              dataspace, &exec_packet, session->get_schema(),
              session->is_read_only(), false, false, skip_keep_conn,
              off_sql_log_bin);
        } else {
          conn = handler->send_to_server_retry(dataspace, &exec_packet,
                                               session->get_schema(),
                                               session->is_read_only());
        }
      }
      const char *server_name = conn->get_server()->get_name();
      bool non_modified_conn = true;
      LOG_DEBUG("Modify node receiving result from server\n");
      packet = Backend::instance()->get_new_packet(row_packet_size);
#ifndef DBSCALE_TEST_DISABLE
      if (on_test_stmt) {
        Backend *bk = Backend::instance();
        dbscale_test_info *test_info = bk->get_dbscale_test_info();
        if (!strcasecmp(test_info->test_case_name.c_str(),
                        "auth_source_fail") &&
            !strcasecmp(test_info->test_case_operation.c_str(),
                        "fail_for_create_drop_db")) {
          if (dataspace == bk->get_auth_data_space()) {
            bk->set_dbscale_test_info("", "", "");
            throw Error("Fail to receive from auth.");
          }
        }
      }
#endif
      if (need_retry) {
        conn->set_need_kill_timeout_conn(false);
        handler->receive_from_server(conn, packet, &tv);
        conn->set_need_kill_timeout_conn(true);
      }

      else
        handler->receive_from_server(conn, packet);
#ifdef DEBUG
      node_end_timing();
#endif
      sql = "";
      if (driver->is_error_packet(packet)) {
        non_modified_conn = false;
        MySQLErrorResponse error(packet);
        error.unpack();
        if (retry_count && need_retry &&
            Backend::instance()->check_ddl_return_retry_ok(
                plan->statement->get_stmt_node()->type,
                plan->statement->get_stmt_node(), error.get_error_code())) {
          LOG_DEBUG("after retry, ddl statement run success \n");
          delete packet;
          packet = Backend::instance()->get_new_packet();
          MySQLOKResponse ok(0, 0);
          ok.pack(packet);
        } else {
          if (!plan->get_migrate_tool()) {
            LOG_DEBUG("Modify node %@ get an error packet %@, %d (%s) %s.\n",
                      this, packet, error.get_error_code(),
                      error.get_sqlstate(), error.get_error_message());
          } else {
            LOG_ERROR("Modify node %@ get an error packet %@, %d (%s) %s.\n",
                      this, packet, error.get_error_code(),
                      error.get_sqlstate(), error.get_error_message());
          }
          handle_error_packet(packet);
          packet = NULL;
          return FINISHED;
        }
      }
      if (driver->is_ok_packet(packet)) {
        handler->deal_autocommit_with_ok_eof_packet(packet);
        if (!plan->get_migrate_tool()) {
          if (support_show_warning)
            handle_warnings_OK_and_eof_packet(
                plan, packet, handler, dataspace, conn,
                plan->statement->is_cross_node_join());

          if (plan->statement->get_stmt_node()->type == STMT_DROP_TB) {
            table_link *table =
                plan->statement->get_stmt_node()->table_list_head;
            if (table) {  // only will have one table
              const char *schema_name = table->join->schema_name
                                            ? table->join->schema_name
                                            : plan->statement->get_schema();
              const char *table_name = table->join->table_name;
              if (conn->dec_tmp_tb_num(schema_name, table_name)) {
                plan->session->remove_temp_table_set(schema_name, table_name);
                plan->statement->get_stmt_node()->is_create_or_drop_temp_table =
                    true;
              }
            }
          }

          MySQLOKResponse ok(packet);
          ok.unpack();
          if (ok.get_affected_rows() > 0) {
            non_modified_conn = false;
          }
        } else {
          MySQLOKResponse ok(packet);
          ok.unpack();
          uint16_t warnings = ok.get_warnings();
          if (warnings) {
            LOG_ERROR("Modify node %@ get OK packet with %u warnings.\n", this,
                      (unsigned int)warnings);
            delete packet;
            packet = handler->get_error_packet(
                9002, "Migrate modify node get OK packet with warnings.", NULL);
            handle_error_packet(packet);
            packet = NULL;
            print_warning_infos(conn);
            return FINISHED;
          }
        }
      }

      {
        ACE_Guard<ACE_Thread_Mutex> guard(mutex);
        ready_rows->push_back(packet);
        packet = NULL;
        cond_notify.signal();
      }

      if (conn) {
        if (!plan->get_migrate_tool()) {
          if (plan->statement->is_cross_node_join()) {
            if (off_sql_log_bin) {
              if (conn->get_session_var_map_md5() !=
                  session->get_session_var_map_md5()) {
                conn->set_session_var(session->get_session_var_map());
              }
            }
          }
          handler->put_back_connection(dataspace, conn, skip_keep_conn);
        }
      }

      if (!plan->get_migrate_tool()) {
        record_xa_modify_sql(plan, session, dataspace, real_sql.c_str(),
                             non_modified_conn);
        if (!non_modified_conn) {
          session->record_xa_modified_conn(conn);
        }
        record_modify_server(plan, session, server_name,
                             dataspace->get_virtual_machine_id(),
                             non_modified_conn);
      }
      conn = NULL;

      LOG_DEBUG("Modify node %@ finish sql.\n", this);
    }
  } catch (exception &e) {
    LOG_ERROR("Modify node thread %@ got exception %s.\n", this, e.what());
    if (need_retry && retry_count < 5) {
      retry_count++;
      int ret = check_ddl_need_retry(session, dataspace, sql.c_str());
      try {
        if (ret == 0) {
          if (conn && conn->get_need_kill_timeout_connection())
            handler->kill_timeout_connection(conn, packet, true, &tv);
        } else if (ret == 1) {
          if (conn) {
            conn->get_pool()->add_back_to_dead(conn);
            conn = NULL;
          }
          if (packet) {
            delete packet;
            packet = NULL;
          }
          sleep(monitor_interval + 1);
          goto retry;
        } else {
          if (conn) {
            if (conn->get_need_kill_timeout_connection())
              handler->kill_timeout_connection(conn, packet, true, &tv);
            conn->get_pool()->add_back_to_dead(conn);
            conn = NULL;
          }
          if (packet) {
            delete packet;
            packet = NULL;
          }
          sleep(monitor_interval + 1);
          goto retry;
        }
      } catch (exception &kill_exception) {
        LOG_ERROR("try to kill timeout connection faild \n");
      }
    }

    if (conn) {
      if (plan->statement->is_cross_node_join())
        handler->clean_dead_conn(&conn, dataspace, false);
      else if (!plan->get_migrate_tool())
        handler->clean_dead_conn(&conn, dataspace);
      else {
        conn->reset_cluster_xa_session_before_to_dead();
        conn->set_status(CONNECTION_STATUS_TO_DEAD);
        conn = NULL;
      }
    }
    if (packet) delete packet;

    handle_error_packet(NULL);
    if (s) delete s;
    s = NULL;
    cond_notify.signal();
    return FINISHED;
  }
  mutex.acquire();
  status = EXECUTE_STATUS_COMPLETE;
  cond_notify.signal();
  mutex.release();
#ifdef DEBUG
  LOG_DEBUG("MySQLModifyNode %@ cost %d ms\n", this, node_cost_time);
#endif
  return FINISHED;
}

bool MySQLModifyNode::notify_parent() {
  int ret = false;
  Packet *packet = NULL;

  ACE_Guard<ACE_Thread_Mutex> guard(mutex);
  // check fetch node finished or get error
  if (is_finished() || got_error) {
    if (got_error) {
      throw_error_packet();
    }
    ret = false;
  } else {
    if (ready_rows->empty()) cond_notify.wait();

    if (got_error) {
      throw_error_packet();
    } else if (ready_rows->empty()) {  // the result set is empty
      ACE_ASSERT(is_finished());
      ret = false;
    } else {
      while (!ready_rows->empty()) {
        packet = ready_rows->front();
        ready_rows->pop_front();
        parent->add_row_packet(this, packet);
        ret = true;
      }
    }
  }

  return ret;
}

void MySQLModifyNode::add_sql_during_exec(const char *stmt_sql, size_t len) {
  add_sql(stmt_sql, len, true);
}

void MySQLModifyNode::add_sql(const char *stmt_sql, size_t len,
                              bool need_shard_parse) {
  string *s;
  if (dataspace && dataspace->get_virtual_machine_id() > 0) {
    string tmp;
    if (len)
      tmp.assign(stmt_sql, len);
    else
      tmp.assign(stmt_sql);
    if (tmp.length() > 0) {
      if (need_shard_parse && !new_stmt_for_shard) {
        Parser *parser = MySQLParser::instance();
        new_stmt_for_shard = parser->parse(
            tmp.c_str(), plan->statement->get_allow_dot_in_ident(), true, NULL,
            NULL, NULL, handler->get_session()->get_client_charset_type());
      }
      string new_sql_tmp;
      len = 0;
      adjust_virtual_machine_schema(
          dataspace->get_virtual_machine_id(), dataspace->get_partition_id(),
          tmp.c_str(), plan->statement->get_schema(),
          need_shard_parse ? new_stmt_for_shard->get_stmt_node()
                           : plan->statement->get_latest_stmt_node(),
          plan->statement->get_record_scan_all_table_spaces_map(), new_sql_tmp);
      s = new string(new_sql_tmp.c_str());
    } else
      s = new string(stmt_sql);
  } else {
    if (len)
      s = new string(stmt_sql, len);
    else
      s = new string(stmt_sql);
  }
  LOG_DEBUG("Modify node %@ add sql [%s].\n", this, s->c_str());

  try {
    ACE_Guard<ACE_Thread_Mutex> guard(sql_mutex);
    sql_list.push_back(s);
    cond_sql.signal();
  } catch (...) {
    cond_sql.signal();
    throw;
  }
}

/* class MySQLModifyLimitNode */
MySQLModifyLimitNode::MySQLModifyLimitNode(ExecutePlan *plan, record_scan *rs,
                                           PartitionedTable *table,
                                           vector<unsigned int> &par_ids,
                                           unsigned int limit, string &sql)
    : MySQLInnerNode(plan) {
  this->name = "MySQLModifyLimitNode";
  this->sql = sql;
  this->par_ids = par_ids;
  this->rs = rs;
  this->limit = limit;
  this->table = table;
  total_affect_rows = 0;
  total_warning_rows = 0;
}

void MySQLModifyLimitNode::modify_limit_in_sql(string &sql,
                                               unsigned int limit_num) const {
  // generate the limit context
  char new_limit_num[128];
  size_t len = sprintf(new_limit_num, "%u", limit_num);

  unsigned int start_pos, end_pos;
  get_limit_start_and_end(sql, start_pos, end_pos);

  string new_sql;
  new_sql.append(sql.c_str(), STR_LEN_INDEX(0, start_pos - 1));
  new_sql.append(new_limit_num, len);
  new_sql.append(sql.c_str() + end_pos);

  sql.swap(new_sql);
}

/*
  sql:select xxx from xxx limit_100_________where ...;(‘_’ means space)
                                |  |
                            start  end
*/
void MySQLModifyLimitNode::get_limit_start_and_end(
    string &sql, unsigned int &start_pos, unsigned int &end_pos) const {
  start_pos = 0;
  end_pos = sql.length();

  unsigned int start_search_pos = rs->limit_pos + 5;
  for (unsigned int i = start_search_pos; i < sql.length(); i++) {
    if (isdigit(sql[i])) {
      start_pos = i;
      break;
    }
  }

  for (unsigned int i = start_pos; i < sql.length(); i++) {
    if (!isdigit(sql[i])) {
      end_pos = i;
      break;
    }
  }
  return;
}

void MySQLModifyLimitNode::execute() {
  LinkedHashMap modify_nodes;
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START: {
        create_modify_node(modify_nodes);
        init_row_map();
        status = EXECUTE_STATUS_FETCH_DATA;
        break;
      }

      case EXECUTE_STATUS_FETCH_DATA: {
        generate_new_sql(modify_nodes);
        children_execute();
        if (node_can_swap && plan->plan_can_swap()) {
          handle_swap_connections();
          return;
        }
        status = EXECUTE_STATUS_WAIT;
      }

      case EXECUTE_STATUS_WAIT: {
        LOG_DEBUG("MySQLModifyLimitNode wait\n");
        try {
          wait_children();
        } catch (ErrorPacketException &e) {
          children_modify_end();
          LOG_DEBUG("MySQLModifyLimitNode wait error \n");
          status = EXECUTE_STATUS_COMPLETE;
          throw ErrorPacketException(
              "get an error packet during the modify node\n");
        }
        break;
      }

      case EXECUTE_STATUS_HANDLE: {
        LOG_DEBUG("MySQLModifyLimitNode handle\n");
        handle_children_affect_rows(modify_nodes);
        break;
      }

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        rebuild_ok_packet();
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLModifyLimitNode ok\n");
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLModifyLimitNode::generate_new_sql(LinkedHashMap &modify_nodes) {
  unsigned int each_limit_num = limit / modify_nodes.size();
  LinkedHashMap::iterator iter = modify_nodes.begin();
  for (size_t i = 0; iter != modify_nodes.end(); iter++, i++) {
    MySQLModifyNode *tmp = (*iter).first;
    string &exec_sql = modify_sqls[tmp->dataspace];
    if (i == modify_nodes.size() - 1) {
      unsigned int limit_num = each_limit_num + limit % modify_nodes.size();
      (*iter).second = limit_num;
      modify_limit_in_sql(exec_sql, limit_num);
    } else {
      (*iter).second = each_limit_num;
      modify_limit_in_sql(exec_sql, each_limit_num);
    }

    tmp->add_sql(exec_sql.c_str(), exec_sql.length());
  }
}

void MySQLModifyLimitNode::handle_children_affect_rows(
    LinkedHashMap &modify_nodes) {
  handle_response_packet(modify_nodes);
  if (!modify_nodes.empty()) {
    generate_new_sql(modify_nodes);
  } else
    children_modify_end();
  status = EXECUTE_STATUS_WAIT;
  return;
}

void MySQLModifyLimitNode::handle_response_packet(LinkedHashMap &modify_nodes) {
  LinkedHashMap::iterator map_iter = modify_nodes.begin();
  bool all_finish = true;
  for (; map_iter != modify_nodes.end(); map_iter++) {
    if ((*map_iter).second != 0) {
      all_finish = false;
      break;
    }
  }

  if (all_finish == true) {
    map_iter = modify_nodes.begin();
    for (; map_iter != modify_nodes.end();) {
      MySQLModifyNode *tmp = (MySQLModifyNode *)((map_iter++)->first);
      modify_nodes.erase(tmp);
      child_add_sql(tmp, "", 0);
    }
    return;
  } else {
    map<ExecuteNode *,
        AllocList<Packet *, Session *, StaticAllocator<Packet *> > *>::iterator
        iter;
    for (iter = row_map.begin(); iter != row_map.end(); iter++) {
      list<Packet *, StaticAllocator<Packet *> > *pack_list = (*iter).second;
      if (pack_list->empty()) {
        MySQLModifyNode *tmp = (MySQLModifyNode *)((*iter).first);
        child_add_sql(tmp, "", 0);
        continue;
      }
      Packet *packet = pack_list->front();
      pack_list->pop_front();
      if (driver->is_ok_packet(packet)) {
        MySQLOKResponse ok(packet);
        ok.unpack();
        uint16_t warnings = ok.get_warnings();
        if (warnings) {
          LOG_ERROR("Modify node %@ get OK packet with %u warnings.\n", this,
                    (unsigned int)warnings);
        }
        uint64_t affect_rows = ok.get_affected_rows();
        limit -= affect_rows;
        total_affect_rows += affect_rows;
        total_warning_rows += warnings;

        MySQLModifyNode *tmp = (MySQLModifyNode *)((*iter).first);
        unsigned int count;
        if (modify_nodes.find(tmp, count)) {
          if (affect_rows < (uint64_t)(count)) {
            modify_nodes.erase(tmp);
            child_add_sql(tmp, "", 0);
          }
        }
        delete packet;
      }
    }
  }
}

void MySQLModifyLimitNode::clean() {
  MySQLInnerNode::clean();
  while (!children.empty()) {
    MySQLExecuteNode *free_node = children.front();
    children.pop_front();
    free_node->clean();
    delete free_node;
  }
}

void MySQLModifyLimitNode::create_modify_node(LinkedHashMap &modify_nodes) {
  for (size_t i = 0; i < par_ids.size(); i++) {
    DataSpace *ds = table_get_par_dataspace(table, par_ids[i]);
    MySQLModifyNode *node = new MySQLModifyNode(plan, ds);
    modify_nodes.insert(node, 0);
    string adjust_sql = sql;
    table_sql_for_par(table, par_ids[i], adjust_sql);
    modify_sqls[ds] = adjust_sql;
    add_child(node);
  }
}

DataSpace *MySQLModifyLimitNode::table_get_par_dataspace(
    PartitionedTable *par_table, unsigned int id) const {
  return par_table->get_partition(id);
}

void MySQLModifyLimitNode::table_sql_for_par(PartitionedTable *par_table,
                                             unsigned int id,
                                             string &sql) const {
  ACE_UNUSED_ARG(id);
  sql = plan->statement->adjust_stmt_sql_for_shard(par_table, sql.c_str());
}

void MySQLModifyLimitNode::children_modify_end() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); it++) {
    child_add_sql((MySQLModifyNode *)(*it), "", 0);
  }
}

void MySQLModifyLimitNode::rebuild_ok_packet() {
  MySQLOKResponse ok(total_affect_rows, total_warning_rows);
  Packet *ok_packet = Backend::instance()->get_new_packet(row_packet_size);
  ok.pack(ok_packet);
  handler->deal_autocommit_with_ok_eof_packet(ok_packet);
  try {
    handler->send_to_client(ok_packet);
    delete ok_packet;
  } catch (exception &e) {
    delete ok_packet;
    throw e;
  }
}

/* class   MySQLNormalMergeNode */

MySQLNormalMergeNode::MySQLNormalMergeNode(ExecutePlan *plan,
                                           const char *modify_sql,
                                           vector<ExecuteNode *> *nodes)
    : MySQLInnerNode(plan),
      modify_sql(modify_sql),
      init_column_flag(false),
      select_complete_handled(false) {
  this->name = "MySQLNormalMergeNode";
  field_num = 0;
  if (nodes) {
    vector<ExecuteNode *>::iterator it = nodes->begin();
    for (; it != nodes->end(); it++) {
      this->add_select_child(*it);
    }
  }
  max_sql_len = MAX_PACKET_SIZE - PACKET_HEADER_SIZE - 4096;
  // this node don't need to control the RAM.
  plan->session->set_has_fetch_node(false);
  replace_set_value = false;
  generated_sql_length = insert_select_sql_size - PACKET_HEADER_SIZE;
}

void MySQLNormalMergeNode::clean() {
  if (!select_complete_handled && !session->is_in_explain())
    handle_select_complete();
  MySQLExecuteNode *free_node;
  Packet *packet = NULL;
  while (!select_node_children.empty()) {
    free_node = select_node_children.front();
    select_node_children.pop_front();

    // clean remaining rows
    if (row_map.count(free_node)) {
      while (!row_map[free_node]->empty()) {
        packet = row_map[free_node]->front();
        row_map[free_node]->pop_front();
        delete packet;
      }
      delete row_map[free_node];
      row_map[free_node] = NULL;
    }

    free_node->clean();
    delete free_node;
  }
  // we should ensure that MySQLNormalMergeNode::clean should handle the fetch
  // node first, and ensure the fetch node has free the resource, then handle
  // the clean of modify node.
  MySQLInnerNode::clean();

  while (!modify_node_children_for_explain_stmt.empty()) {
    free_node = modify_node_children_for_explain_stmt.front();
    modify_node_children_for_explain_stmt.pop_front();
    free_node->clean();
    delete free_node;
  }
}

void MySQLNormalMergeNode::select_node_children_execute() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    (*it)->execute();
    (*it)->start_thread();
  }
  it = select_node_children.begin();
  if (strcmp((*it)->get_executenode_name(), "MySQLSparkNode")) {
    set_select_node_children_thread_status_start();
    plan->start_all_bthread();
  }
}

void MySQLNormalMergeNode::set_select_node_children_thread_status_start() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    (*it)->set_thread_status_started();
  }
}

void MySQLNormalMergeNode::wait_children_select_node() {
  /*Check the modify children nodes's status, if they got error, stop the
   * processing.*/
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); it++) {
    if ((*it)->status == EXECUTE_STATUS_COMPLETE) {
      /*If the status is EXECUTE_STATUS_COMPLETE, this modify node must
       * goterror, so invoke notify_parent() to throw the exception.*/
      (*it)->notify_parent();
    }
  }
  if (plan->session->get_keep_conn()) {
#ifdef DEBUG
    if (max_mergenode_ready_rows_size && received_packet_num > 0)
      ACE_ASSERT(max_mergenode_buffer_rows > 0);
#endif
    if (max_mergenode_ready_rows_size &&
        max_mergenode_buffer_rows <= received_packet_num &&
        received_packet_num > 0) {
      LOG_ERROR(
          "MySQLNormalMergeNode in kept_conn state, receive [%d] packets, "
          "larger than config size [%d],"
          "plz config option 'max-mergenode-ready-rows-size'\n",
          received_packet_num, max_mergenode_ready_rows_size);
      throw Error(
          "MySQLNormalMergeNode receive packets larger than config size.");
    }
  }
  bool ready_flag = false;
  unsigned int finished_child = 0;
  while (!ready_flag) {
    finished_child = 0;
    for (it = select_node_children.begin(); it != select_node_children.end();
         ++it) {
      if ((*it)->notify_parent()) {
        ready_flag = true;
        status = EXECUTE_STATUS_SELECT_HANDLE;
      } else if ((*it)->is_finished()) {
        finished_child++;
      }
    }

    /* check if all chilren are finishd */
    if (!ready_flag && finished_child == select_node_children.size()) {
      status = EXECUTE_STATUS_SELECT_COMPLETE;
      break;
    }
  }
}

bool is_column_type_present_as_str(MySQLColumnType type) {
  switch (type) {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_JSON:
      return true;
    default:
      return false;
  }
  return false;
}

void MySQLNormalMergeNode::init_column_as_str() {
  if (!init_column_flag) {
    list<Packet *> *field_packets =
        select_node_children.front()->get_field_packets();
    list<Packet *>::iterator it_field;
    for (it_field = field_packets->begin(); it_field != field_packets->end();
         it_field++) {
      MySQLColumnResponse col_resp(*it_field);
      col_resp.unpack();
      column_as_str.push_back(
          is_column_type_present_as_str(col_resp.get_column_type()));
    }

    LOG_DEBUG(
        "MySQLNormalMergeNode Init column to check as_str :"
        " columns = %d\n",
        column_as_str.size());
    init_column_flag = true;
  }
}

void MySQLNormalMergeNode::execute() {
  try {
    while (status != EXECUTE_STATUS_COMPLETE) {
      // 1. execute select_node_children, 2. wait_children select node
      // children, 3. assemble one modify sql, 4. children add modify sql, 5.
      // execute children;
      //
      // loop 1->5 until select_node_children all finish
      //
      // 6. wait_children children 7. return ok packets to parent node
      //
      // loop 6->7 until children all finish and all packets are passed

      switch (status) {
        case EXECUTE_STATUS_START: {
          list<MySQLExecuteNode *>::iterator it;
          init_row_map();
          for (it = select_node_children.begin();
               it != select_node_children.end(); ++it) {
            row_map[*it] = new AllocList<Packet *, Session *,
                                         StaticAllocator<Packet *> >();
            // TODO: add init value for row_map_size and
            // turn need_restrict_row_map_size on when need restrict row_map's
            // size.
            row_map[*it]->set_session(plan->session);
          }

          status = EXECUTE_STATUS_SELECT_FETCH_DATA;
          break;
        }
        case EXECUTE_STATUS_SELECT_FETCH_DATA: {
          select_node_children_execute();
          status = EXECUTE_STATUS_SELECT_WAIT;
        }
        case EXECUTE_STATUS_SELECT_WAIT: {
          try {
            wait_children_select_node();
          } catch (ExecuteNodeError &e) {
            children_modify_end();
            LOG_ERROR(
                "MySQLNormalMergeNode wait_children_select_node get error "
                "[%s]\n",
                e.what());
            string msg =
                "MySQLNormalMergeNode wait_children_select_node get error ";
            msg += e.what();
            record_migrate_error_message(plan, msg);
            status = EXECUTE_STATUS_COMPLETE;
            throw e;
          }
          break;
        }
        case EXECUTE_STATUS_SELECT_HANDLE: {
          // for update set, wait for all fetch node finished
          if (!replace_set_value) {
            init_column_as_str();
            handle_select_node_children();
          } else {
            for (auto it = select_node_children.begin();
                 it != select_node_children.end(); ++it) {
              if (row_map[*it]->size() > 1) {
                LOG_ERROR("Subquery returns more than 1 row\n");
                throw ExecuteNodeError("Subquery returns more than 1 row.");
              }
            }
          }
#ifndef DBSCALE_TEST_DISABLE
          /*just for test coverage*/
          Backend *bk = Backend::instance();
          dbscale_test_info *test_info = bk->get_dbscale_test_info();
          if (!strcasecmp(test_info->test_case_name.c_str(),
                          "insert_select_test") &&
              !strcasecmp(test_info->test_case_operation.c_str(),
                          "handle_select_get_error")) {
            throw Error("handle_select_get_error");
          }
#endif
          if (!replace_set_value) children_execute();
          status = EXECUTE_STATUS_SELECT_FETCH_DATA;
          break;
        }
        case EXECUTE_STATUS_SELECT_COMPLETE:
          LOG_DEBUG("Node %@ finish the select part.\n", this);
          if (replace_set_value) {
            init_column_as_str();
            handle_select_node_children();
            children_execute();
          }
          handle_select_complete();
          select_complete_handled = true;
          status = EXECUTE_STATUS_WAIT;
        case EXECUTE_STATUS_FETCH_DATA:
          status = EXECUTE_STATUS_WAIT;
        case EXECUTE_STATUS_WAIT:
          try {
            wait_children();
          } catch (ExecuteNodeError &e) {
            children_modify_end();
            LOG_ERROR("MySQLNormalMergeNode wait_children get error [%s]\n",
                      e.what());
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
          if (ready_rows->empty() && all_children_finished) {
            LOG_DEBUG(
                "Modify merge node read_rows is 0 and all children "
                "finished.\n");
            status = EXECUTE_STATUS_COMPLETE;
          } else {
            status = EXECUTE_STATUS_FETCH_DATA;
            LOG_DEBUG("Modify merge node gets ready rows %d.\n",
                      ready_rows->size());
            return;
          }
          break;
        case EXECUTE_STATUS_BEFORE_COMPLETE:
          status = EXECUTE_STATUS_COMPLETE;
        case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
          LOG_DEBUG("MySQLNormalMergeNode %@ cost %d ms\n", this,
                    node_cost_time);
#endif
          break;

        default:
          break;
      }
    }
  } catch (exception &e) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR("MySQLNormalMergeNode execute get error [%s]\n", e.what());
    string err("MySQLNormalMergeNode execute get error:");
    err.append(e.what());
    record_migrate_error_message(plan, err.c_str());
    throw;
  }
}

void MySQLNormalMergeNode::handle_children() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (!row_map[*it]->empty()) {
      handle_child(*it);
    }
  }
}

void MySQLNormalMergeNode::handle_child(MySQLExecuteNode *child) {
  Packet *packet = NULL;
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    ready_rows->push_back(packet);
  }
}

Packet *MySQLNormalMergeNode::get_error_packet() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it)->get_error_packet()) {
      return (*it)->get_error_packet();
    }
  }

  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    if ((*it)->get_error_packet()) {
      return (*it)->get_error_packet();
    }
  }

  return NULL;
}

void MySQLNormalMergeNode::rotate_sql(MySQLModifyNode *node, string *sql,
                                      string *added_sql) {
  child_add_sql(node, sql->c_str(), get_sql_length(sql));
  sql->clear();
  sql->append(modify_sql.c_str());
  if (added_sql) sql->append(added_sql->c_str());
}

void MySQLNormalMergeNode::append_field_to_sql(string *sql, int field_pos,
                                               MySQLRowResponse *row,
                                               bool handle_hex) {
  uint64_t field_length = (uint64_t)0;
  const char *field = NULL;
  bool is_field_as_str = false;
  bool is_null = false;
  is_null = row->field_is_null(field_pos);
  if (!is_null) {
    field = row->get_str(field_pos, &field_length);
    is_field_as_str = column_as_str[field_pos];
    add_str_before_field_value(sql, is_null);

    if (handle_hex) {
      sql->append("0x");
      sql->append(field, field_length);
    } else {
      if (is_field_as_str) {
        sql->append("'");
        string tmp_str(field, field_length);
        CharsetType ctype = session->get_client_charset_type();
        deal_with_str_column_value(&tmp_str, ctype);
        sql->append(tmp_str.c_str());
        sql->append("'");
      } else
        sql->append(field, field_length);
    }
  } else {
    add_str_before_field_value(sql, is_null);
    sql->append("NULL");
  }
}

/* class MySQLModifySelectNode */

MySQLModifySelectNode::MySQLModifySelectNode(
    ExecutePlan *plan, const char *modify_sql, vector<string *> *columns,
    vector<ExecuteNode *> *nodes, bool execute_quick, bool is_replace_set_value)
    : MySQLNormalMergeNode(plan, modify_sql, nodes),
      columns(columns),
      execute_quick(execute_quick) {
  this->name = "MySQLModifySelectNode";
  execute_sql.append(this->modify_sql.c_str());
  migrate_iter = children.begin();
  replace_set_value = is_replace_set_value;
}

void MySQLModifySelectNode::clean() {
  MySQLNormalMergeNode::clean();
  vector<string *>::iterator it = columns->begin();
  for (; it != columns->end(); it++) {
    delete *it;
  }
  columns->clear();
  delete columns;
}

void MySQLModifySelectNode::handle_select_node_child(MySQLExecuteNode *child,
                                                     string *sql) {
  LOG_DEBUG("UpdateSelectNode %@ start to assemble modify sql.\n", this);
  Packet *packet = NULL;

  if (replace_set_value) {
    string replace_value;
    if (column_as_str.size() > 1) {
      LOG_ERROR("Operand should contain 1 column\n");
      throw ExecuteNodeError("Operand should contain 1 column(s)");
    }
    if (row_map[child]->size() > 1) {
      LOG_ERROR("Subquery returns more than 1 row\n");
      throw ExecuteNodeError("Subquery returns more than 1 row.");
    } else if (row_map[child]->empty()) {
      replace_value = "= NULL";
    } else {
      packet = row_map[child]->front();
      row_map[child]->pop_front();
      MySQLRowResponse row(packet);
      bool is_null = row.field_is_null(0);
      if (is_null)
        replace_value = "= NULL";
      else
        append_field_to_sql(&replace_value, 0, &row);
      delete packet;
    }
    size_t pos = sql->find(DBSCALE_TMP_COLUMN_VALUE);
    if (pos != string::npos) {
      sql->replace(pos, strlen(DBSCALE_TMP_COLUMN_VALUE), replace_value);
    }
    return;
  }
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row(packet);
    unsigned int i = 0;
    string tmp(" ");
    tmp.append("(");
    tmp.append(columns->at(i)->c_str());
    append_field_to_sql(&tmp, i, &row);
    i++;
    for (; i < field_num; i++) {
      tmp.append(" and ");
      tmp.append(columns->at(i)->c_str());
      append_field_to_sql(&tmp, i, &row);
    }
    tmp.append(") OR");
    delete packet;
    if (!is_sql_len_valid(sql->length() + tmp.length())) {
      if (execute_quick && is_sql_len_valid(tmp.length()))
        rotate_modify_sql(sql, &tmp);
      else {
        LOG_ERROR(
            "Generated temporary SQL for %s is too large, dbscale unsupport"
            "currently.\n",
            plan->statement->get_sql());
        throw ExecuteNodeError(
            "Unsupport sql for dbscale, due to too large temporary SQL, \
                               please check the log for detail.");
      }
    } else
      sql->append(tmp.c_str());
  }
}

void MySQLModifySelectNode::rotate_modify_sql(string *sql, string *added_sql) {
  if (plan->get_migrate_tool()) {
    MySQLModifyNode *node = get_next_migrate_node();
    rotate_sql(node, sql, added_sql);
  } else {
    rotate_sql((MySQLModifyNode *)children.front(), sql, added_sql);
  }
}
MySQLModifyNode *MySQLModifySelectNode::get_next_migrate_node() {
  migrate_iter++;
  if (migrate_iter == children.end()) {
    migrate_iter = children.begin();
  }
  return (MySQLModifyNode *)*migrate_iter;
}

void MySQLModifySelectNode::handle_select_node_children() {
  if (!field_num) get_filed_num();

  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    if (!row_map[*it]->empty() || replace_set_value)
      handle_select_node_child(*it, &execute_sql);
  }
  if (execute_quick && (execute_sql.size() > modify_sql.size())) {
    rotate_modify_sql(&execute_sql, NULL);
  }
  LOG_DEBUG("UpdateSelectNode %@ assemble sql [%s].\n", this,
            execute_sql.c_str());
}

/* class MySQLInsertSelectNode */

MySQLInsertSelectNode::MySQLInsertSelectNode(ExecutePlan *plan,
                                             const char *modify_sql,
                                             vector<ExecuteNode *> *nodes)
    : MySQLNormalMergeNode(plan, modify_sql, nodes) {
  this->name = "MySQLInsertSelectNode";
  modify_sql_exe.clear();
  modify_sql_exe.append(modify_sql);
  migrate_iter = children.begin();
  cross_join_max_rows = (uint64_t)(
      session->get_session_option("max_cross_join_moved_rows").ulong_val);
  select_row_num = 0;
}

void MySQLInsertSelectNode::handle_select_node_child(MySQLExecuteNode *child,
                                                     string *sql) {
  Packet *packet = NULL;
  select_row_num += row_map[child]->size();
  if (plan->statement->is_cross_node_join() &&
      select_row_num > cross_join_max_rows) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR(
        "Reach the max rows of [%u] for cross node join max moved rows.\n",
        select_row_num);
    throw ExecuteNodeError(
        "Reach the max row number of cross node join max moved rows.");
  }
  set<int> &hex_pos = plan->statement->get_hex_pos();
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row(packet);
    unsigned int i = 0;
    string tmp("");
    tmp.append("(");
    append_field_to_sql(&tmp, i, &row, hex_pos.count(i));
    i++;
    for (; i < field_num; i++) {
      tmp.append(",");
      append_field_to_sql(&tmp, i, &row, hex_pos.count(i));
    }
    tmp.append("),");
    delete packet;
    if (!is_sql_len_valid(sql->length() + tmp.length()))
      if (is_sql_len_valid(tmp.length())) {
        rotate_insert_sql(sql, &tmp);
      } else {
        LOG_ERROR(
            "Generated temporary SQL for %s is too large, dbscale unsupport"
            " currently.\n",
            plan->statement->get_sql());
        throw ExecuteNodeError(
            "Unsupport sql for dbscale,  due to too large temporary SQL, \
                               please check the log for detail.");
      }
    else
      sql->append(tmp.c_str());
  }
}

void MySQLInsertSelectNode::rotate_insert_sql(string *sql, string *added_sql) {
  if (plan->get_migrate_tool()) {
    MySQLModifyNode *node = get_next_migrate_node();
    rotate_sql(node, sql, added_sql);
  } else {
    rotate_sql((MySQLModifyNode *)children.front(), sql, added_sql);
  }
}
MySQLModifyNode *MySQLInsertSelectNode::get_next_migrate_node() {
  migrate_iter++;
  if (migrate_iter == children.end()) {
    migrate_iter = children.begin();
  }
  return (MySQLModifyNode *)*migrate_iter;
}
void MySQLInsertSelectNode::handle_select_node_children() {
  LOG_DEBUG("InsertSelectNode %@ start to assemble modify sql.\n", this);

  if (!field_num) get_filed_num();

  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    if (!row_map[*it]->empty()) handle_select_node_child(*it, &modify_sql_exe);
  }
  if (is_sql_len_enough(modify_sql_exe.length()))
    rotate_insert_sql(&modify_sql_exe, NULL);
  LOG_DEBUG("InsertSelectNode %@ assemble sql [%s].\n", this,
            modify_sql_exe.c_str());
}

/* class MySQLPartitionMergeNode*/

MySQLPartitionMergeNode::MySQLPartitionMergeNode(ExecutePlan *plan,
                                                 const char *modify_sql,
                                                 vector<ExecuteNode *> *nodes,
                                                 PartitionedTable *par_table,
                                                 PartitionMethod *method)
    : MySQLNormalMergeNode(plan, modify_sql, nodes),
      par_table(par_table),
      method(method) {
  this->name = "MySQLPartitionMergeNode";
  if (plan->session->is_in_explain()) {
    unsigned int partition_num = par_table->get_real_partition_num();
    for (unsigned int i = 0; i < partition_num; i++) {
      DataSpace *space = par_table->get_partition(i);
      MySQLModifyNode *modify = new MySQLModifyNode(this->plan, space);
      add_modify_child_for_explain_stmt(modify);
    }
  }
}

unsigned int MySQLPartitionMergeNode::get_par_id_one_row(
    MySQLRowResponse *row) {
  vector<const char *> keys;
  map<unsigned int, string> key_value_string;
  uint64_t field_length = (uint64_t)0;
  const char *field = NULL;
  vector<unsigned int>::iterator it;
  for (it = key_pos_vec.begin(); it != key_pos_vec.end(); it++) {
    field = row->get_str(*it, &field_length);
    key_value_string[*it].clear();
    key_value_string[*it].append(field, field_length);
    keys.push_back(key_value_string[*it].c_str());
  }

  string sql_replace_char = plan->session->get_query_sql_replace_null_char();
  return method->get_partition_id(&keys, sql_replace_char);
}

unsigned int MySQLPartitionMergeNode::get_par_id_one_row(
    MySQLRowResponse *row, int auto_inc_key_pos, int64_t curr_auto_inc_val) {
  vector<const char *> keys;
  map<unsigned int, string> key_value_string;
  uint64_t field_length = (uint64_t)0;
  const char *field = NULL;
  vector<unsigned int>::iterator it;
  for (it = key_pos_vec.begin(); it != key_pos_vec.end(); it++) {
    if ((int)*it == auto_inc_key_pos || *it == field_num) {
      // *it == field_num means has auto_increment field but not specified in
      // column_list.
      char auto_inc_str_val[21];
      snprintf(auto_inc_str_val, sizeof(auto_inc_str_val), "%ld",
               curr_auto_inc_val);
      key_value_string[*it].clear();
      key_value_string[*it].append(auto_inc_str_val, strlen(auto_inc_str_val));
      keys.push_back(key_value_string[*it].c_str());
    } else {
      field = row->get_str(*it, &field_length);
      key_value_string[*it].clear();
      key_value_string[*it].append(field, field_length);
      keys.push_back(key_value_string[*it].c_str());
    }
  }

  string sql_replace_char = plan->session->get_query_sql_replace_null_char();
  return method->get_partition_id(&keys, sql_replace_char);
}

void MySQLPartitionMergeNode::children_add_sql() {
  map<unsigned int, MySQLModifyNode *>::iterator it;
  for (it = modify_node_map.begin(); it != modify_node_map.end(); it++) {
    unsigned int id = it->first;
    if (node_sql_map[id].length() > modify_sql.length()) {
      MySQLModifyNode *node = it->second;
      child_add_sql(node, node_sql_map[id].c_str(),
                    get_sql_length(&node_sql_map[id]));
      node_sql_map[id].clear();
      node_sql_map[id].append(modify_sql.c_str());
    }
  }
}

/* MySQLPartitionModifySelectNode */
MySQLPartitionModifySelectNode::MySQLPartitionModifySelectNode(
    ExecutePlan *plan, const char *modify_sql, vector<ExecuteNode *> *nodes,
    PartitionedTable *par_table, PartitionMethod *method,
    vector<string *> *columns, bool execute_quick)
    : MySQLPartitionMergeNode(plan, modify_sql, nodes, par_table, method),
      columns(columns),
      execute_quick(execute_quick) {
  this->name = "MySQLPartitionModifySelectNode";
  fulfill_key_pos_one_row(&key_pos_vec);
}

void MySQLPartitionModifySelectNode::handle_select_node_child(
    MySQLExecuteNode *child) {
  Packet *packet = NULL;
  unsigned int par_id;
  unsigned int i;
  string *sql;

  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row(packet);
    par_id = get_par_id_one_row(&row);
    par_id = par_table->get_real_par_id_from_virtual_id(par_id);
    get_partiton_modify_node(par_id);
    sql = &node_sql_map[par_id];
    string tmp("");
    i = 0;
    tmp.append(" ");

    tmp.append("(");
    tmp.append(columns->at(i)->c_str());
    append_field_to_sql(&tmp, i, &row);
    i++;
    for (; i < field_num; i++) {
      tmp.append(" and ");
      tmp.append(columns->at(i)->c_str());
      append_field_to_sql(&tmp, i, &row);
    }
    tmp.append(") OR");
    delete packet;
    if (!is_sql_len_valid(sql->length() + tmp.length())) {
      if (execute_quick && is_sql_len_valid(tmp.length()))
        rotate_sql(modify_node_map[par_id], sql, &tmp);
      else {
        LOG_ERROR(
            "Generated temporary SQL for %s is too large, dbscale unsupport"
            " currently.\n",
            plan->statement->get_sql());
        throw ExecuteNodeError(
            "Unsupport sql for dbscale,  due to too large temporary SQL, \
                               please check the log for detail.");
      }
    } else
      sql->append(tmp.c_str());
  }
}

void MySQLPartitionModifySelectNode::handle_select_node_children() {
  if (!field_num) get_filed_num();

  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    if (!row_map[*it]->empty()) handle_select_node_child(*it);
  }

  if (execute_quick) {
    children_add_sql();
  }
}

/* MySQLPartitionInsertSelectNode */
MySQLPartitionInsertSelectNode::MySQLPartitionInsertSelectNode(
    ExecutePlan *plan, const char *modify_sql, vector<ExecuteNode *> *nodes,
    PartitionedTable *par_table, PartitionMethod *method,
    vector<unsigned int> &key_pos, const char *schema_name,
    const char *table_name, bool is_duplicated)
    : MySQLPartitionMergeNode(plan, modify_sql, nodes, par_table, method),
      is_duplicated(is_duplicated) {
  this->name = "MySQLPartitionInsertSelectNode";
  key_pos_vec = key_pos;
  schema_name_insert.append(schema_name);
  table_name_insert.append(table_name);
  auto_increment_key_pos = plan->statement->get_auto_increment_key_pos();
  need_update_last_insert_id = false;
  need_auto_inc_lock = true;
  stmt_lock = NULL;

  has_init_duplicated_modify_node = false;
  cross_join_max_rows = (uint64_t)(
      session->get_session_option("max_cross_join_moved_rows").ulong_val);
  select_row_num = 0;
}

void MySQLPartitionInsertSelectNode::auto_inc_prepare() {
  if (plan->statement->get_auto_inc_status() != NO_AUTO_INC_FIELD) {
    stmt_lock = par_table->get_stmt_autoinc_lock(
        plan->statement->get_full_table_name());
    if (stmt_lock) stmt_lock->acquire();
    int enable_last_insert_id_session =
        session->get_session_option("enable_last_insert_id").int_val;
    if (enable_last_insert_id_session) {
      need_update_last_insert_id = true;
      plan->statement->set_old_last_insert_id(
          plan->handler->get_session()->get_last_insert_id());
    }
  }
  need_auto_inc_lock = false;
}

int64_t MySQLPartitionInsertSelectNode::get_auto_increment_value(
    MySQLRowResponse *row) {
  if (plan->statement->get_auto_inc_status() == AUTO_INC_NOT_SPECIFIED) {
    return plan->statement->get_auto_inc_value_multi_insert_row(
        par_table, plan, schema_name_insert.c_str(), table_name_insert.c_str(),
        NULL);
  } else {
    uint64_t field_length = 0;
    string field_string;
    if (row->field_is_null(auto_increment_key_pos)) {
      field_string.append("NULL");
    } else {
      const char *field_str =
          row->get_str(auto_increment_key_pos, &field_length);
      field_string.append(field_str, field_length);
    }
    StrExpression field_expr = StrExpression(field_string.c_str(), EXPR_STRING);
    return plan->statement->get_auto_inc_value_multi_insert_row(
        par_table, plan, schema_name_insert.c_str(), table_name_insert.c_str(),
        &field_expr);
  }
}

unsigned int MySQLPartitionInsertSelectNode::get_part_id(
    MySQLRowResponse *row) {
  unsigned int par_id = 0;
  AutoIncStatus auto_inc_status = plan->statement->get_auto_inc_status();
  if (auto_inc_status != NO_AUTO_INC_FIELD &&
      auto_inc_status != AUTO_INC_NO_NEED_MODIFY) {
    par_id = get_par_id_one_row(row, auto_increment_key_pos, curr_auto_inc_val);
  } else {
    par_id = get_par_id_one_row(row);
  }
  par_id = par_table->get_real_par_id_from_virtual_id(par_id);
  return par_id;
}

void MySQLPartitionInsertSelectNode::build_value_list_str(
    string &str, MySQLRowResponse *row) {
  unsigned int i = 0;
  str.append("(");
  AutoIncStatus auto_inc_status = plan->statement->get_auto_inc_status();
  for (; i < field_num; i++) {
    if (auto_inc_status == AUTO_INC_VALUE_NULL &&
        auto_increment_key_pos == (int)i) {
      char auto_inc_str_val[21];
      snprintf(auto_inc_str_val, sizeof(auto_inc_str_val), "%ld",
               curr_auto_inc_val);
      str.append(auto_inc_str_val);
    } else {
      append_field_to_sql(&str, i, row);
    }
    str.append(",");
  }
  if (auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    char auto_inc_str_val[21];
    snprintf(auto_inc_str_val, sizeof(auto_inc_str_val), "%ld",
             curr_auto_inc_val);
    str.append(auto_inc_str_val);
  } else {
    boost::erase_tail(str, 1);
  }
  str.append("),");
}

void MySQLPartitionInsertSelectNode::update_last_insert_id() {
  AutoIncStatus auto_inc_status = plan->statement->get_auto_inc_status();
  if (auto_inc_status == AUTO_INC_VALUE_NULL ||
      auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    plan->handler->get_session()->set_last_insert_id(curr_auto_inc_val);
    need_update_last_insert_id = false;
  }
}

void MySQLPartitionInsertSelectNode::handle_select_node_child(
    MySQLExecuteNode *child) {
  Packet *packet = NULL;
  unsigned int par_id;
  string *sql;
  select_row_num += row_map[child]->size();
  if (plan->statement->is_cross_node_join() &&
      select_row_num > cross_join_max_rows) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR(
        "Reach the max rows of [%u] for cross node join max moved rows.\n",
        select_row_num);
    throw ExecuteNodeError(
        "Reach the max row number of cross node join max moved rows.");
  }
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row(packet);
    string tmp;
    try {
      if (plan->statement->get_auto_inc_status() != NO_AUTO_INC_FIELD) {
        curr_auto_inc_val = get_auto_increment_value(&row);
      }
      par_id = get_part_id(&row);
      get_partiton_modify_node(par_id);
      sql = &node_sql_map[par_id];

      build_value_list_str(tmp, &row);
      delete packet;
    } catch (...) {
      delete packet;
      if (plan->statement->get_auto_inc_status() != NO_AUTO_INC_FIELD) {
        plan->statement->rollback_auto_inc_params(plan, par_table);
      }
      throw;
    }

    if (need_update_last_insert_id) update_last_insert_id();

    if (!is_sql_len_valid(sql->length() + tmp.length()))
      if (is_sql_len_valid(tmp.length()))
        rotate_sql(modify_node_map[par_id], sql, &tmp);
      else {
        LOG_ERROR(
            "Generated temporary SQL for %s is too large, dbscale unsupport"
            " currently.\n",
            plan->statement->get_sql());
        throw ExecuteNodeError(
            "Unsupport sql for dbscale,  due to too large temporary SQL, \
                               please check the log for detail.");
      }
    else
      sql->append(tmp.c_str());

    if (is_sql_len_enough(sql->length()))
      rotate_sql(modify_node_map[par_id], sql, NULL);
  }
}

void MySQLPartitionInsertSelectNode::handle_select_node_child_duplicated(
    MySQLExecuteNode *child) {
  if (!has_init_duplicated_modify_node) {
    unsigned int num = par_table->get_real_partition_num();
    for (unsigned int i = 0; i < num; i++) {
      DataSpace *space = par_table->get_partition(i);
      if (space->get_virtual_machine_id() > 0) continue;

      get_partiton_modify_node(i);
    }
    has_init_duplicated_modify_node = true;
  }
  Packet *packet = NULL;
  string *sql;
  select_row_num += row_map[child]->size();
  if (plan->statement->is_cross_node_join() &&
      select_row_num > cross_join_max_rows) {
    status = EXECUTE_STATUS_COMPLETE;
    LOG_ERROR(
        "Reach the max rows of [%u] for cross node join max moved rows.\n",
        select_row_num);
    throw ExecuteNodeError(
        "Reach the max row number of cross node join max moved rows.");
  }
  while (!row_map[child]->empty()) {
    packet = row_map[child]->front();
    row_map[child]->pop_front();
    MySQLRowResponse row(packet);
    string tmp;
    build_value_list_str(tmp, &row);
    try {
      map<unsigned int, MySQLModifyNode *>::iterator it =
          modify_node_map.begin();
      for (; it != modify_node_map.end(); it++) {
        sql = &node_sql_map[it->first];
        if (!is_sql_len_valid(sql->length() + tmp.length()))
          if (is_sql_len_valid(tmp.length()))
            rotate_sql(it->second, sql, &tmp);
          else {
            LOG_ERROR(
                "Generated temporary SQL for %s is too large, dbscale unsupport"
                " currently.\n",
                plan->statement->get_sql());
            throw ExecuteNodeError(
                "Unsupport sql for dbscale,  due to too large temporary SQL, \
                                   please check the log for detail.");
          }
        else
          sql->append(tmp.c_str());

        if (is_sql_len_enough(sql->length())) rotate_sql(it->second, sql, NULL);
      }
      delete packet;
    } catch (...) {
      delete packet;
      if (plan->statement->get_auto_inc_status() != NO_AUTO_INC_FIELD) {
        plan->statement->rollback_auto_inc_params(plan, par_table);
      }
      throw;
    }
  }
}

void MySQLPartitionInsertSelectNode::handle_select_node_children() {
  LOG_DEBUG("PartitionInsertSelectNode %@ start to assemble modify sql.\n",
            this);

  if (!field_num) get_filed_num();

  if (need_auto_inc_lock) auto_inc_prepare();

  list<MySQLExecuteNode *>::iterator it;
  for (it = select_node_children.begin(); it != select_node_children.end();
       ++it) {
    if (!row_map[*it]->empty()) {
      if (is_duplicated)
        handle_select_node_child_duplicated(*it);
      else
        handle_select_node_child(*it);
    }
  }
}

/* some global functions */

void handle_max_min(Packet *packet, Packet *field_packet,
                    vector<MySQLRowResponse *> *mysql_rows,
                    unsigned column_index, MySQLColumnType column_type,
                    bool is_max) {
  ACE_UNUSED_ARG(field_packet);
  list<MySQLExecuteNode *>::iterator it;
  MySQLRowResponse *max_row = NULL;
  MySQLRowResponse *row = NULL;
  unsigned int i, n;
  n = mysql_rows->size();
  for (i = 0; i < n; i++) {
    row = (*mysql_rows)[i];
    if (!row->field_is_null(column_index)) {
      max_row = row;
      break;
    }
  }

  if (max_row) {
    SortDesc sort_desc;
    sort_desc.column_index = column_index;
    sort_desc.sort_order = is_max ? ORDER_DESC : ORDER_ASC;
    sort_desc.is_cs = true;
    sort_desc.ctype = CHARSET_TYPE_OTHER;
    for (i++; i < n; i++) {
      row = (*mysql_rows)[i];
      if (!row->field_is_null(column_index)) {
        if (MySQLColumnCompare::compare(max_row, row, sort_desc, column_type) ==
            -1) {
          max_row = row;
        }
      }
    }
  } else {
    max_row = (*mysql_rows)[0];
  }

  char *max_data;
  uint64_t max_data_len;
  uint8_t max_header_len;
  max_row->set_current(column_index);
  max_data = max_row->get_current_data();
  max_data_len = max_row->get_current_length();
  max_header_len = max_row->get_current_header_length();
  max_row->move_next();

  if (packet->size() - (packet->wr_ptr() - packet->base()) <
      1 + max_header_len + max_data_len) {
    uint64_t resize_len =
        1 + max_header_len + max_data_len + packet->wr_ptr() - packet->base();
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              packet->size(), resize_len);
    packet->size(resize_len);
  }
  packet->packdata(max_data - max_header_len, max_header_len + max_data_len);
}

void copy_column_to_packet(MySQLRowResponse *row, unsigned int column_index,
                           Packet *packet) {
  char *start_pos, *end_pos;
  row->set_current(column_index);
  start_pos = row->get_current_data() - row->get_current_header_length();
  end_pos = row->get_current_data() + row->get_current_length();
  if (packet->size() - (packet->wr_ptr() - packet->base()) <
      (unsigned int)(end_pos - start_pos + 1)) {
    uint64_t resize_len =
        end_pos - start_pos + 1 + packet->wr_ptr() - packet->base();
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              packet->size(), resize_len);
    packet->size(resize_len);
  }
  packet->packdata(start_pos, end_pos - start_pos);
  row->move_next();
}

void handle_certain_max(Packet *packet, Packet *field_packet,
                        vector<MySQLRowResponse *> *mysql_rows,
                        unsigned column_index, MySQLColumnType column_type) {
  ACE_UNUSED_ARG(field_packet);
  list<MySQLExecuteNode *>::iterator it;
  MySQLRowResponse *max_row = NULL;
  MySQLRowResponse *row = NULL;
  unsigned int i, rows_count;
  rows_count = mysql_rows->size();
  for (i = 0; i < rows_count; i++) {
    row = (*mysql_rows)[i];
    if (!row->field_is_null(column_index)) {
      max_row = row;
      break;
    }
  }

  if (max_row) {
    SortDesc sort_desc;
    sort_desc.column_index = column_index;
    sort_desc.sort_order = ORDER_DESC;
    sort_desc.is_cs = true;
    sort_desc.ctype = CHARSET_TYPE_OTHER;
    for (i++; i < rows_count; i++) {
      row = (*mysql_rows)[i];
      if (!row->field_is_null(column_index)) {
        if (MySQLColumnCompare::compare(max_row, row, sort_desc, column_type) ==
            -1) {
          max_row = row;
        }
      }
    }
  } else {
    max_row = (*mysql_rows)[0];
  }
  char *data;
  char *max_data;
  uint64_t max_data_len;
  data = max_row->get_row_data();
  max_row->set_current(column_index);
  max_data = max_row->get_current_data();
  max_data_len = max_row->get_current_length();

  if (packet->size() < max_data - data + max_data_len) {
    uint64_t resize_len = 1 + max_data - data + max_data_len;
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              packet->size(), resize_len);
    packet->size(resize_len);
  }
  packet->rewind();
  packet->wr_ptr(PACKET_HEADER_SIZE);
  for (unsigned int i = 0; i <= column_index; i++) {
    copy_column_to_packet(max_row, i, packet);
  }
}

void handle_max(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type) {
  return handle_max_min(packet, field_packet, mysql_rows, column_index,
                        column_type, true);
}

void handle_min(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type) {
  return handle_max_min(packet, field_packet, mysql_rows, column_index,
                        column_type, false);
}

void handle_sum(Packet *packet, Packet *field_packet,
                vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                MySQLColumnType column_type) {
  ACE_UNUSED_ARG(column_type);
  mpf_t sum, tmp_mpf;
  mpf_init2(sum, DECIMAL_STORE_BIT);
  mpf_init2(tmp_mpf, DECIMAL_STORE_BIT);
  MySQLRowResponse *row;
  bool all_sum_is_null = true;
  unsigned int n = mysql_rows->size();
  for (unsigned int i = 0; i < n; i++) {
    row = (*mysql_rows)[i];
    if (!row->field_is_null(column_index)) {
      all_sum_is_null = false;
      row->get_mpf(column_index, tmp_mpf);
      mpf_add(sum, sum, tmp_mpf);
      char str2[200];
      size_t len = gmp_sprintf(str2, "%.Ff", tmp_mpf);
      str2[len] = '\0';
      char str3[200];
      len = gmp_sprintf(str3, "%.Ff", sum);
      str3[len] = '\0';
    }
  }
  if (!all_sum_is_null) {
    char str[200];
    size_t len = gmp_sprintf(str, "%.Ff", sum);
    if (len > 33) {
      if (mpf_sgn(sum) == 0) {
        len = sprintf(str, "%0.2f", 0.00);
      } else {
        if (field_packet) {
          MySQLColumnResponse col_resp(field_packet);
          col_resp.unpack();
          int scale = int(col_resp.get_decimals());
          char formator[10];
          sprintf(formator, "%%.%dFf", scale);
          len = gmp_sprintf(str, formator, sum);
        } else {
          len = gmp_sprintf(str, "%.30Ff", sum);
        }
        str[len] = '\0';
        if (!strcmp(str, "-0.000000000000000000000000000000") ||
            !strcmp(str, "0.000000000000000000000000000000"))
          len = sprintf(str, "%0.2f", 0.00);
      }
    }
    if (packet->size() - (packet->wr_ptr() - packet->base()) < 10 + len) {
      uint64_t resize_len = 10 + len + packet->wr_ptr() - packet->base();
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                packet->size(), resize_len);
      packet->size(resize_len);
    }
    packet->pack_lenenc_int(len);
    packet->packdata(str, len);
  } else {
    char *start_pos, *end_pos;
    row = (*mysql_rows)[0];
    row->set_current(column_index);
    start_pos = row->get_current_data() - row->get_current_header_length();
    end_pos = row->get_current_data() + row->get_current_length();
    packet->packdata(start_pos, end_pos - start_pos);
    row->move_next();
  }
  mpf_clear(sum);
  mpf_clear(tmp_mpf);
}

void handle_count(Packet *packet, Packet *field_packet,
                  vector<MySQLRowResponse *> *mysql_rows, unsigned column_index,
                  MySQLColumnType column_type) {
  ACE_UNUSED_ARG(field_packet);
  ACE_UNUSED_ARG(column_type);
  uint64_t sum = 0;
  MySQLRowResponse *row;
  unsigned int n = mysql_rows->size();
  for (unsigned int i = 0; i < n; i++) {
    row = (*mysql_rows)[i];
    sum += row->get_uint(column_index);
  }

  char str[32];
  size_t len = sprintf(str, "%ld", sum);
  if (packet->size() - (packet->wr_ptr() - packet->base()) < 10 + len) {
    uint64_t resize_len = 10 + len + packet->wr_ptr() - packet->base();
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              packet->size(), resize_len);
    packet->size(resize_len);
  }
  packet->pack_lenenc_int(len);
  packet->packdata(str, len);
}

void handle_normal(Packet *packet, Packet *field_packet,
                   vector<MySQLRowResponse *> *mysql_rows,
                   unsigned column_index, MySQLColumnType column_type) {
  ACE_UNUSED_ARG(field_packet);
  ACE_UNUSED_ARG(column_type);
  MySQLRowResponse *row;

  row = (*mysql_rows)[0];
  copy_column_to_packet(row, column_index, packet);
}

void init_column_handlers(unsigned int column_num,
                          list<AggregateDesc> &aggregate_desc,
                          vector<aggregate_handler> &column_handlers,
                          bool is_certain) {
  unsigned int i;
  list<AggregateDesc>::iterator it;
  for (i = 0; i < column_num; i++) {
    for (it = aggregate_desc.begin(); it != aggregate_desc.end(); it++) {
      if ((unsigned)it->column_index == i) {
        switch (it->type) {
          case AGGREGATE_TYPE_MAX:
            if (is_certain) {
              column_handlers.push_back(handle_certain_max);
              is_certain = false;
            } else
              column_handlers.push_back(handle_max);
            break;
          case AGGREGATE_TYPE_MIN:
            column_handlers.push_back(handle_min);
            break;
          case AGGREGATE_TYPE_COUNT:
            column_handlers.push_back(handle_count);
            break;
          case AGGREGATE_TYPE_SUM:
            column_handlers.push_back(handle_sum);
            break;
          default:
            break;
        }
        break;
      }
    }

    if (it == aggregate_desc.end()) column_handlers.push_back(handle_normal);
  }
}

/* class MySQLAggregateNode */

MySQLAggregateNode::MySQLAggregateNode(ExecutePlan *plan,
                                       list<AggregateDesc> &aggregate_desc)
    : MySQLInnerNode(plan) {
  this->name = "MySQLAggregateNode";
  this->aggregate_desc = aggregate_desc;
  status = EXECUTE_STATUS_START;
  node_can_swap = session->is_may_backend_exec_swap_able();
  result_packet = NULL;
}

void MySQLAggregateNode::init_mysql_rows() {
  max_row_size = 0;
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (row_map[*it]->empty()) {
      continue;
    }
    MySQLRowResponse *row = new MySQLRowResponse(row_map[*it]->front());
    mysql_rows.push_back(row);
    if (max_row_size < row_map[*it]->front()->size()) {
      max_row_size = row_map[*it]->front()->size();
    }
  }
}

void MySQLAggregateNode::handle() {
  list<Packet *> *field_packets = get_field_packets();
  init_column_types(field_packets, column_types, column_num, NULL);
  init_column_handlers(column_num, aggregate_desc, column_handlers);
  init_mysql_rows();

  if (max_row_size >= (unsigned int)row_packet_size) {
    result_packet->size((unsigned int)(max_row_size * 1.2));
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              row_packet_size, int(max_row_size * 1.2));
  } else if ((unsigned int)(row_packet_size * 0.9) < max_row_size) {
    result_packet->size((unsigned int)(row_packet_size * 1.2));
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              row_packet_size, row_packet_size * 1.2);
  }

  list<Packet *>::iterator it_field = field_packets->begin();
  unsigned int i;
  for (i = 0; i < column_num; i++) {
    column_handlers[i](result_packet, *it_field, &mysql_rows, i,
                       column_types[i]);
    ++it_field;
  }

  // set the header of the result set
  size_t load_length = result_packet->length() - PACKET_HEADER_SIZE;
  pack_header(result_packet, load_length);

  ready_rows->push_back(result_packet);
  result_packet = NULL;
}

void MySQLAggregateNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        result_packet = Backend::instance()->get_new_packet(row_packet_size);
        result_packet->wr_ptr(PACKET_HEADER_SIZE);
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
        handle();
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
        LOG_DEBUG("MySQLAggregateNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLAggregateNode::do_clean() {
  MySQLRowResponse *row;
  while (!mysql_rows.empty()) {
    row = mysql_rows.back();
    mysql_rows.pop_back();
    delete row;
  }

  if (result_packet) {
    delete result_packet;
    result_packet = NULL;
  }
}

/* class MySQLWiseGroupNode */

MySQLWiseGroupNode::MySQLWiseGroupNode(ExecutePlan *plan,
                                       list<SortDesc> *group_desc,
                                       list<AggregateDesc> &aggregate_desc)
    : MySQLSortNode(plan, group_desc) {
  this->name = "MySQLWiseGroupNode";
  this->aggregate_desc = aggregate_desc;
  init_column_handler_flag = false;
  max_row_size = 0;
  group_size = 0;
  max_group_buffer_rows = 0;
  max_group_buffer_rows_size = max_wise_group_size * 1024;
}

void MySQLWiseGroupNode::handle_before_complete() {
  group_by();
  if (!group_rows.empty()) {
    Packet *new_row = merge_group();
    ready_rows->push_back(new_row);
  }
}

void MySQLWiseGroupNode::group_by() {
  Packet *packet;
  while (!ready_nodes->empty()) {
    packet = ready_nodes->front();
    if (add_to_group(packet)) {
      ready_nodes->pop_front();
      group_size++;
      if ((group_size & 0x03FF) == 0 || max_group_buffer_rows == 0) {
        max_group_buffer_rows =
            max_group_buffer_rows_size / packet->total_capacity();
        if (!max_group_buffer_rows) max_group_buffer_rows = 1000;
      }
      if (group_size > max_group_buffer_rows) {
        LOG_DEBUG("Wise group size is larger than max_wise_group_size.\n");
        throw Error("Wise group size is larger than max_wise_group_size.");
      }
    } else {
      group_size = 0;
      Packet *new_row = merge_group();
      ready_rows->push_back(new_row);
    }
  }
}

bool MySQLWiseGroupNode::add_to_group(Packet *packet) {
  if (group_rows.empty() ||
      MySQLColumnCompare::compare(group_rows[0]->get_packet(), packet,
                                  &sort_desc, &column_types) == 0) {
    MySQLRowResponse *row = new MySQLRowResponse(packet);
    group_rows.push_back(row);
    if (max_row_size < packet->size()) {
      max_row_size = packet->size();
    }
    return true;
  } else {
    return false;
  }
}

void MySQLWiseGroupNode::handle() {
  init_column_types(get_field_packets(), column_types, column_num,
                    &column_inited_flag);
  check_column_valid();
  if (!init_column_handler_flag) {
    init_column_handlers(column_num, aggregate_desc, column_handlers);
    init_column_handler_flag = true;
  }
  group_by();
}

Packet *MySQLWiseGroupNode::merge_group() {
  MySQLRowResponse *row;
  Packet *result_packet;

  if (group_rows.size() == 1) {
    result_packet = group_rows.back()->get_packet();
    row = group_rows.back();
    group_rows.pop_back();
    delete row;
  } else {
    result_packet = Backend::instance()->get_new_packet(row_packet_size);
    result_packet->wr_ptr(PACKET_HEADER_SIZE);
    if (max_row_size >= (unsigned int)row_packet_size) {
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                row_packet_size, int(max_row_size * 1.2));
      result_packet->size((unsigned int)(max_row_size * 1.2));
    } else if ((unsigned int)(row_packet_size * 0.9) < max_row_size) {
      result_packet->size((unsigned int)(row_packet_size * 1.2));
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                row_packet_size, int(row_packet_size * 1.2));
    }
    for (unsigned int i = 0; i < column_num; i++) {
      column_handlers[i](result_packet, NULL, &group_rows, i, column_types[i]);
    }

    size_t load_length = result_packet->length() - PACKET_HEADER_SIZE;
    pack_header(result_packet, load_length);

    while (!group_rows.empty()) {
      row = group_rows.back();
      group_rows.pop_back();
      delete row->get_packet();
      delete row;
    }
  }

  return result_packet;
}

void MySQLWiseGroupNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        init_row_map();
        ready_nodes = row_map[*(children.begin())];
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
        handle();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;
        if (!ready_rows->empty()) return;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        handle_before_complete();
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLWiseGroupNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLWiseGroupNode::do_clean() {
  MySQLRowResponse *row;
  while (!group_rows.empty()) {
    row = group_rows.back();
    group_rows.pop_back();
    delete row->get_packet();
    delete row;
  }
}

/* class MySQLPagesNode */
MySQLPagesNode::MySQLPagesNode(ExecutePlan *plan, list<SortDesc> *group_desc,
                               list<AggregateDesc> &aggregate_desc,
                               int page_size)
    : MySQLWiseGroupNode(plan, group_desc, aggregate_desc) {
  this->name = "MySQLPagesNode";
  this->page_size = page_size;
  rownum = 0;
  count_index = -1;
  list<AggregateDesc>::iterator it = aggregate_desc.begin();
  for (; it != aggregate_desc.end(); ++it) {
    if (it->type == AGGREGATE_TYPE_COUNT) count_index = it->column_index;
  }
}

void MySQLPagesNode::group_by() {
  Packet *packet;
  while (!ready_nodes->empty()) {
    packet = ready_nodes->front();
    ready_nodes->pop_front();
    ++rownum;

    MySQLRowResponse *row = new MySQLRowResponse(packet);
    group_rows.push_back(row);
    if (max_row_size < packet->size()) {
      max_row_size = packet->size();
    }
    ++group_size;
    if ((group_size & 0x03FF) == 0 || max_group_buffer_rows == 0) {
      max_group_buffer_rows =
          max_group_buffer_rows_size / packet->total_capacity();
      if (!max_group_buffer_rows) max_group_buffer_rows = 1000;
    }
    if (group_size > max_group_buffer_rows) {
      LOG_DEBUG("Wise group size is larger than max_wise_group_size.\n");
      throw Error("Wise group size is larger than max_wise_group_size.");
    }

    if (rownum % page_size == 0) {
      Packet *new_row = merge_group();
      ready_rows->push_back(new_row);
    }
  }
}

Packet *MySQLPagesNode::merge_group() {
  MySQLRowResponse *row;
  Packet *result_packet;

  result_packet = Backend::instance()->get_new_packet(row_packet_size);
  result_packet->wr_ptr(PACKET_HEADER_SIZE);
  if (max_row_size >= (unsigned int)row_packet_size) {
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              row_packet_size, int(max_row_size * 1.2));
    result_packet->size((unsigned int)(max_row_size * 1.2));
  } else if ((unsigned int)(row_packet_size * 0.9) < max_row_size) {
    result_packet->size((unsigned int)(row_packet_size * 1.2));
    LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
              row_packet_size, int(row_packet_size * 1.2));
  }
  for (unsigned int i = 0; i < column_num; i++) {
    if (i == (unsigned int)count_index) {
      char str[32];
      size_t len = sprintf(str, "%ld", rownum);
      if (result_packet->size() -
              (result_packet->wr_ptr() - result_packet->base()) <
          10 + len) {
        uint64_t resize_len =
            10 + len + result_packet->wr_ptr() - result_packet->base();
        LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                  result_packet->size(), resize_len);
        result_packet->size(resize_len);
      }
      result_packet->pack_lenenc_int(len);
      result_packet->packdata(str, len);
    } else {
      column_handlers[i](result_packet, NULL, &group_rows, i, column_types[i]);
    }
  }

  size_t load_length = result_packet->length() - PACKET_HEADER_SIZE;
  pack_header(result_packet, load_length);

  while (!group_rows.empty()) {
    row = group_rows.back();
    group_rows.pop_back();
    delete row->get_packet();
    delete row;
  }
  group_size = 0;

  return result_packet;
}

/* class MySQLGroupNode */

MySQLGroupNode::MySQLGroupNode(ExecutePlan *plan, list<SortDesc> *sort_desc,
                               list<AggregateDesc> &aggregate_desc)
    : MySQLSortNode(plan, sort_desc) {
  this->name = "MySQLGroupNode";
  this->aggregate_desc = aggregate_desc;
  init_column_handler_flag = false;
  this->sort_ready_nodes = &this->ready_nodes;
  max_row_size = 0;
  node_can_swap = session->is_may_backend_exec_swap_able();
}

bool MySQLGroupNode::add_to_group(Packet *packet) {
  if (group_rows.empty() ||
      MySQLColumnCompare::compare(group_rows[0]->get_packet(), packet,
                                  &sort_desc, &column_types) == 0) {
    MySQLRowResponse *row = new MySQLRowResponse(packet);
    group_rows.push_back(row);
    if (max_row_size < packet->size()) {
      max_row_size = packet->size();
    }
    return true;
  } else {
    return false;
  }
}

Packet *MySQLGroupNode::merge_group() {
  MySQLRowResponse *row;
  Packet *result_packet;
#ifdef DEBUG
  LOG_DEBUG("merge group with %d rows.\n", group_rows.size());
#endif
  if (group_rows.size() == 1) {
    result_packet = group_rows.back()->get_packet();
    row = group_rows.back();
    group_rows.pop_back();
    delete row;
  } else {
    result_packet = Backend::instance()->get_new_packet(row_packet_size);
    result_packet->wr_ptr(PACKET_HEADER_SIZE);
    if (max_row_size >= (unsigned int)row_packet_size) {
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                row_packet_size, int(max_row_size * 1.2));
      result_packet->size((unsigned int)(max_row_size * 1.2));
    } else if ((unsigned int)(row_packet_size * 0.9) < max_row_size) {
      result_packet->size((unsigned int)(row_packet_size * 1.2));
      LOG_DEBUG("Packet size not enough, resize packet size from %d to %d\n",
                row_packet_size, int(row_packet_size * 1.2));
    }
    for (unsigned int i = 0; i < column_num; i++) {
      column_handlers[i](result_packet, NULL, &group_rows, i, column_types[i]);
    }

    // set the header of the result set
    size_t load_length = result_packet->length() - PACKET_HEADER_SIZE;
    pack_header(result_packet, load_length);

    while (!group_rows.empty()) {
      row = group_rows.back();
      group_rows.pop_back();
      delete row->get_packet();
      delete row;
    }
  }

  return result_packet;
}

void MySQLGroupNode::group_by() {
  Packet *packet;
  while (!ready_nodes.empty()) {
    packet = ready_nodes.front();

    if (add_to_group(packet)) {
      ready_nodes.pop_front();
    } else {
      Packet *new_row = merge_group();
      ready_rows->push_back(new_row);
    }
  }
}

void MySQLGroupNode::handle_before_complete() {
  last_sort();
  group_by();
  // deal with remaining ready_rows
  if (!group_rows.empty()) {
    Packet *new_row = merge_group();
    ready_rows->push_back(new_row);
  }
}

void MySQLGroupNode::handle() {
  init_column_types(get_field_packets(), column_types, column_num,
                    &column_inited_flag);
  adjust_column_index();
  check_column_valid();
  if (!init_column_handler_flag) {
    if (plan->statement->get_stmt_node()->execute_max_count_certain)
      init_column_handlers(column_num, aggregate_desc, column_handlers, true);
    else
      init_column_handlers(column_num, aggregate_desc, column_handlers);
    init_column_handler_flag = true;
  }
  sort();
  group_by();
}

void MySQLGroupNode::execute() {
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

      case EXECUTE_STATUS_HANDLE:
#ifdef DEBUG
        node_start_timing();
#endif
        handle();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;
        if (!ready_rows->empty()) return;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        handle_before_complete();
        status = EXECUTE_STATUS_COMPLETE;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLGroupNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLGroupNode::do_clean() {
  MySQLRowResponse *row;
  while (!group_rows.empty()) {
    row = group_rows.back();
    group_rows.pop_back();
    delete row->get_packet();
    delete row;
  }

  Packet *packet;
  while (!ready_nodes.empty()) {
    packet = ready_nodes.back();
    ready_nodes.pop_back();
    if (packet) delete packet;
  }
}

/* class MySQLSingleSortNode */

MySQLSingleSortNode::MySQLSingleSortNode(ExecutePlan *plan,
                                         list<SortDesc> *sort_desc)
    : MySQLSortNode(plan, sort_desc) {
  this->name = "MySQLSingleSortNode";
  this->sort_ready_nodes = &this->ready_nodes;
  node_can_swap = session->is_may_backend_exec_swap_able();
  max_single_sort_buffer_rows = 0;
  max_single_sort_buffer_rows_size =
      session->get_session_option("max_single_sort_rows").ulong_val * 1024;
}

void MySQLSingleSortNode::init_heap() {
  list<MySQLExecuteNode *>::iterator it;

  /* dummy node */
  rows_heap.push_back(NULL);
  heap_size = 0;
}

void MySQLSingleSortNode::max_heapify(unsigned int i) {
  unsigned int left = i << 1;
  unsigned int right = left + 1;
  unsigned int largest;

  if (left <= heap_size &&
      MySQLColumnCompare::compare(rows_heap[left], rows_heap[i], &sort_desc,
                                  &column_types) == 1) {
    largest = left;
  } else {
    largest = i;
  }

  if (right <= heap_size &&
      MySQLColumnCompare::compare(rows_heap[right], rows_heap[largest],
                                  &sort_desc, &column_types) == 1) {
    largest = right;
  }

  if (i != largest) {
    Packet *packet;
    packet = rows_heap[i];
    rows_heap[i] = rows_heap[largest];
    rows_heap[largest] = packet;
    max_heapify(largest);
  }
}

void MySQLSingleSortNode::heap_insert(MySQLExecuteNode *node) {
  unsigned int i;
  Packet *packet;
  while (!row_map[node]->empty()) {
    i = ++heap_size;
    if ((heap_size & 0x03FF) == 0 || max_single_sort_buffer_rows == 0) {
      max_single_sort_buffer_rows = max_single_sort_buffer_rows_size /
                                    row_map[node]->front()->total_capacity();
      if (!max_single_sort_buffer_rows) max_single_sort_buffer_rows = 1000;
    }
    if (heap_size > max_single_sort_buffer_rows) {
      vector<Packet *>::iterator it = rows_heap.begin();
      for (; it != rows_heap.end(); it++) {
        delete (*it);
      }
      rows_heap.clear();

      LOG_ERROR(
          "Reach the max allowed single sort rows with row num %d, refuse this "
          "sql to protect dbscale.\n",
          heap_size);
      LOG_ERROR(
          "To solve this problem, you can change the sql from 'select * from "
          "tbl group by c1 order by c2' to 'select * from (select * from tbl "
          "group by c1) T order by c2'. In other words, move the group by part "
          "into a table subquery.\n");
      throw Error(
          "Reach the max allowed single sort rows. Please check manual and the "
          "error message in the log.");
    }
    rows_heap.push_back(row_map[node]->front());
    while (i > 1 &&
           MySQLColumnCompare::compare(rows_heap[i >> 1], rows_heap[i],
                                       &sort_desc, &column_types) == -1) {
      packet = rows_heap[i];
      rows_heap[i] = rows_heap[i >> 1];
      rows_heap[i >> 1] = packet;
      i = i >> 1;
    }

    row_map[node]->pop_front();
  }
}

void MySQLSingleSortNode::sort() {
  list<MySQLExecuteNode *>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if (row_map[*it]->size()) {
      heap_insert(*it);
    }
  }
}

void MySQLSingleSortNode::handle_before_complete() {
  while (heap_size) {
    ready_rows->push_back(rows_heap[1]);
    rows_heap[1] = rows_heap[heap_size--];
    rows_heap.pop_back();
    max_heapify(1);
  }
}

void MySQLSingleSortNode::execute() {
  while (status != EXECUTE_STATUS_COMPLETE) {
    switch (status) {
      case EXECUTE_STATUS_START:
        init_row_map();
        init_heap();
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
        init_column_types(get_field_packets(), column_types, column_num,
                          &column_inited_flag);
        adjust_column_index();
        check_column_valid();
        sort();
#ifdef DEBUG
        node_end_timing();
#endif
        status = EXECUTE_STATUS_FETCH_DATA;
        break;

      case EXECUTE_STATUS_BEFORE_COMPLETE:
        handle_before_complete();
        status = EXECUTE_STATUS_COMPLETE;
        break;

      case EXECUTE_STATUS_COMPLETE:
#ifdef DEBUG
        LOG_DEBUG("MySQLAggregateNode %@ cost %d ms\n", this, node_cost_time);
#endif
        break;

      default:
        ACE_ASSERT(0);
        break;
    }
  }
}

void MySQLSingleSortNode::do_clean() {
  Packet *packet;
  while (!ready_nodes.empty()) {
    packet = ready_nodes.back();
    ready_nodes.pop_back();
    if (packet) delete packet;
  }
}

/* class MySQLLoadLocalExternal */
MySQLLoadLocalExternal::MySQLLoadLocalExternal(ExecutePlan *plan,
                                               DataSpace *dataspace,
                                               DataServer *dataserver,
                                               const char *sql)
    : MySQLExecuteNode(plan, dataspace), dataserver(dataserver), sql(sql) {}

void MySQLLoadLocalExternal::execute() {
#ifdef DEBUG
  node_start_timing();
#endif

  int fifo_id = Backend::instance()->create_fifo();
  sprintf(fifo_name, "/tmp/tmp_fifo_%d", fifo_id);

  build_command();
  spawn_command();
  send_file_request_to_client();

  // feed data into FIFO
  Packet packet;
  size_t load_length = 0;
  bool before_file_close = false;
  try {
    do {
      packet.rewind();
      handler->receive_from_client(&packet);
      if (!driver->is_empty_packet(&packet)) {
        if (param.has_got_error() || param.get_external_load_flag() == FINISH) {
          throw ExecuteNodeError(
              "Got error when execute statement, check log for more "
              "information.");
        }
        load_length = packet.unpack3uint();
        LOG_DEBUG("MySQLLoadLocalExternal %@ get packet %@ from client.\n",
                  this, &packet);
        file.write(packet.base() + PACKET_HEADER_SIZE, load_length);
      }
    } while (!driver->is_empty_packet(&packet));
    before_file_close = true;
    file.close();
    ACE_Thread::join(t_handle);
  } catch (...) {
    status = EXECUTE_STATUS_COMPLETE;
    if (!before_file_close) {
      file.close();
      ACE_Thread::join(t_handle);
    }
    throw;
  }

  status = EXECUTE_STATUS_COMPLETE;
#ifdef DEBUG
  node_end_timing();
#endif

  if (param.has_got_error()) {
    LOG_ERROR("Got error when execute statement, %Q rows inserted.\n",
              param.insert_rows);
    throw ExecuteNodeError(
        "Got error when execute statement, check log for more information.");
  } else {
    send_ok_packet_to_client(handler, param.insert_rows, 0);
#ifdef DEBUG
    LOG_DEBUG("MySQLLoadLocalExternal %@ cost %d ms\n", this, node_cost_time);
#endif
  }
}

void MySQLLoadLocalExternal::send_file_request_to_client() {
  stmt_node *st = plan->statement->get_stmt_node();
  const char *filename = st->sql->load_oper->filename;
  Packet *res = Backend::instance()->get_new_packet(row_packet_size);
  res->wr_ptr(res->base() + PACKET_HEADER_SIZE);
  char load_flag;
  load_flag = 0xfb;
  res->packdata((const char *)&load_flag, 1);
  res->packdata(filename, strlen(filename));
  pack_header(res, strlen(filename) + 1);
  handler->send_to_client(res);
  delete res;
}

void MySQLLoadLocalExternal::build_command() {
  stmt_node *st = plan->statement->get_stmt_node();
  table_link *table = st->table_list_head;
  const char *field_terminate = st->sql->load_oper->field_terminate;
  const char *field_enclose = st->sql->load_oper->field_enclose;
  if (strlen(field_terminate) > 1 || strlen(field_enclose) > 1) {
    LOG_ERROR(
        "DBScale does not support FIELDS TERMINATED/ENCLOSED BY with multiple "
        "characters "
        "for external LOAD DATA.\n");
    throw NotImplementedError(
        "external LOAD DATA FIELDS TERMINATED/ENCLOSED BY has multiple "
        "characters");
  }
  const char *schema_name = table->join->schema_name ? table->join->schema_name
                                                     : session->get_schema();
  const char *table_name = table->join->table_name;
  if (!schema_name || schema_name[0] == '\0') {
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError("No database selected.");
  }
  const char *remote_host = dataserver->get_host_direct();
  const char *remote_user = dataserver->get_remote_user();
  const char *remote_password = dataserver->get_remote_password();
  int remote_port = dataserver->get_remote_port();
  const char *external_load_script = dataserver->get_external_load_script();

  ExternalLoadRemoteExecute ep_ssh(external_load_script, schema_name,
                                   table_name, field_terminate, field_enclose,
                                   fifo_name, remote_host, remote_port,
                                   remote_user, remote_password);
  ep_ssh.build_full_command();
  load_local_external_cmd = ep_ssh.get_command();
  LOG_DEBUG("full command [%s].\n", load_local_external_cmd.c_str());
}

void MySQLLoadLocalExternal::spawn_command() {
  fd = popen(load_local_external_cmd.c_str(), "r");
  if (fd == NULL) {
    fprintf(stderr, "execute command failed");
    LOG_ERROR("Fail to execute command %s.\n", load_local_external_cmd.c_str());
    throw HandlerError("Fail to execute command.");
  }

  param.set_got_error(false);
  param.insert_rows = 0;
  param.fd = fd;
  param.set_external_load_flag(UNDEFINE);
  int ret =
      ACE_Thread::spawn((ACE_THR_FUNC)monitor_worker_via_fifo, &param,
                        THR_JOINABLE | THR_SCHED_DEFAULT, &t_id, &t_handle);
  if (ret == -1) {
    LOG_ERROR("Error when execute command %s.\n",
              load_local_external_cmd.c_str());
    throw HandlerError("Error when execute command.");
  }
  file.open(fifo_name);
  while (param.get_external_load_flag() == UNDEFINE) {
    LOG_DEBUG("Waiting for external load script report load file status.\n");
    ACE_OS::sleep(1);
  }
  if (param.get_external_load_flag() == FILE_NOT_OPEN) {
    file.close();
    ACE_Thread::join(t_handle);
    throw ExecuteNodeError(
        "Failed to execute statement, fail to open file, check log for more "
        "information.");
  }
}

void MySQLLoadLocalExternal::clean() {
  for (int i = 0; i < 3; i++) {  // delete fifo, try 3 times.
    if (!remove(fifo_name)) {    // return 0, delete fifo successful.
      break;
    }
    if (i == 2) {  // try 3 times but failed.
      LOG_ERROR("Error deleting fifo file.\n");
    }
  }
}

/* class MySQLLoadDataInfileExternal */

MySQLLoadDataInfileExternal::MySQLLoadDataInfileExternal(ExecutePlan *plan,
                                                         DataSpace *dataspace,
                                                         DataServer *dataserver)
    : MySQLExecuteNode(plan, dataspace), dataserver(dataserver) {}

void MySQLLoadDataInfileExternal::execute() {
#ifdef DEBUG
  node_start_timing();
#endif
  const char *remote_host = dataserver->get_host_direct();
  const char *remote_user = dataserver->get_remote_user();
  const char *remote_password = dataserver->get_remote_password();
  int remote_port = dataserver->get_remote_port();
  const char *external_load_script = dataserver->get_external_load_script();

  stmt_node *st = plan->statement->get_stmt_node();

  const char *file_name = st->sql->load_oper->filename;
  table_link *table = st->table_list_head;
  const char *schema_name = table->join->schema_name ? table->join->schema_name
                                                     : session->get_schema();
  const char *table_name = table->join->table_name;

  const char *field_terminate = st->sql->load_oper->field_terminate;
  const char *field_enclose = st->sql->load_oper->field_enclose;
  if (strlen(field_terminate) > 1 || strlen(field_enclose) > 1) {
    LOG_ERROR(
        "DBScale does not support FIELDS TERMINATED/ENCLOSED BY with multiple "
        "characters "
        "for external LOAD DATA.\n");
    throw NotImplementedError(
        "external LOAD DATA FIELDS TERMINATED/ENCLOSED BY has multiple "
        "characters");
  }

  if (!schema_name || schema_name[0] == '\0') {
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError("No database selected.");
  }

  ExternalLoadResultHandler exp_handler;
  try {
    ExternalLoadRemoteExecute ep_ssh(external_load_script, schema_name,
                                     table_name, field_terminate, field_enclose,
                                     file_name, remote_host, remote_port,
                                     remote_user, remote_password);
    ep_ssh.set_result_handler(&exp_handler);
    ep_ssh.exec_remote_command();
  } catch (dbscale::sql::SQLError &e) {
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError("Table is not exist for external load.");
  } catch (ExecuteFail &e) {
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError(
        "Fail to execute LOAD DATA due to execution failure.");
  } catch (Exception &e) {
    status = EXECUTE_STATUS_COMPLETE;
    throw ExecuteNodeError(e.what());
  }

  const char *inserted_rows = exp_handler.get_inserted_rows();
  uint64_t affect_rows = atoll(inserted_rows);
  LOG_DEBUG("Get affect rows of GOS load %s %d.\n", inserted_rows, affect_rows);
  Packet ok_packet;
  MySQLOKResponse ok(affect_rows, 0);
  ok.pack(&ok_packet);
  handler->deal_autocommit_with_ok_eof_packet(&ok_packet);
  handler->record_affected_rows(&ok_packet);
  handler->send_to_client(&ok_packet);

  status = EXECUTE_STATUS_COMPLETE;
#ifdef DEBUG
  node_end_timing();
#endif
#ifdef DEBUG
  LOG_DEBUG("MySQLLoadDataInfileExternalNode %@ cost %d ms\n", this,
            node_cost_time);
#endif
}

/* class MySQLLoadLocalNode */

MySQLLoadLocalNode::MySQLLoadLocalNode(ExecutePlan *plan, DataSpace *dataspace,
                                       const char *sql)
    : MySQLExecuteNode(plan, dataspace), sql(sql) {
  this->name = "MySQLLoadLocalNode";
  load_data_node = plan->statement->get_stmt_node()->sql->load_oper;
  field_terminate = load_data_node->field_terminate;
  field_escape = load_data_node->field_escape[0];
  has_field_enclose = load_data_node->has_field_enclose;
  if (has_field_enclose) field_enclose = load_data_node->field_enclose[0];
  line_terminate = load_data_node->line_terminate;
  has_line_starting = load_data_node->has_line_starting;
  if (has_line_starting) {
    line_starting = load_data_node->line_starting;
  }
  warning_count = 0;
  warning_packet_list_size = 0;
  affected_row_count = 0;
  has_add_warning_packet = false;
  warning_packet_list = NULL;
}

void MySQLLoadLocalNode::execute() {
#ifdef DEBUG
  node_start_timing();
#endif
  Packet packet;
  MySQLQueryRequest query(sql);
  query.set_sql_replace_char(plan->session->get_query_sql_replace_null_char());
  Packet exec_query;
  query.pack(&exec_query);
  Connection *conn = NULL;
  try {
    conn = handler->send_to_server_retry(
        dataspace, &exec_query, session->get_schema(), session->is_read_only());
    conn->set_load(true);
    // since load data may execuate a long times.
    TimeValue timeout = TimeValue(UINT_MAX, 0);
    handler->receive_from_server(conn, &packet, &timeout);
    if (driver->is_error_packet(&packet)) {
      status = EXECUTE_STATUS_COMPLETE;
      handler->send_to_client(&packet);
      handler->put_back_connection(dataspace, conn);
      conn = NULL;
      session->set_cur_stmt_execute_fail(true);
      return;
    }
    MySQLResultSetHeaderResponse res(&packet);
    res.unpack();
    if (!res.is_columns_null_length()) {
      const char *errmsg = "Unexpected packet received for LOAD DATA response";
      LOG_ERROR("%s\n", errmsg);
      if (session->defined_lock_need_kept_conn(dataspace))
        session->remove_defined_lock_kept_conn(dataspace);
      handler->clean_dead_conn(&conn, dataspace);
      handler->send_to_client(&packet);
      status = EXECUTE_STATUS_COMPLETE;
      throw ExecuteNodeError(errmsg);
    }
    handler->send_to_client(&packet);
    unsigned long packet_num = 0;
    unsigned long load_once_packet_num =
        session->get_session_option("max_load_once_packet_num").ulong_val;
    do {
      packet.rewind();
      handler->receive_from_client(&packet);
      packet_num++;
      if (!driver->is_empty_packet(&packet)) {
        if (load_once_packet_num && packet_num >= load_once_packet_num) {
          size_t incomplete_row_len = check_first_row_complete(
              &packet, field_terminate, line_terminate, has_field_enclose,
              field_enclose, field_escape, 0);
          while (incomplete_row_len == packet.length()) {
            handler->send_to_server(conn, &packet);
            handler->receive_from_client(&packet);
            if (driver->is_empty_packet(&packet)) break;
            incomplete_row_len = check_first_row_complete(
                &packet, field_terminate, line_terminate, has_field_enclose,
                field_enclose, field_escape, 0);
          }
          if (!driver->is_empty_packet(&packet) && incomplete_row_len) {
            Packet *pf = NULL, *pr = NULL;
            divide_packet_by_first_row(&packet, incomplete_row_len, &pf, &pr);
            if (pf && pr) {
              handler->send_to_server(conn, pf);
              redo_load(conn, sql, handler, driver, affected_row_count,
                        warning_count, warning_packet_list_size,
                        &warning_packet_list, has_add_warning_packet);
              handler->send_to_server(conn, pr);
              packet_num = 0;
              delete pf;
              delete pr;
              pf = NULL;
              pf = NULL;
              continue;
            }
          }
        }
      }
      handler->send_to_server(conn, &packet);
    } while (!driver->is_empty_packet(&packet));
    packet.rewind();
    handler->receive_from_server(conn, &packet, &timeout);
    if (driver->is_ok_packet(&packet)) {
      MySQLOKResponse ok(&packet);
      ok.unpack();
      uint64_t warnings = ok.get_warnings();
      uint64_t affected_rows = ok.get_affected_rows();
      affected_row_count += affected_rows;
      warning_count += warnings;
      if (support_show_warning &&
          (warning_packet_list_size < MAX_LOAD_WARNING_PACKET_LIST_SIZE) &&
          warnings)
        store_warning_packet(conn, handler, driver, &warning_packet_list,
                             has_add_warning_packet, warning_packet_list_size);
      ok.set_affected_rows(affected_row_count);
      ok.set_warnings(warning_count);
      ok.pack(&packet);
    }
    handler->deal_autocommit_with_ok_eof_packet(&packet);
    handler->record_affected_rows(&packet);
    handler->send_to_client(&packet);
    if (warning_packet_list) {
      session->add_load_warning_packet_list(warning_packet_list, warning_count);
      warning_packet_list = NULL;
    }
    status = EXECUTE_STATUS_COMPLETE;
  } catch (...) {
    LOG_ERROR("got exception while handling LOAD DATA\n");
    if (session->defined_lock_need_kept_conn(dataspace))
      session->remove_defined_lock_kept_conn(dataspace);
    if (conn) {
      handler->clean_dead_conn(&conn, dataspace);
    }
    status = EXECUTE_STATUS_COMPLETE;
    throw;
  }
  handler->put_back_connection(dataspace, conn);
  session->record_xa_modified_conn(conn);
  conn = NULL;
#ifdef DEBUG
  node_end_timing();
#endif
#ifdef DEBUG
  LOG_DEBUG("MySQLLoadLocalNode %@ cost %d ms\n", this, node_cost_time);
#endif
}
/* class MySQLLoadDataInfile */

MySQLLoadDataInfile::MySQLLoadDataInfile()
    : max_size(1024L * 16L - 1), new_sql(NULL) {
  data_buffer = new char[max_size];
}
MySQLLoadDataInfile::~MySQLLoadDataInfile() { delete[] data_buffer; }
void MySQLLoadDataInfile::sql_add_local(ExecutePlan *plan, const char *sql) {
  new_sql = new string(sql);
  unsigned int pos =
      plan->statement->get_stmt_node()->sql->load_oper->infile_pos;
  new_sql->insert(pos - 1, "LOCAL ");
  LOG_DEBUG("The SQL [%s] has changed into [%s] in LoadDataInfile.\n", sql,
            new_sql->c_str());
}

void MySQLLoadDataInfile::build_error_packet(Packet &packet, const char *file) {
  string error_message = "Can't get stat of '";
  char buffer[PATH_MAX + 1];
  if (realpath(file, buffer)) {
    LOG_DEBUG("can't get the current working directory.\n");
    error_message = error_message + file;
  } else {
    error_message = error_message + buffer;
  }
  error_message = error_message + "' (Errcode: 2)";
  MySQLErrorResponse error_packet(13, error_message.c_str(), "HY000");
  error_packet.pack(&packet);
}
bool MySQLLoadDataInfile::open_local_file(const char *file_path) {
  filestr.open(file_path, ios::binary);
  if (!filestr)
    return false;
  else
    return true;
}
void MySQLLoadDataInfile::init_file_buf() {
  pbuf = filestr.rdbuf();
  size = pbuf->pubseekoff(0, ios::end, ios::in);
  size_remain = size;
  size_temp = 0;
  pbuf->pubseekpos(0, ios::in);
}
void MySQLLoadDataInfile::read_file_into_buf(Packet &buffer) {
  if (size_remain > max_size) {
    size_remain -= max_size;
    size_temp = max_size;
  } else if (size_remain <= max_size && size_remain > 0) {
    size_temp = size_remain;
    size_remain = 0;
  } else {
    size_temp = 0;
  }
  buffer.size(size_temp + PACKET_HEADER_SIZE + 1);
  try {
    filestr.read(buffer.base() + PACKET_HEADER_SIZE, size_temp);
  } catch (...) {
    throw;
  }
}
void MySQLLoadDataInfile::init_file_buf_for_load_insert() {
  pbuf = filestr.rdbuf();
  pbuf->pubseekpos(0, ios::in);
  size_temp = 0;
  size_remain = 1;
}
void MySQLLoadDataInfile::read_file_into_buf_for_load_insert(Packet &buffer) {
  size_temp = 0;
  char *cbuffer = data_buffer;
  int pos = pbuf->sgetc();
  while (pos != EOF) {
    char ch = pbuf->sgetc();
    cbuffer[size_temp] = ch;
    size_temp++;
    if (size_temp == max_size) {
      pbuf->sbumpc();
      break;
    }
    pos = pbuf->snextc();
  }

  buffer.size(size_temp + PACKET_HEADER_SIZE + 1);

  try {
    memcpy(buffer.base() + PACKET_HEADER_SIZE, cbuffer, size_temp);
  } catch (...) {
    throw;
  }
}

}  // namespace mysql
}  // namespace dbscale
