/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <cross_node_join_manager.h>
#include <exception.h>
#include <executor.h>
#include <expression.h>
#include <log.h>
#include <log_tool.h>
#include <multiple.h>
#include <mysql_comm.h>
#include <mysql_config.h>
#include <mysql_driver.h>
#include <mysql_handler.h>
#include <mysql_parser.h>
#include <mysql_plan.h>
#include <mysql_statement.h>
#include <option.h>
#include <socket_base_manager.h>
#include <sql_exception.h>
#include <ssl_tool.h>
#include <sys/time.h>
#include <xa_transaction.h>

#include <boost/shared_ptr.hpp>
#include <limits>
#include <sstream>

using namespace dbscale;
using namespace dbscale::sql;
using namespace dbscale::plan;
using namespace dbscale::mysql;

#define INTER_BASE_TIMEOUT 5
#define MAX_PREPARE_GROUP_STMT_LEN 1024000  // 1M

extern bool is_implict_commit_sql(stmt_type type);

string MySQLHandler::generate_ddl_comment_sql(const char *sql) {
  if (!sql) {
    return "";
  }
  string res_sql(sql);
  res_sql.append(" /* ");
  res_sql.append(to_string(Backend::instance()->get_cluster_id()));
  res_sql.append("-");
  char time_string[40];
  ACE_Date_Time time = ACE_Date_Time(ACE_OS::gettimeofday());
  snprintf(time_string, sizeof(time_string), "%d-%d-%d %d:%d:%d %d",
           (int)time.year(), (int)time.month(), (int)time.day(),
           (int)time.hour(), (int)time.minute(), (int)time.second(),
           (int)time.microsec());
  res_sql.append(time_string);
  res_sql.append("-");
  res_sql.append(to_string(Backend::instance()->get_ddl_atomic_num()));
  res_sql.append(" */");
  return res_sql;
}

void init_handshake_pack_info(HandShakePacket *handshake) {
  Backend *backend = Backend::instance();
  int prot_v = backend->get_backend_server_protocol_version();
  handshake->protocol_version = (unsigned char)prot_v;
  handshake->server_version = backend->get_backend_server_version_real();
  handshake->thread_id = 0;
  memcpy(handshake->server_scramble, "12345678901234567890", 20);
  handshake->server_scramble[20] = '\0';
  // initial_handshake_info.server_capabilities = 2181036031;
  handshake->server_capabilities = CLIENT_BASIC_FLAGS;
  if (backend->get_backend_server_compress()) {
    handshake->server_capabilities |= CLIENT_COMPRESS;
  }
  handshake->server_charset = 8;  // latin1,这个参数就是mysql固定的
  handshake->server_status = 2;   // autocommit 一直为 1
  handshake->auth_plugin_name = "mysql_native_password";
}

void handle_warnings_OK_and_eof_packet(MySQLDriver *driver, Packet *packet,
                                       Handler *handler, DataSpace *space,
                                       Connection *conn,
                                       bool skip_keep_conn = false) {
  handle_warnings_OK_and_eof_packet_inernal(driver, packet, handler, space,
                                            conn, skip_keep_conn);
}

MySQLHandler::MySQLHandler() {
  driver = (MySQLDriver *)Driver::get_driver();
  if (net_buffer_size) {
    buffer_size = net_buffer_size;
    if (net_buffer_size > MAX_PACKET_LENGTH) {
      LOG_WARN(
          "Too large net-buffer-size for MySQL Driver, adjust it to (16M - "
          "1).\n");
      buffer_size = MAX_PACKET_LENGTH;
    }
    net_buffer = new Packet(buffer_size + PACKET_HEADER_SIZE);
    if (!net_buffer || !(net_buffer->data_block())) {
      if (net_buffer) delete net_buffer;
      net_buffer = new Packet(DEFAULT_NET_BUFFER_SIZE + PACKET_HEADER_SIZE);
      if (!net_buffer || !(net_buffer->data_block())) {
        LOG_ERROR("Fail to allocate net buffer, net buffer is disabled.\n");
        buffer_size = 0;
        if (net_buffer) {
          delete net_buffer;
          net_buffer = NULL;
        }
      } else {
        LOG_WARN(
            "Fail to allocate net buffer with size %d, use the default"
            " value %d instead.\n",
            buffer_size, DEFAULT_NET_BUFFER_SIZE);
        buffer_size = DEFAULT_NET_BUFFER_SIZE;
      }
    }
    if (net_buffer) net_buffer->rd_ptr(net_buffer->base() + PACKET_HEADER_SIZE);
  } else
    net_buffer = NULL;

  s = NULL;
  session = NULL;
}

void MySQLHandler::do_re_init_handler() {
  if (net_buffer) net_buffer->rd_ptr(net_buffer->base() + PACKET_HEADER_SIZE);
}

void reset_auth_packet_for_compress(uint32_t client_flags, Packet *packet) {
  uint32_t new_client_flags;
  new_client_flags = client_flags & (~CLIENT_COMPRESS);
  char *pos = packet->base();
  pos += PACKET_HEADER_SIZE;
  *(uint32_t *)pos = new_client_flags;
}

bool MySQLHandler::white_filter_check(string &client_addr,
                                      const char *user_name,
                                      ssl_option_struct &ssl_option_value) {
#ifdef DEBUG
  LOG_DEBUG("white_filter_check client_addr:%s user_name:%s\n",
            client_addr.c_str(), user_name);
#endif
  Backend *backend = Backend::instance();
  return backend->filter_by_white_user_info(client_addr, user_name,
                                            ssl_option_value);
}

string MySQLHandler::get_login_hostip() {
  InetAddr addr;
  stream->get_remote_addr(addr);
  char host_ip[INET6_ADDRSTRLEN + 1];
  addr.get_host_addr(host_ip, INET6_ADDRSTRLEN);
  string host;
  host.assign(host_ip);
  LOG_DEBUG("Client try to login with host ip [%s].\n", host.c_str());
  return host;
}

void MySQLHandler::check_and_init_session_charset(Session *session) {
  string init_char = session_init_charset;
  set_names_charset_in_session(session, init_char);
}

void MySQLHandler::set_names_charset_in_session(Session *session,
                                                const string &charset) {
  if (!charset.empty() && Backend::instance()->is_valid_charset(charset)) {
    map<string, string> *session_map = session->get_session_var_map();
    (*session_map)["CHARACTER_SET_CLIENT"] = charset;
    (*session_map)["CHARACTER_SET_RESULTS"] = charset;
    (*session_map)["CHARACTER_SET_CONNECTION"] = charset;
    session->set_session_var_map_md5(
        calculate_md5(session->get_session_var_map()));
  }
}

bool MySQLHandler::login_check(const char *user, const char *host_str,
                               const char *schema, bool client_client_ssl) {
  bool is_super_user_or_internal_user = false;
  string host = host_str;

  if (user[0]) {
    LOG_DEBUG("Login with user name: %s.\n", user);
    is_super_user_or_internal_user = !strcmp(user, supreme_admin_user) ||
                                     !strcmp(user, dbscale_internal_user);
    if (is_super_user_or_internal_user) get_session()->set_is_super_user(true);
  }
  ssl_option_struct ssl_option_value;
  ssl_option_value.ssl_option = SSL_OPTION_NULL;
  if (enable_acl && !white_filter_check(host, user, ssl_option_value)) {
    string err_message(
        "Access denied due to white list check fail for client:");
    err_message.append(host.c_str());
    throw HandlerError(err_message.c_str(), 1045);
  }

#ifdef HAVE_OPENSSL
  if (ssl_option_value.ssl_option != SSL_OPTION_NONE &&
      ssl_option_value.ssl_option != SSL_OPTION_NULL) {
    if (!client_client_ssl ||
        (ssl_option_value.ssl_option != SSL_OPTION_SSL &&
         !SSLTool::instance()->check_ssl_cert(this, ssl_option_value))) {
      string err_message(
          "Access denied due to SSL cert check failed for client:");
      err_message.append(host.c_str());
      throw HandlerError(err_message.c_str(), 1045);
    }
  }
#else
  ACE_UNUSED_ARG(client_client_ssl);
#endif

  // max-conn-filter filter
  unsigned int conn_num = driver->get_session_count();
#ifdef DEBUG
  LOG_DEBUG("Max-conn-filter: current conn_num = [%d], max_conn_limit = [%d]\n",
            conn_num, max_conn_limit);
#endif
  if ((conn_num >= max_conn_limit) &&
      !(is_super_user_or_internal_user && (conn_num < (max_conn_limit + 1)))) {
#ifdef DEBUG
    LOG_DEBUG("Login max-conn-filter: block username:%s\n", user);
#endif
    throw HandlerError("Max connection limitation", 1045);
  } else if (user[0] && !strcmp(dbscale_internal_user, user) &&
             !multiple_mode &&
             (!schema[0] || (schema[0] && strcmp(schema, TMP_TABLE_SCHEMA) &&
                             strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA)))) {
    // some dbscale internal operation will need to use dbscale_internal_user
    // to do the login, such as multiple dbsale and federate cross node join.
#ifdef DEBUG
    LOG_DEBUG("Forbid login with dbscale internal user.\n");
#endif
    throw HandlerError("Access denied for dbscale internal user", 1045);
  }
  return true;
}

void MySQLHandler::recover_replication_info_in_slave_dbscale_mode() {
  ConfigHelper *helper = Driver::get_driver()->get_config_helper();
  bool ret = helper->recover_white_user_info(false, true);
  ret = ret && helper->recover_schema_acl_info();
  LOG_DEBUG("recover_replication_info_in_slave_dbscale_mode ret number is %d\n",
            ret);
}

bool MySQLHandler::handle_authenticate_succeed(const char *user,
                                               const char *host,
                                               const char *schema_name,
                                               uint32_t client_flags,
                                               Connection *conn) {
  if (user[0])
    LOG_DEBUG("Authenticate succeed, login from %s@%s.\n", user, host);
  else
    LOG_DEBUG("Authenticate succeed, login from ''@%s.\n", host);
  // set the login schema and user name
  if (user[0]) {
    session->set_username(user);
    LOG_DEBUG("Store the login user name %s into session.\n", user);
  }
  string schema_name_tmp(schema_name);
  if (lower_case_table_names) {
    boost::to_lower(schema_name_tmp);
  }
  session->set_schema(schema_name_tmp.c_str());
  LOG_DEBUG("Store the login db %s into session.\n", schema_name);
  session->set_compress(client_flags & CLIENT_COMPRESS);
  session->set_local_infile(client_flags & CLIENT_LOCAL_FILES);
  LOG_DEBUG("Store compress flag %d local_infile flag %d into session.\n",
            client_flags & CLIENT_COMPRESS, client_flags & CLIENT_LOCAL_FILES);
  // record login time and user addr.
  InetAddr addr;
  stream->get_remote_addr(addr);
  session->set_user_addr(user, addr);
  session->init_user_priority();

  bool auth_failed = true;
  try {
    init_sessionvar(conn);
  } catch (...) {
    if (session->is_got_error_er_must_change_password()) {
      auth_failed = true;
      // we can not throw this exception, that will make client broken
    } else {
      throw;
    }
  }

  if (!session->is_got_error_er_must_change_password()) {
    auth_failed = false;
  }

  // see issue #1888
  if (slave_dbscale_mode && enable_acl) {
    recover_replication_info_in_slave_dbscale_mode();
    if (enable_acl) {
      auth_failed = !get_databases(conn, get_session()->get_is_super_user());
      if (!auth_failed)
        backend->set_auth_info_for_user_slave_dbscale_mode(user, session);
    }
    if (!auth_failed) {
      backend->set_schema_acl_info_for_user(string(user), session);
      backend->set_table_acl_info_for_user(string(user), session);
      backend->set_user_not_allow_operation_time(string(user), session);
    }
  } else if (enable_acl && !auth_failed) {
    if (!backend->get_auth_info_for_user(string(user), session)) {
      bool is_super_user_or_internal_user = false;
      if (user[0]) {
        is_super_user_or_internal_user = !strcmp(user, supreme_admin_user) ||
                                         !strcmp(user, dbscale_internal_user);
      }
      auth_failed = !get_databases(conn, is_super_user_or_internal_user);
      if (!auth_failed) {
        backend->set_auth_info_for_user(string(user), session);
      }
    }
    if (!auth_failed) {
      backend->set_schema_acl_info_for_user(string(user), session);
      backend->set_table_acl_info_for_user(string(user), session);
      backend->set_user_not_allow_operation_time(string(user), session);
    }
  }
  return !auth_failed;
}
void MySQLHandler::handle_auth_switch_request(
    MySQLAuthSwitchRequest &auth_switch_request) {
  auth_switch_request.unpack();
  if (auth_switch_request.is_caching_sha2_password_auth_plugin()) {
    get_session()->set_cur_auth_plugin(CACHING_SHA2_PASSWORD);
  } else {
    get_session()->set_cur_auth_plugin(MYSQL_NATIVE_PASSWORD);
  }
  get_session()->get_server_scramble().assign(
      auth_switch_request.get_auth_plugin_data());
}

void MySQLHandler::handle_auth_switch_response(Packet &packet) {
  MySQLAuthSwitchResponse auth_switch_response(
      &packet, get_session()->get_cur_auth_plugin());
  auth_switch_response.unpack();
  handle_auth_switch_response(auth_switch_response);
}

void MySQLHandler::handle_auth_switch_response(
    MySQLAuthSwitchResponse &auth_switch_response) {
  if (auth_switch_response.get_load_length() >= SCRAMBLE_LENGTH) {
    get_session()->set_client_scramble(
        auth_switch_response.get_auth_plugin_data());
  }
}

void MySQLHandler::get_plainpwd_by_dbscale_RSAkey() {
  Backend *backend = Backend::instance();
  const char *dbscale_rsa_public_key =
      backend->get_dbscale_rsa_public_key_as_pem();
  RSA *dbscale_rsa_private_key = backend->get_dbscale_rsa_private_key();
  int cipher_length = backend->get_dbscale_rsa_cipher();
  if (!dbscale_rsa_public_key || !dbscale_rsa_private_key ||
      cipher_length == 0) {
    LOG_ERROR(
        "Failed to get RSA key in "
        "MySQLHandler::get_plainpwd_by_dbscale_RSAkey.\n");
    throw ExecuteNodeError("Failed to get dbscale RSA key.");
  }

  Packet packet;
  MySQLAuthMoreData response_dbscale_RSA_public_key(&packet);
  response_dbscale_RSA_public_key.set_auth_more_data(
      dbscale_rsa_public_key, strlen(dbscale_rsa_public_key));
  response_dbscale_RSA_public_key.pack(&packet);
  send_to_client(&packet);

  receive_from_client(&packet);
  MySQLAuthMoreData receive_RSA_encrypt_password(&packet);
  receive_RSA_encrypt_password.unpack();

  // RSA Decrypt password
  char plainpwd[PASSWORD_LEN] = {0};
  RSA_private_decrypt(
      cipher_length,
      (unsigned char *)receive_RSA_encrypt_password.get_auth_more_data(),
      (unsigned char *)plainpwd, dbscale_rsa_private_key,
      RSA_PKCS1_OAEP_PADDING);
  plainpwd[cipher_length] = '\0';  // safety
  xor_string(plainpwd, cipher_length,
             (char *)get_session()->get_server_scramble().c_str(),
             SCRAMBLE_LENGTH);
  session->set_password(plainpwd);
}

void MySQLHandler::do_fake_full_auth_get_plainpwd() {
  const char *plainpwd = session->get_password();
  size_t plainpwd_len = strlen(plainpwd);
  Packet packet;
  if (plainpwd_len > 0) {
    // need to check if cached plainpwd is changed
    char sha2_client_scramble[SHA2_SCRAMBLE_LENGTH];
    memset(sha2_client_scramble, 0, SHA2_SCRAMBLE_LENGTH);
    sha256_scramble((unsigned char *)sha2_client_scramble, plainpwd,
                    get_session()->get_server_scramble().c_str());
    // not need update password
    if (memcmp(sha2_client_scramble, get_session()->get_client_scramble(),
               SHA2_SCRAMBLE_LENGTH) == 0) {
      MySQLAuthMoreData response_fast_auth_success(&packet);
      response_fast_auth_success.set_auth_more_data(fast_auth_success,
                                                    strlen(fast_auth_success));
      response_fast_auth_success.pack(&packet);
      send_to_client(&packet);
      return;
    }
  }

  // fast auth is success but dbscale dont know user`s plain
  // password
  MySQLAuthMoreData response_perform_full_authentication(&packet);
  response_perform_full_authentication.set_auth_more_data(
      perform_full_authentication, strlen(perform_full_authentication));
  response_perform_full_authentication.pack(&packet);
  send_to_client(&packet);
  receive_from_client(&packet);

  if (session->get_client_capabilities() & CLIENT_SSL) {
    MySQLAuthMoreData receive_plain_password(&packet);
    receive_plain_password.unpack();
    session->set_password(receive_plain_password.get_auth_more_data());
  } else if (driver->is_request_public_key(&packet)) {
    get_plainpwd_by_dbscale_RSAkey();
  }
}

void MySQLHandler::do_real_full_auth_get_plainpwd(MySQLConnection *conn,
                                                  Packet *packet) {
  send_to_client(packet);
  receive_from_client(packet);

  conn->set_session(session);
  // client send plain password on ensure connection
  if (session->get_client_capabilities() & CLIENT_SSL) {
    MySQLAuthMoreData receive_plain_password(packet);
    receive_plain_password.unpack();
    if (strcmp(session->get_password(),
               receive_plain_password.get_auth_more_data()) != 0) {
      session->set_password(receive_plain_password.get_auth_more_data());
    }

    // build request RSA public key packet
    Packet packet_send;
    MySQLAuthMoreData request_RSA_public_key(&packet_send);
    request_RSA_public_key.set_auth_more_data(&request_public_key, 1);
    request_RSA_public_key.pack(&packet_send);
    conn->sha2_rsa_auth_by_plainpwd(&packet_send, packet);
  } else if (driver->is_request_public_key(packet)) {
    get_plainpwd_by_dbscale_RSAkey();
    conn->sha2_rsa_auth_by_plainpwd(packet, packet);
  } else {
    LOG_ERROR("do_real_full_auth_get_plainpwd get error packet\n");
    throw ExecuteNodeError("Get error packet when auth.");
  }
}

void MySQLHandler::send_login_error_packet(const char *error_msg) {
  Packet packet;
  Packet *error_packet = get_error_packet(1045, error_msg, "28000");
  MySQLErrorResponse error(error_packet);
  error.unpack();
  error.pack(&packet);
  delete error_packet;
  send_to_client(&packet);
  LOG_ERROR("%s\n", error_msg);
}

void MySQLHandler::do_authenticate_with_auth_pool_conn() {
  LOG_DEBUG("MySQLHandler::do_authenticate with auth pool conn\n");
  // 1, Get one free connection from auth pool
  DataSpace *auth_dspace = backend->get_auth_data_space();
  DataServer *auth_dserver =
      auth_dspace->get_data_source()->get_master_server();

  if (!auth_dserver) {
    // some test case may use below code. in real environment should not use
    // below code!
    Connection *c_tmp = auth_dspace->get_connection(get_session());
    if (!c_tmp)
      throw Error(
          "can not get connection from auth when "
          "do_authenticate_with_auth_pool_conn");
    auth_dserver = c_tmp->get_server();
    c_tmp->get_pool()->add_back_to_free(c_tmp);
#ifdef DEBUG
    ACE_ASSERT(auth_dserver);
#endif
  }

  if (auth_dspace->get_data_source()->get_work_state() !=
      DATASOURCE_STATE_WORKING) {
    throw Error("Fail do login due to authentication datasource not working");
  } else if (!auth_dserver->is_alive()) {
    throw Error("Fail do login due to authentication dataserver not working");
  }
  MySQLConnection *pool_conn = (MySQLConnection *)auth_dspace->get_data_source()
                                   ->get_authenticate_connection();
  if (!pool_conn) {
    throw Error("Fail to login due to can't get conn from auth dataspace");
  }
  LOG_DEBUG("Do auth from server %s%@.\n", auth_dserver->get_name(),
            auth_dserver);
  // 2, Send HandShake packet to client
  string host = get_login_hostip();
  string schema_name;
  string user_name;
  Packet packet;
  TimeValue timeout_recv = TimeValue(backend_sql_net_timeout);
  MySQLInitResponse handshake = *pool_conn->get_handshake_packet();
  handshake.set_packet(0);
  try_conceal_dbscale_unsupported_flags_for_server(
      handshake, CLIENT_DEPRECATE_EOF, "CLIENT_DEPRECATE__EOF");
  try_conceal_dbscale_unsupported_flags_for_server(
      handshake, CLIENT_SESSION_TRACK, "CLIENT_SESSION_TRACK");
  try_conceal_dbscale_unsupported_flags_for_server(
      handshake, CLIENT_QUERY_ATTRIBUTE, "CLIENT_QUERY_ATTRIBUTE");
  /*
  try_conceal_dbscale_unsupported_flags_for_server(handshake,
  CLIENT_PLUGIN_AUTH, "CLIENT_PLUGIN_AUTH");
  try_conceal_dbscale_unsupported_flags_for_server(
      handshake, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
      "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA");
  try_conceal_dbscale_unsupported_flags_for_server(
      handshake, CLIENT_CONNECT_ATTRS,
      "CLIENT_CONNECT_ATTRS"); */
#ifdef HAVE_OPENSSL
  if (enable_ssl && SSLTool::instance()->get_ssl_context()) {
    handshake.set_server_capabilities(handshake.get_server_capabilities() |
                                      CLIENT_SSL);
    handshake.set_server_capabilities(handshake.get_server_capabilities() |
                                      CLIENT_SSL_VERIFY_SERVER_CERT);
  }
#endif

  uint32_t session_id = driver->get_next_global_thread_id();
  set_thread_id(session_id);
  thread_id = session_id;
  handshake.set_thread_id(session_id);
  get_session()->get_server_sramble().assign(handshake.get_server_scramble());
  get_session()->set_do_login(true);
  get_session()->record_login_time();
  get_session()->record_idle_time();

  try {
    handshake.pack(&packet);
#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "login_error") &&
        !strcasecmp(test_info->test_case_operation.c_str(), "login_timeout")) {
      LOG_DEBUG("Test login_error with login_timeout.\n");
    } else {
      send_to_client(&packet);
    }
#else
    send_to_client(&packet);
#endif
    // 3, Handle handshake response, handle ssl if needed
    receive_from_client(&packet);
    MySQLAuthRequest handshake_response(&packet);
    handshake_response.set_first_handle_ssl(true);
    handshake_response.unpack();
    session->set_client_capabilities(
        handshake_response.get_client_capabilities());
    session->set_client_charset(handshake_response.get_client_charset());

    bool client_client_ssl =
        (handshake_response.get_client_capabilities() & CLIENT_SSL) > 0 ? true
                                                                        : false;
    if (client_client_ssl) {
      SSLTool *stool = SSLTool::instance();
      stool->handle_ssl_connection(this);

      // re-receive the auth packet after the ssl connection
      receive_from_client(&packet);
      handshake_response.set_packet(&packet);
      handshake_response.set_first_handle_ssl(false);
      handshake_response.unpack();
    }

    try_conceal_dbscale_unsupported_flags_for_client(
        handshake_response, CLIENT_DEPRECATE_EOF, "CLIENT_DEPRECATE_EOF");
    try_conceal_dbscale_unsupported_flags_for_client(
        handshake_response, CLIENT_SESSION_TRACK, "CLIENT_SESSION_TRACK");
    try_conceal_dbscale_unsupported_flags_for_client(
        handshake_response, CLIENT_QUERY_ATTRIBUTE, "CLIENT_QUERY_ATTRIBUTE");
    try_conceal_dbscale_unsupported_flags_for_client(
        handshake_response, CLIENT_QUERY_ATTRIBUTE, "CLIENT_SSL");
    const char *schema = handshake_response.get_schema();
    user_name = handshake_response.get_username();
    uint32_t client_flags = handshake_response.get_client_capabilities();
    if (handshake_response.is_mysql_native_password_auth_plugin()) {
      session->set_cur_auth_plugin(MYSQL_NATIVE_PASSWORD);
    } else if (handshake_response.is_caching_sha2_password_auth_plugin()) {
      session->set_cur_auth_plugin(CACHING_SHA2_PASSWORD);
    } else {
      send_login_error_packet(
          "Unsupport authentication plugins other than "
          "\"mysql_native_password\" and \"caching_sha2_password\".");
      throw HandlerError(
          "Unsupport authentication plugins other than "
          "\"mysql_native_password\" and \"caching_sha2_password\".");
    }
    get_session()->set_client_scramble(
        handshake_response.get_client_scramble());
    reset_auth_packet_for_compress(client_flags, &packet);

    // 4, login check
    login_check(user_name.c_str(), host.c_str(), schema, client_client_ssl);
    if (schema[0] && strcmp(schema, TMP_TABLE_SCHEMA) &&
        strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA)) {
      schema_name = schema;
      LOG_DEBUG("Login with db: %s.\n", schema_name.c_str());
    } else {  // set default login schema
      if (enable_session_swap &&
          strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA) == 0) {
        session->set_need_swap_handler(false);
        // this session is come from other dbscale, no need swap this session
        // this session may do sync operation, is swap, it may block all
        // sessions then this session is also able to continue cause swap.
        // increase traditional_num, the num will be subed when delete session
      }

      schema_name = default_login_schema;
      LOG_DEBUG("Login with default db: %s.\n", schema_name.c_str());
    }
    // 5, Send ChangeUser packet to server with pool conn
    MySQLChangeUserRequest change_user(
        pool_conn->get_client_capabilities(), handshake.get_server_scramble(),
        user_name.c_str(), NULL, schema_name.c_str(),
        get_session()->get_cur_auth_plugin());
    change_user.set_charset(handshake_response.get_client_charset());
    change_user.set_auth_plugin_name(handshake_response.get_auth_plugin_name());
    LOG_DEBUG("Change user client scramble: [%s]\n",
              handshake_response.get_client_scramble());
    change_user.set_client_scramble(handshake_response.get_client_scramble());
    change_user.pack(&packet);
    send_to_server(pool_conn, &packet);
    // 6, Handle auth packets, handle auth switch if needed
    receive_from_server(pool_conn, &packet, &timeout_recv);
    bool is_succeed = true;
    while (1) {
      if (driver->is_ok_packet(&packet)) {
        send_to_client(&packet);
        is_succeed = handle_authenticate_succeed(
            user_name.c_str(), host.c_str(), schema_name.c_str(), client_flags,
            pool_conn);

        string tmp_plainpwd = "";
        if (!backend->get_sha2_plainpwd(user_name, tmp_plainpwd) ||
            strcmp(tmp_plainpwd.c_str(), session->get_password()) != 0) {
          backend->add_sha2_plainpwd(user_name, session->get_password());
        }
        break;
      } else if (driver->is_error_packet(&packet)) {
        send_to_client(&packet);
        MySQLErrorResponse error(&packet);
        error.unpack();
        if (error.is_shutdown()) {
          get_session()->set_do_login(false);
          throw HandlerError(error.get_error_message(), error.get_error_code());
        }
        record_audit_login(thread_id, user_name.c_str(), "", host,
                           schema ? schema : "", error.get_error_code());
        LOG_ERROR("Execution failed with error: %d (%s) %s\n",
                  error.get_error_code(), error.get_sqlstate(),
                  error.get_error_message());
        throw HandlerError(error.get_error_message(), error.get_error_code());
      } else if (driver->is_auth_switch_request_packet(&packet)) {
        MySQLAuthSwitchRequest auth_switch_request(&packet);
        handle_auth_switch_request(auth_switch_request);
        if (strlen(auth_switch_request.get_auth_plugin_data()) > 1) {
          pool_conn->reset_handshake_packet(
              get_session()->get_server_sramble().c_str());
        }
        if (auth_switch_request.is_mysql_native_password_auth_plugin()) {
          if (handshake_response.is_mysql_native_password_auth_plugin()) {
            // For compatibility, we do not send auth switch packet to client
            // when the authenticate-plugin from client and mysql server both
            // are `mysql_native_password`.
            uint8_t client_scramble_len =
                handshake_response.get_client_scramble_len();
            MySQLAuthSwitchResponse auth_switch_response(
                client_scramble_len > 0
                    ? handshake_response.get_client_scramble()
                    : 0,
                MYSQL_NATIVE_PASSWORD);
            handle_auth_switch_response(auth_switch_response);
            auth_switch_response.pack(&packet);
            send_to_server(pool_conn, &packet);
            receive_from_server(pool_conn, &packet, &timeout_recv);
          } else {
            send_to_client(&packet);
            receive_from_client(&packet);
            handle_auth_switch_response(packet);
            send_to_server(pool_conn, &packet);
            receive_from_server(pool_conn, &packet, &timeout_recv);
          }
        } else if (auth_switch_request.is_caching_sha2_password_auth_plugin()) {
          // AuthSwitchRequest (Nonce)
          send_to_client(&packet);
          receive_from_client(&packet);
          handle_auth_switch_response(packet);
          // AuthSwitchResponse (Scramble)
          send_to_server(pool_conn, &packet);
          receive_from_server(pool_conn, &packet, &timeout_recv);

          if (strlen(session->get_password()) == 0) {
            string tmp_plainpwd = "";
            if (backend->get_sha2_plainpwd(user_name, tmp_plainpwd)) {
              session->set_password(tmp_plainpwd.c_str());
            } else {
              session->set_password("");
            }
          }
          if (!driver->is_auth_more_data_packet(&packet)) {
            // client use empty passwd, server directly send OK packet
            continue;
          }

          if (driver->is_fast_auth_success_packet(&packet)) {
            receive_from_server(pool_conn, &packet, &timeout_recv);
            if (driver->is_ok_packet(&packet)) {
              do_fake_full_auth_get_plainpwd();
            }
          } else if (driver->is_perform_full_authentication_packet(&packet)) {
            do_real_full_auth_get_plainpwd(pool_conn, &packet);
          }
        } else {
          send_login_error_packet(
              "Unsupport authentication plugins other than "
              "\"mysql_native_password\" and \"caching_sha2_password\".");
          throw HandlerError(
              "Unsupport authentication plugins other than "
              "\"mysql_native_password\" and \"caching_sha2_password\".");
        }
        continue;
      } else {
        // exchange packets
        receive_from_client(&packet);
        send_to_server(pool_conn, &packet);
        receive_from_server(pool_conn, &packet, &timeout_recv);
        send_to_client(&packet);
        continue;
      }
    }

    if (!is_succeed) {
      record_audit_login(thread_id, user_name.c_str(), string(""), host,
                         schema ? schema : "", 1044);
      if (!session->is_got_error_er_must_change_password()) {
        throw HandlerError("authenticate failed");
      }
    }

    pool_conn->get_cur_user(get_session()->get_user_host());
    record_audit_login(thread_id, session->get_user_name(),
                       session->get_user_host(), session->get_client_ip(),
                       session->get_schema(), 0);

    get_session()->set_do_login(false);
    driver->insert_session(get_session());
    if (!session->is_got_error_er_must_change_password()) {
      check_and_init_session_charset(get_session());
    }

    pool_conn->get_pool()->add_back_to_free(pool_conn);
  } catch (HandlerError &e) {
    pool_conn->get_pool()->add_back_to_dead(pool_conn);
    LOG_DEBUG("Fail do login for client from ip %s caused by: %s.\n",
              host.c_str(), e.what());
    get_session()->set_do_login(false);
    throw;
  } catch (std::exception &e) {
    pool_conn->get_pool()->add_back_to_dead(pool_conn);
    record_audit_login(thread_id, user_name.c_str(), "", host,
                       schema_name.c_str(), 2013);
    LOG_DEBUG("Fail do login for client from ip %s cuased by: %s.\n",
              host.c_str(), e.what());
    get_session()->set_do_login(false);
    throw;
  }
}

void MySQLHandler::do_authenticate() {
  if (authenticate_with_pool_conn) return do_authenticate_with_auth_pool_conn();
  LOG_DEBUG("MySQLHandler::do_authenticate\n");
  char schema_name[SCHEMA_LEN + 1];
  char user_name[USER_LEN + 1];
  const char *schema = NULL;
  const char *user = NULL;
  uint32_t client_flags;
  uint32_t thread_id = 0;
  bool is_auth_server_normal = true;
  string tmp_server_scramble;

  DataSpace *data_space = backend->get_auth_data_space();
  DataServer *data_server = data_space->get_data_source()->get_master_server();
  if (!data_server) {
    // some test case may use below code. in real environment should not use
    // below code!
    Connection *c_tmp = data_space->get_connection(get_session());
    if (!c_tmp)
      throw Error("can not get connection from auth when do_authenticate");
    data_server = c_tmp->get_server();
    c_tmp->get_pool()->add_back_to_free(c_tmp);
#ifdef DEBUG
    ACE_ASSERT(data_server);
#endif
  }
#ifndef DBSCALE_TEST_DISABLE
  /*just for test coverage*/
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "test_lost_connection") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "auth")) {
    Connection *conn_tmp = data_space->get_connection();
    LOG_DEBUG("test_lost_connection [%@] id=[%d]\n", conn_tmp,
              conn_tmp->get_thread_id());
  }
#endif

  MySQLConnection *conn = NULL;
  string host = "";
  MySQLAuthRequest *auth = NULL;
  TimeValue timeout(connect_timeout);
  MySQLConnection auth_conn(driver, data_server, NULL, NULL, NULL, &timeout);
  try {
    host = get_login_hostip();
    if (data_space->get_data_source()->get_work_state() !=
        DATASOURCE_STATE_WORKING) {
      LOG_WARN("When do authenticate, auth datasource status %s\n",
               data_space->get_data_source()->get_status());
      // throw Error("Fail do login due to authentication datasource not
      // working");
      is_auth_server_normal = false;
    } else if (!data_server->is_alive()) {
      LOG_WARN("When do authenticate, data server %s is not alive.\n",
               data_server->get_name());
      // throw Error("Fail do login due to authentication dataserver not
      // working");
      is_auth_server_normal = false;
    }
#ifndef DBSCALE_TEST_DISABLE
    Backend *bk = Backend::instance();
    dbscale_test_info *test_info = bk->get_dbscale_test_info();
    if (!strcasecmp(test_info->test_case_name.c_str(), "auth_abnormal") &&
        !strcasecmp(test_info->test_case_operation.c_str(),
                    "auth_not_available")) {
      is_auth_server_normal = false;
      LOG_DEBUG("Test auth datasource abnormal, construct handshake packet.\n");
    }
#endif
    Packet packet;
    TimeValue timeout_recv = TimeValue(backend_sql_net_timeout);

    if (!strcmp(dbscale_read_only_user, "") && !is_auth_server_normal) {
      structure_handshake_by_dbscale(packet, tmp_server_scramble, thread_id);
    } else {
      LOG_DEBUG("Do auth from server %s%@.\n", data_server->get_name(),
                data_server);
      conn = &auth_conn;
      get_session()->set_audit_log_flag_local();

      conn->raw_connect();
      set_nonblock(conn->get_stream());
      set_tcp_user_timeout_and_keepalive(conn->get_stream(), conn,
                                         tcp_multiple_times);
      receive_from_server(conn, &packet, &timeout_recv);
      // initial handshack packet
      if (driver->is_error_packet(&packet)) {
        send_to_client(&packet);
        conn->quit();
        conn->close();
        conn = NULL;
        LOG_ERROR("Can not login the authentication. Get error Packet\n");
        MySQLErrorResponse error(&packet);
        error.unpack();
        LOG_DEBUG("command login authentication failed: %d (%s) %s\n",
                  error.get_error_code(), error.get_sqlstate(),
                  error.get_error_message());
        record_audit_login(thread_id, string(""), string(""), host, "",
                           error.get_error_code());
        throw HandlerQuit();
      }
      MySQLInitResponse init(&packet);
      init.unpack();
      try_conceal_dbscale_unsupported_flags_for_server(
          init, CLIENT_DEPRECATE_EOF, "CLIENT_DEPRECATE__EOF");
      try_conceal_dbscale_unsupported_flags_for_server(
          init, CLIENT_SESSION_TRACK, "CLIENT_SESSION_TRACK");
      try_conceal_dbscale_unsupported_flags_for_server(
          init, CLIENT_QUERY_ATTRIBUTE, "CLIENT_QUERY_ATTRIBUTE");
      // ensure the server enable ssl support
#ifdef HAVE_OPENSSL
      if (enable_ssl && SSLTool::instance()->get_ssl_context()) {
        init.set_server_capabilities(init.get_server_capabilities() |
                                     CLIENT_SSL);
        init.set_server_capabilities(init.get_server_capabilities() |
                                     CLIENT_SSL_VERIFY_SERVER_CERT);
      }
#endif

      uint32_t session_id = driver->get_next_global_thread_id();
      set_thread_id(session_id);
      thread_id = session_id;
      init.set_thread_id(session_id);
      init.pack(&packet);
      tmp_server_scramble = init.get_server_scramble();
      get_session()->get_server_sramble().assign(init.get_server_scramble());
      get_session()->set_do_login(true);
      get_session()->record_login_time();
      get_session()->record_idle_time();
#ifndef DBSCALE_TEST_DISABLE
      Backend *bk = Backend::instance();
      dbscale_test_info *test_info = bk->get_dbscale_test_info();
      if (!strcasecmp(test_info->test_case_name.c_str(), "login_error") &&
          !strcasecmp(test_info->test_case_operation.c_str(),
                      "login_timeout")) {
        LOG_DEBUG("Test login_error with login_timeout.\n");
      } else {
        send_to_client(&packet);
      }
#else
      send_to_client(&packet);
#endif
      // authenticate packet
      receive_from_client(&packet);
    }
    // get the login schema and user
    auth = new MySQLAuthRequest(&packet);
    auth->set_first_handle_ssl(true);
    auth->unpack();
    ((MySQLSession *)get_session())
        ->set_client_capabilities(auth->get_client_capabilities());
    session->set_client_charset(auth->get_client_charset());
    bool client_client_ssl =
        (auth->get_client_capabilities() & CLIENT_SSL) > 0 ? true : false;
    if (client_client_ssl) {
      SSLTool *stool = SSLTool::instance();
      stool->handle_ssl_connection(this);

      // re-receive the auth packet after the ssl connection
      receive_from_client(&packet);
      delete auth;
      auth = new MySQLAuthRequest(&packet);
      auth->unpack();
    }

    try_conceal_dbscale_unsupported_flags_for_client(
        *auth, CLIENT_DEPRECATE_EOF, "CLIENT_DEPRECATE_EOF");
    try_conceal_dbscale_unsupported_flags_for_client(
        *auth, CLIENT_SESSION_TRACK, "CLIENT_SESSION_TRACK");
    try_conceal_dbscale_unsupported_flags_for_client(
        *auth, CLIENT_QUERY_ATTRIBUTE, "CLIENT_QUERY_ATTRIBUTE");
    //
    // the server part is not doing ssl now
    auth->set_client_capabilities(auth->get_client_capabilities() &
                                  ~CLIENT_SSL);
    schema = auth->get_schema();
    user = auth->get_username();
    client_flags = auth->get_client_capabilities();
    if (auth->is_mysql_native_password_auth_plugin()) {
      session->set_cur_auth_plugin(MYSQL_NATIVE_PASSWORD);
    } else if (auth->is_caching_sha2_password_auth_plugin()) {
      session->set_cur_auth_plugin(CACHING_SHA2_PASSWORD);
    } else {
      send_login_error_packet(
          "Unsupport authentication plugins other than "
          "\"mysql_native_password\" and \"caching_sha2_password\".");
      throw HandlerError(
          "Unsupport authentication plugins other than "
          "\"mysql_native_password\" and \"caching_sha2_password\".");
    }
    get_session()->set_client_scramble(auth->get_client_scramble());
    reset_auth_packet_for_compress(client_flags, &packet);

    bool is_super_user_or_internal_user = false;
    bool is_dbscale_read_only_user = false;

    if (user[0]) {
      copy_string(user_name, user, sizeof(user_name));
      LOG_DEBUG("Login with user name: %s.\n", user_name);
      is_super_user_or_internal_user = !strcmp(user, supreme_admin_user) ||
                                       !strcmp(user, dbscale_internal_user);
      if (is_super_user_or_internal_user)
        get_session()->set_is_super_user(true);
      is_dbscale_read_only_user = !strcmp(user, dbscale_read_only_user);
    }

    if (!is_auth_server_normal && !is_dbscale_read_only_user) {
      throw HandlerError(
          "Auth server abnormal, non-dbscale_read_only_user login is "
          "prohibited.");
    }

    if (is_dbscale_read_only_user) {
      do_authenticate_check_by_dbscale(auth, packet, tmp_server_scramble,
                                       host.c_str());
    } else {
      if (slave_dbscale_mode) {
        recover_replication_info_in_slave_dbscale_mode();
      }

      ssl_option_struct ssl_option_value;
      ssl_option_value.ssl_option = SSL_OPTION_NULL;
      if (enable_acl && !white_filter_check(host, user, ssl_option_value)) {
        string err_message(
            "Access denied due to white list check fail for client:");
        err_message.append(host.c_str());
        Packet *error_packet =
            get_error_packet(1045, err_message.c_str(), "28000");
        MySQLErrorResponse error(error_packet);
        error.unpack();
        error.pack(&packet);
        delete error_packet;
        send_to_client(&packet);
      }

#ifdef HAVE_OPENSSL
      if (ssl_option_value.ssl_option != SSL_OPTION_NONE &&
          ssl_option_value.ssl_option != SSL_OPTION_NULL) {
        if (!client_client_ssl ||
            (ssl_option_value.ssl_option != SSL_OPTION_SSL &&
             !SSLTool::instance()->check_ssl_cert(this, ssl_option_value))) {
          LOG_INFO("Check client cert failed for user:%s@%s.\n", user_name,
                   host.c_str());
          string err_message("SSL cert check failed for client:");
          err_message.append(host.c_str());
          Packet *error_packet =
              get_error_packet(1045, err_message.c_str(), "28000");
          MySQLErrorResponse error(error_packet);
          error.unpack();
          error.pack(&packet);
          delete error_packet;
          send_to_client(&packet);
          throw HandlerError("SSL cert check failed.");
        }
      }
#endif

      // max-conn-filter filter
      unsigned int conn_num = driver->get_session_count();
#ifdef DEBUG
      LOG_DEBUG(
          "Max-conn-filter: current conn_num = [%d], max_conn_limit = [%d]\n",
          conn_num, max_conn_limit);
#endif
      if ((conn_num >= max_conn_limit) &&
          !(is_super_user_or_internal_user &&
            (conn_num < (max_conn_limit + 1)))) {
#ifdef DEBUG
        LOG_DEBUG("Login max-conn-filter: block username:%s\n", user);
#endif
        Packet *error_packet =
            get_error_packet(1045, "Max connection limitation", "28000");
        MySQLErrorResponse error(error_packet);
        error.unpack();
        error.pack(&packet);
        delete error_packet;
        send_to_client(&packet);
      } else if (user[0] && !strcmp(dbscale_internal_user, user_name) &&
                 !multiple_mode &&
                 (!schema[0] ||
                  (schema[0] && strcmp(schema, TMP_TABLE_SCHEMA) &&
                   strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA)))) {
        // some dbscale internal operation will need to use
        // dbscale_internal_user to do the login, such as multiple dbsale and
        // federate cross node join.
#ifdef DEBUG
        LOG_DEBUG("Forbid login with dbscale internal user.\n");
#endif
        Packet *error_packet = get_error_packet(
            1045, "Access denied for dbscale internal user", "28000");
        MySQLErrorResponse error(error_packet);
        error.unpack();
        error.pack(&packet);
        delete error_packet;
        send_to_client(&packet);
      } else {
        if (schema[0] && strcmp(schema, TMP_TABLE_SCHEMA) &&
            strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA)) {
          copy_string(schema_name, schema, sizeof(schema_name));
          LOG_DEBUG("Login with db: %s.\n", schema_name);
          send_to_server(conn, &packet);
          receive_from_server(conn, &packet, &timeout_recv);
        } else {  // set default login schema
          if (enable_session_swap &&
              strcmp(schema, DBSCALE_CLUSTER_ADMIN_SCHEMA) == 0) {
            session->set_need_swap_handler(false);
            // this session is come from other dbscale, no need swap this
            // session this session may do sync operation, is swap, it may block
            // all sessions then this session is also able to continue cause
            // swap. increase traditional_num, the num will be subed when delete
            // session
          }

          Packet new_auth_packet;
          copy_string(schema_name, default_login_schema, sizeof(schema_name));
          LOG_DEBUG("Login with default db: %s.\n", schema_name);
          auth->set_schema(schema_name);
          auth->pack(&new_auth_packet);
          send_to_server(conn, &new_auth_packet);
          receive_from_server(conn, &packet, &timeout_recv);
        }
      }
    }
    bool auth_failed = true;
    bool is_negotiating_auth_plugin = true;
    string str_user_name = user_name;

    while (1) {
      if (driver->is_ok_packet(&packet)) {
        send_to_client(&packet);
        if (user[0])
          LOG_DEBUG("Authenticate succeed, login from %s@%s.\n", user,
                    host.c_str());
        else
          LOG_DEBUG("Authenticate succeed, login from ''@%s.\n", host.c_str());
        // set the login schema and user name
        if (user[0]) {
          session->set_username(user_name);
          LOG_DEBUG("Store the login user name %s into session.\n", user_name);
        }
        if (!is_dbscale_read_only_user) {
          if (schema[0]) {
            string schema_name_tmp(schema_name);
            if (lower_case_table_names) {
              boost::to_lower(schema_name_tmp);
            }
            session->set_schema(schema_name_tmp.c_str());
            LOG_DEBUG("Store the login db %s into session.\n", schema_name);
          }
        }
        session->set_compress(client_flags & CLIENT_COMPRESS);
        session->set_local_infile(client_flags & CLIENT_LOCAL_FILES);
        LOG_DEBUG("Store compress flag %d local_infile flag %d into session.\n",
                  client_flags & CLIENT_COMPRESS,
                  client_flags & CLIENT_LOCAL_FILES);
        // record login time and user addr.
        InetAddr addr;
        stream->get_remote_addr(addr);
        session->set_user_addr(user, addr);
        session->init_user_priority();

        if (!is_dbscale_read_only_user) {
          try {
            init_sessionvar(conn);
          } catch (...) {
            if (session->is_got_error_er_must_change_password()) {
              auth_failed = true;
              // we can not throw this exception, that will make client broken
            } else {
              throw;
            }
          }
          if (!session->is_got_error_er_must_change_password()) {
            auth_failed = false;
          }
          if (enable_acl && slave_dbscale_mode) {
            auth_failed =
                !get_databases(conn, get_session()->get_is_super_user());
            if (!auth_failed)
              backend->set_auth_info_for_user_slave_dbscale_mode(user, session);
            if (!auth_failed) {
              backend->set_schema_acl_info_for_user(user_name, session);
              backend->set_table_acl_info_for_user(user_name, session);
              backend->set_user_not_allow_operation_time(user_name, session);
            }
          } else if (enable_acl && !auth_failed) {
            if (!backend->get_auth_info_for_user(user_name, session)) {
              auth_failed =
                  !get_databases(conn, is_super_user_or_internal_user);
              if (!auth_failed) {
                backend->set_auth_info_for_user(user, session);
              }
            }
            if (!auth_failed) {
              backend->set_schema_acl_info_for_user(user_name, session);
              backend->set_table_acl_info_for_user(user_name, session);
              backend->set_user_not_allow_operation_time(user_name, session);
            }
          }

          string tmp_plainpwd = "";
          if (!backend->get_sha2_plainpwd(str_user_name, tmp_plainpwd) ||
              strcmp(tmp_plainpwd.c_str(), session->get_password()) != 0) {
            backend->add_sha2_plainpwd(str_user_name, session->get_password());
          }
        }
        break;
      } else if (driver->is_error_packet(&packet)) {
        send_to_client(&packet);
        MySQLErrorResponse error(&packet);
        error.unpack();
        if (error.is_shutdown()) {
          if (conn) {
            conn->close();
            conn = NULL;
          }
          get_session()->set_do_login(false);
          throw HandlerError("authenticate failed");
        }
        record_audit_login(thread_id, user ? string(user) : string(""),
                           string(""), host, schema ? schema : "",
                           error.get_error_code());
        LOG_ERROR("Execution failed with error: %d (%s) %s\n",
                  error.get_error_code(), error.get_sqlstate(),
                  error.get_error_message());
        break;
      } else if (session->get_cur_auth_plugin() == CACHING_SHA2_PASSWORD &&
                 driver->is_auth_more_data_packet(&packet)) {
        if (strlen(session->get_password()) == 0) {
          string tmp_plainpwd = "";
          if (backend->get_sha2_plainpwd(user_name, tmp_plainpwd)) {
            session->set_password(tmp_plainpwd.c_str());
          } else {
            session->set_password("");
          }
        }
        if (driver->is_fast_auth_success_packet(&packet)) {
          /*Fast authentication (client part) was successful. The next package
          expected from server to be one of OK, ERROR, or CHANGE_PLUGIN
          packets.*/
          receive_from_server(conn, &packet, &timeout_recv);
          if (driver->is_ok_packet(&packet)) {
            do_fake_full_auth_get_plainpwd();
          }
        } else if (driver->is_perform_full_authentication_packet(&packet)) {
          do_real_full_auth_get_plainpwd(conn, &packet);
        }
      } else {
        /* IF CLIENT_PLUGIN_AUTH of Capabilities in Initial Handshake Packet is
         * different than the one in andshake Response Packet. The server will
         * send Authentication Method Switch Request Packet client and server
         * exchange further packets as required by the authentication method
         * used. If client do not support this plugin, client will disconnect
         * the connection. Or the server will send Old Authentication Method
         * Switch Request Packet. And the client will response some packet. So
         * We won't konw how many packets will exchange. We will transfer them
         * all.
         * */
        send_to_client(&packet);
        if (is_negotiating_auth_plugin) {
          MySQLAuthSwitchRequest auth_switch_request(&packet);
          auth_switch_request.unpack();
          if (auth_switch_request.is_mysql_native_password_auth_plugin()) {
            session->set_cur_auth_plugin(MYSQL_NATIVE_PASSWORD);
          } else if (auth_switch_request
                         .is_caching_sha2_password_auth_plugin()) {
            session->set_cur_auth_plugin(CACHING_SHA2_PASSWORD);
            get_session()->get_server_scramble().assign(
                auth_switch_request.get_auth_plugin_data());
          }
        }
        receive_from_client(&packet);
        if (is_negotiating_auth_plugin) {
          MySQLAuthSwitchResponse auth_switch_response(
              &packet, get_session()->get_cur_auth_plugin());
          auth_switch_response.unpack();
          if (auth_switch_response.get_load_length() >= SCRAMBLE_LENGTH) {
            get_session()->set_client_scramble(
                auth_switch_response.get_auth_plugin_data());
          }
          is_negotiating_auth_plugin = false;
        }
        send_to_server(conn, &packet);
        receive_from_server(conn, &packet, &timeout_recv);
      }
    }
    if (is_dbscale_read_only_user) {
      record_audit_login(thread_id, session->get_user_name(),
                         session->get_user_host(), session->get_client_ip(),
                         session->get_schema(), 0);
    } else if (!auth_failed) {
      conn->get_cur_user(get_session()->get_user_host());
      record_audit_login(thread_id, session->get_user_name(),
                         session->get_user_host(), session->get_client_ip(),
                         session->get_schema(), 0);
    }

    if (conn) {
      conn->quit();
      conn->close();
      conn = NULL;
    }
    get_session()->set_do_login(false);
    if (!is_dbscale_read_only_user && auth_failed) {
      record_audit_login(thread_id, user ? string(user) : string(""),
                         string(""), host, schema ? schema : "", 1044);
      if (!session->is_got_error_er_must_change_password()) {
        throw HandlerError("authenticate failed");
      }
    }
    driver->insert_session(get_session());
    delete auth;
    auth = NULL;
    if (!session->is_got_error_er_must_change_password()) {
      check_and_init_session_charset(get_session());
    }
  } catch (HandlerError &e) {
    if (auth) {
      delete auth;
      auth = NULL;
    }
    if (conn) {
      conn->close();
      conn = NULL;
    }
    LOG_DEBUG("Fail do login for client from ip %s.\n", host.c_str());
    get_session()->set_do_login(false);
    throw;
  } catch (...) {
    record_audit_login(thread_id, user ? string(user) : string(""), string(""),
                       host, schema ? schema : "", 2013);
    if (auth) {
      delete auth;
      auth = NULL;
    }
    if (conn) {
      conn->close();
      conn = NULL;
    }
    LOG_DEBUG("Fail do login for client from ip %s.\n", host.c_str());
    get_session()->set_do_login(false);
    throw;
  }
}

void MySQLHandler::structure_handshake_by_dbscale(Packet &packet,
                                                  string &server_scramble,
                                                  uint32_t &thread_id) {
  LOG_DEBUG(
      "auth server abnormal, and config dbscale_read_only_user, will structure "
      "handshake by dbscale.\n");
  struct HandShakePacket *hand_packet_info = new HandShakePacket();
  init_handshake_pack_info(hand_packet_info);
  MySQLInitResponse init(1);

  init.set_protocol_version(hand_packet_info->protocol_version);
  init.set_server_version(hand_packet_info->server_version.c_str());

  uint32_t session_id = driver->get_next_global_thread_id();
  thread_id = session_id;
  init.set_thread_id(session_id);

  generate_user_salt(hand_packet_info->server_scramble, SCRAMBLE_LENGTH + 1);
  init.set_scramble_len(SCRAMBLE_LENGTH + 1);
  init.set_server_scramble(hand_packet_info->server_scramble);
  server_scramble = init.get_server_scramble();
  init.set_server_capabilities(hand_packet_info->server_capabilities);
  init.set_server_charset(hand_packet_info->server_charset);
  init.set_server_status(hand_packet_info->server_status);
  if (init.get_server_capabilities() & CLIENT_PLUGIN_AUTH) {
    init.set_auth_plugin_name(hand_packet_info->auth_plugin_name.c_str());
  }
  try_conceal_dbscale_unsupported_flags_for_server(init, CLIENT_DEPRECATE_EOF,
                                                   "CLIENT_DEPRECATE_EOF");
  try_conceal_dbscale_unsupported_flags_for_server(init, CLIENT_SESSION_TRACK,
                                                   "CLIENT_SESSION_TRACK");
  try_conceal_dbscale_unsupported_flags_for_server(init, CLIENT_QUERY_ATTRIBUTE,
                                                   "CLIENT_QUERY_ATTRIBUTE");

  delete hand_packet_info;
  hand_packet_info = NULL;
#ifdef HAVE_OPENSSL
  if (SSLTool::instance()->get_ssl_context()) {
    init.set_server_capabilities(init.get_server_capabilities() | CLIENT_SSL);
    init.set_server_capabilities(init.get_server_capabilities() |
                                 CLIENT_SSL_VERIFY_SERVER_CERT);
  }
#endif
  init.pack(&packet);
  get_session()->get_server_sramble().assign(server_scramble);
  get_session()->set_do_login(true);
  get_session()->record_login_time();
  get_session()->record_idle_time();
#ifndef DBSCALE_TEST_DISABLE
  Backend *bk = Backend::instance();
  dbscale_test_info *test_info = bk->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "login_error") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "login_timeout")) {
    LOG_DEBUG("Test login_error with login_timeout.\n");
  } else {
    send_to_client(&packet);
  }
#else
  send_to_client(&packet);
#endif
  // authenticate packet
  receive_from_client(&packet);
}

bool MySQLHandler::do_authenticate_check_password(const char *client_scramble,
                                                  string server_scramble) {
  uint8_t hash_stage[SHA1_HASH_SIZE];
  compute_sha1_hash(hash_stage, dbscale_read_only_password,
                    strlen(dbscale_read_only_password));
  compute_sha1_hash(hash_stage, (const char *)hash_stage, SHA1_HASH_SIZE);
  if (check_scramble_sha1((const uint8_t *)client_scramble,
                          server_scramble.c_str(), hash_stage)) {
    return true;
  }
  return false;
}

void MySQLHandler::do_authenticate_check_by_dbscale(MySQLAuthRequest *auth,
                                                    Packet &packet,
                                                    string &server_scramble,
                                                    const char *host) {
  LOG_DEBUG("do_authenticate_check_by_dbscale\n");
  char user_name[USER_LEN + 1];
  const char *user = auth->get_username();

  if (user[0]) copy_string(user_name, user, sizeof(user_name));

  const char *tmp_client_scramble = auth->get_client_scramble();
  bool is_send_error_packet_client = false;
  if (tmp_client_scramble[0] &&
      !strcmp(auth->get_auth_plugin_name(), DEFAULT_MYSQL_AUTH_PLUGIN_NAME)) {
    is_send_error_packet_client =
        do_authenticate_check_password(tmp_client_scramble, server_scramble);
  } else {
    MySQLAuthSwitchRequest *auth_switch = new MySQLAuthSwitchRequest(NULL);
    auth_switch->set_auth_plugin_data(server_scramble.c_str());
    auth_switch->set_auth_plugin_name(DEFAULT_MYSQL_AUTH_PLUGIN_NAME,
                                      strlen(DEFAULT_MYSQL_AUTH_PLUGIN_NAME));
    auth_switch->pack(&packet);
    delete auth_switch;
    auth_switch = NULL;
    send_to_client(&packet);
    receive_from_client(&packet);
    MySQLAuthSwitchResponse auth_switch_response(&packet);
    auth_switch_response.unpack();
    if (auth_switch_response.get_load_length() >= SCRAMBLE_LENGTH) {
      auth->set_client_scramble(auth_switch_response.get_auth_plugin_data());
      get_session()->set_client_scramble(auth->get_client_scramble());
    } else {
      auth->set_client_scramble(0);
    }
    tmp_client_scramble = auth->get_client_scramble();
    if (tmp_client_scramble[0]) {
      is_send_error_packet_client =
          do_authenticate_check_password(tmp_client_scramble, server_scramble);
    } else {
      is_send_error_packet_client = true;
    }
  }
  if (is_send_error_packet_client) {
    string err_message("Access denied for dbscale read only user ");
    err_message.append(user_name);
    err_message.append("@");
    err_message.append(host);
    Packet *error_packet = get_error_packet(1045, err_message.c_str(), "28000");
    MySQLErrorResponse error(error_packet);
    error.unpack();
    error.pack(&packet);
    delete error_packet;
    send_to_client(&packet);
  }
  MySQLOKResponse ok(0, 0);
  ok.pack(&packet);
  get_session()->set_is_dbscale_read_only_user(true);
}

void MySQLHandler::do_remove_session() {
  driver->remove_session(get_session());
}

// TODO: use libevent in issue #5635
void MySQLHandler::receive_from_client_compress(Packet *packet) {
  size_t head_size = PACKET_HEADER_SIZE + COMP_HEADER_SIZE;
  if (session->uncompress_buff_is_null()) {
    session->uncompress_malloc_buff();
  }
  if (session->uncompress_has_data()) {
    session->get_data_from_uncompress_buffer(packet);
  } else {
    if (wait_timeout == 0) {
      driver->recv_packet(stream, session->get_ssl_stream(), packet, head_size);
    } else {
      driver->recv_packet(stream, session->get_ssl_stream(), packet, head_size,
                          &wait_timeout_tv);
    }
    MySQLCompress::my_uncompress(packet);
    session->set_packet_from_buff(false);
    uint8_t number = driver->get_packet_number(packet);
    session->check_packet_number(session->get_packet_number(), number);
    session->add_compress_packet_number();
  }
  session->add_packet_number();

  char *pos = packet->base();
  size_t packet_real_len = Packet::unpack3uint(pos) + PACKET_HEADER_SIZE;
  size_t packet_len = packet->length();

  // check if has other packets at tail
  if (!session->is_packet_from_buff() && (packet_real_len < packet_len)) {
    session->store_data_to_uncompress_buffer(packet_len - packet_real_len,
                                             packet->base() + packet_real_len);
  }

  // check if has received whole packet
  if (packet_real_len > packet_len) {
    LOG_DEBUG(
        "One packet is splitted into two or more compressed packets,"
        " receiving tailing packets from client.\n");
    packet->size(packet_real_len + 1);
    Packet tmp_packet;
    size_t tmp_packet_len;
    do {
      if (wait_timeout == 0) {
        driver->recv_packet(stream, session->get_ssl_stream(), &tmp_packet,
                            head_size);
      } else {
        driver->recv_packet(stream, session->get_ssl_stream(), &tmp_packet,
                            head_size, &wait_timeout_tv);
      }
      MySQLCompress::my_uncompress(&tmp_packet);
      tmp_packet_len = tmp_packet.length();
      if (packet_len + tmp_packet_len > packet_real_len) {
        packet->packdata(tmp_packet.base(), packet_real_len - packet_len);
        size_t data_len = tmp_packet_len - packet_real_len + packet_len;
        const char *start_pos =
            tmp_packet.base() + packet_real_len - packet_len;
        session->store_data_to_uncompress_buffer(data_len, start_pos);
        session->set_packet_from_buff(true);
      } else {
        packet->packdata(tmp_packet.base(), tmp_packet_len);
      }
      packet_len += tmp_packet_len;
      session->add_compress_packet_number();
    } while (packet_real_len > packet_len);

    char *end = packet->wr_ptr();
    packet->packchar('\0');
    packet->wr_ptr(end);
  }
}

void MySQLHandler::receive_from_client_swap(Packet *packet) {
  packet->rewind();

  size_t recv_len = packet->size();
#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = get_session()->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "receive_error") &&
      !strcasecmp(test_info->test_case_operation.c_str(),
                  "receive_client_slow")) {
    recv_len = 1;
  }
#endif

  Packet *client_buffer_packet = session->get_client_buffer_packet();
  if (client_buffer_packet->length() != 0) {
    // there have buffer client packet
    if (client_buffer_packet->length() < PACKET_HEADER_SIZE) {
      packet->packdata(client_buffer_packet->rd_ptr(),
                       client_buffer_packet->length());
      client_buffer_packet->rewind();
      client_buffer_packet->size(default_receive_packet_size);
    } else {
      packet->packdata(client_buffer_packet->rd_ptr(), PACKET_HEADER_SIZE);
      client_buffer_packet->rd_ptr(client_buffer_packet->rd_ptr() +
                                   PACKET_HEADER_SIZE);
      char *pos = packet->base();
      size_t load_len = Packet::unpack3uint(pos);
      if (load_len < client_buffer_packet->length()) {
        packet->packdata(client_buffer_packet->rd_ptr(), load_len);
        client_buffer_packet->rd_ptr(client_buffer_packet->rd_ptr() + load_len);
      } else {
        packet->packdata(client_buffer_packet->rd_ptr(),
                         client_buffer_packet->length());
        size_t leave_len = load_len - client_buffer_packet->length();
        client_buffer_packet->rewind();
        client_buffer_packet->size(default_receive_packet_size);
        if (leave_len != 0) {
          driver->recv_packet_without_header(stream, session->get_ssl_stream(),
                                             packet, leave_len);
        }
      }
    }
  } else {
    driver->recv_packet(stream, session->get_ssl_stream(), recv_len, packet);
  }

  if (packet->length() < PACKET_HEADER_SIZE) {
    size_t unreceive_header_len = PACKET_HEADER_SIZE - packet->length();
    packet->rd_ptr(packet->wr_ptr());
    LOG_INFO("Receive from client [%d] slowly.\n", stream->get_handle());
    driver->recv_packet_without_header(stream, session->get_ssl_stream(),
                                       packet, unreceive_header_len);
    packet->rd_ptr(packet->base());
  }
  uint8_t number = driver->get_packet_number(packet);
  char *pos = packet->base();
  size_t load_len = Packet::unpack3uint(pos);
  // if default length cannot load all data
  // then use block recv_n to next data
  if (load_len > packet->length() - PACKET_HEADER_SIZE) {
    size_t length = packet->length();
    packet->size(load_len + PACKET_HEADER_SIZE + 1);
    packet->rd_ptr(packet->base() + length);
    packet->wr_ptr(packet->base() + length);
    driver->recv_packet_without_header(stream, session->get_ssl_stream(),
                                       packet,
                                       load_len + PACKET_HEADER_SIZE - length);
  }
  packet->rd_ptr(packet->base());
  // receive more than 1 packet
  if (load_len < packet->length() - PACKET_HEADER_SIZE) {
    size_t leave_len = packet->length() - PACKET_HEADER_SIZE - load_len;
    if (client_buffer_packet->length() == 0) {
      client_buffer_packet->rewind();
      client_buffer_packet->size(default_receive_packet_size);
    }
    if (leave_len > client_buffer_packet->size()) {
      client_buffer_packet->size(leave_len * 2);
    } else if (leave_len >
               client_buffer_packet->size() - client_buffer_packet->length()) {
      client_buffer_packet->size(client_buffer_packet->size() * 2);
    }
    client_buffer_packet->packdata(
        packet->rd_ptr() + PACKET_HEADER_SIZE + load_len, leave_len);
    packet->wr_ptr(packet->base() + PACKET_HEADER_SIZE + load_len);
  }
  char *end = packet->wr_ptr();
  packet->packchar('\0');
  packet->wr_ptr(end);
  session->check_packet_number(session->get_packet_number(), number);
  session->add_packet_number();

  size_t length = packet->length();
  while (length == MAX_PACKET_SIZE + PACKET_HEADER_SIZE) {
    LOG_DEBUG("Receive more than 16M packet, need to receive more.\n");
    Packet next_packet;
    driver->recv_packet(stream, session->get_ssl_stream(), &next_packet,
                        PACKET_HEADER_SIZE);
    number = driver->get_packet_number(&next_packet);
    session->check_packet_number(session->get_packet_number(), number);
    session->add_packet_number();
    if (packet->size() < packet->length() + next_packet.length()) {
      packet->size(packet->size() * 2);
    }
    packet->packdata(next_packet.base() + PACKET_HEADER_SIZE,
                     next_packet.length() - PACKET_HEADER_SIZE);
    length = next_packet.length();
  }
}

void MySQLHandler::wait_socket_read(InternalEventBase *internal_base) {
  session->set_client_can_read(false);
  unsigned int count = 0;
  unsigned int base_timeout = INTER_BASE_TIMEOUT;
  if (get_session()->get_load_data_local_command()) {
    base_timeout = 1;
  }
  unsigned int max_timeout = socket_base_timeout * 1000;
  max_timeout = max_timeout > 1000000 ? max_timeout : 1000000;
  while (1) {
    if (internal_base->handle_event_loop(base_timeout)) {
      /* The possible errno is:
       *
       EBADF  epfd is not a valid file descriptor.

       EFAULT The memory area pointed to by events is not accessible with
              write permissions.

       EINVAL epfd is not an epoll file descriptor, or maxevents is less
              than or equal to zero.
       * */
      LOG_ERROR(
          "MySQLHandler::wait_socket_read get fail %d in handle_event_loop for "
          "session %@.\n",
          (int)errno, session);
      throw HandlerQuit();
    }
    if (session->get_client_can_read()) break;

    if (Backend::instance()->is_exiting()) {
      LOG_INFO("Quit client connection because dbscale is exiting.\n");
      throw HandlerQuit();
    }
    if (be_killed()) {
      LOG_INFO("Handler %@ is killed.\n", this);
      throw ThreadIsKilled();
    }
    struct event *e = get_session()->get_internal_event();
    int pending = event_pending(e, EV_READ | EV_TIMEOUT, NULL);
    if (!pending) {
      LOG_INFO("Find the event un-pending, add it to base again.\n");
      int rc = event_add(e, NULL);
      if (rc != 0) {
        LOG_ERROR("Fail to add event again for event_pending false.\n");
        get_session()->set_client_can_read(true);
        // TODO: if woo many frquency short connections, event_add may fail
        break;
      }
    }
    count++;
    if (count == 10) {
      if (base_timeout * 2 < max_timeout) {
        base_timeout *= 2;
      } else {
        base_timeout = max_timeout;
      }
      if (handler_wait_timeout()) {
        LOG_INFO("Quit client connection because wait timeout.\n");
        throw HandlerQuit();
      }
      if (handler_login_timeout()) {
        LOG_DEBUG("Quit client connection because login timeout.\n");
        throw HandlerQuit();
      }
      count = 0;
    }
  }
}

void MySQLHandler::wait_client_read() {
  if (get_session()->get_load_data_local_command())
    return;  // load do not wait_clean_read to avoid the libevent performance
             // problem

  uint32_t client_swap_timeout = wait_timeout;
  if (wait_timeout > dbscale_exiting_timeout) {
    client_swap_timeout = wait_timeout - dbscale_exiting_timeout;
  } else {
    client_swap_timeout = 0;
  }
  struct timeval client_tv = {client_swap_timeout, 0};

  if (client_tv.tv_sec != 0) {
    LOG_DEBUG(
        "Print sec is %d wait_timeout is %d dbscale_exiting_timeout is %d "
        "swap_timeout is %d.\n",
        client_tv.tv_sec, wait_timeout, dbscale_exiting_timeout,
        client_swap_timeout);
  }

  InternalEventBase *internal_base = get_session()->get_internal_base();
  struct event *e = get_session()->get_internal_event();
  int rc = 0;
  /*
  if (client_tv.tv_sec != 0) {
    rc = event_add(e, &client_tv);
  } else {
    rc = event_add(e, NULL);
  }
  */
  rc = event_add(e, NULL);
  if (rc != 0) {
    get_session()->set_client_can_read(true);
    LOG_INFO("fail to event_add for handler %@\n", this);
    // TODO: if woo many frquency short connections, event_add may fail
    return;
  }

  wait_socket_read(internal_base);
}

void MySQLHandler::receive_from_client_no_swap(Packet *packet) {
  packet->rewind();
  if (!get_session()->get_load_data_local_command()) {
    try {
      driver->recv_packet(stream, session->get_ssl_stream(), PACKET_HEADER_SIZE,
                          packet, &exiting_timeout_tv);
    } catch (SockError &e) {
      if (e.get_errno() != ETIME) {
        throw e;
      }
    } catch (...) {
      throw;
    }
  }
  if (packet->length() != 0) {
    if (packet->length() < PACKET_HEADER_SIZE) {
      size_t unreceive_header_len = PACKET_HEADER_SIZE - packet->length();
      packet->rd_ptr(packet->wr_ptr());
      driver->recv_packet_without_header(stream, session->get_ssl_stream(),
                                         packet, unreceive_header_len);
    }
    char *pos = packet->base();
    size_t load_len = Packet::unpack3uint(pos);
    size_t length = packet->length();
    packet->rd_ptr(packet->wr_ptr());
    if (load_len > packet->size() - PACKET_HEADER_SIZE - 1) {
      packet->size(load_len + PACKET_HEADER_SIZE + 1);
      packet->rd_ptr(packet->base() + length);
      packet->wr_ptr(packet->base() + length);
    }
    if (load_len > 0)
      driver->recv_packet_without_header(stream, session->get_ssl_stream(),
                                         packet, load_len);
    char *end = packet->wr_ptr();
    packet->packchar('\0');
    packet->wr_ptr(end);
    packet->rd_ptr(packet->base());
  } else {
    if (!session->get_do_swap_route() &&
        !(session->get_ssl_stream() &&
          ((MySQLSession *)session)->get_packet_number() > 0)) {
      // do not wait client if it is not the first packet from client in ssl
      // mode
      wait_client_read();
    }
    if (wait_timeout == 0) {
      driver->recv_packet(stream, session->get_ssl_stream(), packet,
                          PACKET_HEADER_SIZE);
    } else {
      driver->recv_packet(stream, session->get_ssl_stream(), packet,
                          PACKET_HEADER_SIZE, &wait_timeout_tv);
    }
  }
  uint8_t number = driver->get_packet_number(packet);
  session->check_packet_number(session->get_packet_number(), number);
  session->add_packet_number();

  size_t length = packet->length();
  while (length == MAX_PACKET_SIZE + PACKET_HEADER_SIZE) {
    LOG_DEBUG("Receive more than 16M packet, need to receive more.\n");
    Packet next_packet;
    if (wait_timeout == 0) {
      driver->recv_packet(stream, session->get_ssl_stream(), &next_packet,
                          PACKET_HEADER_SIZE);
    } else {
      driver->recv_packet(stream, session->get_ssl_stream(), &next_packet,
                          PACKET_HEADER_SIZE, &wait_timeout_tv);
    }
    number = driver->get_packet_number(&next_packet);
    session->check_packet_number(session->get_packet_number(), number);
    session->add_packet_number();
    if (packet->size() < packet->length() + next_packet.length()) {
      packet->size(packet->size() * 2);
    }
    packet->packdata(next_packet.base() + PACKET_HEADER_SIZE,
                     next_packet.length() - PACKET_HEADER_SIZE);
    length = next_packet.length();
  }
}

void MySQLHandler::do_receive_from_client(Packet *packet) {
  bool compress = get_session()->get_compress();
  if (!compress) {
    if (enable_session_swap && get_session()->get_do_swap_route() &&
        !get_session()->get_load_data_local_command()) {
      get_session()->set_do_swap_route(false);
      return receive_from_client_swap(packet);
    } else {
      return receive_from_client_no_swap(packet);
    }
  } else {
    return receive_from_client_compress(packet);
  }
}

void MySQLHandler::do_send_to_client(Packet *packet,
                                     bool need_set_packet_number,
                                     bool is_buffer_packet) {
  if (!is_buffer_packet &&
      method_to_handle_autocommit_flag == HANDLE_AUTOCOMMIT_FLAG_IN_SEND)
    set_auto_commit_off_if_necessary(packet);
  if (driver->is_ok_packet(packet) && session->should_save_postponed_packet()) {
    session->save_postponed_ok_packet(packet, need_set_packet_number);
    return;
  }
  if (need_set_packet_number) {
    driver->set_packet_number(packet, session->get_packet_number());
    session->add_packet_number();
  }
  if (driver->is_ok_packet(packet) && session->get_top_stmt() != NULL &&
      session->get_top_stmt()->get_stmt_node()->type == STMT_UPDATE) {
    int matched_or_change_count = 0;
    MySQLOKResponse res(packet);
    res.unpack();
    if (session->get_client_capabilities() & CLIENT_FOUND_ROWS) {
      matched_or_change_count =
          session->get_matched_or_change_message("matched:", res.get_message());
    } else {
      matched_or_change_count =
          session->get_matched_or_change_message("Changed:", res.get_message());
    }
    if (matched_or_change_count != -1) {
      res.set_affected_rows(matched_or_change_count);
      res.pack(packet);
    }
    LOG_DEBUG(
        "Get Message [%s] matched or change count [%d] affacted rows[%d]\n",
        res.get_message().c_str(), matched_or_change_count,
        res.get_affected_rows());
  }
  bool compress = get_session()->get_compress();
  if (compress) {
    packet = MySQLCompress::my_compress(packet);
    driver->set_packet_number(packet, session->get_compress_packet_number());
    session->add_compress_packet_number();
    driver->send_packet_compress(stream, session->get_ssl_stream(), packet);
    delete packet;
  } else
    driver->send_packet(stream, session->get_ssl_stream(), packet);
}

void MySQLHandler::do_receive_from_server(Connection *conn, Packet *packet,
                                          const TimeValue *timeout) {
#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = get_session()->get_dbscale_test_info();
  if (!strcasecmp(test_info->test_case_name.c_str(), "receive_error") &&
      !strcasecmp(test_info->test_case_operation.c_str(),
                  "receive_server_slow")) {
    test_receive_server_slow = true;
  } else {
    test_receive_server_slow = false;
  }
  if (!strcasecmp(test_info->test_case_name.c_str(), "receive_error") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "receive_failed")) {
    test_receive_data_failed = true;
  } else {
    test_receive_data_failed = false;
  }
#endif
  conn->recv_packet(packet, timeout);
  if (net_status == 1) {
    session->set_net_out(packet->length());
  }

  if (driver->is_error_packet(packet)) {
    get_session()->get_status()->item_inc(TIMES_ERROR_PACKET_FROM_SERVER);
  }
}

void MySQLHandler::send_mysql_packet_to_client_by_buffer(Packet *packet,
                                                         bool is_row_packet) {
  if (!is_row_packet &&
      method_to_handle_autocommit_flag == HANDLE_AUTOCOMMIT_FLAG_IN_SEND)
    set_auto_commit_off_if_necessary(packet);
  if (!net_buffer) {
    send_to_client(packet);
    return;
  }

  driver->set_packet_number(packet, session->get_packet_number());
  session->add_packet_number();
  send_to_client_by_buffer(packet);
}

void MySQLHandler::do_send_to_server(Connection *conn, Packet *packet) {
  if (conn->is_odbconn() == false) {
    MySQLConnection *mysql_conn = (MySQLConnection *)conn;
    mysql_conn->set_is_query_command(session->get_is_query_command());

    if (packet->length() < MAX_PACKET_SIZE + PACKET_HEADER_SIZE) {
      conn->send_packet(packet);
      return;
    }

    size_t left_length = packet->length() - PACKET_HEADER_SIZE;
    uint8_t num = driver->get_packet_number(packet);
    const char *pos = packet->base() + PACKET_HEADER_SIZE;
    while (left_length >= MAX_PACKET_SIZE + PACKET_HEADER_SIZE) {
      LOG_DEBUG("Packet more than 16M, need to split the packet.\n");
      Packet next_packet(MAX_PACKET_SIZE + PACKET_HEADER_SIZE + 1);
      next_packet.pack3int(MAX_PACKET_SIZE);
      next_packet.packchar(num++);
      next_packet.packdata(pos, MAX_PACKET_SIZE);
      pos += MAX_PACKET_SIZE;
      left_length -= MAX_PACKET_SIZE;
      conn->send_packet(&next_packet);
    }

    Packet last_packet(MAX_PACKET_SIZE + PACKET_HEADER_SIZE + 1);
    last_packet.pack3int(left_length);
    last_packet.packchar(num);
    last_packet.packdata(pos, left_length);
    conn->send_packet(&last_packet);
  } else {
    ODBCConnection *odbc_conn = (ODBCConnection *)conn;
    odbc_conn->handle_mysql_packet(packet);
  }
}

Packet *MySQLHandler::do_get_error_packet(uint16_t error_code,
                                          const char *message,
                                          const char *sql_state) {
  MySQLErrorResponse err(error_code, message, sql_state);
  Packet *err_packet = Backend::instance()->get_new_packet();
  err.pack(err_packet);
  return err_packet;
}

inline void deal_ddl_for_consistence_point(Session *session, stmt_type type,
                                           bool is_before_execute) {
  if (session->get_affected_server_type() == AFFETCED_MUL_SERVER) {
    if (type == STMT_TRUNCATE || type == STMT_ALTER_TABLE ||
        type == STMT_CREATE_TB || type == STMT_DROP_DB ||
        type == STMT_ALTER_DB || type == STMT_DROP_TB ||
        type == STMT_CREATE_DB || type == STMT_LOCK_TB ||
        type == STMT_RENAME_TABLE) {
      if (is_before_execute) {
        session->start_commit_consistence_transaction();
      } else {
        session->end_commit_consistence_transaction();
      }
    }
  }
}

inline bool is_drop_current_database(stmt_type type, MySQLStatement *stmt,
                                     const char *cur_schema) {
#ifdef DEBUG
  ACE_ASSERT(stmt);
#endif
  if (type == STMT_DROP_DB && stmt->get_stmt_node()->sql->drop_db_oper &&
      cur_schema &&
      !lower_case_compare(
          stmt->get_stmt_node()->sql->drop_db_oper->dbname->name, cur_schema)) {
    return true;
  }
  return false;
}

void MySQLHandler::do_rebuild_query_merge_sql(Packet *packet, DataSpace *space,
                                              string &sql, Packet *new_packet) {
  const char *ori_sql = driver->get_query(packet);
  string new_sql = sql;
  if (get_session()->has_bridge_mark_sql() &&
      get_session()->get_session_option("is_bridge_session").bool_val) {
    string bridge_mark_sql = get_session()->get_bridge_mark_sql();
    get_session()->adjust_retl_mark_tb_name(space, bridge_mark_sql);
    new_sql.append(bridge_mark_sql).append(";");
  }
  list<char *> savepoint_list = get_session()->get_all_savepoint();
  if (!savepoint_list.empty()) {
    list<char *>::iterator it2 = savepoint_list.begin();
    for (; it2 != savepoint_list.end(); ++it2) {
      new_sql.append("SAVEPOINT ");
      new_sql.append(*it2);
      new_sql.append(";");
    }
  }
  new_sql.append(ori_sql);
#ifdef DEBUG
  LOG_DEBUG("Merge begin sql is: %s\n", new_sql.c_str());
#endif
  MySQLQueryRequest query(new_sql.c_str());
  query.set_sql_replace_char(session->get_query_sql_replace_null_char());
  if (new_packet)
    query.pack(new_packet);
  else
    query.pack(packet);
}

void MySQLHandler::do_handle_server_result() {
#ifdef DEBUG
  ACE_ASSERT(session);
  ACE_ASSERT(session->get_session_state() == SESSION_STATE_HANDLING_RESULT);
#endif
  MySQLExecutePlan *plan = (MySQLExecutePlan *)(session->get_top_plan());
  plan->handler = this;
  MySQLStatement *stmt = (MySQLStatement *)(session->get_top_stmt());
  stmt_type type = stmt->get_stmt_node()->type;
  try {
    try {
      try {
        plan->execute();
        if (get_session()->need_swap_for_server_waiting()) {
#ifdef DEBUG
          LOG_DEBUG(
              "Handler %@ stop processing request due to the session %@ is "
              "waiting for server.\n",
              this, get_session());
#endif
          return;
        }
#ifndef DBSCALE_TEST_DISABLE
        after_handle_recv.start_timing();
#endif
        if (enable_record_transaction_sqls &&
            session->check_for_transaction() &&
            (type != STMT_ROLLBACK && type != STMT_COMMIT)) {
          session->add_transaction_sql(stmt->get_sql());
        }
      } catch (...) {
        if (session->get_audit_log_flag_local() != AUDIT_LOG_MODE_MASK_NON) {
          end_date = ACE_OS::gettimeofday();
          record_audit_log(type);
        }
        deal_ddl_for_consistence_point(session, type, false);
        throw;
      }
      if (session->get_transfer_rule()) {
        session->get_transfer_rule()->close();
        session->set_transfer_rule(NULL);
        session->reinit_follower_session();
      }
      if (!plan->get_is_forward_plan() && type != STMT_SELECT &&
          type != STMT_UPDATE && type != STMT_DELETE && type != STMT_INSERT) {
#ifndef DBSCALE_TEST_DISABLE
        LOG_INFO("deal_with_metadata_final for sql %s.\n", stmt->get_sql());
#endif
        const char *schema_name_tmp = session->get_schema();
        if (type == STMT_CREATE_TB || type == STMT_DROP_TB) {
          stmt_node *st = stmt->get_stmt_node();
          record_scan *rs_tmp = st->scanner;
          table_link *tl = rs_tmp->first_table;
          schema_name_tmp = tl->join->schema_name ? tl->join->schema_name
                                                  : session->get_schema();
        }
        deal_with_metadata_final(type, stmt->get_sql(), schema_name_tmp,
                                 stmt->get_stmt_node());
      }
      if (enable_info_schema_mirror_table > 0 && datasource_in_one &&
          (type > STMT_DDL_START) && (type < STMT_DDL_END) &&
          type != STMT_TRUNCATE) {
        if (type == STMT_ALTER_TABLE || type == STMT_CREATE_TB) {
          table_link *tl = stmt->get_stmt_node()->scanner->first_table;
          const char *schema_name = tl->join->schema_name
                                        ? tl->join->schema_name
                                        : session->get_schema();
          const char *table_name = tl->join->table_name;
          Backend::instance()->set_informationschema_mirror_tb_out_of_date(
              true, schema_name, table_name);
        } else if (type == STMT_DROP_TB) {
          table_link *table = stmt->get_stmt_node()->table_list_head;
          while (table) {
            const char *schema_name = table->join->schema_name
                                          ? table->join->schema_name
                                          : session->get_schema();
            const char *table_name = table->join->table_name;
            Backend::instance()->set_informationschema_mirror_tb_out_of_date(
                true, schema_name, table_name);
            table = table->next;
          }
        } else if (type == STMT_CREATE_DB) {
          const char *schema_name =
              stmt->get_stmt_node()->sql->create_db_oper->dbname->name;
          Backend::instance()->set_informationschema_mirror_tb_out_of_date(
              true, schema_name, NULL);
        } else if (type == STMT_DROP_DB) {
          const char *schema_name =
              stmt->get_stmt_node()->sql->drop_db_oper->dbname->name;
          Backend::instance()->set_informationschema_mirror_tb_out_of_date(
              true, schema_name, NULL);
        } else {
          Backend::instance()->set_informationschema_mirror_tb_out_of_date(
              true, NULL, NULL);
        }
      }
      if (session->get_slow_query_time_local() > 0 ||
          session->get_audit_log_flag_local() != AUDIT_LOG_MODE_MASK_NON) {
        end_date = ACE_OS::gettimeofday();
        record_audit_log(type);
        check_and_record_slow_query_time();
      }
      if (session->is_drop_current_schema()) session->set_schema(NULL);

      deal_ddl_for_consistence_point(session, type, false);

      for (size_t i = 0; i < session->table_info_to_delete_pair_vec.size();
           i++) {
        driver->erase_lru_cache(
            session->table_info_to_delete_pair_vec[i].first,
            session->table_info_to_delete_pair_vec[i].second);
      }

      session->apply_table_info_to_delete();
      if (type == STMT_CALL)
        ((MySQLSession *)get_session())->reset_all_nest_sp_counter();
    } catch (...) {
      if (session->get_transfer_rule()) {
        session->get_transfer_rule()->close();
        session->set_transfer_rule(NULL);
        session->reinit_follower_session();
      }

      if (type == STMT_CREATE_PROCEDURE) {
        string sp_name = stmt->get_current_sp_worker_name();
        session->remove_sp_worker(sp_name);
      }
      if (be_killed()) {
        LOG_DEBUG("Thread %d is killed during plan execution.\n",
                  get_thread_id());
        throw ThreadIsKilled();
      } else
        throw;
    }
  } catch (ThreadIsKilled &e) {
    session->clear_query_sql();
    session->reset_top_stmt_and_plan();
    throw;
  } catch (NestStatementErrorException &e) {
    if (stmt->is_handle_sub_query())
      LOG_DEBUG("Get separated exec subquery fail error for SQL [%s]\n",
                stmt->get_sql());
    if (session->is_call_store_procedure())
      LOG_DEBUG("Get routine sub stmt exec fail error.\n");
    // Nothing need to be down, just go through the normal process to clean
  } catch (CrossNodeJoinFail &e) {
    LOG_ERROR("Cross node join failed for SQL [%s]\n", stmt->get_sql());
  } catch (ServerShutDown &e) {
    LOG_DEBUG("%s\n", e.what());
    session->reset_top_stmt_and_plan();
    throw e;
  } catch (SQLException &e) {
    handle_exception(&e, true, e.get_errno(), e.what(), e.get_sqlstate());
  } catch (SilenceOKStmtFail &e) {
    throw e;
  } catch (Exception &e) {
    handle_exception(&e, true, e.get_errno(), e.what());
  } catch (exception &e) {
    LOG_ERROR("Get exception %s during plan execution.\n", e.what());
    session->reset_top_stmt_and_plan();
    throw e;
  }
  if (session->is_binary_resultset()) {
    session->set_binary_packet(NULL);
    session->set_binary_resultset(false);
  }
  session->clear_query_sql();
  session->reset_top_stmt_and_plan();
#ifndef DBSCALE_TEST_DISABLE
  after_handle_recv.end_timing_and_report();
#endif
}

bool stmt_is_unsafe_redo_sql(stmt_node *st) {
  if (st->is_redo_unsafe) return true;

  switch (st->type) {
    case STMT_UPDATE:
    case STMT_DELETE:
    case STMT_INSERT:
    case STMT_REPLACE:
    case STMT_REPLACE_SELECT:
    case STMT_INSERT_SELECT: {
      if (st->var_item_list || st->session_var_list || st->has_global_var)
        return true;
      return false;
    }
    default: {
      break;
    }
  }
  return false;
}

void count_session_sql_op(Session *session, stmt_type type) {
  switch (type) {
    case STMT_SELECT: {
      session->get_status()->item_inc(TIMES_SELECT);
      session->add_select();
      break;
    }
    case STMT_INSERT:
    case STMT_REPLACE: {
      session->get_status()->item_inc(TIMES_INSERT);
      session->add_insert();
      break;
    }
    case STMT_UPDATE: {
      session->get_status()->item_inc(TIMES_UPDATE);
      session->add_update();
      break;
    }
    case STMT_DELETE: {
      session->get_status()->item_inc(TIMES_DELETE);
      session->add_delete();
      break;
    }
    case STMT_INSERT_SELECT:
    case STMT_REPLACE_SELECT: {
      session->get_status()->item_inc(TIMES_INSERT_SELECT);
      session->add_insert_select();
      break;
    }
    default: {
      break;
    }
  }
}

void MySQLHandler::record_audit_login(uint32_t thread_id, string user,
                                      string host, string ip,
                                      const char *schema, int error_code) {
#ifdef DBSCALE_TEST_DISABLE
  // In release for client mode, the login audit should only be done if the
  // do-audit-log is not empty
  unsigned int audit_log_flag_local = session->get_audit_log_flag_local();
  if (audit_log_flag_local == AUDIT_LOG_MODE_MASK_NON) return;
#endif
  if (!Backend::instance()->is_audit_user_set_contain(user)) return;
  int64_t record_id_head = Backend::instance()->get_audit_log_record_id();
  AuditLogTool *audit_log_tool = Backend::instance()->get_audit_log_tool();
  audit_log_tool->add_login_info_to_buff(record_id_head, thread_id, user, host,
                                         ip, schema, error_code);
}

bool is_stmt_type_need_audit_record(stmt_type type,
                                    unsigned int audit_log_flag_local) {
  return ((type > STMT_DDL_START) && (type < STMT_DDL_END) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_DDL)) ||
         ((type == STMT_SELECT) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_SELECT)) ||
         ((type == STMT_INSERT) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_INSERT)) ||
         ((type == STMT_REPLACE) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_REPLACE)) ||
         ((type == STMT_UPDATE) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_UPDATE)) ||
         ((type == STMT_DELETE) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_DELETE)) ||
         ((type == STMT_LOAD) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_LOAD_DATA)) ||
         ((type == STMT_REPLACE_SELECT) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_REPLACE_SELECT)) ||
         ((type == STMT_INSERT_SELECT) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_INSERT_SELECT)) ||
         ((type == STMT_CREATE_USER) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_CREATE_USER)) ||
         ((type == STMT_DROP_USER) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_DROP_USER)) ||
         (((type == STMT_LOCK_TB) || (type == STMT_UNLOCK_TB)) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_LOCK_TB)) ||
         ((type == STMT_KILL_THREAD) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_KILL_THREAD)) ||
         ((type == STMT_GRANT) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_GRANT)) ||
         ((type == STMT_REVOKE) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_REVOKE)) ||
         (((type == STMT_FLUSH) || (type == STMT_FLUSH_TABLE_WITH_READ_LOCK) ||
           (type == STMT_FLUSH_PRIVILEGES)) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_FLUSH)) ||
         ((type == STMT_SET_PASSWORD) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_SET_PASSWORD)) ||
         ((type > DBSCALE_ADMIN_COMMAND_START) &&
          (type < DBSCALE_ADMIN_COMMAND_END) &&
          (audit_log_flag_local & AUDIT_LOG_MODE_MASK_DBSCALE_CMD)) ||
         (type == STMT_QUIT) || (type == STMT_NON);
}

void MySQLHandler::record_audit_log(stmt_type type, bool force) {
  if (!force && session->get_has_do_audit_log()) return;
  session->set_has_do_audit_log(true);
  unsigned int audit_log_flag_local = session->get_audit_log_flag_local();
  if (audit_log_flag_local == AUDIT_LOG_MODE_MASK_NON) return;
  if (!Backend::instance()->is_audit_user_set_contain(
          session->get_user_name_ref()))
    return;
  if (is_stmt_type_need_audit_record(type, audit_log_flag_local)) {
    int64_t record_id_head = Backend::instance()->get_audit_log_record_id();
    AuditLogTool *audit_log_tool = Backend::instance()->get_audit_log_tool();
    audit_log_tool->add_log_info_to_buff(record_id_head, get_session(), type);
  }
}

void MySQLHandler::handle_query_command(Packet *packet) {
  const char *query_sql = driver->get_query(packet);
  size_t cmd_len = Packet::unpack3uint(packet->base());  // contain the tailed
                                                         // \0
  cmd_len -= 1;
  try {
    int ret;
    if (sql_attach_outline_hint(query_sql)) {
      ret = handle_query_command(cur_hint_sql.c_str(), cur_hint_sql.length());
    } else {
      ret = handle_query_command(query_sql, cmd_len);
    }
    if (enable_monitor_point_flag) {
      if (ret == 0 && !session->get_cur_stmt_execute_fail() &&
          sql_statistic.is_should_record() &&
          !sql_statistic.is_record_as_error()) {
        sql_statistic.end_timing(monitor_stage::SUCCEED);
      } else if (sql_statistic.is_should_record() &&
                 (session->get_cur_stmt_execute_fail() ||
                  sql_statistic.is_record_as_error())) {
        sql_statistic.end_timing(monitor_stage::FAILED);
      }
      sql_statistic.reset();
    }
  } catch (...) {
    if (enable_monitor_point_flag) {
      sql_statistic.end_timing(monitor_stage::FAILED);
      sql_statistic.reset();
    }
    throw;
  }
}

int MySQLHandler::handle_query_command(const char *query_sql, size_t cmd_len) {
  bool need_monitor_local = sql_statistic.need_monitor();
  if (enable_acl) {
    if (session->is_user_not_allow_operation_now()) {
      string times;
      session->get_user_not_allow_operation_now_str(times);
      string msg = "you are forbidden to operate this cluster during ";
      msg = msg + times;
      throw Error(msg.c_str());
    }
  }
  if (session->get_max_query_count_per_flow_control_gap() &&
      strcmp(supreme_admin_user, session->get_username()) &&
      strcmp(normal_admin_user, session->get_username()) &&
      strcmp(dbscale_internal_user, session->get_username())) {
    if (!session->query_command_flow_rate_control_ok()) {
      dbscale::sql::SQLError error(dbscale_err_msg[ERROR_FLOW_CONTROL_CODE], "",
                                   ERROR_FLOW_CONTROL_CODE);
      handle_exception(&error, true, error.get_errno(), error.what());
      return -1;
    }
  }
  MySQLParser *parser = MySQLParser::instance();
  MySQLStatement *stmt = NULL;

  Backend *backend = Backend::instance();
  if (backend->get_refuse_modify_table_version() >
      session->get_refuse_modify_table_version()) {
    session->set_refuse_version(backend->get_refuse_modify_table_version());
    set<string> backend_refuse_modify_table;
    backend->get_refuse_modify_table(backend_refuse_modify_table);
    session->set_refuse_modify_table(backend_refuse_modify_table);
  }
  if (enable_monitor_point_flag && need_monitor_local) {
    sql_statistic.start_timing();
  }
#ifndef DBSCALE_TEST_DISABLE
  parse_sql.start_timing();
#endif

  size_t query_sql_str_len = strlen(query_sql);
  if (query_sql_str_len <= 2048) {
    LOG_DEBUG("Command query: %s\n", query_sql);
  } else {
    for (size_t i = 0; i < query_sql_str_len;) {
      size_t cur_len =
          (query_sql_str_len - i) <= 2048 ? (query_sql_str_len - i) : 2048;
      string tmp = string(query_sql + i, cur_len);
      LOG_DEBUG("Command query: %s\n", tmp.c_str());
      i += cur_len;
    }
  }

  get_session()->add_session_sql_cache(query_sql);
  session->set_query_sql(query_sql, query_sql_str_len);
  session->record_query_time();

  session->set_query_sql_replace_null_char("");
  if (cmd_len > query_sql_str_len) {
    bool need_to_handle_diff_len = false;
    for (size_t p = query_sql_str_len; p < cmd_len; p++) {
      if (query_sql[p] != 0 && query_sql[p] != ' ' && query_sql[p] != '') {
        need_to_handle_diff_len = true;
        break;
      }
    }
    if (need_to_handle_diff_len) {
      // characters that can use to replace NULL value
      string query_sql_replace_null(query_sql, cmd_len);
      set<char> dbscale_replace_null_char_set_session =
          Backend::instance()->get_dbscale_replace_null_char_set();
      session->reset_query_sql_null_pos();
      size_t null_count = 0;
      for (size_t i = 0; i < cmd_len; i++) {
        if (query_sql[i] == 0) {
          session->add_query_sql_null_pos(i);
          null_count++;
        }
        if (dbscale_replace_null_char_set_session.count(query_sql[i]))
          dbscale_replace_null_char_set_session.erase(query_sql[i]);
      }
      if (!dbscale_replace_null_char_set_session.empty()) {
        char replace_null_char =
            *(dbscale_replace_null_char_set_session.begin());
        session->set_query_sql_replace_null_char(string(1, replace_null_char));
        vector<size_t> query_sql_null_pos = session->get_query_sql_null_pos();
        for (size_t i = 0; i < null_count; i++) {
          query_sql_replace_null[query_sql_null_pos[i]] = replace_null_char;
        }
        LOG_DEBUG(
            "NUL replace char is [%c], query sql with NUL character is "
            "modified to [%s]\n",
            replace_null_char, query_sql_replace_null.c_str());
      } else {
        // single-char is not enough, try multi-char
        const char *dbscale_replace_null_char_candidate =
            DBSCALE_REPLACE_NULL_CHAR;
        size_t len = strlen(dbscale_replace_null_char_candidate);
        size_t max_candiate_len = 10;
        bool found_candidate = false;
        for (size_t m = 2; m <= max_candiate_len; m++) {
          for (size_t i = 0; i < len; i++) {
            string candidate = string(
                m, dbscale_replace_null_char_candidate[i]);  // TODO build more
                                                             // candiates
            if (query_sql_replace_null.find(candidate) == std::string::npos) {
              found_candidate = true;
              session->set_query_sql_replace_null_char(candidate);
              vector<size_t> query_sql_null_pos =
                  session->get_query_sql_null_pos();
              for (size_t j = 0; j < null_count; j++) {
                query_sql_replace_null.replace(
                    query_sql_null_pos[j] + j * (m - 1), 1, candidate);
              }
              LOG_DEBUG(
                  "NUL replace char is multi-char [%s], query sql with NUL "
                  "character is modified to [%s]\n",
                  candidate.c_str(), query_sql_replace_null.c_str());
              break;
            }
          }
          if (found_candidate) break;
        }
        if (!found_candidate) {
          LOG_ERROR(
              "the sql which contains NUL character can not be handled\n");
          throw Error(
              "the sql which contains NUL character can not be handled");
        }
      }
      session->set_query_sql_replace_null(query_sql_replace_null);
      query_sql = session->get_query_sql_replace_null().c_str();
    }
  }

  CharsetType ctype = session->get_client_charset_type();
  bool session_allow_dot_in_ident =
      session->get_session_option("allow_dot_in_ident").int_val;
  try {
    if (!session->get_top_stmt()) stmt = (MySQLStatement *)session->get_stmt();
    stmt = static_cast<MySQLStatement *>(parser->parse(
        query_sql, session_allow_dot_in_ident, false, session->get_mem_alloc(),
        stmt, session->get_mem_alloc_for_flex(), ctype));
    session->set_top_stmt(stmt);
  } catch (NotSupportedError &e) {
    session->set_stmt(NULL);
    handle_exception(&e, true, e.get_errno(), e.what());
#ifndef DBSCALE_TEST_DISABLE
    parse_sql.end_timing_and_report();
#endif
    return -1;
  } catch (dbscale::sql::SQLError &e) {
    session->set_stmt(NULL);
    handle_exception(&e, true, e.get_errno(), e.what(), e.get_sqlstate());
#ifndef DBSCALE_TEST_DISABLE
    parse_sql.end_timing_and_report();
#endif
    return -1;
  }
#ifndef DBSCALE_TEST_DISABLE
  parse_sql.end_timing_and_report();
#endif
  if (enable_monitor_point_flag && need_monitor_local) {
    sql_statistic.end_timing(monitor_stage::SQL_PARSE);
  }

  if (!stmt) {
    LOG_ERROR("Fail to parse sql!\n");
    throw Error(
        "Meet an unexpected error during sql parsing,"
        " fail to parse sql!");
  }

  if (stmt->is_partial_parsing_without_table_name)
    session->get_status()->item_inc(TIMES_PARTIAL_PARSING_WITHOUT_TABLE_NAME);
  if (stmt->is_partial_parsing_related_to_table) {
    table_link *table = stmt->get_stmt_node()->table_list_head;
    Backend *backend = Backend::instance();
    while (table) {
      const char *schema_name = table->join->schema_name
                                    ? table->join->schema_name
                                    : session->get_schema();
      const char *table_name = table->join->table_name;
      if (backend->table_is_partitioned(schema_name, table_name)) {
        session->get_status()->item_inc(
            TIMES_PARTIAL_PARSING_RELEATED_TO_PARTITION_TABLE);
        break;
      }
      table = table->next;
    }
  }

  if (!session->is_call_store_procedure()) {
    session->reset_call_sp_nest();
    session->reset_sp_error_response_info();
  }

  stmt_node *node = stmt->get_stmt_node();

  bool need_reset_mul_top_stmt = false;
  bool need_reset_session_next_trx_level = false;
  while (node) {
    stmt_node *tmp_node = node;
    node = node->next;
    auto cur_trx_num = (session->get_auto_commit_is_on()
                            ? (session->get_transaction_sqls_num() - 1)
                            : session->get_transaction_sqls_num());
    if (session->get_transaction_sqls_num() && transaction_max_sql_num &&
        cur_trx_num >= transaction_max_sql_num &&
        (tmp_node->type < STMT_START_TRANSACTION ||
         tmp_node->type > STMT_COMMIT) &&
        !session->is_in_cluster_xa_transaction()) {
      if (!trx_timing_stop) {
        end_transaction_timing();
      }
      Error error(
          "The transaction is stopped since executing sqls is greater than "
          "transaction-max-sql-num.",
          ERROR_TRANSACTION_FAILED_CODE);
      session->rollback();
      set_keep_conn(session->is_keeping_connection());
      session->reset_top_stmt_and_plan();
      session->set_stmt(NULL);
      handle_exception(&error, true, error.get_errno(), error.what());
#ifndef DBSCALE_TEST_DISABLE
      parse_sql.end_timing_and_report();
#endif
      return -1;
    }
    // DDL sql should stop trx timing
    if (tmp_node->type > STMT_DDL_START &&
        tmp_node->type <= STMT_ALTER_DROP_PARTITION && !trx_timing_stop) {
      end_transaction_timing();
    }

    if (session->get_slow_query_time_local() > 0 ||
        session->get_audit_log_flag_local() != AUDIT_LOG_MODE_MASK_NON) {
      start_date = ACE_OS::gettimeofday();
      session->set_has_do_audit_log(false);
    }

    if (node && node->type != STMT_NON) {
      session->set_has_more_result(true);
    } else {
      if (session->get_has_more_result() && !session->in_auto_trx())
        session->set_has_more_result(false);
    }

    if ((node && node->type != STMT_NON) || !tmp_node->top_stmt) {
      try {
        stmt = static_cast<MySQLStatement *>(
            parser->parse(tmp_node->stmt_sql, session_allow_dot_in_ident, false,
                          session->get_mem_alloc(), NULL,
                          session->get_mem_alloc_for_flex(), ctype));
        session->set_top_stmt(stmt);
        need_reset_mul_top_stmt = true;
      } catch (NotSupportedError &e) {
        session->reset_top_stmt_and_plan();
        session->set_stmt(NULL);
        handle_exception(&e, true, e.get_errno(), e.what());
#ifndef DBSCALE_TEST_DISABLE
        parse_sql.end_timing_and_report();
#endif
        return -1;
      }
      if (enable_monitor_point_flag && need_monitor_local) {
        sql_statistic.end_timing(monitor_stage::SQL_PARSE);
      }
#ifndef DBSCALE_TEST_DISABLE
      parse_sql.end_timing_and_report();
#endif
      session->set_is_complex_stmt(true);
    }

    session->set_cur_stmt_type(stmt->get_stmt_node()->type);
    session->reset_table_info_to_delete();
    stmt_type type = stmt->get_stmt_node()->type;
#ifdef DEBUG
    LOG_DEBUG("Statement type: %d\n", type);
#endif

    if (enable_monitor_point_flag && need_monitor_local) {
      if (sql_statistic.should_record_query_sql(type)) {
        sql_statistic.set_query_sql(query_sql);
        sql_statistic.set_is_should_record(true);
        record_outline_info(query_sql, query_sql_str_len);
      }
    }
    if (type == STMT_QUIT) {
      send_ok_packet_to_client(this, 0, 0);
      session->reset_top_stmt();
      session->rollback();
      set_keep_conn(session->is_keeping_connection());
      LOG_DEBUG("execute quit\n");
      throw HandlerQuit();
    }
    try {
      if (session->get_is_dbscale_read_only_user() &&
          !is_dbscale_show_stmt(type)) {
        // read_only_user login will send
        // select @@version_comment limit 1 and select USER()
        // read_only_user will ignore it
        if (stmt->check_is_select_version_comment()) {
          send_ok_packet_to_client(this, 0, 0);
          session->reset_top_stmt();
          return -1;
        } else if (type == STMT_SELECT &&
                   !strcasecmp(query_sql, "select USER()")) {
          send_ok_packet_to_client(this, 0, 0);
          session->reset_top_stmt();
          return -1;
        } else {
          LOG_INFO(
              "Statements other than the DBSCALE SHOW cannot be executed under "
              "the dbscale_read_only_user: "
              "[%s]\n",
              query_sql);
          throw NotSupportedError(
              "Statements other than the DBSCALE SHOW cannot be executed under "
              "the dbscale_read_only_user.");
        }
      }

      if (Backend::instance()->need_deal_with_metadata(type,
                                                       stmt->get_stmt_node())) {
        string ddl_sql_with_comment = generate_ddl_comment_sql(stmt->get_sql());
        session->set_ddl_comment_sql(ddl_sql_with_comment);
        LOG_DEBUG("after add comment, the sql is : %s \n",
                  ddl_sql_with_comment.c_str());
        stmt->set_sql(session->get_ddl_comment_sql()->c_str());
      }
      if (stmt->is_partial_parsed() && type == STMT_CREATE_TB &&
          stmt->get_stmt_node()->sql->create_tb_oper->op_tmp != 1) {
        unsigned int table_name_end_pos =
            stmt->get_stmt_node()->table_list_tail->end_pos;
        string query_sql_str = string(query_sql);
        boost::to_lower(query_sql_str);
        size_t select_pos = query_sql_str.find("select ", table_name_end_pos);
        if (select_pos == std::string::npos) {
          select_pos = query_sql_str.find("select*", table_name_end_pos);
        }
        if (select_pos == std::string::npos) {
          select_pos = query_sql_str.find("select`", table_name_end_pos);
        }
        if (select_pos != std::string::npos) {
          LOG_INFO(
              "Unsupport create table statement with select partial parsed: "
              "[%s]\n",
              query_sql);
          throw NotSupportedError(
              "Unsupport create table statement with select partial parsed.");
        }
      }
      if (type != STMT_CREATE_PROCEDURE && type != STMT_CREATE_FUNCTION) {
        if (stmt->is_partial_parsed() &&
            session->get_session_option("enable_multiple_stmt_check").int_val) {
          const char *term_pos = strchr(query_sql, ';');
          if (term_pos && strlen(term_pos) != 1) {
            LOG_ERROR("Not support multiple stmt in partial parse.\n");
            throw NotSupportedError(
                "Not support multiple stmt in partial parse.");
          }
          term_pos = strstr(query_sql, "\\G");
          if (term_pos && strlen(term_pos) != 2) {
            LOG_ERROR("Not support multiple stmt in partial parse.\n");
            throw NotSupportedError(
                "Not support multiple stmt in partial parse.");
          }
          term_pos = strstr(query_sql, "\\g");
          if (term_pos && strlen(term_pos) != 2) {
            LOG_ERROR("Not support multiple stmt in partial parse.\n");
            throw NotSupportedError(
                "Not support multiple stmt in partial parse.");
          }
        }
      }
    } catch (NotSupportedError &e) {
      handle_exception(&e, true, e.get_errno(), e.what());
#ifndef DBSCALE_TEST_DISABLE
      parse_sql.end_timing_and_report();
#endif
      session->reset_top_stmt();
      return -1;
    }
    stmt->set_default_schema(session->get_schema());
    if (session->is_transaction_error()) {
      deal_with_transaction_error(type);
      session->reset_top_stmt();
      return -1;
    }
    if (type != STMT_CREATE_PROCEDURE && type != STMT_CREATE_FUNCTION) {
      if (this->mul_stmt_send_error_packet && tmp_node && tmp_node->stmt_sql &&
          tmp_node->handled_sql &&
          (strcmp(tmp_node->stmt_sql, tmp_node->handled_sql) != 0)) {
        session->reset_top_stmt_and_plan();
        if (!need_reset_mul_top_stmt) node = NULL;
        if (node) {
          session->reset_statement_level_session_variables(false);
        }
        break;
      }
    }

#ifndef DBSCALE_TEST_DISABLE
    if (type == STMT_DROP_TB) {
      table_link *table = stmt->get_stmt_node()->table_list_head;
      if (!strcmp(table->join->table_name, "kill_core_tb")) {
        session->set_is_killed(true);
      }
    }
#endif

    if (type != STMT_SELECT && type != STMT_UPDATE && type != STMT_DELETE &&
        type != STMT_INSERT) {
      if (is_implict_commit_sql(type) &&
          !session->is_in_cluster_xa_transaction()) {
        if (!session->silence_execute_group_ok_stmt(
                session->get_silence_ok_stmt_id())) {
          LOG_ERROR(
              "Fail to silence_execute_group_ok_stmt for prepare id %d during "
              "implict commit.\n",
              session->get_silence_ok_stmt_id());
          session->reset_top_stmt();
          return -1;
        }
        try {
          deal_with_transaction("COMMIT");
        } catch (...) {
          session->reset_top_stmt();
          throw;
        }
      } else if (type == STMT_COMMIT || type == STMT_SET ||
                 type == STMT_UNLOCK_TB) {
        if (!session->silence_execute_group_ok_stmt(
                session->get_silence_ok_stmt_id())) {
          LOG_ERROR(
              "Fail to silence_execute_group_ok_stmt for prepare id %d before "
              "commit.\n",
              session->get_silence_ok_stmt_id());
          session->reset_top_stmt();
          return -1;
        }
      } else if (type == STMT_ROLLBACK) {
        session->silence_roll_back_group_sql(session->get_silence_ok_stmt_id());
      }
    }
    if (type == STMT_LOCK_TB || type == STMT_START_TRANSACTION)
      if (session->is_in_lock()) session->unlock();

    session->set_read_only(stmt->is_readonly());
    stmt_type check_type = type;
    if (stmt->get_stmt_node()->sql->explain_oper)
      check_type = stmt->get_stmt_node()->sql->explain_oper->explain_type;
    session->set_global_time_queries(
        stmt->is_readonly() ||
        (type == STMT_DBSCALE_EXPLAIN && check_type != STMT_UPDATE));
    session->set_affected_servers(AFFETCED_NON_SERVER);

    if (is_drop_current_database(type, stmt, session->get_schema())) {
      session->set_drop_current_schema(true);
    }
    if (type != STMT_DBSCALE_SHOW_EXECUTION_PROFILE) {
      session->clear_profile_tree();
    }
    /*For federated table sql, we can record it as a federated session by
     * handling the show table status STMT. And check the is_federated_session
     * flag here, to avoid the not necessary federated follower checking.*/
    if (session->get_is_federated_session() &&
        (MethodType)session->get_session_option("cross_node_join_method")
                .int_val == DATA_MOVE_READ &&
        is_federated_follower(stmt)) {
      session->reset_top_stmt();
      return 0;
    }

#ifndef DBSCALE_TEST_DISABLE
    if (on_test_stmt && is_blocking_stmt(stmt)) {
      session->reset_top_stmt();
      return 0;
    }
#endif

    if (enable_monitor_point_flag && need_monitor_local) {
      sql_statistic.start_timing();
    }
#ifndef DBSCALE_TEST_DISABLE
    gen_plan.start_timing();
#endif
    try {
      try {
        MySQLExecutePlan *plan =
            new MySQLExecutePlan(stmt, session, driver, this);
        session->set_top_plan(plan);

        try {
          if (is_should_use_txn_or_throw(stmt->get_stmt_node()->type)) {
            const char *begin_sql = "BEGIN";
            string sql = tmp_node->stmt_sql ? tmp_node->stmt_sql : query_sql;
            stmt->record_statement_stored_string(sql);
            const char *cur_sql = stmt->get_last_statement_stored_string();
            const char *commit_sql = "COMMIT";
            bool surround_sql_with_trx_succeed = false;
            session->set_is_surround_sql_with_trx_succeed(true);
            try {
              session->start_save_postponed_ok_packet();
              handle_query_command(begin_sql, strlen(begin_sql));
              LOG_DEBUG("Auto trx finish handle Begin sql\n");
              if (!session->get_cur_stmt_execute_fail() &&
                  session->get_cur_stmt_error_code() == 0) {
                session->set_has_check_acl(false);
                LOG_DEBUG("handle query command sql: [%s], [%d]\n", cur_sql,
                          strlen(cur_sql));
                handle_query_command(cur_sql, strlen(cur_sql));
                LOG_DEBUG("Auto trx finish handle cur sql: [%s]\n", cur_sql);
                session->set_can_change_affected_rows(false);
                session->set_cancel_reset_stmt_level_variables_one();
                if (!session->get_cur_stmt_execute_fail() &&
                    session->get_cur_stmt_error_code() == 0) {
                  session->set_has_check_acl(false);
                  handle_query_command(commit_sql, strlen(commit_sql));
                  LOG_DEBUG("Auto trx finish handle commit sql\n");
                  if (!session->get_cur_stmt_execute_fail() &&
                      session->get_cur_stmt_error_code() == 0) {
                    surround_sql_with_trx_succeed = true;
                  }
                }
              }
            } catch (...) {
              session->end_save_postponed_ok_packet();
              throw;
            }
            session->end_save_postponed_ok_packet();
            ExecuteNode *n = NULL;
            if (surround_sql_with_trx_succeed &&
                !session->get_is_silence_ok_stmt()) {
              n = plan->get_return_ok_node();
              ((MySQLReturnOKNode *)n)
                  ->set_ok_packet(
                      session->get_postponed_ok_packet(),
                      session
                          ->get_postponed_ok_packet_need_set_packet_number());
            } else {
              n = plan->get_empty_node();
            }
            plan->set_start_node(n);
          } else {
            if (get_session()->get_auto_commit_is_on() &&
                !get_session()->is_in_transaction() &&
                need_set_session_next_trx_type(stmt->get_stmt_node()->type)) {
              need_reset_session_next_trx_level = true;
              session->set_session_next_trx_level();
            }
            count_session_sql_op(session, type);
            if (stmt_is_unsafe_redo_sql(stmt->get_stmt_node()) &&
                session->check_for_transaction()) {
              // for xa transaction, unsafe_redo_sql_action here is ok.
              if (unsafe_redo_sql_action == UNSAFE_REDO_SQL_WARN) {
                LOG_WARN("Find a unsafe redo sql: %s.\n", stmt->get_sql());
              } else if (unsafe_redo_sql_action == UNSAFE_REDO_SQL_REFUSE) {
                LOG_ERROR("Refuse to execute unsafe redo sql: %s.\n",
                          stmt->get_sql());
                throw NotSupportedError(
                    "Refuse to execute unsafe redo sql when "
                    "unsafe_redo_sql_action=2.");
              }
            }
            if (!stmt->is_can_swap_type()) plan->add_session_traditional_num();

            make_plan(stmt, plan);
          }
          if (get_session()->get_execute_plan_touch_partition_nums() == -1) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_ALL_PARTITION);
          } else if (get_session()->get_execute_plan_touch_partition_nums() ==
                     1) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_ONE_PARTITION);
          } else if (get_session()->get_execute_plan_touch_partition_nums() ==
                     2) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_TWO_PARTITION);
          }
          get_session()->set_execute_plan_touch_partition_nums(0);
          if (get_session()->get_times_move_data() > 1) {
            get_session()->get_status()->item_inc(
                TIMES_CROSS_NODE_JOIN_OR_TABLE_SUB_QUERY_MOVE_DATA_MORE_THAN_ONCE);
            get_session()->reset_times_move_data();
          }
          if (use_savepoint && session->check_for_transaction() &&
              session->get_affected_server_type() == AFFETCED_MUL_SERVER &&
              !session->is_in_cluster_xa_transaction()) {
            // xa transaction will not support internal savepoint
            LOG_DEBUG("DBScale add SAVEPOINT for sql [%s]\n", stmt->get_sql());
            string str = "SAVEPOINT ";
            str += SAVEPOINT_NAME;
            deal_with_transaction((const char *)str.c_str());
          }
          deal_ddl_for_consistence_point(session, type, true);
          if (enable_monitor_point_flag && need_monitor_local) {
            sql_statistic.end_timing(monitor_stage::PLAN_GENERATION);
          }
#ifndef DBSCALE_TEST_DISABLE
          gen_plan.end_timing_and_report();
#endif
          plan->generate_plan_can_swap();
          if (!plan->plan_can_swap() && stmt->is_can_swap_type()) {
            plan->add_session_traditional_num();
          }
          try {
            if (enable_receive_next_header &&
                stmt->get_stmt_node()->type == STMT_SELECT) {
              session->set_is_query_command(true);
            }
            if (enable_monitor_point_flag && need_monitor_local) {
              sql_statistic.start_timing();
            }
            session->set_fe_error(false);
            plan->execute();
            session->set_is_query_command(false);
            if (get_session()->need_swap_for_server_waiting()) {
              LOG_DEBUG(
                  "Handler %@ stop processing request due to the session %@ is "
                  "waiting for server.\n",
                  this, get_session());
              return 0;
            }
            // cluster xa transaction should also record
            if (enable_record_transaction_sqls &&
                session->check_for_transaction() &&
                (type != STMT_ROLLBACK && type != STMT_COMMIT)) {
              get_session()->add_transaction_sql(query_sql);
            }
            if (session->get_slow_query_time_local() > 0 ||
                session->get_audit_log_flag_local() !=
                    AUDIT_LOG_MODE_MASK_NON) {
              end_date = ACE_OS::gettimeofday();
              record_audit_log(type);
              check_and_record_slow_query_time();
            }
            if (enable_monitor_point_flag && need_monitor_local) {
              sql_statistic.end_timing(monitor_stage::PLAN_EXECUTION);
            }
          } catch (...) {
            stmt->handle_federated_join();
            deal_ddl_for_consistence_point(session, type, false);
            session->set_is_query_command(false);
            throw;
          }
          if (get_session()->get_transfer_rule()) {
            get_session()->get_transfer_rule()->close();
            get_session()->set_transfer_rule(NULL);
            get_session()->reinit_follower_session();
          }

          if (!plan->get_is_forward_plan() && type != STMT_SELECT &&
              type != STMT_UPDATE && type != STMT_DELETE &&
              type != STMT_INSERT) {
#ifndef DBSCALE_TEST_DISABLE
            LOG_INFO("deal_with_metadata for sql %s.\n", stmt->get_sql());
#endif
            const char *schema_name_tmp = session->get_schema();
            if (type == STMT_CREATE_TB || type == STMT_DROP_TB) {
              stmt_node *st = stmt->get_stmt_node();
              record_scan *rs_tmp = st->scanner;
              table_link *tl = rs_tmp->first_table;
              schema_name_tmp = tl->join->schema_name ? tl->join->schema_name
                                                      : session->get_schema();
            }
            deal_with_metadata_final(type, stmt->get_sql(), schema_name_tmp,
                                     stmt->get_stmt_node());
          }

          if (enable_info_schema_mirror_table > 0 && datasource_in_one &&
              (type > STMT_DDL_START) && (type < STMT_DDL_END) &&
              type != STMT_TRUNCATE) {
            if (type == STMT_ALTER_TABLE || type == STMT_CREATE_TB) {
              table_link *tl = stmt->get_stmt_node()->scanner->first_table;
              const char *schema_name = tl->join->schema_name
                                            ? tl->join->schema_name
                                            : session->get_schema();
              const char *table_name = tl->join->table_name;
              Backend::instance()->set_informationschema_mirror_tb_out_of_date(
                  true, schema_name, table_name);
            } else if (type == STMT_DROP_TB) {
              table_link *table = stmt->get_stmt_node()->table_list_head;
              while (table) {
                const char *schema_name = table->join->schema_name
                                              ? table->join->schema_name
                                              : session->get_schema();
                const char *table_name = table->join->table_name;
                Backend::instance()
                    ->set_informationschema_mirror_tb_out_of_date(
                        true, schema_name, table_name);
                table = table->next;
              }
            } else if (type == STMT_CREATE_DB) {
              const char *schema_name =
                  stmt->get_stmt_node()->sql->create_db_oper->dbname->name;
              Backend::instance()->set_informationschema_mirror_tb_out_of_date(
                  true, schema_name, NULL);
            } else if (type == STMT_DROP_DB) {
              const char *schema_name =
                  stmt->get_stmt_node()->sql->drop_db_oper->dbname->name;
              Backend::instance()->set_informationschema_mirror_tb_out_of_date(
                  true, schema_name, NULL);
            } else {
              Backend::instance()->set_informationschema_mirror_tb_out_of_date(
                  true, NULL, NULL);
            }
          }

          if (session->is_drop_current_schema()) session->set_schema(NULL);

          deal_ddl_for_consistence_point(session, type, false);

          for (size_t i = 0; i < session->table_info_to_delete_pair_vec.size();
               i++) {
            driver->erase_lru_cache(
                session->table_info_to_delete_pair_vec[i].first,
                session->table_info_to_delete_pair_vec[i].second);
          }

          get_session()->apply_table_info_to_delete();
          if (type == STMT_CALL)
            ((MySQLSession *)get_session())->reset_all_nest_sp_counter();

        } catch (...) {
          if (get_session()->get_execute_plan_touch_partition_nums() == -1) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_ALL_PARTITION);
          } else if (get_session()->get_execute_plan_touch_partition_nums() ==
                     1) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_ONE_PARTITION);
          } else if (get_session()->get_execute_plan_touch_partition_nums() ==
                     2) {
            get_session()->get_status()->item_inc(
                TIMES_EXECUTION_PLAN_TOUCH_TWO_PARTITION);
          }
          get_session()->set_execute_plan_touch_partition_nums(0);
          if (get_session()->get_transfer_rule()) {
            get_session()->get_transfer_rule()->close();
            get_session()->set_transfer_rule(NULL);
            get_session()->reinit_follower_session();
          }
          if (type == STMT_CREATE_PROCEDURE) {
            string sp_name = stmt->get_current_sp_worker_name();
            session->remove_sp_worker(sp_name);
          }
          if (session->check_for_transaction() &&
              session->is_server_shutdown()) {
            // when session is down, xa conn should rollback
            session->cluster_xa_rollback();
            session->rollback();
            set_keep_conn(session->is_keeping_connection());
            LOG_ERROR("In transaction, server shutdown.\n");
            throw ServerShutDown("In transaction, server shutdown.");
          }
          // for cluster xa transaction, if not shutdown error, not rollback
          if (!use_savepoint) {
            if (!select_error_not_rollback || type != STMT_SELECT) {
              session->rollback();
              set_keep_conn(session->is_keeping_connection());
            }
          }
          if (be_killed()) {
            LOG_DEBUG("Thread %d is killed during plan execution.\n",
                      get_thread_id());
            throw ThreadIsKilled();
          }
          if (session->get_sp_error_code() > 0) {
            throw dbscale::sql::SQLError(
                session->get_sp_error_message().c_str(),
                session->get_sp_error_state().c_str(),
                session->get_sp_error_code());
          } else
            throw;
        }
      } catch (ThreadIsKilled &e) {
        session->set_error_generated_by_dbscale(true);
        session->reset_top_stmt_and_plan();
        throw;
      } catch (NestStatementErrorException &e) {
        session->set_error_generated_by_dbscale(false);
        if (stmt->is_handle_sub_query())
          LOG_DEBUG("Get separated exec subquery fail error for SQL [%s]\n",
                    stmt->get_sql());
        if (session->is_call_store_procedure())
          LOG_DEBUG("Get routine sub stmt exec fail error.\n");
        // Nothing need to be down, just go through the normal process to clean
      } catch (CrossNodeJoinFail &e) {
        session->set_error_generated_by_dbscale(false);
        LOG_ERROR("Cross node join failed for SQL [%s]\n", stmt->get_sql());
      } catch (ServerShutDown &e) {
        session->set_error_generated_by_dbscale(true);
        LOG_DEBUG("%s\n", e.what());
        session->reset_top_stmt_and_plan();
        throw e;
      } catch (SQLException &e) {
        session->set_error_generated_by_dbscale(true);
        handle_exception(&e, true, e.get_errno(), e.what(), e.get_sqlstate());
      } catch (SilenceOKStmtFail &e) {
        throw e;
      } catch (Exception &e) {
        session->set_error_generated_by_dbscale(true);
        handle_exception(&e, true, e.get_errno(), e.what());
        if (session->get_is_info_schema_mirror_tb_reload_internal_session())
          throw;
      } catch (exception &e) {
        session->set_error_generated_by_dbscale(true);
        LOG_ERROR("Get exception %s during plan execution.\n", e.what());
        session->reset_top_stmt_and_plan();
        throw;
      }
      if (session->is_error_generated_by_dbscale()) {
        session->get_status()->item_inc(TIMES_ERROR_GENERATED_BY_DBSCALE);
        session->set_error_generated_by_dbscale(false);
        sql_statistic.set_record_as_error(true);
      }
    } catch (...) {
      if (session->is_error_generated_by_dbscale()) {
        session->get_status()->item_inc(TIMES_ERROR_GENERATED_BY_DBSCALE);
        session->set_error_generated_by_dbscale(false);
        sql_statistic.set_record_as_error(true);
      }
      throw;
    }
    session->reset_top_stmt_and_plan();
    if (!need_reset_mul_top_stmt) node = NULL;
    if (node) {
      session->reset_statement_level_session_variables(false);
    }
  }
  if (need_reset_mul_top_stmt) session->reset_top_stmt();
  if (need_reset_session_next_trx_level)
    session->reset_session_next_trx_level();
  return 0;
}

bool MySQLHandler::get_meta_for_com_prepare(
    const char *sql, prepare_stmt_group_handle_meta &meta, list<int> &param_pos,
    list<string> &split_prepare_sql, vector<string> &all_table_name,
    stmt_type &st_type, size_t &mem_size) {
  Statement *stmt = NULL;
  bool session_allow_dot_in_ident =
      session->get_session_option("allow_dot_in_ident").int_val;
  try {
    Parser *parser = Driver::get_driver()->get_parser();
    stmt = parser->parse(sql, session_allow_dot_in_ident, true, NULL, NULL,
                         NULL, session->get_client_charset_type());
  } catch (...) {
    if (stmt) {
      stmt->free_resource();
      delete stmt;
      stmt = NULL;
    }
    throw;
  }

  if (!stmt) {
    LOG_ERROR("Fail to parse PREPARE.\n");
    throw Error("Fail to parse PREPARE.");
  }
  st_type = stmt->get_stmt_node()->type;
  if (stmt->is_partial_parsed()) {
    if (Backend::instance()->is_centralized_cluster()) {
      if (stmt) {
        stmt->free_resource();
        delete stmt;
        stmt = NULL;
      }
      return true;
    }
    string err("Not support partial parse for PREPARE: ");
    if (stmt->get_stmt_node() && stmt->get_stmt_node()->error_message)
      err.append(stmt->get_stmt_node()->error_message);
    LOG_ERROR("%s\n", err.c_str());
    stmt->free_resource();
    delete stmt;
    stmt = NULL;

    throw Error(err.c_str());
  }

  table_link *tl = stmt->get_stmt_node()->scanner->first_table;
  while (tl) {
    const char *schema_name =
        tl->join->schema_name ? tl->join->schema_name : session->get_schema();
    const char *table_name = tl->join->table_name;
    string name = schema_name;
    name += '.';
    name += table_name;
    all_table_name.push_back(name);
    mem_size += name.length();
    tl = tl->next;
  }

  preparam_item *preparam = stmt->get_stmt_node()->preparam_item_list;
  param_pos.clear();
  split_prepare_sql.clear();
  int split_start = 0;
  int split_end = strlen(sql);
  while (preparam) {
    param_pos.push_front(preparam->pos);
    split_start = preparam->pos;
    string split_sql = string(sql + split_start, split_end - split_start);
    split_prepare_sql.push_front(split_sql);
    mem_size += (split_sql.length() + sizeof(int));
    split_end = split_start - 1;
    preparam = preparam->next;
  }
  string split_sql = string(sql, split_end);
  split_prepare_sql.push_front(split_sql);
  meta.st_type = stmt->get_stmt_node()->type;
  LOG_DEBUG("prepare type is %d.\n", (int)meta.st_type);
  if (meta.st_type == STMT_INSERT || meta.st_type == STMT_REPLACE) {
    meta.keyword_values_end_pos =
        stmt->get_stmt_node()->sql->insert_oper->keyword_values_end_pos;
  }
  if (stmt) {
    stmt->free_resource();
    delete stmt;
    stmt = NULL;
  }
  mem_size += sizeof(meta);
  return false;
}

void MySQLHandler::record_outline_info(const char *sql, size_t sql_len) {
  stmt_node st;
  init_stmt_node(&st);
  if (do_fast_parse(sql, sql_len, &st)) {
    // if fast parse error, we just ignore this sql
    LOG_ERROR("Fast parse error for sql: [%s]\n", sql);
  } else {
    auto sql_id = calculate_md5(st.no_param_sql);
    sql_statistic.set_outline_info(st.no_param_sql, sql_id.c_str());
  }
  free_stmt_node_mem(&st);
}

void MySQLHandler::progress_explain_head_before_attach_hint(
    string &remove_explain_sql, MySQLStatement *&stmt) {
  boost::smatch result;
  if (boost::regex_match(
          remove_explain_sql,
          boost::regex("explain.*|dbscale explain.*", boost::regex::icase))) {
    MySQLParser *parser = MySQLParser::instance();
    stmt = static_cast<MySQLStatement *>(parser->parse(
        remove_explain_sql.c_str(),
        session->get_session_option("allow_dot_in_ident").int_val, true, NULL,
        NULL, NULL, session->get_client_charset_type()));
    stmt_type type = stmt->get_stmt_node()->type;
    if (type == STMT_DBSCALE_EXPLAIN || type == STMT_EXPLAIN) {
      if (stmt->get_stmt_node()->sql->explain_oper == nullptr) {
        // e: dbscale explain "a";
        const char *err = "not support explain_oper is nullptr";
        LOG_ERROR("%s\n", err);
        throw HandlerError(err);
      } else {
        unsigned int explain_begin_pos =
            stmt->get_stmt_node()->sql->explain_oper->sql_start_pos - 1;
        remove_explain_sql = remove_explain_sql.substr(explain_begin_pos);
        LOG_DEBUG(
            "progress_explain_head_before_attach_hint explain_begin_pos[%d], "
            "remove_explain_sql[%s]\n",
            explain_begin_pos, remove_explain_sql.c_str());
      }
    }
  }
}

bool MySQLHandler::do_prepare_attach_index_hint(
    const char *query_sql, const outline_hint_item &oh_item,
    vector<pair<int, string>> &hint_pos_value, MySQLStatement *&stmt) {
  if (!query_sql) {
    LOG_ERROR("do_prepare_attach_index_hint query_sql is null\n");
    return false;
  }

  int table_order = 1;
  table_link *cur_table_link = NULL;
  if (stmt) {
    if (stmt->get_stmt_node() && stmt->get_stmt_node()->table_list_head)
      cur_table_link = stmt->get_stmt_node()->table_list_head;
    else {
      stmt->free_resource();
      delete stmt;
      stmt = NULL;
    }
  }
  for (outline_hint_data::const_iterator it = oh_item.hint_data.begin();
       it != oh_item.hint_data.end(); ++it) {
    if (it->second.first != INDEX_HINT) continue;
    if (!stmt) {
      MySQLParser *parser = MySQLParser::instance();
      stmt = static_cast<MySQLStatement *>(parser->parse(
          query_sql, session->get_session_option("allow_dot_in_ident").int_val,
          true, NULL, NULL, NULL, session->get_client_charset_type()));
      if (!stmt || !stmt->get_stmt_node() ||
          !stmt->get_stmt_node()->table_list_head) {
        LOG_ERROR("do_prepare_attach_index_hint parse query[%s] error.\n",
                  query_sql);
        return false;
      }
      cur_table_link = stmt->get_stmt_node()->table_list_head;
    }
    // find table_order by table_list
    int tmp_table_order = it->first - INDEX_HINT_SHIFT;
    string tmp_hint = it->second.second;
    while (table_order < tmp_table_order) {
      cur_table_link = cur_table_link->next;
      table_order++;
    }

    int insert_pos = cur_table_link->alias_end_pos > 0
                         ? cur_table_link->alias_end_pos
                         : cur_table_link->end_pos;
    hint_pos_value.push_back(make_pair(insert_pos, tmp_hint));
  }
  return true;
}

bool MySQLHandler::sql_attach_outline_hint(const char *query_sql) {
  LOG_DEBUG("sql_attach_outline_hint start. query_sql[%s]\n", query_sql);
  string remove_explain_sql = query_sql, error_msg = "";
  MySQLStatement *stmt = NULL;
  cur_hint_sql.clear();
  bool is_attach_success = false;
  stmt_node tmp_st;
  bool is_tmp_st_inited = false;
  try {
    progress_explain_head_before_attach_hint(remove_explain_sql, stmt);

    // fast parse get sql_id
    init_stmt_node(&tmp_st);
    is_tmp_st_inited = true;
    if (do_fast_parse(remove_explain_sql.c_str(), remove_explain_sql.length(),
                      &tmp_st)) {
      // if fast parse error, we just ignore this sql
      error_msg = "Fast parse error for sql: " + remove_explain_sql;
      throw HandlerError(error_msg.c_str());
    }

    // get oh_item by sql_id
    string sql_id = calculate_md5(tmp_st.no_param_sql);
    string cur_schema = session->get_schema();
    outline_hint_item oh_item;
    if (!backend->get_outline_hint_item(sql_id, cur_schema, oh_item)) {
      if (stmt) {
        stmt->free_resource();
        delete stmt;
        stmt = NULL;
      }
      free_stmt_node_mem(&tmp_st);
      return false;
    }

    string tmp_query_sql = query_sql;
    vector<pair<int, string>> hint_pos_value;
    // prepare attach optimizer hint if exist
    reset_stmt_node_for_routine(&tmp_st);
    if (!do_prepare_attach_optimizer_hint(tmp_query_sql.c_str(), &tmp_st,
                                          oh_item, hint_pos_value)) {
      error_msg =
          "Prepare attach optimizer hint error for sql: " + tmp_query_sql;
      throw HandlerError(error_msg.c_str());
    }

    // prepare attach index hint if exist
    if (!do_prepare_attach_index_hint(tmp_query_sql.c_str(), oh_item,
                                      hint_pos_value, stmt)) {
      error_msg = "Prepare attach index hint error for sql: " + tmp_query_sql;
      throw HandlerError(error_msg.c_str());
    }

    // really attach optimizer hint and index hint
    sort(hint_pos_value.begin(), hint_pos_value.end(),
         [&](pair<int, string> x, pair<int, string> y) { return x < y; });
    int acc_hint_len = 0;
    for (vector<pair<int, string>>::const_iterator it = hint_pos_value.begin();
         it != hint_pos_value.end(); ++it) {
      tmp_query_sql = tmp_query_sql.substr(0, it->first + acc_hint_len) +
                      it->second +
                      tmp_query_sql.substr(it->first + acc_hint_len);
      acc_hint_len += it->second.length();
    }

    cur_hint_sql.swap(tmp_query_sql);
    LOG_DEBUG(
        "MySQLHandler::sql_attach_outline_hint let sql [%s] attach hint to "
        "[%s]\n",
        query_sql, cur_hint_sql.c_str());
    is_attach_success = true;
  } catch (HandlerError &e) {
    is_attach_success = false;
    LOG_ERROR("sql_attach_outline_hint get error. %s\n", e.what());
  } catch (Exception &e) {
    is_attach_success = false;
    LOG_ERROR("sql_attach_outline_hint get error. %s\n", e.what());
  }

  if (stmt) {
    stmt->free_resource();
    delete stmt;
    stmt = NULL;
  }
  if (is_tmp_st_inited) free_stmt_node_mem(&tmp_st);
  return is_attach_success;
}

void MySQLHandler::prepare_deal_error_packet(Packet *packet, bool no_print) {
  if (no_print) return;
  MySQLErrorResponse error(packet);
  error.unpack();
  LOG_ERROR("Got error packet when execute MYSQL_COM_PREPARE: %d (%s) %s\n",
            error.get_error_code(), error.get_sqlstate(),
            error.get_error_message());
}

void MySQLHandler::prepare_direct_receive_from_server(
    Connection *prepare_conn, string &query_sql, uint32_t &global_stmt_id,
    uint32_t &backend_stmt_id, stmt_type &st_type, bool is_gsid) {
  Packet tmp_packet;

  receive_from_server(prepare_conn, &tmp_packet);

  if (driver->is_error_packet(&tmp_packet)) {
    if (is_gsid) send_to_client(&tmp_packet);
    prepare_deal_error_packet(&tmp_packet, is_gsid);
    return;
  }
  if (is_gsid) global_stmt_id = get_session()->get_gsid();

  MySQLPrepareResponse prep_resp(&tmp_packet);
  prep_resp.unpack();
  backend_stmt_id = prep_resp.statement_id;
  prep_resp.statement_id = global_stmt_id;
  prep_resp.pack(&tmp_packet);
  uint16_t para_num = prep_resp.num_params;

  if (is_gsid) send_to_client_by_buffer(&tmp_packet);

  if (prep_resp.num_params > 0) {
    receive_from_server(prepare_conn, &tmp_packet);
    while (!driver->is_eof_packet(&tmp_packet)) {
      if (is_gsid) send_to_client_by_buffer(&tmp_packet);
      receive_from_server(prepare_conn, &tmp_packet);
    }
    if (is_gsid) {
      send_to_client_by_buffer(&tmp_packet);
    }
  }
  if (prep_resp.num_columns > 0) {
    receive_from_server(prepare_conn, &tmp_packet);
    while (!driver->is_eof_packet(&tmp_packet)) {
      if (is_gsid) send_to_client_by_buffer(&tmp_packet);
      receive_from_server(prepare_conn, &tmp_packet);
    }
    if (is_gsid) {
      send_to_client_by_buffer(&tmp_packet);
    }
  }
  flush_net_buffer();

  if (is_gsid) {
    session->set_prepare_stmt_sql(global_stmt_id, query_sql, st_type, para_num);
    // deallocate the prepared statement
    MySQLCloseRequest close_req(backend_stmt_id);
    Packet pkt;
    close_req.pack(&pkt);
    prepare_conn->reset();
    send_to_server(prepare_conn, &pkt);
  }
}

void MySQLHandler::handle_prepare_command(Packet *packet) {
  Packet tmp_packet;
  Connection *conn = NULL;
  uint32_t stmt_id = 0;
  DataSpace *ds = NULL;

  LOG_DEBUG("command MYSQL_COM_PREPARE\n");
  const char *query_sql = NULL;
  uint32_t global_stmt_id = 0;
  bool is_prepare_partial_parse = false;
  bool need_cache_prep_info = false;
  bool need_push_prepare_cache = true;
  stmt_type st_type = STMT_NON;
  try {
    MySQLPrepareRequest prep_req(packet);
    prep_req.unpack();
    query_sql = prep_req.get_query();
    string tmp_sql(query_sql);
    if (tmp_sql.length() > max_prepare_sql_length) {
      need_push_prepare_cache = false;
    }
    get_session()->set_executing_prepare(true);

    if (!need_push_prepare_cache) {    // sql length > max_prepare_sql_length
      list<int> param_pos;             // position of '?' in query sql
      list<string> split_prepare_sql;  // split of query_sql by param
      prepare_stmt_group_handle_meta meta;
      vector<string> all_table_name;
      size_t mem_size = 0;
      is_prepare_partial_parse = get_meta_for_com_prepare(
          query_sql, meta, param_pos, split_prepare_sql, all_table_name,
          st_type, mem_size);

      // ds = backend->get_auth_data_space();
      bool dbscale_only_contain_catalog_and_dbscaletmp_dataspace =
          Backend::instance()->get_total_data_space_count() <= 2
              ? true
              : false;  // catalog, dbscale_tmp and mysql
      if (dbscale_only_contain_catalog_and_dbscaletmp_dataspace) {
        ds = backend->get_catalog();
      } else {
        ds = backend->get_auth_data_space();
      }

      if (is_prepare_partial_parse) {
        uint32_t backend_stmt_id = 0;
        conn = send_to_server_retry(ds, packet, NULL, false, false, false, true,
                                    false);
        try {
          prepare_direct_receive_from_server(conn, tmp_sql, global_stmt_id,
                                             backend_stmt_id, st_type, true);
        } catch (ServerSendFail &e) {
          if (conn) clean_dead_conn(&conn, ds);
        }
      } else {
        conn = send_to_server_retry(ds, packet, NULL, false, false, false, true,
                                    false);
        receive_from_server(conn, &tmp_packet);
        if (driver->is_error_packet(&tmp_packet)) {
          prepare_deal_error_packet(&tmp_packet);
          send_to_client(&tmp_packet);
          if (conn) {
            session->remove_using_conn(conn);
            conn->reset();
            conn->get_pool()->add_back_to_free(conn);
            conn = NULL;
          }
          return;
        }
        global_stmt_id = get_session()->get_gsid();
        MySQLPrepareResponse prep_resp(&tmp_packet);
        prep_resp.unpack();
        if (prep_resp.num_params != param_pos.size()) {
          LOG_ERROR(
              "prepare num params %d in response packet does not equal the "
              "number %d dbscale parsed",
              prep_resp.num_params, param_pos.size());
          throw HandlerError(
              "prepare num params in response packet does not equal the "
              "number "
              "dbscale parsed");
        }
        stmt_id = prep_resp.statement_id;
        MySQLPrepareItem *prepare_item = new MySQLPrepareItem();
        string str_query = query_sql;
        prepare_item->set_query(str_query);
        prepare_item->set_split_prepare_sql(split_prepare_sql);
        prepare_item->set_num_params(prep_resp.num_params);
        prepare_item->set_num_columns(prep_resp.num_columns);
        prepare_item->set_read_only(false);
        prepare_item->set_prepare_stmt_group_handle_meta(meta);
        Packet *pk = Backend::instance()->get_new_packet();
        prep_req.pack(pk);
        prepare_item->set_prepare_packet(pk);
        session->set_prepare_item(global_stmt_id, prepare_item);
        // TODO work:  in case of global_stmt_id overloop, we need to check
        // whether global_stmt_id is duplicated.
        /* Insert the prepare item into map in this session */
        LOG_DEBUG(
            "The prepare query of statement[%d] (dbscale assigned id [%d]) "
            "is %s.\n",
            stmt_id, global_stmt_id, query_sql);
        prep_resp.statement_id = global_stmt_id;
        prep_resp.pack(&tmp_packet);
        send_to_client_by_buffer(&tmp_packet);
        auto handle_packet_and_send_client =
            [this](Connection *conn, Packet &packet,
                   list<MySQLColumnType> *type_list) {
              receive_from_server(conn, &packet);
              while (!driver->is_eof_packet(&packet)) {
                MySQLColumnResponse column_resp(&packet);
                column_resp.unpack();
                type_list->push_back(column_resp.get_column_type());
                send_to_client_by_buffer(&packet);
                receive_from_server(conn, &packet);
              }
              send_to_client_by_buffer(&packet);
            };
        if (prep_resp.num_params > 0) {
          handle_packet_and_send_client(conn, tmp_packet,
                                        prepare_item->get_param_type_list());
        }
        if (prep_resp.num_columns > 0) {
          handle_packet_and_send_client(conn, tmp_packet,
                                        prepare_item->get_column_type_list());
        }
        flush_net_buffer();
        try {
          // deallocate the prepared statement
          MySQLCloseRequest close_req(stmt_id);
          Packet pkt;
          close_req.pack(&pkt);
          conn->reset();
          send_to_server(conn, &pkt);
        } catch (ServerSendFail &e) {
          if (conn) clean_dead_conn(&conn, ds);
        }
      }
      if (conn) {
        session->remove_using_conn(conn);
        conn->reset();
        conn->get_pool()->add_back_to_free(conn);
        conn = NULL;
      }
      return;
    }
    boost::shared_ptr<MySQLPrepareCacheInfo> prepare_ptr;
    if (!driver->find_in_lru_cache(tmp_sql, prepare_ptr)) {
      need_cache_prep_info = true;
    }
    if (need_cache_prep_info) {
      MySQLPrepareCacheInfo *prep_cache_info = new MySQLPrepareCacheInfo();
      size_t mem_size = sizeof(MySQLPrepareCacheInfo);
      try {
        prep_cache_info->schema = string(session->get_schema());
        prep_cache_info->query_sql = string(query_sql);
        prep_cache_info->sql_mem_size = strlen(query_sql);

        list<int> param_pos;             // position of '?' in query sql
        list<string> split_prepare_sql;  // split of query_sql by param
        vector<string> all_table_name;
        prepare_stmt_group_handle_meta meta;
        is_prepare_partial_parse = get_meta_for_com_prepare(
            query_sql, meta, param_pos, split_prepare_sql, all_table_name,
            st_type, mem_size);

        bool dbscale_only_contain_catalog_and_dbscaletmp_dataspace =
            Backend::instance()->get_total_data_space_count() <= 2
                ? true
                : false;  // catalog, dbscale_tmp and mysql
        if (dbscale_only_contain_catalog_and_dbscaletmp_dataspace) {
          ds = backend->get_catalog();
        } else {
          ds = backend->get_auth_data_space();
        }
        if (is_prepare_partial_parse) {
          uint32_t backend_stmt_id = 0;
          conn = send_to_server_retry(ds, packet, NULL, false, false, false,
                                      true, false);
          try {
            prepare_direct_receive_from_server(conn, tmp_sql, global_stmt_id,
                                               backend_stmt_id, st_type, true);
          } catch (ServerSendFail &e) {
            if (conn) clean_dead_conn(&conn, ds);
          }
        } else {
          prep_cache_info->table_name.swap(all_table_name);
          prep_cache_info->meta = meta;
          prep_cache_info->param_pos.swap(param_pos);
          prep_cache_info->split_prepare_sql.swap(split_prepare_sql);

          conn = send_to_server_retry(ds, packet, NULL, false, false, false,
                                      true, false);
          receive_from_server(conn, &tmp_packet);
          if (driver->is_error_packet(&tmp_packet)) {
            prepare_deal_error_packet(&tmp_packet);
            send_to_client(&tmp_packet);
            delete prep_cache_info;
            prep_cache_info = NULL;
            mem_size = 0;
            if (conn) {
              session->remove_using_conn(conn);
              conn->reset();
              conn->get_pool()->add_back_to_free(conn);
              conn = NULL;
            }
            return;
          }
          global_stmt_id = get_session()->get_gsid();
          MySQLPrepareResponse prep_resp(&tmp_packet);
          prep_resp.unpack();
          prep_cache_info->prepare_resp_elems =
              MySQLPrepareResponseElements(prep_resp);
          mem_size += prep_cache_info->prepare_resp_elems.mem_size;
          if (prep_resp.num_params != prep_cache_info->param_pos.size()) {
            LOG_ERROR(
                "prepare num params %d in response packet does not equal "
                "the "
                "number %d dbscale parsed",
                prep_resp.num_params, prep_cache_info->param_pos.size());
            throw HandlerError(
                "prepare num params in response packet does not equal the "
                "number "
                "dbscale parsed");
          }
          stmt_id = prep_resp.statement_id;
          MySQLPrepareItem *prepare_item = new MySQLPrepareItem();
          prepare_item->set_query(prep_cache_info->query_sql);
          prepare_item->set_split_prepare_sql(
              prep_cache_info->split_prepare_sql);
          prepare_item->set_num_params(prep_resp.num_params);
          prepare_item->set_num_columns(prep_resp.num_columns);
          prepare_item->set_read_only(false);
          prepare_item->set_prepare_stmt_group_handle_meta(meta);
          prep_cache_info->read_only = false;
          Packet *pk = Backend::instance()->get_new_packet();
          prep_req.pack(pk);
          prepare_item->set_prepare_packet(pk);
          session->set_prepare_item(global_stmt_id, prepare_item);
          // TODO work:  in case of global_stmt_id overloop, we need to
          // check whether global_stmt_id is duplicated.
          /* Insert the prepare item into map in this session */
          LOG_DEBUG(
              "The prepare query of statement[%d] (dbscale assigned id "
              "[%d]) is %s.\n",
              stmt_id, global_stmt_id, query_sql);
          prep_resp.statement_id = global_stmt_id;
          prep_resp.pack(&tmp_packet);
          send_to_client_by_buffer(&tmp_packet);
          auto handle_packet_and_send_client =
              [this](Connection *conn, Packet &packet, size_t &mem_size,
                     MySQLPrepareCacheInfo *prep_cache_info,
                     list<MySQLColumnType> *type_list, bool is_param) {
                receive_from_server(conn, &packet);
                while (!driver->is_eof_packet(&packet)) {
                  MySQLColumnResponse column_resp(&packet);
                  column_resp.unpack();
                  if (is_param)
                    prep_cache_info->add_one_params_column_response_item(
                        column_resp, mem_size);
                  else
                    prep_cache_info->add_one_columns_column_response_item(
                        column_resp, mem_size);
                  type_list->push_back(column_resp.get_column_type());
                  send_to_client_by_buffer(&packet);
                  receive_from_server(conn, &packet);
                }
                MySQLEOFResponse eof(&packet);
                eof.unpack();
                if (is_param) {
                  prep_cache_info->add_params_column_eof_response(eof);
                } else {
                  prep_cache_info->add_columns_column_eof_response(eof);
                }
                send_to_client_by_buffer(&packet);
              };
          if (prep_resp.num_params > 0) {
            handle_packet_and_send_client(
                conn, tmp_packet, mem_size, prep_cache_info,
                prepare_item->get_param_type_list(), true);
          }
          if (prep_resp.num_columns > 0) {
            handle_packet_and_send_client(
                conn, tmp_packet, mem_size, prep_cache_info,
                prepare_item->get_column_type_list(), false);
          }
          flush_net_buffer();
          prep_cache_info->mem_size = mem_size;
          prep_cache_info->build_time = ACE_OS::gettimeofday();
          driver->insert_into_lru_cache(prep_cache_info->query_sql,
                                        prep_cache_info);

          try {
            // deallocate the prepared statement
            MySQLCloseRequest close_req(stmt_id);
            Packet pkt;
            close_req.pack(&pkt);
            conn->reset();
            send_to_server(conn, &pkt);
          } catch (ServerSendFail &e) {
            if (conn) clean_dead_conn(&conn, ds);
          }
        }
      } catch (...) {
        if (prep_cache_info) {
          delete prep_cache_info;
          prep_cache_info = NULL;
        }
        throw;
      }
      if (conn) {
        session->remove_using_conn(conn);
        conn->reset();
        conn->get_pool()->add_back_to_free(conn);
        conn = NULL;
      }
      return;
    }

    LOG_DEBUG(
        "session %@ use driver cached Prepare info for prepare sql [%s]\n",
        session, prepare_ptr->query_sql.c_str());
    global_stmt_id = get_session()->get_gsid();
    MySQLPrepareResponse prep_resp(prepare_ptr->prepare_resp_elems);
    MySQLPrepareItem *prepare_item = new MySQLPrepareItem();
    prepare_item->set_query(prepare_ptr->query_sql);
    prepare_item->set_split_prepare_sql(prepare_ptr->split_prepare_sql);
    prepare_item->set_num_params(prep_resp.num_params);
    prepare_item->set_num_columns(prep_resp.num_columns);
    prepare_item->set_read_only(prepare_ptr->read_only);
    prepare_item->set_prepare_stmt_group_handle_meta(prepare_ptr->meta);

    Packet *pk = Backend::instance()->get_new_packet();
    prep_req.pack(pk);
    prepare_item->set_prepare_packet(pk);
    session->set_prepare_item(global_stmt_id, prepare_item);

    prep_resp.statement_id = global_stmt_id;
    Packet tmp_packet;
    prep_resp.pack(&tmp_packet);
    if (method_to_handle_autocommit_flag == HANDLE_AUTOCOMMIT_FLAG_IN_SEND) {
      send_to_client_by_buffer(&tmp_packet);
    } else {
      set_auto_commit_off_if_necessary(&tmp_packet);
      send_to_client_by_buffer(&tmp_packet);
    }

    auto send_packet_to_client =
        [this](Packet &packet, vector<MySQLColumnResponseElements> &column_resp,
               MySQLEOFResponse &column_eof, list<MySQLColumnType> *type_list,
               bool is_param) {
          vector<MySQLColumnResponseElements>::iterator it =
              column_resp.begin();
          for (; it != column_resp.end(); ++it) {
            MySQLColumnResponse column_resp(*it);
            type_list->push_back(column_resp.get_column_type());
            column_resp.pack(&packet);
            send_to_client_by_buffer(&packet);
          }
          if (is_param) {
            column_eof.pack(&packet);
          } else {
            column_eof.pack(&packet);
          }
          set_auto_commit_off_if_necessary(&packet);
          send_to_client_by_buffer(&packet);
        };
    if (prep_resp.num_params > 0) {
      send_packet_to_client(tmp_packet, prepare_ptr->params_column_resp,
                            prepare_ptr->params_column_eof,
                            prepare_item->get_param_type_list(), true);
    }
    if (prep_resp.num_columns > 0) {
      send_packet_to_client(tmp_packet, prepare_ptr->columns_column_resp,
                            prepare_ptr->columns_column_eof,
                            prepare_item->get_column_type_list(), false);
    }
    flush_net_buffer();

  } catch (exception &e) {
    LOG_ERROR("Error occured when execute MYSQL_COM_PREPARE due to [%s].\n",
              e.what());
    if (conn) {
      clean_dead_conn(&conn, ds);
    }
    session->clean_prepare_item(global_stmt_id);
    session->set_executing_prepare(false);
    throw Error(e.what());
  }
}
void MySQLHandler::handle_long_data_command(Packet *packet) {
  LOG_DEBUG("command MYSQL_COM_LONG_DATA\n");
  MySQLLongDataRequest long_data_req(packet);
  long_data_req.unpack();

  if (session->reset_prepare_column_type(long_data_req.statement_id, true)) {
    LOG_ERROR(
        "Notsupport prepare statement (%u) included blob paramer when "
        "use-partial-parse=1.\n",
        long_data_req.statement_id);
    dbscale::sql::SQLError error(
        "NotSupport prepare included blob paramer, when use-partial-parse=1",
        "42000", ERROR_UNSUPPORT_SQL_CODE);
    handle_exception(&error, true, error.get_errno(), error.what());
    return;
  }
  MySQLPrepareItem *prepare_item =
      session->get_prepare_item(long_data_req.statement_id);
  if (!prepare_item) {
    LOG_WARN("Not find prepare item for statement id %d.\n",
             long_data_req.statement_id);
    return;
  }
  char *data = new char[long_data_req.payload_length];
  memcpy(data, long_data_req.payload, long_data_req.payload_length);

  prepare_item->add_long_data_packet(long_data_req.param_id, data,
                                     long_data_req.payload_length);
}
void MySQLHandler::handle_reset_stmt_command(Packet *packet) {
  LOG_DEBUG("command MYSQL_COM_RESET_STMT\n");
  MySQLResetStmtRequest reset_stmt_req(packet);
  reset_stmt_req.unpack();

  if (session->reset_prepare_column_type(reset_stmt_req.statement_id)) {
    send_ok_packet_to_client(this, 1, 0);
    return;
  }
  MySQLPrepareItem *prepare_item =
      session->get_prepare_item(reset_stmt_req.statement_id);
  if (!prepare_item) {
    LOG_ERROR(
        "Unknown prepared statement handler (%u) given to mysqld_stmt_reset.\n",
        reset_stmt_req.statement_id);
    Packet *error_packet = get_error_packet(
        1243, "Unknown prepared statement handler given to mysqld_stmt_reset",
        "HY000");
    MySQLErrorResponse error(error_packet);
    error.unpack();
    error.pack(packet);
    delete error_packet;
    send_to_client(packet);
    return;
  }
  prepare_item->reset_stmt();
  send_ok_packet_to_client(this, 1, 0);
}

void MySQLHandler::handle_close_command(Packet *packet) {
  // MYSQL_COM_CLOSE_STMT always response nothing to client
  LOG_DEBUG("command MYSQL_COM_CLOSE_STMT\n");
  MySQLCloseRequest close_req(packet);
  close_req.unpack();
  uint32_t stmt_id = close_req.statement_id;
  if (session->remove_prepare_stmt_sql(stmt_id)) return;
  MySQLPrepareItem *prepare_item = session->get_prepare_item(stmt_id);
  if (prepare_item == NULL) {
    LOG_ERROR("Cannot get prepare item.\n");
#ifdef DEBUG
    ACE_ASSERT(0);
#endif
  }
  if (!session->silence_execute_group_ok_stmt(
          session->get_silence_ok_stmt_id())) {
    LOG_ERROR(
        "Fail to silence_execute_group_ok_stmt for prepare id %d before "
        "close.\n",
        session->get_silence_ok_stmt_id());
    session->reset_top_stmt();
    return;
  }
  session->clean_prepare_item(stmt_id);
  return;
}

void MySQLHandler::handle_execute_command(Packet *packet) {
  LOG_DEBUG("command MYSQL_COM_EXECUTE\n");
  try {
    MySQLExecuteRequest exec_req(packet);
    exec_req.unpack();
    LOG_DEBUG("The statement id of this execute command is %d.\n",
              exec_req.stmt_id);

    uint32_t global_stmt_id = exec_req.stmt_id;
    string query_sql;
    stmt_type st_type = STMT_NON;
    uint16_t param_num = 0;
    session->get_prepare_stmt_info(global_stmt_id, query_sql, st_type,
                                   param_num);
    if (!query_sql.empty()) {
      Connection *conn = NULL;
      DataSpace *ds = NULL;
      uint32_t backend_stmt_id = 0;
      LOG_DEBUG("The statement id %d execute sql %s.\n", global_stmt_id,
                query_sql.c_str());
      // construct prepare request packet
      MySQLPrepareRequest req_pre(query_sql.c_str());
      Packet tmp_packet;
      Packet *packet_new = new Packet();
      req_pre.pack(&tmp_packet);

      try {
        ds = Backend::instance()->get_catalog();
        // need get kept_connection
        conn =
            send_to_server_retry(ds, &tmp_packet, get_session()->get_schema(),
                                 false, false, false, false, false);
        prepare_direct_receive_from_server(conn, query_sql, global_stmt_id,
                                           backend_stmt_id, st_type, false);
        // deal execute prepare packet, need repackaged
        if (param_num > 0) {
          const char *null_bitmap_pos = exec_req.left_packet;
          uint null_bitmap_bytes = (param_num + 7) / 8;
          uint new_params_bound_flag =
              *(uint8_t *)(null_bitmap_pos + null_bitmap_bytes);
          const char *param_type_pos = null_bitmap_pos + null_bitmap_bytes + 1;
          vector<MySQLColumnType> *column_types =
              session->get_column_types(global_stmt_id);
          if (column_types == NULL) {
            column_types = new vector<MySQLColumnType>();
            session->add_column_types(global_stmt_id, column_types);
          }

          bool is_all_setnull = true;
          for (uint i = 0; i < param_num; ++i) {
            // if param all of them setnull. then new_params_bound_flag would
            // be false. no need deal with, wiil direct send to server.
            if (!(null_bitmap_pos[i / 8] & (1 << (i % 8)))) {
              is_all_setnull = false;
              break;
            }
          }
          if (!is_all_setnull) {
            if (new_params_bound_flag) { /* The first time to execute this stmt
                                          */
              column_types->clear();
              for (uint i = 0; i < param_num; ++i) {
                MySQLColumnType type =
                    (MySQLColumnType)(*((uint8_t *)&param_type_pos[i * 2]));
                column_types->push_back(type);
                LOG_DEBUG("The data type of the #%d column is %d.\n", i + 1,
                          type);
              }
            } else { /* Not the first time to execute this stmt */
              exec_req.set_need_set_new_params_bound_flag_manually();
              exec_req.set_num_params(param_num);
              char *all_type = new char[param_num * 2];
              for (uint i = 0; i < param_num; ++i) {
                Packet::pack2int(all_type + i * 2, (*column_types)[i]);
              }
              exec_req.set_all_param_type(all_type);
              delete[] all_type;
            }
          }
          // Every times needs to be repackaged
          exec_req.change_stmt_id_to_server(backend_stmt_id);
          exec_req.pack(packet_new);
          packet = packet_new;
        }
        conn->reset();
        send_to_server(conn, packet);
        receive_from_server(conn, &tmp_packet);

        if (driver->is_error_packet(&tmp_packet)) {
          MySQLErrorResponse error(&tmp_packet);
          error.unpack();
          LOG_DEBUG("command execute prepare failed: %d (%s) %s\n",
                    error.get_error_code(), error.get_sqlstate(),
                    error.get_error_message());
          send_to_client_by_buffer(&tmp_packet);
          flush_net_buffer();
          throw ErrorPacketException(error.get_error_message(),
                                     error.get_error_code());
        } else if (driver->is_ok_packet(&tmp_packet)) {
          send_to_client_by_buffer(&tmp_packet);
        } else {
          while (!driver->is_eof_packet(&tmp_packet)) {
            send_to_client_by_buffer(&tmp_packet);
            receive_from_server(conn, &tmp_packet);
          }
          do {
            send_to_client_by_buffer(&tmp_packet);
            receive_from_server(conn, &tmp_packet);
          } while (!driver->is_eof_packet(&tmp_packet));
          send_to_client_by_buffer(&tmp_packet);
        }
        flush_net_buffer();
        if (support_show_warning) {
          conn->reset();
          handle_warnings_OK_and_eof_packet(driver, &tmp_packet, this, ds,
                                            conn);
        }

        try {
          // deallocate the prepared statement
          MySQLCloseRequest close_req(backend_stmt_id);
          close_req.pack(&tmp_packet);
          conn->reset();
          send_to_server(conn, &tmp_packet);
          conn->reset();
        } catch (ServerSendFail &e) {
          // server error
          if (conn) clean_dead_conn(&conn, ds);
        }
        if (conn) put_back_connection(ds, conn);
      } catch (ErrorPacketException &e) {
        if (select_error_not_rollback && st_type == STMT_SELECT &&
            !session->in_auto_trx()) {
          put_back_connection(ds, conn);
        } else if (trx_pk_duplicate_not_rollback && e.get_errno() == 1062 &&
                   !session->in_auto_trx()) {
          put_back_connection(ds, conn);
        } else if (session->is_in_cluster_xa_transaction()) {
          put_back_connection(ds, conn);
        } else if (session->get_lock_kept_conn()) {
          put_back_connection(ds, conn);
        } else {
          if (conn) clean_dead_conn(&conn, ds);
        }
      } catch (...) {
        if (conn) clean_dead_conn(&conn, ds);
        delete packet_new;
        throw;
      }
      delete packet_new;
      return;
    }
    MySQLPrepareItem *prepare_item =
        session->get_prepare_item(exec_req.stmt_id);
    session->set_execute_stmt_id(exec_req.stmt_id);
    if (prepare_item) {
      MySQLExecuteItem execute_item;
      execute_param param;
      string query_sql;
      execute_item.set_stmt_id(exec_req.stmt_id);
      execute_item.set_flags(exec_req.flags);

      /* Deal with the parameteres */
      if (prepare_item->get_num_params() > 0) {
        vector<MySQLColumnType> *column_types =
            session->get_column_types(exec_req.stmt_id);
        bool need_clear = false;
        const char *null_bitmap_pos = exec_req.left_packet;
        uint null_bitmap_bytes = (prepare_item->get_num_params() + 7) / 8;
        uint new_params_bound_flag =
            *(uint8_t *)(null_bitmap_pos + null_bitmap_bytes);
        const char *param_type_pos = null_bitmap_pos + null_bitmap_bytes + 1;
        const char *param_value_pos =
            new_params_bound_flag
                ? (param_type_pos + prepare_item->get_num_params() * 2)
                : param_type_pos;

        bool has_check_column_types_size = false;
        int param_list_size = 0;
        param_num = prepare_item->get_num_params();
        for (uint i = 0; i < param_num; ++i) {
          /* Get the null state */
          if (null_bitmap_pos[i / 8] & (1 << (i % 8))) {
            param.is_null = true;
          } else {
            param.is_null = false;
          }
          if (new_params_bound_flag) { /* The first time to execute this stmt */
            if (column_types == NULL) {
              column_types = new vector<MySQLColumnType>();
              session->add_column_types(exec_req.stmt_id, column_types);
            }
            if (!need_clear) {
              need_clear = true;
              column_types->clear();
            }
            param.type =
                (MySQLColumnType)(*((uint8_t *)&param_type_pos[i * 2]));
            /* Save it into current session */
            column_types->push_back(param.type);
          } else { /* Not the first time to execute this stmt */
            // if param num is 1 and is null. then new_params_bound_flag would
            // be false
            if (param.is_null) {
              param.type = MYSQL_TYPE_NULL;
            } else {
              if (!has_check_column_types_size) {
                if (column_types->size() != prepare_item->get_num_params()) {
                  throw ExecuteCommandFail("The column types does not match.");
                }
                has_check_column_types_size = true;
              }
              param.type = (*column_types)[i];
            }
          }
          LOG_DEBUG("The data type of the #%d column is %d.\n", i + 1,
                    param.type);

          param.value.clear();
          string charset;
          if (session->get_client_charset_type() == CHARSET_TYPE_GBK)
            charset.assign("gbk");
          else if (session->get_client_charset_type() == CHARSET_TYPE_GB18030)
            charset.assign("gb18030");
          if (param.is_null) {
            param.value = "NULL";
          } else if (prepare_item->get_long_data_packet(i)) {
            if (param.type >= MYSQL_TYPE_TINY_BLOB &&
                param.type <= MYSQL_TYPE_BLOB) {
              LOG_DEBUG("The long data blob para %d is a blob.\n", i);
              param.value.append("0x");
              vector<long_data_packet_desc> *long_data_packet =
                  prepare_item->get_long_data_packet(i);
              vector<long_data_packet_desc>::iterator it_pd =
                  long_data_packet->begin();
              for (; it_pd != long_data_packet->end(); it_pd++) {
                for (uint32_t i = 0; i != it_pd->len; i++) {
                  char v[8] = {0};
                  sprintf(v, "%02X", (unsigned char)it_pd->data[i]);
                  param.value += v;
                }
                delete it_pd->data;
              }
              long_data_packet->clear();

            } else {
              LOG_DEBUG("The long data blob para %d is not a blob.\n", i);
              param.value.append("\'");
              vector<long_data_packet_desc> *long_data_packet =
                  prepare_item->get_long_data_packet(i);
              vector<long_data_packet_desc>::iterator it_pd =
                  long_data_packet->begin();
              for (; it_pd != long_data_packet->end(); it_pd++) {
                string value = string(it_pd->data, it_pd->len);
                add_translation_char_for_special_char(value, charset.c_str());
                param.value.append(value);
                delete it_pd->data;
              }
              long_data_packet->clear();

              param.value.append("\'");
            }
          } else {
            unpack_param_value(const_cast<char **>(&param_value_pos),
                               param.type, param.value, charset.c_str());
          }
          execute_item.get_param_list()->push_back(param);
          param_list_size++;
        }
        prepare_item->clear_all_long_data_values();

        if ((uint16_t)param_list_size != param_num) {
          LOG_ERROR(
              "The character to replace and param value size do not match.\n");
          throw ExecuteCommandFail("Cannot make execute query sql.");
        }
        list<execute_param> *param_list = execute_item.get_param_list();
        if (execute_replace_str(prepare_item, query_sql, param_list)) {
          LOG_ERROR("The '?' and parameter value do not match.\n");
          throw ExecuteCommandFail("Cannot make execute query sql.");
        }
      } else {
        list<string> *split_prepare_sql_list =
            prepare_item->get_split_prepare_sql();
        query_sql.assign(split_prepare_sql_list->front());
      }
      LOG_DEBUG("The query sql of execute command is: %s.\n",
                query_sql.c_str());

      prepare_stmt_group_handle_meta meta =
          prepare_item->get_prepare_stmt_group_handle_meta();
      uint32_t wid = exec_req.stmt_id;
      if (session->get_enable_silent_execute_prepare_local()) {
        bool need_to_silence_execute = true;
        bool skip_handle_query = false;
        if (!session->is_in_transaction() ||
            !session->get_silence_ok_stmt_id()) {
          session->clean_all_group_sql_of_prepare_stmt();
        } else if (wid != session->get_silence_ok_stmt_id()) {
          session->clean_target_prepare_stmt(wid);
        }
        if (session->is_in_transaction() &&
            (meta.st_type == STMT_INSERT || meta.st_type == STMT_REPLACE) &&
            (!session->get_silence_ok_stmt_id() ||
             wid == session->get_silence_ok_stmt_id())) {
          skip_handle_query = true;
          session->set_silence_ok_stmt_id(wid);
          prepare_item->store_sql_into_group_com_query(query_sql.c_str());
          if (prepare_item->get_group_com_query_stmt_str().length() <
              MAX_PREPARE_GROUP_STMT_LEN)
            need_to_silence_execute = false;
        }
        if (need_to_silence_execute) {
          if (!session->silence_execute_group_ok_stmt(
                  session->get_silence_ok_stmt_id())) {
            LOG_ERROR(
                "Fail to silence_execute_group_ok_stmt for prepare id %d.\n",
                session->get_silence_ok_stmt_id());
            if (!session->need_swap_for_server_waiting()) {
              session->set_binary_packet(NULL);
              session->set_binary_resultset(false);
            }
            return;
          }
        }
        if (!skip_handle_query) {
          /* The resultset should be binary protocol */
          session->set_binary_resultset(true);
          session->set_binary_packet(packet);
          /* Handle the query */
          handle_query_command(query_sql.c_str(), query_sql.length());
        } else {
          send_ok_packet_to_client(this, 1, 0);
        }
      } else {
        /* The resultset should be binary protocol */
        session->set_binary_resultset(true);
        session->set_binary_packet(packet);
        /* Handle the query */
        handle_query_command(query_sql.c_str(), query_sql.length());
      }
      /*
       * poxfix the bug issue 1225
       * if a client continue to exe some query
       *   after a DROP PREPARE operation, the
       *   is_binary_resultset will be true then
       *   will case bug issue 1225
       */
      if (!session->need_swap_for_server_waiting()) {
        session->set_binary_packet(NULL);
        session->set_binary_resultset(false);
      }
    } else {
      LOG_ERROR("Cannot get prepare item for execute command %d.\n",
                exec_req.stmt_id);
      throw ExecuteCommandFail("Cannot get prepare item for execute command.");
    }
  } catch (exception &e) {
    LOG_ERROR("Error occured when execute MYSQL_COM_EXECUTE due to [%s].\n",
              e.what());
    throw;
  }
}

bool ignore_dbscale_column_packet(Packet *row) {
  MySQLColumnResponse col_resp(row);
  col_resp.unpack();
  if (lower_case_compare(col_resp.get_column(), ignore_dbscale_column) == 0) {
    LOG_DEBUG("ignore column [%s]\n", col_resp.get_column());
    return true;
  }
  return false;
}

void MySQLHandler::do_refuse_login(uint16_t error_code, const char *error_msg) {
#ifdef DEBUG
  LOG_DEBUG("MySQLHandler::do_refuse_login\n");
#endif
  Packet error_packet;
  MySQLErrorResponse error(
      error_code,
      error_msg ? error_msg
                : "Datasource's state is abnormal or open "
                  "[reinforce-]enbale-read-only, please check and retry later",
      "HY000");
  error.pack(&error_packet);
  send_to_client(&error_packet);
}

void MySQLHandler::do_handle_request() {
#ifdef DEBUG
  LOG_DEBUG("MySQLHandler::do_handle_request\n");
#endif
#ifndef DBSCALE_TEST_DISABLE
  handler_recv_from_client.start_timing();
#endif
  Catalog *cl = backend->get_catalog();
  DataSpace *metadata_space = backend->get_metadata_data_space();
  Connection *conn = NULL;
  // Packet &packet = get_session()->get_client_packet();
  Packet packet(DEFAULT_PACKET_SIZE, session->get_packet_alloc());
  session->set_packet_number(0);
  session->set_compress_packet_number(0);

  receive_from_client(&packet);
  if (session->is_got_error_er_must_change_password()) {
    MySQLErrorResponse err(ERROR_MUST_CHANGE_PASSWORD_CODE,
                           dbscale_err_msg[ERROR_MUST_CHANGE_PASSWORD_CODE],
                           "HY000");
    Packet packet;
    err.pack(&packet);
    send_to_client(&packet);
    return;
  }

  MySQLCommand cmd = driver->get_command(&packet);
  MySQLEOFResponse eof;

  session->set_call_store_procedure(false);
  // session->clear_all_sp_in_remove_sp_vector();
  session->set_drop_current_schema(false);
  session->reset_transfer_rule_manager();
  session->set_audit_log_flag_local();
  session->set_has_do_audit_log(false);

#ifndef DBSCALE_TEST_DISABLE
  handler_recv_from_client.end_timing_and_report();
#endif
  if (session->is_event_add_failed()) {
    session->sub_traditional_num();
  }
  if (net_status) {
    session->set_net_in(packet.length());
  }

  {
    ACE_Guard<ACE_Thread_Mutex> guard(handler_trx_timing_stop_mutex);
    if (get_trx_timing_stop() && cmd != MYSQL_COM_QUIT) {
      set_trx_timing_stop(false);
      end_transaction_timing();
      MySQLErrorResponse err(
          ERROR_TRANSACTION_FAILED_CODE,
          "The transaction is timeout since executing time is greater than "
          "transaction-executing-timeout",
          "HY000");
      Packet packet;
      err.pack(&packet);
      send_to_client(&packet);
      LOG_DEBUG("Session quit since transaction is timeout\n");
      throw HandlerQuit();
    }
  }

  switch (cmd) {
    case MYSQL_COM_QUIT:
      session->rollback();
      set_keep_conn(session->is_keeping_connection());
      LOG_DEBUG("Command quit\n");
      throw HandlerQuit();
    case MYSQL_COM_QUERY:
      handle_query_command(&packet);
      break;
    case MYSQL_COM_FIELD_LIST:
      LOG_DEBUG("command field list\n");
      try {
        conn = send_to_server_retry(metadata_space ? metadata_space : cl,
                                    &packet, NULL, false, false, false);
        receive_from_server(conn, &packet);
        send_to_client(&packet);
        if (driver->is_error_packet(&packet)) {
          MySQLErrorResponse error(&packet);
          error.unpack();
          LOG_DEBUG("command field list failed: %d (%s) %s\n",
                    error.get_error_code(), error.get_sqlstate(),
                    error.get_error_message());
          break;
        }
        while (!driver->is_eof_packet(&packet)) {
          receive_from_server(conn, &packet);
          if (session->need_all_dbscale_column()) {
            send_to_client(&packet);
          } else if (!driver->is_eof_packet(&packet) &&
                     !driver->is_error_packet(&packet)) {
            if (!ignore_dbscale_column_packet(&packet)) send_to_client(&packet);
          } else {
            send_to_client(&packet);
          }
        }
        put_back_connection(metadata_space ? metadata_space : cl, conn);
        conn = NULL;
      } catch (...) {
        LOG_ERROR("Error occured when execute MYSQL_COM_FIELD_LIST.\n");
        if (conn) {
          clean_dead_conn(&conn, metadata_space ? metadata_space : cl);
          conn = NULL;
        }
        throw;
      }
      break;
    case MYSQL_COM_INIT_DB: {
      char schema_name[SCHEMA_LEN + 1];
      copy_string(schema_name, driver->get_query(&packet), sizeof(schema_name));
      LOG_DEBUG("command init db %s.\n", schema_name);
      string schema_name_string = string(schema_name);
      if (enable_acl && !session->is_contain_schema(schema_name_string)) {
        LOG_DEBUG("authenticate_db does not contain [%s]\n", schema_name);
        // AuthenticateDeniedError error(schema_name);
        string err_msg = "Access denied for user to database '";
        err_msg += schema_name;
        err_msg += "'. authenticate_db does not contain it.";
        dbscale::sql::SQLError error(err_msg.c_str(), "42000",
                                     ERROR_AUTH_DENIED_CODE);
        handle_exception(&error, true, error.get_errno(), error.what());
        break;
      }
      DataSpace *space = backend->get_data_space_for_table(schema_name, NULL);
      try {
        conn = send_to_server_retry(space, &packet, schema_name, false, false,
                                    false);
        receive_from_server(conn, &packet);
        send_to_client(&packet);
      } catch (Exception &e) {
        LOG_ERROR("Error occured when execute MYSQL_COM_INIT_DB.\n");
        if (conn) {
          clean_dead_conn(&conn, space);
          conn = NULL;
        }
        handle_exception(&e, true, e.get_errno(), e.what());
        break;
      }
      if (!driver->is_error_packet(&packet)) {
        conn->set_schema(schema_name);
        session->set_schema(schema_name);
        LOG_DEBUG("change current db to %s.\n", schema_name);
        BackendServerVersion v =
            Backend::instance()->get_backend_server_version();
        if (!(v == MYSQL_VERSION_57 || v == MYSQL_VERSION_8)) {
          if (enable_xa_transaction &&
              session->get_session_option("close_session_xa").int_val == 0 &&
              session->check_for_transaction() &&
              !session->is_in_transaction_consistent()) {
            string use_sql = "USE `";
            use_sql += schema_name;
            use_sql += "`;";
            XA_helper *xa_helper = driver->get_xa_helper();
            xa_helper->record_xa_redo_log(session, space, use_sql.c_str(),
                                          true);
          }
        }
      } else {
        MySQLErrorResponse error(&packet);
        error.unpack();
        LOG_ERROR("SHOW init db failed: %d (%s) %s\n", error.get_error_code(),
                  error.get_sqlstate(), error.get_error_message());
      }
      if (conn) {
        put_back_connection(space, conn);
        conn = NULL;
      }

      break;
    }
    case MYSQL_COM_SHUTDOWN:
      LOG_INFO("command shutdown\n");
      eof.pack(&packet);
      deal_autocommit_with_ok_eof_packet(&packet);
      send_to_client(&packet);
      stop_dbscale();
      break;
    case MYSQL_COM_PREPARE:
      handle_prepare_command(&packet);
      break;
    case MYSQL_COM_LONG_DATA:
      handle_long_data_command(&packet);
      break;
    case MYSQL_COM_RESET_STMT:
      handle_reset_stmt_command(&packet);
      break;
    case MYSQL_COM_EXECUTE:
      handle_execute_command(&packet);
      break;
    case MYSQL_COM_CLOSE_STMT:
      handle_close_command(&packet);
      break;
    case MYSQL_COM_SET_OPTION: {
#ifdef DEBUG
      MySQLSetRequest ret_request(&packet);
      ret_request.unpack();
      uint16_t mul_stmt_option = ret_request.use_mul_statement;
      LOG_DEBUG("Get MYSQL_COM_SET_OPTION with value %d.\n", mul_stmt_option);
#endif
      LOG_DEBUG(
          "Ignore MYSQL_COM_SET_OPTION command and return OK packet "
          "directly.\n");
      send_ok_packet_to_client(this, 0, 0);
      break;
    }
    case MYSQL_COM_CHANGE_USER: {
      LOG_DEBUG(
          "Ignore MYSQL_COM_CHANGE_USER command and return OK packet "
          "directly.\n");
      send_ok_packet_to_client(this, 0, 0);
      break;
    }
    case MYSQL_COM_BINLOG_DUMP:
    case MYSQL_COM_BINLOG_DUMP_GTID: {
      LOG_INFO("Refuse MYSQL_COM_BINLOG_DUMP and MYSQL_COM_BINLOG_DUMP_GTID\n");
      dbscale::sql::SQLError error("NotSupport binlog dump related command",
                                   "42000", ERROR_UNSUPPORT_SQL_CODE);
      handle_exception(&error, true, error.get_errno(), error.what());
      break;
    }
    case MYSQL_COM_RESET_CONNECTION: {
      LOG_INFO(
          "MYSQL_COM_RESET_CONNECTION will make mysql connection reset.\n");
      // reset dbscale
      session->reset_session_to_beginning();
      // reset mysql
      map<DataSpace *, Connection *> kept_conns =
          session->get_kept_connections();
      map<DataSpace *, Connection *>::iterator it = kept_conns.begin();
      for (; it != kept_conns.end(); it++) {
        Connection *cur_conn = NULL;
        try {
          Packet cur_packet(DEFAULT_PACKET_SIZE, session->get_packet_alloc());
          cur_conn = send_to_server_retry(it->first, &packet, NULL, false,
                                          false, false);
          receive_from_server(cur_conn, &cur_packet);
          if (driver->is_error_packet(&cur_packet)) {
            MySQLErrorResponse error(&packet);
            error.unpack();
            LOG_INFO("command %d get error packet: %d (%s) %s\n", (int)cmd,
                     error.get_error_code(), error.get_sqlstate(),
                     error.get_error_message());
          }
          session->remove_using_conn(cur_conn);
        } catch (...) {
          LOG_ERROR("Error occured while sending reset request to mysql.\n");
          if (cur_conn) {
            clean_dead_conn(&cur_conn, it->first);
            cur_conn = NULL;
          }
          send_to_client(&packet);
          throw;
        }
      }
      send_to_client(&packet);
      break;
    }
    default:
      LOG_DEBUG("command %d\n", driver->get_command(&packet));
      try {
        conn = send_to_server_retry(cl, &packet, NULL, false, false, false);
        receive_from_server(conn, &packet);
        if (driver->is_error_packet(&packet)) {
          MySQLErrorResponse error(&packet);
          error.unpack();
          LOG_INFO("command %d get error packet: %d (%s) %s\n", (int)cmd,
                   error.get_error_code(), error.get_sqlstate(),
                   error.get_error_message());
        }
        send_to_client(&packet);
        conn->reset();
      } catch (...) {
        LOG_ERROR("Error occured when execute default command.\n");
        if (conn) {
          clean_dead_conn(&conn, cl);
          conn = NULL;
        }
        throw;
      }
      break;
  }
  get_session()->flush_session_sql_cache(false);
  if (conn) put_back_connection(cl, conn);
}

void MySQLHandler::do_begin_session() {
#ifdef DEBUG
  LOG_DEBUG("MySQLHandler::do_begin_session\n");
#endif
  get_session()->set_handler(this);
}
void MySQLHandler::do_end_session() {
#ifdef DEBUG
  LOG_DEBUG("MySQLHandler::do_end_session\n");
#endif
}

void MySQLHandler::clean_tmp_table_in_transaction(
    CrossNodeJoinTable *tmp_table) {
  Session *s = get_session();
  if (!s) return;
  if (!tmp_table->is_clean) {
    Backend *backend = Backend::instance();
    DataSpace *dataspace = NULL;
    dataspace = backend->get_data_space_for_table(
        TMP_TABLE_SCHEMA, tmp_table->table_name.c_str());
    if (!dataspace) {
      LOG_DEBUG("The tmp table [%s] has been removed.\n",
                tmp_table->table_name.c_str());
    }
    DataSource *ds_tmp = dataspace->get_data_source();
    DataSpace *par_space = NULL;
    string sql("TRUNCATE TABLE dbscale_tmp.");
    sql.append(tmp_table->table_name);
    Packet exec_packet;
    MySQLQueryRequest query(sql.c_str());
    query.set_sql_replace_char(s->get_query_sql_replace_null_char());
    query.pack(&exec_packet);
    TimeValue timeout_recv = TimeValue(backend_sql_net_timeout);

    if (ds_tmp) {
      Connection *conn = NULL;
      try {
        Connection *conn = send_to_server_retry(dataspace, &exec_packet);
        Packet recv_packet;
        receive_from_server(conn, &recv_packet, &timeout_recv);
        if (driver->is_error_packet(&recv_packet)) {
          MySQLErrorResponse error(&recv_packet);
          error.unpack();
          LOG_ERROR("clean tmp_table get error packet: %d (%s) %s\n",
                    error.get_error_code(), error.get_sqlstate(),
                    error.get_error_message());
          throw Error(
              "Fail to clean cross node join tmp table in transaction.");
        }
        conn->reset();
        put_back_connection(dataspace, conn);
      } catch (...) {
        LOG_ERROR("Error occured when clean tmp_table.\n");
        if (conn) {
          clean_dead_conn(&conn, dataspace);
          conn = NULL;
        }
        throw;
      }
    } else {
      PartitionedTable *tab = NULL;
      bool is_dup = false;
      if (((Table *)dataspace)->is_duplicated()) {
        tab = ((DuplicatedTable *)dataspace)->get_partition_table();
        is_dup = true;
      } else {
        tab = (PartitionedTable *)dataspace;
      }
      unsigned int partition_num = tab->get_real_partition_num();
      for (unsigned int par_num = 0; par_num < partition_num; ++par_num) {
        if (ACE_Reactor::instance()->reactor_event_loop_done()) break;
        par_space = tab->get_partition(par_num);
        if (par_space->get_virtual_machine_id() > 0 && is_dup) continue;
        if (par_space->get_virtual_machine_id() > 0) {
          string old_table_name = tmp_table->table_name;
          string new_name_tmp;
          string new_sql_tmp = sql;
          adjust_tmp_shard_table(old_table_name.c_str(), new_name_tmp,
                                 par_space->get_virtual_machine_id(),
                                 par_space->get_partition_id());
          replace_tmp_table_name(new_sql_tmp, old_table_name, new_name_tmp);

          Connection *conn = NULL;
          try {
            MySQLQueryRequest query(new_sql_tmp.c_str());
            query.set_sql_replace_char(s->get_query_sql_replace_null_char());
            query.pack(&exec_packet);
            Connection *conn = send_to_server_retry(par_space, &exec_packet);
            Packet recv_packet;
            receive_from_server(conn, &recv_packet, &timeout_recv);
            if (driver->is_error_packet(&recv_packet)) {
              MySQLErrorResponse error(&recv_packet);
              error.unpack();
              LOG_ERROR("clean tmp_table get error packet: %d (%s) %s\n",
                        error.get_error_code(), error.get_sqlstate(),
                        error.get_error_message());
              throw Error(
                  "Fail to clean cross node join tmp table in transaction.");
            }
            conn->reset();
            put_back_connection(dataspace, conn);
          } catch (...) {
            LOG_ERROR("Error occured when clean tmp_table.\n");
            if (conn) {
              clean_dead_conn(&conn, dataspace);
              conn = NULL;
            }
            throw;
          }
        } else {
          Connection *conn = NULL;
          try {
            Connection *conn = send_to_server_retry(par_space, &exec_packet);
            Packet recv_packet;
            receive_from_server(conn, &recv_packet, &timeout_recv);
            if (driver->is_error_packet(&recv_packet)) {
              MySQLErrorResponse error(&recv_packet);
              error.unpack();
              LOG_ERROR("clean tmp_table get error packet: %d (%s) %s\n",
                        error.get_error_code(), error.get_sqlstate(),
                        error.get_error_message());
              throw Error(
                  "Fail to clean cross node join tmp table in transaction.");
            }
            conn->reset();
            put_back_connection(dataspace, conn);
          } catch (...) {
            LOG_ERROR("Error occured when clean tmp_table.\n");
            if (conn) {
              clean_dead_conn(&conn, dataspace);
              conn = NULL;
            }
            throw;
          }
        }
      }
    }
  }
}

void MySQLHandler::forecast_keep_conn_state(
    Statement *stmt, Session *s, bool *lock_state, bool *transaction_state,
    bool *auto_commit, bool *global_flush_read_lock_state) {
  stmt_type type = stmt->get_stmt_node()->type;
  bool in_lock = s->is_in_lock();
  bool in_transaction = s->is_in_transaction();
  *global_flush_read_lock_state = s->get_has_global_flush_read_lock();
  *auto_commit = false;

  switch (type) {
    case STMT_LOCK_TB: {
      in_lock = true;
      break;
    }
    case STMT_START_TRANSACTION: {
      in_transaction = true;
      s->set_trx_specify_read_only(
          stmt->get_stmt_node()->sql->start_tran_oper->specify_read_only);
      in_lock = false;
      break;
    }
    case STMT_UNLOCK_TB: {
      in_lock = false;
      *global_flush_read_lock_state = false;
      break;
    }
    case STMT_FLUSH_TABLE_WITH_READ_LOCK: {
      *global_flush_read_lock_state = true;
      break;
    }
    case STMT_ROLLBACK: {
      RollbackType rollback_type =
          stmt->get_stmt_node()->sql->rollback_point_oper->type;
      if (rollback_type == ROLLBACK_TYPE_ALL) {
        in_transaction = false;
      }
      break;
    }
    case STMT_COMMIT: {
      in_transaction = false;
      break;
    }
    case STMT_SET: {
      if (stmt->is_partial_parsed()) {
        break;
      }

      string value = s->get_stmt_auto_commit_value();
      if (value.length()) {  // this stmt has autocommit session var
        if (is_on(value)) {
          if (!s->get_auto_commit_is_on()) {
            in_transaction = false;
            *auto_commit = true;
          }
        }
      }
    } break;
    default: {
      // when autocommit=0, when execute implict commit sql such as create and
      // lock. not set set in_transaction true.
      // if stmt_type cluster xa stmt, we forecast it be the old session state
      if (!s->get_auto_commit_is_on() && !is_implict_commit_sql(type) &&
          !is_stmt_type_of_cluster_xa(type)) {
#ifdef DEBUG
        LOG_DEBUG("auto is 0.\n");
#endif
        in_transaction = true;
      }
    } break;
  }
  *lock_state = in_lock;
  *transaction_state = in_transaction;
}

bool MySQLHandler::do_check_keep_conn_beforehand(Statement *stmt, Session *s) {
  // when session is in cluster xa transaction, we ignore check keep conn, so,
  // we don't think about session in xa transaction here.
  if (is_autocommit_on() && stmt->is_partial_parsed())
    return s->is_keeping_connection() || session->check_for_transaction();

  bool lock_state = false;
  bool transaction_state = false;
  bool auto_commit = false;
  bool has_global_flush_read_lock = false;

  forecast_keep_conn_state(stmt, s, &lock_state, &transaction_state,
                           &auto_commit, &has_global_flush_read_lock);

  if (transaction_state && !s->get_is_surround_sql_with_trx_succeed()) {
    start_transaction_timing();
  } else if (!transaction_state) {
    end_transaction_timing();
  }
  return lock_state || transaction_state || has_global_flush_read_lock;
}

bool MySQLHandler::do_need_to_keep_conn(Statement *stmt, Session *s) {
  // when session is in cluster xa transaction, we ignore check keep conn, so,
  // we don't think about session in xa transaction here.
  if (is_autocommit_on() && stmt->is_partial_parsed())
    return s->is_keeping_connection() || session->check_for_transaction();

  bool lock_state = false;
  bool transaction_state = false;
  bool auto_commit = false;
  bool has_global_flush_read_lock = false;

  forecast_keep_conn_state(stmt, s, &lock_state, &transaction_state,
                           &auto_commit, &has_global_flush_read_lock);

  stmt_type type = stmt->get_stmt_node()->type;
  switch (type) {
    case STMT_PREPARE: {
      break;
    }
    case STMT_DROP_PREPARE: {
      break;
    }
    case STMT_UNLOCK_TB: {
      if (s->is_in_lock()) {
        if (session->check_for_transaction()) {
          deal_with_transaction("COMMIT;");
        }
        // unlock will do implict commit when session in lock state.
        // then session's transaction will be false.
        // the value of transaction_state has expired
        // so reset transaction_state
        transaction_state = s->is_in_transaction();
      }
      s->remove_all_lock_tables();
      break;
    }
    case STMT_ROLLBACK: {
      if (!enable_xa_transaction ||
          s->get_session_option("close_session_xa").int_val != 0) {
        RollbackType rollback_type =
            stmt->get_stmt_node()->sql->rollback_point_oper->type;
        if (rollback_type == ROLLBACK_TYPE_ALL) {
          s->remove_all_savepoint();
        } else if (rollback_type == ROLLBACK_TYPE_TO_POINT) {
          s->remove_savepoint(
              stmt->get_stmt_node()->sql->rollback_point_oper->point);
        }
      }
      break;
    }
    case STMT_COMMIT: {
      if (!enable_xa_transaction ||
          s->get_session_option("close_session_xa").int_val != 0)
        s->remove_all_savepoint();
      break;
    }
    case STMT_SET: {
      if (auto_commit) {
        deal_with_transaction("COMMIT;");
      }
      break;
    }
    default:
      break;
  }
  if (s->get_has_global_flush_read_lock() != has_global_flush_read_lock)
    s->set_has_global_flush_read_lock(has_global_flush_read_lock);
  if (s->is_in_lock() != lock_state) s->set_in_lock(lock_state);
  if (s->is_in_transaction() != transaction_state) {
    if (!(stmt->get_stmt_node()->type == STMT_START_TRANSACTION &&
          stmt->get_stmt_node()->sql->start_tran_oper &&
          stmt->get_stmt_node()->sql->start_tran_oper->with_consistence)) {
      if (enable_xa_transaction &&
          s->get_session_option("close_session_xa").int_val == 0) {
        if (!transaction_state) {
          s->reset_seq_id();
          s->set_xa_id((unsigned long)0);
        }
        /*if session get_xa_id is 0, should start_xa_transaction*/
        if (transaction_state && s->get_xa_id() == 0) {
          Driver *driver = Driver::get_driver();
          XA_helper *xa_helper = driver->get_xa_helper();
          xa_helper->start_xa_transaction(s);
        }
      }
      if (transaction_state) {
        /*If session go into transaction state from non-transation state, and
         * autocommit is 1, dbscale should send "begin" to all kept conn to
         * start transaction.
         *
         *  If autocommit=0, we also should do it, cause the autocommit is
         *  maintained by dbscale, and will not be sent to backend mysql, so all
         *  connections of backend mysql should be autocommit=1, so we should
         *  send "begin" to them, except the cur_stmt_conns of the stmt just
         *  after autocommit=0.*/
        /*The start transaction with consistent snapshot is an exception, which
         * has already execute the "start transaction with consistent snapshot"
         * for the kept conns, and should not execute begin again.*/
        map<DataSpace *, Connection *>::iterator it;
        map<DataSpace *, Connection *> kept_conns = s->get_kept_connections();
        LOG_DEBUG(
            "Session go into transaction state from non-transaction state, so "
            "send 'begin' to kept connections.\n");
        if (!is_autocommit_on()) {
          set<Connection *> cur_stmt_conns = s->get_cur_stmt_conns();
          for (it = kept_conns.begin(); it != kept_conns.end(); it++) {
            if (!cur_stmt_conns.count(it->second)) {
              /*Cause these conns has start the transaction during the execution
               * after "autocommit=0", so should skip these connections.*/
              execute_begin(it->first, it->second, NULL, false);
              LOG_DEBUG(
                  "Get a connection [%@] from kept connection for execution.\n",
                  it->second);
            }
          }
        } else {
          for (it = kept_conns.begin(); it != kept_conns.end(); it++) {
            execute_begin(it->first, it->second, NULL, false);
            LOG_DEBUG(
                "Get a connection [%@] from kept connection for execution.\n",
                it->second);
          }
        }
      }
    }
    s->set_in_transaction(transaction_state);
  }

  return s->is_keeping_connection();
}
void MySQLHandler::do_deal_with_set(Statement *stmt, Session *s) {
  if (stmt->is_partial_parsed()) {
    LOG_ERROR("The sql [%s] is partial parsed, the value will not be saved.\n",
              stmt->get_sql());
    return;
  }

  string value = s->get_stmt_auto_commit_value();
  if (value.length()) {
    bool on = is_on(value);
    if (on) {
      session->set_auto_commit_is_on(true);
    } else {
      session->set_auto_commit_is_on(false);
    }
  }
  s->set_session_var_map_md5(calculate_md5(s->get_session_var_map()));
}

void MySQLHandler::do_deal_with_transaction(const char *sql) {
  bool session_allow_dot_in_ident =
      session->get_session_option("allow_dot_in_ident").int_val;
  MySQLParser *parser = MySQLParser::instance();
  MySQLStatement *stmt = static_cast<MySQLStatement *>(
      parser->parse(sql, session_allow_dot_in_ident, true, NULL, NULL, NULL,
                    session->get_client_charset_type()));
  MySQLExecutePlan plan(stmt, session, driver, this);
  session->set_top_stmt(stmt);
  session->set_top_plan(&plan);
  ExecuteNode *node = NULL;
  if (session->is_in_transaction()) session->get_status()->item_inc(TIMES_TXN);
  if (enable_xa_transaction &&
      session->get_session_option("close_session_xa").int_val == 0 &&
      !session->is_in_transaction_consistent())
    node = plan.get_xatransaction_node(sql, false);
  else
    node = plan.get_transaction_unlock_node(sql, false);
  try {
    plan.set_start_node(node);
    plan.execute();
  } catch (...) {
    LOG_ERROR("Get exception for MySQLHandler::do_deal_with_transaction.\n");
    session->remove_top_stmt();
    session->remove_top_plan();
    stmt->free_resource();
    delete stmt;
    stmt = NULL;
    throw;
  }
  session->remove_top_stmt();
  session->remove_top_plan();
  stmt->free_resource();
  delete stmt;
  stmt = NULL;
}

void MySQLHandler::fin_alter_table(stmt_type type, stmt_node *st) {
  if (type == STMT_CREATE_TB || type == STMT_ALTER_TABLE ||
      type == STMT_TRUNCATE) {
    bool need_clear_config_source_inc_info = true;
    if (type == STMT_ALTER_TABLE) need_clear_config_source_inc_info = false;
    string alter_table_name;
    alter_table_name = session->get_alter_table_name();
    if (!alter_table_name.empty()) {
      session->reset_alter_table_name();
      if (multiple_mode &&
          (st->enable_alter_table_sync_stmt || st->modify_column) &&
          !(type == STMT_CREATE_TB &&
            st->sql->create_tb_oper->part_table_def_start_pos > 0) &&
          !(type == STMT_CREATE_TB &&
            session->get_session_option("auto_space_level").uint_val ==
                AUTO_SPACE_TABLE)) {
        /*For slave role dbscale, skip the deal_with_metadata, which has been
         * done on the master role dbscale.*/
        if (MultipleManager::instance()->get_is_cluster_master()) {
          Backend *backend = Backend::instance();
          int r = backend->start_alter_table_sync(alter_table_name);
          if (r != 0) {
            if (r != -1)
              backend->end_alter_table(alter_table_name.c_str(), false);
            LOG_ERROR(
                "CREATE TB or ALTER TB fail due to multiple dbscale sync\n");
            return;
          }
          Backend::instance()->fin_alter_table(
              alter_table_name.c_str(), need_clear_config_source_inc_info);
        }
      }
    }
  }
}

void MySQLHandler::deal_with_metadata_execute(stmt_type type, const char *sql,
                                              const char *schema,
                                              stmt_node *st) {
  if (st->execute_on_auth_master) {
    LOG_INFO("cur stmt %s is execute_on_auth_master, so ignore it.\n", sql);
    return;
  }

  if (!strcasecmp(schema, "oracle_sequence_db")) {
    LOG_INFO(
        "cur stmt %s is drop or create on oracle_sequence_db, so ignore it.\n",
        sql);
    return;
  }
  if (st->is_create_or_drop_temp_table) {
    LOG_INFO(
        "Skip the create/drop temp table for meta data maintain for sql[%s].\n",
        sql);
    return;
  }

  if (type == STMT_CREATE_TB || type == STMT_DROP_TB) {
    record_scan *rs_tmp = st->scanner;
    table_link *tl = rs_tmp->first_table;
    if (tl->join->schema_name) schema = tl->join->schema_name;
  }

  if (Backend::instance()->need_deal_with_metadata(type, st)) {
    if (session->get_cur_stmt_execute_fail()) {
      LOG_INFO(
          "cur stmt %s is execute fail, so ignore the meta data maintain.\n",
          sql);
      return;
    }
    const char *check_reserved_schema = schema;
    switch (type) {
      case STMT_CREATE_DB: {
        check_reserved_schema = st->sql->create_db_oper->dbname->name;
        if (!strcasecmp(st->sql->create_db_oper->dbname->name, "dbscale")) {
          // ignore the metadata operation for dbscale schema, cause it has
          // already in metadata source.
          return;
        }
        break;
      }
      case STMT_DROP_DB: {
        check_reserved_schema = st->sql->drop_db_oper->dbname->name;
        if (!strcasecmp(st->sql->drop_db_oper->dbname->name, "dbscale")) {
          // ignore the metadata operation for dbscale schema, cause it has
          // already in metadata source.
          return;
        }
        if (enable_event) {
          DataSpace *dataspace =
              backend->get_data_space_for_table(schema, NULL);
          if (!dataspace) {
            string err_msg = "Can not get dataspace for schema ";
            err_msg += schema;
            err_msg += " for delete events info when drop database.";
            LOG_ERROR("%s\n", err_msg.c_str());
            throw Error(err_msg.c_str());
          }
          string execute_sql =
              "delete from dbscale.events where event_name like '";
          execute_sql += schema;
          execute_sql += ".%';";
          Connection *conn = NULL;
          try {
            conn = dataspace->get_connection(NULL, schema, false);
            if (conn) {
              session->insert_using_conn(conn);
              if (conn->get_session_var_map_md5() !=
                  session->get_session_var_map_md5()) {
                conn->set_session_var(session->get_session_var_map());
              }
              if (session->get_var_flag()) {
                conn->set_user_var(session->get_var_sql());
                conn->reset();
              }
              conn->execute_one_modify_sql(execute_sql.c_str());
              conn->get_pool()->add_back_to_free(conn);
            } else {
              throw Error("when delete from events, fail to get connection");
            }
          } catch (Exception &e) {
            LOG_ERROR("Error occured when update metadata, due to %s.\n",
                      e.what());
            if (conn) {
              session->remove_using_conn(conn);
              clean_dead_conn(&conn, dataspace);
              conn = NULL;
            }
            throw;
          }
        }
        break;
      }
        // TODO: handle other dbscale schema ddl operation
      default:
        break;
    };
    LOG_DEBUG(
        "schema is %s with type %d before is_dbscale_reserved_schema_name.\n",
        check_reserved_schema, (int)type);
    if (is_dbscale_reserved_schema_name(check_reserved_schema)) return;
    if (!schema || !schema[0]) {
      // schema not set in session, maybe caused by drop current db or something
      // else, we use default_login_schema to connection metadatasource.
      schema = default_login_schema;
    }
    Backend *backend = Backend::instance();
    DataSpace *data_space = backend->get_metadata_data_space();
    int retry_count = 0;
    if (data_space && st->need_apply_metadata) {
      Connection *conn = NULL;
    retry:
      try {
        int check_working_time = 0;
        while (data_space->data_source->get_work_state() !=
                   DATASOURCE_STATE_WORKING &&
               check_working_time < 60) {
          check_working_time += 1;
          sleep(1);
        }
        if (check_working_time == 60) {
          conn = NULL;
          retry_count = 5;  // no need retry
          LOG_ERROR(
              "Fail to get an usable connection, because space is not "
              "working\n");
          throw Error(
              "Fail to get an usable connection, because space is not working");
        }
        conn = data_space->get_connection(get_session(), schema, false);
        if (conn) {
          string use_schema("USE `");
          use_schema.append(schema);
          use_schema.append("`");
          session->insert_using_conn(conn);
          /*In some case, such the source the dump file, there are lots of
           * session var and user var need to be executed also on meta data
           * conn, otherwise we may get error, such as Foreign key constraint
           * fail.*/
          if (conn->get_session_var_map_md5() !=
              session->get_session_var_map_md5()) {
            conn->set_session_var(session->get_session_var_map());
          }
          if (session->get_var_flag()) {
            conn->set_user_var(session->get_var_sql());
            conn->reset();
          }
#ifndef DBSCALE_TEST_DISABLE
          dbscale_test_info *test_info = session->get_dbscale_test_info();
          if (!strcasecmp(test_info->test_case_name.c_str(), "metadata") &&
              !strcasecmp(test_info->test_case_operation.c_str(),
                          "get_error")) {
            LOG_ERROR("Test metadata get error situation.\n");
            throw Error("Test metadata get error situation.");
          }
#endif
          if (type != STMT_CREATE_DB) {
            // for create database, there is no need to first do use db
            conn->execute_one_modify_sql(use_schema.c_str());
            conn->set_schema(schema);
          }
          if (type == STMT_DROP_DB || type == STMT_ALTER_DB)
            backend->clear_schema_cross_node_join_sqls(schema);
          else if (type == STMT_ALTER_TABLE || type == STMT_DROP_TB) {
            record_scan *rs_tmp = st->scanner;
            table_link *table = rs_tmp->first_table;
            do {
              string schema_name =
                  table->join->schema_name ? table->join->schema_name : schema;
              string table_name = table->join->table_name;
              backend->clear_table_cross_node_join_sqls(schema_name,
                                                        table_name);
              table = table->next;
            } while (table != NULL);
          }

          conn->execute_one_modify_sql_with_sqlerror(sql);
          session->remove_using_conn(conn);
          conn->get_pool()->add_back_to_free(conn);
          LOG_DEBUG("Execute meta sql %s.\n", sql);
        }
      } catch (dbscale::sql::SQLError &e) {
        if (conn) {
          session->remove_using_conn(conn);
          conn->get_pool()->add_back_to_dead(conn);
        }
        conn = NULL;
        if (run_ddl_retry && retry_count &&
            backend->check_ddl_return_retry_ok(type, st, e.get_error_code())) {
          LOG_DEBUG("after retry, ddl statement run success \n");
        }
        if (retry_count == 0) {
          get_session()->get_status()->item_inc(TIMES_FAIL_STORE_METADATA);
          Backend *backend = Backend::instance();
          backend->add_metadata_error_sqls(sql);
          string sql_trim = string(sql, 0, 80);
          backend->record_latest_important_dbscale_warning(
              "Failed to add metadata, sql is [%s]", sql_trim.c_str());
          LOG_ERROR("Failed to add metadata, sql is [%s] and type=[%d]\n", sql,
                    type);
          throw;
        }
      } catch (...) {
        if (conn) {
          session->remove_using_conn(conn);
          conn->get_pool()->add_back_to_dead(conn);
        }
        conn = NULL;
        if (run_ddl_retry && retry_count < 5) {
          retry_count++;
          sleep(monitor_interval + 1);
          goto retry;
        }
        get_session()->get_status()->item_inc(TIMES_FAIL_STORE_METADATA);
        Backend *backend = Backend::instance();
        backend->add_metadata_error_sqls(sql);
        string sql_trim = string(sql, 0, 80);
        backend->record_latest_important_dbscale_warning(
            "Failed to add metadata, sql is [%s]", sql_trim.c_str());
        LOG_ERROR("Failed to add metadata, sql is [%s] and type=[%d]\n", sql,
                  type);
        throw;
      }
    }
  }
}

void MySQLHandler::deal_with_metadata_final(stmt_type type, const char *sql,
                                            const char *schema, stmt_node *st) {
  if (st->is_create_or_drop_temp_table) {
    LOG_INFO(
        "Skip the create/drop temp table for meta data final maintain for "
        "sql[%s].\n",
        sql);
    return;
  }
  ACE_UNUSED_ARG(schema);
  ACE_UNUSED_ARG(sql);
  fin_alter_table(type, st);
}

void MySQLHandler::do_deal_with_transaction_error(stmt_type type) {
  Error e(dbscale_err_msg[ERROR_TRANSACTION_FAILED_CODE],
          ERROR_TRANSACTION_FAILED_CODE);
  if (type == STMT_TRUNCATE || type == STMT_ALTER_TABLE ||
      type == STMT_CREATE_TB || type == STMT_DROP_DB || type == STMT_ALTER_DB ||
      type == STMT_DROP_TB || type == STMT_CREATE_DB || type == STMT_LOCK_TB ||
      type == STMT_CREATE_VIEW || type == STMT_DROP_VIEW ||
      type == STMT_CREATE_SELECT || type == STMT_CREATE_LIKE ||
      type == STMT_START_TRANSACTION || type == STMT_RENAME_TABLE ||
      type == STMT_COMMIT || type == STMT_ROLLBACK) {
    handle_exception(&e, true, e.get_errno(),
                     "Execute fail, and transaction has been rollback.");
    session->set_transaction_error(false);
  } else {
    handle_exception(&e, true, e.get_errno(), e.what());
  }
}

int64_t MySQLHandler::do_get_part_table_next_insert_id(
    const char *schema_name, const char *table_name,
    PartitionedTable *part_space) {
  ACE_UNUSED_ARG(part_space);
  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);

  TableInfo *ti =
      TableInfoCollection::instance()->get_table_info_for_read(full_table_name);
  int64_t val;
  try {
    val = ti->element_table_auto_inc_val->get_element_int64_t(get_session());
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info auto inc val(int64_t) of "
        "table [%s.%s]\n",
        schema_name, table_name);
    ti->release_table_info_lock();
    throw;
  }
  ti->release_table_info_lock();

  return val;
}

void MySQLHandler::do_get_table_column_count_name(
    const char *schema_name, const char *table_name,
    PartitionedTable *part_space) {
  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);

  TableInfo *ti =
      TableInfoCollection::instance()->get_table_info_for_read(full_table_name);
  vector<TableColumnInfo> *column_info_vec;
  try {
    column_info_vec =
        ti->element_table_column->get_element_vector_table_column_info(
            get_session());
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info column "
        "info(vector_table_column_info) of table [%s]\n",
        full_table_name.c_str());
    ti->release_table_info_lock();
    throw;
  }

  ACE_RW_Mutex *tc_lock = part_space->get_table_columns_lock(full_table_name);
  tc_lock->acquire_write();

  part_space->clear_table_columns(full_table_name);
  vector<TableColumnInfo>::iterator it;
  for (it = column_info_vec->begin(); it != column_info_vec->end(); it++) {
    part_space->add_table_columns(full_table_name, (*it).column_name);
  }

  tc_lock->release();
  ti->release_table_info_lock();
}

bool MySQLHandler::check_auto_inc_for_temp_table(const char *schema_name,
                                                 const char *table_name,
                                                 DataSpace *dataspace) {
  bool ret = false;
  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);
  auto *conn = get_session()->get_kept_connection(dataspace);
  if (conn) {
    string show_create_sql = "SHOW CREATE TABLE ";
    show_create_sql += schema_name;
    show_create_sql += ".";
    show_create_sql += table_name;
    vector<string> ret_result;
    Statement *stmt = nullptr;
    try {
      conn->query_for_one_column(show_create_sql.c_str(), 1, &ret_result, 0);
      if (ret_result.size() == 1) {
        stmt = MySQLParser::instance()->parse(ret_result.front().c_str());
        if (stmt->get_stmt_node()->sql->create_tb_oper->has_auto_inc_column) {
          Backend::instance()->add_auto_increment_info(full_table_name, true);
          LOG_DEBUG("Table [%s.%s] has auto_increment field.\n", schema_name,
                    table_name);
          ret = true;
        } else {
          Backend::instance()->add_auto_increment_info(full_table_name, false);
          LOG_DEBUG("Table [%s.%s] has no auto_increment field.\n", schema_name,
                    table_name);
        }
        if (stmt) {
          stmt->free_resource();
          delete stmt;
        }
      }
    } catch (std::exception &e) {
      if (stmt) {
        stmt->free_resource();
        delete stmt;
      }
      LOG_ERROR("show create temporary table %s.%s error: %s\n", schema_name,
                table_name, e.what());
      throw;
    }
  }
  return ret;
}

bool MySQLHandler::do_set_auto_inc_info(const char *schema_name,
                                        const char *table_name,
                                        DataSpace *dataspace,
                                        bool is_partition_table) {
  bool ret = false;
  if (get_session()->is_temp_table(schema_name, table_name)) {
    if (is_partition_table) {
      throw NotSupportedError("Not support partitioned temp table");
    }
    check_auto_inc_for_temp_table(schema_name, table_name, dataspace);
    return ret;
  }
  string full_table_name;
  splice_full_table_name(schema_name, table_name, full_table_name);
  TableInfo *ti =
      TableInfoCollection::instance()->get_table_info_for_read(full_table_name);
  int64_t auto_inc_pos;
  try {
    auto_inc_pos =
        ti->element_table_auto_inc_pos->get_element_int64_t(get_session());
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info table auto inc pos(int64_t) "
        "of table [%s.%s]\n",
        schema_name, table_name);
    ti->release_table_info_lock();
    throw;
  }
  ti->release_table_info_lock();
  if (auto_inc_pos == -1) {  // no auto_increment field
    if (is_partition_table) {
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_key_pos(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_base_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_max_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_need_check_auto_increment_value_from_server(full_table_name,
                                                            true);
    }
    Backend::instance()->add_auto_increment_info(full_table_name, false);
    LOG_DEBUG("Table [%s.%s] has no auto_increment field.\n", schema_name,
              table_name);
  } else {
    ret = true;
    TableInfo *ti = TableInfoCollection::instance()->get_table_info_for_read(
        full_table_name);
    string auto_inc_name;
    try {
      auto_inc_name =
          ti->element_table_auto_inc_name->get_element_string(get_session());
    } catch (...) {
      LOG_ERROR(
          "Error occured when try to get table info table auto inc "
          "name(string) of table [%s.%s]\n",
          schema_name, table_name);
      ti->release_table_info_lock();
      throw;
    }
    ti->release_table_info_lock();

    if (is_partition_table) {
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_key_pos(
              full_table_name, auto_inc_pos - 1);  // auto_inc_pos start from 0
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_key_name(full_table_name, auto_inc_name.c_str());
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_base_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_auto_increment_max_value(full_table_name, -1);
      (static_cast<PartitionedTable *>(dataspace))
          ->set_need_check_auto_increment_value_from_server(full_table_name,
                                                            true);
    }
    Backend::instance()->add_auto_increment_info(full_table_name, true);
    LOG_DEBUG(
        "Table [%s.%s] has auto_increment field, index = [%d], name = [%s].\n",
        schema_name, table_name, auto_inc_pos, auto_inc_name.c_str());
  }
  return ret;
}

uint64_t MySQLHandler::do_get_last_insert_id_from_server(DataSpace *dataspace,
                                                         Connection **conn,
                                                         uint64_t num,
                                                         bool set_num) {
  uint64_t ret_val;
  string sql = "SELECT LAST_INSERT_ID(";
  if (set_num) sql.append(to_string(num));
  sql.append(")");
  Packet exec_packet;
  MySQLQueryRequest query(sql.c_str());
  query.pack(&exec_packet);
  try {
    Packet packet;
    if (*conn) {
      (*conn)->reset();
      send_to_server(*conn, &exec_packet);
    } else {
      // since we need use this connection to do followed INSERT stmt,
      // so the 'read_only' parameter need be set as false.
      *conn = send_to_server_retry(dataspace, &exec_packet,
                                   get_session()->get_schema(), false);
    }
    do {
      receive_from_server(*conn, &packet);
      if (driver->is_error_packet(&packet)) {
        throw ErrorPacketException();
      }
    } while (!driver->is_eof_packet(&packet));
    receive_from_server(*conn, &packet);
    if (driver->is_error_packet(&packet)) {
      throw ErrorPacketException();
    }
    MySQLRowResponse row(&packet);
    ret_val = row.get_direct_uint(0);
    do {
      receive_from_server(*conn, &packet);
      if (driver->is_error_packet(&packet)) {
        throw ErrorPacketException();
      }
    } while (!driver->is_eof_packet(&packet));
    // check whether connection has been kept in session
    if (!get_session()->is_dataspace_keeping_connection(dataspace) &&
        get_session()->get_space_from_kept_space_map(dataspace) == NULL) {
      session->set_kept_connection(dataspace, *conn);
    }
  } catch (...) {
    LOG_ERROR("Error occured when get LAST_INSERT_ID.\n");
    if (*conn) {
      clean_dead_conn(conn, dataspace);
      *conn = NULL;
    }
    throw;
  }
  return ret_val;
}

bool MySQLHandler::get_databases(Connection *conn, bool is_super_user) {
  ACE_UNUSED_ARG(is_super_user);
  string sql = "SHOW DATABASES";
  Packet exec_packet;
  MySQLQueryRequest query(sql.c_str());
  query.pack(&exec_packet);
  try {
    Packet packet;
    conn->reset();
    send_to_server(conn, &exec_packet);
    do {
      receive_from_server(conn, &packet);
      if (driver->is_error_packet(&packet)) {
        MySQLErrorResponse error(&packet);
        error.unpack();
        LOG_ERROR("SHOW DATABASES failed: %d (%s) %s\n", error.get_error_code(),
                  error.get_sqlstate(), error.get_error_message());

        return false;
      }
    } while (!driver->is_eof_packet(&packet));
    while (1) {
      receive_from_server(conn, &packet);
      if (driver->is_error_packet(&packet)) {
        return false;
      }
      if (driver->is_eof_packet(&packet)) break;
      MySQLRowResponse row(&packet);
      uint64_t field_len = 0;
      const char *dbname = row.get_str(0, &field_len);
      string tmp_db(dbname);
      session->add_auth_schema_to_set(tmp_db);
    }
  } catch (...) {
    return false;
  }
  return true;
}

void MySQLHandler::init_sessionvar(Connection *conn) {
  vector<string> vars;
  backend->get_default_session_vars(vars);
  string sql = "SELECT ";
  if (!vars.empty()) {
    unsigned int count = 1;
    for (vector<string>::iterator it = vars.begin(); it != vars.end(); ++it) {
      sql.append("@@");
      sql.append(*it);
      if (count < vars.size()) {
        sql.append(",");
      }
      count++;
    }
  } else {
    sql = "SELECT @@TIME_ZONE";  // just do some select to check if access ok
  }
  Packet exec_packet;
  MySQLQueryRequest query(sql.c_str());
  query.pack(&exec_packet);
  try {
    Packet packet;
    vector<bool> is_num;
    conn->reset();
    send_to_server(conn, &exec_packet);
    receive_from_server(conn, &packet);
    if (driver->is_error_packet(&packet)) {
      MySQLErrorResponse error(&packet);
      error.unpack();
      LOG_ERROR("default session init sql: %s\n", sql.c_str());
      LOG_ERROR(
          "select default session variables for init failed: %d (%s) %s\n",
          error.get_error_code(), error.get_sqlstate(),
          error.get_error_message());
      if (error.get_error_code() == ERROR_MUST_CHANGE_PASSWORD_CODE) {
        session->set_got_error_er_must_change_password(true);
      }
      throw HandlerError("Init default session variables failed");
    }
    receive_from_server(conn, &packet);
    while (!driver->is_eof_packet(&packet)) {
      if (driver->is_error_packet(&packet)) {
        MySQLErrorResponse error(&packet);
        error.unpack();
        LOG_ERROR("select session variables for init failed: %d (%s) %s\n",
                  error.get_error_code(), error.get_sqlstate(),
                  error.get_error_message());
        throw HandlerError("Init default session variables failed");
      }
      MySQLColumnResponse var_col(&packet);
      var_col.unpack();
      is_num.push_back(var_col.is_number());
      receive_from_server(conn, &packet);
    }
    while (1) {
      receive_from_server(conn, &packet);
      if (driver->is_error_packet(&packet)) {
        LOG_ERROR("Get exception when init session variables.\n");
        throw HandlerError("Init default session variables failed");
      }
      if (driver->is_eof_packet(&packet)) break;
      MySQLRowResponse row(&packet);
      uint64_t field_len = 0;
      const char *result;
      int i = 0;
      for (vector<string>::iterator it = vars.begin(); it != vars.end(); ++it) {
        result = row.get_str(i, &field_len);
        string value(result, field_len);
        string tmp;
        if (!is_num[i]) {
          tmp.append("'");
          tmp.append(value);
          tmp.append("'");
        } else {
          tmp = value;
        }
        if (close_foreign_key_check &&
            strcasecmp(it->c_str(), "FOREIGN_KEY_CHECKS") == 0)
          tmp.assign("OFF");
        session->set_session_var_map(*it, tmp);
        i++;
      }
      session->set_session_var_map_md5(
          calculate_md5(session->get_session_var_map()));
      backend->init_backend_var_map_once(session->get_session_var_map(),
                                         session->get_session_var_map_md5());
    }
  } catch (exception &e) {
    LOG_ERROR("Get exception '%s' when init session variables.\n", e.what());
    throw HandlerError("Init default session variables failed");
  }
  return;
}

void MySQLHandler::do_record_affected_rows(Packet *packet) {
  if (driver->is_ok_packet(packet)) {
    MySQLOKResponse ok(packet);
    ok.unpack();
    int64_t affect_rows = ok.get_affected_rows();
    session->set_affected_rows(affect_rows);
  } else {
    session->set_affected_rows(-1);
  }
}

void *MySQLHandler::get_execute_plan(Statement *stmt) {
  return new MySQLExecutePlan(stmt, session, driver, this);
}

void MySQLHandler::unpack_param_value(char **pos, MySQLColumnType type,
                                      string &s, const char *charset) {
  switch (type) {
    case MYSQL_TYPE_TINY: {
      int8_t value = Packet::unpackuchar(pos);
      sprintf(execute_param_str_value, "%d", value);
      s = execute_param_str_value;
      break;
    }
    case MYSQL_TYPE_SHORT: {
      int16_t value = Packet::unpack2uint(pos);
      sprintf(execute_param_str_value, "%d", value);
      s = execute_param_str_value;
      break;
    }
    case MYSQL_TYPE_LONG: {
      int32_t value = Packet::unpack4uint(pos);
      sprintf(execute_param_str_value, "%d", value);
      s = execute_param_str_value;
      break;
    }
    case MYSQL_TYPE_LONGLONG: {
      int64_t value = Packet::unpack8uint(pos);
      sprintf(execute_param_str_value, "%ld", value);
      s = execute_param_str_value;
      break;
    }
    case MYSQL_TYPE_FLOAT: {
      float value = *(float *)(*pos);
      sprintf(execute_param_str_value, "%f", value);
      s = execute_param_str_value;
      (*pos) += sizeof(float);
      break;
    }
    case MYSQL_TYPE_DOUBLE: {
      double value = *(double *)(*pos);
      sprintf(execute_param_str_value, "%lf", value);
      s = execute_param_str_value;
      (*pos) += sizeof(double);
      break;
    }
    case MYSQL_TYPE_TIME: {
      uint hour = 0;
      uint minute = 0;
      uint second = 0;
      bool neg = false;
      uint64_t len = Packet::unpack_lenenc_int(pos);
      if (len >= 8) {
        neg = (bool)(*pos)[0];
        const char *to = (*pos) + 1;
        uint day = Packet::unpack4uint(const_cast<char **>(&to));
        hour = (uint)(*pos)[5] + day * 24;
        minute = (uint)(*pos)[6];
        second = (uint)(*pos)[7];

        if (hour > 838) {
          hour = 838;
          minute = 59;
          second = 59;
        }
      }
      sprintf(execute_param_str_value, "%s%02u:%02u:%02u", neg ? "-" : "", hour,
              minute, second);
      s = "'";
      s.append(execute_param_str_value);
      s += "'";
      *(pos) += len;
      break;
    }
    case MYSQL_TYPE_DATE: {
      uint year = 0;
      uint month = 0;
      uint day = 0;
      uint64_t len = Packet::unpack_lenenc_int(pos);
      if (len >= 4) {
        const char *to = *pos;
        year = Packet::unpack2uint(const_cast<char **>(&to));
        month = (uint)(*pos)[2];
        day = (uint)(*pos)[3];
      }
      sprintf(execute_param_str_value, "%04u-%02u-%02u", year, month, day);
      s = "'";
      s.append(execute_param_str_value);
      s += "'";
      (*pos) += len;
      break;
    }
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP: {
      uint year = 0;
      uint month = 0;
      uint day = 0;
      uint hour = 0;
      uint minute = 0;
      uint second = 0;
      uint64_t len = Packet::unpack_lenenc_int(pos);
      if (len >= 4) {
        const char *to = *pos;
        year = Packet::unpack2uint(const_cast<char **>(&to));
        month = (uint)(*pos)[2];
        day = (uint)(*pos)[3];
        if (len > 4) {
          hour = (uint)(*pos)[4];
          minute = (uint)(*pos)[5];
          second = (uint)(*pos)[6];
        }
      }
      sprintf(execute_param_str_value, "%04u-%02u-%02u %02u:%02u:%02u", year,
              month, day, hour, minute, second);
      s = "'";
      s.append(execute_param_str_value);
      s += "'";
      (*pos) += len;
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
    default: {
      uint64_t len = Packet::unpack_lenenc_int(pos);
      s.assign(*pos, len);
      add_translation_char_for_special_char(s, charset);
      s = "'" + s + "'";
      (*pos) += len;
    }
  }
}

int MySQLHandler::execute_replace_str(string &query,
                                      list<execute_param> *param_list) {
  char replace_char = '?';
  uint replace_count = count(query.begin(), query.end(), replace_char);
  if (!param_list || replace_count != param_list->size()) {
    LOG_ERROR("The character to replace and param value size do not match.\n");
    return 1;
  }
  string::size_type pos(0);
  string old_str = "?";
  string new_str;
  for (list<execute_param>::iterator it = param_list->begin();
       it != param_list->end(); it++) {
    new_str = it->value;
    if ((pos = query.find(old_str, pos)) != string::npos) {
      query.replace(pos, old_str.length(), new_str);
      pos += new_str.length();
    }
  }
  LOG_DEBUG("execute_replace_str generate sql [%s]\n", query.c_str());
  return 0;
}

int MySQLHandler::execute_replace_str(MySQLPrepareItem *prepare_item,
                                      string &query_sql,
                                      list<execute_param> *param_list) {
  list<string> *split_prepare_sql_list = prepare_item->get_split_prepare_sql();
  list<string>::iterator it_split_prepare_sql = split_prepare_sql_list->begin();
  list<execute_param>::iterator it_val = param_list->begin();
  for (; it_val != param_list->end(); it_val++) {
    query_sql.append(*it_split_prepare_sql);
    query_sql.append(it_val->value);
    it_split_prepare_sql++;
  }
  query_sql.append(*it_split_prepare_sql);
  LOG_DEBUG("execute_replace_str generate sql [%s]\n", query_sql.c_str());
  return 0;
}

bool MySQLHandler::is_blocking_stmt(MySQLStatement *stmt) {
  ACE_UNUSED_ARG(stmt);
#ifndef DBSCALE_TEST_DISABLE
  dbscale_test_info *test_info = session->get_dbscale_test_info();
  Backend *backend = Backend::instance();
  TransferRule *rule = backend->dispatch_rule;
  DispatchStrategy *dispatch_strategy = rule->get_dispatch_strategy();
  if (!strcasecmp(test_info->test_case_name.c_str(), "multi_send_node") &&
      !strcasecmp(test_info->test_case_operation.c_str(), "blocking_thread") &&
      !strcasecmp(stmt->get_sql(), "select * FROM test_multi_send_node")) {
    rule->add_session(get_session());
    int key = (dispatch_strategy->dispatch_handler).size();
    (dispatch_strategy->dispatch_handler)[key] = this;
    get_session()->follower_session_wait();
    return true;
  } else if (!strcasecmp(test_info->test_case_name.c_str(),
                         "multi_send_node") &&
             (!strcasecmp(test_info->test_case_operation.c_str(),
                          "working_thread") ||
              !strcasecmp(test_info->test_case_operation.c_str(),
                          "working_thread_dup")) &&
             !strcasecmp(stmt->get_sql(),
                         "select * FROM test_multi_send_node")) {
    rule->add_session(get_session());
    int key = (dispatch_strategy->dispatch_handler).size();
    (dispatch_strategy->dispatch_handler)[key] = this;

    return false;
  }
#endif
  return false;
}

/* Check if current thread is a federated follower thread.
 * In cross node join, if there's partitioned table, the federated
 * tmp table on every partition will send fetch data query to dbscale,
 * we will wait till all the partition's federated tmp table has send
 * select query to dbscale, then dbscale do the transfer work, fetch
 * data from previous sub sequence and dispatch the resultset.
 */
bool MySQLHandler::is_federated_follower(MySQLStatement *stmt) {
  if (!(stmt->get_stmt_node()->type == STMT_SELECT) ||
      !stmt->check_federated_name() || stmt->check_federated_select())
    return false;
  Backend *backend = Backend::instance();
  table_link *table = stmt->get_stmt_node()->table_list_head;
  string table_name(table->join->table_name);
  CrossNodeJoinManager *cross_join_manager =
      backend->get_cross_node_join_manager();
  Session *work_session = cross_join_manager->get_CNJ_work_session(table_name);
  get_session()->set_work_session(work_session);
  TransferRule *p_transfer_rule =
      cross_join_manager->get_transfer_rule(table_name);
  bool ret = cross_join_manager->register_federated_client(table_name, this,
                                                           p_transfer_rule);
  if (!ret) {
    LOG_DEBUG("Federated follower %@ thread start to wait.\n", this);
    get_session()->follower_session_wait();
    LOG_DEBUG("Federated follower %@ thread finish wait.\n", this);
    // If the thread is waked up by a empty result set thread, make this thread
    // be a lead thread.
    if (get_session()->is_wakeup_by_empty_leader()) {
      LOG_DEBUG("Federated follower %@ is wake up by empty leader.\n", this);
      get_session()->set_transfer_rule(p_transfer_rule);
      return false;
    }
    get_session()->reinit_follower_session();
    p_transfer_rule->close();
    if (get_session()->get_work_session())
      get_session()->get_work_session()->increase_finished_federated_num();
    if (work_session->get_fe_error()) {
      LOG_ERROR("Got error when execute leader federated thread.\n");
      throw Error(
          "Got error when execute leader federated thread. Please check the "
          "log.");
    }
    return true;
  } else {
    get_session()->set_transfer_rule(p_transfer_rule);
    LOG_DEBUG("Start to execute federated leader thread.\n");
    return false;
  }
}

bool MySQLHandler::is_should_use_txn_or_throw(stmt_type type) {
  if (session->is_in_cluster_xa_transaction()) return false;
  int load_data_for_insert_select_session_val =
      session->get_session_option("use_load_data_for_insert_select").int_val;
  bool is_strict_mode_condition =
      session->is_in_lock() || session->get_has_global_flush_read_lock() ||
      ((load_data_for_insert_select_session_val == FIFO_INSERT_SELECT ||
        load_data_for_insert_select_session_val == LOAD_INSERT_SELECT) &&
       type == STMT_INSERT_SELECT);
  bool should_use_txn = is_stmt_type_of_dml(type) &&
                        session->get_auto_commit_is_on() &&
                        !session->is_in_transaction();
  if (should_use_txn && is_strict_mode_condition) {
    if (dbscale_trx_strict_mode) {
      throw NotSupportedError(
          "DML sqls are not guaranteed to be transactional safe when in lock "
          "or use_load_data_for_insert_select");
    }
    if (!dbscale_trx_strict_mode) {
      return false;
    }
  }
  return should_use_txn;
}

void MySQLHandler::try_conceal_dbscale_unsupported_flags_for_server(
    MySQLInitResponse &init, uint32_t flag, const char *flag_name) {
  if ((init.get_server_capabilities() & flag) > 0) {
#ifdef DEBUG
    LOG_DEBUG("Server's %s flag is on, DBScale set it off.\n", flag_name);
#else
    ACE_UNUSED_ARG(flag_name);
#endif
    init.set_server_capabilities(init.get_server_capabilities() & ~flag);
  }
}
void MySQLHandler::try_conceal_dbscale_unsupported_flags_for_client(
    MySQLAuthRequest &auth, uint32_t flag, const char *flag_name) {
  if ((auth.get_client_capabilities() & flag) > 0) {
#ifdef DEBUG
    LOG_DEBUG("Client's %s flag is on, DBScale set it off.\n", flag_name);
#else
    ACE_UNUSED_ARG(flag_name);
#endif
    auth.set_client_capabilities(auth.get_client_capabilities() & ~flag);
  }
}

void MySQLHandler::set_auto_commit_off_if_necessary(Packet *packet) {
  if (session->get_cluster_xa_transaction_state() == SESSION_XA_TRX_XA_PREPARED)
    return;
  if (driver->is_ok_packet(packet) &&
      !session->get_stmt_auto_commit_int_value()) {
    MySQLOKResponse ok(packet);
    ok.unpack();
    ok.set_auto_commit_is_off();
    ok.pack(packet);
  } else if (driver->is_eof_packet(packet) &&
             !session->get_stmt_auto_commit_int_value()) {
    MySQLEOFResponse eof(packet);
    eof.unpack();
    eof.set_auto_commit_is_off();
    eof.pack(packet);
  }
}

void MySQLHandler::deal_autocommit_with_ok_eof_packet(Packet *packet) {
  if (method_to_handle_autocommit_flag == HANDLE_AUTOCOMMIT_FLAG_BEFORE_SEND)
    set_auto_commit_off_if_necessary(packet);
}

bool MySQLHandler::is_dbscale_show_stmt(stmt_type type) {
  bool ret = false;
  switch (type) {
    case STMT_DBSCALE_SHOW_PARTITIONS:
    case STMT_DBSCALE_SHOW_DATASOURCE:
    case STMT_DBSCALE_SHOW_USER_STATUS:
    case STMT_DBSCALE_SHOW_USER_SQL_COUNT:
    case STMT_DBSCALE_SHOW_USER_PROCESSLIST:
    case STMT_DBSCALE_SHOW_TABLE_LOCATION:
    case STMT_DBSCALE_SHOW_BACKEND_THREADS:
    case STMT_DBSCALE_SHOW_DATASERVER:
    case STMT_DBSCALE_SHOW_PARTITION_SCHEME:
    case STMT_DBSCALE_SHOW_STATUS:
    case STMT_DBSCALE_SHOW_POOL_INFO:
    case STMT_DBSCALE_SHOW_LOCK_USAGE:
    case STMT_DBSCALE_SHOW_EXECUTION_PROFILE:
    case STMT_DBSCALE_SHOW_TRANSACTION_SQLS:
    case STMT_DBSCALE_SHOW_SHARD_PARTITION_TABLE:
    case STMT_DBSCALE_SHOW_BASE_STATUS:
    case STMT_DBSCALE_SHOW_CLIENT_TASK_STATUS:
    case STMT_DBSCALE_SHOW_SERVER_TASK_STATUS:
    case STMT_DBSCALE_SHOW_VERSION:
    case STMT_SHOW_VERSION:
    case STMT_SHOW_COMPONENTS_VERSION:
    case STMT_DBSCALE_SHOW_HOSTNAME:
    case STMT_DBSCALE_SHOW_ALL_FAIL_TRANSACTION:
    case STMT_DBSCALE_SHOW_DETAIL_FAIL_TRANSACTION:
    case STMT_DBSCALE_SHOW_PARTITION_TABLE_STATUS:
    case STMT_DBSCALE_SHOW_SCHEMA_ACL_INFO:
    case STMT_DBSCALE_SHOW_TABLE_ACL_INFO:
    case STMT_DBSCALE_SHOW_SCHEMA:
    case STMT_DBSCALE_SHOW_TABLE:
    case STMT_DBSCALE_SHOW_OPTION:
    case STMT_DBSCALE_SHOW_DYNAMIC_OPTION:
    case STMT_DBSCALE_SHOW_OUTLINE_HINT:
    case STMT_DBSCALE_SHOW_OUTLINE_INFO:
    case STMT_DBSCALE_SHOW_DEFAULT_SESSION_VARIABLES:
    case STMT_DBSCALE_SHOW_HELP:
    case STMT_DBSCALE_SHOW_VIRTUAL_MAP:
    case STMT_DBSCALE_SHOW_SHARD_MAP:
    case STMT_DBSCALE_SHOW_AUTO_INCREMENT_VALUE:
    case STMT_DBSCALE_SHOW_JOIN_STATUS:
    case STMT_DBSCALE_SHOW_USER_NOT_ALLOW_OPERATION_TIME:
    case STMT_DBSCALE_SHOW_PATH_INFO:
    case STMT_DBSCALE_SHOW_CRITICAL_ERRORS:
    case STMT_DBSCALE_SHOW_ASYNC_TASK:
    case STMT_DBSCALE_SHOW_SLOW_SQL_TOP_N:
    case STMT_DBSCALE_SHOW_CHANGED_STARTUP_CONFIG:
    case STMT_DBSCALE_SHOW_TRANSACTION_BLOCK_INFO:
    case STMT_DBSCALE_REQUEST_CLUSTER_INC_INFO:
    case STMT_DBSCALE_REQUEST_ALL_CLUSTER_INC_INFO:
    case STMT_DBSCALE_REQUEST_CLUSTER_ID:
    case STMT_DBSCALE_REQUEST_NODE_INFO:
    case STMT_DBSCALE_REQUEST_CLUSTER_INFO:
    case STMT_DBSCALE_REQUEST_USER_STATUS:
    case STMT_DBSCALE_REQUEST_SLOW_SQL_TOP_N:
    case STMT_DBSCALE_CHECK_METADATA:
    case STMT_DBSCALE_CHECK_DISK_IO: {
      ret = true;
      break;
    }
    default:
      break;
  }
  return ret;
}
