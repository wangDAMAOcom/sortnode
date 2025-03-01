     while (pos < s.size() && s.find(".", pos) != string::npos) {
          pos = s.find(".", pos);
          s.replace(pos, 1, "`.`");
          pos += 3;
        }
        new_sql.append("`").append(s).append("`, ");
      } else {
        new_sql.append(s).append(", ");
      }
      oi = oi->next;
      if (oi == rs->group_by_list) {
        new_sql.erase(new_sql.size() - 2);
        break;
      }
    }
  }
  fetch_sql_tmp = new_sql;
  LOG_DEBUG("Orginal SQL: %s\n", query_sql);
  LOG_DEBUG("    New SQL: %s\n", new_sql.c_str());

  *tail_node = bottom_node;
  return top_node;
}

void Statement::reset_avg_sql(string column_name, AvgDesc &desc) {
  string tmp_str = "";

  if (desc.count_index == 0) {
    tmp_str = "COUNT(";
    tmp_str.append(column_name);
    tmp_str += ") ";

    new_select_items.push_back(tmp_str);
    desc.count_index = --new_select_item_num;
  }
  if (desc.sum_index == 0) {
    tmp_str = "SUM(";
    tmp_str.append(column_name);
    tmp_str += ") ";

    new_select_items.push_back(tmp_str);
    desc.sum_index = --new_select_item_num;
  }
}

void init_avgdesc(AvgDesc &desc) {
  desc.count_index = 0;
  desc.avg_index = 0;
  desc.sum_index = 0;
  desc.mpf_inited = true;
  mpf_init2(desc.value, DECIMAL_STORE_BIT);
}
void release_avg_list(list<AvgDesc> &avg_list) {
  for (auto &desc : avg_list) {
    if (desc.mpf_inited) mpf_clear(desc.value);
  }
  avg_list.clear();
}

void Statement::handle_avg_function(record_scan *rs, list<AvgDesc> &avg_list) {
  AvgDesc desc;
  CurrentStatementFunctionType type;
  int field_num = 0;
  field_item *field = rs->field_list_head;
  while (field) {
    if (field->field_expr) {
      type = field->field_expr->get_cur_func_type();
      if (type == AGGREGATE_TYPE_AVG) {
        init_avgdesc(desc);
        desc.avg_index = field_num;
        string column_name = "";
        try {
          if (dynamic_cast<FunctionExpression *>(field->field_expr)) {
            ((FunctionExpression *)field->field_expr)
                ->param_list->to_string(column_name);
          } else if (ArithmeticUnaryExpression *unary_expr =
                         dynamic_cast<ArithmeticUnaryExpression *>(
                             field->field_expr)) {
            // means select -avg() ...
            if (FunctionExpression *avg_expr =
                    (dynamic_cast<FunctionExpression *>(unary_expr->right))) {
              avg_expr->param_list->to_string(column_name);
            } else {
              LOG_ERROR(
                  "Unsupport multi ArithmeticUnaryExpression before AVG "
                  "function\n");
            }
          } else {
            LOG_ERROR(
                "Unsupport complex Arithmetic expression before AVG "
                "function\n");
          }
        } catch (...) {
          if (desc.mpf_inited) mpf_clear(desc.value);
          throw;
        }
        reset_avg_sql(column_name, desc);
        avg_list.push_back(desc);
      }
    }
    ++field_num;
    field = field->next;
  }
  LOG_DEBUG("in get_avg_function avg_list.size=[%d]\n", avg_list.size());
}

bool Statement::check_field_list_invalid_distinct(record_scan *rs) {
  if (!(rs->options & SQL_OPT_DISTINCT || rs->options & SQL_OPT_DISTINCTROW))
    return false;
  bool contain_valid_field = false;
  field_item *field = rs->field_list_head;
  while (field) {
    if (field->field_expr && field->field_expr->type != EXPR_STR) {
      contain_valid_field = true;
      break;
    }
    field = field->next;
  }
  return contain_valid_field;
}

bool Statement::check_field_list_contains_invalid_function(record_scan *rs) {
  bool contain_valid_function = false;
  field_item *field = rs->field_list_head;
  while (field) {
    if (field->field_expr &&
        is_expression_contain_invaid_function(field->field_expr, rs, false)) {
      contain_valid_function = true;
      break;
    }
    field = field->next;
  }
  return contain_valid_function;
}

void Statement::handle_select_expr(record_scan *rs) {
  SelectExprDesc desc;
  int field_num = 0;
  field_item *field = rs->field_list_head;
  while (field) {
    if (!field->field_expr ||
        !is_expression_contain_aggr(field->field_expr, rs, false)) {
      field = field->next;
      ++field_num;
      continue;
    }
    need_deal_field_expr = false;
    handle_expression(field->field_expr, NULL, rs, FIELD_EXPR);
    if (need_deal_field_expr) {
      desc.index = field_num;
      desc.expr = field->field_expr;
      select_expr_list.push_back(desc);
    }
    ++field_num;
    field = field->next;
  }
}

bool Statement::has_and_rebuild_autocommit_sql(ExecutePlan *plan) {
  bool is_on = plan->session->get_auto_commit_is_on();
  const char *auto_commit_value = "0";
  if (is_on) {
    auto_commit_value = "1";
  }
  bool has_autocommit = false;
  int head_pos = 0, tail_pos = 0;
  string replace_sql;
  const char *tmp = sql_tmp.c_str();
  session_var_item *session_var_list = st.session_var_list_head;
  while (session_var_list != NULL) {
    string var_name = session_var_list->value;
    boost::to_upper(var_name);
    if (!strcmp(var_name.c_str(), "AUTOCOMMIT")) {
      replace_sql.append(tmp + tail_pos,
                         session_var_list->start_pos_at_flag - tail_pos - 1);
      head_pos = session_var_list->start_pos_at_flag;
      tail_pos = session_var_list->end_pos;
      replace_sql.append(auto_commit_value);
      if (session_var_list->rs->where_pos == 0 ||
          session_var_list->rs->where_pos >
              (unsigned int)session_var_list->start_pos_at_flag) {
        replace_sql.append(" AS '");
        replace_sql.append(tmp + head_pos - 1, tail_pos - head_pos + 1);
        replace_sql.append("'");
      }
      has_autocommit = true;
    }
    session_var_list = session_var_list->next;
  }

  if (has_autocommit) {
    replace_sql.append(tmp + tail_pos, strlen(tmp) - tail_pos);
    LOG_DEBUG("replace sql is %s.\n", replace_sql.c_str());
    sql_tmp = replace_sql;
  }

  return has_autocommit;
}

void Statement::replace_sql_function_field(
    PreviousStatementFunctionType func_type, string &sql, record_scan *rs,
    ExecutePlan *plan) {
#ifdef DEBUG
  ACE_ASSERT(func_type == FUNCTION_TYPE_ROW_COUNT ||
             func_type == FUNCTION_TYPE_LAST_INSERT_ID);
#endif

  int head_pos = 0, tail_pos = 0;
  char new_num[128];
  int64_t number = 0;
  field_item *field = rs->field_list_tail;
  switch (func_type) {
    case FUNCTION_TYPE_ROW_COUNT: {
      head_pos = rs->row_count_head_pos;
      tail_pos = rs->row_count_tail_pos;
      number = plan->session->get_affected_rows();
      field_item *tmp_field = field;
      while (tmp_field) {
        if (tmp_field->field_expr->has_function_type(FUNCTION_TYPE_ROW_COUNT)) {
          if (!tmp_field->alias) {
            sql = add_original_field(sql.c_str(), tmp_field->head_pos,
                                     tmp_field->tail_pos);
          }
        }
        tmp_field = tmp_field->pre;
      }
      break;
    }
    case FUNCTION_TYPE_LAST_INSERT_ID: {
      head_pos = rs->last_insert_id_head_pos;
      tail_pos = rs->last_insert_id_tail_pos;
      number = plan->handler->get_session()->get_last_insert_id();
      field_item *tmp_field = field;
      while (tmp_field) {
        if (tmp_field->field_expr->has_function_type(
                FUNCTION_TYPE_LAST_INSERT_ID)) {
          if (!tmp_field->alias) {
            sql = add_original_field(sql.c_str(), tmp_field->head_pos,
                                     tmp_field->tail_pos);
          }
        }
        tmp_field = tmp_field->pre;
      }
      break;
    }
    default: {
#ifdef DEBUG
      ACE_ASSERT(0);
#endif
      break;
    }
  }
  string head_str, tail_str;
  head_str.assign(sql, 0, head_pos - 1);
  tail_str.assign(sql, tail_pos, sql.length() - tail_pos);

  int len = sprintf(new_num, "%ld", number);
  head_str.append(new_num, len);

  sql.clear();
  sql.append(head_str.c_str());
  sql.append(tail_str.c_str());
}

bool Statement::is_set_password(ExecutePlan *plan) {
  if (!st.sql->set_oper || st.sql->set_oper->names) return false;

  expr_list_item *head =
      ((ListExpression *)st.sql->set_oper->set_list)->expr_list_head;
  CompareExpression *cmp = (CompareExpression *)head->expr;
  VarExpression *var_expression = (VarExpression *)cmp->left;
  if (head == head->next &&
      !strcasecmp(var_expression->str_value, "PASSWORD")) {
    if (plan->session->is_in_lock()) {
      LOG_ERROR("Forbid set password in lock table mode.\n");
      throw NotSupportedError("Not support set password in lock table mode.");
    }
    const char *com_str = NULL;
    if (st.sql->set_oper->user_name) {
      com_str = st.sql->set_oper->user_name;
    } else {
      com_str = plan->session->get_username();
    }
    if (!strcmp(dbscale_internal_user, com_str)) {
      LOG_ERROR("Not support change password for dbscale internal user.\n");
      throw NotSupportedError(
          "Not support set password for dbscale internal user.");
    }
    if (!strcmp(admin_user, com_str)) {
      LOG_ERROR("Not support change password for admin user now.\n");
      throw NotSupportedError("Not support set password for admin user now.");
    }
    Backend *backend = Backend::instance();
    map<string, DataSource *> so_map;
    backend->get_data_sources(so_map);
    map<string, DataSource *>::iterator so_it = so_map.begin();
    for (; so_it != so_map.end(); so_it++) {
      const char *kept_user = so_it->second->get_user();
      if (kept_user && !strcmp(kept_user, com_str)) {
        LOG_ERROR("Not support change password for dbscale keep user.\n");
        throw NotSupportedError(
            "Not support change password for dbscale keep user.");
      }
    }
    map<string, DataServer *> se_map;
    backend->get_data_servers(se_map);
    map<string, DataServer *>::iterator se_it = se_map.begin();
    for (; se_it != se_map.end(); se_it++) {
      const char *kept_user = se_it->second->get_user();
      if (kept_user && !strcmp(kept_user, com_str)) {
        LOG_ERROR("Not support change password for dbscale keep user.\n");
        throw NotSupportedError(
            "Not support change password for dbscale keep user.");
      }
    }

    // the SET PASSWORD stmt will be:
    // 1. SET PASSWORD=password('xxx'), the password is a function
    // 2. SET PASSWORD = 'xxx', the password is a simple string
    StrExpression *str_expr = NULL;
    Expression *expr = cmp->right;
    if (expr->type == EXPR_FUNC) {
      FunctionExpression *func_expr = (FunctionExpression *)expr;
      StrExpression *name_expr = func_expr->name;
      if (strcasecmp(name_expr->str_value, "PASSWORD") == 0) {
        ListExpression *param_list = func_expr->param_list;
        if (param_list->expr_list_head->expr->type == EXPR_STRING) {
          str_expr = (StrExpression *)(param_list->expr_list_head->expr);
        }
      }
    } else if (expr->type == EXPR_STRING) {
      str_expr = (StrExpression *)expr;
    }

    if (str_expr != NULL) {
      const char *password = str_expr->str_value;
      string err_msg;
      bool is_legal = validate_password_complex(password, err_msg);
      if (!is_legal) {
        LOG_ERROR("Password is illegal, %s.\n", err_msg.c_str());
        throw Error(err_msg.c_str());
      }
    }

    replace_set_pass_host();
    st.type = STMT_SET_PASSWORD;
    return true;
  }
  return false;
}

void Statement::deal_set_var() {
  if (!st.sql->set_oper) return;

  if (st.sql->set_oper->names) {
    uservar_list.clear();
    sessionvar_list.clear();
    sessionvar_list.push_back("CHARACTER_SET_CLIENT");
    sessionvar_list.push_back("CHARACTER_SET_RESULTS");
    string select_var_sql(
        "SELECT @@CHARACTER_SET_CLIENT, @@CHARACTER_SET_RESULTS");
    if (!st.sql->set_oper->no_character_set_connection) {
      sessionvar_list.push_back("CHARACTER_SET_CONNECTION");
      select_var_sql.append(", @@CHARACTER_SET_CONNECTION");
    }
    set_select_var_sql(select_var_sql);
    return;
  }

  uservar_list.clear();
  sessionvar_list.clear();
  expr_list_item *head = NULL, *tmp;
  head = ((ListExpression *)st.sql->set_oper->set_list)->expr_list_head;
  tmp = head;

  string uservar_sql;
  string sessionvar_sql;
  CompareExpression *cmp;
  VarExpression *var_expression;
  do {
    cmp = (CompareExpression *)tmp->expr;
    var_expression = (VarExpression *)cmp->left;
    string var(var_expression->str_value);
    boost::to_upper(var);
    if (var_expression->var_scope == VAR_SCOPE_USER) {
      if (uservar_list.size() != 0) {
        uservar_sql.append(", ");
      }
      uservar_sql.append("@");
      uservar_sql.append(var);
      uservar_list.push_back(var);
    } else if (var_expression->var_scope == VAR_SCOPE_SESSION) {
      if (useful_session_vars.count(var)) {
        if (sessionvar_list.size() != 0) {
          sessionvar_sql.append(", ");
        }
        sessionvar_sql.append("@@");
        sessionvar_sql.append(var);
        sessionvar_list.push_back(var);
      }
    }

    tmp = tmp->next;
  } while (tmp != head);

  if (uservar_list.empty() && sessionvar_list.empty()) {
    return;
  }
  string select_var_sql("SELECT ");
  if (uservar_list.empty()) {
    select_var_sql += sessionvar_sql;
    set_select_var_sql(select_var_sql);
    return;
  }
  select_uservar_flag = true;
  select_var_sql += uservar_sql;
  if (!sessionvar_list.empty()) {
    select_var_sql += ",";
    select_var_sql += sessionvar_sql;
  }
  set_select_var_sql(select_var_sql);
}

void Statement::handle_session_var(ExecutePlan *plan) {
  if (!st.sql->set_oper || st.sql->set_oper->names) {
    has_ignore_session_var = false;
    return;
  }

  Backend *backend = Backend::instance();
  unsigned int pos = 0;
  unsigned int var_num = 0;
  bool is_auto_commit = false;
  expr_list_item *head = NULL, *tmp;
  head = ((ListExpression *)st.sql->set_oper->set_list)->expr_list_head;
  tmp = head;

  CompareExpression *cmp;
  VarExpression *var_expression;
  string next_trx_level_value = "";
  do {
    cmp = (CompareExpression *)tmp->expr;
    var_expression = (VarExpression *)cmp->left;
    string var(var_expression->str_value);
    boost::to_upper(var);
    if (get_session()->is_in_transaction() &&
        var_expression->var_scope == VAR_SCOPE_SESSION &&
        var == "TRANSACTION_ISOLATION") {
      LOG_ERROR(
          "Transaction characteristics can't be changed while a transaction is "
          "in progress.\n");
      throw HandlerError(
          "Transaction characteristics can't be changed while a transaction is "
          "in progress.",
          1568);
    }
    if ((static_cast<VarExpression *>(var_expression))->next_trx_level) {
      if (boost::regex_match(
              var, boost::regex("TX_ISOLATION", boost::regex::icase))) {
        LOG_ERROR(
            "Unsupport set @@TX_ISOLATION, please use set "
            "@@TRANSACTION_ISOLATION.\n");
        throw NotSupportedError(
            "Unsupport set @@TX_ISOLATION, please use set "
            "@@TRANSACTION_ISOLATION.");
      }
      boost::regex support_level(
          "READ-COMMITTED|READ-UNCOMMITTED|REPEATABLE-READ",
          boost::regex::icase);
      if (!boost::regex_match(cmp->right->str_value, support_level)) {
        string err_msg = "Unsupport set @@TRANSACTION_ISOLATION = ";
        err_msg += cmp->right->str_value;
        LOG_ERROR("%s.\n", err_msg.c_str());
        throw NotSupportedError(err_msg.c_str());
      }
      next_trx_level_value.assign(cmp->right->str_value);
      has_ignore_session_var = true;
      session_var_sql.append(sql, pos, cmp->start_pos - pos - 1);
      pos = (tmp->next != head) ? tmp->next->expr->start_pos : cmp->end_pos;
      tmp = tmp->next;
      continue;
    }

    if (is_forbidden_session_var(
            var, Backend::instance()->get_backend_server_version() ==
                     MYSQL_VERSION_MARIADB)) {
      LOG_WARN("Ignore the unsupport variable %s.\n", var.c_str());
      has_ignore_session_var = true;
      session_var_sql.append(sql, pos, cmp->start_pos - pos - 1);
      pos = (tmp->next != head) ? tmp->next->expr->start_pos : cmp->end_pos;
      tmp = tmp->next;
      continue;
    }

    if (var_expression->var_scope != VAR_SCOPE_SESSION) {
      if (var_expression->var_scope == VAR_SCOPE_GLOBAL) {
        has_ignore_session_var = true;
        session_var_sql.append(sql, pos, cmp->start_pos - pos - 1);
        pos = (tmp->next != head) ? tmp->next->expr->start_pos : cmp->end_pos;
        tmp = tmp->next;
        continue;
      }
      tmp = tmp->next;
      ++var_num;
      continue;
    } else if (var == "TRANSACTION_ISOLATION") {
      next_trx_level_value.clear();
      get_session()->overwrite_session_next_trx_level();
    }
    if (!strcmp(var.c_str(), "AUTOCOMMIT")) {
      string value;
      try {
        cmp->right->to_string(value);
      } catch (exception &e) {
        LOG_ERROR("Unsupport set session.\n");
        throw NotSupportedError("Unsupport set session");
      }
      bool auto_value = check_and_transform_autocommit_value(value);
      plan->session->set_stmt_auto_commit_value(auto_value);
      is_auto_commit = true;
    }
    if (is_auto_commit || backend->is_ignore_session_var(var)) {
      has_ignore_session_var = true;
      session_var_sql.append(sql, pos, cmp->right->start_pos - pos - 1);
      session_var_sql.append("@@");
      session_var_sql.append(var);
      pos = cmp->right->end_pos;
      is_auto_commit = false;
    } else {
      useful_session_vars.insert(var);
      ++var_num;
    }
    tmp = tmp->next;
  } while (tmp != head);

  if (!next_trx_level_value.empty()) {
    next_trx_level_value = "\'" + next_trx_level_value + "\'";
    get_session()->prepare_set_session_next_trx_level(next_trx_level_value);
  }
  if (!get_session()->get_auto_commit_is_on() ||
      !get_session()->get_stmt_auto_commit_int_value()) {
    get_session()->set_session_next_trx_level();
  }

  if (has_ignore_session_var) {
    if (var_num == 0) {
      session_var_sql.assign("");
      LOG_DEBUG("After rebuild ignore/skip all session var.\n");
    } else {
      session_var_sql.append(sql, pos, strlen(sql) - pos);
      LOG_DEBUG("rebuild session var sql : %s.\n", session_var_sql.c_str());
    }
  }
}

void Statement::build_uservar_sql(var_item *var_item_list, ExecutePlan *plan) {
  Session *session = plan->session;
  map<string, string> *user_var_map = session->get_user_var_map();

  string var_sql = "SET ";
  string select_var_sql;
  set<string> uservar_set;
  while (var_item_list != NULL) {
    string var_name = var_item_list->value;
    boost::to_upper(var_name);
    if (!uservar_set.count(var_name)) {
      uservar_set.insert(var_name);

      var_sql.append("@");
      var_sql.append(var_name);
      var_sql.append("=");

      if (user_var_map->count(var_name)) {
        var_sql.append((*user_var_map)[var_name]);
      } else {
        var_sql.append("NULL");
      }
      var_sql.append(", ");
    }

    if (var_item_list->expr) {
      VarExpression *expr = (VarExpression *)var_item_list->expr;
      if (user_var_map->count(var_name)) {
        expr->set_user_var_null(false);
        expr->set_user_var_value(((*user_var_map)[var_name]).c_str());
      } else {
        expr->set_user_var_null(true);
      }
    }

    var_item_list = var_item_list->next;
  }

  boost::erase_tail(var_sql, 2);
  session->set_var_sql(var_sql);
  LOG_DEBUG("Build variable sql is [%s].\n", var_sql.c_str());

  if (select_uservar_flag) {
    return;
  }
  if (uservar_set.empty()) {
    return;
  }
  if (st.type == STMT_CALL) {
    uservar_list.clear();
    select_uservar_flag = true;
    select_var_sql = "SELECT ";
    for (set<string>::iterator it = uservar_set.begin();
         it != uservar_set.end(); ++it) {
      string var_name = *it;
      uservar_list.push_back(var_name);

      select_var_sql.append("@");
      select_var_sql.append(var_name);
      select_var_sql.append(", ");
    }
  }
  boost::erase_tail(select_var_sql, 2);
  this->set_select_var_sql(select_var_sql);
  LOG_DEBUG("Build select variable sql is [%s].\n", select_var_sql.c_str());
}

void Statement::deal_select_assign_uservar(record_scan *rs) {
  field_item *field = rs->field_list_head;
  uservar_list.clear();
  string select_var_sql = "SELECT ";
  unsigned int i = 0;
  while (field) {
    // the field_item should like "@v:=expr"
    Expression *expr = field->field_expr;

    if (expr && expr->type == EXPR_STR) {
      if (select_uservar_flag) {
        if (strchr(((StrExpression *)expr)->str_value, '*') != NULL) {
          LOG_ERROR(
              "The assign user variable field in SELECT statement"
              " must appear after field like '*' or 'TABLE.*.'\n");
          throw NotSupportedError(
              "The assign user variable field must appear after field like '*' "
              "or 'TABLE.*.'");
        }
      }
    }

    if (expr && expr->type != EXPR_ASSIGN) {
      ++i;
      field = field->next;
      continue;
    }

    select_uservar_flag = true;
    CompareExpression *cmp_expr = (CompareExpression *)expr;
    VarExpression *var_expr = (VarExpression *)(cmp_expr->left);
    string var_name(var_expr->str_value);
    boost::to_upper(var_name);
    pair<unsigned int, string> uservar_pair(i++, var_name);
    select_uservar_vec.push_back(uservar_pair);
    need_clean_select_uservar_vec = true;
    uservar_list.push_back(var_name);
    select_var_sql.append("@");
    select_var_sql.append(var_name);
    select_var_sql.append(", ");
    field = field->next;
  }
  if (select_uservar_flag) {
    select_field_num = i;
    boost::erase_tail(select_var_sql, 2);
    this->set_select_var_sql(select_var_sql);
  }
}

void Statement::rebuild_found_rows_sql(record_scan *rs, ExecutePlan *plan) {
  field_item *field = rs->field_list_tail;
  join_node *tables = rs->join_tables;
  if (!tables || (tables && tables->table_name &&
                  !strcasecmp(tables->table_name, "dual"))) {
    while (field) {
      replace_func_found_rows_with_value(field, rs, plan, field);
      field = field->pre;
    }
  }
  if (tables && tables->type == JOIN_NODE_SUBSELECT) {
    rebuild_found_rows_sql(tables->sub_select, plan);
  }
  if (tables && tables->type == JOIN_NODE_JOIN) {
    while (tables->type == JOIN_NODE_JOIN) {
      join_node *tables_tmp = tables->right;
      if (tables_tmp->type == JOIN_NODE_SUBSELECT) {
        rebuild_found_rows_sql(tables_tmp->sub_select, plan);
      }
      tables = tables->left;
    }
    if (tables->type == JOIN_NODE_SUBSELECT) {
      rebuild_found_rows_sql(tables->sub_select, plan);
    }
  }
}

void Statement::replace_func_found_rows_with_value(field_item *field,
                                                   const record_scan *scan,
                                                   ExecutePlan *plan,
                                                   field_item *top_field) {
  if (field->field_expr->has_function_type(FUNCTION_TYPE_FOUND_ROWS)) {
    if (!top_field->alias) {
      sql_tmp = add_original_field(sql_tmp.c_str(), top_field->head_pos,
                                   top_field->tail_pos);
    }
    sql_tmp = replace_found_rows(sql_tmp.c_str(), plan,
                                 scan->found_rows_pos_list->head_pos,
                                 scan->found_rows_pos_list->tail_pos);
  } else if (field->field_expr->type == EXPR_SUBSELECT) {
    SubSelectExpression *sub_expr = (SubSelectExpression *)field->field_expr;
    const record_scan *sub_select = sub_expr->sub_select;
    field_item *sub_field = sub_select->field_list_tail;
    replace_func_found_rows_with_value(sub_field, sub_select, plan, top_field);
  }
}

string Statement::replace_found_rows(const char *query, ExecutePlan *plan,
                                     unsigned int head_pos,
                                     unsigned int tail_pos) {
  string tmp_sql(query);
  char new_num[32];
  uint64_t number = 0;
  string head_str, tail_str, tmp_str, add_str;

  head_str.assign(tmp_sql, 0, head_pos - 1);
  tail_str.assign(tmp_sql, tail_pos, tmp_sql.length() - tail_pos);
  add_str.assign(tmp_sql, head_pos - 1, tail_pos - head_pos + 1);

  number = plan->session->get_found_rows();
  int len = sprintf(new_num, "%llu", (unsigned long long)number);
  head_str.append(new_num, len);
  if ((tail_pos + 1) >= (head_pos + len)) {
    tmp_str.assign(tail_pos + 1 - head_pos - len, ' ');
  }
  head_str.append(tmp_str);
  tmp_sql.clear();
  tmp_sql.append(head_str.c_str());
  tmp_sql.append(tail_str.c_str());

  return tmp_sql;
}

string Statement::add_original_field(const char *query, unsigned int head_pos,
                                     unsigned int tail_pos) {
  string tmp_sql(query);
  string head_str, tail_str, tmp_str, add_str;

  head_str.assign(tmp_sql, 0, tail_pos);
  tail_str.assign(tmp_sql, tail_pos, tmp_sql.length() - tail_pos);
  add_str.assign(tmp_sql, head_pos - 1, tail_pos - head_pos + 1);

  head_str.append(" AS '");
  head_str.append(add_str.c_str());
  head_str.append("'");
  tmp_sql.clear();
  tmp_sql.append(head_str.c_str());
  tmp_sql.append(tail_str.c_str());

  return tmp_sql;
}

bool function_is_valid(CurrentStatementFunctionType type) {
  return type == AGGREGATE_TYPE_MIN || type == AGGREGATE_TYPE_MAX ||
         type == AGGREGATE_TYPE_COUNT || type == AGGREGATE_TYPE_SUM ||
         type == AGGREGATE_TYPE_AVG;
}

void get_aggregate_functions(record_scan *rs, list<AggregateDesc> &aggr_list) {
  AggregateDesc desc;
  CurrentStatementFunctionType type;
  unsigned int column_index = 0;

  field_item *field = rs->field_list_head;
  while (field) {
    if (field->field_expr) {
      type = field->field_expr->get_cur_func_type();
      if (type != AGGREGATE_TYPE_NON) {
        if (function_is_valid(type)) {
          if (type != AGGREGATE_TYPE_AVG) {
            desc.column_index = column_index;
            desc.type = type;
            aggr_list.push_back(desc);
          }
        }
      }
    }
    ++column_index;
    field = field->next;
  }
}

Expression *get_expr_in_select_items(int column_index, record_scan *rs) {
  field_item *field = rs->field_list_head;
  int i = 0;

  while (i++ < column_index) {
    if (!field) {
      LOG_ERROR("Fail to find [%d] column in fields.\n", column_index);
      throw Error("Fail to find correct column in select list.");
    }
    field = field->next;
  }

  if (!field) {
    LOG_ERROR("Fail to find [%d] column in fields.\n", column_index);
    throw Error("Fail to find correct column in select list.");
  }
  return field->field_expr;
}

int Statement::create_new_select_item(Expression *expr, record_scan *rs,
                                      bool for_order_by_list) {
  list<string>::iterator it;
  string expr_str;
  int column_index;

  if (expr->type == EXPR_INT) {
    if (rs->is_contain_star) {
      LOG_ERROR(
          "Not support ORDER BY or GROUP BY column index with '*' in select "
          "list.\n");
      throw Error(
          "Not support ORDER BY or GROUP BY column index with '*' in select "
          "list.");
    }
    column_index = (int)(((IntExpression *)expr)->get_integer_value()) - 1;
    if (column_index < 0) {
      LOG_ERROR("Unknown column in order clause.\n");
      throw Error("Unknown column in order clause.");
    }
  } else {
    column_index = item_in_select_fields_pos(expr, rs);
  }

  if (column_index != COLUMN_INDEX_UNDEF) {
    expr = get_expr_in_select_items(column_index, rs);
  }

  if (!rs->is_contain_star && column_index != COLUMN_INDEX_UNDEF) {
    return column_index;
  } else {
    expr->to_string(expr_str);
    int i = 0;
    for (it = new_select_items.begin(); it != new_select_items.end();
         it++, i--) {
      if (strcasecmp(it->c_str(), expr_str.c_str()) == 0) {
        return i - 1;
      }
    }
    if (expr->type == EXPR_STR) {
      size_t first_dot_pos = expr_str.find(".");
      size_t second_dot_pos = expr_str.find(".", first_dot_pos + 1);

      if (first_dot_pos == string::npos) {
        string str("`");
        str.append(expr_str);
        str.append("`");
        new_select_items.push_back(str);
      } else if (second_dot_pos == string::npos) {
        string str("`");
        str.append(expr_str, 0, first_dot_pos);
        str.append("`.`");
        str.append(expr_str, first_dot_pos + 1,
                   expr_str.length() - first_dot_pos - 1);
        str.append("`");
        new_select_items.push_back(str);
      } else {
        string str("`");
        str.append(expr_str, 0, first_dot_pos);
        str.append("`.`");
        str.append(expr_str, first_dot_pos + 1,
                   second_dot_pos - first_dot_pos - 1);
        str.append("`.`");
        str.append(expr_str, second_dot_pos + 1,
                   expr_str.length() - second_dot_pos - 1);
        str.append("`");
        new_select_items.push_back(str);
      }
    } else {
      if (for_order_by_list && !expr->check_contains_str_expression()) {
        LOG_ERROR(
            "Only support ORDER BY contains column names or one single int "
            "number.\n");
        throw Error(
            "Only support ORDER BY contains column names or one single int "
            "number.");
      }
      new_select_items.push_back(expr_str);
    }
    return --new_select_item_num;
  }
}

bool Statement::is_expression_contain_invaid_function(
    Expression *expr, record_scan *rs, bool error_on_unknown_expr,
    bool is_check_for_update) {
  FieldFunctionSituation type = expression_contain_function_type(
      expr, rs, error_on_unknown_expr, is_check_for_update);
  if (type == CONTAINS_OTHER_FUNC) {
    return true;
  }
  return false;
}

bool Statement::is_expression_contain_aggr(Expression *expr, record_scan *rs,
                                           bool error_on_unknown_expr,
                                           bool is_check_for_update) {
  bool ret = false;
  FieldFunctionSituation type = expression_contain_function_type(
      expr, rs, error_on_unknown_expr, is_check_for_update);
  if (type == CONTAINS_VALID_AGGR)
    ret = true;
  else if (type == CONTAINS_NO_FUNCTION)
    ret = false;
  else {
    string expr_str;
    expr->to_string(expr_str);
    LOG_ERROR("Unsupport aggregate function in expression %s.\n",
              expr_str.c_str());
    throw UnSupportPartitionSQL(
        "Unsupport expression with "
        "invalid aggregate function.");
  }
  return ret;
}

FieldFunctionSituation Statement::expression_contain_function_type(
    Expression *expr, record_scan *rs, bool error_on_unknown_expr,
    bool is_check_for_update) {
  expr_type type = expr->type;
  switch (type) {
    case EXPR_IS:
    case EXPR_IS_NOT:
    case EXPR_EQ:
    case EXPR_GR:
    case EXPR_LESS:
    case EXPR_GE:
    case EXPR_LESSE:
    case EXPR_NE:
    case EXPR_EQ_N:
    case EXPR_AND:
    case EXPR_OR:
    case EXPR_XOR: {
      BoolBinaryExpression *bi_expr = (BoolBinaryExpression *)expr;
      FieldFunctionSituation ret = expression_contain_function_type(
          bi_expr->left, rs, error_on_unknown_expr, is_check_for_update);
      if (ret == CONTAINS_VALID_AGGR || ret == CONTAINS_OTHER_FUNC)
        return ret;
      else
        return expression_contain_function_type(
            bi_expr->right, rs, error_on_unknown_expr, is_check_for_update);
    } break;

    case EXPR_IF_COND:
    case EXPR_WHEN_THEN: {
      ConditionExpression *cond_expr = (ConditionExpression *)expr;
      FieldFunctionSituation ret = expression_contain_function_type(
          cond_expr->left, rs, error_on_unknown_expr, is_check_for_update);
      if (ret == CONTAINS_VALID_AGGR || ret == CONTAINS_OTHER_FUNC)
        return ret;
      else
        return expression_contain_function_type(
            cond_expr->right, rs, error_on_unknown_expr, is_check_for_update);
    } break;

    case EXPR_ADD:
    case EXPR_SUB:
    case EXPR_MUL:
    case EXPR_DIV: {
      ArithmeticBinaryExpression *bi_expr = (ArithmeticBinaryExpression *)expr;
      FieldFunctionSituation ret = expression_contain_function_type(
          bi_expr->left, rs, error_on_unknown_expr, is_check_for_update);
      if (ret == CONTAINS_VALID_AGGR || ret == CONTAINS_OTHER_FUNC)
        return ret;
      else
        return expression_contain_function_type(
            bi_expr->right, rs, error_on_unknown_expr, is_check_for_update);
    } break;

    case EXPR_MINUS: {
      ArithmeticUnaryExpression *un_expr = (ArithmeticUnaryExpression *)expr;
      return expression_contain_function_type(
          un_expr->right, rs, error_on_unknown_expr, is_check_for_update);
    } break;

    case EXPR_IF:
    case EXPR_IFNULL:
    case EXPR_CASE: {
      bool contains_valid_function = false;
      TerCondExpression *case_expr = (TerCondExpression *)expr;
      expr_list_item *item = case_expr->list_expr->expr_list_head;
      do {
        FieldFunctionSituation ret = expression_contain_function_type(
            item->expr, rs, error_on_unknown_expr, is_check_for_update);
        if (ret == CONTAINS_OTHER_FUNC)
          return ret;
        else if (ret == CONTAINS_VALID_AGGR)
          contains_valid_function = true;
        item = item->next;
      } while (item != case_expr->list_expr->expr_list_head);
      if (case_expr->else_expr) {
        FieldFunctionSituation ret = expression_contain_function_type(
            case_expr->else_expr, rs, error_on_unknown_expr,
            is_check_for_update);
        if (ret == CONTAINS_OTHER_FUNC || ret == CONTAINS_VALID_AGGR)
          return ret;
      }
      if (contains_valid_function) return CONTAINS_VALID_AGGR;
      return CONTAINS_NO_FUNCTION;
    } break;

    case EXPR_FUNC:
    case EXPR_STR: {
      if (is_check_for_update) return CONTAINS_VALID_AGGR;
      int col_index;
      CurrentStatementFunctionType func_type;
      col_index = item_in_select_fields_pos(expr, rs);
      if (col_index != COLUMN_INDEX_UNDEF) {
        expr = get_expr_in_select_items(col_index, rs);
      }
      func_type = expr->get_cur_func_type();
      if (func_type != AGGREGATE_TYPE_NON) {
        if (function_is_valid(func_type)) {
          return CONTAINS_VALID_AGGR;
        } else {
          return CONTAINS_OTHER_FUNC;
        }
      }

      if (expr->check_contains_aggregate_function()) return CONTAINS_OTHER_FUNC;
      return CONTAINS_NO_FUNCTION;
    } break;

    case EXPR_INT:
    case EXPR_STRING:
    case EXPR_FLOAT:
    case EXPR_BOOL:
    case EXPR_VAR:
      break;

    case EXPR_NULL:
      break;

    default: {
      if (error_on_unknown_expr) {
        LOG_ERROR("Filter expression %d is too complex.\n", type);
        throw NotImplementedError("Filter expression is too complex.");
      }
      return CONTAINS_NO_FUNCTION;
    } break;
  }
  return CONTAINS_NO_FUNCTION;
}

void Statement::handle_expression(Expression *expr, Expression **parent,
                                  record_scan *rs,
                                  STMT_EXPR_TYPE stmt_expr_type) {
  expr_type type = expr->type;
  switch (type) {
    case EXPR_EQ:
    case EXPR_GR:
    case EXPR_LESS:
    case EXPR_GE:
    case EXPR_LESSE:
    case EXPR_NE:
    case EXPR_EQ_N:
    case EXPR_AND:
    case EXPR_OR:
    case EXPR_XOR: {
      BoolBinaryExpression *bi_expr = (BoolBinaryExpression *)expr;
      handle_expression(bi_expr->left, &bi_expr->left, rs, stmt_expr_type);
      handle_expression(bi_expr->right, &bi_expr->right, rs, stmt_expr_type);
    } break;

    case EXPR_IF_COND:
    case EXPR_WHEN_THEN: {
      ConditionExpression *cond_expr = (ConditionExpression *)expr;
      handle_expression(cond_expr->left, &cond_expr->left, rs, stmt_expr_type);
      handle_expression(cond_expr->right, &cond_expr->right, rs,
                        stmt_expr_type);
    } break;

    case EXPR_ADD:
    case EXPR_SUB:
    case EXPR_MUL:
    case EXPR_DIV: {
      ArithmeticBinaryExpression *bi_expr = (ArithmeticBinaryExpression *)expr;
      handle_expression(bi_expr->left, &bi_expr->left, rs, stmt_expr_type);
      handle_expression(bi_expr->right, &bi_expr->right, rs, stmt_expr_type);
    } break;
    case EXPR_IS:
    case EXPR_IS_NOT: {
      TruthExpression *truth_expr = (TruthExpression *)expr;
      handle_expression(truth_expr->left, &truth_expr->left, rs,
                        stmt_expr_type);
      handle_expression(truth_expr->right, &truth_expr->right, rs,
                        stmt_expr_type);
    } break;
    case EXPR_MINUS: {
      ArithmeticUnaryExpression *un_expr = (ArithmeticUnaryExpression *)expr;
      handle_expression(un_expr->right, &un_expr->right, rs, stmt_expr_type);
    } break;

    case EXPR_IF:
    case EXPR_IFNULL:
    case EXPR_CASE: {
      TerCondExpression *ter_cond_expr = (TerCondExpression *)expr;
      expr_list_item *item = ter_cond_expr->list_expr->expr_list_head;
      do {
        handle_expression(item->expr, &item->expr, rs, stmt_expr_type);
        item = item->next;
      } while (item != ter_cond_expr->list_expr->expr_list_head);
      if (ter_cond_expr->else_expr)
        handle_expression(ter_cond_expr->else_expr, &ter_cond_expr->else_expr,
                          rs, stmt_expr_type);
    } break;

    case EXPR_FUNC:
    case EXPR_STR: {
      int col_index;
      FieldExpression *field_expr;

      if (stmt_expr_type == HAVING_EXPR) {
        col_index = create_new_select_item(expr, rs);
        if (expr->get_cur_func_type() == AGGREGATE_TYPE_AVG)
          add_avg_func(col_index, expr);
        Expression *tmp = NULL;
        if (parent && *parent) tmp = *parent;
        field_expr = get_field_expression(col_index, tmp);
        new_field_expressions.push_back(field_expr);
        need_clean_new_field_expressions = true;
        if (parent && *parent) {
          *parent = field_expr;
        }
      } else if (stmt_expr_type == FIELD_EXPR) {
        if (parent && *parent) {  // don't deal 'select sum() from table', deal
                                  // 'select sum()+1 from table'
          col_index = create_new_select_item(expr, rs);
          if (expr->get_cur_func_type() == AGGREGATE_TYPE_AVG)
            add_avg_func(col_index, expr);
          field_expr = get_field_expression(col_index, *parent);
          new_field_expressions.push_back(field_expr);
          need_clean_new_field_expressions = true;
          *parent = field_expr;
          need_deal_field_expr = true;
        }
      }
    } break;

    case EXPR_INT:
    case EXPR_STRING:
    case EXPR_FLOAT:
    case EXPR_BOOL:
      break;

    case EXPR_NULL:
      break;

    default:
      if (stmt_expr_type == HAVING_EXPR) {
        LOG_ERROR("Filter expression %d is too complex.", type);
        throw NotImplementedError("Filter expression is too complex.");
      }
      break;
  }
}

void Statement::add_avg_func(int field_num, Expression *expr) {
  AvgDesc desc;
  init_avgdesc(desc);
  try {
    desc.avg_index = field_num;
    string column_name = "";
    ((FunctionExpression *)expr)->param_list->to_string(column_name);
    reset_avg_sql(column_name, desc);
  } catch (...) {
    if (desc.mpf_inited) mpf_clear(desc.value);
    throw;
  }
  avg_list.push_back(desc);
}
ExecuteNode *Statement::generate_aggregate_by_subplan(
    ExecutePlan *plan, record_scan *rs, list<AggregateDesc> &aggr_list,
    list<AvgDesc> &avg_list, ExecuteNode **tail_node) {
  ACE_ASSERT(aggr_list.size());
  ACE_UNUSED_ARG(rs);

  ExecuteNode *head_node = NULL;
  ExecuteNode *aggr_node = plan->get_aggr_node(aggr_list);
  *tail_node = aggr_node;
  head_node = aggr_node;
  if (!avg_list.empty()) {
    ExecuteNode *avg_node = plan->get_avg_node(avg_list);
    avg_node->add_child(head_node);
    head_node = avg_node;
  }
  if (!select_expr_list.empty()) {
    ExecuteNode *calculate_expr_node =
        plan->get_expr_calculate_node(select_expr_list);
    calculate_expr_node->add_child(head_node);
    head_node = calculate_expr_node;
  }

  if (!simple_having) {
    string new_sql;

    ExecuteNode *having_node = plan->get_expr_filter_node(having);
    having_node->add_child(head_node);
    head_node = having_node;
    new_sql.append(fetch_sql_tmp, 0, rs->having_pos - rs->start_pos);

    LOG_DEBUG("Orginal SQL: %s\n", fetch_sql_tmp.c_str());
    fetch_sql_tmp = new_sql;
    LOG_DEBUG("    New SQL: %s\n", new_sql.c_str());
  }
  return head_node;
}

void Statement::re_parser_stmt(record_scan **new_rs, Statement **new_stmt,
                               const char *query_sql) {
  ACE_UNUSED_ARG(new_rs);
  Parser *parser = Driver::get_driver()->get_parser();
  *new_stmt = parser->parse(query_sql, stmt_allow_dot_in_ident, true, NULL,
                            NULL, NULL, ctype);
  re_parser_stmt_list.push_back(*new_stmt);
  if ((*new_stmt)->is_partial_parsed()) {
    LOG_ERROR(
        "not support partial parse when do sql reparse, the sql is [%s]\n",
        query_sql);
    throw Error("got partial parsed sql when do sql reparse");
  }
  *new_rs = (*new_stmt)->get_stmt_node()->scanner;
}

void Statement::set_charset_and_isci_accroding_to_collation(
    string &collation_name, CharsetType &ctype, bool &is_cs) {
  ctype = CHARSET_TYPE_OTHER;
  is_cs = false;
  if (collation_name.find("utf8") != string::npos) {
    ctype = CHARSET_TYPE_UTF8;
    if (collation_name.find("utf8mb4") != string::npos) {
      ctype = CHARSET_TYPE_UTF8MB4;
    }
  }
  if (collation_name.find("bin") != string::npos) is_cs = true;
}

void Statement::add_group_item_list(order_item *start,
                                    list<SortDesc> &group_list,
                                    record_scan *rs) {
  order_item *item = start;
  if (item == NULL) return;
  need_clean_group_by_list = true;
  Expression *expr;
  int column_index;
  string collation_name;
  SortDesc og_desc;
  do {
    expr = item->field->field_expr;
    column_index = create_new_select_item(expr, rs, true);
    collation_name = get_column_collation(expr, rs);
    og_desc.column_index = column_index;
    og_desc.sort_order = item->order;
    set_charset_and_isci_accroding_to_collation(collation_name, og_desc.ctype,
                                                og_desc.is_cs);
    if (!og_desc.is_cs) {
      if (!collation_name.empty())  // The collaction is not empty, so it is a
                                    // char and not utf8_bin/utf8mb4_bin
        rs->group_by_ci = true;
    }
    group_list.push_back(og_desc);
    item = item->next;
  } while (item != start);
}

TableStruct Statement::get_table_struct_from_record_scan(string column_name,
                                                         record_scan *rs) {
  table_link *tl = rs->first_table;
  while (tl) {
    if (tl->join->cur_rec_scan == rs) {
      string schema_name =
          tl->join->schema_name ? tl->join->schema_name : schema;
      string table_name = tl->join->table_name;

      TableInfoCollection *tic = TableInfoCollection::instance();
      TableInfo *ti =
          tic->get_table_info_for_read(schema_name.c_str(), table_name.c_str());
      map<string, TableColumnInfo *, strcasecomp> *column_info_map;
      try {
        column_info_map =
            ti->element_table_column
                ->get_element_map_columnname_table_column_info(stmt_session);
      } catch (...) {
        LOG_ERROR(
            "Error occured when try to get table info column "
            "info(map_columnname_table_column_info) of table [%s.%s]\n",
            schema_name.c_str(), table_name.c_str());
        ti->release_table_info_lock();
        throw;
      }

      string tmp_colname(column_name);
      boost::to_lower(tmp_colname);
      if (column_info_map != NULL) {
        if (column_info_map->count(tmp_colname)) {
          ti->release_table_info_lock();
          TableStruct ret;
          ret.schema_name = schema_name;
          ret.table_name = table_name;
          ret.alias = table_name;
          return ret;
        }
      }
      ti->release_table_info_lock();
    }
    tl = tl->next;
  }
  TableStruct ret;
  return ret;
}

// If return true means this table_name is belong to subquery, which not be
// able to get the real table_name
bool adjust_alias_to_table_name_in_rs(record_scan *rs, string &table_name,
                                      string &schema_name, const char *schema) {
  LOG_DEBUG("adjust_alias_to_table_name_in_rs for table_name %s with %@.\n",
            table_name.c_str(), rs->first_table);
  table_link *table = rs->first_table;

  // TODO: should record the map<table_name, alias> for one record_scan in
  // statement.
  if (!table)  // no table means it is a table subquery stmt
    return true;
  bool find_match_table = false;
  while (table) {
    if (table->join->alias &&
        !strcasecmp(table->join->alias, table_name.c_str())) {
      if (table->join->sub_select) return true;
      table_name.assign(
          table->join->table_name);  // replace the alias to the real table name
      schema_name.assign(table->join->schema_name ? table->join->schema_name
                                                  : schema);
      return false;
    }
    if (table->join->table_name &&
        !strcasecmp(table->join->table_name, table_name.c_str()))
      find_match_table = true;
    table = table->next;
    if (table && table->join->cur_rec_scan != rs) break;
  }
  if (find_match_table) return false;
  /*This table is not find in current record_scan, it may be the merged
   * record_scan. In this situation, we should consider it as not find
   * table.*/
  return true;
}

Expression *find_group_by_alias_expr(string group_str, record_scan *rs) {
  field_item *fi = rs->field_list_head;
  if (fi) {
    do {
      if (fi->alias && !strcasecmp(fi->alias, group_str.c_str())) {
        return fi->field_expr;
      }
      fi = fi->next;
    } while (fi && fi != rs->field_list_tail);
  }

  return NULL;
}

string Statement::get_column_collation(Expression *expr, record_scan *rs,
                                       bool check_alias) {
#ifdef DEBUG
  string tmp_str;
  expr->to_string(tmp_str);
  LOG_DEBUG("Statement::get_column_collation handle expr %s with rs %@.\n",
            tmp_str.c_str(), this);
#endif
  string ret;
  bool has_adjust_alias = false;
  bool fail_to_get_real_table_name = false;
  if (expr->type == EXPR_STR) {
    string column_string;
    expr->to_string(column_string);
    string schema_name;
    string table_name;
    string column_name;
    split_value_into_schema_table_column(column_string, schema_name, table_name,
                                         column_name);
    if (table_name.empty()) {
      TableStruct ts = get_table_struct_from_record_scan(column_name, rs);
      if (check_alias && ts.schema_name.empty()) {
        Expression *tmp_expr = find_group_by_alias_expr(column_string, rs);
        if (tmp_expr) {
          if (tmp_expr == expr) {
            string tmp_str;
            expr->to_string(tmp_str);
            LOG_WARN(
                "Fail to Statement::get_column_collation with expr [%s] for "
                "sql [%s].\n",
                tmp_str.c_str(), sql);
            return ret;
          }
          return get_column_collation(tmp_expr, rs, false);
        }
        return ret;  // this means a empty result
      }
      schema_name = ts.schema_name;
      table_name = ts.table_name;
    } else if (schema_name.empty()) {
      /*we should adjust the alias to the real table name, otherwise the
       * get_column_collation_using_schema_table may fail.
       *
       *  Here we should adjust the possible alias to the real table name,
       *  such as select * from test.t1 as t2, test1.t2 as t1 group by t1.c1;
       *  the c1 should be check from table test1.t2*/
      fail_to_get_real_table_name =
          adjust_alias_to_table_name_in_rs(rs, table_name, schema_name, schema);
      has_adjust_alias = true;
      if (schema_name.empty()) schema_name = schema;
    }
    if (!strcasecmp(schema_name.c_str(),
                    "dbscale_tmp"))  // TODO: support to check collaction for
                                     // dbscale_tmp table
      return ret;
    if (fail_to_get_real_table_name ||
        (!has_adjust_alias &&
         adjust_alias_to_table_name_in_rs(rs, table_name, schema_name, schema)))
      return ret;
    bool is_column_collation_cs_or_bin = false;
    bool is_data_type_string_like = true;
    ret = get_column_collation_using_schema_table(
        schema_name, table_name, column_name, is_column_collation_cs_or_bin,
        is_data_type_string_like);
  } else {
    list<Expression *> str_list;
    expr->get_str_expression(&str_list);
    list<Expression *>::iterator it = str_list.begin();
    for (; it != str_list.end(); ++it) {
      if (*it == expr) {
        string tmp_str;
        expr->to_string(tmp_str);
        LOG_WARN(
            "Fail to Statement::get_column_collation with expr [%s] for sql "
            "[%s].\n",
            tmp_str.c_str(), sql);
        return ret;
      }
      ret = get_column_collation(*it, rs, check_alias);
      if (!ret.empty()) {
        break;
      }
    }
  }
  return ret;
}

string Statement::get_column_collation_using_schema_table(
    string schema_name, string table_name, string column_name,
    bool &is_column_collation_cs_or_bin, bool &is_data_type_string_like) {
  string ret;
  TableInfoCollection *tic = TableInfoCollection::instance();
  TableInfo *ti =
      tic->get_table_info_for_read(schema_name.c_str(), table_name.c_str());
  map<string, TableColumnInfo *, strcasecomp> *column_info_map;
  try {
    column_info_map =
        ti->element_table_column->get_element_map_columnname_table_column_info(
            stmt_session);
  } catch (...) {
    LOG_ERROR(
        "Error occured when try to get table info column "
        "info(map_columnname_table_column_info) of table [%s.%s]\n",
        schema_name.c_str(), table_name.c_str());
    ti->release_table_info_lock();
    return ret;  // we should not just skip get collation if the table not find.
  }

  string tmp_colname(column_name);
  boost::to_lower(tmp_colname);
  if (column_info_map != NULL) {
    if (column_info_map->count(tmp_colname)) {
      TableColumnInfo *table_col_info = (*column_info_map)[tmp_colname];
      ret = table_col_info->collation_name;
      is_column_collation_cs_or_bin =
          table_col_info->is_column_collation_cs_or_bin;
      is_data_type_string_like = table_col_info->is_data_type_string_like;
    }
  }
  ti->release_table_info_lock();

  return ret;
}

void Statement::add_order_item_list(order_item *start,
                                    list<SortDesc> &order_list,
                                    record_scan *rs) {
  return add_group_item_list(start, order_list, rs);
}

bool order_by_in_group_by(order_item *order, order_item *group) {
  if (order == NULL) {
    return true;
  }

  if (group == NULL) {
    return false;
  }

  order_item *item1 = order;
  order_item *item2 = group;
  Expression *expr1;
  Expression *expr2;
  do {
    expr1 = item1->field->field_expr;
    expr2 = item2->field->field_expr;
    if (!expr1->is_equal(expr2) || item1->order != item2->order) {
      return false;
    }

    item1 = item1->next;
    item2 = item2->next;
  } while (item1 != order && item2 != group);

  if (item1 != order) {
    return false;
  }

  return true;
}

/* add select item if necessary */
void Statement::prepare_select_item(record_scan *rs) {
  list<AggregateDesc> aggr_list;

  // If there is having or with_rollup we can not deal with it easily.
  if (rs->group_by_list && rs->is_with_rollup) {
    LOG_ERROR("Do not support rollup currently.\n");
    throw UnSupportPartitionSQL("Do not support rollup currently");
  }

  order_in_group_flag =
      order_by_in_group_by(rs->order_by_list, rs->group_by_list);

  groupby_all_par_keys = group_by_all_keys(rs);
  if (rs->group_by_list || rs->having) {
    add_group_item_list(rs->group_by_list, group_by_list, rs);
    if (groupby_all_par_keys && hash_method_cs && rs->group_by_ci)
      groupby_all_par_keys =
          false;  // If the partition key collection is ci, but the partition
                  // method handle as cs, the group by should not pushdown.
    if (!groupby_all_par_keys || (!rs->order_by_list || order_in_group_flag)) {
      if (rs->having) {
        simple_having = !is_expression_contain_aggr(rs->having, rs, true);
        if (!simple_having) {
          handle_expression(rs->having, NULL, rs, HAVING_EXPR);
          having = rs->having;
        }
      }
    }
  }

  handle_avg_function(rs, avg_list);
  handle_select_expr(rs);

  if (rs->order_by_list) {
    if (!rs->group_by_list || !order_in_group_flag)
      add_order_item_list(rs->order_by_list, order_by_list, rs);
  }
}

void Statement::modify_select_item(record_scan **new_rs, Statement **new_stmt,
                                   const char *query_sql, size_t sql_len) {
  string new_sql;
  list<string>::reverse_iterator it;
  record_scan *rs = *new_rs;

  if (!rs || !rs->from_pos) {
    LOG_ERROR("Unsupport sql for Statement::modify_select_item for sql [%s].\n",
              sql);
    throw Error("Unsupport sql for Statement::modify_select_item.");
  }
  if (rs->from_pos >= sql_len) {
    re_parser_stmt(new_rs, new_stmt, query_sql);
    rs = *new_rs;
  }

  new_sql.append(query_sql + rs->start_pos - 1, rs->from_pos - rs->start_pos);
  for (it = new_select_items.rbegin(); it != new_select_items.rend(); ++it) {
    new_sql.append(",");
    new_sql.append(*it);
    new_sql.append(" ");
  }

  new_sql.append(query_sql + rs->from_pos - 1);
  re_parser_stmt(new_rs, new_stmt, new_sql.c_str());
  fetch_sql_tmp = new_sql;
}

order_item *get_last_groupby(order_item *groupby) {
  order_item *head = groupby;
  order_item *tmp = head;
  while (tmp && tmp->next != head) {
    tmp = tmp->next;
  }

  return tmp;
}

bool is_group_by_partition_key(record_scan *rs, table_link *par_table,
                               const char *schema) {
  if (!par_table || !par_table->join) {
    throw Error(
        "Fail to get par table or its join in the param of "
        "is_group_by_partition_key.\n");
  }
  const char *schema_name =
      par_table->join->schema_name ? par_table->join->schema_name : schema;
  const char *table_name = par_table->join->table_name;
  const char *tb_alias = par_table->join->alias;
  PartitionedTable *par_space = get_part_ds_from_table(par_table, schema);
  if (!par_space) {
    throw Error("Fail to get par space in is_group_by_partition_key.\n");
  }
  vector<const char *> *key_names = par_space->get_key_names();
  if (!key_names) {
    throw Error("Fail to get key names in is_group_by_partition_key.\n");
  }
  if (key_names->size() == 1) {
    order_item *groupby = get_last_groupby(rs->group_by_list);
    if (groupby && groupby_on_par_key(groupby, schema_name, table_name,
                                      tb_alias, key_names->at(0))) {
      return true;
    }
  }
  return false;
}

void Statement::check_single_par_table_in_sub_query() {
  if (par_table_num == 1) {
    table_link *par_table = par_tables[0];
    record_scan *rs = par_table->join->cur_rec_scan;
    record_scan *temp = rs;
    while (temp->upper && temp->upper->field_list_head) {
      for (field_item *fi = temp->upper->field_list_head;
           fi != temp->upper->field_list_tail->next; fi = fi->next) {
        if (fi->field_expr == temp->condition_belong) {
          LOG_ERROR(
              "Unsupport sql, the sql is [%s], Unsupport single partition "
              "table in sub query\n",
              sql);
          throw UnSupportPartitionSQL(
              "Unsupport single partition table in sub query");
        }
      }
      temp = temp->upper;
    }
    while (rs->join_belong) {
      if (rs_contains_aggr_group_limit(rs, par_table)) {
        LOG_ERROR(
            "Unsupport sql, the sql is [%s], Unsupport single partition table "
            "in"
            " sub query\n",
            sql);
        throw UnSupportPartitionSQL(
            "Unsupport single partition table in sub query");
      }
      rs = rs->join_belong->cur_rec_scan;
    }
  }
}

bool Statement::rs_contains_aggr_group_limit(record_scan *rs,
                                             table_link *par_table) {
  list<AggregateDesc> aggr_list;
  int avg_num = 0;
  get_aggregate_functions(rs, aggr_list);
  field_item *field = rs->field_list_head;
  while (field) {
    if (field->field_expr) {
      if (is_expression_contain_aggr(field->field_expr, rs, false)) {
        ++avg_num;
        break;
      }
    }
    field = field->next;
  }
  bool group_by_par_key = false;
  try {
    if (rs->group_by_list && is_group_by_partition_key(rs, par_table, schema))
      group_by_par_key = true;
  } catch (Exception &e) {
    LOG_ERROR("[%s] get exception in is_group_by_partition_key due to %s.\n",
              sql, e.what());
    throw;
  }

  if (!group_by_par_key &&
      (rs->group_by_list || !aggr_list.empty() || rs->having || avg_num)) {
    return 1;
  }
  if ((rs->limit || rs->order_by_list)) {
    return 1;
  }
  return 0;
}

record_scan *Statement::generate_select_plan_nodes(
    ExecutePlan *plan, vector<unsigned int> *par_ids,
    PartitionedTable *par_table, vector<ExecuteNode *> *nodes,
    const char *query_sql, bool need_check_distinct) {
  record_scan *new_rs;
  Statement *new_stmt = NULL;

  if (st.scanner->field_list_head == NULL) {
    re_parser_stmt(&new_rs, &new_stmt, query_sql);
  } else {
    new_rs = st.scanner;
  }

  ExecuteNode *tmp = NULL;
  ExecuteNode *head = NULL;
  ExecuteNode *tail = NULL;
  const char *fetch_sql = query_sql;
  try {
    if (par_tables.size() > 0) {
      table_link *par_table = par_tables[0];
      if (need_check_distinct &&
          check_and_replace_par_distinct_with_groupby(par_table, fetch_sql)) {
        // sql has update by replacing DISTINCT with GROUP BY, so need reparse.
        fetch_sql = no_distinct_sql.c_str();
        re_parser_stmt(&new_rs, &new_stmt, fetch_sql);
      }
    }
    check_single_par_table_in_sub_query();

    list<AggregateDesc> aggr_list;
    fetch_sql_tmp.append(fetch_sql);

    prepare_select_item(new_rs);
    if (!new_select_items.empty()) {
      modify_select_item(&new_rs, &new_stmt, fetch_sql_tmp.c_str(),
                         fetch_sql_tmp.length());
      fetch_sql = fetch_sql_tmp.c_str();
    }

    get_aggregate_functions(new_rs, aggr_list);
    if (!new_rs->limit && !new_rs->order_by_list && !new_rs->group_by_list &&
        aggr_list.empty() && avg_list.empty()) {
      ExecuteNode *fetch_node = NULL;
      DataSpace *dataspace = NULL;

      if (is_connect_by) {
        ExecuteNode *conn_node = plan->get_connect_by_node(connect_by_desc);
        ExecuteNode *filter_node =
            plan->get_project_node(new_select_items.size());
        filter_node->add_child(conn_node);

        unsigned int i = 0, id;
        for (; i < par_ids->size(); ++i) {
          id = par_ids->at(i);
          dataspace = par_table->get_partition(id);
          const char *used_sql =
              adjust_stmt_sql_for_shard(dataspace, fetch_sql);
          fetch_node = plan->get_fetch_node(dataspace, used_sql);
          conn_node->add_child(fetch_node);
        }
        nodes->push_back(filter_node);
      } else {
        unsigned int i = 0, id;
        for (; i < par_ids->size(); ++i) {
          id = par_ids->at(i);
          dataspace = par_table->get_partition(id);
          const char *used_sql =
              adjust_stmt_sql_for_shard(dataspace, fetch_sql);
          fetch_node = plan->get_fetch_node(dataspace, used_sql);
          nodes->push_back(fetch_node);
        }
      }

      if (par_ids->size() == par_table->get_real_partition_num()) {
        plan->session->set_execute_plan_touch_partition_nums(-1);
      } else {
        plan->session->set_execute_plan_touch_partition_nums(par_ids->size());
      }

      if (st.type == STMT_SELECT && max_fetch_node_threads == 1)
        plan->set_fetch_node_no_thread(true);
      return new_rs;
    }
    if (new_rs->is_contain_star && !aggr_list.empty()) {
      LOG_ERROR(
          "Unsupport sql, the sql is [%s], Unsupport select * and aggregate "
          "function"
          "for PartitionedTable",
          sql);
      release_avg_list(avg_list);
      throw UnSupportPartitionSQL(
          "Unsupport select * and aggregate function for PartitionedTable");
    }

    if (new_rs->limit) {
      ExecuteNode *limit = generate_limit_subplan(plan, new_rs, &tmp);
      if (!head) head = limit;
      if (tail) tail->add_child(limit);
      tail = tmp;
    }

    if (!aggr_list.empty() && !new_rs->group_by_list) {
      ExecuteNode *aggr = generate_aggregate_by_subplan(plan, new_rs, aggr_list,
                                                        avg_list, &tmp);
      if (!head) head = aggr;
      if (tail) tail->add_child(aggr);
      tail = tmp;
    } else if (new_rs->group_by_list || new_rs->order_by_list) {
      ExecuteNode *og =
          generate_order_by_subplan(plan, new_rs, aggr_list, avg_list, &tmp);
      if (!head) head = og;
      if (tail) tail->add_child(og);
      tail = tmp;
      // TODO: if there is order by or group by and there is other child node(s)
      // after it, which needs info from new_rs, we may need to re-parse the sql
      // to get correct new_rs
    }

    if (is_connect_by) {
      ExecuteNode *conn_node = plan->get_connect_by_node(connect_by_desc);
      conn_node->add_child(head);
      head = conn_node;
    }

    /* add project node */
    if (!new_select_items.empty()) {
      ExecuteNode *filter_node =
          plan->get_project_node(new_select_items.size());
      filter_node->add_child(head);
      head = filter_node;
    }

    if (fetch_sql_tmp.size()) {
      fetch_sql = fetch_sql_tmp.c_str();
      if (strcmp(query_sql, fetch_sql) != 0) {
        LOG_DEBUG("sql[%s] has changed to [%s], reparse for sql\n", query_sql,
                  fetch_sql);
        re_parser_stmt(&new_rs, &new_stmt, fetch_sql);
      }
    }
    vector<unsigned int>::iterator it = par_ids->begin();
    for (; it != par_ids->end(); ++it) {
      const char *used_sql =
          adjust_stmt_sql_for_shard(par_table->get_partition(*it), fetch_sql);
      ExecuteNode *fetch_node =
          plan->get_fetch_node(par_table->get_partition(*it), used_sql);
      tail->add_child(fetch_node);
    }
    if (par_ids->size() == par_table->get_real_partition_num()) {
      plan->session->set_execute_plan_touch_partition_nums(-1);
    } else {
      plan->session->set_execute_plan_touch_partition_nums(par_ids->size());
    }

    nodes->push_back(head);
  } catch (Exception &e) {
    clean_up_execute_nodes(nodes);
    throw;
  }
  return new_rs;
}

table_link *Statement::get_one_modify_table() {
  table_link *modify_table = st.scanner->first_table;
  if (modify_table->join->upper) {  // upper means join with other table.

    // TODO: support the sql rebuild of delete/update with multiple normal
    // tables.
    if (st.type == STMT_UPDATE) {
      if (Backend::instance()->is_centralized_cluster()) return modify_table;
      if (is_tables_share_same_dataserver(modify_table)) return modify_table;
      vector<table_link *> modify_tables;
      get_modify_tables(&modify_tables);
      if (modify_tables.size() == 1) {
        modify_table = modify_tables[0];
        if (modify_table && can_tables_associate(modify_table) &&
            table_contains_set_list(modify_table))
          return modify_table;
      }
      LOG_ERROR(
          "Unsupport sql[%s] with more than 1 modify table when "
          "this sql needs seperated execution.\n",
          sql);
      throw UnSupportPartitionSQL(
          "Unsupport sql with more than 1 modify table when this sql needs "
          "seperated execution");
    } else if (st.type == STMT_DELETE &&
               st.sql->delete_oper->delete_list->next !=
                   st.sql->delete_oper->delete_list &&
               !can_tables_associate(modify_table)) {
      LOG_ERROR(
          "Unsupport sql[%s] with more than 1 modify table when "
          "this sql needs seperated execution.\n",
          sql);
      throw UnSupportPartitionSQL(
          "Unsupport sql with more than 1 modify table when this sql needs "
          "seperated execution");
    } else if (st.type != STMT_UPDATE && st.type != STMT_DELETE) {
      if (can_tables_associate(modify_table))
        return modify_table;
      else {
        LOG_ERROR(
            "Unsupport sql[%s] with more than 1 modify table when "
            "this sql needs seperated execution.\n",
            sql);
        throw UnSupportPartitionSQL(
            "Unsupport sql with more than 1 modify table when this sql needs "
            "seperated execution");
      }
    }
    modify_table =
        find_table_link(st.sql->delete_oper->delete_list->name, st.scanner);
    if (!modify_table) {
      LOG_ERROR("Fail to get modify table for this sql.\n");
      throw Error("Fail to get modify table for this sql.");
    }
  }
  return modify_table;
}

bool Statement::first_table_contains_all_data(table_link *tables) {
  table_link *table1 = tables;
  Backend *backend = Backend::instance();
  const char *schema1 =
      table1->join->schema_name ? table1->join->schema_name : schema;
  DataSpace *space1 =
      backend->get_data_space_for_table(schema1, table1->join->table_name);

  table_link *table2 = table1->next;
  while (table2) {
    const char *schema2 =
        table2->join->schema_name ? table2->join->schema_name : schema;
    DataSpace *space2 =
        backend->get_data_space_for_table(schema2, table2->join->table_name);

    if (!stmt_session->is_dataspace_cover_session_level(space1, space2)) {
      return false;
    }
    table2 = table2->next;
  }
  return true;
}

bool Statement::table_contains_all_data(table_link *tables,
                                        table_link *first_table) {
  table_link *table1 = tables;
  Backend *backend = Backend::instance();
  const char *schema1 =
      table1->join->schema_name ? table1->join->schema_name : schema;
  DataSpace *space1 =
      backend->get_data_space_for_table(schema1, table1->join->table_name);
  table_link *table2 = first_table;
  while (table2) {
    const char *schema2 =
        table2->join->schema_name ? table2->join->schema_name : schema;
    DataSpace *space2 =
        backend->get_data_space_for_table(schema2, table2->join->table_name);

    if (!stmt_session->is_dataspace_cover_session_level(space1, space2)) {
      return false;
    }
    table2 = table2->next;
  }
  return true;
}

void Statement::get_modify_tables(vector<table_link *> *modify_tables) {
  string full_name;
  set<string> tables;
  table_link *modify_table;
  Expression *set_list = st.sql->update_oper->update_set_list;
  ListExpression *expr_list = (ListExpression *)set_list;
  expr_list_item *head = expr_list->expr_list_head;

  expr_list_item *tmp = head;
  CompareExpression *cmp_tmp = NULL;
  StrExpression *str_tmp = NULL;

  do {
    cmp_tmp = (CompareExpression *)tmp->expr;
    str_tmp = (StrExpression *)cmp_tmp->left;
    str_tmp->to_string(full_name);
    string full_table_name, tmp_schema_name, table, column;
    split_value_into_schema_table_column(full_name, tmp_schema_name, table,
                                         column);
    if (tmp_schema_name.empty()) {
      full_table_name.append(table);
      modify_table = find_table_link(full_table_name.c_str(), st.scanner, true);
    } else {
      full_table_name.append(tmp_schema_name).append(".").append(table);
      modify_table = find_table_link(full_table_name.c_str(), st.scanner);
    }
    if (lower_case_table_names) boost::to_lower(full_table_name);
    if (st.type == STMT_UPDATE && full_table_name.size() == 0 &&
        column == full_name && modify_tables->size()) {
      full_name.clear();
      tmp = tmp->next;
      continue;
    }
    if (tables.find(full_table_name) == tables.end()) {
      modify_tables->push_back(modify_table);
      tables.insert(full_table_name);
    }
    full_name.clear();
    tmp = tmp->next;
  } while (tmp != head);
}

bool Statement::can_tables_associate(table_link *tables) {
  table_link *table1 = tables;
  while (table1) {
    table_link *table2 = table1->next;
    while (table2) {
      Backend *backend = Backend::instance();
      const char *schema1 =
          table1->join->schema_name ? table1->join->schema_name : schema;
      DataSpace *space1 =
          backend->get_data_space_for_table(schema1, table1->join->table_name);
      const char *schema2 =
          table2->join->schema_name ? table2->join->schema_name : schema;
      DataSpace *space2 =
          backend->get_data_space_for_table(schema2, table2->join->table_name);

      if (!stmt_session->is_dataspace_cover_session_level(space2, space1) &&
          !stmt_session->is_dataspace_cover_session_level(space1, space2)) {
        return false;
      }
      table2 = table2->next;
    }
    table1 = table1->next;
  }

  return true;
} /* can_tables_associate */

bool Statement::is_tables_share_same_dataserver(table_link *tables) {
  table_link *table1 = tables;
  if (!table1) return false;
  Backend *backend = Backend::instance();
  const char *schema1 =
      table1->join->schema_name ? table1->join->schema_name : schema;
  DataSpace *space1 =
      backend->get_data_space_for_table(schema1, table1->join->table_name);
  table_link *table2 = table1->next;
  while (table2) {
    const char *schema2 =
        table2->join->schema_name ? table2->join->schema_name : schema;
    DataSpace *space2 =
        backend->get_data_space_for_table(schema2, table2->join->table_name);

    if (!is_share_same_server(space1, space2)) {
      return false;
    }
    table2 = table2->next;
  }
  return true;
} /* is_tables_share_same_dataserver */

bool Statement::table_contains_set_list(table_link *tables) {
  string column;
  Expression *set_list = st.sql->update_oper->update_set_list;
  ListExpression *expr_list = (ListExpression *)set_list;
  expr_list_item *head = expr_list->expr_list_head;

  expr_list_item *tmp = head;
  CompareExpression *cmp_tmp = NULL;
  StrExpression *str_tmp = NULL;
  do {
    cmp_tmp = (CompareExpression *)tmp->expr;
    str_tmp = (StrExpression *)cmp_tmp->left;
    str_tmp->to_string(column);
    if (get_column_in_table(column, tables).empty()) return false;
    tmp = tmp->next;
    column.clear();
  } while (tmp != head);

  return true;
}

/*Fulfill the vector 'key_pos_real' with the real key pos of insert stmt. If
 * the insert column list is not exist, return the define key pos, otherwise
 * get the real key pos from the insert column list.*/
void Statement::fulfill_insert_key_pos(
    name_item *column_list, vector<const char *> *key_names,
    const char *schema_name, size_t schema_len, const char *table_name,
    size_t table_len, const char *table_alias,
    vector<unsigned int> *key_pos_def, vector<unsigned int> *key_pos_real) {
  if (!column_list) {
    *key_pos_real = *key_pos_def;
    return;
  }

  vector<const char *>::iterator it = key_names->begin();
  for (; it != key_names->end(); ++it) {
    unsigned int key_pos = 0;
    const char *key_name = *it;
    key_pos = get_key_pos_from_column_list(column_list, schema_name, schema_len,
                                           table_name, table_len, table_alias,
                                           key_name);
    key_pos_real->push_back(key_pos);
  }
}

void Statement::handle_one_par_table_mul_spaces(ExecutePlan *plan) {
  table_link *par_table =
      one_table_node.only_one_table ? one_table_node.table : par_tables[0];
  stmt_type type = st.type;
  record_scan *rs = par_table->join->cur_rec_scan;
  DataSpace *dspace = one_table_node.only_one_table
                          ? one_table_node.space
                          : record_scan_all_table_spaces[par_table];
  PartitionedTable *par_space = NULL;
  bool par_is_duplicated = false;
  if (((Table *)dspace)->is_duplicated()) {
    par_space = ((DuplicatedTable *)dspace)->get_partition_table();
    par_is_duplicated = true;
#ifdef DEBUG
    /*Currenlty, duplicated code will only be used fro cross node join.*/
    ACE_ASSERT(type == STMT_INSERT_SELECT);
#endif
  } else
    par_space = (PartitionedTable *)(dspace);

  vector<const char *> *key_names = par_space->get_key_names();
  const char *schema_name =
      par_table->join->schema_name ? par_table->join->schema_name : schema;
  const char *table_name = par_table->join->table_name;
  const char *table_alias = par_table->join->alias;

  switch (type) {
    case STMT_SELECT: {
      if (spaces.size() > 1) {
        LOG_ERROR("Unsupport the select [%s], which need to be divided.\n",
                  sql);
        throw UnSupportPartitionSQL(
            "Unsuport the select, which need to be divided");
      }

      handle_one_par_table_one_spaces(plan);
      break;
    }
      // For below 5 cases, we need to check this sql match the following
      // pattern: {operate normal table by using data from partition table} or
      //{operate partition table by using data from normal table}.  Then we
      // need to merge all dataspace except the dataspace of top record_scan.
    case STMT_UPDATE:
    case STMT_DELETE: {
      if (spaces.size() > 2 ||
          record_scan_one_space_tmp_map[st.scanner] != spaces[0]) {
        LOG_ERROR("Unsupport the sql [%s], which need to be divided.\n", sql);
        throw UnSupportPartitionSQL(
            "Unsuport the sql, which need to be divided");
      }

      if (type == STMT_UPDATE) {
        vector<table_link *> modify_tables;
        get_modify_tables(&modify_tables);
        if (modify_tables.size() == 1 && modify_tables[0]) {
          update_on_one_server = table_contains_all_data(
              modify_tables[0], st.scanner->first_table);
        }
      }
      /* If the modify table is the partition table and without limit
       * constrant, we donot need to divide it into 'one modify sql + one
       * select sql'
       */
      if (((!(st.scanner->first_table->next &&
              st.scanner->first_table->next->join->cur_rec_scan ==
                  st.scanner) &&
            st.scanner->first_table == par_table) ||
           update_on_one_server || delete_on_one_server) &&
          !rs->limit) {
        handle_one_par_table_one_spaces(plan);
        break;
      }
      plan->session->set_is_complex_stmt(true);

      table_link *modify_table = get_one_modify_table();

      // Update partition key columns is forbidden currently. TODO: support it
      if (table_link_same_dataspace(modify_table, par_table) &&
          st.type == STMT_UPDATE) {
        Expression *set_list = st.sql->update_oper->update_set_list;
        string key_value = "";
        if (set_list_contain_key(set_list, key_names, schema_name, table_name,
                                 table_alias, key_value)) {
          LOG_ERROR(
              "Unsupport SQL[%s], which modify the partition key columns "
              "[%s].\n",
              sql, (*key_names)[0]);
          throw UnSupportPartitionSQL(
              "Unsupport to modify the partition key columns");
        }
      }
      vector<unsigned int> par_ids;
      unsigned int partition_num = par_space->get_partition_num();
      PartitionMethod *method = par_space->get_partition_method();
      get_partitons_one_record_scan(rs, method, partition_num, &par_ids);
      par_space->replace_real_par_id_vec_from_virtual_ids(&par_ids);

      if (par_ids.size() > 1) {
        if (!is_acceptable_join_par_table(par_table->join)) {
          LOG_ERROR("Unsupport join type for partition table with sql [%s].\n",
                    sql);
          throw UnSupportPartitionSQL(
              "Unsupport join type for partition table");
        }
        check_partition_distinct(par_table);
      }

      Backend *backend = Backend::instance();
      const char *schema_name = modify_table->join->schema_name
                                    ? modify_table->join->schema_name
                                    : schema;
      const char *table_name = modify_table->join->table_name;
      DataSpace *modify_space =
          backend->get_data_space_for_table(schema_name, table_name);

      if (!table_link_same_dataspace(modify_table, par_table)) {
        if (!modify_space->get_data_source())
          assemble_two_phase_modify_partition_plan(
              plan, NULL, method, modify_table, NULL, &par_ids);
        else
          assemble_two_phase_modify_normal_plan(plan, &par_ids, par_space,
                                                modify_table);
      } else {
        assemble_two_phase_modify_partition_plan(plan, NULL, method,
                                                 modify_table, NULL, &par_ids);
      }

      break;
    }
    case STMT_REPLACE_SELECT:
    case STMT_INSERT_SELECT: {
#ifdef DEBUG
      ACE_ASSERT(st.sql->insert_oper);
#endif
      plan->session->set_is_complex_stmt(true);
      insert_op_node *insert_op = st.sql->insert_oper;
      if (rs == st.scanner) {  // insert/replace into a partition table
        if (spaces.size() > 2 ||
            (record_scan_dependent_map.count(st.scanner->children_begin) &&
             !record_scan_dependent_map[st.scanner->children_begin].empty())) {
          // The select part can not be merged.
          LOG_ERROR("Unsupport the SQL [%s], which need to be divided.\n", sql);
          throw UnSupportPartitionSQL(
              "Unsupport the SQL, which need to be divided");
        }
      }

      table_link *modify_table = get_one_modify_table();

      vector<unsigned int> par_ids;
      PartitionMethod *method = NULL;
      if (par_is_duplicated) {
#ifdef DEBUG
        ACE_ASSERT(modify_table == par_table);
#endif
      } else {
        unsigned int partition_num = par_space->get_partition_num();
        method = par_space->get_partition_method();
        get_partitons_one_record_scan(rs, method, partition_num, &par_ids);
        par_space->replace_real_par_id_vec_from_virtual_ids(&par_ids);

        if (par_ids.size() > 1) {
          if (!is_acceptable_join_par_table(par_table->join)) {
            LOG_ERROR(
                "Unsupport join type for partition table with sql [%s].\n",
                sql);
            throw UnSupportPartitionSQL(
                "Unsupport join type for partition table");
          }
        }
      }

      if (!table_link_same_dataspace(modify_table, par_table)) {
        assemble_two_phase_modify_normal_plan(plan, &par_ids, par_space,
                                              modify_table);
      } else {
        if (par_ids.size() > 1) {
          check_partition_distinct(par_table);
        }
        vector<unsigned int> key_pos_real;
        if (!par_is_duplicated) {
          set_full_table_name(schema_name, table_name);
          vector<unsigned int> key_pos_def;
          par_space->get_key_pos_vec(schema_name, table_name, key_pos_def,
                                     stmt_session);
          int column_count = 0;
          init_auto_increment_params(plan, schema_name, table_name, par_space);
          if (auto_inc_status != NO_AUTO_INC_FIELD &&
              insert_op->insert_column_list) {
            check_column_count_name(par_space, insert_op->insert_column_list,
                                    column_count);
            rectify_auto_inc_key_pos(insert_op->insert_column_list, schema_name,
                                     strlen(schema_name), table_name,
                                     strlen(table_name), table_alias);
          }
          fulfill_insert_key_pos(insert_op->insert_column_list, key_names,
                                 schema_name, strlen(schema_name), table_name,
                                 strlen(table_name), table_alias, &key_pos_def,
                                 &key_pos_real);
        }

        assemble_two_phase_modify_partition_plan(
            plan, spaces[0], method, modify_table, &key_pos_real, NULL);
      }

      break;
    }
    case STMT_DBSCALE_ESTIMATE: {
      if (spaces.size() > 2) {
        LOG_ERROR("Unsupport estimate partition sql type, the sql is [%s].\n",
                  sql);
        throw UnSupportPartitionSQL("Unsupport estimate partition sql type");
      }
      if ((spaces[0])->get_dataspace_type() == TABLE_TYPE &&
          st.estimate_type == STMT_INSERT_SELECT) {
        Table *first_table = (Table *)spaces[0];
        if (first_table->is_partitioned()) {
          PartitionedTable *par_space = (PartitionedTable *)spaces[0];
          vector<unsigned int> *par_ids = new vector<unsigned int>();
          Statement *stmt = plan->statement;
          record_scan *rs =
              stmt->get_stmt_node()->sql->insert_oper->select_values;
          unsigned int partition_num = par_space->get_partition_num();
          PartitionMethod *method = par_space->get_partition_method();
          stmt->get_partitons_one_record_scan(rs, method, partition_num,
                                              par_ids);
          par_space->replace_real_par_id_vec_from_virtual_ids(par_ids);
          assemble_dbscale_estimate_select_partition_plan(plan, par_ids,
                                                          par_space, sql, this);
        } else {
          assemble_dbscale_estimate_select_plan(plan, spaces[0], this);
        }
      } else {
        LOG_ERROR("Unsupport estimate partition sql type, the sql is [%s].\n",
                  sql);
        throw UnSupportPartitionSQL("Unsupport estimate partition sql type");
      }
    } break;
    case STMT_CREATE_SELECT: {
      // TODO: implement it.
      break;
    }
      // Prepare stmt support need to be added in the future.
    case STMT_PREPARE: {
      // TODO:Implement it;
      break;
    }
    case STMT_TRUNCATE: {
      // TODO: implement it by seperate the partition table and normal table.
      // For partition table, send truncate to all parititons; for normal
      // table, send truncate to master.
      break;
    }
    case STMT_CREATE_TB: {
      if (par_space->get_key_names()->size() == 1) {
        const char *par_key = par_space->get_key_names()->front();
        if (!is_partition_table_contain_partition_key(par_key)) {
          LOG_ERROR("Fail to find the partition key [%s] from table %s.%s\n",
                    par_key, schema_name, table_name);
          throw Error("Fail to find partition key from table.");
        }
      } else {
        LOG_ERROR("Not specify partition key for table %s.%s\n", schema_name,
                  table_name);
        throw Error("Not specify partition key for table.");
      }
      if (st.sql->create_tb_oper->has_foreign_key) {
        LOG_INFO(
            "Create table defination contains foreign key restriction, "
            "please make sure these keys are partitioned appropriately.\n");
      }
      // TODO: implement it by send this request to parent dataspace of
      // partition table (master).
      break;
    }

    default:
      LOG_ERROR("Unsupport partition sql type, the sql is [%s].\n", sql);
      throw UnSupportPartitionSQL("Unsupport partition sql type");
      break;
  }
}

void Statement::prepare_insert_select_via_load(ExecutePlan *plan,
                                               const char *schema,
                                               const char *table) {
  if (schema == NULL || table == NULL) {
    LOG_ERROR(
        "Unsupport INSERT SELECT statement for load-insert-select"
        " without schema or table name, the sql is [%s].\n",
        sql);
    throw NotSupportedError(
        "Unsupport INSERT SELECT statement for"
        "load-insert-select without schema or table name.");
  }
  if (st.sql->insert_oper->insert_column_list) {
    LOG_ERROR(
        "Unsupport INSERT SELECT statement for load-insert-select"
        " with insert column list, the sql is [%s].\n",
        sql);
    throw NotSupportedError(
        "Unsupport INSERT SELECT statement for"
        "load-insert-select with insert column list.");
  }
  record_scan *insert_select = st.sql->insert_oper->select_values;
  unsigned int start_pos = insert_select->start_pos;
  select_sub_sql.clear();
  select_sub_sql.append(sql + start_pos - 1, strlen(sql) - start_pos + 1);

  handle_insert_select_hex_column();
  re_parser_stmt(&(st.scanner), &(plan->statement), select_sub_sql.c_str());
}

void Statement::prepare_insert_select_via_fifo(ExecutePlan *plan,
                                               DataServer *modify_server,
                                               const char *schema,
                                               const char *table,
                                               bool load_insert) {
  if (!load_insert) {
    if (access(modify_server->get_local_load_script(),
               F_OK)) {  // script not found
      LOG_ERROR("Could not find load script.\n");
      throw Error("Could not find load script, please check.");
    }
  }
  if (schema == NULL || table == NULL) {
    LOG_ERROR(
        "Unsupport INSERT SELECT statement for external-load table or "
        "load-insert-select"
        " without schema or table name, the sql is [%s].\n",
        sql);
    throw NotSupportedError(
        "Unsupport INSERT SELECT statement for external-load"
        "table or load-insert-select without schema or table name.");
  }
  if (st.sql->insert_oper->insert_column_list) {
    LOG_ERROR(
        "Unsupport INSERT SELECT statement for external-load table or "
        "load-insert-select"
        " with insert column list, the sql is [%s].\n",
        sql);
    throw NotSupportedError(
        "Unsupport INSERT SELECT statement for external-load"
        " table or load-insert-select with insert column list.");
  }
  Backend *backend = Backend::instance();
  record_scan *insert_select = st.sql->insert_oper->select_values;
  unsigned int start_pos = insert_select->start_pos;
  select_sub_sql.clear();
  select_sub_sql.append(sql + start_pos - 1, strlen(sql) - start_pos + 1);

  handle_insert_select_hex_column();

  int fifo_id = backend->create_fifo();
  char fifo_name[30];
  sprintf(fifo_name, "/tmp/tmp_fifo_%d", fifo_id);

  outfile_sql = select_sub_sql;
  outfile_sql.append(" into outfile '");
  outfile_sql.append(fifo_name);
  if (load_insert) {
    if (strlen(
            plan->session->get_session_option("load_insert_select_fields_term")
                .char_val) > 1 ||
        strlen(
            plan->session->get_session_option("load_insert_select_lines_term")
                .char_val) > 1) {
      LOG_ERROR(
          "DBScale does not support FIELDS/LINES TERMINATED BY with multiple "
          "characters "
          "when use load for insert select.\n");
      throw NotImplementedError(
          "Load insert select with FIELDS or LINES TERMINATED BY has multiple "
          "characters");
    }
    char fields_term =
        plan->session->get_session_option("load_insert_select_fields_term")
            .char_val[0];
    char lines_term =
        plan->session->get_session_option("load_insert_select_lines_term")
            .char_val[0];
    outfile_sql.append("' fields terminated by '");
    outfile_sql.append(1, fields_term);
    outfile_sql.append("' enclosed by '\"' lines terminated by '");
    outfile_sql.append(1, lines_term);
    outfile_sql.append("'");
  } else {
    outfile_sql.append("' fields terminated by ',' enclosed by '\"'");
  }
  re_parser_stmt(&(st.scanner), &(plan->statement), outfile_sql.c_str());
  plan->statement->insert_select_modify_schema = schema;
  plan->statement->insert_select_modify_table = table;
  if (load_insert) {
    plan->statement->set_default_schema(get_schema());
    plan->statement->use_load_insert_select = true;
  } else {
    plan->statement->is_insert_select_via_fifo = true;
    plan->statement->local_load_script = modify_server->get_local_load_script();
  }
}

bool key_equal_in_expr(record_scan *rs, const char *schema1,
                       const char *schema2, const char *table1,
                       const char *table2, const char *table1_alias,
                       const char *table2_alias, const char *key1,
                       const char *key2) {
  expr_list_item *str_list_head = rs->str_expr_list_head;
  expr_list_item *tmp = str_list_head;
  StrExpression *str_expr1, *str_expr2;
  condition_type cond_type;
  ConditionAndOr and_or_type;
  do {
    str_expr1 = (StrExpression *)tmp->expr;
    cond_type = check_condition_type(str_expr1);
    if (cond_type != CONDITION_TYPE_EQUAL) {
      tmp = tmp->next;
      continue;
    }
    if (!column_name_equal_key_alias(str_expr1->str_value, schema1,
                                     strlen(schema1), table1, strlen(table1),
                                     key1, table1_alias)) {
      tmp = tmp->next;
      continue;
    }

    and_or_type = is_and_or_condition(str_expr1->parent, rs);
    switch (and_or_type) {
      case CONDITION_AND: {
        Expression *peer = get_peer(str_expr1);
        if (peer->type == EXPR_STR) {
          str_expr2 = (StrExpression *)peer;
          if (column_name_equal_key_alias(str_expr2->str_value, schema2,
                                          strlen(schema2), table2,
                                          strlen(table2), key2, table2_alias)) {
            return true;
          }
        }
      }
      default:
        tmp = tmp->next;
        break;
    }
  } while (tmp != str_list_head);

  return false;
}
void Statement::assemble_show_pool_info_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show pool info plan.\n");
  ExecuteNode *node = plan->get_show_pool_info_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_pool_version_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show pool version plan.\n");
  ExecuteNode *node = plan->get_show_pool_version_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_execution_profile_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show execution profile.\n");
  ExecuteNode *node = plan->get_show_execution_profile_node();
  plan->set_start_node(node);
}
void Statement::assemble_show_lock_usage_plan(ExecutePlan *plan) {
  LOG_DEBUG("Assemble show lock usage plan.\n");
  ExecuteNode *node = plan->get_show_lock_usage_node();
  plan->set_start_node(node);
}
void Statement::handle_mul_par_table_mul_spaces(ExecutePlan *plan) {
  stmt_type type = st.type;
  if (st.has_where_rownum) {
    LOG_ERROR("Unsupport par table use rownum.\n");
    throw UnSupportPartitionSQL("Unsupport par table use rownum.");
  }

  switch (type) {
    case STMT_INSERT_SELECT:
    case STMT_REPLACE_SELECT: {
      if (par_table_num == 2) {
#ifdef DEBUG
        ACE_ASSERT(st.sql->insert_oper);
#endif
        table_link *par_insert = par_tables[1];
        table_link *par_select = par_tables[0];
        record_scan *rs_select = par_select->join->cur_rec_scan;
        Table *space_insert = (Table *)record_scan_all_table_spaces[par_insert];
        Table *space_select = (Table *)record_scan_all_table_spaces[par_select];
#ifdef DEBUG
        ACE_ASSERT(!space_select->is_duplicated());
#endif
        bool insert_is_duplicated = false;
        PartitionedTable *par_space_insert = NULL;
        if (space_insert->is_duplicated()) {
          insert_is_duplicated = true;
        } else {
          par_space_insert = (PartitionedTable *)space_insert;
        }
        const char *schema_name_insert = par_insert->join->schema_name
                                             ? par_insert->join->schema_name
                                             : schema;
        const char *table_name_insert = par_insert->join->table_name;
        const char *table_alias_insert = par_insert->join->alias;
        PartitionedTable *par_space_select = (PartitionedTable *)space_select;

        insert_op_node *insert_op = st.sql->insert_oper;
        if (spaces.size() > 2 ||
            (record_scan_dependent_map.count(st.scanner->children_begin) &&
             !record_scan_dependent_map[st.scanner->children_begin].empty())) {
          LOG_ERROR("Unsupport the SQL [%s], which need to be divided.\n", sql);
          throw UnSupportPartitionSQL(
              "Unsupport the SQL, which need to be divided.");
        }

#ifdef DEBUG
        table_link *modify_table = get_one_modify_table();
        // after can_dataspace_merge, the modify_table must be par_table
        // currently
        ACE_ASSERT(table_link_same_dataspace(par_insert, modify_table));
#endif
        vector<unsigned int> key_pos_real;
        set_full_table_name(schema_name_insert, table_name_insert);
        if (!insert_is_duplicated) {
          vector<unsigned int> key_pos_def;
          par_space_insert->get_key_pos_vec(
              schema_name_insert, table_name_insert, key_pos_def, stmt_session);
          int column_count = 0;
          init_auto_increment_params(plan, schema_name_insert,
                                     table_name_insert, par_space_insert);
          if (auto_inc_status != NO_AUTO_INC_FIELD &&
              insert_op->insert_column_list) {
            check_column_count_name(
                par_space_insert, insert_op->insert_column_list, column_count);
            rectify_auto_inc_key_pos(
                insert_op->insert_column_list, schema_name_insert,
                strlen(schema_name_insert), table_name_insert,
                strlen(table_name_insert), table_alias_insert);
          }
          vector<const char *> *key_names_insert =
              par_space_insert->get_key_names();
          fulfill_insert_key_pos(insert_op->insert_column_list,
                                 key_names_insert, schema_name_insert,
                                 strlen(schema_name_insert), table_name_insert,
                                 strlen(table_name_insert), table_alias_insert,
                                 &key_pos_def, &key_pos_real);
        }

        vector<unsigned int> par_ids;
        unsigned int partition_num = par_space_select->get_partition_num();
        PartitionMethod *method = par_space_select->get_partition_method();
        get_partitons_one_record_scan(rs_select, method, partition_num,
                                      &par_ids);
        par_space_select->replace_real_par_id_vec_from_virtual_ids(&par_ids);

        if (par_ids.size() > 1) {
          if (!is_acceptable_join_par_table(par_select->join)) {
            LOG_ERROR(
                "Unsupport join type for partition table with sql [%s].\n",
                sql);
            throw UnSupportPartitionSQL(
                "Unsupport join type for partition table");
          }
        }
        assemble_two_phase_insert_part_select_part_plan(
            plan, par_insert, &key_pos_real, par_space_select, &par_ids);
        return;
      }
      break;
    }
    case STMT_DBSCALE_ESTIMATE: {
      if (spaces.size() > 2) break;
      if (st.estimate_type == STMT_INSERT_SELECT) {
        PartitionedTable *par_space = (PartitionedTable *)spaces[0];
        vector<unsigned int> *par_ids = new vector<unsigned int>();
        Statement *stmt = plan->statement;
        record_scan *rs =
            stmt->get_stmt_node()->sql->insert_oper->select_values;
        unsigned int partition_num = par_space->get_partition_num();
        PartitionMethod *method = par_space->get_partition_method();
        stmt->get_partitons_one_record_scan(rs, method, partition_num, par_ids);
        par_space->replace_real_par_id_vec_from_virtual_ids(par_ids);
        assemble_dbscale_estimate_select_partition_plan(plan, par_ids,
                                                        par_space, sql, this);
        return;
      }
    }
    default:
      break;
  }
  LOG_ERROR("Unsupport partition sql, the sql is [%s].\n", sql);
  throw UnSupportPartitionSQL(
      "Unsupport partition sql type for mul par table mul spaces.");
}

bool Statement::can_two_par_table_record_scan_merge(record_scan *rs1,
                                                    record_scan *rs2) {
  table_link *par_table1 = record_scan_par_table_map[rs1];
  table_link *par_table2 = record_scan_par_table_map[rs2];

  // the same upper select
  if (rs1->upper == rs2->upper && rs1->subquerytype == SUB_SELECT_TABLE &&
      rs2->subquerytype == SUB_SELECT_TABLE) {
    const char *schema_name1 =
        par_table1->join->schema_name ? par_table1->join->schema_name : schema;
    const char *table_name1 = par_table1->join->table_name;
    const char *schema_name2 =
        par_table2->join->schema_name ? par_table2->join->schema_name : schema;
    const char *table_name2 = par_table2->join->table_name;
    const char *tb_alias1 = par_table1->join->alias;
    const char *tb_alias2 = par_table2->join->alias;
    PartitionedTable *par_space1 =
        (PartitionedTable *)record_scan_all_table_spaces[par_table1];
    PartitionedTable *par_space2 =
        (PartitionedTable *)record_scan_all_table_spaces[par_table2];

    // the same table
    if (!strcasecmp(schema_name1, schema_name2) &&
        !strcasecmp(table_name1, table_name2)) {
      vector<const char *> *key_names1 = par_space1->get_key_names();
      vector<const char *> *key_names2 = par_space2->get_key_names();
      if (key_names1->size() == 1 && key_names2->size() == 1) {
        // group by on partition key
        order_item *groupby1 = get_last_groupby(rs1->group_by_list);
        order_item *groupby2 = get_last_groupby(rs2->group_by_list);
        if (groupby1 && groupby2 &&
            groupby_on_par_key(groupby1, schema_name1, table_name1, tb_alias1,
                               key_names1->at(0)) &&
            groupby_on_par_key(groupby2, schema_name2, table_name2, tb_alias2,
                               key_names2->at(0))) {
          const char *outer_alias1 = rs1->join_belong->alias;
          const char *outer_alias2 = rs2->join_belong->alias;
          if (key_equal_in_expr(rs1->upper, schema_name1, schema_name2,
                                table_name1, table_name2, outer_alias1,
                                outer_alias2, key_names1->at(0),
                                key_names2->at(0))) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

bool Statement::can_par_global_table_record_scan_merge(record_scan *rs1,
                                                       record_scan *rs2) {
  table_link *par_table1 = record_scan_par_table_map[rs1];
  DataSpace *global_space = record_scan_one_space_tmp_map[rs2];
  if (rs2->is_select_union)
    return false;
  else if (!global_space)
    return true;

  if (rs1->order_by_list || rs1->limit) return false;
  // the same upper select
  if (rs1->upper == rs2->upper && rs1->subquerytype == SUB_SELECT_TABLE &&
      rs2->subquerytype == SUB_SELECT_TABLE) {
    const char *schema_name1 =
        par_table1->join->schema_name ? par_table1->join->schema_name : schema;
    const char *table_name1 = par_table1->join->table_name;
    const char *tb_alias1 = par_table1->join->alias;
    PartitionedTable *par_space1 =
        (PartitionedTable *)record_scan_all_table_spaces[par_table1];

    vector<const char *> *key_names1 = par_space1->get_key_names();
    if (key_names1->size() == 1) {
      // group by on partition key
      order_item *groupby1 = get_last_groupby(rs1->group_by_list);
      if (groupby1 && groupby_on_par_key(groupby1, schema_name1, table_name1,
                                         tb_alias1, key_names1->at(0))) {
        if (stmt_session->is_dataspace_cover_session_level(par_space1,
                                                           global_space))
          return true;
      }
    }
  }
  return false;
}

void Statement::assemble_modify_all_partition_plan(
    ExecutePlan *plan, PartitionedTable *par_space) {
  vector<unsigned int> par_ids;
  unsigned int partition_num = par_space->get_real_partition_num();
  par_ids.clear();
  unsigned int i = 0;
  for (; i < partition_num; ++i) par_ids.push_back(i);

  assemble_mul_par_modify_plan(plan, &par_ids, par_space);
  if (st.type <= STMT_DDL_START || st.type >= STMT_DDL_END) {
    plan->session->set_execute_plan_touch_partition_nums(-1);
  }
}

void Statement::build_insert_sql_assign_auto_inc(string &insert_sql,
                                                 Expression *value_expr,
                                                 int64_t curr_auto_inc_val) {
  char auto_inc_str_val[21];
  snprintf(auto_inc_str_val, sizeof(auto_inc_str_val), "%ld",
           curr_auto_inc_val);
  if (auto_inc_status != AUTO_INC_NOT_SPECIFIED) {
    int pos_before = value_expr->start_pos;
    int pos_after = value_expr->end_pos;
    insert_sql.append(sql, pos_before - 1);
    insert_sql.append(auto_inc_str_val);
    insert_sql.append(&sql[pos_after]);
  } else if (auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    insert_sql.append(sql);
    insert_sql.append(", ");
    insert_sql.append(auto_inc_key_name);
    insert_sql.append("=");
    insert_sql.append(auto_inc_str_val);
  }
}

void Statement::build_insert_sql_auto_inc(string &insert_sql,
                                          insert_op_node *insert,
                                          expr_list_item *auto_inc_expr_item,
                                          int64_t curr_auto_inc_val) {
  build_insert_sql_prefix(insert_sql, insert);
  append_insert_sql_row_auto_inc(insert_sql, insert->insert_values,
                                 auto_inc_expr_item, curr_auto_inc_val);
  boost::erase_tail(insert_sql, 1);
}

void Statement::build_insert_sql_prefix(string &sql_prefix,
                                        insert_op_node *insert) {
  sql_prefix.clear();
  if (auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    sql_prefix.append(sql, insert->column_list_end_pos);
    sql_prefix.append(", ");
    sql_prefix.append(auto_inc_key_name);
    sql_prefix.append(") VALUES ");
  } else {
    sql_prefix.append(sql, insert->keyword_values_end_pos);
    sql_prefix.append(" ");
  }
}

void Statement::append_insert_sql_row(string &part_sql,
                                      ListExpression *expr_list) {
  int start_pos = expr_list->expr_list_head->expr->start_pos - 1;
  int end_pos = expr_list->expr_list_tail->expr->end_pos - 1;
  part_sql.append("(");
  part_sql.append(&sql[start_pos], end_pos - start_pos + 1);
  part_sql.append("),");
}

void Statement::append_insert_sql_row_auto_inc(
    string &part_sql, insert_row *row, expr_list_item *auto_inc_expr_item,
    int64_t curr_auto_inc_val) {
  char auto_inc_str_val[21];
  int start_pos = 0;
  int end_pos = 0;
  snprintf(auto_inc_str_val, 21, "%ld", curr_auto_inc_val);
  if (auto_inc_status != AUTO_INC_NOT_SPECIFIED) {
    start_pos = ((ListExpression *)row->column_values)
                    ->expr_list_head->expr->start_pos -
                1;
    end_pos = auto_inc_expr_item->expr->start_pos - 1;
    part_sql.append("(");
    part_sql.append(&sql[start_pos], end_pos - start_pos);
    part_sql.append(auto_inc_str_val);
    if (auto_inc_expr_item !=
        ((ListExpression *)row->column_values)->expr_list_tail) {
      part_sql.append(", ");
      start_pos = auto_inc_expr_item->next->expr->start_pos - 1;
      end_pos = ((ListExpression *)row->column_values)
                    ->expr_list_tail->expr->end_pos -
                1;
      part_sql.append(&sql[start_pos], end_pos - start_pos + 1);
    }
    part_sql.append("),");
  } else if (auto_inc_status == AUTO_INC_NOT_SPECIFIED) {
    start_pos = ((ListExpression *)row->column_values)
                    ->expr_list_head->expr->start_pos -
                1;
    end_pos =
        ((ListExpression *)row->column_values)->expr_list_tail->expr->end_pos -
        1;
    part_sql.append("(");
    part_sql.append(&sql[start_pos], end_pos - start_pos + 1);
    part_sql.append(", ");
    part_sql.append(auto_inc_str_val);
    part_sql.append("),");
  }
}

void Statement::check_value_count_auto_increment(int column_count,
                                                 int value_count,
                                                 PartitionedTable *part_space,
                                                 int row_index) {
  int field_count = 0;
  ACE_RW_Mutex *tc_lock = NULL;

  if (!column_count) {
    tc_lock = part_space->get_table_columns_lock(full_table_name);
    tc_lock->acquire_read();
    field_count = part_space->get_table_field_count(full_table_name);
    ;
    tc_lock->release();
  }
  bool is_column_count_ok =
      column_count ? value_count == column_count : value_count == field_count;
  if (!is_column_count_ok) {
    char msg[70];
    snprintf(msg, sizeof(msg), "%s%d",
             "Column count doesn't match value count at row ", row_index + 1);
    LOG_ERROR("%s\n", msg);
    throw dbscale::sql::SQLError(msg, "21S01",
                                 ERROR_COLUMN_COUNT_NOT_MATCH_CODE);
  }
}

expr_list_item *Statement::get_auto_inc_expr(expr_list_item *head) {
  expr_list_item *item = NULL;
  if (auto_inc_status != AUTO_INC_NOT_SPECIFIED) {
    item = head;
    int i = 0;
    for (; i < auto_increment_key_pos; ++i) {
      item = item->next;
    }
  }
  return item;
}

void Statement::rectify_auto_inc_key_pos(
    name_item *column_list, const char *schema_name, size_t schema_len,
    const char *table_name, size_t table_len, const char *table_alias) {
  int ret = get_auto_inc_pos_from_column_list(
      column_list, schema_name, schema_len, table_name, table_len, table_alias);
  if (auto_increment_key_pos != ret) {
    LOG_DEBUG("Set real auto_increment_key_pos, old=%d, new=%d\n",
              auto_increment_key_pos, ret);
    auto_increment_key_pos = ret;
  }
}

void fulfill_set_col_names(
    ListExpression *set_list,
    map<string, CompareExpression *, strcasecomp> &set_col_names) {
  expr_list_item *head = set_list->expr_list_head;
  expr_list_item *tmp = head;
  CompareExpression *cmp_tmp = NULL;
  StrExpression *str_tmp = NULL;
  do {
    cmp_tmp = (CompareExpression *)tmp->expr;
    str_tmp = (StrExpression *)cmp_tmp->left;
    string set_col_name = str_tmp->str_value;
    size_t pos = set_col_name.find_first_of('.');
    if (pos != string::npos) {
      if (set_col_name.find_first_of('.', pos + 1) != string::npos) {
        throw Error("do not support column name contain more than 1 dot(.)");
      }
      set_col_name = set_col_name.substr(pos + 1);
    }
    set_col_names[set_col_name] = cmp_tmp;
    tmp = tmp->next;
  } while (tmp != head);
}

void Statement::check_auto_inc_value_in_update_set_list(
    Expression *set_list,
    map<string, CompareExpression *, strcasecomp> &set_col_names,
    PartitionedTable *par_space, ExecutePlan *plan, const char *schema_name,
    const char *table_name) {
  ListExpression *expr_list = (ListExpression *)set_list;
  fulfill_set_col_names(expr_list, set_col_names);
  if (set_col_names.count(auto_inc_key_name)) {
    // auto inc value should
    // 1. not 0 or NULL
    // 2. be valid at dbscale level
    CompareExpression *ce = set_col_names[auto_inc_key_name];
    Expression *se = ce->right;
    string val = se->str_value ? se->str_value : "";
    if (val.empty() || !is_natural_number(val.c_str()) ||
        !strcmp(val.c_str(), "0")) {
      LOG_ERROR(
          "if set, should set auto_inc column exactly when modify partition "
          "key column value. SQL[%s]\n",
          sql);
      throw Error(
          "if set, should set auto increment column exactly (not 0 or NULL) "
          "when modify partition key column value");
    } else {  // check auto inc validity
      IntExpression ie(val.c_str());
      get_auto_inc_value_one_insert_row(par_space, plan, schema_name,
                                        table_name, &ie);
    }
  }
}

void check_par_key_eq_in_where_clause(
    Expression *where_expr, const char *schema_name, const char *table_name,
    vector<const char *> *key_names, const char *table_alias,
    string &key_value_in_where_clause, bool &par_key_eq_in_where_clause)

{
#ifdef DEBUG
  ACE_ASSERT(where_expr);
#endif
  vector<CompareExpression *> where_comp_exprs;
  if (where_expr->type == EXPR_EQ) {
    where_comp_exprs.push_back((CompareExpression *)where_expr);
  } else {
    ((BoolBinaryCaculateExpression *)where_expr)
        ->get_all_comp_exprs(&where_comp_exprs);
  }
  vector<CompareExpression *>::iterator it = where_comp_exprs.begin();
  for (; it != where_comp_exprs.end(); ++it) {
    CompareExpression *cmp_tmp = *it;
    if (cmp_tmp->type != EXPR_EQ) {
      continue;
    }
    StrExpression *str_tmp = (StrExpression *)cmp_tmp->left;
    if (column_name_equal_key_alias(
            str_tmp->str_value, schema_name, strlen(schema_name), table_name,
            strlen(table_name), key_names->at(0), table_alias)) {
      Expression *right_expr = cmp_tmp->right;
      const char *str_val = right_expr->str_value;
      key_value_in_where_clause = str_val ? str_val : "";
      par_key_eq_in_where_clause = true;
      break;
    }
  }
}

bool check_is_all_plain_value(Expression *expr) {
  bool ret = true;
  if (!expr) {
    ret = false;
  } else if (expr->type != EXPR_EQ) {
    if (expr->type == EXPR_AND || expr->type == EXPR_OR ||
        expr->type == EXPR_XOR) {
      ret = ((BoolBinaryCaculateExpression *)expr)->is_all_plain_expr();
    } else if (expr->is_plain_expression()) {
      ret = true;
    } else {
      ret = false;
    }
  } else {
    Expression *right = ((CompareExpression *)expr)->right;
    ret = right->is_plain_expression();
  }
  return ret;
}

bool check_is_all_and_expr(Expression *where_expr) {
  bool is_where_clause_and_expr = true;
  if (!where_expr) {
    is_where_clause_and_expr = false;
  } else if (where_expr->type != EXPR_EQ) {
    if (where_expr->type == EXPR_AND) {  // should be like: a=b and c<d and e=f
                                         // ... where 'a' is partition key
      is_where_clause_and_expr =
          ((BoolBinaryCaculateExpression *)where_expr)->is_all_and_expr();
    } else {  //"where 1" is not valid
      is_where_clause_and_expr = false;
    }
  }
  return is_where_clause_and_expr;
}

void fetch_rows_to_update(DataSpace *dataspace_where_clause, string &fetch_sql,
                          vector<vector<string> > &result_vec,
                          vector<vector<bool> > &result_null_vec,
                          int &fetch_row_count, bool is_count) {
  Connection *conn = NULL;
  try {
    conn = dataspace_where_clause->get_connection(NULL);
    if (!conn) throw Error("Fail to get connection to fetch_rows_to_update");
    conn->query_for_all_column(fetch_sql.c_str(), &result_vec,
                               &result_null_vec);
    conn->get_pool()->add_back_to_free(conn);
    conn = NULL;
  } catch (...) {
    if (conn) {
      conn->get_pool()->add_back_to_dead(conn);
      conn = NULL;
    }
    throw;
  }
  if (is_count) {
    fetch_row_count = atoi(result_vec[0][0].c_str());
  } else {
    fetch_row_count = result_vec.size();
  }
  LOG_DEBUG("get [%d] rows\n", fetch_row_count);
}

void build_update_sql_insert_part(
    string &insert_head, string &update_sql_insert_part,
    int fetch_col_names_size, int fetch_row_count,
    map<string, TableColumnInfo *, strcasecomp> *column_info_map,
    vector<vector<string> > &result_vec, vector<vector<bool> > &result_null_vec,
    vector<string> &fetch_col_names, const char *charset) {
  for (int i = 0; i < fetch_row_count;) {
    if (result_vec.empty()) {
      // all column are in set list
      update_sql_insert_part.append(insert_head);
    } else {
      // some column are in result_vec, we need append it at end of insert_head
      string insert_sql = insert_head;
      for (int j = 0; j < fetch_col_names_size; ++j) {
        insert_sql.append(",`");
        insert_sql.append(fetch_col_names[j]);
        if (result_null_vec[i][j]) {
          insert_sql.append("`=NULL");
        } else {
          if ((*column_info_map)[fetch_col_names[j]]->is_data_type_blob_like) {
            throw Error("can not update row when blob column not NULL");
          }
          insert_sql.append("`=\'");
          if ((*column_info_map)[fetch_col_names[j]]
                  ->is_data_type_string_like) {
            string val = result_vec[i][j];
            add_translation_char_for_special_char(val, charset);
            insert_sql.append(val);
          } else {
            insert_sql.append(result_vec[i][j]);
          }
          insert_sql.append("\'");
        }
      }
      update_sql_insert_part.append(insert_sql);
    }
    ++i;
    if (i < fetch_row_count) {
      update_sql_insert_part.append(";");
    }
  }
}

void build_fetch_sql_for_update_sql(
    string &fetch_sql, int set_col_count, int table_column_count,
    map<string, TableColumnInfo *, strcasecomp> *column_info_map,
    map<string, CompareExpression *, strcasecomp> &set_col_names,
    vector<string> &fetch_col_names, string &new_schema_tmp,
    const char *table_name, const char *sql, stmt_node &st) {
  fetch_sql = "select ";
  if (set_col_count == table_column_count) {
    fetch_sql.append("count(*)");
  } else {
    map<string, TableColumnInfo *, strcasecomp>::iterator ittc =
        column_info_map->begin();
    for (; ittc != column_info_map->end();) {
      if (!set_col_names.count(ittc->second->column_name)) {
        fetch_sql.append("`");
        fetch_sql.append(ittc->second->column_name);
        fetch_sql.append("`");
        fetch_sql.append(", ");
        fetch_col_names.push_back(ittc->second->column_name);
      }
      ++ittc;
    }
    boost::erase_tail(fetch_sql, 2);
  }

  fetch_sql.append(" from `");
  fetch_sql.append(new_schema_tmp);
  fetch_sql.append("`.`");
  fetch_sql.append(table_name);
  fetch_sql.append("` ");
  fetch_sql.append(sql, st.scanner->where_pos - 1, strlen(sql));
  LOG_DEBUG("generate sql [%s] to fetch rows for update sql [%s]\n",
            fetch_sql.c_str(), sql);
}

void Statement::handle_one_par_table_one_spaces(ExecutePlan *plan) {
  table_link *par_table =
      one_table_node.only_one_table ? one_table_node.table : par_tables[0];
  PartitionedTable *par_space =
      (PartitionedTable *)(one_table_node.only_one_table ? one_table_node.space
                                                         : spaces[0]);
  record_scan *rs = par_table->join->cur_rec_scan;
  vector<const char *> *key_names = par_space->get_key_names();
  const char *schema_name =
      par_table->join->schema_name ? par_table->join->schema_name : schema;
  const char *table_name = par_table->join->table_name;
  const char *table_alias = par_table->join->alias;
  stmt_type type = st.type;

  set_sql_schema(schema_name);

  set_full_table_name(schema_name, table_name);

  if (st.has_where_rownum) {
    LOG_ERROR("Unsupport par table use rownum.\n");
    throw UnSupportPartitionSQL("Unsupport par table use rownum.");
  }

  switch (type) {
    case STMT_INSERT:
    case STMT_REPLACE: {
#ifndef DBSCALE_TEST_DISABLE
      if (on_test_stmt) {
        dbscale_test_info *test_info = plan->session->get_dbscale_test_info();
        if (!strcasecmp(test_info->test_case_name.c_str(), "auto_increment") &&
            !strcasecmp(test_info->test_case_operation.c_str(),
                        "table_non_exist") &&
            !strcasecmp(sql, "INSERT INTO test.t1 (c1,c2,c3) VALUES (0,0,0)")) {
          full_table_name = string("test.table_non_exist");
          table_name = "table_not_exist";
          LOG_DEBUG(
              "Do dbscale test operation 'table_non_exist' for case "
              "'auto_increment'\n");
        }
      }
#endif
      size_t schema_len = strlen(schema_name);
      size_t table_len = strlen(table_name);

      int64_t curr_auto_inc_val = 0;
      insert_op_node *insert = NULL;
      if (plan->session->is_call_store_procedure()) {
        insert = st.sql->insert_oper;
      } else {
        insert = get_latest_stmt_node()->sql->insert_oper;
      }
      vector<unsigned int> key_pos_def;
      par_space->get_key_pos_vec(schema_name, table_name, key_pos_def,
                                 stmt_session);
      vector<unsigned int> key_pos_real;
      unsigned int part_id = 0;
      PartitionMethod *method = par_space->get_partition_method();
      int column_count = 0;
      int value_count = 0;

      init_auto_increment_params(plan, schema_name, table_name, par_space);

      if (insert->insert_column_list && auto_inc_status != NO_AUTO_INC_FIELD) {
        if (auto_inc_lock_mode != AUTO_INC_LOCK_MODE_INTERLEAVED ||
            insert->insert_row_num > 1) {
          /*If only one row and
           *  auto_inc_lock_mode == AUTO_INC_LOCK_MODE_INTERLEAVED
           *
           *  The checking work can be pushed down to backend server.*/
          check_column_count_name(par_space, insert->insert_column_list,
                                  column_count);
        }
        rectify_auto_inc_key_pos(insert->insert_column_list, schema_name,
                                 schema_len, table_name, table_len,
                                 table_alias);
      }
      fulfill_insert_key_pos(insert->insert_column_list, key_names, schema_name,
                             schema_len, table_name, table_len, table_alias,
                             &key_pos_def, &key_pos_real);
      int enable_last_insert_id_session =
          plan->handler->get_session()
              ->get_session_option("enable_last_insert_id")
              .int_val;
      if (insert->insert_row_num > 1) {
        int row_index = 0;
        string sql_prefix;
        expr_list_item *auto_inc_expr_item = NULL;
        multi_insert_id_sqls.clear();
        build_insert_sql_prefix(sql_prefix, insert);
        bool need_update_last_insert_id = enable_last_insert_id_session;
        if (auto_inc_status != NO_AUTO_INC_FIELD &&
            need_update_last_insert_id) {
          old_last_insert_id =
              plan->handler->get_session()->get_last_insert_id();
        }
        insert_row *row = insert->insert_values;
        ACE_Thread_Mutex *stmt_lock = NULL;
        if (auto_inc_status != NO_AUTO_INC_FIELD) {
          stmt_lock = par_space->get_stmt_autoinc_lock(full_table_name);
          if (stmt_lock) stmt_lock->acquire();
        }
        try {
          for (; row_index < insert->insert_row_num; ++row_index) {
            ListExpression *expr_list = (ListExpression *)row->column_values;
            if (auto_inc_status != NO_AUTO_INC_FIELD) {
              value_count = expr_list->list_size;
              check_value_count_auto_increment(column_count, value_count,
                                               par_space, row_index);
              auto_inc_expr_item = get_auto_inc_expr(expr_list->expr_list_head);
              curr_auto_inc_val = get_auto_inc_value_multi_insert_row(
                  par_space, plan, schema_name, table_name,
                  auto_inc_expr_item ? auto_inc_expr_item->expr : NULL,
                  insert->insert_row_num - row_index);
            }
            part_id = get_partition_one_insert_row(
                row->column_values,
                key_pos_real.size() ? &key_pos_real : &key_pos_def,
                curr_auto_inc_val, value_count, method, "");
            part_id = par_space->get_real_par_id_from_virtual_id(part_id);
            if (!multi_insert_id_sqls.count(part_id)) {
              multi_insert_id_sqls[part_id].append(sql_prefix);
            }
            if (auto_inc_status == NO_AUTO_INC_FIELD ||
                auto_inc_status == AUTO_INC_NO_NEED_MODIFY) {
              append_insert_sql_row(multi_insert_id_sqls[part_id], expr_list);
            } else {
              append_insert_sql_row_auto_inc(multi_insert_id_sqls[part_id], row,
                                             auto_inc_expr_item,
                                             curr_auto_inc_val);
              if (need_update_last_insert_id &&
                  (auto_inc_status == AUTO_INC_VALUE_NULL ||
                   auto_inc_status == AUTO_INC_NOT_SPECIFIED)) {
                plan->handler->get_session()->set_last_insert_id(
                    curr_auto_inc_val);
                need_update_last_insert_id = false;
              }
            }
            row = row->next;
          }
        } catch (...) {
          if (auto_inc_status != NO_AUTO_INC_FIELD) {
            rollback_auto_inc_params(plan, par_space);
            if (stmt_lock) stmt_lock->release();
          }
          throw;
        }
        if (auto_inc_status != NO_AUTO_INC_FIELD) {
          if (stmt_lock) stmt_lock->release();
        }

        map<unsigned int, string>::iterator it = multi_insert_id_sqls.begin();
        string on_dup_info;
        if (insert->on_dup_update_list_end_pos -
                insert->on_dup_update_list_start_pos >
            1) {
          on_dup_info = string(" ") +
                        string(sql, insert->on_dup_update_list_start_pos - 1,
                               insert->on_dup_update_list_end_pos -
                                   insert->on_dup_update_list_start_pos + 1);
        }
        for (; it != multi_insert_id_sqls.end(); ++it) {
          boost::erase_tail(it->second, 1);
          if (!on_dup_info.empty()) {
            it->second.append(on_dup_info);
          }
        }

        if (multi_insert_id_sqls.size() == 1) {  // only one partition is used.
          DataSpace *dataspace = par_space->get_partition(part_id);
          assemble_one_partition_plan(plan, dataspace,
                                      multi_insert_id_sqls[part_id]);
          plan->session->set_execute_plan_touch_partition_nums(1);
        } else {
          assemble_mul_part_insert_plan(plan, par_space);
          if (multi_insert_id_sqls.size() ==
              par_space->get_real_partition_num()) {
            plan->session->set_execute_plan_touch_partition_nums(-1);
          } else {
            plan->session->set_execute_plan_touch_partition_nums(
                multi_insert_id_sqls.size());
          }
        }
        break;
      } else if (insert->insert_values) {
        expr_list_item *auto_inc_expr_item = NULL;
        if (auto_inc_status != NO_AUTO_INC_FIELD) {
          ListExpression *le =
              (ListExpression *)insert->insert_values->column_values;
          expr_list_item *head = le->expr_list_head;
          value_count = le->list_size;
          if (auto_inc_lock_mode != AUTO_INC_LOCK_MODE_INTERLEAVED ||
              (column_count > 0 && column_count != value_count)) {
            /*Only has one row, and AUTO_INC_LOCK_MODE_INTERLEAVED, so the
             * error checking can be pushed down to backend server. */
            check_value_count_auto_increment(column_count, value_count,
                                             par_space, 0);
          }
          auto_inc_expr_item = get_auto_inc_expr(head);
          curr_auto_inc_val = get_auto_inc_value_one_insert_row(
              par_space, plan, schema_name, table_name,
              auto_inc_expr_item ? auto_inc_expr_item->expr : NULL);
        }

        part_id = get_partition_one_insert_row(
            insert->insert_values->column_values,
            key_pos_real.size() ? &key_pos_real : &key_pos_def,
            curr_auto_inc_val, value_count, method, "");
        part_id = par_space->get_real_par_id_from_virtual_id(part_id);
        if (auto_inc_status != NO_AUTO_INC_FIELD &&
            auto_inc_status != AUTO_INC_NO_NEED_MODIFY) {
          build_insert_sql_auto_inc(insert_sql, insert, auto_inc_expr_item,
                                    curr_auto_inc_val);
          if (enable_last_insert_id_session &&
              (auto_inc_status == AUTO_INC_VALUE_NULL ||
               auto_inc_status == AUTO_INC_NOT_SPECIFIED)) {
            plan->handler->get_session()->set_last_insert_id(curr_auto_inc_val);
          }
          DataSpace *dataspace = par_space->get_partition(part_id);
          assemble_one_partition_plan(plan, dataspace, insert_sql);
          plan->session->set_execute_plan_touch_partition_nums(1);
          return;
        }
      } else if (insert->assign_list) {
        Expression *value_expr = NULL;
        if (auto_inc_status != NO_AUTO_INC_FIELD) {
          if (auto_inc_lock_mode != AUTO_INC_LOCK_MODE_INTERLEAVED)
            check_column_count_name(par_space, insert->assign_list);
          expr_list_item *expr_item = NULL;
          if (get_auto_inc_from_insert_assign(insert->assign_list, schema_name,
                                              schema_len, table_name, table_len,
                                              table_alias, &expr_item)) {
            value_expr = ((CompareExpression *)expr_item->expr)->right;
          } else {
            set_auto_inc_status(AUTO_INC_NOT_SPECIFIED);
          }
          curr_auto_inc_val = get_auto_inc_value_one_insert_row(
              par_space, plan, schema_name, table_name, value_expr);
        }

        part_id = get_partition_insert_asign(
            insert->assign_list, schema_name, schema_len, table_name, table_len,
            table_alias, key_names, curr_auto_inc_val, method);
        part_id = par_space->get_real_par_id_from_virtual_id(part_id);
        if (auto_inc_status != NO_AUTO_INC_FIELD &&
            auto_inc_status != AUTO_INC_NO_NEED_MODIFY) {
          build_insert_sql_assign_auto_inc(insert_sql, value_expr,
                                           curr_auto_inc_val);
          if (enable_last_insert_id_session &&
              (auto_inc_status == AUTO_INC_VALUE_NULL ||
               auto_inc_status == AUTO_INC_NOT_SPECIFIED)) {
            plan->handler->get_session()->set_last_insert_id(curr_auto_inc_val);
          }
          DataSpace *dataspace = par_space->get_partition(part_id);
          assemble_one_partition_plan(plan, dataspace, insert_sql);
          plan->session->set_execute_plan_touch_partition_nums(1);
          return;
        }
      } else {
        LOG_ERROR("Unsupport insert stmt [%s].\n", sql);
        throw UnSupportPartitionSQL("Unsupport insert stmt");
      }
      DataSpace *dataspace = par_space->get_partition(part_id);
      assemble_one_partition_plan(plan, dataspace);
      plan->session->set_execute_plan_touch_partition_nums(1);
      return;
    }
    case STMT_UPDATE: {
      Expression *set_list = st.sql->update_oper->update_set_list;
      string key_value_in_set_list = "";
      if (set_list_contain_key(set_list, key_names, schema_name, table_name,
                               table_alias, key_value_in_set_list)) {
        // update set list constain partition key
        Expression *where_expr = st.sql->update_oper->condition;
        if (!check_is_all_and_expr(where_expr)) {
          LOG_ERROR(
              "only support update partition key with where clause with type "
              "AND "
              "or directly equivalent condition. Unsupport SQL[%s]\n",
              sql);
          throw UnSupportPartitionSQL(
              "only support update partition key with where clause with type "
              "AND "
              "or directly equivalent condition");
        }
        expr_list_item *head = ((ListExpression *)set_list)->expr_list_head;
        expr_list_item *tmp = head;
        do {
          CompareExpression *cmp_tmp = (CompareExpression *)tmp->expr;
          Expression *right = cmp_tmp->right;
          if (!right->is_plain_expression()) {
            throw UnSupportPartitionSQL(
                "only support plain expression in set list");
          }
          tmp = tmp->next;
        } while (tmp != head);
        if (!check_is_all_plain_value(where_expr)) {
          throw UnSupportPartitionSQL(
              "only support plain or NULL expression in where clause");
        }

        // the parititon key in where clause should be an EXPR_EQ, like c1=2
        string key_value_in_where_clause = "";
        bool par_key_eq_in_where_clause = false;
        check_par_key_eq_in_where_clause(
            where_expr, schema_name, table_name, key_names, table_alias,
            key_value_in_where_clause, par_key_eq_in_where_clause);
        if (!par_key_eq_in_where_clause) {
          LOG_ERROR(
              "only support update partition key when where "
              "clause also has partition key, and it should be in equivalent "
              "condition. Unsupport SQL[%s]\n",
              sql);
          throw UnSupportPartitionSQL(
              "only support update partition key when where "
              "clause also has partition key, and it should be in equivalent "
              "condition");
        }

        if (key_value_in_set_list.empty() ||
            key_value_in_where_clause.empty()) {
          LOG_ERROR(
              "fail to get partition key value from sql, key_value_in_set_list "
              "[%s], key_value_in_where_clause [%s], sql [%s]\n",
              key_value_in_set_list.c_str(), key_value_in_where_clause.c_str(),
              sql);
          if (key_value_in_set_list.empty()) {
            throw Error("fail to get partition key value from set list");
          }
          if (key_value_in_where_clause.empty()) {
            throw Error("fail to get partition key value from where clause");
          }
        }

        unsigned int part_id_set_list = -1;
        unsigned int part_id_where_clause = -2;
        vector<const char *> key_values;
        key_values.push_back(key_value_in_set_list.c_str());
        PartitionMethod *method = par_space->get_partition_method();
        string replace_null_char =
            plan->session->get_query_sql_replace_null_char();
        part_id_set_list =
            method->get_partition_id(&key_values, replace_null_char);
        part_id_set_list =
            par_space->get_real_par_id_from_virtual_id(part_id_set_list);

        if (key_value_in_set_list != key_value_in_where_clause) {
          key_values.clear();
          key_values.push_back(key_value_in_where_clause.c_str());
          part_id_where_clause =
              method->get_partition_id(&key_values, replace_null_char);
          part_id_where_clause =
              par_space->get_real_par_id_from_virtual_id(part_id_where_clause);
        }

        map<string, CompareExpression *, strcasecomp> set_col_names;
        bool update_del_quick_limit =
            plan->session->get_session_option("update_delete_quick_limit")
                .int_val;
        init_auto_increment_params(plan, schema_name, table_name, par_space);
        if (!auto_inc_key_name.empty()) {
          check_auto_inc_value_in_update_set_list(set_list, set_col_names,
                                                  par_space, plan, schema_name,
                                                  table_name);
        }

        DataSpace *dataspace_set_list =
            par_space->get_partition(part_id_set_list);
        if ((key_value_in_set_list == key_value_in_where_clause) ||
            (part_id_set_list == part_id_where_clause)) {
          // the update sql do not move row from one partition to another, we
          // can try to generate direct execution plan
          assemble_direct_exec_plan(plan, dataspace_set_list);
          plan->session->set_execute_plan_touch_partition_nums(1);
          return;
        }

        // situation: part_id_set_list != part_id_where_clause
        if (!plan->session->is_in_transaction()) {
          LOG_ERROR(
              "session should in transaction when modify "
              "partition key column value from one partition to another. "
              "SQL[%s]\n",
              sql);
          throw Error(
              "session should in transaction when modify partition key column "
              "value from one partition to another");
        }
        if (set_col_names.empty()) {
          ListExpression *expr_list = (ListExpression *)set_list;
          fulfill_set_col_names(expr_list, set_col_names);
        }

        if (update_del_quick_limit) {
          DataSpace *dataspace_where_clause = NULL;
          string new_schema_tmp;
          dataspace_where_clause =
              par_space->get_partition(part_id_where_clause);
          adjust_shard_schema(par_table->join->schema_name, schema,
                              new_schema_tmp,
                              dataspace_where_clause->get_virtual_machine_id(),
                              dataspace_where_clause->get_partition_id());
          update_sql_delete_part = "delete from `";
          update_sql_delete_part.append(new_schema_tmp);
          update_sql_delete_part.append("`.`");
          update_sql_delete_part.append(table_name);
          update_sql_delete_part.append("` ");
          update_sql_delete_part.append(sql, st.scanner->where_pos - 1,
                                        strlen(sql));
          update_sql_delete_part.append(" limit 1");

          new_schema_tmp.clear();
          adjust_shard_schema(par_table->join->schema_name, schema,
                              new_schema_tmp,
                              dataspace_set_list->get_virtual_machine_id(),
                              dataspace_set_list->get_partition_id());
          int set_start_pos = st.sql->update_oper->update_set_start_pos;
          int set_end_pos = st.sql->update_oper->update_set_end_pos;
          update_sql_insert_part = "insert into `";
          update_sql_insert_part.append(new_schema_tmp);
          update_sql_insert_part.append("`.`");
          update_sql_insert_part.append(table_name);
          update_sql_insert_part.append("` ");
          update_sql_insert_part.append(sql, set_start_pos - 1,
                                        set_end_pos - (set_start_pos - 1));
          if (!auto_inc_key_name.empty() &&
              !set_col_names.count(auto_inc_key_name)) {
            update_sql_insert_part.append(", `");
            update_sql_insert_part.append(auto_inc_key_name);
            update_sql_insert_part.append("`=");
            int64_t auto_inc_val = get_auto_increment_value(
                par_space, plan, schema_name, table_name, NULL, 1, false);
            char tmp[21];
            sprintf(tmp, "%ld", auto_inc_val);
            update_sql_insert_part.append(tmp);
          }

          update_sql_insert_part_revert = "delete from `";
          update_sql_insert_part_revert.append(new_schema_tmp);
          update_sql_insert_part_revert.append("`.`");
          update_sql_insert_part_revert.append(table_name);
          update_sql_insert_part_revert.append("` where ");
          ListExpression *expr_list = (ListExpression *)set_list;
          expr_list_item *head = expr_list->expr_list_head;
          expr_list_item *tmp = head;
          do {
            Expression *expr = tmp->expr;  // like a=b
            update_sql_insert_part_revert.append(
                sql, expr->start_pos - 1, expr->end_pos - expr->start_pos + 1);
            tmp = tmp->next;
            if (tmp != head) {
              update_sql_insert_part_revert.append(" and ");
            }
          } while (tmp != head);
          update_sql_insert_part_revert.append(" limit 1");

          LOG_DEBUG(
              "for update_del_quick_limit, the UPDATE sql of "
              "primary/unique-key table is split into 1 delele and 1 insert "
              "sql:\n");
          LOG_DEBUG("delete sql [%s]\n", update_sql_delete_part.c_str());
          LOG_DEBUG("insert sql [%s]\n", update_sql_insert_part.c_str());
          LOG_DEBUG("revert sql [%s]\n", update_sql_insert_part_revert.c_str());

          map<DataSpace *, const char *> spaces_map;
          spaces_map[dataspace_where_clause] = update_sql_delete_part.c_str();
          spaces_map[dataspace_set_list] = update_sql_insert_part.c_str();
          ExecuteNode *node = NULL;
          node = plan->get_mul_modify_node(spaces_map);
          plan->set_start_node(node);
          is_update_via_delete_insert_revert = true;
          update_via_delete_insert_space_insert = dataspace_set_list;
          plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
          plan->session->set_execute_plan_touch_partition_nums(2);
          return;
        } else {
          int set_col_count = set_col_names.size();
          map<string, TableColumnInfo *, strcasecomp> *column_info_map;
          int table_column_count = 0;
          TableInfoCollection *tic = TableInfoCollection::instance();
          TableInfo *ti = tic->get_table_info_for_read(schema_name, table_name);
          try {
            column_info_map =
                ti->element_table_column
                    ->get_element_map_columnname_table_column_info(
                        stmt_session);
            table_column_count = column_info_map->size();
          } catch (...) {
            LOG_ERROR(
                "Error occured when try to get table info column "
                "info(map_table_column_info) of table [%s.%s]\n",
                schema_name, table_name);
            ti->release_table_info_lock();
            throw;
          }

          int fetch_row_count =
              0;  // TODO add config item to control total-update-rows
          vector<vector<string> > result_vec;
          vector<vector<bool> > result_null_vec;
          string fetch_sql;
          vector<string> fetch_col_names;
          DataSpace *dataspace_where_clause = NULL;
          string new_schema_tmp;
          try {
            dataspace_where_clause =
                par_space->get_partition(part_id_where_clause);
            adjust_shard_schema(
                par_table->join->schema_name, schema, new_schema_tmp,
                dataspace_where_clause->get_virtual_machine_id(),
                dataspace_where_clause->get_partition_id());
          } catch (...) {
            ti->release_table_info_lock();
            throw;
          }
          try {  // try-catch to make sure ti->release_table_info_lock()
            build_fetch_sql_for_update_sql(fetch_sql, set_col_count,
                                           table_column_count, column_info_map,
                                           set_col_names, fetch_col_names,
                                           new_schema_tmp, table_name, sql, st);
            fetch_rows_to_update(dataspace_where_clause, fetch_sql, result_vec,
                                 result_null_vec, fetch_row_count,
                                 set_col_count == table_column_count);
            if (fetch_row_count == 0) {
              // 0 row fetched, this means 0 rows will be updated, so return ok
              // packet directly
              ti->release_table_info_lock();
              ExecuteNode *node = plan->get_return_ok_node();
              plan->set_start_node(node);
              return;
            }

            // then build one delete sql and one(or more) insert sql
            update_sql_delete_part = "delete from `";
            update_sql_delete_part.append(new_schema_tmp);
            update_sql_delete_part.append("`.`");
            update_sql_delete_part.append(table_name);
            update_sql_delete_part.append("` ");
            update_sql_delete_part.append(sql, st.scanner->where_pos - 1,
                                          strlen(sql));

            new_schema_tmp.clear();
            adjust_shard_schema(par_table->join->schema_name, schema,
                                new_schema_tmp,
                                dataspace_set_list->get_virtual_machine_id(),
                                dataspace_set_list->get_partition_id());
            int set_start_pos = st.sql->update_oper->update_set_start_pos;
            int set_end_pos = st.sql->update_oper->update_set_end_pos;
            string insert_head = "insert into `";
            insert_head.append(new_schema_tmp);
            insert_head.append("`.`");
            insert_head.append(table_name);
            insert_head.append("` ");
            insert_head.append(sql, set_start_pos - 1,
                               set_end_pos - (set_start_pos - 1));
            string charset;
            if (get_client_charset_type() == CHARSET_TYPE_GBK)
              charset.assign("gbk");
            else if (get_client_charset_type() == CHARSET_TYPE_GB18030)
              charset.assign("gb18030");
            build_update_sql_insert_part(
                insert_head, update_sql_insert_part, fetch_col_names.size(),
                fetch_row_count, column_info_map, result_vec, result_null_vec,
                fetch_col_names, charset.c_str());
            ti->release_table_info_lock();
          } catch (...) {
            ti->release_table_info_lock();
            throw;
          }

          LOG_DEBUG(
              "after split, the UPDATE sql is split into 1 delete sql and %d "
              "insert sqls:\n",
              fetch_row_count);
          LOG_DEBUG("delete sql [%s]\n", update_sql_delete_part.c_str());
          LOG_DEBUG("insert sql [%s]\n", update_sql_insert_part.c_str());

          map<DataSpace *, const char *> spaces_map;
          map<DataSpace *, int> sql_count_map;
          spaces_map[dataspace_where_clause] = update_sql_delete_part.c_str();
          spaces_map[dataspace_set_list] = update_sql_insert_part.c_str();
          sql_count_map[dataspace_where_clause] = 1;
          sql_count_map[dataspace_set_list] = fetch_row_count;
          ExecuteNode *node = NULL;
          if (fetch_row_count > 1) {
            node = plan->get_mul_modify_node(spaces_map, true, &sql_count_map);
          } else {
            node = plan->get_mul_modify_node(spaces_map);
          }
          plan->set_start_node(node);
          is_update_via_delete_insert = true;
          plan->session->set_affected_servers(AFFETCED_MUL_SERVER);
          plan->session->set_execute_plan_touch_partition_nums(2);
        }
        return;
      }
    }
    case STMT_DELETE: {
      /*If the modify table is small table or modify table is (or related to,
       * such as "DELETE FROM t2 USING par_table,t2") a partition table with
       * the limit condition, we need to seperated this sql into one modify
       * sql + one select sql.*/
      if ((st.scanner->first_table->next &&
           st.scanner->first_table->next->join->cur_rec_scan == st.scanner &&
           !update_on_one_server && !delete_on_one_server &&
           !(st.type == STMT_UPDATE && table_contains_set_list(par_table))) ||
          (rs->limit && !stmt_with_limit_using_quick_limit)) {
        handle_one_par_table_mul_spaces(plan);
        break;
      }
    }
    case STMT_SELECT: {
      vector<unsigned int> par_ids;
      unsigned int partition_num = par_space->get_partition_num();
      PartitionMethod *method = par_space->get_partition_method();
      get_partitons_one_record_scan(rs, method, partition_num, &par_ids);
      par_space->replace_real_par_id_vec_from_virtual_ids(&par_ids);
      unsigned int par_ids_size = par_ids.size();
      DataSpace *dataspace = NULL;
      // in multiple_mode, when session delete part table auto_increment value,
      // record value. then this session can insert the value to the part table
      if (multiple_mode && type == STMT_DELETE &&
          instance_option_value["record_auto_increment_delete_value"]
              .uint_val) {
        string full_table_name;
        splice_full_table_name(schema_name, table_name, full_table_name);
        Backend *backend = Backend::instance();
        // check whether table has set auto_increment info
        if (backend->get_alter_table_flag(full_table_name,
                                          ALTER_TABLE_FLAG_AUTO_INCREMENT) ||
            !backend->has_set_auto_increment_info(full_table_name)) {
          init_auto_increment_params(plan, schema_name, table_name, par_space);
        }
        // check whether table is Auto_increment table
        if (backend->has_auto_increment_field(full_table_name)) {
          string auto_inc_key_name =
              par_space->get_auto_increment_key(full_table_name);
          string part_key = key_names->at(0);
          // check whether partition key is auto increment key
          if (part_key == auto_inc_key_name) {
            vector<const char *> *vec = get_key_values(rs, key_names->at(0));
            plan->session->record_auto_increment_delete_values(full_table_name,
                                                               vec);
          }
        }
      }
      if (par_ids_size == 1) {
        dataspace = par_space->get_partition(par_ids[0]);
        assemble_one_partition_plan(plan, dataspace);
        plan->session->set_execute_plan_touch_partition_nums(1);
        return;
      } else if (par_ids_size > 1 &&
                 par_ids_size <= par_space->get_real_partition_num()) {
        if (type == STMT_SELECT) {
          if (!is_acceptable_join_par_table(par_table->join)) {
            LOG_ERROR(
                "Unsupport join type for partition table with sql [%s].\n",
                sql);
            throw UnSupportPartitionSQL(
                "Unsupport join type for partition table");
          }
          bool need_check_distinct = true;
          if (check_and_replace_par_distinct_with_groupby(par_table)) {
            // sql has update by replacing DISTINCT with GROUP BY, so need
            // reparse.
            Statement *new_stmt = NULL;
            sql = no_distinct_sql.c_str();
            re_parser_stmt(&(st.scanner), &new_stmt, sql);
            need_check_distinct = false;
          }
          assemble_mul_par_result_set_plan(plan, &par_ids, par_space,
                                           need_check_distinct);
        } else {
          if (close_cross_node_transaction) {
            LOG_ERROR(
                "Refuse to execute cross node transaction when "
                "close_cross_node_transaction != 0\n");
            throw Error(
                "Refuse to execute cross node transaction when "
                "close_cross_node_transaction != 0");
          }
          assemble_mul_par_modify_plan(plan, &par_ids, par_space);
        }
        plan->session->set_execute_plan_touch_partition_nums(par_ids_size);
        return;
      }
      break;
    }
    case STMT_DBSCALE_ESTIMATE: {
      vector<unsigned int> *par_ids = new vector<unsigned int>();
      Statement *stmt = plan->statement;
      record_scan *rs = stmt->get_stmt_node()->cur_rec_scan;
      unsigned int partition_num = par_space->get_partition_num();
      PartitionMethod *method = par_space->get_partition_method();
      stmt->get_partitons_one_record_scan(rs, method, partition_num, par_ids);
      par_space->replace_real_par_id_vec_from_virtual_ids(par_ids);
      assemble_dbscale_estimate_select_partition_plan(plan, par_ids, par_space,
                                                      sql, this);
    } break;
    case STMT_PREPARE: {
      // TODO: implement it in the future.
      break;
    }
    case STMT_TRUNCATE: {
      if (st.table_list_num > 1) {
        LOG_ERROR(
            "Drop multiple tables, including partiton table, is not "
            "supported!");
        throw UnSupportPartitionSQL(
            "Drop multiple tables, including partiton table, is not supported");
      }
      st.enable_alter_table_sync_stmt = enable_alter_table_sync;
      table_link *table = st.table_list_head;
      schema_name =
          table->join->schema_name ? table->join->schema_name : schema;
      if (dbscale_safe_sql_mode > 0 &&
          Backend::instance()->is_system_schema(schema_name)) {
        throw Error(
            "Refuse truncate table of system schema for "
            "dbscale_safe_sql_mode>0");
      }
      if (multiple_mode && st.enable_alter_table_sync_stmt) {
        if (MultipleManager::instance()->get_is_cluster_master()) {
          plan->session->set_alter_table_name(full_table_name);
        } else {
          assemble_forward_master_role_plan(plan, true);
          break;
        }
      }
      if (enable_table_recycle_bin)
        assemble_move_table_to_recycle_bin_plan(plan);
      else
        assemble_modify_all_partition_plan(plan, par_space);
      break;
    }
    case STMT_CREATE_TB: {
      if (par_space->get_key_names()->size() == 1) {
        const char *par_key = par_space->get_key_names()->front();
        if (!is_partition_table_contain_partition_key(par_key)) {
          LOG_ERROR("Fail to find the partition key [%s] from table %s.%s\n",
                    par_key, schema_name, table_name);
          throw Error("Fail to find partition key from table.");
        }
      } else {
        LOG_ERROR("Not specify partition key for table %s.%s\n", schema_name,
                  table_name);
        throw Error("Not specify partition key for table.");
      }

      if (st.sql->create_tb_oper->has_foreign_key) {
        LOG_INFO(
            "Create table defination contains foreign key restriction, "
            "please make sure these keys are partitioned appropriately.\n");
      }
      if (st.sql->create_tb_oper->part_table_def_start_pos > 0) {
        no_part_table_def_sql.assign(
            sql, st.sql->create_tb_oper->part_table_def_start_pos - 1);
        no_part_table_def_sql.append(
            sql + st.sql->create_tb_oper->part_table_def_end_pos);
        sql = no_part_table_def_sql.c_str();
        Statement *new_stmt = NULL;
        re_parser_stmt(&(st.scanner), &new_stmt, sql);
      }
    }
    case STMT_ALTER_TABLE: {
      if ((is_partial_parsed() && type == STMT_ALTER_TABLE)) {
        st.modify_column = true;
      }
      if (type == STMT_CREATE_TB) {
        string full_table_name;
        splice_full_table_name(schema_name, table_name, full_table_name);
        if (Backend::instance()->has_auto_increment_field(full_table_name)) {
          LOG_DEBUG("DBScale contains auto_increment info for table [%s]\n",
                    full_table_name.c_str());
          st.modify_column = true;
        }
      }
      st.enable_alter_table_sync_stmt = enable_alter_table_sync;
      if (!multiple_mode)
        Backend::instance()->alter_table_flag_on(full_table_name);
      if (multiple_mode &&
          (st.enable_alter_table_sync_stmt || st.modify_column) &&
          !(type == STMT_CREATE_TB &&
            st.sql->create_tb_oper->part_table_def_start_pos > 0) &&
          !(type == STMT_CREATE_TB &&
            plan->session->get_session_option("auto_space_level").uint_val ==
                AUTO_SPACE_TABLE)) {
        if (MultipleManager::instance()->get_is_cluster_master()) {
          plan->session->set_alter_table_name(full_table_name);
        } else {
          LOG_DEBUG("CREATE TB or ALTER TB be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan, true);
          break;
        }
      }
      assemble_modify_all_partition_plan(plan, par_space);
      break;
    }
    case STMT_LOAD:
      par_space->set_auto_increment_info(plan->handler, this, schema_name,
                                         table_name, true, true);
      if (!support_load_set) {
        if (st.sql->load_oper->has_ignore ||
            st.sql->load_oper->has_set_columns) {
          LOG_ERROR("unsupport load data with ignore or set columns.\n");
          throw UnSupportPartitionSQL(
              "unsupport load data with ignore or set columns.");
        }
      }
      if (st.sql->load_oper->local) {
        assemble_partition_load_local_plan(plan, par_space, schema_name,
                                           table_name);
      } else {
        assemble_partition_load_data_infile_plan(plan, par_space, schema_name,
                                                 table_name);
      }
      break;

    case STMT_TABLE_MAINTENANCE: {
      vector<unsigned int> par_ids;
      unsigned int partition_num = par_space->get_real_partition_num();
      par_ids.clear();
      unsigned int i = 0;
      for (; i < partition_num; ++i) par_ids.push_back(i);

      ExecuteNode *send_node = plan->get_send_node();
      ExecuteNode *fetch_node = NULL;
      DataSpace *dataspace = NULL;

      unsigned int id;
      for (i = 0; i < par_ids.size(); ++i) {
        id = par_ids[i];
        dataspace = par_space->get_partition(id);
        const char *used_sql = adjust_stmt_sql_for_shard(dataspace, sql);
        fetch_node = plan->get_fetch_node(dataspace, used_sql);
        send_node->add_child(fetch_node);
      }
      plan->session->set_execute_plan_touch_partition_nums(-1);
      if (max_fetch_node_threads == 1) plan->set_fetch_node_no_thread(true);
      plan->set_start_node(send_node);
      break;
    }
    case STMT_SHOW_INDEX: {
      DataSpace *meta = Backend::instance()->get_metadata_data_space();
      if (!meta) {
        throw Error("fail to get metadata dataspace");
      }
      assemble_direct_exec_plan(plan, meta);
      break;
    }
    case STMT_SHOW_FIELDS:
    case STMT_SHOW_CREATE_TABLE: {
      DataSpace *dataspace = Backend::instance()->get_metadata_data_space();
      if (!dataspace) {
        dataspace = par_space->get_partition(0);
      }
      assemble_one_partition_plan(plan, dataspace);
      break;
    }
    case STMT_DBSCALE_SHOW_PARTITIONS: {
      assemble_dbscale_show_partition_plan(plan);
      break;
    }
    case STMT_REPLACE_SELECT:
    case STMT_INSERT_SELECT: {
      LOG_ERROR(
          "Unsupport INSERT SELECT no table, please use plain INSERT statement "
          "instead, the sql is [%s].\n",
          sql);
      throw UnSupportPartitionSQL(
          "Unsupport INSERT SELECT no table, please use plain INSERT statement "
          "instead");
    } break;
    default:
      LOG_ERROR("Unsupport one partition sql type, the sql is [%s].\n", sql);
      throw UnSupportPartitionSQL("Unsupport one partition sql type");
      break;
  }
}

SPExecuteFlag Statement::check_procedure_execution_flag(
    const char *sp_schema, const char *cur_schema, Statement *stmt,
    DataSpace **data_space) {
  stmt_node *sp_stmt = stmt->get_stmt_node();
  *data_space = NULL;
  Backend *backend = Backend::instance();
  DataSpace *schema_space =
      Backend::instance()->get_data_space_for_table(sp_schema, NULL);
  DataSpace *tmp_space = NULL;
  name_item *modify_schema_list = sp_stmt->modify_schema_list;
  name_item *tmp = modify_schema_list;
  const char *tmp_schema = NULL;

  DataSpace *standard_space = NULL;

  /*For modify schema, such as drop_db, it must be exec on the same server of
   * schema space of store procedure.*/
  if (tmp) {
    if (!tmp->name) {
#ifdef DEBUG
      ACE_ASSERT(0);
#endif
    }
    standard_space = backend->get_data_space_for_table(tmp->name, NULL);
    tmp = tmp->next;
    while (tmp != modify_schema_list) {
      tmp_schema = tmp->name;
      if (!tmp_schema) {
#ifdef DEBUG
        ACE_ASSERT(0);
#endif
        continue;
      }
      tmp_space = backend->get_data_space_for_table(tmp_schema, NULL);
      if (!is_share_same_server(standard_space, tmp_space))
        return EXECUTE_ON_DBSCALE;
      tmp = tmp->next;
    }
  }

  /*For used tables, if it will be modify, it also must be exec on the same
   * server of schema space of store procedure. */

  table_link *table = sp_stmt->table_list_head;
  const char *schema_name = NULL;
  const char *table_name = NULL;

  while (table) {
    const char *tmp_schema_name = sp_schema ? sp_schema : cur_schema;
    schema_name =
        table->join->schema_name ? table->join->schema_name : tmp_schema_name;
    table_name = table->join->table_name;

    tmp_space = backend->get_data_space_for_table(schema_name, table_name);
    if (!(tmp_space->get_data_source())) {
      return EXECUTE_ON_DBSCALE;
    }

    if (!standard_space) {
      standard_space = tmp_space;
      continue;
    }

    if (table->join->is_modify_table) {
      // for modify table, must be the same server
      if (!is_share_same_server(standard_space, tmp_space))
        return EXECUTE_ON_DBSCALE;
      standard_space = tmp_space;
    } else {
      // for non-modify table, dataspace cover is enough
      if (!stmt_session->is_dataspace_cover_session_level(standard_space,
                                                          tmp_space))
        return EXECUTE_ON_DBSCALE;
    }
    table = table->next;
  }

  if (standard_space) {
    if (!is_share_same_server(standard_space, schema_space)) {
      *data_space = standard_space;
      return EXECUTE_ON_ONE_DATASPACE;
    }
  }
  *data_space = schema_space;
  return EXECUTE_ON_SCHEMA_DATASPACE;
}

bool Statement::recursive_check_call_stmt_dataspace_info(
    Statement *stmt, const char *top_sp_schema, const char *schema_name,
    bool &has_partial_parsed, bool &has_non_single_node_stmt,
    SPExecuteFlag &sp_exe_flag, bool &stored_procedure_contain_cursor,
    DataSpace **data_space, string &sp_name) {
  bool ret = true;
#ifdef DEBUG
  LOG_DEBUG("in Statement::recursive_check_call_stmt_dataspace_info()\n");
#endif
  *data_space = NULL;
  sp_exe_flag = (SPExecuteFlag)check_procedure_execution_flag(
      top_sp_schema, schema_name, stmt, data_space);
  if (sp_exe_flag == EXECUTE_ON_DBSCALE) ret = false;

  list<Statement *> stmt_list_with_sub_call_stmt;
  list<string> stmt_sql_list;  // to avoid tmp const char* freed during re_parse
  set<string> sp_name_set;

  size_t pos = sp_name.find(DBSCALE_RESERVED_STR);
  if (pos != string::npos)
    sp_name_set.insert(string(sp_name, 0, pos));
  else
    sp_name_set.insert(sp_name);

  if (stmt->get_stmt_node()->r_call_stmt_list)
    stmt_list_with_sub_call_stmt.push_back(stmt);

  while (!stmt_list_with_sub_call_stmt.empty()) {
    Statement *curr_stmt = stmt_list_with_sub_call_stmt.front();
    recursive_call_stmt_item *call_stmt_list =
        curr_stmt->get_stmt_node()->r_call_stmt_list;
    stmt_list_with_sub_call_stmt.pop_front();

    while (call_stmt_list) {
      routine_simple_stmt *call_stmt = call_stmt_list->call_stmt;
      size_t start_pos = call_stmt->start_pos;
      size_t end_pos = call_stmt->end_pos;
      string sub_call_sql = string(curr_stmt->get_sql() + (start_pos - 1),
                                   end_pos - (start_pos - 1));
      LOG_DEBUG(
          "Statement::recursive_check_call_stmt_dataspace_info() check sub "
          "call stored procedure [%s]\n",
          sub_call_sql.c_str());
      record_scan *tmp_rs_of_sub_call = NULL;
      Statement *tmp_stmt_of_sub_call = NULL;
      re_parser_stmt(&tmp_rs_of_sub_call, &tmp_stmt_of_sub_call,
                     sub_call_sql.c_str());
      const char *sp_schema_name =
          tmp_stmt_of_sub_call->get_stmt_node()->sql->call_oper->db_name
              ? tmp_stmt_of_sub_call->get_stmt_node()->sql->call_oper->db_name
              : top_sp_schema;
      const char *procedure_name =
          tmp_stmt_of_sub_call->get_stmt_node()->sql->call_oper->procedure_name;
      DataSpace *dataspace =
          Backend::instance()->get_data_space_for_table(top_sp_schema, NULL);
      string sub_sp_name(sp_schema_name);
      sub_sp_name.append(".");
      sub_sp_name.append(procedure_name);

      string tmp_sp_name;
      size_t pos = sub_sp_name.find(DBSCALE_RESERVED_STR);
      if (pos != string::npos)
        tmp_sp_name = string(sub_sp_name, 0, pos);
      else
        tmp_sp_name = sub_sp_name;

      if (sp_name_set.count(tmp_sp_name)) {
        call_stmt_list = call_stmt_list->next;
        continue;
      }
      sp_name_set.insert(tmp_sp_name);

      string sp_name_with_enclose("`");
      sp_name_with_enclose.append(sp_schema_name);
      sp_name_with_enclose.append("`.`");
      sp_name_with_enclose.append(procedure_name);
      sp_name_with_enclose.append("`");

      ProcedureMetaData *meta_data =
          Backend::instance()->get_procedure_meta_data(sub_sp_name);
      const char *create_sp_sql = NULL;
      vector<string> sp_vec;
      if (meta_data) {
        if (meta_data->execution_flag == EXECUTE_ON_DBSCALE) {
          sp_exe_flag = EXECUTE_ON_DBSCALE;
          ret = false;
        }
        create_sp_sql = meta_data->procedure_string.c_str();
      } else {
        try {
          fetch_create_procedure_sql(sp_name_with_enclose.c_str(), dataspace,
                                     &sp_vec);
        } catch (...) {
          LOG_ERROR(
              "Statement::recursive_check_call_stmt_dataspace_info() fail to "
              "fetch create sql of stored procedure [%s]\n",
              sp_name_with_enclose.c_str());
          return false;
        }
        if (!sp_vec.empty()) {
          create_sp_sql = sp_vec[0].c_str();
          LOG_DEBUG(
              "Statement::recursive_check_call_stmt_dataspace_info() get "
              "create procedure sql [%s]\n",
              create_sp_sql);
        } else {
          LOG_ERROR(
              "Statement::recursive_check_call_stmt_dataspace_info() fail to "
              "fetch create sql of stored procedure [%s]\n",
              sp_name_with_enclose.c_str());
          return false;
        }
      }

      record_scan *tmp_rs_of_sub_create_sp_sql = NULL;
      Statement *tmp_stmt_of_sub_create_sp_sql = NULL;
      stmt_sql_list.push_back(create_sp_sql);
      create_sp_sql = stmt_sql_list.back().c_str();
      re_parser_stmt(&tmp_rs_of_sub_create_sp_sql,
                     &tmp_stmt_of_sub_create_sp_sql, create_sp_sql);
      if (tmp_stmt_of_sub_create_sp_sql->is_partial_parsed()) {
        has_partial_parsed = true;
        ret = false;
      }
      if (tmp_stmt_of_sub_create_sp_sql->get_stmt_node()
              ->stored_procedure_contain_cursor) {
        stored_procedure_contain_cursor = true;
      }
      if (tmp_stmt_of_sub_create_sp_sql->get_stmt_node()
              ->has_non_single_node_stmt) {
        has_non_single_node_stmt = true;
        ret = false;
      }

      DataSpace *data_space_tmp;
      DataSpace **p_data_space = &data_space_tmp;
      sp_exe_flag = check_procedure_execution_flag(
          sp_schema_name, schema_name, tmp_stmt_of_sub_create_sp_sql,
          p_data_space);
      if (sp_exe_flag == EXECUTE_ON_DBSCALE)
        ret = false;
      else if (!(*data_space))
        *data_space = data_space_tmp;
      else {
        if (!is_share_same_server(*data_space, data_space_tmp)) ret = false;
      }

      if (tmp_stmt_of_sub_create_sp_sql->get_stmt_node()->r_call_stmt_list)
        stmt_list_with_sub_call_stmt.push_back(tmp_stmt_of_sub_create_sp_sql);

      call_stmt_list = call_stmt_list->next;
    }
  }
  return ret;
}

void Statement::init_sp_worker_from_create_sql(
    SPWorker *spworker, ExecutePlan *plan, const char *create_sql,
    const char *routine_schema, const char *cur_schema, string &sp_name) {
  try {
    /*Here we need to reparse the create procedure sql to store the
     * parsing result stmt_node into the spworker, which will be kept
     * and used in the handling of call statement later.*/
    Parser *parser = Driver::get_driver()->get_parser();
    Statement *new_stmt = parser->parse(create_sql, stmt_allow_dot_in_ident,
                                        true, NULL, NULL, NULL, ctype);
    const char *tmp_schema =
        routine_schema ? routine_schema : plan->session->get_schema();
    new_stmt->set_default_schema(tmp_schema);
    new_stmt->set_session(stmt_session);
    spworker->set_statement(new_stmt);

    DataSpace *data_space = NULL;
    SPExecuteFlag sp_exe_flag = EXECUTE_ON_SCHEMA_DATASPACE;

    bool has_partial_parsed = new_stmt->is_partial_parsed();
    bool has_non_single_node_stmt =
        new_stmt->get_stmt_node()->has_non_single_node_stmt;
    bool stored_procedure_contain_cursor =
        new_stmt->get_stmt_node()->stored_procedure_contain_cursor;
    string tmp_create_sql = string(create_sql);
    boost::to_lower(tmp_create_sql);
    bool has_sequence_keyword =
        new_stmt->get_stmt_node()->seq_items_head ? true : false;
    bool ret = false;
    if (!has_partial_parsed && !has_non_single_node_stmt &&
        !has_sequence_keyword) {
      ret = recursive_check_call_stmt_dataspace_info(
          new_stmt, routine_schema, cur_schema, has_partial_parsed,
          has_non_single_node_stmt, sp_exe_flag,
          stored_procedure_contain_cursor, &data_space, sp_name);
    }
    if (data_space && data_space->get_data_source() &&
        data_space->get_data_source()->get_data_source_type() ==
            DATASOURCE_TYPE_ODBC)
      sp_exe_flag = EXECUTE_ON_DBSCALE;
    else if (has_sequence_keyword)
      sp_exe_flag = EXECUTE_ON_DBSCALE;
    else if (has_partial_parsed)
      sp_exe_flag = EXECUTE_ON_SCHEMA_DATASPACE;
    else if (!ret || has_non_single_node_stmt)
      sp_exe_flag = EXECUTE_ON_DBSCALE;

    if (sp_exe_flag == EXECUTE_ON_DBSCALE && stored_procedure_contain_cursor &&
        plan->session->check_for_transaction()) {
      if (plan->session->get_session_option("cursor_use_free_conn").int_val ==
          0) {
        LOG_ERROR(
            "can not call stored procedure with cursor when current session is "
            "in transaction.\n");
        throw NotSupportedError(
            "not support call stored procedure with cursor when current "
            "session is in transaction");
      }
    }

    spworker->set_execution_flag(sp_exe_flag);

    /*There is no need to do the spworker init for the situation that
     * the sp can be exec on one server.*/
    if (spworker->get_execution_flag() == EXECUTE_ON_DBSCALE) {
      LOG_DEBUG(
          "SP %s can not be exec on one server after analiyzing create sql.\n",
          spworker->get_name());
      routine_node *new_routine_d = new_stmt->get_stmt_node()->routine_d;
      spworker->init_sp_worker_by_routine_node(new_routine_d);
    } else {
      LOG_DEBUG(
          "SP %s can be exec on one server after analiyzing create sql.\n",
          spworker->get_name());
      spworker->set_data_space(data_space);
    }

  } catch (...) {
    if (spworker) {
      spworker->clean_sp_worker_for_free();
      delete spworker;
      LOG_ERROR("Get exception for the create and init of spworker.\n");
      throw;
    }
  }
}

void check_dbscale_management_acl(ExecutePlan *plan,
                                  bool is_dbscle_read_only_user = false) {
  if (is_dbscle_read_only_user) return;
  if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
      strcmp(normal_admin_user, plan->session->get_username()) != 0 &&
      strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
    LOG_ERROR(
        "Current User does not have the privilege to execute current command,"
        " only root/dbscale/dbscale_internal_user can.\n");
    throw Error(
        "Current User does not have the privilege to execute current command,"
        " only root/dbscale/dbscale_internal_user can.");
  }
}

void Statement::generate_execution_plan_with_no_table(ExecutePlan *plan) {
  DataSpace *dataspace = NULL;
  Backend *backend = Backend::instance();
  bool is_dbscale_read_only_user =
      !strcmp(dbscale_read_only_user, plan->session->get_username());

  switch (st.type) {
    case STMT_SHOW_TABLES: {
      assemble_show_tables_plan(plan);
    } break;
    case STMT_SHOW_DATABASES:
      assemble_show_databases_plan(plan);
      break;
    case STMT_SHOW_CREATE_EVENT: {
      assemble_show_create_event_plan(plan);
      break;
    }
    case STMT_SHOW_EVENTS: {
      assemble_show_events_plan(plan);
      break;
    }
    case STMT_SHOW_ENGINES:
    case STMT_SHOW_PLUGINS: {
      dataspace = backend->get_metadata_data_space();
      if (!dataspace) dataspace = backend->get_catalog();
      assemble_direct_exec_plan(plan, dataspace);

      break;
    }
    case STMT_PLUGIN: {
      throw NotSupportedError("Not support plugin related stmt.");
      break;
    }
    case STMT_LOCK_TB: {
      assemble_lock_tables_plan(plan);
      break;
    }
    case STMT_CREATE_TRIGGER: {
      assemble_create_trigger_plan(plan);
      break;
    }
    case STMT_DROP_TRIGGER: {
      assemble_create_trigger_plan(plan);
      break;
    }
    case STMT_DBSCALE_SHUTDOWN: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(normal_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to SHUTDOWN.\n");
        throw Error("Current User does not have the privilege to SHUTDOWN");
      }

#ifndef CLOSE_MULTIPLE
      int cluster_id = st.sql->dbscale_shutdown_oper->cluster_id;
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      } else {
        LOG_ERROR(
            "'DBSCALE SHUTDOWN cluster_id' is only supported under Multiple "
            "DBScale mode.\n");
        throw NotSupportedError(
            "'DBSCALE SHUTDOWN cluster_id' is only supported under Multiple "
            "DBScale mode.");
      }

      if (cluster_id == ERROR_CLUSTER_ID) {
        LOG_ERROR("Cluster_id cannot be 0.\n");
        throw Error("Cluster_id cannot be 0");
      }
      // just send SHUTDOWN command when pointed cluster_id is current dbscale's
      // cluster_id
      if (cluster_id == Backend::instance()->get_cluster_id())
        return assemble_dbscale_shutdown_plan(plan);

      // else, always use master dbscale to do SHUTDOWN
      if (is_master) {
        assemble_dbscale_shutdown_plan(plan, cluster_id);
      } else {
        LOG_DEBUG("dynamic add dataspace cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#else
      LOG_ERROR(
          "'DBSCALE SHUTDOWN cluster_id' is only supported under Multiple "
          "DBScale mode.\n");
      throw NotSupportedError(
          "'DBSCALE SHUTDOWN cluster_id' is only supported under Multiple "
          "DBScale mode.");
#endif

    } break;
    case STMT_DBSCALE_CLEAN_TEMPORARY_TABLE_CACHE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master || st.sql->clean_temp_table_cache_oper->is_internal) {
#endif
        assemble_dbscale_clean_temp_table_cache(plan);
#ifndef CLOSE_MULTIPLE
      } else {
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_XA_START_TRANSACTION:
    case STMT_XA_END_TRANSACTION:
    case STMT_XA_PREPARE_TRANSACTION:
    case STMT_XA_COMMIT_TRANSACTION:
    case STMT_XA_ROLLBACK_TRANSACTION:
    case STMT_XA_COMMIT_ONE_PHASE_TRANSACTION:
    case STMT_XA_RECOVER:
      if (!enable_cluster_xa_transaction) {
        LOG_ERROR(
            "enable_cluster_xa_transaction is off, cann't execute xa stmt.\n");
        throw Error(
            "enable_cluster_xa_transaction is off, cann't execute xa stmt.");
      }
      if (!backend->is_centralized_cluster()) {
        LOG_ERROR("is not centralized group, cann't execute xa stmt.\n");
        throw Error("is not centralized group, cann't execute xa stmt.");
      }
      assemble_cluster_xatransaction_plan(plan);
      break;
    case STMT_DBSCALE_REQUEST_CLUSTER_INC_INFO:
      assemble_dbscale_request_cluster_inc_info_plan(plan);
      break;
    case STMT_DBSCALE_REQUEST_ALL_CLUSTER_INC_INFO:
      assemble_dbscale_request_all_cluster_inc_info_plan(plan);
      break;
    case STMT_DBSCALE_REQUEST_CLUSTER_ID:
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      assemble_dbscale_request_cluster_id_plan(plan);
      break;
    case STMT_DBSCALE_REQUEST_NODE_INFO:
      assemble_dbscale_request_node_info_plan(plan);
      break;
    case STMT_DBSCALE_REQUEST_CLUSTER_INFO:
      assemble_dbscale_request_cluster_info_plan(plan);
      break;
    case STMT_DBSCALE_SHOW_ASYNC_TASK: {
      if (strcmp(dbscale_read_only_user, plan->session->get_username())) {
        if (strcmp(supreme_admin_user, plan->session->get_username()) &&
            strcmp(normal_admin_user, plan->session->get_username()) &&
            strcmp(dbscale_internal_user, plan->session->get_username())) {
          LOG_ERROR("Refuse user [%s] to execute dbscale show async task.\n",
                    plan->session->get_username());
          throw Error(
              "Only 'root' or 'dbscale' allowed to do async task management.");
        }
      }
      assemble_dbscale_show_async_task_plan(plan);
      break;
    }
    case STMT_DBSCALE_SUSPEND_ASYNC_TASK:
    case STMT_DBSCALE_CONTINUE_ASYNC_TASK:
    case STMT_DBSCALE_CANCEL_ASYNC_TASK:
    case STMT_DBSCALE_DELETE_ASYNC_TASK: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) &&
          strcmp(normal_admin_user, plan->session->get_username()) &&
          strcmp(dbscale_internal_user, plan->session->get_username())) {
        LOG_ERROR("Refuse user [%s] to manage dbscale async task.\n",
                  plan->session->get_username());
        throw Error(
            "Only 'root' or 'dbscale' allowed to do async task management.");
      }

      unsigned long long id = st.sql->dbscale_async_task_oper->id;
      assemble_async_task_control_plan(plan, id);
      break;
    }
    case STMT_DBSCALE_LOAD_DATASPACE_CONFIG_FILE: {
      check_dbscale_management_acl(plan);
      const char *filename = st.sql->load_config_oper->filename;
      assemble_load_dataspace_config_file_plan(plan, filename);
      break;
    }
    case STMT_DBSCALE_REQUEST_USER_STATUS: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      const char *id = st.sql->show_user_status_oper->user_id;
      bool only_show_running = st.sql->show_user_status_oper->only_show_running;
      bool show_staus_count = st.sql->show_user_status_oper->show_status_count;
      assemble_dbscale_request_cluster_user_status_plan(
          plan, id, only_show_running, show_staus_count);
      break;
    }
    case STMT_SHOW_TABLE_STATUS:
      assemble_show_table_status_plan(plan);
      break;
    case STMT_ROLLBACK: {
      if (!enable_xa_transaction ||
          plan->session->get_session_option("close_session_xa").int_val != 0) {
        RollbackType rollback_type = st.sql->rollback_point_oper->type;
        /* when session is not in transaction, we just return an error packet */
        if (rollback_type == ROLLBACK_TYPE_TO_POINT &&
            !plan->session->is_in_transaction()) {
          dataspace = backend->get_data_space_for_table(schema, NULL);
          assemble_direct_exec_plan(plan, dataspace);
        } else {
          assemble_transaction_unlock_plan(plan);
        }
      } else {
        if (plan->session->is_in_transaction_consistent()) {
          assemble_transaction_unlock_plan(plan);
        } else {
          assemble_xatransaction_plan(plan);
        }
      }
    } break;
    case STMT_COMMIT:
    case STMT_START_TRANSACTION: {
      if (!st.sql->start_tran_oper ||
          !st.sql->start_tran_oper->with_consistence) {
        if (enable_xa_transaction &&
            plan->session->get_session_option("close_session_xa").int_val ==
                0 &&
            !plan->session->is_in_transaction_consistent()) {
          assemble_xatransaction_plan(plan);
        } else {
          assemble_transaction_unlock_plan(plan);
        }
      } else {
        map<DataSpace *, const char *> spaces_map;
        Backend *backend = Backend::instance();
        plan->session->set_in_transaction_consistent(true);

        list<DataSpace *> spaces =
            backend->get_config_machine_space_for_server_level_execute();
        list<DataSpace *>::iterator it = spaces.begin();
        for (; it != spaces.end(); ++it) {
          spaces_map[*it] = sql;
        }
        ExecuteNode *node = plan->get_mul_modify_node(spaces_map, false);
        plan->set_start_node(node);
      }
      break;
    }
    case STMT_UNLOCK_TB:
    case STMT_SAVEPOINT:
      assemble_transaction_unlock_plan(plan);
      break;
    case STMT_SHUTDOWN: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to SHUTDOWN.\n");
        throw Error("Current User does not have the privilege to SHUTDOWN");
      }

      assemble_dbscale_shutdown_plan(plan);
    } break;
    case STMT_FLUSH_PRIVILEGES: {
      dataspace = backend->get_auth_data_space();
      assemble_direct_exec_plan(plan, dataspace);
    } break;
    case STMT_FLUSH_TABLE_WITH_READ_LOCK: {
      map<DataSpace *, const char *> spaces_map;
      Backend *backend = Backend::instance();

      list<DataSpace *> spaces =
          backend->get_config_machine_space_for_server_level_execute();
      list<DataSpace *>::iterator it = spaces.begin();
      for (; it != spaces.end(); ++it) {
        spaces_map[*it] = sql;
      }
      ExecuteNode *node = plan->get_mul_modify_node(spaces_map, false);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_SHOW_INNODB_LOCK_WAITING: {
      assemble_show_engine_lock_waiting_status_plan(plan, INNODB);
    } break;
    case STMT_DBSCALE_SHOW_TOKUDB_LOCK_WAITING: {
      assemble_show_engine_lock_waiting_status_plan(plan, TOKUDB);
    } break;
    case STMT_DBSCALE_SHOW_DEFAULT_SESSION_VARIABLES: {
      assemble_dbscale_show_default_session_variables(plan);
    } break;
    case STMT_DBSCALE_SHOW_VERSION: {
      assemble_dbscale_show_version_plan(plan);
    } break;
    case STMT_SHOW_VERSION: {
      if (Backend::instance()->is_grid())
        assemble_show_version_plan(plan);
      else
        assemble_dbscale_show_version_plan(plan);
    } break;
    case STMT_SHOW_COMPONENTS_VERSION: {
      assemble_show_components_version_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_HOSTNAME: {
      assemble_dbscale_show_hostname_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_ALL_FAIL_TRANSACTION: {
      assemble_dbscale_show_all_fail_transaction_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_DETAIL_FAIL_TRANSACTION: {
      const char *xid = st.sql->show_detail_fail_transaction_oper->xid;
      assemble_dbscale_show_detail_fail_transaction_plan(plan, xid);
    } break;
    case STMT_DBSCALE_SHOW_PARTITION_TABLE_STATUS: {
      const char *table_name =
          st.sql->show_partition_table_status_oper->table_name;
      assemble_dbscale_show_partition_table_status_plan(plan, table_name);
    } break;
    case STMT_DBSCALE_SHOW_SCHEMA_ACL_INFO: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, no schema ACL info to show\n");
        throw Error("enable-acl=0, no schema ACL info to show");
      }
      bool is_show_all = st.sql->show_schema_acl_info_oper->show_all;
      if (is_show_all &&
          strcmp(supreme_admin_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege to show ALL schema "
            "ACL.\n");
        throw Error(
            "Current User does not have the privilege to show ALL schema ACL");
      }
      assemble_dbscale_show_schema_acl_info(plan, is_show_all);
    } break;
    case STMT_DBSCALE_SHOW_TABLE_ACL_INFO: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, no table ACL info to show\n");
        throw Error("enable-acl=0, no table ACL info to show");
      }
      bool is_show_all = st.sql->show_table_acl_info_oper->show_all;
      if (is_show_all &&
          strcmp(supreme_admin_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege to show ALL table "
            "ACL.\n");
        throw Error(
            "Current User does not have the privilege to show ALL table ACL");
      }
      assemble_dbscale_show_table_acl_info(plan, is_show_all);
    } break;
    case STMT_DBSCALE_SHOW_PATH_INFO: {
      assemble_dbscale_show_path_info(plan);
    } break;
    case STMT_DBSCALE_SHOW_WARNINGS: {
      assemble_dbscale_show_warnings(plan);
    } break;
    case STMT_DBSCALE_SHOW_CRITICAL_ERRORS: {
      assemble_dbscale_show_critical_errors(plan);
    } break;
    case STMT_DBSCALE_CLEAN_FAIL_TRANSACTION: {
      const char *xid = st.sql->clean_fail_transaction_oper->xid;
      assemble_dbscale_clean_fail_transaction_plan(plan, xid);
    } break;
    case STMT_DBSCALE_SHOW_SHARD_PARTITION_TABLE: {
      const char *scheme_name =
          st.sql->show_shard_partition_tb_oper->scheme_name;
      assemble_dbscale_show_shard_partition_table_plan(plan, scheme_name);
    } break;
    case STMT_DBSCALE_SHOW_REBALANCE_WORK_LOAD: {
      const char *scheme_name =
          st.sql->show_rebalance_work_load_oper->scheme_name;
      const char *schema_name =
          st.sql->show_rebalance_work_load_oper->schema_name;
      name_item *source_head =
          st.sql->show_rebalance_work_load_oper->source_list;
      int is_remove = st.sql->show_rebalance_work_load_oper->is_remove;
      name_item *tmp = source_head;
      list<string> sources;
      while (tmp) {
        sources.push_back(string(tmp->name));
        tmp = tmp->next;
        if (tmp == source_head) break;
      }
      assemble_dbscale_show_rebalance_work_load_plan(plan, scheme_name, sources,
                                                     schema_name, is_remove);
    } break;

    case STMT_DBSCALE_ADD_DEFAULT_SESSION_VARIABLES: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        bool is_master = mul->get_is_cluster_master();
        if (!is_master) {
          LOG_DEBUG("Add default session be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      assemble_dbscale_add_default_session_variables(plan);
    } break;
    case STMT_DBSCALE_REMOVE_DEFAULT_SESSION_VARIABLES: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        bool is_master = mul->get_is_cluster_master();
        if (!is_master) {
          LOG_DEBUG("Remove default session be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        }
      }
#endif
      assemble_dbscale_remove_default_session_variables(plan);
    } break;
    case STMT_DBSCALE_SHOW_USER_SQL_COUNT: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      const char *user_id = st.sql->show_user_sql_count_oper->user_id;
      assemble_dbscale_show_user_sql_count_plan(plan, user_id);
    } break;
    case STMT_DBSCALE_SHOW_DATASOURCE: {
      assemble_show_datasource_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_MIGRATE_CLEAN_TABLES: {
      check_dbscale_management_acl(plan);
      assemble_show_migrate_clean_tables_plan(plan);
    } break;
    case STMT_DBSCALE_MIGRATE_CLEAN: {
      check_dbscale_management_acl(plan);
      assemble_migrate_clean_tables_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_DATASERVER: {
      assemble_show_dataserver_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_BACKEND_THREADS: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      assemble_show_backend_threads_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_PARTITION_SCHEME: {
      assemble_show_partition_scheme_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_STATUS: {
      assemble_dbscale_show_status_plan(plan);
      break;
    }
    case STMT_DBSCALE_SHOW_CATCHUP: {
      check_dbscale_management_acl(plan);
      const char *source_name = st.sql->show_catchup_oper->source_name;
      assemble_show_catchup_plan(plan, source_name);
      break;
    }
    case STMT_DBSCALE_SKIP_WAIT_CATCHUP: {
      check_dbscale_management_acl(plan);
      const char *source_name = st.sql->skip_wait_catchup_oper->source_name;
      assemble_dbscale_skip_wait_catchup_plan(plan, source_name);
    } break;
    case STMT_DBSCALE_SHOW_VIRTUAL_MAP:
    case STMT_DBSCALE_SHOW_SHARD_MAP: {
      assemble_dbscale_show_virtual_map_plan(plan);
      break;
    }
    case STMT_DBSCALE_SHOW_AUTO_INCREMENT_VALUE: {
      assemble_dbscale_show_auto_increment_info(plan);
      break;
    }
    case STMT_DBSCALE_MUL_SYNC: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_mul_sync_plan(plan);
      break;
    }
    case STMT_DBSCALE_GET_GLOBAL_CONSISTENCE_POINT: {
      check_dbscale_management_acl(plan);
      assemble_dbscale_get_global_consistence_point_plan(plan);
    } break;
    case STMT_DBSCALE_MIGRATE: {
      check_dbscale_management_acl(plan);
      assemble_migrate_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_USER_MEMORY_STATUS: {
      assemble_show_user_memory_status_plan(plan);
    } break;
    case STMT_DBSCALE_SHOW_USER_STATUS: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      const char *user_id = st.sql->show_user_status_oper->user_id;
      const char *user_name = st.sql->show_user_status_oper->user_name;
      bool only_show_running = st.sql->show_user_status_oper->only_show_running;
      bool instance = st.sql->show_user_status_oper->instance;
      bool show_status_count = st.sql->show_user_status_oper->show_status_count;
      assemble_show_user_status_plan(plan, user_id, user_name,
                                     only_show_running, instance,
                                     show_status_count);
    } break;
    case STMT_DBSCALE_SHOW_USER_PROCESSLIST: {
      check_dbscale_management_acl(plan, is_dbscale_read_only_user);
      const char *cluster_id = st.sql->show_user_processlist_oper->cluster_id;
      const char *user_id = st.sql->show_user_processlist_oper->user_id;
      int local = st.sql->show_user_processlist_oper->local;
      assemble_show_user_processlist_plan(plan, cluster_id, user_id, local);
    } break;
    case STMT_DBSCALE_SHOW_SESSION_ID:
    case STMT_DBSCALE_REQUEST_SESSION_ID: {
      check_dbscale_management_acl(plan);
      const char *server_name = st.sql->show_session_id_oper->server_name;
      int connection_id = st.sql->show_session_id_oper->connection_id;
      assemble_show_session_id_plan(plan, server_name, connection_id);
    } break;
    case STMT_DBSCALE_SHOW_SCHEMA: {
      const char *schema_name = st.sql->dbscale_show_schema_oper->schema;
      assemble_show_schema_plan(plan, schema_name);
    } break;
    case STMT_DBSCALE_SHOW_TABLE: {
      const char *schema_name = st.sql->dbscale_show_table_oper->schema_name;
      const char *table_name = st.sql->dbscale_show_table_oper->table_name;
      bool use_like = st.sql->dbscale_show_table_oper->use_like;
      assemble_show_table_plan(plan, schema_name, table_name, use_like);
    } break;
    case STMT_DBSCALE_BACKEND_SERVER_EXECUTE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG("backend_server_execute cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      const char *stmt_sql = st.sql->backend_server_execute_oper->stmt_sql;
      assemble_backend_server_execute_plan(plan, stmt_sql);
    } break;
    case STMT_EXECUTE_ON_ALL_MASTERSERVER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG(
            "execute_on_all_masterserver_execute cmd be execute on slave "
            "dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      const char *stmt_sql = st.sql->execute_on_all_masterserver_oper->stmt_sql;
      assemble_execute_on_all_masterserver_execute_plan(plan, stmt_sql);
      break;
    }
    case STMT_DBSCALE_EXECUTE_ON_DATASERVER: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(normal_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Only root and dbscale is allowed to run "
            "STMT_DBSCALE_EXECUTE_ON_DATASERVER.\n");
        throw Error(
            "Only root and dbscale is allowed to run "
            "STMT_DBSCALE_EXECUTE_ON_DATASERVER");
      }
      const char *dataserver_name =
          st.sql->dbscale_execute_on_dataserver_oper->dataserver_name;
      const char *stmt_sql =
          st.sql->dbscale_execute_on_dataserver_oper->stmt_sql;
      assemble_dbscale_execute_on_dataserver_plan(plan, dataserver_name,
                                                  stmt_sql);
    } break;
    case STMT_DBSCALE_SHOW_TABLE_LOCATION: {
      const char *schema_name = st.table_list_head->join->schema_name
                                    ? st.table_list_head->join->schema_name
                                    : schema;
      const char *table_name = st.table_list_head->join->table_name;
      assemble_show_table_location_plan(plan, schema_name, table_name);
    } break;
    case STMT_DBSCALE_ERASE_AUTH_INFO: {
      check_dbscale_management_acl(plan);
      name_item *username_list =
          st.sql->dbscale_erase_auth_info_oper->username_list;
      bool all_dbscale_node =
          st.sql->dbscale_erase_auth_info_oper->all_dbscale_node;
      LOG_DEBUG("Assemble dbscale erase auth info plan%s.\n",
                all_dbscale_node ? " for all dbscale node" : "");
      ExecuteNode *node = plan->get_dbscale_erase_auth_info_node(
          username_list, all_dbscale_node);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_UPDATE_AUDIT_USER: {
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege of dynamic "
            "configuration.\n");
        throw Error(
            "Current User does not have the privilege of dynamic "
            "configuration.");
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG("update audit user cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      const char *username = st.sql->dbscale_update_audit_user_oper->username;
      bool is_add = st.sql->dbscale_update_audit_user_oper->is_add;
      LOG_DEBUG("assemble_dbscale_update_audit_user plan.\n");
      ExecuteNode *node =
          plan->get_dbscale_update_audit_user_node(username, is_add);
      plan->set_start_node(node);
    } break;
    case STMT_DBSCALE_DYNAMIC_UPDATE_WHITE: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, can not set ACL\n");
        throw Error("enable-acl=0, can not set ACL");
      }
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR(
            "Current User does not have the privilege of dynamic "
            "configuration.\n");
        throw Error(
            "Current User does not have the privilege of dynamic "
            "configuration.");
      }
      const char *comment = st.sql->dynamic_update_white_oper->comment;
      if (comment && strlen(comment) > 512) {
        throw Error("comment length sould no larger than 512 bytes");
      }
      ssl_option_struct ssl_option_value;
      ssl_option_value.ssl_option =
          st.sql->dynamic_update_white_oper->ssl_option;
      ssl_option_value.ssl_cipher =
          st.sql->dynamic_update_white_oper->ssl_cipher
              ? st.sql->dynamic_update_white_oper->ssl_cipher
              : "";
      ssl_option_value.x509_issuer =
          st.sql->dynamic_update_white_oper->x509_issuer
              ? st.sql->dynamic_update_white_oper->x509_issuer
              : "";
      ssl_option_value.x509_subject =
          st.sql->dynamic_update_white_oper->x509_subject
              ? st.sql->dynamic_update_white_oper->x509_subject
              : "";
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG(
            "dynamic update white list cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      bool is_add = st.sql->dynamic_update_white_oper->is_add;
      const char *user_name = st.sql->dynamic_update_white_oper->user_name;
      const char *ip = st.sql->dynamic_update_white_oper->ip;
      assemble_dbscale_dynamic_update_white_plan(
          plan, is_add, ip, ssl_option_value, comment, user_name);
    } break;
    case STMT_DBSCALE_DYNAMIC_UPDATE_FUCTION_TYPE_MAP: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master || st.sql->dynamic_update_function_type_oper->is_internal) {
#endif
        assemble_dbscale_reload_func_type_plan(plan);
#ifndef CLOSE_MULTIPLE
      } else {
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_ADD_DATASERVER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_add_data_server_plan(
            plan, st.sql->dynamic_add_data_server_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add server cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_ADD_SLAVE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_add_slave_plan(plan, st.sql->dynamic_add_slave_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add slave cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_ADD_PRE_DISASTER_MASTER: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_add_pre_disaster_master_plan(
            plan, st.sql->dynamic_add_pre_disaster_master_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add slave cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_ADD_DATASOURCE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        if (st.sql->dynamic_add_data_source_oper->type ==
                DATASOURCE_TYPE_REPLICATION &&
            !st.sql->dynamic_add_data_source_oper->semi_sync &&
            auto_master_failover_flashback) {
          throw Error(
              "For replication datasource, 'SEMI_SYNC' should be used when "
              "'auto-master-failover-flashback' is enabled");
        }
        assemble_dynamic_add_data_source_plan(
            plan, st.sql->dynamic_add_data_source_oper,
            st.sql->dynamic_add_data_source_oper->type);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add datasource cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_DYNAMIC_ADD_DATASPACE: {
      check_dbscale_management_acl(plan);
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (is_master) {
#endif
        assemble_dynamic_add_data_space_plan(
            plan, st.sql->dynamic_add_data_space_oper);
#ifndef CLOSE_MULTIPLE
      } else {
        LOG_DEBUG("dynamic add dataspace cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
    } break;
    case STMT_DBSCALE_SET_SCHEMA_ACL: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, can not set ACL\n");
        throw Error("enable-acl=0, can not set ACL");
      }
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to set ACL.\n");
        throw Error("Current User does not have the privilege to set ACL.");
      }
      const char *username = st.sql->set_schema_acl_oper->username;
      if (!strcmp(username, supreme_admin_user)) {
        LOG_ERROR("can not set schema ACL on target user\n");
        throw Error("can not set schema ACL on target user");
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG("dynamic set schema acl cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      assemble_set_schema_acl_plan(plan, st.sql->set_schema_acl_oper);
    } break;
    case STMT_DBSCALE_RELOAD_USER_ALLOW_OPERATION_TIME: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, can not set ACL\n");
        throw Error("enable-acl=0, can not set ACL");
      }
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to set ACL.\n");
        throw Error("Current User does not have the privilege to set ACL.");
      }
      string user_name =
          st.sql->set_user_not_allow_operation_time_oper->user_name;
      assemble_reload_user_not_allow_operation_time_plan(plan, user_name);
    } break;
    case STMT_DBSCALE_SHOW_USER_NOT_ALLOW_OPERATION_TIME: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, no ACL info to show\n");
        throw Error("enable-acl=0, no ACL info to show");
      }
      const char *user_name_str =
          st.sql->set_user_not_allow_operation_time_oper->user_name;
      string user_name = user_name_str ? string(user_name_str) : string("");
      if (!is_dbscale_read_only_user &&
          strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        if (!user_name.empty() &&
            strcmp(user_name.c_str(), plan->session->get_username()) == 0) {
        } else {
          LOG_ERROR("Current User does not have the privilege to show ACL.\n");
          throw Error("Current User does not have the privilege to show ACL.");
        }
      }
      assemble_show_user_not_allow_operation_time_plan(plan, user_name);
    } break;
    case STMT_DBSCALE_SET_USER_ALLOW_OPERATION_TIME: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, can not set ACL\n");
        throw Error("enable-acl=0, can not set ACL");
      }
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to set ACL.\n");
        throw Error("Current User does not have the privilege to set ACL.");
      }
      const char *username =
          st.sql->set_user_not_allow_operation_time_oper->user_name;
      if (!strcmp(username, supreme_admin_user) ||
          !strcmp(username, dbscale_internal_user)) {
        LOG_ERROR("can not set table ACL on user root or dbscale_internal\n");
        throw Error("can not set table ACL on user root or dbscale_internal");
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG("dynamic set table acl cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      assemble_set_user_not_allow_operation_time_plan(
          plan, st.sql->set_user_not_allow_operation_time_oper);
    } break;
    case STMT_DBSCALE_SET_TABLE_ACL: {
      if (!enable_acl) {
        LOG_ERROR("enable-acl=0, can not set ACL\n");
        throw Error("enable-acl=0, can not set ACL");
      }
      if (strcmp(supreme_admin_user, plan->session->get_username()) != 0 &&
          strcmp(dbscale_internal_user, plan->session->get_username()) != 0) {
        LOG_ERROR("Current User does not have the privilege to set ACL.\n");
        throw Error("Current User does not have the privilege to set ACL.");
      }
      const char *username = st.sql->set_table_acl_oper->user_name;
      if (!strcmp(username, supreme_admin_user) ||
          !strcmp(username, dbscale_internal_user)) {
        LOG_ERROR("can not set table ACL on target user\n");
        throw Error("can not set table ACL on target user");
      }
#ifndef CLOSE_MULTIPLE
      bool is_master = true;
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
      if (!is_master) {
        LOG_DEBUG("dynamic set table acl cmd be execute on slave dbscale\n");
        assemble_forward_master_role_plan(plan);
        break;
      }
#endif
      assemble_set_table_acl_plan(plan, st.sql->set_table_acl_oper);
    } break;
    case STMT_DBSCALE_SET_SCHEMA_PUSHDOWN_PROCEDURE: {
      check_dbscale_management_acl(plan);
      int pushdown_value = st.sql->set_schema_pushdown_proc_oper->pushdown_proc;
      const char *schema_name = st.sql->set_schema_pushdown_proc_oper->schema;
      DataSpace *s = backend->get_data_space_for_table(schema_name, NULL);
      if (s == backend->get_catalog()) {
        LOG_ERROR(
            "The schema [%s] is not configed, and refuse to adjust the "
            "pushdown_procedure behavior.\n",
            schema_name);
        throw Error(
            "The schema is not configed, and refuse to adjust the "
            "pushdown_procedure behavior.");
      }
      Schema *schema = (Schema *)s;

      bool is_master = true;
#ifndef CLOSE_MULTIPLE
      if (multiple_mode) {
        MultipleManager *mul = MultipleManager::instance();
        is_master = mul->get_is_cluster_master();
      }
#endif
      if (!st.sql->set_schema_pushdown_proc_oper->is_real) {
#ifndef CLOSE_MULTIPLE
        if (!is_master) {
          LOG_DEBUG("dynamic set table acl cmd be execute on slave dbscale\n");
          assemble_forward_master_role_plan(plan);
          break;
        } else {
          if (multiple_mode)
            MultipleManager::instance()->adjust_schema_pushdown(
                string(schema_name), pushdown_value);
        }
#endif
      }
      int old_val = schema->get_schema_pushdown_sp_config_value();
      schema->set_schema_pushdown_stored_procedure(pushdown_value);
      if (is_master) {
        list<string> update_list;
        update_list.push_back(
            Driver::get_driver()->get_config_helper()->generate_schema_config(
                schema));
        bool ret = Driver::get_driver()->get_config_helper()->update_config(
            update_list);
  