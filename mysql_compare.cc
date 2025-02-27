/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <mysql_compare.h>
#include <mysql_driver.h>
#include <mysql_request.h>
#include <option.h>

#include <boost/algorithm/string.hpp>

#ifdef HAVE_PWD_H
#undef HAVE_PWD_H
#endif
#ifdef HAVE_SYS_TYPES_H
#undef HAVE_SYS_TYPES_H
#endif
#ifdef HAVE_UNISTD_H
#undef HAVE_UNISTD_H
#endif
#ifdef HAVE_LONG_LONG
#undef HAVE_LONG_LONG
#endif

#include "m_ctype.h"

using namespace dbscale;
using namespace dbscale::mysql;

namespace dbscale {
namespace mysql {

int MySQLColumnCompare::compare(MySQLRowResponse *row1, MySQLRowResponse *row2,
                                int column_index, MySQLColumnType column_type,
                                CharsetType ctype, bool is_cs) {
#ifdef DEBUG
  ACE_ASSERT(field_is_valid_for_compare(column_type));
#endif
  int ret;
  bool row1_null, row2_null;

  if (field_cmp_funcs[column_type] == NULL) {
    LOG_ERROR("Unsupport column type %d for compare.\n", column_type);

    throw HandlerError(
        "Unsupport column type for compare,"
        "please check the log for detail.");
  }

  row1->set_current(column_index);
  row2->set_current(column_index);

  row1_null = row1->field_is_null();
  row2_null = row2->field_is_null();

  string row1_str(row1->get_current_data(), row1->get_current_length());
  string row2_str(row2->get_current_data(), row2->get_current_length());

  if (row1_null || row2_null) {
    row1->move_next();
    row2->move_next();
    return (row1_null == row2_null ? 0 : row1_null ? -1 : 1);
  }

  ret = field_cmp_funcs[column_type](
      row1->get_current_data(), row1->get_current_length(),
      row2->get_current_data(), row2->get_current_length(), ctype, is_cs);
  row1->move_next();
  row2->move_next();
  if (ret > 0)
    ret = 1;
  else if (ret < 0)
    ret = -1;
  return ret;
}

int MySQLColumnCompare::compare(Packet *packet1, Packet *packet2,
                                list<int> *column_nums,
                                vector<MySQLColumnType> *column_types) {
  MySQLRowResponse row1(packet1);
  MySQLRowResponse row2(packet2);

  list<int>::iterator it;
  for (it = column_nums->begin(); it != column_nums->end(); ++it) {
    int ret = compare(&row1, &row2, (*it), (*column_types)[*it],
                      CHARSET_TYPE_OTHER, false);
    if (ret) return ret;
  }

  return 0;
}

int MySQLColumnCompare::compare(Packet *packet1, Packet *packet2,
                                list<SortDesc> *sort_desc,
                                vector<MySQLColumnType> *column_types) {
  MySQLRowResponse row1(packet1);
  MySQLRowResponse row2(packet2);

  list<SortDesc>::iterator it;
  for (it = sort_desc->begin(); it != sort_desc->end(); ++it) {
    int ret = compare(&row1, &row2, (*it), (*column_types)[it->column_index]);
    if (ret) return ret;
  }

  return 0;
}

int field_int_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                  CharsetType ctype, bool is_cs) {
  ACE_UNUSED_ARG(is_cs);
  ACE_UNUSED_ARG(ctype);
  int ret;
  int negative = 0;

  if (s1[0] == '-' || s2[0] == '-') {
    if (s1[0] != s2[0]) {
      return (s1[0] == '-' ? -1 : 1);
    }

    negative = 1;
  }

  if (len1 == len2) {
    ret = memcmp(s1, s2, len1);
    ret = ret > 0 ? 1 : (ret == 0 ? 0 : -1);
  } else {
    ret = len1 > len2 ? 1 : -1;
  }

  if (negative) ret *= -1;

  return ret;
}

int field_str_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                  CharsetType ctype, bool is_cs) {
  int ret;
  string str1(s1, len1);
  string str2(s2, len2);
  if (ctype == CHARSET_TYPE_OTHER) {
    ctype = charset_type_maps[default_charset_type];
  }

  switch (ctype) {
    case CHARSET_TYPE_UTF8MB4: {
      if (is_cs) {
        ret = my_charset_utf8mb4_bin.coll->strnncollsp(
            &my_charset_utf8mb4_bin, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      } else {
        ret = my_charset_utf8mb4_general_ci.coll->strnncollsp(
            &my_charset_utf8mb4_general_ci, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      }
      break;
    }
    case CHARSET_TYPE_UTF8: {
      if (is_cs) {
        ret = my_charset_utf8_bin.coll->strnncollsp(
            &my_charset_utf8_bin, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      } else {
        ret = my_charset_utf8_general_ci.coll->strnncollsp(
            &my_charset_utf8_general_ci, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      }
      break;
    }
    case CHARSET_TYPE_GB18030: {
      if (is_cs) {
        ret = my_charset_gb18030_bin.coll->strnncollsp(
            &my_charset_gb18030_bin, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      } else {
        // my_charset_gb18030_chinese_ci;
        ret = my_charset_gb18030_chinese_ci.coll->strnncollsp(
            &my_charset_gb18030_chinese_ci, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      }
      break;
    }
    case CHARSET_TYPE_GBK:
    default: {
      if (is_cs) {
        ret = my_charset_gbk_bin.coll->strnncollsp(
            &my_charset_gbk_bin, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      } else {
        // add my_charset_gbk_chinese_ci, distinguish capital and lower-case
        // letter while do compare
        ret = my_charset_gbk_chinese_ci.coll->strnncollsp(
            &my_charset_gbk_chinese_ci, (const uchar *)str1.c_str(), len1,
            (const uchar *)str2.c_str(), len2, 0);
      }
      break;
    }
  }
  return ret;
}

int field_double_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                     CharsetType ctype, bool is_cs) {
  ACE_UNUSED_ARG(is_cs);
  ACE_UNUSED_ARG(ctype);
  double d1, d2;
  char tmp;
  char *p1;
  char *p2;

  if (s1[0] == '-' || s2[0] == '-') {
    if (s1[0] != s2[0]) {
      return (s1[0] == '-' ? -1 : 1);
    }
  }

  p1 = (char *)s1;
  tmp = p1[len1];
  p1[len1] = '\0';
  d1 = atof(p1);
  p1[len1] = tmp;

  p2 = (char *)s2;
  tmp = p2[len2];
  p2[len2] = '\0';
  d2 = atof(p2);
  p2[len2] = tmp;

  return (d1 > d2) ? 1 : (d1 == d2 ? 0 : -1);
}

field_cmp_func field_cmp_funcs[MYSQL_TYPE_END] = {
    NULL,              // MYSQL_TYPE_DECIMAL,
    field_int_cmp,     // MYSQL_TYPE_TINY,
    field_int_cmp,     // MYSQL_TYPE_SHORT,
    field_int_cmp,     // MYSQL_TYPE_LONG,
    field_double_cmp,  // MYSQL_TYPE_FLOAT,
    field_double_cmp,  // MYSQL_TYPE_DOUBLE,
    NULL,              // MYSQL_TYPE_NULL,
    field_str_cmp,     // MYSQL_TYPE_TIMESTAMP,
    field_int_cmp,     // MYSQL_TYPE_LONGLONG,
    field_int_cmp,     // MYSQL_TYPE_INT24,
    field_str_cmp,     // MYSQL_TYPE_DATE,
    field_str_cmp,     // MYSQL_TYPE_TIME,
    field_str_cmp,     // MYSQL_TYPE_DATETIME,
    NULL,              // MYSQL_TYPE_YEAR,
    NULL,              // MYSQL_TYPE_NEWDATE,
    field_str_cmp,     // MYSQL_TYPE_VARCHAR,
    NULL,              // MYSQL_TYPE_BIT,
    // skip 229 NULL
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL,
    field_int_cmp,  // MYSQL_TYPE_NEWDECIMAL=246,
    NULL,           // MYSQL_TYPE_ENUM=247,
    NULL,           // MYSQL_TYPE_SET=248,
    field_str_cmp,  // MYSQL_TYPE_TINY_BLOB=249,
    field_str_cmp,  // MYSQL_TYPE_MEDIUM_BLOB=250,
    field_str_cmp,  // MYSQL_TYPE_LONG_BLOB=251,
    field_str_cmp,  // MYSQL_TYPE_BLOB=252,
    field_str_cmp,  // MYSQL_TYPE_VAR_STRING=253,
    field_str_cmp,  // MYSQL_TYPE_STRING=254,
    NULL,           // MYSQL_TYPE_GEOMETRY=255,
};

bool field_is_valid_for_compare(MySQLColumnType column_type) {
  int type_int = (int)column_type;
  if (type_int < 0 || type_int > 255) {
    LOG_ERROR(
        "Find a unexpect column type %d for field_is_valid_for_compare.\n",
        type_int);
    return false;
  }
  return field_cmp_funcs[column_type] != NULL;
}

}  // namespace mysql
}  // namespace dbscale
