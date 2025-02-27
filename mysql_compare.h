#ifndef DBSCALE_MYSQL_COMPARE_H
#define DBSCALE_MYSQL_COMPARE_H

/*
 * Copyright (C) 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <exception.h>
#include <mysql_comm.h>
#include <sql_parser.h>

#include "mysql_request.h"

namespace dbscale {
namespace mysql {

typedef int (*field_cmp_func)(const char *s1, size_t len1, const char *s2,
                              size_t len2, CharsetType ctype, bool is_cs);
int field_int_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                  CharsetType ctype, bool is_cs);
int field_str_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                  CharsetType ctype, bool is_cs);
int field_double_cmp(const char *s1, size_t len1, const char *s2, size_t len2,
                     CharsetType ctype, bool is_cs);

extern field_cmp_func field_cmp_funcs[];

bool field_is_valid_for_compare(MySQLColumnType column_type);

class MySQLColumnCompare {
 public:
  static int compare(MySQLRowResponse *row1, MySQLRowResponse *row2,
                     int column_index, MySQLColumnType column_type,
                     CharsetType ctype = CHARSET_TYPE_OTHER,
                     bool is_cs = false);

  static int compare(Packet *packet1, Packet *packet2, list<int> *column_nums,
                     vector<MySQLColumnType> *column_types);

  static int compare(MySQLRowResponse *row1, MySQLRowResponse *row2,
                     SortDesc sort_desc, MySQLColumnType column_type) {
    return compare(row1, row2, sort_desc.column_index, column_type,
                   sort_desc.ctype, sort_desc.is_cs) *
           sort_desc.sort_order;
  }

  static int compare(Packet *packet1, Packet *packet2,
                     list<SortDesc> *sort_desc,
                     vector<MySQLColumnType> *column_types);
};

}  // namespace mysql
}  // namespace dbscale

#endif
