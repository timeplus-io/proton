/*
Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.

The MySQL Connector/C is licensed under the terms of the GPLv2
<http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
MySQL Connectors. There are special exceptions to the terms and
conditions of the GPLv2 as it is applied to this software, see the
FLOSS License Exception
<http://www.mysql.com/about/legal/licensing/foss-exception.html>.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published
by the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#include "my_test.h"

#ifdef ENABLE_IF_IN_USE
static int enable_general_log(MYSQL *mysql, int truncate)
{
  int rc;

  rc= mysql_query(mysql, "set @save_global_general_log=@@global.general_log");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "set @@global.general_log=on");
  check_mysql_rc(rc, mysql);

  if (truncate)
  {
    rc= mysql_query(mysql, "truncate mysql.general_log");
    check_mysql_rc(rc, mysql);
  }

  return OK;
}


static int restore_general_log(MYSQL *mysql)
{
  int rc;
  rc= mysql_query(mysql, "set @@global.general_log=@save_global_general_log");
  check_mysql_rc(rc, mysql);

  return OK;
}
#endif

/* Test update/binary logs */

static int test_logs(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  char       data[255];
  size_t     length;
  int        rc;
  short      id;

  rc= mysql_query(mysql, "SET session sql_mode=''");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_logs");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_logs(id smallint, name varchar(20))");
  check_mysql_rc(rc, mysql);

  strcpy((char *)data, "INSERT INTO test_logs VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(data));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[0].buffer= (void *)&id;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)&data;
  my_bind[1].buffer_length= 255;
  my_bind[1].length= (unsigned long *)&length;

  id= 9876;
  strcpy((char *)data, "MySQL - Open Source Database");
  length= strlen(data);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  strcpy((char *)data, "'");
  length= 1;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  strcpy((char *)data, "\"");
  length= 1;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  strcpy((char *)data, "my\'sql\'");
  length= strlen(data);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  strcpy((char *)data, "my\"sql\"");
  length= strlen(data);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  strcpy((char *)data, "INSERT INTO test_logs VALUES(20, 'mysql')");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(data));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  strcpy((char *)data, "SELECT * FROM test_logs WHERE id=?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(data));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  my_bind[1].buffer_length= 255;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(id == 9876, "id != 9876");
  FAIL_UNLESS(length == 19 || length == 20, "Invalid Length"); /* Due to VARCHAR(20) */
  FAIL_UNLESS(strncmp(data, "MySQL - Open Source", 19) == 0, "data != 'MySQL - Open Source'");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(length == 1, "length != 1");
  FAIL_UNLESS(strcmp(data, "'") == 0, "data != '''");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(length == 1, "length != 1");
  FAIL_UNLESS(strcmp(data, "\"") == 0, "data != '\"'");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(length == 7, "length != 7");
  FAIL_UNLESS(strcmp(data, "my\'sql\'") == 0, "data != my'sql'");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(length == 7, "length != 7");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_logs");
  check_mysql_rc(rc, mysql);

  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_logs", test_logs, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {NULL, NULL, 0, 0, NULL, NULL}
};

int main(int argc, char **argv)
{
  if (argc > 1)
    get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  return(exit_status());
}
