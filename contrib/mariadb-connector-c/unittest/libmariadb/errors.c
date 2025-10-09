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

/* Test warnings */

static int test_client_warnings(MYSQL *mysql)
{
  int        rc;

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 
  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 

  FAIL_IF(!mysql_warning_count(mysql), "Warning expected");

  return OK;
}


static int test_ps_client_warnings(MYSQL *mysql)
{
  int        rc;
  MYSQL_STMT *stmt;
  const char *query= "DROP TABLE IF EXISTS test_non_exists";

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(rc, mysql_stmt_error(stmt));

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(rc, mysql_stmt_error(stmt));

  FAIL_IF(!mysql_warning_count(mysql), "Warning expected");

  mysql_stmt_close(stmt);

  return OK;
}

static int test_server_warnings(MYSQL *mysql)
{
  int        rc;
  MYSQL_RES  *result;

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 
  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 

  rc= mysql_query(mysql, "SHOW WARNINGS");
  check_mysql_rc(rc, mysql); 

  result= mysql_store_result(mysql);
  FAIL_IF(!result, mysql_error(mysql));
  FAIL_IF(!mysql_num_rows(result), "Empty resultset");

  mysql_free_result(result);

  return OK;
}


/* Test errors */

static int test_client_errors(MYSQL *mysql)
{
  int        rc;

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 

  rc= mysql_query(mysql, "DROP TABLE test_non_exists");
  FAIL_IF(!rc, "Error expected"); 

  FAIL_IF(!mysql_errno(mysql), "Error expected");
  FAIL_IF(!strlen(mysql_error(mysql)), "Empty errormsg");
  FAIL_IF(strcmp(mysql_sqlstate(mysql), "00000") == 0, "Invalid SQLstate");

  return OK;
}

static int test_ps_client_errors(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  const char *query= "DROP TABLE test_non_exists";

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(rc, mysql_stmt_error(stmt));

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, mysql_stmt_error(stmt));

  FAIL_IF(!mysql_stmt_errno(stmt), "Error expected");
  FAIL_IF(!strlen(mysql_stmt_error(stmt)), "Empty errormsg");
  FAIL_IF(strcmp(mysql_stmt_sqlstate(stmt), "00000") == 0, "Invalid SQLstate");

  mysql_stmt_close(stmt);

  return OK;
}

static int test_server_errors(MYSQL *mysql)
{
  int        rc;
  MYSQL_RES  *result;

  rc= mysql_query(mysql, "DROP TABLE if exists test_non_exists");
  check_mysql_rc(rc, mysql); 

  rc= mysql_query(mysql, "DROP TABLE test_non_exists");
  check_mysql_rc(rc, mysql); 

  rc= mysql_query(mysql, "SHOW ERRORS");
  check_mysql_rc(rc, mysql); 

  result= mysql_store_result(mysql);
  FAIL_IF(!result, mysql_error(mysql));
  FAIL_IF(!mysql_num_rows(result), "Empty resultset");
  mysql_free_result(result);

  return OK;
}

/* Bug #16143: mysql_stmt_sqlstate returns an empty string instead of '00000' */

static int test_bug16143(MYSQL *mysql)
{
  MYSQL_STMT *stmt;

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  /* Check mysql_stmt_sqlstate return "no error" */
  FAIL_UNLESS(strcmp(mysql_stmt_sqlstate(stmt), "00000") == 0, "Expected SQLstate 000000");

  mysql_stmt_close(stmt);

  return OK;
}

/* Test warnings for cuted rows */

static int test_cuted_rows(MYSQL *mysql)
{
  int        rc, count;
  MYSQL_RES  *result;

  if (!is_mariadb)
    return SKIP;

  mysql_query(mysql, "DROP TABLE if exists t1");
  mysql_query(mysql, "DROP TABLE if exists t2");

  rc= mysql_query(mysql, "CREATE TABLE t1(c1 tinyint)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t2(c1 int not null)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 values(10), (NULL), (NULL)");
  check_mysql_rc(rc, mysql);

  count= mysql_warning_count(mysql);
  FAIL_UNLESS(count == 0, "warnings != 0");

  rc= mysql_query(mysql, "INSERT INTO t2 SELECT * FROM t1");
  check_mysql_rc(rc, mysql);

  count= mysql_warning_count(mysql);
  FAIL_UNLESS(count == 2, "warnings != 2");

  rc= mysql_query(mysql, "SHOW WARNINGS");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 2, "rowcount != 2");
  mysql_free_result(result);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES('junk'), (876789)");
  check_mysql_rc(rc, mysql);

  count= mysql_warning_count(mysql);
  FAIL_UNLESS(count == 2, "warnings != 2");

  rc= mysql_query(mysql, "SHOW WARNINGS");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 2, "rowcount != 2");
  mysql_free_result(result);

  rc= mysql_query(mysql, "DROP TABLE t1, t2");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_parse_error_and_bad_length(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  char stmt_str[128];

  /* check that we get 4 syntax errors over the 4 calls */

  rc= mysql_query(mysql, "SHOW DATABAAAA");
  FAIL_UNLESS(rc, "Error expected");
  rc= mysql_real_query(mysql, SL_BIN("SHOW DATABASES\0AAA"));
  FAIL_UNLESS(rc, "Error expected");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SHOW DATABAAAA"));
  FAIL_IF(!rc, "Error expected");
  mysql_stmt_close(stmt);
  stmt= mysql_stmt_init(mysql);
  FAIL_UNLESS(stmt, "");
  memset(stmt_str, 0, 100);
  strcpy(stmt_str, "SHOW DATABASES");
  rc= mysql_stmt_prepare(stmt, stmt_str, 99);
  FAIL_IF(!rc, "Error expected");
  mysql_stmt_close(stmt);
  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_client_warnings", test_client_warnings, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ps_client_warnings", test_ps_client_warnings, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_server_warnings", test_server_warnings, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_client_errors", test_client_errors, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ps_client_errors", test_ps_client_errors, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_server_errors", test_server_errors, TEST_CONNECTION_DEFAULT, 0, NULL , "Open bug: #42364"},
  {"test_bug16143", test_bug16143, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_cuted_rows", test_cuted_rows, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_parse_error_and_bad_length", test_parse_error_and_bad_length, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
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
