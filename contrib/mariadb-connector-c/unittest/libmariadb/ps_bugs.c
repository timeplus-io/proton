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

#define MY_INT64_NUM_DECIMAL_DIGITS 21
#define MAX_INDEXES 64

/* A workaround for Sun Forte 5.6 on Solaris x86 */

static int cmp_double(double *a, double *b)
{
  return *a == *b;
  return OK;
}

/* Test BUG#1115 (incorrect string parameter value allocation) */

static int test_conc67(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  const char *query= "SELECT a,b FROM conc67 WHERE a=?";
  int rc, i;
  MYSQL_BIND bind[2];
  char val[20];
  MYSQL_BIND rbind;
  MYSQL_RES *res;
  ulong prefetch_rows= 1000;
  ulong cursor_type= CURSOR_TYPE_READ_ONLY;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc67");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE conc67 (a int, b text)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO conc67 VALUES (1, 'foo')");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor_type);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch_rows);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(&rbind, 0, sizeof(MYSQL_BIND));
  i= 1;
  rbind.buffer_type= MYSQL_TYPE_LONG;
  rbind.buffer= &i;
  rbind.buffer_length= 4;
  mysql_stmt_bind_param(stmt, &rbind);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  res= mysql_stmt_result_metadata(stmt);
  mysql_free_result(res);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));

  i= 0;
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= &i;
  bind[0].buffer_length= 4;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer= &val;
  bind[1].buffer_length= 20;

  mysql_stmt_bind_result(stmt, bind);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(i != 1, "expected value 1 for first row");

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != MYSQL_NO_DATA, "Eof expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc67");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug1115(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, rowcount;
  MYSQL_BIND my_bind[1];
  ulong length[1];
  char szData[11];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(\
session_id  char(9) NOT NULL, \
    a       int(8) unsigned NOT NULL, \
    b        int(5) NOT NULL, \
    c      int(5) NOT NULL, \
    d  datetime NOT NULL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES "
                         "(\"abc\", 1, 2, 3, 2003-08-30), "
                         "(\"abd\", 1, 2, 3, 2003-08-30), "
                         "(\"abf\", 1, 2, 3, 2003-08-30), "
                         "(\"abg\", 1, 2, 3, 2003-08-30), "
                         "(\"abh\", 1, 2, 3, 2003-08-30), "
                         "(\"abj\", 1, 2, 3, 2003-08-30), "
                         "(\"abk\", 1, 2, 3, 2003-08-30), "
                         "(\"abl\", 1, 2, 3, 2003-08-30), "
                         "(\"abq\", 1, 2, 3, 2003-08-30) ");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES "
                         "(\"abw\", 1, 2, 3, 2003-08-30), "
                         "(\"abe\", 1, 2, 3, 2003-08-30), "
                         "(\"abr\", 1, 2, 3, 2003-08-30), "
                         "(\"abt\", 1, 2, 3, 2003-08-30), "
                         "(\"aby\", 1, 2, 3, 2003-08-30), "
                         "(\"abu\", 1, 2, 3, 2003-08-30), "
                         "(\"abi\", 1, 2, 3, 2003-08-30), "
                         "(\"abo\", 1, 2, 3, 2003-08-30), "
                         "(\"abp\", 1, 2, 3, 2003-08-30), "
                         "(\"abz\", 1, 2, 3, 2003-08-30), "
                         "(\"abx\", 1, 2, 3, 2003-08-30)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT * FROM test_select WHERE "
                "CONVERT(session_id USING utf8)= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "Paramcount != 1");

  memset(my_bind, '\0', sizeof(MYSQL_BIND));

  strcpy(szData, (char *)"abc");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 3;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount=%d != 1");

  strcpy(szData, (char *)"venu");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 4;
  my_bind[0].is_null= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 0, "rowcount != 0");

  strcpy(szData, (char *)"abc");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 3;
  my_bind[0].is_null= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  return OK;
}
/* Test BUG#1180 (optimized away part of WHERE clause) */

static int test_bug1180(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, rowcount;
  MYSQL_BIND my_bind[1];
  ulong length[1];
  char szData[11];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(session_id  char(9) NOT NULL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES (\"abc\")");
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT * FROM test_select WHERE ?= \"1111\" and "
                "session_id= \"abc\"");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "Paramcount != 1");

  memset(my_bind, '\0', sizeof(MYSQL_BIND));

  strcpy(szData, (char *)"abc");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 3;
  my_bind[0].is_null= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);


  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 0, "rowcount != 0");

  strcpy(szData, (char *)"1111");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 4;
  my_bind[0].is_null= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");

  strcpy(szData, (char *)"abc");
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)szData;
  my_bind[0].buffer_length= 10;
  my_bind[0].length= &length[0];
  length[0]= 3;
  my_bind[0].is_null= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 0, "rowcount != 0");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  return OK;
}


/*
  Test BUG#1644 (Insertion of more than 3 NULL columns with parameter
  binding fails)
*/

static int test_bug1644(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES *result;
  MYSQL_ROW row;
  MYSQL_BIND my_bind[4];
  int num;
  my_bool isnull;
  int rc, i;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS foo_dfr");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
           "CREATE TABLE foo_dfr(col1 int, col2 int, col3 int, col4 int);");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO foo_dfr VALUES (?, ?, ?, ? )");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 4, "Paramcount != 4");

  memset(my_bind, '\0', sizeof(MYSQL_BIND) * 4);

  num= 22;
  isnull= 0;
  for (i= 0 ; i < 4 ; i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_LONG;
    my_bind[i].buffer= (void *)&num;
    my_bind[i].is_null= &isnull;
  }

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  isnull= 1;
  for (i= 0 ; i < 4 ; i++)
    my_bind[i].is_null= &isnull;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  isnull= 0;
  num= 88;
  for (i= 0 ; i < 4 ; i++)
    my_bind[i].is_null= &isnull;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM foo_dfr");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid resultset");

  FAIL_IF(mysql_num_rows(result) != 3, "rowcount != 3");

  mysql_data_seek(result, 0);

  row= mysql_fetch_row(result);
  FAIL_IF(!row, "row = NULL");
  for (i= 0 ; i < 4 ; i++)
  {
    FAIL_UNLESS(strcmp(row[i], "22") == 0, "Wrong value");
  }
  row= mysql_fetch_row(result);
  FAIL_IF(!row, "Invalid row");
  for (i= 0 ; i < 4 ; i++)
  {
    FAIL_UNLESS(row[i] == 0, "row[i] != 0");
  }
  row= mysql_fetch_row(result);
  FAIL_IF(!row, "Invalid row");
  for (i= 0 ; i < 4 ; i++)
  {
    FAIL_UNLESS(strcmp(row[i], "88") == 0, "row[i] != 88");
  }
  row= mysql_fetch_row(result);
  FAIL_IF(row, "row != NULL");

  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS foo_dfr");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug11037(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmt_text;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (id int not null)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (1)");
  check_mysql_rc(rc, mysql);

  stmt_text= "select id FROM t1";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  /* expected error */
  rc = mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc==1, "Error expedted");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc==MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc==MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#11183 "mysql_stmt_reset() doesn't reset information about error" */

static int test_bug11183(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  char bug_statement[]= "insert into t1 values (1)";

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (a int)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(bug_statement));
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  /* Trying to execute statement that should fail on execute stage */
  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_reset(stmt);
  FAIL_IF(mysql_stmt_errno(stmt) != 0, "stmt->error != 0");

  rc= mysql_query(mysql, "create table t1 (a int)");
  check_mysql_rc(rc, mysql);

  /* Trying to execute statement that should pass ok */
  if (mysql_stmt_execute(stmt))
  {
    mysql_stmt_reset(stmt);
    FAIL_IF(mysql_stmt_errno(stmt) == 0, "stmt->error != 0");
  }

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug12744(MYSQL *mysql)
{
  MYSQL_STMT *stmt = NULL;
  int rc;

  stmt = mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, "SET @a:=1", 9);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  /* set reconnect, kill and ping to reconnect */
  rc= mysql_query(mysql, "SET @a:=1");
  check_mysql_rc(rc, mysql);
  rc= mysql_options(mysql, MYSQL_OPT_RECONNECT, "1");
  check_mysql_rc(rc, mysql);
  rc= mysql_kill(mysql, mysql_thread_id(mysql));

  rc= mysql_ping(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug1500(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[3];
  int        rc= 0;
  int32 int_data[3]= {2, 3, 4};
  const char *data;
  const char *query;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bg1500");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bg1500 (i INT)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bg1500 VALUES (1), (2)");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  query= "SELECT i FROM test_bg1500 WHERE i IN (?, ?, ?)";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "paramcount != 3");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)int_data;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[2]= my_bind[1]= my_bind[0];
  my_bind[1].buffer= (void *)(int_data + 1);
  my_bind[2].buffer= (void *)(int_data + 2);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_bg1500");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bg1500 (s VARCHAR(25), FULLTEXT(s)) engine=MyISAM");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
        "INSERT INTO test_bg1500 VALUES ('Gravedigger'), ('Greed'), ('Hollow Dogs')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  query= "SELECT s FROM test_bg1500 WHERE MATCH (s) AGAINST (?)";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "paramcount != 1");

  data= "Dogs";
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *) data;
  my_bind[0].buffer_length= (unsigned long)strlen(data);
  my_bind[0].is_null= 0;
  my_bind[0].length= 0;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  mysql_stmt_close(stmt);

  /* This should work too */
  query= "SELECT s FROM test_bg1500 WHERE MATCH (s) AGAINST (CONCAT(?, 'digger'))";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "paramcount != 1");

  data= "Grave";
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *) data;
  my_bind[0].buffer_length= (unsigned long)strlen(data);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bg1500");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug15510(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query= "select 1 from dual where 1/0";


  rc= mysql_query(mysql, "set @@sql_mode='ERROR_FOR_DIVISION_BY_ZERO'");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(mysql_warning_count(mysql), "Warning expected");

  /* Cleanup */
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "set @@sql_mode=''");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug #15518 - Reusing a stmt that has failed during prepare
  does not clear error
*/

static int test_bug15518(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;

  stmt= mysql_stmt_init(mysql);

  /*
    The prepare of foo should fail with errno 1064 since
    it's not a valid query
  */
  rc= mysql_stmt_prepare(stmt, "foo", 3);
  FAIL_UNLESS(rc && mysql_stmt_errno(stmt) && mysql_errno(mysql), "Error expected");

  /*
    Use the same stmt and reprepare with another query that
    succeeds
  */
  rc= mysql_stmt_prepare(stmt, "SHOW STATUS", 12);
  FAIL_UNLESS(!rc || mysql_stmt_errno(stmt) || mysql_errno(mysql), "Error expected");

  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);
  /*
    part2, when connection to server has been closed
    after first prepare
  */
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, "foo", 3);
  FAIL_UNLESS(rc && mysql_stmt_errno(stmt) && mysql_errno(mysql), "Error expected");

  /* Close connection to server */
  mysql_close(mysql);

  /*
    Use the same stmt and reprepare with another query that
    succeeds. The prepare should fail with error 2013 since
    connection to server has been closed.
  */
  rc= mysql_stmt_prepare(stmt, "SHOW STATUS", 12);
  FAIL_UNLESS(rc && mysql_stmt_errno(stmt), "Error expected");

  mysql_stmt_close(stmt);

  return OK;
}

/*
  Bug #15613: "libmysqlclient API function mysql_stmt_prepare returns wrong
  field length"
*/

static int test_bug15613(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  MYSQL_RES *metadata;
  MYSQL_FIELD *field;
  int rc;

  /* I. Prepare the table */
  rc= mysql_query(mysql, "set names latin1");
  check_mysql_rc(rc, mysql);
  mysql_query(mysql, "drop table if exists t1");
  rc= mysql_query(mysql,
                  "create table t1 (t text character set utf8, "
                                   "tt tinytext character set utf8, "
                                   "mt mediumtext character set utf8, "
                                   "lt longtext character set utf8, "
                                   "vl varchar(255) character set latin1,"
                                   "vb varchar(255) character set binary,"
                                   "vu varchar(255) character set utf8)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);

  /* II. Check SELECT metadata */
  stmt_text= ("select t, tt, mt, lt, vl, vb, vu from t1");
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_fields(metadata);
  FAIL_UNLESS(field[0].length == 65535, "length != 65535");
  FAIL_UNLESS(field[1].length == 255, "length != 244");
  FAIL_UNLESS(field[2].length == 16777215, "length != 166777215");
  FAIL_UNLESS(field[3].length == 4294967295UL, "length != 4294967295UL");
  FAIL_UNLESS(field[4].length == 255, "length != 255");
  FAIL_UNLESS(field[5].length == 255, "length != 255");
  FAIL_UNLESS(field[6].length == 255, "length != 255");
  mysql_free_result(metadata);
  mysql_stmt_free_result(stmt);

  /* III. Cleanup */
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "set names default");
  check_mysql_rc(rc, mysql);
  mysql_stmt_close(stmt);

  return OK;
}

static int test_bug16144(MYSQL *mysql)
{
  const my_bool flag_orig= (my_bool) 0xde;
  my_bool flag= flag_orig;
  MYSQL_STMT *stmt;

  /* Check that attr_get returns correct data on little and big endian CPUs */
  stmt= mysql_stmt_init(mysql);
  mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, (const void*) &flag);
  mysql_stmt_attr_get(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, (void*) &flag);
  FAIL_UNLESS(flag == flag_orig, "flag != flag_orig");

  mysql_stmt_close(stmt);

  return OK;
}

/*
  This tests for various mysql_stmt_send_long_data bugs described in #1664
*/

static int test_bug1664(MYSQL *mysql)
{
    MYSQL_STMT *stmt;
    int        rc, int_data;
    const char *data;
    const char *str_data= "Simple string";
    MYSQL_BIND my_bind[2];
    const char *query= "INSERT INTO test_long_data(col2, col1) VALUES(?, ?)";


    rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data");
    check_mysql_rc(rc, mysql);

    rc= mysql_query(mysql, "CREATE TABLE test_long_data(col1 int, col2 long varchar)");
    check_mysql_rc(rc, mysql);

    stmt= mysql_stmt_init(mysql);
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Param count != 2");

    memset(my_bind, '\0', sizeof(my_bind));

    my_bind[0].buffer_type= MYSQL_TYPE_STRING;
    my_bind[0].buffer= (void *)str_data;
    my_bind[0].buffer_length= (unsigned long)strlen(str_data);

    my_bind[1].buffer= (void *)&int_data;
    my_bind[1].buffer_type= MYSQL_TYPE_LONG;

    rc= mysql_stmt_bind_param(stmt, my_bind);
    check_stmt_rc(rc, stmt);

    int_data= 1;

    /*
      Let us supply empty long_data. This should work and should
      not break following execution.
    */
    data= "";
    rc= mysql_stmt_send_long_data(stmt, 0, SL(data));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    if (verify_col_data(mysql, "test_long_data", "col1", "1"))
      goto error;
    if (verify_col_data(mysql, "test_long_data", "col2", ""))
      goto error;
    rc= mysql_query(mysql, "DELETE FROM test_long_data");
    check_mysql_rc(rc, mysql);

    /* This should pass OK */
    data= (char *)"Data";
    rc= mysql_stmt_send_long_data(stmt, 0, SL(data));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    if (verify_col_data(mysql, "test_long_data", "col1", "1"))
      goto error;
    if (verify_col_data(mysql, "test_long_data", "col2", "Data"))
      goto error;

    /* clean up */
    rc= mysql_query(mysql, "DELETE FROM test_long_data");
    check_mysql_rc(rc, mysql);

    /*
      Now we are changing int parameter and don't do anything
      with first parameter. Second mysql_stmt_execute() should run
      OK treating this first parameter as string parameter.
    */

    int_data= 2;
    /* execute */
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    if (verify_col_data(mysql, "test_long_data", "col1", "2"))
      goto error;
    if (verify_col_data(mysql, "test_long_data", "col2", str_data))
      goto error;

    /* clean up */
    rc= mysql_query(mysql, "DELETE FROM test_long_data");
    check_mysql_rc(rc, mysql);

    /*
      Now we are sending other long data. It should not be
      concatened to previous.
    */

    data= (char *)"SomeOtherData";
    rc= mysql_stmt_send_long_data(stmt, 0, SL(data));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    if (verify_col_data(mysql, "test_long_data", "col1", "2"))
      goto error;
    if (verify_col_data(mysql, "test_long_data", "col2", "SomeOtherData"))
      goto error;

    mysql_stmt_close(stmt);

    /* clean up */
    rc= mysql_query(mysql, "DELETE FROM test_long_data");
    check_mysql_rc(rc, mysql);

    /* Now let us test how mysql_stmt_reset works. */
    stmt= mysql_stmt_init(mysql);
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_bind_param(stmt, my_bind);
    check_stmt_rc(rc, stmt);

    data= (char *)"SomeData";
    rc= mysql_stmt_send_long_data(stmt, 0, SL(data));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_reset(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    if (verify_col_data(mysql, "test_long_data", "col1", "2"))
      goto error;
    if (verify_col_data(mysql, "test_long_data", "col2", str_data))
      goto error;

    mysql_stmt_close(stmt);

    /* Final clean up */
    rc= mysql_query(mysql, "DROP TABLE test_long_data");
    check_mysql_rc(rc, mysql);

    return OK;

error:
    mysql_stmt_close(stmt);
    rc= mysql_query(mysql, "DROP TABLE test_long_data");
    return FAIL;
}
/* Test a misc bug */

static int test_ushort_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[4];
  ushort     short_value;
  uint32     long_value;
  ulong      s_length, l_length, ll_length, t_length;
  ulonglong  longlong_value;
  int        rc;
  uchar      tiny_value;
  const char *query= "SELECT * FROM test_ushort";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_ushort");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_ushort(a smallint unsigned, \
                                                  b smallint unsigned, \
                                                  c smallint unsigned, \
                                                  d smallint unsigned)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "INSERT INTO test_ushort VALUES(35999, 35999, 35999, 200)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[0].buffer= (void *)&short_value;
  my_bind[0].is_unsigned= TRUE;
  my_bind[0].length= &s_length;

  my_bind[1].buffer_type= MYSQL_TYPE_LONG;
  my_bind[1].buffer= (void *)&long_value;
  my_bind[1].length= &l_length;

  my_bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[2].buffer= (void *)&longlong_value;
  my_bind[2].length= &ll_length;

  my_bind[3].buffer_type= MYSQL_TYPE_TINY;
  my_bind[3].buffer= (void *)&tiny_value;
  my_bind[3].is_unsigned= TRUE;
  my_bind[3].length= &t_length;

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(short_value == 35999, "short_value != 35999");
  FAIL_UNLESS(s_length == 2, "length != 2");

  FAIL_UNLESS(long_value == 35999, "long_value != 35999");
  FAIL_UNLESS(l_length == 4, "length != 4");

  FAIL_UNLESS(longlong_value == 35999, "longlong_value != 35999");
  FAIL_UNLESS(ll_length == 8, "length != 8");

  FAIL_UNLESS(tiny_value == 200, "tiny_value != 200");
  FAIL_UNLESS(t_length == 1, "length != 1");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_ushort");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug1946(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query= "INSERT INTO prepare_command VALUES (?)";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS prepare_command");
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, "CREATE TABLE prepare_command(ID INT)");
  check_mysql_rc(rc, mysql)

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_real_query(mysql, SL(query));
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE prepare_command");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug20152(MYSQL *mysql)
{
  MYSQL_BIND my_bind[1];
  MYSQL_STMT *stmt;
  MYSQL_TIME tm;
  int rc;
  const char *query= "INSERT INTO t1 (f1) VALUES (?)";


  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_DATE;
  my_bind[0].buffer= (void*)&tm;

  memset(&tm, 0, sizeof(MYSQL_TIME));

  tm.year = 2006;
  tm.month = 6;
  tm.day = 18;
  tm.hour = 14;
  tm.minute = 9;
  tm.second = 42;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql)
  rc= mysql_query(mysql, "CREATE TABLE t1 (f1 DATE)");
  check_mysql_rc(rc, mysql)

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql)
  FAIL_UNLESS(tm.hour == 14 && tm.minute == 9 && tm.second == 42, "time != 14:09:42");
  return OK;
}

static int test_bug2247(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES *res;
  int rc;
  int i;
  const char *create= "CREATE TABLE bug2247(id INT UNIQUE AUTO_INCREMENT)";
  const char *insert= "INSERT INTO bug2247 VALUES (NULL)";
  const char *SELECT= "SELECT id FROM bug2247";
  const char *update= "UPDATE bug2247 SET id=id+10";
  const char *drop= "DROP TABLE IF EXISTS bug2247";
  ulonglong exp_count;
  enum { NUM_ROWS= 5 };


  /* create table and insert few rows */
  rc= mysql_query(mysql, drop);
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, create);
  check_mysql_rc(rc, mysql)

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(insert));
  check_stmt_rc(rc, stmt);
  for (i= 0; i < NUM_ROWS; ++i)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }
  exp_count= mysql_stmt_affected_rows(stmt);
  FAIL_UNLESS(exp_count == 1, "exp_count != 1");

  rc= mysql_query(mysql, SELECT);
  check_mysql_rc(rc, mysql)
  /*
    mysql_store_result overwrites mysql->affected_rows. Check that
    mysql_stmt_affected_rows() returns the same value, whereas
    mysql_affected_rows() value is correct.
  */
  res= mysql_store_result(mysql);
  FAIL_IF(!res, "Invalid result set");

  FAIL_UNLESS(mysql_affected_rows(mysql) == NUM_ROWS, "affected_rows != NUM_ROWS");
  FAIL_UNLESS(exp_count == mysql_stmt_affected_rows(stmt), "affected_rows != exp_count");

  rc= mysql_query(mysql, update);
  check_mysql_rc(rc, mysql)
  FAIL_UNLESS(mysql_affected_rows(mysql) == NUM_ROWS, "affected_rows != NUM_ROWS");
  FAIL_UNLESS(exp_count == mysql_stmt_affected_rows(stmt), "affected_rows != exp_count");

  mysql_free_result(res);
  mysql_stmt_close(stmt);

  /* check that mysql_stmt_store_result modifies mysql_stmt_affected_rows */
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(SELECT));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);  exp_count= mysql_stmt_affected_rows(stmt);
  FAIL_UNLESS(exp_count == NUM_ROWS, "exp_count != NUM_ROWS");

  rc= mysql_query(mysql, insert);
  check_mysql_rc(rc, mysql)
  FAIL_UNLESS(mysql_affected_rows(mysql) == 1, "affected_rows != 1");
  FAIL_UNLESS(exp_count == mysql_stmt_affected_rows(stmt), "affected_rows != exp_count");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, drop);
  check_mysql_rc(rc, mysql)
  return OK;
}

/*
  Test for bug#2248 "mysql_fetch without prior mysql_stmt_execute hangs"
*/

static int test_bug2248(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query1= "SELECT DATABASE()";
  const char *query2= "INSERT INTO test_bug2248 VALUES (10)";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bug2248");
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, "CREATE TABLE test_bug2248 (id int)");
  check_mysql_rc(rc, mysql)

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query1));
  check_stmt_rc(rc, stmt);

  /* This should not hang */
  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(!rc, "Error expected");

  /* And this too */
  rc= mysql_stmt_store_result(stmt);
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query2));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* This too should not hang but should return proper error */
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 1, "rc != 1");

  /* This too should not hang but should not bark */
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);
  /* This should return proper error */
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 1, "rc != 1");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_bug2248");
  check_mysql_rc(rc, mysql)
  return OK;
}

/*
  BUG#23383: mysql_affected_rows() returns different values than
  mysql_stmt_affected_rows()

  Test that both mysql_affected_rows() and mysql_stmt_affected_rows()
  return -1 on error, 0 when no rows were affected, and (positive) row
  count when some rows were affected.
*/
static int test_bug23383(MYSQL *mysql)
{
  const char *insert_query= "INSERT INTO t1 VALUES (1), (2)";
  const char *update_query= "UPDATE t1 SET i= 4 WHERE i = 3";
  MYSQL_STMT *stmt;
  unsigned long long row_count;
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (i INT UNIQUE)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, insert_query);
  check_mysql_rc(rc, mysql);
  row_count= mysql_affected_rows(mysql);
  FAIL_UNLESS(row_count == 2, "row_count != 2");

  rc= mysql_query(mysql, insert_query);
  FAIL_IF(!rc, "Error expected");
  row_count= mysql_affected_rows(mysql);
  FAIL_UNLESS(row_count == (unsigned long long)-1, "rowcount != -1");

  rc= mysql_query(mysql, update_query);
  check_mysql_rc(rc, mysql);
  row_count= mysql_affected_rows(mysql);
  FAIL_UNLESS(row_count == 0, "");

  rc= mysql_query(mysql, "DELETE FROM t1");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(insert_query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  row_count= mysql_stmt_affected_rows(stmt);
  FAIL_UNLESS(row_count == 2, "row_count != 2");

  rc= mysql_stmt_execute(stmt);
  FAIL_UNLESS(rc != 0, "");
  row_count= mysql_stmt_affected_rows(stmt);
  FAIL_UNLESS(row_count == (unsigned long long)-1, "rowcount != -1");

  rc= mysql_stmt_prepare(stmt, SL(update_query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  row_count= mysql_stmt_affected_rows(stmt);
  FAIL_UNLESS(row_count == 0, "rowcount != 0");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug#27592 (stack overrun when storing datetime value using prepared statements)
*/

static int test_bug27592(MYSQL *mysql)
{
  const int NUM_ITERATIONS= 40;
  int i;
  int rc;
  MYSQL_STMT *stmt= NULL;
  MYSQL_BIND bind[1];
  MYSQL_TIME time_val;

  mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  mysql_query(mysql, "CREATE TABLE t1(c2 DATETIME)");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO t1 VALUES (?)"));
  check_stmt_rc(rc, stmt);

  memset(bind, '\0', sizeof(bind));

  bind[0].buffer_type= MYSQL_TYPE_DATETIME;
  bind[0].buffer= (char *) &time_val;
  bind[0].length= NULL;

  for (i= 0; i < NUM_ITERATIONS; i++)
  {
    time_val.year= 2007;
    time_val.month= 6;
    time_val.day= 7;
    time_val.hour= 18;
    time_val.minute= 41;
    time_val.second= 3;

    time_val.second_part=0;
    time_val.neg=0;

    rc= mysql_stmt_bind_param(stmt, bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  mysql_stmt_close(stmt);
  mysql_query(mysql, "DROP TABLE IF EXISTS t1");

  return OK;
}

/*
  Bug#28934: server crash when receiving malformed com_execute packets
*/

static int test_bug28934(MYSQL *mysql)
{
  my_bool error= 0;
  MYSQL_BIND bind[5];
  MYSQL_STMT *stmt;
  int rc, cnt;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(id int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values(1),(2),(3),(4),(5)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select * from t1 where id in(?,?,?,?,?)"));
  check_stmt_rc(rc, stmt);

  memset (&bind, '\0', sizeof (bind));
  for (cnt= 0; cnt < 5; cnt++)
  {
    bind[cnt].buffer_type= MYSQL_TYPE_LONG;
    bind[cnt].buffer= (char*)&cnt;
    bind[cnt].buffer_length= 0;
  }
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  stmt->param_count=2;
  error= mysql_stmt_execute(stmt);
  FAIL_UNLESS(error != 0, "Error expected");
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug3035(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL_BIND bind_array[12], *my_bind= bind_array, *bind_end= my_bind + 12;
  int8 int8_val;
  uint8 uint8_val;
  int16 int16_val;
  uint16 uint16_val;
  int32 int32_val;
  uint32 uint32_val;
  longlong int64_val;
  ulonglong uint64_val;
  double double_val, udouble_val, double_tmp;
  char longlong_as_string[22], ulonglong_as_string[22];

  /* mins and maxes */
  const int8 int8_min= -128;
  const int8 int8_max= 127;
  const uint8 uint8_min= 0;
  const uint8 uint8_max= 255;

  const int16 int16_min= -32768;
  const int16 int16_max= 32767;
  const uint16 uint16_min= 0;
  const uint16 uint16_max= 65535;

  const int32 int32_max= 2147483647L;
  const int32 int32_min= -int32_max - 1;
  const uint32 uint32_min= 0;
  const uint32 uint32_max= 4294967295U;

  /* it might not work okay everyplace */
  const longlong int64_max= 9223372036854775807LL;
  const longlong int64_min= -int64_max - 1;

  const ulonglong uint64_min= 0U;
  const ulonglong uint64_max= 18446744073709551615ULL;

  const char *stmt_text;


  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "CREATE TABLE t1 (i8 TINYINT, ui8 TINYINT UNSIGNED, "
                              "i16 SMALLINT, ui16 SMALLINT UNSIGNED, "
                              "i32 INT, ui32 INT UNSIGNED, "
                              "i64 BIGINT, ui64 BIGINT UNSIGNED, "
                              "id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  memset(bind_array, '\0', sizeof(bind_array));
  for (my_bind= bind_array; my_bind < bind_end; my_bind++)
    my_bind->error= &my_bind->error_value;

  bind_array[0].buffer_type= MYSQL_TYPE_TINY;
  bind_array[0].buffer= (void *) &int8_val;

  bind_array[1].buffer_type= MYSQL_TYPE_TINY;
  bind_array[1].buffer= (void *) &uint8_val;
  bind_array[1].is_unsigned= 1;

  bind_array[2].buffer_type= MYSQL_TYPE_SHORT;
  bind_array[2].buffer= (void *) &int16_val;

  bind_array[3].buffer_type= MYSQL_TYPE_SHORT;
  bind_array[3].buffer= (void *) &uint16_val;
  bind_array[3].is_unsigned= 1;

  bind_array[4].buffer_type= MYSQL_TYPE_LONG;
  bind_array[4].buffer= (void *) &int32_val;

  bind_array[5].buffer_type= MYSQL_TYPE_LONG;
  bind_array[5].buffer= (void *) &uint32_val;
  bind_array[5].is_unsigned= 1;

  bind_array[6].buffer_type= MYSQL_TYPE_LONGLONG;
  bind_array[6].buffer= (void *) &int64_val;

  bind_array[7].buffer_type= MYSQL_TYPE_LONGLONG;
  bind_array[7].buffer= (void *) &uint64_val;
  bind_array[7].is_unsigned= 1;

  stmt= mysql_stmt_init(mysql);
  check_stmt_rc(rc, stmt);

  stmt_text= "INSERT INTO t1 (i8, ui8, i16, ui16, i32, ui32, i64, ui64) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  mysql_stmt_bind_param(stmt, bind_array);

  int8_val= int8_min;
  uint8_val= uint8_min;
  int16_val= int16_min;
  uint16_val= uint16_min;
  int32_val= int32_min;
  uint32_val= uint32_min;
  int64_val= int64_min;
  uint64_val= uint64_min;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  int8_val= int8_max;
  uint8_val= uint8_max;
  int16_val= int16_max;
  uint16_val= uint16_max;
  int32_val= int32_max;
  uint32_val= uint32_max;
  int64_val= int64_max;
  uint64_val= uint64_max;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  stmt_text= "SELECT i8, ui8, i16, ui16, i32, ui32, i64, ui64, ui64, "
             "cast(ui64 as signed), ui64, cast(ui64 as signed)"
             "FROM t1 ORDER BY id ASC";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  bind_array[8].buffer_type= MYSQL_TYPE_DOUBLE;
  bind_array[8].buffer= (void *) &udouble_val;

  bind_array[9].buffer_type= MYSQL_TYPE_DOUBLE;
  bind_array[9].buffer= (void *) &double_val;

  bind_array[10].buffer_type= MYSQL_TYPE_STRING;
  bind_array[10].buffer= (void *) &ulonglong_as_string;
  bind_array[10].buffer_length= sizeof(ulonglong_as_string);

  bind_array[11].buffer_type= MYSQL_TYPE_STRING;
  bind_array[11].buffer= (void *) &longlong_as_string;
  bind_array[11].buffer_length= sizeof(longlong_as_string);

  mysql_stmt_bind_result(stmt, bind_array);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(int8_val == int8_min, "int8_val != int8_min");
  FAIL_UNLESS(uint8_val == uint8_min, "uint8_val != uint8_min");
  FAIL_UNLESS(int16_val == int16_min, "int16_val != int16_min");
  FAIL_UNLESS(uint16_val == uint16_min, "uint16_val != uint16_min");
  FAIL_UNLESS(int32_val == int32_min, "int32_val != int32_min");
  FAIL_UNLESS(uint32_val == uint32_min, "uint32_val != uint32_min");
  FAIL_UNLESS(int64_val == int64_min, "int64_val != int64_min");
  FAIL_UNLESS(uint64_val == uint64_min, "uint64_val != uint64_min");
  FAIL_UNLESS(double_val == (longlong) uint64_min, "double_val != uint64_min");
  double_tmp= ulonglong2double(uint64_val);
  FAIL_UNLESS(cmp_double(&udouble_val,&double_tmp), "udouble_val != double_tmp");
  FAIL_UNLESS(!strcmp(longlong_as_string, "0"), "longlong_as_string != '0'");
  FAIL_UNLESS(!strcmp(ulonglong_as_string, "0"), "ulonglong_as_string != '0'");

  rc= mysql_stmt_fetch(stmt);

  FAIL_UNLESS(rc == MYSQL_DATA_TRUNCATED || rc == 0, "rc != 0,MYSQL_DATA_TRUNCATED");

  FAIL_UNLESS(int8_val == int8_max, "int8_val != int8_max");
  FAIL_UNLESS(uint8_val == uint8_max, "uint8_val != uint8_max");
  FAIL_UNLESS(int16_val == int16_max, "int16_val != int16_max");
  FAIL_UNLESS(uint16_val == uint16_max, "uint16_val != uint16_max");
  FAIL_UNLESS(int32_val == int32_max, "int32_val != int32_max");
  FAIL_UNLESS(uint32_val == uint32_max, "uint32_val != uint32_max");
  FAIL_UNLESS(int64_val == int64_max, "int64_val != int64_max");
  FAIL_UNLESS(uint64_val == uint64_max, "uint64_val != uint64_max");
  FAIL_UNLESS(double_val == (longlong) uint64_val, "double_val != uint64_val");
  double_tmp= ulonglong2double(uint64_val);
  FAIL_UNLESS(cmp_double(&udouble_val,&double_tmp), "udouble_val != double_tmp");
  FAIL_UNLESS(!strcmp(longlong_as_string, "-1"), "longlong_as_string != '-1'");
  FAIL_UNLESS(!strcmp(ulonglong_as_string, "18446744073709551615"), "ulonglong_as_string != '18446744073709551615'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "");

  mysql_stmt_close(stmt);

  stmt_text= "DROP TABLE t1";
  mysql_real_query(mysql, SL(stmt_text));
  return OK;
}

/*
  Test for BUG#3420 ("select id1, value1 from t where id= ? or value= ?"
  returns all rows in the table)
*/

static int test_ps_conj_select(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  MYSQL_BIND my_bind[2];
  int32      int_data;
  char       str_data[32];
  unsigned long str_length;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (id1 int(11) NOT NULL default '0', "
                         "value2 varchar(100), value1 varchar(100))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (1, 'hh', 'hh'), "
                          "(2, 'hh', 'hh'), (1, 'ii', 'ii'), (2, 'ii', 'ii')");
  check_mysql_rc(rc, mysql);

  strcpy(query, "select id1, value1 from t1 where id1= ? or "
                "CONVERT(value1 USING utf8)= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "param_count != 2");

  /* Always bzero all members of bind parameter */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&int_data;

  my_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[1].buffer= (void *)str_data;
  my_bind[1].buffer_length= array_elements(str_data);
  my_bind[1].length= &str_length;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  int_data= 1;
  strcpy(str_data, "hh");
  str_length= (unsigned long)strlen(str_data);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc=0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
   rc++;
  FAIL_UNLESS(rc == 3, "rc != 3");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test for NULL as PS parameter (BUG#3367, BUG#3371) */

static int test_ps_null_param(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;

  MYSQL_BIND in_bind;
  my_bool    in_is_null;
  long int   in_long;

  MYSQL_BIND out_bind;
  ulong      out_length;
  my_bool    out_is_null;
  char       out_str_data[20];

  const char *queries[]= {"select ?", "select ?+1",
                    "select col1 from test_ps_nulls where col1 <=> ?",
                    NULL
                    };
  const char **cur_query= queries;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_ps_nulls");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_ps_nulls(col1 int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_ps_nulls values (1), (null)");
  check_mysql_rc(rc, mysql);

  /* Always bzero all members of bind parameter */
  memset(&in_bind, '\0', sizeof(in_bind));
  memset(&out_bind, '\0', sizeof(out_bind));
  in_bind.buffer_type= MYSQL_TYPE_LONG;
  in_bind.is_null= &in_is_null;
  in_bind.length= 0;
  in_bind.buffer= (void *)&in_long;
  in_is_null= 1;
  in_long= 1;

  out_bind.buffer_type= MYSQL_TYPE_STRING;
  out_bind.is_null= &out_is_null;
  out_bind.length= &out_length;
  out_bind.buffer= out_str_data;
  out_bind.buffer_length= array_elements(out_str_data);

  /* Execute several queries, all returning NULL in result. */
  for(cur_query= queries; *cur_query; cur_query++)
  {
    char query[MAX_TEST_QUERY_LENGTH];
    strcpy(query, *cur_query);
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    diag("statement: %s", query);
    check_stmt_rc(rc, stmt);
    FAIL_IF(mysql_stmt_param_count(stmt) != 1, "param_count != 1");

    rc= mysql_stmt_bind_param(stmt, &in_bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_bind_result(stmt, &out_bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc != MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
    FAIL_UNLESS(out_is_null, "!out_is_null");
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
    mysql_stmt_close(stmt);
  }
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_ps_nulls");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  utility for the next test; expects 3 rows in the result from a SELECT,
  compares each row/field with an expected value.
 */
#define test_ps_query_cache_result(i1,s1,l1,i2,s2,l2,i3,s3,l3)    \
  r_metadata= mysql_stmt_result_metadata(stmt);                   \
  FAIL_UNLESS(r_metadata != NULL, "");                            \
  rc= mysql_stmt_fetch(stmt);                                     \
  check_stmt_rc(rc,stmt);                                         \
  FAIL_UNLESS((r_int_data == i1) && (r_str_length == l1) &&       \
             (strcmp(r_str_data, s1) == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);                                     \
  check_stmt_rc(rc,stmt);                                         \
  FAIL_UNLESS((r_int_data == i2) && (r_str_length == l2) &&       \
             (strcmp(r_str_data, s2) == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);                                     \
  check_stmt_rc(rc,stmt);                                         \
  FAIL_UNLESS((r_int_data == i3) && (r_str_length == l3) &&       \
             (strcmp(r_str_data, s3) == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);                                     \
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");        \
  mysql_free_result(r_metadata);

/* reads Qcache_hits from server and returns its value */
static int query_cache_hits(MYSQL *mysql)
{
  MYSQL_RES *res;
  MYSQL_ROW row;
  int rc;
  uint result;

  rc= mysql_query(mysql, "show status like 'qcache_hits'");
  check_mysql_rc(rc, mysql);
  res= mysql_use_result(mysql);

  row= mysql_fetch_row(res);

  result= atoi(row[1]);
  mysql_free_result(res);
  return result;
}


/*
  Test that prepared statements make use of the query cache just as normal
  statements (BUG#735).
*/
static int test_ps_query_cache(MYSQL *mysql)
{
  MYSQL      *lmysql= mysql;
  MYSQL_STMT *stmt;
  int        rc;
  MYSQL_BIND p_bind[2],r_bind[2]; /* p: param bind; r: result bind */
  int32      p_int_data, r_int_data;
  char       p_str_data[32], r_str_data[32];
  unsigned long p_str_length, r_str_length;
  MYSQL_RES  *r_metadata;
  char       query[MAX_TEST_QUERY_LENGTH];
  uint       hits1, hits2;
  enum enum_test_ps_query_cache
  {
    /*
      We iterate the same prepare/executes block, but have iterations where
      we vary the query cache conditions.
    */
    /* the query cache is enabled for the duration of prep&execs: */
    TEST_QCACHE_ON= 0,
    /*
      same but using a new connection (to see if qcache serves results from
      the previous connection as it should):
    */
    TEST_QCACHE_ON_WITH_OTHER_CONN,
    /*
      First border case: disables the query cache before prepare and
      re-enables it before execution (to test if we have no bug then):
    */
    TEST_QCACHE_OFF_ON,
    /*
      Second border case: enables the query cache before prepare and
      disables it before execution:
    */
    TEST_QCACHE_ON_OFF
  };
  enum enum_test_ps_query_cache iteration;

  diag("test needs to be fixed");
  return SKIP;
  /* prepare the table */

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (id1 int(11) NOT NULL default '0', "
                         "value2 varchar(100), value1 varchar(100))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (1, 'hh', 'hh'), "
                          "(2, 'hh', 'hh'), (1, 'ii', 'ii'), (2, 'ii', 'ii')");
  check_mysql_rc(rc, mysql);

  for (iteration= TEST_QCACHE_ON; iteration <= TEST_QCACHE_ON_OFF; iteration++)
  {
    switch (iteration) {
    case TEST_QCACHE_ON:
    case TEST_QCACHE_ON_OFF:
      rc= mysql_query(lmysql, "set global query_cache_size=1000000");
      check_mysql_rc(rc, mysql);
      break;
    case TEST_QCACHE_OFF_ON:
      rc= mysql_query(lmysql, "set global query_cache_size=0");
      check_mysql_rc(rc, mysql);
      break;
    case TEST_QCACHE_ON_WITH_OTHER_CONN:
      lmysql= test_connect(NULL);
      FAIL_IF(!lmysql, "Opening new connection failed");
      break;
    }

    strcpy(query, "select id1, value1 from t1 where id1= ? or "
           "CONVERT(value1 USING utf8)= ?");
    stmt= mysql_stmt_init(lmysql);
    FAIL_IF(!stmt, mysql_error(lmysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    FAIL_IF(mysql_stmt_param_count(stmt) != 2, "param_count != 2");

    switch (iteration) {
    case TEST_QCACHE_OFF_ON:
      rc= mysql_query(lmysql, "set global query_cache_size=1000000");
      check_mysql_rc(rc, mysql);
      break;
    case TEST_QCACHE_ON_OFF:
      rc= mysql_query(lmysql, "set global query_cache_size=0");
      check_mysql_rc(rc, mysql);
    default:
      break;
    }

    memset(p_bind, '\0', sizeof(p_bind));
    p_bind[0].buffer_type= MYSQL_TYPE_LONG;
    p_bind[0].buffer= (void *)&p_int_data;
    p_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
    p_bind[1].buffer= (void *)p_str_data;
    p_bind[1].buffer_length= array_elements(p_str_data);
    p_bind[1].length= &p_str_length;

    rc= mysql_stmt_bind_param(stmt, p_bind);
    check_stmt_rc(rc, stmt);
    p_int_data= 1;
    strcpy(p_str_data, "hh");
    p_str_length= (unsigned long)strlen(p_str_data);

    memset(r_bind, '\0', sizeof(r_bind));
    r_bind[0].buffer_type= MYSQL_TYPE_LONG;
    r_bind[0].buffer= (void *)&r_int_data;
    r_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
    r_bind[1].buffer= (void *)r_str_data;
    r_bind[1].buffer_length= array_elements(r_str_data);
    r_bind[1].length= &r_str_length;

    rc= mysql_stmt_bind_result(stmt, r_bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    test_ps_query_cache_result(1, "hh", 2, 2, "hh", 2, 1, "ii", 2);
  r_metadata= mysql_stmt_result_metadata(stmt);
  FAIL_UNLESS(r_metadata != NULL, "");
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);
  FAIL_UNLESS((r_int_data == 1) && (r_str_length == 2) &&
             (strcmp(r_str_data, "hh") == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);
  FAIL_UNLESS((r_int_data == 2) && (r_str_length == 2) &&
             (strcmp(r_str_data, "hh") == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);
  FAIL_UNLESS((r_int_data == 1) && (r_str_length == 2) &&
             (strcmp(r_str_data, "ii") == 0), "test_ps_query_cache_result failure"); \
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
  mysql_free_result(r_metadata);


    /* now retry with the same parameter values and see qcache hits */
    hits1= query_cache_hits(lmysql);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);    test_ps_query_cache_result(1, "hh", 2, 2, "hh", 2, 1, "ii", 2);
    hits2= query_cache_hits(lmysql);
    switch(iteration) {
    case TEST_QCACHE_ON_WITH_OTHER_CONN:
    case TEST_QCACHE_ON:                 /* should have hit */
      FAIL_UNLESS(hits2-hits1 == 1, "hits2 != hits1 + 1");
      break;
    case TEST_QCACHE_OFF_ON:
    case TEST_QCACHE_ON_OFF:             /* should not have hit */
      FAIL_UNLESS(hits2-hits1 == 0, "hits2 != hits1");
      break;
    }

    /* now modify parameter values and see qcache hits */
    strcpy(p_str_data, "ii");
    p_str_length= (unsigned long)strlen(p_str_data);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    test_ps_query_cache_result(1, "hh", 2, 1, "ii", 2, 2, "ii", 2);
    hits1= query_cache_hits(lmysql);

    switch(iteration) {
    case TEST_QCACHE_ON:
    case TEST_QCACHE_OFF_ON:
    case TEST_QCACHE_ON_OFF:             /* should not have hit */
      FAIL_UNLESS(hits2-hits1 == 0, "hits2 != hits1");
      break;
    case TEST_QCACHE_ON_WITH_OTHER_CONN: /* should have hit */
      FAIL_UNLESS(hits1-hits2 == 1, "hits2 != hits1+1");
      break;
    }

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    test_ps_query_cache_result(1, "hh", 2, 1, "ii", 2, 2, "ii", 2);
    hits2= query_cache_hits(lmysql);

    mysql_stmt_close(stmt);

    switch(iteration) {
    case TEST_QCACHE_ON:                 /* should have hit */
      FAIL_UNLESS(hits2-hits1 == 1, "hits2 != hits1+1");
      break;
    case TEST_QCACHE_OFF_ON:
    case TEST_QCACHE_ON_OFF:             /* should not have hit */
      FAIL_UNLESS(hits2-hits1 == 0, "hits2 != hits1");
      break;
    case TEST_QCACHE_ON_WITH_OTHER_CONN: /* should have hit */
      FAIL_UNLESS(hits2-hits1 == 1, "hits2 != hits1+1");
      break;
    }

  } /* for(iteration=...) */

  if (lmysql != mysql)
    mysql_close(lmysql);

  rc= mysql_query(mysql, "set global query_cache_size=0");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug3117(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND buffer;
  longlong lii;
  ulong length;
  my_bool is_null;
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (id int auto_increment primary key)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT LAST_INSERT_ID()"));
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (NULL)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  memset(&buffer, '\0', sizeof(buffer));
  buffer.buffer_type= MYSQL_TYPE_LONGLONG;
  buffer.buffer_length= sizeof(lii);
  buffer.buffer= (void *)&lii;
  buffer.length= &length;
  buffer.is_null= &is_null;

  rc= mysql_stmt_bind_result(stmt, &buffer);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(is_null == 0 && lii == 1, "is_null != 0 || lii != 1");

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (NULL)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(is_null == 0 && lii == 2, "is_null != 0 || lii != 2");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/**
  Bug#36004 mysql_stmt_prepare resets the list of warnings
*/

static int test_bug36004(MYSQL *mysql)
{
  int rc, warning_count= 0;
  MYSQL_STMT *stmt;


  if (mysql_get_server_version(mysql) < 60000) {
    diag("Test requires MySQL Server version 6.0 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "drop table if exists inexistant");
  check_mysql_rc(rc, mysql);

  FAIL_UNLESS(mysql_warning_count(mysql) == 1, "");
  query_int_variable(mysql, "@@warning_count", &warning_count);
  FAIL_UNLESS(warning_count, "Warning expected");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select 1"));
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(mysql_warning_count(mysql) == 0, "No warning expected");
  query_int_variable(mysql, "@@warning_count", &warning_count);
  FAIL_UNLESS(warning_count, "warning expected");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(mysql_warning_count(mysql) == 0, "No warning expected");
  mysql_stmt_close(stmt);

  query_int_variable(mysql, "@@warning_count", &warning_count);
  FAIL_UNLESS(warning_count, "Warning expected");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("drop table if exists inexistant"));
  check_stmt_rc(rc, stmt);

  query_int_variable(mysql, "@@warning_count", &warning_count);
  FAIL_UNLESS(warning_count == 0, "No warning expected");
  mysql_stmt_close(stmt);

  return OK;
}

static int test_bug3796(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  const char *concat_arg0= "concat_with_";
  enum { OUT_BUFF_SIZE= 30 };
  char out_buff[OUT_BUFF_SIZE];
  char canonical_buff[OUT_BUFF_SIZE];
  ulong out_length;
  const char *stmt_text;
  int rc;


  /* Create and fill test table */
  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "CREATE TABLE t1 (a INT, b VARCHAR(30))";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "INSERT INTO t1 VALUES(1, 'ONE'), (2, 'TWO')";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  /* Create statement handle and prepare it with select */
  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT concat(?, b) FROM t1";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  /* Bind input buffers */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *) concat_arg0;
  my_bind[0].buffer_length= (unsigned long)strlen(concat_arg0);

  mysql_stmt_bind_param(stmt, my_bind);

  /* Execute the select statement */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  my_bind[0].buffer= (void *) out_buff;
  my_bind[0].buffer_length= OUT_BUFF_SIZE;
  my_bind[0].length= &out_length;

  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  strcpy(canonical_buff, concat_arg0);
  strcat(canonical_buff, "ONE");
  FAIL_UNLESS(strlen(canonical_buff) == out_length &&
         strncmp(out_buff, canonical_buff, out_length) == 0, "");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  strcpy(canonical_buff + strlen(concat_arg0), "TWO");
  FAIL_UNLESS(strlen(canonical_buff) == out_length &&
         strncmp(out_buff, canonical_buff, out_length) == 0, "");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);

  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug4026(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  MYSQL_TIME time_in, time_out;
  MYSQL_TIME datetime_in, datetime_out;
  const char *stmt_text;
  int rc;


  /* Check that microseconds are inserted and selected successfully */

  /* Create a statement handle and prepare it with select */
  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT ?, ?";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  /* Bind input buffers */
  memset(my_bind, '\0', sizeof(MYSQL_BIND) * 2);
  memset(&time_in, '\0', sizeof(MYSQL_TIME));
  memset(&time_out, '\0', sizeof(MYSQL_TIME));
  memset(&datetime_in, '\0', sizeof(MYSQL_TIME));
  memset(&datetime_out, '\0', sizeof(MYSQL_TIME));
  my_bind[0].buffer_type= MYSQL_TYPE_TIME;
  my_bind[0].buffer= (void *) &time_in;
  my_bind[1].buffer_type= MYSQL_TYPE_DATETIME;
  my_bind[1].buffer= (void *) &datetime_in;

  time_in.hour= 23;
  time_in.minute= 59;
  time_in.second= 59;
  time_in.second_part= 123456;
  /*
    This is not necessary, just to make DIE_UNLESS below work: this field
    is filled in when time is received from server
  */
  time_in.time_type= MYSQL_TIMESTAMP_TIME;

  datetime_in= time_in;
  datetime_in.year= 2003;
  datetime_in.month= 12;
  datetime_in.day= 31;
  datetime_in.time_type= MYSQL_TIMESTAMP_DATETIME;

  mysql_stmt_bind_param(stmt, my_bind);

  /* Execute the select statement */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  my_bind[0].buffer= (void *) &time_out;
  my_bind[1].buffer= (void *) &datetime_out;

  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 0, "rc != 0");
  FAIL_UNLESS(memcmp(&time_in, &time_out, sizeof(time_in)) == 0, "time_in != time_out");
  FAIL_UNLESS(memcmp(&datetime_in, &datetime_out, sizeof(datetime_in)) == 0, "datetime_in != datetime_out");
  mysql_stmt_close(stmt);

  return OK;
}

static int test_bug4030(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[3];
  MYSQL_TIME time_canonical, time_out;
  MYSQL_TIME date_canonical, date_out;
  MYSQL_TIME datetime_canonical, datetime_out;
  const char *stmt_text;
  int rc;


  /* Check that microseconds are inserted and selected successfully */

  /* Execute a query with time values in prepared mode */
  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT '23:59:59.123456', '2003-12-31', "
             "'2003-12-31 23:59:59.123456'";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* Bind output buffers */
  memset(my_bind, '\0', sizeof(my_bind));
  memset(&time_canonical, '\0', sizeof(time_canonical));
  memset(&time_out, '\0', sizeof(time_out));
  memset(&date_canonical, '\0', sizeof(date_canonical));
  memset(&date_out, '\0', sizeof(date_out));
  memset(&datetime_canonical, '\0', sizeof(datetime_canonical));
  memset(&datetime_out, '\0', sizeof(datetime_out));
  my_bind[0].buffer_type= MYSQL_TYPE_TIME;
  my_bind[0].buffer= (void *) &time_out;
  my_bind[1].buffer_type= MYSQL_TYPE_DATE;
  my_bind[1].buffer= (void *) &date_out;
  my_bind[2].buffer_type= MYSQL_TYPE_DATETIME;
  my_bind[2].buffer= (void *) &datetime_out;

  time_canonical.hour= 23;
  time_canonical.minute= 59;
  time_canonical.second= 59;
  time_canonical.second_part= 123456;
  time_canonical.time_type= MYSQL_TIMESTAMP_TIME;

  date_canonical.year= 2003;
  date_canonical.month= 12;
  date_canonical.day= 31;
  date_canonical.time_type= MYSQL_TIMESTAMP_DATE;

  datetime_canonical= time_canonical;
  datetime_canonical.year= 2003;
  datetime_canonical.month= 12;
  datetime_canonical.day= 31;
  datetime_canonical.time_type= MYSQL_TIMESTAMP_DATETIME;

  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 0, "rc != 0");
  FAIL_UNLESS(memcmp(&time_canonical, &time_out, sizeof(time_out)) == 0, "time_canonical != time_out");
  FAIL_UNLESS(memcmp(&date_canonical, &date_out, sizeof(date_out)) == 0, "date_canoncical != date_out");
  FAIL_UNLESS(memcmp(&datetime_canonical, &datetime_out, sizeof(datetime_out)) == 0, "datetime_canonical != datetime_out");
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug4079(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  const char *stmt_text;
  uint32 res;
  int rc;

  /* Create and fill table */
  mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  mysql_query(mysql, "CREATE TABLE t1 (a int)");
  mysql_query(mysql, "INSERT INTO t1 VALUES (1), (2)");

  /* Prepare erroneous statement */
  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT 1 < (SELECT a FROM t1)";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  /* Execute the select statement */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* Bind input buffers */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *) &res;

  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 1, "rc != 1");
  /* buggy version of libmysql hanged up here */
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug4172(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[3];
  const char *stmt_text;
  MYSQL_RES *res;
  MYSQL_ROW row;
  int rc;
  char f[100], d[100], e[100];
  ulong f_len, d_len, e_len;

  mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  mysql_query(mysql, "CREATE TABLE t1 (f float, d double, e decimal(10,4))");
  mysql_query(mysql, "INSERT INTO t1 VALUES (12345.1234, 123456.123456, "
                                            "123456.1234)");

  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT f, d, e FROM t1";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  memset(my_bind, '\0', sizeof(my_bind));  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= f;
  my_bind[0].buffer_length= sizeof(f);
  my_bind[0].length= &f_len;
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= d;
  my_bind[1].buffer_length= sizeof(d);
  my_bind[1].length= &d_len;
  my_bind[2].buffer_type= MYSQL_TYPE_STRING;
  my_bind[2].buffer= e;
  my_bind[2].buffer_length= sizeof(e);
  my_bind[2].length= &e_len;

  mysql_stmt_bind_result(stmt, my_bind);

  mysql_stmt_store_result(stmt);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);

  diag("expected %s %s %s", row[0], row[1], row[2]);
  diag("fetched %s %s %s", f, d, e);
  FAIL_UNLESS(!strcmp(f, row[0]) && !strcmp(d, row[1]) && !strcmp(e, row[2]), "");

  mysql_free_result(res);
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug4231(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  MYSQL_TIME tm[2];
  const char *stmt_text;
  int rc;


  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "CREATE TABLE t1 (a int)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "INSERT INTO t1 VALUES (1)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT a FROM t1 WHERE ? = ?";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  /* Bind input buffers */
  memset(my_bind, '\0', sizeof(my_bind));  memset(tm, '\0', sizeof(tm));
  my_bind[0].buffer_type= MYSQL_TYPE_DATE;
  my_bind[0].buffer= &tm[0];
  my_bind[1].buffer_type= MYSQL_TYPE_DATE;
  my_bind[1].buffer= &tm[1];

  mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  /*
    First set server-side params to some non-zero non-equal values:
    then we will check that they are not used when client sends
    new (zero) times.
  */
  tm[0].time_type = MYSQL_TIMESTAMP_DATE;
  tm[0].year = 2000;
  tm[0].month = 1;
  tm[0].day = 1;
  tm[1]= tm[0];
  --tm[1].year;                                 /* tm[0] != tm[1] */

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);

  /* binds are unequal, no rows should be returned */
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* Set one of the dates to zero */
  tm[0].year= tm[0].month= tm[0].day= 0;
  tm[1]= tm[0];
  mysql_stmt_execute(stmt);
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 0, "rc != 0");

  mysql_stmt_close(stmt);
  stmt_text= "DROP TABLE t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug4236(MYSQL *mysql)
{
  MYSQL_STMT *stmt, *stmt1;
  const char *stmt_text;
  int rc;
  MYSQL_STMT backup;
  MYSQL      *mysql1;


  stmt= mysql_stmt_init(mysql);

  /* mysql_stmt_execute() of statement with statement id= 0 crashed server */
  stmt_text= "SELECT 1";
  /* We need to prepare statement to pass by possible check in libmysql */
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);  /* Hack to check that server works OK if statement wasn't found */
  backup.stmt_id= stmt->stmt_id;
  stmt->stmt_id= 0;
  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected");

  /* lets try to hack with a new connection */
  mysql1= test_connect(NULL);
  stmt1= mysql_stmt_init(mysql1);
  stmt_text= "SELECT 2";
  rc= mysql_stmt_prepare(stmt1, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  stmt->stmt_id= stmt1->stmt_id;
  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt1);
  mysql_close(mysql1);

  /* Restore original statement id to be able to reprepare it */
  stmt->stmt_id= backup.stmt_id;

  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug5126(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  int32 c1, c2;
  const char *stmt_text;
  int rc;


  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "CREATE TABLE t1 (a mediumint, b int)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "INSERT INTO t1 VALUES (8386608, 1)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT a, b FROM t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* Bind output buffers */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= &c1;
  my_bind[1].buffer_type= MYSQL_TYPE_LONG;
  my_bind[1].buffer= &c2;

  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 0, "rc != 0");
  FAIL_UNLESS(c1 == 8386608 && c2 == 1, "c1 != 8386608 || c2 != 1");
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug5194(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND *my_bind;
  char *query;
  char *param_str;
  int param_str_length;
  const char *stmt_text;
  int rc;
  float float_array[250] =
  {
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,  0.5,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,
    0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25,  0.25
  };
  float *fa_ptr= float_array;
  /* Number of columns per row */
  const int COLUMN_COUNT= sizeof(float_array)/sizeof(*float_array);
  /* Number of rows per bulk insert to start with */
  const int MIN_ROWS_PER_INSERT= 262;
  /* Max number of rows per bulk insert to end with */
  const int MAX_ROWS_PER_INSERT= 300;
  const int MAX_PARAM_COUNT= COLUMN_COUNT*MAX_ROWS_PER_INSERT;
  const char *query_template= "insert into t1 values %s";
  const int CHARS_PER_PARAM= 5; /* space needed to place ", ?" in the query */
  const int uint16_max= 65535;
  int nrows, i;


  stmt_text= "drop table if exists t1";
  rc= mysql_real_query(mysql, SL(stmt_text));

  stmt_text= "create table if not exists t1"
   "(c1 float, c2 float, c3 float, c4 float, c5 float, c6 float, "
   "c7 float, c8 float, c9 float, c10 float, c11 float, c12 float, "
   "c13 float, c14 float, c15 float, c16 float, c17 float, c18 float, "
   "c19 float, c20 float, c21 float, c22 float, c23 float, c24 float, "
   "c25 float, c26 float, c27 float, c28 float, c29 float, c30 float, "
   "c31 float, c32 float, c33 float, c34 float, c35 float, c36 float, "
   "c37 float, c38 float, c39 float, c40 float, c41 float, c42 float, "
   "c43 float, c44 float, c45 float, c46 float, c47 float, c48 float, "
   "c49 float, c50 float, c51 float, c52 float, c53 float, c54 float, "
   "c55 float, c56 float, c57 float, c58 float, c59 float, c60 float, "
   "c61 float, c62 float, c63 float, c64 float, c65 float, c66 float, "
   "c67 float, c68 float, c69 float, c70 float, c71 float, c72 float, "
   "c73 float, c74 float, c75 float, c76 float, c77 float, c78 float, "
   "c79 float, c80 float, c81 float, c82 float, c83 float, c84 float, "
   "c85 float, c86 float, c87 float, c88 float, c89 float, c90 float, "
   "c91 float, c92 float, c93 float, c94 float, c95 float, c96 float, "
   "c97 float, c98 float, c99 float, c100 float, c101 float, c102 float, "
   "c103 float, c104 float, c105 float, c106 float, c107 float, c108 float, "
   "c109 float, c110 float, c111 float, c112 float, c113 float, c114 float, "
   "c115 float, c116 float, c117 float, c118 float, c119 float, c120 float, "
   "c121 float, c122 float, c123 float, c124 float, c125 float, c126 float, "
   "c127 float, c128 float, c129 float, c130 float, c131 float, c132 float, "
   "c133 float, c134 float, c135 float, c136 float, c137 float, c138 float, "
   "c139 float, c140 float, c141 float, c142 float, c143 float, c144 float, "
   "c145 float, c146 float, c147 float, c148 float, c149 float, c150 float, "
   "c151 float, c152 float, c153 float, c154 float, c155 float, c156 float, "
   "c157 float, c158 float, c159 float, c160 float, c161 float, c162 float, "
   "c163 float, c164 float, c165 float, c166 float, c167 float, c168 float, "
   "c169 float, c170 float, c171 float, c172 float, c173 float, c174 float, "
   "c175 float, c176 float, c177 float, c178 float, c179 float, c180 float, "
   "c181 float, c182 float, c183 float, c184 float, c185 float, c186 float, "
   "c187 float, c188 float, c189 float, c190 float, c191 float, c192 float, "
   "c193 float, c194 float, c195 float, c196 float, c197 float, c198 float, "
   "c199 float, c200 float, c201 float, c202 float, c203 float, c204 float, "
   "c205 float, c206 float, c207 float, c208 float, c209 float, c210 float, "
   "c211 float, c212 float, c213 float, c214 float, c215 float, c216 float, "
   "c217 float, c218 float, c219 float, c220 float, c221 float, c222 float, "
   "c223 float, c224 float, c225 float, c226 float, c227 float, c228 float, "
   "c229 float, c230 float, c231 float, c232 float, c233 float, c234 float, "
   "c235 float, c236 float, c237 float, c238 float, c239 float, c240 float, "
   "c241 float, c242 float, c243 float, c244 float, c245 float, c246 float, "
   "c247 float, c248 float, c249 float, c250 float)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  my_bind= (MYSQL_BIND*) malloc(MAX_PARAM_COUNT * sizeof(MYSQL_BIND));
  query= (char*) malloc(strlen(query_template) +
                        MAX_PARAM_COUNT * CHARS_PER_PARAM + 1);
  param_str= (char*) malloc(COLUMN_COUNT * CHARS_PER_PARAM);

  FAIL_IF(my_bind == 0 || query == 0 || param_str == 0, "Not enough memory")

  stmt= mysql_stmt_init(mysql);

  /* setup a template for one row of parameters */
  sprintf(param_str, "(");
  for (i= 1; i < COLUMN_COUNT; ++i)
    strcat(param_str, "?, ");
  strcat(param_str, "?)");
  param_str_length= (int)strlen(param_str);

  /* setup bind array */
  memset(my_bind, '\0', MAX_PARAM_COUNT * sizeof(MYSQL_BIND));
  for (i= 0; i < MAX_PARAM_COUNT; ++i)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_FLOAT;
    my_bind[i].buffer= fa_ptr;
    if (++fa_ptr == float_array + COLUMN_COUNT)
      fa_ptr= float_array;
  }

  /*
    Test each number of rows per bulk insert, so that we can see where
    MySQL fails.
  */
  for (nrows= MIN_ROWS_PER_INSERT; nrows <= MAX_ROWS_PER_INSERT; ++nrows)
  {
    char *query_ptr;
    /* Create statement text for current number of rows */
    sprintf(query, query_template, param_str);
    query_ptr= query + (unsigned long)strlen(query);
    for (i= 1; i < nrows; ++i)
    {
      memcpy(query_ptr, ", ", 2);
      query_ptr+= 2;
      memcpy(query_ptr, param_str, param_str_length);
      query_ptr+= param_str_length;
    }
    *query_ptr= '\0';

    rc= mysql_stmt_prepare(stmt, query, (ulong)(query_ptr - query));

    if (rc && nrows * COLUMN_COUNT > uint16_max) /* expected error */
      break;

    check_stmt_rc(rc, stmt);

    /* bind the parameter array and execute the query */
    rc= mysql_stmt_bind_param(stmt, my_bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_reset(stmt);
  }

  free(param_str);
  free(query);
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);
  free(my_bind);
  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug5315(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  int rc;

  if (!is_mariadb)
    return SKIP;

  stmt_text= "SELECT 1";
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_change_user(mysql, username, password, schema);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  FAIL_UNLESS(rc != 0, "Error expected");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug5399(MYSQL *mysql)
{
  /*
    Ascii 97 is 'a', which gets mapped to Ascii 65 'A' unless internal
    statement id hash in the server uses binary collation.
  */
#define NUM_OF_USED_STMT 97
  MYSQL_STMT *stmt_list[NUM_OF_USED_STMT];
  MYSQL_STMT **stmt;
  MYSQL_BIND my_bind[1];
  char buff[600];
  int rc;
  int32 no;


  memset(my_bind, '\0', sizeof(my_bind));  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= &no;

  for (stmt= stmt_list; stmt != stmt_list + NUM_OF_USED_STMT; ++stmt)
  {
    sprintf(buff, "select %d", (int) (stmt - stmt_list));
    *stmt= mysql_stmt_init(mysql);
    rc= mysql_stmt_prepare(*stmt, SL(buff));
    check_stmt_rc(rc, *stmt);    mysql_stmt_bind_result(*stmt, my_bind);
  }

  for (stmt= stmt_list; stmt != stmt_list + NUM_OF_USED_STMT; ++stmt)
  {
    rc= mysql_stmt_execute(*stmt);
    check_stmt_rc(rc, *stmt);
    rc= mysql_stmt_store_result(*stmt);
    check_stmt_rc(rc, *stmt);
    rc= mysql_stmt_fetch(*stmt);
    FAIL_UNLESS((int32) (stmt - stmt_list) == no, "");
  }

  for (stmt= stmt_list; stmt != stmt_list + NUM_OF_USED_STMT; ++stmt)
    mysql_stmt_close(*stmt);
#undef NUM_OF_USED_STMT
  return OK;
}

static int test_bug6046(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  int rc;
  short b= 1;
  MYSQL_BIND my_bind[1];


  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "CREATE TABLE t1 (a int, b int)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "INSERT INTO t1 VALUES (1,1),(2,2),(3,1),(4,2)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);

  stmt_text= "SELECT t1.a FROM t1 NATURAL JOIN t1 as X1 "
             "WHERE t1.b > ? ORDER BY t1.a";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  b= 1;
  memset(my_bind, '\0', sizeof(my_bind));  my_bind[0].buffer= &b;
  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;

  mysql_stmt_bind_param(stmt, my_bind);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_store_result(stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug6049(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  MYSQL_RES *res;
  MYSQL_ROW row;
  const char *stmt_text;
  char buffer[30];
  ulong length;
  int rc;


  stmt_text= "SELECT MAKETIME(-25, 12, 12)";

  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type    = MYSQL_TYPE_STRING;
  my_bind[0].buffer         = &buffer;
  my_bind[0].buffer_length  = sizeof(buffer);
  my_bind[0].length         = &length;

  mysql_stmt_bind_result(stmt, my_bind);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(row[0], (char*) buffer) == 0, "row[0] != buffer");

  mysql_free_result(res);
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug6058(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  MYSQL_RES *res;
  MYSQL_ROW row;
  const char *stmt_text;
  char buffer[30];
  ulong length;
  int rc;


  stmt_text= "SELECT CAST('0000-00-00' AS DATE)";

  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type    = MYSQL_TYPE_STRING;
  my_bind[0].buffer         = &buffer;
  my_bind[0].buffer_length  = sizeof(buffer);
  my_bind[0].length         = &length;

  mysql_stmt_bind_result(stmt, my_bind);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(row[0], buffer) == 0, "row[0] != buffer");

  mysql_free_result(res);
  mysql_stmt_close(stmt);
  return OK;
}


static int test_bug6059(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  int rc;

  stmt_text= "SELECT 'foo' INTO OUTFILE 'x.3'";

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(mysql_stmt_field_count(stmt) == 0, "");
  mysql_stmt_close(stmt);
  return OK;
}

static int test_bug6096(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES *query_result, *stmt_metadata;
  const char *stmt_text;
  MYSQL_BIND my_bind[12];
  MYSQL_FIELD *query_field_list, *stmt_field_list;
  ulong query_field_count, stmt_field_count;
  int rc;
  my_bool update_max_length= TRUE;
  uint i;


  stmt_text= "drop table if exists t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  mysql_query(mysql, "set sql_mode=''");
  stmt_text= "create table t1 (c_tinyint tinyint, c_smallint smallint, "
                             " c_mediumint mediumint, c_int int, "
                             " c_bigint bigint, c_float float, "
                             " c_double double, c_varchar varchar(20), "
                             " c_char char(20), c_time time, c_date date, "
                             " c_datetime datetime)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "insert into t1  values (-100, -20000, 30000000, 4, 8, 1.0, "
                                     "2.0, 'abc', 'def', now(), now(), now())";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "select * from t1";

  /* Run select in prepared and non-prepared mode and compare metadata */
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  query_result= mysql_store_result(mysql);
  query_field_list= mysql_fetch_fields(query_result);
  FAIL_IF(!query_field_list, "fetch_fields failed");
  query_field_count= mysql_num_fields(query_result);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);  mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH,
                      (void*) &update_max_length);
  mysql_stmt_store_result(stmt);
  stmt_metadata= mysql_stmt_result_metadata(stmt);
  stmt_field_list= mysql_fetch_fields(stmt_metadata);
  stmt_field_count= mysql_num_fields(stmt_metadata);
  FAIL_UNLESS(stmt_field_count == query_field_count, "");


  /* Bind and fetch the data */

  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < stmt_field_count; ++i)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_STRING;
    my_bind[i].buffer_length= stmt_field_list[i].max_length + 1;
    my_bind[i].buffer= malloc(my_bind[i].buffer_length);
  }
  mysql_stmt_bind_result(stmt, my_bind);
  rc= mysql_stmt_fetch(stmt);
  diag("rc=%d", rc);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* Clean up */

  for (i= 0; i < stmt_field_count; ++i)
    free(my_bind[i].buffer);
  mysql_stmt_close(stmt);
  mysql_free_result(query_result);
  mysql_free_result(stmt_metadata);
  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Bug#7990 - mysql_stmt_close doesn't reset mysql->net.last_error */

static int test_bug7990(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, "foo", 3);
  /*
    XXX: the fact that we store errno both in STMT and in
    MYSQL is not documented and is subject to change in 5.0
  */
  FAIL_UNLESS(rc && mysql_stmt_errno(stmt) && mysql_errno(mysql), "Error expected");
  mysql_stmt_close(stmt);
  FAIL_UNLESS(!mysql_errno(mysql), "errno != 0");
  return OK;
}

/* Bug#8330 - mysql_stmt_execute crashes (libmysql) */

static int test_bug8330(MYSQL *mysql)
{
  const char *stmt_text;
  MYSQL_STMT *stmt[2];
  int i, rc;
  const char *query= "select a,b from t1 where a=?";
  MYSQL_BIND my_bind[2];
  long lval[2]= {1,2};

  stmt_text= "drop table if exists t1";
  /* in case some previos test failed */
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "create table t1 (a int, b int)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  memset(my_bind, '\0', sizeof(my_bind));
  for (i=0; i < 2; i++)
  {
    stmt[i]= mysql_stmt_init(mysql);
    rc= mysql_stmt_prepare(stmt[i], SL(query));
    check_stmt_rc(rc, stmt[i]);
    my_bind[i].buffer_type= MYSQL_TYPE_LONG;
    my_bind[i].buffer= (void*) &lval[i];
    my_bind[i].is_null= 0;
    mysql_stmt_bind_param(stmt[i], &my_bind[i]);
  }

  rc= mysql_stmt_execute(stmt[0]);
  check_stmt_rc(rc, stmt[0]);
  rc= mysql_stmt_execute(stmt[1]);
  FAIL_UNLESS(rc && mysql_stmt_errno(stmt[1]) == CR_COMMANDS_OUT_OF_SYNC, "Error expected");
  rc= mysql_stmt_execute(stmt[0]);
  check_stmt_rc(rc, stmt[0]);
  mysql_stmt_close(stmt[0]);
  mysql_stmt_close(stmt[1]);

  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test misc field information, bug: #74 */

static int test_field_misc(MYSQL *mysql)
{
  MYSQL_STMT  *stmt;
  MYSQL_RES   *result;
  int         rc;


  rc= mysql_query(mysql, "SELECT @@autocommit");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  verify_prepare_field(result, 0,
                       "@@autocommit", "",  /* field and its org name */
                       MYSQL_TYPE_LONGLONG, /* field type */
                       "", "",              /* table and its org name */
                       "", 1, 0);           /* db name, length(its bool flag)*/

  mysql_free_result(result);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT @@autocommit"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  verify_prepare_field(result, 0,
                       "@@autocommit", "",  /* field and its org name */
                       MYSQL_TYPE_LONGLONG, /* field type */
                       "", "",              /* table and its org name */
                       "", 1, 0);           /* db name, length(its bool flag)*/

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT @@max_error_count"));
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  if (verify_prepare_field(result, 0,
                       "@@max_error_count", "",   /* field and its org name */
                       MYSQL_TYPE_LONGLONG, /* field type */
                       "", "",              /* table and its org name */
                       /* db name, length */
                       "", MY_INT64_NUM_DECIMAL_DIGITS , 0))
    goto error;

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT @@max_allowed_packet"));
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  if (verify_prepare_field(result, 0,
                       "@@max_allowed_packet", "", /* field and its org name */
                       MYSQL_TYPE_LONGLONG, /* field type */
                       "", "",              /* table and its org name */
                       /* db name, length */
                       "", MY_INT64_NUM_DECIMAL_DIGITS, 0))
    goto error;

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT @@sql_warnings"));
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  if (verify_prepare_field(result, 0,
                       "@@sql_warnings", "",  /* field and its org name */
                       MYSQL_TYPE_LONGLONG,   /* field type */
                       "", "",                /* table and its org name */
                       "", 1, 0))             /* db name, length */
    goto error;

  mysql_free_result(result);
  mysql_stmt_close(stmt);
  return OK;

error:
  mysql_free_result(result);
  mysql_stmt_close(stmt);
  return FAIL;
}

/* Test a memory ovverun bug */

static int test_mem_overun(MYSQL *mysql)
{
  char       buffer[10000], field[12];
  MYSQL_STMT *stmt;
  MYSQL_RES  *field_res, *res;
  int        rc, i, length;

  /*
    Test a memory ovverun bug when a table had 1000 fields with
    a row of data
  */
  rc= mysql_query(mysql, "drop table if exists t_mem_overun");
  check_mysql_rc(rc, mysql);

  strcpy(buffer, "create table t_mem_overun(");
  for (i= 0; i < 1000; i++)
  {
    sprintf(field, "c%d int, ", i);
    strcat(buffer, field);
  }
  length= (int)strlen(buffer);
  buffer[length-2]= ')';
  buffer[--length]= '\0';

  rc= mysql_real_query(mysql, buffer, length);
  check_mysql_rc(rc, mysql);

  strcpy(buffer, "insert into t_mem_overun values(");
  for (i= 0; i < 1000; i++)
  {
    strcat(buffer, "1, ");
  }
  length= (int)strlen(buffer);
  buffer[length-2]= ')';
  buffer[--length]= '\0';

  rc= mysql_real_query(mysql, buffer, length);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "select * from t_mem_overun");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  rc= 0;
  while (mysql_fetch_row(res))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");
  mysql_free_result(res);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select * from t_mem_overun"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  field_res= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!field_res, "Invalid result set");

  FAIL_UNLESS( 1000 == mysql_num_fields(field_res), "fields != 1000");

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "");

  mysql_free_result(field_res);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table if exists t_mem_overun");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug8722(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmt_text;

  /* Prepare test data */
  stmt_text= "drop table if exists t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "drop view if exists v1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "CREATE TABLE t1 (c1 varchar(10), c2 varchar(10), c3 varchar(10),"
                             " c4 varchar(10), c5 varchar(10), c6 varchar(10),"
                             " c7 varchar(10), c8 varchar(10), c9 varchar(10),"
                             "c10 varchar(10))";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "INSERT INTO t1 VALUES (1,2,3,4,5,6,7,8,9,10)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "CREATE VIEW v1 AS SELECT * FROM t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  stmt_text= "select * from v1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);
  stmt_text= "drop table if exists t1, v1";
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test DECIMAL conversion */

static int test_decimal_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char       data[30];
  int        rc;
  my_bool    is_null;

  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "drop table if exists test_decimal_bug");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table test_decimal_bug(c1 decimal(10, 2))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into test_decimal_bug value(8), (10.22), (5.61)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select c1 from test_decimal_bug where c1=?"));
  check_stmt_rc(rc, stmt);

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  memset(data, 0, sizeof(data));
  my_bind[0].buffer_type= MYSQL_TYPE_NEWDECIMAL;
  my_bind[0].buffer= (void *)data;
  my_bind[0].buffer_length= 25;
  my_bind[0].is_null= &is_null;

  is_null= 0;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  strcpy(data, "8.0");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  data[0]= 0;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(data, "8.00") == 0, "data != '8.00'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  strcpy(data, "5.61");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  data[0]= 0;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(data, "5.61") == 0, "data != '5.61'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  is_null= 1;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  strcpy(data, "10.22"); is_null= 0;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  data[0]= 0;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(data, "10.22") == 0, "data != '10.22'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table if exists test_decimal_bug");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test EXPLAIN bug (#115, reported by mark@mysql.com & georg@php.net). */

static int test_explain_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES  *result;
  int        rc;

  if (!is_mariadb)
    return SKIP;

  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_explain");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_explain(id int, name char(2))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("explain test_explain"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 2, "rowcount != 2");

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  FAIL_UNLESS(6 == mysql_num_fields(result), "fields != 6");

  if (verify_prepare_field(result, 0, "Field", "COLUMN_NAME",
                       mysql_get_server_version(mysql) <= 50000 ?
                       MYSQL_TYPE_STRING : MYSQL_TYPE_VAR_STRING,
                       0, 0,
                       mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                       64, 0))
    goto error;

  if (verify_prepare_field(result, 1, "Type", "COLUMN_TYPE", MYSQL_TYPE_BLOB,
                       0, 0,
                       mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                       0, 0))
    goto error;

  if (verify_prepare_field(result, 2, "Null", "IS_NULLABLE",
                       mysql_get_server_version(mysql) <= 50000 ?
                       MYSQL_TYPE_STRING : MYSQL_TYPE_VAR_STRING,
                       0, 0,
                       mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                       3, 0))
    goto error;

  if (verify_prepare_field(result, 3, "Key", "COLUMN_KEY",
                       mysql_get_server_version(mysql) <= 50000 ?
                       MYSQL_TYPE_STRING : MYSQL_TYPE_VAR_STRING,
                       0, 0,
                       mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                       3, 0))
    goto error;

  if ( mysql_get_server_version(mysql) >= 50027 )
  {
    /*  The patch for bug#23037 changes column type of DEAULT to blob */
    if (verify_prepare_field(result, 4, "Default", "COLUMN_DEFAULT",
                         MYSQL_TYPE_BLOB, 0, 0,
                         mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                         0, 0))
      goto error;
  }
  else
  {
    if (verify_prepare_field(result, 4, "Default", "COLUMN_DEFAULT",
                         mysql_get_server_version(mysql) >= 50027 ?
                         MYSQL_TYPE_BLOB :
                         mysql_get_server_version(mysql) <= 50000 ?
                         MYSQL_TYPE_STRING : MYSQL_TYPE_VAR_STRING,
                         0, 0,
                         mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                         mysql_get_server_version(mysql) >= 50027 ? 0 :64, 0))
      goto error;
  }

  if (verify_prepare_field(result, 5, "Extra", "EXTRA",
                       mysql_get_server_version(mysql) <= 50000 ?
                       MYSQL_TYPE_STRING : MYSQL_TYPE_VAR_STRING,
                       0, 0,
                       mysql_get_server_version(mysql) <= 50400 ? "" : "information_schema",
                       27, 0))
    goto error;

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("explain select id, name FROM test_explain"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid result set");

  FAIL_UNLESS(10 == mysql_num_fields(result), "fields != 10");

  if (verify_prepare_field(result, 0, "id", "", MYSQL_TYPE_LONGLONG, "", "", "", 3, 0))
    goto error;

  if (verify_prepare_field(result, 1, "select_type", "", MYSQL_TYPE_VAR_STRING, "", "", "", 19, 0))
    goto error;

  if (verify_prepare_field(result, 2, "table", "", MYSQL_TYPE_VAR_STRING, "", "", "", NAME_CHAR_LEN, 0))
    goto error;

  if (verify_prepare_field(result, 3, "type", "", MYSQL_TYPE_VAR_STRING, "", "", "", 10, 0))
    goto error;

  if (verify_prepare_field(result, 4, "possible_keys", "", MYSQL_TYPE_VAR_STRING, "", "", "", NAME_CHAR_LEN*MAX_KEY, 0))
    goto error;

  if ( verify_prepare_field(result, 5, "key", "", MYSQL_TYPE_VAR_STRING, "", "", "", NAME_CHAR_LEN, 0))
    goto error;

  if (mysql_get_server_version(mysql) <= 50000)
  {
    if (verify_prepare_field(result, 6, "key_len", "", MYSQL_TYPE_LONGLONG, "", "", "", 3, 0))
      goto error;
  }
  else if (mysql_get_server_version(mysql) <= 60000)
  {
    if (verify_prepare_field(result, 6, "key_len", "", MYSQL_TYPE_VAR_STRING, "", "", "", NAME_CHAR_LEN*MAX_KEY, 0))
      goto error;
  }
  else
  {
    if (verify_prepare_field(result, 6, "key_len", "", MYSQL_TYPE_VAR_STRING, "", "", "", (MAX_KEY_LENGTH_DECIMAL_WIDTH + 1) * MAX_KEY, 0))
    goto error;
  }

  if (verify_prepare_field(result, 7, "ref", "", MYSQL_TYPE_VAR_STRING, "", "", "",
                           NAME_CHAR_LEN*16, 0))
    goto error;

  if (verify_prepare_field(result, 8, "rows", "", MYSQL_TYPE_LONGLONG, "", "", "", 10, 0))
    goto error;

  if (verify_prepare_field(result, 9, "Extra", "", MYSQL_TYPE_VAR_STRING, "", "", "", 255, 0))
    goto error;

  mysql_free_result(result);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_explain");
  check_mysql_rc(rc, mysql);
  return OK;
error:
  mysql_free_result(result);
  mysql_stmt_close(stmt);
  return FAIL;
}

static int test_sshort_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[4];
  short      short_value;
  int32      long_value;
  ulong      s_length, l_length, ll_length, t_length;
  ulonglong  longlong_value;
  int        rc;
  uchar      tiny_value;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_sshort");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_sshort(a smallint signed, \
                                                  b smallint signed, \
                                                  c smallint unsigned, \
                                                  d smallint unsigned)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_sshort VALUES(-5999, -5999, 35999, 200)");
  check_mysql_rc(rc, mysql);


  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_sshort"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[0].buffer= (void *)&short_value;
  my_bind[0].length= &s_length;

  my_bind[1].buffer_type= MYSQL_TYPE_LONG;
  my_bind[1].buffer= (void *)&long_value;
  my_bind[1].length= &l_length;

  my_bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[2].buffer= (void *)&longlong_value;
  my_bind[2].length= &ll_length;

  my_bind[3].buffer_type= MYSQL_TYPE_TINY;
  my_bind[3].buffer= (void *)&tiny_value;
  my_bind[3].is_unsigned= TRUE;
  my_bind[3].length= &t_length;

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(short_value == -5999, "sv != -5999");
  FAIL_UNLESS(s_length == 2, "s_length != 2");

  FAIL_UNLESS(long_value == -5999, "l_v != -5999");
  FAIL_UNLESS(l_length == 4, "l_length != 4");

  FAIL_UNLESS(longlong_value == 35999, "llv != 35999");
  FAIL_UNLESS(ll_length == 8, "ll_length != 8");

  FAIL_UNLESS(tiny_value == 200, "t_v != 200");
  FAIL_UNLESS(t_length == 1, "t_length != 1");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_sshort");
  check_mysql_rc(rc, mysql);
  return OK;
}


/* Test a misc tinyint-signed conversion bug */

static int test_stiny_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[4];
  short      short_value;
  int32      long_value;
  ulong      s_length, l_length, ll_length, t_length;
  ulonglong  longlong_value;
  int        rc;
  uchar      tiny_value;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_stiny");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_stiny(a tinyint signed, \
                                                  b tinyint signed, \
                                                  c tinyint unsigned, \
                                                  d tinyint unsigned)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_stiny VALUES(-128, -127, 255, 0)");
  check_mysql_rc(rc, mysql);


  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_stiny"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[0].buffer= (void *)&short_value;
  my_bind[0].length= &s_length;

  my_bind[1].buffer_type= MYSQL_TYPE_LONG;
  my_bind[1].buffer= (void *)&long_value;
  my_bind[1].length= &l_length;

  my_bind[2].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[2].buffer= (void *)&longlong_value;
  my_bind[2].length= &ll_length;

  my_bind[3].buffer_type= MYSQL_TYPE_TINY;
  my_bind[3].buffer= (void *)&tiny_value;
  my_bind[3].length= &t_length;

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(short_value == -128, "s_v != -128");
  FAIL_UNLESS(s_length == 2, "s_length != 2");

  FAIL_UNLESS(long_value == -127, "l_v != -127");
  FAIL_UNLESS(l_length == 4, "l_length != 4");

  FAIL_UNLESS(longlong_value == 255, "llv != 255");
  FAIL_UNLESS(ll_length == 8, "ll_length != 8");

  FAIL_UNLESS(tiny_value == 0, "t_v != 0");
  FAIL_UNLESS(t_length == 1, "t_length != 1");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_stiny");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bug53311(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  int i;
  const char *query= "INSERT INTO bug53311 VALUES (1)";

  rc= mysql_options(mysql, MYSQL_OPT_RECONNECT, "1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bug53311");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bug53311 (a int)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i=0; i < 2; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  /* kill connection */
  rc= mysql_kill(mysql, mysql_thread_id(mysql));

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(rc == 0, "Error expected");
  FAIL_IF(mysql_stmt_errno(stmt) == 0, "Errno != 0 expected");
  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bug53311");
  check_mysql_rc(rc, mysql);

  return OK;
}
#define PREPARE_SQL "EXPLAIN SELECT t1.*, t2.* FROM test AS t1, test AS t2"

#ifdef NOT_IN_USE
static int test_metadata(MYSQL *mysql)
{
  int rc;

	rc= mysql_query(mysql, "DROP TABLE IF EXISTS test");
  check_mysql_rc(rc, mysql);
	rc= mysql_query(mysql, "CREATE TABLE test(id INT, label CHAR(1), PRIMARY KEY(id)) ENGINE=MYISAM");
  check_mysql_rc(rc, mysql);

	rc= mysql_query(mysql, "INSERT INTO test(id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')");
  check_mysql_rc(rc, mysql);
	printf("Client=%s\n", mysql_get_client_info());
	printf("Server=%s\n", mysql_get_server_info(mysql));

	{
		MYSQL_STMT * stmt = mysql_stmt_init(mysql);
		if (!stmt) {
			fprintf(stderr, "Failed to init stmt: Error: %s\n", mysql_error(mysql));
			goto end;
		}
		if (mysql_stmt_prepare(stmt, PREPARE_SQL, sizeof(PREPARE_SQL) - 1)) {
			fprintf(stderr, "Failed to prepare stmt: Error: %s\n", mysql_stmt_error(stmt));
			goto end2;
		}
		if (mysql_stmt_execute(stmt)) {
			fprintf(stderr, "Failed to execute stmt: Error: %s\n", mysql_stmt_error(stmt));
			goto end2;
		}
		{
			MYSQL_FIELD * field = NULL;
			MYSQL_RES * res = mysql_stmt_result_metadata(stmt);
			if (!res) {
				fprintf(stderr, "Failed to get metadata: Error: %s\n", mysql_stmt_error(stmt));
				goto end2;
			}
			while ((field = mysql_fetch_field(res))) {
				printf("name=%s\n", field->name);
				printf("catalog=%s\n", field->catalog);
			}
			mysql_free_result(res);

		}
end2:
		mysql_stmt_close(stmt);
	}
end:
	return 0;
}
#endif

static int test_conc_5(MYSQL *mysql)
{
  const char *query= "SELECT a FROM t1";
  MYSQL_RES *res;
  MYSQL_STMT *stmt;
  MYSQL_FIELD *fields;
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, "couldn't allocate memory");

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  res= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!res, "Can't obtain resultset");

  fields= mysql_fetch_fields(res);
  FAIL_IF(!fields, "Can't obtain fields");

  FAIL_IF(strcmp("def", fields[0].catalog), "unexpected value for field->catalog");

  mysql_free_result(res);
  mysql_stmt_close(stmt);
  return OK;
}

static int test_conc141(MYSQL *mysql)
{
  int rc;
  const char *query= "CALL p_conc141";
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc141");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE conc141 (KeyVal int not null primary key)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO conc141 VALUES(1)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p_conc141");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE PROCEDURE p_conc141()\n"
                   "BEGIN\n"
                     "select * from conc141;\n"
                     "insert into conc141(KeyVal) VALUES(1);\n"
                   "END");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* skip first result */
  rc= mysql_stmt_next_result(stmt);
  FAIL_IF(rc==-1, "No more results and error expected");
  mysql_stmt_free_result(stmt);
  FAIL_IF(mysql_stmt_errno(stmt), "No Error expected");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc141");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p_conc141");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc154(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmtstr= "SELECT * FROM t1";
  int rc;

  /* 1st: empty result set without free_result */
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a varchar(20))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmtstr));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  /* 2nd: empty result set with free_result */
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmtstr));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  /* 3rd: non empty result without free_result */
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES ('test_conc154')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmtstr));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  /* 4th non empty result set with free_result */
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmtstr));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_conc155(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND bind;
  char buffer[50];
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a TEXT)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES ('zero terminated string')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL("SELECT a FROM t1"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(buffer, 'X', 50);
  memset(&bind, 0, sizeof(MYSQL_BIND));

  bind.buffer= buffer;
  bind.buffer_length= 50;
  bind.buffer_type= MYSQL_TYPE_STRING;

  rc= mysql_stmt_bind_result(stmt, &bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  if (strlen(buffer) != strlen("zero terminated string"))
  {
    diag("Wrong buffer length");
    return FAIL;
  }

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc168(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);

  MYSQL_BIND bind;
  char buffer[100];
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc168");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE conc168(a datetime(3))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO conc168 VALUES ('2016-03-09 07:51:49.000'),('2016-03-09 07:51:49.001'),('2016-03-09 07:51:49.010')");
  check_mysql_rc(rc, mysql);

  memset(&bind, 0, sizeof(MYSQL_BIND));
  bind.buffer= buffer;
  bind.buffer_type= MYSQL_TYPE_STRING;
  bind.buffer_length= 100;

  rc= mysql_stmt_prepare(stmt, SL("SELECT a FROM conc168"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, &bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(strcmp(buffer, "2016-03-09 07:51:49.000"), "expected: 2016-03-09 07:51:49.000");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(strcmp(buffer, "2016-03-09 07:51:49.001"), "expected: 2016-03-09 07:51:49.001");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(strcmp(buffer, "2016-03-09 07:51:49.010"), "expected: 2016-03-09 07:51:49.010");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc168");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc167(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);

  MYSQL_BIND bind[3];
  char buffer[100];
  int bit1=0, bit2=0;
  int rc;
  const char *stmt_str= "SELECT a,b,c FROM conc168";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc168");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE conc168(a bit, b bit, c varchar(10))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO conc168 VALUES (1,0, 'test12345')");
  check_mysql_rc(rc, mysql);

  memset(bind, 0, 3 * sizeof(MYSQL_BIND));
  bind[0].buffer= &bit1;
  bind[0].buffer_type= MYSQL_TYPE_BIT;
  bind[0].buffer_length= sizeof(int);
  bind[1].buffer= &bit2;
  bind[1].buffer_type= MYSQL_TYPE_BIT;
  bind[1].buffer_length= sizeof(int);
  bind[2].buffer= buffer;
  bind[2].buffer_type= MYSQL_TYPE_STRING;
  bind[2].buffer_length= 100;

  rc= mysql_stmt_prepare(stmt, SL(stmt_str));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  diag("bit=%d %d char=%s", bit1, bit2, buffer);

  mysql_stmt_close(stmt);
  return OK;
}

static int test_conc177(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL_BIND bind[2];
  const char *stmt_str= "SELECT a,b FROM t1";
  char buf1[128], buf2[128];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a double zerofill default 8.8,b float zerofill default 8.8)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (DEFAULT, DEFAULT)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_str));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer= &buf1;
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].buffer_length= 128;
  bind[1].buffer= &buf2;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer_length= 128;

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  mysql_stmt_close(stmt);

  diag("buf1 %s\nbuf2 %s", buf1, buf2);

  FAIL_IF(strcmp(buf1, "00000000000000000008.8"), "Expected 00000000000000000008.8");
  FAIL_IF(strcmp(buf2, "0000000008.8"), "Expected 0000000008.8");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int(8) zerofill default 1, b int(4) zerofill default 1)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (DEFAULT, DEFAULT)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_str));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer= &buf1;
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].buffer_length= 128;
  bind[1].buffer= &buf2;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer_length= 128;

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  mysql_stmt_close(stmt);

  diag("buf1 %s\nbuf2 %s", buf1, buf2);

  FAIL_IF(strcmp(buf1, "00000001"), "Expected 00000001");
  FAIL_IF(strcmp(buf2, "0001"), "Expected 0001");
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc179(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmtstr= "select 1 as ' '";

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmtstr));
  check_stmt_rc(rc, stmt);

  if (mysql_get_server_version(mysql) >= 100100)
  {
    FAIL_IF(mysql_warning_count(mysql) < 1, "expected 1 or more warnings");
    FAIL_IF(mysql_stmt_warning_count(stmt) < 1, "expected 1 or more warnings");
  }

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_conc182(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL_BIND bind[2];
  char buf1[22];
  MYSQL_RES *result;
  MYSQL_ROW row;

  stmt= mysql_stmt_init(mysql);
  rc= mariadb_stmt_execute_direct(stmt, "DROP TABLE IF EXISTS t1", -1);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "DROP TABLE IF EXISTS t1", -1);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1", -1);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1", -1);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT row_count()");
  result= mysql_store_result(mysql);
  row= mysql_fetch_row(result);
  diag("buf: %s", row[0]);
  mysql_free_result(result);


  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, "SELECT row_count()", -1);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer= &buf1;
  bind[0].buffer_length= bind[1].buffer_length= 20;
  bind[0].buffer_type= bind[1].buffer_type= MYSQL_TYPE_STRING;

  rc= mysql_stmt_bind_result(stmt, bind);

  while(!mysql_stmt_fetch(stmt))
  diag("b1: %s", buf1);
  rc= mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc181(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL_BIND bind;
  const char *stmt_str= "SELECT a FROM t1";
  float f=1;
  my_bool err= 0;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES(1073741825)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(stmt_str));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(&bind, 0, sizeof(MYSQL_BIND));
  bind.buffer= &f;
  bind.error= &err;
  bind.buffer_type= MYSQL_TYPE_FLOAT;
  rc= mysql_stmt_bind_result(stmt, &bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  diag("rc=%d err=%d float=%f, %d", rc, err, f, MYSQL_DATA_TRUNCATED);

  rc= mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc198(MYSQL *mysql)
{
  MYSQL_STMT *stmt1, *stmt2;
  MYSQL_BIND my_bind[1];
  int32 a;
  int rc;
  int num_rows= 0;
  ulong type;
  ulong prefetch_rows= 3;


  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (id integer not null primary key)");
  rc= mysql_query(mysql, "insert into t1 (id) values "
                         " (1), (2), (3), (4), (5), (6), (7), (8), (9)");
  check_mysql_rc(rc, mysql);

  stmt1= mysql_stmt_init(mysql);
  stmt2= mysql_stmt_init(mysql);
  /* Not implemented in 5.0 */
  type= (ulong) CURSOR_TYPE_SCROLLABLE;
  rc= mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  FAIL_UNLESS(rc, "Error expected");
  rc= mysql_stmt_attr_set(stmt2, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  FAIL_UNLESS(rc, "Error expected");

  type= (ulong) CURSOR_TYPE_READ_ONLY;
  rc= mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_attr_set(stmt2, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt2);
  rc= mysql_stmt_attr_set(stmt1, STMT_ATTR_PREFETCH_ROWS,
                          (void*) &prefetch_rows);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_attr_set(stmt2, STMT_ATTR_PREFETCH_ROWS,
                          (void*) &prefetch_rows);
  check_stmt_rc(rc, stmt2);
  rc= mysql_stmt_prepare(stmt1, "SELECT * FROM t1 ORDER by id ASC" , -1);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_prepare(stmt2, "SELECT * FROM t1 ORDER by id DESC", -1);
  check_stmt_rc(rc, stmt2);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_execute(stmt2);
  check_stmt_rc(rc, stmt2);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void*) &a;
  my_bind[0].buffer_length= sizeof(a);
  mysql_stmt_bind_result(stmt1, my_bind);
  mysql_stmt_bind_result(stmt2, my_bind);

  while ((rc= mysql_stmt_fetch(stmt1)) == 0)
    ++num_rows;
  FAIL_UNLESS(num_rows == 9, "num_rows != 9");

  num_rows= 0;
  while ((rc= mysql_stmt_fetch(stmt2)) == 0)
    ++num_rows;
  FAIL_UNLESS(num_rows == 9, "num_rows != 9");

  rc= mysql_stmt_close(stmt1);
  rc= mysql_stmt_close(stmt2);
  FAIL_UNLESS(rc == 0, "");

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc205(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[3];
  char       data[8];
  ulong      length[3];
  int        rc, int_col;
  short      smint_col;
  my_bool    is_null[3];
  const char *query = "SELECT text_col, smint_col, int_col FROM test_conc205";

  rc= mysql_query(mysql, "drop table if exists test_conc205");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_conc205 (text_col TEXT, smint_col SMALLINT, int_col INT)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_conc205 VALUES('data01', 21893, 1718038908), ('data2', -25734, -1857802040)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)data;
  my_bind[0].buffer_length= sizeof(data);
  my_bind[0].is_null= &is_null[0];
  my_bind[0].length= &length[0];

  my_bind[1].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[1].buffer= &smint_col;
  my_bind[1].buffer_length= 2;
  my_bind[1].is_null= &is_null[1];
  my_bind[1].length= &length[1];

  my_bind[2].buffer_type= MYSQL_TYPE_LONG;
  my_bind[2].buffer= &int_col;
  my_bind[2].buffer_length= 4;
  my_bind[2].is_null= &is_null[2];
  my_bind[2].length= &length[2];

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(length[0] != 6, "Wrong fetched string length");
  FAIL_IF(length[1] != 2, "Wrong fetched short length");
  FAIL_IF(length[2] != 4, "Wrong fetched int length");

  FAIL_IF(strncmp(data, "data01", length[0] + 1) != 0, "Wrong string value");
  FAIL_IF(smint_col != 21893, "Expected 21893");
  FAIL_IF(int_col != 1718038908, "Expected 1718038908");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(length[0] != 5, "Wrong fetched string length");
  FAIL_IF(length[1] != 2, "Wrong fetched short length");
  FAIL_IF(length[2] != 4, "Wrong fetched int length");

  FAIL_IF(strncmp(data, "data2", length[0] + 1) != 0, "Wrong string value");
  FAIL_IF(smint_col != -25734, "Expected 21893");
  FAIL_IF(int_col != -1857802040, "Expected 1718038908");

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != MYSQL_NO_DATA, "Expected MYSQL_NO_DATA");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table test_conc205");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_conc217(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;

  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1 FROM nonexisting_table", -1);
  FAIL_IF(rc==0, "Expected error\n");
  rc= mysql_query(mysql, "drop table if exists t_count");
  check_mysql_rc(rc, mysql);
  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc208(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  int data;
  MYSQL_BIND bind;

  rc= mysql_stmt_prepare(stmt, "SELECT \"100\" UNION SELECT \"88\" UNION SELECT \"389789\"", -1);
  check_stmt_rc(rc, stmt);

  memset(&bind, 0, sizeof(MYSQL_BIND));
  bind.buffer_type= MYSQL_TYPE_LONG;
  bind.buffer= (void *)&data;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, &bind);

  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
  {
    diag("data=%d", data);
    FAIL_IF(data != 100 && data != 88 && data != 389789, "Wrong value");
  }
  mysql_stmt_close(stmt);
  return OK;
}

static int test_mdev14165(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_FIELD *fields;
  MYSQL_RES *result;
  my_bool val= 1;
  MYSQL_BIND bind[1];
  char buf1[52];

  rc= mysql_options(mysql, MYSQL_REPORT_DATA_TRUNCATION, &val);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  rc= mysql_query(mysql, "CREATE TABLE t1 (i INT(20) ZEROFILL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (2),(1)");
  check_mysql_rc(rc, mysql);
  rc= mysql_stmt_prepare(stmt, "SELECT i FROM t1", -1);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND));
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].buffer_length= 51;
  bind[0].buffer= buf1;

  mysql_stmt_bind_result(stmt, bind);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &val);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);

  fields= mysql_fetch_fields(result);

  FAIL_IF(fields[0].length < 20, "Expected length=20");
  FAIL_IF(fields[0].max_length < 20, "Expected max_length=20");

  mysql_stmt_fetch(stmt);

  FAIL_UNLESS(strcmp(buf1, "00000000000000000002") == 0, "Wrong result");
  mysql_free_result(result);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_compress(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;

  rc= mariadb_stmt_execute_direct(stmt, SL("SELECT 1 FROM DUAL"));
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  return OK;
}

static int equal_MYSQL_TIME(MYSQL_TIME *tm1, MYSQL_TIME *tm2)
{
  return tm1->day==tm2->day && tm1->hour==tm2->hour && tm1->minute==tm2->minute &&
    tm1->month==tm2->month && tm1->neg==tm2->neg && tm1->second==tm2->second &&
    tm1->second_part==tm2->second_part && tm1->time_type==tm2->time_type && tm1->year==tm2->year;
}

static int test_str_to_int(MYSQL *mysql)
{
 int i;
 struct st_atoi_test{
    const char *str_value;
    int int_value;
    int rc;
  } atoi_tests[]=
  {
    {"0", 0, 0},
    {" 1",1, 0},
    {"123 ",123, 0},
    {"10.2",10, MYSQL_DATA_TRUNCATED},
    {"a", 0, MYSQL_DATA_TRUNCATED},
    {"1 2 3", 1, MYSQL_DATA_TRUNCATED},
    {NULL, 0, 0}
  };

  for(i=0; atoi_tests[i].str_value; i++)
  {
    int rc;
    MYSQL_STMT *stmt;
    MYSQL_BIND bind[1];
    struct st_atoi_test *test= &atoi_tests[i];
    char sql[256];
    int int_value;

    snprintf(sql, sizeof(sql), "SELECT '%s'",test->str_value);

    stmt= mysql_stmt_init(mysql);

    rc= mysql_stmt_prepare(stmt, sql, (ulong)strlen(sql));
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_store_result(stmt);

    memset(bind, 0, sizeof(MYSQL_BIND));
    bind[0].buffer_type= MYSQL_TYPE_LONG;
    bind[0].buffer= &int_value;
    bind[0].buffer_length= sizeof(int_value);

    rc= mysql_stmt_bind_result(stmt, bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);

    diag("test: str='%s', expected/returned value =%d/%d, expected/returned rc=%d/%d",
      test->str_value, test->int_value, int_value, test->rc, rc);
    FAIL_UNLESS(rc == test->rc, "unexpected return code");
    FAIL_UNLESS(int_value == test->int_value, "unexpected int value");
    mysql_stmt_close(stmt);
  }
  return OK;
}


static int test_codbc138(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[1];
  MYSQL_TIME tm;
  int i= 0;

  struct st_time_test {
    const char *statement;
    MYSQL_TIME tm;
  } time_test[]={
    { "SELECT DATE_ADD('2018-02-01', INTERVAL -188 DAY)",
  { 2017,7,28,0,0,0,0L,0, MYSQL_TIMESTAMP_DATE }
    },
  { "SELECT '2001-02-03 11:12:13.123456'",
  { 2001,2,3,11,12,13,123456L,0, MYSQL_TIMESTAMP_DATETIME }
  },
  { "SELECT '2001-02-03 11:12:13.123'",
  { 2001,2,3,11,12,13,123000L,0, MYSQL_TIMESTAMP_DATETIME }
  },
  { "SELECT '-11:12:13'",
  { 0,0,0,11,12,13,0,1, MYSQL_TIMESTAMP_TIME }
  },
  { "SELECT ' '",
  { 0,0,0,0,0,0,0,0, MYSQL_TIMESTAMP_ERROR }
  },
  { "SELECT '1--'",
  { 0,0,0,0,0,0,0,0, MYSQL_TIMESTAMP_ERROR }
  },
  { "SELECT '-2001-01-01'",
  { 0,0,0,0,0,0,0,0, MYSQL_TIMESTAMP_ERROR }
  },
  { "SELECT '-11:00'",
  { 0,0,0,0,0,0,0,0, MYSQL_TIMESTAMP_ERROR }
  },
  {"SELECT '1972-04-22'",
  {1972,4,22, 0,0,0, 0,0,MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT ' 1972-04-22 '",
  {1972,4,22, 0,0,0, 0,0,MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '1972-04-22a'",
  {1972,4,22, 0,0,0, 0,0,MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '0000-00-00'",
  {0,0,0, 0,0,0 ,0,0,MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '1970-01-00'",
  {1970,1,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '0069-12-31'",
  {69,12,31, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '69-12-31'",
  {2069,12,31, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '68-12-31'",
  {2068,12,31, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '70-01-01'",
  {1970,1,1, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },
  {"SELECT '2010-1-1'",
  {2010,1,1, 0,0,0, 0,0, MYSQL_TIMESTAMP_DATE}
  },

  {"SELECT '10000-01-01'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-a-01'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-01-32'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-13-01'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1YYY-01-01'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-0M-01'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-00-'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979-00'",
  {0,0,0, 0,0,0, 0,0,MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '1979'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },
  {"SELECT '79'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR}
  },

  {"SELECT '10:15:00'", 
  {0,0,0, 10,15,0, 0,0, MYSQL_TIMESTAMP_TIME}
  },
  {"SELECT '10:15:01'",
  {0,0,0, 10,15,1, 0,0, MYSQL_TIMESTAMP_TIME}
  },
  {"SELECT '00:00:00'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_TIME}
  },
  {"SELECT '0:0:0'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_TIME}
  },
  {"SELECT '10:15:01.'",
  {0,0,0, 10,15,1, 0,0, MYSQL_TIMESTAMP_TIME},
  },
  {"SELECT '25:59:59'",
  {0,0,0, 25,59,59, 0,0, MYSQL_TIMESTAMP_TIME},
  },
  {"SELECT '838:59:59'",
  {0,0,0, 838,59,59, 0,0, MYSQL_TIMESTAMP_TIME},
  },
  {"SELECT '-838:59:59'",
  {0,0,0, 838,59,59, 0, 1, MYSQL_TIMESTAMP_TIME},
  },
 
  {"SELECT '00:60:00'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR},
  },
  {"SELECT '00:60:00'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR},
  },
  {"SELECT '839:00:00'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR},
  },
  {"SELECT '-839:00:00'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR},
  },
  {"SELECT '-10:15:a'",
  { 0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR },
  },
  {"SELECT '1999-12-31 23:59:59.9999999'",
  {1999,12,31, 23,59,59, 999999, 0, MYSQL_TIMESTAMP_DATETIME},
  },
  {"SELECT '00-08-11 8:46:40'", 
  {2000,8,11, 8,46,40, 0,0, MYSQL_TIMESTAMP_DATETIME},
  },
  {"SELECT '1999-12-31 25:59:59.999999'",
  {0,0,0, 0,0,0, 0,0, MYSQL_TIMESTAMP_ERROR },
  },
  { NULL,{ 0 } }
  };

  while (time_test[i].statement)
  {
    stmt= mysql_stmt_init(mysql);
    rc= mysql_stmt_prepare(stmt, SL(time_test[i].statement));
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_store_result(stmt);

    memset(bind, 0, sizeof(MYSQL_BIND));
    bind[0].buffer_type= MYSQL_TYPE_DATETIME;
    bind[0].buffer= &tm;
    bind[0].buffer_length= sizeof(MYSQL_TIME);

    rc= mysql_stmt_bind_result(stmt, bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);
    diag("test: %s %d %d", time_test[i].statement, tm.time_type, time_test[i].tm.time_type);
    if (time_test[i].tm.time_type == MYSQL_TIMESTAMP_ERROR)
    {
      FAIL_UNLESS(tm.time_type == MYSQL_TIMESTAMP_ERROR, "MYSQL_TIMESTAMP_ERROR expected");
    }
    else
      FAIL_UNLESS(equal_MYSQL_TIME(&tm, &time_test[i].tm), "time_in != time_out");
    mysql_stmt_close(stmt);
    i++;
  }

  return OK;
}

static int test_conc334(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_RES *result;
  MYSQL_FIELD *field;
  int rc;

  rc= mysql_stmt_prepare(stmt, SL("SHOW ENGINES"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  if (!result)
  {
    diag("Coudn't retrieve result set");
    mysql_stmt_close(stmt);
    return FAIL;
  }

  mysql_field_seek(result, 0);

  while ((field= mysql_fetch_field(result)))
  {
    FAIL_IF(field->name_length == 0, "Invalid name length (0)");
    FAIL_IF(field->table_length == 0, "Invalid name length (0)");
  }
  mysql_free_result(result);
  mysql_stmt_close(stmt);

  return OK;
}
static int test_conc344(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
 
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int, b int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1,1), (2,2),(3,3),(4,4),(5,5)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM t1 ORDER BY a"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  while (!mysql_stmt_fetch(stmt));
  FAIL_IF(mysql_stmt_num_rows(stmt) != 5, "expected 5 rows");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  diag("num_rows: %lld", mysql_stmt_num_rows(stmt));
  FAIL_IF(mysql_stmt_num_rows(stmt) != 1, "expected 1 row");

  mysql_stmt_close(stmt);
  return OK;
}


static int test_conc_fraction(MYSQL *mysql)
{
  MYSQL_TIME tm;
  MYSQL_BIND bind[1];
  char query[1024];
  int i;
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  unsigned long frac= 0;

  for (i=0; i < 10; i++, frac=frac*10+i)
  {
    unsigned long expected= 0;
    sprintf(query, "SELECT '2018-11-05 22:25:59.%ld'", frac);

    diag("%d: %s", i, query);

    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_store_result(stmt);

    memset(bind, 0, sizeof(MYSQL_BIND));
    bind[0].buffer_type= MYSQL_TYPE_DATETIME;
    bind[0].buffer= &tm;
    bind[0].buffer_length= sizeof(MYSQL_TIME);

    rc= mysql_stmt_bind_result(stmt, bind);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    diag("second_part: %ld", tm.second_part);

    expected= i > 6 ? 123456 : frac * (unsigned int)powl(10, (6 - i));

    diag("tm.second_part=%ld expected=%ld", tm.second_part, expected);
    FAIL_IF(tm.second_part != expected, "expected fractional part to be 900000");

  }
  mysql_stmt_close(stmt);
  return OK;
}

static int test_zerofill_1byte(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  MYSQL_BIND bind;
  char buffer[3];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int zerofill)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES(1)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("SELECT a FROM t1"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(&bind, 0, sizeof(MYSQL_BIND));
  bind.buffer_type= MYSQL_TYPE_STRING;
  bind.buffer= buffer;
  bind.buffer_length= 1;

  rc= mysql_stmt_bind_result(stmt, &bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != 101, "expected truncation warning");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_conc424(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  my_bool max_len= 1;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_table1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_table1 (test_int INT, b int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_table1 values(10,11),(11,12)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS testCursor");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE PROCEDURE testCursor()\n"
                  "BEGIN\n"
                  "DECLARE test_int INT;\n"
                  "DECLARE b INT;\n"
                  "DECLARE done INT DEFAULT FALSE;\n"
                  "DECLARE testCursor CURSOR\n"
                  "FOR\n"
                  "SELECT test_int,b FROM test_table1;\n"
                  "DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;\n"
                  "OPEN testCursor;\n"

                  " read_loop: LOOP\n"
                  "   FETCH testCursor INTO test_int, b;\n"
                  "   IF done THEN\n"
                  "     LEAVE read_loop;\n"
                  "   END IF;\n"
                  "   SELECT test_int,b;"
                  " END LOOP;\n"
                  "CLOSE testCursor;\n"
                  "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL("CALL testCursor()"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &max_len);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  do {
    if (mysql_stmt_field_count(stmt))
    {
      MYSQL_RES *res= mysql_stmt_result_metadata(stmt);
      rc= mysql_stmt_fetch(stmt);
      FAIL_IF(rc, "Wrong return code");
      mysql_free_result(res);
    }
    rc= mysql_stmt_next_result(stmt);

  } while (!rc);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP PROCEDURE testCursor");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE test_table1");
  check_mysql_rc(rc, mysql);

  return OK;
}

struct my_tests_st my_tests[] = {
  {"test_conc424", test_conc424, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_conc344", test_conc344, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_conc334", test_conc334, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_compress", test_compress, TEST_CONNECTION_NEW, CLIENT_COMPRESS, NULL, NULL},
  {"test_zerofill_1byte", test_zerofill_1byte, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_codbc138", test_codbc138, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc208", test_conc208, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_mdev14165", test_mdev14165, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc208", test_conc208, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc217", test_conc217, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc205", test_conc205, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc198", test_conc198, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc182", test_conc182, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc181", test_conc181, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc179", test_conc179, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc177", test_conc177, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc167", test_conc167, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc168", test_conc168, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc155", test_conc155, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc154", test_conc154, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_conc141", test_conc141, TEST_CONNECTION_NEW, 0, NULL , NULL},
  {"test_conc67", test_conc67, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_conc_5", test_conc_5, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1115", test_bug1115, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1180", test_bug1180, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1644", test_bug1644, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11037", test_bug11037, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11183", test_bug11183, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug12744", test_bug12744, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1500", test_bug1500, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug15510", test_bug15510, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug15518", test_bug15518, TEST_CONNECTION_NEW | TEST_CONNECTION_DONT_CLOSE, CLIENT_MULTI_STATEMENTS, NULL , NULL},
  {"test_bug15613", test_bug15613, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug16144", test_bug16144, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1664", test_bug1664, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug1946", test_bug1946, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug2247", test_bug2247, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug2248", test_bug2248, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug20152", test_bug20152, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug23383", test_bug23383, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug27592", test_bug27592, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug28934", test_bug28934, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug36004", test_bug36004, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug3035", test_bug3035, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug3117", test_bug3117, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug3796", test_bug3796, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4026", test_bug4026, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4030", test_bug4030, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4079", test_bug4079, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4172", test_bug4172, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4231", test_bug4231, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug4236", test_bug4236, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug5126", test_bug5126, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug5194", test_bug5194, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug5315", test_bug5315, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug5399", test_bug5399, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug6046", test_bug6046, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug6049", test_bug6049, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug6058", test_bug6058, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug6059", test_bug6059, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug6096", test_bug6096, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug7990", test_bug7990, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug8330", test_bug8330, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug8722", test_bug8722, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ps_conj_select", test_ps_conj_select, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ps_null_param", test_ps_null_param, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ps_query_cache", test_ps_query_cache, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_ushort_bug", test_ushort_bug, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_field_misc", test_field_misc, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_mem_overun", test_mem_overun, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_decimal_bug", test_decimal_bug, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_explain_bug", test_explain_bug, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_sshort_bug", test_sshort_bug, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_stiny_bug", test_stiny_bug, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug53311", test_bug53311, TEST_CONNECTION_NEW, 0, NULL , NULL},
  {"test_conc_fraction", test_conc_fraction, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_str_to_int", test_str_to_int, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
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
