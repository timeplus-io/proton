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
/**
  Some basic tests of the client API.
*/

#include "my_test.h"

static int client_store_result(MYSQL *mysql)
{
  MYSQL_RES *result;
  int       rc, rowcount= 0;

  rc= mysql_query(mysql, "SELECT 'foo' FROM DUAL UNION SELECT 'bar' FROM DUAL");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  /* since we use store result, we should be able execute other api calls */
  rc= mysql_ping(mysql);
  FAIL_IF(rc, "mysql_ping failed");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 2, "rowcount != 2");
  
  mysql_free_result(result);

  return OK;
}

static int client_use_result(MYSQL *mysql)
{
  MYSQL_RES *result;
  int       rc, rowcount= 0;

  rc= mysql_query(mysql, "SELECT 'foo' FROM DUAL UNION SELECT 'bar' FROM DUAL");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  /* since we use use result, we shouldn't be able execute other api calls */
  rc= mysql_ping(mysql);
  FAIL_IF(!rc, "Error expected");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 2, "rowcount != 2");
  
  mysql_free_result(result);

  return OK;
}

static int test_free_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char       c2[5];
  ulong      bl1, l2;
  int        rc, c1, bc1;
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "drop table if exists test_free_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table test_free_result("
                         "c1 int primary key auto_increment)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into test_free_result values(), (), ()");
  check_mysql_rc(rc, mysql);

  strcpy(query, "select * from test_free_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&bc1;
  my_bind[0].length= &bl1;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  c2[0]= '\0'; l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)c2;
  my_bind[0].buffer_length= 7;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(strncmp(c2, "1", 1) == 0, "c2 != '1'");
  FAIL_UNLESS(l2 == 1, "l2 != 1");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  c1= 0, l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&c1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(c1 == 2, "c1 != 2");
  FAIL_UNLESS(l2 == 4, "l2 != 4");

  rc= mysql_query(mysql, "drop table test_free_result");
  FAIL_IF(!rc, "Error commands out of sync expected");

  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "drop table test_free_result");
  check_mysql_rc(rc, mysql);  /* should be successful */

  mysql_stmt_close(stmt);

  return OK;
}


/* Test mysql_stmt_store_result() */

static int test_free_store_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char       c2[5];
  ulong      bl1, l2;
  int        rc, c1, bc1;
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "drop table if exists test_free_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table test_free_result(c1 int primary key auto_increment)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into test_free_result values(), (), ()");
  check_mysql_rc(rc, mysql);

  strcpy(query, "select * from test_free_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&bc1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &bl1;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  c2[0]= '\0'; l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)c2;
  my_bind[0].buffer_length= 7;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(strncmp(c2, "1", 1) == 0, "c2 != '1'");
  FAIL_UNLESS(l2 == 1, "l2 != 1");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  c1= 0, l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&c1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(c1 == 2, "c1 != 2");
  FAIL_UNLESS(l2 == 4, "l2 != 4");

  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "drop table test_free_result");
  check_mysql_rc(rc, mysql);

  mysql_stmt_close(stmt);

  return OK;
}

static int test_store_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  int32      nData;
  char       szData[100];
  MYSQL_BIND my_bind[2];
  ulong      length, length1;
  my_bool    is_null[2];
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_store_result(col1 int , col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result VALUES(10, 'venu'), (20, 'mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result(col2) VALUES('monty')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* fetch */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *) &nData;       /* integer data */
  my_bind[0].length= &length;
  my_bind[0].is_null= &is_null[0];

  length= 0;
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= szData;                /* string data */
  my_bind[1].buffer_length= sizeof(szData);
  my_bind[1].length= &length1;
  my_bind[1].is_null= &is_null[1];
  length1= 0;

  strcpy(query, "SELECT * FROM test_store_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 10, "nData != 10");
  FAIL_UNLESS(strcmp(szData, "venu") == 0, "szData != 'Venu'");
  FAIL_UNLESS(length1 == 4, "length1 != 4");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 20, "nData != 20");
  FAIL_UNLESS(strcmp(szData, "mysql") == 0, "szDaza != 'mysql'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  length= 99;
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(is_null[0], "isnull set");
  FAIL_UNLESS(strcmp(szData, "monty") == 0, "szData != 'monty'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 10, "nData != 10");
  FAIL_UNLESS(strcmp(szData, "venu") == 0, "szData != 'Venu'");
  FAIL_UNLESS(length1 == 4, "length1 != 4");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 20, "nData != 20");
  FAIL_UNLESS(strcmp(szData, "mysql") == 0, "szDaza != 'mysql'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  length= 99;
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(is_null[0], "isnull set");
  FAIL_UNLESS(strcmp(szData, "monty") == 0, "szData != 'monty'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Test simple bind store result */

static int test_store_result1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_store_result(col1 int , col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result VALUES(10, 'venu'), (20, 'mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result(col2) VALUES('monty')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT * FROM test_store_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 3, "rowcount != 3");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 3, "rowcount != 3");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Another test for bind and store result */

static int test_store_result2(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  int        nData;
  ulong      length;
  MYSQL_BIND my_bind[1];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_store_result(col1 int , col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result VALUES(10, 'venu'), (20, 'mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_store_result(col2) VALUES('monty')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *) &nData;      /* integer data */
  my_bind[0].length= &length;
  my_bind[0].is_null= 0;

  strcpy((char *)query , "SELECT col1 FROM test_store_result where col1= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  nData= 10; length= 0;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  nData= 0;
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 10, "nData != 10");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  nData= 20;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  nData= 0;
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 20, "nData != 20");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_store_result");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug11718(MYSQL *mysql)
{
  MYSQL_RES	*res;
  int rc;
  const char *query= "select str_to_date(concat(f3),'%Y%m%d') from t1,t2 "
                     "where f1=f2 order by f1";

  rc= mysql_query(mysql, "drop table if exists t1, t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (f1 int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t2 (f2 int, f3 numeric(8))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1), (2)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (1,20050101), (2,20050202)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);
  res = mysql_store_result(mysql);

  FAIL_UNLESS(res->fields[0].type == MYSQL_TYPE_DATE, "type != MYSQL_TYPE_DATE");
  mysql_free_result(res);
  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug19671(MYSQL *mysql)
{
  MYSQL_RES *result;
  int rc;

  mysql_query(mysql, "set sql_mode=''");
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, "drop view if exists v1");
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, "create table t1(f1 int)");
  check_mysql_rc(rc, mysql)

  rc= mysql_query(mysql, "create view v1 as select va.* from t1 va");
  check_mysql_rc(rc, mysql)

  result= mysql_list_fields(mysql, "v1", NULL);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 0, "");

  if (verify_prepare_field(result, 0, "f1", "f1", MYSQL_TYPE_LONG,
                       "v1", "v1", schema, 11, "0")) {
    mysql_free_result(result);
    diag("verify_prepare_field failed");
    return FAIL;
  }

  mysql_free_result(result);
  check_mysql_rc(mysql_query(mysql, "drop view v1"), mysql)
  check_mysql_rc(mysql_query(mysql, "drop table t1"), mysql)
  return OK;
}

/*
  Bug#21726: Incorrect result with multiple invocations of
  LAST_INSERT_ID

  Test that client gets updated value of insert_id on UPDATE that uses
  LAST_INSERT_ID(expr).
  select_query added to test for bug
    #26921 Problem in mysql_insert_id() Embedded C API function
*/
static int test_bug21726(MYSQL *mysql)
{
  const char *create_table[]=
  {
    "DROP TABLE IF EXISTS t1",
    "CREATE TABLE t1 (i INT)",
    "INSERT INTO t1 VALUES (1)",
  };
  const char *update_query= "UPDATE t1 SET i= LAST_INSERT_ID(i + 1)";
  int rc;
  unsigned long long insert_id;
  const char *select_query= "SELECT * FROM t1";
  MYSQL_RES  *result;

  rc= mysql_query(mysql, create_table[0]);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, create_table[1]);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, create_table[2]);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, update_query);
  check_mysql_rc(rc, mysql)
  insert_id= mysql_insert_id(mysql);
  FAIL_UNLESS(insert_id == 2, "insert_id != 2");

  rc= mysql_query(mysql, update_query);
  check_mysql_rc(rc, mysql)
  insert_id= mysql_insert_id(mysql);
  FAIL_UNLESS(insert_id == 3, "insert_id != 3");

  rc= mysql_query(mysql, select_query);
  check_mysql_rc(rc, mysql)
  insert_id= mysql_insert_id(mysql);
  FAIL_UNLESS(insert_id == 3, "insert_id != 3");
  result= mysql_store_result(mysql);
  mysql_free_result(result);

  return OK;
}

/* Bug#6761 - mysql_list_fields doesn't work */

static int test_bug6761(MYSQL *mysql)
{
  const char *stmt_text;
  MYSQL_RES *res;
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  stmt_text= "CREATE TABLE t1 (a int, b char(255), c decimal)";
  rc= mysql_real_query(mysql, stmt_text, (unsigned long)strlen(stmt_text));
  check_mysql_rc(rc, mysql);

  res= mysql_list_fields(mysql, "t1", "%");
  FAIL_UNLESS(res && mysql_num_fields(res) == 3, "num_fields != 3");
  mysql_free_result(res);

  stmt_text= "DROP TABLE t1";
  rc= mysql_real_query(mysql, stmt_text, (unsigned long)strlen(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test field flags (verify .NET provider) */

static int test_field_flags(MYSQL *mysql)
{
  int          rc;
  MYSQL_RES    *result;
  MYSQL_FIELD  *field;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_flags");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_field_flags(id int NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                                                        id1 int NOT NULL, \
                                                        id2 int UNIQUE, \
                                                        id3 int, \
                                                        id4 int NOT NULL, \
                                                        id5 int, \
                                                        KEY(id3, id4))");
  check_mysql_rc(rc, mysql);

  /* with table name included with TRUE column name */
  rc= mysql_query(mysql, "SELECT * FROM test_field_flags");
  check_mysql_rc(rc, mysql);

  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  mysql_field_seek(result, 0);

  field= mysql_fetch_field(result);
  FAIL_UNLESS(field->flags & NOT_NULL_FLAG && 
              field->flags & PRI_KEY_FLAG &&
              field->flags & AUTO_INCREMENT_FLAG, "Wrong flags for field 0");

  field= mysql_fetch_field(result);
  FAIL_UNLESS(field->flags & NOT_NULL_FLAG, "Wrong flags for field 1");

  field= mysql_fetch_field(result);
  FAIL_UNLESS(field->flags & UNIQUE_KEY_FLAG, "Wrong flags for field 2");

  field= mysql_fetch_field(result);
  FAIL_UNLESS(field->flags & MULTIPLE_KEY_FLAG, "Wrong flags for field 3");

  field= mysql_fetch_field(result);
  FAIL_UNLESS(field->flags & NOT_NULL_FLAG, "Wrong flags for field 4");

  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_flags");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test real and alias names */

static int test_field_names(MYSQL *mysql)
{
  int        rc;
  MYSQL_RES  *result;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_names1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_names2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_field_names1(id int, name varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_field_names2(id int, name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* with table name included with TRUE column name */
  rc= mysql_query(mysql, "SELECT id as 'id-alias' FROM test_field_names1");
  check_mysql_rc(rc, mysql);

  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 0, "rowcount != 0");
  mysql_free_result(result);

  /* with table name included with TRUE column name */
  rc= mysql_query(mysql, "SELECT t1.id as 'id-alias', test_field_names2.name FROM test_field_names1 t1, test_field_names2");
  check_mysql_rc(rc, mysql);

  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 0, "rowcount != 0");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_names1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_field_names2");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test FUNCTION field info / DATE_FORMAT() table_name . */

static int test_func_fields(MYSQL *mysql)
{
  int        rc;
  MYSQL_RES  *result;
  MYSQL_FIELD *field;


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_dateformat");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_dateformat(id int, \
                                                       ts timestamp)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_dateformat(id) values(10)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT ts FROM test_dateformat");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  field= mysql_fetch_field(result);
  FAIL_IF(!field, "Invalid field");
  FAIL_UNLESS(strcmp(field->table, "test_dateformat") == 0, "field->table != 'test_dateformat'");

  field= mysql_fetch_field(result);
  FAIL_IF(field, "no more fields expected");

  mysql_free_result(result);

  /* DATE_FORMAT */
  rc= mysql_query(mysql, "SELECT DATE_FORMAT(ts, '%Y') AS 'venu' FROM test_dateformat");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  field= mysql_fetch_field(result);
  FAIL_IF(!field, "Invalid field");
  FAIL_UNLESS(field->table[0] == '\0', "field->table != ''");

  field= mysql_fetch_field(result);
  FAIL_IF(field, "no more fields expected");

  mysql_free_result(result);

  /* FIELD ALIAS TEST */
  rc= mysql_query(mysql, "SELECT DATE_FORMAT(ts, '%Y')  AS 'YEAR' FROM test_dateformat");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  field= mysql_fetch_field(result);
  FAIL_IF(!field, "Invalid field");
  FAIL_UNLESS(strcmp(field->name, "YEAR") == 0, "name != 'YEAR'");
  FAIL_UNLESS(field->org_name[0] == '\0', "org_name != ''");

  field= mysql_fetch_field(result);
  FAIL_IF(field, "no more fields expected");

  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_dateformat");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test mysql_list_fields() */

static int test_list_fields(MYSQL *mysql)
{
  MYSQL_RES *result;
  int rc;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1(c1 int primary key auto_increment, c2 char(10) default 'mysql')");
  check_mysql_rc(rc, mysql);

  result= mysql_list_fields(mysql, "t1", NULL);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 0, "rowcount != 0");

  if (verify_prepare_field(result, 0, "c1", "c1", MYSQL_TYPE_LONG,
                       "t1", "t1",
                       schema, 11, "0"))
    goto error;

  if (verify_prepare_field(result, 1, "c2", "c2", MYSQL_TYPE_STRING,
                       "t1", "t1",
                       schema, 10, "mysql"))
    goto error;

  mysql_free_result(result);
  check_mysql_rc(mysql_query(mysql, "drop table t1"), mysql);
  return OK;

error:
  mysql_free_result(result);
  check_mysql_rc(mysql_query(mysql, "drop table t1"), mysql);
  return FAIL;
}

/* Test correct max length for MEDIUMTEXT and LONGTEXT columns */

static int test_bug9735(MYSQL *mysql)
{
  MYSQL_RES *res;
  int rc;


  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (a mediumtext, b longtext) "
                         "character set latin1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "select * from t1");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  if (verify_prepare_field(res, 0, "a", "a", MYSQL_TYPE_BLOB,
                       "t1", "t1", schema, (1U << 24)-1, 0)) 
    goto error;
  if (verify_prepare_field(res, 1, "b", "b", MYSQL_TYPE_BLOB,
                       "t1", "t1", schema, ~0U, 0))
    goto error;
  mysql_free_result(res);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
error:
  mysql_free_result(res);
  rc= mysql_query(mysql, "drop table t1");
  return FAIL;
}

/*
  Check that mysql_next_result works properly in case when one of
  the statements used in a multi-statement query is erroneous
*/

static int test_bug9992(MYSQL *mysql)
{
  MYSQL_RES* res ;
  int   rc;

  /* Sic: SHOW DATABASE is incorrect syntax. */
  rc= mysql_query(mysql, "SHOW TABLES; SHOW DATABASE; SELECT 1;");
  check_mysql_rc(rc, mysql);
  
  res= mysql_store_result(mysql);
  FAIL_UNLESS(res, "Invalid resultset");
  mysql_free_result(res);
  rc= mysql_next_result(mysql);
  FAIL_UNLESS(rc == 1, "Error expected");                         /* Got errors, as expected */

  return OK;
}

/* Test the support of multi-statement executions */

static int test_multi_statements(MYSQL *mysql)
{
  MYSQL *mysql_local;
  MYSQL_RES *result;
  int    rc;

  const char *query= "\
DROP TABLE IF EXISTS test_multi_tab;\
CREATE TABLE test_multi_tab(id int, name char(20));\
INSERT INTO test_multi_tab(id) VALUES(10), (20);\
INSERT INTO test_multi_tab VALUES(20, 'insert;comma');\
SELECT * FROM test_multi_tab;\
UPDATE test_multi_tab SET name='new;name' WHERE id=20;\
DELETE FROM test_multi_tab WHERE name='new;name';\
SELECT * FROM test_multi_tab;\
DELETE FROM test_multi_tab WHERE id=10;\
SELECT * FROM test_multi_tab;\
DROP TABLE test_multi_tab;\
select 1;\
DROP TABLE IF EXISTS test_multi_tab";
  uint count, exp_value;
  uint rows[]= {0, 0, 2, 1, 3, 2, 2, 1, 1, 0, 0, 1, 0};
  my_bool reconnect= 1;

  /*
    First test that we get an error for multi statements
    (Because default connection is not opened with CLIENT_MULTI_STATEMENTS)
  */
  mysql_local= mysql;
  mysql = test_connect(NULL);
  rc= mysql_query(mysql, query); /* syntax error */
  FAIL_IF(!rc, "Error expected");

  rc= mysql_next_result(mysql);
  FAIL_UNLESS(rc == -1, "rc != -1");
  rc= mysql_more_results(mysql);
  FAIL_UNLESS(rc == 0, "rc != 0");

  mysql_close(mysql);
  mysql= mysql_local;

  mysql_options(mysql_local, MYSQL_OPT_RECONNECT, &reconnect);

  rc= mysql_query(mysql_local, query);
  check_mysql_rc(rc, mysql);

  for (count= 0 ; count < array_elements(rows) ; count++)
  {
    if ((result= mysql_store_result(mysql_local)))
    {
      mysql_free_result(result);
    }

    exp_value= (uint) mysql_affected_rows(mysql_local);
    FAIL_IF(rows[count] !=  exp_value, "row[count] != exp_value");
    if (count != array_elements(rows) -1)
    {
      rc= mysql_more_results(mysql_local);
      FAIL_IF(!rc, "More results expected");
      rc= mysql_next_result(mysql_local);
      check_mysql_rc(rc, mysql_local);
    }
    else
    {
      rc= mysql_more_results(mysql_local);
      FAIL_UNLESS(rc == 0, "rc != 0");
      rc= mysql_next_result(mysql_local);
      FAIL_UNLESS(rc == -1, "rc != -1");
    }
  }

  /* check that errors abort multi statements */

  rc= mysql_query(mysql_local, "select 1+1+a;select 1+1");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_more_results(mysql_local);
  FAIL_UNLESS(rc == 0, "rc != 0");
  rc= mysql_next_result(mysql_local);
  FAIL_UNLESS(rc == -1, "rc != -1");

  rc= mysql_query(mysql_local, "select 1+1;select 1+1+a;select 1");
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql_local);
  FAIL_IF(!result, "Invalid result set");
  mysql_free_result(result);
  rc= mysql_more_results(mysql_local);
  FAIL_UNLESS(rc == 1, "rc != 1");
  rc= mysql_next_result(mysql_local);
  FAIL_UNLESS(rc > 0, "rc <= 0");

  /*
    Ensure that we can now do a simple query (this checks that the server is
    not trying to send us the results for the last 'select 1'
  */
  rc= mysql_query(mysql_local, "select 1+1+1");
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql_local);
  FAIL_IF(!result, "Invalid result set");
  mysql_free_result(result);

  /*
    Check if errors in one of the queries handled properly.
  */
  rc= mysql_query(mysql_local, "select 1; select * from not_existing_table");
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql_local);
  mysql_free_result(result);

  rc= mysql_next_result(mysql_local);
  FAIL_UNLESS(rc > 0, "rc <= 0");

  rc= mysql_next_result(mysql_local);
  FAIL_UNLESS(rc < 0, "rc >= 0");

  return OK;
}

static int test_conc160(MYSQL *mysql)
{
  MYSQL_RES *result;
  MYSQL_FIELD *field;
  int rc;

  rc= mysql_query(mysql, "SELECT cast(1.234 AS DECIMAL)");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  field= mysql_fetch_field(result);

  FAIL_UNLESS(field->flags & NUM_FLAG, "Numceric flag not set");

  mysql_free_result(result);
  return OK;
}



struct my_tests_st my_tests[] = {
  {"test_conc160", test_conc160, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"client_store_result", client_store_result, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"client_use_result", client_use_result, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_free_result", test_free_result, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_free_store_result", test_free_store_result, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_store_result", test_store_result, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_store_result1", test_store_result1, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_store_result2", test_store_result2, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug11718", test_bug11718, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug19671", test_bug19671, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug21726", test_bug21726, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug6761", test_bug6761, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_field_flags", test_field_flags, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_field_names", test_field_names, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_func_fields", test_func_fields, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_list_fields", test_list_fields, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug9735", test_bug9735, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug9992", test_bug9992, TEST_CONNECTION_NEW, CLIENT_MULTI_STATEMENTS,  NULL,  NULL},
  {"test_multi_statements", test_multi_statements, TEST_CONNECTION_NEW, CLIENT_MULTI_STATEMENTS,  NULL,  NULL},
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
