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

/* Utility function to verify the field members */

static int test_conc97(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;

  diag("Please run this test manually");
  return SKIP;
  stmt= mysql_stmt_init(mysql);

  mysql_close(mysql);

  rc= mysql_stmt_reset(stmt);
  FAIL_IF(!rc, "Error expected while resetting stmt");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  mysql= mysql_init(NULL);

  return OK;
}

static int test_conc83(MYSQL *unused __attribute__((unused)))
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL *mysql= mysql_init(NULL);
  my_bool reconnect= 1;

  const char *query= "SELECT 1,2,3 FROM DUAL";

  stmt= mysql_stmt_init(mysql);

  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);
  FAIL_IF(!(my_test_connect(mysql, hostname, username, password,
                           schema, port, socketname, 0)), "my_test_connect failed");

  /* 1. Status is inited, so prepare should work */

  rc= mysql_kill(mysql, mysql_thread_id(mysql));

  rc= mysql_ping(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  diag("Ok");

  /* 2. Status is prepared, execute should fail */
  rc= mysql_kill(mysql, mysql_thread_id(mysql));

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected"); 

  mysql_stmt_close(stmt);
  mysql_close(mysql);
  return OK;
}


static int test_conc60(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query= "SELECT * FROM agendas";
  my_bool x= 1;

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, (void *)&x);

  rc= mysql_stmt_prepare(stmt, SL(query));
  if (rc && mysql_stmt_errno(stmt) == 1146) {
    diag("Internal test - customer data not available");
    mysql_stmt_close(stmt);
    return SKIP;
  }
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_free_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  return OK;
}

static int test_prepare_insert_update(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  int        i;
  const char *testcase[]= {
    "CREATE TABLE t1 (a INT, b INT, c INT, UNIQUE (A), UNIQUE(B))",
    "INSERT t1 VALUES (1,2,10), (3,4,20)",
    "INSERT t1 VALUES (5,6,30), (7,4,40), (8,9,60) ON DUPLICATE KEY UPDATE c=c+100",
    "SELECT * FROM t1",
    "INSERT t1 SET a=5 ON DUPLICATE KEY UPDATE b=0",
    "SELECT * FROM t1",
    "INSERT t1 VALUES (2,1,11), (7,4,40) ON DUPLICATE KEY UPDATE c=c+VALUES(a)",
    NULL};
  const char **cur_query;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  for (cur_query= testcase; *cur_query; cur_query++)
  {
    char query[MAX_TEST_QUERY_LENGTH];
    strcpy(query, *cur_query);
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    FAIL_IF(mysql_stmt_param_count(stmt) != 0, "Paramcount is not 0");
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    /* try the last query several times */
    if (!cur_query[1])
    {
      for (i=0; i < 3;i++)
      {
        rc= mysql_stmt_execute(stmt);
        check_stmt_rc(rc, stmt);
        rc= mysql_stmt_execute(stmt);
        check_stmt_rc(rc, stmt);
      }
    }
    mysql_stmt_close(stmt);
  }

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Generalized conversion routine to handle DATE, TIME and DATETIME
  conversion using MYSQL_TIME structure
*/

static int test_bind_date_conv(MYSQL *mysql, uint row_count)
{
  MYSQL_STMT   *stmt= 0;
  uint         rc, i, count= row_count;
  MYSQL_BIND   my_bind[4];
  my_bool      is_null[4]= {0,0,0,0};
  MYSQL_TIME   tm[4];
  ulong        second_part;
  uint         year, month, day, hour, minute, sec;

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO test_date VALUES(?, ?, ?, ?)"));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 4, "param_count != 4");

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_TIMESTAMP;
  my_bind[1].buffer_type= MYSQL_TYPE_TIME;
  my_bind[2].buffer_type= MYSQL_TYPE_DATETIME;
  my_bind[3].buffer_type= MYSQL_TYPE_DATETIME;

  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].buffer= (void *) &tm[i];
    my_bind[i].is_null= &is_null[i];
    my_bind[i].buffer_length= sizeof(MYSQL_TIME);
  }

  second_part= 0;

  year= 2000;
  month= 01;
  day= 10;

  hour= 11;
  minute= 16;
  sec= 20;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (count= 0; count < row_count; count++)
  {
    for (i= 0; i < (int) array_elements(my_bind); i++)
    {
      memset(&tm[i],  0, sizeof(MYSQL_TIME));
      tm[i].neg= 0;
      tm[i].second_part= second_part+count;
      if (my_bind[i].buffer_type != MYSQL_TYPE_TIME)
      {
        tm[i].year= year+count;
        tm[i].month= month+count;
        tm[i].day= day+count;
      }
      else
        tm[i].year= tm[i].month= tm[i].day= 0;
      if (my_bind[i].buffer_type != MYSQL_TYPE_DATE)
      {
        tm[i].hour= hour+count;
        tm[i].minute= minute+count;
        tm[i].second= sec+count;
      }
      else
        tm[i].hour= tm[i].minute= tm[i].second= 0;
    }
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  mysql_stmt_close(stmt);

  rc= my_stmt_result(mysql, "SELECT * FROM test_date");
  FAIL_UNLESS(row_count == rc, "rowcount != rc");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_date"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  for (count= 0; count < row_count; count++)
  {
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc == 0 || rc == MYSQL_DATA_TRUNCATED, "rc != 0 | rc != MYSQL_DATA_TRUNCATED");

    for (i= 0; i < array_elements(my_bind); i++)
    {
      FAIL_UNLESS(tm[i].year == 0 || tm[i].year == year+count, "wrong value for year");
      FAIL_UNLESS(tm[i].month == 0 || tm[i].month == month+count, "wrong value for month");
      FAIL_UNLESS(tm[i].day == 0 || tm[i].day == day+count, "wrong value for day");
      FAIL_UNLESS(tm[i].hour == 0 || tm[i].hour % 24 == 0 || tm[i].hour % 24 == hour+count, "wrong value for hour");
      FAIL_UNLESS(tm[i].minute == 0 || tm[i].minute == minute+count, "wrong value for minute");
      FAIL_UNLESS(tm[i].second == 0 || tm[i].second == sec+count, "wrong value for second");
      FAIL_UNLESS(tm[i].second_part == 0 ||
                 tm[i].second_part == second_part+count, "wrong value for second_part");
    }
  }
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  return OK;
}


/* Test simple prepares of all DML statements */

static int test_prepare_simple(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_simple");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prepare_simple("
                         "id int, name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert */
  strcpy(query, "INSERT INTO test_prepare_simple VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount is not 2");
  mysql_stmt_close(stmt);

  /* update */
  strcpy(query, "UPDATE test_prepare_simple SET id=? "
                "WHERE id=? AND CONVERT(name USING utf8)= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "Paramcount is not 3");
  mysql_stmt_close(stmt);

  /* delete */
  strcpy(query, "DELETE FROM test_prepare_simple WHERE id=10");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 0, "Paramcount is not 0");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  /* delete */
  strcpy(query, "DELETE FROM test_prepare_simple WHERE id=?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "Paramcount != 1");

  mysql_stmt_close(stmt);

  /* select */
  strcpy(query, "SELECT * FROM test_prepare_simple WHERE id=? "
                "AND CONVERT(name USING utf8)= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_simple");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_prepare_field_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES  *result;
  int        rc;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_field_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prepare_field_result(int_c int, "
                         "var_c varchar(50), ts_c timestamp, "
                         "char_c char(4), date_c date, extra tinyint)");
  check_mysql_rc(rc, mysql);

  /* insert */
  strcpy(query, "SELECT int_c, var_c, date_c as date, ts_c, char_c FROM "
                " test_prepare_field_result as t1 WHERE int_c=?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "Paramcount != 1");

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, mysql_stmt_error(stmt));

  if (verify_prepare_field(result, 0, "int_c", "int_c", MYSQL_TYPE_LONG,
                       "t1", "test_prepare_field_result", schema, 11, 0))
    goto error;
  if (verify_prepare_field(result, 1, "var_c", "var_c", MYSQL_TYPE_VAR_STRING,
                       "t1", "test_prepare_field_result", schema, 50, 0))
    goto error;
  if (verify_prepare_field(result, 2, "date", "date_c", MYSQL_TYPE_DATE,
                       "t1", "test_prepare_field_result", schema, 10, 0))
    goto error;
  if (verify_prepare_field(result, 3, "ts_c", "ts_c", MYSQL_TYPE_TIMESTAMP,
                       "t1", "test_prepare_field_result", schema, 19, 0))
    goto error;
  if (verify_prepare_field(result, 4, "char_c", "char_c",
                       (mysql_get_server_version(mysql) <= 50000 ?
                        MYSQL_TYPE_VAR_STRING : MYSQL_TYPE_STRING),
                       "t1", "test_prepare_field_result", schema, 4, 0))
    goto error;

  FAIL_IF(mysql_num_fields(result) != 5, "Paramcount != 5");
  mysql_free_result(result);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_field_result");
  check_mysql_rc(rc, mysql);

  return OK;

error:
  mysql_free_result(result);
  mysql_stmt_close(stmt);
  return FAIL;
}


/* Test simple prepare field results */

static int test_prepare_syntax(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_syntax");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prepare_syntax("
                         "id int, name varchar(50), extra int)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_prepare_syntax VALUES(?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "error expected");

  strcpy(query, "SELECT id, name FROM test_prepare_syntax WHERE id=? AND WHERE");
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "error expected");

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_syntax");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_prepare(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, i;
  int        int_data, o_int_data;
  char       str_data[50], data[50];
  char       tiny_data, o_tiny_data;
  short      small_data, o_small_data;
  longlong   big_data, o_big_data;
  float      real_data, o_real_data;
  double     double_data, o_double_data;
  ulong      length[7], len;
  my_bool    is_null[7];
  MYSQL_BIND my_bind[7];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS my_prepare");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE my_prepare(col1 tinyint, "
                         "col2 varchar(15), col3 int, "
                         "col4 smallint, col5 bigint, "
                         "col6 float, col7 double )");
  check_mysql_rc(rc, mysql);

  /* insert by prepare */
  strcpy(query, "INSERT INTO my_prepare VALUES(?, ?, ?, ?, ?, ?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 7, "Paramcount != 7");

  memset(my_bind, '\0', sizeof(my_bind));

  /* tinyint */
  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&tiny_data;
  /* string */
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)str_data;
  my_bind[1].buffer_length= 1000;                  /* Max string length */
  /* integer */
  my_bind[2].buffer_type= MYSQL_TYPE_LONG;
  my_bind[2].buffer= (void *)&int_data;
  /* short */
  my_bind[3].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[3].buffer= (void *)&small_data;
  /* bigint */
  my_bind[4].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[4].buffer= (void *)&big_data;
  /* float */
  my_bind[5].buffer_type= MYSQL_TYPE_FLOAT;
  my_bind[5].buffer= (void *)&real_data;
  /* double */
  my_bind[6].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[6].buffer= (void *)&double_data;

  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].length= &length[i];
    my_bind[i].is_null= &is_null[i];
    is_null[i]= 0;
  }

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  int_data= 320;
  small_data= 1867;
  big_data= 1000;
  real_data= 2;
  double_data= 6578.001;

  /* now, execute the prepared statement to insert 10 records.. */
  for (tiny_data= 0; tiny_data < 100; tiny_data++)
  {
    length[1]= sprintf(str_data, "MySQL%d", int_data);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    int_data += 25;
    small_data += 10;
    big_data += 100;
    real_data += 1;
    double_data += 10.09;
  }

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= my_stmt_result(mysql, "SELECT * FROM my_prepare");
  FAIL_UNLESS(rc != 1, "rowcount != 1");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, "SELECT * FROM my_prepare", 25);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  /* get the result */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  o_int_data= 320;
  o_small_data= 1867;
  o_big_data= 1000;
  o_real_data= 2;
  o_double_data= 6578.001;

  /* now, execute the prepared statement to insert 10 records.. */
  for (o_tiny_data= 0; o_tiny_data < 100; o_tiny_data++)
  {
    len= sprintf(data, "MySQL%d", o_int_data);

    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    FAIL_UNLESS(tiny_data == o_tiny_data, "Wrong value for tiny_data");
    FAIL_UNLESS(is_null[0] == 0, "Wrong value for is_null");
    FAIL_UNLESS(length[0] == 1, "length != 0");

    FAIL_UNLESS(int_data == o_int_data, "Wrong value for int_data");
    FAIL_UNLESS(length[2] == 4, "length != 4");

    FAIL_UNLESS(small_data == o_small_data, "Wrong value for small_data");
    FAIL_UNLESS(length[3] == 2, "length != 2");

    FAIL_UNLESS(big_data == o_big_data, "Wrong value for big_data");
    FAIL_UNLESS(length[4] == 8, "length != 8");

    FAIL_UNLESS(real_data == o_real_data, "Wrong value for real_data");
    FAIL_UNLESS(length[5] == 4, "length != 4");

    FAIL_UNLESS(double_data == o_double_data, "Wrong value for double_data");
    FAIL_UNLESS(length[6] == 8, "length != 8");

    FAIL_UNLESS(strcmp(data, str_data) == 0, "Wrong value for data");
    FAIL_UNLESS(length[1] == len, "length != len");

    o_int_data += 25;
    o_small_data += 10;
    o_big_data += 100;
    o_real_data += 1;
    o_double_data += 10.09;
  }

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "MYSQL_NO_DATA expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS my_prepare");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_prepare_multi_statements(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  char query[MAX_TEST_QUERY_LENGTH];
  int rc;

  strcpy(query, "select 1; select 'another value'");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);

  return OK;
}

static int test_prepare_ext(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       *sql;
  int        nData= 1;
  char       tData= 1;
  short      sData= 10;
  longlong   bData= 20;
  int        rowcount= 0;
  MYSQL_BIND my_bind[6];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_ext");
  check_mysql_rc(rc, mysql)

  sql= (char *)"CREATE TABLE test_prepare_ext"
               "("
               " c1  tinyint,"
               " c2  smallint,"
               " c3  mediumint,"
               " c4  int,"
               " c5  integer,"
               " c6  bigint,"
               " c7  float,"
               " c8  double,"
               " c9  double precision,"
               " c10 real,"
               " c11 decimal(7, 4),"
               " c12 numeric(8, 4),"
               " c13 date,"
               " c14 datetime,"
               " c15 timestamp,"
               " c16 time,"
               " c17 year,"
               " c18 bit,"
               " c19 bool,"
               " c20 char,"
               " c21 char(10),"
               " c22 varchar(30),"
               " c23 tinyblob,"
               " c24 tinytext,"
               " c25 blob,"
               " c26 text,"
               " c27 mediumblob,"
               " c28 mediumtext,"
               " c29 longblob,"
               " c30 longtext,"
               " c31 enum('one', 'two', 'three'),"
               " c32 set('monday', 'tuesday', 'wednesday'))";

  rc= mysql_query(mysql, sql);
  check_mysql_rc(rc, mysql)

  /* insert by prepare - all integers */
  strcpy(query, "INSERT INTO test_prepare_ext(c1, c2, c3, c4, c5, c6) VALUES(?, ?, ?, ?, ?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 6, "Paramcount != 6");

  memset(my_bind, '\0', sizeof(my_bind));

  /*tinyint*/
  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&tData;

  /*smallint*/
  my_bind[1].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[1].buffer= (void *)&sData;

  /*mediumint*/
  my_bind[2].buffer_type= MYSQL_TYPE_LONG;
  my_bind[2].buffer= (void *)&nData;

  /*int*/
  my_bind[3].buffer_type= MYSQL_TYPE_LONG;
  my_bind[3].buffer= (void *)&nData;

  /*integer*/
  my_bind[4].buffer_type= MYSQL_TYPE_LONG;
  my_bind[4].buffer= (void *)&nData;

  /*bigint*/
  my_bind[5].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[5].buffer= (void *)&bData;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  /*
  *  integer to integer
  */
  for (nData= 0; nData<10; nData++, tData++, sData++, bData++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }
  mysql_stmt_close(stmt);

  /* now fetch the results ..*/

  strcpy(query, "SELECT c1, c2, c3, c4, c5, c6 FROM test_prepare_ext");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  /* get the result */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;

  FAIL_UNLESS(nData == rowcount, "Invalid rowcount");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_ext");
  check_mysql_rc(rc, mysql)

  return OK;
}

static int test_prepare_alter(MYSQL *mysql)
{
  MYSQL_STMT  *stmt;
  MYSQL       *mysql_new;
  int         rc, id;
  MYSQL_BIND  my_bind[1];
  my_bool     is_null;
  char        query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prep_alter");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prep_alter(id int, name char(20))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_prep_alter values(10, 'venu'), (20, 'mysql')");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_prep_alter VALUES(?, 'monty')");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "Paramcount != 1");

  memset(my_bind, '\0', sizeof(my_bind));

  is_null= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[0].buffer= (void *)&id;
  my_bind[0].is_null= &is_null;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  id= 30;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_new= mysql_init(NULL);
  FAIL_IF(!mysql_new, "mysql_init failed");
  FAIL_IF(!(my_test_connect(mysql_new, hostname, username, password,
                           schema, port, socketname, 0)), "my_test_connect failed");
  rc= mysql_query(mysql_new, "ALTER TABLE test_prep_alter change id id_new varchar(20)");
  diag("Error: %d %s", mysql_errno(mysql_new), mysql_error(mysql_new));
  check_mysql_rc(rc, mysql_new);
  mysql_close(mysql_new);

  is_null= 1;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= my_stmt_result(mysql, "SELECT * FROM test_prep_alter");
  FAIL_UNLESS(rc == 4, "rowcount != 4");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prep_alter");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_prepare_resultset(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  MYSQL_RES  *result;
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_resultset");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prepare_resultset(id int, \
                                name varchar(50), extra double)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  strcpy(query, "SELECT * FROM test_prepare_resultset");
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt), "Paramcount != 0");

  result= mysql_stmt_result_metadata(stmt);
  FAIL_IF(!result, "Invalid resultset");
  mysql_free_result(result);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prepare_resultset");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test the direct query execution in the middle of open stmts */

static int test_open_direct(MYSQL *mysql)
{
  MYSQL_STMT  *stmt;
  MYSQL_RES   *result;
  int         rc;
  char        query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_open_direct");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_open_direct(id int, name char(6))");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_open_direct values(10, 'mysql')");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "SELECT * FROM test_open_direct");

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "invalid resultset");

  FAIL_IF(mysql_num_rows(result), "rowcount != 0");
  mysql_free_result(result);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "affected rows != 1");

  rc= mysql_query(mysql, "SELECT * FROM test_open_direct");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "invalid resultset");

  FAIL_IF(mysql_num_rows(result) != 1, "rowcount != 1");
  mysql_free_result(result);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "affected rows != 1");

  rc= mysql_query(mysql, "SELECT * FROM test_open_direct");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid resultset");
  FAIL_IF(mysql_num_rows(result) != 2, "rowcount != 2");

  mysql_free_result(result);

  mysql_stmt_close(stmt);

  /* run a direct query in the middle of a fetch */
 
  strcpy(query, "SELECT * FROM test_open_direct");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "INSERT INTO test_open_direct(id) VALUES(20)");
  FAIL_IF(!rc, "Error expected");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "INSERT INTO test_open_direct(id) VALUES(20)");
  check_mysql_rc(rc, mysql);

  /* run a direct query with store result */
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "drop table test_open_direct");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  return OK;
}

static int test_select_show(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char query[MAX_TEST_QUERY_LENGTH];
  int        rowcount;

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_show");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_show(id int(4) NOT NULL primary "
                         " key, name char(2))");
  check_mysql_rc(rc, mysql);

  strcpy(query, "show columns from test_show");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 0, "Paramcount != 0");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 2, "rowcount != 2");

  mysql_stmt_close(stmt);

  strcpy(query, "show tables from mysql like ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "Error expected");

  strcpy(query, "show tables like \'test_show\'");
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_stmt_close(stmt);

  strcpy(query, "describe test_show");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 2, "rowcount != 2");
  mysql_stmt_close(stmt);

  strcpy(query, "show keys from test_show");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rowcount= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_show");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_simple_update(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       szData[25];
  int        nData= 1;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  ulong      length[2];
  int        rowcount= 0;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_update");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_update(col1 int, "
                         " col2 varchar(50), col3 int )");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_update VALUES(1, 'MySQL', 100)");
  check_mysql_rc(rc, mysql);

  FAIL_IF(mysql_affected_rows(mysql) != 1, "Affected rows != 1");

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* insert by prepare */
  strcpy(query, "UPDATE test_update SET col2= ? WHERE col1= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  nData= 1;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= szData;                /* string data */
  my_bind[0].buffer_length= sizeof(szData);
  my_bind[0].length= &length[0];
  length[0]= sprintf(szData, "updated-data");

  my_bind[1].buffer= (void *) &nData;
  my_bind[1].buffer_type= MYSQL_TYPE_LONG;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "Affected_rows != 1");

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM test_update");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid resultset");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 1, "rowcount != 1");

  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_update");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Test simple long data handling */

static int test_long_data(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, int_data;
  char       *data= NullS;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[3];
  int        rowcount;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_long_data(col1 int, "
                         "      col2 long varchar, col3 long varbinary)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_long_data(col1, col2) VALUES(?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "Error expected");
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  strcpy(query, "INSERT INTO test_long_data(col1, col2, col3) VALUES(?, ?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "Paramcount != 3");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)&int_data;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;

  my_bind[2]= my_bind[1];
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  int_data= 999;
  data= (char *)"Michael";

  /* supply data in pieces */
  rc= mysql_stmt_send_long_data(stmt, 1, SL(data));
  check_stmt_rc(rc, stmt);
  data= (char *)" 'Monty' Widenius";
  rc= mysql_stmt_send_long_data(stmt, 1, SL(data));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_send_long_data(stmt, 2, "Venu (venu@mysql.com)", 4);
  check_stmt_rc(rc, stmt);

  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* now fetch the results ..*/
  rc= mysql_query(mysql, "SELECT * FROM test_long_data");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rowcount= 0;
  while (mysql_fetch_row(result))
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_free_result(result);

  if (verify_col_data(mysql, "test_long_data", "col1", "999"))
    goto error;
  if (verify_col_data(mysql, "test_long_data", "col2", "Michael 'Monty' Widenius"))
    goto error;
  if (verify_col_data(mysql, "test_long_data", "col3", "Venu"))
    goto error;

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data");
  check_mysql_rc(rc, mysql);
  return OK;

error:
  mysql_stmt_close(stmt);
  return FAIL;
}


/* Test long data (string) handling */

static int test_long_data_str(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, i, rowcount= 0;
  char       data[255];
  long       length;
  ulong      length1;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  my_bool    is_null[2];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data_str");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_long_data_str(id int, longstr long varchar)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_long_data_str VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)&length;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].is_null= &is_null[0];
  is_null[0]= 0;
  length= 0;

  my_bind[1].buffer= data;                          /* string data */
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].length= &length1;
  my_bind[1].is_null= &is_null[1];
  is_null[1]= 0;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  length= 40;
  strcpy(data, "MySQL AB");

  /* supply data in pieces */
  for(i= 0; i < 4; i++)
  {
    rc= mysql_stmt_send_long_data(stmt, 1, (char *)data, 5);
    check_stmt_rc(rc, stmt);
  }
  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* now fetch the results ..*/
  rc= mysql_query(mysql, "SELECT LENGTH(longstr), longstr FROM test_long_data_str");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");

  mysql_free_result(result);

  sprintf(data, "%d", i*5);
  if (verify_col_data(mysql, "test_long_data_str", "LENGTH(longstr)", data))
    goto error;
  strcpy(data, "MySQLMySQLMySQLMySQL");
  if (verify_col_data(mysql, "test_long_data_str", "longstr", data))
    goto error;

  rc= mysql_query(mysql, "DROP TABLE test_long_data_str");
  check_mysql_rc(rc, mysql);

  return OK;

error:
  rc= mysql_query(mysql, "DROP TABLE test_long_data_str");
  check_mysql_rc(rc, mysql);
  return FAIL; 
}


/* Test long data (string) handling */

static int test_long_data_str1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, i, rowcount= 0;
  char       data[255];
  long       length;
  unsigned long max_blob_length, blob_length, length1;
  my_bool    true_value;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  MYSQL_FIELD *field;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data_str");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_long_data_str(longstr long varchar, blb long varbinary)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_long_data_str VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= data;            /* string data */
  my_bind[0].buffer_length= sizeof(data);
  my_bind[0].length= (unsigned long *)&length1;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  length1= 0;

  my_bind[1]= my_bind[0];
  my_bind[1].buffer_type= MYSQL_TYPE_BLOB;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  length= sprintf(data, "MySQL AB");

  /* supply data in pieces */
  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_send_long_data(stmt, 0, data, length);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_send_long_data(stmt, 1, data, 2);
    check_stmt_rc(rc, stmt);
  }

  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* now fetch the results ..*/
  rc= mysql_query(mysql, "SELECT LENGTH(longstr), longstr, LENGTH(blb), blb FROM test_long_data_str");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);

  mysql_field_seek(result, 1);
  field= mysql_fetch_field(result);
  max_blob_length= field->max_length;

  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_free_result(result);

  sprintf(data, "%ld", (long)i*length);
  if (verify_col_data(mysql, "test_long_data_str", "length(longstr)", data))
    return FAIL;

  sprintf(data, "%d", i*2);
  if (verify_col_data(mysql, "test_long_data_str", "length(blb)", data))
    return FAIL;

  /* Test length of field->max_length */
  strcpy(query, "SELECT * from test_long_data_str");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 0, "Paramcount != 0");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_fields(result);

  /* First test what happens if STMT_ATTR_UPDATE_MAX_LENGTH is not used */
  FAIL_IF(field->max_length != 0, "field->max_length != 0");
  mysql_free_result(result);

  /* Enable updating of field->max_length */
  true_value= 1;
  mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, (void*) &true_value);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  result= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_fields(result);

  diag("max_length: %lu  max_blob_length: %lu", (unsigned long)field->max_length, (unsigned long)max_blob_length);
  FAIL_UNLESS(field->max_length == max_blob_length, "field->max_length != max_blob_length");

  /* Fetch results into a data buffer that is smaller than data */
  memset(my_bind, '\0', sizeof(*my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_BLOB;
  my_bind[0].buffer= (void *) &data; /* this buffer won't be altered */
  my_bind[0].buffer_length= 16;
  my_bind[0].length= (unsigned long *)&blob_length;
  my_bind[0].error= &my_bind[0].error_value;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  data[16]= 0;

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_DATA_TRUNCATED, "truncation expected");
  FAIL_UNLESS(my_bind[0].error_value, "No error value");
  FAIL_UNLESS(strlen(data) == 16, "Invalid string length");
  FAIL_UNLESS(blob_length == max_blob_length, "blob_length != max_blob_length");

  /* Fetch all data */
  memset((my_bind+1), '\0', sizeof(*my_bind));
  my_bind[1].buffer_type= MYSQL_TYPE_BLOB;
  my_bind[1].buffer= (void *) &data; /* this buffer won't be altered */
  my_bind[1].buffer_length= sizeof(data);
  my_bind[1].length= (unsigned long *)&blob_length;
  memset(data, '\0', sizeof(data));
  mysql_stmt_fetch_column(stmt, my_bind+1, 0, 0);
  FAIL_UNLESS(strlen(data) == max_blob_length, "strlen(data) != max_blob_length");

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  /* Drop created table */
  rc= mysql_query(mysql, "DROP TABLE test_long_data_str");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Test long data (binary) handling */

static int test_long_data_bin(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, rowcount= 0;
  char       data[255];
  long       length;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data_bin");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_long_data_bin(id int, longbin long varbinary)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_long_data_bin VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)&length;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  length= 0;

  my_bind[1].buffer= data;           /* string data */
  my_bind[1].buffer_type= MYSQL_TYPE_LONG_BLOB;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  length= 10;
  strcpy(data, "MySQL AB");

  /* supply data in pieces */
  {
    int i;
    for (i= 0; i < 100; i++)
    {
      rc= mysql_stmt_send_long_data(stmt, 1, (char *)data, 4);
      check_stmt_rc(rc, stmt);
    }
  }
  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* now fetch the results ..*/
  rc= mysql_query(mysql, "SELECT LENGTH(longbin), longbin FROM test_long_data_bin");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_free_result(result);
 
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_long_data_bin");
  check_mysql_rc(rc, mysql);
  return OK;
}


/* Test simple delete */

static int test_simple_delete(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, rowcount= 0;
  char       szData[30]= {0};
  int        nData= 1;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  ulong length[2];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_simple_delete");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_simple_delete(col1 int, \
                                col2 varchar(50), col3 int )");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_simple_delete VALUES(1, 'MySQL', 100)");
  check_mysql_rc(rc, mysql);

  FAIL_IF(mysql_affected_rows(mysql) != 1, "Affected rows != 1");

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* insert by prepare */
  strcpy(query, "DELETE FROM test_simple_delete WHERE col1= ? AND "
                "CONVERT(col2 USING utf8)= ? AND col3= 100");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt)

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  nData= 1;
  strcpy(szData, "MySQL");
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= szData;               /* string data */
  my_bind[1].buffer_length= sizeof(szData);
  my_bind[1].length= &length[1];
  length[1]= 5;

  my_bind[0].buffer= (void *)&nData;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "Affected rows != 1");

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM test_simple_delete");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount, "rowcount > 0");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_simple_delete");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_update(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       szData[25];
  int        nData= 1, rowcount= 0;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  ulong length[2];
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_update");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_update("
                               "col1 int primary key auto_increment, "
                               "col2 varchar(50), col3 int )");
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_update(col2, col3) VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  /* string data */
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= szData;
  my_bind[0].buffer_length= sizeof(szData);
  my_bind[0].length= &length[0];
  length[0]= sprintf(szData, "inserted-data");

  my_bind[1].buffer= (void *)&nData;
  my_bind[1].buffer_type= MYSQL_TYPE_LONG;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  nData= 100;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "Affected rows != 1");
  mysql_stmt_close(stmt);

  strcpy(query, "UPDATE test_update SET col2= ? WHERE col3= ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Paramcount != 2");
  nData= 100;

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= szData;
  my_bind[0].buffer_length= sizeof(szData);
  my_bind[0].length= &length[0];
  length[0]= sprintf(szData, "updated-data");

  my_bind[1].buffer= (void *)&nData;
  my_bind[1].buffer_type= MYSQL_TYPE_LONG;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(mysql_stmt_affected_rows(stmt) != 1, "Affected rows != 1");


  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM test_update");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;
  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_update");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Test prepare without parameters */

static int test_prepare_noparam(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, rowcount= 0;
  MYSQL_RES  *result;
  char query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS my_prepare");
  check_mysql_rc(rc, mysql);


  rc= mysql_query(mysql, "CREATE TABLE my_prepare(col1 int, col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert by prepare */
  strcpy(query, "INSERT INTO my_prepare VALUES(10, 'venu')");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 0, "Paramcount != 0");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM my_prepare");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result))
    rowcount++;

  FAIL_IF(rowcount != 1, "rowcount != 1");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS my_prepare");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Test simple bind result */

static int test_bind_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  int        nData;
  ulong      length1;
  char       szData[100];
  MYSQL_BIND my_bind[2];
  my_bool    is_null[2];
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_result(col1 int , col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bind_result VALUES(10, 'venu')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bind_result VALUES(20, 'MySQL')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bind_result(col2) VALUES('monty')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* fetch */

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *) &nData;      /* integer data */
  my_bind[0].is_null= &is_null[0];

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= szData;                /* string data */
  my_bind[1].buffer_length= sizeof(szData);
  my_bind[1].length= &length1;
  my_bind[1].is_null= &is_null[1];

  strcpy(query, "SELECT * FROM test_bind_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 10, "nData != 10");
  FAIL_UNLESS(strcmp(szData, "venu") == 0, "szData != 'Venu'");
  FAIL_UNLESS(length1 == 4, "length1 != 4");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(nData == 20, "nData != 20");
  FAIL_UNLESS(strcmp(szData, "MySQL") == 0, "szData != 'MySQL'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(is_null[0], "null flag not set");
  FAIL_UNLESS(strcmp(szData, "monty") == 0, "szData != 'Monty'");
  FAIL_UNLESS(length1 == 5, "length1 != 5");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "MYSQL_NO_DATA expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bind_result_ext(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, i;
  uchar      t_data;
  short      s_data;
  int        i_data;
  longlong   b_data;
  float      f_data;
  double     d_data;
  char       szData[20], bData[20];
  ulong       szLength, bLength;
  MYSQL_BIND my_bind[8];
  ulong      length[8];
  my_bool    is_null[8];
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_result(c1 tinyint, "
                                                      " c2 smallint, "
                                                      " c3 int, c4 bigint, "
                                                      " c5 float, c6 double, "
                                                      " c7 varbinary(10), "
                                                      " c8 varchar(50))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bind_result "
                         "VALUES (19, 2999, 3999, 4999999, "
                         " 2345.6, 5678.89563, 'venu', 'mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].length=  &length[i];
    my_bind[i].is_null= &is_null[i];
  }

  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&t_data;

  my_bind[1].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[2].buffer_type= MYSQL_TYPE_LONG;

  my_bind[3].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[1].buffer= (void *)&s_data;

  my_bind[2].buffer= (void *)&i_data;
  my_bind[3].buffer= (void *)&b_data;

  my_bind[4].buffer_type= MYSQL_TYPE_FLOAT;
  my_bind[4].buffer= (void *)&f_data;

  my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[5].buffer= (void *)&d_data;

  my_bind[6].buffer_type= MYSQL_TYPE_STRING;
  my_bind[6].buffer= (void *)szData;
  my_bind[6].buffer_length= sizeof(szData);
  my_bind[6].length= &szLength;

  my_bind[7].buffer_type= MYSQL_TYPE_TINY_BLOB;
  my_bind[7].buffer= (void *)&bData;
  my_bind[7].length= &bLength;
  my_bind[7].buffer_length= sizeof(bData);

  strcpy(query, "select * from test_bind_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(t_data == 19, "tdata != 19");
  FAIL_UNLESS(s_data == 2999, "s_data != 2999");
  FAIL_UNLESS(i_data == 3999, "i_data != 3999");
  FAIL_UNLESS(b_data == 4999999, "b_data != 4999999");
  FAIL_UNLESS(strcmp(szData, "venu") == 0, "szData != 'Venu'");
  FAIL_UNLESS(strncmp(bData, "mysql", 5) == 0, "nData != 'mysql'");
  FAIL_UNLESS(szLength == 4, "szLength != 4");
  FAIL_UNLESS(bLength == 5, "bLength != 5");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "MYSQL_NO_DATA expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);
  return OK;
}


/* Test ext bind result */

static int test_bind_result_ext1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  uint       i;
  int        rc;
  char       t_data[20];
  float      s_data;
  short      i_data;
  uchar      b_data;
  int        f_data;
  long       bData;
  char       d_data[20];
  double     szData;
  MYSQL_BIND my_bind[8];
  ulong      length[8];
  my_bool    is_null[8];
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_result(c1 tinyint, c2 smallint, \
                                                        c3 int, c4 bigint, \
                                                        c5 float, c6 double, \
                                                        c7 varbinary(10), \
                                                        c8 varchar(10))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_bind_result VALUES(120, 2999, 3999, 54, \
                                                              2.6, 58.89, \
                                                              '206', '6.7')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *) t_data;
  my_bind[0].buffer_length= sizeof(t_data);
  my_bind[0].error= &my_bind[0].error_value;

  my_bind[1].buffer_type= MYSQL_TYPE_FLOAT;
  my_bind[1].buffer= (void *)&s_data;
  my_bind[1].buffer_length= 0;
  my_bind[1].error= &my_bind[1].error_value;

  my_bind[2].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[2].buffer= (void *)&i_data;
  my_bind[2].buffer_length= 0;
  my_bind[2].error= &my_bind[2].error_value;

  my_bind[3].buffer_type= MYSQL_TYPE_TINY;
  my_bind[3].buffer= (void *)&b_data;
  my_bind[3].buffer_length= 0;
  my_bind[3].error= &my_bind[3].error_value;

  my_bind[4].buffer_type= MYSQL_TYPE_LONG;
  my_bind[4].buffer= (void *)&f_data;
  my_bind[4].buffer_length= 0;
  my_bind[4].error= &my_bind[4].error_value;

  my_bind[5].buffer_type= MYSQL_TYPE_STRING;
  my_bind[5].buffer= (void *)d_data;
  my_bind[5].buffer_length= sizeof(d_data);
  my_bind[5].error= &my_bind[5].error_value;

  my_bind[6].buffer_type= MYSQL_TYPE_LONG;
  my_bind[6].buffer= (void *)&bData;
  my_bind[6].buffer_length= 0;
  my_bind[6].error= &my_bind[6].error_value;

  my_bind[7].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[7].buffer= (void *)&szData;
  my_bind[7].buffer_length= 0;
  my_bind[7].error= &my_bind[7].error_value;

  for (i= 0; i < array_elements(my_bind); i++)
  {
    my_bind[i].is_null= &is_null[i];
    my_bind[i].length= &length[i];
  }

  strcpy(query, "select * from test_bind_result");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(t_data, "120") == 0, "t_data != 120");
  FAIL_UNLESS(i_data == 3999, "i_data != 3999");
  FAIL_UNLESS(f_data == 2, "f_data != 2");
  FAIL_UNLESS(strcmp(d_data, "58.89") == 0, "d_data != 58.89");
  FAIL_UNLESS(b_data == 54, "b_data != 54");

  FAIL_UNLESS(length[0] == 3, "Wrong length");
  FAIL_UNLESS(length[1] == 4, "Wrong length");
  FAIL_UNLESS(length[2] == 2, "Wrong length");
  FAIL_UNLESS(length[3] == 1, "Wrong length");
  FAIL_UNLESS(length[4] == 4, "Wrong length");
  FAIL_UNLESS(length[5] == 5, "Wrong length");
  FAIL_UNLESS(length[6] == 4, "Wrong length");
  FAIL_UNLESS(length[7] == 8, "Wrong length");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "MYSQL_NO_DATA expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_bind_negative(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  char *query;
  int rc;
  MYSQL_BIND      my_bind[1];
  int32           my_val= 0;
  ulong           my_length= 0L;
  my_bool         my_null= FALSE;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create temporary table t1 (c1 int unsigned)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1), (-1)");
  check_mysql_rc(rc, mysql);

  query= (char*)"INSERT INTO t1 VALUES (?)";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  /* bind parameters */
  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&my_val;
  my_bind[0].length= &my_length;
  my_bind[0].is_null= &my_null;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  my_val= -1;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_buffers(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  int        rc;
  ulong      length;
  my_bool    is_null;
  char       buffer[20];
  char       query[MAX_TEST_QUERY_LENGTH];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_buffer");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_buffer(str varchar(20))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into test_buffer values('MySQL')\
                          , ('Database'), ('Open-Source'), ('Popular')");
  check_mysql_rc(rc, mysql);

  strcpy(query, "select str from test_buffer");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(buffer, '\0', sizeof(buffer));              /* Avoid overruns in printf() */

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].length= &length;
  my_bind[0].is_null= &is_null;
  my_bind[0].buffer_length= 1;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)buffer;
  my_bind[0].error= &my_bind[0].error_value;

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  buffer[1]= 'X';
  rc= mysql_stmt_fetch(stmt);

  FAIL_UNLESS(rc == MYSQL_DATA_TRUNCATED, "rc != MYSQL_DATA_TRUNCATED");
  FAIL_UNLESS(my_bind[0].error_value, "Errorflag not set");
  FAIL_UNLESS(buffer[0] == 'M', "buffer[0] != M");
  FAIL_UNLESS(buffer[1] == 'X', "buffer[1] != X");
  FAIL_UNLESS(length == 5, "length != 5");

  my_bind[0].buffer_length= 8;
  rc= mysql_stmt_bind_result(stmt, my_bind);/* re-bind */
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(strncmp(buffer, "Database", 8) == 0, "buffer != 'Database'");
  FAIL_UNLESS(length == 8, "length != 8");

  my_bind[0].buffer_length= 12;
  rc= mysql_stmt_bind_result(stmt, my_bind);/* re-bind */
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(strcmp(buffer, "Open-Source") == 0, "buffer != 'Open-Source'");
  FAIL_UNLESS(length == 11, "Length != 11");

  my_bind[0].buffer_length= 6;
  rc= mysql_stmt_bind_result(stmt, my_bind);/* re-bind */
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_DATA_TRUNCATED, "rc != MYSQL_DATA_TRUNCATED");
  FAIL_UNLESS(my_bind[0].error_value, "Errorflag not set");
  FAIL_UNLESS(strncmp(buffer, "Popula", 6) == 0, "buffer != 'Popula'");
  FAIL_UNLESS(length == 7, "length != 7");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_buffer");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_xjoin(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query=
    "select t.id, p1.value, n1.value, p2.value, n2.value from t3 t LEFT JOIN t1 p1 ON (p1.id=t.param1_id) LEFT JOIN t2 p2 ON (p2.id=t.param2_id) LEFT JOIN t4 n1 ON (n1.id=p1.name_id) LEFT JOIN t4 n2 ON (n2.id=p2.name_id) where t.id=1";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2, t3, t4");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t3 (id int(8), param1_id int(8), param2_id int(8)) ENGINE=InnoDB DEFAULT CHARSET=utf8");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 ( id int(8), name_id int(8), value varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t2 (id int(8), name_id int(8), value varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t4(id int(8), value varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t3 values (1, 1, 1), (2, 2, null)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (1, 1, 'aaa'), (2, null, 'bbb')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t2 values (1, 2, 'ccc')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t4 values (1, 'Name1'), (2, null)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == 1, "rowcount != 1");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1, t2, t3, t4");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_union_param(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  char *query;
  int rc, i;
  MYSQL_BIND      my_bind[2];
  char            my_val[4];
  ulong           my_length= 3L;
  my_bool         my_null= FALSE;

  strcpy(my_val, "abc");

  query= (char*)"select ? as my_col union distinct select ?";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  /* bind parameters */
  my_bind[0].buffer_type=    MYSQL_TYPE_STRING;
  my_bind[0].buffer=         (char*) &my_val;
  my_bind[0].buffer_length=  4;
  my_bind[0].length=         &my_length;
  my_bind[0].is_null=        &my_null;
  my_bind[1].buffer_type=    MYSQL_TYPE_STRING;
  my_bind[1].buffer=         (char*) &my_val;
  my_bind[1].buffer_length=  4;
  my_bind[1].length=         &my_length;
  my_bind[1].is_null=        &my_null;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == 1, "rowcount != 1");
  }

  mysql_stmt_close(stmt);

  return OK;
}

static int test_union(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query= "SELECT t1.name FROM t1 UNION "
                     "SELECT t2.name FROM t2";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "CREATE TABLE t1 "
                  "(id INTEGER NOT NULL PRIMARY KEY, "
                  " name VARCHAR(20) NOT NULL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "INSERT INTO t1 (id, name) VALUES "
                  "(2, 'Ja'), (3, 'Ede'), "
                  "(4, 'Haag'), (5, 'Kabul'), "
                  "(6, 'Almere'), (7, 'Utrecht'), "
                  "(8, 'Qandahar'), (9, 'Amsterdam'), "
                  "(10, 'Amersfoort'), (11, 'Constantine')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE TABLE t2 "
                  "(id INTEGER NOT NULL PRIMARY KEY, "
                  " name VARCHAR(20) NOT NULL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "INSERT INTO t2 (id, name) VALUES "
                  "(4, 'Guam'), (5, 'Aruba'), "
                  "(6, 'Angola'), (7, 'Albania'), "
                  "(8, 'Anguilla'), (9, 'Argentina'), "
                  "(10, 'Azerbaijan'), (11, 'Afghanistan'), "
                  "(12, 'Burkina Faso'), (13, 'Faroe Islands')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;
  FAIL_UNLESS(rc == 20, "rc != 20");
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_union2(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query= "select col1 FROM t1 where col1=1 union distinct "
                     "select col1 FROM t1 where col1=2";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1(col1 INT, \
                                         col2 VARCHAR(40),      \
                                         col3 SMALLINT, \
                                         col4 TIMESTAMP)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == 0, "rowcount != 0");
  }

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Misc tests to keep pure coverage happy */

static int test_pure_coverage(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  int        rc;
  ulong      length;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_pure");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_pure(c1 int, c2 varchar(20))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("insert into test_pure(c67788) values(10)"));
  FAIL_IF(!rc, "Error expected");
  mysql_stmt_close(stmt);

  /* Query without params and result should allow to bind 0 arrays */
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("insert into test_pure(c2) values(10)"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, (MYSQL_BIND*)0);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_bind_result(stmt, (MYSQL_BIND*)0);
  FAIL_UNLESS(rc == 1, "");

  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("insert into test_pure(c2) values(?)"));
  check_stmt_rc(rc, stmt);

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].length= &length;
  my_bind[0].is_null= 0;
  my_bind[0].buffer_length= 0;

  my_bind[0].buffer_type= MYSQL_TYPE_GEOMETRY;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  FAIL_IF(!rc, "Error expected");

  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select * from test_pure"));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  mysql_query(mysql, "DROP TABLE test_pure");
  return OK;
}

static int test_insert_select(MYSQL *mysql)
{
  MYSQL_STMT *stmt_insert, *stmt_select;
  char *query;
  int rc;
  uint i;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t2 (a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t2 values (1)");
  check_mysql_rc(rc, mysql);

  query= (char*)"insert into t1 select a from t2";
  stmt_insert= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_insert, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_insert, SL(query));
  check_stmt_rc(rc, stmt_insert);

  query= (char*)"select * from t1";
  stmt_select= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_select, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_select, SL(query));
  check_stmt_rc(rc, stmt_select);

  for(i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt_insert);
    check_stmt_rc(rc, stmt_insert);

    rc= mysql_stmt_execute(stmt_select);
    check_stmt_rc(rc, stmt_select);
    rc= 0;
    while (mysql_stmt_fetch(stmt_select) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == (int)(i+1), "rc != i+1");
  }

  mysql_stmt_close(stmt_insert);
  mysql_stmt_close(stmt_select);
  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple prepare-insert */

static int test_insert(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       str_data[50];
  char       tiny_data;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[2];
  ulong      length;


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prep_insert");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_prep_insert(col1 tinyint, \
                                col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert by prepare */
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO test_prep_insert VALUES(?, ?)"));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "Param_count != 2");

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  /* tinyint */
  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&tiny_data;

  /* string */
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= str_data;
  my_bind[1].buffer_length= sizeof(str_data);;
  my_bind[1].length= &length;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  /* now, execute the prepared statement to insert 10 records.. */
  for (tiny_data= 0; tiny_data < 3; tiny_data++)
  {
    length= sprintf(str_data, "MySQL%d", tiny_data);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM test_prep_insert");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS((int) tiny_data == rc, "rowcount != tinydata");
  mysql_free_result(result);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_prep_insert");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_join(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i, j;
  const char *query[]= {"SELECT * FROM t2 join t1 on (t1.a=t2.a)",
                        "SELECT * FROM t2 natural join t1",
                        "SELECT * FROM t2 join t1 using(a)",
                        "SELECT * FROM t2 left join t1 on(t1.a=t2.a)",
                        "SELECT * FROM t2 natural left join t1",
                        "SELECT * FROM t2 left join t1 using(a)",
                        "SELECT * FROM t2 right join t1 on(t1.a=t2.a)",
                        "SELECT * FROM t2 natural right join t1",
                        "SELECT * FROM t2 right join t1 using(a)"};


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int , b int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t2 (a int , c int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);");
  check_mysql_rc(rc, mysql);

  for (j= 0; j < 9; j++)
  {
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query[j]));
    check_stmt_rc(rc, stmt);
    for (i= 0; i < 3; i++)
    {
      rc= mysql_stmt_execute(stmt);
      check_stmt_rc(rc, stmt);
      rc= 0;
      while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
        rc++;
      FAIL_UNLESS(rc == 5, "rowcount != 5");
    }
    mysql_stmt_close(stmt);
  }

  rc= mysql_query(mysql, "DROP TABLE t1, t2");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_left_join_view(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query=
    "select t1.a, v1.x from t1 left join v1 on (t1.a= v1.x);";


  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t1,v1");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql, "DROP VIEW IF EXISTS v1,t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"insert into t1 values (1), (2), (3)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"create view v1 (x) as select a from t1 where a > 1");
  check_mysql_rc(rc, mysql);
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
     while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
       rc++;
     FAIL_UNLESS(rc == 3, "rowcount != 3");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple sample - manual */

static int test_manual_sample(MYSQL *mysql)
{
  unsigned int param_count;
  MYSQL_STMT   *stmt;
  short        small_data;
  int          int_data;
  int          rc;
  char         str_data[50];
  ulonglong    affected_rows;
  MYSQL_BIND   my_bind[3];
  my_bool      is_null;
  char query[MAX_TEST_QUERY_LENGTH];


  /*
    Sample which is incorporated directly in the manual under Prepared
    statements section (Example from mysql_stmt_execute()
  */

  memset(str_data, 0, sizeof(str_data));
  mysql_autocommit(mysql, 1);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_table");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_table(col1 int, col2 varchar(50), \
                                                 col3 smallint, \
                                                 col4 timestamp)");
  check_mysql_rc(rc, mysql);

  /* Prepare a insert query with 3 parameters */
  strcpy(query, "INSERT INTO test_table(col1, col2, col3) values(?, ?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  /* Get the parameter count from the statement */
  param_count= mysql_stmt_param_count(stmt);
  FAIL_IF(param_count != 3, "param_count != 3");

  memset(my_bind, '\0', sizeof(my_bind));

  /* INTEGER PART */
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&int_data;

  /* STRING PART */
  my_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[1].buffer= (void *)str_data;
  my_bind[1].buffer_length= sizeof(str_data);

  /* SMALLINT PART */
  my_bind[2].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[2].buffer= (void *)&small_data;
  my_bind[2].is_null= &is_null;
  is_null= 0;

  /* Bind the buffers */
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  /* Specify the data */
  int_data= 10;             /* integer */
  strcpy(str_data, "MySQL"); /* string  */

  /* INSERT SMALLINT data as NULL */
  is_null= 1;

  /* Execute the insert statement - 1*/
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  /* Get the total rows affected */
  affected_rows= mysql_stmt_affected_rows(stmt);
  FAIL_IF(affected_rows != 1, "affected-rows != 1");

  /* Re-execute the insert, by changing the values */
  int_data= 1000;
  strcpy(str_data, "The most popular open source database");
  small_data= 1000;         /* smallint */
  is_null= 0;               /* reset */

  /* Execute the insert statement - 2*/
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  /* Get the total rows affected */
  affected_rows= mysql_stmt_affected_rows(stmt);

  FAIL_IF(affected_rows != 1, "affected_rows != 1");

  /* Close the statement */
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  /* DROP THE TABLE */
  rc= mysql_query(mysql, "DROP TABLE test_table");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_create_drop(MYSQL *mysql)
{
  MYSQL_STMT *stmt_create, *stmt_drop, *stmt_select, *stmt_create_select;
  char *query;
  int rc, i;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t2 (a int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (a int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t2 values (3), (2), (1);");
  check_mysql_rc(rc, mysql);

  query= (char*)"create table t1 (a int)";
  stmt_create= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_create, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_create, SL(query));
  check_stmt_rc(rc, stmt_create);

  query= (char*)"drop table t1";
  stmt_drop= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_drop, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_drop, SL(query));
  check_stmt_rc(rc, stmt_drop);

  query= (char*)"select a in (select a from t2) from t1";
  stmt_select= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_select, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_select, SL(query));
  check_stmt_rc(rc, stmt_select);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  query= (char*)"create table t1 select a from t2";
  stmt_create_select= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_create_select, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_create_select, SL(query));
  check_stmt_rc(rc, stmt_create_select);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt_create);
    check_stmt_rc(rc, stmt_create);

    rc= mysql_stmt_execute(stmt_select);
    check_stmt_rc(rc, stmt_select);

    rc= 0;
    while (mysql_stmt_fetch(stmt_select) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == 0, "rowcount != 0");

    rc= mysql_stmt_execute(stmt_drop);
    check_stmt_rc(rc, stmt_drop);

    rc= mysql_stmt_execute(stmt_create_select);
    check_stmt_rc(rc, stmt_create);

    rc= mysql_stmt_execute(stmt_select);
    check_stmt_rc(rc, stmt_select);
    rc= 0;
    while (mysql_stmt_fetch(stmt_select) != MYSQL_NO_DATA)
      rc++;
    FAIL_UNLESS(rc == 3, "rowcount != 3");

    rc= mysql_stmt_execute(stmt_drop);
    check_stmt_rc(rc, stmt_drop);
  }

  mysql_stmt_close(stmt_create);
  mysql_stmt_close(stmt_drop);
  mysql_stmt_close(stmt_select);
  mysql_stmt_close(stmt_create_select);

  rc= mysql_query(mysql, "DROP TABLE t2");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test DATE, TIME, DATETIME and TS with MYSQL_TIME conversion */

static int test_date(MYSQL *mysql)
{
  int        rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_date(c1 TIMESTAMP, \
                                                 c2 TIME, \
                                                 c3 DATETIME, \
                                                 c4 DATE)");

  check_mysql_rc(rc, mysql);

  rc= test_bind_date_conv(mysql, 5);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  return rc;
}


/* Test all time types to DATE and DATE to all types */

static int test_date_date(MYSQL *mysql)
{
  int        rc;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_date(c1 DATE, \
                                                 c2 DATE, \
                                                 c3 DATE, \
                                                 c4 DATE)");

  check_mysql_rc(rc, mysql);

  rc= test_bind_date_conv(mysql, 3);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  return rc;
}

/* Test all time types to TIMESTAMP and TIMESTAMP to all types */

static int test_date_ts(MYSQL *mysql)
{
  int        rc;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_date(c1 TIMESTAMP, \
                                                 c2 TIMESTAMP, \
                                                 c3 TIMESTAMP, \
                                                 c4 TIMESTAMP)");

  check_mysql_rc(rc, mysql);

  rc= test_bind_date_conv(mysql, 2);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  return rc;
}


/* Test all time types to DATETIME and DATETIME to all types */

static int test_date_dt(MYSQL *mysql)
{
  int rc;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_date(c1 datetime, "
                         " c2 datetime, c3 datetime, c4 date)");
  check_mysql_rc(rc, mysql);

  rc= test_bind_date_conv(mysql, 2);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  return rc;
}

/* Test all time types to TIME and TIME to all types */

static int test_date_time(MYSQL *mysql)
{
  int        rc;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_date(c1 TIME, \
                                                 c2 TIME, \
                                                 c3 TIME, \
                                                 c4 TIME)");

  check_mysql_rc(rc, mysql);

  rc= test_bind_date_conv(mysql, 3);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_date");
  return rc;
}

/*
  Test of basic checks that are performed in server for components
  of MYSQL_TIME parameters.
*/

static int test_datetime_ranges(MYSQL *mysql)
{
  const char *stmt_text;
  int rc, i;
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[6];
  MYSQL_TIME tm[6];

  if (!is_mariadb)
    return SKIP;

  stmt_text= "drop table if exists t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "create table t1 (year datetime, month datetime, day datetime, "
                              "hour datetime, min datetime, sec datetime)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  stmt_text= "INSERT INTO t1 VALUES (?, ?, ?, ?, ?, ?)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  FAIL_IF(mysql_stmt_param_count(stmt) != 6, "param_count != 6");

  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < 6; i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_DATETIME;
    my_bind[i].buffer= &tm[i];
  }
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  tm[0].year= 2004; tm[0].month= 11; tm[0].day= 10;
  tm[0].hour= 12; tm[0].minute= 30; tm[0].second= 30;
  tm[0].second_part= 0; tm[0].neg= 0;

  tm[5]= tm[4]= tm[3]= tm[2]= tm[1]= tm[0];
  tm[0].year= 10000;  tm[1].month= 13; tm[2].day= 32;
  tm[3].hour= 24; tm[4].minute= 60; tm[5].second= 60;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(!mysql_warning_count(mysql), "warnings expected");

  if (verify_col_data(mysql, "t1", "year", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "month", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "day", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "hour", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "min", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "sec", "0000-00-00 00:00:00"))
    goto error;

  mysql_stmt_close(stmt);

  stmt_text= "delete from t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "INSERT INTO t1 (year, month, day) VALUES (?, ?, ?)";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  /*
    We reuse contents of bind and tm arrays left from previous part of test.
  */
  for (i= 0; i < 3; i++)
    my_bind[i].buffer_type= MYSQL_TYPE_DATE;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(!mysql_warning_count(mysql), "warnings expected");

  if (verify_col_data(mysql, "t1", "year", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "month", "0000-00-00 00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "day", "0000-00-00 00:00:00"))
    goto error;

  mysql_stmt_close(stmt);

  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "create table t1 (day_ovfl time, day time, hour time, min time, sec time)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  stmt_text= "INSERT INTO t1 VALUES (?,?,?,?,?)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  FAIL_IF(mysql_stmt_param_count(stmt) != 5, "param_count != 5");

  /*
    Again we reuse what we can from previous part of test.
  */
  for (i= 0; i < 5; i++)
    my_bind[i].buffer_type= MYSQL_TYPE_TIME;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  tm[0].year= 0; tm[0].month= 0; tm[0].day= 10;
  tm[0].hour= 12; tm[0].minute= 30; tm[0].second= 30;
  tm[0].second_part= 0; tm[0].neg= 0;

  tm[4]= tm[3]= tm[2]= tm[1]= tm[0];
  tm[0].day= 35; tm[1].day= 34; tm[2].hour= 30; tm[3].minute= 60; tm[4].second= 60;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(mysql_warning_count(mysql) != 2, "warning_count != 2");

  if (verify_col_data(mysql, "t1", "day_ovfl", "838:59:59"))
    goto error;
  if (verify_col_data(mysql, "t1", "day", "828:30:30"))
    goto error;
  if (verify_col_data(mysql, "t1", "hour", "270:30:30"))
    goto error;
  if (verify_col_data(mysql, "t1", "min", "00:00:00"))
    goto error;
  if (verify_col_data(mysql, "t1", "sec", "00:00:00"))
    goto error;

  mysql_stmt_close(stmt);
  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
error:
  mysql_stmt_close(stmt);
  stmt_text= "drop table t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_derived(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  MYSQL_BIND      my_bind[1];
  int32           my_val= 0;
  ulong           my_length= 0L;
  my_bool         my_null= FALSE;
  const char *query=
    "select count(1) from (select f.id from t1 f where f.id=?) as x";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (id  int(8), primary key (id)) \
ENGINE=InnoDB DEFAULT CHARSET=utf8");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (1)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&my_val;
  my_bind[0].length= &my_length;
  my_bind[0].is_null= &my_null;
  my_val= 1;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
    while (!mysql_stmt_fetch(stmt))
      rc++;
    FAIL_UNLESS(rc == 1, "rowcount != 1");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_distinct(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query=
    "SELECT 2+count(distinct b), group_concat(a) FROM t1 group by a";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int , b int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), \
(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);");
  check_mysql_rc(rc, mysql);

  for (i= 0; i < 3; i++)
  {
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= 0;
    while (!mysql_stmt_fetch(stmt))
      rc++;
    FAIL_UNLESS(rc == 5, "rowcount != 5");
    mysql_stmt_close(stmt);
  }

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_do_set(MYSQL *mysql)
{
  MYSQL_STMT *stmt_do, *stmt_set;
  char *query;
  int rc, i;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (a int)");
  check_mysql_rc(rc, mysql);

  query= (char*)"do @var:=(1 in (select * from t1))";
  stmt_do= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_do, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_do, SL(query));
  check_stmt_rc(rc, stmt_do);

  query= (char*)"set @var=(1 in (select * from t1))";
  stmt_set= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_set, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_set, SL(query));
  check_stmt_rc(rc, stmt_set);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt_do);
    check_stmt_rc(rc, stmt_do);
    rc= mysql_stmt_execute(stmt_set);
    check_stmt_rc(rc, stmt_set);
  }

  mysql_stmt_close(stmt_do);
  mysql_stmt_close(stmt_set);
  return OK;
}

static int test_double_compare(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       real_data[10], tiny_data;
  double     double_data;
  MYSQL_RES  *result;
  MYSQL_BIND my_bind[3];
  ulong      length[3];
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_double_compare");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_double_compare(col1 tinyint, "
                         " col2 float, col3 double )");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_double_compare "
                         "VALUES (1, 10.2, 34.5)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "UPDATE test_double_compare SET col1=100 "
                "WHERE col1 = ? AND col2 = ? AND COL3 = ?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "param_count != 3");

  memset(my_bind, '\0', sizeof(my_bind));

  /* tinyint */
  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&tiny_data;

  /* string->float */
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)&real_data;
  my_bind[1].buffer_length= sizeof(real_data);
  my_bind[1].length= &length[1];

  /* double */
  my_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[2].buffer= (void *)&double_data;

  tiny_data= 1;
  strcpy(real_data, "10.2");
  length[1]= (ulong)strlen(real_data);
  double_data= 34.5;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_affected_rows(stmt), "affected_rows != 0");

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  /* test the results now, only one row should exist */
  rc= mysql_query(mysql, "SELECT * FROM test_double_compare");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS((int)tiny_data == rc, "rowcount != tinydata");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_double_compare");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_multi(MYSQL *mysql)
{
  MYSQL_STMT *stmt_delete, *stmt_update, *stmt_select1, *stmt_select2;
  char *query;
  MYSQL_BIND my_bind[1];
  int rc, i;
  int32 param= 1;
  ulong length= 1;

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&param;
  my_bind[0].length= &length;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (a int, b int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t2 (a int, b int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t1 values (3, 3), (2, 2), (1, 1)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "insert into t2 values (3, 3), (2, 2), (1, 1)");
  check_mysql_rc(rc, mysql);

  query= (char*)"delete t1, t2 from t1, t2 where t1.a=t2.a and t1.b=10";
  stmt_delete= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_delete, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_delete, SL(query));
  check_stmt_rc(rc, stmt_delete);

  query= (char*)"update t1, t2 set t1.b=10, t2.b=10 where t1.a=t2.a and t1.b=?";
  stmt_update= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_update, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_update, SL(query));
  check_stmt_rc(rc, stmt_update);

  query= (char*)"select * from t1";
  stmt_select1= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_select1, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_select1, SL(query));
  check_stmt_rc(rc, stmt_select1);

  query= (char*)"select * from t2";
  stmt_select2= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_select2, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_select2, SL(query));
  check_stmt_rc(rc, stmt_select2);

  for(i= 0; i < 3; i++)
  {
    rc= mysql_stmt_bind_param(stmt_update, my_bind);
    check_stmt_rc(rc, stmt_update);

    rc= mysql_stmt_execute(stmt_update);
    check_stmt_rc(rc, stmt_update);

    rc= mysql_stmt_execute(stmt_delete);
    check_stmt_rc(rc, stmt_delete);

    rc= mysql_stmt_execute(stmt_select1);
    check_stmt_rc(rc, stmt_select1);
    rc= 0;
    while (!mysql_stmt_fetch(stmt_select1))
      rc++;
    FAIL_UNLESS(rc == 3-param, "rc != 3 - param");

    rc= mysql_stmt_execute(stmt_select2);
    check_stmt_rc(rc, stmt_select2);
    rc= 0;
    while (!mysql_stmt_fetch(stmt_select2))
      rc++;
    FAIL_UNLESS(rc == 3-param, "rc != 3 - param");

    param++;
  }

  mysql_stmt_close(stmt_delete);
  mysql_stmt_close(stmt_update);
  mysql_stmt_close(stmt_select1);
  mysql_stmt_close(stmt_select2);
  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Multiple stmts .. */

static int test_multi_stmt(MYSQL *mysql)
{

  MYSQL_STMT  *stmt, *stmt1, *stmt2;
  int         rc;
  uint32      id;
  char        name[50];
  MYSQL_BIND  my_bind[2];
  ulong       length[2];
  my_bool     is_null[2];
  const char *query;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_multi_table");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_multi_table(id int, name char(20))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_multi_table values(10, 'mysql')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  query= "SELECT * FROM test_multi_table WHERE id=?";
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  stmt2= mysql_stmt_init(mysql);
  FAIL_IF(!stmt2, mysql_error(mysql));
  query= "UPDATE test_multi_table SET name='updated' WHERE id=10";
  rc= mysql_stmt_prepare(stmt2, SL(query));
  check_stmt_rc(rc, stmt2);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "param_count != 1");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&id;
  my_bind[0].is_null= &is_null[0];
  my_bind[0].length= &length[0];
  is_null[0]= 0;
  length[0]= 0;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)name;
  my_bind[1].buffer_length= sizeof(name);
  my_bind[1].length= &length[1];
  my_bind[1].is_null= &is_null[1];

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  id= 10;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  id= 999;
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(id == 10, "id != 10");
  FAIL_UNLESS(strcmp(name, "mysql") == 0, "name != 'mysql'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "");

  /* alter the table schema now */
  stmt1= mysql_stmt_init(mysql);
  FAIL_IF(!stmt1, mysql_error(mysql));
  query= "DELETE FROM test_multi_table WHERE id=? AND CONVERT(name USING utf8)=?";
  rc= mysql_stmt_prepare(stmt1, SL(query));
  check_stmt_rc(rc, stmt1);

  FAIL_IF(mysql_stmt_param_count(stmt1) != 2, "param_count != 2");

  rc= mysql_stmt_bind_param(stmt1, my_bind);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_execute(stmt2);
  check_stmt_rc(rc, stmt2);

  FAIL_IF(mysql_stmt_affected_rows(stmt2) != 1, "affected_rows != 1");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(id == 10, "id != 10");
  FAIL_UNLESS(strcmp(name, "updated") == 0, "name != 'updated'");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  FAIL_IF(mysql_stmt_affected_rows(stmt1) != 1, "affected_rows != 1");

  mysql_stmt_close(stmt1);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  rc= my_stmt_result(mysql, "SELECT * FROM test_multi_table");
  FAIL_UNLESS(rc == 0, "rc != 0");

  mysql_stmt_close(stmt);
  mysql_stmt_close(stmt2);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_multi_table");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test 'n' statements create and close */

static int test_nstmts(MYSQL *mysql)
{
  MYSQL_STMT  *stmt;
  char        query[255];
  int         rc;
  static uint i, total_stmts= 2000;
  MYSQL_BIND  my_bind[1];

  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_nstmts");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_nstmts(id int)");
  check_mysql_rc(rc, mysql);

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)&i;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;

  for (i= 0; i < total_stmts; i++)
  {
    strcpy(query, "insert into test_nstmts values(?)");
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_bind_param(stmt, my_bind);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    mysql_stmt_close(stmt);
  }

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(" select count(*) from test_nstmts"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  i= 0;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS( i == total_stmts, "total_stmts != i");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_nstmts");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple null */

static int test_null(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  uint       nData;
  MYSQL_BIND my_bind[2];
  my_bool    is_null[2];
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_null");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_null(col1 int, col2 varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert by prepare, wrong column name */
  strcpy(query, "INSERT INTO test_null(col3, col2) VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_IF(!rc, "Error expected");
  mysql_stmt_close(stmt);

  strcpy(query, "INSERT INTO test_null(col1, col2) VALUES(?, ?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "param_count != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].is_null= &is_null[0];
  is_null[0]= 1;
  my_bind[1]= my_bind[0];

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  /* now, execute the prepared statement to insert 10 records.. */
  for (nData= 0; nData<10; nData++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  /* Re-bind with MYSQL_TYPE_NULL */
  my_bind[0].buffer_type= MYSQL_TYPE_NULL;
  is_null[0]= 0; /* reset */
  my_bind[1]= my_bind[0];

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (nData= 0; nData<10; nData++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  mysql_stmt_close(stmt);

  /* now fetch the results ..*/
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  nData*= 2;
  rc= my_stmt_result(mysql, "SELECT * FROM test_null");;
  FAIL_UNLESS((int) nData == rc, "rc != ndata");

  /* Fetch results */
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&nData; /* this buffer won't be altered */
  my_bind[0].length= 0;
  my_bind[1]= my_bind[0];
  my_bind[0].is_null= &is_null[0];
  my_bind[1].is_null= &is_null[1];

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_null"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= 0;
  is_null[0]= is_null[1]= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
  {
    FAIL_UNLESS(is_null[0], "!is_null");
    FAIL_UNLESS(is_null[1], "!is_null");
    rc++;
    is_null[0]= is_null[1]= 0;
  }
  FAIL_UNLESS(rc == (int) nData, "rc != nData");
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_null");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_order_param(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *query;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1(a INT, b char(10))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  query= "select sum(a) + 200, 1 from t1 "
         " union distinct "
         "select sum(a) + 200, 1 from t1 group by b ";
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  query= "select sum(a) + 200, ? from t1 group by b "
         " union distinct "
         "select sum(a) + 200, 1 from t1 group by b ";
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  query= "select sum(a) + 200, ? from t1 "
         " union distinct "
         "select sum(a) + 200, 1 from t1 group by b ";
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_rename(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *query= "rename table t1 to t2, t3 to t4";
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2, t3, t4");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "create table t1 (a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected");

  rc= mysql_query(mysql, "create table t3 (a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Errr expected");

  rc= mysql_query(mysql, "rename table t2 to t1, t4 to t3");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t2, t4");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_rewind(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind;
  int rc = 0;
  const char *stmt_text;
  long unsigned int length=4, Data=0;
  my_bool isnull=0;


  stmt_text= "CREATE TABLE t1 (a int)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "INSERT INTO t1 VALUES(2),(3),(4)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  stmt_text= "SELECT * FROM t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(&my_bind, '\0', sizeof(MYSQL_BIND));
  my_bind.buffer_type= MYSQL_TYPE_LONG;
  my_bind.buffer= (void *)&Data; /* this buffer won't be altered */
  my_bind.length= &length;
  my_bind.is_null= &isnull;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, &my_bind);
  check_stmt_rc(rc, stmt);

  /* retrieve all result sets till we are at the end */
  while(!(rc=mysql_stmt_fetch(stmt)));
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* seek to the first row */
  mysql_stmt_data_seek(stmt, 0);

  /* now we should be able to fetch the results again */
  /* but mysql_stmt_fetch returns MYSQL_NO_DATA */
  while(!(rc= mysql_stmt_fetch(stmt)));
  
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  stmt_text= "DROP TABLE t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  rc= mysql_stmt_free_result(stmt);
  rc= mysql_stmt_close(stmt);
  return OK;
}

/* Test simple select */

static int test_select(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  char       szData[25];
  int        nData= 1;
  MYSQL_BIND my_bind[2];
  ulong length[2];
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(id int, name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert a row and commit the transaction */
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES(10, 'venu')");
  check_mysql_rc(rc, mysql);

  /* now insert the second row, and roll back the transaction */
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES(20, 'mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT * FROM test_select WHERE id= ? "
                "AND CONVERT(name USING utf8) =?");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 2, "param_count != 2");

  memset(my_bind, '\0', sizeof(my_bind));

  /* string data */
  nData= 10;
  strcpy(szData, (char *)"venu");
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)szData;
  my_bind[1].buffer_length= 4;
  my_bind[1].length= &length[1];
  length[1]= 4;

  my_bind[0].buffer= (void *)&nData;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 1, "rc != 1");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple select with prepare */

static int test_select_prepare(MYSQL *mysql)
{
  int        rc;
  MYSQL_STMT *stmt;


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(id int, name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert a row and commit the transaction */
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES(10, 'venu')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_select"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(id tinyint, id1 int, "
                                                "  id2 float, id3 float, "
                                                "  name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert a row and commit the transaction */
  rc= mysql_query(mysql, "INSERT INTO test_select(id, id1, id2, name) VALUES(10, 5, 2.3, 'venu')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_select"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 1");
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple show */

static int test_select_show_table(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc, i;

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SHOW TABLES FROM mysql"));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt), "param_count != 0");

  for (i= 1; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }

  while (!mysql_stmt_fetch(stmt));
  mysql_stmt_close(stmt);
  return OK;
}

/* Test simple select */

static int test_select_version(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;


  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT @@version"));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt), "param_count != 0");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  while (!mysql_stmt_fetch(stmt));
  mysql_stmt_close(stmt);
  return OK;
}

static int test_selecttmp(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query= "select a, (select count(distinct t1.b) as sum from t1, t2 where t1.a=t2.a and t2.b > 0 and t1.a <= t3.b group by t1.a order by sum limit 1) from t3";


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, t2, t3");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int , b int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t2 (a int, b int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t3 (a int, b int);");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,
                  "insert into t1 values (0, 100), (1, 2), (1, 3), (2, 2), (2, 7), \
(2, -1), (3, 10);");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "insert into t2 values (0, 0), (1, 1), (2, 1), (3, 1), (4, 1);");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "insert into t3 values (3, 3), (2, 2), (1, 1);");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= 0;
    while (!mysql_stmt_fetch(stmt))
      rc++;
    FAIL_UNLESS(rc == 3, "rowcount != 3");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1, t2, t3");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_set_option(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_RES  *result;
  int        rc;


  mysql_autocommit(mysql, TRUE);

  /* LIMIT the rows count to 2 */
  rc= mysql_query(mysql, "SET SQL_SELECT_LIMIT= 2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_limit");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_limit(a tinyint)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_limit VALUES(10), (20), (30), (40)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM test_limit");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 2, "rowcunt != 2");
  mysql_free_result(result);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_limit"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 2, "");

  mysql_stmt_close(stmt);

  /* RESET the LIMIT the rows count to 0 */
  rc= mysql_query(mysql, "SET SQL_SELECT_LIMIT=DEFAULT");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("SELECT * FROM test_limit"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (!mysql_stmt_fetch(stmt))
    rc++;
  FAIL_UNLESS(rc == 4, "rowcount != 4");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_limit");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple set-variable prepare */

static int test_set_variable(MYSQL *mysql)
{
  MYSQL_STMT *stmt, *stmt1;
  int        rc;
  int        set_count, def_count, get_count;
  ulong      length;
  char       var[NAME_LEN+1];
  MYSQL_BIND set_bind[1], get_bind[2];


  mysql_autocommit(mysql, TRUE);

  stmt1= mysql_stmt_init(mysql);
  FAIL_IF(!stmt1, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt1, SL("show variables like 'max_error_count'"));
  check_stmt_rc(rc, stmt1);

  memset(get_bind, '\0', sizeof(get_bind));

  get_bind[0].buffer_type= MYSQL_TYPE_STRING;
  get_bind[0].buffer= (void *)var;
  get_bind[0].length= &length;
  get_bind[0].buffer_length= (int)NAME_LEN;
  length= NAME_LEN;

  get_bind[1].buffer_type= MYSQL_TYPE_LONG;
  get_bind[1].buffer= (void *)&get_count;

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_bind_result(stmt1, get_bind);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);

  def_count= get_count;

  FAIL_UNLESS(strcmp(var, "max_error_count") == 0, "var != max_error_count");
  rc= mysql_stmt_fetch(stmt1);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("set max_error_count=?"));
  check_stmt_rc(rc, stmt);

  memset(set_bind, '\0', sizeof(set_bind));

  set_bind[0].buffer_type= MYSQL_TYPE_LONG;
  set_bind[0].buffer= (void *)&set_count;

  rc= mysql_stmt_bind_param(stmt, set_bind);
  check_stmt_rc(rc, stmt);

  set_count= 31;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_commit(mysql);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);

  FAIL_UNLESS(get_count == set_count, "get_count != set_count");

  rc= mysql_stmt_fetch(stmt1);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* restore back to default */
  set_count= def_count;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);

  FAIL_UNLESS(get_count == set_count, "get_count != set_count");

  rc= mysql_stmt_fetch(stmt1);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  mysql_stmt_close(stmt1);
  return OK;
}

/* Test SQLmode */

static int test_sqlmode(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  char       c1[5], c2[5];
  int        rc;
  int        ignore_space= 0;
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_piping");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_piping(name varchar(10))");
  check_mysql_rc(rc, mysql);

  /* PIPES_AS_CONCAT */
  strcpy(query, "SET SQL_MODE= \"PIPES_AS_CONCAT\"");
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_piping VALUES(?||?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  
  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)c1;
  my_bind[0].buffer_length= 2;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)c2;
  my_bind[1].buffer_length= 3;

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  strcpy(c1, "My"); strcpy(c2, "SQL");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  if (verify_col_data(mysql, "test_piping", "name", "MySQL"))
    return FAIL;

  rc= mysql_query(mysql, "DELETE FROM test_piping");
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT connection_id    ()");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  /* ANSI */
  strcpy(query, "SET SQL_MODE= \"ANSI\"");
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  strcpy(query, "INSERT INTO test_piping VALUES(?||?)");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  strcpy(c1, "My"); strcpy(c2, "SQL");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  if (verify_col_data(mysql, "test_piping", "name", "MySQL"))
    return FAIL;

  /* ANSI mode spaces ... 
    skip, if ignore_space was set
  */
  query_int_variable(mysql, "@@sql_mode LIKE '%IGNORE_SPACE%'", &ignore_space);

  if (!ignore_space)
  {
    strcpy(query, "SELECT connection_id ()");
    stmt= mysql_stmt_init(mysql);
    FAIL_IF(!stmt, mysql_error(mysql));
    rc= mysql_stmt_prepare(stmt, SL(query));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

    mysql_stmt_close(stmt);
  }
  /* IGNORE SPACE MODE */
  strcpy(query, "SET SQL_MODE= \"IGNORE_SPACE\"");
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  strcpy(query, "SELECT connection_id    ()");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_piping");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test mysql_stmt_close for open stmts */

static int test_stmt_close(MYSQL *mysql)
{
  MYSQL_STMT *stmt1, *stmt2, *stmt3, *stmt_x;
  MYSQL_BIND  my_bind[1];
  MYSQL_RES   *result;
  unsigned int  count;
  int   rc;
  char query[MAX_TEST_QUERY_LENGTH];
  my_bool reconnect= 1;

  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  /* set AUTOCOMMIT to ON*/
  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_stmt_close");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_stmt_close(id int)");
  check_mysql_rc(rc, mysql);

  strcpy(query, "DO \"nothing\"");
  stmt1= mysql_stmt_init(mysql);
  FAIL_IF(!stmt1, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt1, SL(query));
  check_stmt_rc(rc, stmt1);

  FAIL_IF(mysql_stmt_param_count(stmt1), "param_count != 0");

  strcpy(query, "INSERT INTO test_stmt_close(id) VALUES(?)");
  stmt_x= mysql_stmt_init(mysql);
  FAIL_IF(!stmt_x, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt_x, SL(query));
  check_stmt_rc(rc, stmt_x);

  FAIL_IF(mysql_stmt_param_count(stmt_x) != 1, "param_count != 1");

  strcpy(query, "UPDATE test_stmt_close SET id= ? WHERE id= ?");
  stmt3= mysql_stmt_init(mysql);
  FAIL_IF(!stmt3, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt3, SL(query));
  check_stmt_rc(rc, stmt3);

  FAIL_IF(mysql_stmt_param_count(stmt3) != 2, "param_count != 2");

  strcpy(query, "SELECT * FROM test_stmt_close WHERE id= ?");
  stmt2= mysql_stmt_init(mysql);
  FAIL_IF(!stmt2, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt2, SL(query));
  check_stmt_rc(rc, stmt2);

  FAIL_IF(mysql_stmt_param_count(stmt2) != 1, "param_count != 1");

  rc= mysql_stmt_close(stmt1);

  /*
    Originally we were going to close all statements automatically in
    mysql_close(). This proved to not work well - users weren't able to
    close statements by hand once mysql_close() had been called.
    Now mysql_close() doesn't free any statements, so this test doesn't
    serve its original designation any more.
    Here we free stmt2 and stmt3 by hand to avoid memory leaks.
  */
  mysql_stmt_close(stmt2);
  mysql_stmt_close(stmt3);

  /*
    We need to bzero bind structure because mysql_stmt_bind_param checks all
    its members.
  */
  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer= (void *)&count;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  count= 100;

  rc= mysql_stmt_bind_param(stmt_x, my_bind);
  check_stmt_rc(rc, stmt_x);

  rc= mysql_stmt_execute(stmt_x);
  check_stmt_rc(rc, stmt_x);

  FAIL_IF(mysql_stmt_affected_rows(stmt_x) != 1, "affected_rows != 1");

  rc= mysql_stmt_close(stmt_x);
  check_stmt_rc(rc, stmt_x);

  rc= mysql_query(mysql, "SELECT id FROM test_stmt_close");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 1, "rwcount != 1");
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_stmt_close");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_new_date(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[1];
  int rc;
  char buffer[50];
  my_bool reconnect= 1;
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  /* set AUTOCOMMIT to ON*/
  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a date, b date)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (now(), now() + INTERVAL 1 day)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, "SELECT if(1, a, b) FROM t1", 26);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND));
  bind[0].buffer_length= 50;
  bind[0].buffer= (void *)buffer;
  bind[0].buffer_type= MYSQL_TYPE_STRING;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != MYSQL_NO_DATA, "NO DATA expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_long_data1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  MYSQL_BIND bind[1];
  char query[MAX_TEST_QUERY_LENGTH];
  const char *data= "12345";

  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tld");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE tld (col1 int, "
                         "col2 long varbinary)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO tld VALUES (1,'test')");
  check_mysql_rc(rc, mysql);

  strcpy(query, "UPDATE tld SET col2=? WHERE col1=1");
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  memset(bind, 0, sizeof(MYSQL_BIND));
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_send_long_data(stmt, 0, data, 6);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tld");
  check_mysql_rc(rc, mysql);
  return OK;
}

int test_blob_9000(MYSQL *mysql)
{
  MYSQL_BIND bind[1];
  MYSQL_STMT *stmt;
  int rc;
  char buffer[9200];
  const char *query= "INSERT INTO tb9000 VALUES (?)";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tb9000");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE tb9000 (a blob)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));

  memset(bind, 0, sizeof(MYSQL_BIND));
  memset(buffer, 'C', 9200);
  bind[0].buffer= buffer;
  bind[0].buffer_length= 9200;
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tb9000");
  check_mysql_rc(rc, mysql);
  return OK;
}

int test_fracseconds(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *str= "SELECT NOW(6)";
  char buffer[60], buffer1[60];
  MYSQL_BIND bind[2];

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(str));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(&bind, 0, sizeof(MYSQL_BIND));
  bind[0].buffer= buffer;
  bind[0].buffer_length=60;
  bind[0].buffer_type= MYSQL_TYPE_STRING;

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(strlen(buffer) != 26, "Expected timestamp with length of 26");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a timestamp(6), b time(6))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES ('2012-04-25 10:20:49.0194','10:20:49.0194' )");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, "SELECT a,b FROM t1", 18);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer= buffer;
  bind[1].buffer= buffer1;
  bind[0].buffer_length=  bind[1].buffer_length= 60;
  bind[0].buffer_type= bind[1].buffer_type= MYSQL_TYPE_STRING;

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_IF(strcmp(buffer, "2012-04-25 10:20:49.019400") != 0, "Wrong result");
  FAIL_IF(strcmp(buffer1, "10:20:49.019400") != 0, "Wrong result");

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");

  return OK;  
}

int test_notrunc(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  my_bool trunc= 1;
  MYSQL_BIND bind[2];
  char buffer[5], buffer2[5];
  int rc;
  my_bool error= 0;
  unsigned long len= 1;

  const char *query= "SELECT '1234567890', 'foo' FROM DUAL";

  mysql_options(mysql, MYSQL_REPORT_DATA_TRUNCATION, &trunc);

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt); 
  check_stmt_rc(rc, stmt);

  strcpy(buffer, "bar");

  memset(bind, 0, sizeof(MYSQL_BIND) * 2);
  bind[0].buffer_type= MYSQL_TYPE_NULL;
  bind[0].buffer= buffer;
  bind[0].buffer_length= 1;
  bind[0].length= &len;
  bind[0].flags|= MADB_BIND_DUMMY;
  bind[0].error= &error;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer= buffer2;
  bind[1].buffer_length= 5;

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);
  mysql_stmt_store_result(stmt);

  rc= mysql_stmt_fetch(stmt);
  mysql_stmt_close(stmt);

  FAIL_IF(rc!= 0, "expected rc= 0");
  FAIL_IF(strcmp(buffer, "bar"), "Bind dummy failed");
  FAIL_IF(strcmp(buffer2, "foo"), "Invalid second buffer");

  return OK;
}

static int test_bit2tiny(MYSQL *mysql)
{
  MYSQL_BIND bind[2];
  char       data[11];
  unsigned   long length[2];
  my_bool    is_null[2], error[2];
  const char *query = "SELECT val FROM justbit";
  MYSQL_STMT *stmt;
  int rc;

  mysql_query(mysql, "DROP TABLE IF EXISTS justbit");
  mysql_query(mysql, "CREATE TABLE justbit(val bit(1) not null)");
  mysql_query(mysql, "INSERT INTO justbit values (1)");

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  memset(bind, '\0', sizeof(bind));

  bind[0].buffer_type=  MYSQL_TYPE_TINY;
  bind[0].buffer=       &data[0];
  bind[0].buffer_length= 1;
  bind[0].is_null=      &is_null[0];
  bind[0].length=       &length[0];
  bind[0].error=        &error[0];

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_fetch(stmt);

  FAIL_IF(data[0] != 1, "Value should be 1");

  mysql_stmt_free_result(stmt);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS justbit");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_reexecute(MYSQL *mysql)
{
    MYSQL_STMT *stmt;
  MYSQL_BIND ps_params[3];  /* input parameter buffers */
  int        int_data[3];   /* input/output values */
  int        rc;

  if (!mariadb_connection(mysql))
    return SKIP;

  /* set up stored procedure */
  rc = mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql,
      "CREATE PROCEDURE p1("
      "  IN p_in INT, "
      "  OUT p_out INT, "
      "  INOUT p_inout INT) "
      "BEGIN "
      "  SELECT p_in, p_out, p_inout; "
      "  SET p_in = 100, p_out = 200, p_inout = 300; "
      "  SELECT p_in, p_out, p_inout; "
      "END");
  check_mysql_rc(rc, mysql);

  /* initialize and prepare CALL statement with parameter placeholders */
  stmt = mysql_stmt_init(mysql);
  if (!stmt)
  {
    diag("Could not initialize statement");
    exit(1);
  }
  rc = mysql_stmt_prepare(stmt, "CALL p1(?, ?, ?)", 16);
  check_stmt_rc(rc, stmt);

  /* initialize parameters: p_in, p_out, p_inout (all INT) */
  memset(ps_params, 0, sizeof (ps_params));

  ps_params[0].buffer_type = MYSQL_TYPE_LONG;
  ps_params[0].buffer = (char *) &int_data[0];
  ps_params[0].length = 0;
  ps_params[0].is_null = 0;

  ps_params[1].buffer_type = MYSQL_TYPE_LONG;
  ps_params[1].buffer = (char *) &int_data[1];
  ps_params[1].length = 0;
  ps_params[1].is_null = 0;

  ps_params[2].buffer_type = MYSQL_TYPE_LONG;
  ps_params[2].buffer = (char *) &int_data[2];
  ps_params[2].length = 0;
  ps_params[2].is_null = 0;

  /* bind parameters */
  rc = mysql_stmt_bind_param(stmt, ps_params);
  check_stmt_rc(rc, stmt);

  /* assign values to parameters and execute statement */
  int_data[0]= 10;  /* p_in */
  int_data[1]= 20;  /* p_out */
  int_data[2]= 30;  /* p_inout */

  rc = mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc = mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_prepare_error(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;

  rc= mysql_stmt_prepare(stmt, SL("SELECT 1 FROM tbl_not_exists"));
  FAIL_IF(!rc, "Expected error");

  rc= mysql_stmt_reset(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_prepare(stmt, SL("SELECT 1 FROM tbl_not_exists"));
  FAIL_IF(!rc, "Expected error");

  rc= mysql_stmt_reset(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_prepare(stmt, SL("SET @a:=1"));
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  return OK;
}

static int test_conc349(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  enum mysql_stmt_state state;

  rc= mysql_stmt_attr_get(stmt, STMT_ATTR_STATE, &state);
  FAIL_IF(state != MYSQL_STMT_INITTED, "expected status MYSQL_STMT_INITTED");

  rc= mysql_stmt_prepare(stmt, SL("SET @a:=1"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_get(stmt, STMT_ATTR_STATE, &state);
  FAIL_IF(state != MYSQL_STMT_PREPARED, "expected status MYSQL_STMT_PREPARED");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_get(stmt, STMT_ATTR_STATE, &state);
  FAIL_IF(state != MYSQL_STMT_EXECUTED, "expected status MYSQL_STMT_EXECUTED");

  mysql_stmt_close(stmt);
  return OK;
}

struct my_tests_st my_tests[] = {
  {"test_conc349", test_conc349, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_prepare_error", test_prepare_error, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_reexecute", test_reexecute, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_bit2tiny", test_bit2tiny, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_conc97", test_conc97, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_conc83", test_conc83, TEST_CONNECTION_NONE, 0, NULL, NULL},
  {"test_conc60", test_conc60, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_notrunc", test_notrunc, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_fracseconds", test_fracseconds, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_blob_9000", test_blob_9000, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_long_data1", test_long_data1, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_insert_update", test_prepare_insert_update, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_simple", test_prepare_simple, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_syntax", test_prepare_syntax, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_field_result", test_prepare_field_result, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare", test_prepare, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_ext", test_prepare_ext, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_multi_statements", test_prepare_multi_statements, TEST_CONNECTION_NEW, 0, NULL , NULL},
  {"test_prepare_alter", test_prepare_alter, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_resultset", test_prepare_resultset, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_open_direct", test_open_direct, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_select_show", test_select_show, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_select", test_select, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_long_data", test_long_data, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_long_data_str", test_long_data_str, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_long_data_str1", test_long_data_str1, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_long_data_bin", test_long_data_bin, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_simple_update", test_simple_update, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_simple_delete", test_simple_delete, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_update", test_update, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_prepare_noparam", test_prepare_noparam, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bind_result", test_bind_result, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bind_result_ext", test_bind_result_ext, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bind_result_ext1", test_bind_result_ext1, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bind_negative", test_bind_negative, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_buffers", test_buffers, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_xjoin", test_xjoin, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_union", test_union, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_union2", test_union2, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_union_param", test_union_param, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_pure_coverage", test_pure_coverage, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_insert_select", test_insert_select, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_insert", test_insert, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_join", test_join, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_left_join_view", test_left_join_view, TEST_CONNECTION_DEFAULT, 0, NULL , NULL}, 
  {"test_manual_sample", test_manual_sample, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_create_drop", test_create_drop, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_date", test_date, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_date_ts", test_date_ts, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_date_dt", test_date_dt, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_date_date", test_date_date, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_date_time", test_date_time, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_datetime_ranges", test_datetime_ranges, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_derived", test_derived, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_distinct", test_distinct, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_do_set", test_do_set, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_double_compare", test_double_compare, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_multi", test_multi, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_multi_stmt", test_multi_stmt, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_nstmts", test_nstmts, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_null", test_null, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_order_param", test_order_param, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_rename", test_rename, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_rewind", test_rewind, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_select_prepare", test_select_prepare, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_select_show_table", test_select_show_table, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_select_version", test_select_version, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_selecttmp", test_selecttmp, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_set_option", test_set_option, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_set_variable", test_set_variable, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_sqlmode", test_sqlmode, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_stmt_close", test_stmt_close, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_new_date", test_new_date, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
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
