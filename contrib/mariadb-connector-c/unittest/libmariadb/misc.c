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
#include "ma_common.h"

#include <mysql/client_plugin.h>


/*
  Bug#28075 "COM_DEBUG crashes mysqld"
*/
#ifdef _WIN32
#define R_OK 4
#endif

static int test_bug28075(MYSQL *mysql)
{
  int rc;

  rc= mysql_dump_debug_info(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_ping(mysql);
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug#28505: mysql_affected_rows() returns wrong value if CLIENT_FOUND_ROWS
  flag is set.
*/

static int test_bug28505(MYSQL *mysql)
{
  unsigned long long res;
  int rc;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(f1 int primary key)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values(1)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values(1) on duplicate key update f1=1");
  check_mysql_rc(rc, mysql);
  res= mysql_affected_rows(mysql);
  FAIL_UNLESS(!res, "res != 0");
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Bug #29692  	Single row inserts can incorrectly report a huge number of 
  row insertions
*/

static int test_bug29692(MYSQL *mysql)
{
  int rc;
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(f1 int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values(1)");
  check_mysql_rc(rc, mysql);
  FAIL_UNLESS(1 == mysql_affected_rows(mysql), "affected_rows != 1");
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bug31418_impl()
{
  my_bool is_null;
  MYSQL *mysql;
  int rc;


  /* Create a new connection. */

  mysql= test_connect(NULL);
  if (!mysql)
    return FAIL;

  /***********************************************************************
    Check that lock is free:
      - IS_FREE_LOCK() should return 1;
      - IS_USED_LOCK() should return NULL;
  ***********************************************************************/

  is_null= query_int_variable(mysql,
                              "IS_FREE_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(!is_null && rc, "rc = 0");

  is_null= query_int_variable(mysql,
                              "IS_USED_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(is_null, "rc = 0");

  /***********************************************************************
    Acquire lock and check the lock status (the lock must be in use):
      - IS_FREE_LOCK() should return 0;
      - IS_USED_LOCK() should return non-zero thread id;
  ***********************************************************************/

  query_int_variable(mysql, "GET_LOCK('bug31418', 1)", &rc);
  FAIL_UNLESS(rc, "rc = 0");

  is_null= query_int_variable(mysql,
                              "IS_FREE_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(!is_null && !rc, "rc = 0");

  is_null= query_int_variable(mysql,
                              "IS_USED_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(!is_null && rc, "rc = 0");

  /***********************************************************************
    Issue COM_CHANGE_USER command and check the lock status
    (the lock must be free):
      - IS_FREE_LOCK() should return 1;
      - IS_USED_LOCK() should return NULL;
  **********************************************************************/

  rc= mysql_change_user(mysql, username, password, schema ? schema : "test");
  check_mysql_rc(rc, mysql);

  is_null= query_int_variable(mysql,
                              "IS_FREE_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(!is_null && rc, "rc = 0");

  is_null= query_int_variable(mysql,
                              "IS_USED_LOCK('bug31418')",
                              &rc);
  FAIL_UNLESS(is_null, "rc = 0");

  /***********************************************************************
   That's it. Cleanup.
  ***********************************************************************/

  mysql_close(mysql);
  return OK;
}

static int test_bug31418(MYSQL *unused __attribute__((unused)))
{
  int i;

  if (!is_mariadb)
    return SKIP;
  /* Run test case for BUG#31418 for three different connections. */

  for (i=0; i < 3; i++)
    if (bug31418_impl())
      return FAIL;

  return OK;
}

/* Query processing */

static int test_debug_example(MYSQL *mysql)
{
  int rc;
  MYSQL_RES *result;


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_debug_example");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_debug_example("
                         "id INT PRIMARY KEY AUTO_INCREMENT, "
                         "name VARCHAR(20), xxx INT)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_debug_example (name) "
                         "VALUES ('mysql')");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "UPDATE test_debug_example SET name='updated' "
                         "WHERE name='deleted'");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM test_debug_example where name='mysql'");
  check_mysql_rc(rc, mysql);

  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  while (mysql_fetch_row(result));
  mysql_free_result(result);

  rc= mysql_query(mysql, "DROP TABLE test_debug_example");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Test a crash when invalid/corrupted .frm is used in the
  SHOW TABLE STATUS
  bug #93 (reported by serg@mysql.com).
*/

static int test_frm_bug(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  MYSQL_RES  *result;
  MYSQL_ROW  row;
  FILE       *test_file;
  char       data_dir[FN_REFLEN];
  char       test_frm[1024];
  int        rc;

  mysql_autocommit(mysql, TRUE);

  rc= mysql_query(mysql, "drop table if exists test_frm_bug");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "flush tables");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("show variables like 'datadir'"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= data_dir;
  my_bind[0].buffer_length= FN_REFLEN;
  my_bind[1]= my_bind[0];

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  snprintf(test_frm, sizeof(test_frm)-1, "%s/%s/test_frm_bug.frm", data_dir, schema);

  if (!(test_file= fopen(test_frm, "w")))
  {
    mysql_stmt_close(stmt);
    diag("Can't write to file %s -> SKIP", test_frm);
    return SKIP;
  }

  rc= mysql_query(mysql, "SHOW TABLE STATUS like 'test_frm_bug'");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");/* It can't be NULL */

  rc= 0;
  while (mysql_fetch_row(result))
    rc++;
  FAIL_UNLESS(rc == 1, "rowcount != 0");

  mysql_data_seek(result, 0);

  row= mysql_fetch_row(result);
  FAIL_IF(!row, "couldn't fetch row");

  FAIL_UNLESS(row[17] != 0, "row[17] != 0");

  mysql_free_result(result);
  mysql_stmt_close(stmt);

  fclose(test_file);
  mysql_query(mysql, "drop table if exists test_frm_bug");
  unlink(test_frm);
  return OK;
}

static int test_wl4166_1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        int_data;
  char       str_data[50];
  char       tiny_data;
  short      small_data;
  longlong   big_data;
  float      real_data;
  double     double_data;
  ulong      length[7];
  my_bool    is_null[7];
  MYSQL_BIND my_bind[7];
  const char *query;
  int rc;
  int i;

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS table_4166");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE table_4166(col1 tinyint NOT NULL, "
                         "col2 varchar(15), col3 int, "
                         "col4 smallint, col5 bigint, "
                         "col6 float, col7 double, "
                         "colX varchar(10) default NULL)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  query= "INSERT INTO table_4166(col1, col2, col3, col4, col5, col6, col7) "
          "VALUES(?, ?, ?, ?, ?, ?, ?)";
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 7, "param_count != 7");

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
  for (tiny_data= 0; tiny_data < 10; tiny_data++)
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

  /* force a re-prepare with some DDL */

  rc= mysql_query(mysql,
    "ALTER TABLE table_4166 change colX colX varchar(20) default NULL");
  check_mysql_rc(rc, mysql);

  /*
    execute the prepared statement again,
    without changing the types of parameters already bound.
  */

  for (tiny_data= 50; tiny_data < 60; tiny_data++)
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

  rc= mysql_query(mysql, "DROP TABLE table_4166");
  check_mysql_rc(rc, mysql);
  return OK;
}


static int test_wl4166_2(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        c_int;
  MYSQL_TIME d_date;
  MYSQL_BIND bind_out[2];
  int rc;

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (c_int int, d_date date)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "insert into t1 (c_int, d_date) values (42, '1948-05-15')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("select * from t1"));
  check_stmt_rc(rc, stmt);

  memset(bind_out, '\0', sizeof(bind_out));
  bind_out[0].buffer_type= MYSQL_TYPE_LONG;
  bind_out[0].buffer= (void*) &c_int;

  bind_out[1].buffer_type= MYSQL_TYPE_DATE;
  bind_out[1].buffer= (void*) &d_date;

  rc= mysql_stmt_bind_result(stmt, bind_out);
  check_stmt_rc(rc, stmt);

  /* int -> varchar transition */

  rc= mysql_query(mysql,
                  "alter table t1 change column c_int c_int varchar(11)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(c_int == 42, "c_int != 42");
  FAIL_UNLESS(d_date.year == 1948, "y!=1948");
  FAIL_UNLESS(d_date.month == 5, "m != 5");
  FAIL_UNLESS(d_date.day == 15, "d != 15");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* varchar to int retrieval with truncation */

  rc= mysql_query(mysql, "update t1 set c_int='abcde'");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(!rc, "Error expected");

  FAIL_UNLESS(c_int == 0, "c != 0");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  /* alter table and increase the number of columns */
  rc= mysql_query(mysql, "alter table t1 add column d_int int");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(!rc, "Error expected");

  rc= mysql_stmt_reset(stmt);
  check_stmt_rc(rc, stmt);

  /* decrease the number of columns */
  rc= mysql_query(mysql, "alter table t1 drop d_date, drop d_int");
  check_mysql_rc(rc, mysql);
  rc= mysql_stmt_execute(stmt);
  diag("rc=%d error: %d\n", rc, mysql_stmt_errno(stmt));
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}


/**
  Test how warnings generated during assignment of parameters
  are (currently not) preserve in case of reprepare.
*/

static int test_wl4166_3(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  MYSQL_TIME tm[1];

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 (year datetime)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL("insert into t1 (year) values (?)"));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 1, "param_count != 1");

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_DATETIME;
  my_bind[0].buffer= &tm[0];

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  tm[0].year= 2014;
  tm[0].month= 1; tm[0].day= 1;
  tm[0].hour= 1; tm[0].minute= 1; tm[0].second= 1;
  tm[0].second_part= 0; tm[0].neg= 0;

  /* Cause a statement reprepare */
  rc= mysql_query(mysql, "alter table t1 add column c int");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  diag("rc=%d %s", rc, mysql_stmt_error(stmt));
  check_stmt_rc(rc, stmt);

  if (verify_col_data(mysql, "t1", "year", "2014-01-01 01:01:01")) {
    mysql_stmt_close(stmt);
    rc= mysql_query(mysql, "drop table t1");
    return FAIL;
  }

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}


/**
  Test that long data parameters, as well as parameters
  that were originally in a different character set, are
  preserved in case of reprepare.
*/

static int test_wl4166_4(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmt_text;
  MYSQL_BIND bind_array[2];

  /* Represented as numbers to keep UTF8 tools from clobbering them. */
  const char *koi8= "\xee\xd5\x2c\x20\xda\xc1\x20\xd2\xd9\xc2\xc1\xcc\xcb\xd5";
  const char *cp1251= "\xcd\xf3\x2c\x20\xe7\xe0\x20\xf0\xfb\xe1\xe0\xeb\xea\xf3";
  char buf1[16], buf2[16];
  ulong buf1_len, buf2_len;

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  /*
    Create table with binary columns, set session character set to cp1251,
    client character set to koi8, and make sure that there is conversion
    on insert and no conversion on select
  */
  rc= mysql_query(mysql,
                  "create table t1 (c1 varbinary(255), c2 varbinary(255))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "set character_set_client=koi8r, "
                         "character_set_connection=cp1251, "
                         "character_set_results=koi8r");
  check_mysql_rc(rc, mysql);

  memset(bind_array, '\0', sizeof(bind_array));

  bind_array[0].buffer_type= MYSQL_TYPE_STRING;

  bind_array[1].buffer_type= MYSQL_TYPE_STRING;
  bind_array[1].buffer= (void *) koi8;
  bind_array[1].buffer_length= (unsigned long)strlen(koi8);

  stmt= mysql_stmt_init(mysql);
  check_stmt_rc(rc, stmt);

  stmt_text= "insert into t1 (c1, c2) values (?, ?)";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  mysql_stmt_bind_param(stmt, bind_array);

  mysql_stmt_send_long_data(stmt, 0, koi8, (unsigned long)strlen(koi8));

  /* Cause a reprepare at statement execute */
  rc= mysql_query(mysql, "alter table t1 add column d int");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  stmt_text= "select c1, c2 from t1";

  /* c1 and c2 are binary so no conversion will be done on select */
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  bind_array[0].buffer= buf1;
  bind_array[0].buffer_length= sizeof(buf1);
  bind_array[0].length= &buf1_len;

  bind_array[1].buffer= buf2;
  bind_array[1].buffer_length= sizeof(buf2);
  bind_array[1].length= &buf2_len;

  mysql_stmt_bind_result(stmt, bind_array);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(buf1_len == strlen(cp1251), "");
  FAIL_UNLESS(buf2_len == strlen(cp1251), "");
  FAIL_UNLESS(!memcmp(buf1, cp1251, buf1_len), "");
  FAIL_UNLESS(!memcmp(buf2, cp1251, buf1_len), "");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "set names default");
  check_mysql_rc(rc, mysql);
  return OK;
}

/**
  Test that COM_REFRESH issues a implicit commit.
*/

static int test_wl4284_1(MYSQL *mysql)
{
  int rc;
  MYSQL_ROW row;
  MYSQL_RES *result;

  diag("Test temporarily disabled");
  return SKIP;

  if (mysql_get_server_version(mysql) < 60000) {
    diag("Test requires MySQL Server version 6.0 or above");
    return SKIP;
  }

  /* set AUTOCOMMIT to OFF */
  rc= mysql_autocommit(mysql, FALSE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS trans");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE trans (a INT) ENGINE=InnoDB");

  if (mysql_errno(mysql) == ER_UNKNOWN_STORAGE_ENGINE)
  {
    diag("InnoDB not configured or available");
    return SKIP;
  }

  check_mysql_rc(rc, mysql);


  rc= mysql_query(mysql, "INSERT INTO trans VALUES(1)");
  check_mysql_rc(rc, mysql);

  rc= mysql_refresh(mysql, REFRESH_GRANT | REFRESH_TABLES);
  check_mysql_rc(rc, mysql);

  rc= mysql_rollback(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM trans");
  check_mysql_rc(rc, mysql);

  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  row= mysql_fetch_row(result);
  FAIL_IF(!row, "Can't fetch row");

  mysql_free_result(result);

  /* set AUTOCOMMIT to OFF */
  rc= mysql_autocommit(mysql, FALSE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE trans");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug49694(MYSQL *mysql)
{
  int rc;
  int i;
  FILE *fp;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS enclist");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE `enclist` ("
                         "  `pat_id` int(11) NOT NULL,"
                         "  `episode_id` int(11) NOT NULL,"
                         "  `enc_id` double NOT NULL,"
                         "  PRIMARY KEY (`pat_id`,`episode_id`,`enc_id`)"
                         ") ENGINE=MyISAM DEFAULT CHARSET=latin1");
  check_mysql_rc(rc, mysql);

  fp= fopen("data.csv", "w");
  FAIL_IF(!fp, "Can't open data.csv");

  for (i=0; i < 100; i++)
    fprintf (fp, "%.08d,%d,%f\r\n", 100 + i, i % 3 + 1, 60000.0 + i/100);
  fclose(fp);

  rc= mysql_query(mysql, "LOAD DATA LOCAL INFILE 'data.csv' INTO TABLE enclist "
                         "FIELDS TERMINATED BY '.' LINES TERMINATED BY '\r\n'");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DELETE FROM enclist");
  check_mysql_rc(rc, mysql);

  FAIL_IF(mysql_affected_rows(mysql) != 100, "Import failure. Expected 2 imported rows");

  rc= mysql_query(mysql, "DROP TABLE enclist");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_conc49(MYSQL *mysql)
{
  int rc;
  MYSQL_RES *res;
  int i;
  FILE *fp= fopen("./sample.csv", "w");
  for (i=1; i < 4; i++)
    fprintf(fp, "\"%d\", \"%d\", \"%d\"\r\n", i, i, i);
  fclose(fp);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc49");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE conc49 (a int, b int, c int) Engine=InnoDB DEFAULT CHARSET=latin1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "LOAD DATA LOCAL INFILE './sample.csv' INTO TABLE conc49 FIELDS ESCAPED BY ' ' TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT a FROM conc49");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 3, "3 rows expected");
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc49");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_ldi_path(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);

#ifdef _WIN32
  rc= mysql_query(mysql, "LOAD DATA LOCAL INFILE 'X:/non_existing_path/data.csv' INTO TABLE t1 "
                         "FIELDS TERMINATED BY '.' LINES TERMINATED BY '\r\n'");
#else
  rc= mysql_query(mysql, "LOAD DATA LOCAL INFILE '/non_existing_path/data.csv' INTO TABLE t1 "
                         "FIELDS TERMINATED BY '.' LINES TERMINATED BY '\r\n'");
#endif
  FAIL_IF(rc== 0, "Error expected");
  diag("Error: %d", mysql_errno(mysql));
  FAIL_IF(mysql_errno(mysql) == 0, "Error expected");

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

#if _WIN32
static int test_conc44(MYSQL *mysql)
{
  char query[1024];
  char *a_filename= "æøå.csv";
  int rc;
  int i;
  FILE *fp;

  rc= mysql_set_character_set(mysql, "latin1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS enclist");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE `enclist` ("
                         "  `pat_id` int(11) NOT NULL,"
                         "  `episode_id` int(11) NOT NULL,"
                         "  `enc_id` double NOT NULL,"
                         "  PRIMARY KEY (`pat_id`,`episode_id`,`enc_id`)"
                         ") ENGINE=MyISAM DEFAULT CHARSET=latin1");
  check_mysql_rc(rc, mysql);

  fp= fopen(a_filename, "w");
  FAIL_IF(!fp, "Can't open file");

  for (i=0; i < 100; i++)
    fprintf (fp, "%.08d,%d,%f\r\n", 100 + i, i % 3 + 1, 60000.0 + i/100);
  fclose(fp);

  sprintf(query, "LOAD DATA LOCAL INFILE '%s' INTO TABLE enclist "
                         "FIELDS TERMINATED BY '.' LINES TERMINATED BY '\r\n'", a_filename);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DELETE FROM enclist");
  check_mysql_rc(rc, mysql);

  FAIL_IF(mysql_affected_rows(mysql) != 100, "Import failure. Expected 2 imported rows");

  rc= mysql_query(mysql, "DROP TABLE enclist");
  check_mysql_rc(rc, mysql);
  return OK;
}
#endif

static int test_connect_attrs(MYSQL *my)
{
  MYSQL *mysql;
  MYSQL_RES *result;
  int rc, len;

  rc= mysql_query(my, "SELECT * FROM performance_schema.session_connect_attrs LIMIT 1");
  if (rc != 0)
  {
    diag("Server doesn't support connection attributes");
    return SKIP;
  }

  result= mysql_store_result(my);
  /* MariaDB Connector/C already sent connection attrs after handshake. So if the table is
     empty, it indicates that the performance schema is disabled */
  if (!mysql_num_rows(result))
  {
    diag("skip: performance_schema not enabled");
    mysql_free_result(result);
    return SKIP;
  }
  mysql_free_result(result);

  mysql= mysql_init(NULL);

  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo0", "bar0");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo1", "bar1");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo2", "bar2");

  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));

  if (!(mysql->server_capabilities & CLIENT_CONNECT_ATTRS))
  {
    diag("Server doesn't support connection attributes");
    return SKIP;
  }

  rc= mysql_query(mysql, "SELECT * FROM performance_schema.session_connect_attrs where attr_name like 'foo%'");
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(result);
  mysql_free_result(result);

  mysql_options(mysql, MYSQL_OPT_CONNECT_ATTR_RESET, NULL);
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo0", "bar0");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo1", "bar1");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo2", "bar2");
  mysql_options(mysql, MYSQL_OPT_CONNECT_ATTR_DELETE, "foo0");
  mysql_options(mysql, MYSQL_OPT_CONNECT_ATTR_DELETE, "foo1");
  mysql_options(mysql, MYSQL_OPT_CONNECT_ATTR_DELETE, "foo2");

  len= (int)mysql->options.extension->connect_attrs_len;

  mysql_close(mysql);

  FAIL_IF(rc < 3, "Expected 3 or more rows");
  FAIL_IF(len != 0, "Expected connection_attr_len=0");

  return OK;
}

static int test_conc_114(MYSQL *mysql)
{
  if (mysql_client_find_plugin(mysql, "foo", 0))
  {
    diag("Null pointer expected");
    return FAIL;
  }
  diag("Error: %s", mysql_error(mysql));
  return OK;
}

/* run with valgrind */
static int test_conc117(MYSQL *unused __attribute__((unused)))
{
  my_bool reconnect= 1;
  MYSQL *my= mysql_init(NULL);
  FAIL_IF(!my_test_connect(my, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));
  
  mysql_kill(my, mysql_thread_id(my));

  mysql_options(my, MYSQL_OPT_RECONNECT, &reconnect);

  mysql_query(my, "SET @a:=1");
  mysql_close(my);

  return OK;
}

static int test_read_timeout(MYSQL *unused __attribute__((unused)))
{
  int timeout= 5, rc;
  MYSQL *my= mysql_init(NULL);
  mysql_options(my, MYSQL_OPT_READ_TIMEOUT, &timeout);
  FAIL_IF(!my_test_connect(my, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));
 
  rc= mysql_query(my, "SELECT SLEEP(50)");

  FAIL_IF(rc == 0, "error expected");
  diag("error: %s", mysql_error(my));
  
  mysql_close(my);

  return OK;
}

#ifdef HAVE_REMOTEIO
void *remote_plugin;
static int test_remote1(MYSQL *mysql)
{
  int rc;

  remote_plugin= (void *)mysql_client_find_plugin(mysql, "remote_io", MARIADB_CLIENT_REMOTEIO_PLUGIN);
  if (!remote_plugin)
  {
    diag("skip - no remote io plugin available");
    diag("error: %s", mysql_error(mysql));
    return SKIP;
  }

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a text)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "LOAD DATA LOCAL INFILE 'http://www.example.com' INTO TABLE t1");
  if (rc && mysql_errno(mysql) == 2058)
  {
    diag("remote_io plugin not available");
    return SKIP;
  }
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_remote2(MYSQL *my)
{
  MYSQL *mysql;

  if (!remote_plugin)
  {
    diag("skip - no remote io plugin available");
    return SKIP;
  }
  mysql= mysql_init(NULL);

  mysql_options(mysql, MYSQL_READ_DEFAULT_FILE, "http://localhost/test.cnf");
  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "test");
  my_test_connect(mysql, hostname, username, password, schema,
                         0, socketname, 0), mysql_error(my);
  diag("port: %d", mysql->port);
  mysql_close(mysql);
  return OK;
}
#endif

#ifndef _WIN32
static int test_mdev12965(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql;
  my_bool reconnect = 0;
  FILE *fp= NULL;
  const char *env= getenv("MYSQL_TMP_DIR");
  char cnf_file1[FN_REFLEN + 1];

  if (travis_test)
    return SKIP;

  if (!env)
    env= "/tmp";

  setenv("HOME", env, 1);

  snprintf(cnf_file1, FN_REFLEN, "%s%c.my.cnf", env, FN_LIBCHAR);

  diag("Config file: %s", cnf_file1);

  FAIL_IF(!access(cnf_file1, R_OK), "access");

  mysql= mysql_init(NULL);
  fp= fopen(cnf_file1, "w");
  FAIL_IF(!fp, "fopen");

  fprintf(fp, "[client]\ndefault-character-set=latin2\nreconnect=1\n");
  fclose(fp);

  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "");
  my_test_connect(mysql, hostname, username, password,
                  schema, 0, socketname, 0);

  remove(cnf_file1);

  FAIL_IF(strcmp(mysql_character_set_name(mysql), "latin2"), "expected charset latin2");
  mysql_get_optionv(mysql, MYSQL_OPT_RECONNECT, &reconnect);
  FAIL_IF(reconnect != 1, "expected reconnect=1");
  mysql_close(mysql);
  return OK;
}
#endif

static int test_get_info(MYSQL *mysql)
{
  size_t sval;
  unsigned int ival;
  char *cval;
  int rc;
  MY_CHARSET_INFO cs;
  MARIADB_CHARSET_INFO *ci;
  char **errors;
   
  rc= mariadb_get_infov(mysql, MARIADB_MAX_ALLOWED_PACKET, &sval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("max_allowed_packet: %lu", (unsigned long)sval);
  rc= mariadb_get_infov(mysql, MARIADB_NET_BUFFER_LENGTH, &sval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("net_buffer_length: %lu", (unsigned long)sval);
  rc= mariadb_get_infov(mysql, MARIADB_CLIENT_VERSION_ID, &sval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("client_version_id: %lu", (unsigned long)sval);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_VERSION_ID, &sval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("server_version_id: %lu", (unsigned long)sval);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_MARIADB_CHARSET_INFO, &cs);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("charset name: %s", cs.csname);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_PVIO_TYPE, &ival);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("connection type: %d", ival);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_PROTOCOL_VERSION_ID, &ival);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("protocol_version: %d", ival);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_TYPE, &cval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("server_type: %s", cval);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_VERSION, &cval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("server_version: %s", cval);
  rc= mariadb_get_infov(mysql, MARIADB_CLIENT_VERSION, &cval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("client_version: %s", cval);
  rc= mariadb_get_infov(mysql, MARIADB_CHARSET_NAME, &ci, "utf8");
  FAIL_IF(rc, "mysql_get_info failed");
  diag("charset_name: %s", ci->csname);
  diag("charset_nr: %d", ci->nr);
  rc= mariadb_get_infov(mysql, MARIADB_CHARSET_ID, &ci, 63);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("charset_name: %s", ci->csname);
  rc= mariadb_get_infov(mysql, MARIADB_CLIENT_ERRORS, &errors);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("error[0]: %s", errors[0]);
  rc= mysql_query(mysql, "DROP TABLE IF exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1),(2)");
  check_mysql_rc(rc, mysql);
  rc= mariadb_get_infov(mysql, MARIADB_CONNECTION_INFO, &cval);
  FAIL_IF(rc, "mysql_get_info failed");
  diag("mariadb_info: %s", cval);
  return OK;
}

static int test_zerofill(MYSQL *mysql)
{
  int rc;
  MYSQL_ROW row;
  MYSQL_RES *res;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int(10) zerofill)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT a FROM t1");
  check_mysql_rc(rc, mysql);

  if ((res= mysql_store_result(mysql)))
  {
    row= mysql_fetch_row(res);
    diag("zerofill: %s", row[0]);
    mysql_free_result(res);
  }
  return OK;
}

static int test_server_status(MYSQL *mysql)
{
  int rc;
  unsigned int server_status;
  MYSQL_STMT *stmt;

  if (mysql_get_server_version(mysql) < 100200)
    return SKIP;

  stmt= mysql_stmt_init(mysql);

  rc= mysql_autocommit(mysql, 1);
  mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_STATUS, &server_status);
  FAIL_IF(!(server_status & SERVER_STATUS_AUTOCOMMIT),
          "autocommit flag not set");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int, b int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 (a) VALUES (1),(2),(3),(4),(5)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "UPDATE t1 SET a=9 WHERE a=8");
  check_mysql_rc(rc, mysql);

  mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_STATUS, &server_status);
  FAIL_IF(!(server_status & SERVER_QUERY_NO_INDEX_USED), "autocommit flag not set");

  rc= mysql_query(mysql, "CREATE SCHEMA test_tmp");
  check_mysql_rc(rc, mysql);

  rc= mysql_select_db(mysql, "test_tmp");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP SCHEMA test_tmp");
  check_mysql_rc(rc, mysql);

  mariadb_get_infov(mysql, MARIADB_CONNECTION_SERVER_STATUS, &server_status);
  FAIL_IF(!(server_status & SERVER_STATUS_DB_DROPPED),
          "DB_DROP flag not set");

  FAIL_IF(!(server_status & SERVER_SESSION_STATE_CHANGED),
          "SESSION_STATE_CHANGED flag not set");

  rc= mysql_select_db(mysql, schema);
  check_mysql_rc(rc, mysql);

  mysql_stmt_close(stmt);

  return OK;
}

static int test_wl6797(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  const char *stmt_text;
  my_ulonglong res;

  if (mysql_get_server_version(mysql) < 50703 ||
      (mariadb_connection(mysql) && mysql_get_server_version(mysql) < 100203))
  {
    diag("Skipping test_wl6797: "
            "tested feature does not exist in versions before MySQL 5.7.3 and MariaDB 10.2\n");
    return OK;
  }
  /* clean up the session */
  rc= mysql_reset_connection(mysql);
  FAIL_UNLESS(rc == 0, "");

  /* do prepare of a query */
  mysql_query(mysql, "use test");
  mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  mysql_query(mysql, "CREATE TABLE t1 (a int)");

  stmt= mysql_stmt_init(mysql);
  stmt_text= "INSERT INTO t1 VALUES (1), (2)";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  /* Execute the insert statement */
  rc= mysql_stmt_execute(stmt);
  check_mysql_rc(rc, mysql);

  /*
   clean the session this should remove the prepare statement
   from the cache.
  */
  rc= mysql_reset_connection(mysql);
  FAIL_UNLESS(rc == 0, "");

  /* this below stmt should report error */
  rc= mysql_stmt_execute(stmt);
  FAIL_IF(rc == 0, "");

  /*
   bug#17653288: MYSQL_RESET_CONNECTION DOES NOT RESET LAST_INSERT_ID
  */

  mysql_query(mysql, "DROP TABLE IF EXISTS t2");
  rc= mysql_query(mysql, "CREATE TABLE t2 (a int NOT NULL PRIMARY KEY"\
                         " auto_increment)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t2 VALUES (null)");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 1, "");
  rc= mysql_reset_connection(mysql);
  FAIL_UNLESS(rc == 0, "");
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");

  rc= mysql_query(mysql, "INSERT INTO t2 VALUES (last_insert_id(100))");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 100, "");
  rc= mysql_reset_connection(mysql);
  FAIL_UNLESS(rc == 0, "");
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");

  mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  mysql_query(mysql, "DROP TABLE IF EXISTS t2");
  mysql_stmt_close(stmt);
  return OK;
}

static int test_conc384(MYSQL *my __attribute__((unused)))
{
  char value[1000];
  int len;
  MYSQL *mysql= mysql_init(NULL);

  memset(&value, 'A', 999);
  value[999]= 0;

  mysql_optionsv(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "foo", value);
  len= (int)mysql->options.extension->connect_attrs_len;
  /* Length: 1 (=len) + 3 (="foo") + 3 (=len) + 999 (="AAA...") = 1006 */
  FAIL_IF(len != 1006, "Wrong length");
  mysql_optionsv(mysql, MYSQL_OPT_CONNECT_ATTR_DELETE, "foo");
  len= (int)mysql->options.extension->connect_attrs_len;
  /* Length should be zero after deleting the connection attribute */
  FAIL_IF(len != 0, "Wrong length");
  mysql_close(mysql);
  return OK;
}

#ifndef _WIN32
static int test_conc395(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql;
  FILE *fp= NULL;
  const char *env= getenv("MYSQL_TMP_DIR");
  char cnf_file1[FN_REFLEN + 1];

  if (travis_test)
    return SKIP;

  if (!env)
    env= "/tmp";

  setenv("HOME", env, 1);

  snprintf(cnf_file1, FN_REFLEN, "%s%c.my.cnf", env, FN_LIBCHAR);

  FAIL_IF(!access(cnf_file1, R_OK), "access");

  mysql= mysql_init(NULL);
  fp= fopen(cnf_file1, "w");
  FAIL_IF(!fp, "fopen");

  /* Mix dash and underscore */
  fprintf(fp, "[client]\ndefault_character-set=latin2\n");
  fclose(fp);

  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "");
  my_test_connect(mysql, hostname, username, password,
                  schema, 0, socketname, 0);

  remove(cnf_file1);

  FAIL_IF(strcmp(mysql_character_set_name(mysql), "latin2"), "expected charset latin2");
  mysql_close(mysql);
  return OK;
}

static int test_sslenforce(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql;
  FILE *fp= NULL;
  const char *env= getenv("MYSQL_TMP_DIR");
  char cnf_file1[FN_REFLEN + 1];

  if (travis_test)
    return SKIP;

  if (!env)
    env= "/tmp";
  setenv("HOME", env, 1);

  snprintf(cnf_file1, FN_REFLEN, "%s%c.my.cnf", env, FN_LIBCHAR);

  if (travis_test)
    return SKIP;


  FAIL_IF(!access(cnf_file1, R_OK), "access");

  mysql= mysql_init(NULL);
  fp= fopen(cnf_file1, "w");
  FAIL_IF(!fp, "fopen");

  /* Mix dash and underscore */
  fprintf(fp, "[client]\nssl_enforce=1\n");
  fclose(fp);

  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "");
  my_test_connect(mysql, hostname, username, password,
                  schema, 0, socketname, 0);

  remove(cnf_file1);

  FAIL_IF(!mysql_get_ssl_cipher(mysql), "no secure connection");
  mysql_close(mysql);
  return OK;
}
#endif


struct my_tests_st my_tests[] = {
  {"test_conc384", test_conc384, TEST_CONNECTION_NONE, 0, NULL, NULL},
#ifndef _WIN32
  {"test_mdev12965", test_mdev12965, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_conc395", test_conc395, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_sslenforce", test_sslenforce, TEST_CONNECTION_NONE, 0, NULL, NULL},
#endif
  {"test_wl6797", test_wl6797, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_server_status", test_server_status, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_read_timeout", test_read_timeout, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
  {"test_zerofill", test_zerofill, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
#ifdef HAVE_REMOTEIO
  {"test_remote1", test_remote1, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_remote2", test_remote2, TEST_CONNECTION_NEW, 0, NULL, NULL},
#endif
  {"test_get_info", test_get_info, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_conc117", test_conc117, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_conc_114", test_conc_114, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_connect_attrs", test_connect_attrs, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_conc49", test_conc49, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_bug28075", test_bug28075, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_bug28505", test_bug28505, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_debug_example", test_debug_example, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_bug29692", test_bug29692, TEST_CONNECTION_NEW, CLIENT_FOUND_ROWS,  NULL, NULL},
  {"test_bug31418", test_bug31418, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_frm_bug", test_frm_bug, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_wl4166_1", test_wl4166_1, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_wl4166_2", test_wl4166_2, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_wl4166_3", test_wl4166_3, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_wl4166_4", test_wl4166_4, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_wl4284_1", test_wl4284_1, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_bug49694", test_bug49694, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_ldi_path", test_ldi_path, TEST_CONNECTION_NEW, 0, NULL, NULL},
#ifdef _WIN32
  {"test_conc44", test_conc44, TEST_CONNECTION_NEW, 0, NULL, NULL},
#endif 
  {NULL, NULL, 0, 0, NULL, 0}
};


int main(int argc, char **argv)
{
  if (argc > 1)
    get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  return(exit_status());
}
