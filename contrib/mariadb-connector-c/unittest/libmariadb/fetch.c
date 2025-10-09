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

/* Generalized fetch conversion routine for all basic types */

static int bind_fetch(MYSQL *mysql, int row_count)
{
  MYSQL_STMT   *stmt;
  int          rc, i, count= row_count;
  int32        data[10];
  int8         i8_data;
  int16        i16_data;
  int          i32_data;
  longlong     i64_data;
  float        f_data;
  double       d_data;
  char         s_data[10];
  ulong        length[10];
  MYSQL_BIND   my_bind[7];
  my_bool      is_null[7];
  char         query[MAX_TEST_QUERY_LENGTH];

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  strcpy(query, "INSERT INTO test_bind_fetch VALUES (?, ?, ?, ?, ?, ?, ?)");
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc,stmt);

  FAIL_UNLESS(mysql_stmt_param_count(stmt) == 7, "ParamCount != 7");

  memset(my_bind, '\0', sizeof(my_bind));

  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_LONG;
    my_bind[i].buffer= (void *) &data[i];
  }
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc,stmt);

  while (count--)
  {
    rc= 10+count;
    for (i= 0; i < (int) array_elements(my_bind); i++)
    {
      data[i]= rc+i;
      rc+= 12;
    }
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc,stmt);
  }

  rc= mysql_commit(mysql);
  check_stmt_rc(rc,stmt);

  mysql_stmt_close(stmt);

  rc= my_stmt_result(mysql, "SELECT * FROM test_bind_fetch");
  FAIL_UNLESS(row_count == rc, "Wrong number of rows");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  strcpy(query, "SELECT * FROM test_bind_fetch");
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc,stmt);

  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].buffer= (void *) &data[i];
    my_bind[i].length= &length[i];
    my_bind[i].is_null= &is_null[i];
  }

  my_bind[0].buffer_type= MYSQL_TYPE_TINY;
  my_bind[0].buffer= (void *)&i8_data;

  my_bind[1].buffer_type= MYSQL_TYPE_SHORT;
  my_bind[1].buffer= (void *)&i16_data;

  my_bind[2].buffer_type= MYSQL_TYPE_LONG;
  my_bind[2].buffer= (void *)&i32_data;

  my_bind[3].buffer_type= MYSQL_TYPE_LONGLONG;
  my_bind[3].buffer= (void *)&i64_data;

  my_bind[4].buffer_type= MYSQL_TYPE_FLOAT;
  my_bind[4].buffer= (void *)&f_data;

  my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[5].buffer= (void *)&d_data;

  my_bind[6].buffer_type= MYSQL_TYPE_STRING;
  my_bind[6].buffer= (void *)&s_data;
  my_bind[6].buffer_length= sizeof(s_data);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc,stmt);

  while (row_count--)
  {
    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc,stmt);

    rc= 10+row_count;

    /* TINY */
    FAIL_UNLESS((int) i8_data == rc, "Invalid value for i8_data");
    FAIL_UNLESS(length[0] == 1, "Invalid length");
    rc+= 13;

    /* SHORT */
    FAIL_UNLESS((int) i16_data == rc, "Invalid value for i16_data");
    FAIL_UNLESS(length[1] == 2, "Invalid length");
    rc+= 13;

    /* LONG */
    FAIL_UNLESS((int) i32_data == rc, "Invalid value for i32_data");
    FAIL_UNLESS(length[2] == 4, "Invalid length");
    rc+= 13;

    /* LONGLONG */
    FAIL_UNLESS((int) i64_data == rc, "Invalid value for i64_data");
    FAIL_UNLESS(length[3] == 8, "Invalid length");
    rc+= 13;

    /* FLOAT */
    FAIL_UNLESS((int)f_data == rc, "Invalid value for f_data");
    FAIL_UNLESS(length[4] == 4, "Invalid length");
    rc+= 13;

    /* DOUBLE */
    FAIL_UNLESS((int)d_data == rc, "Invalid value for d_data");
    FAIL_UNLESS(length[5] == 8, "Invalid length");
    rc+= 13;

    /* CHAR */
    {
      char buff[20];
      long len= sprintf(buff, "%d", rc);
      FAIL_UNLESS(strcmp(s_data, buff) == 0, "Invalid value for s_data");
      FAIL_UNLESS(length[6] == (ulong) len, "Invalid length");
    }
  }
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "Expected MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  return OK;
}


static int test_fetch_seek(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[3];
  MYSQL_ROW_OFFSET row;
  int        rc;
  int32      c1;
  char       c2[11], c3[20];
  const char *query = "SELECT * FROM t1";

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(c1 int primary key auto_increment, c2 char(10), c3 timestamp)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1(c2) values('venu'), ('mysql'), ('open'), ('source')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc,stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&c1;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)c2;
  my_bind[1].buffer_length= sizeof(c2);

  my_bind[2]= my_bind[1];
  my_bind[2].buffer= (void *)c3;
  my_bind[2].buffer_length= sizeof(c3);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc,stmt);


  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  row= mysql_stmt_row_tell(stmt);

  row= mysql_stmt_row_seek(stmt, row);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  row= mysql_stmt_row_seek(stmt, row);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  mysql_stmt_data_seek(stmt, 0);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != MYSQL_NO_DATA, "Expected MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test mysql_stmt_fetch_column() with offset */

static int test_fetch_offset(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  char       data[11], chunk[5];
  ulong      length[2];
  int        rc;
  my_bool    is_null[2];
  const char *query = "SELECT * FROM t1";


  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(a char(10), b mediumblob)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values('abcdefghij', 'klmnopqrstzy'), (null, null)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc,stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)data;
  my_bind[0].buffer_length= 11;
  my_bind[0].is_null= &is_null[0];
  my_bind[0].length= &length[0];

  my_bind[1].buffer_type= MYSQL_TYPE_MEDIUM_BLOB;
  my_bind[1].buffer= NULL;
  my_bind[1].buffer_length= 0;
  my_bind[1].is_null= &is_null[1];
  my_bind[1].length= &length[1];

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  FAIL_IF(!rc, "Error expected");

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc,stmt);
diag("truncation: %d", mysql->options.report_data_truncation);
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_DATA_TRUNCATED, "rc != MYSQL_DATA_TRUNCATED");

  data[0]= '\0';
  rc= mysql_stmt_fetch_column(stmt, &my_bind[0], 0, 0);
  check_stmt_rc(rc,stmt);


  FAIL_IF(!(strncmp(data, "abcdefghij", 11) == 0 && length[0] == 10), "Wrong value");
  FAIL_IF(my_bind[0].error_value, "No truncation, but error is set");

  rc= mysql_stmt_fetch_column(stmt, &my_bind[0], 0, 5);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(data, "fghij", 6) == 0 && length[0] == 10), "Wrong value");
  FAIL_IF(my_bind[0].error_value, "No truncation, but error is set");

  rc= mysql_stmt_fetch_column(stmt, &my_bind[0], 0, 9);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(data, "j", 2) == 0 && length[0] == 10), "Wrong value");
  FAIL_IF(my_bind[0].error_value, "No truncation, but error is set");

  /* Now blob field */
  my_bind[1].buffer= chunk;
  my_bind[1].buffer_length= sizeof(chunk);

  rc= mysql_stmt_fetch_column(stmt, &my_bind[1], 1, 0);
  check_stmt_rc(rc,stmt);

  FAIL_IF(!(strncmp(chunk, "klmno", 5) == 0 && length[1] == 12), "Wrong value");
  FAIL_IF(my_bind[1].error_value == '\0', "Truncation, but error is not set");

  rc= mysql_stmt_fetch_column(stmt, &my_bind[1], 1, 5);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(chunk, "pqrst", 5) == 0 && length[1] == 12), "Wrong value");
  FAIL_IF(my_bind[1].error_value == '\0', "Truncation, but error is not set");

  rc= mysql_stmt_fetch_column(stmt, &my_bind[1], 1, 10);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(chunk, "zy", 2) == 0 && length[1] == 12), "Wrong value");
  FAIL_IF(my_bind[1].error_value, "No truncation, but error is set");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  memset(is_null, 0, sizeof(is_null));

  rc= mysql_stmt_fetch_column(stmt, &my_bind[0], 0, 0);
  check_stmt_rc(rc,stmt);

  FAIL_IF(is_null[0] != 1, "Null flag not set");

  rc= mysql_stmt_fetch_column(stmt, &my_bind[1], 1, 0);
  check_stmt_rc(rc,stmt);

  FAIL_IF(is_null[1] != 1, "Null flag not set");

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc != MYSQL_NO_DATA, "Expected MYSQL_NO_DATA");

  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test mysql_stmt_fetch_column() */

static int test_fetch_column(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  char       c2[20], bc2[20];
  ulong      l1, l2, bl1, bl2;
  int        rc, c1, bc1;
  const char *query= "SELECT * FROM t1 ORDER BY c2 DESC";

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1(c1 int primary key auto_increment, c2 char(10))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1(c2) values('venu'), ('mysql')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc,stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&bc1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &bl1;
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void *)bc2;
  my_bind[1].buffer_length= 7;
  my_bind[1].is_null= 0;
  my_bind[1].length= &bl2;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc,stmt);

  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0); /* No-op at this point */
  FAIL_IF(!rc, "Error expected");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  c2[0]= '\0'; l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)c2;
  my_bind[0].buffer_length= 7;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(c2, "venu", 4) == 0 && l2 == 4), "Expected c2='venu'");

  c2[0]= '\0'; l2= 0;
  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strcmp(c2, "venu") == 0 && l2 == 4), "Expected c2='venu'");

  c1= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&c1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l1;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(c1 == 1 && l1 == 4), "Expected c1=1");

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc,stmt);

  c2[0]= '\0'; l2= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)c2;
  my_bind[0].buffer_length= 7;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l2;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strncmp(c2, "mysq", 4) == 0 && l2 == 5), "Expected c2='mysql'");

  c2[0]= '\0'; l2= 0;
  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(strcmp(c2, "mysql") == 0 && l2 == 5), "Expected c2='mysql'");

  c1= 0;
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void *)&c1;
  my_bind[0].buffer_length= 0;
  my_bind[0].is_null= 0;
  my_bind[0].length= &l1;

  rc= mysql_stmt_fetch_column(stmt, my_bind, 0, 0);
  check_stmt_rc(rc,stmt);
  FAIL_IF(!(c1 == 2 && l1 == 4), "Expected c2=2");

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc!=MYSQL_NO_DATA, "Expected MYSQL_NO_DATA");

  rc= mysql_stmt_fetch_column(stmt, my_bind, 1, 0);
  FAIL_IF(!rc, "Error expected");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test fetch without prior bound buffers */

static int test_fetch_nobuffs(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[4];
  char       str[4][50];
  int        rc;
  const char  *query = "SELECT DATABASE(), CURRENT_USER(), \
                       CURRENT_DATE(), CURRENT_TIME()";

  stmt = mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rc++;

  FAIL_IF(rc != 1, "Expected 1 row");

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void *)str[0];
  my_bind[0].buffer_length= sizeof(str[0]);
  my_bind[1]= my_bind[2]= my_bind[3]= my_bind[0];
  my_bind[1].buffer= (void *)str[1];
  my_bind[2].buffer= (void *)str[2];
  my_bind[3].buffer= (void *)str[3];

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
  {
    rc++;
  }
  FAIL_IF(rc != 1, "Expected 1 row");

  mysql_stmt_close(stmt);

  return OK;
}

/* Test fetch null */

static int test_fetch_null(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  int        i;
  int        nData= 0;
  MYSQL_BIND my_bind[11];
  ulong      length[11];
  my_bool    is_null[11];
  char query[MAX_TEST_QUERY_LENGTH];


  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_fetch_null");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_fetch_null("
                     " col1 tinyint, col2 smallint, "
                     " col3 int, col4 bigint, "
                     " col5 float, col6 double, "
                     " col7 date, col8 time, "
                     " col9 varbinary(10), "
                     " col10 varchar(50), "
                     " col11 char(20))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO test_fetch_null (col11) "
                    "VALUES (1000), (88), (389789)");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  FAIL_IF(rc, mysql_error(mysql));

  /* fetch */
  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < (int) array_elements(my_bind); i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_LONG;
    my_bind[i].is_null= &is_null[i];
    my_bind[i].length= &length[i];
  }
  my_bind[i-1].buffer= (void *)&nData;              /* Last column is not null */

  strcpy((char *)query , "SELECT * FROM test_fetch_null");

  rc= my_stmt_result(mysql, query);
  FAIL_UNLESS(rc == 3, "Exoected 3 rows");

  stmt = mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= 0;
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
  {
    rc++;
    for (i= 0; i < 10; i++)
    {
      FAIL_IF(!is_null[i], "Expected is_null");
    }
    FAIL_UNLESS(nData == 1000 || nData == 88 || nData == 389789, "Wrong value for nData");
    FAIL_UNLESS(is_null[i] == 0, "Exoected !is_null");
    FAIL_UNLESS(length[i] == 4, "Expected length=4");
  }
  FAIL_UNLESS(rc == 3, "Expected 3 rows");
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE test_fetch_null");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test fetching of date, time and ts */

static int test_fetch_date(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  uint       i;
  int        rc;
  long       year;
  char       date[25], my_time[25], ts[25], ts_4[25], ts_6[20], dt[20];
  ulong      d_length, t_length, ts_length, ts4_length, ts6_length,
             dt_length, y_length;
  MYSQL_BIND my_bind[8];
  my_bool    is_null[8];
  ulong      length[8];
  const char *query= "SELECT * FROM test_bind_result";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_result(c1 date, c2 time, \
                                                        c3 timestamp, \
                                                        c4 year, \
                                                        c5 datetime, \
                                                        c6 timestamp, \
                                                        c7 timestamp)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SET SQL_MODE=''");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO test_bind_result VALUES('2002-01-02', \
                                                              '12:49:00', \
                                                              '2002-01-02 17:46:59', \
                                                              2010, \
                                                              '2010-07-10', \
                                                              '2020', '1999-12-29')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  FAIL_IF(rc, mysql_error(mysql));

  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < array_elements(my_bind); i++)
  {
    my_bind[i].is_null= &is_null[i];
    my_bind[i].length= &length[i];
  }

  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1]= my_bind[2]= my_bind[0];

  my_bind[0].buffer= (void *)&date;
  my_bind[0].buffer_length= sizeof(date);
  my_bind[0].length= &d_length;

  my_bind[1].buffer= (void *)&my_time;
  my_bind[1].buffer_length= sizeof(my_time);
  my_bind[1].length= &t_length;

  my_bind[2].buffer= (void *)&ts;
  my_bind[2].buffer_length= sizeof(ts);
  my_bind[2].length= &ts_length;

  my_bind[3].buffer_type= MYSQL_TYPE_LONG;
  my_bind[3].buffer= (void *)&year;
  my_bind[3].length= &y_length;

  my_bind[4].buffer_type= MYSQL_TYPE_STRING;
  my_bind[4].buffer= (void *)&dt;
  my_bind[4].buffer_length= sizeof(dt);
  my_bind[4].length= &dt_length;

  my_bind[5].buffer_type= MYSQL_TYPE_STRING;
  my_bind[5].buffer= (void *)&ts_4;
  my_bind[5].buffer_length= sizeof(ts_4);
  my_bind[5].length= &ts4_length;

  my_bind[6].buffer_type= MYSQL_TYPE_STRING;
  my_bind[6].buffer= (void *)&ts_6;
  my_bind[6].buffer_length= sizeof(ts_6);
  my_bind[6].length= &ts6_length;

  rc= my_stmt_result(mysql, "SELECT * FROM test_bind_result");
  FAIL_UNLESS(rc == 1, "Expected 1 row");

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  ts_4[0]= '\0';
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_UNLESS(strcmp(date, "2002-01-02") == 0, "date != '2002-01-02'");
  FAIL_UNLESS(d_length == 10, "d_length != 10");

  FAIL_UNLESS(strcmp(my_time, "12:49:00") == 0, "mytime != '12:49:00'");
  FAIL_UNLESS(t_length == 8, "t_length != 8");

  FAIL_UNLESS(strcmp(ts, "2002-01-02 17:46:59") == 0, "ts != '2002-01-02 17:46:59'");
  FAIL_UNLESS(ts_length == 19, "ts_length != 19");

  FAIL_UNLESS(strcmp(dt, "2010-07-10 00:00:00") == 0, "dt != 2010-07-10 00:00:00");
  FAIL_UNLESS(dt_length == 19, "dt_length != 19");

  FAIL_UNLESS(strcmp(ts_4, "0000-00-00 00:00:00") == 0, "ts4 != '0000-00-00 00:00:00'");
  FAIL_UNLESS(ts4_length == strlen("0000-00-00 00:00:00"), "ts4_length != 19");

  FAIL_UNLESS(strcmp(ts_6, "1999-12-29 00:00:00") == 0, "ts_6 != '1999-12-29 00:00:00'");
  FAIL_UNLESS(ts6_length == 19, "ts6_length != 19");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_result");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Test fetching of str to all types */

static int test_fetch_str(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 char(10), \
                                                     c2 char(10), \
                                                     c3 char(20), \
                                                     c4 char(20), \
                                                     c5 char(30), \
                                                     c6 char(40), \
                                                     c7 char(20))");
  check_mysql_rc(rc, mysql);

  rc= bind_fetch(mysql, 3);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}

/* Test fetching of long to all types */

static int test_fetch_long(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 int unsigned, \
                                                     c2 int unsigned, \
                                                     c3 int, \
                                                     c4 int, \
                                                     c5 int, \
                                                     c6 int unsigned, \
                                                     c7 int)");
  check_mysql_rc(rc, mysql);
  rc= bind_fetch(mysql, 4);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}


/* Test fetching of short to all types */

static int test_fetch_short(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 smallint unsigned, \
                                                     c2 smallint, \
                                                     c3 smallint unsigned, \
                                                     c4 smallint, \
                                                     c5 smallint, \
                                                     c6 smallint, \
                                                     c7 smallint unsigned)");
  check_mysql_rc(rc, mysql);
  rc= bind_fetch(mysql, 5);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}


/* Test fetching of tiny to all types */

static int test_fetch_tiny(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 tinyint unsigned, \
                                                     c2 tinyint, \
                                                     c3 tinyint unsigned, \
                                                     c4 tinyint, \
                                                     c5 tinyint, \
                                                     c6 tinyint, \
                                                     c7 tinyint unsigned)");
  check_mysql_rc(rc, mysql);
  rc= bind_fetch(mysql, 3);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}


/* Test fetching of longlong to all types */

static int test_fetch_bigint(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 bigint, \
                                                   c2 bigint, \
                                                   c3 bigint unsigned, \
                                                   c4 bigint unsigned, \
                                                   c5 bigint unsigned, \
                                                   c6 bigint unsigned, \
                                                   c7 bigint unsigned)");
  check_mysql_rc(rc, mysql);
  rc= bind_fetch(mysql, 2);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}


/* Test fetching of float to all types */

static int test_fetch_float(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 float(3), \
                                                   c2 float, \
                                                   c3 float unsigned, \
                                                   c4 float, \
                                                   c5 float, \
                                                   c6 float, \
                                                   c7 float(10) unsigned)");
  check_mysql_rc(rc, mysql);

  rc= bind_fetch(mysql, 2);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}


/* Test fetching of double to all types */

static int test_fetch_double(MYSQL *mysql)
{
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE test_bind_fetch(c1 double(5, 2), "
                     "c2 double unsigned, c3 double unsigned, "
                     "c4 double unsigned, c5 double unsigned, "
                     "c6 double unsigned, c7 double unsigned)");
  check_mysql_rc(rc, mysql);
  rc= bind_fetch(mysql, 3);
  mysql_query(mysql, "DROP TABLE IF EXISTS test_bind_fetch");
  return rc;
}

static int test_conc281(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_BIND bind[2];
  unsigned long length= 0;
  char buffer[2048];

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS conc282");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE conc282 (a blob, b varchar(1000), c int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO conc282 VALUES (REPEAT('A',2000), REPEAT('B', 999),3)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, "SELECT a, b FROM conc282", -1);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND) * 2);

  bind[0].buffer_type= MYSQL_TYPE_BLOB;
  bind[0].buffer= buffer;
  bind[0].buffer_length= 2048;
  bind[0].length= &length;

  rc= mysql_stmt_fetch_column(stmt, &bind[0], 0, 0);
  check_stmt_rc(rc, stmt);

  FAIL_IF(length != 2000, "Expected length= 2000");
  FAIL_IF(buffer[0] != 'A' || buffer[1999] != 'A', "Wrong result");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE conc282");
  check_mysql_rc(rc, mysql);

  return OK;

}

struct my_tests_st my_tests[] = {
  {"test_conc281", test_conc281, 1, 0, NULL, NULL},
  {"test_fetch_seek", test_fetch_seek, 1, 0, NULL , NULL},
  {"test_fetch_offset", test_fetch_offset, 1, 0, NULL , NULL},
  {"test_fetch_column", test_fetch_column, 1, 0, NULL , NULL},
  {"test_fetch_nobuffs", test_fetch_nobuffs, 1, 0, NULL , NULL},
  {"test_fetch_null", test_fetch_null, 1, 0, NULL , NULL},
  {"test_fetch_date", test_fetch_date, 1, 0, NULL , NULL},
  {"test_fetch_str", test_fetch_str, 1, 0, NULL , NULL},
  {"test_fetch_long", test_fetch_long, 1, 0, NULL , NULL},
  {"test_fetch_short", test_fetch_short, 1, 0, NULL , NULL},
  {"test_fetch_tiny", test_fetch_tiny, 1, 0, NULL , NULL},
  {"test_fetch_bigint", test_fetch_bigint, 1, 0, NULL , NULL},
  {"test_fetch_float", test_fetch_float, 1, 0, NULL , NULL},
  {"test_fetch_double", test_fetch_double, 1, 0, NULL , NULL},
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
