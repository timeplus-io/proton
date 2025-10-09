/*
  Copyright 2011 Kristian Nielsen and Monty Program Ab.

  This file is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "my_test.h"
#include "ma_common.h"

#define TEST_ARRAY_SIZE 1024

static my_bool bulk_enabled= 0;

char *rand_str(size_t length) {
    const char charset[] = "0123456789"
                     "abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    char *dest= (char *)malloc(length+1);
    char *p= dest;
    while (length-- > 0) {
        *dest++ = charset[rand() % sizeof(charset)];
    }
    *dest = '\0';
    return p;
}

static int check_bulk(MYSQL *mysql)
{
  bulk_enabled= (!(mysql->server_capabilities & CLIENT_MYSQL) &&
      (mysql->extension->mariadb_server_capabilities &
      (MARIADB_CLIENT_STMT_BULK_OPERATIONS >> 32)));
  diag("bulk %ssupported", bulk_enabled ? "" : "not ");
  return OK;
}

static int bulk1(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  const char *stmt_str= "INSERT INTO bulk1 VALUES (?,?)";
  unsigned int array_size= TEST_ARRAY_SIZE;
  int rc;
  unsigned int i;
  char **buffer;
  unsigned long *lengths;
  unsigned int *vals;
  MYSQL_BIND bind[2];
  MYSQL_RES *res;
  MYSQL_ROW row;
  unsigned int intval;

  if (!bulk_enabled)
    return SKIP;

  rc= mysql_select_db(mysql, "testc");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk1 (a int , b VARCHAR(255))");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL(stmt_str));
  check_stmt_rc(rc, stmt);

  /* allocate memory */
  buffer= calloc(TEST_ARRAY_SIZE, sizeof(char *));
  lengths= (unsigned long *)calloc(sizeof(long), TEST_ARRAY_SIZE);
  vals= (unsigned int *)calloc(sizeof(int), TEST_ARRAY_SIZE);

  for (i=0; i < TEST_ARRAY_SIZE; i++)
  {
    buffer[i]= rand_str(254);
    lengths[i]= -1;
    vals[i]= i;
  }

  memset(bind, 0, sizeof(MYSQL_BIND) * 2);
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= vals;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer= (void *)buffer;
  bind[1].length= (unsigned long *)lengths;

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  for (i=0; i < 100; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    FAIL_IF(mysql_stmt_affected_rows(stmt) != TEST_ARRAY_SIZE, "affected_rows != TEST_ARRAY_SIZE");
  }

  for (i=0; i < array_size; i++)
    free(buffer[i]);

  free(buffer);
  free(lengths);
  free(vals);

  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT COUNT(*) FROM bulk1");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);
  intval= atoi(row[0]);
  mysql_free_result(res);
  FAIL_IF(intval != array_size * 100, "Expected 102400 rows");

  rc= mysql_query(mysql, "SELECT MAX(a) FROM bulk1");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);
  intval= atoi(row[0]);
  mysql_free_result(res);
  FAIL_IF(intval != array_size - 1, "Expected max value 1024");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk1");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bulk2(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  MYSQL_BIND bind[2];
  unsigned int i;
  unsigned int array_size=1024;
  char indicator[1024];
  long lval[1024];

  if (!bulk_enabled)
    return SKIP;
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk2 (a int default 4, b int default 2)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk2 VALUES (?,1)"));
  check_stmt_rc(rc, stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));

  for (i=0; i < array_size; i++)
  {
    indicator[i]= STMT_INDICATOR_DEFAULT;
    lval[i]= i;
  }

  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].u.indicator= indicator;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= &lval;

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk2");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int bulk3(MYSQL *mysql)
{
  struct st_bulk3 {
    char char_value[20];
    unsigned long length;
    int  int_value;
  };

  struct st_bulk3 val[3]= {{"Row 1", 5, 1},
                           {"Row 02", 6, 2},
                           {"Row 003", 7, 3}};
  int rc;
  MYSQL_BIND bind[2];
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  size_t row_size= sizeof(struct st_bulk3);
  int array_size= 3;

  if (!bulk_enabled)
    return SKIP;
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk3");
  check_mysql_rc(rc,mysql);
  rc= mysql_query(mysql, "CREATE TABLE bulk3 (name varchar(20), row int)");
  check_mysql_rc(rc,mysql);

  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk3 VALUES (?,?)"));
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND)*2);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);
  check_stmt_rc(rc, stmt);

  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].buffer= &val[0].char_value;
  bind[0].length= &val[0].length;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= &val[0].int_value;

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk3");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bulk4(MYSQL *mysql)
{
  struct st_bulk4 {
    char char_value[20];
    char indicator1;
    int  int_value;
    char indicator2;
  };

  struct st_bulk4 val[]= {{"Row 1", STMT_INDICATOR_NTS, 0, STMT_INDICATOR_DEFAULT},
                          {"Row 2", STMT_INDICATOR_NTS, 0, STMT_INDICATOR_DEFAULT},
                          {"Row 3", STMT_INDICATOR_NTS, 0, STMT_INDICATOR_DEFAULT}};
  int rc;
  MYSQL_BIND bind[2];
  MYSQL_RES *res;
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  size_t row_size= sizeof(struct st_bulk4);
  int array_size= 3;
  unsigned long lengths[3]= {-1, -1, -1};

  if (!bulk_enabled)
    return SKIP;
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk4");
  check_mysql_rc(rc,mysql);
  rc= mysql_query(mysql, "CREATE TABLE bulk4 (name varchar(20), row int not null default 3)");
  check_mysql_rc(rc,mysql);

  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk4 VALUES (?,?)"));
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND)*2);
  
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);
  check_stmt_rc(rc, stmt);

  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].u.indicator= &val[0].indicator1;
  bind[0].buffer= &val[0].char_value;
  bind[0].length= lengths;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].u.indicator= &val[0].indicator2;

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM bulk4 WHERE row=3");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 3, "expected 3 rows");
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk4");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bulk_null(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  MYSQL_BIND bind[2];
  unsigned int param_count= 2;
  unsigned int array_size= 2;
  unsigned long lengths[2]= {-1, -1};
  char **buf= calloc(1, 2 * sizeof(char *));

  if (!bulk_enabled)
  {
    free(buf);
    return SKIP;
  }

  buf[0]= strdup("foo");
  buf[1]= strdup("foobar");

  rc= mariadb_stmt_execute_direct(stmt, "DROP TABLE IF EXISTS bulk_null", -1);
  check_stmt_rc(rc, stmt);

  rc= mariadb_stmt_execute_direct(stmt, "CREATE TABLE bulk_null (a int not null auto_increment primary key, b varchar(20))", -1);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer_type= MYSQL_TYPE_NULL;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer= buf;
  bind[1].length= lengths;

  mysql_stmt_close(stmt);
  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mariadb_stmt_execute_direct(stmt, "INSERT INTO bulk_null VALUES (?, ?)", -1);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);
  free(buf[0]);
  free(buf[1]);
  free(buf);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk_null");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bulk5(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_BIND bind[3];
  MYSQL_RES *res;
  unsigned long rows;
  unsigned int array_size= 5;
  int rc;
  int intval[]= {12,13,14,15,16};
  int id[]= {1,2,3,4,5};

  if (!bulk_enabled)
    return SKIP;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk5");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk5 (a int, b int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO bulk5 VALUES (1,1), (2,2), (3,3), (4,4), (5,5)");
  check_mysql_rc(rc, mysql);


  memset(bind, 0, sizeof(MYSQL_BIND) * 3);

  rc= mysql_stmt_prepare(stmt, SL("UPDATE bulk5 SET a=? WHERE a=?"));
  check_stmt_rc(rc, stmt);

  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= &intval;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= &id;

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM bulk5 WHERE a=b+11");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  rows= (unsigned long)mysql_num_rows(res);
  diag("rows: %lu", rows);
  mysql_free_result(res);

  FAIL_IF(rows != 5, "expected 5 rows");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk5");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int bulk6(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_BIND bind[3];
  MYSQL_RES *res;
  unsigned long rows;
  unsigned int array_size= 5;
  int rc;
  int intval[]= {12,13,14,15,16};
  int id[]= {1,2,3,4,5};
  char indicator[5];

  if (!bulk_enabled)
    return SKIP;
  memset(indicator, STMT_INDICATOR_IGNORE, 5);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk6");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk6 (a int, b int default 4)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO bulk6 VALUES (1,1), (2,2), (3,3), (4,4), (5,5)");
  check_mysql_rc(rc, mysql);


  memset(bind, 0, sizeof(MYSQL_BIND) * 3);

  /* 1st case: UPDATE */
  rc= mysql_stmt_prepare(stmt, SL("UPDATE bulk6 SET a=?, b=? WHERE a=?"));
  check_stmt_rc(rc, stmt);

  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= &intval;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= &intval;
  bind[1].u.indicator= indicator;
  bind[2].buffer_type= MYSQL_TYPE_LONG;
  bind[2].buffer= &id;

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM bulk6 WHERE a=b+11");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  rows= (unsigned long)mysql_num_rows(res);
  mysql_free_result(res);

  FAIL_IF(rows != 5, "expected 5 rows");

  /* 2nd case: INSERT - ignore indicator should be same as default */
  rc= mysql_query(mysql, "DELETE FROM bulk6");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk6 VALUES (?,?)"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  /* this should insert 5 default values (=4) */
  memset(indicator, STMT_INDICATOR_DEFAULT, 5);
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  /* this should insert 5 default values (=4) */
  memset(indicator, STMT_INDICATOR_IGNORE, 5);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM bulk6 WHERE b=4");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  rows= (unsigned long)mysql_num_rows(res);
  mysql_free_result(res);

  FAIL_IF(rows != 10, "expected 10 rows");
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk6");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_conc243(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[3];
  MYSQL_RES  *result;
  MYSQL_ROW  row;

  struct st_data {
    unsigned long id;
    char id_ind;
    char forename[30];
    char forename_ind;
    char surname[30];
    char surname_ind;
  };

  struct st_data data[]= {
    {0, STMT_INDICATOR_NULL, "Monty", STMT_INDICATOR_NTS, "Widenius", STMT_INDICATOR_NTS},
    {0, STMT_INDICATOR_NULL, "David", STMT_INDICATOR_NTS, "Axmark", STMT_INDICATOR_NTS},
    {0, STMT_INDICATOR_NULL, "default", STMT_INDICATOR_DEFAULT, "N.N.", STMT_INDICATOR_NTS},
  };

  unsigned int array_size= 1;
  size_t row_size= sizeof(struct st_data);
  int rc;

  if (!bulk_enabled)
    return SKIP;
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk_example2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk_example2 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"\
                         "forename CHAR(30) NOT NULL DEFAULT 'unknown', surname CHAR(30))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk_example2 VALUES (?,?,?)"));
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND) * 3);

  /* We autogenerate id's, so all indicators are STMT_INDICATOR_NULL */
  bind[0].u.indicator= &data[0].id_ind;
  bind[0].buffer_type= MYSQL_TYPE_LONG;

  bind[1].buffer= &data[0].forename;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].u.indicator= &data[0].forename_ind;

  bind[2].buffer_type= MYSQL_TYPE_STRING;
  bind[2].buffer= &data[0].surname;
  bind[2].u.indicator= &data[0].surname_ind;

  /* set array size */
  mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);

  /* set row size */
  mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

  /* bind parameter */
  mysql_stmt_bind_param(stmt, bind);

  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT forename, surname FROM bulk_example2");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result || !mysql_num_rows(result), "Invalid resultset");
  row = mysql_fetch_row(result);
  if (strcmp(row[0], "Monty") || strcmp(row[1], "Widenius"))
  {
    mysql_free_result(result);
    diag("Wrong values");
    return FAIL;
  }
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE bulk_example2");
  check_mysql_rc(rc, mysql);
  return OK;
}
static int bulk7(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;
  int array_size= 5;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1)");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("UPDATE t1 SET a=a+1"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);

  FAIL_IF(!rc, "Error expected: Bulk operation without parameters is not supported");
  diag("%s", mysql_stmt_error(stmt));

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_char_conv1(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  MYSQL_BIND bind_in, bind_out;
  char buffer[100];
  char outbuffer[100];

  if (!bulk_enabled)
    return SKIP;
  stmt= mysql_stmt_init(mysql);
  strcpy (buffer, "\xC3\x82\xC3\x83\xC3\x84\x00");

  rc= mysql_query(mysql, "SET NAMES UTF8");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS char_conv");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE char_conv (a varchar(20)) CHARSET=latin1");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO char_conv VALUES (?)"));
  check_stmt_rc(rc, stmt);

  memset(&bind_in, 0, sizeof(MYSQL_BIND));
  bind_in.buffer_type= MYSQL_TYPE_STRING;
  bind_in.buffer_length= -1;
  bind_in.buffer= &buffer;

  rc= mysql_stmt_bind_param(stmt, &bind_in);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, SL("SELECT a from char_conv"));
  check_stmt_rc(rc, stmt);

  memset(&bind_out, 0, sizeof(MYSQL_BIND));
  bind_out.buffer_type= MYSQL_TYPE_STRING;
  bind_out.buffer_length= 100;
  bind_out.buffer= outbuffer;

  rc= mysql_stmt_bind_result(stmt, &bind_out);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc == MYSQL_NO_DATA, "Error");

  mysql_stmt_close(stmt);


  if (strcmp(buffer, outbuffer))
  {
    diag("Error: Expected '%s' instead of '%s'", buffer, outbuffer);
    return FAIL;
  }

  rc= mysql_query(mysql, "DROP TABLE char_conv");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_char_conv2(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  int array_size= 1;
  MYSQL_BIND bind_in, bind_out;
  char *buffer[1];
  char outbuffer[100];

  if (!bulk_enabled)
    return SKIP;

  stmt= mysql_stmt_init(mysql);
  buffer[0]= calloc(1, 7);
  strcpy (buffer[0], "\xC3\x82\xC3\x83\xC3\x84\x00");

  rc= mysql_query(mysql, "SET NAMES UTF8");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS char_conv");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE char_conv (a varchar(20)) CHARSET=latin1");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO char_conv VALUES (?)"));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);

  memset(&bind_in, 0, sizeof(MYSQL_BIND));
  bind_in.buffer_type= MYSQL_TYPE_STRING;
  bind_in.buffer_length= -1;
  bind_in.buffer= &buffer;

  rc= mysql_stmt_bind_param(stmt, &bind_in);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, SL("SELECT a from char_conv"));
  check_stmt_rc(rc, stmt);

  memset(&bind_out, 0, sizeof(MYSQL_BIND));
  bind_out.buffer_type= MYSQL_TYPE_STRING;
  bind_out.buffer_length= 100;
  bind_out.buffer= outbuffer;

  rc= mysql_stmt_bind_result(stmt, &bind_out);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(rc == MYSQL_NO_DATA, "Error");

  mysql_stmt_close(stmt);


  if (strcmp(buffer[0], outbuffer))
  {
    diag("Error: Expected '%s' instead of '%s'", buffer[0], outbuffer);
    return FAIL;
  }
  free(buffer[0]);

  rc= mysql_query(mysql, "DROP TABLE char_conv");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int bulk_skip_row(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[3];
  MYSQL_RES  *result;
  MYSQL_ROW  row;

  struct st_data {
    unsigned long id;
    char id_ind;
    char forename[30];
    char forename_ind;
    char surname[30];
    char surname_ind;
  };

  struct st_data data[]={
    { 0, STMT_INDICATOR_NULL, "Monty", STMT_INDICATOR_NTS, "Widenius", STMT_INDICATOR_IGNORE_ROW },
    { 0, STMT_INDICATOR_IGNORE_ROW, "David", STMT_INDICATOR_NTS, "Axmark", STMT_INDICATOR_NTS },
    { 0, STMT_INDICATOR_NULL, "default", STMT_INDICATOR_DEFAULT, "N.N.", STMT_INDICATOR_NTS },
  };

  unsigned int array_size= 3;
  size_t row_size= sizeof(struct st_data);
  int rc;

  if (!bulk_enabled)
    return SKIP;
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk_example2");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE bulk_example2 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"\
    "forename CHAR(30) NOT NULL DEFAULT 'unknown', surname CHAR(30))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL("INSERT INTO bulk_example2 VALUES (?,?,?)"));
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND) * 3);

  /* We autogenerate id's, so all indicators are STMT_INDICATOR_NULL */
  bind[0].u.indicator= &data[0].id_ind;
  bind[0].buffer_type= MYSQL_TYPE_LONG;

  bind[1].buffer= &data[0].forename;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].u.indicator= &data[0].forename_ind;

  bind[2].buffer_type= MYSQL_TYPE_STRING;
  bind[2].buffer= &data[0].surname;
  bind[2].u.indicator= &data[0].surname_ind;

  /* set array size */
  mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);

  /* set row size */
  mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

  /* bind parameter */
  mysql_stmt_bind_param(stmt, bind);

  /* execute */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT forename, surname FROM bulk_example2");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result || mysql_num_rows(result) != 1, "Invalid resultset");
  
  row = mysql_fetch_row(result);
  if (strcmp(row[0], "unknown") || strcmp(row[1], "N.N."))
  {
    mysql_free_result(result);
    diag("Wrong values");
    return FAIL;
  }
  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE bulk_example2");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int bulk_null_null(MYSQL *mysql)
{
  struct st_bulk4 {
    char char_value[20];
    char indicator1;
    int  int_value;
    char indicator2;
    double double_value;
    char indicator3;
    char time_value[20];
    char indicator4;
    char decimal_value[4];
    char indicator5;
  };

  struct st_bulk4 val[]= {{"3",      STMT_INDICATOR_NTS,
                           3,        STMT_INDICATOR_NONE,
                           3.0,      STMT_INDICATOR_NONE,
                           "00:00:00",  STMT_INDICATOR_NTS,
                           "3.0",    STMT_INDICATOR_NTS},
                          {"3",      STMT_INDICATOR_NULL,
                           3,        STMT_INDICATOR_NULL,
                           3.0,      STMT_INDICATOR_NULL,
                           "00:00:00",  STMT_INDICATOR_NULL,
                           "3.0",    STMT_INDICATOR_NULL},
                          {"3",      STMT_INDICATOR_NTS,
                           3,        STMT_INDICATOR_NONE,
                           3.0,      STMT_INDICATOR_NONE,
                           "00:00:00",  STMT_INDICATOR_NTS,
                           "3.0",    STMT_INDICATOR_NTS}};
  int rc;
  MYSQL_BIND bind[5];
  MYSQL_RES *res;
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  size_t row_size= sizeof(struct st_bulk4);
  int array_size= 3;
  unsigned long server_version= mysql_get_server_version(mysql);
  unsigned long lengths[3]= {-1, -1, -1};

  if (!bulk_enabled)
    return SKIP;

  if (server_version > 100300 &&
      server_version < 100305)
    return SKIP;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bulk_null");
  check_mysql_rc(rc,mysql);
  rc= mysql_query(mysql, "CREATE TABLE bulk_null "
                         "(s varchar(20), "
                         " i int, "
                         " d double, "
                         " t time, "
                         " c decimal(3,1))");
  check_mysql_rc(rc,mysql);

  rc= mysql_stmt_prepare(stmt, "INSERT INTO bulk_null VALUES (?,?,?,?,?)", -1);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND)*2);

  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);
  check_stmt_rc(rc, stmt);

  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].u.indicator= &val[0].indicator1;
  bind[0].buffer= &val[0].char_value;
  bind[0].length= lengths;
  bind[1].buffer_type= MYSQL_TYPE_LONG;
  bind[1].buffer= &val[0].int_value;
  bind[1].u.indicator= &val[0].indicator2;
  bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
  bind[2].buffer= &val[0].double_value;
  bind[2].u.indicator= &val[0].indicator3;
  bind[3].buffer_type= MYSQL_TYPE_STRING;
  bind[3].u.indicator= &val[0].indicator4;
  bind[3].buffer= &val[0].time_value;
  bind[3].length= lengths;
  bind[4].buffer_type= MYSQL_TYPE_NEWDECIMAL;
  bind[4].u.indicator= &val[0].indicator5;
  bind[4].buffer= &val[0].decimal_value;
  bind[4].length= lengths;

  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "SELECT * FROM bulk_null WHERE s='3'");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 2, "expected 2 rows");

  rc= mysql_query(mysql, "SELECT * FROM bulk_null WHERE i=3");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 2, "expected 2 rows");

  rc= mysql_query(mysql, "SELECT * FROM bulk_null WHERE d=3.0");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 2, "expected 2 rows");

  rc= mysql_query(mysql, "SELECT * FROM bulk_null WHERE t='00:00:00'");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 2, "expected 2 rows");

  rc= mysql_query(mysql, "SELECT * FROM bulk_null WHERE c=3.0");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  rc= (int)mysql_num_rows(res);
  mysql_free_result(res);
  FAIL_IF(rc != 2, "expected 2 rows");

  rc= mysql_query(mysql, "DROP TABLE bulk_null");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_mdev16593(MYSQL *mysql)
{
  int i;
  int rc;
  MYSQL_BIND bind[2];
  unsigned int array_size= 2;
  int val_a[2]= {1,2};
  char indicators[2]= {STMT_INDICATOR_NULL, STMT_INDICATOR_NULL};
  const char *testcase[]= {"MYSQL_TYPE_LONG", "MYSQL_TYPE_NULL", "STMT_INDICATOR_NULL"};

  diag("waiting for server fix");
  return SKIP;

  for (i=0; i < 3; i++)
  {
    MYSQL_RES *res;
    MYSQL_ROW row;
    MYSQL_STMT *stmt= mysql_stmt_init(mysql);
    rc= mysql_query(mysql, "CREATE OR REPLACE TABLE t1 (a int not null auto_increment primary key, b int)");
    check_mysql_rc(rc, mysql);

    memset(&bind, 0, sizeof(MYSQL_BIND));
    switch (i) {
    case 0:
      bind[0].buffer_type= MYSQL_TYPE_LONG;
      break;
    case 1:
      bind[0].buffer_type= MYSQL_TYPE_NULL;
      break;
    case 2:
      bind[0].buffer_type= MYSQL_TYPE_LONG;
      bind[0].u.indicator= indicators;
      break;
    }
    bind[0].buffer= val_a;
    bind[1].buffer_type= MYSQL_TYPE_LONG;
    bind[1].buffer= val_a;

    rc= mysql_stmt_prepare(stmt, SL("insert into t1 values(?,?)"));
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_bind_param(stmt, bind);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_query(mysql, "COMMIT");
    check_mysql_rc(rc, mysql);

    diag("Insert id with buffer_type %s: %lld", 
        testcase[i],
        mysql_stmt_insert_id(stmt));

    rc= mysql_query(mysql, "SELECT max(a) FROM t1");
    check_mysql_rc(rc, mysql);

    res= mysql_store_result(mysql);
    row= mysql_fetch_row(res);
    diag("Max value for t1.a=%s", row[0]);
    mysql_free_result(res);

    mysql_stmt_close(stmt);
  }
  return OK;
}

struct my_tests_st my_tests[] = {
  {"check_bulk", check_bulk, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_mdev16593", test_mdev16593, TEST_CONNECTION_NEW, 0,  NULL,  NULL},
  {"bulk_null_null", bulk_null_null, TEST_CONNECTION_NEW, 0,  NULL,  NULL},
  {"test_char_conv1", test_char_conv1, TEST_CONNECTION_NEW, 0,  NULL,  NULL},
  {"test_char_conv2", test_char_conv2, TEST_CONNECTION_NEW, 0,  NULL,  NULL},
  {"test_conc243", test_conc243, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"update_no_param", bulk7, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk5", bulk5, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk6", bulk6, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk1", bulk1, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk2", bulk2, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk3", bulk3, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk4", bulk4, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk_null", bulk_null, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bulk_skip_row", bulk_skip_row, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
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
