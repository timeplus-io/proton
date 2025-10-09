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

/* helper functions */
enum { MAX_COLUMN_LENGTH= 255 };

typedef struct st_stmt_fetch
{
  const char *query;
  unsigned stmt_no;
  MYSQL_STMT *handle;
  my_bool is_open;
  MYSQL_BIND *bind_array;
  char **out_data;
  unsigned long *out_data_length;
  unsigned column_count;
  unsigned row_count;
} Stmt_fetch;

MYSQL_STMT *open_cursor(MYSQL *mysql, const char *query)
{
  int rc;
  const ulong type= (ulong)CURSOR_TYPE_READ_ONLY;

  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  if (rc) {
    diag("Error: %s", mysql_stmt_error(stmt));
    return NULL;
  }
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  return stmt;
}

/*
  Create statement handle, prepare it with statement, execute and allocate
  fetch buffers.
*/

int stmt_fetch_init(MYSQL *mysql, Stmt_fetch *fetch, unsigned int stmt_no_arg,
                     const char *query_arg)
{
  unsigned long type= CURSOR_TYPE_READ_ONLY;
  int rc;
  unsigned int i;
  MYSQL_RES *metadata;

  /* Save query and statement number for error messages */
  fetch->stmt_no= stmt_no_arg;
  fetch->query= query_arg;

  fetch->handle= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(fetch->handle, SL(fetch->query));
  FAIL_IF(rc, mysql_stmt_error(fetch->handle));

  /*
    The attribute is sent to server on execute and asks to open read-only
    for result set
  */
  mysql_stmt_attr_set(fetch->handle, STMT_ATTR_CURSOR_TYPE,
                      (const void*) &type);

  rc= mysql_stmt_execute(fetch->handle);
  FAIL_IF(rc, mysql_stmt_error(fetch->handle));

  /* Find out total number of columns in result set */
  metadata= mysql_stmt_result_metadata(fetch->handle);
  fetch->column_count= mysql_num_fields(metadata);
  mysql_free_result(metadata);

  /*
    Now allocate bind handles and buffers for output data:
    calloc memory to reduce number of MYSQL_BIND members we need to
    set up.
  */

  fetch->bind_array= (MYSQL_BIND *) calloc(1, sizeof(MYSQL_BIND) *
                                              fetch->column_count);
  fetch->out_data= (char**) calloc(1, sizeof(char*) * fetch->column_count);
  fetch->out_data_length= (ulong*) calloc(1, sizeof(ulong) *
                                             fetch->column_count);
  for (i= 0; i < fetch->column_count; ++i)
  {
    fetch->out_data[i]= (char*) calloc(1, MAX_COLUMN_LENGTH);
    fetch->bind_array[i].buffer_type= MYSQL_TYPE_STRING;
    fetch->bind_array[i].buffer= fetch->out_data[i];
    fetch->bind_array[i].buffer_length= MAX_COLUMN_LENGTH;
    fetch->bind_array[i].length= fetch->out_data_length + i;
  }

  mysql_stmt_bind_result(fetch->handle, fetch->bind_array);

  fetch->row_count= 0;
  fetch->is_open= TRUE;

  /* Ready for reading rows */
  return OK;
}


int fill_tables(MYSQL *mysql, const char **query_list, unsigned query_count)
{
  int rc;
  const char **query;
  for (query= query_list; query < query_list + query_count;
       ++query)
  {
    rc= mysql_query(mysql, *query);
    check_mysql_rc(rc, mysql);
  }
  return OK;
}

int stmt_fetch_fetch_row(Stmt_fetch *fetch)
{
  int rc;
  unsigned i;

  if ((rc= mysql_stmt_fetch(fetch->handle)) == 0)
  {
    ++fetch->row_count;
    for (i= 0; i < fetch->column_count; ++i)
    {
      fetch->out_data[i][fetch->out_data_length[i]]= '\0';
    }
  }
  else
    fetch->is_open= FALSE;

  return rc;
}

void stmt_fetch_close(Stmt_fetch *fetch)
{
  unsigned i;

  for (i= 0; i < fetch->column_count; ++i)
    free(fetch->out_data[i]);
  free(fetch->out_data);
  free(fetch->out_data_length);
  free(fetch->bind_array);
  mysql_stmt_close(fetch->handle);
}



enum fetch_type { USE_ROW_BY_ROW_FETCH= 0, USE_STORE_RESULT= 1 };

int fetch_n(MYSQL *mysql, const char **query_list, unsigned query_count,
                enum fetch_type fetch_type)
{
  unsigned open_statements= query_count;
  int rc, error_count= 0;
  Stmt_fetch *fetch_array= (Stmt_fetch*) calloc(1, sizeof(Stmt_fetch) *
                                                  query_count);
  Stmt_fetch *fetch;

  for (fetch= fetch_array; fetch < fetch_array + query_count; ++fetch)
  {
    if (stmt_fetch_init(mysql, fetch, (unsigned int)(fetch - fetch_array),
                    query_list[fetch - fetch_array]))
      return FAIL;
  }

  if (fetch_type == USE_STORE_RESULT)
  {
    for (fetch= fetch_array; fetch < fetch_array + query_count; ++fetch)
    {
      rc= mysql_stmt_store_result(fetch->handle);
      FAIL_IF(rc, mysql_stmt_error(fetch->handle));
    }
  }

  while (open_statements)
  {
    for (fetch= fetch_array; fetch < fetch_array + query_count; ++fetch)
    {
      if (fetch->is_open && (rc= stmt_fetch_fetch_row(fetch)))
      {
        open_statements--;
        /*
          We try to fetch from the rest of the statements in case of
          error
        */
        if (rc != MYSQL_NO_DATA)
          error_count++;
      }
    }
  }
  if (!error_count)
  {
    unsigned total_row_count= 0;
    for (fetch= fetch_array; fetch < fetch_array + query_count; ++fetch)
      total_row_count+= fetch->row_count;
  }
  for (fetch= fetch_array; fetch < fetch_array + query_count; ++fetch)
    stmt_fetch_close(fetch);
  free(fetch_array);

  return (error_count) ? FAIL:OK;
}

static int test_basic_cursors(MYSQL *mysql)
{
  const char *basic_tables[]=
  {
    "DROP TABLE IF EXISTS t1, t2",

    "CREATE TABLE t1 "
    "(id INTEGER NOT NULL PRIMARY KEY, "
    " name VARCHAR(20) NOT NULL)",

    "INSERT INTO t1 (id, name) VALUES "
    "  (2, 'Ja'), (3, 'Ede'), "
    "  (4, 'Haag'), (5, 'Kabul'), "
    "  (6, 'Almere'), (7, 'Utrecht'), "
    "  (8, 'Qandahar'), (9, 'Amsterdam'), "
    "  (10, 'Amersfoort'), (11, 'Constantine')",

    "CREATE TABLE t2 "
    "(id INTEGER NOT NULL PRIMARY KEY, "
    " name VARCHAR(20) NOT NULL)",

    "INSERT INTO t2 (id, name) VALUES "
    "  (4, 'Guam'), (5, 'Aruba'), "
    "  (6, 'Angola'), (7, 'Albania'), "
    "  (8, 'Anguilla'), (9, 'Argentina'), "
    "  (10, 'Azerbaijan'), (11, 'Afghanistan'), "
    "  (12, 'Burkina Faso'), (13, 'Faroe Islands')"
  };

  const char *queries[]=
  {
    "SELECT * FROM t1",
    "SELECT * FROM t2"
  };

  
  FAIL_IF(fill_tables(mysql, basic_tables, sizeof(basic_tables)/sizeof(*basic_tables)), "fill_tables failed");

  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_ROW_BY_ROW_FETCH), "fetch_n failed");
  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_STORE_RESULT), "fetch_n failed");
  return OK;
}


static int test_cursors_with_union(MYSQL *mysql)
{
  const char *queries[]=
  {
    "SELECT t1.name FROM t1 UNION SELECT t2.name FROM t2",
    "SELECT t1.id FROM t1 WHERE t1.id < 5"
  };
  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_ROW_BY_ROW_FETCH), "fetch_n failed");
  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_STORE_RESULT), "fetch_n failed");

  return OK;
}


static int test_cursors_with_procedure(MYSQL *mysql)
{
  const char *queries[]=
  {
    "SELECT * FROM t1 procedure analyse()"
  };
  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_ROW_BY_ROW_FETCH), "fetch_n failed");
  FAIL_IF(fetch_n(mysql, queries, sizeof(queries)/sizeof(*queries), USE_STORE_RESULT), "fetch_n failed");

  return OK;
}

/*
  Bug#21206: memory corruption when too many cursors are opened at once

  Memory corruption happens when more than 1024 cursors are open
  simultaneously.
*/
static int test_bug21206(MYSQL *mysql)
{
  int retcode= OK;

  const size_t cursor_count= 1025;

  const char *create_table[]=
  {
    "DROP TABLE IF EXISTS t1",
    "CREATE TABLE t1 (i INT)",
    "INSERT INTO t1 VALUES (1), (2), (3)"
  };
  const char *query= "SELECT * FROM t1";

  Stmt_fetch *fetch_array=
    (Stmt_fetch*) calloc(cursor_count, sizeof(Stmt_fetch));

  Stmt_fetch *fetch;

  FAIL_IF(fill_tables(mysql, create_table, sizeof(create_table) / sizeof(*create_table)), "fill_tables failed");

  for (fetch= fetch_array; fetch < fetch_array + cursor_count; ++fetch)
  {
    if ((retcode= stmt_fetch_init(mysql, fetch, (unsigned int)(fetch - fetch_array), query)))
      break;
  }

  for (fetch= fetch_array; fetch < fetch_array + cursor_count; ++fetch)
    stmt_fetch_close(fetch);

  free(fetch_array);

  return retcode;
}

static int test_bug10729(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char a[21];
  int rc;
  const char *stmt_text;
  int i= 0;
  const char *name_array[3]= { "aaa", "bbb", "ccc" };
  ulong type;

  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (id integer not null primary key,"
                                      "name VARCHAR(20) NOT NULL)");
  rc= mysql_query(mysql, "insert into t1 (id, name) values "
                         "(1, 'aaa'), (2, 'bbb'), (3, 'ccc')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));

  type= (ulong) CURSOR_TYPE_READ_ONLY;
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt);
  stmt_text= "select name from t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void*) a;
  my_bind[0].buffer_length= sizeof(a);
  mysql_stmt_bind_result(stmt, my_bind);

  for (i= 0; i < 3; i++)
  {
    int row_no= 0;
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    while ((rc= mysql_stmt_fetch(stmt)) == 0)
    {
      FAIL_UNLESS(strcmp(a, name_array[row_no]) == 0, "a != name_array[row_no]");
      ++row_no;
    }
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
  }
  rc= mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#10736: cursors and subqueries, memroot management */

static int test_bug10736(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char a[21];
  int rc;
  const char *stmt_text;
  int i= 0;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (id integer not null primary key,"
                                      "name VARCHAR(20) NOT NULL)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 (id, name) values "
                         "(1, 'aaa'), (2, 'bbb'), (3, 'ccc')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);

  type= (ulong) CURSOR_TYPE_READ_ONLY;
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt);
  stmt_text= "select name from t1 where name=(select name from t1 where id=2)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void*) a;
  my_bind[0].buffer_length= sizeof(a);
  mysql_stmt_bind_result(stmt, my_bind);

  for (i= 0; i < 3; i++)
  {
    int row_no= 0;
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    while ((rc= mysql_stmt_fetch(stmt)) == 0)
      ++row_no;
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
  }
  rc= mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#10794: cursors, packets out of order */

static int test_bug10794(MYSQL *mysql)
{
  MYSQL_STMT *stmt, *stmt1;
  MYSQL_BIND my_bind[2];
  char a[21];
  int id_val;
  ulong a_len;
  int rc;
  const char *stmt_text;
  int i= 0;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (id integer not null primary key,"
                                      "name varchar(20) not null)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  stmt_text= "insert into t1 (id, name) values (?, ?)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void*) &id_val;
  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer= (void*) a;
  my_bind[1].length= &a_len;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  for (i= 0; i < 42; i++)
  {
    id_val= (i+1)*10;
    sprintf(a, "a%d", i);
    a_len= (unsigned long)strlen(a); /* safety against broken sprintf */
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }
  stmt_text= "select name from t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  type= (ulong) CURSOR_TYPE_READ_ONLY;
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  stmt1= mysql_stmt_init(mysql);
  mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (void*) a;
  my_bind[0].buffer_length= sizeof(a);
  my_bind[0].length= &a_len;
  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  /* Don't optimize: an attribute of the original test case */
  mysql_stmt_free_result(stmt);
  mysql_stmt_reset(stmt);
  stmt_text= "select name from t1 where id=10";
  rc= mysql_stmt_prepare(stmt1, SL(stmt_text));
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_bind_result(stmt1, my_bind);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);
  while (1)
  {
    rc= mysql_stmt_fetch(stmt1);
    if (rc == MYSQL_NO_DATA)
    {
      break;
    }
    check_stmt_rc(rc, stmt1);
  }
  mysql_stmt_close(stmt);
  mysql_stmt_close(stmt1);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#10760: cursors, crash in a fetch after rollback. */

static int test_bug10760(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  int rc;
  const char *stmt_text;
  char id_buf[20];
  ulong id_len;
  int i= 0;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1, t2");
  check_mysql_rc(rc, mysql);

  /* create tables */
  rc= mysql_query(mysql, "create table t1 (id integer not null primary key)"
                         " engine=MyISAM");
  check_mysql_rc(rc, mysql);;
  for (; i < 42; ++i)
  {
    char buf[100];
    sprintf(buf, "insert into t1 (id) values (%d)", i+1);
    rc= mysql_query(mysql, buf);
    check_mysql_rc(rc, mysql);;
  }
  mysql_autocommit(mysql, FALSE);
  /* create statement */
  stmt= mysql_stmt_init(mysql);
  type= (ulong) CURSOR_TYPE_READ_ONLY;
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);

  /*
    1: check that a deadlock within the same connection
    is resolved and an error is returned. The deadlock is modelled
    as follows:
    con1: open cursor for select * from t1;
    con1: insert into t1 (id) values (1)
  */
  stmt_text= "select id from t1 order by 1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);;
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);;
  rc= mysql_query(mysql, "update t1 set id=id+100");
  /*
    If cursors are not materialized, the update will return an error;
    we mainly test that it won't deadlock.
  */
  /*  FAIL_IF(!rc, "Error expected"); */
  /*
    2: check that MyISAM tables used in cursors survive
    COMMIT/ROLLBACK.
  */
  rc= mysql_rollback(mysql);                  /* should not close the cursor */
  check_mysql_rc(rc, mysql);;
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);;

  /*
    3: check that cursors to InnoDB tables are closed (for now) by
    COMMIT/ROLLBACK.
  */
  if (check_variable(mysql, "@@have_innodb", "YES"))
  {
    stmt_text= "select id from t1 order by 1";
    rc= mysql_stmt_prepare(stmt, SL(stmt_text));
    check_stmt_rc(rc, stmt);;

    rc= mysql_query(mysql, "alter table t1 engine=InnoDB");
    check_mysql_rc(rc, mysql);;

    memset(my_bind, '\0', sizeof(my_bind));
    my_bind[0].buffer_type= MYSQL_TYPE_STRING;
    my_bind[0].buffer= (void*) id_buf;
    my_bind[0].buffer_length= sizeof(id_buf);
    my_bind[0].length= &id_len;
    check_stmt_rc(rc, stmt);;
    mysql_stmt_bind_result(stmt, my_bind);

    rc= mysql_stmt_execute(stmt);
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc == 0, "rc != 0");
    rc= mysql_rollback(mysql);                  /* should close the cursor */
  }

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_autocommit(mysql, TRUE);                /* restore default */
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#11172: cursors, crash on a fetch from a datetime column */

static int test_bug11172(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND bind_in[1], bind_out[2];
  MYSQL_TIME hired;
  int rc;
  const char *stmt_text;
  int i= 0, id;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (id integer not null primary key,"
                                      "hired date not null)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "insert into t1 (id, hired) values (1, '1933-08-24'), "
                  "(2, '1965-01-01'), (3, '1949-08-17'), (4, '1945-07-07'), "
                  "(5, '1941-05-15'), (6, '1978-09-15'), (7, '1936-03-28')");
  check_mysql_rc(rc, mysql);
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  stmt_text= "SELECT id, hired FROM t1 WHERE hired=?";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  type= (ulong) CURSOR_TYPE_READ_ONLY;
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);

  memset(bind_in, '\0', sizeof(bind_in));
  memset(bind_out, '\0', sizeof(bind_out));
  memset(&hired, '\0', sizeof(hired));
  hired.year= 1965;
  hired.month= 1;
  hired.day= 1;
  bind_in[0].buffer_type= MYSQL_TYPE_DATE;
  bind_in[0].buffer= (void*) &hired;
  bind_in[0].buffer_length= sizeof(hired);
  bind_out[0].buffer_type= MYSQL_TYPE_LONG;
  bind_out[0].buffer= (void*) &id;
  bind_out[1]= bind_in[0];

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_bind_param(stmt, bind_in);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_bind_result(stmt, bind_out);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    while ((rc= mysql_stmt_fetch(stmt)) == 0);
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
    if (!mysql_stmt_free_result(stmt))
      mysql_stmt_reset(stmt);
  }
  mysql_stmt_close(stmt);
  mysql_rollback(mysql);
  mysql_rollback(mysql);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#11656: cursors, crash on a fetch from a query with distinct. */

static int test_bug11656(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  int rc;
  const char *stmt_text;
  char buf[2][20];
  int i= 0;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1 ("
                  "server varchar(40) not null, "
                  "test_kind varchar(1) not null, "
                  "test_id varchar(30) not null , "
                  "primary key (server,test_kind,test_id))");
  check_mysql_rc(rc, mysql);

  stmt_text= "select distinct test_kind, test_id from t1 "
             "where server in (?, ?)";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  type= (ulong) CURSOR_TYPE_READ_ONLY;
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);

  memset(my_bind, '\0', sizeof(my_bind));
  strcpy(buf[0], "pcint502_MY2");
  strcpy(buf[1], "*");
  for (i=0; i < 2; i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_STRING;
    my_bind[i].buffer= (uchar* *)&buf[i];
    my_bind[i].buffer_length= (unsigned long)strlen(buf[i]);
  }
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Cursors: opening a cursor to a compilicated query with ORDER BY */

static int test_bug11901(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[2];
  int rc;
  char workdept[20];
  ulong workdept_len; 
  uint32 empno; 
  const ulong type= (ulong)CURSOR_TYPE_READ_ONLY;
  const char *stmt_text;


  stmt_text= "drop table if exists t1, t2";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "create table t1 ("
    "  empno int(11) not null, firstname varchar(20) not null,"
    "  midinit varchar(20) not null, lastname varchar(20) not null,"
    "  workdept varchar(6) not null, salary double not null,"
    "  bonus float not null, primary key (empno), "
    " unique key (workdept, empno) "
    ") default charset=latin1 collate=latin1_bin";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "insert into t1 values "
     "(10,  'CHRISTINE', 'I', 'HAAS',      'A00', 52750, 1000),"
     "(20,  'MICHAEL',   'L', 'THOMPSON',  'B01', 41250, 800), "
     "(30,  'SALLY',     'A', 'KWAN',      'C01', 38250, 800), "
     "(50,  'JOHN',      'B', 'GEYER',     'E01', 40175, 800), "
     "(60,  'IRVING',    'F', 'STERN',     'D11', 32250, 500), "
     "(70,  'EVA',       'D', 'PULASKI',   'D21', 36170, 700), "
     "(90,  'EILEEN',    'W', 'HENDERSON', 'E11', 29750, 600), "
     "(100, 'THEODORE',  'Q', 'SPENSER',   'E21', 26150, 500), "
     "(110, 'VINCENZO',  'G', 'LUCCHESSI', 'A00', 46500, 900), "
     "(120, 'SEAN',      '',  'O\\'CONNELL', 'A00', 29250, 600), "
     "(130, 'DOLORES',   'M', 'QUINTANA',  'C01', 23800, 500), "
     "(140, 'HEATHER',   'A', 'NICHOLLS',  'C01', 28420, 600), "
     "(150, 'BRUCE',     '',  'ADAMSON',   'D11', 25280, 500), "
     "(160, 'ELIZABETH', 'R', 'PIANKA',    'D11', 22250, 400), "
     "(170, 'MASATOSHI', 'J', 'YOSHIMURA', 'D11', 24680, 500), "
     "(180, 'MARILYN',   'S', 'SCOUTTEN',  'D11', 21340, 500), "
     "(190, 'JAMES',     'H', 'WALKER',    'D11', 20450, 400), "
     "(200, 'DAVID',     '',  'BROWN',     'D11', 27740, 600), "
     "(210, 'WILLIAM',   'T', 'JONES',     'D11', 18270, 400), "
     "(220, 'JENNIFER',  'K', 'LUTZ',      'D11', 29840, 600), "
     "(230, 'JAMES',     'J', 'JEFFERSON', 'D21', 22180, 400), "
     "(240, 'SALVATORE', 'M', 'MARINO',    'D21', 28760, 600), "
     "(250, 'DANIEL',    'S', 'SMITH',     'D21', 19180, 400), "
     "(260, 'SYBIL',     'P', 'JOHNSON',   'D21', 17250, 300), "
     "(270, 'MARIA',     'L', 'PEREZ',     'D21', 27380, 500), "
     "(280, 'ETHEL',     'R', 'SCHNEIDER', 'E11', 26250, 500), "
     "(290, 'JOHN',      'R', 'PARKER',    'E11', 15340, 300), "
     "(300, 'PHILIP',    'X', 'SMITH',     'E11', 17750, 400), "
     "(310, 'MAUDE',     'F', 'SETRIGHT',  'E11', 15900, 300), "
     "(320, 'RAMLAL',    'V', 'MEHTA',     'E21', 19950, 400), "
     "(330, 'WING',      '',  'LEE',       'E21', 25370, 500), "
     "(340, 'JASON',     'R', 'GOUNOT',    'E21', 23840, 500)";

  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "create table t2 ("
    " deptno varchar(6) not null, deptname varchar(20) not null,"
    " mgrno int(11) not null, location varchar(20) not null,"
    " admrdept varchar(6) not null, refcntd int(11) not null,"
    " refcntu int(11) not null, primary key (deptno)"
    ") default charset=latin1 collate=latin1_bin";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "insert into t2 values "
    "('A00', 'SPIFFY COMPUTER SERV', 10, '', 'A00', 0, 0), "
    "('B01', 'PLANNING',             20, '', 'A00', 0, 0), "
    "('C01', 'INFORMATION CENTER',   30, '', 'A00', 0, 0), "
    "('D01', 'DEVELOPMENT CENTER',   0,  '', 'A00', 0, 0),"
    "('D11', 'MANUFACTURING SYSTEM', 60, '', 'D01', 0, 0), "
    "('D21', 'ADMINISTRATION SYSTE', 70, '', 'D01', 0, 0), "
    "('E01', 'SUPPORT SERVICES',     50, '', 'A00', 0, 0), "
    "('E11', 'OPERATIONS',           90, '', 'E01', 0, 0), "
    "('E21', 'SOFTWARE SUPPORT',     100,'', 'E01', 0, 0)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text=        "select t1.empno, t1.workdept "
                    "from (t1 left join t2 on t2.deptno = t1.workdept) "
                    "where t2.deptno in "
                    "   (select t2.deptno "
                    "    from (t1 left join t2 on t2.deptno = t1.workdept) "
                    "    where t1.empno = ?) "
                    "order by 1";
  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt);


  memset(my_bind, '\0', sizeof(my_bind));

  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= &empno;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  my_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[1].buffer= (void*) workdept;
  my_bind[1].buffer_length= sizeof(workdept);
  my_bind[1].length= &workdept_len;

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  empno= 10;

  /* ERROR: next statement causes a server crash */
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#11904: mysql_stmt_attr_set CURSOR_TYPE_READ_ONLY grouping wrong result */

static int test_bug11904(MYSQL *mysql)
{
  MYSQL_STMT *stmt1;
  int rc;
  const char *stmt_text;
  const ulong type= (ulong)CURSOR_TYPE_READ_ONLY;
  MYSQL_BIND my_bind[2];
  int country_id=0;
  char row_data[11]= {0};

  /* create tables */
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS bug11904b");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE bug11904b (id int, name char(10), primary key(id, name))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO bug11904b VALUES (1, 'sofia'), (1,'plovdiv'),"
                          " (1,'varna'), (2,'LA'), (2,'new york'), (3,'heidelberg'),"
                          " (3,'berlin'), (3, 'frankfurt')");

  check_mysql_rc(rc, mysql);
  mysql_commit(mysql);
  /* create statement */
  stmt1= mysql_stmt_init(mysql);
  mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, (const void*) &type);

  stmt_text= "SELECT id, MIN(name) FROM bug11904b GROUP BY id ORDER BY id";

  rc= mysql_stmt_prepare(stmt1, SL(stmt_text));
  check_stmt_rc(rc, stmt1);

  memset(my_bind, 0, sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer=& country_id;
  my_bind[0].buffer_length= 0;
  my_bind[0].length= 0;

  my_bind[1].buffer_type= MYSQL_TYPE_STRING;
  my_bind[1].buffer=& row_data;
  my_bind[1].buffer_length= sizeof(row_data) - 1;
  my_bind[1].length= 0;

  rc= mysql_stmt_bind_result(stmt1, my_bind);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);
  FAIL_UNLESS(country_id == 1, "country_id != 1");
  FAIL_UNLESS(memcmp(row_data, "plovdiv", 7) == 0, "row_data != 'plovdiv'");

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);
  FAIL_UNLESS(country_id == 2, "country_id != 2");
  FAIL_UNLESS(memcmp(row_data, "LA", 2) == 0, "row_data != 'LA'");

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);
  FAIL_UNLESS(country_id == 3, "country_id != 3");
  FAIL_UNLESS(memcmp(row_data, "berlin", 6) == 0, "row_data != 'Berlin'");

  rc= mysql_stmt_close(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_query(mysql, "drop table bug11904b");
  check_mysql_rc(rc, mysql);

  return OK;
}


/* Bug#12243: multiple cursors, crash in a fetch after commit. */

static int test_bug12243(MYSQL *mysql)
{
  MYSQL_STMT *stmt1, *stmt2;
  int rc;
  const char *stmt_text;
  ulong type;

  if (!check_variable(mysql, "@@have_innodb", "YES"))
  {
    diag("Skip -> Test required InnoDB");
    return SKIP;
  }

  /* create tables */
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (a int) engine=InnoDB");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 (a) values (1), (2)");
  check_mysql_rc(rc, mysql);
  mysql_autocommit(mysql, FALSE);
  /* create statement */
  stmt1= mysql_stmt_init(mysql);
  stmt2= mysql_stmt_init(mysql);
  type= (ulong) CURSOR_TYPE_READ_ONLY;
  rc= mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_attr_set(stmt2, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  check_stmt_rc(rc, stmt1);

  stmt_text= "select a from t1";

  rc= mysql_stmt_prepare(stmt1, SL(stmt_text));
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);
  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_prepare(stmt2, SL(stmt_text));
  check_stmt_rc(rc, stmt2);
  rc= mysql_stmt_execute(stmt2);
  check_stmt_rc(rc, stmt2);
  rc= mysql_stmt_fetch(stmt2);
  check_stmt_rc(rc, stmt2);

  rc= mysql_stmt_close(stmt1);
  check_stmt_rc(rc, stmt1);
  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);
  rc= mysql_stmt_fetch(stmt2);
  check_stmt_rc(rc, stmt2);

  mysql_stmt_close(stmt2);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  mysql_autocommit(mysql, TRUE);                /* restore default */

  return OK;
}

/* Bug#11909: wrong metadata if fetching from two cursors */

static int test_bug11909(MYSQL *mysql)
{
  MYSQL_STMT *stmt1, *stmt2;
  MYSQL_BIND my_bind[7];
  int rc;
  char firstname[20], midinit[20], lastname[20], workdept[20];
  ulong firstname_len, midinit_len, lastname_len, workdept_len;
  uint32 empno;
  double salary;
  float bonus;
  const char *stmt_text;
  const ulong type= (ulong)CURSOR_TYPE_READ_ONLY;


  stmt_text= "drop table if exists t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "create table t1 ("
    "  empno int(11) not null, firstname varchar(20) not null,"
    "  midinit varchar(20) not null, lastname varchar(20) not null,"
    "  workdept varchar(6) not null, salary double not null,"
    "  bonus float not null, primary key (empno)"
    ") default charset=latin1 collate=latin1_bin";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "insert into t1 values "
    "(10, 'CHRISTINE', 'I', 'HAAS',     'A00', 52750, 1000), "
    "(20, 'MICHAEL',   'L', 'THOMPSON', 'B01', 41250, 800),"
    "(30, 'SALLY',     'A', 'KWAN',     'C01', 38250, 800),"
    "(50, 'JOHN',      'B', 'GEYER',    'E01', 40175, 800), "
    "(60, 'IRVING',    'F', 'STERN',    'D11', 32250, 500)";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  /* ****** Begin of trace ****** */

  stmt_text= "SELECT empno, firstname, midinit, lastname,"
             "workdept, salary, bonus FROM t1 ORDER BY empno";
  stmt1= mysql_stmt_init(mysql);
  FAIL_IF(!stmt1, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt1, SL(stmt_text));
  check_stmt_rc(rc, stmt1);
  mysql_stmt_attr_set(stmt1, STMT_ATTR_CURSOR_TYPE,
                      (const void*) &type);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void*) &empno;

  my_bind[1].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[1].buffer= (void*) firstname;
  my_bind[1].buffer_length= sizeof(firstname);
  my_bind[1].length= &firstname_len;

  my_bind[2].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[2].buffer= (void*) midinit;
  my_bind[2].buffer_length= sizeof(midinit);
  my_bind[2].length= &midinit_len;

  my_bind[3].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[3].buffer= (void*) lastname;
  my_bind[3].buffer_length= sizeof(lastname);
  my_bind[3].length= &lastname_len;

  my_bind[4].buffer_type= MYSQL_TYPE_VAR_STRING;
  my_bind[4].buffer= (void*) workdept;
  my_bind[4].buffer_length= sizeof(workdept);
  my_bind[4].length= &workdept_len;

  my_bind[5].buffer_type= MYSQL_TYPE_DOUBLE;
  my_bind[5].buffer= (void*) &salary;

  my_bind[6].buffer_type= MYSQL_TYPE_FLOAT;
  my_bind[6].buffer= (void*) &bonus;
  rc= mysql_stmt_bind_result(stmt1, my_bind);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  FAIL_UNLESS(rc == 0, "rc != 0");
  FAIL_UNLESS(empno == 10, "empno != 10");
  FAIL_UNLESS(strcmp(firstname, "CHRISTINE""") == 0, "firstname != 'Christine'");
  FAIL_UNLESS(strcmp(midinit, "I""") == 0, "");
  FAIL_UNLESS(strcmp(lastname, "HAAS""") == 0, "lastname != 'HAAS'");
  FAIL_UNLESS(strcmp(workdept, "A00""") == 0, "workdept != 'A00'");
  FAIL_UNLESS(salary == (double) 52750.0, "salary != 52750");
  FAIL_UNLESS(bonus == (float) 1000.0, "bonus =! 1000");

  stmt_text = "SELECT empno, firstname FROM t1";
  stmt2= mysql_stmt_init(mysql);
  FAIL_IF(!stmt2, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt2, SL(stmt_text));
  check_stmt_rc(rc, stmt2);
  mysql_stmt_attr_set(stmt2, STMT_ATTR_CURSOR_TYPE,
                      (const void*) &type);
  rc= mysql_stmt_bind_result(stmt2, my_bind);
  check_stmt_rc(rc, stmt2);

  rc= mysql_stmt_execute(stmt2);
  check_stmt_rc(rc, stmt2);

  rc= mysql_stmt_fetch(stmt2);
  FAIL_UNLESS(rc == 0, "rc != 0")

  FAIL_UNLESS(empno == 10, "empno != 10");
  FAIL_UNLESS(strcmp(firstname, "CHRISTINE""") == 0, "firstname != 'Christine'");

  rc= mysql_stmt_reset(stmt2);
  check_stmt_rc(rc, stmt2);

  /* ERROR: next statement should return 0 */

  rc= mysql_stmt_fetch(stmt1);
  FAIL_UNLESS(rc == 0, "rc != 0");

  mysql_stmt_close(stmt1);
  mysql_stmt_close(stmt2);
  rc= mysql_rollback(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/* Bug#13488: wrong column metadata when fetching from cursor */

static int test_bug13488(MYSQL *mysql)
{
  MYSQL_BIND my_bind[3];
  MYSQL_STMT *stmt1;
  int rc, f1, f2, f3, i;
  const ulong type= CURSOR_TYPE_READ_ONLY;
  const char *query= "select f1, f2, f3 from t1 left join t2 on f1=f2 where f1=1";


  rc= mysql_query(mysql, "drop table if exists t1, t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (f1 int not null primary key)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t2 (f2 int not null primary key, "
                  "f3 int not null)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1), (2)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (1,2), (2,4)");
  check_mysql_rc(rc, mysql);

  memset(my_bind, 0, sizeof(my_bind));
  for (i= 0; i < 3; i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_LONG;
    my_bind[i].buffer_length= 4;
    my_bind[i].length= 0;
  }
  my_bind[0].buffer=&f1;
  my_bind[1].buffer=&f2;
  my_bind[2].buffer=&f3;

  stmt1= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt1,STMT_ATTR_CURSOR_TYPE, (const void *)&type);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_prepare(stmt1, SL(query));
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_execute(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_bind_result(stmt1, my_bind);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_fetch(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_free_result(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_reset(stmt1);
  check_stmt_rc(rc, stmt1);

  rc= mysql_stmt_close(stmt1);
  check_stmt_rc(rc, stmt1);

  FAIL_UNLESS(f1 == 1, "f1 != 1");
  FAIL_UNLESS(f2 == 1, "f2 != 1");
  FAIL_UNLESS(f3 == 2, "f3 != 2");
  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug#13524: warnings of a previous command are not reset when fetching
  from a cursor.
*/

static int test_bug13524(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  unsigned int warning_count;
  const ulong type= CURSOR_TYPE_READ_ONLY;
  const char *query= "select * from t1";


  rc= mysql_query(mysql, "drop table if exists t1, t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (a int not null primary key)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1), (2), (3), (4)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  warning_count= mysql_warning_count(mysql);
  FAIL_UNLESS(warning_count == 0, "warning_count != 0");

  /* Check that DROP TABLE produced a warning (no such table) */
  rc= mysql_query(mysql, "drop table if exists t2");
  check_mysql_rc(rc, mysql);
  warning_count= mysql_warning_count(mysql);
  FAIL_UNLESS(warning_count == 1, "warning_count != 1");

  /*
    Check that fetch from a cursor cleared the warning from the previous
    command.
  */
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  warning_count= mysql_warning_count(mysql);
  FAIL_UNLESS(warning_count == 0, "warning_count != 0");

  /* Cleanup */
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug#14845 "mysql_stmt_fetch returns MYSQL_NO_DATA when COUNT(*) is 0"
*/

static int test_bug14845(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const ulong type= CURSOR_TYPE_READ_ONLY;
  const char *query= "select count(*) from t1 where 1 = 0";


  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (id int(11) default null, "
                         "name varchar(20) default null)"
                         "engine=MyISAM DEFAULT CHARSET=utf8");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1,'abc'),(2,'def')");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == 0, "");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "");

  /* Cleanup */
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Bug#14210 "Simple query with > operator on large table gives server
  crash"
*/

static int test_bug14210(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *stmt_text;
  ulong type;

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  /*
    To trigger the problem the table must be InnoDB, although the problem
    itself is not InnoDB related. In case the table is MyISAM this test
    is harmless.
  */
  rc= mysql_query(mysql, "create table t1 (a varchar(255)) engine=InnoDB");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 (a) values (repeat('a', 256))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "set @@session.max_heap_table_size=16384");

  /* Create a big enough table (more than max_heap_table_size) */
  for (i= 0; i < 8; i++)
  {
    rc= mysql_query(mysql, "insert into t1 (a) select a from t1");
    check_mysql_rc(rc, mysql);
  }
  /* create statement */
  stmt= mysql_stmt_init(mysql);
  type= (ulong) CURSOR_TYPE_READ_ONLY;
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void*) &type);

  stmt_text= "select a from t1";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  while ((rc= mysql_stmt_fetch(stmt)) == 0);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  rc= mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "set @@session.max_heap_table_size=default");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
  Bug#24179 "select b into $var" fails with --cursor_protocol"
  The failure is correct, check that the returned message is meaningful.
*/

static int test_bug24179(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;

  stmt= open_cursor(mysql, "select 1 into @a");
  rc= mysql_stmt_execute(stmt);
  FAIL_UNLESS(rc, "Error expected");
  FAIL_UNLESS(mysql_stmt_errno(stmt) == 1323, "stmt_errno != 1323");
  mysql_stmt_close(stmt);

  return OK;
}

/**
  Bug#32265 Server returns different metadata if prepared statement is used
*/

static int test_bug32265(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  MYSQL_FIELD *field;
  MYSQL_RES *metadata;

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS v1");
  rc= mysql_query(mysql, "CREATE  TABLE t1 (a INTEGER)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE VIEW v1 AS SELECT * FROM t1");
  check_mysql_rc(rc, mysql);

  stmt= open_cursor(mysql, "SELECT * FROM t1");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_field(metadata);
  FAIL_UNLESS(field, "couldn't fetch field");
  FAIL_UNLESS(strcmp(field->table, "t1") == 0, "table != t1");
  FAIL_UNLESS(strcmp(field->org_table, "t1") == 0, "org_table != t1");
  FAIL_UNLESS(strcmp(field->db, schema) == 0, "db != schema");
  mysql_free_result(metadata);
  mysql_stmt_close(stmt);

  stmt= open_cursor(mysql, "SELECT a '' FROM t1 ``");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_field(metadata);
  FAIL_UNLESS(strcmp(field->table, "") == 0, "field != ''");
  FAIL_UNLESS(strcmp(field->org_table, "t1") == 0, "org_table != t1");
  FAIL_UNLESS(strcmp(field->db, schema) == 0, "db != schema");
  mysql_free_result(metadata);
  mysql_stmt_close(stmt);

  stmt= open_cursor(mysql, "SELECT a '' FROM t1 ``");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_field(metadata);
  FAIL_UNLESS(strcmp(field->table, "") == 0, "table != ''");
  FAIL_UNLESS(strcmp(field->org_table, "t1") == 0, "org_table != t1");
  FAIL_UNLESS(strcmp(field->db, schema) == 0, "db != schema");
  mysql_free_result(metadata);
  mysql_stmt_close(stmt);

  stmt= open_cursor(mysql, "SELECT * FROM v1");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_field(metadata);
  FAIL_UNLESS(strcmp(field->table, "v1") == 0, "table != v1");
  FAIL_UNLESS(strcmp(field->org_table, "v1") == 0, "org_table != v1");
  FAIL_UNLESS(strcmp(field->db, schema) == 0, "db != schema");
  mysql_free_result(metadata);
  mysql_stmt_close(stmt);

  stmt= open_cursor(mysql, "SELECT * FROM v1 /* SIC */ GROUP BY 1");
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  metadata= mysql_stmt_result_metadata(stmt);
  field= mysql_fetch_field(metadata);
  FAIL_UNLESS(strcmp(field->table, "v1") == 0, "table != v1");
  FAIL_UNLESS(strcmp(field->org_table, "v1") == 0, "org_table != v1");
  FAIL_UNLESS(strcmp(field->db, schema) == 0, "schema != db");
  mysql_free_result(metadata);
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

/**
  Bug#38486 Crash when using cursor protocol
*/

static int test_bug38486(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  int rc;
  unsigned long type= CURSOR_TYPE_READ_ONLY;

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*)&type);
  check_stmt_rc(rc, stmt);
  stmt_text= "CREATE TABLE t1 (a INT)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  mysql_stmt_close(stmt);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*)&type);
  check_stmt_rc(rc, stmt);
  stmt_text= "INSERT INTO t1 VALUES (1)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  return OK;
}

static int test_bug8880(MYSQL *mysql)
{
  MYSQL_STMT *stmt_list[2], **stmt;
  MYSQL_STMT **stmt_list_end= (MYSQL_STMT**) stmt_list + 2;
  int rc;


  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (a int not null primary key, b int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1,1)");
  check_mysql_rc(rc, mysql);
  /*
    when inserting 2 rows everything works well
    mysql_query(mysql, "INSERT INTO t1 VALUES (1,1),(2,2)");
  */
  for (stmt= stmt_list; stmt < stmt_list_end; stmt++)
    *stmt= open_cursor(mysql, "select a from t1");
  for (stmt= stmt_list; stmt < stmt_list_end; stmt++)
  {
    rc= mysql_stmt_execute(*stmt);
    check_stmt_rc(rc, *stmt);
  }
  for (stmt= stmt_list; stmt < stmt_list_end; stmt++)
    mysql_stmt_close(*stmt);
  return OK;
}

static int test_bug9159(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmt_text= "select a, b from t1";
  const unsigned long type= CURSOR_TYPE_READ_ONLY;


  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (a int not null primary key, b int)");
  rc= mysql_query(mysql, "insert into t1 values (1,1)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  mysql_stmt_prepare(stmt, SL(stmt_text));
  mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (const void *)&type);

  mysql_stmt_execute(stmt);
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  We can't have more than one cursor open for a prepared statement.
  Test re-executions of a PS with cursor; mysql_stmt_reset must close
  the cursor attached to the statement, if there is one.
*/

static int test_bug9478(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char a[6];
  ulong a_len;
  int rc, i;

  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (id integer not null primary key, "
                     " name varchar(20) not null)");
  rc= mysql_query(mysql, "insert into t1 (id, name) values "
                         " (1, 'aaa'), (2, 'bbb'), (3, 'ccc')");
  check_mysql_rc(rc, mysql);

  stmt= open_cursor(mysql, "select name from t1 where id=2");

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (char*) a;
  my_bind[0].buffer_length= sizeof(a);
  my_bind[0].length= &a_len;
  mysql_stmt_bind_result(stmt, my_bind);

  for (i= 0; i < 5; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    /*
      The query above is a one-row result set. Therefore, there is no
      cursor associated with it, as the server won't bother with opening
      a cursor for a one-row result set. The first row was read from the
      server in the fetch above. But there is eof packet pending in the
      network. mysql_stmt_execute will flush the packet and successfully
      execute the statement.
    */

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

    {
      char buff[8];
      /* Fill in the fetch packet */
      int4store(buff, stmt->stmt_id);
      buff[4]= 1;                               /* prefetch rows */
/*      rc= ((*mysql->methods->advanced_command)(mysql, COM_STMT_FETCH,
                                               (uchar*) buff,
                                               sizeof(buff), 0,0,1,NULL) ||
           (*mysql->methods->read_query_result)(mysql)); */
      FAIL_UNLESS(rc, "error expected");
    }

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_reset(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);

    /* mariadb client supports GEOMETRY, so no error will
       be returned 
    FAIL_UNLESS(rc && mysql_stmt_errno(stmt), "Error expected");
    */
  }
  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  /* Test the case with a server side cursor */
  stmt= open_cursor(mysql, "select name from t1");

  mysql_stmt_bind_result(stmt, my_bind);

  for (i= 0; i < 5; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    while (! (rc= mysql_stmt_fetch(stmt)));
    FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_fetch(stmt);
    check_stmt_rc(rc, stmt);

    rc= mysql_stmt_reset(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(rc && mysql_stmt_errno(stmt), "Error expected");
  }

  rc= mysql_stmt_close(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Crash when opening a cursor to a query with DISTICNT and no key */

static int test_bug9520(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  char a[6];
  ulong a_len;
  int rc, row_count= 0;


  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (a char(5), b char(5), c char(5),"
                     " primary key (a, b, c))");
  rc= mysql_query(mysql, "insert into t1 values ('x', 'y', 'z'), "
                  " ('a', 'b', 'c'), ('k', 'l', 'm')");
  check_mysql_rc(rc, mysql);

  stmt= open_cursor(mysql, "select distinct b from t1");

  /*
    Not crashes with:
    stmt= open_cursor(mysql, "select distinct a from t1");
  */

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (char*) a;
  my_bind[0].buffer_length= sizeof(a);
  my_bind[0].length= &a_len;

  mysql_stmt_bind_result(stmt, my_bind);

  while (!(rc= mysql_stmt_fetch(stmt)))
    row_count++;

  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  FAIL_UNLESS(row_count == 3, "row_count != 3");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Error message is returned for unsupported features.
  Test also cursors with non-default PREFETCH_ROWS
*/

static int test_bug9643(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND my_bind[1];
  int32 a;
  int rc;
  const char *stmt_text;
  int num_rows= 0;
  ulong type;
  ulong prefetch_rows= 5;


  mysql_query(mysql, "drop table if exists t1");
  mysql_query(mysql, "create table t1 (id integer not null primary key)");
  rc= mysql_query(mysql, "insert into t1 (id) values "
                         " (1), (2), (3), (4), (5), (6), (7), (8), (9)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  /* Not implemented in 5.0 */
  type= (ulong) CURSOR_TYPE_SCROLLABLE;
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  FAIL_UNLESS(rc, "Error expected");

  type= (ulong) CURSOR_TYPE_READ_ONLY;
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS,
                          (void*) &prefetch_rows);
  check_stmt_rc(rc, stmt);
  stmt_text= "select * from t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer_type= MYSQL_TYPE_LONG;
  my_bind[0].buffer= (void*) &a;
  my_bind[0].buffer_length= sizeof(a);
  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  while ((rc= mysql_stmt_fetch(stmt)) == 0)
    ++num_rows;
  FAIL_UNLESS(num_rows == 9, "num_rows != 9");

  rc= mysql_stmt_close(stmt);
  FAIL_UNLESS(rc == 0, "");

  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);
  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_basic_cursors", test_basic_cursors, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_cursors_with_union", test_cursors_with_union, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_cursors_with_procedure", test_cursors_with_procedure, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug21206", test_bug21206, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug10729", test_bug10729, TEST_CONNECTION_DEFAULT, 0, NULL , NULL}, 
  {"test_bug10736", test_bug10736, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug10794", test_bug10794, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug10760", test_bug10760, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11172", test_bug11172, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11656", test_bug11656, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11901", test_bug11901, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11904", test_bug11904, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug12243", test_bug12243, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11909", test_bug11909, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug13488", test_bug13488, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug13524", test_bug13524, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug14845", test_bug14845, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug14210", test_bug14210, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug24179", test_bug24179, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug32265", test_bug32265, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug38486", test_bug38486, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug8880", test_bug8880, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug9159", test_bug9159, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug9478", test_bug9478, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug9520", test_bug9520, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug9643", test_bug9643, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
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
