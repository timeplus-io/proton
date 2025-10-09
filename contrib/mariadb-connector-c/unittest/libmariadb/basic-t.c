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
#include "ma_common.h"

static int test_conc75(MYSQL *my)
{
  int rc;
  MYSQL *mysql;
  int i;
  my_bool reconnect= 1;

  mysql= mysql_init(NULL);

  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);
  my_test_connect(mysql, hostname, username, password, schema, port, socketname, 0| CLIENT_MULTI_RESULTS | CLIENT_REMEMBER_OPTIONS);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS a");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE a (a varchar(200))");
  check_mysql_rc(rc, mysql);

  rc= mysql_set_character_set(mysql, "utf8");
  check_mysql_rc(rc, mysql);

  for (i=0; i < 10; i++)
  {
    ulong thread_id= mysql_thread_id(mysql);
    /* force reconnect */
    mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);
    diag("killing connection");
    mysql_kill(my, thread_id);
    mysql_ping(mysql);
    rc= mysql_query(mysql, "load data local infile './nonexistingfile.csv' into table a (`a`)");
    FAIL_IF(!test(mysql->options.client_flag | CLIENT_LOCAL_FILES), "client_flags not correct");
    diag("thread1: %ld %ld", thread_id, mysql_thread_id(mysql));
    FAIL_IF(thread_id == mysql_thread_id(mysql), "new thread id expected");
    //diag("cs: %s", mysql->charset->csname);
    //FAIL_IF(strcmp(mysql->charset->csname, "utf8"), "wrong character set");
  }
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS a");
  check_mysql_rc(rc, mysql);
  mysql_close(mysql);
  return OK;
}


static int test_conc74(MYSQL *unused __attribute__((unused)))
{
  int rc;
  MYSQL *mysql;

  mysql= mysql_init(NULL);


  if (!my_test_connect(mysql, hostname, username, password, schema, port, socketname, 0| CLIENT_MULTI_RESULTS | CLIENT_REMEMBER_OPTIONS))
  {
    diag("Error: %s", mysql_error(mysql));
    mysql_close(mysql);
    return FAIL;
  }

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS a");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE a (a varchar(200))");
  check_mysql_rc(rc, mysql);

  mysql->options.client_flag&= ~CLIENT_LOCAL_FILES;

  rc= mysql_query(mysql, "load data local infile './nonexistingfile.csv' into table a (`a`)");
  FAIL_IF(!rc, "Error expected");

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS a");
  check_mysql_rc(rc, mysql);

  mysql_close(mysql);
  return OK;
}


static int test_conc71(MYSQL *my)
{
  int rc;
  MYSQL *mysql;
  
  /* uncomment if you want to test manually */
  return SKIP;

  mysql= mysql_init(NULL);


  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");
  mysql_options(mysql, MYSQL_OPT_COMPRESS, 0);
  mysql_options(mysql, MYSQL_INIT_COMMAND, "/*!40101 SET SQL_MODE='' */");
  mysql_options(mysql, MYSQL_INIT_COMMAND, "/*!40101 set @@session.wait_timeout=28800 */");

  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));

  diag("kill server");

  rc= mysql_query(mysql, "SELECT 'foo' FROM DUAL");
  check_mysql_rc(rc, mysql);

  mysql_close(mysql);
  return OK;
}

static int test_conc70(MYSQL *my)
{
  int rc;
  MYSQL_RES *res;
  MYSQL_ROW row;
  MYSQL *mysql;

  SKIP_CONNECTION_HANDLER;

  mysql= mysql_init(NULL);

  rc= mysql_query(my, "SET @a:=@@max_allowed_packet");
  check_mysql_rc(rc, my);

  mysql_query(my, "SET global max_allowed_packet=1024*1024*22");
  check_mysql_rc(rc, my);

  mysql_options(mysql, MYSQL_OPT_COMPRESS, (void *)1);
  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a LONGBLOB) engine=MyISAM");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (REPEAT('A', 1024 * 1024 * 20))");
  check_mysql_rc(rc, mysql);

  if (mysql_warning_count(mysql))
  {
    diag("server doesn't accept package size");
    return SKIP;
  }


  rc= mysql_query(mysql, "SELECT a FROM t1");
  check_mysql_rc(rc, mysql);

  if (!(res= mysql_store_result(mysql)))
  {
    diag("Error: %s", mysql_error(mysql));
    return FAIL;
  }

  row= mysql_fetch_row(res);
  diag("Length: %ld", (long)strlen(row[0]));
  FAIL_IF(strlen(row[0]) != 1024 * 1024 * 20, "Wrong length");

  mysql_free_result(res);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  mysql_close(mysql);

  rc= mysql_query(my, "SET global max_allowed_packet=@a");
  check_mysql_rc(rc, my);

  return OK;
}

static int test_conc68(MYSQL *my)
{
  int rc;
  MYSQL_RES *res;
  MYSQL_ROW row;
  MYSQL *mysql;

  SKIP_CONNECTION_HANDLER;
  
  mysql= mysql_init(NULL);

  rc= mysql_query(my, "SET @a:=@@max_allowed_packet");
  check_mysql_rc(rc, my);

  mysql_query(my, "SET global max_allowed_packet=1024*1024*22");

  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a LONGBLOB) ENGINE=MyISAM");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (REPEAT('A', 1024 * 1024 * 20))");
  check_mysql_rc(rc, mysql); 
  if (mysql_warning_count(mysql))
  {
    diag("server doesn't accept package size");
    return SKIP;
  }

  rc= mysql_query(mysql, "SELECT a FROM t1");
  check_mysql_rc(rc, mysql);

  if (!(res= mysql_store_result(mysql)))
  {
    diag("Error: %s", mysql_error(mysql));
    return FAIL;
  }

  row= mysql_fetch_row(res);
  diag("Length: %ld", (long)strlen(row[0]));
  FAIL_IF(strlen(row[0]) != 1024 * 1024 * 20, "Wrong length");

  mysql_free_result(res);
  mysql_close(mysql);

  rc= mysql_query(my, "SET global max_allowed_packet=@a");
  check_mysql_rc(rc, my);

  return OK;
}


static int basic_connect(MYSQL *unused __attribute__((unused)))
{
  MYSQL_ROW row;
  MYSQL_RES *res;
  MYSQL_FIELD *field;
  int rc;

  MYSQL *my= mysql_init(NULL);
  FAIL_IF(!my, "mysql_init() failed");

  FAIL_IF(!my_test_connect(my, hostname, username, password, schema,
                         port, socketname, 0), mysql_error(my));

  rc= mysql_query(my, "SELECT @@version");
  check_mysql_rc(rc, my);

  res= mysql_store_result(my);
  FAIL_IF(!res, mysql_error(my));
  field= mysql_fetch_fields(res);
  FAIL_IF(!field, "couldn't fetch field");
  while ((row= mysql_fetch_row(res)) != NULL)
  {
    FAIL_IF(mysql_num_fields(res) != 1, "Got the wrong number of fields");
  }
  FAIL_IF(mysql_errno(my), mysql_error(my));

  mysql_free_result(res);
  mysql_close(my);

  return OK;
}


static int use_utf8(MYSQL *my)
{
  MYSQL_ROW row;
  MYSQL_RES *res;
  int rc;

  /* Make sure that we actually ended up with utf8. */
  rc= mysql_query(my, "SELECT @@character_set_connection");
  check_mysql_rc(rc, my);

  res= mysql_store_result(my);
  FAIL_IF(!res, mysql_error(my));

  while ((row= mysql_fetch_row(res)) != NULL)
  {
    FAIL_IF(strcmp(row[0], "utf8"), "wrong character set");
  }
  FAIL_IF(mysql_errno(my), mysql_error(my));
  mysql_free_result(res);

  return OK;
}

int client_query(MYSQL *mysql) {
  int rc;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1("
                         "id int primary key auto_increment, "
                         "name varchar(20))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1(id int, name varchar(20))");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_query(mysql, "INSERT INTO t1(name) VALUES('mysql')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1(name) VALUES('monty')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1(name) VALUES('venu')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1(name) VALUES('deleted')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO t1(name) VALUES('deleted')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "UPDATE t1 SET name= 'updated' "
                          "WHERE name= 'deleted'");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "UPDATE t1 SET id= 3 WHERE name= 'updated'");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug12001(MYSQL *mysql)
{
  MYSQL_RES *result;
  const char *query= "DROP TABLE IF EXISTS test_table;"
                     "CREATE TABLE test_table(id INT);"
                     "INSERT INTO test_table VALUES(10);"
                     "UPDATE test_table SET id=20 WHERE id=10;"
                     "SELECT * FROM test_table;"
                     "INSERT INTO non_existent_table VALUES(11);";
  int rc, res;


  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  do
  {
    if (mysql_field_count(mysql) &&
        (result= mysql_use_result(mysql)))
    {
      mysql_free_result(result);
    }
  }
  while (!(res= mysql_next_result(mysql)));

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_table");
  check_mysql_rc(rc, mysql);

  FAIL_UNLESS(res==1, "res != 1");

  return OK;
}


/* connection options */
struct my_option_st opt_utf8[] = {
  {MYSQL_SET_CHARSET_NAME, (char *)"utf8"},
  {0, NULL}
};

static int test_bad_union(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int        rc;
  const char *query= "SELECT 1, 2 union SELECT 1";

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  rc= mysql_stmt_prepare(stmt, SL(query));
  FAIL_UNLESS(rc && mysql_errno(mysql) == 1222, "Error expected");

  mysql_stmt_close(stmt);
  return OK;
}

/*
   Test that mysql_insert_id() behaves as documented in our manual
*/
static int test_mysql_insert_id(MYSQL *mysql)
{
  unsigned long long res;
  int rc;

  if (mysql_get_server_version(mysql) < 50100) {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }

  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "drop table if exists t2");
  check_mysql_rc(rc, mysql);
  /* table without auto_increment column */
  rc= mysql_query(mysql, "create table t1 (f1 int, f2 varchar(255), key(f1))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1,'a')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "insert into t1 values (null,'b')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "insert into t1 select 5,'c'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");

  /*
    Test for bug #34889: mysql_client_test::test_mysql_insert_id test fails
    sporadically
  */
  rc= mysql_query(mysql, "create table t2 (f1 int not null primary key auto_increment, f2 varchar(255))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (null,'b')");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 select 5,'c'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "drop table t2");
  check_mysql_rc(rc, mysql);
  
  rc= mysql_query(mysql, "insert into t1 select null,'d'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "insert into t1 values (null,last_insert_id(300))");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 300, "");
  rc= mysql_query(mysql, "insert into t1 select null,last_insert_id(400)");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  /*
    Behaviour change: old code used to return 0; but 400 is consistent
    with INSERT VALUES, and the manual's section of mysql_insert_id() does not
    say INSERT SELECT should be different.
  */
  FAIL_UNLESS(res == 400, "");

  /* table with auto_increment column */
  rc= mysql_query(mysql, "create table t2 (f1 int not null primary key auto_increment, f2 varchar(255)) engine=MyISAM");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (1,'a')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 1, "");
  /* this should not influence next INSERT if it doesn't have auto_inc */
  rc= mysql_query(mysql, "insert into t1 values (10,'e')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");

  rc= mysql_query(mysql, "insert into t2 values (null,'b')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 2, "");
  rc= mysql_query(mysql, "insert into t2 select 5,'c'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  /*
    Manual says that for multirow insert this should have been 5, but does not
    say for INSERT SELECT. This is a behaviour change: old code used to return
    0. We try to be consistent with INSERT VALUES.
  */
  FAIL_UNLESS(res == 5, "");
  rc= mysql_query(mysql, "insert into t2 select null,'d'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 6, "");
  /* with more than one row */
  rc= mysql_query(mysql, "insert into t2 values (10,'a'),(11,'b')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 11, "");
  rc= mysql_query(mysql, "insert into t2 select 12,'a' union select 13,'b'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  /*
    Manual says that for multirow insert this should have been 13, but does
    not say for INSERT SELECT. This is a behaviour change: old code used to
    return 0. We try to be consistent with INSERT VALUES.
  */
  FAIL_UNLESS(res == 13, "");
  rc= mysql_query(mysql, "insert into t2 values (null,'a'),(null,'b')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 14, "");
  rc= mysql_query(mysql, "insert into t2 select null,'a' union select null,'b'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 16, "");
  rc= mysql_query(mysql, "insert into t2 select 12,'a' union select 13,'b'");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_query(mysql, "insert ignore into t2 select 12,'a' union select 13,'b'");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "insert into t2 values (12,'a'),(13,'b')");
  FAIL_IF(!rc, "Error expected");
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "insert ignore into t2 values (12,'a'),(13,'b')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  /* mixing autogenerated and explicit values */
  rc= mysql_query(mysql, "insert into t2 values (null,'e'),(12,'a'),(13,'b')");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_query(mysql, "insert into t2 values (null,'e'),(12,'a'),(13,'b'),(25,'g')");
  FAIL_IF(!rc, "Error expected");
  rc= mysql_query(mysql, "insert into t2 values (null,last_insert_id(300))");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  /*
    according to the manual, this might be 20 or 300, but it looks like
    auto_increment column takes priority over last_insert_id().
  */
  diag("res: %lld", res);
  FAIL_UNLESS(res == 20, "");
  /* If first autogenerated number fails and 2nd works: */
  rc= mysql_query(mysql, "drop table t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t2 (f1 int not null primary key "
                  "auto_increment, f2 varchar(200), unique (f2)) engine=MyISAM");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (null,'e')");
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 1, "");
  rc= mysql_query(mysql, "insert ignore into t2 values (null,'e'),(null,'a'),(null,'e')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 2, "");
  /* If autogenerated fails and explicit works: */
  rc= mysql_query(mysql, "insert ignore into t2 values (null,'e'),(12,'c'),(null,'d')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  /*
    Behaviour change: old code returned 3 (first autogenerated, even if it
    fails); we now return first successful autogenerated.
  */
  FAIL_UNLESS(res == 13, "");
  /* UPDATE may update mysql_insert_id() if it uses LAST_INSERT_ID(#) */
  rc= mysql_query(mysql, "update t2 set f1=14 where f1=12");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "update t2 set f1=0 where f1=14");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  rc= mysql_query(mysql, "update t2 set f2=last_insert_id(372) where f1=0");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 372, "");
  /* check that LAST_INSERT_ID() does not update mysql_insert_id(): */
  rc= mysql_query(mysql, "insert into t2 values (null,'g')");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 15, "");
  rc= mysql_query(mysql, "update t2 set f2=(@li:=last_insert_id()) where f1=15");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 0, "");
  /*
    Behaviour change: now if ON DUPLICATE KEY UPDATE updates a row,
    mysql_insert_id() returns the id of the row, instead of not being
    affected.
  */
  rc= mysql_query(mysql, "insert into t2 values (null,@li) on duplicate key "
                  "update f2=concat('we updated ',f2)");
  check_mysql_rc(rc, mysql);
  res= mysql_insert_id(mysql);
  FAIL_UNLESS(res == 15, "");

  rc= mysql_query(mysql, "drop table t1,t2");
  check_mysql_rc(rc, mysql);
  return OK;
}

/* Test simple select to debug */

static int test_select_direct(MYSQL *mysql)
{
  int        rc;
  MYSQL_RES  *result;


  rc= mysql_autocommit(mysql, TRUE);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE test_select(id int, id1 tinyint, "
                                                 " id2 float, "
                                                 " id3 double, "
                                                 " name varchar(50))");
  check_mysql_rc(rc, mysql);

  /* insert a row and commit the transaction */
  rc= mysql_query(mysql, "INSERT INTO test_select VALUES(10, 5, 2.3, 4.5, 'venu')");
  check_mysql_rc(rc, mysql);

  rc= mysql_commit(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM test_select");
  check_mysql_rc(rc, mysql);

  /* get the result */
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  mysql_free_result(result);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS test_select");
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Ensure we execute the status code while testing
*/

static int test_status(MYSQL *mysql)
{
  mysql_stat(mysql);
  check_mysql_rc(mysql_errno(mysql), mysql);
  return OK;
}

static int bug_conc1(MYSQL *mysql)
{
  my_test_connect(mysql, hostname, username, password, schema,
                     port, socketname, 0);
  diag("errno: %d", mysql_errno(mysql));
  FAIL_IF(mysql_errno(mysql) != CR_ALREADY_CONNECTED,
          "Expected errno=CR_ALREADY_CONNECTED");
  return OK;
}

static int test_options_initcmd(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql= mysql_init(NULL);
  MYSQL_RES *res;
  int rc;

  mysql_options(mysql, MYSQL_INIT_COMMAND, "DROP TABLE IF EXISTS t1; CREATE TABLE t1 (a int)");
  mysql_options(mysql, MYSQL_INIT_COMMAND, "INSERT INTO t1 VALUES (1),(2),(3)");
  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                              port, socketname, 
                              CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS), mysql_error(mysql));

  rc= mysql_query(mysql, "SELECT a FROM t1");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  FAIL_IF(mysql_num_rows(res) != 3, "Expected 3 rows");

  mysql_free_result(res);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  mysql_close(mysql);
  return OK;
}

static int test_extended_init_values(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql= mysql_init(NULL);

  mysql_options(mysql, MYSQL_DEFAULT_AUTH, "unknown");
  FAIL_IF(strcmp(mysql->options.extension->default_auth, "unknown"), "option not set");

  mysql_options(mysql, MYSQL_PLUGIN_DIR, "/tmp/foo");
  FAIL_IF(strcmp(mysql->options.extension->plugin_dir, "/tmp/foo"), "option not set");

  mysql_close(mysql);
  return OK;
}

static int test_reconnect_maxpackage(MYSQL *unused __attribute__((unused)))
{
  int rc;
  ulong max_packet= 0;
  MYSQL *mysql;
  MYSQL_RES *res;
  MYSQL_ROW row;
  char *query;
  my_bool reconnect= 1;

  SKIP_CONNECTION_HANDLER;

  mysql= mysql_init(NULL);

  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                              port, socketname,
                              CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS), mysql_error(mysql));
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  rc= mysql_query(mysql, "SELECT @@max_allowed_packet");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);
  max_packet= atol(row[0]);
  diag("max_allowed_packet=%lu", max_packet);
  mysql_free_result(res);

  query= (char *)malloc(max_packet + 30);
  memset(query, 0, max_packet + 30);

  strcpy(query, "SELECT '");
  memset(query + 8, 'A', max_packet);
  strcat(query, "' FROM DUAL");

  rc= mysql_query(mysql, query);
  free(query);
  if (!rc)
  {
    diag("expected error");
    mysql_close(mysql);
    return FAIL;
  }
  else
    diag("Error (expected): %s", mysql_error(mysql));

  rc= mysql_ping(mysql);
  /* if the server is under load, poll might not report closed
     socket since FIN packet came too late */
  if (rc)
    rc= mysql_ping(mysql);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "SELECT @@max_allowed_packet");
  check_mysql_rc(rc, mysql);
   res= mysql_store_result(mysql);
  row= mysql_fetch_row(res);
  max_packet= atol(row[0]);
  diag("max_allowed_packet=%lu", max_packet);
  mysql_free_result(res);


  mysql_close(mysql);
  return OK;
}

static int test_compressed(MYSQL *unused __attribute__((unused)))
{
  int rc;
  MYSQL *mysql= mysql_init(NULL);
  MYSQL_RES *res;
  my_bool reconnect= 1;

  mysql_options(mysql, MYSQL_OPT_COMPRESS, (void *)1);
  FAIL_IF(!my_test_connect(mysql, hostname, username, password, schema,
                              port, socketname, 
                              CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS), mysql_error(mysql));
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  rc= mysql_query(mysql, "SHOW VARIABLES");
  check_mysql_rc(rc, mysql);

  if ((res= mysql_store_result(mysql)))
    mysql_free_result(res);

  mysql_close(mysql);

  return OK;
}

struct my_tests_st my_tests[] = {
  {"test_conc75", test_conc75, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_conc74", test_conc74, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_conc71", test_conc71, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_conc70", test_conc70, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_conc68", test_conc68, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_compressed", test_compressed, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_reconnect_maxpackage", test_reconnect_maxpackage, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"basic_connect", basic_connect, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"use_utf8", use_utf8, TEST_CONNECTION_NEW, 0,  opt_utf8,  NULL},
  {"client_query", client_query, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bad_union", test_bad_union, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_select_direct", test_select_direct, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_mysql_insert_id", test_mysql_insert_id, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_bug12001", test_bug12001, TEST_CONNECTION_NEW, CLIENT_MULTI_STATEMENTS,  NULL,  NULL},
  {"test_status", test_status, TEST_CONNECTION_NEW, CLIENT_MULTI_STATEMENTS,  NULL,  NULL},
  {"bug_conc1", bug_conc1, TEST_CONNECTION_NEW, 0, NULL, NULL},
  {"test_options_initcmd", test_options_initcmd, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_extended_init_values", test_extended_init_values, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {NULL, NULL, 0, 0, NULL, NULL}
};


int main(int argc, char **argv)
{
  if (argc > 1)
    get_options(argc, argv);

  get_envvars();

  diag("user: %s", username);

  run_tests(my_tests);

  return(exit_status());
}
