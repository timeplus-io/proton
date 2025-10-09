/*
*/

#include "my_test.h"

static int execute_direct(MYSQL *mysql)
{
  int rc= 0;
  long i= 0;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind;
  unsigned int param_count= 1;
  MYSQL_RES *res= NULL;

  stmt= mysql_stmt_init(mysql);

  rc= mariadb_stmt_execute_direct(stmt, "DROP TABLE IF EXISTS t1", -1);
  check_stmt_rc(rc, stmt);

  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1", -1);
  check_stmt_rc(rc, stmt);

  while (!mysql_stmt_fetch(stmt));

  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1", -1);
  check_stmt_rc(rc, stmt);

  rc= mariadb_stmt_execute_direct(stmt, "CREATE TABLE t1 (a int)", -1);
  check_stmt_rc(rc, stmt);

  memset(&bind, 0, sizeof(MYSQL_BIND));

  bind.buffer= &i;
  bind.buffer_type= MYSQL_TYPE_LONG;
  bind.buffer_length= sizeof(long);

  mysql_stmt_close(stmt);
  stmt= mysql_stmt_init(mysql);
  mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);

  rc= mysql_stmt_bind_param(stmt, &bind);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "INSERT INTO t1 VALUES (?)", -1);
  check_stmt_rc(rc, stmt);

  for (i=1; i < 1000; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
  }
  rc= mysql_stmt_close(stmt);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM t1");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  FAIL_IF(mysql_num_rows(res) != 1000, "Expected 1000 rows");

  mysql_free_result(res);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int execute_direct_example(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_BIND bind[2];
  int intval= 1;
  int param_count= 2;
  int rc;
  const char *strval= "execute_direct_example1";

  /* Direct execution without parameters */
  rc= mariadb_stmt_execute_direct(stmt, "DROP TABLE IF EXISTS execute_direct", -1);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "CREATE TABLE execute_direct (a int, b varchar(20))", -1);
  rc= mysql_stmt_close(stmt);
  stmt= mysql_stmt_init(mysql);
  check_stmt_rc(rc, stmt);
  memset(bind, 0, sizeof(MYSQL_BIND) * 2);
  bind[0].buffer_type= MYSQL_TYPE_SHORT;
  bind[0].buffer= &intval;
  bind[1].buffer_type= MYSQL_TYPE_STRING;
  bind[1].buffer= (char *)strval;
  bind[1].buffer_length= (unsigned long)strlen(strval);

  /* set number of parameters */
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);
  check_stmt_rc(rc, stmt);

  /* bind parameters */
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mariadb_stmt_execute_direct(stmt, "INSERT INTO execute_direct VALUES (?,?)", -1);
  check_stmt_rc(rc, stmt);

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE execute_direct");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int conc_213(MYSQL *mysql)
{
  MYSQL_BIND bind;
  unsigned int param_count= 1;
  long id= 1234;
  MYSQL_STMT *stmt;

  stmt = mysql_stmt_init(mysql);

  memset(&bind, '\0', sizeof(bind));

  bind.buffer_type = MYSQL_TYPE_LONG;
  bind.buffer = (void *)&id;
  bind.buffer_length = sizeof(long);
/*  bind.is_null = &is_null;
  bind.length = &length;
  bind.error = &error; */

  mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);
  check_stmt_rc(mysql_stmt_bind_param(stmt, &bind), stmt);
  check_stmt_rc(mariadb_stmt_execute_direct(stmt, "SELECT ?", -1), stmt);
  check_stmt_rc(mysql_stmt_store_result(stmt), stmt);
  check_stmt_rc(mysql_stmt_free_result(stmt), stmt);

  mysql_stmt_close(stmt);

  return OK;
}

static int conc_212(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  int rc;

  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1, 2", -1);
  check_stmt_rc(rc, stmt);
  mysql_stmt_store_result(stmt);
  mysql_stmt_free_result(stmt);

  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1, 2", -1);
  check_stmt_rc(rc, stmt);
  mysql_stmt_store_result(stmt);
  mysql_stmt_free_result(stmt);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc,mysql);


  rc= mysql_stmt_close(stmt);

  return OK;
}

static int conc_218(MYSQL *mysql)
{
  MYSQL_STMT *stmt= mysql_stmt_init(mysql);
  MYSQL_BIND bind[2];
  int id=1;
  my_bool is_null= 0, error= 0;
  unsigned int param_count= 1;

  memset(bind, 0, 2 * sizeof(MYSQL_BIND));
  bind[0].buffer_type = MYSQL_TYPE_LONG;
  bind[0].buffer = (void *)&id;
  bind[0].buffer_length = 4;
  bind[0].is_null = &is_null;
  bind[0].error = &error;

  mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);
  check_stmt_rc(mysql_stmt_bind_param(stmt, bind), stmt);
  check_stmt_rc(mariadb_stmt_execute_direct(stmt, "SELECT ?", -1), stmt);
  check_stmt_rc(mysql_stmt_store_result(stmt), stmt);

  check_stmt_rc(mysql_stmt_free_result(stmt), stmt);

  param_count= 1;
  mysql_stmt_attr_set(stmt, STMT_ATTR_PREBIND_PARAMS, &param_count);
  check_stmt_rc(mysql_stmt_bind_param(stmt, bind), stmt);
  check_stmt_rc(mariadb_stmt_execute_direct(stmt, "SELECT ?", -1), stmt);
  mysql_stmt_close(stmt);

  return OK;
}

static int test_cursor(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  unsigned long prefetch_rows= 1;
  unsigned long cursor_type= CURSOR_TYPE_READ_ONLY;

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor_type);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch_rows);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1 FROM DUAL UNION SELECT 2 FROM DUAL", -1);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  rc= mariadb_stmt_execute_direct(stmt, "SELECT 1 FROM DUAL UNION SELECT 2 FROM DUAL", -1);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);
  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_cursor", test_cursor, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"conc_218", conc_218, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"conc_212", conc_212, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"conc_213", conc_213, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"execute_direct", execute_direct, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"execute_direct_example", execute_direct_example, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {NULL, NULL, 0, 0, NULL, NULL}
};


int main(int argc, char **argv)
{

  mysql_library_init(0,0,NULL);

  if (argc > 1)
    get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  mysql_server_end();
  return(exit_status());
}
