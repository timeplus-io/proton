/*
*/

#include "my_test.h"
#include "ma_pvio.h"

static int aurora1(MYSQL *unused __attribute__((unused)))
{
  int rc;
  my_bool read_only= 1;
  char *primary, *my_schema;
  MYSQL_RES *res;
  MYSQL *mysql= mysql_init(NULL);

  if (!mysql_real_connect(mysql, hostname, username, password, schema, port, NULL, 0))
  {
    diag("Error: %s", mysql_error(mysql));
    mysql_close(mysql);
    return FAIL;
  }

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t1 (a int, b varchar(20))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (1, 'foo'), (2, 'bar')");
  check_mysql_rc(rc, mysql);

  mariadb_get_infov(mysql, MARIADB_CONNECTION_HOST, &primary);
  diag("primary: %s", primary);

  mysql_options(mysql, MARIADB_OPT_CONNECTION_READ_ONLY, &read_only);

  /* ensure, that this is a replica, so INSERT should fail */
  rc= mysql_query(mysql, "INSERT INTO t1 VALUES (3, 'error')");
  if (rc)
    diag("Expected error: %s", mysql_error(mysql));

  rc= mysql_query(mysql, "SELECT a, b FROM t1");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);

  diag("Num_rows: %lld", mysql_num_rows(res));
  mysql_free_result(res);

  mariadb_get_infov(mysql, MARIADB_CONNECTION_SCHEMA, &my_schema);
  diag("db: %s", my_schema);

  mysql_close(mysql);

  return OK;
}

static int test_wrong_user(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql= mysql_init(NULL);

  if (mysql_real_connect(mysql, hostname, "wrong_user", NULL, NULL, 0, NULL, 0))
  {
    diag("Error expected");
    mysql_close(mysql);
    return FAIL;
  }
  mysql_close(mysql);
  return OK;
}

static int test_reconnect(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql= mysql_init(NULL);
  MYSQL_RES *res;
  my_bool read_only= 1;
  int rc;
  my_bool reconnect= 1;
  char *aurora_host;

  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  if (!mysql_real_connect(mysql, hostname, username, password, schema, port, NULL, 0))
  {
    diag("Error: %s", mysql_error(mysql));
    mysql_close(mysql);
    return FAIL;
  }

  mariadb_get_infov(mysql, MARIADB_CONNECTION_HOST, &aurora_host);
  diag("host: %s", aurora_host);

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tx01");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE tx01 (a int)");
  check_mysql_rc(rc, mysql);

  /* we force cluster restart and promoting new primary:
   * we wait for 50 seconds - however there is no guarantee that
   * cluster was restarted already - so this test might fail */
  rc= system("/usr/local/aws/bin/aws rds failover-db-cluster --db-cluster-identifier instance-1-cluster");

  diag("aws return code: %d", rc);

  sleep(50);
  diag("Q1");
  rc= mysql_query(mysql, "INSERT INTO tx01 VALUES (1)");
  if (!rc)
    diag("error expected!");
  diag("Error: %s", mysql_error(mysql));

  diag("Q2");
  rc= mysql_query(mysql, "INSERT INTO tx01 VALUES (1)");
  if (rc)
  {  
    diag("no error expected!");
    diag("Error: %s", mysql_error(mysql));
    diag("host: %s", mysql->host);
  }
  else
  {
    mariadb_get_infov(mysql, MARIADB_CONNECTION_HOST, &aurora_host);
    diag("host: %s", aurora_host);
  }

  mysql_options(mysql, MARIADB_OPT_CONNECTION_READ_ONLY, &read_only);

  rc= mysql_query(mysql, "SELECT * from tx01");
  check_mysql_rc(rc, mysql);

  if ((res= mysql_store_result(mysql)))
  {
    diag("num_rows: %lld", mysql_num_rows(res));
    mysql_free_result(res);
  }

  mariadb_get_infov(mysql, MARIADB_CONNECTION_HOST, &aurora_host);
  diag("host: %s", aurora_host);

  mysql_close(mysql); 
  return OK;
}

struct my_tests_st my_tests[] = {
  {"aurora1", aurora1, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_wrong_user", test_wrong_user, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_reconnect", test_reconnect, TEST_CONNECTION_NONE, 0, NULL, NULL}, 
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
