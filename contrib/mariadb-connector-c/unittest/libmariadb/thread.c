/*
*/

#include "my_test.h"
#include <ma_pthread.h>

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
  FAIL_IF(!field, "Couldn't fetch fields");

  while ((row= mysql_fetch_row(res)) != NULL)
  {
    FAIL_IF(mysql_num_fields(res) != 1, "Got the wrong number of fields");
  }
  FAIL_IF(mysql_errno(my), mysql_error(my));

  mysql_free_result(res);
  mysql_close(my);


  return OK;
}

pthread_mutex_t LOCK_test;

#ifndef _WIN32
int thread_conc27(void);
#else
DWORD WINAPI thread_conc27(void);
#endif

#define THREAD_NUM 100 

/* run this test as root and increase the number of handles (ulimit -n) */
static int test_conc_27(MYSQL *mysql)
{

  int rc;
  int i;
  MYSQL_ROW row;
  MYSQL_RES *res;
#ifndef _WIN32
  pthread_t threads[THREAD_NUM];
#else
  HANDLE hthreads[THREAD_NUM];
  DWORD threads[THREAD_NUM];
#endif

  diag("please run this test manually as root");
  return SKIP;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t_conc27");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE TABLE t_conc27(a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "INSERT INTO t_conc27 VALUES(0)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SET @a:=@@max_connections");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SET GLOBAL max_connections=100000");
  check_mysql_rc(rc, mysql);

  pthread_mutex_init(&LOCK_test, NULL);
  for (i=0; i < THREAD_NUM; i++)
  {
#ifndef _WIN32
    pthread_create(&threads[i], NULL, (void *)thread_conc27, NULL);
#else
    hthreads[i]= CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)thread_conc27, NULL, 0, &threads[i]);
    if (hthreads[i]==NULL)
      diag("error while starting thread");
#endif
  }
  for (i=0; i < THREAD_NUM; i++)
  {
#ifndef _WIN32
    pthread_join(threads[i], NULL);
#else
    WaitForSingleObject(hthreads[i], INFINITE);
#endif
  }

  pthread_mutex_destroy(&LOCK_test);
 
  rc= mysql_query(mysql, "SET GLOBAL max_connections=@a");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT a FROM t_conc27");
  check_mysql_rc(rc,mysql);

  res= mysql_store_result(mysql);
  FAIL_IF(!res, "invalid result");

  row= mysql_fetch_row(res);
  FAIL_IF(!row, "can't fetch row");

  diag("row=%s", row[0]);
  FAIL_IF(atoi(row[0]) != THREAD_NUM, "expected value THREAD_NUM");
  mysql_free_result(res);
  rc= mysql_query(mysql, "DROP TABLE t_conc27");
  check_mysql_rc(rc,mysql);

  return OK;
}

#ifndef _WIN32
int thread_conc27(void)
#else
DWORD WINAPI thread_conc27(void)
#endif
{
  MYSQL *mysql;
  int rc;
  MYSQL_RES *res;
  mysql_thread_init();
  mysql= mysql_init(NULL);
  if(!my_test_connect(mysql, hostname, username, password, schema,
          port, socketname, 0))
  {
    diag(">Error: %s", mysql_error(mysql));
    mysql_close(mysql);
    mysql_thread_end();
    goto end;
  }
  pthread_mutex_lock(&LOCK_test);
  rc= mysql_query(mysql, "UPDATE t_conc27 SET a=a+1");
  check_mysql_rc(rc, mysql);
  pthread_mutex_unlock(&LOCK_test);
  check_mysql_rc(rc, mysql);
  if ((res= mysql_store_result(mysql)))
    mysql_free_result(res);
  mysql_close(mysql);
end:
  mysql_thread_end();
  return 0;
}

struct my_tests_st my_tests[] = {
  {"basic_connect", basic_connect, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_conc_27", test_conc_27, TEST_CONNECTION_NEW, 0, NULL, NULL},
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
