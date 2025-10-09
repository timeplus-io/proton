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


#ifndef _WIN32
#include <poll.h>
#else
#include <winsock2.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <mysql.h>

my_bool skip_async= 0;

static int test_async(MYSQL *mysql)
{
  int type;
  mariadb_get_info(mysql, MARIADB_CONNECTION_PVIO_TYPE, &type);
  if (type > MARIADB_CONNECTION_TCP)
  {
    skip_async= 1;
    diag("Asnyc IO not supported");
  }
  return OK;
}

static int
wait_for_mysql(MYSQL *mysql, int status)
{
#ifdef _WIN32
  fd_set rs, ws, es;
  int res;
  struct timeval tv, *timeout;
  my_socket s= mysql_get_socket(mysql);
  FD_ZERO(&rs);
  FD_ZERO(&ws);
  FD_ZERO(&es);
  if (status & MYSQL_WAIT_READ)
    FD_SET(s, &rs);
  if (status & MYSQL_WAIT_WRITE)
    FD_SET(s, &ws);
  if (status & MYSQL_WAIT_EXCEPT)
    FD_SET(s, &es);
  if (status & MYSQL_WAIT_TIMEOUT)
  {
    tv.tv_sec= mysql_get_timeout_value(mysql);
    tv.tv_usec= 0;
    timeout= &tv;
  }
  else
    timeout= NULL;
  res= select(1, &rs, &ws, &es, timeout);
  if (res == 0)
    return MYSQL_WAIT_TIMEOUT;
  else if (res == SOCKET_ERROR)
  {
    /*
      In a real event framework, we should handle errors and re-try the select.
    */
    return MYSQL_WAIT_TIMEOUT;
  }
  else
  {
    int status= 0;
    if (FD_ISSET(s, &rs))
      status|= MYSQL_WAIT_READ;
    if (FD_ISSET(s, &ws))
      status|= MYSQL_WAIT_WRITE;
    if (FD_ISSET(s, &es))
      status|= MYSQL_WAIT_EXCEPT;
    return status;
  }
#else
  struct pollfd pfd;
  int timeout;
  int res= -1;

  pfd.fd= mysql_get_socket(mysql);
  pfd.events=
    (status & MYSQL_WAIT_READ ? POLLIN : 0) |
    (status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
    (status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);
  if (status & MYSQL_WAIT_TIMEOUT)
  {
    timeout= mysql_get_timeout_value_ms(mysql);
  }
  else
    timeout= -1;
  do {
    res= poll(&pfd, 1, timeout);
  } while (res == -1 && errno == EINTR);
  if (res == 0)
    return MYSQL_WAIT_TIMEOUT;
  else if (res < 0)
  {
    /*
      In a real event framework, we should handle EINTR and re-try the poll.
    */
    return MYSQL_WAIT_TIMEOUT;
  }
  else
  {
    int status= 0;
    if (pfd.revents & POLLIN)
      status|= MYSQL_WAIT_READ;
    if (pfd.revents & POLLOUT)
      status|= MYSQL_WAIT_WRITE;
    if (pfd.revents & POLLPRI)
      status|= MYSQL_WAIT_EXCEPT;
    return status;
  }
#endif
}

static int async1(MYSQL *unused __attribute__((unused)))
{
  int err= 0, rc;
  MYSQL mysql, *ret;
  MYSQL_RES *res;
  MYSQL_ROW row;
  int status;
  uint default_timeout;
  int i;

  if (skip_async)
    return SKIP;

  for (i=0; i < 100; i++)
  {

    mysql_init(&mysql);
    rc= mysql_options(&mysql, MYSQL_OPT_NONBLOCK, 0);
    check_mysql_rc(rc, (MYSQL *)&mysql);

    /* set timeouts to 300 microseconds */
    default_timeout= 3;
    mysql_options(&mysql, MYSQL_OPT_READ_TIMEOUT, &default_timeout);
    mysql_options(&mysql, MYSQL_OPT_CONNECT_TIMEOUT, &default_timeout);
    mysql_options(&mysql, MYSQL_OPT_WRITE_TIMEOUT, &default_timeout);
    mysql_options(&mysql, MYSQL_READ_DEFAULT_GROUP, "myapp");
    if (force_tls)
      mysql_ssl_set(&mysql, NULL, NULL, NULL, NULL,NULL);

    /* Returns 0 when done, else flag for what to wait for when need to block. */
    status= mysql_real_connect_start(&ret, &mysql, hostname, username, password, schema, port, socketname, 0);
    while (status)
    {
      status= wait_for_mysql(&mysql, status);
      status= mysql_real_connect_cont(&ret, &mysql, status);
    }
    if (!ret)
    {
      diag("Error: %s", mysql_error(&mysql));
      FAIL_IF(!ret, "Failed to mysql_real_connect()");
    }

    if (force_tls && !mysql_get_ssl_cipher(&mysql))
    {
      diag("Error: No tls connection");
      return FAIL;
    }

    status= mysql_real_query_start(&err, &mysql, SL("SHOW STATUS"));
    while (status)
    {
      status= wait_for_mysql(&mysql, status);
      status= mysql_real_query_cont(&err, &mysql, status);
    }
    FAIL_IF(err, "mysql_real_query() returns error");

    /* This method cannot block. */
    res= mysql_use_result(&mysql);
    FAIL_IF(!res, "mysql_use_result() returns error");

    for (;;)
    {
      status= mysql_fetch_row_start(&row, res);
      while (status)
      {
        status= wait_for_mysql(&mysql, status);
        status= mysql_fetch_row_cont(&row, res, status);
      }
      if (!row)
        break;
    }
    FAIL_IF(mysql_errno(&mysql), "Got error while retrieving rows");
    mysql_free_result(res);

    /*
      mysql_close() sends a COM_QUIT packet, and so in principle could block
      waiting for the socket to accept the data.
      In practise, for many applications it will probably be fine to use the
      blocking mysql_close().
     */
    status= mysql_close_start(&mysql);
    while (status)
    {
      status= wait_for_mysql(&mysql, status);
      status= mysql_close_cont(&mysql, status);
    }
  }
  return OK;
}

static int test_conc131(MYSQL *unused __attribute__((unused)))
{
  int rc;
  /* this test needs to run under valgrind */
  MYSQL *mysql;
  
  if (skip_async)
    return SKIP;

  mysql= mysql_init(NULL);
  rc= mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0);
  check_mysql_rc(rc, mysql);
  mysql_close(mysql);
  return OK;
}

static int test_conc129(MYSQL *unused __attribute__((unused)))
{
  MYSQL *mysql;
  
  if (skip_async)
    return SKIP;

  mysql= mysql_init(NULL);
  FAIL_IF(mysql_close_start(mysql), "No error expected");
  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_async", test_async, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"async1", async1, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"test_conc131", test_conc131, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
  {"test_conc129", test_conc129, TEST_CONNECTION_NONE, 0,  NULL,  NULL},
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
