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

/* Bug#15752 "Lost connection to MySQL server when calling a SP from C API" */

static int test_bug15752(MYSQL *mysql)
{
  int rc, i;
  const int ITERATION_COUNT= 100;
  const char *query= "CALL p1()";


  rc= mysql_query(mysql, "drop procedure if exists p1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create procedure p1() select 1");
  check_mysql_rc(rc, mysql);

  rc= mysql_real_query(mysql, SL(query));
  check_mysql_rc(rc, mysql);
  mysql_free_result(mysql_store_result(mysql));

  rc= mysql_real_query(mysql, SL(query));
  FAIL_UNLESS(rc && mysql_errno(mysql) == CR_COMMANDS_OUT_OF_SYNC, "Error expected");

  rc= mysql_next_result(mysql);
  check_mysql_rc(rc, mysql);

  mysql_free_result(mysql_store_result(mysql));

  rc= mysql_next_result(mysql);
  FAIL_IF(rc != -1, "rc != -1");

  for (i = 0; i < ITERATION_COUNT; i++)
  {
    rc= mysql_real_query(mysql, SL(query));
    check_mysql_rc(rc, mysql);
    mysql_free_result(mysql_store_result(mysql));
    rc= mysql_next_result(mysql);
    check_mysql_rc(rc, mysql);
    mysql_free_result(mysql_store_result(mysql));
    rc= mysql_next_result(mysql);
    FAIL_IF(rc != -1, "rc != -1");

  }
  rc= mysql_query(mysql, "drop procedure p1");
  check_mysql_rc(rc, mysql);

  return OK;
}




struct my_tests_st my_tests[] = {
  {"test_bug15752", test_bug15752, TEST_CONNECTION_NEW, 0, NULL , NULL},
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
