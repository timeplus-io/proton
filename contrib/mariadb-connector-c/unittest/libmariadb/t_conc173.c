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

static int test_conc_173(MYSQL *unused __attribute__((unused)))
{
  MYSQL mysql;
  int arg;
  int i=0;

  for (;;)
  {
    mysql_init(&mysql);
    mysql_options(&mysql, MYSQL_READ_DEFAULT_GROUP, "client");
    mysql_options(&mysql, MYSQL_OPT_COMPRESS, 0);

    mysql_options(&mysql, MYSQL_OPT_NAMED_PIPE, 0);

    arg = MYSQL_PROTOCOL_SOCKET;

    mysql_options(&mysql, MYSQL_OPT_PROTOCOL, &arg);

    if(!mysql_real_connect(&mysql, hostname, username, password, schema, 0, 0, 0))  {
      fprintf(stderr, "Failed to connect to database after %d iterations: Error: %s\n", i, mysql_error(&mysql));
      return 1;
    }
    mysql_close(&mysql);
  }
  return OK;
}

struct my_tests_st my_tests[] = {
  {"test_conc_173", test_conc_173, TEST_CONNECTION_DEFAULT, 0, NULL,  NULL},
};


int main(int argc, char **argv)
{
  if (argc > 1)
    get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  return(exit_status());
}
