/*
Copyright (c) 2018 MariaDB Corporation AB

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
#include "mariadb_rpl.h"

static int test_rpl_01(MYSQL *mysql)
{
  MARIADB_RPL_EVENT *event= NULL;
  MARIADB_RPL *rpl= mariadb_rpl_init(mysql);
  mysql_query(mysql, "SET @mariadb_slave_capability=4");
  mysql_query(mysql, "SET NAMES latin1");
  mysql_query(mysql, "SET @slave_gtid_strict_mode=1");
  mysql_query(mysql, "SET @slave_gtid_ignore_duplicates=1");
  mysql_query(mysql, "SET NAMES utf8");
  mysql_query(mysql, "SET @master_binlog_checksum= @@global.binlog_checksum");
  rpl->server_id= 12;
  rpl->start_position= 4;
  rpl->flags= MARIADB_RPL_BINLOG_SEND_ANNOTATE_ROWS;

  if (mariadb_rpl_open(rpl))
    return FAIL;

  while((event= mariadb_rpl_fetch(rpl, event)))
  {
    diag("event: %d\n", event->event_type);
  }
  mariadb_free_rpl_event(event);
  mariadb_rpl_close(rpl);
  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_rpl_01", test_rpl_01, TEST_CONNECTION_DEFAULT, 0, NULL, NULL},
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
