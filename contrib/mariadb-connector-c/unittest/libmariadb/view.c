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

static int test_view(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  MYSQL_BIND      my_bind[1];
  char            str_data[50];
  ulong           length = 0L;
  my_bool         is_null = 0;
  const char *query=
    "SELECT COUNT(*) FROM v1 WHERE SERVERNAME=?";

  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t1,t2,t3,v1");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql, "DROP VIEW IF EXISTS v1,t1,t2,t3");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE TABLE t1 ("
                        " SERVERGRP varchar(20) NOT NULL default '', "
                        " DBINSTANCE varchar(20) NOT NULL default '', "
                        " PRIMARY KEY  (SERVERGRP)) "
                        " CHARSET=latin1 collate=latin1_bin");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE TABLE t2 ("
                        " SERVERNAME varchar(20) NOT NULL, "
                        " SERVERGRP varchar(20) NOT NULL, "
                        " PRIMARY KEY (SERVERNAME)) "
                        " CHARSET=latin1 COLLATE latin1_bin");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE TABLE t3 ("
                  " SERVERGRP varchar(20) BINARY NOT NULL, "
                  " TABNAME varchar(30) NOT NULL, MAPSTATE char(1) NOT NULL, "
                  " ACTSTATE char(1) NOT NULL , "
                  " LOCAL_NAME varchar(30) NOT NULL, "
                  " CHG_DATE varchar(8) NOT NULL default '00000000', "
                  " CHG_TIME varchar(6) NOT NULL default '000000', "
                  " MXUSER varchar(12) NOT NULL default '', "
                  " PRIMARY KEY (SERVERGRP, TABNAME, MAPSTATE, ACTSTATE, "
                  " LOCAL_NAME)) CHARSET=latin1 COLLATE latin1_bin");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE VIEW v1 AS select sql_no_cache"
                  " T0001.SERVERNAME AS SERVERNAME, T0003.TABNAME AS"
                  " TABNAME,T0003.LOCAL_NAME AS LOCAL_NAME,T0002.DBINSTANCE AS"
                  " DBINSTANCE from t2 T0001 join t1 T0002 join t3 T0003 where"
                  " ((T0002.SERVERGRP = T0001.SERVERGRP) and"
                  " (T0002.SERVERGRP = T0003.SERVERGRP)"
                  " and (T0003.MAPSTATE = _latin1'A') and"
                  " (T0003.ACTSTATE = _latin1' '))");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  strcpy(str_data, "TEST");
  memset(my_bind, '\0', sizeof(MYSQL_BIND));
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;
  my_bind[0].buffer= (char *)&str_data;
  my_bind[0].buffer_length= 50;
  my_bind[0].length= &length;
  length= 4;
  my_bind[0].is_null= &is_null;
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    int rowcount= 0;

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);

    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rowcount++; 
    FAIL_IF(rowcount != 1, "Expected 1 row");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1,t2,t3");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_view_where(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query=
    "select v1.c,v2.c from v1, v2";

  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t1,v1,v2");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql, "DROP VIEW IF EXISTS v1,v2,t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE TABLE t1 (a int, b int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"insert into t1 values (1,2), (1,3), (2,4), (2,5), (3,10)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"create view v1 (c) as select b from t1 where a<3");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"create view v2 (c) as select b from t1 where a>=3");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    int rowcount= 0;

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rowcount++;
    FAIL_UNLESS(4 == rowcount, "Expected 4 rows");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW v1, v2");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_view_2where(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  MYSQL_BIND      my_bind[8];
  char            parms[8][100];
  ulong           length[8];
  const char *query=
    "select relid, report, handle, log_group, username, variant, type, "
    "version, erfdat, erftime, erfname, aedat, aetime, aename, dependvars, "
    "inactive from V_LTDX where mandt = ? and relid = ? and report = ? and "
    "handle = ? and log_group = ? and username in ( ? , ? ) and type = ?";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS LTDX");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS V_LTDX");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE TABLE LTDX (MANDT char(3) NOT NULL default '000', "
                  " RELID char(2) NOT NULL, REPORT varchar(40) NOT NULL,"
                  " HANDLE varchar(4) NOT NULL, LOG_GROUP varchar(4) NOT NULL,"
                  " USERNAME varchar(12) NOT NULL,"
                  " VARIANT varchar(12) NOT NULL,"
                  " TYPE char(1) NOT NULL, SRTF2 int(11) NOT NULL,"
                  " VERSION varchar(6) NOT NULL default '000000',"
                  " ERFDAT varchar(8) NOT NULL default '00000000',"
                  " ERFTIME varchar(6) NOT NULL default '000000',"
                  " ERFNAME varchar(12) NOT NULL,"
                  " AEDAT varchar(8) NOT NULL default '00000000',"
                  " AETIME varchar(6) NOT NULL default '000000',"
                  " AENAME varchar(12) NOT NULL,"
                  " DEPENDVARS varchar(10) NOT NULL,"
                  " INACTIVE char(1) NOT NULL, CLUSTR smallint(6) NOT NULL,"
                  " CLUSTD blob,"
                  " PRIMARY KEY (MANDT, RELID, REPORT, HANDLE, LOG_GROUP, "
                                "USERNAME, VARIANT, TYPE, SRTF2))"
                 " CHARSET=latin1 COLLATE latin1_bin");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE VIEW V_LTDX AS select T0001.MANDT AS "
                  " MANDT,T0001.RELID AS RELID,T0001.REPORT AS "
                  " REPORT,T0001.HANDLE AS HANDLE,T0001.LOG_GROUP AS "
                  " LOG_GROUP,T0001.USERNAME AS USERNAME,T0001.VARIANT AS "
                  " VARIANT,T0001.TYPE AS TYPE,T0001.VERSION AS "
                  " VERSION,T0001.ERFDAT AS ERFDAT,T0001.ERFTIME AS "
                  " ERFTIME,T0001.ERFNAME AS ERFNAME,T0001.AEDAT AS "
                  " AEDAT,T0001.AETIME AS AETIME,T0001.AENAME AS "
                  " AENAME,T0001.DEPENDVARS AS DEPENDVARS,T0001.INACTIVE AS "
                  " INACTIVE from LTDX T0001 where (T0001.SRTF2 = 0)");
  check_mysql_rc(rc, mysql);
  memset(my_bind, '\0', sizeof(MYSQL_BIND));
  for (i=0; i < 8; i++) {
    strcpy(parms[i], "1");
    my_bind[i].buffer_type = MYSQL_TYPE_VAR_STRING;
    my_bind[i].buffer = (char *)&parms[i];
    my_bind[i].buffer_length = 1;
    my_bind[i].is_null = 0;
    length[i] = 1;
    my_bind[i].length = &length[i];
  }
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(MYSQL_NO_DATA == rc, "Expected 0 rows");

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP VIEW V_LTDX");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE LTDX");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_view_star(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  MYSQL_BIND      my_bind[8];
  char            parms[8][100];
  ulong           length[8];
  const char *query= "SELECT * FROM vt1 WHERE a IN (?,?)";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, vt1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS t1, vt1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE VIEW vt1 AS SELECT a FROM t1");
  check_mysql_rc(rc, mysql);
  memset(my_bind, '\0', sizeof(MYSQL_BIND));
  for (i= 0; i < 2; i++) {
    sprintf((char *)&parms[i], "%d", i);
    my_bind[i].buffer_type = MYSQL_TYPE_VAR_STRING;
    my_bind[i].buffer = (char *)&parms[i];
    my_bind[i].buffer_length = 100;
    my_bind[i].is_null = 0;
    my_bind[i].length = &length[i];
    length[i] = 1;
  }

  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    rc= mysql_stmt_fetch(stmt);
    FAIL_UNLESS(MYSQL_NO_DATA == rc, "Expected 0 rows");
  }

  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW vt1");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_view_insert(MYSQL *mysql)
{
  MYSQL_STMT *insert_stmt, *select_stmt;
  int rc, i;
  MYSQL_BIND      my_bind[1];
  int             my_val = 0;
  ulong           my_length = 0L;
  my_bool         my_null = 0;
  const char *query=
    "insert into v1 values (?)";

  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t1,v1");
  check_mysql_rc(rc, mysql);
  rc = mysql_query(mysql, "DROP VIEW IF EXISTS t1,v1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql,"create table t1 (a int, primary key (a))");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create view v1 as select a from t1 where a>=1");
  check_mysql_rc(rc, mysql);

  insert_stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(insert_stmt, SL(query));
  check_stmt_rc(rc, insert_stmt);
  query= "select * from t1";
  select_stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(select_stmt, SL(query));
  check_stmt_rc(rc, select_stmt);

  memset(my_bind, '\0', sizeof(MYSQL_BIND));
  my_bind[0].buffer_type = MYSQL_TYPE_LONG;
  my_bind[0].buffer = (char *)&my_val;
  my_bind[0].length = &my_length;
  my_bind[0].is_null = &my_null;
  rc= mysql_stmt_bind_param(insert_stmt, my_bind);
  check_stmt_rc(rc, select_stmt);

  for (i= 0; i < 3; i++)
  {
    int rowcount= 0;
    my_val= i;

    rc= mysql_stmt_execute(insert_stmt);
    check_stmt_rc(rc, insert_stmt);;

    rc= mysql_stmt_execute(select_stmt);
    check_stmt_rc(rc, select_stmt);;
    while (mysql_stmt_fetch(select_stmt) != MYSQL_NO_DATA)
      rowcount++;
    FAIL_UNLESS((i+1) == rowcount, "rowcount != i+1");
  }
  mysql_stmt_close(insert_stmt);
  mysql_stmt_close(select_stmt);

  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_left_join_view(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc, i;
  const char *query=
    "select t1.a, v1.x from t1 left join v1 on (t1.a= v1.x);";

  rc = mysql_query(mysql, "DROP TABLE IF EXISTS t1,v1");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql, "DROP VIEW IF EXISTS v1,t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"insert into t1 values (1), (2), (3)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,"create view v1 (x) as select a from t1 where a > 1");
  check_mysql_rc(rc, mysql);
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);

  for (i= 0; i < 3; i++)
  {
    int rowcount= 0;

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
      rowcount++;
    FAIL_UNLESS(3 == rowcount, "Expected 3 rows");
  }
  mysql_stmt_close(stmt);

  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}


static int test_view_insert_fields(MYSQL *mysql)
{
  MYSQL_STMT    *stmt;
  char          parm[11][1000];
  ulong         l[11];
  int           rc, i;
  int           rowcount= 0;
  MYSQL_BIND    my_bind[11];
  const char    *query= "INSERT INTO `v1` ( `K1C4` ,`K2C4` ,`K3C4` ,`K4N4` ,`F1C4` ,`F2I4` ,`F3N5` ,`F7F8` ,`F6N4` ,`F5C8` ,`F9D8` ) VALUES( ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )";

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1, v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS t1, v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE TABLE t1 (K1C4 varchar(4) NOT NULL,"
                  "K2C4 varchar(4) NOT NULL, K3C4 varchar(4) NOT NULL,"
                  "K4N4 varchar(4) NOT NULL default '0000',"
                  "F1C4 varchar(4) NOT NULL, F2I4 int(11) NOT NULL,"
                  "F3N5 varchar(5) NOT NULL default '00000',"
                  "F4I4 int(11) NOT NULL default '0', F5C8 varchar(8) NOT NULL,"
                  "F6N4 varchar(4) NOT NULL default '0000',"
                  "F7F8 double NOT NULL default '0',"
                  "F8F8 double NOT NULL default '0',"
                  "F9D8 decimal(8,2) NOT NULL default '0.00',"
                  "PRIMARY KEY (K1C4,K2C4,K3C4,K4N4)) "
                  "CHARSET=latin1 COLLATE latin1_bin");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql,
                  "CREATE VIEW v1 AS select sql_no_cache "
                  " K1C4 AS K1C4, K2C4 AS K2C4, K3C4 AS K3C4, K4N4 AS K4N4, "
                  " F1C4 AS F1C4, F2I4 AS F2I4, F3N5 AS F3N5,"
                  " F7F8 AS F7F8, F6N4 AS F6N4, F5C8 AS F5C8, F9D8 AS F9D8"
                  " from t1 T0001");

  memset(my_bind, '\0', sizeof(my_bind));
  for (i= 0; i < 11; i++)
  {
    l[i]= 20;
    my_bind[i].buffer_type= MYSQL_TYPE_STRING;
    my_bind[i].is_null= 0;
    my_bind[i].buffer= (char *)&parm[i];

    strcpy(parm[i], "1");
    my_bind[i].buffer_length= 2;
    my_bind[i].length= &l[i];
  }
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_bind_param(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  mysql_stmt_close(stmt);

  query= "select * from t1";
  stmt= mysql_stmt_init(mysql);
  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    rowcount++;
  FAIL_UNLESS(1 == rowcount, "Expected 1 row");

  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_view_sp_list_fields(MYSQL *mysql)
{
  int           rc;
  MYSQL_RES     *res;
  MYSQL_ROW     row;
  int           skip;

  /* skip this test if bin_log is on */
  rc= mysql_query(mysql, "SHOW VARIABLES LIKE 'log_bin'");
  check_mysql_rc(rc, mysql);
  res= mysql_store_result(mysql);
  FAIL_IF(!res, "empty/invalid resultset");
  row = mysql_fetch_row(res);
  skip= (strcmp((char *)row[1], "ON") == 0);
  mysql_free_result(res);

  if (skip) {
    diag("bin_log is ON -> skip");
    return SKIP;
  }

  rc= mysql_query(mysql, "DROP FUNCTION IF EXISTS f1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS v1, t1, t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS v1, t1, t2");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create function f1 () returns int return 5");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (s1 char,s2 char)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t2 (s1 int);");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create view v1 as select s2,sum(s1) - \
count(s2) as vx from t1 group by s2 having sum(s1) - count(s2) < (select f1() \
from t2);");
  check_mysql_rc(rc, mysql);
  res= mysql_list_fields(mysql, "v1", NullS);
  FAIL_UNLESS(res != 0 && mysql_num_fields(res) != 0, "0 Fields");
  rc= mysql_query(mysql, "DROP FUNCTION f1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE t1, t2");
  mysql_free_result(res);
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug19671(MYSQL *mysql)
{
  MYSQL_RES *result;
  MYSQL_FIELD *field;
  int rc, retcode= OK;


  rc= mysql_query(mysql, "set sql_mode=''");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "drop table if exists t1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "drop view if exists v1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create table t1(f1 int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "create view v1 as select va.* from t1 va");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "SELECT * FROM v1");
  check_mysql_rc(rc, mysql);

  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  field= mysql_fetch_field(result);
  FAIL_IF(!field, "Can't fetch field");

  if (strcmp(field->table, "v1") != 0) {
    diag("Wrong value '%s' for field_table. Expected 'v1'. (%s: %d)", field->table, __FILE__, __LINE__);
    retcode= FAIL;
  }

  mysql_free_result(result);

  rc= mysql_query(mysql, "drop view v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "drop table t1");
  check_mysql_rc(rc, mysql);

  return retcode;
}

/*
  Bug#11111: fetch from view returns wrong data
*/

static int test_bug11111(MYSQL *mysql)
{
  MYSQL_STMT    *stmt;
  MYSQL_BIND    my_bind[2];
  char          buf[2][20];
  ulong         len[2];
  int i;
  int rc;
  const char *query= "SELECT DISTINCT f1,ff2 FROM v1";

  rc= mysql_query(mysql, "drop table if exists t1, t2, v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "drop view if exists t1, t2, v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t1 (f1 int, f2 int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create table t2 (ff1 int, ff2 int)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "create view v1 as select * from t1, t2 where f1=ff1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t1 values (1,1), (2,2), (3,3)");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "insert into t2 values (1,1), (2,2), (3,3)");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, SL(query));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  for (i=0; i < 2; i++)
  {
    my_bind[i].buffer_type= MYSQL_TYPE_STRING;
    my_bind[i].buffer= (uchar* *)&buf[i];
    my_bind[i].buffer_length= 20;
    my_bind[i].length= &len[i];
  }

  rc= mysql_stmt_bind_result(stmt, my_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(!strcmp(buf[1],"1"), "buf[1] != '1'");
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "drop view v1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "drop table t1, t2");
  check_mysql_rc(rc, mysql);

  return OK;
}

/**
  Bug#29306 Truncated data in MS Access with decimal (3,1) columns in a VIEW
*/

static int test_bug29306(MYSQL *mysql)
{
  MYSQL_FIELD *field;
  int rc;
  MYSQL_RES *res;

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS tab17557");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW IF EXISTS view17557");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE tab17557 (dd decimal (3,1))");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE VIEW view17557 as SELECT dd FROM tab17557");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "INSERT INTO tab17557 VALUES (7.6)");
  check_mysql_rc(rc, mysql);

  /* Checking the view */
  res= mysql_list_fields(mysql, "view17557", NULL);
  while ((field= mysql_fetch_field(res)))
  {
    FAIL_UNLESS(field->decimals == 1, "field->decimals != 1");
  }
  mysql_free_result(res);

  rc= mysql_query(mysql, "DROP TABLE tab17557");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP VIEW view17557");
  check_mysql_rc(rc, mysql);

  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_view", test_view, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_view_where", test_view_where, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_view_2where", test_view_2where, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_view_star", test_view_star, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_view_insert", test_view_insert, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_left_join_view", test_left_join_view, TEST_CONNECTION_DEFAULT, 0, NULL , NULL}, 
  {"test_view_insert_fields", test_view_insert_fields, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_view_sp_list_fields", test_view_sp_list_fields,TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug19671", test_bug19671, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug29306", test_bug29306, TEST_CONNECTION_DEFAULT, 0, NULL , NULL},
  {"test_bug11111", test_bug11111, TEST_CONNECTION_DEFAULT, 0, NULL , NULL}, 
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
