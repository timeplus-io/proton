/************************************************************************************
  Copyright (C) 2012 Monty Program AB

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/

#include "my_test.h"

/* Utility function to verify the field members */


static int test_multi_result(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND ps_params[3];  /* input parameter buffers */
  MYSQL_BIND rs_bind[3];
  int        int_data[3];   /* input/output values */
  my_bool    is_null[3];    /* output value nullability */
  int        rc, i;

  /* set up stored procedure */
  rc = mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  rc = mysql_query(mysql,
      "CREATE PROCEDURE p1("
      "  IN p_in INT, "
      "  OUT p_out INT, "
      "  INOUT p_inout INT) "
      "BEGIN "
      "  SELECT p_in, p_out, p_inout; "
      "  SET p_in = 100, p_out = 200, p_inout = 300; "
      "  SELECT p_in, p_out, p_inout; "
      "END");
  check_mysql_rc(rc, mysql);

  /* initialize and prepare CALL statement with parameter placeholders */
  stmt = mysql_stmt_init(mysql);
  if (!stmt)
  {
    diag("Could not initialize statement");
    exit(1);
  }
  rc = mysql_stmt_prepare(stmt, "CALL p1(?, ?, ?)", 16);
  check_stmt_rc(rc, stmt);

  /* initialize parameters: p_in, p_out, p_inout (all INT) */
  memset(ps_params, 0, sizeof (ps_params));

  ps_params[0].buffer_type = MYSQL_TYPE_LONG;
  ps_params[0].buffer = (char *) &int_data[0];
  ps_params[0].length = 0;
  ps_params[0].is_null = 0;

  ps_params[1].buffer_type = MYSQL_TYPE_LONG;
  ps_params[1].buffer = (char *) &int_data[1];
  ps_params[1].length = 0;
  ps_params[1].is_null = 0;

  ps_params[2].buffer_type = MYSQL_TYPE_LONG;
  ps_params[2].buffer = (char *) &int_data[2];
  ps_params[2].length = 0;
  ps_params[2].is_null = 0;

  /* bind parameters */
  rc = mysql_stmt_bind_param(stmt, ps_params);
  check_stmt_rc(rc, stmt);

  /* assign values to parameters and execute statement */
  int_data[0]= 10;  /* p_in */
  int_data[1]= 20;  /* p_out */
  int_data[2]= 30;  /* p_inout */

  rc = mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_field_count(stmt) != 3, "expected 3 fields");

  memset(rs_bind, 0, sizeof (MYSQL_BIND) * 3);
  for (i=0; i < 3; i++)
  {
    rs_bind[i].buffer = (char *) &(int_data[i]);
    rs_bind[i].buffer_length = sizeof (int_data);
    rs_bind[i].buffer_type = MYSQL_TYPE_LONG;
    rs_bind[i].is_null = &is_null[i];
  }
  rc= mysql_stmt_bind_result(stmt, rs_bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
 
  FAIL_IF(int_data[0] != 10 || int_data[1] != 20 || int_data[2] != 30,
          "expected 10 20 30");
  rc= mysql_stmt_next_result(stmt);
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_bind_result(stmt, rs_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(mysql_stmt_field_count(stmt) != 3, "expected 3 fields");
  FAIL_IF(int_data[0] != 100 || int_data[1] != 200 || int_data[2] != 300,
          "expected 100 200 300");

  FAIL_IF(mysql_stmt_next_result(stmt) != 0, "expected more results");
  rc= mysql_stmt_bind_result(stmt, rs_bind);

  rc= mysql_stmt_fetch(stmt);
  FAIL_IF(mysql_stmt_field_count(stmt) != 2, "expected 2 fields");
  FAIL_IF(int_data[0] != 200 || int_data[1] != 300,
          "expected 100 200 300");
  
  FAIL_IF(mysql_stmt_next_result(stmt) != 0, "expected more results");
  FAIL_IF(mysql_stmt_field_count(stmt) != 0, "expected 0 fields");

  rc= mysql_stmt_close(stmt);
  rc = mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);
  return OK;
}

int test_sp_params(MYSQL *mysql)
{
  int i, rc;
  MYSQL_STMT *stmt;
  int a[] = {10,20,30};
  MYSQL_BIND bind[3];
  const char *stmtstr= "CALL P1(?,?,?)";
  char res[3][20];

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE PROCEDURE p1(OUT p_out VARCHAR(19), IN p_in INT, INOUT p_inout INT)" 
                         "BEGIN "
                          "  SET p_in = 300, p_out := 'This is OUT param', p_inout = 200; "
                          "  SELECT p_inout, p_in, substring(p_out, 9);"
                         "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt,SL(stmtstr));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "expected param_count=3");

  memset(bind, 0, sizeof(MYSQL_BIND) * 3);
  for (i=0; i < 3; i++)
  {
    bind[i].buffer= &a[i];
    bind[i].buffer_type= MYSQL_TYPE_LONG;
  }
  bind[0].buffer_type= MYSQL_TYPE_NULL;
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(res, 0, 60);

  memset(bind, 0, sizeof(MYSQL_BIND) * 3);
  for (i=0; i < 3; i++)
  {
    bind[i].buffer_type= MYSQL_TYPE_STRING;
    bind[i].buffer_length= 20;
    bind[i].buffer= res[i];
  }

  do {
    if (mysql->server_status & SERVER_PS_OUT_PARAMS)
    {
      diag("out param result set");
      FAIL_IF(mysql_stmt_field_count(stmt) != 2, "expected 2 columns");
      FAIL_IF(strcmp(stmt->fields[0].org_name, "p_out") != 0, "wrong field name");
      FAIL_IF(strcmp(stmt->fields[1].org_name, "p_inout") != 0, "wrong field name");
      rc= mysql_stmt_bind_result(stmt, bind);
      check_stmt_rc(rc, stmt);
      rc= mysql_stmt_fetch(stmt);
      check_stmt_rc(rc, stmt);
      FAIL_IF(strcmp(res[0],"This is OUT param") != 0, "comparison failed");
      FAIL_IF(strcmp(res[1],"200") != 0, "comparison failed");
    }
    else
    if (mysql_stmt_field_count(stmt))
    {
      diag("sp result set");
      FAIL_IF(mysql_stmt_field_count(stmt) != 3, "expected 3 columns");
      rc= mysql_stmt_bind_result(stmt, bind);
      check_stmt_rc(rc, stmt);
      rc= mysql_stmt_fetch(stmt);
      check_stmt_rc(rc, stmt);
      FAIL_IF(strcmp(res[0],"200") != 0, "comparison failed");
      FAIL_IF(strcmp(res[1],"300") != 0, "comparison failed");
      FAIL_IF(strcmp(res[2],"OUT param") != 0, "comparison failed");

    }
  } while (mysql_stmt_next_result(stmt) == 0);

  rc= mysql_stmt_close(stmt);
  return OK;
}

int test_sp_reset(MYSQL *mysql)
{
 int i, rc;
  MYSQL_STMT *stmt;
  int a[] = {10,20,30};
  MYSQL_BIND bind[3];
  const char *stmtstr= "CALL P1(?,?,?)";

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE PROCEDURE p1(OUT p_out VARCHAR(19), IN p_in INT, INOUT p_inout INT)" 
                         "BEGIN "
                          "  SET p_in = 300, p_out := 'This is OUT param', p_inout = 200; "
                          "  SELECT p_inout, p_in, substring(p_out, 9);"
                         "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt,SL(stmtstr));
  check_stmt_rc(rc, stmt);

  FAIL_IF(mysql_stmt_param_count(stmt) != 3, "expected param_count=3");

  memset(bind, 0, sizeof(MYSQL_BIND) * 3);
  for (i=0; i < 3; i++)
  {
    bind[i].buffer= &a[i];
    bind[i].buffer_type= MYSQL_TYPE_LONG;
  }
  bind[0].buffer_type= MYSQL_TYPE_NULL;
  rc= mysql_stmt_bind_param(stmt, bind);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_reset(stmt);
  check_stmt_rc(rc, stmt);

  /*connection shouldn't be blocked now */

  rc= mysql_query(mysql, "DROP PROCEDURE p1");
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_close(stmt);
  return OK;
}

int test_sp_reset1(MYSQL *mysql)
{
  int rc;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[1];

  char tmp[20];
  const char *stmtstr= "CALL P1(?)";

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE PROCEDURE p1(OUT p_out VARCHAR(19))" 
                         "BEGIN "
                          "  SET p_out = 'foo';"
                          "  SELECT 'foo' FROM DUAL;"
                          "  SELECT 'bar' FROM DUAL;"
                         "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt,SL(stmtstr));
  check_stmt_rc(rc, stmt);

  memset(tmp, 0, sizeof(tmp));
  memset(bind, 0, sizeof(MYSQL_BIND));
  bind[0].buffer= tmp;
  bind[0].buffer_type= MYSQL_TYPE_STRING;
  bind[0].buffer_length= 4;

  mysql_stmt_bind_param(stmt, bind);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_store_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_next_result(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);

  /* mysql_stmt_reset should set statement in prepared state.
   * this means: all subsequent result sets should be flushed.
   * Let's try!
   */ 
  rc= mysql_stmt_reset(stmt);
  check_stmt_rc(rc, stmt);

  rc= mysql_query(mysql, "DROP PROCEDURE p1");
  check_mysql_rc(rc, mysql);

  mysql_stmt_close(stmt);
  return OK;
}

int test_sp_reset2(MYSQL *mysql)
{
  int rc, i;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[4];
  long l[4];
  const char *stmtstr= "CALL P1()";

  memset(l, 0, sizeof(l));

  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE TABLE t1 (a int)");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "CREATE PROCEDURE p1()" 
                         "BEGIN "
                          "  SET @a:=1;"
                          "  INSERT INTO t1 VALUES(1);" 
                          "  SELECT 1 FROM DUAL;"
                          "  SELECT 2,3 FROM DUAL;"
                          "  INSERT INTO t1 VALUES(2);" 
                          "  SELECT 3,4,5 FROM DUAL;"
                          "  SELECT 4,5,6,7 FROM DUAL;"
                         "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt,SL(stmtstr));
  check_stmt_rc(rc, stmt);

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  memset(bind, 0, sizeof(MYSQL_BIND) * 4);
  for (i=0; i < 4; i++)
  {
    bind[i].buffer_type= MYSQL_TYPE_LONG;
    bind[i].buffer= &l[i];
  }

  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  while (rc != MYSQL_NO_DATA)
  { 
    rc= mysql_stmt_fetch(stmt);
    diag("l=%ld", l[0]);
  }
  
  rc= mysql_stmt_next_result(stmt);
  check_stmt_rc(rc, stmt);

  /* now rebind since we expect 2 columns */
  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  while (rc != MYSQL_NO_DATA)
  { 
    rc= mysql_stmt_fetch(stmt);
    diag("l=%ld l=%ld", l[0], l[1]);
  }


  rc= mysql_stmt_next_result(stmt);
  check_stmt_rc(rc, stmt);

  /* now rebind since we expect 2 columns */
  rc= mysql_stmt_bind_result(stmt, bind);
  check_stmt_rc(rc, stmt);

  while (rc != MYSQL_NO_DATA)
  { 
    rc= mysql_stmt_fetch(stmt);
    diag("l=%ld l=%ld l=%ld", l[0], l[1], l[2]);
  }

  rc= mysql_stmt_close(stmt);


  rc= mysql_query(mysql, "DROP PROCEDURE p1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "DROP TABLE IF EXISTS t1");
  check_mysql_rc(rc, mysql);

  return OK;
}

int test_query(MYSQL *mysql)
{
  int rc;
  int i;
  MYSQL_STMT *stmt;
  MYSQL_BIND bind[1];

  char tmp[20];
  const char *stmtstr= "CALL P1(?)";

  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "CREATE PROCEDURE p1(OUT p_out VARCHAR(19))" 
                         "BEGIN "
                          "  SET p_out = 'foo';"
                          "  SELECT 1 FROM DUAL;"
                         "END");
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  check_mysql_rc(rc, mysql);

  rc= mysql_stmt_prepare(stmt,SL(stmtstr));
  check_stmt_rc(rc, stmt);

  for (i=0; i < 1000; i++)
  {
    int status;
    memset(tmp, 0, sizeof(tmp));
    memset(bind, 0, sizeof(MYSQL_BIND));
    bind[0].buffer= tmp;
    bind[0].buffer_type= MYSQL_TYPE_STRING;
    bind[0].buffer_length= 4;

    mysql_stmt_bind_param(stmt, bind);

    rc= mysql_stmt_execute(stmt);
    check_stmt_rc(rc, stmt);
    do {
      if (stmt->field_count)
      {
        mysql_stmt_bind_result(stmt, bind);
        rc= mysql_stmt_store_result(stmt);
        check_stmt_rc(rc, stmt);
        while(mysql_stmt_fetch(stmt) == 0);

        rc= mysql_stmt_free_result(stmt);
        check_stmt_rc(rc, stmt);
      }
      status= mysql_stmt_next_result(stmt);
      if (status == 1)
        check_stmt_rc(status, stmt);
    } while (status == 0);

    rc= mysql_stmt_reset(stmt);
    if (rc)
      diag("reset failed after %d iterations", i);
    check_stmt_rc(rc, stmt);
  }
  mysql_stmt_close(stmt);
  rc= mysql_query(mysql, "DROP PROCEDURE IF EXISTS p1");
  check_mysql_rc(rc, mysql);

  return OK;
}


struct my_tests_st my_tests[] = {
  {"test_query", test_query, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_RESULTS , NULL , NULL},
  {"test_sp_params", test_sp_params, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_STATEMENTS, NULL , NULL},
  {"test_sp_reset", test_sp_reset, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_STATEMENTS, NULL , NULL}, 
  {"test_sp_reset1", test_sp_reset1, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_STATEMENTS, NULL , NULL},
  {"test_sp_reset2", test_sp_reset2, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_STATEMENTS, NULL , NULL},
  {"test_multi_result", test_multi_result, TEST_CONNECTION_DEFAULT, CLIENT_MULTI_STATEMENTS, NULL , NULL},
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
