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

/*
 test gbk charset escaping

 The important part is that 0x27 (') is the second-byte in a invalid
 two-byte GBK character here. But 0xbf5c is a valid GBK character, so
 it needs to be escaped as 0x5cbf27

*/
#define TEST_BUG8378_IN  "\xef\xbb\xbf\x27\xbf\x10"
#define TEST_BUG8378_OUT "\xef\xbb\x5c\xbf\x5c\x27\x5c\xbf\x10"

/* set connection options */
struct my_option_st opt_bug8378[] = {
  {MYSQL_SET_CHARSET_NAME, (char *) "gbk"},
  {0, NULL}
};

int bug_8378(MYSQL *mysql) {
  int rc, len;
  char out[9], buf[256];
  MYSQL_RES *res;
  MYSQL_ROW row;

  len= mysql_real_escape_string(mysql, out, TEST_BUG8378_IN, 4);
  FAIL_IF(memcmp(out, TEST_BUG8378_OUT, len), "wrong result");

  sprintf(buf, "SELECT '%s' FROM DUAL", TEST_BUG8378_OUT);
 
  rc= mysql_query(mysql, buf);
  check_mysql_rc(rc, mysql);

  if ((res= mysql_store_result(mysql))) {
    row= mysql_fetch_row(res);
    if (memcmp(row[0], TEST_BUG8378_IN, 4)) {
      mysql_free_result(res);
      return FAIL;
    }
    mysql_free_result(res);
  } else
    return FAIL;

  return OK;
}

int test_client_character_set(MYSQL *mysql)
{
  MY_CHARSET_INFO cs;
  char *csname= (char*) "utf8";
  char *csdefault= (char*)mysql_character_set_name(mysql);

  FAIL_IF(mysql_set_character_set(mysql, csname), mysql_error(mysql));

  mysql_get_character_set_info(mysql, &cs);

  FAIL_IF(strcmp(cs.csname, "utf8") || strcmp(cs.name, "utf8_general_ci"), "Character set != UTF8");
  FAIL_IF(mysql_set_character_set(mysql, csdefault), mysql_error(mysql));

  return OK;
}

/*
 * Regression test for bug #10214
 *
 * Test escaping with NO_BACKSLASH_ESCAPES
 *
 */
int bug_10214(MYSQL *mysql)
{
  int   len, rc;
  char  out[8];

  /* reset sql_mode */
  rc= mysql_query(mysql, "SET sql_mode=''");
  check_mysql_rc(rc, mysql);

  len= mysql_real_escape_string(mysql, out, "a'b\\c", 5);
  FAIL_IF(memcmp(out, "a\\'b\\\\c", len), NULL);

  rc= mysql_query(mysql, "set sql_mode='NO_BACKSLASH_ESCAPES'");
  check_mysql_rc(rc, mysql);
  FAIL_IF(!(mysql->server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES), 
          "wrong server status: NO_BACKSLASH_ESCAPES not set");

  len= mysql_real_escape_string(mysql, out, "a'b\\c", 5);
  FAIL_IF(memcmp(out, "a''b\\c", len), "wrong result");

  return OK;
}

/*
 *  simple escaping of special chars
 */
int test_escaping(MYSQL *mysql)
{
  int i= 0, rc, len;
  char out[20];
  const char *escape_chars[] = {"'", "\x0", "\n", "\r", "\\", "\0", NULL};

  /* reset sql_mode, mysql_change_user call doesn't reset it */
  rc= mysql_query(mysql, "SET sql_mode=''");
  check_mysql_rc(rc, mysql);

  while (escape_chars[i]) {
    len= mysql_real_escape_string(mysql, out, escape_chars[i], 1);
    FAIL_IF(len < 2, "Len < 2");
    i++;
  }

  return OK;
}

/*
 * server doesn't reset sql_mode after COM_CHANGE_USER
 */
int bug_41785(MYSQL *mysql)
{
  char out[10];
  int rc, len;

  len= mysql_real_escape_string(mysql, out, "\\", 1);
  FAIL_IF(len != 2, "len != 2");

  rc= mysql_query(mysql, "SET SQL_MODE=NO_BACKSLASH_ESCAPES");
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "SET sql_mode=''");
  check_mysql_rc(rc, mysql);

  mysql_change_user(mysql, "root", "", "test");

  len= mysql_real_escape_string(mysql, out, "\\", 1);
  FAIL_IF(len != 2, "len != 2");

  return OK;
}

static int test_conversion(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  const char *stmt_text;
  int rc;
  MYSQL_BIND my_bind[1];
  uchar buff[4];
  ulong length;

  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "CREATE TABLE t1 (a TEXT) DEFAULT CHARSET latin1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "SET character_set_connection=utf8, character_set_client=utf8, "
             " character_set_results=latin1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt= mysql_stmt_init(mysql);
  FAIL_IF(!stmt, mysql_error(mysql));
  stmt_text= "INSERT INTO t1 (a) VALUES (?)";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);

  memset(my_bind, '\0', sizeof(my_bind));
  my_bind[0].buffer= (char*) buff;
  my_bind[0].length= &length;
  my_bind[0].buffer_type= MYSQL_TYPE_STRING;

  mysql_stmt_bind_param(stmt, my_bind);

  buff[0]= (uchar) 0xC3;
  buff[1]= (uchar) 0xA0;
  length= 2;

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  stmt_text= "SELECT a FROM t1";
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);

  my_bind[0].buffer_length= sizeof(buff);
  mysql_stmt_bind_result(stmt, my_bind);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(length == 1, "length != 1");
  FAIL_UNLESS(buff[0] == 0xE0, "buff[0] != 0xE0");
  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  mysql_stmt_close(stmt);
  stmt_text= "DROP TABLE t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "SET NAMES DEFAULT";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  return OK;
}

static int test_bug27876(MYSQL *mysql)
{
  int rc;
  MYSQL_RES *result;

  uchar utf8_func[] =
  {
    0xd1, 0x84, 0xd1, 0x83, 0xd0, 0xbd, 0xd0, 0xba,
    0xd1, 0x86, 0xd0, 0xb8, 0xd0, 0xb9, 0xd0, 0xba,
    0xd0, 0xb0,
    0x00
  };

  uchar utf8_param[] =
  {
    0xd0, 0xbf, 0xd0, 0xb0, 0xd1, 0x80, 0xd0, 0xb0,
    0xd0, 0xbc, 0xd0, 0xb5, 0xd1, 0x82, 0xd1, 0x8a,
    0xd1, 0x80, 0x5f, 0xd0, 0xb2, 0xd0, 0xb5, 0xd1,
    0x80, 0xd1, 0x81, 0xd0, 0xb8, 0xd1, 0x8f,
    0x00
  };

  char query[500];

  rc= mysql_query(mysql, "set names utf8");
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "select version()");
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");
  mysql_free_result(result);

  sprintf(query, "DROP FUNCTION IF EXISTS %s", (char*) utf8_func);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  sprintf(query,
          "CREATE FUNCTION %s( %s VARCHAR(25))"
          " RETURNS VARCHAR(25) DETERMINISTIC RETURN %s",
          (char*) utf8_func, (char*) utf8_param, (char*) utf8_param);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);
  sprintf(query, "SELECT %s(VERSION())", (char*) utf8_func);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);
  result= mysql_store_result(mysql);
  FAIL_IF(!result, "Invalid result set");
  mysql_free_result(result);

  sprintf(query, "DROP FUNCTION %s", (char*) utf8_func);
  rc= mysql_query(mysql, query);
  check_mysql_rc(rc, mysql);

  rc= mysql_query(mysql, "set names default");
  check_mysql_rc(rc, mysql);
  return OK;
}

static int test_ps_i18n(MYSQL *mysql)
{
  MYSQL_STMT *stmt;
  int rc;
  const char *stmt_text;
  MYSQL_BIND bind_array[2];

  /* Represented as numbers to keep UTF8 tools from clobbering them. */
  const char *koi8= "\xee\xd5\x2c\x20\xda\xc1\x20\xd2\xd9\xc2\xc1\xcc\xcb\xd5";
  const char *cp1251= "\xcd\xf3\x2c\x20\xe7\xe0\x20\xf0\xfb\xe1\xe0\xeb\xea\xf3";
  char buf1[16], buf2[16];
  ulong buf1_len, buf2_len;

  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  /*
    Create table with binary columns, set session character set to cp1251,
    client character set to koi8, and make sure that there is conversion
    on insert and no conversion on select
  */

  stmt_text= "CREATE TABLE t1 (c1 VARBINARY(255), c2 VARBINARY(255))";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "SET CHARACTER_SET_CLIENT=koi8r, "
                 "CHARACTER_SET_CONNECTION=cp1251, "
                 "CHARACTER_SET_RESULTS=koi8r";

  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  memset(bind_array, '\0', sizeof(bind_array));
  bind_array[0].buffer_type= MYSQL_TYPE_STRING;
  bind_array[0].buffer= (void *) koi8;
  bind_array[0].buffer_length= (unsigned long)strlen(koi8);

  bind_array[1].buffer_type= MYSQL_TYPE_STRING;
  bind_array[1].buffer= (void *) koi8;
  bind_array[1].buffer_length= (unsigned long)strlen(koi8);

  stmt= mysql_stmt_init(mysql);
  check_stmt_rc(rc, stmt);

  stmt_text= "INSERT INTO t1 (c1, c2) VALUES (?, ?)";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  mysql_stmt_bind_param(stmt, bind_array);
  check_stmt_rc(rc, stmt);

//  mysql_stmt_send_long_data(stmt, 0, koi8, strlen(koi8));

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  stmt_text= "SELECT c1, c2 FROM t1";

  /* c1 and c2 are binary so no conversion will be done on select */
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  bind_array[0].buffer= buf1;
  bind_array[0].buffer_length= sizeof(buf1);
  bind_array[0].length= &buf1_len;

  bind_array[1].buffer= buf2;
  bind_array[1].buffer_length= sizeof(buf2);
  bind_array[1].length= &buf2_len;

  mysql_stmt_bind_result(stmt, bind_array);

  rc= mysql_stmt_fetch(stmt);
  check_stmt_rc(rc, stmt);
  FAIL_UNLESS(buf1_len == strlen(cp1251), "buf1_len != strlen(cp1251)");
  FAIL_UNLESS(buf2_len == strlen(cp1251), "buf2_len != strlen(cp1251)");
  FAIL_UNLESS(!memcmp(buf1, cp1251, buf1_len), "buf1 != cp1251");
  FAIL_UNLESS(!memcmp(buf2, cp1251, buf1_len), "buf2 != cp1251");

  rc= mysql_stmt_fetch(stmt);
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");

  stmt_text= "DROP TABLE IF EXISTS t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  /*
    Now create table with two cp1251 columns, set client character
    set to koi8 and supply columns of one row as string and another as
    binary data. Binary data must not be converted on insert, and both
    columns must be converted to client character set on select.
  */

  stmt_text= "CREATE TABLE t1 (c1 VARCHAR(255) CHARACTER SET cp1251, "
                              "c2 VARCHAR(255) CHARACTER SET cp1251)";

  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);

  stmt_text= "INSERT INTO t1 (c1, c2) VALUES (?, ?)";

  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  /* this data must be converted */
  bind_array[0].buffer_type= MYSQL_TYPE_STRING;
  bind_array[0].buffer= (void *) koi8;
  bind_array[0].buffer_length= (unsigned long)strlen(koi8);

  bind_array[1].buffer_type= MYSQL_TYPE_STRING;
  bind_array[1].buffer= (void *) koi8;
  bind_array[1].buffer_length= (unsigned long)strlen(koi8);

  mysql_stmt_bind_param(stmt, bind_array);

//  mysql_stmt_send_long_data(stmt, 0, koi8, strlen(koi8));

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* this data must not be converted */
  bind_array[0].buffer_type= MYSQL_TYPE_BLOB;
  bind_array[0].buffer= (void *) cp1251;
  bind_array[0].buffer_length= (unsigned long)strlen(cp1251);

  bind_array[1].buffer_type= MYSQL_TYPE_BLOB;
  bind_array[1].buffer= (void *) cp1251;
  bind_array[1].buffer_length= (unsigned long)strlen(cp1251);

  mysql_stmt_bind_param(stmt, bind_array);

//  mysql_stmt_send_long_data(stmt, 0, cp1251, strlen(cp1251));

  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  /* Fetch data and verify that rows are in koi8 */

  stmt_text= "SELECT c1, c2 FROM t1";

  /* c1 and c2 are binary so no conversion will be done on select */
  rc= mysql_stmt_prepare(stmt, SL(stmt_text));
  check_stmt_rc(rc, stmt);
  rc= mysql_stmt_execute(stmt);
  check_stmt_rc(rc, stmt);
  bind_array[0].buffer= buf1;
  bind_array[0].buffer_length= sizeof(buf1);
  bind_array[0].length= &buf1_len;

  bind_array[1].buffer= buf2;
  bind_array[1].buffer_length= sizeof(buf2);
  bind_array[1].length= &buf2_len;

  mysql_stmt_bind_result(stmt, bind_array);

  while ((rc= mysql_stmt_fetch(stmt)) == 0)
  {
    FAIL_UNLESS(buf1_len == strlen(koi8), "buf1_len != strlen(koi8)");
    FAIL_UNLESS(buf2_len == strlen(koi8), "buf2_len != strlen(koi8)");
    FAIL_UNLESS(!memcmp(buf1, koi8, buf1_len), "buf1 != koi8");
    FAIL_UNLESS(!memcmp(buf2, koi8, buf1_len), "buf2 != koi8");
  }
  FAIL_UNLESS(rc == MYSQL_NO_DATA, "rc != MYSQL_NO_DATA");
  mysql_stmt_close(stmt);

  stmt_text= "DROP TABLE t1";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  stmt_text= "SET NAMES DEFAULT";
  rc= mysql_real_query(mysql, SL(stmt_text));
  check_mysql_rc(rc, mysql);
  return OK;
}

/*
  Bug#30472: libmysql doesn't reset charset, insert_id after succ.
  mysql_change_user() call row insertions.
*/

static int bug30472_retrieve_charset_info(MYSQL *con,
                                           char *character_set_name,
                                           char *character_set_client,
                                           char *character_set_results,
                                           char *collation_connection)
{
  MYSQL_RES *rs;
  MYSQL_ROW row;
  int       rc;

  /* Get the cached client character set name. */

  strcpy(character_set_name, mysql_character_set_name(con));

  /* Retrieve server character set information. */

  rc= mysql_query(con, "SHOW VARIABLES LIKE 'character_set_client'");
  check_mysql_rc(rc, con);

  rs= mysql_store_result(con);
  FAIL_IF(!rs, "Invalid result set");
  row= mysql_fetch_row(rs);
  FAIL_IF(!row, "Couldn't fetch row");
  strcpy(character_set_client, row[1]);
  diag("cs: %s", row[1]);
  mysql_free_result(rs);

  rc= mysql_query(con, "SHOW VARIABLES LIKE 'character_set_results'");
  check_mysql_rc(rc, con);
  rs= mysql_store_result(con);
  FAIL_IF(!rs, "Invalid result set");
  row= mysql_fetch_row(rs);
  FAIL_IF(!row, "Couldn't fetch row");
  strcpy(character_set_results, row[1]);
  mysql_free_result(rs);

  rc= mysql_query(con, "SHOW VARIABLES LIKE 'collation_connection'");
  check_mysql_rc(rc, con);
  rs= mysql_store_result(con);
  FAIL_IF(!rs, "Invalid result set");
  row= mysql_fetch_row(rs);
  FAIL_IF(!row, "Couldn't fetch row");
  strcpy(collation_connection, row[1]);
  mysql_free_result(rs);
  return OK;
}

#define MY_CS_NAME_SIZE 32

static int test_bug30472(MYSQL *mysql)
{
  int   rc;

  char character_set_name_1[MY_CS_NAME_SIZE];
  char character_set_client_1[MY_CS_NAME_SIZE];
  char character_set_results_1[MY_CS_NAME_SIZE];
  char collation_connnection_1[MY_CS_NAME_SIZE];

  char character_set_name_2[MY_CS_NAME_SIZE];
  char character_set_client_2[MY_CS_NAME_SIZE];
  char character_set_results_2[MY_CS_NAME_SIZE];
  char collation_connnection_2[MY_CS_NAME_SIZE];

  char character_set_name_3[MY_CS_NAME_SIZE];
  char character_set_client_3[MY_CS_NAME_SIZE];
  char character_set_results_3[MY_CS_NAME_SIZE];
  char collation_connnection_3[MY_CS_NAME_SIZE];

  char character_set_name_4[MY_CS_NAME_SIZE];
  char character_set_client_4[MY_CS_NAME_SIZE];
  char character_set_results_4[MY_CS_NAME_SIZE];
  char collation_connnection_4[MY_CS_NAME_SIZE];

  if (mysql_get_server_version(mysql) < 50100 || !is_mariadb) 
  {
    diag("Test requires MySQL Server version 5.1 or above");
    return SKIP;
  }
  /* Retrieve character set information. */

  mysql_set_character_set(mysql, "latin1");
  bug30472_retrieve_charset_info(mysql,
                                 character_set_name_1,
                                 character_set_client_1,
                                 character_set_results_1,
                                 collation_connnection_1);

  /* Switch client character set. */

  FAIL_IF(mysql_set_character_set(mysql, "utf8"), "Setting cs to utf8 failed");

  /* Retrieve character set information. */

  bug30472_retrieve_charset_info(mysql,
                                 character_set_name_2,
                                 character_set_client_2,
                                 character_set_results_2,
                                 collation_connnection_2);

  /*
    Check that
      1) character set has been switched and
      2) new character set is different from the original one.
  */

  FAIL_UNLESS(strcmp(character_set_name_2, "utf8") == 0, "cs_name != utf8");
  FAIL_UNLESS(strcmp(character_set_client_2, "utf8") == 0, "cs_client != utf8");
  FAIL_UNLESS(strcmp(character_set_results_2, "utf8") == 0, "cs_result != ut8");
  FAIL_UNLESS(strcmp(collation_connnection_2, "utf8_general_ci") == 0, "collation != utf8_general_ci");

  diag("%s %s", character_set_name_1, character_set_name_2);
  FAIL_UNLESS(strcmp(character_set_name_1, character_set_name_2) != 0, "cs_name1 = cs_name2");
  FAIL_UNLESS(strcmp(character_set_client_1, character_set_client_2) != 0, "cs_client1 = cs_client2");
  FAIL_UNLESS(strcmp(character_set_results_1, character_set_results_2) != 0, "cs_result1 = cs_result2");
  FAIL_UNLESS(strcmp(collation_connnection_1, collation_connnection_2) != 0, "collation1 = collation2");

  /* Call mysql_change_user() with the same username, password, database. */

  rc= mysql_change_user(mysql, username, password, (schema) ? schema : "test");
  mysql_set_character_set(mysql, "latin1");
  check_mysql_rc(rc, mysql);

  /* Retrieve character set information. */

  bug30472_retrieve_charset_info(mysql,
                                 character_set_name_3,
                                 character_set_client_3,
                                 character_set_results_3,
                                 collation_connnection_3);

  /* Check that character set information has been reset. */

  FAIL_UNLESS(strcmp(character_set_name_1, character_set_name_3) == 0, "cs_name1 != cs_name3");
  FAIL_UNLESS(strcmp(character_set_client_1, character_set_client_3) == 0, "cs_client1 != cs_client3");
  FAIL_UNLESS(strcmp(character_set_results_1, character_set_results_3) == 0, "cs_result1 != cs_result3");
  FAIL_UNLESS(strcmp(collation_connnection_1, collation_connnection_3) == 0, "collation1 != collation3");

  /* Change connection-default character set in the client. */

  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");

  /*
    Call mysql_change_user() in order to check that new connection will
    have UTF8 character set on the client and on the server.
  */

  rc= mysql_change_user(mysql, username, password, (schema) ? schema : "test");
  check_mysql_rc(rc, mysql);

  /* Retrieve character set information. */

  bug30472_retrieve_charset_info(mysql,
                                 character_set_name_4,
                                 character_set_client_4,
                                 character_set_results_4,
                                 collation_connnection_4);

  /* Check that we have UTF8 on the server and on the client. */

  FAIL_UNLESS(strcmp(character_set_name_4, "utf8") == 0, "cs_name != utf8");
  FAIL_UNLESS(strcmp(character_set_client_4, "utf8") == 0, "cs_client != utf8");
  FAIL_UNLESS(strcmp(character_set_results_4, "utf8") == 0, "cs_result != utf8");
  FAIL_UNLESS(strcmp(collation_connnection_4, "utf8_general_ci") == 0, "collation_connection != utf8_general_ci");

  /* That's it. Cleanup. */

  return OK;
}

static int test_bug_54100(MYSQL *mysql)
{
  MYSQL_RES *result;
  MYSQL_ROW row;
  int rc;

  rc= mysql_query(mysql, "SHOW CHARACTER SET");
  check_mysql_rc(rc, mysql);
  
  result= mysql_store_result(mysql);

  while ((row= mysql_fetch_row(result)))
  {
    /* ignore ucs2 */
    if (strcmp(row[0], "ucs2") && strcmp(row[0], "utf16le") && strcmp(row[0], "utf8mb4") && 
        strcmp(row[0], "utf16") && strcmp(row[0], "utf32")) {
      rc= mysql_set_character_set(mysql, row[0]);
      check_mysql_rc(rc, mysql);
    }
  }
  mysql_free_result(result);

  return OK;
}


/* We need this internal function for the test */

static int test_utf16_utf32_noboms(MYSQL *mysql __attribute__((unused)))
{
  const char *csname[]= {"utf16", "utf16le", "utf32", "utf8"};
  MARIADB_CHARSET_INFO  *csinfo[sizeof(csname)/sizeof(char*)];

  const int     UTF8= sizeof(csname)/sizeof(char*) - 1;

  unsigned char in_string[][8]= {"\xd8\x02\xdc\x60\0",      /* utf16(be) */
                                 "\x02\xd8\x60\xdc\0",      /* utf16le   */
                                 "\x00\x01\x08\x60\0\0\0",  /* utf32(be) */
                                 "\xF0\x90\xA1\xA0" };      /* utf8      */
  size_t       in_oct_len[]= {6, 6, 8, 5};

  char   buffer[8], as_hex[16];
  int    i, error;
  size_t rc, in_len, out_len;

  for (i= 0; i < (int)(sizeof(csname)/sizeof(char*)); ++i)
  {
    csinfo[i]= mariadb_get_charset_by_name(csname[i]);

    if (csinfo[i] == NULL)
    {
      diag("Could not get cs info for %s", csname[i]);
      return FAIL;
    }
  }

  for (i= 0; i < UTF8; ++i)
  {
    in_len=  in_oct_len[i];
    out_len= sizeof(buffer);

    diag("Converting %s->%s", csname[i], csname[UTF8]);
    rc= mariadb_convert_string((char *)in_string[i], &in_len, csinfo[i], buffer, &out_len, csinfo[UTF8], &error);

    FAIL_IF(rc == (size_t)-1, "Conversion failed");
    FAIL_IF(rc != in_oct_len[UTF8], "Incorrect number of written bytes");

    if (memcmp(buffer, in_string[UTF8], rc) != 0)
    {
      mysql_hex_string(as_hex, buffer, (unsigned long)rc);
      diag("Converted string(%s) does not match the expected one", as_hex);
      return FAIL;
    }

    in_len=  in_oct_len[UTF8];
    out_len= sizeof(buffer);

    diag("Converting %s->%s", csname[UTF8], csname[i]);
    rc= mariadb_convert_string((char *)in_string[UTF8], &in_len, csinfo[UTF8], buffer, &out_len, csinfo[i], &error);

    FAIL_IF(rc == (size_t)-1, "Conversion failed");
    diag("rc=%lu oct_len: %lu", (unsigned long)rc, (unsigned long)in_oct_len[i]);
    FAIL_IF(rc != in_oct_len[i], "Incorrect number of written bytes");

    if (memcmp(buffer, in_string[i], rc) != 0)
    {
      mysql_hex_string(as_hex, buffer, (unsigned long)rc);
      diag("Converted string(%s) does not match the expected one", as_hex);
      return FAIL;
    }
  }

  return OK;
}

static int charset_auto(MYSQL *my __attribute__((unused)))
{
  const char *csname1, *csname2;
  const char *osname;
  MYSQL *mysql= mysql_init(NULL);
  int rc;

  osname= madb_get_os_character_set();

  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "auto");

  FAIL_IF(!my_test_connect(mysql, hostname, username,
                             password, schema, port, socketname, 0), 
         mysql_error(mysql));

  csname1= mysql_character_set_name(mysql);
  diag("Character set: %s os charset: %s", csname1, osname);

  FAIL_IF(strcmp(osname, csname1), "character set is not os character set");

  if (strcmp(osname, "utf8"))
  {
    rc= mysql_set_character_set(mysql, "utf8");
    check_mysql_rc(rc, mysql);

    csname2= mysql_character_set_name(mysql);
    diag("Character set: %s", csname2);

    FAIL_IF(!strcmp(csname2, csname1), "Wrong charset: expected utf8");

    rc= mysql_set_character_set(mysql, "auto");
    check_mysql_rc(rc, mysql);

    csname2= mysql_character_set_name(mysql);
    diag("Character set: %s", csname2);
    FAIL_IF(strcmp(csname2, osname), "Wrong charset: expected os charset");
  }
  mysql_close(mysql);
  return OK;
}

/* check if all server character sets are supported */
static int test_conc223(MYSQL *mysql)
{
  int rc;
  MYSQL_RES *res;
  MYSQL_ROW row;
  int found= 0;

  rc= mysql_query(mysql, "SELECT ID, CHARACTER_SET_NAME, COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS");
  check_mysql_rc(rc, mysql);

  res= mysql_store_result(mysql);
  while ((row = mysql_fetch_row(res)))
  {
    int id= atoi(row[0]);
    if (!mariadb_get_charset_by_nr(id))
    {
      diag("%04d %s %s", id, row[1], row[2]);
      found++;
    }
  }
  mysql_free_result(res);
  if (found)
  {
    diag("%d character sets/collations not found", found);
    return FAIL;
  }
  return OK;
}

struct my_tests_st my_tests[] = {
  {"test_conc223", test_conc223, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"charset_auto", charset_auto, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"bug_8378: mysql_real_escape with gbk", bug_8378, TEST_CONNECTION_NEW, 0,  opt_bug8378,  NULL},
  {"test_client_character_set", test_client_character_set, TEST_CONNECTION_DEFAULT, 0,  NULL,  NULL},
  {"bug_10214: mysql_real_escape with NO_BACKSLASH_ESCAPES", bug_10214, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_escaping", test_escaping, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL}, 
  {"test_conversion", test_conversion, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"bug_41785", bug_41785, TEST_CONNECTION_DEFAULT, 0,  NULL, "not fixed yet"},
  {"test_bug27876", test_bug27876, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_bug30472", test_bug30472, TEST_CONNECTION_NEW, 0,  NULL, NULL},
  {"test_ps_i18n", test_ps_i18n, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {"test_bug_54100", test_bug_54100, TEST_CONNECTION_NEW, 0, NULL, NULL}, 
  {"test_utf16_utf32_noboms", test_utf16_utf32_noboms, TEST_CONNECTION_DEFAULT, 0,  NULL, NULL},
  {NULL, NULL, 0, 0, NULL, 0}
};


int main(int argc, char **argv)
{
  if (argc > 1)
   get_options(argc, argv);

  get_envvars();

  run_tests(my_tests);

  return(exit_status());
}
