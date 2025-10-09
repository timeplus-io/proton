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
#include <ma_global.h>
#include <ma_sys.h>
#include <mysql.h>
#include <tap.h>
#include "ma_getopt.h"
#include <memory.h>
#include <string.h>
#include <errmsg.h>
#include <stdlib.h>
#include <ma_server_error.h>
#include <mysql/client_plugin.h>

#ifndef WIN32
#include <pthread.h>
#else
#include <io.h>
#endif

#ifndef OK
# define OK 0
#endif
#ifndef FAIL
# define FAIL 1
#endif
#ifndef SKIP
# define SKIP -1
#endif
#ifndef FALSE
# define FALSE 0
#endif
#ifndef TRUE
# define TRUE 1
#endif

#define MAX_KEY MAX_INDEXES
#define MAX_KEY_LENGTH_DECIMAL_WIDTH 4          /* strlen("4096") */

#define SL(s) (s), (unsigned long)strlen((s))
#define SL_BIN(s) (s), (unsigned long)sizeof((s))

#define MAX_TEST_QUERY_LENGTH 300 /* MAX QUERY BUFFER LENGTH */

/* prevent warnings on Win64 by using STMT_LEN instead of strlen */
#define STMT_LEN(A) (unsigned long)strlen((A))

#define check_mysql_rc(rc, mysql) \
if (rc)\
{\
  diag("Error (%d): %s (%d) in %s line %d", rc, mysql_error(mysql), \
       mysql_errno(mysql), __FILE__, __LINE__);\
  return(FAIL);\
}

#define check_stmt_rc(rc, stmt) \
if (rc)\
{\
  diag("Error: %s (%s: %d)", mysql_stmt_error(stmt), __FILE__, __LINE__);\
  return(FAIL);\
}

#define FAIL_IF(expr, reason)\
if (expr)\
{\
  diag("Error: %s (%s: %d)", (reason) ? reason : "", __FILE__, __LINE__);\
  return FAIL;\
}

#define FAIL_UNLESS(expr, reason)\
if (!(expr))\
{\
  diag("Error: %s (%s: %d)", reason, __FILE__, __LINE__);\
  return FAIL;\
}

#define SKIP_CONNECTION_HANDLER \
 if (hostname && strstr(hostname, "://"))\
  {\
    diag("Test skipped (connection handler)");\
    return SKIP;\
  } 

/* connection options */
#define TEST_CONNECTION_DEFAULT    1 /* default connection */
#define TEST_CONNECTION_NONE       2 /* tests creates own connection */
#define TEST_CONNECTION_NEW        4 /* create a separate connection */
#define TEST_CONNECTION_DONT_CLOSE 8 /* don't close connection */

struct my_option_st
{
  enum mysql_option option;
  char              *value;
};

struct my_tests_st
{
  const char *name;
  int  (*function)(MYSQL *);
  int   connection;
  ulong connect_flags;
  struct my_option_st *options;
  const char *skipmsg;
};

MYSQL *my_test_connect(MYSQL *mysql,
                       const char *host,
                       const char *user,
                       const char *passwd,
                       const char *db,
                       unsigned int port,
                       const char *unix_socket,
                       unsigned long clientflag);

static const char *schema = 0;
static char *hostname = 0;
static char *password = 0;
static unsigned int port = 0;
static char *socketname = 0;
static char *username = 0;
static int force_tls= 0;
static uchar is_mariadb= 0;
static char *this_host= 0;
static char *plugindir= 0;
static unsigned char travis_test= 0;
/*
static struct my_option test_options[] =
{
  {"schema", 'd', "database to use", (uchar **) &schema, (uchar **) &schema,
   0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"help", '?', "Display this help and exit", 0, 0, 0, GET_NO_ARG, NO_ARG, 0,
   0, 0, 0, 0, 0},
  {"host", 'h', "Connect to host", (uchar **) &hostname, (uchar **) &hostname,
   0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"password", 'p',
   "Password to use when connecting to server.", (uchar **) &password, (uchar **) &password,
   0, GET_STR, OPT_ARG, 0, 0, 0, 0, 0, 0},
  {"port", 'P', "Port number to use for connection or 0 for default to, in "
   "order of preference, my.cnf, $MYSQL_TCP_PORT, "
#if MYSQL_PORT_DEFAULT == 0
   "/etc/services, "
#endif
   "built-in default (" STRINGIFY_ARG(MYSQL_PORT) ").",
   (uchar **) &port,
   (uchar **) &port, 0, GET_UINT, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"socket", 'S', "Socket file to use for connection",
   (uchar **) &socketname, (uchar **) &socketname, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"user", 'u', "User for login if not current user", (uchar **) &username,
   (uchar **) &username, 0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  { 0, 0, 0, 0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0}
};
*/
#define verify_prepare_field(result,no,name,org_name,type,table,\
                             org_table,db,length,def) \
          do_verify_prepare_field((result),(no),(name),(org_name),(type), \
                                  (table),(org_table),(db),(length),(def), \
                                  __FILE__, __LINE__)

int do_verify_prepare_field(MYSQL_RES *result,
                            unsigned int no, const char *name,
                            const char *org_name,
                            enum enum_field_types type __attribute__((unused)),
                            const char *table,
                            const char *org_table, const char *db,
                            unsigned long length __attribute__((unused)), 
                            const char *def __attribute__((unused)),
                            const char *file __attribute__((unused)),
                            int line __attribute__((unused)))
{
  MYSQL_FIELD *field;
/*  MARIADB_CHARSET_INFO *cs; */

  FAIL_IF(!(field= mysql_fetch_field_direct(result, no)), "FAILED to get result");
/*  cs= mysql_find_charset_nr(field->charsetnr);
  FAIL_UNLESS(cs, "Couldn't get character set"); */
  FAIL_UNLESS(strcmp(field->name, name) == 0, "field->name differs");
  FAIL_UNLESS(strcmp(field->org_name, org_name) == 0, "field->org_name differs");
/*
  if ((expected_field_length= length * cs->mbmaxlen) > UINT_MAX32)
    expected_field_length= UINT_MAX32;
*/
  /*
    XXX: silent column specification change works based on number of
    bytes a column occupies. So CHAR -> VARCHAR upgrade is possible even
    for CHAR(2) column if its character set is multibyte.
    VARCHAR -> CHAR downgrade won't work for VARCHAR(3) as one would
    expect.
  */
//  if (cs->char_maxlen == 1)
//    FAIL_UNLESS(field->type == type, "field->type differs");
  if (table)
    FAIL_UNLESS(strcmp(field->table, table) == 0, "field->table differs");
  if (org_table)
    FAIL_UNLESS(strcmp(field->org_table, org_table) == 0, "field->org_table differs");
  if (strcmp(field->db,db))
    diag("%s / %s", field->db, db);
  FAIL_UNLESS(strcmp(field->db, db) == 0, "field->db differs");
  /*
    Character set should be taken into account for multibyte encodings, such
    as utf8. Field length is calculated as number of characters * maximum
    number of bytes a character can occupy.
  */

  return OK;
}

void get_this_host(MYSQL *mysql)
{
  MYSQL_RES *res;
  MYSQL_ROW row;

  if (mysql_query(mysql, "select substr(current_user(), locate('@', current_user())+1)"))
    return;

  if ((res= mysql_store_result(mysql)))
  {
    if ((row= mysql_fetch_row(res)))
      this_host= strdup(row[0]);
    mysql_free_result(res);
  }
}

/* Prepare statement, execute, and process result set for given query */

int my_stmt_result(MYSQL *mysql, const char *buff)
{
  MYSQL_STMT *stmt;
  int        row_count= 0;
  int        rc;

  stmt= mysql_stmt_init(mysql);

  rc= mysql_stmt_prepare(stmt, buff, (unsigned long)strlen(buff));
  FAIL_IF(rc, mysql_stmt_error(stmt));

  rc= mysql_stmt_execute(stmt);
  FAIL_IF(rc, mysql_stmt_error(stmt));

  while (mysql_stmt_fetch(stmt) != MYSQL_NO_DATA)
    row_count++;

  mysql_stmt_close(stmt);

  return row_count;
}
/*
static my_bool
get_one_option(int optid, const struct my_option *opt __attribute__((unused)),
               char *argument)
{
  switch (optid) {
  case '?':
  case 'I':                           
    my_print_help(test_options);
    exit(0);
    break;
  }
  return 0;
}
*/
/* Utility function to verify a particular column data */

int verify_col_data(MYSQL *mysql, const char *table, const char *col,
                            const char *exp_data)
{
  static char query[MAX_TEST_QUERY_LENGTH];
  MYSQL_RES *result;
  MYSQL_ROW row;
  int       rc;

  if (table && col)
  {
    sprintf(query, "SELECT %s FROM %s LIMIT 1", col, table);
    rc= mysql_query(mysql, query);
    check_mysql_rc(rc, mysql);
  }
  result= mysql_use_result(mysql);
  FAIL_IF(!result, "Invalid result set");

  if (!(row= mysql_fetch_row(result)) || !row[0]) {
    diag("Failed to get the result");
    goto error;
  }
  if(strcmp(row[0], exp_data)) {
    diag("Expected %s, got %s", exp_data, row[0]);
    goto error;
  }
  mysql_free_result(result);

  return OK;

error:
  mysql_free_result(result);
  return FAIL;
}

my_bool query_int_variable(MYSQL *con, const char *var_name, int *var_value)
{
  MYSQL_RES *rs;
  MYSQL_ROW row;

  char query_buffer[MAX_TEST_QUERY_LENGTH];

  my_bool is_null;

  sprintf(query_buffer,
          "SELECT %s",
          (const char *) var_name);

  FAIL_IF(mysql_query(con, query_buffer), "Query failed");
  FAIL_UNLESS(rs= mysql_store_result(con), "Invaliid result set");
  FAIL_UNLESS(row= mysql_fetch_row(rs), "Nothing to fetch");

  is_null= row[0] == NULL;

  if (!is_null)
    *var_value= atoi(row[0]);

  mysql_free_result(rs);

  return is_null;
}

static void usage()
{
  printf("Execute test with the following options:\n");
  printf("-h hostname\n");
  printf("-u username\n");
  printf("-p password\n");
  printf("-d database\n");
  printf("-S socketname\n");
  printf("-t force use of TLS\n");
  printf("-P port number\n");
  printf("?  displays this help and exits\n");
}

void get_options(int argc, char **argv)
{
  int c= 0;

  while ((c=getopt(argc,argv, "h:u:p:d:w:P:S:t:?")) >= 0)
  {
    switch(c) {
    case 'h':
      hostname= optarg;
      break;
    case 'u':
      username= optarg;
      break;
    case 'p':
      password= optarg;
      break;
    case 'd':
      schema= optarg;
      break;
    case 'P':
      port= atoi(optarg);
      break;
    case 'S':
      socketname= optarg;
      break;
    case 't':
      force_tls= 1;
      break;
    case '?':
      usage();
      exit(0);
      break;
    default:
      usage();
      BAIL_OUT("Unknown option %c\n", c);
    }
  }
}


int check_variable(MYSQL *mysql, const char *variable, const char *value)
{
  char query[MAX_TEST_QUERY_LENGTH];
  MYSQL_RES *result;
  MYSQL_ROW row;

  sprintf(query, "SELECT %s", variable);
  result= mysql_store_result(mysql);
  if (!result)
    return FAIL;

  if ((row = mysql_fetch_row(result)))
    if (strcmp(row[0], value) == 0) {
      mysql_free_result(result);
      return OK;
    }
  mysql_free_result(result);
  return FAIL;
}

/* 
 * function *test_connect
 *
 * returns a new connection. This function will be called, if the test doesn't
 * use default_connection.
 */
MYSQL *test_connect(struct my_tests_st *test)
{
  MYSQL *mysql;
  int i= 0;
  int timeout= 10;
  my_bool truncation_report= 1;
  if (!(mysql = mysql_init(NULL))) {
    BAIL_OUT("Not enough memory available - mysql_init failed");
  }
  mysql_options(mysql, MYSQL_REPORT_DATA_TRUNCATION, &truncation_report);
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

  /* option handling */
  if (test && test->options) {

    while (test->options[i].option)
    {
      if (mysql_options(mysql, test->options[i].option, test->options[i].value)) {
        diag("Couldn't set option %d. Error (%d) %s", test->options[i].option,
                      mysql_errno(mysql), mysql_error(mysql));
        mysql_close(mysql);
        return(NULL);
      }
      i++;
    }
  }
  if (!(my_test_connect(mysql, hostname, username, password,
                           schema, port, socketname, (test) ? test->connect_flags:0)))
  {
    diag("Couldn't establish connection to server %s. Error (%d): %s", 
                   hostname, mysql_errno(mysql), mysql_error(mysql));
    mysql_close(mysql);
    return(NULL);
  }
  return(mysql);
}

static int reset_connection(MYSQL *mysql) {
  int rc;

  if (is_mariadb)
    rc= mysql_change_user(mysql, username, password, schema);
  else
    rc= mysql_reset_connection(mysql);
  check_mysql_rc(rc, mysql);
  rc= mysql_query(mysql, "SET sql_mode=''");
  check_mysql_rc(rc, mysql);

  return OK;
}

/*
 * function get_envvars((
 *
 * checks for connection related environment variables
 */
void get_envvars() {
  char  *envvar;

  if (getenv("MYSQL_TEST_TRAVIS"))
    travis_test= 1;

  if (!hostname && (envvar= getenv("MYSQL_TEST_HOST")))
    hostname= envvar;
  if (!username)
  {
    if ((envvar= getenv("MYSQL_TEST_USER")))
      username= envvar;
    else
      username= (char *)"root";
  }
  if (!password && (envvar= getenv("MYSQL_TEST_PASSWD")))
    password= envvar;
  if (!schema && (envvar= getenv("MYSQL_TEST_DB")))
    schema= envvar;
  if (!schema)
    schema= "test";
  if (!port)
  {
    if ((envvar= getenv("MYSQL_TEST_PORT")))
      port= atoi(envvar);
    else if ((envvar= getenv("MASTER_MYPORT")))
      port= atoi(envvar);
    diag("port: %d", port);
  }
  if (!force_tls && (envvar= getenv("MYSQL_TEST_TLS")))
    force_tls= atoi(envvar);
  if (!socketname)
  {
    if ((envvar= getenv("MYSQL_TEST_SOCKET")))
      socketname= envvar;
    else if ((envvar= getenv("MASTER_MYSOCK")))
      socketname= envvar;
    diag("socketname: %s", socketname);
  }
  if ((envvar= getenv("MYSQL_TEST_PLUGINDIR")))
    plugindir= envvar;
}

MYSQL *my_test_connect(MYSQL *mysql,
                       const char *host,
                       const char *user,
                       const char *passwd,
                       const char *db,
                       unsigned int port,
                       const char *unix_socket,
                       unsigned long clientflag)
{
  if (force_tls)
    mysql_options(mysql, MYSQL_OPT_SSL_ENFORCE, &force_tls); 
  if (!mysql_real_connect(mysql, host, user, passwd, db, port, unix_socket, clientflag))
  {
    diag("error: %s", mysql_error(mysql));
    return NULL;
  }

  if (mysql && force_tls && !mysql_get_ssl_cipher(mysql))
  {
    diag("Error: TLS connection not established");
    return NULL;
  }
  if (!this_host)
    get_this_host(mysql);
  return mysql;
}


void run_tests(struct my_tests_st *test) {
  int i, rc, total=0;
  MYSQL *mysql, *mysql_default= NULL;  /* default connection */

  while (test[total].function)
    total++;
  plan(total);

  if ((mysql_default= test_connect(NULL)))
  {
    diag("Testing against MySQL Server %s", mysql_get_server_info(mysql_default));
    diag("Host: %s", mysql_get_host_info(mysql_default));
    diag("Client library: %s", mysql_get_client_info());
    is_mariadb= mariadb_connection(mysql_default);
  }
  else
  {
    BAIL_OUT("Can't connect to a server. Aborting....");
  }

  for (i=0; i < total; i++) {
    if (!mysql_default && (test[i].connection & TEST_CONNECTION_DEFAULT))
    {
      diag("MySQL server not running");
      skip(1, "%s", test[i].name);
    } else if (!test[i].skipmsg) {
      mysql= mysql_default;
      if (test[i].connection & TEST_CONNECTION_NEW)
        mysql= test_connect(&test[i]);
      if (test[i].connection & TEST_CONNECTION_NONE)
        mysql= NULL;

      /* run test */
      rc= test[i].function(mysql);

      if (rc == SKIP)
        skip(1, "%s", test[i].name);
      else
        ok(rc == OK, "%s", test[i].name);

      /* if test failed, close and reopen default connection to prevent
         errors for further tests */
      if ((rc == FAIL || mysql_errno(mysql_default)) && (test[i].connection & TEST_CONNECTION_DEFAULT)) {
        mysql_close(mysql_default);
        mysql_default= test_connect(&test[i]);
      }
      /* clear connection: reset default connection or close extra connection */
      else if (mysql_default && (test[i].connection & TEST_CONNECTION_DEFAULT))  {
          if (reset_connection(mysql))
            return; /* default doesn't work anymore */
      }
      else if (mysql && !(test[i].connection & TEST_CONNECTION_DONT_CLOSE))
      {
          mysql_close(mysql);
      }
    } else {
      skip(1, "%s", test[i].skipmsg);
    }
  }
  if (this_host)
    free(this_host);

  if (mysql_default) {
    diag("close default");
    mysql_close(mysql_default);
  }
}

