/************************************************************************************
  Copyright (C) 2015-2018 MariaDB Corporation AB

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

  Part of this code includes code from the PHP project which
  is freely available from http://www.php.net
 *************************************************************************************/

/* MariaDB Connection plugin for Aurora failover  */

#include <ma_global.h>
#include <ma_sys.h>
#include <errmsg.h>
#include <ma_common.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <ma_string.h>

#ifndef WIN32
#include <sys/time.h>
#endif

/* function prototypes */
int aurora_init(char *errormsg __attribute__((unused)),
    size_t errormsg_size __attribute__((unused)),
    int unused  __attribute__((unused)), 
    va_list unused1 __attribute__((unused)));

MYSQL *aurora_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd,
    const char *db, unsigned int port, const char *unix_socket, unsigned long clientflag);
void aurora_close(MYSQL *mysql);
int aurora_command(MYSQL *mysql,enum enum_server_command command, const char *arg,
    size_t length, my_bool skipp_check, void *opt_arg);
my_bool aurora_reconnect(MYSQL *mysql);

#define AURORA_MAX_INSTANCES 16

#define AURORA_UNKNOWN -1
#define AURORA_PRIMARY 0
#define AURORA_REPLICA 1
#define AURORA_UNAVAILABLE 2

static struct st_mariadb_api *libmariadb_api= NULL;

#ifndef PLUGIN_DYNAMIC
MARIADB_CONNECTION_PLUGIN aurora_client_plugin =
#else
MARIADB_CONNECTION_PLUGIN _mysql_client_plugin_declaration_ =
#endif
{
  MARIADB_CLIENT_CONNECTION_PLUGIN,
  MARIADB_CLIENT_CONNECTION_PLUGIN_INTERFACE_VERSION,
  "aurora",
  "Georg Richter",
  "MariaDB connection plugin for Aurora failover",
  {1, 0, 0},
  "LGPL",
  NULL,
  aurora_init,
  NULL,
  NULL,
  aurora_connect,
  aurora_close,
  NULL,
  aurora_command,
  aurora_reconnect,
  NULL
};


typedef struct st_aurora_instance {
  char *host;
  unsigned int  port;
  time_t blacklisted;
  int type;
} AURORA_INSTANCE;

typedef struct st_conn_aurora {
  MYSQL *mysql[2],
        save_mysql;
  char *url;
  unsigned int num_instances;
  AURORA_INSTANCE instance[AURORA_MAX_INSTANCES];
  char *username, *password, *database;
  unsigned int port;
  unsigned long client_flag;
  char primary_id[100];
} AURORA;

#define AURORA_BLACKLIST_TIMEOUT 150

#define AURORA_IS_BLACKLISTED(a, i) \
  ((time(NULL) - (a)->instance[(i)].blacklisted) < AURORA_BLACKLIST_TIMEOUT)

/* {{{ my_bool aurora_swutch_connection */
my_bool aurora_switch_connection(MYSQL *mysql, AURORA *aurora, int type)
{
  switch (type)
  {
    case AURORA_REPLICA:
      if (aurora->mysql[AURORA_REPLICA])
      {
        *mysql= *aurora->mysql[AURORA_REPLICA];
      }
      break;
    case AURORA_PRIMARY:
      if (aurora->mysql[AURORA_PRIMARY])
      {
        *mysql= *aurora->mysql[AURORA_PRIMARY];
      }
      break;
    default:
      return 1;
  }
  return 0;
}
/* }}} */

/* {{{ int aurora_init
 *
 *     plugin initialization function
 */
int aurora_init(char *errormsg __attribute__((unused)),
    size_t errormsg_size __attribute__((unused)),
    int unused  __attribute__((unused)), 
    va_list unused1 __attribute__((unused)))
{
  /* random generator initialization */
#ifndef WIN32
  struct timeval tp;
  gettimeofday(&tp,NULL);
  srand(tp.tv_usec / 1000 + tp.tv_sec * 1000);
#else
  srand(GetTickCount());
#endif
  return 0;
}
/* }}} */

/* {{{ void aurora_close_memory */
void aurora_close_memory(AURORA *aurora)
{
  free(aurora->url);
  free(aurora->username);
  free(aurora->password);
  free(aurora->database);
  free(aurora);
}
/* }}} */

/* {{{ my_bool aurora_parse_url 
 * 
 *    parse url
 *   Url has the following format:
 *   instance1:port, instance2:port, .., instanceN:port
 *
 */
my_bool aurora_parse_url(const char *url, AURORA *aurora)
{
  char *p, *c;
  unsigned int i;

  if (!url || url[0] == 0)
    return 1;

  memset(aurora->instance, 0, (AURORA_MAX_INSTANCES + 1) * sizeof(char *));
  memset(&aurora->port, 0, (AURORA_MAX_INSTANCES + 1) * sizeof(int));

  if (aurora->url)
    free(aurora->url);

  aurora->url= strdup(url);
  c= aurora->url;

  /* get instances */ 
  while((c))
  {
    if ((p= strchr(c, ',')))
    {
      *p= '\0';
      p++;
    }
    if (*c)
    {
      aurora->instance[aurora->num_instances].host= c;
      aurora->num_instances++;
    }
    c= p;
  }

  if (!aurora->num_instances)
    return 0;

  /* check ports */
  for (i=0; i < aurora->num_instances && aurora->instance[i].host; i++)
  { 
    aurora->instance[i].type= AURORA_UNKNOWN;

    /* We need to be aware of IPv6 addresses: According to RFC3986 sect. 3.2.2
       hostnames have to be enclosed in square brackets if a port is given */
    if (aurora->instance[i].host[0]== '[' && 
        strchr(aurora->instance[i].host, ':') && 
        (p= strchr(aurora->instance[i].host,']')))
    {
      /* ignore first square bracket */
      memmove(aurora->instance[i].host, 
          aurora->instance[i].host+1, 
          strlen(aurora->instance[i].host) - 1);
      p= strchr(aurora->instance[i].host,']');
      *p= 0;
      p++;
    }
    else
      p= aurora->instance[i].host;
    if (p && (p= strchr(p, ':')))
    {
      *p= '\0';
      p++;
      aurora->instance[i].port= atoi(p);
    }
  }
  return 0;
}
/* }}} */

/* {{{ int aurora_get_instance_type 
 *
 *  RETURNS:
 *
 *    AURORA_PRIMARY
 *    AURORA_REPLICA
 *    -1 on error
 */
int aurora_get_instance_type(MYSQL *mysql)
{
  int rc= -1;
  MA_CONNECTION_HANDLER *save_hdlr= mysql->extension->conn_hdlr;

  const char *query= "select variable_value from information_schema.global_variables where variable_name='INNODB_READ_ONLY' AND variable_value='OFF'";

  if (!mysql)
    return -1;

  mysql->extension->conn_hdlr= 0;
  if (!libmariadb_api->mysql_query(mysql, query))
  {
    MYSQL_RES *res= libmariadb_api->mysql_store_result(mysql);
    rc= libmariadb_api->mysql_num_rows(res) ? AURORA_PRIMARY : AURORA_REPLICA;
    libmariadb_api->mysql_free_result(res);
  }
  mysql->extension->conn_hdlr= save_hdlr;
  return rc;
}
/* }}} */

/* {{{ my_bool aurora_get_primary_id 
 *
 *   try to find primary instance from slave by retrieving
 *   primary_id information_schema.replica_host_status information
 *
 *   If the function succeeds, primary_id will be copied into
 *   aurora->primary_id
 *
 *   Returns:
 *     1 on success
 *     0 if an error occurred or primary_id couldn't be 
 *       found
 */
my_bool aurora_get_primary_id(MYSQL *mysql, AURORA *aurora)
{
  my_bool rc= 0;
  MA_CONNECTION_HANDLER *save_hdlr= mysql->extension->conn_hdlr;

  mysql->extension->conn_hdlr= 0;
  if (!libmariadb_api->mysql_query(mysql, "select server_id from information_schema.replica_host_status "
        "where session_id = 'MASTER_SESSION_ID'"))
  {
    MYSQL_RES *res;
    MYSQL_ROW row;

    if ((res= libmariadb_api->mysql_store_result(mysql)))
    {
      if ((row= libmariadb_api->mysql_fetch_row(res)))
      {
        if (row[0])
        {
          strcpy(aurora->primary_id, row[0]);
          rc= 1;
        }
      }
      libmariadb_api->mysql_free_result(res);
    }
  }
  mysql->extension->conn_hdlr= save_hdlr;
  return rc;
}
/* }}} */

/* {{{ unsigned int aurora_get_valid_instances 
 *
 *     returns the number of instances which are
 *     not blacklisted or don't have a type assigned.
 */
static unsigned int aurora_get_valid_instances(AURORA *aurora, AURORA_INSTANCE **instances)
{
  unsigned int i, valid_instances= 0;

  memset(instances, 0, sizeof(AURORA_INSTANCE *) * AURORA_MAX_INSTANCES);

  for (i=0; i < aurora->num_instances; i++)
  {
    if (aurora->instance[i].type != AURORA_UNAVAILABLE)
    {
      if (aurora->instance[i].type == AURORA_PRIMARY && aurora->mysql[AURORA_PRIMARY])
        continue;
      instances[valid_instances]= &aurora->instance[i];
      valid_instances++;
    }
  }
  return valid_instances;
}
/* }}} */

/* {{{ void aurora_refresh_blacklist() */
void aurora_refresh_blacklist(AURORA *aurora)
{
  unsigned int i;
  for (i=0; i < aurora->num_instances; i++)
  {
    if (aurora->instance[i].blacklisted &&
        !(AURORA_IS_BLACKLISTED(aurora, i)))
    {
      aurora->instance[i].blacklisted= 0;
      aurora->instance[i].type= AURORA_UNKNOWN;
    }
  }
}
/* }}} */

/* {{{ MYSQL *aurora_connect_instance() */
MYSQL *aurora_connect_instance(AURORA *aurora, AURORA_INSTANCE *instance, MYSQL *mysql)
{
  if (!libmariadb_api->mysql_real_connect(mysql,
        instance->host,
        aurora->username,
        aurora->password,
        aurora->database,
        instance->port ? instance->port : aurora->port,
        NULL,
        aurora->client_flag | CLIENT_REMEMBER_OPTIONS))
  {
    /* connection not available */
    instance->blacklisted= time(NULL);
    instance->type= AURORA_UNAVAILABLE;
    return NULL;
  }

  /* check if we are slave or master */
  switch (aurora_get_instance_type(mysql))
  {
    case AURORA_PRIMARY:
      instance->type= AURORA_PRIMARY;
      return mysql;
      break;
    case AURORA_REPLICA:
      instance->type= AURORA_REPLICA;
      break;
    default:
      instance->type= AURORA_UNAVAILABLE;
      instance->blacklisted= time(NULL);
      return NULL;
  }
  if (!aurora->primary_id[0])
    if (aurora_get_primary_id(mysql, aurora))
      return NULL;
  return mysql;
}
/* }}} */

/* {{{ void aurora_close_internal */
void aurora_close_internal(MYSQL *mysql)
{
  if (mysql)
  {
    mysql->extension->conn_hdlr= 0;
    memset(&mysql->options, 0, sizeof(struct st_mysql_options));
    libmariadb_api->mysql_close(mysql);
  }
}
/* }}} */

/* {{{ my_bool aurora_find_replica() */
my_bool aurora_find_replica(AURORA *aurora)
{
  int valid_instances;
  my_bool replica_found= 0;
  AURORA_INSTANCE *instance[AURORA_MAX_INSTANCES];
  MYSQL *mysql;

  if (aurora->num_instances < 2)
    return 0;


  valid_instances= aurora_get_valid_instances(aurora, instance);

  while (valid_instances && !replica_found)
  {
    int random_pick= rand() % valid_instances;
    mysql= libmariadb_api->mysql_init(NULL);
    mysql->options= aurora->save_mysql.options;

    /* don't execute init_command on slave */
//    mysql->extension->conn_hdlr= aurora->save_mysql.extension->conn_hdlr;
    if ((aurora_connect_instance(aurora, instance[random_pick], mysql)))
    {
      switch (instance[random_pick]->type) {
        case AURORA_REPLICA:
          if (!aurora->mysql[AURORA_REPLICA])
            aurora->mysql[AURORA_REPLICA]= mysql;
          return 1;
          break;
        case AURORA_PRIMARY:
          if (!aurora->mysql[AURORA_PRIMARY])
            aurora->mysql[AURORA_PRIMARY]= mysql;
          else
            aurora_close_internal(mysql);
          continue;
          break;
        default:
          aurora_close_internal(mysql);
          return 0;
          break;
      }
    }
    else
      aurora_close_internal(mysql);
    valid_instances= aurora_get_valid_instances(aurora, instance);
  }
  return 0;
}
/* }}} */

/* {{{ AURORA_INSTANCE aurora_get_primary_id_instance() */
AURORA_INSTANCE *aurora_get_primary_id_instance(AURORA *aurora)
{
  unsigned int i;

  if (!aurora->primary_id[0])
    return 0;

  for (i=0; i < aurora->num_instances; i++)
  {
    if (!strncmp(aurora->instance[i].host, aurora->primary_id, strlen(aurora->primary_id)))
      return &aurora->instance[i];
  }
  return NULL;
}
/* }}} */

/* {{{ my_bool aurora_find_primary() */
my_bool aurora_find_primary(AURORA *aurora)
{
  unsigned int i;
  AURORA_INSTANCE *instance= NULL;
  MYSQL *mysql;
  my_bool check_primary= 1;

  /* We try to find a primary:
   * by looking 1st if a replica connect provided primary_id already
   * by walking through instances */

  if (!aurora->num_instances)
    return 0;

  for (i=0; i < aurora->num_instances; i++)
  {
    mysql= libmariadb_api->mysql_init(NULL);
    mysql->options= aurora->save_mysql.options;

    if (check_primary && aurora->primary_id[0])
    {
      if ((instance= aurora_get_primary_id_instance(aurora)) &&
          aurora_connect_instance(aurora, instance, mysql) &&
          instance->type == AURORA_PRIMARY)
      {
        aurora->primary_id[0]= 0;
        aurora->mysql[AURORA_PRIMARY]= mysql;
        return 1;
      }
      /* primary id connect failed, don't try again */
      aurora->primary_id[0]= 0;
      check_primary= 0;
    }
    else if (aurora->instance[i].type != AURORA_UNAVAILABLE)
    {
      if (aurora_connect_instance(aurora, &aurora->instance[i], mysql)
          && aurora->instance[i].type == AURORA_PRIMARY)
      {
        aurora->mysql[AURORA_PRIMARY]= mysql;
        return 1;
      }
    }
    aurora_close_internal(mysql);
  }
  return 0;
}
/* }}} */

/* {{{ MYSQL *aurora_connect */
MYSQL *aurora_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd,
    const char *db, unsigned int port, const char *unix_socket __attribute__((unused)), unsigned long client_flag)
{
  AURORA *aurora= NULL;
  MA_CONNECTION_HANDLER *save_hdlr= mysql->extension->conn_hdlr;

  if (!libmariadb_api)
    libmariadb_api= mysql->methods->api;

  /* we call aurora_connect either from mysql_real_connect or from mysql_reconnect,
   * so make sure in case of reconnect we don't allocate aurora twice */
  if (!(aurora= (AURORA *)save_hdlr->data))
  {
    if (!(aurora= (AURORA *)calloc(1, sizeof(AURORA))))
    {
      mysql->methods->set_error(mysql, CR_OUT_OF_MEMORY, "HY000", 0);
      return NULL;
    }
    aurora->save_mysql= *mysql;

    save_hdlr->data= (void *)aurora;

    if (aurora_parse_url(host, aurora))
    {
      goto error;
    }

    /* store login credentials for connect/reconnect */
    if (user)
      aurora->username= strdup(user);
    if (passwd)
      aurora->password= strdup(passwd);
    if (db)
      aurora->database= strdup(db);
    aurora->port= port;
    aurora->client_flag= client_flag;
  }

  /* we look for replica first:
     if it's a primary we don't need to call find_aurora_primary
     if it's a replica we can obtain primary_id */
  if (!aurora->mysql[AURORA_REPLICA])
  {
    if (!aurora_find_replica(aurora))
      aurora->mysql[AURORA_REPLICA]= NULL;
    else
      aurora->mysql[AURORA_REPLICA]->extension->conn_hdlr= save_hdlr;
  }

  if (!aurora->mysql[AURORA_PRIMARY])
  {
    if (!aurora_find_primary(aurora))
      aurora->mysql[AURORA_PRIMARY]= NULL;
    else
      aurora->mysql[AURORA_PRIMARY]->extension->conn_hdlr= save_hdlr;
  }

  if (!aurora->mysql[AURORA_PRIMARY] && !aurora->mysql[AURORA_REPLICA])
    goto error;

  if (aurora->mysql[AURORA_PRIMARY])
    aurora_switch_connection(mysql, aurora, AURORA_PRIMARY);
  else
    aurora_switch_connection(mysql, aurora, AURORA_REPLICA);
  mysql->extension->conn_hdlr= save_hdlr;
  return mysql;
error:
  aurora_close_memory(aurora);
  return NULL;
}
/* }}} */

/* {{{ my_bool aurora_reconnect */
my_bool aurora_reconnect(MYSQL *mysql)
{
  AURORA *aurora;
  MA_CONNECTION_HANDLER *save_hdlr= mysql->extension->conn_hdlr;
  unsigned int i;

  /* We can't determine if a new primary was promotoed, or if
   * line just dropped - we will close both primary and replica
   * connection and establish a new connection via
   * aurora_connect */

  aurora= (AURORA *)save_hdlr->data;

  /* removed blacklisted instances */
  for (i=0; i < aurora->num_instances; i++)
    aurora->instance[i].type= AURORA_UNKNOWN;

  if (aurora->mysql[AURORA_PRIMARY]->thread_id == mysql->thread_id)
  {
    /* don't send COM_QUIT */
    aurora->mysql[AURORA_PRIMARY]->net.pvio= NULL;
    aurora_close_internal(aurora->mysql[AURORA_PRIMARY]);
    aurora->mysql[AURORA_PRIMARY]= NULL;
    aurora_close_internal(aurora->mysql[AURORA_REPLICA]);
    aurora->mysql[AURORA_REPLICA]= NULL;
  }
  else if (aurora->mysql[AURORA_REPLICA]->thread_id == mysql->thread_id)
  {
    /* don't send COM_QUIT */
    aurora->mysql[AURORA_REPLICA]->net.pvio= NULL;
    aurora_close_internal(aurora->mysql[AURORA_REPLICA]);
    aurora->mysql[AURORA_REPLICA]= NULL;
    aurora_close_internal(aurora->mysql[AURORA_PRIMARY]);
    aurora->mysql[AURORA_PRIMARY]= NULL;
  }

  /* unset connections, so we can connect to primary and replica again */
  aurora->mysql[AURORA_PRIMARY]= aurora->mysql[AURORA_REPLICA]= NULL;

  if (aurora_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL, 0))
  {
    if (aurora->mysql[AURORA_PRIMARY])
      *mysql= *aurora->mysql[AURORA_PRIMARY];
    return 0;
  }
  if (aurora->mysql[AURORA_REPLICA])
    *mysql= *aurora->mysql[AURORA_REPLICA];
  else
    *mysql= aurora->save_mysql;
  return 1;
}
/* }}} */

/* {{{  void aurora_close */
void aurora_close(MYSQL *mysql)
{
  MA_CONNECTION_HANDLER *hdlr= mysql->extension->conn_hdlr;
  AURORA *aurora;
  int i;

  if (!hdlr || !hdlr->data)
    return;
  
  aurora= (AURORA *)hdlr->data;
  *mysql= aurora->save_mysql;

  if (!aurora->mysql[AURORA_PRIMARY] && !aurora->mysql[AURORA_REPLICA])
    goto end;

  for (i=0; i < 2; i++)
  {
    if (aurora->mysql[i])
    {
      /* Make sure that connection wasn't closed before, e.g. after disconnect */
      if (mysql->thread_id == aurora->mysql[i]->thread_id && !mysql->net.pvio)
        aurora->mysql[i]->net.pvio= 0;

      aurora_close_internal(aurora->mysql[i]);
      aurora->mysql[i]= NULL;
    }
  }
  /* free information  */
end:
  aurora_close_memory(aurora);
  mysql->extension->conn_hdlr= hdlr;
}
/* }}} */

/* {{{ my_bool is_replica_command */
my_bool is_replica_command(const char *buffer, size_t buffer_len)
{
  const char *buffer_end= buffer + buffer_len;

  for (; buffer < buffer_end; ++buffer)
  {
    char c;
    if (isalpha(c=*buffer))
    {
      if (tolower(c) == 's')
        return 1;
      return 0;
    }
  }
  return 0;
}
/* }}} */

/* {{{ my_bool is_replica_stmt */
my_bool is_replica_stmt(MYSQL *mysql, const char *buffer)
{
  unsigned long stmt_id= uint4korr(buffer);
  LIST *stmt_list= mysql->stmts;

  for (; stmt_list; stmt_list= stmt_list->next)
  {
    MYSQL_STMT *stmt= (MYSQL_STMT *)stmt_list->data;
    if (stmt->stmt_id == stmt_id)
      return 1;
  }
  return 0;
}
/* }}} */

/* {{{ int aurora_command */
int aurora_command(MYSQL *mysql,enum enum_server_command command, const char *arg,
    size_t length __attribute__((unused)), my_bool skipp_check __attribute__((unused)), void *opt_arg __attribute__((unused)))
{
  MA_CONNECTION_HANDLER *save_hdlr= mysql->extension->conn_hdlr;
  AURORA *aurora= (AURORA *)save_hdlr->data;

  /* if we don't have slave or slave became unavailable root traffic to master */
  if (!aurora->mysql[AURORA_REPLICA] || !OPT_EXT_VAL(mysql, read_only))
  {
    if (command != COM_INIT_DB)
    {
      aurora_switch_connection(mysql, aurora, AURORA_PRIMARY);
      goto end;
    }
  }

  switch(command) {
    case COM_INIT_DB:
      /* we need to change default database on primary and replica */
      if (aurora->mysql[AURORA_REPLICA] && mysql->thread_id == aurora->mysql[AURORA_PRIMARY]->thread_id)
      {
        aurora->mysql[AURORA_REPLICA]->extension->conn_hdlr= 0;
        libmariadb_api->mysql_select_db(aurora->mysql[AURORA_REPLICA], arg);
        aurora->mysql[AURORA_REPLICA]->extension->conn_hdlr= mysql->extension->conn_hdlr;
      }
      break;
    case COM_QUERY:
    case COM_STMT_PREPARE:
      if (aurora->mysql[AURORA_REPLICA])
        aurora_switch_connection(mysql, aurora, AURORA_REPLICA);
      break;
    case COM_STMT_EXECUTE:
    case COM_STMT_FETCH:
      if (aurora->mysql[AURORA_REPLICA] && aurora->mysql[AURORA_REPLICA]->stmts && 
          is_replica_stmt(aurora->mysql[AURORA_REPLICA], arg))
      {
        aurora_switch_connection(mysql, aurora, AURORA_REPLICA);
      }
      else
      {
        aurora_switch_connection(mysql, aurora, AURORA_PRIMARY);
      }
      break;
    default:
      aurora_switch_connection(mysql, aurora, AURORA_PRIMARY);
      break; 
  }
end:
  mysql->extension->conn_hdlr= save_hdlr;
  return 0;
}
/* }}} */
