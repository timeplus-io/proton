/*
 * Copyright 2013 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <bson/bson.h>
#include <mongoc/mongoc.h>
#include <mongoc/mongoc-host-list-private.h>

#include "mongoc/mongoc-server-description.h"
#include "mongoc/mongoc-server-description-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-linux-distro-scanner-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

#if defined(_MSC_VER) && defined(_WIN64)
#include <errhandlingapi.h>

// warning C4091: 'typedef ': ignored on left of '' when no variable is declared
#pragma warning(push)
#pragma warning(disable : 4091)
#include <DbgHelp.h>
#pragma warning(pop)
#endif

#ifdef MONGOC_ENABLE_SSL_OPENSSL
#include "mongoc/mongoc-openssl-private.h"
#endif

typedef struct {
   mongoc_log_level_t level;
   char *msg;
} log_entry_t;

static bson_mutex_t captured_logs_mutex;
static mongoc_array_t captured_logs;
static bool capturing_logs;
#ifdef MONGOC_ENABLE_SSL
static mongoc_ssl_opt_t gSSLOptions;
#endif


static log_entry_t *
log_entry_create (mongoc_log_level_t level, const char *msg)
{
   log_entry_t *log_entry;

   log_entry = bson_malloc (sizeof (log_entry_t));
   log_entry->level = level;
   log_entry->msg = bson_strdup (msg);

   return log_entry;
}


static void
log_entry_destroy (log_entry_t *log_entry)
{
   bson_free (log_entry->msg);
   bson_free (log_entry);
}


void
capture_logs (bool capture)
{
   capturing_logs = capture;
   clear_captured_logs ();
}


void
clear_captured_logs (void)
{
   size_t i;
   log_entry_t *log_entry;

   bson_mutex_lock (&captured_logs_mutex);
   for (i = 0; i < captured_logs.len; i++) {
      log_entry = _mongoc_array_index (&captured_logs, log_entry_t *, i);
      log_entry_destroy (log_entry);
   }

   captured_logs.len = 0;
   bson_mutex_unlock (&captured_logs_mutex);
}


bool
has_captured_log (mongoc_log_level_t level, const char *msg)
{
   size_t i;
   log_entry_t *log_entry;

   bson_mutex_lock (&captured_logs_mutex);

   for (i = 0; i < captured_logs.len; i++) {
      log_entry = _mongoc_array_index (&captured_logs, log_entry_t *, i);
      if (level == log_entry->level && strstr (log_entry->msg, msg)) {
         bson_mutex_unlock (&captured_logs_mutex);
         return true;
      }
   }

   bson_mutex_unlock (&captured_logs_mutex);

   return false;
}


bool
has_captured_logs (void)
{
   bool ret;

   bson_mutex_lock (&captured_logs_mutex);
   ret = 0 != captured_logs.len;
   bson_mutex_unlock (&captured_logs_mutex);

   return ret;
}


void
assert_all_captured_logs_have_prefix (const char *prefix)
{
   size_t i;
   log_entry_t *log_entry;

   bson_mutex_lock (&captured_logs_mutex);

   for (i = 0; i < captured_logs.len; i++) {
      log_entry = _mongoc_array_index (&captured_logs, log_entry_t *, i);
      ASSERT_STARTSWITH (log_entry->msg, prefix);
   }

   bson_mutex_unlock (&captured_logs_mutex);
}


void
print_captured_logs (const char *prefix)
{
   size_t i;
   log_entry_t *log_entry;

   bson_mutex_lock (&captured_logs_mutex);
   for (i = 0; i < captured_logs.len; i++) {
      log_entry = _mongoc_array_index (&captured_logs, log_entry_t *, i);
      if (prefix) {
         fprintf (stderr, "%s%s %s\n", prefix, mongoc_log_level_str (log_entry->level), log_entry->msg);
      } else {
         fprintf (stderr, "%s %s\n", mongoc_log_level_str (log_entry->level), log_entry->msg);
      }
   }
   bson_mutex_unlock (&captured_logs_mutex);
}


#define DEFAULT_FUTURE_TIMEOUT_MS 10 * 1000

int64_t
get_future_timeout_ms (void)
{
   return test_framework_getenv_int64 ("MONGOC_TEST_FUTURE_TIMEOUT_MS", DEFAULT_FUTURE_TIMEOUT_MS);
}

static void
log_handler (mongoc_log_level_t log_level, const char *log_domain, const char *message, void *user_data)
{
   TestSuite *suite;
   log_entry_t *log_entry;

   suite = (TestSuite *) user_data;

   if (log_level <= MONGOC_LOG_LEVEL_INFO) {
      bson_mutex_lock (&captured_logs_mutex);
      if (capturing_logs) {
         log_entry = log_entry_create (log_level, message);
         _mongoc_array_append_val (&captured_logs, log_entry);
         bson_mutex_unlock (&captured_logs_mutex);
         return;
      }
      bson_mutex_unlock (&captured_logs_mutex);

      if (!suite->silent) {
         mongoc_log_default_handler (log_level, log_domain, message, NULL);
      }
   } else if (log_level == MONGOC_LOG_LEVEL_DEBUG && test_suite_debug_output ()) {
      mongoc_log_default_handler (log_level, log_domain, message, NULL);
   }
}


mongoc_database_t *
get_test_database (mongoc_client_t *client)
{
   return mongoc_client_get_database (client, "test");
}


char *
gen_collection_name (const char *str)
{
   return bson_strdup_printf ("%s_%u_%u", str, (uint32_t) bson_get_monotonic_time (), (uint32_t) gettestpid ());
}


mongoc_collection_t *
get_test_collection (mongoc_client_t *client, const char *prefix)
{
   ASSERT (client);
   mongoc_collection_t *ret;
   char *str;

   str = gen_collection_name (prefix);
   ret = mongoc_client_get_collection (client, "test", str);
   bson_free (str);

   return ret;
}


char *
test_framework_getenv (const char *name)
{
   return _mongoc_getenv (name);
}

char *
test_framework_getenv_required (const char *name)
{
   char *ret = _mongoc_getenv (name);
   if (!ret) {
      test_error ("Expected environment variable %s to be set", name);
   }
   return ret;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_getenv_bool --
 *
 *       Check if an environment variable is set.
 *
 * Returns:
 *       True if the variable is set, or set to "on", false if it is not set
 *       or set to "off".
 *
 * Side effects:
 *       Logs and aborts if there is another value like "yes" or "true".
 *
 *--------------------------------------------------------------------------
 */
bool
test_framework_getenv_bool (const char *name)
{
   char *value = test_framework_getenv (name);
   bool ret = false;

   if (value) {
      if (!strcasecmp (value, "off")) {
         ret = false;
      } else if (!strcasecmp (value, "") || !strcasecmp (value, "on")) {
         ret = true;
      } else {
         test_error ("Unrecognized value for %s: \"%s\". Use \"on\" or \"off\".", name, value);
      }
   }

   bson_free (value);
   return ret;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_getenv_int64 --
 *
 *       Get a number from an environment variable.
 *
 * Returns:
 *       The number, or default.
 *
 * Side effects:
 *       Logs and aborts if there is a non-numeric value.
 *
 *--------------------------------------------------------------------------
 */
int64_t
test_framework_getenv_int64 (const char *name, int64_t default_value)
{
   char *value = test_framework_getenv (name);
   char *endptr;
   int64_t ret;

   if (value) {
      errno = 0;
      ret = bson_ascii_strtoll (value, &endptr, 10);
      if (errno) {
         perror (bson_strdup_printf ("Parsing %s from environment", name));
         test_error ("Failed to parse %s as int64", name);
      }

      bson_free (value);
      return ret;
   }

   return default_value;
}


static char *
test_framework_get_unix_domain_socket_path (void)
{
   char *path = test_framework_getenv ("MONGOC_TEST_UNIX_DOMAIN_SOCKET");

   if (path) {
      return path;
   }

   return bson_strdup_printf ("/tmp/mongodb-%d.sock", test_framework_get_port ());
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_unix_domain_socket_path_escaped --
 *
 *       Get the path to Unix Domain Socket .sock of the test MongoDB server,
 *       URI-escaped ("/" is replaced with "%2F").
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_unix_domain_socket_path_escaped (void)
{
   char *path = test_framework_get_unix_domain_socket_path (), *c = path;
   bson_string_t *escaped = bson_string_new (NULL);

   /* Connection String Spec: "The host information cannot contain an unescaped
    * slash ("/"), if it does then an exception MUST be thrown informing users
    * that paths must be URL encoded."
    *
    * Even though the C Driver does not currently enforce the spec, let's pass
    * a correctly escaped URI.
    */
   do {
      if (*c == '/') {
         bson_string_append (escaped, "%2F");
      } else {
         bson_string_append_c (escaped, *c);
      }
   } while (*(++c));

   bson_string_append_c (escaped, '\0');
   bson_free (path);

   return bson_string_free (escaped, false /* free_segment */);
}

static char *
_uri_str_from_env (void)
{
   if (test_framework_getenv_bool ("MONGOC_TEST_LOADBALANCED")) {
      char *loadbalanced_uri_str = test_framework_getenv ("SINGLE_MONGOS_LB_URI");
      if (!loadbalanced_uri_str) {
         test_error ("SINGLE_MONGOS_LB_URI and MULTI_MONGOS_LB_URI must be set "
                     "when MONGOC_TEST_LOADBALANCED is enabled");
      }
      return loadbalanced_uri_str;
   }
   return test_framework_getenv ("MONGOC_TEST_URI");
}

static mongoc_uri_t *
_uri_from_env (void)
{
   char *env_uri_str;
   mongoc_uri_t *uri;

   env_uri_str = _uri_str_from_env ();
   if (env_uri_str) {
      uri = mongoc_uri_new (env_uri_str);
      bson_free (env_uri_str);
      return uri;
   }

   return NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_host --
 *
 *       Get the hostname of the test MongoDB server.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_host (void)
{
   mongoc_uri_t *env_uri;
   const mongoc_host_list_t *hosts;
   char *host;

   /* MONGOC_TEST_URI takes precedence */
   env_uri = _uri_from_env ();
   if (env_uri) {
      /* choose first host */
      hosts = mongoc_uri_get_hosts (env_uri);
      ASSERT_WITH_MSG (hosts, "could not obtain hosts from URI [%s]", mongoc_uri_get_string (env_uri));
      host = bson_strdup (hosts->host);
      mongoc_uri_destroy (env_uri);
      return host;
   }

   host = test_framework_getenv ("MONGOC_TEST_HOST");

   return host ? host : bson_strdup ("localhost");
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_port --
 *
 *       Get the port number of the test MongoDB server.
 *
 * Returns:
 *       The port number, 27017 by default.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
uint16_t
test_framework_get_port (void)
{
   mongoc_uri_t *env_uri;
   const mongoc_host_list_t *hosts;
   char *port_str;
   unsigned long port = MONGOC_DEFAULT_PORT;

   /* MONGOC_TEST_URI takes precedence */
   env_uri = _uri_from_env ();
   if (env_uri) {
      /* choose first port */
      hosts = mongoc_uri_get_hosts (env_uri);
      port = hosts->port;
      mongoc_uri_destroy (env_uri);
   } else {
      port_str = test_framework_getenv ("MONGOC_TEST_PORT");
      if (port_str && strlen (port_str)) {
         port = strtoul (port_str, NULL, 10);
         if (port == 0 || port > UINT16_MAX) {
            /* parse err or port out of range -- mongod prohibits port 0 */
            port = MONGOC_DEFAULT_PORT;
         }
      }

      bson_free (port_str);
   }

   return (uint16_t) port;
}

char *
test_framework_get_host_and_port (void)
{
   char *host = test_framework_get_host ();
   uint16_t port = test_framework_get_port ();
   char *host_and_port;
   if (strchr (host, ':')) {
      /* wrap IPv6 address in square brackets. */
      host_and_port = bson_strdup_printf ("[%s]:%hu", host, port);
   } else {
      host_and_port = bson_strdup_printf ("%s:%hu", host, port);
   }
   bson_free (host);
   return host_and_port;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_admin_user --
 *
 *       Get the username of an admin user on the test MongoDB server.
 *
 * Returns:
 *       A string you must bson_free, or NULL.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_admin_user (void)
{
   char *retval = NULL;
   mongoc_uri_t *env_uri = _uri_from_env ();

   /* MONGOC_TEST_URI takes precedence */
   if (env_uri) {
      const char *tmp = mongoc_uri_get_username (env_uri);

      if (tmp) {
         retval = bson_strdup (tmp);
      }
      mongoc_uri_destroy (env_uri);
   }
   if (!retval) {
      retval = test_framework_getenv ("MONGOC_TEST_USER");
   }

   return retval;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_admin_password --
 *
 *       Get the password of an admin user on the test MongoDB server.
 *
 * Returns:
 *       A string you must bson_free, or NULL.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_admin_password (void)
{
   char *retval = NULL;
   mongoc_uri_t *env_uri = _uri_from_env ();

   /* MONGOC_TEST_URI takes precedence */
   if (env_uri) {
      const char *tmp = mongoc_uri_get_password (env_uri);

      if (tmp) {
         retval = bson_strdup (tmp);
      }
      mongoc_uri_destroy (env_uri);
   }
   if (!retval) {
      retval = test_framework_getenv ("MONGOC_TEST_PASSWORD");
   }

   return retval;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_user_password --
 *
 *       Get the username and password of an admin user on the test MongoDB
 *       server.
 *
 * Returns:
 *       True if username and password environment variables are set.
 *
 * Side effects:
 *       Sets passed-in string pointers to strings you must free, or NULL.
 *       Logs and aborts if user or password is set in the environment
 *       but not both, or if user and password are set but SSL is not
 *       compiled in (SSL is required for SCRAM-SHA-1, see CDRIVER-520).
 *
 *--------------------------------------------------------------------------
 */
static bool
test_framework_get_user_password (char **user, char **password)
{
   /* TODO: uri-escape username and password */
   *user = test_framework_get_admin_user ();
   *password = test_framework_get_admin_password ();

   if ((*user && !*password) || (!*user && *password)) {
      test_error ("Specify both MONGOC_TEST_USER and"
                  " MONGOC_TEST_PASSWORD, or neither");
   }

#ifndef MONGOC_ENABLE_CRYPTO
   if (*user && *password) {
      test_error ("You need to configure with ENABLE_SSL"
                  " when providing user+password (for SCRAM-SHA-1)");
   }
#endif

   return *user != NULL;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_add_user_password --
 *
 *       Copy a connection string, with user and password added.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_add_user_password (const char *uri_str, const char *user, const char *password)
{
   return bson_strdup_printf ("mongodb://%s:%s@%s", user, password, uri_str + strlen ("mongodb://"));
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_add_user_password_from_env --
 *
 *       Add password of an admin user on the test MongoDB server.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       Same as test_framework_get_user_password.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_add_user_password_from_env (const char *uri_str)
{
   char *user;
   char *password;
   char *uri_str_auth;

   if (test_framework_get_user_password (&user, &password)) {
      uri_str_auth = test_framework_add_user_password (uri_str, user, password);

      bson_free (user);
      bson_free (password);
   } else {
      uri_str_auth = bson_strdup (uri_str);
   }

   return uri_str_auth;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_compressors --
 *
 *      Get the list of compressors to enable
 *
 * Returns:
 *       A string you must bson_free, or NULL if the variable is not set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_compressors (void)
{
   return test_framework_getenv ("MONGOC_TEST_COMPRESSORS");
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_has_compressors --
 *
 *      Check if the test suite has been configured to use compression
 *
 * Returns:
 *       true if compressors should be used.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
test_framework_has_compressors (void)
{
   bool retval;
   char *compressors = test_framework_get_compressors ();

   retval = !!compressors;
   bson_free (compressors);

   return retval;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_ssl --
 *
 *       Should we connect to the test MongoDB server over SSL?
 *
 * Returns:
 *       True if any MONGOC_TEST_SSL_* environment variables are set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
test_framework_get_ssl (void)
{
   char *ssl_option_names[] = {"MONGOC_TEST_SSL_PEM_FILE",
                               "MONGOC_TEST_SSL_PEM_PWD",
                               "MONGOC_TEST_SSL_CA_FILE",
                               "MONGOC_TEST_SSL_CA_DIR",
                               "MONGOC_TEST_SSL_CRL_FILE",
                               "MONGOC_TEST_SSL_WEAK_CERT_VALIDATION"};
   char *ssl_option_value;
   size_t i;

   for (i = 0; i < sizeof ssl_option_names / sizeof (char *); i++) {
      ssl_option_value = test_framework_getenv (ssl_option_names[i]);

      if (ssl_option_value) {
         bson_free (ssl_option_value);
         return true;
      }
   }

   return test_framework_getenv_bool ("MONGOC_TEST_SSL");
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_unix_domain_socket_uri_str --
 *
 *       Get the connection string (unix domain socket style) of the test
 *       MongoDB server based on the variables set in the environment.
 *       Does *not* call hello to discover your actual topology.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       Same as test_framework_get_user_password.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_unix_domain_socket_uri_str (void)
{
   char *path;
   char *test_uri_str;
   char *test_uri_str_auth;

   path = test_framework_get_unix_domain_socket_path_escaped ();
   test_uri_str = bson_strdup_printf ("mongodb://%s/%s", path, test_framework_get_ssl () ? "?ssl=true" : "");

   test_uri_str_auth = test_framework_add_user_password_from_env (test_uri_str);

   bson_free (path);
   bson_free (test_uri_str);

   return test_uri_str_auth;
}


/*
 *--------------------------------------------------------------------------
 *
 * call_hello_with_host_and_port --
 *
 *       Call hello or legacy hello on a server, possibly over SSL.
 *
 * Side effects:
 *       Fills reply with hello response. Logs and aborts on error.
 *
 *--------------------------------------------------------------------------
 */
static void
call_hello_with_host_and_port (const char *host_and_port, bson_t *reply)
{
   char *user;
   char *password;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   bson_error_t error;

   if (test_framework_get_user_password (&user, &password)) {
      uri_str = bson_strdup_printf (
         "mongodb://%s:%s@%s%s", user, password, host_and_port, test_framework_get_ssl () ? "/?ssl=true" : "");
      bson_free (user);
      bson_free (password);
   } else {
      uri_str = bson_strdup_printf ("mongodb://%s%s", host_and_port, test_framework_get_ssl () ? "/?ssl=true" : "");
   }

   uri = mongoc_uri_new (uri_str);
   BSON_ASSERT (uri);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 10000);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 10000);
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false);
   if (test_framework_has_compressors ()) {
      char *compressors = test_framework_get_compressors ();

      mongoc_uri_set_compressors (uri, compressors);
      bson_free (compressors);
   }

   if (test_framework_is_loadbalanced ()) {
      mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   }

   client = test_framework_client_new_from_uri (uri, NULL);

#ifdef MONGOC_ENABLE_SSL
   test_framework_set_ssl_opts (client);
#endif

   if (!mongoc_client_command_simple (client, "admin", tmp_bson ("{'hello': 1}"), NULL, reply, &error)) {
      bson_destroy (reply);

      if (!mongoc_client_command_simple (
             client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, reply, &error)) {
         test_error ("error calling legacy hello: '%s'\n"
                     "URI = %s",
                     error.message,
                     uri_str);
      }
   }

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
}

/*
 *--------------------------------------------------------------------------
 *
 * call_hello --
 *
 *       Call hello or legacy hello on the test server, possibly over SSL, using
 *       host and port from the environment.
 *
 * Side effects:
 *       Fills reply with hello response. Logs and aborts on error.
 *
 *--------------------------------------------------------------------------
 */
static void
call_hello (bson_t *reply)
{
   char *host_and_port = test_framework_get_host_and_port ();

   call_hello_with_host_and_port (host_and_port, reply);

   bson_free (host_and_port);
}


static char *
set_name (bson_t *hello_response)
{
   bson_iter_t iter;

   if (bson_iter_init_find (&iter, hello_response, "setName")) {
      return bson_strdup (bson_iter_utf8 (&iter, NULL));
   } else {
      return NULL;
   }
}


static bool
uri_str_has_db (bson_string_t *uri_string)
{
   BSON_ASSERT_PARAM (uri_string);

   const char *const str = uri_string->str;
   const char *const standard = "mongodb://";
   const char *const srv = "mongodb+srv://";

   const bool is_standard = strstr (str, standard) == str;
   const bool is_srv = strstr (str, srv) == str;

   ASSERT_WITH_MSG (is_standard || is_srv, "[%s] does not start with [%s] or [%s]", uri_string->str, standard, srv);

   if (is_standard) {
      return strchr (str + strlen (standard), '/') != NULL;
   } else {
      return strchr (str + strlen (srv), '/') != NULL;
   }
}


static void
add_option_to_uri_str (bson_string_t *uri_string, const char *option, const char *value)
{
   if (strchr (uri_string->str, '?')) {
      /* already has some options */
      bson_string_append_c (uri_string, '&');
   } else if (uri_str_has_db (uri_string)) {
      /* like "mongodb://host/db" */
      bson_string_append_c (uri_string, '?');
   } else {
      /* like "mongodb://host" */
      bson_string_append_printf (uri_string, "/?");
   }

   bson_string_append_printf (uri_string, "%s=%s", option, value);
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_uri_str_no_auth --
 *
 *       Get the connection string of the test MongoDB topology --
 *       standalone, replica set, mongos, or mongoses -- along with
 *       SSL options, but not username and password. Calls calls hello with
 *       that connection string to discover your topology, and
 *       returns an appropriate connection string for the topology
 *       type.
 *
 *       database_name is optional.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       Same as test_framework_get_user_password.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_uri_str_no_auth (const char *database_name)
{
   char *env_uri_str;
   bson_t hello_response;
   bson_string_t *uri_string;
   char *name;
   bson_iter_t iter;
   bson_iter_t hosts_iter;
   bool first;
   char *host;
   uint16_t port;

   env_uri_str = _uri_str_from_env ();
   if (env_uri_str) {
      uri_string = bson_string_new (env_uri_str);
      if (database_name) {
         if (uri_string->str[uri_string->len - 1] != '/') {
            bson_string_append (uri_string, "/");
         }
         bson_string_append (uri_string, database_name);
      }
      bson_free (env_uri_str);
   } else {
      /* construct a direct connection or replica set connection URI */
      call_hello (&hello_response);
      uri_string = bson_string_new ("mongodb://");

      if ((name = set_name (&hello_response))) {
         /* make a replica set URI */
         bson_iter_init_find (&iter, &hello_response, "hosts");
         bson_iter_recurse (&iter, &hosts_iter);
         first = true;

         /* append "host1,host2,host3" */
         while (bson_iter_next (&hosts_iter)) {
            BSON_ASSERT (BSON_ITER_HOLDS_UTF8 (&hosts_iter));
            if (!first) {
               bson_string_append (uri_string, ",");
            }

            bson_string_append (uri_string, bson_iter_utf8 (&hosts_iter, NULL));
            first = false;
         }

         bson_string_append (uri_string, "/");
         if (database_name) {
            bson_string_append (uri_string, database_name);
         }

         add_option_to_uri_str (uri_string, MONGOC_URI_REPLICASET, name);
         bson_free (name);
      } else {
         host = test_framework_get_host ();
         port = test_framework_get_port ();
         bson_string_append_printf (uri_string, "%s:%hu", host, port);
         bson_string_append (uri_string, "/");
         if (database_name) {
            bson_string_append (uri_string, database_name);
         }

         bson_free (host);
      }

      if (test_framework_get_ssl ()) {
         add_option_to_uri_str (uri_string, MONGOC_URI_SSL, "true");
      }

      bson_destroy (&hello_response);
   }

   if (test_framework_has_compressors ()) {
      char *compressors = test_framework_get_compressors ();

      add_option_to_uri_str (uri_string, MONGOC_URI_COMPRESSORS, compressors);
      bson_free (compressors);
   }

   // Required by test-atlas-executor. Not required by normal unified test
   // runner, but make tests a little more resilient to transient errors.
   add_option_to_uri_str (uri_string, MONGOC_URI_SERVERSELECTIONTRYONCE, "false");

   return bson_string_free (uri_string, false);
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_uri_str --
 *
 *       Get the connection string of the test MongoDB topology --
 *       standalone, replica set, mongos, or mongoses -- along with
 *       SSL options, username and password.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       Same as test_framework_get_user_password.
 *
 *--------------------------------------------------------------------------
 */
char *
test_framework_get_uri_str (void)
{
   char *uri_str_no_auth;
   char *uri_str;

   if (test_framework_getenv_bool ("MONGOC_TEST_ATLAS")) {
      // User and password is already embedded in URI.
      return test_framework_get_uri_str_no_auth (NULL);
   } else {
      /* no_auth also contains compressors. */
      uri_str_no_auth = test_framework_get_uri_str_no_auth (NULL);
      uri_str = test_framework_add_user_password_from_env (uri_str_no_auth);
   }

   bson_free (uri_str_no_auth);

   return uri_str;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_uri --
 *
 *       Like test_framework_get_uri_str (). Get the URI of the test
 *       MongoDB server.
 *
 * Returns:
 *       A mongoc_uri_t* you must destroy.
 *
 * Side effects:
 *       Same as test_framework_get_user_password.
 *
 *--------------------------------------------------------------------------
 */
mongoc_uri_t *
test_framework_get_uri (void)
{
   bson_error_t error = {0};

   char *test_uri_str = test_framework_get_uri_str ();
   mongoc_uri_t *uri = mongoc_uri_new_with_error (test_uri_str, &error);

   ASSERT_OR_PRINT (uri, error);
   bson_free (test_uri_str);

   return uri;
}

mongoc_uri_t *
test_framework_get_uri_multi_mongos_loadbalanced (void)
{
   char *uri_str_no_auth;
   char *uri_str;
   mongoc_uri_t *uri;
   bson_error_t error;

   uri_str_no_auth = _mongoc_getenv ("MULTI_MONGOS_LB_URI");
   if (!uri_str_no_auth) {
      test_error ("expected MULTI_MONGOS_LB_URI to be set");
   }
   uri_str = test_framework_add_user_password_from_env (uri_str_no_auth);
   uri = mongoc_uri_new_with_error (uri_str, &error);

   ASSERT_OR_PRINT (uri, error);

   bson_free (uri_str_no_auth);
   bson_free (uri_str);
   return uri;
}

bool
test_framework_uri_apply_multi_mongos (mongoc_uri_t *uri, bool use_multi, bson_error_t *error)
{
   bool ret = false;

   if (!test_framework_is_mongos ()) {
      ret = true;
      goto done;
   }

   /* TODO Once CDRIVER-3285 is resolved, update this to no longer hardcode the
    * hosts. */
   if (use_multi) {
      if (!mongoc_uri_upsert_host_and_port (uri, "localhost:27017", error)) {
         goto done;
      }
      if (!mongoc_uri_upsert_host_and_port (uri, "localhost:27018", error)) {
         goto done;
      }
   } else {
      const mongoc_host_list_t *hosts;

      hosts = mongoc_uri_get_hosts (uri);
      if (hosts->next) {
         test_set_error (error,
                         "useMultiMongoses is false, so expected single "
                         "host listed, but got: %s",
                         mongoc_uri_get_string (uri));
         goto done;
      }
   }

   ret = true;
done:
   return ret;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_mongos_count --
 *
 *       Returns the number of servers in the test framework's MongoDB URI.
 *
 *--------------------------------------------------------------------------
 */

size_t
test_framework_mongos_count (void)
{
   mongoc_uri_t *uri = test_framework_get_uri ();
   const mongoc_host_list_t *h;
   size_t count = 0;

   BSON_ASSERT (uri);
   h = mongoc_uri_get_hosts (uri);
   while (h) {
      ++count;
      h = h->next;
   }

   mongoc_uri_destroy (uri);

   return count;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_replset_name --
 *
 *       Returns the replica set name or NULL. You must free the string.
 *
 *--------------------------------------------------------------------------
 */

char *
test_framework_replset_name (void)
{
   bson_t reply;
   bson_iter_t iter;
   char *replset_name = NULL;

   call_hello (&reply);
   if (!bson_iter_init_find (&iter, &reply, "setName")) {
      goto cleanup;
   }

   replset_name = bson_strdup (bson_iter_utf8 (&iter, NULL));

cleanup:
   bson_destroy (&reply);
   return replset_name;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_replset_member_count --
 *
 *       Returns the number of replica set members (including arbiters).
 *
 *--------------------------------------------------------------------------
 */

size_t
test_framework_replset_member_count (void)
{
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;
   bool r;
   bson_iter_t iter, array;
   size_t count = 0;

   client = test_framework_new_default_client ();
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'replSetGetStatus': 1}"), NULL, &reply, &error);

   if (r) {
      if (bson_iter_init_find (&iter, &reply, "members") && BSON_ITER_HOLDS_ARRAY (&iter)) {
         bson_iter_recurse (&iter, &array);
         while (bson_iter_next (&array)) {
            ++count;
         }
      }
   } else if (!strstr (error.message, "not running with --replSet") &&
              !strstr (error.message, "replSetGetStatus is not supported through mongos")) {
      /* failed for some other reason */
      ASSERT_OR_PRINT (false, error);
   }

   bson_destroy (&reply);
   mongoc_client_destroy (client);

   return count;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_data_nodes_count --
 *
 *       Returns the number of replica set members (excluding arbiters),
 *       or number of mongos servers or 1 for a standalone.
 *
 *--------------------------------------------------------------------------
 */

size_t
test_framework_data_nodes_count (void)
{
   bson_t reply;
   bson_iter_t iter, array;
   size_t count = 0;


   call_hello (&reply);
   if (!bson_iter_init_find (&iter, &reply, "hosts")) {
      bson_destroy (&reply);
      return test_framework_mongos_count ();
   }

   BSON_ASSERT (bson_iter_recurse (&iter, &array));
   while (bson_iter_next (&array)) {
      ++count;
   }

   bson_destroy (&reply);

   return count;
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_server_count --
 *
 *       Returns the number of mongos servers or replica set members,
 *       or 1 if the server is standalone.
 *
 *--------------------------------------------------------------------------
 */
size_t
test_framework_server_count (void)
{
   size_t count = 0;

   count = test_framework_replset_member_count ();
   if (count > 0) {
      return count;
   }

   return test_framework_mongos_count ();
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_set_ssl_opts --
 *
 *       Configure a client to connect to the test MongoDB server.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Logs and aborts if any MONGOC_TEST_SSL_* environment variables are
 *       set but the driver is not built with SSL enabled.
 *
 *--------------------------------------------------------------------------
 */
void
test_framework_set_ssl_opts (mongoc_client_t *client)
{
   ASSERT (client);

   if (test_framework_get_ssl ()) {
#ifndef MONGOC_ENABLE_SSL
      test_error ("SSL test config variables are specified in the environment, but"
                  " SSL isn't enabled");
#else
      mongoc_client_set_ssl_opts (client, &gSSLOptions);
#endif
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_new_default_client --
 *
 *       Get a client connected to the test MongoDB topology.
 *
 * Returns:
 *       A client you must mongoc_client_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_t *
test_framework_new_default_client (void)
{
   char *test_uri_str = test_framework_get_uri_str ();
   mongoc_client_t *client = test_framework_client_new (test_uri_str, NULL);

   BSON_ASSERT (client);
   test_framework_set_ssl_opts (client);

   bson_free (test_uri_str);

   return client;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_client_new_no_server_api --
 *
 *       Get a client connected to the test MongoDB topology, with no server
 *       API version set.
 *
 * Returns:
 *       A client you must mongoc_client_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_t *
test_framework_client_new_no_server_api (void)
{
   mongoc_uri_t *uri = test_framework_get_uri ();
   mongoc_client_t *client = mongoc_client_new_from_uri (uri);

   BSON_ASSERT (client);
   test_framework_set_ssl_opts (client);

   mongoc_uri_destroy (uri);

   return client;
}


mongoc_server_api_t *
test_framework_get_default_server_api (void)
{
   char *api_version = test_framework_getenv ("MONGODB_API_VERSION");
   mongoc_server_api_version_t version;

   if (!api_version) {
      return NULL;
   }

   ASSERT (mongoc_server_api_version_from_string (api_version, &version));

   bson_free (api_version);

   return mongoc_server_api_new (version);
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_client_new --
 *
 *       Get a client connected to the indicated connection string
 *
 * Parameters:
 *       @uri_str: A connection string to the test deployment
 *       @api: A mongoc_server_api_t that declares an API version. If omitted,
 *             the API version indicated in the MONGODB_API_VERSION env variable
 *             is used.
 *
 * Returns:
 *       A client you must mongoc_client_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_t *
test_framework_client_new (const char *uri_str, const mongoc_server_api_t *api)
{
   mongoc_client_t *client = mongoc_client_new (uri_str);
   bson_error_t error;
   mongoc_server_api_t *default_api = NULL;

   if (!client) {
      return client;
   }

   if (api) {
      ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
   } else {
      default_api = test_framework_get_default_server_api ();
      if (default_api) {
         ASSERT_OR_PRINT (mongoc_client_set_server_api (client, default_api, &error), error);
      }
   }

   mongoc_server_api_destroy (default_api);

   return client;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_client_new_from_uri --
 *
 *       Get a client connected to the indicated URI
 *
 * Parameters:
 *       @uri_str: A mongoc_uri_t to connect with
 *       @api: A mongoc_server_api_t that declares an API version. If omitted,
 *             the API version indicated in the MONGODB_API_VERSION env variable
 *             is used.
 *
 * Returns:
 *       A client you must mongoc_client_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_t *
test_framework_client_new_from_uri (const mongoc_uri_t *uri, const mongoc_server_api_t *api)
{
   mongoc_client_t *client = mongoc_client_new_from_uri (uri);
   bson_error_t error;
   mongoc_server_api_t *default_api = NULL;

   if (!client) {
      return client;
   }

   if (api) {
      ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
   } else {
      default_api = test_framework_get_default_server_api ();
      if (default_api) {
         ASSERT_OR_PRINT (mongoc_client_set_server_api (client, default_api, &error), error);
      }
   }

   mongoc_server_api_destroy (default_api);

   return client;
}


#ifdef MONGOC_ENABLE_SSL
/*
 *--------------------------------------------------------------------------
 *
 * test_framework_get_ssl_opts --
 *
 *       Get options for connecting to mongod over SSL (even if mongod
 *       isn't actually SSL-enabled).
 *
 * Returns:
 *       A pointer to constant global SSL-test options.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
const mongoc_ssl_opt_t *
test_framework_get_ssl_opts (void)
{
   return &gSSLOptions;
}
#endif

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_set_pool_ssl_opts --
 *
 *       Configure a client pool to connect to the test MongoDB server.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Logs and aborts if any MONGOC_TEST_SSL_* environment variables are
 *       set but the driver is not built with SSL enabled.
 *
 *--------------------------------------------------------------------------
 */
void
test_framework_set_pool_ssl_opts (mongoc_client_pool_t *pool)
{
   BSON_ASSERT (pool);

   if (test_framework_get_ssl ()) {
#ifndef MONGOC_ENABLE_SSL
      test_error ("SSL test config variables are specified in the environment, but"
                  " SSL isn't enabled");
#else
      mongoc_client_pool_set_ssl_opts (pool, &gSSLOptions);
#endif
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * test_framework_new_default_client_pool --
 *
 *       Get a client pool connected to the test MongoDB topology.
 *
 * Returns:
 *       A pool you must destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_pool_t *
test_framework_new_default_client_pool (void)
{
   mongoc_uri_t *test_uri = test_framework_get_uri ();
   mongoc_client_pool_t *pool = test_framework_client_pool_new_from_uri (test_uri, NULL);

   BSON_ASSERT (pool);
   test_framework_set_pool_ssl_opts (pool);

   mongoc_uri_destroy (test_uri);
   BSON_ASSERT (pool);
   return pool;
}

/*
 *--------------------------------------------------------------------------
 *
 * test_framework_client_pool_new_from_uri --
 *
 *       Get a client pool connected to the indicated connection string
 *
 * Parameters:
 *       @uri_str: A mongoc_uri_t to connect to
 *       @api: A mongoc_server_api_t that declares an API version. If omitted,
 *             the API version indicated in the MONGODB_API_VERSION env variable
 *             is used.
 *
 * Returns:
 *       A pool you must mongoc_client_pool_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_pool_t *
test_framework_client_pool_new_from_uri (const mongoc_uri_t *uri, const mongoc_server_api_t *api)
{
   mongoc_client_pool_t *pool = mongoc_client_pool_new (uri);
   bson_error_t error;
   mongoc_server_api_t *default_api = NULL;

   if (!pool) {
      return pool;
   }

   if (api) {
      ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, api, &error), error);
   } else {
      default_api = test_framework_get_default_server_api ();
      if (default_api) {
         ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, default_api, &error), error);
      }
   }

   mongoc_server_api_destroy (default_api);

   return pool;
}

#ifdef MONGOC_ENABLE_SSL
static void
test_framework_global_ssl_opts_init (void)
{
   memcpy (&gSSLOptions, mongoc_ssl_opt_get_default (), sizeof gSSLOptions);

   gSSLOptions.pem_file = test_framework_getenv ("MONGOC_TEST_SSL_PEM_FILE");
   gSSLOptions.pem_pwd = test_framework_getenv ("MONGOC_TEST_SSL_PEM_PWD");
   gSSLOptions.ca_file = test_framework_getenv ("MONGOC_TEST_SSL_CA_FILE");
   gSSLOptions.ca_dir = test_framework_getenv ("MONGOC_TEST_SSL_CA_DIR");
   gSSLOptions.crl_file = test_framework_getenv ("MONGOC_TEST_SSL_CRL_FILE");
   gSSLOptions.weak_cert_validation = test_framework_getenv_bool ("MONGOC_TEST_SSL_WEAK_CERT_VALIDATION");
}

static void
test_framework_global_ssl_opts_cleanup (void)
{
   bson_free ((void *) gSSLOptions.pem_file);
   bson_free ((void *) gSSLOptions.pem_pwd);
   bson_free ((void *) gSSLOptions.ca_file);
   bson_free ((void *) gSSLOptions.ca_dir);
   bson_free ((void *) gSSLOptions.crl_file);
}
#endif


bool
test_framework_is_mongos (void)
{
   bson_t reply;
   bson_iter_t iter;
   bool is_mongos;

   call_hello (&reply);

   is_mongos = (bson_iter_init_find (&iter, &reply, "msg") && BSON_ITER_HOLDS_UTF8 (&iter) &&
                !strcasecmp (bson_iter_utf8 (&iter, NULL), "isdbgrid"));

   bson_destroy (&reply);

   return is_mongos;
}


bool
test_framework_is_replset (void)
{
   return test_framework_replset_member_count () > 0;
}

bool
test_framework_server_is_secondary (mongoc_client_t *client, uint32_t server_id)
{
   bson_t reply;
   bson_iter_t iter;
   mongoc_server_description_t const *sd;
   bson_error_t error;
   bool ret;

   ASSERT (client);

   sd = mongoc_topology_description_server_by_id_const (client->topology->_shared_descr_.ptr, server_id, &error);
   ASSERT_OR_PRINT (sd, error);

   call_hello_with_host_and_port (sd->host.host_and_port, &reply);

   ret = bson_iter_init_find (&iter, &reply, "secondary") && bson_iter_as_bool (&iter);

   bson_destroy (&reply);

   return ret;
}


bool
test_framework_clustertime_supported (void)
{
   bson_t reply;
   bool has_cluster_time;

   call_hello (&reply);
   has_cluster_time = bson_has_field (&reply, "$clusterTime");
   bson_destroy (&reply);

   return has_cluster_time;
}


int64_t
test_framework_session_timeout_minutes (void)
{
   bson_t reply;
   bson_iter_t iter;
   int64_t timeout = -1;

   if (!TestSuite_CheckLive ()) {
      return -1;
   }

   call_hello (&reply);
   if (bson_iter_init_find (&iter, &reply, "logicalSessionTimeoutMinutes")) {
      timeout = bson_iter_as_int64 (&iter);
   }

   bson_destroy (&reply);

   return timeout;
}


void
test_framework_get_max_wire_version (int64_t *max_version)
{
   bson_t reply;
   bson_iter_t iter;

   call_hello (&reply);
   BSON_ASSERT (bson_iter_init_find (&iter, &reply, "maxWireVersion"));
   *max_version = bson_iter_as_int64 (&iter);

   bson_destroy (&reply);
}

bool
test_framework_has_auth (void)
{
   char *user;

#ifndef MONGOC_ENABLE_SSL
   /* requires SSL for SCRAM implementation, can't test auth */
   return false;
#endif

   /* checks if the MONGOC_TEST_USER env var is set */
   user = test_framework_get_admin_user ();
   bson_free (user);
   if (user) {
      return true;
   } else {
      return false;
   }
}


int
test_framework_skip_if_auth (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   if (test_framework_has_auth ()) {
      return 0;
   }

   return 1;
}


int
test_framework_skip_if_no_auth (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   if (!test_framework_has_auth ()) {
      return 0;
   }

   return 1;
}

static bool
_test_framework_has_crypto (void)
{
#ifdef MONGOC_ENABLE_CRYPTO
   return true;
#else
   return false;
#endif
}

int
test_framework_skip_if_no_sessions (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   if (!_test_framework_has_crypto ()) {
      return 0;
   }

   return -1 != test_framework_session_timeout_minutes ();
}


int
test_framework_skip_if_no_cluster_time (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   return test_framework_clustertime_supported () ? 1 : 0;
}


int
test_framework_skip_if_crypto (void)
{
   return _test_framework_has_crypto () ? 0 : 1;
}


int
test_framework_skip_if_no_crypto (void)
{
   return test_framework_skip_if_crypto () ? 0 : 1;
}


int
test_framework_skip_if_offline (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_OFFLINE") ? 0 : 1;
}


int
test_framework_skip_if_slow (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_SKIP_SLOW") ? 0 : 1;
}


int
test_framework_skip_if_slow_or_live (void)
{
   return test_framework_skip_if_slow () && TestSuite_CheckLive ();
}


int
test_framework_skip_if_windows (void)
{
#ifdef _WIN32
   return 0;
#else
   return 1;
#endif
}


/* skip if no Unix domain socket */
int
test_framework_skip_if_no_uds (void)
{
#ifdef _WIN32
   return 0;
#else
   char *path;
   int ret;

   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   path = test_framework_get_unix_domain_socket_path ();
   ret = access (path, R_OK | W_OK) == 0 ? 1 : 0;

   bson_free (path);

   return ret;
#endif
}


int
test_framework_skip_if_no_txns (void)
{
   if (test_framework_skip_if_no_crypto () && test_framework_skip_if_no_sessions () &&
       test_framework_skip_if_not_replset () && test_framework_skip_if_max_wire_version_less_than_7 ()) {
      return 1;
   }

   if (test_framework_skip_if_no_crypto () && test_framework_skip_if_no_sessions () &&
       test_framework_skip_if_not_mongos () && test_framework_skip_if_max_wire_version_less_than_8 ()) {
      return 1;
   }

   /* transactions not supported, skip the test */
   return 0;
}


bool
test_framework_max_wire_version_at_least (int version)
{
   int64_t max_version;

   BSON_ASSERT (version > 0);
   test_framework_get_max_wire_version (&max_version);
   return max_version >= version;
}


int64_t
test_framework_max_write_batch_size (void)
{
   bson_t reply;
   bson_iter_t iter;
   int64_t size;

   call_hello (&reply);

   if (bson_iter_init_find (&iter, &reply, "maxWriteBatchSize")) {
      size = bson_iter_as_int64 (&iter);
   } else {
      size = 1000;
   }

   bson_destroy (&reply);

   return size;
}

#define N_SERVER_VERSION_PARTS 3

static server_version_t
_parse_server_version (const bson_t *buildinfo)
{
   bson_iter_t iter;
   bson_iter_t array_iter;
   int i;
   server_version_t ret = 0;

   ASSERT (bson_iter_init_find (&iter, buildinfo, "versionArray"));
   ASSERT (bson_iter_recurse (&iter, &array_iter));

   /* Server returns a 4-part version like [3, 2, 0, 0], or like [3, 2, 0, -49]
    * for an RC. Ignore the 4th part since RCs are equivalent to non-RCs for
    * testing purposes. */
   for (i = 0; i < N_SERVER_VERSION_PARTS && bson_iter_next (&array_iter); i++) {
      ret *= 1000;
      ret += 100 + bson_iter_as_int64 (&array_iter);
   }

   ASSERT_CMPINT (i, ==, N_SERVER_VERSION_PARTS);
   return ret;
}

server_version_t
test_framework_get_server_version_with_client (mongoc_client_t *client)
{
   bson_t reply;
   bson_error_t error;
   server_version_t ret = 0;

   ASSERT (client);

   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "admin", tmp_bson ("{'buildinfo': 1}"), NULL, &reply, &error),
                    error);

   ret = _parse_server_version (&reply);

   bson_destroy (&reply);

   return ret;
}

server_version_t
test_framework_get_server_version (void)
{
   mongoc_client_t *client;

   server_version_t ret = 0;

   client = test_framework_new_default_client ();

   ret = test_framework_get_server_version_with_client (client);

   mongoc_client_destroy (client);

   return ret;
}

server_version_t
test_framework_str_to_version (const char *version_str)
{
   char *str_copy;
   char *part;
   char *end;
   int i;
   server_version_t ret = 0;

   str_copy = bson_strdup (version_str);
   part = strtok (str_copy, ".");

   /* Versions can have 4 parts like "3.2.0.0", or like "3.2.0.-49" for an RC.
    * Ignore the 4th part since RCs are equivalent to non-RCs for testing
    * purposes. */
   for (i = 0; i < N_SERVER_VERSION_PARTS && part; i++) {
      ret *= 1000;
      ret += 100 + bson_ascii_strtoll (part, &end, 10);
      part = strtok (NULL, ".");
   }

   /* pad out a short version like "3.0" to three parts */
   for (; i < N_SERVER_VERSION_PARTS; i++) {
      ret *= 1000;
      ret += 100;
   }

   bson_free (str_copy);

   return ret;
}

/* self-tests for a test framework feature */
static void
test_version_cmp (void)
{
   server_version_t v2_6_12 = 102106112;
   server_version_t v3_0_0 = 103100100;
   server_version_t v3_0_1 = 103100101;
   server_version_t v3_0_10 = 103100110;
   server_version_t v3_2_0 = 103102100;

   ASSERT (v2_6_12 == test_framework_str_to_version ("2.6.12"));
   ASSERT (v2_6_12 == _parse_server_version (tmp_bson ("{'versionArray': [2, 6, 12, 0]}")));

   ASSERT (v3_0_0 == test_framework_str_to_version ("3"));
   ASSERT (v3_0_0 == _parse_server_version (tmp_bson ("{'versionArray': [3, 0, 0, 0]}")));

   ASSERT (v3_0_1 == test_framework_str_to_version ("3.0.1"));
   ASSERT (v3_0_1 == _parse_server_version (tmp_bson ("{'versionArray': [3, 0, 1, 0]}")));

   ASSERT (v3_0_10 == test_framework_str_to_version ("3.0.10"));
   ASSERT (v3_0_10 == _parse_server_version (tmp_bson ("{'versionArray': [3, 0, 10, 0]}")));

   /* release candidates should be equivalent to non-rcs. */
   ASSERT (v3_2_0 == test_framework_str_to_version ("3.2.0.-49"));
   ASSERT (v3_2_0 == _parse_server_version (tmp_bson ("{'versionArray': [3, 2, 0, -49]}")));

   ASSERT (v3_2_0 > test_framework_str_to_version ("3.1.9"));
   ASSERT (v3_2_0 == test_framework_str_to_version ("3.2"));
   ASSERT (v3_2_0 < test_framework_str_to_version ("3.2.1"));
}

int
test_framework_skip_if_single (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return (test_framework_is_mongos () || test_framework_is_replset ());
}

bool
test_framework_is_mongohouse (void)
{
   return test_framework_getenv_bool ("RUN_MONGOHOUSE_TESTS");
}

int
test_framework_skip_if_no_mongohouse (void)
{
   if (!test_framework_is_mongohouse ()) {
      return 0;
   }
   return 1;
}

int
test_framework_skip_if_mongos (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return test_framework_is_mongos () ? 0 : 1;
}

int
test_framework_skip_if_replset (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return test_framework_is_replset () ? 0 : 1;
}

int
test_framework_skip_if_not_single (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return !test_framework_skip_if_single ();
}

int
test_framework_skip_if_not_mongos (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return !test_framework_skip_if_mongos ();
}

int
test_framework_skip_if_not_replset (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return !test_framework_skip_if_replset ();
}

/* convenience skip functions based on the wire version. */
#define WIRE_VERSION_CHECKS(wv)                                                                       \
   int test_framework_skip_if_max_wire_version_more_than_##wv (void)                                  \
   {                                                                                                  \
      if (!TestSuite_CheckLive ()) {                                                                  \
         return 0;                                                                                    \
      }                                                                                               \
      return test_framework_max_wire_version_at_least (wv + 1) ? 0 : 1;                               \
   }                                                                                                  \
   int test_framework_skip_if_max_wire_version_less_than_##wv (void)                                  \
   {                                                                                                  \
      if (!TestSuite_CheckLive ()) {                                                                  \
         return 0;                                                                                    \
      }                                                                                               \
      return test_framework_max_wire_version_at_least (wv);                                           \
   }                                                                                                  \
   int test_framework_skip_if_not_rs_version_##wv (void)                                              \
   {                                                                                                  \
      if (!TestSuite_CheckLive ()) {                                                                  \
         return 0;                                                                                    \
      }                                                                                               \
      return (test_framework_max_wire_version_at_least (wv) && test_framework_is_replset ()) ? 1 : 0; \
   }                                                                                                  \
   int test_framework_skip_if_rs_version_##wv (void)                                                  \
   {                                                                                                  \
      if (!TestSuite_CheckLive ()) {                                                                  \
         return 0;                                                                                    \
      }                                                                                               \
      return (test_framework_max_wire_version_at_least (wv) && test_framework_is_replset ()) ? 0 : 1; \
   }

WIRE_VERSION_CHECKS (7)
WIRE_VERSION_CHECKS (8)
WIRE_VERSION_CHECKS (9)
/* wire versions 10, 11, 12 were internal to the 5.0 release cycle */
WIRE_VERSION_CHECKS (13)
/* wire version 14 begins with the 5.1 prerelease. */
WIRE_VERSION_CHECKS (14)
/* wire version 17 begins with the 6.0 release. */
WIRE_VERSION_CHECKS (17)
/* wire version 19 begins with the 6.2 release. */
WIRE_VERSION_CHECKS (19)
/* wire version 21 begins with the 7.0 release. */
WIRE_VERSION_CHECKS (21)
/* wire version 22 begins with the 7.1 release. */
WIRE_VERSION_CHECKS (22)
/* wire version 23 begins with the 7.2 release. */
WIRE_VERSION_CHECKS (23)
/* wire version 24 begins with the 7.3 release. */
WIRE_VERSION_CHECKS (24)
/* wire version 25 begins with the 8.0 release. */
WIRE_VERSION_CHECKS (25)

int
test_framework_skip_if_no_dual_ip_hostname (void)
{
   struct addrinfo hints = {0}, *res = NULL, *iter;
   int res_count = 0;
   char *host = test_framework_getenv ("MONGOC_TEST_IPV4_AND_IPV6_HOST");
   bool needs_free = false;
   if (host) {
      needs_free = true;
   } else {
      host = "localhost";
   }

   hints.ai_family = AF_UNSPEC;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_flags = 0;
   hints.ai_protocol = 0;

   BSON_ASSERT (getaddrinfo (host, "27017", &hints, &res) != -1);

   iter = res;

   while (iter) {
      res_count++;
      iter = iter->ai_next;
   }

   freeaddrinfo (res);
   if (needs_free) {
      bson_free (host);
   }

   ASSERT_CMPINT (res_count, >, 0);
   return res_count > 1;
}

int
test_framework_skip_if_no_compressors (void)
{
   char *compressors = test_framework_get_compressors ();
   bool ret = compressors != NULL;
   bson_free (compressors);
   return ret;
}

int
test_framework_skip_if_compressors (void)
{
   return !test_framework_skip_if_no_compressors ();
}

int
test_framework_skip_if_no_failpoint (void)
{
   mongoc_client_t *client;
   bool ret;
   bson_error_t error;

   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   ret = mongoc_client_command_simple (client,
                                       "admin",
                                       tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': 'off', 'data': "
                                                 "{'errorCode': 10107, 'failCommands': ['count']}}"),
                                       NULL,
                                       NULL,
                                       &error);
   mongoc_client_destroy (client);

   if (!ret) {
      return 0; /* do not proceed */
   }

   /* proceed. */
   return 1;
}

int
test_framework_skip_if_no_client_side_encryption (void)
{
   const char *required_env_vars[] = {"MONGOC_TEST_AWS_SECRET_ACCESS_KEY",
                                      "MONGOC_TEST_AWS_ACCESS_KEY_ID",
                                      "MONGOC_TEST_AWSNAME2_SECRET_ACCESS_KEY",
                                      "MONGOC_TEST_AWSNAME2_ACCESS_KEY_ID",
                                      "MONGOC_TEST_AZURE_TENANT_ID",
                                      "MONGOC_TEST_AZURE_CLIENT_ID",
                                      "MONGOC_TEST_AZURE_CLIENT_SECRET",
                                      "MONGOC_TEST_GCP_EMAIL",
                                      "MONGOC_TEST_GCP_PRIVATEKEY",
                                      "MONGOC_TEST_CSFLE_TLS_CA_FILE",
                                      "MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE",
                                      NULL};
   const char **iter;
   bool has_creds = true;

   for (iter = required_env_vars; *iter != NULL; iter++) {
      char *val;

      val = test_framework_getenv (*iter);
      if (!val) {
         MONGOC_DEBUG ("%s not defined", *iter);
         has_creds = false;
         break;
      }
      bson_free (val);
   }

   if (has_creds) {
#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION
      return 1; /* 1 == proceed. */
#else
      return 0; /* 0 == do not proceed. */
#endif
   }
   return 0; /* 0 == do not proceed. */
}

int
test_framework_skip_if_no_aws (void)
{
#ifdef MONGOC_ENABLE_MONGODB_AWS_AUTH
   return 1; /* proceed. */
#else
   return 0; /* do not proceed. */
#endif
}

/* test-libmongoc may not have permissions to set environment variables. */
int
test_framework_skip_if_no_setenv (void)
{
   char *value;
   if (!_mongoc_setenv ("MONGOC_TEST_CANARY", "VALUE")) {
      return 0; /* do not proceed. */
   }
   value = test_framework_getenv ("MONGOC_TEST_CANARY");
   if (!value || 0 != strcmp (value, "VALUE")) {
      return 0; /* do not proceed. */
   }
   bson_free (value);
   return 1;
}

bool
test_framework_is_serverless (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_IS_SERVERLESS");
}

int
test_framework_skip_if_serverless (void)
{
   if (test_framework_is_serverless ()) {
      return 0; // do not proceed
   }
   return 1; // proceed.
}

int
test_framework_skip_due_to_cdriver3708 (void)
{
   if (0 == test_framework_skip_if_auth () && 0 == test_framework_skip_if_replset () &&
       test_framework_get_server_version () > test_framework_str_to_version ("4.4.0")) {
      /* If auth is enabled, we're using a replica set, and using a > 4.4
       * server, skip test. */
      return 0;
   }
   return 1;
}

static char MONGOC_TEST_UNIQUE[32];

#if defined(_MSC_VER) && defined(_WIN64)
LONG WINAPI
windows_exception_handler (EXCEPTION_POINTERS *pExceptionInfo)
{
   HANDLE process = GetCurrentProcess ();

   fprintf (stderr, "entering windows exception handler\n");
   SymInitialize (process, NULL, TRUE);

   /* Shamelessly stolen from https://stackoverflow.com/a/28115589 */

   /* StackWalk64() may modify context record passed to it, so we will
    use a copy.
    */
   CONTEXT context_record = *pExceptionInfo->ContextRecord;
   DWORD exception_code = pExceptionInfo->ExceptionRecord->ExceptionCode;
   /* Initialize stack walking. */
   char exception_string[128];
   bson_snprintf (exception_string,
                  sizeof (exception_string),
                  (exception_code == EXCEPTION_ACCESS_VIOLATION) ? "(access violation)" : "0x%08X",
                  exception_code);

   char address_string[32];
   bson_snprintf (address_string, sizeof (address_string), "0x%p", pExceptionInfo->ExceptionRecord->ExceptionAddress);

   fprintf (stderr, "exception '%s' at '%s', terminating\n", exception_string, address_string);

   STACKFRAME64 stack_frame;
   memset (&stack_frame, 0, sizeof (stack_frame));
#if defined(_WIN64)
   int machine_type = IMAGE_FILE_MACHINE_AMD64;
   stack_frame.AddrPC.Offset = context_record.Rip;
   stack_frame.AddrFrame.Offset = context_record.Rbp;
   stack_frame.AddrStack.Offset = context_record.Rsp;
#else
   int machine_type = IMAGE_FILE_MACHINE_I386;
   stack_frame.AddrPC.Offset = context_record.Eip;
   stack_frame.AddrFrame.Offset = context_record.Ebp;
   stack_frame.AddrStack.Offset = context_record.Esp;
#endif
   stack_frame.AddrPC.Mode = AddrModeFlat;
   stack_frame.AddrFrame.Mode = AddrModeFlat;
   stack_frame.AddrStack.Mode = AddrModeFlat;

   SYMBOL_INFO *symbol;
   symbol = bson_malloc0 (sizeof (SYMBOL_INFO) + 256);
   symbol->MaxNameLen = 255;
   symbol->SizeOfStruct = sizeof (SYMBOL_INFO);

   fprintf (stderr, "begin stack trace\n");
   while (StackWalk64 (machine_type,
                       GetCurrentProcess (),
                       GetCurrentThread (),
                       &stack_frame,
                       &context_record,
                       NULL,
                       &SymFunctionTableAccess64,
                       &SymGetModuleBase64,
                       NULL)) {
      DWORD64 displacement = 0;

      if (SymFromAddr (process, (DWORD64) stack_frame.AddrPC.Offset, &displacement, symbol)) {
         IMAGEHLP_MODULE64 moduleInfo;
         memset (&moduleInfo, 0, sizeof (moduleInfo));
         moduleInfo.SizeOfStruct = sizeof (moduleInfo);

         if (SymGetModuleInfo64 (process, symbol->ModBase, &moduleInfo))
            fprintf (stderr, "%s : ", moduleInfo.ModuleName);

         fprintf (stderr, "%s", symbol->Name);
      }

      IMAGEHLP_LINE line;
      line.SizeOfStruct = sizeof (IMAGEHLP_LINE);

      DWORD offset_ln = 0;
      if (SymGetLineFromAddr (process, (DWORD64) stack_frame.AddrPC.Offset, &offset_ln, &line)) {
         fprintf (stderr, " %s:%d ", line.FileName, line.LineNumber);
      }

      fprintf (stderr, "\n");
   }
   fprintf (stderr, "end stack trace\n");
   fflush (stderr);
   return EXCEPTION_EXECUTE_HANDLER;
}
#endif


void
test_libmongoc_init (TestSuite *suite, BSON_MAYBE_UNUSED const char *name, int argc, char **argv)
{
#if defined(_MSC_VER) && defined(_WIN64)
   SetUnhandledExceptionFilter (windows_exception_handler);
#endif
   mongoc_init ();

   bson_snprintf (
      MONGOC_TEST_UNIQUE, sizeof MONGOC_TEST_UNIQUE, "test_%u_%u", (unsigned) time (NULL), (unsigned) gettestpid ());

   bson_mutex_init (&captured_logs_mutex);
   _mongoc_array_init (&captured_logs, sizeof (log_entry_t *));
   mongoc_log_set_handler (log_handler, (void *) suite);

#ifdef MONGOC_ENABLE_SSL
   test_framework_global_ssl_opts_init ();
   atexit (test_framework_global_ssl_opts_cleanup);
#endif

   TestSuite_Init (suite, "", argc, argv);
   TestSuite_Add (suite, "/TestSuite/version_cmp", test_version_cmp);
}


void
test_libmongoc_destroy (TestSuite *suite)
{
   TestSuite_Destroy (suite);
   capture_logs (false); /* clear entries */
   _mongoc_array_destroy (&captured_logs);
   mongoc_cleanup ();
}

/*
 * test_framework_skip_if_no_legacy_opcodes returns 0 if the connected server
 * does not support legacy wire protocol op codes.
 *
 * As of SERVER-57457 and SERVER-57391, the following legacy wire protocol
 * op codes have been removed in the server 5.1:
 * - OP_KILL_CURSORS
 * - OP_INSERT
 * - OP_UPDATE
 * - OP_DELETE
 * - OP_GET_MORE
 * - OP_QUERY (for any command other than isMaster, which drivers use to
 * initially discover the min/max wire version of a server)
 */
bool
test_framework_supports_legacy_opcodes (void)
{
   /* Wire v14+ removed legacy opcodes */
   return test_framework_skip_if_max_wire_version_less_than_14 () == 0;
}

int
test_framework_skip_if_no_legacy_opcodes (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   if (test_framework_supports_legacy_opcodes ()) {
      return 1;
   }

   return 0;
}

/* SERVER-57390 removed the getLastError command on 5.1 servers. */
int
test_framework_skip_if_no_getlasterror (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   if (test_framework_supports_legacy_opcodes ()) {
      return 1;
   }

   return 0;
}

bool
test_framework_is_loadbalanced (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_LOADBALANCED") ||
          test_framework_getenv_bool ("MONGOC_TEST_DNS_LOADBALANCED");
}
