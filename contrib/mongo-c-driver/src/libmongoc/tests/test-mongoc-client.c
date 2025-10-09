#include <fcntl.h>
#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-cursor-private.h"
#include "mongoc/mongoc-cluster-private.h"
#include "mongoc/mongoc-database-private.h"
#include "mongoc/mongoc-handshake-private.h"
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-read-concern-private.h"
#include "mongoc/mongoc-set-private.h"
#ifdef MONGOC_ENABLE_SSL
#include "mongoc/mongoc-ssl.h"
#include "mongoc/mongoc-ssl-private.h"
#endif
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-write-concern-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "mock_server/mock-rs.h"


#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "client-test"


static void
test_client_cmd_w_server_id (void)
{
   mock_rs_t *rs;
   mongoc_client_t *client;
   bson_error_t error;
   bson_t *opts;
   bson_t reply;
   future_t *future;
   request_t *request;

   rs = mock_rs_with_auto_hello (WIRE_VERSION_MIN, true /* has primary */, 1 /* secondary   */, 0 /* arbiters    */);

   mock_rs_run (rs);
   client = test_framework_client_new_from_uri (mock_rs_get_uri (rs), NULL);

   /* use serverId instead of prefs to select the secondary */
   opts = tmp_bson ("{'serverId': 2, 'readConcern': {'level': 'local'}}");
   future = future_client_read_command_with_opts (
      client, "db", tmp_bson ("{'ping': 1}"), NULL /* prefs */, opts, &reply, &error);

   /* recognized that wire version is recent enough for readConcern */
   request = mock_rs_receives_msg (rs,
                                   MONGOC_MSG_NONE,
                                   tmp_bson ("{'$db': 'db',"
                                             " 'ping': 1,"
                                             " 'readConcern': {'level': 'local'},"
                                             " 'serverId': {'$exists': false}}"));

   ASSERT (mock_rs_request_is_to_secondary (rs, request));
   reply_to_request_simple (request, "{'ok': 1}");
   ASSERT_OR_PRINT (future_get_bool (future), error);

   bson_destroy (&reply);
   future_destroy (future);
   request_destroy (request);
   mongoc_client_destroy (client);
   mock_rs_destroy (rs);
}


static void
test_client_cmd_w_server_id_sharded (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_error_t error;
   bson_t *opts;
   bson_t reply;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   opts = tmp_bson ("{'serverId': 1}");
   future = future_client_read_command_with_opts (
      client, "db", tmp_bson ("{'ping': 1}"), NULL /* prefs */, opts, &reply, &error);

   /* does NOT set secondaryOk, since this is a sharded topology */
   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'ping': 1, 'serverId': {'$exists': false}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);

   bson_destroy (&reply);
   future_destroy (future);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_server_id_option (void *ctx)
{
   mongoc_client_t *client;
   bson_error_t error;
   bson_t *cmd;
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   cmd = tmp_bson ("{'ping': 1}");
   r = mongoc_client_read_command_with_opts (
      client, "test", cmd, NULL /* prefs */, tmp_bson ("{'serverId': 'foo'}"), NULL, &error);

   ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "must be an integer");

   r = mongoc_client_read_command_with_opts (
      client, "test", cmd, NULL /* prefs */, tmp_bson ("{'serverId': 0}"), NULL, &error);

   ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "must be >= 1");

   r = mongoc_client_read_command_with_opts (
      client, "test", cmd, NULL /* prefs */, tmp_bson ("{'serverId': 1}"), NULL, &error);

   ASSERT_OR_PRINT (r, error);

   mongoc_client_destroy (client);
}


static void
test_client_cmd_w_write_concern (void *ctx)
{
   mongoc_write_concern_t *good_wc;
   mongoc_write_concern_t *bad_wc;
   mongoc_client_t *client;
   bson_t *command = tmp_bson ("{'insert' : 'test', "
                               "'documents' : [{'hello' : 'world'}]}");
   bson_t reply;
   bson_t *opts = NULL;
   bson_error_t error;

   BSON_UNUSED (ctx);

   opts = bson_new ();
   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, 2);

   good_wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (good_wc, 1);

   bad_wc = mongoc_write_concern_new ();
   /* writeConcern that will not pass mongoc_write_concern_is_valid */
   bad_wc->wtimeout = -10;

   mongoc_write_concern_append (good_wc, opts);
   ASSERT_OR_PRINT (mongoc_client_write_command_with_opts (client, "test", command, opts, &reply, &error), error);

   bson_reinit (opts);
   bson_destroy (&reply);

   mongoc_write_concern_append_bad (bad_wc, opts);
   ASSERT (!mongoc_client_write_command_with_opts (client, "test", command, opts, &reply, &error));

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid writeConcern");
   bad_wc->wtimeout = 0;
   bson_destroy (&reply);
   error.code = 0;
   error.domain = 0;

   if (!test_framework_is_mongos ()) {
      mongoc_write_concern_set_w (bad_wc, 99);
      bson_reinit (opts);
      mongoc_write_concern_append_bad (bad_wc, opts);

      /* bad write concern in opts */
      ASSERT (!mongoc_client_write_command_with_opts (client, "test", command, opts, &reply, &error));
      assert_wc_oob_error (&error);
      bson_destroy (&reply);
   }

   mongoc_write_concern_destroy (good_wc);
   mongoc_write_concern_destroy (bad_wc);
   bson_destroy (opts);
   mongoc_client_destroy (client);
}


/*
 * test_client_cmd_write_concern:
 *
 * This test ensures that there is a lack of special
 * handling for write concerns and write concern
 * errors in generic functions that support commands
 * that write.
 *
 */

static void
test_client_cmd_write_concern (void)
{
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;
   future_t *future;
   request_t *request;
   mock_server_t *server;
   bson_t *cmd;

   /* set up client and wire protocol version */
   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* command with invalid writeConcern */
   cmd = tmp_bson ("{'$db': 'test', 'foo' : 1, 'writeConcern' : {'w' : 99 }}");
   future = future_client_command_simple (client, "test", cmd, NULL, &reply, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, cmd);
   BSON_ASSERT (request);

   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));

   future_destroy (future);
   bson_destroy (&reply);

   /* standalone response */
   future = future_client_command_simple (client, "test", cmd, NULL, &reply, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, cmd);
   BSON_ASSERT (request);

   reply_to_request_simple (request,
                            "{ 'ok' : 0, 'errmsg' : 'cannot use w > 1 when a "
                            "host is not replicated', 'code' : 2 }");

   BSON_ASSERT (!future_get_bool (future));
   future_destroy (future);
   request_destroy (request);
   bson_destroy (&reply);

   /* replicaset response */
   future = future_client_command_simple (client, "test", cmd, NULL, &reply, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, cmd);
   reply_to_request_simple (request,
                            "{ 'ok' : 1, 'n': 1, "
                            "'writeConcernError': {'code': 17, 'errmsg': 'foo'}}");
   BSON_ASSERT (future_get_bool (future));

   bson_destroy (&reply);
   future_destroy (future);
   mock_server_destroy (server);
   mongoc_client_destroy (client);
   request_destroy (request);
}


static void
test_client_cmd_write_concern_fam (void)
{
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_t *fam;
   bson_t reply;
   bson_error_t error;
   future_t *future;
   request_t *request;
   mock_server_t *server;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_client_set_write_concern (client, wc);
   fam = tmp_bson ("{'findAndModify': 'collection'}");

   future = future_client_read_write_command_with_opts (client, "test", fam, NULL, NULL, &reply, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'test', 'findAndModify': 'collection', "
                                                 "'writeConcern': {'w': 2}}"));

   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));

   bson_destroy (&reply);
   future_destroy (future);
   mock_server_destroy (server);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
}


static char *
gen_test_user (void)
{
   return bson_strdup_printf ("testuser_%u_%u", (unsigned) time (NULL), (unsigned) gettestpid ());
}


static char *
gen_good_uri (const char *username, const char *dbname)
{
   char *host = test_framework_get_host ();
   uint16_t port = test_framework_get_port ();
   char *uri = bson_strdup_printf ("mongodb://%s:testpass@%s:%hu/%s", username, host, port, dbname);

   bson_free (host);
   return uri;
}


static void
test_mongoc_client_authenticate (void *context)
{
   mongoc_client_t *admin_client;
   char *username;
   char *uri;
   bson_t roles;
   mongoc_database_t *database;
   char *uri_str_no_auth;
   char *uri_str_auth;
   mongoc_collection_t *collection;
   mongoc_client_t *auth_client;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;
   bool r;
   bson_t q;

   BSON_UNUSED (context);

   /*
    * Log in as admin.
    */
   admin_client = test_framework_new_default_client ();

   /*
    * Add a user to the test database.
    */
   username = gen_test_user ();
   uri = gen_good_uri (username, "test");

   database = mongoc_client_get_database (admin_client, "test");
   (void) mongoc_database_remove_user (database, username, &error);
   bson_init (&roles);
   BCON_APPEND (&roles, "0", "{", "role", "read", "db", "test", "}");

   r = mongoc_database_add_user (
      database, username, "testpass", tmp_bson ("[{'role': 'read', 'db': 'test'}]"), NULL, &error);

   ASSERT_OR_PRINT (r, error);
   mongoc_database_destroy (database);

   /*
    * Try authenticating with that user.
    */
   bson_init (&q);
   uri_str_no_auth = test_framework_get_uri_str_no_auth ("test");
   uri_str_auth = test_framework_add_user_password (uri_str_no_auth, username, "testpass");
   auth_client = test_framework_client_new (uri_str_auth, NULL);
   test_framework_set_ssl_opts (auth_client);
   collection = mongoc_client_get_collection (auth_client, "test", "test");
   cursor = mongoc_collection_find_with_opts (collection, &q, NULL, NULL);
   r = mongoc_cursor_next (cursor, &doc);
   if (!r) {
      r = mongoc_cursor_error (cursor, &error);
      if (r) {
         fprintf (stderr, "Authentication failure: \"%s\"", error.message);
      }
      BSON_ASSERT (!r);
   }

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (auth_client);

   /*
    * Remove all test users.
    */
   database = mongoc_client_get_database (admin_client, "test");
   r = mongoc_database_remove_all_users (database, &error);
   BSON_ASSERT (r);

   bson_destroy (&q);
   bson_free (uri_str_no_auth);
   bson_free (uri_str_auth);
   bson_destroy (&roles);
   bson_free (uri);
   bson_free (username);
   mongoc_database_destroy (database);
   mongoc_client_destroy (admin_client);
}


static void
test_mongoc_client_speculative_auth_failure (bool pooled)
{
   mongoc_client_t *admin_client;
   char *username;
   bson_t roles;
   mongoc_database_t *database;
   char *uri_str_no_auth;
   char *uri_str_auth;
   mongoc_collection_t *collection;
   mongoc_client_t *auth_client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;
   bool r;
   bson_t q;

   /*
    * Log in as admin.
    */
   admin_client = test_framework_new_default_client ();

   /*
    * Add a user to the test database.
    */
   username = gen_test_user ();
   database = mongoc_client_get_database (admin_client, "test");
   (void) mongoc_database_remove_user (database, username, &error);
   bson_init (&roles);
   BCON_APPEND (&roles, "0", "{", "role", "read", "db", "test", "}");

   r = mongoc_database_add_user (
      database, username, "testpass", tmp_bson ("[{'role': 'read', 'db': 'test'}]"), NULL, &error);

   ASSERT_OR_PRINT (r, error);
   mongoc_database_destroy (database);

   bson_init (&q);
   uri_str_no_auth = test_framework_get_uri_str_no_auth ("test");
   uri_str_auth = test_framework_add_user_password (uri_str_no_auth, username, "testpass");

   {
      mongoc_uri_t *const uri = mongoc_uri_new (uri_str_auth);

      ASSERT (mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYREADS, false));

      if (pooled) {
         pool = test_framework_client_pool_new_from_uri (uri, NULL);
         test_framework_set_pool_ssl_opts (pool);
         auth_client = mongoc_client_pool_pop (pool);
      } else {
         auth_client = test_framework_client_new_from_uri (uri, NULL);
         test_framework_set_ssl_opts (auth_client);
      }
      mongoc_uri_destroy (uri);
   }

   collection = mongoc_client_get_collection (auth_client, "test", "test");

   database = mongoc_client_get_database (admin_client, "admin");

   /* Enable failpoint to break saslContinue */
   r = mongoc_database_command_simple (database,
                                       tmp_bson ("{'configureFailPoint': 'failCommand', "
                                                 "'mode': {'times': 1}, "
                                                 "'data': {'failCommands': ['saslContinue'], 'closeConnection': "
                                                 "true, 'errorCode': 10107}}"),
                                       NULL,
                                       NULL,
                                       &error);
   ASSERT_OR_PRINT (r, error);
   mongoc_database_destroy (database);

   /* Try authenticating by running a find operation */
   capture_logs (true);
   cursor = mongoc_collection_find_with_opts (collection, &q, NULL, NULL);
   r = mongoc_cursor_next (cursor, &doc);
   if (!r) {
      r = mongoc_cursor_error (cursor, &error);
      BSON_ASSERT (r);
      /* A client pool on servers supporting speculative auth (4.4+) will get an
       * error on saslContinue and subsequently attempt to start auth from the
       * beginning. The failpoint closes the stream causing an error from
       * saslStart. */
      if (pooled && test_framework_max_wire_version_at_least (WIRE_VERSION_4_4)) {
         ASSERT_CAPTURED_LOG ("cluster", MONGOC_LOG_LEVEL_WARNING, "Failed to send \"saslStart\" command");
         ASSERT_ERROR_CONTAINS (
            error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Failed to send \"saslStart\" command");
      } else {
         ASSERT_ERROR_CONTAINS (
            error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Failed to send \"saslContinue\" command");
      }
   }
   capture_logs (false);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, auth_client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (auth_client);
   }

   /*
    * Remove all test users.
    */
   database = mongoc_client_get_database (admin_client, "test");
   r = mongoc_database_remove_all_users (database, &error);
   BSON_ASSERT (r);

   bson_destroy (&q);
   bson_free (uri_str_no_auth);
   bson_free (uri_str_auth);
   bson_destroy (&roles);
   bson_free (username);
   mongoc_database_destroy (database);
   mongoc_client_destroy (admin_client);
}

static void
test_mongoc_client_single_speculative_auth_failure (void *context)
{
   BSON_UNUSED (context);

   test_mongoc_client_speculative_auth_failure (false);
}

static void
test_mongoc_client_pooled_speculative_auth_failure (void *context)
{
   BSON_UNUSED (context);

   test_mongoc_client_speculative_auth_failure (true);
}


static void
test_mongoc_client_authenticate_cached (bool pooled)
{
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_t insert = BSON_INITIALIZER;
   const bson_t *doc;
   bson_error_t error;
   bool r;
   int i = 0;
   uint32_t server_id;

   {
      mongoc_uri_t *const uri = test_framework_get_uri ();

      // Avoid retryable handshakes from interfering with screwed-up cache.
      mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYREADS, false);

      if (pooled) {
         pool = test_framework_client_pool_new_from_uri (uri, NULL);
         test_framework_set_pool_ssl_opts (pool);
         client = mongoc_client_pool_pop (pool);
      } else {
         client = test_framework_client_new_from_uri (uri, NULL);
         test_framework_set_ssl_opts (client);
      }

      mongoc_uri_destroy (uri);
   }

   collection = mongoc_client_get_collection (client, "test", "test");
   mongoc_collection_insert_one (collection, &insert, NULL, NULL, &error);
   for (i = 0; i < 10; i++) {
      mongoc_topology_scanner_node_t *scanner_node;

      cursor = mongoc_collection_find_with_opts (collection, &insert, NULL, NULL);
      r = mongoc_cursor_next (cursor, &doc);
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT (r);
      server_id = mongoc_cursor_get_hint (cursor);
      mongoc_cursor_destroy (cursor);

      if (pooled) {
         mongoc_cluster_disconnect_node (&client->cluster, server_id);
      } else {
         scanner_node = mongoc_topology_scanner_get_node (client->topology->scanner, server_id);
         mongoc_stream_destroy (scanner_node->stream);
         scanner_node->stream = NULL;
      }
   }

   mongoc_collection_destroy (collection);
   if (pooled) {
      capture_logs (true);
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   bson_destroy (&insert);
}


static void
test_mongoc_client_authenticate_cached_pooled (void *context)
{
   BSON_UNUSED (context);

   test_mongoc_client_authenticate_cached (true);
}


static void
test_mongoc_client_authenticate_cached_single (void *context)
{
   BSON_UNUSED (context);

   test_mongoc_client_authenticate_cached (false);
}


static void
test_mongoc_client_authenticate_failure (void *context)
{
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   mongoc_client_t *client;
   const bson_t *doc;
   bson_error_t error;
   bool r;
   bson_t q;
   bson_t empty = BSON_INITIALIZER;
   char *host = test_framework_get_host ();
   char *uri_str_no_auth = test_framework_get_uri_str_no_auth (NULL);
   char *bad_uri_str = test_framework_add_user_password (uri_str_no_auth, "baduser", "badpass");

   BSON_UNUSED (context);

   /*
    * Try authenticating with bad user.
    */
   bson_init (&q);
   client = test_framework_client_new (bad_uri_str, NULL);
   test_framework_set_ssl_opts (client);

   collection = mongoc_client_get_collection (client, "test", "test");
   cursor = mongoc_collection_find_with_opts (collection, &q, NULL, NULL);
   r = mongoc_cursor_next (cursor, &doc);
   BSON_ASSERT (!r);
   r = mongoc_cursor_error (cursor, &error);
   BSON_ASSERT (r);
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_CLIENT);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_CLIENT_AUTHENTICATE);
   mongoc_cursor_destroy (cursor);

   /*
    * Try various commands while in the failed state to ensure we get the
    * same sort of errors.
    */
   r = mongoc_collection_insert_one (collection, &empty, NULL, NULL, &error);
   BSON_ASSERT (!r);
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_CLIENT);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_CLIENT_AUTHENTICATE);

   /*
    * Try various commands while in the failed state to ensure we get the
    * same sort of errors.
    */
   r = mongoc_collection_update_one (collection, &q, &empty, NULL, NULL, &error);
   BSON_ASSERT (!r);
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_CLIENT);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_CLIENT_AUTHENTICATE);

   bson_destroy (&q);
   bson_destroy (&empty);
   bson_free (host);
   bson_free (uri_str_no_auth);
   bson_free (bad_uri_str);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}


static void
test_mongoc_client_authenticate_timeout (void *context)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;
   future_t *future;
   request_t *request;

   BSON_UNUSED (context);

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_username (uri, "user");
   mongoc_uri_set_password (uri, "password");
   mongoc_uri_set_option_as_int32 (uri, "socketTimeoutMS", 10);
   client = test_framework_client_new_from_uri (uri, NULL);

   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, &reply, &error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin'}"));

   ASSERT (request);
   ASSERT_CMPSTR (request->command_name, "saslStart");

   /* don't reply */
   BSON_ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "Failed to send \"saslStart\" command with database \"admin\"");

   bson_destroy (&reply);
   future_destroy (future);
   request_destroy (request);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


/* Update: this test was changed after CDRIVER-3653 was fixed.
 * Originally, the test used cursor operations, assuming changes in the
 * min/maxWireVersion from the mock server would be re-evaluated on each cursor
 * operation.
 * After CDRIVER-3653 was fixed, this is no longer true. The cursor will examine
 * the server description associated with the connection handshake. If the
 * connection has not been closed, changes from monitoring will not affect the
 * connection's server description.
 * This test now uses mongoc_client_select_server to validate wire version
 * checks. */
static void
test_wire_version (void)
{
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mock_server_t *server;
   bson_error_t error;
   bson_t q = BSON_INITIALIZER;
   mongoc_server_description_t *sd;

   if (!test_framework_skip_if_slow ()) {
      bson_destroy (&q);
      return;
   }

   server = mock_server_new ();

   /* too new */
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MAX + 1,
                           WIRE_VERSION_MAX + 1);

   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 500);
   client = test_framework_client_new_from_uri (uri, NULL);
   sd = mongoc_client_select_server (client, true, NULL, &error);
   BSON_ASSERT (!sd);
   BSON_ASSERT (error.domain == MONGOC_ERROR_PROTOCOL);
   BSON_ASSERT (error.code == MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION);

   /* too old */
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MIN - 1,
                           WIRE_VERSION_MIN - 1);

   /* wait until it's time for next heartbeat */
   _mongoc_usleep (600 * 1000);
   sd = mongoc_client_select_server (client, true, NULL, &error);
   BSON_ASSERT (!sd);
   BSON_ASSERT (error.domain == MONGOC_ERROR_PROTOCOL);
   BSON_ASSERT (error.code == MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION);

   /* compatible again */
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX);

   /* wait until it's time for next heartbeat */
   _mongoc_usleep (600 * 1000);
   sd = mongoc_client_select_server (client, true, NULL, &error);
   ASSERT_OR_PRINT (sd, error);
   mongoc_server_description_destroy (sd);

   bson_destroy (&q);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static void
test_mongoc_client_command (void)
{
   mongoc_client_t *client;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bool r;
   bson_t cmd = BSON_INITIALIZER;

   client = test_framework_new_default_client ();
   BSON_ASSERT (client);

   bson_append_int32 (&cmd, "ping", 4, 1);

   cursor = mongoc_client_command (client, "admin", MONGOC_QUERY_NONE, 0, 1, 0, &cmd, NULL, NULL);

   r = mongoc_cursor_next (cursor, &doc);
   BSON_ASSERT (r);
   BSON_ASSERT (doc);

   r = mongoc_cursor_next (cursor, &doc);
   BSON_ASSERT (!r);
   BSON_ASSERT (!doc);

   mongoc_cursor_destroy (cursor);
   mongoc_client_destroy (client);
   bson_destroy (&cmd);
}


static void
test_mongoc_client_command_defaults (void)
{
   mongoc_client_t *client;
   mongoc_read_prefs_t *read_prefs;
   mongoc_read_concern_t *read_concern;
   mongoc_cursor_t *cursor;


   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "majority");

   client = test_framework_new_default_client ();
   mongoc_client_set_read_prefs (client, read_prefs);
   mongoc_client_set_read_concern (client, read_concern);

   cursor = mongoc_client_command (client, "admin", MONGOC_QUERY_NONE, 0, 0, 0, tmp_bson ("{'ping': 1}"), NULL, NULL);

   /* Read and Write Concern spec: "If your driver offers a generic RunCommand
    * method on your database object, ReadConcern MUST NOT be applied
    * automatically to any command. A user wishing to use a ReadConcern in a
    * generic command must supply it manually." Server Selection Spec: "The
    * generic command method MUST ignore any default read preference from
    * client, database or collection configuration. The generic command method
    * SHOULD allow an optional read preference argument."
    */
   ASSERT (cursor->read_concern->level == NULL);
   ASSERT (cursor->read_prefs->mode == MONGOC_READ_PRIMARY);

   mongoc_cursor_destroy (cursor);
   mongoc_read_concern_destroy (read_concern);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_client_destroy (client);
}


static void
test_mongoc_client_command_secondary (void)
{
   mongoc_client_t *client;
   mongoc_cursor_t *cursor;
   mongoc_read_prefs_t *read_prefs;
   bson_t cmd = BSON_INITIALIZER;
   const bson_t *reply;

   client = test_framework_new_default_client ();
   BSON_ASSERT (client);

   BSON_APPEND_INT32 (&cmd, "invalid_command_here", 1);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   cursor = mongoc_client_command (client, "admin", MONGOC_QUERY_NONE, 0, 1, 0, &cmd, NULL, read_prefs);
   mongoc_cursor_next (cursor, &reply);

   if (test_framework_is_replset ()) {
      BSON_ASSERT (test_framework_server_is_secondary (client, mongoc_cursor_get_hint (cursor)));
   }

   mongoc_read_prefs_destroy (read_prefs);

   mongoc_cursor_destroy (cursor);
   mongoc_client_destroy (client);
   bson_destroy (&cmd);
}


static void
_test_command_read_prefs (bool simple, bool pooled)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_read_prefs_t *secondary_pref;
   bson_t *cmd;
   future_t *future;
   bson_error_t error;
   request_t *request;
   mongoc_cursor_t *cursor;
   const bson_t *reply;

   /* mock mongos: easiest way to test that read preference is configured */
   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   secondary_pref = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_uri_set_read_prefs_t (uri, secondary_pref);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   ASSERT_CMPINT (MONGOC_READ_SECONDARY, ==, mongoc_read_prefs_get_mode (mongoc_client_get_read_prefs (client)));

   cmd = tmp_bson ("{'foo': 1}");

   if (simple) {
      /* simple, without read preference */
      future = future_client_command_simple (client, "db", cmd, NULL, NULL, &error);

      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'foo': 1}"));

      reply_to_request_with_ok_and_destroy (request);
      ASSERT_OR_PRINT (future_get_bool (future), error);
      future_destroy (future);

      /* with read preference */
      future = future_client_command_simple (client, "db", cmd, secondary_pref, NULL, &error);

      request = mock_server_receives_msg (server,
                                          MONGOC_MSG_NONE,
                                          tmp_bson ("{'$db': 'db',"
                                                    " 'foo': 1,"
                                                    " '$readPreference': {'mode': 'secondary'}}"));
      reply_to_request_with_ok_and_destroy (request);
      ASSERT_OR_PRINT (future_get_bool (future), error);
      future_destroy (future);
   } else {
      /* not simple, no read preference */
      cursor = mongoc_client_command (client, "db", MONGOC_QUERY_NONE, 0, 0, 0, cmd, NULL, NULL);
      future = future_cursor_next (cursor, &reply);
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'foo': 1}"));

      reply_to_request_with_ok_and_destroy (request);
      ASSERT (future_get_bool (future));
      future_destroy (future);
      mongoc_cursor_destroy (cursor);

      /* with read preference */
      cursor = mongoc_client_command (client, "db", MONGOC_QUERY_NONE, 0, 0, 0, cmd, NULL, secondary_pref);
      future = future_cursor_next (cursor, &reply);
      request = mock_server_receives_msg (server,
                                          MONGOC_MSG_NONE,
                                          tmp_bson ("{'$db': 'db',"
                                                    " 'foo': 1,"
                                                    " '$readPreference': {'mode': 'secondary'}}"));

      reply_to_request_with_ok_and_destroy (request);
      ASSERT (future_get_bool (future));
      future_destroy (future);
      mongoc_cursor_destroy (cursor);
   }

   mongoc_uri_destroy (uri);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_read_prefs_destroy (secondary_pref);
   mock_server_destroy (server);
}


static void
test_command_simple_read_prefs_single (void)
{
   _test_command_read_prefs (true, false);
}


static void
test_command_simple_read_prefs_pooled (void)
{
   _test_command_read_prefs (true, true);
}


static void
test_command_read_prefs_single (void)
{
   _test_command_read_prefs (false, false);
}


static void
test_command_read_prefs_pooled (void)
{
   _test_command_read_prefs (false, true);
}


static void
test_command_not_found (void)
{
   mongoc_client_t *client;
   const bson_t *doc;
   bson_error_t error;
   mongoc_cursor_t *cursor;

   client = test_framework_new_default_client ();
   cursor = mongoc_client_command (client, "test", MONGOC_QUERY_NONE, 0, 0, 0, tmp_bson ("{'foo': 1}"), NULL, NULL);

   ASSERT (!mongoc_cursor_next (cursor, &doc));
   ASSERT (mongoc_cursor_error (cursor, &error));
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_QUERY);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_QUERY_COMMAND_NOT_FOUND);

   mongoc_cursor_destroy (cursor);
   mongoc_client_destroy (client);
}


static void
test_command_not_found_simple (void)
{
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;

   client = test_framework_new_default_client ();
   ASSERT (!mongoc_client_command_simple (client, "test", tmp_bson ("{'foo': 1}"), NULL, &reply, &error));

   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_QUERY);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_QUERY_COMMAND_NOT_FOUND);

   bson_destroy (&reply);
   mongoc_client_destroy (client);
}


static void
test_command_with_opts_read_prefs (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_read_prefs_t *read_prefs;
   bson_t *cmd;
   bson_t *opts;
   bson_error_t error;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_client_set_read_prefs (client, read_prefs);

   /* read prefs omitted for command that writes */
   cmd = tmp_bson ("{'create': 'db'}");
   future = future_client_write_command_with_opts (client, "admin", cmd, NULL /* opts */, NULL, &error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'create': 'db'}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* read prefs are included for read command */
   cmd = tmp_bson ("{'count': 'collection'}");
   future = future_client_read_command_with_opts (client, "admin", cmd, NULL, NULL /* opts */, NULL, &error);

   /* Server Selection Spec: For all read preference modes that are not
    * 'primary', drivers MUST set '$readPreference'. */
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'count': 'collection',"
                                                 " '$readPreference': {'mode': 'secondary'}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* read prefs not included for read/write command, but read concern is */
   cmd = tmp_bson ("{'whatever': 1}");
   opts = tmp_bson ("{'readConcern': {'level': 'majority'}}");
   future = future_client_read_write_command_with_opts (client, "admin", cmd, NULL, opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'whatever': 1,"
                                                 " 'readConcern': {'level': 'majority'},"
                                                 " '$readPreference': {'$exists': false}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   mongoc_read_prefs_destroy (read_prefs);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_read_write_cmd_with_opts (void)
{
   mock_rs_t *rs;
   mongoc_client_t *client;
   mongoc_read_prefs_t *secondary;
   bson_error_t error;
   bson_t reply;
   future_t *future;
   request_t *request;

   rs = mock_rs_with_auto_hello (WIRE_VERSION_MIN, true /* has primary */, 1 /* secondary */, 0 /* arbiters */);

   mock_rs_run (rs);
   client = test_framework_client_new_from_uri (mock_rs_get_uri (rs), NULL);
   secondary = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   /* mongoc_client_read_write_command_with_opts must ignore read prefs
    * CDRIVER-2224
    */
   future = future_client_read_write_command_with_opts (
      client, "db", tmp_bson ("{'ping': 1}"), secondary, NULL, &reply, &error);

   request = mock_rs_receives_msg (rs, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'ping': 1}"));

   ASSERT (mock_rs_request_is_to_primary (rs, request));
   reply_to_request_simple (request, "{'ok': 1}");
   ASSERT_OR_PRINT (future_get_bool (future), error);

   bson_destroy (&reply);
   future_destroy (future);
   request_destroy (request);
   mongoc_read_prefs_destroy (secondary);
   mongoc_client_destroy (client);
   mock_rs_destroy (rs);
}


static void
test_read_command_with_opts (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_t *cmd;
   bson_t *opts;
   mongoc_write_concern_t *wc;
   mongoc_read_concern_t *read_concern;
   bson_error_t error;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* collation allowed */
   cmd = tmp_bson ("{'create': 'db'}");
   opts = tmp_bson ("{'collation': {'locale': 'en_US'}}");
   future = future_client_write_command_with_opts (client, "admin", cmd, opts, NULL, &error);

   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'create': 'db', 'collation': {'locale': 'en_US'}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* writeConcern included */
   cmd = tmp_bson ("{'create': 'db'}");
   opts = tmp_bson ("{'writeConcern': {'w': 1}}");
   future = future_client_write_command_with_opts (client, "admin", cmd, opts, NULL, &error);

   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'create': 'db', 'writeConcern': {'w': 1}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* apply client's write concern by default */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 1);
   mongoc_client_set_write_concern (client, wc);
   future = future_client_write_command_with_opts (client, "admin", cmd, NULL /* opts */, NULL, &error);

   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'create': 'db', 'writeConcern': {'w': 1}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* apply write concern from opts, not client */
   opts = tmp_bson ("{'writeConcern': {'w': 2}}");
   mongoc_write_concern_destroy (wc);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 4);
   mongoc_client_set_write_concern (client, wc);
   future = future_client_write_command_with_opts (client, "admin", cmd, opts, NULL, &error);

   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'create': 'db', 'writeConcern': {'w': 2}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* readConcern allowed */
   cmd = tmp_bson ("{'count': 'collection'}");
   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "local");
   opts = tmp_bson (NULL);
   mongoc_read_concern_append (read_concern, opts);
   future = future_client_read_command_with_opts (client, "admin", cmd, NULL, opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'count': 'collection',"
                                                 " 'readConcern': {'level': 'local'}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* apply client's readConcern by default */
   mongoc_client_set_read_concern (client, read_concern);
   future = future_client_read_command_with_opts (client, "admin", cmd, NULL, NULL /* opts */, NULL, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'count': 'collection',"
                                                 " 'readConcern': {'level': 'local'}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   mongoc_read_concern_destroy (read_concern);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static void
test_command_with_opts (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_t *cmd;
   mongoc_write_concern_t *wc;
   mongoc_read_concern_t *read_concern;
   mongoc_read_prefs_t *prefs;
   bson_error_t error;
   future_t *future;
   request_t *request;
   bson_t opts = BSON_INITIALIZER;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* client's write concern, read concern, read prefs are ignored */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_client_set_write_concern (client, wc);

   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "majority");
   mongoc_client_set_read_concern (client, read_concern);

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_client_set_read_prefs (client, prefs);

   cmd = tmp_bson ("{'create': 'db'}");
   future = future_client_command_with_opts (client, "admin", cmd, NULL, NULL, NULL, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'create': 'db',"
                                                 " 'readConcern': {'$exists': false},"
                                                 " 'writeConcern': {'$exists': false}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* write concern, read concern, and read preference passed in explicitly */
   mongoc_write_concern_append (wc, &opts);
   mongoc_read_concern_append (read_concern, &opts);
   future = future_client_command_with_opts (client, "admin", cmd, prefs, &opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'admin',"
                                                 " 'create':'db',"
                                                 " 'writeConcern': {'w': 2},"
                                                 " 'readConcern': {'level':'majority'},"
                                                 " '$readPreference': {'mode':'secondary'}}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   bson_destroy (&opts);
   mongoc_read_prefs_destroy (prefs);
   mongoc_read_concern_destroy (read_concern);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_command_with_opts_op_msg (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_t *cmd;
   mongoc_write_concern_t *wc;
   mongoc_read_concern_t *read_concern;
   mongoc_read_prefs_t *prefs;
   bson_error_t error;
   future_t *future;
   request_t *request;
   bson_t opts = BSON_INITIALIZER;

   server = mock_mongos_new (WIRE_VERSION_MAX);

   mock_server_auto_endsessions (server);

   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* client's write concern, read concern, read prefs are ignored */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_client_set_write_concern (client, wc);

   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "majority");
   mongoc_client_set_read_concern (client, read_concern);

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_client_set_read_prefs (client, prefs);

   cmd = tmp_bson ("{'create': 'db'}");
   future = future_client_command_with_opts (client, "admin", cmd, NULL, NULL, NULL, &error);

   request = mock_server_receives_msg (server,
                                       0,
                                       tmp_bson ("{"
                                                 "   'create': 'db',"
                                                 "   'readConcern': {'$exists': false},"
                                                 "   'writeConcern': {'$exists': false}"
                                                 "}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* write concern, read concern, and read preference passed in explicitly */
   mongoc_write_concern_append (wc, &opts);
   mongoc_read_concern_append (read_concern, &opts);
   future = future_client_command_with_opts (client, "admin", cmd, prefs, &opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       0,
                                       tmp_bson ("{"
                                                 "   'create':'db',"
                                                 "   'writeConcern': {'w': 2},"
                                                 "   'readConcern': {'level':'majority'},"
                                                 "   '$readPreference': {"
                                                 "      'mode':'secondary'"
                                                 "   }"
                                                 "}"));

   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   bson_destroy (&opts);
   mongoc_read_prefs_destroy (prefs);
   mongoc_read_concern_destroy (read_concern);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_command_empty (void)
{
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_error_t error;
   bool r;

   client = test_framework_new_default_client ();
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{}"), NULL, NULL, &error);

   ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Empty command document");

   r = mongoc_client_command_with_opts (client, "admin", tmp_bson ("{}"), NULL, tmp_bson ("{}"), NULL, &error);

   ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Empty command document");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 1);
   mongoc_client_set_write_concern (client, wc);

   r = mongoc_client_write_command_with_opts (client, "admin", tmp_bson ("{}"), NULL, NULL, &error);

   ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Empty command document");

   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
}


static void
test_command_no_errmsg (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_t *cmd;
   bson_error_t error;
   future_t *future;
   request_t *request;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   mongoc_client_set_error_api (client, 2);

   cmd = tmp_bson ("{'command': 1}");
   future = future_client_command_simple (client, "admin", cmd, NULL, NULL, &error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin'}"));

   /* auth errors have $err, not errmsg. we'd raised "Unknown command error",
    * see CDRIVER-1928 */
   reply_to_request_simple (request, "{'ok': 0, 'code': 7, '$err': 'bad!'}");
   ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 7, "bad!");

   future_destroy (future);
   request_destroy (request);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_unavailable_seeds (void)
{
   mock_server_t *servers[2];
   char **uri_strs;
   char **uri_str;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_t query = BSON_INITIALIZER;
   const bson_t *doc;
   bson_error_t error;

   int i;

   for (i = 0; i < 2; i++) {
      servers[i] = mock_server_down (); /* hangs up on all requests */
      mock_server_run (servers[i]);
   }

   uri_str = uri_strs = bson_malloc0 (7 * sizeof (char *));
   *(uri_str++) = bson_strdup_printf ("mongodb://%s", mock_server_get_host_and_port (servers[0]));

   *(uri_str++) = bson_strdup_printf (
      "mongodb://%s,%s", mock_server_get_host_and_port (servers[0]), mock_server_get_host_and_port (servers[1]));

   *(uri_str++) = bson_strdup_printf ("mongodb://%s,%s/?replicaSet=rs",
                                      mock_server_get_host_and_port (servers[0]),
                                      mock_server_get_host_and_port (servers[1]));

   *(uri_str++) = bson_strdup_printf ("mongodb://u:p@%s", mock_server_get_host_and_port (servers[0]));

   *(uri_str++) = bson_strdup_printf (
      "mongodb://u:p@%s,%s", mock_server_get_host_and_port (servers[0]), mock_server_get_host_and_port (servers[1]));

   *(uri_str++) = bson_strdup_printf ("mongodb://u:p@%s,%s/?replicaSet=rs",
                                      mock_server_get_host_and_port (servers[0]),
                                      mock_server_get_host_and_port (servers[1]));

   for (i = 0; i < (sizeof (uri_strs) / sizeof (const char *)); i++) {
      client = test_framework_client_new (uri_strs[i], NULL);
      BSON_ASSERT (client);

      collection = mongoc_client_get_collection (client, "test", "test");
      cursor = mongoc_collection_find_with_opts (collection, &query, NULL, NULL);
      BSON_ASSERT (!mongoc_cursor_next (cursor, &doc));
      BSON_ASSERT (mongoc_cursor_error (cursor, &error));
      ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_SERVER_SELECTION);
      ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_SERVER_SELECTION_FAILURE);

      mongoc_cursor_destroy (cursor);
      mongoc_collection_destroy (collection);
      mongoc_client_destroy (client);
   }

   for (i = 0; i < 2; i++) {
      mock_server_destroy (servers[i]);
   }

   bson_strfreev (uri_strs);
   bson_destroy (&query);
}


typedef enum { NO_CONNECT, CONNECT, RECONNECT } connection_option_t;


static bool
responder (request_t *request, void *data)
{
   BSON_UNUSED (data);

   if (!strcmp (request->command_name, "foo")) {
      reply_to_request_with_ok_and_destroy (request);
      return true;
   }

   return false;
}


/* mongoc_set_for_each callback */
static bool
host_equals (const void *item, void *ctx)
{
   const mongoc_server_description_t *sd = item;
   const char *host_and_port = ctx;

   return !strcasecmp (sd->host.host_and_port, host_and_port);
}


/* CDRIVER-721 catch errors in _mongoc_cluster_destroy */
static void
test_seed_list (bool rs, connection_option_t connection_option, bool pooled)
{
   mock_server_t *server;
   mock_server_t *down_servers[3];
   int i;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_topology_t *topology;
   mongoc_read_prefs_t *primary_pref;
   uint32_t discovered_nodes_len;
   bson_t reply;
   bson_error_t error;
   uint32_t id;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;

   server = mock_server_new ();
   mock_server_run (server);

   for (i = 0; i < 3; i++) {
      down_servers[i] = mock_server_down ();
      mock_server_run (down_servers[i]);
   }

   uri_str = bson_strdup_printf ("mongodb://%s,%s,%s,%s",
                                 mock_server_get_host_and_port (server),
                                 mock_server_get_host_and_port (down_servers[0]),
                                 mock_server_get_host_and_port (down_servers[1]),
                                 mock_server_get_host_and_port (down_servers[2]));

   uri = mongoc_uri_new (uri_str);
   BSON_ASSERT (uri);

   if (pooled) {
      /* must be >= minHeartbeatFrequencyMS=500 or the "reconnect"
       * case won't have time to succeed */
      mongoc_uri_set_option_as_int32 (uri, "serverSelectionTimeoutMS", 1000);
   }

   if (rs) {
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'isWritablePrimary': true,"
                              " 'setName': 'rs',"
                              " 'hosts': ['%s']}",
                              mock_server_get_host_and_port (server));

      mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   } else {
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'isWritablePrimary': true,"
                              " 'msg': 'isdbgrid'}");
   }

   /* auto-respond to "foo" command */
   mock_server_autoresponds (server, responder, NULL, NULL);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   topology = client->topology;

   /* a mongos load-balanced connection never removes down nodes */
   discovered_nodes_len = rs ? 1 : 4;

   primary_pref = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   if (connection_option == CONNECT || connection_option == RECONNECT) {
      /* only localhost:port responds to initial discovery. the other seeds are
       * discarded from replica set topology, but remain for sharded. */
      ASSERT_OR_PRINT (
         mongoc_client_command_simple (client, "test", tmp_bson ("{'foo': 1}"), primary_pref, &reply, &error), error);

      bson_destroy (&reply);

      /* td may be invalidated by client_command_simple */
      mc_tpld_renew_ref (&td, topology);
      ASSERT_CMPINT (discovered_nodes_len, ==, (int) mc_tpld_servers_const (td.ptr)->items_len);

      if (rs) {
         ASSERT_CMPINT (td.ptr->type, ==, MONGOC_TOPOLOGY_RS_WITH_PRIMARY);
      } else {
         ASSERT_CMPINT (td.ptr->type, ==, MONGOC_TOPOLOGY_SHARDED);
      }

      if (pooled) {
         /* nodes created on demand when we use servers for actual operations */
         ASSERT_CMPINT ((int) client->cluster.nodes->items_len, ==, 1);
      }
   }

   if (connection_option == RECONNECT) {
      id = mongoc_set_find_id (
         mc_tpld_servers_const (td.ptr), host_equals, (void *) mock_server_get_host_and_port (server));
      ASSERT_CMPINT (id, !=, 0);
      _mongoc_topology_invalidate_server (topology, id);
      mc_tpld_renew_ref (&td, topology);
      if (rs) {
         ASSERT_CMPINT (td.ptr->type, ==, MONGOC_TOPOLOGY_RS_NO_PRIMARY);
      } else {
         ASSERT_CMPINT (td.ptr->type, ==, MONGOC_TOPOLOGY_SHARDED);
      }

      ASSERT_OR_PRINT (
         mongoc_client_command_simple (client, "test", tmp_bson ("{'foo': 1}"), primary_pref, &reply, &error), error);

      bson_destroy (&reply);

      /* td may be invalidated by client_command_simple */
      mc_tpld_renew_ref (&td, topology);
      ASSERT_CMPINT (discovered_nodes_len, ==, (int) mc_tpld_servers_const (td.ptr)->items_len);

      if (pooled) {
         ASSERT_CMPINT ((int) client->cluster.nodes->items_len, ==, 1);
      }
   }

   /* testing for crashes like CDRIVER-721 */

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_read_prefs_destroy (primary_pref);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);

   for (i = 0; i < 3; i++) {
      mock_server_destroy (down_servers[i]);
   }

   mock_server_destroy (server);
   mc_tpld_drop_ref (&td);
}


static void
test_rs_seeds_no_connect_single (void)
{
   test_seed_list (true, NO_CONNECT, false);
}


static void
test_rs_seeds_no_connect_pooled (void)
{
   test_seed_list (true, NO_CONNECT, true);
}


static void
test_rs_seeds_connect_single (void)
{
   test_seed_list (true, CONNECT, false);
}

static void
test_rs_seeds_connect_pooled (void)
{
   test_seed_list (true, CONNECT, true);
}


static void
test_rs_seeds_reconnect_single (void)
{
   test_seed_list (true, RECONNECT, false);
}


static void
test_rs_seeds_reconnect_pooled (void)
{
   test_seed_list (true, RECONNECT, true);
}


static void
test_mongos_seeds_no_connect_single (void)
{
   test_seed_list (false, NO_CONNECT, false);
}


static void
test_mongos_seeds_no_connect_pooled (void)
{
   test_seed_list (false, NO_CONNECT, true);
}


static void
test_mongos_seeds_connect_single (void)
{
   test_seed_list (false, CONNECT, false);
}


static void
test_mongos_seeds_connect_pooled (void)
{
   test_seed_list (false, CONNECT, true);
}


static void
test_mongos_seeds_reconnect_single (void)
{
   test_seed_list (false, RECONNECT, false);
}


static void
test_mongos_seeds_reconnect_pooled (void)
{
   test_seed_list (false, RECONNECT, true);
}


static void
test_recovering (void *ctx)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_read_mode_t read_mode;
   mongoc_read_prefs_t *prefs;
   bson_error_t error;

   BSON_UNUSED (ctx);

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_new ();
   mock_server_run (server);

   /* server is "recovering": not primary, not secondary */
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': false,"
                           " 'secondary': false,"
                           " 'setName': 'rs',"
                           " 'hosts': ['%s']}",
                           mock_server_get_host_and_port (server));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   client = test_framework_client_new_from_uri (uri, NULL);
   prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   /* recovering member matches no read mode */
   for (read_mode = MONGOC_READ_PRIMARY; read_mode <= MONGOC_READ_NEAREST; read_mode++) {
      mongoc_read_prefs_set_mode (prefs, read_mode);
      BSON_ASSERT (!mongoc_topology_select (client->topology, MONGOC_SS_READ, prefs, NULL, &error));
   }

   mongoc_read_prefs_destroy (prefs);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static void
test_server_status (void)
{
   mongoc_client_t *client;
   bson_error_t error;
   bson_iter_t iter;
   bson_t reply;

   client = test_framework_new_default_client ();
   BSON_ASSERT (client);

   BEGIN_IGNORE_DEPRECATIONS
   ASSERT_OR_PRINT (mongoc_client_get_server_status (client, NULL, &reply, &error), error);
   END_IGNORE_DEPRECATIONS

   BSON_ASSERT (bson_iter_init_find (&iter, &reply, "host"));
   BSON_ASSERT (bson_iter_init_find (&iter, &reply, "version"));
   BSON_ASSERT (bson_iter_init_find (&iter, &reply, "ok"));

   bson_destroy (&reply);

   mongoc_client_destroy (client);
}


static void
test_get_database_names (void)
{
   mock_server_t *server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mongoc_client_t *client;
   bson_error_t error;
   future_t *future;
   request_t *request;
   char **names;

   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   future = future_client_get_database_names_with_opts (client, NULL, &error);
   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'listDatabases': 1, 'nameOnly': true}"));
   reply_to_request (request, 0, 0, 0, 1, "{'ok': 1.0, 'databases': [{'name': 'a'}, {'name': 'local'}]}");
   names = future_get_char_ptr_ptr (future);
   BSON_ASSERT (!strcmp (names[0], "a"));
   BSON_ASSERT (!strcmp (names[1], "local"));
   BSON_ASSERT (NULL == names[2]);

   bson_strfreev (names);
   request_destroy (request);
   future_destroy (future);

   future = future_client_get_database_names_with_opts (client, NULL, &error);
   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'listDatabases': 1, 'nameOnly': true}"));
   reply_to_request (request, 0, 0, 0, 1, "{'ok': 0.0, 'code': 17, 'errmsg': 'err'}");

   names = future_get_char_ptr_ptr (future);
   BSON_ASSERT (!names);
   ASSERT_CMPINT (MONGOC_ERROR_QUERY, ==, error.domain);
   ASSERT_CMPSTR ("err", error.message);

   request_destroy (request);
   future_destroy (future);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
_test_mongoc_client_ipv6 (bool pooled)
{
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
#if (defined(__APPLE__) || defined(_WIN32)) && defined(MONGOC_ENABLE_SSL)
   mongoc_ssl_opt_t ssl_opts;
#endif
   bson_error_t error;

   uri_str = test_framework_add_user_password_from_env ("mongodb://[::1]/");
   uri = mongoc_uri_new (uri_str);
   BSON_ASSERT (uri);

#if (defined(__APPLE__) || defined(_WIN32)) && defined(MONGOC_ENABLE_SSL)
   /* This is necessary because macOS & Windows seem to treat [::1] as not
    * matching the hostname "0:0:0:0:0:0:0:1", which results in a certificate
    * trust failure.  This behavior has been captured in CDRIVER-3765 (macOS)
    * and CDRIVER-3766 (Windows).  When those are fixed, the code in this
    * function should be restored to its previous state without the use of
    * ssl_opts.allow_invalid_hostname. */
   ssl_opts = *test_framework_get_ssl_opts ();
   ssl_opts.allow_invalid_hostname = true;
#endif

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
#if (defined(__APPLE__) || defined(_WIN32)) && defined(MONGOC_ENABLE_SSL)
      mongoc_client_pool_set_ssl_opts (pool, &ssl_opts);
#else
      test_framework_set_pool_ssl_opts (pool);
#endif
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
#if (defined(__APPLE__) || defined(_WIN32)) && defined(MONGOC_ENABLE_SSL)
      mongoc_client_set_ssl_opts (client, &ssl_opts);
#else
      test_framework_set_ssl_opts (client);
#endif
   }

#if (defined(__APPLE__) || defined(_WIN32)) && defined(MONGOC_ENABLE_SSL)
   BSON_ASSERT (client->ssl_opts.allow_invalid_hostname);
#endif

   ASSERT_OR_PRINT (
      mongoc_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error),
      error);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   bson_free (uri_str);
}


static void
test_mongoc_client_ipv6_single (void)
{
   _test_mongoc_client_ipv6 (false);
}


static void
test_mongoc_client_ipv6_pooled (void)
{
   _test_mongoc_client_ipv6 (true);
}


static void
test_mongoc_client_unix_domain_socket (void *context)
{
   mongoc_client_t *client;
   bson_error_t error;
   char *uri_str;

   BSON_UNUSED (context);

   uri_str = test_framework_get_unix_domain_socket_uri_str ();
   client = test_framework_client_new (uri_str, NULL);
   test_framework_set_ssl_opts (client);

   BSON_ASSERT (client);

   ASSERT_OR_PRINT (
      mongoc_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error),
      error);

   mongoc_client_destroy (client);
   bson_free (uri_str);
}


static void
test_mongoc_client_mismatched_me (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_read_prefs_t *prefs;
   bson_error_t error;
   future_t *future;
   request_t *request;
   char *reply;

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   client = test_framework_client_new_from_uri (uri, NULL);
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   /* any operation should fail with server selection error */
   future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), prefs, NULL, &error);

   request = mock_server_receives_any_hello (server);

   reply = bson_strdup_printf ("{'ok': 1,"
                               " 'setName': 'rs',"
                               " 'isWritablePrimary': false,"
                               " 'secondary': true,"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d,"
                               " 'me': 'foo.com'," /* mismatched "me" field */
                               " 'hosts': ['%s']}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX,
                               mock_server_get_host_and_port (server));

   capture_logs (true);
   reply_to_request_simple (request, reply);

   BSON_ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "No suitable servers");
   ASSERT_CAPTURED_LOG ("client", MONGOC_LOG_LEVEL_WARNING, "Last server removed from topology");
   capture_logs (false);

   bson_free (reply);
   request_destroy (request);
   future_destroy (future);
   mongoc_read_prefs_destroy (prefs);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


#ifdef MONGOC_ENABLE_SSL
static void
_test_mongoc_client_ssl_opts (bool pooled)
{
   char *host_and_port;
   char *uri_str;
   char *uri_str_auth;
   char *uri_str_auth_ssl;
   mongoc_uri_t *uri;
   const mongoc_ssl_opt_t *ssl_opts;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bool ret;
   bson_error_t error;
   int add_ssl_to_uri;

   host_and_port = test_framework_get_host_and_port ();
   uri_str = bson_strdup_printf ("mongodb://%s/?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000", host_and_port);

   uri_str_auth = test_framework_add_user_password_from_env (uri_str);
   uri_str_auth_ssl = bson_strdup_printf ("%s&ssl=true", uri_str_auth);

   ssl_opts = test_framework_get_ssl_opts ();

   /* client uses SSL once SSL options are set, regardless of "ssl=true" */
   for (add_ssl_to_uri = 0; add_ssl_to_uri < 2; add_ssl_to_uri++) {
      if (add_ssl_to_uri) {
         uri = mongoc_uri_new (uri_str_auth_ssl);
      } else {
         uri = mongoc_uri_new (uri_str_auth);
      }

      if (pooled) {
         pool = test_framework_client_pool_new_from_uri (uri, NULL);
         mongoc_client_pool_set_ssl_opts (pool, ssl_opts);
         client = mongoc_client_pool_pop (pool);
      } else {
         client = test_framework_client_new_from_uri (uri, NULL);
         mongoc_client_set_ssl_opts (client, ssl_opts);
      }

      /* any operation */
      ret = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

      if (test_framework_get_ssl ()) {
         ASSERT_OR_PRINT (ret, error);
      } else {
         /* TODO: CDRIVER-936 check the err msg has "SSL handshake failed" */
         ASSERT (!ret);
         ASSERT_CMPINT (MONGOC_ERROR_SERVER_SELECTION, ==, error.domain);
      }

      if (pooled) {
         mongoc_client_pool_push (pool, client);
         mongoc_client_pool_destroy (pool);
      } else {
         mongoc_client_destroy (client);
      }

      mongoc_uri_destroy (uri);
   }

   bson_free (uri_str_auth_ssl);
   bson_free (uri_str_auth);
   bson_free (uri_str);
   bson_free (host_and_port);
}


static void
test_ssl_single (void)
{
   _test_mongoc_client_ssl_opts (false);
}


static void
test_ssl_pooled (void)
{
   _test_mongoc_client_ssl_opts (true);
}

static void
test_client_buildinfo_hang (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_database_t *database;
   bson_error_t error;
   bson_t command;
   bson_t reply;

   pool = test_framework_new_default_client_pool ();
   BSON_ASSERT (pool);
   client = mongoc_client_pool_pop (pool);

   database = mongoc_client_get_database (client, "admin");
   bson_init (&command);
   bson_append_int32 (&command, "buildInfo", -1, 1);

   /* Prior to a bug fix this command caused a hang - see CDRIVER-3318 */
   ASSERT_OR_PRINT (mongoc_database_command_simple (database, &command, NULL, &reply, &error), error);

   bson_destroy (&command);
   bson_destroy (&reply);
   mongoc_database_destroy (database);

   capture_logs (true);
   mongoc_client_destroy (client);
   mongoc_client_pool_destroy (pool);
   capture_logs (false);
}

#else
/* MONGOC_ENABLE_SSL is not defined */
static void
test_mongoc_client_ssl_disabled (void)
{
   capture_logs (true);
   ASSERT (NULL == test_framework_client_new ("mongodb://host/?ssl=true", NULL));
   ASSERT_CAPTURED_LOG ("mongoc_client_new", MONGOC_LOG_LEVEL_ERROR, "SSL not enabled in this build.");
   capture_logs (false);
}
#endif


static void
_test_mongoc_client_get_description (bool pooled)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   uint32_t server_id;
   mongoc_server_description_t *sd;
   mongoc_host_list_t host;

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
   }

   /* bad server_id handled correctly */
   ASSERT (NULL == mongoc_client_get_server_description (client, 1234));

   collection = get_test_collection (client, "test_mongoc_client_description");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), NULL, NULL);
   ASSERT (!mongoc_cursor_next (cursor, &doc));
   server_id = mongoc_cursor_get_hint (cursor);
   ASSERT (0 != server_id);
   sd = mongoc_client_get_server_description (client, server_id);
   ASSERT (sd);
   mongoc_cursor_get_host (cursor, &host);
   ASSERT (_mongoc_host_list_compare_one (&host, mongoc_server_description_host (sd)));

   mongoc_server_description_destroy (sd);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
}


static void
test_mongoc_client_get_description_single (void)
{
   _test_mongoc_client_get_description (false);
}


static void
test_mongoc_client_get_description_pooled (void)
{
   _test_mongoc_client_get_description (true);
}

static void
test_mongoc_client_descriptions_single (void)
{
   mongoc_client_t *client;
   mongoc_server_description_t **sds;
   size_t n, expected_n;
   bson_error_t error;
   bool r;
   bson_t *ping = tmp_bson ("{'ping': 1}");

   expected_n = test_framework_server_count ();
   n = 0;

   /*
    * single-threaded
    */
   client = test_framework_new_default_client ();

   /* before connecting */
   sds = mongoc_client_get_server_descriptions (client, &n);
   ASSERT_CMPSIZE_T (n, ==, (size_t) 0);
   bson_free (sds);

   /* connect */
   r = mongoc_client_command_simple (client, "db", ping, NULL, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   sds = mongoc_client_get_server_descriptions (client, &n);
   ASSERT_CMPSIZE_T (n, ==, expected_n);

   mongoc_server_descriptions_destroy_all (sds, n);
   mongoc_client_destroy (client);
}

static void
test_mongoc_client_descriptions_pooled (void *unused)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;
   mongoc_server_description_t **sds;
   size_t n, expected_n;
   int64_t start;

   BSON_UNUSED (unused);

   expected_n = test_framework_server_count ();
   n = 0;

   /*
    * pooled
    */
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);

   /* wait for background thread to discover all members */
   start = bson_get_monotonic_time ();
   do {
      _mongoc_usleep (1000);
      /* Windows IPv4 tasks may take longer to connect since connection to the
       * first address returned by getaddrinfo may be IPv6, and failure to
       * connect may take a couple seconds. See CDRIVER-3639. */
      if (bson_get_monotonic_time () - start > 3 * 1000 * 1000) {
         test_error ("still have %d descriptions, not expected %d, after 1 sec", (int) n, (int) expected_n);
      }

      sds = mongoc_client_get_server_descriptions (client, &n);
      mongoc_server_descriptions_destroy_all (sds, n);
   } while (n != expected_n);

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}


static bool
is_standalone_or_rs_primary_or_mongos (const char *server_type)
{
   BSON_ASSERT_PARAM (server_type);
   // Avoid -Woverlength-strings in ASSERT macros.
   return strcmp (server_type, "Standalone") == 0 || strcmp (server_type, "RSPrimary") == 0 ||
          strcmp (server_type, "Mongos") == 0;
}

static bool
is_standalone_or_rs_secondary_or_mongos (const char *server_type)
{
   BSON_ASSERT_PARAM (server_type);
   // Avoid -Woverlength-strings in ASSERT macros.
   return strcmp (server_type, "Standalone") == 0 || strcmp (server_type, "RSSecondary") == 0 ||
          strcmp (server_type, "Mongos") == 0;
}

static bool
is_standalone_or_mongos (const char *server_type)
{
   BSON_ASSERT_PARAM (server_type);
   // Avoid -Woverlength-strings in ASSERT macros.
   return strcmp (server_type, "Standalone") == 0 || strcmp (server_type, "Mongos") == 0;
}


static void
_test_mongoc_client_select_server (bool pooled)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_server_description_t *sd;
   const char *server_type;
   bson_error_t error;
   mongoc_read_prefs_t *prefs;

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
   }

   sd = mongoc_client_select_server (client,
                                     true, /* for writes */
                                     NULL,
                                     &error);

   ASSERT (sd);
   server_type = mongoc_server_description_type (sd);
   ASSERT (is_standalone_or_rs_primary_or_mongos (server_type));

   /* Do not inherit read prefs from the client. */
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_client_set_read_prefs (client, prefs);

   mongoc_server_description_destroy (sd);
   sd = mongoc_client_select_server (client,
                                     true, /* for writes */
                                     NULL,
                                     &error);

   ASSERT (sd);
   server_type = mongoc_server_description_type (sd);
   ASSERT (is_standalone_or_rs_primary_or_mongos (server_type));

   mongoc_server_description_destroy (sd);
   sd = mongoc_client_select_server (client,
                                     false, /* for reads */
                                     NULL,
                                     &error);

   ASSERT (sd);
   server_type = mongoc_server_description_type (sd);
   ASSERT (is_standalone_or_rs_primary_or_mongos (server_type));

   mongoc_server_description_destroy (sd);
   sd = mongoc_client_select_server (client,
                                     false, /* for reads */
                                     prefs,
                                     &error);

   ASSERT (sd);
   server_type = mongoc_server_description_type (sd);
   ASSERT (is_standalone_or_rs_secondary_or_mongos (server_type));

   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (prefs);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
}


static void
test_mongoc_client_select_server_single (void)
{
   _test_mongoc_client_select_server (false);
}


static void
test_mongoc_client_select_server_pooled (void)
{
   _test_mongoc_client_select_server (true);
}


static void
_test_mongoc_client_select_server_error (bool pooled)
{
   mongoc_uri_t *uri = NULL;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_server_description_t *sd;
   bson_error_t error;
   mongoc_read_prefs_t *prefs;
   mongoc_topology_description_type_t tdtype;
   const char *server_type;

   if (pooled) {
      uri = test_framework_get_uri ();
      mongoc_uri_set_option_as_int32 (uri, "serverSelectionTimeoutMS", 3000);
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      test_framework_set_pool_ssl_opts (pool);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
      test_framework_set_ssl_opts (client);
   }

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_read_prefs_set_tags (prefs, tmp_bson ("[{'does-not-exist': 'x'}]"));
   sd = mongoc_client_select_server (client,
                                     true, /* for writes */
                                     prefs,
                                     &error);

   ASSERT (!sd);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "Cannot use read preference");

   sd = mongoc_client_select_server (client,
                                     false, /* for reads */
                                     prefs,
                                     &error);

   /* Server Selection Spec: "With topology type Single, the single server is
    * always suitable for reads if it is available." */
   tdtype = _mongoc_topology_get_type (client->topology);
   if (tdtype == MONGOC_TOPOLOGY_SINGLE || tdtype == MONGOC_TOPOLOGY_SHARDED) {
      ASSERT (sd);
      server_type = mongoc_server_description_type (sd);
      ASSERT (is_standalone_or_mongos (server_type));
      mongoc_server_description_destroy (sd);
   } else {
      ASSERT (!sd);
      ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_SERVER_SELECTION);
      ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_SERVER_SELECTION_FAILURE);
   }

   mongoc_read_prefs_destroy (prefs);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
      mongoc_uri_destroy (uri);
   } else {
      mongoc_client_destroy (client);
   }
}


static void
test_mongoc_client_select_server_error_single (void)
{
   _test_mongoc_client_select_server_error (false);
}


static void
test_mongoc_client_select_server_error_pooled (void)
{
   _test_mongoc_client_select_server_error (true);
}


/* CDRIVER-2172: in single mode, if the selected server has a socket that's been
 * idle for socketCheckIntervalMS, check it with ping. If it fails, retry once.
 */
static void
_test_mongoc_client_select_server_retry (bool retry_succeeds)
{
   char *hello;
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   bson_error_t error;
   request_t *request;
   future_t *future;
   mongoc_server_description_t *sd;

   server = mock_server_new ();
   mock_server_run (server);
   hello = bson_strdup_printf ("{'ok': 1,"
                               " 'isWritablePrimary': true,"
                               " 'secondary': false,"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d,"
                               " 'setName': 'rs',"
                               " 'hosts': ['%s']}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX,
                               mock_server_get_host_and_port (server));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   mongoc_uri_set_option_as_int32 (uri, "socketCheckIntervalMS", 50);
   client = test_framework_client_new_from_uri (uri, NULL);

   /* first selection succeeds */
   future = future_client_select_server (client, true, NULL, &error);
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, hello);
   request_destroy (request);
   sd = future_get_mongoc_server_description_ptr (future);
   ASSERT_OR_PRINT (sd, error);

   future_destroy (future);
   mongoc_server_description_destroy (sd);

   /* let socketCheckIntervalMS pass */
   _mongoc_usleep (100 * 1000);

   /* second selection requires ping, which fails */
   future = future_client_select_server (client, true, NULL, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));

   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* mongoc_client_select_server retries once */
   request = mock_server_receives_any_hello (server);
   if (retry_succeeds) {
      reply_to_request_simple (request, hello);
      sd = future_get_mongoc_server_description_ptr (future);
      ASSERT_OR_PRINT (sd, error);
      mongoc_server_description_destroy (sd);
   } else {
      reply_to_request_with_hang_up (request);
      sd = future_get_mongoc_server_description_ptr (future);
      BSON_ASSERT (sd == NULL);
   }

   future_destroy (future);
   request_destroy (request);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   bson_free (hello);
   mock_server_destroy (server);
}


static void
test_mongoc_client_select_server_retry_succeed (void)
{
   _test_mongoc_client_select_server_retry (true);
}

static void
test_mongoc_client_select_server_retry_fail (void)
{
   _test_mongoc_client_select_server_retry (false);
}


/* CDRIVER-2172: in single mode, if the selected server has a socket that's been
 * idle for socketCheckIntervalMS, check it with ping. If it fails, retry once.
 */
static void
_test_mongoc_client_fetch_stream_retry (bool retry_succeeds)
{
   char *hello;
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   bson_error_t error;
   request_t *request;
   future_t *future;

   server = mock_server_new ();
   mock_server_run (server);
   hello = bson_strdup_printf ("{'ok': 1,"
                               " 'isWritablePrimary': true,"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "socketCheckIntervalMS", 50);
   client = test_framework_client_new_from_uri (uri, NULL);

   /* first time succeeds */
   future = future_client_command_simple (client, "db", tmp_bson ("{'cmd': 1}"), NULL, NULL, &error);
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, hello);
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'cmd': 1}"));
   reply_to_request_with_ok_and_destroy (request);

   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* let socketCheckIntervalMS pass */
   _mongoc_usleep (100 * 1000);

   /* second selection requires ping, which fails */
   future = future_client_command_simple (client, "db", tmp_bson ("{'cmd': 1}"), NULL, NULL, &error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));

   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* mongoc_client_select_server retries once */
   request = mock_server_receives_any_hello (server);
   if (retry_succeeds) {
      reply_to_request_simple (request, hello);
      request_destroy (request);

      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'cmd': 1}"));

      reply_to_request_simple (request, "{'ok': 1}");
      ASSERT_OR_PRINT (future_get_bool (future), error);
   } else {
      reply_to_request_with_hang_up (request);
      BSON_ASSERT (!future_get_bool (future));
   }

   future_destroy (future);
   request_destroy (request);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   bson_free (hello);
   mock_server_destroy (server);
}


static void
test_mongoc_client_fetch_stream_retry_succeed (void)
{
   _test_mongoc_client_fetch_stream_retry (true);
}

static void
test_mongoc_client_fetch_stream_retry_fail (void)
{
   _test_mongoc_client_fetch_stream_retry (false);
}


#if defined(MONGOC_ENABLE_SSL_OPENSSL) || defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
static bool
_cmd (mock_server_t *server, mongoc_client_t *client, bool server_replies, bson_error_t *error)
{
   future_t *future;
   request_t *request;
   bool r;

   ASSERT (client);

   future = future_client_command_simple (client, "db", tmp_bson ("{'cmd': 1}"), NULL, NULL, error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'cmd': 1}"));
   ASSERT (request);

   if (server_replies) {
      reply_to_request_simple (request, "{'ok': 1}");
   }

   r = future_get_bool (future);

   future_destroy (future);
   request_destroy (request);

   return r;
}

static void
test_client_set_ssl_copies_args (bool pooled)
{
   mock_server_t *server;
   mongoc_ssl_opt_t client_opts = {0};
   mongoc_ssl_opt_t server_opts = {0};
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bson_error_t error;
   char *mutable_client_ca = NULL;
   const size_t ca_bufsize = strlen (CERT_CA) + 1;

   mutable_client_ca = bson_malloc (ca_bufsize);
   bson_strncpy (mutable_client_ca, CERT_CA, ca_bufsize);

   client_opts.ca_file = mutable_client_ca;

   server_opts.weak_cert_validation = true;
   server_opts.ca_file = CERT_CA;
   server_opts.pem_file = CERT_SERVER;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_set_ssl_opts (server, &server_opts);
   mock_server_run (server);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (mock_server_get_uri (server), NULL);
      mongoc_client_pool_set_ssl_opts (pool, &client_opts);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
      mongoc_client_set_ssl_opts (client, &client_opts);
   }

   /* Now change the client ca string to be something else */
   bson_strncpy (mutable_client_ca, "garbage", ca_bufsize);

   ASSERT_OR_PRINT (_cmd (server, client, true /* server replies */, &error), error);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   bson_free (mutable_client_ca);
   mock_server_destroy (server);
}

static void
test_ssl_client_single_copies_args (void)
{
   test_client_set_ssl_copies_args (false);
}


static void
test_ssl_client_pooled_copies_args (void)
{
   test_client_set_ssl_copies_args (true);
}


static void
_test_ssl_reconnect (bool pooled)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_ssl_opt_t client_opts = {0};
   mongoc_ssl_opt_t server_opts = {0};
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bson_error_t error;
   future_t *future;

   client_opts.ca_file = CERT_CA;

   server_opts.weak_cert_validation = true;
   server_opts.ca_file = CERT_CA;
   server_opts.pem_file = CERT_SERVER;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_set_ssl_opts (server, &server_opts);
   mock_server_run (server);

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "socketTimeoutMS", 1000);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      mongoc_client_pool_set_ssl_opts (pool, &client_opts);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      mongoc_client_set_ssl_opts (client, &client_opts);
   }

   ASSERT_OR_PRINT (_cmd (server, client, true /* server replies */, &error), error);

   /* man-in-the-middle: certificate changed, for example expired */
   server_opts.pem_file = CERT_EXPIRED;
   mock_server_set_ssl_opts (server, &server_opts);

   /* network timeout */
   capture_logs (true);
   ASSERT (!_cmd (server, client, false /* server hangs up */, &error));
   if (pooled) {
      ASSERT_CAPTURED_LOG ("failed to write data because server closed the connection",
                           MONGOC_LOG_LEVEL_WARNING,
                           "Failed to buffer 4 bytes");
   }
   capture_logs (false);

   /* next operation comes on a new connection, server verification fails */
   capture_logs (true);
   future = future_client_command_simple (client, "db", tmp_bson ("{'cmd': 1}"), NULL, NULL, &error);
   ASSERT (!future_get_bool (future));
   ASSERT_CONTAINS (error.message, "TLS handshake failed");
   if (pooled) {
      ASSERT_CAPTURED_LOG ("cluster", MONGOC_LOG_LEVEL_WARNING, "TLS handshake failed");
   }
   capture_logs (false);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   future_destroy (future);
   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
}


static void
test_ssl_reconnect_single (void)
{
   _test_ssl_reconnect (false);
}


static void
test_ssl_reconnect_pooled (void)
{
   _test_ssl_reconnect (true);
}
#endif /* OpenSSL or Secure Transport */


static void
test_mongoc_client_application_handshake (void)
{
   enum { BUFFER_SIZE = HANDSHAKE_MAX_SIZE };
   char big_string[BUFFER_SIZE];
   const char *short_string = "hallo thar";
   mongoc_client_t *client;

   client = test_framework_client_new ("mongodb://example", NULL);

   memset (big_string, 'a', BUFFER_SIZE - 1);
   big_string[BUFFER_SIZE - 1] = '\0';

   /* Check that setting too long a name causes failure */
   capture_logs (true);
   ASSERT (!mongoc_client_set_appname (client, big_string));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "is invalid");
   clear_captured_logs ();

   /* Success case */
   ASSERT (mongoc_client_set_appname (client, short_string));

   /* Make sure we can't set it twice */
   ASSERT (!mongoc_client_set_appname (client, "a"));
   ASSERT_CAPTURED_LOG (
      "_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "Cannot set appname more than once");
   capture_logs (false);

   mongoc_client_destroy (client);
}

static void
_assert_hello_valid (request_t *request, bool needs_meta)
{
   const bson_t *request_doc;

   ASSERT (request);
   request_doc = request_get_doc (request, 0);
   ASSERT (request_doc);
   ASSERT (bson_has_field (request_doc, HANDSHAKE_FIELD) == needs_meta);
}

/* For single threaded clients, to cause a hello to be sent, we must wait
 * until we're overdue for a heartbeat, and then execute some command */
static future_t *
_force_hello_with_ping (mongoc_client_t *client, int heartbeat_ms)
{
   future_t *future;

   BSON_ASSERT_PARAM (client);

   /* Wait until we're overdue to send a hello */
   _mongoc_usleep (heartbeat_ms * 2 * 1000);

   /* Send a ping */
   future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);
   ASSERT (future);
   return future;
}

/* Call after we've dealt with the hello sent by
 * _force_hello_with_ping */
static void
_respond_to_ping (future_t *future, mock_server_t *server)
{
   request_t *request;

   ASSERT (future);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));

   reply_to_request_with_ok_and_destroy (request);

   ASSERT (future_get_bool (future));
   future_destroy (future);
}

static void
test_mongoc_handshake_pool (void)
{
   mock_server_t *server;
   request_t *request1;
   request_t *request2;
   mongoc_uri_t *uri;
   mongoc_client_t *client1;
   mongoc_client_t *client2;
   mongoc_client_pool_t *pool;
   const char *const server_reply = tmp_str ("{'ok': 1,"
                                             " '" HANDSHAKE_RESPONSE_LEGACY_HELLO "': true,"
                                             " 'minWireVersion': %d,"
                                             " 'maxWireVersion': %d}",
                                             WIRE_VERSION_MIN,
                                             WIRE_VERSION_MAX);
   future_t *future;

   server = mock_server_new ();
   mock_server_run (server);

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   ASSERT (mongoc_uri_set_appname (uri, BSON_FUNC));

   pool = mongoc_client_pool_new (uri);

   client1 = mongoc_client_pool_pop (pool);
   request1 = mock_server_receives_legacy_hello (server, NULL);
   _assert_hello_valid (request1, true);
   reply_to_request_simple (request1, server_reply);
   request_destroy (request1);

   client2 = mongoc_client_pool_pop (pool);
   future = future_client_command_simple (client2, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);

   request2 = mock_server_receives_legacy_hello (server, NULL);
   _assert_hello_valid (request2, true);
   reply_to_request_simple (request2, server_reply);
   request_destroy (request2);

   request2 = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'test'}"));
   reply_to_request_with_ok_and_destroy (request2);
   ASSERT (future_get_bool (future));
   future_destroy (future);

   mongoc_client_pool_push (pool, client1);
   mongoc_client_pool_push (pool, client2);

   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
_test_client_sends_handshake (bool pooled)
{
   request_t *request = NULL;
   future_t *future = NULL;
   mongoc_client_t *client = NULL;
   mongoc_client_pool_t *pool = NULL;
   const char *const server_reply = tmp_str ("{'ok': 1,"
                                             " 'isWritablePrimary': true,"
                                             " 'minWireVersion': %d,"
                                             " 'maxWireVersion': %d}",
                                             WIRE_VERSION_MIN,
                                             WIRE_VERSION_MAX);
   const int heartbeat_ms = 500;

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);
   mongoc_uri_t *const uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", heartbeat_ms);
   mongoc_uri_set_option_as_int32 (uri, "connectTimeoutMS", 100);

   if (pooled) {
      pool = mongoc_client_pool_new (uri);

      /* Pop a client to trigger the topology scanner */
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      future = _force_hello_with_ping (client, heartbeat_ms);
   }

   request = mock_server_receives_any_hello (server);

   /* Make sure the hello request has a "client" field: */
   _assert_hello_valid (request, true);
   reply_to_request_simple (request, server_reply);
   request_destroy (request);

   if (!pooled) {
      _respond_to_ping (future, server);

      /* Wait until another hello is sent */
      future = _force_hello_with_ping (client, heartbeat_ms);
   }

   request = mock_server_receives_any_hello (server);
   _assert_hello_valid (request, false);

   reply_to_request_simple (request, server_reply);
   request_destroy (request);

   if (!pooled) {
      _respond_to_ping (future, server);
      future = _force_hello_with_ping (client, heartbeat_ms);
   }

   /* Now wait for the client to send another hello command, but this
    * time the server hangs up */
   request = mock_server_receives_any_hello (server);
   _assert_hello_valid (request, false);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Client retries once (CDRIVER-2075) */
   request = mock_server_receives_any_hello (server);
   _assert_hello_valid (request, true);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   if (!pooled) {
      /* The ping wasn't sent since we hung up with hello */
      ASSERT (!future_get_bool (future));
      future_destroy (future);

      /* We're in cooldown for the next few seconds, so we're not
       * allowed to send hellos. Wait for the cooldown to end. */
      _mongoc_usleep ((MONGOC_TOPOLOGY_COOLDOWN_MS + 1000) * 1000);
      future = _force_hello_with_ping (client, heartbeat_ms);
   }

   /* Now the client should try to reconnect. They think the server's down
    * so now they SHOULD send hello */
   request = mock_server_receives_any_hello (server);
   _assert_hello_valid (request, true);

   reply_to_request_simple (request, server_reply);
   request_destroy (request);

   if (!pooled) {
      _respond_to_ping (future, server);
   }

   /* cleanup */
   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
test_client_sends_handshake_single (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_client_sends_handshake (false);
}

static void
test_client_sends_handshake_pooled (void)
{
   _test_client_sends_handshake (true);
}

static void
test_client_appname (bool pooled, bool use_uri)
{
   future_t *future = NULL;
   mongoc_client_t *client = NULL;
   mongoc_client_pool_t *pool = NULL;
   const char *const server_reply = tmp_str ("{'ok': 1,"
                                             " 'isWritablePrimary': true,"
                                             " 'minWireVersion': %d,"
                                             " 'maxWireVersion': %d}",
                                             WIRE_VERSION_MIN,
                                             WIRE_VERSION_MAX);
   const int heartbeat_ms = 500;

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);
   mongoc_uri_t *const uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", heartbeat_ms);
   mongoc_uri_set_option_as_int32 (uri, "connectTimeoutMS", 120 * 1000);

   if (use_uri) {
      mongoc_uri_set_option_as_utf8 (uri, "appname", "testapp");
   }

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      if (!use_uri) {
         ASSERT (mongoc_client_pool_set_appname (pool, "testapp"));
      }
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      if (!use_uri) {
         ASSERT (mongoc_client_set_appname (client, "testapp"));
      }
      future = _force_hello_with_ping (client, heartbeat_ms);
   }

   request_t *const request = mock_server_receives_any_hello_with_match (server,
                                                                         "{'client': {"
                                                                         "    'application': {"
                                                                         "       'name': 'testapp'}}}",
                                                                         "{'client': {"
                                                                         "    'application': {"
                                                                         "       'name': 'testapp'}}}");

   reply_to_request_simple (request, server_reply);
   if (!pooled) {
      _respond_to_ping (future, server);
   }

   request_destroy (request);

   /* cleanup */
   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
test_client_appname_single_uri (void)
{
   test_client_appname (false, true);
}

static void
test_client_appname_single_no_uri (void)
{
   test_client_appname (false, false);
}

static void
test_client_appname_pooled_uri (void)
{
   test_client_appname (true, true);
}

static void
test_client_appname_pooled_no_uri (void)
{
   test_client_appname (true, false);
}

/* test a disconnect with a NULL bson_error_t * passed to command_simple() */
static void
_test_null_error_pointer (bool pooled)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   future_t *future;
   request_t *request;

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "serverSelectionTimeoutMS", 1000);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   /* connect */
   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'test', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT (future_get_bool (future));
   future_destroy (future);

   /* disconnect */
   mock_server_destroy (server);
   if (pooled) {
      mongoc_cluster_disconnect_node (&client->cluster, 1);
   } else {
      mongoc_topology_scanner_node_t *scanner_node;

      scanner_node = mongoc_topology_scanner_get_node (client->topology->scanner, 1);
      mongoc_stream_destroy (scanner_node->stream);
      scanner_node->stream = NULL;
   }

   /* doesn't abort with assertion failure */
   capture_logs (true);
   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL /* error */);
   ASSERT (!future_get_bool (future));
   future_destroy (future);
   if (pooled) {
      ASSERT_CAPTURED_LOG ("cluster", MONGOC_LOG_LEVEL_WARNING, "Failed to connect to target host");
   }
   capture_logs (false);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
}

static void
test_null_error_pointer_single (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_null_error_pointer (false);
}

static void
test_null_error_pointer_pooled (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_null_error_pointer (true);
}

#ifdef MONGOC_ENABLE_SSL
static void
test_set_ssl_opts (void)
{
   const mongoc_ssl_opt_t *opts = mongoc_ssl_opt_get_default ();

   ASSERT (opts->pem_file == NULL);
   ASSERT (opts->pem_pwd == NULL);
   ASSERT (opts->ca_file == NULL);
   ASSERT (opts->ca_dir == NULL);
   ASSERT (opts->crl_file == NULL);
   ASSERT (!opts->weak_cert_validation);
   ASSERT (!opts->allow_invalid_hostname);
}
#endif

static void
test_client_reset_sessions (void)
{
   bson_error_t error;
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_client_session_t *session_lookup;
   future_t *future;
   request_t *request;
   bson_t opts = BSON_INITIALIZER;
   uint32_t csid;
   bson_t lsid;
   bool res;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   ASSERT (client->generation == 0);

   /* Ensure that resetting client removes existing sessions from its set */
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   ASSERT (session->client_generation == client->generation);
   csid = session->client_session_id;

   mongoc_client_reset (client);
   ASSERT (client->generation == 1);

   ASSERT (!_mongoc_client_lookup_session (client, csid, &session_lookup, &error));

   /* Ensure that resetting did not send endSessions. To do this, we wait for
      a ping, so if we receive endSessions instead we will fail. */
   future = future_client_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error);

   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1, 'lsid': {'$exists': true}}"));
   reply_to_request_with_ok_and_destroy (request);

   ASSERT (future_get_bool (future));

   future_destroy (future);

   /* Ensure that a session left over from before the reset call cannot
      be used for any operations. */
   bson_copy_to (mongoc_client_session_get_lsid (session), &lsid);
   res = (mongoc_client_session_append (session, &opts, &error));
   ASSERT_OR_PRINT (res, error);
   future = future_client_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, &opts, NULL, &error);

   ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid sessionId");

   /* Add an autoresponder for endSessions to unblock the test. */
   mock_server_auto_endsessions (server);

   bson_destroy (&opts);
   bson_destroy (&lsid);
   future_destroy (future);
   mongoc_client_session_destroy (session);
   mongoc_client_session_destroy (session_lookup);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static void
test_client_reset_cursors (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_cursor_t *cursor;
   mongoc_database_t *database;
   mongoc_collection_t *coll;
   future_t *future;
   request_t *request;
   bson_error_t error;
   const bson_t *doc;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* Ensure that cursors with an old client generation don't send killCursors.
      This test should timeout and fail if the client does send killCursors. */

   coll = mongoc_client_get_collection (client, "test", "test");
   cursor = mongoc_collection_find (coll, MONGOC_QUERY_NONE, 0, 0, 0, tmp_bson (NULL), NULL, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'test', 'find': 'test'}"));

   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 4,"
                            "    'ns': 'test.test',"
                            "    'firstBatch': [{}]}}");

   BSON_ASSERT (future_get_bool (future));
   ASSERT (cursor->cursor_id);

   mongoc_client_reset (client);

   /* Attempt to call next() on the cursor after a reset--should fail without
      sending any requests to the server. */
   ASSERT (!mongoc_cursor_next (cursor, &doc));
   ASSERT (mongoc_cursor_error (cursor, &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "Cannot advance cursor after client reset");

   mongoc_cursor_destroy (cursor);

   request_destroy (request);
   future_destroy (future);

   /* Expect a ping here, and send one after destroying cursor. If a killCursors
      command intervened, this test will fail. */
   database = mongoc_client_get_database (client, "admin");
   future = future_database_command_simple (database, tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);

   ASSERT (future_get_bool (future));

   future_destroy (future);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
   mongoc_database_destroy (database);
   mock_server_destroy (server);
}

static bool
mongoc_topology_scanner_is_connected (mongoc_topology_scanner_t *scanner)
{
   mongoc_topology_scanner_node_t *node;

   BSON_ASSERT (scanner);
   node = scanner->nodes;

   if (!node) {
      return false;
   }

   while (node) {
      if (!node->stream) {
         return false;
      }

      node = node->next;
   }

   return true;
}

static void
test_client_reset_connections (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_database_t *database;
   mongoc_uri_t *uri;
   future_t *future;
   request_t *request;
   int autoresponder_id;

   server = mock_server_new ();
   autoresponder_id = mock_server_auto_hello (server, "{ 'isWritablePrimary': true }");
   mock_server_run (server);

   /* After calling reset, check that connections are left as-is. Set
      heartbeat frequency high, so a background scan won't interfere. */
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 99999);
   client = test_framework_client_new_from_uri (uri, NULL);

   database = mongoc_client_get_database (client, "admin");
   future = future_database_command_simple (database, tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);

   ASSERT (future_get_bool (future));

   mock_server_remove_autoresponder (server, autoresponder_id);

   ASSERT (mongoc_topology_scanner_is_connected (client->topology->scanner));

   mongoc_client_reset (client);

   ASSERT (mongoc_topology_scanner_is_connected (client->topology->scanner));

   future_destroy (future);
   mongoc_uri_destroy (uri);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_get_database (void)
{
   mongoc_client_t *client;
   mongoc_database_t *database;
   mongoc_write_concern_t *wc;
   mongoc_read_concern_t *rc;
   mongoc_read_prefs_t *read_prefs;

   client = test_framework_client_new (NULL, NULL);

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_client_set_write_concern (client, wc);

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, "majority");
   mongoc_client_set_read_concern (client, rc);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_client_set_read_prefs (client, read_prefs);

   database = mongoc_client_get_database (client, "test");

   ASSERT_CMPINT32 (database->write_concern->w, ==, 2);
   ASSERT_CMPSTR (database->read_concern->level, "majority");
   ASSERT_CMPINT (database->read_prefs->mode, ==, MONGOC_READ_SECONDARY);

   mongoc_database_destroy (database);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_read_concern_destroy (rc);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
}


static void
test_invalid_server_id (void)
{
   mongoc_client_t *client;
   bson_error_t error;
   bool ret;

   client = test_framework_client_new ("mongodb://localhost", NULL);
   ret = mongoc_client_command_simple_with_server_id (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, 123, NULL /* reply */, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Could not find server with id: 123");
   mongoc_client_destroy (client);
}

#ifdef MONGOC_ENABLE_SSL
static void
test_ssl_opts_override (void)
{
   /* Test that mongoc_client_set_ssl_opts does not override the internal TLS
    * options that can only be set through the URI. */
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_ssl_opt_t ssl_opts = {0};

   uri = mongoc_uri_new ("mongodb://localhost:27017/?tls=true&tlsDisableOCSPEndpointCheck=true");
   client = test_framework_client_new_from_uri (uri, NULL);
   ssl_opts.allow_invalid_hostname = true;
   mongoc_client_set_ssl_opts (client, &ssl_opts);
   BSON_ASSERT (client->ssl_opts.allow_invalid_hostname);
   BSON_ASSERT (((_mongoc_internal_tls_opts_t *) client->ssl_opts.internal)->tls_disable_ocsp_endpoint_check);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
}

static void
test_ssl_opts_padding_not_null (void)
{
   mongoc_client_t *client;
   mongoc_ssl_opt_t ssl_opt = {0};

   ssl_opt.allow_invalid_hostname = true;
   ssl_opt.internal = (void *) 123;
   client = test_framework_client_new ("mongodb://localhost:27017", NULL);
   mongoc_client_set_ssl_opts (client, &ssl_opt);
   BSON_ASSERT (client->ssl_opts.internal == NULL);
   mongoc_client_destroy (client);
}
#endif

static void
test_mongoc_client_recv_network_error (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   future_t *future;
   request_t *request;
   bson_error_t error;
   mongoc_server_description_t const *sd;
   int generation;
   mcd_rpc_message *rpc = NULL;
   mongoc_buffer_t buffer;
   mongoc_server_stream_t *stream;
   mc_shared_tpld td;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   request = mock_server_receives_request (server);
   reply_to_request_with_ok_and_destroy (request);
   future_wait (future);
   future_destroy (future);

   /* The server should be a standalone. */
   sd = mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 1, &error);
   ASSERT_OR_PRINT (sd, error);
   generation = mc_tpl_sd_get_generation (sd, &kZeroServiceId);
   BSON_ASSERT (sd->type == MONGOC_SERVER_STANDALONE);
   mock_server_destroy (server);

   /* A network error when calling _mongoc_client_recv should mark the server
    * unknown and increment the generation. */
   _mongoc_buffer_init (
      &buffer, NULL /* initial buffer */, 0 /* initial length */, NULL /* realloc fn */, NULL /* realloc ctx */);
   rpc = mcd_rpc_message_new ();
   stream = mongoc_cluster_stream_for_server (&client->cluster, 1, false, NULL, NULL, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (!_mongoc_client_recv (client, rpc, &buffer, stream, &error));

   td = mc_tpld_take_ref (client->topology);
   sd = mongoc_topology_description_server_by_id_const (td.ptr, 1, &error);
   ASSERT_OR_PRINT (sd, error);
   ASSERT_CMPINT (mc_tpl_sd_get_generation (sd, &kZeroServiceId), ==, generation + 1);
   BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);

   mongoc_client_destroy (client);
   _mongoc_buffer_destroy (&buffer);
   mcd_rpc_message_destroy (rpc);
   mongoc_server_stream_cleanup (stream);
   mc_tpld_drop_ref (&td);
}

void
test_mongoc_client_get_handshake_hello_response_single (void)
{
   mongoc_client_t *client;
   mongoc_server_description_t *monitor_sd;
   mongoc_server_description_t *invalidated_sd;
   mongoc_server_description_t *handshake_sd;
   bson_error_t error = {0};

   client = test_framework_new_default_client ();
   /* Perform server selection to establish a connection. */
   monitor_sd = mongoc_client_select_server (client, false /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (monitor_sd)));

   /* Invalidate the server. */
   _mongoc_topology_invalidate_server (client->topology, monitor_sd->id);

   /* Get the new invalidated server description from monitoring. */
   invalidated_sd = mongoc_client_get_server_description (client, monitor_sd->id);
   BSON_ASSERT (NULL != invalidated_sd);
   ASSERT_CMPSTR ("Unknown", mongoc_server_description_type (invalidated_sd));

   /* The previously established connection should have a valid server
    * description. */
   handshake_sd = mongoc_client_get_handshake_description (client, monitor_sd->id, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (handshake_sd)));

   mongoc_server_description_destroy (handshake_sd);
   mongoc_server_description_destroy (invalidated_sd);
   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_destroy (client);
}

void
test_mongoc_client_get_handshake_hello_response_pooled (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_description_t *monitor_sd;
   mongoc_server_description_t *invalidated_sd;
   mongoc_server_description_t *handshake_sd;
   bson_error_t error = {0};
   bool ret;

   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);
   monitor_sd = mongoc_client_select_server (client, false /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (monitor_sd)));

   /* Send a ping to establish a connection. */
   ret = mongoc_client_command_simple_with_server_id (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL, monitor_sd->id, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Invalidate the server. */
   _mongoc_topology_invalidate_server (client->topology, monitor_sd->id);

   /* Get the new invalidated server description from monitoring. */
   invalidated_sd = mongoc_client_get_server_description (client, monitor_sd->id);
   BSON_ASSERT (NULL != invalidated_sd);

   // Check the resulting server description.
   // Invalidating sets the type to Unknown.
   // A background monitor may have set the type to PossiblePrimary.
   const char *got_description_type = mongoc_server_description_type (invalidated_sd);
   if (0 != strcmp ("Unknown", got_description_type) && 0 != strcmp ("PossiblePrimary", got_description_type)) {
      test_error ("Expected server to have type 'Unknown' or "
                  "'PossiblePrimary', got: '%s'",
                  got_description_type);
   }

   /* The previously established connection should have a valid server
    * description. */
   handshake_sd = mongoc_client_get_handshake_description (client, monitor_sd->id, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (handshake_sd)));

   mongoc_server_description_destroy (handshake_sd);
   mongoc_server_description_destroy (invalidated_sd);
   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}

/* Test that calling mongoc_client_get_handshake_description establishes a
 * connection if a connection has not already been established. */
void
test_mongoc_client_get_handshake_establishes_connection_single (void)
{
   mongoc_client_t *client;
   mongoc_server_description_t *handshake_sd;
   bson_error_t error = {0};
   uint32_t server_id = 1;

   client = test_framework_new_default_client ();

   handshake_sd = mongoc_client_get_handshake_description (client, server_id, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (handshake_sd)));

   mongoc_server_description_destroy (handshake_sd);
   mongoc_client_destroy (client);
}

void
test_mongoc_client_get_handshake_establishes_connection_pooled (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_description_t *handshake_sd;
   bson_error_t error = {0};
   uint32_t server_id = 1;

   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);

   /* The previously established connection should have a valid server
    * description. */
   handshake_sd = mongoc_client_get_handshake_description (client, server_id, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);
   BSON_ASSERT (0 != strcmp ("Unknown", mongoc_server_description_type (handshake_sd)));

   mongoc_server_description_destroy (handshake_sd);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}

/* Regression test for CDRIVER-4207. */
void
test_mongoc_client_resends_handshake_on_network_error (void)
{
   mongoc_client_t *client;
   mock_server_t *server;
   future_t *future;
   request_t *request;
   bson_error_t error;
   bson_t *ping = tmp_bson ("{'ping': 1}");

   server = mock_server_new ();
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   mongoc_client_set_appname (client, "foo");

   /* Send a "ping" command. */
   future = future_client_command_simple (client, "db", ping, NULL /* read_prefs */, NULL /* reply */, &error);
   /* The first command on the new connection is handshake. It uses the legacy
    * hello and includes the client.application.name. */
   request = mock_server_receives_any_hello_with_match (
      server,
      "{'" HANDSHAKE_CMD_HELLO "': 1, 'client': {'application': {'name': 'foo'}}}",
      "{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1, 'client': {'application': {'name': 'foo'}}}");
   reply_to_request_simple (
      request, tmp_str ("{'ok': 1, 'minWireVersion': %d, 'maxWireVersion': %d}", WIRE_VERSION_MIN, WIRE_VERSION_5_1));
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_hang_up (request);
   future_wait (future);
   future_destroy (future);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");
   request_destroy (request);

   /* Send another "ping" command. */
   future = future_client_command_simple (client, "db", ping, NULL /* read_prefs */, NULL /* reply */, &error);
   /* Expect the new connection to send the full handshake. */
   request = mock_server_receives_any_hello_with_match (
      server,
      "{'" HANDSHAKE_CMD_HELLO "': 1, 'client': {'application': {'name': 'foo'}}}",
      "{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1, 'client': {'application': {'name': 'foo'}}}");
   reply_to_request_simple (
      request, tmp_str ("{'ok': 1, 'minWireVersion': %d, 'maxWireVersion': %d }", WIRE_VERSION_MIN, WIRE_VERSION_5_1));
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT (future_get_bool (future));
   future_destroy (future);

   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

// test_failure_to_auth is a regression test for the leak reported in
// CDRIVER-4699.
static void
test_failure_to_auth (void)
{
   mongoc_uri_t *uri = mongoc_uri_new_for_host_port ("localhost", 12345);
   mongoc_uri_set_username (uri, "foo");
   mongoc_uri_set_password (uri, "bar");
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false);
   // Set a shorter serverSelectionTimeoutMS for a faster test.
   // serverSelectionTimeoutMS must be long enough to require a second attempt
   // of authentication. Experimentally: 100ms appears long enough to reproduce
   // leak reported in CDRIVER-4699.
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 100);
   mongoc_client_t *client = mongoc_client_new_from_uri (uri);

   // Override minHeartbeatFrequencyMS to reduce the time between server checks.
   client->topology->min_heartbeat_frequency_msec = 1;

   // Disable the cooldown period to reduce the time between server checks.
   // Single threaded clients wait for a default 5 second cooldown period after
   // failing to connect to a server before making another attempt.
   _mongoc_topology_bypass_cooldown (client->topology);

   bool ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, NULL /* error */);
   ASSERT_WITH_MSG (!ok, "expected command to fail, got success");
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
}
void
test_client_install (TestSuite *suite)
{
   if (test_framework_getenv_bool ("MONGOC_CHECK_IPV6")) {
      /* try to validate ipv6 too */
      TestSuite_AddLive (suite, "/Client/ipv6/single", test_mongoc_client_ipv6_single);

      /* try to validate ipv6 too */
      TestSuite_AddLive (suite, "/Client/ipv6/single", test_mongoc_client_ipv6_pooled);
   }

   TestSuite_AddFull (
      suite, "/Client/authenticate", test_mongoc_client_authenticate, NULL, NULL, test_framework_skip_if_no_auth);
   TestSuite_AddFull (suite,
                      "/Client/speculative_auth_failure/single",
                      test_mongoc_client_single_speculative_auth_failure,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_no_failpoint);
   TestSuite_AddFull (suite,
                      "/Client/speculative_auth_failure/pooled",
                      test_mongoc_client_pooled_speculative_auth_failure,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_no_failpoint);
   TestSuite_AddFull (suite,
                      "/Client/authenticate_cached/pool",
                      test_mongoc_client_authenticate_cached_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth);
   TestSuite_AddFull (suite,
                      "/Client/authenticate_cached/client",
                      test_mongoc_client_authenticate_cached_single,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      // speculativeAuthentication in single-threaded clients
                      // does not use the scram cache. speculativeAuthentication
                      // was introduced in server 4.4 (maxWireVersion=9)
                      test_framework_skip_if_max_wire_version_more_than_8);
   TestSuite_AddFull (suite,
                      "/Client/authenticate_failure",
                      test_mongoc_client_authenticate_failure,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth);
   TestSuite_AddFull (suite,
                      "/Client/authenticate_timeout",
                      test_mongoc_client_authenticate_timeout,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth);
   TestSuite_AddLive (suite, "/Client/command", test_mongoc_client_command);
   TestSuite_AddLive (suite, "/Client/command_defaults", test_mongoc_client_command_defaults);
   TestSuite_AddLive (suite, "/Client/command_secondary", test_mongoc_client_command_secondary);
   TestSuite_AddMockServerTest (suite, "/Client/command_w_server_id", test_client_cmd_w_server_id);
   TestSuite_AddMockServerTest (suite, "/Client/command_w_server_id/sharded", test_client_cmd_w_server_id_sharded);
   TestSuite_AddFull (
      suite, "/Client/command_w_server_id/option", test_server_id_option, NULL, NULL, test_framework_skip_if_auth);
   TestSuite_AddFull (
      suite, "/Client/command_w_write_concern", test_client_cmd_w_write_concern, NULL, NULL, TestSuite_CheckLive);
   TestSuite_AddMockServerTest (suite, "/Client/command/write_concern", test_client_cmd_write_concern);
   TestSuite_AddMockServerTest (suite, "/Client/command/write_concern_fam", test_client_cmd_write_concern_fam);
   TestSuite_AddMockServerTest (
      suite, "/Client/command/read_prefs/simple/single", test_command_simple_read_prefs_single);
   TestSuite_AddMockServerTest (
      suite, "/Client/command/read_prefs/simple/pooled", test_command_simple_read_prefs_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/command/read_prefs/single", test_command_read_prefs_single);
   TestSuite_AddMockServerTest (suite, "/Client/command/read_prefs/pooled", test_command_read_prefs_pooled);
   TestSuite_AddLive (suite, "/Client/command_not_found/cursor", test_command_not_found);
   TestSuite_AddLive (suite, "/Client/command_not_found/simple", test_command_not_found_simple);
   TestSuite_AddMockServerTest (suite, "/Client/command_with_opts/read_prefs", test_command_with_opts_read_prefs);
   TestSuite_AddMockServerTest (suite, "/Client/command_with_opts/read_write", test_read_write_cmd_with_opts);
   TestSuite_AddMockServerTest (suite, "/Client/command_with_opts", test_command_with_opts);
   TestSuite_AddMockServerTest (suite, "/Client/command_with_opts/op_msg", test_command_with_opts_op_msg);
   TestSuite_AddMockServerTest (suite, "/Client/command_with_opts/read", test_read_command_with_opts);
   TestSuite_AddLive (suite, "/Client/command/empty", test_command_empty);
   TestSuite_AddMockServerTest (suite, "/Client/command/no_errmsg", test_command_no_errmsg);
   TestSuite_AddMockServerTest (suite, "/Client/unavailable_seeds", test_unavailable_seeds);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_no_connect/single", test_rs_seeds_no_connect_single);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_no_connect/pooled", test_rs_seeds_no_connect_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_connect/single", test_rs_seeds_connect_single);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_connect/pooled", test_rs_seeds_connect_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_reconnect/single", test_rs_seeds_reconnect_single);
   TestSuite_AddMockServerTest (suite, "/Client/rs_seeds_reconnect/pooled", test_rs_seeds_reconnect_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_no_connect/single", test_mongos_seeds_no_connect_single);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_no_connect/pooled", test_mongos_seeds_no_connect_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_connect/single", test_mongos_seeds_connect_single);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_connect/pooled", test_mongos_seeds_connect_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_reconnect/single", test_mongos_seeds_reconnect_single);
   TestSuite_AddMockServerTest (suite, "/Client/mongos_seeds_reconnect/pooled", test_mongos_seeds_reconnect_pooled);
   TestSuite_AddFull (suite, "/Client/recovering", test_recovering, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddLive (suite, "/Client/server_status", test_server_status);
   TestSuite_AddMockServerTest (suite, "/Client/database_names", test_get_database_names);
   TestSuite_AddFull (
      suite, "/Client/connect/uds", test_mongoc_client_unix_domain_socket, NULL, NULL, test_framework_skip_if_no_uds);
   TestSuite_AddMockServerTest (suite, "/Client/mismatched_me", test_mongoc_client_mismatched_me);

   TestSuite_AddMockServerTest (suite, "/Client/handshake/pool", test_mongoc_handshake_pool);
   TestSuite_Add (suite, "/Client/application_handshake", test_mongoc_client_application_handshake);
   TestSuite_AddFull (suite,
                      "/Client/sends_handshake_single",
                      test_client_sends_handshake_single,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_Add (suite, "/Client/sends_handshake_pooled", test_client_sends_handshake_pooled);
   TestSuite_AddMockServerTest (suite, "/Client/appname_single_uri", test_client_appname_single_uri);
   TestSuite_AddMockServerTest (suite, "/Client/appname_single_no_uri", test_client_appname_single_no_uri);
   TestSuite_AddMockServerTest (suite, "/Client/appname_pooled_uri", test_client_appname_pooled_uri);
   TestSuite_AddMockServerTest (suite, "/Client/appname_pooled_no_uri", test_client_appname_pooled_no_uri);
   TestSuite_AddMockServerTest (suite, "/Client/wire_version", test_wire_version);
#ifdef MONGOC_ENABLE_SSL
   TestSuite_AddLive (suite, "/Client/ssl_opts/single", test_ssl_single);
   TestSuite_AddLive (suite, "/Client/ssl_opts/pooled", test_ssl_pooled);
   TestSuite_Add (suite, "/Client/set_ssl_opts", test_set_ssl_opts);
   TestSuite_Add (suite, "/Client/ssl_opts_override", test_ssl_opts_override);
   TestSuite_Add (suite, "/Client/ssl_opts_padding_not_null/single", test_ssl_opts_padding_not_null);
   TestSuite_AddLive (suite, "/Client/ssl_hang", test_client_buildinfo_hang);

#if defined(MONGOC_ENABLE_SSL_OPENSSL) || defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
   TestSuite_AddMockServerTest (suite, "/Client/ssl_opts/copies_single", test_ssl_client_single_copies_args);
   TestSuite_AddMockServerTest (suite, "/Client/ssl_opts/copies_pooled", test_ssl_client_pooled_copies_args);
   TestSuite_AddMockServerTest (suite, "/Client/ssl/reconnect/single", test_ssl_reconnect_single);
   TestSuite_AddMockServerTest (suite, "/Client/ssl/reconnect/pooled", test_ssl_reconnect_pooled);

#endif
#else
   /* No SSL support at all */
   TestSuite_Add (suite, "/Client/ssl_disabled", test_mongoc_client_ssl_disabled);
#endif

   TestSuite_AddMockServerTest (
      suite, "/Client/client_reset/sessions", test_client_reset_sessions, test_framework_skip_if_no_crypto);

   TestSuite_AddMockServerTest (suite, "/Client/client_reset/cursors", test_client_reset_cursors);
   TestSuite_AddMockServerTest (suite, "/Client/client_reset/connections", test_client_reset_connections);

   TestSuite_AddLive (suite, "/Client/get_description/single", test_mongoc_client_get_description_single);
   TestSuite_AddLive (suite, "/Client/get_description/pooled", test_mongoc_client_get_description_pooled);
   TestSuite_AddLive (suite, "/Client/descriptions/single", test_mongoc_client_descriptions_single);
   TestSuite_AddFull (
      suite, "/Client/descriptions/pooled", test_mongoc_client_descriptions_pooled, NULL, NULL, TestSuite_CheckLive);
   TestSuite_AddLive (suite, "/Client/select_server/single", test_mongoc_client_select_server_single);
   TestSuite_AddLive (suite, "/Client/select_server/pooled", test_mongoc_client_select_server_pooled);
   TestSuite_AddLive (suite, "/Client/select_server/err/single", test_mongoc_client_select_server_error_single);
   TestSuite_AddLive (suite, "/Client/select_server/err/pooled", test_mongoc_client_select_server_error_pooled);
   TestSuite_AddMockServerTest (
      suite, "/Client/select_server/retry/succeed", test_mongoc_client_select_server_retry_succeed);
   TestSuite_AddMockServerTest (suite, "/Client/select_server/retry/fail", test_mongoc_client_select_server_retry_fail);
   TestSuite_AddMockServerTest (
      suite, "/Client/fetch_stream/retry/succeed", test_mongoc_client_fetch_stream_retry_succeed);
   TestSuite_AddMockServerTest (suite, "/Client/fetch_stream/retry/fail", test_mongoc_client_fetch_stream_retry_fail);
   TestSuite_AddFull (suite,
                      "/Client/null_error_pointer/single",
                      test_null_error_pointer_single,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Client/null_error_pointer/pooled",
                      test_null_error_pointer_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_Add (suite, "/Client/get_database", test_get_database);
   TestSuite_Add (suite, "/Client/invalid_server_id", test_invalid_server_id);
   TestSuite_AddMockServerTest (suite, "/Client/recv_network_error", test_mongoc_client_recv_network_error);
   TestSuite_AddLive (
      suite, "/Client/get_handshake_hello_response/single", test_mongoc_client_get_handshake_hello_response_single);
   TestSuite_AddLive (
      suite, "/Client/get_handshake_hello_response/pooled", test_mongoc_client_get_handshake_hello_response_pooled);
   TestSuite_AddLive (suite,
                      "/Client/get_handshake_establishes_connection/single",
                      test_mongoc_client_get_handshake_establishes_connection_single);
   TestSuite_AddLive (suite,
                      "/Client/get_handshake_establishes_connection/pooled",
                      test_mongoc_client_get_handshake_establishes_connection_pooled);
   TestSuite_AddMockServerTest (
      suite, "/Client/resends_handshake_on_network_error", test_mongoc_client_resends_handshake_on_network_error);
   TestSuite_Add (suite, "/Client/failure_to_auth", test_failure_to_auth);
}
