#include <mongoc/mongoc-util-private.h>
#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc-topology-background-monitoring-private.h"
#include "mongoc/mongoc-uri-private.h"

#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "cluster-test"


static uint32_t
server_id_for_reads (mongoc_cluster_t *cluster)
{
   bson_error_t error;
   mongoc_server_stream_t *server_stream;
   uint32_t id;

   server_stream = mongoc_cluster_stream_for_reads (cluster, NULL, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   id = server_stream->sd->id;

   mongoc_server_stream_cleanup (server_stream);

   return id;
}


static void
test_get_max_bson_obj_size (void)
{
   mongoc_server_description_t *sd;
   mongoc_cluster_node_t *node;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   int32_t max_bson_obj_size = 16;
   uint32_t id;
   mc_tpld_modification tdmod;

   /* single-threaded */
   client = test_framework_new_default_client ();
   BSON_ASSERT (client);

   id = server_id_for_reads (&client->cluster);
   tdmod = mc_tpld_modify_begin (client->topology);
   sd = mongoc_set_get (mc_tpld_servers (tdmod.new_td), id);
   sd->max_bson_obj_size = max_bson_obj_size;
   mc_tpld_modify_commit (tdmod);
   BSON_ASSERT (max_bson_obj_size == mongoc_cluster_get_max_bson_obj_size (&client->cluster));

   mongoc_client_destroy (client);

   /* multi-threaded */
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);

   id = server_id_for_reads (&client->cluster);
   node = (mongoc_cluster_node_t *) mongoc_set_get (client->cluster.nodes, id);
   node->handshake_sd->max_bson_obj_size = max_bson_obj_size;
   BSON_ASSERT (max_bson_obj_size == mongoc_cluster_get_max_bson_obj_size (&client->cluster));

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}

static void
test_get_max_msg_size (void)
{
   mongoc_server_description_t *sd;
   mongoc_cluster_node_t *node;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   int32_t max_msg_size = 32;
   uint32_t id;
   mc_tpld_modification tdmod;

   /* single-threaded */
   client = test_framework_new_default_client ();
   id = server_id_for_reads (&client->cluster);

   tdmod = mc_tpld_modify_begin (client->topology);
   sd = mongoc_set_get (mc_tpld_servers (tdmod.new_td), id);
   sd->max_msg_size = max_msg_size;
   mc_tpld_modify_commit (tdmod);
   BSON_ASSERT (max_msg_size == mongoc_cluster_get_max_msg_size (&client->cluster));

   mongoc_client_destroy (client);

   /* multi-threaded */
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);

   id = server_id_for_reads (&client->cluster);
   node = (mongoc_cluster_node_t *) mongoc_set_get (client->cluster.nodes, id);
   node->handshake_sd->max_msg_size = max_msg_size;
   BSON_ASSERT (max_msg_size == mongoc_cluster_get_max_msg_size (&client->cluster));

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}


#define ASSERT_CURSOR_ERR()                                                                                          \
   do {                                                                                                              \
      BSON_ASSERT (!future_get_bool (future));                                                                       \
      BSON_ASSERT (mongoc_cursor_error (cursor, &error));                                                            \
      ASSERT_ERROR_CONTAINS (                                                                                        \
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to read 4 bytes: socket error or timeout"); \
   } while (0)


#define START_QUERY(client_port_variable)                                                      \
   do {                                                                                        \
      cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), NULL, NULL);     \
      future = future_cursor_next (cursor, &doc);                                              \
      request = mock_server_receives_msg (                                                     \
         server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'test', 'find': 'test', 'filter': {}}")); \
      BSON_ASSERT (request);                                                                   \
      client_port_variable = request_get_client_port (request);                                \
   } while (0)


#define CLEANUP_QUERY()               \
   do {                               \
      request_destroy (request);      \
      future_destroy (future);        \
      mongoc_cursor_destroy (cursor); \
   } while (0)


/* test that we reconnect a cluster node after disconnect */
static void
_test_cluster_node_disconnect (bool pooled)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   const bson_t *doc;
   mongoc_cursor_t *cursor;
   future_t *future;
   request_t *request;
   uint16_t client_port_0, client_port_1;
   bson_error_t error;

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   capture_logs (true);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   uri = mongoc_uri_copy (mock_server_get_uri (server));

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   collection = mongoc_client_get_collection (client, "test", "test");

   /* query 0 fails. set client_port_0 to the port used by the query. */
   START_QUERY (client_port_0);

   reply_to_request_with_reset (request);
   ASSERT_CURSOR_ERR ();
   CLEANUP_QUERY ();

   /* query 1 opens a new socket. set client_port_1 to the new port. */
   START_QUERY (client_port_1);
   ASSERT_CMPINT (client_port_1, !=, client_port_0);
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "   'id': 0,"
                            "   'ns': 'db.collection',"
                            "   'firstBatch': [{'a': 1}]}}");

   /* success! */
   BSON_ASSERT (future_get_bool (future));

   CLEANUP_QUERY ();
   mongoc_collection_destroy (collection);

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
test_cluster_node_disconnect_single (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_cluster_node_disconnect (false);
}


static void
test_cluster_node_disconnect_pooled (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_cluster_node_disconnect (true);
}


static void
_test_cluster_command_timeout (bool pooled)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bson_error_t error;
   future_t *future;
   request_t *request;
   uint16_t client_port;
   mongoc_server_description_t const *sd;
   bson_t reply;

   capture_logs (true);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "socketTimeoutMS", 200);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   /* server doesn't respond in time */
   future = future_client_command_simple (client, "db", tmp_bson ("{'foo': 1}"), NULL, NULL, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'foo': 1}"));
   client_port = request_get_client_port (request);

   ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to send \"foo\" command with database \"db\"");

   /* a network timeout does NOT invalidate the server description */
   sd = mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 1, NULL);
   BSON_ASSERT (sd->type != MONGOC_SERVER_UNKNOWN);

   /* late response */
   reply_to_request_simple (request, "{'ok': 1, 'bar': 1}");
   request_destroy (request);
   future_destroy (future);

   future = future_client_command_simple (client, "db", tmp_bson ("{'baz': 1}"), NULL, &reply, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'baz': 1}"));
   ASSERT (request);
   /* new socket */
   ASSERT_CMPUINT16 (client_port, !=, request_get_client_port (request));
   reply_to_request_simple (request, "{'ok': 1, 'quux': 1}");
   ASSERT (future_get_bool (future));

   /* got the proper response */
   ASSERT_HAS_FIELD (&reply, "quux");

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   bson_destroy (&reply);
   request_destroy (request);
   future_destroy (future);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static void
test_cluster_command_timeout_single (void)
{
   _test_cluster_command_timeout (false);
}


static void
test_cluster_command_timeout_pooled (void)
{
   _test_cluster_command_timeout (true);
}


static void
_test_write_disconnect (void)
{
   mock_server_t *server;
   char *hello;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_error_t error;
   future_t *future;
   request_t *request;
   mongoc_topology_scanner_node_t *scanner_node;
   mongoc_server_description_t const *sd;

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_new ();
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /*
    * establish connection with an "hello" and "ping"
    */
   future = future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   request = mock_server_receives_any_hello (server);
   hello = bson_strdup_printf ("{'ok': 1.0,"
                               " 'isWritablePrimary': true,"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX);

   reply_to_request_simple (request, hello);
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'ping': 1}"));
   reply_to_request_simple (request, "{'ok': 1}");
   ASSERT_OR_PRINT (future_get_bool (future), error);

   /*
    * close the socket
    */
   reply_to_request_with_hang_up (request);

   /*
    * next operation detects the hangup
    */
   collection = mongoc_client_get_collection (client, "db", "collection");
   future_destroy (future);
   future = future_collection_insert_one (collection, tmp_bson ("{'_id': 1}"), NULL, NULL, &error);

   ASSERT (!future_get_bool (future));
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_STREAM);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_STREAM_SOCKET);

   scanner_node = mongoc_topology_scanner_get_node (client->topology->scanner, 1 /* server_id */);
   ASSERT (scanner_node && !scanner_node->stream);

   /* a hangup DOES invalidate the server description */
   sd = mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 1, NULL);
   BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);

   mongoc_collection_destroy (collection);
   request_destroy (request);
   future_destroy (future);
   bson_free (hello);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_write_command_disconnect (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_write_disconnect ();
}


typedef struct {
   int calls;
   bson_t *cluster_time;
   bson_t *command;
} cluster_time_test_t;


static void
test_cluster_time_cmd_started_cb (const mongoc_apm_command_started_t *event)
{
   const bson_t *cmd;
   cluster_time_test_t *test;
   bson_iter_t iter;
   bson_t client_cluster_time;

   cmd = mongoc_apm_command_started_get_command (event);
   if (!strcmp (_mongoc_get_command_name (cmd), "killCursors")) {
      /* ignore killCursors */
      return;
   }

   test = (cluster_time_test_t *) mongoc_apm_command_started_get_context (event);

   test->calls++;
   bson_destroy (test->command);
   test->command = bson_copy (cmd);

   /* Only a MongoDB 3.6+ server reports $clusterTime. If we've received a
    * $clusterTime, we send it to any server. In this case, we got a
    * $clusterTime during the initial handshake. */
   if (test_framework_clustertime_supported ()) {
      BSON_ASSERT (bson_iter_init_find (&iter, cmd, "$clusterTime"));
      BSON_ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));

      if (test->calls == 2) {
         /* previous call to cmd_succeeded_cb saved server's clusterTime */
         BSON_ASSERT (!bson_empty0 (test->cluster_time));
         bson_iter_bson (&iter, &client_cluster_time);
         if (!bson_equal (test->cluster_time, &client_cluster_time)) {
            test_error ("Unequal clusterTimes.\nServer sent %s\nClient sent %s",
                        bson_as_json (test->cluster_time, NULL),
                        bson_as_json (&client_cluster_time, NULL));
         }

         bson_destroy (&client_cluster_time);
      }
   } else {
      BSON_ASSERT (!bson_has_field (event->command, "$clusterTime"));
   }
}


static void
test_cluster_time_cmd_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   const bson_t *reply;
   cluster_time_test_t *test;
   bson_iter_t iter;
   uint32_t len;
   const uint8_t *data;

   reply = mongoc_apm_command_succeeded_get_reply (event);
   test = (cluster_time_test_t *) mongoc_apm_command_succeeded_get_context (event);

   /* Only a MongoDB 3.6+ server reports $clusterTime. Save it in "test". */
   if (test_framework_clustertime_supported ()) {
      BSON_ASSERT (bson_iter_init_find (&iter, reply, "$clusterTime"));
      BSON_ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
      bson_iter_document (&iter, &len, &data);
      bson_destroy (test->cluster_time);
      test->cluster_time = bson_new_from_data (data, len);
   }
}


typedef bool (*command_fn_t) (mongoc_client_t *, bson_error_t *);


/* test $clusterTime handling according to the test instructions in the
 * Driver Sessions Spec */
static void
_test_cluster_time (bool pooled, command_fn_t command)
{
   mongoc_apm_callbacks_t *callbacks;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;
   cluster_time_test_t cluster_time_test;

   cluster_time_test.calls = 0;
   cluster_time_test.command = NULL;
   cluster_time_test.cluster_time = NULL;

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_cluster_time_cmd_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, test_cluster_time_cmd_succeeded_cb);

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      mongoc_client_pool_set_apm_callbacks (pool, callbacks, &cluster_time_test);
      client = mongoc_client_pool_pop (pool);
      /* CDRIVER-3596 - prevent client discovery of the pool interfering with
       * the test operations. */
      _mongoc_usleep (5000 * 1000); /* 5 s */
   } else {
      client = test_framework_new_default_client ();
      mongoc_client_set_apm_callbacks (client, callbacks, &cluster_time_test);
   }

   r = command (client, &error);
   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT (cluster_time_test.calls, ==, 1);

   /* repeat */
   r = command (client, &error);
   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT (cluster_time_test.calls, ==, 2);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_apm_callbacks_destroy (callbacks);
   bson_destroy (cluster_time_test.command);
   bson_destroy (cluster_time_test.cluster_time);
}


static bool
command_simple (mongoc_client_t *client, bson_error_t *error)
{
   return mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, error);
}


static void
test_cluster_time_command_simple_single (void)
{
   _test_cluster_time (false, command_simple);
}


static void
test_cluster_time_command_simple_pooled (void)
{
   _test_cluster_time (true, command_simple);
}


/* test the deprecated mongoc_client_command function with $clusterTime */
static bool
client_command (mongoc_client_t *client, bson_error_t *error)
{
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bool r;

   ASSERT (client);

   cursor = mongoc_client_command (client, "test", MONGOC_QUERY_NONE, 0, 0, 0, tmp_bson ("{'ping': 1}"), NULL, NULL);

   mongoc_cursor_next (cursor, &doc);
   r = !mongoc_cursor_error (cursor, error);
   mongoc_cursor_destroy (cursor);
   return r;
}


static void
test_cluster_time_command_single (void)
{
   _test_cluster_time (false, client_command);
}


static void
test_cluster_time_command_pooled (void)
{
   _test_cluster_time (true, client_command);
}


/* test modern mongoc_client_read_command_with_opts with $clusterTime */
static bool
client_command_with_opts (mongoc_client_t *client, bson_error_t *error)
{
   /* any of the with_opts command functions should work */
   return mongoc_client_read_command_with_opts (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, error);
}


static void
test_cluster_time_command_with_opts_single (void)
{
   _test_cluster_time (false, client_command_with_opts);
}


static void
test_cluster_time_command_with_opts_pooled (void)
{
   _test_cluster_time (true, client_command_with_opts);
}


/* test aggregate with $clusterTime */
static bool
aggregate (mongoc_client_t *client, bson_error_t *error)
{
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bool r;

   ASSERT (client);

   collection = mongoc_client_get_collection (client, "test", "collection");
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL, NULL);

   mongoc_cursor_next (cursor, &doc);
   r = !mongoc_cursor_error (cursor, error);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   return r;
}


static void
test_cluster_time_aggregate_single (void)
{
   _test_cluster_time (false, aggregate);
}


static void
test_cluster_time_aggregate_pooled (void)
{
   _test_cluster_time (true, aggregate);
}


/* test queries with $clusterTime */
static bool
cursor_next (mongoc_client_t *client, bson_error_t *error)
{
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bool r;

   ASSERT (client);
   collection = get_test_collection (client, "test_cluster_time_cursor");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{'ping': 1}"), NULL, NULL);

   mongoc_cursor_next (cursor, &doc);
   r = !mongoc_cursor_error (cursor, error);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   return r;
}


static void
test_cluster_time_cursor_single (void)
{
   _test_cluster_time (false, cursor_next);
}


static void
test_cluster_time_cursor_pooled (void)
{
   _test_cluster_time (true, cursor_next);
}


/* test inserts with $clusterTime */
static bool
insert (mongoc_client_t *client, bson_error_t *error)
{
   mongoc_collection_t *collection;
   bool r;

   ASSERT (client);

   collection = get_test_collection (client, "test_cluster_time_cursor");
   r = mongoc_collection_insert_one (collection, tmp_bson ("{}"), NULL, NULL, error);

   mongoc_collection_destroy (collection);

   return r;
}


static void
test_cluster_time_insert_single (void)
{
   _test_cluster_time (false, insert);
}


static void
test_cluster_time_insert_pooled (void)
{
   _test_cluster_time (true, insert);
}


static void
test_cluster_command_timeout_negative (void)
{
   bson_error_t error;

   mongoc_uri_t *const uri = test_framework_get_uri ();

   // CDRIVER-4781: libmongoc historically supports negative values as
   // fallback to a "default" value for timeouts.
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, -1);

   mongoc_client_t *const client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);

   // There should not be an error when validating sockettimeoutms.
   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error),
                    error);

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
}


static void
replies_with_cluster_time (request_t *request, int t, int i, const char *docs_json)
{
   char *quotes_replaced;
   bson_t doc;
   bson_error_t error;
   bool r;

   BSON_ASSERT (request);

   if (docs_json) {
      quotes_replaced = single_quotes_to_double (docs_json);
      r = bson_init_from_json (&doc, quotes_replaced, -1, &error);
      bson_free (quotes_replaced);
   } else {
      r = bson_init_from_json (&doc, "{}", -1, &error);
   }

   if (!r) {
      MONGOC_WARNING ("%s", error.message);
      return;
   }

   BSON_APPEND_DOCUMENT (
      &doc, "$clusterTime", tmp_bson ("{'clusterTime': {'$timestamp': {'t': %d, 'i': %d}}, 'x': 'y'}", t, i));

   reply_to_request_with_multiple_docs (request, MONGOC_REPLY_NONE, &doc, 1, 0 /* cursor id */);

   bson_destroy (&doc);
   request_destroy (request);
}


static request_t *
receives_with_cluster_time (mock_server_t *server, uint32_t timestamp, uint32_t increment, bson_t *command)
{
   request_t *request;
   const bson_t *doc;
   bson_iter_t cluster_time;
   uint32_t t;
   uint32_t i;

   request = mock_server_receives_msg (server, 0, command);
   BSON_ASSERT (request);
   doc = request_get_doc (request, 0);

   BSON_ASSERT (bson_iter_init_find (&cluster_time, doc, "$clusterTime"));
   BSON_ASSERT (BSON_ITER_HOLDS_DOCUMENT (&cluster_time));
   BSON_ASSERT (bson_iter_recurse (&cluster_time, &cluster_time));
   BSON_ASSERT (bson_iter_find (&cluster_time, "clusterTime"));
   BSON_ASSERT (BSON_ITER_HOLDS_TIMESTAMP (&cluster_time));
   bson_iter_timestamp (&cluster_time, &t, &i);
   if (t != timestamp || i != increment) {
      test_error ("Expected Timestamp(%d, %d), got Timestamp(%d, %d)", timestamp, increment, t, i);
   }

   return request;
}


static void
assert_ok (future_t *future, const bson_error_t *error)
{
   bool r = future_get_bool (future);
   ASSERT_OR_PRINT (r, (*error));
   future_destroy (future);
}


static future_t *
future_ping (mongoc_client_t *client, bson_error_t *error)
{
   return future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, error);
}


static void
_test_cluster_time_comparison (bool pooled)
{
   const char *hello = tmp_str ("{'ok': 1.0,"
                                " 'isWritablePrimary': true,"
                                " 'msg': 'isdbgrid',"
                                " 'minWireVersion': %d,"
                                " 'maxWireVersion': %d}",
                                WIRE_VERSION_MIN,
                                WIRE_VERSION_MAX);
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bson_error_t error;
   future_t *future;
   request_t *request;
   bson_t *ping = tmp_bson ("{'ping': 1}");

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 500);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   future = future_ping (client, &error);

   /* timestamp is 1 */
   request = mock_server_receives_any_hello (server);
   replies_with_cluster_time (request, 1, 1, hello);

   if (pooled) {
      /* a pooled client handshakes its own connection */
      request = mock_server_receives_any_hello (server);
      replies_with_cluster_time (request, 1, 1, hello);
   }

   request = receives_with_cluster_time (server, 1, 1, ping);

   /* timestamp is 2, increment is 2 */
   replies_with_cluster_time (request, 2, 2, "{'ok': 1.0}");
   assert_ok (future, &error);

   future = future_ping (client, &error);
   request = receives_with_cluster_time (server, 2, 2, ping);

   /* timestamp is 2, increment is only 1 */
   replies_with_cluster_time (request, 2, 1, "{'ok': 1.0}");
   assert_ok (future, &error);

   future = future_ping (client, &error);

   /* client doesn't update cluster time, since new value is less than old */
   request = receives_with_cluster_time (server, 2, 2, ping);
   reply_to_request_with_ok_and_destroy (request);
   assert_ok (future, &error);

   if (pooled) {
      /* wait for next heartbeat, it should contain newest cluster time */
      request = mock_server_receives_any_hello_with_match (server,
                                                           "{'$clusterTime': "
                                                           "{'clusterTime': {'$timestamp': "
                                                           "{'t': 2, 'i': 2}}}}",

                                                           "{'$clusterTime': "
                                                           "{'clusterTime': {'$timestamp': "
                                                           "{'t': 2, 'i': 2}}}}");

      replies_with_cluster_time (request, 2, 1, hello);

      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      /* trigger next heartbeat, it should contain newest cluster time */
      _mongoc_usleep (750 * 1000); /* 750 ms */
      future = future_ping (client, &error);
      request = mock_server_receives_any_hello_with_match (server,
                                                           "{'$clusterTime': "
                                                           "{'clusterTime': {'$timestamp': "
                                                           "{'t': 2, 'i': 2}}}}",

                                                           "{'$clusterTime': "
                                                           "{'clusterTime': {'$timestamp': "
                                                           "{'t': 2, 'i': 2}}}}");

      replies_with_cluster_time (request, 2, 1, hello);
      request = receives_with_cluster_time (server, 2, 2, ping);
      reply_to_request_with_ok_and_destroy (request);
      assert_ok (future, &error);

      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static void
test_cluster_time_comparison_single (void)
{
   _test_cluster_time_comparison (false);
}


static void
test_cluster_time_comparison_pooled (void)
{
   _test_cluster_time_comparison (true);
}


typedef future_t *(*run_command_fn_t) (mongoc_client_t *);
typedef void (*cleanup_fn_t) (future_t *);


typedef struct {
   const char *errmsg;
   bool is_not_primary_err;
} test_error_msg_t;


test_error_msg_t errors[] = {{"not master", true},
                             {"not master or secondary", true},
                             {"node is recovering", true},
                             {"not master and secondaryOk=false", true},
                             {"replicatedToNum called but not master anymore", true},
                             {"??? node is recovering ???", true},
                             {"??? not master ???", true},
                             {"foo", false},
                             {0}};

/* a "not primary" or "node is recovering" error marks server Unknown.
   "not primary" and "node is recovering" need only be substrings of the error
   message. */
static void
_test_not_primary (bool pooled, run_command_fn_t run_command, cleanup_fn_t cleanup_fn)
{
   test_error_msg_t *test_error_msg;
   mock_server_t *server;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   const char *cmd = "{'cmd': 1}";
   bson_error_t error;
   future_t *future;
   request_t *request;
   mongoc_topology_description_t const *td;
   const mongoc_server_description_t *sd;
   char *reply;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (mock_server_get_uri (server), NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   }

   for (test_error_msg = errors; test_error_msg->errmsg; test_error_msg++) {
      /*
       * successful command results in known server type
       */
      future = future_client_command_simple (client, "test", tmp_bson (cmd), NULL, NULL, &error);

      request = mock_server_receives_request (server);
      reply_to_request_with_ok_and_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);

      /* Topology may be invalidated by client_command_simple */
      td = mc_tpld_unsafe_get_const (client->topology);
      sd = mongoc_set_get_const (mc_tpld_servers_const (td), 1);

      BSON_ASSERT (sd->type == MONGOC_SERVER_STANDALONE);

      /*
       * command error marks server Unknown if it's a "not primary" error
       */
      future = run_command (client);
      request = mock_server_receives_request (server);

      reply = bson_strdup_printf ("{'ok': 0, 'errmsg': '%s'}", test_error_msg->errmsg);
      reply_to_request_simple (request, reply);
      BSON_ASSERT (!future_get_bool (future));

      /* Topology should be invalidated by run_command */
      td = mc_tpld_unsafe_get_const (client->topology);
      sd = mongoc_set_get_const (mc_tpld_servers_const (td), 1);

      if (test_error_msg->is_not_primary_err) {
         BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);
      } else {
         BSON_ASSERT (sd->type == MONGOC_SERVER_STANDALONE);
      }

      bson_free (reply);
      request_destroy (request);
      cleanup_fn (future);
   }

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mock_server_destroy (server);
}


static future_t *
future_command_simple (mongoc_client_t *client)
{
   return future_client_command_simple (client, "test", tmp_bson ("{'cmd': 1}"), NULL, NULL, NULL);
}


static void
function_command_simple_cleanup (future_t *future)
{
   future_destroy (future);
}


static void
test_not_primary_single (void)
{
   _test_not_primary (false, future_command_simple, function_command_simple_cleanup);
}


static void
test_not_primary_pooled (void)
{
   _test_not_primary (true, future_command_simple, function_command_simple_cleanup);
}


/* parts must remain valid after future_command_private exits */
mongoc_cmd_parts_t parts;

static future_t *
future_command_private (mongoc_client_t *client)
{
   bson_error_t error;
   mongoc_server_stream_t *server_stream;

   ASSERT (client);

   server_stream = mongoc_cluster_stream_for_writes (&client->cluster, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);

   mongoc_cmd_parts_init (&parts, client, "test", MONGOC_QUERY_NONE, tmp_bson ("{'cmd': 1}"));

   /* mongoc_cluster_run_command_parts will call mongoc_cmd_parts_cleanup */
   return future_cluster_run_command_parts (&client->cluster, server_stream, &parts, NULL, NULL);
}


static void
future_command_private_cleanup (future_t *future)
{
   mongoc_server_stream_t *server_stream = future_value_get_mongoc_server_stream_ptr (future_get_param (future, 1));
   mongoc_server_stream_cleanup (server_stream);
   future_destroy (future);
}


static void
test_not_primary_auth_single (void)
{
   _test_not_primary (false, future_command_private, future_command_private_cleanup);
}


static void
test_not_primary_auth_pooled (void)
{
   _test_not_primary (true, future_command_private, future_command_private_cleanup);
}


typedef struct {
   const char *name;
   const char *q;
   const char *e;
   bool secondary;
   bool cluster_time;
} dollar_query_test_t;


static bool
auto_hello_callback (request_t *request, void *data, bson_t *hello_response)
{
   dollar_query_test_t *test;
   bson_t cluster_time;

   BSON_UNUSED (request);

   test = (dollar_query_test_t *) data;

   bson_init (hello_response);
   BSON_APPEND_INT32 (hello_response, "ok", 1);
   BSON_APPEND_BOOL (hello_response, "isWritablePrimary", !test->secondary);
   BSON_APPEND_BOOL (hello_response, "secondary", test->secondary);
   BSON_APPEND_INT32 (hello_response, "minWireVersion", WIRE_VERSION_MIN);
   BSON_APPEND_INT32 (hello_response, "maxWireVersion", WIRE_VERSION_MAX);
   BSON_APPEND_UTF8 (hello_response, "setName", "rs");

   if (test->cluster_time) {
      BSON_APPEND_DOCUMENT_BEGIN (hello_response, "$clusterTime", &cluster_time);
      BSON_APPEND_TIMESTAMP (&cluster_time, "clusterTime", 1, 1);
      bson_append_document_end (hello_response, &cluster_time);
   }

   return true;
}


static void
_test_dollar_query (void *ctx)
{
   dollar_query_test_t *test;
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_read_prefs_t *read_prefs;
   mongoc_cursor_t *cursor;
   future_t *future;
   request_t *request;
   const bson_t *doc;
   bson_error_t error;

   test = (dollar_query_test_t *) ctx;

   server = mock_server_new ();
   mock_server_auto_hello_callback (server, auto_hello_callback, test, NULL);
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   if (test->secondary) {
      read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   } else {
      read_prefs = NULL;
   }

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, tmp_bson (test->q), NULL, read_prefs);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server, 0, tmp_bson (test->e));
   reply_to_op_msg_request (request,
                            MONGOC_MSG_NONE,
                            tmp_bson ("{'ok': 1,"
                                      " 'cursor': {"
                                      "    'id': {'$numberLong': '0'},"
                                      "    'ns': 'db.collection',"
                                      "    'firstBatch': []}}"));

   BSON_ASSERT (!future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   future_destroy (future);
   request_destroy (request);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_cursor_destroy (cursor);

   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


dollar_query_test_t tests[] = {{"/Cluster/cluster_time/query/",
                                "{'a': 1}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': false}"
                                "}",
                                false,
                                false},
                               {"/Cluster/cluster_time/query/cluster_time",
                                "{'a': 1}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': true}"
                                "}",
                                false,
                                true},
                               {"/Cluster/cluster_time/query/secondary",
                                "{'a': 1}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1}, "
                                "   '$clusterTime': {'$exists': false},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                false},
                               {"/Cluster/cluster_time/query/cluster_time_secondary",
                                "{'a': 1}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1}, "
                                "   '$clusterTime': {'$exists': true},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                true},
                               {"/Cluster/cluster_time/dollar_query/from_user",
                                "{'$query': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': false}"
                                "}",
                                false,
                                false},
                               {"/Cluster/cluster_time/dollar_query/from_user/cluster_time",
                                "{'$query': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': true}"
                                "}",
                                false,
                                true},
                               {"/Cluster/cluster_time/dollar_query/from_user/secondary",
                                "{'$query': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': false},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                false},
                               {"/Cluster/cluster_time/dollar_query/from_user/cluster_time_secondary",
                                "{'$query': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   '$clusterTime': {'$exists': true},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                true},
                               {"/Cluster/cluster_time/dollar_orderby",
                                "{'$query': {'a': 1}, '$orderby': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   'sort': {'a': 1}"
                                "}",
                                false,
                                false},
                               {"/Cluster/cluster_time/dollar_orderby/secondary",
                                "{'$query': {'a': 1}, '$orderby': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   'sort': {'a': 1},"
                                "   '$clusterTime': {'$exists': false},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                false},
                               {"/Cluster/cluster_time/dollar_orderby/cluster_time",
                                "{'$query': {'a': 1}, '$orderby': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   'sort': {'a': 1},"
                                "   '$clusterTime': {'$exists': true}"
                                "}",
                                false,
                                true},
                               {"/Cluster/cluster_time/dollar_orderby/cluster_time_secondary",
                                "{'$query': {'a': 1}, '$orderby': {'a': 1}}",
                                "{"
                                "   'find': 'collection', 'filter': {'a': 1},"
                                "   'sort': {'a': 1},"
                                "   '$clusterTime': {'$exists': true},"
                                "   '$readPreference': {'mode': 'secondary'}"
                                "}",
                                true,
                                true},
                               {NULL}};

static void
_test_cluster_hello_fails (bool hangup)
{
   mock_server_t *mock_server;
   mongoc_uri_t *uri;
   mongoc_server_description_t *sd;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   request_t *request;
   future_t *future;
   bson_error_t error;
   int autoresponder_id;

   mock_server = mock_server_new ();
   autoresponder_id = mock_server_auto_hello (mock_server, "{ 'isWritablePrimary': true }");
   mock_server_run (mock_server);
   uri = mongoc_uri_copy (mock_server_get_uri (mock_server));
   /* increase heartbeatFrequencyMS to prevent background server selection. */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 99999);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   mongoc_client_pool_set_error_api (pool, 2);
   mongoc_uri_destroy (uri);
   client = mongoc_client_pool_pop (pool);
   /* do server selection to add this server to the topology. this does not add
    * a cluster node for this server. */
   sd = mongoc_client_select_server (client, false, NULL, NULL);
   BSON_ASSERT (sd);
   mongoc_server_description_destroy (sd);
   mock_server_remove_autoresponder (mock_server, autoresponder_id);
   /* now create a cluster node by running a command. */
   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   /* the client adds a cluster node, creating a stream to the server, and then
    * sends a hello request. */
   request = mock_server_receives_any_hello (mock_server);
   /* CDRIVER-2576: the server replies with an error, so
    * _mongoc_stream_run_hello returns NULL, which
    * _mongoc_cluster_run_hello must check. */

   if (hangup) {
      capture_logs (true); /* suppress "failed to buffer" warning */
      reply_to_request_with_hang_up (request);
      BSON_ASSERT (!future_get_bool (future));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket err");
   } else {
      reply_to_request_simple (request, "{'ok': 0, 'code': 123}");
      BSON_ASSERT (!future_get_bool (future));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 123, "Unknown command error");
   }

   request_destroy (request);
   future_destroy (future);
   mock_server_destroy (mock_server);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}

static void
test_cluster_hello_fails (void)
{
   _test_cluster_hello_fails (false);
}


static void
test_cluster_hello_hangup (void)
{
   _test_cluster_hello_fails (true);
}

static void
test_cluster_command_error (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_error_t err;
   request_t *request;
   future_t *future;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   future = future_client_command_simple (
      client, "db", tmp_bson ("{'ping': 1}"), NULL /* opts */, NULL /* read prefs */, &err);
   request = mock_server_receives_msg (server, MONGOC_QUERY_NONE, tmp_bson ("{'$db': 'db', 'ping': 1}"));
   reply_to_request_with_hang_up (request);
   BSON_ASSERT (!future_get_bool (future));
   future_destroy (future);
   request_destroy (request);
   /* _mongoc_buffer_append_from_stream, used by opmsg gives more detail. */
   ASSERT_ERROR_CONTAINS (err,
                          MONGOC_ERROR_STREAM,
                          MONGOC_ERROR_STREAM_SOCKET,
                          "Failed to send \"ping\" command with database "
                          "\"db\": Failed to read 4 bytes: socket error or "
                          "timeout");
   mock_server_destroy (server);
   mongoc_client_destroy (client);
}

static void
test_advanced_cluster_time_not_sent_to_standalone (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_session_t *cs;
   bson_t opts = BSON_INITIALIZER;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   future_t *future;
   request_t *request;
   const bson_t *doc;
   bson_error_t error;

   server = mock_server_new ();
   mock_server_auto_endsessions (server);
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'logicalSessionTimeoutMinutes': 30}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   cs = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (cs, error);

   mongoc_client_session_advance_cluster_time (cs, tmp_bson ("{'clusterTime': {'$timestamp': {'t': 1, 'i': 1}}}"));

   ASSERT_OR_PRINT (mongoc_client_session_append (cs, &opts, &error), error);

   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), &opts, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       0,
                                       tmp_bson ("{"
                                                 "   'find': 'collection', 'filter': {},"
                                                 "   '$clusterTime': {'$exists': false}"
                                                 "}"));
   reply_to_op_msg_request (request,
                            MONGOC_MSG_NONE,
                            tmp_bson ("{'ok': 1,"
                                      " 'cursor': {"
                                      "    'id': {'$numberLong': '0'},"
                                      "    'ns': 'db.collection',"
                                      "    'firstBatch': []}}"));

   BSON_ASSERT (!future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   bson_destroy (&opts);
   mongoc_collection_destroy (collection);
   mongoc_client_session_destroy (cs);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Responds properly to hello, hangs up on serverStatus, and replies {ok:1}
 * to everything else. */
static bool
_responder (request_t *req, void *data)
{
   char *hello;

   hello = (char *) data;
   if (0 == strcasecmp (req->command_name, HANDSHAKE_CMD_LEGACY_HELLO) ||
       0 == strcasecmp (req->command_name, "hello")) {
      reply_to_request_simple (req, hello);
      request_destroy (req);
      return true;
   } else if (0 == strcmp (req->command_name, "serverStatus")) {
      reply_to_request_with_hang_up (req);
      request_destroy (req);
      return true;
   }

   /* Otherwise, reply {ok:1} */
   reply_to_request_with_ok_and_destroy (req);
   return true;
}

static mongoc_stream_t *
_initiator_fn (const mongoc_uri_t *uri, const mongoc_host_list_t *host, void *user_data, bson_error_t *error)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   bool ret;
   bson_t *cmd;
   bson_error_t ss_error;
   mongoc_stream_t *stream;

   cmd = BCON_NEW ("serverStatus", BCON_INT32 (1));
   pool = (mongoc_client_pool_t *) user_data;
   client = mongoc_client_pool_pop (pool);

   /* Hide warnings that get logged from network errors. */
   capture_logs (true);
   ret = mongoc_client_command_simple (client, "db", cmd, NULL, NULL, &ss_error);
   capture_logs (false);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (ss_error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");
   stream = mongoc_client_default_stream_initiator (uri, host, client, error);
   ASSERT_OR_PRINT (stream != NULL, (*error));
   mongoc_client_pool_push (pool, client);
   bson_destroy (cmd);
   return stream;
}

void
test_hello_on_unknown (void)
{
   mock_server_t *mock_server;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   bson_error_t error;
   bool ret;
   mongoc_uri_t *uri;

   mock_server = mock_server_new ();
   mock_server_run (mock_server);
   mock_server_autoresponds (mock_server,
                             _responder,
                             (void *) tmp_str ("{ 'ok': 1.0,"
                                               " 'isWritablePrimary': true,"
                                               " 'minWireVersion': %d,"
                                               " 'maxWireVersion': %d,"
                                               " 'msg': 'isdbgrid'}",
                                               WIRE_VERSION_MIN,
                                               WIRE_VERSION_MAX),
                             NULL);

   uri = mongoc_uri_copy (mock_server_get_uri (mock_server));

   /* Add a placeholder additional host, so the topology type can be SHARDED.
    * The host will get removed on the first failed hello. */
   ret = mongoc_uri_upsert_host (uri, "localhost", 12345, &error);
   ASSERT_OR_PRINT (ret, error);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   client = mongoc_client_pool_pop (pool);

   mongoc_client_set_stream_initiator (client, _initiator_fn, pool);

   /* The other client marked the server as unknown after this client selected
    * the server and created a stream, but *before* constructing the initial
    * hello. This reproduces the crash reported in CDRIVER-3404. */
   ret = mongoc_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (ret, error);


   mongoc_uri_destroy (uri);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mock_server_destroy (mock_server);
}


/* Test what happens when running a command directly on a server (by passing an
 * explicit server id) that is marked as "unknown" in the topology description.
 * Prior to the bug fix of CDRIVER-3404, a pooled client would erroneously
 * attempt to send the command.
 *
 * Update: After applying the fix to CDRIVER-3653 this test was updated.
 * Connections will track their own server description from the handshake
 * response.
 * Marking the server unknown in the shared topology description no longer
 * affects established connections.
 */
void
_test_cmd_on_unknown_serverid (bool pooled)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   bson_error_t error;
   bool ret;
   mongoc_uri_t *uri;

   uri = test_framework_get_uri ();
   /* Set a lower heartbeatFrequencyMS.
    * Servers supporting streamable hello will only respond to an awaitable
    * hello until heartbeatFrequencyMS has passed or the server had changed
    * state. This test marks the server Unknown in the client's topology
    * description. During cleanup, _mongoc_client_end_sessions will attempt to
    * do server selection again and wait for a server to become discovered. */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 5000);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      test_framework_set_pool_ssl_opts (pool);
      client = mongoc_client_pool_pop (pool);
   } else {
      pool = NULL;
      client = test_framework_client_new_from_uri (uri, NULL);
      test_framework_set_ssl_opts (client);
   }

   /* Do the initial topology scan and selection. */
   ret = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{ 'ping': 1 }"), NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);


   ret = mongoc_client_command_simple_with_server_id (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, 1, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Invalidate the server, giving it the server type MONGOC_SERVER_UNKNOWN */
   _mongoc_topology_invalidate_server (client->topology, 1);

   /* The next command is attempted directly on the unknown server and should
    * result in an error. */
   ret = mongoc_client_command_simple_with_server_id (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, 1, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
   mongoc_uri_destroy (uri);
}

void
test_cmd_on_unknown_serverid_pooled (void)
{
   _test_cmd_on_unknown_serverid (true /* pooled */);
}

void
test_cmd_on_unknown_serverid_single (void)
{
   _test_cmd_on_unknown_serverid (false /* pooled */);
}


/* Test that server streams are invalidated as expected. */
static void
test_cluster_stream_invalidation_single (void)
{
   mongoc_client_t *client;
   mongoc_server_description_t *sd;
   bson_error_t error;
   mongoc_server_stream_t *stream;
   mc_tpld_modification tdmod;

   client = test_framework_new_default_client ();
   /* Select a server to start monitoring. */
   sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (sd, error);

   /* Test "clearing the pool". This should invalidate existing server streams.
    */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   tdmod = mc_tpld_modify_begin (client->topology);
   _mongoc_topology_description_clear_connection_pool (
      tdmod.new_td, mongoc_server_description_id (sd), &kZeroServiceId);
   mc_tpld_modify_commit (tdmod);
   BSON_ASSERT (!mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   /* Test closing the connection. This should invalidate existing server
    * streams. */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_cluster_disconnect_node (&client->cluster, sd->id);
   BSON_ASSERT (!mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   /* Test that a new stream is considered valid. */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   mongoc_server_description_destroy (sd);
   mongoc_client_destroy (client);
}

/* Test that server streams are invalidated as expected. */
static void
test_cluster_stream_invalidation_pooled (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_description_t *sd;
   bson_error_t error;
   mongoc_server_stream_t *stream;
   mc_tpld_modification tdmod;

   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);
   /* Select a server. */
   sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (sd, error);

   /* Test "clearing the pool". This should invalidate existing server streams.
    */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   tdmod = mc_tpld_modify_begin (client->topology);
   _mongoc_topology_description_clear_connection_pool (
      tdmod.new_td, mongoc_server_description_id (sd), &kZeroServiceId);
   mc_tpld_modify_commit (tdmod);
   BSON_ASSERT (!mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   /* Test closing the connection. This should invalidate existing server
    * streams. */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_cluster_disconnect_node (&client->cluster, sd->id);
   BSON_ASSERT (!mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   /* Test that a new stream is considered valid. */
   stream = mongoc_cluster_stream_for_writes (
      &client->cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (stream, error);
   BSON_ASSERT (mongoc_cluster_stream_valid (&client->cluster, stream));
   mongoc_server_stream_cleanup (stream);

   mongoc_server_description_destroy (sd);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}

void
test_cluster_install (TestSuite *suite)
{
   dollar_query_test_t *p = tests;

   while (p->name) {
      TestSuite_AddFull (suite, p->name, _test_dollar_query, NULL, p, TestSuite_CheckMockServerAllowed);

      p++;
   }

   TestSuite_AddLive (suite, "/Cluster/test_get_max_bson_obj_size", test_get_max_bson_obj_size);
   TestSuite_AddLive (suite, "/Cluster/test_get_max_msg_size", test_get_max_msg_size);
   TestSuite_AddFull (suite,
                      "/Cluster/disconnect/single",
                      test_cluster_node_disconnect_single,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Cluster/disconnect/pooled",
                      test_cluster_node_disconnect_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite, "/Cluster/command/timeout/single", test_cluster_command_timeout_single);
   TestSuite_AddMockServerTest (suite, "/Cluster/command/timeout/pooled", test_cluster_command_timeout_pooled);
   TestSuite_AddFull (suite,
                      "/Cluster/write_command/disconnect",
                      test_write_command_disconnect,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/command_simple/single", test_cluster_time_command_simple_single);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/command_simple/pooled", test_cluster_time_command_simple_pooled);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/command/single", test_cluster_time_command_single);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/command/pooled", test_cluster_time_command_pooled);
   TestSuite_AddLive (
      suite, "/Cluster/cluster_time/command_with_opts/single", test_cluster_time_command_with_opts_single);
   TestSuite_AddLive (
      suite, "/Cluster/cluster_time/command_with_opts/pooled", test_cluster_time_command_with_opts_pooled);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/aggregate/single", test_cluster_time_aggregate_single);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/aggregate/pooled", test_cluster_time_aggregate_pooled);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/cursor/single", test_cluster_time_cursor_single);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/cursor/pooled", test_cluster_time_cursor_pooled);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/insert/single", test_cluster_time_insert_single);
   TestSuite_AddLive (suite, "/Cluster/cluster_time/insert/pooled", test_cluster_time_insert_pooled);
   TestSuite_AddLive (suite, "/Cluster/command/timeout/negative", test_cluster_command_timeout_negative);
   TestSuite_AddMockServerTest (suite,
                                "/Cluster/cluster_time/comparison/single",
                                test_cluster_time_comparison_single,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Cluster/cluster_time/comparison/pooled",
                                test_cluster_time_comparison_pooled,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Cluster/cluster_time/advanced_not_sent_to_standalone",
                                test_advanced_cluster_time_not_sent_to_standalone,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (
      suite, "/Cluster/not_primary/single", test_not_primary_single, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Cluster/not_primary/pooled", test_not_primary_pooled, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Cluster/not_primary_auth/single", test_not_primary_auth_single, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Cluster/not_primary_auth/pooled", test_not_primary_auth_pooled, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite, "/Cluster/hello_fails", test_cluster_hello_fails);
   TestSuite_AddMockServerTest (suite, "/Cluster/hello_hangup", test_cluster_hello_hangup);
   TestSuite_AddMockServerTest (suite, "/Cluster/command_error/op_msg", test_cluster_command_error);
   TestSuite_AddMockServerTest (suite, "/Cluster/hello_on_unknown/mock", test_hello_on_unknown);
   /* These tests exhibit some mysterious behavior after the new feature
   changes-- see: "https://jira.mongodb.org/browse/CDRIVER-4293".
      TestSuite_AddLive (suite,
                         "/Cluster/cmd_on_unknown_serverid/pooled",
                         test_cmd_on_unknown_serverid_pooled);
      TestSuite_AddLive (suite,
                         "/Cluster/cmd_on_unknown_serverid/single",
                         test_cmd_on_unknown_serverid_single);
   */
   TestSuite_AddLive (suite, "/Cluster/stream_invalidation/single", test_cluster_stream_invalidation_single);
   TestSuite_AddLive (suite, "/Cluster/stream_invalidation/pooled", test_cluster_stream_invalidation_pooled);
}
