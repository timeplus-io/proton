#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc-util-private.h"
#include "mongoc/utlist.h"

#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "topology-reconcile-test"


static mongoc_topology_scanner_node_t *
get_node (mongoc_topology_t *topology, const char *host_and_port)
{
   mongoc_topology_scanner_t *ts;
   mongoc_topology_scanner_node_t *node;
   mongoc_topology_scanner_node_t *sought = NULL;
   BSON_ASSERT (topology->single_threaded);

   ts = topology->scanner;

   DL_FOREACH (ts->nodes, node)
   {
      if (!strcmp (host_and_port, node->host.host_and_port)) {
         sought = node;
         break;
      }
   }

   return sought;
}


static bool
has_server_description (const mongoc_topology_t *topology, const char *host_and_port)
{
   mc_shared_tpld td = mc_tpld_take_ref (topology);
   const mongoc_set_t *servers = mc_tpld_servers_const (td.ptr);
   bool found = false;
   const mongoc_server_description_t *sd;

   for (size_t i = 0; i < servers->items_len; i++) {
      sd = mongoc_set_get_item_const (servers, i);
      if (!strcmp (sd->host.host_and_port, host_and_port)) {
         found = true;
         break;
      }
   }

   mc_tpld_drop_ref (&td);
   return found;
}


bool
selects_server (mongoc_client_t *client, mongoc_read_prefs_t *read_prefs, mock_server_t *server)
{
   bson_error_t error;
   mongoc_server_description_t *sd;
   bool result;

   ASSERT (client);

   sd = mongoc_topology_select (client->topology, MONGOC_SS_READ, read_prefs, NULL, &error);

   if (!sd) {
      fprintf (stderr, "%s\n", error.message);
      return false;
   }

   result = (0 == strcmp (mongoc_server_description_host (sd)->host_and_port, mock_server_get_host_and_port (server)));

   mongoc_server_description_destroy (sd);

   return result;
}


static void
_test_topology_reconcile_rs (bool pooled)
{
   mock_server_t *server0;
   mock_server_t *server1;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   debug_stream_stats_t debug_stream_stats = {0};
   mongoc_read_prefs_t *secondary_read_prefs;
   mongoc_read_prefs_t *primary_read_prefs;
   mongoc_read_prefs_t *tag_read_prefs;

   server0 = mock_server_new ();
   server1 = mock_server_new ();
   mock_server_run (server0);
   mock_server_run (server1);

   /* secondary, no tags */
   RS_RESPONSE_TO_HELLO (server0, WIRE_VERSION_MIN, false, false, server0, server1);
   /* primary, no tags */
   RS_RESPONSE_TO_HELLO (server1, WIRE_VERSION_MIN, true, false, server0, server1);

   /* provide secondary in seed list */
   uri_str = bson_strdup_printf ("mongodb://%s/?replicaSet=rs", mock_server_get_host_and_port (server0));

   uri = mongoc_uri_new (uri_str);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new (uri_str, NULL);
   }

   if (!pooled) {
      test_framework_set_debug_stream (client, &debug_stream_stats);
   }

   secondary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   primary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   tag_read_prefs = mongoc_read_prefs_new (MONGOC_READ_NEAREST);
   mongoc_read_prefs_add_tag (tag_read_prefs, tmp_bson ("{'key': 'value'}"));

   /*
    * server0 is selected, server1 is discovered and added to scanner.
    */
   BSON_ASSERT (selects_server (client, secondary_read_prefs, server0));
   if (!pooled) {
      BSON_ASSERT (get_node (client->topology, mock_server_get_host_and_port (server1)));
   }

   /*
    * select again with mode "primary": server1 is selected.
    */
   BSON_ASSERT (selects_server (client, primary_read_prefs, server1));

   /*
    * remove server1 from set. server0 is the primary, with tags.
    */
   RS_RESPONSE_TO_HELLO (server0, WIRE_VERSION_MIN, true, true, server0); /* server1 absent */

   BSON_ASSERT (selects_server (client, tag_read_prefs, server0));
   BSON_ASSERT (!client->topology->stale);

   if (!pooled) {
      ASSERT_CMPINT (1, ==, debug_stream_stats.n_failed);
   }

   /*
    * server1 returns as a secondary. its scanner node is un-retired.
    */
   RS_RESPONSE_TO_HELLO (server0, WIRE_VERSION_MIN, true, true, server0, server1);
   RS_RESPONSE_TO_HELLO (server1, WIRE_VERSION_MIN, false, false, server0, server1);

   BSON_ASSERT (selects_server (client, secondary_read_prefs, server1));

   if (!pooled) {
      /* no additional failed streams */
      ASSERT_CMPINT (1, ==, debug_stream_stats.n_failed);
   }

   mongoc_read_prefs_destroy (primary_read_prefs);
   mongoc_read_prefs_destroy (secondary_read_prefs);
   mongoc_read_prefs_destroy (tag_read_prefs);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   mock_server_destroy (server1);
   mock_server_destroy (server0);
}


static void
test_topology_reconcile_rs_single (void)
{
   _test_topology_reconcile_rs (false);
}


static void
test_topology_reconcile_rs_pooled (void)
{
   _test_topology_reconcile_rs (true);
}


static void
_test_topology_reconcile_sharded (bool pooled)
{
   mock_server_t *mongos;
   mock_server_t *secondary;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_read_prefs_t *primary_read_prefs;
   bson_error_t error;
   future_t *future;
   request_t *request;
   char *secondary_response;
   mongoc_server_description_t *sd;

   mongos = mock_server_new ();
   secondary = mock_server_new ();
   mock_server_run (mongos);
   mock_server_run (secondary);

   /* provide both servers in seed list */
   uri_str = bson_strdup_printf (
      "mongodb://%s,%s", mock_server_get_host_and_port (mongos), mock_server_get_host_and_port (secondary));

   uri = mongoc_uri_new (uri_str);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new (uri_str, NULL);
   }

   primary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_read_prefs, NULL, &error);

   /* mongos */
   request = mock_server_receives_any_hello (mongos);
   reply_to_request_simple (request,
                            tmp_str ("{'ok': 1,"
                                     " 'isWritablePrimary': true,"
                                     " 'minWireVersion': %d,"
                                     " 'maxWireVersion': %d,"
                                     " 'msg': 'isdbgrid'}",
                                     WIRE_VERSION_MIN,
                                     WIRE_VERSION_MAX));

   request_destroy (request);

   /* make sure the mongos response is processed first */
   _mongoc_usleep (1000 * 1000);

   /* replica set secondary - topology removes it */
   request = mock_server_receives_any_hello (secondary);
   secondary_response = bson_strdup_printf ("{'ok': 1, "
                                            " 'setName': 'rs',"
                                            " 'isWritablePrimary': false,"
                                            " 'secondary': true,"
                                            " 'minWireVersion': %d,"
                                            " 'maxWireVersion': %d,"
                                            " 'hosts': ['%s', '%s']}",
                                            WIRE_VERSION_MIN,
                                            WIRE_VERSION_MAX,
                                            mock_server_get_host_and_port (mongos),
                                            mock_server_get_host_and_port (secondary));

   reply_to_request_simple (request, secondary_response);

   request_destroy (request);

   /*
    * mongos is selected, secondary is removed.
    */
   sd = future_get_mongoc_server_description_ptr (future);
   ASSERT_CMPSTR (sd->host.host_and_port, mock_server_get_host_and_port (mongos));

   if (!pooled) {
      BSON_ASSERT (!get_node (client->topology, mock_server_get_host_and_port (secondary)));
   }

   mongoc_server_description_destroy (sd);
   bson_free (secondary_response);
   mongoc_read_prefs_destroy (primary_read_prefs);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   future_destroy (future);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   mock_server_destroy (secondary);
   mock_server_destroy (mongos);
}


static void
test_topology_reconcile_sharded_single (void)
{
   _test_topology_reconcile_sharded (false);
}


static void
test_topology_reconcile_sharded_pooled (void)
{
   _test_topology_reconcile_sharded (true);
}


typedef struct {
   bson_mutex_t mutex;
   size_t servers;
} reconcile_test_data_t;


static void
server_opening (const mongoc_apm_server_opening_t *event)
{
   reconcile_test_data_t *data = (reconcile_test_data_t *) event->context;

   bson_mutex_lock (&data->mutex);
   data->servers++;
   bson_mutex_unlock (&data->mutex);
}


static void
test_topology_reconcile_from_handshake (void *ctx)
{
   reconcile_test_data_t data;
   mongoc_apm_callbacks_t *callbacks;
   char *host_and_port;
   char *replset_name;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_topology_t *topology;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;
   int count;
   mongoc_topology_scanner_node_t *node;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;
   mongoc_async_cmd_t *cmd;

   BSON_UNUSED (ctx);

   bson_mutex_init (&data.mutex);
   data.servers = 0;

   callbacks = mongoc_apm_callbacks_new ();

   /* single seed - not the full test_framework_get_uri */
   host_and_port = test_framework_get_host_and_port ();
   replset_name = test_framework_replset_name ();
   uri_str = bson_strdup_printf ("mongodb://%s/?replicaSet=%s", host_and_port, replset_name);

   uri = mongoc_uri_new (uri_str);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   mongoc_apm_set_server_opening_cb (callbacks, server_opening);
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, &data);
   test_framework_set_pool_ssl_opts (pool);

   /* make the bg thread lose the data race: prevent it starting by pretending
    * it already has */
   topology = _mongoc_client_pool_get_topology (pool);
   topology->scanner_state = MONGOC_TOPOLOGY_SCANNER_BG_RUNNING;

   /* ordinarily would start bg thread */
   client = mongoc_client_pool_pop (pool);

   /* command in the foreground (hello, just because it doesn't need auth) */
   r = mongoc_client_read_command_with_opts (client,
                                             "admin",
                                             tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"),
                                             NULL,
                                             tmp_bson ("{'serverId': 1}"),
                                             NULL,
                                             &error);

   ASSERT_OR_PRINT (r, error);

   /* added server descriptions */
   mc_tpld_renew_ref (&td, topology);
   ASSERT_CMPSIZE_T (mc_tpld_servers_const (td.ptr)->items_len, >, (size_t) 1);
   mc_tpld_drop_ref (&td);

   /* didn't add nodes yet, since we're not in the scanner loop */
   DL_COUNT (topology->scanner->nodes, node, count);
   ASSERT_CMPINT (count, ==, 1);

   /* if CDRIVER-2073 isn't fixed, then when we discovered the other replicas
    * during the handshake, we also created mongoc_async_cmd_t's for them
    */
   DL_COUNT (topology->scanner->async->cmds, cmd, count);
   ASSERT_CMPINT (count, ==, 0);

   /* allow pool to start scanner thread */
   bson_atomic_int_exchange (&topology->scanner_state, MONGOC_TOPOLOGY_SCANNER_OFF, bson_memory_order_seq_cst);
   mongoc_client_pool_push (pool, client);
   client = mongoc_client_pool_pop (pool);

   /* no serverId, waits for topology scan */
   r = mongoc_client_read_command_with_opts (
      client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   bson_mutex_lock (&data.mutex);
   ASSERT_CMPSIZE_T (data.servers, ==, test_framework_replset_member_count ());
   bson_mutex_unlock (&data.mutex);

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   bson_free (replset_name);
   bson_free (host_and_port);
}


/* CDRIVER-2552 in mongoc_topology_scanner_node_setup, assert (!node->retired)
 * failed after this sequence in pooled mode:
 *
 * 1. scanner discovers a replica set with primary and at least one secondary
 * 2. cluster opens a new stream to the primary
 * 3. cluster handshakes the new connection by calling hello on the primary
 * 4. the primary, for some reason, suddenly omits the secondary from its host
 *    list, perhaps because the secondary was removed from the RS configuration
 * 5. scanner marks the secondary scanner node "retired" to be destroyed later
 * 6. the scanner is disconnected from the secondary for some reason
 * 7. on the next scan, mongoc_topology_scanner_node_setup sees that the
 *    secondary is disconnected, and before creating a new stream it asserts
 *    !node->retired.
 *
 * test that between step 5 and 7, mongoc_topology_scanner_reset destroys the
 * secondary node, avoiding the assert failure. test both pooled and single
 * mode for good measure.
 */
static void
test_topology_reconcile_retire_single (void)
{
   mock_server_t *secondary;
   mock_server_t *primary;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_topology_t *topology;
   mongoc_read_prefs_t *primary_read_prefs;
   mongoc_read_prefs_t *secondary_read_prefs;
   mongoc_read_prefs_t *tag_read_prefs;
   mongoc_topology_scanner_node_t *node;
   bson_error_t error;
   future_t *future;
   request_t *request;

   secondary = mock_server_new ();
   primary = mock_server_new ();
   mock_server_run (secondary);
   mock_server_run (primary);

   RS_RESPONSE_TO_HELLO (primary, WIRE_VERSION_MIN, true, false, secondary, primary);
   RS_RESPONSE_TO_HELLO (secondary, WIRE_VERSION_MIN, false, false, secondary, primary);

   /* selection timeout must be > MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS,
    * otherwise we skip second scan in pooled mode and don't hit the assert */
   uri_str = bson_strdup_printf ("mongodb://%s,%s/?replicaSet=rs"
                                 "&serverSelectionTimeoutMS=600&heartbeatFrequencyMS=999999999",
                                 mock_server_get_host_and_port (primary),
                                 mock_server_get_host_and_port (secondary));

   uri = mongoc_uri_new (uri_str);

   client = test_framework_client_new (uri_str, NULL);
   topology = client->topology;


   /* step 1: discover both replica set members */
   primary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   BSON_ASSERT (selects_server (client, primary_read_prefs, primary));
   secondary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   BSON_ASSERT (selects_server (client, secondary_read_prefs, secondary));

   /* remove secondary from primary's config */
   RS_RESPONSE_TO_HELLO (primary, WIRE_VERSION_MIN, true, false, primary);

   /* step 2: cluster opens new stream to primary - force new stream in single
    * mode by disconnecting scanner nodes (also includes step 6) */
   DL_FOREACH (topology->scanner->nodes, node)
   {
      BSON_ASSERT (node);
      BSON_ASSERT (node->stream);
      mongoc_stream_destroy (node->stream);
      node->stream = NULL;
   }

   /* step 3: run "ping" on primary, triggering a connection and handshake, thus
    * step 4 & 5: the primary tells the scanner to retire the secondary node */
   future = future_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error);
   request = mock_server_receives_msg (primary, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);

   BSON_ASSERT (!has_server_description (topology, mock_server_get_host_and_port (secondary)));

   /* server removed from topology description. in pooled mode, the scanner node
    * is untouched, in single mode mongoc_cluster_fetch_stream_single scans and
    * updates topology */


   BSON_ASSERT (!get_node (topology, mock_server_get_host_and_port (secondary)));


   /* step 7: trigger a scan by selecting with an unsatisfiable read preference.
    * should not crash with BSON_ASSERT. */
   tag_read_prefs = mongoc_read_prefs_new (MONGOC_READ_NEAREST);
   mongoc_read_prefs_add_tag (tag_read_prefs, tmp_bson ("{'key': 'value'}"));
   BSON_ASSERT (!mongoc_client_select_server (client, false, tag_read_prefs, NULL));


   BSON_ASSERT (!get_node (topology, mock_server_get_host_and_port (secondary)));

   mongoc_client_destroy (client);


   future_destroy (future);
   mock_server_destroy (primary);
   mock_server_destroy (secondary);
   mongoc_read_prefs_destroy (primary_read_prefs);
   mongoc_read_prefs_destroy (secondary_read_prefs);
   mongoc_read_prefs_destroy (tag_read_prefs);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
}


/* CDRIVER-2552 in mongoc_topology_scanner_start, assert (!node->cmd)
 * failed after this sequence in libmongoc 1.6.0:
 *
 * 1. scanner discovers a replica set with primary
 * 2. cluster opens a new stream to the primary
 * 3. cluster handshakes the new connection by calling hello on the primary
 * 4. the primary suddenly includes a new secondary in its host list, perhaps
 *    because the secondary was added
 * 5. _mongoc_topology_update_from_handshake adds the secondary to the topology
 *    and erroneously creates a scanner node with an async_cmd_t for it,
 *    although it's not in the scanner loop
 * 6. on the next mongoc_topology_scanner_start, assert (!node->cmd) fails
 *
 * test that in step 5 the new node has no new async_cmd_t
 */
static void
test_topology_reconcile_add_single (void)
{
   mock_server_t *secondary;
   mock_server_t *primary;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_topology_t *topology;
   mongoc_read_prefs_t *primary_read_prefs;
   mongoc_topology_scanner_node_t *node;
   bson_error_t error;
   future_t *future;
   request_t *request;

   secondary = mock_server_new ();
   primary = mock_server_new ();
   mock_server_run (secondary);
   mock_server_run (primary);

   /* omit secondary from primary's hello, to start with */
   RS_RESPONSE_TO_HELLO (primary, WIRE_VERSION_MIN, true, false, primary);
   RS_RESPONSE_TO_HELLO (secondary, WIRE_VERSION_MIN, false, false, secondary, primary);

   /* selection timeout must be > MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS,
    * otherwise we skip second scan in pooled mode and don't hit the assert */
   uri_str = bson_strdup_printf ("mongodb://%s,%s/?replicaSet=rs"
                                 "&serverSelectionTimeoutMS=600&heartbeatFrequencyMS=999999999",
                                 mock_server_get_host_and_port (primary),
                                 mock_server_get_host_and_port (secondary));

   uri = mongoc_uri_new (uri_str);

   client = test_framework_client_new (uri_str, NULL);
   topology = client->topology;

   /* step 1: discover primary */
   primary_read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   BSON_ASSERT (selects_server (client, primary_read_prefs, primary));

   /* add secondary to primary's config */
   RS_RESPONSE_TO_HELLO (primary, WIRE_VERSION_MIN, true, false, primary, secondary);

   /* step 2: cluster opens new stream to primary - force new stream in single
    * mode by disconnecting primary scanner node */
   node = get_node (topology, mock_server_get_host_and_port (primary));
   BSON_ASSERT (node);
   BSON_ASSERT (node->stream);
   mongoc_stream_destroy (node->stream);
   node->stream = NULL;

   /* step 3: run "ping" on primary, triggering a connection and handshake, thus
    * step 4 & 5: we add the secondary to the topology description */
   future = future_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error);
   request = mock_server_receives_msg (primary, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);

   /* added server description */
   BSON_ASSERT (has_server_description (topology, mock_server_get_host_and_port (secondary)));

   node = get_node (topology, mock_server_get_host_and_port (secondary));

   /* in single mode the client completes a scan inline and frees all cmds */
   BSON_ASSERT (!topology->scanner->async->cmds);
   BSON_ASSERT (node);


   mongoc_client_destroy (client);


   future_destroy (future);
   mock_server_destroy (primary);
   mock_server_destroy (secondary);
   mongoc_read_prefs_destroy (primary_read_prefs);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
}


void
test_topology_reconcile_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (
      suite, "/TOPOLOGY/reconcile/rs/pooled", test_topology_reconcile_rs_pooled, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/TOPOLOGY/reconcile/rs/single", test_topology_reconcile_rs_single, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite, "/TOPOLOGY/reconcile/sharded/pooled", test_topology_reconcile_sharded_pooled);
   TestSuite_AddMockServerTest (suite, "/TOPOLOGY/reconcile/sharded/single", test_topology_reconcile_sharded_single);
   TestSuite_AddFull (suite,
                      "/TOPOLOGY/reconcile/from_handshake",
                      test_topology_reconcile_from_handshake,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);
   TestSuite_AddMockServerTest (
      suite, "/TOPOLOGY/reconcile/retire/single", test_topology_reconcile_retire_single, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/TOPOLOGY/reconcile/add/single", test_topology_reconcile_add_single, test_framework_skip_if_slow);
}
