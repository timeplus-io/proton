#include <mongoc/mongoc.h>
#include <mongoc/mongoc-uri-private.h>
#include <mongoc/mongoc-client-pool-private.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-server-api-private.h"
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-topology-background-monitoring-private.h"
#include "TestSuite.h"

#include "test-libmongoc.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "test-conveniences.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "topology-test"

typedef struct {
   size_t n_started;
   size_t n_succeeded;
   size_t n_failed;
   size_t n_unknowns;
   bson_mutex_t mutex;
} checks_t;

static void
checks_init (checks_t *checks)
{
   memset (checks, 0, sizeof (*checks));
   bson_mutex_init (&checks->mutex);
}

static void
checks_cleanup (checks_t *checks)
{
   bson_mutex_destroy (&checks->mutex);
}

static bool
checks_cmp (checks_t *checks, const char *metric, char cmp, size_t expected)
{
   size_t actual = 0;

   bson_mutex_lock (&checks->mutex);
   if (0 == strcmp (metric, "n_started")) {
      actual = checks->n_started;
   } else if (0 == strcmp (metric, "n_succeeded")) {
      actual = checks->n_succeeded;
   } else if (0 == strcmp (metric, "n_failed")) {
      actual = checks->n_failed;
   } else if (0 == strcmp (metric, "n_unknowns")) {
      actual = checks->n_unknowns;
   } else {
      test_error ("unknown metric: %s", metric);
   }

   bson_mutex_unlock (&checks->mutex);

   if (cmp == '=') {
      return actual == expected;
   } else if (cmp == '>') {
      return actual > expected;
   } else if (cmp == '<') {
      return actual < expected;
   } else {
      test_error ("unknown comparison: %c", cmp);
   }
   return false;
}

static void
check_started (const mongoc_apm_server_heartbeat_started_t *event)
{
   checks_t *c;

   c = (checks_t *) mongoc_apm_server_heartbeat_started_get_context (event);
   bson_mutex_lock (&c->mutex);
   c->n_started++;
   bson_mutex_unlock (&c->mutex);
}


static void
check_succeeded (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   checks_t *c;

   c = (checks_t *) mongoc_apm_server_heartbeat_succeeded_get_context (event);
   bson_mutex_lock (&c->mutex);
   c->n_succeeded++;
   bson_mutex_unlock (&c->mutex);
}


static void
check_failed (const mongoc_apm_server_heartbeat_failed_t *event)
{
   checks_t *c;

   c = (checks_t *) mongoc_apm_server_heartbeat_failed_get_context (event);
   bson_mutex_lock (&c->mutex);
   c->n_failed++;
   bson_mutex_unlock (&c->mutex);
}

static void
server_changed_callback (const mongoc_apm_server_changed_t *event)
{
   checks_t *c;
   const mongoc_server_description_t *sd;

   c = (checks_t *) mongoc_apm_server_changed_get_context (event);
   bson_mutex_lock (&c->mutex);
   sd = mongoc_apm_server_changed_get_new_description (event);
   if (sd->type == MONGOC_SERVER_UNKNOWN) {
      c->n_unknowns++;
   }
   bson_mutex_unlock (&c->mutex);
}


static mongoc_apm_callbacks_t *
heartbeat_callbacks (void)
{
   mongoc_apm_callbacks_t *callbacks;

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_server_heartbeat_started_cb (callbacks, check_started);
   mongoc_apm_set_server_heartbeat_succeeded_cb (callbacks, check_succeeded);
   mongoc_apm_set_server_heartbeat_failed_cb (callbacks, check_failed);
   mongoc_apm_set_server_changed_cb (callbacks, server_changed_callback);

   return callbacks;
}

static void
test_topology_client_creation (void)
{
   mongoc_uri_t *uri;
   mongoc_topology_scanner_node_t *node;
   mongoc_topology_t *topology_a;
   mongoc_topology_t *topology_b;
   mongoc_client_t *client_a;
   mongoc_client_t *client_b;
   mongoc_stream_t *topology_stream;
   mongoc_server_stream_t *server_stream;
   bson_error_t error;

   uri = test_framework_get_uri ();
   mongoc_uri_set_option_as_int32 (uri, "localThresholdMS", 42);
   mongoc_uri_set_option_as_int32 (uri, "connectTimeoutMS", 12345);
   mongoc_uri_set_option_as_int32 (uri, "serverSelectionTimeoutMS", 54321);

   /* create two clients directly */
   client_a = test_framework_client_new_from_uri (uri, NULL);
   client_b = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client_a);
   BSON_ASSERT (client_b);

#ifdef MONGOC_ENABLE_SSL
   test_framework_set_ssl_opts (client_a);
   test_framework_set_ssl_opts (client_b);
#endif

   /* ensure that they are using different topologies */
   topology_a = client_a->topology;
   topology_b = client_b->topology;
   BSON_ASSERT (topology_a);
   BSON_ASSERT (topology_b);
   BSON_ASSERT (topology_a != topology_b);

   BSON_ASSERT (topology_a->local_threshold_msec == 42);
   BSON_ASSERT (topology_a->connect_timeout_msec == 12345);
   BSON_ASSERT (topology_a->server_selection_timeout_msec == 54321);

   /* ensure that their topologies are running in single-threaded mode */
   BSON_ASSERT (topology_a->single_threaded);
   BSON_ASSERT (topology_a->scanner_state == MONGOC_TOPOLOGY_SCANNER_OFF);

   /* ensure that we are sharing streams with the client */
   server_stream = mongoc_cluster_stream_for_reads (&client_a->cluster, NULL, NULL, NULL, NULL, &error);

   ASSERT_OR_PRINT (server_stream, error);
   node = mongoc_topology_scanner_get_node (client_a->topology->scanner, server_stream->sd->id);
   BSON_ASSERT (node);
   topology_stream = node->stream;
   BSON_ASSERT (topology_stream);
   BSON_ASSERT (topology_stream == server_stream->stream);

   mongoc_server_stream_cleanup (server_stream);
   mongoc_client_destroy (client_a);
   mongoc_client_destroy (client_b);
   mongoc_uri_destroy (uri);
}

static void
assert_topology_state (mongoc_topology_t *topology, mongoc_topology_scanner_state_t state)
{
   ASSERT (topology);
   ASSERT (topology->scanner_state == state);
}

static void
test_topology_thread_start_stop (void)
{
   mongoc_client_pool_t *pool;
   mongoc_topology_t *topology;

   pool = test_framework_new_default_client_pool ();
   topology = _mongoc_client_pool_get_topology (pool);

   /* Test starting up the scanner */
   _mongoc_topology_background_monitoring_start (topology);
   assert_topology_state (topology, MONGOC_TOPOLOGY_SCANNER_BG_RUNNING);

   /* Test that starting the topology while it is already
      running is ok to do. */
   _mongoc_topology_background_monitoring_start (topology);
   assert_topology_state (topology, MONGOC_TOPOLOGY_SCANNER_BG_RUNNING);

   /* Test that we can stop the topology */
   _mongoc_topology_background_monitoring_stop (topology);
   assert_topology_state (topology, MONGOC_TOPOLOGY_SCANNER_OFF);

   /* Test that stopping the topology when it is already
      stopped is ok to do. */
   _mongoc_topology_background_monitoring_stop (topology);
   assert_topology_state (topology, MONGOC_TOPOLOGY_SCANNER_OFF);

   /* Test that we can start the topology again after stopping it */
   _mongoc_topology_background_monitoring_start (topology);
   assert_topology_state (topology, MONGOC_TOPOLOGY_SCANNER_BG_RUNNING);

   mongoc_client_pool_destroy (pool);
}

static void
test_topology_client_pool_creation (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client_a;
   mongoc_client_t *client_b;
   mongoc_topology_t *topology_a;
   mongoc_topology_t *topology_b;

   /* create two clients through a client pool */
   pool = test_framework_new_default_client_pool ();
   client_a = mongoc_client_pool_pop (pool);
   client_b = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client_a);
   BSON_ASSERT (client_b);

   /* ensure that they are using the same topology */
   topology_a = client_a->topology;
   topology_b = client_b->topology;
   BSON_ASSERT (topology_a);
   BSON_ASSERT (topology_a == topology_b);

   /* ensure that this topology is running in a background thread */
   BSON_ASSERT (!topology_a->single_threaded);
   BSON_ASSERT (topology_a->scanner_state != MONGOC_TOPOLOGY_SCANNER_OFF);

   mongoc_client_pool_push (pool, client_a);
   mongoc_client_pool_push (pool, client_b);
   mongoc_client_pool_destroy (pool);
}

static void
test_server_selection_try_once_option (void *ctx)
{
   const char *uri_strings[3] = {
      "mongodb://a", "mongodb://a/?serverSelectionTryOnce=true", "mongodb://a/?serverSelectionTryOnce=false"};

   unsigned long i;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;

   BSON_UNUSED (ctx);

   /* try_once is on by default for non-pooled, can be turned off */
   client = test_framework_client_new (uri_strings[0], NULL);
   BSON_ASSERT (client->topology->server_selection_try_once);
   mongoc_client_destroy (client);

   client = test_framework_client_new (uri_strings[1], NULL);
   BSON_ASSERT (client->topology->server_selection_try_once);
   mongoc_client_destroy (client);

   client = test_framework_client_new (uri_strings[2], NULL);
   BSON_ASSERT (!client->topology->server_selection_try_once);
   mongoc_client_destroy (client);

   /* off for pooled clients, can't be enabled */
   for (i = 0; i < sizeof (uri_strings) / sizeof (char *); i++) {
      uri = mongoc_uri_new ("mongodb://a");
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
      BSON_ASSERT (!client->topology->server_selection_try_once);
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
      mongoc_uri_destroy (uri);
   }
}

static void
_test_server_selection (bool try_once)
{
   mock_server_t *server;
   char *secondary_response;
   char *primary_response;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_read_prefs_t *primary_pref;
   future_t *future;
   bson_error_t error;
   request_t *request;
   mongoc_server_description_t *sd;

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_new ();
   mock_server_run (server);

   secondary_response = bson_strdup_printf ("{'ok': 1, "
                                            " 'isWritablePrimary': false,"
                                            " 'secondary': true,"
                                            " 'setName': 'rs',"
                                            " 'minWireVersion': %d,"
                                            " 'maxWireVersion': %d,"
                                            " 'hosts': ['%s']}",
                                            WIRE_VERSION_MIN,
                                            WIRE_VERSION_MAX,
                                            mock_server_get_host_and_port (server));

   primary_response = bson_strdup_printf ("{'ok': 1, "
                                          " 'isWritablePrimary': true,"
                                          " 'setName': 'rs',"
                                          " 'minWireVersion': %d,"
                                          " 'maxWireVersion': %d,"
                                          " 'hosts': ['%s']}",
                                          WIRE_VERSION_MIN,
                                          WIRE_VERSION_MAX,
                                          mock_server_get_host_and_port (server));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 500);
   mongoc_uri_set_option_as_int32 (uri, "serverSelectionTimeoutMS", 100);
   if (!try_once) {
      /* serverSelectionTryOnce is on by default */
      mongoc_uri_set_option_as_bool (uri, "serverSelectionTryOnce", false);
   }

   client = test_framework_client_new_from_uri (uri, NULL);
   primary_pref = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   /* no primary, selection fails after one try */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   request = mock_server_receives_any_hello (server);
   BSON_ASSERT (request);
   reply_to_request_simple (request, secondary_response);
   request_destroy (request);

   /* the selection timeout is 100 ms, and we can't rescan until a half second
    * passes, so selection fails without another hello call */
   mock_server_set_request_timeout_msec (server, 600);
   BSON_ASSERT (!mock_server_receives_any_hello (server));
   mock_server_set_request_timeout_msec (server, get_future_timeout_ms ());

   /* selection fails */
   BSON_ASSERT (!future_get_mongoc_server_description_ptr (future));
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_SERVER_SELECTION);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_SERVER_SELECTION_FAILURE);
   ASSERT_STARTSWITH (error.message, "No suitable servers found");

   if (try_once) {
      ASSERT_CONTAINS (error.message, "serverSelectionTryOnce");
   } else {
      ASSERT_CONTAINS (error.message, "serverselectiontimeoutms");
   }

   BSON_ASSERT (client->topology->stale);
   future_destroy (future);

   _mongoc_usleep (510 * 1000); /* one heartbeat, plus a few milliseconds */

   /* second selection, now we try hello again */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   request = mock_server_receives_any_hello (server);
   BSON_ASSERT (request);

   /* the secondary is now primary, selection succeeds */
   reply_to_request_simple (request, primary_response);
   sd = future_get_mongoc_server_description_ptr (future);
   BSON_ASSERT (sd);
   BSON_ASSERT (!client->topology->stale);
   request_destroy (request);
   future_destroy (future);

   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (primary_pref);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   bson_free (secondary_response);
   bson_free (primary_response);
   mock_server_destroy (server);
}

static void
test_server_selection_try_once (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_server_selection (true);
}

static void
test_server_selection_try_once_false (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_server_selection (false);
}

static void
host_list_init (mongoc_host_list_t *host_list, int family, const char *host, uint16_t port)
{
   memset (host_list, 0, sizeof *host_list);
   host_list->family = family;
   bson_snprintf (host_list->host, sizeof host_list->host, "%s", host);
   bson_snprintf (host_list->host_and_port, sizeof host_list->host_and_port, "%s:%hu", host, port);
}

static void
_test_topology_invalidate_server (bool pooled)
{
   mongoc_server_description_t *fake_sd;
   const mongoc_server_description_t *sd;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   bson_error_t error;
   mongoc_host_list_t fake_host_list;
   uint32_t fake_id = 42;
   uint32_t id;
   mongoc_server_stream_t *server_stream;
   checks_t checks;
   mc_shared_tpld td;
   mc_tpld_modification tdmod;

   checks_init (&checks);

   mongoc_uri_t *const uri = test_framework_get_uri ();
   /* no auto heartbeat */
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", INT32_MAX);
   mongoc_uri_set_option_as_int32 (uri, "connectTimeoutMS", 3000);

   const size_t server_count = test_framework_server_count ();
   mongoc_apm_callbacks_t *const callbacks = heartbeat_callbacks ();

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      mongoc_client_pool_set_apm_callbacks (pool, callbacks, &checks);
      test_framework_set_pool_ssl_opts (pool);
      client = mongoc_client_pool_pop (pool);

      /* wait for all nodes to be scanned. */
      WAIT_UNTIL (checks_cmp (&checks, "n_succeeded", '=', server_count));

      /* background scanner complains about failed connection */
      capture_logs (true);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      test_framework_set_ssl_opts (client);
   }

   /* call explicitly */
   server_stream = mongoc_cluster_stream_for_reads (&client->cluster, NULL, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   sd = server_stream->sd;
   id = server_stream->sd->id;
   BSON_ASSERT (sd->type == MONGOC_SERVER_STANDALONE || sd->type == MONGOC_SERVER_RS_PRIMARY ||
                sd->type == MONGOC_SERVER_MONGOS);

   ASSERT_CMPINT64 (sd->round_trip_time_msec, !=, (int64_t) -1);

   _mongoc_topology_invalidate_server (client->topology, id);
   td = mc_tpld_take_ref (client->topology);
   sd = mongoc_set_get_const (mc_tpld_servers_const (td.ptr), id);
   BSON_ASSERT (sd);
   BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);
   ASSERT_CMPINT64 (sd->round_trip_time_msec, ==, (int64_t) -1);

   fake_sd = (mongoc_server_description_t *) bson_malloc0 (sizeof (*fake_sd));

   /* insert a 'fake' server description and ensure that it is invalidated by
    * driver */
   host_list_init (&fake_host_list, AF_INET, "fakeaddress", 27033);
   mongoc_server_description_init (fake_sd, fake_host_list.host_and_port, fake_id);

   fake_sd->type = MONGOC_SERVER_STANDALONE;
   tdmod = mc_tpld_modify_begin (client->topology);
   mongoc_set_add (mc_tpld_servers (tdmod.new_td), fake_id, fake_sd);
   mongoc_topology_scanner_add (client->topology->scanner, &fake_host_list, fake_id, false);
   mc_tpld_modify_commit (tdmod);
   BSON_ASSERT (!mongoc_cluster_stream_for_server (&client->cluster, fake_id, true, NULL, NULL, &error));

   mc_tpld_renew_ref (&td, client->topology);
   sd = mongoc_set_get_const (mc_tpld_servers_const (td.ptr), fake_id);
   /* A single threaded client, during reconnect, will scan ALL servers.
    * When it receives a response from one of those nodes, showing that
    * "fakeaddress" is not in the host list, it will remove the
    * server description from the topology description. */
   if (!pooled && test_framework_is_replset ()) {
      BSON_ASSERT (!sd);
   } else {
      BSON_ASSERT (sd);
      BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);
      BSON_ASSERT (sd->error.domain != 0);
      ASSERT_CMPINT64 (sd->round_trip_time_msec, ==, (int64_t) -1);
      BSON_ASSERT (bson_empty (&sd->last_hello_response));
      BSON_ASSERT (bson_empty (&sd->hosts));
      BSON_ASSERT (bson_empty (&sd->passives));
      BSON_ASSERT (bson_empty (&sd->arbiters));
      BSON_ASSERT (bson_empty (&sd->compressors));
   }

   mongoc_server_stream_cleanup (server_stream);
   mongoc_uri_destroy (uri);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
   mongoc_apm_callbacks_destroy (callbacks);
   checks_cleanup (&checks);
   mc_tpld_drop_ref (&td);
}

static void
test_topology_invalidate_server_single (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_topology_invalidate_server (false);
}

static void
test_topology_invalidate_server_pooled (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_topology_invalidate_server (true);
}

static void
test_invalid_cluster_node (void *ctx)
{
   mongoc_client_pool_t *pool;
   mongoc_cluster_node_t *cluster_node;
   bson_error_t error;
   mongoc_client_t *client;
   mongoc_cluster_t *cluster;
   mongoc_server_stream_t *server_stream;
   uint32_t id;
   const mongoc_server_description_t *sd;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;
   mc_tpld_modification tdmod;

   BSON_UNUSED (ctx);

   /* use client pool, this test is only valid when multi-threaded */
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);
   cluster = &client->cluster;

   /* load stream into cluster */
   server_stream = mongoc_cluster_stream_for_reads (&client->cluster, NULL, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   id = server_stream->sd->id;
   mongoc_server_stream_cleanup (server_stream);

   cluster_node = (mongoc_cluster_node_t *) mongoc_set_get (cluster->nodes, id);
   BSON_ASSERT (cluster_node);
   BSON_ASSERT (cluster_node->stream);

   td = mc_tpld_take_ref (client->topology);
   sd = mongoc_topology_description_server_by_id_const (td.ptr, id, &error);
   ASSERT_OR_PRINT (sd, error);
   /* Both generations match, and are the first generation. */
   ASSERT_CMPINT32 (cluster_node->handshake_sd->generation, ==, 0);
   ASSERT_CMPINT32 (mc_tpl_sd_get_generation (sd, &kZeroServiceId), ==, 0);

   /* update the server's generation, simulating a connection pool clearing */
   tdmod = mc_tpld_modify_begin (client->topology);
   mc_tpl_sd_increment_generation (mongoc_topology_description_server_by_id (tdmod.new_td, id, &error),
                                   &kZeroServiceId);
   mc_tpld_modify_commit (tdmod);

   /* cluster discards node and creates new one with the current generation */
   server_stream = mongoc_cluster_stream_for_server (&client->cluster, id, true, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   cluster_node = (mongoc_cluster_node_t *) mongoc_set_get (cluster->nodes, id);
   ASSERT_CMPINT64 (cluster_node->handshake_sd->generation, ==, 1);

   mongoc_server_stream_cleanup (server_stream);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mc_tpld_drop_ref (&td);
}

static void
test_max_wire_version_race_condition (void *ctx)
{
   mongoc_server_description_t *sd;
   mongoc_database_t *database;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   bson_error_t error;
   mongoc_server_stream_t *server_stream;
   uint32_t id;
   mc_tpld_modification tdmod;
   bool r;

   BSON_UNUSED (ctx);

   /* connect directly and add our user, test is only valid with auth */
   client = test_framework_new_default_client ();
   database = mongoc_client_get_database (client, "test");
   (void) mongoc_database_remove_user (database, "pink", &error);

   r = mongoc_database_add_user (
      database, "pink", "panther", tmp_bson ("[{'role': 'read', 'db': 'test'}]"), NULL, &error);

   ASSERT_OR_PRINT (r, error);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);

   /* use client pool, test is only valid when multi-threaded */
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);

   /* load stream into cluster */
   server_stream = mongoc_cluster_stream_for_reads (&client->cluster, NULL, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   id = server_stream->sd->id;
   mongoc_server_stream_cleanup (server_stream);

   /* "disconnect": increment generation and reset server description */
   tdmod = mc_tpld_modify_begin (client->topology);
   sd = mongoc_set_get (mc_tpld_servers (tdmod.new_td), id);
   BSON_ASSERT (sd);
   mc_tpl_sd_increment_generation (sd, &kZeroServiceId);
   mongoc_server_description_reset (sd);
   mc_tpld_modify_commit (tdmod);

   /* new stream, ensure that we can still auth with cached wire version */
   server_stream = mongoc_cluster_stream_for_server (&client->cluster, id, true, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   BSON_ASSERT (server_stream);

   mongoc_server_stream_cleanup (server_stream);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}


static void
test_cooldown_standalone (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_read_prefs_t *primary_pref;
   future_t *future;
   bson_error_t error;
   request_t *request;
   mongoc_server_description_t *sd;
   int64_t start;

   server = mock_server_new ();
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   primary_pref = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   /* first hello fails, selection fails */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   request = mock_server_receives_any_hello (server);
   BSON_ASSERT (request);
   reply_to_request_with_hang_up (request);
   BSON_ASSERT (!future_get_mongoc_server_description_ptr (future));
   request_destroy (request);
   future_destroy (future);

   /* second selection doesn't try to call hello: we're in cooldown */
   start = bson_get_monotonic_time ();
   sd = mongoc_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   BSON_ASSERT (!sd);
   /* waited less than 500ms (minHeartbeatFrequencyMS), in fact
    * didn't wait at all since all nodes are in cooldown */
   ASSERT_CMPINT64 (bson_get_monotonic_time () - start, <, (int64_t) 500000);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_SERVER_SELECTION,
                          MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                          "No servers yet eligible for rescan");

   _mongoc_usleep (1000 * 1000); /* 1 second */

   /* third selection doesn't try to call hello: we're still in cooldown */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   mock_server_set_request_timeout_msec (server, 100);
   BSON_ASSERT (!mock_server_receives_any_hello (server)); /* no hello call */
   BSON_ASSERT (!future_get_mongoc_server_description_ptr (future));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "No suitable servers");

   future_destroy (future);
   mock_server_set_request_timeout_msec (server, get_future_timeout_ms ());

   _mongoc_usleep (5100 * 1000); /* 5.1 seconds */

   /* cooldown ends, now we try hello again, this time succeeding */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);
   request = mock_server_receives_any_hello (server); /* not in cooldown now */
   BSON_ASSERT (request);
   reply_to_request_simple (request,
                            tmp_str ("{'ok': 1,"
                                     " 'isWritablePrimary': true,"
                                     " 'minWireVersion': %d,"
                                     " 'maxWireVersion': %d}",
                                     WIRE_VERSION_MIN,
                                     WIRE_VERSION_MAX));
   sd = future_get_mongoc_server_description_ptr (future);
   BSON_ASSERT (sd);
   request_destroy (request);
   future_destroy (future);

   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (primary_pref);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_cooldown_rs (void)
{
   mock_server_t *servers[2]; /* two secondaries, no primary */
   int i;
   mongoc_client_t *client;
   mongoc_read_prefs_t *primary_pref;
   char *secondary_response;
   char *primary_response;
   future_t *future;
   bson_error_t error;
   request_t *request;
   mongoc_server_description_t *sd;

   for (i = 0; i < 2; i++) {
      servers[i] = mock_server_new ();
      mock_server_run (servers[i]);
   }

   {
      char *uri_str = bson_strdup_printf ("mongodb://localhost:%hu/?replicaSet=rs"
                                          "&serverSelectionTimeoutMS=100"
                                          "&connectTimeoutMS=100",
                                          mock_server_get_port (servers[0]));

      mongoc_uri_t *const uri = mongoc_uri_new_with_error (uri_str, &error);
      ASSERT_OR_PRINT (uri, error);

      // Prevent retryable handshakes from interfering with mock server hangups.
      mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYREADS, false);

      client = test_framework_client_new_from_uri (uri, NULL);

      bson_free (uri_str);
      mongoc_uri_destroy (uri);
   }

   primary_pref = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   secondary_response = bson_strdup_printf ("{'ok': 1,"
                                            " 'isWritablePrimary': false,"
                                            " 'minWireVersion': %d,"
                                            " 'maxWireVersion': %d, "
                                            " 'secondary': true,"
                                            " 'setName': 'rs',"
                                            " 'hosts': ['localhost:%hu', 'localhost:%hu']}",
                                            WIRE_VERSION_MIN,
                                            WIRE_VERSION_MAX,
                                            mock_server_get_port (servers[0]),
                                            mock_server_get_port (servers[1]));

   primary_response = bson_strdup_printf ("{'ok': 1,"
                                          " 'isWritablePrimary': true,"
                                          " 'minWireVersion': %d,"
                                          " 'maxWireVersion': %d ,"
                                          " 'setName': 'rs',"
                                          " 'hosts': ['localhost:%hu']}",
                                          WIRE_VERSION_MIN,
                                          WIRE_VERSION_MAX,
                                          mock_server_get_port (servers[1]));

   /* server 0 is a secondary. */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);

   request = mock_server_receives_any_hello (servers[0]);
   BSON_ASSERT (request);
   reply_to_request_simple (request, secondary_response);
   request_destroy (request);

   /* server 0 told us about server 1. we check it immediately but it's down. */
   request = mock_server_receives_any_hello (servers[1]);
   BSON_ASSERT (request);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* selection fails. */
   BSON_ASSERT (!future_get_mongoc_server_description_ptr (future));
   future_destroy (future);

   _mongoc_usleep (1000 * 1000); /* 1 second */

   /* second selection doesn't try hello on server 1: it's in cooldown */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);

   request = mock_server_receives_any_hello (servers[0]);
   BSON_ASSERT (request);
   reply_to_request_simple (request, secondary_response);
   request_destroy (request);

   mock_server_set_request_timeout_msec (servers[1], 100);
   BSON_ASSERT (!mock_server_receives_any_hello (servers[1]));
   mock_server_set_request_timeout_msec (servers[1], get_future_timeout_ms ());

   /* still no primary */
   BSON_ASSERT (!future_get_mongoc_server_description_ptr (future));
   future_destroy (future);

   _mongoc_usleep (5100 * 1000); /* 5.1 seconds. longer than 5 sec cooldown. */

   /* cooldown ends, now we try hello on server 1, this time succeeding */
   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);

   request = mock_server_receives_any_hello (servers[1]);
   BSON_ASSERT (request);
   reply_to_request_simple (request, primary_response);
   request_destroy (request);

   /* server 0 doesn't need to respond */
   sd = future_get_mongoc_server_description_ptr (future);
   BSON_ASSERT (sd);
   future_destroy (future);

   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (primary_pref);
   mongoc_client_destroy (client);
   bson_free (secondary_response);
   bson_free (primary_response);
   mock_server_destroy (servers[0]);
   mock_server_destroy (servers[1]);
}


/* test single-threaded client's cooldown with serverSelectionTryOnce false */
static void
test_cooldown_retry (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_read_prefs_t *primary_pref;
   future_t *future;
   bson_error_t error;
   request_t *request;
   mongoc_server_description_t *sd;
   int64_t start;
   int64_t duration;

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false);
   client = test_framework_client_new_from_uri (uri, NULL);
   primary_pref = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   future = future_topology_select (client->topology, MONGOC_SS_READ, primary_pref, NULL, &error);

   /* first hello fails */
   request = mock_server_receives_any_hello (server);
   BSON_ASSERT (request);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* after cooldown passes, driver sends another hello */
   start = bson_get_monotonic_time ();
   request = mock_server_receives_any_hello (server);
   BSON_ASSERT (request);
   duration = bson_get_monotonic_time () - start;
   /* waited at least cooldownMS, but not unreasonably longer than that */
   ASSERT_CMPINT64 (duration, >, (int64_t) 5 * 1000 * 1000);
   ASSERT_CMPINT64 (duration, <, (int64_t) 10 * 1000 * 1000);

   reply_to_request_simple (request,
                            tmp_str ("{'ok': 1,"
                                     " 'isWritablePrimary': true,"
                                     " 'minWireVersion': %d,"
                                     " 'maxWireVersion': %d}",
                                     WIRE_VERSION_MIN,
                                     WIRE_VERSION_MAX));
   sd = future_get_mongoc_server_description_ptr (future);
   ASSERT_OR_PRINT (sd, error);
   request_destroy (request);
   future_destroy (future);

   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (primary_pref);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static void
_test_select_succeed (bool try_once)
{
   const int32_t connect_timeout_ms = 200;

   mock_server_t *primary;
   mock_server_t *secondary;
   mongoc_server_description_t *sd;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   future_t *future;
   int64_t start;
   bson_error_t error;
   int64_t duration_usec;

   primary = mock_server_new ();
   mock_server_run (primary);

   secondary = mock_server_new ();
   mock_server_run (secondary);

   /* Note: do not use localhost here. If localhost has both A and AAAA records,
    * an attempt to connect to IPv6 occurs first. Most platforms refuse the IPv6
    * attempt immediately, so IPv4 succeeds immediately. Windows is an
    * exception, and waits 1 second before refusing:
    * https://support.microsoft.com/en-us/help/175523/info-winsock-tcp-connection-performance-to-unused-ports
    */
   /* primary auto-responds, secondary never responds */
   mock_server_auto_hello (primary,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['127.0.0.1:%hu', '127.0.0.1:%hu']}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_port (primary),
                           mock_server_get_port (secondary));

   uri_str = bson_strdup_printf ("mongodb://127.0.0.1:%hu,127.0.0.1:%hu/"
                                 "?replicaSet=rs&connectTimeoutMS=%d",
                                 mock_server_get_port (primary),
                                 mock_server_get_port (secondary),
                                 connect_timeout_ms);

   uri = mongoc_uri_new (uri_str);
   BSON_ASSERT (uri);
   if (!try_once) {
      /* override default */
      mongoc_uri_set_option_as_bool (uri, "serverSelectionTryOnce", false);
   }

   client = test_framework_client_new_from_uri (uri, NULL);

   /* start waiting for a primary (NULL read pref) */
   start = bson_get_monotonic_time ();
   future = future_topology_select (client->topology, MONGOC_SS_READ, NULL, NULL, &error);

   /* selection succeeds */
   sd = future_get_mongoc_server_description_ptr (future);
   ASSERT_OR_PRINT (sd, error);
   future_destroy (future);

   duration_usec = bson_get_monotonic_time () - start;

   ASSERT_ALMOST_EQUAL (duration_usec / 1000, connect_timeout_ms);

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   mongoc_server_description_destroy (sd);
   mock_server_destroy (primary);
   mock_server_destroy (secondary);
}


/* CDRIVER-1219: a secondary is unavailable, scan should take connectTimeoutMS,
 * then we select primary */
static void
test_select_after_timeout (void)
{
   _test_select_succeed (false);
}


/* CDRIVER-1219: a secondary is unavailable, scan should try it once,
 * then we select primary */
static void
test_select_after_try_once (void)
{
   _test_select_succeed (true);
}


static void
test_multiple_selection_errors (void)
{
   const char *const uri = "mongodb://doesntexist.invalid,example.invalid/"
                           "?replicaSet=rs&connectTimeoutMS=100";
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;

   client = test_framework_client_new (uri, NULL);
   mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, &reply, &error);

   ASSERT_CMPINT (MONGOC_ERROR_SERVER_SELECTION, ==, error.domain);
   ASSERT_CMPINT (MONGOC_ERROR_SERVER_SELECTION_FAILURE, ==, error.code);

   /* Like:
    * "No suitable servers found (`serverselectiontryonce` set):
    *  [Failed to resolve 'doesntexist.invalid']
    *  [Failed to resolve 'example.invalid']
    */
   ASSERT_CONTAINS (error.message, "No suitable servers found");
   /* either "connection error" or "connection timeout" calling hello */
   ASSERT_CONTAINS (error.message, "[Failed to resolve 'doesntexist.invalid']");
   ASSERT_CONTAINS (error.message, "[Failed to resolve 'example.invalid']");

   bson_destroy (&reply);
   mongoc_client_destroy (client);
}


static void
test_invalid_server_id (void)
{
   mongoc_client_t *client;
   bson_error_t error;

   client = test_framework_new_default_client ();

   BSON_ASSERT (
      !mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 99999, &error));
   ASSERT_STARTSWITH (error.message, "Could not find description for node");

   mongoc_client_destroy (client);
}


static bool
auto_ping (request_t *request, void *data)
{
   BSON_UNUSED (data);

   if (!request->is_command || strcasecmp (request->command_name, "ping")) {
      return false;
   }

   reply_to_request_with_ok_and_destroy (request);

   return true;
}


/* Tests CDRIVER-562: after calling hello to handshake a new connection we
 * must update topology description with the server response.
 */
static void
_test_server_removed_during_handshake (bool pooled)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;
   mongoc_server_description_t *sd;
   mongoc_server_description_t **sds;
   size_t n;

   server = mock_server_new ();
   mock_server_run (server);
   mock_server_autoresponds (server, auto_ping, NULL, NULL);
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['%s']}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_host_and_port (server));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   /* no auto heartbeat */
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", INT32_MAX);
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   /* initial connection, discover one-node replica set */
   r = mongoc_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);

   ASSERT_CMPINT (_mongoc_topology_get_type (client->topology), ==, MONGOC_TOPOLOGY_RS_WITH_PRIMARY);
   sd = mongoc_client_get_server_description (client, 1);
   ASSERT_CMPINT ((int) MONGOC_SERVER_RS_PRIMARY, ==, sd->type);
   mongoc_server_description_destroy (sd);

   /* primary changes setName */
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'BAD NAME',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['%s']}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_host_and_port (server));

   /* pretend to close a connection. does NOT affect server description yet */
   mongoc_cluster_disconnect_node (&client->cluster, 1);
   sd = mongoc_client_get_server_description (client, 1);
   /* still primary */
   ASSERT_CMPINT ((int) MONGOC_SERVER_RS_PRIMARY, ==, sd->type);
   mongoc_server_description_destroy (sd);

   /* opens new stream and runs hello again, discovers bad setName. */
   capture_logs (true);
   r = mongoc_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT (!r);
   ASSERT_CAPTURED_LOG ("topology", MONGOC_LOG_LEVEL_WARNING, "Last server removed from topology");
   capture_logs (false);

   if (!pooled) {
      ASSERT_ERROR_CONTAINS (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NOT_ESTABLISHED, "Could not find stream for node");
   } else {
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NOT_ESTABLISHED, "removed from topology");
   }

   sds = mongoc_client_get_server_descriptions (client, &n);
   ASSERT_CMPSIZE_T (n, ==, (size_t) 0);
   ASSERT_CMPINT (_mongoc_topology_get_type (client->topology), ==, MONGOC_TOPOLOGY_RS_NO_PRIMARY);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_server_descriptions_destroy_all (sds, n);
   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
}


static void
test_server_removed_during_handshake_single (void)
{
   _test_server_removed_during_handshake (false);
}


static void
test_server_removed_during_handshake_pooled (void)
{
   _test_server_removed_during_handshake (true);
}


static void
test_rtt (void *ctx)
{
   mock_server_t *server;
   mongoc_client_t *client;
   future_t *future;
   request_t *request;
   bson_error_t error;
   mongoc_server_description_t const *sd;
   int64_t rtt_msec;

   BSON_UNUSED (ctx);

   if (!TestSuite_CheckMockServerAllowed ()) {
      return;
   }

   server = mock_server_new ();
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   future = future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   request = mock_server_receives_any_hello (server);
   _mongoc_usleep (1000 * 1000); /* one second */
   reply_to_request (
      request,
      MONGOC_REPLY_NONE,
      0,
      0,
      1,
      tmp_str ("{'ok': 1, 'minWireVersion': %d, 'maxWireVersion': %d}", WIRE_VERSION_MIN, WIRE_VERSION_MAX));
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'ping': 1}"));
   reply_to_request (
      request,
      MONGOC_REPLY_NONE,
      0,
      0,
      1,
      tmp_str ("{'ok': 1, 'minWireVersion': %d, 'maxWireVersion': %d}", WIRE_VERSION_MIN, WIRE_VERSION_MAX));
   request_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);

   sd = mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 1, NULL);
   ASSERT (sd);

   /* assert, with plenty of slack, that rtt was calculated in ms, not usec */
   rtt_msec = mongoc_server_description_round_trip_time (sd);
   ASSERT_CMPINT64 (rtt_msec, >, (int64_t) 900);  /* 900 ms */
   ASSERT_CMPINT64 (rtt_msec, <, (int64_t) 9000); /* 9 seconds */

   future_destroy (future);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


/* mongoc_topology_scanner_add and mongoc_topology_scan are called while holding
 * a topology modification lock to add a discovered node and call getaddrinfo on
 * its host immediately - test that this doesn't cause a recursive acquire this
 * lock. */
static void
test_add_and_scan_failure (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   future_t *future;
   request_t *request;
   bson_error_t error;
   mongoc_server_description_t const *sd;

   server = mock_server_new ();
   mock_server_run (server);
   /* client will discover "fake" host and fail to connect */
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['%s', 'fake:1']}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_host_and_port (server));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client = mongoc_client_pool_pop (pool);
   future = future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);

   {
      mc_shared_tpld shared_tpld = mc_tpld_take_ref (client->topology);
      sd = mongoc_topology_description_server_by_id_const (shared_tpld.ptr, 1, NULL);
      ASSERT (sd);
      ASSERT_CMPSTR (mongoc_server_description_type (sd), "RSPrimary");
      mc_tpld_drop_ref (&shared_tpld);
   }

   {
      mc_shared_tpld shared_tpld = mc_tpld_take_ref (client->topology);
      sd = mongoc_topology_description_server_by_id_const (mc_tpld_unsafe_get_const (client->topology), 2, NULL);
      ASSERT (sd);
      ASSERT_CMPSTR (mongoc_server_description_type (sd), "Unknown");
      mc_tpld_drop_ref (&shared_tpld);
   }

   future_destroy (future);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


static future_t *
future_command (mongoc_client_t *client, bson_error_t *error)
{
   return future_client_command_simple (client, "admin", tmp_bson ("{'foo': 1}"), NULL, NULL, error);
}


static void
receives_command (mock_server_t *server, future_t *future)
{
   request_t *request;
   bson_error_t error;

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'foo': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);
}


static bool
has_known_server (mongoc_client_t *client)
{
   mongoc_server_description_t *sd;
   bool r;

   ASSERT (client);

   /* in this test we know the server id is always 1 */
   sd = mongoc_client_get_server_description (client, 1);
   r = (sd->type != MONGOC_SERVER_UNKNOWN);
   mongoc_server_description_destroy (sd);
   return r;
}


static void
_test_hello_retry_single (bool hangup, size_t n_failures)
{
   checks_t checks;
   mongoc_apm_callbacks_t *callbacks;
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   char *hello;
   future_t *future;
   request_t *request;
   bson_error_t error;
   int64_t t;

   checks_init (&checks);
   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500);
   mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_REPLICASET, "rs");
   if (!hangup) {
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 100);
   }

   client = test_framework_client_new_from_uri (uri, NULL);
   callbacks = heartbeat_callbacks ();
   mongoc_client_set_apm_callbacks (client, callbacks, &checks);

   hello = bson_strdup_printf ("{'ok': 1,"
                               " 'isWritablePrimary': true,"
                               " 'setName': 'rs',"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d,"
                               " 'hosts': ['%s']}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX,
                               mock_server_get_host_and_port (server));

   /* start a {foo: 1} command, handshake normally */
   future = future_command (client, &error);
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, hello);
   request_destroy (request);
   receives_command (server, future);

   /* wait for the next server check */
   _mongoc_usleep (600 * 1000);

   /* start a {foo: 1} command, server check fails and retries immediately */
   future = future_command (client, &error);
   request = mock_server_receives_any_hello (server);
   t = bson_get_monotonic_time ();
   if (hangup) {
      reply_to_request_with_hang_up (request);
   }

   request_destroy (request);

   /* retry immediately (for testing, "immediately" means less than 250ms */
   request = mock_server_receives_any_hello (server);
   ASSERT_CMPINT64 (bson_get_monotonic_time () - t, <, (int64_t) 250 * 1000);

   if (n_failures == 2u) {
      if (hangup) {
         reply_to_request_with_hang_up (request);
      }

      BSON_ASSERT (!future_get_bool (future));
      future_destroy (future);
   } else {
      reply_to_request_simple (request, hello);
      /* the {foo: 1} command finishes */
      receives_command (server, future);
   }

   request_destroy (request);

   ASSERT_CMPSIZE_T (checks.n_started, ==, 3u);
   WAIT_UNTIL (checks.n_succeeded == 3u - n_failures);
   WAIT_UNTIL (checks.n_failed == n_failures);

   if (n_failures == 2u) {
      BSON_ASSERT (!has_known_server (client));
   } else {
      BSON_ASSERT (has_known_server (client));
   }

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
   bson_free (hello);
   mongoc_apm_callbacks_destroy (callbacks);
   checks_cleanup (&checks);
}


static void
_test_hello_retry_pooled (bool hangup, size_t n_failures)
{
   checks_t checks;
   mongoc_apm_callbacks_t *callbacks;
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   char *hello;
   future_t *future;
   request_t *request;
   bson_error_t error;
   int64_t t;

   checks_init (&checks);
   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500);
   mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_REPLICASET, "rs");
   if (!hangup) {
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 100);
   }

   pool = mongoc_client_pool_new (uri);
   callbacks = heartbeat_callbacks ();
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, &checks);
   client = mongoc_client_pool_pop (pool);

   hello = bson_strdup_printf ("{'ok': 1,"
                               " 'isWritablePrimary': true,"
                               " 'setName': 'rs',"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d,"
                               " 'hosts': ['%s']}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX,
                               mock_server_get_host_and_port (server));

   /* As soon as the client is popped, background monitoring starts. */
   request = mock_server_receives_legacy_hello (server, NULL);
   reply_to_request_simple (request, hello);
   request_destroy (request);

   /* start a {foo: 1} command, handshake normally */
   future = future_command (client, &error);

   /* Another hello to handshake the connection */
   request = mock_server_receives_legacy_hello (server, NULL);
   reply_to_request_simple (request, hello);
   request_destroy (request);

   /* the {foo: 1} command finishes */
   receives_command (server, future);

   /* wait for the next server check */
   request = mock_server_receives_legacy_hello (server, NULL);
   t = bson_get_monotonic_time ();
   if (hangup) {
      reply_to_request_with_hang_up (request);
   }

   request_destroy (request);

   /* retry immediately (for testing, "immediately" means less than 250ms */
   request = mock_server_receives_legacy_hello (server, NULL);
   ASSERT_CMPINT64 (bson_get_monotonic_time () - t, <, (int64_t) 250 * 1000);
   /* The server is marked as Unknown, but immediately rescanned. This behavior
    * comes from the server monitoring spec:
    * "To handle the case that the server is truly down, the monitor makes the
    * server unselectable by marking it Unknown. To handle the case of a
    * transient network glitch or restart, the monitor immediately runs the next
    * check without waiting".
    */
   BSON_ASSERT (!has_known_server (client));
   if (n_failures == 2u) {
      if (hangup) {
         reply_to_request_with_hang_up (request);
      }
   } else {
      reply_to_request_simple (request, hello);
      WAIT_UNTIL (has_known_server (client));
   }

   request_destroy (request);

   WAIT_UNTIL (checks_cmp (&checks, "n_succeeded", '=', 3u - n_failures));
   WAIT_UNTIL (checks_cmp (&checks, "n_failed", '=', n_failures));
   BSON_ASSERT (checks_cmp (&checks, "n_started", '=', 3u));

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
   bson_free (hello);
   mongoc_apm_callbacks_destroy (callbacks);
   checks_cleanup (&checks);
}


static void
test_hello_retry_single_hangup (void)
{
   _test_hello_retry_single (true, 1u);
}


static void
test_hello_retry_single_timeout (void)
{
   _test_hello_retry_single (false, 1u);
}

static void
test_hello_retry_single_hangup_fail (void)
{
   _test_hello_retry_single (true, 2u);
}


static void
test_hello_retry_single_timeout_fail (void)
{
   _test_hello_retry_single (false, 2u);
}


static void
test_hello_retry_pooled_hangup (void)
{
   _test_hello_retry_pooled (true, 1u);
}


static void
test_hello_retry_pooled_timeout (void)
{
   _test_hello_retry_pooled (false, 1u);
}


static void
test_hello_retry_pooled_hangup_fail (void)
{
   _test_hello_retry_pooled (true, 2u);
}


static void
test_hello_retry_pooled_timeout_fail (void)
{
   _test_hello_retry_pooled (false, 2u);
}


static void
test_incompatible_error (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   bson_error_t error;
   char *msg;

   /* incompatible */
   server = mock_server_new ();
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MIN - 1,
                           WIRE_VERSION_MIN - 1);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 500);
   client = test_framework_client_new_from_uri (uri, NULL);

   /* trigger connection, fails due to incompatibility */
   ASSERT (!mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, NULL, &error));

   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_PROTOCOL,
                          MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION,
                          tmp_str ("reports wire version %d, but this version of libmongoc "
                                   "requires at least %d (MongoDB %s)",
                                   WIRE_VERSION_MIN - 1,
                                   WIRE_VERSION_MIN,
                                   _mongoc_wire_version_to_server_version (WIRE_VERSION_MIN)));

   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MAX + 1,
                           WIRE_VERSION_MAX + 1);

   /* wait until it's time for next heartbeat */
   _mongoc_usleep (600 * 1000);
   ASSERT (!mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, NULL, &error));

   msg = bson_strdup_printf ("requires wire version %d, but this version of "
                             "libmongoc only supports up to %d",
                             WIRE_VERSION_MAX + 1,
                             WIRE_VERSION_MAX);

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION, msg);

   bson_free (msg);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}


/* ensure there's no invalid access if a null bson_error_t pointer is passed
 * to mongoc_topology_compatible () */
static void
test_compatible_null_error_pointer (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_topology_description_t const *td;
   bson_error_t error;

   /* incompatible */
   server = mock_server_new ();
   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MIN - 1,
                           WIRE_VERSION_MIN - 1);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   /* trigger connection, fails due to incompatibility */
   ASSERT (!mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, NULL, &error));

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION, "");

   /* null error pointer is ok */
   td = mc_tpld_unsafe_get_const (client->topology);
   ASSERT (!mongoc_topology_compatible (td, NULL /* read prefs */, NULL /* error */));

   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static char *
cluster_time_fmt (int t)
{
   return bson_strdup_printf ("{"
                              "  'clusterTime': {'$timestamp': {'t': %d, 'i': 1}},"
                              "  'signature': {"
                              "    'hash': {'$binary': {'subType': '0', 'base64': 'Yw=='}},"
                              "    'keyId': {'$numberLong': '6446735049323708417'}"
                              "   },"
                              "  'operationTime': {'$timestamp': {'t': 1, 'i': 1}}"
                              "}",
                              t);
}

static void
test_cluster_time_updated_during_handshake (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;
   char *cluster_time;
   mongoc_server_description_t *sd;

   server = mock_server_new ();
   mock_server_run (server);
   mock_server_autoresponds (server, auto_ping, NULL, NULL);
   cluster_time = cluster_time_fmt (1);
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['%s'],"
                           " '$clusterTime': %s}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_host_and_port (server),
                           cluster_time);

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   /* set a large heartbeatFrequencyMS so we don't do a background scan in
    * between the first scan and handshake. */
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 99999);
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");

   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client = mongoc_client_pool_pop (pool);

   /* ensure a topology scan has run, populating the topology description
    * cluster time. */
   sd = mongoc_client_select_server (client, false, NULL, &error);
   ASSERT_OR_PRINT (sd, error);
   mongoc_server_description_destroy (sd);

   /* check the cluster time stored on the topology description. */
   ASSERT_MATCH (&mc_tpld_unsafe_get_const (client->topology)->cluster_time, cluster_time);
   bson_free (cluster_time);
   cluster_time = cluster_time_fmt (2);

   /* primary changes clusterTime */
   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['%s'],"
                           " '$clusterTime': %s}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_host_and_port (server),
                           cluster_time);

   /* remove the node from the cluster to trigger a hello handshake. */
   mongoc_cluster_disconnect_node (&client->cluster, 1);

   /* opens new stream and does a hello handshake (in pooled mode only). */
   r = mongoc_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   ASSERT_MATCH (&mc_tpld_unsafe_get_const (client->topology)->cluster_time, cluster_time);
   bson_free (cluster_time);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
}

/* test that when a command receives a "not primary" or "node is recovering"
 * error that the client takes the appropriate action:
 * - a pooled client should mark the server as unknown and request a full scan
 *   of the topology
 * - a single-threaded client should mark the server as unknown and mark the
 *   topology as stale.
 */
static void
_test_request_scan_on_error (
   bool pooled, const char *err_response, bool should_scan, bool should_mark_unknown, const char *server_err)
{
   mock_server_t *primary, *secondary;
   char *uri_str;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *client_pool = NULL;
   mongoc_client_t *client = NULL;
   bson_t reply;
   bson_error_t error = {0};
   future_t *future = NULL;
   request_t *request;
   const int64_t minHBMS = 10;
   int64_t ping_started_usec = 0;
   mongoc_apm_callbacks_t *callbacks;
   checks_t checks;
   mongoc_server_description_t *sd;
   uint32_t primary_id;
   mongoc_read_prefs_t *read_prefs;

   MONGOC_DEBUG ("pooled? %d", (int) pooled);
   MONGOC_DEBUG ("err_response %s", err_response);
   MONGOC_DEBUG ("should_scan %d, should_mark_unknown: %d", (int) should_scan, (int) should_mark_unknown);
   MONGOC_DEBUG ("server_error %s", server_err);

   checks_init (&checks);
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY_PREFERRED);

   primary = mock_server_new ();
   secondary = mock_server_new ();
   mock_server_run (primary);
   mock_server_run (secondary);

   RS_RESPONSE_TO_HELLO (primary, WIRE_VERSION_MIN, true, false, primary, secondary);
   RS_RESPONSE_TO_HELLO (secondary, WIRE_VERSION_MIN, false, false, primary, secondary);

   /* set a high heartbeatFrequency. Only the first and requested scans run. */
   uri_str = bson_strdup_printf ("mongodb://%s,%s/?replicaSet=rs&heartbeatFrequencyMS=999999",
                                 mock_server_get_host_and_port (primary),
                                 mock_server_get_host_and_port (secondary));
   uri = mongoc_uri_new (uri_str);
   bson_free (uri_str);

   if (pooled) {
      mongoc_topology_t *topology;
      client_pool = test_framework_client_pool_new_from_uri (uri, NULL);
      topology = _mongoc_client_pool_get_topology (client_pool);
      /* set a small minHeartbeatFrequency, so scans don't block for 500ms. */
      topology->min_heartbeat_frequency_msec = minHBMS;
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      /* set a small minHeartbeatFrequency, so scans don't block for 500ms. */
      client->topology->min_heartbeat_frequency_msec = minHBMS;
   }

   callbacks = heartbeat_callbacks ();
   if (pooled) {
      mongoc_client_pool_set_apm_callbacks (client_pool, callbacks, &checks);
   } else {
      mongoc_client_set_apm_callbacks (client, callbacks, &checks);
   }
   mongoc_apm_callbacks_destroy (callbacks);

   if (pooled) {
      client = mongoc_client_pool_pop (client_pool);
      /* Scanning starts, wait for the initial scan. */
      WAIT_UNTIL (checks_cmp (&checks, "n_succeeded", '=', 2));
   }

   sd = mongoc_client_select_server (client, true, NULL, &error);
   ASSERT_OR_PRINT (sd, error);
   primary_id = sd->id;
   mongoc_server_description_destroy (sd);
   BSON_ASSERT (checks_cmp (&checks, "n_succeeded", '=', 2));

   mongoc_uri_destroy (uri);
   ping_started_usec = bson_get_monotonic_time ();
   /* run a ping command on the primary. */
   future = future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), read_prefs, &reply, &error);
   request = mock_server_receives_msg (primary, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));

   /* Capture logs to swallow warnings about endSessions */
   capture_logs (true);

   reply_to_request_simple (request, err_response);
   request_destroy (request);
   /* don't check the return value of future. write concern errors are still
    * considered successful results. */
   future_wait (future);
   future_destroy (future);
   bson_destroy (&reply);

   sd = mongoc_client_get_server_description (client, primary_id);
   if (should_mark_unknown) {
      BSON_ASSERT (checks_cmp (&checks, "n_unknowns", '=', 1));
      /* background monitoring may have already overwritten the unknown server
       * description if the scan was requested. */
      if (pooled) {
         if (sd->type == MONGOC_SERVER_UNKNOWN) {
            if (server_err) {
               ASSERT_CMPSTR (server_err, sd->error.message);
            }
         }
      } else {
         /* after the 'ping' command and returning, the server should
          * have been marked as unknown. */
         BSON_ASSERT (sd->type == MONGOC_SERVER_UNKNOWN);
         ASSERT_CMPINT64 (sd->last_update_time_usec, >=, ping_started_usec);
         ASSERT_CMPINT64 (sd->last_update_time_usec, <=, bson_get_monotonic_time ());
         /* check that the error on the server description matches the error
          * message in the response. */
         if (server_err) {
            ASSERT_CMPSTR (server_err, sd->error.message);
         }
      }
   } else {
      BSON_ASSERT (sd->type != MONGOC_SERVER_UNKNOWN);
   }
   mongoc_server_description_destroy (sd);

   if (pooled) {
      if (should_scan) {
         /* a scan is requested immediately. wait for the scan to finish. */
         WAIT_UNTIL (checks_cmp (&checks, "n_started", '=', 4));
      } else {
         _mongoc_usleep (minHBMS * 2);
         BSON_ASSERT (checks_cmp (&checks, "n_started", '=', 2));
      }
   } else {
      /* a single threaded client may mark the topology as stale. if a scan
       * should occur, it won't be triggered until the next command. */
      future = future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), read_prefs, &reply, &error);
      if (should_scan || !should_mark_unknown) {
         request = mock_server_receives_msg (primary, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
      } else {
         /* if the primary was marked as UNKNOWN, and no scan occurred, the ping
          * goes to the secondary. */
         request = mock_server_receives_msg (secondary, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
      }
      reply_to_request_simple (request, "{'ok': 1}");
      request_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);
      bson_destroy (&reply);
      if (should_scan) {
         BSON_ASSERT (checks_cmp (&checks, "n_started", '=', 4));
      } else {
         BSON_ASSERT (checks_cmp (&checks, "n_started", '=', 2));
      }
   }

   if (pooled) {
      mongoc_client_pool_push (client_pool, client);
      mongoc_client_pool_destroy (client_pool);
   } else {
      mongoc_client_destroy (client);
   }
   mock_server_destroy (primary);
   mock_server_destroy (secondary);
   mongoc_read_prefs_destroy (read_prefs);
   checks_cleanup (&checks);
}

static void
test_last_server_removed_warning (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_server_description_t *description;
   mongoc_read_prefs_t *read_prefs;
   bson_error_t error;

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "set");
   client = test_framework_client_new_from_uri (uri, NULL);
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   mock_server_auto_hello (server,
                           "{'ok': 1,"
                           " 'isWritablePrimary': true,"
                           " 'setName': 'rs',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': ['127.0.0.1:%hu']}",
                           WIRE_VERSION_MIN,
                           WIRE_VERSION_MAX,
                           mock_server_get_port (server));

   capture_logs (true);
   description = mongoc_topology_select (client->topology, MONGOC_SS_READ, read_prefs, NULL, &error);
   ASSERT_CAPTURED_LOG ("topology", MONGOC_LOG_LEVEL_WARNING, "Last server removed from topology");
   capture_logs (false);

   mongoc_server_description_destroy (description);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
test_request_scan_on_error (void)
{
#define TEST_POOLED(msg, should_scan, should_mark_unknown, server_err) \
   _test_request_scan_on_error (true, msg, should_scan, should_mark_unknown, server_err)
#define TEST_SINGLE(msg, should_scan, should_mark_unknown, server_err) \
   _test_request_scan_on_error (false, msg, should_scan, should_mark_unknown, server_err)
#define TEST_BOTH(msg, should_scan, should_mark_unknown, server_err) \
   TEST_POOLED (msg, should_scan, should_mark_unknown, server_err);  \
   TEST_SINGLE (msg, should_scan, should_mark_unknown, server_err)

   TEST_BOTH (
      "{'ok': 0, 'errmsg': 'not master'}", true /* should_scan */, true /* should_mark_unknown */, "not master");
   /* "node is recovering" behaves differently for single and pooled clients. */
   TEST_SINGLE ("{'ok': 0, 'errmsg': 'node is recovering'}",
                false /* should_scan */,
                true /* should_mark_unknown */,
                "node is recovering");
   /* Test that "not primary or secondary" is considered a "node is recovering"
    * error, not a "not primary" error. */
   TEST_SINGLE ("{'ok': 0, 'errmsg': 'not master or secondary'}",
                false /* should_scan */,
                true /* should_mark_unknown */,
                "not master or secondary");
   TEST_POOLED ("{'ok': 0, 'errmsg': 'node is recovering'}",
                true /* should_scan */,
                true /* should_mark_unknown */,
                "node is recovering");
   /* Test that "not primary or secondary" is considered a "node is recovering"
    * error, not a "not primary" error. */
   TEST_POOLED ("{'ok': 0, 'errmsg': 'not master or secondary'}",
                true /* should_scan */,
                true /* should_mark_unknown */,
                "not master or secondary");
   TEST_BOTH (
      "{'ok': 0, 'errmsg': 'random error'}", false /* should_scan */, false /* should_mark_unknown */, "random error");
   /* check the error code for NotPrimary, which should be considered a "not
    * primary" error. */
   TEST_BOTH (
      "{'ok': 0, 'code': 10107 }", true /* should_scan */, true /* should_mark_unknown */, NULL /* server_err */);
   /* for an unknown code, the message should not be checked. */
   TEST_BOTH ("{'ok': 0, 'code': 12345, 'errmsg': 'not master'}",
              false /* should_scan */,
              false /* should_mark_unknown */,
              "not master");
   /* check the error code for InterruptedAtShutdown, which behaves
    * much like a "node is recovering" error. */
   TEST_SINGLE (
      "{'ok': 0, 'code': 11600 }", false /* should_scan */, true /* should_mark_unknown */, NULL /* server_err */);
   TEST_POOLED (
      "{'ok': 0, 'code': 11600 }", true /* should_scan */, true /* should_mark_unknown */, NULL /* server_err */);
   /* write concern errors are also checked. */
   _test_request_scan_on_error (1, "{'ok': 1, 'writeConcernError': { 'errmsg': 'not master' }}", 1, 1, "not master");
   _test_request_scan_on_error (0, "{'ok': 1, 'writeConcernError': { 'errmsg': 'not master' }}", 1, 1, "not master");
   TEST_BOTH ("{'ok': 1, 'writeConcernError': { 'code': 10107 }}",
              true, /* should_scan */
              true /* should_mark_unknown */,
              NULL /* server_err */);

#undef TEST_BOTH
#undef TEST_POOLED
#undef TEST_SINGLE
}

/* Test that the issue described in CDRIVER-3625 is fixed.
 * A slow-to-respond server should not block the scan of other servers
 * in background monitoring.
 */
static void
test_slow_server_pooled (void)
{
   mock_server_t *primary;
   mock_server_t *secondary;
   char *hello_common;
   char *hello_primary;
   char *hello_secondary;
   mongoc_read_prefs_t *prefs_secondary;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_apm_callbacks_t *callbacks;
   request_t *request;
   checks_t checks;
   bool ret;
   bson_error_t error;
   mc_tpld_modification tdmod;

   checks_init (&checks);
   primary = mock_server_new ();
   secondary = mock_server_new ();

   mock_server_run (primary);
   mock_server_run (secondary);

   mock_server_autoresponds (primary, auto_ping, NULL, NULL);
   mock_server_autoresponds (secondary, auto_ping, NULL, NULL);

   hello_common = bson_strdup_printf ("{'ok': 1,"
                                      " 'setName': 'rs',"
                                      " 'hosts': ['%s', '%s'],"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d",
                                      mock_server_get_host_and_port (primary),
                                      mock_server_get_host_and_port (secondary),
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX);
   hello_primary = bson_strdup_printf ("%s, 'isWritablePrimary': true, 'secondary': false }", hello_common);
   hello_secondary = bson_strdup_printf ("%s, 'isWritablePrimary': false, 'secondary': true }", hello_common);

   /* Primary responds immediately, but secondary does not. */
   mock_server_auto_hello (primary, hello_primary);

   uri = mongoc_uri_copy (mock_server_get_uri (primary));
   /* Do not connect as topology type Single, so the client pool discovers the
    * secondary. */
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_DIRECTCONNECTION, false);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 500);

   pool = mongoc_client_pool_new (uri);
   callbacks = heartbeat_callbacks ();
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, &checks);

   /* Set a shorter heartbeat frequencies for faster responses. */
   tdmod = mc_tpld_modify_begin (_mongoc_client_pool_get_topology (pool));
   tdmod.new_td->heartbeat_msec = 10;
   mc_tpld_modify_commit (tdmod);
   _mongoc_client_pool_get_topology (pool)->min_heartbeat_frequency_msec = 10;

   client = mongoc_client_pool_pop (pool);
   /* As soon as a client is popped, background scanning starts.
    * Wait for two scans of the primary. */
   WAIT_UNTIL (checks_cmp (&checks, "n_started", '>', 1));

   request = mock_server_receives_legacy_hello (secondary, NULL);

   /* A command to the primary succeeds. */
   ret = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (ret, error);

   /* A command to the secondary fails. */
   prefs_secondary = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   ret = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), prefs_secondary, NULL, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "expired");
   BSON_ASSERT (!ret);

   /* Set up an auto responder so future hellos on the secondary do not
    * block until connectTimeoutMS. Otherwise, the shutdown sequence will be
    * blocked for connectTimeoutMS. */
   mock_server_auto_hello (secondary, hello_secondary);
   /* Respond to the first hello. */
   reply_to_request_simple (request, hello_secondary);
   request_destroy (request);

   /* Now a command to the secondary succeeds. */
   ret = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), prefs_secondary, NULL, &error);
   ASSERT_OR_PRINT (ret, error);

   mongoc_read_prefs_destroy (prefs_secondary);
   mongoc_client_pool_push (pool, client);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   bson_free (hello_secondary);
   bson_free (hello_primary);
   bson_free (hello_common);
   mock_server_destroy (secondary);
   mock_server_destroy (primary);
   checks_cleanup (&checks);
}

static void
_test_hello_versioned_api (bool pooled)
{
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client = NULL;
   future_t *future = NULL;
   request_t *request = NULL;
   bson_error_t error;
   mongoc_server_api_version_t version;

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);
   mongoc_uri_t *const uri = mongoc_uri_copy (mock_server_get_uri (server));

   BSON_ASSERT (mongoc_server_api_version_from_string ("1", &version));
   mongoc_server_api_t *const api = mongoc_server_api_new (version);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, api);

      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, api);
   }

   char *const hello_reply = bson_strdup_printf ("{'ok': 1,"
                                                 " 'isWritablePrimary': true,"
                                                 " 'setName': 'rs',"
                                                 " 'minWireVersion': %d,"
                                                 " 'maxWireVersion': %d,"
                                                 " 'hosts': ['%s']}",
                                                 WIRE_VERSION_MIN,
                                                 WIRE_VERSION_MAX,
                                                 mock_server_get_host_and_port (server));

   /* For client pools, the first handshake happens when the client is popped.
    * For non-pooled clients, we send a ping command to trigger a handshake. */
   if (!pooled) {
      future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   }

   request = mock_server_receives_hello_op_msg (server);

   BSON_ASSERT (request);
   BSON_ASSERT (bson_has_field (request_get_doc (request, 0), "apiVersion"));
   BSON_ASSERT (bson_has_field (request_get_doc (request, 0), "helloOk"));

   reply_to_request_simple (request, hello_reply);

   request_destroy (request);

   if (!pooled) {
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));

      reply_to_request_with_ok_and_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);
   }

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_server_api_destroy (api);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
   bson_free (hello_reply);
}

static void
test_hello_versioned_api_single (void)
{
   _test_hello_versioned_api (false);
}

static void
test_hello_versioned_api_pooled (void)
{
   _test_hello_versioned_api (true);
}

static void
_test_hello_ok (bool pooled)
{
   mock_server_t *server = NULL;
   mongoc_uri_t *uri = NULL;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client = NULL;
   char *hello = NULL;
   char *hello_not_ok = NULL;
   future_t *future = NULL;
   request_t *request = NULL;
   bson_error_t error;

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
   }

   hello = bson_strdup_printf ("{'ok': 1,"
                               " 'isWritablePrimary': true,"
                               " 'helloOk': true,"
                               " 'setName': 'rs',"
                               " 'minWireVersion': %d,"
                               " 'maxWireVersion': %d,"
                               " 'hosts': ['%s']}",
                               WIRE_VERSION_MIN,
                               WIRE_VERSION_MAX,
                               mock_server_get_host_and_port (server));

   hello_not_ok = bson_strdup_printf ("{'ok': 1,"
                                      " 'isWritablePrimary': true,"
                                      " 'setName': 'rs',"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d,"
                                      " 'hosts': ['%s']}",
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX,
                                      mock_server_get_host_and_port (server));

   /* For client pools, the first handshake happens when the client is popped.
    * For non-pooled clients, send a ping command to trigger a handshake. */
   if (!pooled) {
      future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   }

   request = mock_server_receives_any_hello_with_match (
      server, NULL, "{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1, 'helloOk': true}");

   BSON_ASSERT (request);
   reply_to_request_simple (request, hello);
   request_destroy (request);

   /* For non-pooled clients, handle the ping */
   if (!pooled) {
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
      reply_to_request_with_ok_and_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);

      /* Send off another ping for non-pooled clients, making sure to wait long
       * enough to require another heartbeat. */
      _mongoc_usleep (600 * 1000);
      future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   }

   /* Hang up to ensure that the next check runs legacy hello again */
   request = mock_server_receives_any_hello_with_match (server, "{}", "{}");
   BSON_ASSERT (request);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* The previous failure will trigger another handshake using legacy hello */
   request = mock_server_receives_any_hello_with_match (server,
                                                        "{'" HANDSHAKE_CMD_HELLO "': 1, 'helloOk': true}",
                                                        "{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1, 'helloOk': true}");

   BSON_ASSERT (request);
   reply_to_request_simple (request, hello_not_ok);
   request_destroy (request);

   /* Once again, handle the ping */
   if (!pooled) {
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
      reply_to_request_with_ok_and_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);

      /* Send off another ping for non-pooled clients, making sure to wait long
       * enough to require another heartbeat. */
      _mongoc_usleep (600 * 1000);
      future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   }

   /* Since we never responded with helloOk: true, we're expecting another
    * hello. */
   request = mock_server_receives_any_hello_with_match (server,
                                                        "{'" HANDSHAKE_CMD_HELLO "': 1, 'helloOk': true}",
                                                        "{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1, 'helloOk': true}");

   BSON_ASSERT (request);
   reply_to_request_simple (request, hello_not_ok);
   request_destroy (request);

   /* Once again, handle the ping */
   if (!pooled) {
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
      reply_to_request_with_ok_and_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);
   }

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
   bson_free (hello);
   bson_free (hello_not_ok);
}

static void
test_hello_ok_single (void)
{
   _test_hello_ok (false);
}

static void
test_hello_ok_pooled (void)
{
   _test_hello_ok (true);
}

// initiator_fail is a stream initiator that always fails.
static mongoc_stream_t *
initiator_fail (const mongoc_uri_t *uri, const mongoc_host_list_t *host, void *user_data, bson_error_t *error)
{
   BSON_UNUSED (uri);
   BSON_UNUSED (host);
   BSON_UNUSED (user_data);

   bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "failing in initiator");
   printf ("failing in initiator\n");

   return false;
}

// Test failure in `mongoc_topology_scanner_node_setup` during retry of scanning
// a known server. This is a regression test of CDRIVER-4666.
static void
test_failure_to_setup_after_retry (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   future_t *future;
   bson_error_t error;

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   client = mongoc_client_new_from_uri_with_error (uri, &error);
   ASSERT_OR_PRINT (client, error);

   // Override the heartbeatFrequencyMS (default 60 seconds) and
   // minHeartbeatFrequencyMS (default 500ms) to speed up the test.
   const int64_t overridden_heartbeat_ms = 1;
   {
      mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
      tdmod.new_td->heartbeat_msec = overridden_heartbeat_ms;
      mc_tpld_modify_commit (tdmod);
      client->topology->min_heartbeat_frequency_msec = overridden_heartbeat_ms;
   }

   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   // The first command starts the first topology scan.
   // Expect legacy hello with handshake.
   {
      request_t *request = mock_server_receives_legacy_hello (server, "{}");
      char *reply = bson_strdup_printf ("{'ok': 1,"
                                        " 'minWireVersion': %d,"
                                        " 'maxWireVersion': %d }",
                                        WIRE_VERSION_MIN,
                                        WIRE_VERSION_MAX);

      reply_to_request_simple (request, reply);
      bson_free (reply);
      request_destroy (request);
   }

   // Expect "ping" command.
   {
      request_t *request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'ping': 1}"));
      reply_to_request_with_ok_and_destroy (request);
      ASSERT_OR_PRINT (future_get_bool (future), error);
      future_destroy (future);
   }

   // Wait until ready for next topology scan.
   _mongoc_usleep (overridden_heartbeat_ms * 1000);

   // Send another command.
   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   // Expect legacy hello with handshake.
   {
      request_t *request = mock_server_receives_legacy_hello (server, "{}");
      // Set the initiator to fail.
      mongoc_client_set_stream_initiator (client, initiator_fail, NULL);
      // A network error on a previously known server triggers the retry.
      reply_to_request_with_hang_up (request);
      request_destroy (request);
   }

   // The initiator fails in `mongoc_topology_scanner_node_setup`. Causes a
   // deadlock similar to that observed in CDRIVER-4666.
   // Test fails on macOS due to deadlock with "future_get_bool timed out".

   ASSERT (!future_get_bool (future));
   future_destroy (future);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "No suitable servers found");

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
test_detect_nongenuine_hosts (void)
{
   const char *cosmos_uris[] = {
      "mongodb://a.mongo.cosmos.azure.com:19555/",
      /* Test case-insensitive matching */
      "mongodb://a.MONGO.COSMOS.AZURE.COM:19555/",
      /* Mixing genuine and nongenuine hosts (unlikely in practice) */
      "mongodb://a.example.com:27017,b.mongo.cosmos.azure.com:19555/",
      /* Test SRV matching */
      "mongodb+srv://a.mongo.cosmos.azure.com/",
      /* Test SRV case-insensitive matching */
      "mongodb+srv://A.MONGO.COSMOS.AZURE.COM/",
   };

   const char *docdb_uris[] = {
      "mongodb://a.docdb.amazonaws.com:27017/",
      "mongodb://a.docdb-elastic.amazonaws.com:27017/",
      "mongodb://a.DOCDB.AMAZONAWS.COM:27017/",
      "mongodb://a.DOCDB-ELASTIC.AMAZONAWS.COM:27017/",
      "mongodb://a.example.com:27017,b.docdb.amazonaws.com:27017/",
      "mongodb://a.example.com:27017,b.docdb-elastic.amazonaws.com:27017/",
      "mongodb+srv://a.DOCDB.AMAZONAWS.COM/",
      "mongodb+srv://a.DOCDB-ELASTIC.AMAZONAWS.COM/",
   };

   const char *genuine_uris[] = {
      "mongodb://a.example.com:27017,b.example.com:27017/",
      "mongodb://a.mongodb.net:27017",
      /* Host names do not end with expected suffix */
      "mongodb://a.mongo.cosmos.azure.com.tld:19555/",
      "mongodb://a.docdb.amazonaws.com.tld:27017/",
      "mongodb://a.docdb-elastic.amazonaws.com.tld:27017/",
      /* Test genuine SRV URIs */
      "mongodb+srv://a.example.com/",
      "mongodb+srv://a.mongodb.net/",
      /* SRV host names do not end with expected suffix */
      "mongodb+srv://a.mongo.cosmos.azure.com.tld/",
      "mongodb+srv://a.docdb.amazonaws.com.tld/",
      "mongodb+srv://a.docdb-elastic.amazonaws.com.tld/",
   };

   for (size_t i = 0u; i < sizeof (cosmos_uris) / sizeof (*cosmos_uris); ++i) {
      capture_logs (true);
      mongoc_uri_t *const uri = mongoc_uri_new (cosmos_uris[i]);
      ASSERT (uri);
      mongoc_topology_t *const topology = mongoc_topology_new (uri, true);
      ASSERT (topology);
      ASSERT_CAPTURED_LOG (cosmos_uris[i],
                           MONGOC_LOG_LEVEL_INFO,
                           "You appear to be connected to a CosmosDB cluster. For more "
                           "information regarding feature compatibility and support please visit "
                           "https://www.mongodb.com/supportability/cosmosdb");
      mongoc_topology_destroy (topology);
      mongoc_uri_destroy (uri);
   }

   for (size_t i = 0u; i < sizeof (docdb_uris) / sizeof (*docdb_uris); ++i) {
      capture_logs (true);
      mongoc_uri_t *const uri = mongoc_uri_new (docdb_uris[i]);
      ASSERT (uri);
      mongoc_topology_t *const topology = mongoc_topology_new (uri, true);
      ASSERT (topology);
      ASSERT_CAPTURED_LOG (docdb_uris[i],
                           MONGOC_LOG_LEVEL_INFO,
                           "You appear to be connected to a DocumentDB cluster. For more "
                           "information regarding feature compatibility and support please visit "
                           "https://www.mongodb.com/supportability/documentdb");
      mongoc_topology_destroy (topology);
      mongoc_uri_destroy (uri);
   }

   for (size_t i = 0u; i < sizeof (genuine_uris) / sizeof (*genuine_uris); ++i) {
      capture_logs (true);
      mongoc_uri_t *const uri = mongoc_uri_new (genuine_uris[i]);
      ASSERT (uri);
      mongoc_topology_t *const topology = mongoc_topology_new (uri, true);
      ASSERT (topology);
      ASSERT_NO_CAPTURED_LOGS (genuine_uris[i]);
      mongoc_topology_destroy (topology);
      mongoc_uri_destroy (uri);
   }
}

void
test_topology_install (TestSuite *suite)
{
   TestSuite_AddLive (suite, "/Topology/client_creation", test_topology_client_creation);
   TestSuite_AddLive (suite, "/Topology/client_pool_creation", test_topology_client_pool_creation);
   TestSuite_AddLive (suite, "/Topology/start_stop", test_topology_thread_start_stop);
   TestSuite_AddFull (suite,
                      "/Topology/server_selection_try_once_option",
                      test_server_selection_try_once_option,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Topology/server_selection_try_once",
                      test_server_selection_try_once,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Topology/server_selection_try_once_false",
                      test_server_selection_try_once_false,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Topology/invalidate_server/single",
                      test_topology_invalidate_server_single,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow_or_live);
   TestSuite_AddFull (suite,
                      "/Topology/invalidate_server/pooled",
                      test_topology_invalidate_server_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow_or_live);
   TestSuite_AddFull (suite,
                      "/Topology/invalid_cluster_node",
                      test_invalid_cluster_node,
                      NULL,
                      NULL,
                      test_framework_skip_if_slow_or_live);
   TestSuite_AddFull (suite,
                      "/Topology/max_wire_version_race_condition",
                      test_max_wire_version_race_condition,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth);
   TestSuite_AddMockServerTest (
      suite, "/Topology/cooldown/standalone", test_cooldown_standalone, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/cooldown/rs", test_cooldown_rs, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/cooldown/retry", test_cooldown_retry, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_Add (suite, "/Topology/multiple_selection_errors", test_multiple_selection_errors);
   TestSuite_AddMockServerTest (suite, "/Topology/connect_timeout/succeed", test_select_after_timeout);
   TestSuite_AddMockServerTest (suite, "/Topology/try_once/succeed", test_select_after_try_once);
   TestSuite_AddLive (suite, "/Topology/invalid_server_id", test_invalid_server_id);
   TestSuite_AddMockServerTest (suite, "/Topology/server_removed/single", test_server_removed_during_handshake_single);
   TestSuite_AddMockServerTest (suite, "/Topology/server_removed/pooled", test_server_removed_during_handshake_pooled);
   TestSuite_AddFull (suite, "/Topology/rtt", test_rtt, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite, "/Topology/add_and_scan_failure", test_add_and_scan_failure);
   TestSuite_AddMockServerTest (
      suite, "/Topology/hello_retry/single/hangup", test_hello_retry_single_hangup, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/hello_retry/single/timeout", test_hello_retry_single_timeout, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Topology/hello_retry/single/hangup/fail",
                                test_hello_retry_single_hangup_fail,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Topology/hello_retry/single/timeout/fail",
                                test_hello_retry_single_timeout_fail,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/hello_retry/pooled/hangup", test_hello_retry_pooled_hangup, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/hello_retry/pooled/timeout", test_hello_retry_pooled_timeout, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Topology/hello_retry/pooled/hangup/fail",
                                test_hello_retry_pooled_hangup_fail,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Topology/hello_retry/pooled/timeout/fail",
                                test_hello_retry_pooled_timeout_fail,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/incompatible_error", test_incompatible_error, test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (suite,
                                "/Topology/compatible_null_error_pointer",
                                test_compatible_null_error_pointer,
                                test_framework_skip_if_slow);
   TestSuite_AddMockServerTest (
      suite, "/Topology/handshake/updates_clustertime", test_cluster_time_updated_during_handshake);
   TestSuite_AddMockServerTest (suite, "/Topology/request_scan_on_error", test_request_scan_on_error);
   TestSuite_AddMockServerTest (suite, "/Topology/last_server_removed_warning", test_last_server_removed_warning);
   TestSuite_AddMockServerTest (suite, "/Topology/slow_server/pooled", test_slow_server_pooled);

   TestSuite_AddMockServerTest (suite, "/Topology/hello/versioned_api/single", test_hello_versioned_api_single);
   TestSuite_AddMockServerTest (suite, "/Topology/hello/versioned_api/pooled", test_hello_versioned_api_pooled);

   TestSuite_AddMockServerTest (suite, "/Topology/hello_ok/single", test_hello_ok_single);
   TestSuite_AddMockServerTest (suite, "/Topology/hello_ok/pooled", test_hello_ok_pooled);
   TestSuite_AddMockServerTest (suite, "/Topology/failure_to_setup_after_retry", test_failure_to_setup_after_retry);
   TestSuite_Add (suite, "/Topology/detect_nongenuine_hosts", test_detect_nongenuine_hosts);
}
