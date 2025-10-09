/*
 * Copyright 2021-present MongoDB, Inc.
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

#include "mongoc/mongoc.h"
#include "mongoc/mongoc-client-session-private.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"

#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "mock_server/request.h"

typedef struct {
   int server_changed_events;
   int server_opening_events;
   int server_closed_events;
   int topology_changed_events;
   int topology_opening_events;
   int topology_closed_events;
} stats_t;

static void
server_changed (const mongoc_apm_server_changed_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_server_changed_get_context (event);
   context->server_changed_events++;
}


static void
server_opening (const mongoc_apm_server_opening_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_server_opening_get_context (event);
   context->server_opening_events++;
}


static void
server_closed (const mongoc_apm_server_closed_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_server_closed_get_context (event);
   context->server_closed_events++;
}


static void
topology_changed (const mongoc_apm_topology_changed_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_topology_changed_get_context (event);
   context->topology_changed_events++;
}


static void
topology_opening (const mongoc_apm_topology_opening_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_topology_opening_get_context (event);
   context->topology_opening_events++;
}


static void
topology_closed (const mongoc_apm_topology_closed_t *event)
{
   stats_t *context;

   context = (stats_t *) mongoc_apm_topology_closed_get_context (event);
   context->topology_closed_events++;
}

static mongoc_apm_callbacks_t *
make_callbacks (void)
{
   mongoc_apm_callbacks_t *cbs;

   cbs = mongoc_apm_callbacks_new ();
   mongoc_apm_set_server_changed_cb (cbs, server_changed);
   mongoc_apm_set_server_opening_cb (cbs, server_opening);
   mongoc_apm_set_server_closed_cb (cbs, server_closed);
   mongoc_apm_set_topology_changed_cb (cbs, topology_changed);
   mongoc_apm_set_topology_opening_cb (cbs, topology_opening);
   mongoc_apm_set_topology_closed_cb (cbs, topology_closed);
   return cbs;
}

static stats_t *
set_client_callbacks (mongoc_client_t *client)
{
   mongoc_apm_callbacks_t *cbs;
   stats_t *stats;

   ASSERT (client);

   stats = bson_malloc0 (sizeof (stats_t));
   cbs = make_callbacks ();
   mongoc_client_set_apm_callbacks (client, cbs, stats);
   mongoc_apm_callbacks_destroy (cbs);
   return stats;
}

static stats_t *
set_client_pool_callbacks (mongoc_client_pool_t *pool)
{
   mongoc_apm_callbacks_t *cbs;
   stats_t *stats;

   ASSERT (pool);

   stats = bson_malloc0 (sizeof (stats_t));
   cbs = make_callbacks ();
   mongoc_client_pool_set_apm_callbacks (pool, cbs, stats);
   mongoc_apm_callbacks_destroy (cbs);
   return stats;
}

static void
free_and_assert_stats (stats_t *stats)
{
   ASSERT_CMPINT (stats->topology_opening_events, ==, 1);
   ASSERT_CMPINT (stats->topology_changed_events, ==, 2);
   ASSERT_CMPINT (stats->server_opening_events, ==, 1);
   ASSERT_CMPINT (stats->server_changed_events, ==, 1);
   ASSERT_CMPINT (stats->server_closed_events, ==, 1);
   ASSERT_CMPINT (stats->topology_closed_events, ==, 1);
   bson_free (stats);
}

static void
test_loadbalanced_sessions_supported (void *unused)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   bson_error_t error;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   session = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session, error);

   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}

static void
test_loadbalanced_sessions_do_not_expire (void *unused)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session1;
   mongoc_client_session_t *session2;
   bson_error_t error;
   bson_t *session1_lsid;
   bson_t *session2_lsid;
   mc_tpld_modification tdmod;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   /* Mock a timeout so session expiration applies. */
   tdmod = mc_tpld_modify_begin (client->topology);
   tdmod.new_td->session_timeout_minutes = 1;
   mc_tpld_modify_commit (tdmod);

   /* Start two sessions, to ensure that pooled sessions remain in the pool when
    * the pool is accessed. */
   session1 = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session1, error);
   session2 = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session2, error);

   session1_lsid = bson_copy (mongoc_client_session_get_lsid (session1));
   session2_lsid = bson_copy (mongoc_client_session_get_lsid (session2));

   /* Expire both sessions. */
   session1->server_session->last_used_usec = 1;
   session2->server_session->last_used_usec = 1;
   mongoc_client_session_destroy (session1);
   mongoc_client_session_destroy (session2);

   /* Get a new session, it should reuse the most recently pushed session2. */
   session2 = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session2, error);
   if (!bson_equal (mongoc_client_session_get_lsid (session2), session2_lsid)) {
      test_error ("Session not reused: %s != %s",
                  tmp_json (mongoc_client_session_get_lsid (session2)),
                  tmp_json (session2_lsid));
   }

   session1 = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session1, error);
   if (!bson_equal (mongoc_client_session_get_lsid (session1), session1_lsid)) {
      test_error ("Session not reused: %s != %s",
                  tmp_json (mongoc_client_session_get_lsid (session1)),
                  tmp_json (session1_lsid));
   }

   bson_destroy (session1_lsid);
   bson_destroy (session2_lsid);
   mongoc_client_session_destroy (session1);
   mongoc_client_session_destroy (session2);
   mongoc_client_destroy (client);
}

/* Test that invalid loadBalanced URI configurations are validated during client
 * construction. */
static void
test_loadbalanced_client_uri_validation (void *unused)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   bson_error_t error;

   BSON_UNUSED (unused);

   uri = mongoc_uri_new ("mongodb://localhost:27017");
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_DIRECTCONNECTION, true);
   client = mongoc_client_new_from_uri_with_error (uri, &error);

   BSON_ASSERT (!client);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "URI with \"loadBalanced\" enabled must not contain "
                          "option \"directConnection\" enabled");

   mongoc_uri_destroy (uri);
}

/* Test basic connectivity to a load balanced cluster. */
static void
test_loadbalanced_connect_single (void *unused)
{
   mongoc_client_t *client;
   bson_error_t error;
   bool ok;
   mongoc_server_description_t *monitor_sd;
   stats_t *stats;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   stats = set_client_callbacks (client);
   ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ok, error);

   /* Ensure the server description is unchanged and remains as type
    * LoadBalancer. */
   monitor_sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   ASSERT_CMPSTR ("LoadBalancer", mongoc_server_description_type (monitor_sd));

   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_destroy (client);
   free_and_assert_stats (stats);
}

static void
test_loadbalanced_connect_pooled (void *unused)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   bson_error_t error;
   bool ok;
   mongoc_server_description_t *monitor_sd;
   stats_t *stats;

   BSON_UNUSED (unused);

   pool = test_framework_new_default_client_pool ();
   stats = set_client_pool_callbacks (pool);
   client = mongoc_client_pool_pop (pool);

   ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ok, error);

   /* Ensure the server description is unchanged and remains as type
    * LoadBalancer. */
   monitor_sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   ASSERT_CMPSTR ("LoadBalancer", mongoc_server_description_type (monitor_sd));

   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   free_and_assert_stats (stats);
}

/* Ensure that server selection on single threaded clients establishes a
 * connection against load balanced clusters. */
static void
test_loadbalanced_server_selection_establishes_connection_single (void *unused)
{
   mongoc_client_t *client;
   bson_error_t error;
   mongoc_server_description_t *monitor_sd;
   mongoc_server_description_t *handshake_sd;
   stats_t *stats;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   stats = set_client_callbacks (client);
   monitor_sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   ASSERT_CMPSTR ("LoadBalancer", mongoc_server_description_type (monitor_sd));

   /* Ensure that a connection has been established by getting the handshake's
    * server description. */
   handshake_sd = mongoc_client_get_handshake_description (client, monitor_sd->id, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);
   ASSERT_CMPSTR ("Mongos", mongoc_server_description_type (handshake_sd));

   mongoc_server_description_destroy (monitor_sd);
   mongoc_server_description_destroy (handshake_sd);
   mongoc_client_destroy (client);
   free_and_assert_stats (stats);
}

/* Test that the 5 second cooldown does not apply when establishing a new
 * connection to the load balancer after a network error. */
static void
test_loadbalanced_cooldown_is_bypassed_single (void *unused)
{
   mongoc_client_t *client;
   bson_error_t error;
   bool ok;
   stats_t *stats;
   mongoc_server_description_t *monitor_sd;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   stats = set_client_callbacks (client);

   ok = mongoc_client_command_simple (client,
                                      "admin",
                                      tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': { 'times': 2 }, "
                                                "'data': {'closeConnection': true, 'failCommands': ['ping', "
                                                "'hello']}}"),
                                      NULL /* read prefs */,
                                      NULL /* reply */,
                                      &error);
   ASSERT_OR_PRINT (ok, error);

   ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   BSON_ASSERT (!ok);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error");

   /* The next attempted command should attempt to scan, and fail when
    * performing the handshake with the hello command. */
   ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   BSON_ASSERT (!ok);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NOT_ESTABLISHED, "Could not establish stream");

   /* Failing to "scan" would normally cause the node to be in cooldown and fail
    * to reconnect (until the 5 second period has passed). But in load balancer
    * mode cooldown is bypassed, so the subsequent connect attempt should
    * succeed. */
   ok = mongoc_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ok, error);

   /* Ensure the server description is unchanged and remains as type
    * LoadBalancer. */
   monitor_sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   ASSERT_CMPSTR ("LoadBalancer", mongoc_server_description_type (monitor_sd));

   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_destroy (client);
   free_and_assert_stats (stats);
}

/* Tests:
 * - loadBalanced: true is added to the handshake
 * - serviceId is set in the server description.
 */
#define LB_HELLO                                                   \
   tmp_str ("{'ismaster': true,"                                   \
            " 'minWireVersion': %d,"                               \
            " 'maxWireVersion': %d,"                               \
            " 'msg': 'isdbgrid',"                                  \
            " 'serviceId': {'$oid': 'AAAAAAAAAAAAAAAAAAAAAAAA'}}", \
            WIRE_VERSION_MIN,                                      \
            WIRE_VERSION_5_0)
static void
test_loadbalanced_handshake_sends_loadbalanced (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   request_t *request;
   future_t *future;
   bson_error_t error;
   mongoc_server_description_t *monitor_sd;
   mongoc_server_description_t *handshake_sd;
   bson_oid_t expected;
   const bson_oid_t *actual;

   server = mock_server_new ();
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   client = test_framework_client_new_from_uri (uri, NULL);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   request = mock_server_receives_any_hello_with_match (server, "{'loadBalanced': true}", "{'loadBalanced': true}");
   reply_to_request_simple (request, LB_HELLO);
   request_destroy (request);

   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);

   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   monitor_sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (monitor_sd, error);
   handshake_sd = mongoc_client_get_handshake_description (client, 1, NULL /* opts */, &error);
   ASSERT_OR_PRINT (handshake_sd, error);

   bson_oid_init_from_string (&expected, "AAAAAAAAAAAAAAAAAAAAAAAA");
   actual = &handshake_sd->service_id;
   BSON_ASSERT (actual);
   ASSERT_CMPOID (actual, &expected);

   mongoc_server_description_destroy (handshake_sd);
   mongoc_server_description_destroy (monitor_sd);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Tests that a connection is rejected if the handshake reply does not include a
 * serviceID field. */
#define NON_LB_HELLO                 \
   tmp_str ("{'ismaster': true,"     \
            " 'minWireVersion': %d," \
            " 'maxWireVersion': %d," \
            " 'msg': 'isdbgrid'}",   \
            WIRE_VERSION_MIN,        \
            WIRE_VERSION_5_0)
static void
test_loadbalanced_handshake_rejects_non_loadbalanced (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   request_t *request;
   future_t *future;
   bson_error_t error;

   server = mock_server_new ();
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   client = test_framework_client_new_from_uri (uri, NULL);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   request = mock_server_receives_any_hello_with_match (server, "{'loadBalanced': true}", "{'loadBalanced': true}");
   reply_to_request_simple (request, NON_LB_HELLO);
   request_destroy (request);
   BSON_ASSERT (!future_get_bool (future));
   future_destroy (future);

   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_LOAD_BALANCER,
                          "Driver attempted to initialize in load balancing "
                          "mode, but the server does not support this mode");

   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Test that an error before the MongoDB handshake completes does NOT go through
 * SDAM error handling flow. */
static void
test_pre_handshake_error_does_not_clear_pool (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client_1;
   mongoc_client_t *client_2;
   future_t *future;
   request_t *request;
   bson_error_t error;
   const bson_t *match_loadBalanced = tmp_bson ("{'loadBalanced': true}");

   server = mock_server_new ();
   mock_server_auto_endsessions (server);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   pool = mongoc_client_pool_new (uri);
   client_1 = mongoc_client_pool_pop (pool);
   client_2 = mongoc_client_pool_pop (pool);

   /* client_1 opens a new connection to send "ping" */
   future = future_client_command_simple (
      client_1, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));

   BSON_ASSERT (request);
   reply_to_request_simple (request, LB_HELLO);
   request_destroy (request);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* client_2 attempts to open a new connection, but receives an error on the
    * handshake. */
   future = future_client_command_simple (
      client_2, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));
   BSON_ASSERT (request);
   capture_logs (true); /* hide Failed to buffer logs. */
   reply_to_request_with_hang_up (request);
   request_destroy (request);
   BSON_ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to send");
   future_destroy (future);

   /* client_1 sends another "ping". */
   future = future_client_command_simple (
      client_1, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);

   /* The connection pool must not have been cleared. It can reuse the previous
    * connection. The next command is the "ping". */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   mongoc_client_pool_push (pool, client_2);
   mongoc_client_pool_push (pool, client_1);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

#define LB_HELLO_A                                                 \
   tmp_str ("{'ismaster': true,"                                   \
            " 'minWireVersion': %d,"                               \
            " 'maxWireVersion': %d,"                               \
            " 'msg': 'isdbgrid',"                                  \
            " 'serviceId': {'$oid': 'AAAAAAAAAAAAAAAAAAAAAAAA'}}", \
            WIRE_VERSION_MIN,                                      \
            WIRE_VERSION_5_0)

#define LB_HELLO_B                                                 \
   tmp_str ("{'ismaster': true,"                                   \
            " 'minWireVersion': %d,"                               \
            " 'maxWireVersion': %d,"                               \
            " 'msg': 'isdbgrid',"                                  \
            " 'serviceId': {'$oid': 'BBBBBBBBBBBBBBBBBBBBBBBB'}}", \
            WIRE_VERSION_MIN,                                      \
            WIRE_VERSION_5_0)

/* Test that a post handshake error clears the pool ONLY for connections with
 * the same serviceID. Test that a post handshake error does not mark the server
 * unknown. */
static void
test_post_handshake_error_clears_pool (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client_1_serviceid_a;
   mongoc_client_t *client_2_serviceid_a;
   mongoc_client_t *client_3_serviceid_b;
   future_t *future;
   request_t *request;
   bson_error_t error;
   mongoc_server_description_t *monitor_sd;
   const bson_t *match_loadBalanced = tmp_bson ("{'loadBalanced': true}");

   server = mock_server_new ();
   mock_server_auto_endsessions (server);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
   pool = mongoc_client_pool_new (uri);
   client_1_serviceid_a = mongoc_client_pool_pop (pool);
   client_2_serviceid_a = mongoc_client_pool_pop (pool);
   client_3_serviceid_b = mongoc_client_pool_pop (pool);

   /* client_1_serviceid_a opens a new connection to send "ping" */
   future = future_client_command_simple (
      client_1_serviceid_a, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));
   BSON_ASSERT (request);
   reply_to_request_simple (request, LB_HELLO_A);
   request_destroy (request);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* client_2_serviceid_a also opens a new connection and receives the same
    * service ID. */
   future = future_client_command_simple (
      client_2_serviceid_a, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));
   BSON_ASSERT (request);
   reply_to_request_simple (request, LB_HELLO_A);
   request_destroy (request);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* client_3_serviceid_b also opens a new connection, but receives a different
    * service ID. */
   future = future_client_command_simple (
      client_3_serviceid_b, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));
   BSON_ASSERT (request);
   reply_to_request_simple (request, LB_HELLO_B);
   request_destroy (request);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* client_1_serviceid_a receives a network error. */
   future = future_client_command_simple (
      client_1_serviceid_a, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   capture_logs (true); /* hide Failed to buffer logs. */
   reply_to_request_with_hang_up (request);
   request_destroy (request);
   BSON_ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to send");
   future_destroy (future);

   /* Assert that the server is NOT marked Unknown. */
   monitor_sd = mongoc_client_select_server (client_1_serviceid_a, true, NULL /* read prefs */, &error);
   ASSERT_CMPSTR ("LoadBalancer", mongoc_server_description_type (monitor_sd));

   /* This should have invalidated the connection for client_2_serviceid_a. */
   future = future_client_command_simple (
      client_2_serviceid_a, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* A new connection is opened. */
   request = mock_server_receives_hello_op_msg (server);
   ASSERT (request_matches_msg (request, MONGOC_MSG_NONE, &match_loadBalanced, 1));
   BSON_ASSERT (request);
   reply_to_request_simple (request, LB_HELLO_A);
   request_destroy (request);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* But the connection for client_3_serviceid_b should still be OK. */
   future = future_client_command_simple (
      client_3_serviceid_b, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* The "ping" command is sent. */
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   BSON_ASSERT (request);
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   mongoc_server_description_destroy (monitor_sd);
   mongoc_client_pool_push (pool, client_3_serviceid_b);
   mongoc_client_pool_push (pool, client_2_serviceid_a);
   mongoc_client_pool_push (pool, client_1_serviceid_a);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static int
skip_if_not_loadbalanced (void)
{
   return test_framework_is_loadbalanced () ? 1 : 0;
}

static void
store_last_command_started_callback (const mongoc_apm_command_started_t *event)
{
   bson_t **last_command = mongoc_apm_command_started_get_context (event);
   const bson_t *cmd = mongoc_apm_command_started_get_command (event);
   bson_destroy (*last_command);
   *last_command = bson_copy (cmd);
}

// `test_loadbalanced_sends_recoveryToken` is a regression test for
// CDRIVER-4718. Ensure that a `recoveryToken` is included in the outgoing
// `commitTransaction` and `abortTransaction` commands when connected to a load
// balanced cluster.
static void
test_loadbalanced_sends_recoveryToken (void *unused)
{
   mongoc_client_t *client;
   bson_error_t error;
   bson_t *last_command = NULL;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();
   // Set a callback to store the most recent command started.
   {
      mongoc_apm_callbacks_t *cbs = mongoc_apm_callbacks_new ();
      mongoc_apm_set_command_started_cb (cbs, store_last_command_started_callback);
      mongoc_client_set_apm_callbacks (client, cbs, &last_command);
      mongoc_apm_callbacks_destroy (cbs);
   }

   mongoc_client_session_t *session = mongoc_client_start_session (client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (session, error);

   mongoc_collection_t *coll = mongoc_client_get_collection (client, "db", "coll");

   // Commit a transaction. Expect `commitTransaction` to include
   // `recoveryToken`.
   {
      bool ok = mongoc_client_session_start_transaction (session, NULL /* opts */, &error);
      ASSERT_OR_PRINT (ok, error);

      bson_t *insert_opts = tmp_bson ("{}");
      ok = mongoc_client_session_append (session, insert_opts, &error);
      ASSERT_OR_PRINT (ok, error);

      ok = mongoc_collection_insert_one (coll, tmp_bson ("{}"), insert_opts, NULL, &error);
      ASSERT_OR_PRINT (ok, error);

      ok = mongoc_client_session_commit_transaction (session, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT_MATCH (last_command, "{'commitTransaction': 1, 'recoveryToken': { '$exists': true } }");
   }

   // Abort a transaction. Expect `abortTransaction` to include
   // `recoveryToken`.
   {
      bool ok = mongoc_client_session_start_transaction (session, NULL /* opts */, &error);
      ASSERT_OR_PRINT (ok, error);

      bson_t *insert_opts = tmp_bson ("{}");
      ok = mongoc_client_session_append (session, insert_opts, &error);
      ASSERT_OR_PRINT (ok, error);

      ok = mongoc_collection_insert_one (coll, tmp_bson ("{}"), insert_opts, NULL, &error);
      ASSERT_OR_PRINT (ok, error);

      ok = mongoc_client_session_abort_transaction (session, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT_MATCH (last_command, "{'abortTransaction': 1, 'recoveryToken': { '$exists': true } }");
   }

   mongoc_collection_destroy (coll);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
   bson_destroy (last_command);
}

void
test_loadbalanced_install (TestSuite *suite)
{
   TestSuite_AddFull (suite,
                      "/loadbalanced/sessions/supported",
                      test_loadbalanced_sessions_supported,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);
   TestSuite_AddFull (suite,
                      "/loadbalanced/sessions/do_not_expire",
                      test_loadbalanced_sessions_do_not_expire,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);
   TestSuite_AddFull (suite,
                      "/loadbalanced/client_uri_validation",
                      test_loadbalanced_client_uri_validation,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      NULL);

   TestSuite_AddFull (suite,
                      "/loadbalanced/connect/single",
                      test_loadbalanced_connect_single,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);

   TestSuite_AddFull (suite,
                      "/loadbalanced/connect/pooled",
                      test_loadbalanced_connect_pooled,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);

   TestSuite_AddFull (suite,
                      "/loadbalanced/server_selection_establishes_connection/single",
                      test_loadbalanced_server_selection_establishes_connection_single,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);

   TestSuite_AddFull (suite,
                      "/loadbalanced/cooldown_is_bypassed/single",
                      test_loadbalanced_cooldown_is_bypassed_single,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      skip_if_not_loadbalanced,
                      test_framework_skip_if_no_failpoint);

   TestSuite_AddMockServerTest (
      suite, "/loadbalanced/handshake_sends_loadbalanced", test_loadbalanced_handshake_sends_loadbalanced);

   TestSuite_AddMockServerTest (
      suite, "/loadbalanced/handshake_rejects_non_loadbalanced", test_loadbalanced_handshake_rejects_non_loadbalanced);

   TestSuite_AddMockServerTest (
      suite, "/loadbalanced/pre_handshake_error_does_not_clear_pool", test_pre_handshake_error_does_not_clear_pool);

   TestSuite_AddMockServerTest (
      suite, "/loadbalanced/post_handshake_error_clears_pool", test_post_handshake_error_clears_pool);

   TestSuite_AddFull (suite,
                      "/loadbalanced/sends_recoveryToken",
                      test_loadbalanced_sends_recoveryToken,
                      NULL /* ctx */,
                      NULL /* dtor */,
                      skip_if_not_loadbalanced);
}
