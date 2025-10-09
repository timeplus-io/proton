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

#include "mock_server/mock-server.h"
#include "mock_server/request.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mongoc.h"
#include "mongoc-client-private.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"

#define HELLO_SERVER_ONE                  \
   tmp_str ("{'ok': 1,"                   \
            " 'isWritablePrimary': true," \
            " 'minWireVersion': %d, "     \
            " 'maxWireVersion': %d }",    \
            WIRE_VERSION_MIN,             \
            WIRE_VERSION_MIN)

#define HELLO_SERVER_TWO                  \
   tmp_str ("{'ok': 1,"                   \
            " 'isWritablePrimary': true," \
            " 'minWireVersion': %d,"      \
            " 'maxWireVersion': %d }",    \
            WIRE_VERSION_MIN,             \
            WIRE_VERSION_DELETE_HINT)

/* run_delete_with_hint_and_wc0 runs a delete command with a "hint" option and
 * unacknowledged write concern.
 *
 * If @expect_error is true, expect a client-side error from a maxWireVersion <
 * WIRE_VERSION_DELETE_HINT. */
static void
run_delete_with_hint_and_wc0 (bool expect_error, mongoc_client_t *client, mock_server_t *server)
{
   mongoc_collection_t *coll;
   mongoc_write_concern_t *wc;
   bson_t *delete_selector;
   bson_t *delete_opts;
   bool r;
   bson_error_t error;
   future_t *future;
   request_t *request;

   ASSERT (client);

   coll = mongoc_client_get_collection (client, "db", "coll");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);

   delete_selector = bson_new ();
   delete_opts = BCON_NEW ("hint", "{", "}");
   r = mongoc_write_concern_append (wc, delete_opts);
   ASSERT_WITH_MSG (r, "mongoc_write_concern_append failed");

   future = future_collection_delete_one (coll, delete_selector, delete_opts, NULL /* reply */, &error);
   if (expect_error) {
      /* Expect a client side error. The server does not receive anything. */
      r = future_get_bool (future);
      ASSERT_ERROR_CONTAINS (error,
                             MONGOC_ERROR_COMMAND,
                             MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION,
                             "The selected server does not support hint for delete");
      ASSERT (!r);
   } else {
      request = mock_server_receives_msg (
         server, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{ 'delete': 'coll' }"), tmp_bson ("{'q': {}, 'hint': {}}"));
      reply_to_request_with_ok_and_destroy (request);
      r = future_get_bool (future);
      ASSERT (r);
   }

   future_destroy (future);
   bson_destroy (delete_opts);
   bson_destroy (delete_selector);
   mongoc_write_concern_destroy (wc);
   mongoc_collection_destroy (coll);
}

/* Test that a connection uses the server description from the handshake when
 * checking wire version (instead of the server description from the topology
 * description). */
static void
test_server_stream_ties_server_description_pooled (void *unused)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client_one;
   mongoc_client_t *client_two;
   mongoc_uri_t *uri;
   mock_server_t *server;
   request_t *request;
   future_t *future;
   bson_error_t error;
   mongoc_server_description_t *sd;

   BSON_UNUSED (unused);

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client_one = mongoc_client_pool_pop (pool);
   client_two = mongoc_client_pool_pop (pool);

   /* Respond to the monitoring hello with server one hello. */
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, HELLO_SERVER_ONE);
   request_destroy (request);

   /* Create a connection on client_one. */
   future = future_client_command_simple (
      client_one, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* The first command on a pooled client creates a new connection. */
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, HELLO_SERVER_ONE);
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* Create a connection on client_two. */
   future = future_client_command_simple (
      client_two, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* The first command on a pooled client creates a new connection. */
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, HELLO_SERVER_TWO);
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* Check that selecting the server returns the second server */
   sd = mongoc_client_select_server (client_two, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (sd, error);
   ASSERT_MATCH (mongoc_server_description_hello_response (sd),
                 tmp_str ("{'maxWireVersion': %d}", WIRE_VERSION_DELETE_HINT));
   mongoc_server_description_destroy (sd);

   /* Expect client_one to continue to use maxWireVersion=WIRE_VERSION_MIN for
    * wire version checks. Expect an error when using delete with hint and
    * unacknowledged write concern. */
   run_delete_with_hint_and_wc0 (true, client_one, server);
   /* Expect client_two to continue to use
    * maxWireVersion=WIRE_VERSION_DELETE_HINT for wire version checks. Expect no
    * error when using delete with hint and unacknowledged write concern. */
   run_delete_with_hint_and_wc0 (false, client_two, server);

   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_push (pool, client_one);
   mongoc_client_pool_push (pool, client_two);
   mongoc_client_pool_destroy (pool);
}

/* Test that a connection uses the server description from the handshake when
 * checking wire version (instead of the server description from the topology
 * description). */
static void
test_server_stream_ties_server_description_single (void *unused)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mock_server_t *server;
   request_t *request;
   future_t *future;
   bson_error_t error;
   mongoc_server_description_t *sd;
   mc_tpld_modification tdmod;

   BSON_UNUSED (unused);

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   client = test_framework_client_new_from_uri (uri, NULL);

   /* Create a connection on client. */
   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   /* The first command on a client creates a new connection. */
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, HELLO_SERVER_TWO);
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* Muck with the topology description. */
   /* Pass in a zeroed out error. */
   memset (&error, 0, sizeof (bson_error_t));
   tdmod = mc_tpld_modify_begin (client->topology);
   mongoc_topology_description_handle_hello (tdmod.new_td, 1, tmp_bson (HELLO_SERVER_ONE), 0, &error);
   mc_tpld_modify_commit (tdmod);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* Check that selecting the server returns the first server */
   sd = mongoc_client_select_server (client, true /* for writes */, NULL /* read prefs */, &error);
   ASSERT_OR_PRINT (sd, error);
   ASSERT_MATCH (mongoc_server_description_hello_response (sd), tmp_str ("{'maxWireVersion': %d}", WIRE_VERSION_MIN));
   mongoc_server_description_destroy (sd);

   /* Expect client to continue to use maxWireVersion=WIRE_VERSION_DELETE_HINT
    * for wire version checks. Expect no error when using delete with hint and
    * unacknowledged write concern. */
   run_delete_with_hint_and_wc0 (false, client, server);

   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
}


void
test_server_stream_install (TestSuite *suite)
{
   TestSuite_AddFull (suite,
                      "/server_stream/ties_server_description/pooled",
                      test_server_stream_ties_server_description_pooled,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      NULL);
   TestSuite_AddFull (suite,
                      "/server_stream/ties_server_description/single",
                      test_server_stream_ties_server_description_single,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      NULL);
}
