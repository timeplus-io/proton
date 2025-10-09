/*
 * Copyright 2020 MongoDB, Inc.
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

#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "mock_server/mock-server.h"
#include "mock_server/future-functions.h"
#include "test-libmongoc.h"
#include "mongoc-cluster-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "cmd-test-options"


/* CDRIVER-3303 - mongoc_cmd_parts_assemble sometimes fails to set options;
 * the fix was to refactor the code and this test guards against regressions
 */
static void
test_client_cmd_options (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_read_concern_t *rc;
   bson_t opts;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_MAJORITY);
   bson_init (&opts);
   mongoc_read_concern_append (rc, &opts);

   future =
      future_client_command_with_opts (client, "db", tmp_bson ("{'ping': 1, '$db': 'db'}"), NULL, &opts, NULL, &error);

   request = mock_server_receives_msg (server, MONGOC_QUERY_NONE, tmp_bson ("{'readConcern': { '$exists': true }}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (future_get_bool (future), error);

   request_destroy (request);
   future_destroy (future);

   bson_destroy (&opts);
   mongoc_read_concern_destroy (rc);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static void
capture_last_command (const mongoc_apm_command_started_t *event)
{
   bson_t *last_captured = mongoc_apm_command_started_get_context (event);
   bson_destroy (last_captured);
   const bson_t *cmd = mongoc_apm_command_started_get_command (event);
   bson_copy_to (cmd, last_captured);
}

// `test_cmd_with_two_payload1` tests sending an OP_MSG with two document sequence payloads (payloadType=1).
static void
test_cmd_with_two_payload1 (void *ctx)
{
   BSON_UNUSED (ctx);
   mongoc_client_t *client = test_framework_new_default_client ();

   bson_t last_captured = BSON_INITIALIZER;
   // Set callback to capture the last command.
   {
      mongoc_apm_callbacks_t *cbs = mongoc_apm_callbacks_new ();
      mongoc_apm_set_command_started_cb (cbs, capture_last_command);
      mongoc_client_set_apm_callbacks (client, cbs, &last_captured);
      mongoc_apm_callbacks_destroy (cbs);
   }

   mongoc_cluster_t *cluster = &client->cluster;
   bson_error_t error;

   // Use `bulkWrite`. Currently, only the `bulkWrite` command supports two document sequence payloads.
   bson_t *payload0 = tmp_bson (BSON_STR ({"bulkWrite" : 1}));
   bson_t *op = tmp_bson (BSON_STR ({"insert" : 0, "document" : {}}));
   bson_t *nsInfo = tmp_bson (BSON_STR ({"ns" : "db.coll"}));

   // Create the `mongoc_cmd_t`.
   mongoc_cmd_parts_t parts;
   mongoc_cmd_parts_init (&parts, client, "admin", MONGOC_QUERY_NONE, payload0);
   mongoc_server_stream_t *server_stream = mongoc_cluster_stream_for_writes (
      cluster, NULL /* session */, NULL /* deprioritized servers */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (server_stream, error);
   bool ok = mongoc_cmd_parts_assemble (&parts, server_stream, &error);
   ASSERT_OR_PRINT (ok, error);

   parts.assembled.payloads_count = 2;

   // Set `ops` as a payload1 (of one document)
   parts.assembled.payloads[0].identifier = "ops";
   parts.assembled.payloads[0].documents = bson_get_data (op);
   parts.assembled.payloads[0].size = op->len;

   // Set `nsInfo` as a payload1 (of one document)
   parts.assembled.payloads[1].identifier = "nsInfo";
   parts.assembled.payloads[1].documents = bson_get_data (nsInfo);
   parts.assembled.payloads[1].size = nsInfo->len;

   // Run the command.
   bson_t reply;
   ok = mongoc_cluster_run_command_monitored (cluster, &parts.assembled, &reply, &error);
   ASSERT_OR_PRINT (ok, error);
   ASSERT_MATCH (&reply, BSON_STR ({"ok" : 1}));

   // Check that document sequences are converted to a BSON arrays for command monitoring.
   ASSERT_MATCH (
      &last_captured,
      BSON_STR ({"bulkWrite" : 1, "ops" : [ {"insert" : 0, "document" : {}} ], "nsInfo" : [ {"ns" : "db.coll"} ]}));

   bson_destroy (&reply);
   mongoc_server_stream_cleanup (server_stream);
   mongoc_cmd_parts_cleanup (&parts);
   mongoc_client_destroy (client);
   // Destroy `last_captured` after `client`. `mongoc_client_destroy` sends an `endSessions` command.
   bson_destroy (&last_captured);
}

void
test_client_cmd_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/Client/cmd/options", test_client_cmd_options);
   TestSuite_AddFull (suite,
                      "/cmd/with_two_payload1",
                      test_cmd_with_two_payload1,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_max_wire_version_less_than_25 // require server 8.0 for `bulkWrite`
   );
}
