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
#include "mock_server/mock-server.h"
#include "mock_server/future-functions.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "client-test-hedged-reads"


static void
test_mongos_hedged_reads_read_pref (void)
{
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   bson_t hedge_doc = BSON_INITIALIZER;
   mongoc_read_prefs_t *prefs;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);

   /* For all read preference modes that are not 'primary', drivers MUST set
    * readPreference. */
   mongoc_collection_set_read_prefs (collection, prefs);

   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " '$readPreference': {'mode': 'secondaryPreferred'}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   /* CDRIVER-3583:
    * with readPreference mode secondaryPreferred and hedge set, readPreference
    * MUST be sent. */
   bson_append_bool (&hedge_doc, "enabled", 7, true);
   mongoc_read_prefs_set_hedge (prefs, &hedge_doc);
   mongoc_collection_set_read_prefs (collection, prefs);

   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " '$readPreference': {"
                                                 "   'mode': 'secondaryPreferred',"
                                                 "   'hedge': {'enabled': true}}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   mongoc_read_prefs_destroy (prefs);
   bson_destroy (&hedge_doc);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


void
test_client_hedged_reads_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/Client/hedged_reads/mongos", test_mongos_hedged_reads_read_pref);
}
