#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>

#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "json-test.h"
#include "test-libmongoc.h"
#include "mock_server/mock-server.h"
#include "mock_server/future-functions.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "client-test-max-staleness"


static int64_t
get_max_staleness (const mongoc_client_t *client)
{
   const mongoc_read_prefs_t *prefs;

   ASSERT (client);

   prefs = mongoc_client_get_read_prefs (client);

   return mongoc_read_prefs_get_max_staleness_seconds (prefs);
}


/* the next few tests are from max-staleness-tests.rst */
static void
test_mongoc_client_max_staleness (void)
{
   mongoc_client_t *client;

   client = test_framework_client_new (NULL, NULL);
   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) -1);
   mongoc_client_destroy (client);

   client = test_framework_client_new ("mongodb://a/?" MONGOC_URI_READPREFERENCE "=secondary", NULL);
   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) -1);
   mongoc_client_destroy (client);

   /* -1 is the default, means "no max staleness" */
   client = test_framework_client_new ("mongodb://a/?" MONGOC_URI_MAXSTALENESSSECONDS "=-1", NULL);
   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) -1);
   mongoc_client_destroy (client);

   client = test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_READPREFERENCE "=primary&" MONGOC_URI_MAXSTALENESSSECONDS "=-1", NULL);

   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) -1);
   mongoc_client_destroy (client);

   /* no " MONGOC_URI_MAXSTALENESSSECONDS " with primary mode */
   capture_logs (true);
   ASSERT (!test_framework_client_new ("mongodb://a/?" MONGOC_URI_MAXSTALENESSSECONDS "=120", NULL));
   ASSERT_CAPTURED_LOG (MONGOC_URI_MAXSTALENESSSECONDS "=120", MONGOC_LOG_LEVEL_WARNING, "Invalid readPreferences");

   capture_logs (true);
   ASSERT (!test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_READPREFERENCE "=primary&" MONGOC_URI_MAXSTALENESSSECONDS "=120", NULL));
   ASSERT_CAPTURED_LOG (MONGOC_URI_MAXSTALENESSSECONDS "=120", MONGOC_LOG_LEVEL_WARNING, "Invalid readPreferences");
   capture_logs (false);

   /* zero is prohibited */
   capture_logs (true);
   client = test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_MAXSTALENESSSECONDS "=0", NULL);

   ASSERT_CAPTURED_LOG (MONGOC_URI_MAXSTALENESSSECONDS "=0",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Unsupported value for \"" MONGOC_URI_MAXSTALENESSSECONDS "\": \"0\"");
   capture_logs (false);

   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) -1);
   mongoc_client_destroy (client);

   client = test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_MAXSTALENESSSECONDS "=120&" MONGOC_URI_READPREFERENCE "=secondary", NULL);

   ASSERT_CMPINT64 (get_max_staleness (client), ==, (int64_t) 120);
   mongoc_client_destroy (client);

   /* float is ignored */
   capture_logs (true);
   ASSERT (!test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_READPREFERENCE "=secondary&" MONGOC_URI_MAXSTALENESSSECONDS "=10.5", NULL));

   ASSERT_CAPTURED_LOG (
      MONGOC_URI_MAXSTALENESSSECONDS "=10.5", MONGOC_LOG_LEVEL_WARNING, "Invalid " MONGOC_URI_MAXSTALENESSSECONDS);
   capture_logs (false);

   /* 1 is allowed, it'll be rejected once we begin server selection */
   client = test_framework_client_new (
      "mongodb://a/?" MONGOC_URI_READPREFERENCE "=secondary&" MONGOC_URI_MAXSTALENESSSECONDS "=1", NULL);

   ASSERT_EQUAL_DOUBLE (get_max_staleness (client), 1);
   mongoc_client_destroy (client);
}


static void
test_mongos_max_staleness_read_pref (void)
{
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_read_prefs_t *prefs;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   /* count command with mode "secondary", no MONGOC_URI_MAXSTALENESSSECONDS. */
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_collection_set_read_prefs (collection, prefs);
   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " '$readPreference': {"
                                                 "   'mode': 'secondary',"
                                                 "   'maxStalenessSeconds': {'$exists': false}}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   /* count command with mode "secondary". MONGOC_URI_MAXSTALENESSSECONDS=1 is
    * allowed by client, although in real life mongos will reject it */
   mongoc_read_prefs_set_max_staleness_seconds (prefs, 1);
   mongoc_collection_set_read_prefs (collection, prefs);

   mongoc_collection_set_read_prefs (collection, prefs);
   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " '$readPreference': {"
                                                 "   'mode': 'secondary',"
                                                 "   'maxStalenessSeconds': {'$numberLong': '1'}}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   /* For all read preference modes that are not 'primary', drivers MUST set
    * readPreference. */
   mongoc_read_prefs_set_mode (prefs, MONGOC_READ_SECONDARY_PREFERRED);
   mongoc_read_prefs_set_max_staleness_seconds (prefs, MONGOC_NO_MAX_STALENESS);
   mongoc_collection_set_read_prefs (collection, prefs);

   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (
      server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', '$readPreference': {'mode': 'secondaryPreferred'}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   /* CDRIVER-3633: with readPreference mode secondaryPreferred and
    * maxStalenessSeconds set, readPreference MUST be sent. */
   mongoc_read_prefs_set_max_staleness_seconds (prefs, 1);
   mongoc_collection_set_read_prefs (collection, prefs);

   future = future_collection_count (collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " '$readPreference': {"
                                                 "   'mode': 'secondaryPreferred',"
                                                 "   'maxStalenessSeconds': {'$numberLong': '1'}}}"));

   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   ASSERT_OR_PRINT (1 == future_get_int64_t (future), error);

   request_destroy (request);
   future_destroy (future);

   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
_test_last_write_date (bool pooled)
{
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_error_t error;
   bool r;
   mongoc_server_description_t *s0, *s1;
   int64_t delta;

   uri = test_framework_get_uri ();
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 500);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);
      test_framework_set_pool_ssl_opts (pool);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);
      test_framework_set_ssl_opts (client);
   }
   mongoc_uri_destroy (uri);

   collection = get_test_collection (client, "test_last_write_date");
   r = mongoc_collection_insert_one (collection, tmp_bson ("{}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (r, error);

   _mongoc_usleep (1000 * 1000);
   s0 = mongoc_topology_select (client->topology, MONGOC_SS_WRITE, NULL, NULL, &error);
   ASSERT_OR_PRINT (s0, error);

   _mongoc_usleep (1000 * 1000);
   r = mongoc_collection_insert_one (collection, tmp_bson ("{}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (r, error);

   _mongoc_usleep (1000 * 1000);
   s1 = mongoc_topology_select (client->topology, MONGOC_SS_WRITE, NULL, NULL, &error);
   ASSERT_OR_PRINT (s1, error);
   ASSERT_CMPINT64 (s1->last_write_date_ms, !=, (int64_t) -1);

   /* lastWriteDate increased by roughly one second - be lenient, just check
    * it increased by less than 10 seconds */
   delta = s1->last_write_date_ms - s0->last_write_date_ms;
   ASSERT_CMPINT64 (delta, >, (int64_t) 0);
   ASSERT_CMPINT64 (delta, <, (int64_t) 10 * 1000);

   mongoc_server_description_destroy (s0);
   mongoc_server_description_destroy (s1);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
}


static void
test_last_write_date (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_last_write_date (false);
}


static void
test_last_write_date_pooled (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_last_write_date (true);
}


/* run only if wire version is older than 5 */
static void
_test_last_write_date_absent (bool pooled)
{
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   bson_error_t error;
   mongoc_server_description_t *sd;

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
   }

   sd = mongoc_topology_select (client->topology, MONGOC_SS_READ, NULL, NULL, &error);
   ASSERT_OR_PRINT (sd, error);

   /* lastWriteDate absent */
   ASSERT_CMPINT64 (sd->last_write_date_ms, ==, (int64_t) -1);

   mongoc_server_description_destroy (sd);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
}


static void
test_last_write_date_absent (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_last_write_date_absent (false);
}


static void
test_last_write_date_absent_pooled (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_last_write_date_absent (true);
}


static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite (suite, JSON_DIR, "max_staleness", &test_server_selection_logic_cb);
}

void
test_client_max_staleness_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_Add (suite, "/Client/max_staleness", test_mongoc_client_max_staleness);
   TestSuite_AddMockServerTest (suite, "/Client/max_staleness/mongos", test_mongos_max_staleness_read_pref);
   TestSuite_AddFull (suite,
                      "/Client/last_write_date",
                      test_last_write_date,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/Client/last_write_date/pooled",
                      test_last_write_date_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (
      suite, "/Client/last_write_date_absent", test_last_write_date_absent, NULL, NULL, test_framework_skip_if_replset);
   TestSuite_AddFull (suite,
                      "/Client/last_write_date_absent/pooled",
                      test_last_write_date_absent_pooled,
                      NULL,
                      NULL,
                      test_framework_skip_if_replset);
}
