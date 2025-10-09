#include <mongoc/mongoc.h>

#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-topology-description-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "streamable-hello"

#define TV1 "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }"
#define TV2 "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 2 }"

static mongoc_server_description_t *
_force_scan (mongoc_client_t *client, mock_server_t *server, const char *hello)
{
   bson_error_t error;
   request_t *request;
   future_t *future;
   mongoc_server_description_t *sd;

   ASSERT (client);

   /* Mark the topology as "stale" to trigger a scan. */
   client->topology->stale = true;
   future = future_client_select_server (client, true /* for writes */, NULL, &error);
   request = mock_server_receives_any_hello (server);
   reply_to_request_simple (request, hello);
   sd = future_get_mongoc_server_description_ptr (future);
   BSON_ASSERT (sd);

   future_destroy (future);
   request_destroy (request);
   return sd;
}

static void
test_topology_version_update (void)
{
   mongoc_client_t *client;
   mock_server_t *server;
   mongoc_server_description_t *sd;

   server = mock_server_new ();
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   /* Override minHeartbeatFrequencyMS so test does not wait for 500ms when a
    * scan is needed. */
   client->topology->min_heartbeat_frequency_msec = 1;

   sd = _force_scan (client,
                     server,
                     tmp_str ("{'isWritablePrimary': true,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'topologyVersion': " TV1 "}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_4_4));
   ASSERT_MATCH (&sd->topology_version, TV1);
   mongoc_server_description_destroy (sd);

   /* Returned topology version with higher counter overrides. */
   sd = _force_scan (client,
                     server,
                     tmp_str ("{ 'isWritablePrimary': true,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'topologyVersion': " TV2 "}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_4_4));
   ASSERT_MATCH (&sd->topology_version, TV2);
   mongoc_server_description_destroy (sd);

   /* But returned topology version with lower counter does nothing. */
   sd = _force_scan (client,
                     server,
                     tmp_str ("{'isWritablePrimary': true,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'topologyVersion': " TV1 "}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_4_4));
   ASSERT_MATCH (&sd->topology_version, TV2);
   mongoc_server_description_destroy (sd);

   /* Empty topology version overrides. */
   sd = _force_scan (client,
                     server,
                     tmp_str ("{'isWritablePrimary': true,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_4_4));
   BSON_ASSERT (bson_empty (&sd->topology_version));
   mongoc_server_description_destroy (sd);

   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static char *
_single_to_double (const char *in)
{
   char *in_copy;
   char *iter;

   in_copy = bson_strdup (in);

   for (iter = in_copy; *iter; iter++) {
      if (*iter == '\'') {
         *iter = '"';
      }
   }
   return in_copy;
}

static void
_assert_topology_version_compare (const char *tv1, const char *tv2, int expected)
{
   int actual;
   bson_t *tv1_bson;
   bson_t *tv2_bson;
   bson_error_t error;
   char *tv1_quoted = _single_to_double (tv1);
   char *tv2_quoted = _single_to_double (tv2);

   tv1_bson = bson_new_from_json ((const uint8_t *) tv1_quoted, strlen (tv1_quoted), &error);
   ASSERT_OR_PRINT (tv1_bson, error);

   tv2_bson = bson_new_from_json ((const uint8_t *) tv2_quoted, strlen (tv2_quoted), &error);
   ASSERT_OR_PRINT (tv2_bson, error);

   actual = mongoc_server_description_topology_version_cmp (tv1_bson, tv2_bson);
   ASSERT_CMPINT (actual, ==, expected);

   bson_free (tv1_quoted);
   bson_free (tv2_quoted);
   bson_destroy (tv1_bson);
   bson_destroy (tv2_bson);
}

static void
test_topology_version_compare (void)
{
   _assert_topology_version_compare ("{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }",
                                     "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 2 }",
                                     -1);
   _assert_topology_version_compare ("{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }",
                                     "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }",
                                     0);
   _assert_topology_version_compare ("{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 2 }",
                                     "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }",
                                     1);
   /* Different process IDs always compare less. */
   _assert_topology_version_compare ("{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }",
                                     "{ 'processId': { '$oid': 'CCCCCCCCCCCCCCCCCCCCCCCC' }, 'counter': 1 }",
                                     -1);
   /* Missing fields or malformed always compare less. */
   _assert_topology_version_compare (
      "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }", "{ }", -1);
   _assert_topology_version_compare (
      "{ }", "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }", -1);
   _assert_topology_version_compare (
      "{ 'counter': 2 }", "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }", -1);
}

void
test_streamable_hello_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/streamable/topology_version/update", test_topology_version_update);
   TestSuite_Add (suite, "/streamable/topology_version/compare", test_topology_version_compare);
}
