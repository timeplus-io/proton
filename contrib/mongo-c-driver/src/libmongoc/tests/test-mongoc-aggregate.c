#include <mongoc/mongoc.h>
#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

static void
_test_query_flag (mongoc_query_flags_t flag, bson_t *opt)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   future_t *future;
   request_t *request;
   const bson_t *doc;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_aggregate (collection, flag, tmp_bson ("{'pipeline': []}"), opt, NULL);

   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   /* "aggregate" command */
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{'aggregate': 'collection',"
                                                 " 'pipeline': [ ],"
                                                 " 'tailable': {'$exists': false}}"));
   ASSERT (request);
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': {'$numberLong': '123'},"
                            "    'ns': 'db.collection',"
                            "    'nextBatch': [{}]}}");
   ASSERT (future_get_bool (future));
   request_destroy (request);
   future_destroy (future);

   /* "getMore" command */
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'collection',"
                                                 " 'tailable': {'$exists': false}}"));
   ASSERT (request);
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': {'$numberLong': '0'},"
                            "    'ns': 'db.collection',"
                            "    'nextBatch': [{}]}}");

   ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static void
test_query_flags (void)
{
   int i;

   typedef struct {
      mongoc_query_flags_t flag;
      bson_t *opt;
   } flag_and_opt_t;

   flag_and_opt_t flags_and_opts[] = {
      {MONGOC_QUERY_TAILABLE_CURSOR, tmp_bson ("{'tailable': true}")},
      {MONGOC_QUERY_TAILABLE_CURSOR | MONGOC_QUERY_AWAIT_DATA, tmp_bson ("{'tailable': true, 'awaitData': true}")}};

   /* test with both flag and opt */
   for (i = 0; i < (sizeof flags_and_opts) / (sizeof (flag_and_opt_t)); i++) {
      _test_query_flag (flags_and_opts[i].flag, NULL);
      _test_query_flag (MONGOC_QUERY_NONE, flags_and_opts[i].opt);
   }
}

void
test_aggregate_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/Aggregate/query_flags", test_query_flags);
}
