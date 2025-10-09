#include "mongoc/mongoc.h"
#include "mongoc/mongoc-read-concern-private.h"
#include "mongoc/mongoc-util-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "mock_server/future.h"
#include "mock_server/mock-server.h"
#include "mock_server/future-functions.h"


static void
test_read_concern_append (void)
{
   mongoc_read_concern_t *rc;
   bson_t *cmd;

   cmd = tmp_bson ("{'foo': 1}");

   /* append default readConcern */
   rc = mongoc_read_concern_new ();
   ASSERT (mongoc_read_concern_is_default (rc));
   ASSERT_MATCH (cmd, "{'foo': 1, 'readConcern': {'$exists': false}}");

   /* append readConcern with level */
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   ASSERT (mongoc_read_concern_append (rc, cmd));

   ASSERT_MATCH (cmd, "{'foo': 1, 'readConcern': {'level': 'local'}}");

   mongoc_read_concern_destroy (rc);
}

static void
test_read_concern_basic (void)
{
   mongoc_read_concern_t *read_concern;

   read_concern = mongoc_read_concern_new ();

   BEGIN_IGNORE_DEPRECATIONS

   /*
    * Test defaults.
    */
   ASSERT (read_concern);
   ASSERT (mongoc_read_concern_is_default (read_concern));
   ASSERT (!mongoc_read_concern_get_level (read_concern));

   /*
    * Test changes to level.
    */
   mongoc_read_concern_set_level (read_concern, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   ASSERT (!mongoc_read_concern_is_default (read_concern));
   ASSERT_CMPSTR (mongoc_read_concern_get_level (read_concern), MONGOC_READ_CONCERN_LEVEL_LOCAL);

   /*
    * Check generated bson.
    */
   ASSERT_MATCH (_mongoc_read_concern_get_bson (read_concern), "{'level': 'local'}");

   mongoc_read_concern_destroy (read_concern);
}


static void
test_read_concern_bson_omits_defaults (void)
{
   mongoc_read_concern_t *read_concern;
   const bson_t *bson;
   bson_iter_t iter;

   read_concern = mongoc_read_concern_new ();

   /*
    * Check generated bson.
    */
   ASSERT (read_concern);

   bson = _mongoc_read_concern_get_bson (read_concern);
   ASSERT (bson);
   ASSERT (!bson_iter_init_find (&iter, bson, "level"));

   mongoc_read_concern_destroy (read_concern);
}


static void
test_read_concern_always_mutable (void)
{
   mongoc_read_concern_t *read_concern;

   read_concern = mongoc_read_concern_new ();

   ASSERT (read_concern);

   mongoc_read_concern_set_level (read_concern, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   ASSERT_MATCH (_mongoc_read_concern_get_bson (read_concern), "{'level': 'local'}");

   mongoc_read_concern_set_level (read_concern, MONGOC_READ_CONCERN_LEVEL_MAJORITY);
   ASSERT_MATCH (_mongoc_read_concern_get_bson (read_concern), "{'level': 'majority'}");

   mongoc_read_concern_set_level (read_concern, MONGOC_READ_CONCERN_LEVEL_LINEARIZABLE);
   ASSERT_MATCH (_mongoc_read_concern_get_bson (read_concern), "{'level': 'linearizable'}");

   mongoc_read_concern_destroy (read_concern);
}


static void
_test_read_concern_wire_version (bool explicit)
{
   mongoc_read_concern_t *rc;
   bson_t opts = BSON_INITIALIZER;
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;
   bson_error_t error;

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, "foo");

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   if (explicit) {
      mongoc_read_concern_append (rc, &opts);
   } else {
      mongoc_client_set_read_concern (client, rc);
      mongoc_collection_set_read_concern (collection, rc);
   }

   /*
    * aggregate
    */
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson (NULL), &opts, NULL);

   future = future_cursor_next (cursor, &doc);
   request =
      mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'readConcern': {'level': 'foo'}}"));
   reply_to_request_simple (request, "{'ok': 1, 'cursor': {'id': 0, 'firstBatch': []}}");
   request_destroy (request);
   BSON_ASSERT (future_wait (future));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   future_destroy (future);

   /*
    * generic mongoc_client_write_command_with_opts
    */
   future = future_client_read_command_with_opts (client, "db", tmp_bson ("{'foo': 1}"), NULL, &opts, NULL, &error);
   request =
      mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'readConcern': {'level': 'foo'}}"));
   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));

   future_destroy (future);

   /*
    * count
    */
   future =
      future_collection_count_with_opts (collection, MONGOC_QUERY_NONE, tmp_bson ("{}"), 0, 0, &opts, NULL, &error);
   request =
      mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'readConcern': {'level': 'foo'}}"));
   reply_to_request_simple (request, "{'ok': 1, 'n': 1}");
   request_destroy (request);
   ASSERT_CMPINT64 (future_get_int64_t (future), ==, (int64_t) 1);

   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
   mongoc_read_concern_destroy (rc);
   bson_destroy (&opts);
}


static void
test_inherited_read_concern (void)
{
   _test_read_concern_wire_version (false);
}


static void
test_explicit_read_concern (void)
{
   _test_read_concern_wire_version (true);
}


void
test_read_concern_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/ReadConcern/append", test_read_concern_append);
   TestSuite_Add (suite, "/ReadConcern/basic", test_read_concern_basic);
   TestSuite_Add (suite, "/ReadConcern/bson_omits_defaults", test_read_concern_bson_omits_defaults);
   TestSuite_Add (suite, "/ReadConcern/always_mutable", test_read_concern_always_mutable);
   TestSuite_AddMockServerTest (suite, "/ReadConcern/inherited", test_inherited_read_concern);
   TestSuite_AddMockServerTest (suite, "/ReadConcern/explicit", test_explicit_read_concern);
}
