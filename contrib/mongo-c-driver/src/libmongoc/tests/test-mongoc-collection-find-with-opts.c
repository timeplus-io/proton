#include <mongoc/mongoc.h>
#include "mongoc/mongoc-cursor-private.h"
#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "test-libmongoc.h"
#include "mock_server/mock-rs.h"


typedef struct {
   const char *filter;
   bson_t *filter_bson;
   const char *opts;
   bson_t *opts_bson;
   mongoc_read_prefs_t *read_prefs;
   const char *expected_find_command;
   int32_t expected_n_return;
   mongoc_query_flags_t expected_flags;
   uint32_t expected_skip;
} test_collection_find_with_opts_t;


/*--------------------------------------------------------------------------
 *
 * _test_collection_find_command --
 *
 *       Start a mock server with @max_wire_version, connect a client, and
 *       execute a query with @test_data->filter and @test_data->opts. Use
 *       the @check_request_fn callback to verify the client formatted the
 *       query correctly, and @reply_json to respond to the client.
 *
 *--------------------------------------------------------------------------
 */

static void
_test_collection_find_command (test_collection_find_with_opts_t *test_data)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   future_t *future;
   request_t *request;
   const bson_t *doc;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find_with_opts (
      collection, test_data->filter_bson, test_data->opts_bson, test_data->read_prefs);

   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (test_data->expected_find_command));
   ASSERT (request);
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 0,"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");
   ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
_test_collection_find_with_opts (test_collection_find_with_opts_t *test_data)
{
   BSON_ASSERT (test_data->expected_find_command);

   test_data->filter_bson = tmp_bson (test_data->filter);
   test_data->opts_bson = tmp_bson (test_data->opts);

   _test_collection_find_command (test_data);
}


static void
test_dollar_or (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'$or': [{'_id': 1}]}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'$or': [{'_id': 1}]}}";

   _test_collection_find_with_opts (&test_data);
}


/* test '$or': [{'_id': 1}], 'snapshot': true
 * we just use snapshot to prove that an option can be passed
 * alongside '$or'
 */
static void
test_snapshot_dollar_or (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'$or': [{'_id': 1}]}";
   test_data.opts = "{'snapshot': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'$or': [{'_id': 1}]},"
                                     " 'snapshot': true}";

   _test_collection_find_with_opts (&test_data);
}


/* test that we can query for a document by a key named "filter" */
static void
test_key_named_filter (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'filter': 2}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'filter': 2}}";
   _test_collection_find_with_opts (&test_data);
}


/* test 'filter': {'filter': {'i': 2}} */
static void
test_op_query_subdoc_named_filter (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'filter': {'i': 2}}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'filter': {'i': 2}}}";
   _test_collection_find_with_opts (&test_data);
}


/* test 'filter': {'filter': {'i': 2}}, 'snapshot': true
 * we just use snapshot to prove that an option can be passed
 * alongside 'filter'
 */
static void
test_find_cmd_subdoc_named_filter_with_option (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'filter': {'i': 2}}";
   test_data.opts = "{'snapshot': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'filter': {'i': 2}}, "
                                     " 'snapshot': true}";

   _test_collection_find_with_opts (&test_data);
}


static void
test_newoption (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.filter = "{'_id': 1}";
   test_data.opts = "{'newOption': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {'_id': 1}, 'newOption': true}";

   _test_collection_find_with_opts (&test_data);
}


static void
test_sort (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'sort': {'_id': -1}}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'sort': {'_id': -1}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_fields (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'projection': {'_id': 0, 'b': 1}}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'projection': {'_id': 0, 'b': 1}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_slice (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'projection': {'array': {'$slice': 10}}}";
   test_data.expected_find_command = "{'find': 'collection', "
                                     " 'filter': {},"
                                     " 'projection': {'array': {'$slice': 10}}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_int_modifiers (void)
{
   const char *modifiers[] = {
      "maxScan",
      "maxTimeMS",
   };

   const char *mod;
   size_t i;
   char *opts;
   char *find_command;
   test_collection_find_with_opts_t test_data = {0};

   for (i = 0; i < sizeof (modifiers) / sizeof (const char *); i++) {
      mod = modifiers[i];
      opts = bson_strdup_printf ("{'%s': {'$numberLong': '9999'}}", mod);

      /* find command has same modifier, without the $-prefix */
      find_command = bson_strdup_printf ("{'find': 'collection', 'filter': {},"
                                         " '%s': {'$numberLong': '9999'}}",
                                         mod);

      test_data.opts = opts;
      test_data.expected_find_command = find_command;
      _test_collection_find_with_opts (&test_data);

      bson_free (opts);
      bson_free (find_command);
   }
}


static void
test_index_spec_modifiers (void)
{
   const char *modifiers[] = {
      "hint",
      "min",
      "max",
   };

   const char *mod;
   size_t i;
   char *opts;
   char *find_command;
   test_collection_find_with_opts_t test_data = {0};

   for (i = 0; i < sizeof (modifiers) / sizeof (const char *); i++) {
      mod = modifiers[i];
      opts = bson_strdup_printf ("{'%s': {'_id': 1}}", mod);

      /* find command options have no $-prefix: hint, min, max */
      find_command = bson_strdup_printf ("{'find': 'collection', 'filter': {}, '%s': {'_id': 1}}", mod);

      test_data.opts = opts;
      test_data.expected_find_command = find_command;
      _test_collection_find_with_opts (&test_data);

      bson_free (opts);
      bson_free (find_command);
   }
}


static void
test_comment (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'comment': 'COMMENT'}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'comment': 'COMMENT'}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_snapshot (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'snapshot': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'snapshot': true}";
   _test_collection_find_with_opts (&test_data);
}


/* showRecordId becomes $showDiskLoc */
static void
test_diskloc (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'showRecordId': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'showRecordId': true}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_returnkey (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'returnKey': true}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'returnKey': true}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_skip (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.expected_skip = 1;
   test_data.opts = "{'skip': {'$numberLong': '1'}}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'skip': {'$numberLong': '1'}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_batch_size (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'batchSize': {'$numberLong': '2'}}";
   test_data.expected_n_return = 2;
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'batchSize': {'$numberLong': '2'}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_limit (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'limit': {'$numberLong': '2'}}";
   test_data.expected_n_return = 2;
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'limit': {'$numberLong': '2'}}";
   _test_collection_find_with_opts (&test_data);
}


static void
test_singlebatch (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'limit': {'$numberLong': '2'}, 'singleBatch': true}";
   test_data.expected_n_return = -2;
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, "
                                     " 'singleBatch': true, 'limit': {'$numberLong': '2'}}";

   _test_collection_find_with_opts (&test_data);
}

static void
test_singlebatch_no_limit (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'singleBatch': true}";
   /* singleBatch doesn't affect OP_QUERY with limit 0, nToReturn is still 0 */
   test_data.expected_n_return = 0;
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'singleBatch': true}";

   _test_collection_find_with_opts (&test_data);
}


static void
test_unrecognized_dollar_option (void)
{
   test_collection_find_with_opts_t test_data = {0};

   test_data.opts = "{'dumb': 1}";
   test_data.expected_find_command = "{'find': 'collection', 'filter': {}, 'dumb': 1}";

   _test_collection_find_with_opts (&test_data);
}


static void
test_query_flags (void)
{
   int i;
   char *opts;
   char *find_cmd;
   test_collection_find_with_opts_t test_data = {0};

   typedef struct {
      mongoc_query_flags_t flag;
      const char *json_fragment;
   } flag_and_name_t;

   /* secondaryOk is not supported as an option, exhaust is tested separately */
   flag_and_name_t flags_and_frags[] = {
      {MONGOC_QUERY_TAILABLE_CURSOR, "'tailable': true"},
      {MONGOC_QUERY_OPLOG_REPLAY, "'oplogReplay': true"},
      {MONGOC_QUERY_NO_CURSOR_TIMEOUT, "'noCursorTimeout': true"},
      {MONGOC_QUERY_PARTIAL, "'allowPartialResults': true"},
      {MONGOC_QUERY_TAILABLE_CURSOR | MONGOC_QUERY_AWAIT_DATA, "'tailable': true, 'awaitData': true"},
   };

   for (i = 0; i < (sizeof flags_and_frags) / (sizeof (flag_and_name_t)); i++) {
      opts = bson_strdup_printf ("{%s}", flags_and_frags[i].json_fragment);
      find_cmd = bson_strdup_printf ("{'find': 'collection', 'filter': {}, %s}", flags_and_frags[i].json_fragment);

      test_data.opts = opts;
      test_data.expected_flags = flags_and_frags[i].flag;
      test_data.expected_find_command = find_cmd;

      _test_collection_find_with_opts (&test_data);

      bson_free (find_cmd);
      bson_free (opts);
   }
}


static void
test_exhaust (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   request_t *request;
   future_t *future;
   const bson_t *doc;
   bson_error_t error;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), tmp_bson ("{'exhaust': true}"), NULL);

   future = future_cursor_next (cursor, &doc);

   /* Find, getMore and killCursors commands spec: "The find command does not
    * support the exhaust flag from OP_QUERY. Drivers that support exhaust MUST
    * fallback to existing OP_QUERY wire protocol messages."
    */
   request = mock_server_receives_request (server);
   reply_to_find_request (
      request, MONGOC_QUERY_SECONDARY_OK | MONGOC_QUERY_EXHAUST, 0, 0, "db.collection", "{}", false /* is_command */);

   ASSERT (future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_getmore_cmd_await (void)
{
   bson_t *opts;
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   future_t *future;
   request_t *request;
   const bson_t *doc;

   opts = tmp_bson ("{'tailable': true,"
                    " 'awaitData': true,"
                    " 'maxAwaitTimeMS': {'$numberLong': '9999'}}");

   /*
    * "find" command
    */
   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), opts, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'find': 'collection',"
                                                 " 'filter': {},"
                                                 " 'maxTimeMS': {'$exists': false},"
                                                 " 'maxAwaitTimeMS': {'$exists': false}}"));

   ASSERT (request);
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': {'$numberLong': '123'},"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");

   ASSERT (future_get_bool (future));
   request_destroy (request);
   future_destroy (future);

   /*
    * "getMore" command
    */
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'collection',"
                                                 " 'maxAwaitTimeMS': {'$exists': false},"
                                                 " 'maxTimeMS': {'$numberLong': '9999'}}"));

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
test_find_w_server_id (void)
{
   mock_rs_t *rs;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t *opts;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   rs = mock_rs_with_auto_hello (
      WIRE_VERSION_MIN /* wire version */, true /* has primary  */, 1 /* secondary    */, 0 /* arbiters     */);

   mock_rs_run (rs);
   client = test_framework_client_new_from_uri (mock_rs_get_uri (rs), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   /* use serverId instead of prefs to select the secondary */
   opts = tmp_bson ("{'serverId': 2}");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), opts, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_rs_receives_msg (rs,
                                   MONGOC_MSG_NONE,
                                   tmp_bson ("{'$db': 'db',"
                                             " 'find': 'collection',"
                                             " 'filter': {},"
                                             " '$readPreference': {'mode': 'primaryPreferred'}}"));

   ASSERT (mock_rs_request_is_to_secondary (rs, request));
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "   'ns': 'db.collection',"
                            "   'firstBatch': [{}]}}");
   ASSERT_OR_PRINT (future_get_bool (future), cursor->error);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_rs_destroy (rs);
}


static void
test_find_cmd_w_server_id (void)
{
   mock_rs_t *rs;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t *opts;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;
   bson_error_t error;

   rs = mock_rs_with_auto_hello (WIRE_VERSION_MIN, true /* has primary  */, 1 /* secondary    */, 0 /* arbiters     */);

   mock_rs_run (rs);
   client = test_framework_client_new_from_uri (mock_rs_get_uri (rs), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   /* use serverId instead of prefs to select the secondary */
   opts = tmp_bson ("{'serverId': 2, 'readConcern': {'level': 'local'}}");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), opts, NULL);

   future = future_cursor_next (cursor, &doc);

   /* recognized that wire version is recent enough for readConcern */
   request = mock_rs_receives_msg (rs,
                                   MONGOC_MSG_NONE,
                                   tmp_bson ("{'$db': 'db',"
                                             " 'find': 'collection', "
                                             " 'filter': {},"
                                             " 'readConcern': {'level': 'local'},"
                                             " 'serverId': {'$exists': false}}"));

   ASSERT (mock_rs_request_is_to_secondary (rs, request));
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 0,"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");

   ASSERT_OR_PRINT (future_get_bool (future), error);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_rs_destroy (rs);
}


static void
test_find_w_server_id_sharded (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t *opts;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   opts = tmp_bson ("{'serverId': 1}");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), opts, NULL);

   future = future_cursor_next (cursor, &doc);

   /* Does NOT set '$readPreference', since this is a sharded topology. */
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'find': 'collection',"
                                                 " 'filter': {},"
                                                 " '$readPreference': {'$exists': false}}"));

   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 0,"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");
   ASSERT_OR_PRINT (future_get_bool (future), error);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_find_cmd_w_server_id_sharded (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t *opts;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   opts = tmp_bson ("{'serverId': 1, 'readConcern': {'level': 'local'}}");
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson (NULL), opts, NULL);

   future = future_cursor_next (cursor, &doc);

   /* recognized that wire version is recent enough for readConcern */
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'find': 'collection', "
                                                 " 'filter': {},"
                                                 " 'readConcern': {'level': 'local'},"
                                                 " 'serverId': {'$exists': false}}"));

   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 0,"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");

   ASSERT_OR_PRINT (future_get_bool (future), error);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

static void
test_server_id_option (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t *q;
   bson_error_t error;
   mongoc_cursor_t *cursor;

   client = test_framework_new_default_client ();
   collection = mongoc_client_get_collection (client, "db", "collection");
   q = tmp_bson (NULL);
   cursor = mongoc_collection_find_with_opts (collection, q, tmp_bson ("{'serverId': 'foo'}"), NULL);

   ASSERT_ERROR_CONTAINS (cursor->error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "must be an integer");

   mongoc_cursor_destroy (cursor);
   cursor = mongoc_collection_find_with_opts (collection, q, tmp_bson ("{'serverId': 0}"), NULL);

   ASSERT_ERROR_CONTAINS (cursor->error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "must be >= 1");

   mongoc_cursor_destroy (cursor);
   cursor = mongoc_collection_find_with_opts (collection, q, tmp_bson ("{'serverId': 1}"), NULL);

   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}

static void
test_find_batchSize (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_error_t error;
   mongoc_cursor_t *cursor;

   client = test_framework_new_default_client ();
   collection = mongoc_client_get_collection (client, "db", "collection");

   // Test a cursor with an int32 batchSize.
   {
      cursor = mongoc_collection_find_with_opts (
         collection, tmp_bson ("{}"), tmp_bson ("{'batchSize': { '$numberInt': '1' }}"), NULL);

      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

      // Exhaust the cursor.
      {
         // Note: Cursors are lazy. The `find` command is sent on the first call
         // to `mongoc_cursor_next`.
         const bson_t *got;
         while (mongoc_cursor_next (cursor, &got))
            ;
      }

      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      mongoc_cursor_destroy (cursor);
   }

   // Test a cursor with an int64 batchSize.
   {
      cursor = mongoc_collection_find_with_opts (
         collection, tmp_bson ("{}"), tmp_bson ("{'batchSize': { '$numberLong': '1' }}"), NULL);

      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

      // Exhaust the cursor.
      {
         // Note: Cursors are lazy. The `find` command is sent on the first call
         // to `mongoc_cursor_next`.
         const bson_t *got;
         while (mongoc_cursor_next (cursor, &got))
            ;
      }

      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      mongoc_cursor_destroy (cursor);
   }

   // Test a cursor with a string batchSize.
   {
      cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), tmp_bson ("{'batchSize': 'foo'}"), NULL);

      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

      // Attempt to exhaust the cursor.
      {
         // Note: Cursors are lazy. The `find` command is sent on the first call
         // to `mongoc_cursor_next`.
         const bson_t *got;
         while (mongoc_cursor_next (cursor, &got))
            ;
      }

      // Expect an error from the server.
      ASSERT (mongoc_cursor_error (cursor, &error));
      mongoc_cursor_destroy (cursor);
   }

   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}

void
test_collection_find_with_opts_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/dollar_or", test_dollar_or);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/snapshot_dollar_or", test_snapshot_dollar_or);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/key_named_filter", test_key_named_filter);
   TestSuite_AddMockServerTest (
      suite, "/Collection/find_with_opts/query/subdoc_named_filter", test_op_query_subdoc_named_filter);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/newoption", test_newoption);
   TestSuite_AddMockServerTest (
      suite, "/Collection/find_with_opts/cmd/subdoc_named_filter", test_find_cmd_subdoc_named_filter_with_option);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/orderby", test_sort);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/fields", test_fields);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/slice", test_slice);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/modifiers/integer", test_int_modifiers);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/modifiers/index_spec", test_index_spec_modifiers);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/comment", test_comment);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/modifiers/bool", test_snapshot);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/showdiskloc", test_diskloc);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/returnkey", test_returnkey);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/skip", test_skip);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/batch_size", test_batch_size);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/limit", test_limit);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/singlebatch", test_singlebatch);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/singlebatch/no_limit", test_singlebatch_no_limit);
   TestSuite_AddMockServerTest (
      suite, "/Collection/find_with_opts/unrecognized_dollar", test_unrecognized_dollar_option);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/flags", test_query_flags);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/exhaust", test_exhaust);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/await/getmore_cmd", test_getmore_cmd_await);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/server_id", test_find_w_server_id);
   TestSuite_AddMockServerTest (suite, "/Collection/find_cmd_with_opts/server_id", test_find_cmd_w_server_id);
   TestSuite_AddMockServerTest (suite, "/Collection/find_with_opts/server_id/sharded", test_find_w_server_id_sharded);
   TestSuite_AddMockServerTest (
      suite, "/Collection/find_cmd_with_opts/server_id/sharded", test_find_cmd_w_server_id_sharded);
   TestSuite_AddLive (suite, "/Collection/find_with_opts/server_id/option", test_server_id_option);
   TestSuite_AddLive (suite, "/Collection/find/batchSize", test_find_batchSize);
}
