#include <mongoc/mongoc.h>
#include <mongoc/mongoc-bulk-operation-private.h>
#include <mongoc/mongoc-collection-private.h>

#include "json-test.h"
#include "test-libmongoc.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "json-test-operations.h"
#include "unified/runner.h"


const char *
first_key (const bson_t *bson)
{
   bson_iter_t iter;

   BSON_ASSERT (bson_iter_init (&iter, bson));
   if (!bson_iter_next (&iter)) {
      return NULL;
   }

   return bson_iter_key (&iter);
}


void
check_operation_ids (const bson_t *events)
{
   bson_iter_t iter;
   int64_t first_operation_id = -1;
   int64_t operation_id;
   bson_t event;

   /* check op ids of events like {command_started_event: {operation_id: N}} */
   BSON_ASSERT (bson_iter_init (&iter, events));
   while (bson_iter_next (&iter)) {
      bson_iter_bson (&iter, &event);
      if (!strcmp (first_key (&event), "command_started_event")) {
         operation_id = bson_lookup_int64 (&event, "command_started_event.operation_id");

         if (first_operation_id == -1) {
            first_operation_id = operation_id;
         } else if (operation_id != first_operation_id) {
            test_error ("%s sent wrong operation_id", bson_lookup_utf8 (&event, "command_started_event.command_name"));
         }
      }
   }
}


static bool
command_monitoring_test_run_operation (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bson_t reply;
   bool res;

   /* Command Monitoring tests don't use explicit session */
   res = json_test_operation (ctx, test, operation, ctx->collection, NULL, &reply);

   bson_destroy (&reply);

   return res;
}


/*
 *-----------------------------------------------------------------------
 *
 * test_command_monitoring_cb --
 *
 *       Runs the JSON tests included with the Command Monitoring spec.
 *
 *-----------------------------------------------------------------------
 */

static void
test_command_monitoring_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;
   config.run_operation_cb = command_monitoring_test_run_operation;
   config.scenario = scenario;
   config.events_check_cb = check_operation_ids;
   run_json_general_test (&config);
   json_test_config_cleanup (&config);
}


/*
 *-----------------------------------------------------------------------
 *
 * Runner for the JSON tests for command monitoring.
 *
 *-----------------------------------------------------------------------
 */
static void
test_all_spec_tests (TestSuite *suite)
{
   run_unified_tests (suite, JSON_DIR, "command_monitoring/unified");
   install_json_test_suite (suite, JSON_DIR, "command_monitoring/legacy", &test_command_monitoring_cb);
}


static void
test_get_error_failed_cb (const mongoc_apm_command_failed_t *event)
{
   bson_error_t *error;

   error = (bson_error_t *) mongoc_apm_command_failed_get_context (event);
   mongoc_apm_command_failed_get_error (event, error);
}


static void
test_get_error (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_apm_callbacks_t *callbacks;
   future_t *future;
   request_t *request;
   bson_error_t error = {0};

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_failed_cb (callbacks, test_get_error_failed_cb);
   mongoc_client_set_apm_callbacks (client, callbacks, (void *) &error);
   future = future_client_command_simple (client, "db", tmp_bson ("{'foo': 1}"), NULL, NULL, NULL);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'foo': 1}"));
   reply_to_request_simple (request, "{'ok': 0, 'errmsg': 'foo', 'code': 42}");
   ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_QUERY, 42, "foo");

   future_destroy (future);
   request_destroy (request);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
insert_200_docs (mongoc_collection_t *collection)
{
   int i;
   bson_t *doc;
   bool r;
   bson_error_t error;

   /* insert 200 docs so we have a couple batches */
   doc = tmp_bson (NULL);
   for (i = 0; i < 200; i++) {
      r = mongoc_collection_insert_one (collection, doc, NULL, NULL, &error);

      ASSERT_OR_PRINT (r, error);
   }
}


static void
increment (const mongoc_apm_command_started_t *event)
{
   int *i = (int *) mongoc_apm_command_started_get_context (event);

   ++(*i);
}


static mongoc_apm_callbacks_t *
increment_callbacks (void)
{
   mongoc_apm_callbacks_t *callbacks;

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, increment);

   return callbacks;
}


static void
decrement (const mongoc_apm_command_started_t *event)
{
   int *i = (int *) mongoc_apm_command_started_get_context (event);

   --(*i);
}


static mongoc_apm_callbacks_t *
decrement_callbacks (void)
{
   mongoc_apm_callbacks_t *callbacks;

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, decrement);

   return callbacks;
}


static void
test_change_callbacks (void)
{
   mongoc_apm_callbacks_t *inc_callbacks;
   mongoc_apm_callbacks_t *dec_callbacks;
   int incremented = 0;
   int decremented = 0;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_error_t error;
   mongoc_cursor_t *cursor;
   const bson_t *b;

   inc_callbacks = increment_callbacks ();
   dec_callbacks = decrement_callbacks ();

   client = test_framework_new_default_client ();
   mongoc_client_set_apm_callbacks (client, inc_callbacks, &incremented);

   collection = get_test_collection (client, "test_change_callbacks");

   insert_200_docs (collection);
   ASSERT_CMPINT (incremented, ==, 200);

   mongoc_client_set_apm_callbacks (client, dec_callbacks, &decremented);
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson (NULL), NULL, NULL);

   ASSERT (mongoc_cursor_next (cursor, &b));
   ASSERT_CMPINT (decremented, ==, -1);

   mongoc_client_set_apm_callbacks (client, inc_callbacks, &incremented);
   while (mongoc_cursor_next (cursor, &b)) {
   }
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   ASSERT_CMPINT (incremented, ==, 201);

   mongoc_collection_drop (collection, NULL);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mongoc_apm_callbacks_destroy (inc_callbacks);
   mongoc_apm_callbacks_destroy (dec_callbacks);
}


static void
test_reset_callbacks (void)
{
   mongoc_apm_callbacks_t *inc_callbacks;
   mongoc_apm_callbacks_t *dec_callbacks;
   int incremented = 0;
   int decremented = 0;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bool r;
   bson_t *cmd;
   bson_t cmd_reply;
   bson_error_t error;
   mongoc_server_description_t *sd;
   mongoc_cursor_t *cursor;
   const bson_t *b;

   inc_callbacks = increment_callbacks ();
   dec_callbacks = decrement_callbacks ();

   client = test_framework_new_default_client ();
   collection = get_test_collection (client, "test_reset_apm_callbacks");

   /* insert 200 docs so we have a couple batches */
   insert_200_docs (collection);

   mongoc_client_set_apm_callbacks (client, inc_callbacks, &incremented);
   cmd = tmp_bson ("{'aggregate': '%s', 'pipeline': [], 'cursor': {}}", collection->collection);

   sd = mongoc_client_select_server (client, true /* for writes */, NULL, &error);
   ASSERT_OR_PRINT (sd, error);

   r = mongoc_client_read_command_with_opts (
      client, "test", cmd, NULL, tmp_bson ("{'serverId': %d}", sd->id), &cmd_reply, &error);

   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT (incremented, ==, 1);

   /* reset callbacks */
   mongoc_client_set_apm_callbacks (client, NULL, NULL);
   /* destroys cmd_reply */
   cursor = mongoc_cursor_new_from_command_reply (client, &cmd_reply, sd->id);
   ASSERT (mongoc_cursor_next (cursor, &b));
   ASSERT_CMPINT (incremented, ==, 1); /* same value as before */

   mongoc_client_set_apm_callbacks (client, dec_callbacks, &decremented);
   while (mongoc_cursor_next (cursor, &b)) {
   }
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   ASSERT_CMPINT (decremented, ==, -1);

   mongoc_collection_drop (collection, NULL);

   mongoc_cursor_destroy (cursor);
   mongoc_server_description_destroy (sd);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mongoc_apm_callbacks_destroy (inc_callbacks);
   mongoc_apm_callbacks_destroy (dec_callbacks);
}


static void
test_set_callbacks_cb (const mongoc_apm_command_started_t *event)
{
   int *n_calls = (int *) mongoc_apm_command_started_get_context (event);

   (*n_calls)++;
}


static void
_test_set_callbacks (bool pooled, bool try_pop)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_apm_callbacks_t *callbacks;
   int n_calls = 0;
   bson_error_t error;

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_set_callbacks_cb);

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      ASSERT (mongoc_client_pool_set_apm_callbacks (pool, callbacks, (void *) &n_calls));
      if (try_pop) {
         client = mongoc_client_pool_try_pop (pool);
      } else {
         client = mongoc_client_pool_pop (pool);
      }
   } else {
      client = test_framework_new_default_client ();
      ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, (void *) &n_calls));
   }

   ASSERT_OR_PRINT (
      mongoc_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL, &error),
      error);
   ASSERT_CMPINT (1, ==, n_calls);

   capture_logs (true);

   if (pooled) {
      ASSERT (!mongoc_client_pool_set_apm_callbacks (pool, NULL, (void *) &n_calls));
      ASSERT_CAPTURED_LOG (
         "mongoc_client_pool_set_apm_callbacks", MONGOC_LOG_LEVEL_ERROR, "Can only set callbacks once");

      clear_captured_logs ();
      ASSERT (!mongoc_client_set_apm_callbacks (client, NULL, (void *) &n_calls));
      ASSERT_CAPTURED_LOG (
         "mongoc_client_pool_set_apm_callbacks", MONGOC_LOG_LEVEL_ERROR, "Cannot set callbacks on a pooled client");
   } else {
      /* repeated calls ok, null is ok */
      ASSERT (mongoc_client_set_apm_callbacks (client, NULL, NULL));
   }

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_set_callbacks_single (void)
{
   _test_set_callbacks (false, false);
}


static void
test_set_callbacks_pooled (void)
{
   _test_set_callbacks (true, false);
}

static void
test_set_callbacks_pooled_try_pop (void)
{
   _test_set_callbacks (true, true);
}


typedef struct {
   int64_t request_id;
   int64_t op_id;
} ids_t;


typedef struct {
   mongoc_array_t started_ids;
   mongoc_array_t succeeded_ids;
   mongoc_array_t failed_ids;
   int started_calls;
   int succeeded_calls;
   int failed_calls;
   char *db;
} op_id_test_t;


static void
op_id_test_init (op_id_test_t *test)
{
   _mongoc_array_init (&test->started_ids, sizeof (ids_t));
   _mongoc_array_init (&test->succeeded_ids, sizeof (ids_t));
   _mongoc_array_init (&test->failed_ids, sizeof (ids_t));

   test->started_calls = 0;
   test->succeeded_calls = 0;
   test->failed_calls = 0;
   test->db = NULL;
}


static void
op_id_test_cleanup (op_id_test_t *test)
{
   _mongoc_array_destroy (&test->started_ids);
   _mongoc_array_destroy (&test->succeeded_ids);
   _mongoc_array_destroy (&test->failed_ids);
   bson_free (test->db);
}


static void
test_op_id_started_cb (const mongoc_apm_command_started_t *event)
{
   op_id_test_t *test;
   ids_t ids;

   test = (op_id_test_t *) mongoc_apm_command_started_get_context (event);
   ids.request_id = mongoc_apm_command_started_get_request_id (event);
   ids.op_id = mongoc_apm_command_started_get_operation_id (event);

   _mongoc_array_append_val (&test->started_ids, ids);

   test->started_calls++;
}


static void
test_op_id_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   op_id_test_t *test;
   ids_t ids;

   test = (op_id_test_t *) mongoc_apm_command_succeeded_get_context (event);
   ids.request_id = mongoc_apm_command_succeeded_get_request_id (event);
   ids.op_id = mongoc_apm_command_succeeded_get_operation_id (event);

   _mongoc_array_append_val (&test->succeeded_ids, ids);

   test->succeeded_calls++;
}


static void
test_op_id_failed_cb (const mongoc_apm_command_failed_t *event)
{
   op_id_test_t *test;
   ids_t ids;

   test = (op_id_test_t *) mongoc_apm_command_failed_get_context (event);
   ids.request_id = mongoc_apm_command_failed_get_request_id (event);
   ids.op_id = mongoc_apm_command_failed_get_operation_id (event);

   bson_free (test->db);
   test->db = bson_strdup (mongoc_apm_command_failed_get_database_name (event));

   _mongoc_array_append_val (&test->failed_ids, ids);

   test->failed_calls++;
}


#define REQUEST_ID(_event_type, _index) _mongoc_array_index (&test._event_type##_ids, ids_t, _index).request_id

#define OP_ID(_event_type, _index) _mongoc_array_index (&test._event_type##_ids, ids_t, _index).op_id

static void
_test_bulk_operation_id (bool pooled, bool use_bulk_operation_new)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *collection;
   bson_t opts = BSON_INITIALIZER;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   op_id_test_t test;
   int64_t op_id;

   op_id_test_init (&test);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_op_id_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, test_op_id_succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, test_op_id_failed_cb);

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      ASSERT (mongoc_client_pool_set_apm_callbacks (pool, callbacks, (void *) &test));
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
      ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test));
   }

   collection = get_test_collection (client, "test_bulk_operation_id");
   if (use_bulk_operation_new) {
      bulk = mongoc_bulk_operation_new (false);
      mongoc_bulk_operation_set_client (bulk, client);
      mongoc_bulk_operation_set_database (bulk, collection->db);
      mongoc_bulk_operation_set_collection (bulk, collection->collection);
   } else {
      bson_append_bool (&opts, "ordered", 7, false);
      bulk = mongoc_collection_create_bulk_operation_with_opts (collection, &opts);
   }

   mongoc_bulk_operation_insert (bulk, tmp_bson ("{'_id': 1}"));
   mongoc_bulk_operation_update_one (bulk, tmp_bson ("{'_id': 1}"), tmp_bson ("{'$set': {'x': 1}}"), false);
   mongoc_bulk_operation_remove (bulk, tmp_bson ("{}"));

   /* ensure we monitor with bulk->operation_id, not cluster->operation_id */
   client->cluster.operation_id = 42;

   /* write errors don't trigger failed events, so we only test success */
   ASSERT_OR_PRINT (mongoc_bulk_operation_execute (bulk, NULL, &error), error);
   ASSERT_CMPINT (test.started_calls, ==, 3);
   ASSERT_CMPINT (test.succeeded_calls, ==, 3);

   ASSERT_CMPINT64 (REQUEST_ID (started, 0), ==, REQUEST_ID (succeeded, 0));
   ASSERT_CMPINT64 (REQUEST_ID (started, 1), ==, REQUEST_ID (succeeded, 1));
   ASSERT_CMPINT64 (REQUEST_ID (started, 2), ==, REQUEST_ID (succeeded, 2));

   /* 3 unique request ids */
   ASSERT_CMPINT64 (REQUEST_ID (started, 0), !=, REQUEST_ID (started, 1));
   ASSERT_CMPINT64 (REQUEST_ID (started, 0), !=, REQUEST_ID (started, 2));
   ASSERT_CMPINT64 (REQUEST_ID (started, 1), !=, REQUEST_ID (started, 2));
   ASSERT_CMPINT64 (REQUEST_ID (succeeded, 0), !=, REQUEST_ID (succeeded, 1));
   ASSERT_CMPINT64 (REQUEST_ID (succeeded, 0), !=, REQUEST_ID (succeeded, 2));
   ASSERT_CMPINT64 (REQUEST_ID (succeeded, 1), !=, REQUEST_ID (succeeded, 2));

   /* events' operation ids all equal bulk->operation_id */
   op_id = bulk->operation_id;
   ASSERT_CMPINT64 (op_id, !=, (int64_t) 0);
   ASSERT_CMPINT64 (op_id, ==, OP_ID (started, 0));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (started, 1));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (started, 2));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (succeeded, 0));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (succeeded, 1));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (succeeded, 2));

   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   bson_destroy (&opts);
   op_id_test_cleanup (&test);
   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_collection_bulk_op_single (void)
{
   _test_bulk_operation_id (false, false);
}


static void
test_collection_bulk_op_pooled (void)
{
   _test_bulk_operation_id (true, false);
}


static void
test_bulk_op_single (void)
{
   _test_bulk_operation_id (false, true);
}


static void
test_bulk_op_pooled (void)
{
   _test_bulk_operation_id (true, true);
}


static void
_test_query_operation_id (bool pooled)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *collection;
   op_id_test_t test;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;
   int64_t op_id;

   op_id_test_init (&test);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_op_id_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, test_op_id_succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, test_op_id_failed_cb);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (mock_server_get_uri (server), NULL);
      ASSERT (mongoc_client_pool_set_apm_callbacks (pool, callbacks, (void *) &test));
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
      ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test));
   }

   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 1, tmp_bson ("{}"), NULL, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_request (server);
   reply_to_op_msg_request (request,
                            MONGOC_MSG_NONE,
                            tmp_bson ("{'ok': 1,"
                                      " 'cursor': {"
                                      "    'id': {'$numberLong': '123'},"
                                      "    'ns': 'db2.collection',"
                                      "    'firstBatch': [{}]}}"));

   ASSERT (future_get_bool (future));
   future_destroy (future);
   request_destroy (request);

   ASSERT_CMPINT (test.started_calls, ==, 1);
   ASSERT_CMPINT (test.succeeded_calls, ==, 1);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_request (server);
   reply_to_request_simple (request, "{'ok': 0, 'code': 42, 'errmsg': 'bad!'}");

   ASSERT (!future_get_bool (future));
   future_destroy (future);
   request_destroy (request);

   ASSERT_CMPINT (test.started_calls, ==, 2);
   ASSERT_CMPINT (test.succeeded_calls, ==, 1);
   ASSERT_CMPINT (test.failed_calls, ==, 1);
   ASSERT_CMPSTR (test.db, "db2");

   ASSERT_CMPINT64 (REQUEST_ID (started, 0), ==, REQUEST_ID (succeeded, 0));
   ASSERT_CMPINT64 (REQUEST_ID (started, 1), ==, REQUEST_ID (failed, 0));

   /* unique request ids */
   ASSERT_CMPINT64 (REQUEST_ID (started, 0), !=, REQUEST_ID (started, 1));

   /* operation ids all the same */
   op_id = OP_ID (started, 0);
   ASSERT_CMPINT64 (op_id, !=, (int64_t) 0);
   ASSERT_CMPINT64 (op_id, ==, OP_ID (started, 1));
   ASSERT_CMPINT64 (op_id, ==, OP_ID (failed, 0));

   mock_server_destroy (server);

   /* client logs warning because it can't send killCursors or endSessions */
   capture_logs (true);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   op_id_test_cleanup (&test);
   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_query_operation_id_single_cmd (void)
{
   _test_query_operation_id (false);
}


static void
test_query_operation_id_pooled_cmd (void)
{
   _test_query_operation_id (true);
}

typedef struct {
   int started_calls;
   int succeeded_calls;
   int failed_calls;
   char db[100];
   char cmd_name[100];
   bson_t cmd;
} cmd_test_t;


static void
cmd_test_init (cmd_test_t *test)
{
   memset (test, 0, sizeof *test);
   bson_init (&test->cmd);
}


static void
cmd_test_cleanup (cmd_test_t *test)
{
   bson_destroy (&test->cmd);
}


static void
cmd_started_cb (const mongoc_apm_command_started_t *event)
{
   cmd_test_t *test;

   if (!strcmp (mongoc_apm_command_started_get_command_name (event), "endSessions")) {
      /* the test is ending */
      return;
   }

   test = (cmd_test_t *) mongoc_apm_command_started_get_context (event);
   test->started_calls++;
   bson_destroy (&test->cmd);
   bson_strncpy (test->db, mongoc_apm_command_started_get_database_name (event), sizeof (test->db));
   bson_copy_to (mongoc_apm_command_started_get_command (event), &test->cmd);
   bson_strncpy (test->cmd_name, mongoc_apm_command_started_get_command_name (event), sizeof (test->cmd_name));
}


static void
cmd_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   cmd_test_t *test;
   int64_t duration;

   if (!strcmp (mongoc_apm_command_succeeded_get_command_name (event), "endSessions")) {
      return;
   }

   test = (cmd_test_t *) mongoc_apm_command_succeeded_get_context (event);
   test->succeeded_calls++;
   ASSERT_CMPSTR (test->cmd_name, mongoc_apm_command_succeeded_get_command_name (event));

   duration = mongoc_apm_command_succeeded_get_duration (event);
   ASSERT_CMPINT64 (duration, >=, (int64_t) 0);
   ASSERT_CMPINT64 (duration, <=, (int64_t) 10000000); /* ten seconds */
}


static void
cmd_failed_cb (const mongoc_apm_command_failed_t *event)
{
   cmd_test_t *test;
   int64_t duration;

   test = (cmd_test_t *) mongoc_apm_command_failed_get_context (event);
   test->failed_calls++;
   ASSERT_CMPSTR (test->cmd_name, mongoc_apm_command_failed_get_command_name (event));

   duration = mongoc_apm_command_failed_get_duration (event);
   ASSERT_CMPINT64 (duration, >=, (int64_t) 0);
   ASSERT_CMPINT64 (duration, <=, (int64_t) 10000000); /* ten seconds */
}


static void
set_cmd_test_callbacks (mongoc_client_t *client, void *context)
{
   mongoc_apm_callbacks_t *callbacks;

   ASSERT (client);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, cmd_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, cmd_succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, cmd_failed_cb);
   ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, context));
   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_client_cmd (void)
{
   cmd_test_t test;
   mongoc_client_t *client;
   mongoc_cursor_t *cursor;
   const bson_t *reply;

   cmd_test_init (&test);
   client = test_framework_new_default_client ();
   set_cmd_test_callbacks (client, (void *) &test);
   cursor =
      mongoc_client_command (client, "admin", MONGOC_QUERY_SECONDARY_OK, 0, 0, 0, tmp_bson ("{'ping': 1}"), NULL, NULL);

   ASSERT (mongoc_cursor_next (cursor, &reply));
   ASSERT_CMPSTR (test.cmd_name, "ping");
   ASSERT_MATCH (&test.cmd, "{'ping': 1}");
   ASSERT_CMPSTR (test.db, "admin");
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (1, ==, test.succeeded_calls);
   ASSERT_CMPINT (0, ==, test.failed_calls);

   cmd_test_cleanup (&test);
   mongoc_cursor_destroy (cursor);

   cmd_test_init (&test);
   cursor =
      mongoc_client_command (client, "admin", MONGOC_QUERY_SECONDARY_OK, 0, 0, 0, tmp_bson ("{'foo': 1}"), NULL, NULL);

   ASSERT (!mongoc_cursor_next (cursor, &reply));
   ASSERT_CMPSTR (test.cmd_name, "foo");
   ASSERT_MATCH (&test.cmd, "{'foo': 1}");
   ASSERT_CMPSTR (test.db, "admin");
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (0, ==, test.succeeded_calls);
   ASSERT_CMPINT (1, ==, test.failed_calls);

   mongoc_cursor_destroy (cursor);
   mongoc_client_destroy (client);
   cmd_test_cleanup (&test);
}


static void
test_client_cmd_simple (void)
{
   cmd_test_t test;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;

   cmd_test_init (&test);
   client = test_framework_new_default_client ();
   set_cmd_test_callbacks (client, (void *) &test);
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPSTR (test.cmd_name, "ping");
   ASSERT_MATCH (&test.cmd, "{'ping': 1}");
   ASSERT_CMPSTR (test.db, "admin");
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (1, ==, test.succeeded_calls);
   ASSERT_CMPINT (0, ==, test.failed_calls);

   cmd_test_cleanup (&test);
   cmd_test_init (&test);
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'foo': 1}"), NULL, NULL, &error);

   ASSERT (!r);
   ASSERT_CMPSTR (test.cmd_name, "foo");
   ASSERT_MATCH (&test.cmd, "{'foo': 1}");
   ASSERT_CMPSTR (test.db, "admin");
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (0, ==, test.succeeded_calls);
   ASSERT_CMPINT (1, ==, test.failed_calls);

   mongoc_client_destroy (client);
   cmd_test_cleanup (&test);
}


static void
test_client_cmd_op_ids (void)
{
   op_id_test_t test;
   mongoc_client_t *client;
   mongoc_apm_callbacks_t *callbacks;
   bool r;
   bson_error_t error;
   int64_t op_id;

   op_id_test_init (&test);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_op_id_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, test_op_id_succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, test_op_id_failed_cb);

   client = test_framework_new_default_client ();
   mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test);

   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (1, ==, test.succeeded_calls);
   ASSERT_CMPINT (0, ==, test.failed_calls);
   ASSERT_CMPINT64 (REQUEST_ID (started, 0), ==, REQUEST_ID (succeeded, 0));
   ASSERT_CMPINT64 (OP_ID (started, 0), ==, OP_ID (succeeded, 0));
   op_id = OP_ID (started, 0);
   ASSERT_CMPINT64 (op_id, !=, (int64_t) 0);

   op_id_test_cleanup (&test);
   op_id_test_init (&test);

   /* again. test that we use a new op_id. */
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT (1, ==, test.started_calls);
   ASSERT_CMPINT (1, ==, test.succeeded_calls);
   ASSERT_CMPINT (0, ==, test.failed_calls);
   ASSERT_CMPINT64 (REQUEST_ID (started, 0), ==, REQUEST_ID (succeeded, 0));
   ASSERT_CMPINT64 (OP_ID (started, 0), ==, OP_ID (succeeded, 0));
   ASSERT_CMPINT64 (OP_ID (started, 0), !=, (int64_t) 0);

   /* new op_id */
   ASSERT_CMPINT64 (OP_ID (started, 0), !=, op_id);

   mongoc_client_destroy (client);
   op_id_test_cleanup (&test);
   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_killcursors_deprecated (void *unused)
{
   cmd_test_t test;
   mongoc_client_t *client;
   bool r;
   bson_error_t error;

   BSON_UNUSED (unused);

   cmd_test_init (&test);
   client = test_framework_new_default_client ();

   /* connect */
   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);

   ASSERT_OR_PRINT (r, error);
   set_cmd_test_callbacks (client, (void *) &test);

   /* deprecated function without "db" or "collection", skips APM. This sends
    * OP_KILL_CURSORS. */
   mongoc_client_kill_cursor (client, 123);

   ASSERT_CMPINT (0, ==, test.started_calls);
   ASSERT_CMPINT (0, ==, test.succeeded_calls);
   ASSERT_CMPINT (0, ==, test.failed_calls);

   mongoc_client_destroy (client);
   cmd_test_cleanup (&test);
}


typedef struct {
   int failed_calls;
   bson_t reply;
   char *db;
} cmd_failed_reply_test_t;


static void
cmd_failed_reply_test_init (cmd_failed_reply_test_t *test)
{
   memset (test, 0, sizeof *test);
   bson_init (&test->reply);
}


static void
cmd_failed_reply_test_cleanup (cmd_failed_reply_test_t *test)
{
   bson_destroy (&test->reply);
   bson_free (test->db);
}


static void
command_failed_reply_command_failed_cb (const mongoc_apm_command_failed_t *event)
{
   cmd_failed_reply_test_t *test;

   test = (cmd_failed_reply_test_t *) mongoc_apm_command_failed_get_context (event);
   test->failed_calls++;

   bson_free (test->db);
   test->db = bson_strdup (mongoc_apm_command_failed_get_database_name (event));

   bson_destroy (&test->reply);
   bson_copy_to (mongoc_apm_command_failed_get_reply (event), &test->reply);
}


static void
test_command_failed_reply_mock (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *collection;
   cmd_failed_reply_test_t test;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   /* test that the command_failed_event's reply is the same as a mocked reply
    */
   cmd_failed_reply_test_init (&test);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_failed_cb (callbacks, command_failed_reply_command_failed_cb);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test));

   collection = mongoc_client_get_collection (client, "db", "collection");
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 1, tmp_bson ("{}"), NULL, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_request (server);
   reply_to_request_simple (request, "{'ok': 0, 'code': 42, 'errmsg': 'bad!'}");

   ASSERT (!future_get_bool (future));
   future_destroy (future);
   request_destroy (request);

   ASSERT_MATCH (&test.reply, "{'ok': 0, 'code': 42, 'errmsg': 'bad!'}");
   ASSERT_CMPINT (test.failed_calls, ==, 1);
   ASSERT_CMPSTR (test.db, "db");

   mock_server_destroy (server);

   /* client logs warning because it can't send killCursors or endSessions */
   capture_logs (true);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   mongoc_client_destroy (client);

   cmd_failed_reply_test_cleanup (&test);
   mongoc_apm_callbacks_destroy (callbacks);
}


static void
test_command_failed_reply_hangup (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *collection;
   cmd_failed_reply_test_t test;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   /* test that the command_failed_event's reply is empty if there is a network
    * error (i.e. the server hangs up) */
   cmd_failed_reply_test_init (&test);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_failed_cb (callbacks, command_failed_reply_command_failed_cb);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test));

   collection = mongoc_client_get_collection (client, "db2", "collection");
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 1, tmp_bson ("{}"), NULL, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_request (server);
   reply_to_request_simple (request, "{'ok': 0, 'code': 42, 'errmsg': 'bad!'}");

   ASSERT (!future_get_bool (future));
   future_destroy (future);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   ASSERT_MATCH (&test.reply, "{}");
   ASSERT_CMPINT (test.failed_calls, ==, 1);
   ASSERT_CMPSTR (test.db, "db2");

   mock_server_destroy (server);

   /* client logs warning because it can't send killCursors or endSessions */
   capture_logs (true);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   mongoc_client_destroy (client);

   cmd_failed_reply_test_cleanup (&test);
   mongoc_apm_callbacks_destroy (callbacks);
}


typedef struct {
   int started_calls;
   int succeeded_calls;
   int failed_calls;
   bool has_service_id;
   bson_oid_t expected_service_id;
} service_id_test_t;


static void
assert_service_id (service_id_test_t *test, const bson_oid_t *actual_service_id)
{
   if (test->has_service_id) {
      BSON_ASSERT (actual_service_id);
      ASSERT_CMPOID (actual_service_id, &test->expected_service_id);
   } else {
      BSON_ASSERT (!actual_service_id);
   }
}


static void
service_id_cmd_started_cb (const mongoc_apm_command_started_t *event)
{
   service_id_test_t *test = mongoc_apm_command_started_get_context (event);

   test->started_calls++;
   assert_service_id (test, mongoc_apm_command_started_get_service_id (event));
}


static void
service_id_cmd_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   service_id_test_t *test = mongoc_apm_command_succeeded_get_context (event);

   test->succeeded_calls++;
   assert_service_id (test, mongoc_apm_command_succeeded_get_service_id (event));
}


static void
service_id_cmd_failed_cb (const mongoc_apm_command_failed_t *event)
{
   service_id_test_t *test = mongoc_apm_command_failed_get_context (event);

   test->failed_calls++;
   assert_service_id (test, mongoc_apm_command_failed_get_service_id (event));
}


static void
_test_service_id (bool is_loadbalanced)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   request_t *request;
   future_t *future;
   bson_error_t error;
   service_id_test_t context = {0};
   mongoc_apm_callbacks_t *callbacks;

   server = mock_server_new ();
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, is_loadbalanced);
   client = test_framework_client_new_from_uri (uri, NULL);

   if (is_loadbalanced) {
      context.has_service_id = true;
      bson_oid_init_from_string (&context.expected_service_id, "AAAAAAAAAAAAAAAAAAAAAAAA");
   }

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, service_id_cmd_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, service_id_cmd_succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, service_id_cmd_failed_cb);
   ASSERT (mongoc_client_set_apm_callbacks (client, callbacks, &context));
   mongoc_apm_callbacks_destroy (callbacks);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);

   if (is_loadbalanced) {
      request = mock_server_receives_any_hello_with_match (server, "{'loadBalanced': true}", "{'loadBalanced': true}");
      reply_to_request_simple (request,
                               tmp_str ("{'ismaster': true,"
                                        " 'minWireVersion': %d,"
                                        " 'maxWireVersion': %d,"
                                        " 'msg': 'isdbgrid',"
                                        " 'serviceId': {'$oid': 'AAAAAAAAAAAAAAAAAAAAAAAA'}}",
                                        WIRE_VERSION_MIN,
                                        WIRE_VERSION_5_0));
   } else {
      request = mock_server_receives_any_hello_with_match (
         server, "{'loadBalanced': { '$exists': false }}", "{'loadBalanced': { '$exists': false }}");
      reply_to_request_simple (request,
                               tmp_str ("{'ismaster': true,"
                                        " 'minWireVersion': %d,"
                                        " 'maxWireVersion': %d,"
                                        " 'msg': 'isdbgrid'}",
                                        WIRE_VERSION_MIN,
                                        WIRE_VERSION_5_0));
   }
   request_destroy (request);

   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   reply_to_request_with_ok_and_destroy (request);

   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   future = future_client_command_simple (
      client, "admin", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL /* reply */, &error);

   request = mock_server_receives_msg (server, 0, tmp_bson ("{'ping': 1}"));
   reply_to_request_simple (request, "{'ok': 0, 'code': 8, 'errmsg': 'UnknownError'}");
   request_destroy (request);

   ASSERT (!future_get_bool (future));
   future_destroy (future);

   ASSERT_CMPINT (2, ==, context.started_calls);
   ASSERT_CMPINT (1, ==, context.succeeded_calls);
   ASSERT_CMPINT (1, ==, context.failed_calls);

   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


void
test_service_id_loadbalanced (void)
{
   _test_service_id (true);
}


void
test_service_id_not_loadbalanced (void)
{
   _test_service_id (false);
}


void
test_command_monitoring_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_AddMockServerTest (suite, "/command_monitoring/get_error", test_get_error);
   TestSuite_AddLive (suite, "/command_monitoring/set_callbacks/single", test_set_callbacks_single);
   TestSuite_AddLive (suite, "/command_monitoring/set_callbacks/pooled", test_set_callbacks_pooled);
   TestSuite_AddLive (suite, "/command_monitoring/set_callbacks/pooled_try_pop", test_set_callbacks_pooled_try_pop);
   /* require aggregation cursor */
   TestSuite_AddLive (suite, "/command_monitoring/set_callbacks/change", test_change_callbacks);
   TestSuite_AddLive (suite, "/command_monitoring/set_callbacks/reset", test_reset_callbacks);
   TestSuite_AddLive (suite, "/command_monitoring/operation_id/bulk/collection/single", test_collection_bulk_op_single);
   TestSuite_AddLive (suite, "/command_monitoring/operation_id/bulk/collection/pooled", test_collection_bulk_op_pooled);
   TestSuite_AddLive (suite, "/command_monitoring/operation_id/bulk/new/single", test_bulk_op_single);
   TestSuite_AddLive (suite, "/command_monitoring/operation_id/bulk/new/pooled", test_bulk_op_pooled);
   TestSuite_AddMockServerTest (
      suite, "/command_monitoring/operation_id/query/single/cmd", test_query_operation_id_single_cmd);
   TestSuite_AddMockServerTest (
      suite, "/command_monitoring/operation_id/query/pooled/cmd", test_query_operation_id_pooled_cmd);
   TestSuite_AddLive (suite, "/command_monitoring/client_cmd", test_client_cmd);
   TestSuite_AddLive (suite, "/command_monitoring/client_cmd_simple", test_client_cmd_simple);
   TestSuite_AddLive (suite, "/command_monitoring/client_cmd/op_ids", test_client_cmd_op_ids);
   TestSuite_AddFull (suite,
                      "/command_monitoring/killcursors_deprecated",
                      test_killcursors_deprecated,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_legacy_opcodes);
   TestSuite_AddMockServerTest (suite, "/command_monitoring/failed_reply_mock", test_command_failed_reply_mock);
   TestSuite_AddMockServerTest (suite, "/command_monitoring/failed_reply_hangup", test_command_failed_reply_hangup);
   TestSuite_AddMockServerTest (suite, "/command_monitoring/service_id/loadbalanced", test_service_id_loadbalanced);
   TestSuite_AddMockServerTest (
      suite, "/command_monitoring/service_id/not_loadbalanced", test_service_id_not_loadbalanced);
}
