#include <mongoc/mongoc.h>

#include "mongoc/mongoc-collection-private.h"

#include "json-test.h"
#include "test-libmongoc.h"
#include "mock_server/mock-rs.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "json-test-operations.h"
#include "test-mongoc-retryability-helpers.h"


static bool
retryable_writes_test_run_operation (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bool *explicit_session = (bool *) ctx->config->ctx;
   bson_t reply;
   bool res;

   res =
      json_test_operation (ctx, test, operation, ctx->collection, *explicit_session ? ctx->sessions[0] : NULL, &reply);

   bson_destroy (&reply);

   return res;
}


static test_skip_t skips[] = {
   {"InsertOne fails after multiple retryable writeConcernErrors", "Waiting on CDRIVER-3790"}, {0}};

/* Callback for JSON tests from Retryable Writes Spec */
static void
test_retryable_writes_cb (bson_t *scenario)
{
   bool explicit_session;
   json_test_config_t config = JSON_TEST_CONFIG_INIT;

   config.skips = skips;

   /* use the context pointer to send "explicit_session" to the callback */
   config.ctx = &explicit_session;
   config.run_operation_cb = retryable_writes_test_run_operation;
   config.scenario = scenario;
   config.command_started_events_only = true;
   explicit_session = true;
   run_json_general_test (&config);
   explicit_session = false;
   run_json_general_test (&config);
}


/* "Replica Set Failover Test" from Retryable Writes Spec */
static void
test_rs_failover (void)
{
   mock_rs_t *rs;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_client_session_t *cs;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;
   bson_t *b = tmp_bson ("{}");

   rs = mock_rs_with_auto_hello (WIRE_VERSION_MAX, true /* has primary */, 2 /* secondaries */, 0 /* arbiters */);

   mock_rs_run (rs);
   uri = mongoc_uri_copy (mock_rs_get_uri (rs));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");
   cs = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (cs, error);
   ASSERT_OR_PRINT (mongoc_client_session_append (cs, &opts, &error), error);

   /* initial insert triggers replica set discovery */
   future = future_collection_insert_one (collection, b, &opts, NULL, &error);
   request = mock_rs_receives_msg (rs, 0, tmp_bson ("{'insert': 'collection'}"), b);
   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));
   future_destroy (future);

   /* failover */
   mock_rs_stepdown (rs);
   mock_rs_elect (rs, 1 /* server id */);

   /* insert receives "not primary" from old primary, reselects and retries */
   future = future_collection_insert_one (collection, tmp_bson ("{}"), &opts, NULL, &error);

   request = mock_rs_receives_msg (rs, 0, tmp_bson ("{'insert': 'collection'}"), b);
   BSON_ASSERT (mock_rs_request_is_to_secondary (rs, request));
   reply_to_request_simple (request,
                            "{"
                            " 'ok': 0,"
                            " 'code': 10107,"
                            " 'errmsg': 'not primary',"
                            " 'errorLabels': ['RetryableWriteError']"
                            "}");
   request_destroy (request);

   request = mock_rs_receives_msg (rs, 0, tmp_bson ("{'insert': 'collection'}"), b);
   BSON_ASSERT (mock_rs_request_is_to_primary (rs, request));
   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));
   future_destroy (future);

   bson_destroy (&opts);
   mongoc_client_session_destroy (cs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_rs_destroy (rs);
}


/* Test code paths for _mongoc_client_command_with_opts.
 * This test requires a 3.6+ replica set to support the
 * onPrimaryTransactionalWrite failpoint. */
static void
test_command_with_opts (void *ctx)
{
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   uint32_t server_id;
   mongoc_collection_t *collection;
   bson_t *cmd;
   bson_t reply;
   bson_t reply_result;
   bson_error_t error;

   BSON_UNUSED (ctx);

   uri = test_framework_get_uri ();
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);

   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_uri_destroy (uri);

   /* clean up in case a previous test aborted */
   server_id = mongoc_topology_select_server_id (client->topology, MONGOC_SS_WRITE, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_id, error);
   deactivate_fail_points (client, server_id);

   collection = get_test_collection (client, "retryable_writes");

   if (!mongoc_collection_drop (collection, &error)) {
      if (NULL == strstr (error.message, "ns not found")) {
         /* an error besides ns not found */
         ASSERT_OR_PRINT (false, error);
      }
   }

   ASSERT_OR_PRINT (mongoc_collection_insert_one (collection, tmp_bson ("{'_id':1, 'x': 1}"), NULL, NULL, &error),
                    error);

   cmd = tmp_bson ("{'configureFailPoint': 'onPrimaryTransactionalWrite',"
                   " 'mode': {'times': 1},"
                   " 'data': {'failBeforeCommitExceptionCode': 1}}");

   ASSERT_OR_PRINT (mongoc_client_command_simple_with_server_id (client, "admin", cmd, NULL, server_id, NULL, &error),
                    error);

   cmd = tmp_bson ("{'findAndModify': '%s', 'query': {'_id': 1}, 'update': "
                   "{'$inc': {'x': 1}}, 'new': true}",
                   collection->collection);

   ASSERT_OR_PRINT (mongoc_collection_read_write_command_with_opts (collection, cmd, NULL, NULL, &reply, &error),
                    error);

   bson_lookup_doc (&reply, "value", &reply_result);
   assert_match_bson (&reply_result, tmp_bson ("{'_id': 1, 'x': 2}"), false);

   deactivate_fail_points (client, server_id);

   ASSERT_OR_PRINT (mongoc_collection_drop (collection, &error), error);

   bson_destroy (&reply_result);
   bson_destroy (&reply);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}


static void
test_insert_one_unacknowledged (void)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_write_concern_set_journal (wc, false);
   mongoc_write_concern_append (wc, &opts);

   future = future_collection_insert_one (collection, tmp_bson ("{}"), &opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       2, /* set moreToCome bit in mongoc_op_msg_flags_t */
                                       tmp_bson ("{'txnNumber': {'$exists': false}, 'lsid': {'$exists': false}}"),
                                       tmp_bson ("{}"));
   ASSERT (future_get_bool (future));
   mock_server_auto_endsessions (server);

   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
   request_destroy (request);
   bson_destroy (&opts);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_update_one_unacknowledged (void)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_write_concern_set_journal (wc, false);
   mongoc_write_concern_append (wc, &opts);

   future =
      future_collection_update_one (collection, tmp_bson ("{}"), tmp_bson ("{'$set': {'x': 1}}"), &opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       2, /* set moreToCome bit in mongoc_op_msg_flags_t */
                                       tmp_bson ("{'txnNumber': {'$exists': false}, 'lsid': {'$exists': false}}"),
                                       tmp_bson ("{'q': {}, 'u': {'$set': {'x': 1}}}"));
   ASSERT (future_get_bool (future));

   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
   request_destroy (request);
   bson_destroy (&opts);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_delete_one_unacknowledged (void)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_write_concern_set_journal (wc, false);
   mongoc_write_concern_append (wc, &opts);

   future = future_collection_delete_one (collection, tmp_bson ("{}"), &opts, NULL, &error);

   request = mock_server_receives_msg (server,
                                       2, /* set moreToCome bit in mongoc_op_msg_flags_t */
                                       tmp_bson ("{'txnNumber': {'$exists': false}, 'lsid': {'$exists': false}}"),
                                       tmp_bson ("{'q': {}, 'limit': 1}"));
   ASSERT (future_get_bool (future));

   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
   request_destroy (request);
   bson_destroy (&opts);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_bulk_operation_execute_unacknowledged (void)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   mongoc_bulk_operation_t *bulk;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_write_concern_set_journal (wc, false);
   mongoc_write_concern_append (wc, &opts);

   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, &opts);
   mongoc_bulk_operation_insert (bulk, tmp_bson ("{'_id': 1}"));
   future = future_bulk_operation_execute (bulk, NULL, &error);

   request = mock_server_receives_msg (server,
                                       2, /* set moreToCome bit in mongoc_op_msg_flags_t */
                                       tmp_bson ("{'txnNumber': {'$exists': false}, 'lsid': {'$exists': false}}"),
                                       tmp_bson ("{'_id': 1}"));
   ASSERT (future_get_uint32_t (future) == 1);

   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
   mongoc_bulk_operation_destroy (bulk);
   request_destroy (request);
   bson_destroy (&opts);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_remove_unacknowledged (void)
{
   mongoc_uri_t *uri;
   mock_server_t *server;
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   bson_t opts = BSON_INITIALIZER;
   future_t *future;
   request_t *request;
   bson_error_t error;

   server = mock_mongos_new (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_write_concern_set_journal (wc, false);

   future = future_collection_remove (collection, MONGOC_REMOVE_NONE, tmp_bson ("{'a': 1}"), wc, &error);

   request = mock_server_receives_msg (server,
                                       2, /* set moreToCome bit in mongoc_op_msg_flags_t */
                                       tmp_bson ("{'txnNumber': {'$exists': false}, 'lsid': {'$exists': false}}"),
                                       tmp_bson ("{'q': {'a': 1}, 'limit': 0}"));
   ASSERT (future_get_bool (future));

   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
   request_destroy (request);
   bson_destroy (&opts);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_retry_no_crypto (void *ctx)
{
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;

   BSON_UNUSED (ctx);

   capture_logs (true);

   /* Test that no warning is logged if retryWrites is disabled. Warning logic
    * is implemented in mongoc_topology_new, but test all public APIs that use
    * the common code path. */
   client = test_framework_client_new ("mongodb://localhost/?retryWrites=false", NULL);
   BSON_ASSERT (client);
   ASSERT_NO_CAPTURED_LOGS ("test_framework_client_new and retryWrites=false");
   mongoc_client_destroy (client);

   uri = mongoc_uri_new ("mongodb://localhost/?retryWrites=false");
   BSON_ASSERT (uri);

   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);
   ASSERT_NO_CAPTURED_LOGS ("test_framework_client_new_from_uri and retryWrites=false");
   mongoc_client_destroy (client);

   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   BSON_ASSERT (pool);
   ASSERT_NO_CAPTURED_LOGS ("test_framework_client_pool_new_from_uri and retryWrites=false");
   mongoc_client_pool_destroy (pool);

   mongoc_uri_destroy (uri);

   /* Test that a warning is logged if retryWrites is enabled. */
   client = test_framework_client_new ("mongodb://localhost/?retryWrites=true", NULL);
   BSON_ASSERT (client);
   ASSERT_CAPTURED_LOG ("test_framework_client_new and retryWrites=true",
                        MONGOC_LOG_LEVEL_WARNING,
                        "retryWrites not supported without an SSL crypto library");
   mongoc_client_destroy (client);

   clear_captured_logs ();

   uri = mongoc_uri_new ("mongodb://localhost/?retryWrites=true");
   BSON_ASSERT (uri);

   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);
   ASSERT_CAPTURED_LOG ("test_framework_client_new_from_uri and retryWrites=true",
                        MONGOC_LOG_LEVEL_WARNING,
                        "retryWrites not supported without an SSL crypto library");
   mongoc_client_destroy (client);

   clear_captured_logs ();

   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   BSON_ASSERT (pool);
   ASSERT_CAPTURED_LOG ("test_framework_client_pool_new_from_uri and retryWrites=true",
                        MONGOC_LOG_LEVEL_WARNING,
                        "retryWrites not supported without an SSL crypto library");
   mongoc_client_pool_destroy (pool);

   mongoc_uri_destroy (uri);
}

static void
test_unsupported_storage_engine_error (void)
{
   mock_rs_t *rs;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_t reply;
   bson_error_t error;
   future_t *future;
   request_t *request;
   mongoc_client_session_t *session;
   bson_t opts;
   const char *expected_msg = "This MongoDB deployment does not support "
                              "retryable writes. Please add retryWrites=false "
                              "to your connection string.";

   rs = mock_rs_with_auto_hello (WIRE_VERSION_MAX, true, 0, 0);
   mock_rs_run (rs);
   uri = mongoc_uri_copy (mock_rs_get_uri (rs));
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, true);
   client = test_framework_client_new_from_uri (uri, NULL);
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "test", "test");
   bson_init (&opts);
   ASSERT_OR_PRINT (mongoc_client_session_append (session, &opts, &error), error);
   /* findandmodify is retryable through mongoc_client_write_command_with_opts.
    */
   future = future_client_write_command_with_opts (
      client, "test", tmp_bson ("{'findandmodify': 'coll' }"), &opts, &reply, &error);
   request = mock_rs_receives_request (rs);
   reply_to_request_simple (request, "{'ok': 0, 'code': 20, 'errmsg': 'Transaction numbers are great'}");
   request_destroy (request);

   BSON_ASSERT (!future_get_bool (future));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 20, expected_msg);
   ASSERT_MATCH (&reply, "{'code': 20, 'errmsg': '%s'}", expected_msg);

   bson_destroy (&opts);
   mongoc_client_session_destroy (session);
   bson_destroy (&reply);
   future_destroy (future);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_rs_destroy (rs);
}

/* The following 3 tests check that the original reply and error is returned
 * after encountering an error with a RetryableWriteError and a
 * NoWritesPerformed label. The tests use the same callback function.
 *
 * Each test checks a different code path for retryable writes:
 * mongoc_collection_insert_one, mongoc_client_command_simple, and
 * mongoc_collection_find_and_modify_with_opts
 *
 * These tests require a >=6.0 replica set.
 */
typedef struct {
   mongoc_client_t *client;
   bool configure_second_fail;
   char *failCommand;
} prose_test_3_apm_ctx_t;

static void
prose_test_3_on_command_success (const mongoc_apm_command_succeeded_t *event)
{
   bson_iter_t iter;
   bson_error_t error;
   const bson_t *reply = mongoc_apm_command_succeeded_get_reply (event);
   prose_test_3_apm_ctx_t *ctx = mongoc_apm_command_succeeded_get_context (event);

   // wait for a writeConcernError and then set a second failpoint
   if (bson_iter_init_find (&iter, reply, "writeConcernError") && ctx->configure_second_fail) {
      ctx->configure_second_fail = false;
      ASSERT_OR_PRINT (
         mongoc_client_command_simple (ctx->client,
                                       "admin",
                                       tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': {'times': 1},"
                                                 " 'data': { 'failCommands': ['%s'], 'errorCode': "
                                                 "10107, "
                                                 "'errorLabels': ['RetryableWriteError', 'NoWritesPerformed']}}",
                                                 ctx->failCommand),
                                       NULL,
                                       NULL,
                                       &error),
         error);
   }
}

static uint32_t
set_up_original_error_test (mongoc_apm_callbacks_t *callbacks,
                            prose_test_3_apm_ctx_t *apm_ctx,
                            char *failCommand,
                            mongoc_client_t *client)
{
   uint32_t server_id;
   bson_error_t error;

   ASSERT (client);

   // clean up in case a previous test aborted
   server_id = mongoc_topology_select_server_id (client->topology, MONGOC_SS_WRITE, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (server_id, error);
   deactivate_fail_points (client, server_id);

   // set up callbacks for command monitoring
   apm_ctx->client = client;
   apm_ctx->failCommand = failCommand;
   apm_ctx->configure_second_fail = true;
   mongoc_apm_set_command_succeeded_cb (callbacks, prose_test_3_on_command_success);
   mongoc_client_set_apm_callbacks (client, callbacks, apm_ctx);

   // configure the first fail point
   bool ret = mongoc_client_command_simple (client,
                                            "admin",
                                            tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': {'times': 1}, "
                                                      "'data': {'failCommands': ['%s'], 'errorLabels': "
                                                      "['RetryableWriteError'], 'writeConcernError': {'code': 91 }}}",
                                                      failCommand),
                                            NULL,
                                            NULL,
                                            &error);
   ASSERT_OR_PRINT (ret, error);

   return server_id;
}

static void
cleanup_original_error_test (mongoc_client_t *client,
                             uint32_t server_id,
                             bson_t *reply,
                             mongoc_collection_t *coll,
                             mongoc_apm_callbacks_t *callbacks)
{
   ASSERT (client);

   deactivate_fail_points (client, server_id); // disable the fail point
   bson_destroy (reply);
   mongoc_collection_destroy (coll);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_destroy (client);
}

static void
retryable_writes_prose_test_3 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_t reply;
   bson_error_t error = {0};
   mongoc_apm_callbacks_t *callbacks = {0};
   prose_test_3_apm_ctx_t apm_ctx = {0};

   BSON_UNUSED (ctx);

   // setting up the client
   client = test_framework_new_default_client ();
   coll = get_test_collection (client, "retryable_writes");
   callbacks = mongoc_apm_callbacks_new ();

   // setup test
   const uint32_t server_id = set_up_original_error_test (callbacks, &apm_ctx, "insert", client);

   // attempt an insertOne operation
   ASSERT (!mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL /* opts */, &reply, &error));

   // writeConcernErrors are returned in the reply and not as an error
   ASSERT_ERROR_CONTAINS (error, 0, 0, "");

   // the reply holds the original error information
   ASSERT_MATCH (&reply,
                 "{'insertedCount': 1, 'writeConcernErrors': [{ 'code': 91 }], "
                 "'errorLabels': ['RetryableWriteError']}");

   cleanup_original_error_test (client, server_id, &reply, coll, callbacks);
}


static void
retryable_writes_original_error_find_modify (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_t reply;
   bson_error_t error = {0};
   mongoc_apm_callbacks_t *callbacks = {0};
   prose_test_3_apm_ctx_t apm_ctx = {0};
   mongoc_find_and_modify_opts_t *opts;

   BSON_UNUSED (ctx);

   // setting up the client
   client = test_framework_new_default_client ();
   coll = get_test_collection (client, "retryable_writes");
   callbacks = mongoc_apm_callbacks_new ();

   // setup the test
   const uint32_t server_id = set_up_original_error_test (callbacks, &apm_ctx, "findAndModify", client);

   // setup for findAndModify
   bson_t query = BSON_INITIALIZER;
   BSON_APPEND_UTF8 (&query, "x", "1");
   bson_t *update = BCON_NEW ("$inc", "{", "x", BCON_INT32 (1), "}");
   opts = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_set_update (opts, update);

   // attempt a findAndModify operation
   ASSERT (!mongoc_collection_find_and_modify_with_opts (coll, &query, opts, &reply, &error));

   // assert error contains a writeConcernError with original error code
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_WRITE_CONCERN, 91, "");

   // the reply holds the original error information
   ASSERT_MATCH (&reply,
                 "{'lastErrorObject' : { 'n': 0, 'updatedExisting' : false }, 'value' : "
                 "null, 'writeConcernError' : { 'code': 91 }, 'errorLabels' : [ "
                 "'RetryableWriteError' ], 'ok' : 1.0}");

   cleanup_original_error_test (client, server_id, &reply, coll, callbacks);
   mongoc_find_and_modify_opts_destroy (opts);
   bson_destroy (&query);
   bson_destroy (update);
}

static void
retryable_writes_original_error_general_command (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_t reply;
   bson_error_t error = {0};
   mongoc_apm_callbacks_t *callbacks = {0};
   prose_test_3_apm_ctx_t apm_ctx = {0};

   BSON_UNUSED (ctx);

   // setting up the client
   client = test_framework_new_default_client ();
   coll = get_test_collection (client, "retryable_writes");
   callbacks = mongoc_apm_callbacks_new ();

   // setup test
   const uint32_t server_id = set_up_original_error_test (callbacks, &apm_ctx, "insert", client);

   bson_t *cmd = BCON_NEW ("insert", mongoc_collection_get_name (coll), "documents", "[", "{", "}", "]");

   // attempt an insert operation
   ASSERT (!mongoc_client_write_command_with_opts (client, "test", cmd, NULL, &reply, &error));

   // assert error contains a writeConcernError with original error code
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_WRITE_CONCERN, 91, "");

   // the reply holds the original error information
   ASSERT_MATCH (&reply,
                 "{'writeConcernError' : { 'code' : { '$numberInt' : '91' } }, "
                 "'errorLabels' : [ 'RetryableWriteError' ], 'ok': { "
                 "'$numberDouble' : '1.0' }}");

   cleanup_original_error_test (client, server_id, &reply, coll, callbacks);
   bson_destroy (cmd);
}

/*
 *-----------------------------------------------------------------------
 *
 * Runner for the JSON tests for retryable writes.
 *
 *-----------------------------------------------------------------------
 */
static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "retryable_writes/legacy",
                                       test_retryable_writes_cb,
                                       TestSuite_CheckLive,
                                       test_framework_skip_if_no_crypto,
                                       test_framework_skip_if_slow);
}


typedef struct {
   int num_inserts;
   int num_updates;
} _tracks_new_server_counters_t;

static void
_tracks_new_server_cb (const mongoc_apm_command_started_t *event)
{
   const char *cmd_name;
   _tracks_new_server_counters_t *counters;

   cmd_name = mongoc_apm_command_started_get_command_name (event);
   counters = (_tracks_new_server_counters_t *) mongoc_apm_command_started_get_context (event);

   if (0 == strcmp (cmd_name, "insert")) {
      counters->num_inserts++;
   } else if (0 == strcmp (cmd_name, "update")) {
      counters->num_updates++;
   }
}

/* Tests that when a command within a bulk write succeeds after a retryable
 * error, and selects a new server, it continues to use that server in
 * subsequent commands.
 * This test requires running against a replica set with at least one
 * secondary.
 */
static void
test_bulk_retry_tracks_new_server (void *unused)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   mongoc_read_prefs_t *read_prefs;
   bool ret;
   mongoc_server_description_t *sd;
   mongoc_apm_callbacks_t *callbacks;
   _tracks_new_server_counters_t counters = {0};

   BSON_UNUSED (unused);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, _tracks_new_server_cb);

   client = test_framework_new_default_client ();
   mongoc_client_set_apm_callbacks (client, callbacks, &counters);
   collection = get_test_collection (client, "tracks_new_server");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

   /* The bulk write contains two operations, an insert, followed by an
    * update.
    */
   ret = mongoc_bulk_operation_insert_with_opts (bulk, tmp_bson ("{'x': 1}"), NULL /* opts */, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_bulk_operation_update_one (bulk, tmp_bson ("{}"), tmp_bson ("{'$inc': {'x': 1}}"), false /* upsert */);

   /* Explicitly tell the bulk write to use a secondary. That will result in
    * a retryable error, causing the first command to be sent twice. */
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   sd = mongoc_client_select_server (client, false /* for_writes */, read_prefs, &error);
   ASSERT_OR_PRINT (sd, error);
   mongoc_bulk_operation_set_hint (bulk, mongoc_server_description_id (sd));
   ret = mongoc_bulk_operation_execute (bulk, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* The first insert fails with a retryable write error since it is sent to
    * a secondary. The retry selects the primary and succeeds. The second
    * command should use the newly selected server, so the update succeeds on
    * the first try. */
   ASSERT_CMPINT (counters.num_inserts, ==, 2);
   ASSERT_CMPINT (counters.num_updates, ==, 1);
   ASSERT_CMPINT (mongoc_bulk_operation_get_hint (bulk), !=, mongoc_server_description_id (sd));

   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_server_description_destroy (sd);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}

typedef struct _test_retry_writes_sharded_on_other_mongos_ctx {
   int count;
   uint16_t ports[2];
} test_retry_writes_sharded_on_other_mongos_ctx;

static void
_test_retry_writes_sharded_on_other_mongos_cb (const mongoc_apm_command_failed_t *event)
{
   BSON_ASSERT_PARAM (event);

   test_retry_writes_sharded_on_other_mongos_ctx *const ctx =
      (test_retry_writes_sharded_on_other_mongos_ctx *) mongoc_apm_command_failed_get_context (event);
   BSON_ASSERT (ctx);

   ASSERT_WITH_MSG (ctx->count < 2, "expected at most two failpoints to trigger");

   const mongoc_host_list_t *const host = mongoc_apm_command_failed_get_host (event);
   BSON_ASSERT (host);
   BSON_ASSERT (!host->next);
   ctx->ports[ctx->count++] = host->port;
}

// Test that in a sharded cluster writes are retried on a different mongos when
// one is available.
static void
retryable_writes_sharded_on_other_mongos (void *_ctx)
{
   BSON_UNUSED (_ctx);

   bson_error_t error = {0};

   // Create two clients `s0` and `s1` that each connect to a single mongos from
   // the sharded cluster. They must not connect to the same mongos.
   const char *ports[] = {"27017", "27018"};
   const size_t num_ports = sizeof (ports) / sizeof (*ports);
   mongoc_array_t clients = _test_get_mongos_clients (ports, num_ports);
   BSON_ASSERT (clients.len == 2u);
   mongoc_client_t *const s0 = _mongoc_array_index (&clients, mongoc_client_t *, 0u);
   mongoc_client_t *const s1 = _mongoc_array_index (&clients, mongoc_client_t *, 1u);
   BSON_ASSERT (s0 && s1);

   // Deprioritization cannot be deterministically asserted by this test due to
   // randomized selection from suitable servers. Repeat the test a few times to
   // increase the likelihood of detecting incorrect deprioritization behavior.
   for (int i = 0; i < 10; ++i) {
      // Configure the following fail point for both `s0` and `s1`:
      {
         bson_t *const command = tmp_bson ("{"
                                           "  'configureFailPoint': 'failCommand',"
                                           "  'mode': { 'times': 1 },"
                                           "  'data': {"
                                           "    'failCommands': ['insert'],"
                                           "    'errorCode': 6,"
                                           "    'errorLabels': ['RetryableWriteError']"
                                           "  }"
                                           "}");

         ASSERT_OR_PRINT (mongoc_client_command_simple (s0, "admin", command, NULL, NULL, &error), error);
         ASSERT_OR_PRINT (mongoc_client_command_simple (s1, "admin", command, NULL, NULL, &error), error);
      }

      // Create a client `client` with `retryWrites=true` that connects to the
      // cluster with both mongoses used by `s0` and `s1` in the initial seed
      // list.
      mongoc_client_t *client = NULL;
      {
         const char *const host_and_port = "mongodb://localhost:27017,localhost:27018/?retryWrites=true";
         char *const uri_str = test_framework_add_user_password_from_env (host_and_port);
         mongoc_uri_t *const uri = mongoc_uri_new (uri_str);

         client = mongoc_client_new_from_uri_with_error (uri, &error);
         ASSERT_OR_PRINT (client, error);
         test_framework_set_ssl_opts (client);

         mongoc_uri_destroy (uri);
         bson_free (uri_str);
      }
      BSON_ASSERT (client);

      {
         test_retry_writes_sharded_on_other_mongos_ctx ctx = {0};

         // Enable failed command event monitoring for `client`.
         {
            mongoc_apm_callbacks_t *const callbacks = mongoc_apm_callbacks_new ();
            mongoc_apm_set_command_failed_cb (callbacks, _test_retry_writes_sharded_on_other_mongos_cb);
            mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
            mongoc_apm_callbacks_destroy (callbacks);
         }

         // Execute an `insert` command with `client`. Assert that the command
         // failed.
         {
            mongoc_database_t *const db = mongoc_client_get_database (client, "db");
            mongoc_collection_t *const coll = mongoc_database_get_collection (db, "test");
            ASSERT_WITH_MSG (!mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL, NULL, &error),
                             "expected insert command to fail");
            MONGOC_DEBUG ("insert error: %s", error.message);
            mongoc_collection_destroy (coll);
            mongoc_database_destroy (db);
         }

         // Assert that two failed command events occurred.
         ASSERT_WITH_MSG (ctx.count == 2,
                          "expected exactly 2 failpoints to trigger, but "
                          "observed %d with error: %s",
                          ctx.count,
                          error.message);

         // Assert that the failed command events occurred on different
         // mongoses.
         ASSERT_WITH_MSG ((ctx.ports[0] == 27017 || ctx.ports[0] == 27018) &&
                             (ctx.ports[1] == 27017 || ctx.ports[1] == 27018) && (ctx.ports[0] != ctx.ports[1]),
                          "expected failpoints to trigger once on each mongos, "
                          "but observed failures on %d and %d",
                          ctx.ports[0],
                          ctx.ports[1]);

         mongoc_client_destroy (client);
      }

      // Disable the fail points.
      {
         bson_t *const command = tmp_bson ("{"
                                           "  'configureFailPoint': 'failCommand',"
                                           "  'mode': 'off'"
                                           "}");

         ASSERT_OR_PRINT (mongoc_client_command_simple (s0, "admin", command, NULL, NULL, &error), error);
         ASSERT_OR_PRINT (mongoc_client_command_simple (s1, "admin", command, NULL, NULL, &error), error);
      }
   }

   mongoc_client_destroy (s0);
   mongoc_client_destroy (s1);
   _mongoc_array_destroy (&clients);
}

typedef struct _test_retry_writes_sharded_on_same_mongos_ctx {
   int failed_count;
   int succeeded_count;
   uint16_t failed_port;
   uint16_t succeeded_port;
} test_retry_writes_sharded_on_same_mongos_ctx;

static void
_test_retry_writes_sharded_on_same_mongos_cb (test_retry_writes_sharded_on_same_mongos_ctx *ctx,
                                              const mongoc_apm_command_failed_t *failed,
                                              const mongoc_apm_command_succeeded_t *succeeded)
{
   BSON_ASSERT_PARAM (ctx);
   BSON_ASSERT (failed || true);
   BSON_ASSERT (succeeded || true);

   ASSERT_WITH_MSG (ctx->failed_count + ctx->succeeded_count < 2,
                    "expected at most two events, but observed %d failed and %d succeeded",
                    ctx->failed_count,
                    ctx->succeeded_count);

   if (failed) {
      ctx->failed_count += 1;

      const mongoc_host_list_t *const host = mongoc_apm_command_failed_get_host (failed);
      BSON_ASSERT (host);
      BSON_ASSERT (!host->next);
      ctx->failed_port = host->port;
   }

   if (succeeded) {
      ctx->succeeded_count += 1;

      const mongoc_host_list_t *const host = mongoc_apm_command_succeeded_get_host (succeeded);
      BSON_ASSERT (host);
      BSON_ASSERT (!host->next);
      ctx->succeeded_port = host->port;
   }
}

static void
_test_retry_writes_sharded_on_same_mongos_failed_cb (const mongoc_apm_command_failed_t *event)
{
   _test_retry_writes_sharded_on_same_mongos_cb (mongoc_apm_command_failed_get_context (event), event, NULL);
}

static void
_test_retry_writes_sharded_on_same_mongos_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   _test_retry_writes_sharded_on_same_mongos_cb (mongoc_apm_command_succeeded_get_context (event), NULL, event);
}

// Test that in a sharded cluster writes are retried on the same mongos when no
// others are available.
static void
retryable_writes_sharded_on_same_mongos (void *_ctx)
{
   BSON_UNUSED (_ctx);

   bson_error_t error = {0};

   // Create a client `s0` that connects to a single mongos from the cluster.
   const char *ports[] = {"27017"};
   const size_t num_ports = sizeof (ports) / sizeof (*ports);
   mongoc_array_t clients = _test_get_mongos_clients (ports, num_ports);
   BSON_ASSERT (clients.len == 1u);
   mongoc_client_t *const s0 = _mongoc_array_index (&clients, mongoc_client_t *, 0u);
   BSON_ASSERT (s0);

   // Configure the following fail point for `s0`:
   ASSERT_OR_PRINT (mongoc_client_command_simple (s0,
                                                  "admin",
                                                  tmp_bson ("{"
                                                            "  'configureFailPoint': 'failCommand',"
                                                            "  'mode': { 'times': 1 },"
                                                            "  'data': {"
                                                            "    'failCommands': ['insert'],"
                                                            "    'errorCode': 6,"
                                                            "    'errorLabels': ['RetryableWriteError']"
                                                            "  }"
                                                            "}"),
                                                  NULL,
                                                  NULL,
                                                  &error),
                    error);

   // Create a client client with `directConnection=false` (when not set by
   // default) and `retryWrites=true` that connects to the cluster using the
   // same single mongos as `s0`.
   mongoc_client_t *client = NULL;
   {
      const char *const host_and_port = "mongodb://localhost:27017/"
                                        "?retryWrites=true&directConnection=false";
      char *const uri_str = test_framework_add_user_password_from_env (host_and_port);
      mongoc_uri_t *const uri = mongoc_uri_new (uri_str);

      client = mongoc_client_new_from_uri_with_error (uri, &error);
      ASSERT_OR_PRINT (client, error);
      test_framework_set_ssl_opts (client);

      mongoc_uri_destroy (uri);
      bson_free (uri_str);
   }
   BSON_ASSERT (client);

   {
      test_retry_writes_sharded_on_same_mongos_ctx ctx = {
         .failed_count = 0,
         .succeeded_count = 0,
      };

      // Enable succeeded and failed command event monitoring for `client`.
      {
         mongoc_apm_callbacks_t *const callbacks = mongoc_apm_callbacks_new ();
         mongoc_apm_set_command_failed_cb (callbacks, _test_retry_writes_sharded_on_same_mongos_failed_cb);
         mongoc_apm_set_command_succeeded_cb (callbacks, _test_retry_writes_sharded_on_same_mongos_succeeded_cb);
         mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
         mongoc_apm_callbacks_destroy (callbacks);
      }

      // Execute an `insert` command with `client`. Assert that the command
      // succeeded.
      {
         mongoc_database_t *const db = mongoc_client_get_database (client, "db");
         mongoc_collection_t *const coll = mongoc_database_get_collection (db, "test");
         ASSERT_WITH_MSG (mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL, NULL, &error),
                          "expecting insert to succeed, but observed error: %s",
                          error.message);
         mongoc_collection_destroy (coll);
         mongoc_database_destroy (db);
      }

      // Avoid capturing additional events.
      mongoc_client_set_apm_callbacks (client, NULL, NULL);

      // Assert that exactly one failed command event and one succeeded
      // command event occurred.
      ASSERT_WITH_MSG (ctx.failed_count == 1 && ctx.succeeded_count == 1,
                       "expected exactly one failed event and one succeeded "
                       "event, but observed %d failures and %d successes with error: %s",
                       ctx.failed_count,
                       ctx.succeeded_count,
                       ctx.succeeded_count > 1 ? "none" : error.message);

      // Assert that both events occurred on the same mongos.
      ASSERT_WITH_MSG (ctx.failed_port == ctx.succeeded_port,
                       "expected failed and succeeded events on the same mongos, but "
                       "instead observed port %d (failed) and port %d (succeeded)",
                       ctx.failed_port,
                       ctx.succeeded_port);

      mongoc_client_destroy (client);
   }

   // Disable the fail point.
   ASSERT_OR_PRINT (mongoc_client_command_simple (s0,
                                                  "admin",
                                                  tmp_bson ("{"
                                                            "  'configureFailPoint': 'failCommand',"
                                                            "  'mode': 'off'"
                                                            "}"),
                                                  NULL,
                                                  NULL,
                                                  &error),
                    error);

   mongoc_client_destroy (s0);
   _mongoc_array_destroy (&clients);
}


void
test_retryable_writes_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_AddMockServerTest (
      suite, "/retryable_writes/failover", test_rs_failover, test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/command_with_opts",
                      test_command_with_opts,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);
   TestSuite_AddMockServerTest (suite,
                                "/retryable_writes/insert_one_unacknowledged",
                                test_insert_one_unacknowledged,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (suite,
                                "/retryable_writes/update_one_unacknowledged",
                                test_update_one_unacknowledged,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (suite,
                                "/retryable_writes/delete_one_unacknowledged",
                                test_delete_one_unacknowledged,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (
      suite, "/retryable_writes/remove_unacknowledged", test_remove_unacknowledged, test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (suite,
                                "/retryable_writes/bulk_operation_execute_unacknowledged",
                                test_bulk_operation_execute_unacknowledged,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddFull (
      suite, "/retryable_writes/no_crypto", test_retry_no_crypto, NULL, NULL, test_framework_skip_if_crypto);
   TestSuite_AddMockServerTest (suite,
                                "/retryable_writes/unsupported_storage_engine_error",
                                test_unsupported_storage_engine_error,
                                test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/bulk_tracks_new_server",
                      test_bulk_retry_tracks_new_server,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/prose_test_3",
                      retryable_writes_prose_test_3,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_max_wire_version_less_than_17,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/prose_test_3/find_modify",
                      retryable_writes_original_error_find_modify,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_max_wire_version_less_than_17,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/prose_test_3/general_command",
                      retryable_writes_original_error_general_command,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_max_wire_version_less_than_17,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/prose_test_4",
                      retryable_writes_sharded_on_other_mongos,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_mongos,
                      test_framework_skip_if_no_failpoint,
                      // `errorLabels` is a 4.3.1+ feature.
                      test_framework_skip_if_max_wire_version_less_than_9,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/retryable_writes/prose_test_5",
                      retryable_writes_sharded_on_same_mongos,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_mongos,
                      test_framework_skip_if_no_failpoint,
                      // `errorLabels` is a 4.3.1+ feature.
                      test_framework_skip_if_max_wire_version_less_than_9,
                      test_framework_skip_if_no_crypto);
}
