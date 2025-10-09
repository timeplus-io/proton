#include <mongoc/mongoc.h>

#include "json-test.h"
#include "json-test-operations.h"
#include "test-libmongoc.h"

static bool
crud_test_operation_cb (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bson_t reply;
   bool res;

   res = json_test_operation (ctx, test, operation, ctx->collection, NULL, &reply);

   bson_destroy (&reply);

   return res;
}

static void
test_crud_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;
   config.run_operation_cb = crud_test_operation_cb;
   config.command_started_events_only = true;
   config.scenario = scenario;
   run_json_general_test (&config);
}

static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite_with_check (
      suite, JSON_DIR, "crud/legacy", &test_crud_cb, test_framework_skip_if_no_crypto, TestSuite_CheckLive);

   /* Read/write concern spec tests use the same format. */
   install_json_test_suite_with_check (
      suite, JSON_DIR, "read_write_concern/operation", &test_crud_cb, TestSuite_CheckLive);
}

static void
prose_test_1 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bool ret;
   bson_t reply;
   bson_error_t error;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   coll = get_test_collection (client, "coll");

   ret = mongoc_client_command_simple (client,
                                       "admin",
                                       tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': {'times': 1}, "
                                                 " 'data': {'failCommands': ['insert'], 'writeConcernError': {"
                                                 "   'code': 100, 'codeName': 'UnsatisfiableWriteConcern', "
                                                 "   'errmsg': 'Not enough data-bearing nodes', "
                                                 "   'errInfo': {'writeConcern': {'w': 2, 'wtimeout': 0, "
                                                 "               'provenance': 'clientSupplied'}}}}}"),
                                       NULL,
                                       NULL,
                                       &error);
   ASSERT_OR_PRINT (ret, error);

   ret = mongoc_collection_insert_one (coll, tmp_bson ("{'x':1}"), NULL /* opts */, &reply, &error);
   ASSERT (!ret);

   /* libmongoc does not model WriteConcernError, so we only assert that the
    * "errInfo" field set in configureFailPoint matches that in the result */
   ASSERT_MATCH (&reply,
                 "{'writeConcernErrors': [{'errInfo': {'writeConcern': {"
                 "'w': 2, 'wtimeout': 0, 'provenance': 'clientSupplied'}}}]}");

   bson_destroy (&reply);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

typedef struct {
   bool has_reply;
   bson_t reply;
} prose_test_2_apm_ctx_t;

static void
prose_test_2_command_succeeded (const mongoc_apm_command_succeeded_t *event)
{
   if (!strcmp (mongoc_apm_command_succeeded_get_command_name (event), "insert")) {
      prose_test_2_apm_ctx_t *ctx = mongoc_apm_command_succeeded_get_context (event);
      ASSERT (!ctx->has_reply);
      ctx->has_reply = true;
      bson_copy_to (mongoc_apm_command_succeeded_get_reply (event), &ctx->reply);
   }
}

static void
prose_test_2 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *coll, *coll_created;
   mongoc_apm_callbacks_t *callbacks;
   prose_test_2_apm_ctx_t apm_ctx = {0};
   bool ret;
   bson_t reply, reply_errInfo, observed_errInfo;
   bson_error_t error = {0};

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   db = get_test_database (client);
   coll = get_test_collection (client, "coll");

   /* don't care if ns not found. */
   (void) mongoc_collection_drop (coll, NULL);

   coll_created = mongoc_database_create_collection (
      db, mongoc_collection_get_name (coll), tmp_bson ("{'validator': {'x': {'$type': 'string'}}}"), &error);
   ASSERT_OR_PRINT (coll_created, error);
   mongoc_collection_destroy (coll_created);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_succeeded_cb (callbacks, prose_test_2_command_succeeded);
   mongoc_client_set_apm_callbacks (client, callbacks, (void *) &apm_ctx);
   mongoc_apm_callbacks_destroy (callbacks);

   ret = mongoc_collection_insert_one (coll, tmp_bson ("{'x':1}"), NULL /* opts */, &reply, &error);
   ASSERT (!ret);

   /* Assert that the WriteError's code is DocumentValidationFailure */
   ASSERT_MATCH (&reply, "{'writeErrors': [{'code': 121}]}");

   /* libmongoc does not model WriteError, so we only assert that the observed
    * "errInfo" field matches that in the result */
   ASSERT (apm_ctx.has_reply);
   bson_lookup_doc (&apm_ctx.reply, "writeErrors.0.errInfo", &observed_errInfo);
   bson_lookup_doc (&reply, "writeErrors.0.errInfo", &reply_errInfo);
   ASSERT (bson_compare (&reply_errInfo, &observed_errInfo) == 0);

   bson_destroy (&apm_ctx.reply);
   bson_destroy (&reply);
   mongoc_collection_destroy (coll);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

void
test_crud_install (TestSuite *suite)
{
   test_all_spec_tests (suite);

   TestSuite_AddFull (suite,
                      "/crud/prose_test_1",
                      prose_test_1,
                      NULL, /* dtor */
                      NULL, /* ctx */
                      test_framework_skip_if_no_failpoint,
                      test_framework_skip_if_max_wire_version_less_than_7);

   TestSuite_AddFull (suite,
                      "/crud/prose_test_2",
                      prose_test_2,
                      NULL, /* dtor */
                      NULL, /* ctx */
                      test_framework_skip_if_max_wire_version_less_than_13);
}
