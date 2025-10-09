/*
 * Copyright 2018-present MongoDB, Inc.
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


#include "bson/bson.h"

#include "mongoc/mongoc-change-stream-private.h"
#include "mongoc/mongoc-collection-private.h"
#include "mongoc/mongoc-config.h"
#include "mongoc/mongoc-cursor-private.h"
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-server-description-private.h"
#include "mongoc/mongoc-topology-description-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-util-private.h"

#include "json-test-operations.h"
#include "json-test.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"


mongoc_client_session_t *
session_from_name (json_test_ctx_t *ctx, const char *session_name)
{
   BSON_ASSERT (session_name);

   if (!strcmp (session_name, "session0")) {
      return ctx->sessions[0];
   } else if (!strcmp (session_name, "session1")) {
      return ctx->sessions[1];
   } else {
      test_error ("Unrecognized session name: %s", session_name);
   }
}

static json_test_worker_thread_t *
thread_from_name (json_test_ctx_t *ctx, const char *thread_name)
{
   json_test_worker_thread_t *wt = NULL;
   BSON_ASSERT (thread_name);

   if (!strcmp (thread_name, "thread1")) {
      wt = ctx->worker_threads[0];
   } else if (!strcmp (thread_name, "thread2")) {
      wt = ctx->worker_threads[1];
   } else {
      test_error ("Unrecognized thread name: %s", thread_name);
   }
   if (!wt) {
      test_error ("Test runner did not create worker threads");
   }
   return wt;
}

json_test_worker_thread_t *
worker_thread_new (json_test_ctx_t *ctx)
{
   json_test_worker_thread_t *wt;

   wt = bson_malloc0 (sizeof (json_test_worker_thread_t));
   wt->ctx = ctx;
   /* Note: thread safety of the queue is a bit redundant since the worker
    * thread locks around it. */
   wt->queue = q_new ();
   bson_mutex_init (&wt->mutex);
   mongoc_cond_init (&wt->cond);
   return wt;
}

void
worker_thread_destroy (json_test_worker_thread_t *wt)
{
   q_destroy (wt->queue);
   bson_mutex_destroy (&wt->mutex);
   mongoc_cond_destroy (&wt->cond);
   bson_free (wt);
}

#define WORKER_THREAD_WAIT_TIMEOUT_MS 10 * 1000

static BSON_THREAD_FUN (json_test_worker_thread_run, wt_void)
{
   json_test_worker_thread_t *wt;
   bson_t *op = NULL;
   bool shutdown = false;

   wt = wt_void;

   while (true) {
      /* Wait for a new item in the queue or shutdown. */
      bson_mutex_lock (&wt->mutex);
      op = (bson_t *) q_get_nowait (wt->queue);
      shutdown = wt->shutdown_requested;
      while (!op && !shutdown) {
         mongoc_cond_timedwait (&wt->cond, &wt->mutex, WORKER_THREAD_WAIT_TIMEOUT_MS);
         shutdown = wt->shutdown_requested;
         op = q_get_nowait (wt->queue);
      }
      bson_mutex_unlock (&wt->mutex);

      if (op) {
         bson_t reply;

         json_test_operation (wt->ctx, wt->ctx->config->scenario, op, wt->ctx->collection, NULL /* session */, &reply);
         bson_destroy (op);
         op = NULL;
         bson_destroy (&reply);
      } else if (shutdown) {
         /* If there are no queued operations and shutdown was requested, exit
          * thread. */
         break;
      }
   }
   BSON_THREAD_RETURN;
}

static void
start_thread (json_test_worker_thread_t *wt)
{
   wt->shutdown_requested = false;
   int ret = mcommon_thread_create (&wt->thread, json_test_worker_thread_run, wt);
   ASSERT_CMPINT (0, ==, ret);
}

static void
run_on_thread (json_test_worker_thread_t *wt, bson_t *op)
{
   bson_mutex_lock (&wt->mutex);
   q_put (wt->queue, bson_copy (op));
   mongoc_cond_broadcast (&wt->cond);
   bson_mutex_unlock (&wt->mutex);
}

static void
wait_for_thread (json_test_worker_thread_t *wt)
{
   bson_mutex_lock (&wt->mutex);
   wt->shutdown_requested = true;
   mongoc_cond_broadcast (&wt->cond);
   bson_mutex_unlock (&wt->mutex);
   mcommon_thread_join (wt->thread);
}

void
json_test_ctx_init (json_test_ctx_t *ctx,
                    const bson_t *test,
                    mongoc_client_t *client,
                    mongoc_database_t *db,
                    mongoc_collection_t *collection,
                    const json_test_config_t *config)
{
   char *session_name;
   char *session_opts_path;
   int i;
   bson_error_t error;

   ASSERT (client);

   memset (ctx, 0, sizeof (*ctx));

   ctx->client = client;
   ctx->db = db;
   ctx->collection = collection;
   ctx->change_stream = NULL;
   ctx->config = config;
   ctx->n_events = 0;
   bson_init (&ctx->events);

   ctx->test_framework_uri = mongoc_uri_copy (client->uri);
   ctx->acknowledged = true;
   ctx->verbose = test_framework_getenv_bool ("MONGOC_TEST_MONITORING_VERBOSE");
   bson_init (&ctx->lsids[0]);
   bson_init (&ctx->lsids[1]);
   ctx->sessions[0] = NULL;
   ctx->sessions[1] = NULL;
   ctx->has_sessions = test_framework_session_timeout_minutes () > -1 && test_framework_skip_if_no_crypto ();

   /* transactions tests require two sessions named session0 and session1,
    * retryable writes use one explicit session or none */
   if (ctx->has_sessions) {
      for (i = 0; i < 2; i++) {
         session_name = bson_strdup_printf ("session%d", i);
         session_opts_path = bson_strdup_printf ("sessionOptions.session%d", i);
         if (bson_has_field (test, session_opts_path)) {
            ctx->sessions[i] = bson_lookup_session (test, session_opts_path, client);
         } else {
            ctx->sessions[i] = mongoc_client_start_session (client, NULL, &error);
         }

         ASSERT_OR_PRINT (ctx->sessions[i], error);
         bson_concat (&ctx->lsids[i], mongoc_client_session_get_lsid (ctx->sessions[i]));

         bson_free (session_name);
         bson_free (session_opts_path);
      }
   }
   bson_mutex_init (&ctx->mutex);
}


void
json_test_ctx_end_sessions (json_test_ctx_t *ctx)
{
   int i;

   if (ctx->has_sessions) {
      for (i = 0; i < 2; i++) {
         if (ctx->sessions[i]) {
            mongoc_client_session_destroy (ctx->sessions[i]);
            ctx->sessions[i] = NULL;
         }
      }
   }
}


void
json_test_ctx_cleanup (json_test_ctx_t *ctx)
{
   json_test_ctx_end_sessions (ctx);
   mongoc_change_stream_destroy (ctx->change_stream);
   bson_destroy (&ctx->lsids[0]);
   bson_destroy (&ctx->lsids[1]);
   bson_destroy (&ctx->events);
   mongoc_uri_destroy (ctx->test_framework_uri);
   bson_destroy (ctx->sent_lsids[0]);
   bson_destroy (ctx->sent_lsids[1]);
   bson_mutex_destroy (&ctx->mutex);
}


static void
append_session (mongoc_client_session_t *session, bson_t *opts)
{
   if (session) {
      bool r;
      bson_error_t error;

      r = mongoc_client_session_append (session, opts, &error);
      ASSERT_OR_PRINT (r, error);
   }
}

static char *
value_to_str (const bson_value_t *value)
{
   bson_t doc;

   if (value->value_type == BSON_TYPE_DOCUMENT || value->value_type == BSON_TYPE_ARRAY) {
      bson_init_from_value (&doc, value);
      return bson_as_json (&doc, NULL);
   } else {
      return bson_strdup_printf ("%" PRId64, bson_value_as_int64 (value));
   }
}


static void
convert_bulk_write_result (const bson_t *doc, bson_t *r)
{
   bson_iter_t iter;

   ASSERT (bson_iter_init (&iter, doc));

   /* tests expect "insertedCount" etc. from the CRUD Spec for bulk write,
    * we return "nInserted" etc. from the old Bulk API Spec */
   while (bson_iter_next (&iter)) {
      if (BSON_ITER_IS_KEY (&iter, "insertedCount")) {
         BSON_APPEND_VALUE (r, "nInserted", bson_iter_value (&iter));
      } else if (BSON_ITER_IS_KEY (&iter, "deletedCount")) {
         BSON_APPEND_VALUE (r, "nRemoved", bson_iter_value (&iter));
      } else if (BSON_ITER_IS_KEY (&iter, "matchedCount")) {
         BSON_APPEND_VALUE (r, "nMatched", bson_iter_value (&iter));
      } else if (BSON_ITER_IS_KEY (&iter, "modifiedCount")) {
         BSON_APPEND_VALUE (r, "nModified", bson_iter_value (&iter));
      } else if (BSON_ITER_IS_KEY (&iter, "upsertedCount")) {
         BSON_APPEND_VALUE (r, "nUpserted", bson_iter_value (&iter));
      } else if (BSON_ITER_IS_KEY (&iter, "upsertedIds")) {
         bson_array_builder_t *upserted;
         bson_iter_t inner;
         uint32_t i = 0;

         ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));

         /* include the "upserted" field if upsertedIds isn't empty */
         ASSERT (bson_iter_recurse (&iter, &inner));
         while (bson_iter_next (&inner)) {
            i++;
         }

         if (i) {
            ASSERT (bson_iter_recurse (&iter, &inner));
            BSON_APPEND_ARRAY_BUILDER_BEGIN (r, "upserted", &upserted);

            while (bson_iter_next (&inner)) {
               bson_t upsert;
               int64_t index;

               index = bson_ascii_strtoll (bson_iter_key (&inner), NULL, 10);

               bson_array_builder_append_document_begin (upserted, &upsert);
               BSON_APPEND_INT32 (&upsert, "index", (int32_t) index);
               BSON_APPEND_VALUE (&upsert, "_id", bson_iter_value (&inner));
               bson_array_builder_append_document_end (upserted, &upsert);
            }

            bson_append_array_builder_end (r, upserted);
         }
      } else if (BSON_ITER_IS_KEY (&iter, "insertedIds")) {
         /* tests expect insertedIds, but they're optional and we omit them */
      } else {
         BSON_APPEND_VALUE (r, bson_iter_key (&iter), bson_iter_value (&iter));
      }
   }
}


/* convert from spec result in JSON test to a libmongoc result */
static void
convert_spec_result (const char *op_name, const bson_value_t *spec_result, bson_value_t *converted)
{
   bson_t doc;
   bson_t r;
   bson_iter_t iter;

   if (spec_result->value_type != BSON_TYPE_DOCUMENT && spec_result->value_type != BSON_TYPE_ARRAY) {
      bson_value_copy (spec_result, converted);
      return;
   }

   bson_init (&r);
   bson_init_from_value (&doc, spec_result);
   ASSERT (bson_iter_init (&iter, &doc));

   if (!strcmp (op_name, "bulkWrite")) {
      convert_bulk_write_result (&doc, &r);
   } else {
      /* tests expect "upsertedCount": 0, we add "upsertedCount" only if it's
       * not zero. tests also expect insertedId or insertedIds, but they are
       * optional and we omit them. */
      while (bson_iter_next (&iter)) {
         if (BSON_ITER_IS_KEY (&iter, "upsertedCount")) {
            if (bson_iter_as_int64 (&iter) != 0) {
               BSON_APPEND_INT64 (&r, "upsertedCount", bson_iter_as_int64 (&iter));
            }
         } else if (!BSON_ITER_IS_KEY (&iter, "insertedId") && !BSON_ITER_IS_KEY (&iter, "insertedIds")) {
            BSON_APPEND_VALUE (&r, bson_iter_key (&iter), bson_iter_value (&iter));
         }
      }
   }

   /* copies r's contents */
   value_init_from_doc (converted, &r);
   /* preserve spec tests' distinction between array and document */
   converted->value_type = spec_result->value_type;
   bson_destroy (&r);
}


static bool
get_result (const bson_t *test, const bson_t *operation, bson_value_t *value)
{
   const char *op_name;
   bson_value_t pre_conversion;

   /* retryable writes tests specify result at the end of the whole test:
    *   operation:
    *     name: insertOne
    *     arguments: ...
    *   outcome:
    *     result:
    *       insertedId: 3
    *
    * transactions tests specify the result of each operation:
    *    operations:
    *      - name: insertOne
    *        arguments: ...
    *        result:
    *          insertedId: 3
    *
    * command monitoring tests have no results
    */
   if (bson_has_field (test, "outcome.result")) {
      bson_lookup_value (test, "outcome.result", &pre_conversion);
   } else if (bson_has_field (operation, "result")) {
      bson_lookup_value (operation, "result", &pre_conversion);
   } else {
      return false;
   }

   if (value) {
      op_name = bson_lookup_utf8 (operation, "name");
      convert_spec_result (op_name, &pre_conversion, value);
   }

   bson_value_destroy (&pre_conversion);

   return true;
}


static void
check_success_expected (const bson_t *operation, bool succeeded, bool expected, const bson_error_t *error)
{
   char *json = bson_as_json (operation, NULL);

   if (!succeeded && expected) {
      test_error ("Expected success, got error \"%s\":\n%s", error->message, json);
   }
   if (succeeded && !expected) {
      test_error ("Expected error, got success:\n%s", json);
   }

   bson_free (json);
}


static uint32_t
error_code_from_name (const char *name)
{
   if (!strcmp (name, "CannotSatisfyWriteConcern")) {
      return 100;
   } else if (!strcmp (name, "DuplicateKey")) {
      return 11000;
   } else if (!strcmp (name, "NoSuchTransaction")) {
      return 251;
   } else if (!strcmp (name, "WriteConflict")) {
      return 112;
   } else if (!strcmp (name, "Interrupted")) {
      return 11601;
   } else if (!strcmp (name, "MaxTimeMSExpired")) {
      return 50;
   } else if (!strcmp (name, "UnknownReplWriteConcern")) {
      return 79;
   } else if (!strcmp (name, "UnsatisfiableWriteConcern")) {
      return 100;
   } else if (!strcmp (name, "OperationNotSupportedInTransaction")) {
      return 263;
   }

   test_error ("Add errorCodeName \"%s\" to error_code_from_name()", name);

   /* test_error() aborts, but add a return to suppress compiler warnings */
   return 0;
}


static void
check_error_code_name (const bson_t *operation, const bson_error_t *error)
{
   const char *code_name;
   uint32_t expected_error_code;

   if (!bson_has_field (operation, "errorCodeName")) {
      return;
   }

   code_name = bson_lookup_utf8 (operation, "errorCodeName");
   expected_error_code = error_code_from_name (code_name);
   if (error->code != expected_error_code) {
      test_error ("Expected error code %d : %s but got error code %d\n", expected_error_code, code_name, error->code);
   }
}


static void
check_error_contains (const bson_t *operation, const bson_error_t *error)
{
   const char *msg;

   if (!bson_has_field (operation, "errorContains")) {
      return;
   }

   msg = bson_lookup_utf8 (operation, "errorContains");

   ASSERT_CONTAINS (error->message, msg);
}


static void
check_error_labels_contain (const bson_t *operation, const bson_value_t *result)
{
   bson_t reply;
   bson_iter_t operation_iter;
   bson_iter_t expected_labels;
   bson_iter_t expected_label;
   const char *expected_label_str;

   if (!bson_has_field (operation, "errorLabelsContain")) {
      return;
   }

   BSON_ASSERT (result);

   BSON_ASSERT (bson_iter_init (&operation_iter, operation));
   BSON_ASSERT (bson_iter_find_descendant (&operation_iter, "errorLabelsContain", &expected_labels));
   BSON_ASSERT (bson_iter_recurse (&expected_labels, &expected_label));

   /* if the test has "errorLabelsContain" then result must be an error reply */
   ASSERT_CMPSTR (_mongoc_bson_type_to_str (result->value_type), "DOCUMENT");
   bson_init_from_value (&reply, result);

   while (bson_iter_next (&expected_label)) {
      expected_label_str = bson_iter_utf8 (&expected_label, NULL);
      if (!mongoc_error_has_label (&reply, expected_label_str)) {
         test_error ("Expected label \"%s\" not found in %s", expected_label_str, bson_as_json (&reply, NULL));
      }
   }
}


static void
check_error_labels_omit (const bson_t *operation, const bson_value_t *result)
{
   bson_t reply;
   bson_t omitted_labels;
   bson_iter_t omitted_label;

   if (!bson_has_field (operation, "errorLabelsOmit")) {
      return;
   }

   BSON_ASSERT (result);

   if (result->value_type != BSON_TYPE_DOCUMENT) {
      /* successful result from count, distinct, etc. */
      return;
   }

   bson_init_from_value (&reply, result);
   if (!bson_has_field (&reply, "errorLabels")) {
      return;
   }

   bson_lookup_doc (operation, "errorLabelsOmit", &omitted_labels);
   BSON_ASSERT (bson_iter_init (&omitted_label, &omitted_labels));
   while (bson_iter_next (&omitted_label)) {
      if (mongoc_error_has_label (&reply, bson_iter_utf8 (&omitted_label, NULL))) {
         test_error (
            "Label \"%s\" should have been omitted %s", bson_iter_utf8 (&omitted_label, NULL), value_to_str (result));
      }
   }
}


/*--------------------------------------------------------------------------
 *
 * check_result --
 *
 *       Verify that a function call's outcome matches the expected outcome.
 *
 *       Consider a JSON test like:
 *
 *         operations:
 *           - name: insertOne
 *             arguments:
 *               document:
 *                 _id: 1
 *               session: session0
 *             result:
 *               insertedId: 1
 *
 *       @test is the BSON representation of the entire test including the
 *       "operations" array, @operation is one of the documents in array,
 *       @succeeded is true if the function call actually succeeded, @result
 *       is the function call's result (optional), and @error is the call's
 *       error (optional).
 *
 * Side effects:
 *       Logs and aborts if the outcome does not match the expected outcome.
 *
 *--------------------------------------------------------------------------
 */

void
check_result (
   const bson_t *test, const bson_t *operation, bool succeeded, const bson_value_t *result, const bson_error_t *error)
{
   bson_t expected_doc;
   bson_value_t expected_value;
   match_ctx_t ctx = {{0}};

   if (!get_result (test, operation, &expected_value)) {
      /* if there's no "result", e.g. in the command monitoring tests,
       * we don't know if the command is expected to succeed or fail */
      return;
   }

   if (expected_value.value_type == BSON_TYPE_DOCUMENT) {
      bson_init_from_value (&expected_doc, &expected_value);
      if (bson_has_field (&expected_doc, "errorCodeName") || bson_has_field (&expected_doc, "errorContains") ||
          bson_has_field (&expected_doc, "errorLabelsContain") || bson_has_field (&expected_doc, "errorLabelsOmit")) {
         /* Expect the operation has failed. Transactions tests specify errors
          * per-operation, with one or more details:
          *    operations:
          *      - name: insertOne
          *        arguments: ...
          *        result:
          *          errorCodeName: WriteConflict
          *          errorContains: "message substring"
          *          errorLabelsContain: ["TransientTransactionError"]
          *          errorLabelsOmit: ["UnknownTransactionCommitResult"]
          */
         check_success_expected (&expected_doc, succeeded, false, error);
         check_error_code_name (&expected_doc, error);
         check_error_contains (&expected_doc, error);
         check_error_labels_contain (&expected_doc, result);
         check_error_labels_omit (&expected_doc, result);
         bson_value_destroy (&expected_value);
         return;
      }
   }

   /* retryable writes tests and CRUD tests specify error: <bool> at the end of
    * the whole test:
    *   operation:
    *     name: insertOne
    *   outcome:
    *     error: true
    */

   check_success_expected (operation, succeeded, !_mongoc_lookup_bool (test, "outcome.error", false), error);

   BSON_ASSERT (result);

   /* Database-level aggregate tests use $currentOp, which may return a command
    * document containing dots in keys. Retain these as we do for APM checks. */
   ctx.retain_dots_in_keys = true;

   if (!match_bson_value (result, &expected_value, &ctx)) {
      test_error ("Error in \"%s\" test %s\n"
                  "Expected:\n%s\nActual:\n%s",
                  bson_lookup_utf8 (test, "description"),
                  ctx.errmsg,
                  value_to_str (&expected_value),
                  value_to_str (result));
   }

   bson_value_destroy (&expected_value);
}


static bool
add_request_to_bulk (mongoc_bulk_operation_t *bulk, const bson_t *request, bson_error_t *error)
{
   const char *name;
   bson_t args;
   bool r;
   bson_t opts = BSON_INITIALIZER;

   name = bson_lookup_utf8 (request, "name");
   bson_lookup_doc (request, "arguments", &args);

   if (bson_has_field (&args, "arrayFilters")) {
      bson_value_t array_filters;
      bson_lookup_value (&args, "arrayFilters", &array_filters);
      BSON_APPEND_VALUE (&opts, "arrayFilters", &array_filters);
      bson_value_destroy (&array_filters);
   }

   if (bson_has_field (&args, "collation")) {
      bson_t collation;
      bson_lookup_doc (&args, "collation", &collation);
      BSON_APPEND_DOCUMENT (&opts, "collation", &collation);
   }

   if (bson_has_field (&args, "hint")) {
      bson_value_t hint;
      bson_lookup_value (&args, "hint", &hint);
      BSON_APPEND_VALUE (&opts, "hint", &hint);
      bson_value_destroy (&hint);
   }

   if (!strcmp (name, "deleteMany")) {
      bson_t filter;

      bson_lookup_doc (&args, "filter", &filter);

      r = mongoc_bulk_operation_remove_many_with_opts (bulk, &filter, &opts, error);
   } else if (!strcmp (name, "deleteOne")) {
      bson_t filter;

      bson_lookup_doc (&args, "filter", &filter);

      r = mongoc_bulk_operation_remove_one_with_opts (bulk, &filter, &opts, error);
   } else if (!strcmp (name, "insertOne")) {
      bson_t document;

      bson_lookup_doc (&args, "document", &document);

      r = mongoc_bulk_operation_insert_with_opts (bulk, &document, &opts, error);
   } else if (!strcmp (name, "replaceOne")) {
      bson_t filter;
      bson_t replacement;

      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "replacement", &replacement);

      if (bson_has_field (&args, "upsert")) {
         BSON_APPEND_BOOL (&opts, "upsert", _mongoc_lookup_bool (&args, "upsert", false));
      }

      r = mongoc_bulk_operation_replace_one_with_opts (bulk, &filter, &replacement, &opts, error);
   } else if (!strcmp (name, "updateMany")) {
      bson_t filter;
      bson_t update;

      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "update", &update);

      if (bson_has_field (&args, "upsert")) {
         BSON_APPEND_BOOL (&opts, "upsert", _mongoc_lookup_bool (&args, "upsert", false));
      }

      r = mongoc_bulk_operation_update_many_with_opts (bulk, &filter, &update, &opts, error);
   } else if (!strcmp (name, "updateOne")) {
      bson_t filter;
      bson_t update;

      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "update", &update);

      if (bson_has_field (&args, "upsert")) {
         BSON_APPEND_BOOL (&opts, "upsert", _mongoc_lookup_bool (&args, "upsert", false));
      }

      r = mongoc_bulk_operation_update_one_with_opts (bulk, &filter, &update, &opts, error);
   } else {
      test_error ("unrecognized request name %s", name);
   }

   bson_destroy (&opts);

   return r;
}


static void
execute_bulk_operation (mongoc_bulk_operation_t *bulk, const bson_t *test, const bson_t *operation, bson_t *reply)
{
   uint32_t server_id;
   bson_error_t error;
   bson_value_t value;

   server_id = mongoc_bulk_operation_execute (bulk, reply, &error);
   value_init_from_doc (&value, reply);
   check_result (test, operation, server_id != 0, &value, &error);
   bson_value_destroy (&value);
}


static bson_t *
create_bulk_write_opts (const bson_t *operation, mongoc_client_session_t *session, mongoc_write_concern_t *wc)
{
   bson_t *opts;
   bson_t tmp;

   opts = tmp_bson ("{}");

   if (bson_has_field (operation, "arguments.options")) {
      bson_lookup_doc (operation, "arguments.options", &tmp);
      bson_concat (opts, &tmp);
   }

   append_session (session, opts);

   if (!mongoc_write_concern_is_default (wc)) {
      BSON_ASSERT (mongoc_write_concern_append (wc, opts));
   }

   return opts;
}


static bool
bulk_write (mongoc_collection_t *collection,
            const bson_t *test,
            const bson_t *operation,
            mongoc_client_session_t *session,
            mongoc_write_concern_t *wc,
            bson_t *reply)
{
   bson_t *opts;
   mongoc_bulk_operation_t *bulk;
   bson_t requests;
   bson_iter_t iter;
   bson_error_t error;

   opts = create_bulk_write_opts (operation, session, wc);
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, opts);

   bson_lookup_doc (operation, "arguments.requests", &requests);
   ASSERT (bson_iter_init (&iter, &requests));

   while (bson_iter_next (&iter)) {
      bson_t request;

      bson_iter_bson (&iter, &request);
      if (!add_request_to_bulk (bulk, &request, &error)) {
         /* For the sake of the test framework, we must init this */
         bson_init (reply);

         check_result (test, operation, false, NULL, &error);
         mongoc_bulk_operation_destroy (bulk);

         return false;
      }
   }

   execute_bulk_operation (bulk, test, operation, reply);
   mongoc_bulk_operation_destroy (bulk);

   return true;
}


#define COPY_EXCEPT(...) bson_copy_to_excluding_noinit (&args, &opts, "session", "readPreference", __VA_ARGS__, NULL)


static bool
single_write (mongoc_collection_t *collection,
              const bson_t *test,
              const bson_t *operation,
              mongoc_client_session_t *session,
              mongoc_write_concern_t *wc,
              bson_t *reply)
{
   const char *name;
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   bson_value_t value;
   bson_error_t error;
   bool r;

   BSON_UNUSED (wc);

   name = bson_lookup_utf8 (operation, "name");
   bson_lookup_doc (operation, "arguments", &args);
   append_session (session, &opts);

   if (!strcmp (name, "deleteMany")) {
      bson_t filter;
      bson_lookup_doc (&args, "filter", &filter);
      COPY_EXCEPT ("filter");
      r = mongoc_collection_delete_many (collection, &filter, &opts, reply, &error);
   } else if (!strcmp (name, "deleteOne")) {
      bson_t filter;
      bson_lookup_doc (&args, "filter", &filter);
      COPY_EXCEPT ("filter");
      r = mongoc_collection_delete_one (collection, &filter, &opts, reply, &error);
   } else if (!strcmp (name, "insertOne")) {
      bson_t document;
      bson_lookup_doc (&args, "document", &document);
      COPY_EXCEPT ("document");
      r = mongoc_collection_insert_one (collection, &document, &opts, reply, &error);
   } else if (!strcmp (name, "replaceOne")) {
      bson_t filter;
      bson_t replacement;
      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "replacement", &replacement);
      COPY_EXCEPT ("filter", "replacement");
      r = mongoc_collection_replace_one (collection, &filter, &replacement, &opts, reply, &error);
   } else if (!strcmp (name, "updateMany")) {
      bson_t filter;
      bson_t update;
      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "update", &update);
      COPY_EXCEPT ("filter", "update");
      r = mongoc_collection_update_many (collection, &filter, &update, &opts, reply, &error);
   } else if (!strcmp (name, "updateOne")) {
      bson_t filter;
      bson_t update;
      bson_lookup_doc (&args, "filter", &filter);
      bson_lookup_doc (&args, "update", &update);
      COPY_EXCEPT ("filter", "update");
      r = mongoc_collection_update_one (collection, &filter, &update, &opts, reply, &error);
   } else {
      test_error ("unrecognized request name %s", name);
   }

   value_init_from_doc (&value, reply);
   check_result (test, operation, r, &value, &error);
   bson_value_destroy (&value);
   bson_destroy (&opts);

   return r;
}


static mongoc_find_and_modify_opts_t *
create_find_and_modify_opts (const char *name,
                             const bson_t *args,
                             mongoc_client_session_t *session,
                             mongoc_write_concern_t *wc)
{
   mongoc_find_and_modify_opts_t *opts;
   mongoc_find_and_modify_flags_t flags = MONGOC_FIND_AND_MODIFY_NONE;
   bson_t extra = BSON_INITIALIZER;
   bson_iter_t iter;

   opts = mongoc_find_and_modify_opts_new ();

   if (!strcmp (name, "findOneAndDelete")) {
      flags |= MONGOC_FIND_AND_MODIFY_REMOVE;
   }

   if (!strcmp (name, "findOneAndReplace")) {
      bson_t replacement;
      bson_lookup_doc (args, "replacement", &replacement);
      mongoc_find_and_modify_opts_set_update (opts, &replacement);
   }

   if (!strcmp (name, "findOneAndUpdate")) {
      bson_t update;
      bson_lookup_doc (args, "update", &update);
      mongoc_find_and_modify_opts_set_update (opts, &update);
   }

   if (bson_has_field (args, "sort")) {
      bson_t sort;
      bson_lookup_doc (args, "sort", &sort);
      mongoc_find_and_modify_opts_set_sort (opts, &sort);
   }

   if (_mongoc_lookup_bool (args, "upsert", false)) {
      flags |= MONGOC_FIND_AND_MODIFY_UPSERT;
   }

   if (bson_has_field (args, "collation")) {
      bson_t collation = BSON_INITIALIZER;
      bson_t temp;
      bson_lookup_doc (args, "collation", &temp);
      BSON_APPEND_DOCUMENT (&collation, "collation", &temp);
      mongoc_find_and_modify_opts_append (opts, &collation);
      bson_destroy (&collation);
      bson_destroy (&temp);
   }

   if (bson_has_field (args, "arrayFilters")) {
      bson_t array_filters = BSON_INITIALIZER;
      bson_value_t temp;
      bson_lookup_value (args, "arrayFilters", &temp);
      BSON_APPEND_VALUE (&array_filters, "arrayFilters", &temp);
      mongoc_find_and_modify_opts_append (opts, &array_filters);
      bson_destroy (&array_filters);
      bson_value_destroy (&temp);
   }

   if (bson_has_field (args, "returnDocument") && !strcmp ("After", bson_lookup_utf8 (args, "returnDocument"))) {
      flags |= MONGOC_FIND_AND_MODIFY_RETURN_NEW;
   }

   if (bson_iter_init_find (&iter, args, "hint")) {
      bson_append_iter (&extra, "hint", 4, &iter);
   }

   mongoc_find_and_modify_opts_set_flags (opts, flags);
   append_session (session, &extra);

   if (!mongoc_write_concern_is_default (wc)) {
      BSON_ASSERT (mongoc_write_concern_append (wc, &extra));
   }

   ASSERT (mongoc_find_and_modify_opts_append (opts, &extra));
   bson_destroy (&extra);

   return opts;
}


static bool
find_and_modify (mongoc_collection_t *collection,
                 const bson_t *test,
                 const bson_t *operation,
                 mongoc_client_session_t *session,
                 mongoc_write_concern_t *wc,
                 bson_t *reply)
{
   const char *name;
   bson_t args;
   bson_t filter;
   mongoc_find_and_modify_opts_t *opts;
   bson_value_t value = {0};
   bson_error_t error;
   bool r;

   name = bson_lookup_utf8 (operation, "name");
   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (operation, "arguments.filter", &filter);

   opts = create_find_and_modify_opts (name, &args, session, wc);

   r = mongoc_collection_find_and_modify_with_opts (collection, &filter, opts, reply, &error);


   /* Transactions Tests have findAndModify results like:
    *   result: {_id: 3}
    *
    * Or for findOneAndDelete with no result:
    *   result: null
    *
    * But mongoc_collection_find_and_modify_with_opts returns:
    *   { ok: 1, value: {_id: 3}}
    *
    * Or:
    *   { ok: 1, value: null}
    */
   if (get_result (test, operation, NULL)) {
      if (r) {
         bson_lookup_value (reply, "value", &value);
      } else {
         value_init_from_doc (&value, reply);
      }
   }

   check_result (test, operation, r, &value, &error);

   bson_value_destroy (&value);
   mongoc_find_and_modify_opts_destroy (opts);
   bson_destroy (&args);
   bson_destroy (&filter);

   return r;
}


static bool
insert_many (mongoc_collection_t *collection,
             const bson_t *test,
             const bson_t *operation,
             mongoc_client_session_t *session,
             mongoc_write_concern_t *wc,
             bson_t *reply)
{
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   bson_t docs_array;
   bson_t *doc_ptrs[100];
   size_t i, n;
   bson_iter_t iter;
   bson_value_t value;
   bson_error_t error;
   bool r;

   BSON_UNUSED (wc);

   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (&args, "documents", &docs_array);
   ASSERT (bson_iter_init (&iter, &docs_array));
   n = 0;
   while (bson_iter_next (&iter)) {
      bson_t document;
      bson_iter_bson (&iter, &document);
      doc_ptrs[n] = bson_copy (&document);
      n++;
      if (n >= 100) {
         test_error ("Too many documents: %s", bson_as_json (operation, NULL));
      }
   }

   append_session (session, &opts);
   /* transactions tests are like:
    *   operation:
    *     name: "insertMany"
    *     arguments:
    *       documents:
    *         - { _id: 2, x: 22 }
    *       options: { ordered: false }
    */
   BSON_APPEND_BOOL (&opts, "ordered", _mongoc_lookup_bool (&args, "options.ordered", true /* default */));

   COPY_EXCEPT ("documents", "options");

   r = mongoc_collection_insert_many (collection, (const bson_t **) doc_ptrs, n, &opts, reply, &error);

   /* CRUD tests may specify a write result even if an error is expected.
    * From the CRUD spec test readme:
    * "drivers may choose to check the result object if their BulkWriteException
    * (or equivalent) provides access to a write result object"
    * The C driver does not return a full write result for an "InsertMany" on
    * error, but instead just the insertedCount.
    */
   if (!r) {
      BSON_APPEND_INT64 (reply, "deletedCount", 0);
      BSON_APPEND_INT64 (reply, "matchedCount", 0);
      BSON_APPEND_INT64 (reply, "modifiedCount", 0);
      BSON_APPEND_INT64 (reply, "upsertedCount", 0);
      BSON_APPEND_DOCUMENT (reply, "upsertedIds", tmp_bson ("{}"));
   }

   value_init_from_doc (&value, reply);
   check_result (test, operation, r, &value, &error);
   bson_value_destroy (&value);
   bson_destroy (&opts);

   for (i = 0; i < n; i++) {
      bson_destroy (doc_ptrs[i]);
   }

   return r;
}


static bool
rename_op (mongoc_collection_t *collection,
           const bson_t *test,
           const bson_t *operation,
           mongoc_client_session_t *session,
           mongoc_write_concern_t *wc,
           bson_t *reply)
{
   bson_t args;
   const char *db;
   const char *to;
   bson_error_t error;
   bool res;

   BSON_UNUSED (test);
   BSON_UNUSED (session);
   BSON_UNUSED (wc);


   bson_lookup_doc (operation, "arguments", &args);
   db = bson_lookup_utf8 (operation, "database");
   to = bson_lookup_utf8 (&args, "to");

   res = mongoc_collection_rename (collection, db, to, true, &error);

   /* This operation is only run by change stream tests, which use
      it to trigger further events and check all results elsewhere. */
   ASSERT_OR_PRINT (res, error);

   bson_destroy (&args);

   /* fake a reply for the test framework's sake */
   bson_init (reply);

   return res;
}


static bool
drop (mongoc_collection_t *collection,
      const bson_t *test,
      const bson_t *operation,
      mongoc_client_session_t *session,
      mongoc_write_concern_t *wc,
      bson_t *reply)
{
   bson_error_t error;
   bool res;

   BSON_UNUSED (test);
   BSON_UNUSED (operation);
   BSON_UNUSED (session);
   BSON_UNUSED (wc);

   res = mongoc_collection_drop (collection, &error);

   /* This operation is only run by change stream tests, which use
      it to trigger further events and check all results elsewhere. */
   ASSERT_OR_PRINT (res, error);

   /* fake a reply for the test framework's sake */
   bson_init (reply);

   return res;
}


static bool
count (mongoc_collection_t *collection,
       const bson_t *test,
       const bson_t *operation,
       mongoc_client_session_t *session,
       const mongoc_read_prefs_t *read_prefs,
       bson_t *reply)
{
   bson_t filter;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   int64_t r;
   bson_value_t value;
   const char *name;

   if (bson_has_field (operation, "arguments.filter")) {
      bson_lookup_doc (operation, "arguments.filter", &filter);
   }
   if (bson_has_field (operation, "arguments.skip")) {
      BSON_APPEND_INT64 (&opts, "skip", bson_lookup_int32 (operation, "arguments.skip"));
   }
   if (bson_has_field (operation, "arguments.limit")) {
      BSON_APPEND_INT64 (&opts, "limit", bson_lookup_int32 (operation, "arguments.limit"));
   }
   if (bson_has_field (operation, "arguments.collation")) {
      bson_t collation;
      bson_lookup_doc (operation, "arguments.collation", &collation);
      BSON_APPEND_DOCUMENT (&opts, "collation", &collation);
   }
   append_session (session, &opts);

   name = bson_lookup_utf8 (operation, "name");
   if (!strcmp (name, "countDocuments")) {
      r = mongoc_collection_count_documents (collection, &filter, &opts, read_prefs, reply, &error);
   } else if (!strcmp (name, "estimatedDocumentCount")) {
      r = mongoc_collection_estimated_document_count (collection, &opts, read_prefs, reply, &error);
   } else if (!strcmp (name, "count")) {
      /* deprecated old count function */
      r = mongoc_collection_count_with_opts (collection, MONGOC_QUERY_NONE, &filter, 0, 0, &opts, read_prefs, &error);
      /* fake a reply for the test framework's sake */
      bson_init (reply);
   } else {
      test_error ("count() called with unrecognized operation name %s", name);
      return false;
   }

   if (r >= 0) {
      value.value_type = BSON_TYPE_INT64;
      value.value.v_int64 = r;
   } else {
      value_init_from_doc (&value, reply);
   }
   check_result (test, operation, r > -1, &value, &error);

   bson_value_destroy (&value);
   bson_destroy (&opts);

   return (r > -1);
}

static bool
distinct (mongoc_collection_t *collection,
          const bson_t *test,
          const bson_t *operation,
          mongoc_client_session_t *session,
          const mongoc_read_prefs_t *read_prefs,
          bson_t *reply)
{
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   const char *field_name;
   bson_t query;
   bson_value_t value = {0};
   bson_error_t error;
   bool r;

   append_session (session, &opts);
   bson_lookup_doc (operation, "arguments", &args);
   field_name = bson_lookup_utf8 (&args, "fieldName");
   if (bson_has_field (&args, "filter")) {
      bson_lookup_doc (&args, "filter", &query);
      BSON_APPEND_DOCUMENT (&opts, "query", &query);
   }

   COPY_EXCEPT ("fieldName", "filter");

   r = mongoc_collection_read_command_with_opts (
      collection,
      tmp_bson ("{'distinct': '%s', 'key': '%s'}", collection->collection, field_name),
      read_prefs,
      &opts,
      reply,
      &error);

   /* Transactions Tests have "distinct" results like:
    *   result: [1, 2, 3]
    *
    * But the command returns:
    *   { ok: 1, values: [1, 2, 3]} */
   if (r) {
      bson_lookup_value (reply, "values", &value);
   } else {
      value_init_from_doc (&value, reply);
   }

   check_result (test, operation, r, &value, &error);

   bson_value_destroy (&value);
   bson_destroy (&opts);

   return r;
}


static void
check_cursor (mongoc_cursor_t *cursor, const bson_t *test, const bson_t *operation)
{
   const bson_t *doc;
   bson_error_t error;
   bson_t result = BSON_INITIALIZER;
   const char *keyptr = NULL;
   char key[12];
   uint32_t i = 0;
   bson_value_t value;

   i = 0;
   while (mongoc_cursor_next (cursor, &doc)) {
      bson_uint32_to_string (i++, &keyptr, key, sizeof key);
      BSON_APPEND_DOCUMENT (&result, keyptr, doc);
   }

   if (mongoc_cursor_error_document (cursor, &error, &doc)) {
      value_init_from_doc (&value, doc);
      check_result (test, operation, false, &value, &error);
   } else {
      value_init_from_doc (&value, &result);
      value.value_type = BSON_TYPE_ARRAY;
      check_result (test, operation, true, &value, &error);
   }

   bson_value_destroy (&value);
   bson_destroy (&result);
}


static bool
find (mongoc_collection_t *collection,
      const bson_t *test,
      const bson_t *operation,
      mongoc_client_session_t *session,
      const mongoc_read_prefs_t *read_prefs,
      bson_t *reply)
{
   bson_t args;
   bson_t tmp;
   bson_t filter;
   bson_t opts = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   bson_error_t error;

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   bson_lookup_doc (operation, "arguments", &args);
   if (bson_has_field (&args, "filter")) {
      bson_lookup_doc (&args, "filter", &tmp);
      bson_copy_to (&tmp, &filter);
   } else {
      bson_init (&filter);
   }

   /* Command Monitoring Spec tests use OP_QUERY-style modifiers for "find":
    *   arguments:
    *    filter: { _id: { $gt: 1 } }
    *    sort: { _id: 1 }
    *    skip: {"$numberLong": "2"}
    *    modifiers:
    *      $comment: "test"
    *      $showDiskLoc: false
    *
    * Abuse _mongoc_cursor_translate_dollar_query_opts to upgrade "modifiers".
    */
   if (bson_has_field (&args, "modifiers")) {
      bson_t modifiers;
      bson_t *query = tmp_bson ("{'$query': {}}");
      bson_t unwrapped;
      bool r;

      bson_lookup_doc (&args, "modifiers", &modifiers);
      bson_concat (query, &modifiers);
      r = _mongoc_cursor_translate_dollar_query_opts (query, &opts, &unwrapped, &error);
      ASSERT_OR_PRINT (r, error);
      bson_destroy (&unwrapped);
   }

   COPY_EXCEPT ("filter", "modifiers");
   append_session (session, &opts);

   cursor = mongoc_collection_find_with_opts (collection, &filter, &opts, read_prefs);

   check_cursor (cursor, test, operation);
   mongoc_cursor_destroy (cursor);
   bson_destroy (&filter);
   bson_destroy (&opts);

   return true;
}


static bool
find_one (mongoc_collection_t *collection,
          const bson_t *test,
          const bson_t *operation,
          mongoc_client_session_t *session,
          const mongoc_read_prefs_t *read_prefs,
          bson_t *reply)
{
   bson_t filter;
   bson_t opts = BSON_INITIALIZER;
   const bson_t *doc;
   mongoc_cursor_t *cursor;
   bson_value_t value;
   bson_error_t error;

   BSON_UNUSED (session);

   bson_lookup_doc (operation, "arguments.filter", &filter);

   cursor = mongoc_collection_find_with_opts (collection, &filter, &opts, read_prefs);

   if (mongoc_cursor_next (cursor, &doc)) {
      value_init_from_doc (&value, doc);
      mongoc_cursor_error (cursor, &error);
      check_result (test, operation, true, &value, &error);
   } else if (mongoc_cursor_error_document (cursor, &error, &doc)) {
      value_init_from_doc (&value, doc);
      check_result (test, operation, false, &value, &error);
   }

   mongoc_cursor_destroy (cursor);
   bson_destroy (&filter);
   bson_destroy (&opts);
   bson_value_destroy (&value);
   bson_init (reply);
   return true;
}


static bool
_is_aggregate_out (const bson_t *pipeline)
{
   bson_iter_t iter;
   bson_iter_t stage;

   ASSERT (bson_iter_init (&iter, pipeline));
   while (bson_iter_next (&iter)) {
      if (BSON_ITER_HOLDS_DOCUMENT (&iter) && bson_iter_recurse (&iter, &stage)) {
         if (bson_iter_find (&stage, "$out")) {
            return true;
         }
      }
   }

   return false;
}


static bool
aggregate (mongoc_collection_t *collection,
           const bson_t *test,
           const bson_t *operation,
           mongoc_client_session_t *session,
           const mongoc_read_prefs_t *read_prefs,
           bson_t *reply)
{
   bson_t args;
   bson_t pipeline;
   bson_t opts = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (&args, "pipeline", &pipeline);
   append_session (session, &opts);
   COPY_EXCEPT ("pipeline");

   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, &pipeline, &opts, read_prefs);

   /* Driver CRUD API Spec: "$out is a special pipeline stage that causes no
    * results to be returned from the server. As such, the iterable here would
    * never contain documents. Drivers MAY setup a cursor to be executed upon
    * iteration against the $out collection such that if a user were to iterate
    * a pipeline including $out, results would be returned."
    *
    * The C Driver chooses the first option, and returns an empty cursor.
    */
   if (_is_aggregate_out (&pipeline)) {
      const bson_t *doc;
      bson_error_t error;
      bson_value_t value;

      ASSERT (!mongoc_cursor_next (cursor, &doc));
      if (mongoc_cursor_error_document (cursor, &error, &doc)) {
         value_init_from_doc (&value, doc);
         check_result (test, operation, false, &value, &error);
         bson_value_destroy (&value);
      }
   } else {
      check_cursor (cursor, test, operation);
   }

   mongoc_cursor_destroy (cursor);
   bson_destroy (&opts);

   return true;
}


static bool
db_aggregate (mongoc_database_t *db,
              const bson_t *test,
              const bson_t *operation,
              mongoc_client_session_t *session,
              const mongoc_read_prefs_t *read_prefs,
              bson_t *reply)
{
   bson_t args;
   bson_t pipeline;
   bson_t opts = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (&args, "pipeline", &pipeline);
   append_session (session, &opts);
   COPY_EXCEPT ("pipeline");

   cursor = mongoc_database_aggregate (db, &pipeline, &opts, read_prefs);

   /* Driver CRUD API Spec: "$out is a special pipeline stage that causes no
    * results to be returned from the server. As such, the iterable here would
    * never contain documents. Drivers MAY setup a cursor to be executed upon
    * iteration against the $out collection such that if a user were to iterate
    * a pipeline including $out, results would be returned."
    *
    * The C Driver chooses the first option, and returns an empty cursor.
    */
   if (_is_aggregate_out (&pipeline)) {
      const bson_t *doc;
      bson_error_t error;
      bson_value_t value;

      ASSERT (!mongoc_cursor_next (cursor, &doc));
      if (mongoc_cursor_error_document (cursor, &error, &doc)) {
         value_init_from_doc (&value, doc);
         check_result (test, operation, false, &value, &error);
         bson_value_destroy (&value);
      }
   } else {
      check_cursor (cursor, test, operation);
   }

   mongoc_cursor_destroy (cursor);
   bson_destroy (&opts);

   return true;
}


static bool
command (mongoc_database_t *db,
         const bson_t *test,
         const bson_t *operation,
         mongoc_client_session_t *session,
         const mongoc_read_prefs_t *read_prefs,
         bson_t *reply)
{
   bson_t cmd;
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bool r;
   bson_value_t value;

   /* arguments are like:
    *
    *   arguments:
    *     session: session0
    *     command:
    *       find: *collection_name
    *     readConcern:
    *       level: majority
    */
   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (&args, "command", &cmd);
   COPY_EXCEPT ("command");
   append_session (session, &opts);

   r = mongoc_database_command_with_opts (db, &cmd, read_prefs, &opts, reply, &error);

   value_init_from_doc (&value, reply);
   check_result (test, operation, r, &value, &error);
   bson_value_destroy (&value);
   bson_destroy (&opts);
   bson_destroy (&cmd);

   return r;
}


static bool
start_transaction (mongoc_client_session_t *session, const bson_t *test, const bson_t *operation, bson_t *reply)
{
   mongoc_transaction_opt_t *opts = NULL;
   bson_error_t error;
   bool r;

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   if (bson_has_field (operation, "arguments.options")) {
      opts = bson_lookup_txn_opts (operation, "arguments.options");
   }

   r = mongoc_client_session_start_transaction (session, opts, &error);
   check_result (test, operation, r, NULL, &error);

   if (opts) {
      mongoc_transaction_opts_destroy (opts);
   }

   return r;
}


static bool
commit_transaction (mongoc_client_session_t *session, const bson_t *test, const bson_t *operation, bson_t *reply)
{
   bson_value_t value;
   bson_error_t error;
   bool r;

   r = mongoc_client_session_commit_transaction (session, reply, &error);
   value_init_from_doc (&value, reply);
   check_result (test, operation, r, &value, &error);
   bson_value_destroy (&value);

   return r;
}


static bool
abort_transaction (mongoc_client_session_t *session, const bson_t *test, const bson_t *operation, bson_t *reply)
{
   bson_value_t value;
   bson_error_t error;
   bool r;

   r = mongoc_client_session_abort_transaction (session, &error);
   /* fake a reply for the test framework's sake */
   bson_init (reply);
   value_init_from_doc (&value, reply);
   check_result (test, operation, r, &value, &error);
   bson_value_destroy (&value);

   return r;
}


static bool
list_databases (mongoc_client_t *client,
                const bson_t *test,
                const bson_t *operation,
                mongoc_client_session_t *session,
                bson_t *reply)
{
   mongoc_cursor_t *cursor;
   bson_t opts;

   ASSERT (client);
   bson_init (&opts);
   append_session (session, &opts);

   cursor = mongoc_client_find_databases_with_opts (client, &opts);
   bson_destroy (&opts);

   check_cursor (cursor, test, operation);
   mongoc_cursor_destroy (cursor);
   bson_init (reply);
   return true;
}


static bool
list_database_names (mongoc_client_t *client,
                     const bson_t *test,
                     const bson_t *operation,
                     mongoc_client_session_t *session,
                     bson_t *reply)
{
   char **database_names;
   bson_t opts;
   bson_error_t error;

   ASSERT (client);
   bson_init (&opts);
   append_session (session, &opts);

   database_names = mongoc_client_get_database_names_with_opts (client, &opts, &error);
   bson_destroy (&opts);

   check_result (test, operation, database_names != NULL, NULL /* result */, &error);
   bson_init (reply);
   bson_strfreev (database_names);
   return true;
}


static bool
list_indexes (mongoc_collection_t *collection,
              const bson_t *test,
              const bson_t *operation,
              mongoc_client_session_t *session,
              bson_t *reply)
{
   bson_t opts;
   mongoc_cursor_t *cursor;

   BSON_ASSERT (collection);

   bson_init (&opts);
   append_session (session, &opts);

   cursor = mongoc_collection_find_indexes_with_opts (collection, &opts);
   check_cursor (cursor, test, operation);
   mongoc_cursor_destroy (cursor);
   bson_init (reply);
   bson_destroy (&opts);
   return true;
}


static bool
list_collections (
   mongoc_database_t *db, const bson_t *test, const bson_t *operation, mongoc_client_session_t *session, bson_t *reply)
{
   mongoc_cursor_t *cursor;
   bson_t opts;

   bson_init (&opts);
   append_session (session, &opts);

   cursor = mongoc_database_find_collections_with_opts (db, &opts);

   bson_destroy (&opts);

   check_cursor (cursor, test, operation);
   mongoc_cursor_destroy (cursor);
   bson_init (reply);

   return true;
}

static bool
list_collection_names (
   mongoc_database_t *db, const bson_t *test, const bson_t *operation, mongoc_client_session_t *session, bson_t *reply)
{
   char **collection_names;
   bson_t opts;
   bson_error_t error;

   bson_init (&opts);
   append_session (session, &opts);

   collection_names = mongoc_database_get_collection_names_with_opts (db, &opts, &error);

   bson_destroy (&opts);

   check_result (test, operation, collection_names != NULL, NULL /* result */, &error);
   bson_init (reply);
   bson_strfreev (collection_names);

   return true;
}


static bool
gridfs_download (mongoc_database_t *db,
                 const bson_t *test,
                 const bson_t *operation,
                 mongoc_client_session_t *session,
                 const mongoc_read_prefs_t *read_prefs,
                 bson_t *reply)
{
   mongoc_gridfs_bucket_t *bucket;
   bson_value_t value;
   bson_error_t error;
   mongoc_stream_t *stream;
   char buf[512];

   BSON_UNUSED (test);
   BSON_UNUSED (session);

   bson_lookup_value (operation, "arguments.id", &value);

   bucket = mongoc_gridfs_bucket_new (db, NULL, read_prefs, &error);
   stream = mongoc_gridfs_bucket_open_download_stream (bucket, &value, &error);

   if (stream != NULL) {
      mongoc_stream_read (stream, buf, 1, 1, 0);
   }

   mongoc_stream_destroy (stream);
   mongoc_gridfs_bucket_destroy (bucket);
   bson_init (reply);

   return true;
}


static bool
create_collection (
   mongoc_database_t *db, const bson_t *test, const bson_t *operation, mongoc_client_session_t *session, bson_t *reply)
{
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   mongoc_collection_t *collection;
   const char *collection_name;

   BSON_ASSERT (db);

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   append_session (session, &opts);

   bson_lookup_doc (operation, "arguments", &args);
   collection_name = bson_lookup_utf8 (&args, "collection");
   COPY_EXCEPT ("collection");

   collection = mongoc_database_create_collection (db, collection_name, &opts, &error);

   bson_destroy (&opts);

   check_result (test, operation, collection != NULL, NULL, &error);

   if (collection) {
      mongoc_collection_destroy (collection);
   }

   return true;
}


static bool
drop_collection (
   mongoc_database_t *db, const bson_t *test, const bson_t *operation, mongoc_client_session_t *session, bson_t *reply)
{
   bson_t args;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bool r;
   const char *collection_name;
   mongoc_collection_t *collection;

   BSON_ASSERT (db);

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   append_session (session, &opts);

   bson_lookup_doc (operation, "arguments", &args);
   collection_name = bson_lookup_utf8 (&args, "collection");
   COPY_EXCEPT ("collection");

   collection = mongoc_database_get_collection (db, collection_name);

   r = mongoc_collection_drop_with_opts (collection, &opts, &error);

   bson_destroy (&opts);
   mongoc_collection_destroy (collection);

   check_result (test, operation, r, NULL, &error);

   return true;
}


static bool
create_index (mongoc_collection_t *collection,
              const bson_t *test,
              const bson_t *operation,
              mongoc_client_session_t *session,
              bson_t *reply)
{
   bson_t args;
   bson_t keys;
   const char *name;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bool r;

   BSON_ASSERT (collection);

   /* We don't use reply, but we need to initialize it for the test runner */
   bson_init (reply);

   append_session (session, &opts);

   bson_lookup_doc (operation, "arguments", &args);
   bson_lookup_doc (&args, "keys", &keys);
   name = bson_lookup_utf8 (&args, "name");
   COPY_EXCEPT ("keys", "name");

   mongoc_index_model_t *im = mongoc_index_model_new (&keys, tmp_bson ("{'name': '%s'}", name));
   r = mongoc_collection_create_indexes_with_opts (collection, &im, 1, &opts, NULL /* reply */, &error);
   mongoc_index_model_destroy (im);

   bson_destroy (&opts);

   check_result (test, operation, r, NULL, &error);

   return true;
}


static bool
collection_exists (mongoc_client_t *client, const bson_t *operation)
{
   char **names;
   bson_t args;
   const char *database_name;
   const char *collection_name;
   bson_error_t error;
   mongoc_database_t *db;
   bool found = false;
   uint32_t i;

   ASSERT (client);

   bson_lookup_doc (operation, "arguments", &args);
   database_name = bson_lookup_utf8 (&args, "database");
   collection_name = bson_lookup_utf8 (&args, "collection");

   db = mongoc_client_get_database (client, database_name);

   names = mongoc_database_get_collection_names_with_opts (db, NULL, &error);
   if (!names) {
      test_error ("expected error checking if collection '%s' exists: %s", collection_name, error.message);
   }

   for (i = 0; names && names[i] != NULL; i++) {
      if (!strcmp (names[i], collection_name)) {
         found = true;
      }
   }

   mongoc_database_destroy (db);
   bson_strfreev (names);

   return found;
}


static bool
index_exists (mongoc_client_t *client, const bson_t *operation)
{
   bson_t args;
   const char *database_name;
   const char *collection_name;
   const char *index_name;
   mongoc_collection_t *collection;
   mongoc_cursor_t *indexes;
   bson_iter_t index;
   const bson_t *doc;
   bool found = false;

   ASSERT (client);

   bson_lookup_doc (operation, "arguments", &args);
   database_name = bson_lookup_utf8 (&args, "database");
   collection_name = bson_lookup_utf8 (&args, "collection");
   index_name = bson_lookup_utf8 (&args, "index");

   collection = mongoc_client_get_collection (client, database_name, collection_name);

   indexes = mongoc_collection_find_indexes_with_opts (collection, NULL);

   while (mongoc_cursor_next (indexes, &doc)) {
      if (bson_iter_init (&index, doc) && bson_iter_find (&index, "name") && BSON_ITER_HOLDS_UTF8 (&index) &&
          !strcmp (bson_iter_utf8 (&index, NULL), index_name)) {
         found = true;
      }
   }

   mongoc_cursor_destroy (indexes);
   mongoc_collection_destroy (collection);

   return found;
}

static uint32_t
_get_total_pool_cleared_event (json_test_ctx_t *ctx)
{
   uint32_t total = 0;
   mc_shared_tpld td = mc_tpld_take_ref (ctx->client->topology);

   /* Go get total generation counts. */
   for (size_t i = 0u; i < mc_tpld_servers_const (td.ptr)->items_len; i++) {
      const mongoc_server_description_t *sd;

      sd = mongoc_set_get_item_const (mc_tpld_servers_const (td.ptr), i);
      total += mc_tpl_sd_get_generation (sd, &kZeroServiceId);
   }
   mc_tpld_drop_ref (&td);
   return total;
}

#define WAIT_FOR_EVENT_TIMEOUT_MS 10 * 1000
#define WAIT_FOR_EVENT_TICK_MS 10

static void
wait_for_event (json_test_ctx_t *ctx, const bson_t *operation)
{
   bool satisfied = false;
   int64_t measured = 0;
   int64_t total = 0;

   const char *const event_name = bson_lookup_utf8 (operation, "arguments.event");
   const int32_t count = bson_lookup_int32 (operation, "arguments.count");
   const int64_t expires_us = bson_get_monotonic_time () + WAIT_FOR_EVENT_TIMEOUT_MS * 1000;

   while (!satisfied && bson_get_monotonic_time () < expires_us) {
      if (0 == strcmp (event_name, "ServerMarkedUnknownEvent")) {
         bson_mutex_lock (&ctx->mutex);
         measured = ctx->measured_ServerMarkedUnknownEvent;
         total = ctx->total_ServerMarkedUnknownEvent;
         const int64_t diff = total - measured;
         if (diff >= count) {
            /* "count" events were accounted for in the test. There may be more
             * later. */
            ctx->measured_ServerMarkedUnknownEvent += count;
            satisfied = true;
         }
         bson_mutex_unlock (&ctx->mutex);
      } else if (0 == strcmp (event_name, "PoolClearedEvent")) {
         bson_mutex_lock (&ctx->mutex);
         total = (int64_t) _get_total_pool_cleared_event (ctx);
         measured = ctx->measured_PoolClearedEvent;
         const int64_t diff = total - measured;
         if (diff >= count) {
            /* "count" events were accounted for in the test. There may be more
             * later. */
            ctx->measured_PoolClearedEvent += count;
            satisfied = true;
         }
         bson_mutex_unlock (&ctx->mutex);
      } else {
         test_error ("Unknown event: %s", event_name);
      }
      if (!satisfied) {
         _mongoc_usleep (WAIT_FOR_EVENT_TICK_MS * 1000);
      }
   }

   if (!satisfied) {
      test_error ("did not see enough %s events after 10s. %" PRId64 " total occurred. %" PRId64
                  " accounted for. But %" PRId32 " more expected",
                  event_name,
                  total,
                  measured,
                  count);
   }
}

static void
wait_for_primary_change (json_test_ctx_t *ctx, const bson_t *operation)
{
   bool satisfied = false;
   int64_t measured = 0;
   int64_t total = 0;

   const int32_t timeout_ms = bson_lookup_int32 (operation, "arguments.timeoutMS");
   const int64_t expires_us = bson_get_monotonic_time () + timeout_ms * 1000;

   while (!satisfied && bson_get_monotonic_time () < expires_us) {
      bson_mutex_lock (&ctx->mutex);

      total = ctx->total_PrimaryChangedEvent;
      measured = ctx->measured_PrimaryChangedEvent;
      const int64_t diff = total - measured;
      if (diff >= 1) {
         /* 1 event accounted for. There may be more later. */
         ctx->measured_PrimaryChangedEvent++;
         satisfied = true;
      }

      bson_mutex_unlock (&ctx->mutex);
      if (!satisfied) {
         _mongoc_usleep (10 * 1000);
      }
   }

   if (!satisfied) {
      test_error ("did not see any primary change events after %" PRId32 "ms. %" PRId64 " total occurred. "
                  "%" PRId64 " accounted for.",
                  timeout_ms,
                  total,
                  measured);
   }
}

static void
assert_event_count (json_test_ctx_t *ctx, const bson_t *operation)
{
   int64_t total = 0;

   const char *const event_name = bson_lookup_utf8 (operation, "arguments.event");
   const int32_t count = bson_lookup_int32 (operation, "arguments.count");

   if (0 == strcmp (event_name, "ServerMarkedUnknownEvent")) {
      total = ctx->total_ServerMarkedUnknownEvent;
   } else if (0 == strcmp (event_name, "PoolClearedEvent")) {
      total = (int64_t) _get_total_pool_cleared_event (ctx);
   } else {
      test_error ("Unknown event: %s", event_name);
   }

   if (count != total) {
      test_error ("event count %s mismatched. Expected %" PRId32 ", but have %" PRId64, event_name, count, total);
   }
}

static void
assert_session_transaction_state (json_test_ctx_t *ctx, const bson_t *operation)
{
   const char *expected_state;
   const mongoc_client_session_t *session;
   mongoc_transaction_state_t state;
   char *state_strings[] = {"none", "starting", "in progress", "committed", "aborted"};

   expected_state = bson_lookup_utf8 (operation, "arguments.state");
   session = session_from_name (ctx, bson_lookup_utf8 (operation, "arguments.session"));
   state = mongoc_client_session_get_transaction_state (session);

   if (!strcmp (expected_state, "none")) {
      if (state != MONGOC_TRANSACTION_NONE) {
         test_error ("expected session transaction state none, but have %s", state_strings[state]);
      }
   } else if (!strcmp (expected_state, "starting")) {
      if (state != MONGOC_TRANSACTION_STARTING) {
         test_error ("expected session transaction state starting, but have %s", state_strings[state]);
      }
   } else if (!strcmp (expected_state, "in_progress")) {
      if (state != MONGOC_TRANSACTION_IN_PROGRESS) {
         test_error ("expected session transaction state in progress, but have %s", state_strings[state]);
      }
   } else if (!strcmp (expected_state, "committed")) {
      if (state != MONGOC_TRANSACTION_COMMITTED) {
         test_error ("expected session transaction state committed, but have %s", state_strings[state]);
      }
   } else if (!strcmp (expected_state, "aborted")) {
      if (state != MONGOC_TRANSACTION_ABORTED) {
         test_error ("expected session transaction state aborted, but have %s", state_strings[state]);
      }
   } else {
      test_error ("unrecognized state %s for assertSessionTransactionState", expected_state);
   }
}

static bool
op_error (const bson_t *operation)
{
   return bson_has_field (operation, "error") && bson_lookup_bool (operation, "error");
}

bool
json_test_operation (json_test_ctx_t *ctx,
                     const bson_t *test,
                     const bson_t *operation,
                     mongoc_collection_t *collection,
                     mongoc_client_session_t *session,
                     bson_t *reply)
{
   const char *op_name;
   const char *obj_name = "collection";
   mongoc_read_prefs_t *read_prefs = NULL;
   mongoc_write_concern_t *wc;
   mongoc_database_t *db = mongoc_database_copy (ctx->db);
   mongoc_collection_t *c = mongoc_collection_copy (collection);
   bool res = false;

   op_name = bson_lookup_utf8 (operation, "name");

   if (bson_has_field (operation, "object")) {
      obj_name = bson_lookup_utf8 (operation, "object");
   }

   if (bson_has_field (operation, "databaseOptions")) {
      bson_lookup_database_opts (operation, "databaseOptions", db);
   }

   if (bson_has_field (operation, "collectionOptions")) {
      bson_lookup_collection_opts (operation, "collectionOptions", c);
   }

   if (bson_has_field (operation, "read_preference")) {
      /* command monitoring tests */
      read_prefs = bson_lookup_read_prefs (operation, "read_preference");
   } else if (bson_has_field (operation, "arguments.readPreference")) {
      /* transactions tests */
      read_prefs = bson_lookup_read_prefs (operation, "arguments.readPreference");
   }

   if (bson_has_field (operation, "arguments.writeConcern")) {
      wc = bson_lookup_write_concern (operation, "arguments.writeConcern");
   } else {
      wc = mongoc_write_concern_new ();
   }

   if (!strcmp (obj_name, "collection")) {
      if (!strcmp (op_name, "bulkWrite")) {
         res = bulk_write (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "deleteOne") || !strcmp (op_name, "deleteMany") || !strcmp (op_name, "insertOne") ||
                 !strcmp (op_name, "replaceOne") || !strcmp (op_name, "updateOne") || !strcmp (op_name, "updateMany")) {
         res = single_write (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "findOneAndDelete") || !strcmp (op_name, "findOneAndReplace") ||
                 !strcmp (op_name, "findOneAndUpdate")) {
         res = find_and_modify (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "insertMany")) {
         res = insert_many (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "rename")) {
         res = rename_op (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "drop")) {
         res = drop (c, test, operation, session, wc, reply);
      } else if (!strcmp (op_name, "count")) {
         res = count (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "estimatedDocumentCount")) {
         res = count (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "countDocuments")) {
         res = count (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "distinct")) {
         res = distinct (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "find")) {
         res = find (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "findOne")) {
         res = find_one (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "aggregate")) {
         res = aggregate (c, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "listIndexes") || !strcmp (op_name, "listIndexNames")) {
         res = list_indexes (c, test, operation, session, reply);
      } else if (!strcmp (op_name, "watch")) {
         bson_t pipeline = BSON_INITIALIZER;
         mongoc_change_stream_destroy (ctx->change_stream);
         ctx->change_stream = mongoc_collection_watch (c, &pipeline, NULL);
         res = (op_error (operation) == (0 != ctx->change_stream->err.code));
         if (!res) {
            test_error ("expected error=%s, but actual error='%s'",
                        op_error (operation) ? "true" : "false",
                        ctx->change_stream->err.message);
         }

         bson_init (reply);
         bson_destroy (&pipeline);
      } else if (!strcmp (op_name, "mapReduce")) {
         test_error ("operation not implemented in libmongoc");
      } else if (!strcmp (op_name, "createIndex")) {
         res = create_index (c, test, operation, session, reply);
      } else {
         test_error ("unrecognized collection operation name %s", op_name);
      }
   } else if (!strcmp (obj_name, "database")) {
      if (!strcmp (op_name, "aggregate")) {
         res = db_aggregate (db, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "runCommand")) {
         res = command (db, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "listCollections")) {
         res = list_collections (db, test, operation, session, reply);
      } else if (!strcmp (op_name, "listCollectionNames")) {
         res = list_collection_names (db, test, operation, session, reply);
      } else if (!strcmp (op_name, "watch")) {
         bson_t pipeline = BSON_INITIALIZER;
         mongoc_change_stream_destroy (ctx->change_stream);
         ctx->change_stream = mongoc_database_watch (db, &pipeline, NULL);
         res = (op_error (operation) == (0 != ctx->change_stream->err.code));
         if (!res) {
            test_error ("expected error=%s, but actual error='%s'",
                        op_error (operation) ? "true" : "false",
                        ctx->change_stream->err.message);
         }

         bson_init (reply);
         bson_destroy (&pipeline);
      } else if (!strcmp (op_name, "listCollectionObjects")) {
         test_error ("listCollectionObjects is not implemented in libmongoc");
      } else if (!strcmp (op_name, "createCollection")) {
         create_collection (db, test, operation, session, reply);
      } else if (!strcmp (op_name, "dropCollection")) {
         drop_collection (db, test, operation, session, reply);
      } else {
         test_error ("unrecognized database operation name %s", op_name);
      }
   } else if (!strncmp (obj_name, "session", 7)) {
      mongoc_client_session_t *named_session = session_from_name (ctx, obj_name);

      if (!strcmp (op_name, "startTransaction")) {
         res = start_transaction (named_session, test, operation, reply);
      } else if (!strcmp (op_name, "commitTransaction")) {
         res = commit_transaction (named_session, test, operation, reply);
      } else if (!strcmp (op_name, "abortTransaction")) {
         res = abort_transaction (named_session, test, operation, reply);
      } else if (!strcmp (op_name, "endSession")) {
         mongoc_client_session_destroy (named_session);
         if (0 == strcmp (obj_name, "session0")) {
            ctx->sessions[0] = NULL;
         } else if (0 == strcmp (obj_name, "session1")) {
            ctx->sessions[1] = NULL;
         } else {
            test_error ("unrecognized session: %s", op_name);
         }
         res = true;
         bson_init (reply);
      } else {
         test_error ("unrecognized session operation name %s", op_name);
      }
   } else if (!strcmp (obj_name, "testRunner")) {
      /* We don't use reply, but we need to initialize it for the test runner */
      bson_init (reply);
      if (!strcmp (op_name, "assertSessionPinned")) {
         res = (0 != mongoc_client_session_get_server_id (session));
      } else if (!strcmp (op_name, "assertSessionUnpinned")) {
         res = (0 == mongoc_client_session_get_server_id (session));
      } else if (!strcmp (op_name, "targetedFailPoint")) {
         mongoc_client_t *client;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         activate_fail_point (client, session->server_id, operation, "arguments.failPoint");

         mongoc_client_destroy (client);
      } else if (!strcmp (op_name, "assertSessionNotDirty")) {
         BSON_ASSERT (!session->server_session->dirty);
      } else if (!strcmp (op_name, "assertSessionDirty")) {
         BSON_ASSERT (session->server_session->dirty);
      } else if (!strcmp (op_name, "assertSameLsidOnLastTwoCommands")) {
         if (!ctx->sent_lsids[0] || !ctx->sent_lsids[1]) {
            test_error ("attempting to check last two session IDs, but test "
                        "runner has not captured them");
         }
         BSON_ASSERT (bson_equal (ctx->sent_lsids[0], ctx->sent_lsids[1]));
      } else if (!strcmp (op_name, "assertDifferentLsidOnLastTwoCommands")) {
         if (!ctx->sent_lsids[0] || !ctx->sent_lsids[1]) {
            test_error ("attempting to check last two session IDs, but test "
                        "runner has not captured them");
         }
         BSON_ASSERT (!bson_equal (ctx->sent_lsids[0], ctx->sent_lsids[1]));
      } else if (!strcmp (op_name, "assertCollectionExists")) {
         mongoc_client_t *client;
         bool exists;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         exists = collection_exists (client, operation);
         mongoc_client_destroy (client);

         BSON_ASSERT (exists);
      } else if (!strcmp (op_name, "assertCollectionNotExists")) {
         mongoc_client_t *client;
         bool exists;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         exists = collection_exists (client, operation);
         mongoc_client_destroy (client);

         BSON_ASSERT (!exists);
      } else if (!strcmp (op_name, "assertIndexExists")) {
         mongoc_client_t *client;
         bool exists;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         exists = index_exists (client, operation);
         mongoc_client_destroy (client);

         BSON_ASSERT (exists);
      } else if (!strcmp (op_name, "assertIndexNotExists")) {
         mongoc_client_t *client;
         bool exists;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         exists = index_exists (client, operation);
         mongoc_client_destroy (client);

         BSON_ASSERT (!exists);
      } else if (!strcmp (op_name, "waitForEvent")) {
         wait_for_event (ctx, operation);
      } else if (!strcmp (op_name, "assertEventCount")) {
         assert_event_count (ctx, operation);
      } else if (!strcmp (op_name, "configureFailPoint")) {
         mongoc_client_t *client;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         activate_fail_point (client, 0 /* primary */, operation, "arguments.failPoint");

         mongoc_client_destroy (client);
      } else if (!strcmp (op_name, "wait")) {
         _mongoc_usleep (bson_lookup_int32 (operation, "arguments.ms") * 1000);
      } else if (!strcmp (op_name, "recordPrimary")) {
         /* It doesn't matter who the primary is. We just want to assert in
          * tests later that the primary changed x times after this operation.
          */
         bson_mutex_lock (&ctx->mutex);
         ctx->measured_PrimaryChangedEvent = ctx->total_PrimaryChangedEvent;
         bson_mutex_unlock (&ctx->mutex);
      } else if (!strcmp (op_name, "runAdminCommand")) {
         mongoc_client_t *client;
         mongoc_database_t *admin_db;

         client = test_framework_client_new_from_uri (ctx->client->uri, NULL);
         test_framework_set_ssl_opts (client);
         admin_db = mongoc_client_get_database (client, "admin");
         bson_destroy (reply);
         res = command (admin_db, test, operation, session, read_prefs, reply);
         if (!res) {
            test_error ("admin command failed: %s", bson_as_json (reply, NULL));
         }
         mongoc_database_destroy (admin_db);
         mongoc_client_destroy (client);
      } else if (!strcmp (op_name, "waitForPrimaryChange")) {
         wait_for_primary_change (ctx, operation);
      } else if (!strcmp (op_name, "startThread")) {
         json_test_worker_thread_t *wt;

         wt = thread_from_name (ctx, bson_lookup_utf8 (operation, "arguments.name"));
         start_thread (wt);
      } else if (!strcmp (op_name, "waitForThread")) {
         json_test_worker_thread_t *wt;

         wt = thread_from_name (ctx, bson_lookup_utf8 (operation, "arguments.name"));
         wait_for_thread (wt);
      } else if (!strcmp (op_name, "runOnThread")) {
         json_test_worker_thread_t *wt;
         bson_t op;

         bson_lookup_doc (operation, "arguments.operation", &op);
         wt = thread_from_name (ctx, bson_lookup_utf8 (operation, "arguments.name"));
         run_on_thread (wt, &op);
      } else if (!strcmp (op_name, "assertSessionTransactionState")) {
         assert_session_transaction_state (ctx, operation);
      } else {
         test_error ("unrecognized testRunner operation name %s", op_name);
      }
   } else if (!strcmp (obj_name, "client")) {
      if (!strcmp (op_name, "listDatabases")) {
         res = list_databases (c->client, test, operation, session, reply);
      } else if (!strcmp (op_name, "listDatabaseNames")) {
         res = list_database_names (c->client, test, operation, session, reply);
      } else if (!strcmp (op_name, "watch")) {
         bson_t pipeline = BSON_INITIALIZER;
         mongoc_change_stream_destroy (ctx->change_stream);
         ctx->change_stream = mongoc_client_watch (c->client, &pipeline, NULL);
         res = (op_error (operation) == (0 != ctx->change_stream->err.code));
         if (!res) {
            test_error ("expected error=%s, but actual error='%s'",
                        op_error (operation) ? "true" : "false",
                        ctx->change_stream->err.message);
         }

         bson_init (reply);
         bson_destroy (&pipeline);
      } else if (!strcmp (op_name, "listDatabaseObjects")) {
         test_error ("listDatabaseObjects is not implemented in libmongoc");
      } else {
         test_error ("unrecognized client operation name %s", op_name);
      }
   } else if (!strcmp (obj_name, "gridfsbucket")) {
      if (!strcmp (op_name, "download")) {
         res = gridfs_download (db, test, operation, session, read_prefs, reply);
      } else if (!strcmp (op_name, "download_by_name")) {
         test_error ("download_by_name is part of the optional advanced API "
                     "and not implemented in libmongoc");
      } else {
         test_error ("unrecognized gridfs operation name %s", op_name);
      }
   } else {
      test_error ("unrecognized object name %s", obj_name);
   }

   mongoc_read_prefs_destroy (read_prefs);
   mongoc_write_concern_destroy (wc);
   mongoc_collection_destroy (c);
   mongoc_database_destroy (db);

   return res;
}


static void
one_operation (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   const char *op_name;
   mongoc_write_concern_t *wc = NULL;

   op_name = bson_lookup_utf8 (operation, "name");
   if (ctx->verbose) {
      char *op_str;

      op_str = bson_as_json (operation, NULL);
      MONGOC_DEBUG ("     running operation %s : %s\n", op_name, op_str);
      bson_free (op_str);
   }

   if (bson_has_field (operation, "arguments.writeConcern")) {
      wc = bson_lookup_write_concern (operation, "arguments.writeConcern");
   } else if (bson_has_field (operation, "collectionOptions.writeConcern")) {
      wc = bson_lookup_write_concern (operation, "collectionOptions.writeConcern");
   }

   if (wc) {
      ctx->acknowledged = mongoc_write_concern_is_acknowledged (wc);
      mongoc_write_concern_destroy (wc);
   } else {
      ctx->acknowledged = true;
   }

   if (ctx->config->run_operation_cb) {
      ctx->config->run_operation_cb (ctx, test, operation);
   } else {
      test_error ("set json_test_config_t.run_operation_cb to a callback"
                  " that executes json_test_operation()");
   }
}


void
json_test_operations (json_test_ctx_t *ctx, const bson_t *test)
{
   bson_t operations;
   bson_t operation;
   bson_iter_t iter;

   /* run each CRUD operation in the test, using the config's run-operation
    * callback, by default json_test_operation(). retryable writes tests have
    * one operation each, transactions tests have an array of them. */
   if (bson_has_field (test, "operation")) {
      bson_lookup_doc (test, "operation", &operation);
      one_operation (ctx, test, &operation);
   } else {
      bson_lookup_doc (test, "operations", &operations);
      BSON_ASSERT (bson_iter_init (&iter, &operations));
      while (bson_iter_next (&iter)) {
         bson_iter_bson (&iter, &operation);
         one_operation (ctx, test, &operation);
      }
   }
}
