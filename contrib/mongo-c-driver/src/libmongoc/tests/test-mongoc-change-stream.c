/*
 * Copyright 2017-present MongoDB, Inc.
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
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mongoc/mongoc-change-stream-private.h"
#include "mongoc/mongoc-cursor-private.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"
#include "json-test.h"
#include "json-test-operations.h"

#define DESTROY_CHANGE_STREAM(cursor_id)                                                                              \
   do {                                                                                                               \
      future_t *_future = future_change_stream_destroy (stream);                                                      \
      request_t *_request = mock_server_receives_msg (server,                                                         \
                                                      MONGOC_MSG_NONE,                                                \
                                                      tmp_bson ("{'$db': 'db',"                                       \
                                                                " 'killCursors': 'coll',"                             \
                                                                " 'cursors': [{'$numberLong': '" #cursor_id "'}]}")); \
      reply_to_request_simple (_request, "{ 'cursorsKilled': [ " #cursor_id " ] }");                                  \
      future_wait (_future);                                                                                          \
      future_destroy (_future);                                                                                       \
      request_destroy (_request);                                                                                     \
   } while (0)


typedef struct _data_change_stream_t {
   mongoc_cursor_response_t response;
   bson_t post_batch_resume_token;
} _data_change_stream_t;

static void
_setup_for_resume (mongoc_change_stream_t *stream)
{
   bool ret;
   bson_error_t error;
   mongoc_client_t *client;
   const char *cmd;

   client = stream->cursor->client;
   if (stream->max_wire_version >= WIRE_VERSION_4_4) {
      cmd = "{'configureFailPoint': 'failGetMoreAfterCursorCheckout', 'mode': "
            "{ 'times': 1 }, 'data': { 'errorCode': 6 }}";
   } else {
      cmd = "{'configureFailPoint': 'failCommand', 'mode': { 'times': 1 }, "
            "'data': { 'failCommands': ['getMore'], 'errorCode': 6 }}";
   }
   ret = mongoc_client_command_simple_with_server_id (
      client, "admin", tmp_bson (cmd), NULL /* read prefs */, stream->cursor->server_id, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
}


static int
test_framework_skip_if_not_single_version_5 (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   return (!test_framework_is_replset () && !test_framework_is_mongos ()) ? 1 : 0;
}

static mongoc_collection_t *
drop_and_get_coll (mongoc_client_t *client, const char *db_name, const char *coll_name)
{
   ASSERT (client);

   mongoc_collection_t *coll = mongoc_client_get_collection (client, db_name, coll_name);
   mongoc_collection_drop (coll, NULL);
   return coll;
}

/* From Change Streams Spec tests:
 * "$changeStream must be the first stage in a change stream pipeline sent
 * to the server" */
static void
test_change_stream_pipeline (void)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;
   bson_t *nonempty_pipeline = tmp_bson ("{ 'pipeline' : [ { '$project' : { 'ns': false } } ] }");

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   ASSERT (client);

   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT (coll);

   future = future_collection_watch (coll, tmp_bson ("{}"), NULL);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'aggregate': 'coll',"
                                                 " 'pipeline': [{'$changeStream': {}}],"
                                                 " 'cursor': {}}"));

   reply_to_request_simple (request, "{'cursor' : {'id': 123, 'ns': 'db.coll', 'firstBatch': []}, 'ok': 1 }");

   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);

   future_destroy (future);
   request_destroy (request);

   future = future_change_stream_next (stream, &next_doc);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'coll'}"));
   reply_to_request_simple (request, "{'cursor' : { 'nextBatch' : [] }, 'ok': 1}");
   ASSERT (!future_get_bool (future));
   ASSERT (!mongoc_change_stream_error_document (stream, NULL, NULL));
   ASSERT (next_doc == NULL);
   future_destroy (future);
   request_destroy (request);

   /* Another call to next should produce another getMore */
   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'coll'}"));
   reply_to_request_simple (request, "{ 'cursor': { 'nextBatch': [] }, 'ok': 1 }");
   ASSERT (!future_get_bool (future));
   ASSERT (!mongoc_change_stream_error_document (stream, NULL, NULL));
   ASSERT (next_doc == NULL);
   future_destroy (future);
   request_destroy (request);

   DESTROY_CHANGE_STREAM (123);

   /* Test non-empty pipeline */
   future = future_collection_watch (coll, nonempty_pipeline, NULL);

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'aggregate': 'coll',"
                                                 " 'pipeline': ["
                                                 "   {'$changeStream': {}},"
                                                 "   {'$project': {'ns': false}}],"
                                                 " 'cursor': {}}"));
   reply_to_request_simple (request, "{'cursor': {'id': 123, 'ns': 'db.coll','firstBatch': []},'ok': 1}");

   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);

   future_destroy (future);
   request_destroy (request);


   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'coll'}"));
   reply_to_request_simple (request, "{ 'cursor': { 'nextBatch': [] }, 'ok': 1 }");
   ASSERT (!future_get_bool (future));
   ASSERT (!mongoc_change_stream_error_document (stream, NULL, NULL));
   ASSERT (next_doc == NULL);
   future_destroy (future);
   request_destroy (request);

   DESTROY_CHANGE_STREAM (123);

   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
   mock_server_destroy (server);
}

/* From Change Streams Spec tests:
 * "The watch helper must not throw a custom exception when executed against a
 * single server topology, but instead depend on a server error"
 */
static void
test_change_stream_live_single_server (void *test_ctx)
{
   /* Temporarily skip on arm64 until mongod tested against is updated */
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   bson_error_t error;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;
   const bson_t *reported_err_doc = NULL;
   const char *not_replset_doc = "{'errmsg': 'The $changeStream stage is "
                                 "only supported on replica sets', 'code': "
                                 "40573, 'ok': 0}";

   /* Don't use the errmsg field since it contains quotes. */
   const char *not_supported_doc = "{'code' : 40324, 'ok' : 0 }";

   BSON_UNUSED (test_ctx);

   ASSERT (client);

   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &error), error);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), NULL);
   ASSERT (stream);

   ASSERT (mongoc_change_stream_error_document (stream, NULL, &reported_err_doc));
   ASSERT (next_doc == NULL);

   if (test_framework_max_wire_version_at_least (6)) {
      ASSERT_MATCH (reported_err_doc, not_replset_doc);
   } else {
      ASSERT_MATCH (reported_err_doc, not_supported_doc);
      ASSERT_CONTAINS (bson_lookup_utf8 (reported_err_doc, "errmsg"), "Unrecognized pipeline stage");
   }

   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}


typedef struct _test_resume_token_ctx_t {
   bool expecting_resume_token;
   const bson_t *expected_resume_token_bson;
} test_resume_token_ctx_t;

static void
test_resume_token_command_start (const mongoc_apm_command_started_t *event)
{
   const bson_t *cmd = mongoc_apm_command_started_get_command (event);
   const char *cmd_name = mongoc_apm_command_started_get_command_name (event);

   test_resume_token_ctx_t *ctx = (test_resume_token_ctx_t *) mongoc_apm_command_started_get_context (event);

   if (strcmp (cmd_name, "aggregate") == 0) {
      if (ctx->expecting_resume_token) {
         char *rt_pattern = bson_as_canonical_extended_json (ctx->expected_resume_token_bson, NULL);
         char *pattern = bson_strdup_printf ("{'aggregate': 'coll_resume', 'pipeline': "
                                             "[{'$changeStream': { 'resumeAfter': %s }}]}",
                                             rt_pattern);
         ASSERT_MATCH (cmd, pattern);
         bson_free (pattern);
         bson_free (rt_pattern);
      } else {
         ASSERT_MATCH (cmd,
                       "{'aggregate': 'coll_resume', 'pipeline': [{ "
                       "'$changeStream': { 'resumeAfter': { '$exists': "
                       "false } }}]}");
      }
   }
}

/* From Change Streams Spec tests:
 * "ChangeStream must continuously track the last seen resumeToken"
 * Note: we should not inspect the resume token, since the format may change.
 */
static void
test_change_stream_live_track_resume_token (void *test_ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_error_t error;
   test_resume_token_ctx_t ctx = {0};
   const bson_t *next_doc = NULL;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   bson_t doc0_rt, doc1_rt, doc2_rt;
   const bson_t *resume_token;

   BSON_UNUSED (test_ctx);

   client = test_framework_new_default_client ();
   ASSERT (client);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_resume_token_command_start);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);

   coll = drop_and_get_coll (client, "db", "coll_resume");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &error), error);

   /* Set the batch size to 1 so we only get one document per call to next. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"));
   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   /* Insert a few docs to listen for. Use write concern majority, so subsequent
    * call to watch will be guaranteed to retrieve them. */
   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 0}"), &opts, NULL, &error), error);

   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 1}"), &opts, NULL, &error), error);

   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 2}"), &opts, NULL, &error), error);

   /* The resume token should be updated to the most recently iterated doc */
   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   bson_copy_to (resume_token, &doc0_rt);

   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   ASSERT (bson_compare (resume_token, &doc0_rt) != 0);
   bson_copy_to (resume_token, &doc1_rt);

   _setup_for_resume (stream);
   /* A resume will occur, forcing a resumeAfter token in the aggregate cmd. */

   ctx.expecting_resume_token = true;
   ctx.expected_resume_token_bson = &doc1_rt;
   ASSERT (mongoc_change_stream_next (stream, &next_doc));

   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   ASSERT (bson_compare (resume_token, &doc0_rt) != 0);
   ASSERT (bson_compare (resume_token, &doc1_rt) != 0);
   bson_copy_to (resume_token, &doc2_rt);

   /* There are no docs left. But the next call should still keep the same
    * resume token */
   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);
   ASSERT (!next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   ASSERT (bson_compare (resume_token, &doc2_rt) == 0);

   bson_destroy (&doc0_rt);
   bson_destroy (&doc1_rt);
   bson_destroy (&doc2_rt);
   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}

typedef struct _test_batch_size_ctx {
   uint32_t num_get_mores;
   uint32_t expected_getmore_batch_size;
   uint32_t expected_agg_batch_size;
} test_batch_size_ctx_t;

static void
test_batch_size_command_succeeded (const mongoc_apm_command_succeeded_t *event)
{
   const bson_t *reply = mongoc_apm_command_succeeded_get_reply (event);
   const char *cmd_name = mongoc_apm_command_succeeded_get_command_name (event);

   test_batch_size_ctx_t *ctx = (test_batch_size_ctx_t *) mongoc_apm_command_succeeded_get_context (event);

   if (strcmp (cmd_name, "getMore") == 0) {
      bson_t next_batch;
      ++ctx->num_get_mores;
      bson_lookup_doc (reply, "cursor.nextBatch", &next_batch);
      ASSERT (bson_count_keys (&next_batch) == ctx->expected_getmore_batch_size);
   } else if (strcmp (cmd_name, "aggregate") == 0) {
      bson_t first_batch;
      bson_lookup_doc (reply, "cursor.firstBatch", &first_batch);
      ASSERT (bson_count_keys (&first_batch) == ctx->expected_agg_batch_size);
   }
}
/* Test that the batch size option applies to both the initial aggregate and
 * subsequent getMore commands.
 */
static void
test_change_stream_live_batch_size (void *test_ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   test_batch_size_ctx_t ctx = {0};
   const bson_t *next_doc = NULL;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   bson_error_t err;
   uint32_t i;

   BSON_UNUSED (test_ctx);

   client = test_framework_new_default_client ();
   ASSERT (client);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_succeeded_cb (callbacks, test_batch_size_command_succeeded);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);

   coll = drop_and_get_coll (client, "db", "coll_batch");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &err), err);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"));
   ASSERT (stream);

   ctx.expected_agg_batch_size = 0;
   ctx.expected_getmore_batch_size = 0;

   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT (!mongoc_change_stream_error_document (stream, NULL, NULL));
   ASSERT (next_doc == NULL);

   ctx.expected_getmore_batch_size = 1;

   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   for (i = 0; i < 10; i++) {
      bson_t *doc = BCON_NEW ("_id", BCON_INT32 (i));
      ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, doc, &opts, NULL, &err), err);
      bson_destroy (doc);
   }

   ctx.expected_getmore_batch_size = 1;
   for (i = 0; i < 10; i++) {
      mongoc_change_stream_next (stream, &next_doc);
   }

   ctx.expected_getmore_batch_size = 0;
   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);
   ASSERT (next_doc == NULL);

   /* 10 getMores for results, 1 for initial next, 1 for last empty next */
   ASSERT (ctx.num_get_mores == 12);

   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}


/* From Change Streams Spec tests:
 * "ChangeStream will throw an exception if the server response is missing the
 * resume token." In the C driver case, return an error.
 */
static void
_test_resume_token_error (const char *id_projection)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   const bson_t *next_doc = NULL;
   mongoc_change_stream_t *stream;
   bson_error_t err;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;

   client = test_framework_new_default_client ();
   ASSERT (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);

   coll = drop_and_get_coll (client, "db", "coll_missing_resume");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &err), err);

   stream =
      mongoc_collection_watch (coll, tmp_bson ("{'pipeline': [{'$project': {'_id': %s }}]}", id_projection), NULL);

   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);

   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 2}"), &opts, NULL, &err), err);

   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT (mongoc_change_stream_error_document (stream, &err, NULL));

   /* Newer server versions emit different errors. */
   if (!test_framework_max_wire_version_at_least (8)) {
      ASSERT_ERROR_CONTAINS (
         err, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CHANGE_STREAM_NO_RESUME_TOKEN, "Cannot provide resume functionality");
   } else {
      ASSERT_ERROR_CONTAINS (err,
                             MONGOC_ERROR_SERVER,
                             280,
                             "Only transformations that retain the unmodified "
                             "_id field are allowed.");
   }

   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}

static void
test_change_stream_live_missing_resume_token (void *test_ctx)
{
   BSON_UNUSED (test_ctx);

   _test_resume_token_error ("0");
}

static void
test_change_stream_live_invalid_resume_token (void *test_ctx)
{
   BSON_UNUSED (test_ctx);

   /* test a few non-document BSON types */
   _test_resume_token_error ("{'$literal': 1}");
   _test_resume_token_error ("{'$literal': true}");
   _test_resume_token_error ("{'$literal': 'foo'}");
   _test_resume_token_error ("{'$literal': []}");
}

static void
_test_getmore_error (const char *server_reply, bool should_resume, bool resume_kills_cursor)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   coll = mongoc_client_get_collection (client, "db", "coll");
   future = future_collection_watch (coll, tmp_bson ("{}"), NULL);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'aggregate': 'coll'}"));
   reply_to_request_simple (request, "{'cursor': {'id': 123, 'ns': 'db.coll','firstBatch': []},'ok': 1 }");
   stream = future_get_mongoc_change_stream_ptr (future);
   BSON_ASSERT (stream);
   future_destroy (future);
   request_destroy (request);

   /* the first getMore receives an error. */
   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'coll'}"));
   reply_to_request_simple (request, server_reply);
   request_destroy (request);
   if (should_resume) {
      /* client should retry the aggregate. */
      if (resume_kills_cursor) {
         /* errors that are considered "not primary" or "node is recovering"
          * errors by SDAM will mark the connected server as UNKNOWN, and no
          * killCursors will be executed. */
         request =
            mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'killCursors': 'coll'}"));
         reply_to_request_simple (request, "{'cursorsKilled': [123]}");
         request_destroy (request);
      }
      request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'aggregate': 'coll'}"));
      reply_to_request_simple (request,
                               "{'cursor':"
                               "  {'id': 124,"
                               "   'ns': 'db.coll',"
                               "   'firstBatch':"
                               "    [{'_id': {'resume': 'doc'}}]},"
                               "'ok': 1}");
      request_destroy (request);
      BSON_ASSERT (future_get_bool (future));
      BSON_ASSERT (!mongoc_change_stream_error_document (stream, NULL, NULL));
      DESTROY_CHANGE_STREAM (124);
   } else {
      BSON_ASSERT (!future_get_bool (future));
      BSON_ASSERT (mongoc_change_stream_error_document (stream, NULL, NULL));
      DESTROY_CHANGE_STREAM (123);
   }
   future_destroy (future);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Test a variety of resumable and non-resumable errors that may be returned
 * from a getMore. */
static void
test_getmore_errors (void)
{
   _test_getmore_error (
      "{'ok': 0, 'code': 1, 'errmsg': 'internal error'}", false /* should_resume */, false /* ignored */);
   _test_getmore_error (
      "{'ok': 0, 'code': 6, 'errmsg': 'host unreachable'}", true /* should_resume */, true /* resume_kills_cursor */);
   _test_getmore_error (
      "{'ok': 0, 'code': 12345, 'errmsg': 'random error'}", false /* should_resume */, false /* ignored */);
   /* most error codes are resumable, excluding a few deny listed ones. */
   _test_getmore_error (
      "{'ok': 0, 'code': 11601, 'errmsg': 'interrupted'}", false /* should_resume */, false /* ignored */);
   _test_getmore_error (
      "{'ok': 0, 'code': 136, 'errmsg': 'capped position lost'}", false /* should_resume */, false /* ignored */);
   _test_getmore_error (
      "{'ok': 0, 'code': 237, 'errmsg': 'cursor killed'}", false /* should_resume */, false /* ignored */);
   _test_getmore_error ("{'ok': 0, 'errmsg': 'random error'}", false /* should_resume */, false /* ignored */);
   /* Even an error with a 'NonResumableChangeStreamError' label will resume if
    * it is on the allow list. */
   _test_getmore_error ("{'ok': 0, 'code': 6, 'errorLabels': "
                        "['NonResumableChangeStreamError'], 'errmsg': 'host unreachable'}",
                        true /* should_resume */,
                        true /* resume_kills_cursor */);
   _test_getmore_error ("{'ok': 0, 'code': 6, 'errorLabels': "
                        "['NonRetryableChangeStreamError'], 'errmsg': 'host unreachable'}",
                        true /* should_resume */,
                        true /* resume_kills_cursor */);
}
/* From Change Streams Spec tests:
 * "ChangeStream will automatically resume one time on a resumable error
 * (including not primary) with the initial pipeline and options, except for the
 * addition/update of a resumeToken"
 * "The killCursors command sent during the “Resume Process” must not be
 * allowed to throw an exception."
 */
static void
test_change_stream_resumable_error (void)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   mongoc_uri_t *uri;
   bson_error_t err;
   const bson_t *err_doc = NULL;
   const bson_t *next_doc = NULL;
   const char *not_primary_err = "{ 'code': 10107, 'errmsg': 'not primary', 'ok': 0 }";
   const char *interrupted_err = "{ 'code': 11601, 'errmsg': 'interrupted', 'ok': 0 }";
   const bson_t *watch_cmd = tmp_bson ("{'$db': 'db',"
                                       " 'aggregate': 'coll',"
                                       " 'pipeline': [{'$changeStream': {}}],"
                                       " 'cursor': {}}");
   const char *expected_msg = "{'$db': 'db', 'getMore': {'$numberLong': '%d'}, 'collection': 'coll' }";

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "socketTimeoutMS", 100);
   client = test_framework_client_new_from_uri (uri, NULL);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "db", "coll");

   future = future_collection_watch (coll, tmp_bson ("{}"), NULL);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);

   reply_to_request_simple (request,
                            "{'cursor': {'id': 123, 'ns': "
                            "'db.coll','firstBatch': []},'ok': 1 "
                            "}");

   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);

   future_destroy (future);
   request_destroy (request);

   /* Test that a network hangup results in a resumable error */
   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 123));
   BSON_ASSERT (request);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Retry command */
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);
   BSON_ASSERT (request);
   reply_to_request_simple (request, "{'cursor': {'id': 124,'ns': 'db.coll','firstBatch': []},'ok': 1 }");
   request_destroy (request);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 124));
   reply_to_request_simple (request, "{ 'cursor': { 'nextBatch': [] }, 'ok': 1 }");
   request_destroy (request);
   ASSERT (!future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);
   ASSERT (next_doc == NULL);
   future_destroy (future);

   /* Test the "not_primary" resumable error occurring twice in a row */
   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 124));
   reply_to_request_simple (request, not_primary_err);
   request_destroy (request);

   /* Retry command */
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);
   reply_to_request_simple (request, "{'cursor': {'id': 125, 'ns': 'db.coll','firstBatch': []},'ok': 1}");
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 125));
   reply_to_request_simple (request, not_primary_err);
   request_destroy (request);

   /* Retry command */
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);
   reply_to_request_simple (request,
                            "{'cursor': {'id': 126, 'ns': "
                            "'db.coll','firstBatch': []},'ok': 1 "
                            "}");
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 126));
   reply_to_request_simple (request, interrupted_err);
   request_destroy (request);

   /* Check that error is returned */
   ASSERT (!future_get_bool (future));
   ASSERT (mongoc_change_stream_error_document (stream, &err, &err_doc));
   ASSERT (next_doc == NULL);
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_SERVER, 11601, "interrupted");
   ASSERT_MATCH (err_doc, interrupted_err);
   future_destroy (future);
   DESTROY_CHANGE_STREAM (126);

   /* Test an error on the initial aggregate when resuming. */
   future = future_collection_watch (coll, tmp_bson ("{}"), NULL);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);
   reply_to_request_simple (request,
                            "{'cursor': {'id': 123, 'ns': "
                            "'db.coll','firstBatch': []},'ok': 1 "
                            "}");
   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);
   request_destroy (request);
   future_destroy (future);

   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_msg, 123));
   reply_to_request_simple (request, "{ 'code': 10107, 'errmsg': 'not primary', 'ok': 0 }");
   request_destroy (request);

   /* Retry command */
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, watch_cmd);
   reply_to_request_simple (request, "{'code': 123, 'errmsg': 'bad cmd', 'ok': 0}");
   request_destroy (request);

   /* Check that error is returned */
   ASSERT (!future_get_bool (future));
   ASSERT (mongoc_change_stream_error_document (stream, &err, &err_doc));
   ASSERT (next_doc == NULL);
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_SERVER, 123, "bad cmd");
   ASSERT_MATCH (err_doc, "{'code': 123, 'errmsg': 'bad cmd', 'ok': 0}");
   future_destroy (future);

   mongoc_change_stream_destroy (stream);
   mongoc_uri_destroy (uri);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Test that options are sent correctly.
 */
static void
test_change_stream_options (void)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;
   bson_error_t err;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   ASSERT (client);

   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT (coll);


   /*
    * fullDocument: 'default'|'updateLookup', passed to $changeStream stage
    * resumeAfter: optional<Doc>, passed to $changeStream stage
    * startAfter: optional<Doc>, passed to $changeStream stage
    * startAtOperationTime: optional<Timestamp>, passed to $changeStream stage
    * maxAwaitTimeMS: Optional<Int64>, passed to cursor
    * batchSize: Optional<Int32>, passed as agg option, {cursor: { batchSize: }}
    * collation: Optional<Document>, passed as agg option
    */

   /* fullDocument */
   future = future_collection_watch (coll,
                                     tmp_bson ("{}"),
                                     tmp_bson ("{ 'fullDocument': 'updateLookup', "
                                               "'resumeAfter': {'resume': 'after'}, "
                                               "'startAfter': {'start': 'after'}, "
                                               "'startAtOperationTime': { '$timestamp': { 't': 1, 'i': 1 }}, "
                                               "'maxAwaitTimeMS': 5000, 'batchSize': "
                                               "5, 'collation': { 'locale': 'en' }}"));

   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'aggregate': 'coll',"
                                                 " 'pipeline': ["
                                                 "   {'$changeStream': {"
                                                 "     'fullDocument': 'updateLookup',"
                                                 "     'resumeAfter': {'resume': 'after'},"
                                                 "     'startAfter': {'start': 'after'},"
                                                 "     'startAtOperationTime': {'$timestamp': { 't': 1, 'i': 1 }}}}],"
                                                 " 'cursor': {'batchSize': 5},"
                                                 " 'collation': {'locale': 'en'}}"));

   reply_to_request_simple (request, "{'cursor': {'id': 123,'ns': 'db.coll','firstBatch': []},'ok': 1 }");

   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);
   future_destroy (future);
   request_destroy (request);

   future = future_change_stream_next (stream, &next_doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'db',"
                                                 " 'getMore': {'$numberLong': '123'},"
                                                 " 'collection': 'coll',"
                                                 " 'maxTimeMS': {'$numberLong': '5000'},"
                                                 " 'batchSize': {'$numberLong': '5'}}"));
   reply_to_request_simple (request, "{ 'cursor': { 'nextBatch': [] }, 'ok': 1 }");
   request_destroy (request);
   ASSERT (!future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);
   ASSERT (next_doc == NULL);
   future_destroy (future);

   DESTROY_CHANGE_STREAM (123);

   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Test basic watch functionality and validate the server documents */
static void
test_change_stream_live_watch (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   bson_t *inserted_doc = tmp_bson ("{ 'x': 'y'}");
   const bson_t *next_doc = NULL;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   bson_error_t err;

   BSON_UNUSED (test_ctx);

   mongoc_write_concern_set_wmajority (wc, 30000);

   coll = drop_and_get_coll (client, "db", "coll_watch");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &err), err);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), NULL);
   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);

   /* Test that inserting a doc produces the expected change stream doc */
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, inserted_doc, &opts, NULL, &err), err);

   ASSERT (mongoc_change_stream_next (stream, &next_doc));

   /* Validation rules as follows:
    * { _id: <present>, operationType: "insert", ns: <doc>, documentKey:
    * <present>,
    *   updateDescription: <missing>, fullDocument: <inserted doc> }
    */
   ASSERT_HAS_FIELD (next_doc, "_id");
   ASSERT (!strcmp (bson_lookup_utf8 (next_doc, "operationType"), "insert"));

   ASSERT_MATCH (next_doc,
                 "{ '_id': { '$exists': true },'operationType': 'insert', 'ns': "
                 "{ 'db': 'db', 'coll': 'coll_watch' },'documentKey': { "
                 "'$exists': true }, 'updateDescription': { '$exists': false }, "
                 "'fullDocument': { '_id': { '$exists': true }, 'x': 'y' }}");

   /* Test updating a doc */
   ASSERT_OR_PRINT (
      mongoc_collection_update_one (coll, tmp_bson ("{}"), tmp_bson ("{'$set': {'x': 'z'} }"), &opts, NULL, &err), err);

   ASSERT (mongoc_change_stream_next (stream, &next_doc));

   ASSERT_MATCH (next_doc,
                 "{ '_id': { '$exists': true },'operationType': 'update', 'ns': { 'db': "
                 "'db', 'coll': 'coll_watch' },'documentKey': { '$exists': "
                 "true }, 'updateDescription': { 'updatedFields': { 'x': 'z' } "
                 "}, 'fullDocument': { '$exists': false }}");

   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

/* From Change Streams Spec tests:
 * "ChangeStream will resume after a killCursors command is issued for its child
 * cursor."
 * "ChangeStream will perform server selection before attempting to resume,
 * using initial readPreference"
 */
static void
test_change_stream_live_read_prefs (void *test_ctx)
{
   /*
    - connect with secondary read preference
    - verify we are connected to a secondary
    - issue a killCursors to trigger a resume
    - after resume, check that the cursor connected to a secondary
    */

   mongoc_read_prefs_t *prefs;
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   mongoc_cursor_t *raw_cursor;
   const bson_t *next_doc = NULL;
   bson_error_t err;
   uint64_t first_cursor_id;

   BSON_UNUSED (test_ctx);

   coll = drop_and_get_coll (client, "db", "coll_read_prefs");
   ASSERT (coll);
   ASSERT_OR_PRINT (
      mongoc_collection_insert_one (coll,
                                    tmp_bson (NULL),
                                    tmp_bson ("{'writeConcern': {'w': %d}}", test_framework_data_nodes_count ()),
                                    NULL,
                                    &err),
      err);

   prefs = mongoc_read_prefs_copy (mongoc_collection_get_read_prefs (coll));
   mongoc_read_prefs_set_mode (prefs, MONGOC_READ_SECONDARY);
   mongoc_collection_set_read_prefs (coll, prefs);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), NULL);
   ASSERT (stream);
   mongoc_change_stream_next (stream, &next_doc);

   raw_cursor = stream->cursor;
   ASSERT (raw_cursor);

   ASSERT (test_framework_server_is_secondary (client, raw_cursor->server_id));
   first_cursor_id = mongoc_cursor_get_id (raw_cursor);

   /* Call next to create the cursor, should return no documents. */
   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);

   _setup_for_resume (stream);

   /* Change stream client will resume with another cursor. */
   /* depending on the server version, this may or may not receive another
    * document on resume */
   (void) mongoc_change_stream_next (stream, &next_doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, &next_doc), err);

   raw_cursor = stream->cursor;
   ASSERT (first_cursor_id != mongoc_cursor_get_id (raw_cursor));
   ASSERT (test_framework_server_is_secondary (client, raw_cursor->server_id));

   mongoc_read_prefs_destroy (prefs);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

/* Test that a failed server selection returns an error. This verifies a bug
 * is fixed, which would trigger an assert in this case. */
static void
test_change_stream_server_selection_fails (void)
{
   const bson_t *bson;
   bson_error_t err;
   mongoc_client_t *client = test_framework_client_new ("mongodb://localhost:12345/", NULL);
   mongoc_collection_t *coll = mongoc_client_get_collection (client, "test", "test");
   mongoc_change_stream_t *cs = mongoc_collection_watch (coll, tmp_bson ("{}"), NULL);

   mongoc_change_stream_next (cs, &bson);
   BSON_ASSERT (mongoc_change_stream_error_document (cs, &err, &bson));
   ASSERT_ERROR_CONTAINS (
      err, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "No servers yet eligible for rescan");
   mongoc_change_stream_destroy (cs);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

/* Test calling next on a change stream which errors after construction. This
 * verifies a bug is fixed, which would try to access a NULL cursor. */
static void
test_change_stream_next_after_error (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *bson;
   bson_error_t err;

   BSON_UNUSED (test_ctx);

   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &err), err);
   stream = mongoc_collection_watch (coll, tmp_bson ("{'pipeline': ['invalid_stage']}"), NULL);
   BSON_ASSERT (!mongoc_change_stream_next (stream, &bson));
   BSON_ASSERT (mongoc_change_stream_error_document (stream, &err, &bson));
   BSON_ASSERT (err.domain == MONGOC_ERROR_SERVER);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

typedef struct {
   char *pattern;
   int agg_count;
} array_started_ctx_t;

static void
_accepts_array_started (const mongoc_apm_command_started_t *event)
{
   const bson_t *cmd = mongoc_apm_command_started_get_command (event);
   const char *cmd_name = mongoc_apm_command_started_get_command_name (event);
   array_started_ctx_t *ctx = (array_started_ctx_t *) mongoc_apm_command_started_get_context (event);
   if (strcmp (cmd_name, "aggregate") != 0) {
      return;
   }
   ctx->agg_count++;
   ASSERT_MATCH (cmd, ctx->pattern);
}

/* Test that watch accepts an array document {0: {}, 1: {}} as the pipeline,
 * similar to mongoc_collection_aggregate */
static void
test_change_stream_accepts_array (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_apm_callbacks_t *callbacks = mongoc_apm_callbacks_new ();
   array_started_ctx_t ctx = {0};
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *bson;
   bson_error_t err;
   bson_t *opts = tmp_bson ("{'maxAwaitTimeMS': 1}"); /* to speed up the test. */

   BSON_UNUSED (test_ctx);

   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   /* set up apm callbacks to listen for the agg commands. */
   ctx.pattern = bson_strdup ("{'aggregate': 'coll', 'pipeline': [ {'$changeStream': {}}, "
                              "{'$match': {'x': 1}}, {'$project': {'x': 1}}]}");
   mongoc_apm_set_command_started_cb (callbacks, _accepts_array_started);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &err), err);
   /* try starting a change stream with a { "pipeline": [...] } argument */
   stream =
      mongoc_collection_watch (coll, tmp_bson ("{'pipeline': [{'$match': {'x': 1}}, {'$project': {'x': 1}}]}"), opts);
   (void) mongoc_change_stream_next (stream, &bson);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, &bson), err);
   ASSERT_CMPINT32 (ctx.agg_count, ==, 1);
   mongoc_change_stream_destroy (stream);
   /* try with an array like document. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{'0': {'$match': {'x': 1}}, '1': {'$project': {'x': 1}}}"), opts);
   (void) mongoc_change_stream_next (stream, &bson);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, &bson), err);
   ASSERT_CMPINT32 (ctx.agg_count, ==, 2);
   mongoc_change_stream_destroy (stream);
   /* try with malformed { "pipeline": [...] } argument. */
   bson_free (ctx.pattern);
   ctx.pattern = bson_strdup ("{'aggregate': 'coll', 'pipeline': [ {'$changeStream': {}}, 42 ]}");
   stream = mongoc_collection_watch (coll, tmp_bson ("{'pipeline': [42] }"), NULL);
   (void) mongoc_change_stream_next (stream, &bson);
   BSON_ASSERT (mongoc_change_stream_error_document (stream, &err, &bson));
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_SERVER, 14, "Each element of the 'pipeline' array must be an object");
   ASSERT_CMPINT32 (ctx.agg_count, ==, 3);
   mongoc_change_stream_destroy (stream);
   /* try with malformed array doc argument. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{'0': 42 }"), NULL);
   (void) mongoc_change_stream_next (stream, &bson);
   BSON_ASSERT (mongoc_change_stream_error_document (stream, &err, &bson));
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_SERVER, 14, "Each element of the 'pipeline' array must be an object");
   ASSERT_CMPINT32 (ctx.agg_count, ==, 4);
   mongoc_change_stream_destroy (stream);
   bson_free (ctx.pattern);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

/* A simple test that passing 'startAtOperationTime' does not error. */
void
test_change_stream_start_at_operation_time (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *doc;
   bson_t opts;
   mongoc_client_session_t *session;
   bson_error_t error;

   BSON_UNUSED (test_ctx);

   session = mongoc_client_start_session (client, NULL, &error);
   coll = mongoc_client_get_collection (client, "db", "coll");
   bson_init (&opts);
   ASSERT_OR_PRINT (mongoc_client_session_append (session, &opts, &error), error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), &opts, NULL, &error), error);
   BSON_APPEND_TIMESTAMP (&opts, "startAtOperationTime", session->operation_timestamp, session->operation_increment);
   stream = mongoc_collection_watch (coll, tmp_bson ("{'pipeline': []}"), &opts);

   (void) mongoc_change_stream_next (stream, &doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_client_session_destroy (session);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

typedef struct {
   bool has_initiated;
   bool has_resumed;
   bson_t agg_reply;
} resume_ctx_t;

#define RESUME_INITIALIZER           \
   {                                 \
      false, false, BSON_INITIALIZER \
   }

static void
_resume_at_optime_started (const mongoc_apm_command_started_t *event)
{
   resume_ctx_t *ctx;

   ctx = (resume_ctx_t *) mongoc_apm_command_started_get_context (event);
   if (0 != strcmp (mongoc_apm_command_started_get_command_name (event), "aggregate")) {
      return;
   }

   if (!ctx->has_initiated) {
      ctx->has_initiated = true;
      return;
   }

   ctx->has_resumed = true;

   /* postBatchResumeToken (MongoDB 4.0.7+) supersedes operationTime. Since
    * test_change_stream_resume_at_optime runs for wire version 7+, decide
    * whether to skip operationTime assertion based on the command reply. */
   if (!bson_has_field (&ctx->agg_reply, "cursor.postBatchResumeToken")) {
      bson_value_t replied_optime, sent_optime;
      match_ctx_t match_ctx = {{0}};

      /* it should re-use the same optime on resume. */
      bson_lookup_value (&ctx->agg_reply, "operationTime", &replied_optime);
      bson_lookup_value (
         mongoc_apm_command_started_get_command (event), "pipeline.0.$changeStream.startAtOperationTime", &sent_optime);
      BSON_ASSERT (replied_optime.value_type == BSON_TYPE_TIMESTAMP);
      BSON_ASSERT (match_bson_value (&sent_optime, &replied_optime, &match_ctx));
      bson_value_destroy (&sent_optime);
      bson_value_destroy (&replied_optime);
   }
}

static void
_resume_at_optime_succeeded (const mongoc_apm_command_succeeded_t *event)
{
   resume_ctx_t *ctx;

   ctx = (resume_ctx_t *) mongoc_apm_command_succeeded_get_context (event);
   if (!strcmp (mongoc_apm_command_succeeded_get_command_name (event), "aggregate")) {
      bson_destroy (&ctx->agg_reply);
      bson_copy_to (mongoc_apm_command_succeeded_get_reply (event), &ctx->agg_reply);
   }
}

/* Test that "operationTime" in aggregate reply is used on resume */
static void
test_change_stream_resume_at_optime (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *doc;
   bson_error_t error;
   mongoc_apm_callbacks_t *callbacks;
   resume_ctx_t ctx = RESUME_INITIALIZER;

   BSON_UNUSED (test_ctx);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, _resume_at_optime_started);
   mongoc_apm_set_command_succeeded_cb (callbacks, _resume_at_optime_succeeded);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
   coll = mongoc_client_get_collection (client, "db", "coll");
   stream = mongoc_collection_watch (coll, tmp_bson ("{'pipeline': []}"), NULL);

   _setup_for_resume (stream);
   (void) mongoc_change_stream_next (stream, &doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);
   BSON_ASSERT (ctx.has_initiated);
   BSON_ASSERT (ctx.has_resumed);

   bson_destroy (&ctx.agg_reply);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_destroy (client);
}

static void
_resume_with_post_batch_resume_token_started (const mongoc_apm_command_started_t *event)
{
   resume_ctx_t *ctx;

   ctx = (resume_ctx_t *) mongoc_apm_command_started_get_context (event);
   if (0 != strcmp (mongoc_apm_command_started_get_command_name (event), "aggregate")) {
      return;
   }

   if (!ctx->has_initiated) {
      ctx->has_initiated = true;
      return;
   }

   ctx->has_resumed = true;

   /* postBatchResumeToken is available since MongoDB 4.0.7, but the test runs
    * for wire version 7+. Decide whether to skip postBatchResumeToken assertion
    * based on the command reply. */
   if (bson_has_field (&ctx->agg_reply, "cursor.postBatchResumeToken")) {
      bson_value_t replied_pbrt, sent_pbrt;
      match_ctx_t match_ctx = {{0}};

      /* it should re-use the same postBatchResumeToken on resume. */
      bson_lookup_value (&ctx->agg_reply, "cursor.postBatchResumeToken", &replied_pbrt);
      bson_lookup_value (
         mongoc_apm_command_started_get_command (event), "pipeline.0.$changeStream.resumeAfter", &sent_pbrt);
      BSON_ASSERT (replied_pbrt.value_type == BSON_TYPE_DOCUMENT);
      BSON_ASSERT (match_bson_value (&sent_pbrt, &replied_pbrt, &match_ctx));
      bson_value_destroy (&sent_pbrt);
      bson_value_destroy (&replied_pbrt);
   }
}

static void
_resume_with_post_batch_resume_token_succeeded (const mongoc_apm_command_succeeded_t *event)
{
   resume_ctx_t *ctx;

   ctx = (resume_ctx_t *) mongoc_apm_command_succeeded_get_context (event);
   if (!strcmp (mongoc_apm_command_succeeded_get_command_name (event), "aggregate")) {
      bson_destroy (&ctx->agg_reply);
      bson_copy_to (mongoc_apm_command_succeeded_get_reply (event), &ctx->agg_reply);
   }
}

/* Test that "postBatchResumeToken" in aggregate reply is used on resume */
static void
test_change_stream_resume_with_post_batch_resume_token (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *doc;
   bson_error_t error;
   mongoc_apm_callbacks_t *callbacks;
   resume_ctx_t ctx = RESUME_INITIALIZER;

   BSON_UNUSED (test_ctx);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, _resume_with_post_batch_resume_token_started);
   mongoc_apm_set_command_succeeded_cb (callbacks, _resume_with_post_batch_resume_token_succeeded);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
   coll = mongoc_client_get_collection (client, "db", "coll");
   stream = mongoc_collection_watch (coll, tmp_bson ("{'pipeline': []}"), NULL);

   _setup_for_resume (stream);
   (void) mongoc_change_stream_next (stream, &doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);
   BSON_ASSERT (ctx.has_initiated);
   BSON_ASSERT (ctx.has_resumed);

   bson_destroy (&ctx.agg_reply);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_destroy (client);
}

/* A simple test of database watch. */
void
test_change_stream_database_watch (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_database_t *db;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *doc;
   bson_t opts;
   bson_error_t error;

   BSON_UNUSED (test_ctx);

   db = mongoc_client_get_database (client, "db");
   bson_init (&opts);

   stream = mongoc_database_watch (db, tmp_bson ("{}"), NULL);

   coll = mongoc_database_get_collection (db, "coll");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), &opts, NULL, &error), error);

   (void) mongoc_change_stream_next (stream, &doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_database_destroy (db);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

/* A simple test of client watch. */
void
test_change_stream_client_watch (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *doc;
   bson_t opts;
   bson_error_t error;

   BSON_UNUSED (test_ctx);

   bson_init (&opts);

   stream = mongoc_client_watch (client, tmp_bson ("{}"), NULL);

   coll = mongoc_client_get_collection (client, "db", "coll");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), &opts, NULL, &error), error);

   (void) mongoc_change_stream_next (stream, &doc);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


static int
_skip_if_rs_version_less_than (const char *version)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }
   if (!test_framework_skip_if_not_replset ()) {
      return 0;
   }
   if (test_framework_get_server_version () >= test_framework_str_to_version (version)) {
      return 1;
   }
   return 0;
}

static int
_skip_if_no_client_watch (void)
{
   return _skip_if_rs_version_less_than ("3.8.0");
}

static int
_skip_if_no_db_watch (void)
{
   return _skip_if_rs_version_less_than ("3.8.0");
}

static int
_skip_if_no_start_at_optime (void)
{
   return _skip_if_rs_version_less_than ("3.8.0");
}

static void
_test_resume (const char *opts,
              const char *expected_change_stream_opts,
              const char *first_doc,
              const char *expected_resume_change_stream_opts,
              const char *cursor_pbr)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_error_t err;
   char *msg;
   const bson_t *doc = NULL;

   server = mock_server_with_auto_hello (WIRE_VERSION_4_0);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "db", "coll");
   future = future_collection_watch (coll, tmp_bson ("{}"), tmp_bson (opts));
   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline' : [ { '$changeStream': { %s "
                                                 " 'fullDocument': null } } ], 'cursor': {  } }",
                                                 expected_change_stream_opts));
   msg = bson_strdup_printf ("{'cursor': {'id': 123, 'ns': 'db.coll',"
                             "'firstBatch': [%s]%s }, 'operationTime': "
                             "{ '$timestamp': {'t': 1, 'i': 2} }, 'ok': 1 }",
                             first_doc,
                             cursor_pbr);
   reply_to_request_simple (request, msg);
   bson_free (msg);
   stream = future_get_mongoc_change_stream_ptr (future);
   BSON_ASSERT (stream);
   future_destroy (future);
   request_destroy (request);
   /* if a first document was returned, the first call to next returns it. */
   if (*first_doc) {
      mongoc_change_stream_next (stream, &doc);
      ASSERT_MATCH (doc, first_doc);
   }
   future = future_change_stream_next (stream, &doc);
   request = mock_server_receives_msg (
      server, MONGOC_QUERY_NONE, tmp_bson ("{ 'getMore': {'$numberLong': '123'}, 'collection': 'coll' }"));
   reply_to_request_with_hang_up (request);
   request_destroy (request);
   /* since the server closed the connection, a resume is attempted. */
   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline' : [ { '$changeStream': { %s "
                                                 " 'fullDocument': null }} ], 'cursor': {  } }",
                                                 expected_resume_change_stream_opts));
   reply_to_request_simple (request, "{'cursor': {'id': 0,'ns': 'db.coll','firstBatch': []},'ok': 1 }");
   request_destroy (request);

   BSON_ASSERT (!future_get_bool (future));
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &err, NULL), err);
   BSON_ASSERT (doc == NULL);
   future_destroy (future);

   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


/* test resume behavior before and after the first document is received. */
static void
test_resume_cases (void)
{
#define NO_OPT_RA "'resumeAfter': {'$exists': false}"
#define NO_OPT_SA "'startAfter': {'$exists': false}"
#define NO_OPT_OP "'startAtOperationTime': {'$exists': false}"
#define AGG_OP "'startAtOperationTime': {'$timestamp': {'t': 1, 'i': 2}}"
#define DOC "{'_id': {'resume': 'doc'}}"
#define OPT_OP "'startAtOperationTime': {'$timestamp': {'t': 111, 'i': 222}}"
#define DOC_RA "'resumeAfter': {'resume': 'doc'}"
#define OPT_RA "'resumeAfter': {'resume': 'opt'}"
#define OPT_SA "'startAfter': {'resume': 'opt'}"

   /* test features:
    * - whether the change stream returns a document before resuming.
    * - whether 'startAtOperationTime' is specified
    * - whether 'resumeAfter' is specified
    * - whether 'startAfterAfter' is specified */

   /* no options specified. */
   /* - if no doc recv'ed, use the operationTime returned by aggregate. */
   _test_resume ("{}", NO_OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", "", AGG_OP "," NO_OPT_RA "," NO_OPT_SA ",", "");
   /* - if doc recv'ed and iterated, use the doc's resume token. */
   _test_resume ("{}", NO_OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", DOC, DOC_RA "," NO_OPT_OP "," NO_OPT_SA ",", "");

   /* only 'startAtOperationTime' specified. */
   /* - if no doc recv'ed, use the startAtOperationTime option. */
   _test_resume (
      "{" OPT_OP "}", OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", "", OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", "");
   /* - if doc recv'ed and iterated, use the doc's resume token. */
   _test_resume (
      "{" OPT_OP "}", OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", DOC, DOC_RA "," NO_OPT_OP "," NO_OPT_SA ",", "");

   /* only 'resumeAfter' specified. */
   /* - if no doc recv'ed, use the resumeAfter option. */
   _test_resume (
      "{" OPT_RA "}", OPT_RA "," NO_OPT_OP "," NO_OPT_SA ",", "", OPT_RA "," NO_OPT_OP "," NO_OPT_SA ",", "");
   /* - if doc recv'ed and iterated, use the doc's resume token. */
   _test_resume (
      "{" OPT_RA "}", OPT_RA "," NO_OPT_OP "," NO_OPT_SA ",", DOC, DOC_RA "," NO_OPT_OP "," NO_OPT_SA ",", "");

   /* only 'startAfter' specified. */
   /* - if no doc recv'ed, use the startAfter option for the original aggregate
    *   whether or not we are resuming. */
   _test_resume (
      "{" OPT_SA "}", OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",", "", OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",", "");
   /* - if doc recv'ed and iterated, use the doc's resume token. */
   _test_resume (
      "{" OPT_SA "}", OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",", DOC, DOC_RA "," NO_OPT_OP "," NO_OPT_SA ",", "");

   /* 'resumeAfter', 'startAfter', and 'startAtOperationTime' are all specified.
    * All should be passed (although the server currently returns an error). */
   /* - if no doc recv'ed, use startAfter. */
   _test_resume ("{" OPT_RA "," OPT_SA "," OPT_OP "}",
                 OPT_RA "," OPT_SA "," OPT_OP ",",
                 "",
                 OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",",
                 "");
   /* - if one doc recv'ed and iterated, use resumeAfter with doc's resume
    * token. */
   _test_resume ("{" OPT_RA "," OPT_SA "," OPT_OP "}",
                 OPT_RA "," OPT_SA "," OPT_OP ",",
                 DOC,
                 DOC_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "");
}


/* test resume behavior before and after the first document is received when a
   postBatchResumeToken is available. */
static void
test_resume_cases_with_post_batch_resume_token (void)
{
#define CURSOR_PBR "'postBatchResumeToken': {'resume': 'pbr'}"
#define PBR_RA "'resumeAfter': {'resume': 'pbr'}"
#define PBR_SA "'startAfter': {'resume': 'pbr'}"

   /* test features:
    * - whether the change stream returns a document before resuming.
    * - whether 'postBatchResumeToken' is available
    * - whether 'startAtOperationTime' is specified
    * - whether 'resumeAfter' is specified
    * - whether 'startAfterAfter' is specified */

   /* postBatchResumeToken always takes priority over specified options or
    * operation time. It will also take priority over the resume token of the
    * last document in the batch (if _test_resume() iterates to that point). */

   /* no options specified. */
   /* - if no doc recv'ed, use resumeAfter with postBatchResumeToken. */
   _test_resume (
      "{}", NO_OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", "", PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",", "," CURSOR_PBR);
   /* - if one doc recv'ed and iterated, use resumeAfter with
    * postBatchResumeToken. */
   _test_resume (
      "{}", NO_OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",", DOC, PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",", "," CURSOR_PBR);

   /* only 'startAtOperationTime' specified. */
   /* - if no doc recv'ed, use resumeAfter with postBatchResumeToken. */
   _test_resume ("{" OPT_OP "}",
                 OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",",
                 "",
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);
   /* - if one doc recv'ed and iterated, use resumeAfter with
    * postBatchResumeToken. */
   _test_resume ("{" OPT_OP "}",
                 OPT_OP "," NO_OPT_RA "," NO_OPT_SA ",",
                 DOC,
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);

   /* only 'resumeAfter' specified. */
   /* - if no doc recv'ed, use resumeAfter with postBatchResumeToken. */
   _test_resume ("{" OPT_RA "}",
                 OPT_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "",
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);
   /* - if one doc recv'ed and iterated, use resumeAfter with
    * postBatchResumeToken. */
   _test_resume ("{" OPT_RA "}",
                 OPT_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 DOC,
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);

   /* only 'startAfter' specified. */
   /* - if no doc recv'ed, use startAfter with postBatchResumeToken. */
   _test_resume ("{" OPT_SA "}",
                 OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",",
                 "",
                 PBR_SA "," NO_OPT_OP "," NO_OPT_RA ",",
                 "," CURSOR_PBR);
   /* - if one doc recv'ed and iterated, use resumeAfter with
    * postBatchResumeToken. */
   _test_resume ("{" OPT_SA "}",
                 OPT_SA "," NO_OPT_OP "," NO_OPT_RA ",",
                 DOC,
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);

   /* 'resumeAfter', 'startAfter', and 'startAtOperationTime' are all specified.
    * All should be passed (although the server currently returns an error). */
   /* - if no doc recv'ed, use startAfter with postBatchResumeToken. */
   _test_resume ("{" OPT_RA "," OPT_SA "," OPT_OP "}",
                 OPT_RA "," OPT_SA "," OPT_OP ",",
                 "",
                 PBR_SA "," NO_OPT_OP "," NO_OPT_RA ",",
                 "," CURSOR_PBR);
   /* - if one doc recv'ed and iterated, use resumeAfter with
    * postBatchResumeToken. */
   _test_resume ("{" OPT_RA "," OPT_SA "," OPT_OP "}",
                 OPT_RA "," OPT_SA "," OPT_OP ",",
                 DOC,
                 PBR_RA "," NO_OPT_OP "," NO_OPT_SA ",",
                 "," CURSOR_PBR);
}


void
test_error_null_doc (void *ctx)
{
   mongoc_client_t *client;
   mongoc_change_stream_t *stream;
   bson_error_t err;
   const bson_t *error_doc = tmp_bson ("{}"); /* assign to a non-zero address. */

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   stream = mongoc_client_watch (client, tmp_bson ("{}"), NULL);
   /* error_doc starts as non-NULL. */
   BSON_ASSERT (error_doc);
   BSON_ASSERT (!mongoc_change_stream_error_document (stream, &err, &error_doc));
   /* error_doc is set to NULL no error occurred. */
   BSON_ASSERT (!error_doc);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
}


void
_check_doc_resume_token (const bson_t *doc, const bson_t *resume_token)
{
   bson_t document_resume_token;

   bson_lookup_doc (doc, "_id", &document_resume_token);
   ASSERT (bson_equal (resume_token, &document_resume_token));

   bson_destroy (&document_resume_token);
}


void
prose_test_11 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_error_t error;
   const bson_t *next_doc = NULL;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   const bson_t *resume_token;
   _data_change_stream_t *post_batch_expected;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   ASSERT (client);

   coll = drop_and_get_coll (client, "db", "coll_resume");
   ASSERT (coll);

   /* Set the batch size to 1 so we only get one document per call to next. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"));
   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   /* The resume token should be updated to the post batch resume token */
   ASSERT (!mongoc_change_stream_next (stream, &next_doc));
   ASSERT (!next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));

   /* Look into the struct and get the actual post batch resume token, assert it
    * is equal to our resume token */
   post_batch_expected = (_data_change_stream_t *) stream->cursor->impl.data;
   ASSERT (bson_compare (resume_token, &post_batch_expected->post_batch_resume_token) == 0);

   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 0}"), &opts, NULL, &error), error);

   /* Checking that a resume token is returned */
   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   ASSERT (bson_compare (resume_token, &post_batch_expected->post_batch_resume_token) == 0);

   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}


void
prose_test_12 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_error_t error;
   const bson_t *next_doc = NULL;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   const bson_t *resume_token;
   bson_iter_t iter, child;
   bson_t expected_token;
   bson_t expected_doc;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   ASSERT (client);

   coll = drop_and_get_coll (client, "db", "coll_resume");
   ASSERT (coll);

   /* Set the batch size to 1 so we only get one document per call to next. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"));
   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 0}"), &opts, NULL, &error), error);

   /* Checking that a resume token is returned */
   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));

   /* Need to now check that we are getting back the _id of the last inserted
    * document when we iterate to the last document */
   bson_copy_to (next_doc, &expected_doc);
   _check_doc_resume_token (&expected_doc, resume_token);

   ASSERT (bson_iter_init_find (&iter, next_doc, "documentKey"));
   ASSERT (bson_iter_recurse (&iter, &child));
   ASSERT (bson_iter_find (&child, "_id") && bson_iter_int32 (&child) == 0);

   /* Must check that getResumeToken returns resumeAfter correctly when
    * specified. */
   bson_copy_to (resume_token, &expected_token);
   mongoc_change_stream_destroy (stream);
   bson_destroy (&opts);
   bson_init (&opts);
   BSON_APPEND_DOCUMENT (&opts, "resumeAfter", &expected_token);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), &opts);
   ASSERT (stream);

   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (bson_equal (resume_token, &expected_token));

   bson_destroy (&expected_doc);
   bson_destroy (&expected_token);
   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}


void
prose_test_13 (void *ctx)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_error_t error;
   const bson_t *next_doc = NULL;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   const bson_t *resume_token;
   bson_iter_t iter, child;

   client = test_framework_new_default_client ();
   ASSERT (client);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, test_resume_token_command_start);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);

   coll = drop_and_get_coll (client, "db", "coll_resume");
   ASSERT (coll);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson (NULL), NULL, NULL, &error), error);

   /* Set the batch size to 1 so we only get one document per call to next. */
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"));
   ASSERT (stream);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (stream, &error, NULL), error);

   /* Insert a few docs to listen for. Use write concern majority, so subsequent
    * call to watch will be guaranteed to retrieve them. */
   mongoc_write_concern_set_wmajority (wc, 30000);
   mongoc_write_concern_append (wc, &opts);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 0}"), &opts, NULL, &error), error);

   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 1}"), &opts, NULL, &error), error);

   /* The resume token should be updated to the most recently iterated doc */
   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   _check_doc_resume_token (next_doc, resume_token);

   ASSERT (mongoc_change_stream_next (stream, &next_doc));
   ASSERT (next_doc);
   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (!bson_empty0 (resume_token));
   _check_doc_resume_token (next_doc, resume_token);

   ASSERT (bson_iter_init_find (&iter, next_doc, "documentKey"));
   ASSERT (bson_iter_recurse (&iter, &child));
   ASSERT (bson_iter_find (&child, "_id") && bson_iter_int32 (&child) == 1);

   bson_destroy (&opts);
   mongoc_write_concern_destroy (wc);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_collection_destroy (coll);
}

static void
_save_operation_time_from_agg (const mongoc_apm_command_succeeded_t *event)
{
   if (0 == strcmp ("aggregate", mongoc_apm_command_succeeded_get_command_name (event))) {
      mongoc_timestamp_t *timestamp;
      bson_iter_t iter;
      const bson_t *cmd;

      cmd = mongoc_apm_command_succeeded_get_reply (event);
      timestamp = mongoc_apm_command_succeeded_get_context (event);
      /* Capture the operationTime from the first aggregate reply. */
      if (timestamp->timestamp == 0) {
         BSON_ASSERT (bson_iter_init_find (&iter, cmd, "operationTime"));
         _mongoc_timestamp_set_from_bson (timestamp, &iter);
      }
   }
}

void
prose_test_14 (void *test_ctx)
{
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   bson_t opts;
   bson_error_t error;
   const bson_t *resume_token;
   bson_t expected_token;
   const bson_t *doc = NULL;
   mongoc_timestamp_t optime = {0};
   mongoc_apm_callbacks_t *callbacks;

   BSON_UNUSED (test_ctx);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_succeeded_cb (callbacks, _save_operation_time_from_agg);
   mongoc_client_set_apm_callbacks (client, callbacks, &optime);
   mongoc_apm_callbacks_destroy (callbacks);

   coll = drop_and_get_coll (client, "db", "coll");
   bson_init (&opts);
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), &opts);
   /* The _save_operation_time_from_agg listener must have stored the operation
    * time. */
   BSON_ASSERT (optime.timestamp != 0);

   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 0}"), &opts, NULL, &error), error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 1}"), &opts, NULL, &error), error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 2}"), &opts, NULL, &error), error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{'_id': 3}"), &opts, NULL, &error), error);

   ASSERT (mongoc_change_stream_next (stream, &doc));
   resume_token = mongoc_change_stream_get_resume_token (stream);

   bson_copy_to (resume_token, &expected_token);
   BSON_APPEND_DOCUMENT (&opts, "startAfter", &expected_token);

   mongoc_change_stream_destroy (stream);

   /* Start a new change stream using "startAfter" set to a previously obtained
   resume token to guarantee a non-empty initial batch */
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), &opts);

   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (bson_equal (resume_token, &expected_token));

   /* Doing the same using "resumeAfter" instead */
   mongoc_change_stream_destroy (stream);
   bson_destroy (&opts);
   bson_init (&opts);
   BSON_APPEND_DOCUMENT (&opts, "resumeAfter", &expected_token);

   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), &opts);

   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (bson_equal (resume_token, &expected_token));
   mongoc_change_stream_destroy (stream);

   /* Finally, with neither. */
   bson_destroy (&opts);
   bson_init (&opts);
   BSON_APPEND_TIMESTAMP (&opts, "startAtOperationTime", optime.timestamp, optime.increment);
   stream = mongoc_collection_watch (coll, tmp_bson ("{}"), &opts);

   resume_token = mongoc_change_stream_get_resume_token (stream);
   ASSERT (resume_token == NULL);

   bson_destroy (&expected_token);
   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


void
prose_test_17 (void)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   coll = mongoc_client_get_collection (client, "db", "coll");
   /* Pass an arbitrary document as the resume token, like {'x': 1} */
   future = future_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'startAfter': {'x': 1}}"));

   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline' : [ { '$changeStream': { "
                                                 "'startAfter': {'x': 1} , 'resumeAfter': { '$exists': false }, "
                                                 "'startAtOperationTime': { '$exists': false } } } ]}"));

   reply_to_request_simple (request, "{'cursor': {'id': 123, 'ns': 'db.coll', 'firstBatch': []}, 'ok': 1 }");

   request_destroy (request);

   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);
   future_destroy (future);

   future = future_change_stream_next (stream, &next_doc);

   request = mock_server_receives_msg (
      server, MONGOC_QUERY_NONE, tmp_bson ("{ 'getMore': {'$numberLong': '123'}, 'collection': 'coll' }"));

   reply_to_request_simple (request,
                            "{ 'code': 10107, 'errmsg': 'not primary', 'errorLabels': "
                            "['ResumableChangeStreamError'], 'ok': 0 }");

   request_destroy (request);

   /* Resume occurs. */
   request = mock_server_receives_msg (
      server, MONGOC_QUERY_NONE, tmp_bson ("{ 'killCursors': 'coll', 'cursors': [{ '$numberLong': '123'}]}"));
   reply_to_request_with_ok_and_destroy (request);

   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline': [ { "
                                                 "'$changeStream': { 'startAfter': {'x': 1}, 'resumeAfter': { "
                                                 "'$exists': false }, 'startAtOperationTime': { '$exists': "
                                                 "false } } "
                                                 "}]}"));

   /* Reply with a 0 cursor ID to prevent a killCursors command. */
   reply_to_request_simple (request, "{'cursor': {'id': 0, 'ns': 'db.coll', 'firstBatch': []}, 'ok': 1 }");
   request_destroy (request);
   BSON_ASSERT (!future_get_bool (future));
   future_destroy (future);
   mongoc_change_stream_destroy (stream);

   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


void
prose_test_18 (void)
{
   mock_server_t *server;
   request_t *request;
   future_t *future;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   const bson_t *next_doc = NULL;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   coll = mongoc_client_get_collection (client, "db", "coll");
   /* Pass an arbitrary document as the resume token, like {'x': 1} */
   future = future_collection_watch (coll, tmp_bson ("{}"), tmp_bson ("{'startAfter': {'x': 1}}"));

   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline' : [ { '$changeStream': { "
                                                 "'startAfter': {'x': 1}, 'resumeAfter': { '$exists': false }, "
                                                 "'startAtOperationTime': { '$exists': false } } } ]}"));

   reply_to_request_simple (request,
                            "{'cursor': {'id': 123, 'ns': "
                            "'db.coll', 'firstBatch': [{'_id': "
                            "{'y': 1}}]}, 'ok': 1 }");

   request_destroy (request);
   stream = future_get_mongoc_change_stream_ptr (future);
   ASSERT (stream);
   future_destroy (future);

   /* The first call to mongoc_change_stream_next returns the batched document.
    */
   mongoc_change_stream_next (stream, &next_doc);

   future = future_change_stream_next (stream, &next_doc);

   request = mock_server_receives_msg (
      server, MONGOC_QUERY_NONE, tmp_bson ("{ 'getMore': {'$numberLong': '123'}, 'collection': 'coll' }"));

   reply_to_request_simple (request,
                            "{ 'code': 10107, 'errmsg': 'not primary', 'errorLabels': "
                            "['ResumableChangeStreamError'], 'ok': 0 }");

   request_destroy (request);

   request = mock_server_receives_msg (
      server, MONGOC_QUERY_NONE, tmp_bson ("{ 'killCursors': 'coll', 'cursors': [{ '$numberLong': '123'}]}"));
   reply_to_request_with_ok_and_destroy (request);

   request = mock_server_receives_msg (server,
                                       MONGOC_QUERY_NONE,
                                       tmp_bson ("{ 'aggregate': 'coll', 'pipeline': [ { "
                                                 "'$changeStream': { 'resumeAfter': {'y': 1}, 'startAfter': { "
                                                 "'$exists': false }, 'startAtOperationTime': { '$exists': "
                                                 "false } } "
                                                 "}]}"));
   /* Reply with a 0 cursor ID to prevent a killCursors command. */
   reply_to_request_simple (request, "{'cursor': {'id': 0, 'ns': 'db.coll', 'firstBatch': []}, 'ok': 1 }");
   request_destroy (request);
   BSON_ASSERT (!future_get_bool (future));
   future_destroy (future);
   mongoc_change_stream_destroy (stream);

   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


void
test_change_stream_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/change_stream/pipeline", test_change_stream_pipeline);

   TestSuite_AddFull (suite,
                      "/change_stream/live/single_server",
                      test_change_stream_live_single_server,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_single_version_5);

   TestSuite_AddFull (suite,
                      "/change_stream/live/track_resume_token",
                      test_change_stream_live_track_resume_token,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_no_failpoint);

   TestSuite_AddFull (suite,
                      "/change_stream/live/batch_size",
                      test_change_stream_live_batch_size,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);

   TestSuite_AddFull (suite,
                      "/change_stream/live/missing_resume_token",
                      test_change_stream_live_missing_resume_token,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);

   TestSuite_AddFull (suite,
                      "/change_stream/live/invalid_resume_token",
                      test_change_stream_live_invalid_resume_token,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);

   TestSuite_AddMockServerTest (suite, "/change_stream/resumable_error", test_change_stream_resumable_error);

   TestSuite_AddMockServerTest (suite, "/change_stream/options", test_change_stream_options);

   TestSuite_AddFull (suite,
                      "/change_stream/live/watch",
                      test_change_stream_live_watch,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);

   TestSuite_AddFull (suite,
                      "/change_stream/live/read_prefs",
                      test_change_stream_live_read_prefs,
                      NULL,
                      NULL,
                      _skip_if_no_start_at_optime,
                      test_framework_skip_if_no_failpoint);

   TestSuite_Add (suite, "/change_stream/server_selection_fails", test_change_stream_server_selection_fails);

   TestSuite_AddFull (suite,
                      "/change_stream/next_after_error",
                      test_change_stream_next_after_error,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);

   TestSuite_AddFull (suite,
                      "/change_stream/accepts_array",
                      test_change_stream_accepts_array,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset);
   TestSuite_AddMockServerTest (suite, "/change_stream/getmore_errors", test_getmore_errors);
   TestSuite_AddFull (suite,
                      "/change_stream/start_at_operation_time",
                      test_change_stream_start_at_operation_time,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_rs_version_7,
                      test_framework_skip_if_no_crypto,
                      _skip_if_no_start_at_optime);
   TestSuite_AddFull (suite,
                      "/change_stream/resume_at_optime",
                      test_change_stream_resume_at_optime,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_rs_version_7,
                      test_framework_skip_if_no_crypto,
                      _skip_if_no_start_at_optime,
                      test_framework_skip_if_no_failpoint);
   TestSuite_AddFull (suite,
                      "/change_stream/resume_with_post_batch_resume_token",
                      test_change_stream_resume_with_post_batch_resume_token,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_rs_version_7,
                      test_framework_skip_if_no_crypto,
                      _skip_if_no_start_at_optime,
                      test_framework_skip_if_no_failpoint);
   TestSuite_AddFull (
      suite, "/change_stream/database", test_change_stream_database_watch, NULL, NULL, _skip_if_no_db_watch);
   TestSuite_AddFull (
      suite, "/change_stream/client", test_change_stream_client_watch, NULL, NULL, _skip_if_no_client_watch);
   TestSuite_AddMockServerTest (suite, "/change_stream/resume_with_first_doc", test_resume_cases);
   TestSuite_AddMockServerTest (suite,
                                "/change_stream/resume_with_first_doc/post_batch_resume_token",
                                test_resume_cases_with_post_batch_resume_token);
   TestSuite_AddFull (
      suite, "/change_stream/error_null_doc", test_error_null_doc, NULL, NULL, _skip_if_no_client_watch);
   TestSuite_AddFull (suite,
                      "/change_stream/live/prose_test_11",
                      prose_test_11,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/change_stream/live/prose_test_12",
                      prose_test_12,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      test_framework_skip_if_max_wire_version_more_than_7);
   TestSuite_AddFull (suite,
                      "/change_stream/live/prose_test_13",
                      prose_test_13,
                      NULL,
                      NULL,
                      test_framework_skip_if_not_replset,
                      _skip_if_no_start_at_optime);
   TestSuite_AddFull (suite,
                      "/change_stream/live/prose_test_14",
                      prose_test_14,
                      NULL,
                      NULL,
                      test_framework_skip_if_mongos,
                      test_framework_skip_if_not_rs_version_7);
   TestSuite_AddMockServerTest (suite, "/change_streams/prose_test_17", prose_test_17);
   TestSuite_AddMockServerTest (suite, "/change_streams/prose_test_18", prose_test_18);
}
