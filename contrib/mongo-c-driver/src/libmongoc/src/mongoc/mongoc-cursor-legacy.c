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

/* cursor functions for pre-3.2 MongoDB, including:
 * - OP_QUERY find (superseded by the find command)
 * - OP_GETMORE (superseded by the getMore command)
 * - receiving OP_REPLY documents in a stream (instead of batch)
 */

#include "mongoc-cursor.h"
#include "mongoc-cursor-private.h"
#include "mongoc-client-private.h"
#include "mongoc-counters-private.h"
#include "mongoc-error.h"
#include "mongoc-log.h"
#include "mongoc-trace-private.h"
#include "mongoc-read-concern-private.h"
#include "mongoc-util-private.h"
#include "mongoc-write-concern-private.h"
#include "mongoc-read-prefs-private.h"
#include "mongoc-rpc-private.h"

#include <bson-dsl.h>

static bool
_mongoc_cursor_monitor_legacy_get_more (mongoc_cursor_t *cursor, mongoc_server_stream_t *server_stream)
{
   bson_t doc;
   char *db;
   mongoc_client_t *client;
   mongoc_apm_command_started_t event;

   ENTRY;

   client = cursor->client;
   if (!client->apm_callbacks.started) {
      /* successful */
      RETURN (true);
   }

   _mongoc_cursor_prepare_getmore_command (cursor, &doc);

   db = bson_strndup (cursor->ns, cursor->dblen);
   mongoc_apm_command_started_init (&event,
                                    &doc,
                                    db,
                                    "getMore",
                                    client->cluster.request_id,
                                    cursor->operation_id,
                                    &server_stream->sd->host,
                                    server_stream->sd->id,
                                    &server_stream->sd->service_id,
                                    server_stream->sd->server_connection_id,
                                    NULL,
                                    client->apm_context);

   client->apm_callbacks.started (&event);
   mongoc_apm_command_started_cleanup (&event);
   bson_destroy (&doc);
   bson_free (db);

   RETURN (true);
}


static bool
_mongoc_cursor_monitor_legacy_query (mongoc_cursor_t *cursor,
                                     const bson_t *filter,
                                     mongoc_server_stream_t *server_stream)
{
   bson_t doc;
   mongoc_client_t *client;
   char *db;
   bool r;

   ENTRY;

   client = cursor->client;
   if (!client->apm_callbacks.started) {
      /* successful */
      RETURN (true);
   }

   bson_init (&doc);
   db = bson_strndup (cursor->ns, cursor->dblen);

   /* simulate a MongoDB 3.2+ "find" command */
   _mongoc_cursor_prepare_find_command (cursor, filter, &doc);

   bsonBuildAppend (cursor->opts, insert (doc, not(key ("serverId", "maxAwaitTimeMS", "sessionId"))));

   r = _mongoc_cursor_monitor_command (cursor, server_stream, &doc, "find");

   bson_destroy (&doc);
   bson_free (db);

   RETURN (r);
}


static bool
_mongoc_cursor_op_getmore_send (mongoc_cursor_t *cursor,
                                mongoc_server_stream_t *server_stream,
                                int32_t request_id,
                                int32_t flags,
                                mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (cursor);
   BSON_ASSERT_PARAM (server_stream);
   BSON_ASSERT_PARAM (rpc);

   const int32_t n_return = (flags & MONGOC_OP_QUERY_FLAG_TAILABLE_CURSOR) != 0 ? 0 : _mongoc_n_return (cursor);

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, request_id);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_GET_MORE);

      message_length += sizeof (int32_t); // ZERO
      message_length += mcd_rpc_op_get_more_set_full_collection_name (rpc, cursor->ns);
      message_length += mcd_rpc_op_get_more_set_number_to_return (rpc, n_return);
      message_length += mcd_rpc_op_get_more_set_cursor_id (rpc, cursor->cursor_id);

      mcd_rpc_message_set_length (rpc, message_length);
   }

   if (!_mongoc_cursor_monitor_legacy_get_more (cursor, server_stream)) {
      return false;
   }

   if (!mongoc_cluster_legacy_rpc_sendv_to_server (&cursor->client->cluster, rpc, server_stream, &cursor->error)) {
      return false;
   }

   return true;
}

void
_mongoc_cursor_op_getmore (mongoc_cursor_t *cursor, mongoc_cursor_response_legacy_t *response)
{
   BSON_ASSERT_PARAM (cursor);
   BSON_ASSERT_PARAM (response);

   ENTRY;

   const int64_t started = bson_get_monotonic_time ();

   mongoc_server_stream_t *const server_stream = _mongoc_cursor_fetch_stream (cursor);

   if (!server_stream) {
      GOTO (done);
   }

   int32_t flags;
   if (!_mongoc_cursor_opts_to_flags (cursor, server_stream, &flags)) {
      GOTO (fail);
   }
   mongoc_cluster_t *const cluster = &cursor->client->cluster;

   const int32_t request_id =
      cursor->in_exhaust ? mcd_rpc_header_get_request_id (response->rpc) : ++cluster->request_id;

   if (!cursor->in_exhaust &&
       !_mongoc_cursor_op_getmore_send (cursor, server_stream, request_id, flags, response->rpc)) {
      GOTO (fail);
   }

   mcd_rpc_message_reset (response->rpc);
   _mongoc_buffer_clear (&response->buffer, false);
   cursor->cursor_id = 0;

   if (!_mongoc_client_recv (cursor->client, response->rpc, &response->buffer, server_stream, &cursor->error)) {
      GOTO (fail);
   }

   const int32_t op_code = mcd_rpc_header_get_op_code (response->rpc);
   if (op_code != MONGOC_OP_CODE_REPLY) {
      bson_set_error (&cursor->error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid opcode for OP_GET_MORE: expected %" PRId32 ", got %" PRId32,
                      MONGOC_OP_CODE_REPLY,
                      op_code);
      GOTO (fail);
   }

   const int32_t response_to = mcd_rpc_header_get_response_to (response->rpc);
   if (response_to != request_id) {
      bson_set_error (&cursor->error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid response_to for OP_GET_MORE: expected %" PRId32 ", got %" PRId32,
                      request_id,
                      response_to);
      GOTO (fail);
   }

   if (!mcd_rpc_message_check_ok (
          response->rpc, cursor->client->error_api_version, &cursor->error, &cursor->error_doc)) {
      GOTO (fail);
   }

   if (response->reader) {
      bson_reader_destroy (response->reader);
   }

   cursor->cursor_id = mcd_rpc_op_reply_get_cursor_id (response->rpc);

   const void *documents = mcd_rpc_op_reply_get_documents (response->rpc);
   if (documents == NULL) {
      // Use a non-NULL pointer to satisfy precondition of
      // `bson_reader_new_from_data`:
      documents = "";
   }

   response->reader = bson_reader_new_from_data (documents, mcd_rpc_op_reply_get_documents_len (response->rpc));

   _mongoc_cursor_monitor_succeeded (cursor,
                                     response,
                                     bson_get_monotonic_time () - started,
                                     false, /* not first batch */
                                     server_stream,
                                     "getMore");

   GOTO (done);

fail:
   _mongoc_cursor_monitor_failed (cursor, bson_get_monotonic_time () - started, server_stream, "getMore");

done:
   mongoc_server_stream_cleanup (server_stream);
}


#define OPT_CHECK(_type)                                         \
   do {                                                          \
      if (!BSON_ITER_HOLDS_##_type (&iter)) {                    \
         bson_set_error (&cursor->error,                         \
                         MONGOC_ERROR_COMMAND,                   \
                         MONGOC_ERROR_COMMAND_INVALID_ARG,       \
                         "invalid option %s, should be type %s", \
                         key,                                    \
                         #_type);                                \
         return NULL;                                            \
      }                                                          \
   } while (false)


#define OPT_CHECK_INT()                                          \
   do {                                                          \
      if (!BSON_ITER_HOLDS_INT (&iter)) {                        \
         bson_set_error (&cursor->error,                         \
                         MONGOC_ERROR_COMMAND,                   \
                         MONGOC_ERROR_COMMAND_INVALID_ARG,       \
                         "invalid option %s, should be integer", \
                         key);                                   \
         return NULL;                                            \
      }                                                          \
   } while (false)


#define OPT_ERR(_msg)                                                                                \
   do {                                                                                              \
      bson_set_error (&cursor->error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, _msg); \
      return NULL;                                                                                   \
   } while (false)


#define OPT_BSON_ERR(_msg)                                                                 \
   do {                                                                                    \
      bson_set_error (&cursor->error, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, _msg); \
      return NULL;                                                                         \
   } while (false)


#define OPT_FLAG(_flag)                \
   do {                                \
      OPT_CHECK (BOOL);                \
      if (bson_iter_as_bool (&iter)) { \
         *flags |= _flag;              \
      }                                \
   } while (false)


#define PUSH_DOLLAR_QUERY()                                 \
   do {                                                     \
      if (!pushed_dollar_query) {                           \
         pushed_dollar_query = true;                        \
         bson_append_document (query, "$query", 6, filter); \
      }                                                     \
   } while (false)


#define OPT_SUBDOCUMENT(_opt_name, _legacy_name)                           \
   do {                                                                    \
      OPT_CHECK (DOCUMENT);                                                \
      bson_iter_document (&iter, &len, &data);                             \
      if (!bson_init_static (&subdocument, data, (size_t) len)) {          \
         OPT_BSON_ERR ("Invalid '" #_opt_name "' subdocument in 'opts'."); \
      }                                                                    \
      BSON_APPEND_DOCUMENT (query, "$" #_legacy_name, &subdocument);       \
   } while (false)

static bson_t *
_mongoc_cursor_parse_opts_for_op_query (mongoc_cursor_t *cursor,
                                        mongoc_server_stream_t *stream,
                                        bson_t *filter,
                                        bson_t *query /* OUT */,
                                        bson_t *fields /* OUT */,
                                        int32_t *flags /* OUT */,
                                        int32_t *skip /* OUT */)
{
   bool pushed_dollar_query;
   bson_iter_t iter;
   uint32_t len;
   const uint8_t *data;
   bson_t subdocument;
   const char *key;
   char *dollar_modifier;

   *flags = MONGOC_OP_QUERY_FLAG_NONE;
   *skip = 0;

   /* assume we'll send filter straight to server, like "{a: 1}". if we find an
    * opt we must add, like "sort", we push the query like "$query: {a: 1}",
    * then add a query modifier for the option, in this example "$orderby".
    */
   pushed_dollar_query = false;

   if (!bson_iter_init (&iter, &cursor->opts)) {
      OPT_BSON_ERR ("Invalid 'opts' parameter.");
   }

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);

      /* most common options first */
      if (!strcmp (key, MONGOC_CURSOR_PROJECTION)) {
         OPT_CHECK (DOCUMENT);
         bson_iter_document (&iter, &len, &data);
         if (!bson_init_static (&subdocument, data, (size_t) len)) {
            OPT_BSON_ERR ("Invalid 'projection' subdocument in 'opts'.");
         }
         bson_destroy (fields);
         bson_copy_to (&subdocument, fields);
      } else if (!strcmp (key, MONGOC_CURSOR_SORT)) {
         PUSH_DOLLAR_QUERY ();
         OPT_SUBDOCUMENT (sort, orderby);
      } else if (!strcmp (key, MONGOC_CURSOR_SKIP)) {
         OPT_CHECK_INT ();
         *skip = (int32_t) bson_iter_as_int64 (&iter);
      }
      /* the rest of the options, alphabetically */
      else if (!strcmp (key, MONGOC_CURSOR_ALLOW_PARTIAL_RESULTS)) {
         OPT_FLAG (MONGOC_OP_QUERY_FLAG_PARTIAL);
      } else if (!strcmp (key, MONGOC_CURSOR_AWAIT_DATA)) {
         OPT_FLAG (MONGOC_OP_QUERY_FLAG_AWAIT_DATA);
      } else if (!strcmp (key, MONGOC_CURSOR_COMMENT)) {
         OPT_CHECK (UTF8);
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_UTF8 (query, "$comment", bson_iter_utf8 (&iter, NULL));
      } else if (!strcmp (key, MONGOC_CURSOR_HINT)) {
         if (BSON_ITER_HOLDS_UTF8 (&iter)) {
            PUSH_DOLLAR_QUERY ();
            BSON_APPEND_UTF8 (query, "$hint", bson_iter_utf8 (&iter, NULL));
         } else if (BSON_ITER_HOLDS_DOCUMENT (&iter)) {
            PUSH_DOLLAR_QUERY ();
            OPT_SUBDOCUMENT (hint, hint);
         } else {
            OPT_ERR ("Wrong type for 'hint' field in 'opts'.");
         }
      } else if (!strcmp (key, MONGOC_CURSOR_MAX)) {
         PUSH_DOLLAR_QUERY ();
         OPT_SUBDOCUMENT (max, max);
      } else if (!strcmp (key, MONGOC_CURSOR_MAX_SCAN)) {
         OPT_CHECK_INT ();
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_INT64 (query, "$maxScan", bson_iter_as_int64 (&iter));
      } else if (!strcmp (key, MONGOC_CURSOR_MAX_TIME_MS)) {
         OPT_CHECK_INT ();
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_INT64 (query, "$maxTimeMS", bson_iter_as_int64 (&iter));
      } else if (!strcmp (key, MONGOC_CURSOR_MIN)) {
         PUSH_DOLLAR_QUERY ();
         OPT_SUBDOCUMENT (min, min);
      } else if (!strcmp (key, MONGOC_CURSOR_READ_CONCERN)) {
         OPT_ERR ("Set readConcern on client, database, or collection,"
                  " not in a query.");
      } else if (!strcmp (key, MONGOC_CURSOR_RETURN_KEY)) {
         OPT_CHECK (BOOL);
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_BOOL (query, "$returnKey", bson_iter_as_bool (&iter));
      } else if (!strcmp (key, MONGOC_CURSOR_SHOW_RECORD_ID)) {
         OPT_CHECK (BOOL);
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_BOOL (query, "$showDiskLoc", bson_iter_as_bool (&iter));
      } else if (!strcmp (key, MONGOC_CURSOR_SNAPSHOT)) {
         OPT_CHECK (BOOL);
         PUSH_DOLLAR_QUERY ();
         BSON_APPEND_BOOL (query, "$snapshot", bson_iter_as_bool (&iter));
      } else if (!strcmp (key, MONGOC_CURSOR_COLLATION)) {
         bson_set_error (&cursor->error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION,
                         "The selected server does not support collation");
         return NULL;
      }
      /* singleBatch limit and batchSize are handled in _mongoc_n_return,
       * exhaust noCursorTimeout oplogReplay tailable in _mongoc_cursor_flags
       * maxAwaitTimeMS is handled in _mongoc_cursor_prepare_getmore_command
       * sessionId is used to retrieve the mongoc_client_session_t
       */
      else if (strcmp (key, MONGOC_CURSOR_SINGLE_BATCH) && strcmp (key, MONGOC_CURSOR_LIMIT) &&
               strcmp (key, MONGOC_CURSOR_BATCH_SIZE) && strcmp (key, MONGOC_CURSOR_EXHAUST) &&
               strcmp (key, MONGOC_CURSOR_NO_CURSOR_TIMEOUT) && strcmp (key, MONGOC_CURSOR_OPLOG_REPLAY) &&
               strcmp (key, MONGOC_CURSOR_TAILABLE) && strcmp (key, MONGOC_CURSOR_MAX_AWAIT_TIME_MS)) {
         /* pass unrecognized options to server, prefixed with $ */
         PUSH_DOLLAR_QUERY ();
         dollar_modifier = bson_strdup_printf ("$%s", key);
         if (!bson_append_iter (query, dollar_modifier, -1, &iter)) {
            bson_set_error (&cursor->error,
                            MONGOC_ERROR_BSON,
                            MONGOC_ERROR_BSON_INVALID,
                            "Error adding \"%s\" to query",
                            dollar_modifier);
            bson_free (dollar_modifier);
            return NULL;
         }
         bson_free (dollar_modifier);
      }
   }

   if (!_mongoc_cursor_opts_to_flags (cursor, stream, flags)) {
      /* cursor->error is set */
      return NULL;
   }

   return pushed_dollar_query ? query : filter;
}

#undef OPT_CHECK
#undef OPT_ERR
#undef OPT_BSON_ERR
#undef OPT_FLAG
#undef OPT_SUBDOCUMENT


static bool
_mongoc_cursor_op_query_find_send (mongoc_cursor_t *cursor,
                                   mongoc_server_stream_t *server_stream,
                                   int32_t request_id,
                                   bson_t *filter,
                                   mcd_rpc_message *rpc)
{
   bool ret = false;

   cursor->operation_id = ++cursor->client->cluster.operation_id;

   mongoc_assemble_query_result_t result = ASSEMBLE_QUERY_RESULT_INIT;
   bson_t query = BSON_INITIALIZER;
   bson_t fields = BSON_INITIALIZER;
   int32_t skip;
   int32_t flags;
   const bson_t *const query_ptr =
      _mongoc_cursor_parse_opts_for_op_query (cursor, server_stream, filter, &query, &fields, &flags, &skip);

   if (!query_ptr) {
      GOTO (done);
   }

   assemble_query (cursor->read_prefs, server_stream, query_ptr, flags, &result);

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, request_id);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_QUERY);

      message_length += mcd_rpc_op_query_set_flags (rpc, result.flags);
      message_length += mcd_rpc_op_query_set_full_collection_name (rpc, cursor->ns);
      message_length += mcd_rpc_op_query_set_number_to_skip (rpc, skip);
      message_length += mcd_rpc_op_query_set_number_to_return (rpc, _mongoc_n_return (cursor));
      message_length += mcd_rpc_op_query_set_query (rpc, bson_get_data (result.assembled_query));

      if (!bson_empty (&fields)) {
         message_length += mcd_rpc_op_query_set_return_fields_selector (rpc, bson_get_data (&fields));
      }

      mcd_rpc_message_set_length (rpc, message_length);
   }

   if (!_mongoc_cursor_monitor_legacy_query (cursor, filter, server_stream)) {
      GOTO (done);
   }

   if (!mongoc_cluster_legacy_rpc_sendv_to_server (&cursor->client->cluster, rpc, server_stream, &cursor->error)) {
      GOTO (done);
   }

   ret = true;

done:
   assemble_query_result_cleanup (&result);
   bson_destroy (&fields);
   bson_destroy (&query);

   return ret;
}

bool
_mongoc_cursor_op_query_find (mongoc_cursor_t *cursor, bson_t *filter, mongoc_cursor_response_legacy_t *response)
{
   BSON_ASSERT_PARAM (cursor);
   BSON_ASSERT_PARAM (filter);
   BSON_ASSERT_PARAM (response);

   ENTRY;

   bool ret = false;

   mongoc_server_stream_t *const server_stream = _mongoc_cursor_fetch_stream (cursor);

   if (!server_stream) {
      RETURN (false);
   }

   const int64_t started = bson_get_monotonic_time ();
   const int32_t request_id = ++cursor->client->cluster.request_id;
   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   if (!_mongoc_cursor_op_query_find_send (cursor, server_stream, request_id, filter, rpc)) {
      GOTO (done);
   }

   mcd_rpc_message_reset (rpc);
   _mongoc_buffer_clear (&response->buffer, false);

   if (!_mongoc_client_recv (cursor->client, response->rpc, &response->buffer, server_stream, &cursor->error)) {
      GOTO (done);
   }

   const int32_t op_code = mcd_rpc_header_get_op_code (response->rpc);
   if (op_code != MONGOC_OP_CODE_REPLY) {
      bson_set_error (&cursor->error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid opcode for OP_QUERY: expected %" PRId32 ", got %" PRId32,
                      MONGOC_OP_CODE_REPLY,
                      op_code);
      GOTO (done);
   }

   const int32_t response_to = mcd_rpc_header_get_response_to (response->rpc);
   if (response_to != request_id) {
      bson_set_error (&cursor->error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid response_to for OP_QUERY: expected %" PRId32 ", got %" PRId32,
                      request_id,
                      response_to);
      GOTO (done);
   }

   if (!mcd_rpc_message_check_ok (
          response->rpc, cursor->client->error_api_version, &cursor->error, &cursor->error_doc)) {
      GOTO (done);
   }

   if (response->reader) {
      bson_reader_destroy (response->reader);
   }

   cursor->cursor_id = mcd_rpc_op_reply_get_cursor_id (response->rpc);

   const void *documents = mcd_rpc_op_reply_get_documents (response->rpc);
   if (documents == NULL) {
      // Use a non-NULL pointer to satisfy precondition of
      // `bson_reader_new_from_data`:
      documents = "";
   }

   response->reader = bson_reader_new_from_data (documents, mcd_rpc_op_reply_get_documents_len (response->rpc));

   if (_mongoc_cursor_get_opt_bool (cursor, MONGOC_CURSOR_EXHAUST)) {
      cursor->in_exhaust = true;
      cursor->client->in_exhaust = true;
   }

   _mongoc_cursor_monitor_succeeded (cursor,
                                     response,
                                     bson_get_monotonic_time () - started,
                                     true, /* first_batch */
                                     server_stream,
                                     "find");

   ret = true;

done:
   if (!ret) {
      _mongoc_cursor_monitor_failed (cursor, bson_get_monotonic_time () - started, server_stream, "find");
   }

   mcd_rpc_message_destroy (rpc);
   mongoc_server_stream_cleanup (server_stream);

   return ret;
}


void
_mongoc_cursor_response_legacy_init (mongoc_cursor_response_legacy_t *response)
{
   response->rpc = mcd_rpc_message_new ();
   _mongoc_buffer_init (&response->buffer, NULL, 0, NULL, NULL);
}


void
_mongoc_cursor_response_legacy_destroy (mongoc_cursor_response_legacy_t *response)
{
   if (response->reader) {
      bson_reader_destroy (response->reader);
      response->reader = NULL;
   }
   _mongoc_buffer_destroy (&response->buffer);
   mcd_rpc_message_destroy (response->rpc);
}
