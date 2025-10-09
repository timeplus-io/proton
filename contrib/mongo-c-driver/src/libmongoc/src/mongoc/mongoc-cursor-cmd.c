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
#include "mongoc.h"
#include "mongoc-cursor-private.h"
#include "mongoc-client-private.h"

typedef enum { NONE, CMD_RESPONSE, OP_GETMORE_RESPONSE } reading_from_t;
typedef enum { UNKNOWN, GETMORE_CMD, OP_GETMORE } getmore_type_t;
typedef struct _data_cmd_t {
   /* Two paths:
    * - Mongo 3.2+, sent "getMore" cmd, we're reading reply's "nextBatch" array
    * - Mongo 2.6 to 3, after "aggregate" or similar command we sent OP_GETMORE,
    *   we're reading the raw reply from a stream
    */
   mongoc_cursor_response_t response;
   mongoc_cursor_response_legacy_t response_legacy;
   reading_from_t reading_from;
   getmore_type_t getmore_type; /* cache after first getmore. */
   bson_t cmd;
} data_cmd_t;


static getmore_type_t
_getmore_type (mongoc_cursor_t *cursor)
{
   mongoc_server_stream_t *server_stream;
   int32_t wire_version;
   data_cmd_t *data = (data_cmd_t *) cursor->impl.data;
   if (data->getmore_type != UNKNOWN) {
      return data->getmore_type;
   }
   server_stream = _mongoc_cursor_fetch_stream (cursor);
   if (!server_stream) {
      return UNKNOWN;
   }
   wire_version = server_stream->sd->max_wire_version;
   mongoc_server_stream_cleanup (server_stream);

   // CDRIVER-4722: always GETMORE_CMD once WIRE_VERSION_MIN >=
   // WIRE_VERSION_4_2.
   if (_mongoc_cursor_use_op_msg (cursor, wire_version)) {
      data->getmore_type = GETMORE_CMD;
   } else {
      data->getmore_type = OP_GETMORE;
   }

   return data->getmore_type;
}


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   data_cmd_t *data = (data_cmd_t *) cursor->impl.data;
   bson_t copied_opts;
   bson_init (&copied_opts);

   cursor->operation_id = ++cursor->client->cluster.operation_id;
   /* commands like agg have a cursor field, so copy opts without "batchSize" */
   bson_copy_to_excluding_noinit (&cursor->opts, &copied_opts, "batchSize", "tailable", NULL);

   /* server replies to aggregate/listIndexes/listCollections with:
    * {cursor: {id: N, firstBatch: []}} */
   _mongoc_cursor_response_refresh (cursor, &data->cmd, &copied_opts, &data->response);
   data->reading_from = CMD_RESPONSE;
   bson_destroy (&copied_opts);
   return IN_BATCH;
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   data_cmd_t *data = (data_cmd_t *) cursor->impl.data;

   switch (data->reading_from) {
   case CMD_RESPONSE:
      _mongoc_cursor_response_read (cursor, &data->response, &cursor->current);
      break;
   case OP_GETMORE_RESPONSE:
      cursor->current = bson_reader_read (data->response_legacy.reader, NULL);
      break;
   case NONE:
   default:
      fprintf (stderr, "trying to pop from an uninitialized cursor reader.\n");
      BSON_ASSERT (false);
   }
   if (cursor->current) {
      return IN_BATCH;
   } else {
      return cursor->cursor_id ? END_OF_BATCH : DONE;
   }
}


static mongoc_cursor_state_t
_get_next_batch (mongoc_cursor_t *cursor)
{
   data_cmd_t *data = (data_cmd_t *) cursor->impl.data;
   bson_t getmore_cmd;
   getmore_type_t getmore_type = _getmore_type (cursor);

   switch (getmore_type) {
   case GETMORE_CMD:
      _mongoc_cursor_prepare_getmore_command (cursor, &getmore_cmd);
      _mongoc_cursor_response_refresh (cursor, &getmore_cmd, NULL /* opts */, &data->response);
      bson_destroy (&getmore_cmd);
      data->reading_from = CMD_RESPONSE;
      return IN_BATCH;
   case OP_GETMORE:
      _mongoc_cursor_op_getmore (cursor, &data->response_legacy);
      data->reading_from = OP_GETMORE_RESPONSE;
      return IN_BATCH;
   case UNKNOWN:
   default:
      return DONE;
   }
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_cmd_t *data = (data_cmd_t *) impl->data;
   bson_destroy (&data->response.reply);
   bson_destroy (&data->cmd);
   _mongoc_cursor_response_legacy_destroy (&data->response_legacy);
   bson_free (data);
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_cmd_t *data_src = (data_cmd_t *) src->data;
   data_cmd_t *data_dst = BSON_ALIGNED_ALLOC0 (data_cmd_t);
   bson_init (&data_dst->response.reply);
   _mongoc_cursor_response_legacy_init (&data_dst->response_legacy);
   bson_copy_to (&data_src->cmd, &data_dst->cmd);
   dst->data = data_dst;
}


mongoc_cursor_t *
_mongoc_cursor_cmd_new (mongoc_client_t *client,
                        const char *db_and_coll,
                        const bson_t *cmd,
                        const bson_t *opts,
                        const mongoc_read_prefs_t *user_prefs,
                        const mongoc_read_prefs_t *default_prefs,
                        const mongoc_read_concern_t *read_concern)
{
   BSON_ASSERT_PARAM (client);

   mongoc_cursor_t *cursor;
   data_cmd_t *data = BSON_ALIGNED_ALLOC0 (data_cmd_t);

   cursor = _mongoc_cursor_new_with_opts (client, db_and_coll, opts, user_prefs, default_prefs, read_concern);
   _mongoc_cursor_response_legacy_init (&data->response_legacy);
   _mongoc_cursor_check_and_copy_to (cursor, "command", cmd, &data->cmd);
   bson_init (&data->response.reply);
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.get_next_batch = _get_next_batch;
   cursor->impl.destroy = _destroy;
   cursor->impl.clone = _clone;
   cursor->impl.data = (void *) data;
   return cursor;
}


mongoc_cursor_t *
_mongoc_cursor_cmd_new_from_reply (mongoc_client_t *client, const bson_t *cmd, const bson_t *opts, bson_t *reply)
{
   BSON_ASSERT_PARAM (client);

   mongoc_cursor_t *cursor = _mongoc_cursor_cmd_new (client, NULL, cmd, opts, NULL, NULL, NULL);
   data_cmd_t *data = (data_cmd_t *) cursor->impl.data;

   data->reading_from = CMD_RESPONSE;
   cursor->state = IN_BATCH;

   bson_destroy (&data->response.reply);
   if (!bson_steal (&data->response.reply, reply)) {
      bson_destroy (&data->response.reply);
      BSON_ASSERT (bson_steal (&data->response.reply, bson_copy (reply)));
   }

   if (!_mongoc_cursor_start_reading_response (cursor, &data->response)) {
      bson_set_error (
         &cursor->error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "Couldn't parse cursor document");
   }

   if (0 != cursor->cursor_id && 0 == cursor->server_id) {
      // A non-zero cursor_id means the cursor is still open on the server.
      // Expect the "serverId" option to have been passed. The "serverId" option
      // identifies the server with the cursor.
      // The server with the cursor is required to send a "getMore" or
      // "killCursors" command.
      bson_set_error (&cursor->error,
                      MONGOC_ERROR_CURSOR,
                      MONGOC_ERROR_CURSOR_INVALID_CURSOR,
                      "Expected `serverId` option to identify server with open cursor "
                      "(cursor ID is %" PRId64 "). "
                      "Consider using `mongoc_client_select_server` and using the "
                      "resulting server ID to create the cursor.",
                      cursor->cursor_id);
      // Reset cursor_id to 0 to avoid an assertion error in
      // `mongoc_cursor_destroy` when attempting to send "killCursors".
      cursor->cursor_id = 0;
   }

   return cursor;
}
