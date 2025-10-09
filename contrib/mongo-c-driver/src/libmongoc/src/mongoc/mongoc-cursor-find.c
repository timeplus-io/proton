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

typedef struct _data_find_t {
   bson_t filter;
} data_find_t;


extern void
_mongoc_cursor_impl_find_cmd_init (mongoc_cursor_t *cursor, bson_t *filter);
extern void
_mongoc_cursor_impl_find_opquery_init (mongoc_cursor_t *cursor, bson_t *filter);


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   int32_t wire_version;
   mongoc_server_stream_t *server_stream;
   data_find_t *data = (data_find_t *) cursor->impl.data;

   /* determine if this should be a command or op_query cursor. */
   server_stream = _mongoc_cursor_fetch_stream (cursor);
   if (!server_stream) {
      return DONE;
   }
   wire_version = server_stream->sd->max_wire_version;
   mongoc_server_stream_cleanup (server_stream);

   /* set all mongoc_impl_t function pointers. */
   /* CDRIVER-4722: always find_cmd when server >= 4.2 */
   if (_mongoc_cursor_use_op_msg (cursor, wire_version)) {
      _mongoc_cursor_impl_find_cmd_init (cursor, &data->filter /* stolen */);
   } else {
      _mongoc_cursor_impl_find_opquery_init (cursor, &data->filter /* stolen */);
   }
   /* destroy this impl data since impl functions have been replaced. */
   bson_free (data);
   /* prime with the new implementation. */
   return cursor->impl.prime (cursor);
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_find_t *data_dst = BSON_ALIGNED_ALLOC0 (data_find_t);
   data_find_t *data_src = (data_find_t *) src->data;
   bson_copy_to (&data_src->filter, &data_dst->filter);
   dst->data = data_dst;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_find_t *data = (data_find_t *) impl->data;
   bson_destroy (&data->filter);
   bson_free (data);
}


mongoc_cursor_t *
_mongoc_cursor_find_new (mongoc_client_t *client,
                         const char *db_and_coll,
                         const bson_t *filter,
                         const bson_t *opts,
                         const mongoc_read_prefs_t *user_prefs,
                         const mongoc_read_prefs_t *default_prefs,
                         const mongoc_read_concern_t *read_concern)
{
   BSON_ASSERT_PARAM (client);

   mongoc_cursor_t *cursor;
   data_find_t *data = BSON_ALIGNED_ALLOC0 (data_find_t);
   cursor = _mongoc_cursor_new_with_opts (client, db_and_coll, opts, user_prefs, default_prefs, read_concern);
   _mongoc_cursor_check_and_copy_to (cursor, "filter", filter, &data->filter);
   cursor->impl.prime = _prime;
   cursor->impl.clone = _clone;
   cursor->impl.destroy = _destroy;
   cursor->impl.data = data;
   return cursor;
}
