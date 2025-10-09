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

typedef struct _data_find_cmd_t {
   mongoc_cursor_response_t response;
   bson_t filter;
} data_find_cmd_t;


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   data_find_cmd_t *data = (data_find_cmd_t *) cursor->impl.data;
   bson_t find_cmd;

   bson_init (&find_cmd);
   cursor->operation_id = ++cursor->client->cluster.operation_id;
   /* construct { find: "<collection>", filter: {<filter>} } */
   _mongoc_cursor_prepare_find_command (cursor, &data->filter, &find_cmd);
   _mongoc_cursor_response_refresh (cursor, &find_cmd, &cursor->opts, &data->response);
   bson_destroy (&find_cmd);
   return IN_BATCH;
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   data_find_cmd_t *data = (data_find_cmd_t *) cursor->impl.data;
   _mongoc_cursor_response_read (cursor, &data->response, &cursor->current);
   if (cursor->current) {
      return IN_BATCH;
   } else {
      return cursor->cursor_id ? END_OF_BATCH : DONE;
   }
}


static mongoc_cursor_state_t
_get_next_batch (mongoc_cursor_t *cursor)
{
   data_find_cmd_t *data = (data_find_cmd_t *) cursor->impl.data;
   bson_t getmore_cmd;

   if (!cursor->cursor_id) {
      return DONE;
   }
   _mongoc_cursor_prepare_getmore_command (cursor, &getmore_cmd);
   _mongoc_cursor_response_refresh (cursor, &getmore_cmd, NULL /* opts */, &data->response);
   bson_destroy (&getmore_cmd);
   return IN_BATCH;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_find_cmd_t *data = (data_find_cmd_t *) impl->data;
   bson_destroy (&data->filter);
   bson_destroy (&data->response.reply);
   bson_free (data);
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_find_cmd_t *data_src = (data_find_cmd_t *) src->data;
   data_find_cmd_t *data_dst = BSON_ALIGNED_ALLOC0 (data_find_cmd_t);
   bson_init (&data_dst->response.reply);
   bson_copy_to (&data_src->filter, &data_dst->filter);
   dst->data = data_dst;
}


/* transition a find cursor to use the find command. */
void
_mongoc_cursor_impl_find_cmd_init (mongoc_cursor_t *cursor, bson_t *filter)
{
   data_find_cmd_t *data = BSON_ALIGNED_ALLOC0 (data_find_cmd_t);
   BSON_ASSERT (bson_steal (&data->filter, filter));
   bson_init (&data->response.reply);
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.get_next_batch = _get_next_batch;
   cursor->impl.destroy = _destroy;
   cursor->impl.clone = _clone;
   cursor->impl.data = (void *) data;
}
