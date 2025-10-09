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
#include "mongoc-rpc-private.h"
#include "mongoc-cursor-private.h"

typedef struct _data_find_opquery_t {
   mongoc_cursor_response_legacy_t response_legacy;
   bson_t filter;
} data_find_opquery_t;


static bool
_hit_limit (mongoc_cursor_t *cursor)
{
   int64_t limit, limit_abs;
   limit = mongoc_cursor_get_limit (cursor);
   /* don't use llabs, that is a C99 function. */
   limit_abs = limit > 0 ? limit : -limit;
   /* mark as done if we've hit the limit. */
   if (limit && cursor->count >= limit_abs) {
      return true;
   }
   return false;
}


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   data_find_opquery_t *data = (data_find_opquery_t *) cursor->impl.data;
   if (_hit_limit (cursor)) {
      return DONE;
   }

   _mongoc_cursor_op_query_find (cursor, &data->filter, &data->response_legacy);
   return IN_BATCH;
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   data_find_opquery_t *data = (data_find_opquery_t *) cursor->impl.data;

   if (_hit_limit (cursor)) {
      return DONE;
   }

   cursor->current = bson_reader_read (data->response_legacy.reader, NULL);
   if (cursor->current) {
      return IN_BATCH;
   } else {
      return cursor->cursor_id ? END_OF_BATCH : DONE;
   }
}


static mongoc_cursor_state_t
_get_next_batch (mongoc_cursor_t *cursor)
{
   data_find_opquery_t *data = (data_find_opquery_t *) cursor->impl.data;
   _mongoc_cursor_op_getmore (cursor, &data->response_legacy);
   return IN_BATCH;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_find_opquery_t *data = (data_find_opquery_t *) impl->data;
   _mongoc_cursor_response_legacy_destroy (&data->response_legacy);
   bson_destroy (&data->filter);
   bson_free (data);
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_find_opquery_t *data_dst = bson_malloc0 (sizeof (data_find_opquery_t));
   data_find_opquery_t *data_src = (data_find_opquery_t *) src->data;
   _mongoc_cursor_response_legacy_init (&data_dst->response_legacy);
   bson_copy_to (&data_src->filter, &data_dst->filter);
   dst->data = data_dst;
}


void
_mongoc_cursor_impl_find_opquery_init (mongoc_cursor_t *cursor, bson_t *filter)
{
   data_find_opquery_t *data = BSON_ALIGNED_ALLOC0 (data_find_opquery_t);
   _mongoc_cursor_response_legacy_init (&data->response_legacy);
   BSON_ASSERT (bson_steal (&data->filter, filter));
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.get_next_batch = _get_next_batch;
   cursor->impl.destroy = _destroy;
   cursor->impl.clone = _clone;
   cursor->impl.data = data;
}
