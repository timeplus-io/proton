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

typedef struct _data_array_t {
   bson_t cmd;
   bson_t array;
   bson_iter_t iter;
   bson_t bson; /* current document */
   char *field_name;
} data_array_t;


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   bson_iter_t iter;
   data_array_t *data = (data_array_t *) cursor->impl.data;

   bson_destroy (&data->array);
   /* this cursor is only used with the listDatabases command. it iterates
    * over the array in the response's "databases" field. */
   if (_mongoc_cursor_run_command (cursor, &data->cmd, &cursor->opts, &data->array, false) &&
       bson_iter_init_find (&iter, &data->array, data->field_name) && BSON_ITER_HOLDS_ARRAY (&iter) &&
       bson_iter_recurse (&iter, &data->iter)) {
      return IN_BATCH;
   }
   return DONE;
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   uint32_t document_len;
   const uint8_t *document;
   data_array_t *data = (data_array_t *) cursor->impl.data;
   if (bson_iter_next (&data->iter)) {
      bson_iter_document (&data->iter, &document_len, &document);
      BSON_ASSERT (bson_init_static (&data->bson, document, document_len));
      cursor->current = &data->bson;
      return IN_BATCH;
   }
   return DONE;
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_array_t *data_dst = BSON_ALIGNED_ALLOC0 (data_array_t);
   data_array_t *data_src = (data_array_t *) src->data;
   bson_init (&data_dst->array);
   bson_copy_to (&data_src->cmd, &data_dst->cmd);
   data_dst->field_name = bson_strdup (data_src->field_name);
   dst->data = data_dst;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_array_t *data = (data_array_t *) impl->data;
   bson_destroy (&data->array);
   bson_destroy (&data->cmd);
   bson_free (data->field_name);
   bson_free (data);
}


mongoc_cursor_t *
_mongoc_cursor_array_new (
   mongoc_client_t *client, const char *db_and_coll, const bson_t *cmd, const bson_t *opts, const char *field_name)
{
   BSON_ASSERT_PARAM (client);

   mongoc_cursor_t *cursor = _mongoc_cursor_new_with_opts (client, db_and_coll, opts, NULL, NULL, NULL);
   data_array_t *data = BSON_ALIGNED_ALLOC0 (data_array_t);
   bson_copy_to (cmd, &data->cmd);
   bson_init (&data->array);
   data->field_name = bson_strdup (field_name);
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.destroy = _destroy;
   cursor->impl.clone = _clone;
   cursor->impl.data = (void *) data;
   return cursor;
}
