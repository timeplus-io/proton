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

/* This cursor is returned by the deprecated functions mongoc_client_command,
 * mongoc_database_command, and mongoc_collection_command. It runs the command
 * on the first call to mongoc_cursor_next and returns the only result. */
typedef struct _data_cmd_deprecated_t {
   bson_t cmd;
   bson_t reply;
} data_cmd_deprecated_t;


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   data_cmd_deprecated_t *data = (data_cmd_deprecated_t *) cursor->impl.data;
   bson_destroy (&data->reply);
   if (_mongoc_cursor_run_command (cursor, &data->cmd, &cursor->opts, &data->reply, true)) {
      return IN_BATCH;
   } else {
      return DONE;
   }
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   data_cmd_deprecated_t *data = (data_cmd_deprecated_t *) cursor->impl.data;
   cursor->current = &data->reply;
   /* don't return DONE here. a cursor is marked DONE when it returns NULL. */
   return END_OF_BATCH;
}

static mongoc_cursor_state_t
_get_next_batch (mongoc_cursor_t *cursor)
{
   BSON_UNUSED (cursor);

   /* there's no next batch to get, return DONE immediately. */
   return DONE;
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   data_cmd_deprecated_t *data_src = (data_cmd_deprecated_t *) src->data;
   data_cmd_deprecated_t *data_dst = BSON_ALIGNED_ALLOC0 (data_cmd_deprecated_t);
   bson_init (&data_dst->reply);
   bson_copy_to (&data_src->cmd, &data_dst->cmd);
   dst->data = data_dst;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   data_cmd_deprecated_t *data = (data_cmd_deprecated_t *) impl->data;
   bson_destroy (&data->reply);
   bson_destroy (&data->cmd);
   bson_free (data);
}


mongoc_cursor_t *
_mongoc_cursor_cmd_deprecated_new (mongoc_client_t *client,
                                   const char *db_and_coll,
                                   const bson_t *cmd,
                                   const mongoc_read_prefs_t *read_prefs)
{
   BSON_ASSERT_PARAM (client);

   mongoc_cursor_t *cursor = _mongoc_cursor_new_with_opts (
      client, db_and_coll, NULL, read_prefs /* user prefs */, NULL /* default prefs */, NULL);
   data_cmd_deprecated_t *data = BSON_ALIGNED_ALLOC0 (data_cmd_deprecated_t);
   _mongoc_cursor_check_and_copy_to (cursor, "command", cmd, &data->cmd);
   bson_init (&data->reply);
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.get_next_batch = _get_next_batch;
   cursor->impl.data = data;
   cursor->impl.clone = _clone;
   cursor->impl.destroy = _destroy;
   return cursor;
}
