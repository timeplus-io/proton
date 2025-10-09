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

typedef struct _data_change_stream_t {
   mongoc_cursor_response_t response;
   bson_t post_batch_resume_token;
} _data_change_stream_t;


static void
_update_post_batch_resume_token (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;
   bson_iter_t iter, child;

   if (mongoc_cursor_error (cursor, NULL)) {
      return;
   }

   if (bson_iter_init (&iter, &data->response.reply) &&
       bson_iter_find_descendant (&iter, "cursor.postBatchResumeToken", &child) && BSON_ITER_HOLDS_DOCUMENT (&child)) {
      uint32_t len;
      const uint8_t *buf;
      bson_t post_batch_resume_token;

      bson_iter_document (&child, &len, &buf);
      BSON_ASSERT (bson_init_static (&post_batch_resume_token, buf, len));
      bson_destroy (&data->post_batch_resume_token);
      bson_copy_to (&post_batch_resume_token, &data->post_batch_resume_token);
   }
}


static mongoc_cursor_state_t
_prime (mongoc_cursor_t *cursor)
{
   BSON_UNUSED (cursor);

   fprintf (stderr, "Prime unsupported on change stream cursor.");
   BSON_ASSERT (false);

   return IN_BATCH;
}


static mongoc_cursor_state_t
_pop_from_batch (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;
   _mongoc_cursor_response_read (cursor, &data->response, &cursor->current);
   if (cursor->current) {
      return IN_BATCH;
   } else {
      return cursor->cursor_id ? END_OF_BATCH : DONE;
   }
}


mongoc_cursor_state_t
_get_next_batch (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;
   bson_t getmore_cmd;

   _mongoc_cursor_prepare_getmore_command (cursor, &getmore_cmd);
   _mongoc_cursor_response_refresh (cursor, &getmore_cmd, NULL /* opts */, &data->response);
   bson_destroy (&getmore_cmd);

   _update_post_batch_resume_token (cursor);

   return IN_BATCH;
}


static void
_destroy (mongoc_cursor_impl_t *impl)
{
   _data_change_stream_t *data = (_data_change_stream_t *) impl->data;
   bson_destroy (&data->response.reply);
   bson_destroy (&data->post_batch_resume_token);
   bson_free (data);
}


static void
_clone (mongoc_cursor_impl_t *dst, const mongoc_cursor_impl_t *src)
{
   BSON_UNUSED (dst);
   BSON_UNUSED (src);

   fprintf (stderr, "Clone unsupported on change stream cursor.");
   BSON_ASSERT (false);
}


mongoc_cursor_t *
_mongoc_cursor_change_stream_new (mongoc_client_t *client, bson_t *reply, const bson_t *getmore_opts)
{
   mongoc_cursor_t *cursor;
   _data_change_stream_t *data;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (reply);

   data = BSON_ALIGNED_ALLOC0 (_data_change_stream_t);
   /* _mongoc_cursor_response_t.reply is already uninitialized and we can trust
    * that reply comes from mongoc_client_read_command_with_opts() */
   BSON_ASSERT (bson_steal (&data->response.reply, reply));
   bson_init (&data->post_batch_resume_token);

   cursor = _mongoc_cursor_new_with_opts (client, NULL, getmore_opts, NULL, NULL, NULL);
   cursor->impl.prime = _prime;
   cursor->impl.pop_from_batch = _pop_from_batch;
   cursor->impl.get_next_batch = _get_next_batch;
   cursor->impl.destroy = _destroy;
   cursor->impl.clone = _clone;
   cursor->impl.data = (void *) data;
   cursor->state = IN_BATCH;

   if (!_mongoc_cursor_start_reading_response (cursor, &data->response)) {
      bson_set_error (
         &cursor->error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "Couldn't parse cursor document");
   }

   _update_post_batch_resume_token (cursor);

   return cursor;
}


static bool
_bson_iter_has_next (bson_iter_t *iter)
{
   bson_iter_t iter_copy = {0};

   memcpy (&iter_copy, iter, sizeof (bson_iter_t));

   return bson_iter_next (&iter_copy);
}


bool
_mongoc_cursor_change_stream_end_of_batch (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;

   return !_bson_iter_has_next (&data->response.batch_iter);
}


const bson_t *
_mongoc_cursor_change_stream_get_post_batch_resume_token (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;

   return &data->post_batch_resume_token;
}


bool
_mongoc_cursor_change_stream_has_post_batch_resume_token (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;

   return !bson_empty (&data->post_batch_resume_token);
}


const bson_t *
_mongoc_cursor_change_stream_get_reply (mongoc_cursor_t *cursor)
{
   _data_change_stream_t *data = (_data_change_stream_t *) cursor->impl.data;

   return &data->response.reply;
}
