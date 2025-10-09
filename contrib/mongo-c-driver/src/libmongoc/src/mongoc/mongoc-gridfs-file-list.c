/*
 * Copyright 2013 MongoDB Inc.
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


#include <limits.h>

#include "mongoc-cursor.h"
#include "mongoc-cursor-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-gridfs.h"
#include "mongoc-gridfs-private.h"
#include "mongoc-gridfs-file.h"
#include "mongoc-gridfs-file-private.h"
#include "mongoc-gridfs-file-list.h"
#include "mongoc-gridfs-file-list-private.h"
#include "mongoc-trace-private.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "gridfs_file_list"


mongoc_gridfs_file_list_t *
_mongoc_gridfs_file_list_new (mongoc_gridfs_t *gridfs, const bson_t *query, uint32_t limit)
{
   mongoc_gridfs_file_list_t *list;
   mongoc_cursor_t *cursor;
   bool use_unwrapped;
   bson_t opts;
   bson_t unwrapped;
   bson_error_t error;
   bson_init (&opts);
   use_unwrapped = _mongoc_cursor_translate_dollar_query_opts (query, &opts, &unwrapped, &error);


   cursor = _mongoc_cursor_find_new (gridfs->client,
                                     gridfs->files->ns,
                                     use_unwrapped ? &unwrapped : query,
                                     &opts,
                                     NULL,
                                     gridfs->files->read_prefs,
                                     gridfs->files->read_concern);
   BSON_ASSERT (cursor);
   bson_destroy (&opts);
   if (limit) {
      (void) mongoc_cursor_set_limit (cursor, limit);
   }
   bson_destroy (&unwrapped);
   if (error.domain) {
      memcpy (&cursor->error, &error, sizeof (bson_error_t));
   }

   list = (mongoc_gridfs_file_list_t *) bson_malloc0 (sizeof *list);

   list->cursor = cursor;
   list->gridfs = gridfs;

   return list;
}


mongoc_gridfs_file_list_t *
_mongoc_gridfs_file_list_new_with_opts (mongoc_gridfs_t *gridfs, const bson_t *filter, const bson_t *opts)
{
   mongoc_gridfs_file_list_t *list;
   mongoc_cursor_t *cursor;

   cursor = mongoc_collection_find_with_opts (gridfs->files, filter, opts, NULL /* read prefs */);

   BSON_ASSERT (cursor);

   list = (mongoc_gridfs_file_list_t *) bson_malloc0 (sizeof *list);

   list->cursor = cursor;
   list->gridfs = gridfs;

   return list;
}


mongoc_gridfs_file_t *
mongoc_gridfs_file_list_next (mongoc_gridfs_file_list_t *list)
{
   const bson_t *bson;

   BSON_ASSERT (list);

   if (mongoc_cursor_next (list->cursor, &bson)) {
      return _mongoc_gridfs_file_new_from_bson (list->gridfs, bson);
   } else {
      return NULL;
   }
}


bool
mongoc_gridfs_file_list_error (mongoc_gridfs_file_list_t *list, bson_error_t *error)
{
   return mongoc_cursor_error (list->cursor, error);
}


void
mongoc_gridfs_file_list_destroy (mongoc_gridfs_file_list_t *list)
{
   if (!list) {
      return;
   }

   mongoc_cursor_destroy (list->cursor);
   bson_free (list);
}
