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


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "gridfs"

#include "mongoc-bulk-operation.h"
#include "mongoc-client-private.h"
#include "mongoc-collection.h"
#include "mongoc-collection-private.h"
#include "mongoc-error.h"
#include "mongoc-index.h"
#include "mongoc-gridfs.h"
#include "mongoc-gridfs-private.h"
#include "mongoc-gridfs-file.h"
#include "mongoc-gridfs-file-private.h"
#include "mongoc-gridfs-file-list.h"
#include "mongoc-gridfs-file-list-private.h"
#include "mongoc-client.h"
#include "mongoc-trace-private.h"
#include "mongoc-cursor-private.h"
#include "mongoc-util-private.h"

#define MONGOC_GRIDFS_STREAM_CHUNK 4096


/**
 * _mongoc_gridfs_ensure_index:
 *
 * ensure gridfs indexes
 *
 * Ensure fast searches for chunks via [ files_id, n ]
 * Ensure fast searches for files via [ filename ]
 */
static bool
_mongoc_gridfs_ensure_index (mongoc_gridfs_t *gridfs, bson_error_t *error)
{
   bson_t keys;
   bson_t opts;
   bool r;

   ENTRY;

   bson_init (&opts);
   BSON_APPEND_BOOL (&opts, "unique", true);

   bson_init (&keys);

   BSON_APPEND_INT32 (&keys, "files_id", 1);
   BSON_APPEND_INT32 (&keys, "n", 1);

   r = _mongoc_collection_create_index_if_not_exists (gridfs->chunks, &keys, &opts, error);

   bson_destroy (&opts);
   bson_destroy (&keys);

   if (!r) {
      RETURN (r);
   }

   bson_init (&keys);

   BSON_APPEND_INT32 (&keys, "filename", 1);
   BSON_APPEND_INT32 (&keys, "uploadDate", 1);

   r = _mongoc_collection_create_index_if_not_exists (gridfs->files, &keys, NULL, error);

   bson_destroy (&keys);

   if (!r) {
      RETURN (r);
   }

   RETURN (1);
}


mongoc_gridfs_t *
_mongoc_gridfs_new (mongoc_client_t *client, const char *db, const char *prefix, bson_error_t *error)
{
   mongoc_gridfs_t *gridfs;
   char buf[128];
   bool r;
   uint32_t prefix_len;

   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db);

   if (!prefix) {
      prefix = "fs";
   }

   /* make sure prefix is short enough to bucket the chunks and files
    * collections
    */
   prefix_len = (uint32_t) strlen (prefix);
   BSON_ASSERT (prefix_len + sizeof (".chunks") < sizeof (buf));

   gridfs = (mongoc_gridfs_t *) bson_malloc0 (sizeof *gridfs);

   gridfs->client = client;

   bson_snprintf (buf, sizeof (buf), "%s.chunks", prefix);
   gridfs->chunks = mongoc_client_get_collection (client, db, buf);

   bson_snprintf (buf, sizeof (buf), "%s.files", prefix);
   gridfs->files = mongoc_client_get_collection (client, db, buf);

   r = _mongoc_gridfs_ensure_index (gridfs, error);

   if (!r) {
      mongoc_gridfs_destroy (gridfs);
      RETURN (NULL);
   }

   RETURN (gridfs);
}


bool
mongoc_gridfs_drop (mongoc_gridfs_t *gridfs, bson_error_t *error)
{
   bool r;

   ENTRY;

   r = mongoc_collection_drop (gridfs->files, error);
   if (!r) {
      RETURN (0);
   }

   r = mongoc_collection_drop (gridfs->chunks, error);
   if (!r) {
      RETURN (0);
   }

   RETURN (1);
}


void
mongoc_gridfs_destroy (mongoc_gridfs_t *gridfs)
{
   ENTRY;

   if (!gridfs) {
      EXIT;
   }

   mongoc_collection_destroy (gridfs->files);
   mongoc_collection_destroy (gridfs->chunks);

   bson_free (gridfs);

   EXIT;
}


/** find all matching gridfs files */
mongoc_gridfs_file_list_t *
mongoc_gridfs_find (mongoc_gridfs_t *gridfs, const bson_t *query)
{
   return _mongoc_gridfs_file_list_new (gridfs, query, 0);
}


/** find a single gridfs file */
mongoc_gridfs_file_t *
mongoc_gridfs_find_one (mongoc_gridfs_t *gridfs, const bson_t *query, bson_error_t *error)
{
   mongoc_gridfs_file_list_t *list;
   mongoc_gridfs_file_t *file;

   ENTRY;

   list = _mongoc_gridfs_file_list_new (gridfs, query, 1);

   file = mongoc_gridfs_file_list_next (list);
   if (!mongoc_gridfs_file_list_error (list, error) && error) {
      /* no error, but an error out-pointer was provided - clear it */
      memset (error, 0, sizeof (*error));
   }

   mongoc_gridfs_file_list_destroy (list);

   RETURN (file);
}


/** find all matching gridfs files */
mongoc_gridfs_file_list_t *
mongoc_gridfs_find_with_opts (mongoc_gridfs_t *gridfs, const bson_t *filter, const bson_t *opts)
{
   return _mongoc_gridfs_file_list_new_with_opts (gridfs, filter, opts);
}


/** find a single gridfs file */
mongoc_gridfs_file_t *
mongoc_gridfs_find_one_with_opts (mongoc_gridfs_t *gridfs,
                                  const bson_t *filter,
                                  const bson_t *opts,
                                  bson_error_t *error)
{
   mongoc_gridfs_file_list_t *list;
   mongoc_gridfs_file_t *file;
   bson_t new_opts;

   ENTRY;

   bson_init (&new_opts);

   if (opts) {
      bson_copy_to_excluding_noinit (opts, &new_opts, "limit", (char *) NULL);
   }

   BSON_APPEND_INT32 (&new_opts, "limit", 1);

   list = _mongoc_gridfs_file_list_new_with_opts (gridfs, filter, &new_opts);
   file = mongoc_gridfs_file_list_next (list);

   if (!mongoc_gridfs_file_list_error (list, error) && error) {
      /* no error, but an error out-pointer was provided - clear it */
      memset (error, 0, sizeof (*error));
   }

   mongoc_gridfs_file_list_destroy (list);
   bson_destroy (&new_opts);

   RETURN (file);
}


/** find a single gridfs file by filename */
mongoc_gridfs_file_t *
mongoc_gridfs_find_one_by_filename (mongoc_gridfs_t *gridfs, const char *filename, bson_error_t *error)
{
   mongoc_gridfs_file_t *file;

   bson_t filter;

   bson_init (&filter);

   bson_append_utf8 (&filter, "filename", -1, filename, -1);

   file = mongoc_gridfs_find_one_with_opts (gridfs, &filter, NULL, error);

   bson_destroy (&filter);

   return file;
}


/** create a gridfs file from a stream
 *
 * The stream is fully consumed in creating the file
 */
mongoc_gridfs_file_t *
mongoc_gridfs_create_file_from_stream (mongoc_gridfs_t *gridfs, mongoc_stream_t *stream, mongoc_gridfs_file_opt_t *opt)
{
   mongoc_gridfs_file_t *file;
   ssize_t r;
   uint8_t buf[MONGOC_GRIDFS_STREAM_CHUNK];
   mongoc_iovec_t iov;
   int timeout;

   ENTRY;

   BSON_ASSERT (gridfs);
   BSON_ASSERT (stream);

   iov.iov_base = (void *) buf;
   iov.iov_len = 0;

   file = _mongoc_gridfs_file_new (gridfs, opt);
   timeout = gridfs->client->cluster.sockettimeoutms;

   for (;;) {
      r = mongoc_stream_read (stream, iov.iov_base, MONGOC_GRIDFS_STREAM_CHUNK, 0, timeout);

      if (r > 0) {
         iov.iov_len = r;
         if (mongoc_gridfs_file_writev (file, &iov, 1, timeout) < 0) {
            MONGOC_ERROR ("%s", file->error.message);
            mongoc_gridfs_file_destroy (file);
            RETURN (NULL);
         }
      } else if (r == 0) {
         break;
      } else {
         MONGOC_ERROR ("Error reading from GridFS file source stream");
         mongoc_gridfs_file_destroy (file);
         RETURN (NULL);
      }
   }

   mongoc_stream_failed (stream);

   if (-1 == mongoc_gridfs_file_seek (file, 0, SEEK_SET)) {
      MONGOC_ERROR ("%s", file->error.message);
      mongoc_gridfs_file_destroy (file);
      RETURN (NULL);
   }

   RETURN (file);
}


/** create an empty gridfs file */
mongoc_gridfs_file_t *
mongoc_gridfs_create_file (mongoc_gridfs_t *gridfs, mongoc_gridfs_file_opt_t *opt)
{
   mongoc_gridfs_file_t *file;

   ENTRY;

   BSON_ASSERT (gridfs);

   file = _mongoc_gridfs_file_new (gridfs, opt);

   RETURN (file);
}

/** accessor functions for collections */
mongoc_collection_t *
mongoc_gridfs_get_files (mongoc_gridfs_t *gridfs)
{
   BSON_ASSERT (gridfs);

   return gridfs->files;
}

mongoc_collection_t *
mongoc_gridfs_get_chunks (mongoc_gridfs_t *gridfs)
{
   BSON_ASSERT (gridfs);

   return gridfs->chunks;
}


bool
mongoc_gridfs_remove_by_filename (mongoc_gridfs_t *gridfs, const char *filename, bson_error_t *error)
{
   mongoc_bulk_operation_t *bulk_files = NULL;
   mongoc_bulk_operation_t *bulk_chunks = NULL;
   mongoc_cursor_t *cursor = NULL;
   bson_error_t files_error;
   bson_error_t chunks_error;
   const bson_t *doc;
   const char *key;
   char keybuf[16];
   int count = 0;
   bool chunks_ret;
   bool files_ret;
   bool ret = false;
   bson_iter_t iter;
   bson_t *files_q = NULL;
   bson_t *chunks_q = NULL;
   bson_t find_filter = BSON_INITIALIZER;
   bson_t find_opts = BSON_INITIALIZER;
   bson_t find_opts_project;
   bson_t ar = BSON_INITIALIZER;
   bson_t opts = BSON_INITIALIZER;

   BSON_ASSERT (gridfs);

   if (!filename) {
      bson_set_error (
         error, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_INVALID_FILENAME, "A non-NULL filename must be specified.");
      return false;
   }

   /*
    * Find all files matching this filename. Hopefully just one, but not
    * strictly required!
    */

   BSON_APPEND_UTF8 (&find_filter, "filename", filename);
   BSON_APPEND_DOCUMENT_BEGIN (&find_opts, "projection", &find_opts_project);
   BSON_APPEND_INT32 (&find_opts_project, "_id", 1);
   bson_append_document_end (&find_opts, &find_opts_project);

   cursor = _mongoc_cursor_find_new (gridfs->client,
                                     gridfs->files->ns,
                                     &find_filter,
                                     &find_opts,
                                     NULL /* user_prefs */,
                                     NULL /* default_prefs */,
                                     NULL /* read_concern */);

   BSON_ASSERT (cursor);

   while (mongoc_cursor_next (cursor, &doc)) {
      if (bson_iter_init_find (&iter, doc, "_id")) {
         const bson_value_t *value = bson_iter_value (&iter);

         bson_uint32_to_string (count, &key, keybuf, sizeof keybuf);
         BSON_APPEND_VALUE (&ar, key, value);
      }
   }

   if (mongoc_cursor_error (cursor, error)) {
      goto failure;
   }

   bson_append_bool (&opts, "ordered", 7, false);
   bulk_files = mongoc_collection_create_bulk_operation_with_opts (gridfs->files, &opts);
   bulk_chunks = mongoc_collection_create_bulk_operation_with_opts (gridfs->chunks, &opts);

   bson_destroy (&opts);

   files_q = BCON_NEW ("_id", "{", "$in", BCON_ARRAY (&ar), "}");
   chunks_q = BCON_NEW ("files_id", "{", "$in", BCON_ARRAY (&ar), "}");

   mongoc_bulk_operation_remove (bulk_files, files_q);
   mongoc_bulk_operation_remove (bulk_chunks, chunks_q);

   files_ret = mongoc_bulk_operation_execute (bulk_files, NULL, &files_error);
   chunks_ret = mongoc_bulk_operation_execute (bulk_chunks, NULL, &chunks_error);

   if (error) {
      if (!files_ret) {
         memcpy (error, &files_error, sizeof *error);
      } else if (!chunks_ret) {
         memcpy (error, &chunks_error, sizeof *error);
      }
   }

   ret = (files_ret && chunks_ret);

failure:
   if (cursor) {
      mongoc_cursor_destroy (cursor);
   }
   if (bulk_files) {
      mongoc_bulk_operation_destroy (bulk_files);
   }
   if (bulk_chunks) {
      mongoc_bulk_operation_destroy (bulk_chunks);
   }
   bson_destroy (&find_filter);
   bson_destroy (&find_opts);
   bson_destroy (&ar);
   if (files_q) {
      bson_destroy (files_q);
   }
   if (chunks_q) {
      bson_destroy (chunks_q);
   }

   return ret;
}
