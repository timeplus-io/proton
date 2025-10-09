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

#include "bson/bson.h"
#include "mongoc.h"
#include "mongoc-cursor-private.h"
#include "mongoc-database-private.h"
#include "mongoc-gridfs-bucket-private.h"
#include "mongoc-gridfs-bucket-file-private.h"
#include "mongoc-opts-private.h"
#include "mongoc-read-concern-private.h"
#include "mongoc-stream-gridfs-download-private.h"
#include "mongoc-stream-gridfs-upload-private.h"
#include "mongoc-stream-private.h"
#include "mongoc-write-concern-private.h"

/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_find_file_with_id --
 *
 *       Attempts to find the file corresponding to the given file_id in
 *       GridFS.
 *
 * Return:
 *       True on success and initializes file. Otherwise, returns false
 *       and sets error.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_gridfs_find_file_with_id (mongoc_gridfs_bucket_t *bucket,
                                  const bson_value_t *file_id,
                                  bson_t *file,
                                  bson_error_t *error)
{
   mongoc_cursor_t *cursor;
   bson_t filter;
   const bson_t *doc;
   bool r;

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);

   bson_init (&filter);

   BSON_APPEND_VALUE (&filter, "_id", file_id);

   cursor = mongoc_collection_find_with_opts (bucket->files, &filter, NULL, NULL);
   bson_destroy (&filter);

   r = mongoc_cursor_next (cursor, &doc);
   if (!r) {
      if (!mongoc_cursor_error (cursor, error)) {
         bson_set_error (
            error, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_BUCKET_FILE_NOT_FOUND, "No file with given id exists");
      }
   } else {
      if (file) {
         bson_copy_to (doc, file);
      }
   }

   mongoc_cursor_destroy (cursor);
   return r;
}

mongoc_gridfs_bucket_t *
mongoc_gridfs_bucket_new (mongoc_database_t *db,
                          const bson_t *opts,
                          const mongoc_read_prefs_t *read_prefs,
                          bson_error_t *error)
{
   mongoc_gridfs_bucket_t *bucket;
   char buf[128];
   mongoc_gridfs_bucket_opts_t gridfs_opts;

   BSON_ASSERT (db);

   if (!_mongoc_gridfs_bucket_opts_parse (db->client, opts, &gridfs_opts, error)) {
      _mongoc_gridfs_bucket_opts_cleanup (&gridfs_opts);
      return NULL;
   }

   /* Initialize the bucket fields */
   if (strlen (gridfs_opts.bucketName) + strlen (".chunks") + 1 > sizeof (buf)) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "bucketName \"%s\" must have fewer than %d characters",
                      gridfs_opts.bucketName,
                      (int) (sizeof (buf) - (strlen (".chunks") + 1)));
   }

   bucket = (mongoc_gridfs_bucket_t *) bson_malloc0 (sizeof *bucket);

   bson_snprintf (buf, sizeof (buf), "%s.chunks", gridfs_opts.bucketName);
   bucket->chunks = mongoc_database_get_collection (db, buf);

   bson_snprintf (buf, sizeof (buf), "%s.files", gridfs_opts.bucketName);
   bucket->files = mongoc_database_get_collection (db, buf);

   if (gridfs_opts.writeConcern) {
      mongoc_collection_set_write_concern (bucket->chunks, gridfs_opts.writeConcern);
      mongoc_collection_set_write_concern (bucket->files, gridfs_opts.writeConcern);
   }

   if (gridfs_opts.readConcern) {
      mongoc_collection_set_read_concern (bucket->chunks, gridfs_opts.readConcern);
      mongoc_collection_set_read_concern (bucket->files, gridfs_opts.readConcern);
   }

   if (read_prefs) {
      mongoc_collection_set_read_prefs (bucket->chunks, read_prefs);
      mongoc_collection_set_read_prefs (bucket->files, read_prefs);
   }

   bucket->chunk_size = gridfs_opts.chunkSizeBytes;
   bucket->bucket_name = bson_strdup (gridfs_opts.bucketName);

   _mongoc_gridfs_bucket_opts_cleanup (&gridfs_opts);

   return bucket;
}


mongoc_stream_t *
mongoc_gridfs_bucket_open_upload_stream_with_id (mongoc_gridfs_bucket_t *bucket,
                                                 const bson_value_t *file_id,
                                                 const char *filename,
                                                 const bson_t *opts,
                                                 bson_error_t *error)
{
   mongoc_gridfs_bucket_file_t *file;
   size_t len;
   mongoc_gridfs_bucket_upload_opts_t gridfs_opts;

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);
   BSON_ASSERT (filename);

   if (!_mongoc_gridfs_bucket_upload_opts_parse (bucket->files->client, opts, &gridfs_opts, error)) {
      _mongoc_gridfs_bucket_upload_opts_cleanup (&gridfs_opts);
      return NULL;
   }

   /* default to bucket's chunk size. */
   if (!gridfs_opts.chunkSizeBytes) {
      gridfs_opts.chunkSizeBytes = bucket->chunk_size;
   }

   /* Initialize the file's fields */
   len = strlen (filename);

   file = (mongoc_gridfs_bucket_file_t *) bson_malloc0 (sizeof *file);

   file->filename = bson_malloc0 (len + 1);
   bson_strncpy (file->filename, filename, len + 1);

   file->file_id = (bson_value_t *) bson_malloc0 (sizeof *(file->file_id));
   bson_value_copy (file_id, file->file_id);

   file->bucket = bucket;
   file->chunk_size = gridfs_opts.chunkSizeBytes;
   file->metadata = bson_copy (&gridfs_opts.metadata);
   file->buffer = bson_malloc ((size_t) gridfs_opts.chunkSizeBytes);
   file->in_buffer = 0;

   _mongoc_gridfs_bucket_upload_opts_cleanup (&gridfs_opts);
   return _mongoc_upload_stream_gridfs_new (file);
}

mongoc_stream_t *
mongoc_gridfs_bucket_open_upload_stream (mongoc_gridfs_bucket_t *bucket,
                                         const char *filename,
                                         const bson_t *opts,
                                         bson_value_t *file_id /* OUT */,
                                         bson_error_t *error)
{
   mongoc_stream_t *stream;
   bson_oid_t object_id;
   bson_value_t val;

   BSON_ASSERT (bucket);
   BSON_ASSERT (filename);

   /* Create an objectId to use as the file's id */
   bson_oid_init (&object_id, NULL);
   val.value_type = BSON_TYPE_OID;
   val.value.v_oid = object_id;

   stream = mongoc_gridfs_bucket_open_upload_stream_with_id (bucket, &val, filename, opts, error);

   if (!stream) {
      return NULL;
   }

   if (file_id) {
      bson_value_copy (&val, file_id);
   }

   return stream;
}

bool
mongoc_gridfs_bucket_upload_from_stream_with_id (mongoc_gridfs_bucket_t *bucket,
                                                 const bson_value_t *file_id,
                                                 const char *filename,
                                                 mongoc_stream_t *source,
                                                 const bson_t *opts,
                                                 bson_error_t *error)
{
   mongoc_stream_t *upload_stream;
   ssize_t bytes_read;
   ssize_t bytes_written;
   char buf[512];

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);
   BSON_ASSERT (filename);
   BSON_ASSERT (source);

   upload_stream = mongoc_gridfs_bucket_open_upload_stream_with_id (bucket, file_id, filename, opts, error);

   if (!upload_stream) {
      return false;
   }

   while ((bytes_read = mongoc_stream_read (source, buf, 512, 1, 0)) > 0) {
      bytes_written = mongoc_stream_write (upload_stream, buf, bytes_read, 0);
      if (bytes_written < 0) {
         BSON_ASSERT (mongoc_gridfs_bucket_stream_error (upload_stream, error));
         mongoc_gridfs_bucket_abort_upload (upload_stream);
         mongoc_stream_destroy (upload_stream);
         return false;
      }
   }

   if (bytes_read < 0) {
      mongoc_gridfs_bucket_abort_upload (upload_stream);
      bson_set_error (
         error, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_BUCKET_STREAM, "Error occurred on the provided stream.");
      mongoc_stream_destroy (upload_stream);
      return false;
   } else {
      mongoc_stream_destroy (upload_stream);
      return true;
   }
}

bool
mongoc_gridfs_bucket_upload_from_stream (mongoc_gridfs_bucket_t *bucket,
                                         const char *filename,
                                         mongoc_stream_t *source,
                                         const bson_t *opts,
                                         bson_value_t *file_id /* OUT */,
                                         bson_error_t *error)
{
   bool r;
   bson_oid_t object_id;
   bson_value_t val;

   BSON_ASSERT (bucket);
   BSON_ASSERT (filename);
   BSON_ASSERT (source);

   /* Create an objectId to use as the file's id */
   bson_oid_init (&object_id, bson_context_get_default ());
   val.value_type = BSON_TYPE_OID;
   val.value.v_oid = object_id;

   r = mongoc_gridfs_bucket_upload_from_stream_with_id (bucket, &val, filename, source, opts, error);

   if (!r) {
      return false;
   }

   if (file_id) {
      bson_value_copy (&val, file_id);
   }

   return true;
}


mongoc_stream_t *
mongoc_gridfs_bucket_open_download_stream (mongoc_gridfs_bucket_t *bucket,
                                           const bson_value_t *file_id,
                                           bson_error_t *error)
{
   mongoc_gridfs_bucket_file_t *file;
   bson_t file_doc;
   const char *key;
   bson_iter_t iter;
   uint32_t data_len;
   const uint8_t *data;
   bool r;

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);

   r = _mongoc_gridfs_find_file_with_id (bucket, file_id, &file_doc, error);
   if (!r) {
      /* Error should already be set. */
      return NULL;
   }

   if (!bson_iter_init (&iter, &file_doc)) {
      bson_set_error (error, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "File document malformed");
      return NULL;
   }

   file = (mongoc_gridfs_bucket_file_t *) bson_malloc0 (sizeof *file);

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);
      if (strcmp (key, "length") == 0) {
         file->length = bson_iter_as_int64 (&iter);
      } else if (strcmp (key, "chunkSize") == 0) {
         file->chunk_size = bson_iter_int32 (&iter);
      } else if (strcmp (key, "filename") == 0) {
         file->filename = bson_strdup (bson_iter_utf8 (&iter, NULL));
      } else if (strcmp (key, "metadata") == 0) {
         bson_iter_document (&iter, &data_len, &data);
         file->metadata = bson_new_from_data (data, data_len);
      }
   }

   bson_destroy (&file_doc);

   file->file_id = (bson_value_t *) bson_malloc0 (sizeof *(file->file_id));
   bson_value_copy (file_id, file->file_id);
   file->bucket = bucket;
   file->buffer = bson_malloc0 ((size_t) file->chunk_size);

   BSON_ASSERT (file->file_id);

   return _mongoc_download_stream_gridfs_new (file);
}

bool
mongoc_gridfs_bucket_download_to_stream (mongoc_gridfs_bucket_t *bucket,
                                         const bson_value_t *file_id,
                                         mongoc_stream_t *destination,
                                         bson_error_t *error)
{
   mongoc_stream_t *download_stream;
   ssize_t bytes_read;
   ssize_t bytes_written;
   char buf[512];

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);
   BSON_ASSERT (destination);

   /* Make the download stream */
   download_stream = mongoc_gridfs_bucket_open_download_stream (bucket, file_id, error);

   while ((bytes_read = mongoc_stream_read (download_stream, buf, 256, 1, 0)) > 0) {
      bytes_written = mongoc_stream_write (destination, buf, bytes_read, 0);
      if (bytes_written < 0) {
         bson_set_error (
            error, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_BUCKET_STREAM, "Error occurred on the provided stream.");
         mongoc_stream_destroy (download_stream);
         return false;
      }
   }

   mongoc_stream_destroy (download_stream);
   return bytes_read != -1;
}

bool
mongoc_gridfs_bucket_delete_by_id (mongoc_gridfs_bucket_t *bucket, const bson_value_t *file_id, bson_error_t *error)
{
   bson_t files_selector;
   bson_t chunks_selector;
   bson_t reply;
   bson_iter_t iter;
   bool r;

   BSON_ASSERT (bucket);
   BSON_ASSERT (file_id);

   bson_init (&files_selector);

   BSON_APPEND_VALUE (&files_selector, "_id", file_id);

   r = mongoc_collection_delete_one (bucket->files, &files_selector, NULL, &reply, error);
   bson_destroy (&files_selector);
   if (!r) {
      bson_destroy (&reply);
      return false;
   }

   BSON_ASSERT (bson_iter_init_find (&iter, &reply, "deletedCount"));

   if (bson_iter_as_int64 (&iter) != 1) {
      bson_set_error (error, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_BUCKET_FILE_NOT_FOUND, "File not found");
      bson_destroy (&reply);
      return false;
   }

   bson_destroy (&reply);

   bson_init (&chunks_selector);

   BSON_APPEND_VALUE (&chunks_selector, "files_id", file_id);

   r = mongoc_collection_delete_many (bucket->chunks, &chunks_selector, NULL, NULL, error);
   bson_destroy (&chunks_selector);
   if (!r) {
      return false;
   }

   return true;
}

mongoc_cursor_t *
mongoc_gridfs_bucket_find (mongoc_gridfs_bucket_t *bucket, const bson_t *filter, const bson_t *opts)
{
   mongoc_cursor_t *cursor;
   BSON_ASSERT (bucket);
   BSON_ASSERT (filter);

   cursor = mongoc_collection_find_with_opts (bucket->files, filter, opts, NULL);
   if (!cursor->error.code && opts && bson_has_field (opts, "sessionId")) {
      bson_set_error (
         &cursor->error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "Cannot pass sessionId as an option");
   }
   return cursor;
}

bool
mongoc_gridfs_bucket_stream_error (mongoc_stream_t *stream, bson_error_t *error)
{
   bson_error_t *stream_err;
   BSON_ASSERT (stream);
   BSON_ASSERT (error);


   if (stream->type == MONGOC_STREAM_GRIDFS_UPLOAD) {
      stream_err = &((mongoc_gridfs_upload_stream_t *) stream)->file->err;
   } else if (stream->type == MONGOC_STREAM_GRIDFS_DOWNLOAD) {
      stream_err = &((mongoc_gridfs_download_stream_t *) stream)->file->err;
   } else {
      return false;
   }

   if (stream_err->code) {
      memcpy (error, stream_err, sizeof (*stream_err));
      return true;
   } else {
      return false;
   }
}

void
mongoc_gridfs_bucket_destroy (mongoc_gridfs_bucket_t *bucket)
{
   if (bucket) {
      mongoc_collection_destroy (bucket->chunks);
      mongoc_collection_destroy (bucket->files);
      bson_free (bucket->bucket_name);
      bson_free (bucket);
   }
}

bool
mongoc_gridfs_bucket_abort_upload (mongoc_stream_t *stream)
{
   mongoc_gridfs_bucket_file_t *file;
   bson_t chunks_selector;
   bool r;

   BSON_ASSERT (stream);
   BSON_ASSERT (stream->type == MONGOC_STREAM_GRIDFS_UPLOAD);

   file = ((mongoc_gridfs_upload_stream_t *) stream)->file;

   /* Pretend we've already saved. This way we won't add an entry to the files
    * collection when the stream is closed */
   file->saved = true;

   bson_init (&chunks_selector);
   BSON_APPEND_VALUE (&chunks_selector, "files_id", file->file_id);

   r = mongoc_collection_delete_many (file->bucket->chunks, &chunks_selector, NULL, NULL, &file->err);
   bson_destroy (&chunks_selector);
   return r;
}
