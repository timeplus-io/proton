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
#include "mongoc-gridfs-bucket-file-private.h"
#include "mongoc-gridfs-bucket-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-stream-gridfs-download-private.h"
#include "mongoc-stream-gridfs-upload-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-util-private.h"

/* Returns the minimum of two numbers */
static size_t
_mongoc_min (const size_t a, const size_t b)
{
   return a < b ? a : b;
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_bucket_create_indexes --
 *
 *       Creates the indexes needed for GridFS on the 'files' and 'chunks'
 *       collections.
 *
 * Return:
 *       True if creating the indexes was successful, otherwise returns
 *       false.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_gridfs_bucket_create_indexes (mongoc_gridfs_bucket_t *bucket, bson_error_t *error)
{
   mongoc_read_prefs_t *prefs;
   bson_t filter;
   bson_t opts;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t files_index;
   bson_t chunks_index;
   bool r;

   /* Check to see if there already exists a document in the files collection */
   bson_init (&filter);
   bson_append_int32 (&filter, "_id", 3, 1);
   bson_init (&opts);
   bson_append_bool (&opts, "singleBatch", 11, true);
   bson_append_int32 (&opts, "limit", 5, 1);
   prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   cursor = mongoc_collection_find_with_opts (bucket->files, &filter, &opts, prefs);
   bson_destroy (&filter);
   bson_destroy (&opts);

   r = mongoc_cursor_next (cursor, &doc);

   mongoc_read_prefs_destroy (prefs);
   if (r) {
      /* Files exist */
      mongoc_cursor_destroy (cursor);
      return true;
   } else if (mongoc_cursor_error (cursor, error)) {
      mongoc_cursor_destroy (cursor);
      return false;
   }
   mongoc_cursor_destroy (cursor);

   /* Create files index */
   bson_init (&files_index);
   BSON_APPEND_INT32 (&files_index, "filename", 1);
   BSON_APPEND_INT32 (&files_index, "uploadDate", 1);

   r = _mongoc_collection_create_index_if_not_exists (bucket->files, &files_index, NULL, error);
   bson_destroy (&files_index);
   if (!r) {
      return false;
   }

   /* Create unique chunks index */
   bson_init (&opts);
   BSON_APPEND_BOOL (&opts, "unique", true);
   BSON_APPEND_UTF8 (&opts, "name", "files_id_1_n_1");

   bson_init (&chunks_index);
   BSON_APPEND_INT32 (&chunks_index, "files_id", 1);
   BSON_APPEND_INT32 (&chunks_index, "n", 1);

   r = _mongoc_collection_create_index_if_not_exists (bucket->chunks, &chunks_index, &opts, error);
   bson_destroy (&opts);
   bson_destroy (&chunks_index);

   if (!r) {
      return false;
   }

   return true;
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_bucket_write_chunk --
 *
 *       Writes a chunk from the buffer into the chunks collection.
 *
 * Return:
 *       Returns true if the chunk was successfully written. Otherwise,
 *       returns false and sets an error on the bucket file.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_gridfs_bucket_write_chunk (mongoc_gridfs_bucket_file_t *file)
{
   bson_t chunk;
   bool r;

   BSON_ASSERT (file);

   bson_init (&chunk);

   BSON_APPEND_INT32 (&chunk, "n", file->curr_chunk);
   BSON_APPEND_VALUE (&chunk, "files_id", file->file_id);
   BSON_APPEND_BINARY (&chunk, "data", BSON_SUBTYPE_BINARY, file->buffer, (uint32_t) file->in_buffer);


   r = mongoc_collection_insert_one (file->bucket->chunks, &chunk, NULL /* opts */, NULL /* reply */, &file->err);
   bson_destroy (&chunk);
   if (!r) {
      return false;
   }

   file->curr_chunk++;
   file->in_buffer = 0;
   return true;
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_bucket_init_cursor --
 *
 *       Initializes the cursor at file->cursor for the given file.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_gridfs_bucket_init_cursor (mongoc_gridfs_bucket_file_t *file)
{
   bson_t filter;
   bson_t opts;
   bson_t sort;

   BSON_ASSERT (file);

   bson_init (&filter);
   bson_init (&opts);
   bson_init (&sort);

   BSON_APPEND_VALUE (&filter, "files_id", file->file_id);
   BSON_APPEND_INT32 (&sort, "n", 1);
   BSON_APPEND_DOCUMENT (&opts, "sort", &sort);

   file->cursor = mongoc_collection_find_with_opts (file->bucket->chunks, &filter, &opts, NULL);

   bson_destroy (&filter);
   bson_destroy (&opts);
   bson_destroy (&sort);
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_bucket_read_chunk --
 *
 *       Reads a chunk from the server and places it into the file's buffer
 *
 * Return:
 *       True if the buffer has been filled with any available data.
 *       Otherwise, false and sets the error on the bucket file.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_gridfs_bucket_read_chunk (mongoc_gridfs_bucket_file_t *file)
{
   const bson_t *next;
   bool r;
   bson_iter_t iter;
   int32_t n;
   const uint8_t *data;
   uint32_t data_len;
   int64_t total_chunks;
   int64_t expected_size;

   BSON_ASSERT (file);

   if (file->length == 0) {
      /* This file has zero length */
      file->in_buffer = 0;
      file->finished = true;
      return true;
   }

   /* Calculate the total number of chunks for this file */
   total_chunks = (file->length / file->chunk_size);
   if (file->length % file->chunk_size != 0) {
      total_chunks++;
   }

   if (file->curr_chunk == total_chunks) {
      /* All chunks have been read! */
      file->in_buffer = 0;
      file->finished = true;
      return true;
   }

   if (file->cursor == NULL) {
      _mongoc_gridfs_bucket_init_cursor (file);
   }

   r = mongoc_cursor_next (file->cursor, &next);

   if (mongoc_cursor_error (file->cursor, &file->err)) {
      return false;
   }

   if (!r) {
      bson_set_error (
         &file->err, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_CHUNK_MISSING, "Missing chunk %d.", file->curr_chunk);
      return false;
   }

   r = bson_iter_init_find (&iter, next, "n");
   if (!r) {
      bson_set_error (&file->err,
                      MONGOC_ERROR_GRIDFS,
                      MONGOC_ERROR_GRIDFS_CORRUPT,
                      "Chunk %d missing a required field 'n'.",
                      file->curr_chunk);
      return false;
   }

   n = bson_iter_int32 (&iter);

   if (n != file->curr_chunk) {
      bson_set_error (
         &file->err, MONGOC_ERROR_GRIDFS, MONGOC_ERROR_GRIDFS_CHUNK_MISSING, "Missing chunk %d.", file->curr_chunk);
      return false;
   }

   r = bson_iter_init_find (&iter, next, "data");
   if (!r) {
      bson_set_error (&file->err,
                      MONGOC_ERROR_GRIDFS,
                      MONGOC_ERROR_GRIDFS_CORRUPT,
                      "Chunk %d missing a required field 'data'.",
                      file->curr_chunk);
      return false;
   }

   bson_iter_binary (&iter, NULL, &data_len, &data);

   /* Assert that the data is the correct length */
   if (file->curr_chunk != total_chunks - 1) {
      expected_size = file->chunk_size;
   } else {
      expected_size = file->length - ((total_chunks - 1) * file->chunk_size);
   }

   if (data_len != expected_size) {
      bson_set_error (&file->err,
                      MONGOC_ERROR_GRIDFS,
                      MONGOC_ERROR_GRIDFS_CORRUPT,
                      "Chunk %d expected to have size %" PRId64 " but is size %d.",
                      file->curr_chunk,
                      expected_size,
                      data_len);
      return false;
   }

   memcpy (file->buffer, data, data_len);
   file->in_buffer = data_len;
   file->bytes_read = 0u;
   file->curr_chunk++;

   return true;
}

ssize_t
_mongoc_gridfs_bucket_file_writev (mongoc_gridfs_bucket_file_t *file, const mongoc_iovec_t *iov, size_t iovcnt)
{
   BSON_ASSERT (file);
   BSON_ASSERT (iov);
   BSON_ASSERT (iovcnt);

   size_t total = 0;

   if (file->err.code) {
      return -1;
   }

   if (file->saved) {
      bson_set_error (&file->err,
                      MONGOC_ERROR_GRIDFS,
                      MONGOC_ERROR_GRIDFS_PROTOCOL_ERROR,
                      "Cannot write after saving/aborting on a GridFS file.");
      return -1;
   }

   if (!file->bucket->indexed) {
      if (!_mongoc_gridfs_bucket_create_indexes (file->bucket, &file->err)) {
         /* Error is set on file. */
         return -1;
      } else {
         file->bucket->indexed = true;
      }
   }

   BSON_ASSERT (bson_in_range_signed (size_t, file->chunk_size));
   const size_t chunk_size = (size_t) file->chunk_size;

   for (size_t i = 0u; i < iovcnt; i++) {
      size_t written_this_iov = 0u;

      while (written_this_iov < iov[i].iov_len) {
         const size_t bytes_available = iov[i].iov_len - written_this_iov;
         const size_t space_available = chunk_size - file->in_buffer;
         const size_t to_write = _mongoc_min (bytes_available, space_available);

         memcpy (file->buffer + file->in_buffer, ((char *) iov[i].iov_base) + written_this_iov, to_write);

         file->in_buffer += to_write;
         written_this_iov += to_write;
         total += to_write;

         if (file->in_buffer == chunk_size) {
            /* Buffer is filled, write the chunk */
            _mongoc_gridfs_bucket_write_chunk (file);
         }
      }
   }

   BSON_ASSERT (bson_in_range_unsigned (ssize_t, total));
   return (ssize_t) total;
}

ssize_t
_mongoc_gridfs_bucket_file_readv (mongoc_gridfs_bucket_file_t *file, mongoc_iovec_t *iov, size_t iovcnt)
{
   BSON_ASSERT (file);
   BSON_ASSERT (iov);
   BSON_ASSERT (iovcnt);

   if (file->err.code) {
      return -1;
   }

   if (file->finished) {
      return 0;
   }

   size_t total = 0u;

   for (size_t i = 0u; i < iovcnt; i++) {
      size_t read_this_iov = 0u;

      while (read_this_iov < iov[i].iov_len) {
         const size_t bytes_available = file->in_buffer - file->bytes_read;
         const size_t space_available = iov[i].iov_len - read_this_iov;
         const size_t to_read = _mongoc_min (bytes_available, space_available);

         memcpy (((char *) iov[i].iov_base) + read_this_iov, file->buffer + file->bytes_read, to_read);

         file->bytes_read += to_read;
         read_this_iov += to_read;
         total += to_read;

         if (file->bytes_read == file->in_buffer) {
            /* Everything in the current chunk has been read, so read a new
             * chunk */
            if (!_mongoc_gridfs_bucket_read_chunk (file)) {
               /* an error occured while reading the chunk */
               return -1;
            }
            if (file->finished) {
               /* There's nothing left to read */
               BSON_ASSERT (bson_in_range_unsigned (ssize_t, total));
               RETURN ((ssize_t) total);
            }
         }
      }
   }

   BSON_ASSERT (bson_in_range_unsigned (ssize_t, total));
   RETURN ((ssize_t) total);
}


/*--------------------------------------------------------------------------
 *
 * _mongoc_gridfs_bucket_file_save --
 *
 *       Saves the file to the files collection in gridFS. This locks the
 *       file into GridFS, and no more chunks are allowed to be written.
 *
 * Return:
 *       True if saved or no-op. False otherwise, and sets the file error.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_gridfs_bucket_file_save (mongoc_gridfs_bucket_file_t *file)
{
   bson_t new_doc;
   int64_t length;
   bool r;

   BSON_ASSERT (file);

   if (file->saved) {
      /* Already saved, or aborted. */
      return true;
   }

   if (file->err.code) {
      return false;
   }

   length = ((int64_t) file->curr_chunk) * file->chunk_size;

   if (file->in_buffer != 0) {
      length += file->in_buffer;
      _mongoc_gridfs_bucket_write_chunk (file);
   }

   file->length = length;

   bson_init (&new_doc);
   BSON_APPEND_VALUE (&new_doc, "_id", file->file_id);
   BSON_APPEND_INT64 (&new_doc, "length", file->length);
   BSON_APPEND_INT32 (&new_doc, "chunkSize", file->chunk_size);
   BSON_APPEND_DATE_TIME (&new_doc, "uploadDate", _mongoc_get_real_time_ms ());
   BSON_APPEND_UTF8 (&new_doc, "filename", file->filename);
   if (file->metadata) {
      BSON_APPEND_DOCUMENT (&new_doc, "metadata", file->metadata);
   }

   r = mongoc_collection_insert_one (file->bucket->files, &new_doc, NULL, NULL, &file->err);
   bson_destroy (&new_doc);
   file->saved = r;
   return (file->err.code) ? false : true;
}

void
_mongoc_gridfs_bucket_file_destroy (mongoc_gridfs_bucket_file_t *file)
{
   if (file) {
      bson_value_destroy (file->file_id);
      bson_free (file->file_id);
      bson_destroy (file->metadata);
      mongoc_cursor_destroy (file->cursor);
      bson_free (file->buffer);
      bson_free (file->filename);
      bson_free (file);
   }
}
