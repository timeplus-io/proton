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
#include "mongoc-prelude.h"

#ifndef MONGOC_GRIDFS_BUCKET_FILE_PRIVATE_H
#define MONGOC_GRIDFS_BUCKET_FILE_PRIVATE_H

#include "bson/bson.h"
#include "mongoc-collection.h"
#include "mongoc-stream.h"
#include "mongoc-gridfs-bucket.h"

BSON_BEGIN_DECLS

typedef struct {
   /* corresponding bucket */
   mongoc_gridfs_bucket_t *bucket;

   /* file data */
   char *filename;
   bson_value_t *file_id;
   bson_t *metadata;
   int32_t chunk_size;
   int64_t length;

   /* fields for reading and writing */
   uint8_t *buffer;
   size_t in_buffer;
   int32_t curr_chunk;

   /* for writing */
   bool saved;

   /* for reading */
   mongoc_cursor_t *cursor;
   size_t bytes_read;
   bool finished;

   /* Error */
   bson_error_t err;
} mongoc_gridfs_bucket_file_t;

ssize_t
_mongoc_gridfs_bucket_file_writev (mongoc_gridfs_bucket_file_t *file, const mongoc_iovec_t *iov, size_t iovcnt);

ssize_t
_mongoc_gridfs_bucket_file_readv (mongoc_gridfs_bucket_file_t *file, mongoc_iovec_t *iov, size_t iovcnt);

bool
_mongoc_gridfs_bucket_file_save (mongoc_gridfs_bucket_file_t *file);

void
_mongoc_gridfs_bucket_file_destroy (mongoc_gridfs_bucket_file_t *file);

BSON_END_DECLS

#endif /* MONGOC_GRIDFS_BUCKET_FILE_PRIVATE_H */
