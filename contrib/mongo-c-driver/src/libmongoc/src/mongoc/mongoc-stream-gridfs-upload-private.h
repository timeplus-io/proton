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

#ifndef MONGOC_STREAM_GRIDFS_UPLOAD_PRIVATE_H
#define MONGOC_STREAM_GRIDFS_UPLOAD_PRIVATE_H

#include "mongoc-gridfs-bucket-file-private.h"
#include "mongoc-stream.h"

typedef struct {
   mongoc_stream_t stream;
   mongoc_gridfs_bucket_file_t *file;
} mongoc_gridfs_upload_stream_t;

mongoc_stream_t *
_mongoc_upload_stream_gridfs_new (mongoc_gridfs_bucket_file_t *file);

#endif /* MONGOC_STREAM_GRIDFS_UPLOAD_PRIVATE_H */
