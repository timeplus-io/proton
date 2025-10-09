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

#ifndef MONGOC_GRIDFS_BUCKET_PRIVATE_H
#define MONGOC_GRIDFS_BUCKET_PRIVATE_H

#include "mongoc-collection.h"

BSON_BEGIN_DECLS

struct _mongoc_gridfs_bucket_t {
   mongoc_collection_t *chunks;
   mongoc_collection_t *files;
   int32_t chunk_size;
   char *bucket_name;
   bool indexed;
};

BSON_END_DECLS

#endif /* MONGOC_GRIDFS_BUCKET_PRIVATE_H */
