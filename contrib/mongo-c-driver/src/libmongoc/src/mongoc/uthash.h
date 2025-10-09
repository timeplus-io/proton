/*
 * Copyright 2024-present MongoDB, Inc.
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

#ifndef MONGOC_UTHASH_H
#define MONGOC_UTHASH_H

#include <mongoc-prelude.h>
#include <bson/bson.h>

#define uthash_malloc(sz) bson_malloc (sz)
#define uthash_free(ptr, sz) bson_free (ptr)

#include <uthash-2.3.0/uthash.h>

#endif // MONGOC_UTHASH_H
