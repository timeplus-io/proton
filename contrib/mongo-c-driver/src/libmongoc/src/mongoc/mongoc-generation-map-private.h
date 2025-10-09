/*
 * Copyright 2021-present MongoDB, Inc.
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

#include "bson/bson.h"

#ifndef MONGOC_GENERATION_MAP_PRIVATE_H
#define MONGOC_GENERATION_MAP_PRIVATE_H

/* mongoc_generation_map_t maps a BSON ObjectID to an unsigned integer.
 * It is used to track connection generations. */
typedef struct _mongoc_generation_map mongoc_generation_map_t;

mongoc_generation_map_t *
mongoc_generation_map_new (void);

mongoc_generation_map_t *
mongoc_generation_map_copy (const mongoc_generation_map_t *gm);

uint32_t
mongoc_generation_map_get (const mongoc_generation_map_t *gm, const bson_oid_t *key);

void
mongoc_generation_map_increment (mongoc_generation_map_t *gm, const bson_oid_t *key);

void
mongoc_generation_map_destroy (mongoc_generation_map_t *gm);

#endif /* MONGOC_GENERATION_MAP_PRIVATE_H */
