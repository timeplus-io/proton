/*
 * Copyright 2014 MongoDB, Inc.
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

#ifndef MONGOC_SET_PRIVATE_H
#define MONGOC_SET_PRIVATE_H

#include <bson/bson.h>

BSON_BEGIN_DECLS

typedef void (*mongoc_set_item_dtor) (void *item, void *ctx);

/* return true to continue iteration, false to stop */
typedef bool (*mongoc_set_for_each_cb_t) (void *item, void *ctx);
typedef bool (*mongoc_set_for_each_const_cb_t) (const void *item, void *ctx);
typedef bool (*mongoc_set_for_each_with_id_cb_t) (uint32_t id, void *item, void *ctx);

typedef struct {
   uint32_t id;
   void *item;
} mongoc_set_item_t;

typedef struct {
   mongoc_set_item_t *items;
   size_t items_len;
   size_t items_allocated;
   mongoc_set_item_dtor dtor;
   void *dtor_ctx;
} mongoc_set_t;

mongoc_set_t *
mongoc_set_new (size_t nitems, mongoc_set_item_dtor dtor, void *dtor_ctx);

void
mongoc_set_add (mongoc_set_t *set, uint32_t id, void *item);

void
mongoc_set_rm (mongoc_set_t *set, uint32_t id);

void *
mongoc_set_get (mongoc_set_t *set, uint32_t id);

static BSON_INLINE const void *
mongoc_set_get_const (const mongoc_set_t *set, uint32_t id)
{
   return mongoc_set_get ((mongoc_set_t *) set, id);
}

void *
mongoc_set_get_item (mongoc_set_t *set, size_t idx);

static BSON_INLINE const void *
mongoc_set_get_item_const (const mongoc_set_t *set, size_t idx)
{
   return mongoc_set_get_item ((mongoc_set_t *) set, idx);
}

void *
mongoc_set_get_item_and_id (mongoc_set_t *set, size_t idx, uint32_t *id /* OUT */);

static BSON_INLINE const void *
mongoc_set_get_item_and_id_const (const mongoc_set_t *set, size_t idx, uint32_t *id)
{
   return mongoc_set_get_item_and_id ((mongoc_set_t *) set, idx, id);
}

void
mongoc_set_destroy (mongoc_set_t *set);

/* loops over the set safe-ish.
 *
 * Caveats:
 *   - you can add items at any iteration
 *   - if you remove elements other than the one you're currently looking at,
 *     you may see it later in the iteration
 */
void
mongoc_set_for_each (mongoc_set_t *set, mongoc_set_for_each_cb_t cb, void *ctx);

static BSON_INLINE void
mongoc_set_for_each_const (const mongoc_set_t *set, mongoc_set_for_each_const_cb_t cb, void *ctx)
{
   mongoc_set_for_each ((mongoc_set_t *) set, (mongoc_set_for_each_cb_t) cb, ctx);
}

void
mongoc_set_for_each_with_id (mongoc_set_t *set, mongoc_set_for_each_with_id_cb_t cb, void *ctx);

/* first item in set for which "cb" returns true */
void *
mongoc_set_find_item (mongoc_set_t *set, mongoc_set_for_each_cb_t cb, void *ctx);

/* id of first item in set for which "cb" returns true, or 0. */
uint32_t
mongoc_set_find_id (const mongoc_set_t *set, mongoc_set_for_each_const_cb_t cb, void *ctx);

BSON_END_DECLS

#endif /* MONGOC_SET_PRIVATE_H */
