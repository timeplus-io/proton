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

#include "mongoc-generation-map-private.h"

#include "utlist.h"

typedef struct _gm_node_t {
   bson_oid_t key;
   uint32_t val;
   struct _gm_node_t *next;
} gm_node_t;

static gm_node_t *
gm_node_new (void)
{
   return bson_malloc0 (sizeof (gm_node_t));
}

static void
gm_node_destroy (gm_node_t *node)
{
   bson_free (node);
}

static gm_node_t *
gm_node_copy (const gm_node_t *node)
{
   gm_node_t *node_copy = gm_node_new ();

   BSON_ASSERT (node_copy);
   BSON_ASSERT (node);

   bson_oid_copy (&node->key, &node_copy->key);
   node_copy->val = node->val;
   return node_copy;
}

struct _mongoc_generation_map {
   gm_node_t *list;
};

mongoc_generation_map_t *
mongoc_generation_map_new (void)
{
   mongoc_generation_map_t *gm;

   gm = bson_malloc0 (sizeof (mongoc_generation_map_t));
   return gm;
}

mongoc_generation_map_t *
mongoc_generation_map_copy (const mongoc_generation_map_t *gm)
{
   mongoc_generation_map_t *gm_copy;
   gm_node_t *iter;

   gm_copy = mongoc_generation_map_new ();
   BSON_ASSERT (gm_copy);

   LL_FOREACH (gm->list, iter)
   {
      gm_node_t *node_copy;

      node_copy = gm_node_copy (iter);
      BSON_ASSERT (node_copy);
      LL_PREPEND (gm_copy->list, node_copy);
   }

   return gm_copy;
}

uint32_t
mongoc_generation_map_get (const mongoc_generation_map_t *gm, const bson_oid_t *key)
{
   gm_node_t *iter = NULL;

   BSON_ASSERT (gm);
   BSON_ASSERT (key);

   LL_FOREACH (gm->list, iter)
   {
      if (bson_oid_equal (key, &iter->key)) {
         break;
      }
   }

   if (!iter) {
      return 0;
   }

   return iter->val;
}

void
mongoc_generation_map_increment (mongoc_generation_map_t *gm, const bson_oid_t *key)
{
   gm_node_t *match;
   gm_node_t *iter = NULL;

   BSON_ASSERT (gm);
   BSON_ASSERT (key);

   LL_FOREACH (gm->list, iter)
   {
      if (bson_oid_equal (key, &iter->key)) {
         break;
      }
   }

   if (iter) {
      match = iter;
   } else {
      gm_node_t *new_node = gm_node_new ();
      BSON_ASSERT (new_node);
      bson_oid_copy (key, &new_node->key);
      LL_PREPEND (gm->list, new_node);
      match = new_node;
   }

   BSON_ASSERT (match);
   match->val++;
}

void
mongoc_generation_map_destroy (mongoc_generation_map_t *gm)
{
   gm_node_t *iter = NULL;
   gm_node_t *tmp = NULL;

   if (!gm) {
      return;
   }

   LL_FOREACH_SAFE (gm->list, iter, tmp)
   {
      gm_node_destroy (iter);
   }

   bson_free (gm);
}
