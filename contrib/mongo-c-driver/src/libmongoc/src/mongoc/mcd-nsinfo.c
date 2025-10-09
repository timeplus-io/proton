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

#include <mcd-nsinfo.h>

#include <mongoc/mongoc-buffer-private.h>
#include <mongoc/mongoc-error.h>
#include <mongoc/uthash.h>

typedef struct {
   char *ns; // Hash key.
   int32_t index;
   UT_hash_handle hh;
} ns_to_index_t;

struct _mcd_nsinfo_t {
   ns_to_index_t *n2i;
   int32_t count;
   mongoc_buffer_t payload;
};

mcd_nsinfo_t *
mcd_nsinfo_new (void)
{
   mcd_nsinfo_t *self = bson_malloc0 (sizeof (*self));
   _mongoc_buffer_init (&self->payload, NULL, 0, NULL, NULL);
   return self;
}

void
mcd_nsinfo_destroy (mcd_nsinfo_t *self)
{
   if (!self) {
      return;
   }
   // Delete hash table.
   ns_to_index_t *entry, *tmp;
   HASH_ITER (hh, self->n2i, entry, tmp)
   {
      HASH_DEL (self->n2i, entry);
      bson_free (entry->ns);
      bson_free (entry);
   }
   _mongoc_buffer_destroy (&self->payload);
   bson_free (self);
}

int32_t
mcd_nsinfo_append (mcd_nsinfo_t *self, const char *ns, bson_error_t *error)
{
   BSON_ASSERT_PARAM (self);
   BSON_ASSERT_PARAM (ns);
   BSON_ASSERT (error || true);

   const int32_t ns_index = self->count;
   if (self->count == INT32_MAX) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Only %" PRId32 " distinct collections may be used",
                      INT32_MAX);
      return -1;
   }
   self->count++;

   // Add to hash table.
   ns_to_index_t *entry = bson_malloc (sizeof (*entry));
   *entry = (ns_to_index_t){.index = ns_index, .ns = bson_strdup (ns), .hh = {0}};
   HASH_ADD_KEYPTR (hh, self->n2i, entry->ns, strlen (entry->ns), entry);

   // Append to buffer.
   bson_t mcd_nsinfo_bson = BSON_INITIALIZER;
   BSON_ASSERT (bson_append_utf8 (&mcd_nsinfo_bson, "ns", 2, ns, -1));
   BSON_ASSERT (_mongoc_buffer_append (&self->payload, bson_get_data (&mcd_nsinfo_bson), mcd_nsinfo_bson.len));
   bson_destroy (&mcd_nsinfo_bson);
   return ns_index;
}

int32_t
mcd_nsinfo_find (const mcd_nsinfo_t *self, const char *ns)
{
   BSON_ASSERT_PARAM (self);
   BSON_ASSERT_PARAM (ns);

   ns_to_index_t *found;
   HASH_FIND_STR (self->n2i, ns, found);
   if (found == NULL) {
      return -1;
   }

   return found->index;
}

uint32_t
mcd_nsinfo_get_bson_size (const char *ns)
{
   BSON_ASSERT_PARAM (ns);

   // Calculate overhead of the BSON document { "ns": "<ns>" }. See BSON specification.
   bson_t as_bson = BSON_INITIALIZER;
   BSON_ASSERT (bson_append_utf8 (&as_bson, "ns", 2, ns, -1));
   uint32_t size = as_bson.len;
   bson_destroy (&as_bson);
   return size;
}

const mongoc_buffer_t *
mcd_nsinfo_as_document_sequence (const mcd_nsinfo_t *self)
{
   BSON_ASSERT_PARAM (self);

   return &self->payload;
}
