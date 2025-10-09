/*
 * Copyright 2013 MongoDB, Inc.
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


#include "mongoc-array-private.h"


void
_mongoc_array_init (mongoc_array_t *array, size_t element_size)
{
   BSON_ASSERT_PARAM (array);
   BSON_ASSERT (element_size > 0);

   array->len = 0;
   array->element_alignment = 0;
   array->element_size = element_size;
   array->allocated = 128;
   array->data = (void *) bson_malloc0 (array->allocated);
}


void
_mongoc_array_aligned_init (mongoc_array_t *array, size_t element_alignment, size_t element_size)
{
   BSON_ASSERT_PARAM (array);
   BSON_ASSERT (element_alignment > 0);
   BSON_ASSERT (element_size > 0);

   array->len = 0;
   array->element_alignment = element_alignment;
   array->element_size = element_size;
   array->allocated = 128;
   array->data = bson_aligned_alloc0 (element_alignment, array->allocated);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_array_copy --
 *
 *       Destroy dst and copy src into it. Both arrays must be initialized.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
_mongoc_array_copy (mongoc_array_t *dst, const mongoc_array_t *src)
{
   _mongoc_array_destroy (dst);

   dst->len = src->len;
   dst->element_alignment = src->element_alignment;
   dst->element_size = src->element_size;
   dst->allocated = src->allocated;
   if (dst->element_alignment == 0) {
      dst->data = bson_malloc (dst->allocated);
   } else {
      dst->data = bson_aligned_alloc (dst->element_alignment, dst->allocated);
   }
   memcpy (dst->data, src->data, dst->allocated);
}


void
_mongoc_array_destroy (mongoc_array_t *array)
{
   if (array && array->data) {
      bson_free (array->data);
   }
}


void
_mongoc_array_append_vals (mongoc_array_t *array, const void *data, uint32_t n_elements)
{
   size_t len;
   size_t off;
   size_t next_size;

   BSON_ASSERT (array);
   BSON_ASSERT (data);

   off = array->element_size * array->len;
   len = (size_t) n_elements * array->element_size;
   if ((off + len) > array->allocated) {
      next_size = bson_next_power_of_two (off + len);

      if (array->element_alignment == 0) {
         array->data = bson_realloc (array->data, next_size);
         array->allocated = next_size;
      } else {
         void *const old_data = array->data;

         array->data = bson_aligned_alloc (array->element_alignment, next_size);
         memmove (array->data, old_data, array->allocated);
         array->allocated = next_size;

         bson_free (old_data);
      }
   }

   memcpy ((uint8_t *) array->data + off, data, len);

   array->len += n_elements;
}
