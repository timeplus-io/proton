/*
 * Copyright 2020 MongoDB, Inc.
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

#include "mongoc-timeout-private.h"
#include "mongoc.h"

struct _mongoc_timeout_t {
   bool is_set;
   int64_t timeout_ms;
};

int64_t
mongoc_timeout_get_timeout_ms (const mongoc_timeout_t *timeout)
{
   BSON_ASSERT (timeout);
   BSON_ASSERT (timeout->is_set);

   return timeout->timeout_ms;
}

bool
_mongoc_timeout_set_timeout_ms (mongoc_timeout_t *timeout, int64_t timeout_ms)
{
   BSON_ASSERT (timeout);

   if (timeout_ms < 0) {
      MONGOC_ERROR ("timeout must not be negative");
      return false;
   }

   timeout->timeout_ms = timeout_ms;
   timeout->is_set = true;
   return true;
}

bool
mongoc_timeout_set_timeout_ms (mongoc_timeout_t *timeout, int64_t timeout_ms)
{
   return _mongoc_timeout_set_timeout_ms (timeout, timeout_ms);
}

mongoc_timeout_t *
mongoc_timeout_new (void)
{
   return (mongoc_timeout_t *) bson_malloc0 (sizeof (mongoc_timeout_t));
}

mongoc_timeout_t *
mongoc_timeout_new_timeout_int64 (int64_t timeout_ms)
{
   mongoc_timeout_t *timeout = mongoc_timeout_new ();

   if (_mongoc_timeout_set_timeout_ms (timeout, timeout_ms))
      return timeout;

   mongoc_timeout_destroy (timeout);
   return NULL;
}

mongoc_timeout_t *
mongoc_timeout_copy (const mongoc_timeout_t *timeout)
{
   mongoc_timeout_t *copy = NULL;

   BSON_ASSERT (timeout);

   copy = mongoc_timeout_new ();
   copy->timeout_ms = timeout->timeout_ms;
   copy->is_set = timeout->is_set;

   return copy;
}

void
mongoc_timeout_destroy (mongoc_timeout_t *timeout)
{
   bson_free (timeout);
}

bool
mongoc_timeout_is_set (const mongoc_timeout_t *timeout)
{
   return timeout && timeout->is_set;
}
