/*
 * Copyright 2021 MongoDB, Inc.
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

#include "mongoc-optional.h"

void
mongoc_optional_init (mongoc_optional_t *opt)
{
   opt->is_set = false;
   opt->value = false;
}

bool
mongoc_optional_is_set (const mongoc_optional_t *opt)
{
   BSON_ASSERT (opt);
   return opt->is_set;
}

bool
mongoc_optional_value (const mongoc_optional_t *opt)
{
   BSON_ASSERT (opt);
   return opt->value;
}

void
mongoc_optional_set_value (mongoc_optional_t *opt, bool val)
{
   BSON_ASSERT (opt);
   opt->value = val;
   opt->is_set = true;
}

void
mongoc_optional_copy (const mongoc_optional_t *source, mongoc_optional_t *copy)
{
   copy->value = source->value;
   copy->is_set = source->is_set;
}
