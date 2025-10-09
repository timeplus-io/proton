/*
 * Copyright 2020-present MongoDB, Inc.
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

#ifndef UNIFIED_UTIL_H
#define UNIFIED_UTIL_H

#include "mongoc/mongoc.h"

bson_t *
bson_copy_and_sort (const bson_t *in);

bson_type_t
bson_type_from_string (const char *in);

const char *
bson_type_to_string (bson_type_t btype);

/* Returns true if this is an event type (part of observeEvents or
 * expectedEvents) that is unsupported and not emitted by the C driver. */
bool
is_unsupported_event_type (const char *event_type);

int64_t
usecs_since_epoch (void);

#endif /* UNIFIED_UTIL_H */
