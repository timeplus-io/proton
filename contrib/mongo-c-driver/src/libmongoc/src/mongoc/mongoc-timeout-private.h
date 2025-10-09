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

#include "mongoc-prelude.h"

#ifndef MONGOC_TIMEOUT_PRIVATE_H
#define MONGOC_TIMEOUT_PRIVATE_H

#include <stdint.h>
#include <stdbool.h>

typedef struct _mongoc_timeout_t mongoc_timeout_t;

mongoc_timeout_t *
mongoc_timeout_new (void);

mongoc_timeout_t *
mongoc_timeout_new_timeout_int64 (int64_t timeout_ms);

void
mongoc_timeout_destroy (mongoc_timeout_t *timeout);

mongoc_timeout_t *
mongoc_timeout_copy (const mongoc_timeout_t *timeout);

int64_t
mongoc_timeout_get_timeout_ms (const mongoc_timeout_t *timeout);

bool
mongoc_timeout_set_timeout_ms (mongoc_timeout_t *timeout, int64_t timeout_ms);

bool
mongoc_timeout_is_set (const mongoc_timeout_t *timeout);

#endif /* MONGOC_TIMEOUT_PRIVATE_H */
