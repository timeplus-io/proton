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

#ifndef UNIFIED_OPERATION_RESULT_H
#define UNIFIED_OPERATION_RESULT_H

#include "test-diagnostics.h"
#include "bsonutil/bson-val.h"
#include "entity-map.h"
#include "mongoc-cursor.h"

typedef struct _result_t result_t;

result_t *
result_new (void);

void
result_destroy (result_t *result);

void
result_from_bulk_write (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_insert_one (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_insert_many (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_delete (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_distinct (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_update_or_replace (result_t *result, const bson_t *reply, const bson_error_t *error);

void
result_from_cursor (result_t *result, mongoc_cursor_t *cursor);

void
result_from_val_and_reply (result_t *result, const bson_val_t *value, const bson_t *reply, const bson_error_t *error);

void
result_from_ok (result_t *result);

const char *
result_to_string (result_t *result);

bson_val_t *
result_get_val (result_t *result);

bson_t *
rewrite_bulk_write_result (const bson_t *bulk_write_result);

bool
result_check (result_t *result, entity_map_t *em, bson_val_t *expect_result, bson_t *expect_error, bson_error_t *error);

#endif /* UNIFIED_OPERATION_RESULT_H */
