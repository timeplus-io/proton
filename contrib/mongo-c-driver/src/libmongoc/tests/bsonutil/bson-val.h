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

#ifndef BSONUTIL_BSON_VAL_H
#define BSONUTIL_BSON_VAL_H

#include "bson/bson.h"

/*
 * bson_val_t is a convenience wrapper around a bson_value_t.
 */
typedef struct _bson_val_t bson_val_t;
typedef enum { BSON_VAL_FLEXIBLE_NUMERICS = 1 << 0, BSON_VAL_UNORDERED = 1 << 1 } bson_val_comparison_flags_t;

bson_val_t *
bson_val_from_value (const bson_value_t *value);

bson_val_t *
bson_val_from_json (const char *single_quoted_json);

bson_val_t *
bson_val_from_iter (const bson_iter_t *iter);

bson_val_t *
bson_val_from_bson (const bson_t *bson);

bson_val_t *
bson_val_from_doc (const bson_t *bson);

bson_val_t *
bson_val_from_array (const bson_t *bson);

bson_val_t *
bson_val_from_int64 (int64_t val);

bson_val_t *
bson_val_from_bytes (const uint8_t *bytes, uint32_t len);

bson_val_t *
bson_val_copy (const bson_val_t *val);

const bson_t *
bson_val_to_document (const bson_val_t *val);

const bson_t *
bson_val_to_array (const bson_val_t *val);

const bson_t *
bson_val_to_bson (const bson_val_t *val);

const uint8_t *
bson_val_to_binary (const bson_val_t *val, uint32_t *len);

const bson_value_t *
bson_val_to_value (const bson_val_t *val);

const char *
bson_val_to_utf8 (const bson_val_t *val);

bool
bson_val_is_numeric (const bson_val_t *val);

int64_t
bson_val_convert_int64 (const bson_val_t *val);

bool
bson_val_eq (const bson_val_t *a, const bson_val_t *b, bson_val_comparison_flags_t flags);

bson_type_t
bson_val_type (const bson_val_t *val);

const char *
bson_val_to_json (const bson_val_t *val);

void
bson_val_destroy (bson_val_t *val);

#endif /* BSONUTIL_BSON_VAL_H */
