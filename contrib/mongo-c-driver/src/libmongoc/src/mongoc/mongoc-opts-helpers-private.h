/*
 * Copyright 2019-present MongoDB, Inc.
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

#include <bson/bson.h>
#include "mongoc-client-session-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-write-command-private.h"

#ifndef LIBMONGOC_MONGOC_OPTS_HELPERS_H
#define LIBMONGOC_MONGOC_OPTS_HELPERS_H

#define _mongoc_convert_session_id _mongoc_client_session_from_iter

typedef struct _mongoc_timestamp_t {
   uint32_t timestamp;
   uint32_t increment;
} mongoc_timestamp_t;

bool
_mongoc_timestamp_empty (mongoc_timestamp_t *timestamp);

void
_mongoc_timestamp_set (mongoc_timestamp_t *dst, mongoc_timestamp_t *src);

void
_mongoc_timestamp_set_from_bson (mongoc_timestamp_t *timestamp, bson_iter_t *iter);

void
_mongoc_timestamp_append (mongoc_timestamp_t *timestamp, bson_t *bson, char *key);

void
_mongoc_timestamp_clear (mongoc_timestamp_t *timestamp);

bool
_mongoc_convert_document (mongoc_client_t *client, const bson_iter_t *iter, bson_t *doc, bson_error_t *error);

bool
_mongoc_convert_array (mongoc_client_t *client, const bson_iter_t *iter, bson_t *doc, bson_error_t *error);

bool
_mongoc_convert_int64_positive (mongoc_client_t *client, const bson_iter_t *iter, int64_t *num, bson_error_t *error);

bool
_mongoc_convert_int32_t (mongoc_client_t *client, const bson_iter_t *iter, int32_t *num, bson_error_t *error);

bool
_mongoc_convert_int32_positive (mongoc_client_t *client, const bson_iter_t *iter, int32_t *num, bson_error_t *error);

bool
_mongoc_convert_bool (mongoc_client_t *client, const bson_iter_t *iter, bool *flag, bson_error_t *error);

bool
_mongoc_convert_bson_value_t (mongoc_client_t *client,
                              const bson_iter_t *iter,
                              bson_value_t *value,
                              bson_error_t *error);

bool
_mongoc_convert_timestamp (mongoc_client_t *client,
                           const bson_iter_t *iter,
                           mongoc_timestamp_t *timestamp,
                           bson_error_t *error);

bool
_mongoc_convert_utf8 (mongoc_client_t *client, const bson_iter_t *iter, const char **comment, bson_error_t *error);

bool
_mongoc_convert_validate_flags (mongoc_client_t *client,
                                const bson_iter_t *iter,
                                bson_validate_flags_t *flags,
                                bson_error_t *error);

bool
_mongoc_convert_mongoc_write_bypass_document_validation_t (mongoc_client_t *client,
                                                           const bson_iter_t *iter,
                                                           bool *bdv,
                                                           bson_error_t *error);

bool
_mongoc_convert_write_concern (mongoc_client_t *client,
                               const bson_iter_t *iter,
                               mongoc_write_concern_t **wc,
                               bson_error_t *error);

bool
_mongoc_convert_server_id (mongoc_client_t *client, const bson_iter_t *iter, uint32_t *server_id, bson_error_t *error);

bool
_mongoc_convert_read_concern (mongoc_client_t *client,
                              const bson_iter_t *iter,
                              mongoc_read_concern_t **rc,
                              bson_error_t *error);

bool
_mongoc_convert_hint (mongoc_client_t *client, const bson_iter_t *iter, bson_value_t *value, bson_error_t *error);

#endif
