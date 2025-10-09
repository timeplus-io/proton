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

#include "mongoc-prelude.h"

#ifndef MONGOC_READ_PREFS_PRIVATE_H
#define MONGOC_READ_PREFS_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-cluster-private.h"
#include "mongoc-read-prefs.h"


BSON_BEGIN_DECLS

struct _mongoc_read_prefs_t {
   mongoc_read_mode_t mode;
   bson_t tags;
   int64_t max_staleness_seconds;
   bson_t hedge;
};


typedef struct _mongoc_assemble_query_result_t {
   bson_t *assembled_query;
   bool query_owned;
   int32_t flags;
} mongoc_assemble_query_result_t;


#define ASSEMBLE_QUERY_RESULT_INIT   \
   {                                 \
      NULL, false, MONGOC_QUERY_NONE \
   }

const char *
_mongoc_read_mode_as_str (mongoc_read_mode_t mode);

void
assemble_query (const mongoc_read_prefs_t *read_prefs,
                const mongoc_server_stream_t *server_stream,
                const bson_t *query_bson,
                int32_t initial_flags,
                mongoc_assemble_query_result_t *result);

void
assemble_query_result_cleanup (mongoc_assemble_query_result_t *result);

bool
_mongoc_read_prefs_validate (const mongoc_read_prefs_t *read_prefs, bson_error_t *error);

#define IS_PREF_PRIMARY(_pref) (!(_pref) || ((_pref)->mode == MONGOC_READ_PRIMARY))

BSON_END_DECLS


#endif /* MONGOC_READ_PREFS_PRIVATE_H */
