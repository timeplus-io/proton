/*
 * Copyright 2019 MongoDB, Inc.
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

#ifndef MONGOC_AGGREGATE_PRIVATE_H
#define MONGOC_AGGREGATE_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-client.h"
#include "mongoc-flags.h"
#include "mongoc-read-concern.h"
#include "mongoc-read-prefs.h"
#include "mongoc-write-concern.h"


BSON_BEGIN_DECLS


mongoc_cursor_t *
_mongoc_aggregate (mongoc_client_t *client,
                   const char *ns,
                   mongoc_query_flags_t flags,
                   const bson_t *pipeline,
                   const bson_t *opts,
                   const mongoc_read_prefs_t *user_rp,
                   const mongoc_read_prefs_t *default_rp,
                   const mongoc_read_concern_t *default_rc,
                   const mongoc_write_concern_t *default_wc);

bool
_has_write_key (bson_iter_t *iter);

BSON_END_DECLS


#endif /* MONGOC_AGGREGATE_PRIVATE_H */
