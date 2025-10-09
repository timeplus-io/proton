/*
 * Copyright 2024 MongoDB, Inc.
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

#ifndef MONGOC_DEPRIORITIZED_SERVERS_PRIVATE_H
#define MONGOC_DEPRIORITIZED_SERVERS_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-server-description.h"

#include <stdbool.h>

BSON_BEGIN_DECLS

typedef struct _mongoc_deprioritized_servers_t mongoc_deprioritized_servers_t;

mongoc_deprioritized_servers_t *
mongoc_deprioritized_servers_new (void);

void
mongoc_deprioritized_servers_destroy (mongoc_deprioritized_servers_t *ds);

void
mongoc_deprioritized_servers_add (mongoc_deprioritized_servers_t *ds, const mongoc_server_description_t *sd);

bool
mongoc_deprioritized_servers_contains (const mongoc_deprioritized_servers_t *ds, const mongoc_server_description_t *sd);

BSON_END_DECLS

#endif // MONGOC_DEPRIORITIZED_SERVERS_PRIVATE_H
