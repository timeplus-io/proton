/*
 * Copyright 2015 MongoDB, Inc.
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

#ifndef MONGOC_SERVER_STREAM_H
#define MONGOC_SERVER_STREAM_H

#include "mongoc-config.h"

#include <bson/bson.h>

#include "mongoc-topology-description-private.h"
#include "mongoc-server-description-private.h"
#include "mongoc-stream.h"

BSON_BEGIN_DECLS

typedef struct _mongoc_server_stream_t {
   mongoc_topology_description_type_t topology_type;
   mongoc_server_description_t *sd; // owned
   bson_t cluster_time;             // owned
   mongoc_stream_t *stream;         // borrowed
   // If the stream was created in a way that may have overwritten the user's
   // readPreference, we need to know if server selection forced that change.
   bool must_use_primary;
   // True if this server stream was acquired during a retry attempt triggered
   // by a network error establishing an initial connection. Used to avoid
   // further retry attempts.
   bool retry_attempted;
} mongoc_server_stream_t;


mongoc_server_stream_t *
mongoc_server_stream_new (const mongoc_topology_description_t *td,
                          mongoc_server_description_t *sd,
                          mongoc_stream_t *stream);

int32_t
mongoc_server_stream_max_bson_obj_size (mongoc_server_stream_t *server_stream);

int32_t
mongoc_server_stream_max_msg_size (mongoc_server_stream_t *server_stream);

int32_t
mongoc_server_stream_max_write_batch_size (mongoc_server_stream_t *server_stream);

void
mongoc_server_stream_cleanup (mongoc_server_stream_t *server_stream);

BSON_END_DECLS


#endif /* MONGOC_SERVER_STREAM_H */
