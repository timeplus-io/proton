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

#ifndef MONGOC_CLIENT_PRIVATE_H
#define MONGOC_CLIENT_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-apm-private.h"
#include "mongoc-buffer-private.h"
#include "mongoc-client.h"
#include "mongoc-cluster-private.h"
#include "mongoc-config.h"
#include "mongoc-host-list.h"
#include "mongoc-read-prefs.h"
#include "mongoc-rpc-private.h"
#include "mongoc-opcode.h"
#ifdef MONGOC_ENABLE_SSL
#include "mongoc-ssl.h"
#endif

#include "mongoc-stream.h"
#include "mongoc-topology-private.h"
#include "mongoc-write-concern.h"
#include "mongoc-crypt-private.h"

BSON_BEGIN_DECLS

/* version corresponding to server 3.6 release */
#define WIRE_VERSION_3_6 6
/* version corresponding to server 4.0 release */
#define WIRE_VERSION_4_0 7
/* first version to support hint for "update" command */
#define WIRE_VERSION_UPDATE_HINT 8
/* version corresponding to server 4.2 release */
#define WIRE_VERSION_4_2 8
/* version corresponding to client side field level encryption support. */
#define WIRE_VERSION_CSE 8
/* first version to throw server-side errors for unsupported hint in
 * "findAndModify" command */
#define WIRE_VERSION_FIND_AND_MODIFY_HINT_SERVER_SIDE_ERROR 8
/* first version to support hint for "delete" command */
#define WIRE_VERSION_DELETE_HINT 9
/* first version to support hint for "findAndModify" command */
#define WIRE_VERSION_FIND_AND_MODIFY_HINT 9
/* version corresponding to server 4.4 release */
#define WIRE_VERSION_4_4 9
/* version corresponding to retryable writes error label */
#define WIRE_VERSION_RETRYABLE_WRITE_ERROR_LABEL 9
/* first version to support server hedged reads */
#define WIRE_VERSION_HEDGED_READS 9
/* first version to support estimatedDocumentCount with collStats */
#define WIRE_VERSION_4_9 12
/* version corresponding to server 5.0 release */
#define WIRE_VERSION_5_0 13
/* first version to support snapshot reads */
#define WIRE_VERSION_SNAPSHOT_READS 13
/* version corresponding to server 5.1 release */
#define WIRE_VERSION_5_1 14
/* version corresponding to server 6.0 release */
#define WIRE_VERSION_6_0 17
/* version corresponding to server 7.0 release */
#define WIRE_VERSION_7_0 21
/* version corresponding to server 7.1 release */
#define WIRE_VERSION_7_1 22
#define WIRE_VERSION_MONGOS_EXHAUST 22

/* Range of wire protocol versions this driver supports. Bumping
 * WIRE_VERSION_MAX must be accompanied by an update to
 * `_mongoc_wire_version_to_server_version`. */
#define WIRE_VERSION_MIN WIRE_VERSION_3_6 /* a.k.a. minWireVersion */
#define WIRE_VERSION_MAX WIRE_VERSION_7_0 /* a.k.a. maxWireVersion */

struct _mongoc_collection_t;

struct _mongoc_client_t {
   mongoc_uri_t *uri;
   mongoc_cluster_t cluster;
   bool in_exhaust;
   bool is_pooled;

   mongoc_stream_initiator_t initiator;
   void *initiator_data;

#ifdef MONGOC_ENABLE_SSL
   bool use_ssl;
   mongoc_ssl_opt_t ssl_opts;
#endif

   mongoc_topology_t *topology;

   mongoc_read_prefs_t *read_prefs;
   mongoc_read_concern_t *read_concern;
   mongoc_write_concern_t *write_concern;

   mongoc_apm_callbacks_t apm_callbacks;
   void *apm_context;

   int32_t error_api_version;
   bool error_api_set;

   mongoc_server_api_t *api;

   /* mongoc_client_session_t's in use, to look up lsids and clusterTimes */
   mongoc_set_t *client_sessions;
   unsigned int csid_rand_seed;

   uint32_t generation;
};

/* Defines whether _mongoc_client_command_with_opts() is acting as a read
 * command helper for a command like "distinct", or a write command helper for
 * a command like "createRole", or both, like "aggregate" with "$out".
 */
typedef enum {
   MONGOC_CMD_RAW = 0,
   MONGOC_CMD_READ = 1,
   MONGOC_CMD_WRITE = 2,
   MONGOC_CMD_RW = 3,
} mongoc_command_mode_t;

BSON_STATIC_ASSERT2 (mongoc_cmd_rw, MONGOC_CMD_RW == (MONGOC_CMD_READ | MONGOC_CMD_WRITE));


/* TODO (CDRIVER-4052): Move MONGOC_RR_DEFAULT_BUFFER_SIZE and
 * _mongoc_client_get_rr to mongoc-topology-private.h or in a separate file.
 * There is no reason these should be in mongoc-client. */
#define MONGOC_RR_DEFAULT_BUFFER_SIZE 1024
bool
_mongoc_client_get_rr (const char *hostname,
                       mongoc_rr_type_t rr_type,
                       mongoc_rr_data_t *rr_data,
                       size_t initial_buffer_size,
                       bool prefer_tcp,
                       bson_error_t *error);

mongoc_client_t *
_mongoc_client_new_from_topology (mongoc_topology_t *topology);

bool
_mongoc_client_set_apm_callbacks_private (mongoc_client_t *client, mongoc_apm_callbacks_t *callbacks, void *context);

mongoc_stream_t *
mongoc_client_default_stream_initiator (const mongoc_uri_t *uri,
                                        const mongoc_host_list_t *host,
                                        void *user_data,
                                        bson_error_t *error);

mongoc_stream_t *
_mongoc_client_create_stream (mongoc_client_t *client, const mongoc_host_list_t *host, bson_error_t *error);

bool
_mongoc_client_recv (mongoc_client_t *client,
                     mcd_rpc_message *rpc,
                     mongoc_buffer_t *buffer,
                     mongoc_server_stream_t *server_stream,
                     bson_error_t *error);

void
_mongoc_client_kill_cursor (mongoc_client_t *client,
                            uint32_t server_id,
                            int64_t cursor_id,
                            int64_t operation_id,
                            const char *db,
                            const char *collection,
                            mongoc_client_session_t *cs);
bool
_mongoc_client_command_with_opts (mongoc_client_t *client,
                                  const char *db_name,
                                  const bson_t *command,
                                  mongoc_command_mode_t mode,
                                  const bson_t *opts,
                                  mongoc_query_flags_t flags,
                                  const mongoc_read_prefs_t *user_prefs,
                                  const mongoc_read_prefs_t *default_prefs,
                                  mongoc_read_concern_t *default_rc,
                                  mongoc_write_concern_t *default_wc,
                                  bson_t *reply,
                                  bson_error_t *error);

mongoc_server_session_t *
_mongoc_client_pop_server_session (mongoc_client_t *client, bson_error_t *error);

bool
_mongoc_client_lookup_session (const mongoc_client_t *client,
                               uint32_t client_session_id,
                               mongoc_client_session_t **cs,
                               bson_error_t *error);

void
_mongoc_client_unregister_session (mongoc_client_t *client, mongoc_client_session_t *session);

void
_mongoc_client_push_server_session (mongoc_client_t *client, mongoc_server_session_t *server_session);
void
_mongoc_client_end_sessions (mongoc_client_t *client);

mongoc_stream_t *
mongoc_client_connect_tcp (int32_t connecttimeoutms, const mongoc_host_list_t *host, bson_error_t *error);

mongoc_stream_t *
mongoc_client_connect (bool buffered,
                       bool use_ssl,
                       void *ssl_opts_void,
                       const mongoc_uri_t *uri,
                       const mongoc_host_list_t *host,
                       bson_error_t *error);


/* Returns true if a versioned server API has been selected, otherwise returns
 * false. */
bool
mongoc_client_uses_server_api (const mongoc_client_t *client);


/* Returns true if load balancing mode has been selected, otherwise returns
 * false. */
bool
mongoc_client_uses_loadbalanced (const mongoc_client_t *client);

BSON_END_DECLS

#endif /* MONGOC_CLIENT_PRIVATE_H */
