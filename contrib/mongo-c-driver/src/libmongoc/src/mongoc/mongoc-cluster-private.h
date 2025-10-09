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

#ifndef MONGOC_CLUSTER_PRIVATE_H
#define MONGOC_CLUSTER_PRIVATE_H

#include <bson/bson.h>

#include "mcd-rpc.h"
#include "mongoc-array-private.h"
#include "mongoc-buffer-private.h"
#include "mongoc-config.h"
#include "mongoc-client.h"
#include "mongoc-list-private.h"
#include "mongoc-opcode.h"
#include "mongoc-rpc-private.h"
#include "mongoc-server-stream-private.h"
#include "mongoc-set-private.h"
#include "mongoc-stream.h"
#include "mongoc-topology-private.h"
#include "mongoc-topology-description-private.h"
#include "mongoc-write-concern.h"
#include "mongoc-scram-private.h"
#include "mongoc-cmd-private.h"
#include "mongoc-crypto-private.h"
#include "mongoc-deprioritized-servers-private.h"

BSON_BEGIN_DECLS


typedef struct _mongoc_cluster_node_t {
   mongoc_stream_t *stream;
   char *connection_address;
   /* handshake_sd is a server description created from the handshake on the
    * stream. */
   mongoc_server_description_t *handshake_sd;
} mongoc_cluster_node_t;

typedef struct _mongoc_cluster_t {
   int64_t operation_id;
   int32_t request_id;
   int32_t sockettimeoutms;
   int32_t socketcheckintervalms;
   mongoc_uri_t *uri;
   unsigned requires_auth : 1;

   mongoc_client_t *client;

   mongoc_set_t *nodes;
   mongoc_array_t iov;
} mongoc_cluster_t;


void
mongoc_cluster_init (mongoc_cluster_t *cluster, const mongoc_uri_t *uri, void *client);

void
mongoc_cluster_destroy (mongoc_cluster_t *cluster);

void
mongoc_cluster_set_sockettimeoutms (mongoc_cluster_t *cluster, int32_t sockettimeoutms);

void
mongoc_cluster_reset_sockettimeoutms (mongoc_cluster_t *cluster);

void
mongoc_cluster_disconnect_node (mongoc_cluster_t *cluster, uint32_t id);

int32_t
mongoc_cluster_get_max_bson_obj_size (mongoc_cluster_t *cluster);

int32_t
mongoc_cluster_get_max_msg_size (mongoc_cluster_t *cluster);

size_t
_mongoc_cluster_buffer_iovec (mongoc_iovec_t *iov, size_t iovcnt, int skip, char *buffer);

bool
mongoc_cluster_check_interval (mongoc_cluster_t *cluster, uint32_t server_id);

bool
mongoc_cluster_legacy_rpc_sendv_to_server (mongoc_cluster_t *cluster,
                                           mcd_rpc_message *rpc,
                                           mongoc_server_stream_t *server_stream,
                                           bson_error_t *error);

bool
mongoc_cluster_try_recv (mongoc_cluster_t *cluster,
                         mcd_rpc_message *rpc,
                         mongoc_buffer_t *buffer,
                         mongoc_server_stream_t *server_stream,
                         bson_error_t *error);

/**
 * @brief Obtain a server stream appropriate for read operations on the
 * cluster.
 *
 * Returns a new stream (that must be freed) or NULL and sets an error via
 * `error`.
 *
 * @note The returned stream must be released via
 * `mongoc_server_stream_cleanup`.
 *
 * @note May add nodes and/or update the cluster's topology.
 */
mongoc_server_stream_t *
mongoc_cluster_stream_for_reads (mongoc_cluster_t *cluster,
                                 const mongoc_read_prefs_t *read_prefs,
                                 mongoc_client_session_t *cs,
                                 const mongoc_deprioritized_servers_t *ds,
                                 bson_t *reply,
                                 bson_error_t *error);

/**
 * @brief Obtain a server stream appropriate for write operations on the
 * cluster.
 *
 * Returns a new stream (that must be freed) or NULL and sets an error via
 * `error`.
 *
 * @note The returned stream must be released via `mongoc_server_stream_cleanup`
 *
 * @note May add nodes and/or update the cluster's topology.
 */
mongoc_server_stream_t *
mongoc_cluster_stream_for_writes (mongoc_cluster_t *cluster,
                                  mongoc_client_session_t *cs,
                                  const mongoc_deprioritized_servers_t *ds,
                                  bson_t *reply,
                                  bson_error_t *error);

/**
 * @brief Obtain a server stream appropriate for aggregate operations with
 * writes on the cluster.
 *
 * Returns a new stream (that must be freed) or NULL and sets an error via
 * `error`.
 *
 * @note The returned stream must be released via
 * `mongoc_server_stream_cleanup`.
 *
 * @note May add nodes and/or update the cluster's topology.
 */
mongoc_server_stream_t *
mongoc_cluster_stream_for_aggr_with_write (mongoc_cluster_t *cluster,
                                           const mongoc_read_prefs_t *read_prefs,
                                           mongoc_client_session_t *cs,
                                           bson_t *reply,
                                           bson_error_t *error);

/**
 * @brief Obtain a server stream associated with the cluster node associated
 * with the given server ID.
 *
 * Returns a new server stream (that must be freed) or NULL and sets `error`.
 *
 * @param server_id The ID of a server in the cluster topology.
 * @param reconnect_ok If `true`, the server exists in the topology but is not
 * connected, then attempt to reconnect with the server. If `false`, then only
 * create a stream if the server is connected and ready.
 *
 * @note The returned stream must be released via `mongoc_server_stream_cleanup`
 *
 * @note May update the cluster's topology.
 */
mongoc_server_stream_t *
mongoc_cluster_stream_for_server (mongoc_cluster_t *cluster,
                                  uint32_t server_id,
                                  bool reconnect_ok,
                                  mongoc_client_session_t *cs,
                                  bson_t *reply,
                                  bson_error_t *error);

bool
mongoc_cluster_stream_valid (mongoc_cluster_t *cluster, mongoc_server_stream_t *server_stream);

bool
mongoc_cluster_run_command_monitored (mongoc_cluster_t *cluster, mongoc_cmd_t *cmd, bson_t *reply, bson_error_t *error);

// `mongoc_cluster_run_retryable_write` executes a write command and may apply retryable writes behavior.
// `cmd->server_stream` is set to `*retry_server_stream` on retry. Otherwise, it is unmodified.
// `*retry_server_stream` is set to a new stream on retry. The caller must call `mongoc_server_stream_cleanup`.
// `*reply` must be uninitialized and is always initialized upon return. The caller must call `bson_destroy`.
bool
mongoc_cluster_run_retryable_write (mongoc_cluster_t *cluster,
                                    mongoc_cmd_t *cmd,
                                    bool is_retryable_write,
                                    mongoc_server_stream_t **retry_server_stream,
                                    bson_t *reply,
                                    bson_error_t *error);

bool
mongoc_cluster_run_command_parts (mongoc_cluster_t *cluster,
                                  mongoc_server_stream_t *server_stream,
                                  mongoc_cmd_parts_t *parts,
                                  bson_t *reply,
                                  bson_error_t *error);

bool
mongoc_cluster_run_command_private (mongoc_cluster_t *cluster,
                                    const mongoc_cmd_t *cmd,
                                    bson_t *reply,
                                    bson_error_t *error);

void
_mongoc_cluster_build_sasl_start (bson_t *cmd, const char *mechanism, const char *buf, uint32_t buflen);

void
_mongoc_cluster_build_sasl_continue (bson_t *cmd, int conv_id, const char *buf, uint32_t buflen);

int
_mongoc_cluster_get_conversation_id (const bson_t *reply);

mongoc_server_stream_t *
_mongoc_cluster_create_server_stream (const mongoc_topology_description_t *td,
                                      const mongoc_server_description_t *sd,
                                      mongoc_stream_t *stream);

bool
_mongoc_cluster_get_auth_cmd_x509 (const mongoc_uri_t *uri,
                                   const mongoc_ssl_opt_t *ssl_opts,
                                   bson_t *cmd /* OUT */,
                                   bson_error_t *error /* OUT */);

/* Returns true if a versioned server API has been selected, otherwise returns
 * false. */
bool
mongoc_cluster_uses_server_api (const mongoc_cluster_t *cluster);

/* Returns true if load balancing mode has been selected, otherwise returns
 * false. */
bool
mongoc_cluster_uses_loadbalanced (const mongoc_cluster_t *cluster);

#ifdef MONGOC_ENABLE_CRYPTO
void
_mongoc_cluster_init_scram (const mongoc_cluster_t *cluster,
                            mongoc_scram_t *scram,
                            mongoc_crypto_hash_algorithm_t algo);

bool
_mongoc_cluster_get_auth_cmd_scram (mongoc_crypto_hash_algorithm_t algo,
                                    mongoc_scram_t *scram,
                                    bson_t *cmd /* OUT */,
                                    bson_error_t *error /* OUT */);
#endif /* MONGOC_ENABLE_CRYPTO */

bool
mcd_rpc_message_compress (mcd_rpc_message *rpc,
                          int32_t compressor_id,
                          int32_t compression_level,
                          void **compressed_data,
                          size_t *compressed_data_len,
                          bson_error_t *error);

bool
mcd_rpc_message_decompress (mcd_rpc_message *rpc, void **data, size_t *data_len);

bool
mcd_rpc_message_decompress_if_necessary (mcd_rpc_message *rpc, void **data, size_t *data_len);

BSON_END_DECLS


#endif /* MONGOC_CLUSTER_PRIVATE_H */
