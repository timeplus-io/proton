/*
 * Copyright 2014 MongoDB, Inc.
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

#ifndef MONGOC_TOPOLOGY_SCANNER_PRIVATE_H
#define MONGOC_TOPOLOGY_SCANNER_PRIVATE_H

/* TODO: rename to TOPOLOGY scanner */

#include <bson/bson.h>
#include "mongoc-async-private.h"
#include "mongoc-async-cmd-private.h"
#include "mongoc-handshake-private.h"
#include "mongoc-host-list.h"
#include "mongoc-apm-private.h"
#include "mongoc-scram-private.h"
#include "mongoc-ssl.h"
#include "mongoc-crypto-private.h"
#include "mongoc-server-description-private.h"
#include "common-thread-private.h"

BSON_BEGIN_DECLS

typedef void (*mongoc_topology_scanner_setup_err_cb_t) (uint32_t id, void *data, const bson_error_t *error /* IN */);

typedef void (*mongoc_topology_scanner_cb_t) (
   uint32_t id, const bson_t *bson, int64_t rtt, void *data, const bson_error_t *error /* IN */);

struct mongoc_topology_scanner;
struct mongoc_topology_scanner_node;

typedef struct mongoc_topology_scanner_node {
   uint32_t id;
   /* after scanning, this is set to the successful stream if one exists. */
   mongoc_stream_t *stream;

   int64_t last_used;
   /* last_failed is set upon a network error trying to check a server.
    * last_failed is used to enforce cooldownMS.
    * last_failed is not set upon a network error during an application
    * operation on @stream. */
   int64_t last_failed;
   bool has_auth;
   bool hello_ok;
   mongoc_host_list_t host;
   struct mongoc_topology_scanner *ts;

   struct mongoc_topology_scanner_node *next;
   struct mongoc_topology_scanner_node *prev;

   bool retired;
   bson_error_t last_error;

   /* the hostname for a node may resolve to multiple DNS results.
    * dns_results has the full list of DNS results, ordered by host preference.
    * successful_dns_result is the most recent successful DNS result.
    */
   struct addrinfo *dns_results;
   struct addrinfo *successful_dns_result;
   int64_t last_dns_cache;

   /* used by single-threaded clients to store negotiated sasl mechanisms on a
    * node. */
   mongoc_handshake_sasl_supported_mechs_t sasl_supported_mechs;
   bool negotiated_sasl_supported_mechs;
   bson_t speculative_auth_response;
   mongoc_scram_t scram;

   /* handshake_sd is a server description constructed from the response of the
    * initial handshake. It is bound to the lifetime of stream. */
   mongoc_server_description_t *handshake_sd;
} mongoc_topology_scanner_node_t;

typedef enum handshake_state_t {
   /**
    * The handshake command has no value. The handshake_cmd pointer will be
    * NULL.
    */
   HANDSHAKE_CMD_UNINITIALIZED,
   /**
    * The handshake command could not be constructed because it would be too
    * large. The handshake_cmd pointer will be NULL.
    */
   HANDSHAKE_CMD_TOO_BIG,
   /**
    * The handshake command is valid and ready to be copied-from.
    */
   HANDSHAKE_CMD_OKAY,
} handshake_state_t;

typedef struct mongoc_topology_scanner {
   mongoc_async_t *async;
   int64_t connect_timeout_msec;
   mongoc_topology_scanner_node_t *nodes;
   bson_t hello_cmd;
   bson_t legacy_hello_cmd;
   bson_mutex_t handshake_cmd_mtx;
   bson_t *handshake_cmd;
   handshake_state_t handshake_state;
   bson_t cluster_time;
   const char *appname;

   mongoc_topology_scanner_setup_err_cb_t setup_err_cb;
   mongoc_topology_scanner_cb_t cb;
   void *cb_data;
   const mongoc_uri_t *uri;
   mongoc_async_cmd_setup_t setup;
   mongoc_stream_initiator_t initiator;
   void *initiator_context;
   bson_error_t error;

#ifdef MONGOC_ENABLE_SSL
   mongoc_ssl_opt_t *ssl_opts;
#endif

   mongoc_apm_callbacks_t apm_callbacks;
   void *apm_context;
   int64_t dns_cache_timeout_ms;
   /* only used by single-threaded clients to negotiate auth mechanisms. */
   bool negotiate_sasl_supported_mechs;
   bool bypass_cooldown;
   bool speculative_authentication;

   mongoc_server_api_t *api;
   bool loadbalanced;
} mongoc_topology_scanner_t;

mongoc_topology_scanner_t *
mongoc_topology_scanner_new (const mongoc_uri_t *uri,
                             mongoc_topology_scanner_setup_err_cb_t setup_err_cb,
                             mongoc_topology_scanner_cb_t cb,
                             void *data,
                             int64_t connect_timeout_msec);

void
mongoc_topology_scanner_destroy (mongoc_topology_scanner_t *ts);

bool
mongoc_topology_scanner_valid (mongoc_topology_scanner_t *ts);

void
mongoc_topology_scanner_add (mongoc_topology_scanner_t *ts, const mongoc_host_list_t *host, uint32_t id, bool hello_ok);

void
mongoc_topology_scanner_scan (mongoc_topology_scanner_t *ts, uint32_t id);

void
mongoc_topology_scanner_disconnect (mongoc_topology_scanner_t *scanner);

void
mongoc_topology_scanner_node_retire (mongoc_topology_scanner_node_t *node);

void
mongoc_topology_scanner_node_disconnect (mongoc_topology_scanner_node_t *node, bool failed);

void
mongoc_topology_scanner_node_destroy (mongoc_topology_scanner_node_t *node, bool failed);

bool
mongoc_topology_scanner_in_cooldown (mongoc_topology_scanner_t *ts, int64_t when);

void
mongoc_topology_scanner_start (mongoc_topology_scanner_t *ts, bool obey_cooldown);

void
mongoc_topology_scanner_work (mongoc_topology_scanner_t *ts);

void
_mongoc_topology_scanner_finish (mongoc_topology_scanner_t *ts);

void
mongoc_topology_scanner_get_error (mongoc_topology_scanner_t *ts, bson_error_t *error);

void
mongoc_topology_scanner_reset (mongoc_topology_scanner_t *ts);

void
mongoc_topology_scanner_node_setup (mongoc_topology_scanner_node_t *node, bson_error_t *error);

mongoc_topology_scanner_node_t *
mongoc_topology_scanner_get_node (mongoc_topology_scanner_t *ts, uint32_t id);

void
_mongoc_topology_scanner_add_speculative_authentication (bson_t *cmd,
                                                         const mongoc_uri_t *uri,
                                                         const mongoc_ssl_opt_t *ssl_opts,
                                                         mongoc_scram_t *scram /* OUT */);

void
_mongoc_topology_scanner_parse_speculative_authentication (const bson_t *hello, bson_t *speculative_authenticate);

const char *
_mongoc_topology_scanner_get_speculative_auth_mechanism (const mongoc_uri_t *uri);

const bson_t *
_mongoc_topology_scanner_get_monitoring_cmd (mongoc_topology_scanner_t *ts, bool hello_ok);

/**
 * @brief Get the scanner's associated handshake command BSON document.
 *
 * @param ts The scanner to inspect
 * @param copy_into A pointer to an initialized bson_t. The handshake command
 * will be copied into the pointee.
 */
void
_mongoc_topology_scanner_dup_handshake_cmd (mongoc_topology_scanner_t *ts, bson_t *copy_into);

bool
mongoc_topology_scanner_has_node_for_host (mongoc_topology_scanner_t *ts, mongoc_host_list_t *host);

void
mongoc_topology_scanner_set_stream_initiator (mongoc_topology_scanner_t *ts, mongoc_stream_initiator_t si, void *ctx);
bool
_mongoc_topology_scanner_set_appname (mongoc_topology_scanner_t *ts, const char *name);
void
_mongoc_topology_scanner_set_cluster_time (mongoc_topology_scanner_t *ts, const bson_t *cluster_time);

void
_mongoc_topology_scanner_set_dns_cache_timeout (mongoc_topology_scanner_t *ts, int64_t timeout_ms);

#ifdef MONGOC_ENABLE_SSL
void
mongoc_topology_scanner_set_ssl_opts (mongoc_topology_scanner_t *ts, mongoc_ssl_opt_t *opts);
#endif

bool
mongoc_topology_scanner_node_in_cooldown (mongoc_topology_scanner_node_t *node, int64_t when);

void
_mongoc_topology_scanner_set_server_api (mongoc_topology_scanner_t *ts, const mongoc_server_api_t *api);

void
_mongoc_topology_scanner_set_loadbalanced (mongoc_topology_scanner_t *ts, bool val);

/* for testing. */
mongoc_stream_t *
_mongoc_topology_scanner_tcp_initiate (mongoc_async_cmd_t *acmd);

/* Returns true if versioned server API has been selected, otherwise
 * false. */
bool
mongoc_topology_scanner_uses_server_api (const mongoc_topology_scanner_t *ts);

/* Returns true if load balancing mode has been selected, otherwise false. */
bool
mongoc_topology_scanner_uses_loadbalanced (const mongoc_topology_scanner_t *ts);

BSON_END_DECLS

#endif /* MONGOC_TOPOLOGY_SCANNER_PRIVATE_H */
