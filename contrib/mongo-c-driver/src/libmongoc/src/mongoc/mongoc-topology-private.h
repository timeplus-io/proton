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

#ifndef MONGOC_TOPOLOGY_PRIVATE_H
#define MONGOC_TOPOLOGY_PRIVATE_H

#include "mongoc-config.h"
#include "mongoc-topology-scanner-private.h"
#include "mongoc-server-description-private.h"
#include "mongoc-topology-description-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-uri.h"
#include "mongoc-client-session-private.h"
#include "mongoc-crypt-private.h"
#include "mongoc-ts-pool-private.h"
#include "mongoc-shared-private.h"
#include "mongoc-sleep.h"

#define MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS 500
#define MONGOC_TOPOLOGY_SOCKET_CHECK_INTERVAL_MS 5000
#define MONGOC_TOPOLOGY_COOLDOWN_MS 5000
#define MONGOC_TOPOLOGY_LOCAL_THRESHOLD_MS 15
#define MONGOC_TOPOLOGY_SERVER_SELECTION_TIMEOUT_MS 30000
#define MONGOC_TOPOLOGY_HEARTBEAT_FREQUENCY_MS_MULTI_THREADED 10000
#define MONGOC_TOPOLOGY_HEARTBEAT_FREQUENCY_MS_SINGLE_THREADED 60000
#define MONGOC_TOPOLOGY_MIN_RESCAN_SRV_INTERVAL_MS 60000

typedef enum {
   MONGOC_TOPOLOGY_SCANNER_OFF,
   MONGOC_TOPOLOGY_SCANNER_BG_RUNNING,
   MONGOC_TOPOLOGY_SCANNER_SHUTTING_DOWN
} mongoc_topology_scanner_state_t;

typedef enum mongoc_topology_cse_state_t {
   MONGOC_CSE_DISABLED,
   MONGOC_CSE_STARTING,
   MONGOC_CSE_ENABLED,
} mongoc_topology_cse_state_t;

struct _mongoc_background_monitor_t;
struct _mongoc_client_pool_t;

typedef enum { MONGOC_RR_SRV, MONGOC_RR_TXT } mongoc_rr_type_t;

typedef struct _mongoc_rr_data_t {
   /* Number of records returned by DNS. */
   uint32_t count;

   /* Set to lowest TTL found when polling SRV records. */
   uint32_t min_ttl;

   /* Set to the resulting host list when polling SRV records */
   mongoc_host_list_t *hosts;

   /* Set to the TXT record when polling for TXT */
   char *txt_record_opts;
} mongoc_rr_data_t;

struct _mongoc_topology_t;

MONGOC_DECL_SPECIAL_TS_POOL (mongoc_server_session_t,
                             mongoc_server_session_pool,
                             struct _mongoc_topology_t,
                             /* ctor/dtor/prune are defined in the new_with_params call */
                             NULL,
                             NULL,
                             NULL)

typedef bool (*_mongoc_rr_resolver_fn) (const char *hostname,
                                        mongoc_rr_type_t rr_type,
                                        mongoc_rr_data_t *rr_data,
                                        size_t initial_buffer_size,
                                        bool prefer_tcp,
                                        bson_error_t *error);

/**
 * @brief A reference-counted reference to a topology description.
 *
 * The referred-to topology description should be access via the `.ptr` member
 * of this object.
 */
typedef union mc_shared_tpld {
   /* Private: The reference-counted shared pointer that manages the topology
    * description. */
   mongoc_shared_ptr _sptr_;
   /** The pointed-to topology description */
   mongoc_topology_description_t const *ptr;
} mc_shared_tpld;

/** A null-pointer initializer for an `mc_shared_tpld` */
#define MC_SHARED_TPLD_NULL ((mc_shared_tpld){._sptr_ = MONGOC_SHARED_PTR_NULL})

typedef struct _mongoc_topology_t {
   /**
    * @brief The topology description. Do not access directly. Instead, use
    * mc_tpld_take_ref()
    */
   mc_shared_tpld _shared_descr_;

   /* topology->uri is initialized as a copy of the client/pool's URI.
    * For a "mongodb+srv://" URI, topology->uri is then updated in
    * mongoc_topology_new() after initial seedlist discovery.
    */
   mongoc_uri_t *uri;
   mongoc_topology_scanner_t *scanner;
   bool server_selection_try_once;

   int64_t last_scan;
   int64_t local_threshold_msec;
   int64_t connect_timeout_msec;
   int64_t server_selection_timeout_msec;
   /* defaults to 500ms, configurable by tests */
   int64_t min_heartbeat_frequency_msec;

   /* Minimum of SRV record TTLs, but no lower than 60 seconds.
    * May be zero for non-SRV/non-MongoS topology.
    * DO NOT access directly: use the accessor methods to get/set the value.
    */
   int64_t _atomic_srv_polling_rescan_interval_ms;
   int64_t srv_polling_last_scan_ms;
   /* For multi-threaded, srv polling occurs in a separate thread. */
   bson_thread_t srv_polling_thread;
   bson_mutex_t srv_polling_mtx;
   mongoc_cond_t srv_polling_cond;

   /**
    * @brief Signal for background monitoring threads to signal stop/shutdown.
    *
    * The values stored are mongoc_topology_scanner_state_t values
    */
   int scanner_state;

   /**
    * @brief This lock is held in order to serialize operations that modify the
    * topology description. It *should not* be held while performing read-only
    * operations on the topology.
    *
    * This mutex is also used by server selection to synchronize with threads
    * that may update the topology following a failed server selection. It is
    * used in conjunction with `cond_client`. This protects _shared_descr_, as
    * well as the server_monitors and rtt_monitors.
    */
   bson_mutex_t tpld_modification_mtx;

   /**
    * @brief Condition variable used to signal client threads that the topology
    * has been updated by another thread. This CV should be used with
    * tpld_modification_mtx, as it signals modifications to the topology.
    *
    * Note that mc_tpld_modify_begin/commit/drop will acquire/release
    * tpld_modification_mtx as well.
    */
   mongoc_cond_t cond_client;

   bool single_threaded;
   bool stale;

   mongoc_server_session_pool session_pool;

   /* Is client side encryption enabled? */
   mongoc_topology_cse_state_t cse_state;
   bool is_srv_polling;

#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION
   _mongoc_crypt_t *crypt;
   struct _mongoc_client_t *mongocryptd_client;           /* single threaded */
   struct _mongoc_client_t *keyvault_client;              /* single threaded */
   struct _mongoc_client_pool_t *mongocryptd_client_pool; /* multi threaded */
   struct _mongoc_client_pool_t *keyvault_client_pool;    /* multi threaded */
   char *keyvault_db;
   char *keyvault_coll;
   bool bypass_auto_encryption;
   bool mongocryptd_bypass_spawn;
   char *mongocryptd_spawn_path;
   bson_t *mongocryptd_spawn_args;
   bool bypass_query_analysis;
#endif

   struct {
      struct {
         struct {
            char *cryptSharedLibPath;
            bool cryptSharedLibRequired;
         } extraOptions;
      } autoOptions;
   } clientSideEncryption;

   // Corresponds to AutoEncryptionOpts.encryptedFieldsMap.
   bson_t *encrypted_fields_map;

   /* For background monitoring. */
   mongoc_set_t *server_monitors;
   mongoc_set_t *rtt_monitors;
   bson_mutex_t apm_mutex;

   /* This is overridable for SRV polling tests to mock DNS records. */
   _mongoc_rr_resolver_fn rr_resolver;

   /* valid is false when mongoc_topology_new failed to construct a valid
    * topology. This could occur if the URI is invalid.
    * An invalid topology does not monitor servers. */
   bool valid;

   // `usleep_fn` and `usleep_data` may be overridden by
   // `mongoc_client_set_usleep_impl`.
   mongoc_usleep_func_t usleep_fn;
   void *usleep_data;

   // `srv_prefer_tcp` determines if DNS lookup for SRV tries TCP first instead of UDP.
   // DNS implementations are expected to try UDP first, then retry with TCP if the UDP response indicates truncation.
   // Some DNS servers truncate UDP responses without setting the truncated (TC) flag. This may result in no TCP retry.
   bool srv_prefer_tcp;
} mongoc_topology_t;

mongoc_topology_t *
mongoc_topology_new (const mongoc_uri_t *uri, bool single_threaded);

void
mongoc_topology_set_apm_callbacks (mongoc_topology_t *topology,
                                   mongoc_topology_description_t *td,
                                   mongoc_apm_callbacks_t const *callbacks,
                                   void *context);

void
mongoc_topology_destroy (mongoc_topology_t *topology);

void
mongoc_topology_reconcile (const mongoc_topology_t *topology, mongoc_topology_description_t *td);

bool
mongoc_topology_compatible (const mongoc_topology_description_t *td,
                            const mongoc_read_prefs_t *read_prefs,
                            bson_error_t *error);

/**
 * @brief Select a server description for an operation. May scan and update the
 * topology.
 *
 * A server description might be returned that matches the given `optype` and
 * `read_prefs`. If the topology is out-of-date or due for a scan, then this
 * function will perform a scan and update the topology accordingly. If no
 * matching server is found, returns a NULL pointer.
 *
 * @param topology The topology to inspect and/or update.
 * @param optype The operation that is intended to be performed.
 * @param read_prefs The read preferences for the command.
 * @param must_use_primary An optional output parameter. Server selection might
 * need to override the caller's read preferences' read mode to 'primary'.
 * Whether or not that takes place will be set through this pointer.
 * @param error An output parameter for any error information.
 * @return mongoc_server_description_t* A copy of the topology's server
 * description that matches the request, or NULL if there is no such server.
 *
 * @note The returned object is a COPY, and should be released with
 * `mongoc_server_description_destroy()`
 *
 * @note This function may update the topology description.
 */
mongoc_server_description_t *
mongoc_topology_select (mongoc_topology_t *topology,
                        mongoc_ss_optype_t optype,
                        const mongoc_read_prefs_t *read_prefs,
                        bool *must_use_primary,
                        bson_error_t *error);

/**
 * @brief Obtain the integral ID of a server description matching the requested
 * ops.
 *
 * Refer to @see mongoc_topology_select() for more information
 *
 * @param topology The topology to inspect and/or update.
 * @param optype The operation that is intended to be performed.
 * @param read_prefs The read preferences for the command.
 * @param must_use_primary An optional output parameter. Server selection might
 * need to override the caller's read preferences' read mode to 'primary'.
 * Whether or not that takes place will be set through this pointer.
 * @param ds A list of servers that should be selected only if there are no
 * other suitable servers.
 * @param error An output parameter for any error information.
 * @return uint32_t A non-zero integer ID of the server description. In case of
 * error, sets `error` and returns zero.
 *
 * @note This function may update the topology description.
 */
uint32_t
mongoc_topology_select_server_id (mongoc_topology_t *topology,
                                  mongoc_ss_optype_t optype,
                                  const mongoc_read_prefs_t *read_prefs,
                                  bool *must_use_primary,
                                  const mongoc_deprioritized_servers_t *ds,
                                  bson_error_t *error);

/**
 * @brief Return a new mongoc_host_list_t for the given server matching the
 * given ID.
 *
 * @param topology The topology description to inspect
 * @param id The ID of a server in the topology
 * @param error Output error information
 * @return mongoc_host_list_t* A new host list, or NULL on error
 *
 * @note The returned list should be freed with
 * `_mongoc_host_list_destroy_all()`
 */
mongoc_host_list_t *
_mongoc_topology_host_by_id (const mongoc_topology_description_t *topology, uint32_t id, bson_error_t *error);

/**
 * @brief Update the topology from the response to a handshake on a new
 * application connection.
 *
 * @note Only applicable to a client pool (single-threaded clients reuse
 * monitoring connections).
 *
 * @param topology The topology that will be updated.
 * @param sd The server description that contains the hello response.
 * @return true If the server is valid in the topology.
 * @return false If the server was already removed from the topology.
 */
bool
_mongoc_topology_update_from_handshake (mongoc_topology_t *topology, const mongoc_server_description_t *sd);

void
_mongoc_topology_update_last_used (mongoc_topology_t *topology, uint32_t server_id);

int64_t
mongoc_topology_server_timestamp (mongoc_topology_t *topology, uint32_t id);

/**
 * @brief Get the current type of the topology
 */
mongoc_topology_description_type_t
_mongoc_topology_get_type (const mongoc_topology_t *topology);

bool
_mongoc_topology_set_appname (mongoc_topology_t *topology, const char *appname);

void
_mongoc_topology_update_cluster_time (mongoc_topology_t *topology, const bson_t *reply);

mongoc_server_session_t *
_mongoc_topology_pop_server_session (mongoc_topology_t *topology, bson_error_t *error);

void
_mongoc_topology_push_server_session (mongoc_topology_t *topology, mongoc_server_session_t *server_session);

bool
_mongoc_topology_end_sessions_cmd (mongoc_topology_t *topology, bson_t *cmd);

void
_mongoc_topology_do_blocking_scan (mongoc_topology_t *topology, bson_error_t *error);

/**
 * @brief Duplicate the handshake command of the topology scanner.
 *
 * @param topology The topology to inspect.
 * @param copy_into The destination of the copy. Should be uninitialized storage
 * for a bson_t.
 *
 * @note This API will lazily construct the handshake command for the scanner.
 *
 * @note This is called at the start of the scan in
 * _mongoc_topology_run_background, when a node is added in
 * _mongoc_topology_reconcile_add_nodes, or when running a hello directly on a
 * node in _mongoc_stream_run_hello.
 */
void
_mongoc_topology_dup_handshake_cmd (const mongoc_topology_t *topology, bson_t *copy_into);
void
_mongoc_topology_request_scan (mongoc_topology_t *topology);

void
_mongoc_topology_bypass_cooldown (mongoc_topology_t *topology);

typedef enum {
   MONGOC_SDAM_APP_ERROR_COMMAND,
   MONGOC_SDAM_APP_ERROR_NETWORK,
   MONGOC_SDAM_APP_ERROR_TIMEOUT
} _mongoc_sdam_app_error_type_t;

/**
 * @brief Handle an error from an app connection
 *
 * Processes network errors, timeouts, and command replies.
 *
 * @param topology The topology that will be updated
 * @param server_id The ID of the server on which the error occurred.
 * @param handshake_complete Whether the handshake was complete for this server
 * @param type The type of error to process
 * @param reply If checking for a command error, the server reply. Otherwise
 * NULL
 * @param why An error that will be attached to the server description
 * @param max_wire_version
 * @param generation The generation of the server description the caller was
 * using.
 * @param service_id A service ID for a load-balanced deployment. If not
 * applicable, pass kZeroServiceID.
 * @return true If the topology was updated and the pool was cleared.
 * @return false If no modifications were made and the error was ignored.
 *
 * @note May update the topology description.
 */
bool
_mongoc_topology_handle_app_error (mongoc_topology_t *topology,
                                   uint32_t server_id,
                                   bool handshake_complete,
                                   _mongoc_sdam_app_error_type_t type,
                                   const bson_t *reply,
                                   const bson_error_t *why,
                                   uint32_t max_wire_version,
                                   uint32_t generation,
                                   const bson_oid_t *service_id);

void
mongoc_topology_rescan_srv (mongoc_topology_t *topology);

bool
mongoc_topology_should_rescan_srv (mongoc_topology_t *topology);

/* _mongoc_topology_set_rr_resolver is called by tests to mock DNS responses for
 * SRV polling.
 * This is necessarily called after initial seedlist discovery completes in
 * mongoc_topology_new.
 * Callers should call this before monitoring starts.
 */
void
_mongoc_topology_set_rr_resolver (mongoc_topology_t *topology, _mongoc_rr_resolver_fn rr_resolver);

/**
 * @brief Thread-safe update the SRV polling rescan interval on the given topology
 */
static BSON_INLINE void
_mongoc_topology_set_srv_polling_rescan_interval_ms (mongoc_topology_t *topology, int64_t val)
{
   bson_atomic_int64_exchange (&topology->_atomic_srv_polling_rescan_interval_ms, val, bson_memory_order_seq_cst);
}

/**
 * @brief Thread-safe get the SRV polling interval
 */
static BSON_INLINE int64_t
_mongoc_topology_get_srv_polling_rescan_interval_ms (mongoc_topology_t const *topology)
{
   return bson_atomic_int64_fetch (&topology->_atomic_srv_polling_rescan_interval_ms, bson_memory_order_seq_cst);
}

/**
 * @brief Return the latest connection generation for the server_id and/or
 * service_id.
 *
 * Use this generation for newly established connections.
 *
 * @param td The topology that contains the server
 * @param server_id The ID of the server to inspect
 * @param service_id The service ID of the connection if applicable, or
 * kZeroServiceID.
 * @returns uint32_t A generation counter for the given server, or zero if the
 * server does not exist in the topology.
 */
uint32_t
_mongoc_topology_get_connection_pool_generation (const mongoc_topology_description_t *td,
                                                 uint32_t server_id,
                                                 const bson_oid_t *service_id);

/**
 * @brief Obtain a reference to the current topology description for the given
 * topology.
 *
 * Returns a ref-counted reference to the topology description. The returned
 * reference must later be released with mc_tpld_drop_ref(). The contents of the
 * topology description are immutable.
 */
static BSON_INLINE mc_shared_tpld
mc_tpld_take_ref (const mongoc_topology_t *tpl)
{
   return (mc_shared_tpld){._sptr_ = mongoc_atomic_shared_ptr_load (&tpl->_shared_descr_._sptr_)};
}

/**
 * @brief Release a reference to a topology description obtained via
 * mc_tpld_take_ref().
 *
 * The pointed-to shared reference will be reset to NULL.
 */
static BSON_INLINE void
mc_tpld_drop_ref (mc_shared_tpld *p)
{
   mongoc_shared_ptr_reset_null (&p->_sptr_);
}

/**
 * @brief Refresh a reference to a topology description for the given topology.
 *
 * @param td Pointer-to-shared-pointer of the topology description
 * @param tpl The topology to query.
 *
 * The pointed-to shared pointer will be modified to refer to the topology
 * description of the topology.
 *
 * Equivalent to a call to `mc_tpld_drop_ref()` followed by a call to
 * `mc_tpld_take_ref()`.
 */
static BSON_INLINE void
mc_tpld_renew_ref (mc_shared_tpld *td, mongoc_topology_t *tpl)
{
   mc_tpld_drop_ref (td);
   *td = mc_tpld_take_ref (tpl);
}

/**
 * @brief A pending topology description modification.
 *
 * Create an instance using `mc_tpld_modify_begin()`.
 */
typedef struct mc_tpld_modification {
   /** The new topology. Modifications should be applied to this topology
    * description. Those modifications will be published by
    * `mc_tpld_modify_commit()`. */
   mongoc_topology_description_t *new_td;
   /** The topology that owns the topology description */
   mongoc_topology_t *topology;
} mc_tpld_modification;

/**
 * @brief Begin a new modification transaction of the topology description owned
 * by `tpl`
 *
 * @return mc_tpld_modification A pending modification.
 *
 * @note MUST be followed by a call to `mc_tpld_modify_commit` OR
 * `mc_tpld_modify_drop`
 *
 * @note THIS FUNCTION MAY BLOCK: This call takes a lock, which will only be
 * released by mc_tpld_modify_commit() or mc_tpld_modify_drop(). Do not call
 * this API while the current thread is already performing a modification!
 */
mc_tpld_modification
mc_tpld_modify_begin (mongoc_topology_t *tpl);

/**
 * @brief Commit a topology description modification to the owning topology.
 *
 * All later calls to mc_tpld_take_ref() will see the new topology.
 */
void mc_tpld_modify_commit (mc_tpld_modification);

/**
 * @brief Drop a pending modification to a topology description. No changes will
 * be made to the topology.
 */
void mc_tpld_modify_drop (mc_tpld_modification);

/**
 * @brief Obtain a pointer-to-mutable mongoc_topology_description_t for the
 * given topology.
 *
 * This call is "unsafe" as the returned pointer may be invalidated by
 * concurrent modifications done using mc_tpld_modify_begin() and
 * mc_tpld_modify_commit().
 *
 * To obtain a safe pointer to the topology description, use mc_tpld_take_ref().
 */
static BSON_INLINE mongoc_topology_description_t *
mc_tpld_unsafe_get_mutable (mongoc_topology_t *tpl)
{
   return tpl->_shared_descr_._sptr_.ptr;
}

/**
 * @brief Obtain a pointer-to-const mongoc_topology_description_t for the
 * given topology.
 *
 * This call is "unsafe" as the returned pointer may be invalidated by
 * concurrent modifications done using mc_tpld_modify_begin() and
 * mc_tpld_modify_commit().
 *
 * To obtain a safe pointer to the topology description, use mc_tpld_take_ref().
 *
 * @return const mongoc_topology_description_t* Pointer to the topology
 * description for the given topology.
 */
static BSON_INLINE const mongoc_topology_description_t *
mc_tpld_unsafe_get_const (const mongoc_topology_t *tpl)
{
   return tpl->_shared_descr_._sptr_.ptr;
}

/**
 * @brief Directly invalidate a server in the topology by its ID.
 *
 * This is intended for testing purposes, as it provides thread-safe
 * direct topology modification.
 *
 * @param td The topology to modify.
 * @param server_id The ID of a server in the topology.
 */
static BSON_INLINE void
_mongoc_topology_invalidate_server (mongoc_topology_t *td, uint32_t server_id)
{
   bson_error_t error;
   mc_tpld_modification tdmod = mc_tpld_modify_begin (td);
   bson_set_error (&error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "invalidated");
   mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, &error);
   mc_tpld_modify_commit (tdmod);
}

/* Return an array view to `max_hosts` or fewer elements of `hl`, or NULL if
 * `hl` is empty. The size of the returned array is written to `hl_array_size`
 * even if `hl` is empty.
 *
 * The returned array must be freed with `bson_free()`. The elements of the
 * array must not be freed, as they are still owned by `hl`.
 */
const mongoc_host_list_t **
_mongoc_apply_srv_max_hosts (const mongoc_host_list_t *hl, size_t max_hosts, size_t *hl_array_size);


/* Returns true if a versioned server API has been selected, otherwise returns
 * false. */
bool
mongoc_topology_uses_server_api (const mongoc_topology_t *topology);


/* Returns true if load balancing mode has been seelected, otherwise returns
 * false. */
bool
mongoc_topology_uses_loadbalanced (const mongoc_topology_t *topology);

#endif
