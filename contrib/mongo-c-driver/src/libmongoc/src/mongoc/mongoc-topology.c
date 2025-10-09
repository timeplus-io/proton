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

#include "mongoc-config.h"

#include "mongoc-handshake.h"
#include "mongoc-handshake-private.h"

#include "mongoc-error.h"
#include "mongoc-host-list-private.h"
#include "mongoc-log.h"
#include "mongoc-topology-private.h"
#include "mongoc-topology-description-apm-private.h"
#include "mongoc-client-private.h"
#include "mongoc-cmd-private.h"
#include "mongoc-uri-private.h"
#include "mongoc-util-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-error-private.h"
#include "mongoc-topology-background-monitoring-private.h"
#include "mongoc-read-prefs-private.h"

#include "utlist.h"

#include <stdint.h>

static void
_topology_collect_errors (const mongoc_topology_description_t *topology, bson_error_t *error_out);

static bool
_mongoc_topology_reconcile_add_nodes (mongoc_server_description_t *sd, mongoc_topology_scanner_t *scanner)
{
   mongoc_topology_scanner_node_t *node;

   /* Search by ID and update hello_ok */
   node = mongoc_topology_scanner_get_node (scanner, sd->id);
   if (node) {
      node->hello_ok = sd->hello_ok;
   } else if (!mongoc_topology_scanner_has_node_for_host (scanner, &sd->host)) {
      /* A node for this host was retired in this scan. */
      mongoc_topology_scanner_add (scanner, &sd->host, sd->id, sd->hello_ok);
      mongoc_topology_scanner_scan (scanner, sd->id);
   }

   return true;
}

/* Called from:
 * - the topology scanner callback (when a hello was just received)
 * - at the start of a single-threaded scan (mongoc_topology_scan_once)
 * Not called for multi threaded monitoring.
 */
void
mongoc_topology_reconcile (const mongoc_topology_t *topology, mongoc_topology_description_t *td)
{
   mongoc_set_t *servers;
   mongoc_server_description_t *sd;
   mongoc_topology_scanner_node_t *ele, *tmp;

   BSON_ASSERT (topology->single_threaded);
   servers = mc_tpld_servers (td);
   /* Add newly discovered nodes */
   for (size_t i = 0u; i < servers->items_len; i++) {
      sd = mongoc_set_get_item (servers, i);
      _mongoc_topology_reconcile_add_nodes (sd, topology->scanner);
   }

   /* Remove removed nodes */
   DL_FOREACH_SAFE (topology->scanner->nodes, ele, tmp)
   {
      if (!mongoc_topology_description_server_by_id (td, ele->id, NULL)) {
         mongoc_topology_scanner_node_retire (ele);
      }
   }
}


/* call this while already holding the lock */
static bool
_mongoc_topology_update_no_lock (uint32_t id,
                                 const bson_t *hello_response,
                                 int64_t rtt_msec,
                                 mongoc_topology_description_t *td,
                                 const bson_error_t *error /* IN */)
{
   mongoc_topology_description_handle_hello (td, id, hello_response, rtt_msec, error);

   /* return false if server removed from topology */
   return mongoc_topology_description_server_by_id (td, id, NULL) != NULL;
}


/*
 *-------------------------------------------------------------------------
 *
 * _mongoc_topology_scanner_setup_err_cb --
 *
 *       Callback method to handle errors during topology scanner node
 *       setup, typically DNS or SSL errors.
 *
 *-------------------------------------------------------------------------
 */

void
_mongoc_topology_scanner_setup_err_cb (uint32_t id, void *data, const bson_error_t *error /* IN */)
{
   mongoc_topology_t *topology = BSON_ASSERT_PTR_INLINE (data);

   BSON_ASSERT (topology->single_threaded);
   if (_mongoc_topology_get_type (topology) == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* In load balanced mode, scanning is only for connection establishment.
       * It must not modify the topology description. */
   } else {
      // Use `mc_tpld_unsafe_get_mutable` to get a mutable topology description
      // without locking. This function only applies to single-threaded clients.
      mongoc_topology_description_t *td = mc_tpld_unsafe_get_mutable (topology);
      mongoc_topology_description_handle_hello (td, id, NULL /* hello reply */, -1 /* rtt_msec */, error);
   }
}


/*
 *-------------------------------------------------------------------------
 *
 * _mongoc_topology_scanner_cb --
 *
 *       Callback method to handle hello responses received by async
 *       command objects.
 *
 *       Only called for single-threaded monitoring.
 *
 *-------------------------------------------------------------------------
 */

void
_mongoc_topology_scanner_cb (
   uint32_t id, const bson_t *hello_response, int64_t rtt_msec, void *data, const bson_error_t *error /* IN */)
{
   mongoc_topology_t *const topology = BSON_ASSERT_PTR_INLINE (data);
   mongoc_server_description_t *sd;
   mongoc_topology_description_t *td;

   BSON_ASSERT (topology->single_threaded);
   if (_mongoc_topology_get_type (topology) == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* In load balanced mode, scanning is only for connection establishment.
       * It must not modify the topology description. */
      return;
   }

   // Use `mc_tpld_unsafe_get_mutable` to get a mutable topology description
   // without locking. This function only applies to single-threaded clients.
   td = mc_tpld_unsafe_get_mutable (topology);

   sd = mongoc_topology_description_server_by_id (td, id, NULL);

   if (!hello_response) {
      /* Server monitoring: When a server check fails due to a network error
       * (including a network timeout), the client MUST clear its connection
       * pool for the server */
      _mongoc_topology_description_clear_connection_pool (td, id, &kZeroServiceId);
   }

   /* Server Discovery and Monitoring Spec: "Once a server is connected, the
    * client MUST change its type to Unknown only after it has retried the
    * server once." */
   if (!hello_response && sd && sd->type != MONGOC_SERVER_UNKNOWN) {
      _mongoc_topology_update_no_lock (id, hello_response, rtt_msec, td, error);

      /* add another hello call to the current scan - the scan continues
       * until all commands are done */
      mongoc_topology_scanner_scan (topology->scanner, sd->id);
   } else {
      _mongoc_topology_update_no_lock (id, hello_response, rtt_msec, td, error);

      /* The processing of the hello results above may have added, changed, or
       * removed server descriptions. We need to reconcile that with our
       * monitoring agents
       */
      mongoc_topology_reconcile (topology, td);
   }
}

static void
_server_session_init (mongoc_server_session_t *session, mongoc_topology_t *unused, bson_error_t *error)
{
   BSON_UNUSED (unused);

   _mongoc_server_session_init (session, error);
}

static void
_server_session_destroy (mongoc_server_session_t *session, mongoc_topology_t *unused)
{
   BSON_UNUSED (unused);

   _mongoc_server_session_destroy (session);
}

static int
_server_session_should_prune (mongoc_server_session_t *session, mongoc_topology_t *topo)
{
   BSON_ASSERT_PARAM (session);
   BSON_ASSERT_PARAM (topo);

   /** If "dirty" (i.e. contains a network error), it should be dropped */
   if (session->dirty) {
      return true;
   }

   /** If the session has never been used, it should be dropped */
   if (session->last_used_usec == SESSION_NEVER_USED) {
      return true;
   }

   /* Check for a timeout */
   mc_shared_tpld td = mc_tpld_take_ref (topo);
   const int64_t timeout = td.ptr->session_timeout_minutes;
   const bool is_loadbalanced = td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED;
   mc_tpld_drop_ref (&td);

   /** Load balanced topology sessions never expire */
   if (is_loadbalanced) {
      return false;
   }

   /* Prune the session if it has hit a timeout */
   return _mongoc_server_session_timed_out (session, timeout);
}

static void
_tpld_destroy_and_free (void *tpl_descr)
{
   mongoc_topology_description_t *td = tpl_descr;
   mongoc_topology_description_destroy (td);
}

const mongoc_host_list_t **
_mongoc_apply_srv_max_hosts (const mongoc_host_list_t *hl, size_t max_hosts, size_t *hl_array_size)
{
   const mongoc_host_list_t **hl_array;

   BSON_ASSERT_PARAM (hl_array_size);

   const size_t hl_size = _mongoc_host_list_length (hl);

   if (hl_size == 0u) {
      *hl_array_size = 0u;
      return NULL;
   }

   hl_array = bson_malloc (hl_size * sizeof (mongoc_host_list_t *));

   for (size_t idx = 0u; hl; hl = hl->next) {
      hl_array[idx++] = hl;
   }

   if (max_hosts == 0u ||   // Unlimited.
       hl_size == 1u ||     // Trivial case.
       hl_size <= max_hosts // Already satisfies limit.
   ) {
      /* No random shuffle or selection required. */
      *hl_array_size = hl_size;
      return hl_array;
   }

   /* Initial DNS Seedlist Discovery Spec: If `srvMaxHosts` is greater than zero
    * and less than the number of hosts in the DNS result, the driver MUST
    * randomly select that many hosts and use them to populate the seedlist.
    * Drivers SHOULD use the `Fisher-Yates shuffle` for randomization. */
   for (size_t idx = hl_size - 1u; idx > 0u; --idx) {
      /* 0 <= swap_pos <= idx */
      const size_t swap_pos = _mongoc_rand_size_t (0u, idx, _mongoc_simple_rand_size_t);

      const mongoc_host_list_t *tmp = hl_array[swap_pos];
      hl_array[swap_pos] = hl_array[idx];
      hl_array[idx] = tmp;
   }

   *hl_array_size = max_hosts;

   return hl_array;
}

// `_detect_nongenuine_host` logs if the host string suggests use of CosmosDB or
// DocumentDB. See DRIVERS-2583 for behavior requirements. Returns true if a
// CosmosDB or DocumentDB host is detected.
static bool
_detect_nongenuine_host (const char *host)
{
   char *const host_lowercase = bson_strdup (host);

   mongoc_lowercase (host, host_lowercase);

   if (mongoc_ends_with (host_lowercase, ".cosmos.azure.com")) {
      MONGOC_INFO ("You appear to be connected to a CosmosDB cluster. For more "
                   "information regarding feature compatibility and support please "
                   "visit https://www.mongodb.com/supportability/cosmosdb");
      bson_free (host_lowercase);
      return true;
   }

   if (mongoc_ends_with (host_lowercase, ".docdb.amazonaws.com") ||
       mongoc_ends_with (host_lowercase, ".docdb-elastic.amazonaws.com")) {
      MONGOC_INFO ("You appear to be connected to a DocumentDB cluster. For more "
                   "information regarding feature compatibility and support please "
                   "visit https://www.mongodb.com/supportability/documentdb");
      bson_free (host_lowercase);
      return true;
   }

   bson_free (host_lowercase);
   return false;
}

static void
_detect_nongenuine_hosts (const mongoc_uri_t *uri)
{
   const char *srv_hostname = mongoc_uri_get_srv_hostname (uri);
   if (srv_hostname) {
      _detect_nongenuine_host (srv_hostname);
      return;
   }

   const mongoc_host_list_t *iter;

   LL_FOREACH (mongoc_uri_get_hosts (uri), iter)
   {
      if (_detect_nongenuine_host (iter->host)) {
         return;
      }
   }
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_new --
 *
 *       Creates and returns a new topology object.
 *
 * Returns:
 *       A new topology object.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */
mongoc_topology_t *
mongoc_topology_new (const mongoc_uri_t *uri, bool single_threaded)
{
   mongoc_topology_t *topology;
   mongoc_topology_description_type_t init_type;
   mongoc_topology_description_t *td;
   const char *srv_hostname;
   const mongoc_host_list_t *hl;
   mongoc_rr_data_t rr_data;
   bool has_directconnection;
   bool directconnection;

   BSON_ASSERT (uri);

   _detect_nongenuine_hosts (uri);

#ifndef MONGOC_ENABLE_CRYPTO
   if (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_RETRYWRITES, MONGOC_DEFAULT_RETRYWRITES)) {
      /* retryWrites requires sessions, which require crypto - just warn */
      MONGOC_WARNING ("retryWrites not supported without an SSL crypto library");
   }
#endif

   topology = (mongoc_topology_t *) bson_malloc0 (sizeof *topology);
   // Check if requested to use TCP for SRV lookup.
   {
      char *srv_prefer_tcp = _mongoc_getenv ("MONGOC_EXPERIMENTAL_SRV_PREFER_TCP");
      if (srv_prefer_tcp) {
         topology->srv_prefer_tcp = true;
      }
      bson_free (srv_prefer_tcp);
   }
   topology->usleep_fn = mongoc_usleep_default_impl;
   topology->session_pool = mongoc_server_session_pool_new_with_params (
      _server_session_init, _server_session_destroy, _server_session_should_prune, topology);

   topology->valid = false;

   const int32_t heartbeat_default = single_threaded ? MONGOC_TOPOLOGY_HEARTBEAT_FREQUENCY_MS_SINGLE_THREADED
                                                     : MONGOC_TOPOLOGY_HEARTBEAT_FREQUENCY_MS_MULTI_THREADED;

   const int32_t heartbeat = mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, heartbeat_default);

   topology->_shared_descr_._sptr_ =
      mongoc_shared_ptr_create (BSON_ALIGNED_ALLOC0 (mongoc_topology_description_t), _tpld_destroy_and_free);
   td = mc_tpld_unsafe_get_mutable (topology);
   mongoc_topology_description_init (td, heartbeat);

   td->set_name = bson_strdup (mongoc_uri_get_replica_set (uri));

   topology->uri = mongoc_uri_copy (uri);
   topology->cse_state = MONGOC_CSE_DISABLED;
   topology->single_threaded = single_threaded;
   if (single_threaded) {
      /* Server Selection Spec:
       *
       *   "Single-threaded drivers MUST provide a "serverSelectionTryOnce"
       *   mode, in which the driver scans the topology exactly once after
       *   server selection fails, then either selects a server or raises an
       *   error.
       *
       *   "The serverSelectionTryOnce option MUST be true by default."
       */
      topology->server_selection_try_once =
         mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, true);
   } else {
      topology->server_selection_try_once = false;
   }

   topology->server_selection_timeout_msec = mongoc_uri_get_option_as_int32 (
      topology->uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, MONGOC_TOPOLOGY_SERVER_SELECTION_TIMEOUT_MS);

   /* tests can override this */
   topology->min_heartbeat_frequency_msec = MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS;

   topology->local_threshold_msec = mongoc_uri_get_local_threshold_option (topology->uri);

   /* Total time allowed to check a server is connectTimeoutMS.
    * Server Discovery And Monitoring Spec:
    *
    *   "The socket used to check a server MUST use the same connectTimeoutMS as
    *   regular sockets. Multi-threaded clients SHOULD set monitoring sockets'
    *   socketTimeoutMS to the connectTimeoutMS."
    */
   topology->connect_timeout_msec =
      mongoc_uri_get_option_as_int32 (topology->uri, MONGOC_URI_CONNECTTIMEOUTMS, MONGOC_DEFAULT_CONNECTTIMEOUTMS);

   topology->scanner_state = MONGOC_TOPOLOGY_SCANNER_OFF;
   topology->scanner = mongoc_topology_scanner_new (topology->uri,
                                                    _mongoc_topology_scanner_setup_err_cb,
                                                    _mongoc_topology_scanner_cb,
                                                    topology,
                                                    topology->connect_timeout_msec);

   bson_mutex_init (&topology->tpld_modification_mtx);
   mongoc_cond_init (&topology->cond_client);

   if (single_threaded) {
      /* single threaded drivers attempt speculative authentication during a
       * topology scan */
      topology->scanner->speculative_authentication = true;

      /* single threaded clients negotiate sasl supported mechanisms during
       * a topology scan. */
      if (_mongoc_uri_requires_auth_negotiation (uri)) {
         topology->scanner->negotiate_sasl_supported_mechs = true;
      }
   }

   srv_hostname = mongoc_uri_get_srv_hostname (uri);
   if (srv_hostname) {
      char *prefixed_hostname;

      memset (&rr_data, 0, sizeof (mongoc_rr_data_t));
      /* Set the default resource record resolver */
      topology->rr_resolver = _mongoc_client_get_rr;

      /* Initialize the last scan time and interval. Even if the initial DNS
       * lookup fails, SRV polling will still start when background monitoring
       * starts. */
      topology->srv_polling_last_scan_ms = bson_get_monotonic_time () / 1000;
      _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, MONGOC_TOPOLOGY_MIN_RESCAN_SRV_INTERVAL_MS);

      /* a mongodb+srv URI. try SRV lookup, if no error then also try TXT */
      prefixed_hostname = bson_strdup_printf ("_%s._tcp.%s", mongoc_uri_get_srv_service_name (uri), srv_hostname);
      if (!topology->rr_resolver (prefixed_hostname,
                                  MONGOC_RR_SRV,
                                  &rr_data,
                                  MONGOC_RR_DEFAULT_BUFFER_SIZE,
                                  topology->srv_prefer_tcp,
                                  &topology->scanner->error)) {
         GOTO (srv_fail);
      }

      /* Failure to find TXT records will not return an error (since it is only
       * for options). But _mongoc_client_get_rr may return an error if
       * there is more than one TXT record returned. */
      if (!topology->rr_resolver (srv_hostname,
                                  MONGOC_RR_TXT,
                                  &rr_data,
                                  MONGOC_RR_DEFAULT_BUFFER_SIZE,
                                  topology->srv_prefer_tcp,
                                  &topology->scanner->error)) {
         GOTO (srv_fail);
      }

      /* Use rr_data to update the topology's URI. */
      if (rr_data.txt_record_opts &&
          !mongoc_uri_parse_options (
             topology->uri, rr_data.txt_record_opts, true /* from_dns */, &topology->scanner->error)) {
         GOTO (srv_fail);
      }

      if (!mongoc_uri_init_with_srv_host_list (topology->uri, rr_data.hosts, &topology->scanner->error)) {
         GOTO (srv_fail);
      }

      topology->srv_polling_last_scan_ms = bson_get_monotonic_time () / 1000;
      /* TODO (CDRIVER-4047) use BSON_MIN */
      int64_t new_iv = BSON_MAX (rr_data.min_ttl * 1000, MONGOC_TOPOLOGY_MIN_RESCAN_SRV_INTERVAL_MS);
      _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, new_iv);

      topology->valid = true;
   srv_fail:
      bson_free (rr_data.txt_record_opts);
      bson_free (prefixed_hostname);
      _mongoc_host_list_destroy_all (rr_data.hosts);
   } else {
      topology->valid = true;
   }

   if (!mongoc_uri_finalize (topology->uri, &topology->scanner->error)) {
      topology->valid = false;
   }

   td->max_hosts = mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SRVMAXHOSTS, 0);

   if (td->max_hosts < 0) {
      topology->valid = false;
   }

   /*
    * Set topology type from URI:
    *   + if directConnection=true
    *     - whether or not we have a replicaSet name, initialize to SINGLE
    *     (directConnect with SRV or multiple hosts triggers a URI parse error)
    *   + if directConnection=false
    *     - if we've got a replicaSet name, initialize to RS_NO_PRIMARY
    *     - otherwise, initialize to UNKNOWN
    *   + if directConnection was not specified in the URI (old behavior)
    *     - if we've got a replicaSet name, initialize to RS_NO_PRIMARY
    *     - otherwise, if the seed list has a single host, initialize to SINGLE
    *   - everything else gets initialized to UNKNOWN
    */
   has_directconnection = mongoc_uri_has_option (uri, MONGOC_URI_DIRECTCONNECTION);
   directconnection = has_directconnection && mongoc_uri_get_option_as_bool (uri, MONGOC_URI_DIRECTCONNECTION, false);
   hl = mongoc_uri_get_hosts (topology->uri);
   /* If loadBalanced is enabled, directConnection is disabled. This was
    * validated in mongoc_uri_finalize_loadbalanced, which is called by
    * mongoc_uri_finalize. */
   if (mongoc_uri_get_option_as_bool (topology->uri, MONGOC_URI_LOADBALANCED, false)) {
      init_type = MONGOC_TOPOLOGY_LOAD_BALANCED;
      if (topology->single_threaded) {
         /* Cooldown only applies to server monitoring for single-threaded
          * clients. In load balanced mode, the topology scanner is used to
          * create connections. The cooldown period does not apply. A network
          * error to a load balanced connection does not imply subsequent
          * connection attempts will be to the same server and that a delay
          * should occur. */
         _mongoc_topology_bypass_cooldown (topology);
      }
      _mongoc_topology_scanner_set_loadbalanced (topology->scanner, true);
   } else if (srv_hostname && !has_directconnection) {
      init_type = MONGOC_TOPOLOGY_UNKNOWN;
   } else if (has_directconnection) {
      if (directconnection) {
         init_type = MONGOC_TOPOLOGY_SINGLE;
      } else {
         if (mongoc_uri_get_replica_set (topology->uri)) {
            init_type = MONGOC_TOPOLOGY_RS_NO_PRIMARY;
         } else {
            init_type = MONGOC_TOPOLOGY_UNKNOWN;
         }
      }
   } else if (mongoc_uri_get_replica_set (topology->uri)) {
      init_type = MONGOC_TOPOLOGY_RS_NO_PRIMARY;
   } else {
      if (hl && hl->next) {
         init_type = MONGOC_TOPOLOGY_UNKNOWN;
      } else {
         init_type = MONGOC_TOPOLOGY_SINGLE;
      }
   }

   td->type = init_type;

   if (!topology->single_threaded) {
      topology->server_monitors = mongoc_set_new (1, NULL, NULL);
      topology->rtt_monitors = mongoc_set_new (1, NULL, NULL);
      bson_mutex_init (&topology->apm_mutex);
      bson_mutex_init (&topology->srv_polling_mtx);
      mongoc_cond_init (&topology->srv_polling_cond);
   }

   if (!topology->valid) {
      TRACE ("%s", "topology invalid");
      /* add no nodes */
      return topology;
   }

   size_t hl_array_size = 0u;

   BSON_ASSERT (bson_in_range_signed (size_t, td->max_hosts));
   const mongoc_host_list_t *const *hl_array = _mongoc_apply_srv_max_hosts (hl, (size_t) td->max_hosts, &hl_array_size);

   for (size_t idx = 0u; idx < hl_array_size; ++idx) {
      const mongoc_host_list_t *const elem = hl_array[idx];

      uint32_t id = 0u;

      mongoc_topology_description_add_server (td, elem->host_and_port, &id);
      mongoc_topology_scanner_add (topology->scanner, elem, id, false);
   }

   bson_free ((void *) hl_array);

   return topology;
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_set_apm_callbacks --
 *
 *       Set Application Performance Monitoring callbacks.
 *
 *-------------------------------------------------------------------------
 */
void
mongoc_topology_set_apm_callbacks (mongoc_topology_t *topology,
                                   mongoc_topology_description_t *td,
                                   mongoc_apm_callbacks_t const *callbacks,
                                   void *context)
{
   if (callbacks) {
      td->apm_callbacks = *callbacks;
      topology->scanner->apm_callbacks = *callbacks;
   } else {
      td->apm_callbacks = (mongoc_apm_callbacks_t){0};
      topology->scanner->apm_callbacks = (mongoc_apm_callbacks_t){0};
   }

   td->apm_context = context;
   topology->scanner->apm_context = context;
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_destroy --
 *
 *       Free the memory associated with this topology object.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       @topology will be cleaned up.
 *
 *-------------------------------------------------------------------------
 */
void
mongoc_topology_destroy (mongoc_topology_t *topology)
{
   if (!topology) {
      return;
   }

#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION
   bson_free (topology->keyvault_db);
   bson_free (topology->keyvault_coll);
   mongoc_client_destroy (topology->mongocryptd_client);
   mongoc_client_pool_destroy (topology->mongocryptd_client_pool);
   _mongoc_crypt_destroy (topology->crypt);
   bson_destroy (topology->mongocryptd_spawn_args);
   bson_free (topology->mongocryptd_spawn_path);
#endif

   if (!topology->single_threaded) {
      _mongoc_topology_background_monitoring_stop (topology);
      BSON_ASSERT (topology->scanner_state == MONGOC_TOPOLOGY_SCANNER_OFF);
      mongoc_set_destroy (topology->server_monitors);
      mongoc_set_destroy (topology->rtt_monitors);
      bson_mutex_destroy (&topology->apm_mutex);
      bson_mutex_destroy (&topology->srv_polling_mtx);
      mongoc_cond_destroy (&topology->srv_polling_cond);
   }

   if (topology->valid) {
      /* Do not emit a topology_closed event. A topology opening event was not
       * emitted. */
      _mongoc_topology_description_monitor_closed (mc_tpld_unsafe_get_const (topology));
   }

   mongoc_uri_destroy (topology->uri);
   mongoc_shared_ptr_reset_null (&topology->_shared_descr_._sptr_);
   mongoc_topology_scanner_destroy (topology->scanner);
   mongoc_server_session_pool_free (topology->session_pool);
   bson_free (topology->clientSideEncryption.autoOptions.extraOptions.cryptSharedLibPath);

   mongoc_cond_destroy (&topology->cond_client);
   bson_mutex_destroy (&topology->tpld_modification_mtx);

   bson_destroy (topology->encrypted_fields_map);

   bson_free (topology);
}

/* Returns false if none of the hosts were valid. */
bool
mongoc_topology_apply_scanned_srv_hosts (mongoc_uri_t *uri,
                                         mongoc_topology_description_t *td,
                                         mongoc_host_list_t *hosts,
                                         bson_error_t *error)
{
   mongoc_host_list_t *host;
   mongoc_host_list_t *valid_hosts = NULL;
   bool had_valid_hosts = false;

   /* Validate that the hosts have a matching domain.
    * If validation fails, log it.
    * If no valid hosts remain, do not update the topology description.
    */
   LL_FOREACH (hosts, host)
   {
      if (mongoc_uri_validate_srv_result (uri, host->host, error)) {
         _mongoc_host_list_upsert (&valid_hosts, host);
      } else {
         MONGOC_ERROR ("Invalid host returned by SRV: %s", host->host_and_port);
         /* Continue on, there may still be valid hosts returned. */
      }
   }

   if (valid_hosts) {
      /* Reconcile with the topology description. Newly found servers will start
       * getting monitored and are eligible to be used by clients. */
      mongoc_topology_description_reconcile (td, valid_hosts);
      had_valid_hosts = true;
   } else {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_NAME_RESOLUTION,
                      "SRV response did not contain any valid hosts");
   }

   _mongoc_host_list_destroy_all (valid_hosts);
   return had_valid_hosts;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_should_rescan_srv --
 *
 *      Checks whether it is valid to rescan SRV records on the topology.
 *      Namely, that the topology type is Sharded or Unknown, and that
 *      the topology URI was configured with SRV.
 *
 *      If this returns false, caller can stop scanning SRV records
 *      and does not need to try again in the future.
 *
 * --------------------------------------------------------------------------
 */
bool
mongoc_topology_should_rescan_srv (mongoc_topology_t *topology)
{
   const char *srv_hostname = mongoc_uri_get_srv_hostname (topology->uri);
   mongoc_topology_description_type_t type;

   if (!srv_hostname) {
      /* Only rescan if we have a mongodb+srv:// URI. */
      return false;
   }

   type = _mongoc_topology_get_type (topology);

   /* Only perform rescan for sharded topology. */
   return type == MONGOC_TOPOLOGY_SHARDED || type == MONGOC_TOPOLOGY_UNKNOWN;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_rescan_srv --
 *
 *      Queries SRV records for new hosts in a mongos cluster.
 *      Caller must call mongoc_topology_should_rescan_srv before calling
 *      to ensure preconditions are met.
 *
 *      NOTE: This method may update the topology description.
 *
 * --------------------------------------------------------------------------
 */
void
mongoc_topology_rescan_srv (mongoc_topology_t *topology)
{
   mongoc_rr_data_t rr_data = {0};
   const char *srv_hostname;
   char *prefixed_hostname = NULL;
   int64_t scan_time_ms;
   bool ret;
   mc_shared_tpld td;
   mc_tpld_modification tdmod;

   BSON_ASSERT (mongoc_topology_should_rescan_srv (topology));

   srv_hostname = mongoc_uri_get_srv_hostname (topology->uri);
   scan_time_ms = topology->srv_polling_last_scan_ms + _mongoc_topology_get_srv_polling_rescan_interval_ms (topology);
   if (bson_get_monotonic_time () / 1000 < scan_time_ms) {
      /* Query SRV no more frequently than srv_polling_rescan_interval_ms. */
      return;
   }

   TRACE ("%s", "Polling for SRV records");

   /* Go forth and query... */
   prefixed_hostname =
      bson_strdup_printf ("_%s._tcp.%s", mongoc_uri_get_srv_service_name (topology->uri), srv_hostname);

   ret = topology->rr_resolver (prefixed_hostname,
                                MONGOC_RR_SRV,
                                &rr_data,
                                MONGOC_RR_DEFAULT_BUFFER_SIZE,
                                topology->srv_prefer_tcp,
                                &topology->scanner->error);

   td = mc_tpld_take_ref (topology);
   topology->srv_polling_last_scan_ms = bson_get_monotonic_time () / 1000;
   if (!ret) {
      /* Failed querying, soldier on and try again next time. */
      _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, td.ptr->heartbeat_msec);
      MONGOC_ERROR ("SRV polling error: %s", topology->scanner->error.message);
      GOTO (done);
   }

   /* TODO (CDRIVER-4047) use BSON_MIN */
   const int64_t new_iv = BSON_MAX (rr_data.min_ttl * 1000, MONGOC_TOPOLOGY_MIN_RESCAN_SRV_INTERVAL_MS);
   _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, new_iv);

   tdmod = mc_tpld_modify_begin (topology);
   if (!mongoc_topology_apply_scanned_srv_hosts (
          topology->uri, tdmod.new_td, rr_data.hosts, &topology->scanner->error)) {
      MONGOC_ERROR ("%s", topology->scanner->error.message);
      /* Special case when DNS returns zero records successfully or no valid
       * hosts exist.
       * Leave the toplogy alone and perform another scan at the next interval
       * rather than removing all records and having nothing to connect to.
       * For no verified hosts drivers "MUST temporarily set
       * srv_polling_rescan_interval_ms
       * to heartbeatFrequencyMS until at least one verified SRV record is
       * obtained."
       */
      _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, td.ptr->heartbeat_msec);
   }
   mc_tpld_modify_commit (tdmod);

done:
   mc_tpld_drop_ref (&td);
   bson_free (prefixed_hostname);
   _mongoc_host_list_destroy_all (rr_data.hosts);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_scan_once --
 *
 *      Runs a single complete scan.
 *
 *      NOTE: This method updates the topology description.
 *
 *      Only runs for single threaded monitoring. (obey_cooldown is always
 *      true).
 *
 *--------------------------------------------------------------------------
 */
static void
mongoc_topology_scan_once (mongoc_topology_t *topology, bool obey_cooldown)
{
   mongoc_topology_description_t *td;
   BSON_ASSERT (topology->single_threaded);
   if (mongoc_topology_should_rescan_srv (topology)) {
      /* Prior to scanning hosts, update the list of SRV hosts, if applicable.
       */
      mongoc_topology_rescan_srv (topology);
   }

   /* since the last scan, members may be added or removed from the topology
    * description based on hello responses in connection handshakes, see
    * _mongoc_topology_update_from_handshake. retire scanner nodes for removed
    * members and create scanner nodes for new ones. */
   // Use `mc_tpld_unsafe_get_mutable` to get a mutable topology description
   // without locking. This function only applies to single-threaded clients.
   td = mc_tpld_unsafe_get_mutable (topology);
   mongoc_topology_reconcile (topology, td);

   mongoc_topology_scanner_start (topology->scanner, obey_cooldown);
   mongoc_topology_scanner_work (topology->scanner);

   _mongoc_topology_scanner_finish (topology->scanner);

   topology->last_scan = bson_get_monotonic_time ();
   topology->stale = false;
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_do_blocking_scan --
 *
 *       Monitoring entry for single-threaded use case. Assumes the caller
 *       has checked that it's the right time to scan.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_topology_do_blocking_scan (mongoc_topology_t *topology, bson_error_t *error)
{
   BSON_ASSERT (topology->single_threaded);
   _mongoc_handshake_freeze ();

   mongoc_topology_scan_once (topology, true /* obey cooldown */);
   mongoc_topology_scanner_get_error (topology->scanner, error);
}


bool
mongoc_topology_compatible (const mongoc_topology_description_t *td,
                            const mongoc_read_prefs_t *read_prefs,
                            bson_error_t *error)
{
   if (td->compatibility_error.code) {
      if (error) {
         memcpy (error, &td->compatibility_error, sizeof (bson_error_t));
      }
      return false;
   }

   if (!read_prefs) {
      /* NULL means read preference Primary */
      return true;
   }

   const int64_t max_staleness_seconds = mongoc_read_prefs_get_max_staleness_seconds (read_prefs);

   if (max_staleness_seconds != MONGOC_NO_MAX_STALENESS) {
      /* shouldn't happen if we've properly enforced wire version */
      if (!mongoc_topology_description_all_sds_have_write_date (td)) {
         bson_set_error (
            error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION, "Not all servers have lastWriteDate");
         return false;
      }

      if (!_mongoc_topology_description_validate_max_staleness (td, max_staleness_seconds, error)) {
         return false;
      }
   }

   return true;
}


static void
_mongoc_server_selection_error (const char *msg, const bson_error_t *scanner_error, bson_error_t *error)
{
   if (scanner_error && scanner_error->code) {
      bson_set_error (error,
                      MONGOC_ERROR_SERVER_SELECTION,
                      MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                      "%s: %s",
                      msg,
                      scanner_error->message);
   } else {
      bson_set_error (error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "%s", msg);
   }
}


mongoc_server_description_t *
mongoc_topology_select (mongoc_topology_t *topology,
                        mongoc_ss_optype_t optype,
                        const mongoc_read_prefs_t *read_prefs,
                        bool *must_use_primary,
                        bson_error_t *error)
{
   uint32_t server_id = mongoc_topology_select_server_id (topology, optype, read_prefs, must_use_primary, NULL, error);

   if (server_id) {
      /* new copy of the server description */
      mongoc_server_description_t *ret;
      mc_shared_tpld td = mc_tpld_take_ref (topology);
      mongoc_server_description_t const *sd = mongoc_topology_description_server_by_id_const (td.ptr, server_id, error);
      ret = mongoc_server_description_new_copy (sd);
      mc_tpld_drop_ref (&td);
      return ret;
   } else {
      return NULL;
   }
}

/* Bypasses normal server selection behavior for a load balanced topology.
 * Returns the id of the one load balancer server. Returns 0 on failure.
 * Successful post-condition: On a single threaded client, a connection will
 * have been established. */
static uint32_t
_mongoc_topology_select_server_id_loadbalanced (mongoc_topology_t *topology, bson_error_t *error)
{
   mongoc_server_description_t const *selected_server;
   int32_t selected_server_id;
   mongoc_topology_scanner_node_t *node;
   bson_error_t scanner_error = {0};
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   BSON_ASSERT (td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED);

   /* Emit the opening SDAM events if they have not emitted already. */
   if (!td.ptr->opened) {
      mc_tpld_modification tdmod = mc_tpld_modify_begin (topology);
      _mongoc_topology_description_monitor_opening (tdmod.new_td);
      mc_tpld_modify_commit (tdmod);
      mc_tpld_renew_ref (&td, topology);
   }
   selected_server = mongoc_topology_description_select (td.ptr,
                                                         MONGOC_SS_WRITE,
                                                         NULL /* read prefs */,
                                                         NULL /* chosen read mode */,
                                                         NULL /* deprioritized servers */,
                                                         0 /* local threshold */);

   if (!selected_server) {
      _mongoc_server_selection_error ("No suitable server found in load balanced deployment", NULL, error);
      selected_server_id = 0;
      goto done;
   }

   selected_server_id = selected_server->id;

   if (!topology->single_threaded) {
      goto done;
   }

   /* If this is a single threaded topology, we must ensure that a connection is
    * available to this server. Wrapping drivers make the assumption that
    * successful server selection implies a connection is available. */
   node = mongoc_topology_scanner_get_node (topology->scanner, selected_server_id);
   if (!node) {
      _mongoc_server_selection_error ("Topology scanner in invalid state; cannot find load balancer", NULL, error);
      selected_server_id = 0;
      goto done;
   }

   if (!node->stream) {
      TRACE ("%s",
             "Server selection performing scan since no connection has "
             "been established");
      _mongoc_topology_do_blocking_scan (topology, &scanner_error);
   }

   if (!node->stream) {
      /* Use the same error domain / code that is returned in mongoc-cluster.c
       * when fetching a stream fails. */
      if (scanner_error.code) {
         bson_set_error (error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_NOT_ESTABLISHED,
                         "Could not establish stream for node %s: %s",
                         node->host.host_and_port,
                         scanner_error.message);
      } else {
         bson_set_error (error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_NOT_ESTABLISHED,
                         "Could not establish stream for node %s",
                         node->host.host_and_port);
      }
      selected_server_id = 0;
      goto done;
   }

done:
   mc_tpld_drop_ref (&td);
   return selected_server_id;
}


uint32_t
mongoc_topology_select_server_id (mongoc_topology_t *topology,
                                  mongoc_ss_optype_t optype,
                                  const mongoc_read_prefs_t *read_prefs,
                                  bool *must_use_primary,
                                  const mongoc_deprioritized_servers_t *ds,
                                  bson_error_t *error)
{
   static const char *timeout_msg = "No suitable servers found: `serverSelectionTimeoutMS` expired";

   mongoc_topology_scanner_t *ts;
   int r;
   int64_t local_threshold_ms;
   const mongoc_server_description_t *selected_server = NULL;
   bool try_once;
   int64_t sleep_usec;
   bool tried_once;
   bson_error_t scanner_error = {0};
   int64_t heartbeat_msec;
   uint32_t server_id;
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   /* These names come from the Server Selection Spec pseudocode */
   int64_t loop_start;  /* when we entered this function */
   int64_t loop_end;    /* when we last completed a loop (single-threaded) */
   int64_t scan_ready;  /* the soonest we can do a blocking scan */
   int64_t next_update; /* the latest we must do a blocking scan */
   int64_t expire_at;   /* when server selection timeout expires */

   BSON_ASSERT (topology);
   ts = topology->scanner;

   if (!mongoc_topology_scanner_valid (ts)) {
      if (error) {
         mongoc_topology_scanner_get_error (ts, error);
         error->domain = MONGOC_ERROR_SERVER_SELECTION;
         error->code = MONGOC_ERROR_SERVER_SELECTION_FAILURE;
      }
      server_id = 0;
      goto done;
   }

   if (td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      server_id = _mongoc_topology_select_server_id_loadbalanced (topology, error);
      goto done;
   }

   heartbeat_msec = td.ptr->heartbeat_msec;
   local_threshold_ms = topology->local_threshold_msec;
   try_once = topology->server_selection_try_once;
   loop_start = loop_end = bson_get_monotonic_time ();
   expire_at = loop_start + ((int64_t) topology->server_selection_timeout_msec * 1000);

   if (topology->single_threaded) {
      if (!td.ptr->opened) {
         // Use `mc_tpld_unsafe_get_mutable` to get a mutable topology
         // description without locking. This block only applies to
         // single-threaded clients.
         _mongoc_topology_description_monitor_opening (mc_tpld_unsafe_get_mutable (topology));
         mc_tpld_renew_ref (&td, topology);
      }

      tried_once = false;
      next_update = topology->last_scan + heartbeat_msec * 1000;
      if (next_update < loop_start) {
         /* we must scan now */
         topology->stale = true;
      }

      /* until we find a server or time out */
      for (;;) {
         if (topology->stale) {
            /* how soon are we allowed to scan? */
            scan_ready = topology->last_scan + topology->min_heartbeat_frequency_msec * 1000;

            if (scan_ready > expire_at && !try_once) {
               /* selection timeout will expire before min heartbeat passes */
               _mongoc_server_selection_error ("No suitable servers found: "
                                               "`serverselectiontimeoutms` timed out",
                                               &scanner_error,
                                               error);

               server_id = 0;
               goto done;
            }

            sleep_usec = scan_ready - loop_end;
            if (sleep_usec > 0) {
               if (try_once && mongoc_topology_scanner_in_cooldown (ts, scan_ready)) {
                  _mongoc_server_selection_error ("No servers yet eligible for rescan", &scanner_error, error);

                  server_id = 0;
                  goto done;
               }
               topology->usleep_fn (sleep_usec, topology->usleep_data);
            }

            /* takes up to connectTimeoutMS. sets "last_scan", clears "stale" */
            _mongoc_topology_do_blocking_scan (topology, &scanner_error);
            loop_end = topology->last_scan;
            tried_once = true;
         }

         /* Topology may have just been updated by a scan. */
         mc_tpld_renew_ref (&td, topology);

         if (!mongoc_topology_compatible (td.ptr, read_prefs, error)) {
            server_id = 0;
            goto done;
         }

         selected_server =
            mongoc_topology_description_select (td.ptr, optype, read_prefs, must_use_primary, ds, local_threshold_ms);

         if (selected_server) {
            server_id = selected_server->id;
            goto done;
         }

         topology->stale = true;

         if (try_once) {
            if (tried_once) {
               _mongoc_server_selection_error (
                  "No suitable servers found (`serverSelectionTryOnce` set)", &scanner_error, error);

               server_id = 0;
               goto done;
            }
         } else {
            loop_end = bson_get_monotonic_time ();

            if (loop_end > expire_at) {
               /* no time left in server_selection_timeout_msec */
               _mongoc_server_selection_error (timeout_msg, &scanner_error, error);

               server_id = 0;
               goto done;
            }
         }
      }
   }

   /* With background thread */
   /* we break out when we've found a server or timed out */
   for (;;) {
      /* Topology may have been updated on a previous loop iteration */
      mc_tpld_renew_ref (&td, topology);

      if (!mongoc_topology_compatible (td.ptr, read_prefs, error)) {
         server_id = 0;
         goto done;
      }

      selected_server =
         mongoc_topology_description_select (td.ptr, optype, read_prefs, must_use_primary, ds, local_threshold_ms);

      if (selected_server) {
         server_id = selected_server->id;
         goto done;
      }

      /* tlpd_modification_mtx is used to synchronize updates to the topology.
       * Take that lock to do a wait on the topology to become up-to-date and
       * synchronize with a condition variable that will be signalled upon
       * topology changes. */
      bson_mutex_lock (&topology->tpld_modification_mtx);
      /* Now that we have the lock, check again, since a scan may have
       * occurred while we were waiting on the lock. */
      mc_tpld_renew_ref (&td, topology);
      selected_server =
         mongoc_topology_description_select (td.ptr, optype, read_prefs, must_use_primary, ds, local_threshold_ms);
      if (selected_server) {
         server_id = selected_server->id;
         bson_mutex_unlock (&topology->tpld_modification_mtx);
         goto done;
      }

      /* Still nothing. Request that the scanner do a scan now. */
      TRACE ("server selection requesting an immediate scan, want %s",
             _mongoc_read_mode_as_str (mongoc_read_prefs_get_mode (read_prefs)));
      _mongoc_topology_request_scan (topology);

      TRACE ("server selection about to wait for %" PRId64 "ms", (expire_at - loop_start) / 1000);
      r = mongoc_cond_timedwait (
         &topology->cond_client, &topology->tpld_modification_mtx, (expire_at - loop_start) / 1000);
      TRACE ("%s", "server selection awake");
      /* Refresh our topology handle */
      mc_tpld_renew_ref (&td, topology);
      _topology_collect_errors (td.ptr, &scanner_error);

      bson_mutex_unlock (&topology->tpld_modification_mtx);

#ifdef _WIN32
      if (r == WSAETIMEDOUT) {
#else
      if (r == ETIMEDOUT) {
#endif
         /* handle timeouts */
         _mongoc_server_selection_error (timeout_msg, &scanner_error, error);

         server_id = 0;
         goto done;
      } else if (r) {
         bson_set_error (error,
                         MONGOC_ERROR_SERVER_SELECTION,
                         MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                         "Unknown error '%d' received while waiting on "
                         "thread condition",
                         r);
         server_id = 0;
         goto done;
      }

      loop_start = bson_get_monotonic_time ();

      if (loop_start > expire_at) {
         _mongoc_server_selection_error (timeout_msg, &scanner_error, error);

         server_id = 0;
         goto done;
      }
   }

done:
   mc_tpld_drop_ref (&td);
   return server_id;
}


mongoc_host_list_t *
_mongoc_topology_host_by_id (const mongoc_topology_description_t *td, uint32_t id, bson_error_t *error)
{
   mongoc_server_description_t const *sd;
   mongoc_host_list_t *host = NULL;

   /* not a copy - direct pointer into topology description data */
   sd = mongoc_topology_description_server_by_id_const (td, id, error);

   if (sd) {
      host = bson_malloc0 (sizeof (mongoc_host_list_t));
      memcpy (host, &sd->host, sizeof (mongoc_host_list_t));
   }

   return host;
}


void
_mongoc_topology_request_scan (mongoc_topology_t *topology)
{
   _mongoc_topology_background_monitoring_request_scan (topology);
}

bool
_mongoc_topology_update_from_handshake (mongoc_topology_t *topology, const mongoc_server_description_t *sd)
{
   bool has_server;
   mc_tpld_modification tdmod;

   BSON_ASSERT (topology);
   BSON_ASSERT (sd);
   BSON_ASSERT (!topology->single_threaded);

   if (_mongoc_topology_get_type (topology) == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* In load balanced mode, scanning is only for connection establishment.
       * It must not modify the topology description. */
      return true;
   }

   tdmod = mc_tpld_modify_begin (topology);

   /* return false if server was removed from topology */
   has_server =
      _mongoc_topology_update_no_lock (sd->id, &sd->last_hello_response, sd->round_trip_time_msec, tdmod.new_td, NULL);

   /* if pooled, wake threads waiting in mongoc_topology_server_by_id */
   mongoc_cond_broadcast (&topology->cond_client);
   /* Update background monitoring. */
   _mongoc_topology_background_monitoring_reconcile (topology, tdmod.new_td);
   mc_tpld_modify_commit (tdmod);

   return has_server;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_update_last_used --
 *
 *       Internal function. In single-threaded mode only, track when the socket
 *       to a particular server was last used. This is required for
 *       mongoc_cluster_check_interval to know when a socket has been idle.
 *
 *--------------------------------------------------------------------------
 */

void
_mongoc_topology_update_last_used (mongoc_topology_t *topology, uint32_t server_id)
{
   mongoc_topology_scanner_node_t *node;

   if (!topology->single_threaded) {
      return;
   }

   node = mongoc_topology_scanner_get_node (topology->scanner, server_id);
   if (node) {
      node->last_used = bson_get_monotonic_time ();
   }
}


mongoc_topology_description_type_t
_mongoc_topology_get_type (const mongoc_topology_t *topology)
{
   mc_shared_tpld td = mc_tpld_take_ref (topology);
   mongoc_topology_description_type_t td_type = td.ptr->type;
   mc_tpld_drop_ref (&td);
   return td_type;
}

bool
_mongoc_topology_set_appname (mongoc_topology_t *topology, const char *appname)
{
   bool ret = false;

   if (topology->scanner_state == MONGOC_TOPOLOGY_SCANNER_OFF) {
      ret = _mongoc_topology_scanner_set_appname (topology->scanner, appname);
   } else {
      MONGOC_ERROR ("Cannot set appname after handshake initiated");
   }
   return ret;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_update_cluster_time --
 *
 *       Internal function. If the server reply has a later $clusterTime than
 *       any seen before, update the topology's clusterTime. See the Driver
 *       Sessions Spec.
 *
 *--------------------------------------------------------------------------
 */

void
_mongoc_topology_update_cluster_time (mongoc_topology_t *topology, const bson_t *reply)
{
   bson_iter_t iter;
   bson_iter_t child;
   const uint8_t *data;
   uint32_t size;
   bson_t cluster_time;
   mc_shared_tpld td;

   if (!reply || !bson_iter_init_find (&iter, reply, "$clusterTime")) {
      return;
   }

   if (!BSON_ITER_HOLDS_DOCUMENT (&iter) || !bson_iter_recurse (&iter, &child)) {
      MONGOC_ERROR ("Can't parse $clusterTime");
      return;
   }

   bson_iter_document (&iter, &size, &data);
   BSON_ASSERT (bson_init_static (&cluster_time, data, (size_t) size));

   td = mc_tpld_take_ref (topology);

   /* This func is called frequently and repeatedly, but the cluster time itself
    * is infrequently updated. mc_tpld_modify_begin() is very expensive, so we
    * only want to actually call it if we anticipate performing an update to the
    * cluster time.
    *
    * Check that the cluster time has actually changed from what we have on
    * record before opening a topology modification to update it. */
   if (bson_empty (&td.ptr->cluster_time) || _mongoc_cluster_time_greater (&cluster_time, &td.ptr->cluster_time)) {
      mc_tpld_modification tdmod = mc_tpld_modify_begin (topology);
      /* Check again if we need to update the cluster time, since it may have
       * been updated behind our back. */
      if (bson_empty (&tdmod.new_td->cluster_time) ||
          _mongoc_cluster_time_greater (&cluster_time, &tdmod.new_td->cluster_time)) {
         bson_destroy (&tdmod.new_td->cluster_time);
         bson_copy_to (&cluster_time, &tdmod.new_td->cluster_time);
         _mongoc_topology_scanner_set_cluster_time (topology->scanner, &tdmod.new_td->cluster_time);
         mc_tpld_modify_commit (tdmod);
      } else {
         mc_tpld_modify_drop (tdmod);
      }
   }
   mc_tpld_drop_ref (&td);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_pop_server_session --
 *
 *       Internal function. Get a server session from the pool or create
 *       one. On error, return NULL and fill out @error.
 *
 *--------------------------------------------------------------------------
 */

mongoc_server_session_t *
_mongoc_topology_pop_server_session (mongoc_topology_t *topology, bson_error_t *error)
{
   int64_t timeout;
   mongoc_server_session_t *ss = NULL;
   bool loadbalanced;
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   ENTRY;

   timeout = td.ptr->session_timeout_minutes;
   loadbalanced = td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED;

   /* When the topology type is LoadBalanced, sessions are always supported. */
   if (!loadbalanced && timeout == MONGOC_NO_SESSIONS) {
      /* if needed, connect and check for session timeout again */
      if (!mongoc_topology_description_has_data_node (td.ptr)) {
         if (!mongoc_topology_select_server_id (topology,
                                                MONGOC_SS_READ,
                                                NULL /* read prefs */,
                                                NULL /* chosen read mode */,
                                                NULL /* deprioritized servers */,
                                                error)) {
            ss = NULL;
            goto done;
         }

         /* Topology may have been updated by a scan */
         mc_tpld_renew_ref (&td, topology);

         timeout = td.ptr->session_timeout_minutes;
      }

      if (timeout == MONGOC_NO_SESSIONS) {
         bson_set_error (
            error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_SESSION_FAILURE, "Server does not support sessions");
         ss = NULL;
         goto done;
      }
   }

   ss = mongoc_server_session_pool_get (topology->session_pool, error);

done:
   mc_tpld_drop_ref (&td);
   RETURN (ss);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_push_server_session --
 *
 *       Internal function. Return a server session to the pool.
 *
 *--------------------------------------------------------------------------
 */

void
_mongoc_topology_push_server_session (mongoc_topology_t *topology, mongoc_server_session_t *server_session)
{
   ENTRY;

   /**
    * ! note:
    * At time of writing, this diverges from the spec:
    * https://github.com/mongodb/specifications/blob/df6be82f865e9b72444488fd62ae1eb5fca18569/source/sessions/driver-sessions.rst#algorithm-to-return-a-serversession-instance-to-the-server-session-pool
    *
    * The spec notes that before returning a session, we should first inspect
    * the back of the pool for expired items and delete them. In this case, we
    * simply return the item to the top of the pool and leave the remainder
    * unchanged.
    *
    * The next pop operation that encounters an expired session will clear the
    * entire session pool, thus preventing unbounded growth of the pool.
    */
   mongoc_server_session_pool_return (topology->session_pool, server_session);

   EXIT;
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_end_sessions_cmd --
 *
 *       Internal function. End up to 10,000 server sessions. @cmd is an
 *       uninitialized document. Sessions are destroyed as their ids are
 *       appended to @cmd.
 *
 *       Driver Sessions Spec: "If the number of sessions is very large the
 *       endSessions command SHOULD be run multiple times to end 10,000
 *       sessions at a time (in order to avoid creating excessively large
 *       commands)."
 *
 * Returns:
 *      true if any session ids were appended to @cmd.
 *
 *--------------------------------------------------------------------------
 */

bool
_mongoc_topology_end_sessions_cmd (mongoc_topology_t *topology, bson_t *cmd)
{
   bson_array_builder_t *ar;
   /* Only end up to 10'000 sessions */
   const int ENDED_SESSION_PRUNING_LIMIT = 10000;
   int i = 0;
   mongoc_server_session_t *ss = mongoc_server_session_pool_get_existing (topology->session_pool);

   bson_init (cmd);
   BSON_APPEND_ARRAY_BUILDER_BEGIN (cmd, "endSessions", &ar);

   for (; i < ENDED_SESSION_PRUNING_LIMIT && ss != NULL;
        ++i, ss = mongoc_server_session_pool_get_existing (topology->session_pool)) {
      bson_array_builder_append_document (ar, &ss->lsid);
      mongoc_server_session_pool_drop (topology->session_pool, ss);
   }

   if (ss) {
      /* We deleted at least 10'000 sessions, so we will need to return the
       * final session that we didn't drop */
      mongoc_server_session_pool_return (topology->session_pool, ss);
   }

   bson_append_array_builder_end (cmd, ar);

   return i > 0;
}

void
_mongoc_topology_dup_handshake_cmd (const mongoc_topology_t *topology, bson_t *copy_into)
{
   _mongoc_topology_scanner_dup_handshake_cmd (topology->scanner, copy_into);
}

void
_mongoc_topology_bypass_cooldown (mongoc_topology_t *topology)
{
   BSON_ASSERT (topology->single_threaded);
   topology->scanner->bypass_cooldown = true;
}

static void
_find_topology_version (const bson_t *reply, bson_t *topology_version)
{
   bson_iter_t iter;
   const uint8_t *bytes;
   uint32_t len;

   if (!bson_iter_init_find (&iter, reply, "topologyVersion") || !BSON_ITER_HOLDS_DOCUMENT (&iter)) {
      bson_init (topology_version);
      return;
   }
   bson_iter_document (&iter, &len, &bytes);
   BSON_ASSERT (bson_init_static (topology_version, bytes, len));
}


static bool
_handle_sdam_app_error_command (mongoc_topology_t *topology,
                                const mongoc_topology_description_t *td,
                                uint32_t server_id,
                                uint32_t generation,
                                const bson_oid_t *service_id,
                                const mongoc_server_description_t *sd,
                                uint32_t max_wire_version,
                                const bson_t *reply)
{
   bson_error_t cmd_error;
   bson_t incoming_topology_version;
   bool pool_cleared = false;
   bool should_clear_pool = false;
   mc_tpld_modification tdmod;
   mongoc_server_description_t *mut_sd;

   BSON_UNUSED (td);

   if (_mongoc_cmd_check_ok_no_wce (reply, MONGOC_ERROR_API_VERSION_2, &cmd_error)) {
      /* No error. */
      return false;
   }

   if (!_mongoc_error_is_state_change (&cmd_error)) {
      /* Not a "not primary" or "node is recovering" error. */
      return false;
   }

   /* Check if the error is "stale", i.e. the topologyVersion refers to an
    * older
    * version of the server than we have stored in the topology description.
    */
   _find_topology_version (reply, &incoming_topology_version);
   if (mongoc_server_description_topology_version_cmp (&sd->topology_version, &incoming_topology_version) >= 0) {
      /* The server description is greater or equal, ignore the error. */
      bson_destroy (&incoming_topology_version);
      return false;
   }

   should_clear_pool = (max_wire_version <= WIRE_VERSION_4_0 || _mongoc_error_is_shutdown (&cmd_error));

   tdmod = mc_tpld_modify_begin (topology);

   /* Get the server handle again, which might have been removed. */
   mut_sd = mongoc_topology_description_server_by_id (tdmod.new_td, server_id, NULL);

   if (!mut_sd) {
      /* Server was already removed/invalidated */
      mc_tpld_modify_drop (tdmod);
      bson_destroy (&incoming_topology_version);
      return false;
   }

   /* Check the topology version a second time, now that we have an exclusive
    * lock on the latest topology description. */
   if (mongoc_server_description_topology_version_cmp (&mut_sd->topology_version, &incoming_topology_version) >= 0) {
      /* The server description is greater or equal, ignore the error. */
      mc_tpld_modify_drop (tdmod);
      bson_destroy (&incoming_topology_version);
      return false;
   }

   if (generation < mc_tpl_sd_get_generation (mut_sd, service_id)) {
      /* Our view of the server description is stale. Ignore it. */
      mc_tpld_modify_drop (tdmod);
      bson_destroy (&incoming_topology_version);
      return false;
   }

   /* Overwrite the topology version. */
   mongoc_server_description_set_topology_version (mut_sd, &incoming_topology_version);

   /* SDAM: When handling a "not primary" or "node is recovering" error, the
    * client MUST clear the server's connection pool if and only if the error
    * is "node is shutting down" or the error originated from server version
    * < 4.2.
    */
   if (should_clear_pool) {
      _mongoc_topology_description_clear_connection_pool (tdmod.new_td, server_id, service_id);
      pool_cleared = true;
   }

   /*
    * SDAM: When the client sees a "not primary" or "node is recovering"
    * error and the error's topologyVersion is strictly greater than the
    * current ServerDescription's topologyVersion it MUST replace the
    * server's description with a ServerDescription of type Unknown.
    */
   mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, &cmd_error);

   if (topology->single_threaded) {
      /* SDAM: For single-threaded clients, in the case of a "not primary" or
       * "node is shutting down" error, the client MUST mark the topology as
       * "stale"
       */
      if (_mongoc_error_is_not_primary (&cmd_error)) {
         topology->stale = true;
      }
   } else {
      /* SDAM Spec: "Multi-threaded and asynchronous clients MUST request an
       * immediate check of the server."
       * Instead of requesting a check of the one server, request a scan
       * to all servers (to find the new primary).
       */
      _mongoc_topology_request_scan (topology);
   }

   mc_tpld_modify_commit (tdmod);
   bson_destroy (&incoming_topology_version);

   return pool_cleared;
}


bool
_mongoc_topology_handle_app_error (mongoc_topology_t *topology,
                                   uint32_t server_id,
                                   bool handshake_complete,
                                   _mongoc_sdam_app_error_type_t type,
                                   const bson_t *reply,
                                   const bson_error_t *why,
                                   uint32_t max_wire_version,
                                   uint32_t generation,
                                   const bson_oid_t *service_id)
{
   bson_error_t server_selection_error;
   const mongoc_server_description_t *sd;
   bool cleared_pool = false;
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   /* Start by checking every condition in which we should ignore the error */
   sd = mongoc_topology_description_server_by_id_const (td.ptr, server_id, &server_selection_error);

   if (!sd) {
      /* The server was already removed from the topology. Ignore error. */
      goto ignore_error;
   }

   /* When establishing a new connection in load balanced mode, drivers MUST NOT
    * perform SDAM error handling for any errors that occur before the MongoDB
    * Handshake. */
   if (td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED && !handshake_complete) {
      goto ignore_error;
   }

   if (generation < mc_tpl_sd_get_generation (sd, service_id)) {
      /* This is a stale connection. Ignore. */
      goto ignore_error;
   }

   if (type == MONGOC_SDAM_APP_ERROR_TIMEOUT && handshake_complete) {
      /* Timeout errors after handshake are ok, do nothing. */
      goto ignore_error;
   }

   /* Do something with the error */
   if (type == MONGOC_SDAM_APP_ERROR_COMMAND) {
      cleared_pool = _handle_sdam_app_error_command (
         topology, td.ptr, server_id, generation, service_id, sd, max_wire_version, reply);
   } else {
      /* Invalidate the server that saw the error. */
      mc_tpld_modification tdmod = mc_tpld_modify_begin (topology);
      sd = mongoc_topology_description_server_by_id_const (tdmod.new_td, server_id, NULL);
      /* Check if the server has already been invalidated */
      if (!sd || generation < mc_tpl_sd_get_generation (sd, service_id)) {
         mc_tpld_modify_drop (tdmod);
         goto ignore_error;
      }
      /* Mark server as unknown. */
      mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, why);
      /* Clear the connection pool */
      _mongoc_topology_description_clear_connection_pool (tdmod.new_td, server_id, service_id);
      cleared_pool = true;
      if (!topology->single_threaded) {
         _mongoc_topology_background_monitoring_cancel_check (topology, server_id);
      }
      mc_tpld_modify_commit (tdmod);
   }

ignore_error: /* <- Jump taken if we should ignore the error */

   mc_tpld_drop_ref (&td);
   return cleared_pool;
}

/* Called from application threads
 * Caller must hold topology lock.
 * For single-threaded monitoring, the topology scanner may include errors for
 * servers that were removed from the topology.
 */
static void
_topology_collect_errors (const mongoc_topology_description_t *td, bson_error_t *error_out)
{
   const mongoc_server_description_t *server_description;
   bson_string_t *error_message;

   memset (error_out, 0, sizeof (bson_error_t));
   error_message = bson_string_new ("");

   for (size_t i = 0u; i < mc_tpld_servers_const (td)->items_len; i++) {
      const bson_error_t *error;

      server_description = mc_tpld_servers_const (td)->items[i].item;
      error = &server_description->error;
      if (error->code) {
         if (error_message->len > 0) {
            bson_string_append_c (error_message, ' ');
         }
         bson_string_append_printf (error_message, "[%s]", server_description->error.message);
         /* The last error's code and domain wins. */
         error_out->code = error->code;
         error_out->domain = error->domain;
      }
   }

   bson_strncpy ((char *) &error_out->message, error_message->str, sizeof (error_out->message));
   bson_string_free (error_message, true);
}

void
_mongoc_topology_set_rr_resolver (mongoc_topology_t *topology, _mongoc_rr_resolver_fn rr_resolver)
{
   topology->rr_resolver = rr_resolver;
}

uint32_t
_mongoc_topology_get_connection_pool_generation (const mongoc_topology_description_t *td,
                                                 uint32_t server_id,
                                                 const bson_oid_t *service_id)
{
   const mongoc_server_description_t *sd;
   bson_error_t error;

   BSON_ASSERT (service_id);

   sd = mongoc_topology_description_server_by_id_const (td, server_id, &error);
   if (!sd) {
      /* Server removed, ignore and ignore error. */
      return 0;
   }

   return mc_tpl_sd_get_generation (sd, service_id);
}

mc_tpld_modification
mc_tpld_modify_begin (mongoc_topology_t *tpl)
{
   mc_shared_tpld prev_td;
   mongoc_topology_description_t *new_td;
   bson_mutex_lock (&tpl->tpld_modification_mtx);
   prev_td = mc_tpld_take_ref (tpl);
   new_td = mongoc_topology_description_new_copy (prev_td.ptr);
   mc_tpld_drop_ref (&prev_td);
   return (mc_tpld_modification){
      .new_td = new_td,
      .topology = tpl,
   };
}

void
mc_tpld_modify_commit (mc_tpld_modification mod)
{
   mongoc_shared_ptr old_sptr = mongoc_shared_ptr_copy (mod.topology->_shared_descr_._sptr_);
   mongoc_shared_ptr new_sptr = mongoc_shared_ptr_create (mod.new_td, _tpld_destroy_and_free);
   mongoc_atomic_shared_ptr_store (&mod.topology->_shared_descr_._sptr_, new_sptr);
   bson_mutex_unlock (&mod.topology->tpld_modification_mtx);
   mongoc_shared_ptr_reset_null (&new_sptr);
   mongoc_shared_ptr_reset_null (&old_sptr);
}

void
mc_tpld_modify_drop (mc_tpld_modification mod)
{
   bson_mutex_unlock (&mod.topology->tpld_modification_mtx);
   mongoc_topology_description_destroy (mod.new_td);
}

bool
mongoc_topology_uses_server_api (const mongoc_topology_t *topology)
{
   BSON_ASSERT_PARAM (topology);
   return mongoc_topology_scanner_uses_server_api (topology->scanner);
}

bool
mongoc_topology_uses_loadbalanced (const mongoc_topology_t *topology)
{
   BSON_ASSERT_PARAM (topology);
   return mongoc_topology_scanner_uses_loadbalanced (topology->scanner);
}
