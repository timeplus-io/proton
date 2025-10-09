/*
 * Copyright 2020-present MongoDB, Inc.
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

#include "mongoc-topology-background-monitoring-private.h"

#include "mongoc-client-private.h"
#include "mongoc-log-private.h"
#include "mongoc-server-monitor-private.h"
#ifdef MONGOC_ENABLE_SSL
#include "mongoc-ssl-private.h"
#endif
#include "mongoc-stream-private.h"
#include "mongoc-topology-description-apm-private.h"
#include "mongoc-topology-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-util-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "monitor"

static BSON_THREAD_FUN (srv_polling_run, topology_void)
{
   mongoc_topology_t *topology;

   topology = topology_void;
   while (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) ==
          MONGOC_TOPOLOGY_SCANNER_BG_RUNNING) {
      int64_t now_ms;
      int64_t scan_due_ms;
      int64_t sleep_duration_ms;

      /* This will check if a scan is due. */
      if (!mongoc_topology_should_rescan_srv (topology)) {
         TRACE ("%s\n", "topology ineligible for SRV polling, stopping");
         break;
      }

      mongoc_topology_rescan_srv (topology);

      /* Unlock and sleep until next scan is due, or until shutdown signalled.
       */
      now_ms = bson_get_monotonic_time () / 1000;
      scan_due_ms = topology->srv_polling_last_scan_ms + _mongoc_topology_get_srv_polling_rescan_interval_ms (topology);
      sleep_duration_ms = scan_due_ms - now_ms;

      if (sleep_duration_ms > 0) {
         TRACE ("srv polling thread sleeping for %" PRId64 "ms", sleep_duration_ms);
      }

      /* Check for shutdown again here. mongoc_topology_rescan_srv unlocks the
       * topology srv_polling_mtx for the scan. The topology may have shut
       * down in that time. */
      bson_mutex_lock (&topology->srv_polling_mtx);
      if (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) !=
          MONGOC_TOPOLOGY_SCANNER_BG_RUNNING) {
         bson_mutex_unlock (&topology->srv_polling_mtx);
         break;
      }

      /* If shutting down, stop. */
      mongoc_cond_timedwait (&topology->srv_polling_cond, &topology->srv_polling_mtx, sleep_duration_ms);
      bson_mutex_unlock (&topology->srv_polling_mtx);
   }
   BSON_THREAD_RETURN;
}

/* Create a server monitor if necessary.
 *
 * Called by monitor threads and application threads when reconciling the
 * topology description.
 */
static void
_background_monitor_reconcile_server_monitor (mongoc_topology_t *topology,
                                              mongoc_topology_description_t *td,
                                              mongoc_server_description_t *sd)
{
   mongoc_set_t *server_monitors = topology->server_monitors;
   mongoc_server_monitor_t *server_monitor = mongoc_set_get (server_monitors, sd->id);

   if (!server_monitor) {
      /* Add a new server monitor. */
      server_monitor = mongoc_server_monitor_new (topology, td, sd);
      mongoc_server_monitor_run (server_monitor);
      mongoc_set_add (server_monitors, sd->id, server_monitor);
   }

   /* Check if an RTT monitor is needed. */
   if (!bson_empty (&sd->topology_version)) {
      mongoc_set_t *rtt_monitors;
      mongoc_server_monitor_t *rtt_monitor;

      rtt_monitors = topology->rtt_monitors;
      rtt_monitor = mongoc_set_get (rtt_monitors, sd->id);
      if (!rtt_monitor) {
         rtt_monitor = mongoc_server_monitor_new (topology, td, sd);
         mongoc_server_monitor_run_as_rtt (rtt_monitor);
         mongoc_set_add (rtt_monitors, sd->id, rtt_monitor);
      }
   }
   return;
}

/* Start background monitoring.
 *
 * Called by an application thread popping a client from a pool. Safe to
 * call repeatedly.
 */
void
_mongoc_topology_background_monitoring_start (mongoc_topology_t *topology)
{
   mc_tpld_modification tdmod;
   int prev_state;
   BSON_ASSERT (!topology->single_threaded);

   if (!topology->valid) {
      return;
   }

   prev_state = bson_atomic_int_compare_exchange_strong (&topology->scanner_state,
                                                         MONGOC_TOPOLOGY_SCANNER_OFF,
                                                         MONGOC_TOPOLOGY_SCANNER_BG_RUNNING,
                                                         bson_memory_order_relaxed);

   if (prev_state != MONGOC_TOPOLOGY_SCANNER_OFF) {
      /* The topology scanner is already running, or another thread is starting
       * it up now. */
      return;
   }

   TRACE ("%s", "background monitoring starting");

   tdmod = mc_tpld_modify_begin (topology);

   _mongoc_handshake_freeze ();
   _mongoc_topology_description_monitor_opening (tdmod.new_td);
   if (tdmod.new_td->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* Do not proceed to start monitoring threads. */
      TRACE ("%s", "disabling monitoring for load balanced topology");
   } else {
      /* Reconcile to create the first server monitors. */
      _mongoc_topology_background_monitoring_reconcile (topology, tdmod.new_td);
      /* Start SRV polling thread. */
      if (mongoc_topology_should_rescan_srv (topology)) {
         int ret = mcommon_thread_create (&topology->srv_polling_thread, srv_polling_run, topology);
         if (ret == 0) {
            topology->is_srv_polling = true;
         } else {
            char errmsg_buf[BSON_ERROR_BUFFER_SIZE];
            char *errmsg = bson_strerror_r (ret, errmsg_buf, sizeof errmsg_buf);
            MONGOC_ERROR ("Failed to start SRV polling thread. SRV records "
                          "will not be polled. Error: %s",
                          errmsg);
         }
      }
   }

   mc_tpld_modify_commit (tdmod);
}

/* Remove server monitors that are no longer in the set of server descriptions.
 *
 * Called by monitor threads and application threads when reconciling the
 * topology description.
 */
static void
_remove_orphaned_server_monitors (mongoc_set_t *server_monitors, mongoc_set_t *server_descriptions)
{
   uint32_t *server_monitor_ids_to_remove;
   uint32_t n_server_monitor_ids_to_remove = 0;

   /* Signal shutdown to server monitors no longer in the topology description.
    */
   server_monitor_ids_to_remove = bson_malloc0 (sizeof (uint32_t) * server_monitors->items_len);
   for (size_t i = 0u; i < server_monitors->items_len; i++) {
      mongoc_server_monitor_t *server_monitor;
      uint32_t id;

      server_monitor = mongoc_set_get_item_and_id (server_monitors, i, &id);
      if (!mongoc_set_get (server_descriptions, id)) {
         if (mongoc_server_monitor_request_shutdown (server_monitor)) {
            mongoc_server_monitor_wait_for_shutdown (server_monitor);
            mongoc_server_monitor_destroy (server_monitor);
            server_monitor_ids_to_remove[n_server_monitor_ids_to_remove] = id;
            n_server_monitor_ids_to_remove++;
         }
      }
   }

   /* Remove freed server monitors that have completed shutdown. */
   for (uint32_t i = 0u; i < n_server_monitor_ids_to_remove; i++) {
      mongoc_set_rm (server_monitors, server_monitor_ids_to_remove[i]);
   }
   bson_free (server_monitor_ids_to_remove);
}

/* Reconcile the topology description with the set of server monitors.
 *
 * Called when the topology description is updated (via handshake, monitoring,
 * or invalidation). May be called by server monitor thread or an application
 * thread.
 * Locks server monitor mutexes. May join / remove server monitors that have
 * completed shutdown.
 */
void
_mongoc_topology_background_monitoring_reconcile (mongoc_topology_t *topology, mongoc_topology_description_t *td)
{
   mongoc_set_t *server_descriptions = mc_tpld_servers (td);

   BSON_ASSERT (!topology->single_threaded);

   if (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) !=
       MONGOC_TOPOLOGY_SCANNER_BG_RUNNING) {
      return;
   }

   /* Add newly discovered server monitors, and update existing ones. */
   for (size_t i = 0u; i < server_descriptions->items_len; i++) {
      mongoc_server_description_t *sd;

      sd = mongoc_set_get_item (server_descriptions, i);
      _background_monitor_reconcile_server_monitor (topology, td, sd);
   }

   _remove_orphaned_server_monitors (topology->server_monitors, server_descriptions);
   _remove_orphaned_server_monitors (topology->rtt_monitors, server_descriptions);
}

/* Request all server monitors to scan.
 *
 * Called from application threads (during server selection or "not primary"
 * errors). Locks server monitor mutexes to deliver scan_requested.
 */
void
_mongoc_topology_background_monitoring_request_scan (mongoc_topology_t *topology)
{
   mongoc_set_t *server_monitors;

   BSON_ASSERT (!topology->single_threaded);

   if (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) ==
       MONGOC_TOPOLOGY_SCANNER_SHUTTING_DOWN) {
      return;
   }

   server_monitors = topology->server_monitors;

   for (size_t i = 0u; i < server_monitors->items_len; i++) {
      mongoc_server_monitor_t *server_monitor;
      uint32_t id;

      server_monitor = mongoc_set_get_item_and_id (server_monitors, i, &id);
      mongoc_server_monitor_request_scan (server_monitor);
   }
}

/* Stop, join, and destroy all server monitors.
 *
 * Called by application threads when destroying a client pool.
 * Locks server monitor mutexes to deliver shutdown. This function is
 * thread-safe. But in practice, it is only ever called by one application
 * thread (because mongoc_client_pool_destroy is not thread-safe).
 */
void
_mongoc_topology_background_monitoring_stop (mongoc_topology_t *topology)
{
   mongoc_server_monitor_t *server_monitor;

   BSON_ASSERT (!topology->single_threaded);

   if (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) !=
       MONGOC_TOPOLOGY_SCANNER_BG_RUNNING) {
      return;
   }

   TRACE ("%s", "background monitoring stopping");

   /* Tell the srv polling thread to stop */
   bson_mutex_lock (&topology->srv_polling_mtx);
   bson_atomic_int_exchange (
      &topology->scanner_state, MONGOC_TOPOLOGY_SCANNER_SHUTTING_DOWN, bson_memory_order_relaxed);

   if (topology->is_srv_polling) {
      /* Signal the srv poller to break out of waiting */
      mongoc_cond_signal (&topology->srv_polling_cond);
   }
   bson_mutex_unlock (&topology->srv_polling_mtx);

   bson_mutex_lock (&topology->tpld_modification_mtx);
   const size_t n_srv_monitors = topology->server_monitors->items_len;
   const size_t n_rtt_monitors = topology->rtt_monitors->items_len;
   bson_mutex_unlock (&topology->tpld_modification_mtx);

   /* Signal all server monitors to shut down. */
   for (size_t i = 0u; i < n_srv_monitors; i++) {
      server_monitor = mongoc_set_get_item (topology->server_monitors, i);
      mongoc_server_monitor_request_shutdown (server_monitor);
   }

   /* Signal all RTT monitors to shut down. */
   for (size_t i = 0u; i < n_rtt_monitors; i++) {
      server_monitor = mongoc_set_get_item (topology->rtt_monitors, i);
      mongoc_server_monitor_request_shutdown (server_monitor);
   }

   for (size_t i = 0u; i < n_srv_monitors; i++) {
      /* Wait for the thread to shutdown. */
      server_monitor = mongoc_set_get_item (topology->server_monitors, i);
      mongoc_server_monitor_wait_for_shutdown (server_monitor);
      mongoc_server_monitor_destroy (server_monitor);
   }

   for (size_t i = 0u; i < n_rtt_monitors; i++) {
      /* Wait for the thread to shutdown. */
      server_monitor = mongoc_set_get_item (topology->rtt_monitors, i);
      mongoc_server_monitor_wait_for_shutdown (server_monitor);
      mongoc_server_monitor_destroy (server_monitor);
   }

   /* Wait for SRV polling thread. */
   if (topology->is_srv_polling) {
      mcommon_thread_join (topology->srv_polling_thread);
   }

   /* Signal clients that are waiting on server selection to stop immediately,
    * as there will be no servers available.
    * This uses the tpld_modification_mtx as that is the mutex used with the
    * condition variable that will wait the waiting client threads. */
   bson_mutex_lock (&topology->tpld_modification_mtx);
   mongoc_set_destroy (topology->server_monitors);
   mongoc_set_destroy (topology->rtt_monitors);
   topology->server_monitors = mongoc_set_new (1, NULL, NULL);
   topology->rtt_monitors = mongoc_set_new (1, NULL, NULL);
   bson_atomic_int_exchange (&topology->scanner_state, MONGOC_TOPOLOGY_SCANNER_OFF, bson_memory_order_relaxed);
   mongoc_cond_broadcast (&topology->cond_client);
   bson_mutex_unlock (&topology->tpld_modification_mtx);
}

/* Cancel an in-progress streaming hello for a specific server (if
 * applicable).
 *
 * Called from application threads on network errors.
 */
void
_mongoc_topology_background_monitoring_cancel_check (mongoc_topology_t *topology, uint32_t server_id)
{
   mongoc_server_monitor_t *server_monitor;

   server_monitor = mongoc_set_get (topology->server_monitors, server_id);
   if (!server_monitor) {
      /* Already removed. */
      return;
   }
   mongoc_server_monitor_request_cancel (server_monitor);
}
