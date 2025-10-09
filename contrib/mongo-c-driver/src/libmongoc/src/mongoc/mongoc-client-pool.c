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


#include "mongoc.h"
#include "mongoc-apm-private.h"
#include "mongoc-array-private.h"
#include "mongoc-counters-private.h"
#include "mongoc-client-pool-private.h"
#include "mongoc-client-pool.h"
#include "mongoc-client-private.h"
#include "mongoc-client-side-encryption-private.h"
#include "mongoc-queue-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-topology-private.h"
#include "mongoc-topology-background-monitoring-private.h"
#include "mongoc-trace-private.h"

#ifdef MONGOC_ENABLE_SSL
#include "mongoc-ssl-private.h"
#endif

struct _mongoc_client_pool_t {
   bson_mutex_t mutex;
   mongoc_cond_t cond;
   mongoc_queue_t queue;
   mongoc_topology_t *topology;
   mongoc_uri_t *uri;
   uint32_t min_pool_size;
   uint32_t max_pool_size;
   uint32_t size;
#ifdef MONGOC_ENABLE_SSL
   bool ssl_opts_set;
   mongoc_ssl_opt_t ssl_opts;
#endif
   bool apm_callbacks_set;
   mongoc_apm_callbacks_t apm_callbacks;
   void *apm_context;
   int32_t error_api_version;
   bool error_api_set;
   mongoc_server_api_t *api;
   bool client_initialized;
   // `last_known_serverids` is a sorted array of uint32_t.
   mongoc_array_t last_known_serverids;
};


#ifdef MONGOC_ENABLE_SSL
void
mongoc_client_pool_set_ssl_opts (mongoc_client_pool_t *pool, const mongoc_ssl_opt_t *opts)
{
   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);

   _mongoc_ssl_opts_cleanup (&pool->ssl_opts, false /* don't free internal opts. */);

   pool->ssl_opts_set = false;

   if (opts) {
      _mongoc_ssl_opts_copy_to (opts, &pool->ssl_opts, false /* don't overwrite internal opts. */);
      pool->ssl_opts_set = true;
   }

   mongoc_topology_scanner_set_ssl_opts (pool->topology->scanner, &pool->ssl_opts);

   bson_mutex_unlock (&pool->mutex);
}

void
_mongoc_client_pool_set_internal_tls_opts (mongoc_client_pool_t *pool, _mongoc_internal_tls_opts_t *internal)
{
   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);
   if (!pool->ssl_opts_set) {
      bson_mutex_unlock (&pool->mutex);
      return;
   }
   pool->ssl_opts.internal = bson_malloc (sizeof (_mongoc_internal_tls_opts_t));
   memcpy (pool->ssl_opts.internal, internal, sizeof (_mongoc_internal_tls_opts_t));
   bson_mutex_unlock (&pool->mutex);
}
#endif


mongoc_client_pool_t *
mongoc_client_pool_new (const mongoc_uri_t *uri)
{
   mongoc_client_pool_t *pool;
   bson_error_t error = {0};

   if (!(pool = mongoc_client_pool_new_with_error (uri, &error))) {
      MONGOC_ERROR ("%s", error.message);
   }

   return pool;
}


mongoc_client_pool_t *
mongoc_client_pool_new_with_error (const mongoc_uri_t *uri, bson_error_t *error)
{
   mongoc_topology_t *topology;
   mongoc_client_pool_t *pool;
   const bson_t *b;
   bson_iter_t iter;
   const char *appname;


   ENTRY;

   BSON_ASSERT (uri);

#ifndef MONGOC_ENABLE_SSL
   if (mongoc_uri_get_tls (uri)) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Can't create SSL client pool, SSL not enabled in this "
                      "build.");
      return NULL;
   }
#endif

   topology = mongoc_topology_new (uri, false);

   if (!topology->valid) {
      if (error) {
         memcpy (error, &topology->scanner->error, sizeof (bson_error_t));
      }

      mongoc_topology_destroy (topology);

      RETURN (NULL);
   }

   pool = (mongoc_client_pool_t *) bson_malloc0 (sizeof *pool);
   _mongoc_array_init (&pool->last_known_serverids, sizeof (uint32_t));
   bson_mutex_init (&pool->mutex);
   mongoc_cond_init (&pool->cond);
   _mongoc_queue_init (&pool->queue);
   pool->uri = mongoc_uri_copy (uri);
   pool->min_pool_size = 0;
   pool->max_pool_size = 100;
   pool->size = 0;
   pool->topology = topology;
   pool->error_api_version = MONGOC_ERROR_API_VERSION_LEGACY;

   b = mongoc_uri_get_options (pool->uri);

   if (bson_iter_init_find_case (&iter, b, MONGOC_URI_MINPOOLSIZE)) {
      MONGOC_WARNING (MONGOC_URI_MINPOOLSIZE " is deprecated; its behavior does not match its name, and its actual"
                                             " behavior will likely hurt performance.");

      if (BSON_ITER_HOLDS_INT32 (&iter)) {
         pool->min_pool_size = BSON_MAX (0, bson_iter_int32 (&iter));
      }
   }

   if (bson_iter_init_find_case (&iter, b, MONGOC_URI_MAXPOOLSIZE)) {
      if (BSON_ITER_HOLDS_INT32 (&iter)) {
         pool->max_pool_size = BSON_MAX (1, bson_iter_int32 (&iter));
      }
   }

   appname = mongoc_uri_get_option_as_utf8 (pool->uri, MONGOC_URI_APPNAME, NULL);
   if (appname) {
      /* the appname should have already been validated */
      BSON_ASSERT (mongoc_client_pool_set_appname (pool, appname));
   }

#ifdef MONGOC_ENABLE_SSL
   if (mongoc_uri_get_tls (pool->uri)) {
      mongoc_ssl_opt_t ssl_opt = {0};
      _mongoc_internal_tls_opts_t internal_tls_opts = {0};

      _mongoc_ssl_opts_from_uri (&ssl_opt, &internal_tls_opts, pool->uri);
      /* sets use_ssl = true */
      mongoc_client_pool_set_ssl_opts (pool, &ssl_opt);
      _mongoc_client_pool_set_internal_tls_opts (pool, &internal_tls_opts);
   }
#endif
   mongoc_counter_client_pools_active_inc ();

   RETURN (pool);
}


void
mongoc_client_pool_destroy (mongoc_client_pool_t *pool)
{
   mongoc_client_t *client;

   ENTRY;

   if (!pool) {
      EXIT;
   }

   if (!mongoc_server_session_pool_is_empty (pool->topology->session_pool)) {
      client = mongoc_client_pool_pop (pool);
      _mongoc_client_end_sessions (client);
      mongoc_client_pool_push (pool, client);
   }

   while ((client = (mongoc_client_t *) _mongoc_queue_pop_head (&pool->queue))) {
      mongoc_client_destroy (client);
   }

   mongoc_topology_destroy (pool->topology);

   mongoc_uri_destroy (pool->uri);
   bson_mutex_destroy (&pool->mutex);
   mongoc_cond_destroy (&pool->cond);

   mongoc_server_api_destroy (pool->api);

#ifdef MONGOC_ENABLE_SSL
   _mongoc_ssl_opts_cleanup (&pool->ssl_opts, true);
#endif

   _mongoc_array_destroy (&pool->last_known_serverids);

   bson_free (pool);

   mongoc_counter_client_pools_active_dec ();
   mongoc_counter_client_pools_disposed_inc ();

   EXIT;
}


/*
 * Start the background topology scanner.
 *
 * This function assumes the pool's mutex is locked
 */
static void
_start_scanner_if_needed (mongoc_client_pool_t *pool)
{
   BSON_ASSERT_PARAM (pool);

   if (!pool->topology->single_threaded) {
      _mongoc_topology_background_monitoring_start (pool->topology);
   }
}

static void
_initialize_new_client (mongoc_client_pool_t *pool, mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (pool);
   BSON_ASSERT_PARAM (client);

   /* for tests */
   mongoc_client_set_stream_initiator (
      client, pool->topology->scanner->initiator, pool->topology->scanner->initiator_context);

   pool->client_initialized = true;
   client->is_pooled = true;
   client->error_api_version = pool->error_api_version;
   _mongoc_client_set_apm_callbacks_private (client, &pool->apm_callbacks, pool->apm_context);

   client->api = mongoc_server_api_copy (pool->api);

#ifdef MONGOC_ENABLE_SSL
   if (pool->ssl_opts_set) {
      mongoc_client_set_ssl_opts (client, &pool->ssl_opts);
   }
#endif
}

mongoc_client_t *
mongoc_client_pool_pop (mongoc_client_pool_t *pool)
{
   mongoc_client_t *client;
   int32_t wait_queue_timeout_ms;
   int64_t expire_at_ms = -1;
   int64_t now_ms;
   int r;

   ENTRY;

   BSON_ASSERT_PARAM (pool);

   wait_queue_timeout_ms = mongoc_uri_get_option_as_int32 (pool->uri, MONGOC_URI_WAITQUEUETIMEOUTMS, -1);
   if (wait_queue_timeout_ms > 0) {
      expire_at_ms = (bson_get_monotonic_time () / 1000) + wait_queue_timeout_ms;
   }
   bson_mutex_lock (&pool->mutex);

again:
   if (!(client = (mongoc_client_t *) _mongoc_queue_pop_head (&pool->queue))) {
      if (pool->size < pool->max_pool_size) {
         client = _mongoc_client_new_from_topology (pool->topology);
         BSON_ASSERT (client);
         _initialize_new_client (pool, client);
         pool->size++;
      } else {
         if (wait_queue_timeout_ms > 0) {
            now_ms = bson_get_monotonic_time () / 1000;
            if (now_ms < expire_at_ms) {
               r = mongoc_cond_timedwait (&pool->cond, &pool->mutex, expire_at_ms - now_ms);
               if (mongo_cond_ret_is_timedout (r)) {
                  GOTO (done);
               }
            } else {
               GOTO (done);
            }
         } else {
            mongoc_cond_wait (&pool->cond, &pool->mutex);
         }
         GOTO (again);
      }
   }

   _start_scanner_if_needed (pool);
done:
   bson_mutex_unlock (&pool->mutex);

   RETURN (client);
}


mongoc_client_t *
mongoc_client_pool_try_pop (mongoc_client_pool_t *pool)
{
   mongoc_client_t *client;

   ENTRY;

   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);

   if (!(client = (mongoc_client_t *) _mongoc_queue_pop_head (&pool->queue))) {
      if (pool->size < pool->max_pool_size) {
         client = _mongoc_client_new_from_topology (pool->topology);
         BSON_ASSERT (client);
         _initialize_new_client (pool, client);
         pool->size++;
      }
   }

   if (client) {
      _start_scanner_if_needed (pool);
   }
   bson_mutex_unlock (&pool->mutex);

   RETURN (client);
}

typedef struct {
   mongoc_array_t *known_server_ids;
   mongoc_cluster_t *cluster;
} prune_ctx;

static int
server_id_cmp (const void *a_, const void *b_)
{
   const uint32_t *const a = (const uint32_t *) a_;
   const uint32_t *const b = (const uint32_t *) b_;

   if (*a == *b) {
      return 0;
   }

   return *a < *b ? -1 : 1;
}

// `maybe_prune` removes a `mongoc_cluster_node_t` if the node refers to a removed server.
static bool
maybe_prune (void *item, void *ctx_)
{
   mongoc_cluster_node_t *cn = (mongoc_cluster_node_t *) item;
   prune_ctx *ctx = (prune_ctx *) ctx_;
   // Get the server ID from the cluster node.
   uint32_t server_id = cn->handshake_sd->id;

   // Check if the cluster node's server ID references a removed server.
   if (!bsearch (
          &server_id, ctx->known_server_ids->data, ctx->known_server_ids->len, sizeof (uint32_t), server_id_cmp)) {
      mongoc_cluster_disconnect_node (ctx->cluster, server_id);
   }
   return true;
}

// `prune_client` closes connections from `client` to servers not contained in `known_server_ids`.
static void
prune_client (mongoc_client_t *client, mongoc_array_t *known_server_ids)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT_PARAM (known_server_ids);

   mongoc_cluster_t *cluster = &client->cluster;
   prune_ctx ctx = {.cluster = cluster, .known_server_ids = known_server_ids};
   mongoc_set_for_each (cluster->nodes, maybe_prune, &ctx);
}


void
mongoc_client_pool_push (mongoc_client_pool_t *pool, mongoc_client_t *client)
{
   ENTRY;

   BSON_ASSERT_PARAM (pool);
   BSON_ASSERT_PARAM (client);

   /* reset sockettimeoutms to the default in case it was changed with mongoc_client_set_sockettimeoutms() */
   mongoc_cluster_reset_sockettimeoutms (&client->cluster);

   bson_mutex_lock (&pool->mutex);
   // Check if `last_known_server_ids` needs update.
   bool serverids_have_changed = false;
   {
      mongoc_array_t current_serverids;
      _mongoc_array_init (&current_serverids, sizeof (uint32_t));

      {
         mc_shared_tpld td = mc_tpld_take_ref (pool->topology);
         const mongoc_set_t *servers = mc_tpld_servers_const (td.ptr);
         for (size_t i = 0; i < servers->items_len; i++) {
            _mongoc_array_append_val (&current_serverids, servers->items[i].id);
         }
         mc_tpld_drop_ref (&td);
      }

      serverids_have_changed = (current_serverids.len != pool->last_known_serverids.len) ||
                               memcmp (current_serverids.data,
                                       pool->last_known_serverids.data,
                                       current_serverids.len * current_serverids.element_size) != 0;

      if (serverids_have_changed) {
         _mongoc_array_destroy (&pool->last_known_serverids);
         pool->last_known_serverids = current_serverids; // Ownership transfer.
      } else {
         _mongoc_array_destroy (&current_serverids);
      }
   }

   // Check if pooled clients need to be pruned.
   if (serverids_have_changed) {
      // The set of last known server IDs has changed. Prune all clients in pool.
      mongoc_queue_item_t *ptr = pool->queue.head;
      while (ptr != NULL) {
         prune_client ((mongoc_client_t *) ptr->data, &pool->last_known_serverids);
         ptr = ptr->next;
      }
   }

   // Always prune incoming client. The topology may have changed while client was checked out.
   prune_client (client, &pool->last_known_serverids);

   // Push client back into pool.
   _mongoc_queue_push_head (&pool->queue, client);

   if (pool->min_pool_size && _mongoc_queue_get_length (&pool->queue) > pool->min_pool_size) {
      mongoc_client_t *old_client;
      old_client = (mongoc_client_t *) _mongoc_queue_pop_tail (&pool->queue);
      if (old_client) {
         mongoc_client_destroy (old_client);
         pool->size--;
      }
   }

   mongoc_cond_signal (&pool->cond);
   bson_mutex_unlock (&pool->mutex);

   EXIT;
}

/* for tests */
void
_mongoc_client_pool_set_stream_initiator (mongoc_client_pool_t *pool, mongoc_stream_initiator_t si, void *context)
{
   BSON_ASSERT_PARAM (pool);

   mongoc_topology_scanner_set_stream_initiator (pool->topology->scanner, si, context);
}

/* for tests */
size_t
mongoc_client_pool_get_size (mongoc_client_pool_t *pool)
{
   size_t size = 0;

   ENTRY;
   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);
   size = pool->size;
   bson_mutex_unlock (&pool->mutex);

   RETURN (size);
}


size_t
mongoc_client_pool_num_pushed (mongoc_client_pool_t *pool)
{
   size_t num_pushed = 0;

   ENTRY;
   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);
   num_pushed = pool->queue.length;
   bson_mutex_unlock (&pool->mutex);

   RETURN (num_pushed);
}


mongoc_topology_t *
_mongoc_client_pool_get_topology (mongoc_client_pool_t *pool)
{
   BSON_ASSERT_PARAM (pool);

   return pool->topology;
}


void
mongoc_client_pool_max_size (mongoc_client_pool_t *pool, uint32_t max_pool_size)
{
   ENTRY;
   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);
   pool->max_pool_size = max_pool_size;
   bson_mutex_unlock (&pool->mutex);

   EXIT;
}

void
mongoc_client_pool_min_size (mongoc_client_pool_t *pool, uint32_t min_pool_size)
{
   ENTRY;
   BSON_ASSERT_PARAM (pool);

   MONGOC_WARNING ("mongoc_client_pool_min_size is deprecated; its behavior does not match"
                   " its name, and its actual behavior will likely hurt performance.");

   bson_mutex_lock (&pool->mutex);
   pool->min_pool_size = min_pool_size;
   bson_mutex_unlock (&pool->mutex);

   EXIT;
}

bool
mongoc_client_pool_set_apm_callbacks (mongoc_client_pool_t *pool, mongoc_apm_callbacks_t *callbacks, void *context)
{
   BSON_ASSERT_PARAM (pool);

   mongoc_topology_t *const topology = BSON_ASSERT_PTR_INLINE (pool)->topology;
   mc_tpld_modification tdmod = mc_tpld_modify_begin (topology);

   // Prevent setting callbacks more than once
   if (pool->apm_callbacks_set) {
      mc_tpld_modify_drop (tdmod);
      MONGOC_ERROR ("Can only set callbacks once");
      return false;
   }

   // Update callbacks on the pool
   if (callbacks) {
      pool->apm_callbacks = *callbacks;
   } else {
      pool->apm_callbacks = (mongoc_apm_callbacks_t){0};
   }
   pool->apm_context = context;

   // Update callbacks on the topology
   mongoc_topology_set_apm_callbacks (topology, tdmod.new_td, callbacks, context);

   // Signal that we have already set the callbacks
   pool->apm_callbacks_set = true;

   // Save our updated topology
   mc_tpld_modify_commit (tdmod);

   return true;
}

bool
mongoc_client_pool_set_error_api (mongoc_client_pool_t *pool, int32_t version)
{
   if (version != MONGOC_ERROR_API_VERSION_LEGACY && version != MONGOC_ERROR_API_VERSION_2) {
      MONGOC_ERROR ("Unsupported Error API Version: %" PRId32, version);
      return false;
   }

   BSON_ASSERT_PARAM (pool);

   if (pool->error_api_set) {
      MONGOC_ERROR ("Can only set Error API Version once");
      return false;
   }

   pool->error_api_version = version;
   pool->error_api_set = true;

   return true;
}

bool
mongoc_client_pool_set_appname (mongoc_client_pool_t *pool, const char *appname)
{
   bool ret;

   BSON_ASSERT_PARAM (pool);

   bson_mutex_lock (&pool->mutex);
   ret = _mongoc_topology_set_appname (pool->topology, appname);
   bson_mutex_unlock (&pool->mutex);

   return ret;
}

bool
mongoc_client_pool_enable_auto_encryption (mongoc_client_pool_t *pool,
                                           mongoc_auto_encryption_opts_t *opts,
                                           bson_error_t *error)
{
   BSON_ASSERT_PARAM (pool);

   return _mongoc_cse_client_pool_enable_auto_encryption (pool->topology, opts, error);
}

bool
mongoc_client_pool_set_server_api (mongoc_client_pool_t *pool, const mongoc_server_api_t *api, bson_error_t *error)
{
   BSON_ASSERT_PARAM (pool);
   BSON_ASSERT_PARAM (api);

   if (pool->api) {
      bson_set_error (
         error, MONGOC_ERROR_POOL, MONGOC_ERROR_POOL_API_ALREADY_SET, "Cannot set server api more than once per pool");
      return false;
   }

   if (pool->client_initialized) {
      bson_set_error (error,
                      MONGOC_ERROR_POOL,
                      MONGOC_ERROR_POOL_API_TOO_LATE,
                      "Cannot set server api after a client has been created");
      return false;
   }

   pool->api = mongoc_server_api_copy (api);

   _mongoc_topology_scanner_set_server_api (pool->topology->scanner, api);

   return true;
}
