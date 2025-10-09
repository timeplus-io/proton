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

#include "mongoc-array-private.h"
#include "mongoc-error.h"
#include "mongoc-server-description-private.h"
#include "mongoc-topology-description-apm-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-util-private.h"
#include "mongoc-read-prefs-private.h"
#include "mongoc-set-private.h"
#include "mongoc-client-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-host-list-private.h"
#include "utlist.h"


static bool
_is_data_node (const mongoc_server_description_t *sd)
{
   switch (sd->type) {
   case MONGOC_SERVER_MONGOS:
   case MONGOC_SERVER_STANDALONE:
   case MONGOC_SERVER_RS_SECONDARY:
   case MONGOC_SERVER_RS_PRIMARY:
   case MONGOC_SERVER_LOAD_BALANCER:
      return true;
   case MONGOC_SERVER_RS_OTHER:
   case MONGOC_SERVER_RS_ARBITER:
   case MONGOC_SERVER_UNKNOWN:
   case MONGOC_SERVER_POSSIBLE_PRIMARY:
   case MONGOC_SERVER_RS_GHOST:
   case MONGOC_SERVER_DESCRIPTION_TYPES:
   default:
      return false;
   }
}


static void
_mongoc_topology_server_dtor (void *server_, void *ctx_)
{
   BSON_UNUSED (ctx_);

   mongoc_server_description_destroy ((mongoc_server_description_t *) server_);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_init --
 *
 *       Initialize the given topology description
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_topology_description_init (mongoc_topology_description_t *description, int64_t heartbeat_msec)
{
   ENTRY;

   BSON_ASSERT (description);

   memset (description, 0, sizeof (*description));

   bson_oid_init (&description->topology_id, NULL);
   description->opened = false;
   description->type = MONGOC_TOPOLOGY_UNKNOWN;
   description->heartbeat_msec = heartbeat_msec;
   description->_servers_ = mongoc_set_new (8, _mongoc_topology_server_dtor, NULL);
   description->set_name = NULL;
   description->max_set_version = MONGOC_NO_SET_VERSION;
   description->stale = true;
   description->rand_seed = (unsigned int) bson_get_monotonic_time ();
   bson_init (&description->cluster_time);
   description->session_timeout_minutes = MONGOC_NO_SESSIONS;

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_copy_to --
 *
 *       Deep-copy @src to an uninitialized topology description @dst.
 *       @dst must not already point to any allocated resources. Clean
 *       up with mongoc_topology_description_cleanup.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_topology_description_copy_to (const mongoc_topology_description_t *src, mongoc_topology_description_t *dst)
{
   size_t nitems;
   const mongoc_server_description_t *sd;
   uint32_t id;

   ENTRY;

   BSON_ASSERT (src);
   BSON_ASSERT (dst);

   bson_oid_copy (&src->topology_id, &dst->topology_id);
   bson_oid_copy (&src->max_election_id, &dst->max_election_id);
   dst->opened = src->opened;
   dst->type = src->type;
   dst->heartbeat_msec = src->heartbeat_msec;
   dst->rand_seed = src->rand_seed;

   nitems = bson_next_power_of_two (mc_tpld_servers_const (src)->items_len);
   dst->_servers_ = mongoc_set_new (nitems, _mongoc_topology_server_dtor, NULL);
   for (size_t i = 0u; i < mc_tpld_servers_const (src)->items_len; i++) {
      sd = mongoc_set_get_item_and_id_const (mc_tpld_servers_const (src), i, &id);
      mongoc_set_add (mc_tpld_servers (dst), id, mongoc_server_description_new_copy (sd));
   }

   dst->set_name = bson_strdup (src->set_name);
   dst->max_set_version = src->max_set_version;
   memcpy (&dst->compatibility_error, &src->compatibility_error, sizeof (bson_error_t));
   dst->max_server_id = src->max_server_id;
   dst->max_hosts = src->max_hosts;
   dst->stale = src->stale;
   memcpy (&dst->apm_callbacks, &src->apm_callbacks, sizeof (mongoc_apm_callbacks_t));

   dst->apm_context = src->apm_context;

   bson_copy_to (&src->cluster_time, &dst->cluster_time);

   dst->session_timeout_minutes = src->session_timeout_minutes;

   EXIT;
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_description_new_copy --
 *
 *       Allocates a new topology description and deep-copies @description to it
 *       using _mongoc_topology_description_copy_to.
 *
 * Returns:
 *       A copy of a topology description that you must destroy with
 *       mongoc_topology_description_destroy, or NULL if @description is NULL.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */
mongoc_topology_description_t *
mongoc_topology_description_new_copy (const mongoc_topology_description_t *description)
{
   mongoc_topology_description_t *copy;

   if (!description) {
      return NULL;
   }

   copy = BSON_ALIGNED_ALLOC0 (mongoc_topology_description_t);

   _mongoc_topology_description_copy_to (description, copy);

   return copy;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_cleanup --
 *
 *       Destroy allocated resources within @description but don't free it.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_topology_description_cleanup (mongoc_topology_description_t *description)
{
   ENTRY;

   BSON_ASSERT (description);

   if (mc_tpld_servers (description)) {
      mongoc_set_destroy (mc_tpld_servers (description));
   }

   if (description->set_name) {
      bson_free (description->set_name);
   }

   bson_destroy (&description->cluster_time);

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_destroy --
 *
 *       Destroy allocated resources within @description and free
 *       @description.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_topology_description_destroy (mongoc_topology_description_t *description)
{
   ENTRY;

   if (!description) {
      EXIT;
   }

   mongoc_topology_description_cleanup (description);
   bson_free (description);

   EXIT;
}

/* find the primary, then stop iterating */
static bool
_mongoc_topology_description_has_primary_cb (const void *item, void *ctx /* OUT */)
{
   const mongoc_server_description_t *server = item;
   const mongoc_server_description_t **primary = ctx;

   /* TODO should this include MONGOS? */
   if (server->type == MONGOC_SERVER_RS_PRIMARY || server->type == MONGOC_SERVER_STANDALONE) {
      *primary = (mongoc_server_description_t *) item;
      return false;
   }
   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_has_primary --
 *
 *       If topology has a primary, return it.
 *
 * Returns:
 *       A pointer to the primary, or NULL.
 *
 * Side effects:
 *       None
 *
 *--------------------------------------------------------------------------
 */
static const mongoc_server_description_t *
_mongoc_topology_description_has_primary (const mongoc_topology_description_t *description)
{
   mongoc_server_description_t *primary = NULL;

   mongoc_set_for_each_const (
      mc_tpld_servers_const (description), _mongoc_topology_description_has_primary_cb, &primary);

   return primary;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_server_description_primary_is_not_stale --
 *
 *       Checks if a primary server is not stale by comparing the electionId and
 *       setVersion.
 *
 * Returns:
 *       True if the server's electionId is larger or the server's version is
 *       later than the topology max version.
 *
 * Side effects:
 *       None
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_server_description_primary_is_not_stale (mongoc_topology_description_t *td,
                                                 const mongoc_server_description_t *sd)
{
   /* initially max_set_version is -1 and max_election_id is zeroed */
   return (bson_oid_compare (&sd->election_id, &td->max_election_id) > 0) ||
          ((bson_oid_compare (&sd->election_id, &td->max_election_id) == 0) && sd->set_version >= td->max_set_version);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_later_election --
 *
 *       Check if we've seen a more recent election in the replica set
 *       than this server has.
 *
 * Returns:
 *       True if the topology description's max replica set version plus
 *       election id is later than the server description's.
 *
 * Side effects:
 *       None
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_topology_description_later_election (mongoc_topology_description_t *td, const mongoc_server_description_t *sd)
{
   /* initially max_set_version is -1 and max_election_id is zeroed */
   return td->max_set_version > sd->set_version ||
          (td->max_set_version == sd->set_version && bson_oid_compare (&td->max_election_id, &sd->election_id) > 0);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_set_max_set_version --
 *
 *       Remember that we've seen a new replica set version. Unconditionally
 *       sets td->set_version to sd->set_version.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_set_max_set_version (mongoc_topology_description_t *td,
                                                  const mongoc_server_description_t *sd)
{
   td->max_set_version = sd->set_version;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_set_max_election_id --
 *
 *       Remember that we've seen a new election id. Unconditionally sets
 *       td->max_election_id to sd->election_id.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_set_max_election_id (mongoc_topology_description_t *td,
                                                  const mongoc_server_description_t *sd)
{
   bson_oid_copy (&sd->election_id, &td->max_election_id);
}

static bool
_mongoc_topology_description_server_is_candidate (mongoc_server_description_type_t desc_type,
                                                  mongoc_read_mode_t read_mode,
                                                  mongoc_topology_description_type_t topology_type)
{
   switch ((int) topology_type) {
   case MONGOC_TOPOLOGY_SINGLE:
      switch ((int) desc_type) {
      case MONGOC_SERVER_STANDALONE:
         return true;
      default:
         return false;
      }

   case MONGOC_TOPOLOGY_RS_NO_PRIMARY:
   case MONGOC_TOPOLOGY_RS_WITH_PRIMARY:
      switch ((int) read_mode) {
      case MONGOC_READ_PRIMARY:
         switch ((int) desc_type) {
         case MONGOC_SERVER_RS_PRIMARY:
            return true;
         default:
            return false;
         }
      case MONGOC_READ_SECONDARY:
         switch ((int) desc_type) {
         case MONGOC_SERVER_RS_SECONDARY:
            return true;
         default:
            return false;
         }
      default:
         switch ((int) desc_type) {
         case MONGOC_SERVER_RS_PRIMARY:
         case MONGOC_SERVER_RS_SECONDARY:
            return true;
         default:
            return false;
         }
      }

   case MONGOC_TOPOLOGY_SHARDED:
      switch ((int) desc_type) {
      case MONGOC_SERVER_MONGOS:
         return true;
      default:
         return false;
      }

   /* Note, there is no call path that leads to the
    * MONGOC_TOPOLOGY_LOAD_BALANCED case. Server selection for load balanced
    * topologies bypasses this logic. This silences compiler warnings on
    * unhandled enum values. */
   case MONGOC_TOPOLOGY_LOAD_BALANCED:
      return desc_type == MONGOC_SERVER_LOAD_BALANCER;

   default:
      return false;
   }
}

typedef struct _mongoc_suitable_data_t {
   mongoc_read_mode_t read_mode;
   mongoc_topology_description_type_t topology_type;
   const mongoc_server_description_t *primary;     /* OUT */
   const mongoc_server_description_t **candidates; /* OUT */
   size_t candidates_len;                          /* OUT */
   bool has_secondary;                             /* OUT */
} mongoc_suitable_data_t;

static bool
_mongoc_replica_set_read_suitable_cb (const void *item, void *ctx)
{
   const mongoc_server_description_t *server = item;
   mongoc_suitable_data_t *data = (mongoc_suitable_data_t *) ctx;

   /* primary's used in staleness calculation, even with mode SECONDARY */
   if (server->type == MONGOC_SERVER_RS_PRIMARY) {
      data->primary = server;
   }

   if (_mongoc_topology_description_server_is_candidate (server->type, data->read_mode, data->topology_type)) {
      if (server->type == MONGOC_SERVER_RS_PRIMARY) {
         if (data->read_mode == MONGOC_READ_PRIMARY || data->read_mode == MONGOC_READ_PRIMARY_PREFERRED) {
            /* we want a primary and we have one, done! */
            return false;
         }
      }

      if (server->type == MONGOC_SERVER_RS_SECONDARY) {
         data->has_secondary = true;
      }

      /* add to our candidates */
      data->candidates[data->candidates_len++] = server;
   } else {
      TRACE ("Rejected [%s] [%s] for mode [%s]",
             mongoc_server_description_type (server),
             server->host.host_and_port,
             _mongoc_read_mode_as_str (data->read_mode));
   }

   return true;
}


/* if any mongos are candidates, add them to the candidates array */
static void
_mongoc_try_mode_secondary (mongoc_array_t *set, /* OUT */
                            const mongoc_topology_description_t *topology,
                            const mongoc_read_prefs_t *read_pref,
                            bool *must_use_primary,
                            const mongoc_deprioritized_servers_t *ds,
                            int64_t local_threshold_ms)
{
   mongoc_read_prefs_t *secondary;

   secondary = mongoc_read_prefs_copy (read_pref);
   mongoc_read_prefs_set_mode (secondary, MONGOC_READ_SECONDARY);

   mongoc_topology_description_suitable_servers (
      set, MONGOC_SS_READ, topology, secondary, must_use_primary, ds, local_threshold_ms);

   mongoc_read_prefs_destroy (secondary);
}


static bool
_mongoc_td_servers_to_candidates_array (const void *item, void *ctx)
{
   BSON_ASSERT_PARAM (item);
   BSON_ASSERT_PARAM (ctx);

   const mongoc_server_description_t *const server = item;
   mongoc_suitable_data_t *const data = (mongoc_suitable_data_t *) ctx;

   data->candidates[data->candidates_len++] = server;

   return true;
}

// Server Selection Spec: If a list of deprioritized servers is provided, and
// the topology is a sharded cluster, these servers should be selected only if
// there are no other suitable servers. The server selection algorithm MUST
// ignore the deprioritized servers if the topology is not a sharded cluster.
static void
_mongoc_filter_deprioritized_servers (mongoc_suitable_data_t *data, const mongoc_deprioritized_servers_t *ds)
{
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT_PARAM (ds);

   TRACE ("%s", "deprioritization: filtering list of candidates");

   mongoc_array_t filtered_servers;
   _mongoc_array_init (&filtered_servers, sizeof (const mongoc_server_description_t *));

   for (size_t idx = 0u; idx < data->candidates_len; ++idx) {
      mongoc_server_description_t const *const sd = data->candidates[idx];

      if (!mongoc_deprioritized_servers_contains (ds, sd)) {
         TRACE ("deprioritization: - kept: %s (id: %" PRIu32 ")", sd->host.host_and_port, sd->id);
         _mongoc_array_append_val (&filtered_servers, sd);
      } else {
         TRACE ("deprioritization: - removed: %s (id: %" PRIu32 ")", sd->host.host_and_port, sd->id);
      }
   }

   if (filtered_servers.len == 0u) {
      TRACE ("%s", "deprioritization: reverted due to no other suitable servers");
      _mongoc_array_destroy (&filtered_servers);
   } else if (filtered_servers.len == data->candidates_len) {
      TRACE ("%s", "deprioritization: none found in list of candidates");
      _mongoc_array_destroy (&filtered_servers);
   } else {
      TRACE ("%s", "deprioritization: using filtered list of candidates");
      data->candidates_len = filtered_servers.len;
      // `(void*)`: avoid MSVC error C4090:
      //   'function': different 'const' qualifiers
      memmove ((void *) data->candidates, filtered_servers.data, filtered_servers.len * filtered_servers.element_size);
      _mongoc_array_destroy (&filtered_servers);
   }
}


// Keep only suitable mongoses in the candidates array.
static void
_mongoc_filter_suitable_mongos (mongoc_suitable_data_t *data)
{
   size_t idx = 0u;

   while (idx < data->candidates_len) {
      if (_mongoc_topology_description_server_is_candidate (
             data->candidates[idx]->type, data->read_mode, data->topology_type)) {
         // All candidates in the latency window are suitable.
         ++idx;
      } else {
         // Remove from list using swap+pop.
         // Order doesn't matter; the list will be randomized in
         // mongoc_topology_description_select prior to server selection.
         data->candidates[idx] = data->candidates[--data->candidates_len];
      }
   }
}


/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_description_lowest_max_wire_version --
 *
 *       The topology's max wire version.
 *
 * Returns:
 *       The minimum of all known servers' max wire versions, or INT32_MAX
 *       if there are no known servers.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */
int32_t
mongoc_topology_description_lowest_max_wire_version (const mongoc_topology_description_t *td)
{
   int32_t ret = INT32_MAX;
   const mongoc_set_t *servers = mc_tpld_servers_const (td);

   for (size_t i = 0u; (size_t) i < servers->items_len; i++) {
      const mongoc_server_description_t *sd = mongoc_set_get_item_const (servers, i);
      if (sd->type != MONGOC_SERVER_UNKNOWN && sd->type != MONGOC_SERVER_POSSIBLE_PRIMARY &&
          sd->max_wire_version < ret) {
         ret = sd->max_wire_version;
      }
   }

   return ret;
}


/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_description_all_sds_have_write_date --
 *
 *       Whether the primary and all secondaries' server descriptions have
 *       last_write_date_ms.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */
bool
mongoc_topology_description_all_sds_have_write_date (const mongoc_topology_description_t *td)
{
   for (size_t i = 0u; (size_t) i < mc_tpld_servers_const (td)->items_len; i++) {
      const mongoc_server_description_t *sd = mongoc_set_get_item_const (mc_tpld_servers_const (td), i);

      if (sd->last_write_date_ms <= 0 &&
          (sd->type == MONGOC_SERVER_RS_PRIMARY || sd->type == MONGOC_SERVER_RS_SECONDARY)) {
         return false;
      }
   }

   return true;
}

/*
 *-------------------------------------------------------------------------
 *
 * _mongoc_topology_description_validate_max_staleness --
 *
 *       If the provided "maxStalenessSeconds" component of the read
 *       preference is not valid for this topology, fill out @error and
 *       return false.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */
bool
_mongoc_topology_description_validate_max_staleness (const mongoc_topology_description_t *td,
                                                     int64_t max_staleness_seconds,
                                                     bson_error_t *error)
{
   mongoc_topology_description_type_t td_type;

   /* Server Selection Spec: A driver MUST raise an error if the TopologyType
    * is ReplicaSetWithPrimary or ReplicaSetNoPrimary and either of these
    * conditions is false:
    *
    * maxStalenessSeconds * 1000 >= heartbeatFrequencyMS + idleWritePeriodMS
    * maxStalenessSeconds >= smallestMaxStalenessSeconds
    */

   td_type = td->type;

   if (td_type != MONGOC_TOPOLOGY_RS_WITH_PRIMARY && td_type != MONGOC_TOPOLOGY_RS_NO_PRIMARY) {
      return true;
   }

   if (max_staleness_seconds * 1000 < td->heartbeat_msec + MONGOC_IDLE_WRITE_PERIOD_MS) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "maxStalenessSeconds is set to %" PRId64 ", it must be at least heartbeatFrequencyMS (%" PRId64
                      ") + server's idle write period (%d seconds)",
                      max_staleness_seconds,
                      td->heartbeat_msec,
                      MONGOC_IDLE_WRITE_PERIOD_MS / 1000);
      return false;
   }

   if (max_staleness_seconds < MONGOC_SMALLEST_MAX_STALENESS_SECONDS) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "maxStalenessSeconds is set to %" PRId64 ", it must be at least %d seconds",
                      max_staleness_seconds,
                      MONGOC_SMALLEST_MAX_STALENESS_SECONDS);
      return false;
   }

   return true;
}

static bool
_check_any_server_less_than_wire_version_13 (const void *sd_, void *any_too_old_)
{
   const mongoc_server_description_t *sd = sd_;
   bool *any_too_old = any_too_old_;
   if (sd->max_wire_version < WIRE_VERSION_5_0) {
      *any_too_old = true;
      return false /* Stop searching */;
   }

   return true /* Keep searching */;
}

/**
 * @brief Calculate the read mode that we should be using, based on what was
 * requested and what is available in the topology.
 *
 * Per the CRUD spec, if the requested read mode is *not* primary, and *any*
 * server in the topology has a wire version < server v5.0, we must override the
 * read mode preference with "primary." Server v5.0 indicates support on a
 * secondary server for using aggregate pipelines that contain writing stages
 * (i.e. '$out' and '$merge').
 */
static bool
_must_use_primary (const mongoc_topology_description_t *td,
                   mongoc_ss_optype_t optype,
                   mongoc_read_mode_t requested_read_mode)
{
   if (requested_read_mode == MONGOC_READ_PRIMARY) {
      /* We never alter from a primary read mode. This early-return is just an
       * optimization to skip scanning for old servers, as we would end up
       * returning MONGOC_READ_PRIMARY regardless. */
      return requested_read_mode;
   }
   switch (optype) {
   case MONGOC_SS_WRITE:
      /* We don't deal with write operations */
      return false;
   case MONGOC_SS_READ:
      /* Maintain the requested read mode if it is a regular read operation */
      return false;
   case MONGOC_SS_AGGREGATE_WITH_WRITE: {
      /* Check if any of the servers are too old to support the
       * aggregate-with-write on a secondary server */
      bool any_too_old = false;
      mongoc_set_for_each_const (mc_tpld_servers_const (td), _check_any_server_less_than_wire_version_13, &any_too_old);
      if (any_too_old) {
         /* Force the read preference back to reading from a primary server, as
          * one or more servers in the system may not support the operation */
         return true;
      }
      /* We're okay to send an aggr-with-write to a secondary server, so permit
       * the caller's read mode preference */
      return false;
   }
   default:
      BSON_UNREACHABLE ("Invalid mongoc_ss_optype_t for _must_use_primary()");
   }
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_description_suitable_servers --
 *
 *       Fill out an array of servers matching the read preference and
 *       localThresholdMS.
 *
 * Side effects:
 *       None.
 *
 *-------------------------------------------------------------------------
 */

void
mongoc_topology_description_suitable_servers (mongoc_array_t *set, /* OUT */
                                              mongoc_ss_optype_t optype,
                                              const mongoc_topology_description_t *topology,
                                              const mongoc_read_prefs_t *read_pref,
                                              bool *must_use_primary,
                                              const mongoc_deprioritized_servers_t *ds,
                                              int64_t local_threshold_ms)
{
   const mongoc_set_t *td_servers = mc_tpld_servers_const (topology);
   const mongoc_read_mode_t given_read_mode = mongoc_read_prefs_get_mode (read_pref);
   const bool override_use_primary = _must_use_primary (topology, optype, given_read_mode);

   mongoc_suitable_data_t data = {
      .primary = NULL,
      .topology_type = topology->type,
      .has_secondary = false,
      .candidates_len = 0,
      .candidates = bson_malloc0 (sizeof (mongoc_server_description_t *) * td_servers->items_len),
   };

   /* The "effective" read mode is the read mode that we should behave for, and
    * depends on the user's provided read mode, the type of operation that the
    * user wishes to perform, and the server versions that we are talking to.
    *
    * If the operation is a write operation, read mode is irrelevant.
    *
    * If the operation is a regular read, we just use the caller's read mode.
    *
    * If the operation is an aggregate that contains writing stages, we need to
    * be more careful about selecting an appropriate server.
    */
   data.read_mode = override_use_primary ? MONGOC_READ_PRIMARY : given_read_mode;
   if (must_use_primary) {
      /* The caller wants to know if we have overriden their read preference */
      *must_use_primary = override_use_primary;
   }

   /* Single server --
    * Either it is suitable or it isn't */
   if (topology->type == MONGOC_TOPOLOGY_SINGLE) {
      const mongoc_server_description_t *server = mongoc_set_get_item_const (td_servers, 0);
      if (_mongoc_topology_description_server_is_candidate (server->type, data.read_mode, topology->type)) {
         _mongoc_array_append_val (set, server);
      } else {
         TRACE ("Rejected [%s] [%s] for read mode [%s] with topology type Single",
                mongoc_server_description_type (server),
                server->host.host_and_port,
                _mongoc_read_mode_as_str (data.read_mode));
      }
      goto DONE;
   }

   /* Replica sets --
    * Find suitable servers based on read mode */
   if (topology->type == MONGOC_TOPOLOGY_RS_NO_PRIMARY || topology->type == MONGOC_TOPOLOGY_RS_WITH_PRIMARY) {
      switch (optype) {
      case MONGOC_SS_AGGREGATE_WITH_WRITE:
      case MONGOC_SS_READ: {
         mongoc_set_for_each_const (td_servers, _mongoc_replica_set_read_suitable_cb, &data);

         if (data.read_mode == MONGOC_READ_PRIMARY) {
            if (data.primary) {
               _mongoc_array_append_val (set, data.primary);
            }

            goto DONE;
         }

         if (data.read_mode == MONGOC_READ_PRIMARY_PREFERRED && data.primary) {
            _mongoc_array_append_val (set, data.primary);
            goto DONE;
         }

         if (data.read_mode == MONGOC_READ_SECONDARY_PREFERRED) {
            /* try read_mode SECONDARY */
            _mongoc_try_mode_secondary (set, topology, read_pref, must_use_primary, NULL, local_threshold_ms);

            /* otherwise fall back to primary */
            if (!set->len && data.primary) {
               _mongoc_array_append_val (set, data.primary);
            }

            goto DONE;
         }

         if (data.read_mode == MONGOC_READ_SECONDARY) {
            for (size_t i = 0u; i < data.candidates_len; i++) {
               if (data.candidates[i] && data.candidates[i]->type != MONGOC_SERVER_RS_SECONDARY) {
                  TRACE ("Rejected [%s] [%s] for mode [%s] with RS topology",
                         mongoc_server_description_type (data.candidates[i]),
                         data.candidates[i]->host.host_and_port,
                         _mongoc_read_mode_as_str (data.read_mode));
                  data.candidates[i] = NULL;
               }
            }
         }

         /* mode is SECONDARY or NEAREST, filter by staleness and tags */
         mongoc_server_description_filter_stale (
            data.candidates, data.candidates_len, data.primary, topology->heartbeat_msec, read_pref);

         mongoc_server_description_filter_tags (data.candidates, data.candidates_len, read_pref);
      } break;
      case MONGOC_SS_WRITE: {
         if (topology->type == MONGOC_TOPOLOGY_RS_WITH_PRIMARY) {
            mongoc_set_for_each_const (td_servers, _mongoc_topology_description_has_primary_cb, (void *) &data.primary);
            if (data.primary) {
               _mongoc_array_append_val (set, data.primary);
               goto DONE;
            }
         }
      } break;
      default:
         BSON_UNREACHABLE ("Invalid optype");
      }
   }

   // Sharded clusters --
   if (topology->type == MONGOC_TOPOLOGY_SHARDED) {
      mongoc_set_for_each_const (td_servers, _mongoc_td_servers_to_candidates_array, &data);

      if (ds) {
         _mongoc_filter_deprioritized_servers (&data, ds);
      }

      _mongoc_filter_suitable_mongos (&data);
   }

   /* Load balanced clusters --
    * Always select the only server. */
   if (topology->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      const mongoc_server_description_t *server;
      BSON_ASSERT (td_servers->items_len == 1);
      server = mongoc_set_get_item_const (td_servers, 0);
      _mongoc_array_append_val (set, server);
      goto DONE;
   }

   /* Ways to get here:
    *   - secondary read
    *   - secondary preferred read
    *   - primary_preferred and no primary read
    *   - sharded anything
    * Find the nearest, then select within the window */
   int64_t nearest = INT64_MAX;
   bool found = false;
   for (size_t i = 0u; i < data.candidates_len; i++) {
      if (data.candidates[i]) {
         nearest = BSON_MIN (nearest, data.candidates[i]->round_trip_time_msec);
         found = true;
      }
   }

   // No candidates remaining.
   if (!found) {
      goto DONE;
   }

   const int64_t rtt_limit = nearest + local_threshold_ms;

   for (size_t i = 0u; i < data.candidates_len; i++) {
      if (data.candidates[i] && (data.candidates[i]->round_trip_time_msec <= rtt_limit)) {
         _mongoc_array_append_val (set, data.candidates[i]);
      }
   }

DONE:

   bson_free ((mongoc_server_description_t *) data.candidates);

   return;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_has_data_node --
 *
 *      Internal method: are any servers not Arbiter, Ghost, or Unknown?
 *
 *--------------------------------------------------------------------------
 */
bool
mongoc_topology_description_has_data_node (const mongoc_topology_description_t *td)
{
   const mongoc_set_t *servers = mc_tpld_servers_const (td);

   for (size_t i = 0u; i < servers->items_len; i++) {
      const mongoc_server_description_t *sd = mongoc_set_get_item_const (servers, i);
      if (_is_data_node (sd)) {
         return true;
      }
   }

   return false;
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_topology_description_select --
 *
 *      Return a server description of a node that is appropriate for
 *      the given read preference and operation type.
 *
 *      NOTE: this method simply attempts to select a server from the
 *      current topology, it does not retry or trigger topology checks.
 *
 * Returns:
 *      Selected server description, or NULL upon failure.
 *
 * Side effects:
 *      None.
 *
 *-------------------------------------------------------------------------
 */
mongoc_server_description_t const *
mongoc_topology_description_select (const mongoc_topology_description_t *topology,
                                    mongoc_ss_optype_t optype,
                                    const mongoc_read_prefs_t *read_pref,
                                    bool *must_use_primary,
                                    const mongoc_deprioritized_servers_t *ds,
                                    int64_t local_threshold_ms)
{
   mongoc_array_t suitable_servers;

   ENTRY;

   if (topology->type == MONGOC_TOPOLOGY_SINGLE) {
      mongoc_server_description_t const *const sd = mongoc_set_get_item_const (mc_tpld_servers_const (topology), 0);

      if (optype == MONGOC_SS_AGGREGATE_WITH_WRITE && sd->max_wire_version < WIRE_VERSION_5_0) {
         /* The single server may be part of an unseen replica set that may not
          * support aggr-with-write operations on secondaries. Force the read
          * preference to use a primary. */
         if (must_use_primary) {
            *must_use_primary = true;
         }
      }

      if (sd->has_hello_response) {
         RETURN (sd);
      } else {
         TRACE ("Topology type single, [%s] is down", sd->host.host_and_port);
         RETURN (NULL);
      }
   }

   _mongoc_array_init (&suitable_servers, sizeof (mongoc_server_description_t *));

   mongoc_topology_description_suitable_servers (
      &suitable_servers, optype, topology, read_pref, must_use_primary, ds, local_threshold_ms);

   mongoc_server_description_t const *sd = NULL;

   if (suitable_servers.len != 0) {
      const int rand_n = _mongoc_rand_simple ((unsigned *) &topology->rand_seed);
      sd =
         _mongoc_array_index (&suitable_servers, mongoc_server_description_t *, (size_t) rand_n % suitable_servers.len);
   }

   _mongoc_array_destroy (&suitable_servers);

   if (sd) {
      TRACE ("Topology type [%s], selected [%s] [%s]",
             mongoc_topology_description_type (topology),
             mongoc_server_description_type (sd),
             sd->host.host_and_port);
   }

   RETURN (sd);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_server_by_id --
 *
 *       Get the server description for @id, if that server is present
 *       in @description. Otherwise, return NULL and fill out optional
 *       @error.
 *
 *       NOTE: In most cases, caller should create a duplicate of the
 *       returned server description.
 *
 * Returns:
 *       A mongoc_server_description_t *, or NULL.
 *
 * Side effects:
 *       Fills out optional @error if server not found.
 *
 *--------------------------------------------------------------------------
 */

mongoc_server_description_t *
mongoc_topology_description_server_by_id (mongoc_topology_description_t *description, uint32_t id, bson_error_t *error)
{
   return (mongoc_server_description_t *) mongoc_topology_description_server_by_id_const (description, id, error);
}

const mongoc_server_description_t *
mongoc_topology_description_server_by_id_const (const mongoc_topology_description_t *td,
                                                uint32_t id,
                                                bson_error_t *error)
{
   const mongoc_server_description_t *sd;

   BSON_ASSERT_PARAM (td);

   sd = mongoc_set_get_const (mc_tpld_servers_const (td), id);
   if (!sd) {
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NOT_ESTABLISHED, "Could not find description for node %u", id);
   }

   return sd;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_remove_server --
 *
 *       If present, remove this server from this topology description.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Removes the server description from topology and destroys it.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_remove_server (mongoc_topology_description_t *description,
                                            const mongoc_server_description_t *server)
{
   BSON_ASSERT (description);
   BSON_ASSERT (server);

   _mongoc_topology_description_monitor_server_closed (description, server);
   mongoc_set_rm (mc_tpld_servers (description), server->id);

   /* Check if removing server resulted in an empty set of servers */
   if (mc_tpld_servers_const (description)->items_len == 0) {
      MONGOC_WARNING ("Last server removed from topology");
   }
}

typedef struct _mongoc_address_and_id_t {
   const char *address; /* IN */
   bool found;          /* OUT */
   uint32_t id;         /* OUT */
} mongoc_address_and_id_t;

/* find the given server and stop iterating */
static bool
_mongoc_topology_description_has_server_cb (const void *item, void *ctx /* IN - OUT */)
{
   const mongoc_server_description_t *server = item;
   mongoc_address_and_id_t *data = (mongoc_address_and_id_t *) ctx;

   if (strcasecmp (data->address, server->connection_address) == 0) {
      data->found = true;
      data->id = server->id;
      return false;
   }
   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_has_set_version --
 *
 *       Whether @topology's max replica set version has been set.
 *
 * Returns:
 *       True if the max setVersion was ever set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_topology_description_has_set_version (mongoc_topology_description_t *td)
{
   return td->max_set_version != MONGOC_NO_SET_VERSION;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_topology_has_server --
 *
 *       Return true if @server is in @topology. If so, place its id in
 *       @id if given.
 *
 * Returns:
 *       True if server is in topology, false otherwise.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_topology_description_has_server (mongoc_topology_description_t *description,
                                         const char *address,
                                         uint32_t *id /* OUT */)
{
   mongoc_address_and_id_t data;

   BSON_ASSERT (description);
   BSON_ASSERT (address);

   data.address = address;
   data.found = false;
   mongoc_set_for_each_const (mc_tpld_servers_const (description), _mongoc_topology_description_has_server_cb, &data);

   if (data.found && id) {
      *id = data.id;
   }

   return data.found;
}

typedef struct _mongoc_address_and_type_t {
   const char *address;
   mongoc_server_description_type_t type;
} mongoc_address_and_type_t;

static bool
_mongoc_label_unknown_member_cb (void *item, void *ctx)
{
   mongoc_server_description_t *server = (mongoc_server_description_t *) item;
   mongoc_address_and_type_t *data = (mongoc_address_and_type_t *) ctx;

   if (strcasecmp (server->connection_address, data->address) == 0 && server->type == MONGOC_SERVER_UNKNOWN) {
      mongoc_server_description_set_state (server, data->type);
      return false;
   }
   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_label_unknown_member --
 *
 *       Find the server description with the given @address and if its
 *       type is UNKNOWN, set its type to @type.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_label_unknown_member (mongoc_topology_description_t *description,
                                                   const char *address,
                                                   mongoc_server_description_type_t type)
{
   mongoc_address_and_type_t data;

   BSON_ASSERT (description);
   BSON_ASSERT (address);

   data.type = type;
   data.address = address;

   mongoc_set_for_each (mc_tpld_servers (description), _mongoc_label_unknown_member_cb, &data);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_set_state --
 *
 *       Change the state of this cluster and unblock things waiting
 *       on a change of topology type.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Unblocks anything waiting on this description to change states.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_set_state (mongoc_topology_description_t *description,
                                        mongoc_topology_description_type_t type)
{
   description->type = type;
}


static void
_update_rs_type (mongoc_topology_description_t *topology)
{
   if (_mongoc_topology_description_has_primary (topology)) {
      _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_RS_WITH_PRIMARY);
   } else {
      _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_RS_NO_PRIMARY);
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_check_if_has_primary --
 *
 *       If there is a primary in topology, set topology
 *       type to RS_WITH_PRIMARY, otherwise set it to
 *       RS_NO_PRIMARY.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Changes the topology type.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_check_if_has_primary (mongoc_topology_description_t *topology,
                                                   const mongoc_server_description_t *server)
{
   BSON_UNUSED (server);

   _update_rs_type (topology);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_invalidate_server --
 *
 *      Invalidate a server if a network error occurred while using it in
 *      another part of the client. Server description is set to type
 *      UNKNOWN, the error is recorded, and other parameters are reset to
 *      defaults. Pass in the reason for invalidation in @error.
 *
 * @todo Try to remove this function when CDRIVER-3654 is complete.
 * It is only called when an application thread needs to mark a server Unknown.
 * But an application error is also tied to other behavior, and should also
 * consider the connection generation. This logic is captured in
 * _mongoc_topology_handle_app_error. This should not be called directly
 */
void
mongoc_topology_description_invalidate_server (mongoc_topology_description_t *td,
                                               uint32_t id,
                                               const bson_error_t *error /* IN */)
{
   BSON_ASSERT (error);

   if (td->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* Load balancers must never be marked unknown. */
      return;
   }

   /* send NULL hello reply */
   mongoc_topology_description_handle_hello (td, id, NULL, MONGOC_RTT_UNSET, error);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_add_server --
 *
 *       Add the specified server to the cluster topology if it is not
 *       already a member. If @id, place its id in @id.
 *
 * Return:
 *       True if the server was added or already existed in the topology,
 *       false if an error occurred.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
mongoc_topology_description_add_server (mongoc_topology_description_t *topology,
                                        const char *server,
                                        uint32_t *id /* OUT */)
{
   uint32_t server_id;
   mongoc_server_description_t *description;

   BSON_ASSERT (topology);
   BSON_ASSERT (server);

   if (!_mongoc_topology_description_has_server (topology, server, &server_id)) {
      /* TODO this might not be an accurate count in all cases */
      server_id = ++topology->max_server_id;

      description = BSON_ALIGNED_ALLOC0 (mongoc_server_description_t);
      mongoc_server_description_init (description, server, server_id);

      mongoc_set_add (mc_tpld_servers (topology), server_id, description);

      /* if we're in topology_new then no callbacks are registered and this is
       * a no-op. later, if we discover a new RS member this sends an event. */
      _mongoc_topology_description_monitor_server_opening (topology, description);
   }

   if (id) {
      *id = server_id;
   }

   return true;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_update_cluster_time --
 *
 *  Drivers Session Spec: Drivers MUST examine responses to server commands to
 *  see if they contain a top level field named $clusterTime formatted as
 *  follows:
 *
 *  {
 *      ...
 *      $clusterTime : {
 *          clusterTime : <BsonTimestamp>,
 *          signature : {
 *              hash : <BsonBinaryData>,
 *              keyId : <BsonInt64>
 *          }
 *      },
 *      ...
 *  }
 *
 *  Whenever a driver receives a clusterTime from a server it MUST compare it
 *  to the current highest seen clusterTime for the cluster. If the new
 *  clusterTime is higher than the highest seen clusterTime it MUST become
 *  the new highest seen clusterTime. Two clusterTimes are compared using
 *  only the BsonTimestamp value of the clusterTime embedded field (be sure to
 *  include both the timestamp and the increment of the BsonTimestamp in the
 *  comparison). The signature field does not participate in the comparison.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_topology_description_update_cluster_time (mongoc_topology_description_t *td, const bson_t *reply)
{
   bson_iter_t iter;
   bson_iter_t child;
   const uint8_t *data;
   uint32_t size;
   bson_t cluster_time;

   if (!reply || !bson_iter_init_find (&iter, reply, "$clusterTime")) {
      return;
   }

   if (!BSON_ITER_HOLDS_DOCUMENT (&iter) || !bson_iter_recurse (&iter, &child)) {
      MONGOC_ERROR ("Can't parse $clusterTime");
      return;
   }

   bson_iter_document (&iter, &size, &data);
   BSON_ASSERT (bson_init_static (&cluster_time, data, (size_t) size));

   if (bson_empty (&td->cluster_time) || _mongoc_cluster_time_greater (&cluster_time, &td->cluster_time)) {
      bson_destroy (&td->cluster_time);
      bson_copy_to (&cluster_time, &td->cluster_time);
   }
}


static void
_mongoc_topology_description_add_new_servers (mongoc_topology_description_t *topology,
                                              const mongoc_server_description_t *server)
{
   bson_iter_t member_iter;
   const bson_t *rs_members[3];
   int i;

   rs_members[0] = &server->hosts;
   rs_members[1] = &server->arbiters;
   rs_members[2] = &server->passives;

   for (i = 0; i < 3; i++) {
      BSON_ASSERT (bson_iter_init (&member_iter, rs_members[i]));

      while (bson_iter_next (&member_iter)) {
         mongoc_topology_description_add_server (topology, bson_iter_utf8 (&member_iter, NULL), NULL);
      }
   }
}

typedef struct _mongoc_primary_and_topology_t {
   mongoc_topology_description_t *topology;
   const mongoc_server_description_t *primary;
} mongoc_primary_and_topology_t;

/* invalidate old primaries */
static bool
_mongoc_topology_description_invalidate_primaries_cb (void *item, void *ctx)
{
   mongoc_server_description_t *server = (mongoc_server_description_t *) item;
   mongoc_primary_and_topology_t *data = (mongoc_primary_and_topology_t *) ctx;

   if (server->id != data->primary->id && server->type == MONGOC_SERVER_RS_PRIMARY) {
      mongoc_server_description_set_state (server, MONGOC_SERVER_UNKNOWN);
      mongoc_server_description_set_set_version (server, MONGOC_NO_SET_VERSION);
      mongoc_server_description_set_election_id (server, NULL);
      mongoc_server_description_reset (server);
   }
   return true;
}


/* Remove and destroy all replica set members not in primary's hosts lists */
static void
_mongoc_topology_description_remove_unreported_servers (mongoc_topology_description_t *topology,
                                                        const mongoc_server_description_t *primary)
{
   mongoc_array_t to_remove;

   _mongoc_array_init (&to_remove, sizeof (mongoc_server_description_t *));

   /* Accumulate servers to be removed - do this before calling
    * _mongoc_topology_description_remove_server, which could call
    * mongoc_server_description_cleanup on the primary itself if it
    * doesn't report its own connection_address in its hosts list.
    * See hosts_differ_from_seeds.json */
   for (size_t i = 0u; i < mc_tpld_servers_const (topology)->items_len; i++) {
      const mongoc_server_description_t *member = mongoc_set_get_item_const (mc_tpld_servers_const (topology), i);
      const char *address = member->connection_address;
      if (!mongoc_server_description_has_rs_member (primary, address)) {
         _mongoc_array_append_val (&to_remove, member);
      }
   }

   /* now it's safe to call _mongoc_topology_description_remove_server,
    * even on the primary */
   for (size_t i = 0u; i < to_remove.len; i++) {
      const mongoc_server_description_t *member = _mongoc_array_index (&to_remove, mongoc_server_description_t *, i);

      _mongoc_topology_description_remove_server (topology, member);
   }

   _mongoc_array_destroy (&to_remove);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_matches_me --
 *
 *       Server Discovery And Monitoring Spec: "Removal from the topology of
 *       seed list members where the "me" property does not match the address
 *       used to connect prevents clients from being able to select a server,
 *       only to fail to re-select that server once the primary has responded.
 *
 * Returns:
 *       True if "me" matches "connection_address".
 *
 * Side Effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
static bool
_mongoc_topology_description_matches_me (const mongoc_server_description_t *server)
{
   BSON_ASSERT (server->connection_address);

   if (!server->me) {
      /* "me" is unknown: consider it a match */
      return true;
   }

   return strcasecmp (server->connection_address, server->me) == 0;
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_update_rs_from_primary --
 *
 *       First, determine that this is really the primary:
 *          -If this node isn't in the cluster, do nothing.
 *          -If the cluster's set name is null, set it to node's set name.
 *           Otherwise if the cluster's set name is different from node's,
 *           we found a rogue primary, so remove it from the cluster and
 *           check the cluster for a primary, then return.
 *          -If any of the members of cluster reports an address different
 *           from node's, node cannot be the primary.
 *       Now that we know this is the primary:
 *          -If any hosts, passives, or arbiters in node's description aren't
 *           in the cluster, add them as UNKNOWN servers.
 *          -If the cluster has any servers that aren't in node's description,
 *           remove and destroy them.
 *       Finally, check the cluster for the new primary.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Changes to the cluster, possible removal of cluster nodes.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_update_rs_from_primary (mongoc_topology_description_t *topology,
                                                     const mongoc_server_description_t *server)
{
   mongoc_primary_and_topology_t data;
   bson_error_t error;

   BSON_ASSERT (topology);
   BSON_ASSERT (server);

   if (!_mongoc_topology_description_has_server (topology, server->connection_address, NULL))
      return;

   /* If server->set_name was null this function wouldn't be called from
    * mongoc_server_description_handle_hello(). static code analyzers however
    * don't know that so we check for it explicitly. */
   if (server->set_name) {
      /* 'Server' can only be the primary if it has the right rs name  */

      if (!topology->set_name) {
         topology->set_name = bson_strdup (server->set_name);
      } else if (strcmp (topology->set_name, server->set_name) != 0) {
         _mongoc_topology_description_remove_server (topology, server);
         _update_rs_type (topology);
         return;
      }
   }
   if (server->max_wire_version >= WIRE_VERSION_6_0) {
      /* MongoDB 6.0+ */
      if (_mongoc_server_description_primary_is_not_stale (topology, server)) {
         _mongoc_topology_description_set_max_election_id (topology, server);
         _mongoc_topology_description_set_max_set_version (topology, server);

      } else {
         bson_set_error (
            &error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "member's setVersion or electionId is stale");
         mongoc_topology_description_invalidate_server (topology, server->id, &error);
         _update_rs_type (topology);
         return;
      }
   } else {
      // old comparison rules, namely setVersion is checked before electionId
      if (mongoc_server_description_has_set_version (server) && mongoc_server_description_has_election_id (server)) {
         /* Server Discovery And Monitoring Spec: "The client remembers the
          * greatest electionId reported by a primary, and distrusts primaries
          * with lesser electionIds. This prevents the client from oscillating
          * between the old and new primary during a split-brain period."
          */
         if (_mongoc_topology_description_later_election (topology, server)) {
            // stale primary code return:
            bson_set_error (
               &error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "member's setVersion or electionId is stale");
            mongoc_topology_description_invalidate_server (topology, server->id, &error);
            _update_rs_type (topology);
            return;
         }

         /* server's electionId >= topology's max electionId */
         _mongoc_topology_description_set_max_election_id (topology, server);
      }

      if (mongoc_server_description_has_set_version (server) &&
          (!_mongoc_topology_description_has_set_version (topology) ||
           server->set_version > topology->max_set_version)) {
         _mongoc_topology_description_set_max_set_version (topology, server);
      }
   }
   /* 'Server' is the primary! Invalidate other primaries if found */
   data.primary = server;
   data.topology = topology;
   mongoc_set_for_each (mc_tpld_servers (topology), _mongoc_topology_description_invalidate_primaries_cb, &data);

   /* Add to topology description any new servers primary knows about */
   _mongoc_topology_description_add_new_servers (topology, server);

   /* Remove from topology description any servers primary doesn't know about */
   _mongoc_topology_description_remove_unreported_servers (topology, server);

   /* Finally, set topology type */
   _update_rs_type (topology);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_update_rs_without_primary --
 *
 *       Update cluster's information when there is no primary.
 *
 * Returns:
 *       None.
 *
 * Side Effects:
 *       Alters cluster state, may remove node from cluster.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_update_rs_without_primary (mongoc_topology_description_t *topology,
                                                        const mongoc_server_description_t *server)
{
   BSON_ASSERT (topology);
   BSON_ASSERT (server);

   if (!_mongoc_topology_description_has_server (topology, server->connection_address, NULL)) {
      return;
   }

   /* make sure we're talking about the same replica set */
   if (server->set_name) {
      if (!topology->set_name) {
         topology->set_name = bson_strdup (server->set_name);
      } else if (strcmp (topology->set_name, server->set_name) != 0) {
         _mongoc_topology_description_remove_server (topology, server);
         return;
      }
   }

   /* Add new servers that this replica set member knows about */
   _mongoc_topology_description_add_new_servers (topology, server);

   /* If this server thinks there is a primary, label it POSSIBLE_PRIMARY */
   if (server->current_primary) {
      _mongoc_topology_description_label_unknown_member (
         topology, server->current_primary, MONGOC_SERVER_POSSIBLE_PRIMARY);
   }

   if (!_mongoc_topology_description_matches_me (server)) {
      _mongoc_topology_description_remove_server (topology, server);
      return;
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_update_rs_with_primary_from_member --
 *
 *       Update cluster's information when there is a primary, but the
 *       update is coming from another replica set member.
 *
 * Returns:
 *       None.
 *
 * Side Effects:
 *       Alters cluster state.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_update_rs_with_primary_from_member (mongoc_topology_description_t *topology,
                                                                 const mongoc_server_description_t *server)
{
   BSON_ASSERT (topology);
   BSON_ASSERT (server);

   if (!_mongoc_topology_description_has_server (topology, server->connection_address, NULL)) {
      return;
   }

   /* set_name should never be null here */
   if (strcmp (topology->set_name, server->set_name) != 0) {
      _mongoc_topology_description_remove_server (topology, server);
      _update_rs_type (topology);
      return;
   }

   if (!_mongoc_topology_description_matches_me (server)) {
      _mongoc_topology_description_remove_server (topology, server);
      return;
   }

   /* If there is no primary, label server's current_primary as the
    * POSSIBLE_PRIMARY */
   if (!_mongoc_topology_description_has_primary (topology) && server->current_primary) {
      _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_RS_NO_PRIMARY);
      _mongoc_topology_description_label_unknown_member (
         topology, server->current_primary, MONGOC_SERVER_POSSIBLE_PRIMARY);
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_set_topology_type_to_sharded --
 *
 *       Sets topology's type to SHARDED.
 *
 * Returns:
 *       None
 *
 * Side effects:
 *       Alter's topology's type
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_set_topology_type_to_sharded (mongoc_topology_description_t *topology,
                                                           const mongoc_server_description_t *server)
{
   BSON_UNUSED (server);

   _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_SHARDED);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_transition_unknown_to_rs_no_primary --
 *
 *       Encapsulates transition from cluster state UNKNOWN to
 *       RS_NO_PRIMARY. Sets the type to RS_NO_PRIMARY,
 *       then updates the replica set accordingly.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Changes topology state.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_transition_unknown_to_rs_no_primary (mongoc_topology_description_t *topology,
                                                                  const mongoc_server_description_t *server)
{
   _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_RS_NO_PRIMARY);
   _mongoc_topology_description_update_rs_without_primary (topology, server);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_remove_and_check_primary --
 *
 *       Remove the server and check if the topology still has a primary.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Removes server from topology and destroys it.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_remove_and_check_primary (mongoc_topology_description_t *topology,
                                                       const mongoc_server_description_t *server)
{
   _mongoc_topology_description_remove_server (topology, server);
   _update_rs_type (topology);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_update_unknown_with_standalone --
 *
 *       If the cluster doesn't contain this server, do nothing.
 *       Otherwise, if the topology only has one seed, change its
 *       type to SINGLE. If the topology has multiple seeds, it does not
 *       include us, so remove this server and destroy it.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Changes the topology type, might remove server from topology.
 *
 *--------------------------------------------------------------------------
 */
static void
_mongoc_topology_description_update_unknown_with_standalone (mongoc_topology_description_t *topology,
                                                             const mongoc_server_description_t *server)
{
   BSON_ASSERT (topology);
   BSON_ASSERT (server);

   if (!_mongoc_topology_description_has_server (topology, server->connection_address, NULL))
      return;

   if (mc_tpld_servers_const (topology)->items_len > 1) {
      /* This cluster contains other servers, it cannot be a standalone. */
      _mongoc_topology_description_remove_server (topology, server);
   } else {
      _mongoc_topology_description_set_state (topology, MONGOC_TOPOLOGY_SINGLE);
   }
}

/*
 *--------------------------------------------------------------------------
 *
 *  This table implements the 'ToplogyType' table outlined in the Server
 *  Discovery and Monitoring spec. Each row represents a server type,
 *  and each column represents the topology type. Given a current topology
 *  type T and a newly-observed server type S, use the function at
 *  state_transions[S][T] to transition to a new state.
 *
 *  Rows should be read like so:
 *  { server type for this row
 *     UNKNOWN,
 *     SHARDED,
 *     RS_NO_PRIMARY,
 *     RS_WITH_PRIMARY
 *  }
 *
 *--------------------------------------------------------------------------
 */

typedef void (*transition_t) (mongoc_topology_description_t *topology, const mongoc_server_description_t *server);

transition_t gSDAMTransitionTable[MONGOC_SERVER_DESCRIPTION_TYPES][MONGOC_TOPOLOGY_DESCRIPTION_TYPES] = {
   {
      /* UNKNOWN */
      NULL,                                             /* MONGOC_TOPOLOGY_UNKNOWN */
      NULL,                                             /* MONGOC_TOPOLOGY_SHARDED */
      NULL,                                             /* MONGOC_TOPOLOGY_RS_NO_PRIMARY */
      _mongoc_topology_description_check_if_has_primary /* MONGOC_TOPOLOGY_RS_WITH_PRIMARY
                                                         */
   },
   {/* STANDALONE */
    _mongoc_topology_description_update_unknown_with_standalone,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_remove_and_check_primary},
   {/* MONGOS */
    _mongoc_topology_description_set_topology_type_to_sharded,
    NULL,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_remove_and_check_primary},
   {/* POSSIBLE_PRIMARY */
    NULL,
    NULL,
    NULL,
    NULL},
   {/* PRIMARY */
    _mongoc_topology_description_update_rs_from_primary,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_update_rs_from_primary,
    _mongoc_topology_description_update_rs_from_primary},
   {/* SECONDARY */
    _mongoc_topology_description_transition_unknown_to_rs_no_primary,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_update_rs_without_primary,
    _mongoc_topology_description_update_rs_with_primary_from_member},
   {/* ARBITER */
    _mongoc_topology_description_transition_unknown_to_rs_no_primary,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_update_rs_without_primary,
    _mongoc_topology_description_update_rs_with_primary_from_member},
   {/* RS_OTHER */
    _mongoc_topology_description_transition_unknown_to_rs_no_primary,
    _mongoc_topology_description_remove_server,
    _mongoc_topology_description_update_rs_without_primary,
    _mongoc_topology_description_update_rs_with_primary_from_member},
   {/* RS_GHOST */
    NULL,
    _mongoc_topology_description_remove_server,
    NULL,
    _mongoc_topology_description_check_if_has_primary}};

/*
 *--------------------------------------------------------------------------
 *
 * _tpld_type_str --
 *
 *      Get this topology's type, one of the types defined in the Server
 *      Discovery And Monitoring Spec.
 *
 * Returns:
 *       A string.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
static const char *
_tpld_type_str (mongoc_topology_description_type_t type)
{
   switch (type) {
   case MONGOC_TOPOLOGY_UNKNOWN:
      return "Unknown";
   case MONGOC_TOPOLOGY_SHARDED:
      return "Sharded";
   case MONGOC_TOPOLOGY_RS_NO_PRIMARY:
      return "RSNoPrimary";
   case MONGOC_TOPOLOGY_RS_WITH_PRIMARY:
      return "RSWithPrimary";
   case MONGOC_TOPOLOGY_SINGLE:
      return "Single";
   case MONGOC_TOPOLOGY_LOAD_BALANCED:
      return "LoadBalanced";
   case MONGOC_TOPOLOGY_DESCRIPTION_TYPES:
   default:
      MONGOC_ERROR ("Invalid mongoc_topology_description_type_t type");
      return "Invalid";
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_update_session_timeout --
 *
 *      Fill out td.session_timeout_minutes.
 *
 *      Server Discovery and Monitoring Spec: "set logicalSessionTimeoutMinutes
 *      to the smallest logicalSessionTimeoutMinutes value among all
 *      ServerDescriptions of known ServerType. If any ServerDescription of
 *      known ServerType has a null logicalSessionTimeoutMinutes, then
 *      logicalSessionTimeoutMinutes MUST be set to null."
 *
 * --------------------------------------------------------------------------
 */

static void
_mongoc_topology_description_update_session_timeout (mongoc_topology_description_t *td)
{
   mongoc_set_t *set;
   mongoc_server_description_t *sd;

   set = mc_tpld_servers (td);

   td->session_timeout_minutes = MONGOC_NO_SESSIONS;

   for (size_t i = 0; i < set->items_len; i++) {
      sd = (mongoc_server_description_t *) mongoc_set_get_item (set, i);
      if (!_is_data_node (sd)) {
         continue;
      }

      if (sd->session_timeout_minutes == MONGOC_NO_SESSIONS) {
         td->session_timeout_minutes = MONGOC_NO_SESSIONS;
         return;
      } else if (td->session_timeout_minutes == MONGOC_NO_SESSIONS) {
         td->session_timeout_minutes = sd->session_timeout_minutes;
      } else if (td->session_timeout_minutes > sd->session_timeout_minutes) {
         td->session_timeout_minutes = sd->session_timeout_minutes;
      }
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_check_compatible --
 *
 *      Fill out td.compatibility_error if any server's wire versions do
 *      not overlap with ours. Otherwise clear td.compatibility_error.
 *
 *      If any server is incompatible, the topology as a whole is considered
 *      incompatible.
 *
 *--------------------------------------------------------------------------
 */

static void
_mongoc_topology_description_check_compatible (mongoc_topology_description_t *td)
{
   mongoc_set_t const *const servers = mc_tpld_servers_const (td);

   memset (&td->compatibility_error, 0, sizeof (bson_error_t));

   for (size_t i = 0; i < servers->items_len; i++) {
      mongoc_server_description_t const *const sd = mongoc_set_get_item_const (servers, i);
      if (sd->type == MONGOC_SERVER_UNKNOWN || sd->type == MONGOC_SERVER_POSSIBLE_PRIMARY) {
         continue;
      }

      if (sd->min_wire_version > WIRE_VERSION_MAX) {
         bson_set_error (&td->compatibility_error,
                         MONGOC_ERROR_PROTOCOL,
                         MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION,
                         "Server at %s requires wire version %d,"
                         " but this version of libmongoc only supports up to %d",
                         sd->host.host_and_port,
                         sd->min_wire_version,
                         WIRE_VERSION_MAX);
      } else if (sd->max_wire_version < WIRE_VERSION_MIN) {
         bson_set_error (&td->compatibility_error,
                         MONGOC_ERROR_PROTOCOL,
                         MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION,
                         "Server at %s reports wire version %d, but this"
                         " version of libmongoc requires at least %d (MongoDB %s)",
                         sd->host.host_and_port,
                         sd->max_wire_version,
                         WIRE_VERSION_MIN,
                         _mongoc_wire_version_to_server_version (WIRE_VERSION_MIN));
      }
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_handle_hello --
 *
 *      Handle a hello. This is called by the background SDAM process,
 *      and by client when performing a handshake or invalidating servers.
 *      If there was an error calling hello, pass it in as @error.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_topology_description_handle_hello (mongoc_topology_description_t *topology,
                                          uint32_t server_id,
                                          const bson_t *hello_response,
                                          int64_t rtt_msec,
                                          const bson_error_t *error /* IN */)
{
   mongoc_topology_description_t *prev_td = NULL;
   mongoc_server_description_t *prev_sd = NULL;
   mongoc_server_description_t *sd;
   bson_iter_t iter;
   /* sd_changed is set if the server description meaningfully changed AND
    * callbacks are registered. */
   bool sd_changed = false;

   BSON_ASSERT (topology);
   BSON_ASSERT (server_id != 0);

   sd = mongoc_topology_description_server_by_id (topology, server_id, NULL);
   if (!sd) {
      return; /* server already removed from topology */
   }

   if (topology->apm_callbacks.topology_changed) {
      prev_td = BSON_ALIGNED_ALLOC0 (mongoc_topology_description_t);
      _mongoc_topology_description_copy_to (topology, prev_td);
   }

   if (hello_response && bson_iter_init_find (&iter, hello_response, "topologyVersion") &&
       BSON_ITER_HOLDS_DOCUMENT (&iter)) {
      bson_t incoming_topology_version;
      const uint8_t *bytes;
      uint32_t len;

      bson_iter_document (&iter, &len, &bytes);
      BSON_ASSERT (bson_init_static (&incoming_topology_version, bytes, len));

      if (mongoc_server_description_topology_version_cmp (&sd->topology_version, &incoming_topology_version) == 1) {
         TRACE ("%s", "topology version is strictly less. Skipping.");
         if (prev_td) {
            mongoc_topology_description_cleanup (prev_td);
            bson_free (prev_td);
         }
         return;
      }
   }

   if (topology->apm_callbacks.topology_changed || topology->apm_callbacks.server_changed) {
      /* Only copy the previous server description if a monitoring callback is
       * registered. */
      prev_sd = mongoc_server_description_new_copy (sd);
   }

   DUMP_BSON (hello_response);
   /* pass the current error in */

   mongoc_server_description_handle_hello (sd, hello_response, rtt_msec, error);

   /* if the user specified a set_name in the connection string
    * and they are in topology type single, check that the set name
    * matches. */
   if (topology->set_name && topology->type == MONGOC_TOPOLOGY_SINGLE) {
      bool wrong_set_name = false;
      bson_error_t set_name_err = {0};

      if (!sd->set_name) {
         wrong_set_name = true;
         bson_set_error (&set_name_err,
                         MONGOC_ERROR_SERVER_SELECTION,
                         MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                         "no reported set name, but expected '%s'",
                         topology->set_name);
      } else if (0 != strcmp (sd->set_name, topology->set_name)) {
         wrong_set_name = true;
         bson_set_error (&set_name_err,
                         MONGOC_ERROR_SERVER_SELECTION,
                         MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                         "reported set name '%s' does not match '%s'",
                         sd->set_name,
                         topology->set_name);
      }

      if (wrong_set_name) {
         /* Replace with unknown. */
         TRACE ("%s", "wrong set name");
         mongoc_server_description_handle_hello (sd, NULL, MONGOC_RTT_UNSET, &set_name_err);
      }
   }

   mongoc_topology_description_update_cluster_time (topology, hello_response);

   if (prev_sd) {
      sd_changed = !_mongoc_server_description_equal (prev_sd, sd);
   }
   if (sd_changed) {
      _mongoc_topology_description_monitor_server_changed (topology, prev_sd, sd);
   }

   if (gSDAMTransitionTable[sd->type][topology->type]) {
      TRACE ("Topology description %s handling server description %s",
             _tpld_type_str (topology->type),
             mongoc_server_description_type (sd));
      gSDAMTransitionTable[sd->type][topology->type](topology, sd);
   } else {
      TRACE ("Topology description %s ignoring server description %s",
             _tpld_type_str (topology->type),
             mongoc_server_description_type (sd));
   }

   _mongoc_topology_description_update_session_timeout (topology);

   /* Don't bother checking wire version compatibility if we already errored */
   if (hello_response && (!error || !error->code)) {
      _mongoc_topology_description_check_compatible (topology);
   }

   /* If server description did not change, then neither did topology
    * description */
   if (sd_changed) {
      _mongoc_topology_description_monitor_changed (prev_td, topology);
   }

   if (prev_td) {
      mongoc_topology_description_cleanup (prev_td);
      bson_free (prev_td);
   }

   mongoc_server_description_destroy (prev_sd);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_has_readable_server --
 *
 *      SDAM Monitoring Spec:
 *      "Determines if the topology has a readable server available."
 *
 *      NOTE: this method should only be called by user code in an SDAM
 *      Monitoring callback.
 *
 *--------------------------------------------------------------------------
 */
bool
mongoc_topology_description_has_readable_server (const mongoc_topology_description_t *td,
                                                 const mongoc_read_prefs_t *prefs)
{
   bson_error_t error;

   if (!mongoc_topology_compatible (td, NULL, &error)) {
      return false;
   }

   /* local threshold argument doesn't matter */
   return mongoc_topology_description_select (td, MONGOC_SS_READ, prefs, NULL, NULL, 0) != NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_has_writable_server --
 *
 *      SDAM Monitoring Spec:
 *      "Determines if the topology has a writable server available."
 *
 *      NOTE: this method should only be called by user code in an SDAM
 *      Monitoring callback.
 *
 *--------------------------------------------------------------------------
 */
bool
mongoc_topology_description_has_writable_server (const mongoc_topology_description_t *td)
{
   bson_error_t error;

   if (!mongoc_topology_compatible (td, NULL, &error)) {
      return false;
   }

   return mongoc_topology_description_select (td, MONGOC_SS_WRITE, NULL, NULL, NULL, 0) != NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_type --
 *
 *      Get this topology's type, one of the types defined in the Server
 *      Discovery And Monitoring Spec.
 *
 *      NOTE: this method should only be called by user code in an SDAM
 *      Monitoring callback.
 *
 * Returns:
 *      A string.
 *
 *--------------------------------------------------------------------------
 */
const char *
mongoc_topology_description_type (const mongoc_topology_description_t *td)
{
   switch (td->type) {
   case MONGOC_TOPOLOGY_UNKNOWN:
      return "Unknown";
   case MONGOC_TOPOLOGY_SHARDED:
      return "Sharded";
   case MONGOC_TOPOLOGY_RS_NO_PRIMARY:
      return "ReplicaSetNoPrimary";
   case MONGOC_TOPOLOGY_RS_WITH_PRIMARY:
      return "ReplicaSetWithPrimary";
   case MONGOC_TOPOLOGY_SINGLE:
      return "Single";
   case MONGOC_TOPOLOGY_LOAD_BALANCED:
      return "LoadBalanced";
   case MONGOC_TOPOLOGY_DESCRIPTION_TYPES:
   default:
      fprintf (stderr, "ERROR: Unknown topology type %d\n", td->type);
      BSON_ASSERT (0);
   }

   return NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_topology_description_get_servers --
 *
 *      Fetch an array of server descriptions for all known servers in the
 *      topology.
 *
 * Returns:
 *      An array you must free with mongoc_server_descriptions_destroy_all.
 *
 *--------------------------------------------------------------------------
 */
mongoc_server_description_t **
mongoc_topology_description_get_servers (const mongoc_topology_description_t *td, size_t *n /* OUT */)
{
   const mongoc_set_t *const set = mc_tpld_servers_const (BSON_ASSERT_PTR_INLINE (td));
   /* enough room for all descriptions, even if some are unknown  */
   mongoc_server_description_t **sds = bson_malloc0 (sizeof (mongoc_server_description_t *) * set->items_len);

   BSON_ASSERT_PARAM (n);

   *n = 0;

   for (size_t i = 0; i < set->items_len; ++i) {
      const mongoc_server_description_t *sd = mongoc_set_get_item_const (set, i);

      if (sd->type != MONGOC_SERVER_UNKNOWN) {
         sds[*n] = mongoc_server_description_new_copy (sd);
         ++(*n);
      }
   }

   return sds;
}

typedef struct {
   mongoc_host_list_t *host_list;
   size_t num_missing;
} _count_num_hosts_to_remove_ctx_t;

static bool
_count_num_hosts_to_remove (void *sd_void, void *ctx_void)
{
   mongoc_server_description_t *sd;
   _count_num_hosts_to_remove_ctx_t *ctx;
   mongoc_host_list_t *host_list;

   sd = sd_void;
   ctx = ctx_void;
   host_list = ctx->host_list;

   if (!_mongoc_host_list_contains_one (host_list, &sd->host)) {
      ++ctx->num_missing;
   }

   return true;
}

typedef struct {
   mongoc_host_list_t *host_list;
   mongoc_topology_description_t *td;
} _remove_if_not_in_host_list_ctx_t;

static bool
_remove_if_not_in_host_list_cb (void *sd_void, void *ctx_void)
{
   _remove_if_not_in_host_list_ctx_t *ctx;
   mongoc_topology_description_t *td;
   mongoc_server_description_t *sd;
   mongoc_host_list_t *host_list;

   ctx = ctx_void;
   sd = sd_void;
   host_list = ctx->host_list;
   td = ctx->td;

   if (_mongoc_host_list_contains_one (host_list, &sd->host)) {
      return true;
   }
   _mongoc_topology_description_remove_server (td, sd);
   return true;
}

void
mongoc_topology_description_reconcile (mongoc_topology_description_t *td, mongoc_host_list_t *host_list)
{
   mongoc_set_t *servers;
   size_t host_list_length;
   size_t num_missing;

   BSON_ASSERT_PARAM (td);

   servers = mc_tpld_servers (td);
   host_list_length = _mongoc_host_list_length (host_list);

   /* Avoid removing all servers in topology, even temporarily, by deferring
    * actual removal until after new hosts have been added. */
   {
      _count_num_hosts_to_remove_ctx_t ctx;

      ctx.host_list = host_list;
      ctx.num_missing = 0u;

      mongoc_set_for_each (servers, _count_num_hosts_to_remove, &ctx);

      num_missing = ctx.num_missing;
   }

   /* Polling SRV Records for mongos Discovery Spec: If srvMaxHosts is zero or
    * greater than or equal to the number of valid hosts, each valid new host
    * MUST be added to the topology as Unknown. */
   if (td->max_hosts == 0 || (size_t) td->max_hosts >= host_list_length) {
      mongoc_host_list_t *host;

      LL_FOREACH (host_list, host)
      {
         /* "add" is really an "upsert" */
         mongoc_topology_description_add_server (td, host->host_and_port, NULL);
      }
   }

   /* Polling SRV Records for mongos Discovery Spec: If srvMaxHosts is greater
    * than zero and less than the number of valid hosts, valid new hosts MUST be
    * randomly selected and added to the topology as Unknown until the topology
    * has srvMaxHosts hosts. */
   else {
      const size_t max_with_missing = td->max_hosts + num_missing;

      size_t idx = 0u;
      size_t hl_array_size = 0u;

      /* Polling SRV Records for mongos Discovery Spec: Drivers MUST use the
       * same randomization algorithm as they do for initial selection.
       * Do not limit size of results yet (pass host_list_length) as we want to
       * update any existing hosts in the topology, but add new hosts.
       */
      const mongoc_host_list_t *const *hl_array =
         _mongoc_apply_srv_max_hosts (host_list, host_list_length, &hl_array_size);

      for (idx = 0u; servers->items_len < max_with_missing && idx < hl_array_size; ++idx) {
         const mongoc_host_list_t *const elem = hl_array[idx];

         /* "add" is really an "upsert" */
         mongoc_topology_description_add_server (td, elem->host_and_port, NULL);
      }

      /* There should not be a situation where all items in the valid host list
       * were traversed without the number of hosts in the topology reaching
       * srvMaxHosts. */
      BSON_ASSERT (servers->items_len == max_with_missing);

      bson_free ((void *) hl_array);
   }

   /* Polling SRV Records for mongos Discovery Spec: For all verified host
    * names, as returned through the DNS SRV query, the driver MUST remove
    * all hosts that are part of the topology, but are no longer in the
    * returned set of valid hosts. */
   {
      _remove_if_not_in_host_list_ctx_t ctx;

      ctx.host_list = host_list;
      ctx.td = td;

      mongoc_set_for_each (servers, _remove_if_not_in_host_list_cb, &ctx);
   }

   /* At this point, the number of hosts in the host list should not exceed
    * srvMaxHosts. */
   BSON_ASSERT (td->max_hosts == 0 || servers->items_len <= (size_t) td->max_hosts);
}


void
_mongoc_topology_description_clear_connection_pool (mongoc_topology_description_t *td,
                                                    uint32_t server_id,
                                                    const bson_oid_t *service_id)
{
   mongoc_server_description_t *sd;
   bson_error_t error;

   BSON_ASSERT (service_id);

   sd = mongoc_topology_description_server_by_id (td, server_id, &error);
   if (!sd) {
      /* Server removed, ignore and ignore error. */
      return;
   }

   TRACE ("clearing pool for server: %s", sd->host.host_and_port);

   mc_tpl_sd_increment_generation (sd, service_id);
}


void
mongoc_deprioritized_servers_add_if_sharded (mongoc_deprioritized_servers_t *ds,
                                             mongoc_topology_description_type_t topology_type,
                                             const mongoc_server_description_t *sd)
{
   // In a sharded cluster, the server on which the operation failed MUST
   // be provided to the server selection mechanism as a deprioritized
   // server.
   if (topology_type == MONGOC_TOPOLOGY_SHARDED) {
      TRACE ("deprioritization: add to list: %s (id: %" PRIu32 ")", sd->host.host_and_port, sd->id);
      mongoc_deprioritized_servers_add (ds, sd);
   }
}
