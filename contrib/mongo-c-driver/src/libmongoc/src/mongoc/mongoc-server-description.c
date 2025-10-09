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
#include "mongoc-host-list.h"
#include "mongoc-host-list-private.h"
#include "mongoc-read-prefs.h"
#include "mongoc-read-prefs-private.h"
#include "mongoc-server-description-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-uri.h"
#include "mongoc-util-private.h"
#include "mongoc-compression-private.h"

#include <bson-dsl.h>

#include <stdio.h>

#define ALPHA 0.2

static bson_oid_t kObjectIdZero = {{0}};

const bson_oid_t kZeroServiceId = {{0}};

static bool
_match_tag_set (const mongoc_server_description_t *sd, bson_iter_t *tag_set_iter);

/* Destroy allocated resources within @description, but don't free it */
void
mongoc_server_description_cleanup (mongoc_server_description_t *sd)
{
   BSON_ASSERT (sd);

   bson_destroy (&sd->last_hello_response);
   bson_destroy (&sd->hosts);
   bson_destroy (&sd->passives);
   bson_destroy (&sd->arbiters);
   bson_destroy (&sd->tags);
   bson_destroy (&sd->compressors);
   bson_destroy (&sd->topology_version);
   mongoc_generation_map_destroy (sd->_generation_map_);
}

/* Reset fields inside this sd, but keep same id, host information, RTT,
   generation, topology version, and leave hello in empty inited state */
void
mongoc_server_description_reset (mongoc_server_description_t *sd)
{
   BSON_ASSERT (sd);

   memset (&sd->error, 0, sizeof sd->error);
   sd->set_name = NULL;
   sd->type = MONGOC_SERVER_UNKNOWN;

   sd->min_wire_version = MONGOC_DEFAULT_WIRE_VERSION;
   sd->max_wire_version = MONGOC_DEFAULT_WIRE_VERSION;
   sd->max_msg_size = MONGOC_DEFAULT_MAX_MSG_SIZE;
   sd->max_bson_obj_size = MONGOC_DEFAULT_BSON_OBJ_SIZE;
   sd->max_write_batch_size = MONGOC_DEFAULT_WRITE_BATCH_SIZE;
   sd->session_timeout_minutes = MONGOC_NO_SESSIONS;
   sd->last_write_date_ms = -1;
   sd->hello_ok = false;

   /* always leave last hello in an init-ed state until we destroy sd */
   bson_destroy (&sd->last_hello_response);
   bson_init (&sd->last_hello_response);
   sd->has_hello_response = false;
   sd->last_update_time_usec = bson_get_monotonic_time ();

   bson_destroy (&sd->hosts);
   bson_destroy (&sd->passives);
   bson_destroy (&sd->arbiters);
   bson_destroy (&sd->tags);
   bson_destroy (&sd->compressors);

   bson_init (&sd->hosts);
   bson_init (&sd->passives);
   bson_init (&sd->arbiters);
   bson_init (&sd->tags);
   bson_init (&sd->compressors);

   sd->me = NULL;
   sd->current_primary = NULL;
   sd->set_version = MONGOC_NO_SET_VERSION;
   bson_oid_copy_unsafe (&kObjectIdZero, &sd->election_id);
   bson_oid_copy_unsafe (&kObjectIdZero, &sd->service_id);
   sd->server_connection_id = MONGOC_NO_SERVER_CONNECTION_ID;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_init --
 *
 *       Initialize a new server_description_t.
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
mongoc_server_description_init (mongoc_server_description_t *sd, const char *address, uint32_t id)
{
   ENTRY;

   BSON_ASSERT (sd);
   BSON_ASSERT (address);

   sd->id = id;
   sd->type = MONGOC_SERVER_UNKNOWN;
   sd->round_trip_time_msec = MONGOC_RTT_UNSET;
   sd->generation = 0;
   sd->opened = 0;
   sd->_generation_map_ = mongoc_generation_map_new ();

   if (!_mongoc_host_list_from_string (&sd->host, address)) {
      MONGOC_WARNING ("Failed to parse uri for %s", address);
      return;
   }

   sd->connection_address = sd->host.host_and_port;
   bson_init (&sd->last_hello_response);
   bson_init (&sd->hosts);
   bson_init (&sd->passives);
   bson_init (&sd->arbiters);
   bson_init (&sd->tags);
   bson_init (&sd->compressors);
   bson_init (&sd->topology_version);

   mongoc_server_description_reset (sd);

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_destroy --
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
mongoc_server_description_destroy (mongoc_server_description_t *description)
{
   ENTRY;

   if (!description) {
      EXIT;
   }

   mongoc_server_description_cleanup (description);

   bson_free (description);

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_has_rs_member --
 *
 *       Return true if this address is included in server's list of rs
 *       members, false otherwise.
 *
 * Returns:
 *       true, false
 *
 * Side effects:
 *       None
 *
 *--------------------------------------------------------------------------
 */
bool
mongoc_server_description_has_rs_member (const mongoc_server_description_t *server, const char *address)
{
   bson_iter_t member_iter;
   const bson_t *rs_members[3];
   int i;

   if (server->type != MONGOC_SERVER_UNKNOWN) {
      rs_members[0] = &server->hosts;
      rs_members[1] = &server->arbiters;
      rs_members[2] = &server->passives;

      for (i = 0; i < 3; i++) {
         BSON_ASSERT (bson_iter_init (&member_iter, rs_members[i]));

         while (bson_iter_next (&member_iter)) {
            if (strcasecmp (address, bson_iter_utf8 (&member_iter, NULL)) == 0) {
               return true;
            }
         }
      }
   }

   return false;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_has_set_version --
 *
 *      Did this server's hello response have a "setVersion" field?
 *
 * Returns:
 *      True if the server description's setVersion is set.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_server_description_has_set_version (const mongoc_server_description_t *description)
{
   return description->set_version != MONGOC_NO_SET_VERSION;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_has_election_id --
 *
 *      Did this server's hello response have an "electionId" field?
 *
 * Returns:
 *      True if the server description's electionId is set.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_server_description_has_election_id (const mongoc_server_description_t *description)
{
   return 0 != bson_oid_compare (&description->election_id, &kObjectIdZero);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_id --
 *
 *      Get the id of this server.
 *
 * Returns:
 *      Server's id.
 *
 *--------------------------------------------------------------------------
 */

uint32_t
mongoc_server_description_id (const mongoc_server_description_t *description)
{
   return description->id;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_host --
 *
 *      Return a reference to the host associated with this server description.
 *
 * Returns:
 *      This server description's host, a mongoc_host_list_t * you must
 *      not modify or free.
 *
 *--------------------------------------------------------------------------
 */

mongoc_host_list_t *
mongoc_server_description_host (const mongoc_server_description_t *description)
{
   return &((mongoc_server_description_t *) description)->host;
}

int64_t
mongoc_server_description_last_update_time (const mongoc_server_description_t *description)
{
   return description->last_update_time_usec;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_round_trip_time --
 *
 *      Get the round trip time of this server, which is the client's
 *      measurement of the duration of a "hello" command.
 *
 * Returns:
 *      The server's round trip time in milliseconds.
 *
 *--------------------------------------------------------------------------
 */

int64_t
mongoc_server_description_round_trip_time (const mongoc_server_description_t *description)
{
   return description->round_trip_time_msec;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_type --
 *
 *      Get this server's type, one of the types defined in the Server
 *      Discovery And Monitoring Spec.
 *
 * Returns:
 *      A string.
 *
 *--------------------------------------------------------------------------
 */

const char *
mongoc_server_description_type (const mongoc_server_description_t *description)
{
   switch (description->type) {
   case MONGOC_SERVER_UNKNOWN:
      return "Unknown";
   case MONGOC_SERVER_STANDALONE:
      return "Standalone";
   case MONGOC_SERVER_MONGOS:
      return "Mongos";
   case MONGOC_SERVER_POSSIBLE_PRIMARY:
      return "PossiblePrimary";
   case MONGOC_SERVER_RS_PRIMARY:
      return "RSPrimary";
   case MONGOC_SERVER_RS_SECONDARY:
      return "RSSecondary";
   case MONGOC_SERVER_RS_ARBITER:
      return "RSArbiter";
   case MONGOC_SERVER_RS_OTHER:
      return "RSOther";
   case MONGOC_SERVER_RS_GHOST:
      return "RSGhost";
   case MONGOC_SERVER_LOAD_BALANCER:
      return "LoadBalancer";
   case MONGOC_SERVER_DESCRIPTION_TYPES:
   default:
      MONGOC_ERROR ("Invalid mongoc_server_description_t type");
      return "Invalid";
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_hello_response --
 *
 *      Return this server's most recent "hello" command response.
 *
 * Returns:
 *      A reference to a BSON document, owned by the server description.
 *
 *--------------------------------------------------------------------------
 */

const bson_t *
mongoc_server_description_hello_response (const mongoc_server_description_t *description)
{
   return &description->last_hello_response;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_ismaster --
 *
 *      Return this server's most recent "hello" command response.
 *
 * Returns:
 *      A reference to a BSON document, owned by the server description.
 *
 *--------------------------------------------------------------------------
 */

const bson_t *
mongoc_server_description_ismaster (const mongoc_server_description_t *description)
{
   return mongoc_server_description_hello_response (description);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_set_state --
 *
 *       Set the server description's server type.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_server_description_set_state (mongoc_server_description_t *description, mongoc_server_description_type_t type)
{
   description->type = type;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_set_set_version --
 *
 *       Set the replica set version of this server.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_server_description_set_set_version (mongoc_server_description_t *description, int64_t set_version)
{
   description->set_version = set_version;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_set_election_id --
 *
 *       Set the election_id of this server. Copies the given ObjectId or,
 *       if it is NULL, zeroes description's election_id.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_server_description_set_election_id (mongoc_server_description_t *description, const bson_oid_t *election_id)
{
   if (election_id) {
      bson_oid_copy_unsafe (election_id, &description->election_id);
   } else {
      bson_oid_copy_unsafe (&kObjectIdZero, &description->election_id);
   }
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_server_description_update_rtt --
 *
 *       Calculate this server's rtt calculation using an exponentially-
 *       weighted moving average formula.
 *
 * Side effects:
 *       None.
 *
 * If rtt_msec is MONGOC_RTT_UNSET, the value is not updated.
 *
 *-------------------------------------------------------------------------
 */
void
mongoc_server_description_update_rtt (mongoc_server_description_t *server, int64_t rtt_msec)
{
   if (rtt_msec == MONGOC_RTT_UNSET) {
      return;
   }
   if (server->round_trip_time_msec == MONGOC_RTT_UNSET) {
      bson_atomic_int64_exchange (&server->round_trip_time_msec, rtt_msec, bson_memory_order_relaxed);
   } else {
      bson_atomic_int64_exchange (&server->round_trip_time_msec,
                                  (int64_t) (ALPHA * rtt_msec + (1 - ALPHA) * server->round_trip_time_msec),
                                  bson_memory_order_relaxed);
   }
}


static void
_mongoc_server_description_set_error (mongoc_server_description_t *sd, const bson_error_t *error)
{
   if (error && error->code) {
      memcpy (&sd->error, error, sizeof (bson_error_t));
   } else {
      bson_set_error (&sd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "unknown error calling hello");
   }

   /* Server Discovery and Monitoring Spec: if the server type changes from a
    * known type to Unknown its RTT is set to null. */
   sd->round_trip_time_msec = MONGOC_RTT_UNSET;
}


/*
 *-------------------------------------------------------------------------
 *
 * Called during SDAM, from topology description's hello handler, or
 * when handshaking a connection in _mongoc_cluster_stream_for_server.
 *
 * If @hello_response is empty, @error must say why hello failed.
 *
 *-------------------------------------------------------------------------
 */

void
mongoc_server_description_handle_hello (mongoc_server_description_t *sd,
                                        const bson_t *hello_response,
                                        int64_t rtt_msec,
                                        const bson_error_t *error /* IN */)
{
   bson_iter_t iter;
   bson_iter_t child;
   bool is_primary = false;
   bool is_shard = false;
   bool is_secondary = false;
   bool is_arbiter = false;
   bool is_replicaset = false;
   bool is_hidden = false;
   const uint8_t *bytes;
   uint32_t len;
   int num_keys = 0;
   ENTRY;

   BSON_ASSERT (sd);

   mongoc_server_description_reset (sd);
   if (!hello_response) {
      _mongoc_server_description_set_error (sd, error);
      EXIT;
   }

   bson_destroy (&sd->last_hello_response);
   bsonBuild (sd->last_hello_response, insert (*hello_response, not(key ("speculativeAuthenticate"))));
   sd->has_hello_response = true;

   /* Only reinitialize the topology version if we have a hello response.
    * Resetting a server description should not effect the topology version. */
   bson_reinit (&sd->topology_version);

   BSON_ASSERT (bson_iter_init (&iter, &sd->last_hello_response));

   while (bson_iter_next (&iter)) {
      num_keys++;
      if (strcmp ("ok", bson_iter_key (&iter)) == 0) {
         if (!bson_iter_as_bool (&iter)) {
            /* it doesn't really matter what error API we use. the code and
             * domain will be overwritten. */
            (void) _mongoc_cmd_check_ok (hello_response, MONGOC_ERROR_API_VERSION_2, &sd->error);
            /* TODO CDRIVER-3696: this is an existing bug. If this is handling
             * a hello reply that is NOT from a handshake, this should not
             * be considered an auth error. */
            /* hello response returned ok: 0. According to auth spec: "If the
             * hello of the MongoDB Handshake fails with an error, drivers
             * MUST treat this an authentication error." */
            sd->error.domain = MONGOC_ERROR_CLIENT;
            sd->error.code = MONGOC_ERROR_CLIENT_AUTHENTICATE;
            GOTO (authfailure);
         }
      } else if (strcmp ("isWritablePrimary", bson_iter_key (&iter)) == 0 ||
                 strcmp (HANDSHAKE_RESPONSE_LEGACY_HELLO, bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_BOOL (&iter))
            GOTO (typefailure);
         is_primary = bson_iter_bool (&iter);
      } else if (strcmp ("helloOk", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_BOOL (&iter))
            GOTO (typefailure);
         sd->hello_ok = bson_iter_bool (&iter);
      } else if (strcmp ("me", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_UTF8 (&iter))
            GOTO (typefailure);
         sd->me = bson_iter_utf8 (&iter, NULL);
      } else if (strcmp ("maxMessageSizeBytes", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_INT32 (&iter))
            GOTO (typefailure);
         sd->max_msg_size = bson_iter_int32 (&iter);
      } else if (strcmp ("maxBsonObjectSize", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_INT32 (&iter))
            GOTO (typefailure);
         sd->max_bson_obj_size = bson_iter_int32 (&iter);
      } else if (strcmp ("maxWriteBatchSize", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_INT32 (&iter))
            GOTO (typefailure);
         sd->max_write_batch_size = bson_iter_int32 (&iter);
      } else if (strcmp ("logicalSessionTimeoutMinutes", bson_iter_key (&iter)) == 0) {
         if (BSON_ITER_HOLDS_NUMBER (&iter)) {
            sd->session_timeout_minutes = bson_iter_as_int64 (&iter);
         } else if (BSON_ITER_HOLDS_NULL (&iter)) {
            /* this arises executing standard JSON tests */
            sd->session_timeout_minutes = MONGOC_NO_SESSIONS;
         } else {
            GOTO (typefailure);
         }
      } else if (strcmp ("minWireVersion", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_INT32 (&iter))
            GOTO (typefailure);
         sd->min_wire_version = bson_iter_int32 (&iter);
      } else if (strcmp ("maxWireVersion", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_INT32 (&iter))
            GOTO (typefailure);
         sd->max_wire_version = bson_iter_int32 (&iter);
      } else if (strcmp ("msg", bson_iter_key (&iter)) == 0) {
         const char *msg;
         if (!BSON_ITER_HOLDS_UTF8 (&iter))
            GOTO (typefailure);
         msg = bson_iter_utf8 (&iter, NULL);
         if (msg && 0 == strcmp (msg, "isdbgrid")) {
            is_shard = true;
         }
      } else if (strcmp ("setName", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_UTF8 (&iter))
            GOTO (typefailure);
         sd->set_name = bson_iter_utf8 (&iter, NULL);
      } else if (strcmp ("setVersion", bson_iter_key (&iter)) == 0) {
         mongoc_server_description_set_set_version (sd, bson_iter_as_int64 (&iter));
      } else if (strcmp ("electionId", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_OID (&iter))
            GOTO (typefailure);
         mongoc_server_description_set_election_id (sd, bson_iter_oid (&iter));
      } else if (strcmp ("secondary", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_BOOL (&iter))
            GOTO (typefailure);
         is_secondary = bson_iter_bool (&iter);
      } else if (strcmp ("hosts", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_ARRAY (&iter))
            GOTO (typefailure);
         bson_iter_array (&iter, &len, &bytes);
         bson_destroy (&sd->hosts);
         BSON_ASSERT (bson_init_static (&sd->hosts, bytes, len));
      } else if (strcmp ("passives", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_ARRAY (&iter))
            GOTO (typefailure);
         bson_iter_array (&iter, &len, &bytes);
         bson_destroy (&sd->passives);
         BSON_ASSERT (bson_init_static (&sd->passives, bytes, len));
      } else if (strcmp ("arbiters", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_ARRAY (&iter))
            GOTO (typefailure);
         bson_iter_array (&iter, &len, &bytes);
         bson_destroy (&sd->arbiters);
         BSON_ASSERT (bson_init_static (&sd->arbiters, bytes, len));
      } else if (strcmp ("primary", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_UTF8 (&iter))
            GOTO (typefailure);
         sd->current_primary = bson_iter_utf8 (&iter, NULL);
      } else if (strcmp ("arbiterOnly", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_BOOL (&iter))
            GOTO (typefailure);
         is_arbiter = bson_iter_bool (&iter);
      } else if (strcmp ("isreplicaset", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_BOOL (&iter))
            GOTO (typefailure);
         is_replicaset = bson_iter_bool (&iter);
      } else if (strcmp ("tags", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_DOCUMENT (&iter))
            GOTO (typefailure);
         bson_iter_document (&iter, &len, &bytes);
         bson_destroy (&sd->tags);
         BSON_ASSERT (bson_init_static (&sd->tags, bytes, len));
      } else if (strcmp ("hidden", bson_iter_key (&iter)) == 0) {
         is_hidden = bson_iter_bool (&iter);
      } else if (strcmp ("lastWrite", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_DOCUMENT (&iter) || !bson_iter_recurse (&iter, &child) ||
             !bson_iter_find (&child, "lastWriteDate") || !BSON_ITER_HOLDS_DATE_TIME (&child)) {
            GOTO (typefailure);
         }

         sd->last_write_date_ms = bson_iter_date_time (&child);
      } else if (strcmp ("compression", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_ARRAY (&iter))
            GOTO (typefailure);
         bson_iter_array (&iter, &len, &bytes);
         bson_destroy (&sd->compressors);
         BSON_ASSERT (bson_init_static (&sd->compressors, bytes, len));
      } else if (strcmp ("topologyVersion", bson_iter_key (&iter)) == 0) {
         bson_t incoming_topology_version;

         if (!BSON_ITER_HOLDS_DOCUMENT (&iter)) {
            GOTO (typefailure);
         }

         bson_iter_document (&iter, &len, &bytes);
         BSON_ASSERT (bson_init_static (&incoming_topology_version, bytes, len));
         mongoc_server_description_set_topology_version (sd, &incoming_topology_version);
         bson_destroy (&incoming_topology_version);
      } else if (strcmp ("serviceId", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_OID (&iter))
            GOTO (typefailure);
         bson_oid_copy_unsafe (bson_iter_oid (&iter), &sd->service_id);
      } else if (strcmp ("connectionId", bson_iter_key (&iter)) == 0) {
         if (!BSON_ITER_HOLDS_NUMBER (&iter))
            GOTO (typefailure);
         sd->server_connection_id = bson_iter_as_int64 (&iter);
      }
   }

   if (is_shard) {
      sd->type = MONGOC_SERVER_MONGOS;
   } else if (sd->set_name) {
      if (is_hidden) {
         sd->type = MONGOC_SERVER_RS_OTHER;
      } else if (is_primary) {
         sd->type = MONGOC_SERVER_RS_PRIMARY;
      } else if (is_secondary) {
         sd->type = MONGOC_SERVER_RS_SECONDARY;
      } else if (is_arbiter) {
         sd->type = MONGOC_SERVER_RS_ARBITER;
      } else {
         sd->type = MONGOC_SERVER_RS_OTHER;
      }
   } else if (is_replicaset) {
      sd->type = MONGOC_SERVER_RS_GHOST;
   } else if (num_keys > 0) {
      sd->type = MONGOC_SERVER_STANDALONE;
   } else {
      sd->type = MONGOC_SERVER_UNKNOWN;
   }

   if (!num_keys) {
      /* empty reply means hello failed */
      _mongoc_server_description_set_error (sd, error);
   }

   mongoc_server_description_update_rtt (sd, rtt_msec);

   EXIT;

typefailure:
   bson_set_error (&sd->error,
                   MONGOC_ERROR_STREAM,
                   MONGOC_ERROR_STREAM_INVALID_TYPE,
                   "unexpected type %s for field %s in hello response",
                   _mongoc_bson_type_to_str (bson_iter_type (&iter)),
                   bson_iter_key (&iter));
authfailure:
   sd->type = MONGOC_SERVER_UNKNOWN;
   sd->round_trip_time_msec = MONGOC_RTT_UNSET;

   EXIT;
}

/*
 *-------------------------------------------------------------------------
 *
 * mongoc_server_description_new_copy --
 *
 *       A copy of a server description that you must destroy, or NULL.
 *
 *-------------------------------------------------------------------------
 */
mongoc_server_description_t *
mongoc_server_description_new_copy (const mongoc_server_description_t *description)
{
   mongoc_server_description_t *copy;

   if (!description) {
      return NULL;
   }

   copy = BSON_ALIGNED_ALLOC0 (mongoc_server_description_t);

   copy->id = description->id;
   copy->opened = description->opened;
   memcpy (&copy->host, &description->host, sizeof (copy->host));
   copy->round_trip_time_msec = MONGOC_RTT_UNSET;

   copy->connection_address = copy->host.host_and_port;
   bson_init (&copy->last_hello_response);
   bson_init (&copy->hosts);
   bson_init (&copy->passives);
   bson_init (&copy->arbiters);
   bson_init (&copy->tags);
   bson_init (&copy->compressors);
   bson_copy_to (&description->topology_version, &copy->topology_version);
   bson_oid_copy (&description->service_id, &copy->service_id);
   copy->server_connection_id = description->server_connection_id;

   if (description->has_hello_response) {
      /* calls mongoc_server_description_reset */
      int64_t last_rtt_ms = bson_atomic_int64_fetch (&description->round_trip_time_msec, bson_memory_order_relaxed);
      mongoc_server_description_handle_hello (
         copy, &description->last_hello_response, last_rtt_ms, &description->error);
   } else {
      mongoc_server_description_reset (copy);
      /* preserve the original server description type, which is manually set
       * for a LoadBalancer server */
      copy->type = description->type;
   }

   /* Preserve the error */
   memcpy (&copy->error, &description->error, sizeof copy->error);

   copy->generation = description->generation;
   copy->_generation_map_ = mongoc_generation_map_copy (mc_tpl_sd_generation_map_const (description));
   return copy;
}


/*
 *-------------------------------------------------------------------------
 *
 * mongoc_server_description_filter_stale --
 *
 *       Estimate servers' staleness according to the Server Selection Spec.
 *       Determines the number of eligible servers, and sets any servers that
 *       are too stale to NULL in the descriptions set.
 *
 *-------------------------------------------------------------------------
 */

void
mongoc_server_description_filter_stale (const mongoc_server_description_t **sds,
                                        size_t sds_len,
                                        const mongoc_server_description_t *primary,
                                        int64_t heartbeat_frequency_ms,
                                        const mongoc_read_prefs_t *read_prefs)
{
   int64_t max_staleness_seconds;
   size_t i;

   int64_t heartbeat_frequency_usec;
   int64_t max_last_write_date_usec;
   int64_t staleness_usec;
   int64_t max_staleness_usec;

   if (!read_prefs) {
      /* NULL read_prefs is PRIMARY, no maxStalenessSeconds to filter by */
      return;
   }

   max_staleness_seconds = mongoc_read_prefs_get_max_staleness_seconds (read_prefs);

   if (max_staleness_seconds == MONGOC_NO_MAX_STALENESS) {
      return;
   }

   BSON_ASSERT (max_staleness_seconds > 0);
   max_staleness_usec = max_staleness_seconds * 1000 * 1000;
   heartbeat_frequency_usec = heartbeat_frequency_ms * 1000;

   if (primary) {
      for (i = 0; i < sds_len; i++) {
         if (!sds[i] || sds[i]->type != MONGOC_SERVER_RS_SECONDARY) {
            continue;
         }

         /* See max-staleness.rst for explanation of these formulae. */
         staleness_usec = primary->last_write_date_ms * 1000 +
                          (sds[i]->last_update_time_usec - primary->last_update_time_usec) -
                          sds[i]->last_write_date_ms * 1000 + heartbeat_frequency_usec;

         if (staleness_usec > max_staleness_usec) {
            TRACE ("Rejected stale RSSecondary [%s]", sds[i]->host.host_and_port);
            sds[i] = NULL;
         }
      }
   } else {
      /* find max last_write_date */
      max_last_write_date_usec = 0;
      for (i = 0; i < sds_len; i++) {
         if (sds[i] && sds[i]->type == MONGOC_SERVER_RS_SECONDARY) {
            max_last_write_date_usec = BSON_MAX (max_last_write_date_usec, sds[i]->last_write_date_ms * 1000);
         }
      }

      /* use max last_write_date to estimate each secondary's staleness */
      for (i = 0; i < sds_len; i++) {
         if (!sds[i] || sds[i]->type != MONGOC_SERVER_RS_SECONDARY) {
            continue;
         }

         staleness_usec = max_last_write_date_usec - sds[i]->last_write_date_ms * 1000 + heartbeat_frequency_usec;

         if (staleness_usec > max_staleness_usec) {
            TRACE ("Rejected stale RSSecondary [%s]", sds[i]->host.host_and_port);
            sds[i] = NULL;
         }
      }
   }
}


/*
 *-------------------------------------------------------------------------
 *
 * mongoc_server_description_filter_tags --
 *
 * Given a set of server descriptions, set to NULL any that don't
 * match the read preference's tag sets.
 *
 * https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#tag-set
 *
 *-------------------------------------------------------------------------
 */

void
mongoc_server_description_filter_tags (const mongoc_server_description_t **descriptions,
                                       size_t description_len,
                                       const mongoc_read_prefs_t *read_prefs)
{
   const bson_t *rp_tags;
   bson_iter_t rp_tagset_iter;
   bson_iter_t tag_set_iter;
   bool *sd_matched = NULL;
   bool found;
   size_t i;

   if (!read_prefs) {
      /* NULL read_prefs is PRIMARY, no tags to filter by */
      return;
   }

   rp_tags = mongoc_read_prefs_get_tags (read_prefs);

   if (bson_count_keys (rp_tags) == 0) {
      /* no tags to filter by */
      return;
   }

   sd_matched = (bool *) bson_malloc0 (sizeof (bool) * description_len);

   bson_iter_init (&rp_tagset_iter, rp_tags);

   /* for each read preference tag set */
   while (bson_iter_next (&rp_tagset_iter)) {
      found = false;

      for (i = 0; i < description_len; i++) {
         if (!descriptions[i]) {
            /* NULLed earlier in mongoc_topology_description_suitable_servers */
            continue;
         }

         BSON_ASSERT (bson_iter_recurse (&rp_tagset_iter, &tag_set_iter));
         sd_matched[i] = _match_tag_set (descriptions[i], &tag_set_iter);
         if (sd_matched[i]) {
            found = true;
         }
      }

      if (found) {
         for (i = 0; i < description_len; i++) {
            if (!sd_matched[i] && descriptions[i]) {
               TRACE ("Rejected [%s] [%s], doesn't match tags",
                      mongoc_server_description_type (descriptions[i]),
                      descriptions[i]->host.host_and_port);
               descriptions[i] = NULL;
            }
         }

         goto CLEANUP;
      }
   }

   /* tried each */
   for (i = 0; i < description_len; i++) {
      if (!sd_matched[i]) {
         TRACE ("Rejected [%s] [%s], reached end of tags array without match",
                mongoc_server_description_type (descriptions[i]),
                descriptions[i]->host.host_and_port);

         descriptions[i] = NULL;
      }
   }

CLEANUP:
   bson_free (sd_matched);
}


/*
 *-------------------------------------------------------------------------
 *
 * _match_tag_set --
 *
 *       Check if a server's tags match one tag set, like
 *       {'tag1': 'value1', 'tag2': 'value2'}.
 *
 *-------------------------------------------------------------------------
 */
static bool
_match_tag_set (const mongoc_server_description_t *sd, bson_iter_t *tag_set_iter)
{
   bson_iter_t sd_iter;
   uint32_t read_pref_tag_len;
   uint32_t sd_len;
   const char *read_pref_tag;
   const char *read_pref_val;
   const char *server_val;

   while (bson_iter_next (tag_set_iter)) {
      /* one {'tag': 'value'} pair from the read preference's tag set */
      read_pref_tag = bson_iter_key (tag_set_iter);
      read_pref_val = bson_iter_utf8 (tag_set_iter, &read_pref_tag_len);

      if (bson_iter_init_find (&sd_iter, &sd->tags, read_pref_tag)) {
         /* The server has this tag - does it have the right value? */
         server_val = bson_iter_utf8 (&sd_iter, &sd_len);
         if (sd_len != read_pref_tag_len || memcmp (read_pref_val, server_val, read_pref_tag_len)) {
            /* If the values don't match, no match */
            return false;
         }
      } else {
         /* If the server description doesn't have that key, no match */
         return false;
      }
   }

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_server_description_compressor_id --
 *
 *      Get the compressor id if compression was negotiated.
 *
 * Returns:
 *      The compressor ID, or -1 if none was negotiated.
 *
 *--------------------------------------------------------------------------
 */

int32_t
mongoc_server_description_compressor_id (const mongoc_server_description_t *description)
{
   int id;
   bson_iter_t iter;
   BSON_ASSERT (bson_iter_init (&iter, &description->compressors));

   while (bson_iter_next (&iter)) {
      id = mongoc_compressor_name_to_id (bson_iter_utf8 (&iter, NULL));
      if (id != -1) {
         return id;
      }
   }

   return -1;
}

/* Returns true if either or both is NULL. out is 1 if exactly one NULL, 0 if
 * both NULL */
typedef int (*strcmp_fn) (const char *, const char *);

static int
_nullable_cmp (const char *a, const char *b, strcmp_fn cmp_fn)
{
   if (!a && b) {
      return 1;
   }

   if (a && !b) {
      return 1;
   }

   if (!a && !b) {
      return 0;
   }

   /* Both not NULL. */
   return cmp_fn (a, b);
}
static int
_nullable_strcasecmp (const char *a, const char *b)
{
   return _nullable_cmp (a, b, strcasecmp);
}

static int
_nullable_strcmp (const char *a, const char *b)
{
   return _nullable_cmp (a, b, strcmp);
}

bool
_mongoc_server_description_equal (mongoc_server_description_t *sd1, mongoc_server_description_t *sd2)
{
   if (sd1->type != sd2->type) {
      return false;
   }

   if (sd1->min_wire_version != sd2->min_wire_version) {
      return false;
   }

   if (sd1->max_wire_version != sd2->max_wire_version) {
      return false;
   }

   if (0 != _nullable_strcasecmp (sd1->me, sd2->me)) {
      return false;
   }

   /* CDRIVER-3527: The hosts/passives/arbiters checks should really be a set
    * comparison of case insensitive hostnames. */
   if (!bson_equal (&sd1->hosts, &sd2->hosts)) {
      return false;
   }

   if (!bson_equal (&sd1->passives, &sd2->passives)) {
      return false;
   }

   if (!bson_equal (&sd1->arbiters, &sd2->arbiters)) {
      return false;
   }

   if (!bson_equal (&sd1->tags, &sd2->tags)) {
      return false;
   }

   if (0 != _nullable_strcmp (sd1->set_name, sd2->set_name)) {
      return false;
   }

   if (sd1->set_version != sd2->set_version) {
      return false;
   }

   if (!bson_oid_equal (&sd1->election_id, &sd2->election_id)) {
      return false;
   }

   if (0 != _nullable_strcasecmp (sd1->current_primary, sd2->current_primary)) {
      return false;
   }

   if (sd1->session_timeout_minutes != sd2->session_timeout_minutes) {
      return false;
   }

   if (0 != memcmp (&sd1->error, &sd2->error, sizeof (bson_error_t))) {
      return false;
   }

   if (!bson_equal (&sd1->topology_version, &sd2->topology_version)) {
      return false;
   }

   return true;
}

int
mongoc_server_description_topology_version_cmp (const bson_t *tv1, const bson_t *tv2)
{
   const bson_oid_t *pid1;
   const bson_oid_t *pid2;
   int64_t counter1;
   int64_t counter2;
   bson_iter_t iter;

   BSON_ASSERT (tv1);
   BSON_ASSERT (tv2);

   if (bson_empty (tv1) || bson_empty (tv2)) {
      return -1;
   }

   if (!bson_iter_init_find (&iter, tv1, "processId") || !BSON_ITER_HOLDS_OID (&iter)) {
      return -1;
   }
   pid1 = bson_iter_oid (&iter);

   if (!bson_iter_init_find (&iter, tv2, "processId") || !BSON_ITER_HOLDS_OID (&iter)) {
      return -1;
   }
   pid2 = bson_iter_oid (&iter);

   if (0 != bson_oid_compare (pid1, pid2)) {
      /* Assume greater. */
      return -1;
   }

   if (!bson_iter_init_find (&iter, tv1, "counter") || !BSON_ITER_HOLDS_INT (&iter)) {
      return -1;
   }
   counter1 = bson_iter_as_int64 (&iter);

   if (!bson_iter_init_find (&iter, tv2, "counter") || !BSON_ITER_HOLDS_INT (&iter)) {
      return -1;
   }
   counter2 = bson_iter_as_int64 (&iter);

   if (counter1 < counter2) {
      return -1;
   } else if (counter1 > counter2) {
      return 1;
   }
   return 0;
}

void
mongoc_server_description_set_topology_version (mongoc_server_description_t *sd, const bson_t *tv)
{
   BSON_ASSERT (tv);
   bson_destroy (&sd->topology_version);
   bson_copy_to (tv, &sd->topology_version);
}

bool
mongoc_server_description_has_service_id (const mongoc_server_description_t *description)
{
   if (0 == bson_oid_compare (&description->service_id, &kZeroServiceId)) {
      return false;
   }
   return true;
}
