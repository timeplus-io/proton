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

#ifndef MONGOC_SERVER_DESCRIPTION_PRIVATE_H
#define MONGOC_SERVER_DESCRIPTION_PRIVATE_H

#include "mongoc-server-description.h"
#include "mongoc-generation-map-private.h"


#define MONGOC_DEFAULT_WIRE_VERSION 0
#define MONGOC_DEFAULT_WRITE_BATCH_SIZE 1000
#define MONGOC_DEFAULT_BSON_OBJ_SIZE 16 * 1024 * 1024
#define MONGOC_DEFAULT_MAX_MSG_SIZE 48000000
/* This is slightly out-of-spec as of the current version of the spec (1.0.0),
 * but SPEC-1397 plans to amend "Size limits and Wire Protocol Considerations"
 * to say that drivers MAY split with a reduced maxBsonObjectSize or
 * maxMessageSizeBytes
 * depending on the implementation. It is less invasive for libmongoc to split
 * OP_MSG payload type 1 with a reduced maxMessageSizeBytes and convert it to a
 * payload type 0
 * rather than split a payload type 0 with a reduced maxBsonObjectSize.
 */
#define MONGOC_REDUCED_MAX_MSG_SIZE_FOR_FLE 2097152
#define MONGOC_NO_SESSIONS -1
#define MONGOC_IDLE_WRITE_PERIOD_MS 10 * 1000

/* represent a server or topology with no replica set config version */
#define MONGOC_NO_SET_VERSION -1

#define MONGOC_RTT_UNSET -1

#define MONGOC_NO_SERVER_CONNECTION_ID -1

typedef enum {
   MONGOC_SERVER_UNKNOWN,
   MONGOC_SERVER_STANDALONE,
   MONGOC_SERVER_MONGOS,
   MONGOC_SERVER_POSSIBLE_PRIMARY,
   MONGOC_SERVER_RS_PRIMARY,
   MONGOC_SERVER_RS_SECONDARY,
   MONGOC_SERVER_RS_ARBITER,
   MONGOC_SERVER_RS_OTHER,
   MONGOC_SERVER_RS_GHOST,
   MONGOC_SERVER_LOAD_BALANCER,
   MONGOC_SERVER_DESCRIPTION_TYPES,
} mongoc_server_description_type_t;

struct _mongoc_server_description_t {
   uint32_t id;
   mongoc_host_list_t host;
   int64_t round_trip_time_msec;
   int64_t last_update_time_usec;
   bson_t last_hello_response;
   bool has_hello_response;
   bool hello_ok;
   const char *connection_address;
   /* SDAM dictates storing me/hosts/passives/arbiters after being "normalized
    * to lower-case" Instead, they are stored in the casing they are received,
    * but compared case insensitively. This should be addressed in CDRIVER-3527.
    */
   const char *me;

   /* whether an APM server-opened callback has been fired before */
   bool opened;

   const char *set_name;
   bson_error_t error;
   mongoc_server_description_type_t type;

   int32_t min_wire_version;
   int32_t max_wire_version;
   int32_t max_msg_size;
   int32_t max_bson_obj_size;
   int32_t max_write_batch_size;
   int64_t session_timeout_minutes;

   /* hosts, passives, and arbiters are stored as a BSON array, but compared
    * case insensitively. This should be improved in CDRIVER-3527. */
   bson_t hosts;
   bson_t passives;
   bson_t arbiters;

   bson_t tags;
   const char *current_primary;
   int64_t set_version;
   bson_oid_t election_id;
   int64_t last_write_date_ms;

   bson_t compressors;
   bson_t topology_version;
   /*
   The generation is incremented every time connections to this server should be
   invalidated.
   This happens when:
   1. a monitor receives a network error
   2. an app thread receives any network error before completing a handshake
   3. an app thread receives a non-timeout network error after the handshake
   4. an app thread receives a "not primary" or "node is recovering" error from
   a pre-4.2 server.
   */

   /* generation only applies to a server description tied to a connection.
    * It represents the generation number for this connection. */
   uint32_t generation;

   /* _generation_map_ stores all generations for all service IDs associated
    * with this server. _generation_map_ is only accessed on the server
    * description for monitoring. In non-load-balanced mode, there are no
    * service IDs. The only server generation is mapped from kZeroServiceID */
   mongoc_generation_map_t *_generation_map_;
   bson_oid_t service_id;
   int64_t server_connection_id;
};

/** Get a mutable pointer to the server's generation map */
static BSON_INLINE mongoc_generation_map_t *
mc_tpl_sd_generation_map (mongoc_server_description_t *sd)
{
   return sd->_generation_map_;
}

/** Get a const pointer to the server's generation map */
static BSON_INLINE const mongoc_generation_map_t *
mc_tpl_sd_generation_map_const (const mongoc_server_description_t *sd)
{
   return sd->_generation_map_;
}

/**
 * @brief Increment the generation number on the given server for the associated
 * service ID.
 */
static BSON_INLINE void
mc_tpl_sd_increment_generation (mongoc_server_description_t *sd, const bson_oid_t *service_id)
{
   mongoc_generation_map_increment (mc_tpl_sd_generation_map (sd), service_id);
}

/**
 * @brief Get the generation number of the given server description for the
 * associated service ID.
 */
static BSON_INLINE uint32_t
mc_tpl_sd_get_generation (const mongoc_server_description_t *sd, const bson_oid_t *service_id)
{
   return mongoc_generation_map_get (mc_tpl_sd_generation_map_const (sd), service_id);
}

void
mongoc_server_description_init (mongoc_server_description_t *sd, const char *address, uint32_t id);
bool
mongoc_server_description_has_rs_member (const mongoc_server_description_t *description, const char *address);


bool
mongoc_server_description_has_set_version (const mongoc_server_description_t *description);

bool
mongoc_server_description_has_election_id (const mongoc_server_description_t *description);

void
mongoc_server_description_cleanup (mongoc_server_description_t *sd);

void
mongoc_server_description_reset (mongoc_server_description_t *sd);

void
mongoc_server_description_set_state (mongoc_server_description_t *description, mongoc_server_description_type_t type);
void
mongoc_server_description_set_set_version (mongoc_server_description_t *description, int64_t set_version);
void
mongoc_server_description_set_election_id (mongoc_server_description_t *description, const bson_oid_t *election_id);
void
mongoc_server_description_update_rtt (mongoc_server_description_t *server, int64_t rtt_msec);

void
mongoc_server_description_handle_hello (mongoc_server_description_t *sd,
                                        const bson_t *hello_response,
                                        int64_t rtt_msec,
                                        const bson_error_t *error /* IN */);

void
mongoc_server_description_filter_stale (const mongoc_server_description_t **sds,
                                        size_t sds_len,
                                        const mongoc_server_description_t *primary,
                                        int64_t heartbeat_frequency_ms,
                                        const mongoc_read_prefs_t *read_prefs);

void
mongoc_server_description_filter_tags (const mongoc_server_description_t **descriptions,
                                       size_t description_len,
                                       const mongoc_read_prefs_t *read_prefs);

/* Compares server descriptions following the "Server Description Equality"
 * rules. Not all fields are considered. */
bool
_mongoc_server_description_equal (mongoc_server_description_t *sd1, mongoc_server_description_t *sd2);

int
mongoc_server_description_topology_version_cmp (const bson_t *tv1, const bson_t *tv2);

void
mongoc_server_description_set_topology_version (mongoc_server_description_t *sd, const bson_t *tv);

extern const bson_oid_t kZeroServiceId;

bool
mongoc_server_description_has_service_id (const mongoc_server_description_t *description);

#endif
