/*
 * Copyright 2017 MongoDB, Inc.
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


/*
 * Internal struct to represent a command we will send to the server - command
 * parameters are collected in a mongoc_cmd_parts_t until we know the server's
 * wire version and whether it is mongos, then we collect the parts into a
 * mongoc_cmd_t, and gather that into an RPC message.
 */

#ifndef MONGOC_CMD_PRIVATE_H
#define MONGOC_CMD_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-server-api.h"
#include "mongoc-server-stream-private.h"
#include "mongoc-read-prefs.h"
#include "mongoc.h"
#include "mongoc-opts-private.h"

BSON_BEGIN_DECLS

#define MONGOC_DEFAULT_RETRYREADS true
/* retryWrites requires sessions, which require crypto */
#ifdef MONGOC_ENABLE_CRYPTO
#define MONGOC_DEFAULT_RETRYWRITES true
#else
#define MONGOC_DEFAULT_RETRYWRITES false
#endif

typedef enum {
   MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_UNKNOWN,
   MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_YES,
   MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_NO
} mongoc_cmd_parts_allow_txn_number_t;

// `mongoc_cmd_payload_t` represents a document sequence (OP_MSG Section with payloadType=1).
typedef struct {
   int32_t size;
   const char *identifier;
   const uint8_t *documents;
} mongoc_cmd_payload_t;

// OP_MSG supports any number of document sequences. Increase array size to support more document sequences.
#define MONGOC_CMD_PAYLOADS_COUNT_MAX 2

typedef struct _mongoc_cmd_t {
   const char *db_name;
   mongoc_query_flags_t query_flags;
   const bson_t *command;
   const char *command_name;
   size_t payloads_count;
   // `payloads[i]` may be read only when `0 <= i < payloads_count`.
   mongoc_cmd_payload_t payloads[MONGOC_CMD_PAYLOADS_COUNT_MAX];
   mongoc_server_stream_t *server_stream;
   int64_t operation_id;
   mongoc_client_session_t *session;
   mongoc_server_api_t *api;
   bool is_acknowledged;
   bool is_txn_finish;
   bool op_msg_is_exhaust;
} mongoc_cmd_t;


typedef struct _mongoc_cmd_parts_t {
   mongoc_cmd_t assembled;
   mongoc_query_flags_t user_query_flags;
   const bson_t *body;
   bson_t read_concern_document;
   bson_t write_concern_document;
   bson_t extra;
   const mongoc_read_prefs_t *read_prefs;
   bson_t assembled_body;
   bool is_read_command;
   bool is_write_command;
   bool prohibit_lsid;
   mongoc_cmd_parts_allow_txn_number_t allow_txn_number;
   bool is_retryable_read;
   bool is_retryable_write;
   bool has_temp_session;
   mongoc_client_t *client;
   mongoc_server_api_t *api;
} mongoc_cmd_parts_t;


void
mongoc_cmd_parts_init (mongoc_cmd_parts_t *op,
                       mongoc_client_t *client,
                       const char *db_name,
                       mongoc_query_flags_t user_query_flags,
                       const bson_t *command_body);

void
mongoc_cmd_parts_set_session (mongoc_cmd_parts_t *parts, mongoc_client_session_t *cs);

void
mongoc_cmd_parts_set_server_api (mongoc_cmd_parts_t *parts, mongoc_server_api_t *api);

bool
mongoc_cmd_parts_append_opts (mongoc_cmd_parts_t *parts, bson_iter_t *iter, bson_error_t *error);

bool
mongoc_cmd_parts_set_read_concern (mongoc_cmd_parts_t *parts, const mongoc_read_concern_t *rc, bson_error_t *error);

bool
mongoc_cmd_parts_set_write_concern (mongoc_cmd_parts_t *parts, const mongoc_write_concern_t *wc, bson_error_t *error);

bool
mongoc_cmd_parts_append_read_write (mongoc_cmd_parts_t *parts, mongoc_read_write_opts_t *rw_opts, bson_error_t *error);

bool
mongoc_cmd_parts_assemble (mongoc_cmd_parts_t *parts, mongoc_server_stream_t *server_stream, bson_error_t *error);

bool
mongoc_cmd_is_compressible (const mongoc_cmd_t *cmd);

void
mongoc_cmd_parts_cleanup (mongoc_cmd_parts_t *op);

bool
_is_retryable_read (const mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream);

void
_mongoc_cmd_append_payload_as_array (const mongoc_cmd_t *cmd, bson_t *out);

void
_mongoc_cmd_append_server_api (bson_t *command_body, const mongoc_server_api_t *api);

BSON_END_DECLS


#endif /* MONGOC_CMD_PRIVATE_H */
