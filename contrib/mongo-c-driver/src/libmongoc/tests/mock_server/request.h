/*
 * Copyright 2015 MongoDB, Inc.
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

#ifndef REQUEST_H
#define REQUEST_H

#include <bson/bson.h>
#include <mongoc/mongoc-buffer-private.h>
#include <mongoc/mcd-rpc.h>

#include "mongoc/mongoc.h"

#include "mongoc/mongoc-array-private.h"
#include "mongoc/mongoc-rpc-private.h"
#include "sync-queue.h"

struct _mock_server_t; /* forward declaration */

typedef struct _request_t {
   uint8_t *data;
   size_t data_len;
   mcd_rpc_message *rpc;
   int32_t opcode; /* copied from rpc for convenience */
   struct _mock_server_t *server;
   mongoc_stream_t *client;
   uint16_t client_port;
   bool is_command;
   char *command_name;
   char *as_str;
   mongoc_array_t docs; /* array of bson_t pointers */
   sync_queue_t *replies;
} request_t;


request_t *
request_new (const mongoc_buffer_t *buffer,
             int32_t msg_len,
             struct _mock_server_t *server,
             mongoc_stream_t *client,
             uint16_t client_port,
             sync_queue_t *replies);

const bson_t *
request_get_doc (const request_t *request, size_t n);

void
assert_request_matches_flags (const request_t *request, uint32_t flags);

bool
request_matches_query (const request_t *request,
                       const char *ns,
                       uint32_t flags,
                       uint32_t skip,
                       int32_t n_return,
                       const char *query_json,
                       const char *fields_json,
                       bool is_command);

bool
request_matches_kill_cursors (const request_t *request, int64_t cursor_id);

bool
request_matches_msg (const request_t *request, uint32_t flags, const bson_t **docs, size_t n_docs);

bool
request_matches_msgv (const request_t *request, uint32_t flags, va_list *args);

uint16_t
request_get_server_port (request_t *request);

uint16_t
request_get_client_port (request_t *request);

void
request_destroy (request_t *request);

#endif /* REQUEST_H */
