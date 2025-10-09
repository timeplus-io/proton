/*
 * Copyright 2018-present MongoDB, Inc.
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

#ifndef JSON_TEST_OPERATIONS_H
#define JSON_TEST_OPERATIONS_H

struct _json_test_config_t;

#include "mongoc/mongoc-thread-private.h"
#include "common-thread-private.h"
#include "mock_server/sync-queue.h"

struct _json_test_ctx_t;

typedef struct {
   struct _json_test_ctx_t *ctx;
   bson_thread_t thread;
   sync_queue_t *queue;
   bson_mutex_t mutex;
   mongoc_cond_t cond;
   bool shutdown_requested;
} json_test_worker_thread_t;

typedef struct _json_test_ctx_t {
   const struct _json_test_config_t *config;
   uint32_t n_events;
   bson_t events;
   mongoc_uri_t *test_framework_uri;
   mongoc_client_session_t *sessions[2];
   bson_t lsids[2];
   bool acknowledged;
   bool verbose;
   bool has_sessions;
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;
   mongoc_change_stream_t *change_stream;
   /* Sessions tests check the most recently sent two lsid's */
   bson_t *sent_lsids[2];
   bson_mutex_t mutex;
   /* The total number of times a server description was marked unknown. */
   int64_t total_ServerMarkedUnknownEvent;
   /* How many were accounted for in the test. */
   int64_t measured_ServerMarkedUnknownEvent;
   /* How many connection generation increments were accounted for in the test.
    */
   int64_t measured_PoolClearedEvent;
   mongoc_host_list_t primary_host;

   int64_t total_PrimaryChangedEvent;
   int64_t measured_PrimaryChangedEvent;

   json_test_worker_thread_t *worker_threads[2];
} json_test_ctx_t;

mongoc_client_session_t *
session_from_name (json_test_ctx_t *ctx, const char *session_name);

void
json_test_ctx_init (json_test_ctx_t *ctx,
                    const bson_t *test,
                    mongoc_client_t *client,
                    mongoc_database_t *db,
                    mongoc_collection_t *collection,
                    const struct _json_test_config_t *config);

void
json_test_ctx_end_sessions (json_test_ctx_t *ctx);

void
json_test_ctx_cleanup (json_test_ctx_t *ctx);

typedef bool (*json_test_operation_cb_t) (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation);

typedef void (*json_test_cb_t) (json_test_ctx_t *ctx, const bson_t *test);

bool
json_test_operation (json_test_ctx_t *ctx,
                     const bson_t *test,
                     const bson_t *operation,
                     mongoc_collection_t *collection,
                     mongoc_client_session_t *session,
                     bson_t *reply);

void
json_test_operations (json_test_ctx_t *ctx, const bson_t *test);

void
check_result (
   const bson_t *test, const bson_t *operation, bool succeeded, const bson_value_t *result, const bson_error_t *error);

json_test_worker_thread_t *
worker_thread_new (json_test_ctx_t *ctx);

void
worker_thread_destroy (json_test_worker_thread_t *wt);

#endif
