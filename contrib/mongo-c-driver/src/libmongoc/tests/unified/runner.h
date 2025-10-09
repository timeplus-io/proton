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

#ifndef UNIFIED_RUNNER_H
#define UNIFIED_RUNNER_H

#include "bson/bson.h"
#include "bsonutil/bson-parser.h"
#include "entity-map.h"
#include "mongoc/mongoc-array-private.h"
#include "test-conveniences.h"
#include "TestSuite.h"

/* test_runner_t, test_file_t, and test_t model the types described in the "Test
 * Runner Implementation" section of the Unified Test Format specification. */
typedef struct {
   mongoc_client_t *internal_client;
   semver_t server_version;
   /* topology_type may be "single", "replicaset", "sharded",
    * "sharded-replicaset", or "load-balanced". */
   const char *topology_type;
   mongoc_array_t server_ids;
   bson_t *server_parameters;
   bool is_serverless;
} test_runner_t;

typedef struct {
   test_runner_t *test_runner;

   char *description;
   semver_t schema_version;
   bson_t *run_on_requirements;
   bson_t *create_entities;
   bson_t *initial_data;
   bson_t *yaml_anchors;
   bson_t *tests;
} test_file_t;

typedef struct _failpoint_t failpoint_t;
typedef struct {
   test_file_t *test_file;

   char *description;
   bson_t *run_on_requirements;
   char *skip_reason;
   bson_t *operations;
   bson_t *expect_events;
   bson_t *outcome;
   entity_map_t *entity_map;
   failpoint_t *failpoints;
   bool loop_operation_executed;
} test_t;

/* Set server_id to 0 if the failpoint was not against a pinned mongos. */
void
register_failpoint (test_t *test, char *failpoint, char *client_id, uint32_t server_id);

/* Run a directory of test files through the unified test runner. */
void
run_unified_tests (TestSuite *suite, const char *base, const char *subdir);

void
run_one_test_file (bson_t *bson);

#endif /* UNIFIED_RUNNER_H */
