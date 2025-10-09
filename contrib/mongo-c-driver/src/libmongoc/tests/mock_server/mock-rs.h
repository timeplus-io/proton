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

#ifndef MOCK_RS_H
#define MOCK_RS_H

#include "mongoc/mongoc.h"

#include "mock-server.h"

typedef struct _mock_rs_t mock_rs_t;

mock_rs_t *
mock_rs_with_auto_hello (int32_t max_wire_version, bool has_primary, int n_secondaries, int n_arbiters);


void
mock_rs_tag_secondary (mock_rs_t *rs, int server_number, const bson_t *tags);

void
mock_rs_set_request_timeout_msec (mock_rs_t *rs, int64_t request_timeout_msec);

void
mock_rs_run (mock_rs_t *rs);

const mongoc_uri_t *
mock_rs_get_uri (mock_rs_t *rs);

request_t *
mock_rs_receives_request (mock_rs_t *rs);

request_t *
mock_rs_receives_kill_cursors (mock_rs_t *rs, int64_t cursor_id);

request_t *
_mock_rs_receives_msg (mock_rs_t *rs, uint32_t flags, ...);

#define mock_rs_receives_msg(_rs, _flags, ...) _mock_rs_receives_msg (_rs, _flags, __VA_ARGS__, NULL)

bool
mock_rs_request_is_to_primary (mock_rs_t *rs, request_t *request);

bool
mock_rs_request_is_to_secondary (mock_rs_t *rs, request_t *request);

void
mock_rs_stepdown (mock_rs_t *rs);

void
mock_rs_elect (mock_rs_t *rs, size_t id);

void
mock_rs_destroy (mock_rs_t *rs);

#endif /* MOCK_RS_H */
