
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

#include "mongoc-prelude.h"

#ifndef MONGOC_TOPOLOGY_BACKGROUND_MONITORING_PRIVATE_H
#define MONGOC_TOPOLOGY_BACKGROUND_MONITORING_PRIVATE_H

#include "mongoc.h"
#include "mongoc-topology-private.h"

/* Methods of mongoc_topology_t for managing background monitoring. */

void
_mongoc_topology_background_monitoring_start (mongoc_topology_t *topology);

void
_mongoc_topology_background_monitoring_reconcile (mongoc_topology_t *topology, mongoc_topology_description_t *td);

void
_mongoc_topology_background_monitoring_request_scan (mongoc_topology_t *topology);

void
_mongoc_topology_background_monitoring_stop (mongoc_topology_t *topology);

void
_mongoc_topology_background_monitoring_cancel_check (mongoc_topology_t *topology, uint32_t server_id);

#endif /* MONGOC_TOPOLOGY_BACKGROUND_MONITORING_PRIVATE_H */
