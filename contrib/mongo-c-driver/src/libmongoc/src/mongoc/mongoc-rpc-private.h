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

#ifndef MONGOC_RPC_PRIVATE_H
#define MONGOC_RPC_PRIVATE_H

#include "mongoc-prelude.h"

#include "mcd-rpc.h"

#include <bson/bson.h>

#include <stdbool.h>
#include <stdint.h>

BSON_BEGIN_DECLS

bool
mcd_rpc_message_get_body (const mcd_rpc_message *rpc, bson_t *reply);

bool
mcd_rpc_message_check_ok (mcd_rpc_message *rpc,
                          int32_t error_api_version,
                          bson_error_t *error /* OUT */,
                          bson_t *error_doc /* OUT */);
bool
_mongoc_cmd_check_ok (const bson_t *doc, int32_t error_api_version, bson_error_t *error);

bool
_mongoc_cmd_check_ok_no_wce (const bson_t *doc, int32_t error_api_version, bson_error_t *error);

void
mcd_rpc_message_egress (const mcd_rpc_message *rpc);
void
mcd_rpc_message_ingress (const mcd_rpc_message *rpc);

BSON_END_DECLS


#endif /* MONGOC_RPC_PRIVATE_H */
