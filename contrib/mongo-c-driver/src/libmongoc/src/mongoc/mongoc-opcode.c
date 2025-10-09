/*
 * Copyright 2023-present MongoDB, Inc.
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

#include "mongoc-opcode.h"

#include "mcd-rpc.h"

#include <bson/bson.h>

// Document and ensure consistency between equivalent macros in mcd-rpc and
// libmongoc.

BSON_STATIC_ASSERT (MONGOC_OP_CODE_COMPRESSED == MONGOC_OPCODE_COMPRESSED);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_MSG == MONGOC_OPCODE_MSG);

BSON_STATIC_ASSERT (MONGOC_OP_CODE_REPLY == MONGOC_OPCODE_REPLY);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_UPDATE == MONGOC_OPCODE_UPDATE);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_INSERT == MONGOC_OPCODE_INSERT);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_QUERY == MONGOC_OPCODE_QUERY);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_GET_MORE == MONGOC_OPCODE_GET_MORE);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_DELETE == MONGOC_OPCODE_DELETE);
BSON_STATIC_ASSERT (MONGOC_OP_CODE_KILL_CURSORS == MONGOC_OPCODE_KILL_CURSORS);
