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

#ifndef MONGOC_FLAGS_PRIVATE_H
#define MONGOC_FLAGS_PRIVATE_H

#include <bson/bson.h>

BSON_BEGIN_DECLS

/**
 * mongoc_op_msg_flags_t:
 * @MONGOC_MSG_CHECKSUM_PRESENT: The message ends with 4 bytes containing a
 * CRC-32C checksum.
 * @MONGOC_MSG_MORE_TO_COME: If set to 0, wait for a server response. If set to
 * 1, do not expect a server response.
 * @MONGOC_MSG_EXHAUST_ALLOWED: If set, allows multiple replies to this request
 * using the moreToCome bit.
 */
typedef enum {
   MONGOC_MSG_NONE = 0,
   MONGOC_MSG_CHECKSUM_PRESENT = 1 << 0,
   MONGOC_MSG_MORE_TO_COME = 1 << 1,
   MONGOC_MSG_EXHAUST_ALLOWED = 1 << 16,
} mongoc_op_msg_flags_t;


BSON_END_DECLS


#endif /* MONGOC_FLAGS_PRIVATE_H */
