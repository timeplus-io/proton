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

#include "mongoc-prelude.h"

#include <bson/bson.h>
#include <stddef.h>

#include "mongoc-server-description.h"

BSON_BEGIN_DECLS

typedef enum { MONGOC_READ_ERR_NONE, MONGOC_READ_ERR_OTHER, MONGOC_READ_ERR_RETRY } mongoc_read_err_type_t;

/* Server error codes libmongoc cares about. Compare with:
 * https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
 */
typedef enum {
   MONGOC_SERVER_ERR_HOSTUNREACHABLE = 6,
   MONGOC_SERVER_ERR_HOSTNOTFOUND = 7,
   MONGOC_SERVER_ERR_CURSOR_NOT_FOUND = 43,
   MONGOC_SERVER_ERR_STALESHARDVERSION = 63,
   MONGOC_SERVER_ERR_NETWORKTIMEOUT = 89,
   MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS = 91,
   MONGOC_SERVER_ERR_FAILEDTOSATISFYREADPREFERENCE = 133,
   MONGOC_SERVER_ERR_READCONCERNMAJORITYNOTAVAILABLEYET = 134,
   MONGOC_SERVER_ERR_STALEEPOCH = 150,
   MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN = 189,
   MONGOC_SERVER_ERR_ELECTIONINPROGRESS = 216,
   MONGOC_SERVER_ERR_RETRYCHANGESTREAM = 234,
   MONGOC_SERVER_ERR_EXCEEDEDTIMELIMIT = 262,
   MONGOC_SERVER_ERR_SOCKETEXCEPTION = 9001,
   MONGOC_SERVER_ERR_NOTPRIMARY = 10107,
   MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN = 11600,
   MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE = 11602,
   MONGOC_SERVER_ERR_STALECONFIG = 13388,
   MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK = 13435,
   MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY = 13436,
   MONGOC_SERVER_ERR_LEGACYNOTPRIMARY = 10058,
   MONGOC_SERVER_ERR_NS_NOT_FOUND = 26
} mongoc_server_err_t;

mongoc_read_err_type_t
_mongoc_read_error_get_type (bool cmd_ret, const bson_error_t *cmd_err, const bson_t *reply);

void
_mongoc_error_copy_labels_and_upsert (const bson_t *src, bson_t *dst, char *label);

void
_mongoc_write_error_append_retryable_label (bson_t *reply);

void
_mongoc_write_error_handle_labels (bool cmd_ret,
                                   const bson_error_t *cmd_err,
                                   bson_t *reply,
                                   const mongoc_server_description_t *sd);

bool
_mongoc_error_is_shutdown (bson_error_t *error);

bool
_mongoc_error_is_recovering (bson_error_t *error);

bool
_mongoc_error_is_not_primary (bson_error_t *error);

bool
_mongoc_error_is_state_change (bson_error_t *error);

bool
_mongoc_error_is_network (const bson_error_t *error);

bool
_mongoc_error_is_server (const bson_error_t *error);

bool
_mongoc_error_is_auth (const bson_error_t *error);

BSON_END_DECLS
