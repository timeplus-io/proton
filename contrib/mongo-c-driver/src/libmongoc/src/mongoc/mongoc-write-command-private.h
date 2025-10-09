/*
 * Copyright 2014 MongoDB, Inc.
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

#ifndef MONGOC_WRITE_COMMAND_PRIVATE_H
#define MONGOC_WRITE_COMMAND_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-client.h"
#include "mongoc-error.h"
#include "mongoc-write-concern.h"
#include "mongoc-server-stream-private.h"
#include "mongoc-buffer-private.h"


BSON_BEGIN_DECLS

/* forward decl */
struct _mongoc_crud_opts_t;

#define MONGOC_WRITE_COMMAND_DELETE 0
#define MONGOC_WRITE_COMMAND_INSERT 1
#define MONGOC_WRITE_COMMAND_UPDATE 2

/* MongoDB has a extra allowance to allow updating 16mb document, as the update
 * operators would otherwise overflow the 16mb object limit. See SERVER-10643
 * for context. */
#define BSON_OBJECT_ALLOWANCE (16 * 1024)

#define RETRYABLE_WRITE_ERROR "RetryableWriteError"

struct _mongoc_bulk_write_flags_t {
   bool ordered;
   bool bypass_document_validation;
   bool has_collation;
   bool has_multi_write;
   bool has_array_filters;
   bool has_update_hint;
   bool has_delete_hint;
};


typedef struct {
   int type;
   mongoc_buffer_t payload;
   uint32_t n_documents;
   mongoc_bulk_write_flags_t flags;
   int64_t operation_id;
   bson_t cmd_opts;
} mongoc_write_command_t;


typedef struct {
   uint32_t nInserted;
   uint32_t nMatched;
   uint32_t nModified;
   uint32_t nRemoved;
   uint32_t nUpserted;
   /* like [{"index": int, "code": int, "errmsg": str}, ...] */
   bson_t writeErrors;
   /* like [{"index": int, "_id": value}, ...] */
   bson_t upserted;
   uint32_t n_writeConcernErrors;
   /* like [{"code": 64, "errmsg": "duplicate"}, ...] */
   bson_t writeConcernErrors;
   /* like ["TransientTransactionError", ...] */
   bson_t errorLabels;
   bool failed;    /* The command failed */
   bool must_stop; /* The stream may have been disconnected */
   bson_error_t error;
   uint32_t upsert_append_count;
   /* If the command initially failed with a retryable write, and selected a new
    * primary, this contains the server id of the newly selected primary. Only
    * applies to OP_MSG. Is left at 0 if no retry occurs. */
   uint32_t retry_server_id;
   /* store the raw server response if an error occured */
   uint32_t n_errorReplies;
   bson_t rawErrorReplies;
} mongoc_write_result_t;


typedef enum {
   MONGOC_WRITE_ERR_NONE,
   MONGOC_WRITE_ERR_OTHER,
   MONGOC_WRITE_ERR_RETRY,
   MONGOC_WRITE_ERR_WRITE_CONCERN,
} mongoc_write_err_type_t;


void
_mongoc_write_command_destroy (mongoc_write_command_t *command);
void
_mongoc_write_command_init (bson_t *doc, mongoc_write_command_t *command, const char *collection);
void
_mongoc_write_command_init_insert (mongoc_write_command_t *command,
                                   const bson_t *document,
                                   const bson_t *cmd_opts,
                                   mongoc_bulk_write_flags_t flags,
                                   int64_t operation_id);
void
_mongoc_write_command_init_insert_idl (mongoc_write_command_t *command,
                                       const bson_t *document,
                                       const bson_t *cmd_opts,
                                       int64_t operation_id);
void
_mongoc_write_command_init_delete (mongoc_write_command_t *command,
                                   const bson_t *selectors,
                                   const bson_t *cmd_opts,
                                   const bson_t *opts,
                                   mongoc_bulk_write_flags_t flags,
                                   int64_t operation_id);
void
_mongoc_write_command_init_delete_idl (mongoc_write_command_t *command,
                                       const bson_t *selector,
                                       const bson_t *cmd_opts,
                                       const bson_t *opts,
                                       int64_t operation_id);
void
_mongoc_write_command_init_update (mongoc_write_command_t *command,
                                   const bson_t *selector,
                                   const bson_t *update,
                                   const bson_t *cmd_opts,
                                   const bson_t *opts,
                                   mongoc_bulk_write_flags_t flags,
                                   int64_t operation_id);
void
_mongoc_write_command_init_update_idl (mongoc_write_command_t *command,
                                       const bson_t *selector,
                                       const bson_t *update,
                                       const bson_t *cmd_opts,
                                       const bson_t *opts,
                                       int64_t operation_id);
void
_mongoc_write_command_insert_append (mongoc_write_command_t *command, const bson_t *document);
void
_mongoc_write_command_update_append (mongoc_write_command_t *command,
                                     const bson_t *selector,
                                     const bson_t *update,
                                     const bson_t *opts);

void
_mongoc_write_command_delete_append (mongoc_write_command_t *command, const bson_t *selector, const bson_t *opts);

void
_mongoc_write_command_execute (mongoc_write_command_t *command,
                               mongoc_client_t *client,
                               mongoc_server_stream_t *server_stream,
                               const char *database,
                               const char *collection,
                               const mongoc_write_concern_t *write_concern,
                               uint32_t offset,
                               mongoc_client_session_t *cs,
                               mongoc_write_result_t *result);
void
_mongoc_write_command_execute_idl (mongoc_write_command_t *command,
                                   mongoc_client_t *client,
                                   mongoc_server_stream_t *server_stream,
                                   const char *database,
                                   const char *collection,
                                   uint32_t offset,
                                   const struct _mongoc_crud_opts_t *crud,
                                   mongoc_write_result_t *result);
void
_mongoc_write_result_init (mongoc_write_result_t *result);
#define MONGOC_WRITE_RESULT_COMPLETE(_result, ...) _mongoc_write_result_complete (_result, __VA_ARGS__, NULL)
bool
_mongoc_write_result_complete (mongoc_write_result_t *result,
                               int32_t error_api_version,
                               const mongoc_write_concern_t *wc,
                               mongoc_error_domain_t err_domain_override,
                               bson_t *reply,
                               bson_error_t *error,
                               ...);
void
_mongoc_write_result_destroy (mongoc_write_result_t *result);

mongoc_write_err_type_t
_mongoc_write_error_get_type (bson_t *reply);

bool
_mongoc_write_error_update_if_unsupported_storage_engine (bool cmd_ret, bson_error_t *cmd_err, bson_t *reply);

BSON_END_DECLS


#endif /* MONGOC_WRITE_COMMAND_PRIVATE_H */
