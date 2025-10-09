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

#include <bson/bson.h>

#include "mongoc-error.h"
#include "mongoc-error-private.h"
#include "mongoc-rpc-private.h"
#include "mongoc-client-private.h"
#include "mongoc-server-description-private.h"

bool
mongoc_error_has_label (const bson_t *reply, const char *label)
{
   bson_iter_t iter;
   bson_iter_t error_labels;

   BSON_ASSERT (reply);
   BSON_ASSERT (label);

   if (bson_iter_init_find (&iter, reply, "errorLabels") && bson_iter_recurse (&iter, &error_labels)) {
      while (bson_iter_next (&error_labels)) {
         if (BSON_ITER_HOLDS_UTF8 (&error_labels) && !strcmp (bson_iter_utf8 (&error_labels, NULL), label)) {
            return true;
         }
      }
   }

   return false;
}

bool
_mongoc_error_is_server (const bson_error_t *error)
{
   if (!error) {
      return false;
   }

   return error->domain == MONGOC_ERROR_SERVER || error->domain == MONGOC_ERROR_WRITE_CONCERN;
}

static bool
_mongoc_write_error_is_retryable (bson_error_t *error)
{
   if (!_mongoc_error_is_server (error)) {
      return false;
   }

   switch (error->code) {
   case MONGOC_SERVER_ERR_HOSTUNREACHABLE:
   case MONGOC_SERVER_ERR_HOSTNOTFOUND:
   case MONGOC_SERVER_ERR_NETWORKTIMEOUT:
   case MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS:
   case MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN:
   case MONGOC_SERVER_ERR_EXCEEDEDTIMELIMIT:
   case MONGOC_SERVER_ERR_SOCKETEXCEPTION:
   case MONGOC_SERVER_ERR_NOTPRIMARY:
   case MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN:
   case MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE:
   case MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK:
   case MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY:
      return true;
   default:
      return false;
   }
}

void
_mongoc_write_error_append_retryable_label (bson_t *reply)
{
   bson_t reply_local = BSON_INITIALIZER;

   if (!reply) {
      bson_destroy (&reply_local);
      return;
   }

   bson_copy_to_excluding_noinit (reply, &reply_local, "errorLabels", NULL);
   _mongoc_error_copy_labels_and_upsert (reply, &reply_local, RETRYABLE_WRITE_ERROR);

   bson_destroy (reply);
   bson_steal (reply, &reply_local);
}

void
_mongoc_write_error_handle_labels (bool cmd_ret,
                                   const bson_error_t *cmd_err,
                                   bson_t *reply,
                                   const mongoc_server_description_t *sd)
{
   bson_error_t error;

   /* check for a client error. */
   if (!cmd_ret && _mongoc_error_is_network (cmd_err)) {
      /* Retryable writes spec: When the driver encounters a network error
       * communicating with any server version that supports retryable
       * writes, it MUST add a RetryableWriteError label to that error. */
      _mongoc_write_error_append_retryable_label (reply);
      return;
   }

   if (sd->max_wire_version >= WIRE_VERSION_RETRYABLE_WRITE_ERROR_LABEL) {
      return;
   }

   /* Check for a server error. Do not consult writeConcernError for pre-4.4
    * mongos. */
   if (sd->type == MONGOC_SERVER_MONGOS) {
      if (_mongoc_cmd_check_ok (reply, MONGOC_ERROR_API_VERSION_2, &error)) {
         return;
      }
   } else {
      if (_mongoc_cmd_check_ok_no_wce (reply, MONGOC_ERROR_API_VERSION_2, &error)) {
         return;
      }
   }

   if (_mongoc_write_error_is_retryable (&error)) {
      _mongoc_write_error_append_retryable_label (reply);
   }
}


/*--------------------------------------------------------------------------
 *
 * _mongoc_read_error_get_type --
 *
 *       Checks if the error or reply from a read command is considered
 *       retryable according to the retryable reads spec. Checks both
 *       for a client error (a network exception) and a server error in
 *       the reply. @cmd_ret and @cmd_err come from the result of a
 *       read_command function.
 *
 *
 * Return:
 *       A mongoc_read_error_type_t indicating the type of error (if any).
 *
 *--------------------------------------------------------------------------
 */
mongoc_read_err_type_t
_mongoc_read_error_get_type (bool cmd_ret, const bson_error_t *cmd_err, const bson_t *reply)
{
   bson_error_t error;

   /* check for a client error. */
   if (!cmd_ret && cmd_err && _mongoc_error_is_network (cmd_err)) {
      /* Retryable reads spec: "considered retryable if [...] any network
       * exception (e.g. socket timeout or error) */
      return MONGOC_READ_ERR_RETRY;
   }

   /* check for a server error. */
   if (_mongoc_cmd_check_ok_no_wce (reply, MONGOC_ERROR_API_VERSION_2, &error)) {
      return MONGOC_READ_ERR_NONE;
   }

   switch (error.code) {
   case MONGOC_SERVER_ERR_EXCEEDEDTIMELIMIT:
   case MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN:
   case MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE:
   case MONGOC_SERVER_ERR_NOTPRIMARY:
   case MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK:
   case MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY:
   case MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN:
   case MONGOC_SERVER_ERR_READCONCERNMAJORITYNOTAVAILABLEYET:
   case MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS:
   case MONGOC_SERVER_ERR_HOSTNOTFOUND:
   case MONGOC_SERVER_ERR_HOSTUNREACHABLE:
   case MONGOC_SERVER_ERR_NETWORKTIMEOUT:
   case MONGOC_SERVER_ERR_SOCKETEXCEPTION:
      return MONGOC_READ_ERR_RETRY;
   default:
      if (strstr (error.message, "not master") || strstr (error.message, "node is recovering")) {
         return MONGOC_READ_ERR_RETRY;
      }
      return MONGOC_READ_ERR_OTHER;
   }
}

void
_mongoc_error_copy_labels_and_upsert (const bson_t *src, bson_t *dst, char *label)
{
   bson_iter_t iter;
   bson_iter_t src_label;
   bson_array_builder_t *dst_labels;

   BSON_APPEND_ARRAY_BUILDER_BEGIN (dst, "errorLabels", &dst_labels);
   bson_array_builder_append_utf8 (dst_labels, label, -1);

   /* append any other errorLabels already in "src" */
   if (bson_iter_init_find (&iter, src, "errorLabels") && bson_iter_recurse (&iter, &src_label)) {
      while (bson_iter_next (&src_label) && BSON_ITER_HOLDS_UTF8 (&src_label)) {
         if (strcmp (bson_iter_utf8 (&src_label, NULL), label) != 0) {
            bson_array_builder_append_utf8 (dst_labels, bson_iter_utf8 (&src_label, NULL), -1);
         }
      }
   }

   bson_append_array_builder_end (dst, dst_labels);
}

/* Defined in SDAM spec under "Application Errors".
 * @error should have been obtained from a command reply, e.g. with
 * _mongoc_cmd_check_ok.
 */
bool
_mongoc_error_is_shutdown (bson_error_t *error)
{
   if (!_mongoc_error_is_server (error)) {
      return false;
   }
   switch (error->code) {
   case 11600: /* InterruptedAtShutdown */
   case 91:    /* ShutdownInProgress */
      return true;
   default:
      return false;
   }
}

bool
_mongoc_error_is_not_primary (bson_error_t *error)
{
   if (!_mongoc_error_is_server (error)) {
      return false;
   }

   if (_mongoc_error_is_recovering (error)) {
      return false;
   }
   switch (error->code) {
   case MONGOC_SERVER_ERR_NOTPRIMARY:
   case MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK:
   case MONGOC_SERVER_ERR_LEGACYNOTPRIMARY:
      return true;
      /* All errors where no code was found are marked as
       * MONGOC_ERROR_QUERY_FAILURE */
   case MONGOC_ERROR_QUERY_FAILURE:
      return NULL != strstr (error->message, "not master");
   default:
      return false;
   }
}

bool
_mongoc_error_is_recovering (bson_error_t *error)
{
   if (!_mongoc_error_is_server (error)) {
      return false;
   }
   switch (error->code) {
   case MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN:
   case MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE:
   case MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY:
   case MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN:
   case MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS:
      return true;
   /* All errors where no code was found are marked as
    * MONGOC_ERROR_QUERY_FAILURE */
   case MONGOC_ERROR_QUERY_FAILURE:
      return NULL != strstr (error->message, "not master or secondary") ||
             NULL != strstr (error->message, "node is recovering");
   default:
      return false;
   }
}

/* Assumes @error was parsed as an API V2 error. */
bool
_mongoc_error_is_state_change (bson_error_t *error)
{
   return _mongoc_error_is_recovering (error) || _mongoc_error_is_not_primary (error);
}

bool
_mongoc_error_is_network (const bson_error_t *error)
{
   if (!error) {
      return false;
   }
   if (error->domain == MONGOC_ERROR_STREAM) {
      return true;
   }

   if (error->domain == MONGOC_ERROR_PROTOCOL && error->code == MONGOC_ERROR_PROTOCOL_INVALID_REPLY) {
      return true;
   }

   return false;
}

bool
_mongoc_error_is_auth (const bson_error_t *error)
{
   if (!error) {
      return false;
   }

   return error->domain == MONGOC_ERROR_CLIENT && error->code == MONGOC_ERROR_CLIENT_AUTHENTICATE;
}
