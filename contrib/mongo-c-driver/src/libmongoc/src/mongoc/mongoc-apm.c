/*
 * Copyright 2016 MongoDB, Inc.
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

#include "mongoc-util-private.h"
#include "mongoc-apm-private.h"
#include "mongoc-cmd-private.h"
#include "mongoc-handshake-private.h"

static bson_oid_t kObjectIdZero = {{0}};

/*
 * An Application Performance Management (APM) implementation, complying with
 * MongoDB's Command Logging and Monitoring Spec:
 *
 * https://github.com/mongodb/specifications/tree/master/source/command-logging-and-monitoring
 */

static void
append_documents_from_cmd (const mongoc_cmd_t *cmd, mongoc_apm_command_started_t *event)
{
   // If there are no document sequences (OP_MSG Section with payloadType=1), return the command unchanged.
   if (cmd->payloads_count == 0) {
      return;
   }

   if (!event->command_owned) {
      event->command = bson_copy (event->command);
      event->command_owned = true;
   }

   _mongoc_cmd_append_payload_as_array (cmd, event->command);
}


/*
 * Private initializer / cleanup functions.
 */

static void
mongoc_apm_redact_command (bson_t *command);

static void
mongoc_apm_redact_reply (bson_t *reply);

/*--------------------------------------------------------------------------
 *
 * mongoc_apm_command_started_init --
 *
 *       Initialises the command started event.
 *
 * Side effects:
 *       If provided, is_redacted indicates whether the command document was
 *       redacted to hide sensitive information.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_apm_command_started_init (mongoc_apm_command_started_t *event,
                                 const bson_t *command,
                                 const char *database_name,
                                 const char *command_name,
                                 int64_t request_id,
                                 int64_t operation_id,
                                 const mongoc_host_list_t *host,
                                 uint32_t server_id,
                                 const bson_oid_t *service_id,
                                 int64_t server_connection_id,
                                 bool *is_redacted, /* out */
                                 void *context)
{
   bson_iter_t iter;
   uint32_t len;
   const uint8_t *data;

   /* Command Monitoring Spec:
    *
    * In cases where queries or commands are embedded in a $query parameter
    * when a read preference is provided, they MUST be unwrapped and the value
    * of the $query attribute becomes the filter or the command in the started
    * event. The read preference will subsequently be dropped as it is
    * considered metadata and metadata is not currently provided in the command
    * events.
    */
   if (bson_has_field (command, "$readPreference")) {
      if (bson_iter_init_find (&iter, command, "$query") && BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         bson_iter_document (&iter, &len, &data);
         event->command = bson_new_from_data (data, len);
         event->command_owned = true;
      } else {
         /* Got $readPreference without $query, probably OP_MSG */
         event->command = (bson_t *) command;
         event->command_owned = false;
      }
   } else {
      /* discard "const", we promise not to modify "command" */
      event->command = (bson_t *) command;
      event->command_owned = false;
   }

   if (mongoc_apm_is_sensitive_command_message (command_name, command)) {
      if (!event->command_owned) {
         event->command = bson_copy (event->command);
         event->command_owned = true;
      }

      if (is_redacted) {
         *is_redacted = true;
      }

      mongoc_apm_redact_command (event->command);
   } else if (is_redacted) {
      *is_redacted = false;
   }

   event->database_name = database_name;
   event->command_name = command_name;
   event->request_id = request_id;
   event->operation_id = operation_id;
   event->host = host;
   event->server_id = server_id;
   event->context = context;
   event->server_connection_id = server_connection_id;

   bson_oid_copy_unsafe (service_id, &event->service_id);
}


/*--------------------------------------------------------------------------
 *
 * mongoc_apm_command_started_init_with_cmd --
 *
 *       Initialises the command started event from a mongoc_cmd_t.
 *
 * Side effects:
 *       If provided, is_redacted indicates whether the command document was
 *       redacted to hide sensitive information.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_apm_command_started_init_with_cmd (mongoc_apm_command_started_t *event,
                                          mongoc_cmd_t *cmd,
                                          int64_t request_id,
                                          bool *is_redacted, /* out */
                                          void *context)
{
   mongoc_apm_command_started_init (event,
                                    cmd->command,
                                    cmd->db_name,
                                    cmd->command_name,
                                    request_id,
                                    cmd->operation_id,
                                    &cmd->server_stream->sd->host,
                                    cmd->server_stream->sd->id,
                                    &cmd->server_stream->sd->service_id,
                                    cmd->server_stream->sd->server_connection_id,
                                    is_redacted,
                                    context);

   /* OP_MSG document sequence for insert, update, or delete? */
   append_documents_from_cmd (cmd, event);
}


void
mongoc_apm_command_started_cleanup (mongoc_apm_command_started_t *event)
{
   if (event->command_owned) {
      bson_destroy (event->command);
   }
}


/*--------------------------------------------------------------------------
 *
 * mongoc_apm_command_succeeded_init --
 *
 *       Initialises the command succeeded event.
 *
 * Parameters:
 *       @force_redaction: If true, the reply document is always redacted,
 *       regardless of whether the command contains sensitive information.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_apm_command_succeeded_init (mongoc_apm_command_succeeded_t *event,
                                   int64_t duration,
                                   const bson_t *reply,
                                   const char *command_name,
                                   const char *database_name,
                                   int64_t request_id,
                                   int64_t operation_id,
                                   const mongoc_host_list_t *host,
                                   uint32_t server_id,
                                   const bson_oid_t *service_id,
                                   int64_t server_connection_id,
                                   bool force_redaction,
                                   void *context)
{
   BSON_ASSERT (reply);

   if (force_redaction || mongoc_apm_is_sensitive_command_message (command_name, reply)) {
      event->reply = bson_copy (reply);
      event->reply_owned = true;

      mongoc_apm_redact_reply (event->reply);
   } else {
      /* discard "const", we promise not to modify "reply" */
      event->reply = (bson_t *) reply;
      event->reply_owned = false;
   }

   event->duration = duration;
   event->command_name = command_name;
   event->database_name = database_name;
   event->request_id = request_id;
   event->operation_id = operation_id;
   event->host = host;
   event->server_id = server_id;
   event->server_connection_id = server_connection_id;
   event->context = context;

   bson_oid_copy_unsafe (service_id, &event->service_id);
}


void
mongoc_apm_command_succeeded_cleanup (mongoc_apm_command_succeeded_t *event)
{
   if (event->reply_owned) {
      bson_destroy (event->reply);
   }
}


/*--------------------------------------------------------------------------
 *
 * mongoc_apm_command_failed_init --
 *
 *       Initialises the command failed event.
 *
 * Parameters:
 *       @force_redaction: If true, the reply document is always redacted,
 *       regardless of whether the command contains sensitive information.
 *
 *--------------------------------------------------------------------------
 */
void
mongoc_apm_command_failed_init (mongoc_apm_command_failed_t *event,
                                int64_t duration,
                                const char *command_name,
                                const char *database_name,
                                const bson_error_t *error,
                                const bson_t *reply,
                                int64_t request_id,
                                int64_t operation_id,
                                const mongoc_host_list_t *host,
                                uint32_t server_id,
                                const bson_oid_t *service_id,
                                int64_t server_connection_id,
                                bool force_redaction,
                                void *context)
{
   BSON_ASSERT (reply);

   if (force_redaction || mongoc_apm_is_sensitive_command_message (command_name, reply)) {
      event->reply = bson_copy (reply);
      event->reply_owned = true;

      mongoc_apm_redact_reply (event->reply);
   } else {
      /* discard "const", we promise not to modify "reply" */
      event->reply = (bson_t *) reply;
      event->reply_owned = false;
   }

   event->duration = duration;
   event->command_name = command_name;
   event->database_name = database_name;
   event->error = error;
   event->request_id = request_id;
   event->operation_id = operation_id;
   event->host = host;
   event->server_id = server_id;
   event->server_connection_id = server_connection_id;
   event->context = context;

   bson_oid_copy_unsafe (service_id, &event->service_id);
}


void
mongoc_apm_command_failed_cleanup (mongoc_apm_command_failed_t *event)
{
   if (event->reply_owned) {
      bson_destroy (event->reply);
   }
}


/*
 * event field accessors
 */

/* command-started event fields */

const bson_t *
mongoc_apm_command_started_get_command (const mongoc_apm_command_started_t *event)
{
   return event->command;
}


const char *
mongoc_apm_command_started_get_database_name (const mongoc_apm_command_started_t *event)
{
   return event->database_name;
}


const char *
mongoc_apm_command_started_get_command_name (const mongoc_apm_command_started_t *event)
{
   return event->command_name;
}


int64_t
mongoc_apm_command_started_get_request_id (const mongoc_apm_command_started_t *event)
{
   return event->request_id;
}


int64_t
mongoc_apm_command_started_get_operation_id (const mongoc_apm_command_started_t *event)
{
   return event->operation_id;
}


const mongoc_host_list_t *
mongoc_apm_command_started_get_host (const mongoc_apm_command_started_t *event)
{
   return event->host;
}


uint32_t
mongoc_apm_command_started_get_server_id (const mongoc_apm_command_started_t *event)
{
   return event->server_id;
}


const bson_oid_t *
mongoc_apm_command_started_get_service_id (const mongoc_apm_command_started_t *event)
{
   if (0 == bson_oid_compare (&event->service_id, &kObjectIdZero)) {
      /* serviceId is unset. */
      return NULL;
   }

   return &event->service_id;
}


int32_t
mongoc_apm_command_started_get_server_connection_id (const mongoc_apm_command_started_t *event)
{
   if (event->server_connection_id > INT32_MAX || event->server_connection_id < INT32_MIN) {
      MONGOC_WARNING ("Server connection ID %" PRId64 " is outside of int32 range. Returning -1. Use "
                      "mongoc_apm_command_started_get_server_connection_id_int64.",
                      event->server_connection_id);
      return MONGOC_NO_SERVER_CONNECTION_ID;
   }

   return (int32_t) event->server_connection_id;
}


int64_t
mongoc_apm_command_started_get_server_connection_id_int64 (const mongoc_apm_command_started_t *event)
{
   return event->server_connection_id;
}


void *
mongoc_apm_command_started_get_context (const mongoc_apm_command_started_t *event)
{
   return event->context;
}


/* command-succeeded event fields */

int64_t
mongoc_apm_command_succeeded_get_duration (const mongoc_apm_command_succeeded_t *event)
{
   return event->duration;
}


const bson_t *
mongoc_apm_command_succeeded_get_reply (const mongoc_apm_command_succeeded_t *event)
{
   return event->reply;
}


const char *
mongoc_apm_command_succeeded_get_command_name (const mongoc_apm_command_succeeded_t *event)
{
   return event->command_name;
}

const char *
mongoc_apm_command_succeeded_get_database_name (const mongoc_apm_command_succeeded_t *event)
{
   return event->database_name;
}

int64_t
mongoc_apm_command_succeeded_get_request_id (const mongoc_apm_command_succeeded_t *event)
{
   return event->request_id;
}


int64_t
mongoc_apm_command_succeeded_get_operation_id (const mongoc_apm_command_succeeded_t *event)
{
   return event->operation_id;
}


const mongoc_host_list_t *
mongoc_apm_command_succeeded_get_host (const mongoc_apm_command_succeeded_t *event)
{
   return event->host;
}


uint32_t
mongoc_apm_command_succeeded_get_server_id (const mongoc_apm_command_succeeded_t *event)
{
   return event->server_id;
}


const bson_oid_t *
mongoc_apm_command_succeeded_get_service_id (const mongoc_apm_command_succeeded_t *event)
{
   if (0 == bson_oid_compare (&event->service_id, &kObjectIdZero)) {
      /* serviceId is unset. */
      return NULL;
   }

   return &event->service_id;
}


int32_t
mongoc_apm_command_succeeded_get_server_connection_id (const mongoc_apm_command_succeeded_t *event)
{
   if (event->server_connection_id > INT32_MAX || event->server_connection_id < INT32_MIN) {
      MONGOC_WARNING ("Server connection ID %" PRId64 " is outside of int32 range. Returning -1. Use "
                      "mongoc_apm_command_succeeded_get_server_connection_id_int64.",
                      event->server_connection_id);
      return MONGOC_NO_SERVER_CONNECTION_ID;
   }

   return (int32_t) event->server_connection_id;
}


int64_t
mongoc_apm_command_succeeded_get_server_connection_id_int64 (const mongoc_apm_command_succeeded_t *event)
{
   return event->server_connection_id;
}


void *
mongoc_apm_command_succeeded_get_context (const mongoc_apm_command_succeeded_t *event)
{
   return event->context;
}


/* command-failed event fields */

int64_t
mongoc_apm_command_failed_get_duration (const mongoc_apm_command_failed_t *event)
{
   return event->duration;
}


const char *
mongoc_apm_command_failed_get_command_name (const mongoc_apm_command_failed_t *event)
{
   return event->command_name;
}


void
mongoc_apm_command_failed_get_error (const mongoc_apm_command_failed_t *event, bson_error_t *error)
{
   memcpy (error, event->error, sizeof *event->error);
}

const bson_t *
mongoc_apm_command_failed_get_reply (const mongoc_apm_command_failed_t *event)
{
   return event->reply;
}

int64_t
mongoc_apm_command_failed_get_request_id (const mongoc_apm_command_failed_t *event)
{
   return event->request_id;
}


int64_t
mongoc_apm_command_failed_get_operation_id (const mongoc_apm_command_failed_t *event)
{
   return event->operation_id;
}


const mongoc_host_list_t *
mongoc_apm_command_failed_get_host (const mongoc_apm_command_failed_t *event)
{
   return event->host;
}


uint32_t
mongoc_apm_command_failed_get_server_id (const mongoc_apm_command_failed_t *event)
{
   return event->server_id;
}


const bson_oid_t *
mongoc_apm_command_failed_get_service_id (const mongoc_apm_command_failed_t *event)
{
   if (0 == bson_oid_compare (&event->service_id, &kObjectIdZero)) {
      /* serviceId is unset. */
      return NULL;
   }

   return &event->service_id;
}


int32_t
mongoc_apm_command_failed_get_server_connection_id (const mongoc_apm_command_failed_t *event)
{
   if (event->server_connection_id > INT32_MAX || event->server_connection_id < INT32_MIN) {
      MONGOC_WARNING ("Server connection ID %" PRId64 " is outside of int32 range. Returning -1. Use "
                      "mongoc_apm_command_failed_get_server_connection_id_int64.",
                      event->server_connection_id);
      return MONGOC_NO_SERVER_CONNECTION_ID;
   }

   return (int32_t) event->server_connection_id;
}


int64_t
mongoc_apm_command_failed_get_server_connection_id_int64 (const mongoc_apm_command_failed_t *event)
{
   return event->server_connection_id;
}


void *
mongoc_apm_command_failed_get_context (const mongoc_apm_command_failed_t *event)
{
   return event->context;
}


const char *
mongoc_apm_command_failed_get_database_name (const mongoc_apm_command_failed_t *event)
{
   return event->database_name;
}


/* server-changed event fields */

const mongoc_host_list_t *
mongoc_apm_server_changed_get_host (const mongoc_apm_server_changed_t *event)
{
   return event->host;
}


void
mongoc_apm_server_changed_get_topology_id (const mongoc_apm_server_changed_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


const mongoc_server_description_t *
mongoc_apm_server_changed_get_previous_description (const mongoc_apm_server_changed_t *event)
{
   return event->previous_description;
}


const mongoc_server_description_t *
mongoc_apm_server_changed_get_new_description (const mongoc_apm_server_changed_t *event)
{
   return event->new_description;
}


void *
mongoc_apm_server_changed_get_context (const mongoc_apm_server_changed_t *event)
{
   return event->context;
}


/* server-opening event fields */

const mongoc_host_list_t *
mongoc_apm_server_opening_get_host (const mongoc_apm_server_opening_t *event)
{
   return event->host;
}


void
mongoc_apm_server_opening_get_topology_id (const mongoc_apm_server_opening_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


void *
mongoc_apm_server_opening_get_context (const mongoc_apm_server_opening_t *event)
{
   return event->context;
}


/* server-closed event fields */

const mongoc_host_list_t *
mongoc_apm_server_closed_get_host (const mongoc_apm_server_closed_t *event)
{
   return event->host;
}


void
mongoc_apm_server_closed_get_topology_id (const mongoc_apm_server_closed_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


void *
mongoc_apm_server_closed_get_context (const mongoc_apm_server_closed_t *event)
{
   return event->context;
}


/* topology-changed event fields */

void
mongoc_apm_topology_changed_get_topology_id (const mongoc_apm_topology_changed_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


const mongoc_topology_description_t *
mongoc_apm_topology_changed_get_previous_description (const mongoc_apm_topology_changed_t *event)
{
   return event->previous_description;
}


const mongoc_topology_description_t *
mongoc_apm_topology_changed_get_new_description (const mongoc_apm_topology_changed_t *event)
{
   return event->new_description;
}


void *
mongoc_apm_topology_changed_get_context (const mongoc_apm_topology_changed_t *event)
{
   return event->context;
}


/* topology-opening event field */

void
mongoc_apm_topology_opening_get_topology_id (const mongoc_apm_topology_opening_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


void *
mongoc_apm_topology_opening_get_context (const mongoc_apm_topology_opening_t *event)
{
   return event->context;
}


/* topology-closed event field */

void
mongoc_apm_topology_closed_get_topology_id (const mongoc_apm_topology_closed_t *event, bson_oid_t *topology_id)
{
   bson_oid_copy (&event->topology_id, topology_id);
}


void *
mongoc_apm_topology_closed_get_context (const mongoc_apm_topology_closed_t *event)
{
   return event->context;
}


/* heartbeat-started event field */

const mongoc_host_list_t *
mongoc_apm_server_heartbeat_started_get_host (const mongoc_apm_server_heartbeat_started_t *event)
{
   return event->host;
}


void *
mongoc_apm_server_heartbeat_started_get_context (const mongoc_apm_server_heartbeat_started_t *event)
{
   return event->context;
}

bool
mongoc_apm_server_heartbeat_started_get_awaited (const mongoc_apm_server_heartbeat_started_t *event)
{
   return event->awaited;
}


/* heartbeat-succeeded event fields */

int64_t
mongoc_apm_server_heartbeat_succeeded_get_duration (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   return event->duration_usec;
}


const bson_t *
mongoc_apm_server_heartbeat_succeeded_get_reply (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   return event->reply;
}


const mongoc_host_list_t *
mongoc_apm_server_heartbeat_succeeded_get_host (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   return event->host;
}


void *
mongoc_apm_server_heartbeat_succeeded_get_context (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   return event->context;
}

bool
mongoc_apm_server_heartbeat_succeeded_get_awaited (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   return event->awaited;
}


/* heartbeat-failed event fields */

int64_t
mongoc_apm_server_heartbeat_failed_get_duration (const mongoc_apm_server_heartbeat_failed_t *event)
{
   return event->duration_usec;
}


void
mongoc_apm_server_heartbeat_failed_get_error (const mongoc_apm_server_heartbeat_failed_t *event, bson_error_t *error)
{
   memcpy (error, event->error, sizeof *event->error);
}


const mongoc_host_list_t *
mongoc_apm_server_heartbeat_failed_get_host (const mongoc_apm_server_heartbeat_failed_t *event)
{
   return event->host;
}


void *
mongoc_apm_server_heartbeat_failed_get_context (const mongoc_apm_server_heartbeat_failed_t *event)
{
   return event->context;
}

bool
mongoc_apm_server_heartbeat_failed_get_awaited (const mongoc_apm_server_heartbeat_failed_t *event)
{
   return event->awaited;
}

/*
 * registering callbacks
 */

mongoc_apm_callbacks_t *
mongoc_apm_callbacks_new (void)
{
   size_t s = sizeof (mongoc_apm_callbacks_t);

   return (mongoc_apm_callbacks_t *) bson_malloc0 (s);
}


void
mongoc_apm_callbacks_destroy (mongoc_apm_callbacks_t *callbacks)
{
   bson_free (callbacks);
}


void
mongoc_apm_set_command_started_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_command_started_cb_t cb)
{
   callbacks->started = cb;
}


void
mongoc_apm_set_command_succeeded_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_command_succeeded_cb_t cb)
{
   callbacks->succeeded = cb;
}


void
mongoc_apm_set_command_failed_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_command_failed_cb_t cb)
{
   callbacks->failed = cb;
}

void
mongoc_apm_set_server_changed_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_server_changed_cb_t cb)
{
   callbacks->server_changed = cb;
}


void
mongoc_apm_set_server_opening_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_server_opening_cb_t cb)
{
   callbacks->server_opening = cb;
}


void
mongoc_apm_set_server_closed_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_server_closed_cb_t cb)
{
   callbacks->server_closed = cb;
}


void
mongoc_apm_set_topology_changed_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_topology_changed_cb_t cb)
{
   callbacks->topology_changed = cb;
}


void
mongoc_apm_set_topology_opening_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_topology_opening_cb_t cb)
{
   callbacks->topology_opening = cb;
}


void
mongoc_apm_set_topology_closed_cb (mongoc_apm_callbacks_t *callbacks, mongoc_apm_topology_closed_cb_t cb)
{
   callbacks->topology_closed = cb;
}


void
mongoc_apm_set_server_heartbeat_started_cb (mongoc_apm_callbacks_t *callbacks,
                                            mongoc_apm_server_heartbeat_started_cb_t cb)
{
   callbacks->server_heartbeat_started = cb;
}


void
mongoc_apm_set_server_heartbeat_succeeded_cb (mongoc_apm_callbacks_t *callbacks,
                                              mongoc_apm_server_heartbeat_succeeded_cb_t cb)
{
   callbacks->server_heartbeat_succeeded = cb;
}


void
mongoc_apm_set_server_heartbeat_failed_cb (mongoc_apm_callbacks_t *callbacks,
                                           mongoc_apm_server_heartbeat_failed_cb_t cb)
{
   callbacks->server_heartbeat_failed = cb;
}

static bool
_mongoc_apm_is_sensitive_command_name (const char *command_name)
{
   return 0 == strcasecmp (command_name, "authenticate") || 0 == strcasecmp (command_name, "saslStart") ||
          0 == strcasecmp (command_name, "saslContinue") || 0 == strcasecmp (command_name, "getnonce") ||
          0 == strcasecmp (command_name, "createUser") || 0 == strcasecmp (command_name, "updateUser") ||
          0 == strcasecmp (command_name, "copydbgetnonce") || 0 == strcasecmp (command_name, "copydbsaslstart") ||
          0 == strcasecmp (command_name, "copydb");
}

static bool
_mongoc_apm_is_sensitive_hello_message (const char *command_name, const bson_t *body)
{
   const bool is_hello =
      (0 == strcasecmp (command_name, "hello") || 0 == strcasecmp (command_name, HANDSHAKE_CMD_LEGACY_HELLO));

   if (!is_hello) {
      return false;
   }
   if (bson_empty (body)) {
      /* An empty message body means that it has been redacted */
      return true;
   } else if (bson_has_field (body, "speculativeAuthenticate")) {
      /* "hello" messages are only sensitive if they contain
       * 'speculativeAuthenticate' */
      return true;
   } else {
      /* Other "hello" messages are okay */
      return false;
   }
}

bool
mongoc_apm_is_sensitive_command_message (const char *command_name, const bson_t *body)
{
   BSON_ASSERT (body);

   return _mongoc_apm_is_sensitive_command_name (command_name) ||
          _mongoc_apm_is_sensitive_hello_message (command_name, body);
}

void
mongoc_apm_redact_command (bson_t *command)
{
   BSON_ASSERT (command);

   /* Reinit the command to have an empty document */
   bson_reinit (command);
}


void
mongoc_apm_redact_reply (bson_t *reply)
{
   BSON_ASSERT (reply);

   /* Reinit the reply to have an empty document */
   bson_reinit (reply);
}
