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

#include "mongoc-prelude.h"

#ifndef MONGOC_APM_PRIVATE_H
#define MONGOC_APM_PRIVATE_H

#include <bson/bson.h>
#include "mongoc-apm.h"

BSON_BEGIN_DECLS

/* forward decl */
struct _mongoc_cmd_t;

struct _mongoc_apm_callbacks_t {
   mongoc_apm_command_started_cb_t started;
   mongoc_apm_command_succeeded_cb_t succeeded;
   mongoc_apm_command_failed_cb_t failed;
   mongoc_apm_server_changed_cb_t server_changed;
   mongoc_apm_server_opening_cb_t server_opening;
   mongoc_apm_server_closed_cb_t server_closed;
   mongoc_apm_topology_changed_cb_t topology_changed;
   mongoc_apm_topology_opening_cb_t topology_opening;
   mongoc_apm_topology_closed_cb_t topology_closed;
   mongoc_apm_server_heartbeat_started_cb_t server_heartbeat_started;
   mongoc_apm_server_heartbeat_succeeded_cb_t server_heartbeat_succeeded;
   mongoc_apm_server_heartbeat_failed_cb_t server_heartbeat_failed;
};

/*
 * command monitoring events
 */

struct _mongoc_apm_command_started_t {
   bson_t *command;
   bool command_owned;
   const char *database_name;
   const char *command_name;
   int64_t request_id;
   int64_t operation_id;
   const mongoc_host_list_t *host;
   uint32_t server_id;
   bson_oid_t service_id;
   int64_t server_connection_id;
   void *context;
};

struct _mongoc_apm_command_succeeded_t {
   int64_t duration;
   bson_t *reply;
   bool reply_owned;
   const char *command_name;
   const char *database_name;
   int64_t request_id;
   int64_t operation_id;
   const mongoc_host_list_t *host;
   uint32_t server_id;
   bson_oid_t service_id;
   int64_t server_connection_id;
   void *context;
};

struct _mongoc_apm_command_failed_t {
   int64_t duration;
   const char *command_name;
   const char *database_name;
   const bson_error_t *error;
   bson_t *reply;
   bool reply_owned;
   int64_t request_id;
   int64_t operation_id;
   const mongoc_host_list_t *host;
   uint32_t server_id;
   bson_oid_t service_id;
   int64_t server_connection_id;
   void *context;
};

/*
 * SDAM monitoring events
 */

struct _mongoc_apm_server_changed_t {
   const mongoc_host_list_t *host;
   bson_oid_t topology_id;
   const mongoc_server_description_t *previous_description;
   const mongoc_server_description_t *new_description;
   void *context;
};

struct _mongoc_apm_server_opening_t {
   const mongoc_host_list_t *host;
   bson_oid_t topology_id;
   void *context;
};

struct _mongoc_apm_server_closed_t {
   const mongoc_host_list_t *host;
   bson_oid_t topology_id;
   void *context;
};

struct _mongoc_apm_topology_changed_t {
   bson_oid_t topology_id;
   const mongoc_topology_description_t *previous_description;
   const mongoc_topology_description_t *new_description;
   void *context;
};

struct _mongoc_apm_topology_opening_t {
   bson_oid_t topology_id;
   void *context;
};

struct _mongoc_apm_topology_closed_t {
   bson_oid_t topology_id;
   void *context;
};

struct _mongoc_apm_server_heartbeat_started_t {
   const mongoc_host_list_t *host;
   void *context;
   bool awaited;
};

struct _mongoc_apm_server_heartbeat_succeeded_t {
   int64_t duration_usec;
   const bson_t *reply;
   const mongoc_host_list_t *host;
   void *context;
   bool awaited;
};

struct _mongoc_apm_server_heartbeat_failed_t {
   int64_t duration_usec;
   const bson_error_t *error;
   const mongoc_host_list_t *host;
   void *context;
   bool awaited;
};

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
                                 void *context);

void
mongoc_apm_command_started_init_with_cmd (mongoc_apm_command_started_t *event,
                                          struct _mongoc_cmd_t *cmd,
                                          int64_t request_id,
                                          bool *is_redacted, /* out */
                                          void *context);

void
mongoc_apm_command_started_cleanup (mongoc_apm_command_started_t *event);

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
                                   void *context);

void
mongoc_apm_command_succeeded_cleanup (mongoc_apm_command_succeeded_t *event);

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
                                void *context);

void
mongoc_apm_command_failed_cleanup (mongoc_apm_command_failed_t *event);

/**
 * @brief Determine whether the given command-related message is a "sensitive
 * command."
 *
 * @param command_name The name of the command being checked
 * @param body The body of the command request, reply, or failure.
 */
bool
mongoc_apm_is_sensitive_command_message (const char *command_name, const bson_t *body);

BSON_END_DECLS

#endif /* MONGOC_APM_PRIVATE_H */
