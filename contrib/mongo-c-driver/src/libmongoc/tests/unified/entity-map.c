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

#include "entity-map.h"

#include "bsonutil/bson-parser.h"
#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "utlist.h"
#include "util.h"

#include <bson-dsl.h>

#include "common-b64-private.h"
#include "mongoc-client-side-encryption-private.h"

/* TODO: use public API to reduce min heartbeat once CDRIVER-3130 is resolved.
 */
#include "mongoc-client-private.h"
#include "mongoc-topology-private.h"

#define REDUCED_HEARTBEAT_FREQUENCY_MS 500
#define REDUCED_MIN_HEARTBEAT_FREQUENCY_MS 50

struct _entity_findcursor_t {
   const bson_t *first_result;
   mongoc_cursor_t *cursor;
};

static void
entity_destroy (entity_t *entity);

entity_map_t *
entity_map_new (void)
{
   return bson_malloc0 (sizeof (entity_map_t));
}

void
entity_map_destroy (entity_map_t *entity_map)
{
   entity_t *entity, *tmp;
   LL_FOREACH_SAFE (entity_map->entities, entity, tmp)
   {
      entity_destroy (entity);
   }
   bson_free (entity_map);
}

static bool
uri_apply_options (mongoc_uri_t *uri, bson_t *opts, bson_error_t *error)
{
   bson_iter_t iter;
   bool ret = false;
   bool wcSet = false;
   mongoc_write_concern_t *wc = NULL;

   /* There may be multiple URI options (w, wTimeoutMS, journal) for a write
    * concern. Parse all options before setting the write concern on the URI. */
   wc = mongoc_write_concern_new ();

   BSON_FOREACH (opts, iter)
   {
      const char *key;

      key = bson_iter_key (&iter);

      if (0 == strcmp ("readConcernLevel", key)) {
         mongoc_read_concern_t *rc = NULL;

         rc = mongoc_read_concern_new ();
         mongoc_read_concern_set_level (rc, bson_iter_utf8 (&iter, NULL));
         mongoc_uri_set_read_concern (uri, rc);
         mongoc_read_concern_destroy (rc);
      } else if (0 == strcmp ("w", key)) {
         wcSet = true;
         mongoc_write_concern_set_w (wc, bson_iter_int32 (&iter));
      } else if (mongoc_uri_option_is_int32 (key)) {
         mongoc_uri_set_option_as_int32 (uri, key, bson_iter_int32 (&iter));
      } else if (mongoc_uri_option_is_int64 (key)) {
         mongoc_uri_set_option_as_int64 (uri, key, bson_iter_int64 (&iter));
      } else if (mongoc_uri_option_is_bool (key)) {
         mongoc_uri_set_option_as_bool (uri, key, bson_iter_bool (&iter));
      } else if (0 == strcmp ("appname", key)) {
         mongoc_uri_set_appname (uri, bson_iter_utf8 (&iter, NULL));
      } else {
         test_set_error (error, "Unimplemented test runner support for URI option: %s", key);
         goto done;
      }
   }

   if (wcSet) {
      mongoc_uri_set_write_concern (uri, wc);
   }

   ret = true;

done:
   mongoc_write_concern_destroy (wc);
   return ret;
}

event_t *
event_new (const char *type)
{
   event_t *event = NULL;

   event = bson_malloc0 (sizeof (event_t));
   event->type = bson_strdup (type);
   return event;
}

void
event_destroy (event_t *event)
{
   if (!event) {
      return;
   }

   bson_free (event->command_name);
   bson_free (event->database_name);
   bson_destroy (event->command);
   bson_destroy (event->reply);
   bson_free (event->type);
   bson_free (event);
}

static entity_t *
entity_new (entity_map_t *em, const char *type)
{
   entity_t *entity = NULL;
   entity = bson_malloc0 (sizeof (entity_t));
   entity->type = bson_strdup (type);
   entity->entity_map = em;
   _mongoc_array_init (&entity->observe_events, sizeof (observe_event_t));
   _mongoc_array_init (&entity->store_events, sizeof (store_event_t));
   return entity;
}

static bool
is_sensitive_command (event_t *event)
{
   const bson_t *body = event->reply ? event->reply : event->command;
   BSON_ASSERT (body);
   return mongoc_apm_is_sensitive_command_message (event->command_name, body);
}

bool
should_ignore_event (entity_t *client_entity, event_t *event)
{
   bson_iter_t iter;

   if (0 == strcmp (event->command_name, "configureFailPoint")) {
      return true;
   }

   if (client_entity->ignore_command_monitoring_events) {
      BSON_FOREACH (client_entity->ignore_command_monitoring_events, iter)
      {
         if (0 == strcmp (event->command_name, bson_iter_utf8 (&iter, NULL))) {
            return true;
         }
      }
   }

   {
      observe_event_t *const begin = (observe_event_t *) client_entity->observe_events.data;
      observe_event_t *const end = begin + client_entity->observe_events.len;

      bool is_observed = false;

      for (observe_event_t *iter = begin; iter != end; ++iter) {
         if (bson_strcasecmp (iter->type, event->type) == 0) {
            is_observed = true;
            break;
         }
      }

      if (!is_observed) {
         return true;
      }
   }

   if (client_entity->observe_sensitive_commands && *client_entity->observe_sensitive_commands) {
      return false;
   }

   /* Sensitive commands need to be ignored */
   return is_sensitive_command (event);
}


typedef void *(*apm_func_void_t) (const void *);
typedef const bson_t *(*apm_func_bson_t) (const void *);
typedef const char *(*apm_func_utf8_t) (const void *);
typedef int64_t (*apm_func_int64_t) (const void *);
typedef const bson_oid_t *(*apm_func_bson_oid_t) (const void *);
typedef int32_t (*apm_func_int32_t) (const void *);
typedef const mongoc_host_list_t *(*apm_func_host_list_t) (const void *);
typedef void (*apm_func_serialize_t) (bson_t *, const void *);


typedef struct command_callback_funcs_t {
   apm_func_void_t get_context;
   apm_func_bson_t get_command;
   apm_func_bson_t get_reply;
   apm_func_utf8_t get_command_name;
   apm_func_utf8_t get_database_name;
   apm_func_int64_t get_request_id;
   apm_func_int64_t get_operation_id;
   apm_func_bson_oid_t get_service_id;
   apm_func_host_list_t get_host;
   apm_func_int64_t get_server_connection_id;
   apm_func_serialize_t serialize;
} command_callback_funcs_t;


static void
observe_event (entity_t *entity, command_callback_funcs_t funcs, const char *type, const void *apm_command)
{
   BSON_ASSERT_PARAM (type);
   BSON_ASSERT_PARAM (apm_command);

   BSON_ASSERT (funcs.get_context);
   event_t *const event = event_new (type);

   if (funcs.get_command) {
      event->command = bson_copy (funcs.get_command (apm_command));
   }

   if (funcs.get_reply) {
      event->reply = bson_copy (funcs.get_reply (apm_command));
   }

   BSON_ASSERT (funcs.get_command_name);
   event->command_name = bson_strdup (funcs.get_command_name (apm_command));

   if (funcs.get_database_name) {
      event->database_name = bson_strdup (funcs.get_database_name (apm_command));
   }

   BSON_ASSERT (funcs.get_service_id);
   const bson_oid_t *const service_id = funcs.get_service_id (apm_command);
   if (service_id) {
      bson_oid_copy (service_id, &event->service_id);
   }

   BSON_ASSERT (funcs.get_server_connection_id);
   event->server_connection_id = funcs.get_server_connection_id (apm_command);

   if (should_ignore_event (entity, event)) {
      event_destroy (event);
      return;
   }

   LL_APPEND (entity->events, event);
}


static void
store_event_serialize_started (bson_t *doc, const mongoc_apm_command_started_t *apm_command)
{
   // Spec: The test runner MAY omit the command field for CommandStartedEvent
   // and reply field for CommandSucceededEvent.
   // BSON_APPEND_DOCUMENT (
   //    doc, "command", mongoc_apm_command_started_get_command (apm_command));

   BSON_APPEND_UTF8 (doc, "databaseName", mongoc_apm_command_started_get_database_name (apm_command));

   BSON_APPEND_UTF8 (doc, "commandName", mongoc_apm_command_started_get_command_name (apm_command));

   BSON_APPEND_INT64 (doc, "requestId", mongoc_apm_command_started_get_request_id (apm_command));

   BSON_APPEND_INT64 (doc, "operationId", mongoc_apm_command_started_get_operation_id (apm_command));

   BSON_APPEND_UTF8 (doc, "connectionId", mongoc_apm_command_started_get_host (apm_command)->host_and_port);

   BSON_APPEND_INT64 (
      doc, "serverConnectionId", mongoc_apm_command_started_get_server_connection_id_int64 (apm_command));

   {
      const bson_oid_t *const service_id = mongoc_apm_command_started_get_service_id (apm_command);

      if (service_id) {
         BSON_APPEND_OID (doc, "serviceId", service_id);
      }
   }
}


static void
store_event_serialize_failed (bson_t *doc, const mongoc_apm_command_failed_t *apm_command)
{
   BSON_APPEND_INT64 (doc, "duration", mongoc_apm_command_failed_get_duration (apm_command));

   BSON_APPEND_UTF8 (doc, "commandName", mongoc_apm_command_failed_get_command_name (apm_command));

   BSON_APPEND_UTF8 (doc, "databaseName", mongoc_apm_command_failed_get_database_name (apm_command));

   {
      bson_error_t error;
      mongoc_apm_command_failed_get_error (apm_command, &error);
      BSON_APPEND_UTF8 (doc, "failure", error.message);
   }

   BSON_APPEND_INT64 (doc, "requestId", mongoc_apm_command_failed_get_request_id (apm_command));

   BSON_APPEND_INT64 (doc, "operationId", mongoc_apm_command_failed_get_operation_id (apm_command));

   BSON_APPEND_UTF8 (doc, "connectionId", mongoc_apm_command_failed_get_host (apm_command)->host_and_port);

   BSON_APPEND_INT64 (
      doc, "serverConnectionId", mongoc_apm_command_failed_get_server_connection_id_int64 (apm_command));

   {
      const bson_oid_t *const service_id = mongoc_apm_command_failed_get_service_id (apm_command);

      if (service_id) {
         BSON_APPEND_OID (doc, "serviceId", service_id);
      }
   }
}


static void
store_event_serialize_succeeded (bson_t *doc, const mongoc_apm_command_succeeded_t *apm_command)
{
   BSON_APPEND_INT64 (doc, "duration", mongoc_apm_command_succeeded_get_duration (apm_command));

   // Spec: The test runner MAY omit the command field for CommandStartedEvent
   // and reply field for CommandSucceededEvent.
   // BSON_APPEND_DOCUMENT (
   //    doc, "reply", mongoc_apm_command_succeeded_get_reply (apm_command));

   BSON_APPEND_UTF8 (doc, "commandName", mongoc_apm_command_succeeded_get_command_name (apm_command));

   BSON_APPEND_UTF8 (doc, "databaseName", mongoc_apm_command_succeeded_get_database_name (apm_command));

   BSON_APPEND_INT64 (doc, "requestId", mongoc_apm_command_succeeded_get_request_id (apm_command));

   BSON_APPEND_INT64 (doc, "operationId", mongoc_apm_command_succeeded_get_operation_id (apm_command));

   BSON_APPEND_UTF8 (doc, "connectionId", mongoc_apm_command_succeeded_get_host (apm_command)->host_and_port);

   BSON_APPEND_INT64 (
      doc, "serverConnectionId", mongoc_apm_command_succeeded_get_server_connection_id_int64 (apm_command));

   {
      const bson_oid_t *const service_id = mongoc_apm_command_succeeded_get_service_id (apm_command);

      if (service_id) {
         BSON_APPEND_OID (doc, "serviceId", service_id);
      }
   }
}


static void
store_event_to_entities (entity_t *entity, command_callback_funcs_t funcs, const char *type, const void *apm_command)
{
   BSON_ASSERT_PARAM (entity);
   BSON_ASSERT_PARAM (type);

   BSON_ASSERT (entity->entity_map);

   entity_map_t *const em = entity->entity_map;

   store_event_t *const begin = (store_event_t *) entity->store_events.data;
   store_event_t *const end = begin + entity->store_events.len;

   const int64_t usecs = usecs_since_epoch ();
   const double secs = (double) usecs / 1000000.0;

   bson_error_t error = {0};

   for (store_event_t *iter = begin; iter != end; ++iter) {
      if (bson_strcasecmp (iter->type, type) == 0) {
         mongoc_array_t *arr = entity_map_get_bson_array (em, iter->entity_id, &error);
         ASSERT_OR_PRINT (arr, error);

         bson_t *doc = bson_new ();

         // Spec: the following fields MUST be stored with each event document:
         BSON_APPEND_UTF8 (doc, "name", type);
         BSON_APPEND_DOUBLE (doc, "observedAt", secs);

         // The event subscriber MUST serialize the events it receives into a
         // document, using the documented properties of the event as field
         // names, and append the document to the list stored in the specified
         // entity.
         funcs.serialize (doc, apm_command);

         _mongoc_array_append_val (arr, doc); // Transfer ownership.
      }
   }
}


static void
apm_command_callback (command_callback_funcs_t funcs, const char *type, const void *apm_command)
{
   BSON_ASSERT_PARAM (type);
   BSON_ASSERT_PARAM (apm_command);

   BSON_ASSERT (funcs.get_context);
   entity_t *const entity = (entity_t *) funcs.get_context (apm_command);

   observe_event (entity, funcs, type, apm_command);
   store_event_to_entities (entity, funcs, type, apm_command);
}


static void
command_started (const mongoc_apm_command_started_t *started)
{
   command_callback_funcs_t funcs = {
      .get_context = (apm_func_void_t) mongoc_apm_command_started_get_context,
      .get_command = (apm_func_bson_t) mongoc_apm_command_started_get_command,
      .get_reply = NULL,
      .get_command_name = (apm_func_utf8_t) mongoc_apm_command_started_get_command_name,
      .get_database_name = (apm_func_utf8_t) mongoc_apm_command_started_get_database_name,
      .get_request_id = (apm_func_int64_t) mongoc_apm_command_started_get_request_id,
      .get_operation_id = (apm_func_int64_t) mongoc_apm_command_started_get_operation_id,
      .get_service_id = (apm_func_bson_oid_t) mongoc_apm_command_started_get_service_id,
      .get_host = (apm_func_host_list_t) mongoc_apm_command_started_get_host,
      .get_server_connection_id = (apm_func_int64_t) mongoc_apm_command_started_get_server_connection_id_int64,
      .serialize = (apm_func_serialize_t) store_event_serialize_started,
   };

   apm_command_callback (funcs, "commandStartedEvent", started);
}

static void
command_failed (const mongoc_apm_command_failed_t *failed)
{
   command_callback_funcs_t funcs = {
      .get_context = (apm_func_void_t) mongoc_apm_command_failed_get_context,
      .get_command = NULL,
      .get_reply = (apm_func_bson_t) mongoc_apm_command_failed_get_reply,
      .get_command_name = (apm_func_utf8_t) mongoc_apm_command_failed_get_command_name,
      .get_database_name = (apm_func_utf8_t) mongoc_apm_command_failed_get_database_name,
      .get_request_id = (apm_func_int64_t) mongoc_apm_command_failed_get_request_id,
      .get_operation_id = (apm_func_int64_t) mongoc_apm_command_failed_get_operation_id,
      .get_service_id = (apm_func_bson_oid_t) mongoc_apm_command_failed_get_service_id,
      .get_host = (apm_func_host_list_t) mongoc_apm_command_failed_get_host,
      .get_server_connection_id = (apm_func_int64_t) mongoc_apm_command_failed_get_server_connection_id_int64,
      .serialize = (apm_func_serialize_t) store_event_serialize_failed,
   };

   apm_command_callback (funcs, "commandFailedEvent", failed);
}

static void
command_succeeded (const mongoc_apm_command_succeeded_t *succeeded)
{
   command_callback_funcs_t funcs = {
      .get_context = (apm_func_void_t) mongoc_apm_command_succeeded_get_context,
      .get_command = NULL,
      .get_reply = (apm_func_bson_t) mongoc_apm_command_succeeded_get_reply,
      .get_command_name = (apm_func_utf8_t) mongoc_apm_command_succeeded_get_command_name,
      .get_database_name = (apm_func_utf8_t) mongoc_apm_command_succeeded_get_database_name,
      .get_request_id = (apm_func_int64_t) mongoc_apm_command_succeeded_get_request_id,
      .get_operation_id = (apm_func_int64_t) mongoc_apm_command_succeeded_get_operation_id,
      .get_service_id = (apm_func_bson_oid_t) mongoc_apm_command_succeeded_get_service_id,
      .get_host = (apm_func_host_list_t) mongoc_apm_command_succeeded_get_host,
      .get_server_connection_id = (apm_func_int64_t) mongoc_apm_command_succeeded_get_server_connection_id_int64,
      .serialize = (apm_func_serialize_t) store_event_serialize_succeeded,
   };

   apm_command_callback (funcs, "commandSucceededEvent", succeeded);
}

// Note: multiple invocations of this function is okay, since all it does
// is set the appropriate pointer in `callbacks`, and the callback function(s)
// being used is always the same for a given type.
static void
set_command_callback (mongoc_apm_callbacks_t *callbacks, const char *type)
{
   typedef void (*cb_t) (const void *);
   typedef void (*set_func_t) (mongoc_apm_callbacks_t *, cb_t);

   typedef struct _command_to_cb_t {
      const char *type;
      set_func_t set;
      cb_t cb;
   } command_to_cb_t;

   const command_to_cb_t commands[] = {
      {.type = "commandStartedEvent",
       .set = (set_func_t) mongoc_apm_set_command_started_cb,
       .cb = (cb_t) command_started},
      {.type = "commandFailedEvent", .set = (set_func_t) mongoc_apm_set_command_failed_cb, .cb = (cb_t) command_failed},
      {.type = "commandSucceededEvent",
       .set = (set_func_t) mongoc_apm_set_command_succeeded_cb,
       .cb = (cb_t) command_succeeded},
      {.type = NULL, .set = NULL, .cb = NULL},
   };

   for (const command_to_cb_t *iter = commands; iter->type; ++iter) {
      if (bson_strcasecmp (type, iter->type) == 0) {
         iter->set (callbacks, iter->cb);
         return;
      }
   }
}

static void
add_observe_event (entity_t *entity, const char *type)
{
   observe_event_t event = {.type = type};

   _mongoc_array_append_val (&entity->observe_events, event);
}

static void
add_store_event (entity_t *entity, const char *type, const char *entity_id)
{
   store_event_t event = {.type = type, .entity_id = entity_id};

   _mongoc_array_append_val (&entity->store_events, event);
}


entity_t *
entity_client_new (entity_map_t *em, bson_t *bson, bson_error_t *error)
{
   entity_t *entity = NULL;
   mongoc_client_t *client = NULL;
   mongoc_uri_t *uri = NULL;
   bool ret = false;
   mongoc_apm_callbacks_t *callbacks = NULL;
   bson_t *uri_options = NULL;
   bool use_multiple_mongoses = false;
   bool use_multiple_mongoses_set = false;
   bool can_reduce_heartbeat = false;
   mongoc_server_api_t *api = NULL;
   char *errpath = NULL;
   char *err = NULL;
   const char *store_entity_id = NULL;

   entity = entity_new (em, "client");
   callbacks = mongoc_apm_callbacks_new ();

   bsonParse ( //
      *bson,
      // All clients require an ID string
      find (keyWithType ("id", utf8), storeStrDup (entity->id)),
      else (error ("A client 'id' string is required")),
      // Optional 'uriOptions' for the client
      find (key ("uriOptions"),
            if (not(type (doc)), then (error ("'uriOptions' must be a document value"))),
            storeDocDupPtr (uri_options)),
      // Optional 'useMultipleMongoses' bool
      find (key ("useMultipleMongoses"),
            if (not(type (bool)), then (error ("'useMultipleMongoses' must be a bool value"))),
            do (use_multiple_mongoses_set = true),
            storeBool (use_multiple_mongoses)),
      // Events to observe:
      find (key ("observeEvents"),
            if (not(type (array)), then (error ("'observeEvents' must be an array"))),
            visitEach (case (
               // Ensure all elements are strings:
               when (not(type (utf8)), error ("Every 'observeEvents' element must be a string")),
               // Dispatch based on the event name:
               when (anyOf (iStrEqual ("commandStartedEvent"),
                            iStrEqual ("commandFailedEvent"),
                            iStrEqual ("commandSucceededEvent")),
                     do ({
                        const char *const type = bson_iter_utf8 (&bsonVisitIter, NULL);
                        set_command_callback (callbacks, type);
                        add_observe_event (entity, type);
                     })),
               // Unsupported (but known) event names:
               when (eval (is_unsupported_event_type (bson_iter_utf8 (&bsonVisitIter, NULL))),
                     do (MONGOC_DEBUG ("Skipping unsupported event type '%s'", bsonAs (cstr)))),
               // An unknown event name is a hard-error:
               else (do (test_error ("Unknown event type '%s'", bsonAs (cstr))))))),
      // Command events to ignore
      find (key ("ignoreCommandMonitoringEvents"),
            if (not(type (array)), then (error ("'ignoreCommandMonitoringEvents' must be an array"))),
            visitEach (if (not(type (utf8)),
                           then (error ("Every 'ignoreCommandMonitoringEvents' "
                                        "element must be a string")))),
            storeDocDupPtr (entity->ignore_command_monitoring_events)),
      // Parse the serverApi, if present
      find (key ("serverApi"),
            if (not(type (doc)), then (error ("'serverApi' must be a document"))),
            parse ( // The "version" string is required first:
               find (keyWithType ("version", utf8), do ({
                        mongoc_server_api_version_t ver;
                        if (!mongoc_server_api_version_from_string (bsonAs (cstr), &ver)) {
                           bsonParseError = "Invalid serverApi.version string";
                        } else {
                           api = mongoc_server_api_new (ver);
                        }
                     })),
               else (error ("Missing 'version' property in 'serverApi' object")),
               // Toggle strictness:
               find (key ("strict"),
                     if (not(type (bool)), then (error ("'serverApi.strict' must be a bool"))),
                     do (mongoc_server_api_strict (api, bsonAs (bool)))),
               // Toggle deprecation errors:
               find (key ("deprecationErrors"),
                     if (not(type (bool)), then (error ("serverApi.deprecationErrors must be a bool"))),
                     do (mongoc_server_api_deprecation_errors (api, bsonAs (bool)))))),
      // Toggle observation of sensitive commands
      find (key ("observeSensitiveCommands"),
            if (not(type (bool)), then (error ("'observeSensitiveCommands' must be a bool"))),
            do ({
               bool *p = entity->observe_sensitive_commands = bson_malloc (sizeof (bool));
               *p = bsonAs (bool);
            })),
      // Which events should be available as entities:
      find (key ("storeEventsAsEntities"),
            if (not(type (array)), then (error ("'storeEventsAsEntities' must be an array"))),
            visitEach (parse (
               find (keyWithType ("id", utf8), storeStrRef (store_entity_id), do ({
                        if (!entity_map_add_bson_array (em, store_entity_id, error)) {
                           test_error ("failed to create storeEventsAsEntities "
                                       "entity '%s': %s",
                                       store_entity_id,
                                       error->message);
                        }
                     })),
               find (keyWithType ("events", array),
                     visitEach (case (when (not(type (utf8)),
                                            error ("Every 'storeEventsAsEntities.events' "
                                                   "element must be a string")),
                                      when (anyOf (iStrEqual ("commandStartedEvent"),
                                                   iStrEqual ("commandFailedEvent"),
                                                   iStrEqual ("commandSucceededEvent")),
                                            do ({
                                               const char *const type = bson_iter_utf8 (&bsonVisitIter, NULL);
                                               set_command_callback (callbacks, type);
                                               add_store_event (entity, type, store_entity_id);
                                            })),
                                      when (eval (is_unsupported_event_type (bson_iter_utf8 (&bsonVisitIter, NULL))),
                                            do (MONGOC_DEBUG ("Skipping unsupported event type '%s'", bsonAs (cstr)))),
                                      else (do (test_error ("Unknown event type '%s'", bsonAs (cstr))))))),
               visitOthers (
                  errorf (err, "Unexpected field '%s' in storeEventsAsEntities", bson_iter_key (&bsonVisitIter)))))),
      visitOthers (
         dupPath (errpath),
         errorf (err, "At [%s]: Unknown key '%s' given in entity options", errpath, bson_iter_key (&bsonVisitIter))));

   if (bsonParseError) {
      test_error ("Error while parsing entity object: %s", bsonParseError);
   }

   /* Build the client's URI. */
   uri = test_framework_get_uri ();
   /* Apply "useMultipleMongoses" rules to URI.
    * If useMultipleMongoses is true, modify the connection string to add a
    * host. If useMultipleMongoses is false, require that the connection string
    * has one host. If useMultipleMongoses unspecified, make no assertion.
    */
   if (test_framework_is_loadbalanced ()) {
      /* Quoting the unified test runner specification:
       * If the topology type is LoadBalanced, [...] If useMultipleMongoses is
       * true or unset, the test runner MUST use the URI of the load balancer
       * fronting multiple servers. Otherwise, the test runner MUST use the URI
       * of the load balancer fronting a single server.
       */
      if (!use_multiple_mongoses_set || use_multiple_mongoses == true) {
         mongoc_uri_destroy (uri);
         uri = test_framework_get_uri_multi_mongos_loadbalanced ();
      }
   } else if (use_multiple_mongoses_set) {
      if (!test_framework_uri_apply_multi_mongos (uri, use_multiple_mongoses, error)) {
         goto done;
      }
   }

   if (uri_options) {
      /* Apply URI options. */
      if (!uri_apply_options (uri, uri_options, error)) {
         goto done;
      }
   }

   if (!mongoc_uri_has_option (uri, MONGOC_URI_HEARTBEATFREQUENCYMS)) {
      can_reduce_heartbeat = true;
   }

   if (can_reduce_heartbeat && em->reduced_heartbeat) {
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, REDUCED_HEARTBEAT_FREQUENCY_MS);
   }

   client = test_framework_client_new_from_uri (uri, api);
   test_framework_set_ssl_opts (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   entity->value = client;
   mongoc_client_set_apm_callbacks (client, callbacks, entity);

   if (can_reduce_heartbeat && em->reduced_heartbeat) {
      client->topology->min_heartbeat_frequency_msec = REDUCED_MIN_HEARTBEAT_FREQUENCY_MS;
   }

   ret = true;
done:
   mongoc_uri_destroy (uri);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_server_api_destroy (api);
   bson_destroy (uri_options);
   if (!ret) {
      entity_destroy (entity);
      return NULL;
   }
   return entity;
}

static char *
_entity_client_encryption_getenv (const char *name, bson_error_t *error)
{
   char *res = NULL;

   BSON_ASSERT_PARAM (name);

   if (!(res = _mongoc_getenv (name))) {
      test_set_error (error, "missing required environment variable '%s'", name);
   }

   return res;
}

static bool
_append_kms_provider_value_or_getenv (
   bson_t *bson, const char *key, const char *value, const char *env_name, bson_error_t *error)
{
   BSON_ASSERT_PARAM (bson);
   BSON_ASSERT_PARAM (env_name);

   /* Prefer explicit value if available. */
   if (value) {
      BSON_ASSERT (BSON_APPEND_UTF8 (bson, key, value));
      return true;
   }

   /* Fallback to environment variable. */
   {
      char *const env_var = _entity_client_encryption_getenv (env_name, error);

      if (env_var) {
         BSON_ASSERT (BSON_APPEND_UTF8 (bson, key, env_var));
         bson_free (env_var);
         return true;
      }
   }

   return false;
}

static bool
_validate_string_or_placeholder (const bson_iter_t *iter, bson_error_t *error)
{
   BSON_ASSERT_PARAM (iter);

   /* Holds a UTF-8 string. */
   if (BSON_ITER_HOLDS_UTF8 (iter)) {
      return true;
   }

   /* Otherwise, must be a document with a single '$$placeholder' field. */
   if (BSON_ITER_HOLDS_DOCUMENT (iter)) {
      bson_val_t *const bson_val = bson_val_from_iter (iter);
      bson_val_t *const expected = bson_val_from_json ("{'$$placeholder': { '$exists': true }}");
      bool is_match = false;

      BSON_ASSERT (bson_val);
      BSON_ASSERT (expected);

      is_match = bson_match (expected, bson_val, false, error);

      bson_val_destroy (bson_val);
      bson_val_destroy (expected);

      if (is_match) {
         return true;
      }
   }

   test_set_error (error, "expected string or placeholder value");

   return false;
}

static bool
_parse_kms_provider_aws (
   bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error)
{
   bson_t child;
   bson_iter_t iter;

   BSON_UNUSED (tls_opts);

   BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (kms_providers, provider, &child));

   BSON_FOREACH (kms_doc, iter)
   {
      const char *const key = bson_iter_key (&iter);
      const char *const value = bson_iter_utf8 (&iter, NULL);

      if (!_validate_string_or_placeholder (&iter, error)) {
         return false;
      }

      if (strcmp (key, "accessKeyId") == 0) {
         const char *envvar = "MONGOC_TEST_AWS_ACCESS_KEY_ID";
         if (0 == strcmp (provider, "aws:name2")) {
            envvar = "MONGOC_TEST_AWSNAME2_ACCESS_KEY_ID";
         }
         if (!_append_kms_provider_value_or_getenv (&child, key, value, envvar, error)) {
            return false;
         }
      } else if (strcmp (key, "secretAccessKey") == 0) {
         const char *envvar = "MONGOC_TEST_AWS_SECRET_ACCESS_KEY";
         if (0 == strcmp (provider, "aws:name2")) {
            envvar = "MONGOC_TEST_AWSNAME2_SECRET_ACCESS_KEY";
         }
         if (!_append_kms_provider_value_or_getenv (&child, key, value, envvar, error)) {
            return false;
         }
      } else {
         test_set_error (error, "unexpected field '%s'", key);
         return false;
      }
   }

   BSON_ASSERT (bson_append_document_end (kms_providers, &child));

   return true;
}

static bool
_parse_kms_provider_azure (
   bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error)
{
   bson_t child;
   bson_iter_t iter;

   BSON_UNUSED (tls_opts);

   BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (kms_providers, provider, &child));

   BSON_FOREACH (kms_doc, iter)
   {
      const char *const key = bson_iter_key (&iter);
      const char *const value = bson_iter_utf8 (&iter, NULL);

      if (!_validate_string_or_placeholder (&iter, error)) {
         return false;
      }

      if (strcmp (key, "tenantId") == 0) {
         if (!_append_kms_provider_value_or_getenv (&child, key, value, "MONGOC_TEST_AZURE_TENANT_ID", error)) {
            return false;
         }
      } else if (strcmp (key, "clientId") == 0) {
         if (!_append_kms_provider_value_or_getenv (&child, key, value, "MONGOC_TEST_AZURE_CLIENT_ID", error)) {
            return false;
         }
      } else if (strcmp (key, "clientSecret") == 0) {
         if (!_append_kms_provider_value_or_getenv (&child, key, value, "MONGOC_TEST_AZURE_CLIENT_SECRET", error)) {
            return false;
         }
      } else {
         test_set_error (error, "unexpected field '%s'", value);
         return false;
      }
   }

   BSON_ASSERT (bson_append_document_end (kms_providers, &child));

   return true;
}

static bool
_parse_kms_provider_gcp (
   bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error)
{
   bson_t child;
   bson_iter_t iter;

   BSON_UNUSED (tls_opts);

   BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (kms_providers, provider, &child));

   BSON_FOREACH (kms_doc, iter)
   {
      const char *const key = bson_iter_key (&iter);
      const char *const value = bson_iter_utf8 (&iter, NULL);

      if (!_validate_string_or_placeholder (&iter, error)) {
         return false;
      }

      if (strcmp (key, "email") == 0) {
         if (!_append_kms_provider_value_or_getenv (&child, key, value, "MONGOC_TEST_GCP_EMAIL", error)) {
            return false;
         }
      } else if (strcmp (key, "privateKey") == 0) {
         if (!_append_kms_provider_value_or_getenv (&child, key, value, "MONGOC_TEST_GCP_PRIVATEKEY", error)) {
            return false;
         }
      } else if (strcmp (key, "endpoint") == 0) {
         if (value) {
            BSON_ASSERT (BSON_APPEND_UTF8 (&child, key, value));
         }
      } else {
         test_set_error (error, "unexpected field '%s'", value);
         return false;
      }
   }

   BSON_ASSERT (bson_append_document_end (kms_providers, &child));

   return true;
}

static bool
_parse_kms_provider_kmip (
   bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error)
{
   bson_t child;
   bson_iter_t iter;

   BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (kms_providers, provider, &child));

   BSON_FOREACH (kms_doc, iter)
   {
      const char *const key = bson_iter_key (&iter);
      const char *const value = bson_iter_utf8 (&iter, NULL);

      if (!_validate_string_or_placeholder (&iter, error)) {
         return false;
      }

      if (strcmp (key, "endpoint") == 0) {
         if (value) {
            BSON_ASSERT (BSON_APPEND_UTF8 (&child, key, value));
         } else {
            /* Expect KMIP test server running on port 5698. */
            BSON_ASSERT (BSON_APPEND_UTF8 (&child, key, "localhost:5698"));
         }

         /* Configure tlsOptions to enable KMIP TLS connections. */
         {
            bson_t tls_child;
            BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (tls_opts, provider, &tls_child));
            if (!_append_kms_provider_value_or_getenv (
                   &tls_child, "tlsCAFile", NULL, "MONGOC_TEST_CSFLE_TLS_CA_FILE", error)) {
               return false;
            }
            if (!_append_kms_provider_value_or_getenv (
                   &tls_child, "tlsCertificateKeyFile", NULL, "MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE", error)) {
               return false;
            }
            BSON_ASSERT (bson_append_document_end (tls_opts, &tls_child));
         }
      } else {
         test_set_error (error, "unexpected field '%s'", value);
         return false;
      }
   }

   BSON_ASSERT (bson_append_document_end (kms_providers, &child));

   return true;
}

static bool
_parse_kms_provider_local (
   bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error)
{
   bson_t child;
   bson_iter_t iter;

   BSON_UNUSED (tls_opts);

   BSON_ASSERT (BSON_APPEND_DOCUMENT_BEGIN (kms_providers, provider, &child));

   BSON_FOREACH (kms_doc, iter)
   {
      const char *const key = bson_iter_key (&iter);
      const char *const value = bson_iter_utf8 (&iter, NULL);

      if (!_validate_string_or_placeholder (&iter, error)) {
         return false;
      }

      if (strcmp (key, "key") == 0) {
         if (value) {
            BSON_ASSERT (BSON_APPEND_UTF8 (&child, key, value));
         } else {
            /* LOCAL_MASTERKEY in base64 encoding as defined in Client Side
             * Encryption Tests spec. */
            const char local_masterkey[] = "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6N"
                                           "mdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZG"
                                           "JkTXVyZG9uSjFk";
            uint8_t data[96];
            BSON_ASSERT (mcommon_b64_pton (local_masterkey, data, sizeof (local_masterkey)) == 96);
            BSON_APPEND_BINARY (&child, "key", BSON_SUBTYPE_BINARY, data, 96);
         }
      } else {
         test_set_error (error, "unexpected field '%s'", value);
         return false;
      }
   }

   BSON_ASSERT (bson_append_document_end (kms_providers, &child));

   return true;
}

static bool
_parse_and_set_kms_providers (mongoc_client_encryption_opts_t *ce_opts, bson_t *kms_from_file, bson_error_t *error)
{
   /* Map provider to corresponding KMS parser. */
   typedef struct _prov_map_t {
      const char *provider;
      bool (*parse) (
         bson_t *kms_providers, bson_t *tls_opts, const char *provider, bson_t *kms_doc, bson_error_t *error);
   } prov_map_t;

   const prov_map_t prov_map[] = {{.provider = "aws", .parse = _parse_kms_provider_aws},
                                  {.provider = "aws:name1", .parse = _parse_kms_provider_aws},
                                  {.provider = "aws:name2", .parse = _parse_kms_provider_aws},
                                  {.provider = "azure", .parse = _parse_kms_provider_azure},
                                  {.provider = "azure:name1", .parse = _parse_kms_provider_azure},
                                  {.provider = "gcp", .parse = _parse_kms_provider_gcp},
                                  {.provider = "gcp:name1", .parse = _parse_kms_provider_gcp},
                                  {.provider = "kmip", .parse = _parse_kms_provider_kmip},
                                  {.provider = "kmip:name1", .parse = _parse_kms_provider_kmip},
                                  {.provider = "local", .parse = _parse_kms_provider_local},
                                  {.provider = "local:name1", .parse = _parse_kms_provider_local},
                                  {.provider = "local:name2", .parse = _parse_kms_provider_local}};

   const size_t prov_map_size = sizeof (prov_map) / sizeof (prov_map[0]);

   bool ret = false;
   bson_t kms_providers = BSON_INITIALIZER;
   bson_t tls_opts = BSON_INITIALIZER;
   bson_iter_t iter;

   BSON_FOREACH (kms_from_file, iter)
   {
      const char *const provider = bson_iter_key (&iter);
      bson_t kms_doc;
      size_t i = 0u;
      bool found = false;

      if (!bson_init_from_value (&kms_doc, bson_iter_value (&iter))) {
         test_set_error (error, "kmsProviders field '%s' is not a valid document", provider);
         goto done;
      }

      for (i = 0u; i < prov_map_size; ++i) {
         if (strcmp (provider, prov_map[i].provider) == 0) {
            found = prov_map[i].parse (&kms_providers, &tls_opts, provider, &kms_doc, error);
            goto parsed;
         }
      }

      test_set_error (error, "unexpected KMS provider '%s'", provider);

   parsed:
      bson_destroy (&kms_doc);

      if (!found) {
         goto done;
      }
   }

   mongoc_client_encryption_opts_set_kms_providers (ce_opts, &kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (ce_opts, &tls_opts);

   ret = true;

done:
   bson_destroy (&kms_providers);
   bson_destroy (&tls_opts);

   return ret;
}

entity_t *
entity_client_encryption_new (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   entity_t *const entity = entity_new (entity_map, "clientEncryption");
   bson_parser_t *const parser = bson_parser_new ();
   mongoc_client_encryption_opts_t *const ce_opts = mongoc_client_encryption_opts_new ();

   bson_t *ce_opts_bson = NULL;

   bson_parser_utf8 (parser, "id", &entity->id);
   bson_parser_doc (parser, "clientEncryptionOpts", &ce_opts_bson);

   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   {
      bson_parser_t *const ce_opts_parser = bson_parser_new ();

      bool ce_opts_success = false;
      char *client_id = NULL;
      char *kv_ns = NULL;
      bson_t *kms = NULL;

      bson_parser_utf8 (ce_opts_parser, "keyVaultClient", &client_id);
      bson_parser_utf8 (ce_opts_parser, "keyVaultNamespace", &kv_ns);
      bson_parser_doc (ce_opts_parser, "kmsProviders", &kms);

      if (!bson_parser_parse (ce_opts_parser, ce_opts_bson, error)) {
         goto ce_opts_done;
      }

      {
         entity_t *const client_entity = entity_map_get (entity_map, client_id, error);
         mongoc_client_t *client = NULL;

         if (!client_entity) {
            goto ce_opts_done;
         }

         BSON_ASSERT ((client = (mongoc_client_t *) client_entity->value));

         mongoc_client_encryption_opts_set_keyvault_client (ce_opts, client);
      }

      {
         char *const dot = strchr (kv_ns, '.');
         const char *db = NULL;
         const char *coll = NULL;

         if (!dot) {
            test_set_error (error, "keyVaultNamespace does not have required dot separator");
            goto ce_opts_done;
         }

         *dot = '\0';    /* e.g. "keyvault.datakeys" -> "keyvault\0datakeys". */
         db = kv_ns;     /* "keyvault" (due to null terminator) */
         coll = dot + 1; /* "datakeys" */

         if (strchr (coll, '.') != NULL) {
            test_set_error (error, "keyVaultNamespace contains more than one dot separator");
            goto ce_opts_done;
         }

         mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, db, coll);
      }

      if (!_parse_and_set_kms_providers (ce_opts, kms, error)) {
         goto ce_opts_done;
      }

      ce_opts_success = true;

   ce_opts_done:
      bson_parser_destroy_with_parsed_fields (ce_opts_parser);

      if (!ce_opts_success) {
         goto done;
      }
   }

   entity->value = mongoc_client_encryption_new (ce_opts, error);

done:
   mongoc_client_encryption_opts_destroy (ce_opts);
   bson_destroy (ce_opts_bson);
   bson_parser_destroy (parser);

   if (!entity->value) {
      entity_destroy (entity);
      return NULL;
   }

   return entity;
}

typedef struct {
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   mongoc_read_prefs_t *rp;
} coll_or_db_opts_t;

static coll_or_db_opts_t *
coll_or_db_opts_new (void)
{
   return bson_malloc0 (sizeof (coll_or_db_opts_t));
}

static void
coll_or_db_opts_destroy (coll_or_db_opts_t *opts)
{
   if (!opts) {
      return;
   }
   mongoc_read_concern_destroy (opts->rc);
   mongoc_read_prefs_destroy (opts->rp);
   mongoc_write_concern_destroy (opts->wc);
   bson_free (opts);
}

static bool
coll_or_db_opts_parse (coll_or_db_opts_t *opts, bson_t *in, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   bool ret = false;

   parser = bson_parser_new ();
   bson_parser_read_concern_optional (parser, &opts->rc);
   bson_parser_read_prefs_optional (parser, &opts->rp);
   bson_parser_write_concern_optional (parser, &opts->wc);
   if (!bson_parser_parse (parser, in, error)) {
      goto done;
   }

   ret = true;
done:
   bson_parser_destroy (parser);
   return ret;
}

entity_t *
entity_database_new (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   entity_t *entity = NULL;
   const entity_t *client_entity;
   char *client_id = NULL;
   mongoc_client_t *client = NULL;
   mongoc_database_t *db = NULL;
   char *database_name = NULL;
   bool ret = false;
   bson_t *database_opts = NULL;
   coll_or_db_opts_t *coll_or_db_opts = NULL;

   entity = entity_new (entity_map, "database");
   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "id", &entity->id);
   bson_parser_utf8 (parser, "client", &client_id);
   bson_parser_utf8 (parser, "databaseName", &database_name);
   bson_parser_doc_optional (parser, "databaseOptions", &database_opts);

   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   client_entity = entity_map_get (entity_map, client_id, error);
   if (!client_entity) {
      goto done;
   }

   client = (mongoc_client_t *) client_entity->value;
   db = mongoc_client_get_database (client, database_name);
   entity->value = (void *) db;

   if (database_opts) {
      coll_or_db_opts = coll_or_db_opts_new ();
      if (!coll_or_db_opts_parse (coll_or_db_opts, database_opts, error)) {
         goto done;
      }
      if (coll_or_db_opts->rc) {
         mongoc_database_set_read_concern (db, coll_or_db_opts->rc);
      }
      if (coll_or_db_opts->rp) {
         mongoc_database_set_read_prefs (db, coll_or_db_opts->rp);
      }
      if (coll_or_db_opts->wc) {
         mongoc_database_set_write_concern (db, coll_or_db_opts->wc);
      }
   }

   ret = true;
done:
   bson_free (client_id);
   bson_free (database_name);
   bson_parser_destroy (parser);
   bson_destroy (database_opts);
   coll_or_db_opts_destroy (coll_or_db_opts);
   if (!ret) {
      entity_destroy (entity);
      return NULL;
   }
   return entity;
}

entity_t *
entity_collection_new (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   entity_t *entity = NULL;
   entity_t *database_entity = NULL;
   mongoc_database_t *database = NULL;
   mongoc_collection_t *coll = NULL;
   bool ret = false;
   char *database_id = NULL;
   char *collection_name = NULL;
   bson_t *collection_opts = NULL;
   coll_or_db_opts_t *coll_or_db_opts = NULL;

   entity = entity_new (entity_map, "collection");
   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "id", &entity->id);
   bson_parser_utf8 (parser, "database", &database_id);
   bson_parser_utf8 (parser, "collectionName", &collection_name);
   bson_parser_doc_optional (parser, "collectionOptions", &collection_opts);
   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   database_entity = entity_map_get (entity_map, database_id, error);
   if (!database_entity) {
      goto done;
   }
   database = (mongoc_database_t *) database_entity->value;
   coll = mongoc_database_get_collection (database, collection_name);
   entity->value = (void *) coll;
   if (collection_opts) {
      coll_or_db_opts = coll_or_db_opts_new ();
      if (!coll_or_db_opts_parse (coll_or_db_opts, collection_opts, error)) {
         goto done;
      }
      if (coll_or_db_opts->rc) {
         mongoc_collection_set_read_concern (coll, coll_or_db_opts->rc);
      }
      if (coll_or_db_opts->rp) {
         mongoc_collection_set_read_prefs (coll, coll_or_db_opts->rp);
      }
      if (coll_or_db_opts->wc) {
         mongoc_collection_set_write_concern (coll, coll_or_db_opts->wc);
      }
   }
   ret = true;
done:
   bson_free (collection_name);
   bson_free (database_id);
   bson_parser_destroy (parser);
   bson_destroy (collection_opts);
   coll_or_db_opts_destroy (coll_or_db_opts);
   if (!ret) {
      entity_destroy (entity);
      return NULL;
   }
   return entity;
}

mongoc_session_opt_t *
session_opts_new (bson_t *bson, bson_error_t *error)
{
   bool ret = false;
   mongoc_session_opt_t *opts = NULL;
   bson_parser_t *bp = NULL;
   bson_parser_t *bp_opts = NULL;
   bool *causal_consistency = NULL;
   bool *snapshot = NULL;
   bson_t *default_transaction_opts = NULL;
   mongoc_write_concern_t *wc = NULL;
   mongoc_read_concern_t *rc = NULL;
   mongoc_read_prefs_t *rp = NULL;
   mongoc_transaction_opt_t *topts = NULL;

   bp = bson_parser_new ();
   bson_parser_bool_optional (bp, "causalConsistency", &causal_consistency);
   bson_parser_bool_optional (bp, "snapshot", &snapshot);
   bson_parser_doc_optional (bp, "defaultTransactionOptions", &default_transaction_opts);
   if (!bson_parser_parse (bp, bson, error)) {
      goto done;
   }

   opts = mongoc_session_opts_new ();
   if (causal_consistency) {
      mongoc_session_opts_set_causal_consistency (opts, *causal_consistency);
   }
   if (snapshot) {
      mongoc_session_opts_set_snapshot (opts, *snapshot);
   }

   if (default_transaction_opts) {
      bp_opts = bson_parser_new ();
      topts = mongoc_transaction_opts_new ();

      bson_parser_write_concern_optional (bp_opts, &wc);
      bson_parser_read_concern_optional (bp_opts, &rc);
      bson_parser_read_prefs_optional (bp_opts, &rp);
      if (!bson_parser_parse (bp_opts, default_transaction_opts, error)) {
         goto done;
      }

      if (wc) {
         mongoc_transaction_opts_set_write_concern (topts, wc);
      }

      if (rc) {
         mongoc_transaction_opts_set_read_concern (topts, rc);
      }

      if (rp) {
         mongoc_transaction_opts_set_read_prefs (topts, rp);
      }

      mongoc_session_opts_set_default_transaction_opts (opts, topts);
   }

   ret = true;
done:
   bson_parser_destroy_with_parsed_fields (bp);
   bson_parser_destroy_with_parsed_fields (bp_opts);
   mongoc_transaction_opts_destroy (topts);
   if (!ret) {
      mongoc_session_opts_destroy (opts);
      return NULL;
   }
   return opts;
}

entity_t *
entity_session_new (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   entity_t *entity = NULL;
   entity_t *client_entity = NULL;
   mongoc_client_t *client = NULL;
   char *client_id = NULL;
   bson_t *session_opts_bson = NULL;
   mongoc_session_opt_t *session_opts = NULL;
   bool ret = false;
   mongoc_client_session_t *session = NULL;

   entity = entity_new (entity_map, "session");
   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "id", &entity->id);
   bson_parser_utf8 (parser, "client", &client_id);
   bson_parser_doc_optional (parser, "sessionOptions", &session_opts_bson);
   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   client_entity = entity_map_get (entity_map, client_id, error);
   if (!client_entity) {
      goto done;
   }
   client = (mongoc_client_t *) client_entity->value;
   if (!client) {
      goto done;
   }
   if (session_opts_bson) {
      session_opts = session_opts_new (session_opts_bson, error);
      if (!session_opts) {
         goto done;
      }
   }
   session = mongoc_client_start_session (client, session_opts, error);
   if (!session) {
      goto done;
   }
   entity->value = session;
   /* Ending a session destroys the session object.
    * After a session is ended, match assertions may be made on the lsid.
    * So the lsid is copied from the session object on creation. */
   entity->lsid = bson_copy (mongoc_client_session_get_lsid (session));
   ret = true;

   entity->session_client_id = bson_strdup (client_id);
done:
   mongoc_session_opts_destroy (session_opts);
   bson_free (client_id);
   bson_destroy (session_opts_bson);
   bson_parser_destroy (parser);
   if (!ret) {
      entity_destroy (entity);
      return NULL;
   }
   return entity;
}

entity_t *
entity_bucket_new (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   entity_t *entity = NULL;
   mongoc_database_t *database = NULL;
   char *database_id = NULL;
   bool ret = false;
   bson_t *bucket_opts_bson = NULL;
   bson_parser_t *opts_parser = NULL;
   mongoc_read_concern_t *rc = NULL;
   mongoc_write_concern_t *wc = NULL;
   bson_t *opts = NULL;

   entity = entity_new (entity_map, "bucket");
   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "id", &entity->id);
   bson_parser_utf8 (parser, "database", &database_id);
   bson_parser_doc_optional (parser, "bucketOptions", &bucket_opts_bson);
   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   database = entity_map_get_database (entity_map, database_id, error);
   if (!database) {
      goto done;
   }

   opts_parser = bson_parser_new ();
   bson_parser_allow_extra (opts_parser, true);
   bson_parser_read_concern_optional (opts_parser, &rc);
   bson_parser_write_concern_optional (opts_parser, &wc);
   opts = bson_new ();
   bson_concat (opts, bson_parser_get_extra (opts_parser));
   if (rc) {
      mongoc_read_concern_append (rc, opts);
   }
   if (wc) {
      mongoc_write_concern_append (wc, opts);
   }

   entity->value = mongoc_gridfs_bucket_new (database, opts, NULL /* read prefs */, error);
   if (!entity->value) {
      goto done;
   }

   ret = true;
done:
   bson_free (database_id);
   bson_destroy (bucket_opts_bson);
   bson_parser_destroy (parser);
   bson_parser_destroy_with_parsed_fields (opts_parser);
   bson_destroy (opts);
   if (!ret) {
      entity_destroy (entity);
      return NULL;
   }
   return entity;
}

/* Caveat: The spec encourages, but does not require, that entities are defined
 * in dependency order:
 * "Test files SHOULD define entities in dependency order, such that all
 * referenced entities (e.g. client) are defined before any of their dependent
 * entities (e.g. database, session)."
 * If a test ever does break this pattern (flipping dependency order), that can
 * be solved by:
 * - creating C objects lazily in entity_map_get.
 * - creating entities in dependency order (all clients first, then databases,
 *   etc.)
 * The current implementation here does the simple thing and creates the C
 * object immediately.
 */
bool
entity_map_create (entity_map_t *entity_map, bson_t *bson, bson_error_t *error)
{
   bson_iter_t iter;
   const char *entity_type;
   bson_t entity_bson;
   entity_t *entity = NULL;
   entity_t *entity_iter = NULL;
   bool ret = false;

   bson_iter_init (&iter, bson);
   if (!bson_iter_next (&iter)) {
      test_set_error (error, "Empty entity");
      goto done;
   }

   entity_type = bson_iter_key (&iter);
   bson_iter_bson (&iter, &entity_bson);
   if (bson_iter_next (&iter)) {
      test_set_error (error, "Extra field in entity: %s: %s", bson_iter_key (&iter), tmp_json (bson));
      goto done;
   }

   if (0 == strcmp (entity_type, "client")) {
      entity = entity_client_new (entity_map, &entity_bson, error);
   } else if (0 == strcmp (entity_type, "clientEncryption")) {
      entity = entity_client_encryption_new (entity_map, &entity_bson, error);
   } else if (0 == strcmp (entity_type, "database")) {
      entity = entity_database_new (entity_map, &entity_bson, error);
   } else if (0 == strcmp (entity_type, "collection")) {
      entity = entity_collection_new (entity_map, &entity_bson, error);
   } else if (0 == strcmp (entity_type, "session")) {
      entity = entity_session_new (entity_map, &entity_bson, error);
   } else if (0 == strcmp (entity_type, "bucket")) {
      entity = entity_bucket_new (entity_map, &entity_bson, error);
   } else {
      test_set_error (error, "Unknown entity type: %s: %s", entity_type, tmp_json (bson));
      goto done;
   }

   if (!entity) {
      goto done;
   }

   LL_FOREACH (entity_map->entities, entity_iter)
   {
      if (0 == strcmp (entity_iter->id, entity->id)) {
         test_set_error (error, "Attempting to create duplicate entity: '%s'", entity->id);
         entity_destroy (entity);
         goto done;
      }
   }

   ret = true;
done:
   if (!ret) {
      entity_destroy (entity);
   } else {
      LL_PREPEND (entity_map->entities, entity);
   }
   return ret;
}

static void
entity_destroy (entity_t *entity)
{
   event_t *event = NULL;
   event_t *tmp = NULL;

   if (!entity) {
      return;
   }

   BSON_ASSERT (entity->type);

   if (0 == strcmp ("client", entity->type)) {
      mongoc_client_t *client = NULL;

      client = (mongoc_client_t *) entity->value;
      mongoc_client_destroy (client);
   } else if (0 == strcmp ("clientEncryption", entity->type)) {
      mongoc_client_encryption_t *ce = NULL;

      ce = (mongoc_client_encryption_t *) entity->value;
      mongoc_client_encryption_destroy (ce);
   } else if (0 == strcmp ("database", entity->type)) {
      mongoc_database_t *db = NULL;

      db = (mongoc_database_t *) entity->value;
      mongoc_database_destroy (db);
   } else if (0 == strcmp ("collection", entity->type)) {
      mongoc_collection_t *coll = NULL;

      coll = (mongoc_collection_t *) entity->value;
      mongoc_collection_destroy (coll);
   } else if (0 == strcmp ("session", entity->type)) {
      mongoc_client_session_t *sess = NULL;

      sess = (mongoc_client_session_t *) entity->value;
      mongoc_client_session_destroy (sess);
   } else if (0 == strcmp ("changestream", entity->type)) {
      mongoc_change_stream_t *changestream = NULL;

      changestream = (mongoc_change_stream_t *) entity->value;
      mongoc_change_stream_destroy (changestream);
   } else if (0 == strcmp ("bson", entity->type)) {
      bson_val_t *value = entity->value;

      bson_val_destroy (value);
   } else if (0 == strcmp ("bucket", entity->type)) {
      mongoc_gridfs_bucket_t *bucket = entity->value;

      mongoc_gridfs_bucket_destroy (bucket);
   } else if (0 == strcmp ("findcursor", entity->type)) {
      entity_findcursor_t *findcursor = entity->value;

      mongoc_cursor_destroy (findcursor->cursor);
      bson_free (findcursor);
   } else if (0 == strcmp ("bson_array", entity->type)) {
      mongoc_array_t *array = entity->value;

      bson_t **const begin = array->data;
      bson_t **const end = begin + array->len;
      for (bson_t **iter = begin; iter != end; ++iter) {
         bson_destroy (*iter);
      }

      _mongoc_array_destroy (array);
      bson_free (array);
   } else if (0 == strcmp ("size_t", entity->type)) {
      size_t *v = entity->value;

      bson_free (v);
   } else {
      test_error ("Attempting to destroy unrecognized entity type: %s, id: %s", entity->type, entity->id);
   }

   LL_FOREACH_SAFE (entity->events, event, tmp)
   {
      event_destroy (event);
   }

   _mongoc_array_destroy (&entity->observe_events);
   _mongoc_array_destroy (&entity->store_events);
   bson_destroy (entity->ignore_command_monitoring_events);
   bson_free (entity->type);
   bson_free (entity->id);
   bson_destroy (entity->lsid);
   bson_free (entity->session_client_id);
   bson_free (entity->observe_sensitive_commands);
   bson_free (entity);
}

entity_t *
entity_map_get (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = NULL;
   LL_FOREACH (entity_map->entities, entity)
   {
      if (0 == strcmp (entity->id, id)) {
         return entity;
      }
   }

   test_set_error (error, "Entity '%s' not found", id);
   return NULL;
}

bool
entity_map_delete (entity_map_t *em, const char *id, bson_error_t *error)
{
   entity_t *entity = entity_map_get (em, id, error);
   if (!entity) {
      return false;
   }

   LL_DELETE (em->entities, entity);
   entity_destroy (entity);

   return true;
}

static entity_t *
_entity_map_get_by_type (entity_map_t *entity_map, const char *id, const char *type, bson_error_t *error)
{
   entity_t *entity = NULL;

   entity = entity_map_get (entity_map, id, error);
   if (!entity) {
      return NULL;
   }

   if (0 != strcmp (entity->type, type)) {
      test_set_error (error, "Unexpected entity type. Expected: %s, got %s", type, entity->type);
      return NULL;
   }
   return entity;
}

mongoc_client_t *
entity_map_get_client (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "client", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_client_t *) entity->value;
}

mongoc_client_encryption_t *
entity_map_get_client_encryption (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "clientEncryption", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_client_encryption_t *) entity->value;
}

mongoc_database_t *
entity_map_get_database (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "database", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_database_t *) entity->value;
}

mongoc_collection_t *
entity_map_get_collection (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "collection", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_collection_t *) entity->value;
}

mongoc_change_stream_t *
entity_map_get_changestream (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "changestream", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_change_stream_t *) entity->value;
}

entity_findcursor_t *
entity_map_get_findcursor (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "findcursor", error);
   if (!entity) {
      return NULL;
   }
   return (entity_findcursor_t *) entity->value;
}

bson_val_t *
entity_map_get_bson (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "bson", error);
   if (!entity) {
      return NULL;
   }
   return (bson_val_t *) entity->value;
}

mongoc_array_t *
entity_map_get_bson_array (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "bson_array", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_array_t *) entity->value;
}

size_t *
entity_map_get_size_t (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "size_t", error);
   if (!entity) {
      return NULL;
   }
   return (size_t *) entity->value;
}

mongoc_client_session_t *
entity_map_get_session (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "session", error);
   if (!entity) {
      return NULL;
   }
   if (!entity->value) {
      test_set_error (error, "entity: %s is an ended session that is no longer valid to use", id);
      return NULL;
   }
   return (mongoc_client_session_t *) entity->value;
}

static bson_t *
entity_map_get_lsid (entity_map_t *em, char *session_id, bson_error_t *error)
{
   entity_t *entity = NULL;

   entity = entity_map_get (em, session_id, error);
   if (!entity) {
      return NULL;
   }
   if (!entity->lsid) {
      test_set_error (error, "entity %s of type %s does not have an lsid", session_id, entity->type);
      return NULL;
   }
   return entity->lsid;
}

mongoc_gridfs_bucket_t *
entity_map_get_bucket (entity_map_t *entity_map, const char *id, bson_error_t *error)
{
   entity_t *entity = _entity_map_get_by_type (entity_map, id, "bucket", error);
   if (!entity) {
      return NULL;
   }
   return (mongoc_gridfs_bucket_t *) entity->value;
}

static bool
_entity_map_add (entity_map_t *em, const char *id, const char *type, void *value, bson_error_t *error)
{
   bson_error_t tmperr;
   entity_t *entity = NULL;

   if (NULL != entity_map_get (em, id, &tmperr)) {
      test_set_error (error, "Attempting to overwrite entity: %s", id);
      return false;
   }

   entity = entity_new (em, type);
   entity->value = value;
   entity->id = bson_strdup (id);
   LL_PREPEND (em->entities, entity);
   return true;
}

bool
entity_map_add_changestream (entity_map_t *em,
                             const char *id,
                             mongoc_change_stream_t *changestream,
                             bson_error_t *error)
{
   return _entity_map_add (em, id, "changestream", (void *) changestream, error);
}

void
entity_findcursor_iterate_until_document_or_error (entity_findcursor_t *findcursor,
                                                   const bson_t **document,
                                                   bson_error_t *error,
                                                   const bson_t **error_document)
{
   *document = NULL;

   if (findcursor->first_result) {
      *document = findcursor->first_result;
      findcursor->first_result = NULL;
      return;
   }

   while (!mongoc_cursor_next (findcursor->cursor, document)) {
      if (mongoc_cursor_error_document (findcursor->cursor, error, error_document)) {
         return;
      }
   }
}

bool
entity_map_add_findcursor (
   entity_map_t *em, const char *id, mongoc_cursor_t *cursor, const bson_t *first_result, bson_error_t *error)
{
   entity_findcursor_t *findcursor;

   findcursor = (entity_findcursor_t *) bson_malloc0 (sizeof (entity_findcursor_t));
   findcursor->cursor = cursor;
   findcursor->first_result = first_result;
   return _entity_map_add (em, id, "findcursor", (void *) findcursor, error);
}

bool
entity_map_add_bson (entity_map_t *em, const char *id, bson_val_t *val, bson_error_t *error)
{
   return _entity_map_add (em, id, "bson", (void *) bson_val_copy (val), error);
}

bool
entity_map_add_bson_array (entity_map_t *em, const char *id, bson_error_t *error)
{
   // Note: the specification states we should be storing a BSON object of array
   // type, but we use an array of BSON objects instead to make append and
   // iteration easier.
   mongoc_array_t *array = bson_malloc (sizeof (mongoc_array_t));
   mongoc_array_aligned_init (array, bson_t *);
   return _entity_map_add (em, id, "bson_array", (void *) array, error);
}

bool
entity_map_add_size_t (entity_map_t *em, const char *id, size_t *value, bson_error_t *error)
{
   return _entity_map_add (em, id, "size_t", value, error);
}

/* implement $$sessionLsid */
static bool
special_session_lsid (bson_matcher_t *matcher,
                      const bson_t *assertion,
                      const bson_val_t *actual,
                      void *ctx,
                      const char *path,
                      bson_error_t *error)
{
   bool ret = false;
   const char *id;
   bson_val_t *session_val = NULL;
   bson_t *lsid = NULL;
   entity_map_t *em = (entity_map_t *) ctx;
   bson_iter_t iter;

   bson_iter_init (&iter, assertion);
   bson_iter_next (&iter);

   if (!BSON_ITER_HOLDS_UTF8 (&iter)) {
      test_set_error (error, "unexpected $$sessionLsid does not contain utf8: %s", tmp_json (assertion));
      goto done;
   }

   id = bson_iter_utf8 (&iter, NULL);
   lsid = entity_map_get_lsid (em, (char *) id, error);
   if (!lsid) {
      goto done;
   }

   session_val = bson_val_from_bson (lsid);
   if (!bson_matcher_match (matcher, session_val, actual, path, false, error)) {
      goto done;
   }


   ret = true;
done:
   bson_val_destroy (session_val);
   return ret;
}

/* implement $$matchesEntity */
bool
special_matches_entity (bson_matcher_t *matcher,
                        const bson_t *assertion,
                        const bson_val_t *actual,
                        void *ctx,
                        const char *path,
                        bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;
   entity_map_t *em = (entity_map_t *) ctx;
   bson_val_t *entity_val = NULL;
   const char *id;

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));

   if (!BSON_ITER_HOLDS_UTF8 (&iter)) {
      test_set_error (error, "unexpected $$matchesEntity does not contain utf8: %s", tmp_json (assertion));
      goto done;
   }

   id = bson_iter_utf8 (&iter, NULL);
   entity_val = entity_map_get_bson (em, id, error);
   if (!entity_val) {
      goto done;
   }

   if (!bson_matcher_match (matcher, entity_val, actual, path, false, error)) {
      goto done;
   }

   ret = true;
done:
   return ret;
}

bool
entity_map_match (
   entity_map_t *em, const bson_val_t *expected, const bson_val_t *actual, bool array_of_root_docs, bson_error_t *error)
{
   bson_matcher_t *matcher;
   bool ret;

   matcher = bson_matcher_new ();
   bson_matcher_add_special (matcher, "$$sessionLsid", special_session_lsid, em);
   bson_matcher_add_special (matcher, "$$matchesEntity", special_matches_entity, em);
   ret = bson_matcher_match (matcher, expected, actual, "", array_of_root_docs, error);
   bson_matcher_destroy (matcher);
   return ret;
}

char *
event_list_to_string (event_t *events)
{
   bson_string_t *str = NULL;
   event_t *eiter = NULL;

   str = bson_string_new ("");
   LL_FOREACH (events, eiter)
   {
      bson_string_append_printf (str, "- %s:", eiter->type);
      if (eiter->command_name) {
         bson_string_append_printf (str, " cmd=%s", eiter->command_name);
      }
      if (eiter->database_name) {
         bson_string_append_printf (str, " db=%s", eiter->database_name);
      }
      if (eiter->command) {
         bson_string_append_printf (str, " sent %s", tmp_json (eiter->command));
      }
      if (eiter->reply) {
         bson_string_append_printf (str, " received %s", tmp_json (eiter->reply));
      }
      bson_string_append (str, "\n");
   }
   return bson_string_free (str, false);
}


bool
entity_map_end_session (entity_map_t *em, char *session_id, bson_error_t *error)
{
   bool ret = false;
   entity_t *entity = NULL;

   entity = entity_map_get (em, session_id, error);
   if (!entity) {
      goto done;
   }

   if (0 != strcmp (entity->type, "session")) {
      test_set_error (error, "expected session for %s but got %s", session_id, entity->type);
      goto done;
   }

   mongoc_client_session_destroy ((mongoc_client_session_t *) entity->value);
   entity->value = NULL;
   ret = true;
done:
   return ret;
}

char *
entity_map_get_session_client_id (entity_map_t *em, char *session_id, bson_error_t *error)
{
   char *ret = NULL;
   entity_t *entity = NULL;

   entity = entity_map_get (em, session_id, error);
   if (!entity) {
      goto done;
   }

   if (0 != strcmp (entity->type, "session")) {
      test_set_error (error, "expected session for %s but got %s", session_id, entity->type);
      goto done;
   }

   ret = entity->session_client_id;
done:
   return ret;
}

void
entity_map_set_reduced_heartbeat (entity_map_t *em, bool val)
{
   em->reduced_heartbeat = val;
}

void
entity_map_disable_event_listeners (entity_map_t *em)
{
   entity_t *eiter = NULL;

   LL_FOREACH (em->entities, eiter)
   {
      if (0 == strcmp (eiter->type, "client")) {
         mongoc_client_t *client = (mongoc_client_t *) eiter->value;

         mongoc_client_set_apm_callbacks (client, NULL, NULL);
      }
   }
}
