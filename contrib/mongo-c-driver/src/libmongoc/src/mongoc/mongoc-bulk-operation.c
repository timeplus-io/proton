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


#include "mongoc-bulk-operation.h"
#include "mongoc-bulk-operation-private.h"
#include "mongoc-client-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-write-concern-private.h"
#include "mongoc-util-private.h"
#include "mongoc-opts-private.h"
#include "mongoc-write-command-private.h"


/*
 * This is the implementation of both write commands and bulk write commands.
 * They are all implemented as one contiguous set since we'd like to cut down
 * on code duplication here.
 *
 * This implementation is currently naive.
 *
 * Some interesting optimizations might be:
 *
 *   - If unordered mode, send operations as we get them instead of waiting
 *     for execute() to be called. This could save us memcpy()'s too.
 *   - If there is no acknowledgement desired, keep a count of how many
 *     replies we need and ask the socket layer to skip that many bytes
 *     when reading.
 *   - Try to use iovec to send write commands with subdocuments rather than
 *     copying them into the write command document.
 */


mongoc_bulk_operation_t *
mongoc_bulk_operation_new (bool ordered)
{
   mongoc_bulk_operation_t *bulk;

   bulk = BSON_ALIGNED_ALLOC0 (mongoc_bulk_operation_t);
   bulk->flags.bypass_document_validation = false;
   bulk->flags.ordered = ordered;
   bulk->server_id = 0;

   bson_init (&bulk->let);

   mongoc_array_aligned_init (&bulk->commands, mongoc_write_command_t);
   _mongoc_write_result_init (&bulk->result);

   return bulk;
}


mongoc_bulk_operation_t *
_mongoc_bulk_operation_new (mongoc_client_t *client,                     /* IN */
                            const char *database,                        /* IN */
                            const char *collection,                      /* IN */
                            mongoc_bulk_write_flags_t flags,             /* IN */
                            const mongoc_write_concern_t *write_concern) /* IN */
{
   mongoc_bulk_operation_t *bulk;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT_PARAM (collection);

   bulk = mongoc_bulk_operation_new (flags.ordered);
   bulk->client = client;
   bulk->database = bson_strdup (database);
   bulk->collection = bson_strdup (collection);
   bulk->write_concern = mongoc_write_concern_copy (write_concern);
   bulk->executed = false;
   bulk->flags = flags;
   bulk->operation_id = ++client->cluster.operation_id;

   return bulk;
}


void
mongoc_bulk_operation_destroy (mongoc_bulk_operation_t *bulk) /* IN */
{
   mongoc_write_command_t *command;

   if (bulk) {
      for (size_t i = 0; i < bulk->commands.len; i++) {
         command = &_mongoc_array_index (&bulk->commands, mongoc_write_command_t, i);
         _mongoc_write_command_destroy (command);
      }

      bson_free (bulk->database);
      bson_free (bulk->collection);
      bson_value_destroy (&bulk->comment);
      bson_destroy (&bulk->let);
      mongoc_write_concern_destroy (bulk->write_concern);
      _mongoc_array_destroy (&bulk->commands);

      _mongoc_write_result_destroy (&bulk->result);

      bson_free (bulk);
   }
}


/* already failed, e.g. a bad call to mongoc_bulk_operation_insert? */
#define BULK_EXIT_IF_PRIOR_ERROR       \
   do {                                \
      if (bulk->result.error.domain) { \
         EXIT;                         \
      }                                \
   } while (0)

#define BULK_RETURN_IF_PRIOR_ERROR                                            \
   do {                                                                       \
      if (bulk->result.error.domain) {                                        \
         if (error != &bulk->result.error) {                                  \
            bson_set_error (error,                                            \
                            MONGOC_ERROR_COMMAND,                             \
                            MONGOC_ERROR_COMMAND_INVALID_ARG,                 \
                            "Bulk operation is invalid from prior error: %s", \
                            bulk->result.error.message);                      \
         };                                                                   \
         return false;                                                        \
      };                                                                      \
   } while (0)


static bool
_mongoc_bulk_operation_remove_with_opts (mongoc_bulk_operation_t *bulk,
                                         const bson_t *selector,
                                         const mongoc_bulk_remove_opts_t *remove_opts,
                                         int32_t limit,
                                         bson_error_t *error) /* OUT */
{
   mongoc_write_command_t command = {0};
   mongoc_write_command_t *last;
   bson_t cmd_opts = BSON_INITIALIZER;
   bson_t opts;
   bool has_collation;
   bool ret = false;
   bool has_delete_hint;

   ENTRY;

   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (selector);

   bson_init (&opts);

   /* allow "limit" in opts, but it must be the correct limit */
   if (remove_opts->limit != limit) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Invalid \"limit\" in opts: %" PRId32 "."
                      " The value must be %" PRId32 ", or omitted.",
                      remove_opts->limit,
                      limit);
      GOTO (done);
   }

   bson_append_int32 (&opts, "limit", 5, limit);
   has_collation = !bson_empty (&remove_opts->collation);
   if (has_collation) {
      bson_append_document (&opts, "collation", 9, &remove_opts->collation);
   }

   has_delete_hint = !!(remove_opts->hint.value_type);
   if (has_delete_hint) {
      bson_append_value (&opts, "hint", 4, &remove_opts->hint);
   }

   if (bulk->commands.len) {
      last = &_mongoc_array_index (&bulk->commands, mongoc_write_command_t, bulk->commands.len - 1);
      if (last->type == MONGOC_WRITE_COMMAND_DELETE) {
         last->flags.has_collation |= has_collation;
         last->flags.has_delete_hint |= has_delete_hint;
         last->flags.has_multi_write |= (remove_opts->limit == 0);
         _mongoc_write_command_delete_append (last, selector, &opts);
         ret = true;
         GOTO (done);
      }
   }

   if (bulk->comment.value_type != BSON_TYPE_EOD) {
      bson_append_value (&cmd_opts, "comment", 7, &bulk->comment);
   }

   if (!bson_empty (&bulk->let)) {
      bson_append_document (&cmd_opts, "let", 3, &bulk->let);
   }

   _mongoc_write_command_init_delete (&command, selector, &cmd_opts, &opts, bulk->flags, bulk->operation_id);

   command.flags.has_collation = has_collation;
   command.flags.has_delete_hint = has_delete_hint;
   command.flags.has_multi_write = (remove_opts->limit == 0);

   _mongoc_array_append_val (&bulk->commands, command);
   ret = true;

done:
   bson_destroy (&cmd_opts);
   bson_destroy (&opts);
   RETURN (ret);
}


bool
mongoc_bulk_operation_remove_one_with_opts (mongoc_bulk_operation_t *bulk,
                                            const bson_t *selector,
                                            const bson_t *opts,
                                            bson_error_t *error) /* OUT */
{
   mongoc_bulk_remove_one_opts_t remove_opts;
   bool ret;

   ENTRY;

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_remove_one_opts_parse (bulk->client, opts, &remove_opts, error)) {
      _mongoc_bulk_remove_one_opts_cleanup (&remove_opts);
      RETURN (false);
   }

   ret = _mongoc_bulk_operation_remove_with_opts (bulk, selector, &remove_opts.remove, 1, error);

   _mongoc_bulk_remove_one_opts_cleanup (&remove_opts);
   RETURN (ret);
}


bool
mongoc_bulk_operation_remove_many_with_opts (mongoc_bulk_operation_t *bulk,
                                             const bson_t *selector,
                                             const bson_t *opts,
                                             bson_error_t *error) /* OUT */
{
   mongoc_bulk_remove_many_opts_t remove_opts;
   bool ret;

   ENTRY;

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_remove_many_opts_parse (bulk->client, opts, &remove_opts, error)) {
      _mongoc_bulk_remove_many_opts_cleanup (&remove_opts);
      RETURN (false);
   }

   ret = _mongoc_bulk_operation_remove_with_opts (bulk, selector, &remove_opts.remove, 0, error);

   _mongoc_bulk_remove_many_opts_cleanup (&remove_opts);
   RETURN (ret);
}


void
mongoc_bulk_operation_remove (mongoc_bulk_operation_t *bulk, /* IN */
                              const bson_t *selector)        /* IN */
{
   bson_error_t *error = &bulk->result.error;

   ENTRY;

   BULK_EXIT_IF_PRIOR_ERROR;

   if (!mongoc_bulk_operation_remove_many_with_opts (bulk, selector, NULL, error)) {
      MONGOC_WARNING ("%s", error->message);
   }

   if (error->domain) {
      MONGOC_WARNING ("%s", error->message);
   }

   EXIT;
}


void
mongoc_bulk_operation_remove_one (mongoc_bulk_operation_t *bulk, /* IN */
                                  const bson_t *selector)        /* IN */
{
   bson_error_t *error = &bulk->result.error;

   ENTRY;

   BULK_EXIT_IF_PRIOR_ERROR;

   if (!mongoc_bulk_operation_remove_one_with_opts (bulk, selector, NULL, error)) {
      MONGOC_WARNING ("%s", error->message);
   }

   if (error->domain) {
      MONGOC_WARNING ("%s", error->message);
   }

   EXIT;
}

void
mongoc_bulk_operation_delete (mongoc_bulk_operation_t *bulk, const bson_t *selector)
{
   ENTRY;

   mongoc_bulk_operation_remove (bulk, selector);

   EXIT;
}

void
mongoc_bulk_operation_delete_one (mongoc_bulk_operation_t *bulk, const bson_t *selector)
{
   ENTRY;

   mongoc_bulk_operation_remove_one (bulk, selector);

   EXIT;
}

void
mongoc_bulk_operation_insert (mongoc_bulk_operation_t *bulk, const bson_t *document)
{
   ENTRY;

   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (document);

   if (!mongoc_bulk_operation_insert_with_opts (bulk, document, NULL /* opts */, &bulk->result.error)) {
      MONGOC_WARNING ("%s", bulk->result.error.message);
   }

   EXIT;
}

bool
mongoc_bulk_operation_insert_with_opts (mongoc_bulk_operation_t *bulk,
                                        const bson_t *document,
                                        const bson_t *opts,
                                        bson_error_t *error)
{
   mongoc_bulk_insert_opts_t insert_opts;
   mongoc_write_command_t command = {0};
   mongoc_write_command_t *last;
   bson_t cmd_opts = BSON_INITIALIZER;
   bool ret = false;

   ENTRY;

   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (document);

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_insert_opts_parse (bulk->client, opts, &insert_opts, error)) {
      GOTO (done);
   }

   if (!_mongoc_validate_new_document (document, insert_opts.validate, error)) {
      GOTO (done);
   }

   /* Note: mongoc_bulk_insert_opts_t specifies allow_extra=False, so there is
    * no reason to concatenate cmd_opts with &insert_opts.extra. */

   if (bulk->commands.len) {
      last = &_mongoc_array_index (&bulk->commands, mongoc_write_command_t, bulk->commands.len - 1);

      if (last->type == MONGOC_WRITE_COMMAND_INSERT) {
         _mongoc_write_command_insert_append (last, document);
         ret = true;
         GOTO (done);
      }
   }

   if (bulk->comment.value_type != BSON_TYPE_EOD) {
      bson_append_value (&cmd_opts, "comment", 7, &bulk->comment);
   }

   _mongoc_write_command_init_insert (&command, document, &cmd_opts, bulk->flags, bulk->operation_id);

   _mongoc_array_append_val (&bulk->commands, command);

   ret = true;

done:
   _mongoc_bulk_insert_opts_cleanup (&insert_opts);
   bson_destroy (&cmd_opts);

   RETURN (ret);
}

static void
_mongoc_bulk_operation_update_append (mongoc_bulk_operation_t *bulk,
                                      const bson_t *selector,
                                      const bson_t *document,
                                      const mongoc_bulk_update_opts_t *update_opts,
                                      const bson_t *array_filters,
                                      const bson_t *extra_opts)
{
   mongoc_write_command_t command = {0};
   mongoc_write_command_t *last;
   bson_t cmd_opts = BSON_INITIALIZER;
   bson_t opts;
   bool has_collation;
   bool has_array_filters;
   bool has_update_hint;

   bson_init (&opts);
   bson_append_bool (&opts, "upsert", 6, update_opts->upsert);
   bson_append_bool (&opts, "multi", 5, update_opts->multi);

   has_array_filters = !bson_empty0 (array_filters);
   if (has_array_filters) {
      bson_append_array (&opts, "arrayFilters", 12, array_filters);
   }

   has_collation = !bson_empty (&update_opts->collation);
   if (has_collation) {
      bson_append_document (&opts, "collation", 9, &update_opts->collation);
   }

   has_update_hint = !!(update_opts->hint.value_type);
   if (has_update_hint) {
      bson_append_value (&opts, "hint", 4, &update_opts->hint);
   }

   if (extra_opts) {
      bson_concat (&opts, extra_opts);
   }

   if (bulk->commands.len) {
      last = &_mongoc_array_index (&bulk->commands, mongoc_write_command_t, bulk->commands.len - 1);
      if (last->type == MONGOC_WRITE_COMMAND_UPDATE) {
         last->flags.has_array_filters |= has_array_filters;
         last->flags.has_collation |= has_collation;
         last->flags.has_update_hint |= has_update_hint;
         last->flags.has_multi_write |= update_opts->multi;
         _mongoc_write_command_update_append (last, selector, document, &opts);
         GOTO (done);
      }
   }

   if (bulk->comment.value_type != BSON_TYPE_EOD) {
      bson_append_value (&cmd_opts, "comment", 7, &bulk->comment);
   }

   if (!bson_empty (&bulk->let)) {
      bson_append_document (&cmd_opts, "let", 3, &bulk->let);
   }

   _mongoc_write_command_init_update (&command, selector, document, &cmd_opts, &opts, bulk->flags, bulk->operation_id);

   command.flags.has_array_filters = has_array_filters;
   command.flags.has_collation = has_collation;
   command.flags.has_update_hint = has_update_hint;
   command.flags.has_multi_write = update_opts->multi;

   _mongoc_array_append_val (&bulk->commands, command);

done:
   bson_destroy (&cmd_opts);
   bson_destroy (&opts);
}

static bool
_mongoc_bulk_operation_update_with_opts (mongoc_bulk_operation_t *bulk,
                                         const bson_t *selector,
                                         const bson_t *document,
                                         const mongoc_bulk_update_opts_t *update_opts,
                                         const bson_t *array_filters,
                                         const bson_t *extra_opts,
                                         bool multi,
                                         bson_error_t *error) /* OUT */
{
   ENTRY;

   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (selector);
   BSON_ASSERT_PARAM (document);

   if (!_mongoc_validate_update (document, update_opts->validate, error)) {
      RETURN (false);
   }

   /* allow "multi" in opts, but it must be the correct multi */
   if (update_opts->multi != multi) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Invalid \"multi\" in opts: %s."
                      " The value must be %s, or omitted.",
                      update_opts->multi ? "true" : "false",
                      multi ? "true" : "false");
      RETURN (false);
   }

   _mongoc_bulk_operation_update_append (bulk, selector, document, update_opts, array_filters, extra_opts);

   RETURN (true);
}

bool
mongoc_bulk_operation_update_one_with_opts (mongoc_bulk_operation_t *bulk,
                                            const bson_t *selector,
                                            const bson_t *document,
                                            const bson_t *opts,
                                            bson_error_t *error) /* OUT */
{
   mongoc_bulk_update_one_opts_t update_opts;
   bool ret;

   ENTRY;

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_update_one_opts_parse (bulk->client, opts, &update_opts, error)) {
      _mongoc_bulk_update_one_opts_cleanup (&update_opts);
      RETURN (false);
   }

   ret = _mongoc_bulk_operation_update_with_opts (bulk,
                                                  selector,
                                                  document,
                                                  &update_opts.update,
                                                  &update_opts.arrayFilters,
                                                  &update_opts.extra,
                                                  false /* multi */,
                                                  error);

   _mongoc_bulk_update_one_opts_cleanup (&update_opts);
   RETURN (ret);
}

bool
mongoc_bulk_operation_update_many_with_opts (mongoc_bulk_operation_t *bulk,
                                             const bson_t *selector,
                                             const bson_t *document,
                                             const bson_t *opts,
                                             bson_error_t *error) /* OUT */
{
   mongoc_bulk_update_many_opts_t update_opts;
   bool ret;

   ENTRY;

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_update_many_opts_parse (bulk->client, opts, &update_opts, error)) {
      _mongoc_bulk_update_many_opts_cleanup (&update_opts);
      RETURN (false);
   }

   ret = _mongoc_bulk_operation_update_with_opts (bulk,
                                                  selector,
                                                  document,
                                                  &update_opts.update,
                                                  &update_opts.arrayFilters,
                                                  &update_opts.extra,
                                                  true /* multi */,
                                                  error);

   _mongoc_bulk_update_many_opts_cleanup (&update_opts);
   RETURN (ret);
}

void
mongoc_bulk_operation_update (mongoc_bulk_operation_t *bulk,
                              const bson_t *selector,
                              const bson_t *document,
                              bool upsert)
{
   bson_t opts;
   bson_error_t *error = &bulk->result.error;

   ENTRY;

   BULK_EXIT_IF_PRIOR_ERROR;

   bson_init (&opts);
   if (upsert) {
      BSON_APPEND_BOOL (&opts, "upsert", upsert);
   }

   if (!mongoc_bulk_operation_update_many_with_opts (bulk, selector, document, &opts, error)) {
      MONGOC_WARNING ("%s", error->message);
   }

   bson_destroy (&opts);

   if (error->domain) {
      MONGOC_WARNING ("%s", error->message);
   }

   EXIT;
}

void
mongoc_bulk_operation_update_one (mongoc_bulk_operation_t *bulk,
                                  const bson_t *selector,
                                  const bson_t *document,
                                  bool upsert)
{
   bson_t opts;
   bson_error_t *error = &bulk->result.error;

   ENTRY;

   BULK_EXIT_IF_PRIOR_ERROR;

   bson_init (&opts);
   BSON_APPEND_BOOL (&opts, "upsert", upsert);

   if (!mongoc_bulk_operation_update_one_with_opts (bulk, selector, document, &opts, error)) {
      MONGOC_WARNING ("%s", error->message);
   }

   bson_destroy (&opts);

   if (error->domain) {
      MONGOC_WARNING ("%s", error->message);
   }

   EXIT;
}

bool
mongoc_bulk_operation_replace_one_with_opts (mongoc_bulk_operation_t *bulk,
                                             const bson_t *selector,
                                             const bson_t *document,
                                             const bson_t *opts,
                                             bson_error_t *error) /* OUT */
{
   mongoc_bulk_replace_one_opts_t repl_opts;
   mongoc_bulk_update_opts_t *update_opts = &repl_opts.update;
   bool ret = false;

   ENTRY;

   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (selector);
   BSON_ASSERT_PARAM (document);

   BULK_RETURN_IF_PRIOR_ERROR;

   if (!_mongoc_bulk_replace_one_opts_parse (bulk->client, opts, &repl_opts, error)) {
      GOTO (done);
   }

   if (!_mongoc_validate_replace (document, update_opts->validate, error)) {
      GOTO (done);
   }

   /* allow "multi" in opts, but it must be the correct multi */
   if (update_opts->multi) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Invalid \"multi\": true in opts for"
                      " mongoc_bulk_operation_replace_one_with_opts."
                      " The value must be true, or omitted.");
      GOTO (done);
   }

   _mongoc_bulk_operation_update_append (bulk, selector, document, update_opts, NULL, &repl_opts.extra);
   ret = true;

done:
   _mongoc_bulk_replace_one_opts_cleanup (&repl_opts);
   RETURN (ret);
}

void
mongoc_bulk_operation_replace_one (mongoc_bulk_operation_t *bulk,
                                   const bson_t *selector,
                                   const bson_t *document,
                                   bool upsert)
{
   bson_t opts = BSON_INITIALIZER;
   bson_error_t *error = &bulk->result.error;

   ENTRY;

   BSON_APPEND_BOOL (&opts, "upsert", upsert);

   if (!mongoc_bulk_operation_replace_one_with_opts (bulk, selector, document, &opts, error)) {
      MONGOC_WARNING ("%s", error->message);
   }

   bson_destroy (&opts);

   EXIT;
}

uint32_t
mongoc_bulk_operation_execute (mongoc_bulk_operation_t *bulk, /* IN */
                               bson_t *reply,                 /* OUT */
                               bson_error_t *error)           /* OUT */
{
   mongoc_cluster_t *cluster;
   mongoc_write_command_t *command;
   mongoc_server_stream_t *server_stream;
   bool ret;
   uint32_t offset = 0;

   ENTRY;

   BSON_ASSERT_PARAM (bulk);

   if (!bulk->client) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "mongoc_bulk_operation_execute() requires a client "
                      "and one has not been set.");
      GOTO (err);
   }
   cluster = &bulk->client->cluster;

   if (bulk->executed) {
      _mongoc_write_result_destroy (&bulk->result);
      _mongoc_write_result_init (&bulk->result);
   }

   bulk->executed = true;

   if (!bulk->database) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "mongoc_bulk_operation_execute() requires a database "
                      "and one has not been set.");
      GOTO (err);
   } else if (!bulk->collection) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "mongoc_bulk_operation_execute() requires a collection "
                      "and one has not been set.");
      GOTO (err);
   }

   /* error stored by functions like mongoc_bulk_operation_insert that
    * can't report errors immediately */
   if (bulk->result.error.domain) {
      if (error) {
         memcpy (error, &bulk->result.error, sizeof (bson_error_t));
      }

      GOTO (err);
   }

   if (!bulk->commands.len) {
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Cannot do an empty bulk write");
      GOTO (err);
   }

   for (size_t i = 0u; i < bulk->commands.len; i++) {
      if (bulk->server_id) {
         server_stream = mongoc_cluster_stream_for_server (
            cluster, bulk->server_id, true /* reconnect_ok */, bulk->session, reply, error);
      } else {
         server_stream = mongoc_cluster_stream_for_writes (cluster, bulk->session, NULL, reply, error);
      }

      if (!server_stream) {
         /* stream_for_server and stream_for_writes initialize reply on error */
         RETURN (false);
      }

      command = &_mongoc_array_index (&bulk->commands, mongoc_write_command_t, i);

      _mongoc_write_command_execute (command,
                                     bulk->client,
                                     server_stream,
                                     bulk->database,
                                     bulk->collection,
                                     bulk->write_concern,
                                     offset,
                                     bulk->session,
                                     &bulk->result);

      bulk->server_id = server_stream->sd->id;
      /* If a retryable error occurred and a new primary was selected, use it in
       * subsequent commands. */
      if (bulk->result.retry_server_id) {
         bulk->server_id = bulk->result.retry_server_id;
      }

      if (bulk->result.failed && (bulk->flags.ordered || bulk->result.must_stop)) {
         mongoc_server_stream_cleanup (server_stream);
         GOTO (cleanup);
      }

      offset += command->n_documents;
      mongoc_server_stream_cleanup (server_stream);
   }

cleanup:
   _mongoc_bson_init_if_set (reply);
   ret = MONGOC_WRITE_RESULT_COMPLETE (&bulk->result,
                                       bulk->client->error_api_version,
                                       bulk->write_concern,
                                       MONGOC_ERROR_COMMAND /* err domain */,
                                       reply,
                                       error);

   RETURN (ret ? bulk->server_id : 0);

err:
   _mongoc_bson_init_if_set (reply);
   RETURN (false);
}

void
mongoc_bulk_operation_set_write_concern (mongoc_bulk_operation_t *bulk, const mongoc_write_concern_t *write_concern)
{
   BSON_ASSERT_PARAM (bulk);

   if (bulk->write_concern) {
      mongoc_write_concern_destroy (bulk->write_concern);
   }

   if (write_concern) {
      bulk->write_concern = mongoc_write_concern_copy (write_concern);
   } else {
      bulk->write_concern = mongoc_write_concern_new ();
   }
}

const mongoc_write_concern_t *
mongoc_bulk_operation_get_write_concern (const mongoc_bulk_operation_t *bulk)
{
   BSON_ASSERT_PARAM (bulk);

   return bulk->write_concern;
}


void
mongoc_bulk_operation_set_database (mongoc_bulk_operation_t *bulk, const char *database)
{
   BSON_ASSERT_PARAM (bulk);

   if (bulk->database) {
      bson_free (bulk->database);
   }

   bulk->database = bson_strdup (database);
}


void
mongoc_bulk_operation_set_collection (mongoc_bulk_operation_t *bulk, const char *collection)
{
   BSON_ASSERT_PARAM (bulk);

   if (bulk->collection) {
      bson_free (bulk->collection);
   }

   bulk->collection = bson_strdup (collection);
}


void
mongoc_bulk_operation_set_client (mongoc_bulk_operation_t *bulk, void *client)
{
   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (client);

   if (bulk->session) {
      BSON_ASSERT (bulk->session->client == client);
   }

   bulk->client = (mongoc_client_t *) client;

   /* if you call set_client, bulk was likely made by mongoc_bulk_operation_new,
    * not mongoc_collection_create_bulk_operation_with_opts(), so operation_id
    * is 0. */
   if (!bulk->operation_id) {
      bulk->operation_id = ++bulk->client->cluster.operation_id;
   }
}


void
mongoc_bulk_operation_set_client_session (mongoc_bulk_operation_t *bulk,
                                          struct _mongoc_client_session_t *client_session)
{
   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (client_session);

   if (bulk->client) {
      BSON_ASSERT (bulk->client == client_session->client);
   }

   bulk->session = client_session;
}


uint32_t
mongoc_bulk_operation_get_hint (const mongoc_bulk_operation_t *bulk)
{
   BSON_ASSERT_PARAM (bulk);

   return bulk->server_id;
}


void
mongoc_bulk_operation_set_hint (mongoc_bulk_operation_t *bulk, uint32_t server_id)
{
   BSON_ASSERT_PARAM (bulk);

   bulk->server_id = server_id;
}


void
mongoc_bulk_operation_set_bypass_document_validation (mongoc_bulk_operation_t *bulk, bool bypass)
{
   BSON_ASSERT_PARAM (bulk);

   bulk->flags.bypass_document_validation = bypass;
}


void
mongoc_bulk_operation_set_comment (mongoc_bulk_operation_t *bulk, const bson_value_t *comment)
{
   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (comment);
   BSON_ASSERT (comment->value_type != BSON_TYPE_EOD);

   /* This method cannot be called after appending operations, as the CRUD spec
    * states the option should apply to all commands. Since commands are
    * initialized as operations are added, allowing "comment" to be changed at
    * any time could violate that contract. */
   BSON_ASSERT (bulk->commands.len == 0);

   bson_value_destroy (&bulk->comment);
   bson_value_copy (comment, &bulk->comment);
}


void
mongoc_bulk_operation_set_let (mongoc_bulk_operation_t *bulk, const bson_t *let)
{
   BSON_ASSERT_PARAM (bulk);
   BSON_ASSERT_PARAM (let);

   /* This method cannot be called after appending operations, as the CRUD spec
    * states the option should apply to all commands (excluding insert). Since
    * commands are initialized as operations are added, allowing "let" to be
    * changed at any time could violate that contract. */
   BSON_ASSERT (bulk->commands.len == 0);

   bson_destroy (&bulk->let);
   bson_copy_to (let, &bulk->let);
}
