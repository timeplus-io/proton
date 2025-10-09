/*
 * Copyright 2017 MongoDB, Inc.
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


#include "mongoc-cmd-private.h"
#include "mongoc-read-prefs-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-client-private.h"
#include "mongoc-read-concern-private.h"
#include "mongoc-server-api-private.h"
#include "mongoc-write-concern-private.h"
/* For strcasecmp on Windows */
#include "mongoc-util-private.h"


void
mongoc_cmd_parts_init (mongoc_cmd_parts_t *parts,
                       mongoc_client_t *client,
                       const char *db_name,
                       mongoc_query_flags_t user_query_flags,
                       const bson_t *command_body)
{
   BSON_ASSERT_PARAM (client);

   parts->body = command_body;
   parts->user_query_flags = user_query_flags;
   parts->read_prefs = NULL;
   parts->is_read_command = false;
   parts->is_write_command = false;
   parts->prohibit_lsid = false;
   parts->allow_txn_number = MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_UNKNOWN;
   parts->is_retryable_read = false;
   parts->is_retryable_write = false;
   parts->has_temp_session = false;
   parts->client = client;
   bson_init (&parts->read_concern_document);
   bson_init (&parts->write_concern_document);
   bson_init (&parts->extra);
   bson_init (&parts->assembled_body);

   parts->assembled.db_name = db_name;
   parts->assembled.command = NULL;
   parts->assembled.query_flags = MONGOC_QUERY_NONE;
   parts->assembled.op_msg_is_exhaust = false;
   parts->assembled.payloads_count = 0;
   parts->assembled.session = NULL;
   parts->assembled.is_acknowledged = true;
   parts->assembled.is_txn_finish = false;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cmd_parts_set_session --
 *
 *       Set the client session field.
 *
 * Side effects:
 *       Aborts if the command is assembled or if mongoc_cmd_parts_append_opts
 *       was called before.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_cmd_parts_set_session (mongoc_cmd_parts_t *parts, mongoc_client_session_t *cs)
{
   BSON_ASSERT (parts);
   BSON_ASSERT (!parts->assembled.command);
   BSON_ASSERT (!parts->assembled.session);

   parts->assembled.session = cs;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cmd_parts_append_opts --
 *
 *       Take an iterator over user-supplied options document and append the
 *       options to @parts->command_extra, taking the selected server's max
 *       wire version into account.
 *
 * Return:
 *       True if the options were successfully applied. If any options are
 *       invalid, returns false and fills out @error. In that case @parts is
 *       invalid and must not be used.
 *
 * Side effects:
 *       May partly apply options before returning an error.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cmd_parts_append_opts (mongoc_cmd_parts_t *parts, bson_iter_t *iter, bson_error_t *error)
{
   mongoc_client_session_t *cs = NULL;
   mongoc_write_concern_t *wc;
   uint32_t len;
   const uint8_t *data;
   bson_t read_concern;
   const char *to_append;

   ENTRY;

   /* not yet assembled */
   BSON_ASSERT (!parts->assembled.command);

   while (bson_iter_next (iter)) {
      if (BSON_ITER_IS_KEY (iter, "writeConcern")) {
         wc = _mongoc_write_concern_new_from_iter (iter, error);
         if (!wc) {
            RETURN (false);
         }

         if (!mongoc_cmd_parts_set_write_concern (parts, wc, error)) {
            mongoc_write_concern_destroy (wc);
            RETURN (false);
         }

         mongoc_write_concern_destroy (wc);
         continue;
      } else if (BSON_ITER_IS_KEY (iter, "readConcern")) {
         if (!BSON_ITER_HOLDS_DOCUMENT (iter)) {
            bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION, "Invalid readConcern");
            RETURN (false);
         }

         /* add readConcern later, once we know about causal consistency */
         bson_iter_document (iter, &len, &data);
         BSON_ASSERT (bson_init_static (&read_concern, data, (size_t) len));
         bson_destroy (&parts->read_concern_document);
         bson_copy_to (&read_concern, &parts->read_concern_document);
         continue;
      } else if (BSON_ITER_IS_KEY (iter, "sessionId")) {
         BSON_ASSERT (!parts->assembled.session);

         if (!_mongoc_client_session_from_iter (parts->client, iter, &cs, error)) {
            RETURN (false);
         }

         parts->assembled.session = cs;
         continue;
      } else if (BSON_ITER_IS_KEY (iter, "serverId") || BSON_ITER_IS_KEY (iter, "maxAwaitTimeMS") ||
                 BSON_ITER_IS_KEY (iter, "exhaust")) {
         continue;
      }

      to_append = bson_iter_key (iter);
      if (!bson_append_iter (&parts->extra, to_append, -1, iter)) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Failed to append \"%s\" to create command.",
                         to_append);
         RETURN (false);
      }
   }

   RETURN (true);
}


#define OPTS_ERR(_code, ...)                                                           \
   do {                                                                                \
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_##_code, __VA_ARGS__); \
      RETURN (false);                                                                  \
   } while (0)


/* set readConcern if allowed, otherwise error */
bool
mongoc_cmd_parts_set_read_concern (mongoc_cmd_parts_t *parts, const mongoc_read_concern_t *rc, bson_error_t *error)
{
   const char *command_name;

   ENTRY;

   /* In a txn, set read concern in mongoc_cmd_parts_assemble, not here. *
    * Transactions Spec: "The readConcern MUST NOT be inherited from the
    * collection, database, or client associated with the driver method that
    * invokes the first command." */
   if (_mongoc_client_session_in_txn (parts->assembled.session)) {
      RETURN (true);
   }

   command_name = _mongoc_get_command_name (parts->body);

   if (!command_name) {
      OPTS_ERR (COMMAND_INVALID_ARG, "Empty command document");
   }

   if (mongoc_read_concern_is_default (rc)) {
      RETURN (true);
   }

   bson_destroy (&parts->read_concern_document);
   bson_copy_to (_mongoc_read_concern_get_bson ((mongoc_read_concern_t *) rc), &parts->read_concern_document);

   RETURN (true);
}


/* set writeConcern if allowed, otherwise ignore - unlike set_read_concern, it's
 * the caller's responsibility to check if writeConcern is supported */
bool
mongoc_cmd_parts_set_write_concern (mongoc_cmd_parts_t *parts, const mongoc_write_concern_t *wc, bson_error_t *error)
{
   ENTRY;

   if (!wc) {
      RETURN (true);
   }

   const char *const command_name = _mongoc_get_command_name (parts->body);

   if (!command_name) {
      OPTS_ERR (COMMAND_INVALID_ARG, "Empty command document");
   }

   parts->assembled.is_acknowledged = mongoc_write_concern_is_acknowledged (wc);
   bson_destroy (&parts->write_concern_document);
   bson_copy_to (_mongoc_write_concern_get_bson ((mongoc_write_concern_t *) wc), &parts->write_concern_document);

   RETURN (true);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cmd_parts_append_read_write --
 *
 *       Append user-supplied options to @parts->command_extra, taking the
 *       selected server's max wire version into account.
 *
 * Return:
 *       True if the options were successfully applied. If any options are
 *       invalid, returns false and fills out @error. In that case @parts is
 *       invalid and must not be used.
 *
 * Side effects:
 *       May partly apply options before returning an error.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cmd_parts_append_read_write (mongoc_cmd_parts_t *parts, mongoc_read_write_opts_t *rw_opts, bson_error_t *error)
{
   ENTRY;

   /* not yet assembled */
   BSON_ASSERT (!parts->assembled.command);

   if (!bson_empty (&rw_opts->collation)) {
      if (!bson_append_document (&parts->extra, "collation", 9, &rw_opts->collation)) {
         OPTS_ERR (COMMAND_INVALID_ARG, "'opts' with 'collation' is too large");
      }
   }

   if (!mongoc_cmd_parts_set_write_concern (parts, rw_opts->writeConcern, error)) {
      RETURN (false);
   }

   /* process explicit read concern */
   if (!bson_empty (&rw_opts->readConcern)) {
      /* save readConcern for later, once we know about causal consistency */
      bson_destroy (&parts->read_concern_document);
      bson_copy_to (&rw_opts->readConcern, &parts->read_concern_document);
   }

   if (rw_opts->client_session) {
      BSON_ASSERT (!parts->assembled.session);
      parts->assembled.session = rw_opts->client_session;
   }

   if (!bson_concat (&parts->extra, &rw_opts->extra)) {
      OPTS_ERR (COMMAND_INVALID_ARG, "'opts' with extra fields is too large");
   }

   RETURN (true);
}

#undef OPTS_ERR

static void
_mongoc_cmd_parts_ensure_copied (mongoc_cmd_parts_t *parts)
{
   if (parts->assembled.command == parts->body) {
      bson_concat (&parts->assembled_body, parts->body);
      bson_concat (&parts->assembled_body, &parts->extra);
      parts->assembled.command = &parts->assembled_body;
   }
}


static void
_mongoc_cmd_parts_add_write_concern (mongoc_cmd_parts_t *parts)
{
   if (!bson_empty (&parts->write_concern_document)) {
      _mongoc_cmd_parts_ensure_copied (parts);
      bson_append_document (&parts->assembled_body, "writeConcern", 12, &parts->write_concern_document);
   }
}


/* The server type must be mongos, or message must be OP_MSG. */
static void
_mongoc_cmd_parts_add_read_prefs (bson_t *query, const mongoc_read_prefs_t *prefs)
{
   bson_t child;
   const char *mode_str;
   const bson_t *tags;
   int64_t stale;
   const bson_t *hedge;

   mode_str = _mongoc_read_mode_as_str (mongoc_read_prefs_get_mode (prefs));
   tags = mongoc_read_prefs_get_tags (prefs);
   stale = mongoc_read_prefs_get_max_staleness_seconds (prefs);
   hedge = mongoc_read_prefs_get_hedge (prefs);

   bson_append_document_begin (query, "$readPreference", 15, &child);
   bson_append_utf8 (&child, "mode", 4, mode_str, -1);
   if (!bson_empty0 (tags)) {
      bson_append_array (&child, "tags", 4, tags);
   }

   if (stale != MONGOC_NO_MAX_STALENESS) {
      bson_append_int64 (&child, "maxStalenessSeconds", 19, stale);
   }

   if (!bson_empty0 (hedge)) {
      bson_append_document (&child, "hedge", 5, hedge);
   }

   bson_append_document_end (query, &child);
}


static void
_iter_concat (bson_t *dst, bson_iter_t *iter)
{
   uint32_t len;
   const uint8_t *data;
   bson_t src;

   bson_iter_document (iter, &len, &data);
   BSON_ASSERT (bson_init_static (&src, data, len));
   BSON_ASSERT (bson_concat (dst, &src));
}


/* Update result with the read prefs. Server must be mongos.
 */
static void
_mongoc_cmd_parts_assemble_mongos (mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream)
{
   mongoc_read_mode_t mode;
   const bson_t *tags = NULL;
   int64_t max_staleness_seconds = MONGOC_NO_MAX_STALENESS;
   const bson_t *hedge = NULL;
   bool add_read_prefs = false;
   bson_t query;
   bson_iter_t dollar_query;
   bool has_dollar_query = false;
   bool requires_read_concern;
   bool requires_write_concern;

   ENTRY;

   mode = mongoc_read_prefs_get_mode (parts->read_prefs);
   if (parts->read_prefs) {
      max_staleness_seconds = mongoc_read_prefs_get_max_staleness_seconds (parts->read_prefs);

      tags = mongoc_read_prefs_get_tags (parts->read_prefs);
      hedge = mongoc_read_prefs_get_hedge (parts->read_prefs);
   }

   if (server_stream->must_use_primary) {
      /* Server selection has overriden the read mode used to generate this
       * server stream. This has effects on the body of the message that we send
       * to the server */
      mode = MONGOC_READ_PRIMARY;
   }

   /* Server Selection Spec says:
    *
    * For mode 'primary', drivers MUST NOT set the secondaryOk wire protocol
    * flag and MUST NOT use $readPreference
    *
    * For mode 'secondary', drivers MUST set the secondaryOk wire protocol flag
    * and MUST also use $readPreference
    *
    * For mode 'primaryPreferred', drivers MUST set the secondaryOk wire
    * protocol flag and MUST also use $readPreference
    *
    * For mode 'secondaryPreferred', drivers MUST set the secondaryOk wire
    * protocol flag. If the read preference contains a non-empty tag_sets
    * parameter, maxStalenessSeconds is a positive integer, or the hedge
    * parameter is non-empty, drivers MUST use $readPreference; otherwise,
    * drivers MUST NOT use $readPreference
    *
    * For mode 'nearest', drivers MUST set the secondaryOk wire protocol flag
    * and MUST also use $readPreference
    */
   switch (mode) {
   case MONGOC_READ_PRIMARY:
      break;
   case MONGOC_READ_SECONDARY_PREFERRED:
      if (!bson_empty0 (tags) || max_staleness_seconds > 0 || !bson_empty0 (hedge)) {
         add_read_prefs = true;
      }
      parts->assembled.query_flags |= MONGOC_QUERY_SECONDARY_OK;
      break;
   case MONGOC_READ_PRIMARY_PREFERRED:
   case MONGOC_READ_SECONDARY:
   case MONGOC_READ_NEAREST:
   default:
      parts->assembled.query_flags |= MONGOC_QUERY_SECONDARY_OK;
      add_read_prefs = true;
   }

   requires_read_concern =
      !bson_empty (&parts->read_concern_document) && strcmp (parts->assembled.command_name, "getMore") != 0;

   requires_write_concern = !bson_empty (&parts->write_concern_document);

   if (add_read_prefs) {
      /* produce {$query: {user query, readConcern}, $readPreference: ... } */
      bson_append_document_begin (&parts->assembled_body, "$query", 6, &query);

      if (bson_iter_init_find (&dollar_query, parts->body, "$query")) {
         /* user provided something like {$query: {key: "x"}} */
         has_dollar_query = true;
         _iter_concat (&query, &dollar_query);
      } else {
         bson_concat (&query, parts->body);
      }

      bson_concat (&query, &parts->extra);
      if (requires_read_concern) {
         bson_append_document (&query, "readConcern", 11, &parts->read_concern_document);
      }

      if (requires_write_concern) {
         bson_append_document (&query, "writeConcern", 12, &parts->write_concern_document);
      }

      bson_append_document_end (&parts->assembled_body, &query);
      _mongoc_cmd_parts_add_read_prefs (&parts->assembled_body, parts->read_prefs);

      if (has_dollar_query) {
         /* copy anything that isn't in user's $query */
         bson_copy_to_excluding_noinit (parts->body, &parts->assembled_body, "$query", NULL);
      }

      parts->assembled.command = &parts->assembled_body;
   } else if (bson_iter_init_find (&dollar_query, parts->body, "$query")) {
      /* user provided $query, we have no read prefs */
      bson_append_document_begin (&parts->assembled_body, "$query", 6, &query);
      _iter_concat (&query, &dollar_query);
      bson_concat (&query, &parts->extra);
      if (requires_read_concern) {
         bson_append_document (&query, "readConcern", 11, &parts->read_concern_document);
      }

      if (requires_write_concern) {
         bson_append_document (&query, "writeConcern", 12, &parts->write_concern_document);
      }

      bson_append_document_end (&parts->assembled_body, &query);
      /* copy anything that isn't in user's $query */
      bson_copy_to_excluding_noinit (parts->body, &parts->assembled_body, "$query", NULL);

      parts->assembled.command = &parts->assembled_body;
   } else {
      if (requires_read_concern) {
         _mongoc_cmd_parts_ensure_copied (parts);
         bson_append_document (&parts->assembled_body, "readConcern", 11, &parts->read_concern_document);
      }

      _mongoc_cmd_parts_add_write_concern (parts);
   }

   if (!bson_empty (&parts->extra)) {
      /* if none of the above logic has merged "extra", do it now */
      _mongoc_cmd_parts_ensure_copied (parts);
   }

   EXIT;
}


static void
_mongoc_cmd_parts_assemble_mongod (mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream)
{
   ENTRY;

   if (!parts->is_write_command) {
      switch (server_stream->topology_type) {
      case MONGOC_TOPOLOGY_SINGLE:
         /* Server Selection Spec: for topology type single and server types
          * besides mongos, "clients MUST always set the secondaryOk wire
          * protocol flag on reads to ensure that any server type can handle
          * the request."
          */
         parts->assembled.query_flags |= MONGOC_QUERY_SECONDARY_OK;
         break;

      case MONGOC_TOPOLOGY_RS_NO_PRIMARY:
      case MONGOC_TOPOLOGY_RS_WITH_PRIMARY:
         /* Server Selection Spec: for RS topology types, "For all read
          * preferences modes except primary, clients MUST set the secondaryOk
          * wire protocol flag to ensure that any suitable server can handle the
          * request. Clients MUST  NOT set the secondaryOk wire protocol flag if
          * the read preference mode is primary.
          */
         if (parts->read_prefs && parts->read_prefs->mode != MONGOC_READ_PRIMARY) {
            parts->assembled.query_flags |= MONGOC_QUERY_SECONDARY_OK;
         }

         break;
      case MONGOC_TOPOLOGY_SHARDED:
      case MONGOC_TOPOLOGY_UNKNOWN:
      case MONGOC_TOPOLOGY_LOAD_BALANCED:
      case MONGOC_TOPOLOGY_DESCRIPTION_TYPES:
      default:
         /* must not call this function w/ sharded, load balanced, or unknown
          * topology type */
         BSON_ASSERT (false);
      }
   } /* if (!parts->is_write_command) */

   if (!bson_empty (&parts->extra)) {
      _mongoc_cmd_parts_ensure_copied (parts);
   }

   if (!bson_empty (&parts->read_concern_document) && strcmp (parts->assembled.command_name, "getMore") != 0) {
      _mongoc_cmd_parts_ensure_copied (parts);
      bson_append_document (&parts->assembled_body, "readConcern", 11, &parts->read_concern_document);
   }

   _mongoc_cmd_parts_add_write_concern (parts);

   EXIT;
}


static const bson_t *
_largest_cluster_time (const bson_t *a, const bson_t *b)
{
   if (!a) {
      return b;
   }

   if (!b) {
      return a;
   }

   if (_mongoc_cluster_time_greater (a, b)) {
      return a;
   }

   return b;
}


/* Check if the command should allow a transaction number if that has not
 * already been determined.
 *
 * This should only return true for write commands that are always retryable for
 * the server stream's wire version.
 *
 * The basic write commands (i.e. insert, update, delete) are intentionally
 * excluded here. While insert is always retryable, update and delete are only
 * retryable if they include no multi-document writes. Since it would be costly
 * to inspect the command document here, the bulk operation API explicitly sets
 * allow_txn_number for us. This means that insert, update, and delete are not
 * retryable if executed via mongoc_client_write_command_with_opts(); however,
 * documentation already instructs users not to use that for basic writes.
 */
static bool
_allow_txn_number (const mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream)
{
   /* There is no reason to call this function if allow_txn_number is set */
   BSON_ASSERT (parts->allow_txn_number == MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_UNKNOWN);

   if (!parts->is_write_command) {
      return false;
   }

   if (server_stream->retry_attempted) {
      return false;
   }

   if (!parts->assembled.is_acknowledged) {
      return false;
   }

   if (!strcasecmp (parts->assembled.command_name, "findandmodify")) {
      return true;
   }

   return false;
}


/* Check if the write command should support retryable behavior. */
static bool
_is_retryable_write (const mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream)
{
   if (!parts->assembled.session) {
      return false;
   }

   if (!parts->is_write_command) {
      return false;
   }

   if (parts->allow_txn_number != MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_YES) {
      return false;
   }

   if (server_stream->retry_attempted) {
      return false;
   }

   if (server_stream->sd->type == MONGOC_SERVER_STANDALONE) {
      return false;
   }

   if (_mongoc_client_session_in_txn (parts->assembled.session)) {
      return false;
   }

   if (!mongoc_uri_get_option_as_bool (parts->client->uri, MONGOC_URI_RETRYWRITES, MONGOC_DEFAULT_RETRYWRITES)) {
      return false;
   }

   return true;
}


/* Check if the read command should support retryable behavior. */
bool
_is_retryable_read (const mongoc_cmd_parts_t *parts, const mongoc_server_stream_t *server_stream)
{
   if (!parts->is_read_command) {
      return false;
   }

   /* Commands that go through read_write_command helpers are also write
    * commands. Prohibit from read retry. */
   if (parts->is_write_command) {
      return false;
   }

   if (server_stream->retry_attempted) {
      return false;
   }

   if (_mongoc_client_session_in_txn (parts->assembled.session)) {
      return false;
   }

   if (!mongoc_uri_get_option_as_bool (parts->client->uri, MONGOC_URI_RETRYREADS, MONGOC_DEFAULT_RETRYREADS)) {
      return false;
   }

   return true;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cmd_parts_assemble --
 *
 *       Assemble the command body, options, and read preference into one
 *       command.
 *
 * Return:
 *       True if the options were successfully applied. If any options are
 *       invalid, returns false and fills out @error. In that case @parts is
 *       invalid and must not be used.
 *
 * Side effects:
 *       May partly assemble before returning an error.
 *       mongoc_cmd_parts_cleanup should be called in all cases.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cmd_parts_assemble (mongoc_cmd_parts_t *parts, mongoc_server_stream_t *server_stream, bson_error_t *error)
{
   mongoc_server_description_type_t server_type;
   mongoc_client_session_t *cs;
   const bson_t *cluster_time = NULL;
   mongoc_read_prefs_t *prefs = NULL;
   const char *cmd_name;
   bool is_get_more;
   const mongoc_read_prefs_t *prefs_ptr;
   mongoc_read_mode_t mode;
   bool ret = false;

   ENTRY;

   BSON_ASSERT (parts);
   BSON_ASSERT (server_stream);

   server_type = server_stream->sd->type;

   cs = parts->prohibit_lsid ? NULL : parts->assembled.session;

   /* Assembling the command depends on the type of server. If the server has
    * been invalidated, error. */
   if (server_type == MONGOC_SERVER_UNKNOWN) {
      if (error) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Cannot assemble command for invalidated server: %s",
                         server_stream->sd->error.message);
      }
      RETURN (false);
   }

   /* must not be assembled already */
   BSON_ASSERT (!parts->assembled.command);
   BSON_ASSERT (bson_empty (&parts->assembled_body));

   /* begin with raw flags/cmd as assembled flags/cmd, might change below */
   parts->assembled.command = parts->body;
   /* unused in OP_MSG: */
   parts->assembled.query_flags = parts->user_query_flags;
   parts->assembled.server_stream = server_stream;
   cmd_name = parts->assembled.command_name = _mongoc_get_command_name (parts->assembled.command);

   if (!parts->assembled.command_name) {
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Empty command document");
      GOTO (done);
   }

   TRACE ("Preparing '%s'", cmd_name);

   is_get_more = !strcmp (cmd_name, "getMore");
   parts->assembled.is_txn_finish = !strcmp (cmd_name, "commitTransaction") || !strcmp (cmd_name, "abortTransaction");

   if (!parts->is_write_command && IS_PREF_PRIMARY (parts->read_prefs) &&
       server_stream->topology_type == MONGOC_TOPOLOGY_SINGLE && server_type != MONGOC_SERVER_MONGOS) {
      prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY_PREFERRED);
      prefs_ptr = prefs;
   } else {
      prefs_ptr = parts->read_prefs;
   }

   mode = mongoc_read_prefs_get_mode (prefs_ptr);
   if (server_stream->must_use_primary) {
      /* Server selection may have overriden the read mode used to generate this
       * server stream. This has effects on the body of the message that we send
       * to the server */
      mode = MONGOC_READ_PRIMARY;
   }

   if (mongoc_client_uses_server_api (parts->client) || mongoc_client_uses_loadbalanced (parts->client) ||
       server_stream->sd->max_wire_version >= WIRE_VERSION_MIN) {
      if (!bson_has_field (parts->body, "$db")) {
         BSON_APPEND_UTF8 (&parts->extra, "$db", parts->assembled.db_name);
      }

      if (cs && _mongoc_client_session_in_txn (cs)) {
         if (!IS_PREF_PRIMARY (cs->txn.opts.read_prefs) && !parts->is_write_command) {
            bson_set_error (error,
                            MONGOC_ERROR_TRANSACTION,
                            MONGOC_ERROR_TRANSACTION_INVALID_STATE,
                            "Read preference in a transaction must be primary");
            GOTO (done);
         }
      } else if (mode != MONGOC_READ_PRIMARY && server_type != MONGOC_SERVER_STANDALONE) {
         /* "Type Standalone: clients MUST NOT send the read preference to the
          * server" */
         _mongoc_cmd_parts_add_read_prefs (&parts->extra, prefs_ptr);
      }

      if (!bson_empty (&parts->extra)) {
         _mongoc_cmd_parts_ensure_copied (parts);
      }

      /* If an explicit session was not provided and lsid is not prohibited,
       * attempt to create an implicit session (ignoring any errors). */
      if (!cs && !parts->prohibit_lsid && parts->assembled.is_acknowledged) {
         cs = mongoc_client_start_session (parts->client, NULL, NULL);

         if (cs) {
            parts->assembled.session = cs;
            parts->has_temp_session = true;
         }
      }

      /* Driver Sessions Spec: "For unacknowledged writes with an explicit
       * session, drivers SHOULD raise an error.... Without an explicit
       * session, drivers SHOULD NOT use an implicit session." We intentionally
       * do not restrict this logic to parts->is_write_command, since
       * mongoc_client_command_with_opts() does not identify as a write
       * command but may still include a write concern.
       */
      if (cs) {
         if (!parts->assembled.is_acknowledged) {
            bson_set_error (error,
                            MONGOC_ERROR_COMMAND,
                            MONGOC_ERROR_COMMAND_INVALID_ARG,
                            "Cannot use client session with unacknowledged command");
            GOTO (done);
         }

         _mongoc_cmd_parts_ensure_copied (parts);
         bson_append_document (&parts->assembled_body, "lsid", 4, mongoc_client_session_get_lsid (cs));

         cs->server_session->last_used_usec = bson_get_monotonic_time ();
         cluster_time = mongoc_client_session_get_cluster_time (cs);
      }

      /* Ensure we know if the write command allows a transaction number */
      if (!_mongoc_client_session_txn_in_progress (cs) && parts->is_write_command &&
          parts->allow_txn_number == MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_UNKNOWN) {
         parts->allow_txn_number = _allow_txn_number (parts, server_stream) ? MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_YES
                                                                            : MONGOC_CMD_PARTS_ALLOW_TXN_NUMBER_NO;
      }

      /* Determine if the command is retryable. If so, append txnNumber now
       * for future use and mark the command as such. */
      if (_is_retryable_write (parts, server_stream)) {
         _mongoc_cmd_parts_ensure_copied (parts);
         bson_append_int64 (&parts->assembled_body, "txnNumber", 9, 0);
         parts->is_retryable_write = true;
      }

      /* Conversely, check if the command is retryable if it is a read. */
      if (_is_retryable_read (parts, server_stream) && !is_get_more) {
         parts->is_retryable_read = true;
      }

      if (!bson_empty (&server_stream->cluster_time)) {
         cluster_time = _largest_cluster_time (&server_stream->cluster_time, cluster_time);
      }

      if (cluster_time && server_type != MONGOC_SERVER_STANDALONE) {
         _mongoc_cmd_parts_ensure_copied (parts);
         bson_append_document (&parts->assembled_body, "$clusterTime", 12, cluster_time);
      }

      /* Add versioned server api, if it is set. */
      if (mongoc_client_uses_server_api (parts->client)) {
         _mongoc_cmd_append_server_api (&parts->assembled_body, parts->client->api);
      }

      if (!is_get_more) {
         if (cs) {
            /* Snapshot Sessions Spec: "Snapshot reads require MongoDB 5.0+."
             * Throw an error if snapshot is enabled and wire version is less
             * than 13 before potentially appending "snapshot" read concern. */
            if (mongoc_session_opts_get_snapshot (&cs->opts) &&
                server_stream->sd->max_wire_version < WIRE_VERSION_SNAPSHOT_READS) {
               bson_set_error (error,
                               MONGOC_ERROR_CLIENT,
                               MONGOC_ERROR_CLIENT_SESSION_FAILURE,
                               "Snapshot reads require MongoDB 5.0 or later");
               GOTO (done);
            }

            _mongoc_cmd_parts_ensure_copied (parts);
            _mongoc_client_session_append_read_concern (
               cs, &parts->read_concern_document, parts->is_read_command, &parts->assembled_body);
         } else if (!bson_empty (&parts->read_concern_document)) {
            _mongoc_cmd_parts_ensure_copied (parts);
            bson_append_document (&parts->assembled_body, "readConcern", 11, &parts->read_concern_document);
         }
      }

      /* if transaction is in progress do not inherit write concern */
      if (parts->assembled.is_txn_finish || !_mongoc_client_session_in_txn (cs)) {
         _mongoc_cmd_parts_add_write_concern (parts);
      }

      _mongoc_cmd_parts_ensure_copied (parts);
      if (!_mongoc_client_session_append_txn (cs, &parts->assembled_body, error)) {
         GOTO (done);
      }

      ret = true;
   } else if (server_type == MONGOC_SERVER_MONGOS || server_stream->topology_type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      /* TODO (CDRIVER-4117) remove the check of the topology description type.
       */
      _mongoc_cmd_parts_assemble_mongos (parts, server_stream);
      ret = true;
   } else {
      _mongoc_cmd_parts_assemble_mongod (parts, server_stream);
      ret = true;
   }

done:
   mongoc_read_prefs_destroy (prefs);
   RETURN (ret);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cmd_parts_cleanup --
 *
 *       Free memory associated with a stack-allocated mongoc_cmd_parts_t.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_cmd_parts_cleanup (mongoc_cmd_parts_t *parts)
{
   bson_destroy (&parts->read_concern_document);
   bson_destroy (&parts->write_concern_document);
   bson_destroy (&parts->extra);
   bson_destroy (&parts->assembled_body);

   if (parts->has_temp_session) {
      /* client session returns its server session to server session pool */
      mongoc_client_session_destroy (parts->assembled.session);
   }
}

bool
mongoc_cmd_is_compressible (const mongoc_cmd_t *cmd)
{
   BSON_ASSERT (cmd);
   BSON_ASSERT (cmd->command_name);

   return !!strcasecmp (cmd->command_name, "hello") && !!strcasecmp (cmd->command_name, HANDSHAKE_CMD_LEGACY_HELLO) &&
          !!strcasecmp (cmd->command_name, "authenticate") && !!strcasecmp (cmd->command_name, "getnonce") &&
          !!strcasecmp (cmd->command_name, "saslstart") && !!strcasecmp (cmd->command_name, "saslcontinue") &&
          !!strcasecmp (cmd->command_name, "createuser") && !!strcasecmp (cmd->command_name, "updateuser");
}


//`_mongoc_cmd_append_payload_as_array` appends document seqence payloads as BSON arrays.
// `cmd` must contain one or more document sequence payloads (`cmd->payloads_count` > 0).
// `out` must be initialized by the caller.
// Used by APM and In-Use Encryption (document sequences are not supported for auto encryption).
void
_mongoc_cmd_append_payload_as_array (const mongoc_cmd_t *cmd, bson_t *out)
{
   int32_t doc_len;
   bson_t doc;
   const uint8_t *pos;
   const char *field_name;
   bson_array_builder_t *bson;

   BSON_ASSERT (cmd->payloads_count > 0);
   BSON_ASSERT (cmd->payloads_count <= MONGOC_CMD_PAYLOADS_COUNT_MAX);

   for (size_t i = 0; i < cmd->payloads_count; i++) {
      BSON_ASSERT (cmd->payloads[i].documents && cmd->payloads[i].size);

      // Create a BSON array from a document sequence (OP_MSG Section with payloadType=1).
      field_name = cmd->payloads[i].identifier;
      BSON_ASSERT (field_name);
      BSON_ASSERT (BSON_APPEND_ARRAY_BUILDER_BEGIN (out, field_name, &bson));

      pos = cmd->payloads[i].documents;
      while (pos < cmd->payloads[i].documents + cmd->payloads[i].size) {
         memcpy (&doc_len, pos, sizeof (doc_len));
         doc_len = BSON_UINT32_FROM_LE (doc_len);
         BSON_ASSERT (bson_init_static (&doc, pos, (size_t) doc_len));
         bson_array_builder_append_document (bson, &doc);

         pos += doc_len;
      }
      bson_append_array_builder_end (out, bson);
   }
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_cmd_append_server_api --
 *    Append versioned API fields to a mongoc_cmd_t
 *
 * Arguments:
 *    cmd The mongoc_cmd_t, which will have versioned API fields added
 *    api A mongoc_server_api_t holding server API information
 *
 * Pre-conditions:
 *    - @api is initialized.
 *    - @command_body is initialised
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_cmd_append_server_api (bson_t *command_body, const mongoc_server_api_t *api)
{
   const char *string_version;

   BSON_ASSERT (command_body);
   BSON_ASSERT (api);

   string_version = mongoc_server_api_version_to_string (api->version);

   BSON_ASSERT (string_version);

   bson_append_utf8 (command_body, "apiVersion", -1, string_version, -1);

   if (api->strict.is_set) {
      bson_append_bool (command_body, "apiStrict", -1, api->strict.value);
   }

   if (api->deprecation_errors.is_set) {
      bson_append_bool (command_body, "apiDeprecationErrors", -1, api->deprecation_errors.value);
   }
}
