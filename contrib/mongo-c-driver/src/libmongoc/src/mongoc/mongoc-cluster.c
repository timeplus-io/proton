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


#include "mongoc-config.h"

#include <string.h>

#include "mcd-rpc.h"
#include "mongoc-cluster-private.h"
#include "mongoc-client-private.h"
#include "mongoc-client-side-encryption-private.h"
#include "mongoc-counters-private.h"
#include "mongoc-config.h"
#include "mongoc-error.h"
#include "mongoc-flags-private.h"
#include "mongoc-host-list-private.h"
#include "mongoc-log.h"
#include "mongoc-cluster-sasl-private.h"
#ifdef MONGOC_ENABLE_SSL
#include "mongoc-ssl.h"
#include "mongoc-ssl-private.h"
#include "mongoc-stream-tls.h"
#endif
#include "common-b64-private.h"
#include "mongoc-scram-private.h"
#include "mongoc-set-private.h"
#include "mongoc-socket.h"
#include "mongoc-stream-private.h"
#include "mongoc-stream-socket.h"
#include "mongoc-stream-tls.h"
#include "mongoc-thread-private.h"
#include "mongoc-topology-private.h"
#include "mongoc-topology-background-monitoring-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-util-private.h"
#include "mongoc-write-concern-private.h"
#include "mongoc-uri-private.h"
#include "mongoc-rpc-private.h"
#include "mongoc-compression-private.h"
#include "mongoc-cmd-private.h"
#include "utlist.h"
#include "mongoc-handshake-private.h"
#include "mongoc-cluster-aws-private.h"
#include "mongoc-error-private.h"

#include <bson-dsl.h>

#include <inttypes.h>

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "cluster"


#define CHECK_CLOSED_DURATION_MSEC 1000

#define IS_NOT_COMMAND(_name) (!!strcasecmp (cmd->command_name, _name))

static mongoc_server_stream_t *
_cluster_fetch_stream_single (mongoc_cluster_t *cluster,
                              const mongoc_topology_description_t *td,
                              uint32_t server_id,
                              bool reconnect_ok,
                              bson_error_t *error);

static mongoc_server_stream_t *
_cluster_fetch_stream_pooled (mongoc_cluster_t *cluster,
                              const mongoc_topology_description_t *td,
                              uint32_t server_id,
                              bool reconnect_ok,
                              bson_error_t *error);

static bool
mongoc_cluster_run_opmsg (mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, bson_t *reply, bson_error_t *error);

static void
_bson_error_message_printf (bson_error_t *error, const char *format, ...) BSON_GNUC_PRINTF (2, 3);

static void
_handle_not_primary_error (mongoc_cluster_t *cluster, const mongoc_server_stream_t *server_stream, const bson_t *reply)
{
   uint32_t server_id;

   server_id = server_stream->sd->id;
   if (_mongoc_topology_handle_app_error (cluster->client->topology,
                                          server_id,
                                          true /* handshake complete */,
                                          MONGOC_SDAM_APP_ERROR_COMMAND,
                                          reply,
                                          NULL,
                                          server_stream->sd->max_wire_version,
                                          server_stream->sd->generation,
                                          &server_stream->sd->service_id)) {
      mongoc_cluster_disconnect_node (cluster, server_id);
   }
}

/* Called when a network error occurs on an application socket.
 */
static void
_handle_network_error (mongoc_cluster_t *cluster, mongoc_server_stream_t *server_stream, const bson_error_t *why)
{
   mongoc_topology_t *topology;
   uint32_t server_id;
   _mongoc_sdam_app_error_type_t type;

   BSON_ASSERT (server_stream);

   ENTRY;
   topology = cluster->client->topology;
   server_id = server_stream->sd->id;
   type = MONGOC_SDAM_APP_ERROR_NETWORK;
   if (mongoc_stream_timed_out (server_stream->stream)) {
      type = MONGOC_SDAM_APP_ERROR_TIMEOUT;
   }

   _mongoc_topology_handle_app_error (topology,
                                      server_id,
                                      true, // handshake_complete
                                      type,
                                      NULL,
                                      why,
                                      server_stream->sd->max_wire_version,
                                      server_stream->sd->generation,
                                      &server_stream->sd->service_id);
   /* Always disconnect the current connection on network error. */
   mongoc_cluster_disconnect_node (cluster, server_id);

   EXIT;
}


static int32_t
_int32_from_le (const void *data)
{
   BSON_ASSERT_PARAM (data);
   return bson_iter_int32_unsafe (&(bson_iter_t){.raw = data});
}


static int32_t
_compression_level_from_uri (int32_t compressor_id, const mongoc_uri_t *uri)
{
   if (compressor_id == MONGOC_COMPRESSOR_ZLIB_ID) {
      return mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, -1);
   }

   return -1;
}


size_t
_mongoc_cluster_buffer_iovec (mongoc_iovec_t *iov, size_t iovcnt, int skip, char *buffer)
{
   size_t buffer_offset = 0;
   int total_iov_len = 0;
   size_t difference = 0;

   for (size_t n = 0u; n < iovcnt; n++) {
      BSON_ASSERT (bson_in_range_unsigned (int, iov[n].iov_len));
      const int iov_len = (int) iov[n].iov_len;

      total_iov_len += iov_len;

      if (total_iov_len <= skip) {
         continue;
      }

      /* If this iovec starts before the skip, and takes the total count
       * beyond the skip, we need to figure out the portion of the iovec
       * we should skip passed */
      const int remaining = total_iov_len - iov_len;
      if (remaining < skip) {
         difference = (size_t) (skip - remaining);
      } else {
         difference = 0u;
      }

      memcpy (buffer + buffer_offset, ((char *) iov[n].iov_base) + difference, iov[n].iov_len - difference);
      buffer_offset += iov[n].iov_len - difference;
   }

   return buffer_offset;
}

/* Allows caller to safely overwrite error->message with a formatted string,
 * even if the formatted string includes original error->message. */
static void
_bson_error_message_printf (bson_error_t *error, const char *format, ...)
{
   va_list args;
   char error_message[sizeof error->message];

   if (error) {
      va_start (args, format);
      bson_vsnprintf (error_message, sizeof error->message, format, args);
      va_end (args);

      bson_strncpy (error->message, error_message, sizeof error->message);
   }
}

#define RUN_CMD_ERR_DECORATE                                                                \
   do {                                                                                     \
      _bson_error_message_printf (error,                                                    \
                                  "Failed to send \"%s\" command with database \"%s\": %s", \
                                  cmd->command_name,                                        \
                                  cmd->db_name,                                             \
                                  error->message);                                          \
   } while (0)

#define RUN_CMD_ERR(_domain, _code, ...)                   \
   do {                                                    \
      bson_set_error (error, _domain, _code, __VA_ARGS__); \
      RUN_CMD_ERR_DECORATE;                                \
   } while (0)


// msgHeader consists of four int32 fields.
static const int32_t message_header_length = 4u * sizeof (int32_t);


static bool
_mongoc_cluster_run_command_opquery_send (
   mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, int32_t compressor_id, mcd_rpc_message *rpc, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (error);

   bool ret = false;

   mongoc_stream_t *const stream = cmd->server_stream->stream;

   char *const ns = bson_strdup_printf ("%s.$cmd", cmd->db_name);
   const int32_t request_id = ++cluster->request_id;

   // Find, getMore And killCursors Commands Spec: "When sending a find command
   // rather than a legacy OP_QUERY find, only the secondaryOk flag is honored."
   // For other cursor-typed commands like aggregate, only secondaryOk can be
   // set. Clear bits except secondaryOk; leave secondaryOk set only if it is
   // already.
   const int32_t flags = (int32_t) cmd->query_flags & MONGOC_OP_QUERY_FLAG_SECONDARY_OK;

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, request_id);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_QUERY);

      message_length += mcd_rpc_op_query_set_flags (rpc, flags);
      message_length += mcd_rpc_op_query_set_full_collection_name (rpc, ns);
      message_length += mcd_rpc_op_query_set_number_to_skip (rpc, 0);
      message_length += mcd_rpc_op_query_set_number_to_return (rpc, -1);
      message_length += mcd_rpc_op_query_set_query (rpc, bson_get_data (cmd->command));

      mcd_rpc_message_set_length (rpc, message_length);
   }

   size_t num_iovecs = 0u;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   const bool is_compressible =
      compressor_id != -1 && IS_NOT_COMMAND (HANDSHAKE_CMD_LEGACY_HELLO) && IS_NOT_COMMAND ("hello") &&
      IS_NOT_COMMAND ("saslstart") && IS_NOT_COMMAND ("saslcontinue") && IS_NOT_COMMAND ("getnonce") &&
      IS_NOT_COMMAND ("authenticate") && IS_NOT_COMMAND ("createuser") && IS_NOT_COMMAND ("updateuser");

   void *compressed_data = NULL;
   size_t compressed_data_len = 0u;

   if (is_compressible && !mcd_rpc_message_compress (rpc,
                                                     compressor_id,
                                                     _compression_level_from_uri (compressor_id, cluster->uri),
                                                     &compressed_data,
                                                     &compressed_data_len,
                                                     error)) {
      goto done;
   }

   if (cluster->client->in_exhaust) {
      bson_set_error (
         error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_IN_EXHAUST, "a cursor derived from this client is in exhaust");
      goto done;
   }

   mcd_rpc_message_egress (rpc);
   if (!_mongoc_stream_writev_full (stream, iovecs, num_iovecs, cluster->sockettimeoutms, error)) {
      RUN_CMD_ERR_DECORATE;
      _handle_network_error (cluster, cmd->server_stream, error);
      goto done;
   }

   ret = true;

done:
   bson_free (compressed_data);
   bson_free (iovecs);
   bson_free (ns);

   return ret;
}

static bool
_mongoc_cluster_run_command_opquery_recv (
   mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, mcd_rpc_message *rpc, bson_t *reply, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT_PARAM (error);

   bool ret = false;

   mongoc_stream_t *const stream = cmd->server_stream->stream;

   mongoc_buffer_t buffer;
   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);

   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   if (!_mongoc_buffer_append_from_stream (&buffer, stream, sizeof (int32_t), cluster->sockettimeoutms, error)) {
      RUN_CMD_ERR (MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");
      _handle_network_error (cluster, cmd->server_stream, error);
      goto done;
   }

   const int32_t message_length = _int32_from_le (buffer.data);

   if (message_length < message_header_length || message_length > MONGOC_DEFAULT_MAX_MSG_SIZE) {
      RUN_CMD_ERR (MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "invalid message length");
      _handle_network_error (cluster, cmd->server_stream, error);
      goto done;
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   if (!_mongoc_buffer_append_from_stream (&buffer, stream, remaining_bytes, cluster->sockettimeoutms, error)) {
      RUN_CMD_ERR (MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");
      _handle_network_error (cluster, cmd->server_stream, error);
      goto done;
   }

   if (!mcd_rpc_message_from_data_in_place (rpc, buffer.data, buffer.len, NULL)) {
      RUN_CMD_ERR (MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "malformed reply from server");
      goto done;
   }

   mcd_rpc_message_ingress (rpc);

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      RUN_CMD_ERR (MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "could not decompress server reply");
      goto done;
   }

   {
      bson_t body;

      if (!mcd_rpc_message_get_body (rpc, &body)) {
         RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "malformed reply from server");
         goto done;
      }

      bson_copy_to (&body, reply);
      bson_destroy (&body);
   }

   if (!_mongoc_cmd_check_ok (reply, cluster->client->error_api_version, error)) {
      goto done;
   }

   ret = true;

done:
   bson_free (decompressed_data);
   _mongoc_buffer_destroy (&buffer);

   return ret;
}

static bool
mongoc_cluster_run_command_opquery (
   mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, int32_t compressor_id, bson_t *reply, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (cmd->server_stream);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT_PARAM (error);

   ENTRY;

   bool ret = false;

   bson_init (reply);
   error->code = 0;

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   if (!_mongoc_cluster_run_command_opquery_send (cluster, cmd, compressor_id, rpc, error)) {
      GOTO (done);
   }

   mcd_rpc_message_reset (rpc);

   if (!_mongoc_cluster_run_command_opquery_recv (cluster, cmd, rpc, reply, error)) {
      GOTO (done);
   }

   ret = true;

done:
   if (!ret && error->code == 0) {
      /* generic error */
      RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "invalid reply from server");
   }

   mcd_rpc_message_destroy (rpc);

   RETURN (ret);
}

static bool
_in_sharded_txn (const mongoc_client_session_t *session)
{
   return session && _mongoc_client_session_in_txn_or_ending (session) &&
          _mongoc_topology_get_type (session->client->topology) == MONGOC_TOPOLOGY_SHARDED;
}

static bool
_in_sharded_or_loadbalanced_txn (const mongoc_client_session_t *session)
{
   if (!session) {
      return false;
   }

   if (!_mongoc_client_session_in_txn_or_ending (session)) {
      return false;
   }

   mongoc_topology_description_type_t type = _mongoc_topology_get_type (session->client->topology);

   return (type == MONGOC_TOPOLOGY_SHARDED) || (type == MONGOC_TOPOLOGY_LOAD_BALANCED);
}

static void
_handle_txn_error_labels (bool cmd_ret, const bson_error_t *cmd_err, const mongoc_cmd_t *cmd, bson_t *reply)
{
   if (!cmd->is_txn_finish) {
      return;
   }

   _mongoc_write_error_handle_labels (cmd_ret, cmd_err, reply, cmd->server_stream->sd);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_run_command_monitored --
 *
 *       Internal function to run a command on a given stream.
 *       @error and @reply are optional out-pointers.
 *
 * Returns:
 *       true if successful; otherwise false and @error is set.
 *
 * Side effects:
 *       If the client's APM callbacks are set, they are executed.
 *       @reply is set and should ALWAYS be released with bson_destroy().
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cluster_run_command_monitored (mongoc_cluster_t *cluster, mongoc_cmd_t *cmd, bson_t *reply, bson_error_t *error)
{
   bool retval;
   const int32_t request_id = ++cluster->request_id;
   uint32_t server_id;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_apm_command_started_t started_event;
   mongoc_apm_command_succeeded_t succeeded_event;
   mongoc_apm_command_failed_t failed_event;
   int64_t started = bson_get_monotonic_time ();
   const mongoc_server_stream_t *server_stream;
   bson_t reply_local;
   bson_error_t error_local;
   bson_iter_t iter;
   bson_t encrypted = BSON_INITIALIZER;
   bson_t decrypted = BSON_INITIALIZER;
   mongoc_cmd_t encrypted_cmd;
   bool is_redacted = false;

   server_stream = cmd->server_stream;
   server_id = server_stream->sd->id;

   callbacks = &cluster->client->apm_callbacks;
   if (!reply) {
      reply = &reply_local;
   }
   if (!error) {
      error = &error_local;
   }

   if (_mongoc_cse_is_enabled (cluster->client)) {
      bson_destroy (&encrypted);

      retval = _mongoc_cse_auto_encrypt (cluster->client, cmd, &encrypted_cmd, &encrypted, error);
      cmd = &encrypted_cmd;
      if (!retval) {
         bson_init (reply);
         goto fail_no_events;
      }
   }

   if (callbacks->started) {
      mongoc_apm_command_started_init_with_cmd (
         &started_event, cmd, request_id, &is_redacted, cluster->client->apm_context);

      callbacks->started (&started_event);
      mongoc_apm_command_started_cleanup (&started_event);
   }

   retval = mongoc_cluster_run_opmsg (cluster, cmd, reply, error);

   if (retval && callbacks->succeeded) {
      bson_t fake_reply = BSON_INITIALIZER;
      /*
       * Unacknowledged writes must provide a CommandSucceededEvent with an
       * {ok: 1} reply.
       * https://github.com/mongodb/specifications/blob/master/source/command-logging-and-monitoring/command-logging-and-monitoring.rst#unacknowledged-acknowledged-writes
       */
      if (!cmd->is_acknowledged) {
         bson_append_int32 (&fake_reply, "ok", 2, 1);
      }
      mongoc_apm_command_succeeded_init (&succeeded_event,
                                         bson_get_monotonic_time () - started,
                                         cmd->is_acknowledged ? reply : &fake_reply,
                                         cmd->command_name,
                                         cmd->db_name,
                                         request_id,
                                         cmd->operation_id,
                                         &server_stream->sd->host,
                                         server_id,
                                         &server_stream->sd->service_id,
                                         server_stream->sd->server_connection_id,
                                         is_redacted,
                                         cluster->client->apm_context);

      callbacks->succeeded (&succeeded_event);
      mongoc_apm_command_succeeded_cleanup (&succeeded_event);
      bson_destroy (&fake_reply);
   }
   if (!retval && callbacks->failed) {
      mongoc_apm_command_failed_init (&failed_event,
                                      bson_get_monotonic_time () - started,
                                      cmd->command_name,
                                      cmd->db_name,
                                      error,
                                      reply,
                                      request_id,
                                      cmd->operation_id,
                                      &server_stream->sd->host,
                                      server_id,
                                      &server_stream->sd->service_id,
                                      server_stream->sd->server_connection_id,
                                      is_redacted,
                                      cluster->client->apm_context);

      callbacks->failed (&failed_event);
      mongoc_apm_command_failed_cleanup (&failed_event);
   }

   if (retval && _mongoc_cse_is_enabled (cluster->client)) {
      bson_destroy (&decrypted);
      retval = _mongoc_cse_auto_decrypt (cluster->client, cmd->db_name, reply, &decrypted, error);
      bson_destroy (reply);
      bson_steal (reply, &decrypted);
      bson_init (&decrypted);
      if (!retval) {
         goto fail_no_events;
      }
   }

   _handle_not_primary_error (cluster, server_stream, reply);

   _handle_txn_error_labels (retval, error, cmd, reply);

   if (retval && _in_sharded_or_loadbalanced_txn (cmd->session) &&
       bson_iter_init_find (&iter, reply, "recoveryToken")) {
      bson_destroy (cmd->session->recovery_token);
      if (BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         cmd->session->recovery_token = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data,
                                                            bson_iter_value (&iter)->value.v_doc.data_len);
      } else {
         MONGOC_ERROR ("Malformed recovery token from server");
         cmd->session->recovery_token = NULL;
      }
   }

fail_no_events:
   if (reply == &reply_local) {
      bson_destroy (&reply_local);
   }

   bson_destroy (&encrypted);
   bson_destroy (&decrypted);

   _mongoc_topology_update_last_used (cluster->client->topology, server_id);

   return retval;
}


static bool
_should_use_op_msg (const mongoc_cluster_t *cluster)
{
   return mongoc_cluster_uses_server_api (cluster) || mongoc_cluster_uses_loadbalanced (cluster);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_run_command_private --
 *
 *       Internal function to run a command on a given stream.
 *       @error and @reply are optional out-pointers.
 *       The client's APM callbacks are not executed.
 *       Automatic encryption/decryption is not performed.
 *
 * Returns:
 *       true if successful; otherwise false and @error is set.
 *
 * Side effects:
 *       @reply is set and should ALWAYS be released with bson_destroy().
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cluster_run_command_private (mongoc_cluster_t *cluster,
                                    const mongoc_cmd_t *cmd,
                                    bson_t *reply,
                                    bson_error_t *error)
{
   bool retval;
   const mongoc_server_stream_t *server_stream;
   bson_t reply_local;
   bson_error_t error_local;

   if (!error) {
      error = &error_local;
   }

   /* If NULL was passed, we use our local variable as a temporary sink: */
   if (!reply) {
      reply = &reply_local;
   }

   server_stream = cmd->server_stream;

   if (_should_use_op_msg (cluster) || server_stream->sd->max_wire_version >= WIRE_VERSION_MIN) {
      retval = mongoc_cluster_run_opmsg (cluster, cmd, reply, error);
   } else {
      retval = mongoc_cluster_run_command_opquery (cluster, cmd, -1, reply, error);
   }

   _handle_not_primary_error (cluster, server_stream, reply);

   if (reply == &reply_local) {
      bson_destroy (&reply_local);
   }

   _mongoc_topology_update_last_used (cluster->client->topology, server_stream->sd->id);

   return retval;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_run_command_parts --
 *
 *       Internal function to assemble command parts and run a command
 *       on a given stream. @error and @reply are optional out-pointers.
 *       The client's APM callbacks are not executed.
 *
 * Returns:
 *       true if successful; otherwise false and @error is set.
 *
 * Side effects:
 *       @reply is set and should ALWAYS be released with bson_destroy().
 *       mongoc_cmd_parts_cleanup will be always be called on parts. The
 *       caller should *not* call cleanup on the parts.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cluster_run_command_parts (mongoc_cluster_t *cluster,
                                  mongoc_server_stream_t *server_stream,
                                  mongoc_cmd_parts_t *parts,
                                  bson_t *reply,
                                  bson_error_t *error)
{
   bool ret;

   if (!mongoc_cmd_parts_assemble (parts, server_stream, error)) {
      _mongoc_bson_init_if_set (reply);
      mongoc_cmd_parts_cleanup (parts);
      return false;
   }

   ret = mongoc_cluster_run_command_private (cluster, &parts->assembled, reply, error);
   mongoc_cmd_parts_cleanup (parts);
   return ret;
}

/*
 *--------------------------------------------------------------------------
 *
 * _stream_run_hello --
 *
 *       Run a hello command on the given stream. If
 *       @negotiate_sasl_supported_mechs is true, then saslSupportedMechs is
 *       added to the hello command.
 *
 * Returns:
 *       A mongoc_server_description_t you must destroy or NULL. If the call
 *       failed its error is set and its type is MONGOC_SERVER_UNKNOWN.
 *
 *--------------------------------------------------------------------------
 */
static mongoc_server_description_t *
_stream_run_hello (mongoc_cluster_t *cluster,
                   mongoc_stream_t *stream,
                   const char *address,
                   uint32_t server_id,
                   bool negotiate_sasl_supported_mechs,
                   mongoc_scram_t *scram,
                   bson_t *speculative_auth_response /* OUT */,
                   bson_error_t *error)
{
   mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (cluster)->client->topology);

   ENTRY;

   BSON_ASSERT (stream);

   bson_t handshake_command;
   _mongoc_topology_dup_handshake_cmd (cluster->client->topology, &handshake_command);

   if (cluster->requires_auth && speculative_auth_response) {
      mongoc_ssl_opt_t *ssl_opts = NULL;
#ifdef MONGOC_ENABLE_SSL
      ssl_opts = &cluster->client->ssl_opts;
#endif

      _mongoc_topology_scanner_add_speculative_authentication (&handshake_command, cluster->uri, ssl_opts, scram);
   }

   if (negotiate_sasl_supported_mechs) {
      _mongoc_handshake_append_sasl_supported_mechs (cluster->uri, &handshake_command);
   }

   const int64_t start = bson_get_monotonic_time ();
   /* TODO CDRIVER-3654: do not use a mongoc_server_stream here.
    * Instead, use a plain stream. If a network error occurs, check the cluster
    * node's generation (which is the generation of the created connection) to
    * determine if the error should be handled.
    * The current behavior may double invalidate.
    * If a network error occurs in mongoc_cluster_run_command_private below,
    * that invalidates (thinking the error is a post-handshake network error).
    * Then _mongoc_cluster_stream_for_server also handles the error, and
    * invalidates again.
    */
   mongoc_server_description_t empty_sd;
   mongoc_server_description_init (&empty_sd, address, server_id);
   mongoc_server_stream_t *const server_stream = _mongoc_cluster_create_server_stream (td.ptr, &empty_sd, stream);
   mongoc_server_description_cleanup (&empty_sd);

   mongoc_query_flags_t query_flags = MONGOC_QUERY_NONE;
   /* Use OP_QUERY for the handshake, unless the user has specified an
    * API version; the correct hello_cmd has already been selected: */
   if (!_should_use_op_msg (cluster)) {
      /* Complete OPCODE_QUERY setup: */
      query_flags |= MONGOC_QUERY_SECONDARY_OK;
   } else {
      /* We're using OP_MSG, and require some additional doctoring: */
      bson_append_utf8 (&handshake_command, "$db", 3, "admin", 5);
   }

   /* Set up the shared parts of the mongo_cmd_t, which will later be converted
   to either an op_msg or op_query: */
   const mongoc_cmd_t hello_cmd = {
      .db_name = "admin",
      .command = &handshake_command,
      .command_name = _mongoc_get_command_name (&handshake_command),
      .server_stream = server_stream,
      .is_acknowledged = true,
      .query_flags = query_flags,
   };

   bson_t reply;
   // The final resulting server description
   mongoc_server_description_t *ret_handshake_sd = NULL;
   if (!mongoc_cluster_run_command_private (cluster, &hello_cmd, &reply, error)) {
      // Command execution failed.
      if (negotiate_sasl_supported_mechs) {
         // Negotiating a new SASL mechanism
         bsonParse (reply,
                    find (allOf (key ("ok"), isFalse), //
                          do ({
                             /* hello response returned ok: 0. According to
                              * auth spec: "If the hello of the MongoDB
                              * Handshake fails with an error, drivers MUST
                              * treat this an authentication error." */
                             error->domain = MONGOC_ERROR_CLIENT;
                             error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;
                          })));
      }
   } else {
      // "hello" succeeded

      // Round-trip time for the hello command
      const int64_t rtt_msec = (bson_get_monotonic_time () - start) / 1000;

      ret_handshake_sd = BSON_ALIGNED_ALLOC0 (mongoc_server_description_t);
      mongoc_server_description_init (ret_handshake_sd, address, server_id);
      /* send the error from run_command IN to handle_hello */
      mongoc_server_description_handle_hello (ret_handshake_sd, &reply, rtt_msec, error);

      if (cluster->requires_auth && speculative_auth_response) {
         _mongoc_topology_scanner_parse_speculative_authentication (&reply, speculative_auth_response);
      }

      /* Note: This call will render our copy of the topology description to be
       * stale */
      const bool update_okay = _mongoc_topology_update_from_handshake (cluster->client->topology, ret_handshake_sd);
      if (!update_okay) {
         mongoc_server_description_reset (ret_handshake_sd);
         bson_set_error (&ret_handshake_sd->error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_NOT_ESTABLISHED,
                         "\"%s\" removed from topology",
                         address);
      }
   }

   mongoc_server_stream_cleanup (server_stream);
   bson_destroy (&handshake_command);
   bson_destroy (&reply);
   mc_tpld_drop_ref (&td);

   RETURN (ret_handshake_sd);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_run_hello --
 *
 *       Run an initial hello command for the given node and handle result.
 *
 * Returns:
 *       mongoc_server_description_t on success, NULL otherwise.
 *       the mongoc_server_description_t MUST BE DESTROYED BY THE CALLER.
 *
 * Side effects:
 *       Makes a blocking I/O call, updates cluster->topology->description
 *       with hello result.
 *
 *--------------------------------------------------------------------------
 */
static mongoc_server_description_t *
_cluster_run_hello (mongoc_cluster_t *cluster,
                    mongoc_cluster_node_t *node,
                    uint32_t server_id,
                    mongoc_scram_t *scram /* OUT */,
                    bson_t *speculative_auth_response /* OUT */,
                    bson_error_t *error /* OUT */)
{
   mongoc_server_description_t *sd;

   ENTRY;

   BSON_ASSERT (cluster);
   BSON_ASSERT (node);
   BSON_ASSERT (node->stream);

   sd = _stream_run_hello (cluster,
                           node->stream,
                           node->connection_address,
                           server_id,
                           _mongoc_uri_requires_auth_negotiation (cluster->uri),
                           scram,
                           speculative_auth_response,
                           error);

   if (!sd) {
      return NULL;
   }

   if (sd->type == MONGOC_SERVER_UNKNOWN) {
      memcpy (error, &sd->error, sizeof (bson_error_t));
      mongoc_server_description_destroy (sd);
      return NULL;
   }

   return sd;
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_build_basic_auth_digest --
 *
 *       Computes the Basic Authentication digest using the credentials
 *       configured for @cluster and the @nonce provided.
 *
 *       The result should be freed by the caller using bson_free() when
 *       they are finished with it.
 *
 * Returns:
 *       A newly allocated string containing the digest.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static char *
_mongoc_cluster_build_basic_auth_digest (mongoc_cluster_t *cluster, const char *nonce)
{
   const char *username;
   const char *password;
   char *password_digest;
   char *password_md5;
   char *digest_in;
   char *ret;

   ENTRY;

   /*
    * The following generates the digest to be used for basic authentication
    * with a MongoDB server. More information on the format can be found
    * at the following location:
    *
    * https://www.mongodb.com/docs/meta-driver/latest/legacy/
    *   implement-authentication-in-driver/
    */

   BSON_ASSERT (cluster);
   BSON_ASSERT (cluster->uri);

   username = mongoc_uri_get_username (cluster->uri);
   password = mongoc_uri_get_password (cluster->uri);
   password_digest = bson_strdup_printf ("%s:mongo:%s", username, password);
   password_md5 = _mongoc_hex_md5 (password_digest);
   digest_in = bson_strdup_printf ("%s%s%s", nonce, username, password_md5);
   ret = _mongoc_hex_md5 (digest_in);
   bson_free (digest_in);
   bson_free (password_md5);
   bson_free (password_digest);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_node_cr --
 *
 *       Performs authentication of @node using the credentials provided
 *       when configuring the @cluster instance.
 *
 *       This is the Challenge-Response mode of authentication.
 *
 * Returns:
 *       true if authentication was successful; otherwise false and
 *       @error is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_node_cr (mongoc_cluster_t *cluster,
                              mongoc_stream_t *stream,
                              mongoc_server_description_t *sd,
                              bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   bson_iter_t iter;
   const char *auth_source;
   bson_t command;
   bson_t reply;
   char *digest;
   char *nonce;
   bool ret;
   mongoc_server_stream_t *server_stream;
   mc_shared_tpld td;

   ENTRY;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);

   if (!(auth_source = mongoc_uri_get_auth_source (cluster->uri)) || (*auth_source == '\0')) {
      auth_source = "admin";
   }

   /*
    * To authenticate a node using basic authentication, we need to first
    * get the nonce from the server. We use that to hash our password which
    * is sent as a reply to the server. If everything went good we get a
    * success notification back from the server.
    */

   /*
    * Execute the getnonce command to fetch the nonce used for generating
    * md5 digest of our password information.
    */
   bson_init (&command);
   bson_append_int32 (&command, "getnonce", 8, 1);
   mongoc_cmd_parts_init (&parts, cluster->client, auth_source, MONGOC_QUERY_SECONDARY_OK, &command);
   parts.prohibit_lsid = true;

   td = mc_tpld_take_ref (cluster->client->topology);
   server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);
   mc_tpld_drop_ref (&td);

   if (!mongoc_cluster_run_command_parts (cluster, server_stream, &parts, &reply, error)) {
      mongoc_server_stream_cleanup (server_stream);
      bson_destroy (&command);
      bson_destroy (&reply);
      RETURN (false);
   }
   bson_destroy (&command);
   if (!bson_iter_init_find_case (&iter, &reply, "nonce")) {
      bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_GETNONCE, "Invalid reply from getnonce");
      bson_destroy (&reply);
      RETURN (false);
   }

   /*
    * Build our command to perform the authentication.
    */
   nonce = bson_iter_dup_utf8 (&iter, NULL);
   digest = _mongoc_cluster_build_basic_auth_digest (cluster, nonce);
   bson_init (&command);
   bson_append_int32 (&command, "authenticate", 12, 1);
   bson_append_utf8 (&command, "user", 4, mongoc_uri_get_username (cluster->uri), -1);
   bson_append_utf8 (&command, "nonce", 5, nonce, -1);
   bson_append_utf8 (&command, "key", 3, digest, -1);
   bson_destroy (&reply);
   bson_free (nonce);
   bson_free (digest);

   /*
    * Execute the authenticate command. mongoc_cluster_run_command_private
    * checks for {ok: 1} in the response.
    */
   mongoc_cmd_parts_init (&parts, cluster->client, auth_source, MONGOC_QUERY_SECONDARY_OK, &command);
   parts.prohibit_lsid = true;
   ret = mongoc_cluster_run_command_parts (cluster, server_stream, &parts, &reply, error);

   if (!ret) {
      /* error->message is already set */
      error->domain = MONGOC_ERROR_CLIENT;
      error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;
   }

   mongoc_server_stream_cleanup (server_stream);
   bson_destroy (&command);
   bson_destroy (&reply);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_node_plain --
 *
 *       Perform SASL PLAIN authentication for @node. We do this manually
 *       instead of using the SASL module because it is rather simplistic.
 *
 * Returns:
 *       true if successful; otherwise false and error is set.
 *
 * Side effects:
 *       error may be set.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_node_plain (mongoc_cluster_t *cluster,
                                 mongoc_stream_t *stream,
                                 mongoc_server_description_t *sd,
                                 bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   char buf[4096];
   int buflen = 0;
   const char *username;
   const char *password;
   bson_t b = BSON_INITIALIZER;
   bson_t reply;
   size_t len;
   char *str;
   bool ret;
   mongoc_server_stream_t *server_stream;
   mc_shared_tpld td;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);

   username = mongoc_uri_get_username (cluster->uri);
   if (!username) {
      username = "";
   }

   password = mongoc_uri_get_password (cluster->uri);
   if (!password) {
      password = "";
   }

   str = bson_strdup_printf ("%c%s%c%s", '\0', username, '\0', password);
   len = strlen (username) + strlen (password) + 2;
   buflen = mcommon_b64_ntop ((const uint8_t *) str, len, buf, sizeof buf);
   bson_free (str);

   if (buflen == -1) {
      bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "failed base64 encoding message");
      return false;
   }

   BSON_APPEND_INT32 (&b, "saslStart", 1);
   BSON_APPEND_UTF8 (&b, "mechanism", "PLAIN");
   bson_append_utf8 (&b, "payload", 7, (const char *) buf, buflen);
   BSON_APPEND_INT32 (&b, "autoAuthorize", 1);

   mongoc_cmd_parts_init (&parts, cluster->client, "$external", MONGOC_QUERY_SECONDARY_OK, &b);
   parts.prohibit_lsid = true;

   td = mc_tpld_take_ref (cluster->client->topology);
   server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);
   mc_tpld_drop_ref (&td);

   ret = mongoc_cluster_run_command_parts (cluster, server_stream, &parts, &reply, error);
   mongoc_server_stream_cleanup (server_stream);
   if (!ret) {
      /* error->message is already set */
      error->domain = MONGOC_ERROR_CLIENT;
      error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;
   }

   bson_destroy (&b);
   bson_destroy (&reply);

   return ret;
}

bool
_mongoc_cluster_get_auth_cmd_x509 (const mongoc_uri_t *uri,
                                   const mongoc_ssl_opt_t *ssl_opts,
                                   bson_t *cmd /* OUT */,
                                   bson_error_t *error /* OUT */)
{
#ifndef MONGOC_ENABLE_SSL
   bson_set_error (error,
                   MONGOC_ERROR_CLIENT,
                   MONGOC_ERROR_CLIENT_AUTHENTICATE,
                   "The MONGODB-X509 authentication mechanism requires "
                   "libmongoc built with ENABLE_SSL");
   return false;
#else
   const char *username_from_uri = NULL;
   char *username_from_subject = NULL;

   BSON_ASSERT (uri);

   username_from_uri = mongoc_uri_get_username (uri);
   if (username_from_uri) {
      TRACE ("%s", "X509: got username from URI");
   } else {
      if (!ssl_opts || !ssl_opts->pem_file) {
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_AUTHENTICATE,
                         "cannot determine username for "
                         "X-509 authentication.");
         return false;
      }

      username_from_subject = mongoc_ssl_extract_subject (ssl_opts->pem_file, ssl_opts->pem_pwd);
      if (!username_from_subject) {
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_AUTHENTICATE,
                         "No username provided for X509 authentication.");
         return false;
      }

      TRACE ("%s", "X509: got username from certificate");
   }

   bson_init (cmd);
   BSON_APPEND_INT32 (cmd, "authenticate", 1);
   BSON_APPEND_UTF8 (cmd, "mechanism", "MONGODB-X509");
   BSON_APPEND_UTF8 (cmd, "user", username_from_uri ? username_from_uri : username_from_subject);

   bson_free (username_from_subject);

   return true;
#endif
}


static bool
_mongoc_cluster_auth_node_x509 (mongoc_cluster_t *cluster,
                                mongoc_stream_t *stream,
                                mongoc_server_description_t *sd,
                                bson_error_t *error)
{
#ifndef MONGOC_ENABLE_SSL
   bson_set_error (error,
                   MONGOC_ERROR_CLIENT,
                   MONGOC_ERROR_CLIENT_AUTHENTICATE,
                   "The MONGODB-X509 authentication mechanism requires "
                   "libmongoc built with ENABLE_SSL");
   return false;
#else
   mongoc_cmd_parts_t parts;
   bson_t cmd;
   bson_t reply;
   bool ret;
   mongoc_server_stream_t *server_stream;
   mc_shared_tpld td;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);

   if (!_mongoc_cluster_get_auth_cmd_x509 (cluster->uri, &cluster->client->ssl_opts, &cmd, error)) {
      return false;
   }

   mongoc_cmd_parts_init (&parts, cluster->client, "$external", MONGOC_QUERY_SECONDARY_OK, &cmd);
   parts.prohibit_lsid = true;
   td = mc_tpld_take_ref (cluster->client->topology);
   server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);
   mc_tpld_drop_ref (&td);

   ret = mongoc_cluster_run_command_parts (cluster, server_stream, &parts, &reply, error);
   mongoc_server_stream_cleanup (server_stream);
   if (!ret) {
      /* error->message is already set */
      error->domain = MONGOC_ERROR_CLIENT;
      error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;
   }

   bson_destroy (&cmd);
   bson_destroy (&reply);

   return ret;
#endif
}

bool
mongoc_cluster_uses_server_api (const mongoc_cluster_t *cluster)
{
   BSON_ASSERT_PARAM (cluster);
   return mongoc_client_uses_server_api (cluster->client);
}

bool
mongoc_cluster_uses_loadbalanced (const mongoc_cluster_t *cluster)
{
   BSON_ASSERT_PARAM (cluster);
   return mongoc_client_uses_loadbalanced (cluster->client);
}

#ifdef MONGOC_ENABLE_CRYPTO
void
_mongoc_cluster_init_scram (const mongoc_cluster_t *cluster, mongoc_scram_t *scram, mongoc_crypto_hash_algorithm_t algo)
{
   _mongoc_uri_init_scram (cluster->uri, scram, algo);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_get_auth_cmd_scram --
 *
 *       Generates the saslStart command for scram authentication. Used
 *       during explicit authentication as well as speculative
 *       authentication during hello.
 *
 *
 * Returns:
 *       true if the command could be generated, false otherwise
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

bool
_mongoc_cluster_get_auth_cmd_scram (mongoc_crypto_hash_algorithm_t algo,
                                    mongoc_scram_t *scram,
                                    bson_t *cmd /* OUT */,
                                    bson_error_t *error /* OUT */)
{
   uint8_t buf[4096] = {0};
   uint32_t buflen = 0;
   bson_t options;

   if (!_mongoc_scram_step (scram, buf, buflen, buf, sizeof buf, &buflen, error)) {
      return false;
   }

   BSON_ASSERT (scram->step == 1);

   bson_init (cmd);

   BSON_APPEND_INT32 (cmd, "saslStart", 1);
   if (algo == MONGOC_CRYPTO_ALGORITHM_SHA_1) {
      BSON_APPEND_UTF8 (cmd, "mechanism", "SCRAM-SHA-1");
   } else if (algo == MONGOC_CRYPTO_ALGORITHM_SHA_256) {
      BSON_APPEND_UTF8 (cmd, "mechanism", "SCRAM-SHA-256");
   } else {
      BSON_ASSERT (false);
   }
   bson_append_binary (cmd, "payload", 7, BSON_SUBTYPE_BINARY, buf, buflen);
   BSON_APPEND_INT32 (cmd, "autoAuthorize", 1);

   BSON_APPEND_DOCUMENT_BEGIN (cmd, "options", &options);
   BSON_APPEND_BOOL (&options, "skipEmptyExchange", true);
   bson_append_document_end (cmd, &options);

   bson_destroy (&options);

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_run_scram_command --
 *
 *       Runs a scram authentication command, handling auth_source and
 *       errors during the command.
 *
 *
 * Returns:
 *       true if the command was successful, false otherwise
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_run_scram_command (mongoc_cluster_t *cluster,
                                   mongoc_stream_t *stream,
                                   const mongoc_server_description_t *handshake_sd,
                                   const bson_t *cmd,
                                   bson_t *reply,
                                   bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   mongoc_server_stream_t *server_stream;
   const char *auth_source;
   mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (cluster)->client->topology);

   if (!(auth_source = mongoc_uri_get_auth_source (cluster->uri)) || (*auth_source == '\0')) {
      auth_source = "admin";
   }

   mongoc_cmd_parts_init (&parts, cluster->client, auth_source, MONGOC_QUERY_SECONDARY_OK, cmd);
   parts.prohibit_lsid = true;
   server_stream = _mongoc_cluster_create_server_stream (td.ptr, handshake_sd, stream);
   mc_tpld_drop_ref (&td);

   if (!mongoc_cluster_run_command_parts (cluster, server_stream, &parts, reply, error)) {
      mongoc_server_stream_cleanup (server_stream);
      bson_destroy (reply);

      /* error->message is already set */
      error->domain = MONGOC_ERROR_CLIENT;
      error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;

      return false;
   }

   mongoc_server_stream_cleanup (server_stream);

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_scram_start --
 *
 *       Starts scram authentication by generating and sending the saslStart
 *       command. The conversation can then be resumed using
 *       _mongoc_cluster_auth_scram_continue.
 *
 *
 * Returns:
 *       true if the saslStart command was successful, false otherwise
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_scram_start (mongoc_cluster_t *cluster,
                                  mongoc_stream_t *stream,
                                  const mongoc_server_description_t *handshake_sd,
                                  mongoc_crypto_hash_algorithm_t algo,
                                  mongoc_scram_t *scram,
                                  bson_t *reply,
                                  bson_error_t *error)
{
   bson_t cmd;

   BSON_ASSERT (scram->step == 0);

   if (!_mongoc_cluster_get_auth_cmd_scram (algo, scram, &cmd, error)) {
      /* error->message is already set */
      error->domain = MONGOC_ERROR_CLIENT;
      error->code = MONGOC_ERROR_CLIENT_AUTHENTICATE;

      return false;
   }

   if (!_mongoc_cluster_run_scram_command (cluster, stream, handshake_sd, &cmd, reply, error)) {
      bson_destroy (&cmd);

      return false;
   }

   bson_destroy (&cmd);

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_scram_handle_reply --
 *
 *       Handles replies from _mongoc_cluster_run_scram_command. The @done
 *       argument will be set to true if the scram conversation was
 *       completed successfully.
 *
 *
 * Returns:
 *       true if the reply was handled successfully, false if there was an
 *       error. Note that the return value itself does not indicate whether
 *       authentication was completed successfully.
 *
 * Side effects:
 *       @error is set on failure. @done, @conv_id, @buf, and @buflen are
 *       set for use in the next scram step.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_scram_handle_reply (mongoc_scram_t *scram,
                                    const bson_t *reply,
                                    bool *done /* OUT */,
                                    int *conv_id /* OUT */,
                                    uint8_t *buf /* OUT */,
                                    uint32_t bufmax,
                                    uint32_t *buflen /* OUT */,
                                    bson_error_t *error)
{
   bson_iter_t iter;
   bson_subtype_t btype;
   const char *tmpstr;

   BSON_ASSERT (scram);

   bool is_done = false;
   bsonParse (*reply, find (key ("done"), storeBool (is_done)));

   if (is_done) {
      if (scram->step < 2) {
         /* Prior to step 2, we haven't even received server proof. */
         bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Incorrect step for 'done'");
         return false;
      }
      *done = true;
      if (scram->step >= 3) {
         return true;
      }
   }

   if (!bson_iter_init_find (&iter, reply, "conversationId") || !BSON_ITER_HOLDS_INT32 (&iter) ||
       !(*conv_id = bson_iter_int32 (&iter)) || !bson_iter_init_find (&iter, reply, "payload") ||
       !BSON_ITER_HOLDS_BINARY (&iter)) {
      const char *errmsg = "Received invalid SCRAM reply from MongoDB server.";

      MONGOC_DEBUG ("SCRAM: authentication failed");

      if (bson_iter_init_find (&iter, reply, "errmsg") && BSON_ITER_HOLDS_UTF8 (&iter)) {
         errmsg = bson_iter_utf8 (&iter, NULL);
      }

      bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "%s", errmsg);
      return false;
   }

   bson_iter_binary (&iter, &btype, buflen, (const uint8_t **) &tmpstr);

   if (*buflen > bufmax) {
      bson_set_error (
         error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "SCRAM reply from MongoDB is too large.");
      return false;
   }

   memcpy (buf, tmpstr, *buflen);

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_scram_continue --
 *
 *       Continues the scram conversation from the reply to a saslStart
 *       command, either sent explicitly or received through speculative
 *       authentication during hello.
 *
 *
 * Returns:
 *       true if authenticated. false on failure and @error is set.
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_scram_continue (mongoc_cluster_t *cluster,
                                     mongoc_stream_t *stream,
                                     const mongoc_server_description_t *handshake_sd,
                                     mongoc_scram_t *scram,
                                     const bson_t *sasl_start_reply,
                                     bson_error_t *error)
{
   bson_t cmd;
   uint8_t buf[4096] = {0};
   uint32_t buflen = 0;
   int conv_id = 0;
   bool done = false;
   bson_t reply_local;

   if (!_mongoc_cluster_scram_handle_reply (
          scram, sasl_start_reply, &done, &conv_id, buf, sizeof buf, &buflen, error)) {
      return false;
   }

   for (;;) {
      if (!_mongoc_scram_step (scram, buf, buflen, buf, sizeof buf, &buflen, error)) {
         return false;
      }

      if (done && (scram->step >= 3)) {
         break;
      }

      bson_init (&cmd);

      BSON_APPEND_INT32 (&cmd, "saslContinue", 1);
      BSON_APPEND_INT32 (&cmd, "conversationId", conv_id);
      bson_append_binary (&cmd, "payload", 7, BSON_SUBTYPE_BINARY, buf, buflen);

      TRACE ("SCRAM: authenticating (step %d)", scram->step);

      if (!_mongoc_cluster_run_scram_command (cluster, stream, handshake_sd, &cmd, &reply_local, error)) {
         bson_destroy (&cmd);
         return false;
      }

      bson_destroy (&cmd);

      if (!_mongoc_cluster_scram_handle_reply (scram, &reply_local, &done, &conv_id, buf, sizeof buf, &buflen, error)) {
         bson_destroy (&reply_local);
         return false;
      }

      bson_destroy (&reply_local);

      if (done && (scram->step >= 3)) {
         break;
      }
   }

   TRACE ("%s", "SCRAM: authenticated");

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_node_scram --
 *
 *       Invokes scram authentication by sending a saslStart command and
 *       handling all replies.
 *
 *
 * Returns:
 *       true if authenticated. false on failure and @error is set.
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_node_scram (mongoc_cluster_t *cluster,
                                 mongoc_stream_t *stream,
                                 mongoc_server_description_t *handshake_sd,
                                 mongoc_crypto_hash_algorithm_t algo,
                                 bson_error_t *error)
{
   mongoc_scram_t scram;
   bool ret = false;
   bson_t reply;

   BSON_ASSERT (cluster);

   _mongoc_cluster_init_scram (cluster, &scram, algo);

   if (!_mongoc_cluster_auth_scram_start (cluster, stream, handshake_sd, algo, &scram, &reply, error)) {
      goto failure;
   }

   if (!_mongoc_cluster_auth_scram_continue (cluster, stream, handshake_sd, &scram, &reply, error)) {
      bson_destroy (&reply);

      goto failure;
   }

   TRACE ("%s", "SCRAM: authenticated");

   ret = true;

   bson_destroy (&reply);

failure:
   _mongoc_scram_destroy (&scram);

   return ret;
}
#endif

static bool
_mongoc_cluster_auth_node_scram_sha_1 (mongoc_cluster_t *cluster,
                                       mongoc_stream_t *stream,
                                       mongoc_server_description_t *sd,
                                       bson_error_t *error)
{
#ifndef MONGOC_ENABLE_CRYPTO
   bson_set_error (error,
                   MONGOC_ERROR_CLIENT,
                   MONGOC_ERROR_CLIENT_AUTHENTICATE,
                   "The SCRAM_SHA_1 authentication mechanism requires "
                   "libmongoc built with ENABLE_SSL");
   return false;
#else
   return _mongoc_cluster_auth_node_scram (cluster, stream, sd, MONGOC_CRYPTO_ALGORITHM_SHA_1, error);
#endif
}

static bool
_mongoc_cluster_auth_node_scram_sha_256 (mongoc_cluster_t *cluster,
                                         mongoc_stream_t *stream,
                                         mongoc_server_description_t *sd,
                                         bson_error_t *error)
{
#ifndef MONGOC_ENABLE_CRYPTO
   bson_set_error (error,
                   MONGOC_ERROR_CLIENT,
                   MONGOC_ERROR_CLIENT_AUTHENTICATE,
                   "The SCRAM_SHA_256 authentication mechanism requires "
                   "libmongoc built with ENABLE_SSL");
   return false;
#else
   return _mongoc_cluster_auth_node_scram (cluster, stream, sd, MONGOC_CRYPTO_ALGORITHM_SHA_256, error);
#endif
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_node --
 *
 *       Authenticate a cluster node depending on the required mechanism.
 *
 * Returns:
 *       true if authenticated. false on failure and @error is set.
 *
 * Side effects:
 *       @error is set on failure.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_cluster_auth_node (mongoc_cluster_t *cluster,
                           mongoc_stream_t *stream,
                           mongoc_server_description_t *sd,
                           const mongoc_handshake_sasl_supported_mechs_t *sasl_supported_mechs,
                           bson_error_t *error)
{
   bool ret = false;
   const char *mechanism;
   ENTRY;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);

   mechanism = mongoc_uri_get_auth_mechanism (cluster->uri);

   if (!mechanism) {
      if (sasl_supported_mechs->scram_sha_256) {
         /* Auth spec: "If SCRAM-SHA-256 is present in the list of mechanisms,
          * then it MUST be used as the default; otherwise, SCRAM-SHA-1 MUST be
          * used as the default, regardless of whether SCRAM-SHA-1 is in the
          * list. Drivers MUST NOT attempt to use any other mechanism (e.g.
          * PLAIN) as the default." [...] "If saslSupportedMechs is not present
          * in the hello results for mechanism negotiation, then SCRAM-SHA-1
          * MUST be used when talking to servers >= 3.0." */
         mechanism = "SCRAM-SHA-256";
      } else {
         mechanism = "SCRAM-SHA-1";
      }
   }

   if (0 == strcasecmp (mechanism, "MONGODB-CR")) {
      ret = _mongoc_cluster_auth_node_cr (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "MONGODB-X509")) {
      ret = _mongoc_cluster_auth_node_x509 (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "SCRAM-SHA-1")) {
      ret = _mongoc_cluster_auth_node_scram_sha_1 (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "SCRAM-SHA-256")) {
      ret = _mongoc_cluster_auth_node_scram_sha_256 (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "GSSAPI")) {
      ret = _mongoc_cluster_auth_node_sasl (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "PLAIN")) {
      ret = _mongoc_cluster_auth_node_plain (cluster, stream, sd, error);
   } else if (0 == strcasecmp (mechanism, "MONGODB-AWS")) {
      ret = _mongoc_cluster_auth_node_aws (cluster, stream, sd, error);
   } else {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_AUTHENTICATE,
                      "Unknown authentication mechanism \"%s\".",
                      mechanism);
   }

   if (!ret) {
      mongoc_counter_auth_failure_inc ();
      MONGOC_DEBUG ("Authentication failed: %s", error->message);
   } else {
      mongoc_counter_auth_success_inc ();
      TRACE ("%s", "Authentication succeeded");
   }

   RETURN (ret);
}


/*
 * Close the connection associated with this server.
 *
 * Called when a network error occurs, or to close connection tied to an exhaust
 * cursor.
 * If the cluster is pooled, removes the node from cluster's set of nodes.
 * WARNING: pointers to a disconnected mongoc_cluster_node_t or its stream are
 * now invalid, be careful of dangling pointers.
 */

void
mongoc_cluster_disconnect_node (mongoc_cluster_t *cluster, uint32_t server_id)
{
   mongoc_topology_t *topology = cluster->client->topology;

   ENTRY;

   if (topology->single_threaded) {
      mongoc_topology_scanner_node_t *scanner_node;

      scanner_node = mongoc_topology_scanner_get_node (topology->scanner, server_id);

      /* might never actually have connected */
      if (scanner_node && scanner_node->stream) {
         mongoc_topology_scanner_node_disconnect (scanner_node, true);
      }
   } else {
      mongoc_set_rm (cluster->nodes, server_id);
   }

   EXIT;
}

static void
_mongoc_cluster_node_destroy (mongoc_cluster_node_t *node)
{
   /* Failure, or Replica Set reconfigure without this node */
   mongoc_stream_failed (node->stream);
   bson_free (node->connection_address);
   mongoc_server_description_destroy (node->handshake_sd);

   bson_free (node);
}

static void
_mongoc_cluster_node_dtor (void *data_, void *ctx_)
{
   mongoc_cluster_node_t *node = (mongoc_cluster_node_t *) data_;

   BSON_UNUSED (ctx_);

   _mongoc_cluster_node_destroy (node);
}

static mongoc_cluster_node_t *
_mongoc_cluster_node_new (mongoc_stream_t *stream, const char *connection_address)
{
   mongoc_cluster_node_t *node;

   if (!stream) {
      return NULL;
   }

   node = (mongoc_cluster_node_t *) bson_malloc0 (sizeof *node);

   node->stream = stream;
   node->connection_address = bson_strdup (connection_address);

   /* Note that the node->sd field is set to NULL by bson_malloc0(),
   rather than being explicitly initialized. */

   return node;
}

static bool
_mongoc_cluster_finish_speculative_auth (mongoc_cluster_t *cluster,
                                         mongoc_stream_t *stream,
                                         mongoc_server_description_t *handshake_sd,
                                         bson_t *speculative_auth_response,
                                         mongoc_scram_t *scram,
                                         bson_error_t *error)
{
   const char *mechanism = _mongoc_topology_scanner_get_speculative_auth_mechanism (cluster->uri);
   bool ret = false;
   bool auth_handled = false;

   BSON_ASSERT (handshake_sd);
   BSON_ASSERT (speculative_auth_response);

   if (!mechanism) {
      return false;
   }

   if (bson_empty (speculative_auth_response)) {
      return false;
   }

#ifdef MONGOC_ENABLE_SSL
   if (strcasecmp (mechanism, "MONGODB-X509") == 0) {
      /* For X509, a successful hello with speculativeAuthenticate field
       * indicates successful auth */
      ret = true;
      auth_handled = true;
   }
#endif

#ifdef MONGOC_ENABLE_CRYPTO
   if (strcasecmp (mechanism, "SCRAM-SHA-1") == 0 || strcasecmp (mechanism, "SCRAM-SHA-256") == 0) {
      /* Don't attempt authentication if scram objects have advanced past
       * saslStart */
      if (scram->step != 1) {
         return false;
      }

      auth_handled = true;

      ret =
         _mongoc_cluster_auth_scram_continue (cluster, stream, handshake_sd, scram, speculative_auth_response, error);
   }
#endif

   if (auth_handled) {
      if (!ret) {
         mongoc_counter_auth_failure_inc ();
         MONGOC_DEBUG ("Speculative authentication failed: %s", error->message);
      } else {
         mongoc_counter_auth_success_inc ();
         TRACE ("%s", "Speculative authentication succeeded");
      }
   }

   bson_reinit (speculative_auth_response);

   return ret;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_add_node --
 *
 *       Add a new node to this cluster for the given server description.
 *
 *       NOTE: does NOT check if this server is already in the cluster.
 *
 * Returns:
 *       A stream connected to the server, or NULL on failure.
 *
 * Side effects:
 *       Adds a cluster node, or sets error on failure.
 *
 *--------------------------------------------------------------------------
 */
static mongoc_cluster_node_t *
_cluster_add_node (mongoc_cluster_t *cluster,
                   const mongoc_topology_description_t *td,
                   uint32_t server_id,
                   bson_error_t *error /* OUT */)
{
   mongoc_host_list_t *host = NULL;
   mongoc_cluster_node_t *cluster_node = NULL;
   mongoc_stream_t *stream;
   mongoc_server_description_t *handshake_sd;
   mongoc_handshake_sasl_supported_mechs_t sasl_supported_mechs;
   mongoc_scram_t scram = {0};
   bson_t speculative_auth_response = BSON_INITIALIZER;

   ENTRY;

   BSON_ASSERT (!cluster->client->topology->single_threaded);

   host = _mongoc_topology_host_by_id (td, server_id, error);

   if (!host) {
      GOTO (error);
   }

   TRACE ("Adding new server to cluster: %s", host->host_and_port);

   stream = _mongoc_client_create_stream (cluster->client, host, error);

   if (!stream) {
      MONGOC_WARNING ("Failed connection to %s (%s)", host->host_and_port, error->message);
      GOTO (error);
      /* TODO CDRIVER-3654: if this is a non-timeout network error and the
       * generation is not stale, mark the server unknown and increment the
       * generation. */
   }

   /* take critical fields from a fresh hello */
   cluster_node = _mongoc_cluster_node_new (stream, host->host_and_port);

   handshake_sd = _cluster_run_hello (cluster, cluster_node, server_id, &scram, &speculative_auth_response, error);
   if (!handshake_sd) {
      GOTO (error);
   }

   _mongoc_handshake_parse_sasl_supported_mechs (&handshake_sd->last_hello_response, &sasl_supported_mechs);

   if (cluster->requires_auth) {
      /* Complete speculative authentication */
      bool is_auth = _mongoc_cluster_finish_speculative_auth (
         cluster, stream, handshake_sd, &speculative_auth_response, &scram, error);

      if (!is_auth &&
          !_mongoc_cluster_auth_node (cluster, cluster_node->stream, handshake_sd, &sasl_supported_mechs, error)) {
         MONGOC_WARNING ("Failed authentication to %s (%s)", host->host_and_port, error->message);
         mongoc_server_description_destroy (handshake_sd);
         GOTO (error);
      }
   }

   /* Transfer ownership of the server description into the cluster node. */
   cluster_node->handshake_sd = handshake_sd;
   /* Copy the latest connection pool generation.
    * TODO (CDRIVER-4078) do not store the generation counter on the server
    * description */
   handshake_sd->generation =
      _mongoc_topology_get_connection_pool_generation (td, server_id, &handshake_sd->service_id);

   bson_destroy (&speculative_auth_response);
   mongoc_set_add (cluster->nodes, server_id, cluster_node);
   _mongoc_host_list_destroy_all (host);

#ifdef MONGOC_ENABLE_CRYPTO
   _mongoc_scram_destroy (&scram);
#endif

   RETURN (cluster_node);

error:
   bson_destroy (&speculative_auth_response);
   _mongoc_host_list_destroy_all (host); /* null ok */

#ifdef MONGOC_ENABLE_CRYPTO
   _mongoc_scram_destroy (&scram);
#endif

   if (cluster_node) {
      _mongoc_cluster_node_destroy (cluster_node); /* also destroys stream */
   }

   RETURN (NULL);
}

static void
node_not_found (const mongoc_topology_description_t *td, uint32_t server_id, bson_error_t *error /* OUT */)
{
   mongoc_server_description_t const *sd;

   if (!error) {
      return;
   }

   sd = mongoc_topology_description_server_by_id_const (td, server_id, error);

   if (!sd) {
      return;
   }

   if (sd->error.code) {
      memcpy (error, &sd->error, sizeof *error);
   } else {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_NOT_ESTABLISHED,
                      "Could not find node %s",
                      sd->host.host_and_port);
   }
}


static void
stream_not_found (const mongoc_topology_description_t *td,
                  uint32_t server_id,
                  const char *connection_address,
                  bson_error_t *error /* OUT */)
{
   mongoc_server_description_t const *sd;

   sd = mongoc_topology_description_server_by_id_const (td, server_id, error);

   if (error) {
      if (sd && sd->error.code) {
         memcpy (error, &sd->error, sizeof *error);
      } else {
         bson_set_error (error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_NOT_ESTABLISHED,
                         "Could not find stream for node %s",
                         connection_address);
      }
   }
}

static mongoc_server_stream_t *
_try_get_server_stream (mongoc_cluster_t *cluster,
                        const mongoc_topology_description_t *td,
                        uint32_t server_id,
                        bool reconnect_ok,
                        bson_error_t *error)
{
   if (cluster->client->topology->single_threaded) {
      /* in the single-threaded use case we share topology's streams */
      return _cluster_fetch_stream_single (cluster, td, server_id, reconnect_ok, error);
   } else {
      return _cluster_fetch_stream_pooled (cluster, td, server_id, reconnect_ok, error);
   }
}

static mongoc_server_stream_t *
_mongoc_cluster_stream_for_server (mongoc_cluster_t *cluster,
                                   uint32_t server_id,
                                   bool reconnect_ok,
                                   const mongoc_client_session_t *cs,
                                   bson_t *reply,
                                   bson_error_t *error /* OUT */)
{
   mongoc_topology_t *const topology = BSON_ASSERT_PTR_INLINE (cluster)->client->topology;
   mongoc_server_stream_t *ret_server_stream;
   bson_error_t err_local;
   /* if fetch_stream fails we need a place to receive error details and pass
    * them to mongoc_topology_description_invalidate_server. */
   bson_error_t *err_ptr = error ? error : &err_local;
   mc_tpld_modification tdmod;
   mc_shared_tpld td;

   ENTRY;

   td = mc_tpld_take_ref (topology);

   ret_server_stream = _try_get_server_stream (cluster, td.ptr, server_id, reconnect_ok, err_ptr);

   if (!ret_server_stream) {
      /* TODO CDRIVER-3654. A null server stream could be due to:
       * 1. Network error during handshake.
       * 2. Failure to retrieve server description (if it was removed from
       * topology).
       * 3. Auth error during handshake.
       * Only (1) should mark the server unknown and clear the pool.
       * Network errors should be checked at a lower layer than this, when an
       * operation on a stream fails, and should take the connection generation
       * into account.
       */

      _mongoc_bson_init_if_set (reply);

      // Add a transient transaction label if applicable.
      _mongoc_add_transient_txn_error (cs, reply);

      /* Update the topology */
      tdmod = mc_tpld_modify_begin (topology);

      /* When establishing a new connection in load balanced mode, drivers MUST
       * NOT perform SDAM error handling for any errors that occur before the
       * MongoDB Handshake. */
      if (tdmod.new_td->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
         mc_tpld_modify_drop (tdmod);
         ret_server_stream = NULL;
         goto done;
      }

      mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, err_ptr);
      mongoc_cluster_disconnect_node (cluster, server_id);
      /* This is not load balanced mode, so there are no service IDs associated
       * with connections. Pass kZeroServiceId to clear the entire connection
       * pool to this server. */
      _mongoc_topology_description_clear_connection_pool (tdmod.new_td, server_id, &kZeroServiceId);

      if (!topology->single_threaded) {
         _mongoc_topology_background_monitoring_cancel_check (topology, server_id);
      }
      mc_tpld_modify_commit (tdmod);
      ret_server_stream = NULL;
      goto done;
   }

   /* If this is a load balanced topology and the server stream does not have a
    * service id, disconnect and return an error. */
   if (td.ptr->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      if (!mongoc_server_description_has_service_id (ret_server_stream->sd)) {
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_INVALID_LOAD_BALANCER,
                         "Driver attempted to initialize in load balancing "
                         "mode, but the server does not support this mode.");
         mongoc_server_stream_cleanup (ret_server_stream);
         mongoc_cluster_disconnect_node (cluster, server_id);
         _mongoc_bson_init_if_set (reply);
         ret_server_stream = NULL;
         goto done;
      }
   }

done:
   mc_tpld_drop_ref (&td);
   RETURN (ret_server_stream);
}


mongoc_server_stream_t *
mongoc_cluster_stream_for_server (mongoc_cluster_t *cluster,
                                  uint32_t server_id,
                                  bool reconnect_ok,
                                  mongoc_client_session_t *cs,
                                  bson_t *reply,
                                  bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT (cs || true);
   BSON_ASSERT (reply || true);
   BSON_ASSERT (error || true);

   ENTRY;

   BSON_ASSERT (cluster);

   if (cs && cs->server_id && cs->server_id != server_id) {
      _mongoc_bson_init_if_set (reply);
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_SERVER_SELECTION_INVALID_ID,
                      "Requested server id does not matched pinned server id");
      RETURN (NULL);
   }


   mongoc_server_stream_t *const server_stream =
      _mongoc_cluster_stream_for_server (cluster, server_id, reconnect_ok, cs, reply, error);

   if (_in_sharded_txn (cs)) {
      _mongoc_client_session_pin (cs, server_id);
   } else {
      /* Transactions Spec: Additionally, any non-transaction operation using
       * a pinned ClientSession MUST unpin the session and the operation MUST
       * perform normal server selection. */
      if (cs && !_mongoc_client_session_in_txn_or_ending (cs)) {
         _mongoc_client_session_unpin (cs);
      }
   }

   RETURN (server_stream);
}


static mongoc_server_stream_t *
_cluster_fetch_stream_single (mongoc_cluster_t *cluster,
                              const mongoc_topology_description_t *td,
                              uint32_t server_id,
                              bool reconnect_ok,
                              bson_error_t *error /* OUT */)
{
   mongoc_server_description_t *handshake_sd;
   mongoc_topology_scanner_node_t *scanner_node;
   char *address;

   scanner_node = mongoc_topology_scanner_get_node (cluster->client->topology->scanner, server_id);
   /* This could happen if a user explicitly passes a bad server id. */
   if (!scanner_node) {
      bson_set_error (
         error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Could not find server with id: %d", server_id);
      return NULL;
   }

   /* Retired scanner nodes are removed at the end of a scan. If the node was
    * retired, that would indicate a bug. */
   if (scanner_node->retired) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Unexpected, selecting server marked for removal: %s",
                      scanner_node->host.host_and_port);
      return NULL;
   }

   if (scanner_node->stream) {
      handshake_sd = mongoc_server_description_new_copy (scanner_node->handshake_sd);
   } else {
      if (!reconnect_ok) {
         stream_not_found (td, server_id, scanner_node->host.host_and_port, error);
         return NULL;
      }

      /* save the scanner node address in case it is removed during the scan. */
      address = bson_strdup (scanner_node->host.host_and_port);
      _mongoc_topology_do_blocking_scan (cluster->client->topology, error);
      if (error->code) {
         bson_free (address);
         return NULL;
      }

      scanner_node = mongoc_topology_scanner_get_node (cluster->client->topology->scanner, server_id);

      if (!scanner_node || !scanner_node->stream) {
         stream_not_found (td, server_id, address, error);
         bson_free (address);
         return NULL;
      }
      bson_free (address);

      handshake_sd = mongoc_server_description_new_copy (scanner_node->handshake_sd);
   }

   if (handshake_sd->type == MONGOC_SERVER_UNKNOWN) {
      *error = handshake_sd->error;
      mongoc_server_description_destroy (handshake_sd);
      return NULL;
   }

   /* stream open but not auth'ed: first use since connect or reconnect */
   if (cluster->requires_auth && !scanner_node->has_auth) {
      /* Complete speculative authentication */
      bool has_speculative_auth = _mongoc_cluster_finish_speculative_auth (cluster,
                                                                           scanner_node->stream,
                                                                           handshake_sd,
                                                                           &scanner_node->speculative_auth_response,
                                                                           &scanner_node->scram,
                                                                           &handshake_sd->error);

#ifdef MONGOC_ENABLE_CRYPTO
      _mongoc_scram_destroy (&scanner_node->scram);
#endif

      if (!scanner_node->stream) {
         *error = handshake_sd->error;
         mongoc_server_description_destroy (handshake_sd);
         return NULL;
      }

      if (!has_speculative_auth &&
          !_mongoc_cluster_auth_node (
             cluster, scanner_node->stream, handshake_sd, &scanner_node->sasl_supported_mechs, &handshake_sd->error)) {
         *error = handshake_sd->error;
         mongoc_server_description_destroy (handshake_sd);
         return NULL;
      }

      scanner_node->has_auth = true;
   }

   /* Copy the latest connection pool generation.
    * TODO (CDRIVER-4078) do not store the generation counter on the server
    * description */
   handshake_sd->generation =
      _mongoc_topology_get_connection_pool_generation (td, server_id, &handshake_sd->service_id);
   return mongoc_server_stream_new (td, handshake_sd, scanner_node->stream);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_stream_valid --
 *
 *       Internal function to determine if @server_stream is valid and
 *       associated with the given cluster.
 *
 * Returns:
 *       true if @server_stream is not NULL, hasn't been freed or changed;
 *       otherwise false.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cluster_stream_valid (mongoc_cluster_t *cluster, mongoc_server_stream_t *server_stream)
{
   mongoc_server_stream_t *tmp_stream = NULL;
   mongoc_topology_t *topology = BSON_ASSERT_PTR_INLINE (cluster)->client->topology;
   const mongoc_server_description_t *sd;
   bool ret = false;
   bson_error_t error;
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   if (!server_stream) {
      goto done;
   }

   tmp_stream = mongoc_cluster_stream_for_server (cluster, server_stream->sd->id, false, NULL, NULL, NULL);
   if (!tmp_stream || tmp_stream->stream != server_stream->stream) {
      /* stream was freed, or has changed. */
      goto done;
   }

   /* Check that the server stream is still valid for the given server, and that
    * the server is still registered. */
   sd = mongoc_topology_description_server_by_id_const (td.ptr, server_stream->sd->id, &error);
   if (!sd || server_stream->sd->generation < _mongoc_topology_get_connection_pool_generation (
                                                 td.ptr, server_stream->sd->id, &server_stream->sd->service_id)) {
      /* No server description, or the pool has been cleared. */
      goto done;
   }

   ret = true;
done:
   mc_tpld_drop_ref (&td);
   mongoc_server_stream_cleanup (tmp_stream);
   return ret;
}

mongoc_server_stream_t *
_mongoc_cluster_create_server_stream (mongoc_topology_description_t const *td,
                                      const mongoc_server_description_t *handshake_sd,
                                      mongoc_stream_t *stream)
{
   mongoc_server_description_t *const sd = mongoc_server_description_new_copy (handshake_sd);
   /* can't just use mongoc_topology_server_by_id(), since we must hold the
    * lock while copying topology->shared_descr.ptr->logical_time below */
   return mongoc_server_stream_new (td, sd, stream);
}


static mongoc_server_stream_t *
_cluster_fetch_stream_pooled (mongoc_cluster_t *cluster,
                              const mongoc_topology_description_t *td,
                              uint32_t server_id,
                              bool reconnect_ok,
                              bson_error_t *error /* OUT */)
{
   mongoc_cluster_node_t *cluster_node;
   mongoc_server_description_t const *sd;
   bool has_server_description = false;

   cluster_node = (mongoc_cluster_node_t *) mongoc_set_get (cluster->nodes, server_id);

   sd = mongoc_topology_description_server_by_id_const (td, server_id, error);
   if (sd) {
      has_server_description = true;
   }

   if (cluster_node) {
      uint32_t connection_pool_generation = 0;
      BSON_ASSERT (cluster_node->stream);

      connection_pool_generation =
         _mongoc_topology_get_connection_pool_generation (td, server_id, &cluster_node->handshake_sd->service_id);

      if (!has_server_description || cluster_node->handshake_sd->generation < connection_pool_generation) {
         /* Since the stream was created, connections to this server were
          * invalidated.
          * This may have happened if:
          * - A background scan removed the server description.
          * - A network error or a "not primary"/"node is recovering" error
          *   occurred on an app connection.
          * - A network error occurred on the monitor connection.
          */
         mongoc_cluster_disconnect_node (cluster, server_id);
      } else {
         return _mongoc_cluster_create_server_stream (td, cluster_node->handshake_sd, cluster_node->stream);
      }
   }

   /* no node, or out of date */
   if (!reconnect_ok) {
      node_not_found (td, server_id, error);
      return NULL;
   }

   cluster_node = _cluster_add_node (cluster, td, server_id, error);
   if (cluster_node) {
      return _mongoc_cluster_create_server_stream (td, cluster_node->handshake_sd, cluster_node->stream);
   } else {
      return NULL;
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_init --
 *
 *       Initializes @cluster using the @uri and @client provided. The
 *       @uri is used to determine the "mode" of the cluster. Based on the
 *       uri we can determine if we are connected to a single host, a
 *       replicaSet, or a shardedCluster.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       @cluster is initialized.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_cluster_init (mongoc_cluster_t *cluster, const mongoc_uri_t *uri, void *client)
{
   ENTRY;

   BSON_ASSERT (cluster);
   BSON_ASSERT (uri);

   memset (cluster, 0, sizeof *cluster);

   cluster->uri = mongoc_uri_copy (uri);
   cluster->client = (mongoc_client_t *) client;
   cluster->requires_auth = (mongoc_uri_get_username (uri) || mongoc_uri_get_auth_mechanism (uri));

   mongoc_cluster_reset_sockettimeoutms (cluster);

   cluster->socketcheckintervalms =
      mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, MONGOC_TOPOLOGY_SOCKET_CHECK_INTERVAL_MS);

   /* TODO for single-threaded case we don't need this */
   cluster->nodes = mongoc_set_new (8, _mongoc_cluster_node_dtor, NULL);

   _mongoc_array_init (&cluster->iov, sizeof (mongoc_iovec_t));
   unsigned int seed = time(NULL);
   cluster->operation_id = rand_r (&seed);

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_destroy --
 *
 *       Clean up after @cluster and destroy all active connections.
 *       All resources for @cluster are released.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Everything.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_cluster_destroy (mongoc_cluster_t *cluster) /* INOUT */
{
   ENTRY;

   BSON_ASSERT (cluster);

   mongoc_uri_destroy (cluster->uri);

   mongoc_set_destroy (cluster->nodes);

   _mongoc_array_destroy (&cluster->iov);

   EXIT;
}

void
mongoc_cluster_set_sockettimeoutms (mongoc_cluster_t *cluster, int32_t timeoutms)
{
   BSON_ASSERT_PARAM (cluster);
   cluster->sockettimeoutms = timeoutms;
}

void
mongoc_cluster_reset_sockettimeoutms (mongoc_cluster_t *cluster)
{
   BSON_ASSERT_PARAM (cluster);
   cluster->sockettimeoutms =
      mongoc_uri_get_option_as_int32 (cluster->uri, MONGOC_URI_SOCKETTIMEOUTMS, MONGOC_DEFAULT_SOCKETTIMEOUTMS);
}

static uint32_t
_mongoc_cluster_select_server_id (mongoc_client_session_t *cs,
                                  mongoc_topology_t *topology,
                                  mongoc_ss_optype_t optype,
                                  const mongoc_read_prefs_t *read_prefs,
                                  bool *must_use_primary,
                                  const mongoc_deprioritized_servers_t *ds,
                                  bson_error_t *error)
{
   BSON_ASSERT (cs || true);
   BSON_ASSERT_PARAM (topology);
   BSON_ASSERT (read_prefs || true);
   BSON_ASSERT_PARAM (must_use_primary);
   BSON_ASSERT (error || true);

   uint32_t server_id;

   if (_in_sharded_txn (cs)) {
      server_id = cs->server_id;
      if (!server_id) {
         server_id = mongoc_topology_select_server_id (topology, optype, read_prefs, must_use_primary, ds, error);
         if (server_id) {
            _mongoc_client_session_pin (cs, server_id);
         }
      }
   } else {
      server_id = mongoc_topology_select_server_id (topology, optype, read_prefs, must_use_primary, ds, error);
      /* Transactions Spec: Additionally, any non-transaction operation using a
       * pinned ClientSession MUST unpin the session and the operation MUST
       * perform normal server selection. */
      if (cs && !_mongoc_client_session_in_txn_or_ending (cs)) {
         _mongoc_client_session_unpin (cs);
      }
   }

   return server_id;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_stream_for_optype --
 *
 *       Internal server selection.
 *
 * Returns:
 *       A mongoc_server_stream_t on which you must call
 *       mongoc_server_stream_cleanup, or NULL on failure (sets @error)
 *
 * Side effects:
 *       May add or disconnect nodes in @cluster->nodes.
 *       Sets @error and initializes @reply on error.
 *
 *--------------------------------------------------------------------------
 */

static mongoc_server_stream_t *
_mongoc_cluster_stream_for_optype (mongoc_cluster_t *cluster,
                                   mongoc_ss_optype_t optype,
                                   const mongoc_read_prefs_t *read_prefs,
                                   mongoc_client_session_t *cs,
                                   bool is_retryable,
                                   const mongoc_deprioritized_servers_t *ds,
                                   bson_t *reply,
                                   bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT (read_prefs || true);
   BSON_ASSERT (cs || true);
   BSON_ASSERT (reply || true);
   BSON_ASSERT (error || true);

   mongoc_server_stream_t *server_stream;
   uint32_t server_id;
   mongoc_topology_t *topology = cluster->client->topology;
   bool must_use_primary = false;

   ENTRY;

   BSON_ASSERT (cluster);

   server_id = _mongoc_cluster_select_server_id (cs, topology, optype, read_prefs, &must_use_primary, ds, error);

   if (!server_id) {
      if (reply) {
         bson_init (reply);
         _mongoc_add_transient_txn_error (cs, reply);
      }
      RETURN (NULL);
   }

   if (!mongoc_cluster_check_interval (cluster, server_id)) {
      /* Server Selection Spec: try once more */
      server_id = _mongoc_cluster_select_server_id (cs, topology, optype, read_prefs, &must_use_primary, ds, error);

      if (!server_id) {
         if (reply) {
            bson_init (reply);
            _mongoc_add_transient_txn_error (cs, reply);
         }
         RETURN (NULL);
      }
   }

   bson_t first_reply;
   bson_error_t first_error = {0};

   server_stream =
      _mongoc_cluster_stream_for_server (cluster, server_id, true /* reconnect_ok */, cs, &first_reply, &first_error);

   if (server_stream) {
      server_stream->must_use_primary = must_use_primary;
      RETURN (server_stream);
   }

   // Important: authentication errors are also considered retryable even if
   // they not considered a network error.
   const bool retryable_error = _mongoc_error_is_network (&first_error) || _mongoc_error_is_auth (&first_error);

   if (is_retryable && retryable_error) {
      bson_t retry_reply;
      bson_error_t retry_error = {0};

      server_stream = _mongoc_cluster_stream_for_server (
         cluster, server_id, true /* reconnect_ok */, cs, &retry_reply, &retry_error);

      if (server_stream) {
         server_stream->must_use_primary = must_use_primary;
         server_stream->retry_attempted = true;
         bson_destroy (&first_reply);
         RETURN (server_stream);
      }

      if (optype != MONGOC_SS_READ) {
         // Retryable Writes Spec: When the driver encounters a network error
         // establishing an initial connection to a server, it MUST add a
         // RetryableWriteError label to that error if the MongoClient
         // performing the operation has the retryWrites configuration option
         // set to true.
         _mongoc_write_error_append_retryable_label (&first_reply);
      }

      bson_destroy (&retry_reply);
   }

   // Retryable Writes Spec: If the driver cannot select a server for the retry
   // attempt [...], retrying is not possible and drivers MUST raise the
   // original retryable error.
   {
      if (reply) {
         bson_copy_to (&first_reply, reply);
      }

      bson_destroy (&first_reply);

      if (error) {
         *error = first_error;
      }
   }

   RETURN (NULL);
}

mongoc_server_stream_t *
mongoc_cluster_stream_for_reads (mongoc_cluster_t *cluster,
                                 const mongoc_read_prefs_t *read_prefs,
                                 mongoc_client_session_t *cs,
                                 const mongoc_deprioritized_servers_t *ds,
                                 bson_t *reply,
                                 bson_error_t *error)
{
   const mongoc_read_prefs_t *const prefs_override =
      _mongoc_client_session_in_txn (cs) ? cs->txn.opts.read_prefs : read_prefs;

   // Retryable Reads Spec: This boolean option determines whether retryable
   // behavior will be applied to all read operations executed within the
   // MongoClient.
   const bool is_retryable =
      mongoc_uri_get_option_as_bool (cluster->uri, MONGOC_URI_RETRYREADS, MONGOC_DEFAULT_RETRYREADS);

   return _mongoc_cluster_stream_for_optype (
      cluster, MONGOC_SS_READ, prefs_override, cs, is_retryable, ds, reply, error);
}

mongoc_server_stream_t *
mongoc_cluster_stream_for_writes (mongoc_cluster_t *cluster,
                                  mongoc_client_session_t *cs,
                                  const mongoc_deprioritized_servers_t *ds,
                                  bson_t *reply,
                                  bson_error_t *error)
{
   const bool is_retryable =
      mongoc_uri_get_option_as_bool (cluster->uri, MONGOC_URI_RETRYWRITES, MONGOC_DEFAULT_RETRYWRITES);

   return _mongoc_cluster_stream_for_optype (cluster, MONGOC_SS_WRITE, NULL, cs, is_retryable, ds, reply, error);
}

mongoc_server_stream_t *
mongoc_cluster_stream_for_aggr_with_write (mongoc_cluster_t *cluster,
                                           const mongoc_read_prefs_t *read_prefs,
                                           mongoc_client_session_t *cs,
                                           bson_t *reply,
                                           bson_error_t *error)
{
   const mongoc_read_prefs_t *const prefs_override =
      _mongoc_client_session_in_txn (cs) ? cs->txn.opts.read_prefs : read_prefs;

   const bool is_retryable =
      mongoc_uri_get_option_as_bool (cluster->uri, MONGOC_URI_RETRYWRITES, MONGOC_DEFAULT_RETRYWRITES);

   return _mongoc_cluster_stream_for_optype (
      cluster, MONGOC_SS_AGGREGATE_WITH_WRITE, prefs_override, cs, is_retryable, NULL, reply, error);
}

static bool
_mongoc_cluster_min_of_max_obj_size_sds (const void *item, void *ctx)
{
   const mongoc_server_description_t *sd = item;
   int32_t *current_min = (int32_t *) ctx;

   if (sd->max_bson_obj_size < *current_min) {
      *current_min = sd->max_bson_obj_size;
   }
   return true;
}

static bool
_mongoc_cluster_min_of_max_obj_size_nodes (void *item, void *ctx)
{
   mongoc_cluster_node_t *node = (mongoc_cluster_node_t *) item;
   int32_t *current_min = (int32_t *) ctx;

   if (node->handshake_sd->max_bson_obj_size < *current_min) {
      *current_min = node->handshake_sd->max_bson_obj_size;
   }
   return true;
}

static bool
_mongoc_cluster_min_of_max_msg_size_sds (const void *item, void *ctx)
{
   const mongoc_server_description_t *sd = item;
   int32_t *current_min = (int32_t *) ctx;

   if (sd->max_msg_size < *current_min) {
      *current_min = sd->max_msg_size;
   }
   return true;
}

static bool
_mongoc_cluster_min_of_max_msg_size_nodes (void *item, void *ctx)
{
   mongoc_cluster_node_t *node = (mongoc_cluster_node_t *) item;
   int32_t *current_min = (int32_t *) ctx;

   if (node->handshake_sd->max_msg_size < *current_min) {
      *current_min = node->handshake_sd->max_msg_size;
   }
   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_get_max_bson_obj_size --
 *
 *      Return the minimum max_bson_obj_size across all servers in cluster.
 *
 * Returns:
 *      The minimum max_bson_obj_size.
 *
 * Side effects:
 *      None
 *
 *--------------------------------------------------------------------------
 */
int32_t
mongoc_cluster_get_max_bson_obj_size (mongoc_cluster_t *cluster)
{
   int32_t max_bson_obj_size = -1;

   max_bson_obj_size = MONGOC_DEFAULT_BSON_OBJ_SIZE;

   if (!cluster->client->topology->single_threaded) {
      mongoc_set_for_each (cluster->nodes, _mongoc_cluster_min_of_max_obj_size_nodes, &max_bson_obj_size);
   } else {
      mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (cluster)->client->topology);
      mongoc_set_for_each_const (
         mc_tpld_servers_const (td.ptr), _mongoc_cluster_min_of_max_obj_size_sds, &max_bson_obj_size);
      mc_tpld_drop_ref (&td);
   }

   return max_bson_obj_size;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_get_max_msg_size --
 *
 *      Return the minimum max msg size across all servers in cluster.
 *
 * Returns:
 *      The minimum max_msg_size
 *
 * Side effects:
 *      None
 *
 *--------------------------------------------------------------------------
 */
int32_t
mongoc_cluster_get_max_msg_size (mongoc_cluster_t *cluster)
{
   int32_t max_msg_size = MONGOC_DEFAULT_MAX_MSG_SIZE;

   if (!cluster->client->topology->single_threaded) {
      mongoc_set_for_each (cluster->nodes, _mongoc_cluster_min_of_max_msg_size_nodes, &max_msg_size);
   } else {
      mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (cluster)->client->topology);
      mongoc_set_for_each_const (
         mc_tpld_servers_const (td.ptr), _mongoc_cluster_min_of_max_msg_size_sds, &max_msg_size);
      mc_tpld_drop_ref (&td);
   }

   return max_msg_size;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_cluster_check_interval --
 *
 *      Server Selection Spec:
 *
 *      Only for single-threaded drivers.
 *
 *      If a server is selected that has an existing connection that has been
 *      idle for socketCheckIntervalMS, the driver MUST check the connection
 *      with the "ping" command. If the ping succeeds, use the selected
 *      connection. If not, set the server's type to Unknown and update the
 *      Topology Description according to the Server Discovery and Monitoring
 *      Spec, and attempt once more to select a server.
 *
 * Returns:
 *      True if the check succeeded or no check was required, false if the
 *      check failed.
 *
 * Side effects:
 *      If a check fails, closes stream and may set server type Unknown.
 *
 *--------------------------------------------------------------------------
 */

bool
mongoc_cluster_check_interval (mongoc_cluster_t *cluster, uint32_t server_id)
{
   mongoc_cmd_parts_t parts;
   mongoc_topology_t *topology;
   mongoc_topology_scanner_node_t *scanner_node;
   mongoc_stream_t *stream;
   int64_t now;
   bson_t command;
   bson_error_t error;
   bool r = true;
   mongoc_server_stream_t *server_stream;
   mongoc_server_description_t *handshake_sd;

   topology = cluster->client->topology;

   if (!topology->single_threaded) {
      return true;
   }

   scanner_node = mongoc_topology_scanner_get_node (topology->scanner, server_id);

   if (!scanner_node) {
      return false;
   }

   BSON_ASSERT (!scanner_node->retired);

   stream = scanner_node->stream;

   if (!stream) {
      return false;
   }

   handshake_sd = scanner_node->handshake_sd;
   BSON_ASSERT (handshake_sd);

   now = bson_get_monotonic_time ();

   if (scanner_node->last_used + (1000 * CHECK_CLOSED_DURATION_MSEC) < now) {
      if (mongoc_stream_check_closed (stream)) {
         mc_tpld_modification tdmod;
         bson_set_error (&error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "connection closed");
         mongoc_cluster_disconnect_node (cluster, server_id);
         tdmod = mc_tpld_modify_begin (topology);
         /* invalidate_server() is okay if 'server_id' was already removed. */
         mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, &error);
         mc_tpld_modify_commit (tdmod);
         return false;
      }
   }

   if (scanner_node->last_used + (1000 * cluster->socketcheckintervalms) < now) {
      mc_shared_tpld td;

      bson_init (&command);
      BSON_APPEND_INT32 (&command, "ping", 1);
      mongoc_cmd_parts_init (&parts, cluster->client, "admin", MONGOC_QUERY_SECONDARY_OK, &command);
      parts.prohibit_lsid = true;

      td = mc_tpld_take_ref (cluster->client->topology);
      server_stream = _mongoc_cluster_create_server_stream (td.ptr, handshake_sd, stream);
      mc_tpld_drop_ref (&td);

      if (!server_stream) {
         bson_destroy (&command);
         return false;
      }
      r = mongoc_cluster_run_command_parts (cluster, server_stream, &parts, NULL, &error);

      mongoc_server_stream_cleanup (server_stream);
      bson_destroy (&command);

      if (!r) {
         mc_tpld_modification tdmod;
         mongoc_cluster_disconnect_node (cluster, server_id);
         tdmod = mc_tpld_modify_begin (cluster->client->topology);
         /* invalidate_server() is okay if 'server_id' was already removed. */
         mongoc_topology_description_invalidate_server (tdmod.new_td, server_id, &error);
         mc_tpld_modify_commit (tdmod);
      }
   }

   return r;
}


bool
mongoc_cluster_legacy_rpc_sendv_to_server (mongoc_cluster_t *cluster,
                                           mcd_rpc_message *rpc,
                                           mongoc_server_stream_t *server_stream,
                                           bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (server_stream);
   BSON_ASSERT_PARAM (error);

   ENTRY;

   bool ret = false;

   void *compressed_data = NULL;
   size_t compressed_data_len = 0u;
   mongoc_iovec_t *iovecs = NULL;
   size_t num_iovecs = 0u;

   if (cluster->client->in_exhaust) {
      bson_set_error (
         error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_IN_EXHAUST, "a cursor derived from this client is in exhaust");
      GOTO (done);
   }

   const int32_t compressor_id = mongoc_server_description_compressor_id (server_stream->sd);

   if (compressor_id != -1 && !mcd_rpc_message_compress (rpc,
                                                         compressor_id,
                                                         _compression_level_from_uri (compressor_id, cluster->uri),
                                                         &compressed_data,
                                                         &compressed_data_len,
                                                         error)) {
      GOTO (done);
   }

   const uint32_t server_id = server_stream->sd->id;
   const int32_t max_msg_size = mongoc_server_stream_max_msg_size (server_stream);

   const int32_t message_length = mcd_rpc_header_get_message_length (rpc);

   if (message_length > max_msg_size) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_TOO_BIG,
                      "attempted to send an RPC message with length %" PRId32
                      " which exceeds the maximum allowed length %" PRId32,
                      message_length,
                      max_msg_size);
      GOTO (done);
   }

   iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   mcd_rpc_message_egress (rpc);
   if (!_mongoc_stream_writev_full (server_stream->stream, iovecs, num_iovecs, cluster->sockettimeoutms, error)) {
      GOTO (done);
   }

   _mongoc_topology_update_last_used (cluster->client->topology, server_id);

   ret = true;

done:
   bson_free (iovecs);
   bson_free (compressed_data);

   RETURN (ret);
}


bool
mongoc_cluster_try_recv (mongoc_cluster_t *cluster,
                         mcd_rpc_message *rpc,
                         mongoc_buffer_t *buffer,
                         mongoc_server_stream_t *server_stream,
                         bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (server_stream);
   BSON_ASSERT_PARAM (error);

   ENTRY;

   bool ret = false;

   TRACE ("Waiting for reply from server_id \"%u\"", server_stream->sd->id);

   const size_t offset = buffer->len;

   if (!_mongoc_buffer_append_from_stream (
          buffer, server_stream->stream, sizeof (int32_t), cluster->sockettimeoutms, error)) {
      MONGOC_DEBUG ("could not read message length, stream probably closed or timed out");
      mongoc_counter_protocol_ingress_error_inc ();
      _handle_network_error (cluster, server_stream, error);
      GOTO (done);
   }

   const int32_t message_length = _int32_from_le (buffer->data + offset);

   const int32_t max_msg_size = mongoc_server_stream_max_msg_size (server_stream);

   if (message_length < message_header_length || message_length > max_msg_size) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "message length %" PRId32 " is not within valid range of %" PRId32 "-%" PRId32 " bytes",
                      message_header_length,
                      message_length,
                      server_stream->sd->max_msg_size);
      _handle_network_error (cluster, server_stream, error);
      mongoc_counter_protocol_ingress_error_inc ();
      GOTO (done);
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   if (!_mongoc_buffer_append_from_stream (
          buffer, server_stream->stream, remaining_bytes, cluster->sockettimeoutms, error)) {
      _handle_network_error (cluster, server_stream, error);
      mongoc_counter_protocol_ingress_error_inc ();
      GOTO (done);
   }

   if (!mcd_rpc_message_from_data_in_place (rpc, buffer->data + offset, (size_t) message_length, NULL)) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "failed to decode reply from server");
      _handle_network_error (cluster, server_stream, error);
      mongoc_counter_protocol_ingress_error_inc ();
      GOTO (done);
   }

   mcd_rpc_message_ingress (rpc);

   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "could not decompress server reply");
      GOTO (done);
   }

   if (decompressed_data) {
      _mongoc_buffer_destroy (buffer);
      _mongoc_buffer_init (buffer, decompressed_data, decompressed_data_len, NULL, NULL);
   }

   ret = true;

done:

   return ret;
}


static void
network_error_reply (bson_t *reply, const mongoc_cmd_t *cmd)
{
   bson_array_builder_t *labels;

   if (reply) {
      bson_init (reply);
   }

   if (cmd->session) {
      if (cmd->session->server_session) {
         cmd->session->server_session->dirty = true;
      }
      /* Transactions Spec defines TransientTransactionError: "Any
       * network error or server selection error encountered running any
       * command besides commitTransaction in a transaction. In the case
       * of command errors, the server adds the label; in the case of
       * network errors or server selection errors where the client
       * receives no server reply, the client adds the label." */
      if (_mongoc_client_session_in_txn (cmd->session) && !cmd->is_txn_finish) {
         /* Transaction Spec: "Drivers MUST unpin a ClientSession when a command
          * within a transaction, including commitTransaction and
          * abortTransaction,
          * fails with a TransientTransactionError". If we're about to add
          * a TransientTransactionError label due to a client side error then we
          * unpin. If commitTransaction/abortTransation includes a label in the
          * server reply, we unpin in _mongoc_client_session_handle_reply. */
         cmd->session->server_id = 0;
         if (!reply) {
            return;
         }

         BSON_APPEND_ARRAY_BUILDER_BEGIN (reply, "errorLabels", &labels);
         bson_array_builder_append_utf8 (labels, TRANSIENT_TXN_ERR, -1);
         bson_append_array_builder_end (reply, labels);
      }
   }
}


static bool
_mongoc_cluster_run_opmsg_send (
   mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, mcd_rpc_message *rpc, bson_t *reply, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT_PARAM (error);

   mongoc_server_stream_t *const server_stream = cmd->server_stream;

   const uint32_t flags = (cmd->is_acknowledged ? MONGOC_OP_MSG_FLAG_NONE : MONGOC_OP_MSG_FLAG_MORE_TO_COME) |
                          (cmd->op_msg_is_exhaust ? MONGOC_OP_MSG_FLAG_EXHAUST_ALLOWED : MONGOC_OP_MSG_FLAG_NONE);

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, ++cluster->request_id);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG);

      BSON_ASSERT (cmd->payloads_count <= MONGOC_CMD_PAYLOADS_COUNT_MAX);
      // Reserve one section for the body (kind 0) and any needed sections for document sequences (kind 1)
      mcd_rpc_op_msg_set_sections_count (rpc, 1u + cmd->payloads_count);

      message_length += mcd_rpc_op_msg_set_flag_bits (rpc, flags);
      message_length += mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0);
      message_length += mcd_rpc_op_msg_section_set_body (rpc, 0u, bson_get_data (cmd->command));

      for (size_t i = 0; i < cmd->payloads_count; i++) {
         const mongoc_cmd_payload_t payload = cmd->payloads[i];

         BSON_ASSERT (bson_in_range_signed (size_t, payload.size));

         const size_t section_length = sizeof (int32_t) + strlen (payload.identifier) + 1u + (size_t) payload.size;
         BSON_ASSERT (bson_in_range_unsigned (int32_t, section_length));

         size_t section_idx = 1u + i;
         message_length += mcd_rpc_op_msg_section_set_kind (rpc, section_idx, 1);
         message_length += mcd_rpc_op_msg_section_set_length (rpc, section_idx, (int32_t) section_length);
         message_length += mcd_rpc_op_msg_section_set_identifier (rpc, section_idx, payload.identifier);
         message_length +=
            mcd_rpc_op_msg_section_set_document_sequence (rpc, section_idx, payload.documents, (size_t) payload.size);
      }

      mcd_rpc_message_set_length (rpc, message_length);
   }

   void *compressed_data = NULL;
   size_t compressed_data_len = 0u;

   if (mongoc_cmd_is_compressible (cmd)) {
      const int32_t compressor_id = mongoc_server_description_compressor_id (server_stream->sd);

      TRACE ("Function '%s' is compressible: %d", cmd->command_name, compressor_id);

      if (compressor_id != -1 && !mcd_rpc_message_compress (rpc,
                                                            compressor_id,
                                                            _compression_level_from_uri (compressor_id, cluster->uri),
                                                            &compressed_data,
                                                            &compressed_data_len,
                                                            error)) {
         RUN_CMD_ERR_DECORATE;
         _handle_network_error (cluster, server_stream, error);
         server_stream->stream = NULL;
         network_error_reply (reply, cmd);
         return false;
      }
   }

   size_t num_iovecs = 0u;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   mcd_rpc_message_egress (rpc);
   const bool res =
      _mongoc_stream_writev_full (server_stream->stream, iovecs, num_iovecs, cluster->sockettimeoutms, error);

   if (!res) {
      RUN_CMD_ERR_DECORATE;
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
   }

   bson_free (iovecs);
   bson_free (compressed_data);

   return res;
}

static bool
_mongoc_cluster_run_opmsg_recv (
   mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, mcd_rpc_message *rpc, bson_t *reply, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT_PARAM (error);

   bool ret = false;

   mongoc_server_stream_t *const server_stream = cmd->server_stream;

   mongoc_buffer_t buffer;
   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);

   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_stream->stream, sizeof (int32_t), cluster->sockettimeoutms, error)) {
      MONGOC_DEBUG ("could not read message length, stream probably closed or timed out");
      RUN_CMD_ERR_DECORATE;
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      goto done;
   }

   const int32_t message_length = _int32_from_le (buffer.data);

   if (message_length < message_header_length || message_length > server_stream->sd->max_msg_size) {
      RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL,
                   MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                   "message length %" PRId32 " is not within valid range of %" PRId32 "-%" PRId32 " bytes",
                   message_header_length,
                   message_length,
                   server_stream->sd->max_msg_size);
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      goto done;
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_stream->stream, remaining_bytes, cluster->sockettimeoutms, error)) {
      RUN_CMD_ERR_DECORATE;
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      goto done;
   }

   if (!mcd_rpc_message_from_data_in_place (rpc, buffer.data, buffer.len, NULL)) {
      RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "malformed server message");
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      goto done;
   }
   mcd_rpc_message_ingress (rpc);

   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "could not decompress message from server");
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      GOTO (done);
   }

   if (decompressed_data) {
      _mongoc_buffer_destroy (&buffer);
      _mongoc_buffer_init (&buffer, decompressed_data, decompressed_data_len, NULL, NULL);
   }

   // CDRIVER-5584
   {
      const int32_t op_code = mcd_rpc_header_get_op_code (rpc);

      if (op_code != MONGOC_OP_CODE_MSG) {
         RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "malformed message from server: expected opCode %" PRId32 ", got %" PRId32,
                      MONGOC_OP_CODE_MSG,
                      op_code);
         _handle_network_error (cluster, server_stream, error);
         server_stream->stream = NULL;
         network_error_reply (reply, cmd);
         goto done;
      }
   }

   bson_t body;

   if (!mcd_rpc_message_get_body (rpc, &body)) {
      RUN_CMD_ERR (MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "malformed message from server");
      _handle_network_error (cluster, server_stream, error);
      server_stream->stream = NULL;
      network_error_reply (reply, cmd);
      goto done;
   }

   cluster->client->in_exhaust = (mcd_rpc_op_msg_get_flag_bits (rpc) & MONGOC_OP_MSG_FLAG_MORE_TO_COME) != 0u;
   _mongoc_topology_update_cluster_time (cluster->client->topology, &body);

   ret = _mongoc_cmd_check_ok (&body, cluster->client->error_api_version, error);

   if (cmd->session) {
      _mongoc_client_session_handle_reply (cmd->session, cmd->is_acknowledged, cmd->command_name, &body);
   }

   bson_copy_to (&body, reply);
   bson_destroy (&body);

done:
   _mongoc_buffer_destroy (&buffer);

   return ret;
}

static bool
mongoc_cluster_run_opmsg (mongoc_cluster_t *cluster, const mongoc_cmd_t *cmd, bson_t *reply, bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT_PARAM (error);

   if (!cmd->command_name) {
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "empty command document");
      bson_init (reply);
      return false;
   }

   if (!cmd->op_msg_is_exhaust && cluster->client->in_exhaust) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_IN_EXHAUST,
                      "another cursor derived from this client is in exhaust");
      bson_init (reply);
      return false;
   }

   bool ret = false;

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   if (!cluster->client->in_exhaust && !_mongoc_cluster_run_opmsg_send (cluster, cmd, rpc, reply, error)) {
      goto done;
   }

   if (!cmd->is_acknowledged) {
      // Nothing more to do.
      bson_init (reply);
      ret = true;
      goto done;
   }

   mcd_rpc_message_reset (rpc);

   if (!_mongoc_cluster_run_opmsg_recv (cluster, cmd, rpc, reply, error)) {
      goto done;
   }

   ret = true;

done:
   mcd_rpc_message_destroy (rpc);

   return ret;
}


bool
mcd_rpc_message_compress (mcd_rpc_message *rpc,
                          int32_t compressor_id,
                          int32_t compression_level,
                          void **data,
                          size_t *data_len,
                          bson_error_t *error)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT_PARAM (data_len);

   bool ret = false;

   char *uncompressed_message = NULL;
   char *compressed_message = NULL;
   mongoc_iovec_t *iovecs = NULL;

   const int32_t original_message_length = mcd_rpc_header_get_message_length (rpc);

   // msgHeader consists of four int32 fields.
   const int32_t message_header_length = 4u * sizeof (int32_t);

   // compressedMessage does not include msgHeader fields.
   BSON_ASSERT (original_message_length >= message_header_length);
   const size_t uncompressed_size = (size_t) (original_message_length - message_header_length);
   BSON_ASSERT (bson_in_range_unsigned (int32_t, uncompressed_size));

   const size_t estimated_compressed_size = mongoc_compressor_max_compressed_length (compressor_id, uncompressed_size);

   if (estimated_compressed_size == 0u) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Could not determine compression bounds for %s",
                      mongoc_compressor_id_to_name (compressor_id));
      goto fail;
   }

   // Store values before they are converted to little endian.
   const int32_t request_id = mcd_rpc_header_get_request_id (rpc);
   const int32_t response_to = mcd_rpc_header_get_response_to (rpc);
   const int32_t op_code = mcd_rpc_header_get_op_code (rpc);

   size_t num_iovecs;
   iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   uncompressed_message = bson_malloc (uncompressed_size);
   BSON_ASSERT (_mongoc_cluster_buffer_iovec (iovecs, num_iovecs, message_header_length, uncompressed_message) ==
                uncompressed_size);

   compressed_message = bson_malloc (estimated_compressed_size);

   // This value may be passed as an argument to an in-out parameter depending
   // on the compressor, not just an out-parameter.
   size_t compressed_size = estimated_compressed_size;

   if (!mongoc_compress (compressor_id,
                         compression_level,
                         uncompressed_message,
                         uncompressed_size,
                         compressed_message,
                         &compressed_size)) {
      MONGOC_WARNING ("Could not compress data with %s", mongoc_compressor_id_to_name (compressor_id));
      goto fail;
   }

   mcd_rpc_message_reset (rpc);

   {
      int32_t message_len = 0;

      message_len += mcd_rpc_header_set_message_length (rpc, 0);
      message_len += mcd_rpc_header_set_request_id (rpc, request_id);
      message_len += mcd_rpc_header_set_response_to (rpc, response_to);
      message_len += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_COMPRESSED);
      message_len += mcd_rpc_op_compressed_set_original_opcode (rpc, op_code);
      message_len += mcd_rpc_op_compressed_set_uncompressed_size (rpc, (int32_t) uncompressed_size);
      message_len += mcd_rpc_op_compressed_set_compressor_id (rpc, (uint8_t) compressor_id);
      message_len += mcd_rpc_op_compressed_set_compressed_message (rpc, compressed_message, compressed_size);

      mcd_rpc_message_set_length (rpc, message_len);
   }

   *data = compressed_message;
   *data_len = compressed_size;
   compressed_message = NULL;

   ret = true;

fail:
   bson_free (compressed_message);
   bson_free (uncompressed_message);
   bson_free (iovecs);

   return ret;
}

bool
mcd_rpc_message_decompress (mcd_rpc_message *rpc, void **data, size_t *data_len)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT_PARAM (data_len);

   BSON_ASSERT (mcd_rpc_header_get_op_code (rpc) == MONGOC_OP_CODE_COMPRESSED);

   // msgHeader consists of four int32 fields.
   const size_t message_header_length = 4u * sizeof (int32_t);

   const size_t uncompressed_size = (size_t) mcd_rpc_op_compressed_get_uncompressed_size (rpc);

   // uncompressedSize does not include msgHeader fields.
   const size_t original_message_length = message_header_length + uncompressed_size;
   uint8_t *const ptr = bson_malloc (original_message_length);

   const int32_t message_length = original_message_length;
   const int32_t request_id = mcd_rpc_header_get_request_id (rpc);
   const int32_t response_to = mcd_rpc_header_get_response_to (rpc);
   const int32_t op_code = mcd_rpc_op_compressed_get_original_opcode (rpc);

   // Populate the msgHeader fields.
   {
      uint32_t storage;

      memcpy (&storage, &message_length, sizeof (storage));
      storage = BSON_UINT32_TO_LE (storage);
      memcpy (ptr + 0, &storage, sizeof (storage));

      memcpy (&storage, &request_id, sizeof (storage));
      storage = BSON_UINT32_TO_LE (storage);
      memcpy (ptr + 4, &storage, sizeof (storage));

      memcpy (&storage, &response_to, sizeof (storage));
      storage = BSON_UINT32_TO_LE (storage);
      memcpy (ptr + 8, &storage, sizeof (storage));

      memcpy (&storage, &op_code, sizeof (storage));
      storage = BSON_UINT32_TO_LE (storage);
      memcpy (ptr + 12, &storage, sizeof (storage));
   }

   // This value may be passed as an argument to an in-out parameter depending
   // on the compressor, not just an out-parameter.
   size_t actual_uncompressed_size = uncompressed_size;

   // Populate the rest of the uncompressed message.
   if (!mongoc_uncompress (mcd_rpc_op_compressed_get_compressor_id (rpc),
                           mcd_rpc_op_compressed_get_compressed_message (rpc),
                           mcd_rpc_op_compressed_get_compressed_message_length (rpc),
                           ptr + message_header_length,
                           &actual_uncompressed_size)) {
      bson_free (ptr);
      return false;
   }

   BSON_ASSERT (uncompressed_size == actual_uncompressed_size);

   *data_len = original_message_length;
   *data = ptr; // Ownership transfer.

   mcd_rpc_message_reset (rpc);

   return mcd_rpc_message_from_data_in_place (rpc, *data, *data_len, NULL);
}

bool
mcd_rpc_message_decompress_if_necessary (mcd_rpc_message *rpc, void **data, size_t *data_len)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT_PARAM (data_len);

   if (mcd_rpc_header_get_op_code (rpc) != MONGOC_OP_CODE_COMPRESSED) {
      // Nothing to do.
      *data = NULL;
      *data_len = 0u;
      return true;
   }

   return mcd_rpc_message_decompress (rpc, data, data_len);
}

bool
mongoc_cluster_run_retryable_write (mongoc_cluster_t *cluster,
                                    mongoc_cmd_t *cmd,
                                    bool is_retryable_write,
                                    mongoc_server_stream_t **retry_server_stream,
                                    bson_t *reply,
                                    bson_error_t *error)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (retry_server_stream);
   BSON_ASSERT_PARAM (reply);
   BSON_ASSERT (error || true);

   bool ret;
   // `can_retry` is set to false on retry. A retry may only happen once.
   bool can_retry = is_retryable_write;

   // Increment the transaction number for the first attempt of each retryable write command.
   if (is_retryable_write) {
      bson_iter_t txn_number_iter;
      BSON_ASSERT (bson_iter_init_find (&txn_number_iter, cmd->command, "txnNumber"));
      bson_iter_overwrite_int64 (&txn_number_iter, ++cmd->session->server_session->txn_number);
   }

   // Store the original error and reply if needed.
   struct {
      bson_t reply;
      bson_error_t error;
      bool set;
   } original_error = {.reply = {0}, .error = {0}, .set = false};

   // Ensure `*retry_server_stream` is always valid or null.
   *retry_server_stream = NULL;

retry:
   ret = mongoc_cluster_run_command_monitored (cluster, cmd, reply, error);

   if (is_retryable_write) {
      _mongoc_write_error_handle_labels (ret, error, reply, cmd->server_stream->sd);
      _mongoc_write_error_update_if_unsupported_storage_engine (ret, error, reply);
   }

   // If a retryable error is encountered and the write is retryable, select a new writable stream and retry. If server
   // selection fails or the selected server does not support retryable writes, fall through and allow the original
   // error to be reported.
   if (can_retry && _mongoc_write_error_get_type (reply) == MONGOC_WRITE_ERR_RETRY) {
      bson_error_t ignored_error;

      can_retry = false; // Only retry once.

      // Select a server.
      {
         mongoc_deprioritized_servers_t *const ds = mongoc_deprioritized_servers_new ();

         // If talking to a sharded cluster, deprioritize the just-used mongos to prefer a new mongos for the retry.
         mongoc_deprioritized_servers_add_if_sharded (ds, cmd->server_stream->topology_type, cmd->server_stream->sd);

         *retry_server_stream =
            mongoc_cluster_stream_for_writes (cluster, cmd->session, ds, NULL /* reply */, &ignored_error);

         mongoc_deprioritized_servers_destroy (ds);
      }

      if (*retry_server_stream) {
         cmd->server_stream = *retry_server_stream; // Non-owning.
         {
            // Store the original error and reply before retry.
            BSON_ASSERT (!original_error.set); // Retry only happens once.
            original_error.set = true;
            bson_copy_to (reply, &original_error.reply);
            if (error) {
               original_error.error = *error;
            }
         }
         bson_destroy (reply);
         GOTO (retry);
      }
   }

   // If a retry attempt fails with an error labeled NoWritesPerformed, drivers MUST return the original error.
   if (original_error.set && mongoc_error_has_label (reply, "NoWritesPerformed")) {
      if (error) {
         *error = original_error.error;
      }
      bson_destroy (reply);
      bson_copy_to (&original_error.reply, reply);
   }

   if (original_error.set) {
      bson_destroy (&original_error.reply);
   }

   RETURN (ret);
}
