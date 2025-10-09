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


#include <bson/bson.h>

#include "mongoc-client.h"
#include "mongoc-async-cmd-private.h"
#include "mongoc-async-private.h"
#include "mongoc-cluster-private.h"
#include "mongoc-error.h"
#include "mongoc-opcode.h"
#include "mongoc-rpc-private.h"
#include "mongoc-stream-private.h"
#include "mongoc-server-description-private.h"
#include "mongoc-topology-scanner-private.h"
#include "mongoc-log.h"
#include "utlist.h"

#ifdef MONGOC_ENABLE_SSL
#include "mongoc-stream-tls.h"
#endif

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "async"

typedef mongoc_async_cmd_result_t (*_mongoc_async_cmd_phase_t) (mongoc_async_cmd_t *cmd);

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_initiate (mongoc_async_cmd_t *cmd);
mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_setup (mongoc_async_cmd_t *cmd);
mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_send (mongoc_async_cmd_t *cmd);
mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_recv_len (mongoc_async_cmd_t *cmd);
mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_recv_rpc (mongoc_async_cmd_t *cmd);

static const _mongoc_async_cmd_phase_t gMongocCMDPhases[] = {
   _mongoc_async_cmd_phase_initiate,
   _mongoc_async_cmd_phase_setup,
   _mongoc_async_cmd_phase_send,
   _mongoc_async_cmd_phase_recv_len,
   _mongoc_async_cmd_phase_recv_rpc,
   NULL, /* no callback for MONGOC_ASYNC_CMD_ERROR_STATE    */
   NULL, /* no callback for MONGOC_ASYNC_CMD_CANCELED_STATE */
};

#ifdef MONGOC_ENABLE_SSL
int
mongoc_async_cmd_tls_setup (mongoc_stream_t *stream, int *events, void *ctx, int32_t timeout_msec, bson_error_t *error)
{
   mongoc_stream_t *tls_stream;
   const char *host = (const char *) ctx;
   int retry_events = 0;


   for (tls_stream = stream; tls_stream->type != MONGOC_STREAM_TLS;
        tls_stream = mongoc_stream_get_base_stream (tls_stream)) {
   }

#if defined(MONGOC_ENABLE_SSL_OPENSSL) || defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
   /* pass 0 for the timeout to begin / continue non-blocking handshake */
   timeout_msec = 0;
#endif
   if (mongoc_stream_tls_handshake (tls_stream, host, timeout_msec, &retry_events, error)) {
      return 1;
   }

   if (retry_events) {
      *events = retry_events;
      return 0;
   }
   return -1;
}
#endif

bool
mongoc_async_cmd_run (mongoc_async_cmd_t *acmd)
{
   mongoc_async_cmd_result_t result;
   int64_t duration_usec;
   _mongoc_async_cmd_phase_t phase_callback;

   BSON_ASSERT (acmd);

   /* if we have successfully connected to the node, call the callback. */
   if (acmd->state == MONGOC_ASYNC_CMD_SEND) {
      acmd->cb (acmd, MONGOC_ASYNC_CMD_CONNECTED, NULL, 0);
   }

   phase_callback = gMongocCMDPhases[acmd->state];
   if (phase_callback) {
      result = phase_callback (acmd);
   } else {
      result = MONGOC_ASYNC_CMD_ERROR;
   }

   if (result == MONGOC_ASYNC_CMD_IN_PROGRESS) {
      return true;
   }

   duration_usec = bson_get_monotonic_time () - acmd->cmd_started;

   if (result == MONGOC_ASYNC_CMD_SUCCESS) {
      acmd->cb (acmd, result, &acmd->reply, duration_usec);
   } else {
      /* we're in ERROR, TIMEOUT, or CANCELED */
      acmd->cb (acmd, result, NULL, duration_usec);
   }

   mongoc_async_cmd_destroy (acmd);
   return false;
}

static void
_mongoc_async_cmd_init_send (const int32_t cmd_opcode, mongoc_async_cmd_t *acmd, const char *dbname)
{
   BSON_ASSERT (cmd_opcode == MONGOC_OP_CODE_QUERY || cmd_opcode == MONGOC_OP_CODE_MSG);

   int32_t message_length = 0;

   message_length += mcd_rpc_header_set_message_length (acmd->rpc, 0);
   message_length += mcd_rpc_header_set_request_id (acmd->rpc, ++acmd->async->request_id);
   message_length += mcd_rpc_header_set_response_to (acmd->rpc, 0);
   message_length += mcd_rpc_header_set_op_code (acmd->rpc, cmd_opcode);

   if (cmd_opcode == MONGOC_OP_CODE_QUERY) {
      acmd->ns = bson_strdup_printf ("%s.$cmd", dbname);
      message_length += mcd_rpc_op_query_set_flags (acmd->rpc, MONGOC_OP_QUERY_FLAG_SECONDARY_OK);
      message_length += mcd_rpc_op_query_set_full_collection_name (acmd->rpc, acmd->ns);
      message_length += mcd_rpc_op_query_set_number_to_skip (acmd->rpc, 0);
      message_length += mcd_rpc_op_query_set_number_to_return (acmd->rpc, -1);
      message_length += mcd_rpc_op_query_set_query (acmd->rpc, bson_get_data (&acmd->cmd));
   } else {
      mcd_rpc_op_msg_set_sections_count (acmd->rpc, 1u);
      message_length += mcd_rpc_op_msg_set_flag_bits (acmd->rpc, MONGOC_OP_MSG_FLAG_NONE);
      message_length += mcd_rpc_op_msg_section_set_kind (acmd->rpc, 0u, 0);
      message_length += mcd_rpc_op_msg_section_set_body (acmd->rpc, 0u, bson_get_data (&acmd->cmd));
   }

   mcd_rpc_message_set_length (acmd->rpc, message_length);

   /* This will always be hello, which are not allowed to be compressed */
   acmd->iovec = mcd_rpc_message_to_iovecs (acmd->rpc, &acmd->niovec);
   BSON_ASSERT (acmd->iovec);

   acmd->bytes_written = 0;
}

void
_mongoc_async_cmd_state_start (mongoc_async_cmd_t *acmd, bool is_setup_done)
{
   if (!acmd->stream) {
      acmd->state = MONGOC_ASYNC_CMD_INITIATE;
   } else if (acmd->setup && !is_setup_done) {
      acmd->state = MONGOC_ASYNC_CMD_SETUP;
   } else {
      acmd->state = MONGOC_ASYNC_CMD_SEND;
   }

   acmd->events = POLLOUT;
}

mongoc_async_cmd_t *
mongoc_async_cmd_new (mongoc_async_t *async,
                      mongoc_stream_t *stream,
                      bool is_setup_done,
                      struct addrinfo *dns_result,
                      mongoc_async_cmd_initiate_t initiator,
                      int64_t initiate_delay_ms,
                      mongoc_async_cmd_setup_t setup,
                      void *setup_ctx,
                      const char *dbname,
                      const bson_t *cmd,
                      const int32_t cmd_opcode, /* OP_QUERY or OP_MSG */
                      mongoc_async_cmd_cb_t cb,
                      void *cb_data,
                      int64_t timeout_msec)
{
   BSON_ASSERT_PARAM (cmd);
   BSON_ASSERT_PARAM (dbname);

   mongoc_async_cmd_t *const acmd = BSON_ALIGNED_ALLOC0 (mongoc_async_cmd_t);
   acmd->async = async;
   acmd->dns_result = dns_result;
   acmd->timeout_msec = timeout_msec;
   acmd->stream = stream;
   acmd->initiator = initiator;
   acmd->initiate_delay_ms = initiate_delay_ms;
   acmd->setup = setup;
   acmd->setup_ctx = setup_ctx;
   acmd->cb = cb;
   acmd->data = cb_data;
   acmd->connect_started = bson_get_monotonic_time ();
   bson_copy_to (cmd, &acmd->cmd);

   if (MONGOC_OP_CODE_MSG == cmd_opcode) {
      /* If we're sending an OP_MSG, we need to add the "db" field: */
      bson_append_utf8 (&acmd->cmd, "$db", 3, "admin", 5);
   }

   acmd->rpc = mcd_rpc_message_new ();
   acmd->iovec = NULL;
   _mongoc_buffer_init (&acmd->buffer, NULL, 0, NULL, NULL);

   _mongoc_async_cmd_init_send (cmd_opcode, acmd, dbname);

   _mongoc_async_cmd_state_start (acmd, is_setup_done);

   async->ncmds++;
   DL_APPEND (async->cmds, acmd);

   return acmd;
}


void
mongoc_async_cmd_destroy (mongoc_async_cmd_t *acmd)
{
   BSON_ASSERT (acmd);

   DL_DELETE (acmd->async->cmds, acmd);
   acmd->async->ncmds--;

   bson_destroy (&acmd->cmd);

   if (acmd->reply_needs_cleanup) {
      bson_destroy (&acmd->reply);
   }

   bson_free (acmd->iovec);
   _mongoc_buffer_destroy (&acmd->buffer);
   mcd_rpc_message_destroy (acmd->rpc);

   bson_free (acmd->ns);
   bson_free (acmd);
}

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_initiate (mongoc_async_cmd_t *acmd)
{
   acmd->stream = acmd->initiator (acmd);
   if (!acmd->stream) {
      return MONGOC_ASYNC_CMD_ERROR;
   }
   /* reset the connect started time after connection starts. */
   acmd->connect_started = bson_get_monotonic_time ();
   if (acmd->setup) {
      acmd->state = MONGOC_ASYNC_CMD_SETUP;
   } else {
      acmd->state = MONGOC_ASYNC_CMD_SEND;
   }
   return MONGOC_ASYNC_CMD_IN_PROGRESS;
}

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_setup (mongoc_async_cmd_t *acmd)
{
   int retval;

   BSON_ASSERT (acmd->timeout_msec < INT32_MAX);
   retval = acmd->setup (acmd->stream, &acmd->events, acmd->setup_ctx, (int32_t) acmd->timeout_msec, &acmd->error);
   switch (retval) {
   case -1:
      return MONGOC_ASYNC_CMD_ERROR;
   case 0:
      break;
   case 1:
      acmd->state = MONGOC_ASYNC_CMD_SEND;
      acmd->events = POLLOUT;
      break;
   default:
      abort ();
   }

   return MONGOC_ASYNC_CMD_IN_PROGRESS;
}

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_send (mongoc_async_cmd_t *acmd)
{
   size_t total_bytes = 0;
   size_t offset;
   ssize_t bytes;
   /* if a continued write, then iovec will be set to a temporary copy */
   bool used_temp_iovec = false;
   mongoc_iovec_t *iovec = acmd->iovec;
   size_t niovec = acmd->niovec;

   for (size_t i = 0u; i < acmd->niovec; i++) {
      total_bytes += acmd->iovec[i].iov_len;
   }

   if (acmd->bytes_written > 0) {
      BSON_ASSERT (acmd->bytes_written < total_bytes);
      /* if bytes have been written before, compute the offset in the next
       * iovec entry to be written. */
      offset = acmd->bytes_written;

      size_t i = 0u;

      /* subtract the lengths of all iovec entries written so far. */
      for (i = 0u; i < acmd->niovec; i++) {
         if (offset < acmd->iovec[i].iov_len) {
            break;
         }
         offset -= acmd->iovec[i].iov_len;
      }

      BSON_ASSERT (i < acmd->niovec);

      /* create a new iovec with the remaining data to be written. */
      niovec = acmd->niovec - i;
      iovec = bson_malloc (niovec * sizeof (mongoc_iovec_t));
      memcpy (iovec, acmd->iovec + i, niovec * sizeof (mongoc_iovec_t));
      iovec[0].iov_base = (char *) iovec[0].iov_base + offset;
      iovec[0].iov_len -= offset;
      used_temp_iovec = true;
   }

   mcd_rpc_message_egress (acmd->rpc);
   bytes = mongoc_stream_writev (acmd->stream, iovec, niovec, 0);

   if (used_temp_iovec) {
      bson_free (iovec);
   }

   if (bytes <= 0 && mongoc_stream_should_retry (acmd->stream)) {
      return MONGOC_ASYNC_CMD_IN_PROGRESS;
   }

   if (bytes < 0) {
      bson_set_error (&acmd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to write rpc bytes.");
      return MONGOC_ASYNC_CMD_ERROR;
   }

   acmd->bytes_written += bytes;

   if (acmd->bytes_written < total_bytes) {
      return MONGOC_ASYNC_CMD_IN_PROGRESS;
   }

   acmd->state = MONGOC_ASYNC_CMD_RECV_LEN;
   acmd->bytes_to_read = 4;
   acmd->events = POLLIN;

   acmd->cmd_started = bson_get_monotonic_time ();

   return MONGOC_ASYNC_CMD_IN_PROGRESS;
}

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_recv_len (mongoc_async_cmd_t *acmd)
{
   ssize_t bytes = _mongoc_buffer_try_append_from_stream (&acmd->buffer, acmd->stream, acmd->bytes_to_read, 0);
   uint32_t msg_len;

   if (bytes <= 0 && mongoc_stream_should_retry (acmd->stream)) {
      return MONGOC_ASYNC_CMD_IN_PROGRESS;
   }

   if (bytes < 0) {
      bson_set_error (
         &acmd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to receive length header from server.");
      return MONGOC_ASYNC_CMD_ERROR;
   }

   if (bytes == 0) {
      bson_set_error (&acmd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Server closed connection.");
      return MONGOC_ASYNC_CMD_ERROR;
   }

   acmd->bytes_to_read = (size_t) (acmd->bytes_to_read - bytes);

   if (!acmd->bytes_to_read) {
      memcpy (&msg_len, acmd->buffer.data, 4);
      msg_len = BSON_UINT32_FROM_LE (msg_len);

      if (msg_len < 16 || msg_len > MONGOC_DEFAULT_MAX_MSG_SIZE || msg_len < acmd->buffer.len) {
         bson_set_error (
            &acmd->error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "Invalid reply from server.");
         return MONGOC_ASYNC_CMD_ERROR;
      }

      acmd->bytes_to_read = msg_len - acmd->buffer.len;
      acmd->state = MONGOC_ASYNC_CMD_RECV_RPC;

      return _mongoc_async_cmd_phase_recv_rpc (acmd);
   }

   return MONGOC_ASYNC_CMD_IN_PROGRESS;
}

mongoc_async_cmd_result_t
_mongoc_async_cmd_phase_recv_rpc (mongoc_async_cmd_t *acmd)
{
   ssize_t bytes = _mongoc_buffer_try_append_from_stream (&acmd->buffer, acmd->stream, acmd->bytes_to_read, 0);

   if (bytes <= 0 && mongoc_stream_should_retry (acmd->stream)) {
      return MONGOC_ASYNC_CMD_IN_PROGRESS;
   }

   if (bytes < 0) {
      bson_set_error (
         &acmd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to receive rpc bytes from server.");
      return MONGOC_ASYNC_CMD_ERROR;
   }

   if (bytes == 0) {
      bson_set_error (&acmd->error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Server closed connection.");
      return MONGOC_ASYNC_CMD_ERROR;
   }

   acmd->bytes_to_read = (size_t) (acmd->bytes_to_read - bytes);

   if (!acmd->bytes_to_read) {
      mcd_rpc_message_reset (acmd->rpc);
      if (!mcd_rpc_message_from_data_in_place (acmd->rpc, acmd->buffer.data, acmd->buffer.len, NULL)) {
         bson_set_error (
            &acmd->error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "Invalid reply from server.");
         return MONGOC_ASYNC_CMD_ERROR;
      }
      mcd_rpc_message_ingress (acmd->rpc);

      void *decompressed_data;
      size_t decompressed_data_len;

      if (!mcd_rpc_message_decompress_if_necessary (acmd->rpc, &decompressed_data, &decompressed_data_len)) {
         bson_set_error (&acmd->error,
                         MONGOC_ERROR_PROTOCOL,
                         MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                         "Could not decompress server reply");
         return MONGOC_ASYNC_CMD_ERROR;
      }

      if (decompressed_data) {
         _mongoc_buffer_destroy (&acmd->buffer);
         _mongoc_buffer_init (&acmd->buffer, decompressed_data, decompressed_data_len, NULL, NULL);
      }

      if (!mcd_rpc_message_get_body (acmd->rpc, &acmd->reply)) {
         bson_set_error (
            &acmd->error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "Invalid reply from server");
         return MONGOC_ASYNC_CMD_ERROR;
      }

      acmd->reply_needs_cleanup = true;

      return MONGOC_ASYNC_CMD_SUCCESS;
   }

   return MONGOC_ASYNC_CMD_IN_PROGRESS;
}
