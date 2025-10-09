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

#include "common-thread-private.h"
#include "mongoc-server-monitor-private.h"

#include "mongoc/mcd-rpc.h"
#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-error-private.h"
#include "mongoc/mongoc-handshake-private.h"
#include "mongoc/mongoc-ssl-private.h"
#include "mongoc/mongoc-stream-private.h"
#include "mongoc/mongoc-topology-background-monitoring-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-trace-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "monitor"

typedef enum {
   MONGOC_THREAD_OFF = 0,
   MONGOC_THREAD_RUNNING,
   MONGOC_THREAD_SHUTTING_DOWN,
   MONGOC_THREAD_JOINABLE
} thread_state_t;

/* Use a signed and wide return type for timeouts as long as you can. Cast only
 * when you know what you're doing with it. */
static int64_t
_now_us (void)
{
   return bson_get_monotonic_time ();
}

static int64_t
_now_ms (void)
{
   return _now_us () / 1000;
}

struct _mongoc_server_monitor_t {
   mongoc_topology_t *topology;
   bson_thread_t thread;

   /* State accessed from multiple threads. */
   struct {
      bson_mutex_t mutex;
      mongoc_cond_t cond;
      thread_state_t state;
      bool scan_requested;
      bool cancel_requested;
   } shared;

   /* Default time to sleep between hello checks (reduced when a scan is
    * requested) */
   int64_t heartbeat_frequency_ms;
   /* The minimum time to sleep between hello checks. */
   int64_t min_heartbeat_frequency_ms;
   int64_t connect_timeout_ms;
   bool use_tls;
#ifdef MONGOC_ENABLE_SSL
   mongoc_ssl_opt_t *ssl_opts;
#endif
   mongoc_uri_t *uri;
   /* A custom initiator may be set if a user provides overrides to create a
    * stream. */
   mongoc_stream_initiator_t initiator;
   void *initiator_context;
   int32_t request_id;
   mongoc_apm_callbacks_t apm_callbacks;
   void *apm_context;

   mongoc_stream_t *stream;
   bool more_to_come;
   mongoc_server_description_t *description;
   uint32_t server_id;
   bool is_rtt;
};

static BSON_GNUC_PRINTF (3, 4) void _server_monitor_log (mongoc_server_monitor_t *server_monitor,
                                                         mongoc_log_level_t level,
                                                         const char *format,
                                                         ...)
{
   va_list ap;
   char *msg;

   va_start (ap, format);
   msg = bson_strdupv_printf (format, ap);
   va_end (ap);

   mongoc_log (level,
               MONGOC_LOG_DOMAIN,
               "[%s%s] %s",
               server_monitor->description->host.host_and_port,
               server_monitor->is_rtt ? "-RTT" : "",
               msg);
   bson_free (msg);
}

#define MONITOR_LOG(sm, ...)                                            \
   do {                                                                 \
      if (MONGOC_TRACE_ENABLED) {                                       \
         _server_monitor_log (sm, MONGOC_LOG_LEVEL_TRACE, __VA_ARGS__); \
      }                                                                 \
   } while (0)

/* TODO CDRIVER-3710 use MONGOC_LOG_LEVEL_ERROR */
#define MONITOR_LOG_ERROR(sm, ...) _server_monitor_log (sm, MONGOC_LOG_LEVEL_DEBUG, __VA_ARGS__)
/* TODO CDRIVER-3710 use MONGOC_LOG_LEVEL_WARNING */
#define MONITOR_LOG_WARNING(sm, ...) _server_monitor_log (sm, MONGOC_LOG_LEVEL_DEBUG, __VA_ARGS__)

static void
_server_monitor_heartbeat_started (mongoc_server_monitor_t *server_monitor, bool awaited)
{
   mongoc_apm_server_heartbeat_started_t event;
   MONGOC_DEBUG_ASSERT (!mcommon_mutex_is_locked (&server_monitor->topology->apm_mutex));

   if (!server_monitor->apm_callbacks.server_heartbeat_started) {
      return;
   }

   event.host = &server_monitor->description->host;
   event.context = server_monitor->apm_context;
   MONITOR_LOG (server_monitor, "%s heartbeat started", awaited ? "awaitable" : "regular");
   event.awaited = awaited;
   bson_mutex_lock (&server_monitor->topology->apm_mutex);
   server_monitor->apm_callbacks.server_heartbeat_started (&event);
   bson_mutex_unlock (&server_monitor->topology->apm_mutex);
}

static void
_server_monitor_heartbeat_succeeded (mongoc_server_monitor_t *server_monitor,
                                     const bson_t *reply,
                                     int64_t duration_usec,
                                     bool awaited)
{
   mongoc_apm_server_heartbeat_succeeded_t event;

   if (!server_monitor->apm_callbacks.server_heartbeat_succeeded) {
      return;
   }

   event.host = &server_monitor->description->host;
   event.context = server_monitor->apm_context;
   event.reply = reply;
   event.duration_usec = duration_usec;
   MONITOR_LOG (server_monitor, "%s heartbeat succeeded", awaited ? "awaitable" : "regular");
   event.awaited = awaited;
   bson_mutex_lock (&server_monitor->topology->apm_mutex);
   server_monitor->apm_callbacks.server_heartbeat_succeeded (&event);
   bson_mutex_unlock (&server_monitor->topology->apm_mutex);
}

static void
_server_monitor_heartbeat_failed (mongoc_server_monitor_t *server_monitor,
                                  const bson_error_t *error,
                                  int64_t duration_usec,
                                  bool awaited)
{
   mongoc_apm_server_heartbeat_failed_t event;

   if (!server_monitor->apm_callbacks.server_heartbeat_failed) {
      return;
   }

   event.host = &server_monitor->description->host;
   event.context = server_monitor->apm_context;
   event.error = error;
   event.duration_usec = duration_usec;
   MONITOR_LOG (server_monitor, "%s heartbeat failed", awaited ? "awaitable" : "regular");
   event.awaited = awaited;
   bson_mutex_lock (&server_monitor->topology->apm_mutex);
   server_monitor->apm_callbacks.server_heartbeat_failed (&event);
   bson_mutex_unlock (&server_monitor->topology->apm_mutex);
}

static void
_server_monitor_append_cluster_time (mongoc_server_monitor_t *server_monitor, bson_t *cmd)
{
   mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (server_monitor)->topology);

   /* Cluster time is updated on every reply. */
   if (!bson_empty (&td.ptr->cluster_time)) {
      bson_append_document (cmd, "$clusterTime", 12, &td.ptr->cluster_time);
   }
   mc_tpld_drop_ref (&td);
}

static int32_t
_int32_from_le (const void *data)
{
   BSON_ASSERT_PARAM (data);
   return bson_iter_int32_unsafe (&(bson_iter_t){.raw = data});
}

static bool
_server_monitor_send_and_recv_hello_opmsg (mongoc_server_monitor_t *server_monitor,
                                           const bson_t *cmd,
                                           bson_t *reply,
                                           bson_error_t *error)
{
   bool ret = false;

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();
   mongoc_buffer_t buffer;
   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);
   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   /* First, let's construct and send our OP_MSG: */
   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, server_monitor->request_id++);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG);

      mcd_rpc_op_msg_set_sections_count (rpc, 1u);

      message_length += mcd_rpc_op_msg_set_flag_bits (rpc, MONGOC_OP_MSG_FLAG_NONE);
      message_length += mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0);
      message_length += mcd_rpc_op_msg_section_set_body (rpc, 0u, bson_get_data (cmd));

      mcd_rpc_message_set_length (rpc, message_length);
   }

   size_t num_iovecs = 0u;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   MONITOR_LOG (server_monitor, "sending with timeout %" PRId64, server_monitor->connect_timeout_ms);

   mcd_rpc_message_egress (rpc);
   if (!_mongoc_stream_writev_full (
          server_monitor->stream, iovecs, num_iovecs, server_monitor->connect_timeout_ms, error)) {
      MONITOR_LOG_ERROR (server_monitor, "failed to write polling hello: %s", error->message);
      goto fail;
   }

   /* Done sending! Now, receive the reply: */
   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_monitor->stream, sizeof (int32_t), server_monitor->connect_timeout_ms, error)) {
      goto fail;
   }

   const int32_t message_length = _int32_from_le (buffer.data);

   // msgHeader consists of four int32 fields.
   const int32_t message_header_length = 4u * sizeof (int32_t);

   if (message_length < message_header_length) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: message length");
      goto fail;
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_monitor->stream, remaining_bytes, server_monitor->connect_timeout_ms, error)) {
      goto fail;
   }

   mcd_rpc_message_reset (rpc);
   if (!mcd_rpc_message_from_data_in_place (rpc, buffer.data, buffer.len, NULL)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: malformed message");
      goto fail;
   }

   mcd_rpc_message_ingress (rpc);

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: decompression failure");
      goto fail;
   }

   bson_t body;
   if (!mcd_rpc_message_get_body (rpc, &body)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: malformed body");
      goto fail;
   }

   bson_copy_to (&body, reply);
   bson_destroy (&body);

   ret = true;

fail:
   if (!ret) {
      bson_init (reply);
   }

   bson_free (decompressed_data);
   _mongoc_buffer_destroy (&buffer);
   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   return ret;
}

static bool
_server_monitor_send_and_recv_opquery (mongoc_server_monitor_t *server_monitor,
                                       const bson_t *cmd,
                                       bson_t *reply,
                                       bson_error_t *error)
{
   bool ret = false;

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();
   size_t num_iovecs = 0u;
   mongoc_iovec_t *iovecs = NULL;
   mongoc_buffer_t buffer;
   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);
   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, server_monitor->request_id++);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_QUERY);

      message_length += mcd_rpc_op_query_set_flags (rpc, MONGOC_OP_QUERY_FLAG_SECONDARY_OK);
      message_length += mcd_rpc_op_query_set_full_collection_name (rpc, "admin.$cmd");
      message_length += mcd_rpc_op_query_set_number_to_skip (rpc, 0);
      message_length += mcd_rpc_op_query_set_number_to_return (rpc, -1);
      message_length += mcd_rpc_op_query_set_query (rpc, bson_get_data (cmd));

      mcd_rpc_message_set_length (rpc, message_length);
   }

   iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   mcd_rpc_message_egress (rpc);
   if (!_mongoc_stream_writev_full (
          server_monitor->stream, iovecs, num_iovecs, server_monitor->connect_timeout_ms, error)) {
      goto fail;
   }

   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_monitor->stream, sizeof (int32_t), server_monitor->connect_timeout_ms, error)) {
      goto fail;
   }

   const int32_t message_length = _int32_from_le (buffer.data);

   // msgHeader consists of four int32 fields.
   const int32_t message_header_length = 4u * sizeof (int32_t);

   if (message_length < message_header_length) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: message length");
      goto fail;
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_monitor->stream, remaining_bytes, server_monitor->connect_timeout_ms, error)) {
      goto fail;
   }

   mcd_rpc_message_reset (rpc);
   if (!mcd_rpc_message_from_data_in_place (rpc, buffer.data, buffer.len, NULL)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: malformed message");
      goto fail;
   }

   mcd_rpc_message_ingress (rpc);

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: decompression failure");
      goto fail;
   }

   bson_t body;
   if (!mcd_rpc_message_get_body (rpc, &body)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "invalid reply from server: malformed body");
      goto fail;
   }

   bson_copy_to (&body, reply);
   bson_destroy (&body);

   ret = true;

fail:
   if (!ret) {
      bson_init (reply);
   }

   bson_free (decompressed_data);
   _mongoc_buffer_destroy (&buffer);
   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   return ret;
}

static bool
_server_monitor_send_and_recv (mongoc_server_monitor_t *server_monitor, bson_t *cmd, bson_t *reply, bson_error_t *error)
{
   if (mongoc_topology_uses_server_api (server_monitor->topology) ||
       mongoc_topology_uses_loadbalanced (server_monitor->topology)) {
      /* OP_MSG requires a "db" parameter: */
      bson_append_utf8 (cmd, "$db", 3, "admin", 5);

      return _server_monitor_send_and_recv_hello_opmsg (server_monitor, cmd, reply, error);
   } else {
      return _server_monitor_send_and_recv_opquery (server_monitor, cmd, reply, error);
   }
}

static bool
_server_monitor_polling_hello (mongoc_server_monitor_t *server_monitor,
                               bool hello_ok,
                               bson_t *hello_response,
                               bson_error_t *error)
{
   bson_t cmd;
   const bson_t *hello;
   bool ret;

   hello = _mongoc_topology_scanner_get_monitoring_cmd (server_monitor->topology->scanner, hello_ok);
   bson_copy_to (hello, &cmd);

   _server_monitor_append_cluster_time (server_monitor, &cmd);

   ret = _server_monitor_send_and_recv (server_monitor, &cmd, hello_response, error);

   bson_destroy (&cmd);
   return ret;
}

static bool
_server_monitor_awaitable_hello_send (mongoc_server_monitor_t *server_monitor, bson_t *cmd, bson_error_t *error)
{
   bool ret = false;

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, server_monitor->request_id++);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG);

      mcd_rpc_op_msg_set_sections_count (rpc, 1);

      message_length += mcd_rpc_op_msg_set_flag_bits (rpc, MONGOC_OP_MSG_FLAG_EXHAUST_ALLOWED);
      message_length += mcd_rpc_op_msg_section_set_kind (rpc, 0u, 0);
      message_length += mcd_rpc_op_msg_section_set_body (rpc, 0u, bson_get_data (cmd));

      mcd_rpc_message_set_length (rpc, message_length);
   }

   size_t num_iovecs;
   mongoc_iovec_t *const iovecs = mcd_rpc_message_to_iovecs (rpc, &num_iovecs);
   BSON_ASSERT (iovecs);

   MONITOR_LOG (server_monitor, "sending with timeout %" PRId64, server_monitor->connect_timeout_ms);

   mcd_rpc_message_egress (rpc);
   if (!_mongoc_stream_writev_full (
          server_monitor->stream, iovecs, num_iovecs, server_monitor->connect_timeout_ms, error)) {
      MONITOR_LOG_ERROR (server_monitor, "failed to write awaitable hello: %s", error->message);
      goto done;
   }

   ret = true;

done:
   bson_free (iovecs);
   mcd_rpc_message_destroy (rpc);

   return ret;
}

/* Poll the server monitor stream for reading. Allows cancellation.
 *
 * Called only from server monitor thread.
 * Locks server monitor mutex.
 * Returns true if stream is readable. False on error or cancellation.
 * On cancellation, no error is set, but cancelled is set to true.
 */
static bool
_server_monitor_poll_with_interrupt (mongoc_server_monitor_t *server_monitor,
                                     int64_t expire_at_ms,
                                     bool *cancelled,
                                     bson_error_t *error)
{
   /* How many milliseconds we should poll for on each tick.
    * On every tick, check whether the awaitable hello was cancelled. */
   const int32_t monitor_tick_ms = MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS;
   int64_t timeleft_ms;

   while ((timeleft_ms = expire_at_ms - _now_ms ()) > 0) {
      ssize_t ret;
      mongoc_stream_poll_t poller[1];

      MONITOR_LOG (server_monitor, "_server_monitor_poll_with_interrupt expires in: %" PRIu64 "ms", timeleft_ms);
      poller[0].stream = server_monitor->stream;
      poller[0].events = POLLIN; /* POLLERR and POLLHUP are added in mongoc_socket_poll. */
      poller[0].revents = 0;

      MONITOR_LOG (server_monitor, "polling for awaitable hello reply with timeleft_ms: %" PRId64, timeleft_ms);
      ret = mongoc_stream_poll (poller, 1, (int32_t) BSON_MIN (timeleft_ms, monitor_tick_ms));
      if (ret == -1) {
         MONITOR_LOG (server_monitor, "mongoc_stream_poll error");
         bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "poll error");
         return false;
      }

      if (poller[0].revents & (POLLERR | POLLHUP)) {
         bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "connection closed while polling");
         return false;
      }

      /* Check for cancellation. */
      bson_mutex_lock (&server_monitor->shared.mutex);
      *cancelled = server_monitor->shared.cancel_requested;
      server_monitor->shared.cancel_requested = false;
      bson_mutex_unlock (&server_monitor->shared.mutex);

      if (*cancelled) {
         MONITOR_LOG (server_monitor, "polling cancelled");
         return false;
      }

      if (poller[0].revents & POLLIN) {
         MONITOR_LOG (server_monitor, "mongoc_stream_poll ready to read");
         return true;
      }
   }
   bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "connection timeout while polling");
   return false;
}

/* Calculate the timeout between the current time and an absolute expiration
 * time in milliseconds.
 *
 * Returns 0 and sets error if time expired.
 */
int64_t
_get_timeout_ms (int64_t expire_at_ms, bson_error_t *error)
{
   int64_t timeout_ms;

   timeout_ms = expire_at_ms - _now_ms ();
   if (timeout_ms <= 0) {
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "connection timed out reading message length");
      return 0;
   }
   return timeout_ms;
}

/* Receive an awaitable hello reply.
 *
 * May be used to receive additional replies when moreToCome is set.
 * Called only from server monitor thread.
 * May lock server monitor mutex in functions that are called.
 * May block for up to heartbeatFrequencyMS + connectTimeoutMS waiting for
 * reply.
 * Returns true if a reply was received. False on error or cancellation.
 * On cancellation, no error is set, but cancelled is set to true.
 */
static bool
_server_monitor_awaitable_hello_recv (mongoc_server_monitor_t *server_monitor,
                                      bson_t *hello_response,
                                      bool *cancelled,
                                      bson_error_t *error)
{
   bool ret = false;

   int64_t timeout_ms;
   mcd_rpc_message *const rpc = mcd_rpc_message_new ();
   mongoc_buffer_t buffer;
   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);
   void *decompressed_data = NULL;
   size_t decompressed_data_len = 0u;

   const int64_t expire_at_ms =
      _now_ms () + server_monitor->heartbeat_frequency_ms + server_monitor->connect_timeout_ms;

   if (!_server_monitor_poll_with_interrupt (server_monitor, expire_at_ms, cancelled, error)) {
      GOTO (fail);
   }

   timeout_ms = _get_timeout_ms (expire_at_ms, error);
   if (timeout_ms == 0) {
      GOTO (fail);
   }

   MONITOR_LOG (server_monitor, "reading first 4 bytes with timeout: %" PRId64, timeout_ms);
   if (!_mongoc_buffer_append_from_stream (
          &buffer, server_monitor->stream, sizeof (int32_t), (int32_t) timeout_ms, error)) {
      GOTO (fail);
   }

   const int32_t message_length = _int32_from_le (buffer.data);

   // msgHeader consists of four int32 fields.
   const int32_t message_header_length = 4u * sizeof (int32_t);

   if ((message_length < message_header_length) || (message_length > server_monitor->description->max_msg_size)) {
      bson_set_error (error,
                      MONGOC_ERROR_PROTOCOL,
                      MONGOC_ERROR_PROTOCOL_INVALID_REPLY,
                      "message size %" PRId32 " is not within expected range 16-%" PRId32 " bytes",
                      message_length,
                      server_monitor->description->max_msg_size);
      GOTO (fail);
   }

   timeout_ms = _get_timeout_ms (expire_at_ms, error);
   if (timeout_ms == 0) {
      GOTO (fail);
   }

   const size_t remaining_bytes = (size_t) message_length - sizeof (int32_t);

   MONITOR_LOG (server_monitor, "reading remaining %zu bytes. Timeout %" PRId64, remaining_bytes, timeout_ms);
   if (!_mongoc_buffer_append_from_stream (&buffer, server_monitor->stream, remaining_bytes, timeout_ms, error)) {
      GOTO (fail);
   }

   if (!mcd_rpc_message_from_data_in_place (rpc, buffer.data, buffer.len, NULL)) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "malformed message from server");
      GOTO (fail);
   }

   mcd_rpc_message_ingress (rpc);

   if (!mcd_rpc_message_decompress_if_necessary (rpc, &decompressed_data, &decompressed_data_len)) {
      bson_set_error (error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "decompression failure");
      GOTO (fail);
   }

   bson_t body;
   if (!mcd_rpc_message_get_body (rpc, &body)) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "malformed BSON payload from server");
      GOTO (fail);
   }

   bson_copy_to (&body, hello_response);
   bson_destroy (&body);

   server_monitor->more_to_come = (mcd_rpc_op_msg_get_flag_bits (rpc) & MONGOC_OP_MSG_FLAG_MORE_TO_COME) != 0;

   ret = true;

fail:
   if (!ret) {
      bson_init (hello_response);
   }

   bson_free (decompressed_data);
   _mongoc_buffer_destroy (&buffer);
   mcd_rpc_message_destroy (rpc);

   return ret;
}

/* Send and receive an awaitable hello.
 *
 * Called only from server monitor thread.
 * May lock server monitor mutex in functions that are called.
 * May block for up to heartbeatFrequencyMS waiting for reply.
 */
static bool
_server_monitor_awaitable_hello (mongoc_server_monitor_t *server_monitor,
                                 const mongoc_server_description_t *description,
                                 bson_t *hello_response,
                                 bool *cancelled,
                                 bson_error_t *error)
{
   bson_t cmd;
   const bson_t *hello;
   bool ret = false;

   hello = _mongoc_topology_scanner_get_monitoring_cmd (server_monitor->topology->scanner, description->hello_ok);
   bson_copy_to (hello, &cmd);

   _server_monitor_append_cluster_time (server_monitor, &cmd);
   bson_append_document (&cmd, "topologyVersion", 15, &description->topology_version);
   bson_append_int64 (&cmd, "maxAwaitTimeMS", 14, server_monitor->heartbeat_frequency_ms);
   bson_append_utf8 (&cmd, "$db", 3, "admin", 5);

   if (!_server_monitor_awaitable_hello_send (server_monitor, &cmd, error)) {
      GOTO (fail);
   }

   if (!_server_monitor_awaitable_hello_recv (server_monitor, hello_response, cancelled, error)) {
      bson_destroy (hello_response);
      GOTO (fail);
   }

   ret = true;
fail:
   if (!ret) {
      bson_init (hello_response);
   }
   bson_destroy (&cmd);
   return ret;
}

/* Update the topology description with a reply or an error.
 *
 * Called only from server monitor thread.
 * Caller must hold no locks.
 * Locks server monitor mutex.
 */
static void
_update_topology_description (mongoc_server_monitor_t *server_monitor, mongoc_server_description_t *description)
{
   mongoc_topology_t *topology;
   bson_t *hello_response = NULL;
   mc_tpld_modification tdmod;

   topology = server_monitor->topology;
   if (description->has_hello_response) {
      hello_response = &description->last_hello_response;
   }

   if (hello_response) {
      _mongoc_topology_update_cluster_time (topology, hello_response);
   }

   if (bson_atomic_int_fetch (&topology->scanner_state, bson_memory_order_relaxed) ==
       MONGOC_TOPOLOGY_SCANNER_SHUTTING_DOWN) {
      return;
   }

   tdmod = mc_tpld_modify_begin (topology);
   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.scan_requested = false;
   bson_mutex_unlock (&server_monitor->shared.mutex);
   mongoc_topology_description_handle_hello (
      tdmod.new_td, server_monitor->server_id, hello_response, description->round_trip_time_msec, &description->error);
   /* Reconcile server monitors. */
   _mongoc_topology_background_monitoring_reconcile (topology, tdmod.new_td);
   /* Wake threads performing server selection. */
   mongoc_cond_broadcast (&server_monitor->topology->cond_client);
   mc_tpld_modify_commit (tdmod);
}

/* Create a new server monitor.
 *
 * Called during reconcile.
 * Caller must hold topology lock.
 */
mongoc_server_monitor_t *
mongoc_server_monitor_new (mongoc_topology_t *topology,
                           mongoc_topology_description_t *td,
                           mongoc_server_description_t *init_description)
{
   mongoc_server_monitor_t *server_monitor = bson_malloc0 (sizeof (*server_monitor));
   server_monitor->description = mongoc_server_description_new_copy (init_description);
   server_monitor->server_id = init_description->id;
   server_monitor->topology = topology;
   server_monitor->heartbeat_frequency_ms = td->heartbeat_msec;
   server_monitor->min_heartbeat_frequency_ms = topology->min_heartbeat_frequency_msec;
   server_monitor->connect_timeout_ms = topology->connect_timeout_msec;
   server_monitor->uri = mongoc_uri_copy (topology->uri);
/* TODO CDRIVER-3682: Do not retrieve ssl opts from topology scanner. They
 * should be stored somewhere else. */
#ifdef MONGOC_ENABLE_SSL
   if (topology->scanner->ssl_opts) {
      server_monitor->ssl_opts = bson_malloc0 (sizeof (mongoc_ssl_opt_t));

      _mongoc_ssl_opts_copy_to (topology->scanner->ssl_opts, server_monitor->ssl_opts, true);
   }
#endif
   memcpy (&server_monitor->apm_callbacks, &td->apm_callbacks, sizeof (mongoc_apm_callbacks_t));
   server_monitor->apm_context = td->apm_context;
   server_monitor->initiator = topology->scanner->initiator;
   server_monitor->initiator_context = topology->scanner->initiator_context;
   mongoc_cond_init (&server_monitor->shared.cond);
   bson_mutex_init (&server_monitor->shared.mutex);
   return server_monitor;
}

/* Creates a stream and performs the initial hello handshake.
 *
 * Called only by server monitor thread.
 * Returns true if both connection and handshake succeeds.
 * Returns false and sets error otherwise.
 * hello_response is always initialized.
 */
static bool
_server_monitor_setup_connection (mongoc_server_monitor_t *server_monitor,
                                  bson_t *hello_response,
                                  int64_t *start_us,
                                  bson_error_t *error)
{
   bson_t cmd = BSON_INITIALIZER;
   bool ret = false;

   ENTRY;

   BSON_ASSERT (!server_monitor->stream);
   bson_init (hello_response);

   server_monitor->more_to_come = false;

   /* Using an initiator isn't really necessary. Users can't set them on
    * pools. But it is used for tests. */
   if (server_monitor->initiator) {
      server_monitor->stream = server_monitor->initiator (
         server_monitor->uri, &server_monitor->description->host, server_monitor->initiator_context, error);
   } else {
      void *ssl_opts_void = NULL;

#ifdef MONGOC_ENABLE_SSL
      ssl_opts_void = server_monitor->ssl_opts;
#endif
      server_monitor->stream = mongoc_client_connect (
         false, ssl_opts_void != NULL, ssl_opts_void, server_monitor->uri, &server_monitor->description->host, error);
   }

   if (!server_monitor->stream) {
      GOTO (fail);
   }

   /* Update the start time just before the handshake. */
   *start_us = _now_us ();
   /* Perform handshake. */
   bson_destroy (&cmd);
   _mongoc_topology_dup_handshake_cmd (server_monitor->topology, &cmd);
   _server_monitor_append_cluster_time (server_monitor, &cmd);
   bson_destroy (hello_response);

   ret = _server_monitor_send_and_recv (server_monitor, &cmd, hello_response, error);

fail:
   bson_destroy (&cmd);
   RETURN (ret);
}

/**
 * @brief Perform a hello check on a server
 *
 * @param server_monitor The server monitor for this server.
 * @param previous_description The most recent view of the description of this
 * server.
 * @param cancelled Output parameter: Whether the monitor check is cancelled.
 * @return mongoc_server_description_t* The newly created updated server
 * description.
 *
 * @note May update the topology description associated with the server monitor.
 *
 * @note In case of error, returns a new server description with the error
 * information, but with no hello reply.
 */
static mongoc_server_description_t *
_server_monitor_check_server (mongoc_server_monitor_t *server_monitor,
                              const mongoc_server_description_t *previous_description,
                              bool *cancelled)
{
   bool ret = false;
   bson_error_t error;
   bson_t hello_response;
   int64_t duration_us;
   int64_t start_us;
   bool command_or_network_error = false;
   bool awaited = false;
   mongoc_server_description_t *description;
   mc_tpld_modification tdmod;

   ENTRY;

   *cancelled = false;
   memset (&error, 0, sizeof (bson_error_t));
   description = BSON_ALIGNED_ALLOC0 (mongoc_server_description_t);
   mongoc_server_description_init (
      description, server_monitor->description->connection_address, server_monitor->description->id);
   start_us = _now_us ();

   if (!server_monitor->stream) {
      MONITOR_LOG (server_monitor, "setting up connection");
      awaited = false;
      _server_monitor_heartbeat_started (server_monitor, awaited);
      ret = _server_monitor_setup_connection (server_monitor, &hello_response, &start_us, &error);
      GOTO (exit);
   }

   if (server_monitor->more_to_come) {
      awaited = true;
      /* Publish a heartbeat started for each additional response read. */
      _server_monitor_heartbeat_started (server_monitor, awaited);
      MONITOR_LOG (server_monitor, "more to come");
      ret = _server_monitor_awaitable_hello_recv (server_monitor, &hello_response, cancelled, &error);
      GOTO (exit);
   }

   if (!bson_empty (&previous_description->topology_version) &&
       _mongoc_handshake_get ()->env == MONGOC_HANDSHAKE_ENV_NONE) {
      // Use stream monitoring if:
      // - Server supports stream monitoring (indicated by `topologyVersion`).
      // - Application is not in an FaaS environment (e.g. AWS Lambda).
      awaited = true;
      _server_monitor_heartbeat_started (server_monitor, awaited);
      MONITOR_LOG (server_monitor, "awaitable hello");
      ret = _server_monitor_awaitable_hello (server_monitor, previous_description, &hello_response, cancelled, &error);
      GOTO (exit);
   }

   MONITOR_LOG (server_monitor, "polling hello");
   awaited = false;
   _server_monitor_heartbeat_started (server_monitor, awaited);
   ret = _server_monitor_polling_hello (server_monitor, previous_description->hello_ok, &hello_response, &error);

exit:
   duration_us = _now_us () - start_us;
   MONITOR_LOG (server_monitor, "server check duration (us): %" PRId64, duration_us);

   /* If ret is true, we have a reply. Check if "ok": 1. */
   if (ret && _mongoc_cmd_check_ok (&hello_response, MONGOC_ERROR_API_VERSION_2, &error)) {
      int64_t rtt_ms = MONGOC_RTT_UNSET;

      /* rtt remains MONGOC_RTT_UNSET if awaited. */
      if (!awaited) {
         rtt_ms = duration_us / 1000;
      }

      mongoc_server_description_handle_hello (description, &hello_response, rtt_ms, NULL);
      /* If the hello reply could not be parsed, consider this a command
       * error. */
      if (description->error.code) {
         MONITOR_LOG_ERROR (server_monitor, "error parsing server reply: %s", description->error.message);
         command_or_network_error = true;
         _server_monitor_heartbeat_failed (server_monitor, &description->error, duration_us, awaited);
      } else {
         _server_monitor_heartbeat_succeeded (server_monitor, &hello_response, duration_us, awaited);
      }
   } else if (*cancelled) {
      MONITOR_LOG (server_monitor, "server monitor cancelled");
      if (server_monitor->stream) {
         mongoc_stream_destroy (server_monitor->stream);
      }
      server_monitor->stream = NULL;
      server_monitor->more_to_come = false;
      _server_monitor_heartbeat_failed (server_monitor, &description->error, duration_us, awaited);
   } else {
      /* The hello reply had "ok":0 or a network error occurred. */
      MONITOR_LOG_ERROR (server_monitor, "command or network error occurred: %s", error.message);
      command_or_network_error = true;
      mongoc_server_description_handle_hello (description, NULL, MONGOC_RTT_UNSET, &error);
      _server_monitor_heartbeat_failed (server_monitor, &description->error, duration_us, awaited);
   }

   if (command_or_network_error) {
      if (server_monitor->stream) {
         mongoc_stream_failed (server_monitor->stream);
      }
      server_monitor->stream = NULL;
      server_monitor->more_to_come = false;
      tdmod = mc_tpld_modify_begin (server_monitor->topology);
      /* clear_connection_pool() is a no-op if 'description->id' was already
       * removed. */
      _mongoc_topology_description_clear_connection_pool (
         tdmod.new_td, server_monitor->description->id, &server_monitor->description->service_id);
      mc_tpld_modify_commit (tdmod);
   }

   bson_destroy (&hello_response);
   return description;
}

/* Request scan of a single server.
 *
 * Locks server monitor mutex to deliver scan_requested.
 */
void
mongoc_server_monitor_request_scan (mongoc_server_monitor_t *server_monitor)
{
   MONITOR_LOG (server_monitor, "requesting scan");
   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.scan_requested = true;
   mongoc_cond_signal (&server_monitor->shared.cond);
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

/* Request cancellation of an in progress awaitable hello.
 *
 * Called from app threads on network errors and during shutdown.
 * Locks server monitor mutex.
 */
void
mongoc_server_monitor_request_cancel (mongoc_server_monitor_t *server_monitor)
{
   MONITOR_LOG (server_monitor, "requesting cancel");
   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.cancel_requested = true;
   mongoc_cond_signal (&server_monitor->shared.cond);
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

/* Wait for heartbeatFrequencyMS or minHeartbeatFrequencyMS if a scan is
 * requested.
 *
 * Locks server monitor mutex.
 */
void
mongoc_server_monitor_wait (mongoc_server_monitor_t *server_monitor)
{
   int64_t start_ms;
   int64_t scan_due_ms;

   start_ms = _now_ms ();
   scan_due_ms = start_ms + server_monitor->heartbeat_frequency_ms;

   bson_mutex_lock (&server_monitor->shared.mutex);
   while (true) {
      int64_t sleep_duration_ms;
      int cond_ret;

      if (server_monitor->shared.state != MONGOC_THREAD_RUNNING) {
         break;
      }

      if (server_monitor->shared.scan_requested) {
         server_monitor->shared.scan_requested = false;
         scan_due_ms = start_ms + server_monitor->min_heartbeat_frequency_ms;
      }

      sleep_duration_ms = scan_due_ms - _now_ms ();

      if (sleep_duration_ms <= 0) {
         break;
      }

      MONITOR_LOG (server_monitor, "sleeping for %" PRId64, sleep_duration_ms);
      cond_ret = mongoc_cond_timedwait (&server_monitor->shared.cond, &server_monitor->shared.mutex, sleep_duration_ms);
      if (mongo_cond_ret_is_timedout (cond_ret)) {
         break;
      }
   }
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

/* The server monitor thread function.
 *
 * Server monitor must be in state MONGOC_THREAD_OFF.
 */
static BSON_THREAD_FUN (_server_monitor_thread, server_monitor_void)
{
   mongoc_server_monitor_t *server_monitor;
   mongoc_server_description_t *description;
   mongoc_server_description_t *previous_description;

   server_monitor = (mongoc_server_monitor_t *) server_monitor_void;
   description = mongoc_server_description_new_copy (server_monitor->description);
   previous_description = NULL;

   while (true) {
      bool cancelled = false;

      bson_mutex_lock (&server_monitor->shared.mutex);
      if (server_monitor->shared.state != MONGOC_THREAD_RUNNING) {
         bson_mutex_unlock (&server_monitor->shared.mutex);
         break;
      }
      bson_mutex_unlock (&server_monitor->shared.mutex);

      mongoc_server_description_destroy (previous_description);
      previous_description = mongoc_server_description_new_copy (description);
      mongoc_server_description_destroy (description);
      description = _server_monitor_check_server (server_monitor, previous_description, &cancelled);

      if (cancelled) {
         mongoc_server_monitor_wait (server_monitor);
         continue;
      }

      _update_topology_description (server_monitor, description);

      /* Immediately proceed to the next check if the previous response was
       * successful and included the topologyVersion field. */
      if (description->type != MONGOC_SERVER_UNKNOWN && !bson_empty (&description->topology_version)) {
         MONITOR_LOG (server_monitor, "immediately proceeding due to topologyVersion");
         continue;
      }

      /* ... or the previous response included the moreToCome flag */
      if (server_monitor->more_to_come) {
         MONITOR_LOG (server_monitor, "immediately proceeding due to moreToCome");
         continue;
      }

      /* ... or the server has just transitioned to Unknown due to a network
       * error. */
      if (_mongoc_error_is_network (&description->error) && previous_description->type != MONGOC_SERVER_UNKNOWN) {
         MONITOR_LOG (server_monitor, "immediately proceeding due to network error");
         continue;
      }

      mongoc_server_monitor_wait (server_monitor);
   }

   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.state = MONGOC_THREAD_JOINABLE;
   bson_mutex_unlock (&server_monitor->shared.mutex);
   mongoc_server_description_destroy (previous_description);
   mongoc_server_description_destroy (description);
   BSON_THREAD_RETURN;
}

static bool
_server_monitor_ping_server (mongoc_server_monitor_t *server_monitor, bool hello_ok, int64_t *rtt_ms)
{
   bool ret = false;
   int64_t start_us = _now_us ();
   bson_t hello_response;
   bson_error_t error;

   *rtt_ms = MONGOC_RTT_UNSET;

   if (!server_monitor->stream) {
      MONITOR_LOG (server_monitor, "rtt setting up connection");
      ret = _server_monitor_setup_connection (server_monitor, &hello_response, &start_us, &error);
      bson_destroy (&hello_response);
   }

   if (server_monitor->stream) {
      MONITOR_LOG (server_monitor, "rtt polling hello");
      ret = _server_monitor_polling_hello (server_monitor, hello_ok, &hello_response, &error);
      if (ret) {
         *rtt_ms = (_now_us () - start_us) / 1000;
      }
      bson_destroy (&hello_response);
   }
   return ret;
}

/* The RTT monitor thread function.
 *
 * Server monitor must be in state MONGOC_THREAD_OFF.
 */
static BSON_THREAD_FUN (_server_monitor_rtt_thread, server_monitor_void)
{
   mongoc_server_monitor_t *server_monitor = server_monitor_void;

   while (true) {
      int64_t rtt_ms;
      bson_error_t error;
      bool hello_ok;

      bson_mutex_lock (&server_monitor->shared.mutex);
      if (server_monitor->shared.state != MONGOC_THREAD_RUNNING) {
         bson_mutex_unlock (&server_monitor->shared.mutex);
         break;
      }
      bson_mutex_unlock (&server_monitor->shared.mutex);

      {
         mc_shared_tpld td = mc_tpld_take_ref (server_monitor->topology);
         const mongoc_server_description_t *sd =
            mongoc_topology_description_server_by_id_const (td.ptr, server_monitor->description->id, &error);
         hello_ok = sd ? sd->hello_ok : false;
         mc_tpld_drop_ref (&td);
      }

      _server_monitor_ping_server (server_monitor, hello_ok, &rtt_ms);
      if (rtt_ms != MONGOC_RTT_UNSET) {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (server_monitor->topology);
         mongoc_server_description_t *const mut_sd =
            mongoc_topology_description_server_by_id (tdmod.new_td, server_monitor->description->id, &error);
         if (mut_sd) {
            mongoc_server_description_update_rtt (mut_sd, rtt_ms);
            mc_tpld_modify_commit (tdmod);
         } else {
            /* If the server description has been removed, the RTT thread will
             * be terminated by background monitoring soon, so we have nothing
             * to do but wait until we are about to be stopped. */
            mc_tpld_modify_drop (tdmod);
         }
      }
      mongoc_server_monitor_wait (server_monitor);
   }

   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.state = MONGOC_THREAD_JOINABLE;
   bson_mutex_unlock (&server_monitor->shared.mutex);
   BSON_THREAD_RETURN;
}

void
mongoc_server_monitor_run (mongoc_server_monitor_t *server_monitor)
{
   bson_mutex_lock (&server_monitor->shared.mutex);
   if (server_monitor->shared.state == MONGOC_THREAD_OFF) {
      server_monitor->is_rtt = false;
      int ret = mcommon_thread_create (&server_monitor->thread, _server_monitor_thread, server_monitor);
      if (ret == 0) {
         server_monitor->shared.state = MONGOC_THREAD_RUNNING;
      } else {
         char errmsg_buf[BSON_ERROR_BUFFER_SIZE];
         char *errmsg = bson_strerror_r (ret, errmsg_buf, sizeof errmsg_buf);
         _server_monitor_log (server_monitor,
                              MONGOC_LOG_LEVEL_ERROR,
                              "Failed to start monitoring thread. This server "
                              "may not be selectable. Error: %s",
                              errmsg);
      }
   }
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

void
mongoc_server_monitor_run_as_rtt (mongoc_server_monitor_t *server_monitor)
{
   bson_mutex_lock (&server_monitor->shared.mutex);
   if (server_monitor->shared.state == MONGOC_THREAD_OFF) {
      server_monitor->is_rtt = true;
      int ret = mcommon_thread_create (&server_monitor->thread, _server_monitor_rtt_thread, server_monitor);
      if (ret == 0) {
         server_monitor->shared.state = MONGOC_THREAD_RUNNING;
      } else {
         char errmsg_buf[BSON_ERROR_BUFFER_SIZE];
         char *errmsg = bson_strerror_r (ret, errmsg_buf, sizeof errmsg_buf);
         _server_monitor_log (server_monitor,
                              MONGOC_LOG_LEVEL_ERROR,
                              "Failed to start Round-Trip Time monitoring thread. Error: %s",
                              errmsg);
      }
   }
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

/* Request thread shutdown.
 *
 * Returns true if in state MONGOC_THREAD_OFF and the server monitor can be
 * safely destroyed.
 * Called during topology description reconcile.
 * Locks server monitor mutex.
 */
bool
mongoc_server_monitor_request_shutdown (mongoc_server_monitor_t *server_monitor)
{
   bool off = false;

   bson_mutex_lock (&server_monitor->shared.mutex);
   if (server_monitor->shared.state == MONGOC_THREAD_RUNNING) {
      server_monitor->shared.state = MONGOC_THREAD_SHUTTING_DOWN;
   }
   if (server_monitor->shared.state == MONGOC_THREAD_JOINABLE) {
      mcommon_thread_join (server_monitor->thread);
      server_monitor->shared.state = MONGOC_THREAD_OFF;
   }
   if (server_monitor->shared.state == MONGOC_THREAD_OFF) {
      off = true;
   }
   mongoc_cond_signal (&server_monitor->shared.cond);
   bson_mutex_unlock (&server_monitor->shared.mutex);
   /* Cancel an in-progress hello check. */
   if (!off) {
      mongoc_server_monitor_request_cancel (server_monitor);
   }
   return off;
}

/* Request thread shutdown and block until the server monitor thread terminates.
 *
 * Called by one thread.
 * Locks the server monitor mutex.
 */
void
mongoc_server_monitor_wait_for_shutdown (mongoc_server_monitor_t *server_monitor)
{
   if (mongoc_server_monitor_request_shutdown (server_monitor)) {
      return;
   }

   /* Shutdown requested, but thread is not yet off. Wait. */
   mcommon_thread_join (server_monitor->thread);
   bson_mutex_lock (&server_monitor->shared.mutex);
   server_monitor->shared.state = MONGOC_THREAD_OFF;
   bson_mutex_unlock (&server_monitor->shared.mutex);
}

/* Destroy a server monitor.
 *
 * Called only by one thread.
 * Caller must not hold server monitor lock.
 * Server monitor thread is in state MONGOC_THREAD_OFF.
 */
void
mongoc_server_monitor_destroy (mongoc_server_monitor_t *server_monitor)
{
   if (!server_monitor) {
      return;
   }

   /* Locking not necessary since this is only called by one thread, and server
    * monitor thread is no longer running. */
   BSON_ASSERT (server_monitor->shared.state == MONGOC_THREAD_OFF);

   mongoc_server_description_destroy (server_monitor->description);
   mongoc_stream_destroy (server_monitor->stream);
   mongoc_uri_destroy (server_monitor->uri);
   mongoc_cond_destroy (&server_monitor->shared.cond);
   bson_mutex_destroy (&server_monitor->shared.mutex);
#ifdef MONGOC_ENABLE_SSL
   if (server_monitor->ssl_opts) {
      _mongoc_ssl_opts_cleanup (server_monitor->ssl_opts, true);
      bson_free (server_monitor->ssl_opts);
   }
#endif
   bson_free (server_monitor);
}
