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

#include "mongoc-http-private.h"

#include "mongoc-client-private.h"
#include "mongoc-host-list-private.h"
#include "mongoc-stream-tls.h"
#include "mongoc-stream-private.h"
#include "mongoc-buffer-private.h"
#include "mcd-time.h"

void
_mongoc_http_request_init (mongoc_http_request_t *request)
{
   memset (request, 0, sizeof (*request));
}

void
_mongoc_http_response_init (mongoc_http_response_t *response)
{
   memset (response, 0, sizeof (*response));
}

void
_mongoc_http_response_cleanup (mongoc_http_response_t *response)
{
   if (!response) {
      return;
   }
   bson_free (response->headers);
   bson_free (response->body);
}

bson_string_t *
_mongoc_http_render_request_head (const mongoc_http_request_t *req)
{
   BSON_ASSERT_PARAM (req);
   char *path = NULL;

   // Default paths
   if (!req->path) {
      // Top path:
      path = bson_strdup ("/");
   } else if (req->path[0] != '/') {
      // Path MUST be prefixed with a separator
      path = bson_strdup_printf ("/%s", req->path);
   } else {
      // Just copy the path
      path = bson_strdup (req->path);
   }

   bson_string_t *const string = bson_string_new ("");
   // Set the request line
   bson_string_append_printf (string, "%s %s HTTP/1.0\r\n", req->method, path);
   // (We're done with the path string:)
   bson_free (path);

   /* Always add Host header. */
   bson_string_append_printf (string, "Host: %s:%d\r\n", req->host, req->port);
   /* Always add Connection: close header to ensure server closes connection. */
   bson_string_append_printf (string, "Connection: close\r\n");
   /* Add Content-Length if body is included. */
   if (req->body_len) {
      bson_string_append_printf (string, "Content-Length: %d\r\n", req->body_len);
   }
   // Add any extra headers
   if (req->extra_headers) {
      bson_string_append (string, req->extra_headers);
   }

   // Final terminator
   bson_string_append (string, "\r\n");
   return string;
}

static int32_t
_mongoc_http_msec_remaining (mcd_timer timer)
{
   const int64_t msec = mcd_get_milliseconds (mcd_timer_remaining (timer));
   BSON_ASSERT (bson_in_range_signed (int32_t, msec));
   return (int32_t) msec;
}

bool
_mongoc_http_send (const mongoc_http_request_t *req,
                   int timeout_ms,
                   bool use_tls,
                   mongoc_ssl_opt_t *ssl_opts,
                   mongoc_http_response_t *res,
                   bson_error_t *error)
{
   mongoc_stream_t *stream = NULL;
   mongoc_host_list_t host_list;
   bool ret = false;
   mongoc_iovec_t iovec;
   char *path = NULL;
   bson_string_t *http_request = NULL;
   mongoc_buffer_t http_response_buf;
   char *http_response_str;
   char *ptr;
   const char *header_delimiter = "\r\n\r\n";

   const mcd_timer timer = mcd_timer_expire_after (mcd_milliseconds (timeout_ms));

   memset (res, 0, sizeof (*res));
   _mongoc_buffer_init (&http_response_buf, NULL, 0, NULL, NULL);

   if (!_mongoc_host_list_from_hostport_with_err (&host_list, req->host, (uint16_t) req->port, error)) {
      goto fail;
   }

   stream = mongoc_client_connect_tcp (
      // +1 to prevent passing zero as a timeout
      _mongoc_http_msec_remaining (timer) + 1,
      &host_list,
      error);
   if (!stream) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to connect to: %s", req->host);
      goto fail;
   }

#ifndef MONGOC_ENABLE_SSL
   if (use_tls) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Failed to connect to %s: libmongoc not built with TLS support",
                      req->host);
      goto fail;
   }
#else
   if (use_tls) {
      mongoc_stream_t *tls_stream;

      BSON_ASSERT (ssl_opts);
      tls_stream = mongoc_stream_tls_new_with_hostname (stream, req->host, ssl_opts, true);
      if (!tls_stream) {
         bson_set_error (
            error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed create TLS stream to: %s", req->host);
         goto fail;
      }

      stream = tls_stream;
      if (!mongoc_stream_tls_handshake_block (stream, req->host, _mongoc_http_msec_remaining (timer), error)) {
         goto fail;
      }
   }
#endif

   if (!req->path) {
      path = bson_strdup ("/");
   } else if (req->path[0] != '/') {
      path = bson_strdup_printf ("/%s", req->path);
   } else {
      path = bson_strdup (req->path);
   }

   http_request = _mongoc_http_render_request_head (req);
   iovec.iov_base = http_request->str;
   iovec.iov_len = http_request->len;

   if (!_mongoc_stream_writev_full (stream, &iovec, 1, _mongoc_http_msec_remaining (timer), error)) {
      goto fail;
   }

   if (req->body && req->body_len) {
      iovec.iov_base = (void *) req->body;
      iovec.iov_len = req->body_len;
      if (!_mongoc_stream_writev_full (stream, &iovec, 1, _mongoc_http_msec_remaining (timer), error)) {
         goto fail;
      }
   }

   /* Read until connection close. */
   while (1) {
      const ssize_t bytes_read = _mongoc_buffer_try_append_from_stream (
         &http_response_buf, stream, 1024 * 32, _mongoc_http_msec_remaining (timer));
      if (mongoc_stream_should_retry (stream)) {
         continue;
      }
      if (bytes_read <= 0) {
         break;
      }
      if (http_response_buf.len > 1024 * 1024 * 8) {
         bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "HTTP response message is too large");
         goto fail;
      }
   }

   if (mongoc_stream_timed_out (stream)) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Timeout reading from stream");
      goto fail;
   }

   if (http_response_buf.len == 0) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "No response received");
      goto fail;
   }

   http_response_str = (char *) http_response_buf.data;
   const char *const resp_end_ptr = http_response_str + http_response_buf.len;


   const char *proto_leader_10 = "HTTP/1.0 ";
   const char *proto_leader_11 = "HTTP/1.1 ";
   ptr = strstr (http_response_str, proto_leader_10);
   if (!ptr) {
      ptr = strstr (http_response_str, proto_leader_11);
   }

   if (!ptr) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "No HTTP version leader in HTTP response. Expected '%s' or '%s'",
                      proto_leader_10,
                      proto_leader_11);
      goto fail;
   }

   /* Both protocol leaders have the same length. */
   ptr += strlen (proto_leader_10);
   ssize_t remain = resp_end_ptr - ptr;
   if (remain < 4) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Short read in HTTP response");
      goto fail;
   }

   char status_buf[4] = {0};
   memcpy (status_buf, ptr, 3);
   char *status_endptr;
   res->status = strtol (status_buf, &status_endptr, 10);
   if (status_endptr != status_buf + 3) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Invalid HTTP response status string %*.s",
                      4,
                      status_buf);
      goto fail;
   }

   /* Find the end of the headers. */
   ptr = strstr (http_response_str, header_delimiter);
   if (NULL == ptr) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Error occurred reading response: end of headers not found");
      goto fail;
   }

   const size_t headers_len = (size_t) (ptr - http_response_str);
   BSON_ASSERT (bson_in_range_unsigned (int, headers_len));

   const size_t body_len = http_response_buf.len - headers_len - strlen (header_delimiter);
   BSON_ASSERT (bson_in_range_unsigned (int, body_len));

   res->headers_len = (int) headers_len;
   res->headers = bson_strndup (http_response_str, (size_t) headers_len);
   res->body_len = (int) body_len;
   /* Add a NULL character in case caller assumes NULL terminated. */
   res->body = bson_malloc0 (body_len + 1u);
   memcpy (res->body, ptr + strlen (header_delimiter), body_len);
   ret = true;

fail:
   mongoc_stream_destroy (stream);
   if (http_request) {
      bson_string_free (http_request, true);
   }
   _mongoc_buffer_destroy (&http_response_buf);
   bson_free (path);
   return ret;
}
