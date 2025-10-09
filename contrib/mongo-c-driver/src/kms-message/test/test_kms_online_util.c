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

#include "test_kms_online_util.h"

#include "test_kms_assert.h"

#include "kms_message/kms_response_parser.h"


mongoc_stream_t *
connect_with_tls (const char *host, const char *port, mongoc_ssl_opt_t *ssl_opt)
{
   mongoc_stream_t *stream;
   mongoc_socket_t *sock = NULL;
   struct addrinfo hints;
   struct addrinfo *result, *rp;
   int64_t expire_at;
   int s;
   const int connecttimeoutms = 5000;

   memset (&hints, 0, sizeof hints);
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_flags = 0;
   hints.ai_protocol = 0;

   if (!port) {
      port = "443";
   }

   s = getaddrinfo (host, port, &hints, &result);
   ASSERT_CMPINT (s, ==, 0);

   for (rp = result; rp; rp = rp->ai_next) {
      if (!(sock = mongoc_socket_new (
               rp->ai_family, rp->ai_socktype, rp->ai_protocol))) {
         continue;
      }

      expire_at = bson_get_monotonic_time () + (connecttimeoutms * 1000L);
      if (0 !=
          mongoc_socket_connect (
             sock, rp->ai_addr, (mongoc_socklen_t) rp->ai_addrlen, expire_at)) {
         mongoc_socket_destroy (sock);
         sock = NULL;
         continue;
      }

      break;
   }

   if (!sock) {
      TEST_ERROR ("Failed to connect: %s", host);
   }

   freeaddrinfo (result);

   stream = mongoc_stream_socket_new (sock);
   ASSERT (stream);
   if (ssl_opt == NULL) {
      ssl_opt = (mongoc_ssl_opt_t *) mongoc_ssl_opt_get_default ();
   }
   return mongoc_stream_tls_new_with_hostname (
      stream, host, ssl_opt, 1);
}

/* Helper to send an HTTP request and receive a response. */
kms_response_t *
send_kms_request (kms_request_t *req, const char *host)
{
   mongoc_stream_t *tls_stream;
   char *req_str;
   int32_t socket_timeout_ms = 5000;
   ssize_t write_ret;
   kms_response_parser_t *response_parser;
   int bytes_to_read;
   int bytes_read;
   uint8_t buf[1024];
   kms_response_t *response;

   tls_stream = connect_with_tls (host, NULL, NULL);
   req_str = kms_request_to_string (req);

   write_ret = mongoc_stream_write (
      tls_stream, req_str, strlen (req_str), socket_timeout_ms);
   ASSERT_CMPINT ((int) write_ret, ==, (int) strlen (req_str));

   response_parser = kms_response_parser_new ();
   while ((bytes_to_read =
              kms_response_parser_wants_bytes (response_parser, 1024)) > 0) {
      bytes_read = (int) mongoc_stream_read (
         tls_stream, buf, bytes_to_read, 0, socket_timeout_ms);
      if (!kms_response_parser_feed (response_parser, buf, bytes_read)) {
         TEST_ERROR ("read failed: %s",
                     kms_response_parser_error (response_parser));
      }
   }

   response = kms_response_parser_get_response (response_parser);
   ASSERT (response);

   kms_request_free_string (req_str);
   kms_response_parser_destroy (response_parser);
   mongoc_stream_destroy (tls_stream);
   return response;
}