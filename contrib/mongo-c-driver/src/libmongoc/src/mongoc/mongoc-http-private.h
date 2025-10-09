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

#include "mongoc.h"
#include "mongoc-ssl.h"

#include "mongoc-prelude.h"

#ifndef MONGOC_HTTP_PRIVATE_H
#define MONGOC_HTTP_PRIVATE_H

typedef struct {
   const char *host;
   int port;
   const char *method;
   const char *path;
   const char *extra_headers;
   const char *body;
   int body_len;
} mongoc_http_request_t;

typedef struct {
   int status;
   char *headers;
   int headers_len;
   char *body;
   int body_len;
} mongoc_http_response_t;

void
_mongoc_http_request_init (mongoc_http_request_t *request);

void
_mongoc_http_response_init (mongoc_http_response_t *response);

void
_mongoc_http_response_cleanup (mongoc_http_response_t *response);

/**
 * @brief Render the HTTP request head based on the given HTTP parameters.
 *
 * @param req The request to render (required)
 * @return bson_string_t* A new bson_string_t that contains the HTTP request
 * head
 *
 * @note The request body (if applicable) is not included in the resulting
 * string.
 * @note The returned bson_string_t must be freed, including the internal
 * segment.
 */
bson_string_t *
_mongoc_http_render_request_head (const mongoc_http_request_t *req);


/**
 * @brief Convenience function to send an HTTP request and receive an HTTP
 * response.
 *
 * This function only speaks HTTP 1.0, and does not maintain a persistent
 * connection. It does not handle 3xx redirects nor 1xx information.
 *
 * @param req The request to send. Uses the "host" attribute to determine the
 * HTTP peer.
 * @param timeout_ms A timeout for the request, in milliseconds
 * @param use_tls Whether the connection should use TLS.
 * @param ssl_opts Options to control TLS (Required only if 'use_tls' is true)
 * @param res Output parameter for the response. Must be uninitialized.
 * Required. This object must later be destroyed with
 * _mongoc_http_response_cleanup.
 * @param error An output parameter for any possible errors. These are errors
 * related to the HTTP transmission, and unrelated to any HTTP response.
 * (Optional)
 * @return true Upon success
 * @return false Otherwise, and sets "error"
 *
 * For more transport control, the HTTP request head content can be manually
 * rendered using @ref _mongo_http_render_request_head.
 */
bool
_mongoc_http_send (mongoc_http_request_t const *req,
                   int timeout_ms,
                   bool use_tls,
                   mongoc_ssl_opt_t *ssl_opts,
                   mongoc_http_response_t *res,
                   bson_error_t *error);

#endif /* MONGOC_HTTP_PRIVATE */
