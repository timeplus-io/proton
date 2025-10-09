/**
 * Copyright 2022 MongoDB, Inc.
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

#ifndef SERVICE_GCP_H_INCLUDED
#define SERVICE_GCP_H_INCLUDED

#include "mongoc-prelude.h"

#include <mongoc/mongoc.h>

#include <mongoc/mongoc-http-private.h>

/**
 * @brief A GCP access token obtained from the GCP metadata server
 */
typedef struct gcp_service_account_token {
   /// The access token string
   char *access_token;
   // The HTTP type of the token
   char *token_type;
} gcp_service_account_token;

/**
 * @brief A GCP request
 */
typedef struct gcp_request {
   /// The underlying HTTP request object to be sent
   mongoc_http_request_t req;
   // optional parameters (used in testing) to override defaults
   char *_owned_path;
   char *_owned_host;
   char *_owned_headers;
} gcp_request;

/**
 * @brief Initialize a new GCP HTTP request
 *
 * @param out The object to initialize
 * @param opt_host (Optional) the IP host of the metadata server (default is
 * metadata.google.internal)
 * @param opt_port (Optional) The port of the HTTP server (default is 80)
 * @param opt_extra_headers (Optional) Set extra HTTP headers for the request
 *
 * @note the request must later be destroyed with gcp_request_destroy
 */
void
gcp_request_init (gcp_request *req, const char *const opt_host, int opt_port, const char *const opt_extra_headers);


/**
 * @brief Destroy an GCP request created with gcp_request_init()
 *
 * @param req
 */
void
gcp_request_destroy (gcp_request *req);

/**
 * @brief Destroy and zero-fill GCP service account token
 *
 * @param token The service account token to destory
 */
void
gcp_access_token_destroy (gcp_service_account_token *token);


/**
 * @brief Try to parse a GCP access token from the metadata server JSON response
 *
 * @param out The token to initialize. Should be uninitialized. Must later be
 * destroyed by the caller.
 * @param json The JSON string body
 * @param len The length of 'body'
 * @param error An output parameter for errors
 * @retval true If 'out' was successfully initialized to a token.
 * @retval false Otherwise
 *
 * @note The 'out' token must later be given to gcp_access_token_destroy
 */
bool
gcp_access_token_try_parse_from_json (gcp_service_account_token *out, const char *json, int len, bson_error_t *error);

/**
 * @brief Attempt to obtain a new GCP service account token from a GCP metadata
 * server.
 *
 * @param out The output parameter for the obtained token. Must later be
 * destroyed
 * @param opt_host (Optional) Override the IP host of the GCP server (used
 * in testing)
 * @param opt_port (Optional) The port of the HTTP server (default is 80)
 * (used in testing)
 * @param opt_extra_headers (Optional) Set extra HTTP headers for the request
 * (used in testing)
 * @param error Output parameter for errors
 * @retval true Upon success
 * @retval false Otherwise. Sets an error via `error`
 *
 */
bool
gcp_access_token_from_gcp_server (gcp_service_account_token *out,
                                  const char *opt_host,
                                  int opt_port,
                                  const char *opt_extra_headers,
                                  bson_error_t *error);

#endif /* SERVICE_GCP_H */
