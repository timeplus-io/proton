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

#ifndef MCD_AZURE_H_INCLUDED
#define MCD_AZURE_H_INCLUDED

#include "mongoc-prelude.h"

#include <mongoc/mongoc.h>

#include <mongoc/mongoc-http-private.h>

#include <mongoc/mcd-time.h>

/**
 * @brief An Azure OAuth2 access token obtained from the Azure API
 */
typedef struct mcd_azure_access_token {
   /// The access token string
   char *access_token;
   /// The resource of the token (the Azure resource for which it is valid)
   char *resource;
   /// The HTTP type of the token
   char *token_type;
   /// The duration after which it will the token will expires. This is relative
   /// to the "issue time" of the token.
   mcd_duration expires_in;
} mcd_azure_access_token;

/**
 * @brief Try to parse an Azure access token from an IMDS metadata JSON response
 *
 * @param out The token to initialize. Should be uninitialized. Must later be
 * destroyed by the caller.
 * @param json The JSON string body
 * @param len The length of 'body'
 * @param error An output parameter for errors
 * @retval true If 'out' was successfully initialized to a token.
 * @retval false Otherwise
 *
 * @note The 'out' token must later be given to @ref
 * mcd_azure_access_token_destroy
 */
bool
mcd_azure_access_token_try_init_from_json_str (mcd_azure_access_token *out,
                                               const char *json,
                                               int len,
                                               bson_error_t *error) BSON_GNUC_WARN_UNUSED_RESULT;

/**
 * @brief Destroy and zero-fill an access token object
 *
 * @param token The access token to destroy
 */
void
mcd_azure_access_token_destroy (mcd_azure_access_token *token);

/**
 * @brief An Azure IMDS HTTP request
 */
typedef struct mcd_azure_imds_request {
   /// The underlying HTTP request object to be sent
   mongoc_http_request_t req;
   char *_owned_path;
   char *_owned_host;
   char *_owned_headers;
} mcd_azure_imds_request;

#define MCD_AZURE_IMDS_REQUEST_INIT                                                 \
   (mcd_azure_imds_request)                                                         \
   {                                                                                \
      .req = {0}, ._owned_path = NULL, ._owned_host = NULL, ._owned_headers = NULL, \
   }

/**
 * @brief Initialize a new IMDS HTTP request
 *
 * @param out The object to initialize
 * @param opt_imds_host (Optional) the IP host of the IMDS server
 * @param opt_port (Optional) The port of the IMDS HTTP server (default is 80)
 * @param opt_extra_headers (Optional) Set extra HTTP headers for the request
 *
 * @note the request must later be destroyed with mcd_azure_imds_request_destroy
 * @note Currently only supports the vault.azure.net resource
 */
void
mcd_azure_imds_request_init (mcd_azure_imds_request *req,
                             const char *const opt_imds_host,
                             int opt_port,
                             const char *const opt_extra_headers);

/**
 * @brief Destroy an IMDS request created with mcd_azure_imds_request_init()
 *
 * @param req
 */
void
mcd_azure_imds_request_destroy (mcd_azure_imds_request *req);

/**
 * @brief Attempt to obtain a new OAuth2 access token from an Azure IMDS HTTP
 * server.
 *
 * @param out The output parameter for the obtained token. Must later be
 * destroyed
 * @param opt_imds_host (Optional) Override the IP host of the IMDS server
 * @param opt_port (Optional) The port of the IMDS HTTP server (default is 80)
 * @param opt_extra_headers (Optional) Set extra HTTP headers for the request
 * @param error Output parameter for errors
 * @retval true Upon success
 * @retval false Otherwise. Sets an error via `error`
 *
 * @note Currently only supports the vault.azure.net resource
 */
bool
mcd_azure_access_token_from_imds (mcd_azure_access_token *const out,
                                  const char *const opt_imds_host,
                                  int opt_port,
                                  const char *opt_extra_headers,
                                  bson_error_t *error);

#endif // MCD_AZURE_H_INCLUDED
