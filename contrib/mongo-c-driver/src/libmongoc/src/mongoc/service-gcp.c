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

#include "./service-gcp.h"

#include "mongoc-util-private.h"

#define HOST "metadata.google.internal"

static const char *const DEFAULT_METADATA_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";

void
gcp_request_init (gcp_request *req, const char *const opt_host, int opt_port, const char *const opt_extra_headers)
{
   BSON_ASSERT_PARAM (req);
   _mongoc_http_request_init (&req->req);

   // The HTTP host of the Google metadata server
   req->req.host = req->_owned_host = bson_strdup (opt_host ? opt_host : HOST);

   if (opt_port) {
      req->req.port = opt_port;
   } else {
      req->req.port = 80;
   }

   // Empty body
   req->req.body = "";
   // We GET
   req->req.method = "GET";

   req->req.extra_headers = req->_owned_headers =
      bson_strdup_printf ("Metadata-Flavor: Google\r\n%s", opt_extra_headers ? opt_extra_headers : "");

   req->req.path = req->_owned_path = bson_strdup (DEFAULT_METADATA_PATH);
}

void
gcp_request_destroy (gcp_request *req)
{
   BSON_ASSERT_PARAM (req);
   bson_free (req->_owned_headers);
   bson_free (req->_owned_host);
   bson_free (req->_owned_path);
   *req = (gcp_request){
      .req = {0},
      ._owned_path = NULL,
      ._owned_host = NULL,
      ._owned_headers = NULL,
   };
}

void
gcp_access_token_destroy (gcp_service_account_token *token)
{
   bson_free (token->access_token);
   bson_free (token->token_type);
   token->access_token = NULL;
   token->token_type = NULL;
}

bool
gcp_access_token_try_parse_from_json (gcp_service_account_token *out, const char *json, int len, bson_error_t *error)
{
   BSON_ASSERT_PARAM (out);
   BSON_ASSERT_PARAM (json);
   bool okay = false;

   // Zero the output
   *out = (gcp_service_account_token){0};

   // Parse the JSON data
   bson_t bson;
   if (!bson_init_from_json (&bson, json, len, error)) {
      return false;
   }

   bson_iter_t iter;
   // access_token
   bool found = bson_iter_init_find (&iter, &bson, "access_token");
   const char *const access_token = !found ? NULL : bson_iter_utf8 (&iter, NULL);
   // token_type
   found = bson_iter_init_find (&iter, &bson, "token_type");
   const char *const token_type = !found ? NULL : bson_iter_utf8 (&iter, NULL);

   if (!(access_token && token_type)) {
      bson_set_error (error,
                      MONGOC_ERROR_GCP,
                      MONGOC_ERROR_KMS_SERVER_BAD_JSON,
                      "One or more required JSON properties are "
                      "missing/invalid: data: %.*s",
                      len,
                      json);
      goto done;
   }

   *out = (gcp_service_account_token){
      .access_token = bson_strdup (access_token),
      .token_type = bson_strdup (token_type),
   };
   okay = true;

done:
   bson_destroy (&bson);
   return okay;
}

bool
gcp_access_token_from_gcp_server (gcp_service_account_token *out,
                                  const char *opt_host,
                                  int opt_port,
                                  const char *opt_extra_headers,
                                  bson_error_t *error)
{
   BSON_ASSERT_PARAM (out);
   bool okay = false;

   // Clear the output
   *out = (gcp_service_account_token){0};

   mongoc_http_response_t resp;
   _mongoc_http_response_init (&resp);

   gcp_request req = {
      .req = {0},
      ._owned_path = NULL,
      ._owned_host = NULL,
      ._owned_headers = NULL,
   };
   gcp_request_init (&req, opt_host, opt_port, opt_extra_headers);

   if (!_mongoc_http_send (&req.req, 3 * 1000, false, NULL, &resp, error)) {
      goto fail;
   }

   // Only accept an HTTP 200 as success
   if (resp.status != 200) {
      bson_set_error (error,
                      MONGOC_ERROR_GCP,
                      MONGOC_ERROR_KMS_SERVER_HTTP,
                      "Error from the GCP metadata server while looking for "
                      "access token: %.*s",
                      resp.body_len,
                      resp.body);
      goto fail;
   }

   if (!gcp_access_token_try_parse_from_json (out, resp.body, resp.body_len, error)) {
      goto fail;
   }

   okay = true;

fail:
   gcp_request_destroy (&req);
   _mongoc_http_response_cleanup (&resp);
   return okay;
}
