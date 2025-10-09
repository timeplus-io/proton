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

/* All interaction with kms_message is limited to this file. */

#include "common-b64-private.h"
#include "mcd-time.h"
#include "mongoc-cluster-aws-private.h"
#include "mongoc-client-private.h"
#include "mongoc-host-list-private.h"
#include "mongoc-rand-private.h"
#include "mongoc-stream-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-uri-private.h"
#include "mongoc-util-private.h"
#include "mongoc-http-private.h"
#include "mongoc-rand-private.h"
#include "mongoc-ssl-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "aws_auth"

#define AUTH_ERROR_AND_FAIL(...)                                                                  \
   do {                                                                                           \
      bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, __VA_ARGS__); \
      goto fail;                                                                                  \
   } while (0)


#ifdef MONGOC_ENABLE_MONGODB_AWS_AUTH
#include "kms_message/kms_message.h"

/*
 * Run a single command on a stream.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * reply is always initialized.
 */
static bool
_run_command (mongoc_cluster_t *cluster,
              mongoc_stream_t *stream,
              mongoc_server_description_t *sd,
              bson_t *command,
              bson_t *reply,
              bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   mongoc_server_stream_t *server_stream;
   bool ret;
   mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (cluster)->client->topology);

   mongoc_cmd_parts_init (&parts, cluster->client, "$external", MONGOC_QUERY_NONE /* unused for OP_MSG */, command);
   /* Drivers must not append session ids to auth commands per sessions spec. */
   parts.prohibit_lsid = true;
   server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);
   mc_tpld_drop_ref (&td);
   ret = mongoc_cluster_run_command_parts (cluster, server_stream, &parts, reply, error);
   mongoc_server_stream_cleanup (server_stream);
   return ret;
}

/*
 * Utility function to parse out a server reply's payload.
 *
 * Given a server reply like { ok: 1, payload: <BSON data>, ... } parse out the
 * payload into a bson_t.
 * On success, returns true.
 * On failure, returns false and sets error.
 * payload is always initialized.
 */
static bool
_sasl_reply_parse_payload_as_bson (const bson_t *reply, bson_t *payload, bson_error_t *error)
{
   bson_iter_t iter;
   bson_subtype_t payload_subtype;
   const uint8_t *payload_data;
   uint32_t payload_len;
   bool ret = false;

   bson_init (payload);

   if (!bson_iter_init_find (&iter, reply, "payload") || !BSON_ITER_HOLDS_BINARY (&iter)) {
      AUTH_ERROR_AND_FAIL ("server reply did not contain binary payload");
   }

   bson_iter_binary (&iter, &payload_subtype, &payload_len, &payload_data);

   if (payload_subtype != BSON_SUBTYPE_BINARY) {
      AUTH_ERROR_AND_FAIL ("server reply contained unexpected binary subtype");
   }

   bson_destroy (payload);
   if (!bson_init_static (payload, payload_data, payload_len)) {
      AUTH_ERROR_AND_FAIL ("server payload is invalid BSON");
   }

   ret = true;
fail:
   return ret;
}


/*
 * Send an HTTP request and get a response.
 * On success, returns true.
 * On failure, returns false and sets error.
 * headers is a \r\n delimitted list of headers (or an empty string).
 * http_response_body is always set, and must be freed.
 * http_response_headers is always set, and must be freed. This may be used for
 * error reporting since the response headers should not include sensitive
 * credentials.
 */
static bool
_send_http_request (bool use_tls,
                    const char *ip,
                    int port,
                    const char *method,
                    const char *path,
                    const char *headers,
                    char **http_response_body,
                    char **http_response_headers,
                    bson_error_t *error)
{
   mongoc_http_request_t req;
   mongoc_http_response_t res;
   const int socket_timeout_ms = 10000;
   bool ret;
   mongoc_ssl_opt_t ssl_opt = {0};

   *http_response_body = NULL;
   *http_response_headers = NULL;
   _mongoc_http_request_init (&req);
   _mongoc_http_response_init (&res);

   req.host = ip;
   req.port = port;
   req.method = method;
   req.path = path;
   req.extra_headers = headers;
   if (use_tls) {
      _mongoc_ssl_opts_copy_to (mongoc_ssl_opt_get_default (), &ssl_opt, true /* copy_internal */);
   }
   ret = _mongoc_http_send (&req, socket_timeout_ms, use_tls /* use_tls */, use_tls ? &ssl_opt : NULL, &res, error);

   if (ret) {
      *http_response_headers = bson_strndup (res.headers, res.headers_len);
      *http_response_body = (char *) bson_malloc0 (res.body_len + 1);
      memcpy (*http_response_body, res.body, res.body_len);
   }

   _mongoc_http_response_cleanup (&res);
   _mongoc_ssl_opts_cleanup (&ssl_opt, true);
   return ret;
}


static bool
_creds_empty (_mongoc_aws_credentials_t *creds)
{
   return creds->access_key_id == NULL && creds->secret_access_key == NULL && creds->session_token == NULL;
}

/*
 * Helper to validate and possibly set credentials.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * Caller should use _creds_empty to determine whether credentials have been
 * set.
 */
static bool
_validate_and_set_creds (const char *access_key_id,
                         const char *secret_access_key,
                         const char *session_token,
                         _mongoc_aws_credentials_t *creds,
                         bson_error_t *error)
{
   bool has_access_key_id = access_key_id && strlen (access_key_id) != 0;
   bool has_secret_access_key = secret_access_key && strlen (secret_access_key) != 0;
   bool has_session_token = session_token && strlen (session_token) != 0;
   bool ret = false;

   /* Check for invalid combinations of URI parameters. */
   if (has_access_key_id && !has_secret_access_key) {
      AUTH_ERROR_AND_FAIL ("ACCESS_KEY_ID is set, but SECRET_ACCESS_KEY is missing");
   }

   if (!has_access_key_id && has_secret_access_key) {
      AUTH_ERROR_AND_FAIL ("SECRET_ACCESS_KEY is set, but ACCESS_KEY_ID is missing");
   }

   if (!has_access_key_id && !has_secret_access_key && has_session_token) {
      AUTH_ERROR_AND_FAIL ("AWS_SESSION_TOKEN is set, but ACCESS_KEY_ID and "
                           "SECRET_ACCESS_KEY are missing");
   }

   creds->access_key_id = bson_strdup (access_key_id);
   creds->secret_access_key = bson_strdup (secret_access_key);
   creds->session_token = session_token ? bson_strdup (session_token) : NULL;

   ret = true;
fail:
   return ret;
}

/*
 * Validate and possibly set credentials.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * Caller should use _creds_empty to determine whether credentials have been
 * set.
 */
static bool
_obtain_creds_from_uri (_mongoc_aws_credentials_t *creds, mongoc_uri_t *uri, bson_error_t *error)
{
   bool ret = false;
   bson_t auth_mechanism_props;
   const char *uri_session_token = NULL;

   if (mongoc_uri_get_mechanism_properties (uri, &auth_mechanism_props)) {
      bson_iter_t iter;
      if (bson_iter_init_find_case (&iter, &auth_mechanism_props, "AWS_SESSION_TOKEN") &&
          BSON_ITER_HOLDS_UTF8 (&iter)) {
         uri_session_token = bson_iter_utf8 (&iter, NULL);
      }
   }

   if (!_validate_and_set_creds (
          mongoc_uri_get_username (uri), mongoc_uri_get_password (uri), uri_session_token, creds, error)) {
      goto fail;
   }

   ret = true;
fail:
   return ret;
}

static bool
_obtain_creds_from_env (_mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   bool ret = false;
   char *env_access_key_id = NULL;
   char *env_secret_access_key = NULL;
   char *env_session_token = NULL;

   /* Check environment variables. */
   env_access_key_id = _mongoc_getenv ("AWS_ACCESS_KEY_ID");
   env_secret_access_key = _mongoc_getenv ("AWS_SECRET_ACCESS_KEY");
   env_session_token = _mongoc_getenv ("AWS_SESSION_TOKEN");

   if (!_validate_and_set_creds (env_access_key_id, env_secret_access_key, env_session_token, creds, error)) {
      goto fail;
   }
   ret = true;
fail:
   bson_free (env_access_key_id);
   bson_free (env_secret_access_key);
   bson_free (env_session_token);
   return ret;
}

// expiration_ms_to_timer converts milliseconds since Unix Epoch into the
// mcd_timer `expiration_timer`.
static bool
expiration_ms_to_timer (int64_t expiration_ms, mcd_timer *expiration_timer, bson_error_t *error)
{
   bool ret = false;

   // get current time in milliseconds since Unix Epoch.
   int64_t now_ms;
   {
      struct timeval now;
      if (0 != bson_gettimeofday (&now)) {
         AUTH_ERROR_AND_FAIL ("bson_gettimeofday returned failure. Unable to "
                              "determine expiration.");
      } else {
         now_ms = (1000 * now.tv_sec) + (now.tv_usec / 1000);
      }
   }

   *expiration_timer =
      mcd_timer_expire_after (mcd_milliseconds (expiration_ms - now_ms - MONGOC_AWS_CREDENTIALS_EXPIRATION_WINDOW_MS));
   ret = true;
fail:
   return ret;
}

// expiration_iso8601_to_timer parses the "Expiration" string value returned
// from an ECS or EC2 response. "Expiration" is expected to be an ISO-8601
// string. Example: "2023-02-07T20:04:27Z". On success, `expiration_timer` is
// set to the expiration time.
static bool
expiration_iso8601_to_timer (const char *expiration_str, mcd_timer *expiration_timer, bson_error_t *error)
{
   bool ret = false;

   // get expiration time in milliseconds since Unix Epoch.
   int64_t expiration_ms;
   {
      bson_error_t json_err;
      bson_t date_doc;
      bson_iter_t date_iter;
      char *date_doc_str;
      // libbson has private API `_bson_iso8601_date_parse` to parse ISO-8601
      // strings. The private API is inaccessible to libmongoc.
      // Create a temporary bson document with a $date to parse.
      date_doc_str = bson_strdup_printf ("{\"Expiration\" : {\"$date\" : \"%s\"}}", expiration_str);

      if (!bson_init_from_json (&date_doc, date_doc_str, -1, &json_err)) {
         bson_free (date_doc_str);
         AUTH_ERROR_AND_FAIL ("failed to parse Expiration: %s", json_err.message);
      }
      BSON_ASSERT (bson_iter_init_find (&date_iter, &date_doc, "Expiration"));
      expiration_ms = bson_iter_date_time (&date_iter);
      bson_free (date_doc_str);
      bson_destroy (&date_doc);
   }

   if (!expiration_ms_to_timer (expiration_ms, expiration_timer, error)) {
      goto fail;
   }
   ret = true;
fail:
   return ret;
}

// generate_AWS_ROLE_SESSION_NAME generates a 16 byte hex string (32 characters)
// to use as the AWS_ROLE_SESSION_NAME.
static char *
generate_AWS_ROLE_SESSION_NAME (bson_error_t *error)
{
#define NUM_BYTES 16
   bool ok = false;
   uint8_t data[NUM_BYTES];
   char *out = bson_malloc (NUM_BYTES * 2 + 1);
   if (!_mongoc_rand_bytes (data, NUM_BYTES)) {
      AUTH_ERROR_AND_FAIL ("unable to generate random bytes for AWS_ROLE_SESSION_NAME");
   }

   size_t i;
   for (i = 0; i < NUM_BYTES; i++) {
      bson_snprintf (out + (2 * i), 3, "%02x", data[i]);
   }
   out[NUM_BYTES * 2] = '\0';

   ok = true;
fail:
   if (!ok) {
      bson_free (out);
      out = NULL;
   }
   return out;
#undef NUM_BYTES
}

static bool
_obtain_creds_from_ecs (_mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   bool ret = false;
   char *http_response_headers = NULL;
   char *http_response_body = NULL;
   char *relative_ecs_uri = NULL;
   bson_t *response_json = NULL;
   bson_iter_t iter;
   const char *ecs_access_key_id = NULL;
   const char *ecs_secret_access_key = NULL;
   const char *ecs_session_token = NULL;
   bson_error_t http_error;

   relative_ecs_uri = _mongoc_getenv ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
   if (!relative_ecs_uri || strlen (relative_ecs_uri) == 0) {
      bson_free (relative_ecs_uri);
      return true;
   }

   if (!_send_http_request (false /* use_tls */,
                            "169.254.170.2",
                            80,
                            "GET",
                            relative_ecs_uri,
                            "",
                            &http_response_body,
                            &http_response_headers,
                            &http_error)) {
      AUTH_ERROR_AND_FAIL ("failed to contact ECS link local server: %s", http_error.message);
   }

   response_json = bson_new_from_json ((const uint8_t *) http_response_body, strlen (http_response_body), error);
   if (!response_json) {
      AUTH_ERROR_AND_FAIL ("invalid JSON in ECS response. Response headers: %s", http_response_headers);
   }

   if (bson_iter_init_find_case (&iter, response_json, "AccessKeyId") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ecs_access_key_id = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "SecretAccessKey") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ecs_secret_access_key = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "Token") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ecs_session_token = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "Expiration") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      if (!expiration_iso8601_to_timer (bson_iter_utf8 (&iter, NULL), &creds->expiration.value, error)) {
         goto fail;
      }
      creds->expiration.set = true;
   }

   if (!_validate_and_set_creds (ecs_access_key_id, ecs_secret_access_key, ecs_session_token, creds, error)) {
      goto fail;
   }

   ret = true;
fail:
   bson_destroy (response_json);
   bson_free (http_response_headers);
   bson_free (http_response_body);
   bson_free (relative_ecs_uri);
   return ret;
}

static bool
_obtain_creds_from_assumerolewithwebidentity (_mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   bool ret = false;
   char *http_response_headers = NULL;
   char *http_response_body = NULL;
   char *aws_web_identity_token_file = NULL;
   char *aws_role_arn = NULL;
   char *aws_role_session_name = NULL;
   bson_t *response_bson = NULL;
   bson_iter_t iter;
   const char *access_key_id = NULL;
   const char *secret_access_key = NULL;
   const char *session_token = NULL;
   bson_error_t http_error;
   mongoc_stream_t *fstream = NULL;
   bson_string_t *token_file_contents = NULL;
   char *path_and_query = NULL;

   aws_web_identity_token_file = _mongoc_getenv ("AWS_WEB_IDENTITY_TOKEN_FILE");
   aws_role_arn = _mongoc_getenv ("AWS_ROLE_ARN");
   if (!aws_web_identity_token_file || strlen (aws_web_identity_token_file) == 0 || !aws_role_arn ||
       strlen (aws_role_arn) == 0) {
      bson_free (aws_role_arn);
      bson_free (aws_web_identity_token_file);
      // Not an error. May need to obtain credentials another way.
      return true;
   }

   aws_role_session_name = _mongoc_getenv ("AWS_ROLE_SESSION_NAME");
   if (!aws_role_session_name) {
      aws_role_session_name = generate_AWS_ROLE_SESSION_NAME (error);
      if (!aws_role_session_name) {
         goto fail;
      }
   }

   // Read the contents of the file given by ``AWS_WEB_IDENTITY_TOKEN_FILE``.
   {
      fstream = mongoc_stream_file_new_for_path (aws_web_identity_token_file, O_RDONLY, 0);
      if (!fstream) {
         AUTH_ERROR_AND_FAIL ("failed to open AWS_WEB_IDENTITY_TOKEN_FILE: %s. Reason: %s",
                              aws_web_identity_token_file,
                              strerror (errno));
      }

      token_file_contents = bson_string_new (NULL);

      for (;;) {
         char buf[128];
         ssize_t got = mongoc_stream_read (fstream,
                                           buf,
                                           sizeof (buf) - 1 /* leave space for null terminator */,
                                           0 /* min_bytes */,
                                           0 /* timeout_msec. Unused for file stream. */);

         if (got > 0) {
            // add null terminator.
            buf[got] = '\0';
            bson_string_append (token_file_contents, (const char *) buf);
         } else if (got == 0) {
            // EOF.
            break;
         } else {
            AUTH_ERROR_AND_FAIL ("failed to read AWS_WEB_IDENTITY_TOKEN_FILE: %s. Reason: %s",
                                 aws_web_identity_token_file,
                                 strerror (errno));
         }
      }
   }

   path_and_query = bson_strdup_printf ("/?Action=AssumeRoleWithWebIdentity"
                                        "&RoleSessionName=%s"
                                        "&RoleArn=%s"
                                        "&WebIdentityToken=%s&Version=2011-06-15",
                                        aws_role_session_name,
                                        aws_role_arn,
                                        token_file_contents->str);

   // send an HTTP request to sts.amazonaws.com.
   if (!_send_http_request (true /* use_tls */,
                            "sts.amazonaws.com",
                            443,
                            "POST",
                            path_and_query,
                            "Accept: application/json\r\n",
                            &http_response_body,
                            &http_response_headers,
                            &http_error)) {
      AUTH_ERROR_AND_FAIL ("failed to contact sts.amazonaws.com: %s", http_error.message);
   }

   response_bson = bson_new_from_json ((const uint8_t *) http_response_body, strlen (http_response_body), error);
   if (!response_bson) {
      AUTH_ERROR_AND_FAIL ("invalid JSON in response from sts.amazonaws.com. "
                           "Response headers: %s",
                           http_response_headers);
   }

   if (!bson_iter_init (&iter, response_bson)) {
      AUTH_ERROR_AND_FAIL ("failed to initialize BSON iterator to response: %s", http_response_body);
   }

   // If the response contains "Error", return only the "Error" document.
   // Do not include http_response_body in subsequent errors to limit risk of
   // including credentials in error messages.
   bson_iter_t Error_iter;
   if (bson_iter_init_find (&Error_iter, response_bson, "Error")) {
      bson_t Error_bson;
      if (!_mongoc_iter_document_as_bson (&Error_iter, &Error_bson, error)) {
         goto fail;
      }
      char *Error_json = bson_as_json (&Error_bson, NULL);
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_AUTHENTICATE,
                      "Response to AssumeRoleWithWebIdentity contains 'Error': %s",
                      Error_json);
      bson_free (Error_json);
      goto fail;
   }

   bson_iter_t Credentials_iter;
   if (!bson_iter_find_descendant (&iter,
                                   "AssumeRoleWithWebIdentityResponse.AssumeRoleWithWebIdentityResult."
                                   "Credentials",
                                   &iter)) {
      AUTH_ERROR_AND_FAIL ("did not find "
                           "AssumeRoleWithWebIdentityResponse.AssumeRoleWithWebIdentityResult."
                           "Credentials in response from sts.amazonaws.com.");
   }

   if (!bson_iter_recurse (&iter, &Credentials_iter)) {
      AUTH_ERROR_AND_FAIL ("Unable to recurse in Credentials in response from sts.amazonaws.com");
   }

   iter = Credentials_iter;
   if (bson_iter_find (&iter, "AccessKeyId") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      access_key_id = bson_iter_utf8 (&iter, NULL);
   } else {
      AUTH_ERROR_AND_FAIL ("did not find AccessKeyId in response from sts.amazonaws.com");
   }

   iter = Credentials_iter;
   if (bson_iter_find (&iter, "SecretAccessKey") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      secret_access_key = bson_iter_utf8 (&iter, NULL);
   } else {
      AUTH_ERROR_AND_FAIL ("did not find SecretAccessKey in response from sts.amazonaws.com");
   }

   iter = Credentials_iter;
   if (bson_iter_find (&iter, "SessionToken") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      session_token = bson_iter_utf8 (&iter, NULL);
   } else {
      AUTH_ERROR_AND_FAIL ("did not find SessionToken in response from sts.amazonaws.com");
   }

   iter = Credentials_iter;
   if (bson_iter_find (&iter, "Expiration") && BSON_ITER_HOLDS_DOUBLE (&iter)) {
      // "Expiration" is returned as a double representing the number of seconds
      // since Unix Epoch. This differs from the ISO-8601 string returned in EC2
      // and ECS responses.
      int64_t expiration_ms = (int64_t) (1000.0 * bson_iter_double (&iter));
      if (!expiration_ms_to_timer (expiration_ms, &creds->expiration.value, error)) {
         goto fail;
      }
      creds->expiration.set = true;
   } else {
      AUTH_ERROR_AND_FAIL ("did not find Expiration in response from sts.amazonaws.com");
   }

   if (!_validate_and_set_creds (access_key_id, secret_access_key, session_token, creds, error)) {
      goto fail;
   }

   ret = true;
fail:
   bson_free (path_and_query);
   bson_destroy (response_bson);
   bson_free (http_response_headers);
   bson_free (http_response_body);
   bson_string_free (token_file_contents, true /* free segment */);
   mongoc_stream_destroy (fstream);
   bson_free (aws_role_session_name);
   bson_free (aws_role_arn);
   bson_free (aws_web_identity_token_file);
   return ret;
}

static bool
_obtain_creds_from_ec2 (_mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   bool ret = false;
   char *http_response_headers = NULL;
   char *http_response_body = NULL;
   char *token_header = NULL;
   char *token = NULL;
   char *role_name = NULL;
   char *relative_ecs_uri = NULL;
   char *path_with_role = NULL;
   bson_t *response_json = NULL;
   bson_iter_t iter;
   const char *ec2_access_key_id = NULL;
   const char *ec2_secret_access_key = NULL;
   const char *ec2_session_token = NULL;
   bson_error_t http_error;
   const char *ip = "169.254.169.254";

   /* Get the token. */
   if (!_send_http_request (false /* use_tls */,
                            ip,
                            80,
                            "PUT",
                            "/latest/api/token",
                            "X-aws-ec2-metadata-token-ttl-seconds: 30\r\n",
                            &token,
                            &http_response_headers,
                            &http_error)) {
      AUTH_ERROR_AND_FAIL ("failed to contact EC2 link local server: %s", http_error.message);
   }

   if (0 == strlen (token)) {
      AUTH_ERROR_AND_FAIL ("unable to retrieve token from EC2 metadata. Headers: %s", http_response_headers);
   }

   bson_free (http_response_headers);
   http_response_headers = NULL;
   token_header = bson_strdup_printf ("X-aws-ec2-metadata-token: %s\r\n", token);

   /* Get the role name. */
   if (!_send_http_request (false /* use_tls */,
                            ip,
                            80,
                            "GET",
                            "/latest/meta-data/iam/security-credentials/",
                            token_header,
                            &role_name,
                            &http_response_headers,
                            &http_error)) {
      AUTH_ERROR_AND_FAIL ("failed to contact EC2 link local server: %s", http_error.message);
   }

   if (0 == strlen (role_name)) {
      AUTH_ERROR_AND_FAIL ("unable to retrieve role_name from EC2 metadata. Headers: %s", http_response_headers);
   }

   /* Get the creds. */
   path_with_role = bson_strdup_printf ("/latest/meta-data/iam/security-credentials/%s", role_name);
   bson_free (http_response_headers);
   http_response_headers = NULL;
   if (!_send_http_request (false /* use_tls */,
                            ip,
                            80,
                            "GET",
                            path_with_role,
                            token_header,
                            &http_response_body,
                            &http_response_headers,
                            &http_error)) {
      AUTH_ERROR_AND_FAIL ("failed to contact EC2 link local server: %s", http_error.message);
   }

   response_json = bson_new_from_json ((const uint8_t *) http_response_body, strlen (http_response_body), error);
   if (!response_json) {
      AUTH_ERROR_AND_FAIL ("invalid JSON in EC2 response. Response headers: %s", http_response_headers);
   }

   if (bson_iter_init_find_case (&iter, response_json, "AccessKeyId") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ec2_access_key_id = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "SecretAccessKey") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ec2_secret_access_key = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "Token") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      ec2_session_token = bson_iter_utf8 (&iter, NULL);
   }

   if (bson_iter_init_find_case (&iter, response_json, "Expiration") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      if (!expiration_iso8601_to_timer (bson_iter_utf8 (&iter, NULL), &creds->expiration.value, error)) {
         goto fail;
      }
      creds->expiration.set = true;
   }

   if (!_validate_and_set_creds (ec2_access_key_id, ec2_secret_access_key, ec2_session_token, creds, error)) {
      goto fail;
   }

   ret = true;
fail:
   bson_destroy (response_json);
   bson_free (http_response_headers);
   bson_free (http_response_body);
   bson_free (token);
   bson_free (role_name);
   bson_free (token_header);
   bson_free (relative_ecs_uri);
   bson_free (path_with_role);
   return ret;
}

/*
 * Attempt to obtain AWS credentials.
 *
 * Credentials may be passed in multiple ways. The precedence is as follows:
 * 1. Username/password in the URI (and authMechanismProperty for session token)
 * 2. From environment variables.
 * 3. From querying the ECS local HTTP server.
 * 4. From querying the EC2 local HTTP server.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 */
bool
_mongoc_aws_credentials_obtain (mongoc_uri_t *uri, _mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   bool ret = false;

   BSON_ASSERT_PARAM (creds);
   *creds = MONGOC_AWS_CREDENTIALS_INIT;

   // Check cache before enviroment variables. This is required by the
   // specification: "Even if the environment variables are present in
   // subsequent authorization attempts, the driver MUST use the cached
   // credentials"
   if (_mongoc_aws_credentials_cache_get (creds)) {
      goto succeed;
   }

   if (uri) {
      TRACE ("%s", "checking URI for credentials");
      if (!_obtain_creds_from_uri (creds, uri, error)) {
         goto fail;
      }
      if (!_creds_empty (creds)) {
         goto succeed;
      }
   }

   TRACE ("%s", "checking environment variables for credentials");
   if (!_obtain_creds_from_env (creds, error)) {
      goto fail;
   }
   if (!_creds_empty (creds)) {
      goto succeed;
   }

   // Try to fetch credentials from cacheable sources:
   // AssumeRoleWithWebIdentity, ECS or EC2. Lock the cache to prevent duplicate
   // requests.
   {
      _mongoc_aws_credentials_cache_lock ();

      // Check again if credentials have been cached.
      if (_mongoc_aws_credentials_cache_get_nolock (creds)) {
         _mongoc_aws_credentials_cache_unlock ();
         goto succeed;
      }

      TRACE ("%s", "checking AssumeRoleWithWebIdentity for credentials");
      if (!_obtain_creds_from_assumerolewithwebidentity (creds, error)) {
         _mongoc_aws_credentials_cache_unlock ();
         goto fail;
      }
      if (!_creds_empty (creds)) {
         if (creds->expiration.set) {
            // Only try to cache credentials if an expiration time is included.
            _mongoc_aws_credentials_cache_put_nolock (creds);
         }
         _mongoc_aws_credentials_cache_unlock ();
         goto succeed;
      }

      TRACE ("%s", "checking ECS metadata for credentials");
      if (!_obtain_creds_from_ecs (creds, error)) {
         _mongoc_aws_credentials_cache_unlock ();
         goto fail;
      }
      if (!_creds_empty (creds)) {
         if (creds->expiration.set) {
            // Only try to cache credentials if an expiration time is included.
            _mongoc_aws_credentials_cache_put_nolock (creds);
         }
         _mongoc_aws_credentials_cache_unlock ();
         goto succeed;
      }

      TRACE ("%s", "checking EC2 metadata for credentials");
      if (!_obtain_creds_from_ec2 (creds, error)) {
         _mongoc_aws_credentials_cache_unlock ();
         goto fail;
      }
      if (!_creds_empty (creds)) {
         if (creds->expiration.set) {
            // Only try to cache credentials if an expiration time is included.
            _mongoc_aws_credentials_cache_put_nolock (creds);
         }
         _mongoc_aws_credentials_cache_unlock ();
         goto succeed;
      }

      _mongoc_aws_credentials_cache_unlock ();
   }

   AUTH_ERROR_AND_FAIL ("unable to get credentials\n");

succeed:
   ret = true;
fail:
   return ret;
}

/*
 * Validate the STS host returned by the server and derive the region.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * region is always set and must be freed by caller.
 */
bool
_mongoc_validate_and_derive_region (char *sts_fqdn, size_t sts_fqdn_len, char **region, bson_error_t *error)
{
   bool ret = false;
   char *ptr;
   char *ptr_prev;
   char *second_part = NULL;

   /* Default to us-east-1. */
   *region = bson_strdup ("us-east-1");

   /* Drivers must also validate that the host is greater than 0 and less than
    * or equal to 255 bytes per RFC 1035 */
   if (sts_fqdn_len == 0u) {
      AUTH_ERROR_AND_FAIL ("invalid STS host: empty");
   }

   if (sts_fqdn_len > 255u) {
      AUTH_ERROR_AND_FAIL ("invalid STS host: too large");
   }

   /* If sts.amazonaws.com, then use default region. */
   if (0 == bson_strcasecmp ("sts.amazonaws.com", sts_fqdn)) {
      goto succeed;
   }

   /* Drivers MUST reject FQDN names with empty labels, e.g., "abc..def" */
   ptr_prev = sts_fqdn;
   ptr = strstr (sts_fqdn, ".");
   if (ptr) {
      second_part = ptr + 1;
   }
   if (0 == ptr - sts_fqdn) {
      AUTH_ERROR_AND_FAIL ("invalid STS host: empty part");
   }
   while (ptr) {
      if (1 == ptr - ptr_prev) {
         AUTH_ERROR_AND_FAIL ("invalid STS host: empty part");
      }
      ptr_prev = ptr;
      ptr = strstr (ptr + 1, ".");
   }
   if (strlen (ptr_prev + 1) == 0) {
      AUTH_ERROR_AND_FAIL ("invalid STS host: empty part");
   }

   if (second_part) {
      char *second_part_end;

      second_part_end = strstr (second_part, ".");
      bson_free (*region);
      if (!second_part_end) {
         *region = bson_strdup (second_part);
      } else {
         *region = bson_strndup (second_part, second_part_end - second_part);
      }
   }

succeed:
   ret = true;
fail:
   return ret;
}

/* --------------------------------------------------------------------------
 * Step 1
 * --------------------------------------------------------------------------
 * Client sends BSON payload:
 * {
 *   "r": <32 byte client nonce>,
 *   "p": 110
 * }
 * Server responds with BSON payload:
 * {
 *   "s": <32 byte client nonce + 32 byte server nonce>,
 *   "h": <domain name of STS service>
 * }
 *
 * Payloads are wrapped in SASL commands. The command a client sends is like:
 * { "saslStart": 1, "mechanism": "MONGODB-AWS", "payload": <BSON payload> }
 * And similar for server responses:
 * { "ok": 1, "conversationId": 1, "done": false, "payload": <BSON payload> }
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * --------------------------------------------------------------------------
 */
static bool
_client_first (mongoc_cluster_t *cluster,
               mongoc_stream_t *stream,
               mongoc_server_description_t *sd,
               uint8_t *server_nonce,
               char **sts_fqdn,
               char **region,
               int *conv_id,
               bson_error_t *error)
{
   bool ret = false;
   uint8_t client_nonce[32];
   bson_t client_payload = BSON_INITIALIZER;
   bson_t client_command = BSON_INITIALIZER;
   bson_t server_payload = BSON_INITIALIZER;
   bson_t server_reply = BSON_INITIALIZER;
   bson_iter_t iter;
   bson_subtype_t reply_nonce_subtype;
   const uint8_t *reply_nonce_data;
   uint32_t reply_nonce_len;
   uint32_t sts_fqdn_len;

   /* Reset out params. */
   memset (server_nonce, 0, 32);
   *sts_fqdn = NULL;
   *region = NULL;
   *conv_id = 0;

#ifdef MONGOC_ENABLE_CRYPTO
   /* Generate secure random nonce. */
   if (!_mongoc_rand_bytes (client_nonce, 32)) {
      AUTH_ERROR_AND_FAIL ("Could not generate client nonce");
   }
#else
   AUTH_ERROR_AND_FAIL ("libmongoc requires a cryptography library (libcrypto, "
                        "Common Crypto, or cng) to support MONGODB-AWS");
#endif

   BCON_APPEND (&client_payload, "r", BCON_BIN (BSON_SUBTYPE_BINARY, client_nonce, 32), "p", BCON_INT32 (110));

   BCON_APPEND (&client_command,
                "saslStart",
                BCON_INT32 (1),
                "mechanism",
                "MONGODB-AWS",
                "payload",
                BCON_BIN (BSON_SUBTYPE_BINARY, bson_get_data (&client_payload), client_payload.len));

   bson_destroy (&server_reply);
   if (!_run_command (cluster, stream, sd, &client_command, &server_reply, error)) {
      goto fail;
   }

   *conv_id = _mongoc_cluster_get_conversation_id (&server_reply);
   if (!*conv_id) {
      AUTH_ERROR_AND_FAIL ("server reply did not contain conversationId");
   }

   bson_destroy (&server_payload);
   if (!_sasl_reply_parse_payload_as_bson (&server_reply, &server_payload, error)) {
      goto fail;
   }

   if (!bson_iter_init_find (&iter, &server_payload, "h") || !BSON_ITER_HOLDS_UTF8 (&iter)) {
      AUTH_ERROR_AND_FAIL ("server payload did not contain string STS FQDN");
   }
   *sts_fqdn = bson_strdup (bson_iter_utf8 (&iter, &sts_fqdn_len));

   if (!_mongoc_validate_and_derive_region (*sts_fqdn, sts_fqdn_len, region, error)) {
      goto fail;
   }

   if (!bson_iter_init_find (&iter, &server_payload, "s") || !BSON_ITER_HOLDS_BINARY (&iter)) {
      AUTH_ERROR_AND_FAIL ("server payload did not contain nonce");
   }

   bson_iter_binary (&iter, &reply_nonce_subtype, &reply_nonce_len, &reply_nonce_data);
   if (reply_nonce_len != 64) {
      AUTH_ERROR_AND_FAIL ("server reply nonce was not 64 bytes");
   }

   if (0 != memcmp (reply_nonce_data, client_nonce, 32)) {
      AUTH_ERROR_AND_FAIL ("server reply nonce prefix did not match client nonce");
   }

   /* Drivers MUST error on any additional fields */
   bson_iter_init (&iter, &server_payload);
   while (bson_iter_next (&iter)) {
      const char *field;

      field = bson_iter_key (&iter);
      if (0 == strcmp (field, "h")) {
         continue;
      }
      if (0 == strcmp (field, "s")) {
         continue;
      }
      AUTH_ERROR_AND_FAIL ("unexpected field from server's reply: %s", field);
   }

   memcpy (server_nonce, reply_nonce_data, 64);

   ret = true;
fail:
   bson_destroy (&client_payload);
   bson_destroy (&client_command);
   bson_destroy (&server_reply);
   bson_destroy (&server_payload);
   return ret;
}

#define KMS_REQUEST_ADD_HEADER(key, value)                       \
   do {                                                          \
      if (!kms_request_add_header_field (request, key, value)) { \
         AUTH_ERROR_AND_FAIL ("Failed to add header '%s'", key); \
      }                                                          \
   } while (0)

#define KMS_REQUEST_SET(fn, name, value)                 \
   do {                                                  \
      if (!fn (request, value)) {                        \
         AUTH_ERROR_AND_FAIL ("Failed to set %s", name); \
      }                                                  \
   } while (0)

/* --------------------------------------------------------------------------
 * Step 2
 * --------------------------------------------------------------------------
 * Client sends BSON payload:
 * {
 *   "a": <signed headers>,
 *   "d": <current date in UTC>
 *   "t": <optional security token>
 * }
 *
 * Server responds with final result.
 *
 * On success, returns true.
 * On failure, returns false and sets error.
 * --------------------------------------------------------------------------
 */
static bool
_client_second (mongoc_cluster_t *cluster,
                mongoc_stream_t *stream,
                mongoc_server_description_t *sd,
                _mongoc_aws_credentials_t *creds,
                const uint8_t *server_nonce,
                const char *sts_fqdn,
                const char *region,
                int conv_id,
                bson_error_t *error)
{
   bool ret = false;
   kms_request_t *request = NULL;
   char *signature = NULL;
   const char *date = NULL;
   const size_t server_nonce_str_len = mcommon_b64_ntop_calculate_target_size (64);
   char *server_nonce_str = NULL;
   const char *body = "Action=GetCallerIdentity&Version=2011-06-15";
   bson_t client_payload = BSON_INITIALIZER;
   bson_t client_command = BSON_INITIALIZER;
   bson_t server_reply = BSON_INITIALIZER;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);
   BSON_ASSERT (sd);
   BSON_ASSERT (creds);
   BSON_ASSERT (server_nonce);
   BSON_ASSERT (sts_fqdn);
   BSON_ASSERT (conv_id);
   BSON_ASSERT (creds->access_key_id);
   BSON_ASSERT (creds->secret_access_key);

   server_nonce_str = bson_malloc (server_nonce_str_len);

   request = kms_request_new ("POST", "/", NULL);
   if (kms_request_get_error (request)) {
      AUTH_ERROR_AND_FAIL ("Failed to create new KMS request: %s", kms_request_get_error (request));
   }

   if (mcommon_b64_ntop (server_nonce, 64, server_nonce_str, server_nonce_str_len) == -1) {
      AUTH_ERROR_AND_FAIL ("Failed to parse server nonce");
   }

   if (!kms_request_append_payload (request, body, strlen (body))) {
      AUTH_ERROR_AND_FAIL ("Failed to append payload");
   }

   KMS_REQUEST_SET (kms_request_set_access_key_id, "access key ID", creds->access_key_id);
   KMS_REQUEST_SET (kms_request_set_secret_key, "secret key", creds->secret_access_key);
   KMS_REQUEST_SET (kms_request_set_date, "date", NULL /* use current time */);
   KMS_REQUEST_SET (kms_request_set_region, "region", region);
   KMS_REQUEST_SET (kms_request_set_service, "service", "sts");

   KMS_REQUEST_ADD_HEADER ("Content-Type", "application/x-www-form-urlencoded");
   KMS_REQUEST_ADD_HEADER ("Host", sts_fqdn);
   KMS_REQUEST_ADD_HEADER ("X-MongoDB-Server-Nonce", server_nonce_str);
   KMS_REQUEST_ADD_HEADER ("X-MongoDB-GS2-CB-Flag", "n");
   if (creds->session_token) {
      KMS_REQUEST_ADD_HEADER ("X-Amz-Security-Token", creds->session_token);
   }

   signature = kms_request_get_signature (request);
   if (kms_request_get_error (request)) {
      AUTH_ERROR_AND_FAIL ("Failed to get signature: %s", kms_request_get_error (request));
   }

   date = kms_request_get_canonical_header (request, "X-Amz-Date");
   if (kms_request_get_error (request)) {
      AUTH_ERROR_AND_FAIL ("Failed to get canonical header: %s", kms_request_get_error (request));
   }

   BCON_APPEND (&client_payload, "a", BCON_UTF8 (signature), "d", BCON_UTF8 (date));
   if (creds->session_token) {
      BCON_APPEND (&client_payload, "t", BCON_UTF8 (creds->session_token));
   }

   BCON_APPEND (&client_command,
                "saslContinue",
                BCON_INT32 (1),
                "conversationId",
                BCON_INT32 (conv_id),
                "payload",
                BCON_BIN (BSON_SUBTYPE_BINARY, bson_get_data (&client_payload), client_payload.len));

   bson_destroy (&server_reply);
   if (!_run_command (cluster, stream, sd, &client_command, &server_reply, error)) {
      goto fail;
   }

   ret = true;
fail:
   bson_destroy (&client_payload);
   bson_destroy (&client_command);
   bson_destroy (&server_reply);
   kms_request_destroy (request);
   bson_free (signature);
   bson_free (server_nonce_str);
   return ret;
}

bool
_mongoc_cluster_auth_node_aws (mongoc_cluster_t *cluster,
                               mongoc_stream_t *stream,
                               mongoc_server_description_t *sd,
                               bson_error_t *error)
{
   bool ret = false;
   uint8_t server_nonce[64];
   char *sts_fqdn = NULL;
   char *region = NULL;
   int conv_id = 0;
   _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;

   if (!_mongoc_aws_credentials_obtain (cluster->client->uri, &creds, error)) {
      goto fail;
   }

   if (!_client_first (cluster, stream, sd, server_nonce, &sts_fqdn, &region, &conv_id, error)) {
      goto fail;
   }

   if (!_client_second (cluster, stream, sd, &creds, server_nonce, sts_fqdn, region, conv_id, error)) {
      goto fail;
   }

   ret = true;
fail:
   if (!ret) {
      _mongoc_aws_credentials_cache_clear ();
   }
   _mongoc_aws_credentials_cleanup (&creds);
   bson_free (sts_fqdn);
   bson_free (region);
   return ret;
}

#else

bool
_mongoc_cluster_auth_node_aws (mongoc_cluster_t *cluster,
                               mongoc_stream_t *stream,
                               mongoc_server_description_t *sd,
                               bson_error_t *error)
{
   AUTH_ERROR_AND_FAIL ("AWS auth not supported, configure libmongoc with "
                        "ENABLE_MONGODB_AWS_AUTH=ON");
fail:
   return false;
}


bool
_mongoc_aws_credentials_obtain (mongoc_uri_t *uri, _mongoc_aws_credentials_t *creds, bson_error_t *error)
{
   AUTH_ERROR_AND_FAIL ("AWS auth not supported, configure libmongoc with "
                        "ENABLE_MONGODB_AWS_AUTH=ON");
fail:
   return false;
}

bool
_mongoc_validate_and_derive_region (char *sts_fqdn, size_t sts_fqdn_len, char **region, bson_error_t *error)
{
   AUTH_ERROR_AND_FAIL ("AWS auth not supported, configure libmongoc with "
                        "ENABLE_MONGODB_AWS_AUTH=ON");
fail:
   return false;
}

#endif /* MONGOC_ENABLE_MONGODB_AWS_AUTH */

void
_mongoc_aws_credentials_cleanup (_mongoc_aws_credentials_t *creds)
{
   bson_free (creds->access_key_id);
   bson_free (creds->secret_access_key);
   bson_free (creds->session_token);
}

void
_mongoc_aws_credentials_copy_to (const _mongoc_aws_credentials_t *src, _mongoc_aws_credentials_t *dst)
{
   BSON_ASSERT_PARAM (src);
   BSON_ASSERT_PARAM (dst);

   dst->access_key_id = bson_strdup (src->access_key_id);
   dst->secret_access_key = bson_strdup (src->secret_access_key);
   dst->session_token = bson_strdup (src->session_token);
   dst->expiration = src->expiration;
}

_mongoc_aws_credentials_cache_t mongoc_aws_credentials_cache;

void
_mongoc_aws_credentials_cache_init (void)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   bson_mutex_init (&cache->mutex);
}

static bool
check_expired (const _mongoc_aws_credentials_t *creds)
{
   if (!creds->expiration.set) {
      return true;
   }
   return mcd_get_milliseconds (mcd_timer_remaining (creds->expiration.value)) == 0;
}

void
_mongoc_aws_credentials_cache_lock (void)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   bson_mutex_lock (&cache->mutex);
}

void
_mongoc_aws_credentials_cache_unlock (void)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   bson_mutex_unlock (&cache->mutex);
}

void
_mongoc_aws_credentials_cache_put_nolock (const _mongoc_aws_credentials_t *creds)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   BSON_ASSERT_PARAM (creds);

   if (check_expired (creds)) {
      // Do not add expired credentials.
      return;
   }
   _mongoc_aws_credentials_cache_clear_nolock ();
   _mongoc_aws_credentials_copy_to (creds, &cache->cached.value);
   cache->cached.set = true;
}

void
_mongoc_aws_credentials_cache_put (const _mongoc_aws_credentials_t *creds)
{
   _mongoc_aws_credentials_cache_lock ();
   _mongoc_aws_credentials_cache_put_nolock (creds);
   _mongoc_aws_credentials_cache_unlock ();
}

bool
_mongoc_aws_credentials_cache_get_nolock (_mongoc_aws_credentials_t *creds)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   BSON_ASSERT_PARAM (creds);
   bool found_valid = false;
   bool expired = false;
   if (cache->cached.set) {
      expired = check_expired (&cache->cached.value);
      if (!expired) {
         found_valid = true;
         _mongoc_aws_credentials_copy_to (&cache->cached.value, creds);
      }
   }
   if (expired) {
      _mongoc_aws_credentials_cache_clear_nolock ();
      return false;
   }
   return found_valid;
}

bool
_mongoc_aws_credentials_cache_get (_mongoc_aws_credentials_t *creds)
{
   _mongoc_aws_credentials_cache_lock ();
   bool got = _mongoc_aws_credentials_cache_get_nolock (creds);
   _mongoc_aws_credentials_cache_unlock ();
   return got;
}

void
_mongoc_aws_credentials_cache_clear_nolock (void)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;

   if (cache->cached.set) {
      _mongoc_aws_credentials_cleanup (&cache->cached.value);
   }
   cache->cached.set = false;
}

void
_mongoc_aws_credentials_cache_clear (void)
{
   _mongoc_aws_credentials_cache_lock ();
   _mongoc_aws_credentials_cache_clear_nolock ();
   _mongoc_aws_credentials_cache_unlock ();
}

void
_mongoc_aws_credentials_cache_cleanup (void)
{
   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   if (cache->cached.set) {
      _mongoc_aws_credentials_cleanup (&cache->cached.value);
   }
   bson_mutex_destroy (&cache->mutex);
}
