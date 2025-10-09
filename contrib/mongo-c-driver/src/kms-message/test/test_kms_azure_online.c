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

#include "kms_message/kms_azure_request.h"
#include "kms_message/kms_b64.h"
#include "kms_message/kms_request.h"
#include "kms_message/kms_response.h"
#include "kms_message/kms_response_parser.h"

#define MONGOC_LOG_DOMAIN "test_kms_azure_online"
#include <mongoc/mongoc.h>

#include "test_kms_assert.h"

#include <stdio.h>

#include "test_kms_online_util.h"

#define SCOPE "https%3A%2F%2Fvault.azure.net%2F.default"

/* Define TEST_TRACING_INSECURE in compiler flags to enable
 * log output with sensitive information (for debugging). */
#ifdef TEST_TRACING_INSECURE
#define TEST_TRACE(...) MONGOC_DEBUG (__VA_ARGS__)
#else
#define TEST_TRACE(...) (void) 0
#endif

typedef struct {
   char *tenant_id;
   char *client_id;
   char *client_secret;
   char *key_url;
   char *key_vault_url;
   char *key_path;
   char *key_host;
   char *key_name;
   char *key_version;
} test_env_t;

static char *
test_getenv (const char *key)
{
   char *value = getenv (key);
   if (!value) {
      fprintf (
         stderr, "Environment variable: %s not set (@@ctest-skip@@)", key);
      exit (2);
   }
   TEST_TRACE ("Env: %s = %s", key, value);
   return value;
}

static void
test_env_init (test_env_t *test_env)
{
   char *azure_domain = "vault.azure.net";
   char *loc;

   test_env->tenant_id = test_getenv ("AZURE_TENANT_ID");
   test_env->client_id = test_getenv ("AZURE_CLIENT_ID");
   test_env->client_secret = test_getenv ("AZURE_CLIENT_SECRET");
   test_env->key_url = test_getenv ("AZURE_KEY_URL");
   test_env->key_name = test_getenv ("AZURE_KEY_NAME");
   test_env->key_version = test_getenv ("AZURE_KEY_VERSION");

   loc = strstr (test_env->key_url, azure_domain);
   ASSERT (loc);
   test_env->key_vault_url = bson_strndup (
      test_env->key_url, strlen (azure_domain) + loc - test_env->key_url);
   test_env->key_path = bson_strdup (loc + strlen (azure_domain));
   loc = strstr (test_env->key_vault_url, "//");
   test_env->key_host = bson_strdup (loc + 2);
}

static void
test_env_cleanup (test_env_t *test_env)
{
   bson_free (test_env->key_vault_url);
   bson_free (test_env->key_path);
   bson_free (test_env->key_host);
}

/*
Authenticate to Azure by sending an oauth request with client_id and
client_secret (set in environment variables).
Returns the base64url encoded bearer token that must be freed with bson_free.

Subsequent requests to Azure can use the returned token by setting the header
Authorization: Bearer <token>.

References:
[1]
https://docs.microsoft.com/en-us/azure/key-vault/general/authentication-requests-and-responses
*/
static char *
azure_authenticate (void)
{
   kms_request_t *req;
   kms_request_opt_t *opt;
   char *req_str;
   const char *res_str;
   bson_t *res_bson;
   bson_iter_t iter;
   char *bearer_token;

   kms_response_t *res;
   test_env_t test_env;
   test_env_init (&test_env);

   opt = kms_request_opt_new ();
   kms_request_opt_set_connection_close (opt, true);
   kms_request_opt_set_provider (opt, KMS_REQUEST_PROVIDER_AZURE);

   req = kms_azure_request_oauth_new ("login.microsoftonline.com",
                                      SCOPE,
                                      test_env.tenant_id,
                                      test_env.client_id,
                                      test_env.client_secret,
                                      opt);
   req_str = kms_request_to_string (req);
   TEST_TRACE ("--> HTTP request:\n%s\n", req_str);

   res = send_kms_request (req, "login.microsoftonline.com");
   res_str = kms_response_get_body (res, NULL);
   TEST_TRACE ("<-- HTTP response:\n%s\n", res_str);
   ASSERT (kms_response_get_status (res) == 200);

   res_bson =
      bson_new_from_json ((const uint8_t *) res_str, strlen (res_str), NULL);
   ASSERT (res_bson);
   if (!bson_iter_init_find (&iter, res_bson, "access_token")) {
      TEST_ERROR ("could not find 'access_token' in HTTP response");
   }

   bearer_token = bson_strdup (bson_iter_utf8 (&iter, NULL));

   kms_request_free_string (req_str);
   kms_response_destroy (res);
   kms_request_destroy (req);
   bson_destroy (res_bson);
   test_env_cleanup (&test_env);
   kms_request_opt_destroy (opt);
   return bearer_token;
}

/* Test wrapping a 96 byte payload (the size of a data key) and unwrapping it
 * back. */
static void
test_azure_wrapkey (void)
{
   test_env_t test_env;
   kms_request_opt_t *opt;
   kms_request_t *req;
   char *req_str;
   char *bearer_token;
   kms_response_t *res;
   const char *res_str;
   uint8_t *encrypted_raw;
   size_t encrypted_raw_len;
   char *decrypted;
   bson_t *res_bson;
   bson_iter_t iter;
   uint8_t *key_data;
   char *key_data_b64url;
   int i;

#define KEYLEN 96

   key_data = bson_malloc0 (KEYLEN);
   for (i = 0; i < KEYLEN; i++) {
      key_data[i] = i;
   }
   key_data_b64url = kms_message_raw_to_b64url (key_data, KEYLEN);

   test_env_init (&test_env);
   bearer_token = azure_authenticate ();

   opt = kms_request_opt_new ();
   kms_request_opt_set_connection_close (opt, true);
   kms_request_opt_set_provider (opt, KMS_REQUEST_PROVIDER_AZURE);
   req = kms_azure_request_wrapkey_new (test_env.key_host,
                                        bearer_token,
                                        test_env.key_name,
                                        test_env.key_version,
                                        key_data,
                                        KEYLEN,
                                        opt);
   req_str = kms_request_to_string (req);
   TEST_TRACE ("--> HTTP request:\n%s\n", req_str);
   res = send_kms_request (req, test_env.key_host);

   res_str = kms_response_get_body (res, NULL);
   TEST_TRACE ("<-- HTTP response:\n%s", res_str);
   res_bson =
      bson_new_from_json ((const uint8_t *) res_str, strlen (res_str), NULL);
   ASSERT (res_bson);
   ASSERT (bson_iter_init_find (&iter, res_bson, "value"));
   encrypted_raw = kms_message_b64url_to_raw (bson_iter_utf8 (&iter, NULL),
                                              &encrypted_raw_len);
   ASSERT (encrypted_raw);

   bson_destroy (res_bson);
   bson_free (req_str);
   kms_request_destroy (req);
   kms_response_destroy (res);

   /* Send a request to unwrap the encrypted key. */
   req = kms_azure_request_unwrapkey_new (test_env.key_host,
                                          bearer_token,
                                          test_env.key_name,
                                          test_env.key_version,
                                          encrypted_raw,
                                          encrypted_raw_len,
                                          opt);
   req_str = kms_request_to_string (req);
   TEST_TRACE ("--> HTTP request:\n%s\n", req_str);
   res = send_kms_request (req, test_env.key_host);
   res_str = kms_response_get_body (res, NULL);
   TEST_TRACE ("<-- HTTP response:\n%s", res_str);
   res_bson =
      bson_new_from_json ((const uint8_t *) res_str, strlen (res_str), NULL);
   ASSERT (res_bson);
   ASSERT (bson_iter_init_find (&iter, res_bson, "value"));
   decrypted = bson_strdup (bson_iter_utf8 (&iter, NULL));
   ASSERT_CMPSTR (decrypted, key_data_b64url);

   bson_destroy (res_bson);
   kms_response_destroy (res);
   bson_free (req_str);
   bson_free (bearer_token);
   test_env_cleanup (&test_env);
   kms_request_destroy (req);
   bson_free (encrypted_raw);
   bson_free (key_data_b64url);
   bson_free (key_data);
   bson_free (decrypted);
   kms_request_opt_destroy (opt);
}

int
main (int argc, char **argv)
{
   kms_message_init ();
   test_azure_wrapkey ();
   return 0;
}