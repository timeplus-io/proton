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

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "mongoc/mongoc-cluster-aws-private.h"

void
test_obtain_credentials (void *unused)
{
   mongoc_uri_t *uri;
   _mongoc_aws_credentials_t creds;
   bool ret;
   bson_error_t error;

   BSON_UNUSED (unused);

   /* A username specified with a password is parsed correctly. */
   uri = mongoc_uri_new ("mongodb://"
                         "access_key_id:secret_access_key@localhost/?"
                         "authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   ASSERT_OR_PRINT (ret, error);
   ASSERT_CMPSTR (creds.access_key_id, "access_key_id");
   ASSERT_CMPSTR (creds.secret_access_key, "secret_access_key");
   BSON_ASSERT (creds.session_token == NULL);
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* A username specified with no password is an error. */
   uri = mongoc_uri_new ("mongodb://access_key_id:@localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "ACCESS_KEY_ID is set, but SECRET_ACCESS_KEY is missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* Password not set at all (not empty string) */
   uri = mongoc_uri_new ("mongodb://access_key_id@localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "ACCESS_KEY_ID is set, but SECRET_ACCESS_KEY is missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* A session token may be set through the AWS_SESSION_TOKEN auth mechanism
    * property */
   uri = mongoc_uri_new ("mongodb://"
                         "access_key_id:secret_access_key@localhost/?"
                         "authMechanism=MONGODB-AWS&authMechanismProperties="
                         "AWS_SESSION_TOKEN:token");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   ASSERT_OR_PRINT (ret, error);
   ASSERT_CMPSTR (creds.access_key_id, "access_key_id");
   ASSERT_CMPSTR (creds.secret_access_key, "secret_access_key");
   ASSERT_CMPSTR (creds.session_token, "token");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* A session token in the URI with no username/password is an error. */
   uri = mongoc_uri_new ("mongodb://localhost/"
                         "?authMechanism=MONGODB-AWS&authMechanismProperties="
                         "AWS_SESSION_TOKEN:token");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "AWS_SESSION_TOKEN is set, but ACCESS_KEY_ID and "
                          "SECRET_ACCESS_KEY are missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);
}

void
test_obtain_credentials_from_env (void *unused)
{
   mongoc_uri_t *uri;
   _mongoc_aws_credentials_t creds;
   bool ret;
   bson_error_t error;

   BSON_UNUSED (unused);

   /* "clear" environment variables by setting them to the empty string. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "");
   _mongoc_setenv ("AWS_SESSION_TOKEN", "");

   /* Environment variables are used if username/password is not set. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "access_key_id");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "secret_access_key");
   uri = mongoc_uri_new ("mongodb://localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   ASSERT_OR_PRINT (ret, error);
   ASSERT_CMPSTR (creds.access_key_id, "access_key_id");
   ASSERT_CMPSTR (creds.secret_access_key, "secret_access_key");
   BSON_ASSERT (creds.session_token == NULL);
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* Omitting one of the required environment variables is an error. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "access_key_id");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "");
   uri = mongoc_uri_new ("mongodb://localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "ACCESS_KEY_ID is set, but SECRET_ACCESS_KEY is missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* Omitting one of the required environment variables is an error. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "secret_access_key");
   uri = mongoc_uri_new ("mongodb://localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "SECRET_ACCESS_KEY is set, but ACCESS_KEY_ID is missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* Only specifying the token is an error. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "");
   _mongoc_setenv ("AWS_SESSION_TOKEN", "token");
   uri = mongoc_uri_new ("mongodb://localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_AUTHENTICATE,
                          "AWS_SESSION_TOKEN is set, but ACCESS_KEY_ID and "
                          "SECRET_ACCESS_KEY are missing");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* But a session token in the environment is picked up. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "access_key_id");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "secret_access_key");
   _mongoc_setenv ("AWS_SESSION_TOKEN", "token");
   uri = mongoc_uri_new ("mongodb://localhost/?authMechanism=MONGODB-AWS");
   ret = _mongoc_aws_credentials_obtain (uri, &creds, &error);
   ASSERT_OR_PRINT (ret, error);
   ASSERT_CMPSTR (creds.access_key_id, "access_key_id");
   ASSERT_CMPSTR (creds.secret_access_key, "secret_access_key");
   ASSERT_CMPSTR (creds.session_token, "token");
   _mongoc_aws_credentials_cleanup (&creds);
   mongoc_uri_destroy (uri);

   /* "clear" environment variables by setting them to the empty string. */
   _mongoc_setenv ("AWS_ACCESS_KEY_ID", "");
   _mongoc_setenv ("AWS_SECRET_ACCESS_KEY", "");
   _mongoc_setenv ("AWS_SESSION_TOKEN", "");
}

static void
test_derive_region (void *unused)
{
   bson_error_t error;
   char *region;
   bool ret;
   char *large;

   BSON_UNUSED (unused);

#define WITH_LEN(s) s, strlen (s)

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("abc..def"), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: empty part");
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("."), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: empty part");
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("..."), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: empty part");
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("first."), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: empty part");
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("sts.amazonaws.com"), &region, &error);
   BSON_ASSERT (ret);
   ASSERT_CMPSTR ("us-east-1", region);
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("first.second"), &region, &error);
   BSON_ASSERT (ret);
   ASSERT_CMPSTR ("second", region);
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN ("first"), &region, &error);
   BSON_ASSERT (ret);
   ASSERT_CMPSTR ("us-east-1", region);
   bson_free (region);

   ret = _mongoc_validate_and_derive_region (WITH_LEN (""), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: empty");
   bson_free (region);

   large = bson_malloc0 (257);
   memset (large, 'a', 256);

   ret = _mongoc_validate_and_derive_region (large, strlen (large), &region, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Invalid STS host: too large");
   bson_free (region);
   bson_free (large);

#undef WITH_LEN
}

// test_aws_cache unit tests the _mongoc_aws_credentials_cache_t. It does not
// require libmongoc to be built with MONGOC_ENABLE_MONGODB_AWS_AUTH.
static void
test_aws_cache (void *unused)
{
   BSON_UNUSED (unused);
   _mongoc_aws_credentials_t valid_creds = MONGOC_AWS_CREDENTIALS_INIT;
   valid_creds.access_key_id = bson_strdup ("access_key_id");
   valid_creds.secret_access_key = bson_strdup ("secret_access_key");
   valid_creds.session_token = bson_strdup ("session_token");
   // Set expiration to one minute from now.
   valid_creds.expiration.set = true;
   valid_creds.expiration.value = mcd_timer_expire_after (mcd_milliseconds (60 * 1000));

   _mongoc_aws_credentials_t expired_creds = MONGOC_AWS_CREDENTIALS_INIT;
   expired_creds.access_key_id = bson_strdup ("access_key_id");
   expired_creds.secret_access_key = bson_strdup ("secret_access_key");
   expired_creds.session_token = bson_strdup ("session_token");
   // Set expiration to one minute before.
   expired_creds.expiration.set = true;
   expired_creds.expiration.value = mcd_timer_expire_after (mcd_milliseconds (-60 * 1000));

   _mongoc_aws_credentials_cache_t *cache = &mongoc_aws_credentials_cache;
   _mongoc_aws_credentials_cache_clear ();

   // Expect `get` to return nothing initially.
   {
      _mongoc_aws_credentials_t got = MONGOC_AWS_CREDENTIALS_INIT;
      bool found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (!found);
   }

   // Expect `get` to return after valid credentials are added with `put`.
   {
      _mongoc_aws_credentials_t got = MONGOC_AWS_CREDENTIALS_INIT;
      _mongoc_aws_credentials_cache_put (&valid_creds);
      bool found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (found);
      ASSERT_CMPSTR (got.access_key_id, valid_creds.access_key_id);
      ASSERT_CMPSTR (got.secret_access_key, valid_creds.secret_access_key);
      ASSERT_CMPSTR (got.session_token, valid_creds.session_token);
      _mongoc_aws_credentials_cleanup (&got);
   }

   // Expect `clear` to clear cached credentials.
   {
      _mongoc_aws_credentials_t got = MONGOC_AWS_CREDENTIALS_INIT;
      _mongoc_aws_credentials_cache_put (&valid_creds);
      _mongoc_aws_credentials_cache_clear ();
      bool found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (!found);
   }

   // Expect expired credentials are not added to cache.
   {
      _mongoc_aws_credentials_t got = MONGOC_AWS_CREDENTIALS_INIT;
      _mongoc_aws_credentials_cache_put (&expired_creds);
      bool found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (!found);
   }

   // Expect credentials that expire are not returned from cache.
   {
      _mongoc_aws_credentials_t got = MONGOC_AWS_CREDENTIALS_INIT;
      _mongoc_aws_credentials_cache_put (&valid_creds);
      bool found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (found);

      // Manually expire the credentials.
      cache->cached.value.expiration.value = expired_creds.expiration.value;
      found = _mongoc_aws_credentials_cache_get (&got);
      ASSERT (!found);
      _mongoc_aws_credentials_cleanup (&got);
   }

   _mongoc_aws_credentials_cache_clear ();
   _mongoc_aws_credentials_cleanup (&expired_creds);
   _mongoc_aws_credentials_cleanup (&valid_creds);
}

void
test_aws_install (TestSuite *suite)
{
   TestSuite_AddFull (suite,
                      "/aws/obtain_credentials",
                      test_obtain_credentials,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_aws);
   TestSuite_AddFull (suite,
                      "/aws/obtain_credentials_from_env",
                      test_obtain_credentials_from_env,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_aws,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (
      suite, "/aws/derive_region", test_derive_region, NULL /* dtor */, NULL /* ctx */, test_framework_skip_if_no_aws);
   TestSuite_AddFull (
      suite, "/aws/cache", test_aws_cache, NULL /* dtor */, NULL /* ctx */, test_framework_skip_if_no_aws);
}
