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

// test-awsauth.c tests authentication with the MONGODB-AWS authMechanism.
// It may be run in an AWS ECS task or EC2 instance.

#include "common-thread-private.h"
#include <mongoc/mongoc.h>
#include "mongoc-cluster-aws-private.h"
#include "mongoc-client-private.h"
#include "mongoc-util-private.h" // _mongoc_getenv

// Ensure stdout and stderr are flushed prior to possible following abort().
#define MONGOC_STDERR_PRINTF(format, ...)    \
   if (1) {                                  \
      fflush (stdout);                       \
      fprintf (stderr, format, __VA_ARGS__); \
      fflush (stderr);                       \
   } else                                    \
      ((void) 0)

#define ASSERT(Cond)                                                                                         \
   if (1) {                                                                                                  \
      if (!(Cond)) {                                                                                         \
         MONGOC_STDERR_PRINTF (                                                                              \
            "FAIL:%s:%d  %s()\n  Condition '%s' failed.\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (Cond)); \
         abort ();                                                                                           \
      }                                                                                                      \
   } else                                                                                                    \
      ((void) 0)

#define ASSERTF(Cond, Fmt, ...)                                                                              \
   if (1) {                                                                                                  \
      if (!(Cond)) {                                                                                         \
         MONGOC_STDERR_PRINTF (                                                                              \
            "FAIL:%s:%d  %s()\n  Condition '%s' failed.\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (Cond)); \
         MONGOC_STDERR_PRINTF ("MESSAGE: " Fmt "\n", __VA_ARGS__);                                           \
         abort ();                                                                                           \
      }                                                                                                      \
   } else                                                                                                    \
      ((void) 0)

#define FAILF(Fmt, ...)                                                                                   \
   if (1) {                                                                                               \
      MONGOC_STDERR_PRINTF (                                                                              \
         "FAIL:%s:%d  %s()\n  Condition '%s' failed.\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (Cond)); \
      MONGOC_STDERR_PRINTF ("MESSAGE: " Fmt "\n", __VA_ARGS__);                                           \
      abort ();                                                                                           \
   } else                                                                                                 \
      ((void) 0)

static void
test_auth (mongoc_database_t *db, bool expect_failure)
{
   bson_error_t error;
   bson_t *ping = BCON_NEW ("ping", BCON_INT32 (1));
   bool ok =
      mongoc_database_command_with_opts (db, ping, NULL /* read_prefs */, NULL /* opts */, NULL /* reply */, &error);
   if (expect_failure) {
      ASSERTF (!ok, "%s", "Expected auth failure, but got success");
   } else {
      ASSERTF (ok, "Expected auth success, but got error: %s", error.message);
   }
   bson_destroy (ping);
}

// creds_eq returns true if `a` and `b` contain the same credentials.
static bool
creds_eq (_mongoc_aws_credentials_t *a, _mongoc_aws_credentials_t *b)
{
   BSON_ASSERT_PARAM (a);
   BSON_ASSERT_PARAM (b);

   if (0 != strcmp (a->access_key_id, b->access_key_id)) {
      return false;
   }
   if (0 != strcmp (a->secret_access_key, b->secret_access_key)) {
      return false;
   }
   if (0 != strcmp (a->session_token, b->session_token)) {
      return false;
   }
   if (a->expiration.set != b->expiration.set) {
      return false;
   }
   if (a->expiration.set) {
      if (mcd_time_compare (a->expiration.value.expire_at, b->expiration.value.expire_at) != 0) {
         return false;
      }
   }
   return true;
}

// clear_env sets all AWS environment variables to empty strings.
static void
clear_env (void)
{
   ASSERT (_mongoc_setenv ("AWS_ACCESS_KEY_ID", ""));
   ASSERT (_mongoc_setenv ("AWS_SECRET_ACCESS_KEY", ""));
   ASSERT (_mongoc_setenv ("AWS_SESSION_TOKEN", ""));
}

// caching_expected returns true if MONGODB-AWS authentication is expected to
// cache credentials. Caching is expected when credentials are not passed
// through the URI or environment variables.
static bool
caching_expected (const mongoc_uri_t *uri)
{
   if (mongoc_uri_get_username (uri)) {
      // AWS credentials in the URI like:
      // "mongodb://<access_key>:<secret_key>@mongodb.example.com/?authMechanism=MONGODB-AWS"
      // are not cached.
      return false;
   }
   char *got = _mongoc_getenv ("AWS_ACCESS_KEY_ID");
   if (NULL != got) {
      bson_free (got);
      // AWS credentials passed in environment are not cached.
      return false;
   }
   return true;
}

// do_find runs a find command. Returns false and sets `error` on error.
static bool
do_find (mongoc_client_t *client, bson_error_t *error)
{
   ASSERT (client);

   bson_t *filter = bson_new ();
   mongoc_collection_t *coll = mongoc_client_get_collection (client, "aws", "coll");
   mongoc_cursor_t *cursor = mongoc_collection_find_with_opts (coll, filter, NULL /* opts */, NULL /* read prefs */);
   const bson_t *doc;
   while (mongoc_cursor_next (cursor, &doc))
      ;
   bool ok = !mongoc_cursor_error (cursor, error);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);
   bson_destroy (filter);
   return ok;
}

static void
test_cache (const mongoc_uri_t *uri)
{
   bson_error_t error;

   if (!caching_expected (uri)) {
      printf ("Caching credentials is not expected. Skipping tests expecting "
              "credential caching.\n");
      return;
   }

   // Clear the cache.
   _mongoc_aws_credentials_cache_clear ();

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);

      // Ensure that a ``find`` operation adds credentials to the cache.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);
      mongoc_client_destroy (client);
   }
   // Override the cached credentials with an "Expiration" that is within one
   // minute of the current UTC time.
   _mongoc_aws_credentials_t first_cached = MONGOC_AWS_CREDENTIALS_INIT;
   {
      ASSERT (mongoc_aws_credentials_cache.cached.set);
      mongoc_aws_credentials_cache.cached.value.expiration.set = true;
      mongoc_aws_credentials_cache.cached.value.expiration.value =
         mcd_timer_expire_after (mcd_milliseconds (60 * 1000 - MONGOC_AWS_CREDENTIALS_EXPIRATION_WINDOW_MS));
      _mongoc_aws_credentials_copy_to (&mongoc_aws_credentials_cache.cached.value, &first_cached);
   }

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);
      // Ensure that a ``find`` operation updates the credentials in the cache.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      ASSERTF (!creds_eq (&first_cached, &mongoc_aws_credentials_cache.cached.value),
               "%s",
               "expected unequal credentials, got equal");
      _mongoc_aws_credentials_cleanup (&creds);
      mongoc_client_destroy (client);
   }
   _mongoc_aws_credentials_cleanup (&first_cached);

   // Poison the cache with an invalid access key id.
   {
      ASSERT (mongoc_aws_credentials_cache.cached.set);
      bson_free (mongoc_aws_credentials_cache.cached.value.access_key_id);
      mongoc_aws_credentials_cache.cached.value.access_key_id = bson_strdup ("invalid");
   }

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);

      // Ensure that a ``find`` operation results in an error.
      ASSERT (!do_find (client, &error));
      ASSERTF (NULL != strstr (error.message, "Authentication failed"),
               "Expected error to contain '%s', but got '%s'",
               "Authentication failed",
               error.message);

      // Ensure that the cache has been cleared.
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (!found);
      _mongoc_aws_credentials_cleanup (&creds);

      // Ensure that a subsequent ``find`` operation succeeds.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);

      // Ensure that the cache has been set.
      found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);

      mongoc_client_destroy (client);
   }
}

static void
test_cache_with_env (const mongoc_uri_t *uri)
{
   bson_error_t error;

   if (!caching_expected (uri)) {
      printf ("Caching credentials is not expected. Skipping tests expecting "
              "credential caching.\n");
      return;
   }

   // Clear the cache.
   _mongoc_aws_credentials_cache_clear ();

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);

      // Ensure that a ``find`` operation adds credentials to the cache.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);

      // Set the AWS environment variables based on the cached credentials.
      ASSERT (_mongoc_setenv ("AWS_ACCESS_KEY_ID", mongoc_aws_credentials_cache.cached.value.access_key_id));
      ASSERT (_mongoc_setenv ("AWS_SECRET_ACCESS_KEY", mongoc_aws_credentials_cache.cached.value.secret_access_key));
      ASSERT (_mongoc_setenv ("AWS_SESSION_TOKEN", mongoc_aws_credentials_cache.cached.value.session_token));

      // Clear the cache.
      _mongoc_aws_credentials_cache_clear ();
      mongoc_client_destroy (client);
   }

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);
      // Ensure that a ``find`` operation succeeds and does not add credentials
      // to the cache.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (!found);
      _mongoc_aws_credentials_cleanup (&creds);
      mongoc_client_destroy (client);
   }

   // Set the AWS environment variables to invalid values.
   ASSERT (_mongoc_setenv ("AWS_ACCESS_KEY_ID", "invalid"));

   // Create a new client.
   {
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);
      // Ensure that a ``find`` operation results in an error.
      ASSERT (!do_find (client, &error));
      ASSERTF (NULL != strstr (error.message, "Authentication failed"),
               "Expected error to contain '%s', but got '%s'",
               "Authentication failed",
               error.message);
      mongoc_client_destroy (client);
   }

   // Clear the AWS environment variables.
   clear_env ();

   // Clear the cache.
   _mongoc_aws_credentials_cache_clear ();

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);

      // Ensure that a ``find`` operation adds credentials to the cache.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);
      mongoc_client_destroy (client);
   }

   // Set the AWS environment variables to invalid values.
   ASSERT (_mongoc_setenv ("AWS_ACCESS_KEY_ID", "invalid"));

   // Create a new client.
   {
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      mongoc_client_t *client = mongoc_client_new_from_uri (uri);
      ASSERT (client);

      // Ensure that a ``find`` operation succeeds.
      ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);
      mongoc_client_destroy (client);
   }

   // Clear the AWS environment variables.
   clear_env ();
}

BSON_THREAD_FUN (auth_fn, uri_void)
{
   bson_error_t error;
   const mongoc_uri_t *uri = uri_void;

   mongoc_client_t *client = mongoc_client_new_from_uri (uri);
   ASSERT (client);

   // Ensure that a ``find`` operation succeeds.
   ASSERTF (do_find (client, &error), "expected success, got: %s", error.message);
   mongoc_client_destroy (client);

   BSON_THREAD_RETURN;
}

static void
test_multithreaded (const mongoc_uri_t *uri)
{
   // Test authenticating in many threads concurrently.
   bson_thread_t threads[64];
   for (size_t i = 0; i < sizeof threads / sizeof threads[0]; i++) {
      ASSERT (0 == mcommon_thread_create (&threads[i], auth_fn, (void *) uri));
   }

   for (size_t i = 0; i < sizeof threads / sizeof threads[0]; i++) {
      ASSERT (0 == mcommon_thread_join (threads[i]));
   }

   // Verify that credentials are cached.
   if (caching_expected (uri)) {
      // Assert credentials are cached.
      _mongoc_aws_credentials_t creds = MONGOC_AWS_CREDENTIALS_INIT;
      bool found = _mongoc_aws_credentials_cache_get (&creds);
      ASSERT (found);
      _mongoc_aws_credentials_cleanup (&creds);
   }
}

static void
log_func (mongoc_log_level_t log_level, const char *log_domain, const char *message, void *user_data)
{
   if (log_level != MONGOC_LOG_LEVEL_TRACE) {
      mongoc_log_default_handler (log_level, log_domain, message, user_data);
      return;
   }

   // Only log trace messages from AWS auth.
   if (0 == strcmp (log_domain, "aws_auth")) {
      mongoc_log_default_handler (log_level, log_domain, message, user_data);
   }

   // Do not print other trace logs to reduce verbosity.
}

int
main (int argc, char *argv[])
{
   mongoc_database_t *db;
   mongoc_client_t *client;
   bson_error_t error;
   mongoc_uri_t *uri;
   bool expect_failure;

   if (argc != 3) {
      FAILF ("usage: %s URI [EXPECT_SUCCESS|EXPECT_FAILURE]\n", argv[0]);
   }

   // Set a custom log callback to only print trace messages related to fetching
   // AWS credentials.
   mongoc_log_set_handler (log_func, NULL /* user_data */);

   mongoc_init ();

   uri = mongoc_uri_new_with_error (argv[1], &error);
   ASSERTF (uri, "Failed to create URI: %s", error.message);

   if (0 == strcmp (argv[2], "EXPECT_FAILURE")) {
      expect_failure = true;
   } else if (0 == strcmp (argv[2], "EXPECT_SUCCESS")) {
      expect_failure = false;
   } else {
      FAILF ("Expected 'EXPECT_FAILURE' or 'EXPECT_SUCCESS' for argument. Got: %s", argv[2]);
   }

   client = mongoc_client_new_from_uri (uri);
   ASSERT (client);

   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   db = mongoc_client_get_database (client, "test");
   test_auth (db, expect_failure);
   if (!expect_failure) {
      // The test_cache_* functions implement the "Cached Credentials" tests
      // from the specification.
      test_cache (uri);
      test_cache_with_env (uri);
      test_multithreaded (uri);
   }

   mongoc_database_destroy (db);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);

   printf ("%s tests passed\n", argv[0]);

   mongoc_cleanup ();
   return EXIT_SUCCESS;
}
