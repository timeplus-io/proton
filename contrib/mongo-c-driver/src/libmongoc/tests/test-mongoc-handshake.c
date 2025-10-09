/*
 * Copyright 2016 MongoDB, Inc.
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

#include <stdint.h>
#include <bson/bson.h>
#include <mongoc/mongoc.h>
#ifdef _POSIX_VERSION
#include <sys/utsname.h>
#endif

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-handshake.h"
#include "mongoc/mongoc-handshake-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"

/*
 * Call this before any test which uses mongoc_handshake_data_append, to
 * reset the global state and unfreeze the handshake struct. Call it
 * after a test so later tests don't have a weird handshake document
 *
 * This is not safe to call while we have any clients or client pools running!
 */
static void
_reset_handshake (void)
{
   _mongoc_handshake_cleanup ();
   _mongoc_handshake_init ();
}

static const char *default_appname = "testapp";
static const char *default_driver_name = "php driver";
static const char *default_driver_version = "version abc";
static const char *default_platform = "./configure -nottoomanyflags";
static const int32_t default_timeout_sec = 60;
static const int32_t default_memory_mb = 1024;

static void
test_mongoc_handshake_appname_in_uri (void)
{
   char long_string[MONGOC_HANDSHAKE_APPNAME_MAX + 2];
   char *uri_str;
   const char *good_uri = "mongodb://host/?" MONGOC_URI_APPNAME "=mongodump";
   mongoc_uri_t *uri;
   const char *appname = "mongodump";
   const char *value;

   memset (long_string, 'a', MONGOC_HANDSHAKE_APPNAME_MAX + 1);
   long_string[MONGOC_HANDSHAKE_APPNAME_MAX + 1] = '\0';

   /* Shouldn't be able to set with appname really long */
   capture_logs (true);
   uri_str = bson_strdup_printf ("mongodb://a/?" MONGOC_URI_APPNAME "=%s", long_string);
   ASSERT (!test_framework_client_new (uri_str, NULL));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_WARNING, "Unsupported value");
   capture_logs (false);

   uri = mongoc_uri_new (good_uri);
   ASSERT (uri);
   value = mongoc_uri_get_appname (uri);
   ASSERT (value);
   ASSERT_CMPSTR (appname, value);
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new (NULL);
   ASSERT (uri);
   ASSERT (!mongoc_uri_set_appname (uri, long_string));
   ASSERT (mongoc_uri_set_appname (uri, appname));
   value = mongoc_uri_get_appname (uri);
   ASSERT (value);
   ASSERT_CMPSTR (appname, value);
   mongoc_uri_destroy (uri);

   bson_free (uri_str);
}

static void
test_mongoc_handshake_appname_frozen_single (void)
{
   mongoc_client_t *client;
   const char *good_uri = "mongodb://host/?" MONGOC_URI_APPNAME "=mongodump";

   client = test_framework_client_new (good_uri, NULL);

   /* Shouldn't be able to set appname again */
   capture_logs (true);
   ASSERT (!mongoc_client_set_appname (client, "a"));
   ASSERT_CAPTURED_LOG (
      "_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "Cannot set appname more than once");
   capture_logs (false);

   mongoc_client_destroy (client);
}

static void
test_mongoc_handshake_appname_frozen_pooled (void)
{
   mongoc_client_pool_t *pool;
   const char *good_uri = "mongodb://host/?" MONGOC_URI_APPNAME "=mongodump";
   mongoc_uri_t *uri;

   uri = mongoc_uri_new (good_uri);

   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   capture_logs (true);
   ASSERT (!mongoc_client_pool_set_appname (pool, "test"));
   ASSERT_CAPTURED_LOG (
      "_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "Cannot set appname more than once");
   capture_logs (false);

   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

static void
_check_arch_string_valid (const char *arch)
{
#ifdef _POSIX_VERSION
   struct utsname system_info;

   ASSERT (uname (&system_info) >= 0);
   ASSERT_CMPSTR (system_info.machine, arch);
#endif
   ASSERT (strlen (arch) > 0);
}

static void
_check_os_version_valid (const char *os_version)
{
#if defined(__linux__) || defined(_WIN32)
   /* On linux we search the filesystem for os version or use uname.
    * On windows we call GetSystemInfo(). */
   ASSERT (os_version);
   ASSERT (strlen (os_version) > 0);
#elif defined(_POSIX_VERSION)
   /* On a non linux posix systems, we just call uname() */
   struct utsname system_info;

   ASSERT (uname (&system_info) >= 0);
   ASSERT (os_version);
   ASSERT_CMPSTR (system_info.release, os_version);
#endif
}

static void
_handshake_check_application (bson_t *doc)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   const char *val;

   ASSERT (bson_iter_init_find (&md_iter, doc, "application"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));
   ASSERT (bson_iter_find (&inner_iter, "name"));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT_CMPSTR (val, default_appname);
}

static void
_handshake_check_driver (bson_t *doc)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   const char *val;
   ASSERT (bson_iter_init_find (&md_iter, doc, "driver"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));
   ASSERT (bson_iter_find (&inner_iter, "name"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strstr (val, default_driver_name) != NULL);
   ASSERT (bson_iter_find (&inner_iter, "version"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strstr (val, default_driver_version));
}

static void
_handshake_check_os (bson_t *doc)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   const char *val;

   /* Check os type not empty */
   ASSERT (bson_iter_init_find (&md_iter, doc, "os"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));

   ASSERT (bson_iter_find (&inner_iter, "type"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strlen (val) > 0);

   /* Check os version valid */
   ASSERT (bson_iter_find (&inner_iter, "version"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   _check_os_version_valid (val);

   /* Check os arch is valid */
   ASSERT (bson_iter_find (&inner_iter, "architecture"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   _check_arch_string_valid (val);

   /* Not checking os_name, as the spec says it can be NULL. */
}

static void
_handshake_check_env (bson_t *doc, int expected_memory_mb, int expected_timeout_sec, const char *expected_region)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   const char *name;
   ASSERT (bson_iter_init_find (&md_iter, doc, "env"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));

   ASSERT (bson_iter_find (&inner_iter, "name"));
   name = bson_iter_utf8 (&inner_iter, NULL);

   bool is_aws = strstr (name, "aws.lambda") == name;
   bool is_azure = strstr (name, "azure.func") == name;
   bool is_gcp = strstr (name, "gcp.func") == name;
   bool is_vercel = strstr (name, "vercel") == name;
   ASSERT (is_aws || is_azure || is_gcp || is_vercel);

   if (expected_timeout_sec) {
      ASSERT (bson_iter_find (&inner_iter, "timeout_sec"));
      ASSERT (BSON_ITER_HOLDS_INT32 (&inner_iter));
      ASSERT_CMPINT32 (bson_iter_int32 (&inner_iter), ==, expected_timeout_sec);
   }

   if (expected_memory_mb) {
      ASSERT (bson_iter_find (&inner_iter, "memory_mb"));
      ASSERT (BSON_ITER_HOLDS_INT32 (&inner_iter));
      ASSERT_CMPINT32 (bson_iter_int32 (&inner_iter), ==, expected_memory_mb);
   }

   if (expected_region) {
      ASSERT (bson_iter_find (&inner_iter, "region"));
      ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
      ASSERT_CMPSTR (bson_iter_utf8 (&inner_iter, NULL), expected_region);
   }
}

static void
_handshake_check_platform (bson_t *doc)
{
   bson_iter_t md_iter;
   const char *val;
   ASSERT (bson_iter_init_find (&md_iter, doc, "platform"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&md_iter));
   val = bson_iter_utf8 (&md_iter, NULL);
   ASSERT (val);
   if (strlen (val) < 250) { /* standard val are < 100, may be truncated on some platform */
      ASSERT (strstr (val, default_platform) != NULL);
   }
}

static void
_handshake_check_env_name (bson_t *doc, const char *expected)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   ASSERT (bson_iter_init_find (&md_iter, doc, "env"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));
   const char *name;

   ASSERT (bson_iter_find (&inner_iter, "name"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   name = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (name);
   ASSERT_CMPSTR (name, expected);
}

static void
_handshake_check_required_fields (bson_t *doc)
{
   _handshake_check_application (doc);
   _handshake_check_driver (doc);
   _handshake_check_os (doc);
   _handshake_check_platform (doc);
}

// Start a mock server, get the driver's handshake doc, and clean up
static bson_t *
_get_handshake_document (bool default_append)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;
   request_t *request;
   const bson_t *request_doc;
   bson_iter_t iter;

   /* Make sure setting the handshake works */
   if (default_append) {
      ASSERT (mongoc_handshake_data_append (default_driver_name, default_driver_version, default_platform));
   }

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_APPNAME, default_appname);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   /* Force topology scanner to start */
   client = mongoc_client_pool_pop (pool);

   request = mock_server_receives_any_hello (server);
   ASSERT (request);
   request_doc = request_get_doc (request, 0);
   ASSERT (request_doc);
   ASSERT (bson_has_field (request_doc, HANDSHAKE_FIELD));

   ASSERT (bson_iter_init_find (&iter, request_doc, HANDSHAKE_FIELD));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));

   uint32_t len;
   const uint8_t *data;
   bson_iter_document (&iter, &len, &data);
   bson_t *handshake_doc = bson_new_from_data (data, len);

   reply_to_request_simple (request, "{'ok': 1, 'isWritablePrimary': true}");
   request_destroy (request);

   /* Cleanup */
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);

   return handshake_doc;
}

/* Override host info with plausible but short strings to avoid any
 * truncation */
static void
_override_host_platform_os (void)
{
   _reset_handshake ();
   mongoc_handshake_t *md = _mongoc_handshake_get ();
   bson_free (md->os_type);
   md->os_type = bson_strdup ("Linux");
   bson_free (md->os_name);
   md->os_name = bson_strdup ("mongoc");
   bson_free (md->driver_name);
   md->driver_name = bson_strdup ("test_e");
   bson_free (md->driver_version);
   md->driver_version = bson_strdup ("1.25.0");
   bson_free (md->platform);
   md->platform = bson_strdup ("posix=1234");
   bson_free (md->compiler_info);
   md->compiler_info = bson_strdup ("CC=GCC");
   bson_free (md->flags);
   md->flags = bson_strdup ("CFLAGS=\"-fPIE\"");
}

// erase all FaaS variables used in testing
static void
clear_faas_env (void)
{
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", ""));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_RUNTIME_API", ""));
   ASSERT (_mongoc_setenv ("AWS_REGION", ""));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", ""));
   ASSERT (_mongoc_setenv ("FUNCTIONS_WORKER_RUNTIME", ""));
   ASSERT (_mongoc_setenv ("K_SERVICE", ""));
   ASSERT (_mongoc_setenv ("FUNCTION_MEMORY_MB", ""));
   ASSERT (_mongoc_setenv ("FUNCTION_TIMEOUT_SEC", ""));
   ASSERT (_mongoc_setenv ("FUNCTION_REGION", ""));
   ASSERT (_mongoc_setenv ("VERCEL", ""));
   ASSERT (_mongoc_setenv ("VERCEL_REGION", ""));
}

static void
test_mongoc_handshake_data_append_success (void)
{
   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   bson_iter_t md_iter;
   bson_iter_init (&md_iter, doc);
   _handshake_check_application (doc);
   _handshake_check_driver (doc);
   _handshake_check_os (doc);
   _handshake_check_platform (doc);

   bson_destroy (doc);
   _reset_handshake ();
}

static void
test_valid_aws_lambda (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_RUNTIME_API", "foo"));
   ASSERT (_mongoc_setenv ("AWS_REGION", "us-east-2"));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, default_memory_mb, 0, "us-east-2");
   _handshake_check_env_name (doc, "aws.lambda");

   bson_iter_t iter;
   ASSERT (bson_iter_init_find (&iter, doc, "env"));
   bson_iter_t inner_iter;
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   bson_iter_recurse (&iter, &inner_iter);

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_valid_aws_and_vercel (void *test_ctx)
{
   // Test that Vercel takes precedence over AWS. From the specification:
   // > When variables for multiple ``client.env.name`` values are present,
   // > ``vercel`` takes precedence over ``aws.lambda``

   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"));
   ASSERT (_mongoc_setenv ("AWS_REGION", "us-east-2"));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"));

   ASSERT (_mongoc_setenv ("VERCEL", "1"));
   ASSERT (_mongoc_setenv ("VERCEL_REGION", "cdg1"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, 0, 0, "cdg1");
   _handshake_check_env_name (doc, "vercel");

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_valid_aws (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"));
   ASSERT (_mongoc_setenv ("AWS_REGION", "us-east-2"));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);

   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, default_memory_mb, 0, "us-east-2");
   _handshake_check_env_name (doc, "aws.lambda");

   bson_iter_t iter;
   ASSERT (bson_iter_init_find (&iter, doc, "env"));
   bson_iter_t inner_iter;
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   bson_iter_recurse (&iter, &inner_iter);

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_valid_azure (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("FUNCTIONS_WORKER_RUNTIME", "node"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, 0, 0, NULL);
   _handshake_check_env_name (doc, "azure.func");

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_valid_gcp (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("K_SERVICE", "servicename"));
   ASSERT (_mongoc_setenv ("FUNCTION_MEMORY_MB", "1024"));
   ASSERT (_mongoc_setenv ("FUNCTION_TIMEOUT_SEC", "60"));
   ASSERT (_mongoc_setenv ("FUNCTION_REGION", "us-central1"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, default_memory_mb, default_timeout_sec, "us-central1");
   _handshake_check_env_name (doc, "gcp.func");

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_valid_vercel (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("VERCEL", "1"));
   ASSERT (_mongoc_setenv ("VERCEL_REGION", "cdg1"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env (doc, 0, 0, "cdg1");
   _handshake_check_env_name (doc, "vercel");

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_multiple_faas (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   // Multiple FaaS variables must cause the entire env field to be omitted
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"));
   ASSERT (_mongoc_setenv ("FUNCTIONS_WORKER_RUNTIME", "node"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);

   ASSERT (!bson_has_field (doc, "env"));

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_truncate_region (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"));
   const size_t region_len = 512;
   char long_region[512];
   memset (&long_region, 'a', region_len - 1);
   long_region[region_len - 1] = '\0';
   ASSERT (_mongoc_setenv ("AWS_REGION", long_region));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env_name (doc, "aws.lambda");

   bson_iter_t iter;
   ASSERT (bson_iter_init_find (&iter, doc, "env"));
   bson_iter_t inner_iter;
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   bson_iter_recurse (&iter, &inner_iter);
   ASSERT (!bson_iter_find (&inner_iter, "memory_mb"));
   ASSERT (!bson_iter_find (&inner_iter, "timeout_sec"));
   ASSERT (!bson_iter_find (&inner_iter, "region"));

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}


static void
test_wrong_types (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"));
   ASSERT (_mongoc_setenv ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "big"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);
   _handshake_check_env_name (doc, "aws.lambda");

   bson_iter_t iter;
   ASSERT (bson_iter_init_find (&iter, doc, "env"));
   bson_iter_t inner_iter;
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   bson_iter_recurse (&iter, &inner_iter);
   ASSERT (!bson_iter_find (&iter, "memory_mb"));

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_aws_not_lambda (void *test_ctx)
{
   BSON_UNUSED (test_ctx);
   // Entire env field must be omitted with non-lambda AWS
   ASSERT (_mongoc_setenv ("AWS_EXECUTION_ENV", "EC2"));

   _override_host_platform_os ();
   bson_t *doc = _get_handshake_document (true);
   _handshake_check_required_fields (doc);

   ASSERT (!bson_has_field (doc, "env"));

   bson_destroy (doc);
   clear_faas_env ();
   _reset_handshake ();
}

static void
test_mongoc_handshake_data_append_null_args (void)
{
   bson_iter_t md_iter;
   bson_iter_t inner_iter;
   const char *val;

   /* Make sure setting the handshake works */
   ASSERT (mongoc_handshake_data_append (NULL, NULL, NULL));

   _reset_handshake ();
   bson_t *handshake_doc = _get_handshake_document (false);
   bson_iter_init (&md_iter, handshake_doc);
   _handshake_check_application (handshake_doc);

   /* Make sure driver.name and driver.version and platform are all right */
   ASSERT (bson_iter_find (&md_iter, "driver"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));
   ASSERT (bson_iter_find (&inner_iter, "name"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strstr (val, " / ") == NULL); /* No append delimiter */

   ASSERT (bson_iter_find (&inner_iter, "version"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strstr (val, " / ") == NULL); /* No append delimiter */

   /* Check os type not empty */
   ASSERT (bson_iter_find (&md_iter, "os"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&md_iter));
   ASSERT (bson_iter_recurse (&md_iter, &inner_iter));

   ASSERT (bson_iter_find (&inner_iter, "type"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   ASSERT (strlen (val) > 0);

   /* Check os version valid */
   ASSERT (bson_iter_find (&inner_iter, "version"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   _check_os_version_valid (val);

   /* Check os arch is valid */
   ASSERT (bson_iter_find (&inner_iter, "architecture"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&inner_iter));
   val = bson_iter_utf8 (&inner_iter, NULL);
   ASSERT (val);
   _check_arch_string_valid (val);

   /* Not checking os_name, as the spec says it can be NULL. */

   /* Check platform field ok */
   ASSERT (bson_iter_find (&md_iter, "platform"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&md_iter));
   val = bson_iter_utf8 (&md_iter, NULL);
   ASSERT (val);
   /* standard val are < 100, may be truncated on some platform */
   if (strlen (val) < 250) {
      /* `printf("%s", NULL)` -> "(null)" with libstdc++, libc++, and STL */
      ASSERT (strstr (val, "null") == NULL);
   }

   bson_destroy (handshake_doc);
}


static void
_test_platform (bool platform_oversized)
{
   mongoc_handshake_t *md;
   size_t platform_len;
   const char *platform_suffix = "b";

   _reset_handshake ();

   md = _mongoc_handshake_get ();

   bson_free (md->os_type);
   md->os_type = bson_strdup ("foo");
   bson_free (md->os_name);
   md->os_name = bson_strdup ("foo");
   bson_free (md->os_version);
   md->os_version = bson_strdup ("foo");
   bson_free (md->os_architecture);
   md->os_architecture = bson_strdup ("foo");
   bson_free (md->driver_name);
   md->driver_name = bson_strdup ("foo");
   bson_free (md->driver_version);
   md->driver_version = bson_strdup ("foo");

   platform_len = HANDSHAKE_MAX_SIZE;
   if (platform_oversized) {
      platform_len += 100;
   }

   bson_free (md->platform);
   md->platform = bson_malloc (platform_len);
   memset (md->platform, 'a', platform_len - 1);
   md->platform[platform_len - 1] = '\0';

   /* returns true, but ignores the suffix; there's no room */
   ASSERT (mongoc_handshake_data_append (NULL, NULL, platform_suffix));
   ASSERT (!strstr (md->platform, "b"));

   bson_t *doc;
   ASSERT (doc = _mongoc_handshake_build_doc_with_application ("my app"));
   ASSERT_CMPUINT32 (doc->len, ==, (uint32_t) HANDSHAKE_MAX_SIZE);

   bson_destroy (doc);
   _reset_handshake (); /* frees the strings created above */
}


static void
test_mongoc_handshake_big_platform (void)
{
   _test_platform (false);
}


static void
test_mongoc_handshake_oversized_platform (void)
{
   _test_platform (true);
}


static void
test_mongoc_handshake_data_append_after_cmd (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   _reset_handshake ();

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?" MONGOC_URI_MAXPOOLSIZE "=1");

   /* Make sure that after we pop a client we can't set global handshake */
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   client = mongoc_client_pool_pop (pool);

   capture_logs (true);
   ASSERT (!mongoc_handshake_data_append ("a", "a", "a"));
   capture_logs (false);

   mongoc_client_pool_push (pool, client);

   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);

   _reset_handshake ();
}

/*
 * Append to the platform field a huge string
 * Make sure that it gets truncated
 */
static void
test_mongoc_handshake_too_big (void)
{
   mongoc_client_t *client;
   mock_server_t *server;
   mongoc_uri_t *uri;
   future_t *future;
   request_t *request;
   const bson_t *hello_doc;
   bson_iter_t iter;

   enum { BUFFER_SIZE = HANDSHAKE_MAX_SIZE };
   char big_string[BUFFER_SIZE];
   uint32_t len;
   const uint8_t *dummy;

   server = mock_server_new ();
   mock_server_run (server);

   _reset_handshake ();

   memset (big_string, 'a', BUFFER_SIZE - 1);
   big_string[BUFFER_SIZE - 1] = '\0';
   ASSERT (mongoc_handshake_data_append (NULL, NULL, big_string));

   uri = mongoc_uri_copy (mock_server_get_uri (server));
   /* avoid rare test timeouts */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 20000);

   client = test_framework_client_new_from_uri (uri, NULL);

   ASSERT (mongoc_client_set_appname (client, "my app"));

   /* Send a ping, mock server deals with it */
   future = future_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);
   request = mock_server_receives_any_hello (server);

   /* Make sure the hello request has a handshake field, and it's not huge */
   ASSERT (request);
   hello_doc = request_get_doc (request, 0);
   ASSERT (hello_doc);
   ASSERT (bson_has_field (hello_doc, HANDSHAKE_FIELD));

   /* hello with handshake isn't too big */
   bson_iter_init_find (&iter, hello_doc, HANDSHAKE_FIELD);
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   bson_iter_document (&iter, &len, &dummy);

   /* Should have truncated the platform field so it fits exactly */
   ASSERT (len == HANDSHAKE_MAX_SIZE);

   reply_to_request_simple (
      request, tmp_str ("{'ok': 1, 'minWireVersion': %d, 'maxWireVersion': %d}", WIRE_VERSION_MIN, WIRE_VERSION_MAX));
   request_destroy (request);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'admin', 'ping': 1}"));

   reply_to_request_simple (request, "{'ok': 1}");
   ASSERT (future_get_bool (future));

   future_destroy (future);
   request_destroy (request);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);

   /* So later tests don't have "aaaaa..." as the md platform string */
   _reset_handshake ();
}

/*
 * Testing whether platform string data is truncated/dropped appropriately
 * drop specifies what case should be tested for
 */
static void
test_mongoc_platform_truncate (int drop)
{
   mongoc_handshake_t *md;
   bson_iter_t iter;

   char *undropped;
   char *expected;
   char big_string[HANDSHAKE_MAX_SIZE];
   memset (big_string, 'a', HANDSHAKE_MAX_SIZE - 1);
   big_string[HANDSHAKE_MAX_SIZE - 1] = '\0';

   /* Need to know how much space storing fields in our BSON will take
    * so that we can make our platform string the correct length here */
   _reset_handshake ();

   /* we manually bypass the defaults of the handshake to ensure an exceedingly
    * long field does not cause our test to incorrectly fail */
   md = _mongoc_handshake_get ();
   bson_free (md->os_type);
   md->os_type = bson_strdup ("test_a");
   bson_free (md->os_name);
   md->os_name = bson_strdup ("test_b");
   bson_free (md->os_architecture);
   md->os_architecture = bson_strdup ("test_d");
   bson_free (md->driver_name);
   md->driver_name = bson_strdup ("test_e");
   bson_free (md->driver_version);
   md->driver_version = bson_strdup ("test_f");

   // Set all fields used to generate the platform string to empty
   bson_free (md->platform);
   md->platform = bson_strdup ("");
   bson_free (md->compiler_info);
   md->compiler_info = bson_strdup ("");
   bson_free (md->flags);
   md->flags = bson_strdup ("");

   // Set os_version to a long string to force drop all os fields except name
   bson_free (md->os_version);
   md->os_version = big_string;

   bson_t *handshake_no_platform = _mongoc_handshake_build_doc_with_application (default_appname);
   size_t handshake_remaining_space = HANDSHAKE_MAX_SIZE - handshake_no_platform->len;
   bson_destroy (handshake_no_platform);

   md->os_version = bson_strdup ("test_c");
   bson_free (md->compiler_info);
   md->compiler_info = bson_strdup ("test_g");
   bson_free (md->flags);
   md->flags = bson_strdup ("test_h");

   /* adjust remaining space depending on which combination of
    * flags/compiler_info we want to test dropping */
   if (drop == 2) {
      undropped = bson_strdup_printf ("%s", "");
   } else if (drop == 1) {
      handshake_remaining_space -= strlen (md->compiler_info);
      undropped = bson_strdup_printf ("%s", md->compiler_info);
   } else {
      handshake_remaining_space -= strlen (md->flags) + strlen (md->compiler_info);
      undropped = bson_strdup_printf ("%s%s", md->compiler_info, md->flags);
   }

   big_string[handshake_remaining_space] = '\0';
   ASSERT (mongoc_handshake_data_append (NULL, NULL, big_string));

   bson_t *doc;
   ASSERT (doc = _mongoc_handshake_build_doc_with_application (default_appname));

   /* doc.len being strictly less than HANDSHAKE_MAX_SIZE proves that we have
    * dropped the flags correctly, instead of truncating anything
    */

   ASSERT_CMPUINT32 (doc->len, <=, (uint32_t) HANDSHAKE_MAX_SIZE);
   ASSERT (bson_iter_init_find (&iter, doc, "platform"));
   expected = bson_strdup_printf ("%s%s", big_string, undropped);
   ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL), expected);

   bson_free (expected);
   bson_free (undropped);
   bson_destroy (doc);
   /* So later tests don't have "aaaaa..." as the md platform string */
   _reset_handshake ();
}

/*
 * Test dropping neither compiler_info/flags, dropping just flags, and dropping
 * both
 */
static void
test_mongoc_oversized_flags (void)
{
   test_mongoc_platform_truncate (0);
   test_mongoc_platform_truncate (1);
   test_mongoc_platform_truncate (2);
}

/* Test the case where we can't prevent the handshake doc being too big
 * and so we just don't send it */
static void
test_mongoc_handshake_cannot_send (void)
{
   mock_server_t *server;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;
   request_t *request;
   const char *const server_reply = "{'ok': 1, 'isWritablePrimary': true}";
   const bson_t *request_doc;
   char big_string[HANDSHAKE_MAX_SIZE];

   _reset_handshake ();
   capture_logs (true);

   memset (big_string, 'a', HANDSHAKE_MAX_SIZE - 1);
   big_string[HANDSHAKE_MAX_SIZE - 1] = '\0';

   /* The handshake cannot be built if a field that cannot be dropped
    * (os.type) is set to a very long string */
   mongoc_handshake_t *md = _mongoc_handshake_get ();
   bson_free (md->os_type);
   md->os_type = bson_strdup (big_string);

   server = mock_server_new ();
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   /* Pop a client to trigger the topology scanner */
   client = mongoc_client_pool_pop (pool);
   request = mock_server_receives_any_hello (server);

   /* Make sure the hello request DOESN'T have a handshake field: */
   ASSERT (request);
   request_doc = request_get_doc (request, 0);
   ASSERT (request_doc);
   ASSERT (!bson_has_field (request_doc, HANDSHAKE_FIELD));

   reply_to_request_simple (request, server_reply);
   request_destroy (request);

   /* Cause failure on client side */
   request = mock_server_receives_any_hello (server);
   ASSERT (request);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Make sure the hello request still DOESN'T have a handshake field
    * on subsequent heartbeats. */
   request = mock_server_receives_any_hello (server);
   ASSERT (request);
   request_doc = request_get_doc (request, 0);
   ASSERT (request_doc);
   ASSERT (!bson_has_field (request_doc, HANDSHAKE_FIELD));

   reply_to_request_simple (request, server_reply);
   request_destroy (request);

   /* cleanup */
   mongoc_client_pool_push (pool, client);

   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);

   /* Reset again so the next tests don't have a handshake doc which
    * is too big */
   _reset_handshake ();
}

extern char *
_mongoc_handshake_get_config_hex_string (void);

static bool
_get_bit (char *config_str, uint32_t bit)
{
   /* get the location of the two byte chars for this bit. */
   uint32_t byte = bit / 8;
   uint32_t bit_of_byte = bit % 8;
   uint32_t char_loc;
   uint32_t as_num;
   char byte_str[3];

   /* byte 0 is represented at the last two characters. */
   char_loc = (uint32_t) strlen (config_str) - 2 - (byte * 2);
   /* index should be past the prefixed "0x" */
   ASSERT_CMPINT32 (char_loc, >, 1);
   /* get the number representation of the byte. */
   byte_str[0] = config_str[char_loc];
   byte_str[1] = config_str[char_loc + 1];
   byte_str[2] = '\0';
   as_num = (uint8_t) strtol (byte_str, NULL, 16);
   return (as_num & (1u << bit_of_byte)) > 0u;
}

void
test_handshake_platform_config (void)
{
   /* Parse the config string, and check that it matches the defined flags. */
   char *config_str = _mongoc_handshake_get_config_hex_string ();
   uint32_t total_bytes = (LAST_MONGOC_MD_FLAG + 7) / 8;
   uint32_t total_bits = 8 * total_bytes;
   uint32_t i;
   /* config_str should have the form 0x?????. */
   ASSERT_CMPINT ((int) strlen (config_str), ==, 2 + (2 * total_bytes));
   BSON_ASSERT (strncmp (config_str, "0x", 2) == 0);

/* go through all flags. */
#ifdef MONGOC_ENABLE_SSL_SECURE_CHANNEL
   BSON_ASSERT (_get_bit (config_str, MONGOC_ENABLE_SSL_SECURE_CHANNEL));
#endif

#ifdef MONGOC_ENABLE_CRYPTO_CNG
   BSON_ASSERT (_get_bit (config_str, MONGOC_ENABLE_CRYPTO_CNG));
#endif

#ifdef MONGOC_ENABLE_SSL_SECURE_TRANSPORT
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SSL_SECURE_TRANSPORT));
#endif

#ifdef MONGOC_ENABLE_CRYPTO_COMMON_CRYPTO
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_CRYPTO_COMMON_CRYPTO));
#endif

#ifdef MONGOC_ENABLE_SSL_OPENSSL
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SSL_OPENSSL));
#endif

#ifdef MONGOC_ENABLE_CRYPTO_LIBCRYPTO
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_CRYPTO_LIBCRYPTO));
#endif

#ifdef MONGOC_ENABLE_SSL
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SSL));
#endif

#ifdef MONGOC_ENABLE_CRYPTO
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_CRYPTO));
#endif

#ifdef MONGOC_ENABLE_CRYPTO_SYSTEM_PROFILE
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_CRYPTO_SYSTEM_PROFILE));
#endif

#ifdef MONGOC_ENABLE_SASL
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SASL));
#endif

#ifdef MONGOC_HAVE_SASL_CLIENT_DONE
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_HAVE_SASL_CLIENT_DONE));
#endif

#ifdef MONGOC_NO_AUTOMATIC_GLOBALS
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_NO_AUTOMATIC_GLOBALS));
#endif

#ifdef MONGOC_EXPERIMENTAL_FEATURES
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_EXPERIMENTAL_FEATURES));
#endif

#ifdef MONGOC_ENABLE_SSL_LIBRESSL
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SSL_LIBRESSL));
#endif

#ifdef MONGOC_ENABLE_SASL_CYRUS
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SASL_CYRUS));
#endif

#ifdef MONGOC_ENABLE_SASL_SSPI
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SASL_SSPI));
#endif

#ifdef MONGOC_HAVE_SOCKLEN
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_HAVE_SOCKLEN));
#endif

#ifdef MONGOC_ENABLE_COMPRESSION
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_COMPRESSION));
#endif

#ifdef MONGOC_ENABLE_COMPRESSION_SNAPPY
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_COMPRESSION_SNAPPY));
#endif

#ifdef MONGOC_ENABLE_COMPRESSION_ZLIB
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_COMPRESSION_ZLIB));
#endif

#ifdef MONGOC_MD_FLAG_ENABLE_SASL_GSSAPI
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SASL_GSSAPI));
#endif

#ifdef MONGOC_HAVE_RES_NSEARCH
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_RES_NSEARCH));
#endif

#ifdef MONGOC_HAVE_RES_NDESTROY
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_RES_NDESTROY));
#endif

#ifdef MONGOC_HAVE_RES_NCLOSE
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_RES_NCLOSE));
#endif

#ifdef MONGOC_HAVE_RES_SEARCH
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_RES_SEARCH));
#endif

#ifdef MONGOC_HAVE_DNSAPI
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_DNSAPI));
#endif

#ifdef MONGOC_HAVE_RDTSCP
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_RDTSCP));
#endif

#ifdef MONGOC_HAVE_SCHED_GETCPU
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_HAVE_SCHED_GETCPU));
#endif

   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SRV) == MONGOC_SRV_ENABLED);

#ifdef MONGOC_ENABLE_SHM_COUNTERS
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_SHM_COUNTERS));
#endif

   if (MONGOC_TRACE_ENABLED) {
      BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_TRACE));
   }

   // Check that `MONGOC_MD_FLAG_ENABLE_ICU` is always unset. libicu dependency
   // was removed in CDRIVER-4680.
   BSON_ASSERT (!_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_ICU_UNUSED));

#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_CLIENT_SIDE_ENCRYPTION));
#endif

#ifdef MONGOC_ENABLE_MONGODB_AWS_AUTH
   BSON_ASSERT (_get_bit (config_str, MONGOC_MD_FLAG_ENABLE_MONGODB_AWS_AUTH));
#endif

   /* any excess bits should all be zero. */
   for (i = LAST_MONGOC_MD_FLAG; i < total_bits; i++) {
      BSON_ASSERT (!_get_bit (config_str, i));
   }
   bson_free (config_str);
}

/* Called by a single thread in test_mongoc_handshake_race_condition */
static BSON_THREAD_FUN (handshake_append_worker, data)
{
   BSON_UNUSED (data);

   mongoc_handshake_data_append (default_driver_name, default_driver_version, default_platform);

   BSON_THREAD_RETURN;
}

/* Run 1000 iterations of mongoc_handshake_data_append() using 4 threads */
static void
test_mongoc_handshake_race_condition (void)
{
   unsigned i, j;
   bson_thread_t threads[4];

   for (i = 0; i < 1000; ++i) {
      _reset_handshake ();

      for (j = 0; j < 4; ++j) {
         BSON_ASSERT (!mcommon_thread_create (&threads[j], &handshake_append_worker, NULL /* args */));
      }
      for (j = 0; j < 4; ++j) {
         mcommon_thread_join (threads[j]);
      }
   }

   _reset_handshake ();
}

void
test_handshake_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/MongoDB/handshake/appname_in_uri", test_mongoc_handshake_appname_in_uri);
   TestSuite_Add (suite, "/MongoDB/handshake/appname_frozen_single", test_mongoc_handshake_appname_frozen_single);
   TestSuite_Add (suite, "/MongoDB/handshake/appname_frozen_pooled", test_mongoc_handshake_appname_frozen_pooled);

   TestSuite_AddMockServerTest (suite, "/MongoDB/handshake/success", test_mongoc_handshake_data_append_success);
   TestSuite_AddMockServerTest (suite, "/MongoDB/handshake/null_args", test_mongoc_handshake_data_append_null_args);
   TestSuite_Add (suite, "/MongoDB/handshake/big_platform", test_mongoc_handshake_big_platform);
   TestSuite_Add (suite, "/MongoDB/handshake/oversized_platform", test_mongoc_handshake_oversized_platform);
   TestSuite_Add (suite, "/MongoDB/handshake/failure", test_mongoc_handshake_data_append_after_cmd);
   TestSuite_AddMockServerTest (suite, "/MongoDB/handshake/too_big", test_mongoc_handshake_too_big);
   TestSuite_Add (suite, "/MongoDB/handshake/oversized_flags", test_mongoc_oversized_flags);
   TestSuite_AddMockServerTest (suite, "/MongoDB/handshake/cannot_send", test_mongoc_handshake_cannot_send);
   TestSuite_Add (suite, "/MongoDB/handshake/platform_config", test_handshake_platform_config);
   TestSuite_Add (suite, "/MongoDB/handshake/race_condition", test_mongoc_handshake_race_condition);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_aws",
                      test_valid_aws,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_azure",
                      test_valid_azure,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_gcp",
                      test_valid_gcp,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_aws_lambda",
                      test_valid_aws_lambda,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_aws_and_vercel",
                      test_valid_aws_and_vercel,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/valid_vercel",
                      test_valid_vercel,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/multiple",
                      test_multiple_faas,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/truncate_region",
                      test_truncate_region,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/wrong_types",
                      test_wrong_types,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
   TestSuite_AddFull (suite,
                      "/MongoDB/handshake/faas/aws_not_lambda",
                      test_aws_not_lambda,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_setenv);
}
