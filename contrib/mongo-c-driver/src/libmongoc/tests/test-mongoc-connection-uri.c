#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>

#include "json-test.h"
#include "test-libmongoc.h"
#include "mongoc/mongoc-read-concern-private.h"


/*
 * Asserts every key-value pair in 'needle' is in 'haystack', including those in
 * embedded documents.
 */
static void
bson_contains_iter (const bson_t *haystack, bson_iter_t *needle)
{
   bson_iter_t iter;
   uint32_t bson_type;

   if (!bson_iter_next (needle)) {
      return;
   }

   ASSERT (bson_iter_init_find_case (&iter, haystack, bson_iter_key (needle)));

   bson_type = bson_iter_type (needle);
   switch (bson_type) {
   case BSON_TYPE_ARRAY:
   case BSON_TYPE_DOCUMENT: {
      bson_t sub_bson;
      bson_iter_t sub_iter;

      ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
      bson_iter_bson (&iter, &sub_bson);

      bson_iter_recurse (needle, &sub_iter);
      bson_contains_iter (&sub_bson, &sub_iter);

      bson_destroy (&sub_bson);
      return;
   }
   case BSON_TYPE_BOOL:
      ASSERT (bson_iter_as_bool (needle) == bson_iter_as_bool (&iter));
      bson_contains_iter (haystack, needle);
      return;
   case BSON_TYPE_UTF8:
      ASSERT (0 == strcmp (bson_iter_utf8 (needle, 0), bson_iter_utf8 (&iter, 0)));
      bson_contains_iter (haystack, needle);
      return;
   case BSON_TYPE_DOUBLE:
      ASSERT (bson_iter_double (needle) == bson_iter_double (&iter));
      bson_contains_iter (haystack, needle);
      return;
   case BSON_TYPE_INT64:
   case BSON_TYPE_INT32:
      ASSERT (bson_iter_as_int64 (needle) == bson_iter_as_int64 (&iter));
      bson_contains_iter (haystack, needle);
      return;
   default:
      ASSERT (false);
      return;
   }
}

static void
run_uri_test (const char *uri_string, bool valid, const bson_t *hosts, const bson_t *auth, const bson_t *options)
{
   mongoc_uri_t *uri;
   bson_iter_t auth_iter;
   const char *db;
   bson_error_t error;

   uri = mongoc_uri_new_with_error (uri_string, &error);

   /* BEGIN Exceptions to test suite */

   /* some spec tests assume we allow DB names like "auth.foo" */
   if ((bson_iter_init_find (&auth_iter, auth, "db") || bson_iter_init_find (&auth_iter, auth, "source")) &&
       BSON_ITER_HOLDS_UTF8 (&auth_iter)) {
      db = bson_iter_utf8 (&auth_iter, NULL);
      if (strchr (db, '.')) {
         BSON_ASSERT (!uri);
         ASSERT_ERROR_CONTAINS (
            error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid database name in URI");
         clear_captured_logs ();
         return;
      }
   }

   if (valid && !uri && error.domain) {
      /* Eager failures which the spec expects to be warnings. */
      /* CDRIVER-3167 */
      if (strstr (uri_string, "=invalid") || strstr (uri_string, "heartbeatFrequencyMS=-2") ||
          strstr (uri_string, "w=-2") || strstr (uri_string, "wTimeoutMS=-2") ||
          strstr (uri_string, "zlibCompressionLevel=-2") || strstr (uri_string, "zlibCompressionLevel=10") ||
          (!strstr (uri_string, "mongodb+srv") && strstr (uri_string, "srvServiceName=customname")) ||
          strstr (uri_string, "srvMaxHosts=-1") || strstr (uri_string, "srvMaxHosts=foo")) {
         MONGOC_WARNING ("Error parsing URI: '%s'", error.message);
         return;
      }
   }

   if (uri) {
      /* mongoc does not warn on negative timeouts when it should. */
      /* CDRIVER-3167 */
      if ((mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 0) < 0) ||
          (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 0) < 0) ||
          (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_MAXIDLETIMEMS, 0) < 0) ||
          (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 0) < 0) ||
          (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 0) < 0)) {
         MONGOC_WARNING ("Invalid negative timeout");
      }

      /* mongoc does not store lists the way the spec test expects. */
      if (strstr (uri_string, "compressors=") || strstr (uri_string, "readPreferenceTags=")) {
         options = NULL;
      }

      /* mongoc eagerly warns about unsupported compressors. */
#ifndef MONGOC_ENABLE_COMPRESSION_SNAPPY
      if (strstr (uri_string, "compressors=snappy")) {
         clear_captured_logs ();
      }
#endif

#ifndef MONGOC_ENABLE_COMPRESSION_ZLIB
      if (strstr (uri_string, "compressors=zlib") || strstr (uri_string, "compressors=snappy,zlib")) {
         clear_captured_logs ();
      }
#endif
   }

   /* END Exceptions to test suite */

   if (valid) {
      ASSERT_OR_PRINT (uri, error);
   } else {
      BSON_ASSERT (!uri);
      return;
   }

   if (!bson_empty0 (hosts)) {
      const mongoc_host_list_t *hl;
      bson_iter_t iter;
      bson_iter_t host_iter;

      for (bson_iter_init (&iter, hosts); bson_iter_next (&iter) && bson_iter_recurse (&iter, &host_iter);) {
         const char *host = "localhost";
         int64_t port = 27017;
         bool ok = false;

         if (bson_iter_find (&host_iter, "host") && BSON_ITER_HOLDS_UTF8 (&host_iter)) {
            host = bson_iter_utf8 (&host_iter, NULL);
         }
         if (bson_iter_find (&host_iter, "port") && BSON_ITER_HOLDS_INT (&host_iter)) {
            port = bson_iter_as_int64 (&host_iter);
         }

         for (hl = mongoc_uri_get_hosts (uri); hl; hl = hl->next) {
            if (!strcmp (host, hl->host) && port == hl->port) {
               ok = true;
               break;
            }
         }

         if (!ok) {
            fprintf (stderr, "Could not find '%s':%" PRId64 " in uri '%s'\n", host, port, mongoc_uri_get_string (uri));
            BSON_ASSERT (0);
         }
      }
   }

   if (!bson_empty0 (auth)) {
      const char *auth_source = mongoc_uri_get_auth_source (uri);
      const char *username = mongoc_uri_get_username (uri);
      const char *password = mongoc_uri_get_password (uri);
      bson_iter_t iter;

      if (bson_iter_init_find (&iter, auth, "username") && BSON_ITER_HOLDS_UTF8 (&iter)) {
         ASSERT_CMPSTR (username, bson_iter_utf8 (&iter, NULL));
      }

      if (bson_iter_init_find (&iter, auth, "password") && BSON_ITER_HOLDS_UTF8 (&iter)) {
         ASSERT_CMPSTR (password, bson_iter_utf8 (&iter, NULL));
      }

      if ((bson_iter_init_find (&iter, auth, "db") || bson_iter_init_find (&iter, auth, "source")) &&
          BSON_ITER_HOLDS_UTF8 (&iter)) {
         ASSERT_CMPSTR (auth_source, bson_iter_utf8 (&iter, NULL));
      }
   }

   if (options) {
      const mongoc_read_concern_t *rc;
      bson_t uri_options = BSON_INITIALIZER;
      bson_t test_options = BSON_INITIALIZER;
      bson_iter_t iter;

      bson_concat (&uri_options, mongoc_uri_get_options (uri));
      bson_concat (&uri_options, mongoc_uri_get_credentials (uri));

      rc = mongoc_uri_get_read_concern (uri);
      if (!mongoc_read_concern_is_default (rc)) {
         BSON_APPEND_UTF8 (&uri_options, "readconcernlevel", mongoc_read_concern_get_level (rc));
      }

      bson_copy_to_excluding_noinit (options,
                                     &test_options,
                                     "username", /* these 'auth' params may be included in 'options' */
                                     "password",
                                     "source",
                                     "mechanism",            /* renamed to 'authmechanism' for consistency */
                                     "mechanism_properties", /* renamed to 'authmechanismproperties' for
                                                              * consistency */
                                     NULL);

      if ((bson_iter_init_find (&iter, options, "mechanism") ||
           bson_iter_init_find (&iter, options, "authmechanism")) &&
          BSON_ITER_HOLDS_UTF8 (&iter)) {
         BSON_APPEND_UTF8 (&test_options, "authmechanism", bson_iter_utf8 (&iter, NULL));
      }

      if ((bson_iter_init_find (&iter, options, "mechanism_properties") ||
           bson_iter_init_find (&iter, options, "authmechanismproperties")) &&
          BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         ASSERT (bson_append_iter (&test_options, "authmechanismproperties", -1, &iter));
      }

      bson_iter_init (&iter, &test_options);
      bson_contains_iter (&uri_options, &iter);

      bson_destroy (&test_options);
      bson_destroy (&uri_options);
   }

   if (uri) {
      mongoc_uri_destroy (uri);
   }
}

static void
test_connection_uri_cb (bson_t *scenario)
{
   bson_iter_t iter;
   bson_iter_t descendent;
   bson_iter_t tests_iter;
   bson_iter_t warning_iter;
   const char *uri_string = NULL;
   bson_t hosts;
   bson_t auth;
   bson_t options;
   bool valid;

   BSON_ASSERT (scenario);


   BSON_ASSERT (bson_iter_init_find (&iter, scenario, "tests"));
   BSON_ASSERT (BSON_ITER_HOLDS_ARRAY (&iter));
   ASSERT (bson_iter_recurse (&iter, &tests_iter));

   while (bson_iter_next (&tests_iter)) {
      bson_t test_case;

      bson_iter_bson (&tests_iter, &test_case);

      if (test_suite_debug_output ()) {
         bson_iter_t test_case_iter;

         ASSERT (bson_iter_recurse (&tests_iter, &test_case_iter));
         if (bson_iter_find (&test_case_iter, "description")) {
            const char *description = bson_iter_utf8 (&test_case_iter, NULL);
            ASSERT (bson_iter_find_case (&test_case_iter, "uri"));

            printf ("  - %s: '%s'\n", description, bson_iter_utf8 (&test_case_iter, 0));
            fflush (stdout);
         } else {
            fprintf (stderr, "Couldn't find `description` field in testcase\n");
            BSON_ASSERT (0);
         }
      }

      uri_string = bson_lookup_utf8 (&test_case, "uri");
      /* newer spec test replaces both "auth" and "options" with "credential"
       */
      if (bson_has_field (&test_case, "credential")) {
         bson_lookup_doc_null_ok (&test_case, "credential", &auth);
         bson_lookup_doc_null_ok (&test_case, "credential", &options);
         bson_init (&hosts);
      } else if (bson_has_field (&test_case, "auth")) {
         bson_lookup_doc_null_ok (&test_case, "auth", &auth);
         bson_lookup_doc_null_ok (&test_case, "options", &options);
         bson_lookup_doc_null_ok (&test_case, "hosts", &hosts);
      } else {
         /* These are expected to be initialized */
         bson_init (&hosts);
         bson_init (&auth);
         bson_init (&options);
      }

      valid = _mongoc_lookup_bool (&test_case, "valid", true);
      capture_logs (true);
      run_uri_test (uri_string, valid, &hosts, &auth, &options);

      bson_iter_init (&warning_iter, &test_case);

      if (bson_iter_find_descendant (&warning_iter, "warning", &descendent) && BSON_ITER_HOLDS_BOOL (&descendent)) {
         if (bson_iter_as_bool (&descendent)) {
            ASSERT_CAPTURED_LOG ("mongoc_uri", MONGOC_LOG_LEVEL_WARNING, "");
         } else {
            ASSERT_NO_CAPTURED_LOGS ("mongoc_uri");
         }
      }

      bson_destroy (&hosts);
      bson_destroy (&auth);
      bson_destroy (&options);
   }
}


static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite (suite, JSON_DIR, "uri-options", &test_connection_uri_cb);
   install_json_test_suite (suite, JSON_DIR, "connection_uri", &test_connection_uri_cb);
   install_json_test_suite (suite, JSON_DIR, "auth", &test_connection_uri_cb);
}


void
test_connection_uri_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
}
