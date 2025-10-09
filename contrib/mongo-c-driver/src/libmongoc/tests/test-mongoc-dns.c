#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc.h"
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-thread-private.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/utlist.h"

#ifdef MONGOC_ENABLE_SSL
#include "mongoc/mongoc-ssl.h"
#include "mongoc/mongoc-ssl-private.h"
#endif

#include "json-test.h"
#include "test-libmongoc.h"

static void
_assert_options_match (const bson_t *test, mongoc_uri_t *uri)
{
   match_ctx_t ctx = {{0}};
   bson_iter_t iter;
   bson_t opts_from_test;
   const bson_t *opts_from_uri;
   const bson_t *creds_from_uri;
   const bson_t *opts_or_creds;
   bson_iter_t test_opts_iter;
   bson_iter_t uri_opts_iter;
   const char *opt_name, *opt_name_canon;
   const bson_value_t *test_value, *uri_value;

   if (!bson_iter_init_find (&iter, test, "options")) {
      /* no URI options specified in the test */
      return;
   }

   bson_iter_bson (&iter, &opts_from_test);
   BSON_ASSERT (bson_iter_init (&test_opts_iter, &opts_from_test));

   opts_from_uri = mongoc_uri_get_options (uri);
   creds_from_uri = mongoc_uri_get_credentials (uri);

   while (bson_iter_next (&test_opts_iter)) {
      opt_name = bson_iter_key (&test_opts_iter);
      opt_name_canon = mongoc_uri_canonicalize_option (opt_name);
      opts_or_creds = !bson_strcasecmp (opt_name, "authSource") ? creds_from_uri : opts_from_uri;
      if (!bson_iter_init_find_case (&uri_opts_iter, opts_or_creds, opt_name_canon)) {
         test_error ("URI options incorrectly set from TXT record: "
                     "no option named \"%s\"\n"
                     "expected: %s\n"
                     "actual: %s",
                     opt_name,
                     bson_as_json (&opts_from_test, NULL),
                     bson_as_json (opts_or_creds, NULL));
      }

      test_value = bson_iter_value (&test_opts_iter);
      uri_value = bson_iter_value (&uri_opts_iter);
      if (!match_bson_value (uri_value, test_value, &ctx)) {
         test_error ("URI option \"%s\" incorrectly set from TXT record: %s\n"
                     "expected: %s\n"
                     "actual: %s",
                     opt_name,
                     ctx.errmsg,
                     bson_as_json (&opts_from_test, NULL),
                     bson_as_json (opts_from_uri, NULL));
      }
   }
}


typedef struct {
   bson_mutex_t mutex;
   mongoc_host_list_t *hosts;
} context_t;


static void
topology_changed (const mongoc_apm_topology_changed_t *event)
{
   context_t *ctx;
   const mongoc_topology_description_t *td;
   size_t i;
   size_t n;
   mongoc_server_description_t **sds;

   ctx = (context_t *) mongoc_apm_topology_changed_get_context (event);

   td = mongoc_apm_topology_changed_get_new_description (event);
   sds = mongoc_topology_description_get_servers (td, &n);

   bson_mutex_lock (&ctx->mutex);
   _mongoc_host_list_destroy_all (ctx->hosts);
   ctx->hosts = NULL;
   for (i = 0; i < n; i++) {
      ctx->hosts = _mongoc_host_list_push (sds[i]->host.host, sds[i]->host.port, AF_UNSPEC, ctx->hosts);
   }
   bson_mutex_unlock (&ctx->mutex);

   mongoc_server_descriptions_destroy_all (sds, n);
}


static bool
host_list_contains (const mongoc_host_list_t *hl, const char *host_and_port)
{
   while (hl) {
      if (!strcmp (hl->host_and_port, host_and_port)) {
         return true;
      }

      hl = hl->next;
   }

   return false;
}


static int64_t
hosts_count (const bson_t *test)
{
   bson_iter_t iter;
   bson_iter_t hosts;
   int64_t c = 0;

   if (bson_iter_init_find (&iter, test, "hosts")) {
      BSON_ASSERT (bson_iter_recurse (&iter, &hosts));
      while (bson_iter_next (&hosts)) {
         c++;
      }
   }

   else if (bson_iter_init_find (&iter, test, "numHosts")) {
      c = bson_iter_as_int64 (&iter);
   }

   return c;
}


static bool
_host_list_matches (const bson_t *test, context_t *ctx)
{
   bson_iter_t iter;
   bson_iter_t hosts;
   const char *host_and_port;
   bool ret = true;

   if (bson_iter_init_find (&iter, test, "hosts")) {
      BSON_ASSERT (bson_iter_recurse (&iter, &hosts));

      bson_mutex_lock (&ctx->mutex);
      BSON_ASSERT (bson_iter_recurse (&iter, &hosts));
      while (bson_iter_next (&hosts)) {
         host_and_port = bson_iter_utf8 (&hosts, NULL);
         if (!host_list_contains (ctx->hosts, host_and_port)) {
            ret = false;
            break;
         }
      }

      _mongoc_host_list_destroy_all (ctx->hosts);
      ctx->hosts = NULL;
      bson_mutex_unlock (&ctx->mutex);
   }

   else if (bson_iter_init_find (&iter, test, "numHosts")) {
      const int64_t expected = bson_iter_as_int64 (&iter);

      bson_mutex_lock (&ctx->mutex);
      const size_t actual = _mongoc_host_list_length (ctx->hosts);
      _mongoc_host_list_destroy_all (ctx->hosts);
      ctx->hosts = NULL;
      bson_mutex_unlock (&ctx->mutex);

      ret = bson_cmp_equal_su (expected, actual);
   }

   return ret;
}

typedef struct {
   const char *uri_str;
   const char *reason;
} skipped_dns_test_t;

skipped_dns_test_t SKIPPED_DNS_TESTS[] = {{"mongodb+srv://test5.test.build.10gen.cc/?authSource=otherDB",
                                           "C driver requires username present if any auth fields are present"},
                                          {0}};

static bool
is_test_skipped (const char *uri_str)
{
   skipped_dns_test_t *skip;

   for (skip = SKIPPED_DNS_TESTS; skip->uri_str != NULL; skip++) {
      if (!strcmp (skip->uri_str, uri_str)) {
         MONGOC_DEBUG ("Skipping test of URI: %s Reason: %s", skip->uri_str, skip->reason);
         return true;
      }
   }

   return false;
}

static void
_test_dns_maybe_pooled (bson_t *test, bool pooled)
{
   context_t ctx;
   bool expect_ssl;
   bool expect_error;
   mongoc_uri_t *uri;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
#ifdef MONGOC_ENABLE_SSL
   mongoc_ssl_opt_t ssl_opts;
#endif
   bson_error_t error;
   bool r;
   const char *uri_str;

   if (!test_framework_get_ssl ()) {
      test_error ("Must configure an SSL replica set and set MONGOC_TEST_SSL=on "
                  "and other ssl options to test DNS");
   }

   uri_str = bson_lookup_utf8 (test, "uri");
   if (is_test_skipped (uri_str)) {
      return;
   }

   bson_mutex_init (&ctx.mutex);
   ctx.hosts = NULL;
   expect_ssl = strstr (uri_str, "ssl=false") == NULL;
   expect_error = _mongoc_lookup_bool (test, "error", false /* default */);

   uri = mongoc_uri_new_with_error (uri_str, &error);
   if (!expect_error) {
      ASSERT_OR_PRINT (uri, error);
   }

   if (!uri) {
      /* expected failure, e.g. we're testing an invalid URI */
      return;
   }

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_topology_changed_cb (callbacks, topology_changed);

   /* suppress "cannot override URI option" messages */
   capture_logs (true);

#ifdef MONGOC_ENABLE_SSL
   ssl_opts = *test_framework_get_ssl_opts ();
   ssl_opts.allow_invalid_hostname = true;
#endif

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (uri, NULL);

      if (!expect_error) {
         BSON_ASSERT (pool);
      }

      if (!pool) {
         /* expected failure, e.g. SRV lookup or URI finalization failed */
         goto cleanup;
      }

      /* before we set SSL on so that we can connect to the test replica set,
       * assert that the URI has SSL on by default, and SSL off if "ssl=false"
       * is in the URI string */
      BSON_ASSERT (mongoc_uri_get_tls (_mongoc_client_pool_get_topology (pool)->uri) == expect_ssl);
#ifdef MONGOC_ENABLE_SSL
      mongoc_client_pool_set_ssl_opts (pool, &ssl_opts);
#else
      test_framework_set_pool_ssl_opts (pool);
#endif
      mongoc_client_pool_set_apm_callbacks (pool, callbacks, &ctx);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (uri, NULL);

      if (!expect_error) {
         BSON_ASSERT (client);
      }

      if (!client) {
         /* expected failure, e.g. SRV lookup or URI finalization failed */
         goto cleanup;
      }

      BSON_ASSERT (mongoc_uri_get_tls (client->uri) == expect_ssl);
#ifdef MONGOC_ENABLE_SSL
      mongoc_client_set_ssl_opts (client, &ssl_opts);
#else
      test_framework_set_ssl_opts (client);
#endif
      mongoc_client_set_apm_callbacks (client, callbacks, &ctx);
   }

#ifdef MONGOC_ENABLE_SSL
   BSON_ASSERT (client->ssl_opts.allow_invalid_hostname);
#endif

   const int64_t n_hosts = hosts_count (test);

   if (pooled) {
      if (n_hosts > 0 && !expect_error) {
         WAIT_UNTIL (_host_list_matches (test, &ctx));
      } else {
         r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
         BSON_ASSERT (!r);
         ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "");
      }
   } else if (NULL == mongoc_uri_get_username (uri)) {
      /* Skip single-threaded tests containing auth credentials. Monitoring
       * connections need to authenticate, and the credentials in the tests do
       * not correspond to the test users. TODO (CDRIVER-4046): unskip these
       * tests. */
      if (n_hosts > 0 && !expect_error) {
         r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
         ASSERT_OR_PRINT (r, error);
         WAIT_UNTIL (_host_list_matches (test, &ctx));
      } else {
         r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
         BSON_ASSERT (!r);
         ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "");
      }
   }

   /* the client's URI is updated after initial seedlist discovery (though for
    * background SRV polling, only the topology's URI is updated). Check that
    * both the topology and client URI have the expected options. */
   _assert_options_match (test, client->uri);
   _assert_options_match (test, client->topology->uri);

   /* the client has a copy of the topology's URI, assert they're the same */
   ASSERT (bson_equal (mongoc_uri_get_options (client->uri), mongoc_uri_get_options (client->topology->uri)));
   ASSERT (bson_equal (mongoc_uri_get_credentials (client->uri), mongoc_uri_get_credentials (client->topology->uri)));
   if (!mongoc_uri_get_hosts (client->uri)) {
      ASSERT (!mongoc_uri_get_hosts (client->topology->uri));
   } else {
      _mongoc_host_list_compare_one (mongoc_uri_get_hosts (client->uri), mongoc_uri_get_hosts (client->topology->uri));
   }

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

cleanup:
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_uri_destroy (uri);
}


static void
test_dns (bson_t *test)
{
   _test_dns_maybe_pooled (test, false);
   _test_dns_maybe_pooled (test, true);
}


static int
test_dns_check_replset (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_DNS") ? 1 : 0;
}

static int
test_dns_check_loadbalanced (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_DNS_LOADBALANCED") ? 1 : 0;
}

static int
test_dns_check_srv_polling (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_DNS_SRV_POLLING") ? 1 : 0;
}


/*
 *-----------------------------------------------------------------------
 *
 * Runner for the JSON tests for mongodb+srv URIs.
 *
 *-----------------------------------------------------------------------
 */
static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "initial_dns_seedlist_discovery/replica-set",
                                       test_dns,
                                       test_dns_check_replset,
                                       test_framework_skip_if_no_crypto);

   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "initial_dns_seedlist_discovery/load-balanced",
                                       test_dns,
                                       test_dns_check_loadbalanced,
                                       test_framework_skip_if_no_crypto);

   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "initial_dns_seedlist_discovery/sharded",
                                       test_dns,
                                       /* Topology of load-balancer tests satisfy topology requirements of
                                        * sharded tests, even though a load balancer is not required. */
                                       test_dns_check_loadbalanced,
                                       test_framework_skip_if_no_crypto);
}

extern bool
mongoc_topology_apply_scanned_srv_hosts (mongoc_uri_t *uri,
                                         mongoc_topology_description_t *td,
                                         mongoc_host_list_t *hosts,
                                         bson_error_t *error);

static mongoc_host_list_t *
make_hosts (char *first_host, ...)
{
   va_list va;
   mongoc_host_list_t *hosts = NULL;
   mongoc_host_list_t host;
   char *host_str;

   _mongoc_host_list_from_string (&host, first_host);
   _mongoc_host_list_upsert (&hosts, &host);

   va_start (va, first_host);
   while ((host_str = va_arg (va, char *))) {
      _mongoc_host_list_from_string (&host, host_str);
      _mongoc_host_list_upsert (&hosts, &host);
   }
   va_end (va);
   return hosts;
}

#define MAKE_HOSTS(...) make_hosts (__VA_ARGS__, NULL)

static void
dump_hosts (mongoc_host_list_t *hosts)
{
   mongoc_host_list_t *host;

   MONGOC_DEBUG ("hosts:");

   LL_FOREACH (hosts, host)
   {
      MONGOC_DEBUG ("- %s", host->host_and_port);
   }
}

static void
dump_topology_description (const mongoc_topology_description_t *td)
{
   const mongoc_server_description_t *sd;
   const mongoc_set_t *servers = mc_tpld_servers_const (td);

   MONGOC_DEBUG ("topology hosts:");
   for (size_t i = 0u; i < servers->items_len; ++i) {
      sd = mongoc_set_get_item_const (servers, i);
      MONGOC_DEBUG ("- %s", sd->host.host_and_port);
   }
}

static void
check_topology_description (mongoc_topology_description_t *td, mongoc_host_list_t *hosts)
{
   size_t nhosts = 0u;
   mongoc_host_list_t *host;
   const mongoc_set_t *servers = mc_tpld_servers_const (td);

   for (host = hosts; host; host = host->next) {
      ++nhosts;

      /* Check that "host" is already in the topology description by upserting
       * it, and ensuring that the number of servers remains constant. */
      const size_t server_count = servers->items_len;
      BSON_ASSERT (mongoc_topology_description_add_server (td, host->host_and_port, NULL));

      if (server_count != servers->items_len) {
         dump_topology_description (td);
         dump_hosts (hosts);
         test_error ("topology description did not have host: %s", host->host_and_port);
      }
   }

   if (nhosts != servers->items_len) {
      dump_topology_description (td);
      dump_hosts (hosts);
      test_error ("topology description had extra hosts");
   }
}

static void
test_srv_polling_mocked (void *unused)
{
   mongoc_uri_t *uri;
   mongoc_topology_description_t td;
   bson_error_t error;
   mongoc_host_list_t *hosts;
   mongoc_host_list_t *expected;
   bool ret;

   BSON_UNUSED (unused);

   mongoc_topology_description_init (&td, 0);
   uri = mongoc_uri_new ("mongodb+srv://server.test.com/?tls=true");
   capture_logs (true);

   hosts = MAKE_HOSTS ("a.test.com", "b.test.com");
   expected = MAKE_HOSTS ("a.test.com", "b.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, hosts, &error);
   ASSERT_OR_PRINT (ret, error);
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   _mongoc_host_list_destroy_all (hosts);
   ASSERT_NO_CAPTURED_LOGS ("topology");

   /* Add an extra host. */
   hosts = MAKE_HOSTS ("x.test.com", "a.test.com", "y.test.com", "b.test.com");
   expected = MAKE_HOSTS ("x.test.com", "a.test.com", "y.test.com", "b.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, hosts, &error);
   ASSERT_OR_PRINT (ret, error);
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   _mongoc_host_list_destroy_all (hosts);
   ASSERT_NO_CAPTURED_LOGS ("topology");

   /* Remove all but one host. */
   hosts = MAKE_HOSTS ("x.test.com");
   expected = MAKE_HOSTS ("x.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, hosts, &error);
   ASSERT_OR_PRINT (ret, error);
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   _mongoc_host_list_destroy_all (hosts);
   ASSERT_NO_CAPTURED_LOGS ("topology");

   /* Add one valid and one invalid. Invalid should skip, warning should be
    * logged. */
   hosts = MAKE_HOSTS ("x.test.com", "y.test.com", "bad.wrongdomain.com");
   expected = MAKE_HOSTS ("x.test.com", "y.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, hosts, &error);
   ASSERT_OR_PRINT (ret, error);
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   _mongoc_host_list_destroy_all (hosts);
   ASSERT_CAPTURED_LOG ("topology", MONGOC_LOG_LEVEL_ERROR, "Invalid host");

   /* An empty host list returns false but does NOT change topology description
    */
   expected = MAKE_HOSTS ("x.test.com", "y.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, NULL, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "SRV response did not contain any valid hosts");
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   ASSERT_CAPTURED_LOG ("topology", MONGOC_LOG_LEVEL_ERROR, "Invalid host");

   /* All invalid hosts returns false but does NOT change topology description
    */
   hosts = MAKE_HOSTS ("bad1.wrongdomain.com", "bad2.wrongdomain.com");
   expected = MAKE_HOSTS ("x.test.com", "y.test.com");
   ret = mongoc_topology_apply_scanned_srv_hosts (uri, &td, NULL, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "SRV response did not contain any valid hosts");
   check_topology_description (&td, expected);
   _mongoc_host_list_destroy_all (expected);
   _mongoc_host_list_destroy_all (hosts);
   ASSERT_CAPTURED_LOG ("topology", MONGOC_LOG_LEVEL_ERROR, "Invalid host");

   mongoc_topology_description_cleanup (&td);
   mongoc_uri_destroy (uri);
}

static void
test_small_initial_buffer (void *unused)
{
   mongoc_rr_type_t rr_type = MONGOC_RR_SRV;
   mongoc_rr_data_t rr_data;
   bson_error_t error;
   /* Size needs to be large enough to fit DNS answer header to not error, but
    * smaller than SRV response to test. The SRV response is 155 bytes. This can
    * be determined with: dig -t SRV _mongodb._tcp.test1.test.build.10gen.cc */
   size_t small_buffer_size = 30;

   BSON_UNUSED (unused);

   memset (&rr_data, 0, sizeof (rr_data));
   ASSERT_OR_PRINT (_mongoc_client_get_rr (
                       "_mongodb._tcp.test1.test.build.10gen.cc", rr_type, &rr_data, small_buffer_size, false, &error),
                    error);
   ASSERT_CMPINT (rr_data.count, ==, 2);
   bson_free (rr_data.txt_record_opts);
   _mongoc_host_list_destroy_all (rr_data.hosts);
}

bool
_mock_rr_resolver_prose_test_9 (const char *service,
                                mongoc_rr_type_t rr_type,
                                mongoc_rr_data_t *rr_data,
                                size_t initial_buffer_size,
                                bool prefer_tcp,
                                bson_error_t *error)
{
   BSON_UNUSED (service);
   BSON_UNUSED (rr_type);
   BSON_UNUSED (rr_data);
   BSON_UNUSED (initial_buffer_size);
   BSON_UNUSED (prefer_tcp);
   BSON_UNUSED (error);

   test_error ("Expected mock resolver to not be called");
   return true;
}

static void
_prose_test_ping (mongoc_client_t *client)
{
   bson_error_t error;
   bson_t *cmd = BCON_NEW ("ping", BCON_INT32 (1));

   ASSERT (client);

   if (!mongoc_client_command_simple (client, "admin", cmd, NULL, NULL, &error)) {
      test_error ("ping failed: %s", error.message);
   }

   bson_destroy (cmd);
}

/* SRV Polling Tests Spec: rescanSRVIntervalMS */
#define RESCAN_INTERVAL_MS 500

static void *
_prose_test_init_resource_single (const mongoc_uri_t *uri, _mongoc_rr_resolver_fn fn)
{
   mongoc_client_t *client;
   mongoc_topology_t *topology;

   BSON_ASSERT_PARAM (uri);
   BSON_ASSERT_PARAM (fn);

   client = mongoc_client_new_from_uri (uri);
   topology = client->topology;

   _mongoc_topology_set_rr_resolver (topology, fn);
   _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, RESCAN_INTERVAL_MS);

#if defined(MONGOC_ENABLE_SSL)
   {
      mongoc_ssl_opt_t ssl_opts = *test_framework_get_ssl_opts ();
      ssl_opts.allow_invalid_hostname = true;
      mongoc_client_set_ssl_opts (client, &ssl_opts);
   }
#endif /* defined(MONGOC_ENABLE_SSL) */

   return client;
}

static void *
_prose_test_init_resource_pooled (const mongoc_uri_t *uri, _mongoc_rr_resolver_fn fn)
{
   mongoc_client_pool_t *pool;
   mongoc_topology_t *topology;

   BSON_ASSERT_PARAM (uri);
   BSON_ASSERT_PARAM (fn);

   pool = mongoc_client_pool_new (uri);
   topology = _mongoc_client_pool_get_topology (pool);

   _mongoc_topology_set_rr_resolver (topology, fn);
   _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, RESCAN_INTERVAL_MS);

#if defined(MONGOC_ENABLE_SSL)
   {
      mongoc_ssl_opt_t ssl_opts = *test_framework_get_ssl_opts ();
      ssl_opts.allow_invalid_hostname = true;
      mongoc_client_pool_set_ssl_opts (pool, &ssl_opts);
   }
#endif /* defined(MONGOC_ENABLE_SSL) */

   return pool;
}

static void
_prose_test_free_resource_single (void *resource)
{
   mongoc_client_destroy ((mongoc_client_t *) resource);
}

static void
_prose_test_free_resource_pooled (void *resource)
{
   mongoc_client_pool_destroy ((mongoc_client_pool_t *) resource);
}

static mongoc_client_t *
_prose_test_get_client_single (void *resource)
{
   BSON_ASSERT_PARAM (resource);
   return (mongoc_client_t *) resource;
}

static mongoc_client_t *
_prose_test_get_client_pooled (void *resource)
{
   BSON_ASSERT_PARAM (resource);
   return mongoc_client_pool_pop (((mongoc_client_pool_t *) resource));
}

static void
_prose_test_release_client_single (void *resource, mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (resource);
   BSON_ASSERT_PARAM (client);
   /* Nothing to do. */
}

static void
_prose_test_release_client_pooled (void *resource, mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (resource);
   BSON_ASSERT_PARAM (client);
   mongoc_client_pool_push ((mongoc_client_pool_t *) resource, client);
}

static void
_prose_test_update_srv_single (void *resource)
{
   mongoc_client_t *client;

   BSON_ASSERT_PARAM (resource);

   client = resource;

   _mongoc_usleep (2000 * RESCAN_INTERVAL_MS);

   /* Avoid ping given `loadBalanced=true`; see prose test 9. */
   if (!mongoc_uri_get_option_as_bool (client->uri, MONGOC_URI_LOADBALANCED, false)) {
      _prose_test_ping (client);
   }
}

static void
_prose_test_update_srv_pooled (void *resource)
{
   BSON_ASSERT_PARAM (resource);

   _mongoc_usleep (2000 * RESCAN_INTERVAL_MS);
}

typedef struct {
   void *(*init_resource) (const mongoc_uri_t *uri, _mongoc_rr_resolver_fn fn);
   void (*free_resource) (void *);
   mongoc_client_t *(*get_client) (void *resource);
   void (*release_client) (void *resource, mongoc_client_t *client);
   void (*update_srv) (void *);
} _prose_test_fns_t;

static const _prose_test_fns_t _prose_test_single_fns = {_prose_test_init_resource_single,
                                                         _prose_test_free_resource_single,
                                                         _prose_test_get_client_single,
                                                         _prose_test_release_client_single,
                                                         _prose_test_update_srv_single};

static const _prose_test_fns_t _prose_test_pooled_fns = {_prose_test_init_resource_pooled,
                                                         _prose_test_free_resource_pooled,
                                                         _prose_test_get_client_pooled,
                                                         _prose_test_release_client_pooled,
                                                         _prose_test_update_srv_pooled};

static void
_prose_test_9 (const _prose_test_fns_t *fns)
{
   void *resource;

   BSON_ASSERT_PARAM (fns);

   {
      mongoc_uri_t *const uri = mongoc_uri_new ("mongodb+srv://test3.test.build.10gen.cc");

      mongoc_uri_set_option_as_bool (uri, MONGOC_URI_LOADBALANCED, true);
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, RESCAN_INTERVAL_MS);

      resource = fns->init_resource (uri, _mock_rr_resolver_prose_test_9);

      mongoc_uri_destroy (uri);
   }

   {
      mongoc_host_list_t *const expected = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->update_srv (resource);

   {
      mongoc_host_list_t *const expected = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->free_resource (resource);
}

static void
prose_test_9_single (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_9 (&_prose_test_single_fns);
}

static void
prose_test_9_pooled (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_9 (&_prose_test_pooled_fns);
}

static bool
_mock_rr_resolver_prose_test_10 (const char *service,
                                 mongoc_rr_type_t rr_type,
                                 mongoc_rr_data_t *rr_data,
                                 size_t initial_buffer_size,
                                 bool prefer_tcp,
                                 bson_error_t *error)
{
   BSON_UNUSED (initial_buffer_size);

   BSON_ASSERT_PARAM (service);
   BSON_ASSERT_PARAM (rr_data);
   BSON_UNUSED (prefer_tcp);
   BSON_ASSERT_PARAM (error);

   if (rr_type == MONGOC_RR_SRV) {
      const size_t count = _mongoc_host_list_length (rr_data->hosts);
      BSON_ASSERT (bson_in_range_unsigned (uint32_t, count));

      rr_data->hosts = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017",
                                   "localhost.test.build.10gen.cc:27019",
                                   "localhost.test.build.10gen.cc:27020");
      rr_data->count = (uint32_t) count;
      rr_data->min_ttl = 0u;
      rr_data->txt_record_opts = NULL;
   }

   error->code = 0u;

   return true;
}

static void
_prose_test_10 (const _prose_test_fns_t *fns)
{
   void *resource;

   BSON_ASSERT_PARAM (fns);

   {
      mongoc_uri_t *const uri = mongoc_uri_new ("mongodb+srv://test1.test.build.10gen.cc");

      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SRVMAXHOSTS, 0);
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, RESCAN_INTERVAL_MS);

      resource = fns->init_resource (uri, _mock_rr_resolver_prose_test_10);

      mongoc_uri_destroy (uri);
   }

   {
      mongoc_host_list_t *const expected =
         MAKE_HOSTS ("localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->update_srv (resource);

   {
      mongoc_host_list_t *const expected = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017",
                                                       "localhost.test.build.10gen.cc:27019",
                                                       "localhost.test.build.10gen.cc:27020");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->free_resource (resource);
}

static void
prose_test_10_single (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_10 (&_prose_test_single_fns);
}

static void
prose_test_10_pooled (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_10 (&_prose_test_pooled_fns);
}

static bool
_mock_rr_resolver_prose_test_11 (const char *service,
                                 mongoc_rr_type_t rr_type,
                                 mongoc_rr_data_t *rr_data,
                                 size_t initial_buffer_size,
                                 bool prefer_tcp,
                                 bson_error_t *error)
{
   BSON_UNUSED (initial_buffer_size);

   BSON_ASSERT_PARAM (service);
   BSON_ASSERT_PARAM (rr_data);
   BSON_UNUSED (prefer_tcp);
   BSON_ASSERT_PARAM (error);

   if (rr_type == MONGOC_RR_SRV) {
      const size_t count = _mongoc_host_list_length (rr_data->hosts);
      BSON_ASSERT (bson_in_range_unsigned (uint32_t, count));

      rr_data->hosts = MAKE_HOSTS ("localhost.test.build.10gen.cc:27019", "localhost.test.build.10gen.cc:27020");
      rr_data->count = (uint32_t) count;
      rr_data->min_ttl = 0u;
      rr_data->txt_record_opts = NULL;
   }

   error->code = 0u;

   return true;
}

static void
_prose_test_11 (const _prose_test_fns_t *fns)
{
   void *resource;

   BSON_ASSERT_PARAM (fns);

   {
      mongoc_uri_t *const uri = mongoc_uri_new ("mongodb+srv://test1.test.build.10gen.cc");

      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SRVMAXHOSTS, 2);
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, RESCAN_INTERVAL_MS);

      resource = fns->init_resource (uri, _mock_rr_resolver_prose_test_11);

      mongoc_uri_destroy (uri);
   }

   {
      mongoc_host_list_t *const expected =
         MAKE_HOSTS ("localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->update_srv (resource);

   {
      mongoc_host_list_t *const expected =
         MAKE_HOSTS ("localhost.test.build.10gen.cc:27019", "localhost.test.build.10gen.cc:27020");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->free_resource (resource);
}

static void
prose_test_11_single (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_11 (&_prose_test_single_fns);
}

static void
prose_test_11_pooled (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_11 (&_prose_test_pooled_fns);
}

static bool
_mock_rr_resolver_prose_test_12 (const char *service,
                                 mongoc_rr_type_t rr_type,
                                 mongoc_rr_data_t *rr_data,
                                 size_t initial_buffer_size,
                                 bool prefer_tcp,
                                 bson_error_t *error)
{
   BSON_UNUSED (initial_buffer_size);

   BSON_ASSERT_PARAM (service);
   BSON_ASSERT_PARAM (rr_data);
   BSON_UNUSED (prefer_tcp);
   BSON_ASSERT_PARAM (error);

   if (rr_type == MONGOC_RR_SRV) {
      const size_t count = _mongoc_host_list_length (rr_data->hosts);
      BSON_ASSERT (bson_in_range_unsigned (uint32_t, count));

      rr_data->hosts = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017",
                                   "localhost.test.build.10gen.cc:27019",
                                   "localhost.test.build.10gen.cc:27020");
      rr_data->count = (uint32_t) count;
      rr_data->min_ttl = 0u;
      rr_data->txt_record_opts = NULL;
   }

   error->code = 0u;

   return true;
}

typedef struct {
   size_t num_existing;
   size_t num_new_valid;
} _prose_test_12_ctx_t;

static bool
_prose_test_12_cb (const void *sd_void, void *ctx_void)
{
   const mongoc_server_description_t *sd;
   _prose_test_12_ctx_t *ctx;
   const mongoc_host_list_t *host;

   BSON_ASSERT_PARAM (sd_void);
   BSON_ASSERT_PARAM (ctx_void);

   sd = sd_void;
   ctx = ctx_void;
   host = &sd->host;

   ASSERT_CMPSTR (host->host, "localhost.test.build.10gen.cc");

   if (host->port == 27017u) {
      ++ctx->num_existing;
   }

   else {
      ASSERT (host->port == 27019 || host->port == 27020);
      ++ctx->num_new_valid;
   }

   return true;
}

static void
_prose_test_12 (const _prose_test_fns_t *fns)
{
   void *resource;

   BSON_ASSERT_PARAM (fns);

   {
      mongoc_uri_t *const uri = mongoc_uri_new ("mongodb+srv://test1.test.build.10gen.cc");

      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SRVMAXHOSTS, 2);
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, RESCAN_INTERVAL_MS);

      resource = fns->init_resource (uri, _mock_rr_resolver_prose_test_12);

      mongoc_uri_destroy (uri);
   }

   {
      mongoc_host_list_t *const expected =
         MAKE_HOSTS ("localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018");
      mongoc_client_t *const client = fns->get_client (resource);

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         check_topology_description (tdmod.new_td, expected);
         mc_tpld_modify_drop (tdmod);
      }

      fns->release_client (resource, client);
      _mongoc_host_list_destroy_all (expected);
   }

   fns->update_srv (resource);

   {
      mongoc_client_t *const client = fns->get_client (resource);
      _prose_test_12_ctx_t ctx;

      ctx.num_existing = 0u;
      ctx.num_new_valid = 0u;

      {
         mc_tpld_modification tdmod = mc_tpld_modify_begin (client->topology);
         const mongoc_set_t *servers = mc_tpld_servers_const (tdmod.new_td);
         mongoc_set_for_each_const (servers, _prose_test_12_cb, &ctx);
         mc_tpld_modify_drop (tdmod);
      }

      ASSERT_WITH_MSG (ctx.num_existing > 0u, "hosts that have not changed must be left alone and unchanged");
      ASSERT_WITH_MSG (
         ctx.num_existing == 1u, "only a single host should have remained, but found %zu", ctx.num_existing);

      ASSERT_WITH_MSG (ctx.num_new_valid == 1u, "exactly one valid new hosts should have been added");

      fns->release_client (resource, client);
   }

   fns->free_resource (resource);
}

static void
prose_test_12_single (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_12 (&_prose_test_single_fns);
}

static void
prose_test_12_pooled (void *unused)
{
   BSON_UNUSED (unused);

   _prose_test_12 (&_prose_test_pooled_fns);
}

typedef struct {
   bson_mutex_t lock;
   mongoc_host_list_t *hosts;
} rr_override_t;

rr_override_t rr_override;

// `_mock_rr_resolver_with_override` allows setting a custom list of hosts with the global override.
static bool
_mock_rr_resolver_with_override (const char *service,
                                 mongoc_rr_type_t rr_type,
                                 mongoc_rr_data_t *rr_data,
                                 size_t initial_buffer_size,
                                 bool prefer_tcp,
                                 bson_error_t *error)
{
   BSON_UNUSED (initial_buffer_size);

   BSON_ASSERT_PARAM (service);
   BSON_ASSERT_PARAM (rr_data);
   BSON_UNUSED (prefer_tcp);
   BSON_ASSERT_PARAM (error);

   if (rr_type == MONGOC_RR_SRV) {
      bson_mutex_lock (&rr_override.lock);
      const size_t count = _mongoc_host_list_length (rr_override.hosts);
      BSON_ASSERT (bson_in_range_unsigned (uint32_t, count));
      rr_data->hosts = _mongoc_host_list_copy_all (rr_override.hosts);
      rr_data->count = (uint32_t) count;
      rr_data->txt_record_opts = NULL;
      bson_mutex_unlock (&rr_override.lock);
   }

   error->code = 0u;

   return true;
}

// Test that after a server is removed from SRV records, all connections are closed.
static void
test_removing_servers_closes_connections (void *unused)
{
   BSON_UNUSED (unused);
   bson_error_t error;
   bool ok;
   bson_t *ping = BCON_NEW ("ping", BCON_INT32 (1));

   // Create a client pool to mongodb+srv://test1.test.build.10gen.cc. The URI resolves to two SRV records:
   // - localhost.test.build.10gen.cc:27017
   // - localhost.test.build.10gen.cc:27018
   mongoc_client_pool_t *pool;
   {
      mongoc_uri_t *uri = mongoc_uri_new ("mongodb+srv://test1.test.build.10gen.cc");
      // Set a short heartbeat so server monitors get quick responses.
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, RESCAN_INTERVAL_MS);
      pool = mongoc_client_pool_new (uri);
#if defined(MONGOC_ENABLE_SSL)
      mongoc_ssl_opt_t ssl_opts = *test_framework_get_ssl_opts ();
      ssl_opts.allow_invalid_hostname = true;
      mongoc_client_pool_set_ssl_opts (pool, &ssl_opts);
#endif /* defined(MONGOC_ENABLE_SSL) */
      // Override the SRV polling callback:
      mongoc_topology_t *topology = _mongoc_client_pool_get_topology (pool);
      bson_mutex_init (&rr_override.lock);
      rr_override.hosts = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018");
      _mongoc_topology_set_rr_resolver (topology, _mock_rr_resolver_with_override);
      // Set a shorter SRV rescan interval.
      _mongoc_topology_set_srv_polling_rescan_interval_ms (topology, RESCAN_INTERVAL_MS);
      mongoc_uri_destroy (uri);
   }

   // Count connections to both servers.
   int32_t conns_27017_before = get_current_connection_count ("localhost:27017");
   int32_t conns_27018_before = get_current_connection_count ("localhost:27018");

   // Pop (and push) a client to start background monitoring.
   {
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      mongoc_client_pool_push (pool, client);
      // Wait for monitoring connections to be created.
      // Expect two monitoring connections per server to be created in background.
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27017", conns_27017_before + 2);
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27018", conns_27018_before + 2);
   }

   // Send 'ping' commands on a client to each server to create operation connections.
   {
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      ok = mongoc_client_command_simple_with_server_id (client, "admin", ping, NULL, 1 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);
      ok = mongoc_client_command_simple_with_server_id (client, "admin", ping, NULL, 2 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);
      mongoc_client_pool_push (pool, client);
      // Expect an operation connection is created.
      ASSERT_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 1);
      ASSERT_CONN_COUNT ("localhost:27018", conns_27018_before + 2 + 1);
   }

   // Mock removal of localhost:27018.
   {
      bson_mutex_lock (&rr_override.lock);
      _mongoc_host_list_destroy_all (rr_override.hosts);
      rr_override.hosts = MAKE_HOSTS ("localhost.test.build.10gen.cc:27017");
      bson_mutex_unlock (&rr_override.lock);
   }

   // Expect connections are closed to removed server.
   {
      // Expect monitoring connections to be closed in background.
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 1);
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27018", conns_27018_before + 1);

      // Pop and push the client to "prune" the stale operation connections.
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      mongoc_client_pool_push (pool, client);
      ASSERT_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 1);
      ASSERT_CONN_COUNT ("localhost:27018", conns_27018_before);
   }

   mongoc_client_pool_destroy (pool);
   bson_mutex_destroy (&rr_override.lock);
   bson_destroy (ping);
}

void
test_dns_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_AddFull (
      suite, "/initial_dns_seedlist_discovery/srv_polling/mocked", test_srv_polling_mocked, NULL, NULL, NULL);
   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/small_initial_buffer",
                      test_small_initial_buffer,
                      NULL,
                      NULL,
                      test_dns_check_replset);

   /* TODO (CDRIVER-4045): remove /initial_dns_seedlist_discovery from the path
    * of the SRV polling tests, since they are defined in the "Polling SRV
    * Records for mongos Discovery" spec, not the "Initial DNS Seedlist
    * Discovery" spec. */
   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_9/single",
                      prose_test_9_single,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_9/pooled",
                      prose_test_9_pooled,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_10/single",
                      prose_test_10_single,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_10/pooled",
                      prose_test_10_pooled,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_11/single",
                      prose_test_11_single,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_11/pooled",
                      prose_test_11_pooled,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_12/single",
                      prose_test_12_single,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (suite,
                      "/initial_dns_seedlist_discovery/srv_polling/prose_test_12/pooled",
                      prose_test_12_pooled,
                      NULL,
                      NULL,
                      test_dns_check_srv_polling);

   TestSuite_AddFull (
      suite,
      "/initial_dns_seedlist_discovery/srv_polling/removing_servers_closes_connections",
      test_removing_servers_closes_connections,
      NULL,
      NULL,
      test_dns_check_srv_polling,
      test_framework_skip_if_max_wire_version_less_than_9 /* require server 4.4+ for streaming monitoring protocol */);
}
