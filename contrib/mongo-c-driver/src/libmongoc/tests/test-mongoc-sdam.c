#include <mongoc/mongoc.h>

#include <mongoc/mongoc-set-private.h>

#include "json-test.h"

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-topology-description-apm-private.h"
#include "test-libmongoc.h"

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif


static void
_topology_has_description (const mongoc_topology_description_t *topology, bson_t *server, const char *address)
{
   mongoc_server_description_t const *sd;
   bson_iter_t server_iter;
   const char *server_type;
   const char *set_name;

   sd = server_description_by_hostname (topology, address);
   BSON_ASSERT (sd);

   bson_iter_init (&server_iter, server);
   while (bson_iter_next (&server_iter)) {
      if (strcmp ("setName", bson_iter_key (&server_iter)) == 0) {
         set_name = bson_iter_utf8 (&server_iter, NULL);
         if (set_name) {
            BSON_ASSERT (sd->set_name);
            ASSERT_CMPSTR (sd->set_name, set_name);
         }
         /* TODO (CDRIVER-4057) this should assert that a null setName means the
         server description also has no setName. Uncomment this when
         CDRIVER-4057 is resolved:

         else if (sd->set_name) {
            test_error ("server: %s, expected NULL setName, got: %s", address,
         sd->set_name);
         }
         */
      } else if (strcmp ("type", bson_iter_key (&server_iter)) == 0) {
         server_type = bson_iter_utf8 (&server_iter, NULL);
         if (sd->type != server_type_from_test (server_type)) {
            test_error ("expected server type %s not %s", server_type, mongoc_server_description_type (sd));
         }
      } else if (strcmp ("setVersion", bson_iter_key (&server_iter)) == 0) {
         int64_t expected_set_version;
         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            expected_set_version = MONGOC_NO_SET_VERSION;
         } else {
            expected_set_version = bson_iter_as_int64 (&server_iter);
         }
         BSON_ASSERT (sd->set_version == expected_set_version);
      } else if (strcmp ("electionId", bson_iter_key (&server_iter)) == 0) {
         bson_oid_t expected_oid;
         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            bson_oid_init_from_string (&expected_oid, "000000000000000000000000");
         } else {
            ASSERT (BSON_ITER_HOLDS_OID (&server_iter));
            bson_oid_copy (bson_iter_oid (&server_iter), &expected_oid);
         }

         ASSERT_CMPOID (&sd->election_id, &expected_oid);
      } else if (strcmp ("topologyVersion", bson_iter_key (&server_iter)) == 0) {
         bson_t expected_topology_version;

         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            bson_init (&expected_topology_version);
         } else {
            ASSERT (BSON_ITER_HOLDS_DOCUMENT (&server_iter));
            bson_lookup_doc (server, "topologyVersion", &expected_topology_version);
         }

         assert_match_bson (&sd->topology_version, &expected_topology_version, false);
         bson_destroy (&expected_topology_version);
      } else if (strcmp ("pool", bson_iter_key (&server_iter)) == 0) {
         bson_iter_t iter;
         uint32_t expected_generation;

         BSON_ASSERT (bson_iter_recurse (&server_iter, &iter));
         BSON_ASSERT (bson_iter_find (&iter, "generation") && BSON_ITER_HOLDS_INT32 (&iter));
         expected_generation = bson_iter_int32 (&iter);
         ASSERT_CMPINT32 (expected_generation, ==, mc_tpl_sd_get_generation (sd, &kZeroServiceId));
      } else if (strcmp ("logicalSessionTimeoutMinutes", bson_iter_key (&server_iter)) == 0) {
         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            if (sd->session_timeout_minutes != MONGOC_NO_SESSIONS) {
               test_error ("ERROR: expected unset value for "
                           "logicalSessionTimeoutMinutes but got: %" PRId64,
                           sd->session_timeout_minutes);
            }
         } else {
            ASSERT_CMPINT64 (bson_iter_as_int64 (&server_iter), ==, sd->session_timeout_minutes);
         }
      } else if (strcmp ("minWireVersion", bson_iter_key (&server_iter)) == 0) {
         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            if (sd->min_wire_version != 0) {
               test_error ("ERROR: expected unset value for minWireVersion but "
                           "got: %" PRId32,
                           sd->min_wire_version);
            }
         } else {
            ASSERT_CMPINT32 (bson_iter_int32 (&server_iter), ==, sd->min_wire_version);
         }
      } else if (strcmp ("maxWireVersion", bson_iter_key (&server_iter)) == 0) {
         if (BSON_ITER_HOLDS_NULL (&server_iter)) {
            if (sd->max_wire_version != 0) {
               test_error ("ERROR: expected unset value for maxWireVersion but "
                           "got: %" PRId32,
                           sd->max_wire_version);
            }
         } else {
            ASSERT_CMPINT32 (bson_iter_int32 (&server_iter), ==, sd->max_wire_version);
         }
      } else {
         fprintf (stderr, "ERROR: unparsed field %s\n", bson_iter_key (&server_iter));
         BSON_ASSERT (0);
      }
   }
}

/*
 *-----------------------------------------------------------------------
 *
 * Run the JSON tests from the Server Discovery and Monitoring spec.
 *
 *-----------------------------------------------------------------------
 */
static void
test_sdam_cb (bson_t *test)
{
   mongoc_client_t *client;
   bson_t phase;
   bson_t phases;
   bson_t servers;
   bson_t server;
   bson_t outcome;
   bson_iter_t phase_iter;
   bson_iter_t phase_field_iter;
   bson_iter_t servers_iter;
   bson_iter_t outcome_iter;
   bson_iter_t iter;
   mc_tpld_modification tdmod;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;
   const char *set_name;
   const char *hostname;

   /* parse out the uri and use it to create a client */
   BSON_ASSERT (bson_iter_init_find (&iter, test, "uri"));
   client = test_framework_client_new (bson_iter_utf8 (&iter, NULL), NULL);

   /* for each phase, parse and validate */
   BSON_ASSERT (bson_iter_init_find (&iter, test, "phases"));
   bson_iter_bson (&iter, &phases);
   bson_iter_init (&phase_iter, &phases);

   /* LoadBalanced topologies change the server from Unknown to LoadBalancer
    * when SDAM monitoring begins. Force an opening, which would occur on the
    * first operation on the client. */
   tdmod = mc_tpld_modify_begin (client->topology);
   _mongoc_topology_description_monitor_opening (tdmod.new_td);
   mc_tpld_modify_commit (tdmod);

   while (bson_iter_next (&phase_iter)) {
      bson_iter_bson (&phase_iter, &phase);

      process_sdam_test_hello_responses (&phase, client->topology);

      /* parse out "outcome" and validate */
      BSON_ASSERT (bson_iter_init_find (&phase_field_iter, &phase, "outcome"));
      bson_iter_bson (&phase_field_iter, &outcome);
      bson_iter_init (&outcome_iter, &outcome);

      while (bson_iter_next (&outcome_iter)) {
         mc_tpld_renew_ref (&td, client->topology);
         if (strcmp ("servers", bson_iter_key (&outcome_iter)) == 0) {
            bson_iter_bson (&outcome_iter, &servers);
            ASSERT_CMPSIZE_T (bson_count_keys (&servers), ==, mc_tpld_servers_const (td.ptr)->items_len);

            bson_iter_init (&servers_iter, &servers);

            /* for each server, ensure topology has a matching entry */
            while (bson_iter_next (&servers_iter)) {
               hostname = bson_iter_key (&servers_iter);
               bson_iter_bson (&servers_iter, &server);

               _topology_has_description (td.ptr, &server, hostname);
            }
         } else if (strcmp ("setName", bson_iter_key (&outcome_iter)) == 0) {
            set_name = bson_iter_utf8 (&outcome_iter, NULL);
            if (set_name) {
               BSON_ASSERT (td.ptr->set_name);
               ASSERT_CMPSTR (td.ptr->set_name, set_name);
            } else {
               if (td.ptr->set_name) {
                  test_error ("expected NULL setName, got: %s", td.ptr->set_name);
               }
            }
         } else if (strcmp ("topologyType", bson_iter_key (&outcome_iter)) == 0) {
            ASSERT_CMPSTR (mongoc_topology_description_type (td.ptr), bson_iter_utf8 (&outcome_iter, NULL));
         } else if (strcmp ("logicalSessionTimeoutMinutes", bson_iter_key (&outcome_iter)) == 0) {
            if (BSON_ITER_HOLDS_NULL (&outcome_iter)) {
               ASSERT_CMPINT64 (td.ptr->session_timeout_minutes, ==, (int64_t) MONGOC_NO_SESSIONS);
            } else {
               ASSERT_CMPINT64 (td.ptr->session_timeout_minutes, ==, bson_iter_as_int64 (&outcome_iter));
            }
         } else if (strcmp ("compatible", bson_iter_key (&outcome_iter)) == 0) {
            if (bson_iter_as_bool (&outcome_iter)) {
               ASSERT_CMPINT (0, ==, td.ptr->compatibility_error.domain);
            } else {
               ASSERT_ERROR_CONTAINS (
                  td.ptr->compatibility_error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION, "");
            }
         } else if (strcmp ("maxSetVersion", bson_iter_key (&outcome_iter)) == 0) {
            if (BSON_ITER_HOLDS_NULL (&outcome_iter)) {
               if (td.ptr->max_set_version != MONGOC_NO_SET_VERSION) {
                  test_error ("ERROR: expected unset value for maxSetVersion "
                              "but got: %" PRId64,
                              td.ptr->max_set_version);
               }
            } else {
               ASSERT_CMPINT64 (bson_iter_as_int64 (&outcome_iter), ==, td.ptr->max_set_version);
            }
         } else if (strcmp ("maxElectionId", bson_iter_key (&outcome_iter)) == 0) {
            const bson_oid_t *expected_oid;
            bson_oid_t zeroed = {.bytes = {0}};

            expected_oid = bson_iter_oid (&outcome_iter);

            if (expected_oid == NULL) {
               expected_oid = &zeroed;
            }

            if (!bson_oid_equal (expected_oid, &td.ptr->max_election_id)) {
               char expected_oid_str[25];
               char actual_oid_str[25];

               bson_oid_to_string (expected_oid, expected_oid_str);
               bson_oid_to_string (&td.ptr->max_election_id, actual_oid_str);
               test_error ("ERROR: Expected topology description's "
                           "maxElectionId to be %s, but was %s",
                           expected_oid_str,
                           actual_oid_str);
            }
         } else if (strcmp ("logicalSessionTimeoutMinutes", bson_iter_key (&outcome_iter)) == 0) {
            if (BSON_ITER_HOLDS_NULL (&outcome_iter)) {
               if (td.ptr->session_timeout_minutes != MONGOC_NO_SESSIONS) {
                  test_error ("ERROR: expected unset value for "
                              "logicalSessionTimeoutMinutes but got: %" PRId64,
                              td.ptr->session_timeout_minutes);
               }
            } else {
               ASSERT_CMPINT64 (bson_iter_as_int64 (&outcome_iter), ==, td.ptr->session_timeout_minutes);
            }
         } else {
            fprintf (stderr, "ERROR: unparsed test field %s\n", bson_iter_key (&outcome_iter));
            BSON_ASSERT (false);
         }
      }
   }
   mc_tpld_drop_ref (&td);
   mongoc_client_destroy (client);
}

/* Initialize a test context to run one SDAM integration test file.
 *
 * Do not use json_test_ctx_init to initialize a context. It sends commands to
 * check for sessions support. That interferes with failpoints set on hello.
 */
static void
sdam_json_test_ctx_init (json_test_ctx_t *ctx, const json_test_config_t *config, mongoc_client_pool_t *pool)
{
   const char *db_name;
   const char *coll_name;

   ASSERT (pool);

   memset (ctx, 0, sizeof (*ctx));
   ctx->config = config;
   bson_init (&ctx->events);
   ctx->acknowledged = true;
   ctx->verbose = test_framework_getenv_bool ("MONGOC_TEST_MONITORING_VERBOSE");
   bson_init (&ctx->lsids[0]);
   bson_init (&ctx->lsids[1]);
   bson_mutex_init (&ctx->mutex);

   /* Pop a client, which starts topology scanning. */
   ctx->client = mongoc_client_pool_pop (pool);
   ctx->test_framework_uri = mongoc_uri_copy (ctx->client->uri);
   db_name = bson_lookup_utf8 (ctx->config->scenario, "database_name");
   coll_name = bson_lookup_utf8 (ctx->config->scenario, "collection_name");
   ctx->db = mongoc_client_get_database (ctx->client, db_name);
   ctx->collection = mongoc_database_get_collection (ctx->db, coll_name);
}

static void
sdam_json_test_ctx_cleanup (json_test_ctx_t *ctx)
{
   mongoc_collection_destroy (ctx->collection);
   mongoc_database_destroy (ctx->db);
   bson_destroy (&ctx->lsids[0]);
   bson_destroy (&ctx->lsids[1]);
   bson_destroy (&ctx->events);
   mongoc_uri_destroy (ctx->test_framework_uri);
   bson_destroy (ctx->sent_lsids[0]);
   bson_destroy (ctx->sent_lsids[1]);
   bson_mutex_destroy (&ctx->mutex);
}

static bool
sdam_integration_operation_cb (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bson_t reply;
   bool res;

   res = json_test_operation (ctx, test, operation, ctx->collection, NULL, &reply);

   bson_destroy (&reply);

   return res;
}

/* Try to get a completely clean slate by disabling failpoints on all servers.
 */
static void
deactivate_failpoints_on_all_servers (mongoc_client_t *client)
{
   uint32_t server_id;
   const mongoc_set_t *servers;
   bson_t cmd;
   bson_error_t error;
   mc_shared_tpld td;

   ASSERT (client);

   bson_init (&cmd);
   BCON_APPEND (&cmd, "configureFailPoint", "failCommand", "mode", "off");

   td = mc_tpld_take_ref (client->topology);
   servers = mc_tpld_servers_const (td.ptr);

   for (size_t i = 0u; i < servers->items_len; i++) {
      bool ret;

      server_id = servers->items[i].id;
      ret = mongoc_client_command_simple_with_server_id (
         client, "admin", &cmd, NULL /* read prefs */, server_id, NULL /* reply */, &error);
      if (!ret) {
         MONGOC_DEBUG ("error disabling failpoint: %s", error.message);
      }
   }

   mc_tpld_drop_ref (&td);
   bson_destroy (&cmd);
}

static void
run_one_integration_test (json_test_config_t *config, bson_t *test)
{
   json_test_ctx_t ctx;
   json_test_ctx_t thread_ctx[2];
   bson_error_t error;
   mongoc_client_pool_t *pool;
   mongoc_client_t *setup_client;
   const char *db_name;
   const char *coll_name;
   mongoc_uri_t *uri;

   MONGOC_DEBUG ("running test: %s", bson_lookup_utf8 (test, "description"));

   uri = test_framework_get_uri ();
   if (bson_has_field (test, "clientOptions")) {
      bson_t client_opts;

      bson_lookup_doc (test, "clientOptions", &client_opts);
      set_uri_opts_from_bson (uri, &client_opts);
   }


   db_name = bson_lookup_utf8 (config->scenario, "database_name");
   coll_name = bson_lookup_utf8 (config->scenario, "collection_name");

   /* SDAM integration tests require streamable hello support, which is only
    * available for a client pool. */
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   mongoc_client_pool_set_error_api (pool, MONGOC_ERROR_API_VERSION_2);
   test_framework_set_pool_ssl_opts (pool);

   setup_client = test_framework_new_default_client ();
   /* Disable failpoints that may have been enabled in a previous test run. */
   deactivate_failpoints_on_all_servers (setup_client);
   mongoc_client_command_simple (setup_client, "admin", tmp_bson ("{'killAllSessions': []}"), NULL, NULL, &error);

   insert_data (db_name, coll_name, config->scenario);

   if (bson_has_field (test, "failPoint")) {
      activate_fail_point (setup_client, 0, test, "failPoint");
   }

   /* Listen for events before topology scanning starts. Some tests
    * check the result of the first hello command. But popping a client
    * starts topology scanning. */
   set_apm_callbacks_pooled (&ctx, pool);

   sdam_json_test_ctx_init (&ctx, config, pool);

   /* Set up test contexts for worker threads, which may be used by tests that
    * have "startThread" operations. These get the same APM event callbacks,
    * which are protected with a mutex. */
   sdam_json_test_ctx_init (&thread_ctx[0], config, pool);
   sdam_json_test_ctx_init (&thread_ctx[1], config, pool);
   ctx.worker_threads[0] = worker_thread_new (&thread_ctx[0]);
   ctx.worker_threads[1] = worker_thread_new (&thread_ctx[1]);

   json_test_operations (&ctx, test);

   if (bson_has_field (test, "expectations")) {
      bson_t expectations;

      bson_lookup_doc (test, "expectations", &expectations);
      check_json_apm_events (&ctx, &expectations);
   }

   if (bson_has_field (test, "outcome.collection")) {
      mongoc_collection_t *outcome_coll;
      outcome_coll = mongoc_client_get_collection (
         setup_client, mongoc_database_get_name (ctx.db), mongoc_collection_get_name (ctx.collection));
      check_outcome_collection (outcome_coll, test);
      mongoc_collection_destroy (outcome_coll);
   }

   deactivate_failpoints_on_all_servers (setup_client);
   worker_thread_destroy (ctx.worker_threads[0]);
   worker_thread_destroy (ctx.worker_threads[1]);
   mongoc_client_pool_push (pool, ctx.client);
   mongoc_client_pool_push (pool, thread_ctx[0].client);
   mongoc_client_pool_push (pool, thread_ctx[1].client);

   /* Capture occasionally emitted "Couldn't end \"endSessions\"" messages. */
   capture_logs (true);
   mongoc_client_pool_destroy (pool);
   mongoc_client_destroy (setup_client);
   capture_logs (false);

   sdam_json_test_ctx_cleanup (&ctx);
   sdam_json_test_ctx_cleanup (&thread_ctx[0]);
   sdam_json_test_ctx_cleanup (&thread_ctx[1]);
   mongoc_uri_destroy (uri);
}

static void
test_sdam_integration_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;
   bson_iter_t tests_iter;

   config.run_operation_cb = sdam_integration_operation_cb;
   config.scenario = scenario;
   config.command_started_events_only = true;

   if (!check_scenario_version (scenario)) {
      return;
   }

   ASSERT (bson_iter_init_find (&tests_iter, scenario, "tests"));
   ASSERT (bson_iter_recurse (&tests_iter, &tests_iter));

   while (bson_iter_next (&tests_iter)) {
      bson_t test;

      ASSERT (BSON_ITER_HOLDS_DOCUMENT (&tests_iter));
      bson_iter_bson (&tests_iter, &test);
      run_one_integration_test (&config, &test);
   }
}

/*
 *-----------------------------------------------------------------------
 *
 * Runner for the JSON tests for server discovery and monitoring..
 *
 *-----------------------------------------------------------------------
 */
static void
test_all_spec_tests (TestSuite *suite)
{
   /* Single */
   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/single", &test_sdam_cb);

   /* Replica set */
   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/rs", &test_sdam_cb);

   /* Sharded */
   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/sharded", &test_sdam_cb);

   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/errors", &test_sdam_cb);

   /* Tests not in official Server Discovery And Monitoring Spec */
   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/supplemental", &test_sdam_cb);

   /* Integration tests. */
   /* The integration tests configure retryable writes, which requires crypto.
    */
   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "server_discovery_and_monitoring/integration",
                                       &test_sdam_integration_cb,
                                       TestSuite_CheckLive,
                                       test_framework_skip_if_no_crypto,
                                       test_framework_skip_if_slow);

   install_json_test_suite (suite, JSON_DIR, "server_discovery_and_monitoring/load-balanced", &test_sdam_cb);
}

static void
test_topology_discovery (void *ctx)
{
   char *host_and_port;
   char *replset_name;
   char *uri_str;
   char *uri_str_auth;
   mongoc_client_t *client;
   mongoc_read_prefs_t *prefs;
   mongoc_server_description_t *sd_secondary;
   mongoc_host_list_t *hl_secondary;
   mongoc_collection_t *collection;
   bson_t doc = BSON_INITIALIZER;
   bson_t reply;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   host_and_port = test_framework_get_host_and_port ();
   replset_name = test_framework_replset_name ();
   uri_str = test_framework_get_uri_str ();

   client = test_framework_client_new (uri_str, NULL);
   test_framework_set_ssl_opts (client);
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   sd_secondary = mongoc_client_select_server (client,
                                               false, /* for reads */
                                               prefs,
                                               &error);
   ASSERT_OR_PRINT (sd_secondary, error);
   hl_secondary = mongoc_server_description_host (sd_secondary);

   /* Scenario: given a replica set deployment with a secondary, where HOST is
    * the address of the secondary, create a MongoClient using
    * ``mongodb://HOST/?directConnection=false`` as the URI.
    * Attempt a write to a collection.
    *
    * Outcome: Verify that the write succeeded. */
   bson_free (uri_str);
   uri_str = bson_strdup_printf ("mongodb://%s/?directConnection=false", hl_secondary->host_and_port);
   uri_str_auth = test_framework_add_user_password_from_env (uri_str);

   mongoc_client_destroy (client);
   client = test_framework_client_new (uri_str_auth, NULL);
   test_framework_set_ssl_opts (client);
   collection = get_test_collection (client, "sdam_dc_test");
   BSON_APPEND_UTF8 (&doc, "hello", "world");
   r = mongoc_collection_insert_one (collection, &doc, NULL, &reply, &error);
   ASSERT_OR_PRINT (r, error);
   ASSERT_CMPINT32 (bson_lookup_int32 (&reply, "insertedCount"), ==, 1);

   bson_destroy (&reply);
   bson_destroy (&doc);
   mongoc_server_description_destroy (sd_secondary);
   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_free (uri_str_auth);
   bson_free (uri_str);
   bson_free (replset_name);
   bson_free (host_and_port);
}

static void
test_direct_connection (void *ctx)
{
   char *host_and_port;
   char *replset_name;
   char *uri_str;
   char *uri_str_auth;
   mongoc_client_t *client;
   mongoc_read_prefs_t *prefs;
   mongoc_server_description_t *sd_secondary;
   mongoc_host_list_t *hl_secondary;
   mongoc_collection_t *collection;
   bson_t doc = BSON_INITIALIZER;
   bson_t reply;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   host_and_port = test_framework_get_host_and_port ();
   replset_name = test_framework_replset_name ();
   uri_str = test_framework_get_uri_str ();

   client = test_framework_client_new (uri_str, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   sd_secondary = mongoc_client_select_server (client,
                                               false, /* for reads */
                                               prefs,
                                               &error);
   ASSERT_OR_PRINT (sd_secondary, error);
   hl_secondary = mongoc_server_description_host (sd_secondary);

   /* Scenario: given a replica set deployment with a secondary, where HOST is
    * the address of the secondary, create a MongoClient using
    * ``mongodb://HOST/?directConnection=true`` as the URI.
    * Attempt a write to a collection.
    *
    * Outcome: Verify that the write failed with a NotPrimary error. */
   bson_free (uri_str);
   uri_str = bson_strdup_printf ("mongodb://%s/?directConnection=true", hl_secondary->host_and_port);
   uri_str_auth = test_framework_add_user_password_from_env (uri_str);

   mongoc_client_destroy (client);
   client = test_framework_client_new (uri_str_auth, NULL);
   test_framework_set_ssl_opts (client);
   collection = get_test_collection (client, "sdam_dc_test");
   BSON_APPEND_UTF8 (&doc, "hello", "world");
   r = mongoc_collection_insert_one (collection, &doc, NULL, &reply, &error);
   ASSERT_OR_PRINT (!r, error);
   ASSERT (strstr (error.message, "not master") || strstr (error.message, "not primary"));

   bson_destroy (&reply);
   bson_destroy (&doc);
   mongoc_server_description_destroy (sd_secondary);
   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_free (uri_str_auth);
   bson_free (uri_str);
   bson_free (replset_name);
   bson_free (host_and_port);
}

static void
test_existing_behavior (void *ctx)
{
   char *host_and_port;
   char *replset_name;
   char *uri_str;
   char *uri_str_auth;
   mongoc_client_t *client;
   mongoc_read_prefs_t *prefs;
   mongoc_server_description_t *sd_secondary;
   mongoc_host_list_t *hl_secondary;
   mongoc_collection_t *collection;
   bson_t doc = BSON_INITIALIZER;
   bson_t reply;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   host_and_port = test_framework_get_host_and_port ();
   replset_name = test_framework_replset_name ();
   uri_str = test_framework_get_uri_str ();

   client = test_framework_client_new (uri_str, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   sd_secondary = mongoc_client_select_server (client,
                                               false, /* for reads */
                                               prefs,
                                               &error);
   ASSERT_OR_PRINT (sd_secondary, error);
   hl_secondary = mongoc_server_description_host (sd_secondary);

   /* Scenario: given a replica set deployment with a secondary, where HOST is
    * the address of the secondary, create a MongoClient using
    * ``mongodb://HOST/`` as the URI.
    * Attempt a write to a collection.
    *
    * Outcome: Verify that the write succeeded or failed depending on existing
    * driver behavior with respect to the starting topology. */
   bson_free (uri_str);
   uri_str = bson_strdup_printf ("mongodb://%s/", hl_secondary->host_and_port);
   uri_str_auth = test_framework_add_user_password_from_env (uri_str);

   mongoc_client_destroy (client);
   client = test_framework_client_new (uri_str_auth, NULL);
   test_framework_set_ssl_opts (client);
   collection = get_test_collection (client, "sdam_dc_test");
   BSON_APPEND_UTF8 (&doc, "hello", "world");
   r = mongoc_collection_insert_one (collection, &doc, NULL, &reply, &error);
   ASSERT_OR_PRINT (!r, error);
   ASSERT (strstr (error.message, "not master") || strstr (error.message, "not primary"));

   bson_destroy (&reply);
   bson_destroy (&doc);
   mongoc_server_description_destroy (sd_secondary);
   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_free (uri_str_auth);
   bson_free (uri_str);
   bson_free (replset_name);
   bson_free (host_and_port);
}

typedef struct {
   uint32_t n_heartbeat_succeeded;
} prose_test_ctx_t;

static void
heartbeat_succeeded (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   prose_test_ctx_t *ctx;

   ctx = (prose_test_ctx_t *) mongoc_apm_server_heartbeat_succeeded_get_context (event);
   ctx->n_heartbeat_succeeded++;
/* The reported duration may be 0 on Windows due to poor clock resolution.
 * bson_get_monotonic_time () uses GetTickCount64. MS docs say:
 * "GetTickCount64 function is limited to the resolution of the system timer,
 * which is typically in the range of 10 milliseconds to 16 milliseconds"
 */
#ifndef _WIN32
   BSON_ASSERT (mongoc_apm_server_heartbeat_succeeded_get_duration (event) > 0);
#endif
}

#define RTT_TEST_TIMEOUT_SEC 60
#define RTT_TEST_INITIAL_SLEEP_SEC 2
#define RTT_TEST_TICK_MS 10

static void
test_prose_rtt (void *unused)
{
   /* Since this tests RTT tracking in the streaming protocol, this test
    * requires a client pool. */
   mongoc_client_pool_t *pool;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_error_t error;
   const bson_t *doc;
   mongoc_cursor_t *cursor;
   mongoc_apm_callbacks_t *callbacks;
   prose_test_ctx_t ctx;
   bson_t cmd;
   bool ret;
   int64_t start_us;
   bool satisfied;
   int64_t rtt = 0;

   BSON_UNUSED (unused);

   uri = test_framework_get_uri ();
   mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_APPNAME, "streamingRttTest");
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_server_heartbeat_succeeded_cb (callbacks, heartbeat_succeeded);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   test_framework_set_pool_ssl_opts (pool);
   memset (&ctx, 0, sizeof (prose_test_ctx_t));
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, &ctx);
   client = mongoc_client_pool_pop (pool);

   /* Run a find command for the server to be discovered. */
   coll = get_test_collection (client, "streamingRttTest");
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
   mongoc_cursor_next (cursor, &doc);

   /* Sleep for RTT_TEST_INITIAL_SLEEP_SEC seconds to allow multiple heartbeats
    * to succeed. */
   _mongoc_usleep (RTT_TEST_INITIAL_SLEEP_SEC * 1000 * 1000);

   /* Set a failpoint to make hello commands take longer. */
   bson_init (&cmd);
   BCON_APPEND (&cmd, "configureFailPoint", "failCommand");
   BCON_APPEND (&cmd, "mode", "{", "times", BCON_INT32 (1000), "}");
   BCON_APPEND (&cmd,
                "data",
                "{",
                "failCommands",
                "[",
                HANDSHAKE_CMD_LEGACY_HELLO,
                "hello",
                "]",
                "blockConnection",
                BCON_BOOL (true),
                "blockTimeMS",
                BCON_INT32 (500),
                "appName",
                "streamingRttTest",
                "}");
   ret = mongoc_client_command_simple (client, "admin", &cmd, NULL /* read prefs. */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Wait for the server's RTT to exceed 250ms. If this does not happen for
    * RTT_TEST_TIMEOUT_SEC seconds, consider it a failure. */
   satisfied = false;
   start_us = bson_get_monotonic_time ();
   while (!satisfied && bson_get_monotonic_time () < start_us + RTT_TEST_TIMEOUT_SEC * 1000 * 1000) {
      mongoc_server_description_t *sd;

      sd = mongoc_client_select_server (client, true, NULL /* read prefs */, &error);
      ASSERT_OR_PRINT (sd, error);
      rtt = mongoc_server_description_round_trip_time (sd);
      if (rtt > 250) {
         satisfied = true;
      }
      mongoc_server_description_destroy (sd);
      _mongoc_usleep (RTT_TEST_TICK_MS * 1000);
   }

   if (!satisfied) {
      test_error ("After %d seconds, the latest observed RTT was only %" PRId64, RTT_TEST_TIMEOUT_SEC, rtt);
   }

   /* Disable the failpoint. */
   bson_reinit (&cmd);
   BCON_APPEND (&cmd, "configureFailPoint", "failCommand");
   BCON_APPEND (&cmd, "mode", "off");
   ret = mongoc_client_command_simple (client, "admin", &cmd, NULL /* read prefs. */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   bson_destroy (&cmd);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
   mongoc_apm_callbacks_destroy (callbacks);

   /* Make this assertion after destroying the pool, to avoid reading while the
    * monitor thread is writing. */
   BSON_ASSERT (ctx.n_heartbeat_succeeded > 0);
}

void
test_sdam_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_AddFull (suite,
                      "/server_discovery_and_monitoring/topology/discovery",
                      test_topology_discovery,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/server_discovery_and_monitoring/directconnection",
                      test_direct_connection,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/server_discovery_and_monitoring/existing/behavior",
                      test_existing_behavior,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/server_discovery_and_monitoring/prose/rtt",
                      test_prose_rtt,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_max_wire_version_less_than_9);
}
