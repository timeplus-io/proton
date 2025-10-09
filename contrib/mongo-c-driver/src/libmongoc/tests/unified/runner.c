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

#include "bsonutil/bson-parser.h"
#include "entity-map.h"
#include "json-test.h"
#include "operation.h"
#include "runner.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "test-diagnostics.h"
#include "utlist.h"
#include "util.h"


typedef struct {
   const char *file_description;
   const char *test_description;
} skipped_unified_test_t;

#define SKIP_ALL_TESTS NULL

// clang-format off
skipped_unified_test_t SKIPPED_TESTS[] = {
   // CDRIVER-4001, DRIVERS-1781, and DRIVERS-1448: 5.0 cursor behavior
   {"poc-command-monitoring", "A successful find event with a getmore and the server kills the cursor"},
   // libmongoc does not have a distinct helper, so skip snapshot tests testing particular distinct functionality
   {"snapshot-sessions", "Distinct operation with snapshot"},
   {"snapshot-sessions", "Mixed operation with snapshot"},
   // CDRIVER-3886: serverless testing (schema version 1.4)
   {"poc-crud", SKIP_ALL_TESTS},
   {"db-aggregate", SKIP_ALL_TESTS},
   {"mongos-unpin", SKIP_ALL_TESTS},
   // CDRIVER-2871: CMAP is not implemented
   {"assertNumberConnectionsCheckedOut", SKIP_ALL_TESTS},
   {"entity-client-cmap-events", SKIP_ALL_TESTS},
   {"expectedEventsForClient-eventType", SKIP_ALL_TESTS},
   // CDRIVER-4115: listCollections does not support batchSize.
   {"cursors are correctly pinned to connections for load-balanced clusters", "listCollections pins the cursor to a connection"},
   // CDRIVER-4116: listIndexes does not support batchSize.
   {"cursors are correctly pinned to connections for load-balanced clusters", "listIndexes pins the cursor to a connection"},
   // libmongoc does not pin connections to cursors. It cannot force an error from waitQueueTimeoutMS by creating cursors in load balanced mode.
   {"wait queue timeout errors include details about checked out connections", SKIP_ALL_TESTS},
   // libmongoc does not support the optional findOne helper.
   {"retryable reads handshake failures", "collection.findOne succeeds after retryable handshake network error"},
   {"retryable reads handshake failures", "collection.findOne succeeds after retryable handshake server error (ShutdownInProgress)"},
   // libmongoc does not support the optional listIndexNames helper.
   {"retryable reads handshake failures", "collection.listIndexNames succeeds after retryable handshake network error"},
   {"retryable reads handshake failures", "collection.listIndexNames succeeds after retryable handshake server error (ShutdownInProgress)"},
   {0},
};
// clang-format on

static bool
is_test_file_skipped (test_file_t *test_file)
{
   skipped_unified_test_t *skip;

   for (skip = SKIPPED_TESTS; skip->file_description != NULL; skip++) {
      if (!strcmp (skip->file_description, test_file->description) && skip->test_description == SKIP_ALL_TESTS) {
         return true;
      }
   }

   return false;
}

static bool
is_test_skipped (test_t *test)
{
   skipped_unified_test_t *skip;

   for (skip = SKIPPED_TESTS; skip->file_description != NULL; skip++) {
      if (!strcmp (skip->file_description, test->test_file->description) &&
          !strcmp (skip->test_description, test->description)) {
         return true;
      }
   }

   return false;
}

struct _failpoint_t {
   char *client_id;
   char *name;
   uint32_t server_id;
   struct _failpoint_t *next;
};

failpoint_t *
failpoint_new (char *name, char *client_id, uint32_t server_id)
{
   failpoint_t *fp = (failpoint_t *) bson_malloc0 (sizeof (failpoint_t));

   fp->name = bson_strdup (name);
   fp->client_id = bson_strdup (client_id);
   fp->server_id = server_id;
   return fp;
}

void
failpoint_destroy (failpoint_t *fp)
{
   if (!fp) {
      return;
   }
   bson_free (fp->name);
   bson_free (fp->client_id);
   bson_free (fp);
}

/* Set server_id to 0 if the failpoint was not against a pinned mongos. */
void
register_failpoint (test_t *test, char *name, char *client_id, uint32_t server_id)
{
   failpoint_t *fp = NULL;

   fp = failpoint_new (name, client_id, server_id);
   LL_APPEND (test->failpoints, fp);
}

static bool
cleanup_failpoints (test_t *test, bson_error_t *error)
{
   bool ret = false;
   failpoint_t *iter = NULL;
   mongoc_read_prefs_t *rp = NULL;

   rp = mongoc_read_prefs_new (MONGOC_READ_PRIMARY_PREFERRED);

   LL_FOREACH (test->failpoints, iter)
   {
      mongoc_client_t *client = NULL;
      bson_t *disable_cmd = NULL;

      client = entity_map_get_client (test->entity_map, iter->client_id, error);
      if (!client) {
         goto done;
      }

      disable_cmd = tmp_bson ("{'configureFailPoint': '%s', 'mode': 'off' }", iter->name);
      if (iter->server_id != 0) {
         if (!mongoc_client_command_simple_with_server_id (
                client, "admin", disable_cmd, rp, iter->server_id, NULL /* reply */, error)) {
            bson_destroy (disable_cmd);
            goto done;
         }
      } else {
         if (!mongoc_client_command_simple (client, "admin", disable_cmd, rp, NULL /* reply */, error)) {
            bson_destroy (disable_cmd);
            goto done;
         }
      }
   }

   ret = true;
done:
   mongoc_read_prefs_destroy (rp);
   return ret;
}

static bool
test_has_operation (test_t *test, char *op_name)
{
   bson_iter_t iter;

   BSON_FOREACH (test->operations, iter)
   {
      bson_t op_bson;

      bson_iter_bson (&iter, &op_bson);
      if (0 == strcmp (bson_lookup_utf8 (&op_bson, "name"), op_name)) {
         return true;
      }
   }
   return false;
}

static const char *
get_topology_type (mongoc_client_t *client);

static bool
is_topology_type_sharded (const char *topology_type)
{
   return 0 == strcmp ("sharded", topology_type) || 0 == strcmp ("sharded-replicaset", topology_type);
}

static bool
is_topology_type_compatible (const char *test_topology_type, const char *server_topology_type)
{
   if (0 == strcmp (test_topology_type, server_topology_type)) {
      return true;
   }
   /* If a requirement specifies a "sharded" topology and server is of type
    * "sharded-replicaset", that is also compatible. */
   return 0 == strcmp (test_topology_type, "sharded") && is_topology_type_sharded (server_topology_type);
}

/* This callback tracks the set of server IDs for all connected servers.
 * The set of server IDs is used when sending a command to each individual
 * server.
 */
static void
on_topology_changed (const mongoc_apm_topology_changed_t *event)
{
   test_runner_t *test_runner = NULL;
   const mongoc_topology_description_t *td;
   mongoc_server_description_t **sds;
   size_t sds_len;
   size_t i;

   test_runner = (test_runner_t *) mongoc_apm_topology_changed_get_context (event);
   _mongoc_array_clear (&test_runner->server_ids);
   td = mongoc_apm_topology_changed_get_new_description (event);
   sds = mongoc_topology_description_get_servers (td, &sds_len);
   for (i = 0; i < sds_len; i++) {
      uint32_t server_id = mongoc_server_description_id (sds[i]);
      MONGOC_DEBUG ("topology changed, adding server id: %d", (int) server_id);
      _mongoc_array_append_val (&test_runner->server_ids, server_id);
   }
   mongoc_server_descriptions_destroy_all (sds, sds_len);
}

/* Returns an array of all known servers IDs that the test runner
 * is connected to. The server IDs can be used to target commands to
 * specific servers with mongoc_client_command_simple_with_server_id().
 */
static void
test_runner_get_all_server_ids (test_runner_t *test_runner, mongoc_array_t *out)
{
   bson_error_t error;
   bool ret;

   /* Run a 'ping' command to make sure topology has been scanned. */
   ret = mongoc_client_command_simple (test_runner->internal_client,
                                       "admin",
                                       tmp_bson ("{'ping': 1}"),
                                       NULL /* read prefs */,
                                       NULL /* reply */,
                                       NULL /* error */);
   ASSERT_OR_PRINT (ret, error);

   _mongoc_array_copy (out, &test_runner->server_ids);
}

/* Run killAllSessions against the primary or each mongos to terminate any
 * lingering open transactions.
 * See also: Spec section "Terminating Open Transactions"
 */
static bool
test_runner_terminate_open_transactions (test_runner_t *test_runner, bson_error_t *error)
{
   bson_t *kill_all_sessions_cmd = NULL;
   bool ret = false;
   bool cmd_ret = false;
   bson_error_t cmd_error = {0};

   if (test_framework_getenv_bool ("MONGOC_TEST_ATLAS")) {
      // Not applicable when running as test-atlas-executor.
      return true;
   } else if (0 == test_framework_skip_if_no_txns ()) {
      ret = true;
      goto done;
   }

   kill_all_sessions_cmd = tmp_bson ("{'killAllSessions': []}");
   /* Run on each mongos. Target each server individually. */
   if (is_topology_type_sharded (test_runner->topology_type)) {
      mongoc_array_t server_ids;
      size_t i;

      _mongoc_array_init (&server_ids, sizeof (uint32_t));
      test_runner_get_all_server_ids (test_runner, &server_ids);
      for (i = 0; i < server_ids.len; i++) {
         uint32_t server_id = _mongoc_array_index (&server_ids, uint32_t, i);

         cmd_ret = mongoc_client_command_simple_with_server_id (test_runner->internal_client,
                                                                "admin",
                                                                kill_all_sessions_cmd,
                                                                NULL /* read prefs. */,
                                                                server_id,
                                                                NULL,
                                                                &cmd_error);

         /* Ignore error code 11601 as a workaround for SERVER-38335. */
         if (!cmd_ret && cmd_error.code != 11601) {
            test_set_error (error,
                            "Unexpected error running killAllSessions on server (%d): %s",
                            (int) server_id,
                            cmd_error.message);
            _mongoc_array_destroy (&server_ids);
            goto done;
         }
      }
      _mongoc_array_destroy (&server_ids);
   } else {
      /* Run on primary. */
      cmd_ret = mongoc_client_command_simple (
         test_runner->internal_client, "admin", kill_all_sessions_cmd, NULL /* read prefs. */, NULL, &cmd_error);

      /* Ignore error code 11601 as a workaround for SERVER-38335. */
      if (!cmd_ret && cmd_error.code != 11601) {
         test_set_error (error, "Unexpected error running killAllSessions on primary: %s", cmd_error.message);
         goto done;
      }
   }

   ret = true;
done:
   return ret;
}

static test_runner_t *
test_runner_new (void)
{
   bson_error_t error;

   // Avoid executing unnecessary commands when running as test-atlas-executor.
   const bool is_atlas = test_framework_getenv_bool ("MONGOC_TEST_ATLAS");

   test_runner_t *const test_runner = bson_malloc0 (sizeof (test_runner_t));

   _mongoc_array_init (&test_runner->server_ids, sizeof (uint32_t));

   {
      mongoc_uri_t *const uri = test_framework_get_uri ();

      test_runner->internal_client = test_framework_client_new_from_uri (uri, NULL);

      /* In load balanced mode, the internal client must use the
       * SINGLE_LB_MONGOS_URI. */
      if (!is_atlas && !test_framework_is_loadbalanced ()) {
         /* Always use multiple mongoses if speaking to a mongos.
          * Some test operations require communicating with all known mongos */
         if (!test_framework_uri_apply_multi_mongos (uri, true, &error)) {
            test_error ("error applying multiple mongos: %s", error.message);
         }
      }

      mongoc_uri_destroy (uri);
   }

   {
      mongoc_apm_callbacks_t *const callbacks = mongoc_apm_callbacks_new ();
      mongoc_apm_set_topology_changed_cb (callbacks, on_topology_changed);
      mongoc_client_set_apm_callbacks (test_runner->internal_client, callbacks, test_runner);
      mongoc_apm_callbacks_destroy (callbacks);
   }

   test_framework_set_ssl_opts (test_runner->internal_client);

   mongoc_client_set_error_api (test_runner->internal_client, MONGOC_ERROR_API_VERSION_2);

   test_runner->topology_type = get_topology_type (test_runner->internal_client);
   server_semver (test_runner->internal_client, &test_runner->server_version);

   test_runner->is_serverless = test_framework_is_serverless ();

   /* Terminate any possible open transactions. */
   if (!test_runner_terminate_open_transactions (test_runner, &error)) {
      test_error ("error terminating transactions: %s", error.message);
   }

   {
      bson_t reply;
      /* Cache server parameters to check runOnRequirements. */
      if (!mongoc_client_command_simple (
             test_runner->internal_client, "admin", tmp_bson ("{'getParameter': '*'}"), NULL, &reply, &error)) {
         test_error ("error getting server parameters: %s, full reply: %s", error.message, tmp_json (&reply));
      }
      test_runner->server_parameters = bson_copy (&reply);
      bson_destroy (&reply);
   }

   return test_runner;
}

static void
test_runner_destroy (test_runner_t *test_runner)
{
   mongoc_client_destroy (test_runner->internal_client);
   _mongoc_array_destroy (&test_runner->server_ids);
   bson_destroy (test_runner->server_parameters);
   bson_free (test_runner);
}

static test_file_t *
test_file_new (test_runner_t *test_runner, bson_t *bson)
{
   test_file_t *test_file = NULL;
   bson_parser_t *parser = NULL;
   char *schema_version = NULL;

   test_file = bson_malloc0 (sizeof (test_file_t));
   test_file->test_runner = test_runner;

   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "description", &test_file->description);
   bson_parser_utf8 (parser, "schemaVersion", &schema_version);
   bson_parser_array_optional (parser, "runOnRequirements", &test_file->run_on_requirements);
   bson_parser_array_optional (parser, "createEntities", &test_file->create_entities);
   bson_parser_array_optional (parser, "initialData", &test_file->initial_data);
   bson_parser_doc_optional (parser, "_yamlAnchors", &test_file->yaml_anchors);
   bson_parser_array (parser, "tests", &test_file->tests);
   bson_parser_parse_or_assert (parser, bson);
   bson_parser_destroy (parser);

   semver_parse (schema_version, &test_file->schema_version);
   bson_free (schema_version);
   return test_file;
}

static void
test_file_destroy (test_file_t *test_file)
{
   bson_free (test_file->description);
   bson_destroy (test_file->tests);
   bson_destroy (test_file->initial_data);
   bson_destroy (test_file->create_entities);
   bson_destroy (test_file->run_on_requirements);
   bson_destroy (test_file->yaml_anchors);
   bson_free (test_file);
}

static test_t *
test_new (test_file_t *test_file, bson_t *bson)
{
   test_t *test = NULL;
   bson_parser_t *parser = NULL;

   test = bson_malloc0 (sizeof (test_t));
   test->test_file = test_file;
   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "description", &test->description);
   bson_parser_array_optional (parser, "runOnRequirements", &test->run_on_requirements);
   bson_parser_utf8_optional (parser, "skipReason", &test->skip_reason);
   bson_parser_array (parser, "operations", &test->operations);
   bson_parser_array_optional (parser, "expectEvents", &test->expect_events);
   bson_parser_array_optional (parser, "outcome", &test->outcome);
   bson_parser_parse_or_assert (parser, bson);
   bson_parser_destroy (parser);

   test->entity_map = entity_map_new ();
   return test;
}

static void
test_destroy (test_t *test)
{
   failpoint_t *fpiter, *fptmp;

   LL_FOREACH_SAFE (test->failpoints, fpiter, fptmp)
   {
      failpoint_destroy (fpiter);
   }

   entity_map_destroy (test->entity_map);
   bson_destroy (test->outcome);
   bson_destroy (test->expect_events);
   bson_destroy (test->operations);
   bson_destroy (test->run_on_requirements);
   bson_free (test->description);
   bson_free (test->skip_reason);
   bson_free (test);
}

static bool
is_replset (bson_t *hello_reply)
{
   if (bson_has_field (hello_reply, "setName")) {
      return true;
   }

   if (bson_has_field (hello_reply, "isreplicaset") && bson_lookup_bool (hello_reply, "isreplicaset") == true) {
      return true;
   }

   return false;
}

static bool
is_sharded (bson_t *hello_reply)
{
   const char *val;
   if (!bson_has_field (hello_reply, "msg")) {
      return false;
   }


   val = bson_lookup_utf8 (hello_reply, "msg");
   if (0 == strcmp (val, "isdbgrid")) {
      return true;
   }
   return false;
}

static const char *
get_topology_type (mongoc_client_t *client)
{
   bool ret;
   bson_t reply;
   bson_error_t error;
   const char *topology_type = "single";

   ASSERT (client);

   if (test_framework_is_loadbalanced ()) {
      return "load-balanced";
   }

   ret = mongoc_client_command_simple (client, "admin", tmp_bson ("{'hello': 1}"), NULL, &reply, &error);
   if (!ret) {
      bson_destroy (&reply);
      ret = mongoc_client_command_simple (
         client, "admin", tmp_bson ("{'" HANDSHAKE_CMD_LEGACY_HELLO "': 1}"), NULL, &reply, &error);
   }
   ASSERT_OR_PRINT (ret, error);

   if (is_replset (&reply)) {
      topology_type = "replicaset";
   } else if (is_sharded (&reply)) {
      bool is_sharded_replset;
      mongoc_collection_t *config_shards = NULL;
      mongoc_cursor_t *cursor = NULL;
      const bson_t *shard_doc;

      /* Check if this is a sharded-replicaset by querying the config.shards
       * collection. */
      is_sharded_replset = true;
      config_shards = mongoc_client_get_collection (client, "config", "shards");
      cursor =
         mongoc_collection_find_with_opts (config_shards, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
      if (mongoc_cursor_error (cursor, &error)) {
         test_error ("Attempting to query config.shards collection failed: %s", error.message);
      }
      while (mongoc_cursor_next (cursor, &shard_doc)) {
         const char *host = bson_lookup_utf8 (shard_doc, "host");
         if (NULL == strstr (host, "/")) {
            is_sharded_replset = false;
            break;
         }
      }

      mongoc_cursor_destroy (cursor);
      mongoc_collection_destroy (config_shards);

      if (is_sharded_replset) {
         topology_type = "sharded-replicaset";
      } else {
         topology_type = "sharded";
      }
   }

   bson_destroy (&reply);
   return topology_type;
}

static void
check_schema_version (test_file_t *test_file)
{
   const char *supported_version_strs[] = {"1.8",  /* fully supported through this version */
                                           "1.12", /* partially supported (expectedError.errorResponse assertions) */
                                           "1.18" /* partially supported (additional properties in kmsProviders) */};
   int i;

   for (i = 0; i < sizeof (supported_version_strs) / sizeof (supported_version_strs[0]); i++) {
      semver_t supported_version;

      semver_parse (supported_version_strs[i], &supported_version);
      if (supported_version.major != test_file->schema_version.major) {
         continue;
      }
      if (!supported_version.has_minor) {
         /* All minor versions for this major version are supported. */
         return;
      }
      if (supported_version.minor >= test_file->schema_version.minor) {
         return;
      }
   }

   test_error ("Unsupported schema version: %s", semver_to_string (&test_file->schema_version));
}

static bool
check_run_on_requirement (test_runner_t *test_runner,
                          bson_t *run_on_requirement,
                          const char *server_topology_type,
                          semver_t *server_version,
                          char **fail_reason)
{
   bson_iter_t req_iter;

   BSON_FOREACH (run_on_requirement, req_iter)
   {
      const char *key = bson_iter_key (&req_iter);

      if (0 == strcmp (key, "minServerVersion")) {
         semver_t min_server_version;

         semver_parse (bson_iter_utf8 (&req_iter, NULL), &min_server_version);
         if (semver_cmp (server_version, &min_server_version) < 0) {
            *fail_reason = bson_strdup_printf ("Server version(%s) is lower than minServerVersion(%s)",
                                               semver_to_string (server_version),
                                               semver_to_string (&min_server_version));
            return false;
         }
         continue;
      }

      if (0 == strcmp (key, "maxServerVersion")) {
         semver_t max_server_version;

         semver_parse (bson_iter_utf8 (&req_iter, NULL), &max_server_version);
         if (semver_cmp (server_version, &max_server_version) > 0) {
            *fail_reason = bson_strdup_printf ("Server version(%s) is higher than maxServerVersion (%s)",
                                               semver_to_string (server_version),
                                               semver_to_string (&max_server_version));
            return false;
         }
         continue;
      }

      if (0 == strcmp (key, "topologies")) {
         bool found = false;
         bson_t topologies;
         bson_iter_t topology_iter;

         bson_iter_bson (&req_iter, &topologies);
         BSON_FOREACH (&topologies, topology_iter)
         {
            const char *test_topology_type = bson_iter_utf8 (&topology_iter, NULL);
            if (is_topology_type_compatible (test_topology_type, server_topology_type)) {
               found = true;
               continue;
            }
         }

         if (!found) {
            *fail_reason = bson_strdup_printf (
               "Topology (%s) was not found among listed topologies: %s", server_topology_type, tmp_json (&topologies));
            return false;
         }
         continue;
      }

      if (0 == strcmp (key, "serverParameters")) {
         bson_t expected_params;
         bson_val_t *actual_val = NULL;
         bson_val_t *expected_val = NULL;
         bool matched;
         bson_error_t error = {0};

         bson_iter_bson (&req_iter, &expected_params);
         expected_val = bson_val_from_bson (&expected_params);
         actual_val = bson_val_from_bson (test_runner->server_parameters);
         matched = bson_match (expected_val, actual_val, false, &error);
         bson_val_destroy (actual_val);
         bson_val_destroy (expected_val);
         if (!matched) {
            *fail_reason = bson_strdup_printf ("serverParameters mismatch: %s", error.message);
            return false;
         }
         continue;
      }

      if (0 == strcmp (key, "serverless")) {
         const char *serverless_mode = bson_iter_utf8 (&req_iter, NULL);

         if (0 == strcmp (serverless_mode, "allow")) {
            continue;
         } else if (0 == strcmp (serverless_mode, "require")) {
            if (!test_runner->is_serverless) {
               *fail_reason = bson_strdup_printf ("Not running in serverless mode");
               return false;
            }

            continue;
         } else if (0 == strcmp (serverless_mode, "forbid")) {
            if (test_runner->is_serverless) {
               *fail_reason = bson_strdup_printf ("Running in serverless mode");
               return false;
            }

            continue;
         } else {
            test_error ("Unexpected serverless mode: %s", serverless_mode);
         }

         continue;
      }

      if (0 == strcmp (key, "auth")) {
         bool auth_requirement = bson_iter_bool (&req_iter);

         if (auth_requirement == test_framework_has_auth ()) {
            continue;
         }

         *fail_reason = bson_strdup_printf ("Server does not match auth requirement, test %s authentication.",
                                            auth_requirement ? "requires" : "forbids");

         return false;
      }

#if defined(MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION)
      if (0 == strcmp (key, "csfle")) {
         const bool csfle_required = bson_iter_bool (&req_iter);
         semver_t min_server_version;

         semver_parse ("4.2.0", &min_server_version);
         if (semver_cmp (server_version, &min_server_version) < 0) {
            *fail_reason = bson_strdup_printf ("Server version %s is lower than "
                                               "minServerVersion %s required by CSFLE",
                                               semver_to_string (server_version),
                                               semver_to_string (&min_server_version));
            return false;
         }

         if (csfle_required) {
            continue;
         }

         *fail_reason = bson_strdup_printf ("CSFLE is not allowed but libmongoc was built "
                                            "with MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION=ON");

         return false;
#else
      if (0 == strcmp (key, "csfle")) {
         const bool csfle_required = bson_iter_bool (&req_iter);

         if (!csfle_required) {
            continue;
         }

         *fail_reason = bson_strdup_printf ("CSFLE is required but libmongoc was built "
                                            "without MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION=ON");

         return false;
#endif /* !defined(MONGOC_CLIENT_SIDE_ENCRYPTION) */
      }

      test_error ("Unexpected runOnRequirement field: %s", key);
   }
   return true;
}

static bool
check_run_on_requirements (test_runner_t *test_runner, bson_t *run_on_requirements, const char **reason)
{
   bson_string_t *fail_reasons = NULL;
   bool requirements_satisfied = false;
   bson_iter_t iter;

   fail_reasons = bson_string_new ("");
   BSON_FOREACH (run_on_requirements, iter)
   {
      bson_t run_on_requirement;
      char *fail_reason = NULL;

      bson_iter_bson (&iter, &run_on_requirement);
      fail_reason = NULL;
      if (check_run_on_requirement (test_runner,
                                    &run_on_requirement,
                                    test_runner->topology_type,
                                    &test_runner->server_version,
                                    &fail_reason)) {
         requirements_satisfied = true;
         break;
      }

      bson_string_append_printf (
         fail_reasons, "- Requirement %s failed because: %s\n", bson_iter_key (&iter), fail_reason);
      bson_free (fail_reason);
   }

   *reason = NULL;
   if (!requirements_satisfied) {
      (*reason) = tmp_str ("runOnRequirements not satisfied:\n%s", fail_reasons->str);
   }
   bson_string_free (fail_reasons, true);
   return requirements_satisfied;
}

static bool
test_setup_initial_data (test_t *test, bson_error_t *error)
{
   test_runner_t *test_runner = NULL;
   test_file_t *test_file = NULL;
   bson_iter_t initial_data_iter;

   test_file = test->test_file;
   test_runner = test_file->test_runner;

   if (!test_file->initial_data) {
      return true;
   }

   BSON_FOREACH (test_file->initial_data, initial_data_iter)
   {
      bson_parser_t *parser = NULL;
      bson_t collection_data;
      char *collection_name = NULL;
      char *database_name = NULL;
      bson_t *documents = NULL;
      mongoc_database_t *db = NULL;
      mongoc_collection_t *coll = NULL;
      mongoc_bulk_operation_t *bulk_insert = NULL;
      mongoc_write_concern_t *wc = NULL;
      bson_t *bulk_opts = NULL;
      bson_t *drop_opts = NULL;
      bson_t *create_opts = NULL;
      bool ret = false;

      bson_iter_bson (&initial_data_iter, &collection_data);
      parser = bson_parser_new ();
      bson_parser_utf8 (parser, "databaseName", &database_name);
      bson_parser_utf8 (parser, "collectionName", &collection_name);
      bson_parser_array (parser, "documents", &documents);
      if (!bson_parser_parse (parser, &collection_data, error)) {
         goto loopexit;
      }

      wc = mongoc_write_concern_new ();
      mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
      bulk_opts = bson_new ();
      mongoc_write_concern_append (wc, bulk_opts);

      /* Drop the collection. */
      /* Check if the server supports majority write concern on 'drop' and
       * 'create'. */
      if (semver_cmp_str (&test_runner->server_version, "3.4") >= 0) {
         drop_opts = bson_new ();
         mongoc_write_concern_append (wc, drop_opts);
         create_opts = bson_new ();
         mongoc_write_concern_append (wc, create_opts);
      }
      coll = mongoc_client_get_collection (test_runner->internal_client, database_name, collection_name);
      if (!mongoc_collection_drop_with_opts (coll, drop_opts, error)) {
         if (error->code != 26 && (NULL == strstr (error->message, "ns not found"))) {
            /* This is not a "ns not found" error. Fail the test. */
            goto loopexit;
         }
         /* Clear an "ns not found" error. */
         memset (error, 0, sizeof (bson_error_t));
      }

      /* Insert documents if specified. */
      if (bson_count_keys (documents) > 0) {
         bson_iter_t documents_iter;

         bulk_insert = mongoc_collection_create_bulk_operation_with_opts (coll, bulk_opts);

         BSON_FOREACH (documents, documents_iter)
         {
            bson_t document;

            bson_iter_bson (&documents_iter, &document);
            mongoc_bulk_operation_insert (bulk_insert, &document);
         }

         if (!mongoc_bulk_operation_execute (bulk_insert, NULL, error)) {
            goto loopexit;
         }
      } else {
         mongoc_collection_t *new_coll = NULL;
         /* Test does not need data inserted, just create the collection. */
         db = mongoc_client_get_database (test_runner->internal_client, database_name);
         new_coll = mongoc_database_create_collection (db, collection_name, create_opts, error);
         if (!new_coll) {
            goto loopexit;
         }
         mongoc_collection_destroy (new_coll);
      }

      ret = true;

   loopexit:
      mongoc_bulk_operation_destroy (bulk_insert);
      bson_destroy (bulk_opts);
      bson_destroy (drop_opts);
      bson_destroy (create_opts);
      bson_destroy (documents);
      mongoc_write_concern_destroy (wc);
      mongoc_collection_destroy (coll);
      bson_free (database_name);
      bson_free (collection_name);
      bson_parser_destroy (parser);
      mongoc_database_destroy (db);
      if (!ret) {
         return false;
      }
   }
   return true;
}

static bool
test_create_entities (test_t *test, bson_error_t *error)
{
   test_file_t *test_file = NULL;
   bson_iter_t iter;

   test_file = test->test_file;

   if (!test_file->create_entities) {
      return true;
   }

   /* If a test runs a 'configureFailPoint' operation, reduce heartbeat on new
    * clients. */
   if (test_has_operation (test, "configureFailPoint")) {
      entity_map_set_reduced_heartbeat (test->entity_map, true);
   }

   BSON_FOREACH (test_file->create_entities, iter)
   {
      bson_t entity_bson;

      bson_iter_bson (&iter, &entity_bson);
      if (!entity_map_create (test->entity_map, &entity_bson, error)) {
         return false;
      }
   }
   return true;
}

static bool
test_run_operations (test_t *test, bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;

   BSON_FOREACH (test->operations, iter)
   {
      bson_t op_bson;
      bson_iter_bson (&iter, &op_bson);

      if (!operation_run (test, &op_bson, error)) {
         test_diagnostics_error_info ("running operation: %s", tmp_json (&op_bson));
         goto done;
      }
   }

   ret = true;
done:
   return ret;
}

static bool
test_check_event (test_t *test, bson_t *expected, event_t *actual, bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;
   bson_t expected_bson;
   bson_parser_t *bp = NULL;
   const char *expected_event_type;
   bson_t *expected_command = NULL;
   char *expected_command_name = NULL;
   char *expected_database_name = NULL;
   bson_t *expected_reply = NULL;
   bool *expected_has_service_id = NULL;
   bool *expected_has_server_connection_id = NULL;

   if (bson_count_keys (expected) != 1) {
      test_set_error (error, "expected 1 key in expected event, but got: %s", tmp_json (expected));
      goto done;
   }

   bson_iter_init (&iter, expected);
   bson_iter_next (&iter);
   expected_event_type = bson_iter_key (&iter);
   if (0 != bson_strcasecmp (expected_event_type, actual->type)) {
      test_set_error (error, "expected event type: %s, but got: %s", expected_event_type, actual->type);
      goto done;
   }

   if (!BSON_ITER_HOLDS_DOCUMENT (&iter)) {
      test_set_error (error, "unexpected non-document event assertion: %s", tmp_json (expected));
      goto done;
   }
   bson_iter_bson (&iter, &expected_bson);

   bp = bson_parser_new ();
   bson_parser_doc_optional (bp, "command", &expected_command);
   bson_parser_utf8_optional (bp, "commandName", &expected_command_name);
   bson_parser_utf8_optional (bp, "databaseName", &expected_database_name);
   bson_parser_doc_optional (bp, "reply", &expected_reply);
   bson_parser_bool_optional (bp, "hasServiceId", &expected_has_service_id);
   bson_parser_bool_optional (bp, "hasServerConnectionId", &expected_has_server_connection_id);
   if (!bson_parser_parse (bp, &expected_bson, error)) {
      goto done;
   }

   if (expected_command) {
      bson_val_t *expected_val;
      bson_val_t *actual_val;

      if (!actual || !actual->command) {
         test_set_error (error, "Expected a value but got NULL");
         goto done;
      }

      expected_val = bson_val_from_bson (expected_command);
      actual_val = bson_val_from_bson (actual->command);

      if (!entity_map_match (test->entity_map, expected_val, actual_val, false, error)) {
         bson_val_destroy (expected_val);
         bson_val_destroy (actual_val);
         goto done;
      }
      bson_val_destroy (expected_val);
      bson_val_destroy (actual_val);
   }

   if (expected_command_name && 0 != strcmp (expected_command_name, actual->command_name)) {
      test_set_error (error, "expected commandName: %s, but got: %s", expected_command_name, actual->command_name);
      goto done;
   }

   if (expected_database_name && 0 != strcmp (expected_database_name, actual->database_name)) {
      test_set_error (error, "expected databaseName: %s, but got: %s", expected_database_name, actual->database_name);
      goto done;
   }

   if (expected_reply) {
      bson_val_t *expected_val = bson_val_from_bson (expected_reply);
      bson_val_t *actual_val = bson_val_from_bson (actual->reply);
      if (!entity_map_match (test->entity_map, expected_val, actual_val, false, error)) {
         bson_val_destroy (expected_val);
         bson_val_destroy (actual_val);
         goto done;
      }
      bson_val_destroy (expected_val);
      bson_val_destroy (actual_val);
   }

   if (expected_has_service_id) {
      char oid_str[25] = {0};
      bool has_service_id = false;

      bson_oid_to_string (&actual->service_id, oid_str);
      has_service_id = 0 != bson_oid_compare (&actual->service_id, &kZeroServiceId);

      if (*expected_has_service_id && !has_service_id) {
         test_error ("expected serviceId, but got none");
      }

      if (!*expected_has_service_id && has_service_id) {
         test_error ("expected no serviceId, but got %s", oid_str);
      }
   }

   if (expected_has_server_connection_id) {
      const bool has_server_connection_id = actual->server_connection_id != MONGOC_NO_SERVER_CONNECTION_ID;

      if (*expected_has_server_connection_id && !has_server_connection_id) {
         test_error ("expected server connectionId, but got none");
      }

      if (!*expected_has_server_connection_id && has_server_connection_id) {
         test_error ("expected no server connectionId, but got %" PRId64, actual->server_connection_id);
      }
   }

   ret = true;
done:
   bson_parser_destroy_with_parsed_fields (bp);
   return ret;
}

static bool
test_check_expected_events_for_client (test_t *test, bson_t *expected_events_for_client, bson_error_t *error)
{
   bool ret = false;
   bson_parser_t *bp = NULL;
   char *client_id = NULL;
   bson_t *expected_events = NULL;
   bool just_false = false;
   bool *ignore_extra_events = &just_false;
   entity_t *entity = NULL;
   bson_iter_t iter;
   event_t *eiter = NULL;
   uint32_t expected_num_events;
   uint32_t actual_num_events = 0;
   char *event_type = NULL;

   bp = bson_parser_new ();
   bson_parser_utf8 (bp, "client", &client_id);
   bson_parser_array (bp, "events", &expected_events);
   bson_parser_bool_optional (bp, "ignoreExtraEvents", &ignore_extra_events);
   bson_parser_utf8_optional (bp, "eventType", &event_type);
   if (!bson_parser_parse (bp, expected_events_for_client, error)) {
      goto done;
   }

   if (event_type) {
      if (0 == strcmp (event_type, "cmap")) {
         /* TODO: (CDRIVER-3525) Explicitly ignore cmap events until CMAP is
          * supported. */
         ret = true;
         goto done;
      } else if (0 != strcmp (event_type, "command")) {
         test_set_error (error, "unexpected event type: %s", event_type);
         goto done;
      }
   }

   entity = entity_map_get (test->entity_map, client_id, error);
   if (0 != strcmp (entity->type, "client")) {
      test_set_error (error, "expected entity %s to be client, got: %s", entity->id, entity->type);
      goto done;
   }

   expected_num_events = bson_count_keys (expected_events);
   LL_COUNT (entity->events, eiter, actual_num_events);
   if (expected_num_events != actual_num_events) {
      bool too_many_events = actual_num_events > expected_num_events;
      if (ignore_extra_events && *ignore_extra_events) {
         // We can never have too many events
         too_many_events = false;
      }
      bool too_few_events = actual_num_events < expected_num_events;
      if (too_few_events || too_many_events) {
         test_set_error (
            error, "expected: %" PRIu32 " events but got %" PRIu32, expected_num_events, actual_num_events);
         goto done;
      }
   }

   eiter = entity->events;
   BSON_FOREACH (expected_events, iter)
   {
      bson_t expected_event;

      bson_iter_bson (&iter, &expected_event);
      if (!eiter) {
         test_set_error (error, "could not find event: %s", tmp_json (&expected_event));
         goto done;
      }
      if (!test_check_event (test, &expected_event, eiter, error)) {
         test_diagnostics_error_info ("checking for expected event: %s", tmp_json (&expected_event));
         goto done;
      }
      eiter = eiter->next;
   }

   ret = true;
done:
   if (!ret) {
      if (entity && entity->events) {
         char *event_list_string = NULL;

         event_list_string = event_list_to_string (entity->events);
         test_diagnostics_error_info ("all captured events:\n%s", event_list_string);
         bson_free (event_list_string);
      }
   }
   bson_parser_destroy_with_parsed_fields (bp);
   return ret;
}

static bool
test_check_expected_events (test_t *test, bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;

   if (!test->expect_events) {
      ret = true;
      goto done;
   }

   BSON_FOREACH (test->expect_events, iter)
   {
      bson_t expected_events_for_client;
      bson_iter_bson (&iter, &expected_events_for_client);
      if (!test_check_expected_events_for_client (test, &expected_events_for_client, error)) {
         test_diagnostics_error_info ("checking expectations: %s", tmp_json (&expected_events_for_client));
         goto done;
      }
   }


   ret = true;
done:
   return ret;
}

static bool
test_check_outcome_collection (test_t *test, bson_t *collection_data, bson_error_t *error)
{
   bool ret = false;
   bson_parser_t *bp = NULL;
   char *database_name = NULL;
   char *collection_name = NULL;
   bson_t *documents = NULL;
   mongoc_collection_t *coll = NULL;
   mongoc_cursor_t *cursor = NULL;
   bson_t *opts = NULL;
   mongoc_read_concern_t *rc = NULL;
   mongoc_read_prefs_t *rp = NULL;
   const bson_t *out;
   bson_t *actual_data = NULL;
   uint32_t i;
   bson_iter_t iter;
   bson_iter_t eiter;

   bp = bson_parser_new ();
   bson_parser_utf8 (bp, "databaseName", &database_name);
   bson_parser_utf8 (bp, "collectionName", &collection_name);
   bson_parser_array (bp, "documents", &documents);
   if (!bson_parser_parse (bp, collection_data, error)) {
      goto done;
   }

   coll = mongoc_client_get_collection (test->test_file->test_runner->internal_client, database_name, collection_name);
   opts = BCON_NEW ("sort", "{", "_id", BCON_INT32 (1), "}");
   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   rp = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), tmp_bson ("{'sort': {'_id': 1}}"), rp);
   /* Read the full cursor into a BSON array so error messages can include the
    * full list of documents. */
   actual_data = bson_new ();
   i = 0;
   while (mongoc_cursor_next (cursor, &out)) {
      char storage[16];
      const char *key;

      bson_uint32_to_string (i, &key, storage, sizeof (storage));
      BSON_APPEND_DOCUMENT (actual_data, key, out);
      i++;
   }

   if (mongoc_cursor_error (cursor, error)) {
      goto done;
   }

   if (bson_count_keys (actual_data) != bson_count_keys (documents)) {
      test_set_error (error,
                      "expected collection %s to contain: %s\nbut got: %s",
                      collection_name,
                      tmp_json (documents),
                      tmp_json (actual_data));
      goto done;
   }


   bson_iter_init (&eiter, documents);
   bson_iter_next (&eiter);

   BSON_FOREACH (actual_data, iter)
   {
      bson_t actual;
      bson_t expected;
      bson_t *actual_sorted = NULL;
      bson_t *expected_sorted = NULL;

      bson_iter_bson (&iter, &actual);
      actual_sorted = bson_copy_and_sort (&actual);

      bson_iter_bson (&eiter, &expected);
      expected_sorted = bson_copy_and_sort (&expected);


      if (!bson_equal (actual_sorted, expected_sorted)) {
         test_set_error (error, "expected %s, but got %s", tmp_json (expected_sorted), tmp_json (actual_sorted));
         bson_destroy (actual_sorted);
         bson_destroy (expected_sorted);
         goto done;
      }

      bson_destroy (actual_sorted);
      bson_destroy (expected_sorted);

      bson_iter_next (&eiter);
   }

   ret = true;
done:
   bson_destroy (opts);
   mongoc_collection_destroy (coll);
   mongoc_cursor_destroy (cursor);
   mongoc_read_concern_destroy (rc);
   mongoc_read_prefs_destroy (rp);
   bson_destroy (actual_data);
   bson_parser_destroy_with_parsed_fields (bp);
   return ret;
}

static bool
test_check_outcome (test_t *test, bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;

   if (!test->outcome) {
      ret = true;
      goto done;
   }

   BSON_FOREACH (test->outcome, iter)
   {
      bson_t collection_data;

      bson_iter_bson (&iter, &collection_data);
      if (!test_check_outcome_collection (test, &collection_data, error)) {
         goto done;
      }
   }

   ret = true;
done:
   return ret;
}

static void
append_size_t (bson_t *doc, const char *key, size_t value)
{
   BSON_ASSERT (bson_in_range_unsigned (int64_t, value));
   BSON_ASSERT (BSON_APPEND_INT64 (doc, key, (int64_t) value));
}

static void
append_bson_array (bson_t *doc, const char *key, const mongoc_array_t *array)
{
   BSON_ASSERT_PARAM (key);
   BSON_ASSERT (array || true);

   if (!array) {
      bson_t empty = BSON_INITIALIZER;
      BSON_ASSERT (BSON_APPEND_ARRAY (doc, key, &empty));
      bson_destroy (&empty);
   } else {
      bson_t **const begin = array->data;
      bson_t **const end = begin + array->len;

      bson_array_builder_t *elements;

      BSON_ASSERT (BSON_APPEND_ARRAY_BUILDER_BEGIN (doc, key, &elements));
      for (bson_t **iter = begin; iter != end; ++iter) {
         BSON_ASSERT (bson_array_builder_append_document (elements, *iter));
      }
      BSON_ASSERT (bson_append_array_builder_end (doc, elements));
   }
}

static bool
test_generate_atlas_results (test_t *test, bson_error_t *error)
{
   BSON_ASSERT_PARAM (test);
   BSON_ASSERT_PARAM (error);

   // This is only applicable when the unified test runner is being run by
   // test-atlas-executor. Must be implemented within unified test runner in
   // order to capture entities before destruction of parent test object.
   if (!test_framework_getenv_bool ("MONGOC_TEST_ATLAS")) {
      return true;
   }

   MONGOC_DEBUG ("generating events.json and results.json files...");

   size_t *const iterations = entity_map_get_size_t (test->entity_map, "iterations", NULL);
   size_t *const successes = entity_map_get_size_t (test->entity_map, "successes", NULL);
   mongoc_array_t *const errors = entity_map_get_bson_array (test->entity_map, "errors", NULL);
   mongoc_array_t *const failures = entity_map_get_bson_array (test->entity_map, "failures", NULL);
   mongoc_array_t *const events = entity_map_get_bson_array (test->entity_map, "events", NULL);

   bson_t events_doc = BSON_INITIALIZER;
   bson_t results_doc = BSON_INITIALIZER;

   append_bson_array (&events_doc, "events", events);
   append_bson_array (&events_doc, "failures", failures);
   append_bson_array (&events_doc, "errors", errors);

   append_size_t (&results_doc, "numErrors", errors ? errors->len : 0u);
   append_size_t (&results_doc, "numFailures", failures ? failures->len : 0u);
   append_size_t (&results_doc, "numIterations", iterations ? *iterations : 0u);
   append_size_t (&results_doc, "numSuccesses", successes ? *successes : 0u);

#ifdef WIN32
   const int perms = _S_IWRITE;
#else
   const int perms = S_IRWXU;
#endif

   mongoc_stream_t *const events_file =
      mongoc_stream_file_new_for_path ("events.json", O_CREAT | O_WRONLY | O_TRUNC, perms);
   ASSERT_WITH_MSG (events_file, "could not open events.json");

   mongoc_stream_t *const results_file =
      mongoc_stream_file_new_for_path ("results.json", O_CREAT | O_WRONLY | O_TRUNC, perms);
   ASSERT_WITH_MSG (results_file, "could not open results.json");

   size_t events_json_len = 0u;
   size_t results_json_len = 0u;
   char *const events_json = bson_as_json (&events_doc, &events_json_len);
   char *const results_json = bson_as_json (&results_doc, &results_json_len);

   ASSERT_WITH_MSG (events_json, "failed to convert events BSON document to JSON");
   ASSERT_WITH_MSG (results_json, "failed to convert results BSON document to JSON");

   ASSERT_WITH_MSG (mongoc_stream_write (events_file, events_json, events_json_len, 500) > 0,
                    "failed to write events to events.json");
   ASSERT_WITH_MSG (mongoc_stream_write (results_file, results_json, results_json_len, 500) > 0,
                    "failed to write results to results.json");

   bson_free (events_json);
   bson_free (results_json);

   mongoc_stream_destroy (events_file);
   mongoc_stream_destroy (results_file);

   bson_destroy (&events_doc);
   bson_destroy (&results_doc);

   MONGOC_DEBUG ("generating events.json and results.json files... done.");

   return true;
}

static bool
run_distinct_on_each_mongos (test_t *test, char *db_name, char *coll_name, bson_error_t *error)
{
   bool ret = false;
   bson_t *cmd = NULL;
   test_runner_t *runner = test->test_file->test_runner;

   cmd = BCON_NEW ("distinct", coll_name, "key", "x", "query", "{", "}");

   for (size_t i = 0u; i < runner->server_ids.len; i++) {
      const uint32_t server_id = _mongoc_array_index (&runner->server_ids, uint32_t, i);
      if (!mongoc_client_command_simple_with_server_id (test->test_file->test_runner->internal_client,
                                                        db_name,
                                                        cmd,
                                                        NULL /* read prefs */,
                                                        server_id,
                                                        NULL /* reply */,
                                                        error)) {
         goto done;
      }
   }


   ret = true;
done:
   bson_destroy (cmd);
   return ret;
}

static bool
test_run_distinct_workaround (test_t *test, bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;
   bson_parser_t *bp = NULL;
   mongoc_collection_t *coll = NULL;

   if (0 != strcmp (test->test_file->test_runner->topology_type, "sharded") &&
       0 != strcmp (test->test_file->test_runner->topology_type, "sharded-replicaset")) {
      ret = true;
      goto done;
   }

   if (!test_has_operation (test, "distinct")) {
      ret = true;
      goto done;
   }

   /* Get the database/collection name from each collection entity. */
   BSON_FOREACH (test->test_file->create_entities, iter)
   {
      bson_t entity_bson;
      char *coll_name = NULL;
      char *db_id = NULL;
      char *db_name = NULL;
      mongoc_database_t *db = NULL;
      bson_iter_t entity_iter;

      if (!BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         test_set_error (error, "unexpected non-document createEntity: %s", bson_iter_key (&iter));
         goto done;
      }

      bson_iter_recurse (&iter, &entity_iter);

      if (!bson_iter_find (&entity_iter, "collection")) {
         continue;
      }

      if (!BSON_ITER_HOLDS_DOCUMENT (&entity_iter)) {
         test_set_error (error, "unexpected non-document in iter: %s", bson_iter_key (&entity_iter));
         goto done;
      }

      bson_iter_bson (&entity_iter, &entity_bson);

      bp = bson_parser_new ();
      bson_parser_allow_extra (bp, true);
      bson_parser_utf8 (bp, "collectionName", &coll_name);
      bson_parser_utf8 (bp, "database", &db_id);
      if (!bson_parser_parse (bp, &entity_bson, error)) {
         goto done;
      }

      db = entity_map_get_database (test->entity_map, db_id, error);
      if (!db) {
         goto done;
      }

      db_name = (char *) mongoc_database_get_name (db);

      if (!run_distinct_on_each_mongos (test, db_name, coll_name, error)) {
         goto done;
      }

      bson_parser_destroy_with_parsed_fields (bp);
      bp = NULL;
   }

   ret = true;
done:
   mongoc_collection_destroy (coll);
   bson_parser_destroy_with_parsed_fields (bp);
   return ret;
}

/* This returns an error on failure instead of asserting where possible.
 * This allows the test runner to perform server clean up even on failure (e.g.
 * disable failpoints).
 */
bool
test_run (test_t *test, bson_error_t *error)
{
   bool ret = false;
   test_runner_t *test_runner = NULL;
   test_file_t *test_file = NULL;
   char *subtest_selector = NULL;
   bson_error_t nonfatal_error;

   test_file = test->test_file;
   test_runner = test_file->test_runner;

   if (is_test_skipped (test)) {
      MONGOC_DEBUG ("SKIPPING test '%s'. Reason: 'explicitly skipped in runner.c'", test->description);
      ret = true;
      goto done;
   }

   subtest_selector = _mongoc_getenv ("MONGOC_JSON_SUBTEST");
   if (subtest_selector && NULL == strstr (test->description, subtest_selector)) {
      MONGOC_DEBUG ("SKIPPING test '%s'. Reason: 'skipped by MONGOC_JSON_SUBTEST'", test->description);
      ret = true;
      goto done;
   }

   if (test->skip_reason != NULL) {
      MONGOC_DEBUG ("SKIPPING test '%s'. Reason: '%s'", test->description, test->skip_reason);
      ret = true;
      goto done;
   }

   if (test->run_on_requirements) {
      const char *reason;
      if (!check_run_on_requirements (test_runner, test->run_on_requirements, &reason)) {
         MONGOC_DEBUG ("SKIPPING test '%s'. Reason: '%s'", test->description, reason);
         ret = true;
         goto done;
      }
   }

   if (!test_setup_initial_data (test, error)) {
      test_diagnostics_error_info ("%s", "setting up initial data");
      goto done;
   }

   if (!test_create_entities (test, error)) {
      test_diagnostics_error_info ("%s", "creating entities");
      goto done;
   }

   if (!test_run_distinct_workaround (test, error)) {
      test_diagnostics_error_info ("%s", "sending distinct to each mongos");
      goto done;
   }

   if (!test_run_operations (test, error)) {
      test_diagnostics_error_info ("%s", "running operations");
      goto done;
   }

   entity_map_disable_event_listeners (test->entity_map);

   if (!test_check_expected_events (test, error)) {
      test_diagnostics_error_info ("%s", "checking expectations");
      goto done;
   }

   if (!test_check_outcome (test, error)) {
      test_diagnostics_error_info ("%s", "checking outcome");
      goto done;
   }

   if (!test_generate_atlas_results (test, error)) {
      test_diagnostics_error_info ("%s", "generating Atlas test results");
      goto done;
   }

   ret = true;
done:
   /* always clean up failpoints, even on test failure */
   if (!cleanup_failpoints (test, &nonfatal_error)) {
      MONGOC_DEBUG ("error cleaning up failpoints: %s", nonfatal_error.message);
   }
   /* always terminate transactions, even on test failure. */
   if (!test_runner_terminate_open_transactions (test_runner, &nonfatal_error)) {
      MONGOC_DEBUG ("error terminating transactions: %s", nonfatal_error.message);
   }
   bson_free (subtest_selector);
   return ret;
}

void
run_one_test_file (bson_t *bson)
{
   test_runner_t *test_runner = NULL;
   test_file_t *test_file = NULL;
   bson_iter_t test_iter;

   test_diagnostics_init ();

   test_runner = test_runner_new ();
   test_file = test_file_new (test_runner, bson);

   test_diagnostics_test_info ("test file: %s", test_file->description);

   if (is_test_file_skipped (test_file)) {
      MONGOC_DEBUG ("SKIPPING test file '%s'. Reason: 'explicitly skipped in runner.c'", test_file->description);
      goto done;
   }

   check_schema_version (test_file);
   if (test_file->run_on_requirements) {
      const char *reason;
      if (!check_run_on_requirements (test_runner, test_file->run_on_requirements, &reason)) {
         MONGOC_DEBUG ("SKIPPING test file (%s). Reason:\n%s", test_file->description, reason);
         goto done;
      }
   }

   BSON_FOREACH (test_file->tests, test_iter)
   {
      test_t *test = NULL;
      bson_t test_bson;
      bool test_ok;
      bson_error_t error = {0};

      test_diagnostics_reset ();
      test_diagnostics_test_info ("test file: %s", test_file->description);

      bson_iter_bson (&test_iter, &test_bson);
      test = test_new (test_file, &test_bson);
      test_diagnostics_test_info ("running test: %s", test->description);
      test_ok = test_run (test, &error);
      if (!test_ok) {
         test_diagnostics_abort (&error);
      }
      test_destroy (test);
   }

done:
   test_file_destroy (test_file);
   test_runner_destroy (test_runner);
   test_diagnostics_cleanup ();
}

void
run_unified_tests (TestSuite *suite, const char *base, const char *subdir)
{
   install_json_test_suite_with_check (
      suite, base, subdir, &run_one_test_file, TestSuite_CheckLive, test_framework_skip_if_no_crypto);
}

void
test_install_unified (TestSuite *suite)
{
   run_unified_tests (suite, JSON_DIR, "unified");

   run_unified_tests (suite, JSON_DIR, "crud/unified");

   run_unified_tests (suite, JSON_DIR, "transactions/unified");

   run_unified_tests (suite, JSON_DIR, "collection-management");

   run_unified_tests (suite, JSON_DIR, "sessions/unified");

   run_unified_tests (suite, JSON_DIR, "change_streams/unified");

   run_unified_tests (suite, JSON_DIR, "load_balancers");

   run_unified_tests (suite, JSON_DIR, "client_side_encryption/unified");

   run_unified_tests (suite, JSON_DIR, "retryable_reads/unified");

   run_unified_tests (suite, JSON_DIR, "retryable_writes/unified");

   run_unified_tests (suite, JSON_DIR, "index-management");
}
