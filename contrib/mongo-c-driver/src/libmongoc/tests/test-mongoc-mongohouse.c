#include <mongoc/mongoc.h>

#include "json-test.h"
#include "json-test-operations.h"
#include "test-libmongoc.h"

static const char *uri_str = "mongodb://mhuser:pencil@localhost/?serverSelectionTryOnce=false";

static bool
mongohouse_test_operation_cb (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bson_t reply;
   bool res;

   res = json_test_operation (ctx, test, operation, ctx->collection, NULL, &reply);

   bson_destroy (&reply);

   return res;
}

static void
test_mongohouse_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;

   config.run_operation_cb = mongohouse_test_operation_cb;
   config.command_started_events_only = false;
   config.command_monitoring_allow_subset = true;
   config.scenario = scenario;
   config.uri_str = uri_str;

   run_json_general_test (&config);
}


typedef struct {
   /* Information from original cursor */
   char *cursor_ns;
   int64_t cursor_id;

   bool parsed_cursor;
   bool parsed_cmd_started;
   bool parsed_cmd_succeeded;

   bson_mutex_t mutex;
} _test_data_t;


static bool
cursor_in_killed_array (bson_t *cursors_killed, int64_t cursor_id)
{
   bson_iter_t iter;

   bson_iter_init (&iter, cursors_killed);
   while (bson_iter_next (&iter)) {
      BSON_ASSERT (BSON_ITER_HOLDS_INT64 (&iter));

      if (bson_iter_int64 (&iter) == cursor_id) {
         return true;
      }
   }

   return false;
}


static void
cmd_started_cb (const mongoc_apm_command_started_t *event)
{
   const uint8_t *array_data;
   bson_t *cursors_killed;
   uint32_t array_len;
   _test_data_t *test;
   const bson_t *cmd;
   bson_iter_t iter;
   const char *coll;
   const char *db;
   char *ns;

   if (strcmp (mongoc_apm_command_started_get_command_name (event), "killCursors") != 0) {
      return;
   }

   test = (_test_data_t *) mongoc_apm_command_started_get_context (event);
   bson_mutex_lock (&test->mutex);

   cmd = mongoc_apm_command_started_get_command (event);
   BSON_ASSERT (cmd);

   db = mongoc_apm_command_started_get_database_name (event);
   BSON_ASSERT (db);

   BSON_ASSERT (bson_iter_init_find (&iter, cmd, "killCursors"));
   BSON_ASSERT (BSON_ITER_HOLDS_UTF8 (&iter));
   coll = bson_iter_utf8 (&iter, NULL);

   BSON_ASSERT (test->parsed_cursor);
   BSON_ASSERT (test->cursor_ns);

   ns = bson_malloc0 (strlen (db) + strlen (coll) + 2);
   strcpy (ns, db);
   strcat (ns, ".");
   strcat (ns, coll);

   /* If the ns does not match, return without validating. */
   if (strcmp (test->cursor_ns, ns) != 0) {
      bson_mutex_unlock (&test->mutex);
      bson_free (ns);
      return;
   }

   bson_free (ns);

   /* Confirm that the cursor id is in the cursors_killed array. */
   BSON_ASSERT (bson_iter_init_find (&iter, cmd, "cursors"));
   BSON_ASSERT (BSON_ITER_HOLDS_ARRAY (&iter));
   bson_iter_array (&iter, &array_len, &array_data);

   cursors_killed = bson_new_from_data (array_data, array_len);

   test->parsed_cmd_started = cursor_in_killed_array (cursors_killed, test->cursor_id);
   bson_mutex_unlock (&test->mutex);

   bson_destroy (cursors_killed);
}

static void
cmd_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   const uint8_t *array_data;
   bson_t *cursors_killed;
   uint32_t array_len;
   const bson_t *reply;
   _test_data_t *test;
   bson_iter_t iter;
   bson_iter_t child_iter;
   const char *cmd;

   test = (_test_data_t *) mongoc_apm_command_succeeded_get_context (event);
   bson_mutex_lock (&test->mutex);

   cmd = mongoc_apm_command_succeeded_get_command_name (event);
   reply = mongoc_apm_command_succeeded_get_reply (event);

   /* Store cursor information from our initial find. */
   if (strcmp (cmd, "find") == 0) {
      BSON_ASSERT (!test->parsed_cursor);

      bson_iter_init (&iter, reply);
      BSON_ASSERT (bson_iter_find_descendant (&iter, "cursor.id", &child_iter));
      BSON_ASSERT (BSON_ITER_HOLDS_INT64 (&child_iter));
      test->cursor_id = bson_iter_int64 (&child_iter);

      bson_iter_init (&iter, reply);
      BSON_ASSERT (bson_iter_find_descendant (&iter, "cursor.ns", &child_iter));
      BSON_ASSERT (BSON_ITER_HOLDS_UTF8 (&child_iter));

      test->cursor_ns = bson_strdup (bson_iter_utf8 (&child_iter, NULL));
      BSON_ASSERT (NULL != test->cursor_ns);

      test->parsed_cursor = true;

      bson_mutex_unlock (&test->mutex);
      return;
   }

   if (strcmp (cmd, "killCursors") != 0) {
      bson_mutex_unlock (&test->mutex);
      return;
   }

   /* Confirm that the cursor id is in the cursors_killed array. */
   BSON_ASSERT (bson_iter_init_find (&iter, reply, "cursorsKilled"));
   BSON_ASSERT (BSON_ITER_HOLDS_ARRAY (&iter));
   bson_iter_array (&iter, &array_len, &array_data);

   cursors_killed = bson_new_from_data (array_data, array_len);

   test->parsed_cmd_succeeded = cursor_in_killed_array (cursors_killed, test->cursor_id);
   bson_mutex_unlock (&test->mutex);

   bson_destroy (cursors_killed);
}

/* Test that the driver properly constructs and issues a killCursors command to
 * ADL. */
static void
test_mongohouse_kill_cursors (void *ctx_unused)
{
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *coll;
   mongoc_client_t *client;
   mongoc_cursor_t *cursor;
   _test_data_t test;
   mongoc_uri_t *uri;
   bson_t query = BSON_INITIALIZER;
   const bson_t *doc;

   BSON_UNUSED (ctx_unused);

   uri = mongoc_uri_new (uri_str);
   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);

   test.cursor_ns = NULL;
   test.parsed_cursor = false;
   test.parsed_cmd_started = false;
   test.parsed_cmd_succeeded = false;
   bson_mutex_init (&test.mutex);

   /* Set callbacks to observe CommandSucceeded and CommandStarted events. */
   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, cmd_started_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, cmd_succeeded_cb);
   mongoc_client_set_apm_callbacks (client, callbacks, (void *) &test);
   mongoc_apm_callbacks_destroy (callbacks);

   coll = mongoc_client_get_collection (client, "test", "driverdata");

   /* Run a find on the server with a batchSize of 2 and a limit of 3. */
   cursor = mongoc_collection_find_with_opts (coll,
                                              &query,
                                              tmp_bson ("{ 'limit' : {'$numberLong' : '3'}, 'batchSize' : "
                                                        "{'$numberLong' : '2'}}"),
                                              NULL);

   /* Iterate the cursor to run the find on the server. */
   ASSERT_CURSOR_NEXT (cursor, &doc);

   /* Close the cursor. */
   mongoc_cursor_destroy (cursor);

   /* Callbacks will observe events for killCursors and validate accordingly. */
   BSON_ASSERT (test.parsed_cursor);
   BSON_ASSERT (test.parsed_cmd_started);
   BSON_ASSERT (test.parsed_cmd_succeeded);

   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   bson_free (test.cursor_ns);
   bson_mutex_destroy (&test.mutex);
}

static void
_run_ping_test (const char *connection_string)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   bson_error_t error;
   bool res;

   uri = mongoc_uri_new (connection_string);
   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);

   res = mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (res, error);

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
}

/* Test that the driver can establish a connection to ADL with authentication.
   Test both SCRAM-SHA-1 and SCRAM-SHA-256. */
static void
test_mongohouse_auth (void *ctx_unused)
{
   BSON_UNUSED (ctx_unused);

   /* SCRAM-SHA-1 */
   _run_ping_test ("mongodb://mhuser:pencil@localhost/?authMechanism=SCRAM-SHA-1");

   /* SCRAM-SHA-256 */
   _run_ping_test ("mongodb://mhuser:pencil@localhost/?authMechanism=SCRAM-SHA-256");
}

/* Test that the driver can connect to ADL without authentication. */
static void
test_mongohouse_no_auth (void *ctx_unused)
{
   BSON_UNUSED (ctx_unused);

   _run_ping_test ("mongodb://localhost:27017");
}


void
test_mongohouse_install (TestSuite *suite)
{
   install_json_test_suite_with_check (
      suite, JSON_DIR, "mongohouse", &test_mongohouse_cb, test_framework_skip_if_no_mongohouse);

   TestSuite_AddFull (suite,
                      "/mongohouse/kill_cursors",
                      test_mongohouse_kill_cursors,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_mongohouse);

   TestSuite_AddFull (
      suite, "/mongohouse/no_auth", test_mongohouse_no_auth, NULL, NULL, test_framework_skip_if_no_mongohouse);

   TestSuite_AddFull (
      suite, "/mongohouse/auth", test_mongohouse_auth, NULL, NULL, test_framework_skip_if_no_mongohouse);
}
