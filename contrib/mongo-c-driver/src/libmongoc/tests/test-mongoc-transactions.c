#include <mongoc/mongoc.h>

#include "mongoc/mongoc-collection-private.h"

#include "json-test.h"
#include "test-libmongoc.h"
#include "mock_server/mock-rs.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "json-test-operations.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-read-concern-private.h"
#include "mongoc/mongoc-write-concern-private.h"

/* Reset server state by disabling failpoints, killing sessions, and... running
 * a distinct command. */
static void
_reset_server (json_test_ctx_t *ctx, const char *host_str)
{
   mongoc_client_t *client;
   bson_error_t error;
   bool res;
   mongoc_uri_t *uri = _mongoc_uri_copy_and_replace_host_list (ctx->test_framework_uri, host_str);

   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);

   /* From Transactions tests runner: "Create a MongoClient and call
    * client.admin.runCommand({killAllSessions: []}) to clean up any open
    * transactions from previous test failures. Ignore a command failure with
    * error code 11601 ("Interrupted") to work around SERVER-38335."
    */
   res = mongoc_client_command_simple (client, "admin", tmp_bson ("{'killAllSessions': []}"), NULL, NULL, &error);
   if (!res && error.code != 11601) {
      test_error ("Unexpected error: %s from killAllSessions\n", error.message);
   }

   /* From Transactions spec test runner: "When testing against a sharded
    * cluster run a distinct command on the newly
    * created collection on all mongoses. For an explanation see, Why do tests
    * that run distinct sometimes fail with StaleDbVersion?" */

   ASSERT_OR_PRINT (mongoc_client_command_simple (client,
                                                  mongoc_database_get_name (ctx->db),
                                                  tmp_bson ("{'distinct': '%s', 'key': 'test', 'query': {}}",
                                                            mongoc_collection_get_name (ctx->collection)),
                                                  NULL /* read prefs */,
                                                  NULL /* reply */,
                                                  &error),
                    error);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
}

static void
_disable_failpoints (json_test_ctx_t *ctx, const char *host_str)
{
   mongoc_client_t *client;
   bson_error_t error;
   int i;
   mongoc_uri_t *uri = _mongoc_uri_copy_and_replace_host_list (ctx->test_framework_uri, host_str);

   /* Some transactions tests have a failCommand for "hello" repeat seven
    * times. Repeat this seven times. And set a reduced server selection timeout
    * so we don't hang on failed hello commands. */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 500);

   for (i = 0; i < 7; i++) {
      bool ret;

      client = test_framework_client_new_from_uri (uri, NULL);
      ret = mongoc_client_command_simple (
         client, "admin", tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': 'off'}"), NULL, NULL, &error);
      if (!ret) {
         /* Tests that fail with hello also fail to disable the failpoint
          * (since we run hello when opening the connection). Ignore those
          * errors. */
         BSON_ASSERT (NULL != strstr (error.message, "No suitable servers found"));
      }
      mongoc_client_destroy (client);
   }
   mongoc_uri_destroy (uri);
}

static void
transactions_test_before_test (json_test_ctx_t *ctx, const bson_t *test)
{
   bson_iter_t test_iter;
   bool is_multi_mongos;

   _reset_server (ctx, "localhost:27017");

   is_multi_mongos = bson_iter_init_find (&test_iter, test, "useMultipleMongoses") && bson_iter_as_bool (&test_iter);

   if (is_multi_mongos) {
      _reset_server (ctx, "localhost:27018");
   }
}


static void
transactions_test_after_test (json_test_ctx_t *ctx, const bson_t *test)
{
   bson_iter_t test_iter;
   bool is_multi_mongos;

   _disable_failpoints (ctx, "localhost:27017");

   is_multi_mongos = bson_iter_init_find (&test_iter, test, "useMultipleMongoses") && bson_iter_as_bool (&test_iter);

   if (is_multi_mongos) {
      _disable_failpoints (ctx, "localhost:27018");
   }
}


typedef struct _cb_ctx_t {
   bson_t callback;
   json_test_ctx_t *ctx;
} cb_ctx_t;


static bool
with_transaction_callback_runner (mongoc_client_session_t *session, void *ctx, bson_t **reply, bson_error_t *error)
{
   cb_ctx_t *cb_ctx = (cb_ctx_t *) ctx;
   bson_t operation;
   bson_t operations;
   bson_t *test;
   bson_iter_t iter;
   bool res = false;
   bson_t local_reply;

   BSON_UNUSED (error);

   test = &(cb_ctx->callback);

   if (bson_has_field (test, "operation")) {
      bson_lookup_doc (test, "operation", &operation);
      res = json_test_operation (cb_ctx->ctx, test, &operation, cb_ctx->ctx->collection, session, &local_reply);
   } else {
      ASSERT (bson_has_field (test, "operations"));
      bson_lookup_doc (test, "operations", &operations);
      BSON_ASSERT (bson_iter_init (&iter, &operations));

      bson_init (&local_reply);

      while (bson_iter_next (&iter)) {
         bson_destroy (&local_reply);
         bson_iter_bson (&iter, &operation);
         res = json_test_operation (cb_ctx->ctx, test, &operation, cb_ctx->ctx->collection, session, &local_reply);
         if (!res) {
            break;
         }
      }
   }

   *reply = bson_copy (&local_reply);
   bson_destroy (&local_reply);

   return res;
}

static bool
transactions_test_run_operation (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   mongoc_transaction_opt_t *opts = NULL;
   mongoc_client_session_t *session = NULL;
   bson_error_t error;
   bson_value_t value;
   bson_t reply;
   bool res;
   cb_ctx_t cb_ctx;

   /* If there is a 'callback' field, run the nested operations through
      mongoc_client_session_with_transaction(). */
   if (bson_has_field (operation, "arguments.callback")) {
      ASSERT (bson_has_field (operation, "object"));
      session = session_from_name (ctx, bson_lookup_utf8 (operation, "object"));
      ASSERT (session);

      bson_lookup_doc (operation, "arguments.callback", &cb_ctx.callback);
      cb_ctx.ctx = ctx;

      if (bson_has_field (operation, "arguments.options")) {
         opts = bson_lookup_txn_opts (operation, "arguments.options");
      }

      res = mongoc_client_session_with_transaction (
         session, with_transaction_callback_runner, opts, &cb_ctx, &reply, &error);

      value_init_from_doc (&value, &reply);
      check_result (test, operation, res, &value, &error);
      bson_value_destroy (&value);

   } else {
      /* If there is no 'callback' field, then run simply. */
      if (bson_has_field (operation, "arguments.session")) {
         session = session_from_name (ctx, bson_lookup_utf8 (operation, "arguments.session"));
      }

      /* expect some warnings from abortTransaction, but don't suppress others:
       * we want to know if any other tests log warnings */
      capture_logs (true);
      res = json_test_operation (ctx, test, operation, ctx->collection, session, &reply);
      assert_all_captured_logs_have_prefix ("Error in abortTransaction:");
      capture_logs (false);
   }

   bson_destroy (&reply);
   mongoc_transaction_opts_destroy (opts);

   return res;
}


static test_skip_t skips[] = {
   {"callback is not retried after non-transient error (DuplicateKeyError)", "Waiting on CDRIVER-4811"}, {0}};


static void
test_transactions_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;

   config.skips = skips;

   config.before_test_cb = transactions_test_before_test;
   config.run_operation_cb = transactions_test_run_operation;
   config.after_test_cb = transactions_test_after_test;
   config.scenario = scenario;
   config.command_started_events_only = true;
   run_json_general_test (&config);
}


static void
test_transactions_supported (void *ctx)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_database_t *db;
   mongoc_collection_t *collection;
   bson_t *majority = tmp_bson ("{'writeConcern': {'w': 'majority'}}");
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   if (test_framework_is_mongos ()) {
      bson_destroy (&opts);
      return;
   }

   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, 2);
   db = mongoc_client_get_database (client, "transaction-tests");

   /* drop and create collection outside of transaction */
   mongoc_database_write_command_with_opts (db, tmp_bson ("{'drop': 'test'}"), majority, NULL, NULL);
   collection = mongoc_database_create_collection (db, "test", majority, &error);
   ASSERT_OR_PRINT (collection, error);

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);

   if ((r = mongoc_client_session_start_transaction (session, NULL, &error))) {
      r = mongoc_client_session_append (session, &opts, &error);
      ASSERT_OR_PRINT (r, error);

      r = mongoc_collection_insert_one (collection, tmp_bson ("{}"), &opts, NULL, &error);

      /* insert should fail if replset has no members */
      BSON_ASSERT (r == test_framework_is_replset ());
   } else {
      ASSERT_CMPINT32 (error.domain, ==, MONGOC_ERROR_TRANSACTION);
      ASSERT_CONTAINS (error.message, "transaction");
   }

   bson_destroy (&opts);
   mongoc_collection_destroy (collection);

   if (!r) {
      /* suppress "error in abortTransaction" warning from session_destroy */
      capture_logs (true);
   }

   mongoc_client_session_destroy (session);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}


static void
test_in_transaction (void *ctx)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_database_t *db;
   mongoc_collection_t *collection;
   bson_t *majority = tmp_bson ("{'writeConcern': {'w': 'majority'}}");
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, 2);
   db = mongoc_client_get_database (client, "transaction-tests");
   /* drop and create collection outside of transaction */
   mongoc_database_write_command_with_opts (db, tmp_bson ("{'drop': 'test'}"), majority, NULL, NULL);
   collection = mongoc_database_create_collection (db, "test", majority, &error);
   ASSERT_OR_PRINT (collection, error);

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   r = mongoc_client_session_append (session, &opts, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (!mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_NONE);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_NONE);

   /* commit an empty transaction */
   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_STARTING);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_STARTING);
   r = mongoc_client_session_commit_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (!mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_COMMITTED);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_COMMITTED_EMPTY);

   /* commit a transaction with an insert */
   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_STARTING);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_STARTING);
   r = mongoc_collection_insert_one (collection, tmp_bson ("{}"), &opts, NULL, &error);
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_IN_PROGRESS);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_IN_PROGRESS);
   ASSERT_OR_PRINT (r, error);
   r = mongoc_client_session_commit_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (!mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_COMMITTED);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_COMMITTED);

   /* abort a transaction */
   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_STARTING);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_STARTING);
   ASSERT_OR_PRINT (r, error);
   r = mongoc_client_session_abort_transaction (session, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (!mongoc_client_session_in_transaction (session));
   ASSERT_CMPINT (mongoc_client_session_get_transaction_state (session), ==, MONGOC_TRANSACTION_ABORTED);
   ASSERT_CMPINT (session->txn.state, ==, MONGOC_INTERNAL_TRANSACTION_ABORTED);

   bson_destroy (&opts);
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (db);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}


static bool
hangup_except_hello (request_t *request, void *data)
{
   BSON_UNUSED (data);

   if (!bson_strcasecmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO) ||
       !bson_strcasecmp (request->command_name, "hello")) {
      /* allow default response */
      return false;
   }

   reply_to_request_with_hang_up (request);
   request_destroy (request);
   return true;
}


static void
_test_transient_txn_err (bool hangup)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   mongoc_bulk_operation_t *bulk;
   mongoc_find_and_modify_opts_t *fam;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bson_t *b;
   bson_t *u;
   const bson_t *doc_out;
   const bson_t *error_doc;
   bson_t reply;
   bool r;

   server = mock_server_new ();
   mock_server_run (server);
   rs_response_to_hello (server, WIRE_VERSION_4_0, true /* primary */, false /* tags */, server, NULL);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   /* allow fast reconnect */
   client->topology->min_heartbeat_frequency_msec = 0;
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   r = mongoc_client_session_append (session, &opts, &error);
   ASSERT_OR_PRINT (r, error);
   collection = mongoc_client_get_collection (client, "db", "collection");

   if (hangup) {
      /* test that network errors have TransientTransactionError */
      mock_server_autoresponds (server, hangup_except_hello, NULL, NULL);
   } else {
      /* test server selection errors have TransientTransactionError */
      mock_server_destroy (server);
      server = NULL;
   }

   /* warnings when trying to abort the transaction and later, end sessions */
   capture_logs (true);

#define ASSERT_TRANSIENT_LABEL(_b, _expr)                                \
   do {                                                                  \
      if (!mongoc_error_has_label ((_b), "TransientTransactionError")) { \
         test_error ("Reply lacks TransientTransactionError label: %s\n" \
                     "Running %s",                                       \
                     bson_as_json ((_b), NULL),                          \
                     #_expr);                                            \
      }                                                                  \
   } while (0)

#define TEST_CMD_ERR(_expr)                   \
   do {                                       \
      r = (_expr);                            \
      BSON_ASSERT (!r);                       \
      ASSERT_TRANSIENT_LABEL (&reply, _expr); \
      bson_destroy (&reply);                  \
      /* clean slate for next test */         \
      memset (&reply, 0, sizeof (reply));     \
   } while (0)


#define TEST_WRITE_ERR(_expr)                 \
   do {                                       \
      r = (_expr);                            \
      ASSERT_TRANSIENT_LABEL (&reply, _expr); \
      bson_destroy (&reply);                  \
      /* clean slate for next test */         \
      memset (&reply, 0, sizeof (reply));     \
   } while (0)

#define TEST_CURSOR_ERR(_cursor_expr)                                 \
   do {                                                               \
      cursor = (_cursor_expr);                                        \
      r = mongoc_cursor_next (cursor, &doc_out);                      \
      BSON_ASSERT (!r);                                               \
      r = !mongoc_cursor_error_document (cursor, &error, &error_doc); \
      BSON_ASSERT (!r);                                               \
      BSON_ASSERT (error_doc);                                        \
      ASSERT_TRANSIENT_LABEL (error_doc, _cursor_expr);               \
      mongoc_cursor_destroy (cursor);                                 \
   } while (0)

   b = tmp_bson ("{'x': 1}");
   u = tmp_bson ("{'$inc': {'x': 1}}");

   TEST_CMD_ERR (mongoc_client_command_with_opts (client, "db", b, NULL, &opts, &reply, NULL));
   TEST_CMD_ERR (mongoc_client_read_command_with_opts (client, "db", b, NULL, &opts, &reply, NULL));
   TEST_CMD_ERR (mongoc_client_write_command_with_opts (client, "db", b, &opts, &reply, NULL));
   TEST_CMD_ERR (mongoc_client_read_write_command_with_opts (client, "db", b, NULL, &opts, &reply, NULL));
   TEST_CMD_ERR (0 < mongoc_collection_count_documents (collection, b, &opts, NULL, &reply, NULL));

   BEGIN_IGNORE_DEPRECATIONS
   TEST_CMD_ERR (mongoc_collection_create_index_with_opts (collection, b, NULL, &opts, &reply, NULL));
   END_IGNORE_DEPRECATIONS

   fam = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_append (fam, &opts);
   TEST_CMD_ERR (mongoc_collection_find_and_modify_with_opts (collection, b, fam, &reply, NULL));

   TEST_WRITE_ERR (mongoc_collection_insert_one (collection, b, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_insert_many (collection, (const bson_t **) &b, 1, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_update_one (collection, b, u, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_update_many (collection, b, u, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_replace_one (collection, b, b, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_delete_one (collection, b, &opts, &reply, NULL));
   TEST_WRITE_ERR (mongoc_collection_delete_many (collection, b, &opts, &reply, NULL));

   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, &opts);
   mongoc_bulk_operation_insert (bulk, b);
   TEST_WRITE_ERR (mongoc_bulk_operation_execute (bulk, &reply, NULL));

   TEST_CURSOR_ERR (mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson ("[{}]"), &opts, NULL));
   TEST_CURSOR_ERR (mongoc_collection_find_with_opts (collection, b, &opts, NULL));

   mongoc_find_and_modify_opts_destroy (fam);
   mongoc_bulk_operation_destroy (bulk);
   bson_destroy (&opts);
   mongoc_collection_destroy (collection);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);

   if (server) {
      mock_server_destroy (server);
   }
}


static void
test_server_selection_error (void)
{
   _test_transient_txn_err (false /* hangup */);
}


static void
test_network_error (void)
{
   _test_transient_txn_err (true /* hangup */);
}


/* Transactions Spec: Drivers add the "UnknownTransactionCommitResult" to a
 * server selection error from commitTransaction, even if this is the first
 * attempt to send commitTransaction. It is true in this case that the driver
 * knows the result: the transaction is definitely not committed. However, the
 * "UnknownTransactionCommitResult" label properly communicates to the
 * application that calling commitTransaction again may succeed.
 */
static void
test_unknown_commit_result (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_collection_t *collection;
   future_t *future;
   request_t *request;
   bson_t opts = BSON_INITIALIZER;
   bson_error_t error;
   bson_t reply;
   bool r;

   server = mock_server_new ();
   mock_server_run (server);
   rs_response_to_hello (server, WIRE_VERSION_4_0, true /* primary */, false /* tags */, server, NULL);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   /* allow fast reconnect */
   client->topology->min_heartbeat_frequency_msec = 0;
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   r = mongoc_client_session_append (session, &opts, &error);
   ASSERT_OR_PRINT (r, error);
   collection = mongoc_client_get_collection (client, "db", "collection");
   future = future_collection_insert_one (collection, tmp_bson ("{}"), &opts, NULL, &error);
   request = mock_server_receives_msg (server, 0, tmp_bson ("{'insert': 'collection'}"), tmp_bson ("{}"));
   reply_to_request_with_ok_and_destroy (request);
   ASSERT_OR_PRINT (future_get_bool (future), error);
   future_destroy (future);

   /* test server selection errors have UnknownTransactionCommitResult */
   mock_server_destroy (server);
   r = mongoc_client_session_commit_transaction (session, &reply, &error);
   BSON_ASSERT (!r);

   if (!mongoc_error_has_label (&reply, "UnknownTransactionCommitResult")) {
      test_error ("Reply lacks UnknownTransactionCommitResult label: %s", bson_as_json (&reply, NULL));
   }

   if (mongoc_error_has_label (&reply, "TransientTransactionError")) {
      test_error ("Reply shouldn't have TransientTransactionError label: %s", bson_as_json (&reply, NULL));
   }

   bson_destroy (&reply);
   bson_destroy (&opts);
   mongoc_collection_destroy (collection);

   /* warning when trying to end the session */
   capture_logs (true);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}


static void
test_cursor_primary_read_pref (void *ctx)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_t opts = BSON_INITIALIZER;
   mongoc_read_prefs_t *read_prefs;
   const bson_t *doc;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   collection = get_test_collection (client, "test_cursor_primary_read_pref");

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);

   r = mongoc_client_session_start_transaction (session, NULL, &error);
   ASSERT_OR_PRINT (r, error);

   r = mongoc_client_session_append (session, &opts, &error);
   ASSERT_OR_PRINT (r, error);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), &opts, read_prefs);

   bson_destroy (&opts);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_collection_destroy (collection);

   ASSERT (!mongoc_cursor_next (cursor, &doc));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   mongoc_cursor_destroy (cursor);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}


/* test the fix to CDRIVER-2815. */
void
test_inherit_from_client (void *ctx)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   bson_error_t error;
   mongoc_uri_t *uri;
   mongoc_read_concern_t *rc;
   const mongoc_read_concern_t *returned_rc;
   mongoc_read_prefs_t *rp;
   const mongoc_read_prefs_t *returned_rp;
   mongoc_write_concern_t *wc;
   const mongoc_write_concern_t *returned_wc;
   mongoc_session_opt_t *sopt;
   const mongoc_session_opt_t *returned_sopt;
   mongoc_transaction_opt_t *topt;
   const mongoc_transaction_opt_t *returned_topt;

   BSON_UNUSED (ctx);

   uri = test_framework_get_uri ();

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_MAJORITY);
   mongoc_uri_set_read_concern (uri, rc);

   rp = mongoc_read_prefs_new (MONGOC_READ_NEAREST);
   mongoc_uri_set_read_prefs_t (uri, rp);

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 0);
   mongoc_uri_set_write_concern (uri, wc);

   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);

   sopt = mongoc_session_opts_new ();
   topt = mongoc_transaction_opts_new ();

   mongoc_transaction_opts_set_read_concern (topt, rc);
   mongoc_transaction_opts_set_read_prefs (topt, rp);
   mongoc_transaction_opts_set_write_concern (topt, wc);

   mongoc_session_opts_set_default_transaction_opts (sopt, topt);

   session = mongoc_client_start_session (client, sopt, &error);
   ASSERT_OR_PRINT (session, error);

   /* test that unacknowledged write concern is actually used, since it should
    * result in an error. */
   ASSERT (!mongoc_client_session_start_transaction (session, NULL, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_TRANSACTION,
                          MONGOC_ERROR_TRANSACTION_INVALID_STATE,
                          "Transactions do not support unacknowledged write concern");

   returned_sopt = mongoc_client_session_get_opts (session);
   returned_topt = mongoc_session_opts_get_default_transaction_opts (returned_sopt);
   returned_rc = mongoc_transaction_opts_get_read_concern (returned_topt);
   returned_rp = mongoc_transaction_opts_get_read_prefs (returned_topt);
   returned_wc = mongoc_transaction_opts_get_write_concern (returned_topt);

   BSON_ASSERT (strcmp (mongoc_read_concern_get_level (returned_rc), mongoc_read_concern_get_level (rc)) == 0);
   BSON_ASSERT (mongoc_write_concern_get_w (returned_wc) == mongoc_write_concern_get_w (wc));
   BSON_ASSERT (mongoc_read_prefs_get_mode (returned_rp) == mongoc_read_prefs_get_mode (rp));

   mongoc_read_concern_destroy (rc);
   mongoc_read_prefs_destroy (rp);
   mongoc_write_concern_destroy (wc);
   mongoc_transaction_opts_destroy (topt);
   mongoc_session_opts_destroy (sopt);
   mongoc_client_session_destroy (session);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
}

void
test_transaction_fails_on_unsupported_version_or_sharded_cluster (void *ctx)
{
   bson_error_t error;
   mongoc_client_session_t *session;
   mongoc_client_t *client;
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);

   r = mongoc_client_session_start_transaction (session, NULL, &error);
   if (!test_framework_max_wire_version_at_least (7) ||
       (test_framework_is_mongos () && !test_framework_max_wire_version_at_least (8))) {
      BSON_ASSERT (!r);
      ASSERT_CONTAINS (error.message,
                       "Multi-document transactions are not supported by this "
                       "server version");
   } else {
      ASSERT_OR_PRINT (r, error);
   }

   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}


static void
test_transaction_recovery_token_cleared (void *ctx)
{
   bson_error_t error;
   mongoc_client_session_t *session;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_uri_t *uri;
   bson_t txn_opts;

   BSON_UNUSED (ctx);

   uri = test_framework_get_uri ();
   ASSERT_OR_PRINT (mongoc_uri_upsert_host_and_port (uri, "localhost:27018", &error), error);
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_uri_destroy (uri);
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   coll = get_test_collection (client, "transaction_test");

   mongoc_client_command_with_opts (client, "admin", tmp_bson ("{'killAllSessions': []}"), NULL, NULL, NULL, &error);
   /* Create the collection by inserting a canary document. You cannot create
    * inside a transaction */
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &error), error);

   bson_init (&txn_opts);
   ASSERT_OR_PRINT (mongoc_client_session_append (session, &txn_opts, &error), error);

   ASSERT_OR_PRINT (mongoc_client_session_start_transaction (session, NULL, &error), error);

   /* Initially no recovery token. */
   BSON_ASSERT (!session->recovery_token);
   mongoc_collection_insert_one (coll, tmp_bson ("{}"), &txn_opts, NULL, &error);
   BSON_ASSERT (session->recovery_token);
   ASSERT_OR_PRINT (mongoc_client_session_commit_transaction (session, NULL, &error), error);
   BSON_ASSERT (session->recovery_token);

   /* Starting a new transaction clears the recovery token. */
   ASSERT_OR_PRINT (mongoc_client_session_start_transaction (session, NULL, &error), error);
   BSON_ASSERT (!session->recovery_token);

   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), &txn_opts, NULL, &error), error);
   BSON_ASSERT (session->recovery_token);
   ASSERT_OR_PRINT (mongoc_client_session_commit_transaction (session, NULL, &error), error);
   BSON_ASSERT (session->recovery_token);

   /* Transitioning to the "none" state (i.e. a new operation outside of a
    * transaction), clears the recovery token */
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), &txn_opts, NULL, &error), error);
   BSON_ASSERT (!session->recovery_token);

   bson_destroy (&txn_opts);
   mongoc_collection_destroy (coll);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}

static void
test_selected_server_is_pinned_to_mongos (void *ctx)
{
   mongoc_uri_t *uri = NULL;
   mongoc_client_t *client = NULL;
   const mongoc_set_t *servers = NULL;
   mongoc_transaction_opt_t *txn_opts = NULL;
   mongoc_session_opt_t *session_opts = NULL;
   mongoc_client_session_t *session = NULL;
   mongoc_server_stream_t *server_stream = NULL;
   bson_error_t error;
   bson_t reply;
   bson_t *insert_opts = NULL;
   mongoc_database_t *db = NULL;
   mongoc_collection_t *coll = NULL;
   bool r;
   uint32_t expected_id;
   uint32_t actual_id;
   const mongoc_server_description_t *sd = NULL;

   BSON_UNUSED (ctx);

   uri = test_framework_get_uri ();
   ASSERT_OR_PRINT (mongoc_uri_upsert_host_and_port (uri, "localhost:27018", &error), error);

   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);
   test_framework_set_ssl_opts (client);

   txn_opts = mongoc_transaction_opts_new ();
   session_opts = mongoc_session_opts_new ();

   session = mongoc_client_start_session (client, session_opts, &error);
   ASSERT_OR_PRINT (session, error);

   /* set the server id to an arbitrary value */
   _mongoc_client_session_pin (session, 42);
   BSON_ASSERT (42 == mongoc_client_session_get_server_id (session));

   /* starting a transaction should clear the server id */
   r = mongoc_client_session_start_transaction (session, txn_opts, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (0 == mongoc_client_session_get_server_id (session));

   expected_id = mongoc_topology_select_server_id (client->topology, MONGOC_SS_WRITE, NULL, NULL, NULL, &error);
   ASSERT_OR_PRINT (expected_id, error);

   /* session should still be unpinned */
   BSON_ASSERT (0 == mongoc_client_session_get_server_id (session));

   /* should pin to the expected server id */
   server_stream = mongoc_cluster_stream_for_server (&client->cluster, expected_id, true, session, NULL, &error);
   ASSERT_OR_PRINT (server_stream, error);
   ASSERT_CMPINT32 (expected_id, ==, mongoc_client_session_get_server_id (session));

   db = mongoc_client_get_database (client, "db");
   coll = mongoc_database_create_collection (db, "coll", NULL, &error);

   insert_opts = bson_new ();
   r = mongoc_client_session_append (session, insert_opts, &error);
   ASSERT_OR_PRINT (r, error);

   /* this should not override the expected server id */
   r = mongoc_collection_insert_one (coll, tmp_bson ("{}"), insert_opts, NULL, &error);
   ASSERT_OR_PRINT (r, error);
   actual_id = mongoc_client_session_get_server_id (session);

   ASSERT_CMPINT32 (actual_id, ==, expected_id);

   /* get a valid server id that's different from the pinned server id */
   servers = mc_tpld_servers_const (mc_tpld_unsafe_get_const (client->topology));
   for (size_t i = 0; i < servers->items_len; i++) {
      sd = mongoc_set_get_item_const (servers, i);
      if (sd && sd->id != actual_id) {
         break;
      }
   }

   /* attempting to pin to a different but valid server id should fail */
   BSON_ASSERT (sd);
   r = mongoc_client_command_with_opts (
      client,
      "db",
      tmp_bson ("{'ping': 1}"),
      NULL,
      tmp_bson ("{'serverId': %d, 'sessionId': {'$numberLong': '%ld'}}", sd->id, session->client_session_id),
      &reply,
      &error);

   BSON_ASSERT (!r);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_SERVER_SELECTION_INVALID_ID,
                          "Requested server id does not matched pinned server id");

   r = mongoc_client_session_abort_transaction (session, &error);
   ASSERT_OR_PRINT (r, error);

   bson_destroy (insert_opts);
   bson_destroy (&reply);
   mongoc_collection_destroy (coll);
   mongoc_database_destroy (db);
   mongoc_session_opts_destroy (session_opts);
   mongoc_transaction_opts_destroy (txn_opts);
   mongoc_server_stream_cleanup (server_stream);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
}

static void
test_get_transaction_opts (void)
{
   mongoc_uri_t *uri = NULL;
   mongoc_client_t *client = NULL;
   mongoc_client_session_t *session = NULL;
   mongoc_transaction_opt_t *expected_txn_opts = NULL;
   mongoc_transaction_opt_t *actual_txn_opts = NULL;
   mongoc_session_opt_t *session_opts = NULL;
   mongoc_read_concern_t *read_concern = NULL;
   mongoc_write_concern_t *write_concern = NULL;
   mongoc_read_prefs_t *read_prefs = NULL;
   mock_server_t *server = NULL;
   int64_t max_commit_time_ms = 123; /* arbitrary */
   bson_error_t error;
   bool r;

   server = mock_server_new ();
   mock_server_run (server);
   rs_response_to_hello (server, WIRE_VERSION_4_0, true /* primary */, false /* tags */, server, NULL);

   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   BSON_ASSERT (client);

   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "snapshot");

   write_concern = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_MAJORITY);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   expected_txn_opts = mongoc_transaction_opts_new ();

   mongoc_transaction_opts_set_read_concern (expected_txn_opts, read_concern);
   mongoc_transaction_opts_set_write_concern (expected_txn_opts, write_concern);
   mongoc_transaction_opts_set_read_prefs (expected_txn_opts, read_prefs);
   mongoc_transaction_opts_set_max_commit_time_ms (expected_txn_opts, max_commit_time_ms);

   session_opts = mongoc_session_opts_new ();
   session = mongoc_client_start_session (client, session_opts, &error);
   ASSERT_OR_PRINT (session, error);
   /* outside of a txn this function should return NULL */
   BSON_ASSERT (!mongoc_session_opts_get_transaction_opts (session));

   r = mongoc_client_session_start_transaction (session, expected_txn_opts, &error);
   ASSERT_OR_PRINT (r, error);

   actual_txn_opts = mongoc_session_opts_get_transaction_opts (session);
   BSON_ASSERT (actual_txn_opts);
   BSON_ASSERT (0 == bson_compare (_mongoc_read_concern_get_bson (actual_txn_opts->read_concern),
                                   _mongoc_read_concern_get_bson (expected_txn_opts->read_concern)));

   BSON_ASSERT (0 == bson_compare (_mongoc_write_concern_get_bson (actual_txn_opts->write_concern),
                                   _mongoc_write_concern_get_bson (expected_txn_opts->write_concern)));

   BSON_ASSERT (mongoc_read_prefs_get_mode (actual_txn_opts->read_prefs) ==
                mongoc_read_prefs_get_mode (expected_txn_opts->read_prefs));

   BSON_ASSERT (actual_txn_opts->max_commit_time_ms == expected_txn_opts->max_commit_time_ms);

   r = mongoc_client_session_abort_transaction (session, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (!mongoc_session_opts_get_transaction_opts (session));

   mongoc_read_concern_destroy (read_concern);
   mongoc_write_concern_destroy (write_concern);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_transaction_opts_destroy (expected_txn_opts);
   mongoc_transaction_opts_destroy (actual_txn_opts);
   mongoc_session_opts_destroy (session_opts);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

static void
test_max_commit_time_ms_is_reset (void *ctx)
{
   mock_rs_t *rs;
   mongoc_uri_t *uri = NULL;
   mongoc_client_t *client = NULL;
   mongoc_transaction_opt_t *txn_opts = NULL;
   mongoc_session_opt_t *session_opts = NULL;
   mongoc_client_session_t *session = NULL;
   bson_error_t error;
   bool r;

   BSON_UNUSED (ctx);

   rs = mock_rs_with_auto_hello (WIRE_VERSION_4_2, true /* has primary */, 2 /* secondaries */, 0 /* arbiters */);

   mock_rs_run (rs);
   uri = mongoc_uri_copy (mock_rs_get_uri (rs));

   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);

   txn_opts = mongoc_transaction_opts_new ();
   session_opts = mongoc_session_opts_new ();

   session = mongoc_client_start_session (client, session_opts, &error);
   ASSERT_OR_PRINT (session, error);

   mongoc_transaction_opts_set_max_commit_time_ms (txn_opts, 1);

   r = mongoc_client_session_start_transaction (session, txn_opts, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (1 == session->txn.opts.max_commit_time_ms);

   r = mongoc_client_session_abort_transaction (session, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (DEFAULT_MAX_COMMIT_TIME_MS == session->txn.opts.max_commit_time_ms);

   mongoc_transaction_opts_set_max_commit_time_ms (txn_opts, DEFAULT_MAX_COMMIT_TIME_MS);

   r = mongoc_client_session_start_transaction (session, txn_opts, &error);
   ASSERT_OR_PRINT (r, error);
   BSON_ASSERT (DEFAULT_MAX_COMMIT_TIME_MS == session->txn.opts.max_commit_time_ms);

   r = mongoc_client_session_abort_transaction (session, &error);
   ASSERT_OR_PRINT (r, error);

   mongoc_session_opts_destroy (session_opts);
   mongoc_transaction_opts_destroy (txn_opts);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_rs_destroy (rs);
}

void
test_transactions_install (TestSuite *suite)
{
   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "transactions/legacy",
                                       test_transactions_cb,
                                       test_framework_skip_if_no_txns,
                                       test_framework_skip_if_slow);

   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "with_transaction",
                                       test_transactions_cb,
                                       test_framework_skip_if_no_txns,
                                       test_framework_skip_if_slow);

   TestSuite_AddFull (
      suite, "/transactions/supported", test_transactions_supported, NULL, NULL, test_framework_skip_if_no_txns);
   TestSuite_AddFull (
      suite, "/transactions/in_transaction", test_in_transaction, NULL, NULL, test_framework_skip_if_no_txns);
   TestSuite_AddMockServerTest (
      suite, "/transactions/server_selection_err", test_server_selection_error, test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (
      suite, "/transactions/network_err", test_network_error, test_framework_skip_if_no_crypto);
   TestSuite_AddMockServerTest (
      suite, "/transactions/unknown_commit_result", test_unknown_commit_result, test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/transactions/cursor_primary_read_pref",
                      test_cursor_primary_read_pref,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_txns);
   TestSuite_AddFull (
      suite, "/transactions/inherit_from_client", test_inherit_from_client, NULL, NULL, test_framework_skip_if_no_txns);
   TestSuite_AddFull (suite,
                      "/transactions/"
                      "transaction_fails_on_unsupported_version_or_sharded_cluster",
                      test_transaction_fails_on_unsupported_version_or_sharded_cluster,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/transactions/recovery_token_cleared",
                      test_transaction_recovery_token_cleared,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_crypto,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_not_mongos);
   TestSuite_AddFull (suite,
                      "/transactions/selected_server_pinned_to_mongos",
                      test_selected_server_is_pinned_to_mongos,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_not_mongos);
   TestSuite_AddMockServerTest (
      suite, "/transactions/get_transaction_opts", test_get_transaction_opts, test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/transactions/max_commit_time_ms_is_reset",
                      test_max_commit_time_ms_is_reset,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_crypto);
}
