#include <mongoc/mongoc.h>

#include "json-test.h"
#include "mongoc/mongoc-client-session-private.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

/* Note, the with_transaction spec tests are in test-mongoc-transactions.c,
 * since it shares the same test runner with the transactions test runner. */

static bool
with_transaction_fail_transient_txn (mongoc_client_session_t *session, void *ctx, bson_t **reply, bson_error_t *error)
{
   bson_array_builder_t *labels;

   BSON_UNUSED (ctx);
   BSON_UNUSED (error);

   _mongoc_usleep (session->with_txn_timeout_ms * 1000);

   *reply = bson_new ();
   BSON_APPEND_ARRAY_BUILDER_BEGIN (*reply, "errorLabels", &labels);
   bson_array_builder_append_utf8 (labels, TRANSIENT_TXN_ERR, -1);
   bson_append_array_builder_end (*reply, labels);

   return false;
}

static bool
with_transaction_do_nothing (mongoc_client_session_t *session, void *ctx, bson_t **reply, bson_error_t *error)
{
   BSON_UNUSED (session);
   BSON_UNUSED (ctx);
   BSON_UNUSED (reply);
   BSON_UNUSED (error);
   return true;
}

static void
test_with_transaction_timeout (void *ctx)
{
   mongoc_client_t *client;
   mongoc_client_session_t *session;
   bson_error_t error;
   bool res;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);

   session->with_txn_timeout_ms = 10;

   /* Test Case 1: Test that if the callback returns an
      error with the TransientTransactionError label and
      we have exceeded the timeout, withTransaction fails. */
   res =
      mongoc_client_session_with_transaction (session, with_transaction_fail_transient_txn, NULL, NULL, NULL, &error);
   ASSERT (!res);

   /* Test Case 2: If committing returns an error with the
      UnknownTransactionCommitResult label and we have exceeded
      the timeout, withTransaction fails. */
   session->fail_commit_label = UNKNOWN_COMMIT_RESULT;
   res = mongoc_client_session_with_transaction (session, with_transaction_do_nothing, NULL, NULL, NULL, &error);
   ASSERT (!res);

   /* Test Case 3: If committing returns an error with the
      TransientTransactionError label and we have exceeded the
      timeout, withTransaction fails. */
   session->fail_commit_label = TRANSIENT_TXN_ERR;
   res = mongoc_client_session_with_transaction (session, with_transaction_do_nothing, NULL, NULL, NULL, &error);
   ASSERT (!res);

   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
}

void
test_with_transaction_install (TestSuite *suite)
{
   TestSuite_AddFull (suite,
                      "/with_transaction/timeout_tests",
                      test_with_transaction_timeout,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_crypto);
}
