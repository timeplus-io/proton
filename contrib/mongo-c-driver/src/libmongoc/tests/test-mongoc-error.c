#include <mongoc/mongoc.h>

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "mongoc/mongoc-error-private.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "error-test"


static void
test_set_error_api_single (void)
{
   mongoc_client_t *client;
   int32_t unsupported_versions[] = {-1, 0, 3};
   int i;

   capture_logs (true);
   client = test_framework_new_default_client ();

   for (i = 0; i < sizeof (unsupported_versions) / sizeof (int32_t); i++) {
      ASSERT (!mongoc_client_set_error_api (client, unsupported_versions[i]));
      ASSERT_CAPTURED_LOG ("mongoc_client_set_error_api", MONGOC_LOG_LEVEL_ERROR, "Unsupported Error API Version");
   }

   mongoc_client_destroy (client);
}


static void
test_set_error_api_pooled (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   int32_t unsupported_versions[] = {-1, 0, 3};
   int i;

   capture_logs (true);
   pool = test_framework_new_default_client_pool ();

   for (i = 0; i < sizeof (unsupported_versions) / sizeof (int32_t); i++) {
      ASSERT (!mongoc_client_pool_set_error_api (pool, unsupported_versions[i]));
      ASSERT_CAPTURED_LOG ("mongoc_client_pool_set_error_api", MONGOC_LOG_LEVEL_ERROR, "Unsupported Error API Version");
   }

   client = mongoc_client_pool_pop (pool);
   ASSERT (!mongoc_client_set_error_api (client, 1));
   ASSERT_CAPTURED_LOG (
      "mongoc_client_set_error_api", MONGOC_LOG_LEVEL_ERROR, "Cannot set Error API Version on a pooled client");

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
}


static void
_test_command_error (int32_t error_api_version)
{
   mock_server_t *server;
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;
   future_t *future;
   request_t *request;

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);

   if (error_api_version != 0) {
      BSON_ASSERT (mongoc_client_set_error_api (client, error_api_version));
   }

   future = future_client_command_simple (client, "db", tmp_bson ("{'foo': 1}"), NULL, &reply, &error);
   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'foo': 1}"));
   reply_to_request_simple (request, "{'ok': 0, 'code': 42, 'errmsg': 'foo'}");
   ASSERT (!future_get_bool (future));

   if (error_api_version >= 2) {
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 42, "foo");
   } else {
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_QUERY, 42, "foo");
   }

   future_destroy (future);
   request_destroy (request);
   bson_destroy (&reply);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static void
test_command_error_default (void)
{
   _test_command_error (0);
}


static void
test_command_error_v1 (void)
{
   _test_command_error (1);
}


static void
test_command_error_v2 (void)
{
   _test_command_error (2);
}

static void
test_has_label (void)
{
   bson_t *reply = tmp_bson ("{'errorLabels': ['foo', 'bar']}");
   BSON_ASSERT (mongoc_error_has_label (reply, "foo"));
   BSON_ASSERT (mongoc_error_has_label (reply, "bar"));
   BSON_ASSERT (!mongoc_error_has_label (reply, "baz"));
   BSON_ASSERT (!mongoc_error_has_label (tmp_bson ("{}"), "foo"));
}

static void
test_state_change_helper (uint32_t domain, bool expect_error)
{
   bson_error_t error;
   mongoc_server_err_t not_primary_codes[] = {
      MONGOC_SERVER_ERR_NOTPRIMARY, MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK, MONGOC_SERVER_ERR_LEGACYNOTPRIMARY};
   mongoc_server_err_t node_is_recovering_codes[] = {MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN,
                                                     MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE,
                                                     MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY,
                                                     MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN,
                                                     MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS};
   mongoc_server_err_t shutdown_codes[] = {MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN,
                                           MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS};
   int i;

   MONGOC_DEBUG ("Checking domain = %d", domain);

   memset (&error, 0, sizeof (bson_error_t));
   error.domain = domain;

   for (i = 0; i < sizeof (not_primary_codes) / sizeof (mongoc_server_err_t); i++) {
      error.code = not_primary_codes[i];
      BSON_ASSERT (expect_error == _mongoc_error_is_not_primary (&error));
      BSON_ASSERT (!_mongoc_error_is_recovering (&error));
      BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
      BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));
   }
   for (i = 0; i < sizeof (node_is_recovering_codes) / sizeof (mongoc_server_err_t); i++) {
      error.code = node_is_recovering_codes[i];
      BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
      BSON_ASSERT (expect_error == _mongoc_error_is_recovering (&error));
      BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));
   }
   for (i = 0; i < sizeof (shutdown_codes) / sizeof (mongoc_server_err_t); i++) {
      error.code = shutdown_codes[i];
      BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
      /* Shutdown errors are a subset of recovering errors. */
      BSON_ASSERT (expect_error == _mongoc_error_is_recovering (&error));
      BSON_ASSERT (expect_error == _mongoc_error_is_shutdown (&error));
      BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));
   }

   /* Fallback code that's used when no code was returned */
   error.code = MONGOC_ERROR_QUERY_FAILURE;
   bson_strncpy (error.message, "... not master ...", sizeof (error.message));
   BSON_ASSERT (expect_error == _mongoc_error_is_not_primary (&error));
   BSON_ASSERT (!_mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));

   bson_strncpy (error.message, "... node is recovering ...", sizeof (error.message));
   BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
   BSON_ASSERT (expect_error == _mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));

   bson_strncpy (error.message, "... not master or secondary ...", sizeof (error.message));
   BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
   BSON_ASSERT (expect_error == _mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (expect_error == _mongoc_error_is_state_change (&error));

   error.code = 123;
   bson_strncpy (error.message, "... not master ...", sizeof (error.message));
   BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
   BSON_ASSERT (!_mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (!_mongoc_error_is_state_change (&error));

   bson_strncpy (error.message, "... node is recovering ...", sizeof (error.message));
   BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
   BSON_ASSERT (!_mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (!_mongoc_error_is_state_change (&error));

   bson_strncpy (error.message, "... not master or secondary ...", sizeof (error.message));
   BSON_ASSERT (!_mongoc_error_is_not_primary (&error));
   BSON_ASSERT (!_mongoc_error_is_recovering (&error));
   BSON_ASSERT (!_mongoc_error_is_shutdown (&error));
   BSON_ASSERT (!_mongoc_error_is_state_change (&error));
}

static void
test_state_change (void)
{
   test_state_change_helper (MONGOC_ERROR_SERVER, true);
   test_state_change_helper (MONGOC_ERROR_WRITE_CONCERN, true);
   test_state_change_helper (MONGOC_ERROR_QUERY, false);
}

void
test_error_install (TestSuite *suite)
{
   TestSuite_AddLive (suite, "/Error/set_api/single", test_set_error_api_single);
   TestSuite_AddLive (suite, "/Error/set_api/pooled", test_set_error_api_pooled);
   TestSuite_AddMockServerTest (suite, "/Error/command/default", test_command_error_default);
   TestSuite_AddMockServerTest (suite, "/Error/command/v1", test_command_error_v1);
   TestSuite_AddMockServerTest (suite, "/Error/command/v2", test_command_error_v2);
   TestSuite_Add (suite, "/Error/has_label", test_has_label);
   TestSuite_Add (suite, "/Error/state_change", test_state_change);
}
