#include <mongoc/mongoc.h>
#include "mongoc/mongoc-util-private.h"
#include "TestSuite.h"
#include "test-libmongoc.h"
#include <mock_server/mock-server.h>
#include <mock_server/future.h>
#include <mock_server/future-functions.h>
#include <test-conveniences.h> // tmp_bson


static void
test_mongoc_usleep_basic (void)
{
   int64_t start;
   int64_t duration;

   start = bson_get_monotonic_time ();
   _mongoc_usleep (50 * 1000); /* 50 ms */
   duration = bson_get_monotonic_time () - start;
   ASSERT_CMPINT ((int) duration, >, 0);
   ASSERT_CMPTIME ((int) duration, 200 * 1000);
}

static void
custom_usleep_impl (int64_t usec, void *user_data)
{
   if (user_data) {
      *(int64_t *) user_data = usec;
   }
}


// `test_mongoc_usleep_custom` tests a custom sleep function set in
// `mongoc_client_set_usleep_impl` is applied when topology scanning sleeps.
static void
test_mongoc_usleep_custom (void)
{
   mock_server_t *server = mock_server_new ();
   mock_server_run (server);

   mongoc_uri_t *uri = mongoc_uri_copy (mock_server_get_uri (server));
   // Tell single-threaded clients to reconnect if an error occcurs.
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false);

   mongoc_client_t *client = test_framework_client_new_from_uri (uri, NULL);
   ASSERT (client);

   // Bypass the five second cooldown to speed up test.
   _mongoc_topology_bypass_cooldown (client->topology);
   // Override `min_heartbeat_frequency_msec` to speed up test.
   client->topology->min_heartbeat_frequency_msec = 50;

   // Override the sleep.
   int64_t last_sleep_dur = 0;
   mongoc_client_set_usleep_impl (client, custom_usleep_impl, &last_sleep_dur);

   bson_error_t error;
   future_t *future =
      future_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL /* read prefs */, NULL, &error);

   // Client sends initial `isMaster`.
   {
      request_t *req = mock_server_receives_any_hello (server);
      ASSERT (req);
      // Fail the request.
      reply_to_request_with_hang_up (req);
      request_destroy (req);
   }

   // Client sleeps for `min_heartbeat_frequency_msec`, then sends another
   // `isMaster`.
   {
      request_t *req = mock_server_receives_any_hello (server);
      ASSERT (req);
      reply_to_request_simple (req,
                               tmp_str ("{ 'minWireVersion': %d, 'maxWireVersion' : %d, "
                                        "'isWritablePrimary': true}",
                                        WIRE_VERSION_MIN,
                                        WIRE_VERSION_MAX));
      request_destroy (req);
   }

   // Expect custom sleep to have been called between making `isMaster` calls.
   ASSERT_CMPINT64 (last_sleep_dur, >, 0);

   // Client sends "ping".
   {
      request_t *req = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'ping': 1}"));
      ASSERT (req);
      reply_to_request_with_ok_and_destroy (req);
   }

   bool ok = future_wait (future);
   ASSERT_OR_PRINT (ok, error);

   future_destroy (future);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mock_server_destroy (server);
}

void
test_usleep_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Sleep/basic", test_mongoc_usleep_basic);
   TestSuite_AddMockServerTest (suite, "/Sleep/custom", test_mongoc_usleep_custom);
}
