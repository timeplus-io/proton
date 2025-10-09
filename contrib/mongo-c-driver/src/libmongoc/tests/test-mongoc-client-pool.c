#include <mongoc/mongoc.h>
#include "mongoc/mongoc-client-pool-private.h"
#include <mongoc/mongoc-client-private.h>
#include "mongoc/mongoc-util-private.h"


#include "TestSuite.h"
#include "test-libmongoc.h"


static void
test_mongoc_client_pool_basic (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}


static void
test_mongoc_client_pool_try_pop (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (!mongoc_client_pool_try_pop (pool));
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

static void
test_mongoc_client_pool_pop_timeout (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   int64_t start;
   int64_t duration_usec;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1&waitqueuetimeoutms=2000");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   start = bson_get_monotonic_time ();
   BSON_ASSERT (!mongoc_client_pool_pop (pool));
   duration_usec = bson_get_monotonic_time () - start;
   /* There is a possibility that the wait is a few milliseconds short.  The
    * assertion is structured like this since the timeout is a rough lower bound
    * and some test environments might slow things down. */
   BSON_ASSERT (duration_usec / 1000 >= 1990);
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

static void
test_mongoc_client_pool_min_size_zero (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client1;
   mongoc_client_t *client2;
   mongoc_client_t *client3;
   mongoc_client_t *client4;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new (NULL);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   client1 = mongoc_client_pool_pop (pool);
   client2 = mongoc_client_pool_pop (pool);
   mongoc_client_pool_push (pool, client2);
   mongoc_client_pool_push (pool, client1);

   BSON_ASSERT (mongoc_client_pool_get_size (pool) == 2);
   client3 = mongoc_client_pool_pop (pool);

   /* min pool size zero means "no min", so clients weren't destroyed */
   BSON_ASSERT (client3 == client1);
   client4 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client4 == client2);

   mongoc_client_pool_push (pool, client4);
   mongoc_client_pool_push (pool, client3);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_client_pool_min_size_dispose (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_client_t *c0, *c1, *c2, *c3;

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://127.0.0.1/?minpoolsize=2");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   c0 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c0);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 1);

   c1 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c1);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   c2 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 3);

   c3 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c3);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c0); /* queue is [c0] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 1);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c1); /* queue is [c1, c0] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c2); /* queue is [c2, c1] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 3);

   mongoc_client_pool_push (pool, c3); /* queue is [c3, c2] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   /* BSON_ASSERT oldest client was destroyed, newest were stored */
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (client == c3);

   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (client == c2);

   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   /* clean up */
   mongoc_client_pool_push (pool, c2);
   mongoc_client_pool_push (pool, c3);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_client_pool_set_max_size (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_array_t conns;
   int i;

   _mongoc_array_init (&conns, sizeof client);

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=10");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   for (i = 0; i < 5; i++) {
      client = mongoc_client_pool_pop (pool);
      BSON_ASSERT (client);
      _mongoc_array_append_val (&conns, client);
      BSON_ASSERT (mongoc_client_pool_get_size (pool) == i + 1);
   }

   mongoc_client_pool_max_size (pool, 3);

   BSON_ASSERT (mongoc_client_pool_try_pop (pool) == NULL);

   for (i = 0; i < 5; i++) {
      client = _mongoc_array_index (&conns, mongoc_client_t *, i);
      BSON_ASSERT (client);
      mongoc_client_pool_push (pool, client);
   }

   _mongoc_array_clear (&conns);
   _mongoc_array_destroy (&conns);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

static void
test_mongoc_client_pool_set_min_size (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_array_t conns;
   int i;

   _mongoc_array_init (&conns, sizeof client);

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=10&minpoolsize=3");
   capture_logs (true);
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   ASSERT_CAPTURED_LOG ("minpoolsize URI option", MONGOC_LOG_LEVEL_WARNING, "is deprecated");

   for (i = 0; i < 10; i++) {
      client = mongoc_client_pool_pop (pool);
      BSON_ASSERT (client);
      _mongoc_array_append_val (&conns, client);
      BSON_ASSERT (mongoc_client_pool_get_size (pool) == i + 1);
   }

   capture_logs (true);
   BEGIN_IGNORE_DEPRECATIONS
   mongoc_client_pool_min_size (pool, 7);
   END_IGNORE_DEPRECATIONS
   ASSERT_CAPTURED_LOG (
      "mongoc_client_pool_min_size", MONGOC_LOG_LEVEL_WARNING, "mongoc_client_pool_min_size is deprecated");

   for (i = 0; i < 10; i++) {
      client = _mongoc_array_index (&conns, mongoc_client_t *, i);
      BSON_ASSERT (client);
      mongoc_client_pool_push (pool, client);
   }

   BSON_ASSERT (mongoc_client_pool_get_size (pool) == 7);

   _mongoc_array_clear (&conns);
   _mongoc_array_destroy (&conns);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

#ifndef MONGOC_ENABLE_SSL
static void
test_mongoc_client_pool_ssl_disabled (void)
{
   mongoc_uri_t *uri = mongoc_uri_new ("mongodb://host/?ssl=true");

   ASSERT (uri);
   capture_logs (true);
   ASSERT (NULL == test_framework_client_pool_new_from_uri (uri, NULL));
   ASSERT_CAPTURED_LOG ("mongoc_client_pool_new", MONGOC_LOG_LEVEL_ERROR, "SSL not enabled in this build.");

   mongoc_uri_destroy (uri);
}
#endif

static void
test_mongoc_client_pool_handshake (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);


   ASSERT (mongoc_client_pool_set_appname (pool, "some application"));
   /* Be sure we can't set it twice */
   capture_logs (true);
   ASSERT (!mongoc_client_pool_set_appname (pool, "a"));
   ASSERT_CAPTURED_LOG (
      "_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "Cannot set appname more than once");
   capture_logs (false);

   mongoc_client_pool_destroy (pool);

   /* Make sure that after we pop a client we can't set handshake anymore */
   pool = test_framework_client_pool_new_from_uri (uri, NULL);

   client = mongoc_client_pool_pop (pool);

   /* Be sure a client can't set it now that we've popped them */
   capture_logs (true);
   ASSERT (!mongoc_client_set_appname (client, "a"));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname",
                        MONGOC_LOG_LEVEL_ERROR,
                        "Cannot call set_appname on a client from a pool");
   capture_logs (false);

   mongoc_client_pool_push (pool, client);

   /* even now that we pushed the client back we shouldn't be able to set
    * the handshake */
   capture_logs (true);
   ASSERT (!mongoc_client_pool_set_appname (pool, "a"));
   ASSERT_CAPTURED_LOG (
      "_mongoc_topology_scanner_set_appname", MONGOC_LOG_LEVEL_ERROR, "Cannot set appname after handshake initiated");
   capture_logs (false);

   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

/* Test that destroying a pool without pushing all clients is ok. */
static void
test_client_pool_destroy_without_pushing (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client_in_pool;
   mongoc_client_t *client1;
   mongoc_client_t *client2;
   bson_error_t error;
   bson_t *cmd;
   bool ret;

   cmd = BCON_NEW ("ping", BCON_INT32 (1));
   pool = test_framework_new_default_client_pool ();
   client1 = mongoc_client_pool_pop (pool);
   client2 = mongoc_client_pool_pop (pool);

   /* Push a client back onto the pool so endSessions succeeds to avoid a
    * warning. */
   client_in_pool = mongoc_client_pool_pop (pool);
   ret = mongoc_client_command_simple (client_in_pool, "admin", cmd, NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_client_pool_push (pool, client_in_pool);


   ret = mongoc_client_command_simple (client1, "admin", cmd, NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
   ret = mongoc_client_command_simple (client2, "admin", cmd, NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Since clients are checked out of pool, it is technically ok to
    * mongoc_client_destroy them instead of pushing. */
   mongoc_client_destroy (client1);

   /* An operation on client2 should still be ok. */
   ret = mongoc_client_command_simple (client2, "admin", cmd, NULL /* read prefs */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_client_destroy (client2);

   /* Destroy the pool, which destroys the shared topology object. */
   mongoc_client_pool_destroy (pool);

   bson_destroy (cmd);
}

static void
command_started_cb (const mongoc_apm_command_started_t *event)
{
   if (strcmp (mongoc_apm_command_started_get_command_name (event), "endSessions") != 0) {
      return;
   }

   int *const count = (int *) mongoc_apm_command_started_get_context (event);
   (*count)++;
}

/* tests that creating and destroying an unused session
 * in pooled mode does not result in an error log */
static void
test_client_pool_create_unused_session (void *context)
{
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;
   mongoc_client_session_t *session;
   mongoc_apm_callbacks_t *callbacks;
   bson_error_t error;
   int count = 0;

   BSON_UNUSED (context);

   capture_logs (true);

   callbacks = mongoc_apm_callbacks_new ();
   pool = test_framework_new_default_client_pool ();
   client = mongoc_client_pool_pop (pool);
   session = mongoc_client_start_session (client, NULL, &error);

   mongoc_apm_set_command_started_cb (callbacks, command_started_cb);
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, &count);

   mongoc_client_session_destroy (session);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_apm_callbacks_destroy (callbacks);
   ASSERT_CMPINT (count, ==, 0);
   ASSERT_NO_CAPTURED_LOGS ("mongoc_client_pool_destroy");
}


/* Tests case where thread is blocked waiting for a client to be pushed back
 * into the client pool.  Specifically this tests that the program terminates.
 * Addresses CDRIVER-3757 */
typedef struct pool_timeout {
   mongoc_client_pool_t *pool;
   bson_mutex_t mutex;
   mongoc_cond_t cond;
   int nleft;
} pool_timeout_args_t;

static BSON_THREAD_FUN (worker, arg)
{
   pool_timeout_args_t *args = arg;
   mongoc_client_t *client = mongoc_client_pool_pop (args->pool);
   BSON_ASSERT (client);
   _mongoc_usleep (10);
   mongoc_client_pool_push (args->pool, client);
   bson_mutex_lock (&args->mutex);
   /* notify main thread that current thread has terminated */
   args->nleft--;
   mongoc_cond_signal (&args->cond);
   bson_mutex_unlock (&args->mutex);
   BSON_THREAD_RETURN;
}

static void
test_client_pool_max_pool_size_exceeded (void)
{
   mongoc_client_pool_t *pool;
   mongoc_uri_t *uri;
   bson_thread_t thread1, thread2;
   pool_timeout_args_t *args = bson_malloc0 (sizeof (pool_timeout_args_t));
   int wait_time = 4000; /* 4000 msec = 4 sec */
   int ret;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   args->pool = pool;
   args->nleft = 2;
   bson_mutex_init (&args->mutex);
   mongoc_cond_init (&args->cond);

   ASSERT_CMPINT (0, ==, mcommon_thread_create (&thread1, worker, args));
   ASSERT_CMPINT (0, ==, mcommon_thread_create (&thread2, worker, args));

   bson_mutex_lock (&args->mutex);
   while (args->nleft > 0) {
      ret = mongoc_cond_timedwait (&args->cond, &args->mutex, wait_time);
      /* ret non-zero indicates an error (a timeout) */
      BSON_ASSERT (!ret);
   }
   bson_mutex_unlock (&args->mutex);

   mcommon_thread_join (thread1);
   mcommon_thread_join (thread2);

   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
   bson_free (args);
}

static void
test_client_pool_can_override_sockettimeoutms (void)
{
   mongoc_uri_t *uri = mongoc_uri_new ("mongodb://localhost:27017/?socketTimeoutMS=1000");
   mongoc_client_pool_t *pool = mongoc_client_pool_new (uri);

   // Override the client's socketTimeoutMS.
   {
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      ASSERT_CMPINT32 (client->cluster.sockettimeoutms, ==, 1000);
      mongoc_client_set_sockettimeoutms (client, 2000);
      ASSERT_CMPINT32 (client->cluster.sockettimeoutms, ==, 2000);
      mongoc_client_pool_push (pool, client);
   }

   // Pop again. Expect the newly popped client to have the socketTimeoutMS from the URI.
   {
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      ASSERT_CMPINT32 (client->cluster.sockettimeoutms, ==, 1000);
      mongoc_client_pool_push (pool, client);
   }

   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

// Test connections to removed servers are closed when a client is pushed back to the pool.
static void
disconnects_removed_servers_on_push (void *unused)
{
   BSON_UNUSED (unused);
   bson_error_t error;
   bool ok;
   bson_t *ping = BCON_NEW ("ping", BCON_INT32 (1));

   // Create a client pool to two servers.
   mongoc_client_pool_t *pool;
   {
      mongoc_uri_t *uri = mongoc_uri_new ("mongodb://localhost:27017,localhost:27018");
      // Set a short heartbeat so server monitors get quick responses.
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS);
      pool = mongoc_client_pool_new (uri);
      test_framework_set_pool_ssl_opts (pool);
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

   // Mock removal of server 27018 from topology.
   {
      mongoc_topology_t *tp = _mongoc_client_pool_get_topology (pool);
      mc_tpld_modification tdmod = mc_tpld_modify_begin (tp);
      mongoc_set_rm (mc_tpld_servers (tdmod.new_td), 2 /* server ID */);
      mc_tpld_modify_commit (tdmod);
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
   bson_destroy (ping);
}

// Test that connections are closed to servers removed from the topology on clients checked into the pool.
static void
disconnects_removed_servers_in_pool (void *unused)
{
   BSON_UNUSED (unused);
   bson_error_t error;
   bool ok;
   bson_t *ping = BCON_NEW ("ping", BCON_INT32 (1));

   // Create a client pool to two servers.
   mongoc_client_pool_t *pool;
   {
      mongoc_uri_t *uri = mongoc_uri_new ("mongodb://localhost:27017,localhost:27018");
      // Set a short heartbeat so server monitors get quick responses.
      mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, MONGOC_TOPOLOGY_MIN_HEARTBEAT_FREQUENCY_MS);
      pool = mongoc_client_pool_new (uri);
      test_framework_set_pool_ssl_opts (pool);
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

   // Send 'ping' commands on two clients to each server to create operation connections.
   {
      mongoc_client_t *client1 = mongoc_client_pool_pop (pool);
      mongoc_client_t *client2 = mongoc_client_pool_pop (pool);

      ok = mongoc_client_command_simple_with_server_id (client1, "admin", ping, NULL, 1 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);
      ok = mongoc_client_command_simple_with_server_id (client1, "admin", ping, NULL, 2 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);

      ok = mongoc_client_command_simple_with_server_id (client2, "admin", ping, NULL, 1 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);
      ok = mongoc_client_command_simple_with_server_id (client2, "admin", ping, NULL, 2 /* server ID */, NULL, &error);
      ASSERT_OR_PRINT (ok, error);

      mongoc_client_pool_push (pool, client2);
      mongoc_client_pool_push (pool, client1);

      // Expect an operation connection is created per client.
      ASSERT_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 2);
      ASSERT_CONN_COUNT ("localhost:27018", conns_27018_before + 2 + 2);
   }

   // Mock removal of server 27018 from topology.
   {
      mongoc_topology_t *tp = _mongoc_client_pool_get_topology (pool);
      mc_tpld_modification tdmod = mc_tpld_modify_begin (tp);
      mongoc_set_rm (mc_tpld_servers (tdmod.new_td), 2 /* server ID */);
      mc_tpld_modify_commit (tdmod);
   }

   // Expect connections are closed to removed server.
   {
      // Expect monitoring connections to be closed in background.
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 2);
      ASSERT_EVENTUAL_CONN_COUNT ("localhost:27018", conns_27018_before + 2);

      // Pop and push one client to "prune" the stale operation connections for both clients.
      mongoc_client_t *client = mongoc_client_pool_pop (pool);
      mongoc_client_pool_push (pool, client);
      ASSERT_CONN_COUNT ("localhost:27017", conns_27017_before + 2 + 2);
      ASSERT_CONN_COUNT ("localhost:27018", conns_27018_before);
   }

   mongoc_client_pool_destroy (pool);
   bson_destroy (ping);
}

void
test_client_pool_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/ClientPool/basic", test_mongoc_client_pool_basic);
   TestSuite_Add (suite, "/ClientPool/try_pop", test_mongoc_client_pool_try_pop);
   TestSuite_Add (suite, "/ClientPool/pop_timeout", test_mongoc_client_pool_pop_timeout);
   TestSuite_Add (suite, "/ClientPool/min_size_zero", test_mongoc_client_pool_min_size_zero);
   TestSuite_Add (suite, "/ClientPool/min_size_dispose", test_mongoc_client_pool_min_size_dispose);
   TestSuite_Add (suite, "/ClientPool/set_max_size", test_mongoc_client_pool_set_max_size);
   TestSuite_Add (suite, "/ClientPool/set_min_size", test_mongoc_client_pool_set_min_size);

   TestSuite_Add (suite, "/ClientPool/handshake", test_mongoc_client_pool_handshake);

   TestSuite_AddFull (suite,
                      "/ClientPool/create_client_pool_unused_session",
                      test_client_pool_create_unused_session,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_sessions);
#ifndef MONGOC_ENABLE_SSL
   TestSuite_Add (suite, "/ClientPool/ssl_disabled", test_mongoc_client_pool_ssl_disabled);
#endif
   TestSuite_AddLive (suite, "/ClientPool/destroy_without_push", test_client_pool_destroy_without_pushing);
   TestSuite_AddLive (suite, "/ClientPool/max_pool_size_exceeded", test_client_pool_max_pool_size_exceeded);
   TestSuite_Add (suite, "/ClientPool/can_override_sockettimeoutms", test_client_pool_can_override_sockettimeoutms);

   TestSuite_AddFull (
      suite,
      "/client_pool/disconnects_removed_servers/on_push",
      disconnects_removed_servers_on_push,
      NULL,
      NULL,
      test_framework_skip_if_not_mongos /* require mongos to ensure two servers available */,
      test_framework_skip_if_max_wire_version_less_than_9 /* require server 4.4+ for streaming monitoring protocol */);

   TestSuite_AddFull (
      suite,
      "/client_pool/disconnects_removed_servers/in_pool",
      disconnects_removed_servers_in_pool,
      NULL,
      NULL,
      test_framework_skip_if_not_mongos /* require mongos to ensure two servers available */,
      test_framework_skip_if_max_wire_version_less_than_9 /* require server 4.4+ for streaming monitoring protocol */);
}
