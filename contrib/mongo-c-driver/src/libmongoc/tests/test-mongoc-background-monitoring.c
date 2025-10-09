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

#include "mock_server/mock-server.h"
#include "mongoc/mongoc.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-handshake-private.h"
#include "mongoc/mongoc-server-description-private.h"
#include "mongoc/mongoc-topology-background-monitoring-private.h"
#include "mongoc/mongoc-topology-description-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"

#define LOG_DOMAIN "test_monitoring"

typedef struct {
   uint32_t n_heartbeat_started;
   uint32_t n_heartbeat_succeeded;
   uint32_t n_heartbeat_failed;
   uint32_t n_server_changed;
   mongoc_topology_description_type_t td_type;
   mongoc_server_description_type_t sd_type;
   bool awaited;
} tf_observations_t;

typedef enum {
   TF_FAST_HEARTBEAT = 1 << 0,
   TF_FAST_MIN_HEARTBEAT = 1 << 1,
   TF_AUTO_RESPOND_POLLING_HELLO = 1 << 2,
   TF_NO_MONGODB_API_VERSION = 1 << 3, /* if set, do not pick up the MONGODB_API_VERSION environment
                                          variable */
} tf_flags_t;

typedef struct {
   tf_flags_t flags;
   mock_server_t *server;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   tf_observations_t *observations;
   bson_mutex_t mutex;
   mongoc_cond_t cond;
   bson_string_t *logs;
} test_fixture_t;

void
tf_dump (test_fixture_t *tf)
{
   printf ("== Begin dump ==\n");
   printf ("-- Current observations --\n");
   printf ("n_heartbeat_started=%d\n", tf->observations->n_heartbeat_started);
   printf ("n_heartbeat_succeeded=%d\n", tf->observations->n_heartbeat_succeeded);
   printf ("n_heartbeat_failed=%d\n", tf->observations->n_heartbeat_failed);
   printf ("n_server_changed=%d\n", tf->observations->n_server_changed);
   printf ("sd_type=%d\n", tf->observations->sd_type);

   printf ("-- Test fixture logs --\n");
   printf ("%s", tf->logs->str);
   printf ("== End dump ==\n");
}

void BSON_GNUC_PRINTF (2, 3) tf_log (test_fixture_t *tf, const char *format, ...)
{
   va_list ap;
   char *str;
   char nowstr[32];
   struct timeval tv;
   struct tm tt;
   time_t t;

   bson_gettimeofday (&tv);
   t = tv.tv_sec;

#ifdef _WIN32
#ifdef _MSC_VER
   localtime_s (&tt, &t);
#else
   tt = *(localtime (&t));
#endif
#else
   localtime_r (&t, &tt);
#endif

   strftime (nowstr, sizeof nowstr, "%Y/%m/%d %H:%M:%S ", &tt);

   va_start (ap, format);
   str = bson_strdupv_printf (format, ap);
   va_end (ap);
   bson_string_append (tf->logs, nowstr);
   bson_string_append (tf->logs, str);
   bson_string_append_c (tf->logs, '\n');
   bson_free (str);
}

#define TF_LOG(_tf, ...) tf_log (_tf, __VA_ARGS__)

static void
_heartbeat_started (const mongoc_apm_server_heartbeat_started_t *event)
{
   test_fixture_t *tf;

   tf = (test_fixture_t *) mongoc_apm_server_heartbeat_started_get_context (event);
   bson_mutex_lock (&tf->mutex);
   tf->observations->n_heartbeat_started++;
   tf->observations->awaited = mongoc_apm_server_heartbeat_started_get_awaited (event);
   TF_LOG (tf, "%s heartbeat started", tf->observations->awaited ? "awaitable" : "polling");
   mongoc_cond_broadcast (&tf->cond);
   bson_mutex_unlock (&tf->mutex);
}

static void
_heartbeat_succeeded (const mongoc_apm_server_heartbeat_succeeded_t *event)
{
   test_fixture_t *tf;

   tf = (test_fixture_t *) mongoc_apm_server_heartbeat_succeeded_get_context (event);
   bson_mutex_lock (&tf->mutex);
   tf->observations->n_heartbeat_succeeded++;
   tf->observations->awaited = mongoc_apm_server_heartbeat_succeeded_get_awaited (event);
   TF_LOG (tf, "%s heartbeat succeeded", tf->observations->awaited ? "awaitable" : "polling");
   mongoc_cond_broadcast (&tf->cond);
   bson_mutex_unlock (&tf->mutex);
}

static void
_heartbeat_failed (const mongoc_apm_server_heartbeat_failed_t *event)
{
   test_fixture_t *tf;

   tf = (test_fixture_t *) mongoc_apm_server_heartbeat_failed_get_context (event);
   bson_mutex_lock (&tf->mutex);
   tf->observations->n_heartbeat_failed++;
   tf->observations->awaited = mongoc_apm_server_heartbeat_failed_get_awaited (event);
   TF_LOG (tf, "%s heartbeat failed", tf->observations->awaited ? "awaitable" : "polling");
   mongoc_cond_broadcast (&tf->cond);
   bson_mutex_unlock (&tf->mutex);
}

static void
_server_changed (const mongoc_apm_server_changed_t *event)
{
   test_fixture_t *tf;
   const mongoc_server_description_t *old_sd;
   const mongoc_server_description_t *new_sd;

   tf = (test_fixture_t *) mongoc_apm_server_changed_get_context (event);
   old_sd = mongoc_apm_server_changed_get_previous_description (event);
   new_sd = mongoc_apm_server_changed_get_new_description (event);
   bson_mutex_lock (&tf->mutex);
   TF_LOG (
      tf, "server changed %s => %s", mongoc_server_description_type (old_sd), mongoc_server_description_type (new_sd));
   tf->observations->sd_type = new_sd->type;
   tf->observations->n_server_changed++;
   mongoc_cond_broadcast (&tf->cond);
   bson_mutex_unlock (&tf->mutex);
}

#define TV "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }"

bool
auto_respond_polling_hello (request_t *request, void *ctx)
{
   BSON_UNUSED (ctx);

   if (0 == strcasecmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO) ||
       0 == strcmp (request->command_name, "hello")) {
      const bson_t *doc;

      doc = request_get_doc ((request), 0);
      if (!bson_has_field (doc, "topologyVersion")) {
         reply_to_request_simple (request, "{'ok': 1, 'topologyVersion': " TV " }");
         request_destroy (request);
         return true;
      }
   }
   return false;
}

#define FAST_HEARTBEAT_MS 10

test_fixture_t *
tf_new (tf_flags_t flags)
{
   mongoc_apm_callbacks_t *callbacks;
   test_fixture_t *tf;

   tf = bson_malloc0 (sizeof (test_fixture_t));
   tf->observations = bson_malloc0 (sizeof (tf_observations_t));
   bson_mutex_init (&tf->mutex);
   mongoc_cond_init (&tf->cond);

   callbacks = mongoc_apm_callbacks_new ();
   tf->server = mock_server_new ();
   mock_server_run (tf->server);

   mongoc_apm_set_server_heartbeat_started_cb (callbacks, _heartbeat_started);
   mongoc_apm_set_server_changed_cb (callbacks, _server_changed);
   mongoc_apm_set_server_heartbeat_succeeded_cb (callbacks, _heartbeat_succeeded);
   mongoc_apm_set_server_heartbeat_failed_cb (callbacks, _heartbeat_failed);

   if (flags & TF_NO_MONGODB_API_VERSION) {
      tf->pool = mongoc_client_pool_new (mock_server_get_uri (tf->server));
   } else {
      tf->pool = test_framework_client_pool_new_from_uri (mock_server_get_uri (tf->server), NULL);
   }

   mongoc_client_pool_set_apm_callbacks (tf->pool, callbacks, tf);
   mongoc_apm_callbacks_destroy (callbacks);

   if (flags & TF_FAST_HEARTBEAT) {
      mc_tpld_modification tdmod = mc_tpld_modify_begin (_mongoc_client_pool_get_topology (tf->pool));
      tdmod.new_td->heartbeat_msec = FAST_HEARTBEAT_MS;
      mc_tpld_modify_commit (tdmod);
      /* A fast heartbeat implies a fast min heartbeat. */
      flags |= TF_FAST_MIN_HEARTBEAT;
   }

   if (flags & TF_FAST_MIN_HEARTBEAT) {
      _mongoc_client_pool_get_topology (tf->pool)->min_heartbeat_frequency_msec = FAST_HEARTBEAT_MS;
   }

   if (flags & TF_AUTO_RESPOND_POLLING_HELLO) {
      mock_server_autoresponds (tf->server, auto_respond_polling_hello, NULL, NULL);
   }
   tf->flags = flags;
   tf->logs = bson_string_new ("");
   tf->client = mongoc_client_pool_pop (tf->pool);
   return tf;
}

void
tf_destroy (test_fixture_t *tf)
{
   mock_server_destroy (tf->server);
   mongoc_client_pool_push (tf->pool, tf->client);
   mongoc_client_pool_destroy (tf->pool);
   bson_string_free (tf->logs, true);
   bson_mutex_destroy (&tf->mutex);
   mongoc_cond_destroy (&tf->cond);
   bson_free (tf->observations);
   bson_free (tf);
}

/* Wait for _predicate to become true over the next five seconds.
 * _predicate is only tested when observations change.
 * Upon failure, dumps logs and observations.
 */
#define OBSERVE_SOON(_tf, _predicate)                                              \
   do {                                                                            \
      int64_t _start_ms = bson_get_monotonic_time () / 1000;                       \
      int64_t _expires_ms = _start_ms + 5000;                                      \
      bson_mutex_lock (&_tf->mutex);                                               \
      while (!(_predicate)) {                                                      \
         if (bson_get_monotonic_time () / 1000 > _expires_ms) {                    \
            bson_mutex_unlock (&_tf->mutex);                                       \
            tf_dump (_tf);                                                         \
            test_error ("Predicate expired: %s", #_predicate);                     \
         }                                                                         \
         mongoc_cond_timedwait (&_tf->cond, &_tf->mutex, _expires_ms - _start_ms); \
      }                                                                            \
      bson_mutex_unlock (&_tf->mutex);                                             \
   } while (0)

/* Check that _predicate is true immediately. Upon failure,
 * dumps logs and observations. */
#define OBSERVE(_tf, _predicate)                           \
   do {                                                    \
      bson_mutex_lock (&_tf->mutex);                       \
      if (!(_predicate)) {                                 \
         tf_dump (_tf);                                    \
         bson_mutex_unlock (&_tf->mutex);                  \
         test_error ("Predicate failed: %s", #_predicate); \
      }                                                    \
      bson_mutex_unlock (&_tf->mutex);                     \
   } while (0)

/* Wait for two periods of the faster heartbeat.
 * Used to make observations that a scan doesn't occur when a test fixture is
 * configured with a faster heartbeat.
 */
#define WAIT_TWO_MIN_HEARTBEAT_MS _mongoc_usleep (2 * FAST_HEARTBEAT_MS * 1000)

static void
_signal_shutdown (test_fixture_t *tf)
{
   mc_tpld_modification tdmod = mc_tpld_modify_begin (tf->client->topology);
   /* Ignore the "Last server removed from topology" warning. */
   capture_logs (true);
   /* remove the server description from the topology description. */
   mongoc_topology_description_reconcile (tdmod.new_td, NULL);
   capture_logs (false);
   /* remove the server monitor from the set of server monitors. */
   _mongoc_topology_background_monitoring_reconcile (tf->client->topology, tdmod.new_td);
   mc_tpld_modify_commit (tdmod);
}

static void
_add_server_monitor (test_fixture_t *tf)
{
   uint32_t id;
   const mongoc_uri_t *uri;
   mc_tpld_modification tdmod = mc_tpld_modify_begin (tf->client->topology);

   uri = mock_server_get_uri (tf->server);
   /* remove the server description from the topology description. */
   mongoc_topology_description_add_server (tdmod.new_td, mongoc_uri_get_hosts (uri)->host_and_port, &id);
   /* add the server monitor from the set of server monitors. */
   _mongoc_topology_background_monitoring_reconcile (tf->client->topology, tdmod.new_td);
   mc_tpld_modify_commit (tdmod);
}

static void
_request_scan (test_fixture_t *tf)
{
   bson_mutex_lock (&tf->client->topology->tpld_modification_mtx);
   _mongoc_topology_request_scan (tf->client->topology);
   bson_mutex_unlock (&tf->client->topology->tpld_modification_mtx);
}

static void
_request_cancel (test_fixture_t *tf)
{
   bson_mutex_lock (&tf->client->topology->tpld_modification_mtx);
   /* Assume server id is 1. */
   _mongoc_topology_background_monitoring_cancel_check (tf->client->topology, 1);
   bson_mutex_unlock (&tf->client->topology->tpld_modification_mtx);
}

void
test_connect_succeeds (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   OBSERVE (tf, !tf->observations->awaited);

   tf_destroy (tf);
}

void
test_connect_faas_use_polling (void)
{
   test_fixture_t *tf;
   mongoc_handshake_t *md = _mongoc_handshake_get ();
   md->env = MONGOC_HANDSHAKE_ENV_AWS;

   /* This mock server will not respond to streaming hello, so OBSERVE_SOON
    will timeout if the server monitor doesn't detect ENV_AWS and switch to
    polling */
   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE (tf, !tf->observations->awaited);

   tf_destroy (tf);
   md->env = MONGOC_HANDSHAKE_ENV_NONE;
}

void
test_connect_hangup (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   OBSERVE (tf, !tf->observations->awaited);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 0);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);
   OBSERVE (tf, !tf->observations->awaited);

   /* No retry occurs since the server was never discovered. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   tf_destroy (tf);
}

void
test_connect_badreply (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_simple (request, "{'ok': 0}");
   request_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 0);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   /* Expect a ServerDescriptionChanged event, since it now has an error. */
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* No retry occurs since the server was never discovered. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   tf_destroy (tf);
}

void
test_connect_shutdown (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_started == 1);
   /* Before the server replies, signal the server monitor to shutdown. */
   _signal_shutdown (tf);

   /* Reply (or hang up) so the request does not wait for connectTimeoutMS to
    * time out. */
   reply_to_request_with_ok_and_destroy (request);

   /* Heartbeat succeeds, but server description is not updated. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 0);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   tf_destroy (tf);
}

void
test_connect_requestscan (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   /* Before the mock server replies, request a scan. */
   _request_scan (tf);
   reply_to_request_with_ok_and_destroy (request);

   /* Because the request occurred during the scan, no subsequent scan occurs.
    */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   OBSERVE (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE (tf, tf->observations->n_server_changed == 1);
   OBSERVE (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   tf_destroy (tf);
}

void
test_retry_succeeds (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_FAST_MIN_HEARTBEAT | TF_NO_MONGODB_API_VERSION);

   /* Initial discovery occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);

   /* Heartbeat succeeds, but server description is not updated. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Request a scan to speed things up. */
   _request_scan (tf);

   /* The next hello occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Server is marked as unknown, but not for long. Next scan is immediate. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* Retry occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 3);
   reply_to_request_with_ok_and_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   tf_destroy (tf);
}

void
test_retry_hangup (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_FAST_MIN_HEARTBEAT | TF_NO_MONGODB_API_VERSION);

   /* Initial discovery occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);

   /* Heartbeat succeeds, but server description is not updated. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Request a scan to speed things up. */
   _request_scan (tf);

   /* The next hello occurs (due to fast heartbeat). */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Server is marked as unknown. Next scan is immediate. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* Retry occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 3);
   reply_to_request_with_hang_up (request);
   request_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   tf_destroy (tf);
}

void
test_retry_badreply (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_FAST_MIN_HEARTBEAT | TF_NO_MONGODB_API_VERSION);

   /* Initial discovery occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);

   /* Heartbeat succeeds, but server description is not updated. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Request a scan to speed things up. */
   _request_scan (tf);

   /* The next hello occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   /* Server is marked unknown. Next scan is immediate. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* Retry occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 3);
   reply_to_request_simple (request, "{'ok': 0}");
   request_destroy (request);
   /* Heartbeat fails. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   tf_destroy (tf);
}

void
test_retry_shutdown (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_FAST_HEARTBEAT | TF_NO_MONGODB_API_VERSION);

   /* Initial discovery occurs. */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);

   /* Heartbeat succeeds, but server description is not updated. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* The next hello occurs (due to fast heartbeat). */
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   _signal_shutdown (tf);
   reply_to_request_with_ok_and_destroy (request);

   /* No retry occurs. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   OBSERVE (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   tf_destroy (tf);
}

/* Test a server monitor being added and removed repeatedly. */
static void
test_flip_flop (void)
{
   test_fixture_t *tf;
   request_t *request;
   int i;

   tf = tf_new (TF_NO_MONGODB_API_VERSION);

   for (i = 1; i < 100; i++) {
      request = mock_server_receives_legacy_hello (tf->server, NULL);
      OBSERVE (tf, request);
      reply_to_request_with_ok_and_destroy (request);
      _signal_shutdown (tf);
      OBSERVE_SOON (tf, tf->observations->n_heartbeat_started == i);
      OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == i);
      _add_server_monitor (tf);
   }

   tf_destroy (tf);
}

static void
test_repeated_requestscan (void)
{
   test_fixture_t *tf;
   request_t *request;
   int i;

   /* Multiple repeated requests before a hello completes should not cause a
    * subsequent scan. */
   tf = tf_new (TF_FAST_MIN_HEARTBEAT | TF_NO_MONGODB_API_VERSION);
   for (i = 0; i < 10; i++) {
      _request_scan (tf);
   }
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, request);
   for (i = 0; i < 10; i++) {
      _request_scan (tf);
   }
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);

   tf_destroy (tf);
}

static void
test_sleep_after_scan (void)
{
   test_fixture_t *tf;
   request_t *request;

   /* After handling a scan request */
   tf = tf_new (TF_FAST_MIN_HEARTBEAT | TF_NO_MONGODB_API_VERSION);
   _request_scan (tf);
   request = mock_server_receives_legacy_hello (tf->server, NULL);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   reply_to_request_with_ok_and_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   WAIT_TWO_MIN_HEARTBEAT_MS;
   /* No subsequent command send. */
   OBSERVE (tf, tf->observations->n_heartbeat_started == 1);
   tf_destroy (tf);
}

static void
test_streaming_succeeds (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   OBSERVE (tf, tf->observations->awaited);
   reply_to_request_with_ok_and_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   OBSERVE (tf, tf->observations->awaited);

   tf_destroy (tf);
}

static void
test_streaming_hangup (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   OBSERVE (tf, tf->observations->awaited);
   reply_to_request_with_hang_up (request);
   request_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   /* Due to network error, server monitor immediately proceeds and performs
    * handshake. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   /* Because of the transition to Unknown, then back to Standalone, three
    * server changed events occurred. */
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 3);
   tf_destroy (tf);
}

static void
test_streaming_badreply (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_request_simple (request, "{'ok': 0}");
   request_destroy (request);

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* Request an immediate scan to trigger the next polling hello. */
   _request_scan (tf);
   /* The auto responder will handle the polling hello. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 3);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   tf_destroy (tf);
}

static void
test_streaming_shutdown (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO | TF_FAST_HEARTBEAT);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   _signal_shutdown (tf);
   /* This should cancel the hello immediately. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   /* No further hello commands should be sent. */
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   request_destroy (request);
   tf_destroy (tf);
}

static void
test_streaming_cancel (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   _request_cancel (tf);

   /* This should cancel the hello immediately. */
   request_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   /* The cancellation closes the connection and waits before creating a new
    * connection. Check that no new heartbeat was started. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   _request_scan (tf);
   /* The handshake will be handled by the auto responder. */

   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 4);
   reply_to_request_with_ok_and_destroy (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 3);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 2);
   tf_destroy (tf);
}

static void
test_moretocome_succeeds (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   OBSERVE (tf, tf->observations->awaited);
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   OBSERVE (tf, tf->observations->awaited);

   /* Server monitor is still streaming replies. */
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 3);
   OBSERVE (tf, tf->observations->awaited);
   /* Reply with no moretocome flag. */
   reply_to_op_msg_request (request, MONGOC_MSG_NONE, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 4);
   OBSERVE (tf, tf->observations->awaited);
   request_destroy (request);
   /* Server monitor immediately sends awaitable hello. */
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_started == 5);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 4);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   OBSERVE (tf, tf->observations->awaited);
   request_destroy (request);
   tf_destroy (tf);
}

static void
test_moretocome_hangup (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   OBSERVE (tf, tf->observations->awaited);
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   OBSERVE (tf, tf->observations->awaited);

   /* Server monitor is still streaming replies. */
   reply_to_request_with_hang_up (request);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   /* Due to network error, server monitor immediately proceeds and performs
    * handshake. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 3);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   /* Because of the transition to Unknown, then back to Standalone, three
    * server changed events occurred. */
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 3);
   request_destroy (request);
   tf_destroy (tf);
}

static void
test_moretocome_badreply (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Server monitor is still streaming replies. */
   reply_to_request_simple (request, "{'ok': 0}");
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 2);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_UNKNOWN);

   /* Server monitor sleeps for next poll. Request an immediate scan. */
   _request_scan (tf);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 3);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 3);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   request_destroy (request);
   tf_destroy (tf);
}

static void
test_moretocome_shutdown (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO | TF_FAST_HEARTBEAT);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Server monitor is still streaming replies. It may be reading, or it may be
    * processing the last reply. Requesting shutdown cancels. */
   _signal_shutdown (tf);

   WAIT_TWO_MIN_HEARTBEAT_MS;
   /* No further heartbeats are attempted. */
   OBSERVE (tf, tf->observations->n_heartbeat_succeeded == 2);
   request_destroy (request);

   tf_destroy (tf);
}

static void
test_moretocome_cancel (void)
{
   test_fixture_t *tf;
   request_t *request;

   tf = tf_new (TF_AUTO_RESPOND_POLLING_HELLO);
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 2);
   reply_to_op_msg_request (request, MONGOC_MSG_MORE_TO_COME, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));

   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 0);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Server monitor is still streaming replies. */
   _request_cancel (tf);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 2);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   request_destroy (request);

   /* The cancellation closes the connection and waits before creating a new
    * connection. Check that no new heartbeat was started. */
   WAIT_TWO_MIN_HEARTBEAT_MS;
   OBSERVE (tf, tf->observations->n_heartbeat_started == 3);
   _request_scan (tf);
   /* The handshake will be handled by the auto responder. */

   /* Cancelling creates a new connection. */
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_started == 4);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 3);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);

   /* Server monitor sends a fresh awaitable hello. */
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 5);
   reply_to_op_msg_request (request, MONGOC_MSG_NONE, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 4);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   request_destroy (request);

   /* Since the reply did not include moretocome, server monitor sends another.
    */
   request = mock_server_receives_msg (
      tf->server, MONGOC_MSG_EXHAUST_ALLOWED, tmp_bson ("{'topologyVersion': { '$exists': true}}"));
   OBSERVE (tf, request);
   OBSERVE (tf, tf->observations->n_heartbeat_started == 6);
   reply_to_op_msg_request (request, MONGOC_MSG_NONE, tmp_bson ("{'ok': 1, 'topologyVersion': " TV "}"));
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_succeeded == 5);
   OBSERVE_SOON (tf, tf->observations->n_heartbeat_failed == 1);
   OBSERVE_SOON (tf, tf->observations->n_server_changed == 1);
   OBSERVE_SOON (tf, tf->observations->sd_type == MONGOC_SERVER_STANDALONE);
   request_destroy (request);

   tf_destroy (tf);
}

void
test_monitoring_install (TestSuite *suite)
{
   /* Tests for initial connection. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/connect/succeeds", test_connect_succeeds);
   TestSuite_AddMockServerTest (
      suite, "/server_monitor_thread/connect/faas_use_polling", test_connect_faas_use_polling);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/connect/hangup", test_connect_hangup);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/connect/badreply", test_connect_badreply);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/connect/shutdown", test_connect_shutdown);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/connect/requestscan", test_connect_requestscan);

   /* Tests for retry. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/retry/succeeds", test_retry_succeeds);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/retry/hangup", test_retry_hangup);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/retry/badreply", test_retry_badreply);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/retry/shutdown", test_retry_shutdown);

   /* Tests for streaming. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/streaming/succeeds", test_streaming_succeeds);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/streaming/hangup", test_streaming_hangup);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/streaming/badreply", test_streaming_badreply);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/streaming/shutdown", test_streaming_shutdown);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/streaming/cancel", test_streaming_cancel);

   /* Tests for moretocome. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/moretocome/succeeds", test_moretocome_succeeds);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/moretocome/hangup", test_moretocome_hangup);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/moretocome/badreply", test_moretocome_badreply);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/moretocome/shutdown", test_moretocome_shutdown);
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/moretocome/cancel", test_moretocome_cancel);

   /* Test flip flopping. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/flip_flop", test_flip_flop);

   /* Test repeated scan requests. */
   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/repeated_requestscan", test_repeated_requestscan);

   TestSuite_AddMockServerTest (suite, "/server_monitor_thread/sleep_after_scan", test_sleep_after_scan);
}
