/*
 * Copyright 2015 MongoDB, Inc.
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


#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc.h"

#include "mongoc/mongoc-buffer-private.h"
#include "mongoc/mongoc-socket-private.h"
#include "mongoc/mongoc-thread-private.h"
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-trace-private.h"
#include "sync-queue.h"
#include "mock-server.h"
#include "../test-conveniences.h"
#include "../test-libmongoc.h"
#include "../TestSuite.h"

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

/* /Async/hello_ssl and /TOPOLOGY/scanner_ssl need a reasonable timeout */
#define TIMEOUT 5000


struct _mock_server_t {
   bool running;
   bool stopped;
   bool rand_delay;
   int64_t request_timeout_msec;
   uint16_t port;
   mongoc_socket_t *sock;
   char *uri_str;
   mongoc_uri_t *uri;
   bson_thread_t main_thread;
   mongoc_cond_t cond;
   bson_mutex_t mutex;
   int32_t last_response_id;
   mongoc_array_t worker_threads;
   sync_queue_t *q;
   mongoc_array_t autoresponders;
   int last_autoresponder_id;
   int64_t start_time;

#ifdef MONGOC_ENABLE_SSL
   bool ssl;
   mongoc_ssl_opt_t ssl_opts;
#endif

   mock_server_bind_opts_t bind_opts;
};


struct _autoresponder_handle_t {
   autoresponder_t responder;
   void *data;
   destructor_t destructor;
   int id;
};

struct _hello_callback_t {
   hello_callback_func_t callback_func;
   void *data;
   destructor_t destructor;
};

typedef enum { REPLY, HANGUP, RESET } reply_type_t;


typedef struct {
   reply_type_t type;
   mongoc_reply_flags_t flags;
   bson_t *docs;
   int n_docs;
   int64_t cursor_id;
   uint16_t client_port;
   int32_t request_opcode;
   int32_t query_flags;
   uint32_t opmsg_flags;
   int32_t response_to;
} reply_t;


static BSON_THREAD_FUN (main_thread, data);

static BSON_THREAD_FUN (worker_thread, data);

static void
_mock_server_reply_with_stream (mock_server_t *server, reply_t *reply, mongoc_stream_t *client);

void
autoresponder_handle_destroy (autoresponder_handle_t *handle);

static uint16_t
get_port (mongoc_socket_t *sock);

/*--------------------------------------------------------------------------
 *
 * mock_server_new --
 *
 *       Get a new mock_server_t. Call mock_server_run to start it,
 *       then mock_server_get_uri to connect.
 *
 *       This server does not autorespond to "hello".
 *
 * Returns:
 *       A server you must mock_server_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mock_server_t *
mock_server_new (void)
{
   mock_server_t *server = (mock_server_t *) bson_malloc0 (sizeof (mock_server_t));

   server->request_timeout_msec = get_future_timeout_ms ();
   _mongoc_array_init (&server->autoresponders, sizeof (autoresponder_handle_t));
   _mongoc_array_init (&server->worker_threads, sizeof (bson_thread_t));
   mongoc_cond_init (&server->cond);
   bson_mutex_init (&server->mutex);
   server->q = q_new ();
   server->start_time = bson_get_monotonic_time ();

   return server;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_with_auto_hello --
 *
 *       A new mock_server_t that autoresponds to hello. Call
 *       mock_server_run to start it, then mock_server_get_uri to
 *       connect.
 *
 * Returns:
 *       A server you must mock_server_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mock_server_t *
mock_server_with_auto_hello (int32_t max_wire_version)
{
   mock_server_t *server = mock_server_new ();

   ASSERT_WITH_MSG (max_wire_version >= WIRE_VERSION_MIN,
                    "max_wire_version %" PRId32 " must be greater than or equal to minimum wire version %d",
                    max_wire_version,
                    WIRE_VERSION_MIN);

   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d}",
                           WIRE_VERSION_MIN,
                           max_wire_version);

   return server;
}


/*--------------------------------------------------------------------------
 *
 * mock_mongos_new --
 *
 *       A new mock_server_t that autoresponds to hello as if it were a
 *       mongos. Call mock_server_run to start it, then mock_server_get_uri
 *       to connect.
 *
 * Returns:
 *       A server you must mock_server_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mock_server_t *
mock_mongos_new (int32_t max_wire_version)
{
   mock_server_t *server = mock_server_new ();

   ASSERT_WITH_MSG (max_wire_version >= WIRE_VERSION_MIN,
                    "max_wire_version %" PRId32 " must be greater than or equal to minimum wire version %d",
                    max_wire_version,
                    WIRE_VERSION_MIN);

   mock_server_auto_hello (server,
                           "{'ok': 1.0,"
                           " 'isWritablePrimary': true,"
                           " 'msg': 'isdbgrid',"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " '$clusterTime': {"
                           "   'clusterTime': {'$timestamp': {'t': 1, 'i': 1}},"
                           "   'signature': {"
                           "     'hash': {'$binary': {'subType': '0', 'base64': ''}},"
                           "     'keyId': {'$numberLong': '6446735049323708417'}"
                           "   },"
                           "   'operationTime': {'$timestamp': {'t': 1, 'i': 1}}},"
                           " 'logicalSessionTimeoutMinutes': 30}",
                           WIRE_VERSION_MIN,
                           max_wire_version);

   return server;
}


static bool
hangup (request_t *request, void *ctx)
{
   BSON_UNUSED (ctx);

   reply_to_request_with_hang_up (request);
   request_destroy (request);

   return true;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_down --
 *
 *       A new mock_server_t hangs up. Call mock_server_run to start it,
 *       then mock_server_get_uri to connect.
 *
 * Returns:
 *       A server you must mock_server_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mock_server_t *
mock_server_down (void)
{
   mock_server_t *server = mock_server_new ();

   mock_server_autoresponds (server, hangup, NULL, NULL);

   return server;
}


#ifdef MONGOC_ENABLE_SSL

/*--------------------------------------------------------------------------
 *
 * mock_server_set_ssl_opts --
 *
 *       Set server-side SSL options before calling mock_server_run.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
mock_server_set_ssl_opts (mock_server_t *server, mongoc_ssl_opt_t *opts)
{
   bson_mutex_lock (&server->mutex);
   server->ssl = true;
   memcpy (&server->ssl_opts, opts, sizeof *opts);
   bson_mutex_unlock (&server->mutex);
}

#endif

/*--------------------------------------------------------------------------
 *
 * mock_server_run --
 *
 *       Start listening on an unused port. After this, call
 *       mock_server_get_uri to connect.
 *
 * Returns:
 *       The bound port.
 *
 * Side effects:
 *       The server's port and URI are set.
 *
 *--------------------------------------------------------------------------
 */
uint16_t
mock_server_run (mock_server_t *server)
{
   mongoc_socket_t *ssock;
   struct sockaddr_in default_bind_addr;
   struct sockaddr_in *bind_addr;
   int optval;
   uint16_t bound_port;
   size_t bind_addr_len = 0;
   int r;

   ssock = mongoc_socket_new (server->bind_opts.family ? server->bind_opts.family : AF_INET, SOCK_STREAM, 0);
   if (!ssock) {
      perror ("Failed to create socket.");
      return 0;
   }

   optval = 1;
   mongoc_socket_setsockopt (ssock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

   optval = server->bind_opts.ipv6_only;
   mongoc_socket_setsockopt (ssock, IPPROTO_IPV6, IPV6_V6ONLY, &optval, sizeof (optval));

   memset (&default_bind_addr, 0, sizeof default_bind_addr);

   default_bind_addr.sin_family = AF_INET;
   default_bind_addr.sin_addr.s_addr = htonl (INADDR_ANY);

   /* bind to unused port */
   default_bind_addr.sin_port = htons (0);

   if (server->bind_opts.bind_addr) {
      bind_addr = server->bind_opts.bind_addr;
      bind_addr_len = server->bind_opts.bind_addr_len;
   } else {
      bind_addr = &default_bind_addr;
      bind_addr_len = sizeof (default_bind_addr);
   }

   // socklen_t: an unsigned opaque integral type of length of at least 32 bits.
   // To forestall portability problems, it is recommended that applications
   // should not use values larger than 2^32 - 1.
   BSON_ASSERT (bson_in_range_unsigned (uint32_t, bind_addr_len));

   if (-1 == mongoc_socket_bind (ssock, (struct sockaddr *) bind_addr, (uint32_t) bind_addr_len)) {
      perror ("Failed to bind socket");
      return 0;
   }

   if (-1 == mongoc_socket_listen (ssock, 10)) {
      perror ("Failed to put socket into listen mode");
      return 0;
   }

   bound_port = get_port (ssock);
   if (!bound_port) {
      perror ("Failed to get bound port number");
      return 0;
   }

   MONGOC_INFO ("Starting mock server on port %d.", bound_port);

   bson_mutex_lock (&server->mutex);

   server->sock = ssock;
   server->port = bound_port;
   /* TODO: configurable timeouts, perhaps from env */
   server->uri_str = bson_strdup_printf ("mongodb://127.0.0.1:%hu/?serverselectiontimeoutms=10000&"
                                         "sockettimeoutms=10000",
                                         bound_port);
   server->uri = mongoc_uri_new (server->uri_str);

   // Many mock server tests do not expect retryable handshakes. Disable by
   // default: tests that expect or require retryable handshakes must opt-in.
   mongoc_uri_set_option_as_bool (server->uri, MONGOC_URI_RETRYREADS, false);
   mongoc_uri_set_option_as_bool (server->uri, MONGOC_URI_RETRYWRITES, false);

   r = mcommon_thread_create (&server->main_thread, main_thread, (void *) server);
   BSON_ASSERT (r == 0);
   while (!server->running) {
      mongoc_cond_wait (&server->cond, &server->mutex);
   }

   bson_mutex_unlock (&server->mutex);

   test_suite_mock_server_log ("listening on port %hu", bound_port);

   return (uint16_t) bound_port;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_autoresponds --
 *
 *       Respond to matching requests. "data" is passed to the responder
 *       callback, and passed to "destructor" when the autoresponder is
 *       destroyed.
 *
 *       Responders are run most-recently-added-first until one returns
 *       true to indicate it has handled the request. If none handles it,
 *       the request is enqueued until a call to mock_server_receives_*.
 *
 *       Autoresponders must call request_destroy after handling a
 *       request.
 *
 * Returns:
 *       An id for mock_server_remove_autoresponder.
 *
 * Side effects:
 *       If a matching request is enqueued, pop it and respond.
 *
 *--------------------------------------------------------------------------
 */

int
mock_server_autoresponds (mock_server_t *server, autoresponder_t responder, void *data, destructor_t destructor)
{
   autoresponder_handle_t handle = {responder, data, destructor};
   int id;

   bson_mutex_lock (&server->mutex);
   id = handle.id = server->last_autoresponder_id++;
   /* TODO: peek and see if a matching request is enqueued */
   _mongoc_array_append_val (&server->autoresponders, handle);
   bson_mutex_unlock (&server->mutex);

   return id;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_remove_autoresponder --
 *
 *       Remove a responder callback. Pass in the id returned by
 *       mock_server_autoresponds.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       The responder's destructor is called on its "data" pointer.
 *
 *--------------------------------------------------------------------------
 */

void
mock_server_remove_autoresponder (mock_server_t *server, int id)
{
   size_t i;
   autoresponder_handle_t *handles;

   bson_mutex_lock (&server->mutex);
   handles = (autoresponder_handle_t *) server->autoresponders.data;
   for (i = 0; i < server->autoresponders.len; i++) {
      if (handles[i].id == id) {
         /* left-shift everyone after */
         server->autoresponders.len--;
         for (; i < server->autoresponders.len; i++) {
            handles[i] = handles[i + 1];
         }

         autoresponder_handle_destroy (handles);

         break;
      }
   }

   bson_mutex_unlock (&server->mutex);
}


static bool
auto_hello_generate_response (request_t *request, void *data, bson_t *hello_response)
{
   const char *response_json = (const char *) data;
   char *quotes_replaced;
   bson_error_t error;

   BSON_UNUSED (request);

   quotes_replaced = single_quotes_to_double (response_json);

   if (!bson_init_from_json (hello_response, quotes_replaced, -1, &error)) {
      test_error ("%s", error.message);
   }

   bson_free (quotes_replaced);

   return true;
}

static bool
auto_hello (request_t *request, void *data)
{
   hello_callback_t *callback = (hello_callback_t *) data;
   bson_t response;
   bool is_hello;
   bool is_legacy_hello;
   char *response_json;
   bson_iter_t iter;

   if (!request->is_command) {
      return false;
   }

   /* Check whether we've got "hello" or legacy hello */
   is_hello = strcasecmp (request->command_name, "hello") == 0;
   is_legacy_hello = strcasecmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO) == 0;

   if (!is_hello && !is_legacy_hello) {
      return false;
   }

   if (!callback->callback_func (request, callback->data, &response)) {
      return false;
   }

   /* Convert responses for legacy hello */
   if (bson_iter_init_find (&iter, &response, "isWritablePrimary")) {
      BSON_APPEND_BOOL (&response, HANDSHAKE_RESPONSE_LEGACY_HELLO, bson_iter_bool (&iter));
   } else if (bson_iter_init_find (&iter, &response, HANDSHAKE_RESPONSE_LEGACY_HELLO)) {
      BSON_APPEND_BOOL (&response, "isWritablePrimary", bson_iter_bool (&iter));
   }

   if (!bson_iter_init_find (&iter, &response, "minWireVersion")) {
      BSON_APPEND_INT32 (&response, "minWireVersion", WIRE_VERSION_MIN);
   }
   if (!bson_iter_init_find (&iter, &response, "maxWireVersion")) {
      BSON_APPEND_INT32 (&response, "maxWireVersion", WIRE_VERSION_MAX);
   }

   response_json = bson_as_json (&response, 0);

   if (mock_server_get_rand_delay (request->server)) {
      _mongoc_usleep ((int64_t) (rand () % 10) * 1000);
   }

   reply_to_request (request, MONGOC_REPLY_NONE, 0, 0, 1, response_json);

   bson_destroy (&response);
   bson_free (response_json);
   request_destroy (request);
   return true;
}

static void
hello_callback_free (void *data)
{
   hello_callback_t *callback = (hello_callback_t *) data;

   if (callback->destructor) {
      callback->destructor (callback->data);
   }

   bson_free (callback);
}

int
mock_server_auto_hello_callback (mock_server_t *server,
                                 hello_callback_func_t callback_func,
                                 void *data,
                                 destructor_t destructor)
{
   hello_callback_t *callback = bson_malloc0 (sizeof (hello_callback_t));

   ASSERT (callback_func);

   callback->callback_func = callback_func;
   callback->data = data;
   callback->destructor = destructor;

   return mock_server_autoresponds (server, auto_hello, (void *) callback, hello_callback_free);
}

/*--------------------------------------------------------------------------
 *
 * mock_server_auto_hello --
 *
 *       Autorespond to "hello" and legacy hello with the provided document.
 *
 * Returns:
 *       An id for mock_server_remove_autoresponder.
 *
 * Side effects:
 *       If a matching request is enqueued, pop it and respond.
 *
 *--------------------------------------------------------------------------
 */

MONGOC_PRINTF_FORMAT (2, 3)
int
mock_server_auto_hello (mock_server_t *server, const char *response_json, ...)
{
   char *formatted_response_json;
   va_list args;

   va_start (args, response_json);
   formatted_response_json = bson_strdupv_printf (response_json, args);
   va_end (args);

   return mock_server_auto_hello_callback (
      server, auto_hello_generate_response, (void *) formatted_response_json, bson_free);
}


static bool
auto_endsessions (request_t *request, void *data)
{
   BSON_UNUSED (data);

   if (!request->is_command || strcasecmp (request->command_name, "endSessions") != 0) {
      return false;
   }

   reply_to_request_with_ok_and_destroy (request);
   return true;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_auto_endsessions --
 *
 *       Autorespond to "endSessions".
 *
 * Returns:
 *       An id for mock_server_remove_autoresponder.
 *
 * Side effects:
 *       If a matching request is enqueued, pop it and respond.
 *
 *--------------------------------------------------------------------------
 */

int
mock_server_auto_endsessions (mock_server_t *server)
{
   return mock_server_autoresponds (server, auto_endsessions, NULL, NULL);
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_uri --
 *
 *       Call after mock_server_run to get the connection string.
 *
 * Returns:
 *       A const URI.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const mongoc_uri_t *
mock_server_get_uri (mock_server_t *server)
{
   mongoc_uri_t *uri;

   bson_mutex_lock (&server->mutex);
   uri = server->uri;
   bson_mutex_unlock (&server->mutex);

   return uri;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_host_and_port --
 *
 *       Call after mock_server_run to get the server's "host:port".
 *
 * Returns:
 *       A const string.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const char *
mock_server_get_host_and_port (mock_server_t *server)
{
   const mongoc_uri_t *uri;

   uri = mock_server_get_uri (server);
   BSON_ASSERT (uri); /* must call after mock_server_run */
   return (mongoc_uri_get_hosts (uri))->host_and_port;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_port --
 *
 *       Call after mock_server_run to get the server's listening port.
 *
 * Returns:
 *       A port number.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

uint16_t
mock_server_get_port (mock_server_t *server)
{
   return server->port;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_request_timeout_msec --
 *
 *       How long mock_server_receives_* functions wait for a client
 *       request before giving up and returning NULL.
 *
 *--------------------------------------------------------------------------
 */

static int64_t
mock_server_get_request_timeout_msec (mock_server_t *server)
{
   int64_t request_timeout_msec;

   bson_mutex_lock (&server->mutex);
   request_timeout_msec = server->request_timeout_msec;
   bson_mutex_unlock (&server->mutex);

   return request_timeout_msec;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_set_request_timeout_msec --
 *
 *       How long mock_server_receives_* functions wait for a client
 *       request before giving up and returning NULL.
 *
 *--------------------------------------------------------------------------
 */

void
mock_server_set_request_timeout_msec (mock_server_t *server, int64_t request_timeout_msec)
{
   bson_mutex_lock (&server->mutex);
   server->request_timeout_msec = request_timeout_msec;
   bson_mutex_unlock (&server->mutex);
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_rand_delay --
 *
 *       Does the server delay a random duration before responding?
 *
 *--------------------------------------------------------------------------
 */

bool
mock_server_get_rand_delay (mock_server_t *server)
{
   bool rand_delay;

   bson_mutex_lock (&server->mutex);
   rand_delay = server->rand_delay;
   bson_mutex_unlock (&server->mutex);

   return rand_delay;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_set_rand_delay --
 *
 *       Whether to delay a random duration before responding.
 *
 *--------------------------------------------------------------------------
 */

void
mock_server_set_rand_delay (mock_server_t *server, bool rand_delay)
{
   bson_mutex_lock (&server->mutex);
   server->rand_delay = rand_delay;
   bson_mutex_unlock (&server->mutex);
}


/*--------------------------------------------------------------------------
 *
 * mock_server_get_uptime_sec --
 *
 *       How long since mock_server_run() was called.
 *
 *--------------------------------------------------------------------------
 */

static double
mock_server_get_uptime_sec (mock_server_t *server)
{
   double uptime;

   bson_mutex_lock (&server->mutex);
   uptime = (bson_get_monotonic_time () - server->start_time) / 1e6;
   bson_mutex_unlock (&server->mutex);

   return uptime;
}


sync_queue_t *
mock_server_get_queue (mock_server_t *server)
{
   sync_queue_t *q;

   bson_mutex_lock (&server->mutex);
   q = server->q;
   bson_mutex_unlock (&server->mutex);

   return q;
}


static void
request_assert_no_duplicate_keys (request_t *request)
{
   for (size_t i = 0u; i < request->docs.len; i++) {
      assert_no_duplicate_keys (request_get_doc (request, i));
   }
}


request_t *
mock_server_receives_request (mock_server_t *server)
{
   sync_queue_t *q;
   int64_t request_timeout_msec;
   request_t *r;

   q = mock_server_get_queue (server);
   request_timeout_msec = mock_server_get_request_timeout_msec (server);
   r = (request_t *) q_get (q, request_timeout_msec);
   if (r) {
      request_assert_no_duplicate_keys (r);
   }

   return r;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_bulk_msg --
 *
 *       Pop a client OP_MSG request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request. Pass
 *       `msg_pattern`, which is matched to the series of exactly `n_doc`
 *       documents in the request, regardless of section boundaries.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the request does not
 *       match.
 *
 * Side effects:
 *       Logs and aborts if the current request is not an OP_MSG matching
 *       flags and expected pattern and number of documents.
 *
 *--------------------------------------------------------------------------
 */
request_t *
mock_server_receives_bulk_msg (
   mock_server_t *server, uint32_t flags, const bson_t *msg_pattern, const bson_t *doc_pattern, size_t n_docs)
{
   request_t *request;
   bool r;

   BSON_UNUSED (flags);

   request = mock_server_receives_request (server);

   {
      const bson_t **docs;
      size_t i;
      docs = bson_malloc (n_docs * sizeof (bson_t *));
      docs[0] = msg_pattern;
      for (i = 1; i < n_docs; ++i) {
         docs[i] = doc_pattern;
      }
      r = request_matches_msg (request, MONGOC_MSG_NONE, docs, n_docs);
      bson_free ((bson_t **) docs);
   }

   if (!r) {
      request_destroy (request);
      return NULL;
   }

   return request;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_receives_command --
 *
 *       Pop a client request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the request does
 *       not match.
 *
 * Side effects:
 *       Logs if the current request is not an OP_QUERY command matching
 *       database_name, command_name, and command_json.
 *
 *--------------------------------------------------------------------------
 */

MONGOC_PRINTF_FORMAT (4, 5)
request_t *
mock_server_receives_command (
   mock_server_t *server, const char *database_name, mongoc_query_flags_t flags, const char *command_json, ...)
{
   va_list args;
   char *formatted_command_json = NULL;
   char *ns;
   request_t *request;

   va_start (args, command_json);
   if (command_json) {
      formatted_command_json = bson_strdupv_printf (command_json, args);
   }
   va_end (args);

   ns = bson_strdup_printf ("%s.$cmd", database_name);

   request = mock_server_receives_request (server);

   if (request && !request_matches_query (request, ns, flags, 0, 1, formatted_command_json, NULL, true)) {
      request_destroy (request);
      request = NULL;
   }

   bson_free (formatted_command_json);
   bson_free (ns);

   return request;
}


/*--------------------------------------------------------------------------
 *
 * mock_server_receives_msg --
 *
 *       Pop a client OP_MSG request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request. Pass varargs
 *       list of bson_t pointers, which are matched to the series of
 *       documents in the request, regardless of section boundaries.
 *
 * Returns:
 *       A request you must request_destroy.
 *
 * Side effects:
 *       Logs and aborts if the current request is not an OP_MSG matching
 *       flags and documents.
 *
 *--------------------------------------------------------------------------
 */

request_t *
_mock_server_receives_msg (mock_server_t *server, uint32_t flags, ...)
{
   request_t *request;
   va_list args;
   bool r;

   request = mock_server_receives_request (server);

   va_start (args, flags);
   r = request_matches_msgv (request, flags, &args);
   va_end (args);

   if (!r) {
      request_destroy (request);
      return NULL;
   }

   return request;
}

request_t *
_mock_server_receives_single_msg (mock_server_t *server, uint32_t flags, const bson_t *doc)
{
   request_t *request;
   bool r;

   BSON_ASSERT (doc);

   request = mock_server_receives_request (server);

   r = request_matches_msg (request, flags, &doc, 1);

   if (!r) {
      request_destroy (request);
      return NULL;
   }

   return request;
}


static request_t *
mock_server_matches_legacy_hello (request_t *request, const char *match_json);

static request_t *
mock_server_matches_any_hello_with_json (request_t *request,
                                         const char *match_json_op_msg,
                                         const char *match_json_op_query)
{
   if (!request) {
      return NULL;
   }

   /* We check the opcode separately because request_matches_msg() and friends
   like to abort the program when checks fail: */
   if (MONGOC_OPCODE_MSG == request->opcode) {
      bson_t *hello_doc = NULL;
      const char *hello_str = "{'hello': 1, 'maxAwaitTimeMS': { '$exists': false }}";

      if (NULL != match_json_op_msg)
         hello_doc = tmp_bson (match_json_op_msg);
      else
         hello_doc = tmp_bson (hello_str);

      if (request_matches_msg (request,
                               0, /* flags */
                               (const bson_t **) &hello_doc,
                               1 /* number of documents */)) {
         return request;
      }
   }

   if (mock_server_matches_legacy_hello (request, match_json_op_query ? match_json_op_query : NULL)) {
      return request;
   }

   /* No match: */
   request_destroy (request);

   return NULL;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_any_hello--
 *
 * Check first for an OP_MSG hello or an OP_QUERY with hello or legacy hello.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the current
 *       request is not a hello command.
 *
 * Side effects:
 *       Logs if the current request is not a legacy hello command ("isMaster")
 *       using OP_QUERY.
 *
 *--------------------------------------------------------------------------
 */
request_t *
mock_server_receives_any_hello (mock_server_t *server)
{
   return mock_server_receives_any_hello_with_match (server, NULL, NULL);
}

/*--------------------------------------------------------------------------
 *
 * mock_server_matches_legacy_hello --
 *
 * Checks to see if a given request matches OP_QUERY hello or legacy hello.
 *
 * Returns:
 *       A request you must request_destroy (the same one passed in), or
 *       NULL if the current request is not a hello command.
 *
 * Side effects:
 *       Logs if the current request is not hello command or legacy hello
 *       command ("isMaster") using OP_QUERY.
 *
 *--------------------------------------------------------------------------
 */
static request_t *
mock_server_matches_legacy_hello (request_t *request, const char *match_json)
{
   char *formatted_command_json = NULL;

   if (!request) {
      return NULL;
   }

   if (strcasecmp (request->command_name, "hello") && strcasecmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO)) {
      request_destroy (request);

      fprintf (stderr,
               "expected hello or legacy hello (\"%s\"), but got \"%s\"\n",
               HANDSHAKE_CMD_LEGACY_HELLO,
               request->command_name);

      return NULL;
   }

   formatted_command_json =
      bson_strdup_printf ("{'%s': 1, 'maxAwaitTimeMS': { '$exists': false }}", request->command_name);

   /* request_matches_query() always checks for OPCODE_QUERY, used by legacy
    * hello: */
   if (!request_matches_query (request,
                               "admin.$cmd",
                               MONGOC_QUERY_SECONDARY_OK,
                               0,
                               1,
                               match_json ? match_json : formatted_command_json,
                               NULL,
                               true)) {
      request_destroy (request);
      request = NULL;
   }

   bson_free (formatted_command_json);

   return request;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_legacy_hello --
 *
 *       Pop a client non-streaming hello call if one is enqueued,
 *       or wait up to request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the current
 *       request is not a hello command.
 *
 * Side effects:
 *       Logs if the current request is not a legacy hello command ("isMaster")
 *       using OP_QUERY.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_legacy_hello (mock_server_t *server, const char *match_json)
{
   return mock_server_matches_legacy_hello (mock_server_receives_request (server), match_json);
}


/*--------------------------------------------------------------------------
 *
 * mock_server_receives_hello --
 *
 *       Pop a client non-streaming hello call if one is enqueued,
 *       or wait up to request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the current
 *       request is not a hello command.
 *
 * Side effects:
 *       Logs if the current request is a hello command using OP_QUERY.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_hello (mock_server_t *server)
{
   return mock_server_receives_command (
      server, "admin", MONGOC_QUERY_SECONDARY_OK, "{'hello': 1, 'maxAwaitTimeMS': { '$exists': false }}");
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_any_hello_with_match --
 *
 *       Pop a client non-streaming hello call if one is enqueued,
 *       or wait up to request_timeout_ms for the client to send a request;
 *       if non-NULL values are provided for either or both of the optional
 *       match_json_op_msg or match_json_op_query parameters, the reply is
 *       matched to those, as per request_matches_msg() or
 *       mock_server_matches_legacy_hello().
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the current
 *       request is not a hello command.
 *
 * Side effects:
 *       Logs if the current request uses OP_QUERY but is not hello or legacy
 *       hello.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_any_hello_with_match (mock_server_t *server,
                                           const char *match_json_op_msg,
                                           const char *match_json_op_query)
{
   request_t *request = mock_server_receives_request (server);

   if (NULL == request) {
      return NULL;
   }

   return mock_server_matches_any_hello_with_json (request, match_json_op_msg, match_json_op_query);
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_query --
 *
 *       Pop a client request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the request does
 *       not match.
 *
 * Side effects:
 *       Logs if the current request is not a query matching ns, flags,
 *       skip, n_return, query_json, and fields_json.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_query (mock_server_t *server,
                            const char *ns,
                            mongoc_query_flags_t flags,
                            uint32_t skip,
                            int32_t n_return,
                            const char *query_json,
                            const char *fields_json)
{
   request_t *request;

   request = mock_server_receives_request (server);

   if (request && !request_matches_query (request, ns, flags, skip, n_return, query_json, fields_json, false)) {
      request_destroy (request);
      return NULL;
   }

   return request;
}

/*--------------------------------------------------------------------------
 *
 * mock_server_receives_hello_op_msg --
 *
 *       Pop a client non-streaming hello call if one is enqueued,
 *       or wait up to request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the current
 *       request is not a hello command.
 *
 * Side effects:
 *       None. (See also request_matches_msg()).
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_hello_op_msg (mock_server_t *server)
{
   return _mock_server_receives_single_msg (
      server, 0, tmp_bson ("{'hello': 1, 'maxAwaitTimeMS': { '$exists': false }}"));
}


/*--------------------------------------------------------------------------
 *
 * mock_server_receives_kill_cursors --
 *
 *       Pop a client request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request.
 *
 *       Real-life OP_KILLCURSORS can take multiple ids, but that is
 *       not yet supported here.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the request
 *       does not match.
 *
 * Side effects:
 *       Logs if the current request is not an OP_KILLCURSORS with the
 *       expected cursor_id.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_server_receives_kill_cursors (mock_server_t *server, int64_t cursor_id)
{
   request_t *request;

   request = mock_server_receives_request (server);

   if (request && !request_matches_kill_cursors (request, cursor_id)) {
      request_destroy (request);
      return NULL;
   }

   return request;
}

/*--------------------------------------------------------------------------
 *
 * reply_to_request_with_hang_up --
 *
 *       Hang up on a client request.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Causes a network error on the client side.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_request_with_hang_up (request_t *request)
{
   reply_t *reply;
   test_suite_mock_server_log ("%5.2f  %hu <- %hu \thang up!",
                               mock_server_get_uptime_sec (request->server),
                               request->client_port,
                               request_get_server_port (request));
   reply = bson_malloc0 (sizeof (reply_t));
   reply->type = HANGUP;
   q_put (request->replies, reply);
}


/*--------------------------------------------------------------------------
 *
 * reply_to_request_with_reset --
 *
 *       Forcefully reset a connection from the client.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Causes ECONNRESET on the client side.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_request_with_reset (request_t *request)
{
   reply_t *reply;
   test_suite_mock_server_log ("%5.2f  %hu <- %hu \treset!",
                               mock_server_get_uptime_sec (request->server),
                               request->client_port,
                               request_get_server_port (request));

   reply = bson_malloc0 (sizeof (reply_t));
   reply->type = RESET;
   q_put (request->replies, reply);
}


/*--------------------------------------------------------------------------
 *
 * reply_to_request --
 *
 *       Respond to a client request.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Sends an OP_REPLY to the client.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_request (request_t *request,
                  mongoc_reply_flags_t flags,
                  int64_t cursor_id,
                  int32_t starting_from,
                  int32_t number_returned,
                  const char *docs_json)
{
   char *quotes_replaced;
   bson_t doc;
   bson_error_t error;
   bool r;

   BSON_UNUSED (starting_from);
   BSON_UNUSED (number_returned);

   BSON_ASSERT (request);

   if (docs_json) {
      quotes_replaced = single_quotes_to_double (docs_json);
      r = bson_init_from_json (&doc, quotes_replaced, -1, &error);
      bson_free (quotes_replaced);
   } else {
      r = bson_init_from_json (&doc, "{}", -1, &error);
   }

   if (!r) {
      MONGOC_WARNING ("%s", error.message);
      return;
   }

   reply_to_request_with_multiple_docs (request, flags, &doc, 1, cursor_id);
   bson_destroy (&doc);
}


/*--------------------------------------------------------------------------
 *
 * reply_to_request_simple --
 *
 *       Respond to a client request.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Sends an OP_REPLY to the client.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_request_simple (request_t *request, const char *docs_json)
{
   reply_to_request (request, MONGOC_REPLY_NONE, 0, 0, 1, docs_json);
}

/* To specify additional flags for OP_MSG replies. */
void
reply_to_op_msg_request (request_t *request, mongoc_op_msg_flags_t flags, const bson_t *doc)
{
   reply_t *reply;

   BSON_ASSERT (request);

   reply = bson_malloc0 (sizeof (reply_t));

   reply->opmsg_flags = flags;
   reply->n_docs = 1;
   reply->docs = BSON_ALIGNED_ALLOC0 (bson_t);
   bson_copy_to (doc, &reply->docs[0]);

   reply->cursor_id = 0;
   reply->client_port = request_get_client_port (request);
   reply->request_opcode = MONGOC_OPCODE_MSG;
   reply->response_to = mcd_rpc_header_get_request_id (request->rpc);

   q_put (request->replies, reply);
}


/*--------------------------------------------------------------------------
 *
 * reply_to_request_with_ok_and_destroy --
 *
 *       Respond to a client request.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Sends an OP_REPLY with "{ok: 1}" to the client.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_request_with_ok_and_destroy (request_t *request)
{
   reply_to_request (request, MONGOC_REPLY_NONE, 0, 0, 1, "{'ok': 1}");
   request_destroy (request);
}


/*--------------------------------------------------------------------------
 *
 * reply_to_find_request --
 *
 *       Receive an OP_QUERY or "find" command and reply appropriately.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Very roughly validates the query or "find" command or aborts.
 *       The intent is not to test the driver's query or find command
 *       implementation here, see _test_kill_cursors for example use.
 *
 *--------------------------------------------------------------------------
 */

void
reply_to_find_request (request_t *request,
                       mongoc_query_flags_t flags,
                       int64_t cursor_id,
                       int32_t number_returned,
                       const char *ns,
                       const char *reply_json,
                       bool is_command)
{
   char *find_reply;
   char *db;

   BSON_ASSERT_PARAM (request);

   db = _mongoc_get_db_name (ns);

   /* minimal validation, we're not testing query / find cmd here */
   if (request->is_command && !is_command) {
      test_error ("expected query, got command");
   }

   if (!request->is_command && is_command) {
      test_error ("expected command, got query");
   }

   assert_request_matches_flags (request, flags);

   if (is_command) {
      find_reply = bson_strdup_printf ("{'ok': 1,"
                                       " 'cursor': {"
                                       "    'id': {'$numberLong': '%" PRId64 "'},"
                                       "    'ns': '%s',"
                                       "    'firstBatch': [%s]}}",
                                       cursor_id,
                                       ns,
                                       reply_json);

      reply_to_request_simple (request, find_reply);
      bson_free (find_reply);
   } else {
      reply_to_request (request, MONGOC_REPLY_NONE, cursor_id, 0, number_returned, reply_json);
   }
   bson_free (db);
}


/*--------------------------------------------------------------------------
 *
 * mock_server_destroy --
 *
 *       Free a mock_server_t.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Closes sockets, joins threads, and calls destructors passed
 *       to mock_server_autoresponds.
 *
 *--------------------------------------------------------------------------
 */

void
mock_server_destroy (mock_server_t *server)
{
   size_t i;
   autoresponder_handle_t *handle;
   int64_t deadline = bson_get_monotonic_time () + 10 * 1000 * 1000;
   request_t *request;

   if (!server) {
      return;
   }

   bson_mutex_lock (&server->mutex);
   if (server->running) {
      server->stopped = true;
   }
   bson_mutex_unlock (&server->mutex);

   while (bson_get_monotonic_time () <= deadline) {
      /* wait 10 seconds */
      bson_mutex_lock (&server->mutex);
      if (!server->running) {
         bson_mutex_unlock (&server->mutex);
         break;
      }

      bson_mutex_unlock (&server->mutex);
      _mongoc_usleep (1000);
   }

   bson_mutex_lock (&server->mutex);
   if (server->running) {
      test_error ("server still running after timeout");
   }

   bson_mutex_unlock (&server->mutex);
   mcommon_thread_join (server->main_thread);

   _mongoc_array_destroy (&server->worker_threads);

   for (i = 0; i < server->autoresponders.len; i++) {
      handle = &_mongoc_array_index (&server->autoresponders, autoresponder_handle_t, i);

      autoresponder_handle_destroy (handle);
   }

   _mongoc_array_destroy (&server->autoresponders);

   mongoc_cond_destroy (&server->cond);
   bson_mutex_destroy (&server->mutex);
   mongoc_socket_destroy (server->sock);
   bson_free (server->uri_str);
   mongoc_uri_destroy (server->uri);

   while ((request = (request_t *) q_get_nowait (server->q))) {
      request_destroy (request);
   }

   q_destroy (server->q);
   bson_free (server);
}


static uint16_t
get_port (mongoc_socket_t *sock)
{
   struct sockaddr_storage bound_addr = {0};
   mongoc_socklen_t addr_len = (mongoc_socklen_t) sizeof bound_addr;

   if (mongoc_socket_getsockname (sock, (struct sockaddr *) &bound_addr, &addr_len) < 0) {
      perror ("Failed to get listening port number");
      return 0;
   }

   if (bound_addr.ss_family == AF_INET6) {
      return ntohs (((struct sockaddr_in6 *) &bound_addr)->sin6_port);
   } else {
      return ntohs (((struct sockaddr_in *) &bound_addr)->sin_port);
   }
}


static bool
_mock_server_stopping (mock_server_t *server)
{
   bool stopped;

   bson_mutex_lock (&server->mutex);
   stopped = server->stopped;
   bson_mutex_unlock (&server->mutex);

   return stopped;
}


typedef struct worker_closure_t {
   mock_server_t *server;
   mongoc_stream_t *client_stream;
   uint16_t port;
} worker_closure_t;


static BSON_THREAD_FUN (main_thread, data)
{
   mock_server_t *server = (mock_server_t *) data;
   mongoc_socket_t *client_sock;
   uint16_t port;
   mongoc_stream_t *client_stream;
   worker_closure_t *closure;
   bson_thread_t thread;
   mongoc_array_t worker_threads;
   size_t i;
   int r;

   bson_mutex_lock (&server->mutex);
   server->running = true;
   mongoc_cond_signal (&server->cond);
   bson_mutex_unlock (&server->mutex);

   for (;;) {
      client_sock = mongoc_socket_accept_ex (server->sock, bson_get_monotonic_time () + 100 * 1000, &port);

      if (_mock_server_stopping (server)) {
         mongoc_socket_destroy (client_sock);
         break;
      }

      if (client_sock) {
         test_suite_mock_server_log (
            "%5.2f  %hu -> server port %hu (connected)", mock_server_get_uptime_sec (server), port, server->port);

         client_stream = mongoc_stream_socket_new (client_sock);

#ifdef MONGOC_ENABLE_SSL
         bson_mutex_lock (&server->mutex);
         if (server->ssl) {
            mongoc_stream_t *tls_stream;
            server->ssl_opts.weak_cert_validation = 1;
            tls_stream = mongoc_stream_tls_new_with_hostname (client_stream, NULL, &server->ssl_opts, 0);
            if (!tls_stream) {
               mongoc_stream_destroy (client_stream);
               bson_mutex_unlock (&server->mutex);
               perror ("Failed to attach tls stream");
               break;
            }
            client_stream = tls_stream;
         }
         bson_mutex_unlock (&server->mutex);
#endif
         closure = (worker_closure_t *) bson_malloc (sizeof *closure);
         closure->server = server;
         closure->client_stream = client_stream;
         closure->port = port;

         bson_mutex_lock (&server->mutex);
         r = mcommon_thread_create (&thread, worker_thread, closure);
         BSON_ASSERT (r == 0);
         _mongoc_array_append_val (&server->worker_threads, thread);
         bson_mutex_unlock (&server->mutex);
      }
   }

   /* copy list of worker threads and join them all */
   _mongoc_array_init (&worker_threads, sizeof (bson_thread_t));
   bson_mutex_lock (&server->mutex);
   _mongoc_array_copy (&worker_threads, &server->worker_threads);
   bson_mutex_unlock (&server->mutex);

   for (i = 0; i < worker_threads.len; i++) {
      mcommon_thread_join (_mongoc_array_index (&worker_threads, bson_thread_t, i));
   }

   _mongoc_array_destroy (&worker_threads);

   bson_mutex_lock (&server->mutex);
   server->running = false;
   bson_mutex_unlock (&server->mutex);

   BSON_THREAD_RETURN;
}


static void
_reply_destroy (reply_t *reply)
{
   int i;

   for (i = 0; i < reply->n_docs; i++) {
      bson_destroy (&reply->docs[i]);
   }

   bson_free (reply->docs);
   bson_free (reply);
}


static BSON_THREAD_FUN (worker_thread, data)
{
   worker_closure_t *closure = (worker_closure_t *) data;
   mock_server_t *server = closure->server;
   mongoc_stream_t *client_stream = closure->client_stream;
   mongoc_buffer_t buffer;
   bool handled;
   bson_error_t error;
   int32_t msg_len;
   sync_queue_t *requests;
   sync_queue_t *replies;
   request_t *request;
   mongoc_array_t autoresponders;
   ssize_t i;
   autoresponder_handle_t handle;
   reply_t *reply;

#ifdef MONGOC_ENABLE_SSL
   bool ssl;
#endif

   ENTRY;

   /* queue of client replies sent over this worker's connection */
   replies = q_new ();

#ifdef MONGOC_ENABLE_SSL
   bson_mutex_lock (&server->mutex);
   ssl = server->ssl;
   bson_mutex_unlock (&server->mutex);

   if (ssl) {
      if (!mongoc_stream_tls_handshake_block (client_stream, "localhost", TIMEOUT, &error)) {
         mongoc_stream_close (client_stream);
         mongoc_stream_destroy (client_stream);
         bson_free (closure);
         q_destroy (replies);
         BSON_THREAD_RETURN;
      }
   }
#endif

   _mongoc_buffer_init (&buffer, NULL, 0, NULL, NULL);
   _mongoc_array_init (&autoresponders, sizeof (autoresponder_handle_t));

again:
   /* loop, checking for requests to receive or replies to send */
   if (_mongoc_buffer_fill (&buffer, client_stream, 4, 10, &error) > 0) {
      BSON_ASSERT (buffer.len >= 4);

      memcpy (&msg_len, buffer.data, 4);
      msg_len = BSON_UINT32_FROM_LE (msg_len);

      if (msg_len < 16) {
         MONGOC_WARNING ("No data");
         GOTO (failure);
      }

      if (_mongoc_buffer_fill (&buffer, client_stream, (size_t) msg_len, -1, &error) == -1) {
         MONGOC_WARNING ("%s():%d: %s", BSON_FUNC, __LINE__, error.message);
         GOTO (failure);
      }

      BSON_ASSERT (buffer.len >= (unsigned) msg_len);

      /* copies message from buffer */
      request = request_new (&buffer, msg_len, server, client_stream, closure->port, replies);

      memmove (buffer.data, buffer.data + msg_len, buffer.len - msg_len);
      buffer.len -= msg_len;

      bson_mutex_lock (&server->mutex);
      _mongoc_array_copy (&autoresponders, &server->autoresponders);
      bson_mutex_unlock (&server->mutex);

      test_suite_mock_server_log (
         "%5.2f  %hu -> %hu %s", mock_server_get_uptime_sec (server), closure->port, server->port, request->as_str);

      /* run responders most-recently-added-first */
      handled = false;

      for (i = server->autoresponders.len - 1u; i >= 0; i--) {
         handle = _mongoc_array_index (&server->autoresponders, autoresponder_handle_t, i);

         if (handle.responder (request, handle.data)) {
            /* responder destroyed request and enqueued a reply in "replies" */
            handled = true;
            request = NULL;
            break;
         }
      }

      if (!handled) {
         /* pass to the main thread via the queue */
         requests = mock_server_get_queue (server);
         q_put (requests, (void *) request);
      }
   }

   if (_mock_server_stopping (server)) {
      GOTO (failure);
   }

   reply = q_get (replies, 10);
   if (reply) {
      _mock_server_reply_with_stream (server, reply, client_stream);
      _reply_destroy (reply);
   }

   if (_mock_server_stopping (server)) {
      GOTO (failure);
   }

   GOTO (again);

failure:
   _mongoc_array_destroy (&autoresponders);
   _mongoc_buffer_destroy (&buffer);

   mongoc_stream_close (client_stream);
   mongoc_stream_destroy (client_stream);
   bson_free (closure);
   _mongoc_buffer_destroy (&buffer);

   while ((reply = q_get_nowait (replies))) {
      _reply_destroy (reply);
   }

   q_destroy (replies);

   BSON_THREAD_RETURN;
}


/* enqueue server reply for this connection's worker thread to send to client */
void
reply_to_request_with_multiple_docs (
   request_t *request, mongoc_reply_flags_t flags, const bson_t *docs, int n_docs, int64_t cursor_id)
{
   reply_t *reply;
   int i;

   BSON_ASSERT (request);

   reply = bson_malloc0 (sizeof (reply_t));

   reply->type = REPLY;
   reply->flags = flags;
   reply->n_docs = n_docs;
   reply->docs = bson_aligned_alloc0 (BSON_ALIGNOF (bson_t), (size_t) n_docs * sizeof (bson_t));

   for (i = 0; i < n_docs; i++) {
      bson_copy_to (&docs[i], &reply->docs[i]);
   }

   reply->cursor_id = cursor_id;
   reply->client_port = request_get_client_port (request);
   reply->response_to = mcd_rpc_header_get_request_id (request->rpc);
   reply->request_opcode = mcd_rpc_header_get_op_code (request->rpc);

   if (reply->request_opcode == MONGOC_OP_CODE_MSG) {
      reply->opmsg_flags = mcd_rpc_op_msg_get_flag_bits (request->rpc);
   } else {
      reply->query_flags = mcd_rpc_op_query_get_flags (request->rpc);
   }

   q_put (request->replies, reply);
}


static void
_mock_server_reply_with_stream (mock_server_t *server, reply_t *reply, mongoc_stream_t *client)
{
   char *doc_json;
   bson_string_t *docs_json;
   uint8_t *buf;
   uint8_t *ptr;
   size_t len;
   bool is_op_msg;
   mongoc_reply_flags_t flags = reply->flags;
   const bson_t *docs = reply->docs;
   int n_docs = reply->n_docs;
   int64_t cursor_id = reply->cursor_id;

   if (reply->type == HANGUP) {
      mongoc_stream_close (client);
      return;
   } else if (reply->type == RESET) {
      struct linger no_linger;
      no_linger.l_onoff = 1;
      no_linger.l_linger = 0;

      /* send RST packet to client */
      mongoc_stream_setsockopt (client, SOL_SOCKET, SO_LINGER, &no_linger, sizeof no_linger);

      mongoc_stream_close (client);
      return;
   }

   docs_json = bson_string_new ("");
   for (int i = 0; i < n_docs; i++) {
      doc_json = bson_as_json (&docs[i], NULL);
      bson_string_append (docs_json, doc_json);
      bson_free (doc_json);
      if (i < n_docs - 1) {
         bson_string_append (docs_json, ", ");
      }
   }

   is_op_msg = reply->request_opcode == MONGOC_OPCODE_MSG;

   test_suite_mock_server_log ("%5.2f  %hu <- %hu %s %s",
                               mock_server_get_uptime_sec (server),
                               reply->client_port,
                               mock_server_get_port (server),
                               is_op_msg ? "OP_MSG" : "OP_REPLY",
                               docs_json->str);

   len = 0;

   for (int i = 0; i < n_docs; i++) {
      len += docs[i].len;
   }

   ptr = buf = bson_malloc (len);

   for (int i = 0; i < n_docs; i++) {
      memcpy (ptr, bson_get_data (&docs[i]), docs[i].len);
      ptr += docs[i].len;
   }

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();
   int32_t message_len = 0;

   bson_mutex_lock (&server->mutex);

   if (!(reply->request_opcode == MONGOC_OPCODE_QUERY && reply->query_flags & MONGOC_QUERY_EXHAUST)) {
      server->last_response_id++;
   }

   message_len += mcd_rpc_header_set_request_id (rpc, server->last_response_id);
   bson_mutex_unlock (&server->mutex);
   message_len += mcd_rpc_header_set_message_length (rpc, 0);
   message_len += mcd_rpc_header_set_response_to (rpc, reply->response_to);

   if (is_op_msg) {
      message_len += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_MSG);
      mcd_rpc_op_msg_set_sections_count (rpc, 1);
      message_len += mcd_rpc_op_msg_set_flag_bits (rpc, reply->opmsg_flags);
      /* we don't yet implement payload type 1, a document stream */
      message_len += mcd_rpc_op_msg_section_set_kind (rpc, 0, 0);
      message_len += mcd_rpc_op_msg_section_set_body (rpc, 0, buf);
   } else {
      message_len += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_REPLY);
      message_len += mcd_rpc_op_reply_set_response_flags (rpc, (int32_t) flags);
      message_len += mcd_rpc_op_reply_set_cursor_id (rpc, cursor_id);
      message_len += mcd_rpc_op_reply_set_starting_from (rpc, 0);
      message_len += mcd_rpc_op_reply_set_number_returned (rpc, 1);
      message_len += mcd_rpc_op_reply_set_documents (rpc, buf, len);
   }
   mcd_rpc_message_set_length (rpc, message_len);

   size_t iovcnt;
   mongoc_iovec_t *const iov = mcd_rpc_message_to_iovecs (rpc, &iovcnt);
   BSON_ASSERT (iov);

   size_t expected = 0;
   for (size_t i = 0u; i < iovcnt; i++) {
      expected += iov[i].iov_len;
   }

   const ssize_t n_written = mongoc_stream_writev (client, iov, iovcnt, -1);

   BSON_ASSERT (bson_cmp_equal_su (n_written, expected));

   bson_free (iov);
   mcd_rpc_message_destroy (rpc);
   bson_string_free (docs_json, true);
   bson_free (buf);
}


void
autoresponder_handle_destroy (autoresponder_handle_t *handle)
{
   if (handle->destructor) {
      handle->destructor (handle->data);
   }
}

void
mock_server_set_bind_opts (mock_server_t *server, mock_server_bind_opts_t *opts)
{
   server->bind_opts = *opts;
}

void
rs_response_to_hello (mock_server_t *server, int max_wire_version, bool primary, int has_tags, ...)
{
   va_list ap;
   bson_string_t *hosts;
   bool first;
   mock_server_t *host;

   ASSERT_WITH_MSG (max_wire_version >= WIRE_VERSION_MIN,
                    "max_wire_version %" PRId32 " must be greater than or equal to minimum wire version %d",
                    max_wire_version,
                    WIRE_VERSION_MIN);

   hosts = bson_string_new ("");

   va_start (ap, has_tags);

   first = true;
   while ((host = va_arg (ap, mock_server_t *))) {
      if (first) {
         first = false;
      } else {
         bson_string_append (hosts, ",");
      }

      bson_string_append_printf (hosts, "'%s'", mock_server_get_host_and_port (host));
   }

   va_end (ap);

   mock_server_auto_endsessions (server);

   mock_server_auto_hello (server,
                           "{'ok': 1, "
                           " 'setName': 'rs',"
                           " 'isWritablePrimary': %s,"
                           " 'secondary': %s,"
                           " 'tags': {%s},"
                           " 'minWireVersion': %d,"
                           " 'maxWireVersion': %d,"
                           " 'hosts': [%s],"
                           " 'logicalSessionTimeoutMinutes': 30}",
                           primary ? "true" : "false",
                           primary ? "false" : "true",
                           has_tags ? "'key': 'value'" : "",
                           WIRE_VERSION_MIN,
                           max_wire_version,
                           hosts->str);

   bson_string_free (hosts, true);
}
