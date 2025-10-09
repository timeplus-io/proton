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


#include <bson/bson.h>
#include <mongoc/mongoc-util-private.h>

#include "mongoc/mongoc-client-private.h"

#include "mock-rs.h"
#include "sync-queue.h"
#include "test-libmongoc.h"
#include "TestSuite.h"


struct _mock_rs_t {
   bool has_primary;
   int n_secondaries;
   int n_arbiters;
   int32_t max_wire_version;
   int64_t request_timeout_msec;

   mock_server_t *primary;
   mongoc_array_t secondaries;
   mongoc_array_t arbiters;
   mongoc_array_t servers;

   char *hosts_str;
   mongoc_uri_t *uri;
   sync_queue_t *q;

   bson_t primary_tags;
   bson_t **secondary_tags;
};


mock_server_t *
get_server (mongoc_array_t *servers, size_t i)
{
   return _mongoc_array_index (servers, mock_server_t *, i);
}


void
append_array (mongoc_array_t *dst, mongoc_array_t *src)
{
   _mongoc_array_append_vals (dst, src->data, (uint32_t) src->len);
}


/* a string like: "localhost:1","localhost:2","localhost:3" */
char *
hosts (mongoc_array_t *servers)
{
   const char *host_and_port;
   bson_string_t *hosts_str = bson_string_new ("");

   for (size_t i = 0u; i < servers->len; i++) {
      host_and_port = mock_server_get_host_and_port (get_server (servers, i));
      bson_string_append_printf (hosts_str, "\"%s\"", host_and_port);

      if (i + 1u < servers->len) {
         bson_string_append_printf (hosts_str, ", ");
      }
   }

   return bson_string_free (hosts_str, false); /* detach buffer */
}


mongoc_uri_t *
make_uri (mongoc_array_t *servers)
{
   const char *host_and_port;
   bson_string_t *uri_str = bson_string_new ("mongodb://");
   mongoc_uri_t *uri;

   for (size_t i = 0u; i < servers->len; i++) {
      host_and_port = mock_server_get_host_and_port (get_server (servers, i));
      bson_string_append_printf (uri_str, "%s", host_and_port);

      if (i + 1u < servers->len) {
         bson_string_append_printf (uri_str, ",");
      }
   }

   bson_string_append_printf (uri_str, "/?replicaSet=rs");

   uri = mongoc_uri_new (uri_str->str);

   // Many mock server tests do not expect retryable handshakes. Disable by
   // default: tests that expect or require retryable handshakes must opt-in.
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYREADS, false);
   mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYWRITES, false);

   bson_string_free (uri_str, true);

   return uri;
}


static char *
hello_json (mock_rs_t *rs, mongoc_server_description_type_t type, const bson_t *tags)
{
   char *server_type;
   char *tags_json;
   char *hosts_str;
   char *json;

   if (type == MONGOC_SERVER_RS_PRIMARY) {
      server_type = "'isWritablePrimary': true, 'secondary': false,";
   } else if (type == MONGOC_SERVER_RS_SECONDARY) {
      server_type = "'isWritablePrimary': false, 'secondary': true,";
   } else {
      BSON_ASSERT (type == MONGOC_SERVER_RS_ARBITER);
      server_type = "'isWritablePrimary': false, 'arbiterOnly': true,";
   }

   if (bson_empty0 (tags)) {
      tags_json = bson_strdup ("{}");
   } else {
      tags_json = bson_as_json (tags, NULL);
   }

   hosts_str = hosts (&rs->servers);

   json = bson_strdup_printf ("{'ok': 1,"
                              " %s"
                              " '$clusterTime': {"
                              "   'clusterTime': {'$timestamp': {'t': 1, 'i': 1}},"
                              "   'signature': {"
                              "     'hash': {'$binary': {'subType': '0', 'base64': ''}},"
                              "     'keyId': {'$numberLong': '6446735049323708417'}"
                              "   },"
                              "   'operationTime': {'$timestamp': {'t': 1, 'i': 1}}"
                              " },"
                              "'logicalSessionTimeoutMinutes': 30,"
                              " 'tags': %s,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'setName': 'rs',"
                              " 'hosts': [%s]}",
                              server_type,
                              tags_json,
                              WIRE_VERSION_MIN,
                              rs->max_wire_version,
                              hosts_str);

   bson_free (tags_json);
   bson_free (hosts_str);

   return json;
}


static char *
primary_json (mock_rs_t *rs)
{
   return hello_json (rs, MONGOC_SERVER_RS_PRIMARY, &rs->primary_tags);
}


static char *
secondary_json (mock_rs_t *rs, int server_number)
{
   return hello_json (rs, MONGOC_SERVER_RS_SECONDARY, rs->secondary_tags[server_number]);
}


static char *
arbiter_json (mock_rs_t *rs)
{
   return hello_json (rs, MONGOC_SERVER_RS_ARBITER, NULL);
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_with_auto_hello --
 *
 *       A new mock replica set. Each member autoresponds to hello.
 *       Call mock_rs_run to start it, then mock_rs_get_uri to connect.
 *
 * Returns:
 *       A replica set you must mock_rs_destroy.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mock_rs_t *
mock_rs_with_auto_hello (int32_t max_wire_version, bool has_primary, int n_secondaries, int n_arbiters)
{
   int i;
   mock_rs_t *rs = BSON_ALIGNED_ALLOC0 (mock_rs_t);

   ASSERT_WITH_MSG (max_wire_version >= WIRE_VERSION_MIN,
                    "max_wire_version %" PRId32 " must be greater than or equal to minimum wire version %d",
                    max_wire_version,
                    WIRE_VERSION_MIN);

   rs->max_wire_version = max_wire_version;
   rs->has_primary = has_primary;
   rs->n_secondaries = n_secondaries;
   rs->n_arbiters = n_arbiters;
   rs->request_timeout_msec = get_future_timeout_ms ();
   rs->q = q_new ();
   bson_init (&rs->primary_tags);
   rs->secondary_tags = bson_malloc (n_secondaries * sizeof (bson_t *));

   for (i = 0; i < n_secondaries; i++) {
      rs->secondary_tags[i] = bson_new ();
   }

   return rs;
}


void
mock_rs_tag_secondary (mock_rs_t *rs, int server_number, const bson_t *tags)
{
   bson_destroy (rs->secondary_tags[server_number]);
   rs->secondary_tags[server_number] = bson_copy (tags);
}


static void
mock_rs_auto_endsessions (mock_rs_t *rs)
{
   for (size_t i = 0u; i < rs->servers.len; i++) {
      mock_server_auto_endsessions (get_server (&rs->servers, i));
   }
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_set_request_timeout_msec --
 *
 *       How long mock_rs_receives_* functions wait for a client
 *       request before giving up and returning NULL.
 *
 *--------------------------------------------------------------------------
 */

void
mock_rs_set_request_timeout_msec (mock_rs_t *rs, int64_t request_timeout_msec)
{
   rs->request_timeout_msec = request_timeout_msec;
}


static bool
rs_q_append (request_t *request, void *data)
{
   mock_rs_t *rs = (mock_rs_t *) data;

   q_put (rs->q, (void *) request);

   return true; /* handled */
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_run --
 *
 *       Start each member listening on an unused port. After this, call
 *       mock_rs_get_uri to connect.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       The replica set's URI is set.
 *
 *--------------------------------------------------------------------------
 */

void
mock_rs_run (mock_rs_t *rs)
{
   mock_server_t *server;
   char *hello;

   if (rs->has_primary) {
      /* start primary */
      rs->primary = mock_server_new ();
      mock_server_run (rs->primary);
   }

   /* start secondaries */
   _mongoc_array_init (&rs->secondaries, sizeof (mock_server_t *));

   for (int i = 0; i < rs->n_secondaries; i++) {
      server = mock_server_new ();
      mock_server_run (server);
      _mongoc_array_append_val (&rs->secondaries, server);
   }

   /* start arbiters */
   _mongoc_array_init (&rs->arbiters, sizeof (mock_server_t *));

   for (int i = 0; i < rs->n_arbiters; i++) {
      server = mock_server_new ();
      mock_server_run (server);
      _mongoc_array_append_val (&rs->arbiters, server);
   }

   /* add all servers to replica set */
   _mongoc_array_init (&rs->servers, sizeof (mock_server_t *));
   if (rs->has_primary) {
      _mongoc_array_append_val (&rs->servers, rs->primary);
   }

   append_array (&rs->servers, &rs->secondaries);
   append_array (&rs->servers, &rs->arbiters);

   /* enqueue unhandled requests in rs->q, they're retrieved with
    * mock_rs_receives_query() &co. rs_q_append is added first so it
    * runs last, after auto_hello.
    */
   for (size_t i = 0u; i < rs->servers.len; i++) {
      mock_server_autoresponds (get_server (&rs->servers, i), rs_q_append, (void *) rs, NULL);
   }


   /* now we know all servers' ports and we have them in one array */
   rs->hosts_str = hosts (&rs->servers);
   rs->uri = make_uri (&rs->servers);

   BSON_ASSERT (rs->max_wire_version > 0);
   if (rs->has_primary) {
      /* primary's hello response */
      hello = primary_json (rs);
      mock_server_auto_hello (rs->primary, hello);
      bson_free (hello);
   }

   /* secondaries' hello response */
   for (int i = 0; i < rs->n_secondaries; i++) {
      hello = secondary_json (rs, i);
      mock_server_auto_hello (get_server (&rs->secondaries, (size_t) i), hello);
      bson_free (hello);
   }

   /* arbiters' hello response */
   hello = arbiter_json (rs);

   for (int i = 0; i < rs->n_arbiters; i++) {
      mock_server_auto_hello (get_server (&rs->arbiters, (size_t) i), hello);
   }

   bson_free (hello);

   mock_rs_auto_endsessions (rs);
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_get_uri --
 *
 *       Call after mock_rs_run to get the connection string.
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
mock_rs_get_uri (mock_rs_t *rs)
{
   return rs->uri;
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_receives_request --
 *
 *       Pop a client request if one is enqueued, or wait up to
 *       request_timeout_ms for the client to send a request.
 *
 * Returns:
 *       A request you must request_destroy, or NULL if the request.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

request_t *
mock_rs_receives_request (mock_rs_t *rs)
{
   return (request_t *) q_get (rs->q, rs->request_timeout_msec);
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_receives_msg --
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
_mock_rs_receives_msg (mock_rs_t *rs, uint32_t flags, ...)
{
   request_t *request;
   va_list args;
   bool r;

   request = (request_t *) q_get (rs->q, rs->request_timeout_msec);

   ASSERT_WITH_MSG (request, "expected an OP_MSG request but did not receive one!");

   va_start (args, flags);
   r = request_matches_msgv (request, flags, &args);
   va_end (args);

   if (!r) {
      request_destroy (request);
      return NULL;
   }

   return request;
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_receives_kill_cursors --
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
mock_rs_receives_kill_cursors (mock_rs_t *rs, int64_t cursor_id)
{
   request_t *request;

   request = (request_t *) q_get (rs->q, rs->request_timeout_msec);

   if (request && !request_matches_kill_cursors (request, cursor_id)) {
      request_destroy (request);
      return NULL;
   }

   return request;
}


static mongoc_server_description_type_t
_mock_rs_server_type (mock_rs_t *rs, uint16_t port)
{
   if (rs->primary && port == mock_server_get_port (rs->primary)) {
      return MONGOC_SERVER_RS_PRIMARY;
   }

   for (size_t i = 0u; i < rs->secondaries.len; i++) {
      if (port == mock_server_get_port (get_server (&rs->secondaries, i))) {
         return MONGOC_SERVER_RS_SECONDARY;
      }
   }

   for (size_t i = 0u; i < rs->arbiters.len; i++) {
      if (port == mock_server_get_port (get_server (&rs->arbiters, i))) {
         return MONGOC_SERVER_RS_ARBITER;
      }
   }

   return MONGOC_SERVER_UNKNOWN;
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_request_is_to_primary --
 *
 *       Check that the request is non-NULL and sent to a
 *       primary in this replica set.
 *
 * Returns:
 *       True if so.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
mock_rs_request_is_to_primary (mock_rs_t *rs, request_t *request)
{
   BSON_ASSERT (request);

   return MONGOC_SERVER_RS_PRIMARY == _mock_rs_server_type (rs, request_get_server_port (request));
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_request_is_to_secondary --
 *
 *       Check that the request is non-NULL and sent to a
 *       secondary in this replica set.
 *
 * Returns:
 *       True if so.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
mock_rs_request_is_to_secondary (mock_rs_t *rs, request_t *request)
{
   BSON_ASSERT (request);

   return MONGOC_SERVER_RS_SECONDARY == _mock_rs_server_type (rs, request_get_server_port (request));
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_stepdown --
 *
 *       Change the primary to a secondary.
 *
 *--------------------------------------------------------------------------
 */

void
mock_rs_stepdown (mock_rs_t *rs)
{
   char *json;

   BSON_ASSERT (rs->primary);
   rs->n_secondaries++;
   rs->secondary_tags = bson_realloc (rs->secondary_tags, rs->n_secondaries * sizeof (bson_t *));

   rs->secondary_tags[rs->n_secondaries - 1] = bson_copy (&rs->primary_tags);
   bson_reinit (&rs->primary_tags);

   json = secondary_json (rs, rs->n_secondaries - 1);
   mock_server_auto_hello (rs->primary, json);
   bson_free (json);

   _mongoc_array_append_val (&rs->secondaries, rs->primary);
   rs->primary = NULL;
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_elect --
 *
 *       Change a secondary to the primary.
 *
 *--------------------------------------------------------------------------
 */

void
mock_rs_elect (mock_rs_t *rs, size_t id)
{
   char *json;
   mock_server_t **ptrs;

   BSON_ASSERT (!rs->primary);
   BSON_ASSERT (id < rs->secondaries.len);

   rs->primary = get_server (&rs->secondaries, id);

   /* as the secondary becomes primary, its tags come along */
   bson_destroy (&rs->primary_tags);
   BSON_ASSERT (bson_steal (&rs->primary_tags, rs->secondary_tags[id]));

   /* primary_json() uses the current primary_tags */
   json = primary_json (rs);
   mock_server_auto_hello (rs->primary, json);
   bson_free (json);

   ptrs = (mock_server_t **) rs->secondaries.data;

   for (size_t i = id + 1u; i < rs->secondaries.len; i++) {
      ptrs[i - 1u] = ptrs[i];
      rs->secondary_tags[i - 1u] = rs->secondary_tags[i];
   }

   rs->secondaries.len--;
   rs->n_secondaries--;
}


/*--------------------------------------------------------------------------
 *
 * mock_rs_destroy --
 *
 *       Free a mock_rs_t.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Destroys each member mock_server_t, closes sockets, joins threads.
 *
 *--------------------------------------------------------------------------
 */

void
mock_rs_destroy (mock_rs_t *rs)
{
   for (size_t i = 0u; i < rs->servers.len; i++) {
      mock_server_destroy (get_server (&rs->servers, i));
   }

   _mongoc_array_destroy (&rs->secondaries);
   _mongoc_array_destroy (&rs->arbiters);
   _mongoc_array_destroy (&rs->servers);

   bson_free (rs->hosts_str);
   mongoc_uri_destroy (rs->uri);
   q_destroy (rs->q);

   bson_destroy (&rs->primary_tags);
   for (int i = 0u; i < rs->n_secondaries; i++) {
      bson_destroy (rs->secondary_tags[i]);
   }

   bson_free (rs->secondary_tags);
   bson_free (rs);
}
