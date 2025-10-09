#include <fcntl.h>
#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-cursor-private.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-util-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "exhaust-test"

/* Server support for exhaust cursors depends on server topology and version:
 * Server Version      Server Behavior
 * ------------------  -----------------------------------
 * mongod <4.2         only supports with OP_QUERY.
 * mongod >=4.2,<5.1   supports with OP_MSG and OP_QUERY.
 * mongod >=5.1        only supports with OP_MSG.

 * mongos <7.1         does not support.
 * mongos >=7.1        only supports with OP_MSG.


 * When attempting to create an exhaust cursor, libmongoc behaves as follows:

 * Server Version      libmongoc behavior
 * ------------------  -----------------------------------
 * mongod <4.2         uses OP_QUERY.
 * mongod >=4.2,<5.1   uses OP_MSG.
 * mongod >=5.1        uses OP_MSG.

 * mongos <7.1         returns error
 * mongos >=7.1        uses OP_MSG.
 */

static int
skip_if_no_exhaust (void)
{
   if (!TestSuite_CheckLive ()) {
      return 0;
   }

   int64_t max_wire_version;
   test_framework_get_max_wire_version (&max_wire_version);

   // mongos only supports exhaust cursors on server version 7.1+.
   if (test_framework_is_mongos ()) {
      if (max_wire_version >= WIRE_VERSION_MONGOS_EXHAUST) {
         return 1;
      }
      return 0;
   }

   // mongod supports exhaust cursors over OP_MSG or OP_QUERY for all versions
   // supported by libmongoc.

   return 1;
}


static uint32_t
get_generation (mongoc_client_t *client, mongoc_cursor_t *cursor)
{
   uint32_t server_id;
   uint32_t generation;
   mongoc_server_description_t const *sd;
   bson_error_t error;
   ASSERT (client);

   mc_shared_tpld td = mc_tpld_take_ref (client->topology);

   server_id = mongoc_cursor_get_hint (cursor);

   sd = mongoc_topology_description_server_by_id_const (td.ptr, server_id, &error);
   ASSERT_OR_PRINT (sd, error);
   generation = mc_tpl_sd_get_generation (sd, &kZeroServiceId);
   mc_tpld_drop_ref (&td);

   return generation;
}

static uint32_t
get_connection_count (mongoc_client_t *client)
{
   bson_error_t error;
   bson_t cmd = BSON_INITIALIZER;
   bson_t reply;
   bool res;
   int conns;

   ASSERT (client);

   BSON_APPEND_INT32 (&cmd, "serverStatus", 1);
   res = mongoc_client_command_simple (client, "admin", &cmd, NULL, &reply, &error);
   ASSERT_OR_PRINT (res, error);

   conns = bson_lookup_int32 (&reply, "connections.totalCreated");
   bson_destroy (&cmd);
   bson_destroy (&reply);
   return conns;
}

static void
test_exhaust_cursor (bool pooled)
{
   mongoc_stream_t *stream;
   mongoc_write_concern_t *wr;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool = NULL;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   mongoc_cursor_t *cursor2;
   const bson_t *doc;
   bson_t q;
   bson_t b[10];
   bson_t *bptr[10];
   int i;
   bool r;
   uint32_t server_id;
   bson_error_t error;
   bson_oid_t oid;
   int64_t generation1;
   uint32_t connection_count1;
   mongoc_client_t *audit_client;

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_new_default_client ();
   }
   BSON_ASSERT (client);

   /* Use a separate client to count connections. */
   audit_client = test_framework_new_default_client ();

   collection = get_test_collection (client, "test_exhaust_cursor");
   BSON_ASSERT (collection);

   /* don't care if ns not found. */
   (void) mongoc_collection_drop (collection, &error);

   wr = mongoc_write_concern_new ();
   mongoc_write_concern_set_journal (wr, true);

   /* bulk insert some records to work on */
   {
      bson_init (&q);

      for (i = 0; i < 10; i++) {
         bson_init (&b[i]);
         bson_oid_init (&oid, NULL);
         bson_append_oid (&b[i], "_id", -1, &oid);
         bson_append_int32 (&b[i], "n", -1, i % 2);
         bptr[i] = &b[i];
      }

      BEGIN_IGNORE_DEPRECATIONS
      ASSERT_OR_PRINT (
         mongoc_collection_insert_bulk (collection, MONGOC_INSERT_NONE, (const bson_t **) bptr, 10, wr, &error), error);
      END_IGNORE_DEPRECATIONS
   }

   /* create a couple of cursors */
   {
      cursor = mongoc_collection_find (collection, MONGOC_QUERY_EXHAUST, 0, 0, 5, &q, NULL, NULL);

      cursor2 = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &q, NULL, NULL);
   }

   /* Read from the exhaust cursor, ensure that we're in exhaust where we
    * should be and ensure that an early destroy properly causes a disconnect
    * */
   {
      r = mongoc_cursor_next (cursor, &doc);
      if (!r) {
         mongoc_cursor_error (cursor, &error);
         fprintf (stderr, "cursor error: %s\n", error.message);
      }
      BSON_ASSERT (r);
      BSON_ASSERT (doc);

      // With OP_MSG, a cursor only becomes exhaust after the first getMore
      while (!cursor->in_exhaust && mongoc_cursor_next (cursor, &doc))
         ;
      BSON_ASSERT (client->in_exhaust);

      /* destroy the cursor, make sure the connection pool was not cleared */
      generation1 = get_generation (client, cursor);
      /* Getting the connection count requires a new enough server. */
      connection_count1 = get_connection_count (audit_client);
      mongoc_cursor_destroy (cursor);
      BSON_ASSERT (!client->in_exhaust);
   }

   /* Grab a new exhaust cursor, then verify that reading from that cursor
    * (putting the client into exhaust), breaks a mid-stream read from a
    * regular cursor */
   {
      cursor = mongoc_collection_find (collection, MONGOC_QUERY_EXHAUST, 0, 0, 5, &q, NULL, NULL);

      r = mongoc_cursor_next (cursor2, &doc);
      if (!r) {
         mongoc_cursor_error (cursor2, &error);
         fprintf (stderr, "cursor error: %s\n", error.message);
      }
      BSON_ASSERT (r);
      BSON_ASSERT (doc);
      /* The pool was not cleared. */
      ASSERT_CMPINT64 (generation1, ==, get_generation (client, cursor2));
      /* But a new connection was made. */
      ASSERT_CMPINT32 (connection_count1 + 1, ==, get_connection_count (audit_client));

      for (i = 0; i < 5; i++) {
         r = mongoc_cursor_next (cursor2, &doc);
         if (!r) {
            mongoc_cursor_error (cursor2, &error);
            fprintf (stderr, "cursor error: %s\n", error.message);
         }
         BSON_ASSERT (r);
         BSON_ASSERT (doc);
      }

      while (!cursor->in_exhaust && (r = mongoc_cursor_next (cursor, &doc)))
         ;
      BSON_ASSERT (client->in_exhaust);
      BSON_ASSERT (r);
      BSON_ASSERT (doc);

      doc = NULL;
      r = mongoc_cursor_next (cursor2, &doc);
      BSON_ASSERT (!r);
      BSON_ASSERT (!doc);

      mongoc_cursor_error (cursor2, &error);
      ASSERT_CMPUINT32 (error.domain, ==, MONGOC_ERROR_CLIENT);
      ASSERT_CMPUINT32 (error.code, ==, MONGOC_ERROR_CLIENT_IN_EXHAUST);

      mongoc_cursor_destroy (cursor2);
   }

   /* make sure writes fail as well */
   {
      BEGIN_IGNORE_DEPRECATIONS
      r = mongoc_collection_insert_bulk (collection, MONGOC_INSERT_NONE, (const bson_t **) bptr, 10, wr, &error);
      END_IGNORE_DEPRECATIONS

      BSON_ASSERT (!r);
      ASSERT_CMPUINT32 (error.domain, ==, MONGOC_ERROR_CLIENT);
      ASSERT_CMPUINT32 (error.code, ==, MONGOC_ERROR_CLIENT_IN_EXHAUST);
   }

   /* we're still in exhaust.
    *
    * 1. check that we can create a new cursor, as long as we don't read from it
    * 2. fully exhaust the exhaust cursor
    * 3. make sure that we don't disconnect at destroy
    * 4. make sure we can read the cursor we made during the exhaust
    */
   {
      cursor2 = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &q, NULL, NULL);

      server_id = cursor->server_id;
      stream = (mongoc_stream_t *) mongoc_set_get (client->cluster.nodes, server_id);

      while (cursor->in_exhaust && (r = mongoc_cursor_next (cursor, &doc)))
         ;
      BSON_ASSERT (!r);
      BSON_ASSERT (!doc);

      mongoc_cursor_destroy (cursor);

      BSON_ASSERT (stream == (mongoc_stream_t *) mongoc_set_get (client->cluster.nodes, server_id));

      r = mongoc_cursor_next (cursor2, &doc);
      BSON_ASSERT (r);
      BSON_ASSERT (doc);
   }

   bson_destroy (&q);
   for (i = 0; i < 10; i++) {
      bson_destroy (&b[i]);
   }

   ASSERT_OR_PRINT (mongoc_collection_drop (collection, &error), error);

   mongoc_write_concern_destroy (wr);
   mongoc_cursor_destroy (cursor2);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
   mongoc_client_destroy (audit_client);
}

static void
test_exhaust_cursor_works (void *context)
{
   BSON_UNUSED (context);
   bson_error_t error;
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll = mongoc_client_get_collection (client, "db", "coll");

   // Drop collection to remove prior test data.
   mongoc_collection_drop (coll, NULL /* ignore error */);

   // Insert enough documents to require more than one batch.
   // With OP_MSG, a cursor only becomes exhaust after the first getMore.
   for (int i = 0; i < 5; i++) {
      bool ok =
         mongoc_collection_insert_one (coll, tmp_bson ("{'i': %d}", i), NULL /* opts */, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ok, error);
   }

   mongoc_cursor_t *cursor = mongoc_collection_find_with_opts (
      coll, tmp_bson ("{}"), tmp_bson ("{'exhaust': true, 'batchSize': 2}"), NULL /* read_prefs */);
   const bson_t *result;
   while (mongoc_cursor_next (cursor, &result))
      ;
   // Expect an error if exhaust cursors are not supported.
   const bool sharded = _mongoc_topology_get_type (cursor->client->topology) == MONGOC_TOPOLOGY_SHARDED;
   int64_t wire_version;
   test_framework_get_max_wire_version (&wire_version);
   if (sharded && wire_version < WIRE_VERSION_MONGOS_EXHAUST) {
      ASSERT (mongoc_cursor_error (cursor, &error));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "exhaust cursors require");
   } else {
      // Expect no error.
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   }

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

// `test_exhaust_cursor_no_match` is a regression test for CDRIVER-5515
static void
test_exhaust_cursor_no_match (void *context)
{
   BSON_UNUSED (context);
   bson_error_t error;
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll = mongoc_client_get_collection (client, "db", "coll");

   // Drop collection to remove prior test data.
   mongoc_collection_drop (coll, NULL /* ignore error */);

   mongoc_cursor_t *cursor =
      mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), tmp_bson ("{'exhaust': true }"), NULL /* read_prefs */);
   const bson_t *result;
   size_t count = 0;
   while (mongoc_cursor_next (cursor, &result)) {
      count++;
   }
   // Expect an error if exhaust cursors are not supported.
   const bool sharded = _mongoc_topology_get_type (cursor->client->topology) == MONGOC_TOPOLOGY_SHARDED;
   int64_t wire_version;
   test_framework_get_max_wire_version (&wire_version);
   if (sharded && wire_version < WIRE_VERSION_MONGOS_EXHAUST) {
      ASSERT (mongoc_cursor_error (cursor, &error));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "exhaust cursors require");
   } else {
      // Expect no error.
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      // Expect no results.
      ASSERT_CMPSIZE_T (count, ==, 0);
   }

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


static void
test_exhaust_cursor_single (void *context)
{
   BSON_UNUSED (context);

   test_exhaust_cursor (false);
}

static void
test_exhaust_cursor_pool (void *context)
{
   BSON_UNUSED (context);

   test_exhaust_cursor (true);
}

static void
test_exhaust_cursor_multi_batch (void *context)
{
   mongoc_client_t *client;
   bson_error_t error;
   mongoc_collection_t *collection;
   bson_t doc = BSON_INITIALIZER;
   mongoc_bulk_operation_t *bulk;
   int i;
   uint32_t server_id;
   mongoc_cursor_t *cursor;
   const bson_t *cursor_doc;

   BSON_UNUSED (context);

   client = test_framework_new_default_client ();
   collection = get_test_collection (client, "test_exhaust_cursor_multi_batch");

   ASSERT_OR_PRINT (collection, error);

   BSON_APPEND_UTF8 (&doc, "key", "value");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

   /* enough to require more than initial batch */
   for (i = 0; i < 1000; i++) {
      mongoc_bulk_operation_insert (bulk, &doc);
   }

   server_id = mongoc_bulk_operation_execute (bulk, NULL, &error);
   ASSERT_OR_PRINT (server_id, error);

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_EXHAUST, 0, 0, 10, tmp_bson ("{}"), NULL, NULL);

   i = 0;
   while (mongoc_cursor_next (cursor, &cursor_doc)) {
      i++;
      ASSERT (mongoc_cursor_more (cursor));
   }

   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   ASSERT (!mongoc_cursor_more (cursor));
   ASSERT_CMPINT (i, ==, 1000);

   mongoc_cursor_destroy (cursor);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   bson_destroy (&doc);
   mongoc_client_destroy (client);
}

static void
test_cursor_server_hint_with_exhaust (void *unused)
{
   BSON_UNUSED (unused);
   bson_error_t error;
   mongoc_client_t *client = test_framework_new_default_client ();
   mongoc_collection_t *coll = get_test_collection (client, "cursor_server_hint_with_exhaust");
   mongoc_cursor_t *cursor = mongoc_collection_find_with_opts (coll,
                                                               tmp_bson ("{}"),
                                                               tmp_bson ("{'exhaust': true}"), /* opts */
                                                               NULL /* read_prefs */);

   // Set a bogus server ID.
   mongoc_cursor_set_hint (cursor, 123);

   // Iterate the cursor.
   const bson_t *result;
   while (mongoc_cursor_next (cursor, &result))
      ;
   // Expect an error due to the bogus server ID.
   ASSERT (mongoc_cursor_error (cursor, &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Could not find server with id: 123");

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

static void
test_cursor_set_max_await_time_ms (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *bson;

   client = test_framework_new_default_client ();
   collection = get_test_collection (client, "test_cursor_set_max_await_time_ms");

   cursor = mongoc_collection_find (
      collection, MONGOC_QUERY_TAILABLE_CURSOR | MONGOC_QUERY_AWAIT_DATA, 0, 0, 0, tmp_bson ("{}"), NULL, NULL);

   ASSERT_CMPINT (0, ==, mongoc_cursor_get_max_await_time_ms (cursor));
   mongoc_cursor_set_max_await_time_ms (cursor, 123);
   ASSERT_CMPINT (123, ==, mongoc_cursor_get_max_await_time_ms (cursor));
   mongoc_cursor_next (cursor, &bson);

   /* once started, cursor ignores set_max_await_time_ms () */
   mongoc_cursor_set_max_await_time_ms (cursor, 42);
   ASSERT_CMPINT (123, ==, mongoc_cursor_get_max_await_time_ms (cursor));

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}

typedef enum {
   FIRST_BATCH,
   SECOND_BATCH,
} exhaust_error_when_t;

typedef enum {
   NETWORK_ERROR,
   SERVER_ERROR,
} exhaust_error_type_t;

static void
_request_error (request_t *request, exhaust_error_type_t error_type)
{
   if (error_type == NETWORK_ERROR) {
      reply_to_request_with_reset (request);
   } else {
      reply_to_request (request, MONGOC_REPLY_QUERY_FAILURE, 123, 0, 0, "{'$err': 'uh oh', 'code': 4321}");
   }
}

static void
_check_error (mongoc_client_t *client, mongoc_cursor_t *cursor, exhaust_error_type_t error_type)
{
   uint32_t server_id;
   bson_error_t error;

   ASSERT (client);

   server_id = mongoc_cursor_get_hint (cursor);
   ASSERT (server_id);
   ASSERT (mongoc_cursor_error (cursor, &error));

   if (error_type == NETWORK_ERROR) {
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");

      /* socket was discarded */
      ASSERT (!mongoc_cluster_stream_for_server (
         &client->cluster, server_id, false /* don't reconnect */, NULL, NULL, &error));

      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error or timeout");
   } else {
      /* query failure */
      ASSERT_ERROR_CONTAINS (
         error, MONGOC_ERROR_QUERY, 4321 /* error from mock server */, "uh oh" /* message from mock server */);
   }
}

static void
_mock_test_exhaust (bool pooled, exhaust_error_when_t error_when, exhaust_error_type_t error_type)
{
   mock_server_t *server;
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   capture_logs (true);

   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   if (pooled) {
      pool = test_framework_client_pool_new_from_uri (mock_server_get_uri (server), NULL);
      client = mongoc_client_pool_pop (pool);
   } else {
      client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   }

   collection = mongoc_client_get_collection (client, "db", "test");
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_EXHAUST, 0, 0, 0, tmp_bson ("{}"), NULL, NULL);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_query (
      server, "db.test", MONGOC_QUERY_SECONDARY_OK | MONGOC_QUERY_EXHAUST, 0, 0, "{}", NULL);

   if (error_when == SECOND_BATCH) {
      /* initial query succeeds, gets a doc and cursor id of 123 */
      reply_to_request (request, MONGOC_REPLY_NONE, 123, 1, 1, "{'a': 1}");
      ASSERT (future_get_bool (future));
      assert_match_bson (doc, tmp_bson ("{'a': 1}"), false);
      ASSERT_CMPINT64 ((int64_t) 123, ==, mongoc_cursor_get_id (cursor));

      future_destroy (future);

      /* error after initial batch */
      future = future_cursor_next (cursor, &doc);
   }

   _request_error (request, error_type);
   ASSERT (!future_get_bool (future));
   _check_error (client, cursor, error_type);

   future_destroy (future);
   request_destroy (request);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   mock_server_destroy (server);
}

static void
test_exhaust_network_err_1st_batch_single (void)
{
   _mock_test_exhaust (false, FIRST_BATCH, NETWORK_ERROR);
}

static void
test_exhaust_network_err_1st_batch_pooled (void)
{
   _mock_test_exhaust (true, FIRST_BATCH, NETWORK_ERROR);
}

static void
test_exhaust_server_err_1st_batch_single (void)
{
   _mock_test_exhaust (false, FIRST_BATCH, SERVER_ERROR);
}

static void
test_exhaust_server_err_1st_batch_pooled (void)
{
   _mock_test_exhaust (true, FIRST_BATCH, SERVER_ERROR);
}

static void
test_exhaust_network_err_2nd_batch_single (void)
{
   _mock_test_exhaust (false, SECOND_BATCH, NETWORK_ERROR);
}

static void
test_exhaust_network_err_2nd_batch_pooled (void)
{
   _mock_test_exhaust (true, SECOND_BATCH, NETWORK_ERROR);
}

static void
test_exhaust_server_err_2nd_batch_single (void)
{
   _mock_test_exhaust (false, SECOND_BATCH, SERVER_ERROR);
}

static void
test_exhaust_server_err_2nd_batch_pooled (void)
{
   _mock_test_exhaust (true, SECOND_BATCH, SERVER_ERROR);
}

#ifndef _WIN32
#include <sys/wait.h>
/* Test that calling mongoc_client_reset on a client that has an exhaust cursor
 * closes the socket open to that server, and marks the client as no longer in
 * exhaust. */
static void
test_exhaust_in_child (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bool ret;
   mongoc_cursor_t *cursor;
   bson_t *to_insert;
   int i;
   bson_error_t error;
   const bson_t *doc;
   mongoc_bulk_operation_t *bulk;
   pid_t pid;
   uint32_t server_id;
   int child_exit_status;

   client = test_framework_new_default_client ();
   coll = get_test_collection (client, "exhaust_in_child");

   /* insert some documents, more than one reply's worth. */
   to_insert = BCON_NEW ("x", BCON_INT32 (1));
   bulk = mongoc_collection_create_bulk_operation (coll, false, NULL /* wc */);
   for (i = 0; i < 1001; i++) {
      ret = mongoc_bulk_operation_insert_with_opts (bulk, to_insert, NULL /* opts */, &error);
      ASSERT_OR_PRINT (ret, error);
   }
   ret = mongoc_bulk_operation_execute (bulk, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_bulk_operation_destroy (bulk);

   /* create an exhaust cursor. */
   cursor =
      mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), tmp_bson ("{'exhaust': true }"), NULL /* read prefs */);
   BSON_ASSERT (mongoc_cursor_next (cursor, &doc));
   BSON_ASSERT (client->in_exhaust);
   server_id = mongoc_cursor_get_hint (cursor);

   pid = fork ();
   if (pid == 0) {
      bson_t *ping;

      /* In child process, reset the client and destroy the cursor. */
      mongoc_client_reset (client);
      mongoc_cursor_destroy (cursor);
      /* The client should no longer be in exhaust */
      BSON_ASSERT (!client->in_exhaust);
      /* A command directly on that server should still work (it should open a
       * new socket). */
      ping = BCON_NEW ("ping", BCON_INT32 (1));
      ret = mongoc_client_command_simple_with_server_id (
         client, "admin", ping, NULL /* read prefs */, server_id, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ret, error);
      mongoc_collection_destroy (coll);
      mongoc_client_destroy (client);
      /* Clean up and exit, so child does not continue running test-libmongoc.
       */
      mongoc_cleanup ();
      exit (0);
   }

   BSON_ASSERT (-1 != waitpid (pid, &child_exit_status, 0 /* opts */));
   BSON_ASSERT (0 == child_exit_status);
   while (mongoc_cursor_next (cursor, &doc))
      ;
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   bson_destroy (to_insert);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_drop (coll, &error);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}
#endif /* _WIN32 */

void
test_exhaust_install (TestSuite *suite)
{
   TestSuite_AddFull (suite, "/Client/exhaust_cursor/works", test_exhaust_cursor_works, NULL, NULL, skip_if_no_exhaust);
   TestSuite_AddFull (
      suite, "/Client/exhaust_cursor/no_match", test_exhaust_cursor_no_match, NULL, NULL, skip_if_no_exhaust);
   TestSuite_AddFull (
      suite, "/Client/exhaust_cursor/single", test_exhaust_cursor_single, NULL, NULL, skip_if_no_exhaust);
   TestSuite_AddFull (suite, "/Client/exhaust_cursor/pool", test_exhaust_cursor_pool, NULL, NULL, skip_if_no_exhaust);
   TestSuite_AddFull (
      suite, "/Client/exhaust_cursor/batches", test_exhaust_cursor_multi_batch, NULL, NULL, skip_if_no_exhaust);
   TestSuite_AddLive (suite, "/Client/set_max_await_time_ms", test_cursor_set_max_await_time_ms);
   TestSuite_AddFull (suite,
                      "/Client/exhaust_cursor/server_hint",
                      test_cursor_server_hint_with_exhaust,
                      NULL,
                      NULL,
                      skip_if_no_exhaust);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/network/1st_batch/single", test_exhaust_network_err_1st_batch_single);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/network/1st_batch/pooled", test_exhaust_network_err_1st_batch_pooled);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/server/1st_batch/single", test_exhaust_server_err_1st_batch_single);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/server/1st_batch/pooled", test_exhaust_server_err_1st_batch_pooled);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/network/2nd_batch/single", test_exhaust_network_err_2nd_batch_single);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/network/2nd_batch/pooled", test_exhaust_network_err_2nd_batch_pooled);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/server/2nd_batch/single", test_exhaust_server_err_2nd_batch_single);
   TestSuite_AddMockServerTest (
      suite, "/Client/exhaust_cursor/err/server/2nd_batch/pooled", test_exhaust_server_err_2nd_batch_pooled);
#ifndef _WIN32
   /* Skip on Windows, since "fork" is not available and this test is not
    * particularly platform dependent. */
   if (!TestSuite_NoFork (suite)) {
      TestSuite_AddLive (suite, "/Client/exhaust_cursor/after_reset", test_exhaust_in_child);
   }
#endif /* _WIN32 */
}
