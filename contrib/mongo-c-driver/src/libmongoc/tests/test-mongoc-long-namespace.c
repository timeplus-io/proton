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


#include "mongoc/mongoc.h"
#include "mongoc/mongoc-change-stream-private.h"
#include "mongoc/mongoc-collection-private.h"
#include "mongoc/mongoc-cursor-private.h"
#include "mongoc/mongoc-database-private.h"
#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

typedef struct {
   char *ns;
   char *ns_coll;
   char *ns_db;
   mongoc_client_t *client;
   mongoc_client_session_t *client_session;
   bson_t opts_w_session;
   mongoc_database_t *db;
   mongoc_collection_t *coll;
} test_fixture_t;

/* Ensure that the command started callback reports the correct database name.
 */
static void
command_started (const mongoc_apm_command_started_t *event)
{
   test_fixture_t *test_fixture;

   test_fixture = mongoc_apm_command_started_get_context (event);
   if (0 == strcmp (mongoc_apm_command_started_get_command_name (event), "renameCollection")) {
      ASSERT_CMPSTR (mongoc_apm_command_started_get_database_name (event), "admin");
      /* Always runs on admin. */
   } else {
      ASSERT_CMPSTR (mongoc_apm_command_started_get_database_name (event), test_fixture->ns_db);
   }
}

/* Test long namespaces. Prior to SERVER-32959, the total namespace limit was
 * 120 characters. */
static void
test_fixture_init (test_fixture_t *test_fixture, uint32_t db_len, uint32_t coll_len)
{
   bool ret;
   bson_error_t error;
   mongoc_apm_callbacks_t *callbacks;

   test_fixture->ns_db = bson_malloc (db_len + 1);
   memset (test_fixture->ns_db, 'd', db_len);
   test_fixture->ns_db[db_len] = '\0';

   test_fixture->ns_coll = bson_malloc (coll_len + 1);
   memset (test_fixture->ns_coll, 'c', coll_len);
   test_fixture->ns_coll[coll_len] = '\0';

   test_fixture->ns = bson_strdup_printf ("%s.%s", test_fixture->ns_db, test_fixture->ns_coll);

   /* Construct client, database, and collection objects. */
   test_fixture->client = test_framework_new_default_client ();
   test_framework_set_ssl_opts (test_fixture->client);
   mongoc_client_set_error_api (test_fixture->client, MONGOC_ERROR_API_VERSION_2);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, command_started);
   mongoc_client_set_apm_callbacks (test_fixture->client, callbacks, test_fixture);

   test_fixture->db = mongoc_client_get_database (test_fixture->client, test_fixture->ns_db);
   ASSERT_CMPSTR (test_fixture->db->name, test_fixture->ns_db);

   test_fixture->coll = mongoc_database_get_collection (test_fixture->db, test_fixture->ns_coll);
   ASSERT_CMPSTR (test_fixture->coll->collection, test_fixture->ns_coll);
   ASSERT_CMPSIZE_T (test_fixture->coll->collectionlen, ==, strlen (test_fixture->ns_coll));
   ASSERT_CMPSTR (test_fixture->coll->db, test_fixture->ns_db);
   ASSERT_CMPSTR (test_fixture->coll->ns, test_fixture->ns);
   ASSERT_CMPSIZE_T (test_fixture->coll->nslen, ==, strlen (test_fixture->ns));

   /* Drop 'coll'. */
   ret = mongoc_collection_drop (test_fixture->coll, &error);
   /* ignore a 'ns not found' error */
   if (!ret && NULL == strstr (error.message, "ns not found")) {
      /* unexpected error. */
      test_error ("unexpected error: %s\n", error.message);
   }

   /* Explicitly create 'coll', so it shows up in listCollections. */
   mongoc_collection_destroy (test_fixture->coll);
   test_fixture->coll =
      mongoc_database_create_collection (test_fixture->db, test_fixture->ns_coll, NULL /* opts */, &error);
   ASSERT_OR_PRINT (test_fixture->coll, error);
   ASSERT_CMPSTR (test_fixture->coll->collection, test_fixture->ns_coll);
   ASSERT_CMPSIZE_T (test_fixture->coll->collectionlen, ==, strlen (test_fixture->ns_coll));
   ASSERT_CMPSTR (test_fixture->coll->db, test_fixture->ns_db);
   ASSERT_CMPSTR (test_fixture->coll->ns, test_fixture->ns);
   ASSERT_CMPSIZE_T (test_fixture->coll->nslen, ==, strlen (test_fixture->ns));

   mongoc_apm_callbacks_destroy (callbacks);
}

static void
test_fixture_cleanup (test_fixture_t *test_fixture)
{
   /* Clear the APM callbacks, since endSessions runs on the admin database. */
   mongoc_client_set_apm_callbacks (test_fixture->client, NULL, NULL);
   mongoc_collection_destroy (test_fixture->coll);
   mongoc_database_destroy (test_fixture->db);
   mongoc_client_destroy (test_fixture->client);
   bson_free (test_fixture->ns_coll);
   bson_free (test_fixture->ns_db);
   bson_free (test_fixture->ns);
}

/* Test crud operations. This should test legacy OP_QUERY cursors and legacy
 * write ops, which were storing fixed size namespaces using
 * MONGOC_NAMESPACE_MAX. */
static void
crud (test_fixture_t *test_fixture)
{
   bson_error_t error;
   bool ret;
   mongoc_cursor_t *cursor;
   const bson_t *found;

   /* Insert. */
   ret = mongoc_collection_insert_one (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Find that document back. */
   cursor = mongoc_collection_find_with_opts (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'_id': 'hello'}");
   ASSERT_CMPSTR (cursor->ns, test_fixture->ns);
   ASSERT_CURSOR_DONE (cursor);
   mongoc_cursor_destroy (cursor);

   /* Update it. */
   ret = mongoc_collection_update_one (test_fixture->coll,
                                       tmp_bson ("{'_id': 'hello'}"),
                                       tmp_bson ("{'$set': {'x':1}}"),
                                       NULL /* opts */,
                                       NULL /* reply */,
                                       &error);
   ASSERT_OR_PRINT (ret, error);

   /* Find that document back to ensure the document in the right collection was
    * updated. */
   cursor = mongoc_collection_find_with_opts (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'_id': 'hello', 'x': 1}");
   ASSERT_CMPSTR (cursor->ns, test_fixture->ns);
   ASSERT_CURSOR_DONE (cursor);
   mongoc_cursor_destroy (cursor);

   /* Delete it. */
   ret = mongoc_collection_delete_one (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Attempt to find that document back to ensure the document in the right
    * collection was deleted. */
   cursor = mongoc_collection_find_with_opts (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* read prefs */);
   ASSERT_CURSOR_DONE (cursor);
   ASSERT_CMPSTR (cursor->ns, test_fixture->ns);
   mongoc_cursor_destroy (cursor);
}

/* Test cursor getmore, which constructed a namespace with MONGOC_NAMESPACE_MAX.
 */
static void
getmore (test_fixture_t *test_fixture)
{
   bson_error_t error;
   bool ret;
   mongoc_cursor_t *cursor;
   const bson_t *found;

   /* Insert two documents. */
   ret = mongoc_collection_insert_one (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   ret = mongoc_collection_insert_one (
      test_fixture->coll, tmp_bson ("{'_id': 'world'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Find each document back in two separate batches. */
   cursor = mongoc_collection_find_with_opts (
      test_fixture->coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"), NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'_id': 'hello'}");
   ASSERT_CMPSTR (cursor->ns, test_fixture->ns);
   /* not DONE, next call will send a getMore */
   BSON_ASSERT (cursor->state != DONE);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'_id': 'world'}");
   ASSERT_CMPSTR (cursor->ns, test_fixture->ns);
   ASSERT_CURSOR_DONE (cursor);
   mongoc_cursor_destroy (cursor);
}

/* Test change streams, which store a namespace in mongoc_change_stream_t */
static void
change_stream (test_fixture_t *test_fixture)
{
   mongoc_change_stream_t *change_stream;
   const bson_t *found;
   bool ret;
   bson_error_t error;
   mongoc_client_session_t *client_session;
   bson_t opts_w_session;

   client_session = mongoc_client_start_session (test_fixture->client, NULL /* opts */, &error);
   ASSERT_OR_PRINT (client_session, error);
   bson_init (&opts_w_session);
   ret = mongoc_client_session_append (client_session, &opts_w_session, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Create a change stream. Do all operations within a session, to guarantee
    * change stream sees the subsequent insert operation. */
   change_stream = mongoc_collection_watch (test_fixture->coll, tmp_bson ("{}"), &opts_w_session);
   ASSERT_CMPSTR (change_stream->db, test_fixture->ns_db);
   ASSERT_CMPSTR (change_stream->coll, test_fixture->ns_coll);

   /* Insert. */
   ret = mongoc_collection_insert_one (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Get a change stream event. */
   ret = mongoc_change_stream_next (change_stream, &found);
   ASSERT_OR_PRINT (!mongoc_change_stream_error_document (change_stream, &error, NULL) && ret, error);
   ASSERT_MATCH (found,
                 "{'operationType': 'insert', 'ns': { 'db': '%s', 'coll': '%s' }}",
                 test_fixture->ns_db,
                 test_fixture->ns_coll);

   mongoc_change_stream_destroy (change_stream);
   mongoc_client_session_destroy (client_session);
   bson_destroy (&opts_w_session);
}


/* Test mongoc_client_command, which constructed a namespace with
 * MONGOC_NAMESPACE_MAX */
static void
client_command (test_fixture_t *test_fixture)
{
   const bson_t *found;
   mongoc_cursor_t *cursor;
   bool ret;
   bson_error_t error;

   cursor = mongoc_client_command (test_fixture->client,
                                   test_fixture->ns_db,
                                   MONGOC_QUERY_NONE,
                                   0 /* skip */,
                                   0 /* limit */,
                                   0 /* batch size */,
                                   tmp_bson ("{'listCollections': 1, 'filter': {'name': '%s'}}", test_fixture->ns_coll),
                                   NULL /* fields */,
                                   NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'cursor': {'firstBatch': [{'name': '%s'}]}}", test_fixture->ns_coll);
   mongoc_cursor_destroy (cursor);
}

/* Test mongoc_database_command, which constructed a namespace with
 * MONGOC_NAMESPACE_MAX. */
static void
database_command (test_fixture_t *test_fixture)
{
   const bson_t *found;
   mongoc_cursor_t *cursor;
   bool ret;
   bson_error_t error;

   cursor =
      mongoc_database_command (test_fixture->db,
                               MONGOC_QUERY_NONE,
                               0 /* skip */,
                               0 /* limit */,
                               0 /* batch size */,
                               tmp_bson ("{'listCollections': 1, 'filter': {'name': '%s'}}", test_fixture->ns_coll),
                               NULL /* fields */,
                               NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'cursor': {'firstBatch': [{'name': '%s'}]}}", test_fixture->ns_coll);
   mongoc_cursor_destroy (cursor);
}

/* Test mongoc_collection_command, which constructed a namespace with
 * MONGOC_NAMESPACE_MAX. */
static void
collection_command (test_fixture_t *test_fixture)
{
   const bson_t *found;
   mongoc_cursor_t *cursor;
   bool ret;
   bson_error_t error;

   cursor =
      mongoc_collection_command (test_fixture->coll,
                                 MONGOC_QUERY_NONE,
                                 0 /* skip */,
                                 0 /* limit */,
                                 0 /* batch size */,
                                 tmp_bson ("{'listCollections': 1, 'filter': {'name': '%s'}}", test_fixture->ns_coll),
                                 NULL /* fields */,
                                 NULL /* read prefs */);
   ret = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error) && ret, error);
   ASSERT_MATCH (found, "{'cursor': {'firstBatch': [{'name': '%s'}]}}", test_fixture->ns_coll);
   mongoc_cursor_destroy (cursor);
}

/* Check whether a collection exists. */
static void
_check_existence (mongoc_client_t *client, char *ns_db, char *ns_coll, bool should_exist)
{
   mongoc_database_t *db;
   char **db_names;
   char **coll_names;
   bool db_exists = false;
   bool coll_exists = false;
   char **iter;
   bson_error_t error;

   ASSERT (client);

   db = mongoc_client_get_database (client, ns_db);
   db_names = mongoc_client_get_database_names_with_opts (client, NULL /* opts */, &error);
   coll_names = mongoc_database_get_collection_names_with_opts (db, NULL /* opts */, &error);

   for (iter = db_names; *iter != NULL; ++iter) {
      if (0 == strcmp (ns_db, *iter)) {
         db_exists = true;
      }
   }

   if (!db_exists && should_exist) {
      test_error ("Database %s does not exist but should", ns_db);
   }

   for (iter = coll_names; *iter != NULL; ++iter) {
      if (0 == strcmp (ns_coll, *iter)) {
         coll_exists = true;
      }
   }

   if (coll_exists && !should_exist) {
      test_error ("Collection %s exists but shouldn't", ns_coll);
   }
   if (!coll_exists && should_exist) {
      test_error ("Collection %s does not exist but should", ns_coll);
   }

   bson_strfreev (db_names);
   bson_strfreev (coll_names);
   mongoc_database_destroy (db);
}

/* Test mongoc_collection_rename, which constructed a namespace
 * with MONGOC_NAMESPACE_MAX */
static void
collection_rename (test_fixture_t *test_fixture)
{
   bool ret;
   bson_error_t error;
   char *new_db;
   char *new_coll;
   char *new_ns;
   mongoc_client_t *client;

   new_db = bson_strdup_printf ("renamed_db");
   new_coll = bson_strdup_printf ("renamed_%s", test_fixture->ns_coll);
   new_ns = bson_strdup_printf ("%s.%s", new_db, new_coll);

   /* Insert to create source namespace. */
   ret = mongoc_collection_insert_one (
      test_fixture->coll, tmp_bson ("{'_id': 'hello'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   ret = mongoc_collection_rename (test_fixture->coll, new_db, new_coll, true, &error);
   ASSERT_OR_PRINT (ret, error);

   /* The fields in the collection struct are updated to the new names. */
   ASSERT_CMPSTR (test_fixture->coll->db, new_db);
   ASSERT_CMPSTR (test_fixture->coll->collection, new_coll);
   ASSERT_CMPSTR (test_fixture->coll->ns, new_ns);
   ASSERT_CMPSIZE_T (test_fixture->coll->nslen, ==, strlen (new_ns));
   ASSERT_CMPSIZE_T (test_fixture->coll->collectionlen, ==, strlen (new_coll));

   /* Check that source collections do not exist anymore.  Use a separate client
    * so commands
    * don't show up in APM on test fixture's client. */
   client = test_framework_new_default_client ();
   _check_existence (client, test_fixture->ns_db, test_fixture->ns_coll, false);

   /* Check that the new collection exists. */
   _check_existence (client, new_db, new_coll, true);
   mongoc_client_destroy (client);
   bson_free (new_db);
   bson_free (new_coll);
   bson_free (new_ns);
}

typedef void (*test_fn) (test_fixture_t *fixture);

static void
run_test (void *ctx)
{
   test_fn one_test;
   test_fixture_t test_fixture;

   one_test = (test_fn) ((TestFnCtx *) ctx)->test_fn;
   /* Small names. */
   test_fixture_init (&test_fixture, 32, 32);
   one_test (&test_fixture);
   test_fixture_cleanup (&test_fixture);
   /* Large collection name. */
   test_fixture_init (&test_fixture, 32, 100);
   one_test (&test_fixture);
   test_fixture_cleanup (&test_fixture);
   /* Maximum valid database name is still 64 characters. */
   test_fixture_init (&test_fixture, 63, 32);
   one_test (&test_fixture);
   test_fixture_cleanup (&test_fixture);
   /* Large for both names. */
   test_fixture_init (&test_fixture, 63, 100);
   one_test (&test_fixture);
   test_fixture_cleanup (&test_fixture);
}

static void
unsupported_long_coll (void *unused)
{
   bson_error_t error;
   bool ret;
   char *long_coll;
   mongoc_client_t *client;
   mongoc_collection_t *coll;

   BSON_UNUSED (unused);

   long_coll = bson_malloc (200);
   memset (long_coll, 'd', 199);
   long_coll[199] = '\0';

   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "test", long_coll);
   /* Insert. */
   ret = mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL /* opts */, NULL /* reply */, &error);
   BSON_ASSERT (!ret);
   /* Error code changed in 4.0 and the message in 4.2. Just validate an error
    * happened. */
   BSON_ASSERT (error.code);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);

   bson_free (long_coll);
}

/* 63 characters is still the database length limit. Test this on all server
 * versions. */
static void
unsupported_long_db (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_error_t error;
   bool ret;
   char *long_db;

   long_db = bson_malloc (65);
   memset (long_db, 'd', 64);
   long_db[64] = '\0';

   client = test_framework_new_default_client ();
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, long_db, "test");
   /* Insert. */
   ret = mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL /* opts */, NULL /* reply */, &error);

   BSON_ASSERT (!ret);
   /* Error code changed in 3.4. Just validate an error happened. */
   BSON_ASSERT (error.code);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   bson_free (long_db);
}

void
test_long_namespace_install (TestSuite *suite)
{
   /* MongoDB 4.4 (wire version 9) introduced support for long namespaces in
    * SERVER-32959 */
   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/client_command",
                                run_test,
                                NULL /* dtor */,
                                client_command,
                                test_framework_skip_if_max_wire_version_less_than_9);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/database_command",
                                run_test,
                                NULL /* dtor */,
                                database_command,
                                test_framework_skip_if_max_wire_version_less_than_9);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/collection_command",
                                run_test,
                                NULL /* dtor */,
                                collection_command,
                                test_framework_skip_if_max_wire_version_less_than_9);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/crud",
                                run_test,
                                NULL /* dtor */,
                                crud,
                                test_framework_skip_if_max_wire_version_less_than_9);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/getmore",
                                run_test,
                                NULL /* dtor */,
                                getmore,
                                test_framework_skip_if_max_wire_version_less_than_9);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/change_stream",
                                run_test,
                                NULL /* dtor */,
                                change_stream,
                                test_framework_skip_if_not_rs_version_9,
                                test_framework_skip_if_no_sessions);

   TestSuite_AddFullWithTestFn (suite,
                                "/long_namespace/collection_rename",
                                run_test,
                                NULL /* dtor */,
                                collection_rename,
                                test_framework_skip_if_max_wire_version_less_than_9,
                                test_framework_skip_if_mongos);

   TestSuite_AddFull (suite,
                      "/long_namespace/unsupported_long_coll",
                      unsupported_long_coll,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_max_wire_version_more_than_8);

   TestSuite_AddLive (suite, "/long_namespace/unsupported_long_db", unsupported_long_db);
}
