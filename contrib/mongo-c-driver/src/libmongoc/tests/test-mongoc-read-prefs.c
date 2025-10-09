#include <mongoc/mongoc.h>
#include <mongoc/mongoc-uri-private.h>

#include "TestSuite.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "mock_server/mock-rs.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

static bool
_can_be_command (const char *query)
{
   return (!bson_empty (tmp_bson (query)));
}


static void
_test_op_msg (const mongoc_uri_t *uri,
              mock_server_t *server,
              const char *query_or_cmd,
              mongoc_read_prefs_t *read_prefs,
              const char *expected_find)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t b = BSON_INITIALIZER;
   future_t *future;
   request_t *request;

   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "test", "test");

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 1, 0, tmp_bson (query_or_cmd), NULL, read_prefs);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server, 0, tmp_bson (expected_find));
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': 0,"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{'a': 1}]}}");

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_destroy (&b);
}


static void
_test_command (const mongoc_uri_t *uri,
               mock_server_t *server,
               const char *command,
               mongoc_read_prefs_t *read_prefs,
               const char *expected_cmd)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t b = BSON_INITIALIZER;
   future_t *future;
   request_t *request;

   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "test", "test");
   mongoc_collection_set_read_prefs (collection, read_prefs);

   cursor = mongoc_collection_command (collection, MONGOC_QUERY_NONE, 0, 1, 0, tmp_bson (command), NULL, read_prefs);

   future = future_cursor_next (cursor, &doc);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_cmd));

   reply_to_request (request,
                     MONGOC_REPLY_NONE, /* flags */
                     0,                 /* cursorId */
                     0,                 /* startingFrom */
                     1,                 /* numberReturned */
                     "{'ok': 1}");

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_destroy (&b);
}

static void
_test_command_simple (const mongoc_uri_t *uri,
                      mock_server_t *server,
                      const char *command,
                      mongoc_read_prefs_t *read_prefs,
                      const char *expected_cmd)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   future_t *future;
   request_t *request;

   client = test_framework_client_new_from_uri (uri, NULL);
   collection = mongoc_client_get_collection (client, "test", "test");
   mongoc_collection_set_read_prefs (collection, read_prefs);

   future = future_client_command_simple (client, "test", tmp_bson (command), read_prefs, NULL, NULL);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson (expected_cmd));

   reply_to_request (request,
                     MONGOC_REPLY_NONE, /* flags */
                     0,                 /* cursorId */
                     0,                 /* startingFrom */
                     1,                 /* numberReturned */
                     "{'ok': 1}");

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
}


typedef enum {
   READ_PREF_TEST_STANDALONE,
   READ_PREF_TEST_MONGOS,
   READ_PREF_TEST_PRIMARY,
   READ_PREF_TEST_SECONDARY,
} read_pref_test_type_t;


static mock_server_t *
_run_server (read_pref_test_type_t test_type)
{
   mock_server_t *server;

   server = mock_server_new ();
   mock_server_run (server);

   switch (test_type) {
   case READ_PREF_TEST_STANDALONE:
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'isWritablePrimary': true}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_MAX);
      break;
   case READ_PREF_TEST_MONGOS:
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'isWritablePrimary': true,"
                              " 'msg': 'isdbgrid'}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_MAX);
      break;
   case READ_PREF_TEST_PRIMARY:
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'isWritablePrimary': true,"
                              " 'setName': 'rs',"
                              " 'hosts': ['%s']}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_MAX,
                              mock_server_get_host_and_port (server));
      break;
   case READ_PREF_TEST_SECONDARY:
      mock_server_auto_hello (server,
                              "{'ok': 1,"
                              " 'minWireVersion': %d,"
                              " 'maxWireVersion': %d,"
                              " 'isWritablePrimary': false,"
                              " 'secondary': true,"
                              " 'setName': 'rs',"
                              " 'hosts': ['%s']}",
                              WIRE_VERSION_MIN,
                              WIRE_VERSION_MAX,
                              mock_server_get_host_and_port (server));
      break;
   default:
      test_error ("Invalid test_type: %d", (int) test_type);
   }

   return server;
}


static mongoc_uri_t *
_get_uri (mock_server_t *server, read_pref_test_type_t test_type)
{
   mongoc_uri_t *uri;

   uri = mongoc_uri_copy (mock_server_get_uri (server));

   switch (test_type) {
   case READ_PREF_TEST_PRIMARY:
   case READ_PREF_TEST_SECONDARY:
      mongoc_uri_set_option_as_utf8 (uri, "replicaSet", "rs");
      break;
   case READ_PREF_TEST_STANDALONE:
   case READ_PREF_TEST_MONGOS:
   default:
      break;
   }

   return uri;
}


static void
_test_read_prefs_op_msg (read_pref_test_type_t test_type,
                         mongoc_read_prefs_t *read_prefs,
                         const char *query_or_cmd,
                         const char *expected_cmd,
                         const char *expected_find)
{
   mock_server_t *server;
   mongoc_uri_t *uri;

   server = _run_server (test_type);
   uri = _get_uri (server, test_type);

   if (_can_be_command (query_or_cmd)) {
      _test_command (uri, server, query_or_cmd, read_prefs, expected_cmd);

      _test_command_simple (uri, server, query_or_cmd, read_prefs, expected_cmd);
   }

   _test_op_msg (uri, server, query_or_cmd, read_prefs, expected_find);

   mock_server_destroy (server);
   mongoc_uri_destroy (uri);
}


static void
_test_read_prefs (read_pref_test_type_t test_type,
                  mongoc_read_prefs_t *read_prefs,
                  const char *query_or_cmd,
                  const char *expected_cmd,
                  const char *expected_find)
{
   _test_read_prefs_op_msg (test_type, read_prefs, query_or_cmd, expected_cmd, expected_find);
}


/* test that a NULL read pref is the same as PRIMARY */
static void
test_read_prefs_standalone_null (void)
{
   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            NULL,
                            "{}",
                            "{}",
                            "{ 'find': 'test', 'filter': {}, "
                            "'$readPreference': { '$exists': false } }");

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            NULL,
                            "{'a': 1}",
                            "{'a': 1}",
                            "{ 'find': 'test', 'filter': {'a': 1}, "
                            "'$readPreference': { '$exists': false } }");
}

static void
test_read_prefs_standalone_primary (void)
{
   mongoc_read_prefs_t *read_prefs;

   /* Server Selection Spec: for topology type single and server types other
    * than mongos, "clients MUST always set the secondaryOk wire protocol flag
    * on reads to ensure that any server type can handle the request."
    * */
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{}",
                            "{}",
                            "{ 'find': 'test', 'filter': {}, "
                            "'$readPreference': { '$exists': false } }");

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{'a': 1}",
                            "{'a': 1}",
                            "{ 'find': 'test', 'filter': {'a': 1}, "
                            "'$readPreference': { '$exists': false } }");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_standalone_secondary (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{}",
                            "{}",
                            "{ 'find': 'test', 'filter': {}, "
                            "'$readPreference': { '$exists': false } }");

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{'a': 1}",
                            "{'a': 1}",
                            "{ 'find': 'test', 'filter': {'a': 1}, "
                            "'$readPreference': { '$exists': false } }");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_standalone_tags (void)
{
   bson_t b = BSON_INITIALIZER;
   mongoc_read_prefs_t *read_prefs;

   bson_append_utf8 (&b, "dc", 2, "ny", 2);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);
   mongoc_read_prefs_add_tag (read_prefs, &b);
   mongoc_read_prefs_add_tag (read_prefs, NULL);

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{}",
                            "{}",
                            "{ 'find': 'test', 'filter': {}, "
                            "'$readPreference': { '$exists': false } }");

   _test_read_prefs_op_msg (READ_PREF_TEST_STANDALONE,
                            read_prefs,
                            "{'a': 1}",
                            "{'a': 1}",
                            "{ 'find': 'test', 'filter': {'a': 1}, "
                            "'$readPreference': { '$exists': false } }");

   bson_destroy (&b);
   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_primary_rsprimary (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   _test_read_prefs (READ_PREF_TEST_PRIMARY, read_prefs, "{}", "{}", "{'find': 'test', 'filter':  {}}");

   _test_read_prefs (
      READ_PREF_TEST_PRIMARY, read_prefs, "{'a': 1}", "{'a': 1}", "{'find': 'test', 'filter':  {'a': 1}}");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_secondary_rssecondary (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   _test_read_prefs (READ_PREF_TEST_SECONDARY, read_prefs, "{}", "{}", "{'find': 'test', 'filter':  {}}");

   _test_read_prefs (
      READ_PREF_TEST_SECONDARY, read_prefs, "{'a': 1}", "{'a': 1}", "{'find': 'test', 'filter':  {'a': 1}}");

   mongoc_read_prefs_destroy (read_prefs);
}


/* test that a NULL read pref is the same as PRIMARY */
static void
test_read_prefs_mongos_null (void)
{
   _test_read_prefs (READ_PREF_TEST_MONGOS, NULL, "{}", "{}", "{'find': 'test', 'filter':  {}}");

   _test_read_prefs (READ_PREF_TEST_MONGOS, NULL, "{'a': 1}", "{'a': 1}", "{'find': 'test', 'filter':  {}}");
}


static void
test_read_prefs_mongos_primary (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   _test_read_prefs (READ_PREF_TEST_MONGOS, read_prefs, "{}", "{}", "{'find': 'test', 'filter':  {}}");

   _test_read_prefs (
      READ_PREF_TEST_MONGOS, read_prefs, "{'a': 1}", "{'a': 1}", "{'find': 'test', 'filter':  {'a': 1}}");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_mongos_secondary (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);

   _test_read_prefs_op_msg (READ_PREF_TEST_MONGOS,
                            read_prefs,
                            "{}",
                            "{'find': 'test','$readPreference': {'mode': 'secondary'}}",
                            "{'find': 'test', 'filter':  {},"
                            " '$readPreference': {'mode': 'secondary'}}");

   _test_read_prefs_op_msg (READ_PREF_TEST_MONGOS,
                            read_prefs,
                            "{'a': 1}",
                            "{'a': 1, '$readPreference': {'mode': 'secondary'}}",
                            "{'find': 'test', 'filter':  {'a': 1},"
                            " '$readPreference': {'mode': 'secondary'}}");

   _test_read_prefs_op_msg (READ_PREF_TEST_MONGOS,
                            read_prefs,
                            "{'$query': {'a': 1}}",
                            "{'$query': {'a': 1}, '$readPreference': {'mode': 'secondary'}}",
                            "{'find': 'test', 'filter':  {'a': 1},"
                            " '$readPreference': {'mode': 'secondary'}}");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_mongos_secondary_preferred (void)
{
   mongoc_read_prefs_t *read_prefs;

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);

   /* $readPreference not sent, only secondaryOk */
   _test_read_prefs (READ_PREF_TEST_MONGOS, read_prefs, "{}", "{}", "{'find': 'test', 'filter':  {}}");

   _test_read_prefs (
      READ_PREF_TEST_MONGOS, read_prefs, "{'a': 1}", "{'a': 1}", "{'find': 'test', 'filter':  {'a': 1}}");

   mongoc_read_prefs_destroy (read_prefs);
}


static void
test_read_prefs_mongos_tags (void)
{
   bson_t b = BSON_INITIALIZER;
   mongoc_read_prefs_t *read_prefs;

   bson_append_utf8 (&b, "dc", 2, "ny", 2);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);
   mongoc_read_prefs_add_tag (read_prefs, &b);
   mongoc_read_prefs_add_tag (read_prefs, NULL);

   _test_read_prefs_op_msg (READ_PREF_TEST_MONGOS,
                            read_prefs,
                            "{}",
                            "{'$query': {}, '$readPreference': {'mode': 'secondaryPreferred',"
                            "                                   'tags': [{'dc': 'ny'}, {}]}}",
                            "{'find': 'test', 'filter':  {},"
                            " '$readPreference': {'mode': 'secondaryPreferred',"
                            "                             'tags': [{'dc': 'ny'}, {}]}}");

   _test_read_prefs_op_msg (READ_PREF_TEST_MONGOS,
                            read_prefs,
                            "{'a': 1}",
                            "{'a': 1,"
                            " '$readPreference': {'mode': 'secondaryPreferred',"
                            "                     'tags': [{'dc': 'ny'}, {}]}}",
                            "{'find': 'test', 'filter':  {},"
                            " '$readPreference': {'mode': 'secondaryPreferred',"
                            "                             'tags': [{'dc': 'ny'}, {}]}}");

   mongoc_read_prefs_destroy (read_prefs);
   bson_destroy (&b);
}


/* CDRIVER-3633 - test read prefs are sent when maxStalenessSeconds is set */
static void
test_read_prefs_mongos_max_staleness (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_read_prefs_t *prefs;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "test", "test");

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);
   mongoc_read_prefs_set_max_staleness_seconds (prefs, 120);

   /* exhaust cursor is required per CDRIVER-3633 so the driver downgrades the
    * OP_QUERY find command to an OP_QUERY legacy find */
   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{'a': 1}"), tmp_bson ("{'exhaust': true}"), prefs);
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_query (server,
                                         "test.test",
                                         MONGOC_QUERY_EXHAUST | MONGOC_QUERY_SECONDARY_OK,
                                         0,
                                         0,
                                         "{'$query': {'a': 1},"
                                         " '$readPreference': {'mode': 'secondaryPreferred',"
                                         "                     'maxStalenessSeconds': 120}}",
                                         "{}");

   reply_to_find_request (request, MONGOC_QUERY_EXHAUST | MONGOC_QUERY_SECONDARY_OK, 0, 1, "test.test", "{}", false);

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* CDRIVER-3583 - support for server hedged reads */
static void
test_read_prefs_mongos_hedged_reads (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t hedge_doc = BSON_INITIALIZER;
   mongoc_read_prefs_t *prefs;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_HEDGED_READS);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "test", "test");

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY_PREFERRED);
   bson_append_bool (&hedge_doc, "enabled", 7, true);

   mongoc_read_prefs_set_hedge (prefs, &hedge_doc);

   cursor = mongoc_collection_find_with_opts (collection, tmp_bson ("{'a': 1}"), tmp_bson ("{'exhaust': true}"), prefs);
   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_EXHAUST_ALLOWED,
                                       tmp_bson ("{'find': 'test',"
                                                 " 'filter': {'a': {'$numberInt': '1'}},"
                                                 " '$readPreference': {'mode': 'secondaryPreferred',"
                                                 "                     'hedge': {'enabled': true}}}"));
   reply_to_request_simple (request,
                            "{'ok': 1,"
                            " 'cursor': {"
                            "    'id': {'$numberLong': '0'},"
                            "    'ns': 'db.collection',"
                            "    'firstBatch': [{}]}}");
   mock_server_auto_endsessions (server);

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_read_prefs_destroy (prefs);
   bson_destroy (&hedge_doc);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* test that we add readConcern only inside $query, not outside it too */
static void
test_mongos_read_concern (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_read_prefs_t *prefs;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   future_t *future;
   request_t *request;

   server = mock_mongos_new (WIRE_VERSION_MIN);
   mock_server_run (server);
   mock_server_auto_endsessions (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "test", "test");
   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   cursor = mongoc_collection_find_with_opts (
      collection, tmp_bson ("{'a': 1}"), tmp_bson ("{'readConcern': {'level': 'foo'}}"), prefs);

   future = future_cursor_next (cursor, &doc);
   request = mock_server_receives_msg (server,
                                       MONGOC_MSG_NONE,
                                       tmp_bson ("{'$db': 'test',"
                                                 " 'find': 'test',"
                                                 " 'filter': {},"
                                                 " 'readConcern': {'level': 'foo'},"
                                                 " '$readPreference': {'mode': 'secondary'},"
                                                 " 'readConcern': {'level': 'foo'}}"));

   reply_to_op_msg_request (request,
                            MONGOC_MSG_NONE,
                            tmp_bson ("{'ok': 1,"
                                      " 'cursor': {"
                                      "    'id': {'$numberLong': '0'},"
                                      "    'ns': 'db.collection',"
                                      "    'firstBatch': [{}]}}"));

   /* mongoc_cursor_next returned true */
   BSON_ASSERT (future_get_bool (future));

   request_destroy (request);
   future_destroy (future);
   mongoc_cursor_destroy (cursor);
   mongoc_read_prefs_destroy (prefs);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


typedef mongoc_cursor_t *(*test_op_msg_direct_fn_t) (mongoc_collection_t *, mongoc_read_prefs_t *);


/* direct connection to a secondary requires read pref primaryPreferred to
 * avoid "not primary" error from server */
static void
_test_op_msg_direct_connection (bool is_mongos, test_op_msg_direct_fn_t fn, const char *expected_cmd)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_read_prefs_t *prefs = NULL;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t *cmd;
   future_t *future;
   request_t *request;
   const char *reply;
   int i;

   if (is_mongos) {
      server = mock_mongos_new (WIRE_VERSION_MAX);
   } else {
      char *hello = bson_strdup_printf ("{'ok': 1.0,"
                                        " 'isWritablePrimary': true,"
                                        " 'setName': 'rs0',"
                                        " 'secondary': true,"
                                        " 'minWireVersion': %d,"
                                        " 'maxWireVersion': %d}",
                                        WIRE_VERSION_MIN,
                                        WIRE_VERSION_MAX);
      server = mock_server_new ();
      mock_server_auto_hello (server, hello);
      bson_free (hello);
   }

   mock_server_auto_endsessions (server);

   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   for (i = 0; i < 2; i++) {
      if (i == 1) {
         /* user-supplied read preference primary makes no difference */
         prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
      }

      cursor = fn (collection, prefs);
      future = future_cursor_next (cursor, &doc);
      cmd = tmp_bson (expected_cmd);
      request = mock_server_receives_msg (server, 0, cmd);
      reply = "{'ok': 1,"
              " 'cursor': {"
              "    'id': 0,"
              "    'ns': 'db.collection',"
              "    'firstBatch': [{'a': 1}]}}";

      reply_to_request_simple (request, reply);
      BSON_ASSERT (future_get_bool (future));
      future_destroy (future);
      request_destroy (request);
      mongoc_cursor_destroy (cursor);
      mongoc_read_prefs_destroy (prefs); /* null ok */
   }

   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}


static mongoc_cursor_t *
find (mongoc_collection_t *collection, mongoc_read_prefs_t *prefs)
{
   return mongoc_collection_find_with_opts (collection, tmp_bson ("{}"), NULL /* opts */, prefs);
}


static mongoc_cursor_t *
aggregate (mongoc_collection_t *collection, mongoc_read_prefs_t *prefs)
{
   return mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL /* opts */, prefs);
}


/* direct connection to a secondary requires read pref primaryPreferred to
 * avoid "not primary" error from server */
static void
test_op_msg_direct_secondary (void)
{
   _test_op_msg_direct_connection (false /* is_mongos */,
                                   find,
                                   "{"
                                   "   'find': 'collection',"
                                   "   '$readPreference': {'mode': 'primaryPreferred'}"
                                   "}");

   _test_op_msg_direct_connection (false /* is_mongos */,
                                   aggregate,
                                   "{"
                                   "   'aggregate': 'collection',"
                                   "   '$readPreference': {'mode': 'primaryPreferred'}"
                                   "}");
}


/* direct connection to mongos must not auto-add read pref primaryPreferred */
static void
test_op_msg_direct_mongos (void)
{
   _test_op_msg_direct_connection (true /* is_mongos */,
                                   find,
                                   "{"
                                   "   'find': 'collection',"
                                   "   '$readPreference': {'$exists': false}"
                                   "}");

   _test_op_msg_direct_connection (true /* is_mongos */,
                                   aggregate,
                                   "{"
                                   "   'aggregate': 'collection',"
                                   "   '$readPreference': {'$exists': false}"
                                   "}");
}


void
test_read_prefs_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/standalone/null", test_read_prefs_standalone_null);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/standalone/primary", test_read_prefs_standalone_primary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/standalone/secondary", test_read_prefs_standalone_secondary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/standalone/tags", test_read_prefs_standalone_tags);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/rsprimary/primary", test_read_prefs_primary_rsprimary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/rssecondary/secondary", test_read_prefs_secondary_rssecondary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/null", test_read_prefs_mongos_null);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/primary", test_read_prefs_mongos_primary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/secondary", test_read_prefs_mongos_secondary);
   TestSuite_AddMockServerTest (
      suite, "/ReadPrefs/mongos/secondaryPreferred", test_read_prefs_mongos_secondary_preferred);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/tags", test_read_prefs_mongos_tags);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/maxStaleness", test_read_prefs_mongos_max_staleness);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/hedgedReads", test_read_prefs_mongos_hedged_reads);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/mongos/readConcern", test_mongos_read_concern);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/OP_MSG/secondary", test_op_msg_direct_secondary);
   TestSuite_AddMockServerTest (suite, "/ReadPrefs/OP_MSG/mongos", test_op_msg_direct_mongos);
}
