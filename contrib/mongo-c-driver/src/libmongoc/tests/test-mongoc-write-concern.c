#include <mongoc/mongoc.h>
#include <mongoc/mongoc-write-concern-private.h>
#include <mongoc/mongoc-util-private.h>

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"


static void
test_write_concern_append (void)
{
   mongoc_write_concern_t *wc;
   bson_t *cmd;

   cmd = tmp_bson ("{'foo': 1}");
   capture_logs (true);

   /* cannot append invalid writeConcern */
   wc = NULL;
   BSON_ASSERT (!mongoc_write_concern_append (wc, cmd));

   /* append default writeConcern */
   wc = mongoc_write_concern_new ();
   ASSERT (mongoc_write_concern_is_default (wc));
   ASSERT_MATCH (cmd, "{'foo': 1, 'writeConcern': {'$exists': false}}");

   /* append writeConcern with w */
   mongoc_write_concern_set_w (wc, 1);
   BSON_ASSERT (mongoc_write_concern_append (wc, cmd));

   assert_match_bson (cmd, tmp_bson ("{'foo': 1, 'writeConcern': {'w': 1}}"), true);

   mongoc_write_concern_destroy (wc);
}

static void
test_write_concern_basic (void)
{
   mongoc_write_concern_t *write_concern;
   const bson_t *bson;
   bson_iter_t iter;

   write_concern = mongoc_write_concern_new ();

   BEGIN_IGNORE_DEPRECATIONS

   /*
    * Test defaults.
    */
   ASSERT (write_concern);
   ASSERT (!mongoc_write_concern_get_fsync (write_concern));
   ASSERT (!mongoc_write_concern_get_journal (write_concern));
   ASSERT (mongoc_write_concern_get_w (write_concern) == MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (!mongoc_write_concern_get_wtimeout_int64 (write_concern));
   ASSERT (!mongoc_write_concern_get_wmajority (write_concern));

   mongoc_write_concern_set_fsync (write_concern, true);
   ASSERT (mongoc_write_concern_get_fsync (write_concern));
   mongoc_write_concern_set_fsync (write_concern, false);
   ASSERT (!mongoc_write_concern_get_fsync (write_concern));

   mongoc_write_concern_set_journal (write_concern, true);
   ASSERT (mongoc_write_concern_get_journal (write_concern));
   mongoc_write_concern_set_journal (write_concern, false);
   ASSERT (!mongoc_write_concern_get_journal (write_concern));

   /*
    * Test changes to w.
    */
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_MAJORITY);
   ASSERT (mongoc_write_concern_get_wmajority (write_concern));
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (!mongoc_write_concern_get_wmajority (write_concern));
   mongoc_write_concern_set_wmajority (write_concern, 1000);
   ASSERT (mongoc_write_concern_get_wmajority (write_concern));
   ASSERT (mongoc_write_concern_get_wtimeout (write_concern) == 1000);
   mongoc_write_concern_set_wtimeout (write_concern, 0);
   ASSERT (!mongoc_write_concern_get_wtimeout (write_concern));
   mongoc_write_concern_set_wtimeout_int64 (write_concern, INT64_MAX);
   ASSERT (mongoc_write_concern_get_wtimeout_int64 (write_concern) == INT64_MAX);
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (mongoc_write_concern_get_w (write_concern) == MONGOC_WRITE_CONCERN_W_DEFAULT);
   mongoc_write_concern_set_w (write_concern, 3);
   ASSERT (mongoc_write_concern_get_w (write_concern) == 3);

   /*
    * Check generated bson.
    */
   mongoc_write_concern_set_fsync (write_concern, true);
   mongoc_write_concern_set_journal (write_concern, true);

   bson = _mongoc_write_concern_get_bson (write_concern);
   ASSERT (bson);
   ASSERT (bson_iter_init_find (&iter, bson, "fsync") && BSON_ITER_HOLDS_BOOL (&iter) && bson_iter_bool (&iter));
   ASSERT (bson_iter_init_find (&iter, bson, "j") && BSON_ITER_HOLDS_BOOL (&iter) && bson_iter_bool (&iter));
   ASSERT (bson_iter_init_find (&iter, bson, "w") && BSON_ITER_HOLDS_INT32 (&iter) && bson_iter_int32 (&iter) == 3);

   mongoc_write_concern_destroy (write_concern);

   END_IGNORE_DEPRECATIONS
}


static void
test_write_concern_bson_omits_defaults (void)
{
   mongoc_write_concern_t *write_concern;
   const bson_t *bson;
   bson_iter_t iter;

   write_concern = mongoc_write_concern_new ();

   /*
    * Check generated bson.
    */
   ASSERT (write_concern);

   bson = _mongoc_write_concern_get_bson (write_concern);
   ASSERT (bson);
   ASSERT (!bson_iter_init_find (&iter, bson, "fsync"));
   ASSERT (!bson_iter_init_find (&iter, bson, "j"));

   mongoc_write_concern_destroy (write_concern);
}


static void
test_write_concern_bson_includes_false_fsync_and_journal (void)
{
   mongoc_write_concern_t *write_concern;
   const bson_t *bson;
   bson_iter_t iter;

   write_concern = mongoc_write_concern_new ();

   /*
    * Check generated bson.
    */
   ASSERT (write_concern);
   mongoc_write_concern_set_fsync (write_concern, false);
   mongoc_write_concern_set_journal (write_concern, false);

   bson = _mongoc_write_concern_get_bson (write_concern);
   ASSERT (bson);
   ASSERT (bson_iter_init_find (&iter, bson, "fsync") && BSON_ITER_HOLDS_BOOL (&iter) && !bson_iter_bool (&iter));
   ASSERT (bson_iter_init_find (&iter, bson, "j") && BSON_ITER_HOLDS_BOOL (&iter) && !bson_iter_bool (&iter));
   ASSERT (!bson_iter_init_find (&iter, bson, "w"));

   mongoc_write_concern_destroy (write_concern);
}


static void
test_write_concern_fsync_and_journal_w1_and_validity (void)
{
   mongoc_write_concern_t *write_concern = mongoc_write_concern_new ();

   /*
    * Journal and fsync should imply w=1 regardless of w; however, journal and
    * fsync logically conflict with w=0 and w=-1, so a write concern with such
    * a combination of options will be considered invalid.
    */

   /* No write concern needs GLE, but not "valid" */
   ASSERT (mongoc_write_concern_is_acknowledged (NULL));
   ASSERT (!mongoc_write_concern_is_valid (NULL));

   /* Default write concern needs GLE and is valid */
   ASSERT (write_concern);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));
   ASSERT (!mongoc_write_concern_journal_is_set (write_concern));

   /* w=0 does not need GLE and is valid */
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   ASSERT (!mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));
   ASSERT (!mongoc_write_concern_journal_is_set (write_concern));

   /* fsync=true needs GLE, but it conflicts with w=0 */
   mongoc_write_concern_set_fsync (write_concern, true);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (!mongoc_write_concern_is_valid (write_concern));
   ASSERT (!mongoc_write_concern_journal_is_set (write_concern));
   mongoc_write_concern_set_fsync (write_concern, false);

   /* journal=true needs GLE, but it conflicts with w=0 */
   mongoc_write_concern_set_journal (write_concern, true);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (!mongoc_write_concern_is_valid (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));
   mongoc_write_concern_set_journal (write_concern, false);

   /* w=-1 does not need GLE and is valid */
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED);
   ASSERT (!mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));

   /* fsync=true needs GLE, but it conflicts with w=-1 */
   mongoc_write_concern_set_fsync (write_concern, true);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (!mongoc_write_concern_is_valid (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));

   /* journal=true needs GLE, but it conflicts with w=-1 */
   mongoc_write_concern_set_fsync (write_concern, false);
   mongoc_write_concern_set_journal (write_concern, true);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));

   /* fsync=true with w=default needs GLE and is valid */
   mongoc_write_concern_set_journal (write_concern, false);
   mongoc_write_concern_set_fsync (write_concern, true);
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));

   /* journal=true with w=default needs GLE and is valid */
   mongoc_write_concern_set_journal (write_concern, false);
   mongoc_write_concern_set_fsync (write_concern, true);
   mongoc_write_concern_set_w (write_concern, MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (mongoc_write_concern_is_acknowledged (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));
   ASSERT (mongoc_write_concern_journal_is_set (write_concern));

   mongoc_write_concern_destroy (write_concern);
}

static void
test_write_concern_wtimeout_validity (void)
{
   mongoc_write_concern_t *write_concern = mongoc_write_concern_new ();

   /* Test defaults */
   ASSERT (write_concern);
   ASSERT (mongoc_write_concern_get_w (write_concern) == MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (mongoc_write_concern_get_wtimeout_int64 (write_concern) == 0);
   ASSERT (!mongoc_write_concern_get_wmajority (write_concern));

   /* mongoc_write_concern_set_wtimeout_int64() ignores invalid wtimeout */
   mongoc_write_concern_set_wtimeout_int64 (write_concern, -1);
   ASSERT (mongoc_write_concern_get_w (write_concern) == MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT (mongoc_write_concern_get_wtimeout_int64 (write_concern) == 0);
   ASSERT (!mongoc_write_concern_get_wmajority (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));

   /* mongoc_write_concern_set_wmajority() ignores invalid wtimeout */
   mongoc_write_concern_set_wmajority (write_concern, -1);
   ASSERT (mongoc_write_concern_get_w (write_concern) == MONGOC_WRITE_CONCERN_W_MAJORITY);
   ASSERT (mongoc_write_concern_get_wtimeout_int64 (write_concern) == 0);
   ASSERT (mongoc_write_concern_get_wmajority (write_concern));
   ASSERT (mongoc_write_concern_is_valid (write_concern));

   /* Manually assigning a negative wtimeout will make the write concern invalid
    */
   write_concern->wtimeout = -1;
   ASSERT (!mongoc_write_concern_is_valid (write_concern));

   mongoc_write_concern_destroy (write_concern);
}

static void
_test_write_concern_from_iterator (const char *swc, bool ok, bool is_default)
{
   bson_t *bson = tmp_bson (swc);
   const bson_t *bson2;
   mongoc_write_concern_t *wc;
   bson_iter_t iter;
   bson_error_t error;

   if (test_suite_debug_output ()) {
      fprintf (stdout, "  - %s\n", swc);
      fflush (stdout);
   }

   bson_iter_init_find (&iter, bson, "writeConcern");
   wc = _mongoc_write_concern_new_from_iter (&iter, &error);
   if (ok) {
      BSON_ASSERT (wc);
      ASSERT (mongoc_write_concern_is_default (wc) == is_default);
      bson2 = _mongoc_write_concern_get_bson (wc);
      ASSERT (bson_compare (bson, bson2));
      mongoc_write_concern_destroy (wc);
   } else {
      BSON_ASSERT (!wc);
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid writeConcern");
   }
}

static void
test_write_concern_from_iterator (void)
{
   _test_write_concern_from_iterator ("{'writeConcern': {}}", true, true);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'majority'}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'majority', 'j': true}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'sometag'}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'sometag', 'j': true}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'sometag', 'j': false}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 1, 'j': true}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 1, 'j': false}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 0, 'j': true}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 0, 'j': false}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 1}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'j': true}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'j': false}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': -1}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': -2}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': -3}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': -4}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': -5}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'majority', 'wtimeout': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 'sometag', 'wtimeout': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'wtimeout': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'wtimeout': -42}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'wtimeout': {'$numberLong': '123'}}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'wtimeout': {'$numberLong': '2147483648'}}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 1, 'wtimeout': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 0, 'wtimeout': 42}}", true, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': 1.0}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': {'some': 'stuff'}}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'w': []}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'wtimeout': 'never'}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'j': 'never'}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'j': 1.0}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'fsync': 1.0}}", false, false);
   _test_write_concern_from_iterator ("{'writeConcern': {'fsync': true}}", true, false);
}


static void
test_write_concern_always_mutable (void)
{
   mongoc_write_concern_t *write_concern;

   write_concern = mongoc_write_concern_new ();

   ASSERT (write_concern);

   mongoc_write_concern_set_fsync (write_concern, true);
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern), "{'fsync': true}");

   mongoc_write_concern_set_journal (write_concern, true);
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern), "{'fsync': true, 'j': true}");

   mongoc_write_concern_set_w (write_concern, 2);
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern), "{'w': 2, 'fsync': true, 'j': true}");

   mongoc_write_concern_set_wtimeout_int64 (write_concern, 100);
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern), "{'w': 2, 'fsync': true, 'j': true, 'wtimeout': 100}");

   mongoc_write_concern_set_wmajority (write_concern, 200);
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern),
                 "{'w': 'majority', 'fsync': true, 'j': true, 'wtimeout': 200}");

   mongoc_write_concern_set_wtag (write_concern, "MultipleDC");
   ASSERT_MATCH (_mongoc_write_concern_get_bson (write_concern),
                 "{'w': 'MultipleDC', 'fsync': true, 'j': true, 'wtimeout': 200}");

   mongoc_write_concern_destroy (write_concern);
}


static void
_test_wc_request (future_t *future, mock_server_t *server, bson_error_t *error)
{
   request_t *request;

   BSON_UNUSED (error);

   request = mock_server_receives_msg (server, MONGOC_MSG_NONE, tmp_bson ("{'$db': 'db', 'writeConcern': {'w': 2}}"));
   reply_to_request_with_ok_and_destroy (request);
   BSON_ASSERT (future_get_bool (future));

   future_destroy (future);
}


static void
test_write_concern (void)
{
   bson_t *opts;
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   future_t *future;
   bson_error_t error;

   opts = tmp_bson ("{'writeConcern': {'w': 2}}");
   server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);
   client = test_framework_client_new_from_uri (mock_server_get_uri (server), NULL);
   collection = mongoc_client_get_collection (client, "db", "collection");

   /*
    * aggregate with $out
    */
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, tmp_bson ("[{'$out': 'foo'}]"), opts, NULL);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   /*
    * generic mongoc_client_write_command_with_opts
    */
   future = future_client_write_command_with_opts (client, "db", tmp_bson ("{'foo': 1}"), opts, NULL, &error);
   _test_wc_request (future, server, &error);

   /*
    * drop
    */
   future = future_collection_drop_with_opts (collection, opts, &error);
   _test_wc_request (future, server, &error);

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

/* Test that CDRIVER-2902 has been fixed.
 * The bug was that we did not correctly swab for the flagBits in OP_MSG. */
static void
test_write_concern_unacknowledged (void)
{
   mongoc_client_t *client;
   mongoc_write_concern_t *wc;
   mongoc_collection_t *coll;
   bson_error_t error;
   bool r;
   bson_t reply;
   bson_t opts;
   const bson_t **docs;

   client = test_framework_new_default_client ();
   coll = mongoc_client_get_collection (client, "db", "coll");

   /* w:0 in OP_MSG is indicated by setting the moreToCome flag in OP_MSG. That
    * tells the recipient not to send a response. */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   bson_init (&opts);
   mongoc_write_concern_append (wc, &opts);

   /* In this insert_one with w:0, we write an OP_MSG on the socket, but don't
    * read a reply. Before CDRIVER-2902 was fixed, since we forget to set
    * moreToCome, the server still sends a reply. */
   r = mongoc_collection_insert_one (coll, tmp_bson ("{}"), &opts, &reply, &error);
   ASSERT_OR_PRINT (r, error);
   ASSERT (bson_empty (&reply));
   bson_destroy (&reply);
   bson_destroy (&opts);

   docs = bson_malloc0 (sizeof (bson_t *) * 2);
   docs[0] = tmp_bson ("{}");
   docs[1] = tmp_bson ("{}");

   /* In the next insert_many, before CDRIVER-2902 was fixed, we would read that
    * old reply. */
   r = mongoc_collection_insert_many (coll, docs, 2, NULL, &reply, &error);
   bson_free ((void *) docs);
   ASSERT_OR_PRINT (r, error);

   /* The replies are distinguished by the insertedCount. */
   ASSERT_MATCH (&reply, "{'insertedCount': 2}");

   bson_destroy (&reply);
   mongoc_collection_destroy (coll);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
}


/* Regression test to to demonstrate that a 64-bit wtimeoutms value is properly
 * preserved. */
static void
test_write_concern_wtimeout_preserved (void)
{
   mongoc_write_concern_t *write_concern = mongoc_write_concern_new ();
   bson_t *cmd = tmp_bson ("{}");
   bson_iter_t iter;
   bson_iter_t child;

   ASSERT (write_concern);

   mongoc_write_concern_set_wtimeout_int64 (write_concern, INT64_MAX);
   mongoc_write_concern_append (write_concern, cmd);

   ASSERT (bson_iter_init_find (&iter, cmd, "writeConcern"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   ASSERT (bson_iter_recurse (&iter, &child));
   ASSERT (bson_iter_next (&child));
   ASSERT_CMPSTR (bson_iter_key (&child), "wtimeout");
   ASSERT_CMPINT64 (bson_iter_int64 (&child), ==, INT64_MAX);

   mongoc_write_concern_destroy (write_concern);
}

/* callback that records write concern for commands */
static void
write_concern_count (const mongoc_apm_command_started_t *event)
{
   const bson_t *command = mongoc_apm_command_started_get_command (event);
   bson_iter_t iter, iter_rec;

   if (bson_iter_init (&iter, command)) {
      int *sent_collection_w = (int *) mongoc_apm_command_started_get_context (event);
      if (bson_iter_find_descendant (&iter, "writeConcern.w", &iter_rec)) {
         *sent_collection_w = bson_iter_int32 (&iter_rec);
      } else {
         *sent_collection_w = MONGOC_WRITE_CONCERN_W_DEFAULT; /* no write
                                                                 concern sent.
                                                                 default used */
      }
   }
}

/* Addresses concerns brought up in CDRIVER-3595. This function comprises
 * three tests. The first tests that with no txn in progress, wc is inherited
 * from collection.  The second tests that with a txn in progress, no
 * wc is inherited from collection, unacknowledged or acknowledged.
 * The third tests that an attempt to send an lsid with an
 * unacknowledged wc fails (where wc is inherited from collection).
 * All commands are fam commands. */
static void
test_write_concern_inheritance_fam_txn (bool in_session, bool in_txn)
{
   mongoc_find_and_modify_opts_t *opts = NULL;
   bson_t *update;
   bson_t *session_id;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   mongoc_write_concern_t *wc;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_client_session_t *session = NULL;
   mongoc_apm_callbacks_t *callbacks;
   int sent_w = MONGOC_WRITE_CONCERN_W_DEFAULT;
   bool success;

   client = test_framework_new_default_client ();
   collection = mongoc_client_get_collection (client, "db", "collection");

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, write_concern_count);
   mongoc_client_set_apm_callbacks (client, callbacks, (void *) &sent_w);

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 1);
   mongoc_collection_set_write_concern (collection, wc);

   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");
   update = bson_new ();
   session_id = bson_new ();
   opts = mongoc_find_and_modify_opts_new ();

   if (in_session) {
      session = mongoc_client_start_session (client, NULL, &error);
      ASSERT_OR_PRINT (session, error);
      success = mongoc_client_session_append (session, session_id, &error);
      ASSERT_OR_PRINT (success, error);
      mongoc_find_and_modify_opts_append (opts, session_id);
   }
   if (in_txn) {
      BSON_ASSERT (in_session);
      success = mongoc_client_session_start_transaction (session, NULL, &error);
      ASSERT_OR_PRINT (success, error);
   }

   mongoc_find_and_modify_opts_set_update (opts, update);
   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, NULL, &error);

   if (in_txn) {
      /* check that the sent write concern is not inherited */
      BSON_ASSERT (sent_w == MONGOC_WRITE_CONCERN_W_DEFAULT);
      BSON_ASSERT (success);
   } else {
      /* assert that write concern is inherited. Two tests reach this
       * code.  No txn in progress. */
      ASSERT_OR_PRINT (success, error);
      BSON_ASSERT (success);
      BSON_ASSERT (sent_w == 1);
   }

   mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   mongoc_collection_set_write_concern (collection, wc);

   sent_w = MONGOC_WRITE_CONCERN_W_DEFAULT;
   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, NULL, &error);

   if (in_txn) {
      /* check that the sent write concern is not inherited */
      BSON_ASSERT (sent_w == MONGOC_WRITE_CONCERN_W_DEFAULT);
      success = mongoc_client_session_commit_transaction (session, NULL, &error);
      ASSERT_OR_PRINT (success, error);
   } else if (!in_session) {
      /* assert that write concern is inherited */
      BSON_ASSERT (success);
      BSON_ASSERT (sent_w == MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   } else {
      /* case where lsid is sent with unacknowledged write concern */
      BSON_ASSERT (!success);
      ASSERT_CONTAINS (error.message,
                       "Cannot use client session with "
                       "unacknowledged command");
   }

   mongoc_write_concern_destroy (wc);
   mongoc_client_session_destroy (session);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   bson_destroy (update);
   bson_destroy (&query);
   bson_destroy (session_id);
   mongoc_find_and_modify_opts_destroy (opts);
}

static void
test_fam_no_session_no_txn (void *unused)
{
   BSON_UNUSED (unused);
   test_write_concern_inheritance_fam_txn (false, false);
}

static void
test_fam_session_no_txn (void *unused)
{
   BSON_UNUSED (unused);
   test_write_concern_inheritance_fam_txn (true, false);
}

static void
test_fam_session_txn (void *unused)
{
   BSON_UNUSED (unused);
   test_write_concern_inheritance_fam_txn (true, true);
}

void
test_write_concern_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/WriteConcern/append", test_write_concern_append);
   TestSuite_Add (suite, "/WriteConcern/basic", test_write_concern_basic);
   TestSuite_Add (suite, "/WriteConcern/bson_omits_defaults", test_write_concern_bson_omits_defaults);
   TestSuite_Add (suite,
                  "/WriteConcern/bson_includes_false_fsync_and_journal",
                  test_write_concern_bson_includes_false_fsync_and_journal);
   TestSuite_Add (
      suite, "/WriteConcern/fsync_and_journal_gle_and_validity", test_write_concern_fsync_and_journal_w1_and_validity);
   TestSuite_Add (suite, "/WriteConcern/wtimeout_validity", test_write_concern_wtimeout_validity);
   TestSuite_Add (suite, "/WriteConcern/from_iterator", test_write_concern_from_iterator);
   TestSuite_Add (suite, "/WriteConcern/always_mutable", test_write_concern_always_mutable);
   TestSuite_Add (suite, "/WriteConcern/wtimeout_preserved", test_write_concern_wtimeout_preserved);
   TestSuite_AddMockServerTest (suite, "/WriteConcern", test_write_concern);
   TestSuite_AddLive (suite, "/WriteConcern/unacknowledged", test_write_concern_unacknowledged);
   TestSuite_AddFull (
      suite, "/WriteConcern/inherited_fam", test_fam_no_session_no_txn, NULL, NULL, TestSuite_CheckLive);
   TestSuite_AddFull (suite,
                      "/WriteConcern/inherited_fam_session_no_txn",
                      test_fam_session_no_txn,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_txns);
   TestSuite_AddFull (suite,
                      "/WriteConcern/inherited_fam_txn",
                      test_fam_session_txn,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_txns);
}
