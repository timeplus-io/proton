/*
 * Copyright 2018-present MongoDB, Inc.
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
#include "mongoc/mongoc-config.h"
#include "mongoc/mongoc-collection-private.h"
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-server-description-private.h"
#include "mongoc/mongoc-topology-description-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-util-private.h"

#include "TestSuite.h"
#include "test-conveniences.h"

#include "json-test.h"
#include "json-test-operations.h"
#include "test-libmongoc.h"

#ifdef _MSC_VER
#include <io.h>
#else
#include <dirent.h>
#endif

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

static bool
ends_with (const char *s, const char *suffix)
{
   size_t s_len;
   size_t suffix_len;

   if (!s) {
      return false;
   }

   s_len = strlen (s);
   suffix_len = strlen (suffix);
   return s_len >= suffix_len && !strcmp (s + s_len - suffix_len, suffix);
}

/* test that an event's "host" field is set to a reasonable value */
static void
assert_host_in_uri (const mongoc_host_list_t *host, const mongoc_uri_t *uri)
{
   const mongoc_host_list_t *hosts;

   hosts = mongoc_uri_get_hosts (uri);
   while (hosts) {
      if (_mongoc_host_list_compare_one (hosts, host)) {
         return;
      }

      hosts = hosts->next;
   }

   test_error ("Host \"%s\" not in \"%s\"", host->host_and_port, mongoc_uri_get_string (uri));
}


static void
started_cb (const mongoc_apm_command_started_t *event)
{
   json_test_ctx_t *ctx = (json_test_ctx_t *) mongoc_apm_command_started_get_context (event);
   bson_t *events = &ctx->events;
   char str[16];
   const char *key;
   bson_t *new_event;

   if (ctx->verbose) {
      char *cmd_json;

      cmd_json = bson_as_canonical_extended_json (event->command, NULL);
      printf ("%s\n", cmd_json);
      fflush (stdout);
      bson_free (cmd_json);
   }

   bson_mutex_lock (&ctx->mutex);

   /* Track the last two lsid's */
   if (bson_has_field (event->command, "lsid")) {
      bson_t lsid;

      /* Push on the circular queue */
      bson_destroy (ctx->sent_lsids[0]);
      ctx->sent_lsids[0] = ctx->sent_lsids[1];
      bson_lookup_doc (event->command, "lsid", &lsid);
      ctx->sent_lsids[1] = bson_copy (&lsid);
      bson_destroy (&lsid);
   }

   BSON_ASSERT (mongoc_apm_command_started_get_request_id (event) > 0);
   BSON_ASSERT (mongoc_apm_command_started_get_server_id (event) > 0);
   /* check that event->host is sane */
   assert_host_in_uri (event->host, ctx->test_framework_uri);
   new_event = BCON_NEW ("command_started_event",
                         "{",
                         "command",
                         BCON_DOCUMENT (event->command),
                         "command_name",
                         BCON_UTF8 (event->command_name),
                         "database_name",
                         BCON_UTF8 (event->database_name),
                         "operation_id",
                         BCON_INT64 (event->operation_id),
                         "}");

   bson_uint32_to_string (ctx->n_events, &key, str, sizeof str);
   BSON_APPEND_DOCUMENT (events, key, new_event);

   ctx->n_events++;

   bson_destroy (new_event);
   bson_mutex_unlock (&ctx->mutex);
}


static void
succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   json_test_ctx_t *ctx = (json_test_ctx_t *) mongoc_apm_command_succeeded_get_context (event);
   char str[16];
   const char *key;
   bson_t *new_event;

   if (ctx->verbose) {
      char *reply_json;

      reply_json = bson_as_canonical_extended_json (event->reply, NULL);
      MONGOC_DEBUG ("<-- COMMAND SUCCEEDED: %s\n", reply_json);
      bson_free (reply_json);
   }

   if (ctx->config->command_started_events_only) {
      return;
   }

   BSON_ASSERT (mongoc_apm_command_succeeded_get_request_id (event) > 0);
   BSON_ASSERT (mongoc_apm_command_succeeded_get_server_id (event) > 0);
   assert_host_in_uri (event->host, ctx->test_framework_uri);
   new_event = BCON_NEW ("command_succeeded_event",
                         "{",
                         "reply",
                         BCON_DOCUMENT (event->reply),
                         "command_name",
                         BCON_UTF8 (event->command_name),
                         "operation_id",
                         BCON_INT64 (event->operation_id),
                         "}");

   bson_mutex_lock (&ctx->mutex);
   bson_uint32_to_string (ctx->n_events, &key, str, sizeof str);
   BSON_APPEND_DOCUMENT (&ctx->events, key, new_event);

   ctx->n_events++;

   bson_destroy (new_event);
   bson_mutex_unlock (&ctx->mutex);
}


static void
failed_cb (const mongoc_apm_command_failed_t *event)
{
   json_test_ctx_t *ctx = (json_test_ctx_t *) mongoc_apm_command_failed_get_context (event);
   char str[16];
   const char *key;
   bson_t *new_event;

   if (ctx->verbose) {
      char *reply_json;

      reply_json = bson_as_canonical_extended_json (event->reply, NULL);
      MONGOC_DEBUG ("<-- %s COMMAND FAILED: %s\nREPLY: %s\n", event->command_name, event->error->message, reply_json);
      bson_free (reply_json);
   }

   if (ctx->config->command_started_events_only) {
      return;
   }

   BSON_ASSERT (mongoc_apm_command_failed_get_request_id (event) > 0);
   BSON_ASSERT (mongoc_apm_command_failed_get_server_id (event) > 0);
   assert_host_in_uri (event->host, ctx->test_framework_uri);

   new_event = BCON_NEW ("command_failed_event",
                         "{",
                         "command_name",
                         BCON_UTF8 (event->command_name),
                         "operation_id",
                         BCON_INT64 (event->operation_id),
                         "}");

   bson_mutex_lock (&ctx->mutex);
   bson_uint32_to_string (ctx->n_events, &key, str, sizeof str);
   BSON_APPEND_DOCUMENT (&ctx->events, key, new_event);

   ctx->n_events++;

   bson_destroy (new_event);
   bson_mutex_unlock (&ctx->mutex);
}

static void
server_changed_cb (const mongoc_apm_server_changed_t *event)
{
   json_test_ctx_t *ctx;
   const mongoc_server_description_t *sd;
   const mongoc_server_description_t *prev_sd;

   ctx = mongoc_apm_server_changed_get_context (event);
   sd = mongoc_apm_server_changed_get_new_description (event);
   prev_sd = mongoc_apm_server_changed_get_previous_description (event);
   if (ctx->verbose) {
      MONGOC_DEBUG ("SERVER CHANGED: (%s) %s --> %s\n",
                    sd->host.host_and_port,
                    mongoc_server_description_type (prev_sd),
                    mongoc_server_description_type (sd));
   }

   bson_mutex_lock (&ctx->mutex);
   if (sd->type == MONGOC_SERVER_UNKNOWN) {
      ctx->total_ServerMarkedUnknownEvent++;
   }
   if (sd->type == MONGOC_SERVER_RS_PRIMARY && !_mongoc_host_list_compare_one (&sd->host, &ctx->primary_host)) {
      ctx->total_PrimaryChangedEvent++;
      memcpy (&ctx->primary_host, &sd->host, sizeof (mongoc_host_list_t));
   }
   bson_mutex_unlock (&ctx->mutex);
}

void
set_apm_callbacks (json_test_ctx_t *ctx, mongoc_client_t *client)
{
   mongoc_apm_callbacks_t *callbacks;

   BSON_UNUSED (client);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, started_cb);
   /* Even if test only checks command started events (i.e.
    * command_started_events_only is set on test config), set callbacks for the
    * benefit of logging. */
   mongoc_apm_set_command_succeeded_cb (callbacks, succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, failed_cb);
   mongoc_client_set_apm_callbacks (ctx->client, callbacks, ctx);

   mongoc_apm_callbacks_destroy (callbacks);
}

void
set_apm_callbacks_pooled (json_test_ctx_t *ctx, mongoc_client_pool_t *pool)
{
   mongoc_apm_callbacks_t *callbacks;

   ASSERT (pool);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, started_cb);
   mongoc_apm_set_server_changed_cb (callbacks, server_changed_cb);
   mongoc_apm_set_command_succeeded_cb (callbacks, succeeded_cb);
   mongoc_apm_set_command_failed_cb (callbacks, failed_cb);
   mongoc_client_pool_set_apm_callbacks (pool, callbacks, ctx);
   mongoc_apm_callbacks_destroy (callbacks);
}


static bool
lsids_match (const bson_t *a, const bson_t *b)
{
   return match_bson (a, b, false);
}


typedef struct {
   char *command_name;
   int64_t cursor_id;
   bson_t lsids[2];
} apm_match_visitor_ctx_t;


void
apm_match_visitor_ctx_reset (apm_match_visitor_ctx_t *ctx)
{
   bson_free (ctx->command_name);
   ctx->command_name = NULL;
}


static match_action_t
apm_match_visitor (match_ctx_t *ctx, bson_iter_t *pattern_iter, bson_iter_t *doc_iter)
{
   const char *key = bson_iter_key (pattern_iter);
   apm_match_visitor_ctx_t *visitor_ctx = (apm_match_visitor_ctx_t *) ctx->visitor_ctx;

#define SHOULD_EXIST                          \
   do {                                       \
      if (!doc_iter) {                        \
         match_err (ctx, "expected %s", key); \
         return MATCH_ACTION_ABORT;           \
      }                                       \
   } while (0)
#define IS_COMMAND(cmd) (ends_with (ctx->path, "command") && !strcmp (key, cmd))

   if (ends_with (ctx->path, "command") && !visitor_ctx->command_name && doc_iter) {
      visitor_ctx->command_name = bson_strdup (bson_iter_key (doc_iter));
   }

   // Subdocuments in `command` should not have extra fields.
   if (NULL != strstr (ctx->path, ".command") && doc_iter) {
      if (BSON_ITER_HOLDS_DOCUMENT (doc_iter) && BSON_ITER_HOLDS_DOCUMENT (pattern_iter)) {
         bson_t doc_subdoc;
         bson_iter_bson (doc_iter, &doc_subdoc);
         bson_iter_t doc_subdoc_iter;
         bson_iter_init (&doc_subdoc_iter, &doc_subdoc);
         while (bson_iter_next (&doc_subdoc_iter)) {
            const char *subdoc_key = bson_iter_key (&doc_subdoc_iter);

            bson_t pattern_subdoc;
            bson_iter_bson (pattern_iter, &pattern_subdoc);
            bson_iter_t pattern_subdoc_iter;

            bool skip = false;
            if (ends_with (ctx->path, "updates") &&
                (0 == strcmp ("multi", subdoc_key) || 0 == strcmp ("upsert", subdoc_key))) {
               // libmongoc includes `multi: false` and `upsert: false`.
               // Some tests do not include `multi: false` and `upsert: false`
               // in expectations. See DRIVERS-2271 and DRIVERS-976.
               skip = true;
            }

            if (!skip && !bson_iter_init_find (&pattern_subdoc_iter, &pattern_subdoc, subdoc_key)) {
               match_err (ctx,
                          "unexpected extra field '%s' in captured event "
                          "command subdocument of field '%s'. pattern_subdoc=%s",
                          subdoc_key,
                          key,
                          tmp_json (&pattern_subdoc));
               return MATCH_ACTION_ABORT;
            }
         }
      }
   }

   if (IS_COMMAND ("find") || IS_COMMAND ("aggregate")) {
      /* New query. Next server reply or getMore will set cursor_id. */
      visitor_ctx->cursor_id = 0;
   } else if (!strcmp (key, "id") && ends_with (ctx->path, "cursor")) {
      visitor_ctx->cursor_id = bson_iter_as_int64 (doc_iter);
   } else if (!strcmp (key, "errmsg")) {
      /* "errmsg values of "" MUST assert that the value is not empty" */
      const char *errmsg = bson_iter_utf8 (pattern_iter, NULL);

      if (strcmp (errmsg, "") == 0) {
         if (!doc_iter || bson_iter_type (doc_iter) != BSON_TYPE_UTF8 ||
             strlen (bson_iter_utf8 (doc_iter, NULL)) == 0) {
            match_err (ctx, "expected non-empty 'errmsg'");
            return MATCH_ACTION_ABORT;
         }
         return MATCH_ACTION_SKIP;
      }
   } else if (IS_COMMAND ("getMore")) {
      /* "When encountering a cursor or getMore value of "42" in a test, the
       * driver MUST assert that the values are equal to each other and
       * greater than zero."
       */
      SHOULD_EXIST;
      if (visitor_ctx->cursor_id == 0) {
         /* A cursor id may not have been set in the visitor context if the spec
          * test only checked command started events. Set the cursor_id now, so
          * it can at least verify subsequent getMores use with the same id. */
         visitor_ctx->cursor_id = bson_iter_as_int64 (doc_iter);
      } else if (visitor_ctx->cursor_id != bson_iter_as_int64 (doc_iter)) {
         match_err (ctx,
                    "cursor requested in getMore (%" PRId64 ") does not match previously seen (%" PRId64 ")",
                    bson_iter_as_int64 (doc_iter),
                    visitor_ctx->cursor_id);
         return MATCH_ACTION_ABORT;
      }
   } else if (!strcmp (key, "lsid")) {
      const char *session_name = bson_iter_utf8 (pattern_iter, NULL);
      bson_t lsid;
      bool fail = false;

      SHOULD_EXIST;
      bson_iter_bson (doc_iter, &lsid);

      /* Transactions tests: "Each command-started event in "expectations"
       * includes an lsid with the value "session0" or "session1". Tests MUST
       * assert that the command's actual lsid matches the id of the correct
       * ClientSession named session0 or session1." */
      if (!strcmp (session_name, "session0") && !lsids_match (&visitor_ctx->lsids[0], &lsid)) {
         fail = true;
      }

      if (!strcmp (session_name, "session1") && !lsids_match (&visitor_ctx->lsids[1], &lsid)) {
         fail = true;
      }

      if (fail) {
         char *str = bson_as_json (&lsid, NULL);
         match_err (ctx, "expected %s, but used session: %s", session_name, str);
         bson_free (str);
         return MATCH_ACTION_ABORT;
      } else {
         return MATCH_ACTION_SKIP;
      }
   } else if (strstr (ctx->path, "updates.")) {
      /* tests expect "multi: false" and "upsert: false" explicitly;
       * we don't send them. fix when path is like "updates.0", "updates.1", ...
       */

      if (!strcmp (key, "multi") && !bson_iter_bool (pattern_iter)) {
         return MATCH_ACTION_SKIP;
      }
      if (!strcmp (key, "upsert") && !bson_iter_bool (pattern_iter)) {
         return MATCH_ACTION_SKIP;
      }
   } else if (visitor_ctx->command_name && !strcmp (visitor_ctx->command_name, "findAndModify") &&
              !strcmp (key, "new")) {
      /* transaction tests expect "new: false" explicitly; we don't send it */
      return MATCH_ACTION_SKIP;
   }

   return MATCH_ACTION_CONTINUE;
}


static void
_apm_match_error_context (const bson_t *actual, const bson_t *expectations)
{
   char *actual_str;
   char *expectations_str;

   actual_str = bson_as_canonical_extended_json (actual, NULL);
   expectations_str = bson_as_canonical_extended_json (expectations, NULL);
   fprintf (stderr,
            "Error in APM matching\nFull list of captured events: %s\nFull "
            "list of expectations: %s",
            actual_str,
            expectations_str);
   bson_free (actual_str);
   bson_free (expectations_str);
}

bool
skip_cse_list_collections (const bson_t *doc)
{
   /* see CDRIVER-3856: Sharing a MongoClient for metadata lookup can lead to
    * deadlock in drivers using automatic encryption. Since the C driver does
    * not use a separate 'mongoc_client_t' for listCollections and finds on the
    * key vault, we skip these checks. */
   const char *val;

   if (!bson_has_field (doc, "command_started_event.command.listCollections"))
      return false;

   if (!bson_has_field (doc, "command_started_event.command.$db"))
      return false;

   val = bson_lookup_utf8 (doc, "command_started_event.command.$db");
   if (0 != strcmp (val, "keyvault"))
      return false;

   return true;
}

/*
 *-----------------------------------------------------------------------
 *
 * check_json_apm_events --
 *
 *      Compare actual APM events with expected sequence. The two docs
 *      are each like:
 *
 * [
 *   {
 *     "command_started_event": {
 *       "command": { ... },
 *       "command_name": "count",
 *       "database_name": "command-monitoring-tests",
 *       "operation_id": 123
 *     }
 *   },
 *   {
 *     "command_failed_event": {
 *       "command_name": "count",
 *       "operation_id": 123
 *     }
 *   }
 * ]
 *
 *      If @allow_subset is true, then expectations is allowed to be
 *      a subset of events.
 *
 *-----------------------------------------------------------------------
 */
void
check_json_apm_events (json_test_ctx_t *ctx, const bson_t *expectations)
{
   bson_iter_t expectations_iter, actual_iter;
   bool allow_subset;
   match_ctx_t match_ctx = {{0}};
   apm_match_visitor_ctx_t apm_match_visitor_ctx = {0};
   int i;

   for (i = 0; i < 2; i++) {
      bson_copy_to (&ctx->lsids[i], &apm_match_visitor_ctx.lsids[i]);
   }

   /* Old mongod returns a double for "count", newer returns int32.
    * Ignore this and other insignificant type differences. */
   match_ctx.strict_numeric_types = false;
   match_ctx.retain_dots_in_keys = true;
   match_ctx.allow_placeholders = true;
   match_ctx.visitor_fn = apm_match_visitor;
   match_ctx.visitor_ctx = &apm_match_visitor_ctx;

   allow_subset = ctx->config->command_monitoring_allow_subset;

   BSON_ASSERT (bson_iter_init (&expectations_iter, expectations));
   BSON_ASSERT (bson_iter_init (&actual_iter, &ctx->events));

   /* Compare the captured actual events against the expectations. */
   while (bson_iter_next (&expectations_iter)) {
      bson_t expectation, actual;
      bool matched = false;

      bson_iter_bson (&expectations_iter, &expectation);
      /* match against the current actual event, and possibly skip actual events
       * if we allow subset matching. */
      while (bson_iter_next (&actual_iter)) {
         bson_iter_bson (&actual_iter, &actual);
         matched = match_bson_with_ctx (&actual, &expectation, &match_ctx);
         apm_match_visitor_ctx_reset (&apm_match_visitor_ctx);
         bson_destroy (&actual);

         if (matched) {
            break;
         }

         if (allow_subset) {
            /* if we allow matching only a subset of actual events, skip
             * non-matching ones */
            continue;
         }

         if (skip_cse_list_collections (&actual)) {
            continue;
         }

         _apm_match_error_context (&ctx->events, expectations);
         test_error ("could not match APM event\n"
                     "\texpected: %s\n\n"
                     "\tactual  : %s\n\n"
                     "\terror   : %s\n\n",
                     bson_as_canonical_extended_json (&expectation, NULL),
                     bson_as_canonical_extended_json (&actual, NULL),
                     match_ctx.errmsg);
      }

      if (!matched) {
         _apm_match_error_context (&ctx->events, expectations);
         test_error ("expectation unmatched\n"
                     "\texpected: %s\n\n",
                     bson_as_canonical_extended_json (&expectation, NULL));
      }

      bson_destroy (&expectation);
   }

   /* If we do not allow matching against a subset of actual events, check if
    * there are extra "actual" events */
   if (!allow_subset && bson_iter_next (&actual_iter)) {
      bson_t extra;

      bson_iter_bson (&actual_iter, &extra);
      _apm_match_error_context (&ctx->events, expectations);
      test_error ("extra actual event was not found in expectations: %s\n",
                  bson_as_canonical_extended_json (&extra, NULL));
   }

   for (i = 0; i < 2; i++) {
      bson_destroy (&apm_match_visitor_ctx.lsids[i]);
   }
}


/* Test that apm_match_visitor verifies the cursor id requested in a getMore
 * is the same cursor id returned in a find reply. */
void
test_apm_matching (void)
{
   apm_match_visitor_ctx_t match_visitor_ctx = {0};
   match_ctx_t match_ctx = {{0}};

   const char *e1 = "{"
                    "  'command_succeeded_event': {"
                    "    'command_name': 'find',"
                    "    'reply': {'cursor': { 'id': 123 }}"
                    "  }"
                    "}";

   const char *e2 = "{"
                    "  'command_started_event': {"
                    "    'command_name': 'getMore',"
                    "    'command': {'getMore': 124}"
                    "  }"
                    "}";

   match_ctx.visitor_fn = apm_match_visitor;
   match_ctx.visitor_ctx = &match_visitor_ctx;

   BSON_ASSERT (match_bson_with_ctx (tmp_bson (e1), tmp_bson (e1), &match_ctx));
   BSON_ASSERT (!match_bson_with_ctx (tmp_bson (e2), tmp_bson (e2), &match_ctx));
   ASSERT_CONTAINS (match_ctx.errmsg, "cursor requested in getMore");
   apm_match_visitor_ctx_reset (&match_visitor_ctx);
}

// Test that documents in command_started_event.command do not permit extra
// fields by default.
static void
test_apm_matching_extra_fields (void)
{
   // Extra fields are permitted in `command`.
   {
      apm_match_visitor_ctx_t match_visitor_ctx = {0};
      match_ctx_t match_ctx = {{0}};

      const char *event = BSON_STR ({"command_started_event" : {"command" : {"a" : 1, "b" : 2}}});
      const char *pattern = BSON_STR ({"command_started_event" : {"command" : {"a" : 1}}});

      match_ctx.visitor_fn = apm_match_visitor;
      match_ctx.visitor_ctx = &match_visitor_ctx;

      bool matched = match_bson_with_ctx (tmp_bson (event), tmp_bson (pattern), &match_ctx);
      ASSERT (matched);
      apm_match_visitor_ctx_reset (&match_visitor_ctx);
   }

   // Extra fields are not permitted in `command` sub-documents.
   {
      apm_match_visitor_ctx_t match_visitor_ctx = {0};
      match_ctx_t match_ctx = {{0}};

      const char *event = BSON_STR ({"command_started_event" : {"command" : {"subdoc" : {"a" : 1, "b" : 2}}}});
      const char *pattern = BSON_STR ({"command_started_event" : {"command" : {"subdoc" : {"a" : 1}}}});

      match_ctx.visitor_fn = apm_match_visitor;
      match_ctx.visitor_ctx = &match_visitor_ctx;

      bool matched = match_bson_with_ctx (tmp_bson (event), tmp_bson (pattern), &match_ctx);
      ASSERT (!matched);
      ASSERT_CONTAINS (match_ctx.errmsg, "unexpected extra field 'b'");
      apm_match_visitor_ctx_reset (&match_visitor_ctx);
   }

   // Extra fields are not permitted in `command` sub-arrays.
   {
      apm_match_visitor_ctx_t match_visitor_ctx = {0};
      match_ctx_t match_ctx = {{0}};

      const char *event = BSON_STR ({"command_started_event" : {"command" : {"subarray" : [ {"a" : 1, "b" : 2} ]}}});
      const char *pattern = BSON_STR ({"command_started_event" : {"command" : {"subarray" : [ {"a" : 1} ]}}});

      match_ctx.visitor_fn = apm_match_visitor;
      match_ctx.visitor_ctx = &match_visitor_ctx;

      bool matched = match_bson_with_ctx (tmp_bson (event), tmp_bson (pattern), &match_ctx);
      ASSERT (!matched);
      ASSERT_CONTAINS (match_ctx.errmsg, "unexpected extra field 'b'");
      apm_match_visitor_ctx_reset (&match_visitor_ctx);
   }
}


void
test_apm_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/apm_test_matching", test_apm_matching);
   TestSuite_Add (suite, "/apm_test_matching/extra_fields", test_apm_matching_extra_fields);
}
