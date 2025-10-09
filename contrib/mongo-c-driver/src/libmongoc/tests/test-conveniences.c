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

#include "bson/bson-types.h"

#include "mongoc/mongoc-array-private.h"
/* For strcasecmp on Windows */
#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-write-concern.h"
#include "mongoc/mongoc-write-concern-private.h"
#include "mongoc/mongoc-cluster-private.h"
#include "mongoc/mongoc-client-private.h"

#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif


static bool gConveniencesInitialized = false;
static mongoc_array_t gTmpBsonArray;
static mongoc_array_t gTmpStringArray;

static char *gHugeString;
static size_t gHugeStringLength;
static char *gFourMBString;


void
test_conveniences_init (void)
{
   if (!gConveniencesInitialized) {
      _mongoc_array_init (&gTmpBsonArray, sizeof (bson_t *));
      _mongoc_array_init (&gTmpStringArray, sizeof (char *));
      atexit (test_conveniences_cleanup);
      gConveniencesInitialized = true;
      gHugeString = NULL;
      gFourMBString = NULL;
   }
}


void
test_conveniences_cleanup (void)
{
   bson_t *doc;

   if (gConveniencesInitialized) {
      for (size_t i = 0u; i < gTmpBsonArray.len; i++) {
         doc = _mongoc_array_index (&gTmpBsonArray, bson_t *, i);
         bson_destroy (doc);
      }

      for (size_t i = 0u; i < gTmpStringArray.len; i++) {
         char *str;

         str = _mongoc_array_index (&gTmpStringArray, char *, i);
         bson_free (str);
      }

      _mongoc_array_destroy (&gTmpBsonArray);
      _mongoc_array_destroy (&gTmpStringArray);

      bson_free (gHugeString);
      bson_free (gFourMBString);
      gConveniencesInitialized = false;
   }
}


void
value_init_from_doc (bson_value_t *value, const bson_t *doc)
{
   BSON_ASSERT (doc);

   value->value_type = BSON_TYPE_DOCUMENT;
   value->value.v_doc.data = bson_malloc ((size_t) doc->len);
   memcpy (value->value.v_doc.data, bson_get_data (doc), (size_t) doc->len);
   value->value.v_doc.data_len = doc->len;
}


MONGOC_PRINTF_FORMAT (1, 2)
bson_t *
tmp_bson (const char *json, ...)
{
   va_list args;
   bson_error_t error;
   char *formatted;
   char *double_quoted;
   bson_t *doc;

   test_conveniences_init ();

   if (json) {
      va_start (args, json);
      formatted = bson_strdupv_printf (json, args);
      va_end (args);

      double_quoted = single_quotes_to_double (formatted);
      doc = bson_new_from_json ((const uint8_t *) double_quoted, -1, &error);

      if (!doc) {
         test_error ("tmp_bson error %s: parsing: %s", error.message, json);
      }

      bson_free (formatted);
      bson_free (double_quoted);

   } else {
      doc = bson_new ();
   }

   _mongoc_array_append_val (&gTmpBsonArray, doc);

   return doc;
}

MONGOC_PRINTF_FORMAT (1, 2)
const char *
tmp_str (const char *format, ...)
{
   va_list args;
   char *str;

   test_conveniences_init ();
   va_start (args, format);
   str = bson_strdupv_printf (format, args);
   va_end (args);

   _mongoc_array_append_val (&gTmpStringArray, str);
   return (const char *) str;
}

const char *
tmp_json (const bson_t *bson)
{
   char *str;

   test_conveniences_init ();

   if (!bson) {
      return "(NULL)";
   }

   str = bson_as_canonical_extended_json (bson, NULL);
   _mongoc_array_append_val (&gTmpStringArray, str);
   return (const char *) str;
}

/* Look up a field by a dot separate path, or abort. */
void
bson_lookup (const bson_t *bson, const char *path, bson_iter_t *out)
{
   bson_iter_t iter;

   bson_iter_init (&iter, bson);
   if (!bson_iter_find_descendant (&iter, path, out)) {
      test_error ("'%s' field not found in BSON: %s", path, tmp_json (bson));
   }
}
/*--------------------------------------------------------------------------
 *
 * bson_iter_bson --
 *
 *       Statically init a bson_t from an iter at an array or document.
 *
 *--------------------------------------------------------------------------
 */
void
bson_iter_bson (const bson_iter_t *iter, bson_t *bson)
{
   uint32_t len;
   const uint8_t *data;

   BSON_ASSERT (BSON_ITER_HOLDS_DOCUMENT (iter) || BSON_ITER_HOLDS_ARRAY (iter));

   if (BSON_ITER_HOLDS_DOCUMENT (iter)) {
      bson_iter_document (iter, &len, &data);
   } else {
      bson_iter_array (iter, &len, &data);
   }

   BSON_ASSERT (bson_init_static (bson, data, len));
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_utf8 --
 *
 *       Return a string by key, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
const char *
bson_lookup_utf8 (const bson_t *b, const char *key)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   ASSERT_WITH_MSG (BSON_ITER_HOLDS_UTF8 (&iter), "'%s' is not a string: %s", key, tmp_json (b));

   return bson_iter_utf8 (&iter, NULL);
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_value --
 *
 *       Return a bson_value_t or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
void
bson_lookup_value (const bson_t *b, const char *key, bson_value_t *value)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   bson_value_copy (bson_iter_value (&iter), value);
}

bson_t *
bson_lookup_bson (const bson_t *b, const char *key)
{
   bson_iter_t iter;
   bson_t tmp;

   bson_lookup (b, key, &iter);
   if (!BSON_ITER_HOLDS_DOCUMENT (&iter) && !BSON_ITER_HOLDS_ARRAY (&iter)) {
      test_error ("Expected '%s' to resolve to BSON: %s", key, tmp_json (b));
   }

   bson_iter_bson (&iter, &tmp);
   return bson_new_from_data (bson_get_data (&tmp), tmp.len);
}

/*--------------------------------------------------------------------------
 *
 * bson_lookup_doc --
 *
 *       Find a subdocument by key and return it by static-initializing
 *       the passed-in bson_t "doc". There is no need to bson_destroy
 *       "doc". Asserts and aborts if the key is absent or not a subdoc.
 *
 *--------------------------------------------------------------------------
 */
void
bson_lookup_doc (const bson_t *b, const char *key, bson_t *doc)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   bson_iter_bson (&iter, doc);
}

void
bson_lookup_doc_null_ok (const bson_t *b, const char *key, bson_t *doc)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   if (!BSON_ITER_HOLDS_NULL (&iter)) {
      bson_iter_bson (&iter, doc);
   } else {
      bson_init (doc);
   }
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_bool --
 *
 *       Return a bool by key, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
bool
bson_lookup_bool (const bson_t *b, const char *key)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   ASSERT_WITH_MSG (BSON_ITER_HOLDS_BOOL (&iter), "'%s' is not a bool: %s", key, tmp_json (b));

   return bson_iter_bool (&iter);
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_int32 --
 *
 *       Return an int32_t by key, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
int32_t
bson_lookup_int32 (const bson_t *b, const char *key)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   ASSERT_WITH_MSG (BSON_ITER_HOLDS_INT32 (&iter), "'%s' is not a int32: %s", key, tmp_json (b));

   return bson_iter_int32 (&iter);
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_int64 --
 *
 *       Return an int64_t by key, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
int64_t
bson_lookup_int64 (const bson_t *b, const char *key)
{
   bson_iter_t iter;

   bson_lookup (b, key, &iter);
   ASSERT_WITH_MSG (BSON_ITER_HOLDS_INT64 (&iter), "'%s' is not a int64: %s", key, tmp_json (b));

   return bson_iter_int64 (&iter);
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_read_concern --
 *
 *       Find a subdocument like {level: "majority"} and interpret it as a
 *       mongoc_read_concern_t, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
mongoc_read_concern_t *
bson_lookup_read_concern (const bson_t *b, const char *key)
{
   bson_t doc;
   mongoc_read_concern_t *rc = mongoc_read_concern_new ();

   bson_lookup_doc (b, key, &doc);
   if (!bson_has_field (&doc, "level")) {
      /* empty document means default read concern. */
      return rc;
   }
   mongoc_read_concern_set_level (rc, bson_lookup_utf8 (&doc, "level"));

   return rc;
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_write_concern --
 *
 *       Find a subdocument like {w: <int32>} and interpret it as a
 *       mongoc_write_concern_t, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
mongoc_write_concern_t *
bson_lookup_write_concern (const bson_t *b, const char *key)
{
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t doc;
   bson_iter_t iter;
   bson_iter_t w;

   bson_lookup_doc (b, key, &doc);
   BSON_ASSERT (bson_iter_init (&iter, &doc));

   if (!bson_iter_find_descendant (&iter, "w", &w)) {
      /* empty document means default write concern. */
      return wc;
   }

   if (BSON_ITER_HOLDS_NUMBER (&w)) {
      mongoc_write_concern_set_w (wc, (int32_t) bson_iter_as_int64 (&w));
   } else if (!strcmp (bson_iter_utf8 (&w, NULL), "majority")) {
      mongoc_write_concern_set_wmajority (wc, 0);
   } else {
      mongoc_write_concern_set_wtag (wc, bson_iter_utf8 (&w, NULL));
   }

   if (bson_has_field (&doc, "wtimeout")) {
      mongoc_write_concern_set_wtimeout_int64 (wc, bson_lookup_int32 (&doc, "wtimeout"));
   }

   if (bson_has_field (&doc, "j")) {
      mongoc_write_concern_set_journal (wc, bson_lookup_bool (&doc, "j"));
   }

   return wc;
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_read_prefs --
 *
 *       Find a subdocument like {mode: "mode"} and interpret it as a
 *       mongoc_read_prefs_t, or BSON_ASSERT and abort.
 *
 *--------------------------------------------------------------------------
 */
mongoc_read_prefs_t *
bson_lookup_read_prefs (const bson_t *b, const char *key)
{
   bson_t doc;
   const char *str;
   mongoc_read_mode_t mode;

   bson_lookup_doc (b, key, &doc);
   str = bson_lookup_utf8 (&doc, "mode");

   if (0 == strcasecmp ("primary", str)) {
      mode = MONGOC_READ_PRIMARY;
   } else if (0 == strcasecmp ("primarypreferred", str)) {
      mode = MONGOC_READ_PRIMARY_PREFERRED;
   } else if (0 == strcasecmp ("secondary", str)) {
      mode = MONGOC_READ_SECONDARY;
   } else if (0 == strcasecmp ("secondarypreferred", str)) {
      mode = MONGOC_READ_SECONDARY_PREFERRED;
   } else if (0 == strcasecmp ("nearest", str)) {
      mode = MONGOC_READ_NEAREST;
   } else {
      test_error ("Bad readPreference: {\"mode\": \"%s\"}.", str);
   }

   return mongoc_read_prefs_new (mode);
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_database_opts --
 *
 *       Interpret a subdocument as database options.
 *
 *--------------------------------------------------------------------------
 */
void
bson_lookup_database_opts (const bson_t *b, const char *key, mongoc_database_t *database)
{
   bson_t doc;
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   mongoc_read_prefs_t *prefs;

   bson_lookup_doc (b, key, &doc);

   if (bson_has_field (&doc, "readConcern")) {
      rc = bson_lookup_read_concern (&doc, "readConcern");
      mongoc_database_set_read_concern (database, rc);
      mongoc_read_concern_destroy (rc);
   }

   if (bson_has_field (&doc, "writeConcern")) {
      wc = bson_lookup_write_concern (&doc, "writeConcern");
      mongoc_database_set_write_concern (database, wc);
      mongoc_write_concern_destroy (wc);
   }

   if (bson_has_field (&doc, "readPreference")) {
      prefs = bson_lookup_read_prefs (&doc, "readPreference");
      mongoc_database_set_read_prefs (database, prefs);
      mongoc_read_prefs_destroy (prefs);
   }
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_collection_opts --
 *
 *       Interpret a subdocument as collection options.
 *
 *--------------------------------------------------------------------------
 */
void
bson_lookup_collection_opts (const bson_t *b, const char *key, mongoc_collection_t *collection)
{
   bson_t doc;
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   mongoc_read_prefs_t *prefs;

   bson_lookup_doc (b, key, &doc);

   if (bson_has_field (&doc, "readConcern")) {
      rc = bson_lookup_read_concern (&doc, "readConcern");
      mongoc_collection_set_read_concern (collection, rc);
      mongoc_read_concern_destroy (rc);
   }

   if (bson_has_field (&doc, "writeConcern")) {
      wc = bson_lookup_write_concern (&doc, "writeConcern");
      mongoc_collection_set_write_concern (collection, wc);
      mongoc_write_concern_destroy (wc);
   }

   if (bson_has_field (&doc, "readPreference")) {
      prefs = bson_lookup_read_prefs (&doc, "readPreference");
      mongoc_collection_set_read_prefs (collection, prefs);
      mongoc_read_prefs_destroy (prefs);
   }
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_txn_opts --
 *
 *       Interpret a subdocument as transaction options.
 *
 *--------------------------------------------------------------------------
 */
mongoc_transaction_opt_t *
bson_lookup_txn_opts (const bson_t *b, const char *key)
{
   bson_t doc;
   mongoc_transaction_opt_t *opts;
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   mongoc_read_prefs_t *prefs;
   int64_t max_commit_time_ms;

   bson_lookup_doc (b, key, &doc);
   opts = mongoc_transaction_opts_new ();

   if (bson_has_field (&doc, "readConcern")) {
      rc = bson_lookup_read_concern (&doc, "readConcern");
      mongoc_transaction_opts_set_read_concern (opts, rc);
      mongoc_read_concern_destroy (rc);
   }

   if (bson_has_field (&doc, "writeConcern")) {
      wc = bson_lookup_write_concern (&doc, "writeConcern");
      mongoc_transaction_opts_set_write_concern (opts, wc);
      mongoc_write_concern_destroy (wc);
   }

   if (bson_has_field (&doc, "readPreference")) {
      prefs = bson_lookup_read_prefs (&doc, "readPreference");
      mongoc_transaction_opts_set_read_prefs (opts, prefs);
      mongoc_read_prefs_destroy (prefs);
   }

   if (bson_has_field (&doc, "maxCommitTimeMS")) {
      max_commit_time_ms = bson_lookup_int32 (&doc, "maxCommitTimeMS");
      mongoc_transaction_opts_set_max_commit_time_ms (opts, max_commit_time_ms);
   }

   return opts;
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_session_opts --
 *
 *       Interpret a subdocument as client session options.
 *
 *--------------------------------------------------------------------------
 */
mongoc_session_opt_t *
bson_lookup_session_opts (const bson_t *b, const char *key)
{
   bson_t doc;
   mongoc_session_opt_t *opts;

   bson_lookup_doc (b, key, &doc);
   opts = mongoc_session_opts_new ();

   mongoc_session_opts_set_causal_consistency (opts, _mongoc_lookup_bool (&doc, "causalConsistency", true));

   if (bson_has_field (&doc, "defaultTransactionOptions")) {
      mongoc_transaction_opt_t *txn_opts;

      txn_opts = bson_lookup_txn_opts (&doc, "defaultTransactionOptions");
      mongoc_session_opts_set_default_transaction_opts (opts, txn_opts);
      mongoc_transaction_opts_destroy (txn_opts);
   }

   return opts;
}


/*--------------------------------------------------------------------------
 *
 * bson_lookup_session --
 *
 *       Interpret a subdocument as a client session with options.
 *
 *--------------------------------------------------------------------------
 */
mongoc_client_session_t *
bson_lookup_session (const bson_t *b, const char *key, mongoc_client_t *client)
{
   ASSERT (client);

   mongoc_session_opt_t *opts;
   mongoc_client_session_t *session;
   bson_error_t error;

   opts = bson_lookup_session_opts (b, key);
   session = mongoc_client_start_session (client, opts, &error);
   ASSERT_OR_PRINT (session, error);
   mongoc_session_opts_destroy (opts);
   return session;
}


static bool
get_exists_operator (const bson_value_t *value, bool *exists);

static bool
get_empty_operator (const bson_value_t *value, bool *exists);

static bool
get_type_operator (const bson_value_t *value, bson_type_t *out);

static bool
is_empty_doc_or_array (const bson_value_t *value);

static bool
find (bson_iter_t *iter, const bson_t *doc, const char *key, bool is_command, bool is_first, bool retain_dots_in_keys);


/*--------------------------------------------------------------------------
 *
 * single_quotes_to_double --
 *
 *       Copy str with single-quotes replaced by double.
 *
 * Returns:
 *       A string you must bson_free.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

char *
single_quotes_to_double (const char *str)
{
   char *result = bson_strdup (str);
   char *p;

   for (p = result; *p; p++) {
      if (*p == '\'') {
         *p = '"';
      }
   }

   return result;
}


/*--------------------------------------------------------------------------
 *
 * match_json --
 *
 *       Call match_bson on "doc" and "json_pattern".
 *       For convenience, single-quotes are synonymous with double-quotes.
 *
 *       A NULL doc or NULL json_pattern means "{}".
 *
 * Returns:
 *       True or false.
 *
 * Side effects:
 *       Logs if no match. Aborts if json is malformed.
 *
 *--------------------------------------------------------------------------
 */

MONGOC_PRINTF_FORMAT (6, 7)
bool
match_json (const bson_t *doc,
            bool is_command,
            const char *filename,
            int lineno,
            const char *funcname,
            const char *json_pattern,
            ...)
{
   va_list args;
   char *json_pattern_formatted;
   char *double_quoted;
   bson_error_t error;
   bson_t *pattern;
   match_ctx_t ctx = {{0}};
   bool matches;

   va_start (args, json_pattern);
   json_pattern_formatted = bson_strdupv_printf (json_pattern ? json_pattern : "{}", args);
   va_end (args);

   double_quoted = single_quotes_to_double (json_pattern_formatted);
   pattern = bson_new_from_json ((const uint8_t *) double_quoted, -1, &error);

   if (!pattern) {
      test_error ("couldn't parse JSON: %s", error.message);
   }

   ctx.is_command = is_command;
   matches = match_bson_with_ctx (doc, pattern, &ctx);

   if (!matches) {
      char *as_string = doc ? bson_as_canonical_extended_json (doc, NULL) : NULL;
      fprintf (stderr,
               "ASSERT_MATCH failed with document:\n\n"
               "%s\n"
               "pattern:\n%s\n"
               "%s\n"
               "%s:%d %s()\n",
               as_string ? as_string : "{}",
               double_quoted,
               ctx.errmsg,
               filename,
               lineno,
               funcname);
      bson_free (as_string);
   }

   bson_destroy (pattern);
   bson_free (json_pattern_formatted);
   bson_free (double_quoted);

   return matches;
}


/*--------------------------------------------------------------------------
 *
 * match_bson --
 *
 *       Does "doc" match "pattern"?
 *
 *       See match_bson_with_ctx for details.
 *
 * Returns:
 *       True or false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
match_bson (const bson_t *doc, const bson_t *pattern, bool is_command)
{
   match_ctx_t ctx = {{0}};

   ctx.strict_numeric_types = true;
   ctx.is_command = is_command;

   return match_bson_with_ctx (doc, pattern, &ctx);
}


MONGOC_PRINTF_FORMAT (2, 3)
void
match_err (match_ctx_t *ctx, const char *fmt, ...)
{
   va_list args;
   char *formatted;

   BSON_ASSERT (ctx);

   va_start (args, fmt);
   formatted = bson_strdupv_printf (fmt, args);
   va_end (args);

   bson_snprintf (ctx->errmsg, sizeof ctx->errmsg, "%s: %s", ctx->path, formatted);

   bson_free (formatted);
}


/* When matching two docs, and preparing to recurse to match two subdocs with
 * the given key, derive context for matching them from the current context. */
static void
derive (match_ctx_t *ctx, match_ctx_t *derived, const char *key)
{
   BSON_ASSERT (ctx);
   BSON_ASSERT (derived);
   BSON_ASSERT (key);

   derived->strict_numeric_types = ctx->strict_numeric_types;

   if (strlen (ctx->path) > 0) {
      bson_snprintf (derived->path, sizeof derived->path, "%s.%s", ctx->path, key);
   } else {
      bson_snprintf (derived->path, sizeof derived->path, "%s", key);
   }
   derived->retain_dots_in_keys = ctx->retain_dots_in_keys;
   derived->allow_placeholders = ctx->allow_placeholders;
   derived->visitor_ctx = ctx->visitor_ctx;
   derived->visitor_fn = ctx->visitor_fn;
   derived->is_command = false;
   derived->errmsg[0] = 0;
}


/*--------------------------------------------------------------------------
 *
 * match_bson_with_ctx --
 *
 *       Does "doc" match "pattern"?
 *
 *       mongoc_matcher_t prohibits $-prefixed keys, which is something
 *       we need to test in e.g. test_mongoc_client_read_prefs, so this
 *       does *not* use mongoc_matcher_t. Instead, "doc" matches "pattern"
 *       if its key-value pairs are a simple superset of pattern's. Order
 *       matters.
 *
 *       The only special pattern syntaxes are:
 *         "field": {"$exists": true/false}
 *         "field": {"$empty": true/false}
 *         "field": {"$$type": "type string"}
 *
 *       The first key matches case-insensitively if ctx->is_command.
 *
 *       An optional match visitor (match_visitor_fn and match_visitor_ctx)
 *       can be set in ctx to provide custom matching behavior.
 *
 *       A NULL doc or NULL pattern means "{}".
 *
 * Returns:
 *       True or false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
match_bson_with_ctx (const bson_t *doc, const bson_t *pattern, match_ctx_t *ctx)
{
   bson_iter_t pattern_iter;
   const char *key;
   const bson_value_t *value;
   bool is_first = true;
   bool is_exists_operator;
   bool is_empty_operator;
   bool is_type_operator;
   bool exists;
   bool empty = false;
   bson_type_t bson_type = (bson_type_t) 0;
   bool found;
   bson_iter_t doc_iter;
   bson_value_t doc_value;
   match_ctx_t derived;

   if (bson_empty0 (pattern)) {
      /* matches anything */
      return true;
   }

   BSON_ASSERT (bson_iter_init (&pattern_iter, pattern));

   while (bson_iter_next (&pattern_iter)) {
      key = bson_iter_key (&pattern_iter);
      value = bson_iter_value (&pattern_iter);

      found = find (&doc_iter, doc, key, ctx->is_command, is_first, ctx->retain_dots_in_keys);
      if (found) {
         bson_value_copy (bson_iter_value (&doc_iter), &doc_value);
      }

      /* is value {"$exists": true} or {"$exists": false} ? */
      is_exists_operator = get_exists_operator (value, &exists);

      /* is value {"$empty": true} or {"$empty": false} ? */
      is_empty_operator = get_empty_operator (value, &empty);

      /* is value {"$$type": "string" } ? */
      is_type_operator = get_type_operator (value, &bson_type);

      derive (ctx, &derived, key);

      if (ctx->visitor_fn) {
         match_action_t action = ctx->visitor_fn (ctx, &pattern_iter, found ? &doc_iter : NULL);
         if (action == MATCH_ACTION_ABORT) {
            // Visitor encountered a match error.
            goto fail;
         } else if (action == MATCH_ACTION_SKIP) {
            // Visitor handled match of this field. Skip any additional matching
            // of this field.
            goto next;
         }
         ASSERT (action == MATCH_ACTION_CONTINUE);
      }

      if (value->value_type == BSON_TYPE_NULL && found) {
         /* pattern has "key": null, and "key" is in doc */
         if (doc_value.value_type != BSON_TYPE_NULL) {
            match_err (&derived, "%s should be null or absent", key);
            goto fail;
         }
      } else if (is_exists_operator) {
         if (exists != found) {
            match_err (&derived, "%s found", found ? "" : "not");
            goto fail;
         }
      } else if (!found) {
         match_err (&derived, "not found");
         goto fail;
      } else if (is_empty_operator) {
         if (empty != is_empty_doc_or_array (&doc_value)) {
            match_err (&derived, "%s found", empty ? "" : " not");
            goto fail;
         }
      } else if (is_type_operator) {
         if (doc_value.value_type != bson_type) {
            match_err (&derived, "incorrect type");
            goto fail;
         }
      } else if (!match_bson_value (&doc_value, value, &derived)) {
         goto fail;
      }

   next:
      is_first = false;
      if (found) {
         bson_value_destroy (&doc_value);
      }
   }

   return true;

fail:
   if (found) {
      bson_value_destroy (&doc_value);
   }

   if (strlen (derived.errmsg) > 0) {
      strcpy (ctx->errmsg, derived.errmsg);
   }

   return false;
}


/*--------------------------------------------------------------------------
 *
 * find --
 *
 *       Find the value for a key.
 *
 * Returns:
 *       Whether the key was found.
 *
 * Side effects:
 *       Copies the found value into "iter_out".
 *
 *--------------------------------------------------------------------------
 */

static bool
find (
   bson_iter_t *iter_out, const bson_t *doc, const char *key, bool is_command, bool is_first, bool retain_dots_in_keys)
{
   bson_iter_t iter;
   bson_iter_t descendent;

   bson_iter_init (&iter, doc);

   if (!retain_dots_in_keys && strchr (key, '.')) {
      if (!bson_iter_find_descendant (&iter, key, &descendent)) {
         return false;
      }

      memcpy (iter_out, &descendent, sizeof (bson_iter_t));
      return true;
   } else if (is_command && is_first) {
      if (!bson_iter_find_case (&iter, key)) {
         return false;
      }
   } else if (!bson_iter_find (&iter, key)) {
      return false;
   }

   memcpy (iter_out, &iter, sizeof (bson_iter_t));
   return true;
}


bool
bson_init_from_value (bson_t *b, const bson_value_t *v)
{
   BSON_ASSERT (v->value_type == BSON_TYPE_ARRAY || v->value_type == BSON_TYPE_DOCUMENT);

   return bson_init_static (b, v->value.v_doc.data, v->value.v_doc.data_len);
}


static bool
_is_operator (const char *op_name, const bson_value_t *value, bool *op_val)
{
   bson_t bson;
   bson_iter_t iter;

   if (value->value_type == BSON_TYPE_DOCUMENT && bson_init_from_value (&bson, value) &&
       bson_iter_init_find (&iter, &bson, op_name)) {
      *op_val = bson_iter_as_bool (&iter);
      return true;
   }

   return false;
}


/*--------------------------------------------------------------------------
 *
 * get_exists_operator --
 *
 *       Is value a subdocument like {"$exists": bool}?
 *
 * Returns:
 *       True if the value is a subdocument with the first key "$exists",
 *       or if value is BSON null.
 *
 * Side effects:
 *       If the function returns true, *exists is set to true or false,
 *       the value of the bool.
 *
 *--------------------------------------------------------------------------
 */

static bool
get_exists_operator (const bson_value_t *value, bool *exists)
{
   if (_is_operator ("$exists", value, exists)) {
      return true;
   }

   if (value->value_type == BSON_TYPE_NULL) {
      *exists = false;
      return true;
   }

   return false;
}


/*--------------------------------------------------------------------------
 *
 * get_empty_operator --
 *
 *       Is value a subdocument like {"$empty": bool}?
 *
 * Returns:
 *       True if the value is a subdocument with the first key "$empty".
 *
 * Side effects:
 *       If the function returns true, *empty is set to true or false,
 *       the value of the bool.
 *
 *--------------------------------------------------------------------------
 */

bool
get_empty_operator (const bson_value_t *value, bool *empty)
{
   return _is_operator ("$empty", value, empty);
}


/*--------------------------------------------------------------------------
 *
 * get_type_operator --
 *
 *       Is value a subdocument like {"$$type": "BSON type string"}?
 *
 * Returns:
 *       True if the value is a subdocument with the first key "$$type",
 *       and sets the @bson_type.
 *
 * Side effects:
 *       If the function returns true, *@bson_type is set.
 *
 *--------------------------------------------------------------------------
 */

static bool
get_type_operator (const bson_value_t *value, bson_type_t *out)
{
   bson_t bson;
   bson_iter_t iter;
   const char *value_string;

   /* See list of aliases on this page:
    * https://www.mongodb.com/docs/manual/reference/bson-types/ */
   if (value->value_type == BSON_TYPE_DOCUMENT && bson_init_from_value (&bson, value) &&
       bson_iter_init_find (&iter, &bson, "$$type")) {
      value_string = bson_iter_utf8 (&iter, NULL);
      if (0 == strcasecmp ("double", value_string)) {
         *out = BSON_TYPE_DOUBLE;
      } else if (0 == strcasecmp ("string", value_string)) {
         *out = BSON_TYPE_UTF8;
      } else if (0 == strcasecmp ("object", value_string)) {
         *out = BSON_TYPE_DOCUMENT;
      } else if (0 == strcasecmp ("array", value_string)) {
         *out = BSON_TYPE_ARRAY;
      } else if (0 == strcasecmp ("binData", value_string)) {
         *out = BSON_TYPE_BINARY;
      } else if (0 == strcasecmp ("undefined", value_string)) {
         *out = BSON_TYPE_UNDEFINED;
      } else if (0 == strcasecmp ("objectId", value_string)) {
         *out = BSON_TYPE_OID;
      } else if (0 == strcasecmp ("bool", value_string)) {
         *out = BSON_TYPE_BOOL;
      } else if (0 == strcasecmp ("date", value_string)) {
         *out = BSON_TYPE_DATE_TIME;
      } else if (0 == strcasecmp ("null", value_string)) {
         *out = BSON_TYPE_NULL;
      } else if (0 == strcasecmp ("regex", value_string)) {
         *out = BSON_TYPE_REGEX;
      } else if (0 == strcasecmp ("dbPointer", value_string)) {
         *out = BSON_TYPE_DBPOINTER;
      } else if (0 == strcasecmp ("javascript", value_string)) {
         *out = BSON_TYPE_CODE;
      } else if (0 == strcasecmp ("symbol", value_string)) {
         *out = BSON_TYPE_SYMBOL;
      } else if (0 == strcasecmp ("javascriptWithScope", value_string)) {
         *out = BSON_TYPE_CODEWSCOPE;
      } else if (0 == strcasecmp ("int", value_string)) {
         *out = BSON_TYPE_INT32;
      } else if (0 == strcasecmp ("timestamp", value_string)) {
         *out = BSON_TYPE_TIMESTAMP;
      } else if (0 == strcasecmp ("long", value_string)) {
         *out = BSON_TYPE_INT64;
      } else if (0 == strcasecmp ("decimal", value_string)) {
         *out = BSON_TYPE_DECIMAL128;
      } else if (0 == strcasecmp ("minKey", value_string)) {
         *out = BSON_TYPE_MINKEY;
      } else if (0 == strcasecmp ("maxKey", value_string)) {
         *out = BSON_TYPE_MAXKEY;
      } else {
         test_error ("unrecognized $$type value: %s", value_string);
      }
      return true;
   }

   return false;
}


/*--------------------------------------------------------------------------
 *
 * is_empty_doc_or_array --
 *
 *       Is value the subdocument {} or the array []?
 *
 *--------------------------------------------------------------------------
 */

static bool
is_empty_doc_or_array (const bson_value_t *value)
{
   bson_t doc;

   if (!(value->value_type == BSON_TYPE_ARRAY || value->value_type == BSON_TYPE_DOCUMENT)) {
      return false;
   }
   BSON_ASSERT (bson_init_static (&doc, value->value.v_doc.data, value->value.v_doc.data_len));

   return bson_count_keys (&doc) == 0;
}


static bool
match_bson_arrays (const bson_t *array, const bson_t *pattern, match_ctx_t *ctx)
{
   uint32_t array_count;
   uint32_t pattern_count;
   bson_iter_t array_iter;
   bson_iter_t pattern_iter;
   const bson_value_t *array_value;
   const bson_value_t *pattern_value;
   match_ctx_t derived;

   array_count = bson_count_keys (array);
   pattern_count = bson_count_keys (pattern);

   if (array_count != pattern_count) {
      match_err (ctx, "expected %" PRIu32 " keys, not %" PRIu32, pattern_count, array_count);
      return false;
   }

   BSON_ASSERT (bson_iter_init (&array_iter, array));
   BSON_ASSERT (bson_iter_init (&pattern_iter, pattern));

   while (bson_iter_next (&array_iter)) {
      BSON_ASSERT (bson_iter_next (&pattern_iter));
      array_value = bson_iter_value (&array_iter);
      pattern_value = bson_iter_value (&pattern_iter);

      derive (ctx, &derived, bson_iter_key (&array_iter));

      if (ctx && ctx->visitor_fn) {
         match_action_t action = ctx->visitor_fn (ctx, &pattern_iter, &array_iter);
         if (action == MATCH_ACTION_ABORT) {
            // Visitor encountered a match error.
            return false;
         } else if (action == MATCH_ACTION_SKIP) {
            // Visitor handled match of this field. Skip any additional matching
            // of this field.
            continue;
         }
         ASSERT (action == MATCH_ACTION_CONTINUE);
      }

      if (!match_bson_value (array_value, pattern_value, &derived)) {
         return false;
      }
   }

   return true;
}


static bool
is_number_type (bson_type_t t)
{
   if (t == BSON_TYPE_DOUBLE || t == BSON_TYPE_INT32 || t == BSON_TYPE_INT64) {
      return true;
   }

   return false;
}


int64_t
bson_value_as_int64 (const bson_value_t *value)
{
   if (value->value_type == BSON_TYPE_DOUBLE) {
      return (int64_t) value->value.v_double;
   } else if (value->value_type == BSON_TYPE_INT32) {
      return (int64_t) value->value.v_int32;
   } else if (value->value_type == BSON_TYPE_INT64) {
      return value->value.v_int64;
   } else {
      test_error ("bson_value_as_int64 called on value of type %d", value->value_type);
   }
}


bool
match_bson_value (const bson_value_t *doc, const bson_value_t *pattern, match_ctx_t *ctx)
{
   bson_t subdoc;
   bson_t pattern_subdoc;
   int64_t doc_int64;
   int64_t pattern_int64;
   bool ret;

   if (ctx && ctx->allow_placeholders) {
      /* The change streams spec tests use the value 42 as a placeholder. */
      bool is_placeholder = false;
      if (is_number_type (pattern->value_type) && bson_value_as_int64 (pattern) == 42) {
         is_placeholder = true;
      }
      if (pattern->value_type == BSON_TYPE_UTF8 && !strcmp (pattern->value.v_utf8.str, "42")) {
         is_placeholder = true;
      }
      if (is_placeholder) {
         return true;
      }
   }

   if (is_number_type (doc->value_type) && is_number_type (pattern->value_type) && ctx && !ctx->strict_numeric_types) {
      doc_int64 = bson_value_as_int64 (doc);
      pattern_int64 = bson_value_as_int64 (pattern);

      if (doc_int64 != pattern_int64) {
         match_err (ctx, "expected %" PRId64 ", got %" PRId64, pattern_int64, doc_int64);
         return false;
      }

      return true;
   }

   if (doc->value_type != pattern->value_type) {
      match_err (ctx,
                 "expected type %s, got %s",
                 _mongoc_bson_type_to_str (pattern->value_type),
                 _mongoc_bson_type_to_str (doc->value_type));
      return false;
   }

   switch (doc->value_type) {
   case BSON_TYPE_ARRAY:
   case BSON_TYPE_DOCUMENT:

      if (!bson_init_from_value (&subdoc, doc)) {
         return false;
      }

      if (!bson_init_from_value (&pattern_subdoc, pattern)) {
         bson_destroy (&subdoc);
         return false;
      }

      if (doc->value_type == BSON_TYPE_ARRAY) {
         ret = match_bson_arrays (&subdoc, &pattern_subdoc, ctx);
      } else {
         ret = match_bson_with_ctx (&subdoc, &pattern_subdoc, ctx);
      }

      bson_destroy (&subdoc);
      bson_destroy (&pattern_subdoc);

      return ret;

   case BSON_TYPE_BINARY:
      ret = doc->value.v_binary.data_len == pattern->value.v_binary.data_len &&
            !memcmp (doc->value.v_binary.data, pattern->value.v_binary.data, doc->value.v_binary.data_len);
      break;

   case BSON_TYPE_BOOL:
      ret = doc->value.v_bool == pattern->value.v_bool;

      if (!ret) {
         match_err (ctx, "expected %d, got %d", pattern->value.v_bool, doc->value.v_bool);
      }

      return ret;

   case BSON_TYPE_CODE:
      ret = doc->value.v_code.code_len == pattern->value.v_code.code_len &&
            !memcmp (doc->value.v_code.code, pattern->value.v_code.code, doc->value.v_code.code_len);

      break;

   case BSON_TYPE_CODEWSCOPE:
      ret =
         doc->value.v_codewscope.code_len == pattern->value.v_codewscope.code_len &&
         !memcmp (doc->value.v_codewscope.code, pattern->value.v_codewscope.code, doc->value.v_codewscope.code_len) &&
         doc->value.v_codewscope.scope_len == pattern->value.v_codewscope.scope_len &&
         !memcmp (doc->value.v_codewscope.scope_data,
                  pattern->value.v_codewscope.scope_data,
                  doc->value.v_codewscope.scope_len);

      break;

   case BSON_TYPE_DATE_TIME:
      ret = doc->value.v_datetime == pattern->value.v_datetime;

      if (!ret) {
         match_err (ctx, "expected %" PRId64 ", got %" PRId64, pattern->value.v_datetime, doc->value.v_datetime);
      }

      return ret;

   case BSON_TYPE_DOUBLE:
      ret = doc->value.v_double == pattern->value.v_double;

      if (!ret) {
         match_err (ctx, "expected %f, got %f", pattern->value.v_double, doc->value.v_double);
      }

      return ret;

   case BSON_TYPE_INT32:
      ret = doc->value.v_int32 == pattern->value.v_int32;

      if (!ret) {
         match_err (ctx, "expected %" PRId32 ", got %" PRId32, pattern->value.v_int32, doc->value.v_int32);
      }

      return ret;

   case BSON_TYPE_INT64:
      ret = doc->value.v_int64 == pattern->value.v_int64;

      if (!ret) {
         match_err (ctx, "expected %" PRId64 ", got %" PRId64, pattern->value.v_int64, doc->value.v_int64);
      }

      return ret;

   case BSON_TYPE_OID:
      ret = bson_oid_equal (&doc->value.v_oid, &pattern->value.v_oid);
      break;

   case BSON_TYPE_REGEX:
      ret = !strcmp (doc->value.v_regex.regex, pattern->value.v_regex.regex) &&
            !strcmp (doc->value.v_regex.options, pattern->value.v_regex.options);

      break;

   case BSON_TYPE_SYMBOL:
      ret = doc->value.v_symbol.len == pattern->value.v_symbol.len &&
            !strncmp (doc->value.v_symbol.symbol, pattern->value.v_symbol.symbol, doc->value.v_symbol.len);

      break;

   case BSON_TYPE_TIMESTAMP:
      ret = doc->value.v_timestamp.timestamp == pattern->value.v_timestamp.timestamp &&
            doc->value.v_timestamp.increment == pattern->value.v_timestamp.increment;

      break;

   case BSON_TYPE_UTF8:
      ret = doc->value.v_utf8.len == pattern->value.v_utf8.len &&
            !strncmp (doc->value.v_utf8.str, pattern->value.v_utf8.str, doc->value.v_utf8.len);

      if (!ret) {
         match_err (ctx, "expected \"%s\", got \"%s\"", pattern->value.v_utf8.str, doc->value.v_utf8.str);
      }

      return ret;


   /* these are empty types, if "a" and "b" are the same type they're equal */
   case BSON_TYPE_EOD:
   case BSON_TYPE_MAXKEY:
   case BSON_TYPE_MINKEY:
   case BSON_TYPE_NULL:
   case BSON_TYPE_UNDEFINED:
      return true;

   case BSON_TYPE_DBPOINTER:
      ret = (0 == strcmp (doc->value.v_dbpointer.collection, pattern->value.v_dbpointer.collection) &&
             bson_oid_equal (&doc->value.v_dbpointer.oid, &pattern->value.v_dbpointer.oid));
      break;

   case BSON_TYPE_DECIMAL128:
      ret = (doc->value.v_decimal128.low == pattern->value.v_decimal128.low &&
             doc->value.v_decimal128.high == pattern->value.v_decimal128.high);
      if (!ret) {
         match_err (ctx,
                    "Decimal128 is not an exact binary match (though "
                    "numeric values may be equal)");
      }
      break;
   default:
      test_error ("unexpected value type %d: %s", doc->value_type, _mongoc_bson_type_to_str (doc->value_type));
   }

   if (!ret) {
      match_err (ctx, "%s values mismatch", _mongoc_bson_type_to_str (pattern->value_type));
   }

   return ret;
}

bool
mongoc_write_concern_append_bad (mongoc_write_concern_t *write_concern, bson_t *command)
{
   mongoc_write_concern_t *wc = mongoc_write_concern_copy (write_concern);

   if (!bson_append_document (command, "writeConcern", 12, _mongoc_write_concern_get_bson (wc))) {
      MONGOC_ERROR ("Could not append writeConcern to command.");
      mongoc_write_concern_destroy (wc);
      return false;
   }

   mongoc_write_concern_destroy (wc);
   return true;
}


static void
init_huge_string (mongoc_client_t *client)
{
   int32_t max_bson_size;

   ASSERT (client);

   test_conveniences_init ();

   if (!gHugeString) {
      max_bson_size = mongoc_cluster_get_max_bson_obj_size (&client->cluster);
      BSON_ASSERT (max_bson_size > 0);
      gHugeStringLength = (size_t) max_bson_size - 36;
      gHugeString = (char *) bson_malloc (gHugeStringLength + 1);
      BSON_ASSERT (gHugeString);
      memset (gHugeString, 'a', gHugeStringLength);
      gHugeString[gHugeStringLength] = '\0';
   }
}


const char *
huge_string (mongoc_client_t *client)
{
   ASSERT (client);
   init_huge_string (client);
   return gHugeString;
}


size_t
huge_string_length (mongoc_client_t *client)
{
   ASSERT (client);
   init_huge_string (client);
   return gHugeStringLength;
}


static void
init_four_mb_string (void)
{
   test_conveniences_init ();

   if (!gFourMBString) {
      gFourMBString = (char *) bson_malloc (FOUR_MB + 1);
      BSON_ASSERT (gFourMBString);
      memset (gFourMBString, 'a', FOUR_MB);
      gFourMBString[FOUR_MB] = '\0';
   }
}


const char *
four_mb_string (void)
{
   init_four_mb_string ();
   return gFourMBString;
}


static bool
find_key (void *current, void *key)
{
   return !strcmp ((const char *) current, (const char *) key);
}


static void
key_dtor (void *item, void *ctx)
{
   BSON_UNUSED (item);
   BSON_UNUSED (ctx);
   /* mongoc_set_t requires a dtor, there's nothing to destroy */
}


void
assert_no_duplicate_keys (const bson_t *doc)
{
   mongoc_set_t *keys;
   bson_iter_t iter;
   bson_t subdoc;

   keys = mongoc_set_new (8, key_dtor, NULL);
   BSON_ASSERT (bson_iter_init (&iter, doc));

   while (bson_iter_next (&iter)) {
      if (mongoc_set_find_item (keys, find_key, (void *) bson_iter_key (&iter))) {
         test_error ("Duplicate key \"%s\" in document:\n%s", bson_iter_key (&iter), bson_as_json (doc, NULL));
      }

      mongoc_set_add (keys, 0 /* index */, (void *) bson_iter_key (&iter));

      if (BSON_ITER_HOLDS_DOCUMENT (&iter) || BSON_ITER_HOLDS_ARRAY (&iter)) {
         bson_iter_bson (&iter, &subdoc);
         assert_no_duplicate_keys (&subdoc);
      }
   }

   mongoc_set_destroy (keys);
}


void
match_in_array (const bson_t *doc, const bson_t *array, match_ctx_t *ctx)
{
   bson_iter_t array_iter;
   bool found = false;

   BSON_ASSERT (bson_iter_init (&array_iter, array));

   while (bson_iter_next (&array_iter)) {
      bson_t array_elem;

      ASSERT (BSON_ITER_HOLDS_DOCUMENT (&array_iter));
      bson_iter_bson (&array_iter, &array_elem);

      if (match_bson_with_ctx (&array_elem, doc, ctx)) {
         found = true;
      }

      bson_destroy (&array_elem);
   }
   if (!found) {
      test_error ("could not match: %s\n\n"
                  "in array:\n%s\n\n",
                  bson_as_canonical_extended_json (doc, NULL),
                  bson_as_canonical_extended_json (array, NULL));
   }
}

bson_t *
bson_with_all_types (void)
{
   bson_t *bson = tmp_bson ("{}");
   bson_oid_t oid;
   bson_decimal128_t dec;

   BSON_ASSERT (bson_decimal128_from_string ("1.23456789", &dec));
   bson_oid_init_from_string (&oid, "000000000000000000000000");
   BSON_ASSERT (BSON_APPEND_DOUBLE (bson, "double", 1.0));
   BSON_ASSERT (BSON_APPEND_UTF8 (bson, "string", "string_example"));
   BSON_ASSERT (BSON_APPEND_DOCUMENT (bson, "document", tmp_bson ("{'x': 'y'}")));
   BSON_ASSERT (BSON_APPEND_ARRAY (bson, "document", tmp_bson ("{'0': 'x'}")));
   BSON_ASSERT (BSON_APPEND_BINARY (bson, "binary", BSON_SUBTYPE_BINARY, (uint8_t *) "data", 4));
   BSON_ASSERT (BSON_APPEND_UNDEFINED (bson, "undefined"));
   BSON_ASSERT (BSON_APPEND_OID (bson, "oid", &oid));
   BSON_ASSERT (BSON_APPEND_BOOL (bson, "bool", true));
   BSON_ASSERT (BSON_APPEND_DATE_TIME (bson, "datetime", 123));
   BSON_ASSERT (BSON_APPEND_NULL (bson, "null"));
   BSON_ASSERT (BSON_APPEND_REGEX (bson, "regex", "a+", NULL));
   BSON_ASSERT (BSON_APPEND_DBPOINTER (bson, "dbpointer", "collection", &oid));
   BSON_ASSERT (BSON_APPEND_CODE (bson, "code", "var x = 1;"));
   BSON_ASSERT (BSON_APPEND_SYMBOL (bson, "symbol", "symbol_example"));
   BSON_ASSERT (BSON_APPEND_CODE (bson, "code", "var x = 1;"));
   BSON_ASSERT (BSON_APPEND_CODE_WITH_SCOPE (bson, "code_w_scope", "var x = 1;", tmp_bson ("{}")));
   BSON_ASSERT (BSON_APPEND_INT32 (bson, "int32", 1));
   BSON_ASSERT (BSON_APPEND_TIMESTAMP (bson, "timestamp", 2, 3));
   BSON_ASSERT (BSON_APPEND_INT64 (bson, "int64", 4));
   BSON_ASSERT (BSON_APPEND_DECIMAL128 (bson, "decimal128", &dec));
   BSON_ASSERT (BSON_APPEND_MINKEY (bson, "minkey"));
   BSON_ASSERT (BSON_APPEND_MAXKEY (bson, "maxkey"));
   /* and an empty key, as it so often is an edge case. */
   BSON_ASSERT (BSON_APPEND_INT32 (bson, "", -1));
   return bson;
}

const char *
json_with_all_types (void)
{
   const char *json = "{\n"
                      "    \"double\": {\n"
                      "        \"$numberDouble\": \"1.0\"\n"
                      "    },\n"
                      "    \"string\": \"string_example\",\n"
                      "    \"document\": {\n"
                      "        \"x\": \"y\"\n"
                      "    },\n"
                      "    \"document\": [\"x\"],\n"
                      "    \"binary\": {\n"
                      "        \"$binary\": {\n"
                      "            \"base64\": \"ZGF0YQ==\",\n"
                      "            \"subType\": \"00\"\n"
                      "        }\n"
                      "    },\n"
                      "    \"undefined\": {\n"
                      "        \"$undefined\": true\n"
                      "    },\n"
                      "    \"oid\": {\n"
                      "        \"$oid\": \"000000000000000000000000\"\n"
                      "    },\n"
                      "    \"bool\": true,\n"
                      "    \"datetime\": {\n"
                      "        \"$date\": {\n"
                      "            \"$numberLong\": \"123\"\n"
                      "        }\n"
                      "    },\n"
                      "    \"null\": null,\n"
                      "    \"regex\": {\n"
                      "        \"$regularExpression\": {\n"
                      "            \"pattern\": \"a+\",\n"
                      "            \"options\": \"\"\n"
                      "        }\n"
                      "    },\n"
                      "    \"dbpointer\": {\n"
                      "        \"$dbPointer\": {\n"
                      "            \"$ref\": \"collection\",\n"
                      "            \"$id\": {\n"
                      "                \"$oid\": \"000000000000000000000000\"\n"
                      "            }\n"
                      "        }\n"
                      "    },\n"
                      "    \"code\": {\n"
                      "        \"$code\": \"var x = 1;\"\n"
                      "    },\n"
                      "    \"symbol\": {\n"
                      "        \"$symbol\": \"symbol_example\"\n"
                      "    },\n"
                      "    \"code\": {\n"
                      "        \"$code\": \"var x = 1;\"\n"
                      "    },\n"
                      "    \"code_w_scope\": {\n"
                      "        \"$code\": \"var x = 1;\",\n"
                      "        \"$scope\": {}\n"
                      "    },\n"
                      "    \"int32\": {\n"
                      "        \"$numberInt\": \"1\"\n"
                      "    },\n"
                      "    \"timestamp\": {\n"
                      "        \"$timestamp\": {\n"
                      "            \"t\": 2,\n"
                      "            \"i\": 3\n"
                      "        }\n"
                      "    },\n"
                      "    \"int64\": {\n"
                      "        \"$numberLong\": \"4\"\n"
                      "    },\n"
                      "    \"decimal128\": {\n"
                      "        \"$numberDecimal\": \"1.23456789\"\n"
                      "    },\n"
                      "    \"minkey\": {\n"
                      "        \"$minKey\": 1\n"
                      "    },\n"
                      "    \"maxkey\": {\n"
                      "        \"$maxKey\": 1\n"
                      "    },\n"
                      "    \"\": {\n"
                      "        \"$numberInt\": \"-1\"\n"
                      "    }\n"
                      "}";
   return json;
}

void
assert_wc_oob_error (bson_error_t *error)
{
   if (test_framework_get_server_version () >= test_framework_str_to_version ("4.3.3")) {
      /* Error reporting changed in SERVER-45584 */
      ASSERT_ERROR_CONTAINS (
         (*error), MONGOC_ERROR_SERVER, 9, "w has to be a non-negative number and not greater than 50");
   } else {
      if (test_framework_is_replset ()) { /* replset */
         ASSERT_ERROR_CONTAINS ((*error), MONGOC_ERROR_WRITE_CONCERN, 100, "Write Concern error:");
      } else { /* standalone */
         ASSERT_CMPINT (error->domain, ==, MONGOC_ERROR_SERVER);
         ASSERT_CMPINT (error->code, ==, 2);
      }
   }
}

void
semver_parse (const char *str, semver_t *out)
{
   char *dot;

   memset (out, 0, sizeof (semver_t));
   out->major = (int) bson_ascii_strtoll (str, &dot, 10);
   if (*dot == '.') {
      dot++;
   } else {
      return;
   }
   out->has_minor = true;
   out->minor = (int) bson_ascii_strtoll (dot, &dot, 10);

   if (*dot == '.') {
      dot++;
   } else {
      return;
   }
   out->has_patch = true;
   out->patch = (int) bson_ascii_strtoll (dot, &dot, 10);
}

void
server_semver (mongoc_client_t *client, semver_t *out)
{
   bson_t reply;
   bson_error_t error;
   const char *server_version_str;

   ASSERT (client);

   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "admin", tmp_bson ("{'buildinfo': 1}"), NULL, &reply, &error),
                    error);

   server_version_str = bson_lookup_utf8 (&reply, "version");
   semver_parse (server_version_str, out);

   bson_destroy (&reply);
}

int
semver_cmp (semver_t *a, semver_t *b)
{
   if (a->major < b->major) {
      return -1;
   } else if (a->major > b->major) {
      return 1;
   }

   if (a->minor < b->minor) {
      return -1;
   } else if (a->minor > b->minor) {
      return 1;
   }

   if (a->patch < b->patch) {
      return -1;
   } else if (a->patch > b->patch) {
      return 1;
   }

   return 0;
}

int
semver_cmp_str (semver_t *a, const char *str)
{
   semver_t b;

   semver_parse (str, &b);
   return semver_cmp (a, &b);
}

const char *
semver_to_string (semver_t *semver)
{
   return tmp_str ("%d.%d.%d", semver->major, semver->minor, semver->patch);
}
