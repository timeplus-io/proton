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

#ifndef TEST_CONVENIENCES_H
#define TEST_CONVENIENCES_H

#include <bson/bson.h>

#include "mongoc/mongoc.h"
#include "mongoc/mongoc-read-prefs-private.h"
#include "mongoc/mongoc-client-private.h"

/* TODO: split this header up.
 * Move bson_lookup_* functions under bsonutil.
 * Move temporary helpers into a separate header.
 */

/* Initialize global test convenience structures.
 * Safe to call repeatedly, or after calling test_conveniences_cleanup().
 */
void
test_conveniences_init (void);

/* Tear down global test conveniences.
 * Safe to call repeatedly.
 * Called automatically at process exit.
 */
void
test_conveniences_cleanup (void);

/* Return a bson_t representation from a single-quoted JSON string, with
 * possible printf format directives.
 * bson_t is freed automatically at test cleanup.
 * E.g. tmp_bson ("{'key': %d}", 123); */
bson_t *
tmp_bson (const char *json, ...);

/* Return a string, with possible printf format directives. String is
 * automatically freed at test cleanup. */
const char *
tmp_str (const char *fmt, ...);

/* Return a JSON string representation of BSON. String is freed automatically at
 * test cleanup. */
const char *
tmp_json (const bson_t *bson);

void
bson_iter_bson (const bson_iter_t *iter, bson_t *bson);

/* create a bson_t containing all types of values, and an empty key. The
 * returned bson_t does not need to be freed. This corresponds to the same
 * document in json_with_all_types. */
bson_t *
bson_with_all_types (void);

/* returns a json string with all types of values, and an empty key. This
 * corresponds to the same document in bson_with_all_types. */
const char *
json_with_all_types (void);


#ifndef PATH_MAX
#define PATH_MAX 1024
#endif

#ifdef _WIN32
#define realpath(path, expanded) GetFullPathName (path, PATH_MAX, expanded, NULL)
#endif

const char *
bson_lookup_utf8 (const bson_t *b, const char *key);

void
value_init_from_doc (bson_value_t *value, const bson_t *doc);

void
bson_lookup_value (const bson_t *b, const char *key, bson_value_t *value);

bson_t *
bson_lookup_bson (const bson_t *b, const char *key);

void
bson_lookup_doc (const bson_t *b, const char *key, bson_t *doc);

void
bson_lookup_doc_null_ok (const bson_t *b, const char *key, bson_t *doc);

bool
bson_lookup_bool (const bson_t *b, const char *key);

int32_t
bson_lookup_int32 (const bson_t *b, const char *key);

int64_t
bson_lookup_int64 (const bson_t *b, const char *key);

mongoc_read_concern_t *
bson_lookup_read_concern (const bson_t *b, const char *key);

mongoc_write_concern_t *
bson_lookup_write_concern (const bson_t *b, const char *key);

mongoc_read_prefs_t *
bson_lookup_read_prefs (const bson_t *b, const char *key);

void
bson_lookup_database_opts (const bson_t *b, const char *key, mongoc_database_t *database);

void
bson_lookup_collection_opts (const bson_t *b, const char *key, mongoc_collection_t *collection);

mongoc_transaction_opt_t *
bson_lookup_txn_opts (const bson_t *b, const char *key);

mongoc_session_opt_t *
bson_lookup_session_opts (const bson_t *b, const char *key);

mongoc_client_session_t *
bson_lookup_session (const bson_t *b, const char *key, mongoc_client_t *client);

bool
bson_init_from_value (bson_t *b, const bson_value_t *v);

char *
single_quotes_to_double (const char *str);

/* match_action_t determines if default check for a field is overridden. */
typedef enum {
   MATCH_ACTION_SKIP,    /* do not use the default check. */
   MATCH_ACTION_ABORT,   /* an error occurred, stop checking. */
   MATCH_ACTION_CONTINUE /* use the default check. */
} match_action_t;

struct _match_ctx_t;
/* doc_iter may be null if the pattern field is not found. */
typedef match_action_t (*match_visitor_fn) (struct _match_ctx_t *ctx, bson_iter_t *pattern_iter, bson_iter_t *doc_iter);

typedef struct _match_ctx_t {
   char errmsg[1000];
   bool strict_numeric_types;
   /* if retain_dots_in_keys is true, then don't consider a path with dots to
    * indicate recursing into a sub document. */
   bool retain_dots_in_keys;
   /* if allow_placeholders is true, treats 42 and "42" as placeholders. I.e.
    * comparing 42 to anything is ok. */
   bool allow_placeholders;
   /* path is the dot separated breadcrumb trail of keys. */
   char path[1000];
   /* if visitor_fn is not NULL, this is called on for every key in the pattern.
    * The returned match_action_t can override the default match behavior. */
   match_visitor_fn visitor_fn;
   void *visitor_ctx;
   /* if is_command is true, then compare the first key case insensitively. */
   bool is_command;
} match_ctx_t;

#define assert_match_bson(doc, pattern, _is_command)                                \
   if (1) {                                                                         \
      match_ctx_t _ctx = {.strict_numeric_types = true, .is_command = _is_command}; \
                                                                                    \
      if (!match_bson_with_ctx (doc, pattern, &_ctx)) {                             \
         test_error ("Expected: %s\n, Got: %s\n, %s\n",                             \
                     bson_as_canonical_extended_json (pattern, NULL),               \
                     bson_as_canonical_extended_json (doc, NULL),                   \
                     _ctx.errmsg);                                                  \
      }                                                                             \
   } else                                                                           \
      (void) 0

bool
match_bson (const bson_t *doc, const bson_t *pattern, bool is_command);

int64_t
bson_value_as_int64 (const bson_value_t *value);

bool
match_bson_value (const bson_value_t *doc, const bson_value_t *pattern, match_ctx_t *ctx);

bool
match_bson_with_ctx (const bson_t *doc, const bson_t *pattern, match_ctx_t *ctx);

bool
match_json (const bson_t *doc,
            bool is_command,
            const char *filename,
            int lineno,
            const char *funcname,
            const char *json_pattern,
            ...);

#define ASSERT_MATCH(doc, ...)                                                           \
   do {                                                                                  \
      BSON_ASSERT (match_json (doc, false, __FILE__, __LINE__, BSON_FUNC, __VA_ARGS__)); \
   } while (0)

bool
mongoc_write_concern_append_bad (mongoc_write_concern_t *write_concern, bson_t *command);

#define FOUR_MB 1024 * 1024 * 4

const char *
huge_string (mongoc_client_t *client);

size_t
huge_string_length (mongoc_client_t *client);

const char *
four_mb_string (void);

void
assert_no_duplicate_keys (const bson_t *doc);

void
match_in_array (const bson_t *doc, const bson_t *array, match_ctx_t *ctx);

void
match_err (match_ctx_t *ctx, const char *fmt, ...);

void
assert_wc_oob_error (bson_error_t *error);

typedef struct {
   int major;
   int minor;
   bool has_minor;
   int patch;
   bool has_patch;
} semver_t;

void
semver_parse (const char *str, semver_t *out);

void
server_semver (mongoc_client_t *client, semver_t *out);

int
semver_cmp (semver_t *a, semver_t *b);

int
semver_cmp_str (semver_t *a, const char *str);

const char *
semver_to_string (semver_t *str);

/* Iterate over a BSON document or array.
 *
 * Example of iterating and printing an array of BSON documents:
 *
 * bson_iter_t iter;
 * bson_t *arr = my_func();
 *
 * BSON_FOREACH (arr, iter) {
 *    bson_t el;
 *    bson_iter_bson (&iter, &el);
 *    printf ("%d: %s", bson_iter_key (&iter), tmp_json (&el));
 * }
 */
#define BSON_FOREACH(bson, iter_varname) \
   for (bson_iter_init (&(iter_varname), (bson)); bson_iter_next (&(iter_varname));)

#define TEST_ERROR_DOMAIN 123456
#define TEST_ERROR_CODE 654321
#define test_set_error(error, ...) bson_set_error (error, TEST_ERROR_DOMAIN, TEST_ERROR_CODE, __VA_ARGS__)

#endif /* TEST_CONVENIENCES_H */
