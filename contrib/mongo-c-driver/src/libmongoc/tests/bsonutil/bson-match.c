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

#include "bsonutil/bson-match.h"
#include "mongoc-util-private.h" // hex_to_bin
#include "test-conveniences.h"
#include "TestSuite.h"
#include "unified/util.h"
#include "utlist.h"

typedef struct _special_functor_t {
   special_fn fn;
   void *ctx;
   char *keyword;
   struct _special_functor_t *next;
} special_functor_t;

struct _bson_matcher_t {
   special_functor_t *specials;
};

#define MATCH_ERR(format, ...) test_set_error (error, "match error at '%s': " format, path, __VA_ARGS__)

static char *
get_first_key (const bson_t *bson)
{
   bson_iter_t iter;

   bson_iter_init (&iter, bson);
   if (!bson_iter_next (&iter)) {
      return "";
   }

   return (char *) bson_iter_key (&iter);
}

static bool
is_special_match (const bson_t *bson)
{
   char *first_key = get_first_key (bson);
   if (strstr (first_key, "$$") != first_key) {
      return false;
   }
   if (bson_count_keys (bson) != 1) {
      return false;
   }
   return true;
}

/* implements $$placeholder */
static bool
special_placeholder (bson_matcher_t *matcher,
                     const bson_t *assertion,
                     const bson_val_t *actual,
                     void *ctx,
                     const char *path,
                     bson_error_t *error)
{
   BSON_UNUSED (matcher);
   BSON_UNUSED (assertion);
   BSON_UNUSED (actual);
   BSON_UNUSED (ctx);
   BSON_UNUSED (path);
   BSON_UNUSED (error);

   /* Nothing to do (not an operator, just a reserved key value). The meaning
    * and corresponding behavior of $$placeholder depends on context. */
   return true;
}

/* implements $$exists */
static bool
special_exists (bson_matcher_t *matcher,
                const bson_t *assertion,
                const bson_val_t *actual,
                void *ctx,
                const char *path,
                bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;
   bool should_exist;

   BSON_UNUSED (matcher);
   BSON_UNUSED (ctx);

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));

   if (!BSON_ITER_HOLDS_BOOL (&iter)) {
      MATCH_ERR ("%s", "unexpected non-bool $$exists assertion");
   }
   should_exist = bson_iter_bool (&iter);

   if (should_exist && NULL == actual) {
      MATCH_ERR ("%s", "should exist but does not");
      goto done;
   }

   if (!should_exist && NULL != actual) {
      MATCH_ERR ("%s", "should not exist but does");
      goto done;
   }

   ret = true;
done:
   return ret;
}

/* implements $$type */
static bool
special_type (bson_matcher_t *matcher,
              const bson_t *assertion,
              const bson_val_t *actual,
              void *ctx,
              const char *path,
              bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;

   BSON_UNUSED (matcher);
   BSON_UNUSED (ctx);

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));

   if (!actual) {
      MATCH_ERR ("%s", "does not exist but should");
      goto done;
   }

   if (BSON_ITER_HOLDS_UTF8 (&iter)) {
      bson_type_t expected_type = bson_type_from_string (bson_iter_utf8 (&iter, NULL));
      if (expected_type != bson_val_type (actual)) {
         MATCH_ERR ("expected type: %s, got: %s",
                    bson_type_to_string (expected_type),
                    bson_type_to_string (bson_val_type (actual)));
         goto done;
      }
   }

   if (BSON_ITER_HOLDS_ARRAY (&iter)) {
      bson_t arr;
      bson_iter_t arriter;
      bool found = false;

      bson_iter_bson (&iter, &arr);
      BSON_FOREACH (&arr, arriter)
      {
         bson_type_t expected_type;

         if (!BSON_ITER_HOLDS_UTF8 (&arriter)) {
            MATCH_ERR ("%s", "unexpected non-UTF8 $$type assertion");
            goto done;
         }

         expected_type = bson_type_from_string (bson_iter_utf8 (&arriter, NULL));
         if (expected_type == bson_val_type (actual)) {
            found = true;
            break;
         }
      }
      if (!found) {
         MATCH_ERR ("expected one of type: %s, got %s", tmp_json (&arr), bson_type_to_string (bson_val_type (actual)));
         goto done;
      }
   }

   ret = true;
done:
   return ret;
}

/* implements $$unsetOrMatches */
static bool
special_unset_or_matches (bson_matcher_t *matcher,
                          const bson_t *assertion,
                          const bson_val_t *actual,
                          void *ctx,
                          const char *path,
                          bson_error_t *error)
{
   bool ret = false;
   bson_iter_t iter;
   bson_val_t *expected = NULL;

   BSON_UNUSED (ctx);

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));
   expected = bson_val_from_iter (&iter);

   if (actual == NULL) {
      ret = true;
      goto done;
   }

   if (!bson_matcher_match (matcher, expected, actual, path, false, error)) {
      goto done;
   }

   ret = true;
done:
   bson_val_destroy (expected);
   return ret;
}

/* implements $$matchesHexBytes */
static bool
special_matches_hex_bytes (bson_matcher_t *matcher,
                           const bson_t *assertion,
                           const bson_val_t *actual,
                           void *ctx,
                           const char *path,
                           bson_error_t *error)
{
   bool ret = false;
   uint8_t *expected_bytes;
   uint32_t expected_bytes_len;
   const uint8_t *actual_bytes;
   uint32_t actual_bytes_len;
   char *expected_bytes_string = NULL;
   char *actual_bytes_string = NULL;
   bson_iter_t iter;

   BSON_UNUSED (matcher);
   BSON_UNUSED (ctx);

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));

   if (!actual) {
      MATCH_ERR ("%s", "does not exist but should");
      goto done;
   }

   if (!BSON_ITER_HOLDS_UTF8 (&iter)) {
      MATCH_ERR ("%s", "$$matchesHexBytes does not contain utf8");
      goto done;
   }

   if (bson_val_type (actual) != BSON_TYPE_BINARY) {
      MATCH_ERR ("%s", "value does not contain binary");
      goto done;
   }

   expected_bytes = hex_to_bin (bson_iter_utf8 (&iter, NULL), &expected_bytes_len);
   actual_bytes = bson_val_to_binary (actual, &actual_bytes_len);
   expected_bytes_string = bin_to_hex (expected_bytes, expected_bytes_len);
   actual_bytes_string = bin_to_hex (actual_bytes, actual_bytes_len);

   if (expected_bytes_len != actual_bytes_len) {
      MATCH_ERR ("expected %" PRIu32 " (%s) but got %" PRIu32 " (%s) bytes",
                 expected_bytes_len,
                 expected_bytes_string,
                 actual_bytes_len,
                 actual_bytes_string);
      bson_free (expected_bytes);
      bson_free (expected_bytes_string);
      bson_free (actual_bytes_string);
      goto done;
   }

   if (0 != memcmp (expected_bytes, actual_bytes, expected_bytes_len)) {
      MATCH_ERR ("expected %s, but got %s", expected_bytes_string, actual_bytes_string);
      bson_free (expected_bytes);
      bson_free (expected_bytes_string);
      bson_free (actual_bytes_string);
      goto done;
   }

   bson_free (expected_bytes);
   bson_free (expected_bytes_string);
   bson_free (actual_bytes_string);

   ret = true;
   goto done;
done:
   return ret;
}

static bool
evaluate_special (
   bson_matcher_t *matcher, const bson_t *assertion, const bson_val_t *actual, const char *path, bson_error_t *error)
{
   bson_iter_t iter;
   const char *assertion_key;
   special_functor_t *special_iter;

   bson_iter_init (&iter, assertion);
   BSON_ASSERT (bson_iter_next (&iter));
   assertion_key = bson_iter_key (&iter);

   LL_FOREACH (matcher->specials, special_iter)
   {
      if (0 == strcmp (assertion_key, special_iter->keyword)) {
         return special_iter->fn (matcher, assertion, actual, special_iter->ctx, path, error);
      }
   }

   MATCH_ERR ("unrecognized special operator: %s", assertion_key);
   return false;
}


bson_matcher_t *
bson_matcher_new (void)
{
   bson_matcher_t *matcher = bson_malloc0 (sizeof (bson_matcher_t));
   /* Add default special functions. */
   bson_matcher_add_special (matcher, "$$placeholder", special_placeholder, NULL);
   bson_matcher_add_special (matcher, "$$exists", special_exists, NULL);
   bson_matcher_add_special (matcher, "$$type", special_type, NULL);
   bson_matcher_add_special (matcher, "$$unsetOrMatches", special_unset_or_matches, NULL);
   bson_matcher_add_special (matcher, "$$matchesHexBytes", special_matches_hex_bytes, NULL);
   return matcher;
}

/* Add a hook function for matching a special $$ operator */
void
bson_matcher_add_special (bson_matcher_t *matcher, const char *keyword, special_fn special, void *ctx)
{
   special_functor_t *functor;

   if (strstr (keyword, "$$") != keyword) {
      test_error ("unexpected special match keyword: %s. Should start with '$$'", keyword);
   }

   functor = bson_malloc (sizeof (special_functor_t));
   functor->keyword = bson_strdup (keyword);
   functor->fn = special;
   functor->ctx = ctx;
   LL_PREPEND (matcher->specials, functor);
}

void
bson_matcher_destroy (bson_matcher_t *matcher)
{
   special_functor_t *special_iter, *tmp;

   if (!matcher) {
      return;
   }

   LL_FOREACH_SAFE (matcher->specials, special_iter, tmp)
   {
      bson_free (special_iter->keyword);
      bson_free (special_iter);
   }
   bson_free (matcher);
}

bool
bson_matcher_match (bson_matcher_t *matcher,
                    const bson_val_t *expected,
                    const bson_val_t *actual,
                    const char *path,
                    bool array_of_root_docs,
                    bson_error_t *error)
{
   bool ret = false;
   bool is_root = (0 == strcmp (path, ""));

   if (bson_val_type (expected) == BSON_TYPE_DOCUMENT) {
      bson_iter_t expected_iter;
      const bson_t *expected_bson = bson_val_to_document (expected);
      const bson_t *actual_bson = NULL;
      uint32_t expected_keys;
      uint32_t actual_keys;

      /* handle special operators (e.g. $$type) */
      if (is_special_match (expected_bson)) {
         ret = evaluate_special (matcher, expected_bson, actual, path, error);
         goto done;
      }

      if (bson_val_type (actual) != BSON_TYPE_DOCUMENT) {
         MATCH_ERR ("expected type document, got %s", bson_type_to_string (bson_val_type (actual)));
         goto done;
      }

      actual_bson = bson_val_to_document (actual);

      BSON_FOREACH (expected_bson, expected_iter)
      {
         const char *key;
         bson_val_t *expected_val = NULL;
         bson_val_t *actual_val = NULL;
         bson_iter_t actual_iter;
         char *path_child = NULL;

         key = bson_iter_key (&expected_iter);
         expected_val = bson_val_from_iter (&expected_iter);

         if (bson_iter_init_find (&actual_iter, actual_bson, key)) {
            actual_val = bson_val_from_iter (&actual_iter);
         }

         if (bson_val_type (expected_val) == BSON_TYPE_DOCUMENT &&
             is_special_match (bson_val_to_document (expected_val))) {
            bool special_ret;
            path_child = bson_strdup_printf ("%s.%s", path, key);
            special_ret = evaluate_special (matcher, bson_val_to_document (expected_val), actual_val, path, error);
            bson_free (path_child);
            bson_val_destroy (expected_val);
            bson_val_destroy (actual_val);
            if (!special_ret) {
               goto done;
            }
            continue;
         }

         if (NULL == actual_val) {
            MATCH_ERR ("key %s is not present", key);
            bson_val_destroy (expected_val);
            bson_val_destroy (actual_val);
            goto done;
         }

         path_child = bson_strdup_printf ("%s.%s", path, key);
         if (!bson_matcher_match (matcher, expected_val, actual_val, path_child, false, error)) {
            bson_val_destroy (expected_val);
            bson_val_destroy (actual_val);
            bson_free (path_child);
            goto done;
         }
         bson_val_destroy (expected_val);
         bson_val_destroy (actual_val);
         bson_free (path_child);
      }

      expected_keys = bson_count_keys (expected_bson);
      actual_keys = bson_count_keys (actual_bson);

      /* Unified test format spec: "When matching root-level documents, test
       * runners MUST permit the actual document to contain additional fields
       * not present in the expected document.""
       *
       * This logic must also handle the case where `expected` is one of any
       * number of root documents within an array (i.e. cursor result). */
      if (!(is_root || array_of_root_docs) && expected_keys < actual_keys) {
         MATCH_ERR ("expected %" PRIu32 " keys in document, got: %" PRIu32, expected_keys, actual_keys);
         goto done;
      }

      ret = true;
      goto done;
   }

   if (bson_val_type (expected) == BSON_TYPE_ARRAY) {
      bson_iter_t expected_iter;
      const bson_t *expected_bson = bson_val_to_array (expected);
      const bson_t *actual_bson = NULL;
      char *path_child = NULL;
      uint32_t expected_keys = bson_count_keys (expected_bson);
      uint32_t actual_keys;

      if (bson_val_type (actual) != BSON_TYPE_ARRAY) {
         MATCH_ERR ("expected array, but got: %s", bson_type_to_string (bson_val_type (actual)));
         goto done;
      }

      actual_bson = bson_val_to_array (actual);
      actual_keys = bson_count_keys (actual_bson);

      if (expected_keys != actual_keys) {
         MATCH_ERR ("expected array of size %" PRIu32 ", but got array of size: %" PRIu32, expected_keys, actual_keys);
         goto done;
      }

      BSON_FOREACH (expected_bson, expected_iter)
      {
         bson_val_t *expected_val = bson_val_from_iter (&expected_iter);
         bson_val_t *actual_val = NULL;
         bson_iter_t actual_iter;
         const char *key;

         key = bson_iter_key (&expected_iter);
         if (!bson_iter_init_find (&actual_iter, actual_bson, key)) {
            MATCH_ERR ("expected array index: %s, but did not exist", key);
            bson_val_destroy (expected_val);
            bson_val_destroy (actual_val);
            goto done;
         }

         actual_val = bson_val_from_iter (&actual_iter);

         path_child = bson_strdup_printf ("%s.%s", path, key);
         if (!bson_matcher_match (
                matcher, expected_val, actual_val, path_child, (is_root && array_of_root_docs), error)) {
            bson_val_destroy (expected_val);
            bson_val_destroy (actual_val);
            bson_free (path_child);
            goto done;
         }
         bson_val_destroy (expected_val);
         bson_val_destroy (actual_val);
         bson_free (path_child);
      }
      ret = true;
      goto done;
   }

   if (!bson_val_eq (expected, actual, BSON_VAL_FLEXIBLE_NUMERICS)) {
      MATCH_ERR ("value %s != %s", bson_val_to_json (expected), bson_val_to_json (actual));
      goto done;
   }

   ret = true;
done:
   if (!ret && is_root) {
      /* Append the error with more context at the root match. */
      bson_error_t tmp_error;

      memcpy (&tmp_error, error, sizeof (bson_error_t));
      test_set_error (error,
                      "BSON match failed: %s\n"
                      "Expected: %s\n"
                      "Actual:   %s",
                      tmp_error.message,
                      bson_val_to_json (expected),
                      bson_val_to_json (actual));
   }
   return ret;
}

bool
bson_match (const bson_val_t *expected, const bson_val_t *actual, bool array_of_root_docs, bson_error_t *error)
{
   bson_matcher_t *matcher = bson_matcher_new ();
   bool matched = bson_matcher_match (matcher, expected, actual, "", array_of_root_docs, error);
   bson_matcher_destroy (matcher);
   return matched;
}

typedef struct {
   const char *desc;
   const char *expected;
   const char *actual;
   bool expect_match;
} testcase_t;

static void
test_match (void)
{
   testcase_t tests[] = {{"int32 ==", "{'a': 1}", "{'a': 1}", true},
                         {"int32 !=", "{'a': 1}", "{'a': 0}", false},
                         {"$$exists", "{'a': {'$$exists': true}}", "{'a': 0}", true},
                         {"$$exists fail", "{'a': {'$$exists': true}}", "{'b': 0}", false},
                         {"does not $$exists", "{'a': {'$$exists': false}}", "{'b': 0}", true},
                         {"$$unsetOrMatches match", "{'a': {'$$unsetOrMatches': 1}}", "{'a': 1}", true},
                         {"$$unsetOrMatches unset", "{'a': {'$$unsetOrMatches': 1}}", "{}", true},
                         {"$$unsetOrMatches mismatch", "{'a': {'$$unsetOrMatches': 'abc'}}", "{'a': 1}", false},
                         {"$$type match", "{'a': {'$$type': 'string'}}", "{'a': 'abc'}", true},
                         {"$$type mismatch", "{'a': {'$$type': 'string'}}", "{'a': 1}", false},
                         {"$$type array match", "{'a': {'$$type': ['string', 'int']}}", "{'a': 1}", true},
                         {"$$type array mismatch", "{'a': {'$$type': ['string', 'int']}}", "{'a': 1.2}", false},
                         {"extra keys in root ok", "{'a': 1}", "{'a': 1, 'b': 2}", true},
                         {"extra keys in subdoc not ok", "{'a': {'b': 1}}", "{'a': {'b': 1, 'c': 2}}", false}};
   int i;

   for (i = 0; i < sizeof (tests) / sizeof (testcase_t); i++) {
      testcase_t *test = tests + i;
      bson_error_t error;
      bson_val_t *expected = bson_val_from_json (test->expected);
      bson_val_t *actual = bson_val_from_json (test->actual);
      bool ret;

      ret = bson_match (expected, actual, false, &error);
      if (test->expect_match) {
         if (!ret) {
            test_error ("%s: did not match with error: %s, but should have", test->desc, error.message);
         }
      } else {
         if (ret) {
            test_error ("%s: matched, but should not have", test->desc);
         }
      }
      bson_val_destroy (expected);
      bson_val_destroy (actual);
   }
}

void
test_bson_match_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/unified/selftest/bson/match", test_match);
}
