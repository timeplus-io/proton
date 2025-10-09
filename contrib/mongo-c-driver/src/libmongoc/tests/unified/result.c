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

#include "result.h"

#include "bson/bson.h"
#include "bsonutil/bson-parser.h"
#include "bsonutil/bson-match.h"
#include "mongoc-error-private.h"
#include "test-conveniences.h"
#include "util.h"
#include "TestSuite.h"

struct _result_t {
   bool ok;
   bson_val_t *value;
   bson_error_t error;
   /* For a write operation, reply is the original unmodified write result for a
      write operation.
      For a read operation, reply is an optional server reply. */
   bson_t *reply;
   char *str;
   bool array_of_root_docs;
};

result_t *
result_new (void)
{
   return bson_malloc0 (sizeof (result_t));
}

static void
_result_init (result_t *result, const bson_val_t *value, const bson_t *reply, const bson_error_t *error)
{
   bson_string_t *str;

   str = bson_string_new ("");

   if (value) {
      result->value = bson_val_copy (value);
      bson_string_append_printf (str, "value=%s ", bson_val_to_json (value));
   }

   if (reply) {
      char *reply_str = bson_as_canonical_extended_json (reply, NULL);

      bson_string_append_printf (str, "reply=%s ", reply_str);
      result->reply = bson_copy (reply);
      bson_free (reply_str);
   }

   bson_string_append_printf (str, "bson_error=%s", error->message);
   memcpy (&result->error, error, sizeof (bson_error_t));
   result->ok = (error->code == 0);
   result->str = bson_string_free (str, false);
}

void
result_destroy (result_t *result)
{
   if (!result) {
      return;
   }
   bson_val_destroy (result->value);
   bson_destroy (result->reply);
   bson_free (result->str);
   bson_free (result);
}

const char *
result_to_string (result_t *result)
{
   return result->str;
}

bson_val_t *
result_get_val (result_t *result)
{
   return result->value;
}

bson_t *
rewrite_upserted_ids (bson_t *mongoc_upserted_ids)
{
   bson_t *upserted_ids;
   bson_iter_t iter;

   upserted_ids = bson_new ();
   BSON_FOREACH (mongoc_upserted_ids, iter)
   {
      bson_t el;
      bson_parser_t *el_bp;
      int64_t *index;
      bson_val_t *id;
      char storage[16];
      const char *key;

      bson_iter_bson (&iter, &el);
      el_bp = bson_parser_new ();
      bson_parser_int (el_bp, "index", &index);
      bson_parser_any (el_bp, "_id", &id);
      bson_parser_parse_or_assert (el_bp, &el);
      bson_uint32_to_string ((uint32_t) *index, &key, storage, sizeof (storage));
      BSON_APPEND_VALUE (upserted_ids, key, bson_val_to_value (id));
      bson_parser_destroy_with_parsed_fields (el_bp);
   }

   return upserted_ids;
}

bson_t *
rewrite_bulk_write_result (const bson_t *bulk_write_result)
{
   bson_t *const res = bson_new ();
   if (!bson_empty0 (bulk_write_result)) {
      BCON_APPEND (res,
                   "insertedCount",
                   BCON_INT32 (bson_lookup_int32 (bulk_write_result, "nInserted")),
                   "deletedCount",
                   BCON_INT32 (bson_lookup_int32 (bulk_write_result, "nRemoved")),
                   "matchedCount",
                   BCON_INT32 (bson_lookup_int32 (bulk_write_result, "nMatched")),
                   "modifiedCount",
                   BCON_INT32 (bson_lookup_int32 (bulk_write_result, "nModified")),
                   "upsertedCount",
                   BCON_INT32 (bson_lookup_int32 (bulk_write_result, "nUpserted")));

      if (bson_has_field (bulk_write_result, "upserted")) {
         bson_t *const upserted_ids = bson_lookup_bson (bulk_write_result, "upserted");
         bson_t *const rewritten_upserted_ids = rewrite_upserted_ids (upserted_ids);
         BSON_APPEND_DOCUMENT (res, "upsertedIds", rewritten_upserted_ids);
         bson_destroy (upserted_ids);
         bson_destroy (rewritten_upserted_ids);
      } else {
         /* upsertedIds is a required field in BulkWriteResult, so append an
          * empty document even if no documents were upserted. */
         bson_t empty = BSON_INITIALIZER;
         BSON_APPEND_DOCUMENT (res, "upsertedIds", &empty);
      }
   }
   return res;
}

void
result_from_bulk_write (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_t *const write_result = rewrite_bulk_write_result (reply);
   bson_val_t *const val = bson_val_from_bson (write_result);

   _result_init (result, val, reply, error);

   bson_val_destroy (val);
   bson_destroy (write_result);
}

void
result_from_insert_one (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_t *write_result;
   bson_val_t *val;

   write_result = bson_new ();
   if (!bson_empty (reply)) {
      BCON_APPEND (write_result, "insertedCount", BCON_INT32 (bson_lookup_int32 (reply, "insertedCount")));
   }

   val = bson_val_from_bson (write_result);
   _result_init (result, val, reply, error);
   bson_val_destroy (val);
   bson_destroy (write_result);
}

void
result_from_insert_many (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_t *write_result;
   bson_val_t *val;

   write_result = bson_new ();
   if (!bson_empty (reply)) {
      BCON_APPEND (write_result,
                   "insertedCount",
                   BCON_INT32 (bson_lookup_int32 (reply, "insertedCount")),
                   "deletedCount",
                   BCON_INT32 (0),
                   "matchedCount",
                   BCON_INT32 (0),
                   "modifiedCount",
                   BCON_INT32 (0),
                   "upsertedCount",
                   BCON_INT32 (0),
                   "upsertedIds",
                   "{",
                   "}");
   }

   val = bson_val_from_bson (write_result);
   _result_init (result, val, reply, error);
   bson_val_destroy (val);
   bson_destroy (write_result);
}

void
result_from_update_or_replace (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_t *write_result;
   bson_val_t *val;
   bson_iter_t iter;

   write_result = bson_new ();
   if (!bson_empty (reply)) {
      BCON_APPEND (write_result,
                   "matchedCount",
                   BCON_INT32 (bson_lookup_int32 (reply, "matchedCount")),
                   "modifiedCount",
                   BCON_INT32 (bson_lookup_int32 (reply, "modifiedCount")),
                   "upsertedCount",
                   BCON_INT32 (bson_lookup_int32 (reply, "upsertedCount")));

      if (bson_iter_init_find (&iter, reply, "upsertedId")) {
         BSON_APPEND_VALUE (write_result, "upsertedId", bson_iter_value (&iter));
      }
   }

   val = bson_val_from_bson (write_result);
   _result_init (result, val, reply, error);
   bson_val_destroy (val);
   bson_destroy (write_result);
}

void
result_from_delete (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_t *write_result;
   bson_val_t *val;

   write_result = bson_new ();
   if (!bson_empty (reply)) {
      BCON_APPEND (write_result, "deletedCount", BCON_INT32 (bson_lookup_int32 (reply, "deletedCount")));
   }

   val = bson_val_from_bson (write_result);
   _result_init (result, val, reply, error);
   bson_val_destroy (val);
   bson_destroy (write_result);
}

void
result_from_distinct (result_t *result, const bson_t *reply, const bson_error_t *error)
{
   bson_val_t *val = NULL;
   bson_iter_t iter;

   if (!bson_empty (reply)) {
      if (bson_iter_init_find (&iter, reply, "values")) {
         val = bson_val_from_iter (&iter);
      }
   }

   _result_init (result, val, reply, error);
   bson_val_destroy (val);
}

void
result_from_cursor (result_t *result, mongoc_cursor_t *cursor)
{
   bson_error_t error = {0};
   const bson_t *reply = NULL;
   bson_t *documents = bson_new ();
   uint32_t i = 0;
   const bson_t *doc;
   bson_val_t *val;

   while (mongoc_cursor_next (cursor, &doc)) {
      const char *key;
      char storage[16];

      bson_uint32_to_string (i, &key, storage, sizeof (storage) / sizeof (char));
      BSON_APPEND_DOCUMENT (documents, key, doc);
      i++;
   }

   mongoc_cursor_error_document (cursor, &error, &reply);
   val = bson_val_from_array (documents);

   _result_init (result, val, (bson_t *) reply, &error);
   result->array_of_root_docs = true;
   bson_destroy (documents);
   bson_val_destroy (val);
}

void
result_from_val_and_reply (result_t *result, const bson_val_t *val, const bson_t *reply, const bson_error_t *error)
{
   _result_init (result, val, reply, error);
}

void
result_from_ok (result_t *result)
{
   bson_error_t error = {0};
   _result_init (result, NULL, NULL, &error);
}

bool
result_check (result_t *result, entity_map_t *em, bson_val_t *expect_result, bson_t *expect_error, bson_error_t *error)
{
   bool ret = false;
   bson_parser_t *parser = NULL;
   bool *is_error;
   bool *is_client_error;
   char *error_contains;
   int64_t *error_code;
   char *error_code_name;
   bson_t *error_labels_contain;
   bson_t *error_labels_omit;
   bson_val_t *error_expect_result;
   bson_val_t *error_response;

   if (!expect_result && !expect_error) {
      if (!result->ok) {
         test_set_error (error, "expected success, but got error: %s", result->error.message);
         goto done;
      }
      ret = true;
      goto done;
   }

   /* check result. */
   if (expect_result) {
      if (!result->ok) {
         test_set_error (error, "expected result, but got error: %s", result->error.message);
         goto done;
      }
      if (!entity_map_match (em, expect_result, result->value, result->array_of_root_docs, error)) {
         test_diagnostics_error_info ("expectResult mismatch:\nExpected: %s\nActual: %s\n",
                                      bson_val_to_json (expect_result),
                                      bson_val_to_json (result->value));
         goto done;
      }
   }

   if (expect_error) {
      parser = bson_parser_new ();
      bson_parser_bool_optional (parser, "isError", &is_error);
      bson_parser_bool_optional (parser, "isClientError", &is_client_error);
      bson_parser_utf8_optional (parser, "errorContains", &error_contains);
      bson_parser_int_optional (parser, "errorCode", &error_code);
      bson_parser_utf8_optional (parser, "errorCodeName", &error_code_name);
      bson_parser_array_optional (parser, "errorLabelsContain", &error_labels_contain);
      bson_parser_array_optional (parser, "errorLabelsOmit", &error_labels_omit);
      bson_parser_any_optional (parser, "expectResult", &error_expect_result);
      bson_parser_any_optional (parser, "errorResponse", &error_response);
      if (!bson_parser_parse (parser, expect_error, error)) {
         goto done;
      }

      MONGOC_DEBUG ("expected error");

      if (result->ok) {
         test_set_error (error, "expected error, but no error: %s", bson_val_to_json (result->value));
         goto done;
      }

      if (is_client_error && *is_client_error) {
         /* from errors.rst: "In Version 2, error codes originating on the
          * server always have error domain ``MONGOC_ERROR_SERVER`` or
          * ``MONGOC_ERROR_WRITE_CONCERN``" */
         if (result->error.domain == MONGOC_ERROR_SERVER || result->error.domain == MONGOC_ERROR_WRITE_CONCERN_ERROR) {
            test_set_error (error,
                            "expected client side error, but got: %" PRIu32 ", %" PRIu32,
                            result->error.domain,
                            result->error.code);
            goto done;
         }
      }

      if (error_contains) {
         if (strstr (result->error.message, error_contains) == NULL) {
            test_set_error (
               error, "expected error to contain \"%s\", but got: \"%s\"", error_contains, result->error.message);
            goto done;
         }
      }

      if (error_code) {
         if ((*error_code) != (int64_t) result->error.code) {
            test_set_error (
               error, "expected error code %" PRIi64 ", but got: %" PRIi64, *error_code, (int64_t) result->error.code);
            goto done;
         }
      }

      /* Waiting on CDRIVER-3147
      if (error_code_name) {
         bson_iter_t iter;

         if (!result->reply) {
            test_set_error (error, "%s", "expected error code name, but no error
      reply set"); goto done;
         }

         if (!bson_iter_init_find (&iter, result->reply, "codeName") ||
             !BSON_ITER_HOLDS_UTF8 (&iter)) {
            test_set_error (error, "utf8 codeName not found in error reply: %s",
      tmp_json (result->reply)); goto done;
         }

         if (0 !=
             bson_strcasecmp (bson_iter_utf8 (&iter, NULL), error_code_name)) {
            test_set_error (error, "expected codeName: %s, got: %s",
      error_code_name, bson_iter_utf8 (&iter, NULL)); goto done;
         }
      }
      */

      if (error_labels_contain) {
         bson_iter_t iter;

         if (!result->reply) {
            test_set_error (error, "%s", "expected error to contain labels, but got no error document");
            goto done;
         }

         BSON_FOREACH (error_labels_contain, iter)
         {
            const char *label;

            if (!BSON_ITER_HOLDS_UTF8 (&iter)) {
               test_set_error (
                  error, "expected UTF8 error label, got: %s", bson_type_to_string (bson_iter_type (&iter)));
               goto done;
            }

            label = bson_iter_utf8 (&iter, NULL);
            if (!mongoc_error_has_label (result->reply, label)) {
               test_set_error (
                  error, "expected error to contain label: %s, but got: %s", label, tmp_json (result->reply));
               goto done;
            }
         }
      }

      if (error_labels_omit) {
         bson_iter_t iter;

         if (!result->reply) {
            test_set_error (error, "%s", "expected error to omit labels, but got no error document");
            goto done;
         }

         BSON_FOREACH (error_labels_omit, iter)
         {
            const char *label;

            if (!BSON_ITER_HOLDS_UTF8 (&iter)) {
               test_set_error (
                  error, "expected UTF8 error label, got: %s", bson_type_to_string (bson_iter_type (&iter)));
               goto done;
            }

            label = bson_iter_utf8 (&iter, NULL);
            if (mongoc_error_has_label (result->reply, label)) {
               test_set_error (error, "expected error to omit label: %s, but got: %s", label, tmp_json (result->reply));
               goto done;
            }
         }
      }

      if (error_expect_result) {
         if (!result->value) {
            test_set_error (error, "%s", "expected error with result, but result unset");
            goto done;
         }

         /* Note: expectError.expectResult is not used for cursor-bearing
          * operations, so array_of_root_docs should always be false */
         BSON_ASSERT (!result->array_of_root_docs);

         if (!bson_match (error_expect_result, result->value, result->array_of_root_docs, error)) {
            test_diagnostics_error_info ("error.expectResult mismatch:\nExpected: %s\nActual: %s\n",
                                         bson_val_to_json (error_expect_result),
                                         bson_val_to_json (result->value));
            goto done;
         }
      }

      if (error_response) {
         if (!result->reply) {
            test_set_error (error, "%s", "expected error with a reply, but reply unset");
            goto done;
         }

         /* Write operations have the raw response wrapped in a errorReplies
          * array. Read operations return the raw response as the reply. */
         bson_t doc_to_match;
         bson_iter_t iter;

         if (bson_iter_init_find (&iter, result->reply, "errorReplies")) {
            bson_iter_t child;

            if (!BSON_ITER_HOLDS_ARRAY (&iter)) {
               test_set_error (error,
                               "expected errorReplies to be an array, but received %s",
                               bson_type_to_string (bson_iter_type (&iter)));
               goto done;
            }
            bson_iter_recurse (&iter, &child);
            bson_iter_next (&child);
            if (!BSON_ITER_HOLDS_DOCUMENT (&child)) {
               test_set_error (error,
                               "expected errorReplies.0 to be a document but received %s",
                               bson_type_to_string (bson_iter_type (&iter)));
               goto done;
            }
            bson_iter_bson (&child, &doc_to_match);
         } else {
            bson_copy_to (result->reply, &doc_to_match);
         }

         bson_val_t *val_to_match = bson_val_from_bson (&doc_to_match);

         if (!bson_match (error_response, val_to_match, result->array_of_root_docs, error)) {
            test_diagnostics_error_info ("error.errorResponse mismatch:\nExpected: %s\nActual: %s\n",
                                         bson_val_to_json (error_response),
                                         bson_as_json (result->reply, NULL));
            bson_val_destroy (val_to_match);
            bson_destroy (&doc_to_match);
            goto done;
         }
         bson_val_destroy (val_to_match);
         bson_destroy (&doc_to_match);
      }
   }

   ret = true;
done:
   bson_parser_destroy_with_parsed_fields (parser);
   return ret;
}

static void
test_resultfrombulkwrite (void)
{
   bson_error_t error;
   result_t *result;
   bson_t *reply;
   bson_val_t *expect;
   bson_error_t empty = {0};

   result = result_new ();
   reply = tmp_bson ("{ 'nInserted': 0, 'nRemoved' : 1, 'nMatched' : 2, "
                     "'nModified' : 3, 'nUpserted' : 4 }");
   expect = bson_val_from_json ("{ 'insertedCount': 0, 'deletedCount': 1, 'matchedCount': 2, "
                                "'modifiedCount': 3, 'upsertedCount': 4}");
   result_from_bulk_write (result, reply, &empty);
   MONGOC_DEBUG ("rewritten to: %s", bson_val_to_json (result->value));
   if (!result_check (result, NULL, expect, NULL, &error)) {
      test_error ("result_check error: %s", error.message);
   }
   result_destroy (result);
   bson_val_destroy (expect);
}

void
test_result_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/unified/result/resultfrombulkwrite", test_resultfrombulkwrite);
}
