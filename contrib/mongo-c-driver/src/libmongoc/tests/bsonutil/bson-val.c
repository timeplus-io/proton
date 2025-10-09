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

#include "bsonutil/bson-val.h"
#include "test-conveniences.h"
#include "TestSuite.h"
#include "unified/util.h"

struct _bson_val_t {
   bson_value_t value;
   bson_type_t type;
   bson_t *as_bson;
   char *as_string;
};

bson_val_t *
bson_val_from_value (const bson_value_t *value)
{
   bson_val_t *val = NULL;
   bson_t tmp;
   char *as_string = NULL;

   BSON_ASSERT (value);

   val = bson_malloc0 (sizeof (bson_val_t));
   bson_value_copy (value, &val->value);
   if (value->value_type == BSON_TYPE_DOCUMENT || value->value_type == BSON_TYPE_ARRAY) {
      val->as_bson = bson_new_from_data (value->value.v_doc.data, value->value.v_doc.data_len);
   }
   val->type = value->value_type;

   bson_init (&tmp);
   bson_append_value (&tmp, "v", 1, value);
   as_string = bson_as_canonical_extended_json (&tmp, NULL);

   if (!as_string) {
      val->as_string = NULL;
   } else {
      /* This produces: { "v" : {...} }. Strip off the wrapping "v" and braces.
       */
      val->as_string = bson_strndup (as_string + 7, strlen (as_string) - 9);
      bson_free (as_string);
   }

   bson_destroy (&tmp);
   return val;
}

bson_val_t *
bson_val_from_json (const char *single_quoted_json)
{
   bson_val_t *val = NULL;
   char *double_quoted = single_quotes_to_double (single_quoted_json);
   bson_t as_bson;
   bson_error_t error;

   if (!bson_init_from_json (&as_bson, double_quoted, -1, &error)) {
      test_error ("unable to construct bson value from: %s", single_quoted_json);
   }

   val = bson_val_from_bson (&as_bson);
   bson_free (double_quoted);
   bson_destroy (&as_bson);
   return val;
}

bson_val_t *
bson_val_from_iter (const bson_iter_t *iter)
{
   return bson_val_from_value (bson_iter_value ((bson_iter_t *) iter));
}

bson_val_t *
bson_val_from_bson (const bson_t *bson)
{
   bson_value_t value;
   bson_iter_t iter;

   value.value.v_doc.data = (uint8_t *) bson_get_data (bson);
   value.value.v_doc.data_len = bson->len;
   value.value_type = BSON_TYPE_DOCUMENT;

   bson_iter_init (&iter, bson);
   if (bson_iter_next (&iter) && 0 == strcmp ("0", bson_iter_key (&iter))) {
      value.value_type = BSON_TYPE_ARRAY;
   }

   return bson_val_from_value (&value);
}

/* Always force to be an array. */
bson_val_t *
bson_val_from_array (const bson_t *bson)
{
   bson_value_t value;

   value.value.v_doc.data = (uint8_t *) bson_get_data (bson);
   value.value.v_doc.data_len = bson->len;
   value.value_type = BSON_TYPE_ARRAY;

   return bson_val_from_value (&value);
}

/* Always force to be an document. */
bson_val_t *
bson_val_from_doc (const bson_t *bson)
{
   bson_value_t value;

   value.value.v_doc.data = (uint8_t *) bson_get_data (bson);
   value.value.v_doc.data_len = bson->len;
   value.value_type = BSON_TYPE_DOCUMENT;

   return bson_val_from_value (&value);
}

bson_val_t *
bson_val_from_int64 (int64_t val)
{
   bson_value_t value;

   value.value_type = BSON_TYPE_INT64;
   value.value.v_int64 = val;
   return bson_val_from_value (&value);
}

bson_val_t *
bson_val_from_bytes (const uint8_t *bytes, uint32_t len)
{
   bson_value_t value;

   value.value_type = BSON_TYPE_BINARY;
   value.value.v_binary.subtype = BSON_SUBTYPE_BINARY;
   value.value.v_binary.data = (uint8_t *) bytes;
   value.value.v_binary.data_len = len;
   return bson_val_from_value (&value);
}

bson_val_t *
bson_val_copy (const bson_val_t *val)
{
   if (!val) {
      return NULL;
   }
   return bson_val_from_value (&val->value);
}

void
bson_val_destroy (bson_val_t *val)
{
   if (!val) {
      return;
   }

   bson_free (val->as_string);
   bson_destroy (val->as_bson);
   bson_value_destroy (&val->value);
   bson_free (val);
}

bool
bson_val_eq (const bson_val_t *a, const bson_val_t *b, bson_val_comparison_flags_t flags)
{
   bson_type_t vtype;

   vtype = a->type;
   if (vtype == BSON_TYPE_DOUBLE || vtype == BSON_TYPE_INT32 || vtype == BSON_TYPE_INT64) {
      if (flags & BSON_VAL_FLEXIBLE_NUMERICS) {
         return bson_val_convert_int64 (a) == bson_val_convert_int64 (b);
      }
   }

   if (a->type != b->type) {
      return false;
   }

   if (vtype == BSON_TYPE_ARRAY || vtype == BSON_TYPE_DOCUMENT) {
      if (flags & BSON_VAL_UNORDERED) {
         bool ret;
         bson_t *a_sorted, *b_sorted;
         a_sorted = bson_copy_and_sort (bson_val_to_bson (a));
         b_sorted = bson_copy_and_sort (bson_val_to_bson (b));
         ret = bson_equal (a_sorted, b_sorted);
         bson_destroy (a_sorted);
         bson_destroy (b_sorted);
         return ret;
      }
   }

   if (vtype == BSON_TYPE_CODEWSCOPE) {
      bool scope_equal;
      bson_t *a_scope, *a_scope_sorted, *b_scope, *b_scope_sorted;

      a_scope = bson_new_from_data (a->value.value.v_codewscope.scope_data, a->value.value.v_codewscope.scope_len);
      b_scope = bson_new_from_data (b->value.value.v_codewscope.scope_data, b->value.value.v_codewscope.scope_len);

      a_scope_sorted = bson_copy_and_sort (a_scope);
      b_scope_sorted = bson_copy_and_sort (b_scope);

      scope_equal = bson_equal (a_scope_sorted, b_scope_sorted);
      bson_destroy (a_scope);
      bson_destroy (b_scope);
      bson_destroy (a_scope_sorted);
      bson_destroy (b_scope_sorted);

      if (!scope_equal) {
         return false;
      }

      return 0 == strcmp (a->value.value.v_codewscope.code, b->value.value.v_codewscope.code);
   }

   /* All other cases, compare exact match by looking at canonical extended JSON
    * string representation. */
   return 0 == strcmp (a->as_string, b->as_string);
}

bson_type_t
bson_val_type (const bson_val_t *val)
{
   return val->type;
}

const bson_t *
bson_val_to_document (const bson_val_t *val)
{
   if (val->type != BSON_TYPE_DOCUMENT) {
      test_error ("expected document, got: %s", bson_type_to_string (val->type));
   }
   return val->as_bson;
}

const bson_t *
bson_val_to_array (const bson_val_t *val)
{
   if (val->type != BSON_TYPE_ARRAY) {
      test_error ("expected array, got: %s", bson_type_to_string (val->type));
   }
   return val->as_bson;
}

/* Either document or array. */
const bson_t *
bson_val_to_bson (const bson_val_t *val)
{
   if (val->type != BSON_TYPE_ARRAY && val->type != BSON_TYPE_DOCUMENT) {
      test_error ("expected document or array, got: %s", bson_type_to_string (val->type));
   }
   return val->as_bson;
}

const uint8_t *
bson_val_to_binary (const bson_val_t *val, uint32_t *len)
{
   if (val->type != BSON_TYPE_BINARY) {
      test_error ("expected binary, got: %s", bson_type_to_string (val->type));
   }
   *len = (uint32_t) val->value.value.v_binary.data_len;
   return val->value.value.v_binary.data;
}

const bson_value_t *
bson_val_to_value (const bson_val_t *val)
{
   return (bson_value_t *) &val->value;
}

const char *
bson_val_to_utf8 (const bson_val_t *val)
{
   BSON_ASSERT (val->type == BSON_TYPE_UTF8);
   return val->value.value.v_utf8.str;
}

bool
bson_val_is_numeric (const bson_val_t *val)
{
   return (val->type == BSON_TYPE_INT32 || val->type == BSON_TYPE_INT64 || val->type == BSON_TYPE_DOUBLE);
}

int64_t
bson_val_convert_int64 (const bson_val_t *val)
{
   if (val->type == BSON_TYPE_INT32) {
      return (int64_t) val->value.value.v_int32;
   }
   if (val->type == BSON_TYPE_INT64) {
      return (int64_t) val->value.value.v_int64;
   }
   if (val->type == BSON_TYPE_DOUBLE) {
      return (int64_t) val->value.value.v_double;
   }

   test_error ("expected int64, int32, or double, got: %s", bson_type_to_string (val->type));
   return 0;
}

const char *
bson_val_to_json (const bson_val_t *val)
{
   if (!val) {
      return "(NULL)";
   }

   return val->as_string;
}
