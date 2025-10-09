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

#include "bsonutil/bson-parser.h"

#include "unified/util.h"
#include "test-conveniences.h"
#include "TestSuite.h"
#include "utlist.h"

typedef enum {
   BSON_PARSER_UTF8,
   BSON_PARSER_INT,
   BSON_PARSER_BOOL,
   BSON_PARSER_DOC,
   BSON_PARSER_ARRAY,
   BSON_PARSER_ARRAY_OR_DOC,
   BSON_PARSER_ANY,
   BSON_PARSER_WRITE_CONCERN,
   BSON_PARSER_READ_CONCERN,
   BSON_PARSER_READ_PREFS
} bson_parser_type_t;

typedef struct _bson_parser_entry_t {
   bson_parser_type_t ptype;
   bool optional;
   void *out;
   char *key;
   bool set;
   struct _bson_parser_entry_t *next;
} bson_parser_entry_t;

struct _bson_parser_t {
   bson_parser_entry_t *entries;
   bool allow_extra;
   bson_t *extra;
};

static const char *
parser_type_to_string (bson_parser_type_t ptype)
{
   switch (ptype) {
   case BSON_PARSER_UTF8:
      return "UTF8";
   case BSON_PARSER_INT:
      return "INT";
   case BSON_PARSER_BOOL:
      return "BOOL";
   case BSON_PARSER_DOC:
      return "DOC";
   case BSON_PARSER_ARRAY:
      return "ARRAY";
   case BSON_PARSER_ARRAY_OR_DOC:
      return "ARRAY or DOC";
   case BSON_PARSER_ANY:
      return "ANY";
   case BSON_PARSER_WRITE_CONCERN:
      return "WRITE_CONCERN";
   case BSON_PARSER_READ_CONCERN:
      return "READ_CONCERN";
   case BSON_PARSER_READ_PREFS:
      return "READ_PREFS";
   default:
      return "INVALID";
   }
}

static mongoc_write_concern_t *
bson_to_write_concern (bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   mongoc_write_concern_t *out = NULL;
   bool *journal = NULL;
   bson_val_t *w_val = NULL;
   int64_t *wtimeoutms;

   parser = bson_parser_new ();
   bson_parser_bool_optional (parser, "journal", &journal);
   bson_parser_any_optional (parser, "w", &w_val);
   bson_parser_int_optional (parser, "wTimeoutMS", &wtimeoutms);

   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   out = mongoc_write_concern_new ();
   if (journal) {
      mongoc_write_concern_set_journal (out, *journal);
   }

   if (w_val) {
      if (bson_val_is_numeric (w_val)) {
         mongoc_write_concern_set_w (out, (int32_t) bson_val_convert_int64 (w_val));
      } else if (bson_val_type (w_val) == BSON_TYPE_UTF8 && 0 == strcmp (bson_val_to_utf8 (w_val), "majority")) {
         mongoc_write_concern_set_w (out, MONGOC_WRITE_CONCERN_W_MAJORITY);
      } else {
         test_set_error (error, "unrecognized value for 'w': %s", bson_val_to_json (w_val));
      }
   }

   if (wtimeoutms) {
      mongoc_write_concern_set_wtimeout_int64 (out, *wtimeoutms);
   }

done:
   bson_parser_destroy_with_parsed_fields (parser);
   return out;
}

static mongoc_read_concern_t *
bson_to_read_concern (bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   mongoc_read_concern_t *out = NULL;
   char *level = NULL;

   parser = bson_parser_new ();
   bson_parser_utf8_optional (parser, "level", &level);

   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   out = mongoc_read_concern_new ();
   if (level) {
      mongoc_read_concern_set_level (out, level);
   }

done:
   bson_parser_destroy_with_parsed_fields (parser);
   return out;
}

/* Returns 0 on error. */
static mongoc_read_mode_t
string_to_read_mode (char *str, bson_error_t *error)
{
   if (0 == bson_strcasecmp ("primary", str)) {
      return MONGOC_READ_PRIMARY;
   } else if (0 == bson_strcasecmp ("primarypreferred", str)) {
      return MONGOC_READ_PRIMARY_PREFERRED;
   } else if (0 == bson_strcasecmp ("secondary", str)) {
      return MONGOC_READ_SECONDARY;
   } else if (0 == bson_strcasecmp ("secondarypreferred", str)) {
      return MONGOC_READ_SECONDARY_PREFERRED;
   } else if (0 == bson_strcasecmp ("nearest", str)) {
      return MONGOC_READ_NEAREST;
   }

   test_set_error (error, "Invalid read mode: %s", str);
   return 0;
}

static mongoc_read_prefs_t *
bson_to_read_prefs (bson_t *bson, bson_error_t *error)
{
   bson_parser_t *parser = NULL;
   mongoc_read_prefs_t *out = NULL;
   char *mode_string = NULL;
   mongoc_read_mode_t read_mode;
   bson_t *tag_sets = NULL;
   int64_t *max_staleness_seconds;
   bson_t *hedge = NULL;

   parser = bson_parser_new ();
   bson_parser_utf8 (parser, "mode", &mode_string);
   bson_parser_array_optional (parser, "tagSets", &tag_sets);
   bson_parser_int_optional (parser, "maxStalenessSeconds", &max_staleness_seconds);
   bson_parser_doc_optional (parser, "hedge", &hedge);

   if (!bson_parser_parse (parser, bson, error)) {
      goto done;
   }

   read_mode = string_to_read_mode (mode_string, error);
   if (read_mode == 0) {
      goto done;
   }

   out = mongoc_read_prefs_new (read_mode);
   if (tag_sets) {
      mongoc_read_prefs_set_tags (out, tag_sets);
   }

   if (max_staleness_seconds) {
      mongoc_read_prefs_set_max_staleness_seconds (out, *max_staleness_seconds);
   }

   if (hedge) {
      mongoc_read_prefs_set_hedge (out, hedge);
   }

done:
   bson_parser_destroy_with_parsed_fields (parser);
   return out;
}


bson_parser_t *
bson_parser_new (void)
{
   bson_parser_t *parser = NULL;

   parser = bson_malloc0 (sizeof (bson_parser_t));
   parser->extra = bson_new ();
   return parser;
}

void
bson_parser_allow_extra (bson_parser_t *parser, bool val)
{
   BSON_ASSERT (parser);
   parser->allow_extra = val;
}

const bson_t *
bson_parser_get_extra (const bson_parser_t *parser)
{
   BSON_ASSERT (parser);
   return parser->extra;
}

static void
bson_parser_entry_destroy (bson_parser_entry_t *entry, bool with_parsed_fields)
{
   if (with_parsed_fields) {
      if (entry->ptype == BSON_PARSER_DOC || entry->ptype == BSON_PARSER_ARRAY ||
          entry->ptype == BSON_PARSER_ARRAY_OR_DOC) {
         bson_t **out;

         out = (bson_t **) entry->out;
         bson_destroy (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_BOOL) {
         bool **out;

         out = (bool **) entry->out;
         bson_free (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_UTF8) {
         char **out;

         out = (char **) entry->out;
         bson_free (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_INT) {
         int64_t **out;

         out = (int64_t **) entry->out;
         bson_free (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_ANY) {
         bson_val_t **out;

         out = (bson_val_t **) entry->out;
         bson_val_destroy (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_WRITE_CONCERN) {
         mongoc_write_concern_t **out;

         out = (mongoc_write_concern_t **) entry->out;
         mongoc_write_concern_destroy (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_READ_CONCERN) {
         mongoc_read_concern_t **out;

         out = (mongoc_read_concern_t **) entry->out;
         mongoc_read_concern_destroy (*out);
         *out = NULL;
      } else if (entry->ptype == BSON_PARSER_READ_PREFS) {
         mongoc_read_prefs_t **out;

         out = (mongoc_read_prefs_t **) entry->out;
         mongoc_read_prefs_destroy (*out);
         *out = NULL;
      }
   }
   bson_free (entry->key);
   bson_free (entry);
}

static void
bson_parser_destroy_helper (bson_parser_t *parser, bool with_parsed_fields)
{
   bson_parser_entry_t *entry, *tmp;

   if (!parser) {
      return;
   }

   LL_FOREACH_SAFE (parser->entries, entry, tmp)
   {
      bson_parser_entry_destroy (entry, with_parsed_fields);
   }
   bson_destroy (parser->extra);
   bson_free (parser);
}

void
bson_parser_destroy (bson_parser_t *parser)
{
   bson_parser_destroy_helper (parser, false);
}

/* This additionally destroys the destination of each parsed field. */
void
bson_parser_destroy_with_parsed_fields (bson_parser_t *parser)
{
   bson_parser_destroy_helper (parser, true);
}

static void
bson_parser_add_entry (bson_parser_t *parser, const char *key, void *out, bson_parser_type_t ptype, bool optional)
{
   bson_parser_entry_t *e = NULL;
   bson_parser_entry_t *match = NULL;

   e = bson_malloc0 (sizeof (*e));
   e->optional = optional;
   e->ptype = ptype;
   e->out = out;
   e->key = bson_strdup (key);

   /* Check if an entry already exists for this key. */
   LL_FOREACH (parser->entries, match)
   {
      if (0 == strcmp (match->key, key)) {
         break;
      }
   }

   if (match != NULL) {
      test_error ("Invalid parser configuration. Attempted to add duplicated "
                  "type for %s.",
                  key);
   }
   LL_PREPEND (parser->entries, e);
}

void
bson_parser_utf8 (bson_parser_t *parser, const char *key, char **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_UTF8, false);
}

void
bson_parser_utf8_optional (bson_parser_t *parser, const char *key, char **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_UTF8, true);
}

void
bson_parser_doc (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_DOC, false);
}

void
bson_parser_doc_optional (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_DOC, true);
}

void
bson_parser_array (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_ARRAY, false);
}
void
bson_parser_array_optional (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_ARRAY, true);
}

void
bson_parser_array_or_doc (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_ARRAY_OR_DOC, false);
}
void
bson_parser_array_or_doc_optional (bson_parser_t *parser, const char *key, bson_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_ARRAY_OR_DOC, true);
}

void
bson_parser_bool (bson_parser_t *parser, const char *key, bool **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_BOOL, false);
}

void
bson_parser_bool_optional (bson_parser_t *parser, const char *key, bool **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_BOOL, true);
}

void
bson_parser_int (bson_parser_t *parser, const char *key, int64_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_INT, false);
}

void
bson_parser_int_optional (bson_parser_t *parser, const char *key, int64_t **out)
{
   *out = NULL;
   bson_parser_add_entry (parser, key, (void *) out, BSON_PARSER_INT, true);
}

void
bson_parser_any (bson_parser_t *bp, const char *key, bson_val_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, key, (void *) out, BSON_PARSER_ANY, false);
}

void
bson_parser_any_optional (bson_parser_t *bp, const char *key, bson_val_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, key, (void *) out, BSON_PARSER_ANY, true);
}

void
bson_parser_write_concern (bson_parser_t *bp, mongoc_write_concern_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "writeConcern", (void *) out, BSON_PARSER_WRITE_CONCERN, false);
}

void
bson_parser_write_concern_optional (bson_parser_t *bp, mongoc_write_concern_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "writeConcern", (void *) out, BSON_PARSER_WRITE_CONCERN, true);
}

void
bson_parser_read_concern (bson_parser_t *bp, mongoc_read_concern_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "readConcern", (void *) out, BSON_PARSER_READ_CONCERN, false);
}

void
bson_parser_read_concern_optional (bson_parser_t *bp, mongoc_read_concern_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "readConcern", (void *) out, BSON_PARSER_READ_CONCERN, true);
}

void
bson_parser_read_prefs (bson_parser_t *bp, mongoc_read_prefs_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "readPreference", (void *) out, BSON_PARSER_READ_PREFS, false);
}

void
bson_parser_read_prefs_optional (bson_parser_t *bp, mongoc_read_prefs_t **out)
{
   *out = NULL;
   bson_parser_add_entry (bp, "readPreference", (void *) out, BSON_PARSER_READ_PREFS, true);
}

void
marshal_error (const char *key, bson_type_t btype, bson_parser_type_t ptype, bson_error_t *error)
{
   test_set_error (
      error, "expecting %s for '%s' but got: %s", parser_type_to_string (ptype), key, bson_type_to_string (btype));
}

bool
entry_marshal (bson_parser_entry_t *entry, bson_iter_t *iter, bson_error_t *error)
{
   bool ret = false;
   const char *key;
   bson_type_t btype;
   bson_parser_type_t ptype;

   ptype = entry->ptype;
   key = bson_iter_key (iter);
   btype = bson_iter_type (iter);

   if (ptype == BSON_PARSER_UTF8) {
      char **out = (char **) entry->out;

      if (btype != BSON_TYPE_UTF8) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      *out = bson_strdup (bson_iter_utf8 (iter, NULL));
   }

   else if (ptype == BSON_PARSER_INT) {
      int64_t **out = (int64_t **) entry->out;

      if (btype != BSON_TYPE_INT32 && btype != BSON_TYPE_INT64) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      *out = bson_malloc0 (sizeof (int64_t));
      **out = bson_iter_as_int64 (iter);
   }

   else if (ptype == BSON_PARSER_BOOL) {
      bool **out = (bool **) entry->out;

      if (btype != BSON_TYPE_BOOL) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      *out = bson_malloc0 (sizeof (bool));
      **out = bson_iter_bool (iter);
   }

   else if (ptype == BSON_PARSER_DOC) {
      bson_t tmp;
      bson_t **out = (bson_t **) entry->out;

      if (btype != BSON_TYPE_DOCUMENT) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_copy (&tmp);
   }

   else if (ptype == BSON_PARSER_ARRAY) {
      bson_t tmp;
      bson_t **out = (bson_t **) entry->out;

      if (btype != BSON_TYPE_ARRAY) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_copy (&tmp);
   }

   else if (ptype == BSON_PARSER_ARRAY_OR_DOC) {
      bson_t tmp;
      bson_t **out = (bson_t **) entry->out;

      if (btype != BSON_TYPE_ARRAY && btype != BSON_TYPE_DOCUMENT) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_copy (&tmp);
   }

   else if (ptype == BSON_PARSER_ANY) {
      bson_val_t **out = (bson_val_t **) entry->out;

      *out = bson_val_from_iter (iter);
   } else if (ptype == BSON_PARSER_WRITE_CONCERN) {
      bson_t tmp;
      mongoc_write_concern_t **out = (mongoc_write_concern_t **) entry->out;

      if (btype != BSON_TYPE_DOCUMENT) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_to_write_concern (&tmp, error);
      if (!*out) {
         goto done;
      }
   }

   else if (ptype == BSON_PARSER_READ_CONCERN) {
      bson_t tmp;
      mongoc_read_concern_t **out = (mongoc_read_concern_t **) entry->out;
      if (btype != BSON_TYPE_DOCUMENT) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_to_read_concern (&tmp, error);
   }

   else if (ptype == BSON_PARSER_READ_PREFS) {
      bson_t tmp;
      mongoc_read_prefs_t **out = (mongoc_read_prefs_t **) entry->out;
      if (btype != BSON_TYPE_DOCUMENT) {
         marshal_error (key, btype, ptype, error);
         goto done;
      }

      bson_iter_bson (iter, &tmp);
      *out = bson_to_read_prefs (&tmp, error);
   } else {
      test_set_error (error, "unimplemented parser type: %s", parser_type_to_string (ptype));
      goto done;
   }

   ret = true;
done:
   return ret;
}

bool
bson_parser_parse (bson_parser_t *parser, bson_t *in, bson_error_t *error)
{
   bson_iter_t iter;
   bson_parser_entry_t *entry = NULL;

   /* Check that document is not null. */
   if (in) {
      BSON_FOREACH (in, iter)
      {
         const char *key = bson_iter_key (&iter);
         bson_parser_entry_t *matched = NULL;
         /* Check for a corresponding entry. */
         LL_FOREACH (parser->entries, entry)
         {
            if (0 == strcmp (entry->key, key)) {
               matched = entry;
               break;
            }
         }

         if (matched) {
            if (!entry_marshal (entry, &iter, error)) {
               return false;
            }
            entry->set = true;
         } else if (parser->allow_extra) {
            BSON_APPEND_VALUE (parser->extra, key, bson_iter_value (&iter));
         } else {
            test_set_error (error, "Extra field '%s' found parsing: %s", key, tmp_json (in));
            return false;
         }
      }
   }


   /* Check if there are any unparsed required entries. */
   LL_FOREACH (parser->entries, entry)
   {
      if (!entry->optional && !entry->set) {
         test_set_error (error, "Required field '%s' was not found parsing: %s", entry->key, tmp_json (in));
         return false;
      }
   }
   return true;
}

void
bson_parser_parse_or_assert (bson_parser_t *parser, bson_t *in)
{
   bson_error_t error;

   if (!bson_parser_parse (parser, in, &error)) {
      test_error ("Unable to parse: %s: %s", error.message, tmp_json (in));
   }
}
