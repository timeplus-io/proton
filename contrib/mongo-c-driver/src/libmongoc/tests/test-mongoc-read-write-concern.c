#include <mongoc/mongoc.h>
#include <bson/bson-types.h>

#include "mongoc/mongoc-util-private.h"
#include "mongoc/mongoc-write-concern-private.h"
#include "mongoc/mongoc-read-concern-private.h"
#include "mongoc/mongoc-opts-helpers-private.h"
#include "json-test.h"
#include "test-libmongoc.h"


static void
compare_write_concern (const mongoc_write_concern_t *wc_correct, const mongoc_write_concern_t *wc)
{
   ASSERT_CMPINT32 (wc_correct->w, ==, wc->w);
   ASSERT_CMPINT64 (wc_correct->wtimeout, ==, wc->wtimeout);
   ASSERT_CMPINT (wc_correct->journal, ==, wc->journal);
}


static void
compare_read_concern (const mongoc_read_concern_t *rc_correct, const mongoc_read_concern_t *rc)
{
   ASSERT_CMPSTR (rc_correct->level, rc->level);
}

mongoc_write_concern_t *
convert_write_concern (const bson_t *wc_doc)
{
   mongoc_write_concern_t *wc;
   bson_iter_t iter;
   const char *key;

   wc = mongoc_write_concern_new ();
   BSON_ASSERT (bson_iter_init (&iter, wc_doc));

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);

      if (strcmp (key, "w") == 0) {
         if (BSON_ITER_HOLDS_UTF8 (&iter)) {
            if (strcmp (bson_lookup_utf8 (wc_doc, "w"), "majority") == 0) {
               mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
            } else {
               mongoc_write_concern_set_wtag (wc, bson_lookup_utf8 (wc_doc, "w"));
            }
         } else {
            if (bson_lookup_int32 (wc_doc, "w") < 0) {
               goto invalid;
            }
            mongoc_write_concern_set_w (wc, bson_lookup_int32 (wc_doc, "w"));
         }
      } else if (strcmp (key, "wtimeoutMS") == 0) {
         if (bson_lookup_int32 (wc_doc, "wtimeoutMS") < 0) {
            goto invalid;
         }
         mongoc_write_concern_set_wtimeout_int64 (wc, bson_lookup_int32 (wc_doc, "wtimeoutMS"));
      } else if (strcmp (key, "journal") == 0) {
         mongoc_write_concern_set_journal (wc, bson_iter_value (&iter)->value.v_bool);
      }
   }

   if (wc->w == 0 && wc->journal == 1) {
      goto invalid;
   }

   return wc;

invalid:
   mongoc_write_concern_destroy (wc);
   return NULL;
}

mongoc_read_concern_t *
convert_read_concern (const bson_t *rc_doc)
{
   mongoc_read_concern_t *rc;
   bson_iter_t iter;
   const char *key;

   rc = mongoc_read_concern_new ();
   BSON_ASSERT (bson_iter_init (&iter, rc_doc));

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);

      if (strcmp (key, "level") == 0) {
         if (BSON_ITER_HOLDS_UTF8 (&iter)) {
            mongoc_read_concern_set_level (rc, bson_lookup_utf8 (rc_doc, "level"));
         } else {
            goto invalid;
         }
      }
   }

   return rc;

invalid:
   mongoc_read_concern_destroy (rc);
   return NULL;
}


static void
test_rw_concern_uri (bson_t *scenario)
{
   bson_iter_t scenario_iter;
   bson_iter_t test_iter;
   bson_t test;
   const char *description;
   const char *uri_str;
   bool valid;
   mongoc_uri_t *uri;
   bson_t rc_doc;
   bson_t wc_doc;
   const mongoc_read_concern_t *rc;
   const mongoc_write_concern_t *wc;
   mongoc_write_concern_t *wc_correct;
   mongoc_read_concern_t *rc_correct;

   /* initialize tests with the scenario */
   BSON_ASSERT (bson_iter_init_find (&scenario_iter, scenario, "tests"));
   BSON_ASSERT (bson_iter_recurse (&scenario_iter, &test_iter));

   while (bson_iter_next (&test_iter)) {
      bson_iter_bson (&test_iter, &test);

      description = bson_lookup_utf8 (&test, "description");
      uri_str = bson_lookup_utf8 (&test, "uri");
      valid = _mongoc_lookup_bool (&test, "valid", true);

      if (_mongoc_lookup_bool (&test, "warning", false)) {
         test_error ("update the \"%s\" test to handle warning: true", description);
      }

      uri = mongoc_uri_new_with_error (uri_str, NULL);
      if (!valid) {
         BSON_ASSERT (!uri);
         continue;
      }

      BSON_ASSERT (uri);

      if (bson_has_field (&test, "readConcern")) {
         rc = mongoc_uri_get_read_concern (uri);
         bson_lookup_doc (&test, "readConcern", &rc_doc);
         rc_correct = convert_read_concern (&rc_doc);
         compare_read_concern (rc_correct, rc);
         mongoc_read_concern_destroy (rc_correct);
      }

      if (bson_has_field (&test, "writeConcern")) {
         wc = mongoc_uri_get_write_concern (uri);
         bson_lookup_doc (&test, "writeConcern", &wc_doc);
         wc_correct = convert_write_concern (&wc_doc);
         compare_write_concern (wc_correct, wc);
         mongoc_write_concern_destroy (wc_correct);
      }

      mongoc_uri_destroy (uri);
   }
}

static void
test_rw_concern_document (bson_t *scenario)
{
   bson_iter_t scenario_iter;
   bson_iter_t test_iter;
   bson_t test;
   bool valid;
   bson_t rc_doc;
   bson_t wc_doc;
   mongoc_write_concern_t *wc;
   mongoc_read_concern_t *rc;
   const bson_t *wc_doc_result;
   const bson_t *rc_doc_result;
   bson_t rc_doc_correct;
   bson_t wc_doc_correct;

   BSON_ASSERT (bson_iter_init_find (&scenario_iter, scenario, "tests"));
   BSON_ASSERT (bson_iter_recurse (&scenario_iter, &test_iter));

   while (bson_iter_next (&test_iter)) {
      bson_iter_bson (&test_iter, &test);
      valid = _mongoc_lookup_bool (&test, "valid", true);

      if (bson_has_field (&test, "readConcern")) {
         bson_lookup_doc (&test, "readConcern", &rc_doc);
         rc = convert_read_concern (&rc_doc);
      } else {
         rc = mongoc_read_concern_new ();
      }

      if (bson_has_field (&test, "writeConcern")) {
         bson_lookup_doc (&test, "writeConcern", &wc_doc);
         wc = convert_write_concern (&wc_doc);
      } else {
         wc = mongoc_write_concern_new ();
      }

      if (!valid) {
         BSON_ASSERT (rc == NULL || wc == NULL);
         mongoc_write_concern_destroy (wc);
         mongoc_read_concern_destroy (rc);
         continue;
      }

      if (bson_has_field (&test, "readConcernDocument")) {
         bson_lookup_doc (&test, "readConcernDocument", &rc_doc_correct);
         rc_doc_result = _mongoc_read_concern_get_bson (rc);
         match_bson (&rc_doc_correct, rc_doc_result, false /* is_command */);
      }

      if (bson_has_field (&test, "writeConcernDocument")) {
         bson_lookup_doc (&test, "writeConcernDocument", &wc_doc_correct);
         wc_doc_result = _mongoc_write_concern_get_bson (wc);
         match_bson (&wc_doc_correct, wc_doc_result, false);
      }

      mongoc_write_concern_destroy (wc);
      mongoc_read_concern_destroy (rc);
   }
}

void
test_read_write_concern_install (TestSuite *suite)
{
   install_json_test_suite (suite, JSON_DIR, "read_write_concern/connection-string", &test_rw_concern_uri);
   install_json_test_suite (suite, JSON_DIR, "read_write_concern/document", &test_rw_concern_document);
}
