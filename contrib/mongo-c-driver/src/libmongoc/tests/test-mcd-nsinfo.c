#include <mongoc/mcd-nsinfo.h>

#include "TestSuite.h"
#include "test-conveniences.h" // ASSERT_MATCH

static void
test_nsinfo_works (void)
{
   mcd_nsinfo_t *nsinfo = mcd_nsinfo_new ();

   // Add several namespaces.
   bson_error_t error;
   ASSERT_OR_PRINT (0 <= mcd_nsinfo_append (nsinfo, "db.coll1", &error), error);
   ASSERT_OR_PRINT (0 <= mcd_nsinfo_append (nsinfo, "db.coll2", &error), error);
   ASSERT_OR_PRINT (0 <= mcd_nsinfo_append (nsinfo, "db.coll3", &error), error);

   // Check resulting indexes.
   ASSERT_CMPINT32 (0, ==, mcd_nsinfo_find (nsinfo, "db.coll1"));
   ASSERT_CMPINT32 (1, ==, mcd_nsinfo_find (nsinfo, "db.coll2"));
   ASSERT_CMPINT32 (2, ==, mcd_nsinfo_find (nsinfo, "db.coll3"));
   ASSERT_CMPINT32 (-1, ==, mcd_nsinfo_find (nsinfo, "db.doesnotexist"));

   // Check the resulting document sequence.
   {
      const mongoc_buffer_t *document_sequence = mcd_nsinfo_as_document_sequence (nsinfo);
      bson_reader_t *reader = bson_reader_new_from_data (document_sequence->data, document_sequence->len);
      bool reached_eof = false;
      const bson_t *bson;
      bson = bson_reader_read (reader, &reached_eof);
      ASSERT_MATCH (bson, BSON_STR ({"ns" : "db.coll1"}));
      bson = bson_reader_read (reader, &reached_eof);
      ASSERT_MATCH (bson, BSON_STR ({"ns" : "db.coll2"}));
      bson = bson_reader_read (reader, &reached_eof);
      ASSERT_MATCH (bson, BSON_STR ({"ns" : "db.coll3"}));
      bson = bson_reader_read (reader, &reached_eof);
      ASSERT (!bson);
      ASSERT (reached_eof);
      bson_reader_destroy (reader);
   }

   mcd_nsinfo_destroy (nsinfo);
}

static void
test_nsinfo_handles_100k_namespaces (void)
{
   // Test repeated finding and adding 100,000 unique namespaces.
   // A `bulkWrite` command supports a maximum of maxWriteBatchSize unique namespaces (currently 100,000).
   const size_t ns_count = 100000;
   bson_error_t error;
   mcd_nsinfo_t *nsinfo = mcd_nsinfo_new ();
   for (size_t i = 0; i < ns_count; i++) {
      char *ns = bson_strdup_printf ("db.coll%zu", i);
      ASSERT_CMPINT32 (-1, ==, mcd_nsinfo_find (nsinfo, ns));
      ASSERT_OR_PRINT (0 <= mcd_nsinfo_append (nsinfo, ns, &error), error);
      bson_free (ns);
   }

   // Check count of resulting document sequence.
   {
      size_t count = 0;
      const mongoc_buffer_t *document_sequence = mcd_nsinfo_as_document_sequence (nsinfo);
      bson_reader_t *reader = bson_reader_new_from_data (document_sequence->data, document_sequence->len);

      while (true) {
         bool reached_eof = false;
         const bson_t *bson = bson_reader_read (reader, &reached_eof);
         if (bson == NULL) {
            ASSERT (reached_eof);
            break;
         }
         count++;
      }
      ASSERT_CMPSIZE_T (count, ==, ns_count);
      bson_reader_destroy (reader);
   }

   mcd_nsinfo_destroy (nsinfo);
}

static void
test_nsinfo_calculates_bson_size (void)
{
   bson_t *expect = tmp_bson ("{'ns': 'foo.bar'}");
   uint32_t got = mcd_nsinfo_get_bson_size ("foo.bar");
   ASSERT_CMPUINT32 (got, ==, expect->len);
}

void
test_mcd_nsinfo_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/nsinfo/works", test_nsinfo_works);
   TestSuite_Add (suite, "/nsinfo/handles_100k_namespaces", test_nsinfo_handles_100k_namespaces);
   TestSuite_Add (suite, "/nsinfo/calculates_bson_size", test_nsinfo_calculates_bson_size);
}
