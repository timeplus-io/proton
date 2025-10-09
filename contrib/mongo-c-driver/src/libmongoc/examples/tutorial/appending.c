#include <bson/bson.h>

int
main (void)
{
   struct tm born = {0};
   struct tm died = {0};
   const char *lang_names[] = {"MATH-MATIC", "FLOW-MATIC", "COBOL"};
   const char *schools[] = {"Vassar", "Yale"};
   const char *degrees[] = {"BA", "PhD"};
   uint32_t i;
   bson_t *document;
   bson_t child;
   bson_array_builder_t *bab;
   char *str;

   document = bson_new ();

   /*
    * Append { "born" : ISODate("1906-12-09") } to the document.
    * Passing -1 for the length argument tells libbson to calculate the
    * string length.
    */
   born.tm_year = 6; /* years are 1900-based */
   born.tm_mon = 11; /* months are 0-based */
   born.tm_mday = 9;
   bson_append_date_time (document, "born", -1, mktime (&born) * 1000);

   /*
    * Append { "died" : ISODate("1992-01-01") } to the document.
    */
   died.tm_year = 92;
   died.tm_mon = 0;
   died.tm_mday = 1;

   /*
    * For convenience, this macro passes length -1 by default.
    */
   BSON_APPEND_DATE_TIME (document, "died", mktime (&died) * 1000);

   /*
    * Append a subdocument.
    */
   BSON_APPEND_DOCUMENT_BEGIN (document, "name", &child);
   BSON_APPEND_UTF8 (&child, "first", "Grace");
   BSON_APPEND_UTF8 (&child, "last", "Hopper");
   bson_append_document_end (document, &child);

   /*
    * Append array of strings. Generate keys "0", "1", "2".
    */
   BSON_APPEND_ARRAY_BUILDER_BEGIN (document, "languages", &bab);
   for (i = 0; i < sizeof lang_names / sizeof (char *); ++i) {
      bson_array_builder_append_utf8 (bab, lang_names[i], -1);
   }
   bson_append_array_builder_end (document, bab);

   /*
    * Array of subdocuments:
    *    degrees: [ { degree: "BA", school: "Vassar" }, ... ]
    */
   BSON_APPEND_ARRAY_BUILDER_BEGIN (document, "degrees", &bab);
   for (i = 0; i < sizeof degrees / sizeof (char *); ++i) {
      bson_array_builder_append_document_begin (bab, &child);
      BSON_APPEND_UTF8 (&child, "degree", degrees[i]);
      BSON_APPEND_UTF8 (&child, "school", schools[i]);
      bson_array_builder_append_document_end (bab, &child);
   }
   bson_append_array_builder_end (document, bab);

   /*
    * Print the document as a JSON string.
    */
   str = bson_as_canonical_extended_json (document, NULL);
   printf ("%s\n", str);
   bson_free (str);

   /*
    * Clean up allocated bson documents.
    */
   bson_destroy (document);
   return 0;
}
