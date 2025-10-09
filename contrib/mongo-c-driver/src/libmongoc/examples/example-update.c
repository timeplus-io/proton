#include "mongoc/mongoc.h"

int
main (void)
{
   bson_t *to_insert = BCON_NEW ("_id", BCON_INT32 (1));
   bson_t *selector = BCON_NEW ("_id", "{", "$gt", BCON_INT32 (0), "}");
   bson_t *update = BCON_NEW ("$set", "{", "x", BCON_INT32 (1), "}");
   const bson_t *next_doc;
   char *to_str;
   bson_error_t error = {0};
   mongoc_cursor_t *cursor;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   const char *uri_string = "mongodb://localhost:27017/?appname=example-update";
   mongoc_uri_t *uri = mongoc_uri_new_with_error (uri_string, &error);

   if (!uri) {
      fprintf (stderr,
               "failed to parse URI: %s\n"
               "error message:       %s\n",
               uri_string,
               error.message);
      return EXIT_FAILURE;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      return EXIT_FAILURE;
   }

   coll = mongoc_client_get_collection (client, "db", "example_coll");

   mongoc_client_set_error_api (client, 2);
   /* insert a document */
   if (!mongoc_collection_insert_one (coll, to_insert, NULL, NULL, &error)) {
      fprintf (stderr, "insert failed: %s\n", error.message);
      return EXIT_FAILURE;
   }

   if (!mongoc_collection_update_one (coll, selector, update, NULL, NULL, &error)) {
      fprintf (stderr, "update failed: %s\n", error.message);
      return EXIT_FAILURE;
   }

   to_str = bson_as_relaxed_extended_json (to_insert, NULL);
   printf ("inserted: %s\n", to_str);
   bson_free (to_str);

   cursor = mongoc_collection_find_with_opts (coll, selector, NULL, NULL);
   BSON_ASSERT (mongoc_cursor_next (cursor, &next_doc));
   printf ("after update, collection has the following document:\n");

   to_str = bson_as_relaxed_extended_json (next_doc, NULL);
   printf ("%s\n", to_str);
   bson_free (to_str);

   BSON_ASSERT (mongoc_collection_drop (coll, NULL));

   bson_destroy (to_insert);
   bson_destroy (update);
   bson_destroy (selector);
   mongoc_collection_destroy (coll);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);

   return EXIT_SUCCESS;
}
