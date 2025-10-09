/* gcc example-client.c -o example-client $(pkg-config --cflags --libs
 * libmongoc-1.0) */

/* ./example-client [CONNECTION_STRING [COLLECTION_NAME]] */

#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

int
main (int argc, char *argv[])
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;
   const char *collection_name = "test";
   bson_t query;
   char *str;
   const char *uri_string = "mongodb://127.0.0.1/?appname=client-example";
   mongoc_uri_t *uri;

   mongoc_init ();
   if (argc > 1) {
      uri_string = argv[1];
   }

   if (argc > 2) {
      collection_name = argv[2];
   }

   uri = mongoc_uri_new_with_error (uri_string, &error);
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

   mongoc_client_set_error_api (client, 2);

   bson_init (&query);
   collection = mongoc_client_get_collection (client, "test", collection_name);
   cursor = mongoc_collection_find_with_opts (collection,
                                              &query,
                                              NULL,  /* additional options */
                                              NULL); /* read prefs, NULL for default */

   while (mongoc_cursor_next (cursor, &doc)) {
      str = bson_as_canonical_extended_json (doc, NULL);
      fprintf (stdout, "%s\n", str);
      bson_free (str);
   }

   if (mongoc_cursor_error (cursor, &error)) {
      fprintf (stderr, "Cursor Failure: %s\n", error.message);
      return EXIT_FAILURE;
   }

   bson_destroy (&query);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
