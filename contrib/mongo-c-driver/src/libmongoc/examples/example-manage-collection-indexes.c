// example-manage-collection-indexes creates, lists and deletes an index from
// the `test.test` collection.

#include <mongoc/mongoc.h>
#include <stdlib.h> // abort

#define HANDLE_ERROR(...)                                         \
   if (1) {                                                       \
      fprintf (stderr, "Failure at %s:%d\n", __FILE__, __LINE__); \
      fprintf (stderr, __VA_ARGS__);                              \
      fprintf (stderr, "\n");                                     \
      goto fail;                                                  \
   } else                                                         \
      (void) 0

int
main (int argc, char *argv[])
{
   mongoc_client_t *client = NULL;
   const char *uri_string = "mongodb://127.0.0.1/?appname=create-indexes-example";
   mongoc_uri_t *uri = NULL;
   mongoc_collection_t *coll = NULL;
   bson_error_t error;
   bool ok = false;

   mongoc_init ();

   if (argc > 2) {
      HANDLE_ERROR ("Unexpected arguments. Expected usage: %s [CONNECTION_STRING]", argv[0]);
   }

   if (argc > 1) {
      uri_string = argv[1];
   }

   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      HANDLE_ERROR ("Failed to parse URI: %s", error.message);
   }
   client = mongoc_client_new_from_uri_with_error (uri, &error);
   if (!client) {
      HANDLE_ERROR ("Failed to create client: %s", error.message);
   }

   coll = mongoc_client_get_collection (client, "test", "test");

   {
      // Create an index ... begin
      // `keys` represents an ascending index on field `x`.
      bson_t *keys = BCON_NEW ("x", BCON_INT32 (1));
      mongoc_index_model_t *im = mongoc_index_model_new (keys, NULL /* opts */);
      if (mongoc_collection_create_indexes_with_opts (coll, &im, 1, NULL /* opts */, NULL /* reply */, &error)) {
         printf ("Successfully created index\n");
      } else {
         bson_destroy (keys);
         HANDLE_ERROR ("Failed to create index: %s", error.message);
      }
      bson_destroy (keys);
      // Create an index ... end
   }

   {
      // List indexes ... begin
      mongoc_cursor_t *cursor = mongoc_collection_find_indexes_with_opts (coll, NULL /* opts */);
      printf ("Listing indexes:\n");
      const bson_t *got;
      while (mongoc_cursor_next (cursor, &got)) {
         char *got_str = bson_as_canonical_extended_json (got, NULL);
         printf ("  %s\n", got_str);
         bson_free (got_str);
      }
      if (mongoc_cursor_error (cursor, &error)) {
         mongoc_cursor_destroy (cursor);
         HANDLE_ERROR ("Failed to list indexes: %s", error.message);
      }
      mongoc_cursor_destroy (cursor);
      // List indexes ... end
   }

   {
      // Drop an index ... begin
      bson_t *keys = BCON_NEW ("x", BCON_INT32 (1));
      char *index_name = mongoc_collection_keys_to_index_string (keys);
      if (mongoc_collection_drop_index_with_opts (coll, index_name, NULL /* opts */, &error)) {
         printf ("Successfully dropped index\n");
      } else {
         bson_free (index_name);
         bson_destroy (keys);
         HANDLE_ERROR ("Failed to drop index: %s", error.message);
      }
      bson_free (index_name);
      bson_destroy (keys);
      // Drop an index ... end
   }

   ok = true;
fail:
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mongoc_cleanup ();
   return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
