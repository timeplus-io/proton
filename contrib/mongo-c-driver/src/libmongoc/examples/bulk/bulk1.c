#include <assert.h>
#include <mongoc/mongoc.h>
#include <stdio.h>

static void
bulk1 (mongoc_collection_t *collection)
{
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   bson_t *doc;
   bson_t reply;
   char *str;
   bool ret;
   int i;

   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

   for (i = 0; i < 10000; i++) {
      doc = BCON_NEW ("i", BCON_INT32 (i));
      mongoc_bulk_operation_insert (bulk, doc);
      bson_destroy (doc);
   }

   ret = mongoc_bulk_operation_execute (bulk, &reply, &error);

   str = bson_as_canonical_extended_json (&reply, NULL);
   printf ("%s\n", str);
   bson_free (str);

   if (!ret) {
      fprintf (stderr, "Error: %s\n", error.message);
   }

   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
}

int
main (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   const char *uri_string = "mongodb://localhost/?appname=bulk1-example";
   mongoc_uri_t *uri;
   bson_error_t error;

   mongoc_init ();

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
   collection = mongoc_client_get_collection (client, "test", "test");

   bulk1 (collection);

   mongoc_uri_destroy (uri);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
