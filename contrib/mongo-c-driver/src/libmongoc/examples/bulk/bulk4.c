#include <assert.h>
#include <mongoc/mongoc.h>
#include <stdio.h>

static void
bulk4 (mongoc_collection_t *collection)
{
   bson_t opts = BSON_INITIALIZER;
   mongoc_write_concern_t *wc;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   bson_t *doc;
   bson_t reply;
   char *str;
   bool ret;

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 4);
   mongoc_write_concern_set_wtimeout_int64 (wc, 100); /* milliseconds */
   mongoc_write_concern_append (wc, &opts);

   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, &opts);

   /* Two inserts */
   doc = BCON_NEW ("_id", BCON_INT32 (10));
   mongoc_bulk_operation_insert (bulk, doc);
   bson_destroy (doc);

   doc = BCON_NEW ("_id", BCON_INT32 (11));
   mongoc_bulk_operation_insert (bulk, doc);
   bson_destroy (doc);

   ret = mongoc_bulk_operation_execute (bulk, &reply, &error);

   str = bson_as_canonical_extended_json (&reply, NULL);
   printf ("%s\n", str);
   bson_free (str);

   if (!ret) {
      printf ("Error: %s\n", error.message);
   }

   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_write_concern_destroy (wc);
   bson_destroy (&opts);
}

int
main (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   const char *uri_string = "mongodb://localhost/?appname=bulk4-example";
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

   bulk4 (collection);

   mongoc_uri_destroy (uri);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
