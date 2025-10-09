#include <mongoc/mongoc.h>
#include <stdio.h>

static void
bulk_collation (mongoc_collection_t *collection)
{
   mongoc_bulk_operation_t *bulk;
   bson_t *opts;
   bson_t *doc;
   bson_t *selector;
   bson_t *update;
   bson_error_t error;
   bson_t reply;
   char *str;
   uint32_t ret;

   /* insert {_id: "one"} and {_id: "One"} */
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW ("_id", BCON_UTF8 ("one"));
   mongoc_bulk_operation_insert (bulk, doc);
   bson_destroy (doc);

   doc = BCON_NEW ("_id", BCON_UTF8 ("One"));
   mongoc_bulk_operation_insert (bulk, doc);
   bson_destroy (doc);

   /* "One" normally sorts before "one"; make "one" come first */
   opts = BCON_NEW ("collation", "{", "locale", BCON_UTF8 ("en_US"), "caseFirst", BCON_UTF8 ("lower"), "}");

   /* set x=1 on the document with _id "One", which now sorts after "one" */
   update = BCON_NEW ("$set", "{", "x", BCON_INT64 (1), "}");
   selector = BCON_NEW ("_id", "{", "$gt", BCON_UTF8 ("one"), "}");
   mongoc_bulk_operation_update_one_with_opts (bulk, selector, update, opts, &error);

   ret = mongoc_bulk_operation_execute (bulk, &reply, &error);

   str = bson_as_canonical_extended_json (&reply, NULL);
   printf ("%s\n", str);
   bson_free (str);

   if (!ret) {
      printf ("Error: %s\n", error.message);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (selector);
   bson_destroy (opts);
   mongoc_bulk_operation_destroy (bulk);
}

int
main (void)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   const char *uri_string = "mongodb://localhost/?appname=bulk-collation";
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
   collection = mongoc_client_get_collection (client, "db", "collection");
   bulk_collation (collection);

   mongoc_uri_destroy (uri);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
