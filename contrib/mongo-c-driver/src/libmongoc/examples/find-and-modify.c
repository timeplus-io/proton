#include <mongoc/mongoc.h>
#include <stdio.h>


int
main (void)
{
   mongoc_collection_t *collection;
   mongoc_client_t *client;
   const char *uri_string = "mongodb://127.0.0.1:27017/?appname=find-and-modify-example";
   mongoc_uri_t *uri;
   bson_error_t error;
   bson_t *query;
   bson_t *update;
   bson_t reply;
   char *str;

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

   /*
    * Build our query, {"cmpxchg": 1}
    */
   query = BCON_NEW ("cmpxchg", BCON_INT32 (1));

   /*
    * Build our update. {"$set": {"cmpxchg": 2}}
    */
   update = BCON_NEW ("$set", "{", "cmpxchg", BCON_INT32 (2), "}");

   /*
    * Submit the findAndModify.
    */
   if (!mongoc_collection_find_and_modify (collection, query, NULL, update, NULL, false, false, true, &reply, &error)) {
      fprintf (stderr, "find_and_modify() failure: %s\n", error.message);
      return EXIT_FAILURE;
   }

   /*
    * Print the result as JSON.
    */
   str = bson_as_canonical_extended_json (&reply, NULL);
   printf ("%s\n", str);
   bson_free (str);

   /*
    * Cleanup.
    */
   bson_destroy (query);
   bson_destroy (update);
   bson_destroy (&reply);
   mongoc_collection_destroy (collection);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
