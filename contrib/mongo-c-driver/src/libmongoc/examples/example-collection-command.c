// BEGIN:mongoc_collection_command_simple
#include <bson/bson.h>
#include <mongoc/mongoc.h>
#include <stdio.h>

static void
do_ping (mongoc_collection_t *collection)
{
   bson_error_t error;
   bson_t *cmd;
   bson_t reply;
   char *str;

   cmd = BCON_NEW ("ping", BCON_INT32 (1));

   if (mongoc_collection_command_simple (collection, cmd, NULL, &reply, &error)) {
      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("Got reply: %s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: %s\n", error.message);
   }

   bson_destroy (&reply);
   bson_destroy (cmd);
}
// END:mongoc_collection_command_simple

int
main (int argc, char **argv)
{
   bson_error_t error;
   char *uri_string;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;

   mongoc_init ();

   if (argc != 2) {
      MONGOC_ERROR ("Error: expected URI argument.\n"
                    "Usage: %s <MongoDB URI>\n"
                    "Example: %s mongodb://localhost:27017",
                    argv[0],
                    argv[0]);
      return EXIT_FAILURE;
   }
   uri_string = argv[1];
   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      MONGOC_ERROR ("failed to parse URI: %s\nError: %s", uri_string, error.message);
      return EXIT_FAILURE;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      MONGOC_ERROR ("failed to create client");
      return EXIT_FAILURE;
   }

   coll = mongoc_client_get_collection (client, "db", "coll");
   do_ping (coll);
   mongoc_collection_destroy (coll);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mongoc_cleanup ();
   return EXIT_SUCCESS;
}
