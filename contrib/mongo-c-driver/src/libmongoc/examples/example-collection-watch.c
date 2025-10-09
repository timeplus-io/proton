#include <mongoc/mongoc.h>

int
main (void)
{
   bson_t empty = BSON_INITIALIZER;
   const bson_t *doc;
   bson_t *to_insert = BCON_NEW ("x", BCON_INT32 (1));
   const bson_t *err_doc;
   bson_error_t error;
   const char *uri_string;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   mongoc_change_stream_t *stream;
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();
   bson_t opts = BSON_INITIALIZER;
   bool r;

   mongoc_init ();

   uri_string = "mongodb://"
                "localhost:27017,localhost:27018,localhost:"
                "27019/db?replicaSet=rs0";

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

   coll = mongoc_client_get_collection (client, "db", "coll");
   stream = mongoc_collection_watch (coll, &empty, NULL);

   mongoc_write_concern_set_wmajority (wc, 10000);
   mongoc_write_concern_append (wc, &opts);
   r = mongoc_collection_insert_one (coll, to_insert, &opts, NULL, &error);
   if (!r) {
      fprintf (stderr, "Error: %s\n", error.message);
      return EXIT_FAILURE;
   }

   while (mongoc_change_stream_next (stream, &doc)) {
      char *as_json = bson_as_relaxed_extended_json (doc, NULL);
      fprintf (stderr, "Got document: %s\n", as_json);
      bson_free (as_json);
   }

   if (mongoc_change_stream_error_document (stream, &error, &err_doc)) {
      if (!bson_empty (err_doc)) {
         fprintf (stderr, "Server Error: %s\n", bson_as_relaxed_extended_json (err_doc, NULL));
      } else {
         fprintf (stderr, "Client Error: %s\n", error.message);
      }
      return EXIT_FAILURE;
   }

   bson_destroy (to_insert);
   mongoc_write_concern_destroy (wc);
   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);
   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
