/* An example of starting a change stream with startAtOperationTime. */
#include <mongoc/mongoc.h>

int
main (void)
{
   int exit_code = EXIT_FAILURE;
   const char *uri_string;
   mongoc_uri_t *uri = NULL;
   bson_error_t error;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *coll = NULL;
   bson_t pipeline = BSON_INITIALIZER;
   bson_t opts = BSON_INITIALIZER;
   mongoc_change_stream_t *stream = NULL;
   bson_iter_t iter;
   const bson_t *doc;
   bson_value_t cached_operation_time = {0};
   int i;
   bool r;

   mongoc_init ();
   uri_string = "mongodb://localhost:27017/db?replicaSet=rs0";
   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      fprintf (stderr,
               "failed to parse URI: %s\n"
               "error message:       %s\n",
               uri_string,
               error.message);
      goto cleanup;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      goto cleanup;
   }

   /* insert five documents. */
   coll = mongoc_client_get_collection (client, "db", "coll");
   for (i = 0; i < 5; i++) {
      bson_t reply;
      bson_t *insert_cmd = BCON_NEW ("insert", "coll", "documents", "[", "{", "x", BCON_INT64 (i), "}", "]");

      r = mongoc_collection_write_command_with_opts (coll, insert_cmd, NULL, &reply, &error);
      bson_destroy (insert_cmd);
      if (!r) {
         bson_destroy (&reply);
         fprintf (stderr, "failed to insert: %s\n", error.message);
         goto cleanup;
      }
      if (i == 0) {
         /* cache the operation time in the first reply. */
         if (bson_iter_init_find (&iter, &reply, "operationTime")) {
            bson_value_copy (bson_iter_value (&iter), &cached_operation_time);
         } else {
            fprintf (stderr, "reply does not contain operationTime.");
            bson_destroy (&reply);
            goto cleanup;
         }
      }
      bson_destroy (&reply);
   }

   /* start a change stream at the first returned operationTime. */
   BSON_APPEND_VALUE (&opts, "startAtOperationTime", &cached_operation_time);
   stream = mongoc_collection_watch (coll, &pipeline, &opts);

   /* since the change stream started at the operation time of the first
    * insert, the five inserts are returned. */
   printf ("listening for changes on db.coll:\n");
   while (mongoc_change_stream_next (stream, &doc)) {
      char *as_json;

      as_json = bson_as_canonical_extended_json (doc, NULL);
      printf ("change received: %s\n", as_json);
      bson_free (as_json);
   }

   exit_code = EXIT_SUCCESS;

cleanup:
   mongoc_uri_destroy (uri);
   bson_destroy (&pipeline);
   bson_destroy (&opts);
   if (cached_operation_time.value_type) {
      bson_value_destroy (&cached_operation_time);
   }
   mongoc_change_stream_destroy (stream);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_cleanup ();
   return exit_code;
}
