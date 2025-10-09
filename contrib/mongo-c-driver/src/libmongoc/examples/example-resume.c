#include <mongoc/mongoc.h>

/* An example implementation of custom resume logic in a change stream.
 * example-resume starts a client-wide change stream and persists the resume
 * token in a file "resume-token.json". On restart, if "resume-token.json"
 * exists, the change stream starts watching after the persisted resume token.
 *
 * This behavior allows a user to exit example-resume, and restart it later
 * without missing any change events.
 */
#include <unistd.h>

static const char *RESUME_TOKEN_PATH = "resume-token.json";

static bool
_save_resume_token (const bson_t *doc)
{
   FILE *file_stream;
   bson_iter_t iter;
   bson_t resume_token_doc;
   char *as_json = NULL;
   size_t as_json_len;
   ssize_t r, n_written;
   const bson_value_t *resume_token;

   if (!bson_iter_init_find (&iter, doc, "_id")) {
      fprintf (stderr, "reply does not contain operationTime.");
      return false;
   }
   resume_token = bson_iter_value (&iter);
   /* store the resume token in a document, { resumeAfter: <resume token> }
    * which we can later append easily. */
   file_stream = fopen (RESUME_TOKEN_PATH, "w+");
   if (!file_stream) {
      fprintf (stderr, "failed to open %s for writing\n", RESUME_TOKEN_PATH);
      return false;
   }
   bson_init (&resume_token_doc);
   BSON_APPEND_VALUE (&resume_token_doc, "resumeAfter", resume_token);
   as_json = bson_as_canonical_extended_json (&resume_token_doc, &as_json_len);
   bson_destroy (&resume_token_doc);
   n_written = 0;
   while (n_written < as_json_len) {
      r = fwrite ((void *) (as_json + n_written), sizeof (char), as_json_len - n_written, file_stream);
      if (r == -1) {
         fprintf (stderr, "failed to write to %s\n", RESUME_TOKEN_PATH);
         bson_free (as_json);
         fclose (file_stream);
         return false;
      }
      n_written += r;
   }

   bson_free (as_json);
   fclose (file_stream);
   return true;
}

bool
_load_resume_token (bson_t *opts)
{
   bson_error_t error;
   bson_json_reader_t *reader;
   bson_t doc;

   /* if the file does not exist, skip. */
   if (-1 == access (RESUME_TOKEN_PATH, R_OK)) {
      return true;
   }
   reader = bson_json_reader_new_from_file (RESUME_TOKEN_PATH, &error);
   if (!reader) {
      fprintf (stderr, "failed to open %s for reading: %s\n", RESUME_TOKEN_PATH, error.message);
      return false;
   }

   bson_init (&doc);
   if (-1 == bson_json_reader_read (reader, &doc, &error)) {
      fprintf (stderr, "failed to read doc from %s\n", RESUME_TOKEN_PATH);
      bson_destroy (&doc);
      bson_json_reader_destroy (reader);
      return false;
   }

   printf ("found cached resume token in %s, resuming change stream.\n", RESUME_TOKEN_PATH);

   bson_concat (opts, &doc);
   bson_destroy (&doc);
   bson_json_reader_destroy (reader);
   return true;
}

int
main (void)
{
   int exit_code = EXIT_FAILURE;
   const char *uri_string;
   mongoc_uri_t *uri = NULL;
   bson_error_t error;
   mongoc_client_t *client = NULL;
   bson_t pipeline = BSON_INITIALIZER;
   bson_t opts = BSON_INITIALIZER;
   mongoc_change_stream_t *stream = NULL;
   const bson_t *doc;

   const int max_time = 30; /* max amount of time, in seconds, that
                               mongoc_change_stream_next can block. */

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

   if (!_load_resume_token (&opts)) {
      goto cleanup;
   }
   BSON_APPEND_INT64 (&opts, "maxAwaitTimeMS", max_time * 1000);

   printf ("listening for changes on the client (max %d seconds).\n", max_time);
   stream = mongoc_client_watch (client, &pipeline, &opts);

   while (mongoc_change_stream_next (stream, &doc)) {
      char *as_json;

      as_json = bson_as_canonical_extended_json (doc, NULL);
      printf ("change received: %s\n", as_json);
      bson_free (as_json);
      if (!_save_resume_token (doc)) {
         goto cleanup;
      }
   }

   exit_code = EXIT_SUCCESS;

cleanup:
   mongoc_uri_destroy (uri);
   bson_destroy (&pipeline);
   bson_destroy (&opts);
   mongoc_change_stream_destroy (stream);
   mongoc_client_destroy (client);
   mongoc_cleanup ();
   return exit_code;
}
