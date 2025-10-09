/* gcc example.c -o example $(pkg-config --cflags --libs libmongoc-1.0) */

/* ./example-scram */

#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

int
main (int argc, char *argv[])
{
   mongoc_client_t *client = NULL;
   mongoc_database_t *database = NULL;
   mongoc_collection_t *collection = NULL;
   mongoc_cursor_t *cursor = NULL;
   bson_error_t error;
   const char *uri_string = "mongodb://127.0.0.1/";
   mongoc_uri_t *uri = NULL;
   const char *authuristr;
   bson_t roles;
   bson_t query;
   const bson_t *doc;
   int exit_code = EXIT_FAILURE;

   if (argc != 2) {
      printf ("%s - [implicit|scram]\n", argv[0]);
      return exit_code;
   }

   if (strcmp (argv[1], "implicit") == 0) {
      authuristr = "mongodb://user,=:pass@127.0.0.1/test?appname=scram-example";
   } else if (strcmp (argv[1], "scram") == 0) {
      authuristr = "mongodb://user,=:pass@127.0.0.1/"
                   "test?appname=scram-example&authMechanism=SCRAM-SHA-1";
   } else {
      printf ("%s - [implicit|scram]\n", argv[0]);
      return exit_code;
   }

   mongoc_init ();

   bson_init (&roles);
   bson_init (&query);

   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      fprintf (stderr,
               "failed to parse URI: %s\n"
               "error message:       %s\n",
               uri_string,
               error.message);
      goto CLEANUP;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      goto CLEANUP;
   }

   mongoc_client_set_error_api (client, 2);

   database = mongoc_client_get_database (client, "test");

   BCON_APPEND (&roles, "0", "{", "role", "root", "db", "admin", "}");

   mongoc_database_add_user (database, "user,=", "pass", &roles, NULL, &error);

   mongoc_database_destroy (database);

   mongoc_client_destroy (client);

   client = mongoc_client_new (authuristr);

   if (!client) {
      fprintf (stderr, "failed to parse SCRAM uri\n");
      goto CLEANUP;
   }

   mongoc_client_set_error_api (client, 2);

   collection = mongoc_client_get_collection (client, "test", "test");

   cursor = mongoc_collection_find_with_opts (collection, &query, NULL, NULL);

   mongoc_cursor_next (cursor, &doc);

   if (mongoc_cursor_error (cursor, &error)) {
      fprintf (stderr, "Auth error: %s\n", error.message);
      goto CLEANUP;
   }

   exit_code = EXIT_SUCCESS;

CLEANUP:

   bson_destroy (&roles);
   bson_destroy (&query);

   if (collection) {
      mongoc_collection_destroy (collection);
   }

   if (uri) {
      mongoc_uri_destroy (uri);
   }

   if (client) {
      mongoc_client_destroy (client);
   }

   if (cursor) {
      mongoc_cursor_destroy (cursor);
   }

   mongoc_cleanup ();

   return exit_code;
}
