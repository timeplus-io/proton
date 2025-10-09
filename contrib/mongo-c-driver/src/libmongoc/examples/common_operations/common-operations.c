/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <mongoc/mongoc.h>
#include <stdio.h>


const char *COLLECTION_NAME = "things";

#include "../doc-common-insert.c"
#include "explain.c"


int
main (int argc, char *argv[])
{
   mongoc_database_t *database = NULL;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *collection = NULL;
   mongoc_uri_t *uri = NULL;
   bson_error_t error;
   char *host_and_port;
   int res = 0;

   if (argc < 2 || argc > 3) {
      fprintf (stderr,
               "usage: %s MONGOD-1-CONNECTION-STRING "
               "[MONGOD-2-HOST-NAME:MONGOD-2-PORT]\n",
               argv[0]);
      fprintf (stderr,
               "MONGOD-1-CONNECTION-STRING can be "
               "of the following forms:\n");
      fprintf (stderr, "localhost\t\t\t\tlocal machine\n");
      fprintf (stderr, "localhost:27018\t\t\t\tlocal machine on port 27018\n");
      fprintf (stderr,
               "mongodb://user:pass@localhost:27017\t"
               "local machine on port 27017, and authenticate with username "
               "user and password pass\n");
      return EXIT_FAILURE;
   }

   mongoc_init ();

   if (strncmp (argv[1], "mongodb://", 10) == 0) {
      host_and_port = bson_strdup (argv[1]);
   } else {
      host_and_port = bson_strdup_printf ("mongodb://%s", argv[1]);
   }

   uri = mongoc_uri_new_with_error (host_and_port, &error);
   if (!uri) {
      fprintf (stderr,
               "failed to parse URI: %s\n"
               "error message:       %s\n",
               host_and_port,
               error.message);
      res = EXIT_FAILURE;
      goto cleanup;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      res = EXIT_FAILURE;
      goto cleanup;
   }

   mongoc_client_set_error_api (client, 2);
   database = mongoc_client_get_database (client, "test");
   collection = mongoc_database_get_collection (database, COLLECTION_NAME);

   printf ("Inserting data\n");
   if (!insert_data (collection)) {
      res = EXIT_FAILURE;
      goto cleanup;
   }

   printf ("explain\n");
   if (!explain (collection)) {
      res = EXIT_FAILURE;
      goto cleanup;
   }

cleanup:
   if (collection) {
      mongoc_collection_destroy (collection);
   }

   if (database) {
      mongoc_database_destroy (database);
   }

   if (client) {
      mongoc_client_destroy (client);
   }

   if (uri) {
      mongoc_uri_destroy (uri);
   }

   bson_free (host_and_port);
   mongoc_cleanup ();
   return res;
}
