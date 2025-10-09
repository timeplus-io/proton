/*
 * Copyright 2013-2014 MongoDB, Inc.
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


int
main (int argc, char *argv[])
{
   mongoc_database_t *database;
   mongoc_client_t *client;
   bson_t reply;
   uint16_t port;
   bson_error_t error;
   bson_t ping;
   char *host_and_port;
   mongoc_uri_t *uri;
   char *str;
   bool r;

   if (argc < 2 || argc > 3) {
      fprintf (stderr, "usage: %s HOSTNAME [PORT]\n", argv[0]);
      return EXIT_FAILURE;
   }

   mongoc_init ();

   port = (argc == 3) ? atoi (argv[2]) : 27017;

   if (!strncmp (argv[1], "mongodb://", 10) || !strncmp (argv[1], "mongodb+srv://", 14)) {
      host_and_port = bson_strdup (argv[1]);
   } else {
      host_and_port = bson_strdup_printf ("mongodb://%s:%hu", argv[1], port);
   }

   uri = mongoc_uri_new_with_error (host_and_port, &error);
   if (!uri) {
      fprintf (stderr,
               "failed to parse URI: %s\n"
               "error message:       %s\n",
               host_and_port,
               error.message);
      return EXIT_FAILURE;
   }
   bson_free (host_and_port);

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      return EXIT_FAILURE;
   }

   mongoc_client_set_error_api (client, 2);

   bson_init (&ping);
   bson_append_int32 (&ping, "ping", 4, 1);
   database = mongoc_client_get_database (client, "test");
   r = mongoc_database_command_with_opts (database, &ping, NULL, NULL, &reply, &error);

   if (r) {
      str = bson_as_canonical_extended_json (&reply, NULL);
      fprintf (stdout, "%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Ping failure: %s\n", error.message);
   }

   bson_destroy (&ping);
   bson_destroy (&reply);
   mongoc_database_destroy (database);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);

   return r ? 0 : 3;
}
