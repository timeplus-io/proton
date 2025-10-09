/*
 * Copyright 2020-present MongoDB, Inc.
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

#include <stdlib.h>

#if defined(__linux__)

#include <mongoc/mongoc.h>
#include <stdio.h>
#include <signal.h>
#include "TestSuite.h"

#ifndef SIGSTOP
#define SIGSTOP 19
#endif
static char *ca_file;

static int
ping (void)
{
   mongoc_client_t *client;
   mongoc_database_t *database;
   bson_t reply;
   bson_error_t error;
   bson_t ping;
   char *uri;
   int ret = EXIT_FAILURE;

   uri = bson_strdup_printf ("mongodb://localhost/?tls=true&tlsCAFile=%s", ca_file);
   ASSERT ((client = mongoc_client_new (uri)));

   bson_init (&ping);
   bson_append_int32 (&ping, "ping", 4, 1);
   database = mongoc_client_get_database (client, "cache");

   if (mongoc_database_command_with_opts (database, &ping, NULL, NULL, &reply, &error)) {
      MONGOC_DEBUG ("Ping success\n");
      ret = EXIT_SUCCESS;
   } else {
      MONGOC_DEBUG ("Ping failure: %s\n", error.message);
      ASSERT_ERROR_CONTAINS (
         error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "TLS handshake failed");
   }

   bson_free (uri);
   bson_destroy (&ping);
   bson_destroy (&reply);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);

   return ret;
}
#endif

int
main (int argc, char *argv[])
{
#if defined(__linux__)
   if (argc != 2) {
      fprintf (stderr, "usage: %s CA_FILE_PATH\n", argv[0]);
      return EXIT_FAILURE;
   }

   ca_file = argv[1];

   mongoc_init ();

   ASSERT (ping () == EXIT_FAILURE);
   raise (SIGSTOP);
   ASSERT (ping () == EXIT_FAILURE);

   mongoc_cleanup ();
#endif
   return EXIT_SUCCESS;
}
