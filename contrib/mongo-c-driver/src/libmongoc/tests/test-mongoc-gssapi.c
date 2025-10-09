/*
 * Copyright 2017-present MongoDB, Inc.
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
#include <mongoc/mongoc-thread-private.h>


static const char *GSSAPI_HOST = "MONGOC_TEST_GSSAPI_HOST";
static const char *GSSAPI_USER = "MONGOC_TEST_GSSAPI_USER";

#define NTHREADS 10
#define NLOOPS 10


char *
_getenv (const char *name)
{
#ifdef _MSC_VER
   char buf[1024];
   size_t buflen;

   if ((0 == getenv_s (&buflen, buf, sizeof buf, name)) && buflen) {
      return bson_strdup (buf);
   } else {
      return NULL;
   }
#else
   char *const value = getenv (name);
   if (value && strlen (value)) {
      return bson_strdup (value);
   } else {
      return NULL;
   }
#endif
}


struct closure_t {
   mongoc_client_pool_t *pool;
   int finished;
   bson_mutex_t mutex;
};


static BSON_THREAD_FUN (gssapi_kerberos_worker, data)
{
   struct closure_t *closure = (struct closure_t *) data;
   mongoc_client_pool_t *pool = closure->pool;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;
   bson_t query = BSON_INITIALIZER;
   int i;

   for (i = 0; i < NLOOPS; i++) {
      bson_t *cmd = BCON_NEW ("ping", BCON_INT32 (1));

      client = mongoc_client_pool_pop (pool);
      if (!mongoc_client_command_with_opts (client, "test", cmd, NULL, NULL, NULL, &error)) {
         fflush (stdout);
         fprintf (stderr, "ping command failed: %s\n", error.message);
         fflush (stderr);
         abort ();
      }
      bson_destroy (cmd);
      collection = mongoc_client_get_collection (client, "kerberos", "test");
      cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &query, NULL, NULL);

      if (!mongoc_cursor_next (cursor, &doc) && mongoc_cursor_error (cursor, &error)) {
         fflush (stdout);
         fprintf (stderr, "Cursor Failure: %s\n", error.message);
         fflush (stderr);
         abort ();
      }

      mongoc_cursor_destroy (cursor);
      mongoc_collection_destroy (collection);
      mongoc_client_pool_push (pool, client);
   }

   bson_destroy (&query);

   bson_mutex_lock (&closure->mutex);
   closure->finished++;
   bson_mutex_unlock (&closure->mutex);

   BSON_THREAD_RETURN;
}


int
main (void)
{
   char *host = _getenv (GSSAPI_HOST);
   char *user = _getenv (GSSAPI_USER);
   char *uri_str;
   mongoc_uri_t *uri;
   struct closure_t closure = {0};
   int i;
   bson_thread_t threads[NTHREADS];
   int r;

   mongoc_init ();

   if (!host || !user) {
      fprintf (stderr, "%s and %s must be defined in environment\n", GSSAPI_HOST, GSSAPI_USER);
      return 1;
   }

   bson_mutex_init (&closure.mutex);

   uri_str = bson_strdup_printf ("mongodb://%s@%s/?authMechanism=GSSAPI&connectTimeoutMS=30000", user, host);

   uri = mongoc_uri_new (uri_str);
   closure.pool = mongoc_client_pool_new (uri);

   for (i = 0; i < NTHREADS; i++) {
      r = mcommon_thread_create (&threads[i], gssapi_kerberos_worker, (void *) &closure);
      BSON_ASSERT (r == 0);
   }

   for (i = 0; i < NTHREADS; i++) {
      mcommon_thread_join (threads[i]);
   }

   bson_mutex_lock (&closure.mutex);
   BSON_ASSERT (NTHREADS == closure.finished);
   bson_mutex_unlock (&closure.mutex);

   mongoc_client_pool_destroy (closure.pool);
   bson_mutex_destroy (&closure.mutex);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   bson_free (host);
   bson_free (user);

   mongoc_cleanup ();

   return 0;
}
