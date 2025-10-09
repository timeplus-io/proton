/* gcc example-with-transaction-cb.c -o example-with-transaction-cb $(pkg-config
 * --cflags --libs libmongoc-1.0) */

/* ./example-with-transaction-cb [CONNECTION_STRING] */

#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * We pass this context object to mongoc_client_session_with_transaction() along
 * with our callback function. The context object will be passed to our callback
 * function when it runs, so we can access it.
 */
typedef struct {
   mongoc_collection_t *collection;
   bson_t *insert_opts;
} ctx_t;

/*
 * We pass this method as the callback to
 * mongoc_client_session_with_transaction(). The insert that this method
 * performs will happen inside of a new transaction.
 */
bool
create_and_insert_doc (mongoc_client_session_t *session,
                       void *ctx,
                       bson_t **reply, /* out param for our server reply */
                       bson_error_t *error)
{
   /*
    * mongoc_collection_insert_one requires an uninitialized, stack-allocated
    * bson_t to receive the update result
    */
   bson_t local_reply;
   bson_t *doc = NULL;
   ctx_t *data = NULL;
   bool retval;

   /*
    * Create a new bson document - { id: 1 }
    */
   doc = BCON_NEW ("_id", BCON_INT32 (1));

   printf ("Running the user-defined callback in a newly created transaction...\n");
   data = (ctx_t *) ctx;
   retval = mongoc_collection_insert_one (data->collection, doc, data->insert_opts, &local_reply, error);

   /*
    * To return to the mongoc_client_session_with_transaction() method, set
    * *reply to a new copy of our local_reply before destroying it.
    */
   *reply = bson_copy (&local_reply);
   bson_destroy (&local_reply);

   bson_destroy (doc);
   return retval;
}

int
main (int argc, char *argv[])
{
   int exit_code = EXIT_FAILURE;

   mongoc_uri_t *uri = NULL;
   const char *uri_string = "mongodb://127.0.0.1/?appname=with-txn-cb-example";
   mongoc_client_t *client = NULL;
   mongoc_database_t *database = NULL;
   mongoc_collection_t *collection = NULL;
   mongoc_client_session_t *session = NULL;
   bson_t *insert_opts = NULL;
   bson_t reply;
   ctx_t ctx;
   char *str;
   bson_error_t error;

   /*
    * Required to initialize libmongoc's internals
    */
   mongoc_init ();

   /*
    * Optionally get MongoDB URI from command line
    */
   if (argc > 1) {
      uri_string = argv[1];
   }

   /*
    * Safely create a MongoDB URI object from the given string
    */
   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      MONGOC_ERROR ("failed to parse URI: %s\n"
                    "error message:       %s\n",
                    uri_string,
                    error.message);
      goto done;
   }

   /*
    * Create a new client instance
    */
   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      goto done;
   }

   mongoc_client_set_error_api (client, 2);

   /*
    * Get a handle on the database "example-with-txn-cb"
    */
   database = mongoc_client_get_database (client, "example-with-txn-cb");

   /*
    * Inserting into a nonexistent collection normally creates it, but a
    * collection can't be created in a transaction; create it now
    */
   collection = mongoc_database_create_collection (database, "collection", NULL, &error);
   if (!collection) {
      /* code 48 is NamespaceExists, see error_codes.err in mongodb source */
      if (error.code == 48) {
         collection = mongoc_database_get_collection (database, "collection");
      } else {
         MONGOC_ERROR ("Failed to create collection: %s", error.message);
         goto done;
      }
   }

   /*
    * Pass NULL for options - by default the session is causally consistent
    */
   session = mongoc_client_start_session (client, NULL, &error);
   if (!session) {
      MONGOC_ERROR ("Failed to start session: %s", error.message);
      goto done;
   }

   /*
    * Append a logical session id to command options
    */
   insert_opts = bson_new ();
   if (!mongoc_client_session_append (session, insert_opts, &error)) {
      MONGOC_ERROR ("Could not add session to opts: %s", error.message);
      goto done;
   }

   ctx.collection = collection;
   ctx.insert_opts = insert_opts;

   /*
    * This method will start a new transaction on session, run our callback
    * function, i.e., &create_and_insert_doc, passing &ctx as an argument and
    * commit the transaction.
    */
   if (!mongoc_client_session_with_transaction (session, &create_and_insert_doc, NULL, &ctx, &reply, &error)) {
      MONGOC_ERROR ("Insert failed: %s", error.message);
      goto done;
   }

   str = bson_as_json (&reply, NULL);
   printf ("%s\n", str);

   exit_code = EXIT_SUCCESS;

done:
   bson_free (str);
   bson_destroy (&reply);
   bson_destroy (insert_opts);
   mongoc_client_session_destroy (session);
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return exit_code;
}
