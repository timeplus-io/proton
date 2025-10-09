/* gcc example-transaction.c -o example-transaction \
 *     $(pkg-config --cflags --libs libmongoc-1.0) */

/* ./example-transaction [CONNECTION_STRING] */

#include <stdio.h>
#include <mongoc/mongoc.h>


int
main (int argc, char *argv[])
{
   int exit_code = EXIT_FAILURE;

   mongoc_client_t *client = NULL;
   mongoc_database_t *database = NULL;
   mongoc_collection_t *collection = NULL;
   mongoc_client_session_t *session = NULL;
   mongoc_session_opt_t *session_opts = NULL;
   mongoc_transaction_opt_t *default_txn_opts = NULL;
   mongoc_transaction_opt_t *txn_opts = NULL;
   mongoc_read_concern_t *read_concern = NULL;
   mongoc_write_concern_t *write_concern = NULL;
   const char *uri_string = "mongodb://127.0.0.1/?appname=transaction-example";
   mongoc_uri_t *uri;
   bson_error_t error;
   bson_t *doc = NULL;
   bson_t *insert_opts = NULL;
   int32_t i;
   int64_t start;
   bson_t reply = BSON_INITIALIZER;
   char *reply_json;
   bool r;

   mongoc_init ();

   if (argc > 1) {
      uri_string = argv[1];
   }

   uri = mongoc_uri_new_with_error (uri_string, &error);
   if (!uri) {
      MONGOC_ERROR ("failed to parse URI: %s\n"
                    "error message:       %s\n",
                    uri_string,
                    error.message);
      goto done;
   }

   client = mongoc_client_new_from_uri (uri);
   if (!client) {
      goto done;
   }

   mongoc_client_set_error_api (client, 2);
   database = mongoc_client_get_database (client, "example-transaction");

   /* inserting into a nonexistent collection normally creates it, but a
    * collection can't be created in a transaction; create it now */
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

   /* a transaction's read preferences, read concern, and write concern can be
    * set on the client, on the default transaction options, or when starting
    * the transaction. for the sake of this example, set read concern on the
    * default transaction options. */
   default_txn_opts = mongoc_transaction_opts_new ();
   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, "snapshot");
   mongoc_transaction_opts_set_read_concern (default_txn_opts, read_concern);
   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_default_transaction_opts (session_opts, default_txn_opts);

   session = mongoc_client_start_session (client, session_opts, &error);
   if (!session) {
      MONGOC_ERROR ("Failed to start session: %s", error.message);
      goto done;
   }

   /* in this example, set write concern when starting the transaction */
   txn_opts = mongoc_transaction_opts_new ();
   write_concern = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (write_concern, 1000 /* wtimeout */);
   mongoc_transaction_opts_set_write_concern (txn_opts, write_concern);

   insert_opts = bson_new ();
   if (!mongoc_client_session_append (session, insert_opts, &error)) {
      MONGOC_ERROR ("Could not add session to opts: %s", error.message);
      goto done;
   }

retry_transaction:
   r = mongoc_client_session_start_transaction (session, txn_opts, &error);
   if (!r) {
      MONGOC_ERROR ("Failed to start transaction: %s", error.message);
      goto done;
   }

   /* insert two documents - on error, retry the whole transaction */
   for (i = 0; i < 2; i++) {
      doc = BCON_NEW ("_id", BCON_INT32 (i));
      bson_destroy (&reply);
      r = mongoc_collection_insert_one (collection, doc, insert_opts, &reply, &error);

      bson_destroy (doc);

      if (!r) {
         MONGOC_ERROR ("Insert failed: %s", error.message);
         mongoc_client_session_abort_transaction (session, NULL);

         /* a network error, primary failover, or other temporary error in a
          * transaction includes {"errorLabels": ["TransientTransactionError"]},
          * meaning that trying the entire transaction again may succeed
          */
         if (mongoc_error_has_label (&reply, "TransientTransactionError")) {
            goto retry_transaction;
         }

         goto done;
      }

      reply_json = bson_as_json (&reply, NULL);
      printf ("%s\n", reply_json);
      bson_free (reply_json);
   }

   /* in case of transient errors, retry for 5 seconds to commit transaction */
   start = bson_get_monotonic_time ();
   while (bson_get_monotonic_time () - start < 5 * 1000 * 1000) {
      bson_destroy (&reply);
      r = mongoc_client_session_commit_transaction (session, &reply, &error);
      if (r) {
         /* success */
         break;
      } else {
         MONGOC_ERROR ("Warning: commit failed: %s", error.message);
         if (mongoc_error_has_label (&reply, "TransientTransactionError")) {
            goto retry_transaction;
         } else if (mongoc_error_has_label (&reply, "UnknownTransactionCommitResult")) {
            /* try again to commit */
            continue;
         }

         /* unrecoverable error trying to commit */
         break;
      }
   }

   exit_code = EXIT_SUCCESS;

done:
   bson_destroy (&reply);
   bson_destroy (insert_opts);
   mongoc_write_concern_destroy (write_concern);
   mongoc_read_concern_destroy (read_concern);
   mongoc_transaction_opts_destroy (txn_opts);
   mongoc_transaction_opts_destroy (default_txn_opts);
   mongoc_client_session_destroy (session);
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (database);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return exit_code;
}
