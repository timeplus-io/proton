
#include <mongoc/mongoc.h>

/* EXAMPLE_FAM_BYPASS_BEGIN */
void
fam_bypass (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t reply;
   bson_t *update;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   bool success;


   /* Find Zlatan Ibrahimovic, the striker */
   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");
   BSON_APPEND_UTF8 (&query, "profession", "Football player");

   /* Bump his age */
   update = BCON_NEW ("$inc", "{", "age", BCON_INT32 (1), "}");

   opts = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_set_update (opts, update);
   /* He can still play, even though he is pretty old. */
   mongoc_find_and_modify_opts_set_bypass_document_validation (opts, true);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (&query);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_BYPASS_END */

/* EXAMPLE_FAM_FLAGS_BEGIN */
void
fam_flags (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t reply;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   bson_t *update;
   bool success;


   /* Find Zlatan Ibrahimovic, the striker */
   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");
   BSON_APPEND_UTF8 (&query, "profession", "Football player");
   BSON_APPEND_INT32 (&query, "age", 34);
   BSON_APPEND_INT32 (&query, "goals", (16 + 35 + 23 + 57 + 16 + 14 + 28 + 84) + (1 + 6 + 62));

   /* Add his football position */
   update = BCON_NEW ("$set", "{", "position", BCON_UTF8 ("striker"), "}");

   opts = mongoc_find_and_modify_opts_new ();

   mongoc_find_and_modify_opts_set_update (opts, update);

   /* Create the document if it didn't exist, and return the updated document */
   mongoc_find_and_modify_opts_set_flags (opts, MONGOC_FIND_AND_MODIFY_UPSERT | MONGOC_FIND_AND_MODIFY_RETURN_NEW);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (&query);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_FLAGS_END */

/* EXAMPLE_FAM_UPDATE_BEGIN */
void
fam_update (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t *update;
   bson_t reply;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   bool success;


   /* Find Zlatan Ibrahimovic */
   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");

   /* Make him a book author */
   update = BCON_NEW ("$set", "{", "author", BCON_BOOL (true), "}");

   opts = mongoc_find_and_modify_opts_new ();
   /* Note that the document returned is the _previous_ version of the document
    * To fetch the modified new version, use
    * mongoc_find_and_modify_opts_set_flags (opts,
    * MONGOC_FIND_AND_MODIFY_RETURN_NEW);
    */
   mongoc_find_and_modify_opts_set_update (opts, update);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (&query);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_UPDATE_END */

/* EXAMPLE_FAM_FIELDS_BEGIN */
void
fam_fields (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t fields = BSON_INITIALIZER;
   bson_t *update;
   bson_t reply;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   bool success;


   /* Find Zlatan Ibrahimovic */
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");
   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");

   /* Return his goal tally */
   BSON_APPEND_INT32 (&fields, "goals", 1);

   /* Bump his goal tally */
   update = BCON_NEW ("$inc", "{", "goals", BCON_INT32 (1), "}");

   opts = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_set_update (opts, update);
   mongoc_find_and_modify_opts_set_fields (opts, &fields);
   /* Return the new tally */
   mongoc_find_and_modify_opts_set_flags (opts, MONGOC_FIND_AND_MODIFY_RETURN_NEW);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (&fields);
   bson_destroy (&query);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_FIELDS_END */

/* EXAMPLE_FAM_OPTS_BEGIN */
void
fam_opts (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t reply;
   bson_t *update;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   mongoc_write_concern_t *wc;
   bson_t extra = BSON_INITIALIZER;
   bool success;


   /* Find Zlatan Ibrahimovic, the striker */
   BSON_APPEND_UTF8 (&query, "firstname", "Zlatan");
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");
   BSON_APPEND_UTF8 (&query, "profession", "Football player");

   /* Bump his age */
   update = BCON_NEW ("$inc", "{", "age", BCON_INT32 (1), "}");

   opts = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_set_update (opts, update);

   /* Abort if the operation takes too long. */
   mongoc_find_and_modify_opts_set_max_time_ms (opts, 100);

   /* Set write concern w: 2 */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_write_concern_append (wc, &extra);

   /* Some future findAndModify option the driver doesn't support conveniently
    */
   BSON_APPEND_INT32 (&extra, "futureOption", 42);
   mongoc_find_and_modify_opts_append (opts, &extra);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (&extra);
   bson_destroy (update);
   bson_destroy (&query);
   mongoc_write_concern_destroy (wc);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_OPTS_END */

/* EXAMPLE_FAM_SORT_BEGIN */
void
fam_sort (mongoc_collection_t *collection)
{
   mongoc_find_and_modify_opts_t *opts;
   bson_t *update;
   bson_t sort = BSON_INITIALIZER;
   bson_t reply;
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   bool success;


   /* Find all users with the lastname Ibrahimovic */
   BSON_APPEND_UTF8 (&query, "lastname", "Ibrahimovic");

   /* Sort by age (descending) */
   BSON_APPEND_INT32 (&sort, "age", -1);

   /* Bump his goal tally */
   update = BCON_NEW ("$set", "{", "oldest", BCON_BOOL (true), "}");

   opts = mongoc_find_and_modify_opts_new ();
   mongoc_find_and_modify_opts_set_update (opts, update);
   mongoc_find_and_modify_opts_set_sort (opts, &sort);

   success = mongoc_collection_find_and_modify_with_opts (collection, &query, opts, &reply, &error);

   if (success) {
      char *str;

      str = bson_as_canonical_extended_json (&reply, NULL);
      printf ("%s\n", str);
      bson_free (str);
   } else {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
   }

   bson_destroy (&reply);
   bson_destroy (update);
   bson_destroy (&sort);
   bson_destroy (&query);
   mongoc_find_and_modify_opts_destroy (opts);
}
/* EXAMPLE_FAM_SORT_END */

/* EXAMPLE_FAM_MAIN_BEGIN */
int
main (void)
{
   mongoc_collection_t *collection;
   mongoc_database_t *database;
   mongoc_client_t *client;
   const char *uri_string = "mongodb://localhost:27017/admin?appname=find-and-modify-opts-example";
   mongoc_uri_t *uri;
   bson_error_t error;
   bson_t *options;

   mongoc_init ();

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

   mongoc_client_set_error_api (client, 2);
   database = mongoc_client_get_database (client, "databaseName");

   options = BCON_NEW ("validator",
                       "{",
                       "age",
                       "{",
                       "$lte",
                       BCON_INT32 (34),
                       "}",
                       "}",
                       "validationAction",
                       BCON_UTF8 ("error"),
                       "validationLevel",
                       BCON_UTF8 ("moderate"));

   collection = mongoc_database_create_collection (database, "collectionName", options, &error);
   if (!collection) {
      fprintf (stderr, "Got error: \"%s\" on line %d\n", error.message, __LINE__);
      return EXIT_FAILURE;
   }

   fam_flags (collection);
   fam_bypass (collection);
   fam_update (collection);
   fam_fields (collection);
   fam_opts (collection);
   fam_sort (collection);

   mongoc_collection_drop (collection, NULL);
   bson_destroy (options);
   mongoc_uri_destroy (uri);
   mongoc_database_destroy (database);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();
   return EXIT_SUCCESS;
}
/* EXAMPLE_FAM_MAIN_END */
