#include <mongoc/mongoc.h>
#include <mongoc/mongoc-find-and-modify-private.h>
#include <mock_server/future.h>
#include <mock_server/future-value.h>

#include "TestSuite.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"
#include "mock_server/mock-rs.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"


/*
 * Test that all "with_opts" functions that accept readConcern, writeConcern,
 * and/or readPreference properly implement inheritance. For each of these
 * options, test that the function inherits the option from its source object
 * (e.g., mongoc_collection_watch inherits readConcern from the collection),
 * that the function uses the option from "opts" if present, and that "opts"
 * overrides the option from the source object.
 *
 * listDatabases, listCollections, and listIndexes don't use any of these
 * options, so don't test their helpers:
 *
 *   mongoc_client_find_databases_with_opts
 *   mongoc_client_get_database_names_with_opts
 *   mongoc_database_find_collections_with_opts
 *   mongoc_database_get_collection_names_with_opts
 *   mongoc_collection_find_indexes_with_opts
 */

/* kinds of options */
typedef enum {
   OPT_READ_CONCERN,
   OPT_WRITE_CONCERN,
   OPT_READ_PREFS,
} opt_type_t;


/* objects on which options can be set */
typedef enum {
   OPT_SOURCE_NONE = 0,
   OPT_SOURCE_FUNC = 1 << 0,
   OPT_SOURCE_COLL = 1 << 1,
   OPT_SOURCE_DB = 1 << 2,
   OPT_SOURCE_CLIENT = 1 << 3,
} opt_source_t;


/* for mongoc_bulk_operation_t tests */
typedef bool (*bulk_op_t) (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd /* OUT */);


struct _opt_inheritance_test_t;


typedef struct {
   struct _opt_inheritance_test_t *test;
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;
   const mongoc_read_prefs_t *prefs;
   const bson_t *opts;
   /* find, aggregate, etc. store the cursor here while running */
   mongoc_cursor_t *cursor;
   bson_error_t error;
   /* allow func_with_opts_t functions to store data and destroy it later */
   void *data;
   void (*destructor) (void *data);
} func_ctx_t;


typedef future_t *(func_with_opts_t) (func_ctx_t *ctx, bson_t *cmd);


typedef struct _opt_inheritance_test_t {
   opt_source_t opt_source;
   func_with_opts_t *func_with_opts;
   const char *func_name;
   opt_type_t opt_type;
   int n_sections;
   /* for mongoc_bulk_operation_t tests */
   bulk_op_t bulk_op;
} opt_inheritance_test_t;


static void
func_ctx_init (func_ctx_t *ctx,
               opt_inheritance_test_t *test,
               mongoc_client_t *client,
               mongoc_database_t *db,
               mongoc_collection_t *collection,
               const mongoc_read_prefs_t *prefs,
               const bson_t *opts)
{
   ASSERT (client);

   ctx->test = test;
   ctx->client = client;
   ctx->db = db;
   ctx->collection = collection;
   ctx->prefs = prefs;
   ctx->opts = opts;
   ctx->cursor = NULL;
   memset (&ctx->error, 0, sizeof (ctx->error));
   ctx->data = NULL;
   ctx->destructor = NULL;
}


static void
func_ctx_cleanup (func_ctx_t *ctx)
{
   mongoc_cursor_destroy (ctx->cursor);
   if (ctx->destructor) {
      ctx->destructor (ctx->data);
   }
}


/* if type is e.g. "collection", set readConcern level collection, writeConcern
 * w=collection, readPreference tags [{collection: "yes"}] */
#define SET_OPT_PREAMBLE(_type)                                                \
   mongoc_read_concern_t *rc = mongoc_read_concern_new ();                     \
   mongoc_write_concern_t *wc = mongoc_write_concern_new ();                   \
   mongoc_read_prefs_t *prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY); \
                                                                               \
   mongoc_read_concern_set_level (rc, #_type);                                 \
   mongoc_write_concern_set_wtag (wc, #_type);                                 \
   mongoc_read_prefs_set_tags (prefs, tmp_bson ("[{'%s': 'yes'}]", #_type));   \
   (void) 0

#define SET_OPT_CLEANUP                  \
   if (1) {                              \
      mongoc_read_concern_destroy (rc);  \
      mongoc_write_concern_destroy (wc); \
      mongoc_read_prefs_destroy (prefs); \
   } else                                \
      (void) 0

#define SET_OPT(_type)                                                          \
   static void set_##_type##_opt (mongoc_##_type##_t *obj, opt_type_t opt_type) \
   {                                                                            \
      SET_OPT_PREAMBLE (_type);                                                 \
                                                                                \
      switch (opt_type) {                                                       \
      case OPT_READ_CONCERN:                                                    \
         mongoc_##_type##_set_read_concern (obj, rc);                           \
         break;                                                                 \
      case OPT_WRITE_CONCERN:                                                   \
         mongoc_##_type##_set_write_concern (obj, wc);                          \
         break;                                                                 \
      case OPT_READ_PREFS:                                                      \
         mongoc_##_type##_set_read_prefs (obj, prefs);                          \
         break;                                                                 \
      default:                                                                  \
         test_error ("invalid opt_type: %d", (int) opt_type);                   \
      }                                                                         \
                                                                                \
      SET_OPT_CLEANUP;                                                          \
   }

SET_OPT (client)
SET_OPT (database)
SET_OPT (collection)


static void
set_func_opt (bson_t *opts, mongoc_read_prefs_t **prefs_ptr, opt_type_t opt_type)
{
   SET_OPT_PREAMBLE (function);

   switch (opt_type) {
   case OPT_READ_CONCERN:
      BSON_ASSERT (mongoc_read_concern_append (rc, opts));
      break;
   case OPT_WRITE_CONCERN:
      BSON_ASSERT (mongoc_write_concern_append (wc, opts));
      break;
   case OPT_READ_PREFS:
      *prefs_ptr = mongoc_read_prefs_copy (prefs);
      break;
   default:
      test_error ("invalid opt_type: %d", (int) opt_type);
   }

   SET_OPT_CLEANUP;
}


/* add BSON we expect to be included in a command due to an inherited option.
 * e.g., when "count" inherits readConcern from the DB, it should include
 * readConcern: {level: 'database'} in the command body. */
void
add_expected_opt (opt_source_t opt_source, opt_type_t opt_type, bson_t *cmd)
{
   const char *source_name;
   bson_t *opt;

   if (opt_source & OPT_SOURCE_FUNC) {
      source_name = "function";
   } else if (opt_source & OPT_SOURCE_COLL) {
      source_name = "collection";
   } else if (opt_source & OPT_SOURCE_DB) {
      source_name = "database";
   } else if (opt_source & OPT_SOURCE_CLIENT) {
      source_name = "client";
   } else {
      test_error ("opt_json called with OPT_SOURCE_NONE");
   }

   switch (opt_type) {
   case OPT_READ_CONCERN:
      opt = tmp_bson ("{'readConcern': {'level': '%s'}}", source_name);
      break;
   case OPT_WRITE_CONCERN:
      opt = tmp_bson ("{'writeConcern': {'w': '%s'}}", source_name);
      break;
   case OPT_READ_PREFS:
      opt = tmp_bson ("{'$readPreference': {'mode': 'secondary', 'tags': [{'%s': 'yes'}]}}", source_name);
      break;
   default:
      test_error ("invalid opt_type: %d", (int) opt_type);
   }

   bson_concat (cmd, opt);
}


static const char *
opt_type_name (opt_type_t opt_type)
{
   switch (opt_type) {
   case OPT_READ_CONCERN:
      return "readConcern";
   case OPT_WRITE_CONCERN:
      return "writeConcern";
   case OPT_READ_PREFS:
      return "readPrefs";
   default:
      test_error ("invalid opt_type: %d", (int) opt_type);
   }
}


static void
cleanup_future (future_t *future)
{
   future_value_t v;

   BSON_ASSERT (future_wait (future));

   v = future->return_value;
   if (v.type == future_value_mongoc_change_stream_ptr_type) {
      mongoc_change_stream_destroy (v.value.mongoc_change_stream_ptr_value);
   } else if (v.type == future_value_char_ptr_ptr_type) {
      bson_strfreev (v.value.char_ptr_ptr_value);
   }

   future_destroy (future);
}


/**********************************************************************
 *
 * func_with_opts_t implementations for client
 *
 **********************************************************************/

static future_t *
client_read_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_INT32 (cmd, "foo", 1);
   return future_client_read_command_with_opts (
      ctx->client, "db", tmp_bson ("{'foo': 1}"), ctx->prefs, ctx->opts, NULL, &ctx->error);
}


static future_t *
client_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "collection");
   return future_client_write_command_with_opts (
      ctx->client, "db", tmp_bson ("{'foo': 'collection'}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
client_read_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "collection");
   return future_client_read_write_command_with_opts (
      ctx->client, "db", tmp_bson ("{'foo': 'collection'}"), ctx->prefs, ctx->opts, NULL, &ctx->error);
}


static future_t *
client_watch (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_INT32 (cmd, "aggregate", 1);
   return future_client_watch (ctx->client, tmp_bson ("{}"), ctx->opts);
}


/**********************************************************************
 *
 * func_with_opts_t implementations for database
 *
 **********************************************************************/

static future_t *
db_drop (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_INT32 (cmd, "dropDatabase", 1);
   return future_database_drop_with_opts (ctx->db, ctx->opts, &ctx->error);
}


static future_t *
db_read_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "db");
   return future_database_read_command_with_opts (
      ctx->db, tmp_bson ("{'foo': 'db'}"), ctx->prefs, ctx->opts, NULL, &ctx->error);
}


static future_t *
db_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "db");
   return future_database_write_command_with_opts (ctx->db, tmp_bson ("{'foo': 'db'}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
db_read_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "db");
   return future_database_read_write_command_with_opts (
      ctx->db, tmp_bson ("{'foo': 'db'}"), ctx->prefs, ctx->opts, NULL, &ctx->error);
}


static future_t *
db_watch (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_INT32 (cmd, "aggregate", 1);
   return future_database_watch (ctx->db, tmp_bson ("{}"), ctx->opts);
}


/**********************************************************************
 *
 * func_with_opts_t implementations for collection
 *
 **********************************************************************/

static future_t *
aggregate (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "aggregate", "collection");
   ctx->cursor = mongoc_collection_aggregate (
      ctx->collection, MONGOC_QUERY_NONE, tmp_bson ("{'pipeline': [{'$out': 'foo'}]}"), ctx->opts, ctx->prefs);

   /* use ctx->data as the bson_t** out-param to mongoc_cursor_next () */
   return future_cursor_next (ctx->cursor, (const bson_t **) &ctx->data);
}


static future_t *
aggregate_raw_pipeline (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "aggregate", "collection");
   ctx->cursor = mongoc_collection_aggregate (
      ctx->collection, MONGOC_QUERY_NONE, tmp_bson ("[{'$out': 'foo'}]"), ctx->opts, ctx->prefs);

   /* use ctx->data as the bson_t** out-param to mongoc_cursor_next () */
   return future_cursor_next (ctx->cursor, (const bson_t **) &ctx->data);
}


static future_t *
collection_drop (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "drop", "collection");
   return future_collection_drop_with_opts (ctx->collection, ctx->opts, &ctx->error);
}


static future_t *
collection_read_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "collection");
   return future_collection_read_command_with_opts (
      ctx->collection, tmp_bson ("{'foo': 'collection'}"), ctx->prefs, ctx->opts, NULL, &ctx->error);
}


static future_t *
collection_read_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "collection");
   return future_collection_read_write_command_with_opts (
      ctx->collection, tmp_bson ("{'foo': 'collection'}"), NULL, ctx->opts, NULL, &ctx->error);
}


static future_t *
collection_write_cmd (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "foo", "collection");
   return future_collection_write_command_with_opts (
      ctx->collection, tmp_bson ("{'foo': 'collection'}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
collection_watch (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "aggregate", "collection");
   return future_collection_watch (ctx->collection, tmp_bson ("{}"), ctx->opts);
}


static future_t *
count (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "count", "collection");
   return future_collection_count_with_opts (
      ctx->collection, MONGOC_QUERY_NONE, NULL, 0, 0, ctx->opts, ctx->prefs, &ctx->error);
}


static future_t *
count_documents (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "aggregate", "collection");
   return future_collection_count_documents (
      ctx->collection, tmp_bson ("{}"), ctx->opts, ctx->prefs, NULL, &ctx->error);
}


static future_t *
create_index (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "createIndexes", "collection");
   return future_collection_create_index_with_opts (
      ctx->collection, tmp_bson ("{}"), NULL, ctx->opts, NULL, &ctx->error);
}


static future_t *
drop_index (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "dropIndexes", "collection");
   return future_collection_drop_index_with_opts (ctx->collection, "index name", ctx->opts, &ctx->error);
}


static future_t *
estimated_document_count (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "count", "collection");
   return future_collection_estimated_document_count (ctx->collection, ctx->opts, ctx->prefs, NULL, &ctx->error);
}


static future_t *
find (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "find", "collection");
   ctx->cursor = mongoc_collection_find_with_opts (ctx->collection, tmp_bson ("{}"), ctx->opts, ctx->prefs);

   /* use ctx->data as the bson_t** out-param to mongoc_cursor_next () */
   return future_cursor_next (ctx->cursor, (const bson_t **) &ctx->data);
}


static void
find_and_modify_cleanup (void *data)
{
   mongoc_find_and_modify_opts_destroy ((mongoc_find_and_modify_opts_t *) data);
}


static future_t *
find_and_modify (func_ctx_t *ctx, bson_t *cmd)
{
   mongoc_find_and_modify_opts_t *fam;

   BSON_APPEND_UTF8 (cmd, "findAndModify", "collection");
   fam = mongoc_find_and_modify_opts_new ();
   bson_concat (&fam->extra, ctx->opts);

   /* destroy the mongoc_find_and_modify_opts_t later */
   ctx->data = fam;
   ctx->destructor = find_and_modify_cleanup;

   return future_collection_find_and_modify_with_opts (ctx->collection, tmp_bson ("{}"), fam, NULL, &ctx->error);
}


/**********************************************************************
 *
 * func_with_opts_t implementations for collection write helpers
 *
 **********************************************************************/

static future_t *
delete_many (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "delete", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_delete_many (ctx->collection, tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
delete_one (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "delete", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_delete_one (ctx->collection, tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
insert_many (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "insert", "collection");
   BSON_ASSERT (!ctx->prefs);
   /* the "array" of input documents must be a valid pointer, stage it here */
   ctx->data = tmp_bson ("{}");
   return future_collection_insert_many (
      ctx->collection, (const bson_t **) &ctx->data, 1, ctx->opts, NULL, &ctx->error);
}


static future_t *
insert_one (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "insert", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_insert_one (ctx->collection, tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
replace_one (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_replace_one (
      ctx->collection, tmp_bson ("{}"), tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
update_many (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_update_many (
      ctx->collection, tmp_bson ("{}"), tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


static future_t *
update_one (func_ctx_t *ctx, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   BSON_ASSERT (!ctx->prefs);
   return future_collection_update_one (
      ctx->collection, tmp_bson ("{}"), tmp_bson ("{}"), ctx->opts, NULL, &ctx->error);
}


/**********************************************************************
 *
 * mongoc_bulk_operation_t test functions
 *
 **********************************************************************/

static void
bulk_operation_cleanup (void *data)
{
   mongoc_bulk_operation_destroy ((mongoc_bulk_operation_t *) data);
}


static future_t *
bulk_exec (func_ctx_t *ctx, bson_t *cmd)
{
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   bool r;

   bulk = mongoc_collection_create_bulk_operation_with_opts (ctx->collection, ctx->opts);

   ctx->data = bulk;
   ctx->destructor = bulk_operation_cleanup;

   r = ctx->test->bulk_op (bulk, &error, cmd);
   ASSERT_OR_PRINT (r, error);

   return future_bulk_operation_execute (bulk, NULL /* reply */, &ctx->error);
}


static bool
bulk_insert (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "insert", "collection");
   return mongoc_bulk_operation_insert_with_opts (bulk, tmp_bson ("{}"), NULL, error);
}


static bool
bulk_remove_many (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "delete", "collection");
   return mongoc_bulk_operation_remove_many_with_opts (bulk, tmp_bson ("{}"), NULL, error);
}


static bool
bulk_remove_one (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "delete", "collection");
   return mongoc_bulk_operation_remove_one_with_opts (bulk, tmp_bson ("{}"), NULL, error);
}

static bool
bulk_replace_one (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   return mongoc_bulk_operation_replace_one_with_opts (bulk, tmp_bson ("{}"), tmp_bson ("{}"), NULL, error);
}


static bool
bulk_update_many (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   return mongoc_bulk_operation_update_many_with_opts (bulk, tmp_bson ("{}"), tmp_bson ("{}"), NULL, error);
}


static bool
bulk_update_one (mongoc_bulk_operation_t *bulk, bson_error_t *error, bson_t *cmd)
{
   BSON_APPEND_UTF8 (cmd, "update", "collection");
   return mongoc_bulk_operation_update_one_with_opts (bulk, tmp_bson ("{}"), tmp_bson ("{}"), NULL, error);
}


static void
test_func_inherits_opts (void *ctx)
{
   opt_inheritance_test_t *test = (opt_inheritance_test_t *) ctx;

   /* for example, test mongoc_collection_find_with_opts with no read pref,
    * with a read pref set on the collection (OPT_SOURCE_COLL), with an explicit
    * read pref (OPT_SOURCE_FUNC), or with one read pref on the collection and
    * a different one passed explicitly */
   opt_source_t source_matrix[] = {
      OPT_SOURCE_NONE, test->opt_source, OPT_SOURCE_FUNC, test->opt_source | OPT_SOURCE_FUNC};

   size_t i;
   func_ctx_t func_ctx;
   mock_rs_t *rs;
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;
   bson_t opts = BSON_INITIALIZER;
   mongoc_read_prefs_t *func_prefs = NULL;
   future_t *future;
   request_t *request;
   bson_t cmd = BSON_INITIALIZER;
   bool expect_secondary;
   bson_error_t error;

   /* one primary, one secondary */
   rs = mock_rs_with_auto_hello (WIRE_VERSION_MAX, true, 1, 0);
   /* we use read pref tags like "collection": "yes" to verify where the
    * pref was inherited from; ensure all secondaries match all tags */
   mock_rs_tag_secondary (rs,
                          0,
                          tmp_bson ("{'client': 'yes',"
                                    " 'database': 'yes',"
                                    " 'collection': 'yes',"
                                    " 'function': 'yes'}"));

   mock_rs_run (rs);

   /* iterate over all combinations of options sources: e.g., an option set on
    * collection and not function, on function not collection, both, neither */
   for (i = 0; i < sizeof (source_matrix) / (sizeof (opt_source_t)); i++) {
      expect_secondary = false;
      func_prefs = NULL;
      bson_reinit (&cmd);
      bson_reinit (&opts);

      client = test_framework_client_new_from_uri (mock_rs_get_uri (rs), NULL);
      if (source_matrix[i] & OPT_SOURCE_CLIENT) {
         set_client_opt (client, test->opt_type);
      }

      db = mongoc_client_get_database (client, "database");
      if (source_matrix[i] & OPT_SOURCE_DB) {
         set_database_opt (db, test->opt_type);
      }

      collection = mongoc_database_get_collection (db, "collection");
      if (source_matrix[i] & OPT_SOURCE_COLL) {
         set_collection_opt (collection, test->opt_type);
      }

      if (source_matrix[i] & OPT_SOURCE_FUNC) {
         set_func_opt (&opts, &func_prefs, test->opt_type);
      }

      func_ctx_init (&func_ctx, test, client, db, collection, func_prefs, &opts);

      /* A warning is thrown if an aggregate command with $out attempts to write
       * to a secondary */
      capture_logs (true);
      /* func_with_opts creates expected "cmd", like {insert: 'collection'} */
      future = test->func_with_opts (&func_ctx, &cmd);
      capture_logs (false);

      if (source_matrix[i] != OPT_SOURCE_NONE) {
         add_expected_opt (source_matrix[i], test->opt_type, &cmd);
         expect_secondary = test->opt_type == OPT_READ_PREFS;
      }

      /* write commands send two OP_MSG sections */
      if (test->n_sections == 2) {
         request = mock_rs_receives_msg (rs, 0, &cmd, tmp_bson ("{}"));
      } else {
         request = mock_rs_receives_msg (rs, 0, &cmd);
      }

      if (expect_secondary) {
         BSON_ASSERT (mock_rs_request_is_to_secondary (rs, request));
      } else {
         BSON_ASSERT (mock_rs_request_is_to_primary (rs, request));
      }

      if (func_ctx.cursor) {
         reply_to_request_simple (request,
                                  "{'ok': 1,"
                                  " 'cursor': {"
                                  "    'id': 0,"
                                  "    'ns': 'db.collection',"
                                  "    'firstBatch': []}}");

         BSON_ASSERT (!future_get_bool (future));
         future_destroy (future);
         ASSERT_OR_PRINT (!mongoc_cursor_error (func_ctx.cursor, &error), error);
      } else {
         reply_to_request_simple (request, "{'ok': 1}");
         cleanup_future (future);
      }

      request_destroy (request);
      mongoc_read_prefs_destroy (func_prefs);
      func_ctx_cleanup (&func_ctx);
      mongoc_collection_destroy (collection);
      mongoc_database_destroy (db);
      mongoc_client_destroy (client);
   }

   bson_destroy (&cmd);
   bson_destroy (&opts);
   mock_rs_destroy (rs);
}


/* commands that send one OP_MSG section */
#define OPT_TEST(_opt_source, _func, _opt_type)                   \
   {                                                              \
      OPT_SOURCE_##_opt_source, _func, #_func, OPT_##_opt_type, 1 \
   }


/* write commands commands that send two OP_MSG sections */
#define OPT_WRITE_TEST(_func)                              \
   {                                                       \
      OPT_SOURCE_COLL, _func, #_func, OPT_WRITE_CONCERN, 2 \
   }


/* mongoc_bulk_operation_t functions */
#define OPT_BULK_TEST(_bulk_op)                                             \
   {                                                                        \
      OPT_SOURCE_COLL, bulk_exec, #_bulk_op, OPT_WRITE_CONCERN, 2, _bulk_op \
   }


static opt_inheritance_test_t gInheritanceTests[] = {
   /*
    * client functions
    */
   OPT_TEST (CLIENT, client_read_cmd, READ_CONCERN),
   OPT_TEST (CLIENT, client_read_cmd, READ_PREFS),
   /* read_write_command functions deliberately ignore read prefs */
   OPT_TEST (CLIENT, client_read_write_cmd, READ_CONCERN),
   OPT_TEST (CLIENT, client_read_write_cmd, WRITE_CONCERN),
   /* watch helpers don't take explicit readPref */
   OPT_TEST (CLIENT, client_watch, READ_CONCERN),
   OPT_TEST (CLIENT, client_write_cmd, WRITE_CONCERN),

   /*
    * database functions
    */
   OPT_TEST (DB, db_drop, WRITE_CONCERN),
   OPT_TEST (DB, db_read_cmd, READ_CONCERN),
   OPT_TEST (DB, db_read_cmd, READ_PREFS),
   OPT_TEST (DB, db_read_write_cmd, READ_CONCERN),
   OPT_TEST (DB, db_read_write_cmd, WRITE_CONCERN),
   OPT_TEST (DB, db_watch, READ_CONCERN),
   OPT_TEST (DB, db_write_cmd, WRITE_CONCERN),

   /*
    * collection functions
    */
   OPT_TEST (COLL, aggregate, READ_CONCERN),
   OPT_TEST (COLL, aggregate, READ_PREFS),
   OPT_TEST (COLL, aggregate, WRITE_CONCERN),
   OPT_TEST (COLL, aggregate_raw_pipeline, READ_CONCERN),
   OPT_TEST (COLL, aggregate_raw_pipeline, READ_PREFS),
   OPT_TEST (COLL, aggregate_raw_pipeline, WRITE_CONCERN),
   OPT_TEST (COLL, collection_drop, WRITE_CONCERN),
   OPT_TEST (COLL, collection_read_cmd, READ_CONCERN),
   OPT_TEST (COLL, collection_read_cmd, READ_PREFS),
   OPT_TEST (COLL, collection_read_write_cmd, READ_CONCERN),
   OPT_TEST (COLL, collection_read_write_cmd, WRITE_CONCERN),
   OPT_TEST (COLL, collection_watch, READ_CONCERN),
   OPT_TEST (COLL, collection_write_cmd, WRITE_CONCERN),
   OPT_TEST (COLL, count, READ_CONCERN),
   OPT_TEST (COLL, count, READ_PREFS),
   OPT_TEST (COLL, count_documents, READ_CONCERN),
   OPT_TEST (COLL, count_documents, READ_PREFS),
   OPT_TEST (COLL, create_index, WRITE_CONCERN),
   OPT_TEST (COLL, drop_index, WRITE_CONCERN),
   OPT_TEST (COLL, estimated_document_count, READ_CONCERN),
   OPT_TEST (COLL, estimated_document_count, READ_PREFS),
   OPT_TEST (COLL, find, READ_CONCERN),
   OPT_TEST (COLL, find, READ_PREFS),
   /* find_and_modify deliberately ignores collection read concern */
   OPT_TEST (COLL, find_and_modify, WRITE_CONCERN),

   /*
    * collection write functions
    */
   OPT_WRITE_TEST (delete_many),
   OPT_WRITE_TEST (delete_one),
   OPT_WRITE_TEST (insert_many),
   OPT_WRITE_TEST (insert_one),
   OPT_WRITE_TEST (replace_one),
   OPT_WRITE_TEST (update_many),
   OPT_WRITE_TEST (update_one),

   /*
    * bulk operations
    */
   OPT_BULK_TEST (bulk_insert),
   OPT_BULK_TEST (bulk_remove_many),
   OPT_BULK_TEST (bulk_remove_one),
   OPT_BULK_TEST (bulk_replace_one),
   OPT_BULK_TEST (bulk_update_many),
   OPT_BULK_TEST (bulk_update_one),
};


static void
install_inheritance_tests (TestSuite *suite, opt_inheritance_test_t *tests, size_t n)
{
   size_t i;
   opt_inheritance_test_t *test;
   char *name;

   for (i = 0; i < n; i++) {
      test = &tests[i];
      name = bson_strdup_printf ("/inheritance/%s/%s", test->func_name, opt_type_name (test->opt_type));

      TestSuite_AddFull (suite, name, test_func_inherits_opts, NULL, test, TestSuite_CheckMockServerAllowed);

      bson_free (name);
   }
}


void
test_opts_install (TestSuite *suite)
{
   install_inheritance_tests (suite, gInheritanceTests, sizeof (gInheritanceTests) / sizeof (opt_inheritance_test_t));
}
