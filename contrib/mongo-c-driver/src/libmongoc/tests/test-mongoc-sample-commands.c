/*
 * Copyright 2017 MongoDB, Inc.
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

/* MongoDB documentation examples
 *
 * One page on the MongoDB docs site shows a set of common tasks, with example
 * code for each driver plus the mongo shell. The source files for these code
 * examples are delimited with "Start Example N" / "End Example N" and so on.
 *
 * These are the C examples for that page.
 */

/* clang-format off */
#include <assert.h>
#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>
#include <mongoc/mongoc-database-private.h>
#include <mongoc/mongoc-collection-private.h>

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"


typedef void (*sample_command_fn_t) (mongoc_database_t *db);
typedef void (*sample_txn_command_fn_t) (mongoc_client_t *client);


static void
test_sample_command (sample_command_fn_t fn,
                     int exampleno,
                     mongoc_database_t *db,
                     mongoc_collection_t *collection,
                     bool drop_collection)
{
   char *example_name = bson_strdup_printf ("example %d", exampleno);
   capture_logs (true);

   fn (db);

   ASSERT_NO_CAPTURED_LOGS (example_name);

   if (drop_collection) {
      mongoc_collection_drop (collection, NULL);
   }

   bson_free (example_name);
}


static void
test_example_1 (mongoc_database_t *db)
{
   /* Start Example 1 */
   mongoc_collection_t *collection;
   bson_t *doc;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   doc = BCON_NEW (
      "item", BCON_UTF8 ("canvas"),
      "qty", BCON_INT64 (100),
      "tags", "[",
      BCON_UTF8 ("cotton"),
      "]",
      "size", "{",
      "h", BCON_DOUBLE (28),
      "w", BCON_DOUBLE (35.5),
      "uom", BCON_UTF8 ("cm"),
      "}");

   r = mongoc_collection_insert_one (collection, doc, NULL, NULL, &error);
   bson_destroy (doc);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 1 */
   ASSERT_COUNT (1, collection);
done:
   /* Start Example 1 Post */
   mongoc_collection_destroy (collection);
   /* End Example 1 Post */
}


static void
test_example_2 (mongoc_database_t *db)
{
   /* Start Example 2 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("item", BCON_UTF8 ("canvas"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 2 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 2 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 2 Post */
}


static void
test_example_3 (mongoc_database_t *db)
{
   /* Start Example 3 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "tags", "[",
      BCON_UTF8 ("blank"), BCON_UTF8 ("red"),
      "]",
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("mat"),
      "qty", BCON_INT64 (85),
      "tags", "[",
      BCON_UTF8 ("gray"),
      "]",
      "size", "{",
      "h", BCON_DOUBLE (27.9),
      "w", BCON_DOUBLE (35.5),
      "uom", BCON_UTF8 ("cm"),
      "}");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("mousepad"),
      "qty", BCON_INT64 (25),
      "tags", "[",
      BCON_UTF8 ("gel"), BCON_UTF8 ("blue"),
      "]",
      "size", "{",
      "h", BCON_DOUBLE (19),
      "w", BCON_DOUBLE (22.85),
      "uom", BCON_UTF8 ("cm"),
      "}");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 3 */
   ASSERT_COUNT (4, collection);
done:
   /* Start Example 3 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 3 Post */
}


static void
test_example_6 (mongoc_database_t *db)
{
   /* Start Example 6 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "qty", BCON_INT64 (50),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "qty", BCON_INT64 (100),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "qty", BCON_INT64 (75),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "qty", BCON_INT64 (45),
      "size", "{",
      "h", BCON_DOUBLE (10),
      "w", BCON_DOUBLE (15.25),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 6 */
   ASSERT_COUNT (5, collection);
done:
   /* Start Example 6 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 6 Post */
}


static void
test_example_7 (mongoc_database_t *db)
{
   /* Start Example 7 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (NULL);
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 7 */
   ASSERT_CURSOR_COUNT (5, cursor);
   /* Start Example 7 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 7 Post */
}


static void
test_example_9 (mongoc_database_t *db)
{
   /* Start Example 9 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("D"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 9 */
   ASSERT_CURSOR_COUNT (2, cursor);
   /* Start Example 9 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 9 Post */
}


static void
test_example_10 (mongoc_database_t *db)
{
   /* Start Example 10 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "status", "{",
      "$in", "[",
      BCON_UTF8 ("A"), BCON_UTF8 ("D"),
      "]",
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 10 */
   ASSERT_CURSOR_COUNT (5, cursor);
   /* Start Example 10 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 10 Post */
}


static void
test_example_11 (mongoc_database_t *db)
{
   /* Start Example 11 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "status", BCON_UTF8 ("A"),
      "qty", "{",
      "$lt", BCON_INT64 (30),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 11 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 11 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 11 Post */
}


static void
test_example_12 (mongoc_database_t *db)
{
   /* Start Example 12 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "$or", "[",
      "{",
      "status", BCON_UTF8 ("A"),
      "}","{",
      "qty", "{",
      "$lt", BCON_INT64 (30),
      "}",
      "}",
      "]");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 12 */
   ASSERT_CURSOR_COUNT (3, cursor);
   /* Start Example 12 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 12 Post */
}


static void
test_example_13 (mongoc_database_t *db)
{
   /* Start Example 13 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "status", BCON_UTF8 ("A"),
      "$or", "[",
      "{",
      "qty", "{",
      "$lt", BCON_INT64 (30),
      "}",
      "}","{",
      "item", BCON_REGEX ("^p", ""),
      "}",
      "]");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 13 */
   ASSERT_CURSOR_COUNT (2, cursor);
   /* Start Example 13 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 13 Post */
}


static void
test_example_14 (mongoc_database_t *db)
{
   /* Start Example 14 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "qty", BCON_INT64 (50),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "qty", BCON_INT64 (100),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "qty", BCON_INT64 (75),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "qty", BCON_INT64 (45),
      "size", "{",
      "h", BCON_DOUBLE (10),
      "w", BCON_DOUBLE (15.25),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 14 */

done:
   /* Start Example 14 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 14 Post */
}


static void
test_example_15 (mongoc_database_t *db)
{
   /* Start Example 15 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 15 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 15 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 15 Post */
}


static void
test_example_16 (mongoc_database_t *db)
{
   /* Start Example 16 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "size", "{",
      "w", BCON_DOUBLE (21),
      "h", BCON_DOUBLE (14),
      "uom", BCON_UTF8 ("cm"),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 16 */
   ASSERT_CURSOR_COUNT (0, cursor);
   /* Start Example 16 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 16 Post */
}


static void
test_example_17 (mongoc_database_t *db)
{
   /* Start Example 17 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("size.uom", BCON_UTF8 ("in"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 17 */
   ASSERT_CURSOR_COUNT (2, cursor);
   /* Start Example 17 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 17 Post */
}


static void
test_example_18 (mongoc_database_t *db)
{
   /* Start Example 18 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "size.h", "{",
      "$lt", BCON_INT64 (15),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 18 */
   ASSERT_CURSOR_COUNT (4, cursor);
   /* Start Example 18 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 18 Post */
}


static void
test_example_19 (mongoc_database_t *db)
{
   /* Start Example 19 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "size.h", "{",
      "$lt", BCON_INT64 (15),
      "}",
      "size.uom", BCON_UTF8 ("in"),
      "status", BCON_UTF8 ("D"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 19 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 19 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 19 Post */
}


static void
test_example_20 (mongoc_database_t *db)
{
   /* Start Example 20 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "tags", "[",
      BCON_UTF8 ("blank"), BCON_UTF8 ("red"),
      "]",
      "dim_cm", "[",
      BCON_INT64 (14), BCON_INT64 (21),
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "qty", BCON_INT64 (50),
      "tags", "[",
      BCON_UTF8 ("red"), BCON_UTF8 ("blank"),
      "]",
      "dim_cm", "[",
      BCON_INT64 (14), BCON_INT64 (21),
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "qty", BCON_INT64 (100),
      "tags", "[",
      BCON_UTF8 ("red"), BCON_UTF8 ("blank"), BCON_UTF8 ("plain"),
      "]",
      "dim_cm", "[",
      BCON_INT64 (14), BCON_INT64 (21),
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "qty", BCON_INT64 (75),
      "tags", "[",
      BCON_UTF8 ("blank"), BCON_UTF8 ("red"),
      "]",
      "dim_cm", "[",
      BCON_DOUBLE (22.85), BCON_INT64 (30),
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "qty", BCON_INT64 (45),
      "tags", "[",
      BCON_UTF8 ("blue"),
      "]",
      "dim_cm", "[",
      BCON_INT64 (10), BCON_DOUBLE (15.25),
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 20 */

done:   
   /* Start Example 20 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 20 Post */
}


static void
test_example_21 (mongoc_database_t *db)
{
   /* Start Example 21 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "tags", "[",
      BCON_UTF8 ("red"), BCON_UTF8 ("blank"),
      "]");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 21 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 21 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 21 Post */
}


static void
test_example_22 (mongoc_database_t *db)
{
   /* Start Example 22 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "tags", "{",
      "$all", "[",
      BCON_UTF8 ("red"), BCON_UTF8 ("blank"),
      "]",
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 22 */
   ASSERT_CURSOR_COUNT (4, cursor);
   /* Start Example 22 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 22 Post */
}


static void
test_example_23 (mongoc_database_t *db)
{
   /* Start Example 23 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("tags", BCON_UTF8 ("red"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 23 */
   ASSERT_CURSOR_COUNT (4, cursor);
   /* Start Example 23 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 23 Post */
}


static void
test_example_24 (mongoc_database_t *db)
{
   /* Start Example 24 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "dim_cm", "{",
      "$gt", BCON_INT64 (25),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 24 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 24 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 24 Post */
}


static void
test_example_25 (mongoc_database_t *db)
{
   /* Start Example 25 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "dim_cm", "{",
      "$gt", BCON_INT64 (15),
      "$lt", BCON_INT64 (20),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 25 */
   ASSERT_CURSOR_COUNT (4, cursor);
   /* Start Example 25 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 25 Post */
}


static void
test_example_26 (mongoc_database_t *db)
{
   /* Start Example 26 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "dim_cm", "{",
      "$elemMatch", "{",
      "$gt", BCON_INT64 (22),
      "$lt", BCON_INT64 (30),
      "}",
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 26 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 26 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 26 Post */
}


static void
test_example_27 (mongoc_database_t *db)
{
   /* Start Example 27 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "dim_cm.1", "{",
      "$gt", BCON_INT64 (25),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 27 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 27 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 27 Post */
}


static void
test_example_28 (mongoc_database_t *db)
{
   /* Start Example 28 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "tags", "{",
      "$size", BCON_INT64 (3),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 28 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 28 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 28 Post */
}


static void
test_example_29 (mongoc_database_t *db)
{
   /* Start Example 29 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (5),
      "}","{",
      "warehouse", BCON_UTF8 ("C"),
      "qty", BCON_INT64 (15),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("C"),
      "qty", BCON_INT64 (5),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (60),
      "}","{",
      "warehouse", BCON_UTF8 ("B"),
      "qty", BCON_INT64 (15),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (40),
      "}","{",
      "warehouse", BCON_UTF8 ("B"),
      "qty", BCON_INT64 (5),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("B"),
      "qty", BCON_INT64 (15),
      "}","{",
      "warehouse", BCON_UTF8 ("C"),
      "qty", BCON_INT64 (35),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 29 */

done:
   /* Start Example 29 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 29 Post */
}


static void
test_example_30 (mongoc_database_t *db)
{
   /* Start Example 30 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock", "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (5),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 30 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 30 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 30 Post */
}


static void
test_example_31 (mongoc_database_t *db)
{
   /* Start Example 31 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock", "{",
      "qty", BCON_INT64 (5),
      "warehouse", BCON_UTF8 ("A"),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 31 */
   ASSERT_CURSOR_COUNT (0, cursor);
   /* Start Example 31 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 31 Post */
}


static void
test_example_32 (mongoc_database_t *db)
{
   /* Start Example 32 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock.0.qty", "{",
      "$lte", BCON_INT64 (20),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 32 */
   ASSERT_CURSOR_COUNT (3, cursor);
   /* Start Example 32 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 32 Post */
}


static void
test_example_33 (mongoc_database_t *db)
{
   /* Start Example 33 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock.qty", "{",
      "$lte", BCON_INT64 (20),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 33 */
   ASSERT_CURSOR_COUNT (5, cursor);
   /* Start Example 33 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 33 Post */
}


static void
test_example_34 (mongoc_database_t *db)
{
   /* Start Example 34 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock", "{",
      "$elemMatch", "{",
      "qty", BCON_INT64 (5),
      "warehouse", BCON_UTF8 ("A"),
      "}",
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 34 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 34 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 34 Post */
}


static void
test_example_35 (mongoc_database_t *db)
{
   /* Start Example 35 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock", "{",
      "$elemMatch", "{",
      "qty", "{",
      "$gt", BCON_INT64 (10),
      "$lte", BCON_INT64 (20),
      "}",
      "}",
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 35 */
   ASSERT_CURSOR_COUNT (3, cursor);
   /* Start Example 35 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 35 Post */
}


static void
test_example_36 (mongoc_database_t *db)
{
   /* Start Example 36 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock.qty", "{",
      "$gt", BCON_INT64 (10),
      "$lte", BCON_INT64 (20),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 36 */
   ASSERT_CURSOR_COUNT (4, cursor);
   /* Start Example 36 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 36 Post */
}


static void
test_example_37 (mongoc_database_t *db)
{
   /* Start Example 37 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "instock.qty", BCON_INT64 (5),
      "instock.warehouse", BCON_UTF8 ("A"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 37 */
   ASSERT_CURSOR_COUNT (2, cursor);
   /* Start Example 37 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 37 Post */
}


static void
test_example_38 (mongoc_database_t *db)
{
   /* Start Example 38 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "_id", BCON_INT64 (1),
      "item", BCON_NULL);

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW ("_id", BCON_INT64 (2));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 38 */

done:
   /* Start Example 38 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 38 Post */
}


static void
test_example_39 (mongoc_database_t *db)
{
   /* Start Example 39 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("item", BCON_NULL);
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 39 */
   ASSERT_CURSOR_COUNT (2, cursor);
   /* Start Example 39 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 39 Post */
}


static void
test_example_40 (mongoc_database_t *db)
{
   /* Start Example 40 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "item", "{",
      "$type", BCON_INT64 (10),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 40 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 40 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 40 Post */
}


static void
test_example_41 (mongoc_database_t *db)
{
   /* Start Example 41 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW (
      "item", "{",
      "$exists", BCON_BOOL (false),
      "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 41 */
   ASSERT_CURSOR_COUNT (1, cursor);
   /* Start Example 41 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 41 Post */
}


static void
test_example_42 (mongoc_database_t *db)
{
   /* Start Example 42 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "status", BCON_UTF8 ("A"),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (5),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "status", BCON_UTF8 ("A"),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("C"),
      "qty", BCON_INT64 (5),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "status", BCON_UTF8 ("D"),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (60),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "status", BCON_UTF8 ("D"),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (40),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "status", BCON_UTF8 ("A"),
      "size", "{",
      "h", BCON_DOUBLE (10),
      "w", BCON_DOUBLE (15.25),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("B"),
      "qty", BCON_INT64 (15),
      "}","{",
      "warehouse", BCON_UTF8 ("C"),
      "qty", BCON_INT64 (35),
      "}",
      "]");

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 42 */

done:
   /* Start Example 42 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 42 Post */
}


static void
test_example_43 (mongoc_database_t *db)
{
   /* Start Example 43 */
   mongoc_collection_t *collection;
   bson_t *filter;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
   /* End Example 43 */
   ASSERT_CURSOR_COUNT (3, cursor);
   /* Start Example 43 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 43 Post */
}


static void
test_example_44 (mongoc_database_t *db)
{
   /* Start Example 44 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "item", BCON_INT64 (1),
   "status", BCON_INT64 (1), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 44 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_NOT_FIELD (doc, "size");
         ASSERT_HAS_NOT_FIELD (doc, "instock");
      }
   }
   /* Start Example 44 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 44 Post */
}


static void
test_example_45 (mongoc_database_t *db)
{
   /* Start Example 45 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "item", BCON_INT64 (1),
   "status", BCON_INT64 (1),
   "_id", BCON_INT64 (0), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 45 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_HAS_NOT_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_NOT_FIELD (doc, "size");
         ASSERT_HAS_NOT_FIELD (doc, "instock");
      }
   }
   /* Start Example 45 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 45 Post */
}


static void
test_example_46 (mongoc_database_t *db)
{
   /* Start Example 46 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "status", BCON_INT64 (0),
   "instock", BCON_INT64 (0), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 46 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_NOT_FIELD (doc, "status");
         ASSERT_HAS_FIELD (doc, "size");
         ASSERT_HAS_NOT_FIELD (doc, "instock");
      }
   }
   /* Start Example 46 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 46 Post */
}


static void
test_example_47 (mongoc_database_t *db)
{
   /* Start Example 47 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "item", BCON_INT64 (1),
   "status", BCON_INT64 (1),
   "size.uom", BCON_INT64 (1), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 47 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         bson_t size;

         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_FIELD (doc, "size");
         ASSERT_HAS_NOT_FIELD (doc, "instock");
         bson_lookup_doc (doc, "size", &size);
         ASSERT_HAS_FIELD (&size, "uom");
         ASSERT_HAS_NOT_FIELD (&size, "h");
         ASSERT_HAS_NOT_FIELD (&size, "w");
      }
   }
   /* Start Example 47 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 47 Post */
}


static void
test_example_48 (mongoc_database_t *db)
{
   /* Start Example 48 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "size.uom", BCON_INT64 (0), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 48 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         bson_t size;

         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_FIELD (doc, "size");
         ASSERT_HAS_FIELD (doc, "instock");
         bson_lookup_doc (doc, "size", &size);
         ASSERT_HAS_NOT_FIELD (&size, "uom");
         ASSERT_HAS_FIELD (&size, "h");
         ASSERT_HAS_FIELD (&size, "w");
      }
   }
   /* Start Example 48 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 48 Post */
}


static void
test_example_49 (mongoc_database_t *db)
{
   /* Start Example 49 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "item", BCON_INT64 (1),
   "status", BCON_INT64 (1),
   "instock.qty", BCON_INT64 (1), "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 49 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_NOT_FIELD (doc, "size");
         ASSERT_HAS_FIELD (doc, "instock");
         {
            bson_iter_t iter;

            BSON_ASSERT (bson_iter_init_find (&iter, doc, "instock"));
            while (bson_iter_next (&iter)) {
               bson_t subdoc;

               bson_iter_bson (&iter, &subdoc);
               ASSERT_HAS_NOT_FIELD (&subdoc, "warehouse");
               ASSERT_HAS_FIELD (&subdoc, "qty");
            }
         }
      }
   }
   /* Start Example 49 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 49 Post */
}


static void
test_example_50 (mongoc_database_t *db)
{
   /* Start Example 50 */
   mongoc_collection_t *collection;
   bson_t *filter;
   bson_t *opts;
   mongoc_cursor_t *cursor;

   collection = mongoc_database_get_collection (db, "inventory");
   filter = BCON_NEW ("status", BCON_UTF8 ("A"));
   opts = BCON_NEW ("projection", "{", "item", BCON_INT64 (1),
   "status", BCON_INT64 (1),
   "instock", "{",
   "$slice", BCON_INT64 (-1),
   "}", "}");
   cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
   /* End Example 50 */
   {
      const bson_t *doc;

      while (mongoc_cursor_next (cursor, &doc)) {
         bson_t subdoc;

         ASSERT_HAS_FIELD (doc, "_id");
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "status");
         ASSERT_HAS_NOT_FIELD (doc, "size");
         ASSERT_HAS_FIELD (doc, "instock");
         bson_lookup_doc (doc, "instock", &subdoc);
         ASSERT_CMPUINT32 (1, ==, bson_count_keys (&subdoc));
      }
   }
   /* Start Example 50 Post */
   mongoc_cursor_destroy (cursor);
   bson_destroy (opts);
   bson_destroy (filter);
   mongoc_collection_destroy (collection);
   /* End Example 50 Post */
}


static void
test_example_51 (mongoc_database_t *db)
{
   /* Start Example 51 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("canvas"),
      "qty", BCON_INT64 (100),
      "size", "{",
      "h", BCON_DOUBLE (28),
      "w", BCON_DOUBLE (35.5),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("mat"),
      "qty", BCON_INT64 (85),
      "size", "{",
      "h", BCON_DOUBLE (27.9),
      "w", BCON_DOUBLE (35.5),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("mousepad"),
      "qty", BCON_INT64 (25),
      "size", "{",
      "h", BCON_DOUBLE (19),
      "w", BCON_DOUBLE (22.85),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("P"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "qty", BCON_INT64 (50),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("P"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "qty", BCON_INT64 (100),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "qty", BCON_INT64 (75),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "qty", BCON_INT64 (45),
      "size", "{",
      "h", BCON_DOUBLE (10),
      "w", BCON_DOUBLE (15.25),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("sketchbook"),
      "qty", BCON_INT64 (80),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("sketch pad"),
      "qty", BCON_INT64 (95),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30.5),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 51 */

done:
   /* Start Example 51 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 51 Post */
}


static void
test_example_52 (mongoc_database_t *db)
{
   /* Start Example 52 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bson_t *update;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW ("item", BCON_UTF8 ("paper"));
   update = BCON_NEW (
      "$set", "{",
      "size.uom", BCON_UTF8 ("cm"),
      "status", BCON_UTF8 ("P"),
      "}",
      "$currentDate", "{",
      "lastModified", BCON_BOOL (true),
      "}");

   r = mongoc_collection_update_one(collection, selector, update, NULL, NULL, &error);
   bson_destroy (selector);
   bson_destroy (update);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 52 */
   {
      bson_t *filter;
      mongoc_cursor_t *cursor;
      const bson_t *doc;

      filter = BCON_NEW ("item", BCON_UTF8 ("paper"));
      cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_CMPSTR (bson_lookup_utf8 (doc, "size.uom"), "cm");
         ASSERT_CMPSTR (bson_lookup_utf8 (doc, "status"), "P");
         ASSERT_HAS_FIELD (doc, "lastModified");
      }
      mongoc_cursor_destroy (cursor);
      bson_destroy (filter);
   }
done:
   /* Start Example 52 Post */
   mongoc_collection_destroy (collection);
   /* End Example 52 Post */
}


static void
test_example_53 (mongoc_database_t *db)
{
   /* Start Example 53 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bson_t *update;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW (
      "qty", "{",
      "$lt", BCON_INT64 (50),
      "}");
   update = BCON_NEW (
      "$set", "{",
      "size.uom", BCON_UTF8 ("in"),
      "status", BCON_UTF8 ("P"),
      "}",
      "$currentDate", "{",
      "lastModified", BCON_BOOL (true),
      "}");

   r = mongoc_collection_update_many(collection, selector, update, NULL, NULL, &error);
   bson_destroy (selector);
   bson_destroy (update);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 53 */
   {
      bson_t *filter;
      mongoc_cursor_t *cursor;
      const bson_t *doc;

      filter = BCON_NEW (
         "qty", "{",
         "$lt", BCON_INT64 (50),
         "}");
      cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);
      while (mongoc_cursor_next (cursor, &doc)) {
         ASSERT_CMPSTR (bson_lookup_utf8 (doc, "size.uom"), "in");
         ASSERT_CMPSTR (bson_lookup_utf8 (doc, "status"), "P");
         ASSERT_HAS_FIELD (doc, "lastModified");
      }
      mongoc_cursor_destroy (cursor);
      bson_destroy (filter);
   }
done:
   /* Start Example 53 Post */
   mongoc_collection_destroy (collection);
   /* End Example 53 Post */
}


static void
test_example_54 (mongoc_database_t *db)
{
   /* Start Example 54 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bson_t *replacement;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW ("item", BCON_UTF8 ("paper"));
   replacement = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "instock", "[",
      "{",
      "warehouse", BCON_UTF8 ("A"),
      "qty", BCON_INT64 (60),
      "}","{",
      "warehouse", BCON_UTF8 ("B"),
      "qty", BCON_INT64 (40),
      "}",
      "]");

   /* MONGOC_UPDATE_NONE means "no special options" */
   r = mongoc_collection_replace_one(collection, selector, replacement, NULL, NULL, &error);
   bson_destroy (selector);
   bson_destroy (replacement);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 54 */
   {
      bson_t *filter;
      bson_t *opts;
      mongoc_cursor_t *cursor;
      const bson_t *doc;

      filter = BCON_NEW ("item", BCON_UTF8 ("paper"));
      opts = BCON_NEW ("projection", "{", "_id", BCON_INT64 (0), "}");
      cursor = mongoc_collection_find_with_opts (collection, filter, opts, NULL);
      while (mongoc_cursor_next (cursor, &doc)) {
         bson_t subdoc;

         ASSERT_CMPUINT32 (2, ==, bson_count_keys (doc));
         ASSERT_HAS_FIELD (doc, "item");
         ASSERT_HAS_FIELD (doc, "instock");
         bson_lookup_doc (doc, "instock", &subdoc);
         ASSERT_CMPUINT32 (2, ==, bson_count_keys (&subdoc));
      }
      mongoc_cursor_destroy (cursor);
      bson_destroy (opts);
      bson_destroy (filter);
   }
done:
   /* Start Example 54 Post */
   mongoc_collection_destroy (collection);
   /* End Example 54 Post */
}


static void
test_example_55 (mongoc_database_t *db)
{
   /* Start Example 55 */
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_t *doc;
   bool r;
   bson_error_t error;
   bson_t reply;

   collection = mongoc_database_get_collection (db, "inventory");
   bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
   doc = BCON_NEW (
      "item", BCON_UTF8 ("journal"),
      "qty", BCON_INT64 (25),
      "size", "{",
      "h", BCON_DOUBLE (14),
      "w", BCON_DOUBLE (21),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("notebook"),
      "qty", BCON_INT64 (50),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("P"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("paper"),
      "qty", BCON_INT64 (100),
      "size", "{",
      "h", BCON_DOUBLE (8.5),
      "w", BCON_DOUBLE (11),
      "uom", BCON_UTF8 ("in"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("planner"),
      "qty", BCON_INT64 (75),
      "size", "{",
      "h", BCON_DOUBLE (22.85),
      "w", BCON_DOUBLE (30),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("D"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   doc = BCON_NEW (
      "item", BCON_UTF8 ("postcard"),
      "qty", BCON_INT64 (45),
      "size", "{",
      "h", BCON_DOUBLE (10),
      "w", BCON_DOUBLE (15.25),
      "uom", BCON_UTF8 ("cm"),
      "}",
      "status", BCON_UTF8 ("A"));

   r = mongoc_bulk_operation_insert_with_opts (bulk, doc, NULL, &error);
   bson_destroy (doc);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }

   /* "reply" is initialized on success or error */
   r = (bool) mongoc_bulk_operation_execute (bulk, &reply, &error);
   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }
   /* End Example 55 */
   ASSERT_COUNT (5, collection);
done:
   /* Start Example 55 Post */
   bson_destroy (&reply);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   /* End Example 55 Post */
}


static void
test_example_57 (mongoc_database_t *db)
{
   /* Start Example 57 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW ("status", BCON_UTF8 ("A"));

   r = mongoc_collection_delete_many (collection, selector, NULL, NULL, &error);
   bson_destroy (selector);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 57 */
   ASSERT_COUNT (3, collection);
done:
   /* Start Example 57 Post */
   mongoc_collection_destroy (collection);
   /* End Example 57 Post */
}


static void
test_example_58 (mongoc_database_t *db)
{
   /* Start Example 58 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW ("status", BCON_UTF8 ("D"));

   r = mongoc_collection_delete_one (collection, selector, NULL, NULL, &error);
   bson_destroy (selector);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 58 */
   ASSERT_COUNT (2, collection);
done:
   /* Start Example 58 Post */
   mongoc_collection_destroy (collection);
   /* End Example 58 Post */
}


static void
test_example_56 (mongoc_database_t *db)
{
   /* Start Example 56 */
   mongoc_collection_t *collection;
   bson_t *selector;
   bool r;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   selector = BCON_NEW (NULL);

   r = mongoc_collection_delete_many (collection, selector, NULL, NULL, &error);
   bson_destroy (selector);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
      goto done;
   }
   /* End Example 56 */
   ASSERT_COUNT (0, collection);
done:
   /* Start Example 56 Post */
   mongoc_collection_destroy (collection);
   /* End Example 56 Post */
}


/* clang-format on */

static bool
insert_pet (mongoc_collection_t *collection, bool is_adoptable)
{
   bson_t *doc = NULL;
   bson_error_t error;
   bool rc;

   doc = BCON_NEW ("adoptable", BCON_BOOL (is_adoptable));

   rc = mongoc_collection_insert_one (collection, doc, NULL, NULL, &error);
   if (!rc) {
      MONGOC_ERROR ("insert into pets.%s failed: %s", mongoc_collection_get_name (collection), error.message);
      goto cleanup;
   }

cleanup:
   bson_destroy (doc);
   return rc;
}


static bool
pet_setup (mongoc_collection_t *cats_collection, mongoc_collection_t *dogs_collection)
{
   bool ok = true;

   mongoc_collection_drop (cats_collection, NULL);
   mongoc_collection_drop (dogs_collection, NULL);

   ok = insert_pet (cats_collection, true);
   if (!ok) {
      goto done;
   }

   ok = insert_pet (dogs_collection, true);
   if (!ok) {
      goto done;
   }

   ok = insert_pet (dogs_collection, false);
   if (!ok) {
      goto done;
   }
done:
   return ok;
}

/*
 * Increment 'accumulator' by the amount of adoptable pets in the given
 * collection.
 */
static bool
accumulate_adoptable_count (const mongoc_client_session_t *cs,
                            mongoc_collection_t *collection,
                            int64_t *accumulator /* OUT */
)
{
   bson_t *pipeline = NULL;
   mongoc_cursor_t *cursor = NULL;
   bool rc = false;
   const bson_t *doc = NULL;
   bson_error_t error;
   bson_iter_t iter;
   bson_t opts = BSON_INITIALIZER;

   rc = mongoc_client_session_append (cs, &opts, &error);
   if (!rc) {
      MONGOC_ERROR ("could not apply session options: %s", error.message);
      goto cleanup;
   }

   pipeline = BCON_NEW ("pipeline",
                        "[",
                        "{",
                        "$match",
                        "{",
                        "adoptable",
                        BCON_BOOL (true),
                        "}",
                        "}",
                        "{",
                        "$count",
                        BCON_UTF8 ("adoptableCount"),
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, pipeline, &opts, NULL);
   bson_destroy (&opts);

   rc = mongoc_cursor_next (cursor, &doc);

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("could not get adoptableCount: %s", error.message);
      rc = false;
      goto cleanup;
   }

   if (!rc) {
      MONGOC_ERROR ("%s", "cursor has no results");
      goto cleanup;
   }

   rc = bson_iter_init_find (&iter, doc, "adoptableCount");
   if (rc) {
      *accumulator += bson_iter_as_int64 (&iter);
   } else {
      MONGOC_ERROR ("%s", "missing key: 'adoptableCount'");
      goto cleanup;
   }

cleanup:
   bson_destroy (pipeline);
   mongoc_cursor_destroy (cursor);
   return rc;
}

/*
 * JIRA: https://jira.mongodb.org/browse/DRIVERS-2181
 */
static void
test_example_59 (mongoc_database_t *db)
{
   BSON_UNUSED (db);

   if (!test_framework_skip_if_no_txns ()) {
      return;
   }

   mongoc_client_t *client = NULL;
   client = test_framework_new_default_client ();

   /* Start Snapshot Query Example 1 */
   mongoc_client_session_t *cs = NULL;
   mongoc_collection_t *cats_collection = NULL;
   mongoc_collection_t *dogs_collection = NULL;
   int64_t adoptable_pets_count = 0;
   bson_error_t error;
   mongoc_session_opt_t *session_opts;

   cats_collection = mongoc_client_get_collection (client, "pets", "cats");
   dogs_collection = mongoc_client_get_collection (client, "pets", "dogs");

   /* Seed 'pets.cats' and 'pets.dogs' with example data */
   if (!pet_setup (cats_collection, dogs_collection)) {
      goto cleanup;
   }

   /* start a snapshot session */
   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_snapshot (session_opts, true);
   cs = mongoc_client_start_session (client, session_opts, &error);
   mongoc_session_opts_destroy (session_opts);
   if (!cs) {
      MONGOC_ERROR ("Could not start session: %s", error.message);
      goto cleanup;
   }

   /*
    * Perform the following aggregation pipeline, and accumulate the count in
    * `adoptable_pets_count`.
    *
    *  adoptablePetsCount = db.cats.aggregate(
    *      [ { "$match": { "adoptable": true } },
    *        { "$count": "adoptableCatsCount" } ], session=s
    *  ).next()["adoptableCatsCount"]
    *
    *  adoptablePetsCount += db.dogs.aggregate(
    *      [ { "$match": { "adoptable": True} },
    *        { "$count": "adoptableDogsCount" } ], session=s
    *  ).next()["adoptableDogsCount"]
    *
    * Remember in order to apply the client session to
    * this operation, you must append the client session to the options passed
    * to `mongoc_collection_aggregate`, i.e.,
    *
    * mongoc_client_session_append (cs, &opts, &error);
    * cursor = mongoc_collection_aggregate (
    *    collection, MONGOC_QUERY_NONE, pipeline, &opts, NULL);
    */
   accumulate_adoptable_count (cs, cats_collection, &adoptable_pets_count);
   accumulate_adoptable_count (cs, dogs_collection, &adoptable_pets_count);

   printf ("there are %" PRId64 " adoptable pets\n", adoptable_pets_count);

   /* End Snapshot Query Example 1 */

   if (adoptable_pets_count != 2) {
      MONGOC_ERROR ("there should be exatly 2 adoptable_pets_count, found: %" PRId64, adoptable_pets_count);
   }

   /* Start Snapshot Query Example 1 Post */
cleanup:
   mongoc_collection_destroy (dogs_collection);
   mongoc_collection_destroy (cats_collection);
   mongoc_client_session_destroy (cs);
   mongoc_client_destroy (client);
   /* End Snapshot Query Example 1 Post */
}

static bool
retail_setup (mongoc_collection_t *sales_collection)
{
   bool ok = true;
   bson_t *doc = NULL;
   bson_error_t error;
   struct timeval tv;
   int64_t unix_time_now = 0;

   if (bson_gettimeofday (&tv)) {
      MONGOC_ERROR ("could not get time of day");
      goto cleanup;
   }
   unix_time_now = 1000 * tv.tv_sec;

   mongoc_collection_drop (sales_collection, NULL);

   doc =
      BCON_NEW ("shoeType", BCON_UTF8 ("boot"), "price", BCON_INT64 (30), "saleDate", BCON_DATE_TIME (unix_time_now));

   ok = mongoc_collection_insert_one (sales_collection, doc, NULL, NULL, &error);
   if (!ok) {
      MONGOC_ERROR ("insert into retail.sales failed: %s", error.message);
      goto cleanup;
   }

cleanup:
   bson_destroy (doc);
   return ok;
}


static void
test_example_60 (mongoc_database_t *db)
{
   BSON_UNUSED (db);

   if (!test_framework_skip_if_no_txns ()) {
      return;
   }

   mongoc_client_t *client = NULL;
   client = test_framework_new_default_client ();

   /* Start Snapshot Query Example 2 */
   mongoc_client_session_t *cs = NULL;
   mongoc_collection_t *sales_collection = NULL;
   bson_error_t error;
   mongoc_session_opt_t *session_opts;
   bson_t *pipeline = NULL;
   bson_t opts = BSON_INITIALIZER;
   mongoc_cursor_t *cursor = NULL;
   const bson_t *doc = NULL;
   bool ok = true;
   bson_iter_t iter;
   int64_t total_sales = 0;

   sales_collection = mongoc_client_get_collection (client, "retail", "sales");

   /* seed 'retail.sales' with example data */
   if (!retail_setup (sales_collection)) {
      goto cleanup;
   }

   /* start a snapshot session */
   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_snapshot (session_opts, true);
   cs = mongoc_client_start_session (client, session_opts, &error);
   mongoc_session_opts_destroy (session_opts);
   if (!cs) {
      MONGOC_ERROR ("Could not start session: %s", error.message);
      goto cleanup;
   }

   if (!mongoc_client_session_append (cs, &opts, &error)) {
      MONGOC_ERROR ("could not apply session options: %s", error.message);
      goto cleanup;
   }

   pipeline = BCON_NEW ("pipeline",
                        "[",
                        "{",
                        "$match",
                        "{",
                        "$expr",
                        "{",
                        "$gt",
                        "[",
                        "$saleDate",
                        "{",
                        "$dateSubtract",
                        "{",
                        "startDate",
                        "$$NOW",
                        "unit",
                        BCON_UTF8 ("day"),
                        "amount",
                        BCON_INT64 (1),
                        "}",
                        "}",
                        "]",
                        "}",
                        "}",
                        "}",
                        "{",
                        "$count",
                        BCON_UTF8 ("totalDailySales"),
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (sales_collection, MONGOC_QUERY_NONE, pipeline, &opts, NULL);
   bson_destroy (&opts);

   ok = mongoc_cursor_next (cursor, &doc);

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("could not get totalDailySales: %s", error.message);
      goto cleanup;
   }

   if (!ok) {
      MONGOC_ERROR ("%s", "cursor has no results");
      goto cleanup;
   }

   ok = bson_iter_init_find (&iter, doc, "totalDailySales");
   if (ok) {
      total_sales = bson_iter_as_int64 (&iter);
   } else {
      MONGOC_ERROR ("%s", "missing key: 'totalDailySales'");
      goto cleanup;
   }

   /* End Snapshot Query Example 2 */

   if (total_sales != 1) {
      MONGOC_ERROR ("there should be exactly 1 total_sales, found: %" PRId64, total_sales);
   }

   /* Start Snapshot Query Example 2 Post */
cleanup:
   mongoc_collection_destroy (sales_collection);
   mongoc_client_session_destroy (cs);
   mongoc_cursor_destroy (cursor);
   bson_destroy (pipeline);
   mongoc_client_destroy (client);
   /* End Snapshot Query Example 2 Post */
}

/* clang-format off */

typedef struct {
   bson_mutex_t lock;
   mongoc_collection_t *collection;
   bool done;
} change_stream_ctx_t;


static
BSON_THREAD_FUN (insert_docs, p)
{
   change_stream_ctx_t *ctx = (change_stream_ctx_t *) p;
   bson_t doc = BSON_INITIALIZER;
   bson_error_t error;
   bool r;

   while (true) {
      bson_mutex_lock (&ctx->lock);
      r = mongoc_collection_insert (
         ctx->collection, MONGOC_INSERT_NONE, &doc, NULL, &error);
      ASSERT_OR_PRINT (r, error);
      if (ctx->done) {
         bson_destroy (&doc);
         bson_mutex_unlock (&ctx->lock);
         BSON_THREAD_RETURN;
      }

      bson_mutex_unlock (&ctx->lock);
      _mongoc_usleep (100 * 1000);  /* 100 ms */
   }
}


static void
test_sample_change_stream_command (sample_command_fn_t fn,
                                   mongoc_database_t *db)
{
   mongoc_client_t *client;
   change_stream_ctx_t ctx;
   bson_thread_t thread;
   int r;

   /* change streams require a replica set */
   if (test_framework_skip_if_not_replset () &&
       test_framework_skip_if_slow ()) {

      /* separate client for the background thread */
      client = test_framework_new_default_client ();

      bson_mutex_init (&ctx.lock);
      ctx.collection = mongoc_client_get_collection (
         client, db->name, "inventory");
      ctx.done = false;

      r = mcommon_thread_create (&thread, insert_docs, (void *) &ctx);
      ASSERT_OR_PRINT_ERRNO (r == 0, r);

      capture_logs (true);
      fn (db);
      ASSERT_NO_CAPTURED_LOGS ("change stream examples");

      bson_mutex_lock (&ctx.lock);
      ctx.done = true;
      bson_mutex_unlock (&ctx.lock);
      mcommon_thread_join (thread);

      mongoc_collection_destroy (ctx.collection);
      mongoc_client_destroy (client);
   }
}


static void
test_example_change_stream (mongoc_database_t *db)
{
   /* Start Changestream Example 1 */
   mongoc_collection_t *collection;
   bson_t *pipeline = bson_new ();
   bson_t opts = BSON_INITIALIZER;
   mongoc_change_stream_t *stream;
   const bson_t *change;
   const bson_t *resume_token;
   bson_error_t error;

   collection = mongoc_database_get_collection (db, "inventory");
   stream = mongoc_collection_watch (collection, pipeline, NULL /* opts */);
   mongoc_change_stream_next (stream, &change);
   if (mongoc_change_stream_error_document (stream, &error, NULL)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_change_stream_destroy (stream);
   /* End Changestream Example 1 */

   /* Start Changestream Example 2 */
   BSON_APPEND_UTF8 (&opts, "fullDocument", "updateLookup");
   stream = mongoc_collection_watch (collection, pipeline, &opts);
   mongoc_change_stream_next (stream, &change);
   if (mongoc_change_stream_error_document (stream, &error, NULL)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_change_stream_destroy (stream);
   /* End Changestream Example 2 */

   bson_reinit (&opts);

   /* Start Changestream Example 3 */
   stream = mongoc_collection_watch (collection, pipeline, NULL);
   if (mongoc_change_stream_next (stream, &change)) {
      resume_token = mongoc_change_stream_get_resume_token (stream);
      BSON_APPEND_DOCUMENT (&opts, "resumeAfter", resume_token);

      mongoc_change_stream_destroy (stream);
      stream = mongoc_collection_watch (collection, pipeline, &opts);
      mongoc_change_stream_next (stream, &change);
      mongoc_change_stream_destroy (stream);
   } else {
      if (mongoc_change_stream_error_document (stream, &error, NULL)) {
         MONGOC_ERROR ("%s\n", error.message);
      }

      mongoc_change_stream_destroy (stream);
   }
   /* End Changestream Example 3 */

   bson_destroy (pipeline);

   /* Start Changestream Example 4 */
   pipeline = BCON_NEW ("pipeline",
                        "[",
                        "{",
                        "$match",
                        "{",
                        "fullDocument.username",
                        BCON_UTF8 ("alice"),
                        "}",
                        "}",
                        "{",
                        "$addFields",
                        "{",
                        "newField",
                        BCON_UTF8 ("this is an added field!"),
                        "}",
                        "}",
                        "]");

   stream = mongoc_collection_watch (collection, pipeline, &opts);
   mongoc_change_stream_next (stream, &change);
   if (mongoc_change_stream_error_document (stream, &error, NULL)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_change_stream_destroy (stream);
   /* End Changestream Example 4 */

   bson_destroy (&opts);
   bson_destroy (pipeline);
   mongoc_collection_destroy (collection);
}


static void
test_sample_causal_consistency (mongoc_client_t *client)
{
   mongoc_session_opt_t *session_opts = NULL;
   mongoc_client_session_t *session1 = NULL;
   mongoc_client_session_t *session2 = NULL;
   mongoc_read_prefs_t *read_prefs = NULL;
   const bson_t *cluster_time = NULL;
   mongoc_write_concern_t *wc = NULL;
   mongoc_read_concern_t *rc = NULL;
   mongoc_collection_t *coll = NULL;
   mongoc_cursor_t *cursor = NULL;
   const bson_t *result = NULL;
   bson_t *update_opts = NULL;
   bson_t *insert_opts = NULL;
   bson_t *find_query = NULL;
   bson_t *find_opts = NULL;
   bson_t *insert = NULL;
   bson_t *update = NULL;
   bson_t *query = NULL;
   bson_t *doc = NULL;
   char *json = NULL;
   uint32_t timestamp;
   uint32_t increment;
   bson_error_t error;
   bool res;
   
   ASSERT (client);

   if (!test_framework_skip_if_no_txns ()) {
      return;
   }

   /* Seed the 'db.items' collection with a document. */
   coll = mongoc_client_get_collection (client, "db", "items");
   mongoc_collection_drop (coll, &error);

   doc = BCON_NEW ("sku", "111", "name", "Peanuts",
		   "start", BCON_DATE_TIME (bson_get_monotonic_time ()));

   res = mongoc_collection_insert_one (coll, doc, NULL, NULL, &error);
   if (!res) {
      fprintf (stderr, "insert failed: %s\n", error.message);
      goto cleanup;
   }

   /* Start Causal Consistency Example 1 */

   /* Use a causally-consistent session to run some operations. */

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   mongoc_collection_set_write_concern (coll, wc);

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_MAJORITY);
   mongoc_collection_set_read_concern (coll, rc);

   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_causal_consistency (session_opts, true);

   session1 = mongoc_client_start_session (client, session_opts, &error);
   if (!session1) {
      fprintf (stderr, "couldn't start session: %s\n", error.message);
      goto cleanup;
   }

   /* Run an update_one with our causally-consistent session. */
   update_opts = bson_new ();
   res = mongoc_client_session_append (session1, update_opts, &error);
   if (!res) {
      fprintf (stderr, "couldn't add session to opts: %s\n", error.message);
      goto cleanup;
   }

   query = BCON_NEW ("sku", "111");
   update = BCON_NEW ("$set", "{", "end",
		      BCON_DATE_TIME (bson_get_monotonic_time ()), "}");
   res = mongoc_collection_update_one (coll,
				       query,
				       update,
				       update_opts,
				       NULL, /* reply */
				       &error);

   if (!res) {
      fprintf (stderr, "update failed: %s\n", error.message);
      goto cleanup;
   }

   /* Run an insert with our causally-consistent session */
   insert_opts = bson_new ();
   res = mongoc_client_session_append (session1, insert_opts, &error);
   if (!res) {
      fprintf (stderr, "couldn't add session to opts: %s\n", error.message);
      goto cleanup;
   }

   insert = BCON_NEW ("sku", "nuts-111", "name", "Pecans",
		      "start", BCON_DATE_TIME (bson_get_monotonic_time ()));
   res = mongoc_collection_insert_one (coll, insert, insert_opts, NULL, &error);
   if (!res) {
      fprintf (stderr, "insert failed: %s\n", error.message);
      goto cleanup;
   }

   /* End Causal Consistency Example 1 */

   /* Start Causal Consistency Example 2 */

   /* Make a new session, session2, and make it causally-consistent
    * with session1, so that session2 will read session1's writes. */
   session2 = mongoc_client_start_session (client, session_opts, &error);
   if (!session2) {
      fprintf (stderr, "couldn't start session: %s\n", error.message);
      goto cleanup;
   }

   /* Set the cluster time for session2 to session1's cluster time */
   cluster_time = mongoc_client_session_get_cluster_time (session1);
   mongoc_client_session_advance_cluster_time (session2, cluster_time);

   /* Set the operation time for session2 to session2's operation time */
   mongoc_client_session_get_operation_time (session1, &timestamp, &increment);
   mongoc_client_session_advance_operation_time (session2,
						 timestamp,
						 increment);

   /* Run a find on session2, which should now find all writes done
    * inside of session1 */
   find_opts = bson_new ();
   res = mongoc_client_session_append (session2, find_opts, &error);
   if (!res) {
      fprintf (stderr, "couldn't add session to opts: %s\n", error.message);
      goto cleanup;
   }

   find_query = BCON_NEW ("end", BCON_NULL);
   read_prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   cursor = mongoc_collection_find_with_opts (coll,
					      query,
					      find_opts,
					      read_prefs);

   while (mongoc_cursor_next (cursor, &result)) {
      json = bson_as_json (result, NULL);
      fprintf (stdout, "Document: %s\n", json);
      bson_free (json);
   }

   if (mongoc_cursor_error (cursor, &error)) {
      fprintf (stderr, "cursor failure: %s\n", error.message);
      goto cleanup;
   }

   /* End Causal Consistency Example 2 */

 cleanup:

   bson_destroy (doc);
   bson_destroy (query);
   bson_destroy (insert);
   bson_destroy (update);
   bson_destroy (find_query);
   bson_destroy (update_opts);
   bson_destroy (find_opts);
   bson_destroy (insert_opts);

   mongoc_read_concern_destroy (rc);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_write_concern_destroy (wc);
   mongoc_collection_destroy (coll);
   mongoc_cursor_destroy (cursor);
   mongoc_session_opts_destroy (session_opts);
   mongoc_client_session_destroy (session1);
   mongoc_client_session_destroy (session2);
}


static void
test_sample_aggregation (mongoc_database_t *db)
{
   /* Start Aggregation Example 1 */
   mongoc_collection_t *collection;
   bson_t *pipeline;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;

   collection = mongoc_database_get_collection (db, "sales");

   pipeline = BCON_NEW ("pipeline", "[",
                        "{",
                        "$match", "{",
                        "items.fruit", BCON_UTF8 ("banana"),
                        "}",
                        "}",
                        "{",
                        "$sort", "{",
                        "date", BCON_INT32 (1),
                        "}",
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (
      collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);
   bson_destroy (pipeline);

   while (mongoc_cursor_next (cursor, &doc)) {
      /* Do something with each doc here */
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_cursor_destroy (cursor);
   /* End Aggregation Example 1 */

   /* Start Aggregation Example 2 */
   pipeline = BCON_NEW ("pipeline", "[",
                        "{",
                        "$unwind", BCON_UTF8 ("$items"),
                        "}",
                        "{",
                        "$match", "{",
                        "items.fruit", BCON_UTF8 ("banana"),
                        "}",
                        "}",
                        "{",
                        "$group", "{",
                        "_id", "{",
                        "day", "{",
                        "$dayOfWeek", BCON_UTF8 ("$date"),
                        "}",
                        "}",
                        "count", "{",
                        "$sum", BCON_UTF8 ("$items.quantity"),
                        "}",
                        "}",
                        "}",
                        "{",
                        "$project", "{",
                        "dayOfWeek", BCON_UTF8 ("$_id.day"),
                        "numberSold", BCON_UTF8 ("$count"),
                        "_id", BCON_INT32 (0),
                        "}",
                        "}",
                        "{",
                        "$sort", "{",
                        "numberSold", BCON_INT32 (1),
                        "}",
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (
      collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);
   bson_destroy (pipeline);

   while (mongoc_cursor_next (cursor, &doc)) {
      /* Do something with each doc here */
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_cursor_destroy (cursor);
   /* End Aggregation Example 2 */

   /* Start Aggregation Example 3 */
   pipeline = BCON_NEW ("pipeline", "[",
                        "{",
                        "$unwind", BCON_UTF8 ("$items"),
                        "}",
                        "{",
                        "$group", "{",
                        "_id", "{",
                        "day", "{",
                        "$dayOfWeek", BCON_UTF8 ("$date"),
                        "}",
                        "}",
                        "items_sold", "{",
                        "$sum", BCON_UTF8 ("$items.quantity"),
                        "}",
                        "revenue", "{",
                        "$sum", "{",
                        "$multiply", "[",
                        BCON_UTF8 ("$items.quantity"),
                        BCON_UTF8 ("$items.price"),
                        "]",
                        "}",
                        "}",
                        "}",
                        "}",
                        "{",
                        "$project", "{",
                        "day", BCON_UTF8 ("$_id.day"),
                        "revenue", BCON_INT32 (1),
                        "items_sold", BCON_INT32 (1),
                        "discount", "{",
                        "$cond", "{",
                        "if", "{",
                        "$lte", "[",
                        "$revenue",
                        BCON_INT32 (250),
                        "]",
                        "}",
                        "then", BCON_INT32 (25),
                        "else", BCON_INT32 (0),
                        "}",
                        "}",
                        "}",
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (
      collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);
   bson_destroy (pipeline);

   while (mongoc_cursor_next (cursor, &doc)) {
      /* Do something with each doc here */
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_cursor_destroy (cursor);
   /* End Aggregation Example 3 */

   mongoc_collection_destroy (collection);


   /* Start Aggregation Example 4 */
   collection = mongoc_database_get_collection (db, "air_alliances");
   pipeline = BCON_NEW ("pipeline", "[",
                        "{",
                        "$lookup", "{",
                        "from", BCON_UTF8 ("air_airlines"),
                        "let", "{",
                        "constituents", BCON_UTF8 ("$airlines"),
                        "}",
                        "pipeline", "[",
                        "{",
                        "$match", "{",
                        "$expr", "{",
                        "$in", "[",
                        "$name",
                        BCON_UTF8 ("$$constituents"),
                        "]",
                        "}",
                        "}",
                        "}",
                        "]",
                        "as", BCON_UTF8 ("airlines"),
                        "}",
                        "}",
                        "{",
                        "$project", "{",
                        "_id", BCON_INT32 (0),
                        "name", BCON_INT32 (1),
                        "airlines", "{",
                        "$filter", "{",
                        "input", BCON_UTF8 ("$airlines"),
                        "as", BCON_UTF8 ("airline"),
                        "cond", "{",
                        "$eq", "[",
                        BCON_UTF8 ("$$airline.country"),
                        BCON_UTF8 ("Canada"),
                        "]",
                        "}",
                        "}",
                        "}",
                        "}",
                        "}",
                        "]");

   cursor = mongoc_collection_aggregate (
      collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);
   bson_destroy (pipeline);

   while (mongoc_cursor_next (cursor, &doc)) {
      /* Do something with each doc here */
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   /* End Aggregation Example 4 */

   ASSERT_NO_CAPTURED_LOGS ("sample aggregation examples");
}

static void
test_sample_run_command (mongoc_database_t *db)
{
   /* Start runCommand Example 1 */
   bson_t *run_command;
   bson_t reply;
   bson_error_t error;
   bool r;

   run_command = BCON_NEW ("buildInfo", BCON_INT32 (1));

   r = mongoc_database_write_command_with_opts (
      db, run_command, NULL /* opts */, &reply, &error);
   bson_destroy (run_command);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   /* Do something with reply here */

   bson_destroy (&reply);
   /* End runCommand Example 1 */

   ASSERT_NO_CAPTURED_LOGS ("sample runCommand examples");
}

static void
test_sample_indexes (mongoc_database_t *db)
{
   /* Start Index Example 1 */
   const char *collection_name = "records";
   bson_t reply;
   bson_t keys;
   bson_t *index_opts;
   bson_error_t error;
   bool r;
   mongoc_collection_t *collection;
   mongoc_index_model_t *im;

   bson_init (&keys);
   BSON_APPEND_INT32 (&keys, "score", 1);
   collection = mongoc_database_get_collection (db, collection_name);
   im = mongoc_index_model_new (&keys, NULL /* opts */);
   r = mongoc_collection_create_indexes_with_opts (
      collection, &im, 1, NULL /* opts */, &reply, &error);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   /* Do something with reply here */

   bson_destroy (&reply);
   bson_destroy (&keys);
   mongoc_index_model_destroy (im);
   mongoc_collection_destroy (collection);
   /* End Index Example 1 */

   /* Start Index Example 2 */
   collection_name = "restaurants";
   collection = mongoc_database_get_collection (db, collection_name);

   bson_init (&keys);
   BSON_APPEND_INT32 (&keys, "cuisine", 1);
   BSON_APPEND_INT32 (&keys, "name", 1);
   index_opts = BCON_NEW ("partialFilterExpression",
                          "{",
                          "rating",
                          "{",
                          "$gt",
                          BCON_INT32 (5),
                          "}",
                          "}");

   im = mongoc_index_model_new (&keys, index_opts);
   r = mongoc_collection_create_indexes_with_opts (
      collection, &im, 1, NULL /* opts */, &reply, &error);

   if (!r) {
      MONGOC_ERROR ("%s\n", error.message);
   }

   /* Do something with reply here */

   bson_destroy (&reply);
   bson_destroy (index_opts);
   bson_destroy (&keys);
   mongoc_index_model_destroy (im);
   mongoc_collection_destroy (collection);
   /* End Index Example 2 */

   ASSERT_NO_CAPTURED_LOGS ("sample index examples");
}


/* convenience function for testing the outcome of example code */
static void
find_and_match (mongoc_collection_t *collection,
                const char *filter,
                const char *pattern)
{
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;

   cursor = mongoc_collection_find_with_opts (
      collection, tmp_bson (filter), NULL, NULL);

   if (!mongoc_cursor_next (cursor, &doc)) {
      if (mongoc_cursor_error (cursor, &error)) {
         ASSERT_OR_PRINT (false, error);
      }

      test_error (
         "No document in %s matching %s", collection->collection, filter);
   }

   ASSERT_MATCH (doc, pattern);
   mongoc_cursor_destroy (cursor);
}


/* setup, preliminary to transactions example code */
static void
insert_employee (mongoc_client_t *client, int employee)
{
   mongoc_collection_t *employees;
   mongoc_collection_t *events;
   bson_error_t error;
   bool r;
   
   ASSERT (client);

   employees = mongoc_client_get_collection (client, "hr", "employees");
   mongoc_collection_drop (employees, NULL);

   r = mongoc_collection_insert_one (
      employees,
      tmp_bson ("{'employee': %d, 'status': 'Active'}", employee),
      NULL,
      NULL,
      &error);
   ASSERT_OR_PRINT (r, error);

   events = mongoc_client_get_collection (client, "reporting", "events");

   mongoc_collection_drop (events, NULL);

   r = mongoc_collection_insert_one (
      events,
      tmp_bson ("{'employee': %d, 'status': {'new': 'Active', 'old': null}}",
                employee),
      NULL,
      NULL,
      &error);
   ASSERT_OR_PRINT (r, error);

   mongoc_collection_destroy (employees);
   mongoc_collection_destroy (events);
}


/* clang-format on */
/* Start Transactions Retry Example 3 */
/* takes a session, an out-param for server reply, and out-param for error. */
typedef bool (*txn_func_t) (mongoc_client_session_t *, bson_t *, bson_error_t *);


/* runs transactions with retry logic */
bool
run_transaction_with_retry (txn_func_t txn_func, mongoc_client_session_t *cs, bson_error_t *error)
{
   bson_t reply;
   bool r;

   while (true) {
      /* perform transaction */
      r = txn_func (cs, &reply, error);
      if (r) {
         /* success */
         bson_destroy (&reply);
         return true;
      }

      MONGOC_WARNING ("Transaction aborted: %s", error->message);
      if (mongoc_error_has_label (&reply, "TransientTransactionError")) {
         /* on transient error, retry the whole transaction */
         MONGOC_WARNING ("TransientTransactionError, retrying transaction...");
         bson_destroy (&reply);
      } else {
         /* non-transient error */
         break;
      }
   }

   bson_destroy (&reply);
   return false;
}


/* commit transactions with retry logic */
bool
commit_with_retry (mongoc_client_session_t *cs, bson_error_t *error)
{
   bson_t reply;
   bool r;

   while (true) {
      /* commit uses write concern set at transaction start, see
       * mongoc_transaction_opts_set_write_concern */
      r = mongoc_client_session_commit_transaction (cs, &reply, error);
      if (r) {
         MONGOC_INFO ("Transaction committed");
         break;
      }

      if (mongoc_error_has_label (&reply, "UnknownTransactionCommitResult")) {
         MONGOC_WARNING ("UnknownTransactionCommitResult, retrying commit ...");
         bson_destroy (&reply);
      } else {
         /* commit failed, cannot retry */
         break;
      }
   }

   bson_destroy (&reply);

   return r;
}


/* updates two collections in a transaction and calls commit_with_retry */
bool
update_employee_info (mongoc_client_session_t *cs, bson_t *reply, bson_error_t *error)
{
   mongoc_client_t *client;
   mongoc_collection_t *employees;
   mongoc_collection_t *events;
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   mongoc_transaction_opt_t *txn_opts;
   bson_t opts = BSON_INITIALIZER;
   bson_t *filter = NULL;
   bson_t *update = NULL;
   bson_t *event = NULL;
   bool r;

   bson_init (reply);

   client = mongoc_client_session_get_client (cs);
   employees = mongoc_client_get_collection (client, "hr", "employees");
   events = mongoc_client_get_collection (client, "reporting", "events");

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_SNAPSHOT);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
   txn_opts = mongoc_transaction_opts_new ();
   mongoc_transaction_opts_set_read_concern (txn_opts, rc);
   mongoc_transaction_opts_set_write_concern (txn_opts, wc);

   r = mongoc_client_session_start_transaction (cs, txn_opts, error);
   if (!r) {
      goto done;
   }

   r = mongoc_client_session_append (cs, &opts, error);
   if (!r) {
      goto done;
   }

   filter = BCON_NEW ("employee", BCON_INT32 (3));
   update = BCON_NEW ("$set", "{", "status", "Inactive", "}");
   /* mongoc_collection_update_one will reinitialize reply */
   bson_destroy (reply);
   r = mongoc_collection_update_one (employees, filter, update, &opts, reply, error);

   if (!r) {
      goto abort;
   }

   event = BCON_NEW ("employee", BCON_INT32 (3));
   BCON_APPEND (event, "status", "{", "new", "Inactive", "old", "Active", "}");

   bson_destroy (reply);
   r = mongoc_collection_insert_one (events, event, &opts, reply, error);
   if (!r) {
      goto abort;
   }

   r = commit_with_retry (cs, error);

abort:
   if (!r) {
      MONGOC_ERROR ("Aborting due to error in transaction: %s", error->message);
      mongoc_client_session_abort_transaction (cs, NULL);
   }

done:
   mongoc_collection_destroy (employees);
   mongoc_collection_destroy (events);
   mongoc_read_concern_destroy (rc);
   mongoc_write_concern_destroy (wc);
   mongoc_transaction_opts_destroy (txn_opts);
   bson_destroy (&opts);
   bson_destroy (filter);
   bson_destroy (update);
   bson_destroy (event);

   return r;
}


void
example_func (mongoc_client_t *client)
{
   mongoc_client_session_t *cs;
   bson_error_t error;
   bool r;

   ASSERT (client);

   cs = mongoc_client_start_session (client, NULL, &error);
   if (!cs) {
      MONGOC_ERROR ("Could not start session: %s", error.message);
      return;
   }

   r = run_transaction_with_retry (update_employee_info, cs, &error);
   if (!r) {
      MONGOC_ERROR ("Could not update employee, permanent error: %s", error.message);
   }

   mongoc_client_session_destroy (cs);
}
/* End Transactions Retry Example 3 */

static void
test_sample_txn_commands (mongoc_client_t *client)
{
   mongoc_collection_t *employees;
   mongoc_collection_t *events;

   ASSERT (client);

   if (!test_framework_skip_if_no_txns ()) {
      return;
   }

   /* preliminary: create collections outside txn */
   insert_employee (client, 3);
   employees = mongoc_client_get_collection (client, "hr", "employees");
   events = mongoc_client_get_collection (client, "reporting", "events");

   capture_logs (true);

   /* test transactions retry example 3 */
   example_func (client);
   ASSERT_NO_CAPTURED_LOGS ("transactions retry example 3");
   find_and_match (employees, "{'employee': 3}", "{'status': 'Inactive'}");

   mongoc_collection_destroy (employees);
   mongoc_collection_destroy (events);
}

static mongoc_client_t *
get_client (void)
{
   return test_framework_new_default_client ();
}

/* Returns a test client without version API options configured. */
static mongoc_client_t *
get_client_for_version_api_example (void)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = test_framework_get_uri ();
   client = mongoc_client_new_from_uri (uri);
   ASSERT (client);
   test_framework_set_ssl_opts (client);
   mongoc_uri_destroy (uri);
   return client;
}

static bool
callback (mongoc_client_session_t *session, void *ctx, bson_t **reply, bson_error_t *error);

/* See additional usage of mongoc_client_session_with_transaction at
 * https://www.mongoc.org/libmongoc/1.15.3/mongoc_client_session_with_transaction.html
 */
/* Start Transactions withTxn API Example 1 */
static bool
with_transaction_example (bson_error_t *error)
{
   mongoc_client_t *client = NULL;
   mongoc_write_concern_t *wc = NULL;
   mongoc_read_concern_t *rc = NULL;
   mongoc_read_prefs_t *rp = NULL;
   mongoc_collection_t *coll = NULL;
   bool success = false;
   bool ret = false;
   bson_t *doc = NULL;
   bson_t *insert_opts = NULL;
   mongoc_client_session_t *session = NULL;
   mongoc_transaction_opt_t *txn_opts = NULL;

   /* For a replica set, include the replica set name and a seedlist of the
    * members in the URI string; e.g.
    * uri_repl = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:" \
    *    "27017/?replicaSet=myRepl";
    * client = mongoc_client_new (uri_repl);
    * For a sharded cluster, connect to the mongos instances; e.g.
    * uri_sharded =
    * "mongodb://mongos0.example.com:27017,mongos1.example.com:27017/";
    * client = mongoc_client_new (uri_sharded);
    */

   client = get_client ();

   /* Prereq: Create collections. */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   insert_opts = bson_new ();
   mongoc_write_concern_append (wc, insert_opts);
   coll = mongoc_client_get_collection (client, "mydb1", "foo");
   doc = BCON_NEW ("abc", BCON_INT32 (0));
   ret = mongoc_collection_insert_one (coll, doc, insert_opts, NULL /* reply */, error);
   if (!ret) {
      goto fail;
   }
   bson_destroy (doc);
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "mydb2", "bar");
   doc = BCON_NEW ("xyz", BCON_INT32 (0));
   ret = mongoc_collection_insert_one (coll, doc, insert_opts, NULL /* reply */, error);
   if (!ret) {
      goto fail;
   }

   /* Step 1: Start a client session. */
   session = mongoc_client_start_session (client, NULL /* opts */, error);
   if (!session) {
      goto fail;
   }

   /* Step 2: Optional. Define options to use for the transaction. */
   txn_opts = mongoc_transaction_opts_new ();
   rp = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   mongoc_transaction_opts_set_read_prefs (txn_opts, rp);
   mongoc_transaction_opts_set_read_concern (txn_opts, rc);
   mongoc_transaction_opts_set_write_concern (txn_opts, wc);

   /* Step 3: Use mongoc_client_session_with_transaction to start a transaction,
    * execute the callback, and commit (or abort on error). */
   ret = mongoc_client_session_with_transaction (session, callback, txn_opts, NULL /* ctx */, NULL /* reply */, error);
   if (!ret) {
      goto fail;
   }

   success = true;
fail:
   bson_destroy (doc);
   mongoc_collection_destroy (coll);
   bson_destroy (insert_opts);
   mongoc_read_concern_destroy (rc);
   mongoc_read_prefs_destroy (rp);
   mongoc_write_concern_destroy (wc);
   mongoc_transaction_opts_destroy (txn_opts);
   mongoc_client_session_destroy (session);
   mongoc_client_destroy (client);
   return success;
}

/* Define the callback that specifies the sequence of operations to perform
 * inside the transactions. */
static bool
callback (mongoc_client_session_t *session, void *ctx, bson_t **reply, bson_error_t *error)
{
   mongoc_client_t *client = NULL;
   mongoc_collection_t *coll = NULL;
   bson_t *doc = NULL;
   bool success = false;
   bool ret = false;

   BSON_UNUSED (ctx);

   client = mongoc_client_session_get_client (session);
   coll = mongoc_client_get_collection (client, "mydb1", "foo");
   doc = BCON_NEW ("abc", BCON_INT32 (1));
   ret = mongoc_collection_insert_one (coll, doc, NULL /* opts */, *reply, error);
   if (!ret) {
      goto fail;
   }
   bson_destroy (doc);
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "mydb2", "bar");
   doc = BCON_NEW ("xyz", BCON_INT32 (999));
   ret = mongoc_collection_insert_one (coll, doc, NULL /* opts */, *reply, error);
   if (!ret) {
      goto fail;
   }

   success = true;
fail:
   mongoc_collection_destroy (coll);
   bson_destroy (doc);
   return success;
}
/* End Transactions withTxn API Example 1 */

static void
_test_sample_versioned_api_example_1 (void)
{
   /* Start Versioned API Example 1 */
   mongoc_client_t *client = NULL;
   mongoc_server_api_t *server_api = NULL;
   mongoc_server_api_version_t server_api_version;
   bson_error_t error;

   /* For a replica set, include the replica set name and a seedlist of the
    * members in the URI string; e.g.
    * uri_repl = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:" \
    *    "27017/?replicaSet=myRepl";
    * client = mongoc_client_new (uri_repl);
    * For a sharded cluster, connect to the mongos instances; e.g.
    * uri_sharded =
    * "mongodb://mongos0.example.com:27017,mongos1.example.com:27017/";
    * client = mongoc_client_new (uri_sharded);
    */

   /* Create a mongoc_client_t without server API options configured. */
   client = get_client_for_version_api_example ();

   mongoc_server_api_version_from_string ("1", &server_api_version);
   server_api = mongoc_server_api_new (server_api_version);

   assert (mongoc_client_set_server_api (client, server_api, &error));
   /* End Versioned API Example 1 */

   mongoc_client_destroy (client);
   mongoc_server_api_destroy (server_api);

   BSON_UNUSED (error);
}

static void
_test_sample_versioned_api_example_2 (void)
{
   /* Start Versioned API Example 2 */
   mongoc_client_t *client = NULL;
   mongoc_server_api_t *server_api = NULL;
   mongoc_server_api_version_t server_api_version;
   bson_error_t error;

   /* For a replica set, include the replica set name and a seedlist of the
    * members in the URI string; e.g.
    * uri_repl = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:" \
    *    "27017/?replicaSet=myRepl";
    * client = mongoc_client_new (uri_repl);
    * For a sharded cluster, connect to the mongos instances; e.g.
    * uri_sharded =
    * "mongodb://mongos0.example.com:27017,mongos1.example.com:27017/";
    * client = mongoc_client_new (uri_sharded);
    */

   /* Create a mongoc_client_t without server API options configured. */
   client = get_client_for_version_api_example ();

   mongoc_server_api_version_from_string ("1", &server_api_version);
   server_api = mongoc_server_api_new (server_api_version);
   mongoc_server_api_strict (server_api, true);

   assert (mongoc_client_set_server_api (client, server_api, &error));
   /* End Versioned API Example 2 */

   mongoc_client_destroy (client);
   mongoc_server_api_destroy (server_api);

   BSON_UNUSED (error);
}

static void
_test_sample_versioned_api_example_3 (void)
{
   /* Start Versioned API Example 3 */
   mongoc_client_t *client = NULL;
   mongoc_server_api_t *server_api = NULL;
   mongoc_server_api_version_t server_api_version;
   bson_error_t error;

   /* For a replica set, include the replica set name and a seedlist of the
    * members in the URI string; e.g.
    * uri_repl = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:" \
    *    "27017/?replicaSet=myRepl";
    * client = mongoc_client_new (uri_repl);
    * For a sharded cluster, connect to the mongos instances; e.g.
    * uri_sharded =
    * "mongodb://mongos0.example.com:27017,mongos1.example.com:27017/";
    * client = mongoc_client_new (uri_sharded);
    */

   /* Create a mongoc_client_t without server API options configured. */
   client = get_client_for_version_api_example ();

   mongoc_server_api_version_from_string ("1", &server_api_version);
   server_api = mongoc_server_api_new (server_api_version);
   mongoc_server_api_strict (server_api, false);

   assert (mongoc_client_set_server_api (client, server_api, &error));
   /* End Versioned API Example 3 */

   mongoc_client_destroy (client);
   mongoc_server_api_destroy (server_api);

   BSON_UNUSED (error);
}

static void
_test_sample_versioned_api_example_4 (void)
{
   /* Start Versioned API Example 4 */
   mongoc_client_t *client = NULL;
   mongoc_server_api_t *server_api = NULL;
   mongoc_server_api_version_t server_api_version;
   bson_error_t error;

   /* For a replica set, include the replica set name and a seedlist of the
    * members in the URI string; e.g.
    * uri_repl = "mongodb://mongodb0.example.com:27017,mongodb1.example.com:" \
    *    "27017/?replicaSet=myRepl";
    * client = mongoc_client_new (uri_repl);
    * For a sharded cluster, connect to the mongos instances; e.g.
    * uri_sharded =
    * "mongodb://mongos0.example.com:27017,mongos1.example.com:27017/";
    * client = mongoc_client_new (uri_sharded);
    */

   /* Create a mongoc_client_t without server API options configured. */
   client = get_client_for_version_api_example ();

   mongoc_server_api_version_from_string ("1", &server_api_version);
   server_api = mongoc_server_api_new (server_api_version);
   mongoc_server_api_deprecation_errors (server_api, true);

   assert (mongoc_client_set_server_api (client, server_api, &error));
   /* End Versioned API Example 4 */

   mongoc_client_destroy (client);
   mongoc_server_api_destroy (server_api);

   BSON_UNUSED (error);
}

static int64_t
iso_to_unix (const char *iso_str)
{
   BSON_UNUSED (iso_str);

   /* TODO (CDRIVER-2945) there is no convenient helper for converting ISO8601
    * strings to Unix timestamps. This is not shown in the example. */
   return 1628330345;
}

static void
_test_sample_versioned_api_example_5_6_7_8 (void)
{
#define N_DOCS 8
   mongoc_client_t *client;
   mongoc_server_api_t *server_api;
   mongoc_server_api_version_t server_api_version;
   bool ok;
   bson_error_t error;
   mongoc_database_t *db;
   mongoc_collection_t *sales;
   bson_t *docs[N_DOCS];
   int i;
   bson_t reply;
   int64_t count;
   bson_t *filter;

   /* Create a mongoc_client_t without server API options configured. */
   client = get_client_for_version_api_example ();
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   mongoc_server_api_version_from_string ("1", &server_api_version);
   server_api = mongoc_server_api_new (server_api_version);
   mongoc_server_api_strict (server_api, true);
   ok = mongoc_client_set_server_api (client, server_api, &error);
   ASSERT_OR_PRINT (ok, error);
   db = mongoc_client_get_database (client, "db");
   sales = mongoc_database_get_collection (db, "sales");
   /* Drop db.sales in case the collection exists. */
   ok = mongoc_collection_drop (sales, &error);
   if (!ok && NULL == strstr (error.message, "ns not found")) {
      /* Ignore an "ns not found" error on dropping the collection in case the
       * namespace does not exist. */
      ASSERT_OR_PRINT (ok, error);
   }

   /* Start Versioned API Example 5 */
   docs[0] = BCON_NEW ("_id",
                       BCON_INT32 (1),
                       "item",
                       "abc",
                       "price",
                       BCON_INT32 (10),
                       "quantity",
                       BCON_INT32 (2),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-01-01T08:00:00Z")));
   docs[1] = BCON_NEW ("_id",
                       BCON_INT32 (2),
                       "item",
                       "jkl",
                       "price",
                       BCON_INT32 (20),
                       "quantity",
                       BCON_INT32 (1),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-03T09:00:00Z")));
   docs[2] = BCON_NEW ("_id",
                       BCON_INT32 (3),
                       "item",
                       "xyz",
                       "price",
                       BCON_INT32 (5),
                       "quantity",
                       BCON_INT32 (5),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-03T09:05:00Z")));
   docs[3] = BCON_NEW ("_id",
                       BCON_INT32 (4),
                       "item",
                       "abc",
                       "price",
                       BCON_INT32 (10),
                       "quantity",
                       BCON_INT32 (10),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-15T08:00:00Z")));
   docs[4] = BCON_NEW ("_id",
                       BCON_INT32 (5),
                       "item",
                       "xyz",
                       "price",
                       BCON_INT32 (5),
                       "quantity",
                       BCON_INT32 (10),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-15T09:05:00Z")));
   docs[5] = BCON_NEW ("_id",
                       BCON_INT32 (6),
                       "item",
                       "xyz",
                       "price",
                       BCON_INT32 (5),
                       "quantity",
                       BCON_INT32 (5),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-15T12:05:10Z")));
   docs[6] = BCON_NEW ("_id",
                       BCON_INT32 (7),
                       "item",
                       "xyz",
                       "price",
                       BCON_INT32 (5),
                       "quantity",
                       BCON_INT32 (10),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-02-15T14:12:12Z")));
   docs[7] = BCON_NEW ("_id",
                       BCON_INT32 (8),
                       "item",
                       "abc",
                       "price",
                       BCON_INT32 (10),
                       "quantity",
                       BCON_INT32 (5),
                       "date",
                       BCON_DATE_TIME (iso_to_unix ("2021-03-16T20:20:13Z")));
   ok = mongoc_collection_insert_many (sales, (const bson_t **) docs, N_DOCS, NULL /* opts */, &reply, &error);
   /* End Versioned API Example 5 */
   ASSERT_OR_PRINT (ok, error);
   bson_destroy (&reply);

   {
      const server_version_t version = test_framework_get_server_version ();

      // count command was added to API version 1 in 6.0 and backported to 5.0.9
      // and 5.3.2 (see SERVER-63850 and DRIVERS-2228). This test assumes count
      // command is not in API version 1. Skip until examples are updated
      // accordingly (see DRIVERS-1846).
      const bool should_skip = (version >= 106100100) ||                        // [6.0.0, inf)
                               (version >= 105103102 && version < 106100100) || // [5.3.2, 6.0.0)
                               (version >= 105100109 && version < 105101100);   // [5.0.9, 5.1.0)

      if (!should_skip) {
         bson_t *cmd = BCON_NEW ("count", "sales");
         ok = mongoc_database_command_simple (db, cmd, NULL /* read_prefs */, &reply, &error);
         ASSERT_ERROR_CONTAINS (error,
                                MONGOC_ERROR_SERVER,
                                323,
                                "Provided apiStrict:true, but the command count "
                                "is not in API Version 1");
         ASSERT (!ok);
         bson_destroy (&reply);
         bson_destroy (cmd);
      }
   }
#if 0
   /* This block not evaluated, but is inserted into documentation to represent the above reply.
    * Don't delete me! */
   /* Start Versioned API Example 6 */
   char *str = bson_as_json (&reply, NULL /* length */);
   printf ("%s", str);
   /* Prints the server reply:
    * { "ok" : 0, "errmsg" : "Provided apiStrict:true, but the command count is not in API Version 1", "code" : 323, "codeName" : "APIStrictError" } */
   bson_free (str);
   /* End Versioned API Example 6 */
#endif

   /* Start Versioned API Example 7 */
   filter = bson_new ();
   count = mongoc_collection_count_documents (sales, filter, NULL /* opts */, NULL /* read_prefs */, &reply, &error);
   /* End Versioned API Example 7 */
   if (N_DOCS != count) {
      test_error ("expected %d documents, got %" PRId64, N_DOCS, count);
   }
   bson_destroy (&reply);

   /* Start Versioned API Example 8 */
   BSON_ASSERT (count == N_DOCS);
   /* End Versioned API Example 8 */

   bson_destroy (filter);
   for (i = 0; i < N_DOCS; i++) {
      bson_destroy (docs[i]);
   }
   mongoc_collection_destroy (sales);
   mongoc_database_destroy (db);
   mongoc_server_api_destroy (server_api);
   mongoc_client_destroy (client);
}

static void
test_sample_versioned_api (void)
{
   _test_sample_versioned_api_example_1 ();
   _test_sample_versioned_api_example_2 ();
   _test_sample_versioned_api_example_3 ();
   _test_sample_versioned_api_example_4 ();
   _test_sample_versioned_api_example_5_6_7_8 ();
}

static void
test_sample_commands (void)
{
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;

   client = test_framework_new_default_client ();
   db = mongoc_client_get_database (client, "test_sample_command");
   collection = mongoc_database_get_collection (db, "inventory");
   mongoc_collection_drop (collection, NULL);

   test_sample_command (test_example_1, 1, db, collection, false);
   test_sample_command (test_example_2, 2, db, collection, false);
   test_sample_command (test_example_3, 3, db, collection, true);
   test_sample_command (test_example_6, 6, db, collection, false);
   test_sample_command (test_example_7, 7, db, collection, false);
   test_sample_command (test_example_9, 9, db, collection, false);
   test_sample_command (test_example_10, 10, db, collection, false);
   test_sample_command (test_example_11, 11, db, collection, false);
   test_sample_command (test_example_12, 12, db, collection, false);
   test_sample_command (test_example_13, 13, db, collection, true);
   test_sample_command (test_example_14, 14, db, collection, false);
   test_sample_command (test_example_15, 15, db, collection, false);
   test_sample_command (test_example_16, 16, db, collection, false);
   test_sample_command (test_example_17, 17, db, collection, false);
   test_sample_command (test_example_18, 18, db, collection, false);
   test_sample_command (test_example_19, 19, db, collection, true);
   test_sample_command (test_example_20, 20, db, collection, false);
   test_sample_command (test_example_21, 21, db, collection, false);
   test_sample_command (test_example_22, 22, db, collection, false);
   test_sample_command (test_example_23, 23, db, collection, false);
   test_sample_command (test_example_24, 24, db, collection, false);
   test_sample_command (test_example_25, 25, db, collection, false);
   test_sample_command (test_example_26, 26, db, collection, false);
   test_sample_command (test_example_27, 27, db, collection, false);
   test_sample_command (test_example_28, 28, db, collection, true);
   test_sample_command (test_example_29, 29, db, collection, false);
   test_sample_command (test_example_30, 30, db, collection, false);
   test_sample_command (test_example_31, 31, db, collection, false);
   test_sample_command (test_example_32, 32, db, collection, false);
   test_sample_command (test_example_33, 33, db, collection, false);
   test_sample_command (test_example_34, 34, db, collection, false);
   test_sample_command (test_example_35, 35, db, collection, false);
   test_sample_command (test_example_36, 36, db, collection, false);
   test_sample_command (test_example_37, 37, db, collection, true);
   test_sample_command (test_example_38, 38, db, collection, false);
   test_sample_command (test_example_39, 39, db, collection, false);
   test_sample_command (test_example_40, 40, db, collection, false);
   test_sample_command (test_example_41, 41, db, collection, true);
   test_sample_command (test_example_42, 42, db, collection, false);
   test_sample_command (test_example_43, 43, db, collection, false);
   test_sample_command (test_example_44, 44, db, collection, false);
   test_sample_command (test_example_45, 45, db, collection, false);
   test_sample_command (test_example_46, 46, db, collection, false);
   test_sample_command (test_example_47, 47, db, collection, false);
   test_sample_command (test_example_48, 48, db, collection, false);
   test_sample_command (test_example_49, 49, db, collection, false);
   test_sample_command (test_example_50, 50, db, collection, true);
   test_sample_command (test_example_51, 51, db, collection, false);
   test_sample_command (test_example_52, 52, db, collection, false);
   test_sample_command (test_example_53, 53, db, collection, false);
   test_sample_command (test_example_54, 54, db, collection, true);
   test_sample_command (test_example_55, 55, db, collection, false);
   test_sample_command (test_example_57, 57, db, collection, false);
   test_sample_command (test_example_58, 58, db, collection, false);
   test_sample_command (test_example_56, 56, db, collection, true);
   test_sample_command (test_example_59, 59, db, collection, true);
   test_sample_command (test_example_60, 60, db, collection, true);
   test_sample_change_stream_command (test_example_change_stream, db);
   test_sample_causal_consistency (client);
   test_sample_aggregation (db);
   test_sample_indexes (db);
   test_sample_run_command (db);
   test_sample_txn_commands (client);

   if (test_framework_max_wire_version_at_least (WIRE_VERSION_4_9)) {
      test_sample_versioned_api ();
   }

   mongoc_collection_drop (collection, NULL);

   mongoc_collection_destroy (collection);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

static void
test_with_txn_example (void *unused)
{
   bson_error_t error;

   BSON_UNUSED (unused);

   ASSERT_OR_PRINT (with_transaction_example (&error), error);
}


void
test_samples_install (TestSuite *suite)
{
   TestSuite_AddLive (suite, "/Samples", test_sample_commands);
   TestSuite_AddFull (suite, "/Samples/with_txn", test_with_txn_example, NULL, NULL, test_framework_skip_if_no_txns);
}
