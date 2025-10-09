#include "mongoc/mongoc-ts-pool-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"

static void
test_ts_pool_empty (void)
{
   mongoc_ts_pool *pool = mongoc_ts_pool_new ((mongoc_ts_pool_params){.element_size = sizeof (int)});
   BSON_ASSERT (mongoc_ts_pool_is_empty (pool));
   mongoc_ts_pool_free (pool);
}


static void
test_ts_pool_simple (void)
{
   mongoc_ts_pool *pool = mongoc_ts_pool_new ((mongoc_ts_pool_params){.element_size = sizeof (int)});
   int *item;
   int *item2;

   item = mongoc_ts_pool_get_existing (pool);
   BSON_ASSERT (!item);

   item = mongoc_ts_pool_get (pool, NULL);
   BSON_ASSERT (item);
   ASSERT_CMPINT (*item, ==, 0);
   *item = 42;
   ASSERT_CMPSIZE_T (mongoc_ts_pool_size (pool), ==, 0);
   mongoc_ts_pool_return (pool, item);
   ASSERT_CMPSIZE_T (mongoc_ts_pool_size (pool), ==, 1);

   item2 = mongoc_ts_pool_get_existing (pool);
   BSON_ASSERT (item2);
   ASSERT_CMPINT (*item2, ==, 42);
   ASSERT_CMPSIZE_T (mongoc_ts_pool_size (pool), ==, 0);

   mongoc_ts_pool_drop (pool, item2);
   ASSERT_CMPSIZE_T (mongoc_ts_pool_size (pool), ==, 0);

   mongoc_ts_pool_free (pool);
}

static int
_is_int_42 (int *v, void *unused)
{
   (void) unused;
   return *v == 42;
}

static void
_set_int_to_7 (int *v, void *unused, bson_error_t *unused2)
{
   (void) unused;
   (void) unused2;
   *v = 7;
}

/* Declare a pool that contains `int`, sets each new int to seven, and drops
 * integers that are equal to 42. */
MONGOC_DECL_SPECIAL_TS_POOL (int, int_pool, void, _set_int_to_7, NULL, _is_int_42)

static void
test_ts_pool_special (void)
{
   int_pool p = int_pool_new (NULL);
   int *item = int_pool_get (p, NULL);

   BSON_ASSERT (item);
   /* Integer items are constructed and set to seven */
   ASSERT_CMPINT (*item, ==, 7);

   ASSERT_CMPSIZE_T (int_pool_size (p), ==, 0);
   int_pool_return (p, item);
   ASSERT_CMPSIZE_T (int_pool_size (p), ==, 1);

   item = int_pool_get_existing (p);
   BSON_ASSERT (item);
   *item = 42;
   int_pool_return (p, item);
   /* The pool will drop integer items that are equal to 42, so the item was not
    * returned to the pool: */
   ASSERT_CMPSIZE_T (int_pool_size (p), ==, 0);

   int_pool_free (p);
}

void
test_ts_pool_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Util/ts-pool-empty", test_ts_pool_empty);
   TestSuite_Add (suite, "/Util/ts-pool", test_ts_pool_simple);
   TestSuite_Add (suite, "/Util/ts-pool-special", test_ts_pool_special);
}
