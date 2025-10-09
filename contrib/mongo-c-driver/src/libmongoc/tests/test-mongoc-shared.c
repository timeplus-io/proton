#include "TestSuite.h"

#include <mongoc/mongoc-shared-private.h>

typedef struct {
   int value;
   int *store_value_on_dtor;
} my_value;

my_value *
my_value_new (void)
{
   my_value *p = bson_malloc0 (sizeof (my_value));
   p->value = 42;
   p->store_value_on_dtor = NULL;
   return p;
}

void
my_value_free (my_value *ptr)
{
   if (ptr->store_value_on_dtor) {
      *ptr->store_value_on_dtor = ptr->value;
   }
   ptr->value = 0;
   ptr->store_value_on_dtor = NULL;
   bson_free (ptr);
}

void
my_value_free_v (void *ptr)
{
   my_value_free ((my_value *) (ptr));
}

static void
test_simple (void)
{
   int destroyed_value = 0;
   mongoc_shared_ptr ptr = MONGOC_SHARED_PTR_NULL;
   mongoc_shared_ptr ptr2, valptr_s;
   my_value *valptr;

   ASSERT (mongoc_shared_ptr_is_null (ptr));
   ptr = mongoc_shared_ptr_create (my_value_new (), my_value_free_v);
   ASSERT (!mongoc_shared_ptr_is_null (ptr));

   ASSERT_CMPINT (mongoc_shared_ptr_use_count (ptr), ==, 1);

   ptr2 = mongoc_shared_ptr_copy (ptr);

   ASSERT (ptr.ptr == ptr2.ptr);
   ASSERT (ptr._aux == ptr2._aux);

   valptr_s = mongoc_shared_ptr_copy (ptr);
   valptr = valptr_s.ptr;
   valptr->store_value_on_dtor = &destroyed_value;
   valptr->value = 133;
   mongoc_shared_ptr_reset_null (&valptr_s);
   /* Value hasn't changed yet */
   ASSERT_CMPINT (destroyed_value, ==, 0);

   /* Now drop the original reference */
   mongoc_shared_ptr_reset_null (&ptr);
   /* Check that the pointer is empty */
   ASSERT (mongoc_shared_ptr_is_null (ptr));

   /* Still not yet destroyed */
   ASSERT_CMPINT (destroyed_value, ==, 0);

   /* Check that the existing pointer is okay */
   ASSERT_CMPINT (((my_value *) ptr2.ptr)->value, ==, 133);

   /* Drop the last one */
   mongoc_shared_ptr_reset_null (&ptr2);
   ASSERT (mongoc_shared_ptr_is_null (ptr2));

   /* Now it was destroyed and set */
   ASSERT_CMPINT (destroyed_value, ==, 133);
}

struct widget {
   int value;
   int *store_value_here;
};

void
widget_delete (void *w_)
{
   struct widget *w = w_;
   *w->store_value_here = w->value;
   bson_free (w);
}

static void
test_aliased (void)
{
   int destroyed_valued = 0;
   int *i;
   struct widget *w;
   mongoc_shared_ptr ptr = MONGOC_SHARED_PTR_NULL;

   ptr = mongoc_shared_ptr_create (bson_malloc0 (sizeof (struct widget)), widget_delete);
   w = ptr.ptr;
   w->store_value_here = &destroyed_valued;

   /* Alias 'ptr' to the `w->value` managed sub-object */
   ptr.ptr = &w->value;
   i = ptr.ptr;
   /* We can store through it okay. */
   *i = 42;
   ASSERT_CMPINT (w->value, ==, 42);

   /* Deleting with the aliased ptr is okay */
   ASSERT_CMPINT (destroyed_valued, ==, 0);
   mongoc_shared_ptr_reset_null (&ptr);
   ASSERT_CMPINT (destroyed_valued, ==, 42);
}

void
test_shared_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/shared/simple", test_simple);
   TestSuite_Add (suite, "/shared/aliased", test_aliased);
}
