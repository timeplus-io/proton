#include "mongoc/mongoc-thread-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"


static void
test_cond_wait (void)
{
   int64_t start, duration_usec;
   bson_mutex_t mutex;
   mongoc_cond_t cond;

   bson_mutex_init (&mutex);
   mongoc_cond_init (&cond);

   bson_mutex_lock (&mutex);
   start = bson_get_monotonic_time ();
   mongoc_cond_timedwait (&cond, &mutex, 100);
   duration_usec = bson_get_monotonic_time () - start;
   bson_mutex_unlock (&mutex);

   if (!((50 * 1000 < duration_usec) && (150 * 1000 > duration_usec))) {
      test_error ("expected to wait 100ms, waited %" PRId64 "\n", duration_usec / 1000);
   }

   mongoc_cond_destroy (&cond);
   bson_mutex_destroy (&mutex);
}


void
test_thread_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Thread/cond_wait", test_cond_wait);
}
