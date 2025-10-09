#include <mongoc/mcd-integer.h>

#include "TestSuite.h"

static void
_test_overflow (void)
{
   BSON_ASSERT (_mcd_i64_add_would_overflow (INT64_MIN, INT64_MIN));
   BSON_ASSERT (_mcd_i64_add_would_overflow (INT64_MIN, -1));
   BSON_ASSERT (_mcd_i64_add_would_overflow (INT64_MIN, -3));
   BSON_ASSERT (!_mcd_i64_add_would_overflow (INT64_MIN, 0));
   BSON_ASSERT (!_mcd_i64_add_would_overflow (INT64_MIN, INT64_MAX));
   BSON_ASSERT (_mcd_i64_sub_would_overflow (INT64_MIN, 4));
   BSON_ASSERT (!_mcd_i64_sub_would_overflow (INT64_MAX, 4));
   BSON_ASSERT (_mcd_i64_sub_would_overflow (INT64_MAX, -4));
   BSON_ASSERT (_mcd_i64_sub_would_overflow (INT64_MIN, 1));
   BSON_ASSERT (!_mcd_i64_sub_would_overflow (INT64_MIN, 0));
   BSON_ASSERT (!_mcd_i64_sub_would_overflow (INT64_MIN, -4));
   BSON_ASSERT (!_mcd_i64_mul_would_overflow (INT64_MIN, 1));
   BSON_ASSERT (_mcd_i64_mul_would_overflow (INT64_MIN, -1));
   BSON_ASSERT (_mcd_i64_mul_would_overflow (-1, INT64_MIN));
   BSON_ASSERT (_mcd_i64_mul_would_overflow (-2, INT64_MIN));
   BSON_ASSERT (!_mcd_i64_mul_would_overflow (-7, INT64_MIN / 8));
}

void
test_mcd_integer_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/integer/overflow", _test_overflow);
}
