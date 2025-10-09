/*
 * Copyright 2022 MongoDB, Inc.
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

#include "TestSuite.h"

#include <bson/bson.h>

static void
test_bson_cmp_equal (void)
{
   BSON_ASSERT (bson_cmp_equal_ss (0, 0));
   BSON_ASSERT (!bson_cmp_equal_ss (0, -1));
   BSON_ASSERT (!bson_cmp_equal_ss (0, 1));
   BSON_ASSERT (!bson_cmp_equal_ss (-1, 0));
   BSON_ASSERT (bson_cmp_equal_ss (-1, -1));
   BSON_ASSERT (!bson_cmp_equal_ss (-1, 1));
   BSON_ASSERT (!bson_cmp_equal_ss (1, 0));
   BSON_ASSERT (!bson_cmp_equal_ss (1, -1));
   BSON_ASSERT (bson_cmp_equal_ss (1, 1));

   BSON_ASSERT (bson_cmp_equal_uu (0u, 0u));
   BSON_ASSERT (!bson_cmp_equal_uu (0u, 1u));
   BSON_ASSERT (!bson_cmp_equal_uu (1u, 0u));
   BSON_ASSERT (bson_cmp_equal_uu (1u, 1u));

   BSON_ASSERT (bson_cmp_equal_su (0, 0u));
   BSON_ASSERT (!bson_cmp_equal_su (0, 1u));
   BSON_ASSERT (!bson_cmp_equal_su (-1, 0u));
   BSON_ASSERT (!bson_cmp_equal_su (-1, 1u));
   BSON_ASSERT (!bson_cmp_equal_su (1, 0u));
   BSON_ASSERT (bson_cmp_equal_su (1, 1u));

   BSON_ASSERT (bson_cmp_equal_us (0u, 0));
   BSON_ASSERT (!bson_cmp_equal_us (0u, -1));
   BSON_ASSERT (!bson_cmp_equal_us (0u, 1));
   BSON_ASSERT (!bson_cmp_equal_us (1u, 0));
   BSON_ASSERT (!bson_cmp_equal_us (1u, -1));
   BSON_ASSERT (bson_cmp_equal_us (1u, 1));
}

static void
test_bson_cmp_not_equal (void)
{
   BSON_ASSERT (!bson_cmp_not_equal_ss (0, 0));
   BSON_ASSERT (bson_cmp_not_equal_ss (0, -1));
   BSON_ASSERT (bson_cmp_not_equal_ss (0, 1));
   BSON_ASSERT (bson_cmp_not_equal_ss (-1, 0));
   BSON_ASSERT (!bson_cmp_not_equal_ss (-1, -1));
   BSON_ASSERT (bson_cmp_not_equal_ss (-1, 1));
   BSON_ASSERT (bson_cmp_not_equal_ss (1, 0));
   BSON_ASSERT (bson_cmp_not_equal_ss (1, -1));
   BSON_ASSERT (!bson_cmp_not_equal_ss (1, 1));

   BSON_ASSERT (!bson_cmp_not_equal_uu (0u, 0u));
   BSON_ASSERT (bson_cmp_not_equal_uu (0u, 1u));
   BSON_ASSERT (bson_cmp_not_equal_uu (1u, 0u));
   BSON_ASSERT (!bson_cmp_not_equal_uu (1u, 1u));

   BSON_ASSERT (!bson_cmp_not_equal_su (0, 0u));
   BSON_ASSERT (bson_cmp_not_equal_su (0, 1u));
   BSON_ASSERT (bson_cmp_not_equal_su (-1, 0u));
   BSON_ASSERT (bson_cmp_not_equal_su (-1, 1u));
   BSON_ASSERT (bson_cmp_not_equal_su (1, 0u));
   BSON_ASSERT (!bson_cmp_not_equal_su (1, 1u));

   BSON_ASSERT (!bson_cmp_not_equal_us (0u, 0));
   BSON_ASSERT (bson_cmp_not_equal_us (0u, -1));
   BSON_ASSERT (bson_cmp_not_equal_us (0u, 1));
   BSON_ASSERT (bson_cmp_not_equal_us (1u, 0));
   BSON_ASSERT (bson_cmp_not_equal_us (1u, -1));
   BSON_ASSERT (!bson_cmp_not_equal_us (1u, 1));
}

static void
test_bson_cmp_less (void)
{
   BSON_ASSERT (!bson_cmp_less_ss (0, 0));
   BSON_ASSERT (!bson_cmp_less_ss (0, -1));
   BSON_ASSERT (bson_cmp_less_ss (0, 1));
   BSON_ASSERT (bson_cmp_less_ss (-1, 0));
   BSON_ASSERT (!bson_cmp_less_ss (-1, -1));
   BSON_ASSERT (bson_cmp_less_ss (-1, 1));
   BSON_ASSERT (!bson_cmp_less_ss (1, 0));
   BSON_ASSERT (!bson_cmp_less_ss (1, -1));
   BSON_ASSERT (!bson_cmp_less_ss (1, 1));

   BSON_ASSERT (!bson_cmp_less_uu (0u, 0u));
   BSON_ASSERT (bson_cmp_less_uu (0u, 1u));
   BSON_ASSERT (!bson_cmp_less_uu (1u, 0u));
   BSON_ASSERT (!bson_cmp_less_uu (1u, 1u));

   BSON_ASSERT (!bson_cmp_less_su (0, 0u));
   BSON_ASSERT (bson_cmp_less_su (0, 1u));
   BSON_ASSERT (bson_cmp_less_su (-1, 0u));
   BSON_ASSERT (bson_cmp_less_su (-1, 1u));
   BSON_ASSERT (!bson_cmp_less_su (1, 0u));
   BSON_ASSERT (!bson_cmp_less_su (1, 1u));

   BSON_ASSERT (!bson_cmp_less_us (0u, 0));
   BSON_ASSERT (!bson_cmp_less_us (0u, -1));
   BSON_ASSERT (bson_cmp_less_us (0u, 1));
   BSON_ASSERT (!bson_cmp_less_us (1u, 0));
   BSON_ASSERT (!bson_cmp_less_us (1u, -1));
   BSON_ASSERT (!bson_cmp_less_us (1u, 1));
}

static void
test_bson_cmp_greater (void)
{
   BSON_ASSERT (!bson_cmp_greater_ss (0, 0));
   BSON_ASSERT (bson_cmp_greater_ss (0, -1));
   BSON_ASSERT (!bson_cmp_greater_ss (0, 1));
   BSON_ASSERT (!bson_cmp_greater_ss (-1, 0));
   BSON_ASSERT (!bson_cmp_greater_ss (-1, -1));
   BSON_ASSERT (!bson_cmp_greater_ss (-1, 1));
   BSON_ASSERT (bson_cmp_greater_ss (1, 0));
   BSON_ASSERT (bson_cmp_greater_ss (1, -1));
   BSON_ASSERT (!bson_cmp_greater_ss (1, 1));

   BSON_ASSERT (!bson_cmp_greater_uu (0u, 0u));
   BSON_ASSERT (!bson_cmp_greater_uu (0u, 1u));
   BSON_ASSERT (bson_cmp_greater_uu (1u, 0u));
   BSON_ASSERT (!bson_cmp_greater_uu (1u, 1u));

   BSON_ASSERT (!bson_cmp_greater_su (0, 0u));
   BSON_ASSERT (!bson_cmp_greater_su (0, 1u));
   BSON_ASSERT (!bson_cmp_greater_su (-1, 0u));
   BSON_ASSERT (!bson_cmp_greater_su (-1, 1u));
   BSON_ASSERT (bson_cmp_greater_su (1, 0u));
   BSON_ASSERT (!bson_cmp_greater_su (1, 1u));

   BSON_ASSERT (!bson_cmp_greater_us (0u, 0));
   BSON_ASSERT (bson_cmp_greater_us (0u, -1));
   BSON_ASSERT (!bson_cmp_greater_us (0u, 1));
   BSON_ASSERT (bson_cmp_greater_us (1u, 0));
   BSON_ASSERT (bson_cmp_greater_us (1u, -1));
   BSON_ASSERT (!bson_cmp_greater_us (1u, 1));
}

static void
test_bson_cmp_less_equal (void)
{
   BSON_ASSERT (bson_cmp_less_equal_ss (0, 0));
   BSON_ASSERT (!bson_cmp_less_equal_ss (0, -1));
   BSON_ASSERT (bson_cmp_less_equal_ss (0, 1));
   BSON_ASSERT (bson_cmp_less_equal_ss (-1, 0));
   BSON_ASSERT (bson_cmp_less_equal_ss (-1, -1));
   BSON_ASSERT (bson_cmp_less_equal_ss (-1, 1));
   BSON_ASSERT (!bson_cmp_less_equal_ss (1, 0));
   BSON_ASSERT (!bson_cmp_less_equal_ss (1, -1));
   BSON_ASSERT (bson_cmp_less_equal_ss (1, 1));

   BSON_ASSERT (bson_cmp_less_equal_uu (0u, 0u));
   BSON_ASSERT (bson_cmp_less_equal_uu (0u, 1u));
   BSON_ASSERT (!bson_cmp_less_equal_uu (1u, 0u));
   BSON_ASSERT (bson_cmp_less_equal_uu (1u, 1u));

   BSON_ASSERT (bson_cmp_less_equal_su (0, 0u));
   BSON_ASSERT (bson_cmp_less_equal_su (0, 1u));
   BSON_ASSERT (bson_cmp_less_equal_su (-1, 0u));
   BSON_ASSERT (bson_cmp_less_equal_su (-1, 1u));
   BSON_ASSERT (!bson_cmp_less_equal_su (1, 0u));
   BSON_ASSERT (bson_cmp_less_equal_su (1, 1u));

   BSON_ASSERT (bson_cmp_less_equal_us (0u, 0));
   BSON_ASSERT (!bson_cmp_less_equal_us (0u, -1));
   BSON_ASSERT (bson_cmp_less_equal_us (0u, 1));
   BSON_ASSERT (!bson_cmp_less_equal_us (1u, 0));
   BSON_ASSERT (!bson_cmp_less_equal_us (1u, -1));
   BSON_ASSERT (bson_cmp_less_equal_us (1u, 1));
}

static void
test_bson_cmp_greater_equal (void)
{
   BSON_ASSERT (bson_cmp_greater_equal_ss (0, 0));
   BSON_ASSERT (bson_cmp_greater_equal_ss (0, -1));
   BSON_ASSERT (!bson_cmp_greater_equal_ss (0, 1));
   BSON_ASSERT (!bson_cmp_greater_equal_ss (-1, 0));
   BSON_ASSERT (bson_cmp_greater_equal_ss (-1, -1));
   BSON_ASSERT (!bson_cmp_greater_equal_ss (-1, 1));
   BSON_ASSERT (bson_cmp_greater_equal_ss (1, 0));
   BSON_ASSERT (bson_cmp_greater_equal_ss (1, -1));
   BSON_ASSERT (bson_cmp_greater_equal_ss (1, 1));

   BSON_ASSERT (bson_cmp_greater_equal_uu (0u, 0u));
   BSON_ASSERT (!bson_cmp_greater_equal_uu (0u, 1u));
   BSON_ASSERT (bson_cmp_greater_equal_uu (1u, 0u));
   BSON_ASSERT (bson_cmp_greater_equal_uu (1u, 1u));

   BSON_ASSERT (bson_cmp_greater_equal_su (0, 0u));
   BSON_ASSERT (!bson_cmp_greater_equal_su (0, 1u));
   BSON_ASSERT (!bson_cmp_greater_equal_su (-1, 0u));
   BSON_ASSERT (!bson_cmp_greater_equal_su (-1, 1u));
   BSON_ASSERT (bson_cmp_greater_equal_su (1, 0u));
   BSON_ASSERT (bson_cmp_greater_equal_su (1, 1u));

   BSON_ASSERT (bson_cmp_greater_equal_us (0u, 0));
   BSON_ASSERT (bson_cmp_greater_equal_us (0u, -1));
   BSON_ASSERT (!bson_cmp_greater_equal_us (0u, 1));
   BSON_ASSERT (bson_cmp_greater_equal_us (1u, 0));
   BSON_ASSERT (bson_cmp_greater_equal_us (1u, -1));
   BSON_ASSERT (bson_cmp_greater_equal_us (1u, 1));
}

/* Sanity check: ensure ssize_t limits are as expected relative to size_t. */
BSON_STATIC_ASSERT2 (ssize_t_size_min_check, SSIZE_MIN + 1 == -SSIZE_MAX);
BSON_STATIC_ASSERT2 (ssize_t_size_max_check, (size_t) SSIZE_MAX <= SIZE_MAX);

static void
test_bson_in_range (void)
{
   const int64_t int8_min = INT8_MIN;
   const int64_t int8_max = INT8_MAX;
   const int64_t int32_min = INT32_MIN;
   const int64_t int32_max = INT32_MAX;

   const uint64_t uint8_max = UINT8_MAX;
   const uint64_t uint32_max = UINT32_MAX;

   const ssize_t ssize_min = SSIZE_MIN;
   const ssize_t ssize_max = SSIZE_MAX;

   BSON_ASSERT (!bson_in_range_signed (int8_t, int8_min - 1));
   BSON_ASSERT (bson_in_range_signed (int8_t, int8_min));
   BSON_ASSERT (bson_in_range_signed (int8_t, 0));
   BSON_ASSERT (bson_in_range_signed (int8_t, int8_max));
   BSON_ASSERT (!bson_in_range_signed (int8_t, int8_max + 1));

   BSON_ASSERT (bson_in_range_unsigned (int8_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (int8_t, (uint64_t) int8_max));
   BSON_ASSERT (!bson_in_range_unsigned (int8_t, (uint64_t) (int8_max + 1)));

   BSON_ASSERT (!bson_in_range_signed (uint8_t, int8_min - 1));
   BSON_ASSERT (!bson_in_range_signed (uint8_t, int8_min));
   BSON_ASSERT (bson_in_range_signed (uint8_t, 0));
   BSON_ASSERT (bson_in_range_signed (uint8_t, int8_max));
   BSON_ASSERT (bson_in_range_signed (uint8_t, int8_max + 1));
   BSON_ASSERT (bson_in_range_signed (uint8_t, (int64_t) uint8_max));
   BSON_ASSERT (!bson_in_range_signed (uint8_t, (int64_t) uint8_max + 1));

   BSON_ASSERT (bson_in_range_unsigned (uint8_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (uint8_t, uint8_max));
   BSON_ASSERT (!bson_in_range_unsigned (uint8_t, uint8_max + 1u));

   BSON_ASSERT (!bson_in_range_signed (int32_t, int32_min - 1));
   BSON_ASSERT (bson_in_range_signed (int32_t, int32_min));
   BSON_ASSERT (bson_in_range_signed (int32_t, 0));
   BSON_ASSERT (bson_in_range_signed (int32_t, int32_max));
   BSON_ASSERT (!bson_in_range_signed (int32_t, int32_max + 1));

   BSON_ASSERT (bson_in_range_unsigned (int32_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (int32_t, (uint64_t) int32_max));
   BSON_ASSERT (!bson_in_range_unsigned (int32_t, (uint64_t) (int32_max + 1)));

   BSON_ASSERT (!bson_in_range_signed (uint32_t, int32_min - 1));
   BSON_ASSERT (!bson_in_range_signed (uint32_t, int32_min));
   BSON_ASSERT (bson_in_range_signed (uint32_t, 0));
   BSON_ASSERT (bson_in_range_signed (uint32_t, int32_max));
   BSON_ASSERT (bson_in_range_signed (uint32_t, int32_max + 1));
   BSON_ASSERT (bson_in_range_signed (uint32_t, (int64_t) uint32_max));
   BSON_ASSERT (!bson_in_range_signed (uint32_t, (int64_t) uint32_max + 1));

   BSON_ASSERT (bson_in_range_unsigned (uint32_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (uint32_t, uint32_max));
   BSON_ASSERT (!bson_in_range_unsigned (uint32_t, uint32_max + 1u));

   BSON_ASSERT (bson_in_range_signed (ssize_t, ssize_min));
   BSON_ASSERT (bson_in_range_signed (ssize_t, 0));
   BSON_ASSERT (bson_in_range_signed (ssize_t, ssize_max));

   BSON_ASSERT (bson_in_range_unsigned (ssize_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (ssize_t, (size_t) ssize_max));
   BSON_ASSERT (!bson_in_range_unsigned (ssize_t, (size_t) ssize_max + 1u));

   BSON_ASSERT (!bson_in_range_signed (size_t, ssize_min));
   BSON_ASSERT (bson_in_range_signed (size_t, 0));
   BSON_ASSERT (bson_in_range_signed (size_t, ssize_max));

   BSON_ASSERT (bson_in_range_unsigned (size_t, 0u));
   BSON_ASSERT (bson_in_range_unsigned (size_t, (size_t) ssize_max));
   BSON_ASSERT (bson_in_range_unsigned (size_t, (size_t) ssize_max + 1u));
}

void
test_bson_cmp_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/bson/cmp/equal", test_bson_cmp_equal);
   TestSuite_Add (suite, "/bson/cmp/not_equal", test_bson_cmp_not_equal);
   TestSuite_Add (suite, "/bson/cmp/less", test_bson_cmp_less);
   TestSuite_Add (suite, "/bson/cmp/greater", test_bson_cmp_greater);
   TestSuite_Add (suite, "/bson/cmp/less_equal", test_bson_cmp_less_equal);
   TestSuite_Add (suite, "/bson/cmp/greater_equal", test_bson_cmp_greater_equal);
   TestSuite_Add (suite, "/bson/cmp/in_range", test_bson_in_range);
}
