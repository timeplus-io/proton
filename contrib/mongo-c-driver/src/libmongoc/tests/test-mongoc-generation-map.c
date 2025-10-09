/*
 * Copyright 2021-present MongoDB, Inc.
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

#include "mongoc-generation-map-private.h"

#include "TestSuite.h"

static void
test_generation_map_basic (void)
{
   bson_oid_t oid_a;
   bson_oid_t oid_b;
   mongoc_generation_map_t *gm;
   mongoc_generation_map_t *gm_copy;

   bson_oid_init_from_string (&oid_a, "AAAAAAAAAAAAAAAAAAAAAAAA");
   bson_oid_init_from_string (&oid_b, "BBBBBBBBBBBBBBBBBBBBBBBB");
   gm = mongoc_generation_map_new ();

   /* The generation map returns 0 for a key not found. */
   ASSERT_CMPUINT32 (0, ==, mongoc_generation_map_get (gm, &oid_a));

   /* The generation map increments to 1 for a key not found. */
   mongoc_generation_map_increment (gm, &oid_b);
   ASSERT_CMPUINT32 (1, ==, mongoc_generation_map_get (gm, &oid_b));

   /* Test incrementing again. */
   mongoc_generation_map_increment (gm, &oid_b);
   ASSERT_CMPUINT32 (2, ==, mongoc_generation_map_get (gm, &oid_b));

   /* Copying a generation map retains values. */
   gm_copy = mongoc_generation_map_copy (gm);
   ASSERT_CMPUINT32 (0, ==, mongoc_generation_map_get (gm_copy, &oid_a));
   ASSERT_CMPUINT32 (2, ==, mongoc_generation_map_get (gm_copy, &oid_b));

   mongoc_generation_map_destroy (gm_copy);
   mongoc_generation_map_destroy (gm);
}

void
test_generation_map_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/generation_map/basic", test_generation_map_basic);
}
