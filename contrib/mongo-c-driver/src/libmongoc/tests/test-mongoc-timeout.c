/*
 * Copyright 2020 MongoDB, Inc.
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
#include "test-libmongoc.h"

#include <mongoc-timeout-private.h>

void
_test_mongoc_timeout_new_success (int64_t expected)
{
   mongoc_timeout_t *timeout;

   timeout = mongoc_timeout_new_timeout_int64 (expected);
   BSON_ASSERT (mongoc_timeout_is_set (timeout));
   BSON_ASSERT (expected == mongoc_timeout_get_timeout_ms (timeout));
   mongoc_timeout_destroy (timeout);
}

void
_test_mongoc_timeout_new_failure (int64_t try, const char *err_msg)
{
   capture_logs (true);
   BSON_ASSERT (!mongoc_timeout_new_timeout_int64 (try));
   ASSERT_CAPTURED_LOG ("mongoc", MONGOC_LOG_LEVEL_ERROR, err_msg);
   clear_captured_logs ();
}

void
test_mongoc_timeout_new (void)
{
   mongoc_timeout_t *timeout = NULL;

   BSON_ASSERT (!mongoc_timeout_is_set (timeout));

   BSON_ASSERT (timeout = mongoc_timeout_new ());
   BSON_ASSERT (!mongoc_timeout_is_set (timeout));
   mongoc_timeout_destroy (timeout);

   _test_mongoc_timeout_new_failure (-1, "timeout must not be negative");
   _test_mongoc_timeout_new_failure (INT64_MIN, "timeout must not be negative");

   _test_mongoc_timeout_new_success (0);
   _test_mongoc_timeout_new_success (1);
   _test_mongoc_timeout_new_success (INT64_MAX);
}

void
_test_mongoc_timeout_set_failure (mongoc_timeout_t *timeout, int64_t try, const char *err_msg)
{
   capture_logs (true);
   BSON_ASSERT (!mongoc_timeout_set_timeout_ms (timeout, try));
   ASSERT_CAPTURED_LOG ("mongoc", MONGOC_LOG_LEVEL_ERROR, err_msg);
   clear_captured_logs ();

   BSON_ASSERT (!mongoc_timeout_is_set (timeout));
}

void
_test_mongoc_timeout_set_success (mongoc_timeout_t *timeout, int64_t expected)
{
   BSON_ASSERT (mongoc_timeout_set_timeout_ms (timeout, expected));
   BSON_ASSERT (mongoc_timeout_is_set (timeout));
   BSON_ASSERT (expected == mongoc_timeout_get_timeout_ms (timeout));
}

void
test_mongoc_timeout_set (void)
{
   mongoc_timeout_t *timeout = NULL;

   timeout = mongoc_timeout_new ();
   BSON_ASSERT (!mongoc_timeout_is_set (timeout));

   _test_mongoc_timeout_set_failure (timeout, -1, "timeout must not be negative");
   _test_mongoc_timeout_set_failure (timeout, INT64_MIN, "timeout must not be negative");

   _test_mongoc_timeout_set_success (timeout, 0);
   _test_mongoc_timeout_set_success (timeout, 1);
   _test_mongoc_timeout_set_success (timeout, INT64_MAX);

   mongoc_timeout_destroy (timeout);
}

void
test_mongoc_timeout_get (void)
{
   mongoc_timeout_t *timeout = NULL;
   int64_t expected;

   BSON_ASSERT (timeout = mongoc_timeout_new ());
   BSON_ASSERT (!mongoc_timeout_is_set (timeout));

   expected = 1;
   mongoc_timeout_set_timeout_ms (timeout, expected);
   BSON_ASSERT (mongoc_timeout_is_set (timeout));
   BSON_ASSERT (expected == mongoc_timeout_get_timeout_ms (timeout));

   mongoc_timeout_destroy (timeout);
}

void
_test_mongoc_timeout_copy (mongoc_timeout_t *expected)
{
   mongoc_timeout_t *actual = mongoc_timeout_copy (expected);

   /* assert different memory addresses */
   BSON_ASSERT (expected != actual);

   BSON_ASSERT (mongoc_timeout_is_set (actual) == mongoc_timeout_is_set (expected));

   if (mongoc_timeout_is_set (actual)) {
      BSON_ASSERT (mongoc_timeout_get_timeout_ms (actual) == mongoc_timeout_get_timeout_ms (expected));
   }

   mongoc_timeout_destroy (actual);
}
void
test_mongoc_timeout_copy (void)
{
   mongoc_timeout_t *timeout = NULL;

   timeout = mongoc_timeout_new ();
   _test_mongoc_timeout_copy (timeout);
   mongoc_timeout_destroy (timeout);

   timeout = mongoc_timeout_new_timeout_int64 (1);
   _test_mongoc_timeout_copy (timeout);
   mongoc_timeout_destroy (timeout);
}

void
test_mongoc_timeout_destroy (void)
{
   mongoc_timeout_destroy (NULL);
}

void
test_timeout_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Timeout/new", test_mongoc_timeout_new);
   TestSuite_Add (suite, "/Timeout/set", test_mongoc_timeout_set);
   TestSuite_Add (suite, "/Timeout/get", test_mongoc_timeout_get);
   TestSuite_Add (suite, "/Timeout/copy", test_mongoc_timeout_copy);
   TestSuite_Add (suite, "/Timeout/destroy", test_mongoc_timeout_destroy);
}
