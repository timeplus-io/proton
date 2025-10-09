/*
 * Copyright 2020-present MongoDB, Inc.
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

#include <bson/bson.h>

#include "TestSuite.h"
#include "common-b64-private.h"

static void
_test_encode_helper (char *input, size_t input_len, char *expected_output, int expected_output_len)
{
   char *output;
   size_t target_size;
   int ret;

   target_size = mcommon_b64_ntop_calculate_target_size (input_len);
   output = bson_malloc (target_size);

   /* bson_ntop_calculate_target_size includes trailing NULL. */
   ASSERT_CMPSIZE_T (target_size, ==, (size_t) expected_output_len + 1);
   /* returned value does not count trailing NULL. */
   ret = mcommon_b64_ntop ((uint8_t *) input, input_len, output, target_size);
   ASSERT (bson_cmp_equal_us (target_size - 1u, ret));
   ASSERT_CMPSTR (output, expected_output);
   bson_free (output);
}

static void
test_bson_b64_encode (void)
{
   char output[32];
   int ret;

   _test_encode_helper ("", 0, "", 0);
   _test_encode_helper ("f", 1, "Zg==", 4);
   _test_encode_helper ("fo", 2, "Zm8=", 4);
   _test_encode_helper ("foo", 3, "Zm9v", 4);
   _test_encode_helper ("foob", 4, "Zm9vYg==", 8);
   _test_encode_helper ("fooba", 5, "Zm9vYmE=", 8);
   _test_encode_helper ("foobar", 6, "Zm9vYmFy", 8);

   /* Even on empty input, the output is still NULL terminated. */
   output[0] = 'a';
   ret = mcommon_b64_ntop ((uint8_t *) "", 0, output, 1);
   ASSERT_CMPINT (ret, ==, 0);
   BSON_ASSERT (output[0] == '\0');

   /* Test NULL input. */
   ret = mcommon_b64_ntop (NULL, 0, output, 32);
   ASSERT_CMPINT (0, ==, ret);
   BSON_ASSERT (output[0] == '\0');

   /* Test NULL output */
   ret = mcommon_b64_ntop (NULL, 0, NULL, 0);
   ASSERT_CMPINT (-1, ==, ret);

   /* Test output not large enough. */
   ret = mcommon_b64_ntop ((uint8_t *) "test", 4, output, 1);
   ASSERT_CMPINT (-1, ==, ret);
}

static void
_test_decode_helper (char *input, char *expected_output, int expected_calculated_target_size, int expected_output_len)
{
   uint8_t *output;
   size_t target_size;
   int exact_target_size;
   int ret;

   target_size = mcommon_b64_pton_calculate_target_size (strlen (input));
   output = bson_malloc (target_size);
   /* bson_malloc returns NULL if requesting 0 bytes, memcmp expects non-NULL.
    */
   if (target_size == 0) {
      output = bson_malloc (1);
   }

   /* Calling mcommon_b64_pton with a NULL output is valid, and
    * returns the exact target size. */
   exact_target_size = mcommon_b64_pton (input, NULL, 0);
   ASSERT_CMPINT (exact_target_size, ==, expected_output_len);
   ASSERT_CMPSIZE_T (target_size, ==, (size_t) expected_calculated_target_size);

   ret = mcommon_b64_pton (input, output, target_size);
   ASSERT_CMPINT (expected_output_len, ==, ret);
   BSON_ASSERT (0 == memcmp (output, expected_output, ret));
   bson_free (output);
}

static void
test_bson_b64_decode (void)
{
   uint8_t output[32];
   int ret;

   _test_decode_helper ("", "", 0, 0);
   _test_decode_helper ("Zg==", "f", 3, 1);
   _test_decode_helper ("Zm8=", "fo", 3, 2);
   _test_decode_helper ("Zm9v", "foo", 3, 3);
   _test_decode_helper ("Zm9vYg==", "foob", 6, 4);
   _test_decode_helper ("Zm9vYmE=", "fooba", 6, 5);
   _test_decode_helper ("Zm9vYmFy", "foobar", 6, 6);

   /* Test NULL input. */
   ret = mcommon_b64_pton (NULL, output, 32);
   ASSERT_CMPINT (ret, ==, -1);

   /* Test NULL output. */
   ret = mcommon_b64_pton ("Zm8=", NULL, 0);
   /* The return value is actually the number of bytes for output. */
   ASSERT_CMPINT (ret, ==, 2);

   /* Test output not large enough. */
   ret = mcommon_b64_pton ("Zm9vYmFy", output, 1);
   ASSERT_CMPINT (ret, ==, -1);

   /* Test malformed base64 */
   ret = mcommon_b64_pton ("bad!", output, 32);
   ASSERT_CMPINT (ret, ==, -1);
}

void
test_b64_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/bson/b64/encode", test_bson_b64_encode);
   TestSuite_Add (suite, "/bson/b64/decode", test_bson_b64_decode);
}
