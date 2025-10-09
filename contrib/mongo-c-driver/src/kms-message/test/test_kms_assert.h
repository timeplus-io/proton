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

#ifndef TEST_KMS_ASSERT_H
#define TEST_KMS_ASSERT_H

#include "kms_request_str.h"
#include "test_kms_util.h"

#include <stdio.h>
#include <string.h>

#define TEST_ERROR(...)                                                        \
   do {                                                                        \
      fprintf (                                                                \
         stderr, "test error %s:%d %s(): ", __FILE__, __LINE__, __FUNCTION__); \
      fprintf (stderr, __VA_ARGS__);                                           \
      fprintf (stderr, "\n");                                                  \
      fflush (stderr);                                                         \
      abort ();                                                                \
   } while (0)

#define ASSERT(stmt)                             \
   if (!(stmt)) {                                \
      TEST_ERROR ("statement failed %s", #stmt); \
   }

#define ASSERT_CMPSTR_WITH_LEN(_expect, _expect_len, _actual, _actual_len)     \
   do {                                                                        \
      kms_request_str_t *_expect_str =                                         \
         kms_request_str_new_from_chars ((_expect), _expect_len);              \
      kms_request_str_t *_actual_str =                                         \
         kms_request_str_new_from_chars ((_actual), _actual_len);              \
      if (0 != strcmp (_expect_str->str, _actual_str->str)) {                  \
         TEST_ERROR (                                                          \
            "strings not equal:\n%s\n%s", _expect_str->str, _actual_str->str); \
      }                                                                        \
      kms_request_str_destroy (_actual_str);                                   \
      kms_request_str_destroy (_expect_str);                                   \
   } while (0)

#define ASSERT_CMPSTR(_expect, _actual) \
   ASSERT_CMPSTR_WITH_LEN (             \
      (_expect), strlen (_expect), (_actual), strlen (_actual))

#define ASSERT_CONTAINS(_a, _b)                                              \
   do {                                                                      \
      kms_request_str_t *_a_str = kms_request_str_new_from_chars ((_a), -1); \
      kms_request_str_t *_b_str = kms_request_str_new_from_chars ((_b), -1); \
      kms_request_str_t *_a_lower = kms_request_str_new ();                  \
      kms_request_str_t *_b_lower = kms_request_str_new ();                  \
      kms_request_str_append_lowercase (_a_lower, (_a_str));                 \
      kms_request_str_append_lowercase (_b_lower, (_b_str));                 \
      if (NULL == strstr ((_a_lower->str), (_b_lower->str))) {               \
         TEST_ERROR ("string \"%s\" does not contain \"%s\"", _a, _b);       \
      }                                                                      \
      kms_request_str_destroy (_a_str);                                      \
      kms_request_str_destroy (_b_str);                                      \
      kms_request_str_destroy (_a_lower);                                    \
      kms_request_str_destroy (_b_lower);                                    \
   } while (0)

#define ASSERT_CMPINT(_a, _operator, _b)                                \
   do {                                                                 \
      int _a_int = _a;                                                  \
      int _b_int = _b;                                                  \
      if (!(_a_int _operator _b_int)) {                                 \
         TEST_ERROR (                                                   \
            "comparison failed: %d %s %d", _a_int, #_operator, _b_int); \
      }                                                                 \
   } while (0);

#define ASSERT_CMPBYTES(                                                \
   expected_bytes, expected_len, actual_bytes, actual_len)              \
   do {                                                                 \
      char *_actual_hex = data_to_hex (actual_bytes, actual_len);       \
      char *_expected_hex = data_to_hex (expected_bytes, expected_len); \
      ASSERT_CMPSTR (_actual_hex, _expected_hex);                       \
      free (_actual_hex);                                               \
      free (_expected_hex);                                             \
   } while (0)

#define ASSERT_REQUEST_OK(req)                                \
   do {                                                       \
      if (kms_request_get_error (req)) {                      \
         TEST_ERROR ("expected request ok but got error: %s", \
                     kms_request_get_error (req));            \
      }                                                       \
   } while (0)

#define ASSERT_REQUEST_ERROR(req, expect_substring)         \
   do {                                                     \
      if (!kms_request_get_error (req)) {                   \
         TEST_ERROR ("expected request error but got ok");  \
      }                                                     \
      const char *_error_str = kms_request_get_error (req); \
      ASSERT_CONTAINS (_error_str, expect_substring);       \
   } while (0)

#define ASSERT_RESPONSE_OK(req)                                \
   do {                                                        \
      if (kms_response_get_error (req)) {                      \
         TEST_ERROR ("expected response ok but got error: %s", \
                     kms_response_get_error (req));            \
      }                                                        \
   } while (0)

#define ASSERT_RESPONSE_ERROR(req, expect_substring)         \
   do {                                                      \
      if (!kms_response_get_error (req)) {                   \
         TEST_ERROR ("expected response error but got ok");  \
      }                                                      \
      const char *_error_str = kms_response_get_error (req); \
      ASSERT_CONTAINS (_error_str, expect_substring);        \
   } while (0)

#define ASSERT_PARSER_OK(parser)                             \
   do {                                                      \
      if (kms_response_parser_error (parser)) {              \
         TEST_ERROR ("expected parser ok but got error: %s", \
                     kms_response_parser_error (parser));    \
      }                                                      \
   } while (0)

#define ASSERT_PARSER_ERROR(parser, expect_substring)              \
   do {                                                            \
      if (!kms_response_parser_error (parser)) {                   \
         TEST_ERROR ("expected parser error but got ok");          \
      }                                                            \
      const char *_error_str = kms_response_parser_error (parser); \
      ASSERT_CONTAINS (_error_str, expect_substring);              \
   } while (0)

#endif /* TEST_KMS_ASSERT_H */