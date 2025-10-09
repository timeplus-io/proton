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

#ifndef TEST_KMS_UTIL_H
#define TEST_KMS_UTIL_H

#include <stdio.h>
#include "kms_request_str.h"

/* copy_and_filter_hex returns a copy of @unfiltered_hex with the following
 * characters removed: ' ', '|' */
char *
copy_and_filter_hex (const char *unfiltered_hex);

/* hex_to_data calls copy_and_filter_hex on @unfiltered_hex, then converts it to
 * binary and returns a byte array. */
uint8_t *
hex_to_data (const char *unfiltered_hex, size_t *outlen);

char *
data_to_hex (const uint8_t *data, size_t len);

/* replace_all returns a copy of @input with all occurrences of @match replaced
 * with @replacement. */
char *
replace_all (const char *input, const char *match, const char *replacement);

/* test_kms_util tests utility functions. */
void
test_kms_util (void);

#endif /* TEST_KMS_UTIL_H */
