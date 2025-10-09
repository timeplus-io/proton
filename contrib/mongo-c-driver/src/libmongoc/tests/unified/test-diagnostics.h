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

#ifndef UNIFIED_TEST_DIAGNOSTICS
#define UNIFIED_TEST_DIAGNOSTICS

#include "bson/bson.h"

void
_test_diagnostics_add (bool fail, const char *fmt, ...) BSON_GNUC_PRINTF (2, 3);

#define test_diagnostics_test_info(fmt, ...) \
   _test_diagnostics_add (false, "[%s:%d %s()]\n" fmt, __FILE__, __LINE__, BSON_FUNC, __VA_ARGS__)

/* Append additional information to an error after it has occurred (similar to
 * backtrace). */
#define test_diagnostics_error_info(fmt, ...) \
   _test_diagnostics_add (true, "[%s:%d %s()]\n" fmt, __FILE__, __LINE__, BSON_FUNC, __VA_ARGS__)

void
test_diagnostics_init (void);

void
test_diagnostics_cleanup (void);

void
test_diagnostics_reset (void);

void
test_diagnostics_abort (bson_error_t *error);

#endif /* UNIFIED_TEST_DIAGNOSTICS */
