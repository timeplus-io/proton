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

#include "test_kms_assert.h"
#include "test_kms_util.h"

#include "kms_message/kms_kmip_response_parser.h"

#include "kms_kmip_response_parser_private.h"

/* The following sample data come from section 9.1.2 of
 * http://docs.oasis-open.org/kmip/spec/v1.4/os/kmip-spec-v1.4-os.html. The
 * data describes "A Structure containing an Enumeration, value 254, followed
 * by an Integer, value 255, having tags 420004 and 420005 respectively." */
static const char *const SAMPLE_KMIP =
   "42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE 00 "
   "00 00 00 | 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00";

/* SAMPLE_KMIP_FIRST_LENGTH is the length of message after the first tag, type,
 * and length. */
static const int SAMPLE_KMIP_FIRST_LENGTH = 32;

/* SAMPLE_KMIP_LARGE_LENGTH is a byte size larger than the message. */
static const int SAMPLE_KMIP_LARGE_LENGTH = 1024;

void
kms_kmip_response_parser_test (void)
{
   kms_response_parser_t *parser;
   uint8_t *data;
   size_t outlen;
   int32_t want_bytes;
   kms_response_t *res;
   bool ok;

   data = hex_to_data (SAMPLE_KMIP, &outlen);
   parser = kms_kmip_response_parser_new (NULL);

   want_bytes =
      kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   ASSERT_CMPINT (KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH, ==, want_bytes);

   /* A smaller maximum size caps the requested bytes. */
   want_bytes = kms_response_parser_wants_bytes (parser, 1);
   ASSERT_CMPINT (1, ==, want_bytes);

   /* Feed one byte */
   ok = kms_response_parser_feed (parser, data, 1);
   ASSERT_PARSER_OK (parser);
   ASSERT (ok);

   want_bytes =
      kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   ASSERT_CMPINT (KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH - 1, ==, want_bytes);

   /* Feed the remaining bytes. */
   ok = kms_response_parser_feed (
      parser, data + 1, KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH - 1);
   ASSERT_PARSER_OK (parser);
   ASSERT (ok);

   /* The parser knows first length. Expect the parser wants the remaining
    * length. */
   want_bytes =
      kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   ASSERT_CMPINT (want_bytes, ==, SAMPLE_KMIP_FIRST_LENGTH);

   ok = kms_response_parser_feed (parser,
                                  data + KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH,
                                  SAMPLE_KMIP_FIRST_LENGTH);
   ASSERT_PARSER_OK (parser);
   ASSERT (ok);

   /* Parser has full message. */
   want_bytes =
      kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   ASSERT_CMPINT (want_bytes, ==, 0);

   res = kms_response_parser_get_response (parser);
   ASSERT_PARSER_OK (parser);
   ASSERT (res);
   kms_response_destroy (res);

   kms_response_parser_destroy (parser);
   free (data);
}

static void
feed_full_response (kms_response_parser_t *parser, uint8_t *data)
{
   uint32_t i = 0;
   int32_t want_bytes;
   bool ok;

   want_bytes =
      kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   ASSERT_CMPINT (KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH, ==, want_bytes);
   while (want_bytes > 0) {
      ok = kms_response_parser_feed (parser, data + i, want_bytes);
      ASSERT_PARSER_OK (parser);
      ASSERT (ok);
      i += want_bytes;
      want_bytes =
         kms_response_parser_wants_bytes (parser, SAMPLE_KMIP_LARGE_LENGTH);
   }
}

void
kms_kmip_response_parser_reuse_test (void)
{
   kms_response_parser_t *parser;
   uint8_t *data;
   size_t outlen;
   kms_response_t *res;

   data = hex_to_data (SAMPLE_KMIP, &outlen);
   parser = kms_kmip_response_parser_new (NULL);

   feed_full_response (parser, data);
   ASSERT_PARSER_OK (parser);

   res = kms_response_parser_get_response (parser);
   ASSERT_PARSER_OK (parser);
   ASSERT (res);
   kms_response_destroy (res);

   /* Feed another full response. */
   feed_full_response (parser, data);
   ASSERT_PARSER_OK (parser);

   res = kms_response_parser_get_response (parser);
   ASSERT_PARSER_OK (parser);
   ASSERT (res);
   kms_response_destroy (res);

   kms_response_parser_destroy (parser);
   free (data);
}

void
kms_kmip_response_parser_excess_test (void)
{
   kms_response_parser_t *parser;
   uint8_t *data;
   size_t outlen;
   bool ok;

   data = hex_to_data (SAMPLE_KMIP, &outlen);
   parser = kms_kmip_response_parser_new (NULL);

   feed_full_response (parser, data);
   ASSERT_PARSER_OK (parser);

   ok = kms_response_parser_feed (parser, data, 1);
   ASSERT_PARSER_ERROR (parser, "KMIP parser was fed too much data");
   ASSERT (!ok);
   kms_response_parser_destroy (parser);
   free (data);
}

void
kms_kmip_response_parser_notenough_test (void)
{
   kms_response_parser_t *parser;
   uint8_t *data;
   size_t outlen;
   kms_response_t *res;
   bool ok;

   data = hex_to_data (SAMPLE_KMIP, &outlen);
   parser = kms_kmip_response_parser_new (NULL);

   ok = kms_response_parser_feed (
      parser, data, KMS_KMIP_RESPONSE_PARSER_FIRST_LENGTH);
   ASSERT_PARSER_OK (parser);
   ASSERT (ok);

   res = kms_response_parser_get_response (parser);
   ASSERT_PARSER_ERROR (parser, "KMIP parser does not have a complete message");
   ASSERT (!res);

   kms_response_parser_destroy (parser);
   free (data);
}
