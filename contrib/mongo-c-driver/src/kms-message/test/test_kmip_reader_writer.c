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

#include "kms_kmip_reader_writer_private.h"

#include <stdio.h>
#include <stdlib.h>

typedef struct {
   char *desc;
   char *expected_hex;
} kms_kmip_writer_test_case_t;

static void
kms_kmip_writer_test_evaluate (kmip_writer_t *writer,
                               const char *expected_hex_in,
                               char *desc)
{
   char *expected_hex;
   const uint8_t *actual_buf;
   size_t actual_len;
   char *actual_hex;

   expected_hex = copy_and_filter_hex (expected_hex_in);
   actual_buf = kmip_writer_get_buffer (writer, &actual_len);
   actual_hex = data_to_hex (actual_buf, actual_len);

   if (0 != strcmp (expected_hex, actual_hex)) {
      fprintf (stderr,
               "expected '%s' but got '%s' for test description: %s\n",
               expected_hex,
               actual_hex,
               desc);
      abort ();
   }

   free (actual_hex);
   free (expected_hex);
}

void
kms_kmip_writer_test (void)
{
   kmip_writer_t *writer;

   /* The following test cases come from section 9.1.2 of
    * http://docs.oasis-open.org/kmip/spec/v1.4/os/kmip-spec-v1.4-os.html */
   writer = kmip_writer_new ();
   kmip_writer_write_integer (writer, KMIP_TAG_CompromiseDate, 8);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 02 | 00 00 00 04 | 00 00 00 08 00 00 00 00",
      "An Integer containing the decimal value 8");
   kmip_writer_destroy (writer);

   writer = kmip_writer_new ();
   kmip_writer_write_long_integer (
      writer, KMIP_TAG_CompromiseDate, 123456789000000000LL);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 03 | 00 00 00 08 | 01 B6 9B 4B A5 74 92 00",
      "A Long Integer containing the decimal value 123456789000000000");
   kmip_writer_destroy (writer);

   /* BigInteger is not implemented. */

   writer = kmip_writer_new ();
   kmip_writer_write_enumeration (writer, KMIP_TAG_CompromiseDate, 255);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 05 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
      "An Enumeration with value 255");
   kmip_writer_destroy (writer);

   /* Boolean is not implemented. */

   writer = kmip_writer_new ();
   kmip_writer_write_string (
      writer, KMIP_TAG_CompromiseDate, "Hello World", 11);
   kms_kmip_writer_test_evaluate (writer,
                                  "42 00 20 | 07 | 00 00 00 0B | 48 65 6C 6C "
                                  "6F 20 57 6F 72 6C 64 00 00 00 00 00",
                                  "A Text String with the value 'Hello World'");
   kmip_writer_destroy (writer);

   writer = kmip_writer_new ();
   kmip_writer_write_bytes (writer, KMIP_TAG_CompromiseDate, "\01\02\03", 3);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 08 | 00 00 00 03 | 01 02 03 00 00 00 00 00",
      "A Byte String with the value { 0x01, 0x02, 0x03 }");
   kmip_writer_destroy (writer);

   writer = kmip_writer_new ();
   kmip_writer_write_datetime (
      writer, KMIP_TAG_CompromiseDate, 0x0000000047DA67F8LL);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 09 | 00 00 00 08 | 00 00 00 00 47 DA 67 F8",
      "A Date-Time, containing the value for Friday, March 14, 2008, 11:56:40 "
      "GMT");
   kmip_writer_destroy (writer);

   /* Interval is not implemented. */

   writer = kmip_writer_new ();
   kmip_writer_begin_struct (writer, KMIP_TAG_CompromiseDate);
   kmip_writer_write_enumeration (
      writer, KMIP_TAG_ApplicationSpecificInformation, 254);
   kmip_writer_write_integer (writer, KMIP_TAG_ArchiveDate, 255);
   kmip_writer_close_struct (writer);
   kms_kmip_writer_test_evaluate (
      writer,
      "42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE "
      "00 00 00 00 | 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
      "A Structure containing an Enumeration, value 254, followed by an "
      "Integer, value 255, having tags 420004 and 420005 respectively");
   kmip_writer_destroy (writer);
}

void
kms_kmip_reader_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   kmip_tag_type_t tag;
   kmip_item_type_t type;
   uint32_t length;
   int32_t i32;
   int64_t i64;
   uint32_t u32;
   uint8_t *ptr;

   /* The following test cases come from section 9.1.2 of
    * http://docs.oasis-open.org/kmip/spec/v1.4/os/kmip-spec-v1.4-os.html */
   /* An Integer containing the decimal value 8 */
   data = hex_to_data ("42 00 20 | 02 | 00 00 00 04 | 00 00 00 08 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Integer);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_integer (reader, &i32));
   ASSERT (i32 == 8);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* A Long Integer containing the decimal value 123456789000000000 */
   data = hex_to_data ("42 00 20 | 03 | 00 00 00 08 | 01 B6 9B 4B A5 74 92 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_LongInteger);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 8);
   ASSERT (kmip_reader_read_long_integer (reader, &i64));
   ASSERT (i64 == 123456789000000000LL);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* Big Integer is not implemented. */

   /* An Enumeration with value 255 */
   data = hex_to_data ("42 00 20 | 05 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Enumeration);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_enumeration (reader, &u32));
   ASSERT (u32 == 255);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* Boolean is not implemented */

   /* A Text String with the value 'Hello World' */
   data = hex_to_data ("42 00 20 | 07 | 00 00 00 0B | 48 65 6C "
                       "6C 6F 20 57 6F 72 6C 64 00 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_TextString);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 11);
   ASSERT (kmip_reader_read_string (reader, &ptr, length));
   ASSERT (0 == strncmp ("Hello World", (const char *) ptr, length));
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* A Byte String with the value { 0x01, 0x02, 0x03 } */
   data = hex_to_data ("42 00 20 | 08 | 00 00 00 03 | 01 02 03 00 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_ByteString);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 3);
   ASSERT (kmip_reader_read_bytes (reader, &ptr, length));
   ASSERT (0 == strncmp ("\01\02\03", (const char *) ptr, length));
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* A Date-Time, containing the value for Friday, March 14, 2008, 11:56:40 GMT
    */
   data = hex_to_data ("42 00 20 | 09 | 00 00 00 08 | 00 00 00 00 47 DA 67 F8",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_DateTime);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 8);
   kmip_reader_read_long_integer (reader, &i64);
   ASSERT (i64 == 0x47DA67F8);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* Interval is not implemented. */

   /* A Structure containing an Enumeration, value 254, followed by an Integer,
    * value 255, having tags 420004 and 420005 respectively */
   data = hex_to_data (
      "42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE "
      "00 00 00 00 | 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
      &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Structure);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 0x20);

   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_ApplicationSpecificInformation);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Enumeration);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_enumeration (reader, &u32));
   ASSERT (u32 == 254);

   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_ArchiveDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Integer);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_integer (reader, &i32));
   ASSERT (i32 == 255);

   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);
}

void
kms_kmip_reader_negative_int_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   kmip_tag_type_t tag;
   kmip_item_type_t type;
   uint32_t length;
   int32_t i32;

   /* Test reading the integer -1. */
   data = hex_to_data ("42 00 20 | 02 | 00 00 00 04 | FF FF FF FF 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Integer);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_integer (reader, &i32));
   ASSERT (i32 == -1);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);

   /* Test reading the integer INT32_MIN (-2^31). */
   data = hex_to_data ("42 00 20 | 02 | 00 00 00 04 | 80 00 00 00 00 00 00 00",
                       &datalen);
   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_read_tag (reader, &tag));
   ASSERT (tag == KMIP_TAG_CompromiseDate);
   ASSERT (kmip_reader_read_type (reader, &type));
   ASSERT (type == KMIP_ITEM_TYPE_Integer);
   ASSERT (kmip_reader_read_length (reader, &length));
   ASSERT (length == 4);
   ASSERT (kmip_reader_read_integer (reader, &i32));
   ASSERT (i32 == INT32_MIN);
   ASSERT (!kmip_reader_has_data (reader));
   kmip_reader_destroy (reader);
   free (data);
}

void
kms_kmip_reader_find_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   bool found;
   size_t pos = 0;
   size_t len = 0;

   /* A Structure containing an Enumeration, value 254, followed by an Integer,
    * value 255, having tags 420004 and 420005 respectively */
   data = hex_to_data (
      "42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE "
      "00 00 00 00 | 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
      &datalen);

   reader = kmip_reader_new (data, datalen);

   /* Finds the top-level Structure. */
   found = kmip_reader_find (
      reader, KMIP_TAG_CompromiseDate, KMIP_ITEM_TYPE_Structure, &pos, &len);
   ASSERT (found);
   ASSERT (pos == 0);
   ASSERT (len == 32);

   /* Mismatched tag does not find the Structure. */
   found = kmip_reader_find (
      reader, KMIP_TAG_ActivationDate, KMIP_ITEM_TYPE_Structure, &pos, &len);
   ASSERT (!found);

   /* Mismatched type does not find the Structure. */
   found = kmip_reader_find (
      reader, KMIP_TAG_CompromiseDate, KMIP_ITEM_TYPE_Integer, &pos, &len);
   ASSERT (!found);

   /* Values nested within the Structure are not found. */
   found = kmip_reader_find (reader,
                             KMIP_TAG_ApplicationSpecificInformation,
                             KMIP_ITEM_TYPE_Enumeration,
                             &pos,
                             &len);
   ASSERT (!found);

   kmip_reader_destroy (reader);
   free (data);
}

void
kms_kmip_reader_find_and_recurse_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   bool found;
   size_t pos = 0;
   size_t len = 0;

   /* A Structure containing an Enumeration, value 254, followed by an Integer,
    * value 255, having tags 420004 and 420005 respectively */
   data = hex_to_data (
      "42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE "
      "00 00 00 00 | 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00",
      &datalen);

   reader = kmip_reader_new (data, datalen);
   ASSERT (kmip_reader_find_and_recurse (reader, KMIP_TAG_CompromiseDate));

   /* Values nested within the Structure are found. */
   found = kmip_reader_find (reader,
                             KMIP_TAG_ApplicationSpecificInformation,
                             KMIP_ITEM_TYPE_Enumeration,
                             &pos,
                             &len);
   ASSERT (found);
   ASSERT (pos == 8);
   ASSERT (len == 4);

   kmip_reader_destroy (reader);
   free (data);
}

void
kms_kmip_reader_find_and_read_enum_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   bool found;
   uint32_t value;

   /* An Integer, value 255, followed by an Enumeration, value 254 having tags
    * 420005 and 420004 respectively. */
   data = hex_to_data (
      "42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00 | 42 00 04 | 05 | "
      "00 00 00 04 | 00 00 00 FE 00 00 00 00",
      &datalen);

   reader = kmip_reader_new (data, datalen);

   found = kmip_reader_find_and_read_enum (
      reader, KMIP_TAG_ApplicationSpecificInformation, &value);
   ASSERT (found);
   ASSERT (value == 254);

   /* The Integer should not be found. */
   found =
      kmip_reader_find_and_read_enum (reader, KMIP_TAG_ArchiveDate, &value);
   ASSERT (!found);

   kmip_reader_destroy (reader);
   free (data);
}

void
kms_kmip_reader_find_and_read_bytes_test (void)
{
   uint8_t *data;
   size_t datalen;
   kmip_reader_t *reader;
   bool found;
   uint8_t *outptr;
   size_t outlen;

   /* An Integer, value 255, followed by ByteString of value 0x1122 having tags
    * 420005 and 420004 respectively. */
   data = hex_to_data (
      "42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00 | 42 00 04 | 08 | "
      "00 00 00 02 | 11 22 00 00 00 00 00 00",
      &datalen);

   reader = kmip_reader_new (data, datalen);

   found = kmip_reader_find_and_read_bytes (
      reader, KMIP_TAG_ApplicationSpecificInformation, &outptr, &outlen);
   ASSERT (found);
   ASSERT (outlen == 2);
   ASSERT (outptr[0] == 0x11);
   ASSERT (outptr[1] == 0x22);

   /* The Integer should not be found. */
   found = kmip_reader_find_and_read_bytes (
      reader, KMIP_TAG_ArchiveDate, &outptr, &outlen);
   ASSERT (!found);

   kmip_reader_destroy (reader);
   free (data);
}
