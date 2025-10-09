/*
 * Copyright 2018-present MongoDB, Inc.
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

/* Needed for strptime */
#define _GNU_SOURCE

#include "kms_message/kms_message.h"
#include "kms_message_private.h"

#ifndef _WIN32
#include <dirent.h>
#else
#include "windows/dirent.h"
#endif
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include "kms_message/kms_azure_request.h"
#include "kms_message/kms_b64.h"
#include "hexlify.h"
#include "kms_request_str.h"
#include "kms_kv_list.h"
#include "kms_port.h"

#include "test_kms_assert.h"

const char *aws_test_suite_dir = "aws-sig-v4-test-suite";

const char *skipped_aws_tests[] = {
   /* we don't yet support temporary security credentials provided by the AWS
    * Security Token Service (AWS STS). see post-sts-token/readme.txt */
   "post-sts-token",
};

bool
skip_aws_test (const char *test_name)
{
   size_t i;

   for (i = 0; i < sizeof (skipped_aws_tests) / sizeof (char *); i++) {
      if (0 == strcmp (test_name, skipped_aws_tests[i])) {
         return true;
      }
   }

   return false;
}

bool
ends_with (const char *str, const char *suffix)
{
   size_t str_len = strlen (str);
   size_t suf_len = strlen (suffix);
   if (str_len >= suf_len &&
       0 == strncmp (&str[str_len - suf_len], suffix, suf_len)) {
      return true;
   }

   return false;
}


char *
last_segment (const char *str)
{
   const char *p = str + strlen (str);

   while (--p > str) {
      if (*p == '/') {
         return strdup (p + 1);
      }
   }

   return strdup (str);
}

char *
test_file_path (const char *path, const char *suffix)
{
   char *r;
   char *test_name = last_segment (path);
   char file_path[PATH_MAX];
   snprintf (file_path, PATH_MAX, "%s/%s.%s", path, test_name, suffix);
   r = strdup (file_path);
   free (test_name);
   return r;
}

void
realloc_buffer (char **buffer, size_t *n, size_t len)
{
   if (*buffer == NULL) {
      *buffer = malloc (len);
      KMS_ASSERT (*buffer);

   } else {
      *buffer = realloc (*buffer, len);
   }

   *n = len;
}

ssize_t
test_getline (char **lineptr, size_t *n, FILE *stream)
{
   if (*lineptr == NULL && *n == 0) {
      realloc_buffer (lineptr, n, 128);
   };

   // Sanity check
   if ((*lineptr == NULL && *n != 0) || (*lineptr != NULL && *n == 0)) {
      abort ();
   }

   ssize_t count = 0;

   while (true) {
      // Read a character
      int c = fgetc (stream);

      // If the buffer is full, grow the buffer
      if ((*n - count) <= 1) {
         realloc_buffer (lineptr, n, *n + 128);
      }

      if (c == EOF) {
         *(*lineptr + count) = '\0';

         if (count > 0) {
            return count;
         }

         return -1;
      }

      *(*lineptr + count) = c;

      ++count;
      // If we hit the end of the line, we are done
      if (c == '\n') {
         *(*lineptr + count) = '\0';

         return count;
      }
   }
}

char *
read_test (const char *path, const char *suffix)
{
   char *file_path = test_file_path (path, suffix);
   FILE *f;
   struct stat file_stat;
   size_t f_size;
   char *str;

   if (0 != stat (file_path, &file_stat)) {
      perror (file_path);
      abort ();
   }

   f = fopen (file_path, "r");
   if (!f) {
      perror (file_path);
      abort ();
   }

   f_size = (size_t) file_stat.st_size;
   str = malloc (f_size + 1);
   KMS_ASSERT (str);

   memset (str, 0, f_size + 1);

// Windows will convert crlf to lf
// We want this behavior in this function call but
// it prevents us from validating we read the whole file here.
#ifndef _WIN32
   if (f_size != fread (str, 1, f_size, f)) {
      perror (file_path);
      abort ();
   }
#else
   fread (str, 1, f_size, f);
#endif

   fclose (f);
   str[f_size] = '\0';

   free (file_path);

   return str;
}

void
set_test_date (kms_request_t *request)
{
   struct tm tm;

   /* all tests use the same date and time: 20150830T123600Z */
   tm.tm_year = 115;
   tm.tm_mon = 7;
   tm.tm_mday = 30;

   tm.tm_yday = 241;
   tm.tm_wday = 0;

   tm.tm_hour = 12;
   tm.tm_min = 36;
   tm.tm_sec = 0;

   KMS_ASSERT (kms_request_set_date (request, &tm));
}

kms_request_t *
read_req (const char *path)
{
   kms_request_t *request;
   char *file_path = test_file_path (path, "req");
   FILE *f;
   size_t len = 0;
   ssize_t line_len;
   char *line = NULL;
   char *method;
   char *uri_path;
   char *field_name;
   char *field_value;
   bool r;

   f = fopen (file_path, "r");
   if (!f) {
      perror (file_path);
      abort ();
   }

   /* like "GET /path HTTP/1.1" */
   line_len = test_getline (&line, &len, f);
   method = kms_strndup (line, strchr (line, ' ') - line);
   uri_path =
      kms_strndup (line + strlen (method) + 1,
                   line_len - strlen (method) - 1 - strlen (" HTTP/1.1\n"));

   request = kms_request_new (method, uri_path, NULL);
   request->auto_content_length = false;
   /* from docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html */
   kms_request_set_region (request, "us-east-1");
   kms_request_set_service (request, "service");
   kms_request_set_access_key_id (request, "AKIDEXAMPLE");
   kms_request_set_secret_key (request,
                               "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

   while ((line_len = test_getline (&line, &len, f)) != -1) {
      if (strchr (line, ':')) {
         /* new header field like Host:example.com */
         field_name = strtok (line, ": ");
         KMS_ASSERT (field_name);
         field_value = strtok (NULL, "\n");
         KMS_ASSERT (field_value);
         r = kms_request_add_header_field (request, field_name, field_value);
         KMS_ASSERT (r);
      } else if (0 == strcmp (line, "\n")) {
         /* end of header */
         break;
      } else if (line_len > 2) {
         /* continuing a multiline header value from previous line */
         /* Header line folding is deprecated by RFC 7230 section 3.2. */
         /* get-header-value-multiline tests this behavior. */
         kms_request_append_header_field_value (request, "\r\n", 2);
         /* omit this line's newline */
         kms_request_append_header_field_value (
            request, line, (size_t) (line_len - 1));
      }
   }

   while ((line_len = test_getline (&line, &len, f)) != -1) {
      KMS_ASSERT (kms_request_append_payload (request, line, (size_t) line_len));
   }

   fclose (f);
   free (file_path);
   free (line);
   free (uri_path);
   free (method);

   set_test_date (request);

   return request;
}

void
test_compare (kms_request_t *request,
              char *(*func) (kms_request_t *),
              const char *dir_path,
              const char *suffix)
{
   char *test_name = last_segment (dir_path);
   char *expect;
   char *actual;

   expect = read_test (dir_path, suffix);
   if (0 == strcmp (suffix, "sreq")) {
      /* The final signed request is an HTTP request.
       * The expected output must contain \r\n, not \n. */
      char* tmp = replace_all (expect, "\n", "\r\n");
      free (expect);
      expect = tmp;
   }
   actual = func (request);
   ASSERT_CMPSTR (expect, actual);
   free (actual);
   free (expect);
   free (test_name);
}

void
test_compare_creq (kms_request_t *request, const char *dir_path)
{
   test_compare (request, kms_request_get_canonical, dir_path, "creq");
}

void
test_compare_sts (kms_request_t *request, const char *dir_path)
{
   test_compare (request, kms_request_get_string_to_sign, dir_path, "sts");
}

void
test_compare_authz (kms_request_t *request, const char *dir_path)
{
   test_compare (request, kms_request_get_signature, dir_path, "authz");
}

void
test_compare_sreq (kms_request_t *request, const char *dir_path)
{
   test_compare (request, kms_request_get_signed, dir_path, "sreq");
}

void
aws_sig_v4_test (const char *dir_path)
{
   kms_request_t *request;

   request = read_req (dir_path);
   test_compare_creq (request, dir_path);
   test_compare_sts (request, dir_path);
   test_compare_authz (request, dir_path);
   test_compare_sreq (request, dir_path);
   kms_request_destroy (request);
}

bool
all_aws_sig_v4_tests (const char *path, const char *selected)
{
   /* Amazon supplies tests, one per directory, 5 files per test, see
    * docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html */
   DIR *dp;
   struct dirent *ent;
   bool ran_tests = false;
   char *test_name = last_segment (path);
   char sub[PATH_MAX];

   dp = opendir (path);
   if (!dp) {
      perror (path);
      abort ();
   }

   if (skip_aws_test (test_name) && !selected) {
      printf ("SKIP: %s\n", test_name);
      goto done;
   }

   while ((ent = readdir (dp))) {
      if (ent->d_name[0] == '.') {
         continue;
      }

      if (ent->d_type & DT_DIR) {
         snprintf (sub, PATH_MAX, "%s/%s", path, ent->d_name);
         ran_tests |= all_aws_sig_v4_tests (sub, selected);
      }

      if (!(ent->d_type & DT_REG) || !ends_with (ent->d_name, ".req")) {
         continue;
      }

      /* "ent" is a "test.req" request file, this is a test directory */
      /* skip the test if it doesn't match the name passed to us */
      if (selected && 0 != strcmp (test_name, selected)) {
         continue;
      }

      printf ("%s\n", path);
      aws_sig_v4_test (path);
      ran_tests = true;
   }

done:
   (void) closedir (dp);
   free (test_name);

   return ran_tests;
}

/* docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html */
void
example_signature_test (void)
{
   const char *expect =
      "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9";
   kms_request_t *request;
   unsigned char signing[32];
   char *sig;

   request = kms_request_new ("GET", "uri", NULL);
   set_test_date (request);

   kms_request_set_region (request, "us-east-1");
   kms_request_set_service (request, "iam");
   kms_request_set_secret_key (request,
                               "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

   KMS_ASSERT (kms_request_get_signing_key (request, signing));
   sig = hexlify (signing, 32);
   ASSERT_CMPSTR (expect, sig);
   free (sig);
   kms_request_destroy (request);
}

void
path_normalization_test (void)
{
   const char *tests[][2] = {
      {"", "/"},
      {"/", "/"},
      {"/..", "/"},
      {"./..", "/"},
      {"../..", "/"},
      {"/../..", "/"},
      {"a", "a"},
      {"a/", "a/"},
      {"a//", "a/"},
      {"a///", "a/"},
      {"/a", "/a"},
      {"//a", "/a"},
      {"///a", "/a"},
      {"/a/", "/a/"},
      {"/a/..", "/"},
      {"/a/../..", "/"},
      {"/a/b/../..", "/"},
      {"/a/b/c/../..", "/a"},
      {"/a/b/../../d", "/d"},
      {"/a/b/c/../../d", "/a/d"},
      {"/a/b", "/a/b"},
      {"a/..", "/"},
      {"a/../..", "/"},
      {"a/b/../..", "/"},
      {"a/b/c/../..", "a"},
      {"a/b/../../d", "d"},
      {"a/b/c/../../d", "a/d"},
      {"a/b", "a/b"},
      {"/a//b", "/a/b"},
      {"/a///b", "/a/b"},
      {"/a////b", "/a/b"},
      {"//", "/"},
      {"//a///", "/a/"},
   };

   const char **test;
   const char *out;
   size_t i;
   kms_request_str_t *in, *norm;

   for (i = 0; i < sizeof (tests) / (2 * sizeof (const char *)); i++) {
      test = tests[i];
      in = kms_request_str_new_from_chars (test[0], -1);
      out = test[1];
      norm = kms_request_str_path_normalized (in);
      ASSERT_CMPSTR (out, norm->str);
      kms_request_str_destroy (in);
      kms_request_str_destroy (norm);
   }
}

kms_request_t *
make_test_request (void)
{
   kms_request_t *request = kms_request_new ("POST", "/", NULL);

   kms_request_set_region (request, "foo-region");
   kms_request_set_service (request, "foo-service");
   kms_request_set_access_key_id (request, "foo-akid");
   kms_request_set_secret_key (request, "foo-key");
   set_test_date (request);

   return request;
}

void
host_test (void)
{
   kms_request_t *request = make_test_request ();
   test_compare_sreq (request, "test/host");
   kms_request_destroy (request);
}

void
content_length_test (void)
{
   const char *payload = "foo-payload";
   kms_request_t *request = make_test_request ();
   KMS_ASSERT (kms_request_append_payload (request, payload, strlen (payload)));
   test_compare_sreq (request, "test/content_length");
   kms_request_destroy (request);
}

void
bad_query_test (void)
{
   kms_request_t *request = kms_request_new ("GET", "/?asdf", NULL);
   ASSERT_CONTAINS (kms_request_get_error (request), "Cannot parse");
   kms_request_destroy (request);
}

void
append_header_field_value_test (void)
{
   kms_request_t *request = kms_request_new ("GET", "/", NULL);
   KMS_ASSERT (kms_request_add_header_field (request, "a", "b"));
   KMS_ASSERT (kms_request_append_header_field_value (request, "asdf", 4));
   /* header field 0 is "X-Amz-Date", field 1 is "a" */
   ASSERT_CMPSTR (request->header_fields->kvs[1].value->str, "basdf");
   kms_request_destroy (request);
}

void
set_date_test (void)
{
// Windows CRT asserts on this negative test because it is a negative test
// so it is skipped on Windows.
#ifndef _WIN32
   struct tm tm = {0};
   kms_request_t *request = kms_request_new ("GET", "/", NULL);

   tm.tm_sec = 9999; /* invalid, shouldn't be > 60 */
   KMS_ASSERT (!kms_request_set_date (request, &tm));
   ASSERT_CONTAINS (kms_request_get_error (request), "Invalid tm struct");
   kms_request_destroy (request);
#endif
}

void
multibyte_test (void)
{
/* euro currency symbol */
#define EU "\xe2\x82\xac"

   kms_request_t *request = kms_request_new ("GET", "/" EU "/?euro=" EU, NULL);

   set_test_date (request);
   KMS_ASSERT (kms_request_set_region (request, EU));
   KMS_ASSERT (kms_request_set_service (request, EU));
   kms_request_set_access_key_id (request, "AKIDEXAMPLE");
   kms_request_set_secret_key (request,
                               "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

   KMS_ASSERT (kms_request_add_header_field (request, EU, EU));
   KMS_ASSERT (kms_request_append_header_field_value (request, "asdf" EU, 7));
   KMS_ASSERT (kms_request_append_payload (request, EU, sizeof (EU)));
   /* header field 0 is "X-Amz-Date" */
   ASSERT_CMPSTR (request->header_fields->kvs[1].value->str, EU "asdf" EU);

   test_compare_creq (request, "test/multibyte");
   test_compare_sreq (request, "test/multibyte");

   kms_request_destroy (request);

#undef EU
}

void
connection_close_test (void)
{
   kms_request_opt_t *opt;
   kms_request_t *request;

   opt = kms_request_opt_new ();
   kms_request_opt_set_connection_close (opt, true);

   request = kms_request_new ("POST", "/", opt);
   kms_request_set_region (request, "foo-region");
   kms_request_set_service (request, "foo-service");
   kms_request_set_access_key_id (request, "foo-akid");
   kms_request_set_secret_key (request, "foo-key");
   set_test_date (request);

   test_compare_sreq (request, "test/connection_close");
   kms_request_opt_destroy (opt);
   kms_request_destroy (request);
}

/* the ciphertext blob from a response to an "Encrypt" API call */
const char ciphertext_blob[] =
   "\x01\x02\x02\x00\x78\xf3\x8e\xd8\xd4\xc6\xba\xfb\xa1\xcf\xc1\x1e\x68\xf2"
   "\xa1\x91\x9e\x36\x4d\x74\xa2\xc4\x9e\x30\x67\x08\x53\x33\x0d\xcd\xe0\xc9"
   "\x1b\x01\x60\x30\xd4\x73\x9e\x90\x1f\xa7\x43\x55\x84\x26\xf9\xd5\xf0\xb1"
   "\x00\x00\x00\x64\x30\x62\x06\x09\x2a\x86\x48\x86\xf7\x0d\x01\x07\x06\xa0"
   "\x55\x30\x53\x02\x01\x00\x30\x4e\x06\x09\x2a\x86\x48\x86\xf7\x0d\x01\x07"
   "\x01\x30\x1e\x06\x09\x60\x86\x48\x01\x65\x03\x04\x01\x2e\x30\x11\x04\x0c"
   "\xa2\xc7\x12\x1c\x25\x38\x0e\xec\x08\x1f\x23\x09\x02\x01\x10\x80\x21\x61"
   "\x03\xcd\xcb\xe2\xac\x36\x4f\x73\xdb\x1b\x73\x2e\x33\xda\x45\x51\xf4\xcd"
   "\xc0\xff\xd2\xe1\xb9\xc4\xc2\x0e\xbf\x53\x90\x46\x18\x42";

void
decrypt_request_test (void)
{
   kms_request_t *request = kms_decrypt_request_new (
      (uint8_t *) ciphertext_blob, sizeof (ciphertext_blob) - 1, NULL);

   set_test_date (request);
   kms_request_set_region (request, "us-east-1");
   kms_request_set_service (request, "service");
   kms_request_set_access_key_id (request, "AKIDEXAMPLE");
   kms_request_set_secret_key (request,
                               "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

   test_compare_creq (request, "test/decrypt");
   test_compare_sreq (request, "test/decrypt");

   kms_request_destroy (request);
}

void
encrypt_request_test (void)
{
   char *plaintext = "foobar";
   kms_request_t *request = kms_encrypt_request_new (
      (uint8_t *) plaintext, strlen (plaintext), "alias/1", NULL);

   set_test_date (request);
   kms_request_set_region (request, "us-east-1");
   kms_request_set_service (request, "service");
   kms_request_set_access_key_id (request, "AKIDEXAMPLE");
   kms_request_set_secret_key (request,
                               "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

   test_compare_creq (request, "test/encrypt");
   test_compare_sreq (request, "test/encrypt");

   kms_request_destroy (request);
}

void
kv_list_del_test (void)
{
   kms_kv_list_t *lst = kms_kv_list_new ();
   kms_request_str_t *k = kms_request_str_new_from_chars ("one", -1);
   kms_request_str_t *v = kms_request_str_new_from_chars ("v", -1);
   kms_kv_list_add (lst, k, v);
   kms_request_str_set_chars (k, "two", -1);
   kms_kv_list_add (lst, k, v);
   kms_request_str_set_chars (k, "three", -1);
   kms_kv_list_add (lst, k, v);
   kms_request_str_set_chars (k, "two", -1); /* dupe */
   kms_kv_list_add (lst, k, v);
   kms_request_str_set_chars (k, "four", -1);
   kms_kv_list_add (lst, k, v);
   KMS_ASSERT (lst->len == 5);
   kms_kv_list_del (lst, "two"); /* delete both "two" keys */
   KMS_ASSERT (lst->len == 3);
   ASSERT_CMPSTR (lst->kvs[0].key->str, "one");
   ASSERT_CMPSTR (lst->kvs[1].key->str, "three");
   ASSERT_CMPSTR (lst->kvs[2].key->str, "four");

   kms_request_str_destroy (k);
   kms_request_str_destroy (v);
   kms_kv_list_destroy (lst);
}

void
b64_test (void)
{
   uint8_t *expected = (uint8_t *) "\x01\x02\x03\x04";
   char encoded[9];
   int r;
   uint8_t data[5];

   r = kms_message_b64_ntop (expected, 4, encoded, 9);
   KMS_ASSERT (r == 8);
   ASSERT_CMPSTR (encoded, "AQIDBA==");
   r = kms_message_b64_pton (encoded, data, 5); /* +1 for terminator */
   KMS_ASSERT (r == 4);
   KMS_ASSERT (0 == memcmp (expected, data, 4));
}

void
b64_b64url_test (void)
{
   char base64_data[64];
   char base64url_data[64];
   int ret;

   memset (base64_data, 0, sizeof (base64_data));
   memset (base64url_data, 0, sizeof (base64url_data));
   strcpy (base64_data, "PDw/Pz8+Pg==");
   ret = kms_message_b64_to_b64url (base64_data,
                                    strlen (base64_data),
                                    base64url_data,
                                    sizeof (base64url_data));
   ASSERT (ret == 12);
   ASSERT_CMPSTR (base64url_data, "PDw_Pz8-Pg==");

   memset (base64_data, 0, sizeof (base64_data));
   ret = kms_message_b64url_to_b64 (base64url_data,
                                    strlen (base64url_data),
                                    base64_data,
                                    sizeof (base64_data));
   ASSERT (ret == 12);
   ASSERT_CMPSTR (base64_data, "PDw/Pz8+Pg==");

   /* Convert to base64url in-place. */
   ret = kms_message_b64_to_b64url (
      base64_data, strlen (base64_data), base64_data, sizeof (base64_data));
   ASSERT (ret == 12);
   ASSERT_CMPSTR (base64_data, "PDw_Pz8-Pg==");
}

void
kms_response_parser_test (void)
{
   kms_response_parser_t *parser = kms_response_parser_new ();
   kms_response_t *response;

   /* the parser resets after returning a response. */
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "Content-Length: 15\r\n", 20));
   ASSERT (kms_response_parser_feed (parser, (uint8_t *) "\r\n", 2));
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "This is a test.", 15));
   ASSERT (0 == kms_response_parser_wants_bytes (parser, 123));
   response = kms_response_parser_get_response (parser);
   ASSERT (response->status == 200)
   ASSERT_CMPSTR (response->body->str, "This is a test.");

   kms_response_destroy (response);
   kms_response_parser_destroy (parser);

   /* We fail to parse invalid HTTP */
   parser = kms_response_parser_new ();
   ASSERT (!kms_response_parser_feed (
      parser, (uint8_t *) "To Whom it May Concern\r\n", 24));
   kms_response_parser_destroy (parser);

   /* We fail on HTTP other than 1.1 */
   parser = kms_response_parser_new ();
   ASSERT (!kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/6.1 200 OK\r\n", 17));
   kms_response_parser_destroy (parser);

   /* We fail if there is no status */
   parser = kms_response_parser_new ();
   ASSERT (!kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 CREATED\r\n", 18));
   kms_response_parser_destroy (parser);

   /* We do not fail when parsing a non-200 status code,
    * as the content may provide a useful error message. */
   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 100 CONTINUE\r\n", 23));
   ASSERT (kms_response_parser_status (parser) == 100);
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 201 CREATED\r\n", 22));
   ASSERT (kms_response_parser_status (parser) == 201);
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 301 MOVED PERMANENTLY\r\n", 32));
   ASSERT (kms_response_parser_status (parser) == 301);
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 400 BAD REQUEST\r\n", 26));
   ASSERT (kms_response_parser_status (parser) == 400);
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 404 NOT FOUND\r\n", 24));
   ASSERT (kms_response_parser_status (parser) == 404);
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "HTTP/1.1 500 INTERNAL SERVER ERROR\r\n", 36));
   ASSERT (kms_response_parser_status (parser) == 500);
   kms_response_parser_destroy (parser);

   /* We fail if the header doesn't have a colon in it */
   parser = kms_response_parser_new ();
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_status (parser) == 200);
   ASSERT (!kms_response_parser_feed (
      parser, (uint8_t *) "Content-Length= 15\r\n", 20));
   ASSERT (strstr (kms_response_parser_error (parser),
                   "Could not parse header, no colon found."));
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_status (parser) == 200);
   ASSERT (
      !kms_response_parser_feed (parser, (uint8_t *) "Anything else\r\n", 15));
   ASSERT (strstr (kms_response_parser_error (parser),
                   "Could not parse header, no colon found."));
   kms_response_parser_destroy (parser);

   /* An empty body is ok. */
   parser = kms_response_parser_new ();
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_status (parser) == 200);
   ASSERT (kms_response_parser_feed (parser, (uint8_t *) "\r\n", 2));
   kms_response_parser_destroy (parser);

   /* Extra content is not ok. */
   parser = kms_response_parser_new ();
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_status (parser) == 200);
   ASSERT (kms_response_parser_feed (parser, (uint8_t *) "\r\n", 2));
   ASSERT (!kms_response_parser_feed (parser, (uint8_t *) "\r\n", 2));
   ASSERT (strstr (kms_response_parser_error (parser),
                   "Unexpected extra HTTP content"));
   kms_response_parser_destroy (parser);

   parser = kms_response_parser_new ();
   ASSERT (
      kms_response_parser_feed (parser, (uint8_t *) "HTTP/1.1 200 OK\r\n", 17));
   ASSERT (kms_response_parser_status (parser) == 200);
   ASSERT (kms_response_parser_feed (
      parser, (uint8_t *) "Content-Length: 5\r\n", 19));
   ASSERT (kms_response_parser_feed (parser, (uint8_t *) "\r\n", 2));
   ASSERT (!kms_response_parser_feed (parser, (uint8_t *) "abcdefghi", 9));
   ASSERT (strstr (kms_response_parser_error (parser),
                   "Unexpected: exceeded content length"));
   kms_response_parser_destroy (parser);
}

typedef struct {
   const char *filepath;
   const char *expected_body;
   int max_to_read;
   int expected_status;
} parser_testcase_t;

/* File should have \r\n line endings (use /etc/rewrite.py) */
static void
parser_testcase_run (parser_testcase_t *testcase)
{
   FILE *response_file;
   kms_response_parser_t *parser;
   kms_response_t *response;
   uint8_t buf[512] = {0};
   int bytes_to_read;

   response_file = fopen (testcase->filepath, "rb");
   ASSERT (response_file);

   parser = kms_response_parser_new ();

   while ((bytes_to_read = kms_response_parser_wants_bytes (
              parser, testcase->max_to_read)) > 0) {
      if (bytes_to_read > testcase->max_to_read) {
         bytes_to_read = testcase->max_to_read;
      }
      size_t ret = fread (buf, 1, (size_t) bytes_to_read, response_file);

      if (!kms_response_parser_feed (parser, buf, (int) ret)) {
         printf ("feed error: %s\n", parser->error);
         ASSERT (false);
      }
   }

   fclose (response_file);

   ASSERT (0 == kms_response_parser_wants_bytes (parser, 123));
   response = kms_response_parser_get_response (parser);
   ASSERT_CMPSTR (testcase->expected_body, response->body->str);
   ASSERT (response->status == testcase->expected_status);

   kms_response_parser_destroy (parser);
   kms_response_destroy (response);
}

static void
kms_response_parser_files (void)
{
   const char *body = "{\"CiphertextBlob\":\"AQICAHifzrL6n/"
                      "3uqZyz+z1bJj80DhqPcSAibAaIoYc+HOVP6QEplwbM0wpvU5zsQG/"
                      "1SBKvAAAAZDBiBgkqhkiG9w0BBwagVTBTAgEAME4GCSqGSIb3DQEHATA"
                      "eBglghkgBZQMEAS4wEQQM5syMJE7RodxDaqYqAgEQgCHMFCnFso4Lih0"
                      "CNbLT1kiET0hQyzjgoa9733353GQkGlM=\",\"KeyId\":\"arn:aws:"
                      "kms:us-east-1:524754917239:key/"
                      "bd05530b-0a7f-4fbd-8362-ab3667370db0\"}";
   const char *chunked_body =
      "{\"access_token\":"
      "\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\","
      "\"expires_in\":3599,\"token_type\":\"Bearer\"}";
   parser_testcase_t tests[] = {
      {"./test/example-response.bin", body, 512, 200},
      {"./test/example-response.bin", body, 1, 200},
      {"./test/example-chunked-response.bin", chunked_body, 512, 200},
      {"./test/example-chunked-response.bin", chunked_body, 1, 200},
      {"./test/example-multi-chunked-response.bin", chunked_body, 512, 200},
      {"./test/example-multi-chunked-response.bin", chunked_body, 1, 200}};
   size_t i;

   for (i = 0; i < sizeof (tests) / sizeof (tests[0]); i++) {
      printf (" parser testcase: %d\n", (int) i);
      parser_testcase_run (tests + i);
   }
}

#define CLEAR(_field)                   \
   do {                                 \
      kms_request_str_destroy (_field); \
      _field = kms_request_str_new ();  \
   } while (0)

void
kms_request_validate_test (void)
{
   kms_request_t *request = NULL;

   request = make_test_request ();
   CLEAR (request->region);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Region not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->service);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Service not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->access_key_id);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Access key ID not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->method);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Method not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->path);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Path not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->date);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Date not set", kms_request_get_error (request));

   kms_request_destroy (request);

   request = make_test_request ();
   CLEAR (request->secret_key);
   ASSERT (NULL == kms_request_get_signed (request));
   ASSERT_CMPSTR ("Secret key not set", kms_request_get_error (request));

   kms_request_destroy (request);
}

/* Test private key signing.
 *
 * private_key_b64 was generated by taking the base64 data out of
 * test-private-key.pem generated as follows: openssl genpkey -out
 * test-private-key.pem -algorithm RSA -pkeyopt rsa_keygen_bits:2048
 *
 * It was used to generate a test signature of the string "data to sign" with:
 * openssl dgst -sign test-private-key.pem -sha256 signme.txt | base64
 */
static void
kms_signature_test (void)
{
   const char *private_key_b64 =
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC4JOyv5z05cL18ztpknRC7"
      "CFY2gYol4DAKerdVUoDJxCTmFMf39dVUEqD0WDiw/qcRtSO1/"
      "FRut08PlSPmvbyKetsLoxlpS8lukSzEFpFK7+L+R4miFOl6HvECyg7lbC1H/"
      "WGAhIz9yZRlXhRo9qmO/"
      "fB6PV9IeYtU+"
      "1xYuXicjCDPp36uuxBAnCz7JfvxJ3mdVc0vpSkbSb141nWuKNYR1mgyvvL6KzxO6mYsCo4hR"
      "AdhuizD9C4jDHk0V2gDCFBk0h8SLEdzStX8L0jG90/Og4y7J1b/cPo/"
      "kbYokkYisxe8cPlsvGBf+rZex7XPxc1yWaP080qeABJb+S88O//"
      "LAgMBAAECggEBAKVxP1m3FzHBUe2NZ3fYCc0Qa2zjK7xl1KPFp2u4CU+"
      "9sy0oZJUqQHUdm5CMprqWwIHPTftWboFenmCwrSXFOFzujljBO7Z3yc1WD3NJl1ZNepLcsRJ"
      "3WWFH5V+NLJ8Bdxlj1DMEZCwr7PC5+vpnCuYWzvT0qOPTl9RNVaW9VVjHouJ9Fg+"
      "s2DrShXDegFabl1iZEDdI4xScHoYBob06A5lw0WOCTayzw0Naf37lM8Y4psRAmI46XLiF/"
      "Vbuorna4hcChxDePlNLEfMipICcuxTcei1RBSlBa2t1tcnvoTy6cuYDqqImRYjp1KnMKlKQB"
      "nQ1NjS2TsRGm+F0FbreVCECgYEA4IDJlm8q/hVyNcPe4OzIcL1rsdYN3bNm2Y2O/"
      "YtRPIkQ446ItyxD06d9VuXsQpFp9jNACAPfCMSyHpPApqlxdc8z/"
      "xATlgHkcGezEOd1r4E7NdTpGg8y6Rj9b8kVlED6v4grbRhKcU6moyKUQT3+"
      "1B6ENZTOKyxuyDEgTwZHtFECgYEA0fqdv9h9s77d6eWmIioP7FSymq93pC4umxf6TVicpjpM"
      "ErdD2ZfJGulN37dq8FOsOFnSmFYJdICj/PbJm6p1i8O21lsFCltEqVoVabJ7/"
      "0alPfdG2U76OeBqI8ZubL4BMnWXAB/"
      "VVEYbyWCNpQSDTjHQYs54qa2I0dJB7OgJt1sCgYEArctFQ02/"
      "7H5Rscl1yo3DBXO94SeiCFSPdC8f2Kt3MfOxvVdkAtkjkMACSbkoUsgbTVqTYSEOEc2jTgR3"
      "iQ13JgpHaFbbsq64V0QP3TAxbLIQUjYGVgQaF1UfLOBv8hrzgj45z/ST/"
      "G80lOl595+0nCUbmBcgG1AEWrmdF0/"
      "3RmECgYAKvIzKXXB3+19vcT2ga5Qq2l3TiPtOGsppRb2XrNs9qKdxIYvHmXo/"
      "9QP1V3SRW0XoD7ez8FpFabp42cmPOxUNk3FK3paQZABLxH5pzCWI9PzIAVfPDrm+"
      "sdnbgG7vAnwfL2IMMJSA3aDYGCbF9EgefG+"
      "STcpfqq7fQ6f5TBgLFwKBgCd7gn1xYL696SaKVSm7VngpXlczHVEpz3kStWR5gfzriPBxXgM"
      "VcWmcbajRser7ARpCEfbxM1UJyv6oAYZWVSNErNzNVb4POqLYcCNySuC6xKhs9FrEQnyKjyk"
      "8wI4VnrEMGrQ8e+qYSwYk9Gh6dKGoRMAPYVXQAO0fIsHF/T0a";
   const char *data_to_sign = "data to sign";
   const char *expected_signature =
      "VocBRhpMmQ2XCzVehWSqheQLnU889gf3dhU4AnVnQTJjsKx/CM23qKDPkZDd2A/"
      "BnQsp99SN7ksIX5Raj0TPwyN5OCN/YrNFNGoOFlTsGhgP/"
      "hyE8X3Duiq6sNO0SMvRYNPFFGlJFsp1Fw3Z94eYMg4/Wpw5s4+Jo5Zm/"
      "qY7aTJIqDKDQ3CNHLeJgcMUOc9sz01/"
      "GzoUYKDVODHSxrYEk5ireFJFz9vP8P7Ha+"
      "VDUZuQIQdXer9NBbGFtYmWprY3nn4D3Dw93Sn0V0dIqYeIo91oKyslvMebmUM95S2PyIJdEp"
      "Pb2DJDxjvX/0LLwSWlSXRWy9gapWoBkb4ynqZBsg==";
   uint8_t *private_key_raw;
   size_t private_key_len;
   unsigned char *signature_raw;
   bool ret;
   char *signature_b64;

   private_key_raw = kms_message_b64_to_raw (private_key_b64, &private_key_len);
   signature_raw = malloc (256);

   ret = kms_sign_rsaes_pkcs1_v1_5 (NULL /* unused ctx */,
                                    (const char *) private_key_raw,
                                    private_key_len,
                                    data_to_sign,
                                    strlen (data_to_sign),
                                    signature_raw);
   KMS_ASSERT (ret);
   signature_b64 = kms_message_raw_to_b64 (signature_raw, 256);

   if (0 != strcmp (signature_b64, expected_signature)) {
      printf ("generated signature: %s\n", signature_b64);
      printf ("but expected signature: %s\n", expected_signature);
      abort ();
   }

   /* Test with an invalid key. */
   ret = kms_sign_rsaes_pkcs1_v1_5 (
      NULL, "blah", 4, data_to_sign, strlen (data_to_sign), signature_raw);

   free (private_key_raw);
   free (signature_raw);
   free (signature_b64);

   KMS_ASSERT (!ret);
}

static void
kms_request_kmip_prohibited_test (void)
{
   kms_request_opt_t *opt;
   kms_request_t *req;

   opt = kms_request_opt_new ();
   kms_request_opt_set_provider (opt, KMS_REQUEST_PROVIDER_KMIP);
   req = kms_request_new ("method", "path_and_query", opt);
   ASSERT_REQUEST_ERROR (req, "Function not applicable to KMIP");
   kms_request_destroy (req);
   kms_request_opt_destroy (opt);
}

static int
count_substrings (const char *big, const char *little)
{
   char *iter;
   int count = 0;

   iter = strstr (big, little);
   while (iter != NULL) {
      count += 1;
      iter += strlen (little);
      iter = strstr (iter, little);
   }
   return count;
}

/* Test that outgoing HTTP requests use \r\n line delimitters, not \n.
 * This is a regression test for MONGOCRYPT-457. */
static void
test_request_newlines (void)
{
   bool ok;
   kms_request_t *req;
   kms_request_opt_t *opt;
   uint8_t example_data[] = {0x01, 0x02, 0x03, 0x04};

   // Test kms_request_to_string.
   {
      opt = kms_request_opt_new ();
      kms_request_opt_set_connection_close (opt, true);
      ASSERT (kms_request_opt_set_provider (opt, KMS_REQUEST_PROVIDER_AZURE));
      req = kms_azure_request_wrapkey_new ("example-host",
                                           "example-access-token",
                                           "example-key-name",
                                           "example-key-version",
                                           example_data,
                                           sizeof (example_data),
                                           opt);
      ASSERT_REQUEST_OK (req);
      char *req_str = kms_request_to_string (req);
      ASSERT_REQUEST_OK (req);
      ASSERT (req_str);
      /* Check that all \n have a \r. */
      int n = count_substrings (req_str, "\n");
      int rn = count_substrings (req_str, "\r\n");
      ASSERT_CMPINT (n, ==, rn);
      ASSERT_CMPINT (rn, ==, 8);
      free (req_str);
      kms_request_opt_destroy (opt);
      kms_request_destroy (req);
   }

   // Test kms_request_get_signed.
   {
      opt = kms_request_opt_new ();
      kms_request_opt_set_connection_close (opt, true);
      req = kms_caller_identity_request_new (opt);
      ASSERT_REQUEST_OK (req);
      ok = kms_request_set_region (req, "example-region");
      ASSERT_REQUEST_OK (req);
      ASSERT (ok);
      ok = kms_request_set_service (req, "example-service");
      ASSERT_REQUEST_OK (req);
      ASSERT (ok);
      ok = kms_request_set_access_key_id (req, "example-access-key-id");
      ASSERT_REQUEST_OK (req);
      ASSERT (ok);
      ok = kms_request_set_secret_key (req, "example-secret-key");
      ASSERT_REQUEST_OK (req);
      ASSERT (ok);
      char *req_str = kms_request_get_signed (req);
      ASSERT_REQUEST_OK (req);
      ASSERT (req_str);
      /* Check that all \n have a \r. */
      int n = count_substrings (req_str, "\n");
      int rn = count_substrings (req_str, "\r\n");
      ASSERT_CMPINT (n, ==, rn);
      ASSERT_CMPINT (rn, ==, 8);
      free (req_str);
      kms_request_opt_destroy (opt);
      kms_request_destroy (req);
   }
}

#define RUN_TEST(_func)                                          \
   do {                                                          \
      if (!selector || 0 == kms_strcasecmp (#_func, selector)) { \
         printf ("%s\n", #_func);                                \
         _func ();                                               \
         ran_tests = true;                                       \
      }                                                          \
   } while (0)

extern void kms_kmip_writer_test (void);
extern void kms_kmip_reader_test (void);
extern void kms_kmip_reader_negative_int_test (void);
extern void kms_kmip_reader_find_test (void);
extern void kms_kmip_reader_find_and_recurse_test (void);
extern void kms_kmip_reader_find_and_read_enum_test (void);
extern void kms_kmip_reader_find_and_read_bytes_test (void);
extern void kms_kmip_request_register_secretdata_test (void);
extern void kms_kmip_request_register_secretdata_invalid_test (void);
extern void kms_kmip_request_get_test (void);
extern void kms_kmip_request_activate_test (void);
extern void kms_kmip_response_parser_test (void);
extern void kms_kmip_response_get_unique_identifier_test (void);
extern void kms_kmip_response_get_secretdata_test (void);
extern void kms_kmip_response_get_secretdata_notfound_test (void);
extern void kms_kmip_response_parser_reuse_test (void);
extern void kms_kmip_response_parser_excess_test (void);
extern void kms_kmip_response_parser_notenough_test (void);

int
main (int argc, char *argv[])
{
   const char *help;
   char *selector = NULL;
   bool ran_tests = false;

   help = "Usage: test_kms_request [TEST_NAME]";

   if (argc > 2) {
      fprintf (stderr, "%s\n", help);
      abort ();
   } else if (argc == 2) {
      selector = argv[1];
   }

   int ret = kms_message_init ();
   if (ret != 0) {
      printf ("kms_message_init failed: 0x%x\n", ret);
      abort ();
   }

   RUN_TEST (example_signature_test);
   RUN_TEST (path_normalization_test);
   RUN_TEST (host_test);
   RUN_TEST (content_length_test);
   RUN_TEST (bad_query_test);
   RUN_TEST (append_header_field_value_test);
   RUN_TEST (set_date_test);
   RUN_TEST (multibyte_test);
   RUN_TEST (connection_close_test);
   RUN_TEST (decrypt_request_test);
   RUN_TEST (encrypt_request_test);
   RUN_TEST (kv_list_del_test);
   RUN_TEST (b64_test);
   RUN_TEST (b64_b64url_test);

   ran_tests |= all_aws_sig_v4_tests (aws_test_suite_dir, selector);

   RUN_TEST (kms_response_parser_test);
   RUN_TEST (kms_response_parser_files);
   RUN_TEST (kms_request_validate_test);

   RUN_TEST (kms_signature_test);

   RUN_TEST (kms_kmip_writer_test);
   RUN_TEST (kms_kmip_reader_test);
   RUN_TEST (kms_kmip_reader_negative_int_test);
   RUN_TEST (kms_kmip_reader_test);
   RUN_TEST (kms_kmip_reader_find_and_recurse_test);
   RUN_TEST (kms_kmip_reader_find_and_read_enum_test);
   RUN_TEST (kms_kmip_reader_find_and_read_bytes_test);
   RUN_TEST (kms_kmip_request_register_secretdata_test);
   RUN_TEST (kms_kmip_request_register_secretdata_invalid_test);
   RUN_TEST (kms_kmip_request_get_test);
   RUN_TEST (kms_kmip_request_activate_test);
   RUN_TEST (kms_request_kmip_prohibited_test);
   RUN_TEST (kms_kmip_response_parser_test);
   RUN_TEST (kms_kmip_response_get_unique_identifier_test);
   RUN_TEST (kms_kmip_response_get_secretdata_test);
   RUN_TEST (kms_kmip_response_get_secretdata_notfound_test);
   RUN_TEST (kms_kmip_response_parser_reuse_test);
   RUN_TEST (kms_kmip_response_parser_excess_test);
   RUN_TEST (kms_kmip_response_parser_notenough_test);
   RUN_TEST (test_request_newlines);
   RUN_TEST (test_kms_util);


   if (!ran_tests) {
      KMS_ASSERT (argc == 2);
      fprintf (stderr, "No such test: \"%s\"\n", argv[1]);
      abort ();
   }

   kms_message_cleanup ();

   return 0;
}
