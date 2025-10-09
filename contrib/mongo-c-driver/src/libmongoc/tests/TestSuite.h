/*
 * Copyright 2014 MongoDB, Inc.
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


#ifndef TEST_SUITE_H
#define TEST_SUITE_H


#include <bson/bson.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>

#include "mongoc/mongoc-array-private.h"
#include "mongoc/mongoc-util-private.h"


#ifdef __cplusplus
extern "C" {
#endif

#ifndef OS_RELEASE_FILE_DIR
#define OS_RELEASE_FILE_DIR "src/libmongoc/tests/release_files"
#endif


#ifndef BINARY_DIR
#define BINARY_DIR "src/libmongoc/tests/binary"
#endif

#ifndef BSON_BINARY_DIR
#define BSON_BINARY_DIR "src/libbson/tests/binary"
#endif

#ifndef JSON_DIR
#define JSON_DIR "src/libmongoc/tests/json"
#endif

#ifndef BSON_JSON_DIR
#define BSON_JSON_DIR "src/libbson/tests/json"
#endif

#ifndef CERT_TEST_DIR
#define CERT_TEST_DIR "src/libmongoc/tests/x509gen"
#endif


#ifdef BSON_OS_WIN32
#include <stdarg.h>
#include <share.h>
static __inline int
bson_open (const char *filename, int flags, ...)
{
   int fd = -1;

   if (_sopen_s (&fd, filename, flags | _O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE) == NO_ERROR) {
      return fd;
   }

   return -1;
}
#define bson_close _close
#define bson_read(f, b, c) ((ssize_t) _read ((f), (b), (int) (c)))
#define bson_write _write
#else
#define bson_open open
#define bson_read read
#define bson_close close
#define bson_write write
#endif


#define TEST_NOFORK (1 << 1)
#define TEST_HELPTEXT (1 << 2)
#define TEST_DEBUGOUTPUT (1 << 3)
#define TEST_TRACE (1 << 4)
#define TEST_LISTTESTS (1 << 5)


#define CERT_CA CERT_TEST_DIR "/ca.pem"
#define CERT_CRL CERT_TEST_DIR "/crl.pem"
#define CERT_SERVER CERT_TEST_DIR "/server.pem" /* 127.0.0.1 & localhost */
#define CERT_CLIENT CERT_TEST_DIR "/client.pem"
#define CERT_ALTNAME                                                               \
   CERT_TEST_DIR "/altname.pem"                         /* alternative.mongodb.org \
                                                         */
#define CERT_WILD CERT_TEST_DIR "/wild.pem"             /* *.mongodb.org */
#define CERT_COMMONNAME CERT_TEST_DIR "/commonName.pem" /* 127.0.0.1 & localhost */
#define CERT_EXPIRED CERT_TEST_DIR "/expired.pem"       /* 127.0.0.1 & localhost */
#define CERT_PASSWORD "qwerty"
#define CERT_PASSWORD_PROTECTED CERT_TEST_DIR "/password_protected.pem"


#ifdef ASSERT
#undef ASSERT
#endif

// Ensure stdout and stderr are flushed prior to possible following abort().
#define MONGOC_STDERR_PRINTF(format, ...)    \
   if (1) {                                  \
      fflush (stdout);                       \
      fprintf (stderr, format, __VA_ARGS__); \
      fflush (stderr);                       \
   } else                                    \
      ((void) 0)

#define ASSERT(Cond)                                                                                         \
   do {                                                                                                      \
      if (!(Cond)) {                                                                                         \
         MONGOC_STDERR_PRINTF (                                                                              \
            "FAIL:%s:%d  %s()\n  Condition '%s' failed.\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (Cond)); \
         abort ();                                                                                           \
      }                                                                                                      \
   } while (0)


void
_test_error (const char *format, ...) BSON_GNUC_PRINTF (1, 2);

#define test_error(...)                                                                    \
   if (1) {                                                                                \
      MONGOC_STDERR_PRINTF ("test error in: %s %d:%s()\n", __FILE__, __LINE__, BSON_FUNC); \
      _test_error (__VA_ARGS__);                                                           \
      abort (); /* suppress missing return errors in non-void functions */                 \
   } else                                                                                  \
      ((void) 0)

#define bson_eq_bson(bson, expected)                                                                            \
   do {                                                                                                         \
      char *bson_json, *expected_json;                                                                          \
      const uint8_t *bson_data = bson_get_data ((bson));                                                        \
      const uint8_t *expected_data = bson_get_data ((expected));                                                \
      int unequal;                                                                                              \
      unsigned o;                                                                                               \
      int off = -1;                                                                                             \
      unequal = ((expected)->len != (bson)->len) ||                                                             \
                memcmp (bson_get_data ((expected)), bson_get_data ((bson)), (expected)->len);                   \
      if (unequal) {                                                                                            \
         bson_json = bson_as_canonical_extended_json (bson, NULL);                                              \
         expected_json = bson_as_canonical_extended_json ((expected), NULL);                                    \
         for (o = 0; o < (bson)->len && o < (expected)->len; o++) {                                             \
            if (bson_data[o] != expected_data[o]) {                                                             \
               off = o;                                                                                         \
               break;                                                                                           \
            }                                                                                                   \
         }                                                                                                      \
         if (off == -1) {                                                                                       \
            off = BSON_MAX ((expected)->len, (bson)->len) - 1;                                                  \
         }                                                                                                      \
         MONGOC_STDERR_PRINTF ("bson objects unequal (byte %u):\n(%s)\n(%s)\n", off, bson_json, expected_json); \
         {                                                                                                      \
            int fd1 = bson_open ("failure.bad.bson", O_RDWR | O_CREAT, 0640);                                   \
            int fd2 = bson_open ("failure.expected.bson", O_RDWR | O_CREAT, 0640);                              \
            ASSERT (fd1 != -1);                                                                                 \
            ASSERT (fd2 != -1);                                                                                 \
            ASSERT ((bson)->len == bson_write (fd1, bson_data, (bson)->len));                                   \
            ASSERT ((expected)->len == bson_write (fd2, expected_data, (expected)->len));                       \
            bson_close (fd1);                                                                                   \
            bson_close (fd2);                                                                                   \
         }                                                                                                      \
         abort ();                                                                                              \
      }                                                                                                         \
   } while (0)


#ifdef ASSERT_OR_PRINT
#undef ASSERT_OR_PRINT
#endif

#define ASSERT_OR_PRINT(_statement, _err)                                                                            \
   do {                                                                                                              \
      if (!(_statement)) {                                                                                           \
         MONGOC_STDERR_PRINTF (                                                                                      \
            "FAIL:%s:%d  %s()\n  %s\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (_statement), _err.message); \
         abort ();                                                                                                   \
      }                                                                                                              \
   } while (0)

#define ASSERT_CURSOR_NEXT(_cursor, _doc)                                                                       \
   do {                                                                                                         \
      bson_error_t _err;                                                                                        \
      if (!mongoc_cursor_next ((_cursor), (_doc))) {                                                            \
         if (mongoc_cursor_error ((_cursor), &_err)) {                                                          \
            MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, _err.message);   \
         } else {                                                                                               \
            MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, "empty cursor"); \
         }                                                                                                      \
         abort ();                                                                                              \
      }                                                                                                         \
   } while (0)


#define ASSERT_CURSOR_DONE(_cursor)                                                                              \
   do {                                                                                                          \
      bson_error_t _err;                                                                                         \
      const bson_t *_doc;                                                                                        \
      if (mongoc_cursor_next ((_cursor), &_doc)) {                                                               \
         MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, "non-empty cursor"); \
         abort ();                                                                                               \
      }                                                                                                          \
      if (mongoc_cursor_error ((_cursor), &_err)) {                                                              \
         MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, _err.message);       \
         abort ();                                                                                               \
      }                                                                                                          \
   } while (0)


#define ASSERT_CMPINT_HELPER(a, eq, b, fmt, type)                               \
   do {                                                                         \
      /* evaluate once */                                                       \
      type _a = a;                                                              \
      type _b = b;                                                              \
      if (!((_a) eq (_b))) {                                                    \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: %" fmt " %s %" fmt "\n" \
                               "%s:%d  %s()\n",                                 \
                               _a,                                              \
                               BSON_STR (eq),                                   \
                               _b,                                              \
                               __FILE__,                                        \
                               __LINE__,                                        \
                               BSON_FUNC);                                      \
         abort ();                                                              \
      }                                                                         \
   } while (0)


#define ASSERT_CMPINT(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "d", int)
#define ASSERT_CMPUINT(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "u", unsigned int)
#define ASSERT_CMPLONG(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "ld", long)
#define ASSERT_CMPULONG(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "lu", unsigned long)
#define ASSERT_CMPINT32(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, PRId32, int32_t)
#define ASSERT_CMPINT64(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, PRId64, int64_t)
#define ASSERT_CMPUINT16(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "hu", uint16_t)
#define ASSERT_CMPUINT32(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, PRIu32, uint32_t)
#define ASSERT_CMPUINT64(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, PRIu64, uint64_t)
#define ASSERT_CMPSIZE_T(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "zu", size_t)
#define ASSERT_CMPSSIZE_T(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "zd", ssize_t)
#define ASSERT_CMPDOUBLE(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "f", double)
#define ASSERT_CMPVOID(a, eq, b) ASSERT_CMPINT_HELPER (a, eq, b, "p", void *)

#define ASSERT_MEMCMP(a, b, n)                                                                                      \
   do {                                                                                                             \
      if (0 != memcmp (a, b, n)) {                                                                                  \
         MONGOC_STDERR_PRINTF ("Failed comparing %d bytes: \"%.*s\" != \"%.*s\"", n, n, (char *) a, n, (char *) b); \
         abort ();                                                                                                  \
      }                                                                                                             \
   } while (0)


#ifdef ASSERT_ALMOST_EQUAL
#undef ASSERT_ALMOST_EQUAL
#endif
#define ASSERT_ALMOST_EQUAL(a, b)                                                                     \
   do {                                                                                               \
      /* evaluate once */                                                                             \
      int64_t _a = (a);                                                                               \
      int64_t _b = (b);                                                                               \
      if (!(_a > (_b * 2) / 3 && (_a < (_b * 3) / 2))) {                                              \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: %" PRId64 " not within 50%% of %" PRId64 "\n" \
                               "%s:%d  %s()\n",                                                       \
                               _a,                                                                    \
                               _b,                                                                    \
                               __FILE__,                                                              \
                               __LINE__,                                                              \
                               BSON_FUNC);                                                            \
         abort ();                                                                                    \
      }                                                                                               \
   } while (0)

#ifdef ASSERT_EQUAL_DOUBLE
#undef ASSERT_EQUAL_DOUBLE
#endif
#define ASSERT_EQUAL_DOUBLE(a, b)                                                   \
   do {                                                                             \
      double _a = fabs ((double) a);                                                \
      double _b = fabs ((double) b);                                                \
      if (!(_a > (_b * 4) / 5 && (_a < (_b * 6) / 5))) {                            \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: %f not within 20%% of %f\n" \
                               "%s:%d  %s()\n",                                     \
                               (double) a,                                          \
                               (double) b,                                          \
                               __FILE__,                                            \
                               __LINE__,                                            \
                               BSON_FUNC);                                          \
         abort ();                                                                  \
      }                                                                             \
   } while (0)


#define ASSERT_CMPSTR(a, b)                                                                 \
   do {                                                                                     \
      /* evaluate once */                                                                   \
      const char *_a = a;                                                                   \
      const char *_b = b;                                                                   \
      if ((_a != _b) && (!_a || !_b || (strcmp (_a, _b) != 0))) {                           \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure:\n  \"%s\"\n  !=\n  \"%s\"\n %s:%d " \
                               " %s()\n",                                                   \
                               _a ? _a : "(null)",                                          \
                               _b ? _b : "(null)",                                          \
                               __FILE__,                                                    \
                               __LINE__,                                                    \
                               BSON_FUNC);                                                  \
         abort ();                                                                          \
      }                                                                                     \
   } while (0)

#define ASSERT_CMPJSON(_a, _b)                                              \
   do {                                                                     \
      size_t i = 0;                                                         \
      char *__a = (char *) (_a);                                            \
      char *__b = (char *) (_b);                                            \
      char *__aa = bson_malloc0 (strlen (__a) + 1);                         \
      char *__bb = bson_malloc0 (strlen (__b) + 1);                         \
      char *f = __a;                                                        \
      do {                                                                  \
         while (bson_isspace (*__a))                                        \
            __a++;                                                          \
         __aa[i++] = *__a++;                                                \
      } while (*__a);                                                       \
      i = 0;                                                                \
      do {                                                                  \
         while (bson_isspace (*__b))                                        \
            __b++;                                                          \
         __bb[i++] = *__b++;                                                \
      } while (*__b);                                                       \
      if (!!strcmp ((__aa), (__bb))) {                                      \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: \"%s\" != \"%s\"\n" \
                               "%s:%d  %s()\n",                             \
                               __aa,                                        \
                               __bb,                                        \
                               __FILE__,                                    \
                               __LINE__,                                    \
                               BSON_FUNC);                                  \
         abort ();                                                          \
      }                                                                     \
      bson_free (__aa);                                                     \
      bson_free (__bb);                                                     \
      bson_free (f);                                                        \
   } while (0)

#define ASSERT_CMPOID(a, b)                                              \
   do {                                                                  \
      if (bson_oid_compare ((a), (b))) {                                 \
         char oid_a[25];                                                 \
         char oid_b[25];                                                 \
         bson_oid_to_string ((a), oid_a);                                \
         bson_oid_to_string ((b), oid_b);                                \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: "                \
                               "ObjectId(\"%s\") != ObjectId(\"%s\")\n", \
                               oid_a,                                    \
                               oid_b);                                   \
         abort ();                                                       \
      }                                                                  \
   } while (0)


#define ASSERT_CONTAINS(a, b)                                                                                    \
   do {                                                                                                          \
      char *_a_lower = bson_strdup (a);                                                                          \
      char *_b_lower = bson_strdup (b);                                                                          \
      mongoc_lowercase (_a_lower, _a_lower);                                                                     \
      mongoc_lowercase (_b_lower, _b_lower);                                                                     \
      if (NULL == strstr ((_a_lower), (_b_lower))) {                                                             \
         MONGOC_STDERR_PRINTF ("%s:%d %s(): [%s] does not contain [%s]\n", __FILE__, __LINE__, BSON_FUNC, a, b); \
         abort ();                                                                                               \
      }                                                                                                          \
      bson_free (_a_lower);                                                                                      \
      bson_free (_b_lower);                                                                                      \
   } while (0)

#define ASSERT_STARTSWITH(a, b)                                                                      \
   do {                                                                                              \
      /* evaluate once */                                                                            \
      const char *_a = a;                                                                            \
      const char *_b = b;                                                                            \
      if ((_a) != strstr ((_a), (_b))) {                                                             \
         MONGOC_STDERR_PRINTF (                                                                      \
            "%s:%d %s(): : [%s] does not start with [%s]\n", __FILE__, __LINE__, BSON_FUNC, _a, _b); \
         abort ();                                                                                   \
      }                                                                                              \
   } while (0)

#define ASSERT_ERROR_CONTAINS(error, _domain, _code, _message)                           \
   do {                                                                                  \
      if (error.domain != _domain) {                                                     \
         MONGOC_STDERR_PRINTF ("%s:%d %s(): error domain %d doesn't match expected %d\n" \
                               "error: \"%s\"",                                          \
                               __FILE__,                                                 \
                               __LINE__,                                                 \
                               BSON_FUNC,                                                \
                               error.domain,                                             \
                               _domain,                                                  \
                               error.message);                                           \
         abort ();                                                                       \
      };                                                                                 \
      if (error.code != _code) {                                                         \
         MONGOC_STDERR_PRINTF ("%s:%d %s(): error code %d doesn't match expected %d\n"   \
                               "error: \"%s\"",                                          \
                               __FILE__,                                                 \
                               __LINE__,                                                 \
                               BSON_FUNC,                                                \
                               error.code,                                               \
                               _code,                                                    \
                               error.message);                                           \
         abort ();                                                                       \
      };                                                                                 \
      ASSERT_CONTAINS (error.message, _message);                                         \
   } while (0)

#define ASSERT_CAPTURED_LOG(_info, _level, _msg)                                                       \
   do {                                                                                                \
      if (!has_captured_log (_level, _msg)) {                                                          \
         MONGOC_STDERR_PRINTF (                                                                        \
            "%s:%d %s(): testing %s didn't log \"%s\"\n", __FILE__, __LINE__, BSON_FUNC, _info, _msg); \
         print_captured_logs ("\t");                                                                   \
         abort ();                                                                                     \
      }                                                                                                \
   } while (0)

#define ASSERT_NO_CAPTURED_LOGS(_info)                                                                \
   do {                                                                                               \
      if (has_captured_logs ()) {                                                                     \
         MONGOC_STDERR_PRINTF (                                                                       \
            "%s:%d %s(): testing %s shouldn't have logged:\n", __FILE__, __LINE__, BSON_FUNC, _info); \
         print_captured_logs ("\t");                                                                  \
         abort ();                                                                                    \
      }                                                                                               \
   } while (0)

#define ASSERT_HAS_FIELD(_bson, _field)                                               \
   do {                                                                               \
      if (!bson_has_field ((_bson), (_field))) {                                      \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: No field \"%s\" in \"%s\"\n", \
                               (_field),                                              \
                               bson_as_canonical_extended_json (_bson, NULL));        \
         abort ();                                                                    \
      }                                                                               \
   } while (0)

#define ASSERT_HAS_NOT_FIELD(_bson, _field)                                                   \
   do {                                                                                       \
      if (bson_has_field ((_bson), (_field))) {                                               \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: Unexpected field \"%s\" in \"%s\"\n", \
                               (_field),                                                      \
                               bson_as_canonical_extended_json (_bson, NULL));                \
         abort ();                                                                            \
      }                                                                                       \
   } while (0)

#define ASSERT_CMPTIME(actual, maxduration)     \
   do {                                         \
      ASSERT_CMPINT64 (actual, <, maxduration); \
   } while (0)

#define ASSERT_WITHIN_TIME_INTERVAL(actual, minduration, maxduration) \
   do {                                                               \
      ASSERT_CMPINT64 (actual, >=, minduration);                      \
      ASSERT_CMPINT64 (actual, <, maxduration);                       \
   } while (0)

#if defined(_WIN32) && !defined(__MINGW32__)
#define gettestpid _getpid
#else
#ifdef __MINGW32__
#include <process.h>
#endif
#define gettestpid getpid
#endif

#define ASSERT_OR_PRINT_ERRNO(_statement, _errcode)                                             \
   do {                                                                                         \
      if (!(_statement)) {                                                                      \
         MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n  Failed with error code: %d (%s)\n\n", \
                               __FILE__,                                                        \
                               __LINE__,                                                        \
                               BSON_FUNC,                                                       \
                               BSON_STR (_statement),                                           \
                               _errcode,                                                        \
                               strerror (_errcode));                                            \
         abort ();                                                                              \
      }                                                                                         \
   } while (0)

#define ASSERT_COUNT(n, collection)                                                                              \
   do {                                                                                                          \
      int count = (int) mongoc_collection_count_documents (collection, tmp_bson ("{}"), NULL, NULL, NULL, NULL); \
      if ((n) != count) {                                                                                        \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: count of %s is %d, not %d\n"                             \
                               "%s:%d  %s()\n",                                                                  \
                               mongoc_collection_get_name (collection),                                          \
                               count,                                                                            \
                               n,                                                                                \
                               __FILE__,                                                                         \
                               __LINE__,                                                                         \
                               BSON_FUNC);                                                                       \
         abort ();                                                                                               \
      }                                                                                                          \
   } while (0)

#define ASSERT_CURSOR_COUNT(_n, _cursor)                                              \
   do {                                                                               \
      int _count = 0;                                                                 \
      const bson_t *_doc;                                                             \
      while (mongoc_cursor_next (_cursor, &_doc)) {                                   \
         _count++;                                                                    \
      }                                                                               \
      if ((_n) != _count) {                                                           \
         MONGOC_STDERR_PRINTF ("FAIL\n\nAssert Failure: cursor count is %d, not %d\n" \
                               "%s:%d  %s()\n",                                       \
                               _count,                                                \
                               _n,                                                    \
                               __FILE__,                                              \
                               __LINE__,                                              \
                               BSON_FUNC);                                            \
         abort ();                                                                    \
      }                                                                               \
   } while (0)

#define WAIT_UNTIL(_pred)                                              \
   do {                                                                \
      int64_t _start = bson_get_monotonic_time ();                     \
      while (!(_pred)) {                                               \
         if (bson_get_monotonic_time () - _start > 10 * 1000 * 1000) { \
            MONGOC_STDERR_PRINTF ("Predicate \"%s\" timed out\n"       \
                                  "   %s:%d  %s()\n",                  \
                                  BSON_STR (_pred),                    \
                                  __FILE__,                            \
                                  __LINE__,                            \
                                  BSON_FUNC);                          \
            abort ();                                                  \
         }                                                             \
         _mongoc_usleep (10 * 1000);                                   \
      }                                                                \
   } while (0)

#define ASSERT_WITH_MSG(_statement, ...)                                                                            \
   do {                                                                                                             \
      if (!(_statement)) {                                                                                          \
         MONGOC_STDERR_PRINTF ("FAIL:%s:%d  %s()\n  %s\n\n", __FILE__, __LINE__, BSON_FUNC, BSON_STR (_statement)); \
         fprintf (stderr, __VA_ARGS__);                                                                             \
         fprintf (stderr, "\n");                                                                                    \
         fflush (stderr);                                                                                           \
         abort ();                                                                                                  \
      }                                                                                                             \
   } while (0)

// bson_value_to_str returns a string representation of a BSON value.
// Example: { "$numberInt": "123" }
// The return string must be freed with bson_free.
char *
bson_value_to_str (const bson_value_t *val);

// bson_value_eq returns true if both bson_value_t contain equal values.
bool
bson_value_eq (const bson_value_t *a, const bson_value_t *b);

// ASSERT_BSONVALUE_EQ asserts that two const bson_value_t* contain equal
// values.
#define ASSERT_BSONVALUE_EQ(a, b)                                               \
   if (1) {                                                                     \
      if (!bson_value_eq ((a), (b))) {                                          \
         test_error ("%s != %s", bson_value_to_str (a), bson_value_to_str (b)); \
      }                                                                         \
   } else                                                                       \
      (void) 0


// `get_current_connection_count` returns the server reported connection count.
int32_t
get_current_connection_count (const char *host_and_port);

#define ASSERT_CONN_COUNT(host, expect)                           \
   if (1) {                                                       \
      int32_t _got = get_current_connection_count (host);         \
      if (_got != expect) {                                       \
         test_error ("Got unexpected connection count to %s:\n"   \
                     "  Expected %" PRId32 ", got %" PRId32 "\n", \
                     host,                                        \
                     expect,                                      \
                     _got);                                       \
      }                                                           \
   } else                                                         \
      (void) 0

#define ASSERT_EVENTUAL_CONN_COUNT(host, expect)                                   \
   if (1) {                                                                        \
      int64_t _start = bson_get_monotonic_time ();                                 \
      while (true) {                                                               \
         int32_t _got = get_current_connection_count (host);                       \
         if (_got == expect) {                                                     \
            break;                                                                 \
         }                                                                         \
         int64_t _now = bson_get_monotonic_time ();                                \
         if (_now - _start > 5 * 1000 * 1000 /* five seconds */) {                 \
            test_error ("Timed out waiting for expected connection count to %s:\n" \
                        "  Expected %" PRId32 ", got %" PRId32 "\n",               \
                        host,                                                      \
                        expect,                                                    \
                        _got);                                                     \
         }                                                                         \
      }                                                                            \
   } else                                                                          \
      (void) 0

#define MAX_TEST_NAME_LENGTH 500
#define MAX_TEST_CHECK_FUNCS 10


typedef void (*TestFunc) (void);
typedef void (*TestFuncWC) (void *);
typedef void (*TestFuncDtor) (void *);
typedef int (*CheckFunc) (void);
typedef struct _Test Test;
typedef struct _TestSuite TestSuite;
typedef struct _TestFnCtx TestFnCtx;
typedef struct _TestSkip TestSkip;


struct _Test {
   Test *next;
   char *name;
   TestFuncWC func;
   TestFuncDtor dtor;
   void *ctx;
   int exit_code;
   unsigned seed;
   CheckFunc checks[MAX_TEST_CHECK_FUNCS];
   size_t num_checks;
};


struct _TestSuite {
   char *prgname;
   char *name;
   mongoc_array_t match_patterns;
   char *ctest_run;
   Test *tests;
   FILE *outfile;
   int flags;
   int silent;
   bson_string_t *mock_server_log_buf;
   FILE *mock_server_log;
   mongoc_array_t failing_flaky_skips;
};


struct _TestFnCtx {
   TestFunc test_fn;
   TestFuncDtor dtor;
};


struct _TestSkip {
   char *test_name;
   char *subtest_desc;
   char *reason;
};


void
TestSuite_Init (TestSuite *suite, const char *name, int argc, char **argv);
void
TestSuite_Add (TestSuite *suite, const char *name, TestFunc func);
int
TestSuite_CheckLive (void);
void
TestSuite_AddLive (TestSuite *suite, const char *name, TestFunc func);
int
TestSuite_CheckMockServerAllowed (void);
void
_TestSuite_AddMockServerTest (TestSuite *suite, const char *name, TestFunc func, ...);
#define TestSuite_AddMockServerTest(_suite, _name, ...) _TestSuite_AddMockServerTest (_suite, _name, __VA_ARGS__, NULL)
void
TestSuite_AddWC (TestSuite *suite, const char *name, TestFuncWC func, TestFuncDtor dtor, void *ctx);
Test *
_V_TestSuite_AddFull (TestSuite *suite, const char *name, TestFuncWC func, TestFuncDtor dtor, void *ctx, va_list ap);
void
_TestSuite_AddFull (TestSuite *suite, const char *name, TestFuncWC func, TestFuncDtor dtor, void *ctx, ...);
void
_TestSuite_TestFnCtxDtor (void *ctx);
#define TestSuite_AddFull(_suite, _name, _func, _dtor, _ctx, ...) \
   _TestSuite_AddFull (_suite, _name, _func, _dtor, _ctx, __VA_ARGS__, NULL)
#define TestSuite_AddFullWithTestFn(_suite, _name, _func, _dtor, _test_fn, ...)                    \
   do {                                                                                            \
      TestFnCtx *ctx = bson_malloc (sizeof (TestFnCtx));                                           \
      ctx->test_fn = (TestFunc) (_test_fn);                                                        \
      ctx->dtor = _dtor;                                                                           \
      _TestSuite_AddFull (_suite, _name, _func, _TestSuite_TestFnCtxDtor, ctx, __VA_ARGS__, NULL); \
   } while (0)
int
TestSuite_Run (TestSuite *suite);
void
TestSuite_Destroy (TestSuite *suite);

int
test_suite_debug_output (void);
void
test_suite_mock_server_log (const char *msg, ...);
void
_process_skip_file (const char *, mongoc_array_t *);

bool
TestSuite_NoFork (TestSuite *suite);

#ifdef __cplusplus
}
#endif


#endif /* TEST_SUITE_H */
