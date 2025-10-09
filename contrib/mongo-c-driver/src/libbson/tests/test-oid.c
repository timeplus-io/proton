/*
 * Copyright 2013 MongoDB, Inc.
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
#define BSON_INSIDE
#include "bson/bson-iso8601-private.h"
#include "bson/bson-context-private.h"
#include "common-thread-private.h"
#undef BSON_INSIDE

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

#include <limits.h>

#include "TestSuite.h"

#define N_THREADS 4

static const char *gTestOids[] = {"000000000000000000000000",
                                  "010101010101010101010101",
                                  "0123456789abcdefafcdef03",
                                  "fcdeab182763817236817236",
                                  "ffffffffffffffffffffffff",
                                  "eeeeeeeeeeeeeeeeeeeeeeee",
                                  "999999999999999999999999",
                                  "111111111111111111111111",
                                  NULL};

static const char *gTestOidsCase[] = {"0123456789ABCDEFAFCDEF03",
                                      "FCDEAB182763817236817236",
                                      "FFFFFFFFFFFFFFFFFFFFFFFF",
                                      "EEEEEEEEEEEEEEEEEEEEEEEE",
                                      "01234567890ACBCDEFabcdef",
                                      NULL};

static const char *gTestOidsFail[] = {"                        ",
                                      "abasdf                  ",
                                      "asdfasdfasdfasdfasdf    ",
                                      "00000000000000000000000z",
                                      "00187263123ghh21382812a8",
                                      NULL};

BSON_THREAD_FUN (oid_worker, data)
{
   bson_context_t *context = data;
   bson_oid_t oid;
   bson_oid_t oid2;
   int i;

   bson_oid_init (&oid2, context);
   for (i = 0; i < 500000; i++) {
      bson_oid_init (&oid, context);
      BSON_ASSERT (false == bson_oid_equal (&oid, &oid2));
      BSON_ASSERT (0 < bson_oid_compare (&oid, &oid2));
      bson_oid_copy (&oid, &oid2);
   }

   BSON_THREAD_RETURN;
}

static void
test_bson_oid_init_from_string (void)
{
   bson_context_t *context;
   bson_oid_t oid;
   char str[25];
   int i;

   context = bson_context_new (BSON_CONTEXT_NONE);

   /*
    * Test successfully parsed oids.
    */

   for (i = 0; gTestOids[i]; i++) {
      bson_oid_init_from_string (&oid, gTestOids[i]);
      bson_oid_to_string (&oid, str);
      BSON_ASSERT (!strcmp (str, gTestOids[i]));
   }

   /*
    * Test successfully parsed oids (case-insensitive).
    */
   for (i = 0; gTestOidsCase[i]; i++) {
      char oid_lower[25];
      int j;

      bson_oid_init_from_string (&oid, gTestOidsCase[i]);
      bson_oid_to_string (&oid, str);
      BSON_ASSERT (!bson_strcasecmp (str, gTestOidsCase[i]));

      for (j = 0; gTestOidsCase[i][j]; j++) {
         oid_lower[j] = tolower (gTestOidsCase[i][j]);
      }

      oid_lower[24] = '\0';
      BSON_ASSERT (!strcmp (str, oid_lower));
   }

   /*
    * Test that sizeof(str) works (len of 25 with \0 instead of 24).
    */
   BSON_ASSERT (bson_oid_is_valid ("ffffffffffffffffffffffff", 24));
   bson_oid_init_from_string (&oid, "ffffffffffffffffffffffff");
   bson_oid_to_string (&oid, str);
   BSON_ASSERT (bson_oid_is_valid (str, sizeof str));

   /*
    * Test the failures.
    */

   for (i = 0; gTestOidsFail[i]; i++) {
      bson_oid_init_from_string (&oid, gTestOidsFail[i]);
      bson_oid_to_string (&oid, str);
      BSON_ASSERT (strcmp (str, gTestOidsFail[i]));
   }

   bson_context_destroy (context);
}


static void
test_bson_oid_hash (void)
{
   bson_oid_t oid;

   bson_oid_init_from_string (&oid, "000000000000000000000000");
   BSON_ASSERT (bson_oid_hash (&oid) == 1487062149);
}


static void
test_bson_oid_compare (void)
{
   bson_oid_t oid;
   bson_oid_t oid2;

   bson_oid_init_from_string (&oid, "000000000000000000001234");
   bson_oid_init_from_string (&oid2, "000000000000000000001234");
   BSON_ASSERT (0 == bson_oid_compare (&oid, &oid2));
   BSON_ASSERT (true == bson_oid_equal (&oid, &oid2));

   bson_oid_init_from_string (&oid, "000000000000000000001234");
   bson_oid_init_from_string (&oid2, "000000000000000000004321");
   BSON_ASSERT (bson_oid_compare (&oid, &oid2) < 0);
   BSON_ASSERT (bson_oid_compare (&oid2, &oid) > 0);
   BSON_ASSERT (false == bson_oid_equal (&oid, &oid2));
}


static void
test_bson_oid_copy (void)
{
   bson_oid_t oid;
   bson_oid_t oid2;

   bson_oid_init_from_string (&oid, "000000000000000000001234");
   bson_oid_init_from_string (&oid2, "000000000000000000004321");
   bson_oid_copy (&oid, &oid2);
   BSON_ASSERT (true == bson_oid_equal (&oid, &oid2));
}


static void
test_bson_oid_init (void)
{
   bson_context_t *context;
   bson_oid_t oid;
   bson_oid_t oid2;
   int i;

   context = bson_context_new (BSON_CONTEXT_NONE);
   bson_oid_init (&oid, context);
   for (i = 0; i < 10000; i++) {
      bson_oid_init (&oid2, context);
      BSON_ASSERT (false == bson_oid_equal (&oid, &oid2));
      BSON_ASSERT (0 > bson_oid_compare (&oid, &oid2));
      bson_oid_copy (&oid2, &oid);
   }
   bson_context_destroy (context);

   /*
    * Test that the shared context works.
    */
   bson_oid_init (&oid, NULL);
   BSON_ASSERT (bson_context_get_default ());
}


static void
test_bson_oid_init_sequence (void)
{
   bson_context_t *context;
   bson_oid_t oid;
   bson_oid_t oid2;
   int i;

   BEGIN_IGNORE_DEPRECATIONS
   context = bson_context_new (BSON_CONTEXT_NONE);
   bson_oid_init_sequence (&oid, context);
   for (i = 0; i < 10000; i++) {
      bson_oid_init_sequence (&oid2, context);
      BSON_ASSERT (false == bson_oid_equal (&oid, &oid2));
      BSON_ASSERT (0 > bson_oid_compare (&oid, &oid2));
      bson_oid_copy (&oid2, &oid);
   }
   bson_context_destroy (context);
   END_IGNORE_DEPRECATIONS
}


static char *
get_time_as_string (const bson_oid_t *oid)
{
   bson_string_t *str = bson_string_new (NULL);
   time_t time = bson_oid_get_time_t (oid);

   _bson_iso8601_date_format (time * 1000, str);
   return bson_string_free (str, false);
}


static void
test_bson_oid_get_time_t (void)
{
   bson_context_t *context;
   bson_oid_t oid;
   uint32_t start = (uint32_t) time (NULL);
   char *str;

   context = bson_context_new (BSON_CONTEXT_NONE);
   bson_oid_init (&oid, context);
   ASSERT_CMPUINT32 ((uint32_t) bson_oid_get_time_t (&oid), >=, start);
   ASSERT_CMPUINT32 ((uint32_t) bson_oid_get_time_t (&oid), <=, (uint32_t) time (NULL));

   bson_oid_init_from_string (&oid, "000000000000000000000000");
   str = get_time_as_string (&oid);
   ASSERT_CMPSTR (str, "1970-01-01T00:00:00Z");
   bson_free (str);

   /* if time_t is a signed int32, then a negative value may be interpreted
    * as a negative date when printing. */
   if (sizeof (time_t) == 8) {
      bson_oid_init_from_string (&oid, "7FFFFFFF0000000000000000");
      str = get_time_as_string (&oid);
      ASSERT_CMPSTR (str, "2038-01-19T03:14:07Z");
      bson_free (str);

      bson_oid_init_from_string (&oid, "800000000000000000000000");
      str = get_time_as_string (&oid);
      ASSERT_CMPSTR (str, "2038-01-19T03:14:08Z");
      bson_free (str);

      bson_oid_init_from_string (&oid, "FFFFFFFF0000000000000000");
      str = get_time_as_string (&oid);
      ASSERT_CMPSTR (str, "2106-02-07T06:28:15Z");
      bson_free (str);
   }

   bson_context_destroy (context);
}


static void
test_bson_oid_init_with_threads (void)
{
   bson_context_t *context;
   int i;
   int r;

   {
      bson_context_flags_t flags = BSON_CONTEXT_NONE;
      bson_context_t *contexts[N_THREADS];
      bson_thread_t threads[N_THREADS];

      for (i = 0; i < N_THREADS; i++) {
         contexts[i] = bson_context_new (flags);
         r = mcommon_thread_create (&threads[i], oid_worker, contexts[i]);
         BSON_ASSERT (r == 0);
      }

      for (i = 0; i < N_THREADS; i++) {
         mcommon_thread_join (threads[i]);
      }

      for (i = 0; i < N_THREADS; i++) {
         bson_context_destroy (contexts[i]);
      }
   }

   /*
    * Test threaded generation of oids using a single context;
    */
   {
      bson_thread_t threads[N_THREADS];

      context = bson_context_new (BSON_CONTEXT_THREAD_SAFE);

      for (i = 0; i < N_THREADS; i++) {
         r = mcommon_thread_create (&threads[i], oid_worker, context);
         BSON_ASSERT (r == 0);
      }

      for (i = 0; i < N_THREADS; i++) {
         r = mcommon_thread_join (threads[i]);
         BSON_ASSERT (r == 0);
      }

      bson_context_destroy (context);
   }
}


static void
test_bson_oid_counter_overflow (void)
{
   bson_oid_t oid;
   char str[25];
   bson_context_t *ctx = bson_context_new (BSON_CONTEXT_NONE);
   ctx->seq32 = 0xFFFFFF;

   bson_oid_init (&oid, ctx);
   bson_oid_to_string (&oid, str);
   /* check that the counter portion of the string is FFFFFF" */
   ASSERT_CMPSTR (str + (24 - 6), "ffffff");
   bson_oid_init (&oid, ctx);
   /* the next oid should have overflowed the counter. */
   bson_oid_to_string (&oid, str);
   ASSERT_CMPSTR (str + (24 - 6), "000000");
   bson_context_destroy (ctx);
}


#ifndef _WIN32
#include <sys/wait.h>


typedef struct {
   uint32_t timestamp; /* timestamp */
   uint64_t rand;      /* only really 5 bytes */
   uint32_t counter;   /* only really 3 bytes */
} _parsed_oid_t;


/* parse an oid into parts usable for comparison. */
static void
_parse_oid (bson_oid_t *oid, _parsed_oid_t *out)
{
   memset (out, 0, sizeof (_parsed_oid_t));
   memcpy (&out->timestamp, oid->bytes, 4);
   out->timestamp = BSON_UINT32_FROM_BE (out->timestamp);
   /* rand_bytes is 5 bytes starting at index 4. Read all of it into an 8 byte
    * uint64_t
    * and chop off the extra 3 bytes. */
   memcpy (&out->rand, oid->bytes + 1, 8);
   out->rand = BSON_UINT64_FROM_BE (out->rand) & 0x000000FFFFFFFFFF;

   /* counter is 3 bytes. Read four bytes and chop off extra 1. */
   memcpy (&out->counter, oid->bytes + 8, 4);
   out->counter = BSON_UINT32_FROM_BE (out->counter) & 0x00FFFFFF;
}


/* Only test where fork() is available. Does not exercise platform specific
 * code. */
static void
test_bson_oid_after_fork (void)
{
   bson_context_t *ctx;
   bson_oid_t parent_oid, self_check;
   _parsed_oid_t parent_parsed, self_check_parsed;
   pid_t pid;
   int child_exit_status = 0;

   /* a self check of the parsing utility. */
   bson_oid_init_from_string (&self_check, "AAAAAAAABBBBBBBBBBCCCCCC");
   _parse_oid (&self_check, &self_check_parsed);
   ASSERT_CMPUINT32 (self_check_parsed.timestamp, ==, 0xAAAAAAAA);
   ASSERT_CMPUINT64 (self_check_parsed.rand, ==, 0x000000BBBBBBBBBB);
   ASSERT_CMPUINT32 (self_check_parsed.counter, ==, 0x00CCCCCC);

   bson_oid_init (&parent_oid, bson_context_get_default ());
   _parse_oid (&parent_oid, &parent_parsed);
   pid = fork ();
   if (pid == 0) {
      bson_oid_t child_oid, child_2_oid;
      _parsed_oid_t child_parsed, child_2_parsed;

      bson_oid_init (&child_oid, bson_context_get_default ());
      _parse_oid (&child_oid, &child_parsed);
      ASSERT_CMPUINT64 (child_parsed.rand, !=, parent_parsed.rand);
      ASSERT_CMPUINT32 (child_parsed.counter, ==, parent_parsed.counter + 1);
      BSON_ASSERT (0 != bson_oid_compare (&parent_oid, &child_oid));

      /* but a different OID gets the same random bytes. */
      bson_oid_init (&child_2_oid, bson_context_get_default ());
      _parse_oid (&child_2_oid, &child_2_parsed);
      ASSERT_CMPUINT64 (child_2_parsed.rand, !=, parent_parsed.rand);
      ASSERT_CMPUINT64 (child_2_parsed.rand, ==, child_parsed.rand);
      ASSERT_CMPUINT32 (child_2_parsed.counter, ==, child_parsed.counter + 1);
      BSON_ASSERT (0 != bson_oid_compare (&child_oid, &child_2_oid));

      exit (0);
   } else {
      bson_oid_t parent_2_oid;
      _parsed_oid_t parent_2_parsed;

      BSON_ASSERT (-1 != waitpid (pid, &child_exit_status, 0 /* opts */));
      BSON_ASSERT (child_exit_status == 0);

      /* but initializing another OID in the parent does *not* change random
       * bytes. */
      bson_oid_init (&parent_2_oid, bson_context_get_default ());
      _parse_oid (&parent_2_oid, &parent_2_parsed);
      ASSERT_CMPUINT64 (parent_2_parsed.rand, ==, parent_parsed.rand);
      ASSERT_CMPUINT32 (parent_2_parsed.counter, ==, parent_parsed.counter + 1);
   }

   /* now test with PID caching enabled. */
   ctx = bson_context_new (BSON_CONTEXT_NONE);
   bson_oid_init (&parent_oid, ctx);
   _parse_oid (&parent_oid, &parent_parsed);
   pid = fork ();
   if (pid == 0) {
      bson_oid_t child_oid;
      _parsed_oid_t child_parsed;

      bson_oid_init (&child_oid, ctx);
      _parse_oid (&child_oid, &child_parsed);

      /* since PID is cached, random value does not get regenerated. */
      ASSERT_CMPUINT64 (child_parsed.rand, ==, parent_parsed.rand);
      ASSERT_CMPUINT32 (child_parsed.counter, ==, parent_parsed.counter + 1);
      BSON_ASSERT (0 != bson_oid_compare (&parent_oid, &child_oid));
      exit (0);
   } else {
      bson_oid_t parent_2_oid;
      _parsed_oid_t parent_2_parsed;

      BSON_ASSERT (-1 != waitpid (pid, &child_exit_status, 0 /* opts */));
      BSON_ASSERT (child_exit_status == 0);

      /* but initializing another OID in the parent does *not* change random
       * bytes. */
      bson_oid_init (&parent_2_oid, ctx);
      _parse_oid (&parent_2_oid, &parent_2_parsed);
      ASSERT_CMPUINT64 (parent_2_parsed.rand, ==, parent_parsed.rand);
      ASSERT_CMPUINT32 (parent_2_parsed.counter, ==, parent_parsed.counter + 1);
   }
   bson_context_destroy (ctx);
}
#endif


void
test_oid_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/bson/oid/init", test_bson_oid_init);
   TestSuite_Add (suite, "/bson/oid/init_from_string", test_bson_oid_init_from_string);
   TestSuite_Add (suite, "/bson/oid/init_sequence", test_bson_oid_init_sequence);
   TestSuite_Add (suite, "/bson/oid/init_with_threads", test_bson_oid_init_with_threads);
   TestSuite_Add (suite, "/bson/oid/hash", test_bson_oid_hash);
   TestSuite_Add (suite, "/bson/oid/compare", test_bson_oid_compare);
   TestSuite_Add (suite, "/bson/oid/copy", test_bson_oid_copy);
   TestSuite_Add (suite, "/bson/oid/get_time_t", test_bson_oid_get_time_t);
   TestSuite_Add (suite, "/bson/oid/counter_overflow", test_bson_oid_counter_overflow);
#ifndef _WIN32
   if (!TestSuite_NoFork (suite)) {
      TestSuite_Add (suite, "/bson/oid/after_fork", test_bson_oid_after_fork);
   }
#endif
}
