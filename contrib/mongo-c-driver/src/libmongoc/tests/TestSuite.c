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

#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include <fcntl.h>
#include <stdarg.h>

#include "mongoc/mongoc-thread-private.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <mongoc/mongoc-util-private.h>
#if !defined(_WIN32)
#include <sys/types.h>
#include <inttypes.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/time.h>

#else
#include <windows.h>
#endif

#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"

#define SKIP_LINE_BUFFER_SIZE 1024

static bson_once_t once = BSON_ONCE_INIT;
static bson_mutex_t gTestMutex;
static TestSuite *gTestSuite;


MONGOC_PRINTF_FORMAT (1, 2)
static void
test_msg (const char *format, ...)
{
   va_list ap;

   va_start (ap, format);
   vprintf (format, ap);
   printf ("\n");
   va_end (ap);
   fflush (stdout);
}


void
_test_error (const char *format, ...)
{
   va_list ap;

   fflush (stdout);
   va_start (ap, format);
   vfprintf (stderr, format, ap);
   fprintf (stderr, "\n");
   va_end (ap);
   fflush (stderr);
   abort ();
}

static void
TestSuite_SeedRand (TestSuite *suite, /* IN */
                    Test *test)       /* IN */
{
   BSON_UNUSED (suite);

#ifndef BSON_OS_WIN32
   int fd = open ("/dev/urandom", O_RDONLY);
   int n_read;
   unsigned seed;
   if (fd != -1) {
      n_read = read (fd, &seed, 4);
      BSON_ASSERT (n_read == 4);
      close (fd);
      test->seed = seed;
      return;
   } else {
      test->seed = (unsigned) time (NULL) * (unsigned) getpid ();
   }
#else
   test->seed = (unsigned) time (NULL);
#endif
}


static BSON_ONCE_FUN (_test_suite_ensure_mutex_once)
{
   bson_mutex_init (&gTestMutex);

   BSON_ONCE_RETURN;
}


void
TestSuite_Init (TestSuite *suite, const char *name, int argc, char **argv)
{
   const char *filename;
   int i;
   char *mock_server_log;

   memset (suite, 0, sizeof *suite);

   suite->name = bson_strdup (name);
   suite->flags = 0;
   suite->prgname = bson_strdup (argv[0]);
   suite->silent = false;
   suite->ctest_run = NULL;
   _mongoc_array_init (&suite->match_patterns, sizeof (char *));
   _mongoc_array_init (&suite->failing_flaky_skips, sizeof (TestSkip *));

   for (i = 1; i < argc; i++) {
      if (0 == strcmp ("-d", argv[i])) {
         suite->flags |= TEST_DEBUGOUTPUT;
      } else if ((0 == strcmp ("-f", argv[i])) || (0 == strcmp ("--no-fork", argv[i]))) {
         suite->flags |= TEST_NOFORK;
      } else if ((0 == strcmp ("-t", argv[i])) || (0 == strcmp ("--trace", argv[i]))) {
         if (!MONGOC_TRACE_ENABLED) {
            test_error ("-t requires mongoc compiled with -DENABLE_TRACING=ON.");
         }
         suite->flags |= TEST_TRACE;
      } else if (0 == strcmp ("-F", argv[i])) {
         if (argc - 1 == i) {
            test_error ("-F requires a filename argument.");
         }
         filename = argv[++i];
         if (0 != strcmp ("-", filename)) {
#ifdef _WIN32
            if (0 != fopen_s (&suite->outfile, filename, "w")) {
               suite->outfile = NULL;
            }
#else
            suite->outfile = fopen (filename, "w");
#endif
            if (!suite->outfile) {
               test_error ("Failed to open log file: %s", filename);
            }
         }
      } else if ((0 == strcmp ("-h", argv[i])) || (0 == strcmp ("--help", argv[i]))) {
         suite->flags |= TEST_HELPTEXT;
      } else if (0 == strcmp ("--list-tests", argv[i])) {
         suite->flags |= TEST_LISTTESTS;
      } else if ((0 == strcmp ("-s", argv[i])) || (0 == strcmp ("--silent", argv[i]))) {
         suite->silent = true;
      } else if ((0 == strcmp ("--ctest-run", argv[i]))) {
         if (suite->ctest_run) {
            test_error ("'--ctest-run' can only be specified once");
         }
         if (argc - 1 == i) {
            test_error ("'--ctest-run' requires an argument");
         }
         suite->flags |= TEST_NOFORK;
         suite->silent = true;
         suite->ctest_run = bson_strdup (argv[++i]);
      } else if ((0 == strcmp ("-l", argv[i])) || (0 == strcmp ("--match", argv[i]))) {
         char *val;
         if (argc - 1 == i) {
            test_error ("%s requires an argument.", argv[i]);
         }
         val = bson_strdup (argv[++i]);
         _mongoc_array_append_val (&suite->match_patterns, val);
      } else if (0 == strcmp ("--skip-tests", argv[i])) {
         if (argc - 1 == i) {
            test_error ("%s requires an argument.", argv[i]);
         }
         filename = argv[++i];
         _process_skip_file (filename, &suite->failing_flaky_skips);
      } else {
         test_error ("Unknown option: %s\n"
                     "Try using the --help option.",
                     argv[i]);
      }
   }

   if (suite->match_patterns.len != 0 && suite->ctest_run != NULL) {
      test_error ("'--ctest-run' cannot be specified with '-l' or '--match'");
   }

   mock_server_log = test_framework_getenv ("MONGOC_TEST_SERVER_LOG");
   if (mock_server_log) {
      if (!strcmp (mock_server_log, "stdout")) {
         suite->mock_server_log = stdout;
      } else if (!strcmp (mock_server_log, "stderr")) {
         suite->mock_server_log = stderr;
      } else if (!strcmp (mock_server_log, "json")) {
         suite->mock_server_log_buf = bson_string_new (NULL);
      } else {
         test_error ("Unrecognized option: MONGOC_TEST_SERVER_LOG=%s", mock_server_log);
      }

      bson_free (mock_server_log);
   }

   if (suite->silent) {
      if (suite->outfile) {
         test_error ("Cannot combine -F with --silent");
      }

      suite->flags &= ~(TEST_DEBUGOUTPUT);
   }

   bson_once (&once, &_test_suite_ensure_mutex_once);
   bson_mutex_lock (&gTestMutex);
   gTestSuite = suite;
   bson_mutex_unlock (&gTestMutex);
}


int
TestSuite_CheckLive (void)
{
   return test_framework_getenv_bool ("MONGOC_TEST_SKIP_LIVE") ? 0 : 1;
}

static int
TestSuite_CheckDummy (void)
{
   return 1;
}

int
TestSuite_CheckMockServerAllowed (void)
{
   if (test_framework_getenv_bool ("MONGOC_TEST_SKIP_MOCK")) {
      return 0;
   }
   return 1;
}

static void
TestSuite_AddHelper (void *ctx)
{
   TestFunc cb = (TestFunc) ((TestFnCtx *) ctx)->test_fn;

   cb ();
}

void
TestSuite_Add (TestSuite *suite, /* IN */
               const char *name, /* IN */
               TestFunc func)    /* IN */
{
   TestSuite_AddFullWithTestFn (suite, name, TestSuite_AddHelper, NULL, func, TestSuite_CheckDummy);
}


void
TestSuite_AddLive (TestSuite *suite, /* IN */
                   const char *name, /* IN */
                   TestFunc func)    /* IN */
{
   TestSuite_AddFullWithTestFn (suite, name, TestSuite_AddHelper, NULL, func, TestSuite_CheckLive);
}


static void
_TestSuite_AddCheck (Test *test, CheckFunc check, const char *name)
{
   test->checks[test->num_checks] = check;
   if (++test->num_checks > MAX_TEST_CHECK_FUNCS) {
      MONGOC_STDERR_PRINTF ("Too many check funcs for %s, increase MAX_TEST_CHECK_FUNCS "
                            "to more than %d\n",
                            name,
                            MAX_TEST_CHECK_FUNCS);
      abort ();
   }
}


Test *
_V_TestSuite_AddFull (TestSuite *suite, const char *name, TestFuncWC func, TestFuncDtor dtor, void *ctx, va_list ap)
{
   CheckFunc check;
   Test *test;
   Test *iter;

   if (suite->ctest_run && (0 != strcmp (suite->ctest_run, name))) {
      if (dtor) {
         dtor (ctx);
      }
      return NULL;
   }

   test = (Test *) bson_malloc0 (sizeof *test);
   test->name = bson_strdup (name);
   test->func = func;
   test->num_checks = 0;

   while ((check = va_arg (ap, CheckFunc))) {
      _TestSuite_AddCheck (test, check, name);
   }

   test->next = NULL;
   test->dtor = dtor;
   test->ctx = ctx;
   TestSuite_SeedRand (suite, test);

   if (!suite->tests) {
      suite->tests = test;
      return test;
   }

   for (iter = suite->tests; iter->next; iter = iter->next) {
   }

   iter->next = test;
   return test;
}


void
_TestSuite_AddMockServerTest (TestSuite *suite, const char *name, TestFunc func, ...)
{
   Test *test;
   va_list ap;

   va_start (ap, func);
   test = _V_TestSuite_AddFull (suite, name, (TestFuncWC) func, NULL, NULL, ap);
   va_end (ap);

   if (test) {
      _TestSuite_AddCheck (test, TestSuite_CheckMockServerAllowed, name);
   }
}


void
TestSuite_AddWC (TestSuite *suite,  /* IN */
                 const char *name,  /* IN */
                 TestFuncWC func,   /* IN */
                 TestFuncDtor dtor, /* IN */
                 void *ctx)         /* IN */
{
   TestSuite_AddFull (suite, name, func, dtor, ctx, TestSuite_CheckDummy);
}


void
_TestSuite_AddFull (TestSuite *suite,  /* IN */
                    const char *name,  /* IN */
                    TestFuncWC func,   /* IN */
                    TestFuncDtor dtor, /* IN */
                    void *ctx,
                    ...) /* IN */
{
   va_list ap;

   va_start (ap, ctx);
   _V_TestSuite_AddFull (suite, name, func, dtor, ctx, ap);
   va_end (ap);
}


void
_TestSuite_TestFnCtxDtor (void *ctx)
{
   TestFuncDtor dtor = ((TestFnCtx *) ctx)->dtor;
   if (dtor) {
      dtor (ctx);
   }
   bson_free (ctx);
}


#if defined(_WIN32)
static void
_print_getlasterror_win (const char *msg)
{
   LPTSTR err_msg;

   FormatMessage (FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                  NULL,
                  GetLastError (),
                  MAKELANGID (LANG_NEUTRAL, SUBLANG_DEFAULT),
                  /* FormatMessage is weird about this param. */
                  (LPTSTR) &err_msg,
                  0,
                  NULL);

   test_error ("%s: %s", msg, err_msg);

   LocalFree (err_msg);
}


static int
TestSuite_RunFuncInChild (TestSuite *suite, /* IN */
                          Test *test)       /* IN */
{
   STARTUPINFO si;
   PROCESS_INFORMATION pi;
   char *cmdline;
   DWORD exit_code = -1;

   ZeroMemory (&si, sizeof (si));
   si.cb = sizeof (si);
   ZeroMemory (&pi, sizeof (pi));

   cmdline = bson_strdup_printf ("%s --silent --no-fork -l %s", suite->prgname, test->name);

   if (!CreateProcess (NULL,
                       cmdline,
                       NULL,  /* Process handle not inheritable  */
                       NULL,  /* Thread handle not inheritable   */
                       FALSE, /* Set handle inheritance to FALSE */
                       0,     /* No creation flags               */
                       NULL,  /* Use parent's environment block  */
                       NULL,  /* Use parent's starting directory */
                       &si,
                       &pi)) {
      test_error ("CreateProcess failed (%ld).", GetLastError ());
      bson_free (cmdline);

      return -1;
   }

   if (WaitForSingleObject (pi.hProcess, INFINITE) != WAIT_OBJECT_0) {
      _print_getlasterror_win ("Couldn't await process");
      goto done;
   }

   if (!GetExitCodeProcess (pi.hProcess, &exit_code)) {
      _print_getlasterror_win ("Couldn't get exit code");
      goto done;
   }

done:
   CloseHandle (pi.hProcess);
   CloseHandle (pi.hThread);
   bson_free (cmdline);

   return exit_code;
}
#else /* Unix */
static int
TestSuite_RunFuncInChild (TestSuite *suite, /* IN */
                          Test *test)       /* IN */
{
   pid_t child;
   int exit_code = -1;
   int fd;
   int pipefd[2];
   const char *envp[] = {"MONGOC_TEST_SERVER_LOG=stdout", NULL};
   char buf[4096];
   ssize_t nread;

   if (suite->outfile) {
      fflush (suite->outfile);
   }

   if (suite->mock_server_log_buf) {
      if (pipe (pipefd) == -1) {
         perror ("pipe");
         exit (-1);
      }
   }

   if (-1 == (child = fork ())) {
      return -1;
   }

   if (!child) {
      if (suite->outfile) {
         fclose (suite->outfile);
         suite->outfile = NULL;
      }

      if (suite->mock_server_log_buf) {
         /* tell mock server to log to stdout, and read its output */
         dup2 (pipefd[1], STDOUT_FILENO);
         close (pipefd[0]);
         close (pipefd[1]);
         execle (suite->prgname, suite->prgname, "--no-fork", "--silent", "-l", test->name, (char *) 0, envp);
      } else {
         /* suppress child output */
         fd = open ("/dev/null", O_WRONLY);
         dup2 (fd, STDOUT_FILENO);
         close (fd);

         execl (suite->prgname, suite->prgname, "--no-fork", "-l", test->name, (char *) 0);
      }

      exit (-1);
   }

   if (suite->mock_server_log_buf) {
      close (pipefd[1]);
      while ((nread = read (pipefd[0], buf, sizeof (buf) - 1)) > 0) {
         buf[nread] = '\0';
         bson_string_append (suite->mock_server_log_buf, buf);
      }
   }

   if (-1 == waitpid (child, &exit_code, 0)) {
      perror ("waitpid()");
   }

   return exit_code;
}
#endif


/* replace " with \", newline with \n, tab with four spaces */
static void
_append_json_escaped (bson_string_t *buf, const char *s)
{
   char *escaped = bson_utf8_escape_for_json (s, -1);
   bson_string_append (buf, escaped);
   bson_free (escaped);
}


/* returns 1 on failure, 0 on success */
static int
TestSuite_RunTest (TestSuite *suite, /* IN */
                   Test *test,       /* IN */
                   int *count)       /* INOUT */
{
   int64_t t1, t2, t3;
   char name[MAX_TEST_NAME_LENGTH];
   bson_string_t *buf;
   bson_string_t *mock_server_log_buf;
   size_t i;
   int status = 0;

   bson_snprintf (name, sizeof name, "%s%s", suite->name, test->name);

   buf = bson_string_new (NULL);

   if (suite->flags & TEST_DEBUGOUTPUT) {
      test_msg ("Begin %s, seed %u", name, test->seed);
   }

   for (i = 0; i < suite->failing_flaky_skips.len; i++) {
      TestSkip *skip = _mongoc_array_index (&suite->failing_flaky_skips, TestSkip *, i);
      if (0 == strcmp (name, skip->test_name) && skip->subtest_desc == NULL) {
         if (suite->ctest_run) {
            /* Write a marker that tells CTest that we are skipping this test */
            test_msg ("@@ctest-skipped@@");
         }
         if (!suite->silent) {
            bson_string_append_printf (buf,
                                       "    { \"status\": \"skip\", \"test_file\": \"%s\","
                                       " \"reason\": \"%s\" }%s",
                                       test->name,
                                       skip->reason,
                                       ((*count) == 1) ? "" : ",");
            test_msg ("%s", buf->str);
            if (suite->outfile) {
               fprintf (suite->outfile, "%s", buf->str);
               fflush (suite->outfile);
            }
         }

         goto done;
      }
   }

   for (i = 0; i < test->num_checks; i++) {
      if (!test->checks[i]()) {
         if (suite->ctest_run) {
            /* Write a marker that tells CTest that we are skipping this test */
            test_msg ("@@ctest-skipped@@");
         }
         if (!suite->silent) {
            bson_string_append_printf (
               buf, "    { \"status\": \"skip\", \"test_file\": \"%s\" }%s", test->name, ((*count) == 1) ? "" : ",");
            test_msg ("%s", buf->str);
            if (suite->outfile) {
               fprintf (suite->outfile, "%s", buf->str);
               fflush (suite->outfile);
            }
         }

         goto done;
      }
   }

   t1 = bson_get_monotonic_time ();


   if (suite->flags & TEST_NOFORK) {
      if (suite->flags & TEST_TRACE) {
         mongoc_log_set_handler (mongoc_log_default_handler, NULL);
         mongoc_log_trace_enable ();
      } else {
         mongoc_log_trace_disable ();
      }

      srand (test->seed);
      test_conveniences_init ();
      test->func (test->ctx);
      test_conveniences_cleanup ();
   } else {
      status = TestSuite_RunFuncInChild (suite, test);
   }

   capture_logs (false);

   if (suite->silent) {
      goto done;
   }

   t2 = bson_get_monotonic_time ();
   // CDRIVER-2567: check that bson_get_monotonic_time does not wrap.
   ASSERT_CMPINT64 (t2, >=, t1);
   t3 = t2 - t1;

   bson_string_append_printf (buf,
                              "    { \"status\": \"%s\", "
                              "\"test_file\": \"%s\", "
                              "\"seed\": \"%u\", "
                              "\"start\": %u.%06u, "
                              "\"end\": %u.%06u, "
                              "\"elapsed\": %u.%06u ",
                              (status == 0) ? "pass" : "fail",
                              name,
                              test->seed,
                              (unsigned) t1 / (1000 * 1000),
                              (unsigned) t1 % (1000 * 1000),
                              (unsigned) t2 / (1000 * 1000),
                              (unsigned) t2 % (1000 * 1000),
                              (unsigned) t3 / (1000 * 1000),
                              (unsigned) t3 % (1000 * 1000));

   mock_server_log_buf = suite->mock_server_log_buf;

   if (mock_server_log_buf && mock_server_log_buf->len) {
      bson_string_append (buf, ", \"log_raw\": \"");
      _append_json_escaped (buf, mock_server_log_buf->str);
      bson_string_append (buf, "\"");

      bson_string_truncate (mock_server_log_buf, 0);
   }

   bson_string_append_printf (buf, " }");

   if (*count > 1) {
      bson_string_append_printf (buf, ",");
   }

   test_msg ("%s", buf->str);
   if (suite->outfile) {
      fprintf (suite->outfile, "%s", buf->str);
      fflush (suite->outfile);
   }

done:
   bson_string_free (buf, true);

   return status ? 1 : 0;
}


static void
TestSuite_PrintHelp (TestSuite *suite) /* IN */
{
   printf ("usage: %s [OPTIONS]\n"
           "\n"
           "Options:\n"
           "    -h, --help            Show this help menu.\n"
           "    --list-tests          Print list of available tests.\n"
           "    -f, --no-fork         Do not spawn a process per test (abort on "
           "first error).\n"
           "    -l, --match PATTERN   Run test by name, e.g. \"/Client/command\" or "
           "\"/Client/*\". May be repeated.\n"
           "    --ctest-run TEST      Run only the named TEST for CTest\n"
           "                          integration.\n"
           "    -s, --silent          Suppress all output.\n"
           "    -F FILENAME           Write test results (JSON) to FILENAME.\n"
           "    -d                    Print debug output (useful if a test hangs).\n"
           "    --skip-tests FILE     Skip known failing or flaky tests.\n"
           "    -t, --trace           Enable mongoc tracing (useful to debug "
           "tests).\n"
           "\n",
           suite->prgname);
}


static void
TestSuite_PrintTests (TestSuite *suite) /* IN */
{
   Test *iter;

   printf ("\nTests:\n");
   for (iter = suite->tests; iter; iter = iter->next) {
      printf ("%s%s\n", suite->name, iter->name);
   }

   printf ("\n");
}


static void
TestSuite_PrintJsonSystemHeader (FILE *stream)
{
#ifdef _WIN32
#define INFO_BUFFER_SIZE 32767

   SYSTEM_INFO si;

   GetSystemInfo (&si);

#if defined(_MSC_VER)
   // CDRIVER-4263: GetVersionEx is deprecated.
#pragma warning(suppress : 4996)
   const DWORD version = GetVersion ();
#else
   const DWORD version = GetVersion ();
#endif

   const DWORD major_version = (DWORD) (LOBYTE (LOWORD (version)));
   const DWORD minor_version = (DWORD) (HIBYTE (LOWORD (version)));
   const DWORD build = version < 0x80000000u ? (DWORD) (HIWORD (version)) : 0u;

   fprintf (stream,
            "  \"host\": {\n"
            "    \"sysname\": \"Windows\",\n"
            "    \"release\": \"%ld.%ld (%ld)\",\n"
            "    \"machine\": \"%ld\",\n"
            "    \"memory\": {\n"
            "      \"pagesize\": %ld,\n"
            "      \"npages\": %d\n"
            "    }\n"
            "  },\n",
            major_version,
            minor_version,
            build,
            si.dwProcessorType,
            si.dwPageSize,
            0);
#else
   struct utsname u;
   uint64_t pagesize;
   uint64_t npages = 0;

   if (uname (&u) == -1) {
      perror ("uname()");
      return;
   }

   pagesize = sysconf (_SC_PAGE_SIZE);

#if defined(_SC_PHYS_PAGES)
   npages = sysconf (_SC_PHYS_PAGES);
#endif
   fprintf (stream,
            "  \"host\": {\n"
            "    \"sysname\": \"%s\",\n"
            "    \"release\": \"%s\",\n"
            "    \"machine\": \"%s\",\n"
            "    \"memory\": {\n"
            "      \"pagesize\": %" PRIu64 ",\n"
            "      \"npages\": %" PRIu64 "\n"
            "    }\n"
            "  },\n",
            u.sysname,
            u.release,
            u.machine,
            pagesize,
            npages);
#endif
}

static const char *
getenv_or (const char *name, const char *dflt)
{
   const char *s = getenv (name);
   return s ? s : dflt;
}

static const char *
egetenv (const char *name)
{
   return getenv_or (name, "");
}

static void
TestSuite_PrintJsonHeader (TestSuite *suite, /* IN */
                           FILE *stream)     /* IN */
{
   char *hostname = test_framework_get_host ();
   char *udspath = test_framework_get_unix_domain_socket_path_escaped ();
   int port = test_framework_get_port ();
   bool ssl = test_framework_get_ssl ();

   ASSERT (suite);

   fprintf (stream, "{\n");
   TestSuite_PrintJsonSystemHeader (stream);
   fprintf (stream,
            "  \"auth\": { \"user\": \"%s\", \"pass\": \"%s\" }, \n"
            "  \"addr\": { \"host\": \"%s\", \"port\": %d, \"uri\": \"%s\" },\n"
            "  \"gssapi\": { \"host\": \"%s\", \"user\": \"%s\" }, \n"
            "  \"uds\": \"%s\", \n"
            "  \"compressors\": \"%s\", \n"
            "  \"SSL\": {\n"
            "    \"enabled\": %s,\n"
            "    \"weak_cert_validation\": %s,\n"
            "    \"pem_file\": \"%s\",\n"
            "    \"pem_pwd\": \"%s\",\n"
            "    \"ca_file\": \"%s\",\n"
            "    \"ca_dir\": \"%s\",\n"
            "    \"crl_file\": \"%s\"\n"
            "  },\n"
            "  \"framework\": {\n"
            "    \"monitoringVerbose\": %s,\n"
            "    \"mockServerLog\": \"%s\",\n"
            "    \"futureTimeoutMS\": %" PRIu64 ",\n"
            "    \"majorityReadConcern\": %s,\n"
            "    \"skipLiveTests\": %s,\n"
            "    \"IPv6\": %s\n"
            "  },\n"
            "  \"options\": {\n"
            "    \"fork\": %s,\n"
            "    \"tracing\": %s,\n"
            "    \"apiVersion\": %s\n"
            "  },\n"
            "  \"results\": [\n",
            egetenv ("MONGOC_TEST_USER"),
            egetenv ("MONGOC_TEST_PASSWORD"),
            hostname,
            port,
            egetenv ("MONGOC_TEST_URI"),
            egetenv ("MONGOC_TEST_GSSAPI_HOST"),
            egetenv ("MONGOC_TEST_GSSAPI_USER"),
            udspath,
            egetenv ("MONGOC_TEST_COMPRESSORS"),
            ssl ? "true" : "false",
            test_framework_getenv_bool ("MONGOC_TEST_SSL_WEAK_CERT_VALIDATION") ? "true" : "false",
            egetenv ("MONGOC_TEST_SSL_PEM_FILE"),
            egetenv ("MONGOC_TEST_SSL_PEM_PWD"),
            egetenv ("MONGOC_TEST_SSL_CA_FILE"),
            egetenv ("MONGOC_TEST_SSL_CA_DIR"),
            egetenv ("MONGOC_TEST_SSL_CRL_FILE"),
            getenv ("MONGOC_TEST_MONITORING_VERBOSE") ? "true" : "false",
            egetenv ("MONGOC_TEST_SERVER_LOG"),
            get_future_timeout_ms (),
            test_framework_getenv_bool ("MONGOC_ENABLE_MAJORITY_READ_CONCERN") ? "true" : "false",
            test_framework_getenv_bool ("MONGOC_TEST_SKIP_LIVE") ? "true" : "false",
            test_framework_getenv_bool ("MONGOC_CHECK_IPV6") ? "true" : "false",
            (suite->flags & TEST_NOFORK) ? "false" : "true",
            (suite->flags & TEST_TRACE) ? "true" : "false",
            getenv_or ("MONGODB_API_VERSION", "null"));

   bson_free (hostname);
   bson_free (udspath);

   fflush (stream);
}


static void
TestSuite_PrintJsonFooter (FILE *stream) /* IN */
{
   fprintf (stream, "  ]\n}\n");
   fflush (stream);
}

static bool
TestSuite_TestMatchesName (const TestSuite *suite, const Test *test, const char *testname)
{
   char name[128];
   bool star = strlen (testname) && testname[strlen (testname) - 1] == '*';

   bson_snprintf (name, sizeof name, "%s%s", suite->name, test->name);

   if (star) {
      /* e.g. testname is "/Client*" and name is "/Client/authenticate" */
      return (0 == strncmp (name, testname, strlen (testname) - 1));
   } else {
      return (0 == strcmp (name, testname));
   }
}


bool
test_matches (TestSuite *suite, Test *test)
{
   if (suite->ctest_run) {
      /* We only want exactly the named test */
      return strcmp (test->name, suite->ctest_run) == 0;
   }

   /* If no match patterns were provided, then assume all match. */
   if (suite->match_patterns.len == 0) {
      return true;
   }

   for (size_t i = 0u; i < suite->match_patterns.len; i++) {
      char *pattern = _mongoc_array_index (&suite->match_patterns, char *, i);
      if (TestSuite_TestMatchesName (suite, test, pattern)) {
         return true;
      }
   }

   return false;
}

void
_process_skip_file (const char *filename, mongoc_array_t *skips)
{
   const int max_lines = 1000;
   int lines_read = 0;
   char buffer[SKIP_LINE_BUFFER_SIZE];
   size_t buflen;
   FILE *skip_file;
   char *fgets_ret;
   TestSkip *skip;
   char *test_name_end;
   size_t comment_len;
   char *comment_char;
   char *comment_text;
   size_t subtest_len;
   size_t new_buflen;
   char *subtest_start;
   char *subtest_end;

#ifdef _WIN32
   if (0 != fopen_s (&skip_file, filename, "r")) {
      skip_file = NULL;
   }
#else
   skip_file = fopen (filename, "r");
#endif
   if (!skip_file) {
      test_error ("Failed to open skip file: %s: errno: %d", filename, errno);
   }

   while (lines_read < max_lines) {
      fgets_ret = fgets (buffer, sizeof (buffer), skip_file);
      buflen = strlen (buffer);

      if (buflen == 0 || !fgets_ret) {
         break; /* error or EOF */
      }

      if (buffer[0] == '#' || buffer[0] == ' ' || buffer[0] == '\n') {
         continue; /* Comment line or blank line */
      }

      skip = (TestSkip *) bson_malloc0 (sizeof *skip);
      if (buffer[buflen - 1] == '\n')
         buflen--;
      test_name_end = buffer + buflen;

      /* First get the comment, starting at '#' to EOL */
      comment_len = 0;
      comment_char = strchr (buffer, '#');
      if (comment_char) {
         test_name_end = comment_char;
         comment_text = comment_char;
         while (comment_text[0] == '#' || comment_text[0] == ' ' || comment_text[0] == '\t') {
            if (++comment_text >= (buffer + buflen))
               break;
         }
         skip->reason = bson_strndup (comment_text, buflen - (comment_text - buffer));
         comment_len = buflen - (comment_char - buffer);
      } else {
         skip->reason = NULL;
      }

      /* Next get the subtest name, from first '"' until last '"' */
      new_buflen = buflen - comment_len;
      subtest_start = strstr (buffer, "/\"");
      if (subtest_start && (!comment_char || (subtest_start < comment_char))) {
         test_name_end = subtest_start;
         subtest_start++;
         /* find the second '"' that marks end of subtest name */
         subtest_end = subtest_start + 1;
         while (subtest_end[0] != '\0' && subtest_end[0] != '"' && (subtest_end < buffer + new_buflen)) {
            subtest_end++;
         }
         /* 'subtest_start + 1' to trim leading and trailing '"' */
         subtest_len = subtest_end - (subtest_start + 1);
         skip->subtest_desc = bson_strndup (subtest_start + 1, subtest_len);
      } else {
         skip->subtest_desc = NULL;
      }

      /* Next get the test name */
      while (test_name_end[-1] == ' ' && test_name_end > buffer) {
         /* trailing space might be between test name and '#' */
         test_name_end--;
      }
      skip->test_name = bson_strndup (buffer, test_name_end - buffer);

      _mongoc_array_append_val (skips, skip);

      lines_read++;
   }
   if (lines_read == max_lines) {
      test_error ("Skip file: %s exceeded maximum lines: %d. Increase "
                  "max_lines in _process_skip_file",
                  filename,
                  max_lines);
   }
   fclose (skip_file);
}

static int
TestSuite_RunAll (TestSuite *suite /* IN */)
{
   Test *test;
   int count = 0;
   int status = 0;

   ASSERT (suite);

   /* initialize "count" so we can omit comma after last test output */
   for (test = suite->tests; test; test = test->next) {
      if (test_matches (suite, test)) {
         count++;
      }
   }

   if (suite->ctest_run) {
      /* We should have matched *at most* one test */
      ASSERT (count <= 1);
      if (count == 0) {
         test_error ("No such test '%s'", suite->ctest_run);
      }
   }

   for (test = suite->tests; test; test = test->next) {
      if (test_matches (suite, test)) {
         status += TestSuite_RunTest (suite, test, &count);
         count--;
      }
   }

   if (suite->silent) {
      return status;
   }

   TestSuite_PrintJsonFooter (stdout);
   if (suite->outfile) {
      TestSuite_PrintJsonFooter (suite->outfile);
   }

   return status;
}


int
TestSuite_Run (TestSuite *suite) /* IN */
{
   int failures = 0;
   int64_t start_us;

   if (suite->flags & TEST_HELPTEXT) {
      TestSuite_PrintHelp (suite);
   }

   if (suite->flags & TEST_LISTTESTS) {
      TestSuite_PrintTests (suite);
   }

   if ((suite->flags & TEST_HELPTEXT) || (suite->flags & TEST_LISTTESTS)) {
      return 0;
   }

   if (!suite->silent) {
      TestSuite_PrintJsonHeader (suite, stdout);
      if (suite->outfile) {
         TestSuite_PrintJsonHeader (suite, suite->outfile);
      }
   }

   start_us = bson_get_monotonic_time ();
   if (suite->tests) {
      failures += TestSuite_RunAll (suite);
   } else if (!suite->silent) {
      TestSuite_PrintJsonFooter (stdout);
      if (suite->outfile) {
         TestSuite_PrintJsonFooter (suite->outfile);
      }
   }
   MONGOC_DEBUG ("Duration of all tests (s): %" PRId64, (bson_get_monotonic_time () - start_us) / (1000 * 1000));

   return failures;
}


void
TestSuite_Destroy (TestSuite *suite)
{
   Test *test;
   Test *tmp;

   bson_mutex_lock (&gTestMutex);
   gTestSuite = NULL;
   bson_mutex_unlock (&gTestMutex);

   for (test = suite->tests; test; test = tmp) {
      tmp = test->next;

      if (test->dtor) {
         test->dtor (test->ctx);
      }
      bson_free (test->name);
      bson_free (test);
   }

   if (suite->outfile) {
      fclose (suite->outfile);
   }

   if (suite->mock_server_log_buf) {
      bson_string_free (suite->mock_server_log_buf, true);
   }

   bson_free (suite->name);
   bson_free (suite->prgname);
   bson_free (suite->ctest_run);
   for (size_t i = 0u; i < suite->match_patterns.len; i++) {
      char *val = _mongoc_array_index (&suite->match_patterns, char *, i);
      bson_free (val);
   }

   _mongoc_array_destroy (&suite->match_patterns);

   for (size_t i = 0u; i < suite->failing_flaky_skips.len; i++) {
      TestSkip *val = _mongoc_array_index (&suite->failing_flaky_skips, TestSkip *, i);
      bson_free (val->test_name);
      bson_free (val->subtest_desc);
      bson_free (val->reason);
      bson_free (val);
   }

   _mongoc_array_destroy (&suite->failing_flaky_skips);
}


int
test_suite_debug_output (void)
{
   int ret;

   bson_mutex_lock (&gTestMutex);
   ret = gTestSuite->flags & TEST_DEBUGOUTPUT;
   bson_mutex_unlock (&gTestMutex);

   return ret;
}


MONGOC_PRINTF_FORMAT (1, 2)
void
test_suite_mock_server_log (const char *msg, ...)
{
   va_list ap;
   char *formatted_msg;

   bson_mutex_lock (&gTestMutex);

   if (gTestSuite->mock_server_log || gTestSuite->mock_server_log_buf) {
      va_start (ap, msg);
      formatted_msg = bson_strdupv_printf (msg, ap);
      va_end (ap);

      if (gTestSuite->mock_server_log_buf) {
         bson_string_append_printf (gTestSuite->mock_server_log_buf, "%s\n", formatted_msg);
      } else {
         fprintf (gTestSuite->mock_server_log, "%s\n", formatted_msg);
         fflush (gTestSuite->mock_server_log);
      }

      bson_free (formatted_msg);
   }

   bson_mutex_unlock (&gTestMutex);
}

bool
TestSuite_NoFork (TestSuite *suite)
{
   if (suite->flags & TEST_NOFORK) {
      return true;
   }
   return false;
}

char *
bson_value_to_str (const bson_value_t *val)
{
   bson_t *tmp = bson_new ();
   BSON_APPEND_VALUE (tmp, "v", val);
   char *str = bson_as_canonical_extended_json (tmp, NULL);
   // str is the string: { "v": ... }
   // remove the prefix and suffix.
   char *ret = bson_strndup (str + 6, strlen (str) - (6 + 1));
   bson_free (str);
   bson_destroy (tmp);
   return ret;
}

bool
bson_value_eq (const bson_value_t *a, const bson_value_t *b)
{
   bson_t *tmp_a = bson_new ();
   bson_t *tmp_b = bson_new ();
   BSON_APPEND_VALUE (tmp_a, "v", a);
   BSON_APPEND_VALUE (tmp_b, "v", b);
   bool ret = bson_equal (tmp_a, tmp_b);
   bson_destroy (tmp_b);
   bson_destroy (tmp_a);
   return ret;
}

int32_t
get_current_connection_count (const char *host_and_port)
{
   char *uri_str = bson_strdup_printf ("mongodb://%s\n", host_and_port);
   char *uri_str_with_auth = test_framework_add_user_password_from_env (uri_str);
   mongoc_client_t *client = mongoc_client_new (uri_str_with_auth);
   test_framework_set_ssl_opts (client);
   bson_t *cmd = BCON_NEW ("serverStatus", BCON_INT32 (1));
   bson_t reply;
   bson_error_t error;
   bool ok = mongoc_client_command_simple (client, "admin", cmd, NULL, &reply, &error);
   if (!ok) {
      printf ("serverStatus failed: %s\n", error.message);
      abort ();
   }
   int32_t conns;
   // Get `connections.current` from the reply.
   {
      bson_iter_t iter;
      BSON_ASSERT (bson_iter_init_find (&iter, &reply, "connections"));
      BSON_ASSERT (bson_iter_recurse (&iter, &iter));
      BSON_ASSERT (bson_iter_find (&iter, "current"));
      conns = bson_iter_int32 (&iter);
   }
   bson_destroy (&reply);
   bson_destroy (cmd);
   mongoc_client_destroy (client);
   bson_free (uri_str_with_auth);
   bson_free (uri_str);
   return conns;
}
