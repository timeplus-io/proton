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


#if defined(__linux__)
#include <sys/syscall.h>
#elif defined(_WIN32)
#include <process.h>
#elif defined(__APPLE__)
#include <pthread.h>
#elif defined(__FreeBSD__)
#include <sys/thr.h>
#elif defined(__NetBSD__)
#include <lwp.h>
#else
#include <unistd.h>
#endif
#include <stdarg.h>
#include <time.h>

#include "mongoc-log.h"
#include "mongoc-log-private.h"
#include "mongoc-thread-private.h"


static bson_once_t once = BSON_ONCE_INIT;
static bson_mutex_t gLogMutex;
static mongoc_log_func_t gLogFunc = mongoc_log_default_handler;
static bool gLogTrace = MONGOC_TRACE_ENABLED;
static void *gLogData;

static BSON_ONCE_FUN (_mongoc_ensure_mutex_once)
{
   bson_mutex_init (&gLogMutex);

   BSON_ONCE_RETURN;
}

void
mongoc_log_set_handler (mongoc_log_func_t log_func, void *user_data)
{
   bson_once (&once, &_mongoc_ensure_mutex_once);

   bson_mutex_lock (&gLogMutex);
   gLogFunc = log_func;
   gLogData = user_data;
   bson_mutex_unlock (&gLogMutex);
}

bool
_mongoc_log_trace_is_enabled (void)
{
   return gLogTrace && MONGOC_TRACE_ENABLED;
}

void
mongoc_log_trace_enable (void)
{
   /* Enable trace logging if-and-only-if tracing is enabled at configure-time,
    * otherwise tracing remains disabled.
    */
   gLogTrace = MONGOC_TRACE_ENABLED;
}

void
mongoc_log_trace_disable (void)
{
   gLogTrace = false;
}

/* just for testing */
void
_mongoc_log_get_handler (mongoc_log_func_t *log_func, void **user_data)
{
   *log_func = gLogFunc;
   *user_data = gLogData;
}


void
mongoc_log (mongoc_log_level_t log_level, const char *log_domain, const char *format, ...)
{
   va_list args;
   char *message;
   int stop_logging;

   bson_once (&once, &_mongoc_ensure_mutex_once);

   stop_logging = !gLogFunc;
   stop_logging = stop_logging || (log_level == MONGOC_LOG_LEVEL_TRACE && !_mongoc_log_trace_is_enabled ());
   if (stop_logging) {
      return;
   }

   BSON_ASSERT (format);

   va_start (args, format);
   message = bson_strdupv_printf (format, args);
   va_end (args);

   bson_mutex_lock (&gLogMutex);
   gLogFunc (log_level, log_domain, message, gLogData);
   bson_mutex_unlock (&gLogMutex);

   bson_free (message);
}


const char *
mongoc_log_level_str (mongoc_log_level_t log_level)
{
   switch (log_level) {
   case MONGOC_LOG_LEVEL_ERROR:
      return "ERROR";
   case MONGOC_LOG_LEVEL_CRITICAL:
      return "CRITICAL";
   case MONGOC_LOG_LEVEL_WARNING:
      return "WARNING";
   case MONGOC_LOG_LEVEL_MESSAGE:
      return "MESSAGE";
   case MONGOC_LOG_LEVEL_INFO:
      return "INFO";
   case MONGOC_LOG_LEVEL_DEBUG:
      return "DEBUG";
   case MONGOC_LOG_LEVEL_TRACE:
      return "TRACE";
   default:
      return "UNKNOWN";
   }
}


void
mongoc_log_default_handler (mongoc_log_level_t log_level, const char *log_domain, const char *message, void *user_data)
{
   struct timeval tv;
   struct tm tt;
   time_t t;
   FILE *stream;
   char nowstr[32];
   int pid;

   BSON_UNUSED (user_data);

   bson_gettimeofday (&tv);
   t = tv.tv_sec;

#ifdef _WIN32
#ifdef _MSC_VER
   localtime_s (&tt, &t);
#else
   tt = *(localtime (&t));
#endif
#else
   localtime_r (&t, &tt);
#endif

   strftime (nowstr, sizeof nowstr, "%Y/%m/%d %H:%M:%S", &tt);

   switch (log_level) {
   case MONGOC_LOG_LEVEL_ERROR:
   case MONGOC_LOG_LEVEL_CRITICAL:
   case MONGOC_LOG_LEVEL_WARNING:
      stream = stderr;
      break;
   case MONGOC_LOG_LEVEL_MESSAGE:
   case MONGOC_LOG_LEVEL_INFO:
   case MONGOC_LOG_LEVEL_DEBUG:
   case MONGOC_LOG_LEVEL_TRACE:
   default:
      stream = stdout;
   }

#ifdef __linux__
   pid = syscall (SYS_gettid);
#elif defined(_WIN32)
   pid = (int) _getpid ();
#elif defined(__FreeBSD__)
   long tid;
   thr_self (&tid);
   pid = (int) tid;
#elif defined(__OpenBSD__)
   pid = (int) getthrid ();
#elif defined(__NetBSD__)
   pid = (int) _lwp_self ();
#elif defined(__APPLE__)
   uint64_t tid;
   pthread_threadid_np (0, &tid);
   pid = (int) tid;
#else
   pid = (int) getpid ();
#endif

   fprintf (stream,
            "%s.%04" PRId64 ": [%5d]: %8s: %12s: %s\n",
            nowstr,
            (int64_t) (tv.tv_usec / 1000),
            pid,
            mongoc_log_level_str (log_level),
            log_domain,
            message);
}

void
mongoc_log_trace_bytes (const char *domain, const uint8_t *_b, size_t _l)
{
   if (!_mongoc_log_trace_is_enabled ()) {
      return;
   }

   bson_string_t *const str = bson_string_new (NULL);
   bson_string_t *const astr = bson_string_new (NULL);

   size_t _i;
   for (_i = 0u; _i < _l; _i++) {
      const uint8_t _v = *(_b + _i);
      const size_t rem = _i % 16u;

      if (rem == 0u) {
         bson_string_append_printf (str, "%05zx: ", _i);
      }

      bson_string_append_printf (str, " %02x", _v);
      if (isprint (_v)) {
         bson_string_append_printf (astr, " %c", _v);
      } else {
         bson_string_append (astr, " .");
      }

      if (rem == 15u) {
         mongoc_log (MONGOC_LOG_LEVEL_TRACE, domain, "%s %s", str->str, astr->str);
         bson_string_truncate (str, 0);
         bson_string_truncate (astr, 0);
      } else if (rem == 7u) {
         bson_string_append (str, " ");
         bson_string_append (astr, " ");
      }
   }

   if (_i != 16u) {
      mongoc_log (MONGOC_LOG_LEVEL_TRACE, domain, "%-56s %s", str->str, astr->str);
   }

   bson_string_free (str, true);
   bson_string_free (astr, true);
}

void
mongoc_log_trace_iovec (const char *domain, const mongoc_iovec_t *_iov, size_t _iovcnt)
{
   bson_string_t *str, *astr;
   const char *_b;
   unsigned _i = 0;
   unsigned _j = 0;
   unsigned _k = 0;
   size_t _l = 0;
   uint8_t _v;

   if (!_mongoc_log_trace_is_enabled ()) {
      return;
   }

   for (_i = 0; _i < _iovcnt; _i++) {
      _l += _iov[_i].iov_len;
   }

   _i = 0;
   str = bson_string_new (NULL);
   astr = bson_string_new (NULL);

   for (_j = 0; _j < _iovcnt; _j++) {
      _b = (char *) _iov[_j].iov_base;
      _l = _iov[_j].iov_len;

      for (_k = 0; _k < _l; _k++, _i++) {
         _v = *(_b + _k);
         if ((_i % 16) == 0) {
            bson_string_append_printf (str, "%05x: ", _i);
         }

         bson_string_append_printf (str, " %02x", _v);
         if (isprint (_v)) {
            bson_string_append_printf (astr, " %c", _v);
         } else {
            bson_string_append (astr, " .");
         }

         if ((_i % 16) == 15) {
            mongoc_log (MONGOC_LOG_LEVEL_TRACE, domain, "%s %s", str->str, astr->str);
            bson_string_truncate (str, 0);
            bson_string_truncate (astr, 0);
         } else if ((_i % 16) == 7) {
            bson_string_append (str, " ");
            bson_string_append (astr, " ");
         }
      }
   }

   if (_i != 16) {
      mongoc_log (MONGOC_LOG_LEVEL_TRACE, domain, "%-56s %s", str->str, astr->str);
   }

   bson_string_free (str, true);
   bson_string_free (astr, true);
}
