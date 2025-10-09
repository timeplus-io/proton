
#include <fcntl.h>
#include <mongoc/mongoc.h>
#include <mongoc/mongoc-stream-private.h>
#include <stdlib.h>

#include "TestSuite.h"


static void
test_buffered_basic (void)
{
   mongoc_stream_t *stream;
   mongoc_stream_t *buffered;
   mongoc_iovec_t iov;
   ssize_t r;
   char buf[16236];

   stream = mongoc_stream_file_new_for_path (BINARY_DIR "/reply2.dat", O_RDONLY, 0);
   BSON_ASSERT (stream);

   /* buffered assumes ownership of stream */
   buffered = mongoc_stream_buffered_new (stream, 1024);

   /* try to read large chunk larger than buffer. */
   iov.iov_len = sizeof buf;
   iov.iov_base = buf;
   r = mongoc_stream_readv (buffered, &iov, 1, iov.iov_len, -1);
   if (r != iov.iov_len) {
      char msg[100];

      bson_snprintf (msg, 100, "Expected %lld got %llu", (long long) r, (unsigned long long) iov.iov_len);
      ASSERT_CMPSTR (msg, "failed");
   }

   /* cleanup */
   mongoc_stream_destroy (buffered);
}


static void
test_buffered_oversized (void)
{
   mongoc_stream_t *stream;
   mongoc_stream_t *buffered;
   mongoc_iovec_t iov;
   ssize_t r;
   char buf[16236];

   stream = mongoc_stream_file_new_for_path (BINARY_DIR "/reply2.dat", O_RDONLY, 0);
   BSON_ASSERT (stream);

   /* buffered assumes ownership of stream */
   buffered = mongoc_stream_buffered_new (stream, 20000);

   /* try to read large chunk larger than buffer. */
   iov.iov_len = sizeof buf;
   iov.iov_base = buf;
   r = mongoc_stream_readv (buffered, &iov, 1, iov.iov_len, -1);
   if (r != iov.iov_len) {
      char msg[100];

      bson_snprintf (msg, 100, "Expected %lld got %llu", (long long) r, (unsigned long long) iov.iov_len);
      ASSERT_CMPSTR (msg, "failed");
   }

   /* cleanup */
   mongoc_stream_destroy (buffered);
}


typedef struct {
   mongoc_stream_t vtable;
   ssize_t rval;
} failing_stream_t;

static ssize_t
failing_stream_writev (mongoc_stream_t *stream, mongoc_iovec_t *iov, size_t iovcnt, int32_t timeout_msec)
{
   failing_stream_t *fstream = (failing_stream_t *) stream;

   BSON_UNUSED (iov);
   BSON_UNUSED (iovcnt);
   BSON_UNUSED (timeout_msec);

   return fstream->rval;
}

void
failing_stream_destroy (mongoc_stream_t *stream)
{
   bson_free (stream);
}

static mongoc_stream_t *
failing_stream_new (ssize_t rval)
{
   failing_stream_t *stream;

   stream = bson_malloc0 (sizeof *stream);
   stream->vtable.type = 999;
   stream->vtable.writev = failing_stream_writev;
   stream->vtable.destroy = failing_stream_destroy;
   stream->rval = rval;

   return (mongoc_stream_t *) stream;
}


static void
test_stream_writev_full (void)
{
   mongoc_stream_t *error_stream = failing_stream_new (-1);
   mongoc_stream_t *short_stream = failing_stream_new (10);
   mongoc_stream_t *success_stream = failing_stream_new (100);
   char bufa[20];
   char bufb[80];
   bool r;
   mongoc_iovec_t iov[2];
   bson_error_t error = {0};
   const char *error_message = "Failure during socket delivery: ";
   const char *short_message = "Failure to send all requested bytes (only "
                               "sent: 10/100 in 100ms) during socket delivery";

   iov[0].iov_base = bufa;
   iov[0].iov_len = sizeof (bufa);
   iov[1].iov_base = bufb;
   iov[1].iov_len = sizeof (bufb);

   errno = EINVAL;
   r = _mongoc_stream_writev_full (error_stream, iov, 2, 100, &error);

   BSON_ASSERT (!r);
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_STREAM);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_STREAM_SOCKET);
   ASSERT_STARTSWITH (error.message, error_message);

   errno = 0;
   r = _mongoc_stream_writev_full (short_stream, iov, 2, 100, &error);
   BSON_ASSERT (!r);
   ASSERT_CMPINT (error.domain, ==, MONGOC_ERROR_STREAM);
   ASSERT_CMPINT (error.code, ==, MONGOC_ERROR_STREAM_SOCKET);
   ASSERT_CMPSTR (error.message, short_message);

   errno = 0;
   r = _mongoc_stream_writev_full (success_stream, iov, 2, 100, &error);
   BSON_ASSERT (r);

   mongoc_stream_destroy (error_stream);
   mongoc_stream_destroy (short_stream);
   mongoc_stream_destroy (success_stream);
}

typedef struct {
   mongoc_stream_t vtable;
   int32_t timeout_msec;
   bool is_set;
} writev_timeout_stream_t;

static void
_writev_timeout_stream_destroy (mongoc_stream_t *stream)
{
   bson_free (stream);
}

static ssize_t
_writev_timeout_stream_writev (mongoc_stream_t *stream_param, mongoc_iovec_t *iov, size_t iovcnt, int32_t timeout_msec)
{
   BSON_UNUSED (iov);
   BSON_UNUSED (iovcnt);

   writev_timeout_stream_t *const stream = (writev_timeout_stream_t *) stream_param;

   stream->is_set = true;
   stream->timeout_msec = timeout_msec;

   return 0;
}

static writev_timeout_stream_t *
_writev_timeout_stream_new (void)
{
   writev_timeout_stream_t *const stream = bson_malloc (sizeof (writev_timeout_stream_t));

   *stream = (writev_timeout_stream_t){
      .vtable =
         {
            .type = 999, // For testing purposes.
            .destroy = _writev_timeout_stream_destroy,
            .writev = _writev_timeout_stream_writev,
         },
      .is_set = false,
      .timeout_msec = 0,
   };

   return stream;
}

static void
test_stream_writev_timeout (void)
{
   bson_error_t error;

   uint8_t data[1] = {0};
   mongoc_iovec_t iov = {.iov_base = (void *) data, .iov_len = 1u};

   // A positive timeout value should be forwarded as-is to the writev function.
   {
      writev_timeout_stream_t *const stream = _writev_timeout_stream_new ();

      ssize_t const res =
         _mongoc_stream_writev_full ((mongoc_stream_t *) stream, &iov, 1u, MONGOC_DEFAULT_SOCKETTIMEOUTMS, &error);
      ASSERT_CMPSSIZE_T (res, ==, 0);
      ASSERT_WITH_MSG (stream->is_set, "expected _writev_timeout_stream_writev() to be invoked");
      ASSERT_CMPINT32 (stream->timeout_msec, ==, MONGOC_DEFAULT_SOCKETTIMEOUTMS);

      mongoc_stream_destroy ((mongoc_stream_t *) stream);
   }

   // A timeout value of 0 should be forwarded as-is to the writev function.
   {
      writev_timeout_stream_t *const stream = _writev_timeout_stream_new ();

      ssize_t const res = _mongoc_stream_writev_full ((mongoc_stream_t *) stream, &iov, 1u, 0, &error);
      ASSERT_CMPSSIZE_T (res, ==, 0);
      ASSERT_WITH_MSG (stream->is_set, "expected _writev_timeout_stream_writev() to be invoked");
      ASSERT_CMPINT32 (stream->timeout_msec, ==, 0);

      mongoc_stream_destroy ((mongoc_stream_t *) stream);
   }

   // CDRIVER-4781: a negative timeout value will fallback to an unspecified
   // default value. The writev function should receive the unspecified default
   // timeout value rather than the negative timeout value.
   {
      // See: MONGOC_DEFAULT_TIMEOUT_MSEC in mongoc-stream.c.
      const int32_t default_timeout_msec = 60 * 60 * 1000;

      writev_timeout_stream_t *const stream = _writev_timeout_stream_new ();

      ssize_t const res = _mongoc_stream_writev_full ((mongoc_stream_t *) stream, &iov, 1u, -1, &error);
      ASSERT_CMPSSIZE_T (res, ==, 0);
      ASSERT_WITH_MSG (stream->is_set, "expected _writev_timeout_stream_writev() to be invoked");
      ASSERT_CMPINT32 (stream->timeout_msec, ==, default_timeout_msec);

      mongoc_stream_destroy ((mongoc_stream_t *) stream);
   }
}


void
test_stream_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Stream/buffered/basic", test_buffered_basic);
   TestSuite_Add (suite, "/Stream/buffered/oversized", test_buffered_oversized);
   TestSuite_Add (suite, "/Stream/writev/full", test_stream_writev_full);
   TestSuite_Add (suite, "/Stream/writev/timeout", test_stream_writev_timeout);
}
