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

#include "mongoc-array-private.h"
#include "mongoc-buffer-private.h"
#include "mongoc-error.h"
#include "mongoc-errno-private.h"
#include "mongoc-flags.h"
#include "mongoc-log.h"
#include "mongoc-opcode.h"
#include "mongoc-rpc-private.h"
#include "mongoc-stream.h"
#include "mongoc-stream-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-util-private.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream"

#ifndef MONGOC_DEFAULT_TIMEOUT_MSEC
#define MONGOC_DEFAULT_TIMEOUT_MSEC (60L * 60L * 1000L)
#endif


/**
 * mongoc_stream_close:
 * @stream: A mongoc_stream_t.
 *
 * Closes the underlying file-descriptor used by @stream.
 *
 * Returns: 0 on success, -1 on failure.
 */
int
mongoc_stream_close (mongoc_stream_t *stream)
{
   int ret;

   ENTRY;

   BSON_ASSERT_PARAM (stream);

   BSON_ASSERT (stream->close);

   ret = stream->close (stream);

   RETURN (ret);
}


/**
 * mongoc_stream_failed:
 * @stream: A mongoc_stream_t.
 *
 * Frees any resources referenced by @stream, including the memory allocation
 * for @stream.
 * This handler is called upon stream failure, such as network errors, invalid
 * replies
 * or replicaset reconfigures deleting the stream
 */
void
mongoc_stream_failed (mongoc_stream_t *stream)
{
   ENTRY;

   BSON_ASSERT_PARAM (stream);

   if (stream->failed) {
      stream->failed (stream);
   } else {
      stream->destroy (stream);
   }

   EXIT;
}


/**
 * mongoc_stream_destroy:
 * @stream: A mongoc_stream_t.
 *
 * Frees any resources referenced by @stream, including the memory allocation
 * for @stream.
 */
void
mongoc_stream_destroy (mongoc_stream_t *stream)
{
   ENTRY;

   if (!stream) {
      EXIT;
   }

   BSON_ASSERT (stream->destroy);

   stream->destroy (stream);

   EXIT;
}


/**
 * mongoc_stream_flush:
 * @stream: A mongoc_stream_t.
 *
 * Flushes the data in the underlying stream to the transport.
 *
 * Returns: 0 on success, -1 on failure.
 */
int
mongoc_stream_flush (mongoc_stream_t *stream)
{
   BSON_ASSERT_PARAM (stream);
   return stream->flush (stream);
}


/**
 * mongoc_stream_writev:
 * @stream: A mongoc_stream_t.
 * @iov: An array of iovec to write to the stream.
 * @iovcnt: The number of elements in @iov.
 *
 * Writes an array of iovec buffers to the underlying stream.
 *
 * Returns: the number of bytes written, or -1 upon failure.
 */
ssize_t
mongoc_stream_writev (mongoc_stream_t *stream, mongoc_iovec_t *iov, size_t iovcnt, int32_t timeout_msec)
{
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT_PARAM (iov);
   BSON_ASSERT (iovcnt);

   BSON_ASSERT (stream->writev);

   // CDRIVER-4781: for backward compatibility.
   if (timeout_msec < 0) {
      timeout_msec = MONGOC_DEFAULT_TIMEOUT_MSEC;
   }

   DUMP_IOVEC (writev, iov, iovcnt);
   ret = stream->writev (stream, iov, iovcnt, timeout_msec);

   RETURN (ret);
}

/**
 * mongoc_stream_write:
 * @stream: A mongoc_stream_t.
 * @buf: A buffer to write.
 * @count: The number of bytes to write into @buf.
 *
 * Simplified access to mongoc_stream_writev(). Creates a single iovec
 * with the buffer provided.
 *
 * Returns: -1 on failure, otherwise the number of bytes written.
 */
ssize_t
mongoc_stream_write (mongoc_stream_t *stream, void *buf, size_t count, int32_t timeout_msec)
{
   mongoc_iovec_t iov;
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT_PARAM (buf);

   iov.iov_base = buf;
   iov.iov_len = count;

   BSON_ASSERT (stream->writev);

   ret = mongoc_stream_writev (stream, &iov, 1, timeout_msec);

   RETURN (ret);
}

/**
 * mongoc_stream_readv:
 * @stream: A mongoc_stream_t.
 * @iov: An array of iovec containing the location and sizes to read.
 * @iovcnt: the number of elements in @iov.
 * @min_bytes: the minimum number of bytes to return, or -1.
 *
 * Reads into the various buffers pointed to by @iov and associated
 * buffer lengths.
 *
 * If @min_bytes is specified, then at least min_bytes will be returned unless
 * eof is encountered.  This may result in ETIMEDOUT
 *
 * Returns: the number of bytes read or -1 on failure.
 */
ssize_t
mongoc_stream_readv (
   mongoc_stream_t *stream, mongoc_iovec_t *iov, size_t iovcnt, size_t min_bytes, int32_t timeout_msec)
{
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT_PARAM (iov);
   BSON_ASSERT (iovcnt);

   BSON_ASSERT (stream->readv);

   ret = stream->readv (stream, iov, iovcnt, min_bytes, timeout_msec);
   if (ret >= 0) {
      DUMP_IOVEC (readv, iov, iovcnt);
   }

   RETURN (ret);
}


/**
 * mongoc_stream_read:
 * @stream: A mongoc_stream_t.
 * @buf: A buffer to write into.
 * @count: The number of bytes to write into @buf.
 * @min_bytes: The minimum number of bytes to receive
 *
 * Simplified access to mongoc_stream_readv(). Creates a single iovec
 * with the buffer provided.
 *
 * If @min_bytes is specified, then at least min_bytes will be returned unless
 * eof is encountered.  This may result in ETIMEDOUT
 *
 * Returns: -1 on failure, otherwise the number of bytes read.
 */
ssize_t
mongoc_stream_read (mongoc_stream_t *stream, void *buf, size_t count, size_t min_bytes, int32_t timeout_msec)
{
   mongoc_iovec_t iov;
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT_PARAM (buf);

   iov.iov_base = buf;
   iov.iov_len = count;

   BSON_ASSERT (stream->readv);

   ret = mongoc_stream_readv (stream, &iov, 1, min_bytes, timeout_msec);

   RETURN (ret);
}


int
mongoc_stream_setsockopt (mongoc_stream_t *stream, int level, int optname, void *optval, mongoc_socklen_t optlen)
{
   BSON_ASSERT_PARAM (stream);

   if (stream->setsockopt) {
      return stream->setsockopt (stream, level, optname, optval, optlen);
   }

   return 0;
}


mongoc_stream_t *
mongoc_stream_get_base_stream (mongoc_stream_t *stream) /* IN */
{
   BSON_ASSERT_PARAM (stream);

   if (stream->get_base_stream) {
      return stream->get_base_stream (stream);
   }

   return stream;
}


mongoc_stream_t *
mongoc_stream_get_root_stream (mongoc_stream_t *stream)
{
   BSON_ASSERT_PARAM (stream);

   while (stream->get_base_stream) {
      stream = stream->get_base_stream (stream);
   }

   return stream;
}

mongoc_stream_t *
mongoc_stream_get_tls_stream (mongoc_stream_t *stream) /* IN */
{
   BSON_ASSERT_PARAM (stream);

   for (; stream && stream->type != MONGOC_STREAM_TLS; stream = stream->get_base_stream (stream))
      ;

   return stream;
}

ssize_t
mongoc_stream_poll (mongoc_stream_poll_t *streams, size_t nstreams, int32_t timeout)
{
   mongoc_stream_poll_t *poller = (mongoc_stream_poll_t *) bson_malloc (sizeof (*poller) * nstreams);

   int last_type = 0;
   ssize_t rval = -1;

   errno = 0;

   for (size_t i = 0u; i < nstreams; i++) {
      poller[i].stream = mongoc_stream_get_root_stream (streams[i].stream);
      poller[i].events = streams[i].events;
      poller[i].revents = 0;

      if (i == 0u) {
         last_type = poller[i].stream->type;
      } else if (last_type != poller[i].stream->type) {
         errno = EINVAL;
         goto CLEANUP;
      }
   }

   if (!poller[0].stream->poll) {
      errno = EINVAL;
      goto CLEANUP;
   }

   rval = poller[0].stream->poll (poller, nstreams, timeout);

   if (rval > 0) {
      for (size_t i = 0u; i < nstreams; i++) {
         streams[i].revents = poller[i].revents;
      }
   }

CLEANUP:
   bson_free (poller);

   return rval;
}

bool
mongoc_stream_check_closed (mongoc_stream_t *stream)
{
   int ret;

   ENTRY;

   if (!stream) {
      return true;
   }

   ret = stream->check_closed (stream);

   RETURN (ret);
}

bool
mongoc_stream_timed_out (mongoc_stream_t *stream)
{
   ENTRY;

   BSON_ASSERT_PARAM (stream);

   /* for e.g. a file stream there is no timed_out function */
   RETURN (stream->timed_out && stream->timed_out (stream));
}

bool
mongoc_stream_should_retry (mongoc_stream_t *stream)
{
   ENTRY;

   BSON_ASSERT_PARAM (stream);

   /* for e.g. a file stream there is no should_retry function */
   RETURN (stream->should_retry && stream->should_retry (stream));
}

bool
_mongoc_stream_writev_full (
   mongoc_stream_t *stream, mongoc_iovec_t *iov, size_t iovcnt, int64_t timeout_msec, bson_error_t *error)
{
   size_t total_bytes = 0;
   ssize_t r;
   ENTRY;

   for (size_t i = 0u; i < iovcnt; i++) {
      total_bytes += iov[i].iov_len;
   }

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, timeout_msec))) {
      // CDRIVER-4589
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "timeout_msec value %" PRId64 " exceeds supported 32-bit range",
                      timeout_msec);
      RETURN (false);
   }

   r = mongoc_stream_writev (stream, iov, iovcnt, (int32_t) timeout_msec);
   TRACE ("writev returned: %zd", r);

   if (r < 0) {
      if (error) {
         char buf[128];
         char *errstr;

         errstr = bson_strerror_r (errno, buf, sizeof (buf));

         bson_set_error (error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_SOCKET,
                         "Failure during socket delivery: %s (%d)",
                         errstr,
                         errno);
      }

      RETURN (false);
   }

   if (bson_cmp_not_equal_su (r, total_bytes)) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Failure to send all requested bytes (only sent: %" PRIu64 "/%zu in %" PRId64
                      "ms) during socket delivery",
                      (uint64_t) r,
                      total_bytes,
                      timeout_msec);

      RETURN (false);
   }

   RETURN (true);
}
