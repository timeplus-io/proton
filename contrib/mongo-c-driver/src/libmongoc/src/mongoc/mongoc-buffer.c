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
#include <stdarg.h>

#include "mongoc-error.h"
#include "mongoc-buffer-private.h"
#include "mongoc-trace-private.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "buffer"

#ifndef MONGOC_BUFFER_DEFAULT_SIZE
#define MONGOC_BUFFER_DEFAULT_SIZE 1024
#endif


static void
make_space_for (mongoc_buffer_t *buffer, size_t data_size)
{
   if (buffer->len + data_size > buffer->datalen) {
      buffer->datalen = bson_next_power_of_two (buffer->len + data_size);
      buffer->data = (uint8_t *) buffer->realloc_func (buffer->data, buffer->datalen, buffer->realloc_data);
   }
}


/**
 * _mongoc_buffer_init:
 * @buffer: A mongoc_buffer_t to initialize.
 * @buf: A data buffer to attach to @buffer.
 * @buflen: The size of @buflen.
 * @realloc_func: A function to resize @buf.
 *
 * Initializes @buffer for use. If additional space is needed by @buffer, then
 * @realloc_func will be called to resize @buf.
 *
 * @buffer takes ownership of @buf and will realloc it to zero bytes when
 * cleaning up the data structure.
 */
void
_mongoc_buffer_init (
   mongoc_buffer_t *buffer, uint8_t *buf, size_t buflen, bson_realloc_func realloc_func, void *realloc_data)
{
   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT (buflen || !buf);

   if (!realloc_func) {
      realloc_func = bson_realloc_ctx;
   }

   if (!buflen) {
      buflen = MONGOC_BUFFER_DEFAULT_SIZE;
   }

   if (!buf) {
      buf = (uint8_t *) realloc_func (NULL, buflen, NULL);
   }

   memset (buffer, 0, sizeof *buffer);

   buffer->data = buf;
   buffer->datalen = buflen;
   buffer->len = 0;
   buffer->realloc_func = realloc_func;
   buffer->realloc_data = realloc_data;
}


/**
 * _mongoc_buffer_destroy:
 * @buffer: A mongoc_buffer_t.
 *
 * Cleanup after @buffer and release any allocated resources.
 */
void
_mongoc_buffer_destroy (mongoc_buffer_t *buffer)
{
   BSON_ASSERT_PARAM (buffer);

   if (buffer->data && buffer->realloc_func) {
      buffer->realloc_func (buffer->data, 0, buffer->realloc_data);
   }

   memset (buffer, 0, sizeof *buffer);
}


/**
 * _mongoc_buffer_clear:
 * @buffer: A mongoc_buffer_t.
 * @zero: If the memory should be zeroed.
 *
 * Clears a buffers contents and resets it to initial state. You can request
 * that the memory is zeroed, which might be useful if you know the contents
 * contain security related information.
 */
void
_mongoc_buffer_clear (mongoc_buffer_t *buffer, bool zero)
{
   BSON_ASSERT_PARAM (buffer);

   if (zero) {
      memset (buffer->data, 0, buffer->datalen);
   }

   buffer->len = 0;
}


bool
_mongoc_buffer_append (mongoc_buffer_t *buffer, const uint8_t *data, size_t data_size)
{
   uint8_t *buf;

   ENTRY;

   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT (data_size);

   BSON_ASSERT (buffer->datalen);

   make_space_for (buffer, data_size);

   buf = &buffer->data[buffer->len];

   BSON_ASSERT ((buffer->len + data_size) <= buffer->datalen);

   memcpy (buf, data, data_size);

   buffer->len += data_size;

   RETURN (true);
}


/**
 * mongoc_buffer_append_from_stream:
 * @buffer; A mongoc_buffer_t.
 * @stream: The stream to read from.
 * @size: The number of bytes to read.
 * @timeout_msec: The number of milliseconds to wait or -1 for the default
 * @error: A location for a bson_error_t, or NULL.
 *
 * Reads from stream @size bytes and stores them in @buffer. This can be used
 * in conjunction with reading RPCs from a stream. You read from the stream
 * into this buffer and then scatter the buffer into the RPC.
 *
 * Returns: true if successful; otherwise false and @error is set.
 */
bool
_mongoc_buffer_append_from_stream (
   mongoc_buffer_t *buffer, mongoc_stream_t *stream, size_t size, int64_t timeout_msec, bson_error_t *error)
{
   uint8_t *buf;
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT (size);

   BSON_ASSERT (buffer->datalen);

   make_space_for (buffer, size);

   buf = &buffer->data[buffer->len];

   BSON_ASSERT ((buffer->len + size) <= buffer->datalen);

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, timeout_msec))) {
      // CDRIVER-4589
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "timeout_msec value %" PRId64 " exceeds supported 32-bit range",
                      timeout_msec);
      RETURN (false);
   }

   ret = mongoc_stream_read (stream, buf, size, size, (int32_t) timeout_msec);
   if (bson_cmp_not_equal_su (ret, size)) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Failed to read %zu bytes: socket error or timeout",
                      size);
      RETURN (false);
   }

   buffer->len += (size_t) ret;

   RETURN (true);
}


/**
 * _mongoc_buffer_fill:
 * @buffer: A mongoc_buffer_t.
 * @stream: A stream to read from.
 * @min_bytes: The minimum number of bytes to read.
 * @error: A location for a bson_error_t or NULL.
 *
 * Attempts to fill the entire buffer, or at least @min_bytes.
 *
 * Returns: The number of buffered bytes, or -1 on failure.
 */
ssize_t
_mongoc_buffer_fill (
   mongoc_buffer_t *buffer, mongoc_stream_t *stream, size_t min_bytes, int64_t timeout_msec, bson_error_t *error)
{
   ssize_t ret;
   size_t avail_bytes;

   ENTRY;

   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT_PARAM (stream);

   BSON_ASSERT (buffer->data);
   BSON_ASSERT (buffer->datalen);

   if (min_bytes <= buffer->len) {
      BSON_ASSERT (bson_in_range_unsigned (ssize_t, buffer->len));
      RETURN ((ssize_t) buffer->len);
   }

   min_bytes -= buffer->len;

   make_space_for (buffer, min_bytes);

   avail_bytes = buffer->datalen - buffer->len;

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, timeout_msec))) {
      // CDRIVER-4589
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "timeout_msec value %" PRId64 " exceeds supported 32-bit range",
                      timeout_msec);
      RETURN (false);
   }

   ret = mongoc_stream_read (stream, &buffer->data[buffer->len], avail_bytes, min_bytes, (int32_t) timeout_msec);

   if (ret < 0) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to buffer %zu bytes", min_bytes);
      RETURN (-1);
   }

   buffer->len += (size_t) ret;

   if (buffer->len < min_bytes) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_SOCKET,
                      "Could only buffer %zu of %zu bytes",
                      buffer->len,
                      min_bytes);
      RETURN (-1);
   }

   BSON_ASSERT (bson_in_range_unsigned (ssize_t, buffer->len));
   RETURN ((ssize_t) buffer->len);
}


/**
 * mongoc_buffer_try_append_from_stream:
 * @buffer; A mongoc_buffer_t.
 * @stream: The stream to read from.
 * @size: The number of bytes to read.
 * @timeout_msec: The number of milliseconds to wait or -1 for the default
 *
 * Reads from stream @size bytes and stores them in @buffer. This can be used
 * in conjunction with reading RPCs from a stream. You read from the stream
 * into this buffer and then scatter the buffer into the RPC.
 *
 * Returns: bytes read if successful; otherwise 0 or -1.
 */
ssize_t
_mongoc_buffer_try_append_from_stream (mongoc_buffer_t *buffer,
                                       mongoc_stream_t *stream,
                                       size_t size,
                                       int64_t timeout_msec)
{
   uint8_t *buf;
   ssize_t ret;

   ENTRY;

   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT_PARAM (stream);
   BSON_ASSERT (size);

   BSON_ASSERT (buffer->datalen);

   make_space_for (buffer, size);

   buf = &buffer->data[buffer->len];

   BSON_ASSERT ((buffer->len + size) <= buffer->datalen);

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, timeout_msec))) {
      // CDRIVER-4589
      MONGOC_ERROR ("timeout_msec value %" PRId64 " exceeds supported 32-bit range", timeout_msec);
      RETURN (-1);
   }

   ret = mongoc_stream_read (stream, buf, size, 0, (int32_t) timeout_msec);

   if (ret > 0) {
      buffer->len += (size_t) ret;
   }

   RETURN (ret);
}
