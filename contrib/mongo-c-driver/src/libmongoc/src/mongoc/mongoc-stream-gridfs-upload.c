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

#include "mongoc-counters-private.h"
#include "mongoc-gridfs-bucket-file-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-stream-gridfs-upload-private.h"
#include "mongoc-stream-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream-gridfs-upload"

static void
_mongoc_upload_stream_gridfs_destroy (mongoc_stream_t *stream)
{
   mongoc_gridfs_upload_stream_t *gridfs = (mongoc_gridfs_upload_stream_t *) stream;

   ENTRY;

   BSON_ASSERT (stream);

   mongoc_stream_close (stream);

   _mongoc_gridfs_bucket_file_destroy (gridfs->file);
   bson_free (stream);

   mongoc_counter_streams_active_dec ();
   mongoc_counter_streams_disposed_inc ();

   EXIT;
}

static void
_mongoc_upload_stream_gridfs_failed (mongoc_stream_t *stream)
{
   ENTRY;

   _mongoc_upload_stream_gridfs_destroy (stream);

   EXIT;
}

static int
_mongoc_upload_stream_gridfs_close (mongoc_stream_t *stream)
{
   mongoc_gridfs_upload_stream_t *gridfs = (mongoc_gridfs_upload_stream_t *) stream;
   int ret = 0;

   ENTRY;

   BSON_ASSERT (stream);

   ret = _mongoc_gridfs_bucket_file_save (gridfs->file);

   RETURN (ret ? 0 : 1);
}

static ssize_t
_mongoc_upload_stream_gridfs_writev (mongoc_stream_t *stream, mongoc_iovec_t *iov, size_t iovcnt, int32_t timeout_msec)
{
   mongoc_gridfs_upload_stream_t *gridfs = (mongoc_gridfs_upload_stream_t *) stream;
   ssize_t ret = 0;

   ENTRY;

   BSON_ASSERT (stream);
   BSON_ASSERT (iov);
   BSON_ASSERT (iovcnt);

   (void) timeout_msec; /* unused. */
   ret = _mongoc_gridfs_bucket_file_writev (gridfs->file, iov, iovcnt);

   if (!ret) {
      RETURN (ret);
   }

   mongoc_counter_streams_egress_add (ret);

   RETURN (ret);
}

static bool
_mongoc_upload_stream_gridfs_check_closed (mongoc_stream_t *stream) /* IN */
{
   mongoc_gridfs_upload_stream_t *gridfs = (mongoc_gridfs_upload_stream_t *) stream;

   ENTRY;

   BSON_ASSERT (stream);

   RETURN (gridfs->file->saved);
}

mongoc_stream_t *
_mongoc_upload_stream_gridfs_new (mongoc_gridfs_bucket_file_t *file)
{
   mongoc_gridfs_upload_stream_t *stream;

   ENTRY;

   BSON_ASSERT (file);

   stream = (mongoc_gridfs_upload_stream_t *) bson_malloc0 (sizeof *stream);
   stream->file = file;
   stream->stream.type = MONGOC_STREAM_GRIDFS_UPLOAD;
   stream->stream.destroy = _mongoc_upload_stream_gridfs_destroy;
   stream->stream.failed = _mongoc_upload_stream_gridfs_failed;
   stream->stream.close = _mongoc_upload_stream_gridfs_close;
   stream->stream.writev = _mongoc_upload_stream_gridfs_writev;
   stream->stream.check_closed = _mongoc_upload_stream_gridfs_check_closed;

   mongoc_counter_streams_active_inc ();

   RETURN ((mongoc_stream_t *) stream);
}
