/*
 * Copyright 2016 MongoDB, Inc.
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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SSL_OPENSSL

#include <bson/bson.h>

#include <errno.h>
#include <string.h>

#include "mongoc-counters-private.h"
#include "mongoc-errno-private.h"
#include "mongoc-stream-tls.h"
#include "mongoc-stream-private.h"
#include "mongoc-stream-tls-private.h"
#include "mongoc-stream-tls-openssl-bio-private.h"
#include "mongoc-stream-tls-openssl-private.h"
#include "mongoc-openssl-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-log.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream-tls-openssl-bio"


#if OPENSSL_VERSION_NUMBER < 0x10100000L || (defined(LIBRESSL_VERSION_NUMBER) && LIBRESSL_VERSION_NUMBER < 0x20700000L)

/* Magic vtable to make our BIO shim */
static BIO_METHOD gMongocStreamTlsOpenSslRawMethods = {BIO_TYPE_FILTER,
                                                       "mongoc-stream-tls-glue",
                                                       mongoc_stream_tls_openssl_bio_write,
                                                       mongoc_stream_tls_openssl_bio_read,
                                                       mongoc_stream_tls_openssl_bio_puts,
                                                       mongoc_stream_tls_openssl_bio_gets,
                                                       mongoc_stream_tls_openssl_bio_ctrl,
                                                       mongoc_stream_tls_openssl_bio_create,
                                                       mongoc_stream_tls_openssl_bio_destroy,
                                                       NULL};

static void
BIO_set_data (BIO *b, void *ptr)
{
   b->ptr = ptr;
}

static void *
BIO_get_data (BIO *b)
{
   return b->ptr;
}

static void
BIO_set_init (BIO *b, int init)
{
   b->init = init;
}

BIO_METHOD *
mongoc_stream_tls_openssl_bio_meth_new (void)
{
   BIO_METHOD *meth = NULL;

   meth = &gMongocStreamTlsOpenSslRawMethods;
   return meth;
}

#else

BIO_METHOD *
mongoc_stream_tls_openssl_bio_meth_new (void)
{
   BIO_METHOD *meth = NULL;

   meth = BIO_meth_new (BIO_TYPE_FILTER, "mongoc-stream-tls-glue");
   if (meth) {
      BIO_meth_set_write (meth, mongoc_stream_tls_openssl_bio_write);
      BIO_meth_set_read (meth, mongoc_stream_tls_openssl_bio_read);
      BIO_meth_set_puts (meth, mongoc_stream_tls_openssl_bio_puts);
      BIO_meth_set_gets (meth, mongoc_stream_tls_openssl_bio_gets);
      BIO_meth_set_ctrl (meth, mongoc_stream_tls_openssl_bio_ctrl);
      BIO_meth_set_create (meth, mongoc_stream_tls_openssl_bio_create);
      BIO_meth_set_destroy (meth, mongoc_stream_tls_openssl_bio_destroy);
   }

   return meth;
}

#endif

void
mongoc_stream_tls_openssl_bio_set_data (BIO *b, void *ptr)
{
   BIO_set_data (b, ptr);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_create --
 *
 *       BIO callback to create a new BIO instance.
 *
 * Returns:
 *       1 if successful.
 *
 * Side effects:
 *       @b is initialized.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_create (BIO *b)
{
   BSON_ASSERT (b);

   BIO_set_init (b, 1);
   BIO_set_data (b, NULL);
   BIO_set_flags (b, 0);

   return 1;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_destroy --
 *
 *       Release resources associated with BIO.
 *
 * Returns:
 *       1 if successful.
 *
 * Side effects:
 *       @b is destroyed.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_destroy (BIO *b)
{
   mongoc_stream_tls_t *tls;

   BSON_ASSERT (b);

   tls = (mongoc_stream_tls_t *) BIO_get_data (b);

   if (!tls) {
      return -1;
   }

   BIO_set_data (b, NULL);
   BIO_set_init (b, 0);
   BIO_set_flags (b, 0);

   ((mongoc_stream_tls_openssl_t *) tls->ctx)->bio = NULL;

   return 1;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_read --
 *
 *       Read from the underlying stream to BIO.
 *
 * Returns:
 *       -1 on failure; otherwise the number of bytes read.
 *
 * Side effects:
 *       @buf is filled with data read from underlying stream.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_read (BIO *b, char *buf, int len)
{
   mongoc_stream_tls_t *tls;
   mongoc_stream_tls_openssl_t *openssl;

   BSON_ASSERT (b);
   BSON_ASSERT (buf);
   ENTRY;

   tls = (mongoc_stream_tls_t *) BIO_get_data (b);

   if (!tls) {
      RETURN (-1);
   }

   if (len < 0) {
      RETURN (-1);
   }

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, tls->timeout_msec))) {
      // CDRIVER-4589
      MONGOC_ERROR ("timeout_msec value %" PRId64 " exceeds supported 32-bit range", tls->timeout_msec);
      return -1;
   }

   openssl = (mongoc_stream_tls_openssl_t *) tls->ctx;

   errno = 0;
   const ssize_t ret = mongoc_stream_read (tls->base_stream, buf, (size_t) len, 0, (int32_t) tls->timeout_msec);
   BIO_clear_retry_flags (b);

   if ((ret <= 0) && MONGOC_ERRNO_IS_AGAIN (errno)) {
      /* this BIO is not the same as "b", which openssl passed in to this func.
       * set its retry flag, which we check with BIO_should_retry in
       * mongoc-stream-tls-openssl.c
       */
      BIO_set_retry_read (openssl->bio);
   }

   BSON_ASSERT (bson_in_range_signed (int, ret));

   RETURN ((int) ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_write --
 *
 *       Write to the underlying stream on behalf of BIO.
 *
 * Returns:
 *       -1 on failure; otherwise the number of bytes written.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_write (BIO *b, const char *buf, int len)
{
   mongoc_stream_tls_t *tls;
   mongoc_stream_tls_openssl_t *openssl;
   mongoc_iovec_t iov;
   ENTRY;

   BSON_ASSERT (b);
   BSON_ASSERT (buf);

   tls = (mongoc_stream_tls_t *) BIO_get_data (b);

   if (!tls) {
      RETURN (-1);
   }

   if (len < 0) {
      RETURN (-1);
   }

   openssl = (mongoc_stream_tls_openssl_t *) tls->ctx;

   iov.iov_base = (void *) buf;
   iov.iov_len = (size_t) len;

   if (BSON_UNLIKELY (!bson_in_range_signed (int32_t, tls->timeout_msec))) {
      // CDRIVER-4589
      MONGOC_ERROR ("timeout_msec value %" PRId64 " exceeds supported 32-bit range", tls->timeout_msec);
      RETURN (-1);
   }

   errno = 0;
   TRACE ("mongoc_stream_writev is expected to write: %d", len);
   const ssize_t ret = mongoc_stream_writev (tls->base_stream, &iov, 1, (int32_t) tls->timeout_msec);
   BIO_clear_retry_flags (b);

   if (len > ret) {
      TRACE ("Returned short write: %zd of %d", ret, len);
   } else {
      TRACE ("Completed the %zd", ret);
   }
   if (ret <= 0 && MONGOC_ERRNO_IS_AGAIN (errno)) {
      /* this BIO is not the same as "b", which openssl passed in to this func.
       * set its retry flag, which we check with BIO_should_retry in
       * mongoc-stream-tls-openssl.c
       */
      TRACE ("%s", "Requesting a retry");
      BIO_set_retry_write (openssl->bio);
   }

   BSON_ASSERT (bson_in_range_signed (int, ret));

   RETURN ((int) ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_ctrl --
 *
 *       Handle ctrl callback for BIO.
 *
 * Returns:
 *       ioctl dependent.
 *
 * Side effects:
 *       ioctl dependent.
 *
 *--------------------------------------------------------------------------
 */

long
mongoc_stream_tls_openssl_bio_ctrl (BIO *b, int cmd, long num, void *ptr)
{
   BSON_UNUSED (b);
   BSON_UNUSED (num);
   BSON_UNUSED (ptr);

   switch (cmd) {
   case BIO_CTRL_FLUSH:
      return 1;
   default:
      return 0;
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_gets --
 *
 *       BIO callback for gets(). Not supported.
 *
 * Returns:
 *       -1 always.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_gets (BIO *b, char *buf, int len)
{
   BSON_UNUSED (b);
   BSON_UNUSED (buf);
   BSON_UNUSED (len);

   return -1;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_tls_openssl_bio_puts --
 *
 *       BIO callback to perform puts(). Just calls the actual write
 *       callback.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_stream_tls_openssl_bio_puts (BIO *b, const char *str)
{
   return mongoc_stream_tls_openssl_bio_write (b, str, (int) strlen (str));
}


#endif /* MONGOC_ENABLE_SSL_OPENSSL */
