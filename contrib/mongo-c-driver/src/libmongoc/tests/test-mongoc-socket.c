#include <fcntl.h>
#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>

#include "mongoc/mongoc-socket-private.h"
#include "mongoc/mongoc-thread-private.h"
#include "mongoc/mongoc-errno-private.h"
#include "TestSuite.h"

#include "test-libmongoc.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "socket-test"

#define TIMEOUT 10000
#define WAIT 1000


static size_t gFourMB = 1024 * 1024 * 4;

typedef struct {
   unsigned short server_port;
   mongoc_cond_t cond;
   bson_mutex_t cond_mutex;
   bool closed_socket;
   int amount;
   int32_t server_sleep_ms;
} socket_test_data_t;


static BSON_THREAD_FUN (socket_test_server, data_)
{
   socket_test_data_t *data = (socket_test_data_t *) data_;
   struct sockaddr_in server_addr = {0};
   mongoc_socket_t *listen_sock;
   mongoc_socket_t *conn_sock;
   mongoc_stream_t *stream;
   mongoc_iovec_t iov;
   mongoc_socklen_t sock_len;
   ssize_t r;
   char buf[5];

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   listen_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   BSON_ASSERT (listen_sock);

   server_addr.sin_family = AF_INET;
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
   server_addr.sin_port = htons (0);

   r = mongoc_socket_bind (listen_sock, (struct sockaddr *) &server_addr, sizeof server_addr);
   BSON_ASSERT (r == 0);

   sock_len = sizeof (server_addr);
   r = mongoc_socket_getsockname (listen_sock, (struct sockaddr *) &server_addr, &sock_len);
   BSON_ASSERT (r == 0);

   r = mongoc_socket_listen (listen_sock, 10);
   BSON_ASSERT (r == 0);

   bson_mutex_lock (&data->cond_mutex);
   data->server_port = ntohs (server_addr.sin_port);
   mongoc_cond_signal (&data->cond);
   bson_mutex_unlock (&data->cond_mutex);

   conn_sock = mongoc_socket_accept (listen_sock, -1);
   BSON_ASSERT (conn_sock);

   stream = mongoc_stream_socket_new (conn_sock);
   BSON_ASSERT (stream);

   r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
   BSON_ASSERT (r == 5);
   BSON_ASSERT (strcmp (buf, "ping") == 0);

   strcpy (buf, "pong");

   _mongoc_usleep (data->server_sleep_ms * 1000);
   r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);

   /* if we sleep the client times out, else assert the client reads the data */
   if (data->server_sleep_ms == 0) {
      BSON_ASSERT (r == 5);
   }

   mongoc_stream_destroy (stream);

   bson_mutex_lock (&data->cond_mutex);
   data->closed_socket = true;
   mongoc_cond_signal (&data->cond);
   bson_mutex_unlock (&data->cond_mutex);

   mongoc_socket_destroy (listen_sock);

   BSON_THREAD_RETURN;
}


static BSON_THREAD_FUN (socket_test_client, data_)
{
   socket_test_data_t *data = (socket_test_data_t *) data_;
   int64_t start;
   mongoc_socket_t *conn_sock;
   char buf[5];
   ssize_t r;
   bool closed;
   struct sockaddr_in server_addr = {0};
   mongoc_stream_t *stream;
   mongoc_iovec_t iov;

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   conn_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   BSON_ASSERT (conn_sock);

   bson_mutex_lock (&data->cond_mutex);
   while (!data->server_port) {
      mongoc_cond_wait (&data->cond, &data->cond_mutex);
   }
   bson_mutex_unlock (&data->cond_mutex);

   server_addr.sin_family = AF_INET;
   server_addr.sin_port = htons (data->server_port);
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);

   r = mongoc_socket_connect (conn_sock, (struct sockaddr *) &server_addr, sizeof (server_addr), -1);
   BSON_ASSERT (r == 0);

   stream = mongoc_stream_socket_new (conn_sock);

   strcpy (buf, "ping");

   closed = mongoc_stream_check_closed (stream);
   BSON_ASSERT (closed == false);

   r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);
   BSON_ASSERT (r == 5);

   closed = mongoc_stream_check_closed (stream);
   BSON_ASSERT (closed == false);

   if (data->server_sleep_ms == 0) {
      r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
      BSON_ASSERT (r == 5);
      BSON_ASSERT (strcmp (buf, "pong") == 0);

      bson_mutex_lock (&data->cond_mutex);
      while (!data->closed_socket) {
         mongoc_cond_wait (&data->cond, &data->cond_mutex);
      }
      bson_mutex_unlock (&data->cond_mutex);

      /* wait up to a second for the client to detect server's shutdown */
      start = bson_get_monotonic_time ();
      while (!mongoc_stream_check_closed (stream)) {
         ASSERT_CMPINT64 (bson_get_monotonic_time (), <, start + 1000 * 1000);
         _mongoc_usleep (1000);
      }
      BSON_ASSERT (!mongoc_stream_timed_out (stream));
   } else {
      r = mongoc_stream_readv (stream, &iov, 1, 5, data->server_sleep_ms / 2);
      ASSERT_CMPSSIZE_T (r, ==, (ssize_t) -1);
      BSON_ASSERT (mongoc_stream_timed_out (stream));
   }

   mongoc_stream_destroy (stream);

   BSON_THREAD_RETURN;
}


static BSON_THREAD_FUN (sendv_test_server, data_)
{
   socket_test_data_t *data = (socket_test_data_t *) data_;
   struct sockaddr_in server_addr = {0};
   mongoc_socket_t *listen_sock;
   mongoc_socket_t *conn_sock;
   mongoc_stream_t *stream;
   mongoc_iovec_t iov;

   char *buf = (char *) bson_malloc (gFourMB);

   iov.iov_base = buf;
   iov.iov_len = gFourMB;

   listen_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   BSON_ASSERT (listen_sock);

   server_addr.sin_family = AF_INET;
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
   server_addr.sin_port = htons (0);

   {
      const int r = mongoc_socket_bind (listen_sock, (struct sockaddr *) &server_addr, sizeof server_addr);
      ASSERT_CMPINT (r, ==, 0);
   }

   {
      mongoc_socklen_t sock_len = (mongoc_socklen_t) sizeof (server_addr);
      const int r = mongoc_socket_getsockname (listen_sock, (struct sockaddr *) &server_addr, &sock_len);
      ASSERT_CMPINT (r, ==, 0);
   }

   {
      const int r = mongoc_socket_listen (listen_sock, 10);
      ASSERT_CMPINT (r, ==, 0);
   }

   bson_mutex_lock (&data->cond_mutex);
   data->server_port = ntohs (server_addr.sin_port);
   mongoc_cond_signal (&data->cond);
   bson_mutex_unlock (&data->cond_mutex);

   conn_sock = mongoc_socket_accept (listen_sock, -1);
   BSON_ASSERT (conn_sock);

   stream = mongoc_stream_socket_new (conn_sock);
   BSON_ASSERT (stream);

   /* Wait until the client has pushed so much data he can't write more */
   bson_mutex_lock (&data->cond_mutex);
   while (!data->amount) {
      mongoc_cond_wait (&data->cond, &data->cond_mutex);
   }
   int amount = data->amount;
   data->amount = 0;
   bson_mutex_unlock (&data->cond_mutex);

   /* Start reading everything off the socket to unblock the client */
   do {
      ASSERT (bson_in_range_signed (size_t, amount));
      const ssize_t r = mongoc_stream_readv (stream, &iov, 1, (size_t) amount, WAIT);
      if (r > 0) {
         ASSERT (bson_in_range_signed (int, r));
         amount -= (int) r;
      }
   } while (amount > 0);

   /* Allow the client to finish all its writes */
   bson_mutex_lock (&data->cond_mutex);
   while (!data->amount) {
      mongoc_cond_wait (&data->cond, &data->cond_mutex);
   }
   /* amount is likely negative value now, we've read more then caused the
    * original blocker */
   amount += data->amount;
   data->amount = 0;
   bson_mutex_unlock (&data->cond_mutex);

   do {
      ASSERT (bson_in_range_signed (size_t, amount));
      const ssize_t r = mongoc_stream_readv (stream, &iov, 1, (size_t) amount, WAIT);
      if (r > 0) {
         ASSERT (bson_in_range_signed (int, r));
         amount -= (int) r;
      }
   } while (amount > 0);
   ASSERT_CMPINT (0, ==, amount);

   bson_free (buf);
   mongoc_stream_destroy (stream);
   mongoc_socket_destroy (listen_sock);

   BSON_THREAD_RETURN;
}


static BSON_THREAD_FUN (sendv_test_client, data_)
{
   socket_test_data_t *data = (socket_test_data_t *) data_;
   mongoc_socket_t *conn_sock;
   struct sockaddr_in server_addr = {0};
   mongoc_iovec_t iov;
   bool done = false;
   char *buf = (char *) bson_malloc (gFourMB);

   BSON_ASSERT (gFourMB > 0);
   memset (buf, 'a', (gFourMB) -1);
   buf[gFourMB - 1] = '\0';

   iov.iov_base = buf;
   iov.iov_len = gFourMB;

   conn_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   BSON_ASSERT (conn_sock);

   bson_mutex_lock (&data->cond_mutex);
   while (!data->server_port) {
      mongoc_cond_wait (&data->cond, &data->cond_mutex);
   }
   bson_mutex_unlock (&data->cond_mutex);

   server_addr.sin_family = AF_INET;
   server_addr.sin_port = htons (data->server_port);
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);

   {
      const ssize_t r = mongoc_socket_connect (conn_sock, (struct sockaddr *) &server_addr, sizeof (server_addr), -1);
      ASSERT_CMPSSIZE_T (r, ==, 0);
   }

   mongoc_stream_t *const stream = mongoc_stream_socket_new (conn_sock);

   int amount = 0;

   for (int i = 0; i < 5; i++) {
      const ssize_t r = mongoc_stream_writev (stream, &iov, 1, WAIT);

      if (r > 0) {
         BSON_ASSERT (bson_in_range_signed (int, r));
         amount += (int) r;
      }

      if (bson_cmp_not_equal_su (r, gFourMB)) {
         if (!done) {
            bson_mutex_lock (&data->cond_mutex);
            data->amount = amount;
            amount = 0;
            mongoc_cond_signal (&data->cond);
            bson_mutex_unlock (&data->cond_mutex);
            done = true;
         }
      }
   }
   BSON_ASSERT (true == done);
   bson_mutex_lock (&data->cond_mutex);
   data->amount = amount;
   mongoc_cond_signal (&data->cond);
   bson_mutex_unlock (&data->cond_mutex);

   mongoc_stream_destroy (stream);
   bson_free (buf);

   BSON_THREAD_RETURN;
}


static void
_test_mongoc_socket_check_closed (int32_t server_sleep_ms)
{
   socket_test_data_t data = {0};
   bson_thread_t threads[2];
   int i, r;

   bson_mutex_init (&data.cond_mutex);
   mongoc_cond_init (&data.cond);
   data.server_sleep_ms = server_sleep_ms;

   r = mcommon_thread_create (threads, &socket_test_server, &data);
   BSON_ASSERT (r == 0);

   r = mcommon_thread_create (threads + 1, &socket_test_client, &data);
   BSON_ASSERT (r == 0);

   for (i = 0; i < 2; i++) {
      r = mcommon_thread_join (threads[i]);
      BSON_ASSERT (r == 0);
   }

   bson_mutex_destroy (&data.cond_mutex);
   mongoc_cond_destroy (&data.cond);
}


static void
test_mongoc_socket_check_closed (void)
{
   _test_mongoc_socket_check_closed (0);
}


static void
test_mongoc_socket_timed_out (void *ctx)
{
   BSON_UNUSED (ctx);

   _test_mongoc_socket_check_closed (1000);
}


static void
test_mongoc_socket_sendv (void *ctx)
{
   socket_test_data_t data = {0};
   bson_thread_t threads[2];
   int i, r;

   BSON_UNUSED (ctx);

   bson_mutex_init (&data.cond_mutex);
   mongoc_cond_init (&data.cond);

   r = mcommon_thread_create (threads, &sendv_test_server, &data);
   BSON_ASSERT (r == 0);

   r = mcommon_thread_create (threads + 1, &sendv_test_client, &data);
   BSON_ASSERT (r == 0);

   for (i = 0; i < 2; i++) {
      r = mcommon_thread_join (threads[i]);
      BSON_ASSERT (r == 0);
   }

   bson_mutex_destroy (&data.cond_mutex);
   mongoc_cond_destroy (&data.cond);
}

static void
test_mongoc_socket_poll_refusal (void *ctx)
{
   mongoc_stream_poll_t *poller;
   mongoc_socket_t *sock;
   mongoc_stream_t *ssock;
   int64_t start;
   struct sockaddr_in ipv4_addr = {0};

   BSON_UNUSED (ctx);

   ipv4_addr.sin_family = AF_INET;
   BSON_ASSERT (inet_pton (AF_INET, "127.0.0.1", &ipv4_addr.sin_addr));
   ipv4_addr.sin_port = htons (12345);

   /* create a new non-blocking socket. */
   sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);

   (void) mongoc_socket_connect (sock, (struct sockaddr *) &ipv4_addr, sizeof (ipv4_addr), 0);

   start = bson_get_monotonic_time ();

   ssock = mongoc_stream_socket_new (sock);

   poller = bson_malloc0 (sizeof (*poller));
   poller->revents = 0;
   poller->events = POLLOUT | POLLERR | POLLHUP;
   poller->stream = ssock;

   while (bson_get_monotonic_time () - start < 5000 * 1000) {
      BSON_ASSERT (mongoc_stream_poll (poller, 1, 10 * 1000) > 0);
      if (poller->revents & POLLHUP) {
         break;
      }
   }

   mongoc_stream_destroy (ssock);
   bson_free (poller);

#ifdef _WIN32
   ASSERT_WITHIN_TIME_INTERVAL ((int) (bson_get_monotonic_time () - start), 1000 * 500, 1500 * 1000);
#else
   ASSERT_WITHIN_TIME_INTERVAL ((int) (bson_get_monotonic_time () - start), 0, 500);
#endif
}

void
test_socket_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Socket/check_closed", test_mongoc_socket_check_closed);
   TestSuite_AddFull (
      suite, "/Socket/timed_out", test_mongoc_socket_timed_out, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddFull (suite, "/Socket/sendv", test_mongoc_socket_sendv, NULL, NULL, test_framework_skip_if_slow);
   TestSuite_AddFull (
      suite, "/Socket/connect_refusal", test_mongoc_socket_poll_refusal, NULL, NULL, test_framework_skip_if_slow);
}
