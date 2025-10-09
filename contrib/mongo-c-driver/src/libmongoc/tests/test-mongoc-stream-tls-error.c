#include <mongoc/mongoc.h>
#include <mongoc/mongoc-thread-private.h>
#include <mongoc/mongoc-util-private.h>
#include <mongoc/mongoc-stream-tls.h>

#ifdef MONGOC_ENABLE_SSL_OPENSSL
#include <openssl/err.h>
#endif

#include "ssl-test.h"
#include "test-conveniences.h"
#include "TestSuite.h"
#include "test-libmongoc.h"

#define TIMEOUT 10000 /* milliseconds */

#if !defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL) && !defined(MONGOC_ENABLE_SSL_LIBRESSL) && \
   !defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
/** run as a child thread by test_mongoc_tls_hangup
 *
 * It:
 *    1. spins up
 *    2. binds and listens to a random port
 *    3. notifies the client of its port through a condvar
 *    4. accepts a request
 *    5. reads a byte
 *    7. hangs up
 */
static BSON_THREAD_FUN (ssl_error_server, ptr)
{
   ssl_test_data_t *data = (ssl_test_data_t *) ptr;

   mongoc_stream_t *sock_stream;
   mongoc_stream_t *ssl_stream;
   mongoc_socket_t *listen_sock;
   mongoc_socket_t *conn_sock;
   mongoc_socklen_t sock_len;
   char buf;
   ssize_t r;
   mongoc_iovec_t iov;
   struct sockaddr_in server_addr = {0};
   bson_error_t error;

   iov.iov_base = &buf;
   iov.iov_len = 1;

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

   sock_stream = mongoc_stream_socket_new (conn_sock);
   BSON_ASSERT (sock_stream);

   ssl_stream = mongoc_stream_tls_new_with_hostname (sock_stream, data->host, data->server, 0);
   BSON_ASSERT (ssl_stream);

   switch (data->behavior) {
   case SSL_TEST_BEHAVIOR_STALL_BEFORE_HANDSHAKE:
      _mongoc_usleep (data->handshake_stall_ms * 1000);
      break;
   case SSL_TEST_BEHAVIOR_HANGUP_AFTER_HANDSHAKE:
      r = mongoc_stream_tls_handshake_block (ssl_stream, data->host, TIMEOUT, &error);
      BSON_ASSERT (r);

      r = mongoc_stream_readv (ssl_stream, &iov, 1, 1, TIMEOUT);
      BSON_ASSERT (r == 1);
      break;
   case SSL_TEST_BEHAVIOR_NORMAL:
   default:
      test_error ("unimplemented ssl_test_behavior_t");
   }

   data->server_result->result = SSL_TEST_SUCCESS;

   mongoc_stream_close (ssl_stream);
   mongoc_stream_destroy (ssl_stream);
   mongoc_socket_destroy (listen_sock);

   BSON_THREAD_RETURN;
}


#if !defined(__sun) && !defined(__APPLE__)
/** run as a child thread by test_mongoc_tls_hangup
 *
 * It:
 *    1. spins up
 *    2. waits on a condvar until the server is up
 *    3. connects to the server's port
 *    4. writes a byte
 *    5. confirms that the server hangs up promptly
 *    6. shuts down
 */
static BSON_THREAD_FUN (ssl_hangup_client, ptr)
{
   ssl_test_data_t *data = (ssl_test_data_t *) ptr;
   mongoc_stream_t *sock_stream;
   mongoc_stream_t *ssl_stream;
   mongoc_socket_t *conn_sock;
   char buf = 'b';
   ssize_t r;
   mongoc_iovec_t riov;
   mongoc_iovec_t wiov;
   struct sockaddr_in server_addr = {0};
   int64_t start_time;
   bson_error_t error;

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

   sock_stream = mongoc_stream_socket_new (conn_sock);
   BSON_ASSERT (sock_stream);

   ssl_stream = mongoc_stream_tls_new_with_hostname (sock_stream, data->host, data->client, 1);
   BSON_ASSERT (ssl_stream);

   r = mongoc_stream_tls_handshake_block (ssl_stream, data->host, TIMEOUT, &error);
   BSON_ASSERT (r);

   wiov.iov_base = (void *) &buf;
   wiov.iov_len = 1;
   r = mongoc_stream_writev (ssl_stream, &wiov, 1, TIMEOUT);
   BSON_ASSERT (r == 1);

   riov.iov_base = (void *) &buf;
   riov.iov_len = 1;

   /* we should notice promptly that the server hangs up */
   start_time = bson_get_monotonic_time ();
   r = mongoc_stream_readv (ssl_stream, &riov, 1, 1, TIMEOUT);
   /* time is in microseconds */
   BSON_ASSERT (bson_get_monotonic_time () - start_time < 1000 * 1000);
   BSON_ASSERT (r == -1);
   mongoc_stream_destroy (ssl_stream);
   data->client_result->result = SSL_TEST_SUCCESS;
   BSON_THREAD_RETURN;
}

static void
test_mongoc_tls_hangup (void)
{
   mongoc_ssl_opt_t sopt = {0};
   mongoc_ssl_opt_t copt = {0};
   ssl_test_result_t sr;
   ssl_test_result_t cr;
   ssl_test_data_t data = {0};
   bson_thread_t threads[2];
   int i, r;

   sopt.pem_file = CERT_SERVER;
   sopt.weak_cert_validation = 1;
   copt.weak_cert_validation = 1;

   data.server = &sopt;
   data.client = &copt;
   data.behavior = SSL_TEST_BEHAVIOR_HANGUP_AFTER_HANDSHAKE;
   data.server_result = &sr;
   data.client_result = &cr;
   data.host = "localhost";

   bson_mutex_init (&data.cond_mutex);
   mongoc_cond_init (&data.cond);

   r = mcommon_thread_create (threads, &ssl_error_server, &data);
   BSON_ASSERT (r == 0);

   r = mcommon_thread_create (threads + 1, &ssl_hangup_client, &data);
   BSON_ASSERT (r == 0);

   for (i = 0; i < 2; i++) {
      r = mcommon_thread_join (threads[i]);
      BSON_ASSERT (r == 0);
   }

   bson_mutex_destroy (&data.cond_mutex);
   mongoc_cond_destroy (&data.cond);

   ASSERT (cr.result == SSL_TEST_SUCCESS);
   ASSERT (sr.result == SSL_TEST_SUCCESS);
}
#endif


/** run as a child thread by test_mongoc_tls_handshake_stall
 *
 * It:
 *    1. spins up
 *    2. waits on a condvar until the server is up
 *    3. connects to the server's port
 *    4. attempts handshake
 *    5. confirms that it times out
 *    6. shuts down
 */
static BSON_THREAD_FUN (handshake_stall_client, ptr)
{
   ssl_test_data_t *data = (ssl_test_data_t *) ptr;
   char *uri_str;
   mongoc_client_t *client;
   bson_t reply;
   bson_error_t error;
   int64_t connect_timeout_ms = data->handshake_stall_ms - 100;
   int64_t duration_ms;

   int64_t start_time;

   bson_mutex_lock (&data->cond_mutex);
   while (!data->server_port) {
      mongoc_cond_wait (&data->cond, &data->cond_mutex);
   }
   bson_mutex_unlock (&data->cond_mutex);

   /* Note: do not use localhost here. If localhost has both A and AAAA records,
    * an attempt to connect to IPv6 occurs first. Most platforms refuse the IPv6
    * attempt immediately, so IPv4 succeeds immediately. Windows is an
    * exception, and waits 1 second before refusing:
    * https://support.microsoft.com/en-us/help/175523/info-winsock-tcp-connection-performance-to-unused-ports
    */
   uri_str = bson_strdup_printf ("mongodb://127.0.0.1:%u/"
                                 "?ssl=true&serverselectiontimeoutms=200&"
                                 "connecttimeoutms=%" PRId64,
                                 data->server_port,
                                 connect_timeout_ms);

   client = test_framework_client_new (uri_str, NULL);
   mongoc_client_set_ssl_opts (client, data->client);

   /* we should time out after about 200ms */
   start_time = bson_get_monotonic_time ();
   mongoc_client_read_command_with_opts (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &reply, &error);

   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "socket timeout");

   /* time is in microseconds */
   duration_ms = (bson_get_monotonic_time () - start_time) / 1000;

   if (llabs (duration_ms - connect_timeout_ms) > 100) {
      test_error ("expected timeout after about 200ms, not %" PRId64, duration_ms);
   }

   data->client_result->result = SSL_TEST_SUCCESS;

   bson_destroy (&reply);
   mongoc_client_destroy (client);
   bson_free (uri_str);

   BSON_THREAD_RETURN;
}


/* CDRIVER-2222 this should be reenabled for Apple Secure Transport too */
#if !defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
static void
test_mongoc_tls_handshake_stall (void)
{
   mongoc_ssl_opt_t sopt = {0};
   mongoc_ssl_opt_t copt = {0};
   ssl_test_result_t sr;
   ssl_test_result_t cr;
   ssl_test_data_t data = {0};
   bson_thread_t threads[2];
   int i, r;

   sopt.ca_file = CERT_CA;
   sopt.pem_file = CERT_SERVER;
   sopt.weak_cert_validation = 1;
   copt.ca_file = CERT_CA;
   copt.weak_cert_validation = 1;

   data.server = &sopt;
   data.client = &copt;
   data.behavior = SSL_TEST_BEHAVIOR_STALL_BEFORE_HANDSHAKE;
   data.handshake_stall_ms = 300;
   data.server_result = &sr;
   data.client_result = &cr;
   data.host = "localhost";

   bson_mutex_init (&data.cond_mutex);
   mongoc_cond_init (&data.cond);

   r = mcommon_thread_create (threads, &ssl_error_server, &data);
   BSON_ASSERT (r == 0);

   r = mcommon_thread_create (threads + 1, &handshake_stall_client, &data);
   BSON_ASSERT (r == 0);

   for (i = 0; i < 2; i++) {
      r = mcommon_thread_join (threads[i]);
      BSON_ASSERT (r == 0);
   }

   bson_mutex_destroy (&data.cond_mutex);
   mongoc_cond_destroy (&data.cond);

   ASSERT (cr.result == SSL_TEST_SUCCESS);
   ASSERT (sr.result == SSL_TEST_SUCCESS);
}

#endif /* !MONGOC_ENABLE_SSL_SECURE_TRANSPORT */
#endif /* !MONGOC_ENABLE_SSL_SECURE_CHANNEL && !MONGOC_ENABLE_SSL_LIBRESSL */

/* TLS stream should be NULL and base stream should still be valid, and error
 * messages should be consistent across TLS libs. Until CDRIVER-2844, just
 * assert message includes the filename, and handle NULL or non-NULL return. */
#define TLS_LOAD_ERR(_field)                                                          \
   do {                                                                               \
      (_field) = "badfile";                                                           \
      capture_logs (true);                                                            \
      base = mongoc_stream_socket_new (mongoc_socket_new (AF_INET, SOCK_STREAM, 0));  \
      tls_stream = mongoc_stream_tls_new_with_hostname (base, NULL, &opt, 0);         \
                                                                                      \
      ASSERT_CAPTURED_LOG ("bad TLS config file", MONGOC_LOG_LEVEL_ERROR, "badfile"); \
                                                                                      \
      if (tls_stream) {                                                               \
         mongoc_stream_destroy (tls_stream);                                          \
      } else {                                                                        \
         mongoc_stream_destroy (base);                                                \
      }                                                                               \
                                                                                      \
      opt.pem_file = opt.ca_file = opt.ca_dir = opt.crl_file = NULL;                  \
   } while (0)

static void
test_mongoc_tls_load_files (void)
{
   mongoc_ssl_opt_t opt = {0};
   mongoc_stream_t *base;
   mongoc_stream_t *tls_stream = NULL;

   TLS_LOAD_ERR (opt.pem_file);
   TLS_LOAD_ERR (opt.ca_file);
}


void
test_stream_tls_error_install (TestSuite *suite)
{
#if !defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL) && !defined(MONGOC_ENABLE_SSL_LIBRESSL)
#if !defined(__APPLE__)
   TestSuite_Add (suite, "/TLS/hangup", test_mongoc_tls_hangup);
#endif

/* see CDRIVER-2222 this occasionally stalls for a few 100ms on Mac */
#if !defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
   TestSuite_Add (suite, "/TLS/handshake_stall", test_mongoc_tls_handshake_stall);
#endif
#endif /* !MONGOC_ENABLE_SSL_SECURE_CHANNEL && !MONGOC_ENABLE_SSL_LIBRESSL */
   TestSuite_Add (suite, "/TLS/load_files", test_mongoc_tls_load_files);
}
