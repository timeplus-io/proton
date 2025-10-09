/*
 * Copyright 2020-present MongoDB, Inc.
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

#include "mongoc/mongoc-errno-private.h"
#include "mongoc/mongoc-interrupt-private.h"
#include "mongoc/mongoc-log.h"
#include "mongoc/mongoc-socket-private.h"
#include "mongoc/mongoc-stream-socket.h"
#include "mongoc/mongoc-trace-private.h"
#include "common-thread-private.h"

/* The interrupt stream is implemented in two ways.
 * On POSIX, this uses the self-pipe trick.
 * On Windows, this uses a pair of TCP sockets.
 */
struct _mongoc_interrupt_t {
   bson_mutex_t mutex;

   union {
      /* For POSIX. pipe_fds[0] is the read end and pipe_fds[1] is the write
       * end. */
      int pipe_fds[2];

      /* For Windows */
      struct {
         mongoc_socket_t *read;
         mongoc_socket_t *write;
      } socket_pair;
   } impl;

   mongoc_stream_t *stream;
};

mongoc_stream_t *
_mongoc_interrupt_get_stream (mongoc_interrupt_t *interrupt)
{
   return interrupt->stream;
}

static void
_log_errno (char *prefix, int _errno)
{
   char buf[128] = {0};

   bson_strerror_r (_errno, buf, sizeof (buf));
   MONGOC_ERROR ("%s: (%d) %s", prefix, _errno, buf);
}

#ifdef _WIN32
/* TCP socket pair implementation. */
mongoc_interrupt_t *
_mongoc_interrupt_new (uint32_t timeout_ms)
{
   mongoc_interrupt_t *interrupt;
   mongoc_socket_t *listen_socket = NULL;
   mongoc_socket_t *interrupt_socket = NULL;
   struct sockaddr_storage server_addr;
   mongoc_socklen_t sock_len;
   int ret;
   bool success = false;
   struct sockaddr_in server_addr_in = {0};

   ENTRY;

   interrupt = (mongoc_interrupt_t *) bson_malloc0 (sizeof *interrupt);
   bson_mutex_init (&interrupt->mutex);

   /* Inspired by cpython's implementation of socketpair. */
   listen_socket = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   if (!listen_socket) {
      MONGOC_ERROR ("socket creation failed");
      GOTO (fail);
   }

   memset (&server_addr_in, 0, sizeof (server_addr_in));
   server_addr_in.sin_family = AF_INET;
   server_addr_in.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
   ret = mongoc_socket_bind (listen_socket, (struct sockaddr *) &server_addr_in, sizeof (server_addr_in));
   if (ret == -1) {
      _log_errno ("bind failed", mongoc_socket_errno (listen_socket));
      GOTO (fail);
   }

   ret = mongoc_socket_listen (listen_socket, 1);
   if (ret == -1) {
      _log_errno ("listen failed", mongoc_socket_errno (listen_socket));
      GOTO (fail);
   }

   sock_len = sizeof (server_addr);
   ret = mongoc_socket_getsockname (listen_socket, (struct sockaddr *) &server_addr, &sock_len);
   if (-1 == ret) {
      _log_errno ("getsockname failed", mongoc_socket_errno (listen_socket));
      GOTO (fail);
   }

   interrupt->impl.socket_pair.read = mongoc_socket_new (server_addr.ss_family, SOCK_STREAM, 0);
   if (!interrupt->impl.socket_pair.read) {
      MONGOC_ERROR ("socket creation failed");
      GOTO (fail);
   }

   /* Begin non-blocking connect. */
   ret = mongoc_socket_connect (interrupt->impl.socket_pair.read, (struct sockaddr *) &server_addr, sock_len, 0);
   if (ret == -1 && !MONGOC_ERRNO_IS_AGAIN (mongoc_socket_errno (interrupt->impl.socket_pair.read))) {
      _log_errno ("connect failed", mongoc_socket_errno (interrupt->impl.socket_pair.read));
      GOTO (fail);
   }

   interrupt->impl.socket_pair.write =
      mongoc_socket_accept (listen_socket, bson_get_monotonic_time () + timeout_ms * 1000);
   if (!interrupt->impl.socket_pair.write) {
      _log_errno ("accept failed", mongoc_socket_errno (listen_socket));
      GOTO (fail);
   }

   /* Create an unowned socket. interrupt_socket has 0 for the pid, so it will
    * be considered unowned. */
   interrupt_socket = bson_malloc0 (sizeof (mongoc_socket_t));
   interrupt_socket->sd = interrupt->impl.socket_pair.read->sd;
   /* Creating the stream takes ownership of the mongoc_socket_t. */
   interrupt->stream = mongoc_stream_socket_new (interrupt_socket);
   success = true;
fail:
   mongoc_socket_destroy (listen_socket);
   if (!success) {
      _mongoc_interrupt_destroy (interrupt);
      interrupt = NULL;
   }
   RETURN (interrupt);
}

void
_mongoc_interrupt_destroy (mongoc_interrupt_t *interrupt)
{
   if (!interrupt) {
      return;
   }

   bson_mutex_destroy (&interrupt->mutex);
   mongoc_socket_destroy (interrupt->impl.socket_pair.read);
   mongoc_socket_destroy (interrupt->impl.socket_pair.write);
   mongoc_stream_destroy (interrupt->stream);
   bson_free (interrupt);
}

bool
_mongoc_interrupt_flush (mongoc_interrupt_t *interrupt)
{
   uint8_t buf[1];
   while (true) {
      if (-1 == mongoc_socket_recv (interrupt->impl.socket_pair.read, buf, sizeof (buf), 0, 0)) {
         if (MONGOC_ERRNO_IS_AGAIN (errno)) {
            /* Nothing left to read. */
            return true;
         } else {
            /* Unexpected error. */
            _log_errno ("interrupt recv failed", mongoc_socket_errno (interrupt->impl.socket_pair.read));
            return false;
         }
      }
   }
   /* Should never be reached. */
   BSON_ASSERT (false);
}

bool
_mongoc_interrupt_interrupt (mongoc_interrupt_t *interrupt)
{
   bson_mutex_lock (&interrupt->mutex);
   if (mongoc_socket_send (interrupt->impl.socket_pair.write, "!", 1, 0) == -1 && !MONGOC_ERRNO_IS_AGAIN (errno)) {
      _log_errno ("interrupt send failed", mongoc_socket_errno (interrupt->impl.socket_pair.write));
      bson_mutex_unlock (&interrupt->mutex);
      return false;
   }
   bson_mutex_unlock (&interrupt->mutex);
   return true;
}

#else
/* Pipe implementation. */

/* Set non-blocking and close on exec. */
static bool
_set_pipe_flags (int pipe_fd)
{
   int flags;

   flags = fcntl (pipe_fd, F_GETFL);

   if (-1 == fcntl (pipe_fd, F_SETFL, (flags | O_NONBLOCK))) {
      return false;
   }

#ifdef FD_CLOEXEC
   flags = fcntl (pipe_fd, F_GETFD);
   if (-1 == fcntl (pipe_fd, F_SETFD, (flags | FD_CLOEXEC))) {
      return false;
   }
#endif
   return true;
}

mongoc_interrupt_t *
_mongoc_interrupt_new (uint32_t timeout_ms)
{
   mongoc_interrupt_t *interrupt;
   mongoc_socket_t *interrupt_socket = NULL;
   bool success = false;

   ENTRY;

   BSON_UNUSED (timeout_ms);

   interrupt = (mongoc_interrupt_t *) bson_malloc0 (sizeof *interrupt);
   bson_mutex_init (&interrupt->mutex);

   if (0 != pipe (interrupt->impl.pipe_fds)) {
      _log_errno ("pipe creation failed", errno);
      GOTO (fail);
   }

   /* Make the pipe non-blocking and close-on-exec. */
   if (!_set_pipe_flags (interrupt->impl.pipe_fds[0]) || !_set_pipe_flags (interrupt->impl.pipe_fds[1])) {
      _log_errno ("unable to configure pipes", errno);
   }

   /* Create an unowned socket. interrupt_socket has 0 for the pid, so it will
    * be considered unowned. */
   interrupt_socket = bson_malloc0 (sizeof (mongoc_socket_t));
   interrupt_socket->sd = interrupt->impl.pipe_fds[0];
   /* Creating the stream takes ownership of the mongoc_socket_t. */
   interrupt->stream = mongoc_stream_socket_new (interrupt_socket);

   success = true;
fail:
   if (!success) {
      _mongoc_interrupt_destroy (interrupt);
      interrupt = NULL;
   }
   RETURN (interrupt);
}

bool
_mongoc_interrupt_flush (mongoc_interrupt_t *interrupt)
{
   char c;
   while (true) {
      if (read (interrupt->impl.pipe_fds[0], &c, 1) == -1) {
         if (MONGOC_ERRNO_IS_AGAIN (errno)) {
            /* Nothing left to read. */
            return true;
         } else {
            /* Unexpected error. */
            MONGOC_ERROR ("failed to read from pipe: %d", errno);
            return false;
         }
      }
   }

   BSON_UNREACHABLE ("reached unreachable code");
}

bool
_mongoc_interrupt_interrupt (mongoc_interrupt_t *interrupt)
{
   bson_mutex_lock (&interrupt->mutex);
   if (write (interrupt->impl.pipe_fds[1], "!", 1) == -1 && !MONGOC_ERRNO_IS_AGAIN (errno)) {
      MONGOC_ERROR ("failed to write to pipe: %d", errno);
      bson_mutex_unlock (&interrupt->mutex);
      return false;
   }
   bson_mutex_unlock (&interrupt->mutex);
   return true;
}

void
_mongoc_interrupt_destroy (mongoc_interrupt_t *interrupt)
{
   if (!interrupt) {
      return;
   }
   bson_mutex_destroy (&interrupt->mutex);
   if (interrupt->impl.pipe_fds[0]) {
      close (interrupt->impl.pipe_fds[0]);
   }
   if (interrupt->impl.pipe_fds[1]) {
      close (interrupt->impl.pipe_fds[1]);
   }
   mongoc_stream_destroy (interrupt->stream);
   bson_free (interrupt);
}
#endif
