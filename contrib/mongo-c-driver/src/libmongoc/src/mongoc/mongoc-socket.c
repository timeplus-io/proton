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


#include <errno.h>
#include <string.h>

#include "mongoc-counters-private.h"
#include "mongoc-errno-private.h"
#include "mongoc-socket-private.h"
#include "mongoc-host-list.h"
#include "mongoc-socket-private.h"
#include "mongoc-trace-private.h"
#ifdef _WIN32
#include <Mstcpip.h>
#include <process.h>
#endif

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "socket"


#define OPERATION_EXPIRED(expire_at) ((expire_at >= 0) && (expire_at < (bson_get_monotonic_time ())))


/* either struct sockaddr or void, depending on platform */
typedef MONGOC_SOCKET_ARG2 mongoc_sockaddr_t;


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_capture_errno --
 *
 *       Save the errno state for contextual use.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static void
_mongoc_socket_capture_errno (mongoc_socket_t *sock) /* IN */
{
#ifdef _WIN32
   errno = sock->errno_ = WSAGetLastError ();
#else
   sock->errno_ = errno;
#endif
   TRACE ("setting errno: %d %s", sock->errno_, strerror (sock->errno_));
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_setflags --
 *
 *       A helper to set socket flags. Sets to nonblocking mode. On
 *       POSIX sets closeonexec.
 *
 *
 * Returns:
 *       true if successful; otherwise false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static bool
#ifdef _WIN32
_mongoc_socket_setflags (SOCKET sd)
#else
_mongoc_socket_setflags (int sd)
#endif
{
#ifdef _WIN32
   u_long io_mode = 1;
   return (NO_ERROR == ioctlsocket (sd, FIONBIO, &io_mode));
#else
   int flags;

   flags = fcntl (sd, F_GETFL);

   if (-1 == fcntl (sd, F_SETFL, (flags | O_NONBLOCK))) {
      return false;
   }

#ifdef FD_CLOEXEC
   flags = fcntl (sd, F_GETFD);
   if (-1 == fcntl (sd, F_SETFD, (flags | FD_CLOEXEC))) {
      return false;
   }
#endif
   return true;
#endif
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_wait --
 *
 *       A single socket poll helper.
 *
 *       @events: in most cases should be POLLIN or POLLOUT.
 *
 *       @expire_at should be an absolute time at which to expire using
 *       the monotonic clock (bson_get_monotonic_time(), which is in
 *       microseconds). Or zero to not block at all. Or -1 to block
 *       forever.
 *
 * Returns:
 *       true if an event matched. otherwise false.
 *       a timeout will return false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_socket_wait (mongoc_socket_t *sock, /* IN */
                     int events,            /* IN */
                     int64_t expire_at)     /* IN */
{
#ifdef _WIN32
   fd_set read_fds;
   fd_set write_fds;
   fd_set error_fds;
   struct timeval timeout_tv;
#else
   struct pollfd pfd;
#endif
   int ret;
   int timeout;
   int64_t now;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (events);

#ifdef _WIN32
   FD_ZERO (&read_fds);
   FD_ZERO (&write_fds);
   FD_ZERO (&error_fds);

   if (events & POLLIN) {
      FD_SET (sock->sd, &read_fds);
   }

   if (events & POLLOUT) {
      FD_SET (sock->sd, &write_fds);
   }

   FD_SET (sock->sd, &error_fds);
#else
   pfd.fd = sock->sd;
   pfd.events = events | POLLERR | POLLHUP;
   pfd.revents = 0;
#endif
   now = bson_get_monotonic_time ();

   for (;;) {
      if (expire_at < 0) {
         timeout = -1;
      } else if (expire_at == 0) {
         timeout = 0;
      } else {
         timeout = (int) ((expire_at - now) / 1000L);
         if (timeout < 0) {
            timeout = 0;
         }
      }

#ifdef _WIN32
      if (timeout == -1) {
         /* not WSAPoll: daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken */
         ret = select (0 /*unused*/, &read_fds, &write_fds, &error_fds, NULL);
      } else {
         timeout_tv.tv_sec = timeout / 1000;
         timeout_tv.tv_usec = (timeout % 1000) * 1000;
         ret = select (0 /*unused*/, &read_fds, &write_fds, &error_fds, &timeout_tv);
      }
      if (ret == SOCKET_ERROR) {
         _mongoc_socket_capture_errno (sock);
         ret = -1;
      } else if (FD_ISSET (sock->sd, &error_fds)) {
         errno = WSAECONNRESET;
         ret = -1;
      }
#else
      ret = poll (&pfd, 1, timeout);
#endif

      if (ret > 0) {
/* Something happened, so return that */
#ifdef _WIN32
         return (FD_ISSET (sock->sd, &read_fds) || FD_ISSET (sock->sd, &write_fds));
#else
         RETURN (0 != (pfd.revents & events));
#endif
      } else if (ret < 0) {
         /* poll itself failed */

         TRACE ("errno is: %d", errno);
         if (MONGOC_ERRNO_IS_AGAIN (errno)) {
            if (OPERATION_EXPIRED (expire_at)) {
               _mongoc_socket_capture_errno (sock);
               RETURN (false);
            } else {
               continue;
            }
         } else {
            /* poll failed for some non-transient reason */
            _mongoc_socket_capture_errno (sock);
            RETURN (false);
         }
      } else {
         /* ret == 0, poll timed out */
         if (timeout) {
            mongoc_counter_streams_timeout_inc ();
         }
#ifdef _WIN32
         sock->errno_ = timeout ? WSAETIMEDOUT : EAGAIN;
#else
         sock->errno_ = timeout ? ETIMEDOUT : EAGAIN;
#endif
         RETURN (false);
      }
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_poll --
 *
 *       A multi-socket poll helper.
 *
 *       @expire_at should be an absolute time at which to expire using
 *       the monotonic clock (bson_get_monotonic_time(), which is in
 *       microseconds). Or zero to not block at all. Or -1 to block
 *       forever.
 *
 * Returns:
 *       The number of sockets ready.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

ssize_t
mongoc_socket_poll (mongoc_socket_poll_t *sds, /* IN */
                    size_t nsds,               /* IN */
                    int32_t timeout)           /* IN */
{
#ifdef _WIN32
   fd_set read_fds;
   fd_set write_fds;
   fd_set error_fds;
   struct timeval timeout_tv;
#else
   struct pollfd *pfds;
#endif
   int ret;

   ENTRY;

   BSON_ASSERT (sds);

#ifdef _WIN32
   FD_ZERO (&read_fds);
   FD_ZERO (&write_fds);
   FD_ZERO (&error_fds);

   for (size_t i = 0u; i < nsds; i++) {
      if (sds[i].events & POLLIN) {
         FD_SET (sds[i].socket->sd, &read_fds);
      }

      if (sds[i].events & POLLOUT) {
         FD_SET (sds[i].socket->sd, &write_fds);
      }

      FD_SET (sds[i].socket->sd, &error_fds);
   }

   timeout_tv.tv_sec = timeout / 1000;
   timeout_tv.tv_usec = (timeout % 1000) * 1000;

   /* not WSAPoll: daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken */
   ret = select (0 /*unused*/, &read_fds, &write_fds, &error_fds, &timeout_tv);
   if (ret == SOCKET_ERROR) {
      errno = WSAGetLastError ();
      return -1;
   }

   for (size_t i = 0u; i < nsds; i++) {
      if (FD_ISSET (sds[i].socket->sd, &read_fds)) {
         sds[i].revents = POLLIN;
      } else if (FD_ISSET (sds[i].socket->sd, &write_fds)) {
         sds[i].revents = POLLOUT;
      } else if (FD_ISSET (sds[i].socket->sd, &error_fds)) {
         sds[i].revents = POLLHUP;
      } else {
         sds[i].revents = 0;
      }
   }
#else
   pfds = (struct pollfd *) bson_malloc (sizeof (*pfds) * nsds);

   for (size_t i = 0u; i < nsds; i++) {
      pfds[i].fd = sds[i].socket->sd;
      pfds[i].events = sds[i].events | POLLERR | POLLHUP;
      pfds[i].revents = 0;
   }

   ret = poll (pfds, nsds, timeout);
   for (size_t i = 0u; i < nsds; i++) {
      sds[i].revents = pfds[i].revents;
   }

   bson_free (pfds);
#endif

   return ret;
}


/* https://jira.mongodb.org/browse/CDRIVER-2176 */
#define MONGODB_KEEPALIVEINTVL 10
#define MONGODB_KEEPIDLE 120
#define MONGODB_KEEPALIVECNT 9

#ifdef _WIN32
static void
_mongoc_socket_setkeepalive_windows (SOCKET sd)
{
   struct tcp_keepalive keepalive;
   DWORD lpcbBytesReturned = 0;
   HKEY hKey;
   DWORD type;
   DWORD data;
   DWORD data_size = sizeof data;
   const char *reg_key = "SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters";
   keepalive.onoff = true;
   keepalive.keepalivetime = MONGODB_KEEPIDLE * 1000;
   keepalive.keepaliveinterval = MONGODB_KEEPALIVEINTVL * 1000;
   /*
    * Windows hardcodes probes to 10:
    * https://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
    * "On Windows Vista and later, the number of keep-alive probes (data
    * retransmissions) is set to 10 and cannot be changed."
    *
    * Note that win2k (and seeminly all versions thereafter) do not set the
    * registry value by default so there is no way to derive the default value
    * programmatically. It is however listed in the docs. A user can however
    * change the default value by setting the registry values.
    */

   if (RegOpenKeyExA (HKEY_LOCAL_MACHINE, reg_key, 0, KEY_QUERY_VALUE, &hKey) == ERROR_SUCCESS) {
      /* https://technet.microsoft.com/en-us/library/cc957549.aspx */
      DWORD default_keepalivetime = 7200000; /* 2 hours */
      /* https://technet.microsoft.com/en-us/library/cc957548.aspx */
      DWORD default_keepaliveinterval = 1000; /* 1 second */

      if (RegQueryValueEx (hKey, "KeepAliveTime", NULL, &type, (LPBYTE) &data, &data_size) == ERROR_SUCCESS) {
         if (type == REG_DWORD && data < keepalive.keepalivetime) {
            keepalive.keepalivetime = data;
         }
      } else if (default_keepalivetime < keepalive.keepalivetime) {
         keepalive.keepalivetime = default_keepalivetime;
      }

      if (RegQueryValueEx (hKey, "KeepAliveInterval", NULL, &type, (LPBYTE) &data, &data_size) == ERROR_SUCCESS) {
         if (type == REG_DWORD && data < keepalive.keepaliveinterval) {
            keepalive.keepaliveinterval = data;
         }
      } else if (default_keepaliveinterval < keepalive.keepaliveinterval) {
         keepalive.keepaliveinterval = default_keepaliveinterval;
      }
      RegCloseKey (hKey);
   }
   if (WSAIoctl (sd, SIO_KEEPALIVE_VALS, &keepalive, sizeof keepalive, NULL, 0, &lpcbBytesReturned, NULL, NULL) ==
       SOCKET_ERROR) {
      TRACE ("%s", "Could not set keepalive values");
   } else {
      TRACE ("%s", "KeepAlive values updated");
      TRACE ("KeepAliveTime: %lu", keepalive.keepalivetime);
      TRACE ("KeepAliveInterval: %lu", keepalive.keepaliveinterval);
   }
}
#else

static const char *
_mongoc_socket_sockopt_value_to_name (int value)
{
   switch (value) {
#ifdef TCP_KEEPIDLE
   case TCP_KEEPIDLE:
      return "TCP_KEEPIDLE";
#endif
#ifdef TCP_KEEPALIVE
   case TCP_KEEPALIVE:
      return "TCP_KEEPALIVE";
#endif
#ifdef TCP_KEEPINTVL
   case TCP_KEEPINTVL:
      return "TCP_KEEPINTVL";
#endif
#ifdef TCP_KEEPCNT
   case TCP_KEEPCNT:
      return "TCP_KEEPCNT";
#endif
   default:
      MONGOC_WARNING ("Don't know what socketopt %d is", value);
      return "Unknown option name";
   }
}

static void
_mongoc_socket_set_sockopt_if_less (int sd, int name, int value)
{
   int optval = 1;
   mongoc_socklen_t optlen;

   optlen = sizeof optval;
   if (getsockopt (sd, IPPROTO_TCP, name, (char *) &optval, &optlen)) {
      TRACE ("Getting '%s' failed, errno: %d", _mongoc_socket_sockopt_value_to_name (name), errno);
   } else {
      TRACE ("'%s' is %d, target value is %d", _mongoc_socket_sockopt_value_to_name (name), optval, value);
      if (optval > value) {
         optval = value;
         if (setsockopt (sd, IPPROTO_TCP, name, (char *) &optval, sizeof optval)) {
            TRACE ("Setting '%s' failed, errno: %d", _mongoc_socket_sockopt_value_to_name (name), errno);
         } else {
            TRACE ("'%s' value changed to %d", _mongoc_socket_sockopt_value_to_name (name), optval);
         }
      }
   }
}

static void
_mongoc_socket_setkeepalive_nix (int sd)
{
#if defined(TCP_KEEPIDLE)
   _mongoc_socket_set_sockopt_if_less (sd, TCP_KEEPIDLE, MONGODB_KEEPIDLE);
#elif defined(TCP_KEEPALIVE)
   _mongoc_socket_set_sockopt_if_less (sd, TCP_KEEPALIVE, MONGODB_KEEPIDLE);
#else
   TRACE ("%s", "Neither TCP_KEEPIDLE nor TCP_KEEPALIVE available");
#endif

#ifdef TCP_KEEPINTVL
   _mongoc_socket_set_sockopt_if_less (sd, TCP_KEEPINTVL, MONGODB_KEEPALIVEINTVL);
#else
   TRACE ("%s", "TCP_KEEPINTVL not available");
#endif

#ifdef TCP_KEEPCNT
   _mongoc_socket_set_sockopt_if_less (sd, TCP_KEEPCNT, MONGODB_KEEPALIVECNT);
#else
   TRACE ("%s", "TCP_KEEPCNT not available");
#endif
}

#endif
static void
#ifdef _WIN32
_mongoc_socket_setkeepalive (SOCKET sd) /* IN */
#else
_mongoc_socket_setkeepalive (int sd) /* IN */
#endif
{
#ifdef SO_KEEPALIVE
   int optval = 1;

   ENTRY;
#ifdef SO_KEEPALIVE
   if (!setsockopt (sd, SOL_SOCKET, SO_KEEPALIVE, (char *) &optval, sizeof optval)) {
      TRACE ("%s", "Setting SO_KEEPALIVE");
#ifdef _WIN32
      _mongoc_socket_setkeepalive_windows (sd);
#else
      _mongoc_socket_setkeepalive_nix (sd);
#endif
   } else {
      TRACE ("%s", "Failed setting SO_KEEPALIVE");
   }
#else
   TRACE ("%s", "SO_KEEPALIVE not available");
#endif
   EXIT;
#endif
}


static bool
#ifdef _WIN32
_mongoc_socket_setnodelay (SOCKET sd) /* IN */
#else
_mongoc_socket_setnodelay (int sd) /* IN */
#endif
{
#ifdef _WIN32
   BOOL optval = 1;
#else
   int optval = 1;
#endif
   int ret;

   ENTRY;

   errno = 0;
   ret = setsockopt (sd, IPPROTO_TCP, TCP_NODELAY, (char *) &optval, sizeof optval);

#ifdef _WIN32
   if (ret == SOCKET_ERROR) {
      MONGOC_WARNING ("WSAGetLastError(): %d", (int) WSAGetLastError ());
   }
#endif

   RETURN (ret == 0);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_errno --
 *
 *       Returns the last error on the socket.
 *
 * Returns:
 *       An integer errno, or 0 on no error.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_socket_errno (mongoc_socket_t *sock) /* IN */
{
   BSON_ASSERT (sock);
   TRACE ("Current errno: %d", sock->errno_);
   return sock->errno_;
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_errno_is_again --
 *
 *       Check to see if we should attempt to make further progress
 *       based on the error of the last operation.
 *
 * Returns:
 *       true if we should try again. otherwise false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_socket_errno_is_again (mongoc_socket_t *sock) /* IN */
{
   TRACE ("errno is: %d", sock->errno_);
   return MONGOC_ERRNO_IS_AGAIN (sock->errno_);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_accept --
 *
 *       Wrapper for BSD socket accept(). Handles portability between
 *       BSD sockets and WinSock2 on Windows Vista and newer.
 *
 * Returns:
 *       NULL upon failure to accept or timeout.
 *       A newly allocated mongoc_socket_t on success.
 *
 * Side effects:
 *       *port contains the client port number.
 *
 *--------------------------------------------------------------------------
 */

mongoc_socket_t *
mongoc_socket_accept (mongoc_socket_t *sock, /* IN */
                      int64_t expire_at)     /* IN */
{
   return mongoc_socket_accept_ex (sock, expire_at, NULL);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_accept_ex --
 *
 *       Private synonym for mongoc_socket_accept, returning client port.
 *
 * Returns:
 *       NULL upon failure to accept or timeout.
 *       A newly allocated mongoc_socket_t on success.
 *
 * Side effects:
 *       *port contains the client port number.
 *
 *--------------------------------------------------------------------------
 */

mongoc_socket_t *
mongoc_socket_accept_ex (mongoc_socket_t *sock, /* IN */
                         int64_t expire_at,     /* IN */
                         uint16_t *port)        /* OUT */
{
   mongoc_socket_t *client;
   struct sockaddr_storage addr = {0};
   mongoc_socklen_t addrlen = sizeof addr;
   bool try_again = false;
   bool failed = false;
#ifdef _WIN32
   SOCKET sd;
#else
   int sd;
#endif

   ENTRY;

   BSON_ASSERT (sock);

again:
   errno = 0;
   sd = accept (sock->sd, (mongoc_sockaddr_t *) &addr, &addrlen);

   _mongoc_socket_capture_errno (sock);
#ifdef _WIN32
   failed = (sd == INVALID_SOCKET);
#else
   failed = (sd == -1);
#endif
   try_again = (failed && _mongoc_socket_errno_is_again (sock));

   if (failed && try_again) {
      if (_mongoc_socket_wait (sock, POLLIN, expire_at)) {
         GOTO (again);
      }
      RETURN (NULL);
   } else if (failed) {
      RETURN (NULL);
   } else if (!_mongoc_socket_setflags (sd)) {
#ifdef _WIN32
      closesocket (sd);
#else
      close (sd);
#endif
      RETURN (NULL);
   }

   client = (mongoc_socket_t *) bson_malloc0 (sizeof *client);
   client->sd = sd;

   if (port) {
      if (addr.ss_family == AF_INET) {
         struct sockaddr_in *tmp = (struct sockaddr_in *) &addr;
         *port = ntohs (tmp->sin_port);
      } else {
         struct sockaddr_in6 *tmp = (struct sockaddr_in6 *) &addr;
         *port = ntohs (tmp->sin6_port);
      }
   }

   if (!_mongoc_socket_setnodelay (client->sd)) {
      MONGOC_WARNING ("Failed to enable TCP_NODELAY.");
   }

   RETURN (client);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongo_socket_bind --
 *
 *       A wrapper around bind().
 *
 * Returns:
 *       0 on success, -1 on failure and errno is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_socket_bind (mongoc_socket_t *sock,       /* IN */
                    const struct sockaddr *addr, /* IN */
                    mongoc_socklen_t addrlen)    /* IN */
{
   int ret;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (addr);
   BSON_ASSERT (addrlen);

   ret = bind (sock->sd, addr, addrlen);

   _mongoc_socket_capture_errno (sock);

   RETURN (ret);
}


int
mongoc_socket_close (mongoc_socket_t *sock) /* IN */
{
   bool owned;

   ENTRY;

   BSON_ASSERT (sock);

#ifdef _WIN32
   owned = (sock->pid == (int) _getpid ());

   if (sock->sd != INVALID_SOCKET) {
      if (owned) {
         shutdown (sock->sd, SD_BOTH);
      }

      if (0 == closesocket (sock->sd)) {
         sock->sd = INVALID_SOCKET;
      } else {
         _mongoc_socket_capture_errno (sock);
         RETURN (-1);
      }
   }
   RETURN (0);
#else
   owned = (sock->pid == (int) getpid ());

   if (sock->sd != -1) {
      if (owned) {
         shutdown (sock->sd, SHUT_RDWR);
      }

      if (0 == close (sock->sd)) {
         sock->sd = -1;
      } else {
         _mongoc_socket_capture_errno (sock);
         RETURN (-1);
      }
   }
   RETURN (0);
#endif
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_connect --
 *
 *       Performs a socket connection but will fail if @expire_at is
 *       reached by the monotonic clock.
 *
 * Returns:
 *       0 if success, otherwise -1 and errno is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_socket_connect (mongoc_socket_t *sock,       /* IN */
                       const struct sockaddr *addr, /* IN */
                       mongoc_socklen_t addrlen,    /* IN */
                       int64_t expire_at)           /* IN */
{
   bool try_again = false;
   bool failed = false;
   int ret;
   int optval;
   /* getsockopt parameter types vary, we check in CheckCompiler.m4 */
   mongoc_socklen_t optlen = (mongoc_socklen_t) sizeof optval;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (addr);
   BSON_ASSERT (addrlen);

   ret = connect (sock->sd, addr, addrlen);

#ifdef _WIN32
   if (ret == SOCKET_ERROR) {
#else
   if (ret == -1) {
#endif
      _mongoc_socket_capture_errno (sock);

      failed = true;
      try_again = _mongoc_socket_errno_is_again (sock);
   }

   if (failed && try_again) {
      if (_mongoc_socket_wait (sock, POLLOUT, expire_at)) {
         optval = -1;
         ret = getsockopt (sock->sd, SOL_SOCKET, SO_ERROR, (char *) &optval, &optlen);
         if ((ret == 0) && (optval == 0)) {
            RETURN (0);
         } else {
            errno = sock->errno_ = optval;
         }
      }
      RETURN (-1);
   } else if (failed) {
      RETURN (-1);
   } else {
      RETURN (0);
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_destroy --
 *
 *       Cleanup after a mongoc_socket_t structure, possibly closing
 *       underlying sockets.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       @sock is freed and should be considered invalid.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_socket_destroy (mongoc_socket_t *sock) /* IN */
{
   if (sock) {
      mongoc_socket_close (sock);
      bson_free (sock);
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_listen --
 *
 *       Listen for incoming requests with a backlog up to @backlog.
 *
 *       If @backlog is zero, a sensible default will be chosen.
 *
 * Returns:
 *       true if successful; otherwise false.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_socket_listen (mongoc_socket_t *sock, /* IN */
                      unsigned int backlog)  /* IN */
{
   int ret;

   ENTRY;

   BSON_ASSERT (sock);

   if (backlog == 0) {
      backlog = 10;
   }

   ret = listen (sock->sd, backlog);

   _mongoc_socket_capture_errno (sock);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_new --
 *
 *       Create a new socket and store the current process id on it.
 *
 *       Free the result with mongoc_socket_destroy().
 *
 * Returns:
 *       A newly allocated socket.
 *       NULL on failure.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_socket_t *
mongoc_socket_new (int domain,   /* IN */
                   int type,     /* IN */
                   int protocol) /* IN */
{
   mongoc_socket_t *sock;
#ifdef _WIN32
   SOCKET sd;
#else
   int sd;
#endif
#ifdef SO_NOSIGPIPE
   int on = 1;
#endif

   ENTRY;

   sd = socket (domain, type, protocol);

#ifdef _WIN32
   if (sd == INVALID_SOCKET) {
#else
   if (sd == -1) {
#endif
      RETURN (NULL);
   }

   if (!_mongoc_socket_setflags (sd)) {
      GOTO (fail);
   }

   if (domain != AF_UNIX) {
      if (!_mongoc_socket_setnodelay (sd)) {
         MONGOC_WARNING ("Failed to enable TCP_NODELAY.");
      }
      _mongoc_socket_setkeepalive (sd);
   }

   /* Set SO_NOSIGPIPE, to ignore SIGPIPE on writes for platforms where
      setting MSG_NOSIGNAL on writes is not supported (primarily OSX). */
#ifdef SO_NOSIGPIPE
   setsockopt (sd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof (on));
#endif

   sock = (mongoc_socket_t *) bson_malloc0 (sizeof *sock);
   sock->sd = sd;
   sock->domain = domain;
#ifdef _WIN32
   sock->pid = (int) _getpid ();
#else
   sock->pid = (int) getpid ();
#endif

   RETURN (sock);

fail:
#ifdef _WIN32
   closesocket (sd);
#else
   close (sd);
#endif

   RETURN (NULL);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_recv --
 *
 *       A portable wrapper around recv() that also respects an absolute
 *       timeout.
 *
 *       @expire_at is 0 for no blocking, -1 for infinite blocking,
 *       or a time using the monotonic clock to expire. Calculate this
 *       using bson_get_monotonic_time() + N_MICROSECONDS.
 *
 * Returns:
 *       The number of bytes received on success.
 *       0 on end of stream.
 *       -1 on failure.
 *
 * Side effects:
 *       @buf will be read into.
 *
 *--------------------------------------------------------------------------
 */

ssize_t
mongoc_socket_recv (mongoc_socket_t *sock, /* IN */
                    void *buf,             /* OUT */
                    size_t buflen,         /* IN */
                    int flags,             /* IN */
                    int64_t expire_at)     /* IN */
{
   ssize_t ret = 0;
   bool failed = false;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (buf);
   BSON_ASSERT (buflen);

again:
   sock->errno_ = 0;
#ifdef _WIN32
   ret = recv (sock->sd, (char *) buf, (int) buflen, flags);
   failed = (ret == SOCKET_ERROR);
#else
   ret = recv (sock->sd, buf, buflen, flags);
   failed = (ret == -1);
#endif
   if (failed) {
      _mongoc_socket_capture_errno (sock);
      if (_mongoc_socket_errno_is_again (sock) && _mongoc_socket_wait (sock, POLLIN, expire_at)) {
         GOTO (again);
      }
   }

   if (failed) {
      RETURN (-1);
   }

   mongoc_counter_streams_ingress_add (ret);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_setsockopt --
 *
 *       A wrapper around setsockopt().
 *
 * Returns:
 *       0 on success, -1 on failure.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

int
mongoc_socket_setsockopt (mongoc_socket_t *sock,   /* IN */
                          int level,               /* IN */
                          int optname,             /* IN */
                          const void *optval,      /* IN */
                          mongoc_socklen_t optlen) /* IN */
{
   int ret;

   ENTRY;

   BSON_ASSERT (sock);

   ret = setsockopt (sock->sd, level, optname, optval, optlen);

   _mongoc_socket_capture_errno (sock);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_send --
 *
 *       A simplified wrapper around mongoc_socket_sendv().
 *
 *       @expire_at is 0 for no blocking, -1 for infinite blocking,
 *       or a time using the monotonic clock to expire. Calculate this
 *       using bson_get_monotonic_time() + N_MICROSECONDS.
 *
 * Returns:
 *       -1 on failure. number of bytes written on success.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

ssize_t
mongoc_socket_send (mongoc_socket_t *sock, /* IN */
                    const void *buf,       /* IN */
                    size_t buflen,         /* IN */
                    int64_t expire_at)     /* IN */
{
   mongoc_iovec_t iov;

   BSON_ASSERT (sock);
   BSON_ASSERT (buf);
   BSON_ASSERT (buflen);

   iov.iov_base = (void *) buf;
   iov.iov_len = buflen;

   return mongoc_socket_sendv (sock, &iov, 1, expire_at);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_try_sendv_slow --
 *
 *       A slow variant of _mongoc_socket_try_sendv() that sends each
 *       iovec entry one by one. This can happen if we hit EMSGSIZE
 *       with sendmsg() on various POSIX systems or WSASend()+WSAEMSGSIZE
 *       on Windows.
 *
 * Returns:
 *       the number of bytes sent or -1 and errno is set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static ssize_t
_mongoc_socket_try_sendv_slow (mongoc_socket_t *sock, /* IN */
                               mongoc_iovec_t *iov,   /* IN */
                               size_t iovcnt)         /* IN */
{
   ssize_t ret = 0;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (iov);
   BSON_ASSERT (iovcnt);

   for (size_t i = 0u; i < iovcnt; i++) {
#ifdef _WIN32
      BSON_ASSERT (bson_in_range_unsigned (int, iov[i].iov_len));
      const int wrote = send (sock->sd, iov[i].iov_base, (int) iov[i].iov_len, 0);
      if (wrote == SOCKET_ERROR) {
#else
      const ssize_t wrote = send (sock->sd, iov[i].iov_base, iov[i].iov_len, 0);
      if (wrote == -1) {
#endif
         _mongoc_socket_capture_errno (sock);

         if (!_mongoc_socket_errno_is_again (sock)) {
            RETURN (-1);
         }
         RETURN (ret ? ret : -1);
      }

      ret += wrote;

      if (bson_cmp_not_equal_su (wrote, iov[i].iov_len)) {
         RETURN (ret);
      }
   }

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_socket_try_sendv --
 *
 *       Helper used by mongoc_socket_sendv() to try to write as many
 *       bytes to the underlying socket until the socket buffer is full.
 *
 *       This is performed in a non-blocking fashion.
 *
 * Returns:
 *       -1 on failure. the number of bytes written on success.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

static ssize_t
_mongoc_socket_try_sendv (mongoc_socket_t *sock, /* IN */
                          mongoc_iovec_t *iov,   /* IN */
                          size_t iovcnt)         /* IN */
{
#ifdef _WIN32
   DWORD dwNumberofBytesSent = 0;
   int ret;
#else
   struct msghdr msg;
   ssize_t ret;
#endif

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (iov);
   BSON_ASSERT (iovcnt);

   DUMP_IOVEC (sendbuf, iov, iovcnt);

#ifdef _WIN32
   BSON_ASSERT (bson_in_range_unsigned (unsigned_long, iovcnt));
   ret = WSASend (sock->sd, (LPWSABUF) iov, (DWORD) iovcnt, &dwNumberofBytesSent, 0, NULL, NULL);
   TRACE ("WSASend sent: %ld (out of: %zu), ret: %d", dwNumberofBytesSent, iov->iov_len, ret);
#else
   memset (&msg, 0, sizeof msg);
   msg.msg_iov = iov;
   msg.msg_iovlen = iovcnt;
   ret = sendmsg (sock->sd,
                  &msg,
#ifdef MSG_NOSIGNAL
                  MSG_NOSIGNAL);
#else
                  0);
#endif
   TRACE ("Send %zu out of %zu bytes", ret, iov->iov_len);
#endif


#ifdef _WIN32
   if (ret == SOCKET_ERROR) {
#else
   if (ret == -1) {
#endif
      _mongoc_socket_capture_errno (sock);

/*
 * Check to see if we have sent an iovec too large for sendmsg to
 * complete. If so, we need to fallback to the slow path of multiple
 * send() commands.
 */
#ifdef _WIN32
      if (mongoc_socket_errno (sock) == WSAEMSGSIZE) {
#else
      if (mongoc_socket_errno (sock) == EMSGSIZE) {
#endif
         RETURN (_mongoc_socket_try_sendv_slow (sock, iov, iovcnt));
      }

      RETURN (-1);
   }

#ifdef _WIN32
   RETURN (dwNumberofBytesSent);
#else
   RETURN (ret);
#endif
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_sendv --
 *
 *       A wrapper around using sendmsg() to send an iovec.
 *       This also deals with the structure differences between
 *       WSABUF and struct iovec.
 *
 *       @expire_at is 0 for no blocking, -1 for infinite blocking,
 *       or a time using the monotonic clock to expire. Calculate this
 *       using bson_get_monotonic_time() + N_MICROSECONDS.
 *
 * Returns:
 *       -1 on failure.
 *       the number of bytes written on success.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

ssize_t
mongoc_socket_sendv (mongoc_socket_t *sock,  /* IN */
                     mongoc_iovec_t *in_iov, /* IN */
                     size_t iovcnt,          /* IN */
                     int64_t expire_at)      /* IN */
{
   ssize_t ret = 0;
   ssize_t sent;
   size_t cur = 0;
   mongoc_iovec_t *iov;

   ENTRY;

   BSON_ASSERT (sock);
   BSON_ASSERT (in_iov);
   BSON_ASSERT (iovcnt);

   iov = bson_malloc (sizeof (*iov) * iovcnt);
   memcpy (iov, in_iov, sizeof (*iov) * iovcnt);

   for (;;) {
      sent = _mongoc_socket_try_sendv (sock, &iov[cur], iovcnt - cur);
      TRACE ("Sent %zd (of %zu) out of iovcnt=%zu", sent, iov[cur].iov_len, iovcnt);

      /*
       * If we failed with anything other than EAGAIN or EWOULDBLOCK,
       * we should fail immediately as there is another issue with the
       * underlying socket.
       */
      if (sent == -1) {
         if (!_mongoc_socket_errno_is_again (sock)) {
            ret = -1;
            GOTO (CLEANUP);
         }
      }

      /*
       * Update internal stream counters.
       */
      if (sent > 0) {
         ret += sent;
         mongoc_counter_streams_egress_add (sent);

         /*
          * Subtract the sent amount from what we still need to send.
          */
         while ((cur < iovcnt) && (sent >= (ssize_t) iov[cur].iov_len)) {
            TRACE ("still got bytes left: sent -= iov_len: %zd -= %zu", sent, iov[cur].iov_len);
            sent -= iov[cur++].iov_len;
         }

         /*
          * Check if that made us finish all of the iovecs. If so, we are done
          * sending data over the socket.
          */
         if (cur == iovcnt) {
            TRACE ("%s", "Finished the iovecs");
            break;
         }

         /*
          * Increment the current iovec buffer to its proper offset and adjust
          * the number of bytes to write.
          */
         TRACE ("Seeked io_base+%zd", sent);
         TRACE ("Subtracting iov_len -= sent; %zu -= %zd", iov[cur].iov_len, sent);
         iov[cur].iov_base = ((char *) iov[cur].iov_base) + sent;
         iov[cur].iov_len -= sent;
         TRACE ("iov_len remaining %zu", iov[cur].iov_len);

         BSON_ASSERT (iovcnt - cur);
         BSON_ASSERT (iov[cur].iov_len);
      } else if (OPERATION_EXPIRED (expire_at)) {
         if (expire_at > 0) {
            mongoc_counter_streams_timeout_inc ();
         }
         GOTO (CLEANUP);
      }

      /*
       * Block on poll() until our desired condition is met.
       */
      if (!_mongoc_socket_wait (sock, POLLOUT, expire_at)) {
         GOTO (CLEANUP);
      }
   }

CLEANUP:
   bson_free (iov);

   RETURN (ret);
}


int
mongoc_socket_getsockname (mongoc_socket_t *sock,     /* IN */
                           struct sockaddr *addr,     /* OUT */
                           mongoc_socklen_t *addrlen) /* INOUT */
{
   int ret;

   ENTRY;

   BSON_ASSERT (sock);

   ret = getsockname (sock->sd, addr, addrlen);

   _mongoc_socket_capture_errno (sock);

   RETURN (ret);
}


char *
mongoc_socket_getnameinfo (mongoc_socket_t *sock) /* IN */
{
   /* getpeername parameter types vary, we check in CheckCompiler.m4 */
   struct sockaddr_storage addr;
   mongoc_socklen_t len = (mongoc_socklen_t) sizeof addr;
   char *ret;
   char host[BSON_HOST_NAME_MAX + 1];

   ENTRY;

   BSON_ASSERT (sock);

   if (getpeername (sock->sd, (struct sockaddr *) &addr, &len)) {
      RETURN (NULL);
   }

   if (getnameinfo ((struct sockaddr *) &addr, len, host, sizeof host, NULL, 0, 0)) {
      RETURN (NULL);
   }

   ret = bson_strdup (host);
   RETURN (ret);
}


bool
mongoc_socket_check_closed (mongoc_socket_t *sock) /* IN */
{
   bool closed = false;
   char buf[1];
   ssize_t r;

   if (_mongoc_socket_wait (sock, POLLIN, 0)) {
      sock->errno_ = 0;

      r = recv (sock->sd, buf, 1, MSG_PEEK);

      if (r < 0) {
         _mongoc_socket_capture_errno (sock);
      }

      if (r < 1) {
         closed = true;
      }
   }

   return closed;
}

/*
 *
 *--------------------------------------------------------------------------
 *
 * mongoc_socket_inet_ntop --
 *
 *       Convert the ip from addrinfo into a c string.
 *
 * Returns:
 *       The value is returned into 'buffer'. The memory has to be allocated
 *       by the caller
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_socket_inet_ntop (struct addrinfo *rp, /* IN */
                         char *buf,           /* INOUT */
                         size_t buflen)       /* IN */
{
   void *ptr;
   char tmp[256];

   switch (rp->ai_family) {
   case AF_INET:
      ptr = &((struct sockaddr_in *) rp->ai_addr)->sin_addr;
      inet_ntop (rp->ai_family, ptr, tmp, sizeof (tmp));
      bson_snprintf (buf, buflen, "ipv4 %s", tmp);
      break;
   case AF_INET6:
      ptr = &((struct sockaddr_in6 *) rp->ai_addr)->sin6_addr;
      inet_ntop (rp->ai_family, ptr, tmp, sizeof (tmp));
      bson_snprintf (buf, buflen, "ipv6 %s", tmp);
      break;
   default:
      bson_snprintf (buf, buflen, "unknown ip %d", rp->ai_family);
      break;
   }
}
