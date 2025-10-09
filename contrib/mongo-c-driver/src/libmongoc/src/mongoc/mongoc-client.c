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
#include "mongoc-config.h"
#ifdef MONGOC_HAVE_DNSAPI
/* for DnsQuery_UTF8 */
#include <Windows.h>
#include <WinDNS.h>
#include <ws2tcpip.h>
#else
#if defined(MONGOC_HAVE_RES_NSEARCH) || defined(MONGOC_HAVE_RES_SEARCH)
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/nameser.h>
#include <resolv.h>
#define BSON_INSIDE
#include <bson/bson-string.h>
#undef BSON_INSIDE

#endif
#endif

#include "mongoc-client-private.h"
#include "mongoc-client-side-encryption-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-counters-private.h"
#include "mongoc-database-private.h"
#include "mongoc-gridfs-private.h"
#include "mongoc-error.h"
#include "mongoc-error-private.h"
#include "mongoc-log.h"
#include "mongoc-queue-private.h"
#include "mongoc-socket.h"
#include "mongoc-stream-buffered.h"
#include "mongoc-stream-socket.h"
#include "mongoc-thread-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-uri-private.h"
#include "mongoc-util-private.h"
#include "mongoc-set-private.h"
#include "mongoc-log.h"
#include "mongoc-write-concern-private.h"
#include "mongoc-read-concern-private.h"
#include "mongoc-host-list-private.h"
#include "mongoc-read-prefs-private.h"
#include "mongoc-change-stream-private.h"
#include "mongoc-client-session-private.h"
#include "mongoc-cursor-private.h"

#ifdef MONGOC_ENABLE_SSL
#include "mongoc-stream-tls.h"
#include "mongoc-ssl-private.h"
#include "mongoc-cmd-private.h"
#include "mongoc-opts-private.h"
#endif


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "client"


static void
_mongoc_client_op_killcursors (mongoc_cluster_t *cluster,
                               mongoc_server_stream_t *server_stream,
                               int64_t cursor_id,
                               int64_t operation_id,
                               const char *db,
                               const char *collection);

static void
_mongoc_client_killcursors_command (mongoc_cluster_t *cluster,
                                    mongoc_server_stream_t *server_stream,
                                    int64_t cursor_id,
                                    const char *db,
                                    const char *collection,
                                    mongoc_client_session_t *cs);

#define DNS_ERROR(_msg, ...)                                                                               \
   do {                                                                                                    \
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, _msg, __VA_ARGS__); \
      GOTO (done);                                                                                         \
   } while (0)


#if MONGOC_ENABLE_SRV == 0 // ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ ENABLE_SRV disabled

/* SRV support is disabled */

#elif defined(MONGOC_HAVE_DNSAPI) // ↑↑↑ ENABLE_SRV disabled / Win32 Dnsapi ↓↓↓↓

typedef bool (*mongoc_rr_callback_t) (const char *hostname,
                                      PDNS_RECORD pdns,
                                      mongoc_rr_data_t *rr_data,
                                      bson_error_t *error);

static bool
srv_callback (const char *hostname, PDNS_RECORD pdns, mongoc_rr_data_t *rr_data, bson_error_t *error)
{
   mongoc_host_list_t new_host;

   if (rr_data && rr_data->hosts) {
      _mongoc_host_list_remove_host (&(rr_data->hosts), pdns->Data.SRV.pNameTarget, pdns->Data.SRV.wPort);
   }

   if (!_mongoc_host_list_from_hostport_with_err (&new_host, pdns->Data.SRV.pNameTarget, pdns->Data.SRV.wPort, error)) {
      return false;
   }
   _mongoc_host_list_upsert (&rr_data->hosts, &new_host);

   return true;
}

/* rr_data is unused, but here to match srv_callback signature */
static bool
txt_callback (const char *hostname, PDNS_RECORD pdns, mongoc_rr_data_t *rr_data, bson_error_t *error)
{
   DWORD i;
   bson_string_t *txt;

   txt = bson_string_new (NULL);

   for (i = 0; i < pdns->Data.TXT.dwStringCount; i++) {
      bson_string_append (txt, pdns->Data.TXT.pStringArray[i]);
   }

   rr_data->txt_record_opts = bson_strdup (txt->str);
   bson_string_free (txt, true);

   return true;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_get_rr_dnsapi --
 *
 *       Fetch SRV or TXT resource records using the Windows DNS API and
 *       put results in @rr_data.
 *
 * Returns:
 *       Success or failure.
 *
 *       For an SRV lookup, returns false if there is any error.
 *
 *       For TXT lookup, ignores any error fetching the resource record and
 *       always returns true.
 *
 * Side effects:
 *       @error is set if there is a failure.
 *       @rr_data->hosts may be set if querying SRV. Caller must destroy.
 *       @rr_data->txt_record_opts may be set if querying TXT. Caller must
 *       free.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_get_rr_dnsapi (
   const char *hostname, mongoc_rr_type_t rr_type, mongoc_rr_data_t *rr_data, bool prefer_tcp, bson_error_t *error)
{
   const char *rr_type_name;
   WORD nst;
   mongoc_rr_callback_t callback;
   PDNS_RECORD pdns = NULL;
   DNS_STATUS res;
   LPVOID lpMsgBuf = NULL;
   bool dns_success;
   bool callback_success = true;
   int i;

   ENTRY;

   if (rr_type == MONGOC_RR_SRV) {
      /* return true only if DNS succeeds */
      dns_success = false;
      rr_type_name = "SRV";
      nst = DNS_TYPE_SRV;
      callback = srv_callback;
   } else {
      /* return true whether or not DNS succeeds */
      dns_success = true;
      rr_type_name = "TXT";
      nst = DNS_TYPE_TEXT;
      callback = txt_callback;
   }

   DWORD options = DNS_QUERY_BYPASS_CACHE;
   if (prefer_tcp) {
      options |= DNS_QUERY_USE_TCP_ONLY;
   }
   res = DnsQuery_UTF8 (hostname, nst, options, NULL /* IP Address */, &pdns, 0 /* reserved */);

   if (res) {
      DWORD flags = FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;

      if (FormatMessage (flags, 0, res, MAKELANGID (LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) &lpMsgBuf, 0, 0)) {
         DNS_ERROR ("Failed to look up %s record \"%s\": %s", rr_type_name, hostname, (char *) lpMsgBuf);
      }

      DNS_ERROR ("Failed to look up %s record \"%s\": Unknown error", rr_type_name, hostname);
   }

   if (!pdns) {
      DNS_ERROR ("No %s records for \"%s\"", rr_type_name, hostname);
   }

   i = 0;

   do {
      /* DnsQuery can return additional records not of the requested type */
      if ((rr_type == MONGOC_RR_TXT && pdns->wType == DNS_TYPE_TEXT) ||
          (rr_type == MONGOC_RR_SRV && pdns->wType == DNS_TYPE_SRV)) {
         if (i > 0 && rr_type == MONGOC_RR_TXT) {
            /* Initial DNS Seedlist Discovery Spec: a client "MUST raise an
            error when multiple TXT records are encountered". */
            callback_success = false;
            DNS_ERROR ("Multiple TXT records for \"%s\"", hostname);
         }

         if (rr_data) {
            if ((i == 0) || (pdns->dwTtl < rr_data->min_ttl)) {
               rr_data->min_ttl = pdns->dwTtl;
            }
         }

         if (!callback (hostname, pdns, rr_data, error)) {
            callback_success = false;
            GOTO (done);
         }

         i++;
      }

      pdns = pdns->pNext;
   } while (pdns);


   rr_data->count = i;
   if (i == 0) {
      DNS_ERROR ("No matching %s records for \"%s\"", rr_type_name, hostname);
   }
   dns_success = true;

done:
   if (pdns) {
      DnsRecordListFree (pdns, DnsFreeRecordList);
   }

   if (lpMsgBuf) {
      LocalFree (lpMsgBuf);
   }

   RETURN (dns_success && callback_success);
}

#elif (defined(MONGOC_HAVE_RES_NSEARCH) || defined(MONGOC_HAVE_RES_SEARCH)) // ↑↑↑↑↑↑↑ Win32 Dnsapi / resolv ↓↓↓↓↓↓↓↓

typedef bool (*mongoc_rr_callback_t) (
   const char *hostname, ns_msg *ns_answer, ns_rr *rr, mongoc_rr_data_t *rr_data, bson_error_t *error);

static const char *
_mongoc_hstrerror (int code)
{
   switch (code) {
   case HOST_NOT_FOUND:
      return "The specified host is unknown.";
   case NO_ADDRESS:
      return "The requested name is valid but does not have an IP address.";
   case NO_RECOVERY:
      return "A nonrecoverable name server error occurred.";
   case TRY_AGAIN:
      return "A temporary error occurred on an authoritative name server. Try "
             "again later.";
   default:
      return "An unknown error occurred.";
   }
}

static bool
srv_callback (const char *hostname, ns_msg *ns_answer, ns_rr *rr, mongoc_rr_data_t *rr_data, bson_error_t *error)
{
   const uint8_t *data;
   char name[1024];
   uint16_t port;
   int size;
   bool ret = false;
   mongoc_host_list_t new_host;

   data = ns_rr_rdata (*rr);
   /* memcpy the network endian port before converting to host endian. we cannot
    * cast (data + 4) directly as a uint16_t*, because it may not align on an
    * 2-byte boundary. */
   memcpy (&port, data + 4, sizeof (port));
   port = ntohs (port);
   size = dn_expand (ns_msg_base (*ns_answer), ns_msg_end (*ns_answer), data + 6, name, sizeof (name));

   if (size < 1) {
      DNS_ERROR ("Invalid record in SRV answer for \"%s\": \"%s\"", hostname, _mongoc_hstrerror (h_errno));
   }

   if (!_mongoc_host_list_from_hostport_with_err (&new_host, name, port, error)) {
      GOTO (done);
   }
   _mongoc_host_list_upsert (&rr_data->hosts, &new_host);
   ret = true;
done:
   return ret;
}

static bool
txt_callback (const char *hostname, ns_msg *ns_answer, ns_rr *rr, mongoc_rr_data_t *rr_data, bson_error_t *error)
{
   char s[256];
   const uint8_t *data;
   bson_string_t *txt;
   uint16_t pos, total;
   uint8_t len;
   bool ret = false;

   BSON_UNUSED (ns_answer);

   total = (uint16_t) ns_rr_rdlen (*rr);
   if (total < 1 || total > 255) {
      DNS_ERROR ("Invalid TXT record size %hu for \"%s\"", total, hostname);
   }

   /* a TXT record has one or more strings, each up to 255 chars, each is
    * prefixed by its length as 1 byte. thus endianness doesn't matter. */
   txt = bson_string_new (NULL);
   pos = 0;
   data = ns_rr_rdata (*rr);

   while (pos < total) {
      memcpy (&len, data + pos, sizeof (uint8_t));
      pos++;
      bson_strncpy (s, (const char *) (data + pos), (size_t) len + 1);
      bson_string_append (txt, s);
      pos += len;
   }

   rr_data->txt_record_opts = bson_strdup (txt->str);
   bson_string_free (txt, true);
   ret = true;

done:
   return ret;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_get_rr_search --
 *
 *       Fetch SRV or TXT resource records using libresolv and put results in
 *       @rr_data.
 *
 * Returns:
 *       Success or failure.
 *
 *       For an SRV lookup, returns false if there is any error.
 *
 *       For TXT lookup, ignores any error fetching the resource record and
 *       always returns true.
 *
 * Side effects:
 *       @error is set if there is a failure.
 *       @rr_data->hosts may be set if querying SRV. Caller must destroy.
 *       @rr_data->txt_record_opts may be set if querying TXT. Caller must
 *       free.
 *
 *--------------------------------------------------------------------------
 */

static bool
_mongoc_get_rr_search (const char *hostname,
                       mongoc_rr_type_t rr_type,
                       mongoc_rr_data_t *rr_data,
                       size_t initial_buffer_size,
                       bool prefer_tcp,
                       bson_error_t *error)
{
#ifdef MONGOC_HAVE_RES_NSEARCH
   struct __res_state state = {0};
#endif
   int size = 0;
   unsigned char *search_buf = NULL;
   size_t buffer_size = initial_buffer_size;
   ns_msg ns_answer;
   int n;
   int i;
   const char *rr_type_name;
   ns_type nst;
   mongoc_rr_callback_t callback;
   ns_rr resource_record;
   bool dns_success;
   bool callback_success = true;
   int num_matching_records;
   uint32_t ttl;

   ENTRY;

   if (rr_type == MONGOC_RR_SRV) {
      /* return true only if DNS succeeds */
      dns_success = false;
      rr_type_name = "SRV";
      nst = ns_t_srv;
      callback = srv_callback;
   } else {
      /* return true whether or not DNS succeeds */
      dns_success = true;
      rr_type_name = "TXT";
      nst = ns_t_txt;
      callback = txt_callback;
   }

   do {
      if (search_buf) {
         bson_free (search_buf);

         /* increase buffer size by the previous response size. This ensures
          * that even if a subsequent response is larger, we'll still be able
          * to fit it in the response buffer */
         buffer_size = buffer_size + size;
      }

      search_buf = (unsigned char *) bson_malloc (buffer_size);
      BSON_ASSERT (search_buf);

#ifdef MONGOC_HAVE_RES_NSEARCH
      /* thread-safe */
      res_ninit (&state);
      if (prefer_tcp) {
         state.options |= RES_USEVC;
      }
      size = res_nsearch (&state, hostname, ns_c_in, nst, search_buf, buffer_size);
#elif defined(MONGOC_HAVE_RES_SEARCH)
      size = res_search (hostname, ns_c_in, nst, search_buf, buffer_size);
#endif

      if (size < 0) {
         DNS_ERROR ("Failed to look up %s record \"%s\": %s", rr_type_name, hostname, _mongoc_hstrerror (h_errno));
      }
   } while (size >= buffer_size);

   if (ns_initparse (search_buf, size, &ns_answer)) {
      DNS_ERROR ("Invalid %s answer for \"%s\"", rr_type_name, hostname);
   }

   n = ns_msg_count (ns_answer, ns_s_an);
   if (!n) {
      DNS_ERROR ("No %s records for \"%s\"", rr_type_name, hostname);
   }

   rr_data->count = n;
   num_matching_records = 0;
   for (i = 0; i < n; i++) {
      if (ns_parserr (&ns_answer, ns_s_an, i, &resource_record)) {
         DNS_ERROR ("Invalid record %d of %s answer for \"%s\": \"%s\"",
                    i,
                    rr_type_name,
                    hostname,
                    _mongoc_hstrerror (h_errno));
      }

      /* Skip records that don't match the ones we requested. CDRIVER-3628 shows
       * that we can receive records that were not requested. */
      if (rr_type == MONGOC_RR_TXT) {
         if (ns_rr_type (resource_record) != ns_t_txt) {
            continue;
         }
      } else if (rr_type == MONGOC_RR_SRV) {
         if (ns_rr_type (resource_record) != ns_t_srv) {
            continue;
         }
      }

      if (num_matching_records > 0 && rr_type == MONGOC_RR_TXT) {
         /* Initial DNS Seedlist Discovery Spec: a client "MUST raise an error
          * when multiple TXT records are encountered". */
         callback_success = false;
         DNS_ERROR ("Multiple TXT records for \"%s\"", hostname);
      }

      num_matching_records++;

      ttl = ns_rr_ttl (resource_record);
      if ((i == 0) || (ttl < rr_data->min_ttl)) {
         rr_data->min_ttl = ttl;
      }

      if (!callback (hostname, &ns_answer, &resource_record, rr_data, error)) {
         callback_success = false;
         GOTO (done);
      }
   }

   if (num_matching_records == 0) {
      DNS_ERROR ("No matching %s records for \"%s\"", rr_type_name, hostname);
   }

   dns_success = true;

done:

   bson_free (search_buf);

#ifdef MONGOC_HAVE_RES_NDESTROY
   /* defined on BSD/Darwin, and only if MONGOC_HAVE_RES_NSEARCH is defined */
   res_ndestroy (&state);
#elif defined(MONGOC_HAVE_RES_NCLOSE)
   /* defined on Linux, and only if MONGOC_HAVE_RES_NSEARCH is defined */
   res_nclose (&state);
#endif
   RETURN (dns_success && callback_success);
}
#endif // ↑↑↑↑↑↑↑↑↑↑↑↑↑ resolv

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_client_get_rr --
 *
 *       Fetch an SRV or TXT resource record and update put results in
 *       @rr_data.
 *
 *       See RFCs 1464 and 2782, MongoDB's "Initial DNS Seedlist Discovery"
 *       spec, and MongoDB's "Polling SRV Records for Mongos Discovery"
 *       spec.
 *
 * Returns:
 *       Success or failure.
 *
 * Side effects:
 *       @error is set if there is a failure. Errors fetching TXT are
 *       ignored.
 *       @rr_data->hosts may be set if querying SRV. Caller must destroy.
 *       @rr_data->txt_record_opts may be set if querying TXT. Caller must
 *       free.
 *
 *--------------------------------------------------------------------------
 */

bool
_mongoc_client_get_rr (const char *hostname,
                       mongoc_rr_type_t rr_type,
                       mongoc_rr_data_t *rr_data,
                       size_t initial_buffer_size,
                       bool prefer_tcp,
                       bson_error_t *error)
{
   BSON_ASSERT (rr_data);

#if MONGOC_ENABLE_SRV == 0
   // Disabled
   bson_set_error (error,
                   MONGOC_ERROR_STREAM,
                   MONGOC_ERROR_STREAM_NAME_RESOLUTION,
                   "libresolv unavailable, cannot use mongodb+srv URI");
   return false;
#elif defined(MONGOC_HAVE_DNSAPI)
   return _mongoc_get_rr_dnsapi (hostname, rr_type, rr_data, prefer_tcp, error);
#elif (defined(MONGOC_HAVE_RES_NSEARCH) || defined(MONGOC_HAVE_RES_SEARCH))
   return _mongoc_get_rr_search (hostname, rr_type, rr_data, initial_buffer_size, prefer_tcp, error);
#else
#error No SRV library is available, but ENABLE_SRV is true!
#endif
}

#undef DNS_ERROR

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_connect_tcp --
 *
 *       Connect to a host using a TCP socket.
 *
 *       This will be performed synchronously and return a mongoc_stream_t
 *       that can be used to connect with the remote host.
 *
 * Returns:
 *       A newly allocated mongoc_stream_t if successful; otherwise
 *       NULL and @error is set.
 *
 * Side effects:
 *       @error is set if return value is NULL.
 *
 *--------------------------------------------------------------------------
 */

mongoc_stream_t *
mongoc_client_connect_tcp (int32_t connecttimeoutms, const mongoc_host_list_t *host, bson_error_t *error)
{
   mongoc_socket_t *sock = NULL;
   struct addrinfo hints;
   struct addrinfo *result, *rp;
   int64_t expire_at;
   char portstr[8];
   int s;

   ENTRY;

   BSON_ASSERT (connecttimeoutms);
   BSON_ASSERT (host);

   bson_snprintf (portstr, sizeof portstr, "%hu", host->port);

   memset (&hints, 0, sizeof hints);
   hints.ai_family = host->family;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_flags = 0;
   hints.ai_protocol = 0;

   TRACE ("DNS lookup for %s", host->host);
   s = getaddrinfo (host->host, portstr, &hints, &result);

   if (s != 0) {
      mongoc_counter_dns_failure_inc ();
      TRACE ("Failed to resolve %s", host->host);
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve %s", host->host);
      RETURN (NULL);
   }

   mongoc_counter_dns_success_inc ();

   for (rp = result; rp; rp = rp->ai_next) {
      /*
       * Create a new non-blocking socket.
       */
      if (!(sock = mongoc_socket_new (rp->ai_family, rp->ai_socktype, rp->ai_protocol))) {
         continue;
      }

      /*
       * Try to connect to the peer.
       */
      expire_at = bson_get_monotonic_time () + (connecttimeoutms * 1000L);
      if (0 != mongoc_socket_connect (sock, rp->ai_addr, (mongoc_socklen_t) rp->ai_addrlen, expire_at)) {
         mongoc_socket_destroy (sock);
         sock = NULL;
         continue;
      }

      break;
   }

   if (!sock) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_CONNECT,
                      "Failed to connect to target host: %s",
                      host->host_and_port);
      freeaddrinfo (result);
      RETURN (NULL);
   }

   freeaddrinfo (result);

   return mongoc_stream_socket_new (sock);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_connect_unix --
 *
 *       Connect to a MongoDB server using a UNIX domain socket.
 *
 * Returns:
 *       A newly allocated mongoc_stream_t if successful; otherwise
 *       NULL and @error is set.
 *
 * Side effects:
 *       @error is set if return value is NULL.
 *
 *--------------------------------------------------------------------------
 */

static mongoc_stream_t *
mongoc_client_connect_unix (const mongoc_host_list_t *host, bson_error_t *error)
{
#ifdef _WIN32
   ENTRY;
   bson_set_error (
      error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "UNIX domain sockets not supported on win32.");
   RETURN (NULL);
#else
   struct sockaddr_un saddr;
   mongoc_socket_t *sock;
   mongoc_stream_t *ret = NULL;

   ENTRY;

   BSON_ASSERT (host);

   memset (&saddr, 0, sizeof saddr);
   saddr.sun_family = AF_UNIX;
   bson_snprintf (saddr.sun_path, sizeof saddr.sun_path - 1, "%s", host->host);

   sock = mongoc_socket_new (AF_UNIX, SOCK_STREAM, 0);

   if (sock == NULL) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to create socket.");
      RETURN (NULL);
   }

   if (-1 == mongoc_socket_connect (sock, (struct sockaddr *) &saddr, sizeof saddr, -1)) {
      mongoc_socket_destroy (sock);
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "Failed to connect to UNIX domain socket.");
      RETURN (NULL);
   }

   ret = mongoc_stream_socket_new (sock);

   RETURN (ret);
#endif
}

mongoc_stream_t *
mongoc_client_connect (bool buffered,
                       bool use_ssl,
                       void *ssl_opts_void,
                       const mongoc_uri_t *uri,
                       const mongoc_host_list_t *host,
                       bson_error_t *error)
{
   mongoc_stream_t *base_stream = NULL;
   int32_t connecttimeoutms;

   BSON_ASSERT (uri);
   BSON_ASSERT (host);

#ifndef MONGOC_ENABLE_SSL
   if (ssl_opts_void || mongoc_uri_get_tls (uri)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_NO_ACCEPTABLE_PEER,
                      "TLS is not enabled in this build of mongo-c-driver.");
      return NULL;
   }
#endif

   connecttimeoutms =
      mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, MONGOC_DEFAULT_CONNECTTIMEOUTMS);

   switch (host->family) {
   case AF_UNSPEC:
#if defined(AF_INET6)
   case AF_INET6:
#endif
   case AF_INET:
      base_stream = mongoc_client_connect_tcp (connecttimeoutms, host, error);
      break;
   case AF_UNIX:
      base_stream = mongoc_client_connect_unix (host, error);
      break;
   default:
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_INVALID_TYPE, "Invalid address family: 0x%02x", host->family);
      break;
   }

#ifdef MONGOC_ENABLE_SSL
   if (base_stream) {
      mongoc_ssl_opt_t *ssl_opts;
      const char *mechanism;

      ssl_opts = (mongoc_ssl_opt_t *) ssl_opts_void;
      mechanism = mongoc_uri_get_auth_mechanism (uri);

      if (use_ssl || (mechanism && (0 == strcmp (mechanism, "MONGODB-X509")))) {
         mongoc_stream_t *original = base_stream;

         base_stream = mongoc_stream_tls_new_with_hostname (base_stream, host->host, ssl_opts, true);

         if (!base_stream) {
            mongoc_stream_destroy (original);
            bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed initialize TLS state.");
            return NULL;
         }

         if (!mongoc_stream_tls_handshake_block (base_stream, host->host, connecttimeoutms, error)) {
            mongoc_stream_destroy (base_stream);
            return NULL;
         }
      }
   }
#endif

   if (!base_stream) {
      return NULL;
   }
   if (buffered) {
      return mongoc_stream_buffered_new (base_stream, 1024);
   }
   return base_stream;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_default_stream_initiator --
 *
 *       A mongoc_stream_initiator_t that will handle the various type
 *       of supported sockets by MongoDB including TCP and UNIX.
 *
 *       Language binding authors may want to implement an alternate
 *       version of this method to use their native stream format.
 *
 * Returns:
 *       A mongoc_stream_t if successful; otherwise NULL and @error is set.
 *
 * Side effects:
 *       @error is set if return value is NULL.
 *
 *--------------------------------------------------------------------------
 */

mongoc_stream_t *
mongoc_client_default_stream_initiator (const mongoc_uri_t *uri,
                                        const mongoc_host_list_t *host,
                                        void *user_data,
                                        bson_error_t *error)
{
   void *ssl_opts_void = NULL;
   bool use_ssl = false;
#ifdef MONGOC_ENABLE_SSL
   mongoc_client_t *client = (mongoc_client_t *) user_data;

   use_ssl = client->use_ssl;
   ssl_opts_void = (void *) &client->ssl_opts;

#endif

   return mongoc_client_connect (true, use_ssl, ssl_opts_void, uri, host, error);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_client_create_stream --
 *
 *       INTERNAL API
 *
 *       This function is used by the mongoc_cluster_t to initiate a
 *       new stream. This is done because cluster is private API and
 *       those using mongoc_client_t may need to override this process.
 *
 *       This function calls the default initiator for new streams.
 *
 * Returns:
 *       A newly allocated mongoc_stream_t if successful; otherwise
 *       NULL and @error is set.
 *
 * Side effects:
 *       @error is set if return value is NULL.
 *
 *--------------------------------------------------------------------------
 */

mongoc_stream_t *
_mongoc_client_create_stream (mongoc_client_t *client, const mongoc_host_list_t *host, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (host);

   return client->initiator (client->uri, host, client->initiator_data, error);
}


bool
_mongoc_client_recv (mongoc_client_t *client,
                     mcd_rpc_message *rpc,
                     mongoc_buffer_t *buffer,
                     mongoc_server_stream_t *server_stream,
                     bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (rpc);
   BSON_ASSERT (buffer);
   BSON_ASSERT (server_stream);
   BSON_ASSERT_PARAM (error);

   return mongoc_cluster_try_recv (&client->cluster, rpc, buffer, server_stream, error);
}


mongoc_client_t *
mongoc_client_new (const char *uri_string)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   bson_error_t error = {0};

   if (!uri_string) {
      uri_string = "mongodb://127.0.0.1/";
   }

   if (!(uri = mongoc_uri_new_with_error (uri_string, &error))) {
      /* Log URI errors as a warning for consistency with mongoc_uri_new */
      MONGOC_WARNING ("Error parsing URI: '%s'", error.message);
      return NULL;
   }

   if (!(client = mongoc_client_new_from_uri_with_error (uri, &error))) {
      MONGOC_ERROR ("%s", error.message);
   }

   mongoc_uri_destroy (uri);

   return client;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_set_ssl_opts
 *
 *       set ssl opts for a client
 *
 * Returns:
 *       Nothing
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

#ifdef MONGOC_ENABLE_SSL
/* Only called internally. Caller must ensure opts->internal is valid. */
void
_mongoc_client_set_internal_tls_opts (mongoc_client_t *client, _mongoc_internal_tls_opts_t *internal)
{
   BSON_ASSERT_PARAM (client);
   if (!client->use_ssl) {
      return;
   }
   client->ssl_opts.internal = bson_malloc (sizeof (_mongoc_internal_tls_opts_t));
   memcpy (client->ssl_opts.internal, internal, sizeof (_mongoc_internal_tls_opts_t));
}

void
mongoc_client_set_ssl_opts (mongoc_client_t *client, const mongoc_ssl_opt_t *opts)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (opts);

   _mongoc_ssl_opts_cleanup (&client->ssl_opts, false /* don't free internal opts */);

   client->use_ssl = true;
   _mongoc_ssl_opts_copy_to (opts, &client->ssl_opts, false /* don't overwrite internal opts */);

   if (client->topology->single_threaded) {
      mongoc_topology_scanner_set_ssl_opts (client->topology->scanner, &client->ssl_opts);
   }
}
#endif


mongoc_client_t *
mongoc_client_new_from_uri (const mongoc_uri_t *uri)
{
   mongoc_client_t *client;
   bson_error_t error = {0};

   if (!(client = mongoc_client_new_from_uri_with_error (uri, &error))) {
      MONGOC_ERROR ("%s", error.message);
   }

   return client;
}


mongoc_client_t *
mongoc_client_new_from_uri_with_error (const mongoc_uri_t *uri, bson_error_t *error)
{
   mongoc_client_t *client;
   mongoc_topology_t *topology;


   ENTRY;

   BSON_ASSERT (uri);

#ifndef MONGOC_ENABLE_SSL
   if (mongoc_uri_get_tls (uri)) {
      bson_set_error (error,
                      MONGOC_ERROR_COMMAND,
                      MONGOC_ERROR_COMMAND_INVALID_ARG,
                      "Can't create SSL client, SSL not enabled in this build.");
      RETURN (NULL);
   }
#endif

   topology = mongoc_topology_new (uri, true);

   if (!topology->valid) {
      if (error) {
         memcpy (error, &topology->scanner->error, sizeof (bson_error_t));
      }

      mongoc_topology_destroy (topology);

      RETURN (NULL);
   }

   client = _mongoc_client_new_from_topology (topology);
   BSON_ASSERT (client);

   RETURN (client);
}


/* precondition: topology is valid */
mongoc_client_t *
_mongoc_client_new_from_topology (mongoc_topology_t *topology)
{
   mongoc_client_t *client;
   const mongoc_read_prefs_t *read_prefs;
   const mongoc_read_concern_t *read_concern;
   const mongoc_write_concern_t *write_concern;
   const char *appname;

   BSON_ASSERT (topology);
   BSON_ASSERT (topology->valid);

   client = (mongoc_client_t *) bson_malloc0 (sizeof *client);
   client->uri = mongoc_uri_copy (topology->uri);
   client->initiator = mongoc_client_default_stream_initiator;
   client->initiator_data = client;
   client->topology = topology;
   client->error_api_version = MONGOC_ERROR_API_VERSION_LEGACY;
   client->error_api_set = false;
   client->client_sessions = mongoc_set_new (8, NULL, NULL);
   client->csid_rand_seed = (unsigned int) bson_get_monotonic_time ();

   write_concern = mongoc_uri_get_write_concern (client->uri);
   client->write_concern = mongoc_write_concern_copy (write_concern);

   read_concern = mongoc_uri_get_read_concern (client->uri);
   client->read_concern = mongoc_read_concern_copy (read_concern);

   read_prefs = mongoc_uri_get_read_prefs_t (client->uri);
   client->read_prefs = mongoc_read_prefs_copy (read_prefs);

   appname = mongoc_uri_get_option_as_utf8 (client->uri, MONGOC_URI_APPNAME, NULL);
   if (appname && client->topology->single_threaded) {
      /* the appname should have already been validated */
      BSON_ASSERT (mongoc_client_set_appname (client, appname));
   }

   mongoc_cluster_init (&client->cluster, client->uri, client);

#ifdef MONGOC_ENABLE_SSL
   client->use_ssl = false;
   if (mongoc_uri_get_tls (client->uri)) {
      mongoc_ssl_opt_t ssl_opt = {0};
      _mongoc_internal_tls_opts_t internal_tls_opts = {0};

      _mongoc_ssl_opts_from_uri (&ssl_opt, &internal_tls_opts, client->uri);
      /* sets use_ssl = true */
      mongoc_client_set_ssl_opts (client, &ssl_opt);
      _mongoc_client_set_internal_tls_opts (client, &internal_tls_opts);
   }
#endif

   mongoc_counter_clients_active_inc ();

   return client;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_destroy --
 *
 *       Destroys a mongoc_client_t and cleans up all resources associated
 *       with the client instance.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       @client is destroyed.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_client_destroy (mongoc_client_t *client)
{
   if (client) {
      if (client->topology->single_threaded) {
         _mongoc_client_end_sessions (client);
         mongoc_topology_destroy (client->topology);
      }

      mongoc_write_concern_destroy (client->write_concern);
      mongoc_read_concern_destroy (client->read_concern);
      mongoc_read_prefs_destroy (client->read_prefs);
      mongoc_cluster_destroy (&client->cluster);
      mongoc_uri_destroy (client->uri);
      mongoc_set_destroy (client->client_sessions);
      mongoc_server_api_destroy (client->api);

#ifdef MONGOC_ENABLE_SSL
      _mongoc_ssl_opts_cleanup (&client->ssl_opts, true);
#endif

      bson_free (client);

      mongoc_counter_clients_active_dec ();
      mongoc_counter_clients_disposed_inc ();
   }
}


void
mongoc_client_set_sockettimeoutms (mongoc_client_t *client, int32_t timeoutms)
{
   BSON_ASSERT_PARAM (client);
   mongoc_cluster_set_sockettimeoutms (&client->cluster, timeoutms);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_uri --
 *
 *       Fetch the URI used for @client.
 *
 * Returns:
 *       A mongoc_uri_t that should not be modified or freed.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const mongoc_uri_t *
mongoc_client_get_uri (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return client->uri;
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_start_session --
 *
 *       Creates a structure to communicate in a session over @client.
 *
 *       This structure should be freed when the caller is done with it
 *       using mongoc_client_session_destroy().
 *
 * Returns:
 *       A newly allocated mongoc_client_session_t.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_client_session_t *
mongoc_client_start_session (mongoc_client_t *client, const mongoc_session_opt_t *opts, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);

   mongoc_server_session_t *ss;
   mongoc_client_session_t *cs;
   uint32_t csid;

   ENTRY;

   ss = _mongoc_client_pop_server_session (client, error);
   if (!ss) {
      RETURN (NULL);
   }

   /* get a random internal id for the session, retrying on collision */
   do {
      csid = (uint32_t) _mongoc_rand_simple (&client->csid_rand_seed);
   } while (mongoc_set_get (client->client_sessions, csid));

   /* causal consistency and snapshot cannot both be set. */
   if (opts && mongoc_session_opts_get_causal_consistency (opts) && mongoc_session_opts_get_snapshot (opts)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_SESSION_FAILURE,
                      "Only one of causal consistency and snapshot can be enabled.");
      _mongoc_client_push_server_session (client, ss);
      RETURN (NULL);
   }
   cs = _mongoc_client_session_new (client, ss, opts, csid);

   /* remember session so if we see its client_session_id in a command, we can
    * find its lsid and clusterTime */
   mongoc_set_add (client->client_sessions, csid, cs);

   RETURN (cs);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_database --
 *
 *       Fetches a newly allocated database structure to communicate with
 *       a database over @client.
 *
 *       @database should be a db name such as "test".
 *
 *       This structure should be freed when the caller is done with it
 *       using mongoc_database_destroy().
 *
 * Returns:
 *       A newly allocated mongoc_database_t.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_database_t *
mongoc_client_get_database (mongoc_client_t *client, const char *name)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (name);

   return _mongoc_database_new (client, name, client->read_prefs, client->read_concern, client->write_concern);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_default_database --
 *
 *       Get the database named in the MongoDB connection URI, or NULL
 *       if none was specified in the URI.
 *
 *       This structure should be freed when the caller is done with it
 *       using mongoc_database_destroy().
 *
 * Returns:
 *       A newly allocated mongoc_database_t or NULL.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_database_t *
mongoc_client_get_default_database (mongoc_client_t *client)
{
   const char *db;

   BSON_ASSERT_PARAM (client);
   db = mongoc_uri_get_database (client->uri);

   if (db) {
      return mongoc_client_get_database (client, db);
   }

   return NULL;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_collection --
 *
 *       This function returns a newly allocated collection structure.
 *
 *       @db should be the name of the database, such as "test".
 *       @collection should be the name of the collection such as "test".
 *
 *       The above would result in the namespace "test.test".
 *
 *       You should free this structure when you are done with it using
 *       mongoc_collection_destroy().
 *
 * Returns:
 *       A newly allocated mongoc_collection_t that should be freed with
 *       mongoc_collection_destroy().
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_collection_t *
mongoc_client_get_collection (mongoc_client_t *client, const char *db, const char *collection)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db);
   BSON_ASSERT (collection);

   return _mongoc_collection_new (
      client, db, collection, client->read_prefs, client->read_concern, client->write_concern);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_gridfs --
 *
 *       This function returns a newly allocated collection structure.
 *
 *       @db should be the name of the database, such as "test".
 *
 *       @prefix optional prefix for GridFS collection names, or NULL. Default
 *       is "fs", thus the default collection names for GridFS are "fs.files"
 *       and "fs.chunks".
 *
 * Returns:
 *       A newly allocated mongoc_gridfs_t that should be freed with
 *       mongoc_gridfs_destroy().
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_gridfs_t *
mongoc_client_get_gridfs (mongoc_client_t *client, const char *db, const char *prefix, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db);

   if (!prefix) {
      prefix = "fs";
   }

   return _mongoc_gridfs_new (client, db, prefix, error);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_write_concern --
 *
 *       Fetches the default write concern for @client.
 *
 * Returns:
 *       A mongoc_write_concern_t that should not be modified or freed.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const mongoc_write_concern_t *
mongoc_client_get_write_concern (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return client->write_concern;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_set_write_concern --
 *
 *       Sets the default write concern for @client.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_client_set_write_concern (mongoc_client_t *client, const mongoc_write_concern_t *write_concern)
{
   BSON_ASSERT_PARAM (client);

   if (write_concern != client->write_concern) {
      if (client->write_concern) {
         mongoc_write_concern_destroy (client->write_concern);
      }
      client->write_concern = write_concern ? mongoc_write_concern_copy (write_concern) : mongoc_write_concern_new ();
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_read_concern --
 *
 *       Fetches the default read concern for @client.
 *
 * Returns:
 *       A mongoc_read_concern_t that should not be modified or freed.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const mongoc_read_concern_t *
mongoc_client_get_read_concern (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return client->read_concern;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_set_read_concern --
 *
 *       Sets the default read concern for @client.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_client_set_read_concern (mongoc_client_t *client, const mongoc_read_concern_t *read_concern)
{
   BSON_ASSERT_PARAM (client);

   if (read_concern != client->read_concern) {
      if (client->read_concern) {
         mongoc_read_concern_destroy (client->read_concern);
      }
      client->read_concern = read_concern ? mongoc_read_concern_copy (read_concern) : mongoc_read_concern_new ();
   }
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_get_read_prefs --
 *
 *       Fetch the default read preferences for @client.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

const mongoc_read_prefs_t *
mongoc_client_get_read_prefs (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return client->read_prefs;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_set_read_prefs --
 *
 *       Set the default read preferences for @client.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_client_set_read_prefs (mongoc_client_t *client, const mongoc_read_prefs_t *read_prefs)
{
   BSON_ASSERT_PARAM (client);

   if (read_prefs != client->read_prefs) {
      if (client->read_prefs) {
         mongoc_read_prefs_destroy (client->read_prefs);
      }
      client->read_prefs =
         read_prefs ? mongoc_read_prefs_copy (read_prefs) : mongoc_read_prefs_new (MONGOC_READ_PRIMARY);
   }
}

mongoc_cursor_t *
mongoc_client_command (mongoc_client_t *client,
                       const char *db_name,
                       mongoc_query_flags_t flags,
                       uint32_t skip,
                       uint32_t limit,
                       uint32_t batch_size,
                       const bson_t *query,
                       const bson_t *fields,
                       const mongoc_read_prefs_t *read_prefs)
{
   char *ns = NULL;
   mongoc_cursor_t *cursor;

   BSON_UNUSED (flags);
   BSON_UNUSED (skip);
   BSON_UNUSED (limit);
   BSON_UNUSED (batch_size);
   BSON_UNUSED (fields);

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db_name);
   BSON_ASSERT (query);

   /*
    * Allow a caller to provide a fully qualified namespace
    */
   if (NULL == strstr (db_name, "$cmd")) {
      ns = bson_strdup_printf ("%s.$cmd", db_name);
      db_name = ns;
   }

   cursor = _mongoc_cursor_cmd_deprecated_new (client, db_name, query, read_prefs);

   bson_free (ns);
   return cursor;
}


static bool
_mongoc_client_retryable_read_command_with_stream (mongoc_client_t *client,
                                                   mongoc_cmd_parts_t *parts,
                                                   mongoc_server_stream_t *server_stream,
                                                   bson_t *reply,
                                                   bson_error_t *error)
{
   mongoc_server_stream_t *retry_server_stream = NULL;
   bool is_retryable = true;
   bool ret;
   bson_t reply_local;

   BSON_ASSERT_PARAM (client);
   BSON_UNUSED (server_stream);

   if (reply == NULL) {
      reply = &reply_local;
   }

   ENTRY;

   BSON_ASSERT (parts->is_retryable_read);

retry:
   ret = mongoc_cluster_run_command_monitored (&client->cluster, &parts->assembled, reply, error);

   /* If a retryable error is encountered and the read is retryable, select
    * a new readable stream and retry. If server selection fails or the selected
    * server does not support retryable reads, fall through and allow the
    * original error to be reported. */
   if (is_retryable && _mongoc_read_error_get_type (ret, error, reply) == MONGOC_READ_ERR_RETRY) {
      bson_error_t ignored_error;

      /* each read command may be retried at most once */
      is_retryable = false;

      {
         mongoc_deprioritized_servers_t *const ds = mongoc_deprioritized_servers_new ();

         if (retry_server_stream) {
            mongoc_deprioritized_servers_add_if_sharded (
               ds, retry_server_stream->topology_type, retry_server_stream->sd);
            mongoc_server_stream_cleanup (retry_server_stream);
         } else {
            mongoc_deprioritized_servers_add_if_sharded (ds, server_stream->topology_type, server_stream->sd);
         }

         retry_server_stream = mongoc_cluster_stream_for_reads (
            &client->cluster, parts->read_prefs, parts->assembled.session, ds, NULL, &ignored_error);

         mongoc_deprioritized_servers_destroy (ds);
      }

      if (retry_server_stream) {
         parts->assembled.server_stream = retry_server_stream;
         bson_destroy (reply);
         GOTO (retry);
      }
   }

   if (retry_server_stream) {
      mongoc_server_stream_cleanup (retry_server_stream);
   }

   if (ret && error) {
      /* if a retry succeeded, clear the initial error */
      memset (error, 0, sizeof (bson_error_t));
   }

   RETURN (ret);
}


static bool
_mongoc_client_command_with_stream (mongoc_client_t *client,
                                    mongoc_cmd_parts_t *parts,
                                    const mongoc_read_prefs_t *read_prefs,
                                    mongoc_server_stream_t *server_stream,
                                    bson_t *reply,
                                    bson_error_t *error)
{
   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_UNUSED (read_prefs);

   parts->assembled.operation_id = ++client->cluster.operation_id;
   if (!mongoc_cmd_parts_assemble (parts, server_stream, error)) {
      _mongoc_bson_init_if_set (reply);
      return false;
   }

   if (parts->is_retryable_write) {
      mongoc_server_stream_t *retry_server_stream = NULL;

      bool ret = mongoc_cluster_run_retryable_write (
         &client->cluster, &parts->assembled, true /* is_retryable */, &retry_server_stream, reply, error);

      if (retry_server_stream) {
         mongoc_server_stream_cleanup (retry_server_stream);
         parts->assembled.server_stream = NULL;
      }

      RETURN (ret);
   }

   if (parts->is_retryable_read) {
      RETURN (_mongoc_client_retryable_read_command_with_stream (client, parts, server_stream, reply, error));
   }

   RETURN (mongoc_cluster_run_command_monitored (&client->cluster, &parts->assembled, reply, error));
}


bool
mongoc_client_command_simple (mongoc_client_t *client,
                              const char *db_name,
                              const bson_t *command,
                              const mongoc_read_prefs_t *read_prefs,
                              bson_t *reply,
                              bson_error_t *error)
{
   mongoc_cluster_t *cluster;
   mongoc_server_stream_t *server_stream = NULL;
   mongoc_cmd_parts_t parts;
   bool ret;

   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db_name);
   BSON_ASSERT (command);

   if (!_mongoc_read_prefs_validate (read_prefs, error)) {
      RETURN (false);
   }

   cluster = &client->cluster;
   mongoc_cmd_parts_init (&parts, client, db_name, MONGOC_QUERY_NONE, command);
   parts.read_prefs = read_prefs;

   /* Server Selection Spec: "The generic command method has a default read
    * preference of mode 'primary'. The generic command method MUST ignore any
    * default read preference from client, database or collection
    * configuration. The generic command method SHOULD allow an optional read
    * preference argument."
    */
   server_stream = mongoc_cluster_stream_for_reads (cluster, read_prefs, NULL, NULL, reply, error);

   if (server_stream) {
      ret = _mongoc_client_command_with_stream (client, &parts, read_prefs, server_stream, reply, error);
   } else {
      /* reply initialized by mongoc_cluster_stream_for_reads */
      ret = false;
   }

   mongoc_cmd_parts_cleanup (&parts);
   mongoc_server_stream_cleanup (server_stream);

   RETURN (ret);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_client_command_with_opts --
 *
 *       Execute a command on the server. If mode is MONGOC_CMD_READ or
 *       MONGOC_CMD_RW, then read concern is applied from @opts, or else from
 *       @default_rc, and read preferences are applied from @user_prefs, or else
 *       from @default_prefs. If mode is MONGOC_CMD_WRITE or MONGOC_CMD_RW, then
 *       write concern is applied from @opts if present, or else @default_wc.
 *
 *       If mode is MONGOC_CMD_RAW, then read concern and write concern are
 *       applied from @opts only. Read preferences are applied from
 *       @user_prefs.
 *
 *       The mongoc_client_t's read preference, read concern, and write concern
 *       are *NOT* applied.
 *
 * Returns:
 *       Success or failure.
 *       A write concern timeout or write concern error is considered a failure.
 *
 * Side effects:
 *       @reply is always initialized.
 *       @error is filled out if the command fails.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_client_command_with_opts (mongoc_client_t *client,
                                  const char *db_name,
                                  const bson_t *command,
                                  mongoc_command_mode_t mode,
                                  const bson_t *opts,
                                  mongoc_query_flags_t flags,
                                  const mongoc_read_prefs_t *user_prefs,
                                  const mongoc_read_prefs_t *default_prefs,
                                  mongoc_read_concern_t *default_rc,
                                  mongoc_write_concern_t *default_wc,
                                  bson_t *reply,
                                  bson_error_t *error)
{
   mongoc_read_write_opts_t read_write_opts;
   mongoc_cmd_parts_t parts;
   const char *command_name;
   const mongoc_read_prefs_t *prefs = COALESCE (user_prefs, default_prefs);
   mongoc_server_stream_t *server_stream = NULL;
   mongoc_cluster_t *cluster;
   mongoc_client_session_t *cs;
   bson_t reply_local;
   bson_t *reply_ptr;
   bool reply_initialized = false;
   bool ret = false;

   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db_name);
   BSON_ASSERT (command);

   command_name = _mongoc_get_command_name (command);
   cluster = &client->cluster;
   reply_ptr = reply ? reply : &reply_local;

   mongoc_cmd_parts_init (&parts, client, db_name, flags, command);
   parts.is_read_command = (mode & MONGOC_CMD_READ);
   parts.is_write_command = (mode & MONGOC_CMD_WRITE);

   if (!_mongoc_read_write_opts_parse (client, opts, &read_write_opts, error)) {
      GOTO (done);
   }

   cs = read_write_opts.client_session;

   if (!command_name) {
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Empty command document");
      GOTO (done);
   }

   if (_mongoc_client_session_in_txn (read_write_opts.client_session)) {
      if ((mode == MONGOC_CMD_READ || mode == MONGOC_CMD_RAW) && !IS_PREF_PRIMARY (user_prefs)) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Read preference in a transaction must be primary");
         GOTO (done);
      }

      if (!bson_empty (&read_write_opts.readConcern)) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Cannot set read concern after starting transaction");
         GOTO (done);
      }

      if (read_write_opts.writeConcern && strcmp (command_name, "commitTransaction") != 0 &&
          strcmp (command_name, "abortTransaction") != 0) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Cannot set write concern after starting transaction");
         GOTO (done);
      }
   }

   if (mode == MONGOC_CMD_READ || mode == MONGOC_CMD_RAW) {
      /* NULL read pref is ok */
      if (!_mongoc_read_prefs_validate (prefs, error)) {
         GOTO (done);
      }

      parts.read_prefs = prefs;
   } else {
      /* this is a command that writes */
      prefs = NULL;
   }

   if (read_write_opts.serverId) {
      /* "serverId" passed in opts */
      server_stream = mongoc_cluster_stream_for_server (
         cluster, read_write_opts.serverId, true /* reconnect ok */, cs, reply_ptr, error);

      if (server_stream && server_stream->sd->type != MONGOC_SERVER_MONGOS) {
         parts.user_query_flags |= MONGOC_QUERY_SECONDARY_OK;
      }
   } else if (parts.is_write_command) {
      server_stream = mongoc_cluster_stream_for_writes (cluster, cs, NULL, reply_ptr, error);
   } else {
      server_stream = mongoc_cluster_stream_for_reads (cluster, prefs, cs, NULL, reply_ptr, error);
   }

   if (!server_stream) {
      /* stream_for_reads/writes/server has initialized reply */
      reply_initialized = true;
      GOTO (done);
   }

   if (!mongoc_cmd_parts_append_read_write (&parts, &read_write_opts, error)) {
      GOTO (done);
   }

   if (mode & MONGOC_CMD_WRITE) {
      /* use default write concern unless it's in opts */
      if (!mongoc_write_concern_is_default (default_wc) && !read_write_opts.write_concern_owned) {
         if (!mongoc_cmd_parts_set_write_concern (&parts, default_wc, error)) {
            GOTO (done);
         }
      }
   }

   /* use default read concern for read command, unless it's in opts */
   if ((mode & MONGOC_CMD_READ) && bson_empty (&read_write_opts.readConcern)) {
      if (!mongoc_cmd_parts_set_read_concern (&parts, default_rc, error)) {
         GOTO (done);
      }
   }

   ret = _mongoc_client_command_with_stream (client, &parts, user_prefs, server_stream, reply_ptr, error);

   reply_initialized = true;

   if (ret && (mode & MONGOC_CMD_WRITE)) {
      ret = !_mongoc_parse_wc_err (reply_ptr, error);
   }

done:
   if (reply_ptr == &reply_local) {
      if (reply_initialized) {
         bson_destroy (reply_ptr);
      }
   } else if (!reply_initialized) {
      _mongoc_bson_init_if_set (reply);
   }

   if (server_stream) {
      mongoc_server_stream_cleanup (server_stream);
   }

   mongoc_cmd_parts_cleanup (&parts);
   _mongoc_read_write_opts_cleanup (&read_write_opts);

   RETURN (ret);
}


bool
mongoc_client_read_command_with_opts (mongoc_client_t *client,
                                      const char *db_name,
                                      const bson_t *command,
                                      const mongoc_read_prefs_t *read_prefs,
                                      const bson_t *opts,
                                      bson_t *reply,
                                      bson_error_t *error)
{
   return _mongoc_client_command_with_opts (client,
                                            db_name,
                                            command,
                                            MONGOC_CMD_READ,
                                            opts,
                                            MONGOC_QUERY_NONE,
                                            read_prefs,
                                            client->read_prefs,
                                            client->read_concern,
                                            client->write_concern,
                                            reply,
                                            error);
}


bool
mongoc_client_write_command_with_opts (mongoc_client_t *client,
                                       const char *db_name,
                                       const bson_t *command,
                                       const bson_t *opts,
                                       bson_t *reply,
                                       bson_error_t *error)
{
   return _mongoc_client_command_with_opts (client,
                                            db_name,
                                            command,
                                            MONGOC_CMD_WRITE,
                                            opts,
                                            MONGOC_QUERY_NONE,
                                            NULL,
                                            client->read_prefs,
                                            client->read_concern,
                                            client->write_concern,
                                            reply,
                                            error);
}


bool
mongoc_client_read_write_command_with_opts (mongoc_client_t *client,
                                            const char *db_name,
                                            const bson_t *command,
                                            const mongoc_read_prefs_t *read_prefs /* IGNORED */,
                                            const bson_t *opts,
                                            bson_t *reply,
                                            bson_error_t *error)
{
   return _mongoc_client_command_with_opts (client,
                                            db_name,
                                            command,
                                            MONGOC_CMD_RW,
                                            opts,
                                            MONGOC_QUERY_NONE,
                                            read_prefs,
                                            client->read_prefs,
                                            client->read_concern,
                                            client->write_concern,
                                            reply,
                                            error);
}


bool
mongoc_client_command_with_opts (mongoc_client_t *client,
                                 const char *db_name,
                                 const bson_t *command,
                                 const mongoc_read_prefs_t *read_prefs,
                                 const bson_t *opts,
                                 bson_t *reply,
                                 bson_error_t *error)
{
   return _mongoc_client_command_with_opts (client,
                                            db_name,
                                            command,
                                            MONGOC_CMD_RAW,
                                            opts,
                                            MONGOC_QUERY_NONE,
                                            read_prefs,
                                            NULL,
                                            client->read_concern,
                                            client->write_concern,
                                            reply,
                                            error);
}


bool
mongoc_client_command_simple_with_server_id (mongoc_client_t *client,
                                             const char *db_name,
                                             const bson_t *command,
                                             const mongoc_read_prefs_t *read_prefs,
                                             uint32_t server_id,
                                             bson_t *reply,
                                             bson_error_t *error)
{
   mongoc_server_stream_t *server_stream;
   mongoc_cmd_parts_t parts;
   bool ret;

   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (db_name);
   BSON_ASSERT (command);

   if (!_mongoc_read_prefs_validate (read_prefs, error)) {
      RETURN (false);
   }

   server_stream =
      mongoc_cluster_stream_for_server (&client->cluster, server_id, true /* reconnect ok */, NULL, reply, error);

   if (server_stream) {
      mongoc_cmd_parts_init (&parts, client, db_name, MONGOC_QUERY_NONE, command);
      parts.read_prefs = read_prefs;

      ret = _mongoc_client_command_with_stream (client, &parts, read_prefs, server_stream, reply, error);

      mongoc_cmd_parts_cleanup (&parts);
      mongoc_server_stream_cleanup (server_stream);
      RETURN (ret);
   } else {
      /* stream_for_server initialized reply */
      RETURN (false);
   }
}


static void
_mongoc_client_prepare_killcursors_command (int64_t cursor_id, const char *collection, bson_t *command)
{
   bson_array_builder_t *child;

   bson_append_utf8 (command, "killCursors", 11, collection, -1);
   bson_append_array_builder_begin (command, "cursors", 7, &child);
   bson_array_builder_append_int64 (child, cursor_id);
   bson_append_array_builder_end (command, child);
}


void
_mongoc_client_kill_cursor (mongoc_client_t *client,
                            uint32_t server_id,
                            int64_t cursor_id,
                            int64_t operation_id,
                            const char *db,
                            const char *collection,
                            mongoc_client_session_t *cs)
{
   mongoc_server_stream_t *server_stream;

   ENTRY;

   BSON_ASSERT_PARAM (client);
   BSON_ASSERT (cursor_id);

   /* don't attempt reconnect if server unavailable, and ignore errors */
   server_stream =
      mongoc_cluster_stream_for_server (&client->cluster, server_id, false /* reconnect_ok */, NULL, NULL, NULL);

   if (!server_stream) {
      return;
   }

   if (db && collection) {
      _mongoc_client_killcursors_command (&client->cluster, server_stream, cursor_id, db, collection, cs);
   } else {
      _mongoc_client_op_killcursors (&client->cluster, server_stream, cursor_id, operation_id, db, collection);
   }

   mongoc_server_stream_cleanup (server_stream);

   EXIT;
}


static void
_mongoc_client_monitor_op_killcursors (mongoc_cluster_t *cluster,
                                       mongoc_server_stream_t *server_stream,
                                       int64_t cursor_id,
                                       int64_t operation_id,
                                       const char *db,
                                       const char *collection)
{
   bson_t doc;
   mongoc_client_t *client;
   mongoc_apm_command_started_t event;

   ENTRY;

   client = cluster->client;

   if (!client->apm_callbacks.started) {
      return;
   }

   bson_init (&doc);
   _mongoc_client_prepare_killcursors_command (cursor_id, collection, &doc);
   mongoc_apm_command_started_init (&event,
                                    &doc,
                                    db,
                                    "killCursors",
                                    cluster->request_id,
                                    operation_id,
                                    &server_stream->sd->host,
                                    server_stream->sd->id,
                                    &server_stream->sd->service_id,
                                    server_stream->sd->server_connection_id,
                                    NULL,
                                    client->apm_context);

   client->apm_callbacks.started (&event);
   mongoc_apm_command_started_cleanup (&event);
   bson_destroy (&doc);

   EXIT;
}


static void
_mongoc_client_monitor_op_killcursors_succeeded (mongoc_cluster_t *cluster,
                                                 int64_t duration,
                                                 mongoc_server_stream_t *server_stream,
                                                 int64_t cursor_id,
                                                 int64_t operation_id,
                                                 const char *db)
{
   mongoc_client_t *client;
   bson_t doc;
   bson_array_builder_t *cursors_unknown;
   mongoc_apm_command_succeeded_t event;

   ENTRY;

   client = cluster->client;

   if (!client->apm_callbacks.succeeded) {
      EXIT;
   }

   /* fake server reply to killCursors command: {ok: 1, cursorsUnknown: [42]} */
   bson_init (&doc);
   bson_append_int32 (&doc, "ok", 2, 1);
   bson_append_array_builder_begin (&doc, "cursorsUnknown", 14, &cursors_unknown);
   bson_array_builder_append_int64 (cursors_unknown, cursor_id);
   bson_append_array_builder_end (&doc, cursors_unknown);

   mongoc_apm_command_succeeded_init (&event,
                                      duration,
                                      &doc,
                                      "killCursors",
                                      db,
                                      cluster->request_id,
                                      operation_id,
                                      &server_stream->sd->host,
                                      server_stream->sd->id,
                                      &server_stream->sd->service_id,
                                      server_stream->sd->server_connection_id,
                                      false,
                                      client->apm_context);

   client->apm_callbacks.succeeded (&event);

   mongoc_apm_command_succeeded_cleanup (&event);
   bson_destroy (&doc);
}


static void
_mongoc_client_monitor_op_killcursors_failed (mongoc_cluster_t *cluster,
                                              int64_t duration,
                                              mongoc_server_stream_t *server_stream,
                                              const bson_error_t *error,
                                              int64_t operation_id,
                                              const char *db)
{
   mongoc_client_t *client;
   bson_t doc;
   mongoc_apm_command_failed_t event;

   ENTRY;

   client = cluster->client;

   if (!client->apm_callbacks.failed) {
      EXIT;
   }

   /* fake server reply to killCursors command: {ok: 0} */
   bson_init (&doc);
   bson_append_int32 (&doc, "ok", 2, 0);

   mongoc_apm_command_failed_init (&event,
                                   duration,
                                   "killCursors",
                                   db,
                                   error,
                                   &doc,
                                   cluster->request_id,
                                   operation_id,
                                   &server_stream->sd->host,
                                   server_stream->sd->id,
                                   &server_stream->sd->service_id,
                                   server_stream->sd->server_connection_id,
                                   false,
                                   client->apm_context);

   client->apm_callbacks.failed (&event);

   mongoc_apm_command_failed_cleanup (&event);
   bson_destroy (&doc);
}


static void
_mongoc_client_op_killcursors (mongoc_cluster_t *cluster,
                               mongoc_server_stream_t *server_stream,
                               int64_t cursor_id,
                               int64_t operation_id,
                               const char *db,
                               const char *collection)
{
   BSON_ASSERT_PARAM (cluster);
   BSON_ASSERT_PARAM (server_stream);
   BSON_ASSERT (db || true);
   BSON_ASSERT (collection || true);

   const bool has_ns = db && collection;
   const int64_t started = bson_get_monotonic_time ();

   mcd_rpc_message *const rpc = mcd_rpc_message_new ();

   {
      int32_t message_length = 0;

      message_length += mcd_rpc_header_set_message_length (rpc, 0);
      message_length += mcd_rpc_header_set_request_id (rpc, ++cluster->request_id);
      message_length += mcd_rpc_header_set_response_to (rpc, 0);
      message_length += mcd_rpc_header_set_op_code (rpc, MONGOC_OP_CODE_KILL_CURSORS);

      message_length += sizeof (int32_t); // ZERO
      message_length += mcd_rpc_op_kill_cursors_set_cursor_ids (rpc, &cursor_id, 1);

      mcd_rpc_message_set_length (rpc, message_length);
   }

   if (has_ns) {
      _mongoc_client_monitor_op_killcursors (cluster, server_stream, cursor_id, operation_id, db, collection);
   }

   bson_error_t error;
   const bool res = mongoc_cluster_legacy_rpc_sendv_to_server (cluster, rpc, server_stream, &error);

   if (has_ns) {
      if (res) {
         _mongoc_client_monitor_op_killcursors_succeeded (
            cluster, bson_get_monotonic_time () - started, server_stream, cursor_id, operation_id, db);
      } else {
         _mongoc_client_monitor_op_killcursors_failed (
            cluster, bson_get_monotonic_time () - started, server_stream, &error, operation_id, db);
      }
   }

   mcd_rpc_message_destroy (rpc);
}


static void
_mongoc_client_killcursors_command (mongoc_cluster_t *cluster,
                                    mongoc_server_stream_t *server_stream,
                                    int64_t cursor_id,
                                    const char *db,
                                    const char *collection,
                                    mongoc_client_session_t *cs)
{
   bson_t command = BSON_INITIALIZER;
   mongoc_cmd_parts_t parts;

   ENTRY;

   _mongoc_client_prepare_killcursors_command (cursor_id, collection, &command);
   mongoc_cmd_parts_init (&parts, cluster->client, db, MONGOC_QUERY_SECONDARY_OK, &command);
   parts.assembled.operation_id = ++cluster->operation_id;
   mongoc_cmd_parts_set_session (&parts, cs);

   if (mongoc_cmd_parts_assemble (&parts, server_stream, NULL)) {
      /* Find, getMore And killCursors Commands Spec: "The result from the
       * killCursors command MAY be safely ignored."
       */
      (void) mongoc_cluster_run_command_monitored (cluster, &parts.assembled, NULL, NULL);
   }

   mongoc_cmd_parts_cleanup (&parts);
   bson_destroy (&command);

   EXIT;
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_kill_cursor --
 *
 *       Destroy a cursor on the server.
 *
 *       NOTE: this is only reliable when connected to a single mongod or
 *       mongos. If connected to a replica set, the driver attempts to
 *       kill the cursor on the primary. If connected to multiple mongoses
 *       the kill-cursors message is sent to a *random* mongos.
 *
 *       If no primary, mongos, or standalone server is known, return
 *       without attempting to reconnect.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

void
mongoc_client_kill_cursor (mongoc_client_t *client, int64_t cursor_id)
{
   BSON_ASSERT_PARAM (client);

   mongoc_topology_t *const topology = BSON_ASSERT_PTR_INLINE (client)->topology;
   mongoc_server_description_t const *selected_server;
   mongoc_read_prefs_t *read_prefs;
   bson_error_t error;
   uint32_t server_id = 0;
   mc_shared_tpld td = mc_tpld_take_ref (topology);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   if (!mongoc_topology_compatible (td.ptr, NULL, &error)) {
      MONGOC_ERROR ("Could not kill cursor: %s", error.message);
      mc_tpld_drop_ref (&td);
      mongoc_read_prefs_destroy (read_prefs);
      return;
   }

   /* see if there's a known writable server - do no I/O or retries */
   selected_server = mongoc_topology_description_select (td.ptr,
                                                         MONGOC_SS_WRITE,
                                                         read_prefs,
                                                         NULL /* chosen read mode */,
                                                         NULL /* deprioritized servers */,
                                                         topology->local_threshold_msec);

   if (selected_server) {
      server_id = selected_server->id;
   }

   if (server_id) {
      _mongoc_client_kill_cursor (
         client, server_id, cursor_id, 0 /* operation_id */, NULL /* db */, NULL /* collection */, NULL /* session */);
   } else {
      MONGOC_INFO ("No server available for mongoc_client_kill_cursor");
   }

   mongoc_read_prefs_destroy (read_prefs);
   mc_tpld_drop_ref (&td);
}


char **
mongoc_client_get_database_names (mongoc_client_t *client, bson_error_t *error)
{
   return mongoc_client_get_database_names_with_opts (client, NULL, error);
}


char **
mongoc_client_get_database_names_with_opts (mongoc_client_t *client, const bson_t *opts, bson_error_t *error)
{
   bson_iter_t iter;
   const char *name;
   char **ret = NULL;
   int i = 0;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t cmd = BSON_INITIALIZER;

   BSON_ASSERT_PARAM (client);
   BSON_APPEND_INT32 (&cmd, "listDatabases", 1);
   BSON_APPEND_BOOL (&cmd, "nameOnly", true);

   /* ignore client read prefs */
   cursor = _mongoc_cursor_array_new (client, "admin", &cmd, opts, "databases");
   bson_destroy (&cmd);

   while (mongoc_cursor_next (cursor, &doc)) {
      if (bson_iter_init (&iter, doc) && bson_iter_find (&iter, "name") && BSON_ITER_HOLDS_UTF8 (&iter) &&
          (name = bson_iter_utf8 (&iter, NULL))) {
         ret = (char **) bson_realloc (ret, sizeof (char *) * (i + 2));
         ret[i] = bson_strdup (name);
         ret[++i] = NULL;
      }
   }

   if (!ret && !mongoc_cursor_error (cursor, error)) {
      ret = (char **) bson_malloc0 (sizeof (void *));
   }

   mongoc_cursor_destroy (cursor);

   return ret;
}


mongoc_cursor_t *
mongoc_client_find_databases (mongoc_client_t *client, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);
   BSON_UNUSED (error);

   /* existing bug in this deprecated API: error pointer is unused */
   return mongoc_client_find_databases_with_opts (client, NULL);
}


mongoc_cursor_t *
mongoc_client_find_databases_with_opts (mongoc_client_t *client, const bson_t *opts)
{
   bson_t cmd = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;

   BSON_ASSERT_PARAM (client);
   BSON_APPEND_INT32 (&cmd, "listDatabases", 1);
   cursor = _mongoc_cursor_array_new (client, "admin", &cmd, opts, "databases");
   bson_destroy (&cmd);
   return cursor;
}


int32_t
mongoc_client_get_max_message_size (mongoc_client_t *client) /* IN */
{
   BSON_ASSERT_PARAM (client);

   return mongoc_cluster_get_max_msg_size (&client->cluster);
}


int32_t
mongoc_client_get_max_bson_size (mongoc_client_t *client) /* IN */
{
   BSON_ASSERT_PARAM (client);

   return mongoc_cluster_get_max_bson_obj_size (&client->cluster);
}


bool
mongoc_client_get_server_status (mongoc_client_t *client,         /* IN */
                                 mongoc_read_prefs_t *read_prefs, /* IN */
                                 bson_t *reply,                   /* OUT */
                                 bson_error_t *error)             /* OUT */
{
   bson_t cmd = BSON_INITIALIZER;
   bool ret = false;

   BSON_ASSERT_PARAM (client);

   BSON_APPEND_INT32 (&cmd, "serverStatus", 1);
   ret = mongoc_client_command_simple (client, "admin", &cmd, read_prefs, reply, error);
   bson_destroy (&cmd);

   return ret;
}


void
mongoc_client_set_stream_initiator (mongoc_client_t *client, mongoc_stream_initiator_t initiator, void *user_data)
{
   BSON_ASSERT_PARAM (client);

   if (!initiator) {
      initiator = mongoc_client_default_stream_initiator;
      user_data = client;
   } else {
      MONGOC_DEBUG ("Using custom stream initiator.");
   }

   client->initiator = initiator;
   client->initiator_data = user_data;

   if (client->topology->single_threaded) {
      mongoc_topology_scanner_set_stream_initiator (client->topology->scanner, initiator, user_data);
   }
}


bool
_mongoc_client_set_apm_callbacks_private (mongoc_client_t *client, mongoc_apm_callbacks_t *callbacks, void *context)
{
   BSON_ASSERT_PARAM (client);

   if (callbacks) {
      memcpy (&client->apm_callbacks, callbacks, sizeof (mongoc_apm_callbacks_t));
   } else {
      memset (&client->apm_callbacks, 0, sizeof (mongoc_apm_callbacks_t));
   }

   client->apm_context = context;

   /* A client pool sets APM callbacks for the entire pool. */
   if (client->topology->single_threaded) {
      mongoc_topology_set_apm_callbacks (client->topology,
                                         /* We are safe to modify the shared_descr directly, since we are
                                          * single-threaded */
                                         mc_tpld_unsafe_get_mutable (client->topology),
                                         callbacks,
                                         context);
   }

   return true;
}


bool
mongoc_client_set_apm_callbacks (mongoc_client_t *client, mongoc_apm_callbacks_t *callbacks, void *context)
{
   BSON_ASSERT_PARAM (client);

   if (!client->topology->single_threaded) {
      MONGOC_ERROR ("Cannot set callbacks on a pooled client, use "
                    "mongoc_client_pool_set_apm_callbacks");
      return false;
   }

   return _mongoc_client_set_apm_callbacks_private (client, callbacks, context);
}

mongoc_server_description_t *
mongoc_client_get_server_description (mongoc_client_t *client, uint32_t server_id)
{
   BSON_ASSERT_PARAM (client);

   mongoc_server_description_t *ret;
   mc_shared_tpld td = mc_tpld_take_ref (client->topology);
   mongoc_server_description_t const *sd =
      mongoc_topology_description_server_by_id_const (td.ptr, server_id, NULL /* <- the error info isn't useful */);
   ret = mongoc_server_description_new_copy (sd);
   mc_tpld_drop_ref (&td);
   return ret;
}


mongoc_server_description_t **
mongoc_client_get_server_descriptions (const mongoc_client_t *client, size_t *n /* OUT */)
{
   BSON_ASSERT_PARAM (client);

   mc_shared_tpld td = mc_tpld_take_ref (BSON_ASSERT_PTR_INLINE (client)->topology);
   mongoc_server_description_t **const sds =
      mongoc_topology_description_get_servers (td.ptr, BSON_ASSERT_PTR_INLINE (n));
   mc_tpld_drop_ref (&td);
   return sds;
}


void
mongoc_server_descriptions_destroy_all (mongoc_server_description_t **sds, size_t n)
{
   size_t i;

   for (i = 0; i < n; ++i) {
      mongoc_server_description_destroy (sds[i]);
   }

   bson_free (sds);
}


mongoc_server_description_t *
mongoc_client_select_server (mongoc_client_t *client,
                             bool for_writes,
                             const mongoc_read_prefs_t *prefs,
                             bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);

   mongoc_ss_optype_t optype = for_writes ? MONGOC_SS_WRITE : MONGOC_SS_READ;
   mongoc_server_description_t *sd;

   if (for_writes && prefs) {
      bson_set_error (error,
                      MONGOC_ERROR_SERVER_SELECTION,
                      MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                      "Cannot use read preferences with for_writes = true");
      return NULL;
   }

   if (!_mongoc_read_prefs_validate (prefs, error)) {
      return NULL;
   }

   sd = mongoc_topology_select (client->topology, optype, prefs, NULL /* chosen read mode */, error);
   if (!sd) {
      return NULL;
   }

   if (mongoc_cluster_check_interval (&client->cluster, sd->id)) {
      /* check not required, or it succeeded */
      return sd;
   }

   /* check failed, retry once */
   mongoc_server_description_destroy (sd);
   sd = mongoc_topology_select (client->topology, optype, prefs, NULL /* chosen read mode */, error);
   if (sd) {
      return sd;
   }

   return NULL;
}

bool
mongoc_client_set_error_api (mongoc_client_t *client, int32_t version)
{
   BSON_ASSERT_PARAM (client);

   if (!client->topology->single_threaded) {
      MONGOC_ERROR ("Cannot set Error API Version on a pooled client, use "
                    "mongoc_client_pool_set_error_api");
      return false;
   }

   if (version != MONGOC_ERROR_API_VERSION_LEGACY && version != MONGOC_ERROR_API_VERSION_2) {
      MONGOC_ERROR ("Unsupported Error API Version: %" PRId32, version);
      return false;
   }

   if (client->error_api_set) {
      MONGOC_ERROR ("Can only set Error API Version once");
      return false;
   }

   client->error_api_version = version;
   client->error_api_set = true;

   return true;
}

bool
mongoc_client_set_appname (mongoc_client_t *client, const char *appname)
{
   BSON_ASSERT_PARAM (client);

   if (!client->topology->single_threaded) {
      MONGOC_ERROR ("Cannot call set_appname on a client from a pool");
      return false;
   }

   return _mongoc_topology_set_appname (client->topology, appname);
}

mongoc_server_session_t *
_mongoc_client_pop_server_session (mongoc_client_t *client, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);

   return _mongoc_topology_pop_server_session (client->topology, error);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_client_lookup_session --
 *
 *       Retrieve a mongoc_client_session_t associated with @client_session_id.
 *       Use this to find the "lsid" and "$clusterTime" to send in the server
 *       command.
 *
 * Returns:
 *       True on success, false on error and @error is set. Will return false
 *       if the session is from an outdated client generation, a holdover
 *       from before a call to mongoc_client_reset.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_client_lookup_session (const mongoc_client_t *client,
                               uint32_t client_session_id,
                               mongoc_client_session_t **cs /* OUT */,
                               bson_error_t *error /* OUT */)
{
   ENTRY;
   BSON_ASSERT_PARAM (client);

   *cs = mongoc_set_get (client->client_sessions, client_session_id);

   if (*cs) {
      RETURN (true);
   }

   bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid sessionId");

   RETURN (false);
}

void
_mongoc_client_unregister_session (mongoc_client_t *client, mongoc_client_session_t *session)
{
   BSON_ASSERT_PARAM (client);

   mongoc_set_rm (client->client_sessions, session->client_session_id);
}

void
_mongoc_client_push_server_session (mongoc_client_t *client, mongoc_server_session_t *server_session)
{
   BSON_ASSERT_PARAM (client);

   _mongoc_topology_push_server_session (client->topology, server_session);
}

/*
 *--------------------------------------------------------------------------
 *
 * mongoc_client_end_sessions --
 *
 *       End all server sessions in the topology's server session pool.
 *       Don't block long: if server selection or connecting fails, quit.
 *
 *       The server session pool becomes invalid, but may not be empty.
 *       Destroy the topology after this without using any sessions.
 *
 *--------------------------------------------------------------------------
 */

void
_mongoc_client_end_sessions (mongoc_client_t *client)
{
   mongoc_topology_t *t = client->topology;
   mongoc_read_prefs_t *prefs;
   bson_error_t error;
   uint32_t server_id;
   bson_t cmd;
   mongoc_server_stream_t *stream;
   mongoc_cmd_parts_t parts;
   mongoc_cluster_t *cluster = &client->cluster;
   bool r;

   BSON_ASSERT_PARAM (client);

   while (!mongoc_server_session_pool_is_empty (t->session_pool)) {
      prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY_PREFERRED);
      server_id = mongoc_topology_select_server_id (
         t, MONGOC_SS_READ, prefs, NULL /* chosen read mode */, NULL /* deprioritized servers */, &error);

      mongoc_read_prefs_destroy (prefs);
      if (!server_id) {
         MONGOC_WARNING ("Couldn't send \"endSessions\": %s", error.message);
         return;
      }

      stream = mongoc_cluster_stream_for_server (cluster, server_id, false /* reconnect_ok */, NULL, NULL, &error);

      if (!stream) {
         MONGOC_WARNING ("Couldn't send \"endSessions\": %s", error.message);
         return;
      }

      /* end sessions in chunks */
      while (_mongoc_topology_end_sessions_cmd (t, &cmd)) {
         mongoc_cmd_parts_init (&parts, client, "admin", MONGOC_QUERY_SECONDARY_OK, &cmd);
         parts.assembled.operation_id = ++cluster->operation_id;
         parts.prohibit_lsid = true;

         r = mongoc_cmd_parts_assemble (&parts, stream, &error);
         if (!r) {
            MONGOC_WARNING ("Couldn't construct \"endSessions\" command: %s", error.message);
         } else {
            r = mongoc_cluster_run_command_monitored (cluster, &parts.assembled, NULL, &error);

            if (!r) {
               MONGOC_WARNING ("Couldn't send \"endSessions\": %s", error.message);
            }
         }

         mongoc_cmd_parts_cleanup (&parts);

         if (!mongoc_cluster_stream_valid (cluster, stream)) {
            /* The stream was invalidated as a result of a network error, so we
             * stop sending commands. */
            break;
         }

         bson_destroy (&cmd);
      }

      bson_destroy (&cmd);
      mongoc_server_stream_cleanup (stream);
   }
}

void
mongoc_client_reset (mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   client->generation++;

   /* Client sessions are owned and destroyed by the user, but we keep
      local pointers to them for reference. On reset, clear our local
      set without destroying the sessions or calling endSessions.
      client_sessions has no dtor, so it won't destroy its items.

      Destroying the local cache of client sessions here ensures they
      cannot be used by future operations--lookup for them will fail. */
   mongoc_set_destroy (client->client_sessions);
   client->client_sessions = mongoc_set_new (8, NULL, NULL);

   /* Server sessions are owned by us, so we clear the pool on reset. */
   mongoc_server_session_pool_clear (client->topology->session_pool);
}

mongoc_change_stream_t *
mongoc_client_watch (mongoc_client_t *client, const bson_t *pipeline, const bson_t *opts)
{
   return _mongoc_change_stream_new_from_client (client, pipeline, opts);
}

bool
mongoc_client_enable_auto_encryption (mongoc_client_t *client, mongoc_auto_encryption_opts_t *opts, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);

   if (!client->topology->single_threaded) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Cannot enable auto encryption on a pooled client, use "
                      "mongoc_client_pool_enable_auto_encryption");
      return false;
   }
   return _mongoc_cse_client_enable_auto_encryption (client, opts, error);
}

bool
mongoc_client_set_server_api (mongoc_client_t *client, const mongoc_server_api_t *api, bson_error_t *error)
{
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT_PARAM (api);

   if (client->is_pooled) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_API_FROM_POOL,
                      "Cannot set server api on a client checked out from a pool");
      return false;
   }

   if (mongoc_client_uses_server_api (client)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_API_ALREADY_SET,
                      "Cannot set server api more than once per client");
      return false;
   }

   client->api = mongoc_server_api_copy (api);
   _mongoc_topology_scanner_set_server_api (client->topology->scanner, api);
   return true;
}

mongoc_server_description_t *
mongoc_client_get_handshake_description (mongoc_client_t *client, uint32_t server_id, bson_t *opts, bson_error_t *error)
{
   mongoc_server_stream_t *server_stream;
   mongoc_server_description_t *sd;

   BSON_ASSERT_PARAM (client);
   BSON_UNUSED (opts);

   server_stream = mongoc_cluster_stream_for_server (
      &client->cluster, server_id, true /* reconnect */, NULL /* client session */, NULL /* reply */, error);
   if (!server_stream) {
      return NULL;
   }

   sd = mongoc_server_description_new_copy (server_stream->sd);
   mongoc_server_stream_cleanup (server_stream);
   return sd;
}

bool
mongoc_client_uses_server_api (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return mongoc_topology_uses_server_api (client->topology);
}

bool
mongoc_client_uses_loadbalanced (const mongoc_client_t *client)
{
   BSON_ASSERT_PARAM (client);

   return mongoc_topology_uses_loadbalanced (client->topology);
}
