#include <mongoc/mongoc.h>
#include <mongoc/mongoc-socket-private.h>
#include <mongoc/mongoc-host-list-private.h>
#include <mongoc/mongoc-util-private.h>
#include <mongoc/mongoc-stream-private.h>
#include <mongoc/utlist.h>

#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "mock_server/mock-server.h"
#include "test-libmongoc.h"

#define TIMEOUT 20000 /* milliseconds */

/* happy eyeballs (he) testing. */
typedef struct he_testcase_server {
   /* { "ipv4", "ipv6", NULL } */
   char *type;
   /* if true, this closes the server socket before the client establishes
    * connection. */
   bool close_before_connection;
   /* how long before the mock server calls `listen` on the server socket.
    * this delays the client from establishing a connection. */
   int listen_delay_ms;
} he_testcase_server_t;

typedef struct he_testcase_client {
   /* { "ipv4", "ipv6", "both" } */
   char *type;
   int64_t dns_cache_timeout_ms;
} he_testcase_client_t;

typedef struct he_testcase_expected {
   /* { "ipv4", "ipv6", "neither" }. which connection succeeds (if any). */
   char *conn_succeeds_to;
   /* how many async commands should be created at the start. */
   int initial_acmds;
   /* bounds for the server selection to finish. */
   int duration_min_ms;
   int duration_max_ms;
} he_testcase_expected_t;

typedef struct he_testcase_state {
   mock_server_t *mock_server;
   mongoc_host_list_t host;
   mongoc_topology_scanner_t *ts;
   int64_t start;
   int last_duration; /* set if timing fails, so it can be retried once. */
} he_testcase_state_t;

typedef struct he_testcase {
   he_testcase_client_t client;
   he_testcase_server_t servers[2];
   he_testcase_expected_t expected;
   he_testcase_state_t state;
} he_testcase_t;

typedef ssize_t (*poll_fn_t) (mongoc_stream_poll_t *streams, size_t nstreams, int32_t timeout);

static poll_fn_t gOriginalPoll;
static he_testcase_t *gCurrentTestCase;

/* if the server testcase specifies a delay or hangup, overwrite the poll
 * response. */
static int
_override_poll_response (he_testcase_server_t *server, mongoc_stream_poll_t *poller)
{
   if (server->listen_delay_ms) {
      int64_t now = bson_get_monotonic_time ();
      if (gCurrentTestCase->state.start + server->listen_delay_ms * 1000 > now) {
         /* should still "sleep". */
         int delta = 0;
         if (poller->revents) {
            delta = -1;
         }
         poller->revents = 0;
         return delta;
      }
   }
   if (server->close_before_connection) {
      poller->revents = POLLHUP;
   }
   return 0;
}

/* get the server testcase that this client stream is connected to (if one
 * exists). */
static he_testcase_server_t *
_server_for_client (mongoc_stream_t *stream)
{
   int i;
   mongoc_socket_t *sock;
   char *stream_type = "ipv4";

   BSON_ASSERT (stream->type == MONGOC_STREAM_SOCKET);
   sock = mongoc_stream_socket_get_socket ((mongoc_stream_socket_t *) stream);
   if (sock->domain == AF_INET6) {
      stream_type = "ipv6";
   }

   for (i = 0; i < 2; i++) {
      const char *server_type = gCurrentTestCase->servers[i].type;
      if (!server_type) {
         break;
      }
      if (strcmp (server_type, stream_type) == 0) {
         return gCurrentTestCase->servers + i;
      }
   }
   return NULL;
}

static ssize_t
_mock_poll (mongoc_stream_poll_t *streams, size_t nstreams, int32_t timeout)
{
   ssize_t starting_nactive;
   /* call the real poll first. */
   /* TODO CDRIVER-2542: ZSeries appears to have excessive delay with repeated
    * calls to poll. As a workaround, set the poll timeout to 5ms. */
   ssize_t nactive = gOriginalPoll (streams, nstreams, 5);

   BSON_UNUSED (timeout);

   starting_nactive = nactive;

   /* check if any of the poll responses need to be overwritten. */
   for (size_t i = 0u; i < nstreams; i++) {
      mongoc_stream_t *stream = mongoc_stream_get_root_stream (streams[i].stream);
      he_testcase_server_t *server = _server_for_client (stream);

      if (server) {
         nactive += _override_poll_response (server, streams + i);
      }
   }
   if (starting_nactive > 0 && nactive == 0) {
      /* if there were active poll responses which were all silenced,
       * sleep for a little while since subsequent calls to poll may not have
       * any delay. */
      _mongoc_usleep (5 * 1000);
   }
   return nactive;
}

static mongoc_stream_t *
_mock_initiator (mongoc_async_cmd_t *acmd)
{
   mongoc_stream_t *stream = _mongoc_topology_scanner_tcp_initiate (acmd);
   /* override poll */
   gOriginalPoll = stream->poll;
   stream->poll = _mock_poll;
   return stream;
}

static void
_test_scanner_callback (
   uint32_t id, const bson_t *bson, int64_t rtt_msec, void *data, const bson_error_t *error /* IN */)
{
   he_testcase_t *testcase = (he_testcase_t *) data;
   int should_succeed = strcmp (testcase->expected.conn_succeeds_to, "neither");

   BSON_UNUSED (id);
   BSON_UNUSED (bson);
   BSON_UNUSED (rtt_msec);

   if (should_succeed) {
      ASSERT_OR_PRINT (!error->code, (*error));
   } else {
      ASSERT_ERROR_CONTAINS ((*error), MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "connection refused");
   }
}

static void
_init_host (mongoc_host_list_t *host, uint16_t port, const char *type)
{
   char *host_str, *host_and_port;
   bool free_host_str = false;

   if (strcmp (type, "ipv4") == 0) {
      host_str = "127.0.0.1";
   } else if (strcmp (type, "ipv6") == 0) {
      host_str = "[::1]";
   } else {
      host_str = test_framework_getenv ("MONGOC_TEST_IPV4_AND_IPV6_HOST");
      if (host_str) {
         free_host_str = true;
      } else {
         /* default to localhost. */
         host_str = "localhost";
      }
   }

   host_and_port = bson_strdup_printf ("%s:%hu", host_str, port);
   BSON_ASSERT (_mongoc_host_list_from_string (host, host_and_port));
   if (free_host_str) {
      bson_free (host_str);
   }
   /* we should only have one host. */
   BSON_ASSERT (!host->next);
   bson_free (host_and_port);
}

static void
_testcase_setup (he_testcase_t *testcase)
{
   mock_server_t *mock_server = NULL;
   mock_server_bind_opts_t opts = {0};
   struct sockaddr_in ipv4_addr = {0};
   struct sockaddr_in6 ipv6_addr = {0};
   char *server_type = "both";

   /* if there is only one server, use that type. */
   if (testcase->servers[0].type && !testcase->servers[1].type) {
      server_type = testcase->servers[0].type;
   }

   if (strcmp ("both", server_type) == 0) {
      opts.bind_addr_len = sizeof (ipv6_addr);
      opts.family = AF_INET6;
      opts.ipv6_only = 0;
      ipv6_addr.sin6_family = AF_INET6;
      ipv6_addr.sin6_port = htons (0);
      ipv6_addr.sin6_addr = in6addr_any;
      opts.bind_addr = (struct sockaddr_in *) &ipv6_addr;
   } else if (strcmp ("ipv4", server_type) == 0) {
      opts.bind_addr_len = sizeof (ipv4_addr);
      opts.family = AF_INET;
      opts.ipv6_only = 0;
      ipv4_addr.sin_family = AF_INET;
      ipv4_addr.sin_port = htons (0);
      BSON_ASSERT (inet_pton (AF_INET, "127.0.0.1", &ipv4_addr.sin_addr));
      opts.bind_addr = &ipv4_addr;
   } else if (strcmp ("ipv6", server_type) == 0) {
      opts.bind_addr_len = sizeof (ipv6_addr);
      opts.family = AF_INET6;
      opts.ipv6_only = 1;
      ipv6_addr.sin6_family = AF_INET6;
      ipv6_addr.sin6_port = htons (0);
      BSON_ASSERT (inet_pton (AF_INET6, "::1", &ipv6_addr.sin6_addr));
      opts.bind_addr = (struct sockaddr_in *) &ipv6_addr;
   }

   mock_server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_set_bind_opts (mock_server, &opts);
   mock_server_run (mock_server);

   _init_host (&testcase->state.host, mock_server_get_port (mock_server), testcase->client.type);

   testcase->state.ts = mongoc_topology_scanner_new (NULL, NULL, &_test_scanner_callback, testcase, TIMEOUT);

   testcase->state.mock_server = mock_server;

   if (testcase->client.dns_cache_timeout_ms > 0) {
      _mongoc_topology_scanner_set_dns_cache_timeout (testcase->state.ts, testcase->client.dns_cache_timeout_ms);
   }
}

static void
_testcase_teardown (he_testcase_t *testcase)
{
   mock_server_destroy (testcase->state.mock_server);
   mongoc_topology_scanner_destroy (testcase->state.ts);
}

static void
_check_stream (mongoc_stream_t *stream, const char *expected, char *message)
{
   /* check the socket that the scanner found. */
   char *actual = "neither";
   if (stream) {
      mongoc_socket_t *sock = mongoc_stream_socket_get_socket ((mongoc_stream_socket_t *) stream);
      actual = (sock->domain == AF_INET) ? "ipv4" : "ipv6";
   }

   ASSERT_WITH_MSG (
      strcmp (expected, actual) == 0, "%s: expected %s stream but got %s stream\n", message, expected, actual);
}

static void
_testcase_run (he_testcase_t *testcase)
{
   /* construct mock servers. */
   mongoc_topology_scanner_t *ts = testcase->state.ts;
   mongoc_topology_scanner_node_t *node;
   he_testcase_expected_t *expected = &testcase->expected;
#ifndef _WIN32
   uint64_t duration_ms;
#endif
   mongoc_async_cmd_t *iter;

   gCurrentTestCase = testcase;
   testcase->state.start = bson_get_monotonic_time ();

   mongoc_topology_scanner_add (ts, &testcase->state.host, 1 /* any server id is ok. */, false);
   mongoc_topology_scanner_scan (ts, 1);
   /* how many commands should we have initially? */
   ASSERT_CMPINT ((int) (ts->async->ncmds), ==, expected->initial_acmds);

   DL_FOREACH (ts->async->cmds, iter)
   {
      iter->initiator = _mock_initiator;
   }

   mongoc_topology_scanner_work (ts);

#ifndef _WIN32
   /* Note: do not check time on Windows. Windows waits 1 second before refusing
    * connection to unused ports:
    * https://support.microsoft.com/en-us/help/175523/info-winsock-tcp-connection-performance-to-unused-ports
    */

   duration_ms = (bson_get_monotonic_time () - testcase->state.start) / (1000);

   {
      bool within_expected_duration =
         duration_ms >= expected->duration_min_ms && duration_ms < expected->duration_max_ms;
      if (!within_expected_duration) {
         /* this is a timing failure, this may have been a fluke, retry once. */
         ASSERT_WITH_MSG (!testcase->state.last_duration,
                          "Timing failed twice. Expected to take between %dms "
                          "and %dms. First duration was %dms, second was %dms.",
                          expected->duration_min_ms,
                          expected->duration_max_ms,
                          testcase->state.last_duration,
                          (int) duration_ms);
         testcase->state.last_duration = duration_ms;
      } else {
         /* clear the last duration in case succeeded on second try. */
         testcase->state.last_duration = 0;
      }
   }
#endif

   node = mongoc_topology_scanner_get_node (ts, 1);
   _check_stream (node->stream, expected->conn_succeeds_to, "checking client's final connection");
}

#define CLIENT(client) \
   {                   \
      #client          \
   }

#define CLIENT_WITH_DNS_CACHE_TIMEOUT(type, timeout) \
   {                                                 \
      #type, timeout                                 \
   }
#define HANGUP true
#define LISTEN false
#define SERVER(type, hangup) \
   {                         \
      #type, hangup          \
   }
#define DELAYED_SERVER(type, hangup, delay) \
   {                                        \
      #type, hangup, delay                  \
   }
#define SERVERS(...) \
   {                 \
      __VA_ARGS__    \
   }
#define DELAY_MS(val) val
#define DURATION_MS(min, max) (min), (max)
#define EXPECT(type, num_acmds, duration) \
   {                                      \
      #type, num_acmds, duration          \
   }
#define NCMDS(n) (n)

static void
_run_testcase (void *ctx)
{
   he_testcase_t *testcase = (he_testcase_t *) ctx;
retry:
   _testcase_setup (testcase);
   _testcase_run (testcase);
   _testcase_teardown (testcase);
   if (testcase->state.last_duration) {
      goto retry;
   }
}

static void
test_happy_eyeballs_dns_cache (void)
{
#define E 1000
   he_testcase_t testcase = {
      CLIENT_WITH_DNS_CACHE_TIMEOUT (both, 1000),
      SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, LISTEN)),
      EXPECT (ipv6, NCMDS (2), DURATION_MS (0, E)),
   };
   _testcase_setup (&testcase);
   _testcase_run (&testcase);
   /* disconnect the node so we perform another DNS lookup. */
   mongoc_topology_scanner_node_disconnect (testcase.state.ts->nodes, false);

   /* after running once, the topology scanner should have cached the DNS
    * result for IPv6. It should complete immediately. */
   testcase.expected.initial_acmds = 1;
   _testcase_run (&testcase);

   /* disconnect the node so we perform another DNS lookup. */
   mongoc_topology_scanner_node_disconnect (testcase.state.ts->nodes, false);

   /* wait for DNS cache to expire. */
   _mongoc_usleep (2000 * 1000);

   /* after running once, the topology scanner should have cached the DNS
    * result for IPv6. It should complete immediately. */
   testcase.expected.initial_acmds = 2;
   _testcase_run (&testcase);

   _testcase_teardown (&testcase);
#undef E
}

void
test_happy_eyeballs_install (TestSuite *suite)
{
#define E 1000 /* epsilon. wiggle room for time constraints. */
#define HE 250 /* delay before ipv4 if ipv6 does not finish. */
   int i, ntests;

   /*  This tests conformity to RFC-6555 (Happy Eyeballs) when the topology
    * scanner connects to a single server. The expected behavior is as follows:
    * - if a hostname has both A and AAAA records, attempt to connect to IPv6
    *   immediately, and schedule a delayed IPv4 connection attempt 250ms after.
    * - if IPv6 fails, schedule the IPv4 connection attempt immediately.
    * - whichever connection attempt succeeds first cancels the other.
    *
    * The testcases are specified in terms of the client and server.
    * Client
    *    - what address is trying connect (e.g. 127.0.0.1, ::1, localhost)?
    * Server
    *    - is the server listening on IPv4, IPv6, or both?
    *    - will it delay connection to any of these connections?
    *    - will it hang up on these connections immediately?
    * */
   static he_testcase_t he_testcases[] = {
      /* client ipv4. */
      {
         CLIENT (ipv4),
         SERVERS (SERVER (ipv4, LISTEN)),
         EXPECT (ipv4, NCMDS (1), DURATION_MS (0, E)),
      },
      {
         CLIENT (ipv4),
         SERVERS (SERVER (ipv6, LISTEN)),
         EXPECT (neither, NCMDS (1), DURATION_MS (0, E)),
      },
      {CLIENT (ipv4),
       SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, HANGUP)),
       EXPECT (ipv4, NCMDS (1), DURATION_MS (0, E))},
      {
         CLIENT (ipv4),
         SERVERS (SERVER (ipv4, HANGUP), SERVER (ipv6, HANGUP)),
         EXPECT (neither, NCMDS (1), DURATION_MS (0, E)),
      },
      /* client ipv6. */
      {
         CLIENT (ipv6),
         SERVERS (SERVER (ipv4, LISTEN)),
         EXPECT (neither, NCMDS (1), DURATION_MS (0, E)),
      },
      {
         CLIENT (ipv6),
         SERVERS (SERVER (ipv6, LISTEN)),
         EXPECT (ipv6, NCMDS (1), DURATION_MS (0, E)),
      },
      {
         CLIENT (ipv6),
         SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, LISTEN)),
         EXPECT (ipv6, NCMDS (1), DURATION_MS (0, E)),
      },
      {
         CLIENT (ipv6),
         SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, HANGUP)),
         EXPECT (neither, NCMDS (1), DURATION_MS (0, E)),
      },
      /* client both ipv4 and ipv6. */
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, LISTEN)),
         /* no delay, ipv6 fails immediately and ipv4 succeeds. */
         EXPECT (ipv4, NCMDS (2), DURATION_MS (0, E)),
      },
      {
         CLIENT (both),
         SERVERS (SERVER (ipv6, LISTEN)),
         /* no delay, ipv6 succeeds immediately. */
         EXPECT (ipv6, NCMDS (2), DURATION_MS (0, E)),
      },
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, LISTEN)),
         /* no delay, ipv6 succeeds immediately. */
         EXPECT (ipv6, NCMDS (2), DURATION_MS (0, E)),
      },
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, LISTEN), SERVER (ipv6, HANGUP)),
         /* no delay, ipv6 fails immediately and ipv4 succeeds. */
         EXPECT (ipv4, NCMDS (2), DURATION_MS (0, E)),
      },
      /* when both client is connecting to both ipv4 and ipv6 and server is
       * listening on both ipv4 and ipv6, test delaying the connections at
       * various times. */
      /* ipv6 {succeeds, fails} before ipv4 starts and {succeeds, fails} */

      {CLIENT (both),
       SERVERS (SERVER (ipv4, HANGUP), SERVER (ipv6, HANGUP)),
       EXPECT (neither, NCMDS (2), DURATION_MS (0, E))},
      /* ipv6 {succeeds, fails} after ipv4 starts but before ipv4 {succeeds,
         fails} */
      {
         CLIENT (both),
         SERVERS (DELAYED_SERVER (ipv4, LISTEN, DELAY_MS (2 * HE)), DELAYED_SERVER (ipv6, LISTEN, HE)),
         EXPECT (ipv6, NCMDS (2), DURATION_MS (HE, HE + E)),
      },
      {
         CLIENT (both),
         SERVERS (DELAYED_SERVER (ipv4, LISTEN, DELAY_MS (2 * HE)), DELAYED_SERVER (ipv6, HANGUP, DELAY_MS (HE))),
         EXPECT (ipv4, NCMDS (2), DURATION_MS (2 * HE, 2 * HE + E)),
      },
      {
         CLIENT (both),
         SERVERS (DELAYED_SERVER (ipv4, HANGUP, DELAY_MS (2 * HE)), DELAYED_SERVER (ipv6, HANGUP, DELAY_MS (HE))),
         EXPECT (neither, NCMDS (2), DURATION_MS (2 * HE, 2 * HE + E)),
      },
      /* ipv4 {succeeds,fails} after ipv6 {succeeds, fails}. */
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, LISTEN), DELAYED_SERVER (ipv6, LISTEN, DELAY_MS (HE + E))),
         /* ipv6 is delayed too long, ipv4 succeeds. */
         EXPECT (ipv4, NCMDS (2), DURATION_MS (HE, HE + E)),
      },
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, HANGUP), DELAYED_SERVER (ipv6, LISTEN, DELAY_MS (HE + E))),
         /* ipv6 is delayed, but ipv4 fails. */
         EXPECT (ipv6, NCMDS (2), DURATION_MS (HE + E, HE + 2 * E)),
      },
      {
         CLIENT (both),
         SERVERS (SERVER (ipv4, HANGUP), DELAYED_SERVER (ipv6, HANGUP, DELAY_MS (HE + E))),
         EXPECT (neither, NCMDS (2), DURATION_MS (HE + E, HE + 2 * E)),
      },
   };
#undef HE
#undef E
   ntests = sizeof (he_testcases) / sizeof (he_testcases[0]);
   for (i = 0; i < ntests; i++) {
      char *name = bson_strdup_printf ("/TOPOLOGY/happy_eyeballs/%d", i);
      TestSuite_AddFull (suite,
                         name,
                         _run_testcase,
                         NULL,
                         he_testcases + i,
                         test_framework_skip_if_no_dual_ip_hostname,
                         TestSuite_CheckMockServerAllowed);
      bson_free (name);
   }
   TestSuite_AddMockServerTest (suite,
                                "/TOPOLOGY/happy_eyeballs/dns_cache/",
                                test_happy_eyeballs_dns_cache,
                                test_framework_skip_if_no_dual_ip_hostname);
}
