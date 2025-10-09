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

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mongoc/mongoc.h"
#include "mongoc/mongoc-interrupt-private.h"
#include "mongoc/mongoc-client-private.h"
#include "common-thread-private.h"

static int64_t
_time_ms (void)
{
   return bson_get_monotonic_time () / 1000;
}

BSON_THREAD_FUN (_interrupt, future_void)
{
   future_t *future;
   future_value_t return_value;
   mongoc_interrupt_t *interrupt;

   future = future_void;
   interrupt = future_get_param (future, 0)->value.void_ptr_value;
   _mongoc_usleep (10 * 1000);
   _mongoc_interrupt_interrupt (interrupt);
   return_value.type = future_value_void_type;
   future_resolve (future, return_value);
   BSON_THREAD_RETURN;
}

/* Run an interrupt in a separate thread. */
static future_t *
_future_interrupt (mongoc_interrupt_t *interrupt)
{
   future_t *future;
   future_value_t *future_value;

   future = future_new (future_value_void_type, 1);
   future_value = future_get_param (future, 0);
   future_value_set_void_ptr (future_value, (void *) interrupt);
   future_start (future, _interrupt);
   return future;
}

static void
test_interrupt (void)
{
   mock_server_t *server;
   mongoc_interrupt_t *interrupt;
   mongoc_stream_poll_t *poller;
   future_t *future;
   const mongoc_uri_t *uri;
   mongoc_stream_t *stream;
   bson_error_t error;

   interrupt = _mongoc_interrupt_new (10000);

   /* Poll the interrupt for input. */
   poller = bson_malloc0 (sizeof (mongoc_stream_poll_t) * 1);
   poller[0].stream = _mongoc_interrupt_get_stream (interrupt);
   poller[0].events = POLLIN;

   /* Test that sending an interrupt before the poll executes quickly. */
   {
      const int64_t started_ms = _time_ms ();
      poller[0].revents = 0;
      _mongoc_interrupt_interrupt (interrupt);
      mongoc_stream_poll (poller, 1, 10000);
      _mongoc_interrupt_flush (interrupt);
      ASSERT_CMPTIME (_time_ms () - started_ms, 10000);
   }

   /* Test that an interrupt after polling executes quickly. */
   {
      const int64_t started_ms = _time_ms ();
      poller[0].revents = 0;
      future = _future_interrupt (interrupt);
      mongoc_stream_poll (poller, 1, 10000);
      _mongoc_interrupt_flush (interrupt);
      ASSERT_CMPTIME (_time_ms () - started_ms, 10000);
      future_wait (future);
      future_destroy (future);
   }

   /* Flushing with nothing queued up does not block. */
   {
      const int64_t started_ms = _time_ms ();
      _mongoc_interrupt_flush (interrupt);
      ASSERT_CMPTIME (_time_ms () - started_ms, 10000);
   }

   /* Test interrupting while polling on another socket. */
   server = mock_server_new ();
   mock_server_run (server);
   uri = mock_server_get_uri (server);
   stream = mongoc_client_connect_tcp (10000, mongoc_uri_get_hosts (uri), &error);
   ASSERT_OR_PRINT (stream, error);

   bson_free (poller);
   poller = bson_malloc0 (sizeof (mongoc_stream_poll_t) * 2);
   poller[0].stream = _mongoc_interrupt_get_stream (interrupt);
   poller[0].events = POLLIN;
   poller[1].stream = stream;
   poller[1].events = POLLIN;

   for (int i = 0; i < 10; i++) {
      const int64_t started_ms = _time_ms ();
      _mongoc_interrupt_interrupt (interrupt);
      mongoc_stream_poll (poller, 2, 10000);
      ASSERT_CMPTIME (_time_ms () - started_ms, 10000);
   }

   /* Swap the order of the streams polled. mongoc_stream_poll uses the poll
    * function associated with the first stream. */
   poller[0].revents = 0;
   poller[0].stream = stream;
   poller[1].revents = 0;
   poller[1].stream = _mongoc_interrupt_get_stream (interrupt);

   for (int i = 0; i < 10; i++) {
      const int64_t started_ms = _time_ms ();
      _mongoc_interrupt_interrupt (interrupt);
      mongoc_stream_poll (poller, 2, 10000);
      ASSERT_CMPTIME (_time_ms () - started_ms, 10000);
   }

   mongoc_stream_destroy (stream);

   mock_server_destroy (server);
   _mongoc_interrupt_destroy (interrupt);
   bson_free (poller);
}

void
test_interrupt_install (TestSuite *suite)
{
   TestSuite_AddMockServerTest (suite, "/interrupt", test_interrupt);
}
