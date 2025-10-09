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

#include <mongoc/mongoc-util-private.h>
#include "mongoc/mongoc-counters-private.h"
#include "mock_server/mock-server.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"
#include "TestSuite.h"
#include "mock_server/future-functions.h"

/* test statistics counters excluding OP_INSERT, OP_UPDATE, and OP_DELETE since
 * those were superseded by write commands in 2.6. */
#ifdef MONGOC_ENABLE_SHM_COUNTERS

/* define prev_* counters for testing convenience. */
#define COUNTER(ident, Category, Name, Description) static int32_t prev_##ident;
#include "mongoc/mongoc-counters.defs"
#undef COUNTER

/* helper to reset a prev_* counter */
#define RESET(ident) \
   bson_atomic_int32_exchange (&prev_##ident, mongoc_counter_##ident##_count (), bson_memory_order_seq_cst)

/* helper to compare and reset a prev_* counter. */
#define DIFF_AND_RESET(ident, cmp, expected)                 \
   do {                                                      \
      int32_t old_count = prev_##ident;                      \
      int32_t new_count = mongoc_counter_##ident##_count (); \
      int32_t _diff = new_count - old_count;                 \
      ASSERT_CMPINT32 (_diff, cmp, expected);                \
      RESET (ident);                                         \
   } while (0)

static void
reset_all_counters (void)
{
#define COUNTER(ident, Category, Name, Description) RESET (ident);
#include "mongoc/mongoc-counters.defs"
#undef COUNTER
}

/* create a client and disable server selection after performing it. */
static mongoc_client_t *
_client_new_disable_ss (bool use_compression)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_server_description_t *sd;
   bson_error_t err;

   uri = test_framework_get_uri ();
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 99999);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 99999);
   if (use_compression) {
      char *compressors = test_framework_get_compressors ();
      mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_COMPRESSORS, compressors);
      bson_free (compressors);
   }
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   sd = mongoc_client_select_server (client, true, NULL, &err);
   ASSERT_OR_PRINT (sd, err);
   mongoc_server_description_destroy (sd);
   // Trigger authentication handshake now to avoid interfering with ping test.
   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &err), err);
   mongoc_uri_destroy (uri);
   /* reset counters to exclude anything done in server selection. */
   reset_all_counters ();
   return client;
}


mongoc_collection_t *
_drop_and_populate_coll (mongoc_client_t *client)
{
   /* insert thrice. */
   mongoc_collection_t *coll;
   bool ret;
   bson_error_t err;
   int i;

   ASSERT (client);

   coll = mongoc_client_get_collection (client, "test", "test");
   mongoc_collection_drop (coll, NULL); /* don't care if ns not found. */
   for (i = 0; i < 3; i++) {
      ret = mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &err);
      ASSERT_OR_PRINT (ret, err);
   }
   return coll;
}


void
_ping (mongoc_client_t *client)
{
   bool ret;
   bson_error_t err;

   ASSERT (client);

   ret = mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &err);
   ASSERT_OR_PRINT (ret, err);
}


static void
test_counters_op_msg (void *ctx)
{
   mongoc_collection_t *coll;
   mongoc_cursor_t *cursor;
   const bson_t *bson;
   mongoc_client_t *client;

   BSON_UNUSED (ctx);

   client = _client_new_disable_ss (false);
   _ping (client);
   DIFF_AND_RESET (op_egress_msg, ==, 1);
   DIFF_AND_RESET (op_egress_total, ==, 1);
   DIFF_AND_RESET (op_ingress_msg, ==, 1);
   DIFF_AND_RESET (op_ingress_total, ==, 1);
   coll = _drop_and_populate_coll (client);
   DIFF_AND_RESET (op_egress_msg, ==, 4);
   DIFF_AND_RESET (op_egress_total, ==, 4);
   DIFF_AND_RESET (op_ingress_msg, ==, 4);
   DIFF_AND_RESET (op_ingress_total, ==, 4);
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL, NULL);
   while (mongoc_cursor_next (cursor, &bson))
      ;
   mongoc_cursor_destroy (cursor);
   DIFF_AND_RESET (op_egress_msg, >, 0);
   DIFF_AND_RESET (op_ingress_msg, >, 0);
   DIFF_AND_RESET (op_egress_query, ==, 0);
   DIFF_AND_RESET (op_ingress_reply, ==, 0);
   DIFF_AND_RESET (op_egress_total, >, 0);
   DIFF_AND_RESET (op_ingress_total, >, 0);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


static void
test_counters_op_compressed (void *ctx)
{
   mongoc_collection_t *coll;
   mongoc_client_t *client;

   BSON_UNUSED (ctx);

   client = _client_new_disable_ss (true);
   _ping (client);
   /* we count one OP_MSG and one OP_COMPRESSED for the same message. */
   DIFF_AND_RESET (op_egress_msg, ==, 1);
   DIFF_AND_RESET (op_egress_compressed, ==, 1);
   DIFF_AND_RESET (op_egress_total, ==, 2);
   DIFF_AND_RESET (op_ingress_msg, ==, 1);
   DIFF_AND_RESET (op_ingress_compressed, ==, 1);
   DIFF_AND_RESET (op_ingress_total, ==, 2);
   coll = _drop_and_populate_coll (client);
   DIFF_AND_RESET (op_egress_msg, ==, 4);
   DIFF_AND_RESET (op_egress_compressed, ==, 4);
   DIFF_AND_RESET (op_egress_total, ==, 8);
   DIFF_AND_RESET (op_ingress_msg, ==, 4);
   DIFF_AND_RESET (op_ingress_compressed, ==, 4);
   DIFF_AND_RESET (op_ingress_total, ==, 8);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


static void
test_counters_cursors (void)
{
   mongoc_collection_t *coll;
   mongoc_cursor_t *cursor;
   const bson_t *bson;
   mongoc_client_t *client;

   client = _client_new_disable_ss (false);
   coll = _drop_and_populate_coll (client);
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), tmp_bson ("{'batchSize': 1}"), NULL);
   DIFF_AND_RESET (cursors_active, ==, 1);
   while (mongoc_cursor_next (cursor, &bson))
      ;
   mongoc_cursor_destroy (cursor);
   DIFF_AND_RESET (cursors_active, ==, -1);
   DIFF_AND_RESET (cursors_disposed, ==, 1);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}


static void
test_counters_clients (void)
{
   mongoc_client_pool_t *client_pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   uri = test_framework_get_uri ();

   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 99999);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 99999);
   reset_all_counters ();
   client = test_framework_client_new_from_uri (uri, NULL);
   BSON_ASSERT (client);
   DIFF_AND_RESET (clients_active, ==, 1);
   DIFF_AND_RESET (clients_disposed, ==, 0);
   DIFF_AND_RESET (client_pools_active, ==, 0);
   DIFF_AND_RESET (client_pools_disposed, ==, 0);
   mongoc_client_destroy (client);
   DIFF_AND_RESET (clients_active, ==, -1);
   DIFF_AND_RESET (clients_disposed, ==, 1);
   DIFF_AND_RESET (client_pools_active, ==, 0);
   DIFF_AND_RESET (client_pools_disposed, ==, 0);
   /* check client pools. */
   client_pool = test_framework_client_pool_new_from_uri (uri, NULL);
   BSON_ASSERT (client_pool);
   DIFF_AND_RESET (clients_active, ==, 0);
   DIFF_AND_RESET (clients_disposed, ==, 0);
   DIFF_AND_RESET (client_pools_active, ==, 1);
   DIFF_AND_RESET (client_pools_disposed, ==, 0);
   client = mongoc_client_pool_pop (client_pool);
   DIFF_AND_RESET (clients_active, ==, 1);
   DIFF_AND_RESET (clients_disposed, ==, 0);
   DIFF_AND_RESET (client_pools_active, ==, 0);
   DIFF_AND_RESET (client_pools_disposed, ==, 0);
   mongoc_client_destroy (client);
   DIFF_AND_RESET (clients_active, ==, -1);
   DIFF_AND_RESET (clients_disposed, ==, 1);
   DIFF_AND_RESET (client_pools_active, ==, 0);
   DIFF_AND_RESET (client_pools_disposed, ==, 0);
   mongoc_client_pool_destroy (client_pool);
   DIFF_AND_RESET (clients_active, ==, 0);
   DIFF_AND_RESET (clients_disposed, ==, 0);
   DIFF_AND_RESET (client_pools_active, ==, -1);
   DIFF_AND_RESET (client_pools_disposed, ==, 1);
   mongoc_uri_destroy (uri);
}


static void
test_counters_streams (void *ctx)
{
   mongoc_client_t *client = _client_new_disable_ss (false);
   mongoc_socket_t *sock;
   mongoc_stream_t *stream_sock;
   mongoc_stream_t *buffered_stream_sock;
   mongoc_stream_t *file_stream;
   mongoc_gridfs_t *gridfs;
   mongoc_gridfs_file_t *file;
   mongoc_stream_t *gridfs_stream;
   bool used_ssl = false;
   bson_error_t err;
   char buf[16] = {0};
   const int TIMEOUT = 500;
   mongoc_gridfs_file_opt_t gridfs_opts = {0};
   bool ret;

   BSON_UNUSED (ctx);

   /* test ingress and egress of a stream to a server. */
   _ping (client);
   DIFF_AND_RESET (streams_egress, >, 0);
   DIFF_AND_RESET (streams_ingress, >, 0);
   /* test that creating and destroying each type of stream changes the
    * streams active and not active. */
   sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   DIFF_AND_RESET (streams_active, ==, 0);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   stream_sock = mongoc_stream_socket_new (sock);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   buffered_stream_sock = mongoc_stream_buffered_new (stream_sock, 16);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
#ifdef MONGOC_ENABLE_SSL
   do {
      const mongoc_ssl_opt_t *default_opts = mongoc_ssl_opt_get_default ();
      mongoc_ssl_opt_t opts = *default_opts;
      mongoc_stream_t *ssl_buffered_stream_socket;

      ssl_buffered_stream_socket = mongoc_stream_tls_new_with_hostname (buffered_stream_sock, NULL, &opts, 0);
      DIFF_AND_RESET (streams_active, ==, 1);
      DIFF_AND_RESET (streams_disposed, ==, 0);
      mongoc_stream_destroy (ssl_buffered_stream_socket);
      DIFF_AND_RESET (streams_active, ==, -3);
      DIFF_AND_RESET (streams_disposed, ==, 3);
      used_ssl = true;
   } while (0);
#endif
   if (!used_ssl) {
      mongoc_stream_destroy (buffered_stream_sock);
      DIFF_AND_RESET (streams_active, ==, -2);
      DIFF_AND_RESET (streams_disposed, ==, 2);
   }
/* check a file stream. */
#ifdef WIN32
   file_stream = mongoc_stream_file_new_for_path (BINARY_DIR "/temp.dat", O_CREAT | O_WRONLY | O_TRUNC, _S_IWRITE);
#else
   file_stream = mongoc_stream_file_new_for_path (BINARY_DIR "/temp.dat", O_CREAT | O_WRONLY | O_TRUNC, S_IRWXU);
#endif
   BSON_ASSERT (file_stream);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   mongoc_stream_write (file_stream, buf, 16, TIMEOUT);
   DIFF_AND_RESET (streams_egress, ==, 16);
   DIFF_AND_RESET (streams_ingress, ==, 0);
   mongoc_stream_destroy (file_stream);
   DIFF_AND_RESET (streams_active, ==, -1);
   DIFF_AND_RESET (streams_disposed, ==, 1);
   file_stream = mongoc_stream_file_new_for_path (BINARY_DIR "/temp.dat", O_RDONLY, 0);
   BSON_ASSERT (file_stream);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   mongoc_stream_read (file_stream, buf, 16, 0, TIMEOUT);
   DIFF_AND_RESET (streams_egress, ==, 0);
   DIFF_AND_RESET (streams_ingress, ==, 16);
   mongoc_stream_destroy (file_stream);
   DIFF_AND_RESET (streams_active, ==, -1);
   DIFF_AND_RESET (streams_disposed, ==, 1);
   unlink (BINARY_DIR "/temp.dat");
   /* check a gridfs stream. */
   gridfs = mongoc_client_get_gridfs (client, "test", "fs", &err);
   ASSERT_OR_PRINT (gridfs, err);
   ret = mongoc_gridfs_drop (gridfs, &err);
   ASSERT_OR_PRINT (ret, err);
   reset_all_counters ();
   gridfs_opts.filename = "example";
   file = mongoc_gridfs_create_file (gridfs, &gridfs_opts);
   ASSERT_OR_PRINT (file, err);
   gridfs_stream = mongoc_stream_gridfs_new (file);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   mongoc_stream_write (gridfs_stream, buf, 16, TIMEOUT);
   DIFF_AND_RESET (streams_egress, ==, 16);
   DIFF_AND_RESET (streams_ingress, ==, 0);
   mongoc_stream_destroy (gridfs_stream);
   DIFF_AND_RESET (streams_active, ==, -1);
   DIFF_AND_RESET (streams_disposed, ==, 1);
   DIFF_AND_RESET (streams_egress, >, 0);
   mongoc_gridfs_file_save (file);
   mongoc_gridfs_file_destroy (file);
   file = mongoc_gridfs_find_one_by_filename (gridfs, "example", &err);
   ASSERT_OR_PRINT (file, err);
   gridfs_stream = mongoc_stream_gridfs_new (file);
   DIFF_AND_RESET (streams_active, ==, 1);
   DIFF_AND_RESET (streams_disposed, ==, 0);
   RESET (streams_egress);
   mongoc_stream_read (gridfs_stream, buf, 16, 0, TIMEOUT);
   DIFF_AND_RESET (streams_egress, >, 0);
   DIFF_AND_RESET (streams_ingress, >, 16);
   mongoc_stream_destroy (gridfs_stream);
   DIFF_AND_RESET (streams_active, ==, -1);
   DIFF_AND_RESET (streams_disposed, ==, 1);
   mongoc_gridfs_file_destroy (file);
   mongoc_gridfs_destroy (gridfs);
   mongoc_client_destroy (client);
}


static void
test_counters_auth (void *ctx)
{
   char *host_and_port = test_framework_get_host_and_port ();
   char *uri_str = test_framework_get_uri_str ();
   char *uri_str_bad = bson_strdup_printf ("mongodb://%s:%s@%s/", "bad_user", "bad_pass", host_and_port);
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   bool ret;
   bson_error_t err;

   BSON_UNUSED (ctx);

   uri = mongoc_uri_new (uri_str);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 99999);
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 99999);
   reset_all_counters ();
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   BSON_ASSERT (client);
   ret = mongoc_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &err);
   ASSERT_OR_PRINT (ret, err);
   DIFF_AND_RESET (auth_success, ==, 1);
   DIFF_AND_RESET (auth_failure, ==, 0);
   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   bson_free (uri_str_bad);
   bson_free (host_and_port);
   mongoc_client_destroy (client);
}


static void
test_counters_dns (void)
{
   mongoc_client_t *client;
   mongoc_server_description_t *sd;
   bson_error_t err;
   reset_all_counters ();
   client = test_framework_new_default_client ();
   sd = mongoc_client_select_server (client, false, NULL, &err);
   ASSERT_OR_PRINT (sd, err);
   DIFF_AND_RESET (dns_success, >, 0);
   DIFF_AND_RESET (dns_failure, ==, 0);
   mongoc_server_description_destroy (sd);
   mongoc_client_destroy (client);
   client = test_framework_client_new ("mongodb://invalidhostname/", NULL);
   test_framework_set_ssl_opts (client);
   sd = mongoc_client_select_server (client, false, NULL, &err);
   ASSERT (!sd);
   DIFF_AND_RESET (dns_success, ==, 0);
   DIFF_AND_RESET (dns_failure, ==, 1);
   mongoc_client_destroy (client);
}


static void
test_counters_streams_timeout (void)
{
   mock_server_t *server;
   bson_error_t err = {0};
   bool ret;
   future_t *future;
   mongoc_client_t *client;
   request_t *request;
   mongoc_uri_t *uri;
   mongoc_server_description_t *sd;

   server = mock_server_with_auto_hello (WIRE_VERSION_MAX);
   mock_server_run (server);
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 300);
   client = test_framework_client_new_from_uri (uri, NULL);
   mongoc_uri_destroy (uri);
   sd = mongoc_client_select_server (client, true, NULL, &err);
   mongoc_server_description_destroy (sd);
   reset_all_counters ();
   future = future_client_command_simple (client, "test", tmp_bson ("{'ping': 1}"), NULL, NULL, &err);
   request = mock_server_receives_msg (server, MONGOC_QUERY_NONE, tmp_bson ("{'ping': 1}"));
   _mongoc_usleep (350);
   request_destroy (request);
   ret = future_get_bool (future);
   BSON_ASSERT (!ret);
   future_destroy (future);
   /* can't ASSERT == because the mock server times out normally reading. */
   DIFF_AND_RESET (streams_timeout, >=, 1);
   mongoc_client_destroy (client);
   mock_server_destroy (server);
}

typedef struct _rpc_op_egress_counters {
   int32_t op_egress_compressed;
   int32_t op_egress_delete;
   int32_t op_egress_getmore;
   int32_t op_egress_insert;
   int32_t op_egress_killcursors;
   int32_t op_egress_msg;
   int32_t op_egress_query;
   int32_t op_egress_total;
   int32_t op_egress_update;
} rpc_op_egress_counters;

static rpc_op_egress_counters
rpc_op_egress_counters_current (void)
{
   return (rpc_op_egress_counters){
      .op_egress_compressed = mongoc_counter_op_egress_compressed_count (),
      .op_egress_delete = mongoc_counter_op_egress_delete_count (),
      .op_egress_getmore = mongoc_counter_op_egress_getmore_count (),
      .op_egress_insert = mongoc_counter_op_egress_insert_count (),
      .op_egress_killcursors = mongoc_counter_op_egress_killcursors_count (),
      .op_egress_msg = mongoc_counter_op_egress_msg_count (),
      .op_egress_query = mongoc_counter_op_egress_query_count (),
      .op_egress_total = mongoc_counter_op_egress_total_count (),
      .op_egress_update = mongoc_counter_op_egress_update_count (),
   };
}

static void
rpc_op_egress_counters_reset (void)
{
   mongoc_counter_op_egress_compressed_reset ();
   mongoc_counter_op_egress_delete_reset ();
   mongoc_counter_op_egress_getmore_reset ();
   mongoc_counter_op_egress_insert_reset ();
   mongoc_counter_op_egress_killcursors_reset ();
   mongoc_counter_op_egress_msg_reset ();
   mongoc_counter_op_egress_query_reset ();
   mongoc_counter_op_egress_total_reset ();
   mongoc_counter_op_egress_update_reset ();
}

#define ASSERT_RPC_OP_EGRESS_COUNTERS(expected, actual)                             \
   if (1) {                                                                         \
      const rpc_op_egress_counters e = (expected);                                  \
      const rpc_op_egress_counters a = (actual);                                    \
      ASSERT_WITH_MSG (e.op_egress_compressed == a.op_egress_compressed,            \
                       "op_egress_compressed: expected %" PRId32 ", got %" PRId32,  \
                       e.op_egress_compressed,                                      \
                       a.op_egress_compressed);                                     \
      ASSERT_WITH_MSG (e.op_egress_delete == a.op_egress_delete,                    \
                       "op_egress_delete: expected %" PRId32 ", got %" PRId32,      \
                       e.op_egress_delete,                                          \
                       a.op_egress_delete);                                         \
      ASSERT_WITH_MSG (e.op_egress_getmore == a.op_egress_getmore,                  \
                       "op_egress_getmore: expected %" PRId32 ", got %" PRId32,     \
                       e.op_egress_getmore,                                         \
                       a.op_egress_getmore);                                        \
      ASSERT_WITH_MSG (e.op_egress_insert == a.op_egress_insert,                    \
                       "op_egress_insert: expected %" PRId32 ", got %" PRId32,      \
                       e.op_egress_insert,                                          \
                       a.op_egress_insert);                                         \
      ASSERT_WITH_MSG (e.op_egress_killcursors == a.op_egress_killcursors,          \
                       "op_egress_killcursors: expected %" PRId32 ", got %" PRId32, \
                       e.op_egress_killcursors,                                     \
                       a.op_egress_killcursors);                                    \
      ASSERT_WITH_MSG (e.op_egress_msg == a.op_egress_msg,                          \
                       "op_egress_msg: expected %" PRId32 ", got %" PRId32,         \
                       e.op_egress_msg,                                             \
                       a.op_egress_msg);                                            \
      ASSERT_WITH_MSG (e.op_egress_query == a.op_egress_query,                      \
                       "op_egress_query: expected %" PRId32 ", got %" PRId32,       \
                       e.op_egress_query,                                           \
                       a.op_egress_query);                                          \
      ASSERT_WITH_MSG (e.op_egress_total == a.op_egress_total,                      \
                       "op_egress_total: expected %" PRId32 ", got %" PRId32,       \
                       e.op_egress_total,                                           \
                       a.op_egress_total);                                          \
      ASSERT_WITH_MSG (e.op_egress_update == a.op_egress_update,                    \
                       "op_egress_update: expected %" PRId32 ", got %" PRId32,      \
                       e.op_egress_update,                                          \
                       a.op_egress_update);                                         \
   } else                                                                           \
      (void) 0

#define ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT(expected) \
   ASSERT_RPC_OP_EGRESS_COUNTERS (expected, rpc_op_egress_counters_current ())


typedef struct _server_monitor_autoresponder_data {
   const char *hello;
   int *responses;
} server_monitor_autoresponder_data;

static bool
test_counters_rpc_op_egress_autoresponder (request_t *request, void *data)
{
   BSON_ASSERT_PARAM (data);

   server_monitor_autoresponder_data *const ar_data = data;

   ASSERT (ar_data->responses);
   ASSERT (ar_data->hello);

   (*ar_data->responses) += 1;

   if (strcmp (request->command_name, HANDSHAKE_CMD_HELLO) == 0 ||
       strcmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO) == 0) {
      reply_to_request_simple (request, ar_data->hello);
      request_destroy (request);
   } else {
      ASSERT_WITH_MSG (request->is_command, "expected only handshakes and commands, but got: %s", request->as_str);
      reply_to_request_with_ok_and_destroy (request);
   }

   return true;
}


static void
_test_counters_rpc_op_egress_cluster_single (bool with_op_msg)
{
   const rpc_op_egress_counters zero = {0};

   const char *const hello = tmp_str ("{'ok': 1,%s"
                                      " 'isWritablePrimary': true,"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d}",
                                      with_op_msg ? " 'helloOk': true," : "",
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX);

   rpc_op_egress_counters_reset ();

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);

   bson_error_t error = {0};
   mongoc_client_t *const client = mongoc_client_new_from_uri_with_error (mock_server_get_uri (server), &error);
   ASSERT_OR_PRINT (client, error);

   // Stable API for Drivers spec: If an API version was declared, drivers MUST
   // NOT use the legacy hello command during the initial handshake or
   // afterwards. Instead, drivers MUST use the `hello` command exclusively and
   // use the `OP_MSG` protocol.
   if (with_op_msg) {
      mongoc_server_api_t *const api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
      ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
      mongoc_server_api_destroy (api);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   rpc_op_egress_counters expected = {0};
   int32_t *const handshake_counter = with_op_msg ? &expected.op_egress_msg : &expected.op_egress_query;

   {
      const bson_t *const command = tmp_bson ("{'ping': 1}");

      // Trigger:
      //  - mongoc_topology_scanner_node_setup
      //  - mongoc_cluster_run_command_monitored
      future_t *const ping = future_client_command_simple (client, "db", command, NULL, NULL, &error);

      {
         request_t *const request =
            with_op_msg ? mock_server_receives_hello_op_msg (server) : mock_server_receives_legacy_hello (server, NULL);

         // OP_QUERY 1 / OP_MSG 1:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by _mongoc_async_cmd_phase_send
         //  - by mongoc_async_cmd_run
         //  - by mongoc_async_run
         //  - by mongoc_topology_scanner_work
         //  - by mongoc_topology_scan_once
         //  - by _mongoc_topology_do_blocking_scan
         //  - by mongoc_topology_select_server_id
         //  - by _mongoc_cluster_select_server_id
         //  - by _mongoc_cluster_stream_for_optype
         //  - by mongoc_cluster_stream_for_reads
         //  - by mongoc_client_command_simple
         *handshake_counter += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_simple (request, hello);
         request_destroy (request);
      }

      {
         request_t *const request = mock_server_receives_msg (server, MONGOC_MSG_NONE, command);

         // OP_MSG 1 / OP_MSG 2:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_monitored
         //  - by _mongoc_client_command_with_stream
         //  - by mongoc_client_command_simple
         expected.op_egress_msg += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_with_ok_and_destroy (request);
      }

      ASSERT_OR_PRINT (future_get_bool (ping), error);
      future_destroy (ping);
   }

   // Ensure no extra requests.
   {
      int responses = 0;

      server_monitor_autoresponder_data data = {.hello = hello, .responses = &responses};

      mock_server_autoresponds (server, test_counters_rpc_op_egress_autoresponder, &data, NULL);

      mongoc_client_destroy (client);
      mock_server_destroy (server);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}

static void
test_counters_rpc_op_egress_cluster_single_op_query (void)
{
   _test_counters_rpc_op_egress_cluster_single (false);
}

static void
test_counters_rpc_op_egress_cluster_single_op_msg (void)
{
   _test_counters_rpc_op_egress_cluster_single (true);
}


static void
test_counters_rpc_op_egress_cluster_legacy (void)
{
   const rpc_op_egress_counters zero = {0};

   const char *const hello = tmp_str ("{'ok': 1,"
                                      " 'isWritablePrimary': true,"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d}",
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX);

   rpc_op_egress_counters_reset ();

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);

   bson_error_t error = {0};
   mongoc_client_t *const client = mongoc_client_new_from_uri_with_error (mock_server_get_uri (server), &error);
   ASSERT_OR_PRINT (client, error);

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   rpc_op_egress_counters expected = {0};

   // Client must know a writeable server to trigger OP_KILL_CURSORS.
   {
      const bson_t *const command = tmp_bson ("{'ping': 1}");

      future_t *const ping = future_client_command_simple (client, "db", command, NULL, NULL, &error);

      {
         request_t *const request = mock_server_receives_legacy_hello (server, NULL);

         // OP_QUERY 1:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by _mongoc_async_cmd_phase_send
         //  - by mongoc_async_cmd_run
         //  - by mongoc_async_run
         //  - by mongoc_topology_scanner_work
         //  - by mongoc_topology_scan_once
         //  - by _mongoc_topology_do_blocking_scan
         //  - by mongoc_topology_select_server_id
         //  - by _mongoc_cluster_select_server_id
         //  - by _mongoc_cluster_stream_for_optype
         //  - by mongoc_cluster_stream_for_reads
         //  - by mongoc_client_command_simple
         expected.op_egress_query += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_simple (request, hello);

         request_destroy (request);
      }

      {
         request_t *const request = mock_server_receives_msg (server, MONGOC_MSG_NONE, command);

         // OP_MSG 1:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_monitored
         //  - by _mongoc_client_command_with_stream
         //  - by mongoc_client_command_simple
         expected.op_egress_msg += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_with_ok_and_destroy (request);
      }

      ASSERT_OR_PRINT (future_get_bool (ping), error);
      future_destroy (ping);
   }

   // Trigger: mongoc_cluster_legacy_rpc_sendv_to_server
   mongoc_client_kill_cursor (client, 123);

   {
      request_t *const request = mock_server_receives_kill_cursors (server, 123);

      ASSERT_WITH_MSG (request->opcode == MONGOC_OPCODE_KILL_CURSORS,
                       "expected OP_KILL_CURSORS request, but received: %s",
                       request->as_str);

      // OP_KILL_CURSORS 1:
      //  - by _mongoc_rpc_op_egress_inc
      //  - by mongoc_cluster_legacy_rpc_sendv_to_server
      //  - by _mongoc_client_op_killcursors
      //  - by _mongoc_client_kill_cursor
      //  - by mongoc_client_kill_cursor
      expected.op_egress_killcursors += 1;
      expected.op_egress_total += 1;
      ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

      // OP_KILL_CURSORS does not require a response.
      request_destroy (request);
   }

   // Ensure no extra requests.
   {
      int responses = 0;

      server_monitor_autoresponder_data data = {.hello = hello, .responses = &responses};

      mock_server_autoresponds (server, test_counters_rpc_op_egress_autoresponder, &data, NULL);

      mongoc_client_destroy (client);
      mock_server_destroy (server);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}


static void
_test_counters_rpc_op_egress_cluster_pooled (bool with_op_msg)
{
   const rpc_op_egress_counters zero = {0};

   const char *const hello = tmp_str ("{'ok': 1,%s"
                                      " 'isWritablePrimary': true,"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d}",
                                      with_op_msg ? " 'helloOk': true," : "",
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX);

   rpc_op_egress_counters_reset ();

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);

   bson_error_t error = {0};
   mongoc_client_pool_t *const pool = mongoc_client_pool_new_with_error (mock_server_get_uri (server), &error);
   ASSERT_OR_PRINT (pool, error);

   // Stable API for Drivers spec: If an API version was declared, drivers MUST
   // NOT use the legacy hello command during the initial handshake or
   // afterwards. Instead, drivers MUST use the `hello` command exclusively and
   // use the `OP_MSG` protocol.
   if (with_op_msg) {
      mongoc_server_api_t *const api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
      ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, api, &error), error);
      mongoc_server_api_destroy (api);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   // Trigger: _server_monitor_check_server
   mongoc_client_t *const client = mongoc_client_pool_pop (pool);

   rpc_op_egress_counters expected = {0};
   int32_t *const handshake_counter = with_op_msg ? &expected.op_egress_msg : &expected.op_egress_query;

   {
      request_t *const request =
         with_op_msg ? mock_server_receives_hello_op_msg (server) : mock_server_receives_legacy_hello (server, NULL);

      // OP_QUERY 1 / OP_MSG 1:
      //  - by _mongoc_rpc_op_egress_inc
      //  - by one of:
      //    - _server_monitor_send_and_recv_opquery
      //    - _server_monitor_send_and_recv_hello_opmsg
      //  - by _server_monitor_send_and_recv
      //  - by _server_monitor_setup_connection
      //  - by _server_monitor_check_server
      //  - by _server_monitor_thread
      *handshake_counter += 1;
      expected.op_egress_total += 1;
      ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

      reply_to_request_simple (request, hello);
      request_destroy (request);
   }

   {
      const bson_t *const command = tmp_bson ("{'ping': 1}");

      future_t *const ping = future_client_command_simple (client, "db", command, NULL, NULL, &error);

      {
         request_t *const request =
            with_op_msg ? mock_server_receives_hello_op_msg (server) : mock_server_receives_legacy_hello (server, NULL);

         // OP_QUERY 2 / OP_MSG 2:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by one of:
         //    - mongoc_cluster_run_command_opquery
         //    - mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_private
         //  - by _stream_run_hello
         //  - by _cluster_run_hello
         //  - by _cluster_add_node
         //  - by _cluster_fetch_stream_pooled
         //  - by _try_get_server_stream
         //  - by _mongoc_cluster_stream_for_server
         //  - by _mongoc_cluster_stream_for_optype
         //  - by mongoc_cluster_stream_for_reads
         //  - by mongoc_client_command_simple
         *handshake_counter += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_simple (request, hello);
         request_destroy (request);
      }

      {
         request_t *const request = mock_server_receives_msg (server, MONGOC_MSG_NONE, command);

         // OP_MSG 1 / OP_MSG 3:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_monitored
         //  - by _mongoc_client_command_with_stream
         //  - by mongoc_client_command_simple
         expected.op_egress_msg += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_with_ok_and_destroy (request);
      }

      ASSERT_OR_PRINT (future_get_bool (ping), error);
      future_destroy (ping);
   }

   // Ensure no extra requests.
   {
      int responses = 0;

      server_monitor_autoresponder_data data = {.hello = hello, .responses = &responses};

      mock_server_autoresponds (server, test_counters_rpc_op_egress_autoresponder, &data, NULL);

      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
      mock_server_destroy (server);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}

static void
test_counters_rpc_op_egress_cluster_pooled_op_query (void)
{
   _test_counters_rpc_op_egress_cluster_pooled (false);
}

static void
test_counters_rpc_op_egress_cluster_pooled_op_msg (void)
{
   _test_counters_rpc_op_egress_cluster_pooled (true);
}


static void
_test_counters_rpc_op_egress_awaitable_hello (bool with_op_msg)
{
   const rpc_op_egress_counters zero = {0};

   const char *const hello = tmp_str ("{'ok': 1,%s"
                                      " 'isWritablePrimary': true,"
                                      " 'topologyVersion': {"
                                      "  'processId': {'$oid': '000000000000000000000001'},"
                                      "  'counter': {'$numberLong': '1'}"
                                      " },"
                                      " 'minWireVersion': %d,"
                                      " 'maxWireVersion': %d}",
                                      with_op_msg ? " 'helloOk': true," : "",
                                      WIRE_VERSION_MIN,
                                      WIRE_VERSION_MAX);

   rpc_op_egress_counters_reset ();

   mock_server_t *const server = mock_server_new ();
   mock_server_run (server);

   bson_error_t error = {0};
   mongoc_uri_t *const uri = mongoc_uri_copy (mock_server_get_uri (server));

   mongoc_client_pool_t *const pool = mongoc_client_pool_new_with_error (uri, &error);
   ASSERT_OR_PRINT (pool, error);

   // Stable API for Drivers spec: If an API version was declared, drivers MUST
   // NOT use the legacy hello command during the initial handshake or
   // afterwards. Instead, drivers MUST use the `hello` command exclusively and
   // use the `OP_MSG` protocol.
   if (with_op_msg) {
      mongoc_server_api_t *const api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
      ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, api, &error), error);
      mongoc_server_api_destroy (api);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   // Trigger:
   //  - _server_monitor_setup_connection
   //  - _server_monitor_polling_hello
   //  - _server_monitor_awaitable_hello
   mongoc_client_t *const client = mongoc_client_pool_pop (pool);

   // Hold on to awaitable hello request.
   request_t *awaitable_hello = NULL;

   // The order of requests from the monitor and RTT threads may vary, so handle
   // all four requests before asserting counters.
   for (int i = 0; i < 4; ++i) {
      request_t *const request = mock_server_receives_request (server);

      if (strstr (request->as_str, "maxAwaitTimeMS")) {
         ASSERT_WITH_MSG (!awaitable_hello, "received more than one awaitable hello");
         awaitable_hello = request;
      } else {
         const bool is_hello = strcmp (request->command_name, HANDSHAKE_CMD_HELLO) == 0;
         const bool is_legacy_hello = strcmp (request->command_name, HANDSHAKE_CMD_LEGACY_HELLO) == 0;

         if (with_op_msg) {
            ASSERT_WITH_MSG (is_hello, "expected only OP_MSG hello requests, but got: %s", request->as_str);
         } else {
            ASSERT_WITH_MSG (is_hello || is_legacy_hello, "expected only hello requests, but got: %s", request->as_str);
         }

         reply_to_request_simple (request, hello);
         request_destroy (request);
      }
   }

   rpc_op_egress_counters expected = {0};
   int32_t *const handshake_counter = with_op_msg ? &expected.op_egress_msg : &expected.op_egress_query;

   // OP_QUERY 1 / OP_MSG 1:
   //  - by _mongoc_rpc_op_egress_inc
   //  - by one of:
   //    - _server_monitor_send_and_recv_opquery
   //    - _server_monitor_send_and_recv_hello_opmsg
   //  - by _server_monitor_send_and_recv
   //  - by _server_monitor_setup_connection
   //  - by _server_monitor_check_server
   //  - by _server_monitor_thread
   // OP_QUERY 2 / OP_MSG 2:
   //  - by _mongoc_rpc_op_egress_inc
   //  - by one of:
   //    - _server_monitor_send_and_recv_opquery
   //    - _server_monitor_send_and_recv_hello_opmsg
   //  - by _server_monitor_send_and_recv
   //  - by _server_monitor_setup_connection
   //  - by _server_monitor_ping_server
   //  - by _server_monitor_rtt_thread
   // OP_QUERY 3 / OP_MSG 3:
   //  - by _mongoc_rpc_op_egress_inc
   //  - by one of:
   //    - _server_monitor_send_and_recv_opquery
   //    - _server_monitor_send_and_recv_hello_opmsg
   //  - by _server_monitor_send_and_recv
   //  - by _server_monitor_polling_hello
   //  - by _server_monitor_ping_server
   //  - by _server_monitor_rtt_thread
   *handshake_counter += 3;
   expected.op_egress_total += 3;

   // OP_MSG 1 / OP_MSG 3:
   //  - by _mongoc_rpc_op_egress_inc
   //  - by _server_monitor_awaitable_hello_send
   //  - by _server_monitor_awaitable_hello
   //  - by _server_monitor_check_server
   //  - by _server_monitor_thread
   expected.op_egress_msg += 1;
   expected.op_egress_total += 1;

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

   {
      const bson_t *const command = tmp_bson ("{'ping': 1}");

      // Trigger:
      //  - mongoc_cluster_run_command_private
      //  - mongoc_cluster_run_command_monitored
      future_t *const ping = future_client_command_simple (client, "db", command, NULL, NULL, &error);

      {
         request_t *const request =
            with_op_msg ? mock_server_receives_hello_op_msg (server) : mock_server_receives_legacy_hello (server, NULL);

         // OP_QUERY 4 / OP_MSG 5:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by one of:
         //    - mongoc_cluster_run_command_opquery
         //    - mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_private
         //  - by _stream_run_hello
         //  - by _cluster_run_hello
         //  - by _cluster_add_node
         //  - by _cluster_fetch_stream_pooled
         //  - by _try_get_server_stream
         //  - by _mongoc_cluster_stream_for_server
         //  - by _mongoc_cluster_stream_for_optype
         //  - by mongoc_cluster_stream_for_reads
         //  - by mongoc_client_command_simple
         *handshake_counter += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_simple (request, hello);
         request_destroy (request);
      }

      {
         request_t *const request = mock_server_receives_msg (server, MONGOC_MSG_NONE, command);

         // OP_MSG 2 / OP_MSG 6:
         //  - by _mongoc_rpc_op_egress_inc
         //  - by mongoc_cluster_run_opmsg
         //  - by mongoc_cluster_run_command_monitored
         //  - by _mongoc_client_command_with_stream
         //  - by mongoc_client_command_simple
         expected.op_egress_msg += 1;
         expected.op_egress_total += 1;
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

         reply_to_request_with_ok_and_destroy (request);
      }

      ASSERT_OR_PRINT (future_get_bool (ping), error);
      future_destroy (ping);
   }

   // Ensure successful client creation and no extra requests.
   {
      int responses = 0;

      server_monitor_autoresponder_data data = {.hello = hello, .responses = &responses};

      mock_server_autoresponds (server, test_counters_rpc_op_egress_autoresponder, &data, NULL);

      mongoc_uri_destroy (uri);
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);

      // Reply to awaitable hello after destroying the client pool to avoid
      // triggering additional awaitable hello requests.
      reply_to_request_simple (awaitable_hello, hello);
      request_destroy (awaitable_hello);

      mock_server_destroy (server);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}

static void
test_counters_rpc_op_egress_awaitable_hello_op_query (void)
{
   _test_counters_rpc_op_egress_awaitable_hello (false);
}

static void
test_counters_rpc_op_egress_awaitable_hello_op_msg (void)
{
   _test_counters_rpc_op_egress_awaitable_hello (true);
}


static void
_test_counters_rpc_op_egress_mock_server (bool with_op_msg)
{
   const rpc_op_egress_counters zero = {0};

   rpc_op_egress_counters_reset ();

   mock_server_t *server = mock_server_with_auto_hello (WIRE_VERSION_MIN);
   mock_server_run (server);

   bson_error_t error = {0};
   mongoc_client_t *const client = mongoc_client_new_from_uri_with_error (mock_server_get_uri (server), &error);
   ASSERT_OR_PRINT (client, error);

   // Stable API for Drivers spec: If an API version was declared, drivers MUST
   // NOT use the legacy hello command during the initial handshake or
   // afterwards. Instead, drivers MUST use the `hello` command exclusively and
   // use the `OP_MSG` protocol.
   if (with_op_msg) {
      mongoc_server_api_t *const api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
      ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
      mongoc_server_api_destroy (api);
   }

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   {
      const bson_t *const command = tmp_bson ("{'ping': 1}");
      future_t *const future = future_client_command_simple (client, "db", command, NULL, NULL, &error);
      reply_to_request_with_ok_and_destroy (mock_server_receives_msg (server, MONGOC_MSG_NONE, command));
      ASSERT_OR_PRINT (future_get_bool (future), error);
      future_destroy (future);
   }

   // Handshake (OP_QUERY 1 / OP_MSG 1) + command (OP_MSG 1 / OP_MSG 2).
   // Mock server replies should not contribute RPC egress counters.
   const rpc_op_egress_counters expected = {
      .op_egress_msg = with_op_msg ? 2 : 1,
      .op_egress_query = with_op_msg ? 0 : 1,
      .op_egress_total = 2,
   };

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

   mongoc_client_destroy (client);
   mock_server_destroy (server);

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}

static void
test_counters_rpc_op_egress_mock_server_op_query (void)
{
   _test_counters_rpc_op_egress_mock_server (false);
}

static void
test_counters_rpc_op_egress_mock_server_op_msg (void)
{
   _test_counters_rpc_op_egress_mock_server (true);
}


#if defined(MONGOC_ENABLE_SSL)
static void
wait_for_background_threads (rpc_op_egress_counters expected)
{
   rpc_op_egress_counters current = rpc_op_egress_counters_current ();

   // Wait up to 10 seconds (default heartbeat frequency) for the server monitor
   // and/or RTT threads to complete their initial handshake and hello requests.
   for (int i = 0; i < 100; ++i) {
      if (current.op_egress_total >= expected.op_egress_total) {
         return;
      }

      _mongoc_usleep (100000); // 100 ms.

      current = rpc_op_egress_counters_current ();
   }

   ASSERT_WITH_MSG (false,
                    "expected %" PRId32 " total requests by background threads, but observed %" PRId32
                    " total requests",
                    expected.op_egress_total,
                    current.op_egress_total);
}


static void
_test_counters_auth (bool with_op_msg, bool pooled)
{
   const rpc_op_egress_counters zero = {0};

   // Number of messages sent by background threads depend on the number of
   // members in the replica set.
   const size_t member_count_zu = test_framework_replset_member_count ();
   ASSERT (bson_in_range_unsigned (int32_t, member_count_zu));
   const int32_t member_count = (int32_t) member_count_zu;

   // MongoDB Handshake Spec: Since MongoDB server 4.4, the initial handshake
   // supports a new argument, `speculativeAuthenticate`, provided as a BSON
   // document. Clients specifying this argument to hello or legacy hello will
   // speculatively include the first command of an authentication handshake.
   const bool has_speculative_auth = test_framework_get_server_version () >= test_framework_str_to_version ("4.4.0");

   // Used to calculate expected values of OP_COMPRESSED RPC egress counter.
   const int32_t has_compressors = test_framework_has_compressors ();

   // Stable API for Drivers spec: If an API version was declared, drivers
   // MUST NOT use the legacy hello command during the initial handshake or
   // afterwards. Instead, drivers MUST use the `hello` command exclusively
   // and use the `OP_MSG` protocol.
   mongoc_server_api_t *const api = with_op_msg ? mongoc_server_api_new (MONGOC_SERVER_API_V1) : NULL;

   // SCRAM-SHA-1 is available since MongoDB server 3.0 and forces OP_MSG
   // requests for authentication steps that follow the initial connection
   // handshake even with speculative authentication.
   const char *const auth_mechanism = "SCRAM-SHA-1";

   char *const test_user = test_framework_get_admin_user ();
   char *const test_password = test_framework_get_admin_password ();

   bson_error_t error = {0};

   // Obtain URI before resetting RPC egress counters.
   char *const uri_str = test_framework_get_uri_str ();

   // Setup complete: reset counters now.
   rpc_op_egress_counters_reset ();

   mongoc_uri_t *const uri = mongoc_uri_new_with_error (uri_str, &error);
   ASSERT_OR_PRINT (uri, error);
   mongoc_uri_set_username (uri, test_user);
   mongoc_uri_set_password (uri, test_password);

   // Specify the authentication mechanism to ensure deterministic request
   // behavior during testing.
   ASSERT (mongoc_uri_set_auth_mechanism (uri, auth_mechanism));

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *client = NULL;

   if (pooled) {
      // Note: no server API version ensures OP_QUERY for initial handshake.
      pool = test_framework_client_pool_new_from_uri (uri, api);
      test_framework_set_pool_ssl_opts (pool);

      ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);

      // Trigger server monitor and/or RTT thread startup.
      client = mongoc_client_pool_pop (pool);

      // Awaitable hello is also a 4.4+ feature.
      if (has_speculative_auth) {
         rpc_op_egress_counters expected = {0};
         int32_t *const handshake_counter = with_op_msg ? &expected.op_egress_msg : &expected.op_egress_query;

         // OP_QUERY / OP_MSG (for each replset member):
         //  - initial connection handshake by server monitor thread
         //  - initial connection handshake by RTT monitor thread
         //  - polling hello by RTT monitor thread
         *handshake_counter += 3 * member_count;
         expected.op_egress_total += 3 * member_count;

         // OP_MSG (for each replset member):
         //  - awaitable hello by server monitor thread
         expected.op_egress_msg += member_count;
         expected.op_egress_total += member_count;

         wait_for_background_threads (expected);
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
      } else {
         // OP_QUERY / OP_MSG (for each replset member):
         //  - initial connection handshake by server monitor
         const rpc_op_egress_counters expected = {
            .op_egress_msg = with_op_msg ? member_count : 0,
            .op_egress_query = with_op_msg ? 0 : member_count,
            .op_egress_total = member_count,
         };

         wait_for_background_threads (expected);
         ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
      }
   } else {
      client = test_framework_client_new_from_uri (uri, api);
      test_framework_set_ssl_opts (client);
      ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (zero);
   }

   mongoc_counter_auth_success_reset ();
   mongoc_counter_auth_failure_reset ();
   rpc_op_egress_counters_reset ();

   // Trigger:
   //  - one of (depending on `pooled`):
   //    - _cluster_fetch_stream_single
   //    - _cluster_fetch_stream_pooled
   //  - _cluster_run_hello (SASL step 1 if `has_speculative_auth`)
   //  - _mongoc_cluster_auth_node (if not `has_speculative_auth`)
   //  - mongoc_cluster_run_command_monitored
   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "db", tmp_bson ("{'ping': 1}"), NULL, NULL, &error), error);

   const int32_t auth_success = mongoc_counter_auth_success_count ();
   const int32_t auth_failure = mongoc_counter_auth_failure_count ();

   // Ensure we are not testing more than we intend.
   ASSERT_WITH_MSG (auth_success == 1 && auth_failure == 0,
                    "expected exactly one authentication attempt to succeed, "
                    "but observed %" PRId32 " successes and %" PRId32 " failures",
                    auth_success,
                    auth_failure);

   rpc_op_egress_counters expected = {0};
   int32_t *const handshake_counter = with_op_msg ? &expected.op_egress_msg : &expected.op_egress_query;

   // The number of expected OP_QUERY requests depends on pooling and the
   // presence of the RTT monitor thread.
   if (pooled) {
      // OP_QUERY / OP_MSG:
      //  - initial connection handshake by new cluster node
      *handshake_counter += 1;
      expected.op_egress_total += 1;
   } else {
      // OP_QUERY / OP_MSG (for each replset member):
      //  - initial connection handshake
      *handshake_counter += member_count;
      expected.op_egress_total += member_count;
   }

   // The number of authentication steps depends on speculative authentication.
   if (has_speculative_auth) {
      // OP_MSG: _mongoc_cluster_finish_speculative_auth (SASL step 2)
      expected.op_egress_msg += 1;
      expected.op_egress_total += 1;
   } else {
      // OP_MSG: _mongoc_cluster_auth_node (SASL step 1)
      // OP_MSG: _mongoc_cluster_auth_node (SASL step 2)
      // OP_MSG: _mongoc_cluster_auth_node (SASL step 3)
      expected.op_egress_msg += 3;
      expected.op_egress_total += 3;
   }

   // OP_MSG (+ OP_COMPRESSED): mongoc_cluster_run_command_monitored (ping)
   expected.op_egress_compressed += has_compressors;
   expected.op_egress_msg += 1;
   expected.op_egress_total += has_compressors + 1;

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);

   mongoc_server_api_destroy (api);
   bson_free (uri_str);
   mongoc_uri_destroy (uri);
   bson_free (test_password);
   bson_free (test_user);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }

   // OP_MSG (+ OP_COMPRESSED): _mongoc_client_end_sessions (endSessions)
   expected.op_egress_compressed += has_compressors;
   expected.op_egress_msg += 1;
   expected.op_egress_total += has_compressors + 1;

   ASSERT_RPC_OP_EGRESS_COUNTERS_CURRENT (expected);
}

static void
test_counters_auth_single_op_query (void *context)
{
   BSON_UNUSED (context);
   _test_counters_auth (false, false);
}

static void
test_counters_auth_single_op_msg (void *context)
{
   BSON_UNUSED (context);
   _test_counters_auth (true, false);
}

static void
test_counters_auth_pooled_op_query (void *context)
{
   BSON_UNUSED (context);
   _test_counters_auth (false, true);
}

static void
test_counters_auth_pooled_op_msg (void *context)
{
   BSON_UNUSED (context);
   _test_counters_auth (true, true);
}
#endif

#endif

void
test_counters_install (TestSuite *suite)
{
#ifdef MONGOC_ENABLE_SHM_COUNTERS
   TestSuite_AddFull (suite,
                      "/counters/op_msg",
                      test_counters_op_msg,
                      NULL,
                      NULL,
                      test_framework_skip_if_compressors,
                      TestSuite_CheckLive);
   TestSuite_AddFull (suite,
                      "/counters/op_compressed",
                      test_counters_op_compressed,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_compressors,
                      TestSuite_CheckLive);
   TestSuite_AddLive (suite, "/counters/cursors", test_counters_cursors);
   TestSuite_AddLive (suite, "/counters/clients", test_counters_clients);
   TestSuite_AddFull (suite, "/counters/streams", test_counters_streams, NULL, NULL, TestSuite_CheckLive);
   TestSuite_AddFull (suite,
                      "/counters/auth",
                      test_counters_auth,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_not_single);
   TestSuite_AddLive (suite, "/counters/dns", test_counters_dns);
   TestSuite_AddMockServerTest (suite, "/counters/streams_timeout", test_counters_streams_timeout);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/cluster/single/op_query", test_counters_rpc_op_egress_cluster_single_op_query);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/cluster/single/op_msg", test_counters_rpc_op_egress_cluster_single_op_msg);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/cluster/legacy", test_counters_rpc_op_egress_cluster_legacy);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/cluster/pooled/op_query", test_counters_rpc_op_egress_cluster_pooled_op_query);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/cluster/pooled/op_msg", test_counters_rpc_op_egress_cluster_pooled_op_msg);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/awaitable_hello/op_query", test_counters_rpc_op_egress_awaitable_hello_op_query);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/awaitable_hello/op_msg", test_counters_rpc_op_egress_awaitable_hello_op_msg);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/mock_server/op_query", test_counters_rpc_op_egress_mock_server_op_query);

   TestSuite_AddMockServerTest (
      suite, "/counters/rpc/op_egress/mock_server/op_msg", test_counters_rpc_op_egress_mock_server_op_msg);

#if defined(MONGOC_ENABLE_SSL)
   TestSuite_AddFull (suite,
                      "/counters/rpc/op_egress/auth/single/op_query",
                      test_counters_auth_single_op_query,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/counters/rpc/op_egress/auth/single/op_msg",
                      test_counters_auth_single_op_msg,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_max_wire_version_less_than_13,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/counters/rpc/op_egress/auth/pooled/op_query",
                      test_counters_auth_pooled_op_query,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_not_replset);
   TestSuite_AddFull (suite,
                      "/counters/rpc/op_egress/auth/pooled/op_msg",
                      test_counters_auth_pooled_op_msg,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_auth,
                      test_framework_skip_if_max_wire_version_less_than_13,
                      test_framework_skip_if_not_replset);
#endif // defined(MONGOC_ENABLE_SSL)
#endif
}
