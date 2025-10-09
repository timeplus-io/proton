/*
 * Copyright 2015 MongoDB, Inc.
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


#include <mongoc/mongoc-rpc-private.h>
#include "mongoc/mongoc.h"

#include "mock-server.h"
#include "../test-conveniences.h"
#include "../TestSuite.h"

static bool
is_command_ns (const char *ns);

static void
request_from_query (request_t *request);

static void
request_from_killcursors (request_t *request);

static void
request_from_getmore (request_t *request);

static void
request_from_op_msg (request_t *request);

static char *
query_flags_str (int32_t flags);

request_t *
request_new (const mongoc_buffer_t *buffer,
             int32_t msg_len,
             mock_server_t *server,
             mongoc_stream_t *client,
             uint16_t client_port,
             sync_queue_t *replies)
{
   BSON_ASSERT_PARAM (buffer);
   BSON_ASSERT_PARAM (server);
   BSON_ASSERT_PARAM (client);
   BSON_ASSERT_PARAM (replies);

   BSON_ASSERT (bson_in_range_signed (size_t, msg_len));

   request_t *const request = (request_t *) bson_malloc0 (sizeof *request);

   request->data = bson_malloc ((size_t) msg_len);
   request->data_len = (size_t) msg_len;
   request->replies = replies;

   memcpy (request->data, buffer->data, request->data_len);

   const void *data_end = NULL;
   request->rpc = mcd_rpc_message_from_data (request->data, request->data_len, &data_end);

   if (!request->rpc) {
      test_error ("failed to parse incoming message due to byte %zu of %zu",
                  (size_t) ((const uint8_t *) data_end - request->data),
                  request->data_len);
   }

   request->opcode = mcd_rpc_header_get_op_code (request->rpc);
   request->server = server;
   request->client = client;
   request->client_port = client_port;
   _mongoc_array_init (&request->docs, sizeof (bson_t *));

   switch (request->opcode) {
   case MONGOC_OP_CODE_COMPRESSED:
      // Nothing to do.
      break;

   case MONGOC_OP_CODE_QUERY:
      request_from_query (request);
      break;

   case MONGOC_OP_CODE_KILL_CURSORS:
      // Still being used for legacy OP_KILL_CURSORS tests.
      request_from_killcursors (request);
      break;

   case MONGOC_OP_CODE_GET_MORE:
      // Still being used for exhaust cursor tests.
      request_from_getmore (request);
      break;

   case MONGOC_OP_CODE_MSG:
      request_from_op_msg (request);
      break;

   case MONGOC_OP_CODE_DELETE:
   case MONGOC_OP_CODE_INSERT:
   case MONGOC_OP_CODE_REPLY:
   case MONGOC_OP_CODE_UPDATE:
   default:
      test_error ("Mock server does not support opcode %d", request->opcode);
   }

   return request;
}

const bson_t *
request_get_doc (const request_t *request, size_t n)
{
   BSON_ASSERT (request);
   return _mongoc_array_index (&request->docs, const bson_t *, n);
}

void
assert_request_matches_flags (const request_t *request, uint32_t flags)
{
   BSON_ASSERT (request);

   const int32_t request_flags = mcd_rpc_op_query_get_flags (request->rpc);
   if (bson_cmp_not_equal_su (request_flags, flags)) {
      test_error ("request's query flags are %s, expected %s",
                  query_flags_str (request_flags),
                  query_flags_str ((int32_t) flags));
   }
}

/* TODO: take file, line, function params from caller, wrap in macro */
bool
request_matches_query (const request_t *request,
                       const char *ns,
                       uint32_t flags,
                       uint32_t skip,
                       int32_t n_return,
                       const char *query_json,
                       const char *fields_json,
                       bool is_command)
{
   const bson_t *doc;
   const bson_t *doc2;
   char *doc_as_json;
   bool ret = false;

   BSON_ASSERT (request);
   BSON_ASSERT (request->docs.len <= 2);

   if (request->docs.len) {
      doc = request_get_doc (request, 0);
      doc_as_json = bson_as_json (doc, NULL);
   } else {
      doc = NULL;
      doc_as_json = NULL;
   }

   if (!match_json (doc, is_command, __FILE__, __LINE__, BSON_FUNC, query_json)) {
      /* match_json has logged the err */
      goto done;
   }

   if (request->docs.len > 1) {
      doc2 = request_get_doc (request, 1);
   } else {
      doc2 = NULL;
   }

   if (!match_json (doc2, false, __FILE__, __LINE__, BSON_FUNC, fields_json)) {
      /* match_json has logged the err */
      goto done;
   }

   if (request->is_command && !is_command) {
      test_error ("expected query, got command: %s", doc_as_json);
      goto done;
   }

   if (!request->is_command && is_command) {
      test_error ("expected command, got query: %s", doc_as_json);
      goto done;
   }

   if (request->opcode != MONGOC_OPCODE_QUERY) {
      test_error ("request's opcode does not match QUERY: %s", doc_as_json);
      goto done;
   }

   const char *const request_ns = mcd_rpc_op_query_get_full_collection_name (request->rpc);
   if (0 != strcmp (request_ns, ns)) {
      test_error ("request's namespace is '%s', expected '%s': %s", request_ns, ns, doc_as_json);
      goto done;
   }

   assert_request_matches_flags (request, flags);

   const int32_t request_skip = mcd_rpc_op_query_get_number_to_skip (request->rpc);
   if (bson_cmp_not_equal_su (request_skip, skip)) {
      test_error ("requests's skip = %" PRId32 ", expected %" PRIu32 ": %s", request_skip, skip, doc_as_json);
      goto done;
   }


   const int32_t request_n_return = mcd_rpc_op_query_get_number_to_return (request->rpc);
   bool n_return_equal = (request_n_return == n_return);

   if (!n_return_equal && abs (request_n_return) == 1) {
      /* quirk: commands from mongoc_client_command_simple have n_return 1,
       * from mongoc_topology_scanner_t have n_return -1
       */
      n_return_equal = abs (request_n_return) == n_return;
   }

   if (!n_return_equal) {
      test_error (
         "requests's n_return = %" PRId32 ", expected %" PRId32 ": %s", request_n_return, n_return, doc_as_json);
      goto done;
   }

   ret = true;

done:
   bson_free (doc_as_json);
   return ret;
}


/* TODO: take file, line, function params from caller, wrap in macro */
bool
request_matches_kill_cursors (const request_t *request, int64_t cursor_id)
{
   BSON_ASSERT (request);

   if (request->opcode != MONGOC_OPCODE_KILL_CURSORS) {
      test_error ("request's opcode does not match KILL_CURSORS, got: %d", request->opcode);
      return false;
   }

   const int32_t request_n_cursors = mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (request->rpc);
   if (request_n_cursors != 1) {
      test_error ("request's n_cursors is %" PRId32 ", expected 1", request_n_cursors);
      return false;
   }

   const int64_t request_cursor_id = mcd_rpc_op_kill_cursors_get_cursor_ids (request->rpc)[0];
   if (request_cursor_id != cursor_id) {
      test_error ("request's cursor_id %" PRId64 ", expected %" PRId64, request_cursor_id, cursor_id);
      return false;
   }

   return true;
}

/*--------------------------------------------------------------------------
 *
 * request_matches_msg --
 *
 *       Test that a client OP_MSG matches a pattern. The OP_MSG consists
 *       of at least one document (the command body) and optional sequence
 *       of additional documents (e.g., documents in a bulk insert). The
 *       documents in the actual client message are compared pairwise to
 *       the patterns in @docs.
 *
 * Returns:
 *       True if the body and document sequence of the request match
 *       the given pattern.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
request_matches_msg (const request_t *request, uint32_t flags, const bson_t **docs, size_t n_docs)
{
   const bson_t *doc;
   const bson_t *pattern;
   bson_error_t bson_error;
   bool is_command_doc;

   BSON_ASSERT (request);
   if (request->opcode != MONGOC_OPCODE_MSG) {
      test_error ("%s", "request's opcode does not match OP_MSG");
   }

   BSON_ASSERT (request->docs.len >= 1);

   for (size_t i = 0u; i < n_docs; i++) {
      pattern = docs[i];

      /* make sure the pattern is reasonable, e.g. that we didn't pass a string
       * instead of a bson_t* by mistake */
      ASSERT_WITH_MSG (bson_validate_with_error (pattern, BSON_VALIDATE_EMPTY_KEYS | BSON_VALIDATE_UTF8, &bson_error),
                       "invalid argument at position %zu (note: must be "
                       "bson_t*, not char*):\ndomain: %" PRIu32 ", code: %" PRIu32 ", message: %s\n",
                       i,
                       bson_error.domain,
                       bson_error.code,
                       bson_error.message);

      if (i > request->docs.len) {
         fprintf (stderr, "Expected at least %zu documents in request, got %zu\n", i, request->docs.len);
         return false;
      }

      doc = request_get_doc (request, i);
      /* pass is_command=true for first doc, including "find" command */
      is_command_doc = (i == 0u);
      assert_match_bson (doc, pattern, is_command_doc);
   }

   if (n_docs < request->docs.len) {
      fprintf (stderr, "Expected %zu documents in request, got %zu\n", n_docs, request->docs.len);
      return false;
   }

   const uint32_t request_flags = mcd_rpc_op_msg_get_flag_bits (request->rpc);
   if (request_flags != flags) {
      fprintf (stderr, "Expected OP_MSG flags %" PRIu32 ", got %" PRIu32 "\n", flags, request_flags);
      return false;
   }

   return true;
}


/*--------------------------------------------------------------------------
 *
 * request_matches_msgv --
 *
 *       Variable-args version of request_matches_msg.
 *
 * Returns:
 *       True if the body and document sequence of the request match
 *       the given pattern.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

bool
request_matches_msgv (const request_t *request, uint32_t flags, va_list *args)
{
   bson_t **docs;
   size_t n_docs, allocated;
   bool r;

   n_docs = 0;
   allocated = 1;
   docs = bson_malloc (allocated * sizeof (bson_t *));
   while ((docs[n_docs] = va_arg (*args, bson_t *))) {
      n_docs++;
      if (n_docs == allocated) {
         allocated = bson_next_power_of_two (allocated + 1);
         docs = bson_realloc (docs, allocated * sizeof (bson_t *));
      }
   }

   r = request_matches_msg (request, flags, (const bson_t **) docs, n_docs);

   bson_free (docs);

   return r;
}

/*--------------------------------------------------------------------------
 *
 * request_get_server_port --
 *
 *       Get the port of the server this request was sent to.
 *
 * Returns:
 *       A port number.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

uint16_t
request_get_server_port (request_t *request)
{
   return mock_server_get_port (request->server);
}


/*--------------------------------------------------------------------------
 *
 * request_get_client_port --
 *
 *       Get the client port this request was sent from.
 *
 * Returns:
 *       A port number.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

uint16_t
request_get_client_port (request_t *request)
{
   return request->client_port;
}


/*--------------------------------------------------------------------------
 *
 * request_destroy --
 *
 *       Free a request_t.
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
request_destroy (request_t *request)
{
   size_t i;
   bson_t *doc;

   for (i = 0; i < request->docs.len; i++) {
      doc = _mongoc_array_index (&request->docs, bson_t *, i);
      bson_destroy (doc);
   }

   _mongoc_array_destroy (&request->docs);
   bson_free (request->command_name);
   bson_free (request->as_str);
   mcd_rpc_message_destroy (request->rpc);
   bson_free (request->data);
   bson_free (request);
}


static bool
is_command_ns (const char *ns)
{
   size_t len = strlen (ns);
   const char *cmd = ".$cmd";
   size_t cmd_len = strlen (cmd);

   return len > cmd_len && !strncmp (ns + len - cmd_len, cmd, cmd_len);
}


static char *
query_flags_str (int32_t flags)
{
   int flag = 1;
   bson_string_t *str = bson_string_new ("");
   bool begun = false;

   if (flags == MONGOC_OP_QUERY_FLAG_NONE) {
      bson_string_append (str, "0");
   } else {
      while (flag <= MONGOC_OP_QUERY_FLAG_PARTIAL) {
         flag <<= 1;

         if (flags & flag) {
            if (begun) {
               bson_string_append (str, "|");
            }

            begun = true;

            switch (flag) {
            case MONGOC_OP_QUERY_FLAG_TAILABLE_CURSOR:
               bson_string_append (str, "TAILABLE");
               break;
            case MONGOC_OP_QUERY_FLAG_SECONDARY_OK:
               bson_string_append (str, "SECONDARY_OK");
               break;
            case MONGOC_OP_QUERY_FLAG_OPLOG_REPLAY:
               bson_string_append (str, "OPLOG_REPLAY");
               break;
            case MONGOC_OP_QUERY_FLAG_NO_CURSOR_TIMEOUT:
               bson_string_append (str, "NO_TIMEOUT");
               break;
            case MONGOC_OP_QUERY_FLAG_AWAIT_DATA:
               bson_string_append (str, "AWAIT_DATA");
               break;
            case MONGOC_OP_QUERY_FLAG_EXHAUST:
               bson_string_append (str, "EXHAUST");
               break;
            case MONGOC_OP_QUERY_FLAG_PARTIAL:
               bson_string_append (str, "PARTIAL");
               break;
            case MONGOC_OP_QUERY_FLAG_NONE:
            default:
               BSON_ASSERT (false);
            }
         }
      }
   }

   return bson_string_free (str, false); /* detach buffer */
}


static int32_t
length_prefix (const uint8_t *data)
{
   uint32_t storage;
   memcpy (&storage, data, sizeof (storage));
   storage = BSON_UINT32_FROM_LE (storage);
   int32_t res;
   memcpy (&res, &storage, sizeof (storage));
   return res;
}


static void
request_from_query (request_t *request)
{
   bson_iter_t iter;
   bson_string_t *query_as_str = bson_string_new ("OP_QUERY ");
   char *str;

   const int32_t request_flags = mcd_rpc_op_query_get_flags (request->rpc);
   const char *const request_coll = mcd_rpc_op_query_get_full_collection_name (request->rpc);
   const int32_t request_skip = mcd_rpc_op_query_get_number_to_skip (request->rpc);
   const int32_t request_return = mcd_rpc_op_query_get_number_to_return (request->rpc);
   const void *const request_query = mcd_rpc_op_query_get_query (request->rpc);
   const void *const request_fields = mcd_rpc_op_query_get_return_fields_selector (request->rpc);

   {
      const int32_t len = length_prefix (request_query);
      bson_t *const query = bson_new_from_data (request_query, (size_t) len);
      BSON_ASSERT (query);
      _mongoc_array_append_val (&request->docs, query);

      bson_string_append_printf (query_as_str, "%s ", request_coll);

      if (is_command_ns (request_coll)) {
         request->is_command = true;

         if (bson_iter_init (&iter, query) && bson_iter_next (&iter)) {
            request->command_name = bson_strdup (bson_iter_key (&iter));
         } else {
            fprintf (stderr, "WARNING: no command name for %s\n", request_coll);
         }
      }

      str = bson_as_json (query, NULL);
      bson_string_append (query_as_str, str);
      bson_free (str);
   }

   if (request_fields) {
      const int32_t len = length_prefix (request_fields);
      bson_t *const fields = bson_new_from_data (request_fields, (size_t) len);
      BSON_ASSERT (fields);
      _mongoc_array_append_val (&request->docs, fields);

      str = bson_as_json (fields, NULL);
      bson_string_append (query_as_str, " fields=");
      bson_string_append (query_as_str, str);
      bson_free (str);
   }

   bson_string_append (query_as_str, " flags=");

   str = query_flags_str (request_flags);
   bson_string_append (query_as_str, str);
   bson_free (str);

   if (request_skip) {
      bson_string_append_printf (query_as_str, " skip=%" PRId32, request_skip);
   }

   if (request_return) {
      bson_string_append_printf (query_as_str, " n_return=%" PRId32, request_return);
   }

   request->as_str = bson_string_free (query_as_str, false);
}


static void
request_from_killcursors (request_t *request)
{
   /* protocol allows multiple cursor ids but we only implement one */
   BSON_ASSERT (mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (request->rpc) == 1);
   request->as_str =
      bson_strdup_printf ("OP_KILLCURSORS %" PRId64, mcd_rpc_op_kill_cursors_get_cursor_ids (request->rpc)[0]);
}


static void
request_from_getmore (request_t *request)
{
   request->as_str = bson_strdup_printf ("OP_GETMORE %s %" PRId64 " n_return=%d",
                                         mcd_rpc_op_get_more_get_full_collection_name (request->rpc),
                                         mcd_rpc_op_get_more_get_cursor_id (request->rpc),
                                         mcd_rpc_op_get_more_get_number_to_return (request->rpc));
}


static void
parse_op_msg_doc (request_t *request, const uint8_t *data, size_t data_len, bson_string_t *msg_as_str)
{
   const uint8_t *pos = data;
   while (pos < data + data_len) {
      if (pos > data) {
         bson_string_append (msg_as_str, ", ");
      }

      const int32_t doc_len = length_prefix (pos);
      const bson_t *const doc = bson_new_from_data (pos, (size_t) doc_len);
      BSON_ASSERT (doc);
      _mongoc_array_append_val (&request->docs, doc);

      char *const str = bson_as_json (doc, NULL);
      bson_string_append (msg_as_str, str);
      bson_free (str);

      pos += doc_len;
   }
}


static void
request_from_op_msg (request_t *request)
{
   bson_string_t *msg_as_str = bson_string_new ("OP_MSG");

   const size_t sections_count = mcd_rpc_op_msg_get_sections_count (request->rpc);

   BSON_ASSERT (sections_count <= 2u);
   for (size_t index = 0; index < sections_count; ++index) {
      bson_string_append (msg_as_str, (index > 0 ? ", " : " "));
      const uint8_t kind = mcd_rpc_op_msg_section_get_kind (request->rpc, index);
      switch (kind) {
      case 0: { /* a single BSON document */
         const void *const body = mcd_rpc_op_msg_section_get_body (request->rpc, index);
         parse_op_msg_doc (request, body, (size_t) length_prefix (body), msg_as_str);
         break;
      }

      case 1: { /* a sequence of BSON documents */
         bson_string_append (msg_as_str, mcd_rpc_op_msg_section_get_identifier (request->rpc, index));
         bson_string_append (msg_as_str, ": [");
         parse_op_msg_doc (request,
                           mcd_rpc_op_msg_section_get_document_sequence (request->rpc, index),
                           mcd_rpc_op_msg_section_get_document_sequence_length (request->rpc, index),
                           msg_as_str);

         bson_string_append (msg_as_str, "]");
         break;
      }

      default:
         test_error ("Unimplemented payload type %d\n", kind);
      }
   }

   request->as_str = bson_string_free (msg_as_str, false);
   request->is_command = true; /* true for all OP_MSG requests */

   if (request->docs.len) {
      const bson_t *doc = request_get_doc (request, 0);
      bson_iter_t iter;
      if (bson_iter_init (&iter, doc) && bson_iter_next (&iter)) {
         request->command_name = bson_strdup (bson_iter_key (&iter));
      }
   }
}
