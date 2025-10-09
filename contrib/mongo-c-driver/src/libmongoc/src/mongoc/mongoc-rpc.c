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


#include "mongoc-rpc-private.h"

#include "mongoc-counters-private.h"
#include "mongoc-trace-private.h"


bool
mcd_rpc_message_get_body (const mcd_rpc_message *rpc, bson_t *reply)
{
   switch (mcd_rpc_header_get_op_code (rpc)) {
   case MONGOC_OP_CODE_MSG: {
      const size_t sections_count = mcd_rpc_op_msg_get_sections_count (rpc);

      // Look for section kind 0.
      for (size_t index = 0u; index < sections_count; ++index) {
         switch (mcd_rpc_op_msg_section_get_kind (rpc, index)) {
         case 0: { // Body.
            const uint8_t *const body = mcd_rpc_op_msg_section_get_body (rpc, index);

            const int32_t body_len = bson_iter_int32_unsafe (&(bson_iter_t){.raw = body});

            return bson_init_static (reply, body, (size_t) body_len);
         }

         case 1: // Document Sequence.
            continue;

         default:
            // Validated by `mcd_rpc_message_from_data`.
            BSON_UNREACHABLE ("invalid OP_MSG section kind");
         }
      }
      break;
   }

   case MONGOC_OP_CODE_REPLY: {
      if (mcd_rpc_op_reply_get_documents_len (rpc) < 1) {
         return false;
      }

      // Assume the first document in OP_REPLY is the body.
      const uint8_t *const body = mcd_rpc_op_reply_get_documents (rpc);

      return bson_init_static (reply, body, (size_t) bson_iter_int32_unsafe (&(bson_iter_t){.raw = body}));
   }

   default:
      break;
   }

   return false;
}


/* returns true if an error was found. */
static bool
_parse_error_reply (const bson_t *doc, bool check_wce, uint32_t *code, const char **msg)
{
   bson_iter_t iter;
   bool found_error = false;

   ENTRY;

   BSON_ASSERT (doc);
   BSON_ASSERT (code);
   *code = 0;

   /* The server only returns real error codes as int32.
    * But it may return as a double or int64 if a failpoint
    * based on how it is configured to error. */
   if (bson_iter_init_find (&iter, doc, "code") && BSON_ITER_HOLDS_NUMBER (&iter)) {
      *code = (uint32_t) bson_iter_as_int64 (&iter);
      BSON_ASSERT (*code);
      found_error = true;
   }

   if (bson_iter_init_find (&iter, doc, "errmsg") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      *msg = bson_iter_utf8 (&iter, NULL);
      found_error = true;
   } else if (bson_iter_init_find (&iter, doc, "$err") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      *msg = bson_iter_utf8 (&iter, NULL);
      found_error = true;
   }

   if (found_error) {
      /* there was a command error */
      RETURN (true);
   }

   if (check_wce) {
      /* check for a write concern error */
      if (bson_iter_init_find (&iter, doc, "writeConcernError") && BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         bson_iter_t child;
         BSON_ASSERT (bson_iter_recurse (&iter, &child));
         if (bson_iter_find (&child, "code") && BSON_ITER_HOLDS_NUMBER (&child)) {
            *code = (uint32_t) bson_iter_as_int64 (&child);
            BSON_ASSERT (*code);
            found_error = true;
         }
         BSON_ASSERT (bson_iter_recurse (&iter, &child));
         if (bson_iter_find (&child, "errmsg") && BSON_ITER_HOLDS_UTF8 (&child)) {
            *msg = bson_iter_utf8 (&child, NULL);
            found_error = true;
         }
      }
   }

   RETURN (found_error);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cmd_check_ok --
 *
 *       Check if a server reply document is an error message.
 *       Optionally fill out a bson_error_t from the server error.
 *       Does *not* check for writeConcernError.
 *
 * Returns:
 *       false if @doc is an error message, true otherwise.
 *
 * Side effects:
 *       If @doc is an error reply and @error is not NULL, set its
 *       domain, code, and message.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_cmd_check_ok (const bson_t *doc, int32_t error_api_version, bson_error_t *error)
{
   mongoc_error_domain_t domain =
      error_api_version >= MONGOC_ERROR_API_VERSION_2 ? MONGOC_ERROR_SERVER : MONGOC_ERROR_QUERY;
   uint32_t code;
   bson_iter_t iter;
   const char *msg = "Unknown command error";

   ENTRY;

   BSON_ASSERT (doc);

   if (bson_iter_init_find (&iter, doc, "ok") && bson_iter_as_bool (&iter)) {
      /* no error */
      RETURN (true);
   }

   if (!_parse_error_reply (doc, false /* check_wce */, &code, &msg)) {
      RETURN (true);
   }

   if (code == MONGOC_ERROR_PROTOCOL_ERROR || code == 13390) {
      code = MONGOC_ERROR_QUERY_COMMAND_NOT_FOUND;
   } else if (code == 0) {
      code = MONGOC_ERROR_QUERY_FAILURE;
   }

   bson_set_error (error, domain, code, "%s", msg);

   /* there was a command error */
   RETURN (false);
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cmd_check_ok_no_wce --
 *
 *       Check if a server reply document is an error message.
 *       Optionally fill out a bson_error_t from the server error.
 *       If the response contains a writeConcernError, this is considered
 *       an error and returns false.
 *
 * Returns:
 *       false if @doc is an error message, true otherwise.
 *
 * Side effects:
 *       If @doc is an error reply and @error is not NULL, set its
 *       domain, code, and message.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_cmd_check_ok_no_wce (const bson_t *doc, int32_t error_api_version, bson_error_t *error)
{
   mongoc_error_domain_t domain =
      error_api_version >= MONGOC_ERROR_API_VERSION_2 ? MONGOC_ERROR_SERVER : MONGOC_ERROR_QUERY;
   uint32_t code;
   const char *msg = "Unknown command error";

   ENTRY;

   BSON_ASSERT (doc);

   if (!_parse_error_reply (doc, true /* check_wce */, &code, &msg)) {
      RETURN (true);
   }

   if (code == MONGOC_ERROR_PROTOCOL_ERROR || code == 13390) {
      code = MONGOC_ERROR_QUERY_COMMAND_NOT_FOUND;
   } else if (code == 0) {
      code = MONGOC_ERROR_QUERY_FAILURE;
   }

   bson_set_error (error, domain, code, "%s", msg);

   /* there was a command error */
   RETURN (false);
}


/* helper function to parse error reply document to an OP_QUERY */
static void
_mongoc_populate_query_error (const bson_t *doc, int32_t error_api_version, bson_error_t *error)
{
   mongoc_error_domain_t domain =
      error_api_version >= MONGOC_ERROR_API_VERSION_2 ? MONGOC_ERROR_SERVER : MONGOC_ERROR_QUERY;
   uint32_t code = MONGOC_ERROR_QUERY_FAILURE;
   bson_iter_t iter;
   const char *msg = "Unknown query failure";

   ENTRY;

   BSON_ASSERT (doc);

   if (bson_iter_init_find (&iter, doc, "code") && BSON_ITER_HOLDS_NUMBER (&iter)) {
      code = (uint32_t) bson_iter_as_int64 (&iter);
      BSON_ASSERT (code);
   }

   if (bson_iter_init_find (&iter, doc, "$err") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      msg = bson_iter_utf8 (&iter, NULL);
   }

   bson_set_error (error, domain, code, "%s", msg);

   EXIT;
}

bool
mcd_rpc_message_check_ok (mcd_rpc_message *rpc,
                          int32_t error_api_version,
                          bson_error_t *error /* OUT */,
                          bson_t *error_doc /* OUT */)
{
   BSON_ASSERT (rpc);

   ENTRY;

   if (mcd_rpc_header_get_op_code (rpc) != MONGOC_OP_CODE_REPLY) {
      bson_set_error (
         error, MONGOC_ERROR_PROTOCOL, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "Received rpc other than OP_REPLY.");
      RETURN (false);
   }

   const int32_t flags = mcd_rpc_op_reply_get_response_flags (rpc);

   if (flags & MONGOC_OP_REPLY_RESPONSE_FLAG_QUERY_FAILURE) {
      bson_t body;

      if (mcd_rpc_message_get_body (rpc, &body)) {
         _mongoc_populate_query_error (&body, error_api_version, error);

         if (error_doc) {
            bson_destroy (error_doc);
            bson_copy_to (&body, error_doc);
         }

         bson_destroy (&body);
      } else {
         bson_set_error (error, MONGOC_ERROR_QUERY, MONGOC_ERROR_QUERY_FAILURE, "Unknown query failure.");
      }

      RETURN (false);
   }

   if (flags & MONGOC_OP_REPLY_RESPONSE_FLAG_CURSOR_NOT_FOUND) {
      bson_set_error (
         error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "The cursor is invalid or has expired.");

      RETURN (false);
   }


   RETURN (true);
}

void
mcd_rpc_message_egress (const mcd_rpc_message *rpc)
{
   // `mcd_rpc_message_egress` is expected to be called after
   // `mcd_rpc_message_to_iovecs`, which converts the opCode field to
   // little endian.
   int32_t op_code = mcd_rpc_header_get_op_code (rpc);
   op_code = bson_iter_int32_unsafe (&(bson_iter_t){.raw = (const uint8_t *) &op_code});

   if (op_code == MONGOC_OP_CODE_COMPRESSED) {
      mongoc_counter_op_egress_compressed_inc ();
      mongoc_counter_op_egress_total_inc ();

      op_code = mcd_rpc_op_compressed_get_original_opcode (rpc);
      op_code = bson_iter_int32_unsafe (&(bson_iter_t){.raw = (const uint8_t *) &op_code});
   }

   switch (op_code) {
   case MONGOC_OP_CODE_COMPRESSED:
      BSON_UNREACHABLE ("invalid opcode (double compression?!)");
      break;

   case MONGOC_OP_CODE_MSG:
      mongoc_counter_op_egress_msg_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_REPLY:
      BSON_UNREACHABLE ("unexpected OP_REPLY egress");
      break;

   case MONGOC_OP_CODE_UPDATE:
      mongoc_counter_op_egress_update_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_INSERT:
      mongoc_counter_op_egress_insert_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_QUERY:
      mongoc_counter_op_egress_query_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_GET_MORE:
      mongoc_counter_op_egress_getmore_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_DELETE:
      mongoc_counter_op_egress_delete_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   case MONGOC_OP_CODE_KILL_CURSORS:
      mongoc_counter_op_egress_killcursors_inc ();
      mongoc_counter_op_egress_total_inc ();
      break;

   default:
      BSON_UNREACHABLE ("invalid opcode");
   }
}

void
mcd_rpc_message_ingress (const mcd_rpc_message *rpc)
{
   // `mcd_rpc_message_ingress` is expected be called after
   // `mcd_rpc_message_from_data`, which converts the opCode field to native
   // endian.
   int32_t op_code = mcd_rpc_header_get_op_code (rpc);

   if (op_code == MONGOC_OP_CODE_COMPRESSED) {
      mongoc_counter_op_ingress_compressed_inc ();
      mongoc_counter_op_ingress_total_inc ();

      op_code = mcd_rpc_op_compressed_get_original_opcode (rpc);
   }

   switch (op_code) {
   case MONGOC_OP_CODE_COMPRESSED:
      BSON_UNREACHABLE ("invalid opcode (double compression?!)");
      break;

   case MONGOC_OP_CODE_MSG:
      mongoc_counter_op_ingress_msg_inc ();
      mongoc_counter_op_ingress_total_inc ();
      break;

   case MONGOC_OP_CODE_REPLY:
      mongoc_counter_op_ingress_reply_inc ();
      mongoc_counter_op_ingress_total_inc ();
      break;

   case MONGOC_OP_CODE_UPDATE:
      BSON_UNREACHABLE ("unexpected OP_UPDATE ingress");
      break;

   case MONGOC_OP_CODE_INSERT:
      BSON_UNREACHABLE ("unexpected OP_INSERT ingress");
      break;

   case MONGOC_OP_CODE_QUERY:
      BSON_UNREACHABLE ("unexpected OP_QUERY ingress");
      break;

   case MONGOC_OP_CODE_GET_MORE:
      BSON_UNREACHABLE ("unexpected OP_GET_MORE ingress");
      break;

   case MONGOC_OP_CODE_DELETE:
      BSON_UNREACHABLE ("unexpected OP_DELETE ingress");
      break;

   case MONGOC_OP_CODE_KILL_CURSORS:
      BSON_UNREACHABLE ("unexpected OP_KILL_CURSORS ingress");
      break;

   default:
      BSON_UNREACHABLE ("invalid opcode");
   }
}
