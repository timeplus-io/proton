#include "mcd-rpc.h"

// Header-only dependency. Does NOT require linking with libmongoc.
#define MONGOC_INSIDE
#include "mongoc-iovec.h"
#undef MONGOC_INSIDE

#include <bson/bson.h>


typedef struct _mcd_rpc_message_header mcd_rpc_message_header;

typedef struct _mcd_rpc_op_compressed mcd_rpc_op_compressed;

typedef struct _mcd_rpc_op_msg_section mcd_rpc_op_msg_section;
typedef struct _mcd_rpc_op_msg mcd_rpc_op_msg;

typedef struct _mcd_rpc_op_reply mcd_rpc_op_reply;
typedef struct _mcd_rpc_op_update mcd_rpc_op_update;
typedef struct _mcd_rpc_op_insert mcd_rpc_op_insert;
typedef struct _mcd_rpc_op_query mcd_rpc_op_query;
typedef struct _mcd_rpc_op_get_more mcd_rpc_op_get_more;
typedef struct _mcd_rpc_op_delete mcd_rpc_op_delete;
typedef struct _mcd_rpc_op_kill_cursors mcd_rpc_op_kill_cursors;


// See: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol
struct _mcd_rpc_message_header {
   int32_t message_length;
   int32_t request_id;
   int32_t response_to;
   int32_t op_code;
   bool is_in_iovecs_state; // Not part of actual message.
};

struct _mcd_rpc_op_compressed {
   mcd_rpc_message_header header;
   int32_t original_opcode;
   int32_t uncompressed_size;
   uint8_t compressor_id;
   const void *compressed_message; // Non-owning.
   size_t compressed_message_len;  // Not part of actual message.
};

struct _mcd_rpc_op_msg_section {
   uint8_t kind;

   union payload {
      // Kind 0.
      struct body {
         int32_t section_len; // Not part of actual message.
         const void *bson;    // bson_t data, non-owning.
      } body;

      // Kind 1.
      struct document_sequence {
         int32_t section_len;
         const char *identifier;   // Non-owning.
         size_t identifier_len;    // Not part of actual message.
         const void *bson_objects; // Array of bson_t data, non-owning.
         size_t bson_objects_len;  // Not part of actual message.
      } document_sequence;
   } payload;
};

struct _mcd_rpc_op_msg {
   mcd_rpc_message_header header;
   uint32_t flag_bits;
   mcd_rpc_op_msg_section *sections; // Owning.
   size_t sections_count;            // Not part of actual message.
   uint32_t checksum;                // Optional, ignored by Drivers.
   bool checksum_set;                // Not part of actual message.
};

struct _mcd_rpc_op_reply {
   mcd_rpc_message_header header;
   int32_t response_flags;
   int64_t cursor_id;
   int32_t starting_from;
   int32_t number_returned;
   const void *documents; // Array of bson_t data, non-owning.
   size_t documents_len;  // Not part of actual message.
};

struct _mcd_rpc_op_update {
   mcd_rpc_message_header header;
   const char *full_collection_name; // Non-owning.
   size_t full_collection_name_len;  // Not part of actual message.
   int32_t flags;
   const void *selector; // bson_t data, non-owning.
   const void *update;   // bson_t data, non-owning.
};

struct _mcd_rpc_op_insert {
   mcd_rpc_message_header header;
   int32_t flags;
   const char *full_collection_name; // Non-owning.
   size_t full_collection_name_len;  // Not part of actual message.
   const void *documents;            // Array of bson_t data, non-owning.
   size_t documents_len;             // Not part of actual message.
};

struct _mcd_rpc_op_query {
   mcd_rpc_message_header header;
   int32_t flags;
   const char *full_collection_name; // Non-owning.
   size_t full_collection_name_len;  // Not part of actual message.
   int32_t number_to_skip;
   int32_t number_to_return;
   const void *query;                  // bson_t, non-owning.
   const void *return_fields_selector; // Optional bson_t, non-owning.
};

struct _mcd_rpc_op_get_more {
   mcd_rpc_message_header header;
   const char *full_collection_name; // Non-owning.
   size_t full_collection_name_len;  // Not part of actual message.
   int32_t number_to_return;
   int64_t cursor_id;
};

struct _mcd_rpc_op_delete {
   mcd_rpc_message_header header;
   const char *full_collection_name;
   size_t full_collection_name_len; // Not part of actual message.
   int32_t flags;
   const void *selector; // bson_t data, non-owning.
};

struct _mcd_rpc_op_kill_cursors {
   mcd_rpc_message_header header;
   int32_t zero; // Reserved.
   int32_t number_of_cursor_ids;
   int64_t *cursor_ids; // Array of int64_t, owning.
};

union _mcd_rpc_message {
   mcd_rpc_message_header msg_header; // Common initial sequence.

   mcd_rpc_op_compressed op_compressed;
   mcd_rpc_op_msg op_msg;

   mcd_rpc_op_reply op_reply;
   mcd_rpc_op_update op_update;
   mcd_rpc_op_insert op_insert;
   mcd_rpc_op_query op_query;
   mcd_rpc_op_get_more op_get_more;
   mcd_rpc_op_delete op_delete;
   mcd_rpc_op_kill_cursors op_kill_cursors;
};


// The minimum byte length of a valid RPC message is 16 bytes (messageHeader is
// the common initial sequence for all opcodes). RPC message lengths less than
// 16 may be encountered parsing due to invalid or malformed input.
#define MONGOC_RPC_MINIMUM_MESSAGE_LENGTH INT32_C (16)

// The minimum byte length of a valid BSON document is 5 bytes (empty document):
//  - length (int32): total document length (including itself).
//  - "\x00": document terminator.
// BSON document lengths less than 5 may be encountered during parsing due to
// invalid or malformed input.
#define MONGOC_RPC_MINIMUM_BSON_LENGTH INT32_C (5)

// To avoid unexpected behavior on big endian targets after
// `mcd_rpc_message_to_iovecs` due to fields being converted to little endian,
// forbid use of accessors unless the RPC message has been reset to an
// initialized state by asserting `!is_in_iovecs_state` even on little endian
// targets.
#define ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS            \
   if (1) {                                              \
      BSON_ASSERT_PARAM (rpc);                           \
      BSON_ASSERT (!rpc->msg_header.is_in_iovecs_state); \
   } else                                                \
      (void) 0


static int32_t
_int32_from_le (const void *data)
{
   BSON_ASSERT_PARAM (data);
   return bson_iter_int32_unsafe (&(bson_iter_t){.raw = data});
}


// In addition to validating expected size against remaining bytes, ensure
// proper conversion from little endian format.
#define MONGOC_RPC_CONSUME(type, raw_type, from_le)                                         \
   static bool _consume_##type (type *target, const uint8_t **ptr, size_t *remaining_bytes) \
   {                                                                                        \
      BSON_ASSERT_PARAM (target);                                                           \
      BSON_ASSERT_PARAM (ptr);                                                              \
      BSON_ASSERT_PARAM (remaining_bytes);                                                  \
                                                                                            \
      if (*remaining_bytes < sizeof (type)) {                                               \
         return false;                                                                      \
      }                                                                                     \
                                                                                            \
      raw_type raw;                                                                         \
      memcpy (&raw, *ptr, sizeof (type));                                                   \
                                                                                            \
      const raw_type native = from_le (raw);                                                \
      memcpy (target, &native, sizeof (type));                                              \
                                                                                            \
      *ptr += sizeof (type);                                                                \
      *remaining_bytes -= sizeof (type);                                                    \
                                                                                            \
      return true;                                                                          \
   }

MONGOC_RPC_CONSUME (uint8_t, uint8_t, (uint8_t))
MONGOC_RPC_CONSUME (int32_t, uint32_t, BSON_UINT32_FROM_LE)
MONGOC_RPC_CONSUME (uint32_t, uint32_t, BSON_UINT32_FROM_LE)
MONGOC_RPC_CONSUME (int64_t, uint64_t, BSON_UINT64_FROM_LE)

static bool
_consume_utf8 (const char **target, size_t *length, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (target);
   BSON_ASSERT_PARAM (length);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   *target = (const char *) *ptr;

   const uint8_t *iter = *ptr;
   size_t rem = *remaining_bytes;

   while (rem > 0u && *iter != '\0') {
      iter += 1u;
      rem -= 1u;
   }

   if (rem == 0u) {
      return false;
   }

   // Consume string including the null terminator.
   iter += 1u;
   rem -= 1u;
   *length = *remaining_bytes - rem;
   *ptr = iter;
   *remaining_bytes = rem;

   return true;
}


static bool
_consume_reserved_zero (const uint8_t **ptr, size_t *remaining_bytes)
{
   int32_t zero;

   if (!_consume_int32_t (&zero, ptr, remaining_bytes)) {
      return false;
   }

   if (zero != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // ZERO as invalid input.
      return false;
   }

   return true;
}


static bool
_consume_bson_objects (const uint8_t **ptr, size_t *remaining_bytes, int32_t *num_parsed, int32_t limit)
{
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);
   BSON_ASSERT (num_parsed || true);

   int32_t count = 0;

   // Validate document count and lengths.
   while ((count < limit) && (*remaining_bytes > 0u)) {
      int32_t doc_len;
      if (!_consume_int32_t (&doc_len, ptr, remaining_bytes)) {
         return false;
      }

      if (doc_len < MONGOC_RPC_MINIMUM_BSON_LENGTH ||
          bson_cmp_greater_su (doc_len, *remaining_bytes + sizeof (int32_t))) {
         *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                   // document as invalid input.
         return false;
      }

      // Consume rest of document without validation.
      *ptr += (size_t) doc_len - sizeof (int32_t);
      *remaining_bytes -= (size_t) doc_len - sizeof (int32_t);

      count += 1;
   }

   if (num_parsed) {
      *num_parsed = count;
   }

   return true;
}


static bool
_consume_op_compressed (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_compressed *const op_compressed = &rpc->op_compressed;

   if (!_consume_int32_t (&op_compressed->original_opcode, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_compressed->uncompressed_size, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_uint8_t (&op_compressed->compressor_id, ptr, remaining_bytes)) {
      return false;
   }

   // Consume compressedMessage without validation.
   op_compressed->compressed_message = *ptr;
   op_compressed->compressed_message_len = *remaining_bytes;
   *ptr += *remaining_bytes;
   *remaining_bytes = 0u;

   return true;
}


static bool
_consume_op_msg_section (
   mcd_rpc_op_msg *op_msg, const uint8_t **ptr, size_t *remaining_bytes, size_t *capacity, bool *found_kind_0)
{
   BSON_ASSERT_PARAM (op_msg);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (found_kind_0);

   mcd_rpc_op_msg_section section;

   if (!_consume_uint8_t (&section.kind, ptr, remaining_bytes)) {
      return false;
   }

   // There is no ordering implied by payload types. A section with payload type
   // 1 can be serialized before payload type 0.
   if (section.kind == 0) {
      if (*found_kind_0) {
         *ptr -= sizeof (uint8_t); // Revert so *data_end points to start of
                                   // section as invalid input.
         return false;
      }

      *found_kind_0 = true;
   }

   switch (section.kind) {
   case 0: { // Body
      section.payload.body.section_len = _int32_from_le (*ptr);
      section.payload.body.bson = *ptr;

      int32_t num_parsed = 0;
      if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1) || num_parsed != 1) {
         return false;
      }

      break;
   }

   case 1: { // Document Sequence
      if (!_consume_int32_t (&section.payload.document_sequence.section_len, ptr, remaining_bytes)) {
         return false;
      }

      // Minimum byte length of a valid document sequence section is 4 bytes
      // (section length). Actual minimum length would also account for the
      // identifier field, but 4 bytes is sufficient to avoid unsigned integer
      // overflow when computing `remaining_section_bytes` and to encourage as
      // much progress is made parsing input data as able.
      if (bson_cmp_less_su (section.payload.document_sequence.section_len, sizeof (int32_t))) {
         *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                   // document sequence as invalid input.
         return false;
      }

      size_t remaining_section_bytes = (size_t) section.payload.document_sequence.section_len - sizeof (int32_t);

      // Section length exceeds remaining data.
      if (remaining_section_bytes > *remaining_bytes) {
         *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                   // document sequence as invalid input.
         return false;
      }

      // Consume identifier without validating uniqueness.
      {
         const uint8_t *const identifier_begin = *ptr;
         if (!_consume_utf8 (&section.payload.document_sequence.identifier,
                             &section.payload.document_sequence.identifier_len,
                             ptr,
                             &remaining_section_bytes)) {
            return false;
         }
         *remaining_bytes -= (size_t) (*ptr - identifier_begin);
      }

      section.payload.document_sequence.bson_objects = *ptr;
      section.payload.document_sequence.bson_objects_len = remaining_section_bytes;

      _consume_bson_objects (ptr, &remaining_section_bytes, NULL, INT32_MAX);

      // Should have exhausted all bytes in the section.
      if (remaining_section_bytes != 0u) {
         return false;
      }

      *remaining_bytes -= (size_t) (*ptr - (const uint8_t *) section.payload.document_sequence.bson_objects);

      break;
   }

   default:
      *ptr -= sizeof (uint8_t); // Revert so *data_end points to start of kind
                                // as invalid input.
      return false;
   }

   // Expand sections capacity if required.
   if (op_msg->sections_count >= *capacity) {
      *capacity *= 2u;
      op_msg->sections = bson_realloc (op_msg->sections, *capacity * sizeof (mcd_rpc_op_msg_section));
   }

   // Append the valid section.
   op_msg->sections[op_msg->sections_count++] = section;

   return true;
}

static bool
_consume_op_msg (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_msg *const op_msg = &rpc->op_msg;

   if (!_consume_uint32_t (&op_msg->flag_bits, ptr, remaining_bytes)) {
      return false;
   }

   {
      const uint32_t defined_bits =
         MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT | MONGOC_OP_MSG_FLAG_MORE_TO_COME | MONGOC_OP_MSG_FLAG_EXHAUST_ALLOWED;

      // Clients MUST error if any unsupported or undefined required bits are
      // set to 1 and MUST ignore all undefined optional bits.
      if (((op_msg->flag_bits & UINT32_C (0x0000FFFF)) & ~defined_bits) != 0u) {
         *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                   // flagBits as invalid input.
         return false;
      }
   }

   // Each message contains one or more sections. Preallocate space for two
   // sections, which should cover the most frequent cases.
   size_t capacity = 2u;
   op_msg->sections = bson_malloc (capacity * sizeof (mcd_rpc_op_msg_section));
   op_msg->sections_count = 0u;

   // A fully constructed OP_MSG MUST contain exactly one Payload Type 0, and
   // optionally any number of Payload Type 1 where each identifier MUST be
   // unique per message.
   {
      bool found_kind_0 = false;

      // A section requires at least 5 bytes for kind (1) + length (4).
      while (*remaining_bytes > 4u) {
         if (!_consume_op_msg_section (op_msg, ptr, remaining_bytes, &capacity, &found_kind_0)) {
            return false;
         }
      }

      if (!found_kind_0) {
         return false;
      }
   }

   if ((op_msg->flag_bits & MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT) != 0u) {
      if (!_consume_uint32_t (&op_msg->checksum, ptr, remaining_bytes)) {
         return false;
      }

      op_msg->checksum_set = true;
   }

   return true;
}


static bool
_consume_op_reply (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_reply *const op_reply = &rpc->op_reply;

   if (!_consume_int32_t (&op_reply->response_flags, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int64_t (&op_reply->cursor_id, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_reply->starting_from, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_reply->number_returned, ptr, remaining_bytes)) {
      return false;
   }

   if (op_reply->number_returned < 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // numberReturned as invalid input.
      return false;
   }

   if (op_reply->number_returned > 0) {
      op_reply->documents = *ptr;
      op_reply->documents_len = *remaining_bytes;
   } else {
      op_reply->documents = NULL;
      op_reply->documents_len = 0u;
   }

   int32_t num_parsed = 0;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, op_reply->number_returned) ||
       num_parsed != op_reply->number_returned) {
      return false;
   }

   return true;
}


static bool
_consume_op_update (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_update *const op_update = &rpc->op_update;

   if (!_consume_reserved_zero (ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_utf8 (&op_update->full_collection_name, &op_update->full_collection_name_len, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_update->flags, ptr, remaining_bytes)) {
      return false;
   }

   // Bits 2-31 are reserved. Must be set to 0.
   if ((op_update->flags & ~(0x00000003)) != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // flags as invalid input.
      return false;
   }

   int32_t num_parsed = 0;

   op_update->selector = *ptr;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1) || num_parsed != 1) {
      return false;
   }

   op_update->update = *ptr;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1) || num_parsed != 1) {
      return false;
   }

   return true;
}


static bool
_consume_op_insert (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_insert *const op_insert = &rpc->op_insert;

   if (!_consume_int32_t (&op_insert->flags, ptr, remaining_bytes)) {
      return false;
   }

   // Bits 1-31 are reserved. Must be set to 0.
   if ((op_insert->flags & ~(0x00000001)) != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // flags as invalid input.
      return false;
   }

   if (!_consume_utf8 (&op_insert->full_collection_name, &op_insert->full_collection_name_len, ptr, remaining_bytes)) {
      return false;
   }

   op_insert->documents = *ptr;
   op_insert->documents_len = *remaining_bytes;

   int32_t num_parsed = 0;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, INT32_MAX) || num_parsed == 0) {
      return false;
   }

   return true;
}


static bool
_consume_op_query (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_query *const op_query = &rpc->op_query;

   if (!_consume_int32_t (&op_query->flags, ptr, remaining_bytes)) {
      return false;
   }

   // Bit 0 is reserved. Must be set to 0.
   if ((op_query->flags & 0x01) != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // flags as invalid input.
      return false;
   }

   // Bits 8-31 are reserved. Must be set to 0.
   if ((op_query->flags & ~(0x0000007F)) != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // flags as invalid input.
      return false;
   }

   if (!_consume_utf8 (&op_query->full_collection_name, &op_query->full_collection_name_len, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_query->number_to_skip, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_query->number_to_return, ptr, remaining_bytes)) {
      return false;
   }

   int32_t num_parsed = 0;

   op_query->query = *ptr;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1) || num_parsed != 1) {
      return false;
   }

   op_query->return_fields_selector = *ptr;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1)) {
      return false;
   }

   // returnFieldsSelector is optional.
   if (num_parsed == 0) {
      op_query->return_fields_selector = NULL;
   }

   return true;
}


static bool
_consume_op_get_more (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_get_more *const op_get_more = &rpc->op_get_more;

   if (!_consume_reserved_zero (ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_utf8 (
          &op_get_more->full_collection_name, &op_get_more->full_collection_name_len, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_get_more->number_to_return, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int64_t (&op_get_more->cursor_id, ptr, remaining_bytes)) {
      return false;
   }

   return true;
}


static bool
_consume_op_delete (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_delete *const op_delete = &rpc->op_delete;

   if (!_consume_reserved_zero (ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_utf8 (&op_delete->full_collection_name, &op_delete->full_collection_name_len, ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_delete->flags, ptr, remaining_bytes)) {
      return false;
   }

   // Bits 1-31 are reserved. Must be set to 0.
   if ((op_delete->flags & ~(0x00000001)) != 0) {
      *ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                                // flags as invalid input.
      return false;
   }

   op_delete->selector = *ptr;

   int32_t num_parsed = 0;
   if (!_consume_bson_objects (ptr, remaining_bytes, &num_parsed, 1) || num_parsed != 1) {
      return false;
   }

   return true;
}


static bool
_consume_op_kill_cursors (mcd_rpc_message *rpc, const uint8_t **ptr, size_t *remaining_bytes)
{
   BSON_ASSERT_PARAM (rpc);
   BSON_ASSERT_PARAM (ptr);
   BSON_ASSERT_PARAM (remaining_bytes);

   mcd_rpc_op_kill_cursors *op_kill_cursors = &rpc->op_kill_cursors;

   if (!_consume_reserved_zero (ptr, remaining_bytes)) {
      return false;
   }

   if (!_consume_int32_t (&op_kill_cursors->number_of_cursor_ids, ptr, remaining_bytes)) {
      return false;
   }

   if (op_kill_cursors->number_of_cursor_ids < 0 ||
       // Truncation may (deliberately) leave unparsed bytes that will later
       // trigger validation failure due to unexpected remaining bytes.
       bson_cmp_greater_su (op_kill_cursors->number_of_cursor_ids, *remaining_bytes / sizeof (int64_t))) {
      *ptr -= sizeof (int32_t); // Revert so *data_len points to start of
                                // numberOfCursorIds as invalid input.
      return false;
   }

   const size_t cursor_ids_length = (size_t) op_kill_cursors->number_of_cursor_ids * sizeof (int64_t);

   bson_free (op_kill_cursors->cursor_ids);

   if (op_kill_cursors->number_of_cursor_ids > 0) {
      op_kill_cursors->cursor_ids = bson_malloc (cursor_ids_length);
      for (int32_t i = 0; i < op_kill_cursors->number_of_cursor_ids; ++i) {
         if (!_consume_int64_t (op_kill_cursors->cursor_ids + i, ptr, remaining_bytes)) {
            return false;
         }
      }
   } else {
      op_kill_cursors->cursor_ids = NULL;
   }

   return true;
}


mcd_rpc_message *
mcd_rpc_message_from_data (const void *data, size_t length, const void **data_end)
{
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT (data_end || true);

   mcd_rpc_message *rpc = bson_malloc (sizeof (mcd_rpc_message));
   mcd_rpc_message *ret = NULL;

   *rpc = (mcd_rpc_message){.msg_header = {0}};

   if (!mcd_rpc_message_from_data_in_place (rpc, data, length, data_end)) {
      goto fail;
   }

   ret = rpc;
   rpc = NULL;

fail:
   mcd_rpc_message_destroy (rpc);
   return ret;
}

bool
mcd_rpc_message_from_data_in_place (mcd_rpc_message *rpc, const void *data, size_t length, const void **data_end)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT_PARAM (data);
   BSON_ASSERT (data_end || true);

   bool ret = false;

   size_t remaining_bytes = length;

   const uint8_t *ptr = data;

   if (!_consume_int32_t (&rpc->msg_header.message_length, &ptr, &remaining_bytes)) {
      goto fail;
   }

   if (rpc->msg_header.message_length < MONGOC_RPC_MINIMUM_MESSAGE_LENGTH ||
       bson_cmp_greater_su (rpc->msg_header.message_length, remaining_bytes + sizeof (int32_t))) {
      ptr -= sizeof (int32_t); // Revert so *data_end points to start of
                               // messageLength as invalid input.
      goto fail;
   }

   // Use reported message length as upper bound.
   remaining_bytes = (size_t) rpc->msg_header.message_length - sizeof (int32_t);

   if (!_consume_int32_t (&rpc->msg_header.request_id, &ptr, &remaining_bytes)) {
      goto fail;
   }

   if (!_consume_int32_t (&rpc->msg_header.response_to, &ptr, &remaining_bytes)) {
      goto fail;
   }

   if (!_consume_int32_t (&rpc->msg_header.op_code, &ptr, &remaining_bytes)) {
      goto fail;
   }

   switch (rpc->msg_header.op_code) {
   case MONGOC_OP_CODE_COMPRESSED:
      if (!_consume_op_compressed (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_MSG:
      if (!_consume_op_msg (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_REPLY:
      if (!_consume_op_reply (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_UPDATE:
      if (!_consume_op_update (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_INSERT:
      if (!_consume_op_insert (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_QUERY:
      if (!_consume_op_query (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_GET_MORE:
      if (!_consume_op_get_more (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_DELETE:
      if (!_consume_op_delete (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_KILL_CURSORS:
      if (!_consume_op_kill_cursors (rpc, &ptr, &remaining_bytes)) {
         goto fail;
      }
      break;

   default:
      ptr -= sizeof (int32_t); // Revert so *data_end points to start of opCode
                               // as invalid input.
      goto fail;
   }

   // Number of bytes parsed do not match the reported message length.
   if (remaining_bytes > 0) {
      goto fail;
   }

   ret = true;

fail:
   if (data_end) {
      *data_end = ptr;
   }

   return ret;
}

static void
_append_iovec_reserve_space_for (mongoc_iovec_t **iovecs,
                                 size_t *capacity,
                                 const mongoc_iovec_t *header_iovecs,
                                 size_t additional_capacity)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (header_iovecs);

   // Expect this function to be invoked only once after initializing the
   // `header_iovecs` array.
   BSON_ASSERT (*capacity == 4u);

   *capacity += additional_capacity;
   *iovecs = bson_malloc (*capacity * sizeof (mongoc_iovec_t));
   memcpy (*iovecs, header_iovecs, 4u * sizeof (mongoc_iovec_t));
}

static bool
_append_iovec (mongoc_iovec_t *iovecs, size_t *capacity, size_t *count, mongoc_iovec_t iovec)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);

   if (!iovec.iov_base || iovec.iov_len == 0u) {
      return false;
   }

   // Expect iovecs array capacity to have been reserved upfront according to
   // the upper bound of potential iovec objects required for the opcode being
   // converted. This is to minimize (re)allocations.
   BSON_ASSERT (*count < *capacity);

   iovecs[*count] = iovec;
   *count += 1u;

   return true;
}

#define MONGOC_RPC_APPEND_IOVEC(type, raw_type, to_le)                                                     \
   static bool _append_iovec_##type (mongoc_iovec_t *iovecs, size_t *capacity, size_t *count, type *value) \
   {                                                                                                       \
      raw_type storage;                                                                                    \
      memcpy (&storage, value, sizeof (raw_type));                                                         \
      storage = to_le (storage);                                                                           \
      memcpy (value, &storage, sizeof (raw_type));                                                         \
      return _append_iovec (iovecs,                                                                        \
                            capacity,                                                                      \
                            count,                                                                         \
                            (mongoc_iovec_t){                                                              \
                               .iov_base = (void *) value,                                                 \
                               .iov_len = sizeof (type),                                                   \
                            });                                                                            \
   }

MONGOC_RPC_APPEND_IOVEC (uint8_t, uint8_t, (uint8_t))
MONGOC_RPC_APPEND_IOVEC (int32_t, uint32_t, BSON_UINT32_TO_LE)
MONGOC_RPC_APPEND_IOVEC (uint32_t, uint32_t, BSON_UINT32_TO_LE)
MONGOC_RPC_APPEND_IOVEC (int64_t, uint64_t, BSON_UINT64_TO_LE)

static bool
_append_iovec_data (mongoc_iovec_t *iovecs, size_t *capacity, size_t *count, const void *data, size_t length)
{
   return _append_iovec (iovecs,
                         capacity,
                         count,
                         (mongoc_iovec_t){
                            .iov_base = (void *) data,
                            .iov_len = length,
                         });
}

static bool
_append_iovec_reserved_zero (mongoc_iovec_t *iovecs, size_t *capacity, size_t *count)
{
   static int32_t zero = 0u;

   return _append_iovec (iovecs,
                         capacity,
                         count,
                         (mongoc_iovec_t){
                            .iov_base = (void *) &zero,
                            .iov_len = sizeof (zero),
                         });
}

static bool
_append_iovec_op_compressed (mongoc_iovec_t **iovecs,
                             size_t *capacity,
                             size_t *count,
                             mcd_rpc_op_compressed *op_compressed,
                             const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_compressed);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 4u);

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_compressed->original_opcode)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_compressed->uncompressed_size)) {
      return false;
   }

   if (!_append_iovec_uint8_t (*iovecs, capacity, count, &op_compressed->compressor_id)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_compressed->compressed_message, op_compressed->compressed_message_len)) {
      return false;
   }

   return true;
}

static bool
_count_section_iovecs (const mcd_rpc_op_msg *op_msg, size_t *section_iovecs)
{
   BSON_ASSERT_PARAM (op_msg);
   BSON_ASSERT_PARAM (section_iovecs);

   for (size_t i = 0u; i < op_msg->sections_count; ++i) {
      *section_iovecs += 1u;

      switch (op_msg->sections[i].kind) {
      case 0: // Body.
         *section_iovecs += 1u;
         break;

      case 1: // Document Sequence.
         *section_iovecs += 3u;
         break;

      default:
         return false;
      }
   }

   return true;
}

static bool
_append_iovec_op_msg (mongoc_iovec_t **iovecs,
                      size_t *capacity,
                      size_t *count,
                      mcd_rpc_op_msg *op_msg,
                      const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_msg);
   BSON_ASSERT_PARAM (header_iovecs);

   size_t section_iovecs = 0u;
   if (!_count_section_iovecs (op_msg, &section_iovecs)) {
      return false;
   }

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 2u + section_iovecs);

   if (!_append_iovec_uint32_t (*iovecs, capacity, count, &op_msg->flag_bits)) {
      return false;
   }

   for (size_t i = 0u; i < op_msg->sections_count; ++i) {
      mcd_rpc_op_msg_section *section = &op_msg->sections[i];

      if (!section) {
         return false;
      }

      if (!_append_iovec_uint8_t (*iovecs, capacity, count, &section->kind)) {
         return false;
      }

      switch (section->kind) {
      case 0: // Body
         if (!_append_iovec_data (
                *iovecs, capacity, count, section->payload.body.bson, (size_t) section->payload.body.section_len)) {
            return false;
         }
         break;

      case 1: // Document Sequence
         if (!_append_iovec_int32_t (*iovecs, capacity, count, &section->payload.document_sequence.section_len)) {
            return false;
         }

         if (!_append_iovec_data (*iovecs,
                                  capacity,
                                  count,
                                  section->payload.document_sequence.identifier,
                                  section->payload.document_sequence.identifier_len)) {
            return false;
         }

         if (!_append_iovec_data (*iovecs,
                                  capacity,
                                  count,
                                  section->payload.document_sequence.bson_objects,
                                  section->payload.document_sequence.bson_objects_len)) {
            return false;
         }

         break;

      default:
         return false;
      }
   }

   if (op_msg->checksum_set) {
      if (!_append_iovec_uint32_t (*iovecs, capacity, count, &op_msg->checksum)) {
         return false;
      }
   }

   return true;
}

static bool
_append_iovec_op_reply (mongoc_iovec_t **iovecs,
                        size_t *capacity,
                        size_t *count,
                        mcd_rpc_op_reply *op_reply,
                        const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_reply);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 5u);

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_reply->response_flags)) {
      return false;
   }

   if (!_append_iovec_int64_t (*iovecs, capacity, count, &op_reply->cursor_id)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_reply->starting_from)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_reply->number_returned)) {
      return false;
   }

   if (op_reply->number_returned > 0 &&
       !_append_iovec_data (*iovecs, capacity, count, op_reply->documents, op_reply->documents_len)) {
      return false;
   }

   return true;
}

static bool
_append_iovec_op_update (mongoc_iovec_t **iovecs,
                         size_t *capacity,
                         size_t *count,
                         mcd_rpc_op_update *op_update,
                         const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_update);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 5u);

   if (!_append_iovec_reserved_zero (*iovecs, capacity, count)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_update->full_collection_name, op_update->full_collection_name_len)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_update->flags)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_update->selector, (size_t) _int32_from_le (op_update->selector))) {
      return false;
   }

   if (!_append_iovec_data (*iovecs, capacity, count, op_update->update, (size_t) _int32_from_le (op_update->update))) {
      return false;
   }

   return true;
}

static bool
_append_iovec_op_insert (mongoc_iovec_t **iovecs,
                         size_t *capacity,
                         size_t *count,
                         mcd_rpc_op_insert *op_insert,
                         const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_insert);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 3u);

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_insert->flags)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_insert->full_collection_name, op_insert->full_collection_name_len)) {
      return false;
   }

   if (!_append_iovec_data (*iovecs, capacity, count, op_insert->documents, op_insert->documents_len)) {
      return false;
   }


   return true;
}

static bool
_append_iovec_op_query (mongoc_iovec_t **iovecs,
                        size_t *capacity,
                        size_t *count,
                        mcd_rpc_op_query *op_query,
                        const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_query);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (
      iovecs, capacity, header_iovecs, 5u + (size_t) (!!op_query->return_fields_selector));

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_query->flags)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_query->full_collection_name, op_query->full_collection_name_len)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_query->number_to_skip)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_query->number_to_return)) {
      return false;
   }

   if (!_append_iovec_data (*iovecs, capacity, count, op_query->query, (size_t) _int32_from_le (op_query->query))) {
      return false;
   }

   if (op_query->return_fields_selector) {
      if (!_append_iovec_data (*iovecs,
                               capacity,
                               count,
                               op_query->return_fields_selector,
                               (size_t) _int32_from_le (op_query->return_fields_selector))) {
         return false;
      }
   }

   return true;
}

static bool
_append_iovec_op_get_more (mongoc_iovec_t **iovecs,
                           size_t *capacity,
                           size_t *count,
                           mcd_rpc_op_get_more *op_get_more,
                           const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (op_get_more);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 4u);

   if (!_append_iovec_reserved_zero (*iovecs, capacity, count)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_get_more->full_collection_name, op_get_more->full_collection_name_len)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_get_more->number_to_return)) {
      return false;
   }

   if (!_append_iovec_int64_t (*iovecs, capacity, count, &op_get_more->cursor_id)) {
      return false;
   }

   return true;
}

static bool
_append_iovec_op_delete (mongoc_iovec_t **iovecs,
                         size_t *capacity,
                         size_t *count,
                         mcd_rpc_op_delete *op_delete,
                         const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_delete);
   BSON_ASSERT_PARAM (header_iovecs);

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 4u);

   if (!_append_iovec_reserved_zero (*iovecs, capacity, count)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_delete->full_collection_name, op_delete->full_collection_name_len)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_delete->flags)) {
      return false;
   }

   if (!_append_iovec_data (
          *iovecs, capacity, count, op_delete->selector, (size_t) _int32_from_le (op_delete->selector))) {
      return false;
   }

   return true;
}

static bool
_append_iovec_op_kill_cursors (mongoc_iovec_t **iovecs,
                               size_t *capacity,
                               size_t *count,
                               mcd_rpc_op_kill_cursors *op_kill_cursors,
                               const mongoc_iovec_t *header_iovecs)
{
   BSON_ASSERT_PARAM (iovecs);
   BSON_ASSERT_PARAM (capacity);
   BSON_ASSERT_PARAM (count);
   BSON_ASSERT_PARAM (op_kill_cursors);
   BSON_ASSERT_PARAM (header_iovecs);

   // Store value before conversion to little endian.
   const int32_t number_of_cursor_ids = op_kill_cursors->number_of_cursor_ids;

   _append_iovec_reserve_space_for (iovecs, capacity, header_iovecs, 3u);

   if (!_append_iovec_reserved_zero (*iovecs, capacity, count)) {
      return false;
   }

   if (!_append_iovec_int32_t (*iovecs, capacity, count, &op_kill_cursors->number_of_cursor_ids)) {
      return false;
   }

   // Each cursorID must be converted to little endian.
   for (int32_t i = 0; i < number_of_cursor_ids; ++i) {
      int64_t *const cursor_id = op_kill_cursors->cursor_ids + i;
      uint64_t storage;
      memcpy (&storage, cursor_id, sizeof (int64_t));
      storage = BSON_UINT64_TO_LE (storage);
      memcpy (cursor_id, &storage, sizeof (int64_t));
   }

   if (number_of_cursor_ids > 0 &&
       !_append_iovec_data (
          *iovecs, capacity, count, op_kill_cursors->cursor_ids, (size_t) number_of_cursor_ids * sizeof (int64_t))) {
      return false;
   }

   return true;
}


void *
mcd_rpc_message_to_iovecs (mcd_rpc_message *rpc, size_t *count)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT_PARAM (count);

   const int32_t op_code = rpc->msg_header.op_code;

   // Preallocated space for msgHeader fields.
   mongoc_iovec_t header_iovecs[4];
   size_t capacity = 4u;
   *count = 0u;

   (void) _append_iovec_int32_t (header_iovecs, &capacity, count, &rpc->msg_header.message_length);
   (void) _append_iovec_int32_t (header_iovecs, &capacity, count, &rpc->msg_header.request_id);
   (void) _append_iovec_int32_t (header_iovecs, &capacity, count, &rpc->msg_header.response_to);
   (void) _append_iovec_int32_t (header_iovecs, &capacity, count, &rpc->msg_header.op_code);

   mongoc_iovec_t *iovecs = NULL;
   mongoc_iovec_t *ret = NULL;

   // Fields may be converted to little endian even on failure, so consider the
   // RPC object to be in an iovecs state from this point forward regardless of
   // success or failure.
   rpc->msg_header.is_in_iovecs_state = true;

   switch (op_code) {
   case MONGOC_OP_CODE_COMPRESSED:
      if (!_append_iovec_op_compressed (&iovecs, &capacity, count, &rpc->op_compressed, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_MSG: {
      if (!_append_iovec_op_msg (&iovecs, &capacity, count, &rpc->op_msg, header_iovecs)) {
         goto fail;
      }
      break;
   }

   case MONGOC_OP_CODE_REPLY:
      if (!_append_iovec_op_reply (&iovecs, &capacity, count, &rpc->op_reply, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_UPDATE:
      if (!_append_iovec_op_update (&iovecs, &capacity, count, &rpc->op_update, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_INSERT:
      if (!_append_iovec_op_insert (&iovecs, &capacity, count, &rpc->op_insert, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_QUERY:
      if (!_append_iovec_op_query (&iovecs, &capacity, count, &rpc->op_query, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_GET_MORE:
      if (!_append_iovec_op_get_more (&iovecs, &capacity, count, &rpc->op_get_more, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_DELETE:
      if (!_append_iovec_op_delete (&iovecs, &capacity, count, &rpc->op_delete, header_iovecs)) {
         goto fail;
      }
      break;

   case MONGOC_OP_CODE_KILL_CURSORS:
      if (!_append_iovec_op_kill_cursors (&iovecs, &capacity, count, &rpc->op_kill_cursors, header_iovecs)) {
         goto fail;
      }
      break;

   default:
      goto fail;
   }

   ret = iovecs;
   iovecs = NULL;

fail:
   bson_free (iovecs);

   return ret;
}

mcd_rpc_message *
mcd_rpc_message_new (void)
{
   mcd_rpc_message *const rpc = bson_malloc (sizeof (mcd_rpc_message));
   *rpc = (mcd_rpc_message){.msg_header = {0}};
   return rpc;
}

static int32_t
_mcd_rpc_header_get_op_code_maybe_le (const mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (rpc);

   int32_t op_code = rpc->msg_header.op_code;

   // May already be in native endian.
   switch (op_code) {
   case MONGOC_OP_CODE_COMPRESSED:
   case MONGOC_OP_CODE_MSG:
   case MONGOC_OP_CODE_REPLY:
   case MONGOC_OP_CODE_UPDATE:
   case MONGOC_OP_CODE_INSERT:
   case MONGOC_OP_CODE_QUERY:
   case MONGOC_OP_CODE_GET_MORE:
   case MONGOC_OP_CODE_DELETE:
   case MONGOC_OP_CODE_KILL_CURSORS:
      return op_code;

   default:
      // May be in little endian.
      op_code = _int32_from_le (&op_code);

      switch (op_code) {
      case MONGOC_OP_CODE_COMPRESSED:
      case MONGOC_OP_CODE_MSG:
      case MONGOC_OP_CODE_REPLY:
      case MONGOC_OP_CODE_UPDATE:
      case MONGOC_OP_CODE_INSERT:
      case MONGOC_OP_CODE_QUERY:
      case MONGOC_OP_CODE_GET_MORE:
      case MONGOC_OP_CODE_DELETE:
      case MONGOC_OP_CODE_KILL_CURSORS:
         return op_code;

      default:
         // Doesn't seem to have been a valid opCode.
         return rpc->msg_header.op_code;
      }
   }
}

static void
_mcd_rpc_message_free_owners (mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (rpc);

   switch (_mcd_rpc_header_get_op_code_maybe_le (rpc)) {
   case MONGOC_OP_CODE_MSG:
      bson_free (rpc->op_msg.sections);
      rpc->op_msg.sections = NULL;
      return;

   case MONGOC_OP_CODE_KILL_CURSORS:
      bson_free (rpc->op_kill_cursors.cursor_ids);
      rpc->op_kill_cursors.cursor_ids = NULL;
      return;

   case MONGOC_OP_CODE_COMPRESSED:
   case MONGOC_OP_CODE_REPLY:
   case MONGOC_OP_CODE_UPDATE:
   case MONGOC_OP_CODE_INSERT:
   case MONGOC_OP_CODE_QUERY:
   case MONGOC_OP_CODE_GET_MORE:
   case MONGOC_OP_CODE_DELETE:
      return;

   default:
      return;
   }
}

void
mcd_rpc_message_destroy (mcd_rpc_message *rpc)
{
   if (!rpc) {
      return;
   }

   _mcd_rpc_message_free_owners (rpc);

   bson_free ((void *) rpc);
}

void
mcd_rpc_message_reset (mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (rpc);

   _mcd_rpc_message_free_owners (rpc);

   *rpc = (mcd_rpc_message){.msg_header = {0}};
}

void
mcd_rpc_message_set_length (mcd_rpc_message *rpc, int32_t value)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->msg_header.message_length = value;
}

int32_t
mcd_rpc_header_get_message_length (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   return rpc->msg_header.message_length;
}

int32_t
mcd_rpc_header_get_request_id (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   return rpc->msg_header.request_id;
}

int32_t
mcd_rpc_header_get_response_to (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   return rpc->msg_header.response_to;
}

int32_t
mcd_rpc_header_get_op_code (const mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (rpc); // Permit read access even if the RPC message object
                            // is in an iovecs state.
   return rpc->msg_header.op_code;
}

int32_t
mcd_rpc_header_set_message_length (mcd_rpc_message *rpc, int32_t message_length)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->msg_header.message_length = message_length;
   return sizeof (message_length);
}

int32_t
mcd_rpc_header_set_request_id (mcd_rpc_message *rpc, int32_t request_id)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->msg_header.request_id = request_id;
   return sizeof (request_id);
}

int32_t
mcd_rpc_header_set_response_to (mcd_rpc_message *rpc, int32_t response_to)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->msg_header.response_to = response_to;
   return sizeof (response_to);
}

int32_t
mcd_rpc_header_set_op_code (mcd_rpc_message *rpc, int32_t op_code)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;

   _mcd_rpc_message_free_owners (rpc);

   rpc->msg_header.op_code = op_code;
   return sizeof (op_code);
}


int32_t
mcd_rpc_op_compressed_get_original_opcode (const mcd_rpc_message *rpc)
{
   BSON_ASSERT_PARAM (rpc); // Permit read access even if the RPC message object
                            // is in an iovecs state.
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   return rpc->op_compressed.original_opcode;
}

int32_t
mcd_rpc_op_compressed_get_uncompressed_size (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   return rpc->op_compressed.uncompressed_size;
}

uint8_t
mcd_rpc_op_compressed_get_compressor_id (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   return rpc->op_compressed.compressor_id;
}

const void *
mcd_rpc_op_compressed_get_compressed_message (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   return rpc->op_compressed.compressed_message;
}

size_t
mcd_rpc_op_compressed_get_compressed_message_length (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   return rpc->op_compressed.compressed_message_len;
}

int32_t
mcd_rpc_op_compressed_set_original_opcode (mcd_rpc_message *rpc, int32_t original_opcode)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   rpc->op_compressed.original_opcode = original_opcode;
   return sizeof (original_opcode);
}

int32_t
mcd_rpc_op_compressed_set_uncompressed_size (mcd_rpc_message *rpc, int32_t uncompressed_size)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   rpc->op_compressed.uncompressed_size = uncompressed_size;
   return sizeof (uncompressed_size);
}

int32_t
mcd_rpc_op_compressed_set_compressor_id (mcd_rpc_message *rpc, uint8_t compressor_id)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   rpc->op_compressed.compressor_id = compressor_id;
   return sizeof (compressor_id);
}

int32_t
mcd_rpc_op_compressed_set_compressed_message (mcd_rpc_message *rpc,
                                              const void *compressed_message,
                                              size_t compressed_message_length)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_COMPRESSED);
   BSON_ASSERT (bson_in_range_unsigned (int32_t, compressed_message_length));
   rpc->op_compressed.compressed_message = compressed_message;
   rpc->op_compressed.compressed_message_len = compressed_message_length;
   return (int32_t) compressed_message_length;
}


uint8_t
mcd_rpc_op_msg_section_get_kind (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   return rpc->op_msg.sections[index].kind;
}

int32_t
mcd_rpc_op_msg_section_get_length (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);

   const mcd_rpc_op_msg_section *const section = &rpc->op_msg.sections[index];

   switch (section->kind) {
   case 0: { // Body
      return _int32_from_le (section->payload.body.bson);
   }

   case 1: { // Document Sequence
      return section->payload.document_sequence.section_len;
   }

   default:
      BSON_UNREACHABLE ("invalid section kind");
   }
}

const char *
mcd_rpc_op_msg_section_get_identifier (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);

   const mcd_rpc_op_msg_section *const section = &rpc->op_msg.sections[index];
   BSON_ASSERT (section->kind == 1);
   return section->payload.document_sequence.identifier;
}

const void *
mcd_rpc_op_msg_section_get_body (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);

   const mcd_rpc_op_msg_section *const section = &rpc->op_msg.sections[index];
   BSON_ASSERT (section->kind == 0);
   return section->payload.body.bson;
}

const void *
mcd_rpc_op_msg_section_get_document_sequence (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);

   const mcd_rpc_op_msg_section *const section = &rpc->op_msg.sections[index];
   BSON_ASSERT (section->kind == 1);
   return section->payload.document_sequence.bson_objects;
}

size_t
mcd_rpc_op_msg_section_get_document_sequence_length (const mcd_rpc_message *rpc, size_t index)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);

   const mcd_rpc_op_msg_section *const section = &rpc->op_msg.sections[index];
   BSON_ASSERT (section->kind == 1);
   return section->payload.document_sequence.bson_objects_len;
}

int32_t
mcd_rpc_op_msg_section_set_kind (mcd_rpc_message *rpc, size_t index, uint8_t kind)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   rpc->op_msg.sections[index].kind = kind;
   return sizeof (kind);
}

int32_t
mcd_rpc_op_msg_section_set_length (mcd_rpc_message *rpc, size_t index, int32_t length)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   BSON_ASSERT (rpc->op_msg.sections[index].kind == 1);
   rpc->op_msg.sections[index].payload.document_sequence.section_len = length;
   return sizeof (length);
}

int32_t
mcd_rpc_op_msg_section_set_identifier (mcd_rpc_message *rpc, size_t index, const char *identifier)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   BSON_ASSERT (rpc->op_msg.sections[index].kind == 1);

   const size_t identifier_len = identifier ? strlen (identifier) + 1u : 0u;

   rpc->op_msg.sections[index].payload.document_sequence.identifier = identifier;
   rpc->op_msg.sections[index].payload.document_sequence.identifier_len = identifier_len;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, identifier_len));
   return (int32_t) identifier_len;
}

int32_t
mcd_rpc_op_msg_section_set_body (mcd_rpc_message *rpc, size_t index, const void *body)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   BSON_ASSERT (rpc->op_msg.sections[index].kind == 0);

   const int32_t section_len = body ? _int32_from_le (body) : 0;

   rpc->op_msg.sections[index].payload.body.bson = body;
   rpc->op_msg.sections[index].payload.body.section_len = section_len;

   return section_len;
}

int32_t
mcd_rpc_op_msg_section_set_document_sequence (mcd_rpc_message *rpc,
                                              size_t index,
                                              const void *document_sequence,
                                              size_t document_sequence_length)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   BSON_ASSERT (index < rpc->op_msg.sections_count);
   BSON_ASSERT (rpc->op_msg.sections[index].kind == 1);

   const size_t bson_objects_len = document_sequence ? document_sequence_length : 0u;

   rpc->op_msg.sections[index].payload.document_sequence.bson_objects = document_sequence;
   rpc->op_msg.sections[index].payload.document_sequence.bson_objects_len = bson_objects_len;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, document_sequence_length));
   return (int32_t) bson_objects_len;
}


uint32_t
mcd_rpc_op_msg_get_flag_bits (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   return rpc->op_msg.flag_bits;
}

size_t
mcd_rpc_op_msg_get_sections_count (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   return rpc->op_msg.sections_count;
}

const uint32_t *
mcd_rpc_op_msg_get_checksum (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   return rpc->op_msg.checksum_set ? &rpc->op_msg.checksum : NULL;
}

int32_t
mcd_rpc_op_msg_set_flag_bits (mcd_rpc_message *rpc, uint32_t flag_bits)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   rpc->op_msg.flag_bits = flag_bits;
   return sizeof (flag_bits);
}

void
mcd_rpc_op_msg_set_sections_count (mcd_rpc_message *rpc, size_t section_count)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);

   rpc->op_msg.sections = bson_realloc (rpc->op_msg.sections, section_count * sizeof (mcd_rpc_op_msg_section));
   rpc->op_msg.sections_count = section_count;
}

int32_t
mcd_rpc_op_msg_set_checksum (mcd_rpc_message *rpc, uint32_t checksum)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_MSG);
   rpc->op_msg.checksum = checksum;
   rpc->op_msg.checksum_set = true;
   return sizeof (checksum);
}


int32_t
mcd_rpc_op_reply_get_response_flags (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.response_flags;
}

int64_t
mcd_rpc_op_reply_get_cursor_id (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.cursor_id;
}

int32_t
mcd_rpc_op_reply_get_starting_from (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.starting_from;
}

int32_t
mcd_rpc_op_reply_get_number_returned (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.number_returned;
}

const void *
mcd_rpc_op_reply_get_documents (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.documents_len > 0 ? rpc->op_reply.documents : NULL;
}

size_t
mcd_rpc_op_reply_get_documents_len (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_REPLY);
   return rpc->op_reply.documents_len;
}

int32_t
mcd_rpc_op_reply_set_response_flags (mcd_rpc_message *rpc, int32_t response_flags)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_reply.response_flags = response_flags;
   return sizeof (response_flags);
}

int32_t
mcd_rpc_op_reply_set_cursor_id (mcd_rpc_message *rpc, int64_t cursor_id)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_reply.cursor_id = cursor_id;
   return sizeof (cursor_id);
}

int32_t
mcd_rpc_op_reply_set_starting_from (mcd_rpc_message *rpc, int32_t starting_from)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_reply.starting_from = starting_from;
   return sizeof (starting_from);
}

int32_t
mcd_rpc_op_reply_set_number_returned (mcd_rpc_message *rpc, int32_t number_returned)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_reply.number_returned = number_returned;
   return sizeof (number_returned);
}

int32_t
mcd_rpc_op_reply_set_documents (mcd_rpc_message *rpc, const void *documents, size_t documents_len)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;

   rpc->op_reply.documents = documents;
   rpc->op_reply.documents_len = documents_len;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, documents_len));
   return (int32_t) documents_len;
}


const char *
mcd_rpc_op_update_get_full_collection_name (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_UPDATE);
   return rpc->op_update.full_collection_name;
}

int32_t
mcd_rpc_op_update_get_flags (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_UPDATE);
   return rpc->op_update.flags;
}

const void *
mcd_rpc_op_update_get_selector (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_UPDATE);
   return rpc->op_update.selector;
}

const void *
mcd_rpc_op_update_get_update (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_UPDATE);
   return rpc->op_update.update;
}

int32_t
mcd_rpc_op_update_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;

   const size_t length = full_collection_name ? strlen (full_collection_name) + 1u : 0u;

   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_UPDATE);
   rpc->op_update.full_collection_name = full_collection_name;
   rpc->op_update.full_collection_name_len = length;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, length));
   return (int32_t) length;
}

int32_t
mcd_rpc_op_update_set_flags (mcd_rpc_message *rpc, int32_t flags)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_update.flags = flags;
   return sizeof (flags);
}

int32_t
mcd_rpc_op_update_set_selector (mcd_rpc_message *rpc, const void *selector)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_update.selector = selector;
   return selector ? _int32_from_le (selector) : 0;
}

int32_t
mcd_rpc_op_update_set_update (mcd_rpc_message *rpc, const void *update)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   rpc->op_update.update = update;
   return update ? _int32_from_le (update) : 0;
}


int32_t
mcd_rpc_op_insert_get_flags (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);
   return rpc->op_insert.flags;
}

const char *
mcd_rpc_op_insert_get_full_collection_name (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);
   return rpc->op_insert.full_collection_name;
}

const void *
mcd_rpc_op_insert_get_documents (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);
   return rpc->op_insert.documents;
}

size_t
mcd_rpc_op_insert_get_documents_len (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);
   return rpc->op_insert.documents_len;
}

int32_t
mcd_rpc_op_insert_set_flags (mcd_rpc_message *rpc, int32_t flags)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);
   rpc->op_insert.flags = flags;
   return sizeof (flags);
}

int32_t
mcd_rpc_op_insert_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);

   const size_t length = full_collection_name ? strlen (full_collection_name) + 1u : 0u;

   rpc->op_insert.full_collection_name = full_collection_name;
   rpc->op_insert.full_collection_name_len = length;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, length));
   return (int32_t) length;
}

int32_t
mcd_rpc_op_insert_set_documents (mcd_rpc_message *rpc, const void *documents, size_t documents_len)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_INSERT);

   rpc->op_insert.documents = documents;
   rpc->op_insert.documents_len = documents_len;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, documents_len));
   return (int32_t) documents_len;
}


int32_t
mcd_rpc_op_query_get_flags (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.flags;
}

const char *
mcd_rpc_op_query_get_full_collection_name (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.full_collection_name;
}

int32_t
mcd_rpc_op_query_get_number_to_skip (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.number_to_skip;
}

int32_t
mcd_rpc_op_query_get_number_to_return (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.number_to_return;
}

const void *
mcd_rpc_op_query_get_query (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.query;
}

const void *
mcd_rpc_op_query_get_return_fields_selector (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   return rpc->op_query.return_fields_selector;
}

int32_t
mcd_rpc_op_query_set_flags (mcd_rpc_message *rpc, int32_t flags)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   rpc->op_query.flags = flags;
   return sizeof (flags);
}

int32_t
mcd_rpc_op_query_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);

   const size_t length = full_collection_name ? strlen (full_collection_name) + 1u : 0u;

   rpc->op_query.full_collection_name = full_collection_name;
   rpc->op_query.full_collection_name_len = length;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, length));
   return (int32_t) length;
}

int32_t
mcd_rpc_op_query_set_number_to_skip (mcd_rpc_message *rpc, int32_t number_to_skip)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   rpc->op_query.number_to_skip = number_to_skip;
   return sizeof (number_to_skip);
}

int32_t
mcd_rpc_op_query_set_number_to_return (mcd_rpc_message *rpc, int32_t number_to_return)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   rpc->op_query.number_to_return = number_to_return;
   return sizeof (number_to_return);
}

int32_t
mcd_rpc_op_query_set_query (mcd_rpc_message *rpc, const void *query)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   rpc->op_query.query = query;
   return _int32_from_le (query);
}

int32_t
mcd_rpc_op_query_set_return_fields_selector (mcd_rpc_message *rpc, const void *return_fields_selector)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_QUERY);
   rpc->op_query.return_fields_selector = return_fields_selector;
   return return_fields_selector ? _int32_from_le (return_fields_selector) : 0;
}


const char *
mcd_rpc_op_get_more_get_full_collection_name (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);
   return rpc->op_get_more.full_collection_name;
}

int32_t
mcd_rpc_op_get_more_get_number_to_return (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);
   return rpc->op_get_more.number_to_return;
}

int64_t
mcd_rpc_op_get_more_get_cursor_id (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);
   return rpc->op_get_more.cursor_id;
}

int32_t
mcd_rpc_op_get_more_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);

   const size_t length = full_collection_name ? strlen (full_collection_name) + 1u : 0u;

   rpc->op_get_more.full_collection_name = full_collection_name;
   rpc->op_get_more.full_collection_name_len = length;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, length));
   return (int32_t) length;
}

int32_t
mcd_rpc_op_get_more_set_number_to_return (mcd_rpc_message *rpc, int32_t number_to_return)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);
   rpc->op_get_more.number_to_return = number_to_return;
   return sizeof (number_to_return);
}

int32_t
mcd_rpc_op_get_more_set_cursor_id (mcd_rpc_message *rpc, int64_t cursor_id)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_GET_MORE);
   rpc->op_get_more.cursor_id = cursor_id;
   return sizeof (cursor_id);
}


const char *
mcd_rpc_op_delete_get_full_collection_name (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);
   return rpc->op_delete.full_collection_name;
}

int32_t
mcd_rpc_op_delete_get_flags (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   return rpc->op_delete.flags;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);
}

const void *
mcd_rpc_op_delete_get_selector (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);
   return rpc->op_delete.selector;
}

int32_t
mcd_rpc_op_delete_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);

   const size_t length = full_collection_name ? strlen (full_collection_name) + 1u : 0u;

   rpc->op_delete.full_collection_name = full_collection_name;
   rpc->op_delete.full_collection_name_len = length;

   BSON_ASSERT (bson_in_range_unsigned (int32_t, length));
   return (int32_t) length;
}

int32_t
mcd_rpc_op_delete_set_flags (mcd_rpc_message *rpc, int32_t flags)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);
   rpc->op_delete.flags = flags;
   return sizeof (flags);
}

int32_t
mcd_rpc_op_delete_set_selector (mcd_rpc_message *rpc, const void *selector)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_DELETE);
   rpc->op_delete.selector = selector;
   return selector ? _int32_from_le (selector) : 0;
}


int32_t
mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_KILL_CURSORS);
   return rpc->op_kill_cursors.number_of_cursor_ids;
}

const int64_t *
mcd_rpc_op_kill_cursors_get_cursor_ids (const mcd_rpc_message *rpc)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_KILL_CURSORS);
   return rpc->op_kill_cursors.number_of_cursor_ids > 0 ? rpc->op_kill_cursors.cursor_ids : NULL;
}

int32_t
mcd_rpc_op_kill_cursors_set_cursor_ids (mcd_rpc_message *rpc, const int64_t *cursor_ids, int32_t number_of_cursor_ids)
{
   ASSERT_MCD_RPC_ACCESSOR_PRECONDITIONS;
   BSON_ASSERT (rpc->msg_header.op_code == MONGOC_OP_CODE_KILL_CURSORS);
   BSON_ASSERT (bson_cmp_less_su (number_of_cursor_ids, (size_t) INT32_MAX / sizeof (int64_t)));

   const size_t cursor_ids_length = (size_t) number_of_cursor_ids * sizeof (int64_t);

   rpc->op_kill_cursors.number_of_cursor_ids = number_of_cursor_ids;

   bson_free (rpc->op_kill_cursors.cursor_ids);

   if (number_of_cursor_ids > 0) {
      rpc->op_kill_cursors.cursor_ids = bson_malloc (cursor_ids_length);
      memcpy (rpc->op_kill_cursors.cursor_ids, cursor_ids, cursor_ids_length);
   } else {
      rpc->op_kill_cursors.cursor_ids = NULL;
   }

   return (int32_t) sizeof (int32_t) + (int32_t) cursor_ids_length;
}
