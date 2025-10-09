/*
 * Copyright 2023 MongoDB, Inc.
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


#if !defined(MCD_RPC_H_INCLUDED)
#define MCD_RPC_H_INCLUDED

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


typedef union _mcd_rpc_message mcd_rpc_message;


// See: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol
#define MONGOC_OP_CODE_NONE INT32_C (0)
#define MONGOC_OP_CODE_COMPRESSED INT32_C (2012)
#define MONGOC_OP_CODE_MSG INT32_C (2013)

#define MONGOC_OP_COMPRESSED_COMPRESSOR_ID_NOOP UINT8_C (0)
#define MONGOC_OP_COMPRESSED_COMPRESSOR_ID_SNAPPY UINT8_C (1)
#define MONGOC_OP_COMPRESSED_COMPRESSOR_ID_ZLIB UINT8_C (2)
#define MONGOC_OP_COMPRESSED_COMPRESSOR_ID_ZSTD UINT8_C (3)

#define MONGOC_OP_MSG_FLAG_NONE UINT32_C (0)
#define MONGOC_OP_MSG_FLAG_CHECKSUM_PRESENT (UINT32_C (0x01) << 0)
#define MONGOC_OP_MSG_FLAG_MORE_TO_COME (UINT32_C (0x01) << 1)
#define MONGOC_OP_MSG_FLAG_EXHAUST_ALLOWED (UINT32_C (0x01) << 16)


// See: https://www.mongodb.com/docs/manual/legacy-opcodes/
#define MONGOC_OP_CODE_REPLY INT32_C (1)
#define MONGOC_OP_CODE_UPDATE INT32_C (2001)
#define MONGOC_OP_CODE_INSERT INT32_C (2002)
#define MONGOC_OP_CODE_QUERY INT32_C (2004)
#define MONGOC_OP_CODE_GET_MORE INT32_C (2005)
#define MONGOC_OP_CODE_DELETE INT32_C (2006)
#define MONGOC_OP_CODE_KILL_CURSORS INT32_C (2007)

#define MONGOC_OP_REPLY_RESPONSE_FLAG_NONE INT32_C (0)
#define MONGOC_OP_REPLY_RESPONSE_FLAG_CURSOR_NOT_FOUND (INT32_C (0x01) << 0)
#define MONGOC_OP_REPLY_RESPONSE_FLAG_QUERY_FAILURE (INT32_C (0x01) << 1)
#define MONGOC_OP_REPLY_RESPONSE_FLAG_SHARD_CONFIG_STALE (INT32_C (0x01) << 2)
#define MONGOC_OP_REPLY_RESPONSE_FLAG_AWAIT_CAPABLE (INT32_C (0x01) << 3)

#define MONGOC_OP_UPDATE_FLAG_NONE INT32_C (0)
#define MONGOC_OP_UPDATE_FLAG_UPSERT (INT32_C (0x01) << 0)
#define MONGOC_OP_UPDATE_FLAG_MULTI_UPDATE (INT32_C (0x01) << 1)

#define MONGOC_OP_INSERT_FLAG_NONE INT32_C (0)
#define MONGOC_OP_INSERT_FLAG_CONTINUE_ON_ERROR (INT32_C (0x01) << 0)

#define MONGOC_OP_QUERY_FLAG_NONE INT32_C (0)
#define MONGOC_OP_QUERY_FLAG_TAILABLE_CURSOR (INT32_C (0x01) << 1)
#define MONGOC_OP_QUERY_FLAG_SECONDARY_OK (INT32_C (0x01) << 2)
#define MONGOC_OP_QUERY_FLAG_OPLOG_REPLAY (INT32_C (0x01) << 3)
#define MONGOC_OP_QUERY_FLAG_NO_CURSOR_TIMEOUT (INT32_C (0x01) << 4)
#define MONGOC_OP_QUERY_FLAG_AWAIT_DATA (INT32_C (0x01) << 5)
#define MONGOC_OP_QUERY_FLAG_EXHAUST (INT32_C (0x01) << 6)
#define MONGOC_OP_QUERY_FLAG_PARTIAL (INT32_C (0x01) << 7)

#define MONGOC_OP_DELETE_FLAG_NONE INT32_C (0)
#define MONGOC_OP_DELETE_FLAG_SINGLE_REMOVE (INT32_C (0x01) << 0)


// Convert the given array of bytes into an RPC message object. The RPC message
// object must be freed by `mcd_rpc_message_destroy`.
//
// data: an array of `length` bytes.
// data_end: if not `NULL`, `*data_end` is set to one past the last byte of
//           valid input data. Useful for diagnosing failures.
//
// Note: the fields of the RPC message object are automatically converted from
// little endian to native endian.
//
// Returns the new RPC message object on success. Returns `NULL` on failure.
mcd_rpc_message *
mcd_rpc_message_from_data (const void *data, size_t length, const void **data_end);

// The in-place version of `mcd_rpc_message_from_data`.
//
// rpc: an RPC message object in an initialized state.
//
// Returns `true` on success. Returns `false` on failure.
bool
mcd_rpc_message_from_data_in_place (mcd_rpc_message *rpc, const void *data, size_t length, const void **data_end);

// Convert the given RPC message object into an array of iovec structures,
// putting the RPC message object in an iovecs state. The return value must be
// freed by `bson_free`.
//
// Unless otherwise specified, it is undefined behavior to access any RPC
// message field when the object is in an iovecs state. Use
// `mcd_rpc_message_reset` to return the object to an initialized state before
// further reuse.
//
// The data layout of the iovec structures is consistent with the definition of
// `mongoc_iovec_t` as defined in `<mongoc/mongoc-iovec.h>`.
//
// rpc: a valid RPC message object whose fields are in native endian.
// length: if not `NULL`, `*length` is set to the number of iovec structures in
//         the array.
//
// Returns the array of iovec structures on success. Returns `NULL` on failure.
void *
mcd_rpc_message_to_iovecs (mcd_rpc_message *rpc, size_t *count);

// Return an RPC message object in an initialized state whose fields will be set
// manually. The return value must be freed by `mcd_rpc_message_destroy`.
mcd_rpc_message *
mcd_rpc_message_new (void);

// Destroy the given RPC message object.
void
mcd_rpc_message_destroy (mcd_rpc_message *rpc);

// Restore the given RPC message object to an initialized state.
void
mcd_rpc_message_reset (mcd_rpc_message *rpc);

// Set the message length for the given RPC message object. Expected to be used
// in conjunction with the return values of setters.
void
mcd_rpc_message_set_length (mcd_rpc_message *rpc, int32_t value);

// Get the msgHeader.messageLength field.
int32_t
mcd_rpc_header_get_message_length (const mcd_rpc_message *rpc);

// Get the msgHeader.requestId field.
int32_t
mcd_rpc_header_get_request_id (const mcd_rpc_message *rpc);

// Get the msgHeader.responseTo field.
int32_t
mcd_rpc_header_get_response_to (const mcd_rpc_message *rpc);

// Get the msgHeader.opCode field.
//
// This function may be called even if the RPC message is in an iovecs state.
int32_t
mcd_rpc_header_get_op_code (const mcd_rpc_message *rpc);

// Set the msgHeader.messageLength field.
//
// Returns the length of the field as part of msgHeader.messageLength.
int32_t
mcd_rpc_header_set_message_length (mcd_rpc_message *rpc, int32_t message_length);

// Set the msgHeader.requestId field.
//
// Returns the length of the field as part of msgHeader.messageLength.
int32_t
mcd_rpc_header_set_request_id (mcd_rpc_message *rpc, int32_t request_id);

// Set the msgHeader.responseTo field.
//
// Returns the length of the field as part of msgHeader.messageLength.
int32_t
mcd_rpc_header_set_response_to (mcd_rpc_message *rpc, int32_t response_to);

// Set the msgHeader.opCode field.
//
// Note: the msgHeader.opCode field may be set more than once.
//
// Returns the length of the field as part of msgHeader.messageLength.
int32_t
mcd_rpc_header_set_op_code (mcd_rpc_message *rpc, int32_t op_code);


// Get the OP_COMPRESSED originalOpcode field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
//
// This function may be called even if the RPC message is in an iovecs state.
int32_t
mcd_rpc_op_compressed_get_original_opcode (const mcd_rpc_message *rpc);

// Get the OP_COMPRESSED uncompressedSize field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
int32_t
mcd_rpc_op_compressed_get_uncompressed_size (const mcd_rpc_message *rpc);

// Get the OP_COMPRESSED compressorId field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
uint8_t
mcd_rpc_op_compressed_get_compressor_id (const mcd_rpc_message *rpc);

// Get the OP_COMPRESSED compressedMessage field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
const void *
mcd_rpc_op_compressed_get_compressed_message (const mcd_rpc_message *rpc);

// Get the length of the OP_COMPRESSED compressedMessage field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
size_t
mcd_rpc_op_compressed_get_compressed_message_length (const mcd_rpc_message *rpc);

// Set the OP_COMPRESSED originalOpcode field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
int32_t
mcd_rpc_op_compressed_set_original_opcode (mcd_rpc_message *rpc, int32_t original_opcode);

// Set the OP_COMPRESSED uncompressedSize field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
int32_t
mcd_rpc_op_compressed_set_uncompressed_size (mcd_rpc_message *rpc, int32_t uncompressed_size);

// Set the OP_COMPRESSED compressorId field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
int32_t
mcd_rpc_op_compressed_set_compressor_id (mcd_rpc_message *rpc, uint8_t compressor_id);

// Set the OP_COMPRESSED compressedMessage field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_COMPRESSED.
int32_t
mcd_rpc_op_compressed_set_compressed_message (mcd_rpc_message *rpc,
                                              const void *compressed_message,
                                              size_t compressed_message_length);


// Get the kind byte for the OP_MSG section at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
uint8_t
mcd_rpc_op_msg_section_get_kind (const mcd_rpc_message *rpc, size_t index);

// Get the length of the OP_MSG section at the given index.
//
// If the section kind is 0, returns the length of the single BSON object.
// If the section kind is 1, returns the total length of the section.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
int32_t
mcd_rpc_op_msg_section_get_length (const mcd_rpc_message *rpc, size_t index);

// Get the document sequence identifier of the OP_MSG document sequence section
// at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
const char *
mcd_rpc_op_msg_section_get_identifier (const mcd_rpc_message *rpc, size_t index);

// Get a pointer to the beginning of the single BSON object of the OP_MSG body
// section at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 0.
const void *
mcd_rpc_op_msg_section_get_body (const mcd_rpc_message *rpc, size_t index);

// Get a pointer to the beginning of the document sequence of the OP_MSG
// document sequence section at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
const void *
mcd_rpc_op_msg_section_get_document_sequence (const mcd_rpc_message *rpc, size_t index);

// Get the length of the document sequence of the OP_MSG document sequence
// section at the given index.
//
// Note: the length is the number of bytes, NOT the number of documents.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
size_t
mcd_rpc_op_msg_section_get_document_sequence_length (const mcd_rpc_message *rpc, size_t index);

// Set the kind byte for the OP_MSG section at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
int32_t
mcd_rpc_op_msg_section_set_kind (mcd_rpc_message *rpc, size_t index, uint8_t kind);

// Set the length of the OP_MSG document sequence section at the given index.
//
// Note: the section length of an OP_MSG body section is equal to the length
// of the single BSON object, thus does not require a seperate setter.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
int32_t
mcd_rpc_op_msg_section_set_length (mcd_rpc_message *rpc, size_t index, int32_t length);

// Set the document sequence identifier of the OP_MSG document sequence section
// at the given index.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
int32_t
mcd_rpc_op_msg_section_set_identifier (mcd_rpc_message *rpc, size_t index, const char *identifier);

// Set the BSON object for the OP_MSG body section at the given index.
//
// Note: the section length of an OP_MSG body section is equal to the length
// of the single BSON object, thus does not require a seperate setter.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 0.
int32_t
mcd_rpc_op_msg_section_set_body (mcd_rpc_message *rpc, size_t index, const void *body);

// Set the document sequence for the OP_MSG document sequence section at the
// given index.
//
// `document_sequence_length` MUST equal the length in bytes of the document
// sequence.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
// The given index MUST be a valid index into the OP_MSG sections array.
// The section kind at the given index MUST equal 1.
int32_t
mcd_rpc_op_msg_section_set_document_sequence (mcd_rpc_message *rpc,
                                              size_t index,
                                              const void *document_sequence,
                                              size_t document_sequence_length);


// Get the OP_MSG flagBits field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
uint32_t
mcd_rpc_op_msg_get_flag_bits (const mcd_rpc_message *rpc);

// Get the number of sections in the OP_MSG sections array.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
size_t
mcd_rpc_op_msg_get_sections_count (const mcd_rpc_message *rpc);

// Get the OP_MSG checksum field.
//
// Returns `NULL` if the field is not set.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
const uint32_t *
mcd_rpc_op_msg_get_checksum (const mcd_rpc_message *rpc);

// Set the OP_MSG flagBits field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
int32_t
mcd_rpc_op_msg_set_flag_bits (mcd_rpc_message *rpc, uint32_t flag_bits);

// Set the number of sections in the OP_MSG section array.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
void
mcd_rpc_op_msg_set_sections_count (mcd_rpc_message *rpc, size_t section_count);

// Set the OP_MSG checksum field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_MSG.
int32_t
mcd_rpc_op_msg_set_checksum (mcd_rpc_message *rpc, uint32_t checksum);


// Get the OP_REPLY responseFlags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_get_response_flags (const mcd_rpc_message *rpc);

// Get the OP_REPLY cursorID field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int64_t
mcd_rpc_op_reply_get_cursor_id (const mcd_rpc_message *rpc);

// Get the OP_REPLY startingFrom field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_get_starting_from (const mcd_rpc_message *rpc);

// Get the OP_REPLY numberReturned field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_get_number_returned (const mcd_rpc_message *rpc);

// Get a pointer to the beginning of the OP_REPLY documents array.
//
// Returns `NULL` if the OP_REPLY numberReturned field equals 0.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
const void *
mcd_rpc_op_reply_get_documents (const mcd_rpc_message *rpc);

// Get the length of the OP_REPLY documents array.
//
// Note: the length is the number of bytes, NOT the number of documents.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
size_t
mcd_rpc_op_reply_get_documents_len (const mcd_rpc_message *rpc);

// Set the OP_REPLY responseFlags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_set_response_flags (mcd_rpc_message *rpc, int32_t response_flags);

// Set the OP_REPLY cursorID field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_set_cursor_id (mcd_rpc_message *rpc, int64_t cursor_id);

// Set the OP_REPLY startingFrom field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_set_starting_from (mcd_rpc_message *rpc, int32_t starting_from);

// Set the OP_REPLY numberReturned field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_set_number_returned (mcd_rpc_message *rpc, int32_t number_returned);

// Set the OP_REPLY documents field.
//
// `documents_len` MUST equal the length in bytes of the documents array.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_REPLY.
int32_t
mcd_rpc_op_reply_set_documents (mcd_rpc_message *rpc, const void *documents, size_t documents_len);


// Get the OP_UPDATE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
const char *
mcd_rpc_op_update_get_full_collection_name (const mcd_rpc_message *rpc);

// Get the OP_UPDATE flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
int32_t
mcd_rpc_op_update_get_flags (const mcd_rpc_message *rpc);

// Get the OP_UPDATE selector field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
const void *
mcd_rpc_op_update_get_selector (const mcd_rpc_message *rpc);

// Get the OP_UPDATE update field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
const void *
mcd_rpc_op_update_get_update (const mcd_rpc_message *rpc);

// Set the OP_UPDATE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
int32_t
mcd_rpc_op_update_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name);

// Set the OP_UPDATE flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
int32_t
mcd_rpc_op_update_set_flags (mcd_rpc_message *rpc, int32_t flags);

// Set the OP_UPDATE selector field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
int32_t
mcd_rpc_op_update_set_selector (mcd_rpc_message *rpc, const void *selector);

// Set the OP_UPDATE update field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_UPDATE.
int32_t
mcd_rpc_op_update_set_update (mcd_rpc_message *rpc, const void *update);


// Get the OP_INSERT flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
int32_t
mcd_rpc_op_insert_get_flags (const mcd_rpc_message *rpc);

// Get the OP_INSERT fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
const char *
mcd_rpc_op_insert_get_full_collection_name (const mcd_rpc_message *rpc);

// Get a pointer to the beginning of the OP_INSERT documents array.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
const void *
mcd_rpc_op_insert_get_documents (const mcd_rpc_message *rpc);

// Get the length of the OP_INSERT documents array.
//
// Note: the length is the number of bytes, NOT the number of documents.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
size_t
mcd_rpc_op_insert_get_documents_len (const mcd_rpc_message *rpc);

// Set the OP_INSERT flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
int32_t
mcd_rpc_op_insert_set_flags (mcd_rpc_message *rpc, int32_t flags);

// Set the OP_INSERT fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
int32_t
mcd_rpc_op_insert_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name);

// Set the OP_INSERT documents array.
//
// `documents_len` MUST equal the length in bytes of the documents array.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_INSERT.
int32_t
mcd_rpc_op_insert_set_documents (mcd_rpc_message *rpc, const void *documents, size_t documents_len);


// Get the OP_QUERY flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_get_flags (const mcd_rpc_message *rpc);

// Get the OP_QUERY fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
const char *
mcd_rpc_op_query_get_full_collection_name (const mcd_rpc_message *rpc);

// Get the OP_QUERY numberToSkip field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_get_number_to_skip (const mcd_rpc_message *rpc);

// Get the OP_QUERY numberToReturn field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_get_number_to_return (const mcd_rpc_message *rpc);

// Get the OP_QUERY query field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
const void *
mcd_rpc_op_query_get_query (const mcd_rpc_message *rpc);

// Get the OP_QUERY returnFieldsSelector field.
//
// Returns `NULL` if the field is not set.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
const void *
mcd_rpc_op_query_get_return_fields_selector (const mcd_rpc_message *rpc);

// Set the OP_QUERY flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_flags (mcd_rpc_message *rpc, int32_t flags);

// Set the OP_QUERY fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name);

// Set the OP_QUERY numberToSkip field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_number_to_skip (mcd_rpc_message *rpc, int32_t number_to_skip);

// Set the OP_QUERY numberToReturn field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_number_to_return (mcd_rpc_message *rpc, int32_t number_to_return);

// Set the OP_QUERY query field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_query (mcd_rpc_message *rpc, const void *query);

// Set the OP_QUERY returnFieldsSelector field.
//
// Note: `return_fields_selector` may be `NULL` to unset the field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_QUERY.
int32_t
mcd_rpc_op_query_set_return_fields_selector (mcd_rpc_message *rpc, const void *return_fields_selector);


// Get the OP_GET_MORE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
const char *
mcd_rpc_op_get_more_get_full_collection_name (const mcd_rpc_message *rpc);

// Get the OP_GET_MORE numberToReturn field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
int32_t
mcd_rpc_op_get_more_get_number_to_return (const mcd_rpc_message *rpc);

// Get the OP_GET_MORE cursorID field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
int64_t
mcd_rpc_op_get_more_get_cursor_id (const mcd_rpc_message *rpc);

// Set the OP_GET_MORE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
int32_t
mcd_rpc_op_get_more_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name);

// Set the OP_GET_MORE numberToReturn field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
int32_t
mcd_rpc_op_get_more_set_number_to_return (mcd_rpc_message *rpc, int32_t number_to_return);

// Set the OP_GET_MORE cursorID field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_GET_MORE.
int32_t
mcd_rpc_op_get_more_set_cursor_id (mcd_rpc_message *rpc, int64_t cursor_id);


// Get the OP_DELETE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
const char *
mcd_rpc_op_delete_get_full_collection_name (const mcd_rpc_message *rpc);

// Get the OP_DELETE flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
int32_t
mcd_rpc_op_delete_get_flags (const mcd_rpc_message *rpc);

// Get the OP_DELETE selector field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
const void *
mcd_rpc_op_delete_get_selector (const mcd_rpc_message *rpc);

// Set the OP_DELETE fullCollectionName field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
int32_t
mcd_rpc_op_delete_set_full_collection_name (mcd_rpc_message *rpc, const char *full_collection_name);

// Set the OP_DELETE flags field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
int32_t
mcd_rpc_op_delete_set_flags (mcd_rpc_message *rpc, int32_t flags);

// Set the OP_DELETE selector field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_DELETE.
int32_t
mcd_rpc_op_delete_set_selector (mcd_rpc_message *rpc, const void *selector);


// Get the OP_KILL_CURSORS numberOfCursorIDs field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_KILL_CURSORS.
int32_t
mcd_rpc_op_kill_cursors_get_number_of_cursor_ids (const mcd_rpc_message *rpc);

// Get the OP_KILL_CURSORS cursorIDs field.
//
// Returns `NULL` if the OP_KILL_CURSORS numberOfCursorIDs field equals 0.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_KILL_CURSORS.
const int64_t *
mcd_rpc_op_kill_cursors_get_cursor_ids (const mcd_rpc_message *rpc);

// Set the OP_KILL_CURSORS cursorIDs field.
//
// The msgHeader.opCode field MUST equal MONGOC_OP_CODE_KILL_CURSORS.
int32_t
mcd_rpc_op_kill_cursors_set_cursor_ids (mcd_rpc_message *rpc, const int64_t *cursor_ids, int32_t number_of_cursor_ids);


#ifdef __cplusplus
}
#endif

#endif // !defined(MCD_RPC_H_INCLUDED)
