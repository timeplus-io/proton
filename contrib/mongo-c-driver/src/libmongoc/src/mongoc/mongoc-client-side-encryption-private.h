/*
 * Copyright 2019-present MongoDB, Inc.
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

#include "mongoc-prelude.h"

#ifndef MONGOC_CLIENT_SIDE_ENCRYPTION_PRIVATE_H
#define MONGOC_CLIENT_SIDE_ENCRYPTION_PRIVATE_H

#include "mongoc-client.h"
#include "mongoc-client-pool.h"
#include "mongoc-client-side-encryption.h"
#include "mongoc-cmd-private.h"
#include "mongoc-topology-private.h"
#include "bson/bson.h"

/* cse is an abbreviation for "Client Side Encryption" */

bool
_mongoc_cse_auto_encrypt (mongoc_client_t *client,
                          const mongoc_cmd_t *cmd,
                          mongoc_cmd_t *encrypted_cmd,
                          bson_t *encrypted,
                          bson_error_t *error);

bool
_mongoc_cse_auto_decrypt (
   mongoc_client_t *client, const char *db_name, const bson_t *reply, bson_t *decrypted, bson_error_t *error);

bool
_mongoc_cse_client_enable_auto_encryption (mongoc_client_t *client,
                                           mongoc_auto_encryption_opts_t *opts /* may be NULL */,
                                           bson_error_t *error);

bool
_mongoc_cse_client_pool_enable_auto_encryption (mongoc_topology_t *topology,
                                                mongoc_auto_encryption_opts_t *opts /* may be NULL */,
                                                bson_error_t *error);

/* If this returns true, client side encryption is enabled
 * on the client (or it's parent client pool), and cannot
 * be disabled. This check is done while holding the
 * topology lock. So if this returns true, callers are
 * guaranteed that CSE remains enabled afterwards. */
bool
_mongoc_cse_is_enabled (mongoc_client_t *client);

/**
 * @brief The context for the automatic creation of a datakey
 */
struct auto_datakey_context {
   /// The output destination for the new key ID. Never NULL.
   bson_value_t *out_keyid;
   /// An error output destination for the key generation. May be NULL.
   bson_error_t *out_error;
   /// The userdata pointer given to @ref
   /// _mongoc_encryptedFields_fill_auto_datakeys
   void *userdata;
};

/**
 * @brief The type of a datakey-creating callback.
 *
 * @param ctx The context of the keyId request. @sa auto_datakey_context
 * @retval true Upon success
 * @retval false Otherwise.
 *
 * @note Errors should be written into `ctx->out_error`.
 */
typedef bool (*auto_datakey_factory) (struct auto_datakey_context *ctx);

/**
 * @brief Process an array of encryptedFields.fields, automatically filling null
 * keyId elements by calling the given factory function.
 *
 * @param[out] out_fields The modified encryptedFields.fields. Must be destroyed
 * by the caller.
 * @param[in] in_fields The input encryptedFields.fields
 * @param factory A keyId factory. @see auto_datakey_factory
 * @param userdata The userdata pointer for `factory`
 * @param[out] error An error output parameter
 * @retval true On success
 * @retval false Otherwise
 */
bool
_mongoc_encryptedFields_fill_auto_datakeys (
   bson_t *out_fields, const bson_t *in_fields, auto_datakey_factory factory, void *userdata, bson_error_t *error);

#endif /* MONGOC_CLIENT_SIDE_ENCRYPTION_PRIVATE_H */
