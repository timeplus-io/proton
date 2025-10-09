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

#ifndef MONGOC_CRYPT_PRIVATE_H
#define MONGOC_CRYPT_PRIVATE_H

#include "mongoc-config.h"

#include "mongoc.h"

typedef struct mc_kms_credentials_callback {
   mongoc_kms_credentials_provider_callback_fn fn;
   void *userdata;
} mc_kms_credentials_callback;

#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION

/* For interacting with libmongocrypt */
typedef struct __mongoc_crypt_t _mongoc_crypt_t;

/*
Creates a new handle into libmongocrypt.
- schema_map may be NULL.
- may return NULL and set error.
*/
_mongoc_crypt_t *
_mongoc_crypt_new (const bson_t *kms_providers,
                   const bson_t *schema_map,
                   const bson_t *encrypted_fields_map,
                   const bson_t *tls_opts,
                   const char *crypt_shared_lib_path,
                   bool crypt_shared_lib_required,
                   bool bypass_auto_encryption,
                   bool bypass_query_analysis,
                   mc_kms_credentials_callback creds_cb,
                   bson_error_t *error);

void
_mongoc_crypt_destroy (_mongoc_crypt_t *crypt);

/*
Perform auto encryption.
- cmd_out is always initialized.
- may return false and set error.
*/
bool
_mongoc_crypt_auto_encrypt (_mongoc_crypt_t *crypt,
                            mongoc_collection_t *key_vault_coll,
                            mongoc_client_t *mongocryptd_client,
                            mongoc_client_t *collinfo_client,
                            const char *db_name,
                            const bson_t *cmd_in,
                            bson_t *cmd_out,
                            bson_error_t *error);

/*
Perform auto decryption.
- doc_out is always initialized.
- may return false and set error.
*/
bool
_mongoc_crypt_auto_decrypt (_mongoc_crypt_t *crypt,
                            mongoc_collection_t *key_vault_coll,
                            const bson_t *doc_in,
                            bson_t *doc_out,
                            bson_error_t *error);

/*
Perform explicit encryption.
Return false on error and sets `error`.
*/
bool
_mongoc_crypt_explicit_encrypt (_mongoc_crypt_t *crypt,
                                mongoc_collection_t *key_vault_coll,
                                const char *algorithm /* may be NULL */,
                                const bson_value_t *keyid /* may be NULL */,
                                const char *keyaltname /* may be NULL */,
                                const char *query_type /* may be NULL */,
                                const int64_t *contention_factor /* may be NULL */,
                                const bson_t *range_opts /* may be NULL */,
                                const bson_value_t *value_in,
                                bson_value_t *value_out,
                                bson_error_t *error);

/*
Perform explicit encryption on an expression.
Return false on error and sets `error`.
*/
bool
_mongoc_crypt_explicit_encrypt_expression (_mongoc_crypt_t *crypt,
                                           mongoc_collection_t *key_vault_coll,
                                           const char *algorithm /* may be NULL */,
                                           const bson_value_t *keyid /* may be NULL */,
                                           const char *keyaltname /* may be NULL */,
                                           const char *query_type /* may be NULL */,
                                           const int64_t *contention_factor /* may be NULL */,
                                           const bson_t *range_opts /* may be NULL */,
                                           const bson_t *expr_in,
                                           bson_t *expr_out,
                                           bson_error_t *error);

/*
Perform explicit decryption.
- value_out is always initialized.
- may return false and set error.
*/
bool
_mongoc_crypt_explicit_decrypt (_mongoc_crypt_t *crypt,
                                mongoc_collection_t *key_vault_coll,
                                const bson_value_t *value_in,
                                bson_value_t *value_out,
                                bson_error_t *error);
/*
Create a data key document (does not insert into key vault).
- keyaltnames may be NULL.
- doc_out is always initialized.
- may return false and set error.
*/
bool
_mongoc_crypt_create_datakey (_mongoc_crypt_t *crypt,
                              const char *kms_provider,
                              const bson_t *masterkey,
                              char **keyaltnames,
                              uint32_t keyaltnames_count,
                              const uint8_t *keymaterial,
                              uint32_t keymaterial_len,
                              bson_t *doc_out,
                              bson_error_t *error);

/*
Rewrap datakeys in keyvault_coll matching the given filter with a new KMS
provider (does not bulk-update into key vault).
- filter may be NULL (equivalent to an empty document).
- kms_provider may be NULL.
- masterkey may be NULL if kms_provider is NULL.
- doc_out is always initialized.
- may return false and set error.
*/
bool
_mongoc_crypt_rewrap_many_datakey (_mongoc_crypt_t *crypt,
                                   mongoc_collection_t *keyvault_coll,
                                   const bson_t *filter,
                                   const char *provider,
                                   const bson_t *master_key,
                                   bson_t *doc_out,
                                   bson_error_t *error);

const char *
_mongoc_crypt_get_crypt_shared_version (const _mongoc_crypt_t *crypt);

#endif /* MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION */
#endif /* MONGOC_CRYPT_PRIVATE_H */
