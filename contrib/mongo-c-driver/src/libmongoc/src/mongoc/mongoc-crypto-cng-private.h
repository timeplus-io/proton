/*
 * Copyright 2016 MongoDB, Inc.
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

#ifdef MONGOC_ENABLE_CRYPTO_CNG

#ifndef MONGOC_CRYPTO_CNG_PRIVATE_H
#define MONGOC_CRYPTO_CNG_PRIVATE_H


#include "mongoc-config.h"


BSON_BEGIN_DECLS

void
mongoc_crypto_cng_init (void);

void
mongoc_crypto_cng_cleanup (void);

void
mongoc_crypto_cng_hmac_sha1 (mongoc_crypto_t *crypto,
                             const void *key,
                             int key_len,
                             const unsigned char *data,
                             int data_len,
                             unsigned char *hmac_out);

bool
mongoc_crypto_cng_sha1 (mongoc_crypto_t *crypto,
                        const unsigned char *input,
                        const size_t input_len,
                        unsigned char *hash_out);

void
mongoc_crypto_cng_hmac_sha256 (mongoc_crypto_t *crypto,
                               const void *key,
                               int key_len,
                               const unsigned char *data,
                               int data_len,
                               unsigned char *hmac_out);

bool
mongoc_crypto_cng_sha256 (mongoc_crypto_t *crypto,
                          const unsigned char *input,
                          const size_t input_len,
                          unsigned char *hash_out);


BSON_END_DECLS

#endif /* MONGOC_CRYPTO_CNG_PRIVATE_H */
#endif /* MONGOC_ENABLE_CRYPTO_CNG */
