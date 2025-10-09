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

#include "mongoc-config.h"
#include <bson/bson.h>

#ifdef MONGOC_ENABLE_CRYPTO

#ifndef MONGOC_CRYPTO_PRIVATE_H
#define MONGOC_CRYPTO_PRIVATE_H


BSON_BEGIN_DECLS

typedef struct _mongoc_crypto_t mongoc_crypto_t;
typedef enum { MONGOC_CRYPTO_ALGORITHM_SHA_1, MONGOC_CRYPTO_ALGORITHM_SHA_256 } mongoc_crypto_hash_algorithm_t;

struct _mongoc_crypto_t {
   void (*hmac) (mongoc_crypto_t *crypto,
                 const void *key,
                 int key_len,
                 const unsigned char *data,
                 int data_len,
                 unsigned char *hmac_out);
   bool (*hash) (mongoc_crypto_t *crypto, const unsigned char *input, const size_t input_len, unsigned char *hash_out);
   mongoc_crypto_hash_algorithm_t algorithm;
};

void
mongoc_crypto_init (mongoc_crypto_t *crypto, mongoc_crypto_hash_algorithm_t algo);

void
mongoc_crypto_hmac (mongoc_crypto_t *crypto,
                    const void *key,
                    int key_len,
                    const unsigned char *data,
                    int data_len,
                    unsigned char *hmac_out);

bool
mongoc_crypto_hash (mongoc_crypto_t *crypto,
                    const unsigned char *input,
                    const size_t input_len,
                    unsigned char *hash_out);

BSON_END_DECLS
#endif /* MONGOC_CRYPTO_PRIVATE_H */
#endif /* MONGOC_ENABLE_CRYPTO */
