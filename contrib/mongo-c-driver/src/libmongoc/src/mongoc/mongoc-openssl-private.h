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

#include "mongoc-prelude.h"

#ifndef MONGOC_OPENSSL_PRIVATE_H
#define MONGOC_OPENSSL_PRIVATE_H

#include <bson/bson.h>
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

#include "mongoc-ssl.h"
#include "mongoc-stream-tls-openssl-private.h"

#if (OPENSSL_VERSION_NUMBER >= 0x10001000L) && !defined(OPENSSL_NO_OCSP) && !defined(LIBRESSL_VERSION_NUMBER)
#define MONGOC_ENABLE_OCSP_OPENSSL
#endif


BSON_BEGIN_DECLS

bool
_mongoc_openssl_check_peer_hostname (SSL *ssl, const char *host, bool allow_invalid_hostname);
SSL_CTX *
_mongoc_openssl_ctx_new (mongoc_ssl_opt_t *opt);
char *
_mongoc_openssl_extract_subject (const char *filename, const char *passphrase);
void
_mongoc_openssl_init (void);
void
_mongoc_openssl_cleanup (void);

#ifdef MONGOC_ENABLE_OCSP_OPENSSL
int
_mongoc_ocsp_tlsext_status (SSL *ssl, mongoc_openssl_ocsp_opt_t *opts);
#endif

bool
_mongoc_tlsfeature_has_status_request (const uint8_t *data, int length);

BSON_END_DECLS


#endif /* MONGOC_OPENSSL_PRIVATE_H */
