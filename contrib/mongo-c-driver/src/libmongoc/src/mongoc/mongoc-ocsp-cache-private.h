/*
 * Copyright 2020-present MongoDB, Inc.
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

#ifndef MONGOC_OCSP_CACHE_PRIVATE_H
#define MONGOC_OCSP_CACHE_PRIVATE_H

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SSL_OPENSSL
#include "mongoc-openssl-private.h"

#ifdef MONGOC_ENABLE_OCSP_OPENSSL
#include <openssl/ocsp.h>

void
_mongoc_ocsp_cache_init (void);

void
_mongoc_ocsp_cache_set_resp (
   OCSP_CERTID *id, int cert_status, int reason, ASN1_GENERALIZEDTIME *this_update, ASN1_GENERALIZEDTIME *next_update);

int
_mongoc_ocsp_cache_length (void);

bool
_mongoc_ocsp_cache_get_status (OCSP_CERTID *id,
                               int *cert_status,
                               int *reason,
                               ASN1_GENERALIZEDTIME **this_update,
                               ASN1_GENERALIZEDTIME **next_update);

void
_mongoc_ocsp_cache_cleanup (void);

#endif /* MONGOC_ENABLE_OCSP_OPENSSL */
#endif /* MONGOC_ENABLE_SSL_OPENSSL */

/* ensure the translation unit is not empty */
extern int no_mongoc_ocsp_cache;
#endif /* MONGO_C_DRIVER_MONGOC_OCSP_CACHE_PRIVATE_H */
