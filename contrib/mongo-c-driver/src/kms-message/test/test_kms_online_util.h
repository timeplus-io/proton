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

#ifndef TEST_KMS_ONLINE_UTIL_H
#define TEST_KMS_ONLINE_UTIL_H

#include <mongoc/mongoc.h>
#include "kms_message/kms_request.h"
#include "kms_message/kms_response.h"

/* connect_with_tls creates a TLS stream.
 * port may be NULL. It defaults to "443".
 * ssl_opt may be NULL. It defaults to mongoc_ssl_opt_default (). */
mongoc_stream_t *
connect_with_tls (const char *host,
                  const char *port,
                  mongoc_ssl_opt_t *ssl_opt);

kms_response_t *
send_kms_request (kms_request_t *req, const char *host);

#endif /* TEST_KMS_ONLINE_UTIL_H */