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

#ifndef MONGOC_CYRUS_PRIVATE_H
#define MONGOC_CYRUS_PRIVATE_H

#include "mongoc-uri.h"
#include "mongoc-cluster-private.h"
#include "mongoc-sasl-private.h"
#include <bson/bson.h>
#include <sasl/sasl.h>


BSON_BEGIN_DECLS


typedef struct _mongoc_cyrus_t mongoc_cyrus_t;


struct _mongoc_cyrus_t {
   mongoc_sasl_t credentials;
   sasl_callback_t callbacks[6];
   sasl_conn_t *conn;
   bool done;
   int step;
   sasl_interact_t *interact;
};


#ifndef SASL_CALLBACK_FN
#define SASL_CALLBACK_FN(_f) ((int (*) (void)) ((void (*) (void)) (_f)))
#endif

int
_mongoc_cyrus_verifyfile_cb (void *context, const char *file, sasl_verify_type_t type);
void
_mongoc_cyrus_init (mongoc_cyrus_t *sasl);
bool
_mongoc_cyrus_new_from_cluster (
   mongoc_cyrus_t *sasl, mongoc_cluster_t *cluster, mongoc_stream_t *stream, const char *hostname, bson_error_t *error);
int
_mongoc_cyrus_log (mongoc_cyrus_t *sasl, int level, const char *message);
void
_mongoc_cyrus_destroy (mongoc_cyrus_t *sasl);
bool
_mongoc_cyrus_step (mongoc_cyrus_t *sasl,
                    const uint8_t *inbuf,
                    uint32_t inbuflen,
                    uint8_t **outbuf,
                    uint32_t *outbuflen,
                    bson_error_t *error);


BSON_END_DECLS


#endif /* MONGOC_CYRUS_PRIVATE_H */
