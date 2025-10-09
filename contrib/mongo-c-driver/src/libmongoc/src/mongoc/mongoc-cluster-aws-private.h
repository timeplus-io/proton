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

#ifndef MONGOC_CLUSTER_AWS_PRIVATE_H
#define MONGOC_CLUSTER_AWS_PRIVATE_H

#include "bson/bson.h"
#include "mcd-time.h"
#include "mongoc/mongoc-cluster-private.h"
#include "common-thread-private.h" // bson_mutex_t

bool
_mongoc_cluster_auth_node_aws (mongoc_cluster_t *cluster,
                               mongoc_stream_t *stream,
                               mongoc_server_description_t *sd,
                               bson_error_t *error);

/* The following are declared in the private header for testing. It is only used
 * in test-mongoc-aws.c, mongoc-cluster-aws.c, and test-awsauth.c */
typedef struct {
   char *access_key_id;
   char *secret_access_key;
   char *session_token;
   // expiration is the time when these credentials expire.
   // If expiration.set is false, the credentials do not have a known
   // expiration.
   struct {
      mcd_timer value;
      bool set;
   } expiration;
} _mongoc_aws_credentials_t;

#define MONGOC_AWS_CREDENTIALS_INIT                                            \
   (_mongoc_aws_credentials_t)                                                 \
   {                                                                           \
      .access_key_id = NULL, .secret_access_key = NULL, .session_token = NULL, \
      .expiration = {.value = {.expire_at = {0}}, .set = false},               \
   }

#define MONGOC_AWS_CREDENTIALS_EXPIRATION_WINDOW_MS 60 * 5 * 1000

// _mongoc_aws_credentials_cache_t is a thread-safe global cache of AWS
// credentials.
typedef struct {
   struct {
      _mongoc_aws_credentials_t value;
      bool set;
   } cached;
   bson_mutex_t mutex; // guards cached.
} _mongoc_aws_credentials_cache_t;

extern _mongoc_aws_credentials_cache_t mongoc_aws_credentials_cache;

// _mongoc_aws_credentials_cache_init initializes the global
// `mongoc_aws_credentials_cache. It is expected to be called by mongoc_init.
void
_mongoc_aws_credentials_cache_init (void);

// _mongoc_aws_credentials_cache_lock exclusively locks the cache.
void
_mongoc_aws_credentials_cache_lock (void);

// _mongoc_aws_credentials_cache_unlock unlocks the cache.
void
_mongoc_aws_credentials_cache_unlock (void);

// _mongoc_aws_credentials_cache_put_nolock is a non-locking variant of
// _mongoc_aws_credentials_cache_put.
void
_mongoc_aws_credentials_cache_put_nolock (const _mongoc_aws_credentials_t *creds);

// _mongoc_aws_credentials_cache_put adds credentials into the global cache.
void
_mongoc_aws_credentials_cache_put (const _mongoc_aws_credentials_t *creds);

// _mongoc_aws_credentials_cache_get_nolock is a non-locking variant of
// _mongoc_aws_credentials_cache_get.
bool
_mongoc_aws_credentials_cache_get_nolock (_mongoc_aws_credentials_t *creds);

// _mongoc_aws_credentials_cache_get returns true if cached credentials were
// retrieved.
// The passed `creds` is expected to be initialized with
// MONGOC_AWS_CREDENTIALS_INIT. Returns true if there are valid cached
// credentials. Retrieved credentials are copied to `creds`. Callers are
// expected to call
// `_mongoc_aws_credentials_cleanup` on `creds`.
// Returns false and zeroes `creds` if there are no valid cached credentials.
bool
_mongoc_aws_credentials_cache_get (_mongoc_aws_credentials_t *creds);

// _mongoc_aws_credentials_cache_clear_nolock is the non-locking variant of
// _mongoc_aws_credentials_cache_clear
void
_mongoc_aws_credentials_cache_clear_nolock (void);

// _mongoc_aws_credentials_cache_clear clears credentials in the global cache
void
_mongoc_aws_credentials_cache_clear (void);

// _mongoc_aws_credentials_cache_cleanup frees data for the global cache.
// It is expected to be called by mongoc_cleanup.
void
_mongoc_aws_credentials_cache_cleanup (void);


bool
_mongoc_aws_credentials_obtain (mongoc_uri_t *uri, _mongoc_aws_credentials_t *creds, bson_error_t *error);

void
_mongoc_aws_credentials_copy_to (const _mongoc_aws_credentials_t *src, _mongoc_aws_credentials_t *dst);

void
_mongoc_aws_credentials_cleanup (_mongoc_aws_credentials_t *creds);

bool
_mongoc_validate_and_derive_region (char *sts_fqdn, size_t sts_fqdn_len, char **region, bson_error_t *error);

#endif /* MONGOC_CLUSTER_AWS_PRIVATE_H */
