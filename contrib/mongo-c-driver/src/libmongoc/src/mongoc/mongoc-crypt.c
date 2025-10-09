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

#define MONGOC_LOG_DOMAIN "client-side-encryption"

#include "mongoc-crypt-private.h"

#ifdef MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION

#include <mongocrypt/mongocrypt.h>

#include "mongoc-client-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-host-list-private.h"
#include "mongoc-stream-private.h"
#include "mongoc-ssl-private.h"
#include "mongoc-cluster-aws-private.h"
#include "mongoc-util-private.h"
#include "mongoc-http-private.h"
#include "mcd-azure.h"
#include "mcd-time.h"
#include "service-gcp.h"

// `mcd_mapof_kmsid_to_tlsopts` maps a KMS ID (e.g. `aws` or `aws:myname`) to a
// `mongoc_ssl_opt_t`. The acryonym TLS is preferred over SSL for
// consistency with the CSE and URI specifications.
typedef struct {
   mongoc_array_t entries;
} mcd_mapof_kmsid_to_tlsopts;

typedef struct {
   char *kmsid;
   mongoc_ssl_opt_t tlsopts;
} mcd_mapof_kmsid_to_tlsopts_entry;

mcd_mapof_kmsid_to_tlsopts *
mcd_mapof_kmsid_to_tlsopts_new (void)
{
   mcd_mapof_kmsid_to_tlsopts *k2t = bson_malloc0 (sizeof (mcd_mapof_kmsid_to_tlsopts));
   _mongoc_array_init (&k2t->entries, sizeof (mcd_mapof_kmsid_to_tlsopts_entry));
   return k2t;
}

void
mcd_mapof_kmsid_to_tlsopts_destroy (mcd_mapof_kmsid_to_tlsopts *k2t)
{
   if (!k2t) {
      return;
   }
   for (size_t i = 0; i < k2t->entries.len; i++) {
      mcd_mapof_kmsid_to_tlsopts_entry *e = &_mongoc_array_index (&k2t->entries, mcd_mapof_kmsid_to_tlsopts_entry, i);
      bson_free (e->kmsid);
      _mongoc_ssl_opts_cleanup (&e->tlsopts, true /* free_internal */);
   }
   _mongoc_array_destroy (&k2t->entries);
   bson_free (k2t);
}

// `mcd_mapof_kmsid_to_tlsopts_insert` adds an entry into the map.
// `kmsid` and `tlsopts` are copied.
// No checking is done to prohibit duplicate entries.
void
mcd_mapof_kmsid_to_tlsopts_insert (mcd_mapof_kmsid_to_tlsopts *k2t, const char *kmsid, const mongoc_ssl_opt_t *tlsopts)
{
   BSON_ASSERT_PARAM (k2t);
   BSON_ASSERT_PARAM (kmsid);
   BSON_ASSERT_PARAM (tlsopts);

   mcd_mapof_kmsid_to_tlsopts_entry e = {.kmsid = bson_strdup (kmsid)};
   _mongoc_ssl_opts_copy_to (tlsopts, &e.tlsopts, true /* copy_internal */);
   _mongoc_array_append_val (&k2t->entries, e);
}

// `mcd_mapof_kmsid_to_tlsopts_get` returns the TLS options for a KMS ID, or
// NULL.
const mongoc_ssl_opt_t *
mcd_mapof_kmsid_to_tlsopts_get (const mcd_mapof_kmsid_to_tlsopts *k2t, const char *kmsid)
{
   BSON_ASSERT_PARAM (k2t);
   BSON_ASSERT_PARAM (kmsid);

   for (size_t i = 0; i < k2t->entries.len; i++) {
      mcd_mapof_kmsid_to_tlsopts_entry *e = &_mongoc_array_index (&k2t->entries, mcd_mapof_kmsid_to_tlsopts_entry, i);
      if (0 == strcmp (e->kmsid, kmsid)) {
         return &e->tlsopts;
      }
   }
   return NULL;
}


bool
mcd_mapof_kmsid_to_tlsopts_has (const mcd_mapof_kmsid_to_tlsopts *k2t, const char *kmsid)
{
   return NULL != mcd_mapof_kmsid_to_tlsopts_get (k2t, kmsid);
}

struct __mongoc_crypt_t {
   mongocrypt_t *handle;
   mongoc_ssl_opt_t kmip_tls_opt;
   mongoc_ssl_opt_t aws_tls_opt;
   mongoc_ssl_opt_t azure_tls_opt;
   mongoc_ssl_opt_t gcp_tls_opt;
   mcd_mapof_kmsid_to_tlsopts *kmsid_to_tlsopts;

   /// The kmsProviders that were provided by the user when encryption was
   /// initiated. We need to remember this in case we need to load on-demand
   /// credentials.
   bson_t kms_providers;
   mc_kms_credentials_callback creds_cb;

   /// The most recently auto-acquired Azure token, on null if it was destroyed
   /// or not yet acquired.
   mcd_azure_access_token azure_token;
   /// The time point at which the `azure_token` was acquired.
   mcd_time_point azure_token_issued_at;
};

static void
_log_callback (mongocrypt_log_level_t mongocrypt_log_level, const char *message, uint32_t message_len, void *ctx)
{
   mongoc_log_level_t log_level = MONGOC_LOG_LEVEL_ERROR;

   BSON_UNUSED (message_len);
   BSON_UNUSED (ctx);

   switch (mongocrypt_log_level) {
   case MONGOCRYPT_LOG_LEVEL_FATAL:
      log_level = MONGOC_LOG_LEVEL_CRITICAL;
      break;
   case MONGOCRYPT_LOG_LEVEL_ERROR:
      log_level = MONGOC_LOG_LEVEL_ERROR;
      break;
   case MONGOCRYPT_LOG_LEVEL_WARNING:
      log_level = MONGOC_LOG_LEVEL_WARNING;
      break;
   case MONGOCRYPT_LOG_LEVEL_INFO:
      log_level = MONGOC_LOG_LEVEL_INFO;
      break;
   case MONGOCRYPT_LOG_LEVEL_TRACE:
      log_level = MONGOC_LOG_LEVEL_TRACE;
      break;
   default:
      log_level = MONGOC_LOG_LEVEL_CRITICAL;
      break;
   }

   mongoc_log (log_level, MONGOC_LOG_DOMAIN, "%s", message);
}

static void
_prefix_mongocryptd_error (bson_error_t *error)
{
   char buf[sizeof (error->message)];

   bson_snprintf (buf, sizeof (buf), "mongocryptd error: %s:", error->message);
   memcpy (error->message, buf, sizeof (buf));
}

static void
_prefix_keyvault_error (bson_error_t *error)
{
   char buf[sizeof (error->message)];

   bson_snprintf (buf, sizeof (buf), "key vault error: %s:", error->message);
   memcpy (error->message, buf, sizeof (buf));
}

static void
_status_to_error (mongocrypt_status_t *status, bson_error_t *error)
{
   bson_set_error (error,
                   MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                   mongocrypt_status_code (status),
                   "%s",
                   mongocrypt_status_message (status, NULL));
}

/* Checks for an error on mongocrypt context.
 * If error_expected, then we expect mongocrypt_ctx_status to report a failure
 * status (due to a previous failed function call). If it did not, return a
 * generic error.
 * Returns true if ok, and does not modify @error.
 * Returns false if error, and sets @error.
 */
bool
_ctx_check_error (mongocrypt_ctx_t *ctx, bson_error_t *error, bool error_expected)
{
   mongocrypt_status_t *status;

   status = mongocrypt_status_new ();
   if (!mongocrypt_ctx_status (ctx, status)) {
      _status_to_error (status, error);
      mongocrypt_status_destroy (status);
      return false;
   } else if (error_expected) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                      "generic error from libmongocrypt operation");
      mongocrypt_status_destroy (status);
      return false;
   }
   mongocrypt_status_destroy (status);
   return true;
}

bool
_kms_ctx_check_error (mongocrypt_kms_ctx_t *kms_ctx, bson_error_t *error, bool error_expected)
{
   mongocrypt_status_t *status;

   status = mongocrypt_status_new ();
   if (!mongocrypt_kms_ctx_status (kms_ctx, status)) {
      _status_to_error (status, error);
      mongocrypt_status_destroy (status);
      return false;
   } else if (error_expected) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                      "generic error from libmongocrypt KMS operation");
      mongocrypt_status_destroy (status);
      return false;
   }
   mongocrypt_status_destroy (status);
   return true;
}

bool
_crypt_check_error (mongocrypt_t *crypt, bson_error_t *error, bool error_expected)
{
   mongocrypt_status_t *status;

   status = mongocrypt_status_new ();
   if (!mongocrypt_status (crypt, status)) {
      _status_to_error (status, error);
      mongocrypt_status_destroy (status);
      return false;
   } else if (error_expected) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                      "generic error from libmongocrypt handle");
      mongocrypt_status_destroy (status);
      return false;
   }
   mongocrypt_status_destroy (status);
   return true;
}

/* Convert a mongocrypt_binary_t to a static bson_t */
static bool
_bin_to_static_bson (mongocrypt_binary_t *bin, bson_t *out, bson_error_t *error)
{
   /* Copy bin into bson_t result. */
   if (!bson_init_static (out, mongocrypt_binary_data (bin), mongocrypt_binary_len (bin))) {
      bson_set_error (error, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "invalid returned bson");
      return false;
   }
   return true;
}


typedef struct {
   mongocrypt_ctx_t *ctx;
   mongoc_collection_t *keyvault_coll;
   mongoc_client_t *mongocryptd_client;
   mongoc_client_t *collinfo_client;
   const char *db_name;
   _mongoc_crypt_t *crypt;
} _state_machine_t;

_state_machine_t *
_state_machine_new (_mongoc_crypt_t *crypt)
{
   _state_machine_t *sm = bson_malloc0 (sizeof (_state_machine_t));
   sm->crypt = crypt;
   return sm;
}

void
_state_machine_destroy (_state_machine_t *state_machine)
{
   if (!state_machine) {
      return;
   }
   mongocrypt_ctx_destroy (state_machine->ctx);
   bson_free (state_machine);
}

/* State handler MONGOCRYPT_CTX_NEED_MONGO_COLLINFO */
static bool
_state_need_mongo_collinfo (_state_machine_t *state_machine, bson_error_t *error)
{
   mongoc_database_t *db = NULL;
   mongoc_cursor_t *cursor = NULL;
   bson_t filter_bson;
   const bson_t *collinfo_bson = NULL;
   bson_t opts = BSON_INITIALIZER;
   mongocrypt_binary_t *filter_bin = NULL;
   mongocrypt_binary_t *collinfo_bin = NULL;
   bool ret = false;

   /* 1. Run listCollections on the encrypted MongoClient with the filter
    * provided by mongocrypt_ctx_mongo_op */
   filter_bin = mongocrypt_binary_new ();
   if (!mongocrypt_ctx_mongo_op (state_machine->ctx, filter_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (!_bin_to_static_bson (filter_bin, &filter_bson, error)) {
      goto fail;
   }

   bson_append_document (&opts, "filter", -1, &filter_bson);
   db = mongoc_client_get_database (state_machine->collinfo_client, state_machine->db_name);
   cursor = mongoc_database_find_collections_with_opts (db, &opts);
   if (mongoc_cursor_error (cursor, error)) {
      goto fail;
   }

   /* 2. Return the first result (if any) with mongocrypt_ctx_mongo_feed or
    * proceed to the next step if nothing was returned. */
   if (mongoc_cursor_next (cursor, &collinfo_bson)) {
      collinfo_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (collinfo_bson), collinfo_bson->len);
      if (!mongocrypt_ctx_mongo_feed (state_machine->ctx, collinfo_bin)) {
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
   } else if (mongoc_cursor_error (cursor, error)) {
      goto fail;
   }

   /* 3. Call mongocrypt_ctx_mongo_done */
   if (!mongocrypt_ctx_mongo_done (state_machine->ctx)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   ret = true;

fail:

   bson_destroy (&opts);
   mongocrypt_binary_destroy (filter_bin);
   mongocrypt_binary_destroy (collinfo_bin);
   mongoc_cursor_destroy (cursor);
   mongoc_database_destroy (db);
   return ret;
}

static bool
_state_need_mongo_markings (_state_machine_t *state_machine, bson_error_t *error)
{
   bool ret = false;
   mongocrypt_binary_t *mongocryptd_cmd_bin = NULL;
   mongocrypt_binary_t *mongocryptd_reply_bin = NULL;
   bson_t mongocryptd_cmd_bson;
   bson_t reply = BSON_INITIALIZER;

   mongocryptd_cmd_bin = mongocrypt_binary_new ();

   if (!mongocrypt_ctx_mongo_op (state_machine->ctx, mongocryptd_cmd_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (!_bin_to_static_bson (mongocryptd_cmd_bin, &mongocryptd_cmd_bson, error)) {
      goto fail;
   }

   /* 1. Use db.runCommand to run the command provided by
    * mongocrypt_ctx_mongo_op on the MongoClient connected to mongocryptd. */
   bson_destroy (&reply);
   if (!mongoc_client_command_simple (state_machine->mongocryptd_client,
                                      state_machine->db_name,
                                      &mongocryptd_cmd_bson,
                                      NULL /* read_prefs */,
                                      &reply,
                                      error)) {
      _prefix_mongocryptd_error (error);
      goto fail;
   }

   /* 2. Feed the reply back with mongocrypt_ctx_mongo_feed. */
   mongocryptd_reply_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (&reply), reply.len);
   if (!mongocrypt_ctx_mongo_feed (state_machine->ctx, mongocryptd_reply_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   /* 3. Call mongocrypt_ctx_mongo_done. */
   if (!mongocrypt_ctx_mongo_done (state_machine->ctx)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   ret = true;
fail:
   bson_destroy (&reply);
   mongocrypt_binary_destroy (mongocryptd_cmd_bin);
   mongocrypt_binary_destroy (mongocryptd_reply_bin);
   return ret;
}

static bool
_state_need_mongo_keys (_state_machine_t *state_machine, bson_error_t *error)
{
   bool ret = false;
   mongocrypt_binary_t *filter_bin = NULL;
   bson_t filter_bson;
   bson_t opts = BSON_INITIALIZER;
   mongocrypt_binary_t *key_bin = NULL;
   const bson_t *key_bson;
   mongoc_cursor_t *cursor = NULL;

   /* 1. Use MongoCollection.find on the MongoClient connected to the key vault
    * client (which may be the same as the encrypted client). Use the filter
    * provided by mongocrypt_ctx_mongo_op. */
   filter_bin = mongocrypt_binary_new ();
   if (!mongocrypt_ctx_mongo_op (state_machine->ctx, filter_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (!_bin_to_static_bson (filter_bin, &filter_bson, error)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   {
      const mongoc_read_concern_t *const rc = mongoc_collection_get_read_concern (state_machine->keyvault_coll);
      const char *const level = rc ? mongoc_read_concern_get_level (rc) : NULL;
      BSON_ASSERT (level && strcmp (level, MONGOC_READ_CONCERN_LEVEL_MAJORITY) == 0);
   }

   cursor = mongoc_collection_find_with_opts (state_machine->keyvault_coll, &filter_bson, &opts, NULL /* read prefs */);
   /* 2. Feed all resulting documents back (if any) with repeated calls to
    * mongocrypt_ctx_mongo_feed. */
   while (mongoc_cursor_next (cursor, &key_bson)) {
      mongocrypt_binary_destroy (key_bin);
      key_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (key_bson), key_bson->len);
      if (!mongocrypt_ctx_mongo_feed (state_machine->ctx, key_bin)) {
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
   }
   if (mongoc_cursor_error (cursor, error)) {
      _prefix_keyvault_error (error);
      goto fail;
   }

   /* 3. Call mongocrypt_ctx_mongo_done. */
   if (!mongocrypt_ctx_mongo_done (state_machine->ctx)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   ret = true;
fail:
   mongocrypt_binary_destroy (filter_bin);
   mongoc_cursor_destroy (cursor);
   bson_destroy (&opts);
   mongocrypt_binary_destroy (key_bin);
   return ret;
}

static mongoc_stream_t *
_get_stream (const char *endpoint, int32_t connecttimeoutms, const mongoc_ssl_opt_t *ssl_opt, bson_error_t *error)
{
   mongoc_stream_t *base_stream = NULL;
   mongoc_stream_t *tls_stream = NULL;
   bool ret = false;
   mongoc_ssl_opt_t ssl_opt_copy = {0};
   mongoc_host_list_t host;

   if (!_mongoc_host_list_from_string_with_err (&host, endpoint, error)) {
      goto fail;
   }

   base_stream = mongoc_client_connect_tcp (connecttimeoutms, &host, error);
   if (!base_stream) {
      goto fail;
   }

   /* Wrap in a tls_stream. */
   _mongoc_ssl_opts_copy_to (ssl_opt, &ssl_opt_copy, true /* copy_internal */);
   tls_stream = mongoc_stream_tls_new_with_hostname (base_stream, host.host, &ssl_opt_copy, 1 /* client */);

   if (!tls_stream) {
      bson_set_error (
         error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Failed to create TLS stream to: %s", endpoint);
      goto fail;
   }

   if (!mongoc_stream_tls_handshake_block (tls_stream, host.host, connecttimeoutms, error)) {
      goto fail;
   }

   ret = true;
fail:
   _mongoc_ssl_opts_cleanup (&ssl_opt_copy, true /* free_internal */);
   if (!ret) {
      if (tls_stream) {
         /* destroys base_stream too */
         mongoc_stream_destroy (tls_stream);
      } else if (base_stream) {
         mongoc_stream_destroy (base_stream);
      }
      return NULL;
   }
   return tls_stream;
}

static bool
_state_need_kms (_state_machine_t *state_machine, bson_error_t *error)
{
   mongocrypt_kms_ctx_t *kms_ctx = NULL;
   mongoc_stream_t *tls_stream = NULL;
   bool ret = false;
   mongocrypt_binary_t *http_req = NULL;
   mongocrypt_binary_t *http_reply = NULL;
   const char *endpoint;
   const int32_t sockettimeout = MONGOC_DEFAULT_SOCKETTIMEOUTMS;
   kms_ctx = mongocrypt_ctx_next_kms_ctx (state_machine->ctx);
   while (kms_ctx) {
      mongoc_iovec_t iov;
      const mongoc_ssl_opt_t *ssl_opt;
      const char *provider;

      provider = mongocrypt_kms_ctx_get_kms_provider (kms_ctx, NULL);

      if (0 == strcmp ("kmip", provider)) {
         ssl_opt = &state_machine->crypt->kmip_tls_opt;
      } else if (0 == strcmp ("aws", provider)) {
         ssl_opt = &state_machine->crypt->aws_tls_opt;
      } else if (0 == strcmp ("azure", provider)) {
         ssl_opt = &state_machine->crypt->azure_tls_opt;
      } else if (0 == strcmp ("gcp", provider)) {
         ssl_opt = &state_machine->crypt->gcp_tls_opt;
      } else if (mcd_mapof_kmsid_to_tlsopts_has (state_machine->crypt->kmsid_to_tlsopts, provider)) {
         ssl_opt = mcd_mapof_kmsid_to_tlsopts_get (state_machine->crypt->kmsid_to_tlsopts, provider);
      } else {
         ssl_opt = mongoc_ssl_opt_get_default ();
      }

      mongocrypt_binary_destroy (http_req);
      http_req = mongocrypt_binary_new ();
      if (!mongocrypt_kms_ctx_message (kms_ctx, http_req)) {
         _kms_ctx_check_error (kms_ctx, error, true);
         goto fail;
      }

      if (!mongocrypt_kms_ctx_endpoint (kms_ctx, &endpoint)) {
         _kms_ctx_check_error (kms_ctx, error, true);
         goto fail;
      }

      mongoc_stream_destroy (tls_stream);
      tls_stream = _get_stream (endpoint, sockettimeout, ssl_opt, error);
#ifdef MONGOC_ENABLE_SSL_SECURE_CHANNEL
      /* Retry once with schannel as a workaround for CDRIVER-3566. */
      if (!tls_stream) {
         tls_stream = _get_stream (endpoint, sockettimeout, ssl_opt, error);
      }
#endif
      if (!tls_stream) {
         goto fail;
      }

      iov.iov_base = (char *) mongocrypt_binary_data (http_req);
      iov.iov_len = mongocrypt_binary_len (http_req);

      if (!_mongoc_stream_writev_full (tls_stream, &iov, 1, sockettimeout, error)) {
         goto fail;
      }

      /* Read and feed reply. */
      while (mongocrypt_kms_ctx_bytes_needed (kms_ctx) > 0) {
#define BUFFER_SIZE 1024
         uint8_t buf[BUFFER_SIZE];
         uint32_t bytes_needed = mongocrypt_kms_ctx_bytes_needed (kms_ctx);
         ssize_t read_ret;

         /* Cap the bytes requested at the buffer size. */
         if (bytes_needed > BUFFER_SIZE) {
            bytes_needed = BUFFER_SIZE;
         }

         read_ret = mongoc_stream_read (tls_stream, buf, bytes_needed, 1 /* min_bytes. */, sockettimeout);
         if (read_ret == -1) {
            bson_set_error (
               error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "failed to read from KMS stream: %d", errno);
            goto fail;
         }

         if (read_ret == 0) {
            bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "unexpected EOF from KMS stream");
            goto fail;
         }

         mongocrypt_binary_destroy (http_reply);

         BSON_ASSERT (bson_in_range_signed (uint32_t, read_ret));
         http_reply = mongocrypt_binary_new_from_data (buf, (uint32_t) read_ret);
         if (!mongocrypt_kms_ctx_feed (kms_ctx, http_reply)) {
            _kms_ctx_check_error (kms_ctx, error, true);
            goto fail;
         }
      }
      kms_ctx = mongocrypt_ctx_next_kms_ctx (state_machine->ctx);
   }
   /* When NULL is returned by mongocrypt_ctx_next_kms_ctx, this can either be
    * an error or end-of-list. */
   if (!_ctx_check_error (state_machine->ctx, error, false)) {
      goto fail;
   }

   if (!mongocrypt_ctx_kms_done (state_machine->ctx)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   ret = true;
fail:
   mongoc_stream_destroy (tls_stream);
   mongocrypt_binary_destroy (http_req);
   mongocrypt_binary_destroy (http_reply);
   return ret;
#undef BUFFER_SIZE
}

/**
 * @brief Determine whether the given kmsProviders has an empty 'aws'
 * subdocument
 *
 * @param kms_providers The user-provided kmsProviders
 * @param error Output parameter for possible errors.
 * @retval true If 'aws' is present and an empty subdocument
 * @retval false Otherwise or on error
 */
static bool
_needs_on_demand_aws_kms (bson_t const *kms_providers)
{
   bson_iter_t iter;
   if (!bson_iter_init_find (&iter, kms_providers, "aws")) {
      // No "aws" subdocument
      return false;
   }

   if (!BSON_ITER_HOLDS_DOCUMENT (&iter)) {
      // "aws" is not a document? Should be validated by libmongocrypt
      return false;
   }

   const uint8_t *dataptr;
   uint32_t datalen;
   bson_iter_document (&iter, &datalen, &dataptr);
   bson_t subdoc;
   if (!bson_init_static (&subdoc, dataptr, datalen)) {
      // Invalid "aws" document? Should be validated by libmongocrypt
      return false;
   }

   if (bson_empty (&subdoc)) {
      // "aws" is present and is an empty subdocument, which means that the user
      // requests that the AWS credentials be loaded on-demand from the
      // environment.
      return true;
   } else {
      // "aws" is present and is non-empty, which means that the user has
      // already provided credentials for AWS.
      return false;
   }
}

/**
 * @brief Check whether the given kmsProviders object requests automatic Azure
 * credentials
 *
 * @param kmsprov The input kmsProviders that may have an "azure" property
 * @param error An output error
 * @retval true If success AND `kmsprov` requests automatic Azure credentials
 * @retval false Otherwise. Check error->code for failure.
 */
static bool
_check_azure_kms_auto (const bson_t *kmsprov, bson_error_t *error)
{
   if (error) {
      *error = (bson_error_t){0};
   }

   bson_iter_t iter;
   if (!bson_iter_init_find (&iter, kmsprov, "azure")) {
      return false;
   }

   bson_t azure_subdoc;
   if (!_mongoc_iter_document_as_bson (&iter, &azure_subdoc, error)) {
      return false;
   }

   return bson_empty (&azure_subdoc);
}

/**
 * @brief Attempt to load AWS credentials from the environment and insert them
 * into the given kmsProviders bson document on the "aws" property.
 *
 * @param out A kmsProviders object to update
 * @param error An error-out parameter
 * @retval true If there was no error and we successfully loaded credentials.
 * @retval false If there was an error while updating the BSON data or obtaining
 * credentials.
 */
static bool
_try_add_aws_from_env (bson_t *out, bson_error_t *error)
{
   // Attempt to obtain AWS credentials from the environment.
   _mongoc_aws_credentials_t creds;
   if (!_mongoc_aws_credentials_obtain (NULL, &creds, error)) {
      // Error while obtaining credentials
      return false;
   }

   // Build the new "aws" subdoc
   bson_t aws;
   bool okay = BSON_APPEND_DOCUMENT_BEGIN (out, "aws", &aws)
               // Add the accessKeyId and the secretAccessKey
               && BSON_APPEND_UTF8 (&aws, "accessKeyId", creds.access_key_id)         //
               && BSON_APPEND_UTF8 (&aws, "secretAccessKey", creds.secret_access_key) //
               // Add the sessionToken, if we got one:
               && (!creds.session_token || BSON_APPEND_UTF8 (&aws, "sessionToken", creds.session_token)) //
               // Finish the document
               && bson_append_document_end (out, &aws);
   BSON_ASSERT (okay && "Failed to build aws credentials document");
   // Good!
   _mongoc_aws_credentials_cleanup (&creds);
   return true;
}

/**
 * @brief Attempt to request a new Azure access token from the IMDS HTTP server
 *
 * @param out The token to populate. Must later be destroyed by the caller.
 * @param error An output parameter to capture any errors
 * @retval true Upon successfully obtaining and parsing a token
 * @retval false If any error occurs.
 */
static bool
_request_new_azure_token (mcd_azure_access_token *out, bson_error_t *error)
{
   return mcd_azure_access_token_from_imds (out,
                                            NULL, // Use the default host
                                            0,    //  Default port as well
                                            NULL, // No extra headers
                                            error);
}

/**
 * @brief Attempt to load an Azure access token from the environment and append
 * them to the kmsProviders
 *
 * @param out A kmsProviders object to update
 * @param error An error-out parameter
 * @retval true If there was no error and we loaded credentials
 * @retval false If there was an error obtaining or appending credentials
 */
static bool
_try_add_azure_from_env (_mongoc_crypt_t *crypt, bson_t *out, bson_error_t *error)
{
   if (crypt->azure_token.access_token) {
      // The access-token is non-null, so we may have one cached.
      mcd_time_point one_min_from_now = mcd_later (mcd_now (), mcd_minutes (1));
      mcd_time_point expires_at = mcd_later (crypt->azure_token_issued_at, crypt->azure_token.expires_in);
      if (mcd_time_compare (expires_at, one_min_from_now) >= 0) {
         // The token is still valid for at least another minute
      } else {
         // The token will expire soon. Destroy it, and below we will below ask
         // IMDS for a new one.
         mcd_azure_access_token_destroy (&crypt->azure_token);
      }
   }

   if (crypt->azure_token.access_token == NULL) {
      // There is no access token in our cache.
      // Save the current time point as the "issue time" of the token, even
      // though it will take some time for the HTTP request to hit the metadata
      // server. This time is only used to track token expiry. IMDS gives us a
      // number of seconds that the token will be valid relative to its issue
      // time. Avoid reliance on system clocks by comparing the issue time to an
      // abstract monotonic "now"
      crypt->azure_token_issued_at = mcd_now ();
      // Get the token:
      if (!_request_new_azure_token (&crypt->azure_token, error)) {
         return false;
      }
   }

   // Build the new KMS credentials
   bson_t new_azure_creds = BSON_INITIALIZER;
   const bool okay = BSON_APPEND_UTF8 (&new_azure_creds, "accessToken", crypt->azure_token.access_token) &&
                     BSON_APPEND_DOCUMENT (out, "azure", &new_azure_creds);
   bson_destroy (&new_azure_creds);
   if (!okay) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_BSON_INVALID,
                      "Failed to build new 'azure' credentials");
   }

   return okay;
}

/**
 * @brief Check whether the given kmsProviders object requests automatic GCP
 * credentials
 *
 * @param kmsprov The input kmsProviders that may have an "gcp" property
 * @param error An output error
 * @retval true If success AND `kmsprov` requests automatic GCP credentials
 * @retval false Otherwise. Check error->code for failure.
 */
static bool
_check_gcp_kms_auto (const bson_t *kmsprov, bson_error_t *error)
{
   if (error) {
      *error = (bson_error_t){0};
   }

   bson_iter_t iter;
   if (!bson_iter_init_find (&iter, kmsprov, "gcp")) {
      return false;
   }

   bson_t gcp_subdoc;
   if (!_mongoc_iter_document_as_bson (&iter, &gcp_subdoc, error)) {
      return false;
   }

   return bson_empty (&gcp_subdoc);
}

/**
 * @brief Attempt to request a new GCP access token from the HTTP server
 *
 * @param out The token to populate. Must later be destroyed by the caller.
 * @param error An output parameter to capture any errors
 * @retval true Upon successfully obtaining and parsing a token
 * @retval false If any error occurs.
 */
static bool
_request_new_gcp_token (gcp_service_account_token *out, bson_error_t *error)
{
   return (gcp_access_token_from_gcp_server (out, NULL, 0, NULL, error));
}

/**
 * @brief Attempt to load an GCP access token from the environment and append
 * them to the kmsProviders
 *
 * @param out A kmsProviders object to update
 * @param error An error-out parameter
 * @retval true If there was no error and we loaded credentials
 * @retval false If there was an error obtaining or appending credentials
 */
static bool
_try_add_gcp_from_env (bson_t *out, bson_error_t *error)
{
   // Not caching gcp tokens, so we will always request a new one from the gcp
   // server.
   gcp_service_account_token gcp_token;
   if (!_request_new_gcp_token (&gcp_token, error)) {
      return false;
   }

   // Build the new KMS credentials
   bson_t new_gcp_creds = BSON_INITIALIZER;
   const bool okay = BSON_APPEND_UTF8 (&new_gcp_creds, "accessToken", gcp_token.access_token) &&
                     BSON_APPEND_DOCUMENT (out, "gcp", &new_gcp_creds);
   bson_destroy (&new_gcp_creds);
   gcp_access_token_destroy (&gcp_token);
   if (!okay) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_BSON_INVALID,
                      "Failed to build new 'gcp' credentials");
   }

   return okay;
}

static bool
_state_need_kms_credentials (_state_machine_t *sm, bson_error_t *error)
{
   bson_t creds = BSON_INITIALIZER;
   const bson_t empty = BSON_INITIALIZER;
   bool okay = false;

   if (sm->crypt->creds_cb.fn) {
      // We have a user-provided credentials callback. Try it.
      if (!sm->crypt->creds_cb.fn (sm->crypt->creds_cb.userdata, &empty, &creds, error)) {
         // User-provided callback indicated failure
         if (!error->code) {
            // The callback did not set an error, so we'll provide a default
            // one.
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "The user-provided callback for on-demand KMS "
                            "credentials failed.");
         }
         goto fail;
      }
      // The user's callback reported success
   }

   bson_iter_t iter;
   const bool callback_provided_aws = bson_iter_init_find (&iter, &creds, "aws");

   if (!callback_provided_aws && _needs_on_demand_aws_kms (&sm->crypt->kms_providers)) {
      // The original kmsProviders had an empty "aws" property, and the
      // user-provided callback did not fill in a new "aws" property for us.
      // Attempt instead to load the AWS credentials from the environment:
      if (!_try_add_aws_from_env (&creds, error)) {
         // Error while trying to add AWS credentials
         goto fail;
      }
   }

   // Whether the callback provided Azure credentials
   const bool cb_provided_azure = bson_iter_init_find (&iter, &creds, "azure");
   // Whether the original kmsProviders requested auto-Azure credentials:
   const bool orig_wants_auto_azure = _check_azure_kms_auto (&sm->crypt->kms_providers, error);
   if (error->code) {
      // _check_azure_kms_auto failed
      goto fail;
   }
   const bool wants_auto_azure = orig_wants_auto_azure && !cb_provided_azure;
   if (wants_auto_azure) {
      if (!_try_add_azure_from_env (sm->crypt, &creds, error)) {
         goto fail;
      }
   }

   // Whether the callback provided GCP credentials
   const bool cb_provided_gcp = bson_iter_init_find (&iter, &creds, "gcp");
   // Whether the original kmsProviders requested auto-GCP credentials:
   const bool orig_wants_auto_gcp = _check_gcp_kms_auto (&sm->crypt->kms_providers, error);
   if (error->code) {
      // _check_gcp_kms_auto failed
      goto fail;
   }
   const bool wants_auto_gcp = orig_wants_auto_gcp && !cb_provided_gcp;
   if (wants_auto_gcp) {
      if (!_try_add_gcp_from_env (&creds, error)) {
         goto fail;
      }
   }

   // Now actually send that data to libmongocrypt
   mongocrypt_binary_t *const def = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (&creds), creds.len);
   okay = mongocrypt_ctx_provide_kms_providers (sm->ctx, def);
   if (!okay) {
      _ctx_check_error (sm->ctx, error, true);
   }
   mongocrypt_binary_destroy (def);

fail:
   bson_destroy (&creds);

   return okay;
}


static bool
_state_ready (_state_machine_t *state_machine, bson_t *result, bson_error_t *error)
{
   mongocrypt_binary_t *result_bin = NULL;
   bson_t tmp;
   bool ret = false;

   bson_init (result);
   result_bin = mongocrypt_binary_new ();
   if (!mongocrypt_ctx_finalize (state_machine->ctx, result_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (!_bin_to_static_bson (result_bin, &tmp, error)) {
      goto fail;
   }

   bson_destroy (result);
   bson_copy_to (&tmp, result);

   ret = true;
fail:
   mongocrypt_binary_destroy (result_bin);
   return ret;
}

/*--------------------------------------------------------------------------
 *
 * _mongoc_cse_run_state_machine --
 *    Run the mongocrypt_ctx state machine.
 *
 * Post-conditions:
 *    *result may be set to a new bson_t, or NULL otherwise. Caller should
 *    not assume return value of true means *result is set. If false returned,
 *    @error is set.
 *
 * --------------------------------------------------------------------------
 */
bool
_state_machine_run (_state_machine_t *state_machine, bson_t *result, bson_error_t *error)
{
   bool ret = false;
   mongocrypt_binary_t *bin = NULL;

   bson_init (result);
   while (true) {
      switch (mongocrypt_ctx_state (state_machine->ctx)) {
      default:
      case MONGOCRYPT_CTX_ERROR:
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      case MONGOCRYPT_CTX_NEED_MONGO_COLLINFO:
         if (!_state_need_mongo_collinfo (state_machine, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_NEED_MONGO_MARKINGS:
         if (!_state_need_mongo_markings (state_machine, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_NEED_MONGO_KEYS:
         if (!_state_need_mongo_keys (state_machine, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_NEED_KMS:
         if (!_state_need_kms (state_machine, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS:
         if (!_state_need_kms_credentials (state_machine, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_READY:
         bson_destroy (result);
         if (!_state_ready (state_machine, result, error)) {
            goto fail;
         }
         break;
      case MONGOCRYPT_CTX_DONE:
         goto success;
         break;
      case MONGOCRYPT_CTX_NEED_MONGO_COLLINFO_WITH_DB:
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                         MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                         "MONGOCRYPT_CTX_NEED_MONGO_COLLINFO_WITH_DB is "
                         "unimplemented");
         goto fail;
         break;
      }
   }

success:
   ret = true;
fail:
   mongocrypt_binary_destroy (bin);
   return ret;
}

/* _parse_one_tls_opts parses one TLS document.
 * Pre-conditions:
 * - @iter is an iterator at the start of a KMS provider key/value pair.
 * - @out_opt must not be initialized.
 * Post-conditions:
 * - @out_opt is always initialized.
 * Returns false and sets @error on error. */
static bool
_parse_one_tls_opts (bson_iter_t *iter, mongoc_ssl_opt_t *out_opt, bson_error_t *error)
{
   bool ok = false;
   const char *kms_provider;
   bson_t tls_opts_doc;
   const uint8_t *data;
   uint32_t len;
   bson_string_t *errmsg;
   bson_iter_t permitted_iter;

   errmsg = bson_string_new (NULL);
   kms_provider = bson_iter_key (iter);
   memset (out_opt, 0, sizeof (mongoc_ssl_opt_t));

   if (!BSON_ITER_HOLDS_DOCUMENT (iter)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Expected TLS options for %s to be a document, got: %s",
                      kms_provider,
                      _mongoc_bson_type_to_str (bson_iter_type (iter)));
      goto fail;
   }

   bson_iter_document (iter, &len, &data);
   if (!bson_init_static (&tls_opts_doc, data, len) || !bson_iter_init (&permitted_iter, &tls_opts_doc)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Error iterating into TLS options document for %s",
                      kms_provider);
      goto fail;
   }

   while (bson_iter_next (&permitted_iter)) {
      const char *key = bson_iter_key (&permitted_iter);

      if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD)) {
         continue;
      }

      if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCERTIFICATEKEYFILE)) {
         continue;
      }

      if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCAFILE)) {
         continue;
      }

      if (0 == bson_strcasecmp (key, MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK)) {
         continue;
      }

      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Error setting TLS option %s for %s. Insecure TLS options prohibited.",
                      key,
                      kms_provider);
      goto fail;
   }

   if (!_mongoc_ssl_opts_from_bson (out_opt, &tls_opts_doc, errmsg)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Error parsing TLS options for %s: %s",
                      kms_provider,
                      errmsg->str);
      goto fail;
   }

   ok = true;
fail:
   bson_string_free (errmsg, true /* free_segment */);
   return ok;
}

/* _parse_all_tls_opts initializes TLS options for all KMS providers.
 * @tls_opts is the BSON document passed through
 * mongoc_client_encryption_opts_set_tls_opts or
 * mongoc_auto_encryption_opts_set_tls_opts.
 * Defaults to using mongoc_ssl_opt_get_default() if options are not passed for
 * a provider. Returns false and sets @error on error. */
static bool
_parse_all_tls_opts (_mongoc_crypt_t *crypt, const bson_t *tls_opts, bson_error_t *error)
{
   bson_iter_t iter;
   bool ok = false;
   bool has_aws = false;
   bool has_azure = false;
   bool has_gcp = false;
   bool has_kmip = false;

   if (!tls_opts) {
      return true;
   }

   if (!bson_iter_init (&iter, tls_opts)) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Error starting iteration of TLS options");
      goto fail;
   }

   while (bson_iter_next (&iter)) {
      const char *key;

      key = bson_iter_key (&iter);
      if (0 == strcmp (key, "aws")) {
         if (has_aws) {
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "Error parsing duplicate TLS options for %s",
                            key);
            goto fail;
         }

         has_aws = true;
         if (!_parse_one_tls_opts (&iter, &crypt->aws_tls_opt, error)) {
            goto fail;
         }
         continue;
      }

      if (0 == strcmp (key, "azure")) {
         if (has_azure) {
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "Error parsing duplicate TLS options for %s",
                            key);
            goto fail;
         }

         has_azure = true;
         if (!_parse_one_tls_opts (&iter, &crypt->azure_tls_opt, error)) {
            goto fail;
         }
         continue;
      }

      if (0 == strcmp (key, "gcp")) {
         if (has_gcp) {
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "Error parsing duplicate TLS options for %s",
                            key);
            goto fail;
         }

         has_gcp = true;
         if (!_parse_one_tls_opts (&iter, &crypt->gcp_tls_opt, error)) {
            goto fail;
         }
         continue;
      }

      if (0 == strcmp (key, "kmip")) {
         if (has_kmip) {
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "Error parsing duplicate TLS options for %s",
                            key);
            goto fail;
         }

         has_kmip = true;
         if (!_parse_one_tls_opts (&iter, &crypt->kmip_tls_opt, error)) {
            goto fail;
         }
         continue;
      }

      const char *colon_pos = strstr (key, ":");
      if (colon_pos != NULL) {
         // Parse TLS options for a named KMS provider.
         if (mcd_mapof_kmsid_to_tlsopts_has (crypt->kmsid_to_tlsopts, key)) {
            bson_set_error (error,
                            MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                            MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                            "Error parsing duplicate TLS options for %s",
                            key);
            goto fail;
         }

         mongoc_ssl_opt_t tlsopts = {0};
         if (!_parse_one_tls_opts (&iter, &tlsopts, error)) {
            _mongoc_ssl_opts_cleanup (&tlsopts, true /* free_internal */);
            goto fail;
         }
         mcd_mapof_kmsid_to_tlsopts_insert (crypt->kmsid_to_tlsopts, key, &tlsopts);
         _mongoc_ssl_opts_cleanup (&tlsopts, true /* free_internal */);
         continue;
      }

      bson_set_error (error,
                      MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                      "Cannot configure TLS options for KMS provider: %s",
                      key);
      goto fail;
   }

   /* Configure with default TLS options. The mongoc_ssl_opt_t returned by
    * mongoc_ssl_opt_get_default may contain non-NULL fields if
    * MONGOC_SSL_DEFAULT_TRUST_FILE or MONGOC_SSL_DEFAULT_TRUST_DIR are defined.
    */
   if (!has_aws) {
      _mongoc_ssl_opts_copy_to (mongoc_ssl_opt_get_default (), &crypt->aws_tls_opt, false /* copy internal */);
   }

   if (!has_azure) {
      _mongoc_ssl_opts_copy_to (mongoc_ssl_opt_get_default (), &crypt->azure_tls_opt, false /* copy internal */);
   }

   if (!has_gcp) {
      _mongoc_ssl_opts_copy_to (mongoc_ssl_opt_get_default (), &crypt->gcp_tls_opt, false /* copy internal */);
   }

   if (!has_kmip) {
      _mongoc_ssl_opts_copy_to (mongoc_ssl_opt_get_default (), &crypt->kmip_tls_opt, false /* copy internal */);
   }
   ok = true;
fail:
   return ok;
}

/* Note, _mongoc_crypt_t only has one member, to the top-level handle of
   libmongocrypt, mongocrypt_t.
   The purpose of defining _mongoc_crypt_t is to limit all interaction with
   libmongocrypt to this one
   file.
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
                   bson_error_t *error)
{
   _mongoc_crypt_t *crypt;
   mongocrypt_binary_t *local_masterkey_bin = NULL;
   mongocrypt_binary_t *schema_map_bin = NULL;
   mongocrypt_binary_t *encrypted_fields_map_bin = NULL;
   mongocrypt_binary_t *kms_providers_bin = NULL;
   bool success = false;

   /* Create the handle to libmongocrypt. */
   crypt = bson_malloc0 (sizeof (*crypt));
   crypt->kmsid_to_tlsopts = mcd_mapof_kmsid_to_tlsopts_new ();
   crypt->handle = mongocrypt_new ();

   // Stash away a copy of the user's kmsProviders in case we need to lazily
   // load credentials.
   bson_copy_to (kms_providers, &crypt->kms_providers);

   if (!_parse_all_tls_opts (crypt, tls_opts, error)) {
      goto fail;
   }

   mongocrypt_setopt_log_handler (crypt->handle, _log_callback, NULL /* context */);

   kms_providers_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (kms_providers), kms_providers->len);
   if (!mongocrypt_setopt_kms_providers (crypt->handle, kms_providers_bin)) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   if (schema_map) {
      schema_map_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (schema_map), schema_map->len);
      if (!mongocrypt_setopt_schema_map (crypt->handle, schema_map_bin)) {
         _crypt_check_error (crypt->handle, error, true);
         goto fail;
      }
   }

   if (encrypted_fields_map) {
      encrypted_fields_map_bin =
         mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (encrypted_fields_map), encrypted_fields_map->len);
      if (!mongocrypt_setopt_encrypted_field_config_map (crypt->handle, encrypted_fields_map_bin)) {
         _crypt_check_error (crypt->handle, error, true);
         goto fail;
      }
   }

   if (!bypass_auto_encryption) {
      mongocrypt_setopt_append_crypt_shared_lib_search_path (crypt->handle, "$SYSTEM");
      if (!_crypt_check_error (crypt->handle, error, false)) {
         goto fail;
      }

      if (crypt_shared_lib_path != NULL) {
         mongocrypt_setopt_set_crypt_shared_lib_path_override (crypt->handle, crypt_shared_lib_path);
         if (!_crypt_check_error (crypt->handle, error, false)) {
            goto fail;
         }
      }
   }

   if (bypass_query_analysis) {
      mongocrypt_setopt_bypass_query_analysis (crypt->handle);
      if (!_crypt_check_error (crypt->handle, error, false)) {
         goto fail;
      }
   }

   // Enable the NEEDS_CREDENTIALS state for on-demand credential loading
   mongocrypt_setopt_use_need_kms_credentials_state (crypt->handle);

   if (!mongocrypt_init (crypt->handle)) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   if (crypt_shared_lib_required) {
      uint32_t len = 0;
      const char *s = mongocrypt_crypt_shared_lib_version_string (crypt->handle, &len);
      if (!s || len == 0) {
         // empty/null version string indicates that crypt_shared was not loaded
         // by libmongocrypt
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                         MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                         "Option 'cryptSharedLibRequired' is 'true', but failed to "
                         "load the crypt_shared runtime library");
         goto fail;
      }
      mongoc_log (
         MONGOC_LOG_LEVEL_DEBUG, MONGOC_LOG_DOMAIN, "crypt_shared library version '%s' was found and loaded", s);
   }

   crypt->creds_cb = creds_cb;

   success = true;
fail:
   mongocrypt_binary_destroy (local_masterkey_bin);
   mongocrypt_binary_destroy (encrypted_fields_map_bin);
   mongocrypt_binary_destroy (schema_map_bin);
   mongocrypt_binary_destroy (kms_providers_bin);

   if (!success) {
      _mongoc_crypt_destroy (crypt);
      return NULL;
   }

   return crypt;
}

void
_mongoc_crypt_destroy (_mongoc_crypt_t *crypt)
{
   if (!crypt) {
      return;
   }
   mongocrypt_destroy (crypt->handle);
   _mongoc_ssl_opts_cleanup (&crypt->kmip_tls_opt, true /* free_internal */);
   _mongoc_ssl_opts_cleanup (&crypt->aws_tls_opt, true /* free_internal */);
   _mongoc_ssl_opts_cleanup (&crypt->azure_tls_opt, true /* free_internal */);
   _mongoc_ssl_opts_cleanup (&crypt->gcp_tls_opt, true /* free_internal */);
   bson_destroy (&crypt->kms_providers);
   mcd_azure_access_token_destroy (&crypt->azure_token);
   mcd_mapof_kmsid_to_tlsopts_destroy (crypt->kmsid_to_tlsopts);
   bson_free (crypt);
}

bool
_mongoc_crypt_auto_encrypt (_mongoc_crypt_t *crypt,
                            mongoc_collection_t *keyvault_coll,
                            mongoc_client_t *mongocryptd_client,
                            mongoc_client_t *collinfo_client,
                            const char *db_name,
                            const bson_t *cmd_in,
                            bson_t *cmd_out,
                            bson_error_t *error)
{
   _state_machine_t *state_machine = NULL;
   mongocrypt_binary_t *cmd_bin = NULL;
   bool ret = false;

   BSON_ASSERT_PARAM (collinfo_client);
   bson_init (cmd_out);

   state_machine = _state_machine_new (crypt);
   state_machine->keyvault_coll = keyvault_coll;
   state_machine->mongocryptd_client = mongocryptd_client;
   state_machine->collinfo_client = collinfo_client;
   state_machine->db_name = db_name;
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   cmd_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (cmd_in), cmd_in->len);
   if (!mongocrypt_ctx_encrypt_init (state_machine->ctx, db_name, -1, cmd_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (cmd_out);
   if (!_state_machine_run (state_machine, cmd_out, error)) {
      goto fail;
   }

   ret = true;
fail:
   mongocrypt_binary_destroy (cmd_bin);
   _state_machine_destroy (state_machine);
   return ret;
}

bool
_mongoc_crypt_auto_decrypt (_mongoc_crypt_t *crypt,
                            mongoc_collection_t *keyvault_coll,
                            const bson_t *doc_in,
                            bson_t *doc_out,
                            bson_error_t *error)
{
   bool ret = false;
   _state_machine_t *state_machine = NULL;
   mongocrypt_binary_t *doc_bin = NULL;

   bson_init (doc_out);

   state_machine = _state_machine_new (crypt);
   state_machine->keyvault_coll = keyvault_coll;
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   doc_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (doc_in), doc_in->len);
   if (!mongocrypt_ctx_decrypt_init (state_machine->ctx, doc_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (doc_out);
   if (!_state_machine_run (state_machine, doc_out, error)) {
      goto fail;
   }

   ret = true;
fail:
   mongocrypt_binary_destroy (doc_bin);
   _state_machine_destroy (state_machine);
   return ret;
}

// _create_explicit_state_machine_t creates a _state_machine_t for explicit
// encryption. The returned state machine may be used encrypting a value or
// encrypting an expression.
static _state_machine_t *
_create_explicit_state_machine (_mongoc_crypt_t *crypt,
                                mongoc_collection_t *keyvault_coll,
                                const char *algorithm,
                                const bson_value_t *keyid,
                                const char *keyaltname,
                                const char *query_type,
                                const int64_t *contention_factor,
                                const bson_t *range_opts,
                                bson_error_t *error)
{
   BSON_ASSERT_PARAM (crypt);
   BSON_ASSERT_PARAM (keyvault_coll);
   BSON_ASSERT (algorithm || true);
   BSON_ASSERT (keyid || true);
   BSON_ASSERT (keyaltname || true);
   BSON_ASSERT (query_type || true);
   BSON_ASSERT (range_opts || true);
   BSON_ASSERT (error || true);

   _state_machine_t *state_machine = NULL;
   bool ok = false;

   /* Create the context for the operation. */
   state_machine = _state_machine_new (crypt);
   state_machine->keyvault_coll = keyvault_coll;
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   if (!mongocrypt_ctx_setopt_algorithm (state_machine->ctx, algorithm, -1)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (range_opts != NULL) {
      /* mongocrypt error checks and parses range options */
      mongocrypt_binary_t *binary_range_opts =
         mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (range_opts), range_opts->len);
      if (!mongocrypt_ctx_setopt_algorithm_range (state_machine->ctx, binary_range_opts)) {
         mongocrypt_binary_destroy (binary_range_opts);
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
      mongocrypt_binary_destroy (binary_range_opts);
   }

   if (query_type != NULL) {
      if (!mongocrypt_ctx_setopt_query_type (state_machine->ctx, query_type, -1)) {
         goto fail;
      }
   }

   if (contention_factor != NULL) {
      if (!mongocrypt_ctx_setopt_contention_factor (state_machine->ctx, *contention_factor)) {
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
   }

   if (keyaltname) {
      bool keyaltname_ret;
      mongocrypt_binary_t *keyaltname_bin;
      bson_t *keyaltname_doc;

      keyaltname_doc = BCON_NEW ("keyAltName", keyaltname);
      keyaltname_bin =
         mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (keyaltname_doc), keyaltname_doc->len);
      keyaltname_ret = mongocrypt_ctx_setopt_key_alt_name (state_machine->ctx, keyaltname_bin);
      mongocrypt_binary_destroy (keyaltname_bin);
      bson_destroy (keyaltname_doc);
      if (!keyaltname_ret) {
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
   }

   if (keyid && keyid->value_type == BSON_TYPE_BINARY) {
      mongocrypt_binary_t *keyid_bin;
      bool keyid_ret;

      if (keyid->value.v_binary.subtype != BSON_SUBTYPE_UUID) {
         bson_set_error (
            error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG, "keyid must be a UUID");
         goto fail;
      }

      keyid_bin = mongocrypt_binary_new_from_data (keyid->value.v_binary.data, keyid->value.v_binary.data_len);
      keyid_ret = mongocrypt_ctx_setopt_key_id (state_machine->ctx, keyid_bin);
      mongocrypt_binary_destroy (keyid_bin);
      if (!keyid_ret) {
         _ctx_check_error (state_machine->ctx, error, true);
         goto fail;
      }
   }

   ok = true;
fail:
   if (!ok) {
      _state_machine_destroy (state_machine);
      state_machine = NULL;
   }
   return state_machine;
}

bool
_mongoc_crypt_explicit_encrypt (_mongoc_crypt_t *crypt,
                                mongoc_collection_t *keyvault_coll,
                                const char *algorithm,
                                const bson_value_t *keyid,
                                const char *keyaltname,
                                const char *query_type,
                                const int64_t *contention_factor,
                                const bson_t *range_opts,
                                const bson_value_t *value_in,
                                bson_value_t *value_out,
                                bson_error_t *error)
{
   BSON_ASSERT_PARAM (crypt);
   BSON_ASSERT_PARAM (keyvault_coll);
   BSON_ASSERT (algorithm || true);
   BSON_ASSERT (keyid || true);
   BSON_ASSERT (keyaltname || true);
   BSON_ASSERT (query_type || true);
   BSON_ASSERT (range_opts || true);
   BSON_ASSERT_PARAM (value_in);
   BSON_ASSERT_PARAM (value_out);
   BSON_ASSERT (error || true);

   _state_machine_t *state_machine = NULL;
   bson_t *to_encrypt_doc = NULL;
   mongocrypt_binary_t *to_encrypt_bin = NULL;
   bson_iter_t iter;
   bool ret = false;
   bson_t result = BSON_INITIALIZER;

   value_out->value_type = BSON_TYPE_EOD;

   state_machine = _create_explicit_state_machine (
      crypt, keyvault_coll, algorithm, keyid, keyaltname, query_type, contention_factor, range_opts, error);
   if (!state_machine) {
      goto fail;
   }

   to_encrypt_doc = bson_new ();
   BSON_APPEND_VALUE (to_encrypt_doc, "v", value_in);
   to_encrypt_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (to_encrypt_doc), to_encrypt_doc->len);
   if (!mongocrypt_ctx_explicit_encrypt_init (state_machine->ctx, to_encrypt_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (&result);
   if (!_state_machine_run (state_machine, &result, error)) {
      goto fail;
   }

   /* extract value */
   if (!bson_iter_init_find (&iter, &result, "v")) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                      "encrypted result unexpected: no 'v' found");
      goto fail;
   } else {
      const bson_value_t *tmp;

      tmp = bson_iter_value (&iter);
      bson_value_copy (tmp, value_out);
   }

   ret = true;
fail:
   _state_machine_destroy (state_machine);
   mongocrypt_binary_destroy (to_encrypt_bin);
   bson_destroy (to_encrypt_doc);
   bson_destroy (&result);
   return ret;
}

bool
_mongoc_crypt_explicit_encrypt_expression (_mongoc_crypt_t *crypt,
                                           mongoc_collection_t *keyvault_coll,
                                           const char *algorithm,
                                           const bson_value_t *keyid,
                                           const char *keyaltname,
                                           const char *query_type,
                                           const int64_t *contention_factor,
                                           const bson_t *range_opts,
                                           const bson_t *expr_in,
                                           bson_t *expr_out,
                                           bson_error_t *error)
{
   BSON_ASSERT_PARAM (crypt);
   BSON_ASSERT_PARAM (keyvault_coll);
   BSON_ASSERT (algorithm || true);
   BSON_ASSERT (keyid || true);
   BSON_ASSERT (keyaltname || true);
   BSON_ASSERT (query_type || true);
   BSON_ASSERT (range_opts || true);
   BSON_ASSERT_PARAM (expr_in);
   BSON_ASSERT_PARAM (expr_out);
   BSON_ASSERT (error || true);

   _state_machine_t *state_machine = NULL;
   bson_t *to_encrypt_doc = NULL;
   mongocrypt_binary_t *to_encrypt_bin = NULL;
   bson_iter_t iter;
   bool ret = false;
   bson_t result = BSON_INITIALIZER;

   bson_init (expr_out);

   state_machine = _create_explicit_state_machine (
      crypt, keyvault_coll, algorithm, keyid, keyaltname, query_type, contention_factor, range_opts, error);
   if (!state_machine) {
      goto fail;
   }

   to_encrypt_doc = bson_new ();
   BSON_APPEND_DOCUMENT (to_encrypt_doc, "v", expr_in);
   to_encrypt_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (to_encrypt_doc), to_encrypt_doc->len);
   if (!mongocrypt_ctx_explicit_encrypt_expression_init (state_machine->ctx, to_encrypt_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (&result);
   if (!_state_machine_run (state_machine, &result, error)) {
      goto fail;
   }

   /* extract document */
   if (!bson_iter_init_find (&iter, &result, "v")) {
      bson_set_error (error,
                      MONGOC_ERROR_CLIENT,
                      MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                      "encrypted result unexpected: no 'v' found");
      goto fail;
   } else {
      bson_t tmp;

      if (!BSON_ITER_HOLDS_DOCUMENT (&iter)) {
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE,
                         "encrypted result unexpected: 'v' is not a document, got: %s",
                         _mongoc_bson_type_to_str (bson_iter_type (&iter)));
         goto fail;
      }

      if (!_mongoc_iter_document_as_bson (&iter, &tmp, error)) {
         goto fail;
      }

      bson_copy_to (&tmp, expr_out);
   }

   ret = true;
fail:
   _state_machine_destroy (state_machine);
   mongocrypt_binary_destroy (to_encrypt_bin);
   bson_destroy (to_encrypt_doc);
   bson_destroy (&result);
   return ret;
}

bool
_mongoc_crypt_explicit_decrypt (_mongoc_crypt_t *crypt,
                                mongoc_collection_t *keyvault_coll,
                                const bson_value_t *value_in,
                                bson_value_t *value_out,
                                bson_error_t *error)
{
   _state_machine_t *state_machine = NULL;
   bson_t *to_decrypt_doc = NULL;
   mongocrypt_binary_t *to_decrypt_bin = NULL;
   bson_iter_t iter;
   bool ret = false;
   bson_t result = BSON_INITIALIZER;

   state_machine = _state_machine_new (crypt);
   state_machine->keyvault_coll = keyvault_coll;
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   to_decrypt_doc = bson_new ();
   BSON_APPEND_VALUE (to_decrypt_doc, "v", value_in);
   to_decrypt_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (to_decrypt_doc), to_decrypt_doc->len);
   if (!mongocrypt_ctx_explicit_decrypt_init (state_machine->ctx, to_decrypt_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (&result);
   if (!_state_machine_run (state_machine, &result, error)) {
      goto fail;
   }

   /* extract value */
   if (!bson_iter_init_find (&iter, &result, "v")) {
      bson_set_error (
         error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE, "decrypted result unexpected");
      goto fail;
   } else {
      const bson_value_t *tmp;

      tmp = bson_iter_value (&iter);
      bson_value_copy (tmp, value_out);
   }

   ret = true;
fail:
   _state_machine_destroy (state_machine);
   mongocrypt_binary_destroy (to_decrypt_bin);
   bson_destroy (to_decrypt_doc);
   bson_destroy (&result);
   return ret;
}

bool
_mongoc_crypt_create_datakey (_mongoc_crypt_t *crypt,
                              const char *kms_provider,
                              const bson_t *masterkey,
                              char **keyaltnames,
                              uint32_t keyaltnames_count,
                              const uint8_t *keymaterial,
                              uint32_t keymaterial_len,
                              bson_t *doc_out,
                              bson_error_t *error)
{
   _state_machine_t *state_machine = NULL;
   bool ret = false;
   bson_t masterkey_w_provider = BSON_INITIALIZER;
   mongocrypt_binary_t *masterkey_w_provider_bin = NULL;

   bson_init (doc_out);
   state_machine = _state_machine_new (crypt);
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   BSON_APPEND_UTF8 (&masterkey_w_provider, "provider", kms_provider);
   if (masterkey) {
      bson_concat (&masterkey_w_provider, masterkey);
   }
   masterkey_w_provider_bin =
      mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (&masterkey_w_provider), masterkey_w_provider.len);

   if (!mongocrypt_ctx_setopt_key_encryption_key (state_machine->ctx, masterkey_w_provider_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   if (keyaltnames) {
      for (uint32_t i = 0u; i < keyaltnames_count; i++) {
         bool keyaltname_ret;
         mongocrypt_binary_t *keyaltname_bin;
         bson_t *keyaltname_doc;

         keyaltname_doc = BCON_NEW ("keyAltName", keyaltnames[i]);
         keyaltname_bin =
            mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (keyaltname_doc), keyaltname_doc->len);
         keyaltname_ret = mongocrypt_ctx_setopt_key_alt_name (state_machine->ctx, keyaltname_bin);
         mongocrypt_binary_destroy (keyaltname_bin);
         bson_destroy (keyaltname_doc);
         if (!keyaltname_ret) {
            _ctx_check_error (state_machine->ctx, error, true);
            goto fail;
         }
      }
   }

   if (keymaterial) {
      bson_t *const bson = BCON_NEW ("keyMaterial", BCON_BIN (BSON_SUBTYPE_BINARY, keymaterial, keymaterial_len));
      mongocrypt_binary_t *const bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (bson), bson->len);

      mongocrypt_ctx_setopt_key_material (state_machine->ctx, bin);

      bson_destroy (bson);
      mongocrypt_binary_destroy (bin);
   }

   if (!mongocrypt_ctx_datakey_init (state_machine->ctx)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (doc_out);
   if (!_state_machine_run (state_machine, doc_out, error)) {
      goto fail;
   }

   ret = true;

fail:
   bson_destroy (&masterkey_w_provider);
   mongocrypt_binary_destroy (masterkey_w_provider_bin);
   _state_machine_destroy (state_machine);
   return ret;
}

bool
_mongoc_crypt_rewrap_many_datakey (_mongoc_crypt_t *crypt,
                                   mongoc_collection_t *keyvault_coll,
                                   const bson_t *filter,
                                   const char *provider,
                                   const bson_t *master_key,
                                   bson_t *doc_out,
                                   bson_error_t *error)
{
   _state_machine_t *state_machine = NULL;
   const bson_t empty_bson = BSON_INITIALIZER;
   mongocrypt_binary_t *filter_bin = NULL;
   bool ret = false;

   // Caller must ensure `provider` is provided alongside `master_key`.
   BSON_ASSERT (!master_key || provider);

   bson_init (doc_out);
   state_machine = _state_machine_new (crypt);
   state_machine->keyvault_coll = keyvault_coll;
   state_machine->ctx = mongocrypt_ctx_new (crypt->handle);
   if (!state_machine->ctx) {
      _crypt_check_error (crypt->handle, error, true);
      goto fail;
   }

   {
      bson_t new_provider = BSON_INITIALIZER;
      mongocrypt_binary_t *new_provider_bin = NULL;
      bool success = true;

      if (provider) {
         BSON_APPEND_UTF8 (&new_provider, "provider", provider);

         if (master_key) {
            bson_concat (&new_provider, master_key);
         }

         new_provider_bin =
            mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (&new_provider), new_provider.len);

         if (!mongocrypt_ctx_setopt_key_encryption_key (state_machine->ctx, new_provider_bin)) {
            _ctx_check_error (state_machine->ctx, error, true);
            success = false;
         }

         mongocrypt_binary_destroy (new_provider_bin);
      }

      bson_destroy (&new_provider);

      if (!success) {
         goto fail;
      }
   }

   if (!filter) {
      filter = &empty_bson;
   }

   filter_bin = mongocrypt_binary_new_from_data ((uint8_t *) bson_get_data (filter), filter->len);

   if (!mongocrypt_ctx_rewrap_many_datakey_init (state_machine->ctx, filter_bin)) {
      _ctx_check_error (state_machine->ctx, error, true);
      goto fail;
   }

   bson_destroy (doc_out);
   if (!_state_machine_run (state_machine, doc_out, error)) {
      goto fail;
   }

   ret = true;

fail:
   mongocrypt_binary_destroy (filter_bin);
   _state_machine_destroy (state_machine);

   return ret;
}

const char *
_mongoc_crypt_get_crypt_shared_version (const _mongoc_crypt_t *crypt)
{
   return mongocrypt_crypt_shared_lib_version_string (crypt->handle, NULL);
}

#else
/* ensure the translation unit is not empty */
extern int no_mongoc_client_side_encryption;
#endif /* MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION */
