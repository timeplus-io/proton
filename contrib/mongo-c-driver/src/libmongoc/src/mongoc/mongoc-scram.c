/* Copyright 2014 MongoDB, Inc.
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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_CRYPTO

#include <string.h>

#include "mongoc-error.h"
#include "mongoc-scram-private.h"
#include "mongoc-rand-private.h"
#include "mongoc-util-private.h"
#include "mongoc-trace-private.h"

#include "mongoc-crypto-private.h"
#include "common-b64-private.h"

#include "mongoc-memcmp-private.h"
#include "common-thread-private.h"
#include <utf8proc.h>

typedef struct _mongoc_scram_cache_entry_t {
   /* book keeping */
   bool taken;
   /* pre-secrets */
   char hashed_password[MONGOC_SCRAM_HASH_MAX_SIZE];
   uint8_t decoded_salt[MONGOC_SCRAM_B64_HASH_MAX_SIZE];
   uint32_t iterations;
   /* secrets */
   uint8_t client_key[MONGOC_SCRAM_HASH_MAX_SIZE];
   uint8_t server_key[MONGOC_SCRAM_HASH_MAX_SIZE];
   uint8_t salted_password[MONGOC_SCRAM_HASH_MAX_SIZE];
} mongoc_scram_cache_entry_t;

#define MONGOC_SCRAM_SERVER_KEY "Server Key"
#define MONGOC_SCRAM_CLIENT_KEY "Client Key"

/* returns true if the first UTF-8 code point in `s` is valid. */
bool
_mongoc_utf8_first_code_point_is_valid (const char *c, size_t length);

/* returns whether a character is between two limits (inclusive). */
bool
_mongoc_utf8_code_unit_in_range (const uint8_t c, const uint8_t lower, const uint8_t upper);

/* returns whether a codepoint exists in the specified table. The table format
 * is that the 2*n element is the lower bound and the 2*n + 1 is the upper bound
 * (both inclusive). */
bool
_mongoc_utf8_code_point_is_in_table (uint32_t code, const uint32_t *table, size_t size);

/* returns the byte length of the UTF-8 code point. Returns -1 if `c` is not a
 * valid UTF-8 code point. */
ssize_t
_mongoc_utf8_code_point_length (uint32_t c);

/* converts a Unicode code point to UTF-8 character. Returns how many bytes the
 * character converted is. Returns -1 if the code point is invalid.
 * char *out must be large enough to contain all of the code units written to
 * it. */
ssize_t
_mongoc_utf8_code_point_to_str (uint32_t c, char *out);

static bson_shared_mutex_t g_scram_cache_rwlock;

static bson_once_t init_cache_once_control = BSON_ONCE_INIT;
static bson_mutex_t clear_cache_lock;

/*
 * Cache lookups are a linear search through this table. This table is a
 * constant size, which is small enough that lookup cost is insignificant.
 *
 * This can be refactored into a hashmap if the cache size needs to grow larger
 * in the future, but a linear lookup is currently fast enough and is much
 * simpler logic to reason about.
 */
static mongoc_scram_cache_entry_t g_scram_cache[MONGOC_SCRAM_CACHE_SIZE];

static void
_mongoc_scram_cache_clear (void)
{
   bson_mutex_lock (&clear_cache_lock);
   memset (g_scram_cache, 0, sizeof (g_scram_cache));
   bson_mutex_unlock (&clear_cache_lock);
}

static BSON_ONCE_FUN (_mongoc_scram_cache_init)
{
   bson_shared_mutex_init (&g_scram_cache_rwlock);
   bson_mutex_init (&clear_cache_lock);
   _mongoc_scram_cache_clear ();

   BSON_ONCE_RETURN;
}

static void
_mongoc_scram_cache_init_once (void)
{
   bson_once (&init_cache_once_control, _mongoc_scram_cache_init);
}

static int
_scram_hash_size (mongoc_scram_t *scram)
{
   if (scram->crypto.algorithm == MONGOC_CRYPTO_ALGORITHM_SHA_1) {
      return MONGOC_SCRAM_SHA_1_HASH_SIZE;
   } else if (scram->crypto.algorithm == MONGOC_CRYPTO_ALGORITHM_SHA_256) {
      return MONGOC_SCRAM_SHA_256_HASH_SIZE;
   }
   return 0;
}

/* Copies the cache's secrets to scram */
static void
_mongoc_scram_cache_apply_secrets (mongoc_scram_cache_entry_t *cache, mongoc_scram_t *scram)
{
   BSON_ASSERT (cache);
   BSON_ASSERT (scram);

   memcpy (scram->client_key, cache->client_key, sizeof (scram->client_key));
   memcpy (scram->server_key, cache->server_key, sizeof (scram->server_key));
   memcpy (scram->salted_password, cache->salted_password, sizeof (scram->salted_password));
}


void
_mongoc_scram_cache_destroy (mongoc_scram_cache_entry_t *cache)
{
   BSON_ASSERT (cache);
   bson_free (cache);
}


/*
 * Checks whether the cache contains scram's pre-secrets.
 * Populate `cache` with the values found in the global cache if found.
 */
static bool
_mongoc_scram_cache_has_presecrets (mongoc_scram_cache_entry_t *cache /* out */, const mongoc_scram_t *scram)
{
   bool cache_hit = false;

   BSON_ASSERT (cache);
   BSON_ASSERT (scram);

   _mongoc_scram_cache_init_once ();

   /*
    * - Take a read lock
    * - Search through g_scram_cache if the hashed_password, decoded_salt, and
    *   iterations match an entry.
    * - If so, then return true
    * - Otherwise return false
    */
   bson_shared_mutex_lock_shared (&g_scram_cache_rwlock);

   for (size_t i = 0; i < MONGOC_SCRAM_CACHE_SIZE; i++) {
      if (g_scram_cache[i].taken) {
         mongoc_scram_cache_entry_t *cache_entry = &g_scram_cache[i];
         cache_hit = !strcmp (cache_entry->hashed_password, scram->hashed_password) &&
                     cache_entry->iterations == scram->iterations &&
                     !memcmp (cache_entry->decoded_salt, scram->decoded_salt, sizeof (cache_entry->decoded_salt));
         if (cache_hit) {
            /* copy the found cache items into the 'cache' output parameter */
            memcpy (cache->client_key, cache_entry->client_key, sizeof (cache->client_key));
            memcpy (cache->server_key, cache_entry->server_key, sizeof (cache->server_key));
            memcpy (cache->salted_password, cache_entry->salted_password, sizeof (cache->salted_password));
            goto done;
         }
      }
   }

done:
   bson_shared_mutex_unlock_shared (&g_scram_cache_rwlock);
   return cache_hit;
}


void
_mongoc_scram_set_pass (mongoc_scram_t *scram, const char *pass)
{
   BSON_ASSERT (scram);

   if (scram->pass) {
      bson_zero_free (scram->pass, strlen (scram->pass));
   }

   scram->pass = pass ? bson_strdup (pass) : NULL;
}


void
_mongoc_scram_set_user (mongoc_scram_t *scram, const char *user)
{
   BSON_ASSERT (scram);

   bson_free (scram->user);
   scram->user = user ? bson_strdup (user) : NULL;
}


void
_mongoc_scram_init (mongoc_scram_t *scram, mongoc_crypto_hash_algorithm_t algo)
{
   BSON_ASSERT (scram);

   memset (scram, 0, sizeof *scram);

   mongoc_crypto_init (&scram->crypto, algo);
}


void
_mongoc_scram_destroy (mongoc_scram_t *scram)
{
   BSON_ASSERT (scram);

   bson_free (scram->user);

   if (scram->pass) {
      bson_zero_free (scram->pass, strlen (scram->pass));
   }

   memset (scram->hashed_password, 0, sizeof (scram->hashed_password));

   bson_free (scram->auth_message);

   memset (scram, 0, sizeof *scram);
}

static void
_mongoc_scram_cache_insert (const mongoc_scram_t *scram)
{
   bson_shared_mutex_lock (&g_scram_cache_rwlock);

again:
   for (size_t i = 0; i < MONGOC_SCRAM_CACHE_SIZE; i++) {
      mongoc_scram_cache_entry_t *cache_entry = &g_scram_cache[i];
      bool already_exists =
         !strcmp (cache_entry->hashed_password, scram->hashed_password) &&
         cache_entry->iterations == scram->iterations &&
         !memcmp (cache_entry->decoded_salt, scram->decoded_salt, sizeof (cache_entry->decoded_salt)) &&
         !memcmp (cache_entry->client_key, scram->client_key, sizeof (cache_entry->client_key)) &&
         !memcmp (cache_entry->server_key, scram->server_key, sizeof (cache_entry->server_key)) &&
         !memcmp (cache_entry->salted_password, scram->salted_password, sizeof (cache_entry->salted_password));

      if (already_exists) {
         /* cache entry already populated between read and write lock
          * acquisition, skipping */
         break;
      }

      if (!cache_entry->taken) {
         /* found an empty slot */
         memcpy (cache_entry->client_key, scram->client_key, sizeof (cache_entry->client_key));
         memcpy (cache_entry->server_key, scram->server_key, sizeof (cache_entry->server_key));
         memcpy (cache_entry->salted_password, scram->salted_password, sizeof (cache_entry->salted_password));
         memcpy (cache_entry->decoded_salt, scram->decoded_salt, sizeof (cache_entry->decoded_salt));
         memcpy (cache_entry->hashed_password, scram->hashed_password, sizeof (cache_entry->hashed_password));
         cache_entry->iterations = scram->iterations;
         cache_entry->taken = true;
         break;
      }

      /* if cache is full, then invalidate the cache and insert again */
      if (i == (MONGOC_SCRAM_CACHE_SIZE - 1)) {
         _mongoc_scram_cache_clear ();
         goto again;
      }
   }

   bson_shared_mutex_unlock (&g_scram_cache_rwlock);
}

/* Updates the cache with scram's last-used pre-secrets and secrets */
static void
_mongoc_scram_update_cache (const mongoc_scram_t *scram)
{
   mongoc_scram_cache_entry_t cache;
   bool found = _mongoc_scram_cache_has_presecrets (&cache, scram);
   if (!found) {
      /* cache miss, insert this as a new cache entry */
      _mongoc_scram_cache_insert (scram);
   }
}


static bool
_mongoc_scram_buf_write (const char *src, int32_t src_len, uint8_t *outbuf, uint32_t outbufmax, uint32_t *outbuflen)
{
   if (src_len < 0) {
      src_len = (int32_t) strlen (src);
   }

   if (*outbuflen + src_len >= outbufmax) {
      return false;
   }

   memcpy (outbuf + *outbuflen, src, src_len);

   *outbuflen += src_len;

   return true;
}


/* generate client-first-message:
 * n,a=authzid,n=encoded-username,r=client-nonce
 *
 * note that a= is optional, so we aren't dealing with that here
 */
static bool
_mongoc_scram_start (
   mongoc_scram_t *scram, uint8_t *outbuf, uint32_t outbufmax, uint32_t *outbuflen, bson_error_t *error)
{
   uint8_t nonce[24];
   const char *ptr;
   bool rval = true;

   BSON_ASSERT (scram);
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbufmax);
   BSON_ASSERT (outbuflen);

   if (!scram->user) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: username is not set");
      goto FAIL;
   }

   if (!scram->pass) {
      // Apply an empty string as a default.
      scram->pass = bson_strdup ("");
   }

   /* auth message is as big as the outbuf just because */
   scram->auth_message = (uint8_t *) bson_malloc (outbufmax);
   scram->auth_messagemax = outbufmax;

   /* the server uses a 24 byte random nonce.  so we do as well */
   if (1 != _mongoc_rand_bytes (nonce, sizeof (nonce))) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: could not generate a cryptographically "
                      "secure nonce in sasl step 1");
      goto FAIL;
   }

   scram->encoded_nonce_len =
      mcommon_b64_ntop (nonce, sizeof (nonce), scram->encoded_nonce, sizeof (scram->encoded_nonce));

   if (-1 == scram->encoded_nonce_len) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: could not encode nonce");
      goto FAIL;
   }

   if (!_mongoc_scram_buf_write ("n,,n=", -1, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   for (ptr = scram->user; *ptr; ptr++) {
      /* RFC 5802 specifies that ',' and '=' and encoded as '=2C' and '=3D'
       * respectively in the user name */
      switch (*ptr) {
      case ',':

         if (!_mongoc_scram_buf_write ("=2C", -1, outbuf, outbufmax, outbuflen)) {
            goto BUFFER;
         }

         break;
      case '=':

         if (!_mongoc_scram_buf_write ("=3D", -1, outbuf, outbufmax, outbuflen)) {
            goto BUFFER;
         }

         break;
      default:

         if (!_mongoc_scram_buf_write (ptr, 1, outbuf, outbufmax, outbuflen)) {
            goto BUFFER;
         }

         break;
      }
   }

   if (!_mongoc_scram_buf_write (",r=", -1, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   if (!_mongoc_scram_buf_write (scram->encoded_nonce, scram->encoded_nonce_len, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   /* we have to keep track of the conversation to create a client proof later
    * on.  This copies the message we're crafting from the 'n=' portion onwards
    * into a buffer we're managing */
   if (!_mongoc_scram_buf_write (
          (char *) outbuf + 3, *outbuflen - 3, scram->auth_message, scram->auth_messagemax, &scram->auth_messagelen)) {
      goto BUFFER_AUTH;
   }

   if (!_mongoc_scram_buf_write (",", -1, scram->auth_message, scram->auth_messagemax, &scram->auth_messagelen)) {
      goto BUFFER_AUTH;
   }

   goto CLEANUP;

BUFFER_AUTH:
   bson_set_error (error,
                   MONGOC_ERROR_SCRAM,
                   MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                   "SCRAM Failure: could not buffer auth message in sasl step1");

   goto FAIL;

BUFFER:
   bson_set_error (
      error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: could not buffer sasl step1");

   goto FAIL;

FAIL:
   rval = false;

CLEANUP:

   return rval;
}


/* Compute the SCRAM step Hi() as defined in RFC5802 */
static void
_mongoc_scram_salt_password (mongoc_scram_t *scram,
                             const char *password,
                             uint32_t password_len,
                             const uint8_t *salt,
                             uint32_t salt_len,
                             uint32_t iterations)
{
   uint8_t intermediate_digest[MONGOC_SCRAM_HASH_MAX_SIZE];
   uint8_t start_key[MONGOC_SCRAM_HASH_MAX_SIZE];

   uint8_t *output = scram->salted_password;

   memcpy (start_key, salt, salt_len);

   start_key[salt_len] = 0;
   start_key[salt_len + 1] = 0;
   start_key[salt_len + 2] = 0;
   start_key[salt_len + 3] = 1;

   mongoc_crypto_hmac (&scram->crypto, password, password_len, start_key, _scram_hash_size (scram), output);

   memcpy (intermediate_digest, output, _scram_hash_size (scram));

   /* intermediateDigest contains Ui and output contains the accumulated XOR:ed
    * result */
   for (uint32_t i = 2u; i <= iterations; i++) {
      const int hash_size = _scram_hash_size (scram);

      mongoc_crypto_hmac (&scram->crypto, password, password_len, intermediate_digest, hash_size, intermediate_digest);

      for (int k = 0; k < hash_size; k++) {
         output[k] ^= intermediate_digest[k];
      }
   }
}


static bool
_mongoc_scram_generate_client_proof (mongoc_scram_t *scram, uint8_t *outbuf, uint32_t outbufmax, uint32_t *outbuflen)
{
   uint8_t stored_key[MONGOC_SCRAM_HASH_MAX_SIZE];
   uint8_t client_signature[MONGOC_SCRAM_HASH_MAX_SIZE];
   unsigned char client_proof[MONGOC_SCRAM_HASH_MAX_SIZE];
   int i;
   int r = 0;

   if (!*scram->client_key) {
      /* ClientKey := HMAC(saltedPassword, "Client Key") */
      mongoc_crypto_hmac (&scram->crypto,
                          scram->salted_password,
                          _scram_hash_size (scram),
                          (uint8_t *) MONGOC_SCRAM_CLIENT_KEY,
                          (int) strlen (MONGOC_SCRAM_CLIENT_KEY),
                          scram->client_key);
   }

   /* StoredKey := H(client_key) */
   mongoc_crypto_hash (&scram->crypto, scram->client_key, (size_t) _scram_hash_size (scram), stored_key);

   /* ClientSignature := HMAC(StoredKey, AuthMessage) */
   mongoc_crypto_hmac (&scram->crypto,
                       stored_key,
                       _scram_hash_size (scram),
                       scram->auth_message,
                       scram->auth_messagelen,
                       client_signature);

   /* ClientProof := ClientKey XOR ClientSignature */

   for (i = 0; i < _scram_hash_size (scram); i++) {
      client_proof[i] = scram->client_key[i] ^ client_signature[i];
   }

   r = mcommon_b64_ntop (client_proof, _scram_hash_size (scram), (char *) outbuf + *outbuflen, outbufmax - *outbuflen);

   if (-1 == r) {
      return false;
   }

   *outbuflen += r;

   return true;
}


/* Parse server-first-message of the form:
 * r=client-nonce|server-nonce,s=user-salt,i=iteration-count
 *
 * Generate client-final-message of the form:
 * c=channel-binding(base64),r=client-nonce|server-nonce,p=client-proof
 */
static bool
_mongoc_scram_step2 (mongoc_scram_t *scram,
                     const uint8_t *inbuf,
                     uint32_t inbuflen,
                     uint8_t *outbuf,
                     uint32_t outbufmax,
                     uint32_t *outbuflen,
                     bson_error_t *error)
{
   uint8_t *val_r = NULL;
   uint32_t val_r_len = 0u;
   uint8_t *val_s = NULL;
   uint32_t val_s_len = 0u;
   uint8_t *val_i = NULL;
   uint32_t val_i_len = 0u;

   uint8_t **current_val = NULL;
   uint32_t *current_val_len = NULL;

   char *tmp = NULL;
   char *hashed_password = NULL;

   uint8_t decoded_salt[MONGOC_SCRAM_B64_HASH_MAX_SIZE] = {0};
   int32_t decoded_salt_len = 0;
   /* the decoded salt leaves four trailing bytes to add the int32 0x00000001 */
   const int32_t expected_salt_length = _scram_hash_size (scram) - 4;
   bool rval = true;

   int iterations = 0;


   BSON_ASSERT (scram);
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbufmax);
   BSON_ASSERT (outbuflen);

   if (scram->crypto.algorithm == MONGOC_CRYPTO_ALGORITHM_SHA_1) {
      /* Auth spec for SCRAM-SHA-1: "The password variable MUST be the mongodb
       * hashed variant. The mongo hashed variant is computed as hash = HEX(
       * MD5( UTF8( username + ':mongo:' + plain_text_password )))" */
      tmp = bson_strdup_printf ("%s:mongo:%s", scram->user, scram->pass);
      hashed_password = _mongoc_hex_md5 (tmp);
      bson_zero_free (tmp, strlen (tmp));
   } else if (scram->crypto.algorithm == MONGOC_CRYPTO_ALGORITHM_SHA_256) {
      /* Auth spec for SCRAM-SHA-256: "Passwords MUST be prepared with SASLprep,
       * per RFC 5802. Passwords are used directly for key derivation; they
       * MUST NOT be digested as they are in SCRAM-SHA-1." */
      hashed_password = _mongoc_sasl_prep (scram->pass, error);
      if (!hashed_password) {
         goto FAIL;
      }
   } else {
      BSON_ASSERT (false);
   }

   /* we need all of the incoming message for the final client proof */
   if (!_mongoc_scram_buf_write (
          (char *) inbuf, inbuflen, scram->auth_message, scram->auth_messagemax, &scram->auth_messagelen)) {
      goto BUFFER_AUTH;
   }

   if (!_mongoc_scram_buf_write (",", -1, scram->auth_message, scram->auth_messagemax, &scram->auth_messagelen)) {
      goto BUFFER_AUTH;
   }

   for (const uint8_t *ptr = inbuf; ptr < inbuf + inbuflen;) {
      switch (*ptr) {
      case 'r':
         current_val = &val_r;
         current_val_len = &val_r_len;
         break;
      case 's':
         current_val = &val_s;
         current_val_len = &val_s_len;
         break;
      case 'i':
         current_val = &val_i;
         current_val_len = &val_i_len;
         break;
      default:
         bson_set_error (error,
                         MONGOC_ERROR_SCRAM,
                         MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                         "SCRAM Failure: unknown key (%c) in sasl step 2",
                         *ptr);
         goto FAIL;
      }

      ptr++;

      if (*ptr != '=') {
         bson_set_error (error,
                         MONGOC_ERROR_SCRAM,
                         MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                         "SCRAM Failure: invalid parse state in sasl step 2");

         goto FAIL;
      }

      ptr++;

      const uint8_t *const next_comma = (const uint8_t *) memchr (ptr, ',', (inbuf + inbuflen) - ptr);

      if (next_comma) {
         *current_val_len = (uint32_t) (next_comma - ptr);
      } else {
         *current_val_len = (uint32_t) ((inbuf + inbuflen) - ptr);
      }

      *current_val = (uint8_t *) bson_malloc (*current_val_len + 1);
      memcpy (*current_val, ptr, *current_val_len);
      (*current_val)[*current_val_len] = '\0';

      if (next_comma) {
         ptr = next_comma + 1;
      } else {
         break;
      }
   }

   if (!val_r) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: no r param in sasl step 2");

      goto FAIL;
   }

   if (!val_s) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: no s param in sasl step 2");

      goto FAIL;
   }

   if (!val_i) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: no i param in sasl step 2");

      goto FAIL;
   }

   /* verify our nonce */
   if (bson_cmp_less_us (val_r_len, scram->encoded_nonce_len) ||
       mongoc_memcmp (val_r, scram->encoded_nonce, scram->encoded_nonce_len)) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: client nonce not repeated in sasl step 2");
   }

   *outbuflen = 0;

   if (!_mongoc_scram_buf_write ("c=biws,r=", -1, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   if (!_mongoc_scram_buf_write ((char *) val_r, val_r_len, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   if (!_mongoc_scram_buf_write (
          (char *) outbuf, *outbuflen, scram->auth_message, scram->auth_messagemax, &scram->auth_messagelen)) {
      goto BUFFER_AUTH;
   }

   if (!_mongoc_scram_buf_write (",p=", -1, outbuf, outbufmax, outbuflen)) {
      goto BUFFER;
   }

   decoded_salt_len = mcommon_b64_pton ((char *) val_s, decoded_salt, sizeof (decoded_salt));

   if (-1 == decoded_salt_len) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: unable to decode salt in sasl step2");
      goto FAIL;
   }

   if (expected_salt_length != decoded_salt_len) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: invalid salt length of %d in sasl step2",
                      decoded_salt_len);
      goto FAIL;
   }

   iterations = (int) bson_ascii_strtoll ((char *) val_i, &tmp, 10);
   /* tmp holds the location of the failed to parse character.  So if it's
    * null, we got to the end of the string and didn't have a parse error */

   if (*tmp) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: unable to parse iterations in sasl step2");
      goto FAIL;
   }

   if (iterations < 0) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: iterations is negative in sasl step2");
      goto FAIL;
   }

   /* drivers MUST enforce a minimum iteration count of 4096 and MUST error if
    * the authentication conversation specifies a lower count. This mitigates
    * downgrade attacks by a man-in-the-middle attacker. */
   if (iterations < 4096) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: iterations must be at least 4096");
      goto FAIL;
   }

   /* Save the presecrets for caching */
   if (hashed_password) {
      bson_strncpy (scram->hashed_password, hashed_password, sizeof (scram->hashed_password));
   }

   scram->iterations = iterations;
   memcpy (scram->decoded_salt, decoded_salt, sizeof (scram->decoded_salt));

   mongoc_scram_cache_entry_t cache;
   if (_mongoc_scram_cache_has_presecrets (&cache, scram)) {
      _mongoc_scram_cache_apply_secrets (&cache, scram);
   }

   if (!*scram->salted_password) {
      _mongoc_scram_salt_password (scram,
                                   hashed_password,
                                   (uint32_t) strlen (hashed_password),
                                   decoded_salt,
                                   decoded_salt_len,
                                   (uint32_t) iterations);
   }

   _mongoc_scram_generate_client_proof (scram, outbuf, outbufmax, outbuflen);

   goto CLEANUP;

BUFFER_AUTH:
   bson_set_error (error,
                   MONGOC_ERROR_SCRAM,
                   MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                   "SCRAM Failure: could not buffer auth message in sasl step2");

   goto FAIL;

BUFFER:
   bson_set_error (
      error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: could not buffer sasl step2");

   goto FAIL;

FAIL:
   rval = false;

CLEANUP:
   bson_free (val_r);
   bson_free (val_s);
   bson_free (val_i);

   if (hashed_password) {
      bson_zero_free (hashed_password, strlen (hashed_password));
   }

   return rval;
}


static bool
_mongoc_scram_verify_server_signature (mongoc_scram_t *scram, uint8_t *verification, uint32_t len)
{
   char encoded_server_signature[MONGOC_SCRAM_B64_HASH_MAX_SIZE];
   int32_t encoded_server_signature_len;
   uint8_t server_signature[MONGOC_SCRAM_HASH_MAX_SIZE];

   if (!*scram->server_key) {
      const size_t key_len = strlen (MONGOC_SCRAM_SERVER_KEY);
      BSON_ASSERT (bson_in_range_unsigned (int, key_len));

      /* ServerKey := HMAC(SaltedPassword, "Server Key") */
      mongoc_crypto_hmac (&scram->crypto,
                          scram->salted_password,
                          _scram_hash_size (scram),
                          (uint8_t *) MONGOC_SCRAM_SERVER_KEY,
                          (int) key_len,
                          scram->server_key);
   }

   /* ServerSignature := HMAC(ServerKey, AuthMessage) */
   mongoc_crypto_hmac (&scram->crypto,
                       scram->server_key,
                       _scram_hash_size (scram),
                       scram->auth_message,
                       scram->auth_messagelen,
                       server_signature);

   encoded_server_signature_len = mcommon_b64_ntop (
      server_signature, _scram_hash_size (scram), encoded_server_signature, sizeof (encoded_server_signature));
   if (encoded_server_signature_len == -1) {
      return false;
   }

   return (len == encoded_server_signature_len) && (mongoc_memcmp (verification, encoded_server_signature, len) == 0);
}


static bool
_mongoc_scram_step3 (mongoc_scram_t *scram,
                     const uint8_t *inbuf,
                     uint32_t inbuflen,
                     uint8_t *outbuf,
                     uint32_t outbufmax,
                     uint32_t *outbuflen,
                     bson_error_t *error)
{
   uint8_t *val_e = NULL;
   uint32_t val_e_len = 0;
   uint8_t *val_v = NULL;
   uint32_t val_v_len = 0;

   uint8_t **current_val;
   uint32_t *current_val_len = 0;

   bool rval = true;

   BSON_ASSERT (scram);
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbufmax);
   BSON_ASSERT (outbuflen);

   for (const uint8_t *ptr = inbuf; ptr < inbuf + inbuflen;) {
      switch (*ptr) {
      case 'e':
         current_val = &val_e;
         current_val_len = &val_e_len;
         break;
      case 'v':
         current_val = &val_v;
         current_val_len = &val_v_len;
         break;
      default:
         bson_set_error (error,
                         MONGOC_ERROR_SCRAM,
                         MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                         "SCRAM Failure: unknown key (%c) in sasl step 3",
                         *ptr);
         goto FAIL;
      }

      ptr++;

      if (*ptr != '=') {
         bson_set_error (error,
                         MONGOC_ERROR_SCRAM,
                         MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                         "SCRAM Failure: invalid parse state in sasl step 3");
         goto FAIL;
      }

      ptr++;

      const uint8_t *const next_comma = (const uint8_t *) memchr (ptr, ',', (inbuf + inbuflen) - ptr);

      if (next_comma) {
         *current_val_len = (uint32_t) (next_comma - ptr);
      } else {
         *current_val_len = (uint32_t) ((inbuf + inbuflen) - ptr);
      }

      *current_val = (uint8_t *) bson_malloc (*current_val_len + 1);
      memcpy (*current_val, ptr, *current_val_len);
      (*current_val)[*current_val_len] = '\0';

      if (next_comma) {
         ptr = next_comma + 1;
      } else {
         break;
      }
   }

   *outbuflen = 0;

   if (val_e) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: authentication failure in sasl step 3 : %s",
                      val_e);
      goto FAIL;
   }

   if (!val_v) {
      bson_set_error (
         error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, "SCRAM Failure: no v param in sasl step 3");
      goto FAIL;
   }

   if (!_mongoc_scram_verify_server_signature (scram, val_v, val_v_len)) {
      bson_set_error (error,
                      MONGOC_ERROR_SCRAM,
                      MONGOC_ERROR_SCRAM_PROTOCOL_ERROR,
                      "SCRAM Failure: could not verify server signature in sasl step 3");
      goto FAIL;
   }

   /* Update the cache if authentication succeeds */
   _mongoc_scram_update_cache (scram);

   goto CLEANUP;

FAIL:
   rval = false;

CLEANUP:
   bson_free (val_e);
   bson_free (val_v);

   return rval;
}


bool
_mongoc_scram_step (mongoc_scram_t *scram,
                    const uint8_t *inbuf,
                    uint32_t inbuflen,
                    uint8_t *outbuf,
                    uint32_t outbufmax,
                    uint32_t *outbuflen,
                    bson_error_t *error)
{
   BSON_ASSERT (scram);
   BSON_ASSERT (inbuf);
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbuflen);

   scram->step++;

   switch (scram->step) {
   case 1:
      return _mongoc_scram_start (scram, outbuf, outbufmax, outbuflen, error);
   case 2:
      return _mongoc_scram_step2 (scram, inbuf, inbuflen, outbuf, outbufmax, outbuflen, error);
   case 3:
      return _mongoc_scram_step3 (scram, inbuf, inbuflen, outbuf, outbufmax, outbuflen, error);
   default:
      bson_set_error (error, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_NOT_DONE, "SCRAM Failure: maximum steps detected");
      return false;
   }
}

bool
_mongoc_sasl_prep_required (const char *str)
{
   BSON_ASSERT_PARAM (str);
   unsigned char c;
   while (*str) {
      c = (unsigned char) *str;
      /* characters below 32 contain all of the control characters.
       * characters above 127 are multibyte UTF-8 characters.
       * character 127 is the DEL character. */
      if (c < 32 || c >= 127) {
         return true;
      }
      str++;
   }
   return false;
}

char *
_mongoc_sasl_prep_impl (const char *name, const char *in_utf8, bson_error_t *err)
{
   BSON_ASSERT_PARAM (name);
   BSON_ASSERT_PARAM (in_utf8);

   uint32_t *utf8_codepoints;
   ssize_t num_chars;
   uint8_t *out_utf8;

#define SASL_PREP_ERR_RETURN(msg)                                                               \
   do {                                                                                         \
      bson_set_error (err, MONGOC_ERROR_SCRAM, MONGOC_ERROR_SCRAM_PROTOCOL_ERROR, (msg), name); \
      return NULL;                                                                              \
   } while (0)

   /* 1. convert str to Unicode codepoints. */
   /* preflight to get the destination length. */
   num_chars = _mongoc_utf8_string_length (in_utf8);
   if (num_chars == -1) {
      SASL_PREP_ERR_RETURN ("could not calculate UTF-8 length of %s");
   }

   /* convert to unicode. */
   utf8_codepoints = bson_malloc (sizeof (uint32_t) * (num_chars + 1)); /* add one for trailing 0 value. */
   const char *c = in_utf8;

   for (size_t i = 0; i < num_chars; ++i) {
      const size_t utf8_char_length = _mongoc_utf8_char_length (c);
      utf8_codepoints[i] = _mongoc_utf8_get_first_code_point (c, utf8_char_length);

      c += utf8_char_length;
   }
   utf8_codepoints[num_chars] = '\0';

   /* 2. perform SASLPREP */

   // the steps below come directly from RFC 3454: 2. Preparation Overview.

   // a. Map - For each character in the input, check if it has a mapping (using
   // the tables) and, if so, replace it with its mapping.

   // because we will have to map some characters to nothing, we'll use two
   // pointers: one for reading the original characters (i) and one for writing
   // the new characters (curr). i will always be >= curr.
   size_t curr = 0;
   for (size_t i = 0; i < num_chars; ++i) {
      if (_mongoc_utf8_code_point_is_in_table (utf8_codepoints[i],
                                               non_ascii_space_character_ranges,
                                               sizeof (non_ascii_space_character_ranges) / sizeof (uint32_t)))
         utf8_codepoints[curr++] = 0x0020;
      else if (_mongoc_utf8_code_point_is_in_table (utf8_codepoints[i],
                                                    commonly_mapped_to_nothing_ranges,
                                                    sizeof (commonly_mapped_to_nothing_ranges) / sizeof (uint32_t))) {
         // effectively skip over the character because we don't increment curr.
      } else
         utf8_codepoints[curr++] = utf8_codepoints[i];
   }
   utf8_codepoints[curr] = '\0';
   num_chars = curr;


   // b. Normalize - normalize the result of step `a` using Unicode
   // normalization.

   // this is an optional step for stringprep, but Unicode normalization with
   // form KC is required for SASLPrep.

   // in order to do this, we must first convert back to UTF-8.

   // preflight for length
   size_t utf8_pre_norm_len = 0;
   for (size_t i = 0; i < num_chars; ++i) {
      const ssize_t len = _mongoc_utf8_code_point_length (utf8_codepoints[i]);
      if (len == -1) {
         bson_free (utf8_codepoints);
         SASL_PREP_ERR_RETURN ("invalid Unicode code point in %s");
      } else {
         utf8_pre_norm_len += len;
      }
   }
   char *utf8_pre_norm = (char *) bson_malloc (sizeof (char) * (utf8_pre_norm_len + 1));

   char *loc = utf8_pre_norm;
   for (size_t i = 0; i < num_chars; ++i) {
      const ssize_t utf8_char_length = _mongoc_utf8_code_point_to_str (utf8_codepoints[i], loc);
      if (utf8_char_length == -1) {
         bson_free (utf8_pre_norm);
         bson_free (utf8_codepoints);
         SASL_PREP_ERR_RETURN ("invalid Unicode code point in %s");
      }
      loc += utf8_char_length;
   }
   *loc = '\0';

   out_utf8 = (uint8_t *) utf8proc_NFKC ((utf8proc_uint8_t *) utf8_pre_norm);

   // the last two steps are both checks for characters that should not be
   // allowed. Because the normalization step is guarenteed to not create any
   // characters that will cause an error, we will use the utf8_codepoints
   // codepoints to check (pre-normalization) as to avoid converting back and
   // forth from UTF-8 to unicode codepoints.

   // c. Prohibit -- Check for any characters
   // that are not allowed in the output. If any are found, return an error.

   for (size_t i = 0; i < num_chars; ++i) {
      if (_mongoc_utf8_code_point_is_in_table (
             utf8_codepoints[i], prohibited_output_ranges, sizeof (prohibited_output_ranges) / sizeof (uint32_t)) ||
          _mongoc_utf8_code_point_is_in_table (utf8_codepoints[i],
                                               unassigned_codepoint_ranges,
                                               sizeof (unassigned_codepoint_ranges) / sizeof (uint32_t))) {
         bson_free (out_utf8);
         bson_free (utf8_pre_norm);
         bson_free (utf8_codepoints);
         SASL_PREP_ERR_RETURN ("prohibited character included in %s");
      }
   }

   // d. Check bidi -- Possibly check for right-to-left characters, and if
   // any are found, make sure that the whole string satisfies the
   // requirements for bidirectional strings.  If the string does not
   // satisfy the requirements for bidirectional strings, return an
   // error.

   // note: bidi stands for directional (text). Most characters are displayed
   // left to right but some are displayed right to left. The requirements are
   // as follows:
   // 1. If a string contains any RandALCat character, it can't contain an LCat
   // character
   // 2. If it contains an RandALCat character, there must be an RandALCat
   // character at the beginning and the end of the string (does not have to be
   // the same character)
   bool contains_LCat = false;
   bool contains_RandALCar = false;


   for (size_t i = 0; i < num_chars; ++i) {
      if (_mongoc_utf8_code_point_is_in_table (
             utf8_codepoints[i], LCat_bidi_ranges, sizeof (LCat_bidi_ranges) / sizeof (uint32_t))) {
         contains_LCat = true;
      }
      if (_mongoc_utf8_code_point_is_in_table (
             utf8_codepoints[i], RandALCat_bidi_ranges, sizeof (RandALCat_bidi_ranges) / sizeof (uint32_t)))
         contains_RandALCar = true;
   }

   if (
      // requirement 1
      (contains_RandALCar && contains_LCat) ||
      // requirement 2
      (contains_RandALCar &&
       (!_mongoc_utf8_code_point_is_in_table (
           utf8_codepoints[0], RandALCat_bidi_ranges, sizeof (RandALCat_bidi_ranges) / sizeof (uint32_t)) ||
        !_mongoc_utf8_code_point_is_in_table (utf8_codepoints[num_chars - 1],
                                              RandALCat_bidi_ranges,
                                              sizeof (RandALCat_bidi_ranges) / sizeof (uint32_t))))) {
      bson_free (out_utf8);
      bson_free (utf8_pre_norm);
      bson_free (utf8_codepoints);
      SASL_PREP_ERR_RETURN ("%s does not meet bidirectional requirements");
   }

   bson_free (utf8_pre_norm);
   bson_free (utf8_codepoints);

   return (char *) out_utf8;
#undef SASL_PREP_ERR_RETURN
}

char *
_mongoc_sasl_prep (const char *in_utf8, bson_error_t *err)
{
   if (_mongoc_sasl_prep_required (in_utf8)) {
      return _mongoc_sasl_prep_impl ("password", in_utf8, err);
   }
   return bson_strdup (in_utf8);
}

size_t
_mongoc_utf8_char_length (const char *s)
{
   BSON_ASSERT_PARAM (s);

   uint8_t *c = (uint8_t *) s;
   // UTF-8 characters are either 1, 2, 3, or 4 bytes and the character length
   // can be determined by the first byte
   if ((*c & UINT8_C (0x80)) == 0)
      return 1u;
   else if ((*c & UINT8_C (0xe0)) == UINT8_C (0xc0))
      return 2u;
   else if ((*c & UINT8_C (0xf0)) == UINT8_C (0xe0))
      return 3u;
   else if ((*c & UINT8_C (0xf8)) == UINT8_C (0xf0))
      return 4u;
   else
      return 1u;
}

ssize_t
_mongoc_utf8_string_length (const char *s)
{
   BSON_ASSERT_PARAM (s);

   const uint8_t *c = (uint8_t *) s;

   ssize_t str_length = 0;

   while (*c) {
      const size_t utf8_char_length = _mongoc_utf8_char_length ((char *) c);

      if (!_mongoc_utf8_first_code_point_is_valid ((char *) c, utf8_char_length))
         return -1;

      str_length++;
      c += utf8_char_length;
   }

   return str_length;
}


bool
_mongoc_utf8_first_code_point_is_valid (const char *c, size_t length)
{
   BSON_ASSERT_PARAM (c);

   uint8_t *temp_c = (uint8_t *) c;
   // Referenced table here:
   // https://lemire.me/blog/2018/05/09/how-quickly-can-you-check-that-a-string-is-valid-unicode-utf-8/
   switch (length) {
   case 1:
      return _mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0x00), UINT8_C (0x7F));
   case 2:
      return _mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xC2), UINT8_C (0xDF)) &&
             _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0xBF));
   case 3:
      // Four options, separated by ||
      return (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xE0), UINT8_C (0xE0)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0xA0), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF))) ||
             (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xE1), UINT8_C (0xEC)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF))) ||
             (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xED), UINT8_C (0xED)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0x9F)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF))) ||
             (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xEE), UINT8_C (0xEF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF)));
   case 4:
      // Three options, separated by ||
      return (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xF0), UINT8_C (0xF0)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x90), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[3], UINT8_C (0x80), UINT8_C (0xBF))) ||
             (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xF1), UINT8_C (0xF3)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[3], UINT8_C (0x80), UINT8_C (0xBF))) ||
             (_mongoc_utf8_code_unit_in_range (temp_c[0], UINT8_C (0xF4), UINT8_C (0xF4)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[1], UINT8_C (0x80), UINT8_C (0x8F)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[2], UINT8_C (0x80), UINT8_C (0xBF)) &&
              _mongoc_utf8_code_unit_in_range (temp_c[3], UINT8_C (0x80), UINT8_C (0xBF)));
   default:
      return true;
   }
}


bool
_mongoc_utf8_code_unit_in_range (const uint8_t c, const uint8_t lower, const uint8_t upper)
{
   return (c >= lower && c <= upper);
}

bool
_mongoc_utf8_code_point_is_in_table (uint32_t code, const uint32_t *table, size_t size)
{
   BSON_ASSERT_PARAM (table);

   // all tables have size / 2 ranges
   for (size_t i = 0; i < size; i += 2) {
      if (code >= table[i] && code <= table[i + 1])
         return true;
   }

   return false;
}

uint32_t
_mongoc_utf8_get_first_code_point (const char *c, size_t length)
{
   BSON_ASSERT_PARAM (c);

   uint8_t *temp_c = (uint8_t *) c;
   switch (length) {
   case 1:
      return (uint32_t) temp_c[0];
   case 2:
      return (uint32_t) (((temp_c[0] & UINT8_C (0x1f)) << 6) | (temp_c[1] & UINT8_C (0x3f)));
   case 3:
      return (uint32_t) (((temp_c[0] & UINT8_C (0x0f)) << 12) | ((temp_c[1] & UINT8_C (0x3f)) << 6) |
                         (temp_c[2] & UINT8_C (0x3f)));
   case 4:
      return (uint32_t) (((temp_c[0] & UINT8_C (0x07)) << 18) | ((temp_c[1] & UINT8_C (0x3f)) << 12) |
                         ((temp_c[2] & UINT8_C (0x3f)) << 6) | (temp_c[3] & UINT8_C (0x3f)));
   default:
      return 0;
   }
}

ssize_t
_mongoc_utf8_code_point_to_str (uint32_t c, char *out)
{
   BSON_ASSERT_PARAM (out);

   uint8_t *ptr = (uint8_t *) out;

   if (c <= UINT8_C (0x7F)) {
      // Plain ASCII
      ptr[0] = (uint8_t) c;
      return 1;
   } else if (c <= 0x07FF) {
      // 2-byte unicode
      ptr[0] = (uint8_t) (((c >> 6) & UINT8_C (0x1F)) | UINT8_C (0xC0));
      ptr[1] = (uint8_t) (((c >> 0) & UINT8_C (0x3F)) | UINT8_C (0x80));
      return 2;
   } else if (c <= 0xFFFF) {
      // 3-byte unicode
      ptr[0] = (uint8_t) (((c >> 12) & UINT8_C (0x0F)) | UINT8_C (0xE0));
      ptr[1] = (uint8_t) (((c >> 6) & UINT8_C (0x3F)) | UINT8_C (0x80));
      ptr[2] = (uint8_t) ((c & UINT8_C (0x3F)) | UINT8_C (0x80));
      return 3;
   } else if (c <= 0x10FFFF) {
      // 4-byte unicode
      ptr[0] = (uint8_t) (((c >> 18) & UINT8_C (0x07)) | UINT8_C (0xF0));
      ptr[1] = (uint8_t) (((c >> 12) & UINT8_C (0x3F)) | UINT8_C (0x80));
      ptr[2] = (uint8_t) (((c >> 6) & UINT8_C (0x3F)) | UINT8_C (0x80));
      ptr[3] = (uint8_t) ((c & UINT8_C (0x3F)) | UINT8_C (0x80));
      return 4;
   } else {
      return -1;
   }
}

ssize_t
_mongoc_utf8_code_point_length (uint32_t c)
{
   if (c <= UINT8_C (0x7F))
      return 1;
   else if (c <= 0x07FF)
      return 2;
   else if (c <= 0xFFFF)
      return 3;
   else if (c <= 0x10FFFF)
      return 4;
   else
      return -1;
}

#endif
