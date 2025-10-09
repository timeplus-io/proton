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

#ifndef MONGOC_UTIL_PRIVATE_H
#define MONGOC_UTIL_PRIVATE_H

#include <bson/bson.h>
#include "mongoc.h"

#ifdef BSON_HAVE_STRINGS_H
#include <strings.h>
#endif

#include <stdint.h>

/* string comparison functions for Windows */
#ifdef _WIN32
#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#endif

#if BSON_GNUC_CHECK_VERSION(4, 6)
#define BEGIN_IGNORE_DEPRECATIONS \
   _Pragma ("GCC diagnostic push") _Pragma ("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define END_IGNORE_DEPRECATIONS _Pragma ("GCC diagnostic pop")
#elif defined(__clang__)
#define BEGIN_IGNORE_DEPRECATIONS \
   _Pragma ("clang diagnostic push") _Pragma ("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#define END_IGNORE_DEPRECATIONS _Pragma ("clang diagnostic pop")
#else
#define BEGIN_IGNORE_DEPRECATIONS
#define END_IGNORE_DEPRECATIONS
#endif

#ifndef _WIN32
#define MONGOC_PRINTF_FORMAT(a, b) __attribute__ ((format (__printf__, a, b)))
#else
#define MONGOC_PRINTF_FORMAT(a, b) /* no-op */
#endif

#define COALESCE(x, y) ((x == 0) ? (y) : (x))


/* Helper macros for stringifying things */
#define MONGOC_STR(s) #s
#define MONGOC_EVALUATE_STR(s) MONGOC_STR (s)

BSON_BEGIN_DECLS

extern const bson_validate_flags_t _mongoc_default_insert_vflags;
extern const bson_validate_flags_t _mongoc_default_replace_vflags;
extern const bson_validate_flags_t _mongoc_default_update_vflags;

int
_mongoc_rand_simple (unsigned int *seed);

char *
_mongoc_hex_md5 (const char *input);

void
_mongoc_usleep (int64_t usec);

/* Get the current time as a number of milliseconds since the Unix Epoch. */
int64_t
_mongoc_get_real_time_ms (void);

const char *
_mongoc_get_command_name (const bson_t *command);

bool
_mongoc_lookup_bool (const bson_t *bson, const char *key, bool default_value);

/* Returns a database name that the caller must free. */
char *
_mongoc_get_db_name (const char *ns);

void
_mongoc_bson_init_if_set (bson_t *bson);

const char *
_mongoc_bson_type_to_str (bson_type_t t);

const char *
_mongoc_wire_version_to_server_version (int32_t version);

bool
_mongoc_get_server_id_from_opts (const bson_t *opts,
                                 mongoc_error_domain_t domain,
                                 mongoc_error_code_t code,
                                 uint32_t *server_id,
                                 bson_error_t *error);

bool
_mongoc_validate_new_document (const bson_t *insert, bson_validate_flags_t vflags, bson_error_t *error);

bool
_mongoc_validate_replace (const bson_t *insert, bson_validate_flags_t vflags, bson_error_t *error);

bool
_mongoc_validate_update (const bson_t *update, bson_validate_flags_t vflags, bson_error_t *error);

bool
mongoc_ends_with (const char *str, const char *suffix);

void
mongoc_lowercase (const char *src, char *buf /* OUT */);

bool
mongoc_parse_port (uint16_t *port, const char *str);

void
_mongoc_bson_array_add_label (bson_t *bson, const char *label);

void
_mongoc_bson_array_copy_labels_to (const bson_t *reply, bson_t *dst);

void
_mongoc_add_transient_txn_error (const mongoc_client_session_t *cs, bson_t *reply);

bool
_mongoc_document_is_pipeline (const bson_t *document);

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_getenv --
 *
 *       Get the value of an environment variable.
 *
 * Returns:
 *       A string you must bson_free, or NULL if the variable is not set.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
char *
_mongoc_getenv (const char *name);

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_setenv --
 *
 *       Set or overwrite the value of an environment variable.
 *
 * Returns:
 *       False if setting the variable was unsuccessful.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_setenv (const char *name, const char *value);

void
bson_copy_to_including_noinit (const bson_t *src, bson_t *dst, const char *first_include, ...)
   BSON_GNUC_NULL_TERMINATED;

void
bson_copy_to_including_noinit_va (const bson_t *src, bson_t *dst, const char *first_include, va_list args);

/* Returns a uniformly-distributed uint32_t generated using
 * `_mongoc_rand_bytes()` if a source of cryptographic randomness is available
 * (defined only if `MONGOC_ENABLE_CRYPTO` is defined).
 */
uint32_t
_mongoc_crypto_rand_uint32_t (void);

/* Returns a uniformly-distributed uint64_t generated using
 * `_mongoc_rand_bytes()` if a source of cryptographic randomness is available
 * (defined only if `MONGOC_ENABLE_CRYPTO` is defined).
 */
uint64_t
_mongoc_crypto_rand_uint64_t (void);

/* Returns a uniformly-distributed size_t generated using
 * `_mongoc_rand_bytes()` if a source of cryptographic randomness is available
 * (defined only if `MONGOC_ENABLE_CRYPTO` is defined).
 */
size_t
_mongoc_crypto_rand_size_t (void);

/* Returns a uniformly-distributed random uint32_t generated using `rand()`.
 * Note: may invoke `srand()`, which may not be thread-safe. Concurrent calls to
 * `_mongoc_simple_rand_*()` functions, however, is thread-safe. */
uint32_t
_mongoc_simple_rand_uint32_t (void);

/* Returns a uniformly-distributed random uint64_t generated using `rand()`.
 * Note: may invoke `srand()`, which may not be thread-safe. Concurrent calls to
 * `_mongoc_simple_rand_*()` functions, however, is thread-safe. */
uint64_t
_mongoc_simple_rand_uint64_t (void);

/* Returns a uniformly-distributed random size_t generated using `rand()`.
 * Note: may invoke `srand()`, which may not be thread-safe. Concurrent calls to
 * `_mongoc_simple_rand_*()` functions, however, is thread-safe. */
size_t
_mongoc_simple_rand_size_t (void);

/* Returns a uniformly-distributed random integer in the range [min, max].
 *
 * The size of the range [min, max] must not equal the size of the representable
 * range of uint32_t (`min == 0 && max == UINT32_MAX` must not be true).
 *
 * The generator `rand` must return a random integer uniformly distributed in
 * the full range of representable values of uint32_t.
 */
uint32_t
_mongoc_rand_uint32_t (uint32_t min, uint32_t max, uint32_t (*rand) (void));

/* Returns a uniformly-distributed random integer in the range [min, max].
 *
 * The size of the range [min, max] must not equal the size of the representable
 * range of uint64_t (`min == 0 && max == UINT64_MAX` must not be true).
 *
 * The generator `rand` must return a random integer uniformly distributed in
 * the full range of representable values of uint64_t.
 */
uint64_t
_mongoc_rand_uint64_t (uint64_t min, uint64_t max, uint64_t (*rand) (void));

/* Returns a uniformly-distributed random integer in the range [min, max].
 *
 * The size of the range [min, max] must not equal the size of the representable
 * range of size_t (`min == 0 && max == SIZE_MAX` must not be true).
 *
 * The generator `rand` must return a random integer uniformly distributed in
 * the full range of representable values of size_t.
 */
size_t
_mongoc_rand_size_t (size_t min, size_t max, size_t (*rand) (void));

/* _mongoc_iter_document_as_bson attempts to read the document from @iter into
 * @bson. */
bool
_mongoc_iter_document_as_bson (const bson_iter_t *iter, bson_t *bson, bson_error_t *error);

uint8_t *
hex_to_bin (const char *hex, uint32_t *len);

char *
bin_to_hex (const uint8_t *bin, uint32_t len);

BSON_END_DECLS

#endif /* MONGOC_UTIL_PRIVATE_H */
