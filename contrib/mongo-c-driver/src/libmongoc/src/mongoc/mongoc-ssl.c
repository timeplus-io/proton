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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SSL

#include <bson/bson.h>
#include "mongoc-ssl.h"
#include "mongoc-ssl-private.h"
#include "mongoc-log.h"
#include "mongoc-uri.h"
#include "mongoc-util-private.h"

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
#include "mongoc-openssl-private.h"
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
#include "mongoc-libressl-private.h"
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
#include "mongoc-secure-transport-private.h"
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
#include "mongoc-secure-channel-private.h"
#endif

/* TODO: we could populate these from a config or something further down the
 * road for providing defaults */
#ifndef MONGOC_SSL_DEFAULT_TRUST_FILE
#define MONGOC_SSL_DEFAULT_TRUST_FILE NULL
#endif
#ifndef MONGOC_SSL_DEFAULT_TRUST_DIR
#define MONGOC_SSL_DEFAULT_TRUST_DIR NULL
#endif

static mongoc_ssl_opt_t gMongocSslOptDefault = {
   NULL,
   NULL,
   MONGOC_SSL_DEFAULT_TRUST_FILE,
   MONGOC_SSL_DEFAULT_TRUST_DIR,
};

const mongoc_ssl_opt_t *
mongoc_ssl_opt_get_default (void)
{
   return &gMongocSslOptDefault;
}

char *
mongoc_ssl_extract_subject (const char *filename, const char *passphrase)
{
   char *retval;

   if (!filename) {
      MONGOC_ERROR ("No filename provided to extract subject from");
      return NULL;
   }

#ifdef _WIN32
   if (_access (filename, 0) != 0) {
#else
   if (access (filename, R_OK) != 0) {
#endif
      MONGOC_ERROR ("Can't extract subject from unreadable file: '%s'", filename);
      return NULL;
   }

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
   retval = _mongoc_openssl_extract_subject (filename, passphrase);
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
   MONGOC_WARNING ("libtls doesn't support automatically extracting subject from "
                   "certificate to use with authentication");
   retval = NULL;
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
retval = _mongoc_secure_transport_extract_subject (filename, passphrase);
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
retval = _mongoc_secure_channel_extract_subject (filename, passphrase);
#endif

   if (!retval) {
      MONGOC_ERROR ("Can't extract subject from file '%s'", filename);
   }

   return retval;
}

void
_mongoc_ssl_opts_from_uri (mongoc_ssl_opt_t *ssl_opt, _mongoc_internal_tls_opts_t *internal, mongoc_uri_t *uri)
{
   bool insecure = mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSINSECURE, false);

   ssl_opt->pem_file = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, NULL);
   ssl_opt->pem_pwd = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, NULL);
   ssl_opt->ca_file = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, NULL);
   ssl_opt->weak_cert_validation =
      mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, insecure);
   ssl_opt->allow_invalid_hostname = mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, insecure);
   ssl_opt->internal = internal;
   internal->tls_disable_certificate_revocation_check =
      mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSDISABLECERTIFICATEREVOCATIONCHECK, false);
   internal->tls_disable_ocsp_endpoint_check =
      mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK, false);
}

void
_mongoc_ssl_opts_copy_to (const mongoc_ssl_opt_t *src, mongoc_ssl_opt_t *dst, bool copy_internal)
{
   BSON_ASSERT (src);
   BSON_ASSERT (dst);

   dst->pem_file = bson_strdup (src->pem_file);
   dst->pem_pwd = bson_strdup (src->pem_pwd);
   dst->ca_file = bson_strdup (src->ca_file);
   dst->ca_dir = bson_strdup (src->ca_dir);
   dst->crl_file = bson_strdup (src->crl_file);
   dst->weak_cert_validation = src->weak_cert_validation;
   dst->allow_invalid_hostname = src->allow_invalid_hostname;
   if (copy_internal) {
      dst->internal = NULL;
      if (src->internal) {
         dst->internal = bson_malloc (sizeof (_mongoc_internal_tls_opts_t));
         memcpy (dst->internal, src->internal, sizeof (_mongoc_internal_tls_opts_t));
      }
   }
}

void
_mongoc_ssl_opts_cleanup (mongoc_ssl_opt_t *opt, bool free_internal)
{
   bson_free ((char *) opt->pem_file);
   bson_free ((char *) opt->pem_pwd);
   bson_free ((char *) opt->ca_file);
   bson_free ((char *) opt->ca_dir);
   bson_free ((char *) opt->crl_file);
   if (free_internal) {
      bson_free (opt->internal);
   }
}

bool
_mongoc_ssl_opts_disable_certificate_revocation_check (const mongoc_ssl_opt_t *ssl_opt)
{
   if (!ssl_opt->internal) {
      return false;
   }
   return ((_mongoc_internal_tls_opts_t *) ssl_opt->internal)->tls_disable_certificate_revocation_check;
}

bool
_mongoc_ssl_opts_disable_ocsp_endpoint_check (const mongoc_ssl_opt_t *ssl_opt)
{
   if (!ssl_opt->internal) {
      return false;
   }
   return ((_mongoc_internal_tls_opts_t *) ssl_opt->internal)->tls_disable_ocsp_endpoint_check;
}

bool
_mongoc_ssl_opts_from_bson (mongoc_ssl_opt_t *ssl_opt, const bson_t *bson, bson_string_t *errmsg)
{
   bson_iter_t iter;

   if (ssl_opt->internal) {
      bson_string_append (errmsg, "SSL options must not have internal state set");
      return false;
   }

   ssl_opt->internal = bson_malloc0 (sizeof (_mongoc_internal_tls_opts_t));

   if (!bson_iter_init (&iter, bson)) {
      bson_string_append (errmsg, "error initializing iterator to BSON SSL options");
      return false;
   }

   while (bson_iter_next (&iter)) {
      const char *key = bson_iter_key (&iter);

      if (BSON_ITER_HOLDS_UTF8 (&iter)) {
         if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCERTIFICATEKEYFILE)) {
            ssl_opt->pem_file = bson_strdup (bson_iter_utf8 (&iter, NULL));
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD)) {
            ssl_opt->pem_pwd = bson_strdup (bson_iter_utf8 (&iter, NULL));
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSCAFILE)) {
            ssl_opt->ca_file = bson_strdup (bson_iter_utf8 (&iter, NULL));
            continue;
         }
      }

      if (BSON_ITER_HOLDS_BOOL (&iter)) {
         if (0 == bson_strcasecmp (key, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES)) {
            /* If MONGOC_URI_TLSINSECURE was parsed, weak_cert_validation must
             * remain true. */
            ssl_opt->weak_cert_validation = ssl_opt->weak_cert_validation || bson_iter_bool (&iter);
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES)) {
            /* If MONGOC_URI_TLSINSECURE was parsed, allow_invalid_hostname must
             * remain true. */
            ssl_opt->allow_invalid_hostname = ssl_opt->allow_invalid_hostname || bson_iter_bool (&iter);
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSINSECURE)) {
            if (bson_iter_bool (&iter)) {
               ssl_opt->weak_cert_validation = true;
               ssl_opt->allow_invalid_hostname = true;
            }
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSDISABLECERTIFICATEREVOCATIONCHECK)) {
            ((_mongoc_internal_tls_opts_t *) ssl_opt->internal)->tls_disable_certificate_revocation_check =
               bson_iter_bool (&iter);
            continue;
         } else if (0 == bson_strcasecmp (key, MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK)) {
            ((_mongoc_internal_tls_opts_t *) ssl_opt->internal)->tls_disable_ocsp_endpoint_check =
               bson_iter_bool (&iter);
            continue;
         }
      }

      bson_string_append_printf (
         errmsg, "unexpected %s option: %s", _mongoc_bson_type_to_str (bson_iter_type (&iter)), key);
      return false;
   }

   return true;
}

#endif
