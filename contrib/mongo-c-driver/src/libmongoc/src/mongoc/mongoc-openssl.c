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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SSL_OPENSSL

#include <bson/bson.h>
#include <limits.h>
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/ocsp.h>
#include <openssl/x509v3.h>
#include <openssl/crypto.h>

#include <string.h>

#include "mongoc-http-private.h"
#include "mongoc-init.h"
#include "mongoc-openssl-private.h"
#include "mongoc-socket.h"
#include "mongoc-ssl.h"
#include "mongoc-stream-tls-openssl-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-trace-private.h"
#include "mongoc-util-private.h"

#ifdef MONGOC_ENABLE_OCSP_OPENSSL
#include "mongoc-ocsp-cache-private.h"
#endif

#ifdef _WIN32
#include <wincrypt.h>
#endif

#if OPENSSL_VERSION_NUMBER < 0x10100000L
static bson_mutex_t *gMongocOpenSslThreadLocks;

static void
_mongoc_openssl_thread_startup (void);
static void
_mongoc_openssl_thread_cleanup (void);
#endif
#ifndef MONGOC_HAVE_ASN1_STRING_GET0_DATA
#define ASN1_STRING_get0_data ASN1_STRING_data
#endif

static int tlsfeature_nid;

/**
 * _mongoc_openssl_init:
 *
 * initialization function for SSL
 *
 * This needs to get called early on and is not threadsafe.  Called by
 * mongoc_init.
 */
void
_mongoc_openssl_init (void)
{
   SSL_CTX *ctx;

   SSL_library_init ();
   SSL_load_error_strings ();
#if OPENSSL_VERSION_NUMBER < 0x30000000L
   // See:
   // https://www.openssl.org/docs/man3.0/man7/migration_guide.html#Deprecated-function-mappings
   ERR_load_BIO_strings ();
#endif
   OpenSSL_add_all_algorithms ();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
   _mongoc_openssl_thread_startup ();
#endif

   ctx = SSL_CTX_new (SSLv23_method ());
   if (!ctx) {
      MONGOC_ERROR ("Failed to initialize OpenSSL.");
   }

#ifdef NID_tlsfeature
   tlsfeature_nid = NID_tlsfeature;
#else
   /* TLS versions before 1.1.0 did not define the TLS Feature extension. */
   tlsfeature_nid = OBJ_create ("1.3.6.1.5.5.7.1.24", "tlsfeature", "TLS Feature");
#endif

   SSL_CTX_free (ctx);
}

void
_mongoc_openssl_cleanup (void)
{
#if OPENSSL_VERSION_NUMBER < 0x10100000L
   _mongoc_openssl_thread_cleanup ();
#endif
}

static int
_mongoc_openssl_password_cb (char *buf, int num, int rwflag, void *user_data)
{
   char *pass = (char *) user_data;
   int pass_len = (int) strlen (pass);

   BSON_UNUSED (rwflag);

   if (num < pass_len + 1) {
      return 0;
   }

   bson_strncpy (buf, pass, num);
   return pass_len;
}

#ifdef _WIN32
bool
_mongoc_openssl_import_cert_store (LPWSTR store_name, DWORD dwFlags, X509_STORE *openssl_store)
{
   PCCERT_CONTEXT cert = NULL;
   HCERTSTORE cert_store;

   cert_store = CertOpenStore (CERT_STORE_PROV_SYSTEM,                  /* provider */
                               X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, /* certificate encoding */
                               0,                                       /* unused */
                               dwFlags,                                 /* dwFlags */
                               store_name);                             /* system store name. "My" or "Root" */

   if (cert_store == NULL) {
      LPTSTR msg = NULL;
      FormatMessage (FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ARGUMENT_ARRAY,
                     NULL,
                     GetLastError (),
                     LANG_NEUTRAL,
                     (LPTSTR) &msg,
                     0,
                     NULL);
      MONGOC_ERROR ("Can't open CA store: 0x%.8X: '%s'", GetLastError (), msg);
      LocalFree (msg);
      return false;
   }

   while ((cert = CertEnumCertificatesInStore (cert_store, cert)) != NULL) {
      X509 *x509Obj = d2i_X509 (NULL, (const unsigned char **) &cert->pbCertEncoded, cert->cbCertEncoded);

      if (x509Obj == NULL) {
         MONGOC_WARNING ("Error parsing X509 object from Windows certificate store");
         continue;
      }

      X509_STORE_add_cert (openssl_store, x509Obj);
      X509_free (x509Obj);
   }

   CertCloseStore (cert_store, 0);
   return true;
}

bool
_mongoc_openssl_import_cert_stores (SSL_CTX *context)
{
   bool retval;
   X509_STORE *store = SSL_CTX_get_cert_store (context);

   if (!store) {
      MONGOC_WARNING ("no X509 store found for SSL context while loading "
                      "system certificates");
      return false;
   }

   retval =
      _mongoc_openssl_import_cert_store (L"root", CERT_SYSTEM_STORE_CURRENT_USER | CERT_STORE_READONLY_FLAG, store);
   retval &=
      _mongoc_openssl_import_cert_store (L"CA", CERT_SYSTEM_STORE_CURRENT_USER | CERT_STORE_READONLY_FLAG, store);

   return retval;
}
#endif

#if OPENSSL_VERSION_NUMBER > 0x10002000L
bool
_mongoc_openssl_check_peer_hostname (SSL *ssl, const char *host, bool allow_invalid_hostname)
{
   X509 *peer = NULL;

   if (allow_invalid_hostname) {
      return true;
   }

   peer = SSL_get_peer_certificate (ssl);
   if (peer && (X509_check_host (peer, host, 0, 0, NULL) == 1 || X509_check_ip_asc (peer, host, 0) == 1)) {
      X509_free (peer);
      return true;
   }

   if (peer) {
      X509_free (peer);
   }
   return false;
}
#else
/** mongoc_openssl_hostcheck
 *
 * rfc 6125 match a given hostname against a given pattern
 *
 * Patterns come from DNS common names or subjectAltNames.
 *
 * This code is meant to implement RFC 6125 6.4.[1-3]
 *
 */
static bool
_mongoc_openssl_hostcheck (const char *pattern, const char *hostname)
{
   const char *pattern_label_end;
   const char *pattern_wildcard;
   const char *hostname_label_end;
   size_t prefixlen;
   size_t suffixlen;

   TRACE ("Comparing '%s' == '%s'", pattern, hostname);
   pattern_wildcard = strchr (pattern, '*');

   if (pattern_wildcard == NULL) {
      return strcasecmp (pattern, hostname) == 0;
   }

   pattern_label_end = strchr (pattern, '.');

   /* Bail out on wildcarding in a couple of situations:
    * o we don't have 2 dots - we're not going to wildcard root tlds
    * o the wildcard isn't in the left most group (separated by dots)
    * o the pattern is embedded in an A-label or U-label
    */
   if (pattern_label_end == NULL || strchr (pattern_label_end + 1, '.') == NULL ||
       pattern_wildcard > pattern_label_end || strncasecmp (pattern, "xn--", 4) == 0) {
      return strcasecmp (pattern, hostname) == 0;
   }

   hostname_label_end = strchr (hostname, '.');

   /* we know we have a dot in the pattern, we need one in the hostname */
   if (hostname_label_end == NULL || strcasecmp (pattern_label_end, hostname_label_end)) {
      return 0;
   }

   /* The wildcard must match at least one character, so the left part of the
    * hostname is at least as large as the left part of the pattern. */
   if ((hostname_label_end - hostname) < (pattern_label_end - pattern)) {
      return 0;
   }

   /* If the left prefix group before the star matches and right of the star
    * matches... we have a wildcard match */
   prefixlen = pattern_wildcard - pattern;
   suffixlen = pattern_label_end - (pattern_wildcard + 1);
   return strncasecmp (pattern, hostname, prefixlen) == 0 &&
          strncasecmp (pattern_wildcard + 1, hostname_label_end - suffixlen, suffixlen) == 0;
}


/** check if a provided cert matches a passed hostname
 */
bool
_mongoc_openssl_check_peer_hostname (SSL *ssl, const char *host, bool allow_invalid_hostname)
{
   X509 *peer;
   X509_NAME *subject_name;
   X509_NAME_ENTRY *entry;
   ASN1_STRING *entry_data;
   int length;
   int idx;
   int r = 0;
   long verify_status;

   size_t addrlen = 0;
   unsigned char addr4[sizeof (struct in_addr)];
   unsigned char addr6[sizeof (struct in6_addr)];
   int i;
   int n_sans = -1;
   int target = GEN_DNS;

   STACK_OF (GENERAL_NAME) *sans = NULL;

   ENTRY;
   BSON_ASSERT (ssl);
   BSON_ASSERT (host);

   if (allow_invalid_hostname) {
      RETURN (true);
   }

   /** if the host looks like an IP address, match that, otherwise we assume we
    * have a DNS name */
   if (inet_pton (AF_INET, host, &addr4)) {
      target = GEN_IPADD;
      addrlen = sizeof addr4;
   } else if (inet_pton (AF_INET6, host, &addr6)) {
      target = GEN_IPADD;
      addrlen = sizeof addr6;
   }

   peer = SSL_get_peer_certificate (ssl);

   if (!peer) {
      MONGOC_WARNING ("SSL Certification verification failed: %s", ERR_error_string (ERR_get_error (), NULL));
      RETURN (false);
   }

   verify_status = SSL_get_verify_result (ssl);

   if (verify_status == X509_V_OK) {
      /* gets a stack of alt names that we can iterate through */
      sans = (STACK_OF (GENERAL_NAME) *) X509_get_ext_d2i ((X509 *) peer, NID_subject_alt_name, NULL, NULL);

      if (sans) {
         n_sans = sk_GENERAL_NAME_num (sans);

         /* loop through the stack, or until we find a match */
         for (i = 0; i < n_sans && !r; i++) {
            const GENERAL_NAME *name = sk_GENERAL_NAME_value (sans, i);

            /* skip entries that can't apply, I.e. IP entries if we've got a
             * DNS host */
            if (name->type == target) {
               const char *check;

               check = (const char *) ASN1_STRING_get0_data (name->d.ia5);
               length = ASN1_STRING_length (name->d.ia5);

               switch (target) {
               case GEN_DNS:

                  /* check that we don't have an embedded null byte */
                  if ((length == bson_strnlen (check, length)) && _mongoc_openssl_hostcheck (check, host)) {
                     r = 1;
                  }

                  break;
               case GEN_IPADD:
                  if (length == addrlen) {
                     if (length == sizeof addr6 && !memcmp (check, &addr6, length)) {
                        r = 1;
                     } else if (length == sizeof addr4 && !memcmp (check, &addr4, length)) {
                        r = 1;
                     }
                  }

                  break;
               default:
                  BSON_ASSERT (0);
                  break;
               }
            }
         }
         GENERAL_NAMES_free (sans);
      } else {
         subject_name = X509_get_subject_name (peer);

         if (subject_name) {
            i = -1;

            /* skip to the last common name */
            while ((idx = X509_NAME_get_index_by_NID (subject_name, NID_commonName, i)) >= 0) {
               i = idx;
            }

            if (i >= 0) {
               entry = X509_NAME_get_entry (subject_name, i);
               entry_data = X509_NAME_ENTRY_get_data (entry);

               if (entry_data) {
                  char *check;

                  /* TODO: I've heard tell that old versions of SSL crap out
                   * when calling ASN1_STRING_to_UTF8 on already utf8 data.
                   * Check up on that */
                  length = ASN1_STRING_to_UTF8 ((unsigned char **) &check, entry_data);

                  if (length >= 0) {
                     /* check for embedded nulls */
                     if ((length == bson_strnlen (check, length)) && _mongoc_openssl_hostcheck (check, host)) {
                        r = 1;
                     }

                     OPENSSL_free (check);
                  }
               }
            }
         }
      }
   }

   X509_free (peer);
   RETURN (r);
}
#endif /* OPENSSL_VERSION_NUMBER */


static bool
_mongoc_openssl_setup_ca (SSL_CTX *ctx, const char *cert, const char *cert_dir)
{
   BSON_ASSERT (ctx);
   BSON_ASSERT (cert || cert_dir);

   if (!SSL_CTX_load_verify_locations (ctx, cert, cert_dir)) {
      MONGOC_ERROR ("Cannot load Certificate Authorities from '%s' and '%s'", cert, cert_dir);
      return 0;
   }

   return 1;
}


static bool
_mongoc_openssl_setup_crl (SSL_CTX *ctx, const char *crlfile)
{
   X509_STORE *store;
   X509_LOOKUP *lookup;
   int status;

   store = SSL_CTX_get_cert_store (ctx);
   X509_STORE_set_flags (store, X509_V_FLAG_CRL_CHECK);

   lookup = X509_STORE_add_lookup (store, X509_LOOKUP_file ());

   status = X509_load_crl_file (lookup, crlfile, X509_FILETYPE_PEM);

   return status != 0;
}


static bool
_mongoc_openssl_setup_pem_file (SSL_CTX *ctx, const char *pem_file, const char *password)
{
   if (!SSL_CTX_use_certificate_chain_file (ctx, pem_file)) {
      MONGOC_ERROR ("Cannot find certificate in '%s'", pem_file);
      return 0;
   }

   if (password) {
      SSL_CTX_set_default_passwd_cb_userdata (ctx, (void *) password);
      SSL_CTX_set_default_passwd_cb (ctx, _mongoc_openssl_password_cb);
   }

   if (!(SSL_CTX_use_PrivateKey_file (ctx, pem_file, SSL_FILETYPE_PEM))) {
      MONGOC_ERROR ("Cannot find private key in: '%s'", pem_file);
      return 0;
   }

   if (!(SSL_CTX_check_private_key (ctx))) {
      MONGOC_ERROR ("Cannot load private key: '%s'", pem_file);
      return 0;
   }

   return 1;
}

#ifdef MONGOC_ENABLE_OCSP_OPENSSL

static X509 *
_get_issuer (X509 *cert, STACK_OF (X509) * chain)
{
   X509 *issuer = NULL, *candidate = NULL;
   X509_NAME *issuer_name = NULL, *candidate_name = NULL;
   int i;

   issuer_name = X509_get_issuer_name (cert);
   for (i = 0; i < sk_X509_num (chain) && issuer == NULL; i++) {
      candidate = sk_X509_value (chain, i);
      candidate_name = X509_get_subject_name (candidate);
      if (0 == X509_NAME_cmp (candidate_name, issuer_name)) {
         issuer = candidate;
      }
   }
   RETURN (issuer);
}

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
/* OpenSSL 1.1.0+ has conveniences that we polyfill in older OpenSSL versions.
 */

STACK_OF (X509) * _get_verified_chain (SSL *ssl)
{
   return SSL_get0_verified_chain (ssl);
}

void
_free_verified_chain (STACK_OF (X509) * verified_chain)
{
   BSON_UNUSED (verified_chain);
   /* _get_verified_chain does not return a copy. Do nothing. */
   return;
}

const STACK_OF (X509_EXTENSION) * _get_extensions (const X509 *cert)
{
   return X509_get0_extensions (cert);
}

#else
/* Polyfill functionality for pre 1.1.0 OpenSSL */

STACK_OF (X509) * _get_verified_chain (SSL *ssl)
{
   X509_STORE *store = NULL;
   X509 *peer = NULL;
   STACK_OF (X509) *peer_chain = NULL;
   X509_STORE_CTX *store_ctx = NULL;
   STACK_OF (X509) *verified_chain = NULL;

   /* Get the certificate the server presented. */
   peer = SSL_get_peer_certificate (ssl);
   /* Get the chain of certificates the server presented. This is not a verified
    * chain. */
   peer_chain = SSL_get_peer_cert_chain (ssl);
   store = SSL_CTX_get_cert_store (SSL_get_SSL_CTX (ssl));
   store_ctx = X509_STORE_CTX_new ();
   if (!X509_STORE_CTX_init (store_ctx, store, peer, peer_chain)) {
      MONGOC_ERROR ("failed to initialize X509 store");
      goto fail;
   }

   if (X509_verify_cert (store_ctx) <= 0) {
      MONGOC_ERROR ("failed to obtain verified chain");
      goto fail;
   }

   verified_chain = X509_STORE_CTX_get1_chain (store_ctx);

fail:
   X509_free (peer);
   X509_STORE_CTX_free (store_ctx);
   return verified_chain;
}

/* On OpenSSL < 1.1.0, this chain isn't attached to the SSL session, so we need
 * it to dispose of itself. */
void
_free_verified_chain (STACK_OF (X509) * verified_chain)
{
   if (!verified_chain) {
      return;
   }
   sk_X509_pop_free (verified_chain, X509_free);
}

const STACK_OF (X509_EXTENSION) * _get_extensions (const X509 *cert)
{
   return cert->cert_info->extensions;
}
#endif /* OPENSSL_VERSION_NUMBER */


#define TLSFEATURE_STATUS_REQUEST 5

/* Check a tlsfeature extension contents for a status_request.
 *
 * Parse just enough of a DER encoded data to check if a SEQUENCE of INTEGER
 * contains the status_request extension (5). There are only five tlsfeature
 * extension types, so this only handles the case that the sequence's length is
 * representable in one byte, and that each integer is representable in one
 * byte. */
bool
_mongoc_tlsfeature_has_status_request (const uint8_t *data, int length)
{
   int i;

   /* Expect a sequence type, with a sequence length representable in one byte.
    */
   if (length < 3 || data[0] != 0x30 || data[1] >= 127) {
      MONGOC_ERROR ("malformed tlsfeature extension sequence");
      return false;
   }

   for (i = 2; i < length; i += 3) {
      /* Expect an integer, representable in one byte. */
      if (length < i + 3 || data[i] != 0x02 || data[i + 1] != 1) {
         MONGOC_ERROR ("malformed tlsfeature extension integer");
         return false;
      }

      if (data[i + 2] == TLSFEATURE_STATUS_REQUEST) {
         TRACE ("%s", "found status request in tlsfeature extension");
         return true;
      }
   }
   return false;
}

/* Check that the certificate has a tlsfeature extension with status_request. */
bool
_get_must_staple (X509 *cert)
{
   const STACK_OF (X509_EXTENSION) *exts = NULL;
   X509_EXTENSION *ext;
   ASN1_STRING *ext_data;
   int idx;

   exts = _get_extensions (cert);
   if (!exts) {
      TRACE ("%s", "certificate extensions not found");
      return false;
   }

   idx = X509v3_get_ext_by_NID (exts, tlsfeature_nid, -1);
   if (-1 == idx) {
      TRACE ("%s", "tlsfeature extension not found");
      return false;
   }

   ext = sk_X509_EXTENSION_value (exts, idx);
   ext_data = X509_EXTENSION_get_data (ext);

   /* Data is a DER encoded sequence of integers. */
   return _mongoc_tlsfeature_has_status_request (ASN1_STRING_get0_data (ext_data), ASN1_STRING_length (ext_data));
}

#define ERR_STR (ERR_error_string (ERR_get_error (), NULL))
#define MONGOC_OCSP_REQUEST_TIMEOUT_MS 5000

static OCSP_RESPONSE *
_contact_ocsp_responder (OCSP_CERTID *id, X509 *peer, mongoc_ssl_opt_t *ssl_opts, int *ocsp_uri_count)
{
   STACK_OF (OPENSSL_STRING) *url_stack = NULL;
   OPENSSL_STRING url = NULL, host = NULL, path = NULL, port = NULL;
   OCSP_REQUEST *req = NULL;
   const unsigned char *resp_data;
   OCSP_RESPONSE *resp = NULL;
   int i, ssl;

   url_stack = X509_get1_ocsp (peer);
   *ocsp_uri_count = sk_OPENSSL_STRING_num (url_stack);
   for (i = 0; i < *ocsp_uri_count && !resp; i++) {
      unsigned char *request_der = NULL;
      int request_der_len;
      mongoc_http_request_t http_req;
      mongoc_http_response_t http_res;
      bson_error_t error;

      _mongoc_http_request_init (&http_req);
      _mongoc_http_response_init (&http_res);
      url = sk_OPENSSL_STRING_value (url_stack, i);
      TRACE ("Contacting OCSP responder '%s'", url);

      /* splits the given url into its host, port and path components */
      if (!OCSP_parse_url (url, &host, &port, &path, &ssl)) {
         MONGOC_DEBUG ("Could not parse URL");
         GOTO (retry);
      }

      if (!(req = OCSP_REQUEST_new ())) {
         MONGOC_DEBUG ("Could not create new OCSP request");
         GOTO (retry);
      }

      /* add the cert ID to the OCSP request object */
      if (!id || !OCSP_request_add0_id (req, OCSP_CERTID_dup (id))) {
         MONGOC_DEBUG ("Could not add cert ID to the OCSP request object");
         GOTO (retry);
      }

      /* add nonce to OCSP request object */
      if (!OCSP_request_add1_nonce (req, 0 /* use random nonce */, -1)) {
         MONGOC_DEBUG ("Could not add nonce to OCSP request object");
         GOTO (retry);
      }

      request_der_len = i2d_OCSP_REQUEST (req, &request_der);
      if (request_der_len < 0) {
         MONGOC_DEBUG ("Could not encode OCSP request");
         GOTO (retry);
      }

      http_req.method = "POST";
      http_req.extra_headers = "Content-Type: application/ocsp-request\r\n";
      http_req.host = host;
      http_req.path = path;
      http_req.port = (int) bson_ascii_strtoll (port, NULL, 10);
      http_req.body = (const char *) request_der;
      http_req.body_len = request_der_len;
      if (!_mongoc_http_send (&http_req, MONGOC_OCSP_REQUEST_TIMEOUT_MS, ssl != 0, ssl_opts, &http_res, &error)) {
         MONGOC_DEBUG ("Could not send HTTP request: %s", error.message);
         GOTO (retry);
      }

      resp_data = (const unsigned char *) http_res.body;

      if (http_res.body_len == 0 || !d2i_OCSP_RESPONSE (&resp, &resp_data, http_res.body_len)) {
         MONGOC_DEBUG ("Could not parse OCSP response from HTTP response");
         MONGOC_DEBUG ("Response headers: %s", http_res.headers);
         GOTO (retry);
      }

   retry:
      if (host)
         OPENSSL_free (host);
      if (port)
         OPENSSL_free (port);
      if (path)
         OPENSSL_free (path);
      if (req)
         OCSP_REQUEST_free (req);
      if (request_der)
         OPENSSL_free (request_der);
      _mongoc_http_response_cleanup (&http_res);
   }

   if (url_stack)
      X509_email_free (url_stack);
   RETURN (resp);
}

#define SOFT_FAIL(...) ((stapled_response) ? MONGOC_ERROR (__VA_ARGS__) : MONGOC_DEBUG (__VA_ARGS__))

#define X509_CHECK_SUCCESS 1
#define OCSP_VERIFY_SUCCESS 1

int
_mongoc_ocsp_tlsext_status (SSL *ssl, mongoc_openssl_ocsp_opt_t *opts)
{
   enum { OCSP_CB_ERROR = -1, OCSP_CB_REVOKED, OCSP_CB_SUCCESS } ret;
   bool stapled_response = true;
   bool must_staple;
   OCSP_RESPONSE *resp = NULL;
   OCSP_BASICRESP *basic = NULL;
   X509_STORE *store = NULL;
   X509 *peer = NULL, *issuer = NULL;
   STACK_OF (X509) *cert_chain = NULL;
   const unsigned char *resp_data = NULL;
   unsigned char *mutable_resp_data = NULL;
   int cert_status, reason, len, status;
   OCSP_CERTID *id = NULL;
   ASN1_GENERALIZEDTIME *produced_at = NULL, *this_update = NULL, *next_update = NULL;
   int ocsp_uri_count = 0;

   if (opts->weak_cert_validation) {
      return OCSP_CB_SUCCESS;
   }

   if (!(peer = SSL_get_peer_certificate (ssl))) {
      MONGOC_ERROR ("No certificate was presented by the peer");
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   /* Get a STACK_OF(X509) certs forming the cert chain of the peer, including
    * the peer's cert */
   if (!(cert_chain = _get_verified_chain (ssl))) {
      MONGOC_ERROR ("Unable to obtain verified chain");
      ret = OCSP_CB_REVOKED;
      GOTO (done);
   }

   if (!(issuer = _get_issuer (peer, cert_chain))) {
      MONGOC_ERROR ("Could not get issuer from peer cert");
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   if (!(id = OCSP_cert_to_id (NULL /* SHA1 */, peer, issuer))) {
      MONGOC_ERROR ("Could not obtain a valid OCSP_CERTID for peer");
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   if (_mongoc_ocsp_cache_get_status (id, &cert_status, &reason, &this_update, &next_update)) {
      GOTO (validate);
   }

   /* Get the stapled OCSP response returned by the server */
   len = SSL_get_tlsext_status_ocsp_resp (ssl, &mutable_resp_data);
   resp_data = mutable_resp_data;
   stapled_response = !!resp_data;
   if (stapled_response) {
      /* obtain an OCSP_RESPONSE object from the OCSP response */
      if (!d2i_OCSP_RESPONSE (&resp, &resp_data, len)) {
         MONGOC_ERROR ("Failed to parse OCSP response");
         ret = OCSP_CB_ERROR;
         GOTO (done);
      }
   } else {
      TRACE ("%s", "Server does not contain a stapled response");
      must_staple = _get_must_staple (peer);
      if (must_staple) {
         MONGOC_ERROR ("Server must contain a stapled response");
         ret = OCSP_CB_REVOKED;
         GOTO (done);
      }

      if (opts->disable_endpoint_check ||
          !(resp = _contact_ocsp_responder (id, peer, &opts->ssl_opts, &ocsp_uri_count))) {
         if (ocsp_uri_count > 0) {
            /* Only log a soft failure if there were OCSP responders listed in
             * the certificate. */
            MONGOC_DEBUG ("Soft-fail: No OCSP responder could be reached");
         }
         ret = OCSP_CB_SUCCESS;
         GOTO (done);
      }
   }

   TRACE ("%s", "Validating OCSP response");
   /* Validate the OCSP response status of the OCSP_RESPONSE object */
   status = OCSP_response_status (resp);
   if (status != OCSP_RESPONSE_STATUS_SUCCESSFUL) {
      SOFT_FAIL ("OCSP response error %d %s", status, OCSP_response_status_str (status));
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   TRACE ("%s", "OCSP response status successful");

   /* Get the OCSP_BASICRESP structure contained in OCSP_RESPONSE object for the
    * peer cert */
   basic = OCSP_response_get1_basic (resp);
   if (!basic) {
      SOFT_FAIL ("Could not find BasicOCSPResponse: %s", ERR_STR);
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   store = SSL_CTX_get_cert_store (SSL_get_SSL_CTX (ssl));

   /*
    * checks that the basic response message is correctly signed and that the
    * signer certificate can be validated.
    * 1. The function first verifies the signer cert of the response is in the
    * given cert chain.
    * 2. Next, the function verifies the signature of the basic response.
    * 3. Finally, the function validates the signer cert, constructing the
    * validation path via the untrusted cert chain.
    *
    * cert_chain has already been verified. Use OCSP_TRUSTOTHER so the signer
    * certificate can be considered verified if it is in cert_chain.
    */
   if (OCSP_basic_verify (basic, cert_chain, store, OCSP_TRUSTOTHER) != OCSP_VERIFY_SUCCESS) {
      SOFT_FAIL ("OCSP response failed verification: %s", ERR_STR);
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   /* searches the basic response for an OCSP response for the given cert ID */
   if (!OCSP_resp_find_status (basic, id, &cert_status, &reason, &produced_at, &this_update, &next_update)) {
      SOFT_FAIL ("No OCSP response found for the peer certificate");
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

   /* checks the validity of this_update and next_update values */
   if (!OCSP_check_validity (this_update, next_update, 0L, -1L)) {
      SOFT_FAIL ("OCSP response has expired: %s", ERR_STR);
      ret = OCSP_CB_ERROR;
      GOTO (done);
   }

validate:
   switch (cert_status) {
   case V_OCSP_CERTSTATUS_GOOD:
      TRACE ("%s", "OCSP Certificate Status: Good");
      _mongoc_ocsp_cache_set_resp (id, cert_status, reason, this_update, next_update);
      break;

   case V_OCSP_CERTSTATUS_REVOKED:
      MONGOC_ERROR ("OCSP Certificate Status: Revoked. Reason: %s", OCSP_crl_reason_str (reason));
      ret = OCSP_CB_REVOKED;
      _mongoc_ocsp_cache_set_resp (id, cert_status, reason, this_update, next_update);
      GOTO (done);

   default:
      MONGOC_DEBUG ("OCSP Certificate Status: Unknown");
      break;
   }

   /* Validate hostname matches cert */
   if (!_mongoc_openssl_check_peer_hostname (ssl, opts->host, opts->allow_invalid_hostname)) {
      ret = OCSP_CB_REVOKED;
      GOTO (done);
   }

   ret = OCSP_CB_SUCCESS;
done:
   if (ret == OCSP_CB_ERROR && !stapled_response) {
      ret = OCSP_CB_SUCCESS;
   }
   if (basic)
      OCSP_BASICRESP_free (basic);
   if (resp)
      OCSP_RESPONSE_free (resp);
   if (id)
      OCSP_CERTID_free (id);
   if (peer)
      X509_free (peer);
   if (cert_chain)
      _free_verified_chain (cert_chain);
   RETURN (ret);
}

#endif /* MONGOC_ENABLE_OCSP_OPENSSL */

/**
 * _mongoc_openssl_ctx_new:
 *
 * Create a new ssl context declaratively
 *
 * The opt.pem_pwd parameter, if passed, must exist for the life of this
 * context object (for storing and loading the associated pem file)
 */
SSL_CTX *
_mongoc_openssl_ctx_new (mongoc_ssl_opt_t *opt)
{
   SSL_CTX *ctx = NULL;
   int ssl_ctx_options = 0;

   /*
    * Ensure we are initialized. This is safe to call multiple times.
    */
   mongoc_init ();

   ctx = SSL_CTX_new (SSLv23_method ());

   BSON_ASSERT (ctx);

   /* SSL_OP_ALL - Activate all bug workaround options, to support buggy client
    * SSL's. */
   ssl_ctx_options |= SSL_OP_ALL;

   /* SSL_OP_NO_SSLv2 - Disable SSL v2 support */
   ssl_ctx_options |= SSL_OP_NO_SSLv2;

/* Disable compression, if we can.
 * OpenSSL 0.9.x added compression support which was always enabled when built
 * against zlib
 * OpenSSL 1.0.0 added the ability to disable it, while keeping it enabled by
 * default
 * OpenSSL 1.1.0 disabled it by default.
 */
#if OPENSSL_VERSION_NUMBER >= 0x10000000L
   ssl_ctx_options |= SSL_OP_NO_COMPRESSION;
#endif

/* man SSL_get_options says: "SSL_OP_NO_RENEGOTIATION options were added in
 * OpenSSL 1.1.1". */
#ifdef SSL_OP_NO_RENEGOTIATION
   ssl_ctx_options |= SSL_OP_NO_RENEGOTIATION;
#endif

   SSL_CTX_set_options (ctx, ssl_ctx_options);

/* only defined in special build, using:
 * --enable-system-crypto-profile (autotools)
 * -DENABLE_CRYPTO_SYSTEM_PROFILE:BOOL=ON (cmake)  */
#ifndef MONGOC_ENABLE_CRYPTO_SYSTEM_PROFILE
   /* HIGH - Enable strong ciphers
    * !EXPORT - Disable export ciphers (40/56 bit)
    * !aNULL - Disable anonymous auth ciphers
    * @STRENGTH - Sort ciphers based on strength */
   SSL_CTX_set_cipher_list (ctx, "HIGH:!EXPORT:!aNULL@STRENGTH");
#endif

   /* If renegotiation is needed, don't return from recv() or send() until it's
    * successful.
    * Note: this is for blocking sockets only. */
   SSL_CTX_set_mode (ctx, SSL_MODE_AUTO_RETRY);

   /* Load my private keys to present to the server */
   if (opt->pem_file && !_mongoc_openssl_setup_pem_file (ctx, opt->pem_file, opt->pem_pwd)) {
      SSL_CTX_free (ctx);
      return NULL;
   }

   /* Load in my Certificate Authority, to verify the server against
    * If none provided, fallback to the distro defaults */
   if (opt->ca_file || opt->ca_dir) {
      if (!_mongoc_openssl_setup_ca (ctx, opt->ca_file, opt->ca_dir)) {
         SSL_CTX_free (ctx);
         return NULL;
      }
   } else {
/* If the server certificate is issued by known CA we trust it by default */
#ifdef _WIN32
      _mongoc_openssl_import_cert_stores (ctx);
#else
      SSL_CTX_set_default_verify_paths (ctx);
#endif
   }

   /* Load my revocation list, to verify the server against */
   if (opt->crl_file && !_mongoc_openssl_setup_crl (ctx, opt->crl_file)) {
      SSL_CTX_free (ctx);
      return NULL;
   }

   return ctx;
}


char *
_mongoc_openssl_extract_subject (const char *filename, const char *passphrase)
{
   X509_NAME *subject = NULL;
   X509 *cert = NULL;
   BIO *certbio = NULL;
   BIO *strbio = NULL;
   char *str = NULL;
   int ret;

   BSON_UNUSED (passphrase);

   if (!filename) {
      return NULL;
   }

   certbio = BIO_new (BIO_s_file ());
   strbio = BIO_new (BIO_s_mem ());

   BSON_ASSERT (certbio);
   BSON_ASSERT (strbio);


   if (BIO_read_filename (certbio, filename) && (cert = PEM_read_bio_X509 (certbio, NULL, 0, NULL))) {
      if ((subject = X509_get_subject_name (cert))) {
         ret = X509_NAME_print_ex (strbio, subject, 0, XN_FLAG_RFC2253);

         if ((ret > 0) && (ret < INT_MAX)) {
            str = (char *) bson_malloc (ret + 2);
            BIO_gets (strbio, str, ret + 1);
            str[ret] = '\0';
         }
      }
   }

   if (cert) {
      X509_free (cert);
   }

   if (certbio) {
      BIO_free (certbio);
   }

   if (strbio) {
      BIO_free (strbio);
   }

   return str;
}

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#ifdef _WIN32

static unsigned long
_mongoc_openssl_thread_id_callback (void)
{
   unsigned long ret;

   ret = (unsigned long) GetCurrentThreadId ();
   return ret;
}

#else

static unsigned long
_mongoc_openssl_thread_id_callback (void)
{
   unsigned long ret;

   ret = (unsigned long) pthread_self ();
   return ret;
}

#endif

static void
_mongoc_openssl_thread_locking_callback (int mode, int type, const char *file, int line)
{
   if (mode & CRYPTO_LOCK) {
      bson_mutex_lock (&gMongocOpenSslThreadLocks[type]);
   } else {
      bson_mutex_unlock (&gMongocOpenSslThreadLocks[type]);
   }
}

static void
_mongoc_openssl_thread_startup (void)
{
   int i;

   gMongocOpenSslThreadLocks = (bson_mutex_t *) OPENSSL_malloc (CRYPTO_num_locks () * sizeof (bson_mutex_t));

   for (i = 0; i < CRYPTO_num_locks (); i++) {
      bson_mutex_init (&gMongocOpenSslThreadLocks[i]);
   }

   if (!CRYPTO_get_locking_callback ()) {
      CRYPTO_set_locking_callback (_mongoc_openssl_thread_locking_callback);
      CRYPTO_set_id_callback (_mongoc_openssl_thread_id_callback);
   }
}

static void
_mongoc_openssl_thread_cleanup (void)
{
   int i;

   if (CRYPTO_get_locking_callback () == _mongoc_openssl_thread_locking_callback) {
      CRYPTO_set_locking_callback (NULL);
   }

   if (CRYPTO_get_id_callback () == _mongoc_openssl_thread_id_callback) {
      CRYPTO_set_id_callback (NULL);
   }

   for (i = 0; i < CRYPTO_num_locks (); i++) {
      bson_mutex_destroy (&gMongocOpenSslThreadLocks[i]);
   }
   OPENSSL_free (gMongocOpenSslThreadLocks);
}
#endif

#endif
