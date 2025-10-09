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

#ifdef MONGOC_ENABLE_SSL_SECURE_TRANSPORT

#include <bson/bson.h>

#include "mongoc-log.h"
#include "mongoc-trace-private.h"
#include "mongoc-ssl.h"
#include "mongoc-stream-tls.h"
#include "mongoc-stream-tls-private.h"
#include "mongoc-secure-transport-private.h"
#include "mongoc-stream-tls-secure-transport-private.h"

#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <Security/SecKey.h>
#include <Security/SecureTransport.h>
#include <CommonCrypto/CommonDigest.h>
#include <Security/Security.h>
#include <Security/SecureTransport.h>
#include <CoreFoundation/CoreFoundation.h>

/* Jailbreak Darwin Private API */
/*
 * An alternative to using SecIdentityCreate is to use
 * SecIdentityCreateWithCertificate with a temporary keychain. However, doing so
 * leads to memory bugs. Unfortunately, using this private API seems to be the
 * best solution.
 */
SecIdentityRef
SecIdentityCreate (CFAllocatorRef allocator, SecCertificateRef certificate, SecKeyRef privateKey);

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream-secure_transport"

char *
_mongoc_cfstringref_to_cstring (CFStringRef str)
{
   CFIndex length;
   CFStringEncoding encoding;
   CFIndex max_size;
   char *cs;

   if (!str) {
      return NULL;
   }

   if (CFGetTypeID (str) != CFStringGetTypeID ()) {
      return NULL;
   }

   length = CFStringGetLength (str);
   encoding = kCFStringEncodingASCII;
   max_size = CFStringGetMaximumSizeForEncoding (length, encoding) + 1;
   cs = bson_malloc ((size_t) max_size);

   if (CFStringGetCString (str, cs, max_size, encoding)) {
      return cs;
   }

   bson_free (cs);
   return NULL;
}

static void
_bson_append_cftyperef (bson_string_t *retval, const char *label, CFTypeRef str)
{
   char *cs;

   if (str) {
      cs = _mongoc_cfstringref_to_cstring (str);

      if (cs) {
         bson_string_append_printf (retval, "%s%s", label, cs);
         bson_free (cs);
      } else {
         bson_string_append_printf (retval, "%s(null)", label);
      }
   }
}

CFTypeRef
_mongoc_secure_transport_dict_get (CFArrayRef values, CFStringRef label)
{
   if (!values || CFGetTypeID (values) != CFArrayGetTypeID ()) {
      return NULL;
   }

   for (CFIndex i = 0; i < CFArrayGetCount (values); ++i) {
      CFStringRef item_label;
      CFDictionaryRef item = CFArrayGetValueAtIndex (values, i);

      if (CFGetTypeID (item) != CFDictionaryGetTypeID ()) {
         continue;
      }

      item_label = CFDictionaryGetValue (item, kSecPropertyKeyLabel);
      if (item_label && CFStringCompare (item_label, label, 0) == kCFCompareEqualTo) {
         return CFDictionaryGetValue (item, kSecPropertyKeyValue);
      }
   }

   return NULL;
}

char *
_mongoc_secure_transport_RFC2253_from_cert (SecCertificateRef cert)
{
   CFTypeRef value;
   bson_string_t *retval;
   CFTypeRef subject_name;
   CFDictionaryRef cert_dict;

   cert_dict = SecCertificateCopyValues (cert, NULL, NULL);
   if (!cert_dict) {
      return NULL;
   }

   subject_name = CFDictionaryGetValue (cert_dict, kSecOIDX509V1SubjectName);
   if (!subject_name) {
      CFRelease (cert_dict);
      return NULL;
   }

   subject_name = CFDictionaryGetValue (subject_name, kSecPropertyKeyValue);
   if (!subject_name) {
      CFRelease (cert_dict);
      return NULL;
   }

   retval = bson_string_new ("");
   ;

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDCountryName);
   _bson_append_cftyperef (retval, "C=", value);

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDStateProvinceName);
   _bson_append_cftyperef (retval, ",ST=", value);

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDLocalityName);
   _bson_append_cftyperef (retval, ",L=", value);

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDOrganizationName);
   _bson_append_cftyperef (retval, ",O=", value);

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDOrganizationalUnitName);
   if (value) {
      /* Can be either one unit name, or array of unit names */
      if (CFGetTypeID (value) == CFStringGetTypeID ()) {
         _bson_append_cftyperef (retval, ",OU=", value);
      } else if (CFGetTypeID (value) == CFArrayGetTypeID ()) {
         CFIndex len = CFArrayGetCount (value);

         if (len > 0) {
            _bson_append_cftyperef (retval, ",OU=", CFArrayGetValueAtIndex (value, 0));
         }
         if (len > 1) {
            _bson_append_cftyperef (retval, ",", CFArrayGetValueAtIndex (value, 1));
         }
         if (len > 2) {
            _bson_append_cftyperef (retval, ",", CFArrayGetValueAtIndex (value, 2));
         }
      }
   }

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDCommonName);
   _bson_append_cftyperef (retval, ",CN=", value);

   value = _mongoc_secure_transport_dict_get (subject_name, kSecOIDStreetAddress);
   _bson_append_cftyperef (retval, ",STREET", value);

   CFRelease (cert_dict);
   return bson_string_free (retval, false);
}


static void
safe_release (CFTypeRef ref)
{
   if (ref) {
      CFRelease (ref);
   }
}


bool
_mongoc_secure_transport_import_pem (const char *filename,
                                     const char *passphrase,
                                     CFArrayRef *items,
                                     SecExternalItemType *type)
{
   SecExternalFormat format = kSecFormatPEMSequence;
   SecItemImportExportKeyParameters params = {0};
   SecTransformRef sec_transform = NULL;
   CFReadStreamRef read_stream = NULL;
   CFDataRef dataref = NULL;
   CFErrorRef error = NULL;
   CFURLRef url = NULL;
   OSStatus res;
   bool r = false;

   if (!filename) {
      TRACE ("%s", "No certificate provided");
      return false;
   }

   params.version = SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION;
   params.flags = 0;
   params.passphrase = NULL;
   params.alertTitle = NULL;
   params.alertPrompt = NULL;
   params.accessRef = NULL;
   params.keyUsage = NULL;
   params.keyAttributes = NULL;

   if (passphrase) {
      params.passphrase = CFStringCreateWithCString (kCFAllocatorDefault, passphrase, kCFStringEncodingUTF8);
   }

   url =
      CFURLCreateFromFileSystemRepresentation (kCFAllocatorDefault, (const UInt8 *) filename, strlen (filename), false);
   read_stream = CFReadStreamCreateWithFile (kCFAllocatorDefault, url);
   if (!CFReadStreamOpen (read_stream)) {
      MONGOC_ERROR ("Cannot find certificate in '%s', error reading file", filename);
      goto done;
   }

   sec_transform = SecTransformCreateReadTransformWithReadStream (read_stream);
   dataref = SecTransformExecute (sec_transform, &error);

   if (error) {
      CFStringRef str = CFErrorCopyDescription (error);
      MONGOC_ERROR (
         "Failed importing PEM '%s': %s", filename, CFStringGetCStringPtr (str, CFStringGetFastestEncoding (str)));

      CFRelease (str);
      goto done;
   }

   res = SecItemImport (dataref, CFSTR (".pem"), &format, type, 0, &params, NULL, items);

   if (res) {
      MONGOC_ERROR ("Failed importing PEM '%s' (code: %d)", filename, res);
      goto done;
   }

   r = true;

done:
   safe_release (dataref);
   safe_release (sec_transform);
   safe_release (read_stream);
   safe_release (url);
   safe_release (params.passphrase);

   return r;
}

char *
_mongoc_secure_transport_extract_subject (const char *filename, const char *passphrase)
{
   bool success;
   char *retval = NULL;
   CFArrayRef items = NULL;
   SecExternalItemType type = kSecItemTypeCertificate;


   success = _mongoc_secure_transport_import_pem (filename, passphrase, &items, &type);

   if (!success) {
      return NULL;
   }

   if (type == kSecItemTypeAggregate) {
      for (CFIndex i = 0; i < CFArrayGetCount (items); ++i) {
         CFTypeID item_id = CFGetTypeID (CFArrayGetValueAtIndex (items, i));

         if (item_id == SecCertificateGetTypeID ()) {
            retval = _mongoc_secure_transport_RFC2253_from_cert ((SecCertificateRef) CFArrayGetValueAtIndex (items, i));
            break;
         }
      }
   } else if (type == kSecItemTypeCertificate) {
      retval = _mongoc_secure_transport_RFC2253_from_cert ((SecCertificateRef) items);
   }

   if (items) {
      CFRelease (items);
   }

   return retval;
}

bool
mongoc_secure_transport_setup_certificate (mongoc_stream_tls_secure_transport_t *secure_transport,
                                           mongoc_ssl_opt_t *opt)
{
   bool success;
   CFArrayRef items;
   SecIdentityRef id;
   SecKeyRef key = NULL;
   SecCertificateRef cert = NULL;
   SecExternalItemType type = kSecItemTypeCertificate;

   if (!opt->pem_file) {
      TRACE ("%s", "No private key provided, the server won't be able to verify us");
      return false;
   }

   success = _mongoc_secure_transport_import_pem (opt->pem_file, opt->pem_pwd, &items, &type);
   if (!success) {
      /* caller will log an error */
      return false;
   }

   if (type != kSecItemTypeAggregate) {
      MONGOC_ERROR ("Cannot work with keys of type \"%d\". Please file a JIRA", type);
      CFRelease (items);
      return false;
   }

   for (CFIndex i = 0; i < CFArrayGetCount (items); ++i) {
      CFTypeID item_id = CFGetTypeID (CFArrayGetValueAtIndex (items, i));

      if (item_id == SecCertificateGetTypeID ()) {
         cert = (SecCertificateRef) CFArrayGetValueAtIndex (items, i);
      } else if (item_id == SecKeyGetTypeID ()) {
         key = (SecKeyRef) CFArrayGetValueAtIndex (items, i);
      }
   }

   if (!cert || !key) {
      MONGOC_ERROR ("Couldn't find valid private key");
      CFRelease (items);
      return false;
   }

   id = SecIdentityCreate (kCFAllocatorDefault, cert, key);
   secure_transport->my_cert = CFArrayCreateMutable (kCFAllocatorDefault, 2, &kCFTypeArrayCallBacks);

   CFArrayAppendValue (secure_transport->my_cert, id);
   CFArrayAppendValue (secure_transport->my_cert, cert);
   CFRelease (id);

   /*
    *  Secure Transport assumes the following:
    *    * The certificate references remain valid for the lifetime of the
    * session.
    *    * The identity specified in certRefs[0] is capable of signing.
    */
   success = !SSLSetCertificate (secure_transport->ssl_ctx_ref, secure_transport->my_cert);
   TRACE ("Setting client certificate %s", success ? "succeeded" : "failed");

   CFRelease (items);
   return success;
}

bool
mongoc_secure_transport_setup_ca (mongoc_stream_tls_secure_transport_t *secure_transport, mongoc_ssl_opt_t *opt)
{
   CFArrayRef items;
   SecExternalItemType type = kSecItemTypeCertificate;
   bool success;

   if (!opt->ca_file) {
      TRACE ("%s", "No CA provided, using defaults");
      return false;
   }

   success = _mongoc_secure_transport_import_pem (opt->ca_file, NULL, &items, &type);

   if (!success) {
      MONGOC_ERROR ("Cannot load Certificate Authorities from file \'%s\'", opt->ca_file);
      return false;
   }

   if (type == kSecItemTypeAggregate) {
      CFMutableArrayRef anchors = CFArrayCreateMutable (kCFAllocatorDefault, 0, &kCFTypeArrayCallBacks);

      for (CFIndex i = 0; i < CFArrayGetCount (items); ++i) {
         CFTypeID item_id = CFGetTypeID (CFArrayGetValueAtIndex (items, i));

         if (item_id == SecCertificateGetTypeID ()) {
            CFArrayAppendValue (anchors, CFArrayGetValueAtIndex (items, i));
         }
      }
      secure_transport->anchors = anchors;
      CFRelease (items);
   } else if (type == kSecItemTypeCertificate) {
      secure_transport->anchors = items;
   } else {
      CFRelease (items);
   }

   /* This should be SSLSetCertificateAuthorities But the /TLS/ tests fail
    * when it is */
   success = !SSLSetTrustedRoots (secure_transport->ssl_ctx_ref, secure_transport->anchors, true);
   TRACE ("Setting certificate authority %s (%s)", success ? "succeeded" : "failed", opt->ca_file);
   return success;
}

OSStatus
mongoc_secure_transport_read (SSLConnectionRef connection, void *data, size_t *data_length)
{
   mongoc_stream_tls_t *tls = (mongoc_stream_tls_t *) connection;
   ssize_t length;
   ENTRY;

   errno = 0;
   /* 4 arguments is *min_bytes* -- This is not a negotiation.
    * Secure Transport wants all or nothing. We must continue reading until
    * we get this amount, or timeout */
   length = mongoc_stream_read (tls->base_stream, data, *data_length, *data_length, tls->timeout_msec);

   if (length > 0) {
      *data_length = length;
      RETURN (noErr);
   }

   if (length == 0) {
      RETURN (errSSLClosedGraceful);
   }

   switch (errno) {
   case ENOENT:
      RETURN (errSSLClosedGraceful);
      break;
   case ECONNRESET:
      RETURN (errSSLClosedAbort);
      break;
   case EAGAIN:
      RETURN (errSSLWouldBlock);
      break;
   default:
      RETURN (-36); /* ioErr */
      break;
   }
}

OSStatus
mongoc_secure_transport_write (SSLConnectionRef connection, const void *data, size_t *data_length)
{
   mongoc_stream_tls_t *tls = (mongoc_stream_tls_t *) connection;
   ssize_t length;
   ENTRY;

   errno = 0;
   length = mongoc_stream_write (tls->base_stream, (void *) data, *data_length, tls->timeout_msec);

   if (length >= 0) {
      *data_length = length;
      RETURN (noErr);
   }

   switch (errno) {
   case EAGAIN:
      RETURN (errSSLWouldBlock);
   default:
      RETURN (-36); /* ioErr */
   }
}

void
CFReleaseSafe (CFTypeRef cf)
{
   if (cf != NULL) {
      CFRelease (cf);
   }
}

#endif
