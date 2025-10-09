#include <mongoc/mongoc.h>
#include "mongoc/mongoc-ssl-private.h"

#ifdef MONGOC_ENABLE_SSL_OPENSSL
#include "mongoc/mongoc-openssl-private.h"
#endif

#include "TestSuite.h"
#include "test-libmongoc.h"

#if defined(MONGOC_ENABLE_SSL) && !defined(MONGOC_ENABLE_SSL_LIBRESSL)
static void
test_extract_subject (void)
{
   char *subject;

   subject = mongoc_ssl_extract_subject (CERT_SERVER, NULL);
   ASSERT_CMPSTR (subject, "C=US,ST=New York,L=New York City,O=MongoDB,OU=Drivers,CN=localhost");
   bson_free (subject);

   subject = mongoc_ssl_extract_subject (CERT_CLIENT, NULL);
   ASSERT_CMPSTR (subject, "C=US,ST=New York,L=New York City,O=MDB,OU=Drivers,CN=client");
   bson_free (subject);
}
#endif

#ifdef MONGOC_ENABLE_OCSP_OPENSSL
/* Test parsing a DER encoded tlsfeature extension contents for the
 * status_request (value 5). This is a SEQUENCE of INTEGER. libmongoc assumes
 * this is a sequence of one byte integers. */

static void
_expect_malformed (const char *data, int32_t len)
{
   bool ret;

   ret = _mongoc_tlsfeature_has_status_request ((const uint8_t *) data, len);
   BSON_ASSERT (!ret);
   ASSERT_CAPTURED_LOG ("mongoc", MONGOC_LOG_LEVEL_ERROR, "malformed");
   clear_captured_logs ();
}

static void
_expect_no_status_request (const char *data, int32_t len)
{
   bool ret;
   ret = _mongoc_tlsfeature_has_status_request ((const uint8_t *) data, len);
   BSON_ASSERT (!ret);
   ASSERT_NO_CAPTURED_LOGS ("mongoc");
}

static void
_expect_status_request (const char *data, int32_t len)
{
   bool ret;
   ret = _mongoc_tlsfeature_has_status_request ((const uint8_t *) data, len);
   BSON_ASSERT (ret);
   ASSERT_NO_CAPTURED_LOGS ("mongoc");
}

static void
test_tlsfeature_parsing (void)
{
   capture_logs (true);
   /* A sequence of one integer = 5. */
   _expect_status_request ("\x30\x03\x02\x01\x05", 5);
   /* A sequence of one integer = 6. */
   _expect_no_status_request ("\x30\x03\x02\x01\x06", 5);
   /* A sequence of two integers = 5,6. */
   _expect_status_request ("\x30\x03\x02\x01\x05\x02\x01\x06", 8);
   /* A sequence of two integers = 6,5. */
   _expect_status_request ("\x30\x03\x02\x01\x06\x02\x01\x05", 8);
   /* A sequence containing a non-integer. Parsing fails. */
   _expect_malformed ("\x30\x03\x03\x01\x05\x02\x01\x06", 8);
   /* A non-sequence. It will not read past the first byte (despite the >1
    * length). */
   _expect_malformed ("\xFF", 2);
   /* A sequence with a length represented in more than one byte. Parsing fails.
    */
   _expect_malformed ("\x30\x82\x04\x48", 4);
   /* An integer with length > 1. Parsing fails. */
   _expect_malformed ("\x30\x03\x02\x02\x05\x05", 6);
}
#endif /* MONGOC_ENABLE_OCSP_OPENSSL */

void
test_x509_install (TestSuite *suite)
{
#if defined(MONGOC_ENABLE_SSL) && !defined(MONGOC_ENABLE_SSL_LIBRESSL)
   TestSuite_Add (suite, "/X509/extract_subject", test_extract_subject);
#endif

#ifdef MONGOC_ENABLE_OCSP_OPENSSL
   TestSuite_Add (suite, "/X509/tlsfeature_parsing", test_tlsfeature_parsing);
#endif
}
