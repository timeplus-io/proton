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

#include "TestSuite.h"

#include "mongoc/mongoc-ocsp-cache-private.h"

#if defined(MONGOC_ENABLE_OCSP_OPENSSL) && OPENSSL_VERSION_NUMBER >= 0x10101000L
#include <openssl/pem.h>

static OCSP_CERTID *
create_cert_id (long serial)
{
   OCSP_CERTID *id;
   X509_NAME *issuer_name;
   ASN1_BIT_STRING *issuer_key;
   ASN1_INTEGER *serial_number;

   issuer_name = X509_NAME_new ();
   issuer_key = ASN1_BIT_STRING_new ();
   serial_number = ASN1_INTEGER_new ();
   ASN1_INTEGER_set (serial_number, serial);

   id = OCSP_cert_id_new (EVP_sha1 (), issuer_name, issuer_key, serial_number);

   ASN1_BIT_STRING_free (issuer_key);
   ASN1_INTEGER_free (serial_number);
   X509_NAME_free (issuer_name);
   return id;
}

#define CLEAR_CACHE                                    \
   do {                                                \
      _mongoc_ocsp_cache_cleanup ();                   \
      _mongoc_ocsp_cache_init ();                      \
      BSON_ASSERT (_mongoc_ocsp_cache_length () == 0); \
   } while (0)

#define ASSERT_TIME_EQUAL(_a, _b)                \
   do {                                          \
      int pday, psec;                            \
      ASN1_TIME_diff (&pday, &psec, (_a), (_b)); \
      BSON_ASSERT (pday == 0);                   \
      BSON_ASSERT (psec == 0);                   \
   } while (0)

static void
test_mongoc_cache_insert (void)
{
   ASN1_GENERALIZEDTIME *this_update_in, *next_update_in;
   ASN1_GENERALIZEDTIME *this_update_out, *next_update_out;
   int i, size = 5, status = V_OCSP_CERTSTATUS_GOOD, reason = OCSP_REVOKED_STATUS_NOSTATUS;

   CLEAR_CACHE;

   next_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL) + 999);
   this_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL));
   for (i = 0; i < size; i++) {
      int s, r;
      OCSP_CERTID *id = create_cert_id (i);

      BSON_ASSERT (!_mongoc_ocsp_cache_get_status (id, &s, &r, &this_update_out, &next_update_out));
      _mongoc_ocsp_cache_set_resp (id, status, reason, this_update_in, next_update_in);

      OCSP_CERTID_free (id);
   }

   BSON_ASSERT (_mongoc_ocsp_cache_length () == size);

   for (i = 0; i < size; i++) {
      OCSP_CERTID *id = create_cert_id (i);
      int s, r;

      BSON_ASSERT (_mongoc_ocsp_cache_get_status (id, &s, &r, &this_update_out, &next_update_out));

      BSON_ASSERT (status == s);
      BSON_ASSERT (reason == r);
      ASSERT_TIME_EQUAL (next_update_in, next_update_out);
      ASSERT_TIME_EQUAL (this_update_in, this_update_out);

      OCSP_CERTID_free (id);
   }

   CLEAR_CACHE;
   ASN1_GENERALIZEDTIME_free (this_update_in);
   ASN1_GENERALIZEDTIME_free (next_update_in);
}

static void
test_mongoc_cache_update (void)
{
   OCSP_CERTID *id;
   ASN1_GENERALIZEDTIME *next_update_in, *next_update_out, *this_update_out;
   int status, reason;

   CLEAR_CACHE;

   next_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL));
   id = create_cert_id (1);

   _mongoc_ocsp_cache_set_resp (id, V_OCSP_CERTSTATUS_GOOD, 0, NULL, next_update_in);
   BSON_ASSERT (_mongoc_ocsp_cache_length () == 1);

   BSON_ASSERT (_mongoc_ocsp_cache_get_status (id, &status, &reason, &this_update_out, &next_update_out));
   BSON_ASSERT (status == V_OCSP_CERTSTATUS_GOOD);

   ASN1_GENERALIZEDTIME_free (next_update_in);
   next_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL) + 999 /* some time in the future */);

   _mongoc_ocsp_cache_set_resp (id, V_OCSP_CERTSTATUS_REVOKED, 0, NULL, next_update_in);
   BSON_ASSERT (_mongoc_ocsp_cache_length () == 1);

   BSON_ASSERT (_mongoc_ocsp_cache_get_status (id, &status, &reason, &this_update_out, &next_update_out));
   BSON_ASSERT (status == V_OCSP_CERTSTATUS_REVOKED);

   ASN1_GENERALIZEDTIME_free (next_update_in);
   next_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL) - 999 /* some time in the past */);

   _mongoc_ocsp_cache_set_resp (id, V_OCSP_CERTSTATUS_GOOD, 0, NULL, next_update_in);
   BSON_ASSERT (_mongoc_ocsp_cache_length () == 1);

   BSON_ASSERT (_mongoc_ocsp_cache_get_status (id, &status, &reason, &this_update_out, &next_update_out));
   BSON_ASSERT (status == V_OCSP_CERTSTATUS_REVOKED);

   CLEAR_CACHE;

   ASN1_GENERALIZEDTIME_free (next_update_in);
   OCSP_CERTID_free (id);
}

static void
test_mongoc_cache_remove_expired_cert (void)
{
   ASN1_GENERALIZEDTIME *this_update_in, *next_update_in;
   int status = V_OCSP_CERTSTATUS_GOOD, reason = OCSP_REVOKED_STATUS_NOSTATUS;
   OCSP_CERTID *id = create_cert_id (1);

   CLEAR_CACHE;

   next_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL) - 1);
   this_update_in = ASN1_GENERALIZEDTIME_set (NULL, time (NULL) - 999);

   _mongoc_ocsp_cache_set_resp (id, status, reason, this_update_in, next_update_in);

   BSON_ASSERT (_mongoc_ocsp_cache_length () == 1);
   BSON_ASSERT (!_mongoc_ocsp_cache_get_status (id, NULL, NULL, NULL, NULL));
   BSON_ASSERT (_mongoc_ocsp_cache_length () == 0);

   OCSP_CERTID_free (id);
   ASN1_GENERALIZEDTIME_free (next_update_in);
   ASN1_GENERALIZEDTIME_free (this_update_in);

   CLEAR_CACHE;
}

void
test_ocsp_cache_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/OCSPCache/insert", test_mongoc_cache_insert);
   TestSuite_Add (suite, "/OCSPCache/update", test_mongoc_cache_update);
   TestSuite_Add (suite, "/OCSPCache/remove_expired_cert", test_mongoc_cache_remove_expired_cert);
}
#else
extern int no_mongoc_ocsp;
#endif /* MONGOC_ENABLE_OCSP_OPENSSL */
