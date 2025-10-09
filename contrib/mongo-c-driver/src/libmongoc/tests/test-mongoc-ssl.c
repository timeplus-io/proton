/*
 * Copyright 2021-present MongoDB, Inc.
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
#include "mongoc-config.h"
#include "test-conveniences.h"

#ifdef MONGOC_ENABLE_SSL
#include "mongoc-ssl-private.h"

typedef struct {
   const char *description;
   const char *bson;
   const char *expect_error;
   const char *expect_pem_file;
   const char *expect_pem_pwd;
   const char *expect_ca_file;
   bool expect_weak_cert_validation;
   bool expect_allow_invalid_hostname;
   bool expect_disable_ocsp_endpoint_check;
   bool expect_disable_certificate_revocation_check;
} testcase_t;

/*
The following are the only valid options for _mongoc_ssl_opts_from_bson:

MONGOC_URI_TLSCERTIFICATEKEYFILE "tlscertificatekeyfile"
MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD "tlscertificatekeyfilepassword"
MONGOC_URI_TLSCAFILE "tlscafile"
MONGOC_URI_TLSALLOWINVALIDCERTIFICATES "tlsallowinvalidcertificates"
MONGOC_URI_TLSALLOWINVALIDHOSTNAMES "tlsallowinvalidhostnames"
MONGOC_URI_TLSINSECURE "tlsinsecure"
MONGOC_URI_TLSDISABLECERTIFICATEREVOCATIONCHECK
"tlsdisablecertificaterevocationcheck"
MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK "tlsdisableocspendpointcheck"
*/

static void
test_mongoc_ssl_opts_from_bson (void)
{
   testcase_t tests[] = {{
                            "test all options set",
                            "{'tlsCertificateKeyFile': 'test_pem_file', "
                            "'tlsCertificateKeyFilePassword': 'test_pem_pwd', 'tlsCAFile': "
                            "'test_ca_file', 'tlsAllowInvalidCertificates': true, "
                            "'tlsAllowInvalidHostnames': true, 'tlsInsecure': true, "
                            "'tlsDisableCertificateRevocationCheck': true, "
                            "'tlsDisableOCSPEndpointCheck': true }",
                            NULL /* expect_error */,
                            "test_pem_file" /* pem_file */,
                            "test_pem_pwd" /* pem_pwd */,
                            "test_ca_file" /* ca_file */,
                            true /* weak_cert_validation */,
                            true /* allow_invalid_hostname */,
                            true /* disable_ocsp_endpoint_check */,
                            true /* disable_certificate_revocation_check */
                         },
                         {
                            "test options are case insentive",
                            "{'tlscertificatekeyfile': 'test_pem_file', "
                            "'tlscertificatekeyfilepassword': 'test_pem_pwd', 'tlscafile': "
                            "'test_ca_file', 'tlsallowinvalidcertificates': true, "
                            "'tlsallowinvalidhostnames': true, 'tlsinsecure': true, "
                            "'tlsdisablecertificaterevocationcheck': true, "
                            "'tlsdisableocspendpointcheck': true }",
                            NULL /* expect_error */,
                            "test_pem_file" /* pem_file */,
                            "test_pem_pwd" /* pem_pwd */,
                            "test_ca_file" /* ca_file */,
                            true /* weak_cert_validation */,
                            true /* allow_invalid_hostname */,
                            true /* disable_ocsp_endpoint_check */,
                            true /* disable_certificate_revocation_check */
                         },
                         {
                            "test no options set",
                            "{}",
                            NULL /* expect_error */,
                            NULL /* pem_file */,
                            NULL /* pem_pwd */,
                            NULL /* ca_file */,
                            false /* weak_cert_validation */,
                            false /* allow_invalid_hostname */,
                            false /* disable_ocsp_endpoint_check */,
                            false /* disable_certificate_revocation_check */
                         },
                         {
                            "test tlsInsecure overrides tlsAllowInvalidHostnames and "
                            "tlsAllowInvalidCertificates set",
                            "{'tlsInsecure': true, 'tlsAllowInvalidHostnames': false, "
                            "'tlsAllowInvalidCertificates': false}",
                            NULL /* expect_error */,
                            NULL /* pem_file */,
                            NULL /* pem_pwd */,
                            NULL /* ca_file */,
                            true /* weak_cert_validation */,
                            true /* allow_invalid_hostname */,
                            false /* disable_ocsp_endpoint_check */,
                            false /* disable_certificate_revocation_check */
                         },
                         {
                            "test unrecognized option",
                            "{'foo': true }",
                            "unexpected BOOL option: foo" /* expect_error */,
                            NULL /* pem_file */,
                            NULL /* pem_pwd */,
                            NULL /* ca_file */,
                            false /* weak_cert_validation */,
                            false /* allow_invalid_hostname */,
                            false /* disable_ocsp_endpoint_check */,
                            false /* disable_certificate_revocation_check */
                         },
                         {
                            "test wrong value type",
                            "{'tlsCaFile': true }",
                            "unexpected BOOL option: tlsCaFile" /* expect_error */,
                            NULL /* pem_file */,
                            NULL /* pem_pwd */,
                            NULL /* ca_file */,
                            false /* weak_cert_validation */,
                            false /* allow_invalid_hostname */,
                            false /* disable_ocsp_endpoint_check */,
                            false /* disable_certificate_revocation_check */
                         },
                         {0}};
   testcase_t *test;

   for (test = tests; test->bson != NULL; test++) {
      mongoc_ssl_opt_t ssl_opt = {0};
      bson_string_t *errmsg = bson_string_new (NULL);
      bool ok = _mongoc_ssl_opts_from_bson (&ssl_opt, tmp_bson (test->bson), errmsg);

      MONGOC_DEBUG ("testcase: %s", test->bson);
      if (test->expect_error) {
         ASSERT_CONTAINS (errmsg->str, test->expect_error);
         ASSERT (!ok);
      } else {
         if (!ok) {
            test_error ("unexpected error parsing: %s", errmsg->str);
         }
      }

      if (!test->expect_pem_file) {
         ASSERT (!ssl_opt.pem_file);
      } else {
         ASSERT (ssl_opt.pem_file);
         ASSERT_CMPSTR (test->expect_pem_file, ssl_opt.pem_file);
      }

      if (!test->expect_pem_pwd) {
         ASSERT (!ssl_opt.pem_pwd);
      } else {
         ASSERT (ssl_opt.pem_pwd);
         ASSERT_CMPSTR (test->expect_pem_pwd, ssl_opt.pem_pwd);
      }

      if (!test->expect_ca_file) {
         ASSERT (!ssl_opt.ca_file);
      } else {
         ASSERT (ssl_opt.ca_file);
         ASSERT_CMPSTR (test->expect_ca_file, ssl_opt.ca_file);
      }

      ASSERT (test->expect_weak_cert_validation == ssl_opt.weak_cert_validation);
      ASSERT (test->expect_allow_invalid_hostname == ssl_opt.allow_invalid_hostname);
      ASSERT (test->expect_disable_ocsp_endpoint_check == _mongoc_ssl_opts_disable_ocsp_endpoint_check (&ssl_opt));
      ASSERT (test->expect_disable_certificate_revocation_check ==
              _mongoc_ssl_opts_disable_certificate_revocation_check (&ssl_opt));

      /* It is not possible to set ca_dir or crl_file. */
      ASSERT (!ssl_opt.ca_dir);
      ASSERT (!ssl_opt.crl_file);

      _mongoc_ssl_opts_cleanup (&ssl_opt, true /* free_internal */);
      bson_string_free (errmsg, true /* free_segment */);
   }
}

/* Test that it is safe to call _mongoc_ssl_opts_cleanup on a zero'd struct. */
static void
test_mongoc_ssl_opts_cleanup_zero (void)
{
   mongoc_ssl_opt_t ssl_opt = {0};

   _mongoc_ssl_opts_cleanup (&ssl_opt, true /* free_internal */);
   _mongoc_ssl_opts_cleanup (&ssl_opt, false /* free_internal */);
}

#endif /* MONGOC_ENABLE_SSL */

void
test_ssl_install (TestSuite *suite)
{
#ifdef MONGOC_ENABLE_SSL
   TestSuite_Add (suite, "/ssl_opt/from_bson", test_mongoc_ssl_opts_from_bson);
   TestSuite_Add (suite, "/ssl_opt/cleanup", test_mongoc_ssl_opts_cleanup_zero);
#endif /* MONGOC_ENABLE_SSL */
}
