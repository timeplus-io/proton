#include <mongoc/service-gcp.h>
#include <mongoc/mongoc-host-list-private.h>
#include "TestSuite.h"

static void
_test_gcp_parse (void)
{
   // Test that we correctly parse the JSON returned by the GCP metadata server
   bson_error_t error;
   gcp_service_account_token token;

   // server output must be json data
   ASSERT (!gcp_access_token_try_parse_from_json (&token, "invalid json", -1, &error));
   ASSERT_CMPUINT32 (error.domain, ==, BSON_ERROR_JSON);

   // server output must contain access_token
   ASSERT (!gcp_access_token_try_parse_from_json (&token, "{}", -1, &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_GCP, MONGOC_ERROR_KMS_SERVER_BAD_JSON, "");

   // server output must contain a value for access_token
   ASSERT (!gcp_access_token_try_parse_from_json (&token, BSON_STR ({"access_token" : null}), -1, &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_GCP, MONGOC_ERROR_KMS_SERVER_BAD_JSON, "One or more required JSON");

   // server output must contain token_type
   ASSERT (!gcp_access_token_try_parse_from_json (&token, BSON_STR ({"access_token" : "helloworld"}), -1, &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_GCP, MONGOC_ERROR_KMS_SERVER_BAD_JSON, "One or more required JSON");

   // can successfully parse JSON datat into a gcp_service_account_token
   ASSERT (gcp_access_token_try_parse_from_json (
      &token, BSON_STR ({"access_token" : "helloworld", "token_type" : "bearer", "expires_in" : "3788"}), -1, &error));
   ASSERT_ERROR_CONTAINS (error, 0, 0, "");
   ASSERT_CMPSTR (token.access_token, "helloworld");
   ASSERT_CMPSTR (token.token_type, "bearer");

   gcp_access_token_destroy (&token);
}


static void
_test_gcp_http_request (void)
{
   // Test that we correctly build a http request for the GCP metadata server
   gcp_request req;
   gcp_request_init (&req, "helloworld.com", 1234, NULL);
   bson_string_t *req_str = _mongoc_http_render_request_head (&req.req);
   gcp_request_destroy (&req);
   ASSERT_CMPSTR (req_str->str,
                  "GET "
                  "/computeMetadata/v1/instance/service-accounts/default/token HTTP/1.0\r\n"
                  "Host: helloworld.com:1234\r\n"
                  "Connection: close\r\n"
                  "Metadata-Flavor: Google\r\n"
                  "\r\n");
   bson_string_free (req_str, true);
}

static const char *
_get_test_host (void)
{
   return getenv ("TEST_KMS_PROVIDER_HOST");
}

static void
_run_http_test_case (const char *case_,
                     mongoc_error_domain_t expect_domain,
                     mongoc_error_code_t expect_code,
                     const char *expect_error_message)
{
   bson_error_t error = {0};
   struct _mongoc_host_list_t host;
   _mongoc_host_list_from_string_with_err (&host, _get_test_host (), &error);
   ASSERT_ERROR_CONTAINS (error, 0, 0, "");

   gcp_service_account_token token = {0};
   char *const header = bson_strdup_printf ("X-MongoDB-HTTP-TestParams: case=%s\r\n", case_);
   gcp_access_token_from_gcp_server (&token, host.host, host.port, header, &error);
   bson_free (header);
   gcp_access_token_destroy (&token);
   ASSERT_ERROR_CONTAINS (error, expect_domain, expect_code, expect_error_message);
}

static void
_test_with_mock_server (void *ctx)
{
   BSON_UNUSED (ctx);
   _run_http_test_case ("", 0, 0, ""); // (No error)
   _run_http_test_case ("404", MONGOC_ERROR_GCP, MONGOC_ERROR_KMS_SERVER_HTTP, "");
   _run_http_test_case ("slow", MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "Timeout");
   _run_http_test_case ("empty-json", MONGOC_ERROR_GCP, MONGOC_ERROR_KMS_SERVER_BAD_JSON, "");
   _run_http_test_case ("bad-json", MONGOC_ERROR_CLIENT, MONGOC_ERROR_STREAM_INVALID_TYPE, "");
   _run_http_test_case ("giant", MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "too large");
}

static int
have_mock_server_env (TestSuite *ctx)
{
   BSON_UNUSED (ctx);
   return _get_test_host () != NULL;
}

void
test_service_gcp_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/gcp/http/parse", _test_gcp_parse);
   TestSuite_Add (suite, "/gcp/http/request", _test_gcp_http_request);
   TestSuite_AddFull (suite, "/gcp/http/talk", _test_with_mock_server, NULL, NULL, have_mock_server_env);
}
