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
#include "test-libmongoc.h"
#include "mongoc/mongoc.h"
#include "mongoc/mongoc-http-private.h"

void
test_mongoc_http_get (void *unused)
{
   mongoc_http_request_t req;
   mongoc_http_response_t res;
   bool r;
   bson_error_t error = {0};

   BSON_UNUSED (unused);

   _mongoc_http_request_init (&req);
   _mongoc_http_response_init (&res);

   /* Basic GET request */
   req.method = "GET";
   req.host = "localhost";
   req.path = "get";
   req.port = 18000;
   r = _mongoc_http_send (&req, 10000, false, NULL, &res, &error);
   ASSERT_OR_PRINT (r, error);

   ASSERT_WITH_MSG (res.status == 200,
                    "unexpected status code %d\n"
                    "RESPONSE BODY BEGIN\n"
                    "%s"
                    "RESPONSE BODY END\n",
                    res.status,
                    res.body_len > 0 ? res.body : "");
   ASSERT_CMPINT (res.body_len, >, 0);
   _mongoc_http_response_cleanup (&res);
}

void
test_mongoc_http_post (void *unused)
{
   mongoc_http_request_t req;
   mongoc_http_response_t res;
   bool r;
   bson_error_t error = {0};

   BSON_UNUSED (unused);

   _mongoc_http_request_init (&req);
   _mongoc_http_response_init (&res);

   /* Basic POST request with a body. */
   req.method = "POST";
   req.host = "localhost";
   req.path = "post";
   req.port = 18000;
   r = _mongoc_http_send (&req, 10000, false, NULL, &res, &error);
   ASSERT_OR_PRINT (r, error);

   ASSERT_WITH_MSG (res.status == 200,
                    "unexpected status code %d\n"
                    "RESPONSE BODY BEGIN\n"
                    "%s"
                    "RESPONSE BODY END\n",
                    res.status,
                    res.body_len > 0 ? res.body : "");
   ASSERT_CMPINT (res.body_len, >, 0);
   _mongoc_http_response_cleanup (&res);
}

void
test_http_install (TestSuite *suite)
{
   TestSuite_AddFull (
      suite, "/http/get", test_mongoc_http_get, NULL /* dtor */, NULL /* ctx */, test_framework_skip_if_offline);

   TestSuite_AddFull (
      suite, "/http/post", test_mongoc_http_post, NULL /* dtor */, NULL /* ctx */, test_framework_skip_if_offline);
}
