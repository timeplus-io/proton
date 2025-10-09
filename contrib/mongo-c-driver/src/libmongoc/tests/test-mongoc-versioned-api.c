/*
 * Copyright 2021 MongoDB, Inc.
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

#include <mongoc/mongoc.h>
#include "mongoc-client-private.h"
#include "mongoc-server-api-private.h"
#include "test-libmongoc.h"

#include "unified/runner.h"

static void
_test_mongoc_server_api_copy (void)
{
   mongoc_server_api_t *api;
   mongoc_server_api_t *copy;

   BSON_ASSERT (!mongoc_server_api_copy (NULL));

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
   mongoc_server_api_strict (api, false);
   copy = mongoc_server_api_copy (api);

   BSON_ASSERT (api->version == copy->version);
   BSON_ASSERT (!copy->strict.value);
   BSON_ASSERT (copy->strict.is_set);
   BSON_ASSERT (!copy->deprecation_errors.value);
   BSON_ASSERT (!copy->deprecation_errors.is_set);

   mongoc_server_api_destroy (api);
   mongoc_server_api_destroy (copy);
}

static void
_test_mongoc_server_api_setters (void)
{
   mongoc_server_api_t *api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   BSON_ASSERT (api->version == MONGOC_SERVER_API_V1);
   BSON_ASSERT (!api->strict.is_set);
   BSON_ASSERT (!api->deprecation_errors.is_set);
   BSON_ASSERT (!api->strict.value);
   BSON_ASSERT (!api->deprecation_errors.value);

   mongoc_server_api_strict (api, true);
   BSON_ASSERT (api->strict.is_set);
   BSON_ASSERT (api->strict.value);

   mongoc_server_api_deprecation_errors (api, false);
   BSON_ASSERT (api->deprecation_errors.is_set);
   BSON_ASSERT (!api->deprecation_errors.value);

   mongoc_server_api_destroy (api);
}

static void
_test_mongoc_server_api_client (void)
{
   mongoc_client_t *client;
   mongoc_server_api_t *api;
   bson_error_t error;

   /* We use mongoc_client_new() both to avoid having a server API set
    * and also to avoid connecting to a server: */
   client = mongoc_client_new ("mongodb://localhost");
   BSON_ASSERT (!client->api);

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
   BSON_ASSERT (client->api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   /* Cannot change server API once it is set */
   ASSERT (!mongoc_client_set_server_api (client, api, &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_API_ALREADY_SET, "Cannot set server api more than once");

   /* client gets its own internal copy */
   mongoc_server_api_destroy (api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   mongoc_client_destroy (client);
}

static void
_test_mongoc_server_api_client_pool (void)
{
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_api_t *api;
   bson_error_t error;

   uri = mongoc_uri_new ("mongodb://localhost");
   pool = mongoc_client_pool_new (uri);

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, api, &error), error);

   /* Cannot change server API once it is set */
   ASSERT (!mongoc_client_pool_set_server_api (pool, api, &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_POOL, MONGOC_ERROR_POOL_API_ALREADY_SET, "Cannot set server api more than once");

   /* Clients popped from pool have matching API */
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client->api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   ASSERT (!mongoc_client_set_server_api (client, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_API_FROM_POOL,
                          "Cannot set server api on a client checked out from a pool");

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_server_api_destroy (api);
   mongoc_uri_destroy (uri);
}

static void
_test_mongoc_server_api_client_pool_once (void)
{
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_api_t *api;
   bson_error_t error;

   uri = mongoc_uri_new ("mongodb://localhost");
   pool = mongoc_client_pool_new (uri);

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (!client->api);

   /* Cannot change server API once a client has been popped. */
   ASSERT (!mongoc_client_pool_set_server_api (pool, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_POOL,
                          MONGOC_ERROR_POOL_API_TOO_LATE,
                          "Cannot set server api after a client has been created");

   ASSERT (!mongoc_client_set_server_api (client, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_API_FROM_POOL,
                          "Cannot set server api on a client checked out from a pool");

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_server_api_destroy (api);
   mongoc_uri_destroy (uri);
}

static void
_test_mongoc_client_uses_server_api (void)
{
   mongoc_client_t *client0; /* versioned */
   mongoc_client_t *client1; /* not versioned */
   mongoc_server_api_t *api;
   bson_error_t error;

   /* We go through mongoc_client_new() rather than
    * test_mongoc_client_uses_no_server_api() because we want no API to be set
    * (directly, or through the environment) and /also/ no
    * attempt to connect to a server: */
   client0 = mongoc_client_new ("mongodb://localhost");
   client1 = mongoc_client_new ("mongodb://localhost");

   /* Ensure that neither client has an API set: */
   ASSERT (!mongoc_client_uses_server_api (client0));
   ASSERT (!mongoc_client_uses_server_api (client1));

   /* Set the API on one and only one client: */
   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
   ASSERT_OR_PRINT (mongoc_client_set_server_api (client0, api, &error), error);

   /* Check to see that we can distinguish whether or not the API was set via
   our function under test: */
   ASSERT (mongoc_client_uses_server_api (client0));
   ASSERT (!mongoc_client_uses_server_api (client1));

   /* Tidy up: */
   mongoc_server_api_destroy (api);

   mongoc_client_destroy (client0);
   mongoc_client_destroy (client1);
}

void
test_client_versioned_api_install (TestSuite *suite)
{
   run_unified_tests (suite, JSON_DIR, "versioned_api");

   TestSuite_Add (suite, "/VersionedApi/client", _test_mongoc_server_api_client);
   TestSuite_Add (suite, "/VersionedApi/client_pool", _test_mongoc_server_api_client_pool);
   TestSuite_Add (suite, "/VersionedApi/client_pool_once", _test_mongoc_server_api_client_pool_once);
   TestSuite_Add (suite, "/VersionedApi/copy", _test_mongoc_server_api_copy);
   TestSuite_Add (suite, "/VersionedApi/setters", _test_mongoc_server_api_setters);
   TestSuite_Add (suite, "/VersionedApi/private/client_uses_server_api", _test_mongoc_client_uses_server_api);
}
