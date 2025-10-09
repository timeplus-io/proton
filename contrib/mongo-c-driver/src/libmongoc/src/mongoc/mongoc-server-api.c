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


#include "mongoc-config.h"
#include "mongoc-server-api-private.h"

const char *
mongoc_server_api_version_to_string (mongoc_server_api_version_t version)
{
   switch (version) {
   case MONGOC_SERVER_API_V1:
      return "1";
   default:
      return NULL;
   }
}

bool
mongoc_server_api_version_from_string (const char *version, mongoc_server_api_version_t *out)
{
   if (strcmp (version, "1") == 0) {
      *out = MONGOC_SERVER_API_V1;
      return true;
   }

   return false;
}

mongoc_server_api_t *
mongoc_server_api_new (mongoc_server_api_version_t version)
{
   mongoc_server_api_t *api;

   api = (mongoc_server_api_t *) bson_malloc0 (sizeof (mongoc_server_api_t));
   api->version = version;
   mongoc_optional_init (&api->strict);
   mongoc_optional_init (&api->deprecation_errors);

   return api;
}

mongoc_server_api_t *
mongoc_server_api_copy (const mongoc_server_api_t *api)
{
   mongoc_server_api_t *copy;

   if (!api) {
      return NULL;
   }

   copy = (mongoc_server_api_t *) bson_malloc0 (sizeof (mongoc_server_api_t));
   copy->version = api->version;
   mongoc_optional_copy (&api->strict, &copy->strict);
   mongoc_optional_copy (&api->deprecation_errors, &copy->deprecation_errors);

   return copy;
}

void
mongoc_server_api_destroy (mongoc_server_api_t *api)
{
   if (!api) {
      return;
   }

   bson_free (api);
}

void
mongoc_server_api_strict (mongoc_server_api_t *api, bool strict)
{
   BSON_ASSERT (api);
   mongoc_optional_set_value (&api->strict, strict);
}

void
mongoc_server_api_deprecation_errors (mongoc_server_api_t *api, bool deprecation_errors)
{
   BSON_ASSERT (api);
   mongoc_optional_set_value (&api->deprecation_errors, deprecation_errors);
}

const mongoc_optional_t *
mongoc_server_api_get_deprecation_errors (const mongoc_server_api_t *api)
{
   BSON_ASSERT (api);
   return &api->deprecation_errors;
}

const mongoc_optional_t *
mongoc_server_api_get_strict (const mongoc_server_api_t *api)
{
   BSON_ASSERT (api);
   return &api->strict;
}

mongoc_server_api_version_t
mongoc_server_api_get_version (const mongoc_server_api_t *api)
{
   BSON_ASSERT (api);
   return api->version;
}
