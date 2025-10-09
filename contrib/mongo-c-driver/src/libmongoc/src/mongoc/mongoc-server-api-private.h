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

#include "mongoc-prelude.h"

#ifndef MONGOC_SERVER_API_PRIVATE_H
#define MONGOC_SERVER_API_PRIVATE_H

#include "mongoc-server-api.h"

struct _mongoc_server_api_t {
   mongoc_server_api_version_t version;
   mongoc_optional_t strict;
   mongoc_optional_t deprecation_errors;
};

#endif /* MONGOC_SERVER_API_PRIVATE_H */
