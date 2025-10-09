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

#include "mongoc-prelude.h"

#ifndef MONGOC_HOST_LIST_PRIVATE_H
#define MONGOC_HOST_LIST_PRIVATE_H

#include "mongoc-host-list.h"


BSON_BEGIN_DECLS

mongoc_host_list_t *
_mongoc_host_list_push (const char *host, uint16_t port, int family, mongoc_host_list_t *next);

void
_mongoc_host_list_upsert (mongoc_host_list_t **list, const mongoc_host_list_t *new_host);

mongoc_host_list_t *
_mongoc_host_list_copy_all (const mongoc_host_list_t *src);

bool
_mongoc_host_list_from_string (mongoc_host_list_t *host_list, const char *host_and_port);

bool
_mongoc_host_list_from_string_with_err (mongoc_host_list_t *host_list, const char *host_and_port, bson_error_t *error);

bool
_mongoc_host_list_from_hostport_with_err (mongoc_host_list_t *host_list,
                                          const char *host,
                                          uint16_t port,
                                          bson_error_t *error);

size_t
_mongoc_host_list_length (const mongoc_host_list_t *list);

bool
_mongoc_host_list_compare_one (const mongoc_host_list_t *host_a, const mongoc_host_list_t *host_b);

void
_mongoc_host_list_remove_host (mongoc_host_list_t **phosts, const char *host, uint16_t port);

void
_mongoc_host_list_destroy_all (mongoc_host_list_t *host);

bool
_mongoc_host_list_contains_one (mongoc_host_list_t *host_list, mongoc_host_list_t *host);

BSON_END_DECLS


#endif /* MONGOC_HOST_LIST_PRIVATE_H */
