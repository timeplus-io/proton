/*
 * Copyright 2015 MongoDB, Inc.
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

#ifndef MONGOC_READ_CONCERN_PRIVATE_H
#define MONGOC_READ_CONCERN_PRIVATE_H

#include <bson/bson.h>
#include "mongoc-read-concern.h"


BSON_BEGIN_DECLS


struct _mongoc_read_concern_t {
   char *level;
   bool frozen;
   bson_t compiled;
};


const bson_t *
_mongoc_read_concern_get_bson (mongoc_read_concern_t *read_concern);


mongoc_read_concern_t *
_mongoc_read_concern_new_from_iter (const bson_iter_t *iter, bson_error_t *error);

BSON_END_DECLS


#endif /* MONGOC_READ_CONCERN_PRIVATE_H */
