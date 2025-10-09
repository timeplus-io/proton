/*
 * Copyright 2024-present MongoDB, Inc.
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

#ifndef MCD_NSINFO_H
#define MCD_NSINFO_H

#include "mongoc-prelude.h"

#include <mongoc/mongoc-buffer-private.h>

// `mcd_nsinfo_t` builds the `nsInfo` payload for a `bulkWrite` command.
typedef struct _mcd_nsinfo_t mcd_nsinfo_t;

mcd_nsinfo_t *
mcd_nsinfo_new (void);

void
mcd_nsinfo_destroy (mcd_nsinfo_t *self);

// `mcd_nsinfo_append` adds `ns`. It is the callers responsibility to ensure duplicates are not inserted.
// Namespaces are assigned indexes in order of insertion, starting at 0.
// Returns the resulting non-negative index on success. Returns -1 on error.
int32_t
mcd_nsinfo_append (mcd_nsinfo_t *self, const char *ns, bson_error_t *error);

// `mcd_nsinfo_find` returns the non-negative index if found. Returns -1 if not found.
int32_t
mcd_nsinfo_find (const mcd_nsinfo_t *self, const char *ns);

// `mcd_nsinfo_get_bson_size` returns the size of the BSON document { "ns": "<ns>" }
// Useful for checking whether a namespace can be added without exceeding a size limit.
uint32_t
mcd_nsinfo_get_bson_size (const char *ns);

// `mcd_nsinfo_as_document_sequence` returns a document sequence.
// Useful for constructing an OP_MSG Section with payloadType=1.
const mongoc_buffer_t *
mcd_nsinfo_as_document_sequence (const mcd_nsinfo_t *self);

#endif // MCD_NSINFO_H
