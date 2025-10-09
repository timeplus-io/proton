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

#ifndef BSONUTIL_BSON_PARSER_H
#define BSONUTIL_BSON_PARSER_H

#include "mongoc/mongoc.h"
#include "bson/bson.h"
#include "bsonutil/bson-val.h"

/* bson_parser_t is a very simplified parser to parse BSON fields into C values.
 * Example usage:
 *
 * typedef struct {
 *    char *name;
 *    bool *has_kids;
 *    bson_t *jobs;
 * } person_t;
 *
 * person_t person;
 * bson_parser_t *parser;
 * bson_t *bson = bson_from_file();
 *
 * parser = bson_parser_new ();
 * bson_parser_utf8 (parser, "name", &person.name);
 * bson_parser_bool_optional (parser, "hasKids", &person.has_kids);
 * bson_parser_array (parser, "jobs", &person.jobs);
 * bson_parser_parse_or_assert (parser, bson);
 *
 * bson_parser_destroy_with_parsed_fields (parser);
 *
 * This parses a document like:
 * { "name": "Kevin", "hasKids": false, "jobs": [ "mongodb", "alk" ] }
 * "name" is required. "hasKids" is optional.
 */
typedef struct _bson_parser_t bson_parser_t;

bson_parser_t *
bson_parser_new (void);

/* Permits extra fields to be ignored when parsing. */
void
bson_parser_allow_extra (bson_parser_t *bp, bool val);

/* Return extra fields a read-only bson_t. */
const bson_t *
bson_parser_get_extra (const bson_parser_t *bp);

void
bson_parser_destroy (bson_parser_t *bp);

/* bson_parser_destroy also destroys the outputs of all parsed fields. */
void
bson_parser_destroy_with_parsed_fields (bson_parser_t *parser);

void
bson_parser_utf8 (bson_parser_t *bp, const char *key, char **out);
void
bson_parser_utf8_optional (bson_parser_t *bp, const char *key, char **out);

void
bson_parser_doc (bson_parser_t *bp, const char *key, bson_t **out);
void
bson_parser_doc_optional (bson_parser_t *bp, const char *key, bson_t **out);

void
bson_parser_array (bson_parser_t *bp, const char *key, bson_t **out);
void
bson_parser_array_optional (bson_parser_t *bp, const char *key, bson_t **out);

void
bson_parser_array_or_doc (bson_parser_t *bp, const char *key, bson_t **out);
void
bson_parser_array_or_doc_optional (bson_parser_t *bp, const char *key, bson_t **out);

void
bson_parser_bool (bson_parser_t *bp, const char *key, bool **out);
void
bson_parser_bool_optional (bson_parser_t *bp, const char *key, bool **out);

/* Accepts either int32 or int64 */
void
bson_parser_int (bson_parser_t *bp, const char *key, int64_t **out);
void
bson_parser_int_optional (bson_parser_t *bp, const char *key, int64_t **out);

void
bson_parser_any (bson_parser_t *bp, const char *key, bson_val_t **out);
void
bson_parser_any_optional (bson_parser_t *bp, const char *key, bson_val_t **out);

void
bson_parser_write_concern (bson_parser_t *bp, mongoc_write_concern_t **out);
void
bson_parser_write_concern_optional (bson_parser_t *bp, mongoc_write_concern_t **out);

void
bson_parser_read_concern (bson_parser_t *bp, mongoc_read_concern_t **out);
void
bson_parser_read_concern_optional (bson_parser_t *bp, mongoc_read_concern_t **out);

void
bson_parser_read_prefs (bson_parser_t *bp, mongoc_read_prefs_t **out);
void
bson_parser_read_prefs_optional (bson_parser_t *bp, mongoc_read_prefs_t **out);

/* Attempt to parse @in into the fields that were registered. If parsing fails,
 * returns false and sets @error. */
bool
bson_parser_parse (bson_parser_t *bp, bson_t *in, bson_error_t *error);

/* Attempt to parse @in. If parsing fails, print an error and abort. */
void
bson_parser_parse_or_assert (bson_parser_t *bp, bson_t *in);

#endif /* BSONUTIL_BSON_PARSER_H */
