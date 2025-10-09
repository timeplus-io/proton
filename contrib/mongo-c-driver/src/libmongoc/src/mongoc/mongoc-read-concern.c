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


#include "mongoc-error.h"
#include "mongoc-log.h"
#include "mongoc-read-concern.h"
#include "mongoc-read-concern-private.h"


static void
_mongoc_read_concern_freeze (mongoc_read_concern_t *read_concern);


/**
 * mongoc_read_concern_new:
 *
 * Create a new mongoc_read_concern_t.
 *
 * Returns: A newly allocated mongoc_read_concern_t. This should be freed
 *    with mongoc_read_concern_destroy().
 */
mongoc_read_concern_t *
mongoc_read_concern_new (void)
{
   mongoc_read_concern_t *read_concern;

   read_concern = BSON_ALIGNED_ALLOC0 (mongoc_read_concern_t);

   bson_init (&read_concern->compiled);

   return read_concern;
}


mongoc_read_concern_t *
mongoc_read_concern_copy (const mongoc_read_concern_t *read_concern)
{
   mongoc_read_concern_t *ret = NULL;

   if (read_concern) {
      ret = mongoc_read_concern_new ();
      ret->level = bson_strdup (read_concern->level);
   }

   return ret;
}


/**
 * mongoc_read_concern_destroy:
 * @read_concern: A mongoc_read_concern_t.
 *
 * Releases a mongoc_read_concern_t and all associated memory.
 */
void
mongoc_read_concern_destroy (mongoc_read_concern_t *read_concern)
{
   if (read_concern) {
      bson_destroy (&read_concern->compiled);
      bson_free (read_concern->level);
      bson_free (read_concern);
   }
}


const char *
mongoc_read_concern_get_level (const mongoc_read_concern_t *read_concern)
{
   BSON_ASSERT (read_concern);

   return read_concern->level;
}


/**
 * mongoc_read_concern_set_level:
 * @read_concern: A mongoc_read_concern_t.
 * @level: The read concern level
 *
 * Sets the read concern level. Any string is supported for future compatibility
 * but MongoDB 3.2 only accepts "local" and "majority", aka:
 *  - MONGOC_READ_CONCERN_LEVEL_LOCAL
 *  - MONGOC_READ_CONCERN_LEVEL_MAJORITY
 * MongoDB 3.4 added
 *  - MONGOC_READ_CONCERN_LEVEL_LINEARIZABLE
 *
 * See the MongoDB docs for more information on readConcernLevel
 */
bool
mongoc_read_concern_set_level (mongoc_read_concern_t *read_concern, const char *level)
{
   BSON_ASSERT (read_concern);

   bson_free (read_concern->level);
   read_concern->level = bson_strdup (level);
   read_concern->frozen = false;

   return true;
}

/**
 * mongoc_read_concern_append:
 * @read_concern: (in): A mongoc_read_concern_t.
 * @opts: (out): A pointer to a bson document.
 *
 * Appends a read_concern document to command options to send to
 * a server.
 *
 * Returns true on success, false on failure.
 *
 */
bool
mongoc_read_concern_append (mongoc_read_concern_t *read_concern, bson_t *command)
{
   BSON_ASSERT (read_concern);

   if (!read_concern->level) {
      return true;
   }

   if (!bson_append_document (command, "readConcern", 11, _mongoc_read_concern_get_bson (read_concern))) {
      MONGOC_ERROR ("Could not append readConcern to command.");
      return false;
   }

   return true;
}


/**
 * mongoc_read_concern_is_default:
 * @read_concern: A const mongoc_read_concern_t.
 *
 * Returns true when read_concern has not been modified.
 */
bool
mongoc_read_concern_is_default (const mongoc_read_concern_t *read_concern)
{
   return !read_concern || !read_concern->level;
}


/**
 * mongoc_read_concern_get_bson:
 * @read_concern: A mongoc_read_concern_t.
 *
 * This is an internal function.
 *
 * Returns: A bson_t representing the read concern, which is owned by the
 *    mongoc_read_concern_t instance and should not be modified or freed.
 */
const bson_t *
_mongoc_read_concern_get_bson (mongoc_read_concern_t *read_concern)
{
   if (!read_concern->frozen) {
      _mongoc_read_concern_freeze (read_concern);
   }

   return &read_concern->compiled;
}


/**
 * _mongoc_read_concern_new_from_iter:
 *
 * Create a new mongoc_read_concern_t from an iterator positioned on
 * a "readConcern" document.
 *
 * Returns: A newly allocated mongoc_read_concern_t. This should be freed
 *    with mongoc_read_concern_destroy().
 */
mongoc_read_concern_t *
_mongoc_read_concern_new_from_iter (const bson_iter_t *iter, bson_error_t *error)
{
   bson_iter_t inner;
   mongoc_read_concern_t *read_concern;

   BSON_ASSERT (iter);

   read_concern = mongoc_read_concern_new ();
   if (!BSON_ITER_HOLDS_DOCUMENT (iter)) {
      goto fail;
   }

   BSON_ASSERT (bson_iter_recurse (iter, &inner));
   if (!bson_iter_find (&inner, "level") || !BSON_ITER_HOLDS_UTF8 (&inner)) {
      goto fail;
   }

   mongoc_read_concern_set_level (read_concern, bson_iter_utf8 (&inner, NULL));

   return read_concern;

fail:
   bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid readConcern");

   mongoc_read_concern_destroy (read_concern);
   return NULL;
}


/**
 * mongoc_read_concern_freeze:
 * @read_concern: A mongoc_read_concern_t.
 *
 * This is an internal function.
 *
 * Encodes the read concern into a bson_t, which may then be returned by
 * mongoc_read_concern_get_bson().
 */
static void
_mongoc_read_concern_freeze (mongoc_read_concern_t *read_concern)
{
   bson_t *compiled;

   BSON_ASSERT (read_concern);

   compiled = &read_concern->compiled;

   read_concern->frozen = true;

   bson_reinit (compiled);

   if (read_concern->level) {
      BSON_APPEND_UTF8 (compiled, "level", read_concern->level);
   }
}
