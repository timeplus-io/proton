/*
 * Copyright 2019-present MongoDB, Inc.
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

#include "mongoc-opts-helpers-private.h"
#include "mongoc-client-session-private.h"
#include "mongoc-write-concern-private.h"
#include "mongoc-util-private.h"
#include "mongoc-read-concern-private.h"

#define BSON_ERR(...)                                                                    \
   do {                                                                                  \
      bson_set_error (error, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, __VA_ARGS__); \
      return false;                                                                      \
   } while (0)


#define CONVERSION_ERR(...)                                                                        \
   do {                                                                                            \
      bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, __VA_ARGS__); \
      return false;                                                                                \
   } while (0)


bool
_mongoc_timestamp_empty (mongoc_timestamp_t *timestamp)
{
   return (timestamp->timestamp == 0 && timestamp->increment == 0);
}

void
_mongoc_timestamp_set (mongoc_timestamp_t *dst, mongoc_timestamp_t *src)
{
   dst->timestamp = src->timestamp;
   dst->increment = src->increment;
}

void
_mongoc_timestamp_set_from_bson (mongoc_timestamp_t *timestamp, bson_iter_t *iter)
{
   bson_iter_timestamp (iter, &(timestamp->timestamp), &(timestamp->increment));
}

void
_mongoc_timestamp_append (mongoc_timestamp_t *timestamp, bson_t *bson, char *key)
{
   const size_t len = strlen (key);
   BSON_ASSERT (bson_in_range_unsigned (int, len));
   bson_append_timestamp (bson, key, (int) len, timestamp->timestamp, timestamp->increment);
}

void
_mongoc_timestamp_clear (mongoc_timestamp_t *timestamp)
{
   timestamp->timestamp = 0;
   timestamp->increment = 0;
}

bool
_mongoc_convert_document (mongoc_client_t *client, const bson_iter_t *iter, bson_t *doc, bson_error_t *error)
{
   uint32_t len;
   const uint8_t *data;
   bson_t value;

   BSON_UNUSED (client);

   if (!BSON_ITER_HOLDS_DOCUMENT (iter)) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts, should contain document,"
                      " not %s",
                      bson_iter_key (iter),
                      _mongoc_bson_type_to_str (bson_iter_type (iter)));
   }

   bson_iter_document (iter, &len, &data);
   if (!bson_init_static (&value, data, len)) {
      BSON_ERR ("Corrupt BSON in field \"%s\" in opts", bson_iter_key (iter));
   }

   bson_destroy (doc);
   bson_copy_to (&value, doc);

   return true;
}

bool
_mongoc_convert_array (mongoc_client_t *client, const bson_iter_t *iter, bson_t *doc, bson_error_t *error)
{
   uint32_t len;
   const uint8_t *data;
   bson_t value;

   BSON_UNUSED (client);

   if (!BSON_ITER_HOLDS_ARRAY (iter)) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts, should contain array,"
                      " not %s",
                      bson_iter_key (iter),
                      _mongoc_bson_type_to_str (bson_iter_type (iter)));
   }

   bson_iter_array (iter, &len, &data);
   if (!bson_init_static (&value, data, len)) {
      BSON_ERR ("Corrupt BSON in field \"%s\" in opts", bson_iter_key (iter));
   }

   bson_destroy (doc);
   bson_copy_to (&value, doc);

   return true;
}

bool
_mongoc_convert_int64_positive (mongoc_client_t *client, const bson_iter_t *iter, int64_t *num, bson_error_t *error)
{
   int64_t i;

   BSON_UNUSED (client);

   if (!BSON_ITER_HOLDS_NUMBER (iter)) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts, should contain number,"
                      " not %s",
                      bson_iter_key (iter),
                      _mongoc_bson_type_to_str (bson_iter_type (iter)));
   }

   i = bson_iter_as_int64 (iter);
   if (i <= 0) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts, should be greater than 0,"
                      " not %" PRId64,
                      bson_iter_key (iter),
                      i);
   }

   *num = bson_iter_as_int64 (iter);
   return true;
}

bool
_mongoc_convert_int32_t (mongoc_client_t *client, const bson_iter_t *iter, int32_t *num, bson_error_t *error)
{
   int64_t i;

   BSON_UNUSED (client);

   if (!BSON_ITER_HOLDS_NUMBER (iter)) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts", bson_iter_key (iter));
   }

   i = bson_iter_as_int64 (iter);
   if (i > INT32_MAX || i < INT32_MIN) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts: %" PRId64 " out of range for int32", bson_iter_key (iter), i);
   }

   *num = (int32_t) i;

   return true;
}

bool
_mongoc_convert_int32_positive (mongoc_client_t *client, const bson_iter_t *iter, int32_t *num, bson_error_t *error)
{
   int32_t i;

   if (!_mongoc_convert_int32_t (client, iter, &i, error)) {
      return false;
   }

   if (i <= 0) {
      CONVERSION_ERR ("Invalid field \"%s\" in opts, should be greater than 0, not %d", bson_iter_key (iter), i);
   }

   *num = i;

   return true;
}

bool
_mongoc_convert_bool (mongoc_client_t *client, const bson_iter_t *iter, bool *flag, bson_error_t *error)
{
   BSON_UNUSED (client);

   if (BSON_ITER_HOLDS_BOOL (iter)) {
      *flag = bson_iter_bool (iter);
      return true;
   }

   CONVERSION_ERR ("Invalid field \"%s\" in opts, should contain bool,"
                   " not %s",
                   bson_iter_key (iter),
                   _mongoc_bson_type_to_str (bson_iter_type (iter)));
}

bool
_mongoc_convert_bson_value_t (mongoc_client_t *client,
                              const bson_iter_t *iter,
                              bson_value_t *value,
                              bson_error_t *error)
{
   BSON_UNUSED (client);
   BSON_UNUSED (error);

   bson_value_copy (bson_iter_value ((bson_iter_t *) iter), value);

   return true;
}

bool
_mongoc_convert_timestamp (mongoc_client_t *client,
                           const bson_iter_t *iter,
                           mongoc_timestamp_t *timestamp,
                           bson_error_t *error)
{
   BSON_UNUSED (client);
   BSON_UNUSED (error);

   bson_iter_timestamp (iter, &timestamp->timestamp, &timestamp->increment);

   return true;
}

bool
_mongoc_convert_utf8 (mongoc_client_t *client, const bson_iter_t *iter, const char **str, bson_error_t *error)
{
   BSON_UNUSED (client);

   if (BSON_ITER_HOLDS_UTF8 (iter)) {
      *str = bson_iter_utf8 (iter, NULL);
      return true;
   }

   CONVERSION_ERR ("Invalid field \"%s\" in opts, should contain string,"
                   " not %s",
                   bson_iter_key (iter),
                   _mongoc_bson_type_to_str (bson_iter_type (iter)));
}

bool
_mongoc_convert_validate_flags (mongoc_client_t *client,
                                const bson_iter_t *iter,
                                bson_validate_flags_t *flags,
                                bson_error_t *error)
{
   BSON_UNUSED (client);

   if (BSON_ITER_HOLDS_BOOL (iter)) {
      if (!bson_iter_as_bool (iter)) {
         *flags = BSON_VALIDATE_NONE;
         return true;
      } else {
         /* validate: false is ok but validate: true is prohibited */
         CONVERSION_ERR ("Invalid option \"%s\": true, must be a bitwise-OR of"
                         " bson_validate_flags_t values.",
                         bson_iter_key (iter));
      }
   } else if (BSON_ITER_HOLDS_INT32 (iter)) {
      if (bson_iter_int32 (iter) <= 0x1F) {
         *flags = (bson_validate_flags_t) bson_iter_int32 (iter);
         return true;
      } else {
         CONVERSION_ERR ("Invalid field \"%s\" in opts, must be a bitwise-OR of"
                         " bson_validate_flags_t values.",
                         bson_iter_key (iter));
      }
   }
   CONVERSION_ERR ("Invalid type for option \"%s\": \"%s\"."
                   " \"%s\" must be a boolean or a bitwise-OR of"
                   " bson_validate_flags_t values.",
                   bson_iter_key (iter),
                   _mongoc_bson_type_to_str (bson_iter_type (iter)),
                   bson_iter_key (iter));
}

bool
_mongoc_convert_write_concern (mongoc_client_t *client,
                               const bson_iter_t *iter,
                               mongoc_write_concern_t **wc,
                               bson_error_t *error)
{
   mongoc_write_concern_t *tmp;

   BSON_UNUSED (client);

   tmp = _mongoc_write_concern_new_from_iter (iter, error);
   if (tmp) {
      *wc = tmp;
      return true;
   }

   return false;
}

bool
_mongoc_convert_server_id (mongoc_client_t *client, const bson_iter_t *iter, uint32_t *server_id, bson_error_t *error)
{
   int64_t tmp;

   BSON_UNUSED (client);

   if (!BSON_ITER_HOLDS_INT (iter)) {
      CONVERSION_ERR ("The serverId option must be an integer");
   }

   tmp = bson_iter_as_int64 (iter);
   if (tmp <= 0) {
      CONVERSION_ERR ("The serverId option must be >= 1");
   }

   *server_id = (uint32_t) tmp;
   return true;
}

bool
_mongoc_convert_read_concern (mongoc_client_t *client,
                              const bson_iter_t *iter,
                              mongoc_read_concern_t **rc,
                              bson_error_t *error)
{
   BSON_UNUSED (client);

   *rc = _mongoc_read_concern_new_from_iter (iter, error);
   if (!*rc) {
      return false;
   }
   return true;
}

bool
_mongoc_convert_hint (mongoc_client_t *client, const bson_iter_t *iter, bson_value_t *value, bson_error_t *error)
{
   BSON_UNUSED (client);

   if (BSON_ITER_HOLDS_UTF8 (iter) || BSON_ITER_HOLDS_DOCUMENT (iter)) {
      bson_value_copy (bson_iter_value ((bson_iter_t *) iter), value);
      return true;
   }

   CONVERSION_ERR ("The hint option must be a string or document");
}
