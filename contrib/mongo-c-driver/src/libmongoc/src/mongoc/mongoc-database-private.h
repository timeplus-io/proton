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

#ifndef MONGOC_DATABASE_PRIVATE_H
#define MONGOC_DATABASE_PRIVATE_H

#include <bson/bson.h>

#include "mongoc-client.h"
#include "mongoc-read-prefs.h"
#include "mongoc-read-concern.h"
#include "mongoc-write-concern.h"

BSON_BEGIN_DECLS


struct _mongoc_database_t {
   mongoc_client_t *client;
   char *name;
   mongoc_read_prefs_t *read_prefs;
   mongoc_read_concern_t *read_concern;
   mongoc_write_concern_t *write_concern;
};


mongoc_database_t *
_mongoc_database_new (mongoc_client_t *client,
                      const char *name,
                      const mongoc_read_prefs_t *read_prefs,
                      const mongoc_read_concern_t *read_concern,
                      const mongoc_write_concern_t *write_concern);

/* _mongoc_get_encryptedFields_from_map checks the collection has an
 * encryptedFields set on the client encryptedFieldsMap.
 * encryptedFields is always initialized on return.
 */
bool
_mongoc_get_encryptedFields_from_map (
   mongoc_client_t *client, const char *dbName, const char *collName, bson_t *encryptedFields, bson_error_t *error);

/* _mongoc_get_encryptedFields_from_map checks the collection has an
 * encryptedFields by running listCollections.
 * encryptedFields is always initialized on return.
 */
bool
_mongoc_get_encryptedFields_from_server (
   mongoc_client_t *client, const char *dbName, const char *collName, bson_t *encryptedFields, bson_error_t *error);

/**
 * @brief Look up the encryptedFields to use for the given collection.
 *
 * If the collection options contains an encryptedFields, those are returned.
 * If the client has an encryptedFieldsMap entry for the collection within the
 * given database, those are returned. If neither, an empty document is
 * returned.
 *
 * @param client The client with which to search an encryptedFieldsMap
 * @param dbName The name of the database where the collection will/does live
 * @param collName The name of the collection
 * @param opts (Optional) The collection options, which may contain the
 * fields
 * @param checkEncryptedFieldsMap If false, the encryptedFieldsMap will not be
 * checked.
 * @param[out] encryptedFields An output where a view of the encryptedFields
 * will be written
 * @param[out] error An error output
 * @retval true If there was no error
 * @retval false Otherwise
 *
 * @note Upon returning `true`, check whether `*encryptedFields` is empty to
 * determine whether fields have been found.
 */
bool
_mongoc_get_collection_encryptedFields (mongoc_client_t *client,
                                        const char *dbName,
                                        const char *collName,
                                        const bson_t *opts,
                                        bool checkEncryptedFieldsMap,
                                        bson_t *encryptedFields,
                                        bson_error_t *error);

/* _mongoc_get_encryptedField_state_collection returns the state collection
 * name. Returns NULL on error. */
char *
_mongoc_get_encryptedField_state_collection (const bson_t *encryptedFields,
                                             const char *data_collection,
                                             const char *state_collection_suffix,
                                             bson_error_t *error);

BSON_END_DECLS


#endif /* MONGOC_DATABASE_PRIVATE_H */
