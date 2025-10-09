#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

#include "client-side-encryption-helpers.h"

/* Helper method to create and return a JSON schema to use for encryption.
The caller will use the returned schema for server-side encryption validation.
*/
static bson_t *
create_schema (bson_t *kms_providers,
               const char *keyvault_db,
               const char *keyvault_coll,
               mongoc_client_t *keyvault_client,
               bson_error_t *error)
{
   mongoc_client_encryption_t *client_encryption = NULL;
   mongoc_client_encryption_opts_t *client_encryption_opts = NULL;
   mongoc_client_encryption_datakey_opts_t *datakey_opts = NULL;
   bson_value_t datakey_id = {0};
   char *keyaltnames[] = {"mongoc_encryption_example_2"};
   bson_t *schema = NULL;

   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, keyvault_db, keyvault_coll);
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, keyvault_client);

   client_encryption = mongoc_client_encryption_new (client_encryption_opts, error);
   if (!client_encryption) {
      goto fail;
   }

   /* Create a new data key and json schema for the encryptedField.
    * https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules
    */
   datakey_opts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_keyaltnames (datakey_opts, keyaltnames, 1);
   if (!mongoc_client_encryption_create_datakey (client_encryption, "local", datakey_opts, &datakey_id, error)) {
      goto fail;
   }

   /* Create a schema describing that "encryptedField" is a string encrypted
    * with the newly created data key using deterministic encryption. */
   schema = BCON_NEW (
      "properties",
      "{",
      "encryptedField",
      "{",
      "encrypt",
      "{",
      "keyId",
      "[",
      BCON_BIN (datakey_id.value.v_binary.subtype, datakey_id.value.v_binary.data, datakey_id.value.v_binary.data_len),
      "]",
      "bsonType",
      "string",
      "algorithm",
      MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC,
      "}",
      "}",
      "}",
      "bsonType",
      "object");

fail:
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_datakey_opts_destroy (datakey_opts);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   bson_value_destroy (&datakey_id);
   return schema;
}

/* This example demonstrates how to use automatic encryption with a server-side
 * schema using the enterprise version of MongoDB */
int
main (void)
{
/* The collection used to store the encryption data keys. */
#define KEYVAULT_DB "encryption"
#define KEYVAULT_COLL "__libmongocTestKeyVault"
/* The collection used to store the encrypted documents in this example. */
#define ENCRYPTED_DB "test"
#define ENCRYPTED_COLL "coll"

   int exit_status = EXIT_FAILURE;
   bool ret;
   uint8_t *local_masterkey = NULL;
   uint32_t local_masterkey_len;
   bson_t *kms_providers = NULL;
   bson_error_t error = {0};
   bson_t *index_keys = NULL;
   bson_t *index_opts = NULL;
   mongoc_index_model_t *index_model = NULL;
   bson_json_reader_t *reader = NULL;
   bson_t *schema = NULL;

   /* The MongoClient used to access the key vault (keyvault_namespace). */
   mongoc_client_t *keyvault_client = NULL;
   mongoc_collection_t *keyvault_coll = NULL;
   mongoc_auto_encryption_opts_t *auto_encryption_opts = NULL;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *coll = NULL;
   bson_t *to_insert = NULL;
   mongoc_client_t *unencrypted_client = NULL;
   mongoc_collection_t *unencrypted_coll = NULL;
   bson_t *create_cmd = NULL;
   bson_t *create_cmd_opts = NULL;
   mongoc_write_concern_t *wc = NULL;

   mongoc_init ();

   /* Configure the master key. This must be the same master key that was used
    * to create
    * the encryption key. */
   local_masterkey = hex_to_bin (getenv ("LOCAL_MASTERKEY"), &local_masterkey_len);
   if (!local_masterkey || local_masterkey_len != 96) {
      fprintf (stderr,
               "Specify LOCAL_MASTERKEY environment variable as a "
               "secure random 96 byte hex value.\n");
      goto fail;
   }

   kms_providers = BCON_NEW ("local", "{", "key", BCON_BIN (0, local_masterkey, local_masterkey_len), "}");

   /* Set up the key vault for this example. */
   keyvault_client = mongoc_client_new ("mongodb://localhost/?appname=client-side-encryption-keyvault");
   BSON_ASSERT (keyvault_client);

   keyvault_coll = mongoc_client_get_collection (keyvault_client, KEYVAULT_DB, KEYVAULT_COLL);
   mongoc_collection_drop (keyvault_coll, NULL);

   /* Create a unique index to ensure that two data keys cannot share the same
    * keyAltName. This is recommended practice for the key vault. */
   index_keys = BCON_NEW ("keyAltNames", BCON_INT32 (1));
   index_opts = BCON_NEW ("unique",
                          BCON_BOOL (true),
                          "partialFilterExpression",
                          "{",
                          "keyAltNames",
                          "{",
                          "$exists",
                          BCON_BOOL (true),
                          "}",
                          "}");
   index_model = mongoc_index_model_new (index_keys, index_opts);
   ret = mongoc_collection_create_indexes_with_opts (
      keyvault_coll, &index_model, 1, NULL /* opts */, NULL /* reply */, &error);

   if (!ret) {
      goto fail;
   }

   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   mongoc_auto_encryption_opts_set_keyvault_client (auto_encryption_opts, keyvault_client);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, KEYVAULT_DB, KEYVAULT_COLL);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   schema = create_schema (kms_providers, KEYVAULT_DB, KEYVAULT_COLL, keyvault_client, &error);

   if (!schema) {
      goto fail;
   }

   client = mongoc_client_new ("mongodb://localhost/?appname=client-side-encryption");
   BSON_ASSERT (client);

   ret = mongoc_client_enable_auto_encryption (client, auto_encryption_opts, &error);
   if (!ret) {
      goto fail;
   }

   coll = mongoc_client_get_collection (client, ENCRYPTED_DB, ENCRYPTED_COLL);

   /* Clear old data */
   mongoc_collection_drop (coll, NULL);

   /* Create the collection with the encryption JSON Schema. */
   create_cmd = BCON_NEW ("create", ENCRYPTED_COLL, "validator", "{", "$jsonSchema", BCON_DOCUMENT (schema), "}");
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 0);
   create_cmd_opts = bson_new ();
   mongoc_write_concern_append (wc, create_cmd_opts);
   ret = mongoc_client_command_with_opts (
      client, ENCRYPTED_DB, create_cmd, NULL /* read prefs */, create_cmd_opts, NULL /* reply */, &error);
   if (!ret) {
      goto fail;
   }

   to_insert = BCON_NEW ("encryptedField", "123456789");
   ret = mongoc_collection_insert_one (coll, to_insert, NULL /* opts */, NULL /* reply */, &error);
   if (!ret) {
      goto fail;
   }
   printf ("decrypted document: ");
   if (!print_one_document (coll, &error)) {
      goto fail;
   }
   printf ("\n");

   unencrypted_client = mongoc_client_new ("mongodb://localhost/?appname=client-side-encryption-unencrypted");
   BSON_ASSERT (unencrypted_client);

   unencrypted_coll = mongoc_client_get_collection (unencrypted_client, ENCRYPTED_DB, ENCRYPTED_COLL);
   printf ("encrypted document: ");
   if (!print_one_document (unencrypted_coll, &error)) {
      goto fail;
   }
   printf ("\n");

   /* Expect a server-side error if inserting with the unencrypted collection.
    */
   ret = mongoc_collection_insert_one (unencrypted_coll, to_insert, NULL /* opts */, NULL /* reply */, &error);
   if (!ret) {
      printf ("insert with unencrypted collection failed: %s\n", error.message);
      memset (&error, 0, sizeof (error));
   }

   exit_status = EXIT_SUCCESS;
fail:
   if (error.code) {
      fprintf (stderr, "error: %s\n", error.message);
   }

   bson_free (local_masterkey);
   bson_destroy (kms_providers);
   mongoc_collection_destroy (keyvault_coll);
   mongoc_index_model_destroy (index_model);
   bson_destroy (index_opts);
   bson_destroy (index_keys);
   bson_json_reader_destroy (reader);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   bson_destroy (to_insert);
   mongoc_collection_destroy (unencrypted_coll);
   mongoc_client_destroy (unencrypted_client);
   mongoc_client_destroy (keyvault_client);
   bson_destroy (schema);
   bson_destroy (create_cmd);
   bson_destroy (create_cmd_opts);
   mongoc_write_concern_destroy (wc);

   mongoc_cleanup ();
   return exit_status;
}
