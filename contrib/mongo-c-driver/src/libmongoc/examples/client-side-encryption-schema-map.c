#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

#include "client-side-encryption-helpers.h"

/* Helper method to create a new data key in the key vault, a schema to use that
 * key, and writes the schema to a file for later use. */
static bool
create_schema_file (bson_t *kms_providers,
                    const char *keyvault_db,
                    const char *keyvault_coll,
                    mongoc_client_t *keyvault_client,
                    bson_error_t *error)
{
   mongoc_client_encryption_t *client_encryption = NULL;
   mongoc_client_encryption_opts_t *client_encryption_opts = NULL;
   mongoc_client_encryption_datakey_opts_t *datakey_opts = NULL;
   bson_value_t datakey_id = {0};
   char *keyaltnames[] = {"mongoc_encryption_example_1"};
   bson_t *schema = NULL;
   char *schema_string = NULL;
   size_t schema_string_len;
   FILE *outfile = NULL;
   bool ret = false;

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

   /* Use canonical JSON so that other drivers and tools will be
    * able to parse the MongoDB extended JSON file. */
   schema_string = bson_as_canonical_extended_json (schema, &schema_string_len);
   outfile = fopen ("jsonSchema.json", "w");
   if (0 == fwrite (schema_string, sizeof (char), schema_string_len, outfile)) {
      fprintf (stderr, "failed to write to file\n");
      goto fail;
   }

   ret = true;
fail:
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_datakey_opts_destroy (datakey_opts);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   bson_free (schema_string);
   bson_destroy (schema);
   bson_value_destroy (&datakey_id);
   if (outfile) {
      fclose (outfile);
   }
   return ret;
}

/* This example demonstrates how to use automatic encryption with a client-side
 * schema map using the enterprise version of MongoDB */
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
   bson_t schema = BSON_INITIALIZER;
   bson_t *schema_map = NULL;

   /* The MongoClient used to access the key vault (keyvault_namespace). */
   mongoc_client_t *keyvault_client = NULL;
   mongoc_collection_t *keyvault_coll = NULL;
   mongoc_auto_encryption_opts_t *auto_encryption_opts = NULL;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *coll = NULL;
   bson_t *to_insert = NULL;
   mongoc_client_t *unencrypted_client = NULL;
   mongoc_collection_t *unencrypted_coll = NULL;

   mongoc_init ();

   /* Configure the master key. This must be the same master key that was used
    * to create the encryption key. */
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

   /* Create a new data key and a schema using it for encryption. Save the
    * schema to the file jsonSchema.json */
   ret = create_schema_file (kms_providers, KEYVAULT_DB, KEYVAULT_COLL, keyvault_client, &error);

   if (!ret) {
      goto fail;
   }

   /* Load the JSON Schema and construct the local schema_map option. */
   reader = bson_json_reader_new_from_file ("jsonSchema.json", &error);
   if (!reader) {
      goto fail;
   }

   bson_json_reader_read (reader, &schema, &error);

   /* Construct the schema map, mapping the namespace of the collection to the
    * schema describing encryption. */
   schema_map = BCON_NEW (ENCRYPTED_DB "." ENCRYPTED_COLL, BCON_DOCUMENT (&schema));

   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   mongoc_auto_encryption_opts_set_keyvault_client (auto_encryption_opts, keyvault_client);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, KEYVAULT_DB, KEYVAULT_COLL);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);

   client = mongoc_client_new ("mongodb://localhost/?appname=client-side-encryption");
   BSON_ASSERT (client);

   /* Enable automatic encryption. It will determine that encryption is
    * necessary from the schema map instead of relying on the server to provide
    * a schema. */
   ret = mongoc_client_enable_auto_encryption (client, auto_encryption_opts, &error);
   if (!ret) {
      goto fail;
   }

   coll = mongoc_client_get_collection (client, ENCRYPTED_DB, ENCRYPTED_COLL);

   /* Clear old data */
   mongoc_collection_drop (coll, NULL);

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
   bson_destroy (&schema);
   bson_destroy (schema_map);
   mongoc_cleanup ();
   return exit_status;
}
