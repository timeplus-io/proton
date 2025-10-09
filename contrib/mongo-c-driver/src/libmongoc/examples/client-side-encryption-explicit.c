#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>

#include "client-side-encryption-helpers.h"

/* This example demonstrates how to use explicit encryption and decryption using
 * the community version of MongoDB */
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
   bson_t *schema = NULL;
   mongoc_client_t *client = NULL;
   mongoc_collection_t *coll = NULL;
   mongoc_collection_t *keyvault_coll = NULL;
   bson_t *to_insert = NULL;
   bson_t *create_cmd = NULL;
   bson_t *create_cmd_opts = NULL;
   mongoc_write_concern_t *wc = NULL;
   mongoc_client_encryption_t *client_encryption = NULL;
   mongoc_client_encryption_opts_t *client_encryption_opts = NULL;
   mongoc_client_encryption_datakey_opts_t *datakey_opts = NULL;
   char *keyaltnames[] = {"mongoc_encryption_example_3"};
   bson_value_t datakey_id = {0};
   bson_value_t encrypted_field = {0};
   bson_value_t to_encrypt = {0};
   mongoc_client_encryption_encrypt_opts_t *encrypt_opts = NULL;
   bson_value_t decrypted = {0};

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

   /* The mongoc_client_t used to read/write application data. */
   client = mongoc_client_new ("mongodb://localhost/?appname=client-side-encryption");
   coll = mongoc_client_get_collection (client, ENCRYPTED_DB, ENCRYPTED_COLL);

   /* Clear old data */
   mongoc_collection_drop (coll, NULL);

   /* Set up the key vault for this example. */
   keyvault_coll = mongoc_client_get_collection (client, KEYVAULT_DB, KEYVAULT_COLL);
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

   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, KEYVAULT_DB, KEYVAULT_COLL);

   /* Set a mongoc_client_t to use for reading/writing to the key vault. This
    * can be the same mongoc_client_t used by the main application. */
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   if (!client_encryption) {
      goto fail;
   }

   /* Create a new data key for the encryptedField.
    * https://dochub.mongodb.org/core/client-side-field-level-encryption-automatic-encryption-rules
    */
   datakey_opts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_keyaltnames (datakey_opts, keyaltnames, 1);
   if (!mongoc_client_encryption_create_datakey (client_encryption, "local", datakey_opts, &datakey_id, &error)) {
      goto fail;
   }

   /* Explicitly encrypt a field */
   encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                        MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
   mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, &datakey_id);
   to_encrypt.value_type = BSON_TYPE_UTF8;
   to_encrypt.value.v_utf8.str = "123456789";
   const size_t len = strlen (to_encrypt.value.v_utf8.str);
   BSON_ASSERT (bson_in_range_unsigned (uint32_t, len));
   to_encrypt.value.v_utf8.len = (uint32_t) len;

   ret = mongoc_client_encryption_encrypt (client_encryption, &to_encrypt, encrypt_opts, &encrypted_field, &error);
   if (!ret) {
      goto fail;
   }

   to_insert = bson_new ();
   BSON_APPEND_VALUE (to_insert, "encryptedField", &encrypted_field);
   ret = mongoc_collection_insert_one (coll, to_insert, NULL /* opts */, NULL /* reply */, &error);
   if (!ret) {
      goto fail;
   }

   printf ("encrypted document: ");
   if (!print_one_document (coll, &error)) {
      goto fail;
   }
   printf ("\n");

   /* Explicitly decrypt a field */
   ret = mongoc_client_encryption_decrypt (client_encryption, &encrypted_field, &decrypted, &error);
   if (!ret) {
      goto fail;
   }
   printf ("decrypted value: %s\n", decrypted.value.v_utf8.str);

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
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   bson_destroy (to_insert);
   bson_destroy (schema);
   bson_destroy (create_cmd);
   bson_destroy (create_cmd_opts);
   mongoc_write_concern_destroy (wc);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_datakey_opts_destroy (datakey_opts);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   bson_value_destroy (&encrypted_field);
   mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);
   bson_value_destroy (&decrypted);
   bson_value_destroy (&datakey_id);

   mongoc_cleanup ();
   return exit_status;
}
