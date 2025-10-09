#include <mongoc/mongoc.h>

int
main (void)
{
   char *mongodb_uri = getenv ("MONGODB_URI");
   char *expect_error = getenv ("EXPECT_ERROR");
   char *keyName = getenv ("KEY_NAME");
   char *keyVaultEndpoint = getenv ("KEY_VAULT_ENDPOINT");

   if (!mongodb_uri || !keyName || !keyVaultEndpoint) {
      MONGOC_ERROR ("Error: expecting environment variables to be set: "
                    "MONGODB_URI, KEY_NAME, KEY_VAULT_ENDPOINT");
      return EXIT_FAILURE;
   }

   mongoc_init ();

   mongoc_client_t *keyvault_client = mongoc_client_new (mongodb_uri);
   MONGOC_DEBUG ("libmongoc version: %s", mongoc_get_version ());

   mongoc_client_encryption_opts_t *ceopts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_client (ceopts, keyvault_client);
   mongoc_client_encryption_opts_set_keyvault_namespace (ceopts, "keyvault", "datakeys");

   bson_t *kms_providers = BCON_NEW ("azure", "{", "}");

   mongoc_client_encryption_opts_set_kms_providers (ceopts, kms_providers);

   bson_error_t error;
   mongoc_client_encryption_t *ce = mongoc_client_encryption_new (ceopts, &error);
   if (!ce) {
      MONGOC_ERROR ("Error in mongoc_client_encryption_new: %s", error.message);
      return EXIT_FAILURE;
   }

   mongoc_client_encryption_datakey_opts_t *dkopts;
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   bson_t *masterkey = BCON_NEW ("keyVaultEndpoint", BCON_UTF8 (keyVaultEndpoint), "keyName", BCON_UTF8 (keyName));
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, masterkey);

   bson_value_t keyid;
   bool got = mongoc_client_encryption_create_datakey (ce, "azure", dkopts, &keyid, &error);

   if (NULL != expect_error) {
      if (got) {
         MONGOC_ERROR ("Expected error to contain %s, but got success", expect_error);
         return EXIT_FAILURE;
      }
      if (NULL == strstr (error.message, expect_error)) {
         MONGOC_ERROR ("Expected error to contain %s, but got: %s", expect_error, error.message);
         return EXIT_FAILURE;
      }
   } else {
      if (!got) {
         MONGOC_ERROR ("Expected to create data key, but got error: %s", error.message);
         return EXIT_FAILURE;
      }
      MONGOC_DEBUG ("Created key\n");
   }
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);
   mongoc_client_encryption_destroy (ce);
   bson_destroy (kms_providers);
   mongoc_client_encryption_opts_destroy (ceopts);
   mongoc_client_destroy (keyvault_client);

   mongoc_cleanup ();
}
